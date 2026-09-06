// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package forging

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"runtime/debug"
	"sync"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/vrf"
	"github.com/prometheus/client_golang/prometheus"
)

// errNoValidTxRefs is returned by buildLeiosEB when all mempool transactions
// are filtered out (invalid hash, zero size, or size > uint16). It is
// treated as a skip rather than a hard failure by checkAndForgeLeiosEB.
var errNoValidTxRefs = errors.New(
	"no valid transaction references for endorser block",
)

// Mode represents the forging mode.
type Mode int

const (
	// ModeDev is a simplified mode where the node produces all blocks on a
	// fixed interval without real VRF/KES. Used for single-node devnets.
	ModeDev Mode = iota

	// ModeProduction uses real VRF leader election and KES signing.
	// Requires loaded pool credentials.
	ModeProduction

	// forgeSyncToleranceSlots is the number of slots the chain tip may lag
	// behind the upstream peer tip before the forger skips block production.
	// This accommodates block processing latency and VRF schedule computation
	// time on fast-slot networks (e.g. 100ms devnet slots) while still
	// catching bulk sync.
	forgeSyncToleranceSlots = 100

	// forgeStaleGapThresholdSlots is the slot gap between the chain tip
	// and the slot clock above which the forger logs an error suggesting
	// the database contains data from a different genesis.
	forgeStaleGapThresholdSlots = 1000
)

// BlockForger coordinates block production for a stake pool.
type BlockForger struct {
	mode   Mode
	logger *slog.Logger

	// Production mode components
	creds            *PoolCredentials
	leaderChecker    LeaderChecker
	blockBuilder     BlockBuilder
	blockBroadcaster BlockBroadcaster
	confirmedTxs     ConfirmedTxRemover
	blockForged      BlockForgedObserver
	slotClock        SlotClockProvider
	slotDuration     time.Duration
	opCertLedgerView LedgerView
	eraParams        ProtocolParamsProvider

	// Equal-slot alternative forging. Both are optional and both must be
	// present, alongside a BlockBuilder that implements
	// AlternativeBlockBuilder, for a contested slot to be contested rather
	// than conceded.
	chainContext   AlternativeChainContextProvider
	siblingAdopter SiblingBlockAdopter

	// Slot battle detection
	slotTracker *SlotTracker

	// Duplicate-slot fence. fenceStore is nil when no metadata store is
	// wired (dev mode, embedders); lastForgedSlot is then in-memory
	// only. Both are touched exclusively from the single forge loop
	// goroutine after construction.
	fenceStore     ForgeFenceStore
	lastForgedSlot uint64
	fenceLoaded    bool

	// Optional Leios EB forging (nil = relay or pre-Dijkstra era)
	leiosChecker   LeiosProduceChecker
	leiosEBCaster  EndorserBlockBroadcaster
	leiosMempool   MempoolProvider
	leiosValidator TxValidator
	leiosCerts     LeiosCertificateProvider
	leiosParent    LeiosParentAnnouncementProvider

	// Prometheus metrics
	metrics *forgingMetrics

	// Configurable forging tolerances
	forgeSyncToleranceSlots     uint64
	forgeStaleGapThresholdSlots uint64

	// Optional self-validation before adoption (nil = disabled)
	blockValidator BlockValidator

	// State
	mu      sync.RWMutex
	running bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// ForgeFenceStore persists the highest slot this node has committed to
// forging. It is the durable half of the duplicate-slot fence: the
// in-memory chain tip is lost on restart, and a tip that has rolled back
// no longer proves which slots were already used.
//
// The fence is written before the block header for a slot is signed, so a
// crash anywhere between signing and adoption still leaves the slot
// recorded. Refusing a slot the node did not actually use costs one
// block; signing a second, different block for a slot whose first block
// may already have reached peers is equivocation.
type ForgeFenceStore interface {
	// LoadLastForgedSlot returns the highest recorded slot and whether
	// any fence has been recorded yet.
	LoadLastForgedSlot() (uint64, bool, error)
	// StoreLastForgedSlot durably records slot as used. It must not
	// return until the record survives a crash.
	StoreLastForgedSlot(slot uint64) error
}

// LeaderChecker determines if the pool should produce a block for a given slot.
type LeaderChecker interface {
	// ShouldProduceBlock returns true if this pool is the leader for the slot.
	ShouldProduceBlock(slot uint64) bool
	// NextLeaderSlot returns the next slot where this pool is leader.
	NextLeaderSlot(fromSlot uint64) (uint64, bool)
}

// BlockBuilder constructs blocks from mempool transactions.
type BlockBuilder interface {
	// BuildBlock creates a new block for the given slot.
	// Returns the block and its CBOR encoding.
	BuildBlock(slot uint64, kesPeriod uint64) (ledger.Block, []byte, error)
}

// BlockContext names the parent a forged block is built on. It mirrors
// ouroboros-consensus' BlockContext (NodeKernel/Forge.hs): a block number and
// the point the block extends.
//
// A nil *BlockContext means "extend the live chain tip", which is what every
// uncontested slot uses and what BuildBlock does. An explicit context exists
// for the equal-slot case, where the tip already holds a rival block at the
// slot being forged: there the alternative carries the rival's block number
// and names the rival's predecessor as parent, so the two blocks are siblings
// that chain selection arbitrates between.
type BlockContext struct {
	// Parent is the point the forged block names as its parent. It must sit
	// strictly below the forged slot.
	Parent ocommon.Point
	// BlockNumber is the block number the forged block carries. For an
	// equal-slot alternative this is the rival's own block number, not the
	// rival's plus one.
	BlockNumber uint64
	// Rival is the chain tip this block is an alternative to. The builder
	// re-checks it against the live tip and abandons the candidate if the
	// chain has moved, so a contest that is already over costs nothing.
	Rival ochainsync.Tip
}

// AlternativeBlockBuilder constructs a block on an explicitly named parent
// rather than on the live chain tip. A BlockBuilder that does not implement it
// cannot forge the equal-slot alternative, and the forger declines contested
// slots for such a builder exactly as it did before this capability existed.
type AlternativeBlockBuilder interface {
	BuildBlockOnContext(
		slot uint64,
		kesPeriod uint64,
		leios LeiosBlockData,
		blockCtx BlockContext,
	) (ledger.Block, []byte, error)
}

// LeiosBlockBuilder constructs Dijkstra blocks with Leios prototype header/body
// extensions. Builders that do not implement it cannot safely announce or
// certify Leios endorser blocks.
type LeiosBlockBuilder interface {
	BuildBlockWithLeios(
		slot uint64,
		kesPeriod uint64,
		leios LeiosBlockData,
	) (ledger.Block, []byte, error)
}

// credentialGenerationBlockBuilder lets the production builder consume the
// exact credential generation pinned by BlockForger. It is intentionally
// package-private: BlockBuilder and LeiosBlockBuilder remain API-compatible,
// while DefaultBlockBuilder avoids re-reading mutable shared credentials.
type credentialGenerationBlockBuilder interface {
	buildBlockWithCredentialGeneration(
		slot uint64,
		kesPeriod uint64,
		leios LeiosBlockData,
		generation *credentialGeneration,
		blockCtx *BlockContext,
	) (ledger.Block, []byte, error)
}

// BlockBroadcaster submits built blocks to the chain.
type BlockBroadcaster interface {
	// AddBlock adds a block to the local chain and propagates to peers.
	AddBlock(block ledger.Block, cbor []byte) error
}

// AlternativeChainContextProvider reports the context needed to forge an
// alternative to the block already at the chain tip: the tip itself and the
// point of its immediate predecessor, which becomes the alternative's parent.
// chain.Chain implements it directly.
//
// ok must be false whenever the context cannot be established. The forger then
// declines the contested slot rather than falling back to the live tip, which
// would produce a block whose parent slot equals its own.
type AlternativeChainContextProvider interface {
	TipPredecessor() (parent ocommon.Point, tip ochainsync.Tip, ok bool)
}

// SiblingBlockAdopter offers a locally forged block that competes with the
// current chain tip -- rather than extending it -- to chain selection, and
// reports whether it was adopted. ledger.LedgerState implements it as
// AdoptLocalForgedSibling.
//
// Losing is a normal outcome, not an error: the block at the tip keeps the
// slot and the forged block is discarded without being diffused.
type SiblingBlockAdopter interface {
	AdoptLocalForgedSibling(block ledger.Block) (adopted bool, err error)
}

// ConfirmedTxRemover removes transactions after the block containing them has
// been adopted locally.
type ConfirmedTxRemover interface {
	RemoveTxsByHash(hashes []string)
}

// BlockForgedObserver observes blocks after they are successfully built and
// chain adoption has been attempted.
type BlockForgedObserver func(
	block ledger.Block,
	cbor []byte,
	latency time.Duration,
)

// BlockValidator validates a locally-forged block before it is adopted
// onto the local chain and diffused to peers. If ValidateForgedBlock
// returns a non-nil error the block is dropped and neither adopted nor
// diffused.
type BlockValidator interface {
	ValidateForgedBlock(block ledger.Block, blockCbor []byte) error
}

// LeiosProduceChecker is the forge-loop seam into the Leios pipeline.
// It reports whether the slot leader may produce an endorser block for
// the given slot (respects the single-EB-per-slot rule and produce window).
// A nil checker means Leios EB forging is disabled (relay or pre-Dijkstra era).
type LeiosProduceChecker interface {
	MayProduceEndorserBlock(
		slot uint64,
	) (allowed bool, reason string, err error)
}

// LeiosCertifiedEndorserBlock is a certified EB ready for inclusion in a
// Dijkstra ranking block.
type LeiosCertifiedEndorserBlock struct {
	SlotNo            uint64
	EndorserBlockHash lcommon.Blake2b256
	Certificate       *lcommon.LeiosEbCertificate
	AnnouncingRbHash  lcommon.Blake2b256
}

// LeiosCertificateProvider supplies certified EBs and records successful
// inclusion after the certifying ranking block is adopted.
type LeiosCertificateProvider interface {
	EligibleCertifiedEndorserBlocks() []LeiosCertifiedEndorserBlock
	CertifiedEndorserBlockTxHashes(
		ebHash lcommon.Blake2b256,
		ebSlot uint64,
	) (hashes []string, ok bool)
	MarkEndorserBlockEmbedded(ebHash lcommon.Blake2b256)
}

// LeiosParentAnnouncementProvider reports the EB announced by the parent
// ranking block. CertRBs may only certify that announced EB.
type LeiosParentAnnouncementProvider interface {
	ParentLeiosAnnouncement() (
		lcommon.Blake2b256,
		lcommon.Blake2b256,
		bool,
		error,
	)
}

// EndorserBlockBroadcaster stores a locally-forged endorser block and
// notifies connected peers via the LeiosNotify protocol. txBodies are the
// referenced transactions' raw CBOR, in manifest order, so the endorser block
// can also be served over leios-fetch.
type EndorserBlockBroadcaster interface {
	BroadcastEndorserBlock(
		slot uint64,
		hash []byte,
		cbor []byte,
		txBodies [][]byte,
	) error
}

// LeiosEndorserBlockAnnouncement is the header extension payload for an
// endorser block announced by a Dijkstra ranking block.
type LeiosEndorserBlockAnnouncement struct {
	Hash lcommon.Blake2b256
	Size uint64
}

// LeiosBlockData carries the Leios prototype data a Dijkstra ranking block
// should commit to. Since prototype-2026w29 a ranking block may certify its
// parent's endorser block and independently announce a new one.
type LeiosBlockData struct {
	Announcement *LeiosEndorserBlockAnnouncement
	Certificate  *lcommon.LeiosEbCertificate
}

func (d LeiosBlockData) empty() bool {
	return d.Announcement == nil && d.Certificate == nil
}

// SlotClockProvider provides current slot information from the slot clock.
type SlotClockProvider interface {
	// CurrentSlot returns the current slot number based on wall-clock time.
	CurrentSlot() (uint64, error)
	// SlotsPerKESPeriod returns the number of slots in a KES period.
	SlotsPerKESPeriod() uint64
	// ChainTipSlot returns the slot number of the current chain tip.
	ChainTipSlot() uint64
	// NextSlotTime returns the wall-clock time when the next slot begins.
	NextSlotTime() (time.Time, error)
	// UpstreamTipSlot returns the latest admitted header slot from upstream
	// peers. Returns 0 if no corroborated target is available.
	UpstreamTipSlot() uint64
	// UpstreamSyncStatus reports whether a live upstream is selected and its
	// corroborated target.
	UpstreamSyncStatus() (targetSlot uint64, active bool)
}

// ChainTipHashProvider is an optional extension of SlotClockProvider.
// When the wired slot clock implements it, the forger can identify the
// block sitting at the chain tip by hash instead of inferring ownership
// of a slot from the forge fence alone.
//
// It is deliberately a separate, optional interface so that existing
// SlotClockProvider implementations outside this repository keep
// compiling; a clock that does not implement it falls back to the fence.
type ChainTipHashProvider interface {
	// ChainTipHash returns the block hash of the current chain tip, or
	// nil when the chain is empty or the hash is unavailable.
	ChainTipHash() []byte
}

// ForgerConfig holds configuration for the block forger.
type ForgerConfig struct {
	Mode         Mode
	Logger       *slog.Logger
	SlotDuration time.Duration

	// Production mode configuration. Credentials must have passed
	// ValidateKESPeriod so the forger can enforce the genesis KES lifetime.
	Credentials      *PoolCredentials
	LeaderChecker    LeaderChecker
	BlockBuilder     BlockBuilder
	BlockBroadcaster BlockBroadcaster
	ConfirmedTxs     ConfirmedTxRemover
	BlockForged      BlockForgedObserver
	SlotClock        SlotClockProvider

	// OpCertLedgerView supplies the highest OpCert issue-number counter the
	// ledger has observed on chain for this pool. When non-nil, the forge
	// loop pre-flights the candidate counter against it using the same
	// era-scoped rule block application enforces (see
	// ledger/verify_opcert.go validateOpCertCounter), after leader
	// selection but before Leios work and the forge-slot fence -- a stale
	// or gapped counter is rejected there instead of reaching an
	// `AddLocalBlock` call the chain would discard anyway. Nil disables
	// the check (dev mode, embedders without ledger wiring). Requires
	// EraParams.
	OpCertLedgerView LedgerView
	// EraParams supplies the era-defining protocol parameters in effect for
	// the slot being forged, so OpCertLedgerView's counter check applies
	// the correct era-scoped rule: TPraos (Shelley-Alonzo) accepts any
	// forward counter movement, Praos (Babbage onward) additionally
	// rejects one that skips ahead of the last-seen value by more than
	// one. Required whenever OpCertLedgerView is set.
	EraParams ProtocolParamsProvider
	// ChainContext and SiblingAdopter enable forging an alternative when the
	// chain tip already holds a rival block at the slot this node leads --
	// ouroboros-consensus mkCurrentBlockContext's EQ case. Both are
	// optional; with either unset, or with a BlockBuilder that does not
	// implement AlternativeBlockBuilder, a contested slot is declined and
	// counted exactly as it was before this capability existed.
	ChainContext   AlternativeChainContextProvider
	SiblingAdopter SiblingBlockAdopter

	// ForgeFence persists the last-forged-slot fence so a restart cannot
	// sign a second block for a slot this node already used. Nil
	// disables the durable fence, which leaves only the in-memory chain
	// tip guarding against duplicate slots.
	ForgeFence ForgeFenceStore

	// LeiosProduceChecker enables EB forging when non-nil. Requires
	// LeiosEBBroadcaster and LeiosMempool to also be set.
	LeiosProduceChecker LeiosProduceChecker
	// LeiosEBBroadcaster propagates locally-forged EBs to peers.
	LeiosEBBroadcaster EndorserBlockBroadcaster
	// LeiosMempool provides transactions for EB building. May reuse the
	// same MempoolProvider as the RB builder.
	LeiosMempool MempoolProvider
	// LeiosTxValidator re-validates endorser-block transactions against one
	// coherent ledger snapshot before the EB is broadcast. Production wiring
	// supplies LedgerState; nil preserves compatibility for test embedders.
	LeiosTxValidator TxValidator
	// LeiosCertificateProvider supplies certified EBs for Dijkstra CertRBs.
	LeiosCertificateProvider LeiosCertificateProvider
	// LeiosParentAnnouncementProvider supplies the EB hash announced by the
	// parent RB so CertRB selection cannot certify an unrelated EB.
	LeiosParentAnnouncementProvider LeiosParentAnnouncementProvider

	// ForgeSyncToleranceSlots controls how far the local chain can lag the
	// upstream tip before forging is skipped. Zero uses the default.
	ForgeSyncToleranceSlots uint64
	// ForgeStaleGapThresholdSlots controls when to log an error if the
	// chain tip is far ahead of the slot clock. Zero uses the default.
	ForgeStaleGapThresholdSlots uint64

	// BlockValidator, when non-nil, validates the forged block (VRF/KES
	// header crypto, body-hash consistency, per-tx ledger rules) before
	// AddBlock is called. A validation failure drops the block without
	// adopting or diffusing it. Nil disables self-validation (default).
	BlockValidator BlockValidator

	// Prometheus metrics registry (optional)
	PromRegistry prometheus.Registerer
}

// NewBlockForger creates a new block forger.
func NewBlockForger(cfg ForgerConfig) (*BlockForger, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	if cfg.SlotDuration == 0 {
		cfg.SlotDuration = time.Second // Default 1 second slots
	}

	f := &BlockForger{
		mode:             cfg.Mode,
		logger:           cfg.Logger,
		slotDuration:     cfg.SlotDuration,
		creds:            cfg.Credentials,
		leaderChecker:    cfg.LeaderChecker,
		blockBuilder:     cfg.BlockBuilder,
		blockBroadcaster: cfg.BlockBroadcaster,
		confirmedTxs:     cfg.ConfirmedTxs,
		blockForged:      cfg.BlockForged,
		slotClock:        cfg.SlotClock,
		slotTracker:      NewSlotTracker(),
		leiosChecker:     cfg.LeiosProduceChecker,
		leiosEBCaster:    cfg.LeiosEBBroadcaster,
		leiosMempool:     cfg.LeiosMempool,
		leiosValidator:   cfg.LeiosTxValidator,
		leiosCerts:       cfg.LeiosCertificateProvider,
		leiosParent:      cfg.LeiosParentAnnouncementProvider,
		blockValidator:   cfg.BlockValidator,
		fenceStore:       cfg.ForgeFence,
		opCertLedgerView: cfg.OpCertLedgerView,
		eraParams:        cfg.EraParams,
		chainContext:     cfg.ChainContext,
		siblingAdopter:   cfg.SiblingAdopter,
	}
	if cfg.ForgeSyncToleranceSlots == 0 {
		cfg.ForgeSyncToleranceSlots = forgeSyncToleranceSlots
	}
	if cfg.ForgeStaleGapThresholdSlots == 0 {
		cfg.ForgeStaleGapThresholdSlots = forgeStaleGapThresholdSlots
	}
	f.forgeSyncToleranceSlots = cfg.ForgeSyncToleranceSlots
	f.forgeStaleGapThresholdSlots = cfg.ForgeStaleGapThresholdSlots

	if cfg.Mode == ModeProduction {
		if cfg.Credentials == nil || !cfg.Credentials.IsLoaded() {
			return nil, errors.New(
				"production mode requires loaded credentials",
			)
		}
		if cfg.LeaderChecker == nil {
			return nil, errors.New("production mode requires leader checker")
		}
		if cfg.BlockBuilder == nil {
			return nil, errors.New("production mode requires block builder")
		}
		if cfg.BlockBroadcaster == nil {
			return nil, errors.New("production mode requires block broadcaster")
		}
		if cfg.SlotClock == nil {
			return nil, errors.New("production mode requires slot clock")
		}
		generation := cfg.Credentials.acquireCredentialGeneration()
		_, _, _, err := generation.validatedKESProtocolLifetime()
		generation.release()
		if err != nil {
			return nil, fmt.Errorf(
				"production mode requires a validated KES protocol lifetime: %w",
				err,
			)
		}
		if cfg.LeiosProduceChecker != nil && cfg.LeiosTxValidator == nil {
			return nil, errors.New(
				"production Leios forging requires transaction validator",
			)
		}
	}
	if cfg.LeiosProduceChecker != nil {
		if cfg.LeiosEBBroadcaster == nil {
			return nil, errors.New(
				"LeiosProduceChecker requires LeiosEBBroadcaster",
			)
		}
		if cfg.LeiosMempool == nil {
			return nil, errors.New("LeiosProduceChecker requires LeiosMempool")
		}
	}
	if cfg.LeiosCertificateProvider != nil &&
		cfg.LeiosParentAnnouncementProvider == nil {
		return nil, errors.New(
			"leios certificate provider requires LeiosParentAnnouncementProvider",
		)
	}
	if cfg.OpCertLedgerView != nil && cfg.EraParams == nil {
		return nil, errors.New(
			"OpCertLedgerView requires EraParams to resolve the era-scoped opcert counter rule",
		)
	}

	// Load the persisted fence before the forger can be started. A store
	// that cannot be read offers no duplicate-slot protection, so fail
	// wiring rather than start a producer without it.
	if f.fenceStore != nil {
		slot, ok, err := f.fenceStore.LoadLastForgedSlot()
		if err != nil {
			return nil, fmt.Errorf("failed to load forge fence: %w", err)
		}
		f.lastForgedSlot = slot
		f.fenceLoaded = ok
		if ok {
			cfg.Logger.Info(
				"loaded last-forged-slot fence",
				"last_forged_slot", slot,
			)
		}
	} else if cfg.Mode == ModeProduction {
		cfg.Logger.Warn(
			"no forge fence store configured; duplicate-slot " +
				"protection will not survive a restart",
		)
	}

	if cfg.PromRegistry != nil {
		f.metrics = initForgingMetrics(cfg.PromRegistry)
	}

	// Set static OpCert gauges immediately so SPO dashboards show
	// certificate info without waiting for the first forged block.
	// Dynamic gauges (currentKESPeriod, remainingKESPeriods) are
	// updated on every slot-win in updateKESMetrics().
	if f.metrics != nil && f.creds != nil {
		generation := f.creds.acquireCredentialGeneration()
		f.updateKESPolicyMetrics(generation)
		generation.release()
	}

	return f, nil
}

// Start begins the block forging process.
// The provided context controls the forger's lifecycle.
func (f *BlockForger) Start(ctx context.Context) error {
	f.mu.Lock()
	if f.running {
		f.mu.Unlock()
		return errors.New("forger already running")
	}
	f.running = true

	ctx, cancel := context.WithCancel(ctx)
	f.cancel = cancel
	f.wg.Add(1)
	f.mu.Unlock()

	f.logger.Info("block forger started", "mode", f.modeString())

	// Set initial KES period metrics so dashboards show correct values
	// immediately rather than waiting for the first block production.
	if f.metrics != nil && f.slotClock != nil && f.creds != nil {
		if slotsPerKES := f.slotClock.SlotsPerKESPeriod(); slotsPerKES > 0 {
			if currentSlot, err := f.slotClock.CurrentSlot(); err == nil {
				if kesPeriod, err := CurrentKESPeriod(
					currentSlot,
					slotsPerKES,
				); err == nil {
					generation := f.creds.acquireCredentialGeneration()
					f.updateKESMetrics(kesPeriod, generation)
					generation.release()
				}
			}
		}
	}

	go f.runLoop(ctx)
	return nil
}

// Stop stops the block forging process.
// It blocks until the runLoop goroutine has exited.
func (f *BlockForger) Stop() {
	f.mu.Lock()
	if !f.running {
		f.mu.Unlock()
		return
	}

	f.running = false
	if f.cancel != nil {
		f.cancel()
	}
	f.mu.Unlock()

	// Wait for the goroutine to finish before returning
	f.wg.Wait()
	f.logger.Info("block forger stopped")
}

// IsRunning returns true if the forger is currently running.
func (f *BlockForger) IsRunning() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.running
}

// runLoop is the main forging loop.
func (f *BlockForger) runLoop(ctx context.Context) {
	defer f.wg.Done()
	defer func() {
		f.mu.Lock()
		f.running = false
		f.mu.Unlock()
	}()

	if f.mode == ModeProduction {
		f.runLoopSlotAligned(ctx)
	} else {
		f.runLoopTicker(ctx)
	}
}

// runLoopSlotAligned wakes at each slot boundary so the forger can
// produce before peer blocks arrive.
func (f *BlockForger) runLoopSlotAligned(ctx context.Context) {
	retries := 0
	for {
		nextSlot, err := f.slotClock.NextSlotTime()
		if err != nil {
			retries++
			if f.metrics != nil {
				f.metrics.slotClockErrors.Inc()
			}
			// Log warning periodically so operators see why forger is stuck
			if retries%50 == 1 {
				f.logger.Warn(
					"slot clock not ready, retrying",
					"error", err,
					"retries", retries,
				)
			}
			// Exponential backoff: 100ms, 200ms, 400ms, ... capped at 5s
			backoff := min(
				time.Duration(100<<min(retries-1, 6))*time.Millisecond,
				5*time.Second,
			)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				continue
			}
		}
		retries = 0

		sleepDur := max(time.Until(nextSlot), 0)

		timer := time.NewTimer(sleepDur)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			if err := f.checkAndForge(ctx); err != nil {
				f.logger.Error("forge check failed", "error", err)
			}
		}
	}
}

// runLoopTicker uses a fixed interval ticker for dev mode.
func (f *BlockForger) runLoopTicker(ctx context.Context) {
	ticker := time.NewTicker(f.slotDuration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := f.checkAndForge(ctx); err != nil {
				f.logger.Error("forge check failed", "error", err)
			}
		}
	}
}

// checkAndForge checks if we should forge a block and does so if appropriate.
func (f *BlockForger) checkAndForge(ctx context.Context) error {
	switch f.mode {
	case ModeDev:
		// Dev mode: forge on every tick (handled elsewhere in existing code)
		return nil
	case ModeProduction:
		return f.checkAndForgeProduction(ctx)
	default:
		return fmt.Errorf("unknown forging mode: %d", f.mode)
	}
}

// checkAndForgeProduction implements production mode forging.
func (f *BlockForger) checkAndForgeProduction(_ context.Context) error {
	forgeStartTime := time.Now()

	// Get current slot from slot clock
	currentSlot, err := f.slotClock.CurrentSlot()
	if err != nil {
		return fmt.Errorf("failed to get current slot: %w", err)
	}

	tipSlot := f.slotClock.ChainTipSlot()

	// Skip if the chain has already moved PAST the current slot.
	// A tip beyond currentSlot means any block we produced would fork
	// the chain below its own tip. A tip AT currentSlot is the
	// contested case and is handled after leader selection below:
	// ouroboros-consensus mkCurrentBlockContext declines only for GT
	// and treats EQ as a slot battle.
	// Count every slot check (matches cardano-node
	// Forge.about_to_lead)
	if f.metrics != nil {
		f.metrics.forgeAboutToLead.Inc()
		f.metrics.tipGapSlots.Set(0)
	}

	if currentSlot < tipSlot {
		// Detect stale data: if the tip is far ahead of the slot clock,
		// the database likely contains chain data from a different genesis.
		// Use subtraction (safe here since tipSlot > currentSlot from
		// the outer check) to avoid uint64 overflow on the addition.
		gap := tipSlot - currentSlot
		if f.metrics != nil {
			f.metrics.tipGapSlots.Set(float64(gap))
		}
		if gap > f.forgeStaleGapThresholdSlots {
			// This gate also runs before leader selection, so it
			// swallows a scheduled leader slot as silently as the
			// skips routed through logGateSkip. The stale-genesis
			// diagnosis stays at Error, but carry the same
			// leader_slot marker so a lost block is still
			// attributable. Reuse isScheduledLeaderSlot rather
			// than repeating the schedule lookup.
			attrs := []any{
				"current_slot",
				currentSlot,
				"tip_slot",
				tipSlot,
				"slot_gap",
				gap,
			}
			if f.isScheduledLeaderSlot(currentSlot) {
				attrs = append(attrs, "leader_slot", true)
			}
			f.logger.Error(
				"chain tip is far ahead of slot clock; database may contain data from a different genesis",
				attrs...,
			)
		} else {
			f.logGateSkip(
				currentSlot,
				"forge skip: chain tip is ahead of the current slot",
				"current_slot", currentSlot,
				"tip_slot", tipSlot,
			)
		}
		return nil
	}

	// The tip is at the current slot. Before treating that as a
	// contested slot, work out whether the block sitting there is one
	// this node produced.
	if currentSlot == tipSlot {
		ownership, ourHash, tipHash := f.tipBlockOwnership(currentSlot)
		fenceCovers := f.fenceLoaded && currentSlot <= f.lastForgedSlot
		switch {
		case ownership == tipOwnershipOurs:
			// The slot-aligned loop has simply re-entered a slot
			// whose block is provably ours. Not a contested slot,
			// and the fence would refuse it anyway.
			f.logger.Debug(
				"forge skip: slot already has our own block",
				"current_slot", currentSlot,
				"tip_slot", tipSlot,
				"last_forged_slot", f.lastForgedSlot,
				"matched_by", "forged_block_hash",
			)
			return nil
		case ownership == tipOwnershipRival && fenceCovers:
			// We committed to this slot and a rival's block is what
			// the chain adopted for it. The fence still forbids
			// forging: a second, different block for a slot whose
			// first block may already have reached peers is
			// equivocation, and losing a battle is not a licence to
			// equivocate. But this is a slot battle we lost, not
			// "the tip block is ours", and dropping it at Debug is
			// exactly the silent loss this change exists to remove.
			//
			// slotBattlesTotal is deliberately NOT incremented here.
			// Reaching this case means a block other than ours was
			// accepted at a slot SlotTracker says we forged, and the
			// only path that accepts a peer's block already ran
			// LedgerState.checkSlotBattle over the same tracker
			// (node wiring points ForgedBlockChecker at this
			// forger's SlotTracker and SlotBattleRecorder at this
			// forger), which found the same hash mismatch and
			// counted the battle. chainsync owns that count;
			// counting it again here would double it.
			f.incCouldNotForge()
			f.logger.Warn(
				"slot battle lost: rival block at tip for a slot this node already forged",
				"current_slot", currentSlot,
				"tip_slot", tipSlot,
				"last_forged_slot", f.lastForgedSlot,
				"our_block_hash", hex.EncodeToString(ourHash),
				"tip_block_hash", hex.EncodeToString(tipHash),
			)
			return nil
		case fenceCovers:
			// Ownership is inconclusive, but the fence says this
			// node already committed to the slot, so it is not
			// available regardless of who holds the tip.
			f.logger.Debug(
				"forge skip: slot already has our own block",
				"current_slot", currentSlot,
				"tip_slot", tipSlot,
				"last_forged_slot", f.lastForgedSlot,
				"matched_by", "forge_fence",
			)
			return nil
		}
		// Neither signal claims the slot: fall through and let leader
		// selection and the contested-slot branch account for it.
	}

	// Skip if the chain is still syncing from a peer.
	// Compare against the admitted upstream header frontier rather than the
	// wall clock. Forging while syncing creates blocks that conflict
	// with the peer's chain, causing persistent header mismatches
	// and resync loops.
	// See forgeSyncToleranceSlots for the tolerance rationale.
	upstreamTip, upstreamActive := f.slotClock.UpstreamSyncStatus()
	if upstreamActive && (upstreamTip == 0 ||
		(upstreamTip > tipSlot &&
			upstreamTip-tipSlot > f.forgeSyncToleranceSlots)) {
		if f.metrics != nil {
			gap := uint64(0)
			if upstreamTip > tipSlot {
				gap = upstreamTip - tipSlot
			}
			f.metrics.forgeSyncSkip.Inc()
			f.metrics.tipGapSlots.Set(
				float64(gap),
			)
		}
		f.logGateSkip(
			currentSlot,
			"chain syncing from peer, skipping forge",
			"current_slot", currentSlot,
			"tip_slot", tipSlot,
			"upstream_tip", upstreamTip,
		)
		return nil
	}

	// Compute and enforce the protocol KES lifetime before Praos leader
	// selection, Leios work, or ranking-block construction. The expiry was
	// checked for overflow when the production forger was created, so these
	// comparisons cannot wrap.
	slotsPerKESPeriod := f.slotClock.SlotsPerKESPeriod()
	if slotsPerKESPeriod == 0 {
		return errors.New("slots per KES period is zero")
	}
	kesPeriod, err := CurrentKESPeriod(currentSlot, slotsPerKESPeriod)
	if err != nil {
		return err
	}
	generation := f.creds.acquireCredentialGeneration()
	generationReleased := false
	defer func() {
		if !generationReleased {
			generation.release()
		}
	}()
	f.updateKESMetrics(kesPeriod, generation)
	opCertStart, maxEvolutions, opCertExpiry, policyErr := generation.validatedKESProtocolLifetime()
	if policyErr != nil {
		f.incCouldNotForge()
		f.logger.Error(
			"forge skip: KES protocol lifetime is not validated",
			"slot", currentSlot,
			"current_kes_period", kesPeriod,
			"credential_generation", generation.id,
			"error", policyErr,
		)
		return nil
	}
	if kesPeriod < opCertStart {
		f.incCouldNotForge()
		f.logger.Error(
			"forge skip: operational certificate is not yet valid",
			"slot", currentSlot,
			"current_kes_period", kesPeriod,
			"opcert_start_period", opCertStart,
			"opcert_expiry_period", opCertExpiry,
			"max_kes_evolutions", maxEvolutions,
		)
		return nil
	}
	if kesPeriod >= opCertExpiry {
		f.incCouldNotForge()
		f.logger.Error(
			"forge skip: operational certificate expired; rotate the operational certificate",
			"slot",
			currentSlot,
			"current_kes_period",
			kesPeriod,
			"opcert_start_period",
			opCertStart,
			"opcert_expiry_period",
			opCertExpiry,
			"max_kes_evolutions",
			maxEvolutions,
		)
		return nil
	}

	// Check if we're the leader for this slot only after the KES gate.
	isLeader := f.checkLeaderSafe(currentSlot)
	if !isLeader {
		f.logger.Debug(
			"forge check: not leader for slot",
			"current_slot", currentSlot,
			"tip_slot", tipSlot,
		)
		if f.metrics != nil {
			f.metrics.forgeNotLeader.Inc()
		}
		return nil
	}

	// Pre-flight the OpCert counter against the ledger's observed on-chain
	// state and the era-scoped rule block application enforces (see
	// checkOpCertSequence). This covers genesis/era context the KES-lifetime
	// gate above does not: LatestOpCertSequence advances as blocks are
	// applied (this node's own or a peer's for the same pool), so a key
	// state that was fine at startup or on an earlier slot can become stale,
	// or -- from Babbage onward -- gapped, by the time a later slot is won.
	// Checked only for the winning slot -- after the cheap leader check --
	// rather than on every KES-valid slot, since the check performs a real
	// ledger read that would otherwise run regardless of whether this pool
	// is even leader for the slot.
	if f.opCertLedgerView != nil {
		if err := f.checkOpCertSequence(currentSlot, generation); err != nil {
			f.incCouldNotForge()
			f.logger.Error(
				"forge skip: operational certificate counter is not valid for chain state",
				"slot",
				currentSlot,
				"credential_generation",
				generation.id,
				"error",
				err,
			)
			return nil
		}
	}

	// The credential snapshot owns its secret material, so the callback above
	// never holds a writer-blocking lease. A reload still invalidates this
	// attempt before any Leios or block-construction work begins.
	if err := generation.ensureCurrent(); err != nil {
		f.incCouldNotForge()
		f.logger.Warn(
			"forge skip: credentials changed during leader selection",
			"slot", currentSlot,
			"selected_generation", generation.id,
			"error", err,
		)
		return nil
	}
	if err := generation.validateKESPeriod(kesPeriod); err != nil {
		f.incCouldNotForge()
		f.logger.Error(
			"forge skip: credential generation became invalid during leader selection",
			"slot",
			currentSlot,
			"credential_generation",
			generation.id,
			"error",
			err,
		)
		return nil
	}

	// We are the slot leader with the same credential generation that passed
	// the pre-selection gate.
	if f.metrics != nil {
		f.metrics.forgeNodeIsLeader.Inc()
	}

	// A rival block already occupies this leader slot. This is a slot
	// battle, not a reason to treat the slot as spent: the reference
	// implementation forges an alternative here -- same block number as the
	// rival, the rival's predecessor as parent -- and lets the leader VRF
	// and chain selection arbitrate (mkCurrentBlockContext's EQ branch).
	//
	// altBlockContext is nil for every uncontested slot, which leaves the
	// build and adoption below on their original live-tip paths.
	var altBlockContext *BlockContext
	if currentSlot == tipSlot {
		if f.metrics != nil {
			f.metrics.slotBattlesTotal.Inc()
		}
		blockCtx, ok := f.alternativeBlockContext(currentSlot)
		if !ok {
			// Nothing to build the alternative on. Record the battle
			// being declined rather than dropping the slot silently.
			f.incCouldNotForge()
			f.logger.Warn(
				"forge skip: leader slot already holds another block and no alternative can be built",
				"current_slot", currentSlot,
				"tip_slot", tipSlot,
			)
			return nil
		}
		altBlockContext = &blockCtx
		f.logger.Info(
			"leader slot already holds another block; forging an alternative",
			"current_slot", currentSlot,
			"rival_hash", hex.EncodeToString(blockCtx.Rival.Point.Hash),
			"fork_point_slot", blockCtx.Parent.Slot,
			"block_number", blockCtx.BlockNumber,
		)
	}

	// Commit to this slot before any signing happens for it, including
	// the Leios endorser block below. The tip check above only rejects
	// slots the local chain already covers; it cannot see a slot whose
	// block was signed and diffused but never adopted, nor one that
	// survived only in a tip that has since rolled back.
	proceed, err := f.reserveForgeSlot(currentSlot)
	if err != nil {
		return err
	}
	if !proceed {
		return nil
	}

	var (
		leiosBlockData LeiosBlockData
		embeddedEb     *lcommon.Blake2b256
		embeddedEbSlot uint64
	)
	// An alternative is built on the contested block's predecessor, not on
	// the contested block, but ParentLeiosAnnouncement resolves "the parent"
	// from the live tip -- which here is the rival. A certificate selected
	// from that answer would certify an endorser block announced by a ranking
	// block this one is not built on. Forge the alternative as a plain
	// ranking block: no certificate, and no new announcement either, so a
	// contested slot cannot also introduce an endorser block whose announcing
	// chain may be the one that loses.
	forgeLeiosData := altBlockContext == nil
	if !forgeLeiosData {
		f.logger.Debug(
			"leios data omitted from an equal-slot alternative",
			"slot", currentSlot,
		)
	}
	var (
		leiosParentRb    lcommon.Blake2b256
		leiosParentKnown bool
	)
	if forgeLeiosData {
		// The parent announcement every piece of Leios data below is
		// resolved against, captured before resolution so it can be
		// re-checked after; see the guard ahead of the build. Only read on
		// the live-tip path: an alternative carries no Leios data, and the
		// provider answers for the tip, which on that path is the rival.
		var leiosParentEb lcommon.Blake2b256
		leiosParentRb, leiosParentEb, leiosParentKnown = f.leiosParentAnnouncement(
			currentSlot,
		)
		leiosBlockData, embeddedEb, embeddedEbSlot = f.leiosBlockDataForSlot(
			currentSlot,
			leiosParentRb,
			leiosParentEb,
			leiosParentKnown,
		)
	}
	if forgeLeiosData && f.leiosChecker != nil {
		var excludedTxHashes map[string]struct{}
		canAnnounce := true
		if embeddedEb != nil {
			hashes, ok := f.leiosCerts.CertifiedEndorserBlockTxHashes(
				*embeddedEb,
				embeddedEbSlot,
			)
			if !ok {
				f.logger.Warn(
					"leios EB announcement skipped: certified closure unavailable for mempool rebase",
					"slot",
					currentSlot,
					"eb_hash",
					embeddedEb.String(),
				)
				canAnnounce = false
			}
			if canAnnounce {
				excludedTxHashes = make(map[string]struct{}, len(hashes))
				for _, hash := range hashes {
					excludedTxHashes[hash] = struct{}{}
				}
			}
		}
		if canAnnounce {
			announcement, err := f.checkAndForgeLeiosEB(
				currentSlot,
				excludedTxHashes,
			)
			if err != nil {
				f.logger.Warn(
					"leios endorser block production failed",
					"slot", currentSlot,
					"error", err,
				)
				if f.metrics != nil {
					f.metrics.leiosEbFailed.Inc()
				}
			} else if announcement != nil {
				leiosBlockData.Announcement = announcement
			}
		}
	}
	// Every piece of Leios data resolved above is parent-dependent. The
	// certificate is chosen for the endorser block the parent ranking block
	// announced, and an announcement is built against that same parent. The
	// builder does not inherit any of that: it binds the block's parent from
	// its own fresh chain-tip read. A tip that moved while the Leios work
	// above ran therefore yields a block whose certificate belongs to a
	// different parent's endorser-block lineage -- a block no peer accepts,
	// signed with this slot's credentials.
	//
	// Re-check the parent announcement and, if it moved, drop the Leios data
	// and the embedded-endorser-block bookkeeping that goes with it, then
	// forge a plain ranking block. Dropping is the fail-closed answer:
	// re-resolving here would have to re-run endorser-block production for
	// the new parent, and a second endorser block for one slot is not a thing
	// this loop may do. Clearing embeddedEb matters as much as clearing the
	// data -- left set, MarkEndorserBlockEmbedded would record an endorser
	// block as embedded in a block that does not carry it.
	if !leiosBlockData.empty() && leiosParentKnown {
		if rb, _, ok := f.leiosParentAnnouncement(currentSlot); !ok ||
			rb != leiosParentRb {
			f.logger.Warn(
				"leios data dropped: parent announcement changed while the block was being prepared",
				"slot", currentSlot,
				"resolved_parent_rb", leiosParentRb.String(),
			)
			leiosBlockData = LeiosBlockData{}
			// embeddedEb alone: embeddedEbSlot has already served its
			// only purpose (resolving the certified closure for the
			// mempool rebase above) and is not read again.
			embeddedEb = nil
		}
	}
	// Leios providers, mempool access, transaction validation, and broadcaster
	// callbacks are all pluggable. They run without credential locks; if one
	// reloads or revalidates credentials, abandon the ranking-block attempt
	// before evolving or consuming the selected snapshot.
	if err := generation.ensureCurrent(); err != nil {
		f.incCouldNotForge()
		f.logger.Warn(
			"forge skip: credentials changed during Leios processing",
			"slot", currentSlot,
			"selected_generation", generation.id,
			"error", err,
		)
		return nil
	}

	f.logger.Info("producing block", "slot", currentSlot)

	// Ensure KES key is at correct period
	if err := generation.updateKESPeriod(kesPeriod); err != nil {
		f.incCouldNotForge()
		return fmt.Errorf("failed to update KES period: %w", err)
	}

	// Build the block
	block, blockCbor, err := f.buildBlock(
		currentSlot,
		kesPeriod,
		leiosBlockData,
		generation,
		altBlockContext,
	)
	if err != nil {
		f.incCouldNotForge()
		return fmt.Errorf("failed to build block: %w", err)
	}
	// Key material is no longer needed after the block is signed. Zeroize the
	// independently owned snapshot before invoking pluggable validation,
	// adoption, or observer callbacks.
	generation.release()
	generationReleased = true

	// Optionally self-validate before adoption and diffusion.
	// Runs here — before success metrics and the blockForged observer — so
	// that forgeForged and RecordForgedBlock are never triggered for a block
	// that is ultimately dropped.
	if f.blockValidator != nil {
		validateStart := time.Now()
		blockHashStr := ""
		if block != nil {
			blockHashStr = hex.EncodeToString(block.Hash().Bytes())
		}
		validationErr := f.validateForgedBlockSafe(block, blockCbor)
		validationDuration := time.Since(validateStart)
		if f.metrics != nil {
			f.metrics.forgeValidationDuration.Observe(
				validationDuration.Seconds(),
			)
		}
		if validationErr != nil {
			f.incCouldNotForge()
			if f.metrics != nil {
				f.metrics.forgeValidationFailed.Inc()
			}
			f.logger.Error(
				"forged block failed self-validation, dropping block",
				"slot", currentSlot,
				"hash", blockHashStr,
				"validation_duration", validationDuration,
				"error", validationErr,
			)
			return fmt.Errorf(
				"forged block self-validation failed: %w",
				validationErr,
			)
		}
		f.logger.Info(
			"forged block passed self-validation",
			"slot", currentSlot,
			"hash", blockHashStr,
			"validation_duration", validationDuration,
		)
	}

	// Block forged successfully
	if f.metrics != nil {
		f.metrics.forgeForged.Inc()
		f.metrics.blockSizeBytes.Observe(
			float64(len(blockCbor)),
		)
		f.metrics.blockTxCount.Observe(
			float64(len(block.Transactions())),
		)
	}

	// Attempt local adoption immediately after building and validation. Keep
	// observability callbacks out of this critical path: subscribers may be
	// slow, while the block's parent must still be the active chain tip.
	//
	// An alternative does not extend the tip, so it goes to chain selection
	// instead: it is adopted only if it beats the block already there, by
	// the same comparison a peer's competing block goes through. Losing is
	// an outcome, not a failure -- the rival keeps the slot and this block
	// is never diffused, so the observer, mempool cleanup and adoption
	// counters below must not run for it.
	if altBlockContext != nil {
		adopted, addErr := f.adoptSiblingBlockSafe(block, blockCbor)
		if addErr != nil {
			f.incCouldNotForge()
			return fmt.Errorf(
				"failed to adopt alternative block: %w",
				addErr,
			)
		}
		if !adopted {
			f.logger.Info(
				"forged alternative lost the slot battle; the block at the tip stands",
				"slot", currentSlot,
				"hash", hex.EncodeToString(block.Hash().Bytes()),
				"rival_hash", hex.EncodeToString(
					altBlockContext.Rival.Point.Hash,
				),
			)
			return nil
		}
	} else if addErr := f.addBlockSafe(block, blockCbor); addErr != nil {
		f.incCouldNotForge()
		return fmt.Errorf("failed to add block: %w", addErr)
	}

	// Publish only after durable acceptance. The observer republishes the
	// block on the event bus and enqueues its Leios announcement for
	// diffusion, so running it for a rejected block would advertise a
	// block this node never adopted. Build-versus-adopt stays observable
	// through forgeForged above and forgeCouldNot on the failure path.
	if f.blockForged != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					f.logger.Error(
						"blockForged observer panic",
						"panic", r,
						"stack", string(debug.Stack()),
					)
				}
			}()
			f.blockForged(block, blockCbor, time.Since(forgeStartTime))
		}()
	}

	// AddBlock accepted the block, so its transactions are confirmed. Remove
	// them synchronously instead of depending on an asynchronous full-pool
	// revalidation that can be invalidated by the next ledger generation.
	if f.confirmedTxs != nil {
		transactions := block.Transactions()
		hashes := make([]string, 0, len(transactions))
		for _, tx := range transactions {
			hashes = append(hashes, tx.Hash().String())
		}
		if len(hashes) > 0 {
			f.confirmedTxs.RemoveTxsByHash(hashes)
		}
	}

	// Block adopted onto chain
	if f.metrics != nil {
		f.metrics.forgeAdopted.Inc()
	}
	if embeddedEb != nil && f.leiosCerts != nil {
		f.leiosCerts.MarkEndorserBlockEmbedded(*embeddedEb)
	}

	// Record the forged block for slot battle detection
	f.slotTracker.RecordForgedBlock(
		currentSlot, block.Hash().Bytes(),
	)

	f.logger.Info("block produced successfully",
		"slot", currentSlot,
		"hash", hex.EncodeToString(block.Hash().Bytes()),
	)
	return nil
}

// leiosParentAnnouncement resolves the parent ranking block and the endorser
// block it announced. Every parent-dependent Leios selection is made against
// this answer, and the pre-build guard re-reads it to detect a chain tip that
// moved underneath that selection.
func (f *BlockForger) leiosParentAnnouncement(slot uint64) (
	parentRbHash lcommon.Blake2b256,
	parentHash lcommon.Blake2b256,
	ok bool,
) {
	if f.leiosParent == nil {
		return lcommon.Blake2b256{}, lcommon.Blake2b256{}, false
	}
	parentRbHash, parentHash, ok, err := f.leiosParent.ParentLeiosAnnouncement()
	if err != nil {
		f.logger.Warn(
			"leios endorser block certificate skipped: parent announcement unavailable",
			"slot",
			slot,
			"error",
			err,
		)
		return lcommon.Blake2b256{}, lcommon.Blake2b256{}, false
	}
	if !ok {
		f.logger.Debug(
			"leios endorser block certificate skipped: parent has no announcement",
			"slot",
			slot,
		)
		return lcommon.Blake2b256{}, lcommon.Blake2b256{}, false
	}
	return parentRbHash, parentHash, true
}

func (f *BlockForger) leiosBlockDataForSlot(
	slot uint64,
	parentRbHash lcommon.Blake2b256,
	parentHash lcommon.Blake2b256,
	parentKnown bool,
) (LeiosBlockData, *lcommon.Blake2b256, uint64) {
	if f.leiosCerts == nil || !parentKnown {
		return LeiosBlockData{}, nil, 0
	}
	eligible := f.leiosCerts.EligibleCertifiedEndorserBlocks()
	for _, eb := range eligible {
		if eb.Certificate == nil {
			continue
		}
		if eb.EndorserBlockHash != parentHash ||
			eb.Certificate.EndorserBlockHash != parentHash ||
			eb.AnnouncingRbHash != parentRbHash {
			continue
		}
		hash := eb.EndorserBlockHash
		f.logger.Info(
			"leios endorser block certificate selected for ranking block",
			"slot", slot,
			"eb_slot", eb.SlotNo,
			"eb_hash", eb.EndorserBlockHash.String(),
		)
		return LeiosBlockData{Certificate: eb.Certificate}, &hash, eb.SlotNo
	}
	return LeiosBlockData{}, nil, 0
}

func (f *BlockForger) buildBlock(
	slot uint64,
	kesPeriod uint64,
	leiosData LeiosBlockData,
	generation *credentialGeneration,
	blockCtx *BlockContext,
) (ledger.Block, []byte, error) {
	var (
		block     ledger.Block
		blockCbor []byte
		err       error
	)
	if generationBuilder, ok := f.blockBuilder.(credentialGenerationBlockBuilder); ok {
		block, blockCbor, err = generationBuilder.buildBlockWithCredentialGeneration(
			slot,
			kesPeriod,
			leiosData,
			generation,
			blockCtx,
		)
	} else if blockCtx != nil {
		// Only reachable for a custom builder, and only when the forger has
		// already confirmed it implements AlternativeBlockBuilder before
		// committing to the contested slot.
		altBuilder, ok := f.blockBuilder.(AlternativeBlockBuilder)
		if !ok {
			return nil, nil, errors.New(
				"an explicit block context requires an AlternativeBlockBuilder",
			)
		}
		block, blockCbor, err = altBuilder.BuildBlockOnContext(
			slot,
			kesPeriod,
			leiosData,
			*blockCtx,
		)
	} else if leiosData.empty() {
		block, blockCbor, err = f.blockBuilder.BuildBlock(slot, kesPeriod)
	} else {
		leiosBuilder, ok := f.blockBuilder.(LeiosBlockBuilder)
		if !ok {
			return nil, nil, errors.New(
				"leios block data requires a LeiosBlockBuilder",
			)
		}
		block, blockCbor, err = leiosBuilder.BuildBlockWithLeios(
			slot,
			kesPeriod,
			leiosData,
		)
	}
	if err != nil {
		return nil, nil, err
	}
	// Custom builders cannot consume the package-private immutable snapshot.
	// Reject their output (and any default-builder output racing a callback
	// reload) if the selected owner generation changed while the callback ran.
	if err := generation.ensureCurrent(); err != nil {
		return nil, nil, err
	}
	return block, blockCbor, nil
}

// incCouldNotForge increments Forge_could_not_forge. Safe to call
// when metrics are nil.
// reserveForgeSlot enforces the duplicate-slot fence for slot and, when
// the slot is usable, records it durably before the caller signs
// anything for it.
//
// It reports whether forging may proceed. A slot at or below the fence is
// refused (false, nil): the node has already committed to that slot, and
// a second block for it would equivocate against a first that may already
// have reached peers. A fence that cannot be persisted fails the forge
// (false, err) rather than signing unprotected.
func (f *BlockForger) reserveForgeSlot(slot uint64) (bool, error) {
	if f.fenceLoaded && slot <= f.lastForgedSlot {
		if f.metrics != nil {
			f.metrics.forgeFenceBlocked.Inc()
		}
		f.logger.Warn(
			"forge skip: slot at or below last-forged-slot fence",
			"current_slot", slot,
			"last_forged_slot", f.lastForgedSlot,
		)
		return false, nil
	}
	if f.fenceStore != nil {
		if err := f.fenceStore.StoreLastForgedSlot(slot); err != nil {
			f.incCouldNotForge()
			return false, fmt.Errorf(
				"failed to persist forge fence for slot %d: %w",
				slot,
				err,
			)
		}
	}
	f.lastForgedSlot = slot
	f.fenceLoaded = true
	return true, nil
}

// tipOwnership is the outcome of comparing the block sitting at the
// chain tip against the block this node forged for a given slot.
type tipOwnership int

const (
	// tipOwnershipUnknown means the comparison could not be made: no
	// recorded hash for the slot, a slot clock that cannot report a tip
	// hash, an empty hash on either side, or a tip that moved between
	// the two reads. The caller must fall back to the fence.
	tipOwnershipUnknown tipOwnership = iota
	// tipOwnershipOurs means the tip block is byte-for-byte the block
	// this node forged for the slot.
	tipOwnershipOurs
	// tipOwnershipRival means the tip holds a different block for a slot
	// this node forged for: a slot battle this node lost.
	tipOwnershipRival
)

// tipBlockOwnership reports whether the block at the chain tip for slot
// is the one this node forged, a rival's, or indeterminate, together
// with the two hashes it compared (nil when indeterminate).
//
// This is identity rather than bookkeeping. SlotTracker already records
// the hash of every block this node forged and adopted, so comparing it
// against the hash at the tip distinguishes "we re-entered our own slot"
// from "we lost this slot to a rival". The forge fence cannot make that
// distinction: it is a high-water mark over slots this node committed
// to, so it reports both cases identically, and it is durable only
// through a ForgeFenceStore, so where none is wired (see fenceStore
// above) it can be silent about a slot this node demonstrably forged.
//
// The fence is not replaced by this, and the caller must keep consulting
// it. It covers the window the tracker cannot: reserveForgeSlot writes
// the fence *before* the header for a slot is signed, while
// RecordForgedBlock runs only after adoption, so between those two
// points the slot is already ours but has no recorded hash yet. More
// importantly, a fence that covers the slot forbids forging even when
// this function answers tipOwnershipRival — losing a slot battle is not
// a licence to sign a second block for a slot whose first block may
// already have reached peers.
//
// Neither signal survives a process restart on its own: the tracker is
// in-memory, and the fence is durable only through a store.
//
// The result can only choose between skipping quietly and skipping
// loudly; no branch of the equal-slot gate forges, so a wrong answer
// here cannot produce a block.
func (f *BlockForger) tipBlockOwnership(
	slot uint64,
) (tipOwnership, []byte, []byte) {
	if f.slotTracker == nil {
		return tipOwnershipUnknown, nil, nil
	}
	forgedHash, ok := f.slotTracker.WasForgedByUs(slot)
	if !ok || len(forgedHash) == 0 {
		return tipOwnershipUnknown, nil, nil
	}
	provider, hasTipHash := f.slotClock.(ChainTipHashProvider)
	if !hasTipHash {
		return tipOwnershipUnknown, nil, nil
	}
	tipHash := provider.ChainTipHash()
	if len(tipHash) == 0 {
		return tipOwnershipUnknown, nil, nil
	}
	// The caller's tipSlot was sampled at the top of the forge cycle and
	// the chain can move underneath it, so this hash need not belong to
	// the slot being decided. Re-read the tip slot next to the hash and
	// refuse to conclude anything if the tip is no longer at this slot.
	// The two reads are still not atomic, so this narrows the window
	// rather than closing it; every remaining disagreement resolves to
	// tipOwnershipUnknown or to a Warn, never to a forge.
	if f.slotClock.ChainTipSlot() != slot {
		return tipOwnershipUnknown, nil, nil
	}
	if bytes.Equal(tipHash, forgedHash) {
		return tipOwnershipOurs, forgedHash, tipHash
	}
	return tipOwnershipRival, forgedHash, tipHash
}

func (f *BlockForger) incCouldNotForge() {
	if f.metrics != nil {
		f.metrics.forgeCouldNot.Inc()
	}
}

// checkOpCertSequence resolves the era in effect for slot and validates the
// generation's OpCert counter against the ledger's on-chain observed value
// using that era's rule (validateOpCertSequence). Called only when
// f.opCertLedgerView is set; NewBlockForger guarantees f.eraParams is then
// also non-nil.
func (f *BlockForger) checkOpCertSequence(
	slot uint64,
	generation *credentialGeneration,
) error {
	opCert := generation.opCert()
	if opCert == nil {
		return errors.New("operational certificate not loaded")
	}
	credsPoolID := f.creds.GetPoolID()
	var poolID [28]byte
	copy(poolID[:], credsPoolID[:])
	stored, found, err := f.opCertLedgerView.LatestOpCertSequence(poolID)
	if err != nil {
		return fmt.Errorf("opcert sequence lookup: %w", err)
	}
	pparams := f.eraParams.ProtocolParamsForSlot(slot)
	if pparams == nil {
		return fmt.Errorf(
			"protocol parameters unavailable for slot %d",
			slot,
		)
	}
	// extractPParamsLimits also rejects a typed-nil pointer of a known
	// era's type stored in this interface, which the plain nil check above
	// cannot see (see its doc comment in eras.go).
	limits, err := extractPParamsLimits(pparams)
	if err != nil {
		return fmt.Errorf("resolve era for opcert counter rule: %w", err)
	}
	return validateOpCertSequence(
		stored,
		found,
		opCert.IssueNumber,
		!limits.era.isTPraos(),
	)
}

// logGateSkip logs a slot dropped by a gate that runs before leader
// selection. Such skips are routine and stay at Debug, but one that
// swallows a slot this node was scheduled to lead is a lost block, and
// nothing downstream will ever mention that slot again, so it is raised
// to Warn.
func (f *BlockForger) logGateSkip(
	slot uint64,
	msg string,
	attrs ...any,
) {
	if f.isScheduledLeaderSlot(slot) {
		f.logger.Warn(msg, append(attrs, "leader_slot", true)...)
		return
	}
	f.logger.Debug(msg, attrs...)
}

// isScheduledLeaderSlot reports whether slot is one this node is
// scheduled to lead, by consulting the precomputed VRF leader schedule
// rather than running leader selection. Election.NextLeaderSlot is a
// read-locked scan of the cached schedule for the slot's epoch, so it
// is cheap enough to call on a skip path.
//
// It only decides a log level, so it fails quiet: a checker with no
// cached schedule for that epoch reports false and the skip stays at
// Debug. A panic in the pluggable checker is recovered for the same
// reason checkLeaderSafe recovers one — it must not take down the
// producer-loop goroutine.
func (f *BlockForger) isScheduledLeaderSlot(slot uint64) (scheduled bool) {
	defer func() {
		if r := recover(); r != nil {
			scheduled = false
			f.reportForgeCallbackPanic("schedule", r)
		}
	}()
	if f.leaderChecker == nil {
		return false
	}
	next, ok := f.leaderChecker.NextLeaderSlot(slot)
	return ok && next == slot
}

// checkLeaderSafe calls the pluggable LeaderChecker, recovering any
// panic so a misbehaving implementation cannot terminate the forger's
// producer-loop goroutine. A recovered panic is treated as "not
// leader" for this slot, the same conservative outcome as a checker
// that simply returns false.
func (f *BlockForger) checkLeaderSafe(slot uint64) (isLeader bool) {
	defer func() {
		if r := recover(); r != nil {
			isLeader = false
			f.reportForgeCallbackPanic("selection", r)
		}
	}()
	return f.leaderChecker.ShouldProduceBlock(slot)
}

// validateForgedBlockSafe calls the pluggable BlockValidator,
// recovering any panic so a misbehaving implementation cannot
// terminate the forger's producer-loop goroutine. A recovered panic
// is treated as a validation failure so the block is dropped rather
// than adopted with unknown validity.
func (f *BlockForger) validateForgedBlockSafe(
	block ledger.Block,
	blockCbor []byte,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("block validator panic: %v", r)
			f.reportForgeCallbackPanic("validation", r)
		}
	}()
	return f.blockValidator.ValidateForgedBlock(block, blockCbor)
}

// alternativeBlockContext resolves the context for forging an alternative to
// the block already at the tip: the tip's block number, and the tip's
// predecessor as parent. This is what ouroboros-consensus'
// mkCurrentBlockContext returns for EQ.
//
// It reports false -- and the caller declines the contested slot -- whenever
// any part of the capability is missing: no durable forge fence, an unwired
// chain context or sibling adopter, a BlockBuilder that cannot take an
// explicit context, a tip with no resolvable predecessor, a tip that is no
// longer at the contested slot, or a predecessor that does not sit strictly
// below it. Declining costs one block; guessing a parent here costs a
// signature over a block no peer will accept.
func (f *BlockForger) alternativeBlockContext(
	slot uint64,
) (BlockContext, bool) {
	// A durable fence is a precondition for contesting a slot at all,
	// because this path cannot tell a rival's block from our own.
	//
	// The caller's "slot already has our own block" gate reads fenceLoaded,
	// and lastForgedSlot is in-memory only when no store is wired. Restart a
	// producer inside a slot it has already forged for and that gate cannot
	// fire: the tip is our own block, the slot equals the tip's, and this
	// function would hand back a context naming our own block as the rival.
	// Forging that alternative signs a second, different block for a slot
	// whose first block may already have reached peers -- equivocation, and
	// the adoption would roll our own good block off the tip to do it.
	//
	// A wired store makes the situation unreachable: the fence is loaded at
	// construction, so the gate refuses the slot before this is called and
	// reserveForgeSlot would refuse it again. Conceding a genuinely
	// contested slot is one block; equivocating is a slashable-class
	// protocol violation, so decline rather than guess.
	if f.fenceStore == nil {
		return BlockContext{}, false
	}
	if f.chainContext == nil || f.siblingAdopter == nil {
		return BlockContext{}, false
	}
	if _, ok := f.blockBuilder.(AlternativeBlockBuilder); !ok {
		return BlockContext{}, false
	}
	parent, tip, ok := f.chainContext.TipPredecessor()
	if !ok {
		return BlockContext{}, false
	}
	// Read from the chain rather than trusting the slot-clock snapshot the
	// gate above used: the tip must still be a block at the contested slot,
	// and its parent must sit strictly below that slot.
	if tip.Point.Slot != slot || parent.Slot >= slot {
		return BlockContext{}, false
	}
	return BlockContext{
		Parent:      parent,
		BlockNumber: tip.BlockNumber,
		Rival:       tip,
	}, true
}

// adoptSiblingBlockSafe offers a forged alternative to chain selection,
// recovering any panic from the pluggable adopter for the same reason
// addBlockSafe does. A recovered panic is treated as an adoption failure.
func (f *BlockForger) adoptSiblingBlockSafe(
	block ledger.Block,
	_ []byte,
) (adopted bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			adopted = false
			err = fmt.Errorf("sibling block adopter panic: %v", r)
			f.reportForgeCallbackPanic("publication", r)
		}
	}()
	return f.siblingAdopter.AdoptLocalForgedSibling(block)
}

// addBlockSafe calls the pluggable BlockBroadcaster, recovering any
// panic so a misbehaving implementation cannot terminate the forger's
// producer-loop goroutine. A recovered panic is treated as a publish
// failure, matching the existing error path for a broadcaster that
// returns an error.
func (f *BlockForger) addBlockSafe(
	block ledger.Block,
	blockCbor []byte,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("block broadcaster panic: %v", r)
			f.reportForgeCallbackPanic("publication", r)
		}
	}()
	return f.blockBroadcaster.AddBlock(block, blockCbor)
}

// reportForgeCallbackPanic logs and records metrics for a panic
// recovered from a pluggable forging callback. Safe to call when
// metrics are nil.
func (f *BlockForger) reportForgeCallbackPanic(phase string, r any) {
	if f.metrics != nil {
		f.metrics.forgePanicRecovered.WithLabelValues(phase).Inc()
	}
	f.logger.Error(
		"forge callback panic recovered",
		"phase", phase,
		"panic", r,
		"stack", string(debug.Stack()),
	)
}

// updateKESMetrics updates KES protocol-lifetime gauges for the current slot.
// Safe to call when metrics are nil.
func (f *BlockForger) updateKESMetrics(
	currentPeriod uint64,
	generation *credentialGeneration,
) {
	if f.metrics == nil {
		return
	}
	f.metrics.currentKESPeriod.Set(float64(currentPeriod))
	f.metrics.remainingKESPeriods.Set(
		float64(generation.periodsRemaining(currentPeriod)),
	)
	f.updateKESPolicyMetrics(generation)
}

func (f *BlockForger) updateKESPolicyMetrics(
	generation *credentialGeneration,
) {
	if f.metrics == nil {
		return
	}
	f.metrics.opCertStartKES.Set(float64(generation.opCertStartKES))
	f.metrics.opCertExpiryKES.Set(float64(generation.opCertExpiryKES))
}

// RecordSlotBattle increments the slot battles counter. This is
// called from external components (e.g., LedgerState) when a slot
// battle is detected.
func (f *BlockForger) RecordSlotBattle() {
	if f.metrics != nil {
		f.metrics.slotBattlesTotal.Inc()
	}
}

// VRFProofForSlot generates a VRF proof for leader election at the given slot.
// Returns (proof, output, error).
func (f *BlockForger) VRFProofForSlot(
	slot uint64,
	epochNonce []byte,
) ([]byte, []byte, error) {
	if f.mode == ModeDev {
		// Dev mode: return dummy proof
		return make([]byte, vrf.ProofSize), make([]byte, vrf.OutputSize), nil
	}

	if f.creds == nil || !f.creds.IsLoaded() {
		return nil, nil, errors.New("credentials not loaded")
	}

	// Validate slot fits in int64 before conversion
	if slot > math.MaxInt64 {
		return nil, nil, fmt.Errorf("slot %d exceeds int64 max", slot)
	}

	// Create VRF input: MkInputVrf(slot, epochNonce)
	alpha, err := vrf.MkInputVrf(
		int64(slot),
		epochNonce,
	) // #nosec G115 -- validated above
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create VRF input: %w", err)
	}

	return f.creds.VRFProve(alpha)
}

// SignBlockHeader signs a block header with KES.
func (f *BlockForger) SignBlockHeader(
	kesPeriod uint64,
	headerBytes []byte,
) ([]byte, error) {
	if f.mode == ModeDev {
		// Dev mode: return dummy signature
		return make([]byte, 448), nil // KES signature size for depth 6
	}

	if f.creds == nil || !f.creds.IsLoaded() {
		return nil, errors.New("credentials not loaded")
	}

	generation := f.creds.acquireCredentialGeneration()
	defer generation.release()
	if err := generation.validateKESPeriod(kesPeriod); err != nil {
		return nil, fmt.Errorf(
			"cannot sign block header outside operational certificate lifetime: %w",
			err,
		)
	}
	if err := generation.updateKESPeriod(kesPeriod); err != nil {
		return nil, fmt.Errorf("failed to update KES period: %w", err)
	}
	return generation.kesSign(kesPeriod, headerBytes)
}

// SlotTracker returns the forger's slot tracker, which can be used
// by other components (e.g., chainsync) to detect slot battles.
func (f *BlockForger) SlotTracker() *SlotTracker {
	return f.slotTracker
}

// checkAndForgeLeiosEB attempts to produce and broadcast a Leios endorser
// block for the given slot. It is called by the slot leader before RB
// construction so the EB can begin diffusing while the RB is assembled.
func (f *BlockForger) checkAndForgeLeiosEB(
	slot uint64,
	excludedTxHashes map[string]struct{},
) (*LeiosEndorserBlockAnnouncement, error) {
	allowed, reason, err := f.leiosChecker.MayProduceEndorserBlock(slot)
	if err != nil {
		return nil, fmt.Errorf("leios produce check: %w", err)
	}
	if !allowed {
		f.logger.Debug(
			"leios EB skipped",
			"slot", slot,
			"reason", reason,
		)
		if f.metrics != nil {
			f.metrics.leiosEbSkipped.WithLabelValues(reason).Inc()
		}
		return nil, nil
	}

	allTxs := f.leiosMempool.Transactions()
	txs := allTxs
	if len(excludedTxHashes) > 0 {
		txs = make([]MempoolTransaction, 0, len(allTxs))
		for _, tx := range allTxs {
			if _, excluded := excludedTxHashes[tx.Hash]; excluded {
				continue
			}
			txs = append(txs, tx)
		}
	}
	if len(txs) == 0 {
		f.logger.Debug("leios EB skipped: mempool empty", "slot", slot)
		if f.metrics != nil {
			f.metrics.leiosEbSkipped.WithLabelValues("no_transactions").Inc()
		}
		return nil, nil
	}
	validatedTxs, err := selectValidLeiosTransactions(txs, f.leiosValidator)
	if err != nil {
		return nil, fmt.Errorf("validate leios EB transactions: %w", err)
	}
	txs = validatedTxs
	if len(txs) == 0 {
		f.logger.Debug("leios EB skipped: no valid transactions", "slot", slot)
		if f.metrics != nil {
			f.metrics.leiosEbSkipped.WithLabelValues("no_valid_transactions").
				Inc()
		}
		return nil, nil
	}

	ebCbor, ebHash, bodies, err := buildLeiosEB(txs)
	if err != nil {
		if errors.Is(err, errNoValidTxRefs) {
			f.logger.Debug("leios EB skipped: no valid tx refs", "slot", slot)
			if f.metrics != nil {
				f.metrics.leiosEbSkipped.WithLabelValues("no_valid_tx_refs").
					Inc()
			}
			return nil, nil
		}
		return nil, fmt.Errorf("build leios EB: %w", err)
	}

	// Pass the transaction bodies alongside the manifest so the endorser
	// block can be served to peers over leios-fetch (they request the bodies
	// after fetching the manifest).
	if err := f.leiosEBCaster.BroadcastEndorserBlock(
		slot,
		ebHash,
		ebCbor,
		bodies,
	); err != nil {
		return nil, fmt.Errorf("broadcast leios EB: %w", err)
	}

	f.logger.Info(
		"leios endorser block produced",
		"slot", slot,
		"hash", hex.EncodeToString(ebHash),
		"tx_refs", len(bodies),
	)
	if f.metrics != nil {
		f.metrics.leiosEbForged.Inc()
	}
	return &LeiosEndorserBlockAnnouncement{
		Hash: lcommon.NewBlake2b256(ebHash),
		Size: uint64(len(ebCbor)),
	}, nil
}

// selectValidLeiosTransactions re-validates an ordered mempool snapshot with
// the same UTxO overlay semantics used at admission. Parent outputs are exposed
// only after the parent passes, so rejecting a parent also rejects descendants
// that depend on it. LedgerState pins the whole pass to one publication through
// TxValidationSessionProvider.
func selectValidLeiosTransactions(
	txs []MempoolTransaction,
	validator TxValidator,
) ([]MempoolTransaction, error) {
	if validator == nil {
		return txs, nil
	}
	selected := make([]MempoolTransaction, 0, len(txs))
	err := withTxValidationSession(
		validator,
		func(
			validate TxValidationFunc,
			stillCurrent func() bool,
		) error {
			consumed := make(map[string]struct{})
			created := make(map[string]lcommon.Utxo)
			for _, mempoolTx := range txs {
				// The EB wire reference is the transaction's only representation
				// in this slot. Do not expose outputs from a transaction that the
				// manifest builder will later drop as unrepresentable.
				if !validLeiosTransactionReference(mempoolTx) {
					continue
				}
				tx, err := decodeMempoolTx(mempoolTx)
				if err != nil || validate(tx, consumed, created) != nil {
					continue
				}
				selected = append(selected, mempoolTx)
				for _, input := range tx.Consumed() {
					key := fmt.Sprintf(
						"%s:%d",
						input.Id().String(),
						input.Index(),
					)
					consumed[key] = struct{}{}
				}
				for _, utxo := range tx.Produced() {
					key := fmt.Sprintf(
						"%s:%d", utxo.Id.Id().String(), utxo.Id.Index(),
					)
					created[key] = utxo
				}
			}
			if !stillCurrent() {
				return errTxValidationSnapshotChanged
			}
			return nil
		},
	)
	return selected, err
}

// buildLeiosEB assembles a LeiosEndorserBlock from mempool transactions.
// Transactions with invalid hex hashes, non-32-byte hashes, zero sizes,
// or sizes exceeding uint16 are silently dropped. Returns an error only
// when no valid references remain after filtering.
func buildLeiosEB(
	txs []MempoolTransaction,
) (
	cbor []byte,
	hash []byte,
	bodies [][]byte,
	err error,
) {
	refs := make([]lcommon.LeiosTransactionReference, 0, len(txs))
	// bodies holds each referenced transaction's raw CBOR, in the same order
	// as refs, so the endorser block can serve them over leios-fetch. A
	// transaction dropped from refs (bad hash or size) is dropped here too,
	// keeping body i aligned with reference i.
	bodies = make([][]byte, 0, len(txs))
	for _, tx := range txs {
		// The manifest reference is content-addressed by (hash, size) over
		// the FULL serialized transaction: TransactionSize is len(tx.Cbor),
		// so TransactionHash must be the hash of that same full CBOR, not the
		// Cardano tx-id / body hash. This matches the fetch-side validator
		// (validateLeiosEndorserBlockTxs) and Haskell reference nodes, so a
		// peer fetching a locally forged EB validates it instead of rejecting
		// every tx (blinklabs-io/dingo#3641).
		if !validLeiosTransactionHash(tx.Hash) ||
			len(tx.Cbor) == 0 || len(tx.Cbor) > math.MaxUint16 {
			continue
		}
		// Bounded above by the MaxUint16 check on len(tx.Cbor) above. Kept
		// on one line so the directive stays attached to the conversion.
		size := uint16(len(tx.Cbor)) // #nosec G115
		refs = append(refs, lcommon.LeiosTransactionReference{
			TransactionHash: lcommon.Blake2b256Hash(tx.Cbor),
			TransactionSize: size,
		})
		bodies = append(bodies, tx.Cbor)
	}
	if len(refs) == 0 {
		return nil, nil, nil, errNoValidTxRefs
	}
	eb := lcommon.LeiosEndorserBlock{TransactionReferences: refs}
	ebCbor, marshalErr := eb.MarshalCBOR()
	if marshalErr != nil {
		return nil, nil, nil, fmt.Errorf("marshal leios EB: %w", marshalErr)
	}
	h := lcommon.Blake2b256Hash(ebCbor)
	return ebCbor, h.Bytes(), bodies, nil
}

func validLeiosTransactionHash(hash string) bool {
	raw, err := hex.DecodeString(hash)
	return err == nil && len(raw) == 32
}

func validLeiosTransactionReference(tx MempoolTransaction) bool {
	return validLeiosTransactionHash(tx.Hash) && len(tx.Cbor) > 0 &&
		len(tx.Cbor) <= math.MaxUint16
}

// modeString returns a string representation of the forging mode.
func (f *BlockForger) modeString() string {
	switch f.mode {
	case ModeDev:
		return "dev"
	case ModeProduction:
		return "production"
	default:
		return "unknown"
	}
}
