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
	blockForged      BlockForgedObserver
	slotClock        SlotClockProvider
	slotDuration     time.Duration

	// Slot battle detection
	slotTracker *SlotTracker

	// Optional Leios EB forging (nil = relay or pre-Dijkstra era)
	leiosChecker  LeiosProduceChecker
	leiosEBCaster EndorserBlockBroadcaster
	leiosMempool  MempoolProvider
	leiosCerts    LeiosCertificateProvider
	leiosParent   LeiosParentAnnouncementProvider

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

// BlockBroadcaster submits built blocks to the chain.
type BlockBroadcaster interface {
	// AddBlock adds a block to the local chain and propagates to peers.
	AddBlock(block ledger.Block, cbor []byte) error
}

// BlockForgedObserver observes blocks after they are successfully built,
// before chain adoption is attempted.
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
	MayProduceEndorserBlock(slot uint64) (allowed bool, reason string, err error)
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
	// UpstreamTipSlot returns the latest known tip slot from upstream peers.
	// Returns 0 if no upstream tip is known.
	UpstreamTipSlot() uint64
}

// ForgerConfig holds configuration for the block forger.
type ForgerConfig struct {
	Mode         Mode
	Logger       *slog.Logger
	SlotDuration time.Duration

	// Production mode configuration
	Credentials      *PoolCredentials
	LeaderChecker    LeaderChecker
	BlockBuilder     BlockBuilder
	BlockBroadcaster BlockBroadcaster
	BlockForged      BlockForgedObserver
	SlotClock        SlotClockProvider

	// LeiosProduceChecker enables EB forging when non-nil. Requires
	// LeiosEBBroadcaster and LeiosMempool to also be set.
	LeiosProduceChecker LeiosProduceChecker
	// LeiosEBBroadcaster propagates locally-forged EBs to peers.
	LeiosEBBroadcaster EndorserBlockBroadcaster
	// LeiosMempool provides transactions for EB building. May reuse the
	// same MempoolProvider as the RB builder.
	LeiosMempool MempoolProvider
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
		blockForged:      cfg.BlockForged,
		slotClock:        cfg.SlotClock,
		slotTracker:      NewSlotTracker(),
		leiosChecker:     cfg.LeiosProduceChecker,
		leiosEBCaster:    cfg.LeiosEBBroadcaster,
		leiosMempool:     cfg.LeiosMempool,
		leiosCerts:       cfg.LeiosCertificateProvider,
		leiosParent:      cfg.LeiosParentAnnouncementProvider,
		blockValidator:   cfg.BlockValidator,
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
			return nil, errors.New("production mode requires loaded credentials")
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
	}
	if cfg.LeiosProduceChecker != nil {
		if cfg.LeiosEBBroadcaster == nil {
			return nil, errors.New("LeiosProduceChecker requires LeiosEBBroadcaster")
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

	if cfg.PromRegistry != nil {
		f.metrics = initForgingMetrics(cfg.PromRegistry)
	}

	// Set static OpCert gauges immediately so SPO dashboards show
	// certificate info without waiting for the first forged block.
	// Dynamic gauges (currentKESPeriod, remainingKESPeriods) are
	// updated on every slot-win in updateKESMetrics().
	if f.metrics != nil && f.creds != nil {
		opCert := f.creds.GetOpCert()
		if opCert != nil {
			f.metrics.opCertStartKES.Set(
				float64(opCert.KESPeriod),
			)
			f.metrics.opCertExpiryKES.Set(
				float64(f.creds.OpCertExpiryPeriod()),
			)
		}
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
					f.updateKESMetrics(kesPeriod)
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

	// Skip if a block already exists at the current slot.
	// When tipSlot >= currentSlot, the chain already has a block at
	// this slot (possibly from a peer). Producing another would create
	// a competing block and fork the chain.
	// Count every slot check (matches cardano-node
	// Forge.about_to_lead)
	if f.metrics != nil {
		f.metrics.forgeAboutToLead.Inc()
		f.metrics.tipGapSlots.Set(0)
	}

	if currentSlot <= tipSlot {
		// Detect stale data: if the tip is far ahead of the slot clock,
		// the database likely contains chain data from a different genesis.
		// Use subtraction (safe here since tipSlot >= currentSlot from
		// the outer check) to avoid uint64 overflow on the addition.
		gap := tipSlot - currentSlot
		if f.metrics != nil {
			f.metrics.tipGapSlots.Set(float64(gap))
		}
		if gap > f.forgeStaleGapThresholdSlots {
			f.logger.Error(
				"chain tip is far ahead of slot clock; database may contain data from a different genesis",
				"current_slot", currentSlot,
				"tip_slot", tipSlot,
				"slot_gap", gap,
			)
		} else {
			f.logger.Debug(
				"forge skip: slot already has block",
				"current_slot", currentSlot,
				"tip_slot", tipSlot,
			)
		}
		return nil
	}

	// Skip if the chain is still syncing from a peer.
	// Compare against the upstream peer tip rather than the wall
	// clock. Forging while syncing creates blocks that conflict
	// with the peer's chain, causing persistent header mismatches
	// and resync loops.
	// See forgeSyncToleranceSlots for the tolerance rationale.
	upstreamTip := f.slotClock.UpstreamTipSlot()
	if upstreamTip > 0 &&
		upstreamTip > tipSlot &&
		upstreamTip-tipSlot > f.forgeSyncToleranceSlots {
		if f.metrics != nil {
			f.metrics.forgeSyncSkip.Inc()
			f.metrics.tipGapSlots.Set(
				float64(upstreamTip - tipSlot),
			)
		}
		f.logger.Debug(
			"chain syncing from peer, skipping forge",
			"current_slot", currentSlot,
			"tip_slot", tipSlot,
			"upstream_tip", upstreamTip,
		)
		return nil
	}

	// Check if we're the leader for this slot
	isLeader := f.leaderChecker.ShouldProduceBlock(currentSlot)
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

	// We are the slot leader
	if f.metrics != nil {
		f.metrics.forgeNodeIsLeader.Inc()
	}

	leiosBlockData, embeddedEb := f.leiosBlockDataForSlot(currentSlot)
	if f.leiosChecker != nil {
		var excludedTxHashes map[string]struct{}
		canAnnounce := true
		if embeddedEb != nil {
			hashes, ok := f.leiosCerts.CertifiedEndorserBlockTxHashes(*embeddedEb)
			if !ok {
				f.logger.Warn(
					"leios EB announcement skipped: certified closure unavailable for mempool rebase",
					"slot", currentSlot,
					"eb_hash", embeddedEb.String(),
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
			announcement, err := f.checkAndForgeLeiosEB(currentSlot, excludedTxHashes)
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

	f.logger.Info("producing block", "slot", currentSlot)

	// Calculate KES period for this slot
	// KES period = slot / slots_per_kes_period
	slotsPerKESPeriod := f.slotClock.SlotsPerKESPeriod()
	if slotsPerKESPeriod == 0 {
		return errors.New("slots per KES period is zero")
	}
	kesPeriod, err := CurrentKESPeriod(currentSlot, slotsPerKESPeriod)
	if err != nil {
		return err
	}

	// Ensure KES key is at correct period
	if err := f.creds.UpdateKESPeriod(kesPeriod); err != nil {
		return fmt.Errorf("failed to update KES period: %w", err)
	}

	// Update KES metrics after successful evolution
	f.updateKESMetrics(kesPeriod)

	// Build the block
	block, blockCbor, err := f.buildBlock(currentSlot, kesPeriod, leiosBlockData)
	if err != nil {
		f.incCouldNotForge()
		return fmt.Errorf("failed to build block: %w", err)
	}

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
		validationErr := f.blockValidator.ValidateForgedBlock(block, blockCbor)
		validationDuration := time.Since(validateStart)
		if f.metrics != nil {
			f.metrics.forgeValidationDuration.Observe(validationDuration.Seconds())
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
			return fmt.Errorf("forged block self-validation failed: %w", validationErr)
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

	// Add block to chain and broadcast
	if err := f.blockBroadcaster.AddBlock(
		block, blockCbor,
	); err != nil {
		f.incCouldNotForge()
		return fmt.Errorf("failed to add block: %w", err)
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

func (f *BlockForger) leiosBlockDataForSlot(
	slot uint64,
) (LeiosBlockData, *lcommon.Blake2b256) {
	if f.leiosCerts == nil {
		return LeiosBlockData{}, nil
	}
	parentRbHash, parentHash, ok, err := f.leiosParent.ParentLeiosAnnouncement()
	if err != nil {
		f.logger.Warn(
			"leios endorser block certificate skipped: parent announcement unavailable",
			"slot", slot,
			"error", err,
		)
		return LeiosBlockData{}, nil
	}
	if !ok {
		f.logger.Debug(
			"leios endorser block certificate skipped: parent has no announcement",
			"slot", slot,
		)
		return LeiosBlockData{}, nil
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
		return LeiosBlockData{Certificate: eb.Certificate}, &hash
	}
	return LeiosBlockData{}, nil
}

func (f *BlockForger) buildBlock(
	slot uint64,
	kesPeriod uint64,
	leiosData LeiosBlockData,
) (ledger.Block, []byte, error) {
	if leiosData.empty() {
		return f.blockBuilder.BuildBlock(slot, kesPeriod)
	}
	leiosBuilder, ok := f.blockBuilder.(LeiosBlockBuilder)
	if !ok {
		return nil, nil, errors.New(
			"leios block data requires a LeiosBlockBuilder",
		)
	}
	return leiosBuilder.BuildBlockWithLeios(slot, kesPeriod, leiosData)
}

// incCouldNotForge increments Forge_could_not_forge. Safe to call
// when metrics are nil.
func (f *BlockForger) incCouldNotForge() {
	if f.metrics != nil {
		f.metrics.forgeCouldNot.Inc()
	}
}

// updateKESMetrics updates KES gauges after a successful KES
// period update. Safe to call when metrics are nil.
func (f *BlockForger) updateKESMetrics(
	currentPeriod uint64,
) {
	if f.metrics == nil {
		return
	}
	f.metrics.currentKESPeriod.Set(float64(currentPeriod))
	f.metrics.remainingKESPeriods.Set(
		float64(f.creds.PeriodsRemaining(currentPeriod)),
	)
	opCert := f.creds.GetOpCert()
	if opCert != nil {
		f.metrics.opCertStartKES.Set(
			float64(opCert.KESPeriod),
		)
		f.metrics.opCertExpiryKES.Set(
			float64(f.creds.OpCertExpiryPeriod()),
		)
	}
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
	alpha, err := vrf.MkInputVrf(int64(slot), epochNonce) // #nosec G115 -- validated above
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

	return f.creds.KESSign(kesPeriod, headerBytes)
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
	txs := make([]MempoolTransaction, 0, len(allTxs))
	for _, tx := range allTxs {
		if _, excluded := excludedTxHashes[tx.Hash]; excluded {
			continue
		}
		txs = append(txs, tx)
	}
	if len(txs) == 0 {
		f.logger.Debug("leios EB skipped: mempool empty", "slot", slot)
		if f.metrics != nil {
			f.metrics.leiosEbSkipped.WithLabelValues("no_transactions").Inc()
		}
		return nil, nil
	}

	ebCbor, ebHash, bodies, err := buildLeiosEB(txs)
	if err != nil {
		if errors.Is(err, errNoValidTxRefs) {
			f.logger.Debug("leios EB skipped: no valid tx refs", "slot", slot)
			if f.metrics != nil {
				f.metrics.leiosEbSkipped.WithLabelValues("no_valid_tx_refs").Inc()
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

// buildLeiosEB assembles a LeiosEndorserBlock from mempool transactions.
// Transactions with invalid hex hashes, non-32-byte hashes, zero sizes,
// or sizes exceeding uint16 are silently dropped. Returns an error only
// when no valid references remain after filtering.
func buildLeiosEB(
	txs []MempoolTransaction,
) (cbor []byte, hash []byte, bodies [][]byte, err error) {
	refs := make([]lcommon.LeiosTransactionReference, 0, len(txs))
	// bodies holds each referenced transaction's raw CBOR, in the same order
	// as refs, so the endorser block can serve them over leios-fetch. A
	// transaction dropped from refs (bad hash or size) is dropped here too,
	// keeping body i aligned with reference i.
	bodies = make([][]byte, 0, len(txs))
	for _, tx := range txs {
		raw, hexErr := hex.DecodeString(tx.Hash)
		if hexErr != nil || len(raw) != 32 {
			continue
		}
		sz := len(tx.Cbor)
		if sz == 0 || sz > math.MaxUint16 {
			continue
		}
		refs = append(refs, lcommon.LeiosTransactionReference{
			TransactionHash: lcommon.NewBlake2b256(raw),
			TransactionSize: uint16(sz), // #nosec G115 -- bounded above
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
