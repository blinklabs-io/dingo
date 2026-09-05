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

package dingo

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/leiosheader"
	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	"github.com/blinklabs-io/dingo/ledger/leader"
	"github.com/blinklabs-io/dingo/ledger/leios"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/gouroboros/consensus"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func (n *Node) validateBlockProducerStartup() (*forging.PoolCredentials, error) {
	if _, err := n.blockProducerShelleyGenesis(); err != nil {
		return nil, err
	}
	if n.ledgerState == nil {
		return nil, errors.New(
			"block producer mode requires ledger state for current slot",
		)
	}
	currentSlot, err := n.ledgerState.CurrentSlot()
	if err != nil {
		if !errors.Is(err, ledger.ErrBeforeGenesis) {
			return nil, fmt.Errorf("compute current slot: %w", err)
		}
		currentSlot = 0
	}
	return n.validateBlockProducerStartupAtSlot(currentSlot)
}

func (n *Node) validateBlockProducerStartupAtSlot(
	currentSlot uint64,
) (*forging.PoolCredentials, error) {
	creds := forging.NewPoolCredentials()
	if err := creds.LoadFromFiles(
		n.config.shelleyVRFKey,
		n.config.shelleyKESKey,
		n.config.shelleyOperationalCertificate,
	); err != nil {
		return nil, fmt.Errorf("load pool credentials: %w", err)
	}
	if err := creds.ValidateOpCert(); err != nil {
		return nil, fmt.Errorf("validate operational certificate: %w", err)
	}
	genesis, err := n.blockProducerShelleyGenesis()
	if err != nil {
		return nil, err
	}
	if err := creds.ValidateKESPeriod(genesis, currentSlot); err != nil {
		return nil, fmt.Errorf("validate KES period: %w", err)
	}
	currentPeriod, err := forging.CurrentKESPeriodFromGenesis(
		genesis,
		currentSlot,
	)
	if err != nil {
		return nil, fmt.Errorf("compute current KES period: %w", err)
	}
	opCert := creds.GetOpCert()
	if opCert == nil {
		return nil, errors.New("block producer operational certificate is nil")
	}
	n.config.logger.Info(
		"block producer credentials validated",
		"component", "node",
		"pool_id", creds.GetPoolID().String(),
		"current_slot", currentSlot,
		"current_kes_period", currentPeriod,
		"opcert_kes_period", opCert.KESPeriod,
		"opcert_counter", opCert.IssueNumber,
		"opcert_expiry_period", creds.OpCertExpiryPeriod(),
	)
	return creds, nil
}

func (n *Node) blockProducerShelleyGenesis() (*shelley.ShelleyGenesis, error) {
	// KES-period plausibility requires a Shelley genesis. Block producer
	// mode without one is unsafe — a node with no genesis cannot tell
	// whether the opcert is current — so refuse to start.
	if n.config.cardanoNodeConfig == nil {
		return nil, errors.New(
			"block producer mode requires Cardano node config with Shelley genesis",
		)
	}
	genesis := n.config.cardanoNodeConfig.ShelleyGenesis()
	if genesis == nil {
		return nil, errors.New(
			"block producer mode requires Shelley genesis information",
		)
	}
	return genesis, nil
}

// blockProducerLedgerView adapts ledger.LedgerState to
// forging.LedgerView. The interface lives in the forging package so the
// credential check can stay free of a ledger import; the concrete
// adapter belongs here in package dingo where both types are visible.
type blockProducerLedgerView struct {
	ls *ledger.LedgerState
}

func (v blockProducerLedgerView) PoolRegistrationVRFKeyHash(
	poolID [28]byte,
) ([32]byte, bool, error) {
	return v.ls.PoolRegistrationVRFKeyHash(poolID)
}

func (v blockProducerLedgerView) LatestOpCertSequence(
	poolID [28]byte,
) (uint64, bool, error) {
	return v.ls.LatestOpCertSequence(poolID)
}

// validateBlockProducerLedger runs the ledger-aware cross-check against
// the loaded credentials. Must be called after the ledger has started so
// pool registrations can be queried. A pool that is not yet registered
// is logged as a warning and the node is allowed to continue.
func (n *Node) validateBlockProducerLedger(
	creds *forging.PoolCredentials,
) error {
	view := blockProducerLedgerView{ls: n.ledgerState}
	return n.validateBlockProducerLedgerWithView(creds, view)
}

func (n *Node) validateBlockProducerLedgerWithView(
	creds *forging.PoolCredentials,
	view forging.LedgerView,
) error {
	if creds == nil {
		return errors.New("nil pool credentials")
	}
	// Startup deliberately checks only for a stale counter, not the
	// era-scoped no-gap rule the forge loop and block application enforce.
	// The era for "now" would have to come from LedgerState.CurrentSlot,
	// which is wall-clock and valid regardless of sync state; the baseline
	// comes from LatestOpCertSequence, which reflects only the applied
	// chain. On a node whose applied tip is behind wall-clock time (an
	// interrupted initial sync, a resume after downtime, a restore to an
	// older snapshot), those two can disagree: the era resolves to
	// whatever the wall clock says while the baseline is still the stale,
	// pre-catch-up counter, so a pool several rotations into its life
	// would look gapped and fail startup -- unable to then sync to the
	// point that would make the baseline correct. The forge loop's own
	// gate does not have this problem: it runs after the upstream-sync
	// skip and the leader check, so both its era and its baseline come
	// from near-tip state, and it fails closed per slot rather than
	// refusing to start the node at all.
	registered, vrfMatched, err := creds.ValidateAgainstLedger(view)
	if err != nil {
		if errors.Is(err, forging.ErrVRFKeyHashMismatch) &&
			n.config.network == "devnet" {
			n.config.logger.Warn(
				"devnet block producer VRF cross-check failed; node will continue",
				"component",
				"node",
				"pool_id",
				creds.GetPoolID().String(),
				"error",
				err,
			)
			return nil
		}
		return err
	}
	poolID := creds.GetPoolID().String()
	switch {
	case !registered:
		n.config.logger.Warn(
			"block producer pool not yet registered on chain; node will continue",
			"component",
			"node",
			"pool_id",
			poolID,
		)
	case vrfMatched:
		n.config.logger.Info(
			"block producer pool registration verified on chain",
			"component", "node",
			"pool_id", poolID,
		)
	default:
		n.config.logger.Warn(
			"block producer VRF cross-check skipped (seed-only VRF key)",
			"component", "node",
			"pool_id", poolID,
		)
	}
	return nil
}

// handleGenesisSnapshotError returns a fatal error for block producers (which
// require the genesis snapshot for leader election) and logs a warning for
// relay nodes (which do not perform leader election).
func (n *Node) handleGenesisSnapshotError(err error) error {
	return snapshot.HandleGenesisSnapshotError(
		n.config.blockProducer,
		n.config.logger,
		err,
	)
}

// initBlockForger initializes the block forger for production mode.
// This requires VRF, KES, and OpCert key files to be configured.
func (n *Node) initBlockForger(
	ctx context.Context,
	creds *forging.PoolCredentials,
) error {
	if creds == nil {
		return errors.New("nil pool credentials")
	}
	// Create mempool adapter for the forging package.
	mempoolAdapter := &forgingMempoolAdapter{source: n.mempool}

	// Create epoch nonce adapter for the builder
	epochNonceAdapter := &epochNonceAdapter{ledgerState: n.ledgerState}

	// Create block builder
	builder, err := forging.NewDefaultBlockBuilder(forging.BlockBuilderConfig{
		Logger:          n.config.logger,
		Mempool:         mempoolAdapter,
		PParamsProvider: n.ledgerState,
		ChainTip:        n.chainManager.PrimaryChain(),
		EpochNonce:      epochNonceAdapter,
		Credentials:     creds,
		TxValidator:     n.ledgerState,
	})
	if err != nil {
		return fmt.Errorf("failed to create block builder: %w", err)
	}

	// Create block broadcaster for synchronous local chain adoption.
	broadcaster := &blockBroadcaster{
		chain:  n.chainManager.PrimaryChain(),
		logger: n.config.logger,
	}

	// Create the leader election component
	// Convert pool ID from PoolId to PoolKeyHash (both are [28]byte)
	poolID := creds.GetPoolID()
	var poolKeyHash lcommon.PoolKeyHash
	copy(poolKeyHash[:], poolID[:])

	// Create adapters for the providers that leader.Election needs
	stakeProvider := &stakeDistributionAdapter{ledgerState: n.ledgerState}
	epochProvider := &epochInfoAdapter{ledgerState: n.ledgerState}

	// Get VRF secret key from credentials
	vrfSKey := creds.GetVRFSKey()

	// Create leader election with real stake distribution
	election := leader.NewElection(
		poolKeyHash,
		vrfSKey,
		stakeProvider,
		epochProvider,
		n.eventBus,
		n.config.logger,
	)
	election.SetPromRegistry(n.config.promRegistry)
	if n.db != nil {
		if scheduleStore := leader.NewSyncStateScheduleStore(
			n.db.Metadata(),
		); scheduleStore != nil {
			election.SetScheduleStore(scheduleStore)
		}
	}

	// Start leader election (subscribes to epoch transitions)
	if err := election.Start(ctx); err != nil {
		return fmt.Errorf("failed to start leader election: %w", err)
	}

	// Create slot clock adapter for the forger
	slotClock := &slotClockAdapter{ledgerState: n.ledgerState}

	// Wire Leios EB forging when the pipeline manager is available
	// (i.e. Dijkstra era is enabled). Relay nodes and pre-Dijkstra
	// block producers leave these nil and skip EB production.
	var leiosChecker forging.LeiosProduceChecker
	var leiosCerts forging.LeiosCertificateProvider
	var leiosParent forging.LeiosParentAnnouncementProvider
	var leiosEBCaster forging.EndorserBlockBroadcaster
	var leiosMempool forging.MempoolProvider
	if n.leiosPipelineManager != nil && n.ouroboros() != nil {
		adapter := &leiosPipelineAdapter{
			mgr:                   n.leiosPipelineManager,
			chain:                 n.chainManager.PrimaryChain(),
			endorserBlockTxHashes: n.ouroboros().EndorserBlockTxHashesByHash,
		}
		leiosChecker = adapter
		leiosCerts = adapter
		leiosParent = adapter
		leiosEBCaster = n.ouroboros()
		leiosMempool = mempoolAdapter
	}
	blockForged := n.ledgerState.RecordForgedBlock
	if n.ouroboros() != nil {
		blockForged = func(block gledger.Block, blockCbor []byte, latency time.Duration) {
			n.ledgerState.RecordForgedBlock(block, blockCbor, latency)
			if header, ok := block.Header().(*gdijkstra.DijkstraBlockHeader); ok {
				if _, _, announces := header.LeiosAnnouncement(); announces {
					n.ouroboros().EnqueueLeiosBlockAnnouncement(header.Cbor())
				}
			}
		}
	}

	// Wire the durable last-forged-slot fence. A block producer must not
	// start without it: the in-memory fallback cannot survive a restart,
	// which is precisely the case the fence exists for. The forging
	// package still tolerates a nil store for embedders and dev-mode
	// wiring, so refuse here rather than there.
	var forgeFence forging.ForgeFenceStore
	if n.db != nil {
		forgeFence = forging.NewSyncStateForgeFenceStore(
			n.db.Metadata(),
			poolKeyHash,
		)
	}
	if forgeFence == nil {
		_ = election.Stop()
		return errors.New(
			"block producer requires a metadata store for the forge fence",
		)
	}

	// Wire self-validation when the operator opts in. The validator runs
	// header crypto, body-hash, and per-tx ledger checks before AddBlock.
	var blockValidator forging.BlockValidator
	if n.config.validateForgedBlock {
		blockValidator = &forgedBlockValidatorAdapter{
			ledgerState: n.ledgerState,
		}
	}

	// Create the block forger with the real leader election
	forger, err := forging.NewBlockForger(forging.ForgerConfig{
		Mode:             forging.ModeProduction,
		Logger:           n.config.logger,
		Credentials:      creds,
		LeaderChecker:    election,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		ConfirmedTxs:     mempoolAdapter,
		BlockForged:      blockForged,
		SlotClock:        slotClock,
		// Equal-slot alternative forging. When a rival block already
		// occupies the slot this node leads, the forger builds an
		// alternative on the rival's predecessor and offers it to chain
		// selection instead of conceding the slot -- ouroboros-consensus
		// mkCurrentBlockContext's EQ case. The primary chain supplies the
		// fork context; LedgerState arbitrates with the same Praos
		// comparison a peer's competing block goes through.
		ChainContext:                    n.chainManager.PrimaryChain(),
		SiblingAdopter:                  n.ledgerState,
		ForgeSyncToleranceSlots:         n.config.forgeSyncToleranceSlots,
		ForgeStaleGapThresholdSlots:     n.config.forgeStaleGapThresholdSlots,
		BlockValidator:                  blockValidator,
		ForgeFence:                      forgeFence,
		PromRegistry:                    n.config.promRegistry,
		LeiosProduceChecker:             leiosChecker,
		LeiosEBBroadcaster:              leiosEBCaster,
		LeiosMempool:                    leiosMempool,
		LeiosTxValidator:                n.ledgerState,
		LeiosCertificateProvider:        leiosCerts,
		LeiosParentAnnouncementProvider: leiosParent,
		OpCertLedgerView: blockProducerLedgerView{
			ls: n.ledgerState,
		},
		EraParams: n.ledgerState,
	})
	if err != nil {
		// Stop election to prevent goroutine leak
		_ = election.Stop()
		return fmt.Errorf("failed to create block forger: %w", err)
	}

	// Start the forger with the passed context
	if err := forger.Start(ctx); err != nil {
		// Stop election to prevent goroutine leak
		_ = election.Stop()
		return fmt.Errorf("failed to start block forger: %w", err)
	}

	// Store election for cleanup during shutdown only after the forger is
	// fully created and running.
	n.leaderElection = election
	n.blockForger = forger
	n.config.logger.Info(
		"block forger started in production mode with leader election",
		"pool_id", poolID.String(),
	)

	return nil
}

type mempoolTxView struct {
	Hash string
	Cbor []byte
	Type uint
}

type mempoolTransactionSource interface {
	Transactions() []mempool.MempoolTransaction
	RemoveTxsByHash(hashes []string)
}

func mempoolTransactions(source mempoolTransactionSource) []mempoolTxView {
	txs := source.Transactions()
	result := make([]mempoolTxView, len(txs))
	for i, tx := range txs {
		result[i] = mempoolTxView{
			Hash: tx.Hash,
			Cbor: tx.Cbor,
			Type: tx.Type,
		}
	}
	return result
}

// ledgerMempoolAdapter adapts mempool.Mempool to ledger.MempoolProvider.
type ledgerMempoolAdapter struct {
	source mempoolTransactionSource
}

func (a *ledgerMempoolAdapter) Transactions() []ledger.PendingTransaction {
	txs := mempoolTransactions(a.source)
	result := make([]ledger.PendingTransaction, len(txs))
	for i, tx := range txs {
		result[i] = ledger.PendingTransaction{
			Hash: tx.Hash,
			Cbor: tx.Cbor,
			Type: tx.Type,
		}
	}
	return result
}

func (a *ledgerMempoolAdapter) RemoveTxsByHash(hashes []string) {
	a.source.RemoveTxsByHash(hashes)
}

// forgingMempoolAdapter adapts mempool.Mempool to forging.MempoolProvider.
type forgingMempoolAdapter struct {
	source mempoolTransactionSource
}

func (a *forgingMempoolAdapter) Transactions() []forging.MempoolTransaction {
	txs := mempoolTransactions(a.source)
	result := make([]forging.MempoolTransaction, len(txs))
	for i, tx := range txs {
		result[i] = forging.MempoolTransaction{
			Hash: tx.Hash,
			Cbor: tx.Cbor,
			Type: tx.Type,
		}
	}
	return result
}

func (a *forgingMempoolAdapter) RemoveTxsByHash(hashes []string) {
	a.source.RemoveTxsByHash(hashes)
}

// The equal-slot alternative path is wired from two existing components
// rather than from adapters, so these assertions are what keeps that wiring
// honest: chain.Chain answers the fork context the alternative is built on,
// and LedgerState arbitrates between the alternative and the block already at
// the tip. A signature change on either side fails the build here instead of
// silently reverting block producers to conceding contested slots, which is
// what a nil provider does.
var (
	_ forging.AlternativeChainContextProvider = (*chain.Chain)(nil)
	_ forging.SiblingBlockAdopter             = (*ledger.LedgerState)(nil)
)

// blockBroadcaster implements forging.BlockBroadcaster through synchronous
// local chain admission. Block proposals are requests, not notifications, so
// they must not wait in the asynchronous EventBus while the chain advances.
type blockBroadcaster struct {
	chain  *chain.Chain
	logger *slog.Logger
}

func (b *blockBroadcaster) AddBlock(
	block gledger.Block,
	_ []byte,
) error {
	if block == nil {
		return errors.New("proposed block is nil")
	}
	if b.chain == nil {
		return errors.New("chain unavailable")
	}
	if err := b.chain.AddLocalBlock(block); err != nil {
		return fmt.Errorf("chain rejected proposed block: %w", err)
	}

	b.logger.Info(
		"block proposal accepted by chain",
		"slot", block.SlotNumber(),
		"hash", block.Hash(),
		"block_number", block.BlockNumber(),
	)

	return nil
}

// stakeDistributionAdapter adapts ledger.LedgerState to leader.StakeDistributionProvider.
// It queries through LedgerView so leader election observes the same stake
// snapshot rotation semantics as other ledger queries.
type stakeDistributionAdapter struct {
	ledgerState *ledger.LedgerState
	// afterPoolStakeReadFn is a test-only hook for coordinating a concurrent
	// snapshot recapture after the transaction has read the numerator.
	afterPoolStakeReadFn func()
}

func (a *stakeDistributionAdapter) getStakeDistribution(
	epoch uint64,
) (_ *ledger.StakeDistribution, err error) {
	if a.ledgerState == nil {
		return nil, errors.New("ledger state unavailable")
	}
	db := a.ledgerState.Database()
	if db == nil {
		return nil, errors.New("database unavailable")
	}
	txn := db.MetadataTxn(false)
	if txn == nil {
		return nil, errors.New("metadata transaction unavailable")
	}
	defer func() {
		if rollbackErr := txn.Rollback(); rollbackErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf(
					"release stake distribution transaction: %w",
					rollbackErr,
				),
			)
		}
	}()
	return a.ledgerState.NewView(txn).GetStakeDistribution(epoch)
}

// GetPoolAndTotalActiveStake reads the sigma numerator and denominator for
// one pool inside a single metadata transaction.
//
// Two defects are fixed here, and both are load-bearing for consensus:
//
// dingo #3814 -- the denominator comes from LedgerView.GetTotalActiveStake,
// a txn-scoped wrapper over Metadata().GetTotalActiveStake, which is the
// same store accessor ledger/verify_header.go resolves the denominator
// through when it checks an incoming header's leader eligibility.
// Verification calls that store method directly rather than through a
// LedgerView, with the snapshotType it resolved for the header under check;
// the shared thing is the store accessor, not the call path. The previous
// implementation returned
// ledger.StakeDistribution.TotalStake, which LedgerView.GetStakeDistribution
// accumulates by summing the mark rows itself, while verification reads
// epoch_summary.total_active_stake. Those two agree only "by construction"
// -- one rotation transaction writes both from one calculation
// (ledger/pool_stake_distribution.go documents the exact conditions) -- so
// the equality is a property of the writer, not of the readers, and nothing
// stops the two paths from drifting. A node whose forge denominator differs
// from its verify denominator can forge a block it would itself reject, or
// decline a slot it is genuinely eligible for. Resolving both through one
// accessor removes the second derivation entirely.
//
// dingo #3815 -- both values are read through one db.MetadataTxn and one
// LedgerView. Opening a transaction per value let a snapshot re-capture land
// between them, yielding a sigma whose halves come from different writes.
func (a *stakeDistributionAdapter) GetPoolAndTotalActiveStake(
	epoch uint64,
	poolKeyHash []byte,
) (poolStake uint64, totalActiveStake uint64, err error) {
	if a.ledgerState == nil {
		return 0, 0, errors.New("ledger state unavailable")
	}
	db := a.ledgerState.Database()
	if db == nil {
		return 0, 0, errors.New("database unavailable")
	}
	txn := db.MetadataTxn(false)
	if txn == nil {
		return 0, 0, errors.New("metadata transaction unavailable")
	}
	defer func() {
		if rollbackErr := txn.Rollback(); rollbackErr != nil {
			err = errors.Join(
				err,
				fmt.Errorf(
					"release stake distribution transaction: %w",
					rollbackErr,
				),
			)
		}
	}()
	view := a.ledgerState.NewView(txn)
	poolKey := hex.EncodeToString(poolKeyHash)
	poolStake, err = view.GetPoolStake(epoch, poolKeyHash)
	if err != nil {
		return 0, 0, fmt.Errorf(
			"get pool stake for epoch %d pool %s: %w",
			epoch,
			poolKey,
			err,
		)
	}
	// Test-only synchronization seam. The transaction has already observed
	// the numerator, so a concurrent recapture can commit before the
	// denominator query without changing this transaction's snapshot.
	if a.afterPoolStakeReadFn != nil {
		a.afterPoolStakeReadFn()
	}
	totalActiveStake, err = view.GetTotalActiveStake(epoch)
	if err != nil {
		return 0, 0, fmt.Errorf(
			"get total active stake for epoch %d: %w",
			epoch,
			err,
		)
	}
	return poolStake, totalActiveStake, nil
}

// The forging adapter must resolve sigma through the atomic pair accessor.
// A drift back to two independent reads, or to summing the mark rows for the
// denominator, is the pair of defects dingo #3814 and #3815 describe; make it
// a compile error rather than a silent consensus divergence.
var _ leader.StakeDistributionProvider = (*stakeDistributionAdapter)(nil)

// epochInfoAdapter adapts ledger.LedgerState to leader.EpochInfoProvider.
type epochInfoAdapter struct {
	ledgerState *ledger.LedgerState
}

// computeSchedule discovers the exact-rational coefficient with a runtime type
// assertion and silently falls back to the float64 accessor when it fails,
// which yields a strictly larger leadership threshold than the reference node's
// (dingo #2798). Make that drift a compile error;
// TestEpochInfoAdapterProvidesExactActiveSlotCoeff covers the same pairing.
var _ leader.ActiveSlotCoeffRatProvider = (*epochInfoAdapter)(nil)

func (a *epochInfoAdapter) CurrentEpoch() uint64 {
	return a.ledgerState.CurrentEpoch()
}

func (a *epochInfoAdapter) EpochNonce(epoch uint64) []byte {
	return a.ledgerState.EpochNonce(epoch)
}

func (a *epochInfoAdapter) NextEpochNonceReadyEpoch() (uint64, bool) {
	return a.ledgerState.NextEpochNonceReadyEpoch()
}

func (a *epochInfoAdapter) EpochSlotRange(
	epoch uint64,
) (leader.EpochSlotRange, error) {
	info, err := a.ledgerState.EpochInfo(epoch)
	if err != nil {
		if !errors.Is(err, hardfork.ErrPastHorizon) {
			return leader.EpochSlotRange{}, err
		}
		// The nonce-stability cutoff can precede the HFC header horizon. Once
		// the next epoch's nonce is stable, leader election still needs that
		// immediate Praos epoch's slot range to precompute its schedule. All
		// Praos eras use the Shelley genesis epoch/slot dimensions.
		readyEpoch, ready := a.ledgerState.NextEpochNonceReadyEpoch()
		currentEpoch := a.ledgerState.CurrentEpoch()
		if !ready ||
			readyEpoch != epoch ||
			currentEpoch == ^uint64(0) ||
			epoch != currentEpoch+1 {
			return leader.EpochSlotRange{}, err
		}
		currentInfo, currentErr := a.ledgerState.EpochInfo(
			currentEpoch,
		)
		if currentErr != nil {
			return leader.EpochSlotRange{}, currentErr
		}
		slotCount := uint64(currentInfo.LengthInSlots)
		if slotCount == 0 ||
			currentInfo.StartSlot > ^uint64(0)-slotCount {
			return leader.EpochSlotRange{}, fmt.Errorf(
				"current epoch slot range is invalid: start=%d count=%d",
				currentInfo.StartSlot,
				slotCount,
			)
		}
		return leader.EpochSlotRange{
			StartSlot: currentInfo.StartSlot + slotCount,
			SlotCount: slotCount,
		}, nil
	}
	return leader.EpochSlotRange{
		StartSlot: info.StartSlot,
		SlotCount: uint64(info.LengthInSlots),
	}, nil
}

func (a *epochInfoAdapter) EpochForSlot(slot uint64) (uint64, error) {
	epoch, err := a.ledgerState.SlotToEpoch(slot)
	if err != nil {
		return 0, err
	}
	return epoch.EpochId, nil
}

func (a *epochInfoAdapter) ActiveSlotCoeff() float64 {
	return a.ledgerState.ActiveSlotCoeff()
}

// ActiveSlotCoeffRat satisfies leader.ActiveSlotCoeffRatProvider so the leader
// schedule derives its threshold from the exact Shelley genesis rational, the
// same value LedgerState.verifyLeaderEligibility uses, instead of a float64
// approximation of it.
func (a *epochInfoAdapter) ActiveSlotCoeffRat() *big.Rat {
	return a.ledgerState.ActiveSlotCoeffRat()
}

func (a *epochInfoAdapter) ConsensusModeForEpoch(
	epoch uint64,
) consensus.ConsensusMode {
	return a.ledgerState.ConsensusModeForEpoch(epoch)
}

// slotClockAdapter adapts ledger.LedgerState to forging.SlotClockProvider.
type slotClockAdapter struct {
	ledgerState *ledger.LedgerState
}

func (a *slotClockAdapter) CurrentSlot() (uint64, error) {
	return a.ledgerState.CurrentSlot()
}

func (a *slotClockAdapter) SlotsPerKESPeriod() uint64 {
	return a.ledgerState.SlotsPerKESPeriod()
}

func (a *slotClockAdapter) ChainTipSlot() uint64 {
	return a.ledgerState.ChainTipSlot()
}

// ChainTipHash satisfies forging.ChainTipHashProvider. It lets the
// forger tell its own block at the current slot from a rival's by hash
// rather than inferring it from the forge fence, which is in-memory only
// when no fence store is wired. Both this and ChainTipSlot read the same
// tip snapshot; a tip that moves between the two reads simply fails the
// hash match and falls back to the fence.
func (a *slotClockAdapter) ChainTipHash() []byte {
	return a.ledgerState.Tip().Point.Hash
}

// The forger type-asserts for this optional interface, so losing the
// method would silently fall back to the fence rather than fail to
// build.
var _ forging.ChainTipHashProvider = (*slotClockAdapter)(nil)

func (a *slotClockAdapter) NextSlotTime() (time.Time, error) {
	return a.ledgerState.NextSlotTime()
}

func (a *slotClockAdapter) UpstreamTipSlot() uint64 {
	return a.ledgerState.UpstreamTipSlot()
}

func (a *slotClockAdapter) UpstreamSyncStatus() (uint64, bool) {
	return a.ledgerState.UpstreamSyncStatus()
}

// leiosPipelineAdapter adapts leios.PipelineManager and the primary chain to
// the narrow Leios interfaces the forge loop expects.
type leiosPipelineAdapter struct {
	mgr                   *leios.PipelineManager
	chain                 leiosParentChain
	endorserBlockTxHashes func(ebHash []byte, ebSlot uint64) ([]string, bool)
}

type leiosParentChain interface {
	Tip() ochainsync.Tip
	BlockByPoint(ocommon.Point, *database.Txn) (models.Block, error)
}

func (a *leiosPipelineAdapter) MayProduceEndorserBlock(
	slot uint64,
) (bool, string, error) {
	dec, err := a.mgr.MayProduceEndorserBlock(slot)
	if err != nil {
		return false, "", err
	}
	return dec.Allowed, dec.Reason, nil
}

func (a *leiosPipelineAdapter) EligibleCertifiedEndorserBlocks() []forging.LeiosCertifiedEndorserBlock {
	eligible := a.mgr.EligibleCertifiedEbs()
	out := make([]forging.LeiosCertifiedEndorserBlock, 0, len(eligible))
	for _, eb := range eligible {
		out = append(out, forging.LeiosCertifiedEndorserBlock{
			SlotNo:            eb.SlotNo,
			EndorserBlockHash: eb.EndorserBlockHash,
			Certificate:       eb.Certificate,
			AnnouncingRbHash:  eb.AnnouncingRbHash,
		})
	}
	return out
}

func (a *leiosPipelineAdapter) CertifiedEndorserBlockTxHashes(
	ebHash lcommon.Blake2b256,
	ebSlot uint64,
) ([]string, bool) {
	if a.endorserBlockTxHashes == nil {
		return nil, false
	}
	return a.endorserBlockTxHashes(ebHash.Bytes(), ebSlot)
}

func (a *leiosPipelineAdapter) MarkEndorserBlockEmbedded(
	ebHash lcommon.Blake2b256,
) {
	a.mgr.MarkEmbedded(ebHash)
}

func (a *leiosPipelineAdapter) ParentLeiosAnnouncement() (
	lcommon.Blake2b256,
	lcommon.Blake2b256,
	bool,
	error,
) {
	if a.chain == nil {
		return lcommon.Blake2b256{}, lcommon.Blake2b256{}, false, errors.New(
			"chain unavailable",
		)
	}
	tip := a.chain.Tip()
	if len(tip.Point.Hash) == 0 {
		return lcommon.Blake2b256{}, lcommon.Blake2b256{}, false, nil
	}
	block, err := a.chain.BlockByPoint(tip.Point, nil)
	if err != nil {
		return lcommon.Blake2b256{}, lcommon.Blake2b256{}, false, fmt.Errorf(
			"resolve parent block: %w",
			err,
		)
	}
	decoded, err := block.Decode()
	if err != nil {
		return lcommon.Blake2b256{}, lcommon.Blake2b256{}, false, fmt.Errorf(
			"decode parent block: %w",
			err,
		)
	}
	hash, _, ok := leiosheader.ReferencedEndorserBlock(decoded.Header())
	rbHash := lcommon.NewBlake2b256(tip.Point.Hash)
	return rbHash, hash, ok, nil
}

// forgedBlockValidatorAdapter adapts ledger.LedgerState to
// forging.BlockValidator so the forger can self-validate blocks before
// adoption without importing the ledger package from within forging.
type forgedBlockValidatorAdapter struct {
	ledgerState *ledger.LedgerState
}

func (a *forgedBlockValidatorAdapter) ValidateForgedBlock(
	block gledger.Block,
	blockCbor []byte,
) error {
	return a.ledgerState.ValidateForgedBlock(block, blockCbor)
}

// epochNonceAdapter adapts ledger.LedgerState to forging.EpochNonceProvider.
type epochNonceAdapter struct {
	ledgerState *ledger.LedgerState
}

func (a *epochNonceAdapter) CurrentEpoch() uint64 {
	return a.ledgerState.CurrentEpoch()
}

func (a *epochNonceAdapter) EpochForSlot(slot uint64) (uint64, error) {
	epoch, err := a.ledgerState.SlotToEpoch(slot)
	if err != nil {
		return 0, err
	}
	return epoch.EpochId, nil
}

func (a *epochNonceAdapter) EpochNonce(epoch uint64) []byte {
	return a.ledgerState.EpochNonce(epoch)
}
