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

package ledger

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	dingoversion "github.com/blinklabs-io/dingo/internal/version"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/consensus"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"
)

const (
	cleanupConsumedUtxosInterval = 5 * time.Minute
	batchSize                    = 50 // Number of blocks to process in a single DB transaction
	ledgerIntersectDenseCount    = 32
	ledgerAncestorSearchWindow   = 10_000
	firstBlockIndex              = 1
	mithrilLedgerSlotSyncKey     = "mithril_ledger_slot"
	mithrilLedgerHashSyncKey     = "mithril_ledger_hash"
)

type metadataDbReader interface {
	DB() *gorm.DB
}

type metadataReadDbReader interface {
	ReadDB() *gorm.DB
}

// DatabaseOperation represents an asynchronous database operation
type DatabaseOperation struct {
	// Operation function that performs the database work
	OpFunc func(db *database.Database) error
	// Channel to send the result back. Must be non-nil and buffered to avoid blocking.
	// If nil, the operation will be executed but the result will be discarded (fire and forget).
	ResultChan chan<- DatabaseResult
}

// DatabaseResult represents the result of a database operation
type DatabaseResult struct {
	Error error
}

// DatabaseWorkerPoolConfig holds configuration for the database worker pool
type DatabaseWorkerPoolConfig struct {
	WorkerPoolSize int
	TaskQueueSize  int
	Disabled       bool
}

// DefaultDatabaseWorkerPoolConfig returns the default configuration for the database worker pool
func DefaultDatabaseWorkerPoolConfig() DatabaseWorkerPoolConfig {
	return DatabaseWorkerPoolConfig{
		WorkerPoolSize: 5,
		TaskQueueSize:  50,
	}
}

// DatabaseWorkerPool manages a pool of workers for async database operations
type DatabaseWorkerPool struct {
	db          *database.Database
	taskQueue   chan DatabaseOperation
	workerWg    sync.WaitGroup // worker goroutine lifecycle
	operationWg sync.WaitGroup // accepted operations until result is delivered
	closed      atomic.Bool    // thread-safe without mutex in hot path
	mu          sync.Mutex
}

// NewDatabaseWorkerPool creates a new database worker pool
func NewDatabaseWorkerPool(
	db *database.Database,
	config DatabaseWorkerPoolConfig,
) *DatabaseWorkerPool {
	if config.WorkerPoolSize <= 0 {
		config.WorkerPoolSize = 5 // Default to 5 workers
	}
	if config.TaskQueueSize <= 0 {
		config.TaskQueueSize = 50 // Default queue size
	}

	taskQ := make(chan DatabaseOperation, config.TaskQueueSize)
	pool := &DatabaseWorkerPool{
		db:        db,
		taskQueue: taskQ,
		// closed is zero-valued (false) by default for atomic.Bool
	}

	// Start workers
	for i := 0; i < config.WorkerPoolSize; i++ {
		pool.workerWg.Add(1)
		go pool.worker()
	}

	return pool
}

// worker runs a single database worker
func (p *DatabaseWorkerPool) worker() {
	defer p.workerWg.Done()

	for op := range p.taskQueue {
		p.executeOperation(op)
	}
}

func (p *DatabaseWorkerPool) executeOperation(op DatabaseOperation) {
	defer p.operationWg.Done()

	result := DatabaseResult{}
	defer func() {
		if r := recover(); r != nil {
			result.Error = fmt.Errorf("panic: %v", r)
			slog.Error("worker panic during operation", "panic", r)
		}
		p.sendResult(op, result)
	}()
	result.Error = op.OpFunc(p.db)
}

// sendResult delivers result on op.ResultChan. It blocks until send succeeds so
// errors are not dropped when the channel is temporarily full (callers should
// use a buffered ResultChan, e.g. cap 1, as in SubmitAsyncDBOperation).
func (p *DatabaseWorkerPool) sendResult(op DatabaseOperation, result DatabaseResult) {
	if op.ResultChan == nil {
		return
	}
	op.ResultChan <- result
}

// Submit submits a database operation for async execution
func (p *DatabaseWorkerPool) Submit(op DatabaseOperation) {
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		p.sendResult(
			op,
			DatabaseResult{Error: errors.New("database worker pool is shut down")},
		)
		return
	}

	p.operationWg.Add(1)
	select {
	case p.taskQueue <- op:
		p.mu.Unlock()
		return
	default:
		p.operationWg.Done()
	}
	p.mu.Unlock()
	p.sendResult(
		op,
		DatabaseResult{Error: errors.New("database worker pool queue full")},
	)
}

// SubmitAsyncDBOperation submits a database operation for execution on the worker pool.
// This method blocks waiting for the result and must be called after Start() and before Close().
// If the worker pool is disabled, it falls back to synchronous execution.
func (ls *LedgerState) SubmitAsyncDBOperation(
	opFunc func(db *database.Database) error,
) error {
	if ls.dbWorkerPool == nil {
		// Fallback to synchronous execution when pool is disabled
		return opFunc(ls.db)
	}

	resultChan := make(chan DatabaseResult, 1)

	ls.dbWorkerPool.Submit(DatabaseOperation{
		OpFunc:     opFunc,
		ResultChan: resultChan,
	})

	// Wait for the result
	result := <-resultChan
	return result.Error
}

// SubmitAsyncDBTxn submits a database transaction operation for execution on the worker pool.
// This method blocks waiting for the result and must be called after Start() and before Close().
// If a partial commit occurs (blob committed but metadata failed), this method will attempt
// to trigger database recovery to restore consistency.
func (ls *LedgerState) SubmitAsyncDBTxn(
	opFunc func(txn *database.Txn) error,
	readWrite bool,
) error {
	err := ls.SubmitAsyncDBOperation(func(db *database.Database) error {
		txn := db.Transaction(readWrite)
		return txn.Do(opFunc)
	})
	// Check for partial commit and trigger recovery if needed.
	// Guard against recursive recovery: if we're already in recovery and another
	// PartialCommitError occurs, don't attempt recovery again to prevent unbounded recursion.
	var partialCommitErr database.PartialCommitError
	if err != nil && errors.As(err, &partialCommitErr) {
		ls.Lock()
		alreadyInRecovery := ls.inRecovery
		if !alreadyInRecovery {
			ls.inRecovery = true
		}
		ls.Unlock()

		if alreadyInRecovery {
			ls.config.Logger.Error(
				"partial commit detected during recovery, skipping nested recovery: " + err.Error(),
			)
			return err
		}

		defer func() {
			ls.Lock()
			ls.inRecovery = false
			ls.Unlock()
		}()

		ls.config.Logger.Error(
			"partial commit detected, attempting recovery: " + err.Error(),
		)
		// Attempt to recover from the partial commit state
		if recoveryErr := ls.RecoverCommitTimestampConflict(); recoveryErr != nil {
			ls.config.Logger.Error(
				"failed to recover from partial commit: " + recoveryErr.Error(),
			)
			// Return both errors joined to preserve error chain for errors.Is checks
			return errors.Join(err, recoveryErr)
		}
		ls.config.Logger.Info("successfully recovered from partial commit")
		// Return an error so callers know the operation failed and should retry.
		// Recovery restored consistency but did NOT complete the original transaction.
		// Wrap the underlying metadata error (not PartialCommitError) so callers
		// won't match errors.Is(err, types.ErrPartialCommit) and attempt recovery again.
		return fmt.Errorf(
			"transaction failed, recovered from partial commit: %w",
			partialCommitErr.MetadataErr,
		)
	}
	return err
}

// SubmitAsyncDBReadTxn submits a read-only database transaction operation for execution on the worker pool.
// This method blocks waiting for the result and must be called after Start() and before Close().
func (ls *LedgerState) SubmitAsyncDBReadTxn(
	opFunc func(txn *database.Txn) error,
) error {
	return ls.SubmitAsyncDBTxn(opFunc, false)
}

// Shutdown gracefully shuts down the worker pool
func (p *DatabaseWorkerPool) Shutdown() {
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		return
	}
	p.closed.Store(true)
	close(p.taskQueue)
	p.mu.Unlock()

	p.operationWg.Wait()
	p.workerWg.Wait()
}

type ChainsyncState string

const (
	InitChainsyncState     ChainsyncState = "init"
	RollbackChainsyncState ChainsyncState = "rollback"
	SyncingChainsyncState  ChainsyncState = "syncing"
)

// FatalErrorFunc is a callback invoked when a fatal error occurs that requires
// the node to shut down. The callback should trigger graceful shutdown.
type FatalErrorFunc func(err error)

// GetActiveConnectionFunc is a callback to retrieve the currently active
// chainsync connection ID for chain selection purposes.
type GetActiveConnectionFunc func() *ouroboros.ConnectionId

// ConnectionLiveFunc reports whether a connection is still registered with the
// connection manager. This allows the ledger to drop late chainsync events that
// arrive after teardown.
type ConnectionLiveFunc func(ouroboros.ConnectionId) bool

// ForgedBlockChecker is an interface for checking whether the local
// node recently forged a block for a given slot. This is used by
// chainsync to detect slot battles when an incoming block from a
// peer occupies the same slot as a locally forged block.
type ForgedBlockChecker interface {
	// WasForgedByUs returns the block hash and true if the local node
	// forged a block for the given slot, or nil and false otherwise.
	WasForgedByUs(slot uint64) (blockHash []byte, ok bool)
}

// SlotBattleRecorder records slot battle events for metrics.
type SlotBattleRecorder interface {
	// RecordSlotBattle increments the slot battle counter.
	RecordSlotBattle()
}

// ConnectionSwitchFunc is called when the active chainsync connection
// changes. Implementations should clear any per-connection state such as
// the header dedup cache so the new connection can re-deliver blocks.
type ConnectionSwitchFunc func()

// ClearSeenHeadersFromFunc clears the header dedup cache for slots
// beyond the given slot. This allows headers that were discarded
// (e.g. by clearQueuedHeaders) to be re-delivered on reconnection.
type ClearSeenHeadersFromFunc func(fromSlot uint64)

// PeerHeaderLookupFunc looks up a previously observed header for a peer
// connection, even if that header was suppressed before entering the ledger
// queue. It returns the recorded chainsync event, the header's prev-hash, and
// whether the header was found.
type PeerHeaderLookupFunc func(
	connId ouroboros.ConnectionId,
	hash []byte,
) (ChainsyncEvent, []byte, bool)

type LedgerStateConfig struct {
	PromRegistry                prometheus.Registerer
	Logger                      *slog.Logger
	Database                    *database.Database
	ChainManager                *chain.ChainManager
	EventBus                    *event.EventBus
	CardanoNodeConfig           *cardano.CardanoNodeConfig
	BlockfetchRequestRangeFunc  BlockfetchRequestRangeFunc
	PeersWithBlockFunc          PeersWithBlockFunc
	RecordBlockfetchLatencyFunc RecordBlockfetchLatencyFunc
	BlockfetchLatencyFunc       BlockfetchLatencyFunc
	BlockfetchLatencyMedianFunc BlockfetchLatencyMedianFunc
	GetActiveConnectionFunc     GetActiveConnectionFunc
	ConnectionLiveFunc          ConnectionLiveFunc
	ConnectionSwitchFunc        ConnectionSwitchFunc
	ClearSeenHeadersFromFunc    ClearSeenHeadersFromFunc
	PeerHeaderLookupFunc        PeerHeaderLookupFunc
	FatalErrorFunc              FatalErrorFunc
	ForgedBlockChecker          ForgedBlockChecker
	SlotBattleRecorder          SlotBattleRecorder
	EndorserBlockProvider       EndorserBlockProviderFunc
	// EndorserBlockFetcher actively fetches a referenced endorser block (its
	// manifest and all transaction bodies) by point and caches it, so the
	// EndorserBlockProvider can then supply it. Unlike the tip path, which waits
	// for the relay to diffuse an endorser block it is already pushing, this is
	// used during historical catch-up: the prototype relay serves any endorser
	// block by point on demand (MsgLeiosBlockRequest), so the node can backfill
	// the endorser-resident outputs of older ranking blocks instead of trusting
	// the chain and leaving the UTxO set incomplete. Nil disables backfill.
	EndorserBlockFetcher EndorserBlockFetcherFunc
	// EndorserBlockWaitSlots is the number of slots that block processing
	// waits at the chain tip for a Dijkstra ranking block's referenced
	// endorser block to finish fetching before applying it. It is sourced from
	// the Leios pipeline timing (CertifyByDeadlineSlots, not the shorter
	// DiffuseWindowSlots: by the time a ranking block references an endorser
	// block that block has already been certified, so the certify-by deadline
	// is the bound for when it is actually available to fetch) rather than a
	// hardcoded duration; the ledger converts it to wall-clock using the
	// Shelley slot length. Zero disables the wait.
	EndorserBlockWaitSlots uint64
	// LeiosTolerateEndorserConflicts enables endorser-block conflict resolution
	// for the Musashi prototype network, where successive endorser blocks carry
	// mutually-conflicting, never-confirmed mempool transactions. When set, an
	// endorser-block transaction whose inputs are already spent is skipped
	// (best-effort) rather than aborting, and an authoritative ranking-block
	// transaction that needs an input a speculative endorser-block transaction
	// already spent revokes that endorser-block transaction instead of wedging
	// the ledger with "UTxO already spent" (issue #2699). Off on every other
	// network, where endorser-block transactions are applied unconditionally.
	LeiosTolerateEndorserConflicts bool
	ValidateHistorical             bool
	EnableDijkstra                 bool
	StartInDijkstra                bool
	TrustedReplay                  bool
	ManualBlockProcessing          bool
	ForgeBlocks                    bool
	DatabaseWorkerPoolConfig       DatabaseWorkerPoolConfig
}

// EndorserBlockProviderFunc returns the slot and the complete set of standalone
// transaction CBORs of the Leios endorser block identified by ebHash, when it
// has been fetched and fully cached; ok is false otherwise. It is used to apply
// an endorser block's transactions to the ledger when the referencing Dijkstra
// ranking block is processed.
type EndorserBlockProviderFunc func(ebHash []byte) (ebSlot uint64, txs []cbor.RawMessage, ok bool)

// EndorserBlockFetcherFunc actively fetches the endorser block identified by
// (ebSlot, ebHash) over leios-fetch (manifest plus all transaction bodies) and
// caches it so a subsequent EndorserBlockProviderFunc call returns it. It
// returns an error when no fetch connection is available or the relay does not
// serve the block. The endorser block shares the slot of the ranking block that
// references it (they are co-produced), so ebSlot is the ranking block's slot.
type EndorserBlockFetcherFunc func(ebSlot uint64, ebHash []byte) error

// BlockfetchRequestRangeFunc describes a callback function used to start a blockfetch request for
// a range of blocks
type BlockfetchRequestRangeFunc func(ouroboros.ConnectionId, ocommon.Point, ocommon.Point) error

// PeersWithBlockFunc returns all tracked connection IDs — excluding
// origin — that have a recorded observed header at the given point.
// Used to locate shadow peers for parallel blockfetch dispatch.
type PeersWithBlockFunc func(
	origin ouroboros.ConnectionId,
	point ocommon.Point,
) []ouroboros.ConnectionId

// RecordBlockfetchLatencyFunc records a first-block latency sample
// for the given connection after a successful RequestRange response.
type RecordBlockfetchLatencyFunc func(ouroboros.ConnectionId, time.Duration)

// BlockfetchLatencyFunc returns the EWMA first-block latency for the
// given connection and whether any samples have been recorded. Used
// to gate shadow blockfetch dispatch on primary peer slowness.
type BlockfetchLatencyFunc func(ouroboros.ConnectionId) (time.Duration, bool)

// BlockfetchLatencyMedianFunc returns the median EWMA first-block
// latency across all tracked peers and the sample count contributing
// to it. Used to adapt the shadow blockfetch gate to the observed
// peer population (primary > 1.5× median triggers shadow dispatch).
type BlockfetchLatencyMedianFunc func() (time.Duration, int)

// PendingTransaction is the transaction view ledger block construction needs.
type PendingTransaction struct {
	Hash string
	Cbor []byte
	Type uint
}

// MempoolProvider provides pending transactions without exposing mempool DTOs.
type MempoolProvider interface {
	Transactions() []PendingTransaction
	// RemoveTxsByHash removes confirmed transactions without cascading to
	// chained descendants, which remain valid against the updated ledger.
	RemoveTxsByHash(hashes []string)
}
type rollbackRecord struct {
	point     ocommon.Point
	connKey   string
	timestamp time.Time
}

type forgedBlockCheckerHolder struct {
	checker ForgedBlockChecker
}

type slotBattleRecorderHolder struct {
	recorder SlotBattleRecorder
}

type LedgerState struct {
	metrics                            stateMetrics
	currentEra                         eras.EraDesc
	activeEras                         []eras.EraDesc
	config                             LedgerStateConfig
	chainsyncBlockfetchTimeoutTimer    *time.Timer // timeout timer for blockfetch operations
	chainsyncBlockfetchTimerGeneration uint64      // generation counter to detect stale timer callbacks
	currentPParams                     lcommon.ProtocolParameters
	prevEraPParams                     lcommon.ProtocolParameters // pparams from the immediately previous era (for era-1 TX validation)
	transitionInfo                     hardfork.TransitionInfo    // upcoming era boundary state (mirrors Haskell HFC TransitionInfo)
	hfiEvalDoneEpoch                   uint64                     // currentEpoch.EpochId for which the HFI tally has been kicked off (held under ls.RWMutex)
	hfiEvalGeneration                  atomic.Uint64              // bumped on rollback to invalidate any in-flight HFI tally
	hfiStabilityEvalInFlight           atomic.Bool                // guard against overlapping async HFI tallies
	mempool                            MempoolProvider
	timerCleanupConsumedUtxos          *time.Timer
	Scheduler                          *Scheduler
	chain                              *chain.Chain
	db                                 *database.Database
	chainsyncState                     ChainsyncState
	currentTipBlockNonce               []byte
	epochCache                         []models.Epoch
	epochNonceHexCache                 map[uint64]string
	checkpoints                        map[uint64]string // configured chain checkpoints keyed by block number (height)
	slotsPerKESPeriod                  atomic.Uint64
	forgedBlockChecker                 atomic.Pointer[forgedBlockCheckerHolder]
	slotBattleRecorder                 atomic.Pointer[slotBattleRecorderHolder]
	cachedShape                        atomic.Pointer[hardfork.Shape] // lazy-built from CardanoNodeConfig; immutable for the LedgerState's lifetime
	reachedTip                         bool
	currentTip                         ochainsync.Tip
	currentEpoch                       models.Epoch
	dbWorkerPool                       *DatabaseWorkerPool
	slotClock                          *SlotClock
	slotTickChan                       <-chan SlotTick
	ctx                                context.Context
	// leiosBackfill prefetches historical Leios endorser blocks by point ahead
	// of the apply cursor (nil when no endorser-block fetcher is configured).
	leiosBackfill *leiosBackfiller
	sync.RWMutex
	chainsyncMutex                sync.Mutex
	chainsyncBlockfetchMutex      sync.Mutex
	chainsyncBlockfetchReadyMutex sync.Mutex
	chainsyncBlockfetchReadyChan  chan struct{}
	activeBlockfetchConnId        ouroboros.ConnectionId // connection used for current blockfetch pipeline
	shadowBlockfetchConnId        ouroboros.ConnectionId // shadow peer dispatched for parallel blockfetch
	selectedBlockfetchConnId      ouroboros.ConnectionId // latest selected chainsync connection for the next batch
	headerPipelineConnId          ouroboros.ConnectionId // connection that currently owns the queued header/blockfetch pipeline
	pendingBlockfetchEvents       []BlockfetchEvent
	activeBlockfetchStart         time.Time           // when RequestRange was issued (for latency measurement)
	firstBlockReceived            bool                // true after latency sample recorded for this batch
	shadowBlockReceivedHashes     map[string]struct{} // blocks delivered this batch (dedup shadow vs primary)
	batchBlocksReceived           int                 // total blocks received in current blockfetch batch (including mid-batch flushes)
	checkpointWrittenForEpoch     bool
	closed                        atomic.Bool
	inRecovery                    bool // guards against recursive recovery in SubmitAsyncDBTxn
	lastAtTipRecovery             *atTipRecoveryAttempt
	mithrilLedgerSlot             uint64 // blocks at or below this slot are Mithril-verified; skip validation
	mithrilLedgerHash             []byte // hash for mithrilLedgerSlot, used as a stable chainsync intersect point
	lastLocalRollbackSeq          uint64
	lastLocalRollbackPoint        ocommon.Point

	// Subscription IDs for event bus unsubscribe on close
	chainsyncSubID           event.EventSubscriberId
	chainsyncAwaitReplySubID event.EventSubscriberId
	blockfetchSubID          event.EventSubscriberId
	chainUpdateSubID         event.EventSubscriberId
	chainSwitchSubID         event.EventSubscriberId
	connClosedSubID          event.EventSubscriberId

	// rollbackMu serializes rollbackWG.Add with Close's rollbackWG.Wait
	// to prevent Add-after-Wait panics from the TOCTOU race between
	// closed.Load() and Add(1) in handleEventChainUpdate.
	rollbackMu sync.Mutex
	// rollbackWG tracks in-flight rollback event emission goroutines
	rollbackWG sync.WaitGroup
	// replayMu serializes replayWG.Add with Close's replayWG.Wait to
	// prevent Add-after-Wait panics from the TOCTOU race between
	// closed.Load() and Add(1) in replayBufferedHeadersAsync (#2107).
	replayMu sync.Mutex
	// replayWG tracks in-flight replayBufferedHeadersAsync goroutines so
	// Close can drain them before the database is closed (issue #2107).
	replayWG          sync.WaitGroup
	validationEnabled bool
	// Sync progress reporting (Fix 4)
	syncProgressLastLog  time.Time     // last time we logged sync progress
	syncProgressLastSlot uint64        // slot at last progress log (for rate calc)
	syncUpstreamTipSlot  atomic.Uint64 // upstream peer's tip slot
	nextNonceReadyEpoch  atomic.Uint64 // last ready epoch emitted for next-epoch nonce stability

	// Rate-limiting for non-active rollback drop messages
	dropRollbackLastLog time.Time // last time we logged a drop rollback
	dropRollbackCount   int64     // count of suppressed drop rollbacks since last log

	rollbackHistory []rollbackRecord // recent rollback slot+time pairs for loop detection

	lastActiveConnId *ouroboros.ConnectionId // tracks active connection for switch detection

	// Header mismatch tracking for fork detection and re-sync
	headerMismatchCount  int // consecutive header mismatch count
	bufferedHeaderEvents map[string][]ChainsyncEvent
	peerHeaderHistory    map[string]*peerHeaderChain
	// Test hook for fork ancestor lookups.
	lookupBlockByHash func([]byte) (models.Block, error)
}

// EraTransitionResult holds computed state from an era transition
type EraTransitionResult struct {
	NewPParams lcommon.ProtocolParameters
	NewEra     eras.EraDesc
}

// HardForkInfo holds details about a detected hard fork
// transition, populated when a protocol parameter update at
// an epoch boundary changes the protocol major version into
// a new era.
type HardForkInfo struct {
	OldVersion ProtocolVersion
	NewVersion ProtocolVersion
	FromEra    uint
	ToEra      uint
}

// EpochRolloverResult holds computed state from epoch rollover
type EpochRolloverResult struct {
	NewEpochCache             []models.Epoch
	NewCurrentEpoch           models.Epoch
	NewCurrentEra             eras.EraDesc
	NewCurrentPParams         lcommon.ProtocolParameters
	NewEpochNum               float64
	CheckpointWrittenForEpoch bool
	SchedulerIntervalMs       uint
	// HardFork is non-nil when a protocol version change
	// in the updated pparams triggers an era transition.
	HardFork *HardForkInfo
}

func NewLedgerState(cfg LedgerStateConfig) (*LedgerState, error) {
	if cfg.ChainManager == nil {
		return nil, errors.New("a ChainManager is required")
	}
	if cfg.Database == nil {
		return nil, errors.New("a Database is required")
	}
	if cfg.Logger == nil {
		// Create logger to throw away logs
		// We do this so we don't have to add guards around every log operation
		cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if cfg.StartInDijkstra && !cfg.EnableDijkstra {
		return nil, errors.New("StartInDijkstra requires EnableDijkstra")
	}
	// Initialize database worker pool config with defaults if not set
	if cfg.DatabaseWorkerPoolConfig.WorkerPoolSize == 0 &&
		cfg.DatabaseWorkerPoolConfig.TaskQueueSize == 0 {
		cfg.DatabaseWorkerPoolConfig = DefaultDatabaseWorkerPoolConfig()
	}
	ls := &LedgerState{
		config:             cfg,
		activeEras:         eras.ActiveEras(cfg.EnableDijkstra),
		chainsyncState:     InitChainsyncState,
		db:                 cfg.Database,
		chain:              cfg.ChainManager.PrimaryChain(),
		epochNonceHexCache: make(map[uint64]string),
		validationEnabled:  cfg.ValidateHistorical,
	}
	// Cache configured chain checkpoints (keyed by block height) so the
	// hot block-processing path does an O(1) lookup. Nil when the network
	// config supplies no CheckpointsFile.
	if cfg.CardanoNodeConfig != nil {
		ls.checkpoints = cfg.CardanoNodeConfig.Checkpoints()
	}
	// Initialize metrics here so any constructed LedgerState is safe to
	// use without requiring Start() to have been called. Benchmarks and
	// other tests that exercise validateTxCore via a constructed-but-
	// not-started LedgerState would otherwise nil-deref on the metrics
	// counters.
	ls.metrics.init(cfg.PromRegistry)
	ls.slotsPerKESPeriod.Store(ls.loadSlotsPerKESPeriod())
	ls.storeForgedBlockChecker(cfg.ForgedBlockChecker)
	ls.storeSlotBattleRecorder(cfg.SlotBattleRecorder)
	ls.leiosBackfill = newLeiosBackfiller(cfg)
	return ls, nil
}

func (ls *LedgerState) eraList() []eras.EraDesc {
	if len(ls.activeEras) > 0 {
		return ls.activeEras
	}
	return eras.Eras
}

func (ls *LedgerState) eraById(eraId uint) (*eras.EraDesc, bool) {
	eraDesc := eras.GetEraByIdIn(ls.eraList(), eraId)
	return eraDesc, eraDesc != nil
}

func (ls *LedgerState) eraForVersion(majorVersion uint) (uint, bool) {
	return EraForVersion(ls.eraList(), majorVersion)
}

func (ls *LedgerState) isValidEraAdvancement(
	currentEraId, nextEraId uint,
) bool {
	return eras.IsValidEraAdvancementIn(
		ls.eraList(),
		currentEraId,
		nextEraId,
	)
}

func (ls *LedgerState) isHardForkTransition(
	oldVersion, newVersion ProtocolVersion,
) bool {
	return IsHardForkTransition(ls.eraList(), oldVersion, newVersion)
}

func (ls *LedgerState) Start(ctx context.Context) error {
	ls.ctx = ctx
	ls.metrics.nodeStartTime.Set(
		float64(time.Now().Unix()),
	)
	// Set Shelley start time and epoch length from genesis config
	if sg := ls.config.CardanoNodeConfig.ShelleyGenesis(); sg != nil {
		ls.metrics.shelleyStartTime.Set(float64(sg.SystemStart.Unix()))
	}
	if ls.currentEpoch.LengthInSlots > 0 {
		ls.metrics.epochLengthSlots.Set(float64(ls.currentEpoch.LengthInSlots))
	}

	ls.loadMithrilTrustBoundary()

	// Initialize database worker pool for async operations
	if !ls.config.DatabaseWorkerPoolConfig.Disabled {
		ls.dbWorkerPool = NewDatabaseWorkerPool(
			ls.db,
			ls.config.DatabaseWorkerPoolConfig,
		)
		ls.config.Logger.Info(
			"database worker pool initialized",
			"workers", ls.config.DatabaseWorkerPoolConfig.WorkerPoolSize,
			"queue_size", ls.config.DatabaseWorkerPoolConfig.TaskQueueSize,
		)
	} else {
		ls.config.Logger.Info("database worker pool disabled")
	}

	// Setup event handlers. The chainsync/blockfetch/chain-update streams
	// can burst at bulk-sync rates (#1556 / #1914), so they opt into the
	// large EventQueueSize buffer. Sparser streams use the default.
	if ls.config.EventBus != nil {
		ls.chainsyncSubID = ls.config.EventBus.SubscribeFuncWithBuffer(
			ChainsyncEventType,
			event.EventQueueSize,
			ls.handleEventChainsync,
		)
		ls.chainsyncAwaitReplySubID = ls.config.EventBus.SubscribeFunc(
			ChainsyncAwaitReplyEventType,
			ls.handleEventChainsyncAwaitReply,
		)
		ls.blockfetchSubID = ls.config.EventBus.SubscribeFuncWithBuffer(
			BlockfetchEventType,
			event.EventQueueSize,
			ls.handleEventBlockfetch,
		)
		ls.chainUpdateSubID = ls.config.EventBus.SubscribeFuncWithBuffer(
			chain.ChainUpdateEventType,
			event.EventQueueSize,
			ls.handleEventChainUpdate,
		)
		ls.chainSwitchSubID = ls.config.EventBus.SubscribeFunc(
			chainselection.ChainSwitchEventType,
			ls.handleChainSwitchEvent,
		)
		ls.connClosedSubID = ls.config.EventBus.SubscribeFunc(
			ConnectionClosedEventType,
			ls.handleConnectionClosedEvent,
		)
	}
	// Schedule periodic process to purge consumed UTxOs outside of the rollback window
	ls.scheduleCleanupConsumedUtxos()
	// Load epoch info from DB
	err := ls.SubmitAsyncDBTxn(func(txn *database.Txn) error {
		return ls.loadEpochs(txn)
	}, true)
	if err != nil {
		return fmt.Errorf("failed to load epoch info: %w", err)
	}
	ls.checkpointWrittenForEpoch = false
	// Load current protocol parameters from DB
	if err := ls.loadPParams(); err != nil {
		return fmt.Errorf("failed to load pparams: %w", err)
	}
	// Reconstruct TransitionInfo from loaded state.  After restart, the
	// in-memory field is zero (TransitionUnknown), but if the node shut down
	// while in the window between an epoch-boundary version bump and the first
	// block of the new era, the pparams will report a major version that maps
	// to a later era than currentEpoch.EraId.  Detecting this here restores
	// the correct TransitionKnown state without persisting extra data.
	ls.reconstructTransitionInfo()
	// Load current tip
	if err := ls.loadTip(); err != nil {
		return fmt.Errorf("failed to load tip: %w", err)
	}
	// Now that both tip and epoch are loaded, check whether the safe zone
	// already covers the epoch end (TransitionImpossible).  This handles the
	// case where the node was shut down after the tip advanced past the
	// stability window but before the next epoch rollover was processed.
	// First honor any TestXHardForkAtEpoch override (TriggerAtEpoch): this
	// short-circuits TransitionUnknown/Impossible with a known-in-advance
	// transition epoch, matching the Haskell HFC semantics.
	ls.evaluateTriggerAtEpoch()
	ls.evaluateTransitionImpossible()
	ls.evaluateHardForkInitiationStability()
	if err := ls.reconcilePrimaryChainTipWithLedgerTip(); err != nil {
		return fmt.Errorf("failed to reconcile primary chain tip: %w", err)
	}
	// Create genesis block
	if err := ls.createGenesisBlock(); err != nil {
		return fmt.Errorf("failed to create genesis block: %w", err)
	}
	// Initialize scheduler
	if err := ls.initScheduler(); err != nil {
		return fmt.Errorf("initialize scheduler: %w", err)
	}
	// Schedule block forging
	ls.initForge()
	// Start slot clock for slot-boundary-aware timing
	if ls.slotClock != nil {
		ls.slotClock.Start(ctx)
		go ls.handleSlotTicks()
	}
	// Start goroutine to process new blocks unless the caller will feed trusted
	// batches directly into the replay loop.
	if !ls.config.ManualBlockProcessing {
		go ls.ledgerProcessBlocks(ctx)
	}
	return nil
}

func (ls *LedgerState) loadMithrilTrustBoundary() {
	// Read Mithril ledger state point if present. Blocks at or below
	// this point were verified by the Mithril certificate chain during
	// import and must not be re-validated during chainsync replay.
	mithrilSlotStr, err := ls.db.GetSyncState(
		mithrilLedgerSlotSyncKey,
		nil,
	)
	if err != nil {
		ls.config.Logger.Warn(
			"failed to read Mithril trust boundary from database",
			"component", "ledger",
			"error", err,
		)
		return
	}
	if mithrilSlotStr == "" {
		return
	}
	mls, parseErr := strconv.ParseUint(mithrilSlotStr, 10, 64)
	if parseErr != nil {
		ls.config.Logger.Warn(
			"malformed mithril_ledger_slot value, ignoring",
			"component", "ledger",
			"value", mithrilSlotStr,
			"error", parseErr,
		)
		return
	}

	ls.mithrilLedgerSlot = mls
	hashStr, err := ls.db.GetSyncState(mithrilLedgerHashSyncKey, nil)
	if err != nil {
		ls.config.Logger.Warn(
			"failed to read Mithril trust boundary hash from database",
			"component", "ledger",
			"mithril_ledger_slot", mls,
			"error", err,
		)
		return
	}
	if hashStr != "" {
		hash, decodeErr := hex.DecodeString(hashStr)
		if decodeErr != nil || len(hash) != lcommon.Blake2b256Size {
			ls.config.Logger.Warn(
				"malformed mithril_ledger_hash value, ignoring hash",
				"component", "ledger",
				"mithril_ledger_slot", mls,
				"value", hashStr,
				"error", decodeErr,
			)
		} else {
			ls.mithrilLedgerHash = hash
		}
	}

	attrs := []any{
		"component", "ledger",
		"mithril_ledger_slot", mls,
	}
	if len(ls.mithrilLedgerHash) > 0 {
		attrs = append(
			attrs,
			"mithril_ledger_hash",
			hex.EncodeToString(ls.mithrilLedgerHash),
		)
	}
	ls.config.Logger.Info("loaded Mithril trust boundary", attrs...)
}

func (ls *LedgerState) RecoverCommitTimestampConflict() error {
	// Load current ledger tip
	tmpTip, err := ls.db.GetTip(nil)
	if err != nil {
		return fmt.Errorf("failed to get tip: %w", err)
	}
	// Check if we can lookup tip block in chain
	_, err = ls.chain.BlockByPoint(tmpTip.Point, nil)
	if err != nil {
		// Rollback to raw chain tip on error.
		// Note: We do NOT hold ls.Lock() here because rollback() calls
		// SubmitAsyncDBTxn() which may trigger PartialCommitError recovery
		// that re-acquires ls.Lock(), causing a deadlock. The rollback
		// method handles its own locking for in-memory state updates.
		chainTip := ls.chain.Tip()
		if err = ls.rollback(chainTip.Point); err != nil {
			return fmt.Errorf(
				"failed to rollback ledger: %w",
				err,
			)
		}
	}
	// Get the current tip after potential rollback for orphan cleanup.
	// This ensures we use the post-rollback tip, not the stale tmpTip.
	currentTip, err := ls.db.GetTip(nil)
	if err != nil {
		return fmt.Errorf(
			"failed to get current tip for orphan cleanup: %w",
			err,
		)
	}
	// Clean up orphaned blobs that may exist beyond the metadata tip.
	// This handles the case where blob committed but metadata failed.
	if cleanupErr := ls.cleanupOrphanedBlobs(currentTip.Point.Slot); cleanupErr != nil {
		// Log but don't fail - partial cleanup is acceptable
		ls.config.Logger.Warn(
			"failed to clean up orphaned blobs",
			"error", cleanupErr,
		)
	}
	return nil
}

// orphanedBlock holds information needed to delete an orphaned block from blob store.
type orphanedBlock struct {
	slot uint64
	hash []byte
	id   uint64
}

// cleanupOrphanedBlobs removes blob blocks beyond the metadata tip.
// Called during recovery when commit timestamp mismatch is detected.
// This handles the case where blob committed successfully but metadata failed,
// leaving orphaned blocks in the blob store.
func (ls *LedgerState) cleanupOrphanedBlobs(tipSlot uint64) error {
	blobStore := ls.db.Blob()
	if blobStore == nil {
		return nil // No blob store configured
	}

	ls.config.Logger.Info(
		"starting orphaned blob cleanup",
		"tip_slot", tipSlot,
	)

	// Phase 1: Scan for orphaned blocks (read-only transaction)
	orphans, err := ls.scanOrphanedBlobs(blobStore, tipSlot)
	if err != nil {
		return err
	}

	if len(orphans) == 0 {
		ls.config.Logger.Info("no orphaned blobs found")
		return nil
	}

	// Phase 2: Delete orphaned blocks (read-write transaction)
	writeTxn := blobStore.NewTransaction(true)
	defer writeTxn.Rollback() //nolint:errcheck
	deleted := 0

	for _, orphan := range orphans {
		if err := blobStore.DeleteBlock(writeTxn, orphan.slot, orphan.hash, orphan.id); err != nil {
			ls.config.Logger.Warn(
				"failed to delete orphaned block",
				"slot", orphan.slot,
				"hash", hex.EncodeToString(orphan.hash),
				"error", err,
			)
			continue
		}
		deleted++
	}

	if err := writeTxn.Commit(); err != nil {
		return fmt.Errorf("failed to commit orphan cleanup: %w", err)
	}

	ls.config.Logger.Info(
		"orphaned blob cleanup complete",
		"scanned", len(orphans),
		"deleted", deleted,
	)
	return nil
}

// scanOrphanedBlobs scans the blob store for blocks beyond the given tip slot.
// Returns a slice of orphaned blocks that should be deleted.
func (ls *LedgerState) scanOrphanedBlobs(
	blobStore interface {
		NewTransaction(readWrite bool) types.Txn
		NewIterator(txn types.Txn, opts types.BlobIteratorOptions) types.BlobIterator
		GetBlock(txn types.Txn, slot uint64, hash []byte) ([]byte, types.BlockMetadata, error)
	},
	tipSlot uint64,
) ([]orphanedBlock, error) {
	var orphans []orphanedBlock

	readTxn := blobStore.NewTransaction(false)
	defer readTxn.Rollback() //nolint:errcheck

	iterOpts := types.BlobIteratorOptions{
		Prefix: []byte(types.BlockBlobKeyPrefix),
	}
	it := blobStore.NewIterator(readTxn, iterOpts)
	if it == nil {
		return nil, errors.New("failed to create blob iterator")
	}
	defer it.Close()

	// Build seek key for slot > tipSlot (tipSlot + 1)
	seekSlot := tipSlot + 1
	seekKey := make([]byte, 0, 10) // "bp" + 8 bytes for slot
	seekKey = append(seekKey, []byte(types.BlockBlobKeyPrefix)...)
	slotBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(slotBytes, seekSlot)
	seekKey = append(seekKey, slotBytes...)

	for it.Seek(seekKey); it.ValidForPrefix([]byte(types.BlockBlobKeyPrefix)); it.Next() {
		item := it.Item()
		if item == nil {
			continue
		}
		key := item.Key()
		if key == nil {
			continue
		}
		// Skip metadata keys (suffix "_metadata")
		if strings.HasSuffix(string(key), types.BlockBlobMetadataKeySuffix) {
			continue
		}
		// Parse slot from key: prefix (2 bytes) + slot (8 bytes) + hash (32 bytes)
		if len(key) < 10 {
			continue
		}
		blockSlot := binary.BigEndian.Uint64(key[2:10])
		// Double-check slot is beyond tip (should be guaranteed by seek)
		if blockSlot <= tipSlot {
			continue
		}
		// Extract hash (remaining bytes after prefix + slot)
		blockHash := make([]byte, len(key)-10)
		copy(blockHash, key[10:])

		// Get block metadata to retrieve the block ID
		_, metadata, err := blobStore.GetBlock(readTxn, blockSlot, blockHash)
		if err != nil {
			ls.config.Logger.Warn(
				"failed to get orphaned block metadata",
				"slot", blockSlot,
				"error", err,
			)
			continue
		}

		orphans = append(orphans, orphanedBlock{
			slot: blockSlot,
			hash: blockHash,
			id:   metadata.ID,
		})
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error during orphan scan: %w", err)
	}

	return orphans, nil
}

func (ls *LedgerState) Chain() *chain.Chain {
	return ls.chain
}

// PoolRegistrationVRFKeyHash returns the VRF key hash recorded on the
// most recent active pool registration certificate for the given pool.
// found is false when the pool has no on-chain registration yet — that
// is informational, not an error condition, since operators commonly
// stage credentials before the registration certificate is on chain.
//
// Used by the block-producer credential check at startup to confirm the
// loaded VRF key matches what the chain has on file.
func (ls *LedgerState) PoolRegistrationVRFKeyHash(
	poolID [28]byte,
) (vrfHash [32]byte, found bool, err error) {
	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolID[:]))
	pool, err := ls.db.GetPool(pkh, false, nil)
	if err != nil {
		if errors.Is(err, models.ErrPoolNotFound) {
			return [32]byte{}, false, nil
		}
		return [32]byte{}, false, err
	}
	if pool == nil {
		return [32]byte{}, false, nil
	}
	if len(pool.Registration) > 0 &&
		len(pool.Registration[0].VrfKeyHash) == 32 {
		copy(vrfHash[:], pool.Registration[0].VrfKeyHash)
		return vrfHash, true, nil
	}
	if len(pool.VrfKeyHash) != 32 {
		return [32]byte{}, false, nil
	}
	copy(vrfHash[:], pool.VrfKeyHash)
	return vrfHash, true, nil
}

func (ls *LedgerState) LatestOpCertSequence(
	poolID [28]byte,
) (sequence uint64, found bool, err error) {
	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolID[:]))
	pool, err := ls.db.GetPool(pkh, false, nil)
	if err != nil {
		if errors.Is(err, models.ErrPoolNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if pool == nil {
		return 0, false, nil
	}
	return ls.db.LatestPoolOpCertSequence(pkh, nil)
}

// Datum looks up a datum by hash & adding this for implementing query.ReadData #741
func (ls *LedgerState) Datum(hash []byte) (*models.Datum, error) {
	return ls.db.GetDatum(hash, nil)
}

func (ls *LedgerState) Close() error {
	if !ls.closed.CompareAndSwap(false, true) {
		return nil
	}

	// Unsubscribe from event bus to stop receiving new events
	if ls.config.EventBus != nil {
		ls.config.EventBus.Unsubscribe(
			ChainsyncEventType,
			ls.chainsyncSubID,
		)
		ls.config.EventBus.Unsubscribe(
			ChainsyncAwaitReplyEventType,
			ls.chainsyncAwaitReplySubID,
		)
		ls.config.EventBus.Unsubscribe(
			BlockfetchEventType,
			ls.blockfetchSubID,
		)
		ls.config.EventBus.Unsubscribe(
			chain.ChainUpdateEventType,
			ls.chainUpdateSubID,
		)
		ls.config.EventBus.Unsubscribe(
			chainselection.ChainSwitchEventType,
			ls.chainSwitchSubID,
		)
		ls.config.EventBus.Unsubscribe(
			ConnectionClosedEventType,
			ls.connClosedSubID,
		)
	}

	// Wait for in-flight rollback event emission goroutines.
	// Hold rollbackMu so no new goroutine can Add(1) between our
	// closed flag and this Wait.
	ls.config.Logger.Info("waiting for in-flight rollback goroutines")
	rollbackStart := time.Now()
	ls.rollbackMu.Lock()
	rollbackDone := make(chan struct{})
	go func() {
		ls.rollbackWG.Wait()
		close(rollbackDone)
	}()
	select {
	case <-rollbackDone:
		ls.config.Logger.Info(
			"rollback goroutines finished",
			"elapsed", time.Since(rollbackStart).Round(time.Millisecond),
		)
	case <-time.After(10 * time.Second):
		ls.config.Logger.Warn(
			"timed out waiting for rollback goroutines",
			"elapsed", time.Since(rollbackStart).Round(time.Millisecond),
		)
	}
	ls.rollbackMu.Unlock()

	// Drain in-flight replayBufferedHeadersAsync goroutines so they
	// finish issuing DB reads before the owner closes the database
	// (#2107). Hold replayMu so no new goroutine can Add(1) between our
	// closed flag and this Wait. The closed flag set above prevents
	// new goroutines from being spawned, so the Wait is bounded by the
	// in-flight workers; we wait unconditionally because returning
	// while replay goroutines are still issuing DB reads would
	// reintroduce the panic this fix is meant to prevent.
	ls.config.Logger.Info("waiting for in-flight header replay goroutines")
	replayStart := time.Now()
	ls.replayMu.Lock()
	ls.replayWG.Wait()
	ls.replayMu.Unlock()
	ls.config.Logger.Info(
		"header replay goroutines finished",
		"elapsed", time.Since(replayStart).Round(time.Millisecond),
	)

	// Stop slot clock
	if ls.slotClock != nil {
		ls.config.Logger.Info("stopping slot clock")
		ls.slotClock.Stop()
		ls.config.Logger.Info("slot clock stopped")
	}

	// Shutdown database worker pool
	if ls.dbWorkerPool != nil {
		ls.config.Logger.Info("shutting down database worker pool")
		poolStart := time.Now()
		poolDone := make(chan struct{})
		go func() {
			ls.dbWorkerPool.Shutdown()
			close(poolDone)
		}()
		select {
		case <-poolDone:
			ls.config.Logger.Info(
				"database worker pool shut down",
				"elapsed", time.Since(poolStart).Round(time.Millisecond),
			)
		case <-time.After(15 * time.Second):
			ls.config.Logger.Warn(
				"timed out waiting for database worker pool shutdown",
				"elapsed", time.Since(poolStart).Round(time.Millisecond),
			)
		}
	}

	// Note: We don't close the database here because LedgerState doesn't own it.
	// The database is passed in via LedgerStateConfig and should be closed by
	// the owner (typically Node.shutdown()).
	return nil
}

func (ls *LedgerState) initScheduler() error {
	// Initialize timer with current slot length
	slotLength := ls.currentEpoch.SlotLength
	if slotLength == 0 {
		shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
		if shelleyGenesis == nil {
			return errors.New("could not get genesis config")
		}
		slotLength = uint(
			new(big.Int).Div(
				new(big.Int).Mul(
					big.NewInt(1000),
					shelleyGenesis.SlotLength.Num(),
				),
				shelleyGenesis.SlotLength.Denom(),
			).Uint64(),
		)
	}
	// nolint:gosec
	// Slot length is small enough to not overflow int64
	interval := time.Duration(slotLength) * time.Millisecond
	ls.Scheduler = NewScheduler(interval)
	ls.Scheduler.Start()

	// Initialize slot clock for slot-boundary-aware timing
	slotClockConfig := SlotClockConfig{
		Logger: ls.config.Logger,
	}
	provider := newLedgerStateSlotProvider(ls)
	ls.slotClock = NewSlotClock(provider, slotClockConfig)
	ls.slotTickChan = ls.slotClock.Subscribe()

	// Initialize epoch tracking based on stored state.
	// This prevents re-emitting events for epochs that have already been processed.
	ls.slotClock.SetLastEmittedEpoch(ls.currentEpoch.EpochId)

	// Log startup epoch info for diagnostics
	if wallClockEpoch, err := ls.slotClock.CurrentEpoch(); err == nil {
		if wallClockEpoch.EpochId != ls.currentEpoch.EpochId {
			ls.config.Logger.Info("startup epoch state",
				"ledger_epoch", ls.currentEpoch.EpochId,
				"wall_clock_epoch", wallClockEpoch.EpochId,
				"epochs_behind", wallClockEpoch.EpochId-ls.currentEpoch.EpochId,
			)
		} else {
			ls.config.Logger.Debug("startup epoch state (synced)",
				"epoch", ls.currentEpoch.EpochId,
			)
		}
	}

	return nil
}

func (ls *LedgerState) initForge() {
	// Schedule block forging if dev mode is enabled
	if ls.config.ForgeBlocks {
		// Calculate block interval from ActiveSlotsCoeff
		shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
		if shelleyGenesis != nil {
			// Calculate block interval (1 / ActiveSlotsCoeff)
			activeSlotsCoeff := shelleyGenesis.ActiveSlotsCoeff
			if activeSlotsCoeff.Rat != nil &&
				activeSlotsCoeff.Rat.Num().Int64() > 0 {
				blockInterval := int(
					(1 * activeSlotsCoeff.Rat.Denom().Int64()) / activeSlotsCoeff.Rat.Num().
						Int64(),
				)
				// Scheduled forgeBlock to run at the calculated block interval
				// TODO: add callback to capture task run failure and increment "missed slot leader check" metric
				ls.Scheduler.Register(blockInterval, ls.forgeBlock, nil)

				ls.config.Logger.Info(
					"dev mode block forging enabled",
					"component", "ledger",
					"block_interval", blockInterval,
					"active_slots_coeff", activeSlotsCoeff.String(),
				)
			}
		}
	}
}

// handleSlotTicks processes slot tick notifications from the slot clock.
// When the current epoch crosses the nonce stability cutoff or reaches an
// epoch boundary, it emits events for subscribers like snapshot managers and
// leader election.
//
// The slot clock provides proactive epoch detection that doesn't depend on block
// arrival. This is critical for block production where the node must wake up at
// slot boundaries for leader election even when no blocks are arriving.
//
// Epoch event emission follows these rules:
//  1. During catch up or load (validationEnabled=false): suppress slot-based events,
//     let block processing handle epoch transitions for historical data.
//  2. When synced (validationEnabled=true): emit slot-based events immediately,
//     using MarkEpochEmitted to coordinate with block-based detection.
func (ls *LedgerState) handleSlotTicks() {
	logger := ls.config.Logger.With("component", "ledger")

	for tick := range ls.slotTickChan {
		// Get current state snapshot
		ls.RLock()
		currentEpoch := ls.currentEpoch
		currentEra := ls.currentEra
		tipSlot := ls.currentTip.Point.Slot
		ls.RUnlock()

		// Update wall-clock-based metrics every tick
		// (must run even when chain is stalled or catching up)
		if tick.Slot > tipSlot {
			ls.metrics.tipGapSlots.Set(float64(tick.Slot - tipSlot))
		} else {
			ls.metrics.tipGapSlots.Set(0)
		}
		if currentEpoch.LengthInSlots > 0 {
			ls.metrics.epochLengthSlots.Set(float64(currentEpoch.LengthInSlots))
		}

		// During catch up, don't emit slot-based epoch events. Block
		// processing handles epoch transitions for historical data. We
		// consider the node "near tip" when the ledger tip is inside the
		// current era's stability window from the upstream peer's tip.
		if !ls.isNearTip(tipSlot) {
			if tick.IsEpochStart {
				logger.Debug(
					"slot clock epoch boundary during catch up (suppressed)",
					"slot_clock_epoch",
					tick.Epoch,
					"ledger_epoch",
					currentEpoch.EpochId,
					"slot",
					tick.Slot,
				)
			}
			continue
		}

		ls.emitNextEpochNonceReady(
			logger,
			tick,
			currentEpoch,
			currentEra,
			tipSlot,
		)

		if !tick.IsEpochStart {
			continue
		}

		// We're synced - emit proactive epoch event for leader election.
		// Use MarkEpochEmitted to coordinate with block-based detection
		// and avoid duplicate events.
		if !ls.slotClock.MarkEpochEmitted(tick.Epoch) {
			// Already emitted by block processing
			logger.Debug(
				"slot clock epoch boundary already emitted by block processing",
				"epoch",
				tick.Epoch,
				"slot",
				tick.Slot,
			)
			continue
		}

		logger.Info("epoch boundary reached via slot clock",
			"slot", tick.Slot,
			"epoch", tick.Epoch,
			"ledger_epoch", currentEpoch.EpochId,
		)

		// Calculate snapshot slot (boundary - 1, or 0 if boundary is 0)
		snapshotSlot := tick.Slot
		if snapshotSlot > 0 {
			snapshotSlot--
		}

		// Emit epoch transition event
		if ls.config.EventBus != nil {
			// Note: EpochNonce is nil for slot-based events because the new epoch's
			// nonce is computed from block headers, which aren't available until
			// block processing runs. Subscribers that need the nonce should wait
			// for the block-based event or query it later.
			epochTransitionEvent := event.EpochTransitionEvent{
				PreviousEpoch:   tick.Epoch - 1,
				NewEpoch:        tick.Epoch,
				BoundarySlot:    tick.Slot,
				EpochNonce:      nil,
				ProtocolVersion: currentEra.Id,
				SnapshotSlot:    snapshotSlot,
			}
			ls.config.EventBus.Publish(
				event.EpochTransitionEventType,
				event.NewEvent(
					event.EpochTransitionEventType,
					epochTransitionEvent,
				),
			)
			logger.Debug("emitted slot-based epoch transition event",
				"epoch", tick.Epoch,
				"boundary_slot", tick.Slot,
			)
		}

		// Log if there's a discrepancy between slot clock epoch and ledger epoch.
		// This can happen if block processing is slightly behind wall-clock time.
		if currentEpoch.EpochId != tick.Epoch &&
			currentEpoch.EpochId != tick.Epoch-1 {
			logger.Warn("epoch discrepancy between slot clock and ledger state",
				"slot_clock_epoch", tick.Epoch,
				"ledger_epoch", currentEpoch.EpochId,
				"slot", tick.Slot,
			)
		}
	}
}

func (ls *LedgerState) emitNextEpochNonceReady(
	logger *slog.Logger,
	tick SlotTick,
	currentEpoch models.Epoch,
	currentEra eras.EraDesc,
	tipSlot uint64,
) {
	if ls.config.EventBus == nil || currentEra.Id == 0 {
		return
	}
	// Only publish when wall-clock and ledger agree on the current epoch.
	if tick.Epoch != currentEpoch.EpochId {
		return
	}

	cutoffSlot, ready := ls.nextEpochNonceReadyCutoffSlot(currentEpoch)
	if !ready || tick.Slot < cutoffSlot || tipSlot < cutoffSlot {
		return
	}

	readyEpoch := currentEpoch.EpochId + 1
	if ls.nextNonceReadyEpoch.Load() == readyEpoch {
		return
	}
	if len(ls.EpochNonce(readyEpoch)) == 0 {
		return
	}
	ls.nextNonceReadyEpoch.Store(readyEpoch)

	readyEvent := event.EpochNonceReadyEvent{
		CurrentEpoch: currentEpoch.EpochId,
		ReadyEpoch:   readyEpoch,
		CutoffSlot:   cutoffSlot,
	}
	ls.config.EventBus.Publish(
		event.EpochNonceReadyEventType,
		event.NewEvent(event.EpochNonceReadyEventType, readyEvent),
	)
	logger.Info(
		"next epoch nonce is stable, leader schedule can be precomputed",
		"current_epoch", currentEpoch.EpochId,
		"ready_epoch", readyEpoch,
		"cutoff_slot", cutoffSlot,
		"slot", tick.Slot,
	)
}

func (ls *LedgerState) resetNextEpochNonceReady() {
	ls.nextNonceReadyEpoch.Store(0)
}

// isNearTip returns true when the given slot is inside the current era's
// stability window from the upstream peer's tip. This is used to decide
// whether to emit slot-clock epoch events. During initial catch-up the node is
// far behind the tip and these checks are skipped; once the node is close to
// the tip they are always on. Returns false when no upstream tip is known yet
// (no peer connected), since we can't determine proximity.
func (ls *LedgerState) isNearTip(slot uint64) bool {
	upstreamTip := ls.syncUpstreamTipSlot.Load()
	if upstreamTip == 0 {
		return false
	}
	if slot >= upstreamTip {
		return true
	}
	return upstreamTip-slot <= ls.calculateStabilityWindow()
}

func (ls *LedgerState) scheduleCleanupConsumedUtxos() {
	ls.Lock()
	defer ls.Unlock()
	if ls.timerCleanupConsumedUtxos != nil {
		ls.timerCleanupConsumedUtxos.Stop()
	}
	ls.timerCleanupConsumedUtxos = time.AfterFunc(
		cleanupConsumedUtxosInterval,
		func() {
			ls.cleanupConsumedUtxos()
			// Schedule the next run
			ls.scheduleCleanupConsumedUtxos()
		},
	)
}

func (ls *LedgerState) cleanupConsumedUtxos() {
	// In API storage mode we retain spent UTxO metadata rows past the
	// stability window so historical transaction queries can resolve
	// input/collateral/reference-input details via spent_at_tx_id,
	// collateral_by_tx_id, and referenced_by_tx_id. Spent state is already
	// encoded by deleted_slot and spent_at_tx_id; live-UTxO queries continue
	// to filter on deleted_slot = 0.
	if ls.db.StorageMode() == types.StorageModeAPI {
		return
	}
	// Get the current tip slot while holding the read lock to avoid TOCTOU race.
	// We capture only the slot value we need, so even if currentTip changes after
	// we release the lock, we're working with a consistent snapshot of the slot.
	ls.RLock()
	tipSlot := ls.currentTip.Point.Slot
	eraId := ls.currentEra.Id
	ls.RUnlock()
	stabilityWindow := ls.calculateStabilityWindowForEra(eraId)

	// Delete UTxOs that are marked as deleted and older than our slot window
	ls.config.Logger.Debug(
		"cleaning up consumed UTxOs",
		"component", "ledger",
	)
	if stabilityWindow == 0 {
		return
	}
	if tipSlot > stabilityWindow {
		for {
			// No lock needed here - the database handles its own consistency
			// and we're not accessing any in-memory LedgerState fields.
			// The tipSlot was captured above with a read lock.
			count, err := ls.db.UtxosDeleteConsumed(
				tipSlot-stabilityWindow,
				10000,
				nil,
			)
			if count == 0 {
				break
			}
			if err != nil {
				ls.config.Logger.Error(
					"failed to cleanup consumed UTxOs",
					"component", "ledger",
					"error", err,
				)
				break
			}
		}
	}
}

func (ls *LedgerState) rollback(point ocommon.Point) error {
	// Rolling back to the point we already sit at is a no-op. Skip
	// it entirely so we don't publish a "local ledger rollback"
	// resync event for a rollback that didn't move the ledger. That
	// event drives RecoverAfterLocalRollback, which on a single-peer
	// block producer wedges the chain in a per-cycle replay loop
	// when the peer rolls back to a point already covered by queued
	// headers extended via tryResolveFork's "fork extends from
	// current tip" branch (issue #2177).
	ls.RLock()
	currentTip := ls.currentTip
	mithrilLedgerSlot := ls.mithrilLedgerSlot
	ls.RUnlock()
	if currentTip.Point.Slot == point.Slot &&
		bytes.Equal(currentTip.Point.Hash, point.Hash) {
		return nil
	}
	if point.Slot > currentTip.Point.Slot {
		ls.config.Logger.Debug(
			"rollback point ahead of ledger tip, skipping metadata rollback",
			"component", "ledger",
			"rollback_slot", point.Slot,
			"ledger_tip_slot", currentTip.Point.Slot,
			"rollback_hash", hex.EncodeToString(point.Hash),
			"ledger_tip_hash", hex.EncodeToString(currentTip.Point.Hash),
		)
		return nil
	}
	if mithrilLedgerSlot > 0 && point.Slot < mithrilLedgerSlot {
		return ErrRollbackExceedsMithrilBoundary
	}
	// Track new tip value built during transaction
	var newTip ochainsync.Tip
	var newNonce []byte
	// Start a transaction
	err := ls.SubmitAsyncDBTxn(func(txn *database.Txn) error {
		// Delete certificates first (they reference transactions)
		if err := ls.db.DeleteCertificatesAfterSlot(
			point.Slot,
			txn,
		); err != nil {
			return fmt.Errorf(
				"delete certificates after rollback: %w",
				err,
			)
		}
		// Revert reward-account changes before account restoration can delete
		// accounts registered after the rollback slot.
		if err := ls.db.DeleteAccountRewardsAfterSlot(
			point.Slot,
			txn,
		); err != nil {
			return fmt.Errorf(
				"delete account reward deltas after rollback: %w",
				err,
			)
		}
		// Restore account delegation state
		if err := ls.db.RestoreAccountStateAtSlot(
			point.Slot,
			txn,
		); err != nil {
			return fmt.Errorf(
				"restore account state after rollback: %w",
				err,
			)
		}
		// Restore pool state
		if err := ls.db.RestorePoolStateAtSlot(
			point.Slot,
			txn,
		); err != nil {
			return fmt.Errorf(
				"restore pool state after rollback: %w",
				err,
			)
		}
		// Restore DRep state
		if err := ls.db.RestoreDrepStateAtSlot(
			point.Slot,
			txn,
		); err != nil {
			return fmt.Errorf(
				"restore DRep state after rollback: %w",
				err,
			)
		}
		// Delete rolled-back protocol parameters
		if err := ls.db.DeletePParamsAfterSlot(
			point.Slot,
			txn,
		); err != nil {
			return fmt.Errorf(
				"delete protocol params after rollback: %w",
				err,
			)
		}
		// Delete rolled-back protocol parameter updates
		if err := ls.db.DeletePParamUpdatesAfterSlot(
			point.Slot,
			txn,
		); err != nil {
			return fmt.Errorf(
				"delete protocol param updates after rollback: %w",
				err,
			)
		}
		// Delete rolled-back governance proposals
		if err := ls.db.DeleteGovernanceProposalsAfterSlot(
			point.Slot,
			txn,
		); err != nil {
			return fmt.Errorf(
				"delete governance proposals after rollback: %w",
				err,
			)
		}
		// Delete rolled-back governance votes
		if err := ls.db.DeleteGovernanceVotesAfterSlot(
			point.Slot,
			txn,
		); err != nil {
			return fmt.Errorf(
				"delete governance votes after rollback: %w",
				err,
			)
		}
		// Delete rolled-back constitutions
		if err := ls.db.DeleteConstitutionsAfterSlot(
			point.Slot,
			txn,
		); err != nil {
			return fmt.Errorf(
				"delete constitutions after rollback: %w",
				err,
			)
		}
		// Delete rolled-back committee state
		if err := ls.db.DeleteCommitteeMembersAfterSlot(
			point.Slot,
			txn,
		); err != nil {
			return fmt.Errorf(
				"delete committee state after rollback: %w",
				err,
			)
		}
		// Delete epoch entries whose nonces were computed from
		// rolled-back blocks. Epochs starting after the rollback
		// slot used blocks that no longer exist, so their nonces
		// are stale and must be recomputed during re-sync.
		if err := ls.db.DeleteEpochsAfterSlot(
			point.Slot,
			txn,
		); err != nil {
			return fmt.Errorf(
				"delete epochs after rollback: %w",
				err,
			)
		}
		// Delete reward-state rows captured at epoch boundaries that no
		// longer exist on the selected chain.
		if err := ls.db.DeleteRewardStateAfterSlot(
			point.Slot,
			txn,
		); err != nil {
			return fmt.Errorf(
				"delete reward state after rollback: %w",
				err,
			)
		}
		// Delete block nonce rows from the abandoned fork. Epoch
		// nonces are derived from slot-range block_nonce lookups, so
		// same-slot competitors and later fork rows must not survive
		// rollback.
		if err := ls.db.DeleteBlockNoncesAfterPoint(
			point,
			txn,
		); err != nil {
			return fmt.Errorf(
				"delete block nonces after rollback: %w",
				err,
			)
		}
		// Delete rolled-back network state records
		if err := ls.db.DeleteNetworkStateAfterSlot(
			point.Slot,
			txn,
		); err != nil {
			return fmt.Errorf(
				"delete network state after rollback: %w",
				err,
			)
		}
		// Delete rolled-back treasury donation records
		if err := ls.db.DeleteNetworkDonationsAfterSlot(
			point.Slot,
			txn,
		); err != nil {
			return fmt.Errorf(
				"delete network donations after rollback: %w",
				err,
			)
		}
		// Delete rolled-back UTxOs (blob offsets and metadata).
		//
		// Floor the deletion slot at mithrilLedgerSlot. UTxOs produced
		// by gap blocks during Mithril bootstrap are written via
		// SetGapBlockTransaction without advancing the ledger tip, so
		// their added_slot values are well above the persisted ledger
		// tip. A rollback whose target is below the Mithril boundary
		// would otherwise bulk-delete every gap-block-produced UTxO,
		// leaving the chain unable to validate the first post-gap
		// block that consumes one of them. The Mithril snapshot is
		// the trust anchor — we never rewind below it — so the
		// authoritative deletion slot is the rollback target or the
		// Mithril boundary, whichever is later.
		deleteSlot := max(mithrilLedgerSlot, point.Slot)
		err := ls.db.UtxosDeleteRolledback(deleteSlot, txn)
		if err != nil {
			return fmt.Errorf("remove rolled-back UTxOs: %w", err)
		}
		// Delete rolled-back transaction offsets and metadata
		err = ls.db.TransactionsDeleteRolledback(deleteSlot, txn)
		if err != nil {
			return fmt.Errorf("remove rolled-back transactions: %w", err)
		}
		// Restore spent UTxOs. Use the same floored slot as the
		// delete calls above so gap-block transactions and the UTxOs
		// they consumed stay in sync: preserving a tx at slot S while
		// restoring its consumed UTxO at deleted_slot=S would leave
		// the tx pointing at a live UTxO it claims to have spent.
		err = ls.db.UtxosUnspend(deleteSlot, txn)
		if err != nil {
			return fmt.Errorf(
				"restore spent UTxOs after rollback: %w",
				err,
			)
		}
		// Build new tip value
		newTip = ochainsync.Tip{
			Point: point,
		}
		if point.Slot > 0 {
			rollbackBlock, err := ls.chain.BlockByPoint(point, txn)
			if err != nil {
				return fmt.Errorf("failed to get rollback block: %w", err)
			}
			newTip.BlockNumber = rollbackBlock.Number
			// Load nonce for rollback point
			newNonce, err = ls.db.GetBlockNonce(point, txn)
			if err != nil {
				return fmt.Errorf("failed to get block nonce: %w", err)
			}
		}
		// Write tip to DB
		if err = ls.db.SetTip(newTip, txn); err != nil {
			return fmt.Errorf("failed to set tip: %w", err)
		}
		return nil
	}, true)
	if err != nil {
		return err
	}
	// Notify subscribers that pool state has been restored (e.g., for cache invalidation)
	if ls.config.EventBus != nil {
		ls.config.EventBus.PublishAsync(
			PoolStateRestoredEventType,
			event.NewEvent(
				PoolStateRestoredEventType,
				PoolStateRestoredEvent{Slot: point.Slot},
			),
		)
	}
	// Reload epoch cache into locals to discard stale nonces from
	// rolled-back epochs. The deleted epoch entries will be recreated
	// with correct nonces when the chain replays past those epoch
	// boundaries during re-sync.
	//
	// All shared state (epochCache, currentEpoch, currentEra,
	// currentPParams, prevEraPParams, currentTip,
	// currentTipBlockNonce) is computed into local variables first,
	// then applied atomically under a single Lock to avoid data
	// races with concurrent readers.
	var (
		newEpochs         []models.Epoch
		newEpochsRepaired bool
		newCurrentEpoch   models.Epoch
		newCurrentEra     eras.EraDesc
		newPParams        lcommon.ProtocolParameters
		newPrevPParams    lcommon.ProtocolParameters
		eraResolved       bool
	)
	// Snapshot current era under read lock for fallback
	ls.RLock()
	newCurrentEra = ls.currentEra
	newCurrentEpoch = ls.currentEpoch
	ls.RUnlock()

	epochs, err := ls.db.GetEpochs(nil)
	if err != nil {
		ls.config.Logger.Warn(
			"failed to reload epochs after rollback",
			"error", err,
			"component", "ledger",
		)
	}
	if epochs != nil {
		newEpochs = epochs
		// Keep rollback reloads consistent with startup reloads: a
		// persisted empty lab must not re-enter the in-memory cache.
		newEpochsRepaired = ls.healEmptyLabNoncesInPlace(newEpochs)
		// Reset currentEpoch to the last remaining epoch so
		// that ledgerProcessBlocks correctly detects the next
		// epoch boundary and EpochNonce() returns the right
		// nonce.
		if len(epochs) > 0 {
			newCurrentEpoch = epochs[len(epochs)-1]
			eraDesc, _ := ls.eraById(
				newCurrentEpoch.EraId,
			)
			if eraDesc != nil {
				newCurrentEra = *eraDesc
				eraResolved = true
			} else {
				ls.config.Logger.Warn(
					"unknown era ID after rollback, "+
						"currentEra may be stale",
					"era_id",
					newCurrentEpoch.EraId,
					"component", "ledger",
				)
			}
		}
	}
	// Reload protocol parameters into locals to reflect the
	// rolled-back state. Skip if era lookup failed (nil) since
	// the DecodePParamsFunc would be wrong. Recompute when:
	//   - eraResolved: era was successfully resolved so
	//     computePParams can use the correct DecodePParamsFunc
	//   - newEpochs == nil: DB error, fall back to stale snapshot
	// Do NOT recompute when len(newEpochs) == 0 (genesis rollback):
	// the genesis rollback branch explicitly clears pparams to nil,
	// and computing them here with the stale pre-rollback era would
	// produce non-nil pparams that overwrite the intentional nil.
	if eraResolved || newEpochs == nil {
		pp, prevPP, ppErr := ls.computePParams(
			newCurrentEpoch,
			newCurrentEra,
			newEpochs,
		)
		if ppErr != nil {
			ls.config.Logger.Warn(
				"failed to reload protocol params "+
					"after rollback",
				"error", ppErr,
				"component", "ledger",
			)
		} else {
			newPParams = pp
			newPrevPParams = prevPP
		}
	}
	newTipDensity := ls.chainFragmentDensity(
		newTip,
		ls.securityParamForEraOrDefault(newCurrentEra.Id),
	)
	// Transaction committed successfully - now update all
	// in-memory state atomically so readers see a consistent
	// snapshot.
	ls.Lock()
	if newEpochs != nil {
		ls.epochCache = newEpochs
		if newEpochsRepaired {
			clear(ls.epochNonceHexCache)
		}

		if len(newEpochs) > 0 {
			ls.currentEpoch = newCurrentEpoch
			// Only update currentEra when we successfully
			// resolved it. Writing a stale era alongside a
			// new epoch would leave currentEra inconsistent
			// with currentEpoch.
			if eraResolved {
				ls.currentEra = newCurrentEra
			}
		} else {
			// Genesis rollback: all epochs deleted, reset
			// to zero-value so epoch boundary detection
			// triggers correctly on re-sync. Zero currentEra
			// and pparams too so they stay consistent with
			// the zeroed epoch.
			ls.currentEpoch = models.Epoch{}
			ls.currentEra = eras.EraDesc{}
			ls.currentPParams = nil
			ls.prevEraPParams = nil
		}
	}
	if newPParams != nil {
		ls.currentPParams = newPParams
		ls.prevEraPParams = newPrevPParams
	}
	ls.lastLocalRollbackSeq++
	ls.lastLocalRollbackPoint = ocommon.Point{
		Slot: point.Slot,
		Hash: append([]byte(nil), point.Hash...),
	}
	ls.currentTip = newTip
	// A rollback invalidates any pending TransitionKnown because the
	// epoch-rollover block that set it may no longer be on the chain.
	// After the reset, re-derive what the rolled-back state implies:
	//
	//   - reconstructTransitionInfo restores Known(currentEpoch) when
	//     the post-rollback pparams already carry a major-version
	//     bump that the rollback didn't undo (the rolled-back chain
	//     still has the bump committed at an earlier point).
	//   - evaluateHardForkInitiationStability restores Known(N+1) if
	//     a HardForkInitiation governance action survived the
	//     rollback and is still ratifiable past the voting deadline.
	//
	// These re-derivations are best-effort at rollback time: if an
	// HFI stability tally is already in flight, the generation bump
	// below invalidates that result and the fresh tally may be
	// deferred until the next block reopens the evaluator. That can
	// leave a transient Unknown between rollback completion and the
	// next successful evaluation, but prevents stale pre-rollback HFI
	// results from committing.
	ls.transitionInfo = hardfork.NewTransitionUnknown()
	ls.reconstructTransitionInfo()
	// Reopen the once-per-epoch gate so rollback can attempt to
	// re-derive transitionInfo, and bump the generation counter so any
	// in-flight tally from before the rollback drops its result instead
	// of committing stale data.
	ls.hfiEvalDoneEpoch = 0
	ls.hfiEvalGeneration.Add(1)
	ls.evaluateHardForkInitiationStability()
	// Always update nonce - clear it on genesis rollback, set
	// it otherwise
	ls.currentTipBlockNonce = newNonce
	// Allow the nonce-ready event to be emitted again if replay crosses
	// the cutoff on a different fork after rollback.
	ls.resetNextEpochNonceReady()
	ls.updateTipMetrics(newTipDensity)
	ls.Unlock()
	if ls.config.EventBus != nil {
		ls.config.EventBus.Publish(
			event.ChainsyncResyncEventType,
			event.NewEvent(
				event.ChainsyncResyncEventType,
				event.ChainsyncResyncEvent{
					Reason: event.ChainsyncResyncReasonLocalLedgerRollback,
					Point:  point,
				},
			),
		)
	}
	var hash string
	if point.Slot == 0 {
		hash = "<genesis>"
	} else {
		hash = hex.EncodeToString(point.Hash)
	}
	ls.config.Logger.Info(
		fmt.Sprintf(
			"chain rolled back, new tip: %s at slot %d",
			hash,
			point.Slot,
		),
		"component",
		"ledger",
	)
	return nil
}

// rollbackChainAndState rewinds the primary chain and then synchronizes the
// metadata-backed ledger state to the same point.
func (ls *LedgerState) rollbackChainAndState(point ocommon.Point) error {
	ls.RLock()
	mithrilLedgerSlot := ls.mithrilLedgerSlot
	ls.RUnlock()
	if mithrilLedgerSlot > 0 && point.Slot < mithrilLedgerSlot {
		return ErrRollbackExceedsMithrilBoundary
	}
	if err := ls.chain.Rollback(point); err != nil {
		return err
	}
	if err := ls.rollback(point); err != nil {
		return fmt.Errorf("synchronize ledger rollback state: %w", err)
	}
	return nil
}

// processChainIteratorRollback applies a rollback emitted by the primary chain
// iterator only when the chain still sits at that rollback point. Iterator
// rollbacks can lag behind live blockfetch/chainsync activity; if the primary
// chain has already re-extended past the rollback point, applying the rollback
// again would desynchronize metadata from the blob-backed chain. In that case
// we must restart the ledger pipeline so the chain iterator rewinds itself to
// the current metadata tip and resumes on the canonical chain instead of
// continuing from stale block indexes on the abandoned fork.
func (ls *LedgerState) processChainIteratorRollback(
	point ocommon.Point,
) error {
	chainTip := ls.chain.Tip()
	if chainTip.Point.Slot != point.Slot ||
		!bytes.Equal(chainTip.Point.Hash, point.Hash) {
		ls.config.Logger.Debug(
			"stale chain iterator rollback detected, restarting ledger pipeline",
			"component", "ledger",
			"rollback_slot", point.Slot,
			"rollback_hash", hex.EncodeToString(point.Hash),
			"chain_tip_slot", chainTip.Point.Slot,
			"chain_tip_hash", hex.EncodeToString(chainTip.Point.Hash),
		)
		return errRestartLedgerPipeline
	}
	ls.RLock()
	currentTip := ls.currentTip
	ls.RUnlock()
	if currentTip.Point.Slot == point.Slot &&
		bytes.Equal(currentTip.Point.Hash, point.Hash) {
		return nil
	}
	return ls.rollback(point)
}

// transitionToEra performs an era transition and returns the result without
// mutating LedgerState. This allows callers to capture the computed state in a
// transaction and apply it to in-memory state after the transaction commits.
// Parameters:
//   - txn: database transaction
//   - nextEraId: the target era ID to transition to
//   - startEpoch: the epoch at which the transition occurs
//   - addedSlot: the slot at which the transition occurs
//   - currentPParams: current protocol parameters (read-only input)
//
// Returns the new era and protocol parameters, or an error.
func (ls *LedgerState) transitionToEra(
	txn *database.Txn,
	nextEraId uint,
	startEpoch uint64,
	addedSlot uint64,
	currentPParams lcommon.ProtocolParameters,
) (*EraTransitionResult, error) {
	nextEraPtr, ok := ls.eraById(nextEraId)
	if !ok || nextEraPtr == nil {
		return nil, fmt.Errorf("unknown era ID %d", nextEraId)
	}
	nextEra := *nextEraPtr
	result := &EraTransitionResult{
		NewPParams: currentPParams,
		NewEra:     nextEra,
	}
	if nextEra.HardForkFunc != nil {
		fromEraId := ls.currentEra.Id
		// Perform hard fork
		// This generally means upgrading pparams from previous era
		newPParams, err := nextEra.HardForkFunc(
			ls.config.CardanoNodeConfig,
			currentPParams,
		)
		if err != nil {
			return nil, fmt.Errorf("hard fork failed: %w", err)
		}
		result.NewPParams = newPParams
		ls.config.Logger.Debug(
			"updated protocol params",
			"pparams",
			fmt.Sprintf("%#v", newPParams),
		)
		// Write pparams update to DB
		pparamsCbor, err := cbor.Encode(&newPParams)
		if err != nil {
			return nil, fmt.Errorf("failed to encode pparams: %w", err)
		}
		err = ls.db.SetPParams(
			pparamsCbor,
			addedSlot,
			startEpoch,
			nextEraId,
			txn,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to set pparams: %w", err)
		}
		if err := governance.TranslateRatifiedGovActions(
			ls.db,
			txn,
			fromEraId,
			nextEraId,
		); err != nil {
			return nil, fmt.Errorf("translate governance actions: %w", err)
		}
	}
	return result, nil
}

// applyEraTransition applies a single era-transition result to the in-memory
// LedgerState fields. Must be called while holding ls.Lock(), except during
// single-threaded startup before LedgerState is visible to concurrent readers.
//
// It unconditionally clears transitionInfo so that any pending
// TransitionKnown is consumed the moment the new era becomes active,
// regardless of whether an epoch rollover is also happening.  This makes
// the clearing self-contained: every code path that applies an
// EraTransitionResult (the epoch-rollover path, startup, or a future
// standalone era-transition path) gets the same behaviour without
// duplicating the clear-on-era-advance logic.
func (ls *LedgerState) applyEraTransition(result *EraTransitionResult) {
	// Preserve the pre-hard-fork pparams for era-1 TX validation.
	ls.prevEraPParams = ls.currentPParams
	ls.currentPParams = result.NewPParams
	ls.currentEra = result.NewEra
	// Any pending TransitionKnown is consumed: the new era is now active.
	ls.transitionInfo = hardfork.NewTransitionUnknown()
}

// IsAtTip reports whether the node has caught up to the chain tip at least
// once since boot. This is used to gate metrics that are only meaningful
// when processing live blocks (e.g., block delay CDF). Unlike
// validationEnabled (which starts true when ValidateHistorical is set),
// reachedTip only flips when the node actually reaches the stability window.
func (ls *LedgerState) IsAtTip() bool {
	ls.RLock()
	defer ls.RUnlock()
	return ls.reachedTip
}

// calculateStabilityWindow returns the stability window based on the current era.
// For Byron era, returns 2k. For Shelley+ eras, returns 3k/f.
// Returns the default threshold if genesis data is unavailable or invalid.
func (ls *LedgerState) calculateStabilityWindow() uint64 {
	ls.RLock()
	eraId := ls.currentEra.Id
	ls.RUnlock()
	return ls.calculateStabilityWindowForEra(eraId)
}

// calculateStabilityWindowForEra calculates the stability window for the given era.
// This pure version takes the era ID as a parameter to avoid data races.
func (ls *LedgerState) calculateStabilityWindowForEra(eraId uint) uint64 {
	if ls.config.CardanoNodeConfig == nil {
		ls.config.Logger.Warn(
			"cardano node config is nil, using default stability window",
		)
		return blockfetchBatchSlotThresholdDefault
	}

	// Byron era only needs Byron genesis
	if eraId == 0 {
		byronGenesis := ls.config.CardanoNodeConfig.ByronGenesis()
		if byronGenesis == nil {
			return blockfetchBatchSlotThresholdDefault
		}
		k := byronGenesis.ProtocolConsts.K
		if k < 0 {
			ls.config.Logger.Warn("invalid negative security parameter", "k", k)
			return blockfetchBatchSlotThresholdDefault
		}
		if k == 0 {
			ls.config.Logger.Warn("security parameter is zero", "k", k)
			return blockfetchBatchSlotThresholdDefault
		}
		// Byron stability window is 2k slots
		return uint64(k) * 2 // #nosec G115
	}

	// Shelley+ eras only need Shelley genesis
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return blockfetchBatchSlotThresholdDefault
	}
	k := shelleyGenesis.SecurityParam
	if k < 0 {
		ls.config.Logger.Warn("invalid negative security parameter", "k", k)
		return blockfetchBatchSlotThresholdDefault
	}
	if k == 0 {
		ls.config.Logger.Warn("security parameter is zero", "k", k)
		return blockfetchBatchSlotThresholdDefault
	}
	securityParam := uint64(k)

	// Calculate 3k/f
	activeSlotsCoeff := shelleyGenesis.ActiveSlotsCoeff.Rat
	if activeSlotsCoeff == nil {
		ls.config.Logger.Warn("ActiveSlotsCoeff.Rat is nil")
		return blockfetchBatchSlotThresholdDefault
	}

	if activeSlotsCoeff.Num().Sign() <= 0 {
		ls.config.Logger.Warn(
			"ActiveSlotsCoeff must be positive",
			"active_slots_coeff",
			activeSlotsCoeff.String(),
		)
		return blockfetchBatchSlotThresholdDefault
	}

	numerator := new(big.Int).SetUint64(securityParam)
	numerator.Mul(numerator, big.NewInt(3))
	numerator.Mul(numerator, activeSlotsCoeff.Denom())
	denominator := new(big.Int).Set(activeSlotsCoeff.Num())
	window, remainder := new(
		big.Int,
	).QuoRem(numerator, denominator, new(big.Int))
	if remainder.Sign() != 0 {
		window.Add(window, big.NewInt(1))
	}
	if window.Sign() <= 0 {
		ls.config.Logger.Warn(
			"stability window calculation produced non-positive result",
			"security_param",
			securityParam,
			"active_slots_coeff",
			activeSlotsCoeff.String(),
		)
		return blockfetchBatchSlotThresholdDefault
	}
	if !window.IsUint64() {
		ls.config.Logger.Warn(
			"stability window calculation overflowed uint64",
			"security_param",
			securityParam,
			"active_slots_coeff",
			activeSlotsCoeff.String(),
			"window_num",
			window.String(),
		)
		return blockfetchBatchSlotThresholdDefault
	}
	return window.Uint64()
}

// CurrentTransitionInfo returns a snapshot of the current TransitionInfo
// under a read lock.  Callers must not hold ls.RLock() when calling this.
func (ls *LedgerState) CurrentTransitionInfo() hardfork.TransitionInfo {
	ls.RLock()
	ti := ls.transitionInfo
	ls.RUnlock()
	return ti
}

func (ls *LedgerState) securityParamForEra(eraId uint) (uint64, bool) {
	if ls.config.CardanoNodeConfig == nil {
		if ls.config.Logger != nil {
			ls.config.Logger.Warn(
				"CardanoNodeConfig is nil, security parameter unavailable",
			)
		}
		return 0, false
	}
	// Byron era only needs Byron genesis
	if eraId == 0 {
		byronGenesis := ls.config.CardanoNodeConfig.ByronGenesis()
		if byronGenesis == nil {
			return 0, false
		}
		k := byronGenesis.ProtocolConsts.K
		if k < 0 {
			if ls.config.Logger != nil {
				ls.config.Logger.Warn(
					"invalid negative security parameter",
					"k", k,
				)
			}
			return 0, false
		}
		if k == 0 {
			if ls.config.Logger != nil {
				ls.config.Logger.Warn("security parameter is zero", "k", k)
			}
			return 0, false
		}
		return uint64(k), true // #nosec G115
	}
	// Shelley+ eras only need Shelley genesis
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return 0, false
	}
	k := shelleyGenesis.SecurityParam
	if k < 0 {
		if ls.config.Logger != nil {
			ls.config.Logger.Warn(
				"invalid negative security parameter",
				"k", k,
			)
		}
		return 0, false
	}
	if k == 0 {
		if ls.config.Logger != nil {
			ls.config.Logger.Warn("security parameter is zero", "k", k)
		}
		return 0, false
	}
	return uint64(k), true
}

// SecurityParam returns the security parameter for the current era
func (ls *LedgerState) SecurityParam() int {
	return ls.securityParamForEraOrDefault(ls.currentEra.Id)
}

func (ls *LedgerState) securityParamForEraOrDefault(eraId uint) int {
	if k, ok := ls.securityParamForEra(eraId); ok {
		return int(k) // #nosec G115 -- k came from a non-negative int genesis field
	}
	return blockfetchBatchSlotThresholdDefault
}

func (ls *LedgerState) securityParamForCurrentEraSnapshot() int {
	ls.RLock()
	eraId := ls.currentEra.Id
	ls.RUnlock()
	return ls.securityParamForEraOrDefault(eraId)
}

// shouldSkipPhase2ValidationForBlock reports whether a block is deep enough
// behind the reference tip that its producer-supplied isValid flag can be
// trusted for replay-only Plutus Phase 2 results.
func (ls *LedgerState) shouldSkipPhase2ValidationForBlock(
	blockNumber uint64,
	referenceBlockNumber uint64,
	eraId uint,
) bool {
	securityParam, ok := ls.securityParamForEra(eraId)
	if !ok || referenceBlockNumber < securityParam {
		return false
	}
	immutableBlockNumber := referenceBlockNumber - securityParam
	return blockNumber <= immutableBlockNumber
}

// shouldSkipPhase2ValidationForBlockAtCurrentTip samples the primary chain tip
// for this specific block. The chain can advance or roll back while ledger
// processing drains a read batch, so callers must not reuse a sub-batch-start
// reference tip for all blocks in the transaction.
func (ls *LedgerState) shouldSkipPhase2ValidationForBlockAtCurrentTip(
	blockNumber uint64,
	eraId uint,
) bool {
	referenceTip := ls.chain.Tip()
	return ls.shouldSkipPhase2ValidationForBlock(
		blockNumber,
		referenceTip.BlockNumber,
		eraId,
	)
}

// StabilityWindow returns the Ouroboros security stability window for the
// current era in slots. For Byron the window is 2k; for Shelley+ it is 3k/f.
// It is safe to call from multiple goroutines.
func (ls *LedgerState) StabilityWindow() uint64 {
	return ls.calculateStabilityWindow()
}

type readChainResult struct {
	rollbackPoint ocommon.Point
	blocks        []ledger.Block
	rollback      bool
	done          chan struct{}
}

func trimReadBatchForRollback(
	nextBatch []ledger.Block,
	rollbackPoint ocommon.Point,
) ([]ledger.Block, bool) {
	for idx, block := range nextBatch {
		if block.SlotNumber() != rollbackPoint.Slot {
			continue
		}
		if !bytes.Equal(block.Hash().Bytes(), rollbackPoint.Hash) {
			continue
		}
		return nextBatch[:idx+1], false
	}
	return nil, true
}

type ledgerReadIterator interface {
	Next(blocking bool) (*chain.ChainIteratorResult, error)
}

func (ls *LedgerState) ledgerReadChain(
	ctx context.Context,
	resultCh chan readChainResult,
) {
	// Ensure the channel is closed when the reader exits for any
	// reason (error, context cancellation, iterator exhaustion).
	// Without this, the consumer blocks forever on the channel
	// read if the reader goroutine exits silently on an error.
	defer close(resultCh)
	const maxReconcileRetries = 3
	reconcileRetries := 0
	for {
		// Snapshot the current tip under lock to avoid a data race with
		// concurrent rollbacks that update ls.currentTip.
		ls.RLock()
		startPoint := ls.currentTip.Point
		ls.RUnlock()
		// Create chain iterator
		iter, err := ls.chain.FromPointContext(ctx, startPoint, false)
		if err != nil {
			if !errors.Is(err, models.ErrBlockNotFound) {
				ls.config.Logger.Warn(
					"failed to create chain iterator",
					"error", err,
					"start_slot", startPoint.Slot,
				)
				return
			}
			if reconcileRetries >= maxReconcileRetries {
				ls.config.Logger.Error(
					"exhausted ledger rollback retries for missing chain iterator start point",
					"error", err,
					"start_slot", startPoint.Slot,
					"start_hash", hex.EncodeToString(startPoint.Hash),
					"retries", reconcileRetries,
					"max_retries", maxReconcileRetries,
				)
				return
			}
			ls.config.Logger.Warn(
				"chain iterator start point not on chain, attempting ledger rollback",
				"error", err,
				"start_slot", startPoint.Slot,
				"start_hash", hex.EncodeToString(startPoint.Hash),
			)
			if reconcileErr := ls.reconcilePrimaryChainTipWithLedgerTip(); reconcileErr != nil {
				ls.config.Logger.Error(
					"failed to recover missing chain iterator start point",
					"error", reconcileErr,
					"start_slot", startPoint.Slot,
					"start_hash", hex.EncodeToString(startPoint.Hash),
				)
				return
			}
			reconcileRetries++
			ls.RLock()
			recoveredPoint := ls.currentTip.Point
			ls.RUnlock()
			if recoveredPoint.Slot == startPoint.Slot &&
				bytes.Equal(recoveredPoint.Hash, startPoint.Hash) {
				ls.config.Logger.Error(
					"ledger rollback did not change missing chain iterator start point",
					"start_slot", startPoint.Slot,
					"start_hash", hex.EncodeToString(startPoint.Hash),
				)
				return
			}
			continue
		}
		defer iter.Cancel()
		ls.ledgerReadChainIterator(ctx, iter, resultCh)
		return
	}
}

func (ls *LedgerState) ledgerReadChainIterator(
	ctx context.Context,
	iter ledgerReadIterator,
	resultCh chan readChainResult,
) {
	// Read blocks from chain iterator and decode
	var next, cachedNext *chain.ChainIteratorResult
	var tmpBlock ledger.Block
	var err error
	var shouldBlock bool
	var result readChainResult
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// We chose 500 as an arbitrary max batch size. A "chain extended" message will be logged after each batch
		nextBatch := make([]ledger.Block, 0, 500)
		// Gather up next batch of blocks
		for {
			if cachedNext != nil {
				next = cachedNext
				cachedNext = nil
			} else {
				next, err = iter.Next(shouldBlock)
				shouldBlock = false
				if err != nil {
					if !errors.Is(err, chain.ErrIteratorChainTip) {
						ls.config.Logger.Error(
							"failed to get next block from chain iterator: " + err.Error(),
						)
						return
					}
					shouldBlock = true
					// Break out of inner loop to flush DB transaction and log
					break
				}
			}
			if next == nil {
				ls.config.Logger.Error("next block from chain iterator is nil")
				return
			}
			if next.Rollback {
				if len(nextBatch) > 0 {
					trimmedBatch, emitRollback := trimReadBatchForRollback(
						nextBatch,
						next.Point,
					)
					if len(trimmedBatch) > 0 {
						nextBatch = trimmedBatch
						if emitRollback {
							cachedNext = next
						}
						break
					}
				}
				result = readChainResult{
					rollback:      true,
					rollbackPoint: next.Point,
					done:          make(chan struct{}),
				}
				break
			}
			// Decode block
			tmpBlock, err = next.Block.Decode()
			if err != nil {
				ls.config.Logger.Error(
					"failed to decode block: " + err.Error(),
				)
				return
			}
			// Add to batch
			nextBatch = append(
				nextBatch,
				tmpBlock,
			)
			// Don't exceed our pre-allocated capacity
			if len(nextBatch) == cap(nextBatch) {
				break
			}
		}
		if !result.rollback {
			result = readChainResult{
				blocks: nextBatch,
				done:   make(chan struct{}),
			}
		}
		select {
		case resultCh <- result:
		case <-ctx.Done():
			return
		}
		select {
		case <-result.done:
		case <-ctx.Done():
			return
		}
		result = readChainResult{}
	}
}

func (ls *LedgerState) ledgerProcessBlocks(ctx context.Context) {
	for {
		attemptCtx, cancel := context.WithCancel(ctx)
		readChainResultCh := make(chan readChainResult)
		go ls.ledgerReadChain(attemptCtx, readChainResultCh)
		err := ls.ledgerProcessBlocksFromSource(
			attemptCtx,
			readChainResultCh,
		)
		cancel()
		if err == nil || ctx.Err() != nil {
			return
		}
		ls.handleLedgerProcessBlocksError(err)
	}
}

func (ls *LedgerState) handleLedgerProcessBlocksError(err error) {
	if errors.Is(err, errRestartLedgerPipeline) {
		return
	}
	if errors.Is(err, errHaltLedgerPipeline) {
		ls.config.Logger.Warn(
			"block processing hit persistent validation failure, restarting pipeline",
			"error", err,
		)
		return
	}
	ls.config.Logger.Warn(
		"block processing failed, restarting pipeline",
		"error", err,
	)
}

// ProcessTrustedBlockBatches processes already-decoded trusted block batches
// synchronously. This is used by immutable load so blocks can be replayed
// directly without first being reread from the chain store.
func (ls *LedgerState) ProcessTrustedBlockBatches(
	ctx context.Context,
	batches <-chan []ledger.Block,
) error {
	// Buffer of 1 so the forwarding goroutine can deposit one result
	// and exit even if ledgerProcessBlocksFromSource has already returned.
	readChainResultCh := make(chan readChainResult, 1)
	go func() {
		defer close(readChainResultCh)
		for {
			select {
			case <-ctx.Done():
				return
			case batch, ok := <-batches:
				if !ok {
					return
				}
				select {
				case readChainResultCh <- readChainResult{blocks: batch}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return ls.ledgerProcessBlocksFromSource(ctx, readChainResultCh)
}

func (ls *LedgerState) ledgerProcessBlocksFromSource(
	ctx context.Context,
	readChainResultCh <-chan readChainResult,
) error {
	// Enable bulk-load optimizations when syncing from behind
	var bulkOptimizer metadata.BulkLoadOptimizer
	bulkLoadActive := false
	ls.RLock()
	bulkLoadAllowed := !ls.validationEnabled && !ls.reachedTip
	ls.RUnlock()
	if bulkLoadAllowed {
		if opt, ok := ls.db.Metadata().(metadata.BulkLoadOptimizer); ok {
			if err := opt.SetBulkLoadPragmas(); err != nil {
				ls.config.Logger.Warn(
					"failed to set bulk-load pragmas",
					"error", err,
				)
			} else {
				bulkOptimizer = opt
				bulkLoadActive = true
				defer func() {
					if bulkLoadActive && bulkOptimizer != nil {
						if restoreErr := bulkOptimizer.RestoreNormalPragmas(); restoreErr != nil {
							ls.config.Logger.Error(
								"failed to restore normal pragmas on exit",
								"error", restoreErr,
							)
						}
					}
				}()
			}
		}
	}
	// Process blocks
	var nextEpochEraId uint
	var needsEpochRollover bool
	var end, i int
	var err error
	var nextBatch, cachedNextBatch []ledger.Block
	var currentReadResultDone chan struct{}
	var delta *LedgerDelta
	var deltaBatch *LedgerDeltaBatch
	completeReadResult := func() {
		if currentReadResultDone != nil {
			close(currentReadResultDone)
			currentReadResultDone = nil
		}
	}
	for {
		if needsEpochRollover {
			needsEpochRollover = false

			// Capture current state with read lock before the transaction.
			// This avoids holding ls.Lock() during SubmitAsyncDBTxn, which
			// would cause deadlock if PartialCommitError recovery tries to
			// re-acquire the lock.
			ls.RLock()
			snapshotEra := ls.currentEra
			snapshotEpoch := ls.currentEpoch
			snapshotPParams := ls.currentPParams
			ls.RUnlock()

			var rolloverResult *EpochRolloverResult
			var eraTransitions []*EraTransitionResult

			// Execute transaction WITHOUT holding ls.Lock()
			err := ls.SubmitAsyncDBTxn(func(txn *database.Txn) error {
				workingPParams := snapshotPParams
				workingEraId := snapshotEra.Id

				// Honor the era schedule when the boundary-crossing
				// block did not bump the era itself. The current
				// era's NextEraTrigger may pin the next transition
				// to a specific epoch; if we are crossing into (or
				// past) that epoch and the block we observed is
				// still in the prior era, transition anyway.
				// Without this, a sole producer that builds in the
				// prior era at the boundary would never advance,
				// since nextEpochEraId would equal snapshotEra.Id.
				newEpochId := snapshotEpoch.EpochId + 1
				if entry, ok := ls.eraShape().EraForID(snapshotEra.Id); ok &&
					entry.NextEraTrigger.Kind == hardfork.TriggerAtEpoch &&
					entry.NextEraTrigger.Epoch <= newEpochId &&
					nextEpochEraId == snapshotEra.Id {
					if _, ok := ls.eraById(snapshotEra.Id + 1); ok {
						nextEpochEraId = snapshotEra.Id + 1
					}
				}

				// Check for era change. The guard rejects multi-step
				// forward, backwards, and to/from-unknown-era cases:
				// each era boundary on a healthy chain is crossed by
				// a block from that era, so anything else implies a
				// non-conforming block, a malformed snapshot, or a
				// chain-selection bug. Allowing a multi-step jump
				// would silently apply each intermediate era's
				// HardForkFunc but skip per-era epoch-based events
				// that should have fired during the omitted span.
				if nextEpochEraId != snapshotEra.Id {
					if !ls.isValidEraAdvancement(
						snapshotEra.Id, nextEpochEraId,
					) {
						return fmt.Errorf(
							"refusing era advancement from %d to %d: "+
								"only single-step forward advancement to "+
								"a known era is permitted",
							snapshotEra.Id, nextEpochEraId,
						)
					}
					result, err := ls.transitionToEra(
						txn,
						nextEpochEraId,
						snapshotEpoch.EpochId,
						snapshotEpoch.StartSlot+uint64(
							snapshotEpoch.LengthInSlots,
						),
						workingPParams,
					)
					if err != nil {
						return err
					}
					workingPParams = result.NewPParams
					workingEraId = result.NewEra.Id
					eraTransitions = append(eraTransitions, result)
				}

				// Process epoch rollover
				workingEraPtr, ok := ls.eraById(workingEraId)
				if !ok {
					return fmt.Errorf("unknown era ID %d", workingEraId)
				}
				result, err := ls.processEpochRollover(
					txn,
					snapshotEpoch,
					*workingEraPtr,
					workingPParams,
				)
				if err != nil {
					return err
				}
				rolloverResult = result
				return nil
			}, true)
			if err != nil {
				return fmt.Errorf("process epoch rollover: %w", err)
			}

			// Apply in-memory state updates with brief lock after successful commit
			ls.Lock()
			for _, eraResult := range eraTransitions {
				// applyEraTransition also clears transitionInfo so that
				// TransitionKnown is consumed whenever the new era is active,
				// regardless of whether an epoch rollover is also happening.
				ls.applyEraTransition(eraResult)
			}
			if rolloverResult != nil {
				ls.epochCache = rolloverResult.NewEpochCache

				ls.currentEpoch = rolloverResult.NewCurrentEpoch
				ls.currentEra = rolloverResult.NewCurrentEra
				ls.currentPParams = rolloverResult.NewCurrentPParams
				ls.checkpointWrittenForEpoch = rolloverResult.CheckpointWrittenForEpoch
				ls.metrics.epochNum.Set(rolloverResult.NewEpochNum)
				// New epoch: any TransitionImpossible set for the previous
				// epoch is now stale.  Reset to Unknown so the tip-update
				// logic re-evaluates for the new epoch's horizon.
				// (applyEraTransition already handles the era-transition case.)
				if len(eraTransitions) == 0 {
					ls.transitionInfo = hardfork.NewTransitionUnknown()
				}
			}
			// Set TransitionKnown only when epoch rolled over in the old era
			// with a version bump AND no era transition already cleared it.
			// applyEraTransition (above) handles the clear for transitions.
			if len(eraTransitions) == 0 &&
				rolloverResult != nil &&
				rolloverResult.HardFork != nil {
				ls.transitionInfo = hardfork.NewTransitionKnown(
					rolloverResult.NewCurrentEpoch.EpochId,
				)
			}
			// Re-apply any TestXHardForkAtEpoch override. This matters both
			// when no eraTransitions/HardFork occurred (the rollover reset
			// transitionInfo to Unknown above) and when an era transition
			// advanced ls.currentEra to a new era whose own successor may
			// carry its own AtEpoch override.
			ls.evaluateTriggerAtEpoch()
			ls.Unlock()

			// Update scheduler (thread-safe, no lock needed)
			if rolloverResult != nil &&
				rolloverResult.SchedulerIntervalMs > 0 &&
				ls.Scheduler != nil {
				// nolint:gosec
				// The slot length will not exceed int64
				interval := time.Duration(
					rolloverResult.SchedulerIntervalMs,
				) * time.Millisecond
				if err := ls.Scheduler.ChangeInterval(interval); err != nil {
					ls.config.Logger.Warn(
						"failed to update scheduler interval",
						"error", err,
						"interval", interval,
					)
				}
			}

			// Emit epoch transition event (coordinated with slot clock)
			if rolloverResult != nil && ls.config.EventBus != nil {
				newEpochId := rolloverResult.NewCurrentEpoch.EpochId

				// Always emit block-based epoch transitions. Even if the
				// slot clock already emitted an event for this epoch, the
				// block-based event is needed because it fires AFTER the
				// epoch nonce has been computed. Subscribers (leader
				// election, snapshot manager) use drain logic to handle
				// duplicates, keeping only the latest event.
				if ls.slotClock != nil {
					ls.slotClock.MarkEpochEmitted(newEpochId)
				}
				{
					// Calculate snapshot slot (boundary - 1, or 0 if boundary is 0)
					snapshotSlot := rolloverResult.NewCurrentEpoch.StartSlot
					if snapshotSlot > 0 {
						snapshotSlot--
					}
					epochTransitionEvent := event.EpochTransitionEvent{
						PreviousEpoch:   snapshotEpoch.EpochId,
						NewEpoch:        newEpochId,
						BoundarySlot:    rolloverResult.NewCurrentEpoch.StartSlot,
						EpochNonce:      rolloverResult.NewCurrentEpoch.Nonce,
						ProtocolVersion: rolloverResult.NewCurrentEra.Id,
						SnapshotSlot:    snapshotSlot,
					}
					ls.config.EventBus.Publish(
						event.EpochTransitionEventType,
						event.NewEvent(
							event.EpochTransitionEventType,
							epochTransitionEvent,
						),
					)
					ls.config.Logger.Debug(
						"emitted block-based epoch transition event",
						"epoch",
						newEpochId,
						"boundary_slot",
						rolloverResult.NewCurrentEpoch.StartSlot,
					)
				}
			}

			// Emit hard fork event if a protocol version
			// change triggered an era transition.
			// Track emitted FromEra/ToEra so the
			// block-era-driven path below can skip
			// duplicates.
			var emittedHFFromEra, emittedHFToEra uint
			emittedHF := false
			if rolloverResult != nil &&
				rolloverResult.HardFork != nil &&
				ls.config.EventBus != nil {
				hf := rolloverResult.HardFork
				hfEvent := event.HardForkEvent{
					Slot: rolloverResult.
						NewCurrentEpoch.StartSlot,
					EpochNo: rolloverResult.
						NewCurrentEpoch.EpochId,
					FromEra:         hf.FromEra,
					ToEra:           hf.ToEra,
					OldMajorVersion: hf.OldVersion.Major,
					OldMinorVersion: hf.OldVersion.Minor,
					NewMajorVersion: hf.NewVersion.Major,
					NewMinorVersion: hf.NewVersion.Minor,
				}
				ls.config.EventBus.Publish(
					event.HardForkEventType,
					event.NewEvent(
						event.HardForkEventType,
						hfEvent,
					),
				)
				emittedHF = true
				emittedHFFromEra = hf.FromEra
				emittedHFToEra = hf.ToEra
				if !ls.config.TrustedReplay {
					ls.config.Logger.Info(
						"emitted hard fork event",
						"from_era", hf.FromEra,
						"to_era", hf.ToEra,
						"epoch",
						rolloverResult.NewCurrentEpoch.EpochId,
						"slot",
						rolloverResult.NewCurrentEpoch.StartSlot,
						"component", "ledger",
					)
				}
			}

			// Emit hard fork events for era transitions
			// triggered by block era changes, skipping
			// any transition already emitted by the
			// pparam path above.
			if len(eraTransitions) > 0 &&
				ls.config.EventBus != nil {
				prevEraId := snapshotEra.Id
				prevPParams := snapshotPParams
				for _, eraResult := range eraTransitions {
					// Skip if the pparam-driven path
					// already emitted this exact
					// FromEra -> ToEra transition
					if emittedHF &&
						prevEraId == emittedHFFromEra &&
						eraResult.NewEra.Id == emittedHFToEra {
						ls.config.Logger.Debug(
							"skipping duplicate "+
								"hard fork event "+
								"(already emitted "+
								"by pparam path)",
							"from_era", prevEraId,
							"to_era",
							eraResult.NewEra.Id,
							"component", "ledger",
						)
						prevEraId = eraResult.NewEra.Id
						prevPParams = eraResult.NewPParams
						continue
					}
					oldVer, oldErr := GetProtocolVersion(
						prevPParams,
					)
					newVer, newErr := GetProtocolVersion(
						eraResult.NewPParams,
					)
					if oldErr != nil {
						ls.config.Logger.Warn(
							"could not extract protocol "+
								"version from previous "+
								"era pparams, skipping "+
								"hard fork event",
							"error", oldErr,
							"pparams_type",
							fmt.Sprintf(
								"%T", prevPParams,
							),
							"component", "ledger",
						)
					}
					if newErr != nil {
						ls.config.Logger.Warn(
							"could not extract protocol "+
								"version from new era "+
								"pparams, skipping hard "+
								"fork event",
							"error", newErr,
							"pparams_type",
							fmt.Sprintf(
								"%T",
								eraResult.NewPParams,
							),
							"component", "ledger",
						)
					}
					if oldErr == nil && newErr == nil {
						hfEvent := event.HardForkEvent{
							Slot: snapshotEpoch.StartSlot +
								uint64(
									snapshotEpoch.LengthInSlots,
								),
							EpochNo:         snapshotEpoch.EpochId + 1,
							FromEra:         prevEraId,
							ToEra:           eraResult.NewEra.Id,
							OldMajorVersion: oldVer.Major,
							OldMinorVersion: oldVer.Minor,
							NewMajorVersion: newVer.Major,
							NewMinorVersion: newVer.Minor,
						}
						ls.config.EventBus.Publish(
							event.HardForkEventType,
							event.NewEvent(
								event.HardForkEventType,
								hfEvent,
							),
						)
						if !ls.config.TrustedReplay {
							ls.config.Logger.Info(
								"emitted hard fork event"+
									" (era transition)",
								"from_era", prevEraId,
								"to_era",
								eraResult.NewEra.Id,
								"component", "ledger",
							)
						}
					}
					prevEraId = eraResult.NewEra.Id
					prevPParams = eraResult.NewPParams
				}
			}

			// Start background cleanup goroutines
			go ls.cleanupConsumedUtxos()

			// Clean up old block nonces and keep only last 3 epochs along with checkpoints
			if rolloverResult != nil {
				var cutoffStart uint64
				if rolloverResult.NewCurrentEpoch.EpochId >= 4 {
					target := rolloverResult.NewCurrentEpoch.EpochId - 3
					for _, ep := range rolloverResult.NewEpochCache {
						if ep.EpochId == target {
							cutoffStart = ep.StartSlot
							break
						}
					}
				}
				if cutoffStart > 0 {
					// Run cleanup inline to avoid SQLITE_BUSY from concurrent goroutine writes
					ls.cleanupBlockNoncesBefore(cutoffStart)
				}
			}
		}
		if cachedNextBatch != nil {
			// Use cached block batch — keep the original
			// currentReadResultDone so the reader goroutine
			// is signalled when all cached blocks are processed.
			nextBatch = cachedNextBatch
			cachedNextBatch = nil
		} else {
			// Only reset when reading fresh from the channel
			currentReadResultDone = nil
			// Read next result from readChain channel
			select {
			case result, ok := <-readChainResultCh:
				if !ok {
					return nil
				}
				currentReadResultDone = result.done
				nextBatch = result.blocks
				// Process rollback
				// Note: We do NOT hold ls.Lock() here because rollback() calls
				// SubmitAsyncDBTxn() which may trigger PartialCommitError recovery
				// that re-acquires ls.Lock(), causing a deadlock. The rollback
				// method handles its own locking for in-memory state updates.
				if result.rollback {
					if err = ls.processChainIteratorRollback(
						result.rollbackPoint,
					); err != nil {
						completeReadResult()
						return fmt.Errorf("process rollback: %w", err)
					}
					completeReadResult()
					continue
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		// Process batch in groups of batchSize to stay under DB txn limits
		var tipForLog ochainsync.Tip
		var checker ForgedBlockChecker
		for i = 0; i < len(nextBatch); i += batchSize {
			end = min(
				len(nextBatch),
				i+batchSize,
			)

			// Leios: gate delivery of this chunk on the availability of the
			// endorser blocks its Dijkstra ranking blocks reference, so the
			// endorser transactions are applied ahead of the ranking blocks
			// that endorse them. Runs outside the DB transaction opened below
			// and is a no-op except at the chain tip.
			ls.ensureReferencedEndorserBlocks(ctx, nextBatch[i:end])

			// Capture snapshots of state needed during transaction.
			// Acquire read lock to prevent race with RecoverCommitTimestampConflict
			// which can trigger rollback() and loadTip() that mutate these fields.
			ls.RLock()
			snapshotEpoch := ls.currentEpoch
			snapshotEra := ls.currentEra
			snapshotPParams := ls.currentPParams
			snapshotPrevEraPParams := ls.prevEraPParams
			snapshotTipHash := ls.currentTip.Point.Hash
			snapshotNonce := ls.currentTipBlockNonce
			localCheckpointWritten := ls.checkpointWrittenForEpoch
			snapshotValidationEnabled := ls.validationEnabled
			snapshotChainsyncState := ls.chainsyncState
			chainTip := ls.chain.Tip()
			chainTipSlot := chainTip.Point.Slot
			snapshotMithrilSlot := ls.mithrilLedgerSlot
			ls.RUnlock()

			// Compute stability window and cutoff slot outside the callback
			// to avoid reading ls fields without the lock. Use the wall-clock
			// slot as a reference when it exceeds the local chain tip. When
			// syncing from genesis the local chain tip starts at 0, which
			// would make cutoffSlot 0 and validate ALL historical blocks.
			stabilityWindow := ls.calculateStabilityWindowForEra(snapshotEra.Id)
			referenceSlot := chainTipSlot
			if wallSlot, err := ls.CurrentSlot(); err == nil && wallSlot > referenceSlot {
				referenceSlot = wallSlot
			}
			var cutoffSlot uint64
			if referenceSlot >= stabilityWindow {
				cutoffSlot = referenceSlot - stabilityWindow
			}

			// Track pending state changes during transaction
			var pendingTip ochainsync.Tip
			var pendingNonce []byte
			var blocksProcessed int
			runningNonce := snapshotNonce
			// Track expected previous hash for batch processing - updated after each block
			expectedPrevHash := snapshotTipHash
			// Flag to enable validation after transaction commits (set inside callback,
			// applied after commit to avoid mutating in-memory state on txn failure)
			var wantEnableValidation bool

			err = ls.SubmitAsyncDBTxn(func(txn *database.Txn) error {
				deltaBatch = NewLedgerDeltaBatch()
				for offset, next := range nextBatch[i:end] {
					tmpPoint := ocommon.Point{
						Slot: next.SlotNumber(),
						Hash: next.Hash().Bytes(),
					}
					// End processing of batch and cache remainder if we get a block from after the current epoch end, or if we need the initial epoch
					if tmpPoint.Slot >= (snapshotEpoch.StartSlot+uint64(snapshotEpoch.LengthInSlots)) ||
						snapshotEpoch.SlotLength == 0 {
						needsEpochRollover = true
						nextEpochEraId = uint(next.Era().Id)
						// Cache rest of the batch for next loop
						cachedNextBatch = nextBatch[i+offset:]
						nextBatch = nil
						break
					}
					// Determine if this block should be validated.
					// Skip validation of historical blocks when
					// ValidateHistorical=false, as they were already
					// validated by the network. Validate blocks within
					// the stability window near the tip.
					var shouldValidateBlock bool
					if snapshotValidationEnabled {
						shouldValidateBlock = true
						// When validation was already enabled from
						// config (ValidateHistorical), we still need
						// to detect reaching the chain tip for
						// metrics gating (IsAtTip).
						if !wantEnableValidation &&
							snapshotChainsyncState == SyncingChainsyncState &&
							next.SlotNumber() >= cutoffSlot {
							wantEnableValidation = true
						}
					} else if !ls.config.TrustedReplay &&
						snapshotChainsyncState == SyncingChainsyncState &&
						next.SlotNumber() >= cutoffSlot {
						// Do not validate blocks covered by the
						// Mithril snapshot. These were already
						// verified by the certificate chain during
						// import; re-validating them fails because
						// the UTxO set from the snapshot does not
						// contain intermediate states for volatile
						// blocks fetched during gap closure.
						if snapshotMithrilSlot > 0 &&
							next.SlotNumber() <= snapshotMithrilSlot {
							// Still within Mithril trust boundary —
							// skip validation but mark that we've
							// reached the tip region so catch-up mode
							// and bulk-load pragmas are disabled.
							wantEnableValidation = true
						} else {
							wantEnableValidation = true
							shouldValidateBlock = true
						}
					}
					// Flush accumulated deltas before the first validated
					// block so that UTxOs created by earlier non-validated
					// blocks are visible during validation lookups.
					if shouldValidateBlock && len(deltaBatch.deltas) > 0 {
						if err := deltaBatch.apply(ls, txn); err != nil {
							deltaBatch.Release()
							return err
						}
						deltaBatch.Release()
						deltaBatch = NewLedgerDeltaBatch()
					}
					// Compute CBOR offsets for this block (required for transaction storage)
					var blockOffsets *database.BlockIngestionResult
					blockCbor := next.Cbor()
					if len(next.Transactions()) > 0 && len(blockCbor) == 0 {
						deltaBatch.Release()
						return fmt.Errorf(
							"block at slot %d hash %x has %d transactions but no CBOR data",
							tmpPoint.Slot,
							tmpPoint.Hash,
							len(next.Transactions()),
						)
					}
					if len(blockCbor) > 0 && len(next.Transactions()) > 0 {
						indexer := database.NewBlockIndexer(
							tmpPoint.Slot,
							tmpPoint.Hash,
						)
						var offsetErr error
						blockOffsets, offsetErr = indexer.ComputeOffsets(
							blockCbor,
							next,
						)
						if offsetErr != nil {
							deltaBatch.Release()
							return fmt.Errorf(
								"compute CBOR offsets for block at slot %d: %w",
								tmpPoint.Slot,
								offsetErr,
							)
						}
					}
					// Skip full block processing for blocks
					// already handled during Mithril gap closure.
					// Their transaction metadata was recorded by
					// SetGapBlockTransaction; re-processing them
					// via SetTransaction would fail with "UTxO
					// already spent" since the Mithril snapshot's
					// UTxO set already reflects the spent state.
					if snapshotMithrilSlot > 0 &&
						tmpPoint.Slot <= snapshotMithrilSlot {
						// Load stored nonce so the rolling nonce
						// stays correct across the gap boundary.
						if storedNonce, nonceErr := ls.db.GetBlockNonce(
							tmpPoint, txn,
						); nonceErr == nil && len(storedNonce) > 0 {
							runningNonce = storedNonce
							pendingNonce = storedNonce
						}
						pendingTip = ochainsync.Tip{
							Point:       tmpPoint,
							BlockNumber: next.BlockNumber(),
						}
						expectedPrevHash = tmpPoint.Hash
						blocksProcessed++
						continue
					}
					// Process block
					skipPhase2Validation := shouldValidateBlock &&
						ls.shouldSkipPhase2ValidationForBlockAtCurrentTip(
							next.BlockNumber(),
							snapshotEra.Id,
						)
					delta, err = ls.ledgerProcessBlock(
						txn,
						tmpPoint,
						next,
						shouldValidateBlock,
						skipPhase2Validation,
						expectedPrevHash,
						blockOffsets,
						snapshotEra,
						snapshotPParams,
						snapshotPrevEraPParams,
					)
					if err != nil {
						deltaBatch.Release()
						return err
					}
					if delta != nil {
						deltaBatch.addDelta(delta)
					}
					// Update expected prev hash for next block in batch
					expectedPrevHash = tmpPoint.Hash
					// Track pending tip (will be committed after txn succeeds)
					pendingTip = ochainsync.Tip{
						Point:       tmpPoint,
						BlockNumber: next.BlockNumber(),
					}
					blocksProcessed++
					// Calculate block rolling nonce (evolving nonce η_v).
					// The evolving nonce is ALWAYS computed for every block.
					// The candidate nonce (used in epoch nonce calc) is
					// computed by iterating blocks from the blob store
					// up to the stability window cutoff.
					var blockNonce []byte
					if snapshotEra.CalculateEtaVFunc != nil {
						tmpEra, ok := ls.eraById(uint(next.Era().Id))
						if ok && tmpEra != nil && tmpEra.CalculateEtaVFunc != nil {
							tmpNonce, err := tmpEra.CalculateEtaVFunc(
								ls.config.CardanoNodeConfig,
								runningNonce,
								next,
							)
							if err != nil {
								deltaBatch.Release()
								return fmt.Errorf("calculate etaV: %w", err)
							}
							blockNonce = tmpNonce
							runningNonce = tmpNonce
						}
					}
					// The loop exits before processing blocks from the next
					// epoch, so every block that reaches this point belongs
					// to snapshotEpoch.
					// First block we persist in the current epoch becomes the checkpoint.
					isCheckpoint := false
					if !localCheckpointWritten {
						isCheckpoint = true
						localCheckpointWritten = true
					}
					// Store block nonce in the DB
					if len(blockNonce) > 0 {
						err = ls.db.SetBlockNonce(
							tmpPoint.Hash,
							tmpPoint.Slot,
							blockNonce,
							isCheckpoint,
							txn,
						)
						if err != nil {
							deltaBatch.Release()
							return err
						}
						// Track pending nonce (will be committed after txn succeeds)
						pendingNonce = blockNonce
					}
				}
				// Apply delta batch
				if err := deltaBatch.apply(ls, txn); err != nil {
					deltaBatch.Release()
					return err
				}
				deltaBatch.Release()
				// Update tip in database only if blocks were processed
				if blocksProcessed > 0 {
					if err := ls.db.SetTip(pendingTip, txn); err != nil {
						return fmt.Errorf("failed to set tip: %w", err)
					}
				}
				return nil
			}, true)
			if err != nil {
				recovered, recoverErr := ls.tryRecoverFromTxValidationError(
					err,
				)
				if recoverErr != nil {
					completeReadResult()
					return fmt.Errorf(
						"process block batch: %w",
						recoverErr,
					)
				}
				if recovered {
					completeReadResult()
					return errRestartLedgerPipeline
				}
				// A stale chain iterator delivers a fork block whose prev-hash
				// doesn't match our current tip. This happens when a concurrent
				// rollback moved the chain behind the iterator's position: the
				// iterator skips the first fork block and reads one that extends
				// a branch we are no longer on. Restarting the pipeline creates
				// a fresh iterator from the (already rolled-back) currentTip,
				// which will walk all fork blocks in order.
				if errors.Is(err, errStaleChainIterator) {
					ls.config.Logger.Debug(
						"stale chain iterator detected, restarting pipeline to resync",
						"component", "ledger",
						"error", err,
					)
					completeReadResult()
					return errRestartLedgerPipeline
				}
				completeReadResult()
				return fmt.Errorf("process block batch: %w", err)
			}
			// Transaction committed successfully - now update in-memory state.
			// Only update if blocks were actually processed to avoid resetting tip to zero.
			if blocksProcessed > 0 {
				tipDensity := ls.chainFragmentDensity(
					pendingTip,
					ls.securityParamForCurrentEraSnapshot(),
				)
				// Brief lock to ensure readers see consistent state.
				ls.Lock()
				ls.currentTip = pendingTip
				if len(pendingNonce) > 0 {
					ls.currentTipBlockNonce = pendingNonce
				}
				ls.checkpointWrittenForEpoch = localCheckpointWritten
				if wantEnableValidation {
					ls.validationEnabled = true
					ls.reachedTip = true
				}
				ls.updateTipMetrics(tipDensity)
				// After advancing the tip, first honor any TestXHardForkAtEpoch
				// override so queries surface the pinned epoch ahead of time;
				// then check whether the stability window reaches or exceeds
				// the epoch end, in which case a hard fork cannot happen
				// within this epoch and TransitionImpossible should be
				// recorded so queryHardForkEraHistory serves the confirmed
				// epoch-end slot instead of a stale safeZone cap.
				ls.evaluateTriggerAtEpoch()
				ls.evaluateTransitionImpossible()
				ls.evaluateHardForkInitiationStability()
				// Capture tip for logging while holding the lock
				tipForLog = ls.currentTip
				checker = ls.config.ForgedBlockChecker
				ls.Unlock()
				// Restore normal DB options outside the lock after validation is enabled
				if wantEnableValidation && bulkLoadActive && bulkOptimizer != nil {
					if restoreErr := bulkOptimizer.RestoreNormalPragmas(); restoreErr != nil {
						ls.config.Logger.Error(
							"failed to restore normal pragmas",
							"error", restoreErr,
						)
					} else {
						bulkLoadActive = false
					}
				}
			}
			if needsEpochRollover {
				break
			}
		}
		if len(nextBatch) > 0 {
			if !ls.config.TrustedReplay {
				// Determine block source for observability
				source := "chainsync"
				if checker != nil {
					if _, forged := checker.WasForgedByUs(
						tipForLog.Point.Slot,
					); forged {
						source = "forged"
					}
				}
				ls.config.Logger.Info(
					fmt.Sprintf(
						"chain extended, new tip: %x at slot %d",
						tipForLog.Point.Hash,
						tipForLog.Point.Slot,
					),
					"component",
					"ledger",
					"source",
					source,
				)
			}
			// Periodic sync progress reporting
			ls.logSyncProgress(tipForLog.Point.Slot)
		}
		completeReadResult()
	}
}

func (ls *LedgerState) ledgerProcessBlock(
	txn *database.Txn,
	point ocommon.Point,
	block ledger.Block,
	shouldValidate bool,
	skipPhase2Validation bool,
	expectedPrevHash []byte,
	offsets *database.BlockIngestionResult,
	currentEra eras.EraDesc,
	pparams lcommon.ProtocolParameters,
	prevEraPParams lcommon.ProtocolParameters,
) (*LedgerDelta, error) {
	// Check that we're processing things in order
	if len(expectedPrevHash) > 0 {
		if string(
			block.PrevHash().Bytes(),
		) != string(
			expectedPrevHash,
		) {
			return nil, fmt.Errorf(
				"%w: block %s (with prev hash %s) does not fit on current chain tip (%x)",
				errStaleChainIterator,
				block.Hash().String(),
				block.PrevHash().String(),
				expectedPrevHash,
			)
		}
	}
	// Enforce configured chain checkpoints regardless of validation mode.
	// A block at a checkpointed height whose hash differs sits on a chain
	// that diverges from the known-good chain, so reject it before doing
	// any further work. Honest chains always agree with the checkpoints.
	if err := ls.validateBlockCheckpoint(block); err != nil {
		return nil, err
	}
	// Reject blocks whose header protocol major version runs more than
	// one ahead of current pparams. Skipped on testnets pre-Dijkstra
	// per cardano-ledger PR 5785.
	if shouldValidate {
		if err := ls.validateBlockHeaderProtocolVersion(
			block.Header(), pparams,
		); err != nil {
			return nil, err
		}
	}
	// Apply the referenced Leios endorser block's transactions before the
	// ranking block's own, so the endorser-resident outputs the ranking block
	// spends are present in the UTxO set. The endorser block is identified by
	// the Dijkstra Leios header extension; its fetched transactions are
	// supplied by the EndorserBlockProvider. Decode/build failures remain
	// best-effort: they leave the interim validation skip in place rather than
	// aborting the block. Storage-phase failures abort the DB transaction so a
	// partial endorser-block application cannot be committed.
	endorserBlockApplied := false
	if currentEra.Id == dijkstra.EraIdDijkstra &&
		ls.config.EndorserBlockProvider != nil {
		if ref, ok := block.Header().(leiosEndorserBlockReferencer); ok {
			if ebHash, ebSize, ok := ref.LeiosEndorserBlockRef(); ok {
				if ebSlot, ebTxs, ok := ls.config.EndorserBlockProvider(
					ebHash.Bytes(),
				); ok {
					applied, err := ls.applyEndorserBlock(
						txn,
						point,
						block.BlockNumber(),
						ebSlot,
						ebHash.Bytes(),
						ebTxs,
					)
					var storageErr *leiosEndorserBlockStorageError
					switch {
					case errors.As(err, &storageErr):
						ls.config.Logger.Warn(
							"failed to apply Leios endorser block after storage mutation",
							"component", "ledger",
							"slot", point.Slot,
							"eb_slot", ebSlot,
							"error", err,
						)
						return nil, err
					case err != nil:
						ls.config.Logger.Warn(
							"failed to apply Leios endorser block transactions",
							"component", "ledger",
							"slot", point.Slot,
							"eb_slot", ebSlot,
							"error", err,
						)
					default:
						endorserBlockApplied = true
						ls.config.Logger.Info(
							"applied Leios endorser block transactions",
							"component", "ledger",
							"slot", point.Slot,
							"eb_slot", ebSlot,
							"eb_txs", applied,
						)
					}
				} else {
					ls.config.Logger.Debug(
						"ranking block references an endorser block not yet cached",
						"component", "ledger",
						"slot", point.Slot,
						"eb_hash", ebHash.String(),
						"eb_size", ebSize,
					)
				}
			} else {
				ls.config.Logger.Debug(
					"dijkstra block has no Leios endorser-block reference",
					"component", "ledger",
					"slot", point.Slot,
				)
			}
		} else {
			ls.config.Logger.Debug(
				"dijkstra block header is not a Leios endorser-block referencer",
				"component", "ledger",
				"slot", point.Slot,
			)
		}
	}
	// Process transactions
	var delta *LedgerDelta
	// Track outputs from earlier transactions in this block for intra-block
	// dependencies only when TX validation is enabled.
	intraBlockUtxos := make(map[string]lcommon.Utxo)
	for i, tx := range block.Transactions() {
		if delta == nil {
			delta = NewLedgerDelta(
				point,
				uint(block.Era().Id),
				block.BlockNumber(),
			)
			delta.Offsets = offsets
		}
		// Validate transaction
		// Skip validation for phase-2 failed TXs (isValid=false).
		// These are consensus-valid: the block producer already
		// determined the script failure, collateral is consumed
		// instead of regular inputs, and tx.Consumed()/Produced()
		// return the correct collateral-based UTxO sets.
		if shouldValidate && tx.IsValid() {
			validationEra, err := resolveValidationEra(
				tx,
				currentEra,
				ls.eraList(),
			)
			if err != nil {
				delta.Release()
				return nil, err
			}
			// Dijkstra/Leios: per-tx UTxO validation is run only when the
			// ranking block's endorser block was applied above, so the
			// endorser-resident inputs these txs spend are now present in the
			// UTxO set and the rules can resolve them. When the endorser block
			// was not applied — e.g. it has not been fetched, or the prototype
			// does not diffuse it (historical endorser blocks) — validation is
			// skipped: the rules could only fail (BadInputsUtxo /
			// ValueNotConserved), the network already admitted the block via
			// its Leios certificate, and running the full rule set per tx on
			// dense near-tip blocks otherwise holds throughput down to the
			// block-production rate and prevents convergence. A validation
			// disagreement on an endorser-applied block is trusted (logged)
			// below rather than rewinding, since the prototype's
			// endorser-block availability and certificate surface are still
			// evolving.
			skipDijkstraValidation := validationEra.Id == dijkstra.EraIdDijkstra &&
				!endorserBlockApplied
			if validationEra.ValidateTxFunc != nil && !skipDijkstraValidation {
				// Use the previous era's protocol
				// parameters when validating an era-1
				// transaction.
				pp := pparams
				if validationEra.Id != currentEra.Id &&
					prevEraPParams != nil {
					pp = prevEraPParams
				}
				lv := &LedgerView{
					txn:                  txn,
					ls:                   ls,
					intraBlockUtxos:      intraBlockUtxos,
					skipPhase2Validation: skipPhase2Validation,
				}
				err := validationEra.ValidateTxFunc(
					tx,
					point.Slot,
					lv,
					pp,
				)
				// When a TX has isValid=true, the block producer's
				// Plutus evaluator verified the script passed. If our
				// evaluator disagrees, the fault is in our VM (known
				// gouroboros CEK machine limitations), not in the block.
				// Log the disagreement but trust the block producer.
				var plutusErr conway.PlutusScriptFailedError
				if err != nil && errors.As(err, &plutusErr) {
					ls.config.Logger.Warn(
						"Plutus evaluation disagrees with block producer (trusting isValid=true)",
						"component", "ledger",
						"tx_hash", tx.Hash().String(),
						"block_slot", point.Slot,
						"script_hash", hex.EncodeToString(plutusErr.ScriptHash[:]),
						"redeemer_tag", plutusErr.Tag,
						"redeemer_index", plutusErr.Index,
						"eval_error", plutusErr.Err.Error(),
					)
					err = nil
				}
				// Dijkstra/Leios: the block was admitted to the chain via its
				// Leios certificate. An endorser block may be only partially
				// resolvable (e.g. an endorser-resident input from an endorser
				// block the prototype did not diffuse), and the prototype's
				// certificate/validation surface is still evolving, so a
				// remaining validation disagreement is logged and trusted
				// rather than rewinding the certified chain. Tracked by #2587:
				// tighten to enforce once endorser-block availability and
				// Leios certificate validation are complete.
				if err != nil &&
					validationEra.Id == dijkstra.EraIdDijkstra {
					ls.config.Logger.Warn(
						"Dijkstra tx validation disagreement (trusting Leios-certified block)",
						"component", "ledger",
						"tx_hash", tx.Hash().String(),
						"block_slot", point.Slot,
						"error", err.Error(),
					)
					err = nil
				}
				if err != nil {
					// Attempt to include raw CBOR for diagnostics (if available)
					var txCborHex string
					txCbor := tx.Cbor()
					if len(txCbor) > 0 {
						txCborHex = hex.EncodeToString(txCbor)
					}
					var bodyCborHex string
					var witnessCborHex string
					var auxCborHex string
					if len(txCbor) > 0 {
						var txArray []cbor.RawMessage
						if _, err := cbor.Decode(txCbor, &txArray); err == nil &&
							len(txArray) >= 3 {
							if len(txArray[0]) > 0 {
								bodyCborHex = hex.EncodeToString(
									[]byte(txArray[0]),
								)
							}
							if len(txArray[1]) > 0 {
								witnessCborHex = hex.EncodeToString(
									[]byte(txArray[1]),
								)
							}
							// Filter placeholders (0xF4 false, 0xF5 true, 0xF6 null)
							if len(txArray[2]) > 0 && txArray[2][0] != 0xF4 &&
								txArray[2][0] != 0xF5 &&
								txArray[2][0] != 0xF6 {
								auxCborHex = hex.EncodeToString(
									[]byte(txArray[2]),
								)
							}
						}
					} else {
						if aux := tx.AuxiliaryData(); aux != nil {
							if ac := aux.Cbor(); len(ac) > 0 {
								auxCborHex = hex.EncodeToString(ac)
							}
						}
					}
					ls.config.Logger.Warn(
						"TX "+tx.Hash().
							String()+
							" failed validation: "+err.Error(),
						"tx_cbor_hex",
						txCborHex,
						"body_cbor_hex",
						bodyCborHex,
						"witness_cbor_hex",
						witnessCborHex,
						"aux_cbor_hex",
						auxCborHex,
					)
					delta.Release()
					return nil, &txValidationError{
						BlockPoint: point,
						TxHash: append(
							[]byte(nil),
							tx.Hash().Bytes()...,
						),
						Inputs: collectReferencedInputs(tx),
						Cause:  err,
					}
				}
			}
		}
		// Populate ledger delta from transaction
		delta.addTransaction(tx, i)

		// Apply delta immediately if we may need the data to validate the next TX
		if shouldValidate {
			if err := delta.apply(ls, txn); err != nil {
				delta.Release()
				return nil, err
			}
			delta.Release()
			delta = nil // reset

			// Add this transaction's outputs to intra-block map for subsequent TX lookups
			// Use tx.Produced() instead of tx.Outputs() to handle failed transactions
			// correctly - for failed TXs, Produced() returns collateral return at the
			// correct index (len(Outputs())), while Outputs() returns regular outputs
			for _, utxo := range tx.Produced() {
				key := fmt.Sprintf(
					"%s:%d",
					utxo.Id.Id().String(),
					utxo.Id.Index(),
				)
				intraBlockUtxos[key] = utxo
			}
		}
	}
	if seqNum, ok := opCertSequenceNumber(block); ok {
		issuerVkey := block.IssuerVkey()
		poolKeyHash := lcommon.PoolKeyHash(issuerVkey.Hash())
		if err := ls.db.UpdatePoolOpCertSequence(
			poolKeyHash,
			uint64(seqNum),
			point.Slot,
			txn,
		); err != nil {
			if delta != nil {
				delta.Release()
			}
			return nil, err
		}
	}
	return delta, nil
}

func opCertSequenceNumber(block ledger.Block) (uint32, bool) {
	switch header := block.Header().(type) {
	case *dijkstra.DijkstraBlockHeader:
		// Distinct concrete type embedding BabbageBlockHeader; a type
		// switch won't fall through to the Babbage case, so it needs an
		// explicit entry or per-pool OpCert sequence tracking is skipped
		// for Dijkstra-era blocks.
		return header.Body.OpCert.SequenceNumber, true
	case *shelley.ShelleyBlockHeader:
		return header.Body.OpCertSequenceNumber, true
	case *allegra.AllegraBlockHeader:
		return header.Body.OpCertSequenceNumber, true
	case *mary.MaryBlockHeader:
		return header.Body.OpCertSequenceNumber, true
	case *alonzo.AlonzoBlockHeader:
		return header.Body.OpCertSequenceNumber, true
	case *babbage.BabbageBlockHeader:
		return header.Body.OpCert.SequenceNumber, true
	case *conway.ConwayBlockHeader:
		return header.Body.OpCert.SequenceNumber, true
	default:
		return 0, false
	}
}

// updateTipMetrics updates gauges from in-memory state. Call under ls.Lock().
// The density value must be computed before taking the lock because fragment
// density can require database lookups.
func (ls *LedgerState) updateTipMetrics(density float64) {
	ls.metrics.blockNum.Set(float64(ls.currentTip.BlockNumber))
	ls.metrics.slotNum.Set(float64(ls.currentTip.Point.Slot))
	ls.metrics.slotInEpoch.Set(
		float64(ls.currentTip.Point.Slot - ls.currentEpoch.StartSlot),
	)
	ls.metrics.density.Set(density)
}

// chainFragmentDensity matches cardano-node's ChainDB fragment density:
// block delta divided by slot delta over the selected chain fragment.
func (ls *LedgerState) chainFragmentDensity(
	tip ochainsync.Tip,
	securityParam int,
) float64 {
	tipSlot := tip.Point.Slot
	tipBlockNum := tip.BlockNumber
	if tipSlot == 0 || tipBlockNum == 0 {
		return 0
	}
	if ls.db == nil {
		return totalChainDensity(tipSlot, tipBlockNum)
	}
	if securityParam <= 0 {
		return totalChainDensity(tipSlot, tipBlockNum)
	}
	tipBlock, err := database.BlockByPoint(ls.db, tip.Point)
	if err != nil {
		return totalChainDensity(tipSlot, tipBlockNum)
	}
	oldestIndex := database.BlockInitialIndex
	if tipBlock.ID > uint64(securityParam) {
		oldestIndex = tipBlock.ID - uint64(securityParam)
	}
	oldestBlock, err := ls.db.BlockByIndex(oldestIndex, nil)
	if err != nil {
		return totalChainDensity(tipSlot, tipBlockNum)
	}
	return fragmentDensity(
		tipBlock.Slot,
		tipBlock.Number,
		oldestBlock.Slot,
		oldestBlock.Number,
	)
}

func totalChainDensity(tipSlot, tipBlockNum uint64) float64 {
	if tipSlot == 0 {
		return 0
	}
	return float64(tipBlockNum) / float64(tipSlot)
}

func fragmentDensity(
	tipSlot, tipBlockNum, oldestSlot, oldestBlockNum uint64,
) float64 {
	if tipSlot <= oldestSlot {
		return 0
	}
	firstBlockNum := oldestBlockNum
	if firstBlockNum == 0 {
		// cardano-node ignores Byron EBB block number 0 in this metric.
		firstBlockNum = 1
	}
	if tipBlockNum <= firstBlockNum {
		return 0
	}
	return float64(tipBlockNum-firstBlockNum) /
		float64(tipSlot-oldestSlot)
}

// loadPParams reads currentEpoch, currentEra, and epochCache and writes
// currentPParams and prevEraPParams without holding a lock. This is safe
// because it is only called from Start() during single-threaded initialization.
func (ls *LedgerState) loadPParams() error {
	pp, prevPP, err := ls.computePParams(
		ls.currentEpoch,
		ls.currentEra,
		ls.epochCache,
	)
	if err != nil {
		return err
	}
	ls.currentPParams = pp
	ls.prevEraPParams = prevPP
	return nil
}

// reconstructTransitionInfo infers the correct TransitionInfo from the
// already-loaded currentPParams, currentEra, and currentEpoch.
//
// This is called once during Start() after loadPParams(), while the
// LedgerState is still single-threaded (no lock required).
//
// The window that needs reconstruction: when an epoch boundary committed
// protocol-parameter updates that bump the major protocol version (signalling
// an upcoming era transition), but the node was stopped before the first
// block of the new era arrived.  After restart, currentEpoch.EraId is still
// the OLD era, but currentPParams carries the bumped version.  If those
// pparams map to a later era than currentEra, we restore TransitionKnown.
func (ls *LedgerState) reconstructTransitionInfo() {
	if ls.currentPParams == nil {
		return
	}
	ver, err := GetProtocolVersion(ls.currentPParams)
	if err != nil {
		// Not all era pparams are versioned (e.g. Byron falls through);
		// silently leave transitionInfo at TransitionUnknown.
		return
	}
	pparamsEraId, ok := ls.eraForVersion(ver.Major)
	if !ok {
		return
	}
	// If the pparams version maps to a later era than the epoch's stored
	// era, restore TransitionKnown.  KnownEpoch is the current epoch: it
	// was created with the old EraId but its StartSlot is the exact
	// upcoming era boundary.
	if pparamsEraId > ls.currentEra.Id {
		ls.transitionInfo = hardfork.NewTransitionKnown(ls.currentEpoch.EpochId)
	}
}

// eraShape returns the resolved hardfork.Shape for this LedgerState's
// CardanoNodeConfig, building and caching it on first access. cfg is
// immutable for the LedgerState's lifetime, so the cached shape is too.
//
// Returns an empty Shape (no error) when CardanoNodeConfig is unset or when
// BuildShape fails; callers must treat an empty Shape as "shape unavailable"
// and skip shape-derived work.
func (ls *LedgerState) eraShape() hardfork.Shape {
	if s := ls.cachedShape.Load(); s != nil {
		return *s
	}
	cfg := ls.config.CardanoNodeConfig
	if cfg == nil {
		return hardfork.Shape{}
	}
	s, err := eras.BuildShapeWithDijkstra(cfg, ls.config.EnableDijkstra)
	if err != nil {
		return hardfork.Shape{}
	}
	ls.cachedShape.CompareAndSwap(nil, &s)
	return *ls.cachedShape.Load()
}

// evaluateTriggerAtEpoch sets transitionInfo to TransitionKnown(e) when the
// current era's NextEraTrigger is TriggerAtEpoch(e) and that epoch has not
// yet arrived. The trigger is resolved once at Shape build time from
// CardanoNodeConfig (TestXHardForkAtEpoch + ExperimentalHardForksEnabled);
// this method only consumes that resolution.
//
// The AtEpoch override is authoritative: it supersedes a prior
// TransitionUnknown / TransitionImpossible, and replaces any
// TransitionKnown(other) that may have been set from an on-chain pparams
// major-version bump, mirroring the Haskell semantics where
// `shelleyTriggerHardFork` short-circuits to the configured epoch without
// inspecting pparams at all.
//
// The call is a no-op when:
//   - the shape is unavailable or the current era is unknown to it,
//   - the current era's NextEraTrigger is not TriggerAtEpoch (i.e. final era,
//     or default AtVersion),
//   - the configured epoch has already been reached (EpochId >= e) — at that
//     point either the rollover has already applied the transition, or
//     queries should naturally fall through to the new era's own trigger.
//
// Call under ls.Lock() (runtime paths) or without a lock during
// single-threaded startup.
func (ls *LedgerState) evaluateTriggerAtEpoch() {
	shape := ls.eraShape()
	if len(shape.Eras) == 0 {
		return
	}
	entry, ok := shape.EraForID(ls.currentEra.Id)
	if !ok {
		return
	}
	if entry.NextEraTrigger.Kind != hardfork.TriggerAtEpoch {
		return
	}
	epoch := entry.NextEraTrigger.Epoch
	if ls.currentEpoch.EpochId >= epoch {
		return
	}
	if ls.transitionInfo.State == hardfork.TransitionKnown &&
		ls.transitionInfo.KnownEpoch == epoch {
		return
	}
	ls.transitionInfo = hardfork.NewTransitionKnown(epoch)
}

// evaluateTransitionImpossible sets transitionInfo to TransitionImpossible
// when the safe-zone end for the current era already reaches or exceeds the
// current epoch's end slot.
//
// At that point a hard-fork transition is impossible within this epoch: the
// stability window has "vouched for" slots up to (and past) the boundary, so
// no rollover can introduce a new era within the epoch.  Serving the full
// epoch-end slot as EraEnd is therefore safe and more informative than the
// stale tipSlot+safeZone cap.
//
// The method is a no-op unless transitionInfo.State is TransitionUnknown; it
// must not override a confirmed TransitionKnown.
//
// Call under ls.Lock() (runtime tip-update) or without a lock during
// single-threaded startup (after loadTip).
func (ls *LedgerState) evaluateTransitionImpossible() {
	if ls.transitionInfo.State != hardfork.TransitionUnknown {
		return
	}
	// Only meaningful when we have a fully-populated epoch.
	if ls.currentEpoch.LengthInSlots == 0 {
		return
	}
	epochEndSlot, addErr := checkedSlotAdd(
		ls.currentEpoch.StartSlot,
		uint64(ls.currentEpoch.LengthInSlots),
	)
	if addErr != nil {
		return
	}
	safeZone := ls.calculateStabilityWindowForEra(ls.currentEra.Id)
	safeEndSlot, addErr := checkedSlotAdd(ls.currentTip.Point.Slot, safeZone)
	if addErr != nil {
		return
	}
	if safeEndSlot >= epochEndSlot {
		ls.transitionInfo = hardfork.NewTransitionImpossible()
	}
}

// evaluateHardForkInitiationStability surfaces an upcoming era boundary
// to clients while the current epoch is still being applied, by
// checking whether any in-flight HardForkInitiation governance action
// would be ratified if the boundary tick fired now.
//
// Why this is correct mid-epoch: governance votes stop being accepted
// at slot epochEnd - 2*stabilityWindow (the voting deadline). After
// that point the inputs to the ratification computation are frozen —
// no new vote, registration, or pool change can flip the outcome — so
// "would ratify now" equals "will ratify at the next boundary tick".
// Before the deadline the answer is volatile and surfacing it would
// give clients a stale view, so the function returns early.
//
// Priority order on a successful detection: TransitionKnown supersedes
// both TransitionUnknown and TransitionImpossible because it carries
// strictly more information (the exact target epoch). The function is
// idempotent when transitionInfo is already TransitionKnown for the
// same target.
//
// The DB-driven assembly is delegated to
// governance.EvaluateRatifiableHardForkInitiation, which runs the same
// tally + threshold check the boundary path would run.
//
// Performance: in steady state the function returns from one of two
// short-circuits (transitionInfo already Known, or tip is pre-deadline)
// before any database access, so per-block invocation cost is a few
// pointer dereferences. The full DB-driven check fires only in the
// post-voting-deadline window before the boundary, which on mainnet
// is at most a few minutes per epoch and at most one HardForkInitiation
// proposal at a time. If contention with concurrent readers becomes a
// concern in that window, the assembly can be moved out of the
// caller's lock-held section using the snapshot/apply pattern
// rollback() uses for newPParams.
//
// Lock semantics: call under ls.Lock() at the per-block tip-update and
// rollback sites (both already hold the write lock when invoking the
// sibling evaluators). Safe to call without a lock during the
// single-threaded startup path before the chainsync goroutine starts.
func (ls *LedgerState) evaluateHardForkInitiationStability() {
	if ls.currentEpoch.LengthInSlots == 0 {
		return
	}
	// Defer to any TransitionKnown already set by a higher-priority
	// source (TestXHardForkAtEpoch override or pparams-bump
	// detection). Only promote from Unknown / Impossible, matching
	// the pattern of the sibling evaluators on this code path. This
	// also gives idempotency: once we've published the upcoming
	// boundary, subsequent block-apply invocations short-circuit
	// without a DB lookup.
	if ls.transitionInfo.State == hardfork.TransitionKnown {
		return
	}
	epochEndSlot, addErr := checkedSlotAdd(
		ls.currentEpoch.StartSlot,
		uint64(ls.currentEpoch.LengthInSlots),
	)
	if addErr != nil {
		return
	}
	stabilityWindow := ls.calculateStabilityWindowForEra(ls.currentEra.Id)
	// votingDeadline = epochEndSlot - 2*stabilityWindow. Guard the
	// subtraction; on a small synthetic epoch the deadline could land
	// before the epoch start, in which case any tip in-epoch is
	// post-deadline.
	deadlineGap := 2 * stabilityWindow
	var votingDeadline uint64
	if epochEndSlot > deadlineGap {
		votingDeadline = epochEndSlot - deadlineGap
	}
	if ls.currentTip.Point.Slot < votingDeadline {
		return
	}
	// Once-per-epoch gate: post-deadline, the inputs to ratification
	// (DRep voting power, SPO voting power, CC votes) are frozen —
	// the pre-epoch stake snapshot fixes voting weights and votes
	// after the deadline don't count toward this epoch's outcome. So
	// one tally per epoch is the entire correctness budget; running
	// it more often only wastes CPU on the SQLite DRep cascade
	// (~94% of total CPU during catchup on preview, where the
	// post-deadline window can span many hours). hfiEvalDoneEpoch is
	// reset to 0 in rollback() to reopen the gate when chain history
	// changes.
	if ls.hfiEvalDoneEpoch == ls.currentEpoch.EpochId {
		return
	}
	if !ls.hfiStabilityEvalInFlight.CompareAndSwap(false, true) {
		return
	}
	ls.hfiEvalDoneEpoch = ls.currentEpoch.EpochId
	gen := ls.hfiEvalGeneration.Load()
	// Snapshot inputs while we hold the caller's write lock, then run
	// the heavy DB call without it. Reacquiring the lock only to write
	// transitionInfo keeps the per-block path off the SQLite query —
	// otherwise the tally would run inline under ls.Lock() and stall
	// the chainsync pipeline (rollback / new-block apply / read-side
	// chainsync server FindIntersect requests all wait on the same
	// RWMutex), adding multi-second pauses to every block on the
	// catchup path. The snapshot/apply pattern matches the rollback()
	// comment that calls this out as the intended approach for this
	// exact contention concern.
	snapshotEpoch := ls.currentEpoch.EpochId
	snapshotPParams := ls.currentPParams
	conwayGenesis := ls.config.CardanoNodeConfig.ConwayGenesis()
	db := ls.db
	logger := ls.config.Logger
	decodeFailures := ls.metrics.governanceProposalDecodeFailures
	onDecodeFailure := func(proposal *models.GovernanceProposal, err error) {
		logger.Warn(
			"skipping ratifiable HardForkInitiation: decode failed",
			"proposal_id", proposal.ID,
			"error", err,
		)
		decodeFailures.Inc()
	}
	go func() {
		defer ls.hfiStabilityEvalInFlight.Store(false)
		result, err := governance.EvaluateRatifiableHardForkInitiation(
			governance.NewStabilityCheckInputs(
				db,
				nil,
				snapshotEpoch,
				snapshotPParams,
				conwayGenesis,
				onDecodeFailure,
			),
		)
		if err != nil {
			logger.Warn(
				"hardfork-initiation stability check failed",
				"error", err,
				"component", "ledger",
			)
			return
		}
		if result == nil {
			return
		}
		// A ratifiable HardForkInitiation is only an era transition
		// if its target ProtocolVersion crosses an era boundary. An
		// intra-era pparams bump (e.g. Plomin's pv9 → pv10, both
		// Conway) ratifies through the same governance path but
		// doesn't end the era; surfacing it as TransitionKnown would
		// mislead clients. Use the snapshotted PParams so the
		// version comparison reflects the state we computed against.
		currentVer, verErr := GetProtocolVersion(snapshotPParams)
		if verErr != nil {
			return
		}
		targetVer := ProtocolVersion{
			Major: result.NewMajor,
			Minor: result.NewMinor,
		}
		if !ls.isHardForkTransition(currentVer, targetVer) {
			return
		}
		ls.Lock()
		defer ls.Unlock()
		// Generation guard: a rollback that landed while the tally
		// was running invalidated our snapshot. Drop the result
		// rather than commit stale data — rollback's own call to
		// evaluateHardForkInitiationStability will spawn a fresh
		// tally against the post-rollback state.
		if ls.hfiEvalGeneration.Load() != gen {
			return
		}
		// A higher-priority source (TestX override or pparams-bump
		// detection) may have set TransitionKnown while the tally
		// was running; don't overwrite.
		if ls.transitionInfo.State == hardfork.TransitionKnown {
			return
		}
		// Re-derive the target epoch from the current state at
		// commit time so an epoch rollover that landed during the
		// tally doesn't pin TransitionKnown to a stale epoch id.
		ls.transitionInfo = hardfork.NewTransitionKnown(
			ls.currentEpoch.EpochId + 1,
		)
	}()
}

// computePParams loads protocol parameters for the given epoch/era
// without writing to any shared LedgerState fields. This allows
// callers to compute pparams into local variables and then apply
// them atomically under a lock.
func (ls *LedgerState) computePParams(
	epoch models.Epoch,
	era eras.EraDesc,
	epochCache []models.Epoch,
) (
	lcommon.ProtocolParameters,
	lcommon.ProtocolParameters,
	error,
) {
	// Only query stored pparams when the era has a decode function.
	// Byron has nil DecodePParamsFunc and never stores pparams, so
	// we skip straight to the genesis fallback for Byron.
	var pparams lcommon.ProtocolParameters
	if era.DecodePParamsFunc != nil {
		var err error
		pparams, err = ls.db.GetPParams(
			epoch.EpochId,
			era.Id,
			era.DecodePParamsFunc,
			nil,
		)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"computePParams: GetPParams epoch %d: %w",
				epoch.EpochId,
				err,
			)
		}
	}
	if pparams == nil {
		var err error
		pparams, err = ls.computeGenesisProtocolParameters(era)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"bootstrap genesis protocol parameters: %w",
				err,
			)
		}
	}

	// Load previous era's pparams for era-1 TX validation.
	// Walk the epoch cache backwards to find the last epoch
	// that belonged to a different (earlier) era, then load
	// its pparams.
	var prevEraPParams lcommon.ProtocolParameters
	if len(epochCache) == 0 {
		return pparams, prevEraPParams, nil
	}
	for _, ep := range slices.Backward(epochCache) {
		if ep.EraId != era.Id {
			prevEra, _ := ls.eraById(ep.EraId)
			if prevEra != nil &&
				prevEra.DecodePParamsFunc != nil {
				prevPP, prevErr := ls.db.GetPParams(
					ep.EpochId,
					ep.EraId,
					prevEra.DecodePParamsFunc,
					nil,
				)
				if prevErr != nil {
					ls.config.Logger.Warn(
						"failed to load previous-era pparams",
						"epoch", ep.EpochId,
						"era", ep.EraId,
						"error", prevErr,
					)
				} else if prevPP != nil {
					prevEraPParams = prevPP
				}
			}
			break
		}
	}
	return pparams, prevEraPParams, nil
}

// computeGenesisProtocolParameters bootstraps protocol parameters
// from genesis config for the given era without reading any shared
// LedgerState fields.
func (ls *LedgerState) computeGenesisProtocolParameters(
	era eras.EraDesc,
) (lcommon.ProtocolParameters, error) {
	// Start with Shelley parameters as the base for all eras
	// (Byron also uses Shelley as base)
	pparams, err := eras.HardForkShelley(
		ls.config.CardanoNodeConfig,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get protocol parameters from HardForkShelley: %w",
			err,
		)
	}

	// If target era is Byron or Shelley, return the Shelley
	// parameters
	if era.Id <= eras.ShelleyEraDesc.Id {
		return pparams, nil
	}

	// Chain through each era up to the target era
	for eraId := eras.AllegraEraDesc.Id; eraId <= era.Id; eraId++ {
		eraStep, _ := ls.eraById(eraId)
		if eraStep == nil {
			return nil, fmt.Errorf(
				"unknown era ID %d",
				eraId,
			)
		}

		if eraStep.HardForkFunc != nil {
			pparams, err = eraStep.HardForkFunc(
				ls.config.CardanoNodeConfig,
				pparams,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"era %s transition: %w",
					eraStep.Name,
					err,
				)
			}
		}
	}

	return pparams, nil
}

func (ls *LedgerState) loadEpochs(txn *database.Txn) error {
	// Load and cache all epochs
	epochs, err := ls.db.GetEpochs(txn)
	if err != nil {
		return err
	}
	return ls.setEpochCache(txn, epochs)
}

// PrepareEpochCacheForStartup loads epoch metadata before LedgerState.Start().
// It is startup-only: callers use this when another component needs
// SlotToEpoch before the ledger processing loop is running.
func (ls *LedgerState) PrepareEpochCacheForStartup() error {
	if ls.ctx != nil {
		return errors.New("PrepareEpochCacheForStartup must be called before LedgerState.Start")
	}
	txn := ls.db.Transaction(true)
	defer txn.Release()
	return txn.Do(func(txn *database.Txn) error {
		return ls.loadEpochs(txn)
	})
}

func (ls *LedgerState) setEpochCache(txn *database.Txn, epochs []models.Epoch) error {
	ls.epochCache = epochs
	clear(ls.epochNonceHexCache)
	if len(epochs) > 0 {
		// Recover epoch records whose LastEpochBlockNonce was persisted empty
		// or stale by pre-fix boundary lookup bugs (see healEmptyLabNonces).
		ls.healEmptyLabNonces()
		// Set current epoch and era after healing so currentEpoch reflects
		// any repaired nonce in epochCache.
		ls.currentEpoch = ls.epochCache[len(ls.epochCache)-1]
		eraDesc, _ := ls.eraById(ls.currentEpoch.EraId)
		if eraDesc == nil {
			return fmt.Errorf("unknown era ID %d", ls.currentEpoch.EraId)
		}
		ls.currentEra = *eraDesc
		// Update metrics
		ls.metrics.epochNum.Set(float64(ls.currentEpoch.EpochId))
		return nil
	}
	// Populate initial epoch
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return errors.New("failed to load Shelley genesis")
	}
	startProtoVersion := shelleyGenesis.ProtocolParameters.ProtocolVersion.Major
	startEra, startEraOk := eras.EraForVersionIn(
		ls.eraList(),
		startProtoVersion,
	)
	// Initialize current era to Byron when starting from genesis
	ls.currentEra = ls.eraList()[0] // Byron era
	// Transition through every era between the current and the target era.
	// If the configured version is unknown, the loop is skipped and the
	// node starts at Byron — same fallback behavior as the previous map
	// lookup, which returned the zero-value EraDesc for unmapped versions.
	// During startup, it's safe to apply results immediately since there's
	// no concurrent access.
	startEraId := uint(0)
	if startEraOk {
		startEraId = startEra.Id
	}
	if ls.config.StartInDijkstra {
		startEraId = eras.DijkstraEraDesc.Id
	}
	for nextEraId := ls.currentEra.Id + 1; nextEraId <= startEraId; nextEraId++ {
		result, err := ls.transitionToEra(
			txn,
			nextEraId,
			ls.currentEpoch.EpochId,
			ls.currentEpoch.StartSlot+uint64(ls.currentEpoch.LengthInSlots),
			ls.currentPParams,
		)
		if err != nil {
			return err
		}
		// Apply result immediately during startup (single-threaded, no lock needed).
		// applyEraTransition clears transitionInfo; reconstructTransitionInfo()
		// called later in Start() will restore it if the startup state implies
		// a pending TransitionKnown.
		ls.applyEraTransition(result)
	}
	// Generate initial epoch
	rolloverResult, err := ls.processEpochRollover(
		txn,
		ls.currentEpoch,
		ls.currentEra,
		ls.currentPParams,
	)
	if err != nil {
		return err
	}
	// Apply result immediately during startup
	ls.epochCache = rolloverResult.NewEpochCache
	clear(ls.epochNonceHexCache)
	ls.currentEpoch = rolloverResult.NewCurrentEpoch
	ls.currentEra = rolloverResult.NewCurrentEra
	ls.currentPParams = rolloverResult.NewCurrentPParams
	ls.checkpointWrittenForEpoch = rolloverResult.CheckpointWrittenForEpoch
	ls.metrics.epochNum.Set(rolloverResult.NewEpochNum)
	return nil
}

// healEmptyLabNonces repairs epoch records whose LastEpochBlockNonce was
// persisted empty by pre-fix boundary block lookup bugs.
//
// LastEpochBlockNonce is the hash of the last block of the previous epoch. It
// feeds the current epoch's nonce as candidateNonce ⭒ lastEpochBlockNonce.
// Earlier boundary lookups scanned block-blob keys, so they could return a
// synthetic Leios endorser block or retained fork blob instead of the active
// chain's real ranking block. Empty or wrong lab values diverge epoch nonces
// from peers and break leader-VRF verification.
//
// This runs once at startup over the loaded epoch cache. For each non-genesis
// epoch whose lab is empty or differs from the boundary block resolved through
// the canonical chain index, it restores the lab and recomputes the affected
// epoch's nonce in the cache. It is a pure function of already-stored chain
// data and is idempotent across restarts.
func (ls *LedgerState) healEmptyLabNonces() {
	if ls.healEmptyLabNoncesInPlace(ls.epochCache) {
		clear(ls.epochNonceHexCache)
	}
}

func (ls *LedgerState) healEmptyLabNoncesInPlace(epochs []models.Epoch) bool {
	repaired := false
	var (
		skippedMissingCandidate            int
		firstMissingCandidateEpoch         uint64
		lastMissingCandidateEpoch          uint64
		skippedInvalidCandidate            int
		firstInvalidCandidateEpoch         uint64
		lastInvalidCandidateEpoch          uint64
		skippedMithrilTrusted              int
		firstMithrilTrustedEpoch           uint64
		lastMithrilTrustedEpoch            uint64
		recordSkippedMissingCandidateEpoch = func(epoch uint64) {
			if skippedMissingCandidate == 0 {
				firstMissingCandidateEpoch = epoch
			}
			lastMissingCandidateEpoch = epoch
			skippedMissingCandidate++
		}
		recordSkippedInvalidCandidateEpoch = func(epoch uint64) {
			if skippedInvalidCandidate == 0 {
				firstInvalidCandidateEpoch = epoch
			}
			lastInvalidCandidateEpoch = epoch
			skippedInvalidCandidate++
		}
		recordSkippedMithrilTrustedEpoch = func(epoch uint64) {
			if skippedMithrilTrusted == 0 {
				firstMithrilTrustedEpoch = epoch
			}
			lastMithrilTrustedEpoch = epoch
			skippedMithrilTrusted++
		}
	)
	for i := range epochs {
		ep := &epochs[i]
		// The genesis epoch legitimately carries no last block nonce.
		if ep.StartSlot == 0 {
			continue
		}
		if ls.mithrilLedgerSlot > 0 &&
			ep.StartSlot <= ls.mithrilLedgerSlot &&
			len(ep.LastEpochBlockNonce) == lcommon.Blake2b256Size {
			recordSkippedMithrilTrustedEpoch(ep.EpochId)
			continue
		}
		candidateNonce, err := ls.epochRepairCandidateNonce(*ep)
		if err != nil {
			if len(ep.CandidateNonce) == 0 {
				recordSkippedMissingCandidateEpoch(ep.EpochId)
			} else {
				recordSkippedInvalidCandidateEpoch(ep.EpochId)
			}
			continue
		}
		boundary, err := ls.canonicalBlockBeforeSlot(nil, ep.StartSlot)
		if err != nil ||
			len(boundary.Hash) != lcommon.Blake2b256Size {
			continue
		}
		if bytes.Equal(ep.LastEpochBlockNonce, boundary.Hash) {
			continue
		}
		// Recompute this epoch's nonce before mutating the record; the lab and
		// nonce must be repaired together or not at all.
		res, err := lcommon.CalculateEpochNonce(
			candidateNonce,
			boundary.Hash,
			nil,
		)
		if err != nil {
			ls.config.Logger.Warn(
				"failed to recompute epoch nonce during lab recovery",
				"epoch", ep.EpochId,
				"error", err,
				"component", "ledger",
			)
			continue
		}
		previousLab := cloneNonce(ep.LastEpochBlockNonce)
		previousNonce := cloneNonce(ep.Nonce)
		newNonce := res.Bytes()
		ep.LastEpochBlockNonce = cloneNonce(boundary.Hash)
		ep.Nonce = newNonce
		repaired = true
		ls.config.Logger.Info(
			"repaired epoch lastEpochBlockNonce",
			"epoch", ep.EpochId,
			"boundary_slot", boundary.Slot,
			"previous_last_epoch_block_nonce",
			hex.EncodeToString(previousLab),
			"last_epoch_block_nonce",
			hex.EncodeToString(ep.LastEpochBlockNonce),
			"component", "ledger",
		)
		ls.config.Logger.Info(
			"recomputed epoch nonce after lab recovery",
			"epoch", ep.EpochId,
			"previous_nonce", hex.EncodeToString(previousNonce),
			"epoch_nonce", hex.EncodeToString(newNonce),
			"component", "ledger",
		)
	}
	if skippedMithrilTrusted > 0 {
		ls.config.Logger.Info(
			"skipped epoch lab recovery for epochs covered by Mithril trust boundary",
			"count", skippedMithrilTrusted,
			"first_epoch", firstMithrilTrustedEpoch,
			"last_epoch", lastMithrilTrustedEpoch,
			"mithril_ledger_slot", ls.mithrilLedgerSlot,
			"component", "ledger",
		)
	}
	if skippedMissingCandidate > 0 {
		ls.config.Logger.Info(
			"skipped epoch lab recovery for epochs without stored candidate nonce",
			"count", skippedMissingCandidate,
			"first_epoch", firstMissingCandidateEpoch,
			"last_epoch", lastMissingCandidateEpoch,
			"component", "ledger",
		)
	}
	if skippedInvalidCandidate > 0 {
		ls.config.Logger.Warn(
			"skipped epoch lab recovery for epochs with invalid candidate nonce",
			"count", skippedInvalidCandidate,
			"first_epoch", firstInvalidCandidateEpoch,
			"last_epoch", lastInvalidCandidateEpoch,
			"component", "ledger",
		)
	}
	return repaired
}

func (ls *LedgerState) epochRepairCandidateNonce(
	ep models.Epoch,
) ([]byte, error) {
	switch len(ep.CandidateNonce) {
	case lcommon.Blake2b256Size:
		return cloneNonce(ep.CandidateNonce), nil
	case 0:
		return nil, errors.New("candidate nonce not stored")
	default:
		return nil, fmt.Errorf(
			"invalid candidate nonce length %d",
			len(ep.CandidateNonce),
		)
	}
}

func (ls *LedgerState) loadTip() error {
	tmpTip, err := ls.db.GetTip(nil)
	if err != nil {
		return err
	}
	if err := ls.db.DeleteBlockNoncesAfterPoint(tmpTip.Point, nil); err != nil {
		return fmt.Errorf("prune block nonces beyond tip: %w", err)
	}
	// Load tip block nonce before acquiring lock
	var tipNonce []byte
	if tmpTip.Point.Slot > 0 {
		tipNonce, err = ls.db.GetBlockNonce(
			tmpTip.Point,
			nil,
		)
		if err != nil {
			return err
		}
	}
	tipDensity := ls.chainFragmentDensity(
		tmpTip,
		ls.securityParamForCurrentEraSnapshot(),
	)
	// Lock only for in-memory state updates
	ls.Lock()
	ls.currentTip = tmpTip
	if tmpTip.Point.Slot > 0 {
		ls.currentTipBlockNonce = tipNonce
	}
	ls.updateTipMetrics(tipDensity)
	ls.Unlock()
	return nil
}

func (ls *LedgerState) reconcilePrimaryChainTipWithLedgerTip() error {
	if ls.chain == nil || ls.config.ChainManager == nil {
		return nil
	}
	ls.RLock()
	ledgerTip := ls.currentTip
	ls.RUnlock()
	chainTip := ls.chain.Tip()
	if chainTip.Point.Slot == ledgerTip.Point.Slot &&
		bytes.Equal(chainTip.Point.Hash, ledgerTip.Point.Hash) {
		return nil
	}
	if chainTip.Point.Slot < ledgerTip.Point.Slot {
		ls.config.Logger.Warn(
			"ledger tip ahead of primary chain tip at startup, rolling back metadata to chain tip",
			"component", "ledger",
			"chain_tip_slot", chainTip.Point.Slot,
			"ledger_tip_slot", ledgerTip.Point.Slot,
			"chain_tip_hash", hex.EncodeToString(chainTip.Point.Hash),
			"ledger_tip_hash", hex.EncodeToString(ledgerTip.Point.Hash),
		)
		if err := ls.rollback(chainTip.Point); err != nil {
			return fmt.Errorf(
				"rollback ledger tip to primary chain tip: %w",
				err,
			)
		}
		return nil
	}
	containsLedgerTip, err := ls.primaryChainContainsPoint(ledgerTip.Point)
	if err != nil {
		return fmt.Errorf("check ledger tip on primary chain: %w", err)
	}
	if containsLedgerTip {
		// The ledger tip is a valid ancestor on the primary chain, so the
		// primary chain is simply a forward extension of the ledger. This is
		// the normal shape when bootstrapping from a Mithril snapshot whose
		// ledger state lags the immutable block data, and the gap can far
		// exceed the security parameter. We must always replay forward to
		// catch up; rewinding the primary chain here would delete the very
		// blocks ledgerProcessBlocks needs and defeat catch-up.
		gap := uint64(0)
		if chainTip.BlockNumber > ledgerTip.BlockNumber {
			gap = chainTip.BlockNumber - ledgerTip.BlockNumber
		}
		ls.config.Logger.Warn(
			"primary chain tip ahead of ledger tip at startup; ledgerProcessBlocks will catch up via chainsync",
			"component", "ledger",
			"chain_tip_slot", chainTip.Point.Slot,
			"ledger_tip_slot", ledgerTip.Point.Slot,
			"chain_tip_hash", hex.EncodeToString(chainTip.Point.Hash),
			"ledger_tip_hash", hex.EncodeToString(ledgerTip.Point.Hash),
			"block_gap", gap,
		)
		return nil
	}
	ancestor, found, err := ls.latestLedgerPrimaryChainAncestor(
		ledgerTip.Point,
		containsLedgerTip,
	)
	if err != nil {
		return fmt.Errorf("find common primary-chain ancestor for ledger tip: %w", err)
	}
	if !found {
		return fmt.Errorf(
			"ledger tip %d/%s is not on primary chain and no common ancestor was found",
			ledgerTip.Point.Slot,
			hex.EncodeToString(ledgerTip.Point.Hash),
		)
	}
	ls.config.Logger.Warn(
		"ledger tip not on primary chain at startup, rolling back metadata to common ancestor",
		"component", "ledger",
		"chain_tip_slot", chainTip.Point.Slot,
		"ledger_tip_slot", ledgerTip.Point.Slot,
		"chain_tip_hash", hex.EncodeToString(chainTip.Point.Hash),
		"ledger_tip_hash", hex.EncodeToString(ledgerTip.Point.Hash),
		"ancestor_slot", ancestor.Slot,
		"ancestor_hash", hex.EncodeToString(ancestor.Hash),
	)
	if err := ls.config.ChainManager.RewindPrimaryChainToPoint(
		ancestor,
	); err != nil {
		return fmt.Errorf(
			"rewind primary chain to common primary-chain ancestor: %w",
			err,
		)
	}
	if err := ls.rollback(ancestor); err != nil {
		return fmt.Errorf(
			"rollback ledger tip to common primary-chain ancestor: %w",
			err,
		)
	}
	return nil
}

// ReconcileLivePrimaryChainLedgerDivergence is the exported entry
// point into the live-divergence reconciler. The plateau watchdog
// calls this when local tip has not advanced for plateau_duration
// while peers report a higher tip: if primary chain has advanced but
// the ledger pipeline is stuck on an abandoned same-slot fork, this
// rolls back the ledger to the latest common ancestor so forward
// application from the canonical chain can resume without a process
// or container restart. Returns (true, nil) when reconciliation
// happened, (false, nil) when no divergence was found.
func (ls *LedgerState) ReconcileLivePrimaryChainLedgerDivergence(
	reason string,
	connId ouroboros.ConnectionId,
) (bool, error) {
	return ls.reconcileLivePrimaryChainLedgerDivergence(reason, connId)
}

func (ls *LedgerState) reconcileLivePrimaryChainLedgerDivergence(
	reason string,
	connId ouroboros.ConnectionId,
) (bool, error) {
	if ls.chain == nil || ls.config.ChainManager == nil {
		return false, nil
	}
	ls.RLock()
	ledgerTip := ls.currentTip
	ls.RUnlock()
	chainTip := ls.chain.Tip()
	if chainTip.Point.Slot == ledgerTip.Point.Slot &&
		bytes.Equal(chainTip.Point.Hash, ledgerTip.Point.Hash) {
		return false, nil
	}

	shouldReconcile := chainTip.Point.Slot < ledgerTip.Point.Slot
	if !shouldReconcile {
		containsLedgerTip, err := ls.primaryChainContainsPoint(
			ledgerTip.Point,
		)
		if err != nil {
			return false, fmt.Errorf(
				"check ledger tip on primary chain: %w",
				err,
			)
		}
		shouldReconcile = !containsLedgerTip
	}
	if !shouldReconcile {
		return false, nil
	}

	ls.config.Logger.Warn(
		"primary chain and ledger diverged during live chainsync recovery, reconciling to common ancestor",
		"component", "ledger",
		"reason", reason,
		"connection_id", connId.String(),
		"chain_tip_slot", chainTip.Point.Slot,
		"ledger_tip_slot", ledgerTip.Point.Slot,
		"chain_tip_hash", hex.EncodeToString(chainTip.Point.Hash),
		"ledger_tip_hash", hex.EncodeToString(ledgerTip.Point.Hash),
	)
	if err := ls.reconcilePrimaryChainTipWithLedgerTip(); err != nil {
		return false, err
	}
	return true, nil
}

func (ls *LedgerState) primaryChainContainsPoint(point ocommon.Point) (bool, error) {
	if point.Slot == 0 && len(point.Hash) == 0 {
		return true, nil
	}
	if ls.db == nil {
		return false, nil
	}
	_, err := database.BlockByPoint(ls.db, point)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, models.ErrBlockNotFound) {
		return false, nil
	}
	return false, err
}

func (ls *LedgerState) latestLedgerPrimaryChainAncestor(
	point ocommon.Point,
	containsPoint bool,
) (ocommon.Point, bool, error) {
	if containsPoint {
		return point, true, nil
	}
	if ls.db == nil {
		return ocommon.Point{}, false, nil
	}
	end := point.Slot
	for {
		start := uint64(0)
		if end > ledgerAncestorSearchWindow {
			start = end - ledgerAncestorSearchWindow
		}
		blockNonces, err := ls.db.GetBlockNoncesInSlotRange(start, end, nil)
		if err != nil {
			return ocommon.Point{}, false, err
		}
		for i := len(blockNonces); i > 0; i-- {
			blockNonce := blockNonces[i-1]
			ancestor := ocommon.NewPoint(blockNonce.Slot, blockNonce.Hash)
			containsAncestor, err := ls.primaryChainContainsPoint(ancestor)
			if err != nil {
				return ocommon.Point{}, false, err
			}
			if containsAncestor {
				return ancestor, true, nil
			}
		}
		if start == 0 {
			break
		}
		end = start
	}
	return ocommon.Point{}, false, nil
}

func (ls *LedgerState) GetBlock(point ocommon.Point) (models.Block, error) {
	ret, err := ls.chain.BlockByPoint(point, nil)
	if err != nil {
		return models.Block{}, err
	}
	return ret, nil
}

func (ls *LedgerState) primaryChainTipAtOrAheadOfLedgerTip() bool {
	if ls.chain == nil {
		return false
	}
	ls.RLock()
	ledgerTip := ls.currentTip
	ls.RUnlock()
	chainTip := ls.chain.Tip()
	if chainTip.Point.Slot > ledgerTip.Point.Slot {
		// The primary chain leads the ledger tip. As long as the ledger tip
		// is a valid ancestor on the primary chain, the chain is a forward
		// extension we can intersect against, regardless of how far it leads
		// (e.g. an old Mithril snapshot whose ledger lags the block data).
		containsLedgerTip, err := ls.primaryChainContainsPoint(ledgerTip.Point)
		if err != nil {
			ls.config.Logger.Warn(
				"failed to confirm ledger tip is on primary chain",
				"component", "ledger",
				"ledger_tip_slot", ledgerTip.Point.Slot,
				"ledger_tip_hash", hex.EncodeToString(ledgerTip.Point.Hash),
				"error", err,
			)
			return false
		}
		return containsLedgerTip
	}
	return chainTip.Point.Slot == ledgerTip.Point.Slot &&
		bytes.Equal(chainTip.Point.Hash, ledgerTip.Point.Hash)
}

func (ls *LedgerState) authoritativeRecentChainPoints(
	count int,
) ([]ocommon.Point, error) {
	if count <= 0 {
		return nil, nil
	}
	ls.RLock()
	currentTip := ls.currentTip
	ls.RUnlock()
	if currentTip.Point.Slot == 0 && len(currentTip.Point.Hash) == 0 {
		return nil, nil
	}
	points := make([]ocommon.Point, 0, count)
	seen := make(map[string]struct{}, count)
	appendBlock := func(block models.Block) {
		if len(points) >= count {
			return
		}
		key := fmt.Sprintf("%d:%x", block.Slot, block.Hash)
		if _, ok := seen[key]; ok {
			return
		}
		points = append(
			points,
			ocommon.NewPoint(block.Slot, block.Hash),
		)
		seen[key] = struct{}{}
	}
	appendBlockByIndex := func(blockIndex uint64) {
		if len(points) >= count {
			return
		}
		block, err := ls.db.BlockByIndex(blockIndex, nil)
		if err != nil {
			return
		}
		appendBlock(block)
	}
	tipBlock, err := database.BlockByPoint(ls.db, currentTip.Point)
	if err != nil {
		// Tolerate a missing authoritative tip: returning the
		// error here would leave us unable to send any intersect
		// points to peers, which breaks both inbound chainsync
		// (peers can't sync from us) and our own outbound
		// chainsync setup (we ship MsgFindIntersect with these
		// points).
		if errors.Is(err, models.ErrBlockNotFound) {
			return points, nil
		}
		return nil, err
	}
	appendBlock(tipBlock)
	denseStartIndex := tipBlock.ID
	if denseStartIndex > firstBlockIndex {
		denseStartIndex--
	} else {
		denseStartIndex = 0
	}
	denseCount := min(count, ledgerIntersectDenseCount)
	for idx := denseStartIndex; idx >= firstBlockIndex && len(points) < denseCount; idx-- {
		appendBlockByIndex(idx)
		if idx == firstBlockIndex {
			break
		}
	}
	if len(points) >= count {
		return points, nil
	}
	if tipBlock.ID > firstBlockIndex {
		for offset := uint64(denseCount); len(points) < count; offset *= 2 {
			if offset == 0 || offset >= tipBlock.ID {
				break
			}
			appendBlockByIndex(tipBlock.ID - offset)
		}
	}
	if len(points) < count {
		appendBlockByIndex(firstBlockIndex)
	}
	return points, nil
}

// RecentChainPoints returns the requested count of recent chain points in
// descending order from the authoritative ledger tip. This avoids exposing
// blob-backed primary-chain points that have not yet been replayed into the
// metadata/ledger state.
func (ls *LedgerState) RecentChainPoints(
	count int,
) ([]ocommon.Point, error) {
	return ls.authoritativeRecentChainPoints(count)
}

// IntersectPoints returns chainsync FindIntersect candidates ordered from
// newest to oldest. The point list stays dense near the tip and spreads out
// deeper in history so lagging peers intersect recent chain state instead of
// falling back to origin after only a small tip gap.
func (ls *LedgerState) IntersectPoints(
	count int,
) ([]ocommon.Point, error) {
	if count <= 0 {
		return nil, nil
	}
	if ls.primaryChainTipAtOrAheadOfLedgerTip() {
		points := ls.chain.IntersectPoints(count)
		if len(points) > 0 {
			return ls.withMithrilTrustBoundaryIntersectPoint(
				points,
				count,
			), nil
		}
	}
	points, err := ls.RecentChainPoints(count)
	if err != nil {
		return nil, err
	}
	return ls.withMithrilTrustBoundaryIntersectPoint(points, count), nil
}

func (ls *LedgerState) withMithrilTrustBoundaryIntersectPoint(
	points []ocommon.Point,
	count int,
) []ocommon.Point {
	if count <= 0 {
		return points
	}
	point, ok := ls.mithrilTrustBoundaryPoint()
	if !ok {
		return points
	}
	for _, existing := range points {
		if existing.Slot == point.Slot &&
			bytes.Equal(existing.Hash, point.Hash) {
			return points
		}
	}

	insertAt := len(points)
	for idx, existing := range points {
		if point.Slot > existing.Slot {
			insertAt = idx
			break
		}
	}
	ret := make([]ocommon.Point, 0, len(points)+1)
	ret = append(ret, points[:insertAt]...)
	ret = append(ret, point)
	ret = append(ret, points[insertAt:]...)
	if len(ret) <= count {
		return ret
	}
	if insertAt >= count {
		ret[count-1] = point
	}
	return ret[:count]
}

func (ls *LedgerState) mithrilTrustBoundaryPoint() (ocommon.Point, bool) {
	if ls == nil || ls.db == nil {
		return ocommon.Point{}, false
	}
	ls.RLock()
	boundarySlot := ls.mithrilLedgerSlot
	boundaryHash := append([]byte(nil), ls.mithrilLedgerHash...)
	currentTip := ls.currentTip
	ls.RUnlock()
	if boundarySlot == 0 || boundarySlot == ^uint64(0) {
		return ocommon.Point{}, false
	}
	if len(boundaryHash) > 0 {
		if len(boundaryHash) != lcommon.Blake2b256Size {
			return ocommon.Point{}, false
		}
		return ocommon.NewPoint(boundarySlot, boundaryHash), true
	}
	if currentTip.Point.Slot == 0 && len(currentTip.Point.Hash) == 0 {
		return ocommon.Point{}, false
	}
	if boundarySlot > currentTip.Point.Slot {
		return ocommon.Point{}, false
	}
	block, err := ls.authoritativeLedgerBlockAtSlot(
		boundarySlot,
		currentTip.Point,
	)
	if err != nil {
		if ls.config.Logger != nil &&
			!errors.Is(err, models.ErrBlockNotFound) {
			ls.config.Logger.Debug(
				"failed to load Mithril trust boundary intersect point",
				"component", "ledger",
				"mithril_ledger_slot", boundarySlot,
				"error", err,
			)
		}
		return ocommon.Point{}, false
	}
	if block.Slot != boundarySlot || len(block.Hash) == 0 {
		return ocommon.Point{}, false
	}
	return ocommon.NewPoint(block.Slot, block.Hash), true
}

func (ls *LedgerState) authoritativeLedgerBlockAtSlot(
	slot uint64,
	tipPoint ocommon.Point,
) (models.Block, error) {
	var ret models.Block
	txn := ls.db.Transaction(false)
	err := txn.Do(func(txn *database.Txn) error {
		block, err := database.BlockByPointTxn(txn, tipPoint)
		if err != nil {
			return err
		}
		if slot > block.Slot {
			return models.ErrBlockNotFound
		}
		// Same-slot fork blocks can coexist in blob storage. Only the
		// current tip's PrevHash chain proves the boundary is canonical.
		for remaining := block.Slot - slot + 1; remaining > 0; remaining-- {
			if block.Slot == slot {
				ret = block
				return nil
			}
			if block.Slot < slot {
				return models.ErrBlockNotFound
			}
			prevHash, err := blockPrevHash(block)
			if err != nil {
				return err
			}
			if len(prevHash) == 0 {
				return models.ErrBlockNotFound
			}
			block, err = database.BlockByHashTxn(txn, prevHash)
			if err != nil {
				return err
			}
		}
		return models.ErrBlockNotFound
	})
	return ret, err
}

func blockPrevHash(block models.Block) ([]byte, error) {
	if len(block.PrevHash) > 0 {
		return block.PrevHash, nil
	}
	decodedBlock, err := block.Decode()
	if err != nil {
		return nil, fmt.Errorf(
			"decode block at slot %d for previous hash: %w",
			block.Slot,
			err,
		)
	}
	prevHash := decodedBlock.PrevHash().Bytes()
	if len(prevHash) == 0 {
		return nil, nil
	}
	return prevHash, nil
}

// GetIntersectPoint returns the intersect between the specified points and the current chain
func (ls *LedgerState) GetIntersectPoint(
	points []ocommon.Point,
) (*ocommon.Point, error) {
	ls.RLock()
	tip := ls.currentTip
	ls.RUnlock()
	// When the chain is empty (tip at origin), origin is the only
	// valid intersect regardless of what points the peer sends.
	// This allows peers to start chainsync before we have blocks.
	if tip.Point.Slot == 0 && len(tip.Point.Hash) == 0 {
		var ret ocommon.Point
		return &ret, nil
	}
	var ret ocommon.Point
	var tmpBlock models.Block
	var err error
	foundOrigin := false
	txn := ls.db.Transaction(false)
	err = txn.Do(func(txn *database.Txn) error {
		for _, point := range points {
			// Ignore points with a slot later than our current tip
			if point.Slot > tip.Point.Slot {
				continue
			}
			// Ignore points with a slot earlier than an existing match
			if point.Slot < ret.Slot {
				continue
			}
			// Check for special origin point
			if point.Slot == 0 && len(point.Hash) == 0 {
				foundOrigin = true
				continue
			}
			// Lookup block in metadata DB
			tmpBlock, err = ls.chain.BlockByPoint(point, txn)
			if err != nil {
				if errors.Is(err, models.ErrBlockNotFound) {
					continue
				}
				return fmt.Errorf("failed to get block: %w", err)
			}
			// Update return value
			ret.Slot = tmpBlock.Slot
			ret.Hash = tmpBlock.Hash
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if ret.Slot > 0 || foundOrigin {
		return &ret, nil
	}
	return nil, nil
}

// GetChainFromPoint returns a ChainIterator starting at the specified point. If inclusive is true, the iterator
// will start at the requested point, otherwise it will start at the next block.
func (ls *LedgerState) GetChainFromPoint(
	point ocommon.Point,
	inclusive bool,
) (*chain.ChainIterator, error) {
	return ls.chain.FromPoint(point, inclusive)
}

// GetChainFromPointContext returns a ChainIterator that inherits cancellation
// from ctx.
func (ls *LedgerState) GetChainFromPointContext(
	ctx context.Context,
	point ocommon.Point,
	inclusive bool,
) (*chain.ChainIterator, error) {
	return ls.chain.FromPointContext(ctx, point, inclusive)
}

// GetChainFromPointReverse returns a ChainIterator that walks backward from
// the specified point toward chain origin. If inclusive is true the iterator
// yields the start point first; otherwise it yields the preceding block.
func (ls *LedgerState) GetChainFromPointReverse(
	point ocommon.Point,
	inclusive bool,
) (*chain.ChainIterator, error) {
	return ls.chain.FromPointReverse(point, inclusive)
}

// GetChainFromPointReverseContext returns a reverse ChainIterator that
// inherits cancellation from ctx.
func (ls *LedgerState) GetChainFromPointReverseContext(
	ctx context.Context,
	point ocommon.Point,
	inclusive bool,
) (*chain.ChainIterator, error) {
	return ls.chain.FromPointReverseContext(ctx, point, inclusive)
}

// Tip returns the current chain tip
func (ls *LedgerState) Tip() ochainsync.Tip {
	ls.RLock()
	defer ls.RUnlock()
	return ls.currentTip
}

// SlotsBehindHead reports how many slots the applied ledger tip is behind the
// wall-clock head (0 if at or ahead of it, or if the wall slot is unknown).
// Unlike IsAtTip it distinguishes "chainsync reached the head" from "the ledger
// has actually applied up to the head", which matters during a from-scratch
// catch-up where the ledger replays a large backlog while chainsync is already
// at the head.
func (ls *LedgerState) SlotsBehindHead() uint64 {
	wall, err := ls.CurrentSlot()
	if err != nil {
		return 0
	}
	tipSlot := ls.Tip().Point.Slot
	if wall <= tipSlot {
		return 0
	}
	return wall - tipSlot
}

// ChainTipSlot returns the slot number of the current chain tip.
func (ls *LedgerState) ChainTipSlot() uint64 {
	ls.RLock()
	defer ls.RUnlock()
	return ls.currentTip.Point.Slot
}

// PrimaryChainTip returns the tip of the primary chain. This can be ahead of
// Tip() while the ledger pipeline is still replaying blocks into committed
// metadata state.
func (ls *LedgerState) PrimaryChainTip() ochainsync.Tip {
	ls.RLock()
	chain := ls.chain
	ls.RUnlock()
	if chain == nil {
		return ochainsync.Tip{}
	}
	return chain.Tip()
}

// PrimaryChainTipSlot returns the slot number of the primary chain tip. This
// can be ahead of ChainTipSlot() while the ledger pipeline is still replaying
// blocks into committed metadata state.
func (ls *LedgerState) PrimaryChainTipSlot() uint64 {
	return ls.PrimaryChainTip().Point.Slot
}

// UpstreamTipSlot returns the latest known tip slot from upstream peers.
// Returns 0 if no upstream tip is known yet.
func (ls *LedgerState) UpstreamTipSlot() uint64 {
	return ls.syncUpstreamTipSlot.Load()
}

// GetCurrentPParams returns the currentPParams value
func (ls *LedgerState) GetCurrentPParams() lcommon.ProtocolParameters {
	ls.RLock()
	defer ls.RUnlock()
	return ls.currentPParams
}

// ProtocolParamsForSlot returns the protocol parameters that should
// govern a block forged at the given slot. When the slot lies in an
// epoch beyond a scheduled fork (the active era's NextEraTrigger is
// TriggerAtEpoch and the slot's epoch is at or past the trigger),
// the returned pparams are the post-fork pparams computed by walking
// each successor era's HardForkFunc up to the slot's era.
//
// The forger uses this when picking an era to build a block in.
// Reading currentPParams alone would lock a sole producer to the
// pre-fork era forever: the boundary-crossing block would be encoded
// in the old era, the rollover (which trusts the observed block's
// era) would not advance, and the chain would never traverse the
// fork. Forecasting from the schedule lets the forger produce a
// block in the era the schedule requires, regardless of how the
// schedule was produced — administrative overrides, on-chain update
// proposals, or HardForkInitiation gov actions all surface as
// TriggerAtEpoch entries on the shape.
func (ls *LedgerState) ProtocolParamsForSlot(
	slot uint64,
) lcommon.ProtocolParameters {
	ls.RLock()
	currentEpoch := ls.currentEpoch
	currentEra := ls.currentEra
	currentPParams := ls.currentPParams
	ls.RUnlock()

	if currentPParams == nil || currentEpoch.LengthInSlots == 0 {
		return currentPParams
	}
	slotEpoch := slot / uint64(currentEpoch.LengthInSlots)
	if slotEpoch <= currentEpoch.EpochId {
		return currentPParams
	}
	shape := ls.eraShape()
	if len(shape.Eras) == 0 {
		return currentPParams
	}
	// Walk forward from the current era, applying each successor's
	// HardForkFunc whose triggerEpoch <= slotEpoch. The single-step
	// case (one fork between currentEpoch and slotEpoch) is the
	// nominal path; the loop also tolerates multi-step jumps so the
	// helper stays sound if a caller skips ahead.
	pparams := currentPParams
	eraID := currentEra.Id
	for {
		entry, ok := shape.EraForID(eraID)
		if !ok ||
			entry.NextEraTrigger.Kind != hardfork.TriggerAtEpoch {
			break
		}
		if entry.NextEraTrigger.Epoch > slotEpoch {
			break
		}
		nextID := eraID + 1
		nextEraPtr, ok := ls.eraById(nextID)
		if !ok || nextEraPtr == nil {
			break
		}
		nextEra := *nextEraPtr
		if nextEra.HardForkFunc == nil {
			eraID = nextID
			continue
		}
		newPParams, err := nextEra.HardForkFunc(
			ls.config.CardanoNodeConfig,
			pparams,
		)
		if err != nil {
			ls.config.Logger.Warn(
				"ProtocolParamsForSlot: HardForkFunc failed",
				"slot", slot,
				"slot_epoch", slotEpoch,
				"from_era", eraID,
				"to_era", nextID,
				"error", err,
			)
			return currentPParams
		}
		pparams = newPParams
		eraID = nextID
	}
	return pparams
}

// CurrentEpoch returns the current epoch number.
func (ls *LedgerState) CurrentEpoch() uint64 {
	ls.RLock()
	defer ls.RUnlock()
	return ls.currentEpoch.EpochId
}

// ConsensusModeForEpoch returns the Praos consensus variant that
// governs leader eligibility for the given epoch. Shelley/Allegra/
// Mary/Alonzo run TPraos; Babbage/Conway run CPraos. Anything else
// (including Byron and unknown eras) defaults to CPraos, matching
// how block production paths fall back today.
//
// Resolution order, mirroring how the leader-election caller can be
// computing the schedule for the current epoch or pre-computing the
// next one across a scheduled hard fork:
//
//  1. Look up the epoch's stored EraId in epochCache (set by the
//     epoch rollover when the epoch is created).
//  2. If we don't have that epoch yet (precompute path) and a hard
//     fork has been confirmed via HardForkInitiation, transitionInfo
//     pins the first epoch of the next era; advance once when the
//     target epoch is at or past that boundary.
//  3. Otherwise forecast the era forward from the current era using
//     the schedule's TriggerAtEpoch trigger (TestXHardForkAtEpoch
//     overrides surface here too), advancing once per scheduled
//     boundary at-or-before the target epoch.
//  4. Fall back to the current era if nothing applies.
func (ls *LedgerState) ConsensusModeForEpoch(epoch uint64) consensus.ConsensusMode {
	ls.RLock()
	cache := ls.epochCache
	currentEra := ls.currentEra
	currentEpoch := ls.currentEpoch
	transitionInfo := ls.transitionInfo
	ls.RUnlock()

	for _, e := range cache {
		if e.EpochId == epoch {
			return consensusModeForEraID(e.EraId)
		}
	}

	if epoch <= currentEpoch.EpochId {
		return consensusModeForEraID(currentEra.Id)
	}

	// HardForkInitiation path: if a confirmed transition pins the next
	// era's first epoch, advance one era for any target epoch at or
	// past it. The shape walk below only handles TriggerAtEpoch (the
	// TestXHardForkAtEpoch override), so without this branch the
	// precompute would stay on the current era's mode through any
	// stable HFI boundary.
	if transitionInfo.State == hardfork.TransitionKnown &&
		epoch >= transitionInfo.KnownEpoch {
		nextID := currentEra.Id + 1
		if _, ok := ls.eraById(nextID); ok {
			return consensusModeForEraID(nextID)
		}
	}

	shape := ls.eraShape()
	eraID := currentEra.Id
	for {
		entry, ok := shape.EraForID(eraID)
		if !ok || entry.NextEraTrigger.Kind != hardfork.TriggerAtEpoch {
			break
		}
		if entry.NextEraTrigger.Epoch > epoch {
			break
		}
		nextID := eraID + 1
		if _, ok := ls.eraById(nextID); !ok {
			break
		}
		eraID = nextID
	}
	return consensusModeForEraID(eraID)
}

// consensusModeForEraID maps an era ID to its Praos consensus variant.
// Shelley/Allegra/Mary/Alonzo are TPraos; Babbage onwards are CPraos.
// Byron and any future-unknown id default to CPraos — the conservative
// choice for a forward-looking unknown era and a no-op for Byron, which
// has no Praos leader election.
func consensusModeForEraID(eraID uint) consensus.ConsensusMode {
	switch eraID {
	case ledger.EraIdShelley,
		ledger.EraIdAllegra,
		ledger.EraIdMary,
		ledger.EraIdAlonzo:
		return consensus.ConsensusModeTPraos
	default:
		return consensus.ConsensusModeCPraos
	}
}

// NextEpochNonceReadyEpoch reports the upcoming epoch when the current
// epoch has already crossed the nonce stability cutoff and the next leader
// schedule can be precomputed immediately.
func (ls *LedgerState) NextEpochNonceReadyEpoch() (uint64, bool) {
	ls.RLock()
	currentEpoch := ls.currentEpoch
	currentEra := ls.currentEra
	tipSlot := ls.currentTip.Point.Slot
	ls.RUnlock()

	if currentEra.Id == 0 {
		return 0, false
	}

	currentSlot, err := ls.CurrentSlot()
	if err != nil {
		return 0, false
	}

	epochLength := uint64(currentEpoch.LengthInSlots)
	if epochLength == 0 {
		return 0, false
	}
	epochEndSlot := currentEpoch.StartSlot + epochLength
	if currentSlot < currentEpoch.StartSlot || currentSlot >= epochEndSlot {
		return 0, false
	}

	cutoffSlot, ready := ls.nextEpochNonceReadyCutoffSlot(currentEpoch)
	if !ready || currentSlot < cutoffSlot || tipSlot < cutoffSlot {
		return 0, false
	}

	readyEpoch := currentEpoch.EpochId + 1
	if len(ls.EpochNonce(readyEpoch)) == 0 {
		return 0, false
	}

	return readyEpoch, true
}

// EpochNonce returns the nonce for the given epoch.
// The epoch nonce is used for VRF-based leader election.
// Returns nil if the epoch nonce is not available (e.g., for Byron era).
//
// When the slot clock fires an epoch transition before block processing
// crosses the boundary, the nonce for the next epoch (currentEpoch+1) is
// computed speculatively from the current epoch's data. This eliminates
// the forging gap at epoch boundaries where the leader schedule would
// otherwise be unavailable until a peer's block triggers epoch rollover.
func (ls *LedgerState) EpochNonce(epoch uint64) []byte {
	ls.RLock()
	// Check current epoch under read lock; copy nonce if it matches
	if epoch == ls.currentEpoch.EpochId {
		if len(ls.currentEpoch.Nonce) > 0 {
			nonce := make([]byte, len(ls.currentEpoch.Nonce))
			copy(nonce, ls.currentEpoch.Nonce)
			ls.RUnlock()
			return nonce
		}
		// In-memory nonce empty (e.g. after Mithril import) —
		// fall through to DB lookup
		ls.RUnlock()
		ep, err := ls.db.GetEpoch(epoch, nil)
		if err != nil {
			ls.config.Logger.Error(
				"failed to look up epoch nonce from DB",
				"epoch", epoch,
				"error", err,
			)
			return nil
		}
		if ep == nil || len(ep.Nonce) == 0 {
			return nil
		}
		nonce := make([]byte, len(ep.Nonce))
		copy(nonce, ep.Nonce)
		return nonce
	}
	// If the requested epoch is ahead of the ledger state (slot clock
	// fired an epoch transition before block processing caught up),
	// try to compute the nonce speculatively for the immediate next
	// epoch. The nonce depends only on data from the current (ending)
	// epoch, so it is computable before block processing catches up.
	if epoch > ls.currentEpoch.EpochId {
		if epoch == ls.currentEpoch.EpochId+1 {
			currentEpoch := ls.currentEpoch
			currentEra := ls.currentEra
			ls.RUnlock()
			return ls.computeNextEpochNonce(currentEpoch, currentEra)
		}
		ls.RUnlock()
		return nil
	}
	ls.RUnlock()

	// For historical epochs, look up in database without holding the lock
	ep, err := ls.db.GetEpoch(epoch, nil)
	if err != nil {
		ls.config.Logger.Error(
			"failed to look up epoch nonce",
			"epoch", epoch,
			"error", err,
		)
		return nil
	}
	if ep == nil || len(ep.Nonce) == 0 {
		return nil
	}
	// Return a defensive copy so callers cannot mutate internal state
	nonce := make([]byte, len(ep.Nonce))
	copy(nonce, ep.Nonce)
	return nonce
}

// nextEpochNonceReadyCutoffSlot returns the slot at which the current epoch's
// candidate nonce stops changing, which is when the next epoch's nonce is
// stable and the next leader schedule can be precomputed.
func (ls *LedgerState) nextEpochNonceReadyCutoffSlot(
	currentEpoch models.Epoch,
) (uint64, bool) {
	epochLength := uint64(currentEpoch.LengthInSlots)
	if epochLength == 0 {
		return 0, false
	}
	stabilityWindow := ls.nonceStabilityWindow(currentEpoch.EraId)
	if stabilityWindow >= epochLength {
		return currentEpoch.StartSlot, true
	}
	return currentEpoch.StartSlot + epochLength - stabilityWindow, true
}

// computeNextEpochNonce speculatively computes the epoch nonce for the
// next epoch (currentEpoch.EpochId + 1) using data from the current epoch.
// Uses the current epoch's last block hash, carrying the stored
// lastEpochBlockNonce only when the epoch has no blocks.
//
// Returns nil if the nonce cannot be computed (e.g., missing block data,
// Byron era, or missing genesis config).
func (ls *LedgerState) computeNextEpochNonce(
	currentEpoch models.Epoch,
	currentEra eras.EraDesc,
) []byte {
	// No epoch nonce in Byron
	if currentEra.Id == 0 {
		return nil
	}
	nextEpochStartSlot := currentEpoch.StartSlot +
		uint64(currentEpoch.LengthInSlots)
	nonce, _, _, _, err := ls.computeEpochNonceForSlot(
		nextEpochStartSlot,
		currentEpoch,
	)
	if err != nil {
		ls.config.Logger.Warn(
			"failed to compute next epoch nonce",
			"component", "ledger",
			"current_epoch", currentEpoch.EpochId,
			"next_epoch", currentEpoch.EpochId+1,
			"error", err,
		)
		return nil
	}
	ls.config.Logger.Debug(
		"speculative epoch nonce computed for next epoch",
		"component", "ledger",
		"next_epoch", currentEpoch.EpochId+1,
		"epoch_nonce", hex.EncodeToString(nonce),
	)
	return nonce
}

// SlotsPerEpoch returns the number of slots in an epoch for the current era.
func (ls *LedgerState) SlotsPerEpoch() uint64 {
	ls.RLock()
	currentEra := ls.currentEra
	ls.RUnlock()

	if currentEra.EpochLengthFunc == nil {
		return 0
	}
	_, epochLength, err := currentEra.EpochLengthFunc(
		ls.config.CardanoNodeConfig,
	)
	if err != nil {
		return 0
	}
	return uint64(epochLength) // #nosec G115 -- epoch length is always positive
}

// ActiveSlotCoeff returns the active slot coefficient (f parameter).
// This is used in the Ouroboros Praos leader election probability.
func (ls *LedgerState) ActiveSlotCoeff() float64 {
	if ls.config.CardanoNodeConfig == nil {
		return 0
	}
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil || shelleyGenesis.ActiveSlotsCoeff.Rat == nil {
		return 0
	}
	rat := shelleyGenesis.ActiveSlotsCoeff.Rat
	num := rat.Num().Int64()
	denom := rat.Denom().Int64()
	if denom == 0 {
		return 0
	}
	return float64(num) / float64(denom)
}

// activeSlotCoeffRat returns the active slot coefficient as a *big.Rat,
// preserving the full precision from the Shelley genesis without a
// float64 roundtrip. Returns nil when the genesis is unavailable.
func (ls *LedgerState) activeSlotCoeffRat() *big.Rat {
	if ls.config.CardanoNodeConfig == nil {
		return nil
	}
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil || shelleyGenesis.ActiveSlotsCoeff.Rat == nil {
		return nil
	}
	return shelleyGenesis.ActiveSlotsCoeff.Rat
}

// Database returns the underlying database for transaction operations.
func (ls *LedgerState) Database() *database.Database {
	return ls.db
}

// SlotsPerKESPeriod returns the number of slots in a KES period.
func (ls *LedgerState) SlotsPerKESPeriod() uint64 {
	if slotsPerKESPeriod := ls.slotsPerKESPeriod.Load(); slotsPerKESPeriod != 0 {
		return slotsPerKESPeriod
	}
	slotsPerKESPeriod := ls.loadSlotsPerKESPeriod()
	if slotsPerKESPeriod == 0 {
		return 0
	}
	ls.slotsPerKESPeriod.Store(slotsPerKESPeriod)
	return slotsPerKESPeriod
}

func (ls *LedgerState) loadSlotsPerKESPeriod() uint64 {
	if ls.config.CardanoNodeConfig == nil {
		return 0
	}
	shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis()
	if shelleyGenesis == nil {
		return 0
	}
	slotsPerKESPeriod := shelleyGenesis.SlotsPerKESPeriod
	if slotsPerKESPeriod < 0 {
		return 0
	}
	return uint64(
		slotsPerKESPeriod,
	) // #nosec G115 -- validated non-negative above
}

// CurrentSlot returns the current slot number based on wall-clock time.
// Delegates to the internal slot clock.
func (ls *LedgerState) CurrentSlot() (uint64, error) {
	if ls.slotClock == nil {
		return 0, errors.New("slot clock not initialized")
	}
	return ls.slotClock.CurrentSlot()
}

// CurrentOrTipSlot returns the current wall-clock slot if available, or the
// current chain tip slot when the slot clock is unavailable. When both are
// available, it returns whichever slot is ahead.
func (ls *LedgerState) CurrentOrTipSlot() uint64 {
	tipSlot := ls.Tip().Point.Slot
	currentSlot, err := ls.CurrentSlot()
	if err != nil || currentSlot < tipSlot {
		return tipSlot
	}
	return currentSlot
}

// NextSlotTime returns the wall-clock time when the next slot begins.
func (ls *LedgerState) NextSlotTime() (time.Time, error) {
	if ls.slotClock == nil {
		return time.Time{}, errors.New("slot clock not initialized")
	}
	return ls.slotClock.NextSlotTime()
}

// NewView creates a new LedgerView for querying ledger state within a transaction.
func (ls *LedgerState) NewView(txn *database.Txn) *LedgerView {
	return &LedgerView{
		ls:  ls,
		txn: txn,
	}
}

// TransactionByHash returns a transaction record by its hash.
func (ls *LedgerState) TransactionByHash(
	hash []byte,
) (*models.Transaction, error) {
	return ls.db.GetTransactionByHash(hash, nil)
}

// BlockByHash returns a block by its hash.
func (ls *LedgerState) BlockByHash(hash []byte) (models.Block, error) {
	return database.BlockByHash(ls.db, hash)
}

// CardanoNodeConfig returns the Cardano node configuration used for this ledger state.
func (ls *LedgerState) CardanoNodeConfig() *cardano.CardanoNodeConfig {
	return ls.config.CardanoNodeConfig
}

// UtxoByRef returns a single UTxO by reference
func (ls *LedgerState) UtxoByRef(
	txId []byte,
	outputIdx uint32,
) (*models.Utxo, error) {
	return ls.db.UtxoByRef(txId, outputIdx, nil)
}

// UtxosByAddress returns all UTxOs that belong to the specified address
func (ls *LedgerState) UtxosByAddress(
	addr ledger.Address,
) ([]models.Utxo, error) {
	utxos, err := ls.db.UtxosByAddress(addr, nil)
	if err != nil {
		return nil, err
	}
	ret := make([]models.Utxo, 0, len(utxos))
	ret = append(ret, utxos...)
	return ret, nil
}

// UtxosByAddressWithOrdering returns UTxOs matching q with ordering metadata.
// See models.UtxoWithOrderingQuery (nil SearchUtxos predicate: MatchAllAddresses).
func (ls *LedgerState) UtxosByAddressWithOrdering(
	q *models.UtxoWithOrderingQuery,
) ([]models.UtxoWithOrdering, error) {
	utxos, err := ls.db.UtxosByAddressWithOrdering(q, nil)
	if err != nil {
		return nil, err
	}
	return utxos, nil
}

// UtxosByAddressAtSlot returns all UTxOs belonging to the
// specified address that existed at the given slot.
func (ls *LedgerState) UtxosByAddressAtSlot(
	addr lcommon.Address,
	slot uint64,
) ([]models.Utxo, error) {
	return ls.db.UtxosByAddressAtSlot(addr, slot, nil)
}

// UtxoByRefIncludingSpent returns a UTxO by reference, including
// spent outputs. This is needed for APIs that must resolve consumed
// inputs to display source address and amount.
func (ls *LedgerState) UtxoByRefIncludingSpent(
	txId []byte,
	outputIdx uint32,
) (*models.Utxo, error) {
	return ls.db.UtxoByRefIncludingSpent(txId, outputIdx, nil)
}

// GetTransactionsByBlockHash returns all transactions for a given
// block hash.
func (ls *LedgerState) GetTransactionsByBlockHash(
	blockHash []byte,
) ([]models.Transaction, error) {
	return ls.db.GetTransactionsByBlockHash(blockHash, nil)
}

// GetTransactionsByHashes returns transactions for the provided hashes.
func (ls *LedgerState) GetTransactionsByHashes(
	hashes [][]byte,
) ([]models.Transaction, error) {
	txs, err := ls.db.GetTransactionsByHashes(hashes, nil)
	if err != nil {
		return nil, fmt.Errorf("get transactions by hashes: %w", err)
	}
	return txs, nil
}

// GetTransactionsByAddress returns transactions involving the given
// address.
func (ls *LedgerState) GetTransactionsByAddress(
	addr lcommon.Address,
	limit int,
	offset int,
) ([]models.Transaction, error) {
	return ls.db.GetTransactionsByAddress(addr, limit, offset, nil)
}

// GetTransactionsByAddressWithOrder returns transactions
// involving the given address with explicit ordering.
func (ls *LedgerState) GetTransactionsByAddressWithOrder(
	addr lcommon.Address,
	limit int,
	offset int,
	order string,
) ([]models.Transaction, error) {
	txs, err := ls.db.GetTransactionsByAddressWithOrder(
		addr,
		limit,
		offset,
		order,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"get transactions by address (limit=%d offset=%d order=%s): %w",
			limit,
			offset,
			order,
			err,
		)
	}
	return txs, nil
}

// CountTransactionsByAddress returns the total number of
// transactions involving the given address.
func (ls *LedgerState) CountTransactionsByAddress(
	addr lcommon.Address,
) (int, error) {
	count, err := ls.db.CountTransactionsByAddress(addr, nil)
	if err != nil {
		return 0, fmt.Errorf("count transactions by address: %w", err)
	}
	return count, nil
}

// CountTransactionsByMetadataLabel returns the total number of transactions
// that include metadata for the requested label.
func (ls *LedgerState) CountTransactionsByMetadataLabel(
	label uint64,
) (int, error) {
	count, err := ls.db.CountTransactionsByMetadataLabel(label, nil)
	if err != nil {
		return 0, fmt.Errorf(
			"count transactions by metadata label %d: %w",
			label,
			err,
		)
	}
	return count, nil
}

// CountTransactionsInSlotRange returns the number of transactions whose slot
// falls within the inclusive range [startSlot, endSlot].
// Used by the Blockfrost adapter CurrentEpoch() path so epoch responses can
// return real tx counts without decoding every block in the epoch on demand.
func (ls *LedgerState) CountTransactionsInSlotRange(
	startSlot, endSlot uint64,
) (int, error) {
	if endSlot < startSlot {
		return 0, nil
	}
	db, err := resolveMetadataQueryDB(ls)
	if err != nil {
		return 0, err
	}
	var count int64
	if err := db.
		Model(&models.Transaction{}).
		Where("slot >= ? AND slot <= ?", startSlot, endSlot).
		Count(&count).Error; err != nil {
		return 0, fmt.Errorf(
			"count transactions in slot range %d-%d: %w",
			startSlot,
			endSlot,
			err,
		)
	}
	return int(count), nil
}

func resolveMetadataQueryDB(ls *LedgerState) (*gorm.DB, error) {
	metaStore := ls.db.Metadata()
	if metaStore == nil {
		return nil, errors.New("metadata store unavailable")
	}
	if reader, ok := metaStore.(metadataReadDbReader); ok {
		if db := reader.ReadDB(); db != nil {
			return db, nil
		}
	}
	if reader, ok := metaStore.(metadataDbReader); ok {
		if db := reader.DB(); db != nil {
			return db, nil
		}
	}
	return nil, errors.New(
		"metadata store does not expose database handle",
	)
}

// CountBlocksInSlotRange returns the number of canonical blocks in the
// inclusive slot range [startSlot, endSlot], along with the first and last
// block slots found. It uses canonical metadata rows instead of raw blob keys
// so orphaned fork blocks do not leak into Blockfrost epoch responses.
func (ls *LedgerState) CountBlocksInSlotRange(
	startSlot, endSlot uint64,
) (int, uint64, uint64, error) {
	if endSlot < startSlot {
		return 0, 0, 0, nil
	}
	db, err := resolveMetadataQueryDB(ls)
	if err != nil {
		return 0, 0, 0, err
	}
	type blockRangeStats struct {
		Count     int64
		FirstSlot *uint64
		LastSlot  *uint64
	}
	var stats blockRangeStats
	if err := db.
		Model(&models.BlockNonce{}).
		Select(
			"COUNT(*) AS count, MIN(slot) AS first_slot, MAX(slot) AS last_slot",
		).
		Where("slot >= ? AND slot <= ?", startSlot, endSlot).
		Scan(&stats).Error; err != nil {
		return 0, 0, 0, fmt.Errorf(
			"count blocks in slot range %d-%d: %w",
			startSlot,
			endSlot,
			err,
		)
	}
	if stats.Count == 0 || stats.FirstSlot == nil || stats.LastSlot == nil {
		return 0, 0, 0, nil
	}
	return int(stats.Count), *stats.FirstSlot, *stats.LastSlot, nil
}

// resolveValidationEra determines the appropriate era descriptor for
// validating a transaction. It returns the current era if the transaction
// matches, the previous era if compatible (era-1), or an error if the
// transaction era is not compatible with the current ledger era.
func resolveValidationEra(
	tx lcommon.Transaction,
	currentEra eras.EraDesc,
	eraList []eras.EraDesc,
) (eras.EraDesc, error) {
	txEraId := uint(tx.Type()) // #nosec G115 -- era IDs are non-negative
	if txEraId == currentEra.Id {
		return currentEra, nil
	}
	if !eras.IsCompatibleEraIn(eraList, txEraId, currentEra.Id) {
		// Typed *gledger.EraMismatch carries the Haskell-canonical
		// wire format via MarshalCBOR; localtxsubmission's
		// encodeRejectReason picks it up via errors.As and emits
		// canonical CBOR to peers.
		return eras.EraDesc{}, newEraMismatchError(txEraId, currentEra.Id)
	}
	txEra := eras.GetEraByIdIn(eraList, txEraId)
	if txEra == nil {
		return eras.EraDesc{}, fmt.Errorf(
			"TX %s era %d not found in era registry",
			tx.Hash(),
			txEraId,
		)
	}
	return *txEra, nil
}

func validationReferenceSlot(
	tipSlot uint64,
	currentSlot uint64,
	currentSlotErr error,
) uint64 {
	if currentSlotErr == nil && currentSlot > tipSlot {
		return currentSlot
	}
	return tipSlot
}

// validateTxCore is the shared validation flow for ValidateTx and
// ValidateTxWithOverlay. It snapshots ledger state, resolves the
// validation era, opens a DB transaction, and invokes the era's
// ValidateTxFunc with a LedgerView built by the provided callback.
func (ls *LedgerState) validateTxCore(
	tx lcommon.Transaction,
	buildLV func(txn *database.Txn) *LedgerView,
) error {
	ls.RLock()
	snapshotEra := ls.currentEra
	snapshotTipSlot := ls.currentTip.Point.Slot
	snapshotPParams := ls.currentPParams
	snapshotPrevEraPParams := ls.prevEraPParams
	ls.RUnlock()
	currentSlot, currentSlotErr := ls.CurrentSlot()
	if currentSlotErr != nil {
		ls.config.Logger.Debug(
			"slot clock unavailable during tx validation, falling back to snapshot tip slot",
			"error",
			currentSlotErr,
			"snapshot_tip_slot",
			snapshotTipSlot,
		)
		ls.metrics.slotClockFallbacks.Inc()
	}
	snapshotSlot := validationReferenceSlot(
		snapshotTipSlot,
		currentSlot,
		currentSlotErr,
	)

	validationEra, err := resolveValidationEra(
		tx,
		snapshotEra,
		ls.eraList(),
	)
	if err != nil {
		return err
	}
	if validationEra.ValidateTxFunc != nil {
		pp := snapshotPParams
		if validationEra.Id != snapshotEra.Id && snapshotPrevEraPParams != nil {
			pp = snapshotPrevEraPParams
		}
		txn := ls.db.Transaction(false)
		err := txn.Do(func(txn *database.Txn) error {
			return validationEra.ValidateTxFunc(
				tx,
				snapshotSlot,
				buildLV(txn),
				pp,
			)
		})
		if err != nil {
			return fmt.Errorf("TX %s failed validation: %w", tx.Hash(), err)
		}
	}
	return nil
}

// ValidateTx runs ledger validation on the provided transaction.
// It accepts transactions from the current era and the immediately
// previous era (era-1), as Cardano allows during the overlap
// period after a hard fork.
func (ls *LedgerState) ValidateTx(
	tx lcommon.Transaction,
) error {
	return ls.validateTxCore(tx, func(txn *database.Txn) *LedgerView {
		return &LedgerView{txn: txn, ls: ls}
	})
}

// ValidateTxWithOverlay runs ledger validation with a UTxO overlay from pending
// mempool transactions. consumedUtxos contains inputs already spent by pending TXs
// (double-spend check), createdUtxos contains outputs created by pending TXs
// (dependent TX chaining). Both may be nil for no overlay.
func (ls *LedgerState) ValidateTxWithOverlay(
	tx lcommon.Transaction,
	consumedUtxos map[string]struct{},
	createdUtxos map[string]lcommon.Utxo,
) error {
	return ls.validateTxCore(tx, func(txn *database.Txn) *LedgerView {
		return &LedgerView{
			txn:             txn,
			ls:              ls,
			intraBlockUtxos: createdUtxos,
			consumedUtxos:   consumedUtxos,
		}
	})
}

// EvaluateTx evaluates the scripts in the provided transaction and returns the calculated
// fee, per-redeemer ExUnits, and total ExUnits
func (ls *LedgerState) EvaluateTx(
	tx lcommon.Transaction,
) (uint64, lcommon.ExUnits, map[lcommon.RedeemerKey]lcommon.ExUnits, error) {
	// Snapshot mutable state under read lock to avoid data races
	ls.RLock()
	snapshotEra := ls.currentEra
	snapshotPParams := ls.currentPParams
	snapshotPrevEraPParams := ls.prevEraPParams
	ls.RUnlock()

	validationEra, err := resolveValidationEra(
		tx,
		snapshotEra,
		ls.eraList(),
	)
	if err != nil {
		return 0, lcommon.ExUnits{}, nil, err
	}

	var fee uint64
	var totalExUnits lcommon.ExUnits
	var redeemerExUnits map[lcommon.RedeemerKey]lcommon.ExUnits
	if validationEra.EvaluateTxFunc != nil {
		// Use the previous era's protocol parameters when evaluating
		// a transaction from the immediately previous era (era-1).
		pp := snapshotPParams
		if validationEra.Id != snapshotEra.Id && snapshotPrevEraPParams != nil {
			pp = snapshotPrevEraPParams
		}
		txn := ls.db.Transaction(false)
		err := txn.Do(func(txn *database.Txn) error {
			lv := &LedgerView{
				txn: txn,
				ls:  ls,
			}
			var err error
			fee, totalExUnits, redeemerExUnits, err = validationEra.EvaluateTxFunc(
				tx,
				lv,
				pp,
			)
			return err
		})
		if err != nil {
			return 0, lcommon.ExUnits{}, nil, fmt.Errorf(
				"TX %s failed evaluation: %w",
				tx.Hash(),
				err,
			)
		}
	}
	return fee, totalExUnits, redeemerExUnits, nil
}

// Sets the mempool for accessing transactions
func (ls *LedgerState) SetMempool(mempool MempoolProvider) {
	ls.mempool = mempool
}

// SetForgingEnabled sets the forging_enabled metric gauge. Call with
// true after the block forger has been initialised successfully.
func (ls *LedgerState) SetForgingEnabled(enabled bool) {
	if enabled {
		ls.metrics.forgingEnabled.Set(1)
	} else {
		ls.metrics.forgingEnabled.Set(0)
	}
}

// SetForgedBlockChecker sets the forged block checker used for slot
// battle detection. This is typically called after the block forger
// is initialized, since the forger is created after the ledger state.
func (ls *LedgerState) SetForgedBlockChecker(checker ForgedBlockChecker) {
	ls.Lock()
	defer ls.Unlock()
	ls.config.ForgedBlockChecker = checker
	ls.storeForgedBlockChecker(checker)
}

// SetSlotBattleRecorder sets the recorder used to increment the
// slot battle metric. This is typically called after the block
// forger is initialized.
func (ls *LedgerState) SetSlotBattleRecorder(
	recorder SlotBattleRecorder,
) {
	ls.Lock()
	defer ls.Unlock()
	ls.config.SlotBattleRecorder = recorder
	ls.storeSlotBattleRecorder(recorder)
}

func (ls *LedgerState) storeForgedBlockChecker(checker ForgedBlockChecker) {
	if checker == nil {
		ls.forgedBlockChecker.Store(nil)
		return
	}
	ls.forgedBlockChecker.Store(
		&forgedBlockCheckerHolder{checker: checker},
	)
}

func (ls *LedgerState) loadForgedBlockChecker() ForgedBlockChecker {
	checker := ls.forgedBlockChecker.Load()
	if checker == nil {
		// Support direct test fixtures that construct LedgerState without
		// going through NewLedgerState/SetForgedBlockChecker.
		return ls.config.ForgedBlockChecker
	}
	return checker.checker
}

func (ls *LedgerState) storeSlotBattleRecorder(
	recorder SlotBattleRecorder,
) {
	if recorder == nil {
		ls.slotBattleRecorder.Store(nil)
		return
	}
	ls.slotBattleRecorder.Store(
		&slotBattleRecorderHolder{recorder: recorder},
	)
}

func (ls *LedgerState) loadSlotBattleRecorder() SlotBattleRecorder {
	recorder := ls.slotBattleRecorder.Load()
	if recorder == nil {
		// Support direct test fixtures that construct LedgerState without
		// going through NewLedgerState/SetSlotBattleRecorder.
		return ls.config.SlotBattleRecorder
	}
	return recorder.recorder
}

// RecordForgedBlock records observability for a block that this node
// successfully forged. Adoption into the local chain is tracked separately.
func (ls *LedgerState) RecordForgedBlock(
	block ledger.Block,
	blockCbor []byte,
	forgingLatency time.Duration,
) {
	if ls == nil || block == nil {
		return
	}
	if ls.metrics.blocksForgedTotal != nil {
		ls.metrics.blocksForgedTotal.Inc()
	}
	if ls.metrics.blockForgingLatency != nil {
		ls.metrics.blockForgingLatency.Observe(
			forgingLatency.Seconds(),
		)
	}
	if ls.config.EventBus == nil {
		return
	}
	ls.config.EventBus.Publish(
		event.BlockForgedEventType,
		event.NewEvent(
			event.BlockForgedEventType,
			event.BlockForgedEvent{
				Slot:        block.SlotNumber(),
				BlockNumber: block.BlockNumber(),
				BlockHash:   block.Hash().Bytes(),
				TxCount:     uint(len(block.Transactions())),
				BlockSize:   uint(len(blockCbor)),
				Timestamp:   time.Now(),
			},
		),
	)
}

// forgeBlock creates a conway block with transactions from mempool
// Also adds it to the primary chain
func (ls *LedgerState) forgeBlock() {
	// Track timing for latency metric - start at beginning of forging process
	forgeStartTime := time.Now()

	// Get current chain tip
	currentTip := ls.chain.Tip()

	// Set Hash if empty
	if len(currentTip.Point.Hash) == 0 {
		currentTip.Point.Hash = make([]byte, 28)
	}

	// Calculate next slot and block number
	nextSlot, err := ls.TimeToSlot(time.Now())
	if err != nil {
		ls.config.Logger.Error(
			"failed to calculate slot from current time",
			"component", "ledger",
			"error", err,
		)
		return
	}
	nextBlockNumber := currentTip.BlockNumber + 1

	// Get current protocol parameters for limits
	pparams := ls.GetCurrentPParams()
	if pparams == nil {
		ls.config.Logger.Error(
			"failed to get protocol parameters",
			"component", "ledger",
		)
		return
	}

	// Safely cast protocol parameters to Conway type
	conwayPParams, ok := pparams.(*conway.ConwayProtocolParameters)
	if !ok {
		ls.config.Logger.Error(
			"protocol parameters are not Conway type",
			"component", "ledger",
		)
		return
	}

	var (
		transactionBodies      []conway.ConwayTransactionBody
		transactionWitnessSets []conway.ConwayTransactionWitnessSet
		transactionMetadataSet = make(map[uint]cbor.RawMessage)
		includedTxHashes       []string
		blockSize              uint64
		totalExUnits           lcommon.ExUnits
		maxTxSize              = uint64(conwayPParams.MaxTxSize)
		maxBlockSize           = uint64(conwayPParams.MaxBlockBodySize)
		maxExUnits             = conwayPParams.MaxBlockExUnits
	)

	ls.config.Logger.Debug(
		"protocol parameter limits",
		"component", "ledger",
		"max_tx_size", maxTxSize,
		"max_block_size", maxBlockSize,
		"max_ex_units", maxExUnits,
	)

	var mempoolTxs []PendingTransaction
	if ls.mempool != nil {
		mempoolTxs = ls.mempool.Transactions()
		ls.config.Logger.Debug(
			"found transactions in mempool",
			"component", "ledger",
			"tx_count", len(mempoolTxs),
		)

		// Iterate through transactions and add them until we hit limits
		for _, mempoolTx := range mempoolTxs {
			// Use raw CBOR from the mempool transaction
			txCbor := mempoolTx.Cbor
			txSize := uint64(len(txCbor))

			// Check MaxTxSize limit
			if txSize > maxTxSize {
				ls.config.Logger.Debug(
					"skipping transaction - exceeds MaxTxSize",
					"component", "ledger",
					"tx_size", txSize,
					"max_tx_size", maxTxSize,
				)
				continue
			}

			// Check MaxBlockSize limit
			if blockSize+txSize > maxBlockSize {
				ls.config.Logger.Debug(
					"block size limit reached",
					"component", "ledger",
					"current_size", blockSize,
					"tx_size", txSize,
					"max_block_size", maxBlockSize,
				)
				break
			}

			// Decode the transaction CBOR into full Conway transaction
			fullTx, err := conway.NewConwayTransactionFromCbor(txCbor)
			if err != nil {
				ls.config.Logger.Debug(
					"failed to decode full transaction, skipping",
					"component", "ledger",
					"error", err,
				)
				continue
			}

			// Pull ExUnits from redeemers in the witness set
			var estimatedTxExUnits lcommon.ExUnits
			var exUnitsErr error
			for _, redeemer := range fullTx.WitnessSet.Redeemers().Iter() {
				estimatedTxExUnits, exUnitsErr = eras.SafeAddExUnits(
					estimatedTxExUnits,
					redeemer.ExUnits,
				)
				if exUnitsErr != nil {
					ls.config.Logger.Debug(
						"skipping transaction - ExUnits overflow",
						"component", "ledger",
						"error", exUnitsErr,
					)
					break
				}
			}
			if exUnitsErr != nil {
				continue
			}

			// Check MaxExUnits limit - skip this tx but
			// continue trying smaller ones.
			// Use SafeAddExUnits to avoid overflow in the
			// comparison.
			candidateExUnits, addErr := eras.SafeAddExUnits(
				totalExUnits,
				estimatedTxExUnits,
			)
			if addErr != nil ||
				candidateExUnits.Memory > maxExUnits.Memory ||
				candidateExUnits.Steps > maxExUnits.Steps {
				ls.config.Logger.Debug(
					"tx exceeds remaining ex units budget, skipping",
					"component", "ledger",
					"current_memory", totalExUnits.Memory,
					"current_steps", totalExUnits.Steps,
					"tx_memory", estimatedTxExUnits.Memory,
					"tx_steps", estimatedTxExUnits.Steps,
					"max_memory", maxExUnits.Memory,
					"max_steps", maxExUnits.Steps,
				)
				continue
			}

			// Handle metadata encoding before adding transaction.
			// Prefer using the original auxiliary data CBOR bytes when available
			// to preserve the producer's encoding (important for metadata hash
			// calculations). Some producers place a single-byte CBOR simple-value
			// (0xF4 false, 0xF5 true, 0xF6 null) into the tx-level auxiliary
			// field as a placeholder; treat those as absent and fall back to
			// the decoded Metadata value or block-level metadata.
			var metadataCbor cbor.RawMessage
			if aux := fullTx.AuxiliaryData(); aux != nil {
				ac := aux.Cbor()
				if len(ac) > 0 &&
					(len(ac) != 1 || (ac[0] != 0xF6 && ac[0] != 0xF5 && ac[0] != 0xF4)) {
					metadataCbor = ac
				}
			}
			if metadataCbor == nil && fullTx.Metadata() != nil {
				var err error
				metadataCbor, err = cbor.Encode(fullTx.Metadata())
				if err != nil {
					ls.config.Logger.Debug(
						"failed to encode transaction metadata",
						"component", "ledger",
						"error", err,
					)
					continue
				}
			}

			// Add transaction to our lists for later block creation
			includedTxHashes = append(includedTxHashes, mempoolTx.Hash)
			transactionBodies = append(transactionBodies, fullTx.Body)
			transactionWitnessSets = append(
				transactionWitnessSets,
				fullTx.WitnessSet,
			)
			if metadataCbor != nil {
				transactionMetadataSet[uint(len(transactionBodies))-1] = metadataCbor
			}
			blockSize += txSize
			// Safe to assign: overflow was already checked
			// via SafeAddExUnits when computing
			// candidateExUnits above.
			totalExUnits = candidateExUnits

			ls.config.Logger.Debug(
				"added transaction to block candidate lists",
				"component", "ledger",
				"tx_size", txSize,
				"block_size", blockSize,
				"tx_count", len(transactionBodies),
				"total_memory", totalExUnits.Memory,
				"total_steps", totalExUnits.Steps,
			)
		}
	}

	// Process transaction metadata set
	var metadataSet lcommon.TransactionMetadataSet
	if len(transactionMetadataSet) > 0 {
		metadataCbor, err := cbor.Encode(transactionMetadataSet)
		if err != nil {
			ls.config.Logger.Error(
				"failed to encode transaction metadata set",
				"component", "ledger",
				"error", err,
			)
			return
		}
		err = metadataSet.UnmarshalCBOR(metadataCbor)
		if err != nil {
			ls.config.Logger.Error(
				"failed to unmarshal transaction metadata set",
				"component", "ledger",
				"error", err,
			)
			return
		}
	}

	// Create Babbage block header body
	headerBody := babbage.BabbageBlockHeaderBody{
		BlockNumber: nextBlockNumber,
		Slot:        nextSlot,
		PrevHash:    lcommon.NewBlake2b256(currentTip.Point.Hash),
		IssuerVkey:  lcommon.IssuerVkey{},
		VrfKey:      []byte{},
		VrfResult: lcommon.VrfResult{
			Output: lcommon.Blake2b256{}.Bytes(),
		},
		BlockBodySize: blockSize,
		BlockBodyHash: lcommon.Blake2b256{},
		OpCert:        babbage.BabbageOpCert{},
		// Keep header-field changes in sync with ledger/forging/builder.go:
		// this dev-mode path duplicates mempool iteration, ExUnits accounting,
		// metadata encoding, and header assembly.
		ProtoVersion: babbage.BabbageProtoVersion{
			Major: uint64(conwayPParams.ProtocolVersion.Major),
			Minor: dingoversion.BlockHeaderProtocolMinor,
		},
	}

	// Create Conway block header
	conwayHeader := &conway.ConwayBlockHeader{
		BabbageBlockHeader: babbage.BabbageBlockHeader{
			Body:      headerBody,
			Signature: []byte{},
		},
	}

	// Create a conway block with transactions
	conwayBlock := &conway.ConwayBlock{
		BlockHeader:            conwayHeader,
		TransactionBodies:      transactionBodies,
		TransactionWitnessSets: transactionWitnessSets,
		TransactionMetadataSet: metadataSet,
		InvalidTransactions:    []uint{},
	}

	// Marshal the conway block to CBOR
	blockCbor, err := cbor.Encode(conwayBlock)
	if err != nil {
		ls.config.Logger.Error(
			"failed to marshal forged conway block to CBOR",
			"component", "ledger",
			"error", err,
		)
		return
	}

	// Re-decode block from CBOR
	// This is a bit of a hack, because things like Hash() rely on having the original CBOR available
	ledgerBlock, err := conway.NewConwayBlockFromCbor(blockCbor)
	if err != nil {
		ls.config.Logger.Error(
			"failed to unmarshal forced Conway block from generated CBOR",
			"error", err,
		)
		return
	}

	forgingLatency := time.Since(forgeStartTime)
	ls.RecordForgedBlock(ledgerBlock, blockCbor, forgingLatency)

	// Add the block to the primary chain
	err = ls.chain.AddBlock(ledgerBlock, nil)
	if err != nil {
		ls.config.Logger.Error(
			"failed to add forged block to primary chain",
			"component", "ledger",
			"error", err,
		)
		return
	}

	// Synchronously evict confirmed transactions so the next forging slot
	// sees a clean mempool without waiting for the async ChainUpdateEvent
	// rebuild cycle.
	if ls.mempool != nil && len(includedTxHashes) > 0 {
		ls.mempool.RemoveTxsByHash(includedTxHashes)
	}

	// Wake chainsync server iterators so connected peers discover
	// the newly forged block immediately.
	ls.chain.NotifyIterators()

	// Log the successful block creation
	ls.config.Logger.Info(
		"successfully forged and added conway block to primary chain",
		"component", "ledger",
		"slot", ledgerBlock.SlotNumber(),
		"hash", ledgerBlock.Hash(),
		"block_number", ledgerBlock.BlockNumber(),
		"prev_hash", ledgerBlock.PrevHash(),
		"block_size", len(blockCbor),
		"block_body_size", blockSize,
		"tx_count", len(transactionBodies),
		"total_memory", totalExUnits.Memory,
		"total_steps", totalExUnits.Steps,
		"forging_latency_ms", forgingLatency.Milliseconds(),
	)
}
