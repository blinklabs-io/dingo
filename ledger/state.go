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
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/mempool"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	cleanupConsumedUtxosInterval = 5 * time.Minute
	batchSize                    = 50 // Number of blocks to process in a single DB transaction
)

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
	db         *database.Database
	taskQueue  chan DatabaseOperation
	drainQueue chan DatabaseOperation // Reference for draining after shutdown
	shutdownCh chan struct{}
	wg         sync.WaitGroup
	closed     atomic.Bool // Use atomic for thread-safe access without mutex in hot path
	mu         sync.Mutex
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
		db:         db,
		taskQueue:  taskQ,
		drainQueue: taskQ,
		shutdownCh: make(chan struct{}),
		// closed is zero-valued (false) by default for atomic.Bool
	}

	// Start workers
	for i := 0; i < config.WorkerPoolSize; i++ {
		pool.wg.Add(1)
		go pool.worker()
	}

	return pool
}

// worker runs a single database worker
func (p *DatabaseWorkerPool) worker() {
	defer p.wg.Done()

	for {
		select {
		case op := <-p.taskQueue:
			// Execute the database operation with panic protection
			func() {
				defer p.wg.Done()
				result := DatabaseResult{}
				defer func() {
					if r := recover(); r != nil {
						result.Error = fmt.Errorf("panic: %v", r)
						slog.Error("worker panic during operation", "panic", r)
					}
					// Send result whether it's from normal execution or panic
					if op.ResultChan != nil {
						select {
						case op.ResultChan <- result:
						default:
							// If result channel is full, skip (fire and forget)
						}
					}
				}()
				result.Error = op.OpFunc(p.db)
			}()
		case <-p.shutdownCh:
			// Shutdown signal received - process any remaining operations in the queue
			// This ensures no operations are lost during shutdown
		drainQueueLoop:
			for {
				select {
				case op := <-p.drainQueue:
					// Process remaining operation with panic protection
					func() {
						defer p.wg.Done()
						result := DatabaseResult{}
						defer func() {
							if r := recover(); r != nil {
								result.Error = fmt.Errorf("panic: %v", r)
								slog.Error("worker panic during drain operation", "panic", r)
							}
							// Send result whether it's from normal execution or panic
							if op.ResultChan != nil {
								select {
								case op.ResultChan <- result:
								default:
									// Result channel is full or closed
								}
							}
						}()
						result.Error = op.OpFunc(p.db)
					}()
				default:
					// Queue is empty, exit
					break drainQueueLoop
				}
			}
			return
		}
	}
}

// Submit submits a database operation for async execution
func (p *DatabaseWorkerPool) Submit(op DatabaseOperation) {
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		if op.ResultChan != nil {
			select {
			case op.ResultChan <- DatabaseResult{Error: errors.New("database worker pool is shut down")}:
			default:
				// If result channel is full, skip
			}
		}
		return
	}

	p.wg.Add(1)
	select {
	case p.taskQueue <- op:
		// Operation submitted successfully
	default:
		// Queue is full - undo the Add since operation won't be queued
		p.wg.Done()
		if op.ResultChan != nil {
			select {
			case op.ResultChan <- DatabaseResult{Error: errors.New("database worker pool queue full")}:
			default:
				// If result channel is full, skip
			}
		}
	}
	p.mu.Unlock()
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
	// Store a reference to the task queue for use after unlocking.
	// This avoids a race: we keep p.taskQueue unchanged so workers
	// can continue reading it without synchronization, while we drain
	// using a local reference.
	taskQueue := p.taskQueue
	p.mu.Unlock()

	close(p.shutdownCh)
	p.wg.Wait()

	// Drain any remaining operations from the queue and send shutdown errors
drainLoop:
	for {
		select {
		case op := <-taskQueue:
			// Send shutdown error to the operation's result channel
			if op.ResultChan != nil {
				select {
				case op.ResultChan <- DatabaseResult{Error: errors.New("database worker pool shutting down")}:
				default:
					// If result channel is full or closed, continue
				}
			}
		default:
			// No more operations in queue
			break drainLoop
		}
	}

	// Close the task queue to prevent further sends
	close(taskQueue)
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

// ForgedBlockChecker is an interface for checking whether the local
// node recently forged a block for a given slot. This is used by
// chainsync to detect slot battles when an incoming block from a
// peer occupies the same slot as a locally forged block.
type ForgedBlockChecker interface {
	// WasForgedByUs returns the block hash and true if the local node
	// forged a block for the given slot, or nil and false otherwise.
	WasForgedByUs(slot uint64) (blockHash []byte, ok bool)
}

type LedgerStateConfig struct {
	PromRegistry               prometheus.Registerer
	Logger                     *slog.Logger
	Database                   *database.Database
	ChainManager               *chain.ChainManager
	EventBus                   *event.EventBus
	CardanoNodeConfig          *cardano.CardanoNodeConfig
	BlockfetchRequestRangeFunc BlockfetchRequestRangeFunc
	GetActiveConnectionFunc    GetActiveConnectionFunc
	FatalErrorFunc             FatalErrorFunc
	ForgedBlockChecker         ForgedBlockChecker
	ValidateHistorical         bool
	ForgeBlocks                bool
	DatabaseWorkerPoolConfig   DatabaseWorkerPoolConfig
}

// BlockfetchRequestRangeFunc describes a callback function used to start a blockfetch request for
// a range of blocks
type BlockfetchRequestRangeFunc func(ouroboros.ConnectionId, ocommon.Point, ocommon.Point) error

// In ledger/state.go or a shared package
type MempoolProvider interface {
	Transactions() []mempool.MempoolTransaction
}
type rollbackRecord struct {
	slot      uint64
	timestamp time.Time
}

type LedgerState struct {
	metrics                            stateMetrics
	currentEra                         eras.EraDesc
	config                             LedgerStateConfig
	chainsyncBlockfetchTimeoutTimer    *time.Timer // timeout timer for blockfetch operations
	chainsyncBlockfetchTimerGeneration uint64      // generation counter to detect stale timer callbacks
	currentPParams                     lcommon.ProtocolParameters
	prevEraPParams                     lcommon.ProtocolParameters // pparams from the immediately previous era (for era-1 TX validation)
	mempool                            MempoolProvider
	timerCleanupConsumedUtxos          *time.Timer
	Scheduler                          *Scheduler
	chain                              *chain.Chain
	db                                 *database.Database
	chainsyncState                     ChainsyncState
	currentTipBlockNonce               []byte
	chainsyncBlockEvents               []BlockfetchEvent
	epochCache                         []models.Epoch
	currentTip                         ochainsync.Tip
	currentEpoch                       models.Epoch
	dbWorkerPool                       *DatabaseWorkerPool
	slotClock                          *SlotClock
	slotTickChan                       <-chan SlotTick
	ctx                                context.Context
	sync.RWMutex
	chainsyncMutex                sync.Mutex
	chainsyncBlockfetchMutex      sync.Mutex
	chainsyncBlockfetchReadyMutex sync.Mutex
	chainsyncBlockfetchReadyChan  chan struct{}
	checkpointWrittenForEpoch     bool
	closed                        atomic.Bool
	inRecovery                    bool // guards against recursive recovery in SubmitAsyncDBTxn

	// Subscription IDs for event bus unsubscribe on close
	chainsyncSubID   event.EventSubscriberId
	blockfetchSubID  event.EventSubscriberId
	chainUpdateSubID event.EventSubscriberId

	// rollbackMu serializes rollbackWG.Add with Close's rollbackWG.Wait
	// to prevent Add-after-Wait panics from the TOCTOU race between
	// closed.Load() and Add(1) in handleEventChainUpdate.
	rollbackMu sync.Mutex
	// rollbackWG tracks in-flight rollback event emission goroutines
	rollbackWG        sync.WaitGroup
	validationEnabled bool

	// Sync progress reporting (Fix 4)
	syncProgressLastLog  time.Time     // last time we logged sync progress
	syncProgressLastSlot uint64        // slot at last progress log (for rate calc)
	syncUpstreamTipSlot  atomic.Uint64 // upstream peer's tip slot

	// Rate-limiting for non-active connection drop messages (Fix 3)
	dropEventLastLog    time.Time // last time we logged a drop event
	dropEventCount      int64     // count of suppressed drop events since last log
	dropRollbackLastLog time.Time // last time we logged a drop rollback
	dropRollbackCount   int64     // count of suppressed drop rollbacks since last log

	rollbackHistory []rollbackRecord // recent rollback slot+time pairs for loop detection

	lastActiveConnId *ouroboros.ConnectionId // tracks active connection for switch detection
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
	// Initialize database worker pool config with defaults if not set
	if cfg.DatabaseWorkerPoolConfig.WorkerPoolSize == 0 &&
		cfg.DatabaseWorkerPoolConfig.TaskQueueSize == 0 {
		cfg.DatabaseWorkerPoolConfig = DefaultDatabaseWorkerPoolConfig()
	}
	ls := &LedgerState{
		config:            cfg,
		chainsyncState:    InitChainsyncState,
		db:                cfg.Database,
		chain:             cfg.ChainManager.PrimaryChain(),
		validationEnabled: cfg.ValidateHistorical,
	}
	return ls, nil
}

func (ls *LedgerState) Start(ctx context.Context) error {
	ls.ctx = ctx
	// Init metrics
	ls.metrics.init(ls.config.PromRegistry)

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

	// Setup event handlers
	if ls.config.EventBus != nil {
		ls.chainsyncSubID = ls.config.EventBus.SubscribeFunc(
			ChainsyncEventType,
			ls.handleEventChainsync,
		)
		ls.blockfetchSubID = ls.config.EventBus.SubscribeFunc(
			BlockfetchEventType,
			ls.handleEventBlockfetch,
		)
		ls.chainUpdateSubID = ls.config.EventBus.SubscribeFunc(
			chain.ChainUpdateEventType,
			ls.handleEventChainUpdate,
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
	// Load current tip
	if err := ls.loadTip(); err != nil {
		return fmt.Errorf("failed to load tip: %w", err)
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
	// Start goroutine to process new blocks
	go ls.ledgerProcessBlocks()
	return nil
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
			BlockfetchEventType,
			ls.blockfetchSubID,
		)
		ls.config.EventBus.Unsubscribe(
			chain.ChainUpdateEventType,
			ls.chainUpdateSubID,
		)
	}

	// Wait for in-flight rollback event emission goroutines.
	// Hold rollbackMu so no new goroutine can Add(1) between our
	// closed flag and this Wait.
	ls.rollbackMu.Lock()
	ls.rollbackWG.Wait()
	ls.rollbackMu.Unlock()

	// Stop slot clock
	if ls.slotClock != nil {
		ls.slotClock.Stop()
	}

	// Shutdown database worker pool
	if ls.dbWorkerPool != nil {
		ls.dbWorkerPool.Shutdown()
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
// When an epoch boundary is detected via slot timing, it can emit an
// EpochTransitionEvent for subscribers like snapshot managers and leader election.
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
		if !tick.IsEpochStart {
			continue
		}

		// Get current state snapshot
		ls.RLock()
		isSynced := ls.validationEnabled
		currentEpoch := ls.currentEpoch
		currentEra := ls.currentEra
		ls.RUnlock()

		// During catch up or load, don't emit slot-based epoch events.
		// Block processing handles epoch transitions for historical data.
		// The slot clock is ticking based on wall-clock time which is far ahead
		// of the blocks being processed.
		if !isSynced {
			logger.Debug(
				"slot clock epoch boundary during catch up (suppressed)",
				"slot_clock_epoch",
				tick.Epoch,
				"ledger_epoch",
				currentEpoch.EpochId,
				"slot",
				tick.Slot,
			)
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
		// Delete rolled-back UTxOs (blob offsets and metadata)
		err := ls.db.UtxosDeleteRolledback(point.Slot, txn)
		if err != nil {
			return fmt.Errorf("remove rolled-back UTxOs: %w", err)
		}
		// Delete rolled-back transaction offsets and metadata
		err = ls.db.TransactionsDeleteRolledback(point.Slot, txn)
		if err != nil {
			return fmt.Errorf("remove rolled-back transactions: %w", err)
		}
		// Restore spent UTxOs
		err = ls.db.UtxosUnspend(point.Slot, txn)
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
	// Reload protocol parameters to reflect the rolled-back state
	if err := ls.loadPParams(); err != nil {
		ls.config.Logger.Warn(
			"failed to reload protocol params after rollback",
			"error", err,
			"component", "ledger",
		)
	}
	// Transaction committed successfully - now update in-memory state.
	// Brief lock to ensure readers see consistent state.
	ls.Lock()
	ls.currentTip = newTip
	// Always update nonce - clear it on genesis rollback, set it otherwise
	ls.currentTipBlockNonce = newNonce
	ls.updateTipMetrics()
	ls.Unlock()
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
	nextEra := eras.Eras[nextEraId]
	result := &EraTransitionResult{
		NewPParams: currentPParams,
		NewEra:     nextEra,
	}
	if nextEra.HardForkFunc != nil {
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
	}
	return result, nil
}

// calculateStabilityWindow returns the stability window based on the current era.
// For Byron era, returns 2k. For Shelley+ eras, returns 3k/f.
// Returns the default threshold if genesis data is unavailable or invalid.
func (ls *LedgerState) calculateStabilityWindow() uint64 {
	return ls.calculateStabilityWindowForEra(ls.currentEra.Id)
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

// SecurityParam returns the security parameter for the current era
func (ls *LedgerState) SecurityParam() int {
	if ls.config.CardanoNodeConfig == nil {
		ls.config.Logger.Warn(
			"CardanoNodeConfig is nil, using default security parameter",
		)
		return blockfetchBatchSlotThresholdDefault
	}
	// Byron era only needs Byron genesis
	if ls.currentEra.Id == 0 {
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
		return int(k) // #nosec G115
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
	return int(k)
}

type readChainResult struct {
	rollbackPoint ocommon.Point
	blocks        []ledger.Block
	rollback      bool
}

func (ls *LedgerState) ledgerReadChain(
	ctx context.Context,
	resultCh chan readChainResult,
) {
	// Create chain iterator
	iter, err := ls.chain.FromPoint(ls.currentTip.Point, false)
	if err != nil {
		ls.config.Logger.Error(
			"failed to create chain iterator: " + err.Error(),
		)
		return
	}
	// Read blocks from chain iterator and decode
	var next, cachedNext *chain.ChainIteratorResult
	var tmpBlock ledger.Block
	var needsRollback, shouldBlock bool
	var rollbackPoint ocommon.Point
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
				// End existing batch and cache rollback if we have any blocks in the batch
				// We need special processing for rollbacks below
				if len(nextBatch) > 0 {
					cachedNext = next
					break
				}
				needsRollback = true
				rollbackPoint = next.Point
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
		if needsRollback {
			needsRollback = false
			result = readChainResult{
				rollback:      true,
				rollbackPoint: rollbackPoint,
			}
		} else {
			result = readChainResult{
				blocks: nextBatch,
			}
		}
		select {
		case resultCh <- result:
		case <-ctx.Done():
			return
		}
	}
}

func (ls *LedgerState) ledgerProcessBlocks() {
	// Start chain reader goroutine
	readChainResultCh := make(chan readChainResult)
	go ls.ledgerReadChain(ls.ctx, readChainResultCh)
	// Enable bulk-load optimizations when syncing from behind
	var bulkOptimizer metadata.BulkLoadOptimizer
	bulkLoadActive := false
	if !ls.validationEnabled {
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
					if bulkLoadActive {
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
	var delta *LedgerDelta
	var deltaBatch *LedgerDeltaBatch
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

				// Check for era change
				if nextEpochEraId != snapshotEra.Id {
					// Transition through every era between the current and the target era
					for nextEraId := snapshotEra.Id + 1; nextEraId <= nextEpochEraId; nextEraId++ {
						result, err := ls.transitionToEra(
							txn,
							nextEraId,
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
				}

				// Process epoch rollover
				result, err := ls.processEpochRollover(
					txn,
					snapshotEpoch,
					eras.Eras[workingEraId],
					workingPParams,
				)
				if err != nil {
					return err
				}
				rolloverResult = result
				return nil
			}, true)
			if err != nil {
				ls.config.Logger.Error(
					"failed to process epoch rollover: " + err.Error(),
				)
				return
			}

			// Apply in-memory state updates with brief lock after successful commit
			ls.Lock()
			for _, eraResult := range eraTransitions {
				// Preserve the pre-hard-fork pparams for era-1 TX validation
				ls.prevEraPParams = ls.currentPParams
				ls.currentPParams = eraResult.NewPParams
				ls.currentEra = eraResult.NewEra
			}
			if rolloverResult != nil {
				ls.epochCache = rolloverResult.NewEpochCache
				ls.currentEpoch = rolloverResult.NewCurrentEpoch
				ls.currentEra = rolloverResult.NewCurrentEra
				ls.currentPParams = rolloverResult.NewCurrentPParams
				ls.checkpointWrittenForEpoch = rolloverResult.CheckpointWrittenForEpoch
				ls.metrics.epochNum.Set(rolloverResult.NewEpochNum)
			}
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
						ls.config.Logger.Info(
							"emitted hard fork event"+
								" (era transition)",
							"from_era", prevEraId,
							"to_era",
							eraResult.NewEra.Id,
							"component", "ledger",
						)
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
			// Use cached block batch
			nextBatch = cachedNextBatch
			cachedNextBatch = nil
		} else {
			// Read next result from readChain channel
			select {
			case result, ok := <-readChainResultCh:
				if !ok {
					return
				}
				nextBatch = result.blocks
				// Process rollback
				// Note: We do NOT hold ls.Lock() here because rollback() calls
				// SubmitAsyncDBTxn() which may trigger PartialCommitError recovery
				// that re-acquires ls.Lock(), causing a deadlock. The rollback
				// method handles its own locking for in-memory state updates.
				if result.rollback {
					if err = ls.rollback(result.rollbackPoint); err != nil {
						ls.config.Logger.Error(
							"failed to process rollback: " + err.Error(),
						)
						return
					}
					continue
				}
			case <-ls.ctx.Done():
				return
			}
		}
		// Process batch in groups of batchSize to stay under DB txn limits
		var tipForLog ochainsync.Tip
		for i = 0; i < len(nextBatch); i += batchSize {
			end = min(
				len(nextBatch),
				i+batchSize,
			)

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
			chainTipSlot := ls.chain.Tip().Point.Slot
			ls.RUnlock()

			// Compute stability window and cutoff slot outside the callback
			// to avoid reading ls fields without the lock. Use chain tip slot
			// (not ledger tip) to prevent enabling validation too early during
			// historical block sync.
			stabilityWindow := ls.calculateStabilityWindowForEra(snapshotEra.Id)
			var cutoffSlot uint64
			if chainTipSlot >= stabilityWindow {
				cutoffSlot = chainTipSlot - stabilityWindow
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
					// Determine if this block should be validated
					// Skip validation of historical blocks when ValidateHistorical=false, as they
					// were already validated by the network. However, validate blocks within the
					// k-slot stability window to ensure live blocks near the tip are validated.
					var shouldValidateBlock bool
					if snapshotValidationEnabled {
						shouldValidateBlock = true
					} else if snapshotChainsyncState == SyncingChainsyncState && next.SlotNumber() >= cutoffSlot {
						wantEnableValidation = true
						shouldValidateBlock = true
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
					// Process block
					delta, err = ls.ledgerProcessBlock(
						txn,
						tmpPoint,
						next,
						shouldValidateBlock,
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
					// Calculate block rolling nonce
					var blockNonce []byte
					if snapshotEra.CalculateEtaVFunc != nil {
						tmpEra := eras.Eras[next.Era().Id]
						if tmpEra.CalculateEtaVFunc != nil {
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
					// TODO: batch this
					// Determine epoch for this slot
					tmpEpoch, err := ls.SlotToEpoch(tmpPoint.Slot)
					if err != nil {
						deltaBatch.Release()
						return fmt.Errorf("slot->epoch: %w", err)
					}

					// First block we persist in the current epoch becomes the checkpoint
					isCheckpoint := false
					if tmpEpoch.EpochId == snapshotEpoch.EpochId &&
						!localCheckpointWritten {
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
				ls.config.Logger.Error(
					"failed to process block: " + err.Error(),
				)
				return
			}
			// Transaction committed successfully - now update in-memory state.
			// Only update if blocks were actually processed to avoid resetting tip to zero.
			if blocksProcessed > 0 {
				// Brief lock to ensure readers see consistent state.
				ls.Lock()
				ls.currentTip = pendingTip
				if len(pendingNonce) > 0 {
					ls.currentTipBlockNonce = pendingNonce
				}
				ls.checkpointWrittenForEpoch = localCheckpointWritten
				if wantEnableValidation {
					ls.validationEnabled = true
				}
				ls.updateTipMetrics()
				// Capture tip for logging while holding the lock
				tipForLog = ls.currentTip
				ls.Unlock()
				// Restore normal DB options outside the lock after validation is enabled
				if wantEnableValidation && bulkLoadActive {
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
			ls.config.Logger.Info(
				fmt.Sprintf(
					"chain extended, new tip: %x at slot %d",
					tipForLog.Point.Hash,
					tipForLog.Point.Slot,
				),
				"component",
				"ledger",
			)
			// Periodic sync progress reporting
			ls.logSyncProgress(tipForLog.Point.Slot)
		}
	}
}

func (ls *LedgerState) ledgerProcessBlock(
	txn *database.Txn,
	point ocommon.Point,
	block ledger.Block,
	shouldValidate bool,
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
				"block %s (with prev hash %s) does not fit on current chain tip (%x)",
				block.Hash().String(),
				block.PrevHash().String(),
				expectedPrevHash,
			)
		}
	}
	// Process transactions
	var delta *LedgerDelta
	// Track outputs from earlier transactions in this block for intra-block dependencies
	intraBlockUtxos := make(map[string]lcommon.Utxo)
	for i, tx := range block.Transactions() {
		if delta == nil {
			delta = NewLedgerDelta(point, uint(block.Era().Id), block.BlockNumber())
			delta.Offsets = offsets
		}
		// Validate transaction
		if shouldValidate {
			validationEra, err := resolveValidationEra(
				tx,
				currentEra,
			)
			if err != nil {
				delta.Release()
				return nil, err
			}
			if validationEra.ValidateTxFunc != nil {
				// Use the previous era's protocol
				// parameters when validating an era-1
				// transaction.
				pp := pparams
				if validationEra.Id != currentEra.Id &&
					prevEraPParams != nil {
					pp = prevEraPParams
				}
				lv := &LedgerView{
					txn:             txn,
					ls:              ls,
					intraBlockUtxos: intraBlockUtxos,
				}
				err := validationEra.ValidateTxFunc(
					tx,
					point.Slot,
					lv,
					pp,
				)
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
					return nil, fmt.Errorf("TX validation failure: %w", err)
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
	return delta, nil
}

func (ls *LedgerState) updateTipMetrics() {
	// Update metrics
	ls.metrics.blockNum.Set(float64(ls.currentTip.BlockNumber))
	ls.metrics.slotNum.Set(float64(ls.currentTip.Point.Slot))
	ls.metrics.slotInEpoch.Set(
		float64(ls.currentTip.Point.Slot - ls.currentEpoch.StartSlot),
	)
}

func (ls *LedgerState) loadPParams() error {
	pparams, err := ls.db.GetPParams(
		ls.currentEpoch.EpochId,
		ls.currentEra.DecodePParamsFunc,
		nil,
	)
	if err != nil {
		return err
	}
	if pparams == nil {
		pparams, err = ls.loadGenesisProtocolParameters()
		if err != nil {
			return fmt.Errorf("bootstrap genesis protocol parameters: %w", err)
		}
	}
	ls.currentPParams = pparams

	// Load previous era's pparams for era-1 TX validation.
	// Walk the epoch cache backwards to find the last epoch that belonged
	// to a different (earlier) era, then load its pparams.
	ls.prevEraPParams = nil
	for i := len(ls.epochCache) - 1; i >= 0; i-- {
		ep := ls.epochCache[i]
		if ep.EraId != ls.currentEra.Id {
			prevEra := eras.GetEraById(ep.EraId)
			if prevEra != nil && prevEra.DecodePParamsFunc != nil {
				prevPP, prevErr := ls.db.GetPParams(
					ep.EpochId,
					prevEra.DecodePParamsFunc,
					nil,
				)
				if prevErr == nil && prevPP != nil {
					ls.prevEraPParams = prevPP
				}
			}
			break
		}
	}
	return nil
}

func (ls *LedgerState) loadGenesisProtocolParameters() (lcommon.ProtocolParameters, error) {
	// Start with Shelley parameters as the base for all eras (Byron also uses Shelley as base)
	pparams, err := eras.HardForkShelley(ls.config.CardanoNodeConfig, nil)
	if err != nil {
		return nil, err
	}

	// If target era is Byron or Shelley, return the Shelley parameters
	if ls.currentEra.Id <= eras.ShelleyEraDesc.Id {
		return pparams, nil
	}

	// Chain through each era up to the target era
	for eraId := eras.AllegraEraDesc.Id; eraId <= ls.currentEra.Id; eraId++ {
		era := eras.GetEraById(eraId)
		if era == nil {
			return nil, fmt.Errorf("unknown era ID %d", eraId)
		}

		pparams, err = era.HardForkFunc(ls.config.CardanoNodeConfig, pparams)
		if err != nil {
			return nil, fmt.Errorf("era %s transition: %w", era.Name, err)
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
	ls.epochCache = epochs
	if len(epochs) > 0 {
		// Set current epoch and era
		ls.currentEpoch = epochs[len(epochs)-1]
		eraDesc := eras.GetEraById(ls.currentEpoch.EraId)
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
	startEra := eras.ProtocolMajorVersionToEra[startProtoVersion]
	// Initialize current era to Byron when starting from genesis
	ls.currentEra = eras.Eras[0] // Byron era
	// Transition through every era between the current and the target era
	// During startup, it's safe to apply results immediately since there's
	// no concurrent access.
	for nextEraId := ls.currentEra.Id + 1; nextEraId <= startEra.Id; nextEraId++ {
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
		// Apply result immediately during startup
		// Preserve the pre-hard-fork pparams for era-1 TX validation
		ls.prevEraPParams = ls.currentPParams
		ls.currentPParams = result.NewPParams
		ls.currentEra = result.NewEra
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
	ls.currentEpoch = rolloverResult.NewCurrentEpoch
	ls.currentEra = rolloverResult.NewCurrentEra
	ls.currentPParams = rolloverResult.NewCurrentPParams
	ls.checkpointWrittenForEpoch = rolloverResult.CheckpointWrittenForEpoch
	ls.metrics.epochNum.Set(rolloverResult.NewEpochNum)
	return nil
}

func (ls *LedgerState) loadTip() error {
	tmpTip, err := ls.db.GetTip(nil)
	if err != nil {
		return err
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
	// Lock only for in-memory state updates
	ls.Lock()
	ls.currentTip = tmpTip
	if tmpTip.Point.Slot > 0 {
		ls.currentTipBlockNonce = tipNonce
	}
	ls.updateTipMetrics()
	ls.Unlock()
	return nil
}

func (ls *LedgerState) GetBlock(point ocommon.Point) (models.Block, error) {
	ret, err := ls.chain.BlockByPoint(point, nil)
	if err != nil {
		return models.Block{}, err
	}
	return ret, nil
}

// RecentChainPoints returns the requested count of recent chain
// points in descending order. This is used mostly for building a set
// of intersect points when acting as a chainsync client.
//
// Points are first collected from the in-memory chain state, which is
// always up-to-date even when the blob store has not yet flushed
// recent writes. Database points are then appended to fill the
// requested count, with duplicates removed.
func (ls *LedgerState) RecentChainPoints(
	count int,
) ([]ocommon.Point, error) {
	var points []ocommon.Point
	// Collect points from the in-memory chain first. The chain's
	// tip and recent blocks are always current, even when the
	// underlying blob store has pending writes that are not yet
	// visible to new read transactions.
	if ls.chain != nil {
		points = ls.chain.RecentPoints(count)
	}
	// Supplement with database points for deeper history
	if len(points) < count {
		remaining := count - len(points)
		tmpBlocks, err := database.BlocksRecent(
			ls.db, remaining,
		)
		if err != nil {
			// If we already have in-memory points, a database
			// error is non-fatal: return what we have.
			if len(points) > 0 {
				return points, nil
			}
			return nil, err
		}
		// Build a set of existing points for deduplication
		seen := make(map[string]struct{}, len(points))
		for _, p := range points {
			seen[pointKey(p)] = struct{}{}
		}
		for _, blk := range tmpBlocks {
			p := ocommon.NewPoint(blk.Slot, blk.Hash)
			key := pointKey(p)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			points = append(points, p)
			if len(points) >= count {
				break
			}
		}
	}
	return points, nil
}

// pointKey returns a string key for deduplication of chain points.
func pointKey(p ocommon.Point) string {
	return fmt.Sprintf("%d:%x", p.Slot, p.Hash)
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

// Tip returns the current chain tip
func (ls *LedgerState) Tip() ochainsync.Tip {
	ls.RLock()
	defer ls.RUnlock()
	return ls.currentTip
}

// ChainTipSlot returns the slot number of the current chain tip.
func (ls *LedgerState) ChainTipSlot() uint64 {
	ls.RLock()
	defer ls.RUnlock()
	return ls.currentTip.Point.Slot
}

// UpstreamTipSlot returns the latest known tip slot from upstream peers.
// Returns 0 if no upstream tip is known yet.
func (ls *LedgerState) UpstreamTipSlot() uint64 {
	return ls.syncUpstreamTipSlot.Load()
}

// GetCurrentPParams returns the currentPParams value
func (ls *LedgerState) GetCurrentPParams() lcommon.ProtocolParameters {
	return ls.currentPParams
}

// CurrentEpoch returns the current epoch number.
func (ls *LedgerState) CurrentEpoch() uint64 {
	ls.RLock()
	defer ls.RUnlock()
	return ls.currentEpoch.EpochId
}

// EpochNonce returns the nonce for the given epoch.
// The epoch nonce is used for VRF-based leader election.
// Returns nil if the epoch nonce is not available (e.g., for Byron era).
func (ls *LedgerState) EpochNonce(epoch uint64) []byte {
	// Check current epoch under read lock; copy nonce if it matches
	ls.RLock()
	if epoch == ls.currentEpoch.EpochId {
		if len(ls.currentEpoch.Nonce) == 0 {
			ls.RUnlock()
			return nil
		}
		nonce := make([]byte, len(ls.currentEpoch.Nonce))
		copy(nonce, ls.currentEpoch.Nonce)
		ls.RUnlock()
		return nonce
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

// Database returns the underlying database for transaction operations.
func (ls *LedgerState) Database() *database.Database {
	return ls.db
}

// SlotsPerKESPeriod returns the number of slots in a KES period.
func (ls *LedgerState) SlotsPerKESPeriod() uint64 {
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

// GetTransactionsByAddress returns transactions involving the given
// address.
func (ls *LedgerState) GetTransactionsByAddress(
	addr lcommon.Address,
	limit int,
	offset int,
) ([]models.Transaction, error) {
	return ls.db.GetTransactionsByAddress(addr, limit, offset, nil)
}

// resolveValidationEra determines the appropriate era descriptor for
// validating a transaction. It returns the current era if the transaction
// matches, the previous era if compatible (era-1), or an error if the
// transaction era is not compatible with the current ledger era.
func resolveValidationEra(
	tx lcommon.Transaction,
	currentEra eras.EraDesc,
) (eras.EraDesc, error) {
	txEraId := uint(tx.Type()) // #nosec G115 -- era IDs are non-negative
	if txEraId == currentEra.Id {
		return currentEra, nil
	}
	if !eras.IsCompatibleEra(txEraId, currentEra.Id) {
		return eras.EraDesc{}, fmt.Errorf(
			"TX %s era %d is not compatible with ledger era %d",
			tx.Hash(),
			txEraId,
			currentEra.Id,
		)
	}
	txEra := eras.GetEraById(txEraId)
	if txEra == nil {
		return eras.EraDesc{}, fmt.Errorf(
			"TX %s era %d not found in era registry",
			tx.Hash(),
			txEraId,
		)
	}
	return *txEra, nil
}

// ValidateTx runs ledger validation on the provided transaction.
// It accepts transactions from the current era and the immediately
// previous era (era-1), as Cardano allows during the overlap
// period after a hard fork.
func (ls *LedgerState) ValidateTx(
	tx lcommon.Transaction,
) error {
	// Snapshot mutable state under read lock to avoid data races
	// with concurrent writers (e.g., ledgerProcessBlocks)
	ls.RLock()
	snapshotEra := ls.currentEra
	snapshotSlot := ls.currentTip.Point.Slot
	snapshotPParams := ls.currentPParams
	snapshotPrevEraPParams := ls.prevEraPParams
	ls.RUnlock()

	validationEra, err := resolveValidationEra(tx, snapshotEra)
	if err != nil {
		return err
	}
	if validationEra.ValidateTxFunc != nil {
		// Use the previous era's protocol parameters when validating
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
			return validationEra.ValidateTxFunc(
				tx,
				snapshotSlot,
				lv,
				pp,
			)
		})
		if err != nil {
			return fmt.Errorf("TX %s failed validation: %w", tx.Hash(), err)
		}
	}
	return nil
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

	validationEra, err := resolveValidationEra(tx, snapshotEra)
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

// SetForgedBlockChecker sets the forged block checker used for slot
// battle detection. This is typically called after the block forger
// is initialized, since the forger is created after the ledger state.
func (ls *LedgerState) SetForgedBlockChecker(checker ForgedBlockChecker) {
	ls.Lock()
	defer ls.Unlock()
	ls.config.ForgedBlockChecker = checker
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

	var mempoolTxs []mempool.MempoolTransaction
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
		ProtoVersion: babbage.BabbageProtoVersion{
			Major: 8,
			Minor: 0,
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

	// Calculate forging latency
	forgingLatency := time.Since(forgeStartTime)

	// Update forging metrics
	ls.metrics.blocksForgedTotal.Inc()
	ls.metrics.blockForgingLatency.Observe(forgingLatency.Seconds())

	// Publish BlockForgedEvent for observability and monitoring
	if ls.config.EventBus != nil {
		ls.config.EventBus.Publish(
			event.BlockForgedEventType,
			event.NewEvent(
				event.BlockForgedEventType,
				event.BlockForgedEvent{
					Slot:        ledgerBlock.SlotNumber(),
					BlockNumber: ledgerBlock.BlockNumber(),
					BlockHash:   ledgerBlock.Hash().Bytes(),
					TxCount:     uint(len(transactionBodies)),
					BlockSize:   uint(len(blockCbor)),
					Timestamp:   time.Now(),
				},
			),
		)
	}

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
