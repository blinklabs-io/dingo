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
	"math"
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
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/consensus"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/pipeline"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
)

// cleanupConsumedUtxosInterval is the period between consumed-UTxO cleanup
// runs. A var, like the Close* drain timeouts below, so lifecycle tests can
// shrink it instead of waiting on a real multi-minute timer.
var cleanupConsumedUtxosInterval = 5 * time.Minute

const (
	// Keep each cleanup transaction short enough that it cannot monopolize
	// SQLite while blockfetch handlers persist sync state during shutdown or
	// catch-up. Later timer or epoch-boundary runs continue the cleanup.
	cleanupConsumedUtxoBatchSize = 1_000
	batchSize                    = 50 // Number of blocks to process in a single DB transaction
	ledgerIntersectDenseCount    = 32
	ledgerAncestorSearchWindow   = 10_000
	firstBlockIndex              = 1
	mithrilLedgerSlotSyncKey     = "mithril_ledger_slot"
	mithrilLedgerHashSyncKey     = "mithril_ledger_hash"
	// blockPipelineDecodeWorkers is the fixed decode worker count for phase 1
	// of the block-processing pipeline (issue #1894). Validation workers stay
	// at 0 (disabled) unless LedgerStateConfig.BlockPipelineValidateEnabled
	// turns them on (phase 3).
	blockPipelineDecodeWorkers = 2
	// blockPipelineValidateWorkers is the fixed VRF/KES validate worker count
	// for phase 3 of the block-processing pipeline (issue #1894). VRF and KES
	// verification are each substantially more expensive than a CBOR decode,
	// so validation is the pipeline's throughput bottleneck; this is kept
	// equal to blockPipelineDecodeWorkers for now rather than scaled
	// differently, matching phase 1's fixed-worker-count approach until
	// there's a throughput profile to size it against.
	blockPipelineValidateWorkers = 2
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
	db        *database.Database
	taskQueue chan DatabaseOperation
	workerWg  sync.WaitGroup // worker goroutine lifecycle
	closed    atomic.Bool    // thread-safe without mutex in hot path
	mu        sync.Mutex
	opCount   int           // accepted operations until result is delivered, guarded by mu
	drained   chan struct{} // closed exactly once, when closed is true and opCount reaches 0
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
		drained:   make(chan struct{}),
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
	defer p.operationDone()

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
func (p *DatabaseWorkerPool) sendResult(
	op DatabaseOperation,
	result DatabaseResult,
) {
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
			DatabaseResult{
				Error: errors.New("database worker pool is shut down"),
			},
		)
		return
	}

	p.opCount++
	select {
	case p.taskQueue <- op:
		p.mu.Unlock()
		return
	default:
		p.opCount--
	}
	p.mu.Unlock()
	p.sendResult(
		op,
		DatabaseResult{Error: errors.New("database worker pool queue full")},
	)
}

// operationDone records that an accepted operation has finished and, if the
// pool is closed and this was the last outstanding one, closes drained so a
// blocked Shutdown call's select wakes immediately. Guarding opCount and the
// closed check with the same mutex Shutdown uses to flip closed makes the
// zero-crossing observed here race-free against a concurrent Shutdown call,
// so drained is closed exactly once.
func (p *DatabaseWorkerPool) operationDone() {
	p.mu.Lock()
	p.opCount--
	drained := p.closed.Load() && p.opCount == 0
	p.mu.Unlock()
	if drained {
		close(p.drained)
	}
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
	err := ls.submitDBTxnOperation(opFunc, readWrite)
	return ls.handleDBTxnResult(err)
}

func (ls *LedgerState) submitDBTxnOperation(
	opFunc func(txn *database.Txn) error,
	readWrite bool,
) error {
	return ls.SubmitAsyncDBOperation(func(db *database.Database) error {
		txn := db.Transaction(readWrite)
		return txn.Do(opFunc)
	})
}

func (ls *LedgerState) handleDBTxnResult(err error) error {
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

// blockApplyCandidatePoint returns the last block that the block-apply
// callback will examine. A block at the next epoch boundary is examined and
// cached, but the suffix after it is not touched until the next loop.
func blockApplyCandidatePoint(
	blocks []ledger.Block,
	epoch models.Epoch,
) ocommon.Point {
	candidate := blocks[0]
	epochEnd := epoch.StartSlot + uint64(epoch.LengthInSlots)
	for _, block := range blocks {
		candidate = block
		if epoch.SlotLength == 0 || block.SlotNumber() >= epochEnd {
			break
		}
	}
	return ocommon.NewPoint(
		candidate.SlotNumber(),
		candidate.Hash().Bytes(),
	)
}

// submitBlockApplyDBTxn serializes a block-apply commit and its after-commit
// transaction events against every primary-chain rollback that emits undo
// events. The expected ledger tip and last block the batch will examine are
// captured before waiting for the serializer. If a rollback wins the race,
// either the ledger tip changes or the examined block disappears from the
// primary chain, and the stale batch is rejected rather than published after
// the undo.
func (ls *LedgerState) submitBlockApplyDBTxn(
	expectedTip ochainsync.Tip,
	candidateTip ocommon.Point,
	opFunc func(txn *database.Txn) error,
) error {
	err := func() error {
		ls.transactionEventMutex.Lock()
		defer ls.transactionEventMutex.Unlock()

		ls.RLock()
		currentTip := ls.currentTip
		ls.RUnlock()
		if currentTip.BlockNumber != expectedTip.BlockNumber ||
			!pointMatches(currentTip.Point, expectedTip.Point) {
			return errStaleChainIterator
		}
		if _, err := database.BlockByPoint(ls.db, candidateTip); err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				return errStaleChainIterator
			}
			return fmt.Errorf("check block-apply candidate tip: %w", err)
		}
		return ls.submitDBTxnOperation(opFunc, true)
	}()
	// Partial-commit recovery can itself rewind the primary chain, so it must
	// run after releasing transactionEventMutex rather than recursively trying
	// to acquire it from rollbackPrimaryChainInSecurityParamWindows.
	return ls.handleDBTxnResult(err)
}

// SubmitAsyncDBReadTxn submits a read-only database transaction operation for execution on the worker pool.
// This method blocks waiting for the result and must be called after Start() and before Close().
func (ls *LedgerState) SubmitAsyncDBReadTxn(
	opFunc func(txn *database.Txn) error,
) error {
	return ls.SubmitAsyncDBTxn(opFunc, false)
}

// Shutdown stops accepting new operations, then waits for every already
// accepted one to finish and the worker goroutines to exit.
//
// The drain wait is bounded by drainTimeout -- callers pass
// CloseDBWorkerPoolShutdownTimeout, the same budget LedgerState.Close's own
// outer wait around the goroutine that calls Shutdown already uses. Close's
// outer wait keeps Close itself from blocking past that budget regardless of
// what Shutdown does, but a bound is still needed here: without one, a
// caller of Shutdown that gives up (like that outer wait) leaves nothing
// waiting on the drain at all, so a still-running worker's operation (and
// the resources it holds, e.g. this pool's db) would never be observed
// finishing -- and the fix could recur for any future operation slower than
// expected, not just the O(n^2) query bug this once surfaced as (see the
// account-lookup fix this guards against regressing).
//
// The wait itself selects the drained channel directly rather than spawning
// a goroutine to block on a sync.WaitGroup: WaitGroup.Wait cannot be
// selected against a timeout, so a wrapper goroutine bridging it to a
// channel would still block for the slow operation's full remaining
// duration after Shutdown times out and returns -- trading the caller's
// leak for an internal one instead of removing it. drained is closed by
// whichever of Shutdown or operationDone observes the closed-and-drained
// transition first, so no goroutine is ever spawned here.
//
// drainTimeout is a parameter rather than a direct read of
// CloseDBWorkerPoolShutdownTimeout so a test that mutates that var for
// isolation (see state_test.go) cannot race this call: Close evaluates the
// argument once, synchronously, before calling Shutdown.
func (p *DatabaseWorkerPool) Shutdown(drainTimeout time.Duration) error {
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		return nil
	}
	p.closed.Store(true)
	close(p.taskQueue)
	alreadyDrained := p.opCount == 0
	p.mu.Unlock()
	if alreadyDrained {
		close(p.drained)
	}

	select {
	case <-p.drained:
	case <-time.After(drainTimeout):
		return fmt.Errorf(
			"database worker pool: operation(s) still running after %s",
			drainTimeout,
		)
	}
	p.workerWg.Wait()
	return nil
}

type ChainsyncState string

const (
	InitChainsyncState     ChainsyncState = "init"
	RollbackChainsyncState ChainsyncState = "rollback"
	SyncingChainsyncState  ChainsyncState = "syncing"
)

func historicalBlockValidationDecision(
	validationEnabled bool,
	trustedReplay bool,
	chainsyncState ChainsyncState,
	blockSlot uint64,
	cutoffSlot uint64,
	mithrilLedgerSlot uint64,
) (shouldValidate bool, reachedTipRegion bool) {
	if validationEnabled {
		coveredByMithril := mithrilLedgerSlot > 0 &&
			blockSlot <= mithrilLedgerSlot
		return !coveredByMithril,
			chainsyncState == SyncingChainsyncState && blockSlot >= cutoffSlot
	}
	if trustedReplay ||
		chainsyncState != SyncingChainsyncState ||
		blockSlot < cutoffSlot {
		return false, false
	}
	if mithrilLedgerSlot > 0 && blockSlot <= mithrilLedgerSlot {
		return false, true
	}
	return true, true
}

// FatalErrorFunc is a callback invoked when a fatal error occurs that requires
// the node to shut down. The callback should trigger graceful shutdown.
type FatalErrorFunc func(err error)

// GetActiveConnectionFunc is a callback to retrieve the currently active
// chainsync connection ID for chain selection purposes.
type GetActiveConnectionFunc func() *ouroboros.ConnectionId

// GetPeerObservedTipFunc returns the delivered frontier tracked for a peer.
// The boolean is false when the connection is no longer tracked.
type GetPeerObservedTipFunc func(
	ouroboros.ConnectionId,
) (ochainsync.Tip, bool)

// GetPeerSyncTargetFunc returns a corroborated remote sync target.
type GetPeerSyncTargetFunc func(
	ouroboros.ConnectionId,
) (ochainsync.Tip, bool)

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

// GenesisSelectionStateFunc returns whether authoritative fork resolution
// should use Ouroboros Genesis density and the active window in slots.
type GenesisSelectionStateFunc func() (active bool, window uint64)

type LedgerStateConfig struct {
	PromRegistry      prometheus.Registerer
	Logger            *slog.Logger
	Database          *database.Database
	ChainManager      *chain.ChainManager
	EventBus          *event.EventBus
	CardanoNodeConfig *cardano.CardanoNodeConfig
	// Network is the CLI/YAML/env network selector dingo was started with
	// (e.g. "mainnet", "preprod", "prime-mainnet"). Shelley genesis alone
	// cannot distinguish real Cardano mainnet from a foreign chain that
	// reuses its identity for wire compatibility -- see isMainnet in
	// header_protocol_version.go. Empty when dingo was configured with a
	// raw NetworkMagic instead of a named network.
	Network                     string
	BlockfetchRequestRangeFunc  BlockfetchRequestRangeFunc
	PeersWithBlockFunc          PeersWithBlockFunc
	RecordBlockfetchLatencyFunc RecordBlockfetchLatencyFunc
	BlockfetchLatencyFunc       BlockfetchLatencyFunc
	BlockfetchLatencyMedianFunc BlockfetchLatencyMedianFunc
	GetActiveConnectionFunc     GetActiveConnectionFunc
	GetPeerObservedTipFunc      GetPeerObservedTipFunc
	GetPeerSyncTargetFunc       GetPeerSyncTargetFunc
	ConnectionLiveFunc          ConnectionLiveFunc
	ConnectionSwitchFunc        ConnectionSwitchFunc
	ClearSeenHeadersFromFunc    ClearSeenHeadersFromFunc
	PeerHeaderLookupFunc        PeerHeaderLookupFunc
	GenesisSelectionStateFunc   GenesisSelectionStateFunc
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
	// Shelley slot length. Zero disables best-effort announcement waiting, but
	// does not permit a Musashi certifying ranking block to commit without its
	// certified closure.
	EndorserBlockWaitSlots uint64
	// LeiosApplyEndorserBlockTxs selects the endorser-block ledger path. When
	// true (the CIP-conformant path, dingo's forward behavior for real Leios),
	// a referenced endorser block's transactions are applied to the UTxO set.
	// When false (the Haskell-conformant path, matching prototype-2026w29), only
	// the certified parent announcement is applied, with full effects but without
	// validation or consumed-input recovery. Set from the network in node.go
	// (false on musashi, true otherwise).
	LeiosApplyEndorserBlockTxs bool
	// SkipLeaderStakeThresholdCheck, when true, downgrades a failed Praos
	// stake-derived leader-eligibility check from a hard header rejection to a
	// logged warning (the block is trusted). It defaults to false so the check
	// is enforced everywhere unless explicitly disabled.
	//
	// dingo derives a pool's leadership stake from delegated UTxO only; it does
	// not yet compute staking rewards (CalculateRewards/GetAdaPots/
	// RewardAccountBalance are unimplemented), so reward-account balances are
	// omitted from the stake distribution. On real networks (many diffuse
	// pools) this omission is proportionally negligible and the check catches
	// genuine ineligibility, so it stays enforced. On the concentrated
	// prototype-2026w29 musashi topology the dominant pool's reward accrual
	// drifts its true relative stake above the UTxO-only figure, so enforcing
	// the threshold falsely rejects that pool's legitimately-eligible blocks and
	// wedges the chain — so it is skipped there. All other header checks (KES,
	// VRF proof, registered-VRF-key binding, opcert) still apply regardless.
	// Separately, TPraos bootstrap epochs with decentralization still active
	// validate genesis overlay assignment in verify_header.go, then skip only
	// the local pool stake-threshold check while d remains active.
	// Interim measure until reward calculation lands and reward balances can be
	// included in the leadership stake. Set from the network in node.go (true
	// on musashi, false otherwise) via Config.prototypeTrustBypassesEnabled,
	// which requires an unambiguous Musashi identity so this can never be
	// reached from a preview/preprod/mainnet configuration.
	SkipLeaderStakeThresholdCheck bool
	// SkipDijkstraTxValidation, when true, skips the Dijkstra per-transaction
	// validation rule set entirely. On the Haskell-conformant Musashi path,
	// certified closure and ranking-block transactions are trusted because the
	// prototype does not validate endorser-block transactions. Running dingo's
	// rule set only to discard any disagreement is
	// wasted work that prevents the node from reaching tip under load. Set true
	// on Musashi in node.go via Config.prototypeTrustBypassesEnabled, which
	// requires an unambiguous Musashi identity so this can never be reached
	// from a preview/preprod/mainnet configuration. Applies to Dijkstra-era
	// transactions only — see LedgerState.skipDijkstraTxValidation. Interim
	// until the Leios certificate / endorser-availability surface is complete
	// (#2587).
	SkipDijkstraTxValidation bool
	// MinPoolMargin is the CIP-23 minimum pool margin (minimum variable fee) in
	// basis points, [0, 10000] (150 = 1.5%); 0 disables it. It is a consensus-
	// affecting operator setting (not derived from the network) that takes
	// effect only in Dijkstra and later. Enable a nonzero value only on a
	// network where every node also enables the same value.
	MinPoolMargin uint
	// PledgeLeverageEnabled turns on the CIP-50 pledge-leverage reward cap. It
	// is a consensus-affecting feature gate that defaults false; enable it only
	// on a network where every node also enables it (mainnet and the public
	// testnets keep it off). Unlike the Musashi-derived toggles above it is set
	// from operator config in node.go, not derived from the network.
	PledgeLeverageEnabled bool
	// PledgeLeverage is L, the CIP-50 maximum ratio of total stake to pledge,
	// in the range [1, 10000]. It is used only when PledgeLeverageEnabled is
	// true.
	PledgeLeverage uint
	// FullPotRewardsEnabled turns on CIP-0163 full-pot reward distribution: the
	// entire epoch reward pot is apportioned across pools that earned a base
	// reward instead of returning the saturation/pledge/performance residual to
	// reserves. It is a consensus-affecting feature gate that defaults false;
	// enable it only on a network where every node also enables it (mainnet and
	// the public testnets keep it off). Like the CIP-50 pledge-leverage gate it
	// is set from operator config in node.go, not derived from the network.
	FullPotRewardsEnabled bool
	// DelegatorInactivityEnabled turns on CIP-0163 reward-account inactivity
	// expiry. Consensus-affecting; defaults false. Set from operator config in
	// node.go (serve mode) and internal/node/load.go (load/replay mode), not
	// derived from the network. Must match across the network.
	DelegatorInactivityEnabled bool
	// DelegatorInactivity is the inactivity window in epochs, used only when
	// DelegatorInactivityEnabled is true.
	DelegatorInactivity      uint64
	ValidateHistorical       bool
	EnableDijkstra           bool
	StartInDijkstra          bool
	TrustedReplay            bool
	ManualBlockProcessing    bool
	ForgeBlocks              bool
	DatabaseWorkerPoolConfig DatabaseWorkerPoolConfig
	// BlockPipelineEnabled turns on parallel block decode in the chainsync
	// replay loop (ledgerReadChainIterator): blocks read back from the
	// primary chain are decoded by a small worker pool (gouroboros'
	// pipeline package) instead of one at a time inline, then re-sequenced
	// before being handed to ledgerProcessBlocksFromSource exactly as
	// today. Validation and apply are untouched -- this only changes how
	// CBOR decode work is scheduled. Off by default: throughput and
	// stability are still being proven (issue #1894 phase 1). See
	// ARCHITECTURE.md ("Block Processing Pipeline").
	//
	// A rollback drains blockPipeline's in-flight decode/validate backlog
	// (drainBlockPipelineBeforeRollback, issue #1894 phase 5) before it
	// proceeds, to shrink -- not eliminate -- the window in which an
	// already-in-flight batch from an abandoned fork could otherwise be
	// applied after the rollback. See ARCHITECTURE.md ("Phase 5: rollback
	// coordination").
	BlockPipelineEnabled bool
	// BlockPipelineValidateEnabled adds parallel VRF/KES validation to the
	// decode pipeline (issue #1894 phase 3). Dingo supplements the generic
	// stage with the OpCert cold-key signature and MaxKESEvolutions checks,
	// and enforces results only where the serial path has validation state:
	// not trusted historical/Mithril replay and only with a cached epoch
	// nonce. A rejection is returned as headerValidationError so the already-
	// persisted chain can be rewound rather than retried forever.
	//
	// This is defense in depth, not a replacement for admission validation.
	// Headers and blocks remain fully checked before entering ls.chain because
	// that chain is served to downstream clients before ledger apply. See
	// ARCHITECTURE.md ("Block Processing Pipeline").
	//
	// This flag's extra CPU cost (two dedicated VRF/KES workers) can make
	// block-apply throughput fall behind header arrival during bursty
	// near-tip conditions more easily than decode-only phase 1 or the
	// pre-pipeline baseline hit in practice; if that happens for long
	// enough, the chain's queued-header backlog can reach capacity while a
	// fork is being resolved. tryResolveFork's failure handling used to
	// silently strand that backlog in exactly that case, stalling sync
	// with no error logged above WARN until chainsyncrecycler's
	// local-tip-plateau watchdog eventually forced a resync (~20 minutes
	// with default config) -- this was a general, pre-existing gap in
	// tryResolveFork, not specific to this flag (reproduced live under
	// this flag, under decode-only, and under the pre-pipeline baseline
	// alike); the flag's throughput cost just makes it easier to reach.
	// See ensureBlockfetchDrainingAfterForkQueueFailure and
	// ARCHITECTURE.md ("Fork-resolution header-queue overflow must still
	// restart blockfetch") for the fix and the full explanation.
	BlockPipelineValidateEnabled bool
}

// EndorserBlockProviderFunc returns the complete set of standalone
// transaction CBORs of the Leios endorser block identified by (ebHash,
// ebSlot), when exactly that occurrence has been fetched and fully cached;
// ok is false otherwise. ebSlot is required, not merely advisory: the
// manifest is content-addressed, so the same hash can be a live,
// independently required occurrence at more than one slot at once, and the
// provider must resolve exactly the occurrence the caller's own reference
// names rather than whichever one happens to be cached for the hash (issue
// #3513 review). It is used to apply an endorser block's transactions to the
// ledger when the referencing Dijkstra ranking block is processed.
type EndorserBlockProviderFunc func(
	ebHash []byte,
	ebSlot uint64,
) (txs []cbor.RawMessage, ok bool)

// EndorserBlockFetcherFunc actively fetches the endorser block identified by
// (ebSlot, ebHash) over leios-fetch (manifest plus all transaction bodies) and
// caches it so a subsequent EndorserBlockProviderFunc call returns it. It
// returns an error when no fetch connection is available or the relay does not
// serve the block. The endorser block shares the slot of the ranking block that
// references it (they are co-produced), so ebSlot is the ranking block's slot.
//
// ctx bounds the whole fetch, including its per-connection failover. The caller
// owns the budget: block application waits for this fetch, so an implementation
// must not outlive the context it was handed (dingo #3552).
type EndorserBlockFetcherFunc func(
	ctx context.Context,
	ebSlot uint64,
	ebHash []byte,
) error

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

// consensusSnapshot contains ledger state that is read frequently and updated
// as one logical unit at epoch/era boundaries and during rollback. Published
// snapshot containers and their owned slices are immutable. Protocol parameter
// values are shared with writer state and must be treated as read-only.
type consensusSnapshot struct {
	generation     uint64
	currentEpoch   models.Epoch
	currentEra     eras.EraDesc
	currentPParams lcommon.ProtocolParameters
	prevEraPParams lcommon.ProtocolParameters
	epochCache     []models.Epoch
	transitionInfo hardfork.TransitionInfo
	// syntheticV2CostModelInEffect mirrors LedgerState.syntheticV2CostModel;
	// see that field's doc comment.
	syntheticV2CostModelInEffect bool
}

// tipSnapshot contains the applied tip and the Praos block nonce belonging to
// that tip. Published snapshots are immutable.
type tipSnapshot struct {
	generation           uint64
	currentTip           ochainsync.Tip
	currentTipBlockNonce []byte
}

type LedgerState struct {
	metrics   stateMetrics
	consensus atomic.Pointer[consensusSnapshot]
	tip       atomic.Pointer[tipSnapshot]
	// timeConverter owns slot/wall-clock time conversion (SlotToTime,
	// TimeToSlot, SlotToEpoch, EpochInfo) and the operational near-now
	// fallbacks used while the applied ledger is behind the wall clock.
	// NewLedgerState builds it eagerly; timeConv() (see slot.go) only lazily
	// builds it for bare-constructed LedgerStates that skip NewLedgerState
	// (test-only), guarded by timeConverterOnce.
	timeConverter     *SlotTimeConverter
	timeConverterOnce sync.Once
	// preByronPrefixWarned records that warnOnPreByronPrefixEpochCache has
	// already reported the stale shape. loadEpochs runs twice on startup --
	// once from PrepareEpochCacheForStartup and again from Start -- and both
	// take the populated-cache branch on a database that already has epochs,
	// so without this the operator sees the diagnosis duplicated.
	preByronPrefixWarned bool
	// snapshotGeneration is incremented while writers are serialized by Lock.
	// It lets readers that need both snapshots reject adjacent publications.
	snapshotGeneration uint64
	// The fields below are writer-owned working state. Lock-free readers use
	// consensus and tip snapshots; writers update these fields under Lock and
	// publish a fresh immutable snapshot before unlocking.
	currentEra                         eras.EraDesc
	activeEras                         []eras.EraDesc
	config                             LedgerStateConfig
	chainsyncBlockfetchTimeoutTimer    *time.Timer // timeout timer for blockfetch operations
	chainsyncBlockfetchTimerGeneration uint64      // generation counter to detect stale timer callbacks
	currentPParams                     lcommon.ProtocolParameters
	prevEraPParams                     lcommon.ProtocolParameters // pparams from the immediately previous era (for era-1 TX validation)
	// syntheticV2CostModel is true from the moment HardForkBabbage fabricates
	// a PlutusV2 cost model (real mainnet/preview/preprod never had one in
	// genesis -- PlutusV2 postdates the Alonzo genesis format entirely, so
	// this always fires on the live hard fork) until a real governance
	// enactment sets one via processEpochRollover. It is never reset back to
	// true once cleared: once real data has been seen for this key, later
	// eras carrying the same map forward must not be reinterpreted as
	// synthetic again. See queryShelleyCurrentProtocolParams
	// (blinklabs-io/dingo#3825) for why this exists: internal script
	// validation must keep using the real default regardless (a genuine
	// PlutusV2 script can arrive before the real update lands), but a
	// LocalStateQuery caller asking "what are the current protocol
	// parameters" should see only what the chain has actually committed to,
	// matching what a real cardano-node reports during the same window.
	syntheticV2CostModel        bool
	transitionInfo              hardfork.TransitionInfo // upcoming era boundary state (mirrors Haskell HFC TransitionInfo)
	hfiEvalDoneEpoch            uint64                  // currentEpoch.EpochId for which the HFI tally has been kicked off (held under ls.RWMutex)
	hfiEvalGeneration           atomic.Uint64           // bumped on rollback to invalidate any in-flight HFI tally
	hfiStabilityEvalInFlight    atomic.Bool             // guard against overlapping async HFI tallies
	rewardInputGeneration       atomic.Uint64           // bracketed around rollback to invalidate in-flight reward calculations
	rewardInputRollbackActive   atomic.Int64            // non-zero while rollback can mutate reward calculation inputs
	mempool                     MempoolProvider
	timerCleanupConsumedUtxos   *time.Timer
	cleanupConsumedUtxosRunning atomic.Bool
	Scheduler                   *Scheduler
	chain                       *chain.Chain
	db                          *database.Database
	chainsyncState              ChainsyncState
	currentTipBlockNonce        []byte
	epochCache                  []models.Epoch
	epochNonceHexCache          map[uint64]epochNonceHexCacheEntry
	checkpoints                 map[uint64]string // configured chain checkpoints keyed by block number (height)
	slotsPerKESPeriod           atomic.Uint64
	forgedBlockChecker          atomic.Pointer[forgedBlockCheckerHolder]
	slotBattleRecorder          atomic.Pointer[slotBattleRecorderHolder]
	cachedShape                 atomic.Pointer[hardfork.Shape]                  // lazy-built from CardanoNodeConfig; immutable for the LedgerState's lifetime
	epochSnapshotHook           atomic.Pointer[epochBoundarySnapshotHookHolder] // optional authoritative epoch-boundary snapshot capture (nil = event-driven fallback only)
	epochSnapshotStakeHook      atomic.Pointer[epochBoundarySnapshotHookHolder] // optional SNAP-point stake read for the authoritative capture (nil = read at persist time)
	reachedTip                  atomic.Bool
	currentTip                  ochainsync.Tip
	byronPBFT                   byronPBFTCache
	currentEpoch                models.Epoch
	dbWorkerPool                *DatabaseWorkerPool
	slotClock                   *SlotClock
	slotTickChan                <-chan SlotTick
	ctx                         context.Context
	// cleanupMu owns timerCleanupConsumedUtxos and serializes cleanup-run
	// registration against Close. Deliberately not the LedgerState RWMutex:
	// Close waits on cleanupWG while an in-flight run still needs RLock to
	// read the tip, so draining under the ledger lock would deadlock.
	cleanupMu sync.Mutex
	// cleanupWG counts consumed-UTxO cleanup runs that have passed the
	// closed check, so Close can join one already touching the database.
	cleanupWG sync.WaitGroup
	// leiosBackfill prefetches historical Leios endorser blocks by point ahead
	// of the apply cursor (nil when no endorser-block fetcher is configured).
	leiosBackfill *leiosBackfiller
	sync.RWMutex
	chainsyncMutex                sync.Mutex
	chainsyncBlockfetchMutex      sync.Mutex
	chainsyncBlockfetchReadyMutex sync.Mutex
	// bufferedHeaderMutex guards bufferedHeaderEvents alone. That map is
	// reached from both the chainsync dispatch goroutine (which holds
	// chainsyncMutex) and the blockfetch path (which holds
	// chainsyncBlockfetchMutex), so neither of those locks covers every
	// access and a concurrent iteration and write is fatal at runtime.
	// Where both are held, acquire this one last; nothing calls out to
	// other ledger code while holding it, so the order stays fixed.
	bufferedHeaderMutex          sync.Mutex
	chainsyncBlockfetchReadyChan chan struct{}
	activeBlockfetchConnId       ouroboros.ConnectionId // connection used for current blockfetch pipeline
	shadowBlockfetchConnId       ouroboros.ConnectionId // shadow peer dispatched for parallel blockfetch
	// blockfetchPrimaryRequestGeneration identifies the currently running
	// primary request. A timeout may fire while the request is blocked in the
	// protocol client; keeping its generation lets the timeout wait instead of
	// issuing a duplicate request on the same batch.
	blockfetchRequestGeneration         uint64
	blockfetchPrimaryRequestGeneration  uint64
	blockfetchRequestsInFlight          map[string]chan struct{}
	blockfetchShadowRequestsInFlight    map[string]struct{}
	blockfetchInFlightTimeoutGeneration uint64
	blockfetchInFlightTimeoutCount      uint8
	selectedBlockfetchConnId            ouroboros.ConnectionId // latest selected chainsync connection for the next batch
	// blockfetchContinuationPending prevents a chainsync handler from starting
	// a competing batch in the short interval after the blockfetch subscriber
	// schedules its next request on a worker. The worker must run outside the
	// subscriber goroutine because GetBlockRange waits for BatchDone, which is
	// delivered back through that same subscriber.
	blockfetchContinuationPending bool
	blockfetchContinuationMu      sync.Mutex
	blockfetchContinuationWG      sync.WaitGroup
	headerPipelineConnId          ouroboros.ConnectionId // connection that currently owns the queued header/blockfetch pipeline
	pendingBlockfetchEvents       []BlockfetchEvent
	activeBlockfetchStart         time.Time           // when RequestRange was issued (for latency measurement)
	firstBlockReceived            bool                // true after latency sample recorded for this batch
	shadowBlockReceivedHashes     map[string]struct{} // blocks delivered this batch (dedup shadow vs primary)
	batchBlocksReceived           int                 // total blocks received in current blockfetch batch (including mid-batch flushes)
	// Failures to obtain one specific queued header range, keyed by its
	// start point and counting both a NoBlocks reply (a synchronous
	// GetBlockRange error) and a batch that completed without delivering a
	// block. Bounded by blockfetchMaxSameRangeFailures so an unfetchable
	// queued range cannot be retried indefinitely (which also latches the
	// header that blocks local forging). Deliberately survives interleaved
	// deliveries for other ranges and header-queue churn; discarded when
	// the tracked range itself is delivered.
	blockfetchRangeFailure blockfetchRangeFailureState
	// deferredHeaderValidation holds block points whose stateful header checks
	// wait for ledger apply. It is guarded by its own deferredHeaderValidationMu
	// (NOT the main RWMutex) so the snapshot retention guard
	// (PrunePoolSnapshotsWithRetentionFloor) can hold the set stable across the
	// eviction and floor computation without contending the hot header-validation
	// read path on the main lock (issue #3727). The guard must NOT hold this mutex
	// across the pool-snapshot prune (nor deletePersistedDeferredMarkers across
	// DeleteSyncState): those open the single SQLite write connection, and block
	// apply holds that connection before taking this mutex via
	// consumeDeferredHeaderValidation, so holding it across that write inverts the
	// lock order and deadlocks the node (issue #3717). The eviction+floor read is
	// atomic; a header admitted after the lock is released is handled by the next
	// cleanup pass (the floor is a lower-watermark recomputed each pass).
	deferredHeaderValidation   map[string]struct{}
	deferredHeaderValidationMu sync.Mutex
	checkpointWrittenForEpoch  bool
	closed                     atomic.Bool
	closeMu                    sync.Mutex
	closeDone                  chan struct{}
	closeErr                   error
	inRecovery                 bool // guards against recursive recovery in SubmitAsyncDBTxn
	lastAtTipRecovery          *atTipRecoveryAttempt
	// At-tip recovery non-convergence tracking (issue #2939). A descending
	// series of *distinct* (block, tx) validation failures each resets the
	// same-block escalation to attempt 1, so the escalate-and-cap logic in
	// lastAtTipRecovery never engages and the primary chain would be rewound
	// ever deeper, falling unboundedly behind the wall clock. These fields
	// detect that pattern and switch recovery to a shallow, hold-at-tip mode.
	// All three are only read/written from the ledger pipeline goroutine.
	atTipRecoveryLastFailSlot uint64 // failing slot of the previous distinct at-tip failure
	atTipRecoveryDescentCount int    // consecutive distinct failures that did not advance
	atTipRecoveryHolding      bool   // sticky: deep rewinds suppressed until forward progress
	// Replay recovery non-convergence tracking (issue #3005). The
	// unresolved-producer fallback can encounter different, slowly advancing
	// failing blocks while repeatedly rebuilding to the same applied tip.
	// Track that applied high-water mark so changing failure identities do not
	// hide the lack of ledger progress. These fields are only accessed by the
	// ledger pipeline goroutine.
	replayRecoveryTipTracked      bool
	replayRecoveryHighWaterSlot   uint64
	replayRecoveryNoProgressCount int
	replayRecoveryHolding         bool
	// Records the one fresh-intersection request already spent on a
	// deterministic transaction rejection, keyed on the failing block and
	// the applied tip. Chain selection gets one alternate-branch
	// opportunity per failing block; after that the branch is still
	// rejected, but peers are no longer rotated for it. See
	// deterministicTxRecoveryLatch in ledger/replay_recovery.go.
	deterministicTxRecoveryResync *deterministicTxRecoveryLatch
	// Consecutive successful recovery attempts refused at the Mithril trust
	// boundary without advancing the applied tip (issues #3261 and #3301).
	// The refusal's only escape is peer rotation, which cannot help for a
	// canonical block, so the applied high-water tally turns an unbounded
	// reject-and-retry loop into a terminal condition even when replay reports
	// changing failing block or transaction identities.
	mithrilBoundaryRecovery *mithrilBoundaryRecoveryProgress
	// Consecutive recovery rewinds the chain refused for exceeding the
	// security parameter without the applied tip advancing (issue #3889).
	// The refusal means recovery has no legal rewind target at all, so a
	// pipeline restart re-derives the same impossible rewind; the tally is
	// what turns that loop into a terminal condition.
	recoveryRewind *recoveryRewindProgress
	// Cross-fork continuation audit (issue #3005). Armed by a local
	// rollback and consumed by the blockfetch handler; see
	// ledger/continuation_audit.go for the cost and soundness argument.
	continuationAudit      atomic.Pointer[continuationAuditWindow]
	mithrilLedgerSlot      uint64 // blocks at or below this slot are Mithril-verified; skip validation
	mithrilLedgerHash      []byte // hash for mithrilLedgerSlot, used as a stable chainsync intersect point
	lastLocalRollbackSeq   uint64
	lastLocalRollbackPoint ocommon.Point

	// Subscription IDs for event bus unsubscribe on close
	chainsyncSubID           event.EventSubscriberId
	chainsyncAwaitReplySubID event.EventSubscriberId
	blockfetchSubID          event.EventSubscriberId
	chainUpdateSubID         event.EventSubscriberId
	chainSwitchSubID         event.EventSubscriberId
	connClosedSubID          event.EventSubscriberId
	rewardPrecomputeSubID    event.EventSubscriberId

	// processBlocksCancel stops the ledgerProcessBlocks goroutine Start
	// launches. It is deliberately its own child context, not the ctx Start
	// was called with: that ctx is n.ctx, the node's long-lived context,
	// which a live database restore/truncate never cancels (see
	// node_lifecycle.go's package doc). Without a way to signal this
	// goroutine specifically, it keeps applying incoming blocks with no
	// awareness that Close is about to run — Close waited for every other
	// background goroutine (replayWG, rewardPrecomputeWG below)
	// but not this one, the actual pipeline that writes blocks to the
	// database. A block landing mid-write exactly as Close proceeds to
	// shut down dbWorkerPool leaves the persisted block-ID index
	// permanently inconsistent with the in-memory tip that already
	// advanced for it -- surfacing later as a "persistent chain index gap"
	// error that never resolves, since the corruption is on disk, not a
	// transient condition a retry can recover from.
	processBlocksCancel context.CancelFunc
	// processBlocksWG tracks that same goroutine so Close can wait for it
	// to actually exit before proceeding, the same way replayWG/
	// rewardPrecomputeWG already do for the others.
	processBlocksWG sync.WaitGroup

	// blockPipeline, when non-nil (LedgerStateConfig.BlockPipelineEnabled
	// and not ManualBlockProcessing -- see NewLedgerState), decodes blocks
	// read back from the primary chain in parallel during
	// ledgerReadChainIterator. It is started in Start (before
	// processBlocksWG's goroutine, which is its only submitter) and stopped
	// in Close (after that goroutine has drained), so its own worker
	// goroutines never outlive the LedgerState. Nil means decode stays
	// fully serial, matching pre-pipeline behavior exactly.
	//
	// It is a single instance shared across every ledgerProcessBlocks retry
	// attempt (see errRestartLedgerPipeline), not recreated per attempt.
	// ledgerProcessBlocks's retry loop is responsible for making sure only
	// one attempt's reader goroutine ever submits to it at a time -- see the
	// loop's doc comment.
	blockPipeline *pipeline.BlockPipeline
	// blockPipelineErrorsDone is closed once drainBlockPipelineErrors (the
	// goroutine that continuously reads blockPipeline.Errors() for the
	// pipeline's full lifetime) has returned. Set alongside blockPipeline.
	// Start in Start; nil whenever blockPipeline is nil or was never
	// started. See drainBlockPipelineErrors's doc comment for why this
	// goroutine exists: gouroboros' pipeline pushes every stage error onto
	// a fixed-size errorsChan before forwarding the item onward, and never
	// drains it itself -- without a permanent reader, errorsChan fills
	// (deterministically, on real chain sync, from expected per-block
	// Byron-era validate-stage errors -- see blockPipelineEta0Provider) and
	// every validate worker permanently blocks on `errors <- err`,
	// cascading into an unrecoverable Submit deadlock.
	blockPipelineErrorsDone chan struct{}
	// blockPipelineGatherMutex serializes ledgerReadChainIterator's
	// gather-then-submit sequence (collecting raw blocks via iter.Next(),
	// then handing them to decodeReadChainBatch, which -- when blockPipeline
	// is non-nil -- Submits every one of them and drains their Results()
	// before returning) against rollbackChainAndStateDeferred's out-of-band
	// rollback path.
	//
	// drainBlockPipelineBeforeRollback alone is not enough for that path:
	// it only waits for work already Submitted to blockPipeline
	// (PendingCount) to finish. A batch the reader has already pulled off
	// the chain iterator into its local rawBatch, but not yet passed to
	// decodeReadChainBatch, holds nothing in the pipeline's queues -- so
	// WaitForDrain observes an empty pipeline and returns immediately, and
	// the reader can then Submit and apply those raw blocks, from the very
	// fork chain-selection just decided to abandon, after the rollback
	// already believed it was safe to proceed. rollbackChainAndStateDeferred
	// (reached from chainsync per-connection handling, never from
	// ledgerProcessBlocks) has no other way to learn the reader is
	// mid-gather.
	//
	// The reader holds the read lock for exactly the gather-plus-submit
	// span (see ledgerReadChainIterator); rollbackChainAndStateDeferred holds the
	// write lock from before it calls drainBlockPipelineBeforeRollback
	// through ls.chain.Rollback, so no gather-then-submit cycle can start,
	// and none already in flight can reach Submit, while a rollback is
	// physically truncating the chain. Once ls.chain.Rollback returns, the
	// chain package's own iterator bookkeeping (ChainIterator.needsRollback,
	// set under the chain's own locks in Chain.rollbackLocked) makes any
	// later iter.Next() call safe on its own, so the write lock does not
	// need to extend past it.
	//
	// This does not replace processChainIteratorRollback's stale-tip
	// backstop -- a batch the reader already hung off decodeReadChainBatch
	// before either lock existed and is now working through
	// ledgerProcessBlocksFromSource's DB-apply is a separate, already
	// documented and accepted window (see drainBlockPipelineBeforeRollback's
	// doc comment); that backstop remains what corrects it. This mutex
	// closes the narrower, earlier window between gathering raw blocks and
	// submitting them, which has no other guard at all.
	blockPipelineGatherMutex sync.RWMutex
	// transactionEventMutex serializes block-apply commits (including their
	// database AfterCommit Apply publication) against rollback validation,
	// Undo publication, and primary-chain truncation. Without it, a rollback
	// can observe a newly committed block before its AfterCommit callback has
	// reached the ordered lane, publishing Undo before Apply. See
	// submitBlockApplyDBTxn and rollbackChainAndStateDeferred.
	transactionEventMutex sync.Mutex

	// publishCtx is cancelled at the top of Close so a ledger.tx publish
	// parked on a full ordered lane cannot outlive shutdown. Only the
	// EventBus stopping otherwise releases such a publisher, and a live
	// restore/truncate closes the LedgerState while keeping the bus
	// running, so without this the Close waits below are unbounded.
	// Nil on a bare-struct LedgerState (tests), which reads as "no
	// cancellation" and matches the pre-existing behaviour.
	publishCtx    context.Context
	publishCancel context.CancelFunc
	// beforeTransactionApplyPublish is a test-only sequencing hook. Nil in
	// production; tests use it to hold the exact post-commit/pre-publication
	// window without relying on scheduler timing.
	beforeTransactionApplyPublish func()
	// beforeReadResultDoneSignal is a test-only hook called once per
	// ledgerProcessBlocksFromSource outer-loop pass, immediately before that
	// pass decides whether to signal the current readChainResult's done
	// channel (see the cachedNextBatch handling there). Nil in production;
	// tests use it to deterministically observe, at each pass boundary,
	// that done has not yet been signalled -- without racing a separate
	// goroutine against the pipeline's own progress.
	beforeReadResultDoneSignal func()

	// replayMu serializes replayWG.Add with Close's replayWG.Wait to
	// prevent Add-after-Wait panics from the TOCTOU race between
	// closed.Load() and Add(1) in replayBufferedHeadersAsync (#2107).
	replayMu sync.Mutex
	// replayWG tracks in-flight replayBufferedHeadersAsync goroutines so
	// Close can drain them before the database is closed (issue #2107).
	replayWG sync.WaitGroup
	// rewardPrecomputeMu serializes rewardPrecomputeWG.Add with Close's
	// rewardPrecomputeWG.Wait and protects the latest-event coalescing state so
	// Close cannot return while precompute is still issuing database reads/writes.
	rewardPrecomputeMu      sync.Mutex
	rewardPrecomputeWG      sync.WaitGroup
	rewardPrecomputeRunning bool
	rewardPrecomputePending *event.EpochTransitionEvent
	rewardPrecomputeRetry   *stakeRewardPrecomputeRetry
	validationEnabled       bool
	// Sync progress reporting (Fix 4)
	syncProgressLastLog  time.Time     // last time we logged sync progress
	syncProgressLastSlot uint64        // slot at last progress log (for rate calc)
	syncUpstreamTipSlot  atomic.Uint64 // latest admitted peer header slot
	syncUpstreamState    atomic.Pointer[upstreamSyncState]
	nextNonceReadyEpoch  atomic.Uint64 // last ready epoch emitted for next-epoch nonce stability

	// Rate-limiting for non-active rollback drop messages
	dropRollbackLastLog time.Time // last time we logged a drop rollback
	dropRollbackCount   int64     // count of suppressed drop rollbacks since last log

	rollbackHistory []rollbackRecord // recent rollback slot+time pairs for loop detection

	// unrecoverableRollbacks tracks rollback points a peer repeatedly asks
	// us to cross to but that we cannot apply locally (block missing below
	// our diverged tip, rollback exceeds K, or the point is below the
	// Mithril trust boundary). Unlike rollbackHistory it is keyed on the
	// rollback point alone (not the connection) and is deliberately NOT
	// cleared by the resync reset, so it survives the reset+reconnect
	// cycle. That cycle otherwise defeats the rollbackHistory-based loop
	// detector: each un-crossable rollback calls resetChainsyncResyncState
	// (which wipes rollbackHistory) and forces a fresh connection, and the
	// detector keys on connection ID, so the per-connection counter never
	// reaches its threshold across reconnects. See issue #2728.
	unrecoverableRollbacks       map[string]unrecoverableRollbackRecord
	lastUnrecoverableRollbackLog time.Time // throttles the stuck-divergence operator error

	lastActiveConnId *ouroboros.ConnectionId // tracks active connection for switch detection

	// Header mismatch tracking for fork detection and re-sync
	headerMismatchCount  int // consecutive header mismatch count
	bufferedHeaderEvents map[string][]ChainsyncEvent
	peerHeaderHistory    map[string]*peerHeaderChain
	// Test hook for fork ancestor lookups.
	lookupBlockByHash func([]byte) (models.Block, error)
	// Test hook called after Close releases the blockfetch continuation mutex
	// and before it waits for continuation workers.
	blockfetchContinuationSchedulingHook func()
	// Test hook called at the top of the consumed-UTxO cleanup timer
	// callback, before any shutdown check, so a lifecycle test can observe
	// whether the timer itself is still armed.
	cleanupConsumedUtxosTimerFiredHook func()
	// Test hook called inside a cleanup run that has registered with
	// cleanupWG, so a lifecycle test can hold a run in flight and assert
	// that Close drains it.
	cleanupConsumedUtxosRunHook func()
}

// upstreamSyncState is one connection-generation snapshot. Consumers must not
// combine an active flag from one peer with a target from another peer.
type upstreamSyncState struct {
	connectionKey string
	targetSlot    uint64
}

// EraTransitionResult holds computed state from an era transition
type EraTransitionResult struct {
	NewPParams lcommon.ProtocolParameters
	NewEra     eras.EraDesc
	// InjectedSyntheticV2CostModel is true when this specific transition is
	// the one that fabricated a PlutusV2 cost model (HardForkBabbage's
	// default), as opposed to one carried forward from a real source. See
	// LedgerState.syntheticV2CostModel.
	InjectedSyntheticV2CostModel bool
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
	// BoundarySnapshotDeferred is true when the caller asked
	// processEpochRollover to skip the authoritative mark-snapshot capture and
	// the rollover reached the point where it would otherwise have captured it.
	// The caller then owns exactly one capture, taken after the remaining
	// boundary era transitions have rewritten NewCurrentEra and
	// NewCurrentPParams. It stays false for the initial-epoch path, which never
	// captures a mark snapshot.
	BoundarySnapshotDeferred bool
	// RealV2CostModelObserved is true when this rollover's enacted governance
	// ParamUpdate explicitly carried a PlutusV2 cost model
	// (governance.EnactmentResult.PlutusV2CostModelWritten), not merely
	// whether the post-enactment pparams happen to contain one. Provenance
	// is tracked from the enacted delta itself rather than by comparing
	// before/after values, because DefaultPlutusV2CostModel is the real
	// canonical mainnet value: real governance re-affirming it verbatim
	// would be indistinguishable from "unchanged" under a value-comparison
	// approach, which would then never clear
	// LedgerState.syntheticV2CostModel on a real network. See
	// blinklabs-io/dingo#3825's PR review.
	RealV2CostModelObserved bool
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
	byronPBFT, err := newByronPBFTCache(cfg)
	if err != nil {
		return nil, err
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
		epochNonceHexCache: make(map[uint64]epochNonceHexCacheEntry),
		validationEnabled:  cfg.ValidateHistorical,
		byronPBFT:          byronPBFT,
	}
	ls.publishCtx, ls.publishCancel = context.WithCancel(context.Background())
	ls.timeConverter = ls.newTimeConverter()
	ls.publishSnapshotsLocked()
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
	if cfg.BlockPipelineEnabled && cfg.BlockPipelineValidateEnabled &&
		!cfg.ManualBlockProcessing && ls.SlotsPerKESPeriod() == 0 {
		ls.publishCancel()
		return nil, errors.New(
			"block pipeline validation requires slotsPerKESPeriod from Shelley genesis",
		)
	}
	ls.storeForgedBlockChecker(cfg.ForgedBlockChecker)
	ls.storeSlotBattleRecorder(cfg.SlotBattleRecorder)
	ls.leiosBackfill = newLeiosBackfiller(cfg)
	// ManualBlockProcessing feeds already-decoded batches directly into
	// ledgerProcessBlocksFromSource via ProcessTrustedBlockBatches,
	// bypassing ledgerReadChainIterator (the pipeline's only submitter)
	// altogether, so no pipeline is constructed for that mode -- leaving
	// blockPipeline non-nil but permanently unstarted otherwise.
	if cfg.BlockPipelineEnabled && !cfg.ManualBlockProcessing {
		// ApplyFunc is left nil in every case -- actual ledger apply
		// continues to happen downstream in ledgerProcessBlocksFromSource
		// exactly as it does today; the pipeline's apply stage here only
		// re-sequences decoded (and, if enabled, validated) results back
		// into submission order.
		pipelineOpts := []pipeline.PipelineOption{
			pipeline.WithDecodeWorkers(blockPipelineDecodeWorkers),
		}
		if cfg.BlockPipelineValidateEnabled {
			// Wire VRF/KES validation (phase 3). Eta0Provider reads the
			// published epoch cache without forecasting or mutation; a
			// missing nonce is handled by the serial-equivalence gate in
			// decodeReadChainBatch.
			//
			// VerifyConfig scopes gouroboros' generic stage to VRF/KES.
			// decodeReadChainBatch supplements a successful result with
			// Dingo's OpCert signature/expiry checks. Body/transaction and
			// registered-pool state validation remain in their existing
			// ledger paths.
			pipelineOpts = append(
				pipelineOpts,
				pipeline.WithValidateWorkers(blockPipelineValidateWorkers),
				pipeline.WithEta0Provider(ls.blockPipelineEta0Provider),
				pipeline.WithSlotsPerKesPeriod(ls.SlotsPerKESPeriod()),
				pipeline.WithVerifyConfig(lcommon.VerifyConfig{
					SkipBodyHashValidation:    true,
					SkipTransactionValidation: true,
					SkipStakePoolValidation:   true,
				}),
			)
		}
		ls.blockPipeline = pipeline.NewBlockPipeline(pipelineOpts...)
	}
	return ls, nil
}

func cloneSnapshotBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}

func cloneTip(value ochainsync.Tip) ochainsync.Tip {
	value.Point.Hash = cloneSnapshotBytes(value.Point.Hash)
	return value
}

func cloneEpoch(value models.Epoch) models.Epoch {
	value.Nonce = cloneSnapshotBytes(value.Nonce)
	value.EvolvingNonce = cloneSnapshotBytes(value.EvolvingNonce)
	value.CandidateNonce = cloneSnapshotBytes(value.CandidateNonce)
	value.LastEpochBlockNonce = cloneSnapshotBytes(value.LastEpochBlockNonce)
	return value
}

func cloneEpochs(values []models.Epoch) []models.Epoch {
	ret := make([]models.Epoch, len(values))
	for i := range values {
		ret[i] = cloneEpoch(values[i])
	}
	return ret
}

// publishSnapshotsLocked publishes a consistent, immutable view of the
// writer-owned fields. Callers must hold ls.Lock, except during construction
// and single-threaded startup before the LedgerState is made visible.
func (ls *LedgerState) publishSnapshotsLocked() {
	ls.snapshotGeneration++
	generation := ls.snapshotGeneration
	// Prevent a later append to the writer-owned slice from reusing storage
	// visible to an already-published snapshot. Element updates still require
	// replacing the cache and its slice-backed Epoch fields.
	ls.epochCache = ls.epochCache[:len(ls.epochCache):len(ls.epochCache)]
	ls.consensus.Store(&consensusSnapshot{
		generation:     generation,
		currentEpoch:   cloneEpoch(ls.currentEpoch),
		currentEra:     ls.currentEra,
		currentPParams: ls.currentPParams,
		prevEraPParams: ls.prevEraPParams,
		// epochCache is immutable after publication. Writers replace it for
		// element updates; its capped capacity also forces accidental appends to
		// allocate. Tip-only publications can therefore safely reuse it instead
		// of copying the full epoch history for every block.
		epochCache:                   ls.epochCache,
		transitionInfo:               ls.transitionInfo,
		syntheticV2CostModelInEffect: ls.syntheticV2CostModel,
	})
	ls.tip.Store(&tipSnapshot{
		generation:           generation,
		currentTip:           cloneTip(ls.currentTip),
		currentTipBlockNonce: cloneSnapshotBytes(ls.currentTipBlockNonce),
	})
}

// loadStateSnapshots returns consensus and tip state from the same publication
// generation. A writer stores the two pointers sequentially, so readers that
// need both retry during the small interval between those stores.
func (ls *LedgerState) loadStateSnapshots() (
	*consensusSnapshot,
	*tipSnapshot,
) {
	for {
		consensusState := ls.consensus.Load()
		tipState := ls.tip.Load()
		if consensusState.generation == tipState.generation {
			return consensusState, tipState
		}
	}
}

func (ls *LedgerState) loadConsensusSnapshot() *consensusSnapshot {
	return ls.consensus.Load()
}

func (ls *LedgerState) loadTipSnapshot() *tipSnapshot {
	return ls.tip.Load()
}

// SetTipForTesting replaces the in-memory tip and publishes a matching
// snapshot generation. It exists for black-box tests that cannot use the
// ledger package's white-box snapshot helpers.
func (ls *LedgerState) SetTipForTesting(tip ochainsync.Tip) {
	ls.Lock()
	defer ls.Unlock()
	ls.currentTip = cloneTip(tip)
	ls.publishSnapshotsLocked()
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

// eraTransitionPath returns the consecutive era IDs needed to advance from
// currentEraID to targetEraID. A two-transition path is accepted only when
// allowTwoTransitions records that boundaryEraForBlock validated the block's
// immediate-successor header elevation (for example, the Prime-mainnet
// Mary->Alonzo->Babbage boundary). More than two transitions in one boundary
// are never accepted: there is no safe way to reconstruct the omitted
// per-epoch rules from a single block.
func (ls *LedgerState) eraTransitionPath(
	currentEraID, targetEraID uint,
	allowTwoTransitions bool,
) ([]uint, bool) {
	if currentEraID == targetEraID {
		return nil, true
	}
	eraList := ls.eraList()
	currentIndex := -1
	targetIndex := -1
	for i := range eraList {
		switch eraList[i].Id {
		case currentEraID:
			currentIndex = i
		case targetEraID:
			targetIndex = i
		}
	}
	distance := targetIndex - currentIndex
	if currentIndex < 0 || targetIndex <= currentIndex || distance > 2 ||
		(distance == 2 && !allowTwoTransitions) {
		return nil, false
	}
	path := make([]uint, 0, targetIndex-currentIndex)
	for i := currentIndex + 1; i <= targetIndex; i++ {
		path = append(path, eraList[i].Id)
	}
	return path, true
}

// boundaryEraForBlock determines the era that a boundary block requires.
// The chainsync era identifies the block body. Once that body has advanced
// from the current era, its header protocol major can identify one additional
// successor era during a hard-fork boundary. A header version alone never
// advances an unchanged body era: source-era blocks can advertise the next
// protocol major before the hard fork. The boolean result records whether the
// validated body-plus-header elevation justifies a two-transition path.
func (ls *LedgerState) boundaryEraForBlock(
	currentEraID, blockEraID, headerMajor uint,
	headerMajorKnown bool,
) (uint, bool) {
	targetEraID := blockEraID
	if blockEraID == currentEraID || !headerMajorKnown {
		return targetEraID, false
	}
	headerEraID, ok := ls.eraForVersion(headerMajor)
	if !ok || headerEraID == blockEraID ||
		!ls.isValidEraAdvancement(blockEraID, headerEraID) {
		return targetEraID, false
	}
	path, ok := ls.eraTransitionPath(currentEraID, headerEraID, true)
	if !ok {
		return targetEraID, false
	}
	return headerEraID, len(path) == 2
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
	// Repopulate the in-memory deferred-header set from the persisted markers
	// so the snapshot retention floor covers headers still awaiting apply from
	// before the restart (issue #3727, finding 3): without this the first
	// post-restart epoch cleanup could prune a pool-stake snapshot such a
	// header needs. Runs here, before the database worker pool and cleanup
	// timer start, because it only reads ls.db directly and must FAIL CLOSED:
	// a scan failure that continued would leave the floor unpinned, and once
	// the apply cursor passes a pre-restart deferred header its now-pruned
	// snapshot is hard-rejected instead of deferred (the exact bug this PR
	// fixes). Aborting before any resource starts means there is nothing to
	// unwind on failure.
	if err := ls.repopulateDeferredHeaderValidation(); err != nil {
		return fmt.Errorf(
			"failed to repopulate deferred-header validation set: %w",
			err,
		)
	}

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

	// Schedule periodic process to purge consumed UTxOs outside of the rollback window
	ls.scheduleCleanupConsumedUtxos()
	// Load epoch info from DB
	//nolint:contextcheck // SubmitAsyncDBTxn has no context-aware variant.
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
	ls.loadSyntheticV2CostModel()
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
	// Reconstruct the evolving-nonce fold across Mithril "gap blocks" (blocks
	// between the ledger-state snapshot slot and the trust boundary) that were
	// imported without folding their VRF output. Must run after the tip and
	// trust boundary are loaded and before any epoch nonce is computed by
	// header verification, or the first post-bootstrap epoch boundary yields a
	// wrong nonce and every leader-VRF check in that epoch fails.
	if err := ls.healMithrilGapBlockNonces(ctx); err != nil {
		return fmt.Errorf("failed to heal Mithril gap block nonces: %w", err)
	}
	// Setup event handlers only after startup nonce repair is complete, so a
	// Mithril-bootstrapped node cannot process chainsync/blockfetch events with
	// stale gap-block nonces. ChainSync and chain-update can burst at bulk-sync
	// rates (#1556 / #1914), so they opt into the large EventQueueSize buffer.
	// Blockfetch events retain fully decoded blocks, so keep that lossless queue
	// to one commit batch and let EventBus backpressure bound decoded CBOR while
	// the chain store catches up. Sparser streams use the default.
	if ls.config.EventBus != nil {
		ls.chainsyncSubID = ls.config.EventBus.SubscribeFuncWithBufferPolicy(
			ChainsyncEventType,
			event.EventQueueSize,
			event.SubscriberBackpressureBlock,
			ls.handleEventChainsync,
		)
		ls.chainsyncAwaitReplySubID = ls.config.EventBus.SubscribeFunc(
			ChainsyncAwaitReplyEventType,
			ls.handleEventChainsyncAwaitReply,
		)
		ls.subscribeBlockfetchEvents(ls.handleEventBlockfetch)
		ls.chainUpdateSubID = ls.config.EventBus.SubscribeFuncWithBufferPolicy(
			chain.ChainUpdateEventType,
			event.EventQueueSize,
			event.SubscriberBackpressureBlock,
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
		ls.rewardPrecomputeSubID = ls.config.EventBus.SubscribeFunc(
			event.EpochTransitionEventType,
			ls.handleRewardPrecomputeEpochTransition,
		)
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
	// Publish the transitionInfo changes made above so snapshot readers observe
	// the reconstructed startup state. The HFI stability evaluation above may
	// launch an asynchronous tally, so serialize this publication with its
	// completion path and every other snapshot writer. Without this, a restart
	// that reconstructs TransitionKnown /
	// TransitionImpossible here would leave every snapshot-based reader
	// (CurrentTransitionInfo, ConsensusModeForEpoch, ...) on the stale
	// pre-loadTip() value until the next unrelated write path republishes.
	ls.Lock()
	ls.publishSnapshotsLocked()
	ls.Unlock()
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
	// Start the block-decode pipeline, if configured, before the goroutine
	// below that is its only submitter (ledgerReadChainIterator, reached via
	// ledgerProcessBlocks). Close stops it explicitly after that goroutine
	// drains -- like processBlocksCancel, this does not rely on ctx itself
	// being cancelled, since ctx is the node's long-lived context that a
	// live database restore/truncate never cancels. blockPipeline is nil
	// under ManualBlockProcessing (see NewLedgerState), so this is a plain
	// nil check rather than also re-checking that mode here.
	if ls.blockPipeline != nil {
		if err := ls.blockPipeline.Start(ctx); err != nil {
			return fmt.Errorf("failed to start block-decode pipeline: %w", err)
		}
		// Launched immediately, before the goroutine below (or anything
		// else) can submit a single block to the pipeline: see
		// drainBlockPipelineErrors's doc comment on why an undrained
		// errorsChan deadlocks the pipeline once enough validate-stage
		// errors (e.g. Byron-era blocks) have flowed through it.
		ls.blockPipelineErrorsDone = make(chan struct{})
		go ls.drainBlockPipelineErrors()
	}
	// Start goroutine to process new blocks unless the caller will feed trusted
	// batches directly into the replay loop. Uses its own child context
	// (not ctx directly) so Close can stop it independently of ctx's own
	// lifetime -- see processBlocksCancel's doc comment on the struct.
	if !ls.config.ManualBlockProcessing {
		processCtx, processCancel := context.WithCancel(ctx)
		ls.processBlocksCancel = processCancel
		ls.processBlocksWG.Go(func() {
			ls.ledgerProcessBlocks(processCtx)
		})
	}
	return nil
}

// subscribeBlockfetchEvents preserves lossless blockfetch delivery. A dropped
// blockfetch event cannot be replayed from the EventBus, so this subscriber
// remains attached until it drains or normal node lifecycle cancellation closes
// it rather than taking the ordinary stalled-subscriber detachment path.
func (ls *LedgerState) subscribeBlockfetchEvents(
	handler event.EventHandlerFunc,
) {
	ls.blockfetchSubID = ls.config.EventBus.SubscribeFuncWithBufferPolicy(
		BlockfetchEventType,
		blockfetchCommitBatchSize,
		event.SubscriberBackpressureBlock,
		handler,
	)
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
	// Pin rather than reading the installed store: this runs a scan, a
	// separate write transaction, and a commit against one store, so it has
	// to keep that store alive for the whole operation rather than sample
	// whichever is installed at each step.
	blobStore, releaseBlob := ls.db.PinBlob()
	defer releaseBlob()
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
	registeredVrfHash, ok := registeredPoolVrfKeyHash(pool)
	if !ok {
		return [32]byte{}, false, nil
	}
	copy(vrfHash[:], registeredVrfHash[:])
	return vrfHash, true, nil
}

// LatestOpCertSequence returns the highest opcert issue-number counter
// observed for poolID, honoring the same Mithril trust boundary block
// application enforces (see latestOpCertCounterAfterMithril): a plain MAX
// over the whole table would trust rows a Mithril import left below the
// certified boundary, giving startup and forge-loop credential checks a
// baseline block application itself does not use.
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
	return ls.latestOpCertCounterAfterMithril(
		pkh,
		ls.mithrilLedgerSlotSnapshot(),
		nil,
	)
}

// Datum looks up a datum by hash & adding this for implementing query.ReadData #741
func (ls *LedgerState) Datum(hash []byte) (*models.Datum, error) {
	return ls.db.GetDatum(hash, nil)
}

// CloseDBWorkerPoolShutdownTimeout, CloseProcessBlocksDrainTimeout, and
// CloseBlockfetchDrainTimeout bound the corresponding waits in Close()
// below. Exported (not local consts) so tests — including cross-package
// node-level tests exercising how a caller reacts to Close failing to
// confirm drain — can shrink them instead of running real multi-second
// timeouts.
var (
	CloseDBWorkerPoolShutdownTimeout = 15 * time.Second
	// A Leios block-processing transaction can include a large endorser
	// closure and legitimately outlive the short blockfetch waits. Keep this
	// below Node's default 30-second shutdown budget so the later ledger and
	// database cleanup stages retain time to finish if processing is stuck.
	CloseProcessBlocksDrainTimeout = 20 * time.Second
	CloseBlockPipelineDrainTimeout = 10 * time.Second
	CloseBlockfetchDrainTimeout    = 10 * time.Second
	CloseResultReplayTimeout       = 10 * time.Second
	// BlockPipelineRollbackDrainTimeout bounds how long an asynchronous
	// rollback (chainsync fork resolution or a peer-reported rollback --
	// see rollbackChainAndStateDeferred) waits for ls.blockPipeline to drain
	// in-flight decode/validate work before proceeding. See
	// drainBlockPipelineBeforeRollback's doc comment for what this
	// protects against and, just as importantly, what it does not.
	// Exported, like the Close* timeouts above, so tests can shrink it
	// instead of running a real multi-second wait.
	BlockPipelineRollbackDrainTimeout = 5 * time.Second
)

func (ls *LedgerState) Close() (retErr error) {
	// Close can be called once by the live lifecycle operation and again by
	// the node's normal shutdown after that operation cancels the node. Keep
	// the first result visible to every caller: returning nil from the second
	// call would make shutdown close storage after the first call reported an
	// unconfirmed drain.
	ls.closeMu.Lock()
	if ls.closeDone != nil {
		done := ls.closeDone
		ls.closeMu.Unlock()
		select {
		case <-done:
		case <-time.After(CloseResultReplayTimeout):
			return errors.New("previous ledger state close still in progress")
		}
		ls.closeMu.Lock()
		retErr = ls.closeErr
		ls.closeMu.Unlock()
		return retErr
	}
	ls.closeDone = make(chan struct{})
	done := ls.closeDone
	if ls.closed.Load() {
		// A few low-level tests mark closed directly to model a lifecycle
		// state that has already begun closing. There is no close result to
		// replay in that case, so publish the completed no-op explicitly.
		close(done)
		ls.closeMu.Unlock()
		return nil
	}
	ls.closed.Store(true)
	ls.closeMu.Unlock()
	defer func() {
		ls.closeMu.Lock()
		ls.closeErr = retErr
		close(done)
		ls.closeMu.Unlock()
	}()

	// Release any ledger.tx publish parked on a full ordered lane before
	// waiting on the goroutines that might be doing the publishing.
	if ls.publishCancel != nil {
		ls.publishCancel()
	}

	// Accumulates errors from the two bounded waits below (rollback
	// goroutines, database worker pool). Both used to only log a Warn on
	// timeout and fall through as if they'd succeeded -- indistinguishable
	// from the other, unconditional waits in this function to a caller, but
	// unlike those, a timeout here means Close() is returning while a
	// goroutine may still be reading/writing state a live restore/truncate
	// is about to close the underlying storage out from under
	// (closeStorageForLiveLifecycleOp runs immediately after this returns).
	var err error

	// Stop the dev-mode block-forging scheduler first, before anything
	// else below: initForge registers ls.forgeBlock on ls.Scheduler as a
	// fixed-interval task that writes directly to ls.chain/the database
	// (its own transaction, entirely bypassing ls.dbWorkerPool -- so
	// shutting down dbWorkerPool below provides no protection against
	// it). Left running, a live restore/truncate's quiesce (which stops
	// only the production BlockForger, node_lifecycle.go) would leave
	// this scheduler free to keep firing forgeBlock against a LedgerState
	// that's being closed and replaced out from under it, racing the
	// live operation's own blob/metadata mutations and the subsequent
	// construction of a new LedgerState with its own new Scheduler. A
	// stray block that lands in that window can leave the persistent
	// block-ID index with a gap whose far side doesn't actually chain
	// from the post-operation tip -- surfaced later, far from the actual
	// race, as "persistent chain index gap" from the chain iterator
	// (chain/chain.go) and a permanently stalled tip. Scheduler.Stop is
	// synchronous: it closes the ticker and waits for its worker pool to
	// drain, so no forgeBlock call can still be in flight once this
	// returns.
	if ls.Scheduler != nil {
		ls.Scheduler.Stop()
	}

	// Stop the periodic consumed-UTxO cleanup timer and drain a run already
	// under way. Timer.Stop does not wait for an AfterFunc callback that has
	// already fired, so the Wait is what keeps Close from returning while
	// cleanup is still issuing deletes. closed was set above, so
	// beginCleanupConsumedUtxosRun now rejects every new run and the timer
	// callback cannot re-arm itself.
	//
	// Unconditional, like the header-replay and reward-precompute waits
	// below and unlike this function's bounded ones: returning early here
	// would reintroduce exactly the use-after-close this is meant to
	// prevent. A run is bounded by cleanupConsumedUtxoBatchSize -- one short
	// delete transaction, deliberately sized so it cannot monopolize
	// SQLite -- so there is no unbounded drain to guard against.
	ls.cleanupMu.Lock()
	if ls.timerCleanupConsumedUtxos != nil {
		ls.timerCleanupConsumedUtxos.Stop()
	}
	ls.cleanupMu.Unlock()
	ls.cleanupWG.Wait()

	// Stop the decode pipeline at the same time as the block-processing
	// goroutine. decodeReadChainBatch drains the pipeline's Results channel
	// without a context select once a batch has been submitted; waiting for
	// ledgerProcessBlocks before stopping the pipeline can therefore deadlock
	// shutdown until CloseProcessBlocksDrainTimeout expires. Stop cancels
	// Submit calls and closes Results, which releases that drain so the
	// block-processing goroutine can exit normally.
	var blockPipelineStopDone chan error
	if ls.blockPipeline != nil {
		blockPipelineStopDone = make(chan error, 1)
		go func() {
			stopErr := ls.blockPipeline.Stop()
			// Stop closes errorsChan as one of its final steps, once every
			// stage has fully drained -- see BlockPipeline.Stop. Waiting for
			// drainBlockPipelineErrors here (it exits via range-over-channel
			// as soon as that close is visible) adds no additional blocking
			// beyond what Stop already guarantees, and ensures the drain
			// goroutine cannot still be running once this reports done.
			if ls.blockPipelineErrorsDone != nil {
				<-ls.blockPipelineErrorsDone
			}
			blockPipelineStopDone <- stopErr
		}()
	}

	// Stop the normal chainsync-driven block-processing pipeline next, for
	// the identical reason as Scheduler.Stop above: ledgerProcessBlocks
	// writes blocks to ls.chain/the database (via dbWorkerPool below), and
	// Start launched it with its own child context specifically so this
	// call can cancel it independently of ctx's own lifetime -- ctx is
	// n.ctx, the node's long-lived context, which a live restore/truncate
	// never cancels (see processBlocksCancel's doc comment on the struct).
	// Bounded, like the rollback/dbWorkerPool waits below, so a genuinely
	// stuck pipeline cannot hang Close forever -- but unlike those two
	// before this fix, a timeout here must not be silently treated as
	// success: a block landing mid-write exactly as dbWorkerPool shuts
	// down below leaves the persisted block-ID index permanently
	// inconsistent with the in-memory tip that already advanced for it,
	// which is exactly the "persistent chain index gap" this whole
	// function's Scheduler.Stop comment already describes for the
	// dev-mode-forging case -- this is the same failure mode, just via the
	// production chainsync path instead.
	if ls.processBlocksCancel != nil {
		ls.processBlocksCancel()
		processBlocksDone := make(chan struct{})
		go func() {
			ls.processBlocksWG.Wait()
			close(processBlocksDone)
		}()
		select {
		case <-processBlocksDone:
		case <-time.After(CloseProcessBlocksDrainTimeout):
			err = errors.Join(
				err,
				fmt.Errorf(
					"timed out after %s waiting for block-processing pipeline to stop",
					CloseProcessBlocksDrainTimeout,
				),
			)
		}
	}

	// Wait for the decode pipeline after its block-processing consumer has
	// drained. Stop was started above so it could release a consumer blocked
	// waiting for Results; this wait still bounds pipeline teardown and
	// surfaces a failure to the caller.
	if blockPipelineStopDone != nil {
		select {
		case stopErr := <-blockPipelineStopDone:
			if stopErr != nil {
				err = errors.Join(
					err,
					fmt.Errorf("block-decode pipeline shutdown: %w", stopErr),
				)
			}
		case <-time.After(CloseBlockPipelineDrainTimeout):
			err = errors.Join(
				err,
				fmt.Errorf(
					"timed out after %s waiting for block-decode pipeline to stop",
					CloseBlockPipelineDrainTimeout,
				),
			)
		}
	}

	// Synchronize with continuation scheduling before waiting. Close() marks
	// the ledger closed above; startQueuedBlockfetchFromEventLocked checks that
	// flag both before and after taking this mutex, so no worker can be added
	// after this synchronization point. Do not hold the mutex across Wait: the
	// blockfetch subscriber may need it while completing the request that lets a
	// worker return.
	continuationSchedulingDone := make(chan struct{})
	ls.blockfetchContinuationMu.Lock()
	close(continuationSchedulingDone)
	ls.blockfetchContinuationMu.Unlock()
	if ls.blockfetchContinuationSchedulingHook != nil {
		ls.blockfetchContinuationSchedulingHook()
	}
	<-continuationSchedulingDone
	blockfetchContinuationsDone := make(chan struct{})
	go func() {
		ls.blockfetchContinuationWG.Wait()
		close(blockfetchContinuationsDone)
	}()
	select {
	case <-blockfetchContinuationsDone:
	case <-time.After(CloseBlockfetchDrainTimeout):
		err = errors.Join(
			err,
			fmt.Errorf(
				"timed out after %s waiting for blockfetch continuations to stop",
				CloseBlockfetchDrainTimeout,
			),
		)
	}

	// Unsubscribe from event bus to stop receiving new events. Use
	// UnsubscribeAndWait, not Unsubscribe: several of these handlers read
	// component fields (e.g. chainsyncState via GetActiveConnectionFunc)
	// that callers of Close() go on to nil out or replace immediately
	// after it returns (see node_lifecycle.go's live restore/truncate
	// path). Plain Unsubscribe only stops future deliveries -- a handler
	// goroutine that already dequeued an event before this loop runs could
	// otherwise still be executing concurrently with that teardown.
	if ls.config.EventBus != nil {
		ls.config.EventBus.UnsubscribeAndWait(
			ChainsyncEventType,
			ls.chainsyncSubID,
		)
		ls.config.EventBus.UnsubscribeAndWait(
			ChainsyncAwaitReplyEventType,
			ls.chainsyncAwaitReplySubID,
		)
		ls.config.EventBus.UnsubscribeAndWait(
			BlockfetchEventType,
			ls.blockfetchSubID,
		)
		ls.config.EventBus.UnsubscribeAndWait(
			chain.ChainUpdateEventType,
			ls.chainUpdateSubID,
		)
		ls.config.EventBus.UnsubscribeAndWait(
			chainselection.ChainSwitchEventType,
			ls.chainSwitchSubID,
		)
		ls.config.EventBus.UnsubscribeAndWait(
			ConnectionClosedEventType,
			ls.connClosedSubID,
		)
		ls.config.EventBus.UnsubscribeAndWait(
			event.EpochTransitionEventType,
			ls.rewardPrecomputeSubID,
		)
	}

	// Rollback transaction events no longer need a drain wait here.
	// They are emitted inline by handleEventChainUpdate (see
	// block_event.go), so the UnsubscribeAndWait on ChainUpdateEventType
	// above already guarantees none is in flight by this point.

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

	ls.config.Logger.Info("waiting for in-flight reward precompute handlers")
	rewardStart := time.Now()
	// closed was set before the subscriptions were removed. Taking the mutex
	// once ensures that any handler which entered before then has either added
	// its worker to the wait group or observed the closed state. Do not hold the
	// mutex while waiting: the worker needs it to discard pending work and exit.
	ls.rewardPrecomputeMu.Lock()
	ls.rewardPrecomputePending = nil
	ls.rewardPrecomputeRetry = nil
	ls.rewardPrecomputeMu.Unlock()
	ls.rewardPrecomputeWG.Wait()
	ls.config.Logger.Info(
		"reward precompute handlers finished",
		"elapsed", time.Since(rewardStart).Round(time.Millisecond),
	)

	// Stop slot clock
	if ls.slotClock != nil {
		ls.config.Logger.Info("stopping slot clock")
		ls.slotClock.Stop()
		ls.config.Logger.Info("slot clock stopped")
	}

	// Shutdown database worker pool. Shutdown's own drain wait is bounded by
	// the same CloseDBWorkerPoolShutdownTimeout this outer select uses (see
	// its doc comment), so the two timeouts fire at approximately the same
	// moment; the outer select still owns the definitive bound on Close
	// itself, and still needs its own timer in case Shutdown is somehow
	// slower to notice its own deadline than this goroutine is to notice
	// Shutdown never returning at all.
	if ls.dbWorkerPool != nil {
		ls.config.Logger.Info("shutting down database worker pool")
		poolStart := time.Now()
		poolDone := make(chan error, 1)
		go func(drainTimeout time.Duration) {
			poolDone <- ls.dbWorkerPool.Shutdown(drainTimeout)
		}(CloseDBWorkerPoolShutdownTimeout)
		select {
		case shutdownErr := <-poolDone:
			if shutdownErr != nil {
				ls.config.Logger.Warn(
					"database worker pool did not fully shut down",
					"elapsed", time.Since(poolStart).Round(time.Millisecond),
					"error", shutdownErr,
				)
				err = errors.Join(
					err,
					fmt.Errorf(
						"database worker pool shutdown: %w",
						shutdownErr,
					),
				)
			} else {
				ls.config.Logger.Info(
					"database worker pool shut down",
					"elapsed", time.Since(poolStart).Round(time.Millisecond),
				)
			}
		case <-time.After(CloseDBWorkerPoolShutdownTimeout):
			ls.config.Logger.Warn(
				"timed out waiting for database worker pool shutdown",
				"elapsed", time.Since(poolStart).Round(time.Millisecond),
			)
			err = errors.Join(
				err,
				fmt.Errorf(
					"timed out after %s waiting for database worker pool shutdown",
					time.Since(poolStart).Round(time.Millisecond),
				),
			)
		}
	}

	// Note: We don't close the database here because LedgerState doesn't own it.
	// The database is passed in via LedgerStateConfig and should be closed by
	// the owner (typically Node.shutdown()).
	return err
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
	provider := newSlotTimeConverterProvider(ls.timeConv())
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
		consensusState, tipState := ls.loadStateSnapshots()
		currentEpoch := consensusState.currentEpoch
		currentEra := consensusState.currentEra
		currentPParams := consensusState.currentPParams
		tipSlot := tipState.currentTip.Point.Slot

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
		// current era's stability window from the admitted upstream-header
		// frontier.
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
				PreviousEpoch: tick.Epoch - 1,
				NewEpoch:      tick.Epoch,
				BoundarySlot:  tick.Slot,
				EpochNonce:    nil,
				ProtocolVersion: ls.protocolMajorForEvent(
					currentPParams,
					currentEra,
				),
				SnapshotSlot: snapshotSlot,
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

// epochBoundarySnapshotHookHolder wraps the optional authoritative
// epoch-boundary snapshot capture so it can live in an atomic.Pointer. The hook
// is invoked from inside the epoch-rollover write transaction (see
// processEpochRollover) with that transaction's handle, so the captured mark
// snapshot commits atomically with the epoch it belongs to.
type epochBoundarySnapshotHookHolder struct {
	fn func(*database.Txn, event.EpochTransitionEvent) error
}

// SetEpochBoundarySnapshotHook installs (or clears, with a nil fn) the
// authoritative epoch-boundary snapshot capture. It is wired at node startup to
// the snapshot manager's CaptureEpochBoundarySnapshot before block sync begins.
// When no hook is set the ledger relies solely on the event-driven fallback
// capture, preserving the pre-wiring behavior.
func (ls *LedgerState) SetEpochBoundarySnapshotHook(
	fn func(*database.Txn, event.EpochTransitionEvent) error,
) {
	if fn == nil {
		ls.epochSnapshotHook.Store(nil)
		return
	}
	ls.epochSnapshotHook.Store(&epochBoundarySnapshotHookHolder{fn: fn})
}

// epochBoundarySnapshotHook returns the installed authoritative capture hook, or
// nil when none is set.
func (ls *LedgerState) epochBoundarySnapshotHook() func(*database.Txn, event.EpochTransitionEvent) error {
	if h := ls.epochSnapshotHook.Load(); h != nil {
		return h.fn
	}
	return nil
}

// SetEpochBoundarySnapshotStakeHook installs (or clears, with a nil fn) the
// SNAP-point stake read of the authoritative epoch-boundary capture. It is wired
// at node startup to the snapshot manager's ComputeEpochBoundarySnapshot,
// alongside SetEpochBoundarySnapshotHook.
//
// cardano-ledger runs SNAP before POOLREAP and before governance enactment, so
// the mark snapshot's stake must be read immediately after the delayed reward
// update and MIR — the boundary rules that precede SNAP — while the snapshot row
// itself can only be written at the end of the rollover, where the new epoch's
// nonce and the post-enactment protocol version exist. This hook is the read
// half; epochSnapshotHook is the write half. Both run in the same rollover
// transaction. With no stake hook installed the write half reads the stake
// itself using boundary-aware historical reconstruction.
func (ls *LedgerState) SetEpochBoundarySnapshotStakeHook(
	fn func(*database.Txn, event.EpochTransitionEvent) error,
) {
	if fn == nil {
		ls.epochSnapshotStakeHook.Store(nil)
		return
	}
	ls.epochSnapshotStakeHook.Store(&epochBoundarySnapshotHookHolder{fn: fn})
}

// epochBoundarySnapshotStakeHook returns the installed SNAP-point stake hook, or
// nil when none is set.
func (ls *LedgerState) epochBoundarySnapshotStakeHook() func(*database.Txn, event.EpochTransitionEvent) error {
	if h := ls.epochSnapshotStakeHook.Load(); h != nil {
		return h.fn
	}
	return nil
}

func (ls *LedgerState) protocolMajorForEvent(
	pparams lcommon.ProtocolParameters,
	era eras.EraDesc,
) uint {
	pv, err := GetProtocolVersion(pparams)
	if err == nil {
		return pv.Major
	}
	if ls.config.Logger != nil {
		ls.config.Logger.Warn(
			"could not extract protocol version for epoch event",
			"error", err,
			"pparams_type", fmt.Sprintf("%T", pparams),
			"fallback_era_id", era.Id,
			"component", "ledger",
		)
	}
	return era.Id
}

// isNearTip returns true when the given slot is inside the current era's
// stability window from the admitted upstream-header frontier. This is used
// to decide whether to emit slot-clock epoch events. During initial catch-up
// the node is far behind the frontier and these checks are skipped; once the
// node is close to the frontier they are always on. Returns false when no
// upstream header is admitted yet (no peer connected), since we can't
// determine proximity.
func (ls *LedgerState) isNearTip(slot uint64) bool {
	return ls.isNearTipWithStabilityWindow(slot, ls.calculateStabilityWindow())
}

func (ls *LedgerState) isNearTipWithStabilityWindow(
	slot, stabilityWindow uint64,
) bool {
	upstreamTip := ls.UpstreamTipSlot()
	if upstreamTip == 0 {
		return false
	}
	return nearUpstreamTip(slot, upstreamTip, stabilityWindow)
}

// nearUpstreamTip reports whether slot is within stabilityWindow of a KNOWN
// upstreamTip. Callers that must tell "upstream tip unknown" apart from
// "known, and we are far behind it" read UpstreamTipSlot themselves and call
// this directly; isNearTipWithStabilityWindow folds unknown into not-near,
// which is the safe answer for its callers but not for all of them.
func nearUpstreamTip(slot, upstreamTip, stabilityWindow uint64) bool {
	if slot >= upstreamTip {
		return true
	}
	return upstreamTip-slot <= stabilityWindow
}

func (ls *LedgerState) scheduleCleanupConsumedUtxos() {
	ls.cleanupMu.Lock()
	defer ls.cleanupMu.Unlock()
	// The timer callback re-arms itself, so a run already in flight when
	// Close stopped the timer would otherwise install a fresh one behind
	// Close's back -- leaving a self-perpetuating timer firing every
	// interval, for the rest of the process, against a database the owner
	// closes as soon as Close returns.
	if ls.closed.Load() {
		return
	}
	if ls.timerCleanupConsumedUtxos != nil {
		ls.timerCleanupConsumedUtxos.Stop()
	}
	ls.timerCleanupConsumedUtxos = time.AfterFunc(
		cleanupConsumedUtxosInterval,
		func() {
			if ls.cleanupConsumedUtxosTimerFiredHook != nil {
				ls.cleanupConsumedUtxosTimerFiredHook()
			}
			ls.cleanupConsumedUtxos()
			// Schedule the next run
			ls.scheduleCleanupConsumedUtxos()
		},
	)
}

// beginCleanupConsumedUtxosRun registers a consumed-UTxO cleanup run with the
// shutdown drain, reporting false once Close has begun. Both cleanup triggers
// go through it: the periodic timer, and the epoch transition's bare
// `go ls.cleanupConsumedUtxos()`, which stopping the timer does not constrain.
//
// closed is set before Close takes cleanupMu, and this re-checks it under that
// same mutex, so no run can register after Close has started waiting.
func (ls *LedgerState) beginCleanupConsumedUtxosRun() bool {
	ls.cleanupMu.Lock()
	defer ls.cleanupMu.Unlock()
	if ls.closed.Load() {
		return false
	}
	ls.cleanupWG.Add(1)
	return true
}

func (ls *LedgerState) cleanupConsumedUtxos() {
	// Refuse to begin database work once Close has started, and keep Close
	// waiting for this run otherwise. LedgerState does not own the database
	// (see the note at the end of Close); its owner closes it immediately
	// after Close returns.
	if !ls.beginCleanupConsumedUtxosRun() {
		return
	}
	defer ls.cleanupWG.Done()
	if ls.cleanupConsumedUtxosRunHook != nil {
		ls.cleanupConsumedUtxosRunHook()
	}
	// Cleanup is advisory pruning. Never let the periodic timer and the epoch
	// transition trigger occupy SQLite's single write connection at the same
	// time. Each invocation handles one bounded batch so cleanup remains
	// resumable and cannot hold the connection for an unbounded drain.
	if !ls.cleanupConsumedUtxosRunning.CompareAndSwap(false, true) {
		return
	}
	defer ls.cleanupConsumedUtxosRunning.Store(false)
	if ls.ctx != nil {
		select {
		case <-ls.ctx.Done():
			return
		default:
		}
	}

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
	// Read once and gate on a KNOWN upstream tip only. An unknown one is not
	// evidence of catching up: no peer has ever connected, or no active
	// connection currently exposes the retained admitted-header frontier.
	// Deferring on that would
	// retain consumed rows forever on a node without peers, where cleanup
	// previously ran off the local tip alone -- an unbounded utxo table in
	// exactly the mode documented as minimal storage.
	upstreamTip, upstreamActive := ls.UpstreamSyncStatus()
	if upstreamActive && upstreamTip == 0 {
		ls.config.Logger.Debug(
			"deferring consumed UTxO cleanup until upstream target is known",
			"component", "ledger",
		)
		return
	}
	if upstreamTip != 0 &&
		!nearUpstreamTip(tipSlot, upstreamTip, stabilityWindow) {
		ls.config.Logger.Debug(
			"deferring consumed UTxO cleanup while catching up",
			"component", "ledger",
			"tip_slot", tipSlot,
			"upstream_tip_slot", upstreamTip,
		)
		return
	}

	// Delete UTxOs that are marked as deleted and older than our slot window
	ls.config.Logger.Debug(
		"cleaning up consumed UTxOs",
		"component", "ledger",
	)
	if stabilityWindow == 0 {
		return
	}
	if tipSlot > stabilityWindow {
		// No lock needed here - the database handles its own consistency
		// and we're not accessing any in-memory LedgerState fields.
		// The tipSlot was captured above with a read lock.
		_, err := ls.db.UtxosDeleteConsumed(
			tipSlot-stabilityWindow,
			cleanupConsumedUtxoBatchSize,
			nil,
		)
		if err != nil {
			ls.config.Logger.Error(
				"failed to cleanup consumed UTxOs",
				"component", "ledger",
				"error", err,
			)
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
		return ls.enforceDurableTipFloor()
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
	// A target sharing the applied tip's slot with a different hash cannot be
	// expressed by the UTxO and transaction truncation predicates in
	// database.TruncateAfterSlot. Those are slot-only (added_slot > slot,
	// deleted_slot > slot), unlike DeleteBlockNoncesAfterPoint beside them,
	// which is point-aware (slot > ? OR (slot = ? AND hash <> ?)) precisely
	// because same-slot competitors must not survive a rollback.
	//
	// Left alone, such a target truncates nothing at the contested slot: the
	// abandoned same-slot block's outputs stay live, and the UTxOs it consumed
	// stay marked spent with deleted_slot equal to the boundary, so the
	// strictly-greater restore never matches them. Once cleanup hard-deletes
	// those rows there is nothing left to restore at all. The next block that
	// legitimately spends one of them cannot resolve the input, which Conway
	// reports as bad inputs and -- because value conservation sums consumed
	// over only the inputs that did resolve -- as value not conserved in the
	// same pass, from the one divergence (issue #3678).
	//
	// Redirect to the newest applied ancestor strictly below the contested
	// slot so the existing predicates truncate that slot whole. The block at
	// point is then re-applied by the pipeline, which is what makes recovery
	// converge instead of rewinding to the same non-repairing target forever.
	//
	// This applies only when the competing tip was genuinely applied, which a
	// recorded block nonce witnesses. A tip that carries no nonce was never
	// applied -- enforceDurableTipFloor also repairs an in-memory tip that
	// leads the durable state -- so there is nothing at the contested slot to
	// undo and the slot-only predicates are already correct for it.
	sameSlotCompetitor := point.Slot == currentTip.Point.Slot &&
		!bytes.Equal(point.Hash, currentTip.Point.Hash)
	if sameSlotCompetitor {
		tipNonce, nonceErr := ls.db.GetBlockNonce(currentTip.Point, nil)
		if nonceErr != nil {
			return fmt.Errorf(
				"read applied nonce for contested tip at slot %d: %w",
				currentTip.Point.Slot,
				nonceErr,
			)
		}
		sameSlotCompetitor = len(tipNonce) > 0
	}
	if sameSlotCompetitor {
		// latestLedgerPrimaryChainAncestor searches block nonces over the
		// half-open range [start, point.Slot), so passing point already means
		// "strictly below the contested slot". Passing point.Slot-1 would skip
		// an applied ancestor sitting at exactly point.Slot-1.
		ancestor, ok, ancestorErr := ls.latestLedgerPrimaryChainAncestor(
			point,
			false,
		)
		if ancestorErr != nil {
			return fmt.Errorf(
				"resolve applied ancestor below contested slot %d: %w",
				point.Slot,
				ancestorErr,
			)
		}
		if !ok {
			return fmt.Errorf(
				"no applied ancestor below contested slot %d: %w",
				point.Slot,
				ErrNoAppliedAncestorBelowContestedSlot,
			)
		}
		ls.config.Logger.Warn(
			"rollback target shares the applied tip's slot with a different hash, redirecting below the contested slot",
			"component", "ledger",
			"contested_slot", point.Slot,
			"rollback_hash", hex.EncodeToString(point.Hash),
			"ledger_tip_hash", hex.EncodeToString(currentTip.Point.Hash),
			"ancestor_slot", ancestor.Slot,
			"ancestor_hash", hex.EncodeToString(ancestor.Hash),
		)
		point = ancestor
	}
	if mithrilLedgerSlot > 0 && point.Slot < mithrilLedgerSlot {
		return ErrRollbackExceedsMithrilBoundary
	}
	// Bracket every rollback mutation so split reward precomputation cannot
	// persist results that mixed pre- and post-rollback blocks, pots, protocol
	// state, or account history. The active count also keeps overlapping
	// rollbacks from exposing an apparently stable even generation.
	ls.rewardInputRollbackActive.Add(1)
	ls.rewardInputGeneration.Add(1)
	defer func() {
		ls.rewardInputGeneration.Add(1)
		ls.rewardInputRollbackActive.Add(-1)
	}()
	// Track new tip value built during transaction
	var newTip ochainsync.Tip
	var newNonce []byte
	// Start a transaction. The metadata+blob truncation sweep itself lives
	// in database.TruncateAfterSlot, shared with offline/live database
	// truncation (database/lifecycle) — this closure wraps it with the
	// CIP-0163 reward-account expiration hooks (ledger-owned, since they
	// need the epoch schedule) and captures the resulting tip/nonce for
	// the in-memory cache reload below.
	err := ls.SubmitAsyncDBTxn(func(txn *database.Txn) error {
		// CIP-0163: capture the reward-account credentials witnessed in the
		// rolled-away blocks (added_slot > rollback slot) before
		// TruncateAfterSlot's certificate/reward-withdrawal deletes remove
		// their rows. Their expiration_epoch is recomputed against the
		// surviving chain once TruncateAfterSlot has restored account
		// state (see below). Gate off => skip.
		var expiryAffectedRefs []models.StakeCredentialRef
		if ls.config.DelegatorInactivityEnabled {
			var affErr error
			expiryAffectedRefs, affErr = ls.db.AccountsWitnessedAfterSlot(
				point.Slot,
				txn,
			)
			if affErr != nil {
				return fmt.Errorf(
					"collect rolled-back witnessed accounts: %w",
					affErr,
				)
			}
		}
		var err error
		newTip, newNonce, err = ls.db.TruncateAfterSlot(
			point,
			mithrilLedgerSlot,
			txn,
		)
		if err != nil {
			return err
		}
		// CIP-0163: now that TruncateAfterSlot has deleted the rolled-away
		// certificate/withdrawal rows and restored the remaining account
		// fields (both ahead of its own pool/DRep/governance/etc. sweep),
		// recompute the expiration_epoch of the affected reward accounts
		// against the surviving chain. Gate off => no-op.
		if err := ls.recomputeAccountExpirationsAfterRollback(
			txn,
			point.Slot,
			expiryAffectedRefs,
		); err != nil {
			return fmt.Errorf(
				"recompute account expirations after rollback: %w",
				err,
			)
		}
		// Undo the synthetic-PlutusV2-cost-model marker if this rollback
		// crosses back before the epoch it was last confirmed cleared at;
		// see database.RecomputeSyntheticV2CostModelMarkerAfterTruncate and
		// blinklabs-io/dingo#3825's PR review (wolf31o2).
		if err := database.RecomputeSyntheticV2CostModelMarkerAfterTruncate(
			ls.db,
			txn,
			point.Slot,
		); err != nil {
			return fmt.Errorf(
				"recompute synthetic PlutusV2 cost model marker after rollback: %w",
				err,
			)
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
		// ppComputed distinguishes "computePParams succeeded and returned
		// nil" from "computePParams was skipped or failed". Byron has no
		// protocol-parameter representation, so a rollback into Byron
		// legitimately computes nil, and a nil check alone cannot tell that
		// apart from an error -- which would leave the pre-rollback Shelley
		// value in place under a Byron currentEra.
		ppComputed  bool
		eraResolved bool
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
			ppComputed = true
		}
	}
	// Reload the synthetic-PlutusV2-cost-model marker against the
	// rolled-back pparams, mirroring every other piece of state reloaded
	// above: computed here (nil newPParams when !ppComputed correctly
	// resolves to "not synthetic", since a Byron-era view has no cost
	// models at all) rather than inside the locked section below, since the
	// database read this needs must not happen while ls.Lock() is held.
	newSyntheticV2CostModelValue, syntheticErr := ls.db.GetSyncState(
		database.SyntheticV2CostModelSyncKey, nil,
	)
	if syntheticErr != nil {
		ls.config.Logger.Warn(
			"failed to reload synthetic PlutusV2 cost model marker after rollback",
			"error",
			syntheticErr,
			"component",
			"ledger",
		)
	}
	newSyntheticV2CostModel := resolveSyntheticV2CostModel(
		newSyntheticV2CostModelValue, newPParams,
	)
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
	// Assign on "was computed", not on "is non-nil". Rolling back into Byron
	// computes nil legitimately, and the previous nil check skipped the
	// assignment there, leaving ls.currentPParams holding its Shelley value
	// while ls.currentEra had already become Byron. That contradicted the
	// era/parameter invariant this path maintains everywhere else, and
	// GetCurrentPParams reported Shelley parameters for a Byron ledger until
	// the next rollover healed it via cloneProtocolParametersForEra.
	if ppComputed {
		ls.currentPParams = newPParams
		ls.prevEraPParams = newPrevPParams
	}
	ls.syntheticV2CostModel = newSyntheticV2CostModel
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
	ls.publishSnapshotsLocked()
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
	if err := ls.enforceDurableTipFloor(); err != nil {
		return err
	}
	return nil
}

// drainBlockPipelineBeforeRollback waits, up to
// BlockPipelineRollbackDrainTimeout, for ls.blockPipeline to finish any
// decode/validate work already submitted before a rollback proceeds to
// physically remove blocks from ls.chain and truncate ledger metadata. It
// is a no-op when ls.blockPipeline is nil (pipeline disabled or
// ManualBlockProcessing), matching every other pipeline-conditional code
// path in this file.
//
// Why this matters (issue #1894 phase 5): ledgerReadChainIterator -- the
// pipeline's only submitter -- runs on its own goroutine, entirely
// decoupled from the goroutine that decides a rollback. rollbackChainAndStateDeferred
// in particular is reached from chainsync per-connection handling
// (handleEventChainsyncRollback, tryResolveFork), never from
// ledgerProcessBlocks itself. Chain-selection can decide to abandon a fork
// while the reader goroutine has already gathered and submitted a batch of
// blocks -- from that very fork -- to the pipeline's decode/validate
// workers and is blocked draining Results() for it. Without this wait,
// ls.chain.Rollback can delete those blocks' rows out from under a
// validate-stage worker still processing them, and -- more importantly --
// ledgerProcessBlocksFromSource can go on to apply that already-decoded
// batch to ls.currentTip immediately afterward, momentarily re-advancing
// the ledger tip onto a fork chain-selection has already discarded.
//
// What this does NOT do: it only waits for ls.blockPipeline's own decode/
// validate stages (PendingCount) to empty, not for
// ledgerProcessBlocksFromSource's subsequent DB-apply of a batch already
// drained from the pipeline before this call started -- that step runs
// entirely outside blockPipeline (issue #1894 phase 2, wiring real ledger
// apply into gouroboros' pipeline.ApplyFunc, is deliberately deferred to
// #3227; see this file's other doc comments on that decision). A rollback
// landing exactly in that narrower window can still leave ls.currentTip
// transiently re-advanced onto an abandoned block. This is not a new
// failure mode introduced by the pipeline: processChainIteratorRollback's
// stale-tip detection (a direct, uncached database.BlockByPoint lookup)
// already exists specifically to self-heal exactly this class of lag
// between chain-selection and ledger apply, and remains the backstop here
// regardless of how this wait performs. This wait exists to shrink that
// window and the resulting spurious errRestartLedgerPipeline churn (a full
// read-chain-attempt restart), not to claim it eliminates the window
// outright.
//
// ctx bounds the wait together with BlockPipelineRollbackDrainTimeout --
// whichever fires first ends the wait -- so a caller with its own
// cancellable context (e.g. processChainIteratorRollback, driven by
// ledgerProcessBlocksFromSource's attempt context) does not block a
// shutdown or restart on the fixed timeout. Callers with no context of
// their own (rollbackChainAndStateDeferred, reached from chainsync event handling
// with no ctx threaded through) pass context.Background(), matching
// decodeReadChainBatch's identical, already-established choice for the
// same reason.
//
// reason is included in the log line on both the success and timeout
// paths purely for operator diagnostics; it does not affect behavior.
func (ls *LedgerState) drainBlockPipelineBeforeRollback(
	ctx context.Context,
	reason string,
) {
	if ls.blockPipeline == nil {
		return
	}
	waitCtx, cancel := context.WithTimeout(
		ctx,
		BlockPipelineRollbackDrainTimeout,
	)
	defer cancel()
	start := time.Now()
	if err := ls.blockPipeline.WaitForDrain(waitCtx); err != nil {
		ls.config.Logger.Warn(
			"timed out waiting for block-processing pipeline to drain before rollback, proceeding anyway",
			"component",
			"ledger",
			"reason",
			reason,
			"elapsed",
			time.Since(start).Round(time.Millisecond),
			"pending",
			ls.blockPipeline.PendingCount(),
			"error",
			err,
		)
		return
	}
	ls.config.Logger.Debug(
		"block-processing pipeline drained before rollback",
		"component", "ledger",
		"reason", reason,
		"elapsed", time.Since(start).Round(time.Millisecond),
	)
}

// rollbackChainAndStateDeferred rewinds the primary chain and ledger state,
// deferring the chain.update events the rewind produces rather than letting the
// chain publish them inline. This method is reached from chainsync event
// handling (handleEventChainsyncRollback, tryResolveFork) while chainsyncMutex
// is held; an inline, back-pressured chain.update publish under that mutex is
// the drain deadlock (see pendingPublishes). RollbackDeferred enqueues those
// events on the chain's shared sequencer under c.mutex, so they are ordered
// against concurrent blockfetch adds in true chain-mutation order (the
// rollback-before-add guarantee is now chain-wide, not merely within this
// handler's queue); registering the chain on pubs.chainDrains drains that
// sequencer after the mutex is released. A nil pubs drains immediately
// (unlocked / test path).
func (ls *LedgerState) rollbackChainAndStateDeferred(
	point ocommon.Point,
	pubs *pendingPublishes,
) error {
	ls.RLock()
	mithrilLedgerSlot := ls.mithrilLedgerSlot
	ls.RUnlock()
	if mithrilLedgerSlot > 0 && point.Slot < mithrilLedgerSlot {
		return ErrRollbackExceedsMithrilBoundary
	}
	// Exclude ledgerReadChainIterator's gather-then-submit cycle for the
	// entire remainder of this function -- see blockPipelineGatherMutex's
	// doc comment on the LedgerState struct for why drainBlockPipelineBeforeRollback
	// alone cannot close this window: it only accounts for work already
	// handed to blockPipeline, not for raw blocks the reader has already
	// pulled off the chain iterator but not yet submitted.
	ls.blockPipelineGatherMutex.Lock()
	defer ls.blockPipelineGatherMutex.Unlock()
	// Drain in-flight pipeline work before validating/emitting the undo
	// events, and validate/emit those events before the physical
	// truncation -- both orderings exist for the same reason: each step
	// can only see, or only outrun, what has already happened before it.
	//
	// Draining first matters because validateAndEmitRollbackUndo's emit
	// (emitRollbackTransactionEvents, via blocksAboveSlot) works from what
	// is already committed to the db. Blocks the pipeline is still
	// decoding/validating/applying for the fork about to be abandoned are
	// not there yet. If they finished applying (and published their own
	// forward ledger.tx events) after the undo emit had already run, no
	// undo event would ever cover them, and chain.Rollback would still
	// physically delete them -- a ledger.tx subscriber would keep derived
	// state for a transaction the chain silently dropped. Draining first
	// lets any such in-flight blocks finish applying and publish their
	// forward events, so blocksAboveSlot's read (and the undo events it
	// drives) covers them too.
	//
	// Emitting before truncating matters for the opposite reason: the
	// block-apply goroutine can start applying the post-rollback chain the
	// moment chain.Rollback lands and publish forward events on the same
	// ledger.tx lane, so the undos have to be enqueued first -- but only
	// once the rollback is known to be acceptable. See
	// validateAndEmitRollbackUndo.
	//
	// No ctx is threaded through the chainsync event-handling call chain
	// that reaches this method (handleEventChainsyncRollback,
	// tryResolveFork), so drainBlockPipelineBeforeRollback deliberately
	// uses context.Background(), identical to decodeReadChainBatch's own
	// Submit calls and for the same reason: this wait, like that
	// submission, must not be cut short by an unrelated per-attempt
	// cancellation.
	//nolint:contextcheck // see comment above
	ls.drainBlockPipelineBeforeRollback(
		context.Background(),
		"chainsync rollback",
	)
	// A database commit becomes visible before its AfterCommit callbacks run.
	// Exclude that window so blocksAboveSlot can never publish an Undo for the
	// new state before the matching Apply reaches the ordered lane.
	var rollbackEvents []event.Event
	err := func() error {
		ls.transactionEventMutex.Lock()
		defer ls.transactionEventMutex.Unlock()
		if err := ls.validateAndEmitRollbackUndo(point); err != nil {
			return err
		}
		evts, rbErr := ls.chain.RollbackDeferred(point)
		if rbErr != nil {
			return rbErr
		}
		rollbackEvents = evts
		return nil
	}()
	if err != nil {
		return err
	}
	// Publish the rollback's chain.update after chainsyncMutex is released.
	// Under the mutex an inline back-pressured publish deadlocks the drain.
	// RollbackDeferred has already enqueued these events on the chain's shared
	// sequencer under c.mutex, so they are ordered against concurrent
	// blockfetch adds in true chain-mutation order (rollback-before-add is no
	// longer merely a within-handler property); registering the chain drains
	// that sequencer once the mutex is released (a nil pubs drains
	// immediately on the unlocked path). See
	// chain.Chain.PublishPendingChainUpdates.
	if len(rollbackEvents) > 0 {
		pubs.drainChain(ls.chain)
	}
	if err := ls.rollback(point); err != nil {
		return fmt.Errorf("synchronize ledger rollback state: %w", err)
	}
	// A primary chain can be ahead of the applied ledger during genesis or
	// snapshot catch-up. In that case ls.rollback intentionally leaves the
	// ledger at its existing tip, so arming the audit at the primary-chain
	// rollback point would report every unapplied continuation as missing a
	// producer. Arm only when both sides actually reached the rollback point,
	// and discard any window left by an earlier rollback when they did not.
	if pointMatches(ls.Tip().Point, point) {
		ls.armContinuationAudit(point, "chainsync rollback")
	} else {
		ls.continuationAudit.Store(nil)
	}
	return nil
}

// processChainIteratorRollback applies a rollback emitted by the primary
// chain iterator. Iterator rollbacks can lag behind live blockfetch/
// chainsync activity: by the time this runs, the primary chain may have
// already re-extended past the rollback point (point). A stale point does
// NOT by itself mean ls.currentTip is fine -- it only means point is not
// the chain's current tip anymore. What actually determines whether
// ls.currentTip needs rolling back is whether ls.currentTip's own block is
// still part of the chain at all. Chain.Rollback (chain/chain.go,
// rollbackLocked) physically deletes an abandoned block's blob/metadata
// rows (database.BlockDeleteTxn, keyed by slot+hash+ID) the moment chain-
// selection decides against it -- independent of how far behind the
// ledger's own rollback/catch-up has gotten -- so a direct, uncached
// database.BlockByPoint(ls.db, ls.currentTip.Point) lookup reliably
// answers "is ls.currentTip still on the canonical chain" even when the
// chain has grown further since this rollback event was emitted. This
// must go straight to the database rather than through
// ls.chain.BlockByPoint: ChainManager.removeBlockByIndex deliberately
// re-inserts the removed block into its own in-memory blockCache
// ("in case other chains are using it", chain/manager.go) as part of
// removal, so a chain-level lookup finds a just-deleted block right after
// its removal and would defeat this check entirely.
//
//   - Found: ls.currentTip is still valid (an ancestor of, or equal to,
//     the current chain tip) -- this rollback event is moot, whatever
//     triggered it has already been superseded. Skip ls.rollback (calling
//     it here would wrongly discard still-valid, already-ledger-processed
//     blocks added after point). The read iterator's own cursor may still
//     need rewinding, though, so still request a pipeline restart.
//   - Not found: ls.currentTip's block was removed -- it was on the
//     abandoned fork. point is the fork/rollback point chain-selection
//     reported when it made that decision, so it's still the correct
//     ancestor to roll back to even though the chain has since grown
//     further from it.
//
// Getting this backwards previously caused two distinct failures: an
// earlier version of this fix rolled back unconditionally on any stale
// mismatch, which (per TestProcessChainIteratorRollbackSkipsStaleRollback)
// wrongly discards valid state whenever the chain has simply grown past
// point without ls.currentTip ever having diverged. The original code
// skipped the rollback unconditionally instead, which left ls.currentTip
// permanently stuck on a genuinely abandoned fork when one existed: every
// subsequent pipeline restart re-derived expectedPrevHash from that same
// un-rolled-back ls.currentTip, so ledgerProcessBlock's prev-hash check
// failed identically forever (errStaleChainIterator in a tight loop, no
// forward progress, eventually exhausting the node).
//
// Also drains ls.blockPipeline (drainBlockPipelineBeforeRollback) before
// either branch below runs. This is the reader iterator's own goroutine
// reporting its own rollback, so by construction ls.blockPipeline is
// already fully drained by the time this runs: decodeReadChainBatch
// submits and drains a whole batch synchronously before
// ledgerReadChainIterator ever emits a result, rollback or otherwise (see
// its doc comment), so nothing from *this* attempt is still in flight
// here. The call is kept anyway as a defensive invariant guard -- issue
// #1894 phase 5's actual cross-goroutine race is closed in
// rollbackChainAndStateDeferred instead, which is reached from chainsync handling
// on a different goroutine and has no equivalent synchronous guarantee.
func (ls *LedgerState) processChainIteratorRollback(
	ctx context.Context,
	point ocommon.Point,
) error {
	ls.drainBlockPipelineBeforeRollback(ctx, "chain iterator rollback")
	chainTip := ls.chain.Tip()
	stale := chainTip.Point.Slot != point.Slot ||
		!bytes.Equal(chainTip.Point.Hash, point.Hash)

	ls.RLock()
	currentTip := ls.currentTip
	ls.RUnlock()

	if stale {
		_, err := database.BlockByPoint(ls.db, currentTip.Point)
		switch {
		case err == nil:
			ls.config.Logger.Debug(
				"stale chain iterator rollback superseded, ledger tip still valid, restarting ledger pipeline",
				"component",
				"ledger",
				"rollback_slot",
				point.Slot,
				"rollback_hash",
				hex.EncodeToString(point.Hash),
				"chain_tip_slot",
				chainTip.Point.Slot,
				"chain_tip_hash",
				hex.EncodeToString(chainTip.Point.Hash),
			)
			return errRestartLedgerPipeline
		case errors.Is(err, models.ErrBlockNotFound):
			ls.config.Logger.Debug(
				"stale chain iterator rollback, ledger tip was on abandoned fork, rolling back and restarting ledger pipeline",
				"component",
				"ledger",
				"rollback_slot",
				point.Slot,
				"rollback_hash",
				hex.EncodeToString(point.Hash),
				"chain_tip_slot",
				chainTip.Point.Slot,
				"chain_tip_hash",
				hex.EncodeToString(chainTip.Point.Hash),
				"ledger_tip_slot",
				currentTip.Point.Slot,
				"ledger_tip_hash",
				hex.EncodeToString(currentTip.Point.Hash),
			)
			if err := ls.rollback(point); err != nil {
				return err
			}
			return errRestartLedgerPipeline
		default:
			return fmt.Errorf(
				"check ledger tip against current chain: %w", err,
			)
		}
	}

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
	return ls.transitionToEraFrom(
		txn,
		nextEraId,
		startEpoch,
		addedSlot,
		currentPParams,
		ls.currentEra.Id,
	)
}

// transitionToEraFrom is transitionToEra with an explicit source era. The
// source must be explicit when two adjacent hard forks are applied at one
// epoch boundary: LedgerState.currentEra is intentionally unchanged until
// the surrounding database transaction commits.
func (ls *LedgerState) transitionToEraFrom(
	txn *database.Txn,
	nextEraId uint,
	startEpoch uint64,
	addedSlot uint64,
	currentPParams lcommon.ProtocolParameters,
	fromEraId uint,
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
		result.InjectedSyntheticV2CostModel = injectedSyntheticV2CostModel(
			currentPParams,
			newPParams,
		)
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
		if result.InjectedSyntheticV2CostModel {
			// Persisted in the same transaction as the pparams write
			// above: writing it separately, after this transaction
			// commits, would leave a window where a crash after the
			// pparams commit but before the marker commit strands the
			// marker stale, exposing the wrong thing on restart.
			if err := ls.persistSyntheticV2CostModel(true, txn); err != nil {
				return nil, err
			}
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

// applyBoundaryEraTransitions applies the era transitions a multi-era boundary
// block requires but the epoch rollover does not perform itself, then takes the
// mark snapshot the rollover deferred.
//
// The rollover has to run first so pending protocol updates are enacted in the
// source era; applying the transitions before it would let an old-era update
// overwrite the successor era's parameters. The consequence is that when the
// rollover would normally capture the mark snapshot, the final era and protocol
// parameters do not exist yet. So the snapshot is captured here instead, after
// the transitions and after the new epoch's era and parameters are made durable,
// which is what makes the snapshot's recorded protocol major agree with the era
// the epoch actually runs at and with the post-commit EpochTransitionEvent.
//
// rolloverResult is updated in place to describe the final era. The returned
// transition results are in application order, for the caller's in-memory state
// and hard-fork events.
func (ls *LedgerState) applyBoundaryEraTransitions(
	txn *database.Txn,
	snapshotEpoch models.Epoch,
	transitionPath []uint,
	rolloverResult *EpochRolloverResult,
) ([]*EraTransitionResult, error) {
	workingPParams := rolloverResult.NewCurrentPParams
	workingEraId := rolloverResult.NewCurrentEra.Id
	transitionResults := make([]*EraTransitionResult, 0, len(transitionPath))
	for _, transitionEraID := range transitionPath {
		result, err := ls.transitionToEraFrom(
			txn,
			transitionEraID,
			snapshotEpoch.EpochId,
			snapshotEpoch.StartSlot+uint64(snapshotEpoch.LengthInSlots),
			workingPParams,
			workingEraId,
		)
		if err != nil {
			return nil, err
		}
		workingPParams = result.NewPParams
		workingEraId = result.NewEra.Id
		transitionResults = append(transitionResults, result)
	}

	newEpoch := rolloverResult.NewCurrentEpoch
	finalEra, ok := ls.eraById(workingEraId)
	if !ok || finalEra == nil {
		return nil, fmt.Errorf(
			"unknown transitioned era ID %d", workingEraId,
		)
	}
	slotLength, epochLength, err := finalEra.EpochLengthFunc(
		ls.config.CardanoNodeConfig,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve transitioned era timing: %w", err,
		)
	}
	newEpoch.EraId = workingEraId
	newEpoch.SlotLength = slotLength
	newEpoch.LengthInSlots = epochLength
	// calculateEpochNonce short-circuits to an all-nil nonce whenever the
	// ROLLOVER'S SOURCE era is Byron (no Praos nonce there), regardless of
	// what era this boundary actually transitions into. That is correct
	// when the new epoch stays in Byron, but here workingEraId has just
	// been advanced past Byron (that is the only way this function runs),
	// so the epoch needs a real nonce for header verification in its new
	// era. Seed it the same way every other from-genesis nonce path does
	// (computeEpochNonceForSlot, calculateEpochNonce's own no-prior-nonce
	// branch): the Shelley genesis hash for nonce/evolving/candidate, with
	// LastEpochBlockNonce left at NeutralNonce (nil) — see #2734. Without
	// this, header verification permanently rejects every block in the
	// new era with "epoch has no nonce for slot" for any Byron-prefixed
	// network that syncs from genesis instead of a Mithril snapshot.
	if workingEraId != 0 && len(newEpoch.Nonce) == 0 {
		if ls.config.CardanoNodeConfig == nil {
			return nil, errors.New(
				"seed post-Byron epoch nonce: CardanoNodeConfig is nil",
			)
		}
		if ls.config.CardanoNodeConfig.ShelleyGenesisHash == "" {
			return nil, errors.New(
				"seed post-Byron epoch nonce: could not get Shelley genesis hash",
			)
		}
		genesisHashBytes, err := hex.DecodeString(
			ls.config.CardanoNodeConfig.ShelleyGenesisHash,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"decode Shelley genesis hash for post-Byron epoch nonce: %w",
				err,
			)
		}
		if len(genesisHashBytes) != lcommon.Blake2b256Size {
			return nil, fmt.Errorf(
				"seed post-Byron epoch nonce: Shelley genesis hash is %d bytes, expected %d",
				len(genesisHashBytes),
				lcommon.Blake2b256Size,
			)
		}
		newEpoch.Nonce = genesisHashBytes
		newEpoch.EvolvingNonce = genesisHashBytes
		newEpoch.CandidateNonce = genesisHashBytes
		newEpoch.LastEpochBlockNonce = nil
	}
	if err := ls.db.SetEpoch(
		newEpoch.StartSlot,
		newEpoch.EpochId,
		newEpoch.Nonce,
		newEpoch.EvolvingNonce,
		newEpoch.CandidateNonce,
		newEpoch.LastEpochBlockNonce,
		newEpoch.EraId,
		newEpoch.SlotLength,
		newEpoch.LengthInSlots,
		txn,
	); err != nil {
		return nil, fmt.Errorf("update transitioned epoch: %w", err)
	}
	pparamsCbor, err := cbor.Encode(&workingPParams)
	if err != nil {
		return nil, fmt.Errorf(
			"encode transitioned protocol parameters: %w", err,
		)
	}
	if err := ls.db.SetPParams(
		pparamsCbor,
		newEpoch.StartSlot,
		newEpoch.EpochId,
		workingEraId,
		txn,
	); err != nil {
		return nil, fmt.Errorf(
			"persist transitioned protocol parameters: %w", err,
		)
	}
	rolloverResult.NewCurrentEpoch = newEpoch
	rolloverResult.NewCurrentPParams = workingPParams
	rolloverResult.NewCurrentEra = *finalEra
	rolloverResult.SchedulerIntervalMs = slotLength
	for i := range rolloverResult.NewEpochCache {
		if rolloverResult.NewEpochCache[i].EpochId == newEpoch.EpochId {
			rolloverResult.NewEpochCache[i].EraId = workingEraId
			rolloverResult.NewEpochCache[i].SlotLength = slotLength
			rolloverResult.NewEpochCache[i].LengthInSlots = epochLength
			rolloverResult.NewEpochCache[i].Nonce = newEpoch.Nonce
			rolloverResult.NewEpochCache[i].EvolvingNonce = newEpoch.EvolvingNonce
			rolloverResult.NewEpochCache[i].CandidateNonce = newEpoch.CandidateNonce
			rolloverResult.NewEpochCache[i].LastEpochBlockNonce = newEpoch.LastEpochBlockNonce
		}
	}

	if rolloverResult.BoundarySnapshotDeferred {
		if err := ls.captureEpochBoundarySnapshot(
			txn, snapshotEpoch, rolloverResult,
		); err != nil {
			return nil, err
		}
		rolloverResult.BoundarySnapshotDeferred = false
	}
	return transitionResults, nil
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
	// Only ever set, never cleared here: a later era's own transition
	// carrying the same CostModels map forward (e.g. Babbage->Conway) must
	// not be read as "this transition re-synthesized it," which would be
	// harmless today (still true) but wrong if a future era's HardForkFunc
	// stops re-detecting it. Clearing happens only where real data is
	// actually observed; see processEpochRollover's governance-enactment
	// handling.
	//
	// The durable marker itself is persisted earlier, transactionally,
	// inside transitionToEraFrom (same transaction as the pparams write);
	// this only updates the in-memory value that mirrors it, under the same
	// lock that guards ls.currentPParams.
	if result.InjectedSyntheticV2CostModel {
		ls.syntheticV2CostModel = true
	}
}

// resolveSyntheticV2CostModel computes the synthetic-marker value implied by
// the durable database marker's raw value (empty string if never written)
// and pp, the protocol parameters the marker describes. An empty value falls
// back to comparing pp's PlutusV2 cost model directly against the known
// synthetic default -- see loadSyntheticV2CostModel's doc comment for why.
// Shared by loadSyntheticV2CostModel (startup, and reloading the in-memory
// mirror after database.RecomputeSyntheticV2CostModelMarkerAfterTruncate
// resets the durable marker during a rollback) and rollback itself (which
// cannot safely call loadSyntheticV2CostModel directly there: this must be
// computed from the rolled-back pparams value before it is applied to
// ls.currentPParams under lock, matching every other piece of rollback's
// reloaded state).
func resolveSyntheticV2CostModel(
	value string,
	pp lcommon.ProtocolParameters,
) bool {
	if value == "" {
		v2, hasV2 := extractRawCostModels(pp)[1]
		return hasV2 && slices.Equal(v2, eras.DefaultPlutusV2CostModel)
	}
	return value == "true"
}

// loadSyntheticV2CostModel restores LedgerState.syntheticV2CostModel from the
// database at startup, so a restart does not silently reconstruct it as
// false (the zero value) regardless of the chain's real history -- see
// database.SyntheticV2CostModelSyncKey. Must run after loadPParams, which
// this otherwise has no ordering dependency on (the bootstrap fallback in
// resolveSyntheticV2CostModel reads ls.currentPParams).
//
// A node whose database predates this field (blinklabs-io/dingo#3825) reads
// an empty value here. Rather than defaulting to false ("not synthetic") --
// which would be wrong for a database that predates this field AND has
// never received a real PlutusV2 update (wolf31o2's PR review: this makes
// the fix inert for any already-running devnet or production node upgraded
// onto this build, since the marker can then only ever be set true again at
// a live era transition, which such a node will never perform again) --
// resolveSyntheticV2CostModel falls back to comparing the current PlutusV2
// cost model directly against the known synthetic default. This can misjudge
// real governance data that happens to re-affirm the exact default value as
// still-synthetic; that is the safe failure direction (suppressing a real
// value that would look identical to the fabricated one) given a database
// with no other provenance signal at all, and matches the heuristic
// injectedSyntheticV2CostModel already applies to detect the original
// fabrication.
func (ls *LedgerState) loadSyntheticV2CostModel() {
	value, err := ls.db.GetSyncState(database.SyntheticV2CostModelSyncKey, nil)
	if err != nil {
		ls.config.Logger.Warn(
			"failed to read synthetic PlutusV2 cost model marker from database",
			"component", "ledger",
			"error", err,
		)
		return
	}
	ls.syntheticV2CostModel = resolveSyntheticV2CostModel(
		value, ls.currentPParams,
	)
}

// persistSyntheticV2CostModel durably records LedgerState.syntheticV2CostModel
// so a restart reconstructs it correctly instead of defaulting to false; see
// database.SyntheticV2CostModelSyncKey. The caller supplies txn so this write
// commits atomically with the protocol-parameter write it describes -- a nil
// txn commits immediately as its own transaction, matching database.Txn's
// usual convention. Errors are propagated rather than logged-and-swallowed:
// when txn is a caller-managed transaction, a write failure here must abort
// that transaction along with the pparams write it accompanies, not silently
// leave the two inconsistent. See blinklabs-io/dingo#3825's PR review.
func (ls *LedgerState) persistSyntheticV2CostModel(
	value bool,
	txn *database.Txn,
) error {
	v := "false"
	if value {
		v = "true"
	}
	if err := ls.db.SetSyncState(
		database.SyntheticV2CostModelSyncKey, v, txn,
	); err != nil {
		return fmt.Errorf(
			"persist synthetic PlutusV2 cost model marker: %w", err,
		)
	}
	return nil
}

// markRealV2CostModelObserved durably records that real (non-synthetic)
// PlutusV2 cost-model data was confirmed written as of epoch, both the
// boolean marker (persistSyntheticV2CostModel) and the epoch it happened at
// (database.SetSyntheticV2CostModelClearedEpoch) -- the latter is what lets
// database.RecomputeSyntheticV2CostModelMarkerAfterTruncate tell whether a
// later rollback crosses back before this confirmation and so must undo it.
// Both writes share the caller's txn so they commit together with the
// pparams write they describe. See blinklabs-io/dingo#3825's PR review.
func (ls *LedgerState) markRealV2CostModelObserved(
	epoch uint64,
	txn *database.Txn,
) error {
	if err := ls.persistSyntheticV2CostModel(false, txn); err != nil {
		return err
	}
	// First-confirmation-wins: only write the cleared-epoch marker if none
	// is recorded yet. A chain can enact more than one real PlutusV2
	// cost-model update over its life; overwriting the marker on every
	// later one would make RecomputeSyntheticV2CostModelMarkerAfterTruncate
	// reset the marker to synthetic on a rollback that crosses back past
	// only the LATEST update but not an EARLIER one -- the earlier real
	// value still survives on the truncated chain and must not be reported
	// as synthetic. Keeping the earliest confirmed epoch is correct for
	// every subsequent comparison: "some real data was confirmed at or
	// before this epoch" only gets stronger as more updates land, never
	// weaker. See blinklabs-io/dingo#3825's PR review (Cubic).
	_, alreadyCleared, err := database.SyntheticV2CostModelClearedEpoch(
		ls.db, txn,
	)
	if err != nil {
		return fmt.Errorf(
			"read synthetic PlutusV2 cost model cleared-epoch marker: %w",
			err,
		)
	}
	if alreadyCleared {
		return nil
	}
	if err := database.SetSyntheticV2CostModelClearedEpoch(
		ls.db, txn, epoch,
	); err != nil {
		return err
	}
	return nil
}

// injectedSyntheticV2CostModel reports whether this specific era transition
// is the one that fabricated a PlutusV2 cost model rather than carrying one
// forward from a real source (genesis or an earlier real update): before had
// no key 1, after has key 1, and its value is exactly
// eras.DefaultPlutusV2CostModel. See LedgerState.syntheticV2CostModel.
func injectedSyntheticV2CostModel(
	before, after lcommon.ProtocolParameters,
) bool {
	afterModels := extractRawCostModels(after)
	afterV2, ok := afterModels[1]
	if !ok {
		return false
	}
	beforeModels := extractRawCostModels(before)
	if _, hadV2 := beforeModels[1]; hadV2 {
		return false
	}
	return slices.Equal(afterV2, eras.DefaultPlutusV2CostModel)
}

// IsAtTip reports whether the node has caught up to the chain tip at least
// once since boot. This is used to gate metrics that are only meaningful
// when processing live blocks (e.g., block delay CDF). Unlike
// validationEnabled (which starts true when ValidateHistorical is set),
// reachedTip only flips when the node actually reaches the stability window.
func (ls *LedgerState) IsAtTip() bool {
	return ls.reachedTip.Load()
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

// CurrentTransitionInfo returns the current TransitionInfo from the lock-free
// consensus snapshot.
func (ls *LedgerState) CurrentTransitionInfo() hardfork.TransitionInfo {
	return ls.loadConsensusSnapshot().transitionInfo
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

// SecurityParam returns the security parameter for the current era. It
// takes a brief read lock around ls.currentEra, which ledgerProcessBlocks
// mutates under the write lock during epoch rollover/era transitions —
// reading it unlocked here raced with those writes (caught by -race in
// TestLiveTruncateUnderRealForgingAndNetworking, which runs real forging
// concurrently with the stall recycler's periodic SecurityParam() calls).
func (ls *LedgerState) SecurityParam() int {
	return ls.securityParamForCurrentEraSnapshot()
}

func (ls *LedgerState) securityParamForEraOrDefault(eraId uint) int {
	if k, ok := ls.securityParamForEra(eraId); ok {
		return int(
			k,
		) // #nosec G115 -- k came from a non-negative int genesis field
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

// shouldSkipConfiguredPhase2Validation preserves the trusted-replay shortcut
// only when historical validation is disabled. When ValidateHistorical is
// enabled, local phase-2 evaluation is the purpose of that setting and must
// remain active across the stability boundary.
func shouldSkipConfiguredPhase2Validation(
	validationEnabled bool,
	shouldValidateBlock bool,
	deepHistoricalBlock bool,
) bool {
	return !validationEnabled && shouldValidateBlock && deepHistoricalBlock
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
	err           error
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
	reportErr := func(err error) {
		select {
		case resultCh <- readChainResult{err: err}:
		case <-ctx.Done():
		}
	}
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
				reportErr(fmt.Errorf("create chain iterator from %v: %w", startPoint, err))
				return
			}
			if reconcileRetries >= maxReconcileRetries {
				ls.config.Logger.Error(
					"exhausted ledger rollback retries for missing chain iterator start point",
					"error",
					err,
					"start_slot",
					startPoint.Slot,
					"start_hash",
					hex.EncodeToString(startPoint.Hash),
					"retries",
					reconcileRetries,
					"max_retries",
					maxReconcileRetries,
				)
				reportErr(fmt.Errorf("exhausted ledger rollback retries from %v: %w", startPoint, err))
				return
			}
			ls.config.Logger.Warn(
				"chain iterator start point not on chain, attempting ledger rollback",
				"error",
				err,
				"start_slot",
				startPoint.Slot,
				"start_hash",
				hex.EncodeToString(startPoint.Hash),
			)
			if reconcileErr := ls.reconcilePrimaryChainTipWithLedgerTip(); reconcileErr != nil {
				ls.config.Logger.Error(
					"failed to recover missing chain iterator start point",
					"error", reconcileErr,
					"start_slot", startPoint.Slot,
					"start_hash", hex.EncodeToString(startPoint.Hash),
				)
				reportErr(fmt.Errorf("recover missing chain iterator start point: %w", reconcileErr))
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
					"start_slot",
					startPoint.Slot,
					"start_hash",
					hex.EncodeToString(startPoint.Hash),
				)
				reportErr(fmt.Errorf("ledger rollback did not change missing chain iterator start point: %v", startPoint))
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
	// Read raw blocks from the chain iterator, then decode them (optionally
	// via the block-decode pipeline for parallelism -- see blockPipeline's
	// doc comment on the LedgerState struct).
	var next, cachedNext *chain.ChainIteratorResult
	var err error
	var shouldBlock bool
	var result readChainResult
	reportErr := func(err error) {
		select {
		case resultCh <- readChainResult{err: err}:
		case <-ctx.Done():
		}
	}
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// Keep only one database transaction batch's worth of raw blocks
		// buffered ahead of decode at a time (see batchSize). Dijkstra
		// bodies retain several canonical-CBOR views while they are live,
		// so the old arbitrary 500-block read-ahead could amplify a slow
		// metadata commit into gigabytes of otherwise reclaimable heap.
		// batchSize is also comfortably under the block-decode pipeline's
		// default prefetch buffer (1000), so submitting a full batch below
		// never blocks mid-batch.
		rawBatch := make([]models.Block, 0, batchSize)
		var rollbackNext *chain.ChainIteratorResult
		// gatherLockHeld tracks whether blockPipelineGatherMutex's read
		// lock is currently held for this batch-gather pass -- see that
		// field's doc comment on the LedgerState struct. It is acquired
		// lazily, immediately before any iter.Next() call that will not
		// block waiting for new chain data, and immediately after any
		// call that does block once it returns real data, so a rollback's
		// write lock is never made to wait on a call that is blocked
		// purely because the chain has not grown yet -- that call holds
		// nothing of this iteration's raw batch to protect. Once held, it
		// stays held through decodeReadChainBatch so nothing gathered
		// under it can be invalidated by a concurrent rollback before it
		// is submitted.
		gatherLockHeld := false
		releaseGatherLock := func() {
			if gatherLockHeld {
				ls.blockPipelineGatherMutex.RUnlock()
				gatherLockHeld = false
			}
		}
		// Gather up next batch of raw blocks
		for {
			// Check cancellation on every iteration, not just once per
			// outer batch: without this, a restart racing this goroutine
			// (see ledgerProcessBlocks's doc comment on why it waits for
			// this goroutine to fully exit) could keep gathering and
			// submitting up to a full extra batch to the shared
			// blockPipeline after cancellation before ever noticing.
			select {
			case <-ctx.Done():
				releaseGatherLock()
				return
			default:
			}
			if cachedNext != nil {
				next = cachedNext
				cachedNext = nil
			} else {
				blockingCall := shouldBlock
				if !blockingCall && !gatherLockHeld {
					ls.blockPipelineGatherMutex.RLock()
					gatherLockHeld = true
				}
				next, err = iter.Next(shouldBlock)
				shouldBlock = false
				if err != nil {
					if !errors.Is(err, chain.ErrIteratorChainTip) {
						ls.config.Logger.Error(
							"failed to get next block from chain iterator",
							"error", err,
						)
						releaseGatherLock()
						reportErr(fmt.Errorf("get next block from chain iterator: %w", err))
						return
					}
					shouldBlock = true
					// Break out of inner loop to flush DB transaction and log
					break
				}
				if blockingCall && !gatherLockHeld {
					// The blocking call above returned real data (a
					// block or a rollback marker), so the iterator was
					// woken rather than still waiting -- safe to take
					// the lock now, before this pass's gathered data
					// (if any) is used or extended further.
					ls.blockPipelineGatherMutex.RLock()
					gatherLockHeld = true
				}
			}
			if next == nil {
				ls.config.Logger.Error("next block from chain iterator is nil")
				releaseGatherLock()
				reportErr(errors.New("chain iterator returned nil block"))
				return
			}
			if next.Rollback {
				rollbackNext = next
				break
			}
			// Add the raw block to the batch; decoding happens once
			// gathering for this pass finishes (see decodeReadChainBatch),
			// potentially in parallel across the whole batch.
			rawBatch = append(rawBatch, next.Block)
			// Don't exceed our pre-allocated capacity
			if len(rawBatch) == cap(rawBatch) {
				break
			}
		}
		nextBatch, decodeErr := ls.decodeReadChainBatchWithError(ctx, rawBatch)
		releaseGatherLock()
		if decodeErr != nil {
			result = readChainResult{
				err:  decodeErr,
				done: make(chan struct{}),
			}
			select {
			case resultCh <- result:
			case <-ctx.Done():
				return
			}
			select {
			case <-result.done:
			case <-ctx.Done():
			}
			return
		}
		if rollbackNext != nil {
			trimmedBatch, emitRollback := trimReadBatchForRollback(
				nextBatch,
				rollbackNext.Point,
			)
			if len(trimmedBatch) > 0 {
				nextBatch = trimmedBatch
				if emitRollback {
					cachedNext = rollbackNext
				}
			} else {
				result = readChainResult{
					rollback:      true,
					rollbackPoint: rollbackNext.Point,
					done:          make(chan struct{}),
				}
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

// drainBlockPipelineErrors continuously reads ls.blockPipeline.Errors() for
// the pipeline's full lifetime, from just after Start() until the channel
// closes (inside Stop(), once every stage has fully drained -- see
// BlockPipeline.Stop). This is required for correctness, not just
// observability: gouroboros' StageWorkerPool.worker pushes every non-nil
// stage error onto errorsChan *before* forwarding the item onward to
// Results(), unconditionally and regardless of whether the error is
// "expected" for this item (e.g. blockPipelineEta0Provider can fail while
// cached epoch nonce state is unavailable, which decodeReadChainBatch
// deliberately defers when it later reads the same item back from Results()
// -- but by then the error has already been pushed here). errorsChan's
// capacity is bounded
// (PipelineConfig.PrefetchBufferSize, 1000 by default) with nothing else in
// this codebase ever reading from it; during a long interval without cached
// nonce state it fills permanently after roughly that many blocks, and every
// validate worker then blocks forever on `errors <- err` inside worker(),
// cascading backpressure through validatedChan/decodedChan/submitChan into
// ls.blockPipeline.Submit() -- which decodeReadChainBatch calls with
// context.Background() deliberately, so it also blocks forever with no log
// line, no error, and no timeout. This goroutine is what prevents that:
// as long as it is running for the pipeline's entire lifetime, errorsChan
// never fills regardless of how many expected-to-fail items flow through
// the validate stage.
func (ls *LedgerState) drainBlockPipelineErrors() {
	defer close(ls.blockPipelineErrorsDone)
	for err := range ls.blockPipeline.Errors() {
		ls.recordBlockPipelineError(err)
	}
}

// recordBlockPipelineError classifies and reports a single error read off
// ls.blockPipeline.Errors(). errorsChan carries a bare error with no item or
// era context (see drainBlockPipelineErrors), so classification here keys on
// error identity rather than era -- decodeReadChainBatch is the path with
// access to the decoded block, and it independently gates enforcement on
// block.Era().Id, so this function's classification only affects operator
// visibility, never whether a block is accepted.
//
// Two cases are expected/transient and logged at debug level under their
// own counters so a full sync does not spam the logs at error level:
//   - errBlockPipelineEta0Unavailable: the cached epoch entry has no Praos
//     nonce. This is expected for Byron and can be transient for later eras;
//     this function cannot inspect the item's era, so the log remains neutral.
//   - errHeaderVerificationDeferred: the pipeline's epoch cache has not yet
//     caught up with a block already committed to ls.chain. ARCHITECTURE.md
//     ("Block Processing Pipeline") documents this as a transient race that
//     resolves once the epoch cache catches up, so it is not lumped in with
//     genuine decode/validate/apply problems below.
//
// Anything else reaching errorsChan indicates a genuine decode/validate/apply
// problem the pipeline itself could not report any other way
// (decodeReadChainBatch separately logs the same decode/validation errors
// when it reads the corresponding item back from Results(), but that only
// covers items that make it that far -- this is the only path that also
// covers, e.g., apply-stage invariant violations such as
// pipeline.ErrBlockNotValidated) and is logged at error level plus its own
// counter for operator visibility.
func (ls *LedgerState) recordBlockPipelineError(err error) {
	if err == nil {
		return
	}
	switch {
	case errors.Is(err, errBlockPipelineEta0Unavailable):
		ls.metrics.incBlockPipelineExpectedEta0Error()
		ls.config.Logger.Debug(
			"block-processing pipeline: cached epoch has no Praos nonce for validate stage",
			"error",
			err,
		)
	case errors.Is(err, errHeaderVerificationDeferred):
		ls.metrics.incBlockPipelineDeferredEpochCacheError()
		ls.config.Logger.Debug(
			"block-processing pipeline: epoch cache does not yet cover validate-stage slot (expected transient, resolves once the epoch cache catches up)",
			"error",
			err,
		)
	default:
		ls.metrics.incBlockPipelineUnexpectedError()
		ls.config.Logger.Error(
			"block-processing pipeline reported an unexpected error",
			"error", err,
		)
	}
}

// decodeReadChainBatch decodes a batch of raw blocks gathered from the chain
// iterator into ledger.Block values, preserving input order. When
// ls.blockPipeline is configured (LedgerStateConfig.BlockPipelineEnabled) it
// submits the whole batch to the pipeline's decode worker pool up front, so
// multiple blocks decode concurrently, then drains the results back in
// submission order (the pipeline's apply stage guarantees this ordering
// regardless of which worker finishes first). Otherwise it decodes serially,
// exactly as ledgerReadChainIterator did before the pipeline existed.
//
// A decode failure or an enforced validation failure anywhere in the batch
// discards the whole batch. The preserved error lets a persisted-block
// validation failure be recovered with its exact block point.
//
// Once a pipeline submission has started, it always runs to completion --
// submit-and-drain is all-or-nothing, using a background context for the
// Submit calls and the results drain regardless of ctx. ctx is only checked
// up front, before anything has been submitted. blockPipeline is a single
// instance shared across every ledgerProcessBlocks retry attempt (see that
// method's doc comment): its apply stage reorders decoded results by a
// single global sequence number with no notion of "whose submission is
// whose", so a submission that aborted partway through ctx cancellation
// would leave already-sequenced, already-submitted items in that shared
// state for a *later* attempt's own call here to mistakenly drain --
// misattributing decoded blocks across a restart. Bailing out only before
// the first Submit call is safe because nothing has been submitted yet;
// once started, only genuine pipeline shutdown (Stop(), which closes the
// Results channel) can still interrupt the drain.
// decodeReadChainBatchWithError is the error-preserving form used by the
// ledger reader. Keeping the cause lets a validation failure on an already
// persisted block enter header-validation recovery instead of silently
// closing the reader and restarting on the same block forever.
func (ls *LedgerState) decodeReadChainBatchWithError(
	ctx context.Context,
	rawBatch []models.Block,
) (decoded []ledger.Block, retErr error) {
	if len(rawBatch) == 0 {
		return nil, nil
	}
	if ls.blockPipeline == nil {
		decoded = make([]ledger.Block, 0, len(rawBatch))
		for _, raw := range rawBatch {
			block, err := raw.Decode()
			if err != nil {
				ls.config.Logger.Error(
					"failed to decode block",
					"error", err,
				)
				return nil, fmt.Errorf(
					"decode block at slot %d: %w",
					raw.Slot,
					err,
				)
			}
			decoded = append(decoded, block)
		}
		return decoded, nil
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	// Refresh the block-pipeline gauges on every exit from here on,
	// success or failure, so a batch that aborts partway through (decode or
	// validation error) still reports the errors it accumulated.
	defer func() {
		ls.metrics.updateBlockPipelineStats(ls.blockPipeline.Stats())
	}()
	// submitCtx is deliberately not ctx (attemptCtx) -- see the doc comment
	// above.
	submitCtx := context.Background()
	for _, raw := range rawBatch {
		tip := ocommon.Tip{
			Point:       ocommon.NewPoint(raw.Slot, raw.Hash),
			BlockNumber: raw.Number,
		}
		//nolint:contextcheck // deliberately not derived from ctx; see the
		// doc comment above -- a submission must run to completion once
		// started, not abort partway through a mere per-attempt cancel.
		if err := ls.blockPipeline.Submit(submitCtx, raw.Type, raw.Cbor, tip); err != nil {
			ls.config.Logger.Error(
				"failed to submit block to decode pipeline",
				"error", err,
			)
			return nil, fmt.Errorf("submit block to decode pipeline: %w", err)
		}
	}
	results := ls.blockPipeline.Results()
	decoded = make([]ledger.Block, 0, len(rawBatch))
	// retErr remembers a decode or validation failure anywhere in the batch.
	// Once set, every remaining iteration below still reads its
	// expected Results() entry (so this call fully drains what it
	// Submitted) but discards it rather than appending to decoded or
	// returning early. Returning as soon as the first failure is seen
	// would leave the rest of this batch's entries unread on the shared
	// results channel for the *next* retry attempt's own
	// decodeReadChainBatch call to mistakenly pull off and mismatch
	// against its own, unrelated submissions -- blockPipeline has no
	// notion of "whose submission is whose" beyond result order (see this
	// function's doc comment on why a submission always runs to
	// completion).
	for range rawBatch {
		item, chOk := <-results
		if !chOk {
			ls.config.Logger.Error(
				"decode pipeline results channel closed unexpectedly",
			)
			return nil, errors.New(
				"decode pipeline results channel closed unexpectedly",
			)
		}
		if retErr != nil {
			continue
		}
		if decodeErr := item.DecodeError(); decodeErr != nil {
			ls.config.Logger.Error(
				"failed to decode block",
				"error", decodeErr,
			)
			retErr = fmt.Errorf("decode block: %w", decodeErr)
			continue
		}
		block := item.Block()
		// Byron-era blocks have no VRF/KES fields and no Praos epoch nonce
		// to validate against (blockPipelineEta0Provider always fails the
		// nonce lookup for them), matching the serial path's
		// verifyBlockHeaderHex, which explicitly skips Byron blocks rather
		// than trying to validate them. Ignore any validation outcome for
		// them here for the same reason.
		if ls.config.BlockPipelineValidateEnabled &&
			block.Era().Id != byron.EraIdByron &&
			ls.shouldEnforceBlockPipelineCrypto(block.SlotNumber()) {
			if valErr := item.ValidationError(); valErr != nil {
				// The validation-state snapshot can change between the gate
				// above and the provider's lookup. A missing/deferred nonce is
				// not evidence that the block is invalid; admission or the
				// deferred apply-time check remains authoritative.
				if errors.Is(valErr, errBlockPipelineEta0Unavailable) ||
					errors.Is(valErr, errHeaderVerificationDeferred) {
					decoded = append(decoded, block)
					continue
				}
				ls.config.Logger.Error(
					"failed to validate block",
					"error", valErr,
					"slot", block.SlotNumber(),
				)
				retErr = &headerValidationError{
					BlockPoint: ocommon.NewPoint(
						block.SlotNumber(),
						block.Hash().Bytes(),
					),
					Cause: fmt.Errorf(
						"block pipeline VRF/KES validation: %w",
						valErr,
					),
				}
				continue
			}
			if !item.IsValid() {
				ls.config.Logger.Error(
					"block failed pipeline VRF/KES validation",
					"slot", block.SlotNumber(),
				)
				retErr = &headerValidationError{
					BlockPoint: ocommon.NewPoint(
						block.SlotNumber(),
						block.Hash().Bytes(),
					),
					Cause: errors.New(
						"block failed pipeline VRF/KES validation",
					),
				}
				continue
			}
			if opCertErr := verifyOpCertHeaderCrypto(
				block.Header(),
				block.SlotNumber(),
				ls.SlotsPerKESPeriod(),
				ls.maxKESEvolutions(),
			); opCertErr != nil {
				ls.config.Logger.Error(
					"failed to validate block operational certificate",
					"error", opCertErr,
					"slot", block.SlotNumber(),
				)
				retErr = &headerValidationError{
					BlockPoint: ocommon.NewPoint(
						block.SlotNumber(),
						block.Hash().Bytes(),
					),
					Cause: fmt.Errorf(
						"block pipeline operational certificate validation: %w",
						opCertErr,
					),
				}
				continue
			}
		}
		decoded = append(decoded, block)
	}
	if retErr != nil {
		return nil, retErr
	}
	return decoded, nil
}

// noProgressBackoffBase/Max bound the delay applied when consecutive
// pipeline restarts land on the exact same (slot, hash) tip with no
// forward progress between them. Without this, a genuinely unrecoverable
// error (one that a bare pipeline restart can never fix on its own, e.g.
// two blocks legitimately racing for the same slot) spins the loop below
// at whatever speed a restart+immediate-refail cycle completes -- observed
// in practice at roughly one attempt every ~40ms, pegging a CPU core
// indefinitely with no possibility of ever making progress on its own.
// This is a backstop, not a fix for any specific error: it does not change
// whether the condition resolves, only how fast the pipeline hammers
// against it while it doesn't.
const (
	noProgressBackoffBase = 10 * time.Millisecond
	noProgressBackoffMax  = 2 * time.Second
	// noProgressStuckThreshold is the number of consecutive no-progress
	// restarts after which the failure is treated as deterministic rather
	// than transient. A transient cause clears within a few restarts; a
	// deterministic one -- a canonical block this node rejects and will
	// reject identically on every replay -- never does, and capping its
	// retry at noProgressBackoffMax means hammering it at that rate for as
	// long as the process runs.
	noProgressStuckThreshold = 50
	// noProgressStuckBackoffMax bounds the wait once stuck. The pipeline
	// keeps retrying, because the condition can still be cleared from
	// outside (a peer serving a different chain, an operator repairing
	// state), but at a rate that neither burns CPU nor buries the logs.
	noProgressStuckBackoffMax = 30 * time.Second
	// noProgressStuckReannounceInterval is how many further no-progress
	// restarts pass between ERROR announcements once the pipeline is stuck.
	// Announcing the transition once and then dropping to WARN every 100
	// restarts made a node that had stopped following the chain look quiet
	// to log-level alerting: one ERROR line covered 18 hours, mixed into
	// 129k WARN lines from everything else (issue #3261). At the stuck
	// backoff ceiling this re-announces roughly every ten minutes, which is
	// often enough to alert on and rare enough not to become the noise it
	// replaces.
	noProgressStuckReannounceInterval = 20
)

// pipelineStuckShouldAnnounce reports whether a stuck no-progress restart
// warrants an ERROR announcement. True on the transition into stuck and every
// noProgressStuckReannounceInterval restarts after it, so the condition stays
// visible for as long as it lasts rather than only when it began.
func pipelineStuckShouldAnnounce(consecutiveNoProgress int) bool {
	if consecutiveNoProgress < noProgressStuckThreshold {
		return false
	}
	return (consecutiveNoProgress-noProgressStuckThreshold)%
		noProgressStuckReannounceInterval == 0
}

// runLedgerReadChainAttempt runs one read+process attempt: it launches
// readChain on a fresh child context of ctx, hands the result channel to
// processFromSource, and -- regardless of how processFromSource finishes --
// cancels that child context and waits for readChain to have fully returned
// before returning processFromSource's error to the caller.
//
// Every attempt's readChain goroutine is the only submitter to the shared,
// whole-LedgerState-lifetime blockPipeline (see its doc comment on the
// struct). A caller that loops on this method to retry (ledgerProcessBlocks)
// MUST NOT start a new attempt until the previous call has returned: the
// pipeline's apply stage reorders decoded results strictly by a single global
// sequence counter, with no notion of "whose submission is whose", so two
// attempts' readChain goroutines submitting concurrently can and do get each
// other's decoded blocks back from Results() (misattributing blocks across a
// restart -- verified by a targeted regression test; see
// TestLedgerProcessBlocksRetryDoesNotMixBlocksAcrossAttempts). This method's
// wait for readChainDone is what gives callers that guarantee: it does not
// return until the retiring attempt's goroutine can no longer touch the
// pipeline, so the next call's own goroutine (and its Submit calls) never
// overlaps with it.
//
// This matters because a restart racing a rollback (the errStaleChainIterator
// path in tryRecoverFromTxValidationError, which wakes the old reader
// goroutine via completeReadResult() before this method's cancel() even
// runs) could otherwise let the old goroutine's gather loop keep pulling and
// submitting blocks to the pipeline concurrently with the new attempt's own
// submissions. Waiting for readChainDone closes that window: cancel() (which
// cascades to the chain iterator's own context, promptly unblocking any
// in-flight blocking Next() call -- see chain.ChainIterator's cancel
// watcher) is guaranteed to have taken full effect on the retiring goroutine
// before the next attempt's goroutine -- and its own Submit calls -- ever
// starts.
//
// This is generic across every recoverable error class that triggers a
// restart -- tx-validation recovery, header-validation recovery, and a
// stale chain iterator all funnel through the same
// ledgerProcessBlocksFromSource return path and the same completeReadResult
// call, so the wait applies uniformly regardless of which one fired.
func (ls *LedgerState) runLedgerReadChainAttempt(
	ctx context.Context,
	readChain func(context.Context, chan readChainResult),
	processFromSource func(context.Context, <-chan readChainResult) error,
) error {
	attemptCtx, cancel := context.WithCancel(ctx)
	readChainResultCh := make(chan readChainResult)
	readChainDone := make(chan struct{})
	go func() {
		defer close(readChainDone)
		readChain(attemptCtx, readChainResultCh)
	}()
	err := processFromSource(attemptCtx, readChainResultCh)
	cancel()
	// Block until the retiring attempt's reader goroutine has fully
	// exited -- see the doc comment above for why this must happen before
	// any later attempt's goroutine is started.
	<-readChainDone
	return err
}

// ledgerPipelineBackoff returns how long to wait before the next pipeline
// restart, and whether the pipeline should be treated as stuck on a
// deterministic failure.
//
// This changes only the retry rate and the operator signal, never whether a
// block is accepted: a node wedged on a rejected block is equally stuck
// before and after, but it now says so once, loudly, and stops spinning.
func ledgerPipelineBackoff(consecutiveNoProgress int) (time.Duration, bool) {
	if consecutiveNoProgress <= 0 {
		return 0, false
	}
	if consecutiveNoProgress < noProgressStuckThreshold {
		shift := min(consecutiveNoProgress-1, 8)
		return min(
			noProgressBackoffBase*(time.Duration(1)<<uint(shift)),
			noProgressBackoffMax,
		), false
	}
	shift := min(consecutiveNoProgress-noProgressStuckThreshold, 8)
	return min(
		noProgressBackoffMax*(time.Duration(1)<<uint(shift)),
		noProgressStuckBackoffMax,
	), true
}

func ledgerPipelineRetryDelay(
	consecutiveNoProgress int,
	minimum time.Duration,
) (time.Duration, bool) {
	backoff, stuck := ledgerPipelineBackoff(consecutiveNoProgress)
	return max(backoff, minimum), stuck
}

// certifiedEndorserBlockPipelineRetryDelay returns how long the pipeline waits
// before restarting after a certified Leios endorser block was unavailable.
//
// The gap escalates with the no-progress count. A flat one-second retry meant
// an endorser block that stays unavailable respun the chain reader, re-read the
// batch and re-decoded it once per second indefinitely -- spending the node on a
// fetch that is not getting anywhere -- and the ledger-side fetch is itself
// bounded and retried now, so a fast pipeline restart adds nothing (dingo
// #3552). The floor stays at certifiedEndorserBlockRetryDelay so the common
// case, where the endorser block lands moments later, still recovers promptly.
func certifiedEndorserBlockPipelineRetryDelay(
	consecutiveNoProgress int,
) time.Duration {
	delay, _ := ledgerPipelineRetryDelay(
		consecutiveNoProgress,
		certifiedEndorserBlockRetryDelay,
	)
	return delay
}

// pipelineProgress is the ledger pipeline's view of whether restarts are
// getting anywhere: how many consecutive ones have failed to move the tip,
// and the tip they last saw. Kept as one value because the fields are only
// meaningful together -- threading them separately is what let one failure
// path update some and not others.
type pipelineProgress struct {
	consecutiveNoProgress int
	haveLastTip           bool
	lastTipSlot           uint64
	lastTipHash           []byte
}

// stuck reports whether the pipeline has restarted without progress often
// enough that the failure should be treated as deterministic.
func (p pipelineProgress) stuck() bool {
	_, stuck := ledgerPipelineBackoff(p.consecutiveNoProgress)
	return stuck
}

// trackPipelineProgress folds one pipeline restart into the no-progress
// accounting. Shared by every failure path so none of them can quietly opt
// out of stuck detection: an error class that retries on its own schedule is
// still a failure to advance, and skipping the counter is what let a
// permanently unavailable endorser block wedge the pipeline without ever
// raising the signal.
//
// The counter resets as soon as the tip moves, so a pipeline that is making
// progress between restarts never accumulates toward stuck.
func (ls *LedgerState) trackPipelineProgress(
	p pipelineProgress,
) pipelineProgress {
	ls.RLock()
	tipSlot := ls.currentTip.Point.Slot
	tipHash := ls.currentTip.Point.Hash
	ls.RUnlock()
	if p.haveLastTip && tipSlot == p.lastTipSlot &&
		bytes.Equal(tipHash, p.lastTipHash) {
		p.consecutiveNoProgress++
	} else {
		p.consecutiveNoProgress = 0
	}
	p.haveLastTip = true
	p.lastTipSlot = tipSlot
	p.lastTipHash = tipHash
	return p
}

// ledgerProcessBlocks drives ledgerProcessBlocksFromSource against a fresh
// chain-reader goroutine on each attempt (via runLedgerReadChainAttempt),
// restarting whenever that attempt returns a recoverable error (see
// errRestartLedgerPipeline/errStaleChainIterator in replay_recovery.go and
// headerValidationError in header_validation_recovery.go), and tracking
// consecutive no-progress restarts (trackPipelineProgress/
// ledgerPipelineBackoff) to back off and surface a stuck-pipeline signal
// when a failure is deterministic rather than transient.
//
// errHaltLedgerPipeline is the one error class that is not retried. Recovery
// raises it once it has established that no local replay can change a block's
// verdict, at which point restarting would only rediscover the same block, so
// the loop announces the terminal condition and returns instead (issue #3261).
func (ls *LedgerState) ledgerProcessBlocks(ctx context.Context) {
	ls.ledgerProcessBlocksWithAttempt(
		ctx,
		func(attemptCtx context.Context) error {
			return ls.runLedgerReadChainAttempt(
				attemptCtx,
				ls.ledgerReadChain,
				ls.ledgerProcessBlocksFromSource,
			)
		},
	)
}

// ledgerProcessBlocksWithAttempt is ledgerProcessBlocks' restart loop with the
// per-attempt work injected, so the loop's own decisions -- back off, announce,
// or stop for good -- can be exercised without standing up a chain reader and a
// block pipeline behind them.
func (ls *LedgerState) ledgerProcessBlocksWithAttempt(
	ctx context.Context,
	attempt func(context.Context) error,
) {
	// Clear the no-progress gauges however this loop exits — normal return,
	// or a shutdown cancelling one of the retry timers. Deferred rather than
	// cleared at each return so a path added later cannot strand a stale
	// "stuck" reading in monitoring for the life of the process.
	// A halted pipeline is not a pipeline making no progress -- it is one
	// that has stopped -- so its own terminal gauge stands instead of a
	// zeroed no-progress reading that would look healthy.
	halted := false
	defer func() {
		if halted {
			return
		}
		ls.metrics.setPipelineNoProgress(0, false)
	}()
	var progress pipelineProgress
	for {
		err := attempt(ctx)
		if err == nil || ctx.Err() != nil {
			return
		}
		ls.handleLedgerProcessBlocksError(err)
		if errors.Is(err, errHaltLedgerPipeline) {
			// Recovery has established that no local replay can change
			// this verdict, so restarting the pipeline would only
			// rediscover the same block. Stop, and leave a terminal
			// signal behind: the announcement is the last log line this
			// pipeline writes, and the gauge is what an operator alerts
			// on afterwards.
			halted = true
			ls.metrics.setPipelineHalted()
			ls.RLock()
			haltTipSlot := ls.currentTip.Point.Slot
			ls.RUnlock()
			ls.config.Logger.Error(
				"ledger pipeline halted on an unrepairable validation failure; the node has stopped following the chain and will not resume without operator intervention",
				"component",
				"ledger",
				"tip_slot",
				haltTipSlot,
				"error",
				err,
			)
			return
		}
		if errors.Is(err, errCertifiedEndorserBlockUnavailable) {
			// This retry has its own delay and used to skip the no-progress
			// accounting entirely, so an endorser block that never becomes
			// available wedged the pipeline without ever raising the stuck
			// signal. Count it like any other failure to advance; the
			// bespoke delay below still governs its pacing.
			progress = ls.trackPipelineProgress(progress)
			endorserStuck := progress.stuck()
			ls.metrics.setPipelineNoProgress(
				progress.consecutiveNoProgress,
				endorserStuck,
			)
			if endorserStuck &&
				pipelineStuckShouldAnnounce(
					progress.consecutiveNoProgress,
				) {
				ls.config.Logger.Error(
					"ledger pipeline stuck: a certified endorser block has stayed unavailable across repeated restarts without advancing the tip; the node is no longer following the chain",
					"component",
					"ledger",
					"consecutive_no_progress",
					progress.consecutiveNoProgress,
					"tip_slot",
					progress.lastTipSlot,
					"error",
					err,
				)
			}
			timer := time.NewTimer(
				certifiedEndorserBlockPipelineRetryDelay(
					progress.consecutiveNoProgress,
				),
			)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}

		progress = ls.trackPipelineProgress(progress)
		tipSlot := progress.lastTipSlot

		backoff, stuck := ledgerPipelineBackoff(
			progress.consecutiveNoProgress,
		)
		ls.metrics.setPipelineNoProgress(
			progress.consecutiveNoProgress,
			stuck,
		)
		if progress.consecutiveNoProgress > 0 {
			// Announce the stuck condition at ERROR on the transition and
			// periodically for as long as it lasts: a deterministic failure
			// is not going to clear on its own, and a node that has stopped
			// following the chain must not fall silent at ERROR after one
			// line while the WARN below is buried among unrelated warnings
			// (issue #3261).
			if stuck &&
				pipelineStuckShouldAnnounce(
					progress.consecutiveNoProgress,
				) {
				ls.config.Logger.Error(
					"ledger pipeline stuck: repeated restarts are not advancing the tip, so the failure is deterministic and will not clear on its own; the node is no longer following the chain",
					"component",
					"ledger",
					"consecutive_no_progress",
					progress.consecutiveNoProgress,
					"tip_slot",
					tipSlot,
					"backoff",
					backoff,
					"error",
					err,
				)
			}
			if progress.consecutiveNoProgress == 10 ||
				progress.consecutiveNoProgress%100 == 0 {
				ls.config.Logger.Warn(
					"ledger pipeline making no progress across repeated restarts, backing off",
					"component",
					"ledger",
					"consecutive_no_progress",
					progress.consecutiveNoProgress,
					"stuck",
					stuck,
					"backoff",
					backoff,
					"tip_slot",
					tipSlot,
					"error",
					err,
				)
			}
			timer := time.NewTimer(backoff)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return
			}
		}
	}
}

func (ls *LedgerState) handleLedgerProcessBlocksError(err error) {
	if errors.Is(err, errRestartLedgerPipeline) {
		return
	}
	if errors.Is(err, errHaltLedgerPipeline) {
		ls.config.Logger.Warn(
			"block processing hit persistent validation failure, halting ledger pipeline",
			"error",
			err,
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
	bulkLoadAllowed := !ls.validationEnabled && !ls.reachedTip.Load()
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
	var allowTwoEraBoundaryTransition bool
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
			boundaryAllowsTwoEraTransition := allowTwoEraBoundaryTransition
			allowTwoEraBoundaryTransition = false

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
			//nolint:contextcheck // SubmitAsyncDBTxn has no context-aware variant.
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

				// A boundary block can be encoded in the era immediately
				// before the era announced by its header. Apply that pair of
				// consecutive transitions in one transaction, but reject any
				// larger jump because omitted per-epoch rules cannot be
				// reconstructed safely from one block.
				transitionPath, ok := ls.eraTransitionPath(
					snapshotEra.Id,
					nextEpochEraId,
					boundaryAllowsTwoEraTransition,
				)
				if !ok {
					return fmt.Errorf(
						"refusing era advancement from %d to %d: "+
							"two consecutive transitions require a "+
							"validated successor header at the boundary",
						snapshotEra.Id, nextEpochEraId,
					)
				}
				// Let the epoch rollover enact pending protocol updates in
				// the source era first. Applying a successor transition before
				// the rollover would decode an update using the successor's
				// shape; fields removed by that era (for example Alonzo's
				// decentralization field in Babbage) then fail to decode.
				transitionsBeforeRollover,
					transitionsAfterRollover := splitEraTransitionsForRollover(
					transitionPath,
				)
				for _, transitionEraID := range transitionsBeforeRollover {
					result, err := ls.transitionToEraFrom(
						txn,
						transitionEraID,
						snapshotEpoch.EpochId,
						snapshotEpoch.StartSlot+uint64(
							snapshotEpoch.LengthInSlots,
						),
						workingPParams,
						workingEraId,
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
					len(transitionsAfterRollover) > 0,
				)
				if err != nil {
					return err
				}
				rolloverResult = result
				if len(transitionsAfterRollover) > 0 {
					transitionResults, err := ls.applyBoundaryEraTransitions(
						txn,
						snapshotEpoch,
						transitionsAfterRollover,
						rolloverResult,
					)
					if err != nil {
						return err
					}
					// applyBoundaryEraTransitions updated
					// rolloverResult in place, so the era and
					// pparams the caller applies after commit
					// already describe the final era.
					eraTransitions = append(
						eraTransitions,
						transitionResults...,
					)
				}
				return nil
			}, true)
			if err != nil {
				// This runs on the pass after a boundary-crossing batch
				// deferred its remainder to cachedNextBatch, which (per the
				// cachedNextBatch != nil branch below) leaves
				// currentReadResultDone live rather than already closed.
				// Without this, a rollover failure here would return
				// without ever signalling the reader goroutine, which
				// would then block on <-result.done forever.
				completeReadResult()
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
				// The durable marker itself was already persisted
				// transactionally inside processEpochRollover; this only
				// updates the in-memory mirror under the same lock that
				// guards ls.currentPParams.
				if rolloverResult.RealV2CostModelObserved {
					ls.syntheticV2CostModel = false
				}
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
			ls.publishSnapshotsLocked()
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
						PreviousEpoch: snapshotEpoch.EpochId,
						NewEpoch:      newEpochId,
						BoundarySlot:  rolloverResult.NewCurrentEpoch.StartSlot,
						EpochNonce:    rolloverResult.NewCurrentEpoch.Nonce,
						ProtocolVersion: ls.protocolMajorForEvent(
							rolloverResult.NewCurrentPParams,
							rolloverResult.NewCurrentEra,
						),
						SnapshotSlot: snapshotSlot,
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
			// currentReadResultDone (do not reset it below) so the reader
			// goroutine is signalled only once cachedNextBatch is fully
			// drained, at the completeReadResult() guard at the bottom of
			// this loop.
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
				if result.err != nil {
					completeReadResult()
					recovered, recoverErr := ls.tryRecoverFromHeaderValidationError( //nolint:contextcheck
						result.err,
					)
					if recoverErr != nil {
						return fmt.Errorf(
							"recover read-chain header validation failure: %w",
							recoverErr,
						)
					}
					if recovered {
						return errRestartLedgerPipeline
					}
					return fmt.Errorf(
						"read-chain decode or validation: %w",
						result.err,
					)
				}
				nextBatch = result.blocks
				// Process rollback
				// Note: We do NOT hold ls.Lock() here because rollback() calls
				// SubmitAsyncDBTxn() which may trigger PartialCommitError recovery
				// that re-acquires ls.Lock(), causing a deadlock. The rollback
				// method handles its own locking for in-memory state updates.
				if result.rollback {
					if err = ls.processChainIteratorRollback(
						ctx,
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
		var parentEnvelope envelopeParent
		var parentEnvelopeSet bool
		for i = 0; i < len(nextBatch); i += batchSize {
			end = min(
				len(nextBatch),
				i+batchSize,
			)

			// Leios: gate delivery of this chunk on the availability of the
			// endorser blocks its Dijkstra ranking blocks reference, so the
			// endorser transactions are applied ahead of the ranking blocks
			// that endorse them. Runs outside the DB transaction opened below
			// and is a no-op for blocks without Leios references.
			if err := ls.ensureReferencedEndorserBlocks(
				ctx,
				nextBatch[i:end],
			); err != nil {
				completeReadResult()
				return fmt.Errorf(
					"ensure referenced Leios endorser blocks: %w",
					err,
				)
			}

			// Capture snapshots of state needed during transaction.
			// Acquire read lock to prevent race with RecoverCommitTimestampConflict
			// which can trigger rollback() and loadTip() that mutate these fields.
			ls.RLock()
			snapshotEpoch := ls.currentEpoch
			snapshotEra := ls.currentEra
			snapshotPParams := ls.currentPParams
			snapshotPrevEraPParams := ls.prevEraPParams
			snapshotTip := ls.currentTip
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
			if wallSlot, err := ls.CurrentSlot(); err == nil &&
				wallSlot > referenceSlot {
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
			trackByronPBFT := batchContainsByronBlocks(nextBatch[i:end])
			var runningByronPBFTState byronPBFTState
			if trackByronPBFT {
				runningByronPBFTState, err = ls.byronPBFTStateAtTip(
					ctx,
					snapshotTip,
				)
				if err != nil {
					completeReadResult()
					return fmt.Errorf(
						"reconstruct Byron PBFT state: %w",
						err,
					)
				}
			}
			var pendingByronPBFTState byronPBFTState
			var pendingByronPBFTTip ocommon.Point
			var pendingByronPBFT bool
			// Track expected previous hash for batch processing - updated after each block
			expectedPrevHash := snapshotTipHash
			if !parentEnvelopeSet {
				var parentBlockType uint
				var parentBlockTypeLoaded bool
				if len(snapshotTip.Point.Hash) > 0 {
					if storedBlock, err := database.BlockByPoint(
						ls.db,
						snapshotTip.Point,
					); err == nil {
						parentBlockType = storedBlock.Type
						parentBlockTypeLoaded = true
					} else {
						ls.config.Logger.Debug(
							"could not load persisted parent block type for envelope validation",
							"component", "ledger",
							"slot", snapshotTip.Point.Slot,
							"error", err,
						)
					}
				}
				parentEnvelope = envelopeParentFromTip(
					snapshotTip.Point.Slot,
					snapshotTip.BlockNumber,
					snapshotTip.Point.Hash,
					parentBlockType,
					parentBlockTypeLoaded,
				)
				parentEnvelopeSet = true
			}
			// Flag to enable validation after transaction commits (set inside callback,
			// applied after commit to avoid mutating in-memory state on txn failure)
			var wantEnableValidation bool

			// The transaction events this eventually publishes are
			// bounded by ls.publishCtx, not by this loop's ctx, on
			// purpose: that ctx is re-derived whenever the pipeline
			// restarts (errRestartLedgerPipeline), and abandoning a
			// publish on a restart would drop events subscribers
			// derive state from. Only Close cancels publishCtx.
			candidateTip := blockApplyCandidatePoint(
				nextBatch[i:end],
				snapshotEpoch,
			)
			err = ls.submitBlockApplyDBTxn(
				snapshotTip,
				candidateTip,
				func(txn *database.Txn) error { //nolint:contextcheck
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
							headerMajor, headerMajorKnown := HeaderProtocolMajor(
								next.Header(),
							)
							nextEpochEraId,
								allowTwoEraBoundaryTransition = ls.boundaryEraForBlock(
								snapshotEra.Id,
								uint(next.Era().Id),
								headerMajor,
								headerMajorKnown,
							)
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
						shouldValidateBlock, reachedTipRegion := historicalBlockValidationDecision(
							snapshotValidationEnabled,
							ls.config.TrustedReplay,
							snapshotChainsyncState,
							next.SlotNumber(),
							cutoffSlot,
							snapshotMithrilSlot,
						)
						if reachedTipRegion {
							wantEnableValidation = true
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
						if trackByronPBFT &&
							next.Era().Id == byron.EraIdByron {
							nextPBFTState, pbftErr := ls.advanceByronPBFTState(
								runningByronPBFTState,
								next,
								shouldValidateBlock,
							)
							if pbftErr != nil {
								deltaBatch.Release()
								return classifyByronPBFTApplyError(
									tmpPoint,
									pbftErr,
									shouldValidateBlock,
								)
							}
							runningByronPBFTState = nextPBFTState
							pendingByronPBFTState = nextPBFTState
							pendingByronPBFTTip = tmpPoint
							pendingByronPBFT = true
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
							parentEnvelope = envelopeParentFromBlock(next)
							blocksProcessed++
							continue
						}
						// Process block
						skipPhase2Validation := shouldSkipConfiguredPhase2Validation(
							snapshotValidationEnabled,
							shouldValidateBlock,
							ls.shouldSkipPhase2ValidationForBlockAtCurrentTip(
								next.BlockNumber(),
								snapshotEra.Id,
							),
						)
						delta, err = ls.ledgerProcessBlock(
							txn,
							tmpPoint,
							next,
							shouldValidateBlock,
							// wantEnableValidation is the same flag that stores
							// reachedTip after this batch commits; passing it here
							// guards the transition batch too (issue #3005 P1).
							wantEnableValidation,
							skipPhase2Validation,
							expectedPrevHash,
							parentEnvelope,
							blockOffsets,
							snapshotEra,
							snapshotPParams,
							snapshotPrevEraPParams,
							snapshotEpoch.EpochId,
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
						parentEnvelope = envelopeParentFromBlock(next)
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
							if ok && tmpEra != nil &&
								tmpEra.CalculateEtaVFunc != nil {
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
				},
			)
			if err != nil {
				// Undo events published down this recovery path are
				// bounded by ls.publishCtx, not this loop's ctx, for the
				// same reason as the apply path above.
				recovered, recoverErr := ls.tryRecoverFromTxValidationError( //nolint:contextcheck
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
				// A deferred header check that rejects an already-persisted
				// block is deterministic: restarting the pipeline re-reads
				// the same block and fails identically. Rewind past it so
				// chain selection can offer another candidate. The block is
				// still rejected.
				recovered, recoverErr = ls.tryRecoverFromHeaderValidationError( //nolint:contextcheck
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
						"component",
						"ledger",
						"error",
						err,
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
				if pendingByronPBFT {
					ls.byronPBFT.state = pendingByronPBFTState
					ls.byronPBFT.tip = pendingByronPBFTTip
					ls.byronPBFT.initialized = true
				}
				if len(pendingNonce) > 0 {
					ls.currentTipBlockNonce = pendingNonce
				}
				// Forward progress past a prior validation-recovery high-water
				// mark clears its non-convergence hold so a later, unrelated
				// failure gets a fresh recovery budget (issues #2939, #3005).
				ls.resetAtTipRecoveryDescent(pendingTip.Point.Slot)
				ls.resetReplayRecoveryNonProgress(pendingTip.Point.Slot)
				ls.resetDeterministicTxRecovery(pendingTip.Point.Slot)
				ls.resetMithrilBoundaryRejections(pendingTip.Point.Slot)
				ls.resetRecoveryRewindRejections(pendingTip.Point.Slot)
				ls.checkpointWrittenForEpoch = localCheckpointWritten
				if wantEnableValidation {
					ls.validationEnabled = true
					ls.reachedTip.Store(true)
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
				ls.publishSnapshotsLocked()
				ls.Unlock()
				ls.maybeQueueStakeRewardPrecomputeRetry(pendingTip.Point.Slot)
				// Restore normal DB options outside the lock after validation is enabled
				if wantEnableValidation && bulkLoadActive &&
					bulkOptimizer != nil {
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
		// An epoch/era boundary mid-batch defers the post-boundary remainder
		// to cachedNextBatch for the next outer-loop pass (see the
		// "cachedNextBatch != nil" branch above) instead of reading a fresh
		// result. Only signal the reader goroutine once that remainder is
		// nil too, so the signal represents the whole original result being
		// processed, not just the pre-boundary chunk of it.
		if ls.beforeReadResultDoneSignal != nil {
			ls.beforeReadResultDoneSignal()
		}
		if cachedNextBatch == nil {
			completeReadResult()
		}
	}
}

// strictConsumedInputsEnabled decides whether a validated block's delta apply
// must refuse to recover an absent consumed-input producer from the append-only
// blob store and error instead (issue #3005). It is enabled only for validated
// block application once the node is at tip, so from-genesis bootstrap and
// Mithril gap-closure — where absent producer rows are legitimately recovered —
// are unaffected.
//
// reachedTip alone is insufficient: it is stored true only after the transition
// batch (the first batch whose blocks cross the tip cutoff) has committed, so a
// guard keyed on reachedTip.Load() would leave that transition batch unguarded.
// reachesTip is the caller's per-block at-tip signal — the same
// wantEnableValidation flag that stores reachedTip once the batch commits — so
// the transition batch is guarded too. That flag is set only for a block at or
// past the tip cutoff while syncing, so it never fires during historical
// catch-up or on Mithril gap blocks.
func (ls *LedgerState) strictConsumedInputsEnabled(
	shouldValidate bool,
	reachesTip bool,
) bool {
	return shouldValidate && (ls.reachedTip.Load() || reachesTip)
}

// skipDijkstraTxValidation reports whether the per-transaction rule set is
// *skipped* for a transaction being validated under era eraId — true means
// ValidateTxFunc is not called for it.
//
// This is the ledger half of the Musashi prototype's accepted non-validating
// behaviour (see LedgerStateConfig.SkipDijkstraTxValidation), and its scope is
// deliberately narrow: the bypass applies to Dijkstra-era transactions only.
// Transactions validated under any earlier era — Conway and before — still run
// their full rule set even when the prototype flag is set, because the
// prototype's trust argument (endorser transactions are stored but never
// applied, so ranking-block transactions that spend endorser-resident outputs
// are unresolvable) exists only in Dijkstra/Leios. Header validation is
// likewise unaffected: KES, VRF proof, registered-VRF-key binding and opcert
// checks all still apply, and only the stake-derived leader threshold is
// downgraded, by the separate SkipLeaderStakeThresholdCheck flag.
//
// The same prototype flag also scopes trustDijkstraTxValidationError. Standard
// Dijkstra/Leios profiles run the rules and reject any disagreement.
func (ls *LedgerState) skipDijkstraTxValidation(eraId uint) bool {
	return eraId == dijkstra.EraIdDijkstra &&
		ls.config.SkipDijkstraTxValidation
}

// trustDijkstraTxValidationError reports whether a per-transaction validation
// failure under era eraId is logged and trusted instead of rejecting the block.
//
// Trust is limited to the explicit Musashi prototype bypass. Standard
// Dijkstra/Leios profiles have complete endorser-block fetch/apply support and
// must reject invalid ranking-block transactions just like every other era.
func (ls *LedgerState) trustDijkstraTxValidationError(eraId uint) bool {
	return eraId == dijkstra.EraIdDijkstra &&
		ls.config.SkipDijkstraTxValidation
}

// dijkstraEraGate uses the pparams-derived active era. A Musashi block may
// decode through the Conway wire type while carrying a Dijkstra header; block
// and header era values are not authoritative for ledger-era gates.
func dijkstraEraGate(currentEra eras.EraDesc) bool {
	return currentEra.Id == dijkstra.EraIdDijkstra
}

func (ls *LedgerState) ledgerProcessBlock(
	txn *database.Txn,
	point ocommon.Point,
	block ledger.Block,
	shouldValidate bool,
	reachesTip bool,
	skipPhase2Validation bool,
	expectedPrevHash []byte,
	parent envelopeParent,
	offsets *database.BlockIngestionResult,
	currentEra eras.EraDesc,
	pparams lcommon.ProtocolParameters,
	prevEraPParams lcommon.ProtocolParameters,
	committeeEpoch uint64,
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
		if err := validateInboundBlockEnvelope(
			block,
			pparams,
			parent,
		); err != nil {
			return nil, err
		}
		if err := ls.validateBlockHeaderProtocolVersion(
			block.Header(), pparams,
		); err != nil {
			return nil, err
		}
	}
	// Validate the operational certificate counter before processing the
	// block's transactions, so a stale or gapped opcert is rejected by this
	// cheap stateful check rather than after full transaction and Plutus
	// validation. The cold-key signature and KES-period expiry were already
	// checked at header verification.
	opCert, hasOpCert := opCertFromHeader(block.Header())
	var opCertPoolKeyHash lcommon.PoolKeyHash
	var opCertIssueNumber uint64
	if hasOpCert {
		if opCert == nil {
			return nil, errors.New(
				"block header reported an operational certificate but returned nil",
			)
		}
		opCertIssueNumber = opCert.IssueNumber
		opCertPoolKeyHash = lcommon.PoolKeyHash(block.IssuerVkey().Hash())
		// The counter is recorded for every applied block, validated or not,
		// so the bound on what the metadata store can record is checked here
		// rather than beside the era rule below: an unvalidated replay would
		// otherwise reach UpdatePoolOpCertSequence with a counter it cannot
		// write and fail after the block's transactions had been processed,
		// with the width limit named nowhere.
		if err := eras.ValidateOpCertPersistableCounter(
			opCertIssueNumber,
		); err != nil {
			return nil, fmt.Errorf("pool %x: %w", opCertPoolKeyHash, err)
		}
		// Counter monotonicity is the stateful half of inbound opcert
		// validation: read the pool's last-seen counter before processing this
		// block, inside the same validation transaction. A backward counter
		// (below the last seen) signals a stale or stolen hot key and is
		// rejected in every era. A gapped counter (more than one past the last
		// seen) is the Praos over-increment case and is rejected only for Praos
		// eras (Babbage onward); TPraos eras (Shelley–Alonzo) enforce only
		// monotonicity, so the gap rule is scoped by era rather than by
		// validation mode (shouldValidate can be true for historical or
		// near-tip TPraos blocks). The counter is recorded once the block's
		// transactions are processed (below); rollback safety is inherited from
		// the per-(pool,slot) PoolOpCertSequence store, which drops rows past
		// the rollback slot and recomputes the latest counter.
		if shouldValidate {
			stored, found, err := ls.latestOpCertCounterForValidation(
				opCertPoolKeyHash,
				txn,
			)
			if err != nil {
				return nil, fmt.Errorf(
					"read opcert counter for pool %x: %w",
					opCertPoolKeyHash,
					err,
				)
			}
			if err := validateOpCertCounter(
				stored,
				found,
				opCertIssueNumber,
				opCertNoGapRuleApplies(block.Era().Id),
			); err != nil {
				return nil, fmt.Errorf("pool %x: %w", opCertPoolKeyHash, err)
			}
		}
	}
	var blockDonation uint64
	// Apply the relevant Leios endorser block's transactions before the ranking
	// block's own. On the forward/CIP path this is the current block's announced
	// EB. On the Musashi prototype-2026w29 path it is the certified EB announced
	// by the parent; a CertRB may simultaneously announce a different, new EB.
	// Decode/build failures remain best-effort on the forward/CIP path. On the
	// Musashi certificate-driven path every certified closure is mandatory, so
	// resolution, availability, decode, and apply failures abort the block.
	// Storage-phase failures always abort the DB transaction so a partial
	// endorser-block application cannot be committed.
	if dijkstraEraGate(currentEra) {
		if ls.config.EndorserBlockProvider == nil {
			if certifier, ok := block.Header().(leiosEndorserBlockCertifier); ok {
				if certified, present := certifier.LeiosCertified(); present &&
					certified &&
					!ls.config.LeiosApplyEndorserBlockTxs {
					return nil, fmt.Errorf(
						"%w: ranking block at slot %d has no endorser block provider",
						errCertifiedEndorserBlockUnavailable,
						point.Slot,
					)
				}
			}
		} else {
			ebHash, ebSlot, ebSize, referenced, refErr := ls.leiosEndorserBlockForApply(
				block,
			)
			switch {
			case refErr != nil:
				if !ls.config.LeiosApplyEndorserBlockTxs {
					return nil, fmt.Errorf(
						"%w: resolve certified endorser block at slot %d: %w",
						errCertifiedEndorserBlockUnavailable,
						point.Slot,
						refErr,
					)
				}
				ls.config.Logger.Warn(
					"failed to resolve Leios endorser block for ranking block",
					"component", "ledger",
					"slot", point.Slot,
					"error", refErr,
				)
			case referenced:
				// ebSlot is the expected slot leiosEndorserBlockForApply
				// derived structurally (the endorser block shares its
				// announcing ranking block's slot), passed to the provider
				// as a required input rather than checked against its
				// output: the manifest is content-addressed, so the same
				// hash can be a live, independently required occurrence at
				// more than one slot at once, and the provider resolves
				// exactly this occurrence rather than whichever one happens
				// to be cached for the hash (issue #3513 review).
				ebTxs, ok := ls.config.EndorserBlockProvider(
					ebHash.Bytes(),
					ebSlot,
				)
				if ok {
					var donation uint64
					applied, donation, err := ls.applyEndorserBlock(
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
						if !ls.config.LeiosApplyEndorserBlockTxs {
							return nil, fmt.Errorf(
								"%w: apply certified endorser block at slot %d: %w",
								errCertifiedEndorserBlockUnavailable,
								point.Slot,
								err,
							)
						}
						ls.config.Logger.Warn(
							"failed to apply Leios endorser block transactions",
							"component", "ledger",
							"slot", point.Slot,
							"eb_slot", ebSlot,
							"error", err,
						)
					default:
						ls.logLeiosEndorserBlockApplyResult(
							point,
							ebSlot,
							ebTxs,
							applied,
						)
						blockDonation, err = addUint64(blockDonation, donation)
						if err != nil {
							return nil, fmt.Errorf(
								"accumulate leios endorser block donation: %w",
								err,
							)
						}
					}
				} else {
					if !ls.config.LeiosApplyEndorserBlockTxs {
						return nil, fmt.Errorf(
							"%w: ranking block at slot %d requires EB %s",
							errCertifiedEndorserBlockUnavailable,
							point.Slot,
							ebHash.String(),
						)
					}
					ls.config.Logger.Debug(
						"ranking block references an endorser block not yet cached",
						"component", "ledger",
						"slot", point.Slot,
						"eb_hash", ebHash.String(),
						"eb_size", ebSize,
					)
				}
			default:
				ls.config.Logger.Debug(
					"dijkstra block has no Leios endorser block to apply",
					"component", "ledger",
					"slot", point.Slot,
				)
			}
		}
	}

	if err := ls.verifyDeferredBlockHeaderState(txn, point, block); err != nil {
		return nil, err
	}
	// Process transactions
	var delta *LedgerDelta
	// Steady-state, at-tip, validated application refuses to recover an absent
	// consumed-input producer from the blob store and treats it as a hard error
	// instead (issue #3005). See strictConsumedInputsEnabled for why the
	// per-block reachesTip signal is required in addition to reachedTip.
	strictConsumedInputs := ls.strictConsumedInputsEnabled(
		shouldValidate,
		reachesTip,
	)
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
			delta.strictConsumedInputs = strictConsumedInputs
			if !shouldValidate && blockDonation > 0 {
				if err := delta.donate(blockDonation); err != nil {
					delta.Release()
					return nil, fmt.Errorf(
						"seed block donation: %w", err,
					)
				}
				blockDonation = 0
			}
		}
		// Era validators run phase-1 rules for every transaction, then require
		// the locally evaluated phase-2 result to match the declared validity
		// flag before applying any transaction state. Phase-2-invalid
		// transactions still need valid intervals, fees, collateral, witnesses,
		// and other structural checks.
		if shouldValidate {
			validationEra, err := resolveValidationEra(
				tx,
				currentEra,
				ls.eraList(),
			)
			if err != nil {
				delta.Release()
				return nil, err
			}
			// Standard Dijkstra/CIP profiles validate ranking-block transactions
			// even when the referenced endorser block is unavailable; missing
			// endorser-resident inputs then produce a validation error. Skip
			// Dijkstra validation only on the Haskell-conformant prototype path
			// (Musashi, SkipDijkstraTxValidation), where endorser transactions
			// are stored but not applied and the Leios certificate is trusted.
			skipDijkstraValidation := ls.skipDijkstraTxValidation(
				validationEra.Id,
			)
			if validationEra.ValidateTxFunc != nil && !skipDijkstraValidation {
				// Use the previous era's protocol
				// parameters when validating an era-1
				// transaction.
				pp := pparams
				if validationEra.Id != currentEra.Id &&
					prevEraPParams != nil {
					pp = prevEraPParams
				}
				lv := (&LedgerView{
					txn:                  txn,
					ls:                   ls,
					intraBlockUtxos:      intraBlockUtxos,
					skipPhase2Validation: skipPhase2Validation,
					// The reference implementation ticks from the block's
					// immediate predecessor, so that is where the era forecast
					// horizon has to be measured from. ls.currentTip is only
					// published once a whole batch commits, and applySafeZone
					// snaps up to an epoch boundary, so trailing by even one
					// block can cost an entire epoch of horizon and reject a
					// canonical Plutus transaction (issue #3844).
					horizonAnchorSlot: parent.slot,
				}).pinCommitteeState(committeeEpoch, pp)
				err := validationEra.ValidateTxFunc(
					tx,
					point.Slot,
					lv,
					pp,
				)
				// The Musashi prototype trusts remaining Dijkstra validation
				// disagreements because its certificate-driven closure is still
				// evolving. Standard profiles leave the error intact and reject
				// the block below.
				if err != nil &&
					ls.trustDijkstraTxValidationError(validationEra.Id) {
					ls.config.Logger.Warn(
						"Dijkstra tx validation disagreement (trusting Leios-certified block)",
						"component",
						"ledger",
						"tx_hash",
						tx.Hash().String(),
						"block_slot",
						point.Slot,
						"error",
						err.Error(),
					)
					err = nil
				}
				if err != nil {
					var plutusErr conway.PlutusScriptFailedError
					if errors.As(err, &plutusErr) {
						ls.config.Logger.Warn(
							"Plutus evaluation disagrees with block producer (rejecting transaction)",
							"component",
							"ledger",
							"tx_hash",
							tx.Hash().String(),
							"block_slot",
							point.Slot,
							"script_hash",
							hex.EncodeToString(plutusErr.ScriptHash[:]),
							"redeemer_tag",
							plutusErr.Tag,
							"redeemer_index",
							plutusErr.Index,
							"eval_error",
							plutusErr.Err.Error(),
						)
					}
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
			if err := delta.applyWithoutRecordingDonations(ls, txn); err != nil {
				delta.Release()
				if errors.Is(err, models.ErrRewardWithdrawalExceedsBalance) {
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
				return nil, err
			}
			var err error
			blockDonation, err = addUint64(blockDonation, delta.donation)
			if err != nil {
				delta.Release()
				return nil, fmt.Errorf(
					"accumulate block donation: %w", err,
				)
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
	if blockDonation > 0 {
		if delta == nil {
			delta = NewLedgerDelta(
				point,
				uint(block.Era().Id),
				block.BlockNumber(),
			)
			delta.Offsets = offsets
		}
		if err := delta.donate(blockDonation); err != nil {
			delta.Release()
			return nil, fmt.Errorf("finalize block donation: %w", err)
		}
	}
	// Record the opcert counter now that the block's transactions are
	// processed. The monotonicity check ran before transaction validation
	// (above); this write is what advances the stored counter, and it is
	// rolled back with the transaction if the block is rejected.
	if hasOpCert {
		if err := ls.db.UpdatePoolOpCertSequence(
			opCertPoolKeyHash,
			opCertIssueNumber,
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

// latestOpCertCounterForValidation returns the highest observed counter after
// the Mithril boundary, or the certified counter at the boundary when no later
// row exists. Rows before the boundary are not part of the certified state.
// Reads mithrilLedgerSlot through the lock-safe mithrilLedgerSlotSnapshot,
// since the block-apply transaction this runs inside does not itself hold
// a lock across that field (the snapshot taken before the transaction,
// e.g. ledgerProcessBlocksFromSource's snapshotMithrilSlot, is not
// threaded down to this call).
func (ls *LedgerState) latestOpCertCounterForValidation(
	poolKeyHash lcommon.PoolKeyHash,
	txn *database.Txn,
) (uint64, bool, error) {
	return ls.latestOpCertCounterAfterMithril(
		poolKeyHash,
		ls.mithrilLedgerSlotSnapshot(),
		txn,
	)
}

// latestOpCertCounterAfterMithril is the Mithril-boundary-aware resolver
// shared by latestOpCertCounterForValidation (block application) and
// LatestOpCertSequence (startup and forge-loop credential checks), so both
// paths agree on which counter is "the latest observed" for a pool instead
// of one trusting a plain MAX over rows a Mithril import may have left
// stale relative to the certified boundary. mithrilLedgerSlot is passed in
// rather than read from ls directly so each caller controls how it is
// obtained (a lock-safe snapshot, or a value already captured under one).
func (ls *LedgerState) latestOpCertCounterAfterMithril(
	poolKeyHash lcommon.PoolKeyHash,
	mithrilLedgerSlot uint64,
	txn *database.Txn,
) (uint64, bool, error) {
	if mithrilLedgerSlot > 0 {
		sequence, found, err := ls.db.LatestPoolOpCertSequenceAfter(
			poolKeyHash,
			mithrilLedgerSlot,
			txn,
		)
		if err != nil || found {
			return sequence, found, err
		}
		return ls.db.LatestPoolOpCertSequenceAfter(
			poolKeyHash,
			mithrilLedgerSlot-1,
			txn,
		)
	}
	return ls.db.LatestPoolOpCertSequence(poolKeyHash, txn)
}

func (ls *LedgerState) logLeiosEndorserBlockApplyResult(
	point ocommon.Point,
	ebSlot uint64,
	ebTxs []cbor.RawMessage,
	applied int,
) {
	switch {
	case applied > 0:
		ls.config.Logger.Info(
			"applied Leios endorser block transactions",
			"component", "ledger",
			"slot", point.Slot,
			"eb_slot", ebSlot,
			"eb_txs", applied,
		)
	case len(ebTxs) == 0:
		ls.config.Logger.Debug(
			"Leios endorser block has no transactions",
			"component", "ledger",
			"slot", point.Slot,
			"eb_slot", ebSlot,
		)
	default:
		ls.config.Logger.Debug(
			"skipped already-applied Leios endorser block transactions",
			"component", "ledger",
			"slot", point.Slot,
			"eb_slot", ebSlot,
		)
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
	ls.publishSnapshotsLocked()
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
	if ls.currentEra.Id == eras.ByronEraDesc.Id {
		// Byron has no protocol-version pparams, so there is no transition to
		// reconstruct and a Shelley-shaped value must not be read as one --
		// that would fabricate a transition at epoch zero, when the first
		// Shelley block on chain is what establishes the real boundary.
		//
		// This is not redundant with the currentPParams == nil check below.
		// rollbackChainAndStateDeferred calls this immediately after setting
		// currentEra, and until ppComputed replaced the old nil test there a
		// rollback into Byron left currentPParams holding its Shelley value
		// under a Byron era -- exactly the shape this guard rejects. The guard
		// stays as the invariant's backstop for any future caller that
		// reaches here with the two out of step.
		return
	}
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
	snapshotDelegatorInactivityOn := ls.config.DelegatorInactivityEnabled
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
				snapshotDelegatorInactivityOn,
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
		shouldPublish := false
		defer func() {
			if shouldPublish {
				ls.publishSnapshotsLocked()
			}
			ls.Unlock()
		}()
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
		shouldPublish = true
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
		pparams, err = ls.loadPersistedProtocolParameters(
			epoch.EpochId,
			era,
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
				prevPP, prevErr := ls.loadPersistedProtocolParameters(
					ep.EpochId,
					*prevEra,
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

// loadPersistedProtocolParameters decodes the era-owned CBOR row and restores
// Dijkstra's genesis-only stake thresholds, which are intentionally absent
// from the on-chain protocol-parameter CBOR representation.
func (ls *LedgerState) loadPersistedProtocolParameters(
	epoch uint64,
	era eras.EraDesc,
	txn *database.Txn,
) (lcommon.ProtocolParameters, error) {
	if era.DecodePParamsFunc == nil {
		return nil, nil
	}
	pparams, err := ls.db.GetPParams(
		epoch,
		era.Id,
		era.DecodePParamsFunc,
		txn,
	)
	if err != nil || pparams == nil || era.Id != eras.DijkstraEraDesc.Id {
		return pparams, err
	}
	dijkstraPParams, ok := pparams.(*dijkstra.DijkstraProtocolParameters)
	if !ok || dijkstraPParams == nil {
		return nil, fmt.Errorf(
			"persisted Dijkstra pparams decoded as %T",
			pparams,
		)
	}
	if ls.config.CardanoNodeConfig != nil {
		genesis := ls.config.CardanoNodeConfig.DijkstraGenesis()
		if genesis != nil {
			if dijkstraPParams.CommitteeStakeCoverage == nil {
				dijkstraPParams.CommitteeStakeCoverage = cloneGenesisRat(
					genesis.CommitteeStakeCoverage,
				)
			}
			if dijkstraPParams.QuorumStakeThreshold == nil {
				dijkstraPParams.QuorumStakeThreshold = cloneGenesisRat(
					genesis.QuorumStakeThreshold,
				)
			}
		}
	}
	if err := dijkstraPParams.ValidateLeiosCommitteeParameters(); err != nil {
		return nil, fmt.Errorf("validate persisted Dijkstra pparams: %w", err)
	}
	return dijkstraPParams, nil
}

func cloneGenesisRat(value *lcommon.GenesisRat) *cbor.Rat {
	if value == nil || value.Rat == nil {
		return nil
	}
	return &cbor.Rat{Rat: new(big.Rat).Set(value.Rat)}
}

// computeGenesisProtocolParameters bootstraps protocol parameters
// from genesis config for the given era without reading any shared
// LedgerState fields.
func (ls *LedgerState) computeGenesisProtocolParameters(
	era eras.EraDesc,
) (lcommon.ProtocolParameters, error) {
	// Byron has no protocol-parameter CBOR representation in the ledger
	// state. Its genesis config supplies the era timing and security values;
	// returning Shelley parameters here would make startup falsely infer a
	// pending Shelley transition and would leave epoch rollovers with a
	// parameter value that cannot be cloned using a Byron decoder.
	if era.Id == eras.ByronEraDesc.Id {
		return nil, nil
	}
	// Start with Shelley parameters as the base for all eras
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
		return errors.New(
			"PrepareEpochCacheForStartup must be called before LedgerState.Start",
		)
	}
	txn := ls.db.Transaction(true)
	defer txn.Release()
	return txn.Do(func(txn *database.Txn) error {
		return ls.loadEpochs(txn)
	})
}

// warnOnPreByronPrefixEpochCache reports a database written before the Byron
// prefix was preserved at startup.
//
// The startup fix is deliberately scoped to the empty-database branch above, so
// a database that already tagged epoch 0 with a post-Byron era is not repaired
// by upgrading. Its Shelley-relative slots stay shifted, and the failure it
// produces is the same genesis-overlay rejection as before the fix, with
// nothing to indicate that the binary already carries it. Detecting the shape
// turns that into a diagnosable message.
//
// Log only. Repair means resyncing from an empty database, which is the
// operator's call and not something to do to their data on their behalf.
//
// The condition is the same one setEpochCache uses to choose a Byron start, so
// the two cannot disagree: a Byron genesis is configured, the network does not
// declare Shelley at genesis, and Dijkstra was not forced. Under those inputs a
// fresh database tags epoch 0 as Byron, so a post-Byron era there could only
// have been written by an earlier binary.
func (ls *LedgerState) warnOnPreByronPrefixEpochCache() {
	if len(ls.epochCache) == 0 ||
		ls.config.CardanoNodeConfig == nil ||
		ls.config.CardanoNodeConfig.ByronGenesis() == nil ||
		ls.config.StartInDijkstra ||
		shelleyDeclaredAtGenesis(ls.config.CardanoNodeConfig) {
		return
	}
	firstEpoch := ls.epochCache[0]
	if firstEpoch.EpochId != 0 || firstEpoch.EraId <= eras.ByronEraDesc.Id {
		return
	}
	// Set only once the shape is confirmed, so a call that returned early
	// above cannot suppress a later real diagnosis.
	if ls.preByronPrefixWarned {
		return
	}
	ls.preByronPrefixWarned = true
	ls.config.Logger.Warn(
		"database predates Byron prefix preservation and cannot be repaired in place",
		"component",
		"ledger",
		"epoch",
		firstEpoch.EpochId,
		"era_id",
		firstEpoch.EraId,
		"expected_era_id",
		eras.ByronEraDesc.Id,
		"action",
		"resync from an empty database to follow the canonical chain",
	)
}

// shelleyDeclaredAtGenesis reports whether the configuration declares that
// Shelley is already active at epoch 0, which is what distinguishes a network
// with no Byron prefix from one that reaches Shelley on chain.
//
// Read through CardanoNodeConfig.DeclaredHardForkEpoch, which reports what the
// file says regardless of ExperimentalHardForksEnabled. HardForkEpoch answers a
// different question -- whether a fork is *scheduled* -- and returns
// (0, false) when the flag is unset, which hides preview's declaration
// (TestShelleyHardForkAtEpoch: 0 with ExperimentalHardForksEnabled: False).
// Both accessors read the same field through the same switch, so there is one
// interpreter of it rather than two.
//
// Only epoch 0 counts. A nonzero value declares a Shelley hard fork some
// epochs in, which means epochs 0..N-1 are Byron -- a Byron prefix, not the
// absence of one -- so those configurations keep the Byron start.
func shelleyDeclaredAtGenesis(cfg *cardano.CardanoNodeConfig) bool {
	if cfg == nil {
		return false
	}
	epoch, declared := cfg.DeclaredHardForkEpoch("shelley")
	return declared && epoch == 0
}

func (ls *LedgerState) setEpochCache(
	txn *database.Txn,
	epochs []models.Epoch,
) error {
	ls.epochCache = epochs
	// Publish every mutation made by this startup writer, including partial
	// state on error returns, so snapshot readers can never retain a stale view
	// if this helper is reused outside fatal startup handling in the future.
	defer ls.publishSnapshotsLocked()
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
		ls.warnOnPreByronPrefixEpochCache()
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
	// A real Cardano network with Byron genesis starts in Byron and reaches
	// Shelley when the first Shelley boundary is observed on chain. The
	// Shelley protocol version identifies the first post-Byron era, but does
	// not identify its absolute hard-fork epoch. Starting Shelley at slot 0
	// shifts all Shelley-relative slots on networks with a Byron prefix (for
	// example preprod), which makes genesis-overlay delegation reject the
	// canonical first Shelley block. Configurations that declare Shelley at
	// epoch 0 have no Byron prefix and keep the immediate transition, as do
	// Shelley-only configs without Byron genesis.
	if startEraId > eras.ByronEraDesc.Id &&
		ls.config.CardanoNodeConfig.ByronGenesis() != nil &&
		!ls.config.StartInDijkstra &&
		!shelleyDeclaredAtGenesis(ls.config.CardanoNodeConfig) {
		startEraId = eras.ByronEraDesc.Id
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
		false,
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
	// The durable marker itself was already persisted transactionally
	// inside processEpochRollover; this only updates the in-memory mirror.
	if rolloverResult.RealV2CostModelObserved {
		ls.syntheticV2CostModel = false
	}
	ls.checkpointWrittenForEpoch = rolloverResult.CheckpointWrittenForEpoch
	ls.metrics.epochNum.Set(rolloverResult.NewEpochNum)
	return nil
}

// healEmptyLabNonces repairs epoch records whose LastEpochBlockNonce or Nonce
// was persisted empty or wrong by pre-fix boundary bugs.
//
// LastEpochBlockNonce is the Praos lab carried at this epoch's opening
// boundary: the parent hash (PrevHash) of the last canonical block before the
// boundary (cardano-ledger praosStateLastEpochBlockNonce). It feeds the NEXT
// epoch's nonce: Nonce(E) = CandidateNonce(E) ⭒ LastEpochBlockNonce(E-1).
// Earlier boundary lookups scanned block-blob keys, so they could return a
// synthetic Leios endorser block or retained fork blob instead of the active
// chain's real ranking block, and older releases stored the boundary block's
// own hash instead of its parent hash. Empty or wrong lab values diverge epoch
// nonces from peers and break leader-VRF verification.
//
// This runs once at startup over the loaded epoch cache, in ascending epoch
// order. For each non-genesis epoch it independently (a) restores the lab from
// the canonical chain index when it differs from the expected epochLabNonce
// semantics, and (b) recomputes the epoch's nonce from its candidate and the
// PREVIOUS epoch's (already repaired) carried lab when they disagree. The lab
// repair needs no candidate; the nonce repair is skipped when the candidate is
// missing/invalid or the previous epoch's lab could not be verified. It is a
// pure function of already-stored chain data and is idempotent across restarts.
// The writer-owned epoch cache must have been replaced with a fresh DB-loaded
// slice and must not have been published before this in-place repair runs.
func (ls *LedgerState) healEmptyLabNonces() {
	if ls.healEmptyLabNoncesInPlace(ls.epochCache) {
		clear(ls.epochNonceHexCache)
	}
}

// healLabNonceRecentEpochs bounds the startup lab/nonce repair to the most
// recent epochs. Epoch labs are immutable historical data, and only the
// resume-tip epoch's lab feeds a nonce the node computes at runtime: the
// current epoch nonce mixes the previous epoch's lab, forward boundaries
// recompute labs fresh in calculateEpochNonce, and rollbacks are bounded by the
// security parameter (well under one epoch on real networks). Re-deriving every
// historical epoch's lab from the chain on every restart is one canonical-block
// lookup per epoch, which on a large blob store costs many minutes for no
// consensus benefit. This window comfortably covers the current epoch, the
// bounded rollback window, and a margin; DBs with fewer epochs are fully
// processed.
const healLabNonceRecentEpochs = 8

// healEmptyLabNoncesInPlace mutates epoch entries and their nonce slices.
// Callers must pass a freshly owned, unpublished slice; passing an epoch cache
// already exposed through consensusSnapshot would corrupt concurrent readers.
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
	// labVerified[i] records that epochs[i].LastEpochBlockNonce is known to
	// match the canonical lab semantics (verified against the chain, repaired,
	// or Mithril-trusted), so it is safe to use as the eta input for
	// epochs[i+1]'s nonce check.
	// Verify/repair the recent window (see healLabNonceRecentEpochs), plus one
	// predecessor epoch. The predecessor is scanned only so its lab is verified
	// and repaired: the oldest in-window epoch's nonce check mixes the PREVIOUS
	// epoch's LastEpochBlockNonce, so without a verified predecessor lab that
	// first in-window nonce would be skipped and left stale. Epochs before the
	// scan are immutable and never feed a runtime nonce, so re-deriving their
	// labs from the chain on every restart is wasted work.
	startIdx := 0
	if scanEpochs := healLabNonceRecentEpochs + 1; len(epochs) > scanEpochs {
		startIdx = len(epochs) - scanEpochs
	}
	labVerified := make([]bool, len(epochs))
	for i := startIdx; i < len(epochs); i++ {
		ep := &epochs[i]
		// The genesis epoch legitimately carries no last block nonce.
		if ep.StartSlot == 0 {
			labVerified[i] = len(ep.LastEpochBlockNonce) == 0
			continue
		}
		if ls.mithrilLedgerSlot > 0 &&
			ep.StartSlot <= ls.mithrilLedgerSlot &&
			len(ep.LastEpochBlockNonce) == lcommon.Blake2b256Size {
			recordSkippedMithrilTrustedEpoch(ep.EpochId)
			labVerified[i] = true
			continue
		}
		boundary, err := ls.canonicalBlockBeforeSlot(nil, ep.StartSlot)
		if err != nil {
			// A pre-Praos epoch carries no lab by definition, so its nil lab
			// is verified even without chain data.
			if len(ep.Nonce) == 0 && len(ep.LastEpochBlockNonce) == 0 {
				labVerified[i] = true
			}
			continue
		}
		expectedLab, ok := ls.expectedEpochRepairLab(epochs, i, boundary)
		if !ok {
			continue
		}
		if !bytes.Equal(ep.LastEpochBlockNonce, expectedLab) {
			previousLab := cloneNonce(ep.LastEpochBlockNonce)
			ep.LastEpochBlockNonce = cloneNonce(expectedLab)
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
		}
		labVerified[i] = true
		// The nonce check needs the PREVIOUS epoch's carried lab (already
		// verified/repaired above, since the loop runs in ascending order):
		//   Nonce(E) = CandidateNonce(E) ⭒ LastEpochBlockNonce(E-1)
		// Mixing with this epoch's OWN lab instead would shift eta by one
		// epoch — the exact #2734 divergence this heal exists to repair.
		if i == 0 || !labVerified[i-1] {
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
		labForEta := epochs[i-1].LastEpochBlockNonce
		var expectedNonce []byte
		if len(labForEta) == 0 {
			// NeutralNonce is the identity element of ⭒:
			//   candidateNonce ⭒ NeutralNonce = candidateNonce
			expectedNonce = cloneNonce(candidateNonce)
		} else {
			res, err := lcommon.CalculateEpochNonce(
				candidateNonce,
				labForEta,
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
			expectedNonce = res.Bytes()
		}
		if !bytes.Equal(ep.Nonce, expectedNonce) {
			previousNonce := cloneNonce(ep.Nonce)
			ep.Nonce = expectedNonce
			repaired = true
			ls.config.Logger.Info(
				"recomputed epoch nonce after lab recovery",
				"epoch", ep.EpochId,
				"previous_nonce", hex.EncodeToString(previousNonce),
				"epoch_nonce", hex.EncodeToString(expectedNonce),
				"component", "ledger",
			)
		}
	}
	if skippedMithrilTrusted > 0 {
		ls.config.Logger.Info(
			"skipped epoch lab recovery for epochs covered by Mithril trust boundary",
			"count",
			skippedMithrilTrusted,
			"first_epoch",
			firstMithrilTrustedEpoch,
			"last_epoch",
			lastMithrilTrustedEpoch,
			"mithril_ledger_slot",
			ls.mithrilLedgerSlot,
			"component",
			"ledger",
		)
	}
	if skippedMissingCandidate > 0 {
		ls.config.Logger.Info(
			"skipped epoch lab recovery for epochs without stored candidate nonce",
			"count",
			skippedMissingCandidate,
			"first_epoch",
			firstMissingCandidateEpoch,
			"last_epoch",
			lastMissingCandidateEpoch,
			"component",
			"ledger",
		)
	}
	if skippedInvalidCandidate > 0 {
		ls.config.Logger.Warn(
			"skipped epoch lab recovery for epochs with invalid candidate nonce",
			"count",
			skippedInvalidCandidate,
			"first_epoch",
			firstInvalidCandidateEpoch,
			"last_epoch",
			lastInvalidCandidateEpoch,
			"component",
			"ledger",
		)
	}
	return repaired
}

// expectedEpochRepairLab returns the canonical LastEpochBlockNonce for
// epochs[idx], given the last canonical block before its start slot.
//
// Pre-Praos (Byron) epochs carry no lab, and the FIRST Praos epoch's carried
// lab is NeutralNonce (nil): cardano-ledger initialChainDepState sets
// csLabNonce = NeutralNonce, and the initial-epoch branch of
// calculateEpochNonce stores a nil lab. "Repairing" it to the last pre-Praos
// block's parent hash would diverge the next boundary's eta on any chain with
// a Byron era. For every other epoch the carried lab is prevHashToNonce of the
// boundary block — regardless of which epoch that block fell in, since an
// empty closing epoch carries the same block's parent hash forward unchanged.
// Consulting the stored previous-epoch lab instead could inject an unhealed
// old-shape (own-hash) value, so the value is always derived from the chain.
func (ls *LedgerState) expectedEpochRepairLab(
	epochs []models.Epoch,
	idx int,
	boundary models.Block,
) ([]byte, bool) {
	if idx < 0 || idx >= len(epochs) {
		return nil, false
	}
	// Pre-Praos epochs have no nonce and carry no lab.
	if len(epochs[idx].Nonce) == 0 {
		return nil, true
	}
	// The first Praos epoch (previous epoch is pre-Praos) carries NeutralNonce.
	if idx > 0 && len(epochs[idx-1].Nonce) == 0 {
		return nil, true
	}
	// Derive the boundary block's parent hash, decoding the block CBOR when
	// the stored PrevHash is empty (legacy empty-PrevHash rows).
	prevHash, err := blockPrevHash(boundary)
	if err != nil || len(prevHash) != lcommon.Blake2b256Size {
		return nil, false
	}
	// cardano-ledger's prevHashToNonce maps GenesisHash to NeutralNonce: a
	// boundary block that is the chain's first block contributes no lab.
	if genesisHash, gErr := GenesisBlockHash(ls.config.CardanoNodeConfig); gErr == nil &&
		bytes.Equal(prevHash, genesisHash[:]) {
		return nil, true
	}
	return cloneNonce(prevHash), true
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
	ls.publishSnapshotsLocked()
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
			"component",
			"ledger",
			"chain_tip_slot",
			chainTip.Point.Slot,
			"ledger_tip_slot",
			ledgerTip.Point.Slot,
			"chain_tip_hash",
			hex.EncodeToString(chainTip.Point.Hash),
			"ledger_tip_hash",
			hex.EncodeToString(ledgerTip.Point.Hash),
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
			"component",
			"ledger",
			"chain_tip_slot",
			chainTip.Point.Slot,
			"ledger_tip_slot",
			ledgerTip.Point.Slot,
			"chain_tip_hash",
			hex.EncodeToString(chainTip.Point.Hash),
			"ledger_tip_hash",
			hex.EncodeToString(ledgerTip.Point.Hash),
			"block_gap",
			gap,
		)
		return nil
	}
	ancestor, found, err := ls.latestLedgerPrimaryChainAncestor(
		ledgerTip.Point,
		containsLedgerTip,
	)
	if err != nil {
		return fmt.Errorf(
			"find common primary-chain ancestor for ledger tip: %w",
			err,
		)
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
		"component",
		"ledger",
		"chain_tip_slot",
		chainTip.Point.Slot,
		"ledger_tip_slot",
		ledgerTip.Point.Slot,
		"chain_tip_hash",
		hex.EncodeToString(chainTip.Point.Hash),
		"ledger_tip_hash",
		hex.EncodeToString(ledgerTip.Point.Hash),
		"ancestor_slot",
		ancestor.Slot,
		"ancestor_hash",
		hex.EncodeToString(ancestor.Hash),
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
		"component",
		"ledger",
		"reason",
		reason,
		"connection_id",
		connId.String(),
		"chain_tip_slot",
		chainTip.Point.Slot,
		"ledger_tip_slot",
		ledgerTip.Point.Slot,
		"chain_tip_hash",
		hex.EncodeToString(chainTip.Point.Hash),
		"ledger_tip_hash",
		hex.EncodeToString(ledgerTip.Point.Hash),
	)
	if err := ls.reconcilePrimaryChainTipWithLedgerTip(); err != nil {
		return false, err
	}
	return true, nil
}

func (ls *LedgerState) primaryChainContainsPoint(
	point ocommon.Point,
) (bool, error) {
	if point.Slot == 0 && len(point.Hash) == 0 {
		return true, nil
	}
	if ls.db == nil {
		return false, nil
	}
	block, err := database.BlockByPoint(ls.db, point)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return false, nil
		}
		return false, err
	}
	return ls.primaryChainContainsBlock(block, point)
}

// primaryChainContainsBlock checks whether block's internal ID currently maps
// to point on the authoritative primary chain. The block itself may come from
// an abandoned fork retained in the append-only blob store, so blob presence
// alone is not authoritative.
func (ls *LedgerState) primaryChainContainsBlock(
	block models.Block,
	point ocommon.Point,
) (bool, error) {
	if block.Slot != point.Slot || !bytes.Equal(block.Hash, point.Hash) {
		return false, nil
	}
	return ls.primaryChainContainsBlockID(block.ID, point)
}

func (ls *LedgerState) primaryChainContainsBlockID(
	blockID uint64,
	point ocommon.Point,
) (bool, error) {
	// Blob presence by point is not authoritative: abandoned-fork blocks remain
	// in the append-only blob store. The current primary chain is identified by
	// its block-index entry, so compare the point encoded at this ID with the
	// requested point. Reading only the index value avoids loading the indexed
	// block's CBOR a second time while chainsyncMutex is held.
	indexedPoint, err := ls.db.BlockPointByIndex(blockID, nil)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return false, nil
		}
		return false, err
	}
	return pointMatches(indexedPoint, point), nil
}

// durableAppliedFloor returns the point of the highest block whose ledger
// effects are durably applied, identified by the highest-slot block_nonce row
// and its application-order ID for same-slot blocks. The nonce table is
// written in the same metadata transaction as block effects and the ledger
// tip, so this is the applied high-water mark used by recovery.
func (ls *LedgerState) durableAppliedFloor() (ocommon.Point, bool, error) {
	if ls.db == nil {
		return ocommon.Point{}, false, nil
	}
	row, ok, err := ls.db.GetLatestBlockNonce(nil)
	if err != nil {
		return ocommon.Point{}, false, err
	}
	if !ok || row.Slot == 0 || len(row.Hash) == 0 {
		return ocommon.Point{}, false, nil
	}
	return ocommon.NewPoint(row.Slot, row.Hash), true, nil
}

// enforceDurableTipFloor repairs currentTip when it leads the durably applied
// state. Equal slots still require a hash comparison: a same-slot competing
// block can be an unapplied fork even though its slot is within the floor.
func (ls *LedgerState) enforceDurableTipFloor() error {
	floor, ok, err := ls.durableAppliedFloor()
	if err != nil {
		return fmt.Errorf("determine durable applied floor: %w", err)
	}
	if !ok {
		return nil
	}
	ls.RLock()
	currentTip := ls.currentTip
	ls.RUnlock()
	if currentTip.Point.Slot < floor.Slot ||
		(currentTip.Point.Slot == floor.Slot &&
			bytes.Equal(currentTip.Point.Hash, floor.Hash)) {
		return nil
	}
	ls.config.Logger.Warn(
		"ledger tip leads durable applied state, repairing tip down to applied floor",
		"component",
		"ledger",
		"ledger_tip_slot",
		currentTip.Point.Slot,
		"ledger_tip_hash",
		hex.EncodeToString(currentTip.Point.Hash),
		"applied_floor_slot",
		floor.Slot,
		"applied_floor_hash",
		hex.EncodeToString(floor.Hash),
	)
	return ls.rollback(floor)
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
	if len(points) == 0 {
		return []ocommon.Point{point}
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
	tip := ls.loadTipSnapshot().currentTip
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
	return cloneTip(ls.loadTipSnapshot().currentTip)
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
	return ls.loadTipSnapshot().currentTip.Point.Slot
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

// UpstreamTipSlot returns the corroborated remote sync target while an upstream
// connection is active. The admitted frontier is retained separately for
// admission bookkeeping.
func (ls *LedgerState) UpstreamTipSlot() uint64 {
	if ls.config.GetActiveConnectionFunc == nil {
		return ls.syncUpstreamTipSlot.Load()
	}
	activeConnId := ls.config.GetActiveConnectionFunc()
	if activeConnId == nil || !ls.isConnectionLive(*activeConnId) {
		return 0
	}
	state := ls.syncUpstreamState.Load()
	if state == nil || state.connectionKey != connIdKey(*activeConnId) {
		return 0
	}
	return state.targetSlot
}

// UpstreamSyncStatus reports whether a live upstream is selected and its
// corroborated target. An active upstream with target 0 is still syncing.
func (ls *LedgerState) UpstreamSyncStatus() (uint64, bool) {
	if ls.config.GetActiveConnectionFunc == nil {
		target := ls.UpstreamTipSlot()
		return target, target != 0
	}
	activeConnId := ls.config.GetActiveConnectionFunc()
	if activeConnId == nil || !ls.isConnectionLive(*activeConnId) {
		return 0, false
	}
	state := ls.syncUpstreamState.Load()
	if state == nil || state.connectionKey != connIdKey(*activeConnId) {
		return 0, true
	}
	return state.targetSlot, true
}

func (ls *LedgerState) advanceUpstreamTipSlot(slot uint64) {
	current := ls.syncUpstreamTipSlot.Load()
	for slot > current {
		if ls.syncUpstreamTipSlot.CompareAndSwap(current, slot) {
			break
		}
		current = ls.syncUpstreamTipSlot.Load()
	}
}

func (ls *LedgerState) publishActiveUpstream(connId ouroboros.ConnectionId) {
	if ls.config.GetActiveConnectionFunc == nil {
		return
	}
	activeConnId := ls.config.GetActiveConnectionFunc()
	if activeConnId == nil || !sameConnectionId(*activeConnId, connId) ||
		!ls.isConnectionLive(connId) {
		return
	}
	current := ls.syncUpstreamState.Load()
	if current != nil && current.connectionKey == connIdKey(connId) {
		return
	}
	ls.syncUpstreamState.Store(
		&upstreamSyncState{connectionKey: connIdKey(connId)},
	)
}

func (ls *LedgerState) clearActiveUpstream() {
	ls.syncUpstreamState.Store(nil)
}

// publishAdmittedUpstreamTarget is called only after a header has been
// authenticated and admitted. Revalidation binds the target to the still-live
// active connection rather than a prior switch generation.
func (ls *LedgerState) publishAdmittedUpstreamTarget(e ChainsyncEvent) {
	connId := e.ConnectionId
	if ls.config.GetActiveConnectionFunc == nil {
		return
	}
	activeConnId := ls.config.GetActiveConnectionFunc()
	if activeConnId == nil || !sameConnectionId(*activeConnId, connId) ||
		!ls.isConnectionLive(connId) {
		return
	}
	if !e.SyncTargetTrusted {
		ls.publishActiveUpstream(connId)
		return
	}
	// Recheck after consulting chain selection: a handoff may have happened
	// while obtaining the target.
	activeConnId = ls.config.GetActiveConnectionFunc()
	if activeConnId == nil || !sameConnectionId(*activeConnId, connId) ||
		!ls.isConnectionLive(connId) {
		return
	}
	ls.syncUpstreamState.Store(&upstreamSyncState{
		connectionKey: connIdKey(connId),
		targetSlot:    e.SyncTarget.Point.Slot,
	})
}

// GetCurrentPParams returns the currentPParams value
func (ls *LedgerState) GetCurrentPParams() lcommon.ProtocolParameters {
	return ls.loadConsensusSnapshot().currentPParams
}

// GetCurrentPParamsForReporting returns the current protocol parameters with
// HardForkBabbage's fabricated PlutusV2 cost model omitted for as long as it
// hasn't been replaced by real governance/protocol-update data -- matching
// what a real cardano-node reports (blinklabs-io/dingo#3825). This is for
// external reporting surfaces only: LocalStateQuery's GetCurrentProtocolParams
// (ledger/queries.go), and the Blockfrost/UTXORPC/Mesh API adapters that
// separately surface protocol parameters. Every other caller (script
// validation, block-building limits, Leios committee parameters, governance
// proposal decoding) must keep calling GetCurrentPParams: internal logic
// needs the real fabricated default unconditionally, since a genuine PlutusV2
// script can arrive before the real update lands.
func (ls *LedgerState) GetCurrentPParamsForReporting() lcommon.ProtocolParameters {
	snapshot := ls.loadConsensusSnapshot()
	return withoutSyntheticV2CostModel(
		snapshot.currentPParams,
		snapshot.syntheticV2CostModelInEffect,
		ls.config.Logger,
	)
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
	snapshot := ls.loadConsensusSnapshot()
	currentEpoch := snapshot.currentEpoch
	currentEra := snapshot.currentEra
	currentPParams := snapshot.currentPParams

	if currentPParams == nil || currentEpoch.LengthInSlots == 0 {
		return currentPParams
	}
	// Epoch lengths can change at an era boundary. Resolve the target slot
	// through the multi-era converter so a Byron prefix does not make a
	// future Shelley slot appear to belong to an early epoch.
	slotEpoch := currentEpoch.EpochId
	// Use the epoch cache from the same immutable snapshot as the other
	// forecast inputs. Calling ls.SlotToEpoch here would load a second snapshot
	// and could mix its epoch cache with currentEpoch/currentEra/currentPParams
	// across a concurrent rollover or rollback.
	for i := len(snapshot.epochCache) - 1; i >= 0; i-- {
		epoch := snapshot.epochCache[i]
		if slot < epoch.StartSlot {
			continue
		}
		slotEpoch = epoch.EpochId
		if epoch.LengthInSlots != 0 {
			slotEpoch += (slot - epoch.StartSlot) /
				uint64(epoch.LengthInSlots)
		}
		break
	}
	if len(snapshot.epochCache) == 0 && slot >= currentEpoch.StartSlot {
		// With no cache, project from the captured current epoch. This retains
		// the absolute epoch offset while preserving the existing bare-state
		// forecast behavior.
		slotEpoch += (slot - currentEpoch.StartSlot) /
			uint64(currentEpoch.LengthInSlots)
	}
	if slotEpoch <= currentEpoch.EpochId {
		return currentPParams
	}
	shape := ls.eraShape()
	if len(shape.Eras) == 0 {
		return currentPParams
	}
	pparams := currentPParams
	// Before walking any era hard fork, apply the pending in-era
	// protocol-parameter update that the rollover will enact at the next
	// epoch boundary (into currentEpoch+1). This mirrors the rollover
	// order in processEpochRollover, where ComputeAndApplyPParamUpdates
	// runs in the current era BEFORE the hard-fork transition. Without
	// it, a normal-boundary update (e.g. the preview epoch 1->2
	// decentralization decrease, which is not an era fork) is never
	// reflected in the forecast, so the genesis-overlay check sees the
	// stale pre-boundary value for the next epoch. The forecast reads
	// the already-collected proposals, so it does not depend on the
	// target epoch's pparams row being persisted yet (it is not, during
	// from-genesis before the node has ticked into that epoch).
	if updated := ls.forecastPendingPParamUpdate(
		currentEra, currentEpoch.EpochId+1, pparams,
	); updated != nil {
		pparams = updated
	}
	// Walk forward from the current era, applying each successor's
	// HardForkFunc whose triggerEpoch <= slotEpoch. The single-step
	// case (one fork between currentEpoch and slotEpoch) is the
	// nominal path; the loop also tolerates multi-step jumps so the
	// helper stays sound if a caller skips ahead.
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

// forecastPendingPParamUpdate returns the protocol parameters produced by
// applying the pending proposed protocol-parameter update that the epoch
// rollover will enact into targetEpoch, computed in era. It is a pure
// forecast: the era update function (which mutates its concrete pointer in
// place) only ever sees an independently cloned copy, so the shared
// snapshot is never touched, and it performs no writes. It returns nil when
// there is nothing to apply — no DB, missing era update funcs, or no
// proposal meeting quorum for targetEpoch — in which case the caller keeps
// the era-fork-only forecast, identical to prior behavior.
func (ls *LedgerState) forecastPendingPParamUpdate(
	era eras.EraDesc,
	targetEpoch uint64,
	pparams lcommon.ProtocolParameters,
) lcommon.ProtocolParameters {
	if ls.db == nil ||
		pparams == nil ||
		era.DecodePParamsUpdateFunc == nil ||
		era.PParamsUpdateFunc == nil {
		return nil
	}
	// Quorum is the Shelley-genesis updateQuorum, exactly as the rollover
	// uses when enacting the same proposals (processEpochRollover).
	updateQuorum := 0
	if ls.config.CardanoNodeConfig != nil {
		if shelleyGenesis := ls.config.CardanoNodeConfig.ShelleyGenesis(); shelleyGenesis != nil {
			updateQuorum = shelleyGenesis.UpdateQuorum
		}
	}
	// Era update functions mutate their concrete pparams pointer in place.
	// Hand ForecastPParamUpdates a clone function so it clones only when it
	// is actually going to enact an update; the common no-op forecast then
	// leaves the shared snapshot's currentPParams untouched.
	updated, err := ls.db.ForecastPParamUpdates(
		targetEpoch,
		updateQuorum,
		pparams,
		era.DecodePParamsUpdateFunc,
		era.PParamsUpdateFunc,
		func(pp lcommon.ProtocolParameters) (lcommon.ProtocolParameters, error) {
			return cloneProtocolParametersForEra(era, pp)
		},
		nil,
	)
	if err != nil {
		ls.config.Logger.Warn(
			"forecastPendingPParamUpdate: forecast failed",
			"target_epoch", targetEpoch,
			"era", era.Id,
			"error", err,
		)
		return nil
	}
	return updated
}

// CurrentEpoch returns the current epoch number.
func (ls *LedgerState) CurrentEpoch() uint64 {
	return ls.loadConsensusSnapshot().currentEpoch.EpochId
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
func (ls *LedgerState) ConsensusModeForEpoch(
	epoch uint64,
) consensus.ConsensusMode {
	snapshot := ls.loadConsensusSnapshot()
	cache := snapshot.epochCache
	currentEra := snapshot.currentEra
	currentEpoch := snapshot.currentEpoch
	transitionInfo := snapshot.transitionInfo

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
	consensusState, tipState := ls.loadStateSnapshots()
	currentEpoch := consensusState.currentEpoch
	currentEra := consensusState.currentEra
	tipSlot := tipState.currentTip.Point.Slot

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
	snapshot := ls.loadConsensusSnapshot()
	currentEpoch := snapshot.currentEpoch
	if epoch == currentEpoch.EpochId {
		if len(currentEpoch.Nonce) > 0 {
			return cloneSnapshotBytes(currentEpoch.Nonce)
		}
		// In-memory nonce empty (e.g. after Mithril import) —
		// fall through to DB lookup
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
		return cloneSnapshotBytes(ep.Nonce)
	}
	// If the requested epoch is ahead of the ledger state (slot clock
	// fired an epoch transition before block processing caught up),
	// try to compute the nonce speculatively for the immediate next
	// epoch. The nonce depends only on data from the current (ending)
	// epoch, so it is computable before block processing catches up.
	if epoch > currentEpoch.EpochId {
		if epoch == currentEpoch.EpochId+1 {
			return ls.computeNextEpochNonce(
				currentEpoch,
				snapshot.currentEra,
			)
		}
		return nil
	}

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
	return cloneSnapshotBytes(ep.Nonce)
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
	currentEra := ls.loadConsensusSnapshot().currentEra

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

// ActiveSlotCoeffRat returns the active slot coefficient (f) as an exact
// *big.Rat taken straight from the Shelley genesis, with no float64 roundtrip.
// Returns nil when the genesis is unavailable.
//
// Prefer this over ActiveSlotCoeff for anything that feeds a leader check.
// ActiveSlotCoeff divides the genesis numerator and denominator as float64, and
// the nearest double to a value like 1/20 is strictly larger than 1/20, so a
// threshold derived from it is strictly larger than the reference node's and
// admits a strict superset of eligible slots. Both the header-verification path
// and the leader-schedule precompute must use this exact value so they cannot
// disagree with each other or with the reference.
func (ls *LedgerState) ActiveSlotCoeffRat() *big.Rat {
	return ls.activeSlotCoeffRat()
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
	// big.Rat is mutable and this value is shared genesis state, so hand out a
	// copy rather than the genesis pointer itself.
	return new(big.Rat).Set(shelleyGenesis.ActiveSlotsCoeff.Rat)
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
	view := &LedgerView{
		ls:  ls,
		txn: txn,
	}
	snapshot := ls.loadConsensusSnapshot()
	if snapshot == nil {
		return view
	}
	return view.pinCommitteeState(
		snapshot.currentEpoch.EpochId,
		snapshot.currentPParams,
	)
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

// ByronProtocolMagic returns the protocol magic configured in Byron genesis.
func (ls *LedgerState) ByronProtocolMagic() (uint32, error) {
	if ls == nil || ls.config.CardanoNodeConfig == nil {
		return 0, errors.New("cardano node config is unavailable")
	}
	byronGenesis := ls.config.CardanoNodeConfig.ByronGenesis()
	if byronGenesis == nil {
		return 0, errors.New("byron genesis is unavailable")
	}
	protocolMagic := byronGenesis.ProtocolConsts.ProtocolMagic
	if protocolMagic < 0 {
		return 0, errors.New("byron protocol magic is negative")
	}
	if protocolMagic > math.MaxUint32 {
		return 0, fmt.Errorf(
			"byron protocol magic exceeds uint32: %d",
			protocolMagic,
		)
	}
	// #nosec G115 -- the protocol magic is checked against the uint32 range above.
	return uint32(protocolMagic), nil
}

// UtxoByRef returns a single UTxO by reference
func (ls *LedgerState) UtxoByRef(
	txId []byte,
	outputIdx uint32,
) (*models.Utxo, error) {
	return ls.db.UtxoByRef(txId, outputIdx, nil)
}

// UtxosByRefs returns the live UTxOs matching the given references in a
// single batch. Refs with no matching live UTxO are simply absent from the
// result.
func (ls *LedgerState) UtxosByRefs(
	refs []models.UtxoId,
) ([]models.Utxo, error) {
	return ls.db.UtxosByRefs(refs, nil)
}

// UtxosByAddress returns all UTxOs that belong to any of the specified addresses
func (ls *LedgerState) UtxosByAddress(
	addrs []ledger.Address,
) ([]models.Utxo, error) {
	utxos, err := ls.db.UtxosByAddress(addrs, nil)
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
	store, ok := ls.db.Metadata().(metadata.SlotRangeStore)
	if !ok {
		return 0, errors.New(
			"metadata store does not support slot-range statistics",
		)
	}
	count, err := store.CountTransactionsInSlotRange(
		startSlot,
		endSlot,
		nil,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"count transactions in slot range %d-%d: %w",
			startSlot,
			endSlot,
			err,
		)
	}
	return count, nil
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
	store, ok := ls.db.Metadata().(metadata.SlotRangeStore)
	if !ok {
		return 0, 0, 0, errors.New(
			"metadata store does not support slot-range statistics",
		)
	}
	stats, err := store.GetBlockSlotRangeStats(startSlot, endSlot, nil)
	if err != nil {
		return 0, 0, 0, fmt.Errorf(
			"count blocks in slot range %d-%d: %w",
			startSlot,
			endSlot,
			err,
		)
	}
	return stats.Count, stats.FirstSlot, stats.LastSlot, nil
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

type txValidationSnapshot struct {
	generation     uint64
	currentEra     eras.EraDesc
	currentPParams lcommon.ProtocolParameters
	prevEraPParams lcommon.ProtocolParameters
	eraList        []eras.EraDesc
	referenceSlot  uint64
	currentEpoch   uint64
}

func (ls *LedgerState) txValidationSnapshot() txValidationSnapshot {
	consensusState, tipState := ls.loadStateSnapshots()
	tipSlot := tipState.currentTip.Point.Slot
	currentSlot, currentSlotErr := ls.CurrentSlot()
	if currentSlotErr != nil {
		ls.config.Logger.Debug(
			"slot clock unavailable during tx validation, falling back to snapshot tip slot",
			"error",
			currentSlotErr,
			"snapshot_tip_slot",
			tipSlot,
		)
		ls.metrics.slotClockFallbacks.Inc()
	}
	return txValidationSnapshot{
		generation:     consensusState.generation,
		currentEra:     consensusState.currentEra,
		currentPParams: consensusState.currentPParams,
		prevEraPParams: consensusState.prevEraPParams,
		eraList:        ls.eraList(),
		referenceSlot: validationReferenceSlot(
			tipSlot,
			currentSlot,
			currentSlotErr,
		),
		currentEpoch: consensusState.currentEpoch.EpochId,
	}
}

// Both the block builder and the mempool discover the validation-session
// capability with a runtime type assertion on the TxValidator they were handed
// and silently fall back to unpinned per-transaction validation when it fails,
// so drift in WithTxValidationSession would quietly drop the snapshot pinning
// rather than break the build. The mempool's identical interface is guarded in
// the root package, which is where *LedgerState is wired in as its validator.
var _ forging.TxValidationSessionProvider = (*LedgerState)(nil)

// WithTxValidationSession pins a mempool revalidation batch to one immutable
// ledger publication, one validation slot/era/parameter set, and one
// repeatable-read database transaction. stillCurrent lets the mempool reject
// the candidate immediately before its atomic swap if a block or rollback
// published a newer generation while validation was running.
func (ls *LedgerState) WithTxValidationSession(
	fn func(
		validate func(
			tx ledger.Transaction,
			consumedUtxos map[string]struct{},
			createdUtxos map[string]lcommon.Utxo,
		) error,
		stillCurrent func() bool,
	) error,
) error {
	snapshot := ls.txValidationSnapshot()

	txn := ls.db.Transaction(false)
	return txn.Do(func(txn *database.Txn) error {
		validate := func(
			tx ledger.Transaction,
			consumedUtxos map[string]struct{},
			createdUtxos map[string]lcommon.Utxo,
		) error {
			validationEra, err := resolveValidationEra(
				tx,
				snapshot.currentEra,
				snapshot.eraList,
			)
			if err != nil {
				return err
			}
			if validationEra.ValidateTxFunc == nil {
				return nil
			}
			pp := snapshot.currentPParams
			if validationEra.Id != snapshot.currentEra.Id &&
				snapshot.prevEraPParams != nil {
				pp = snapshot.prevEraPParams
			}
			err = validationEra.ValidateTxFunc(
				tx,
				snapshot.referenceSlot,
				(&LedgerView{
					txn:             txn,
					ls:              ls,
					intraBlockUtxos: createdUtxos,
					consumedUtxos:   consumedUtxos,
				}).pinCommitteeState(snapshot.currentEpoch, pp),
				pp,
			)
			if err != nil {
				return fmt.Errorf(
					"TX %s failed validation: %w",
					tx.Hash(),
					err,
				)
			}
			return nil
		}
		stillCurrent := func() bool {
			currentConsensus, currentTip := ls.loadStateSnapshots()
			return currentConsensus.generation == snapshot.generation &&
				currentTip.generation == snapshot.generation
		}
		return fn(validate, stillCurrent)
	})
}

// validateTxCore is the shared validation flow for ValidateTx and
// ValidateTxWithOverlay. It snapshots ledger state, resolves the
// validation era, opens a DB transaction, and invokes the era's
// ValidateTxFunc with a LedgerView built by the provided callback.
func (ls *LedgerState) validateTxCore(
	tx lcommon.Transaction,
	buildLV func(txn *database.Txn) *LedgerView,
) error {
	snapshot := ls.txValidationSnapshot()

	validationEra, err := resolveValidationEra(
		tx,
		snapshot.currentEra,
		snapshot.eraList,
	)
	if err != nil {
		return err
	}
	if validationEra.ValidateTxFunc != nil {
		pp := snapshot.currentPParams
		if validationEra.Id != snapshot.currentEra.Id &&
			snapshot.prevEraPParams != nil {
			pp = snapshot.prevEraPParams
		}
		txn := ls.db.Transaction(false)
		err := txn.Do(func(txn *database.Txn) error {
			lv := buildLV(txn).pinCommitteeState(snapshot.currentEpoch, pp)
			return validationEra.ValidateTxFunc(
				tx,
				snapshot.referenceSlot,
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
	// Snapshot mutable state from the lock-free consensus snapshot
	consensusState := ls.loadConsensusSnapshot()
	snapshotEra := consensusState.currentEra
	snapshotPParams := consensusState.currentPParams
	snapshotPrevEraPParams := consensusState.prevEraPParams

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
			lv := (&LedgerView{
				txn: txn,
				ls:  ls,
			}).pinCommitteeState(
				consensusState.currentEpoch.EpochId,
				pp,
			)
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

// persistTipAfterForgedBlock persists block as the new tip in the
// database. forgeBlock's ls.chain.AddBlock call only updates ls.chain's
// in-memory tip -- unlike the normal chainsync/forged-block batch
// pipeline (which calls db.SetTip as part of its own transaction once
// blocksProcessed > 0), forgeBlock has no other call that persists the
// new tip. Without this, database.GetTip (what dingoctl's `database
// info` reports, what a live Truncate uses as its deletion boundary, and
// what BlockForger's leader-election check reads via slotClock) never
// advances for a dev-mode-forged block. A block left in that state is
// physically written (blob + metadata block row, raw block_count keeps
// climbing) but invisible to anything relying on the persisted tip: a
// later Truncate computes its delete-range against the stale tip and
// can never reach this block, leaving it a permanent straggler that
// eventually surfaces as a "persistent chain index gap" error from the
// chain iterator (chain/chain.go) once something tries to walk past it.
func (ls *LedgerState) persistTipAfterForgedBlock(block ledger.Block) error {
	newTip := ochainsync.Tip{
		Point: ocommon.NewPoint(
			block.SlotNumber(),
			block.Hash().Bytes(),
		),
		BlockNumber: block.BlockNumber(),
	}
	return ls.db.SetTip(newTip, nil)
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

	// Compute the real block body hash so the CBOR round-trip decode
	// below (which validates the body hash against actual content)
	// succeeds instead of failing on every forged block.
	bodyHash, bodySize, err := forging.ComputeConwayBlockBodyHash(
		transactionBodies,
		transactionWitnessSets,
		metadataSet,
	)
	if err != nil {
		ls.config.Logger.Error(
			"failed to compute forged block body hash",
			"component", "ledger",
			"error", err,
		)
		return
	}
	// blockSize (checked per-transaction above while filling the mempool
	// candidate list) is only a running sum of each included transaction's
	// own raw CBOR length -- an underestimate of the real assembled block
	// body, which wraps separate arrays of transaction bodies, witness
	// sets, and a metadata map (see ledger/forging/builder.go's identical
	// final actualBlockBodySize check). Without this check here too, this
	// dev-mode path could forge and append a block whose real body size
	// exceeds MaxBlockBodySize despite every individual transaction having
	// passed the earlier, coarser check.
	if bodySize > maxBlockSize {
		ls.config.Logger.Error(
			"forged block body size exceeds MaxBlockBodySize, discarding block",
			"component", "ledger",
			"body_size", bodySize,
			"max_block_size", maxBlockSize,
		)
		return
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
		BlockBodySize: bodySize,
		BlockBodyHash: bodyHash,
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

	// Persist the new tip. See persistTipAfterForgedBlock's doc comment
	// for why this is required in addition to AddBlock above.
	if err := ls.persistTipAfterForgedBlock(ledgerBlock); err != nil {
		ls.config.Logger.Error(
			"failed to persist tip after forged block",
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
