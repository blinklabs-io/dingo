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
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	shelley "github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

var errRestartLedgerPipeline = errors.New(
	"restart ledger pipeline after local state recovery",
)

var errHaltLedgerPipeline = errors.New(
	"persistent tx validation failure after recovery attempts",
)

// errStaleChainIterator is returned by ledgerProcessBlock when a block's
// prev-hash doesn't match the current ledger tip. This signals that the
// chain iterator has been made stale by a concurrent rollback: the iterator's
// nextBlockIndex skipped ahead past the first new fork block and is now
// returning a block that extends a branch we are no longer on. The ledger
// pipeline must restart so its iterator rewinds to the current tip.
var errStaleChainIterator = errors.New(
	"block does not fit chain tip: stale iterator after rollback",
)

type txValidationError struct {
	BlockPoint ocommon.Point
	TxHash     []byte
	Inputs     []lcommon.TransactionInput
	Cause      error
}

func (e *txValidationError) Error() string {
	return fmt.Sprintf(
		"tx %s validation failure at slot %d: %v",
		hex.EncodeToString(e.TxHash),
		e.BlockPoint.Slot,
		e.Cause,
	)
}

func (e *txValidationError) Unwrap() error {
	return e.Cause
}

// maxAtTipRecoveryAttempts caps the depth schedule for recovering from the
// same persistent at-tip validation failure. Each attempt rewinds the primary
// chain progressively deeper to give chainselection room to pick a different
// fork. After the cap, recovery keeps retrying at the deepest rewind depth and
// relies on ChainSync peer rotation to find a valid candidate chain.
const maxAtTipRecoveryAttempts = 3

// maxAtTipRecoveryDescents caps how many consecutive *distinct* at-tip failures
// may fail to advance (each at a slot at or below the previous distinct
// failure) before at-tip recovery declares itself non-converging and stops
// rewinding the primary chain deeper. Distinct failures each reset the
// same-block escalation to attempt 1, so without this bound the escalate-and-cap
// logic never engages and the primary chain is rewound a stability window
// deeper every cycle, falling unboundedly behind the wall clock (issue #2939).
// Once tripped, recovery holds at the ledger tip until the ledger makes forward
// progress past the failing region, relying on ChainSync re-delivery rather
// than a destructive descent that no rewind can fix (e.g. a local
// false-positive validation rejection).
const maxAtTipRecoveryDescents = 2

// maxReplayRecoveryNoProgress caps consecutive unresolved-producer replay
// recoveries that rebuild only to the same (or an older) applied ledger tip.
// The failing block may change and creep forward while the applied high-water
// mark remains fixed, so the ledger tip—not the failure identity—is the
// convergence signal. Once tripped, recovery stops pruning another
// security-parameter window and holds at the applied tip while forcing a fresh
// ChainSync connection (issue #3005).
const maxReplayRecoveryNoProgress = 2

type atTipRecoveryAttempt struct {
	BlockPoint ocommon.Point
	TxHash     []byte
	Attempts   int
}

func newAtTipRecoveryAttempt(
	validationErr *txValidationError,
) *atTipRecoveryAttempt {
	blockPoint := validationErr.BlockPoint
	blockPoint.Hash = append([]byte(nil), blockPoint.Hash...)
	return &atTipRecoveryAttempt{
		BlockPoint: blockPoint,
		TxHash:     append([]byte(nil), validationErr.TxHash...),
		Attempts:   1,
	}
}

func (a *atTipRecoveryAttempt) matches(
	validationErr *txValidationError,
) bool {
	return a != nil &&
		validationErr.BlockPoint.Slot == a.BlockPoint.Slot &&
		bytes.Equal(validationErr.BlockPoint.Hash, a.BlockPoint.Hash) &&
		bytes.Equal(validationErr.TxHash, a.TxHash)
}

type replayRecoveryCandidate struct {
	Input         lcommon.TransactionInput
	ProducerTx    *models.Transaction
	ProducerBlock models.Block
	RollbackPoint ocommon.Point
	Strategy      string
	// ProducerUnresolved distinguishes the security-parameter fallback from
	// strategies that found a concrete producer. Strategy remains a log label.
	ProducerUnresolved bool
}

type replayRecoveryPendingInput struct {
	Input   lcommon.TransactionInput
	MaxSlot uint64
}

type replayRecoveryResolvedProducer struct {
	Input         lcommon.TransactionInput
	ProducerTx    *models.Transaction
	ProducerBlock models.Block
	Tx            lcommon.Transaction
	Strategy      string
}

type replayRecoveryChainIndex struct {
	Txs         map[string]replayRecoveryChainTx
	OldestBlock *models.Block
}

type replayRecoveryChainTx struct {
	Block models.Block
	Tx    lcommon.Transaction
}

func collectReferencedInputs(
	tx lcommon.Transaction,
) []lcommon.TransactionInput {
	var ret []lcommon.TransactionInput
	seen := make(map[string]struct{})
	appendInputs := func(inputs []lcommon.TransactionInput) {
		for _, input := range inputs {
			key := input.String()
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			ret = append(ret, input)
		}
	}
	appendInputs(tx.Inputs())
	appendInputs(tx.Collateral())
	appendInputs(tx.ReferenceInputs())
	return ret
}

func (ls *LedgerState) tryRecoverFromTxValidationError(
	err error,
) (bool, error) {
	var validationErr *txValidationError
	if !errors.As(err, &validationErr) {
		return false, nil
	}
	if isDeterministicTxValidationError(validationErr.Cause) {
		return ls.recoverFromDeterministicTxValidationError(validationErr)
	}
	if ls.IsAtTip() {
		return ls.recoverAtTipFromTxValidationError(validationErr)
	}
	candidate, err := ls.findReplayRecoveryCandidate(validationErr)
	if err != nil {
		return false, err
	}
	if candidate == nil {
		return false, nil
	}
	producerTxHash := candidate.Input.Id().String()
	if candidate.ProducerTx != nil {
		producerTxHash = hex.EncodeToString(candidate.ProducerTx.Hash)
	}
	if ls.recoveryRollbackExceedsMithrilBoundary(candidate.RollbackPoint) {
		if err := ls.rejectReplayRecoveryAtMithrilBoundary(
			validationErr,
			candidate,
			producerTxHash,
		); err != nil {
			return false, err
		}
		return true, nil
	}
	rewindPrimaryChain := candidate.ProducerUnresolved &&
		ls.config.ChainManager != nil
	rewindPoint := candidate.RollbackPoint
	replayHolding := false
	primaryChainAlreadyHeld := false
	var ledgerTip ochainsync.Tip
	if rewindPrimaryChain {
		ls.RLock()
		ledgerTip = ls.currentTip
		ls.RUnlock()
		replayHolding = ls.observeReplayRecoveryTip(ledgerTip.Point.Slot)
		if replayHolding {
			rewindPoint = ledgerTip.Point
			// The applied high-water mark can trail currentTip when a prior
			// slot-based rollback left the tip above durable state. Holding at
			// that decoupled tip repeats the same recovery loop; use the durable
			// floor only when it is also on the current primary chain.
			floor, ok, ferr := ls.durableAppliedFloor()
			if ferr != nil {
				return false, fmt.Errorf(
					"determine durable applied floor for replay hold: %w",
					ferr,
				)
			}
			if ok {
				onChain, ferr := ls.primaryChainContainsPoint(floor)
				if ferr != nil {
					return false, fmt.Errorf(
						"check durable applied floor on primary chain: %w",
						ferr,
					)
				}
				if onChain && (floor.Slot < rewindPoint.Slot ||
					(floor.Slot == rewindPoint.Slot &&
						!bytes.Equal(floor.Hash, rewindPoint.Hash))) {
					rewindPoint = floor
				}
			}
			primaryChainAlreadyHeld = ls.chain.Tip().Point.Slot < rewindPoint.Slot
		}
	}
	recoveryAction := "rewinding metadata state"
	if rewindPrimaryChain {
		recoveryAction = "rewinding primary chain and metadata state"
	}
	ls.config.Logger.Warn(
		"detected inconsistent local ledger state during replay, "+recoveryAction,
		"component",
		"ledger",
		"recovery_strategy",
		candidate.Strategy,
		"tx_hash",
		hex.EncodeToString(validationErr.TxHash),
		"failing_block_slot",
		validationErr.BlockPoint.Slot,
		"missing_input",
		candidate.Input.String(),
		"producer_tx_hash",
		producerTxHash,
		"producer_block_slot",
		candidate.ProducerBlock.Slot,
		"rollback_slot",
		rewindPoint.Slot,
		"rollback_hash",
		hex.EncodeToString(rewindPoint.Hash),
		"holding",
		replayHolding,
	)
	if replayHolding {
		ls.metrics.replayRecoveryNonConverging.Inc()
		ls.config.Logger.Warn(
			"replay recovery not converging after unresolved-producer failures, holding at applied ledger tip",
			"component",
			"ledger",
			"tx_hash",
			hex.EncodeToString(validationErr.TxHash),
			"failing_block_slot",
			validationErr.BlockPoint.Slot,
			"ledger_tip_slot",
			ledgerTip.Point.Slot,
			"ledger_tip_hash",
			hex.EncodeToString(ledgerTip.Point.Hash),
			"primary_chain_tip_slot",
			ls.chain.Tip().Point.Slot,
			"primary_chain_already_held",
			primaryChainAlreadyHeld,
			"no_progress_count",
			ls.replayRecoveryNoProgressCount,
			"fallback_rollback_slot",
			candidate.RollbackPoint.Slot,
			"hint",
			"forcing a fresh chainsync intersection; persistent failures may require operator intervention",
		)
		// Peer rotation is the escape mechanism for a held recovery. Publish
		// it before the rollback calls so an unexpected local rollback error
		// cannot suppress the fresh ChainSync intersection.
		ls.publishReplayRecoveryNonConvergingResync(rewindPoint)
	}
	primaryChainRewound := false
	if rewindPrimaryChain && !primaryChainAlreadyHeld {
		if err := ls.rollbackPrimaryChainInSecurityParamWindows(
			rewindPoint,
		); err != nil {
			return false, fmt.Errorf(
				"rewind primary chain for replay recovery: %w",
				err,
			)
		}
		primaryChainRewound = true
	}
	// The chain moves first while the rollback anchor is guaranteed to remain
	// available. If metadata synchronization fails, the primary chain is still
	// at a valid retained point and the standard divergence reconciler can
	// finish rolling metadata back to its common ancestor.
	if err := ls.rollback(rewindPoint); err != nil {
		return false, fmt.Errorf(
			"rollback ledger state for replay recovery: %w",
			err,
		)
	}
	// Arm only when the corrective primary-chain rewind actually happened and
	// metadata now sits at that same point. A replay target ahead of the
	// applied tip would otherwise make the audit treat an unapplied gap as a
	// cross-fork continuation and produce false diagnostics.
	if primaryChainRewound &&
		pointMatches(ls.chain.Tip().Point, rewindPoint) &&
		pointMatches(ls.Tip().Point, rewindPoint) {
		ls.armContinuationAudit(rewindPoint, "replay recovery rewind")
	}
	return true, nil
}

// isDeterministicTxValidationError identifies transaction-structure failures
// that cannot be repaired by replaying a different local UTxO history. A
// duplicate input is invalid regardless of which chain produced the input,
// so treating it as an unresolved producer sends recovery down the fallback
// path and can repeatedly rediscover the same rejected block.
func isDeterministicTxValidationError(err error) bool {
	var duplicateInputErr shelley.DuplicateInputError
	return errors.As(err, &duplicateInputErr)
}

// recoverFromDeterministicTxValidationError drops a primary-chain block that
// contains a transaction with a deterministic structural error. The ledger
// tip is the last applied good point; rewinding both stores to it rejects the
// branch and lets ChainSync obtain a fresh intersection. This is deliberately
// separate from unresolved-input recovery: the latter is state-dependent and
// still needs producer resolution and the security-parameter fallback.
func (ls *LedgerState) recoverFromDeterministicTxValidationError(
	validationErr *txValidationError,
) (bool, error) {
	if ls.chain == nil || ls.config.ChainManager == nil {
		return false, nil
	}

	ls.RLock()
	ledgerTip := ls.currentTip
	ls.RUnlock()
	rewindPoint := ledgerTip.Point
	if rewindPoint.Slot >= validationErr.BlockPoint.Slot {
		if ls.config.Logger != nil {
			ls.config.Logger.Warn(
				"deterministic transaction validation rejected a block at or behind the ledger tip; no rewind target precedes it",
				"component", "ledger",
				"failing_block_slot", validationErr.BlockPoint.Slot,
				"ledger_tip_slot", rewindPoint.Slot,
				"error", validationErr.Cause,
			)
		}
		return false, nil
	}
	if ls.recoveryRollbackExceedsMithrilBoundary(rewindPoint) {
		return false, nil
	}

	// Chain selection can abandon the ledger tip between the snapshot above
	// and the rewind. If the point is already gone, the rejected block was
	// removed by that chain choice and the pipeline can safely restart.
	if err := ls.chain.ValidateRollback(rewindPoint); err != nil &&
		errors.Is(err, chain.ErrRollbackPointNotOnChain) {
		if ls.config.Logger != nil {
			ls.config.Logger.Warn(
				"chain selection moved the primary chain off the deterministic transaction recovery point; the rejected block is already gone",
				"component", "ledger",
				"failing_block_slot", validationErr.BlockPoint.Slot,
				"rewind_target_slot", rewindPoint.Slot,
				"error", err,
			)
		}
		return true, nil
	}

	if ls.config.Logger != nil {
		ls.config.Logger.Warn(
			"deterministic transaction validation rejected a block on the primary chain; rewinding so chain selection can offer another candidate",
			"component", "ledger",
			"tx_hash", hex.EncodeToString(validationErr.TxHash),
			"failing_block_slot", validationErr.BlockPoint.Slot,
			"rewind_target_slot", rewindPoint.Slot,
			"rewind_target_hash", hex.EncodeToString(rewindPoint.Hash),
			"error", validationErr.Cause,
		)
	}
	if err := ls.rollbackPrimaryChainInSecurityParamWindows(rewindPoint); err != nil {
		if errors.Is(err, chain.ErrRollbackPointNotOnChain) {
			return true, nil
		}
		return false, fmt.Errorf(
			"rewind primary chain after deterministic transaction validation failure: %w",
			err,
		)
	}
	if err := ls.rollback(rewindPoint); err != nil {
		return false, fmt.Errorf(
			"rollback ledger state after deterministic transaction validation failure: %w",
			err,
		)
	}
	if ls.config.EventBus != nil {
		ls.config.EventBus.Publish(
			event.ChainsyncResyncEventType,
			event.NewEvent(
				event.ChainsyncResyncEventType,
				event.ChainsyncResyncEvent{
					Reason: event.
						ChainsyncResyncReasonDeterministicTxValidationRecovery,
					Point: rewindPoint,
				},
			),
		)
	}
	return true, nil
}

// observeReplayRecoveryTip records the applied tip seen immediately before an
// unresolved-producer fallback. Recovery is non-converging when repeated
// attempts fail to exceed the first observed high-water mark, even if peers
// offer different failing blocks at slightly higher slots each cycle.
func (ls *LedgerState) observeReplayRecoveryTip(tipSlot uint64) bool {
	if !ls.replayRecoveryTipTracked {
		ls.replayRecoveryTipTracked = true
		ls.replayRecoveryHighWaterSlot = tipSlot
		return false
	}
	if tipSlot <= ls.replayRecoveryHighWaterSlot {
		ls.replayRecoveryNoProgressCount++
	} else {
		ls.replayRecoveryHighWaterSlot = tipSlot
		ls.replayRecoveryNoProgressCount = 0
		ls.replayRecoveryHolding = false
	}
	if ls.replayRecoveryNoProgressCount >= maxReplayRecoveryNoProgress {
		ls.replayRecoveryHolding = true
	}
	return ls.replayRecoveryHolding
}

// resetReplayRecoveryNonProgress clears a replay hold only after block
// application advances beyond the high-water mark that recovery could not
// cross. Replaying blocks back up to that exact point is not progress.
func (ls *LedgerState) resetReplayRecoveryNonProgress(newTipSlot uint64) {
	if !ls.replayRecoveryTipTracked ||
		newTipSlot <= ls.replayRecoveryHighWaterSlot {
		return
	}
	ls.replayRecoveryTipTracked = false
	ls.replayRecoveryHighWaterSlot = 0
	ls.replayRecoveryNoProgressCount = 0
	ls.replayRecoveryHolding = false
}

func (ls *LedgerState) publishReplayRecoveryNonConvergingResync(
	point ocommon.Point,
) {
	if ls.config.EventBus == nil {
		return
	}
	var activeConnId ouroboros.ConnectionId
	if ls.config.GetActiveConnectionFunc != nil {
		if connId := ls.config.GetActiveConnectionFunc(); connId != nil {
			activeConnId = *connId
		}
	}
	ls.config.EventBus.Publish(
		event.ChainsyncResyncEventType,
		event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{
				ConnectionId: activeConnId,
				Reason: event.
					ChainsyncResyncReasonReplayRecoveryNonConverging,
				Point: point,
			},
		),
	)
}

// rollbackPrimaryChainInSecurityParamWindows reaches a deep corrective rewind
// by applying a sequence of ordinary, security-parameter-bounded rollbacks.
// Each step therefore preserves ChainRollbackEvent's k-bounded contract and
// bounds the blocks retained for event delivery. A failure leaves the chain at
// the last committed intermediate point; startup/live reconciliation can then
// replay or finish rolling metadata back to that valid primary-chain tip.
func (ls *LedgerState) rollbackPrimaryChainInSecurityParamWindows(
	point ocommon.Point,
) error {
	securityParam := ls.SecurityParam()
	if securityParam <= 0 {
		return chain.ErrSecurityParamNotConfigured
	}

	targetIndex := uint64(0)
	if point.Slot > 0 || len(point.Hash) > 0 {
		targetBlock, err := database.BlockByPoint(ls.db, point)
		if err != nil {
			return fmt.Errorf("lookup replay recovery target: %w", err)
		}
		targetIndex = targetBlock.ID
	}

	tip := ls.chain.Tip()
	if tip.Point.Slot == point.Slot &&
		bytes.Equal(tip.Point.Hash, point.Hash) {
		return nil
	}
	tipBlock, err := database.BlockByPoint(ls.db, tip.Point)
	if err != nil {
		return fmt.Errorf("lookup primary chain tip: %w", err)
	}
	tipIndex := tipBlock.ID
	if tipIndex < targetIndex {
		return fmt.Errorf(
			"primary chain tip index %d is behind replay recovery target %d",
			tipIndex,
			targetIndex,
		)
	}
	window := uint64(securityParam)
	for tipIndex-targetIndex > window {
		nextIndex := tipIndex - window
		nextBlock, err := ls.db.BlockByIndex(nextIndex, nil)
		if err != nil {
			return fmt.Errorf(
				"lookup intermediate replay recovery block %d: %w",
				nextIndex,
				err,
			)
		}
		nextPoint := ocommon.NewPoint(nextBlock.Slot, nextBlock.Hash)
		// Validate, then emit undo events, before each window's
		// truncation. This function's own doc comment notes the chain can
		// move between reading the tip and rewinding to it, so the
		// rejection here is reachable, not theoretical: emitting without
		// it would tell subscribers to undo blocks the failed rewind
		// leaves applied. See validateAndEmitRollbackUndo.
		if err := ls.validateAndEmitRollbackUndo(nextPoint); err != nil {
			return fmt.Errorf(
				"rollback primary chain to intermediate point %d: %w",
				nextIndex,
				err,
			)
		}
		if err := ls.chain.Rollback(nextPoint); err != nil {
			return fmt.Errorf(
				"rollback primary chain to intermediate point %d: %w",
				nextIndex,
				err,
			)
		}
		tipIndex = nextIndex
	}
	if err := ls.validateAndEmitRollbackUndo(point); err != nil {
		return fmt.Errorf("rollback primary chain to recovery point: %w", err)
	}
	if err := ls.chain.Rollback(point); err != nil {
		return fmt.Errorf("rollback primary chain to recovery point: %w", err)
	}
	return nil
}

func (ls *LedgerState) recoveryRollbackExceedsMithrilBoundary(
	point ocommon.Point,
) bool {
	ls.RLock()
	defer ls.RUnlock()
	return ls.mithrilLedgerSlot > 0 && point.Slot < ls.mithrilLedgerSlot
}

func (ls *LedgerState) rejectReplayRecoveryAtMithrilBoundary(
	validationErr *txValidationError,
	candidate *replayRecoveryCandidate,
	producerTxHash string,
) error {
	return ls.rejectRecoveryAtMithrilBoundary(
		"replay recovery rollback exceeds Mithril trust boundary",
		func(mithrilLedgerSlot uint64, rewindPoint ocommon.Point) {
			ls.config.Logger.Warn(
				"detected replay recovery below Mithril trust boundary, rejecting peer chain",
				"component",
				"ledger",
				"recovery_strategy",
				candidate.Strategy,
				"tx_hash",
				hex.EncodeToString(validationErr.TxHash),
				"failing_block_slot",
				validationErr.BlockPoint.Slot,
				"missing_input",
				candidate.Input.String(),
				"producer_tx_hash",
				producerTxHash,
				"producer_block_slot",
				candidate.ProducerBlock.Slot,
				"rollback_slot",
				candidate.RollbackPoint.Slot,
				"rollback_hash",
				hex.EncodeToString(candidate.RollbackPoint.Hash),
				"mithril_ledger_slot",
				mithrilLedgerSlot,
				"rewind_target_slot",
				rewindPoint.Slot,
				"rewind_target_hash",
				hex.EncodeToString(rewindPoint.Hash),
			)
		},
	)
}

// resetAtTipRecoveryDescent clears the non-convergence descent tracking once
// the ledger has made forward progress past the failing region. Called from the
// block-apply success path when the tip advances beyond the last recorded
// at-tip failure slot, so a later, unrelated at-tip failure starts with a fresh
// recovery budget instead of inheriting a stale hold. Runs on the ledger
// pipeline goroutine, the same goroutine that mutates these fields during
// recovery, so no additional locking is required.
func (ls *LedgerState) resetAtTipRecoveryDescent(newTipSlot uint64) {
	if ls.atTipRecoveryLastFailSlot == 0 {
		return
	}
	if newTipSlot <= ls.atTipRecoveryLastFailSlot {
		return
	}
	ls.atTipRecoveryLastFailSlot = 0
	ls.atTipRecoveryDescentCount = 0
	ls.atTipRecoveryHolding = false
}

func (ls *LedgerState) recoverAtTipFromTxValidationError(
	validationErr *txValidationError,
) (bool, error) {
	if ls.chain == nil || ls.config.ChainManager == nil {
		return false, nil
	}
	// Determine the rewind target. On the first attempt, rewind to the
	// authoritative ledger tip — the simplest case where chainselection
	// can re-pick from another peer with a compatible chain. On
	// subsequent attempts the same (block, tx) failure means the
	// rewind-to-tip didn't help: peers keep replaying the same losing
	// fork because chainselection's intersection point is still on it.
	// Rewind progressively deeper to expose a wider candidate set, up
	// to the era's stability window. Only halt if even the deepest
	// rewind has been tried.
	isSameFailure := ls.lastAtTipRecovery != nil &&
		ls.lastAtTipRecovery.matches(validationErr)
	attempts := 1
	if isSameFailure {
		attempts = ls.lastAtTipRecovery.Attempts + 1
		if attempts > maxAtTipRecoveryAttempts {
			ls.config.Logger.Warn(
				"at-tip recovery exhausted scheduled rewind attempts, retrying with deepest rewind",
				"component",
				"ledger",
				"failing_slot",
				validationErr.BlockPoint.Slot,
				"failing_block_hash",
				hex.EncodeToString(
					validationErr.BlockPoint.Hash,
				),
				"tx_hash",
				hex.EncodeToString(validationErr.TxHash),
				"attempts",
				attempts,
			)
			attempts = maxAtTipRecoveryAttempts
		}
	}
	// Track non-convergence across DISTINCT failures. Each distinct (block,
	// tx) failure resets the same-block escalation above to attempt 1, so a
	// descending series would otherwise rewind the primary chain a stability
	// window deeper every cycle without ever hitting the escalation cap. When
	// a distinct failure does not advance beyond the previous distinct
	// failure's slot, count it as a descent; once enough accumulate, latch
	// into a hold-at-tip mode that suppresses deep rewinds. The latch clears
	// only when the ledger makes forward progress (see
	// resetAtTipRecoveryDescent, called from the block-apply success path).
	if !isSameFailure {
		if ls.atTipRecoveryLastFailSlot != 0 &&
			validationErr.BlockPoint.Slot <= ls.atTipRecoveryLastFailSlot {
			ls.atTipRecoveryDescentCount++
		} else {
			ls.atTipRecoveryDescentCount = 0
			ls.atTipRecoveryHolding = false
		}
		ls.atTipRecoveryLastFailSlot = validationErr.BlockPoint.Slot
	}
	if ls.atTipRecoveryDescentCount >= maxAtTipRecoveryDescents {
		ls.atTipRecoveryHolding = true
	}
	ls.lastAtTipRecovery = newAtTipRecoveryAttempt(validationErr)
	ls.lastAtTipRecovery.Attempts = attempts
	ls.RLock()
	ledgerTip := ls.currentTip
	ls.RUnlock()
	chainTip := ls.chain.Tip()
	// Compute the rewind target. Depth grows linearly with each retry,
	// capped at the era stability window so we never undo an immutable block.
	// While holding (non-converging descent detected), deep rewinds are
	// suppressed entirely: we rewind only to the ledger tip so the primary
	// chain stops descending and ChainSync can re-deliver, avoiding the
	// unbounded staircase of issue #2939.
	rewindPoint := ledgerTip.Point
	if attempts > 1 && !ls.atTipRecoveryHolding {
		stabilityWindow := ls.calculateStabilityWindow()
		// depth grows linearly so the final attempt reaches the full
		// stability window: (attempts-1)/(maxAttempts-1) of the window.
		depth := stabilityWindow * uint64(attempts-1) /
			uint64(maxAtTipRecoveryAttempts-1)
		if depth > 0 && depth < ledgerTip.Point.Slot {
			rewindSlot := ledgerTip.Point.Slot - depth
			deeperPoint, lookupErr := ls.findRewindPoint(rewindSlot)
			if lookupErr == nil {
				rewindPoint = deeperPoint
			} else {
				ls.config.Logger.Warn(
					"deep rewind lookup failed, using ledger tip",
					"component", "ledger",
					"target_slot", rewindSlot,
					"error", lookupErr.Error(),
				)
			}
		}
	}
	if ls.recoveryRollbackExceedsMithrilBoundary(rewindPoint) {
		if err := ls.rejectAtTipRecoveryAtMithrilBoundary(
			validationErr,
			ledgerTip,
			chainTip,
			rewindPoint,
			attempts,
		); err != nil {
			return false, err
		}
		return true, nil
	}
	if ls.atTipRecoveryHolding {
		ls.metrics.atTipRecoveryNonConverging.Inc()
		ls.config.Logger.Warn(
			"at-tip recovery not converging across distinct validation failures, holding at ledger tip instead of rewinding primary chain deeper",
			"component",
			"ledger",
			"tx_hash",
			hex.EncodeToString(validationErr.TxHash),
			"failing_block_slot",
			validationErr.BlockPoint.Slot,
			"ledger_tip_slot",
			ledgerTip.Point.Slot,
			"ledger_tip_hash",
			hex.EncodeToString(ledgerTip.Point.Hash),
			"primary_chain_tip_slot",
			chainTip.Point.Slot,
			"descent_count",
			ls.atTipRecoveryDescentCount,
			"hint",
			"local ledger validation likely diverging from the network; operator intervention may be required",
		)
	}
	ls.config.Logger.Warn(
		"validation failure after reaching tip, rewinding primary chain",
		"component", "ledger",
		"tx_hash", hex.EncodeToString(validationErr.TxHash),
		"failing_block_slot", validationErr.BlockPoint.Slot,
		"ledger_tip_slot", ledgerTip.Point.Slot,
		"ledger_tip_hash", hex.EncodeToString(ledgerTip.Point.Hash),
		"primary_chain_tip_slot", chainTip.Point.Slot,
		"primary_chain_tip_hash", hex.EncodeToString(chainTip.Point.Hash),
		"rewind_target_slot", rewindPoint.Slot,
		"attempt", attempts,
		"holding", ls.atTipRecoveryHolding,
	)
	if err := ls.rollbackPrimaryChainInSecurityParamWindows(
		rewindPoint,
	); err != nil {
		return false, fmt.Errorf(
			"rewind primary chain after validation failure: %w",
			err,
		)
	}
	// Roll back the ledger metadata state to the rewind point. Without
	// this, the chain is pruned to rewindPoint but the UTxO database
	// still reflects the failing block's post-apply state — consumed
	// inputs stay consumed, created outputs stay created. When peers
	// re-deliver the block we just rewound past, ledger validation
	// looks up its inputs, finds them already marked consumed, and
	// returns "rule 22 bad input(s) ... rule 24 value not conserved
	// (consumed 0)" again, looping the recovery indefinitely until
	// process restart. Primary-chain rollback only touches the chain
	// store — the matching ledger rollback must be explicit.
	if err := ls.rollback(rewindPoint); err != nil {
		return false, fmt.Errorf(
			"rollback ledger state after validation failure: %w",
			err,
		)
	}
	if ls.config.EventBus != nil {
		ls.config.EventBus.Publish(
			event.ChainsyncResyncEventType,
			event.NewEvent(
				event.ChainsyncResyncEventType,
				event.ChainsyncResyncEvent{
					Reason: event.ChainsyncResyncReasonLiveTxValidationRecovery,
					Point:  rewindPoint,
				},
			),
		)
	}
	return true, nil
}

func (ls *LedgerState) rejectAtTipRecoveryAtMithrilBoundary(
	validationErr *txValidationError,
	ledgerTip ochainsync.Tip,
	chainTip ochainsync.Tip,
	requestedRewindPoint ocommon.Point,
	attempts int,
) error {
	return ls.rejectRecoveryAtMithrilBoundary(
		"at-tip recovery rollback exceeds Mithril trust boundary",
		func(mithrilLedgerSlot uint64, rewindPoint ocommon.Point) {
			ls.config.Logger.Warn(
				"at-tip validation recovery would cross Mithril trust boundary, rejecting peer chain",
				"component",
				"ledger",
				"tx_hash",
				hex.EncodeToString(validationErr.TxHash),
				"failing_block_slot",
				validationErr.BlockPoint.Slot,
				"failing_block_hash",
				hex.EncodeToString(validationErr.BlockPoint.Hash),
				"ledger_tip_slot",
				ledgerTip.Point.Slot,
				"ledger_tip_hash",
				hex.EncodeToString(ledgerTip.Point.Hash),
				"primary_chain_tip_slot",
				chainTip.Point.Slot,
				"primary_chain_tip_hash",
				hex.EncodeToString(chainTip.Point.Hash),
				"requested_rewind_slot",
				requestedRewindPoint.Slot,
				"requested_rewind_hash",
				hex.EncodeToString(requestedRewindPoint.Hash),
				"mithril_ledger_slot",
				mithrilLedgerSlot,
				"rewind_target_slot",
				rewindPoint.Slot,
				"rewind_target_hash",
				hex.EncodeToString(rewindPoint.Hash),
				"attempt",
				attempts,
			)
		},
	)
}

func (ls *LedgerState) rejectRecoveryAtMithrilBoundary(
	errContext string,
	logRejection func(mithrilLedgerSlot uint64, rewindPoint ocommon.Point),
) error {
	ls.RLock()
	mithrilLedgerSlot := ls.mithrilLedgerSlot
	rewindPoint := ls.currentTip.Point
	ls.RUnlock()
	if ls.config.ChainManager == nil {
		return fmt.Errorf(
			"%s: %w",
			errContext,
			ErrRollbackExceedsMithrilBoundary,
		)
	}
	logRejection(mithrilLedgerSlot, rewindPoint)
	if err := ls.rollbackPrimaryChainInSecurityParamWindows(rewindPoint); err != nil {
		return fmt.Errorf(
			"rewind primary chain to Mithril trust boundary: %w",
			err,
		)
	}
	ls.chainsyncMutex.Lock()
	ls.resetChainsyncResyncState()
	ls.setChainsyncState(SyncingChainsyncState)
	ls.chainsyncMutex.Unlock()
	if ls.config.EventBus != nil {
		var activeConnId ouroboros.ConnectionId
		if ls.config.GetActiveConnectionFunc != nil {
			if connId := ls.config.GetActiveConnectionFunc(); connId != nil {
				activeConnId = *connId
			}
		}
		ls.config.EventBus.Publish(
			event.ChainsyncResyncEventType,
			event.NewEvent(
				event.ChainsyncResyncEventType,
				event.ChainsyncResyncEvent{
					ConnectionId: activeConnId,
					Reason: event.
						ChainsyncResyncReasonRollbackExceedsMithril,
					Point: rewindPoint,
				},
			),
		)
	}
	return nil
}

// findRewindPoint returns the highest committed chain point at or
// below targetSlot, used to compute deeper rewind anchors during
// at-tip validation recovery. Falls back to slot 0 if no earlier
// committed block can be located.
func (ls *LedgerState) findRewindPoint(
	targetSlot uint64,
) (ocommon.Point, error) {
	if ls.chain == nil {
		return ocommon.Point{Slot: targetSlot}, nil
	}
	block, err := database.BlockBeforeSlot(ls.db, targetSlot+1)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return ocommon.Point{}, nil
		}
		return ocommon.Point{}, err
	}
	return ocommon.Point{Slot: block.Slot, Hash: block.Hash}, nil
}

func (ls *LedgerState) findReplayRecoveryCandidate(
	validationErr *txValidationError,
) (*replayRecoveryCandidate, error) {
	chainIndex, err := ls.buildReplayRecoveryChainIndex(
		validationErr.BlockPoint,
	)
	if err != nil {
		return nil, err
	}
	var candidate *replayRecoveryCandidate
	var unresolvedInputs []lcommon.TransactionInput
	pendingInputs := make(
		[]replayRecoveryPendingInput,
		0,
		len(validationErr.Inputs),
	)
	for _, input := range validationErr.Inputs {
		pendingInputs = append(pendingInputs, replayRecoveryPendingInput{
			Input:   input,
			MaxSlot: validationErr.BlockPoint.Slot,
		})
	}
	seenInputs := make(map[string]struct{})
	expandedTxs := make(map[string]struct{})
	for len(pendingInputs) > 0 {
		pending := pendingInputs[0]
		pendingInputs = pendingInputs[1:]
		inputKey := pending.Input.String()
		if _, ok := seenInputs[inputKey]; ok {
			continue
		}
		seenInputs[inputKey] = struct{}{}
		resolved, err := ls.resolveReplayRecoveryProducer(
			pending,
			chainIndex,
		)
		if err != nil {
			return nil, err
		}
		if resolved == nil {
			unresolvedInputs = append(unresolvedInputs, pending.Input)
			continue
		}
		rollbackPoint, err := ls.replayRecoveryParentPoint(
			resolved.ProducerBlock,
		)
		if err != nil {
			return nil, err
		}
		if candidate == nil ||
			resolved.ProducerBlock.Slot < candidate.ProducerBlock.Slot {
			candidate = &replayRecoveryCandidate{
				Input:         resolved.Input,
				ProducerTx:    resolved.ProducerTx,
				ProducerBlock: resolved.ProducerBlock,
				RollbackPoint: rollbackPoint,
				Strategy:      resolved.Strategy,
			}
		}
		if resolved.Tx == nil {
			continue
		}
		txKey := string(resolved.Tx.Hash().Bytes())
		if _, ok := expandedTxs[txKey]; ok {
			continue
		}
		expandedTxs[txKey] = struct{}{}
		for _, depInput := range collectReferencedInputs(resolved.Tx) {
			pendingInputs = append(pendingInputs, replayRecoveryPendingInput{
				Input:   depInput,
				MaxSlot: resolved.ProducerBlock.Slot,
			})
		}
	}
	if len(unresolvedInputs) > 0 {
		fallbackCandidate, err := ls.replayRecoveryFallbackCandidate(
			validationErr.BlockPoint,
			unresolvedInputs,
		)
		if err != nil {
			return nil, err
		}
		if fallbackCandidate != nil && (candidate == nil ||
			fallbackCandidate.ProducerBlock.Slot < candidate.ProducerBlock.Slot) {
			candidate = fallbackCandidate
		}
	}
	return candidate, nil
}

func (ls *LedgerState) buildReplayRecoveryChainIndex(
	failingPoint ocommon.Point,
) (*replayRecoveryChainIndex, error) {
	failingBlock, err := database.BlockByPoint(ls.db, failingPoint)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return &replayRecoveryChainIndex{
				Txs: make(map[string]replayRecoveryChainTx),
			}, nil
		}
		return nil, fmt.Errorf(
			"lookup failing block %x at slot %d for replay recovery: %w",
			failingPoint.Hash,
			failingPoint.Slot,
			err,
		)
	}
	index := &replayRecoveryChainIndex{
		Txs: make(map[string]replayRecoveryChainTx),
	}
	if failingBlock.ID <= database.BlockInitialIndex {
		return index, nil
	}
	const maxReplayRecoveryScanBlocks = 4096
	scanned := 0
	for blockIndex := failingBlock.ID - 1; ; blockIndex-- {
		if scanned >= maxReplayRecoveryScanBlocks {
			break
		}
		block, err := ls.db.BlockByIndex(blockIndex, nil)
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				if blockIndex == database.BlockInitialIndex {
					break
				}
				continue
			}
			return nil, fmt.Errorf(
				"lookup block %d during replay recovery scan: %w",
				blockIndex,
				err,
			)
		}
		if block.Slot >= failingPoint.Slot {
			if blockIndex == database.BlockInitialIndex {
				break
			}
			continue
		}
		index.OldestBlock = &block
		decodedBlock, err := block.Decode()
		if err != nil {
			ls.config.Logger.Debug(
				"skipping undecodable block during replay recovery scan",
				"component", "ledger",
				"block_slot", block.Slot,
				"block_hash", hex.EncodeToString(block.Hash),
				"error", err,
			)
			if blockIndex == database.BlockInitialIndex {
				break
			}
			scanned++
			continue
		}
		for _, tx := range decodedBlock.Transactions() {
			txKey := string(tx.Hash().Bytes())
			if _, ok := index.Txs[txKey]; ok {
				continue
			}
			index.Txs[txKey] = replayRecoveryChainTx{
				Block: block,
				Tx:    tx,
			}
		}
		scanned++
		if blockIndex == database.BlockInitialIndex {
			break
		}
	}
	return index, nil
}

func (ls *LedgerState) resolveReplayRecoveryProducer(
	pending replayRecoveryPendingInput,
	chainIndex *replayRecoveryChainIndex,
) (*replayRecoveryResolvedProducer, error) {
	utxo, err := ls.db.UtxoByRef(
		pending.Input.Id().Bytes(),
		pending.Input.Index(),
		nil,
	)
	if err != nil && !errors.Is(err, database.ErrUtxoNotFound) {
		return nil, fmt.Errorf(
			"lookup validation input %s: %w",
			pending.Input.String(),
			err,
		)
	}
	if utxo != nil {
		return nil, nil
	}
	producerTx, err := ls.db.GetTransactionByHash(
		pending.Input.Id().Bytes(),
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"lookup producer tx %s: %w",
			pending.Input.Id().String(),
			err,
		)
	}
	if producerTx != nil && len(producerTx.BlockHash) > 0 {
		producerBlock, err := database.BlockByHash(ls.db, producerTx.BlockHash)
		if err != nil {
			return nil, fmt.Errorf(
				"lookup producer block %x: %w",
				producerTx.BlockHash,
				err,
			)
		}
		if producerBlock.Slot >= pending.MaxSlot {
			return nil, nil
		}
		tx := ls.replayRecoveryResolveTxFromBlock(
			producerBlock,
			pending.Input.Id().Bytes(),
			chainIndex,
		)
		return &replayRecoveryResolvedProducer{
			Input:         pending.Input,
			ProducerTx:    producerTx,
			ProducerBlock: producerBlock,
			Tx:            tx,
			Strategy:      "metadata",
		}, nil
	}
	producerBlock, found, err := ls.replayRecoveryBlockFromTxBlob(
		pending.Input.Id().Bytes(),
	)
	if err != nil {
		return nil, err
	}
	if found {
		if producerBlock.Slot >= pending.MaxSlot {
			return nil, nil
		}
		tx := ls.replayRecoveryResolveTxFromBlock(
			producerBlock,
			pending.Input.Id().Bytes(),
			chainIndex,
		)
		return &replayRecoveryResolvedProducer{
			Input:         pending.Input,
			ProducerBlock: producerBlock,
			Tx:            tx,
			Strategy:      "tx-blob",
		}, nil
	}
	if chainIndex != nil {
		chainTx, ok := chainIndex.Txs[string(pending.Input.Id().Bytes())]
		if ok && chainTx.Block.Slot < pending.MaxSlot {
			return &replayRecoveryResolvedProducer{
				Input:         pending.Input,
				ProducerBlock: chainTx.Block,
				Tx:            chainTx.Tx,
				Strategy:      "chain-scan",
			}, nil
		}
	}
	return nil, nil
}

func (ls *LedgerState) replayRecoveryResolveTxFromBlock(
	block models.Block,
	txHash []byte,
	chainIndex *replayRecoveryChainIndex,
) lcommon.Transaction {
	if chainIndex != nil {
		chainTx, ok := chainIndex.Txs[string(txHash)]
		if ok && bytes.Equal(chainTx.Block.Hash, block.Hash) {
			return chainTx.Tx
		}
	}
	decodedBlock, err := block.Decode()
	if err != nil {
		ls.config.Logger.Debug(
			"skipping undecodable producer block during replay recovery",
			"component", "ledger",
			"block_slot", block.Slot,
			"block_hash", hex.EncodeToString(block.Hash),
			"error", err,
		)
		return nil
	}
	for _, tx := range decodedBlock.Transactions() {
		if bytes.Equal(tx.Hash().Bytes(), txHash) {
			return tx
		}
	}
	return nil
}

func (ls *LedgerState) replayRecoveryFallbackCandidate(
	failingPoint ocommon.Point,
	inputs []lcommon.TransactionInput,
) (*replayRecoveryCandidate, error) {
	if len(inputs) == 0 {
		return nil, nil
	}
	failingBlock, err := database.BlockByPoint(ls.db, failingPoint)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf(
			"lookup failing block %x at slot %d for replay fallback: %w",
			failingPoint.Hash,
			failingPoint.Slot,
			err,
		)
	}
	if failingBlock.ID <= database.BlockInitialIndex {
		return nil, nil
	}
	rewindBlocks := ls.SecurityParam()
	if rewindBlocks <= 0 {
		return nil, nil
	}
	targetIndex := database.BlockInitialIndex
	if failingBlock.ID > uint64(rewindBlocks) {
		targetIndex = failingBlock.ID - uint64(rewindBlocks)
	}
	// Anchor no higher than the durable applied floor. Only use the floor when
	// it belongs to the current primary chain; a stale fork floor must not be
	// used to roll the primary chain onto the abandoned branch.
	if floorIndex, ok, err := ls.durableAppliedFloorAnchorIndex(); err != nil {
		return nil, err
	} else if ok && floorIndex < targetIndex {
		targetIndex = floorIndex
	}
	anchorBlock, err := ls.db.BlockByIndex(targetIndex, nil)
	if err != nil {
		return nil, fmt.Errorf(
			"lookup replay fallback block %d: %w",
			targetIndex,
			err,
		)
	}
	rollbackPoint, err := ls.replayRecoveryParentPoint(anchorBlock)
	if err != nil {
		return nil, err
	}
	return &replayRecoveryCandidate{
		Input:              inputs[0],
		ProducerBlock:      anchorBlock,
		RollbackPoint:      rollbackPoint,
		Strategy:           "security-param-fallback",
		ProducerUnresolved: true,
	}, nil
}

// durableAppliedFloorAnchorIndex returns the block index of the durable
// applied floor when that point is present on the current primary chain.
func (ls *LedgerState) durableAppliedFloorAnchorIndex() (uint64, bool, error) {
	floor, ok, err := ls.durableAppliedFloor()
	if err != nil || !ok {
		return 0, false, err
	}
	onChain, err := ls.primaryChainContainsPoint(floor)
	if err != nil || !onChain {
		return 0, false, err
	}
	floorBlock, err := database.BlockByPoint(ls.db, floor)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return floorBlock.ID, true, nil
}

func (ls *LedgerState) replayRecoveryBlockFromTxBlob(
	txHash []byte,
) (models.Block, bool, error) {
	blob := ls.db.Blob()
	if blob == nil {
		return models.Block{}, false, nil
	}
	txn := ls.db.BlobTxn(false)
	if txn == nil || txn.Blob() == nil {
		return models.Block{}, false, nil
	}
	defer txn.Rollback() //nolint:errcheck

	txData, err := blob.GetTx(txn.Blob(), txHash)
	if err != nil {
		if errors.Is(err, dbtypes.ErrBlobKeyNotFound) {
			return models.Block{}, false, nil
		}
		return models.Block{}, false, fmt.Errorf(
			"lookup tx blob %s: %w",
			hex.EncodeToString(txHash),
			err,
		)
	}

	var point ocommon.Point
	switch {
	case database.IsTxOffsetStorage(txData):
		offset, err := database.DecodeTxOffset(txData)
		if err != nil {
			return models.Block{}, false, fmt.Errorf(
				"decode tx offset for %s: %w",
				hex.EncodeToString(txHash),
				err,
			)
		}
		point = ocommon.NewPoint(offset.BlockSlot, offset.BlockHash[:])
	case database.IsTxCborPartsStorage(txData):
		parts, err := database.DecodeTxCborParts(txData)
		if err != nil {
			return models.Block{}, false, fmt.Errorf(
				"decode tx parts for %s: %w",
				hex.EncodeToString(txHash),
				err,
			)
		}
		point = ocommon.NewPoint(parts.BlockSlot, parts.BlockHash[:])
	default:
		return models.Block{}, false, nil
	}

	block, err := database.BlockByPoint(ls.db, point)
	if err != nil {
		return models.Block{}, false, fmt.Errorf(
			"lookup producer block from tx blob %s: %w",
			hex.EncodeToString(txHash),
			err,
		)
	}
	return block, true, nil
}

func (ls *LedgerState) replayRecoveryParentPoint(
	block models.Block,
) (ocommon.Point, error) {
	// The genesis predecessor is encoded as an all-zero hash, not an empty
	// one, so a length check alone lets the first block after genesis fall
	// through to a lookup for a block that cannot exist. Rolling back to
	// origin is what the callers already do with the zero point.
	if block.Slot == 0 || isGenesisPrevHash(block.PrevHash) {
		return ocommon.Point{}, nil
	}
	parentBlock, err := database.BlockByHash(ls.db, block.PrevHash)
	if err != nil {
		return ocommon.Point{}, fmt.Errorf(
			"lookup parent block for replay recovery at slot %d: %w",
			block.Slot,
			err,
		)
	}
	return ocommon.NewPoint(parentBlock.Slot, parentBlock.Hash), nil
}

// isGenesisPrevHash reports whether a block's PrevHash refers to the genesis
// predecessor rather than to a stored block. Decoded blocks may carry no hash
// at all; forged blocks carry an all-zero Blake2b-256 hash. Any other length
// is malformed and is deliberately not treated as genesis, so it surfaces as a
// failed parent lookup rather than being silently rolled back to origin.
func isGenesisPrevHash(prevHash []byte) bool {
	if len(prevHash) == 0 {
		return true
	}
	if len(prevHash) != lcommon.Blake2b256Size {
		return false
	}
	for _, b := range prevHash {
		if b != 0 {
			return false
		}
	}
	return true
}
