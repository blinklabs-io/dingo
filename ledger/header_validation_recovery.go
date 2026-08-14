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
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/event"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// headerValidationError marks a stateful header check that failed while
// applying a block the chain store already holds — the deferred
// registered-VRF-key and Praos leader-eligibility checks that blockfetch
// could not run because the facts they need were still ahead of the apply
// cursor.
//
// It exists to make that failure recoverable in the same sense transaction
// validation failures already are. Rejecting the block is correct and stays
// correct; what was missing is that the rejection had nowhere to go. The
// block is already persisted, so returning a bare error restarts the pipeline
// onto the identical block, which fails identically, at the retry cadence,
// for as long as the process runs. That is a deterministic loop rather than a
// transient one, and no amount of backoff resolves it.
//
// The correct response to a block a node believes is invalid is to reject the
// chain containing it and let chain selection offer another — which is what
// the reference node does — not to halt on it. See
// tryRecoverFromHeaderValidationError.
type headerValidationError struct {
	BlockPoint ocommon.Point
	Cause      error
}

func (e *headerValidationError) Error() string {
	return fmt.Sprintf(
		"header validation failed for block at slot %d: %v",
		e.BlockPoint.Slot,
		e.Cause,
	)
}

func (e *headerValidationError) Unwrap() error { return e.Cause }

// tryRecoverFromHeaderValidationError rewinds past a block whose deferred
// stateful header checks failed, so the pipeline stops re-reading it.
//
// This does not accept the block and does not soften the check: the block is
// dropped from the primary chain and the ledger is rolled back to the last
// good tip, exactly as if the chain had been rejected. Chainsync is then
// asked to re-deliver, giving chain selection the chance to offer a different
// candidate.
//
// If the network genuinely offers no other chain, the node still cannot
// advance — that is the correct outcome for a node that believes the block is
// invalid, and it is the same outcome the reference node would reach. The
// difference is that it now surfaces through the stuck-pipeline detection
// (dingo_ledger_pipeline_stuck and a single ERROR) instead of an unbounded
// retry loop, so the disagreement is diagnosed rather than merely endured.
//
// Returns (true, nil) when the rewind was performed and the pipeline should
// restart, (false, nil) when this error class does not apply or there is
// nothing to rewind.
func (ls *LedgerState) tryRecoverFromHeaderValidationError(
	err error,
) (bool, error) {
	var validationErr *headerValidationError
	if !errors.As(err, &validationErr) {
		return false, nil
	}
	// Nothing to rewind: report not-recovered rather than sending the
	// pipeline back into the same block believing it was handled.
	if ls.chain == nil || ls.config.ChainManager == nil {
		return false, nil
	}

	ls.RLock()
	ledgerTip := ls.currentTip
	ls.RUnlock()

	// The ledger tip is normally the last block that applied cleanly, so it
	// already precedes the failing block and rewinding to it drops the
	// rejected block without undoing valid history.
	//
	// That has to be checked rather than assumed. If the tip is not strictly
	// before the failing block — the deferred marker replayed for a block
	// already applied, say — then both the chain rewind and the ledger
	// rollback are no-ops against their own current position, nothing is
	// dropped, and returning "recovered" would send the pipeline back into
	// the identical block while reporting that it had been handled. That is
	// strictly worse than declining: the loop continues *and* the
	// stuck-pipeline signal is suppressed, because every restart looks like
	// a successful recovery. Decline instead and let the failure surface.
	rewindPoint := ledgerTip.Point
	if rewindPoint.Slot >= validationErr.BlockPoint.Slot {
		if ls.config.Logger != nil {
			ls.config.Logger.Warn(
				"header validation rejected a block at or behind the ledger tip; no rewind target precedes it, so recovery cannot drop it",
				"component",
				"ledger",
				"failing_block_slot",
				validationErr.BlockPoint.Slot,
				"ledger_tip_slot",
				rewindPoint.Slot,
				"error",
				validationErr.Cause,
			)
		}
		return false, nil
	}
	if ls.recoveryRollbackExceedsMithrilBoundary(rewindPoint) {
		return false, nil
	}

	if ls.config.Logger != nil {
		ls.config.Logger.Warn(
			"deferred header validation rejected a block already on the primary chain; rewinding so chain selection can offer another candidate",
			"component",
			"ledger",
			"failing_block_slot",
			validationErr.BlockPoint.Slot,
			"rewind_target_slot",
			rewindPoint.Slot,
			"error",
			validationErr.Cause,
		)
	}

	if err := ls.rollbackPrimaryChainInSecurityParamWindows(
		rewindPoint,
	); err != nil {
		return false, fmt.Errorf(
			"rewind primary chain after header validation failure: %w",
			err,
		)
	}
	// The chain prune alone leaves the ledger reflecting the rejected
	// block's post-apply state; the matching ledger rollback has to be
	// explicit, for the same reason it is on the transaction-validation
	// path.
	if err := ls.rollback(rewindPoint); err != nil {
		return false, fmt.Errorf(
			"rollback ledger state after header validation failure: %w",
			err,
		)
	}
	if ls.config.EventBus != nil {
		ls.config.EventBus.Publish(
			event.ChainsyncResyncEventType,
			event.NewEvent(
				event.ChainsyncResyncEventType,
				event.ChainsyncResyncEvent{
					Reason: event.ChainsyncResyncReasonHeaderValidationRecovery,
					Point:  rewindPoint,
				},
			),
		)
	}
	return true, nil
}
