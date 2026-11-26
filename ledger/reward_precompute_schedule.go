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
	"fmt"
	"math"
	"math/big"
	"slices"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/epochengine"
)

type stakeRewardPrecomputeRetry struct {
	epochEvent event.EpochTransitionEvent
	cutoffSlot uint64
	generation uint64
}

type rewardPrecomputeRollbackSnapshot struct {
	pending      *event.EpochTransitionEvent
	retry        *stakeRewardPrecomputeRetry
	committed    bool
	reloadFailed bool
}

// queueStartupRewardPrecompute queues the stake-reward precompute for the
// epoch that is already in progress.
//
// The EventBus subscription that normally drives the precompute only fires at
// an epoch boundary, so a node started mid-epoch never queues the round its
// next boundary will apply. ApplyStakeRewards then finds no precompute and
// falls back to CalculateStakeRewardApplication -- the whole round, over every
// delegator in the mark snapshot -- inside the epoch-rollover write
// transaction, on the block-processing goroutine. On mainnet that scan takes
// minutes, during which no block is applied and any leader slot in the window
// is lost. The async path exists precisely so the calculation never holds
// SQLite's single writer (see the chunked precompute); this restores it for
// the one case the event cannot cover.
//
// Idempotent and self-limiting: PrecomputeStakeRewardsCalculate returns early
// when a valid precompute already exists, so a restart that has one costs a
// lookup, and a node still syncing defers on the reward prefilter slot rather
// than scanning inputs it does not yet have.
func (ls *LedgerState) queueStartupRewardPrecompute() {
	ls.queueStartupRewardPrecomputeWith(
		ls.precomputeStakeRewardsAfterEpochTransition,
	)
}

func (ls *LedgerState) queueStartupRewardPrecomputeWith(
	precompute func(event.EpochTransitionEvent) error,
) {
	ls.RLock()
	epoch := ls.currentEpoch
	capturedSlot := max(epoch.StartSlot, ls.currentTip.Point.Slot)
	ls.RUnlock()
	// An epoch with no length has not been established yet (fresh database),
	// and queueRewardPrecompute drops an event without a nonce, so there is
	// nothing to catch up on in either case.
	if epoch.LengthInSlots == 0 || len(epoch.Nonce) == 0 {
		return
	}
	evt := event.EpochTransitionEvent{
		NewEpoch:     epoch.EpochId,
		BoundarySlot: capturedSlot,
		EpochNonce:   epoch.Nonce,
	}
	if epoch.EpochId > 0 {
		evt.PreviousEpoch = epoch.EpochId - 1
	}
	if epoch.StartSlot > 0 {
		evt.SnapshotSlot = epoch.StartSlot - 1
	}
	ls.queueRewardPrecompute(evt, precompute)
}

func (ls *LedgerState) handleRewardPrecomputeEpochTransition(evt event.Event) {
	ls.handleRewardPrecomputeEpochTransitionWith(
		evt,
		ls.precomputeStakeRewardsAfterEpochTransition,
	)
}

func (ls *LedgerState) handleRewardPrecomputeEpochTransitionWith(
	evt event.Event,
	precompute func(event.EpochTransitionEvent) error,
) {
	epochEvent, ok := evt.Data.(event.EpochTransitionEvent)
	if !ok || epochEvent.EpochNonce == nil {
		return
	}
	ls.queueRewardPrecompute(epochEvent, precompute)
}

// queueRewardPrecompute keeps EventBus delivery independent of the expensive
// reward calculation. While a calculation is active, each transition replaces
// the pending one so bulk sync retains the newest useful epoch without filling
// and overflowing the EventBus subscriber buffer. The active calculation is
// allowed to finish because its snapshot guard drops stale output; the worker
// then processes the latest transition observed in the meantime.
func (ls *LedgerState) queueRewardPrecompute(
	epochEvent event.EpochTransitionEvent,
	precompute func(event.EpochTransitionEvent) error,
) {
	ls.rewardPrecomputeMu.Lock()
	start := ls.queueRewardPrecomputeLocked(epochEvent)
	ls.rewardPrecomputeMu.Unlock()
	if start {
		go ls.runRewardPrecompute(precompute)
	}
}

// The caller holds rewardPrecomputeMu so releasing a prefilter retry and
// invalidating it during rollback cannot enqueue events in the wrong order.
func (ls *LedgerState) queueRewardPrecomputeLocked(
	epochEvent event.EpochTransitionEvent,
) bool {
	if ls.closed.Load() {
		return false
	}
	// Store an independent copy because EventBus callbacks do not own the
	// publisher's payload after returning.
	epochEvent.EpochNonce = slices.Clone(epochEvent.EpochNonce)
	ls.rewardPrecomputePending = &epochEvent
	if ls.rewardPrecomputeRunning {
		return false
	}
	ls.rewardPrecomputeRunning = true
	ls.rewardPrecomputeWG.Add(1)
	return true
}

// deferStakeRewardPrecompute records the pre-Babbage RUPD cutoff. The retry is
// queued only after the applied ledger tip reaches that slot, so its
// BoundarySlot reflects the actual state captured by the calculation rather
// than a future slot whose certificate history is not available yet.
func (ls *LedgerState) deferStakeRewardPrecompute(
	newEpoch uint64,
	cutoffSlot uint64,
	generation uint64,
) {
	if newEpoch == 0 {
		return
	}
	retry := &stakeRewardPrecomputeRetry{
		epochEvent: event.EpochTransitionEvent{
			NewEpoch: newEpoch - 1,
		},
		cutoffSlot: cutoffSlot,
		generation: generation,
	}
	ls.rewardPrecomputeMu.Lock()
	if ls.rewardInputRollbackActive.Load() != 0 ||
		ls.rewardInputGeneration.Load() != generation {
		ls.rewardPrecomputeMu.Unlock()
		return
	}
	if ls.rewardPrecomputeRetry == nil ||
		ls.rewardPrecomputeRetry.epochEvent.NewEpoch <= retry.epochEvent.NewEpoch {
		ls.rewardPrecomputeRetry = retry
	}
	ls.rewardPrecomputeMu.Unlock()

	ls.RLock()
	capturedSlot := ls.currentTip.Point.Slot
	ls.RUnlock()
	ls.maybeQueueStakeRewardPrecomputeRetry(capturedSlot)
}

// maybeQueueStakeRewardPrecomputeRetry releases a deferred pre-Babbage
// calculation once committed ledger state has reached its RUPD cutoff.
func (ls *LedgerState) maybeQueueStakeRewardPrecomputeRetry(
	capturedSlot uint64,
) {
	ls.rewardPrecomputeMu.Lock()
	retry := ls.rewardPrecomputeRetry
	if retry == nil || capturedSlot < retry.cutoffSlot || ls.closed.Load() {
		ls.rewardPrecomputeMu.Unlock()
		return
	}
	ls.rewardPrecomputeRetry = nil
	if ls.rewardInputRollbackActive.Load() != 0 ||
		retry.generation != ls.rewardInputGeneration.Load() {
		ls.rewardPrecomputeMu.Unlock()
		return
	}
	epochEvent := retry.epochEvent
	epochEvent.BoundarySlot = capturedSlot
	start := ls.queueRewardPrecomputeLocked(
		epochEvent,
	)
	ls.rewardPrecomputeMu.Unlock()
	if start {
		go ls.runRewardPrecompute(
			ls.precomputeStakeRewardsAfterEpochTransition,
		)
	}
}

func (ls *LedgerState) runRewardPrecompute(
	precompute func(event.EpochTransitionEvent) error,
) {
	defer ls.rewardPrecomputeWG.Done()
	for {
		ls.rewardPrecomputeMu.Lock()
		if ls.closed.Load() || ls.rewardPrecomputePending == nil {
			ls.rewardPrecomputePending = nil
			ls.rewardPrecomputeRunning = false
			ls.rewardPrecomputeMu.Unlock()
			return
		}
		epochEvent := *ls.rewardPrecomputePending
		ls.rewardPrecomputePending = nil
		ls.rewardPrecomputeMu.Unlock()

		if err := callRewardPrecompute(precompute, epochEvent); err != nil {
			ls.config.Logger.Warn(
				"failed to precompute stake rewards",
				"component", "ledger",
				"new_epoch", epochEvent.NewEpoch,
				"error", err,
			)
		}
	}
}

// callRewardPrecompute preserves the panic isolation that EventBus provided
// when the calculation ran directly inside its callback. A failed calculation
// must not crash the process or prevent the worker from consuming the newest
// transition that arrived while it was running.
func callRewardPrecompute(
	precompute func(event.EpochTransitionEvent) error,
	epochEvent event.EpochTransitionEvent,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("reward precompute panicked: %v", recovered)
		}
	}()
	return precompute(epochEvent)
}

// precomputeStakeRewardsAfterEpochTransition runs off the EventBus after an
// epoch transition to opportunistically precompute the next boundary's stake
// rewards ahead of time, so that the synchronous rollover application
// (ApplyStakeRewards, called from processEpochRollover with its own short
// transaction) can reuse the precomputed result instead of computing rewards
// inline.
//
// The calculation reads a large amount of reward-input state (every pool and
// per-credential stake input for the mark snapshot epoch, plus per-credential
// cert-history prefilter queries) and, on a mainnet-scale database, can run
// for minutes. It must never hold SQLite's single writer for that long, so
// it is split into a read-only calculation phase and a short write phase:
// see PrecomputeStakeRewardsCalculate and SaveStakeRewardPrecompute. Between
// the two phases the world may have moved on (a rollback replacing the
// reward snapshot, or the boundary-application path already having run);
// the write phase re-validates before persisting and drops a stale result
// rather than writing it -- see epochengine.StakeRewardPrecomputeSnapshotGuardOK.
func (ls *LedgerState) precomputeStakeRewardsAfterEpochTransition(
	evt event.EpochTransitionEvent,
) error {
	if evt.NewEpoch == math.MaxUint64 {
		return nil
	}

	var (
		haveEpoch               bool
		applicationBoundarySlot uint64
	)
	epochTxn := ls.ledgerDB().Begin(context.Background(), false)
	if err := ls.ledgerDB().Run(epochTxn, func(txn ledgerdb.Txn) error {
		epoch, err := ls.ledgerDB().GetEpoch(evt.NewEpoch, txn)
		if err != nil {
			return fmt.Errorf(
				"get epoch %d for reward precompute: %w",
				evt.NewEpoch,
				err,
			)
		}
		if epoch == nil || epoch.LengthInSlots == 0 {
			return nil
		}
		applicationBoundarySlot = epoch.StartSlot + uint64(epoch.LengthInSlots)
		haveEpoch = true
		return nil
	}); err != nil {
		return err
	}
	if !haveEpoch {
		return nil
	}

	newEpoch := evt.NewEpoch + 1
	capturedSlot := evt.BoundarySlot

	handled, err := ls.runChunkedStakeRewardPrecomputeRound(
		newEpoch,
		capturedSlot,
		applicationBoundarySlot,
	)
	if err != nil || handled {
		return err
	}
	return ls.precomputeStakeRewardsSinglePass(
		newEpoch, capturedSlot, applicationBoundarySlot,
	)
}

// precomputeStakeRewardsSinglePass precomputes a round the per-pool
// precompute does not split -- a pre-Allegra or bootstrap round -- with the
// single-pass calculation, in a read-only transaction, then persists it in a
// short write transaction after re-checking its snapshot and rollback
// generation under rewardPrecomputeWriteMu.
func (ls *LedgerState) precomputeStakeRewardsSinglePass(
	newEpoch uint64,
	capturedSlot uint64,
	applicationBoundarySlot uint64,
) error {
	var app *epochengine.StakeRewardApplication
	readTxn := ls.ledgerDB().Begin(context.Background(), false)
	if err := ls.ledgerDB().Run(readTxn, func(txn ledgerdb.Txn) error {
		computed, ok, err := ls.engine().PrecomputeStakeRewardsCalculate(
			txn,
			newEpoch,
			capturedSlot,
			applicationBoundarySlot,
		)
		if err != nil || !ok {
			return err
		}
		app = computed
		return nil
	}); err != nil {
		return err
	}
	if app == nil {
		return nil
	}
	ls.rewardPrecomputeWriteMu.Lock()
	defer ls.rewardPrecomputeWriteMu.Unlock()
	writeTxn := ls.ledgerDB().Begin(context.Background(), true)
	return ls.ledgerDB().Run(writeTxn, func(txn ledgerdb.Txn) error {
		ldb := ls.ledgerDB()
		if _, alreadyApplied, err := ls.engine().PrecomputedStakeRewardApplication(context.Background(),
			txn,
			newEpoch,
			applicationBoundarySlot,
		); err != nil {
			return err
		} else if alreadyApplied {
			return nil
		}
		guardOK, err := epochengine.StakeRewardPrecomputeSnapshotGuardOK(
			ldb,
			txn,
			app,
		)
		if err != nil || !guardOK {
			return err
		}
		if ls.rewardPrecomputeBeforeSaveHook != nil {
			ls.rewardPrecomputeBeforeSaveHook()
		}
		return ls.engine().SaveStakeRewardPrecompute(
			ldb,
			txn,
			app,
			newEpoch,
			capturedSlot,
			applicationBoundarySlot,
		)
	})
}

// MinPoolMargin returns the CIP-23 minimum pool margin as a rational in [0, 1],
// or nil when disabled (config value 0). It satisfies the eras package
// MinPoolMarginProvider interface used by the Dijkstra pool-margin-floor
// certificate rule; the Dijkstra-only era gate is inherent because only
// ValidateTxDijkstra consults it.
func (ls *LedgerState) MinPoolMargin() *big.Rat {
	return epochengine.MinPoolMarginRat(ls.config.MinPoolMargin)
}
