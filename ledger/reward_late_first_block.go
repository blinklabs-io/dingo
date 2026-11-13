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
	"errors"
	"fmt"
	"math"

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
)

// Late first block of an epoch.
//
// cardano-ledger's TICK rule runs RUPD with the epoch state and blocks-made
// of the state it was handed, before NEWEPOCH ran (bheadTransition binds
// nes0 and passes RupdEnv bprev es), while it hands RUPD the reward update
// left by NEWEPOCH, which is SNothing straight after a boundary. When the
// first block of epoch X is later than start(X) plus the randomness
// stabilisation window, that very tick starts the update, so the round is
// built from the state at the end of epoch X-1: the same snapshot,
// performance epoch, reserves and fee seed as the round applied at the
// boundary into X. It is then applied, once, at the boundary into X+1,
// added to the pots as they stand there. The performance of epoch X-1 is
// never paid. On a network that starts in Byron, a late first block of the
// epoch after the first Shelley epoch replays the first Shelley round, whose
// performance epoch is Byron and has no pool performance.
//
// The update's deltas are applied to the live pots (applyRUpdFiltered), and
// its deltaF subtracts the replayed fee seed from the fees the ledger
// carries into the next snapshot, so the next round's fee seed is
// F(X) + carried(X) - seed rather than F(X).

// lateFirstRewardBlock reports whether a first block at firstBlockSlot misses
// the RUPD stability point of the epoch starting at epochStart. RUPD waits
// while the slot is at or below start + window.
func lateFirstRewardBlock(
	firstBlockSlot uint64,
	epochStart uint64,
	window uint64,
) bool {
	if window == 0 || window > math.MaxUint64-epochStart {
		return false
	}
	return firstBlockSlot > epochStart+window
}

// replaysPreviousStakeRewardRound reports whether the update applied at the
// boundary into newEpoch is the one the boundary into newEpoch-1 applied,
// because the first block of newEpoch-1 was late and the epoch before it had
// blocks, so NEWEPOCH ran at that block. It reads persisted blocks, so a
// rollback re-derives the answer.
//
// Only rounds with a regular reward basis (newEpoch-1 >= 3) are replayed. A
// node whose block store lacks the earlier epochs cannot tell, and keeps
// the ordinary round.
func (ls *LedgerState) replaysPreviousStakeRewardRound(
	txn ledgerdb.Txn,
	newEpoch uint64,
) (bool, error) {
	if newEpoch < 4 {
		return false, nil
	}
	window := ls.rewardUpdateStabilityWindow()
	if window == 0 {
		return false, nil
	}
	ldb := ls.ledgerDB()
	ended, err := ldb.GetEpoch(newEpoch-1, txn)
	if err != nil {
		return false, fmt.Errorf("get epoch %d: %w", newEpoch-1, err)
	}
	previous, err := ldb.GetEpoch(newEpoch-2, txn)
	if err != nil {
		return false, fmt.Errorf("get epoch %d: %w", newEpoch-2, err)
	}
	if ended == nil || previous == nil ||
		ended.EraId == eras.ByronEraDesc.Id ||
		previous.EraId == eras.ByronEraDesc.Id ||
		ended.LengthInSlots == 0 ||
		previous.LengthInSlots == 0 {
		return false, nil
	}
	first, found, err := firstBlockInSlotRange(
		ldb, txn, ended.StartSlot, uint64(ended.LengthInSlots),
	)
	if err != nil || !found {
		return false, err
	}
	if !lateFirstRewardBlock(first, ended.StartSlot, window) {
		return false, nil
	}
	_, found, err = firstBlockInSlotRange(
		ldb, txn, previous.StartSlot, uint64(previous.LengthInSlots),
	)
	if err != nil {
		return false, err
	}
	return found, nil
}

// firstBlockInSlotRange returns the slot of the first stored block in
// [start, start+length).
func firstBlockInSlotRange(
	ldb ledgerdb.LedgerDB,
	txn ledgerdb.Txn,
	start uint64,
	length uint64,
) (uint64, bool, error) {
	block, err := ldb.FirstBlockAtOrAfterSlot(start, txn)
	if errors.Is(err, models.ErrBlockNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf(
			"find first block at or after slot %d: %w", start, err,
		)
	}
	if length > math.MaxUint64-start || block.Slot >= start+length {
		return 0, false, nil
	}
	return block.Slot, true, nil
}

// replayedStakeRewardFees returns the fee total the boundary into a replayed
// round's epoch carries forward: the fees collected in the ended epoch plus
// the fees already carried, less the fee seed the replayed update removes.
func replayedStakeRewardFees(
	endedEpochFees uint64,
	carriedFees uint64,
	seedFees uint64,
) (uint64, error) {
	total, overflow := addRewardUint64(endedEpochFees, carriedFees)
	if overflow {
		return 0, errors.New("replayed reward fee carry overflow")
	}
	if total < seedFees {
		return 0, fmt.Errorf(
			"replayed reward fee seed %d exceeds carried fees %d",
			seedFees, total,
		)
	}
	return total - seedFees, nil
}

// applyReplayedStakeRewardRound applies, at the boundary into newEpoch, the
// round the boundary into newEpoch-1 applied, against the pots as they stand
// at this boundary.
func (ls *LedgerState) applyReplayedStakeRewardRound(
	ctx context.Context,
	txn ledgerdb.Txn,
	newEpoch uint64,
	boundarySlot uint64,
) error {
	ldb := ls.ledgerDB()
	baseline, err := ldb.GetRewardAdaPots(newEpoch-1, txn)
	if err != nil {
		return fmt.Errorf(
			"get reward ADA pots for epoch %d: %w", newEpoch-1, err,
		)
	}
	if baseline == nil {
		return ls.requiredStakeRewardBasisUnavailable(
			true, newEpoch, "missing ADA pots", "pots_epoch", newEpoch-1,
		)
	}
	// The round being replayed is that of the boundary into newEpoch-1. When
	// that is the first Shelley round its performance epoch is Byron and it
	// has no pool performance; cardano-ledger rebuilds the same update.
	byronPerformance, err := ls.rewardPerformanceEpochIsByron(txn, newEpoch-1)
	if err != nil {
		return err
	}
	var app *stakeRewardApplication
	if byronPerformance {
		app, err = ls.firstShelleyStakeRewardApplication(txn, newEpoch-1)
		if err != nil {
			return err
		}
	} else {
		var ok bool
		app, ok, err = ls.calculateStakeRewardApplication(ctx,
			txn, newEpoch-1, boundarySlot, boundarySlot, true,
		)
		if err != nil {
			return err
		}
		if !ok || app == nil {
			return nil
		}
	}
	baselineCopy := *baseline
	app.replayBaseline = &baselineCopy
	return ls.applyStakeRewardApplication(ctx, txn, app, boundarySlot)
}

// carriedRewardFees returns the fee total saveRewardAdaPotsForEpoch records
// for newEpoch. It is the ended epoch's own fees except at a boundary that
// replays a round, where the fees carried from the previous boundary, less
// the replayed round's fee seed, are added.
func (ls *LedgerState) carriedRewardFees(
	txn ledgerdb.Txn,
	newEpoch uint64,
	endedEpochFees uint64,
) (uint64, error) {
	replay, err := ls.replaysPreviousStakeRewardRound(txn, newEpoch)
	if err != nil || !replay {
		return endedEpochFees, err
	}
	ldb := ls.ledgerDB()
	carried, err := ldb.GetRewardAdaPots(newEpoch-1, txn)
	if err != nil {
		return 0, fmt.Errorf(
			"get reward ADA pots for epoch %d: %w", newEpoch-1, err,
		)
	}
	seed, err := ldb.GetRewardAdaPots(newEpoch-2, txn)
	if err != nil {
		return 0, fmt.Errorf(
			"get reward ADA pots for epoch %d: %w", newEpoch-2, err,
		)
	}
	if carried == nil || seed == nil {
		return 0, fmt.Errorf(
			"replayed reward round for epoch %d is missing ADA pots",
			newEpoch,
		)
	}
	return replayedStakeRewardFees(
		endedEpochFees, uint64(carried.Fees), uint64(seed.Fees),
	)
}
