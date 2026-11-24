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

	"github.com/blinklabs-io/dingo/database/ledgerdb"
	"github.com/blinklabs-io/dingo/ledger/epochengine"
)

// fenceRewardPrecompute stops background precompute chunks from writing
// before a boundary completes and applies a round in its own transaction. It
// waits for a chunk already in flight, and must be called before the boundary
// transaction opens: on SQLite that chunk may be waiting for the writer.
func (ls *LedgerState) fenceRewardPrecompute() {
	ls.rewardPrecomputeWriteMu.Lock()
	ls.rewardPrecomputeFence.Add(1)
	ls.rewardPrecomputeWriteMu.Unlock()
}

// runChunkedStakeRewardPrecomputeRound drives Pass 2 (the per-pool member
// split) to completion for one reward round, one bounded chunk of pools per
// short write transaction, persisting a resumable cursor with every chunk. It
// reports whether the per-pool precompute took the round at all.
func (ls *LedgerState) runChunkedStakeRewardPrecomputeRound(
	newEpoch uint64,
	capturedSlot uint64,
	boundarySlot uint64,
) (bool, error) {
	credited, err := ls.engine().RewardRoundCredited(nil, newEpoch)
	if err != nil || credited {
		return credited, err
	}
	if err := ls.completePendingRewardStakeInputs(newEpoch); err != nil {
		return false, err
	}
	round, ok, err := ls.engine().ResolveStakeRewardPrecomputeRound(
		newEpoch, capturedSlot, boundarySlot,
	)
	if err != nil || !ok {
		return false, err
	}
	for {
		if ls.closed.Load() {
			return true, nil
		}
		done, err := ls.stakeRewardPrecomputeChunkStep(round)
		if err != nil {
			return true, err
		}
		if done {
			return true, nil
		}
	}
}

// stakeRewardPrecomputeChunkStep processes at most one chunk of pools inside
// its own write transaction. done is true once nothing further should be
// attempted for this round: Pass 2 is complete, or the round was invalidated
// by a rollback or claimed by a boundary and the caller must stop.
func (ls *LedgerState) stakeRewardPrecomputeChunkStep(
	round *epochengine.StakeRewardPrecomputeRound,
) (bool, error) {
	ls.rewardPrecomputeWriteMu.Lock()
	defer ls.rewardPrecomputeWriteMu.Unlock()

	done := false
	writeTxn := ls.beginPrecomputeChunkTxn(context.Background())
	err := ls.ledgerDB().Run(writeTxn, func(txn ledgerdb.Txn) error {
		if ls.rewardInputRollbackActive.Load() != 0 ||
			ls.rewardInputGeneration.Load() != round.Generation ||
			ls.rewardPrecomputeFence.Load() != round.Fence {
			done = true
			return nil
		}
		current, err := ls.ledgerDB().GetRewardSnapshot(
			round.Epochs.Snapshot,
			"mark",
			txn,
		)
		if err != nil {
			return fmt.Errorf(
				"re-check reward snapshot for epoch %d: %w",
				round.Epochs.Snapshot,
				err,
			)
		}
		if !epochengine.RewardPrecomputeSnapshotUnchanged(round.RewardSnapshot, current) {
			done = true
			return nil
		}
		// A round resolved before its boundary committed can reach this step
		// after the boundary credited it: the fence and snapshot still match,
		// but the outputs are now account balances.
		credited, err := ls.engine().RewardRoundCredited(txn, round.NewEpoch)
		if err != nil {
			return err
		}
		if credited {
			done = true
			return nil
		}
		done, err = ls.engine().StakeRewardPrecomputeChunksInTxn(ls.lifecycleContext(), txn, round, 1)
		return err
	})
	return done, err
}

// beginPrecomputeChunkTxn opens the transaction a chunk runs in. A chunk's
// writes only stage changes for the LedgerDB, so the transaction takes no
// metadata write connection and block commits are not queued behind it.
func (ls *LedgerState) beginPrecomputeChunkTxn(
	ctx context.Context,
) ledgerdb.Txn {
	return ls.ledgerDB().BeginStaged(ctx)
}
