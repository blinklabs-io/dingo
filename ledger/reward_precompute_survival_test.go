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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/stretchr/testify/require"
)

const (
	survivalSnapshotEpoch = uint64(1)
	survivalNewEpoch      = uint64(4)
	survivalCapturedSlot  = uint64(200)
	survivalBoundarySlot  = uint64(1_200)
)

func rewardOutputIDs(
	t *testing.T,
	db *database.Database,
) (map[string]uint, map[string]uint) {
	t.Helper()
	pools, err := db.Metadata().GetRewardPoolOutputs(survivalSnapshotEpoch, nil)
	require.NoError(t, err)
	accounts, err := db.Metadata().GetRewardAccountOutputs(
		survivalSnapshotEpoch, nil,
	)
	require.NoError(t, err)
	poolIDs := make(map[string]uint, len(pools))
	for _, output := range pools {
		poolIDs[string(output.PoolKeyHash)] = output.ID
	}
	accountIDs := make(map[string]uint, len(accounts))
	for _, output := range accounts {
		accountIDs[string(output.PoolKeyHash)+"/"+string(output.StakingKey)+
			"/"+output.RewardType] = output.ID
	}
	return poolIDs, accountIDs
}

func requirePrecomputeReusable(t *testing.T, ls *LedgerState) {
	t.Helper()
	txn := ls.db.Transaction(false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		_, ok, err := ls.precomputedStakeRewardApplication(
			txn, survivalNewEpoch, survivalBoundarySlot,
		)
		require.NoError(t, err)
		require.True(t, ok, "the boundary must find the precompute")
		return nil
	}))
}

// TestRewardPrecomputeSurvivesRollbackAboveCapturedSlot pins that a finished
// precompute outlives every rollback that stays above its captured slot: the
// rollback's reward-state sweep keeps its rows, and a re-run under the new
// rollback generation accepts them instead of recomputing the round.
func TestRewardPrecomputeSurvivesRollbackAboveCapturedSlot(t *testing.T) {
	t.Parallel()
	ls, db := seedMultiPoolRewardPrecomputeFixture(t, 7, 4, 7)
	ls.rewardPrecomputeChunkPoolsOverride = 2
	require.NoError(t, ls.runChunkedStakeRewardPrecompute(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	))
	poolIDs, accountIDs := rewardOutputIDs(t, db)
	require.Len(t, poolIDs, 7)

	// A rollback into the epoch the round is applied at, below its
	// application boundary.
	require.NoError(t, db.DeleteRewardStateAfterSlot(
		survivalCapturedSlot+300, nil,
	))
	ls.rewardInputGeneration.Add(2)
	gotPools, gotAccounts := rewardOutputIDs(t, db)
	require.Equal(t, poolIDs, gotPools, "the rollback must keep pool outputs")
	require.Equal(t, accountIDs, gotAccounts)

	require.NoError(t, ls.runChunkedStakeRewardPrecompute(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	))
	gotPools, gotAccounts = rewardOutputIDs(t, db)
	require.Equal(t, poolIDs, gotPools, "a re-run must not recompute the round")
	require.Equal(t, accountIDs, gotAccounts)
	requirePrecomputeReusable(t, ls)
}

// TestRewardPrecomputeRestartsWhenCommittedOutputsAreGone is the negative
// case of resumption: when the outputs a cursor claims are missing -- as after
// a rollback that reached the round's captured slot -- a re-run recomputes the
// whole round instead of resuming over the gap, and still matches an
// uninterrupted run.
func TestRewardPrecomputeRestartsWhenCommittedOutputsAreGone(t *testing.T) {
	t.Parallel()
	ls, db := seedMultiPoolRewardPrecomputeFixture(t, 7, 4, 7)
	ls.rewardPrecomputeChunkPoolsOverride = 2
	ls.rewardPrecomputeChunkHook = func(processed, total int) {
		if processed >= 4 {
			ls.rewardInputGeneration.Add(2)
		}
	}
	require.NoError(t, ls.runChunkedStakeRewardPrecompute(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	))
	partial, _ := rewardOutputIDs(t, db)
	require.Len(t, partial, 4)
	require.NoError(t, db.Metadata().DeleteRewardOutputsForEpoch(
		survivalSnapshotEpoch, nil,
	))

	ls.rewardPrecomputeChunkHook = nil
	require.NoError(t, ls.runChunkedStakeRewardPrecompute(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	))
	fresh, freshDB := seedMultiPoolRewardPrecomputeFixture(t, 7, 4, 7)
	fresh.rewardPrecomputeChunkPoolsOverride = 2
	require.NoError(t, fresh.runChunkedStakeRewardPrecompute(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	))
	require.Equal(
		t,
		snapshotRewardPrecomputeOutputs(t, freshDB, survivalSnapshotEpoch, 3),
		snapshotRewardPrecomputeOutputs(t, db, survivalSnapshotEpoch, 3),
	)
	requirePrecomputeReusable(t, ls)
}

// TestRewardPrecomputeResumesAcrossRollbackGeneration pins resumption: a run
// interrupted by a rollback that leaves its inputs untouched continues from
// its cursor under the new generation, keeping the committed chunks' rows.
func TestRewardPrecomputeResumesAcrossRollbackGeneration(t *testing.T) {
	t.Parallel()
	ls, db := seedMultiPoolRewardPrecomputeFixture(t, 7, 4, 7)
	ls.rewardPrecomputeChunkPoolsOverride = 2
	bumped := false
	ls.rewardPrecomputeChunkHook = func(processed, total int) {
		if !bumped {
			bumped = true
			ls.rewardInputGeneration.Add(2)
		}
	}
	require.NoError(t, ls.runChunkedStakeRewardPrecompute(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	))
	firstChunk, firstAccounts := rewardOutputIDs(t, db)
	require.Len(t, firstChunk, 2)

	ls.rewardPrecomputeChunkHook = nil
	require.NoError(t, ls.runChunkedStakeRewardPrecompute(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	))
	all, allAccounts := rewardOutputIDs(t, db)
	require.Len(t, all, 7)
	for key, id := range firstChunk {
		require.Equal(t, id, all[key], "a resumed run keeps committed chunks")
	}
	for key, id := range firstAccounts {
		require.Equal(t, id, allAccounts[key])
	}
	requirePrecomputeReusable(t, ls)

	// The resumed result equals an uninterrupted run's.
	fresh, freshDB := seedMultiPoolRewardPrecomputeFixture(t, 7, 4, 7)
	fresh.rewardPrecomputeChunkPoolsOverride = 2
	require.NoError(t, fresh.runChunkedStakeRewardPrecompute(
		survivalNewEpoch, survivalCapturedSlot, survivalBoundarySlot,
	))
	require.Equal(
		t,
		snapshotRewardPrecomputeOutputs(t, freshDB, survivalSnapshotEpoch, 3),
		snapshotRewardPrecomputeOutputs(t, db, survivalSnapshotEpoch, 3),
	)
}
