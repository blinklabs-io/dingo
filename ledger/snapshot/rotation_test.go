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

package snapshot

import (
	"bytes"
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
)

// seedRetentionRows writes one row per epoch in [0, throughEpoch] into every
// table cleanupOldSnapshots touches, all describing the same pool, so a
// retention pass can be observed table by table.
func seedRetentionRows(
	t *testing.T,
	db *database.Database,
	poolKeyHash []byte,
	throughEpoch uint64,
) {
	t.Helper()
	meta := db.Metadata()
	stakingKey := bytes.Repeat([]byte{0xdd}, 28)
	rewardAccount := bytes.Repeat([]byte{0xbb}, 28)
	for epoch := uint64(0); epoch <= throughEpoch; epoch++ {
		boundarySlot := epoch * 432000
		require.NoError(t, meta.SaveEpochSummary(&models.EpochSummary{
			Epoch:            epoch,
			TotalActiveStake: types.Uint64(1_000_000 + epoch),
			TotalPoolCount:   1,
			TotalDelegators:  1,
			BoundarySlot:     boundarySlot,
			SnapshotReady:    true,
		}, nil), "save epoch summary %d", epoch)
		require.NoError(t, meta.SavePoolStakeSnapshots(
			[]*models.PoolStakeSnapshot{{
				Epoch:          epoch,
				SnapshotType:   models.PoolStakeSnapshotTypeMark,
				PoolKeyHash:    poolKeyHash,
				TotalStake:     types.Uint64(1_000_000),
				DelegatorCount: 1,
				CapturedSlot:   boundarySlot,
			}},
			nil,
		), "save pool stake snapshot %d", epoch)
		require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
			Epoch:        epoch,
			CapturedSlot: boundarySlot,
		}, nil), "save reward ada pots %d", epoch)
		require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
			Epoch:            epoch,
			SnapshotType:     models.PoolStakeSnapshotTypeMark,
			TotalActiveStake: types.Uint64(1_000_000),
			TotalPoolCount:   1,
			TotalDelegators:  1,
			CapturedSlot:     boundarySlot,
			BoundarySlot:     boundarySlot,
			Authoritative:    true,
		}, nil), "save reward snapshot %d", epoch)
		require.NoError(t, meta.SaveRewardPoolInputs(
			[]*models.RewardPoolInput{{
				Epoch:          epoch,
				PoolKeyHash:    poolKeyHash,
				Pledge:         types.Uint64(1_000_000),
				DelegatedStake: types.Uint64(1_000_000),
				Cost:           types.Uint64(340_000_000),
				Margin:         &types.Rat{Rat: big.NewRat(1, 100)},
				RewardAccount:  rewardAccount,
				DelegatorCount: 1,
				CapturedSlot:   boundarySlot,
				BoundarySlot:   boundarySlot,
			}},
			nil,
		), "save reward pool input %d", epoch)
		require.NoError(t, meta.SaveRewardStakeInputs(
			[]*models.RewardStakeInput{{
				Epoch:        epoch,
				PoolKeyHash:  poolKeyHash,
				StakingKey:   stakingKey,
				Stake:        types.Uint64(1_000_000),
				Registered:   true,
				CapturedSlot: boundarySlot,
				BoundarySlot: boundarySlot,
			}},
			nil,
		), "save reward stake input %d", epoch)
		require.NoError(t, meta.SaveRewardPoolOutputs(
			[]*models.RewardPoolOutput{{
				Epoch:        epoch,
				PoolKeyHash:  poolKeyHash,
				TotalReward:  types.Uint64(500),
				LeaderReward: types.Uint64(100),
				CapturedSlot: boundarySlot,
				BoundarySlot: boundarySlot,
			}},
			nil,
		), "save reward pool output %d", epoch)
		require.NoError(t, meta.SaveRewardAccountOutputs(
			[]*models.RewardAccountOutput{{
				Epoch:        epoch,
				StakingKey:   stakingKey,
				PoolKeyHash:  poolKeyHash,
				RewardType:   "member",
				Amount:       types.Uint64(400),
				Spendable:    true,
				CapturedSlot: boundarySlot,
				BoundarySlot: boundarySlot,
			}},
			nil,
		), "save reward account output %d", epoch)
	}
}

// TestCleanupOldSnapshotsRetainsEpochSummaries pins the retention split that
// dingo #2987 turned up. Rows that scale with delegator count stay bounded to
// the rotation/reward-replay window, while the three tables that scale with
// epoch or pool count — epoch_summary, reward_snapshot, reward_pool_input — are
// kept for the life of the database, so historical closed-epoch comparison has
// per-epoch aggregates and a per-pool reward basis to compare against (and a
// missing summary keeps meaning "never captured").
func TestCleanupOldSnapshotsRetainsEpochSummaries(t *testing.T) {
	db := setupTestDB(t)
	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	meta := db.Metadata()

	const currentEpoch = uint64(10)
	// Matches cleanupOldSnapshots: epochs below this are outside the retained
	// per-pool window.
	const firstRetainedEpoch = currentEpoch - 3
	poolKeyHash := bytes.Repeat([]byte{0xaa}, 28)

	seedRetentionRows(t, db, poolKeyHash, currentEpoch)
	require.NoError(
		t,
		mgr.cleanupOldSnapshots(context.Background(), currentEpoch),
	)

	for epoch := uint64(0); epoch <= currentEpoch; epoch++ {
		summary, err := meta.GetEpochSummary(epoch, nil)
		require.NoError(t, err, "get epoch summary %d", epoch)
		require.NotNil(
			t,
			summary,
			"epoch_summary for epoch %d must survive cleanup",
			epoch,
		)
		require.Equal(t, epoch, summary.Epoch)
		require.Equal(
			t,
			types.Uint64(1_000_000+epoch),
			summary.TotalActiveStake,
			"retained epoch_summary %d must keep its captured totals",
			epoch,
		)
	}

	// The whole reward record except the per-credential rows is retained
	// alongside epoch_summary: pots and snapshot at one row per epoch, pool
	// inputs and pool outputs at one row per pool per epoch.
	for epoch := uint64(0); epoch <= currentEpoch; epoch++ {
		pots, err := meta.GetRewardAdaPots(epoch, nil)
		require.NoError(t, err, "get reward ada pots %d", epoch)
		require.NotNil(
			t,
			pots,
			"reward_ada_pots for epoch %d must survive cleanup",
			epoch,
		)
		rewardSnapshot, err := meta.GetRewardSnapshot(
			epoch, models.PoolStakeSnapshotTypeMark, nil,
		)
		require.NoError(t, err, "get reward snapshot %d", epoch)
		require.NotNil(
			t,
			rewardSnapshot,
			"reward_snapshot for epoch %d must survive cleanup",
			epoch,
		)
		inputs, err := meta.GetRewardPoolInputs(epoch, nil)
		require.NoError(t, err, "get reward pool inputs %d", epoch)
		require.Len(
			t,
			inputs,
			1,
			"reward_pool_input for epoch %d must survive cleanup",
			epoch,
		)
		require.Equal(
			t,
			types.Uint64(1_000_000),
			inputs[0].DelegatedStake,
			"retained reward_pool_input %d must keep its captured stake",
			epoch,
		)
		poolOutputs, err := meta.GetRewardPoolOutputs(epoch, nil)
		require.NoError(t, err, "get reward pool outputs %d", epoch)
		require.Len(
			t,
			poolOutputs,
			1,
			"reward_pool_output for epoch %d must survive cleanup",
			epoch,
		)
		require.Equal(
			t,
			types.Uint64(500),
			poolOutputs[0].TotalReward,
			"retained reward_pool_output %d must keep its computed reward",
			epoch,
		)
	}

	// Only the rows that scale with delegator count stay inside the window.
	for epoch := range firstRetainedEpoch {
		snapshots, err := meta.GetPoolStakeSnapshotsByEpoch(
			epoch, models.PoolStakeSnapshotTypeMark, nil,
		)
		require.NoError(t, err, "get pool stake snapshots %d", epoch)
		require.Empty(
			t,
			snapshots,
			"pool_stake_snapshot for epoch %d must be pruned",
			epoch,
		)
		stakeInputs, err := meta.GetRewardStakeInputs(epoch, nil)
		require.NoError(t, err, "get reward stake inputs %d", epoch)
		require.Empty(
			t,
			stakeInputs,
			"reward_stake_input for epoch %d must be pruned",
			epoch,
		)
		accountOutputs, err := meta.GetRewardAccountOutputs(epoch, nil)
		require.NoError(t, err, "get reward account outputs %d", epoch)
		require.Empty(
			t,
			accountOutputs,
			"reward_account_output for epoch %d must be pruned",
			epoch,
		)
	}

	for epoch := firstRetainedEpoch; epoch <= currentEpoch; epoch++ {
		snapshots, err := meta.GetPoolStakeSnapshotsByEpoch(
			epoch, models.PoolStakeSnapshotTypeMark, nil,
		)
		require.NoError(t, err, "get pool stake snapshots %d", epoch)
		require.Len(
			t,
			snapshots,
			1,
			"pool_stake_snapshot for epoch %d must be retained",
			epoch,
		)
		stakeInputs, err := meta.GetRewardStakeInputs(epoch, nil)
		require.NoError(t, err, "get reward stake inputs %d", epoch)
		require.Len(
			t,
			stakeInputs,
			1,
			"reward_stake_input for epoch %d must be retained",
			epoch,
		)
		accountOutputs, err := meta.GetRewardAccountOutputs(epoch, nil)
		require.NoError(t, err, "get reward account outputs %d", epoch)
		require.Len(
			t,
			accountOutputs,
			1,
			"reward_account_output for epoch %d must be retained",
			epoch,
		)
	}
}

// TestCleanupOldSnapshotsBelowWindowKeepsEverything covers the early-sync case
// where there is not yet enough history to prune anything.
func TestCleanupOldSnapshotsBelowWindowKeepsEverything(t *testing.T) {
	db := setupTestDB(t)
	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	meta := db.Metadata()

	const currentEpoch = uint64(2)
	poolKeyHash := bytes.Repeat([]byte{0xcc}, 28)

	seedRetentionRows(t, db, poolKeyHash, currentEpoch)
	require.NoError(
		t,
		mgr.cleanupOldSnapshots(context.Background(), currentEpoch),
	)

	for epoch := uint64(0); epoch <= currentEpoch; epoch++ {
		summary, err := meta.GetEpochSummary(epoch, nil)
		require.NoError(t, err, "get epoch summary %d", epoch)
		require.NotNil(t, summary, "epoch_summary %d must be retained", epoch)
		snapshots, err := meta.GetPoolStakeSnapshotsByEpoch(
			epoch, models.PoolStakeSnapshotTypeMark, nil,
		)
		require.NoError(t, err, "get pool stake snapshots %d", epoch)
		require.Len(t, snapshots, 1, "pool_stake_snapshot %d retained", epoch)
	}
}
