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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// fixedFloorGuard builds a PoolSnapshotRetentionGuard that lowers the prune
// boundary to a fixed floor, standing in for
// LedgerState.PrunePoolSnapshotsWithRetentionFloor in snapshot-package tests.
func fixedFloorGuard(floor uint64, ok bool) PoolSnapshotRetentionGuard {
	return func(
		defaultBefore uint64,
		minBefore uint64,
		prune func(before uint64) error,
	) error {
		before := defaultBefore
		if ok && floor < before {
			before = floor
		}
		if before < minBefore {
			before = minBefore
		}
		return prune(before)
	}
}

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

// TestRotateSnapshotsPreservesCapturedLeiosKeyAcrossPoolRotation exercises the
// production Mark->Set addressing path: epoch 9 uses mark[8], even after the
// live pool row has rotated to a new key. The committee key must remain the one
// frozen with mark[8], not whichever key the pool carries when it is queried.
func TestRotateSnapshotsPreservesCapturedLeiosKeyAcrossPoolRotation(
	t *testing.T,
) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{{
		EpochId:       7,
		StartSlot:     100,
		LengthInSlots: 100,
	}})

	poolKeyHash := bytes.Repeat([]byte{0x41}, 28)
	oldPublic := bytes.Repeat([]byte{0x51}, 96)
	oldProof := bytes.Repeat([]byte{0x61}, 48)
	importPool := func(slot uint64, public, proof []byte) {
		t.Helper()
		pool := &models.Pool{
			PoolKeyHash:             append([]byte(nil), poolKeyHash...),
			VrfKeyHash:              bytes.Repeat([]byte{0x71}, 32),
			LeiosKeyPublic:          append([]byte(nil), public...),
			LeiosKeyPossessionProof: append([]byte(nil), proof...),
		}
		registration := &models.PoolRegistration{
			PoolKeyHash:             append([]byte(nil), poolKeyHash...),
			VrfKeyHash:              bytes.Repeat([]byte{0x71}, 32),
			AddedSlot:               slot,
			LeiosKeyPublic:          append([]byte(nil), public...),
			LeiosKeyPossessionProof: append([]byte(nil), proof...),
		}
		require.NoError(t, db.ImportPool(nil, pool, registration))
	}
	importPool(50, oldPublic, oldProof)

	var poolHash lcommon.PoolKeyHash
	copy(poolHash[:], poolKeyHash)
	distribution := &StakeDistribution{
		Slot:           199,
		PoolStakes:     map[lcommon.PoolKeyHash]uint64{poolHash: 100},
		DelegatorCount: map[lcommon.PoolKeyHash]uint64{poolHash: 1},
		TotalStake:     100,
		TotalPools:     1,
	}
	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	saved, err := mgr.saveSnapshot(
		context.Background(),
		8,
		models.PoolStakeSnapshotTypeMark,
		distribution,
		event.EpochTransitionEvent{
			PreviousEpoch: 7,
			NewEpoch:      8,
			BoundarySlot:  200,
			SnapshotSlot:  199,
		},
		false,
		false,
		false,
	)
	require.NoError(t, err)
	require.True(t, saved)

	newPublic := bytes.Repeat([]byte{0x52}, 96)
	newProof := bytes.Repeat([]byte{0x62}, 48)
	importPool(250, newPublic, newProof)
	current, err := db.Metadata().GetPools(
		[]lcommon.PoolKeyHash{poolHash}, nil,
	)
	require.NoError(t, err)
	require.Len(t, current, 1)
	require.Equal(t, newPublic, current[0].LeiosKeyPublic)

	// This is the production rotation path: Set/Go are addressed by older Mark
	// epoch numbers instead of copying rows to new snapshot_type values.
	mgr.rotateSnapshots(context.Background(), 9)
	stored, err := db.Metadata().GetPoolStakeSnapshot(
		8,
		models.PoolStakeSnapshotTypeMark,
		poolKeyHash,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, oldPublic, stored.LeiosKeyPublic,
		"mark[8] must retain the key captured before the live rotation")
	require.Equal(t, oldProof, stored.LeiosKeyPossessionProof)
}

// TestCleanupOldSnapshotsRetentionFloorRetainsDeferredHeaderEpochs is the
// snapshot-side regression guard for issue #3727. When a queued/deferred header
// still needs an older epoch's mark snapshot for leader validation, the
// retention-floor provider reports that epoch and cleanupOldSnapshots must keep
// the pool_stake_snapshot rows at/above it instead of pruning them at the
// default currentEpoch-3 boundary — otherwise the deferred header would read
// the pruned rows back as a zero-stake "pool absent" answer. The reward-state
// retention window is unaffected: only pool snapshots are pinned.
func TestCleanupOldSnapshotsRetentionFloorRetainsDeferredHeaderEpochs(
	t *testing.T,
) {
	db := setupTestDB(t)
	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	meta := db.Metadata()

	const currentEpoch = uint64(28)
	poolKeyHash := bytes.Repeat([]byte{0xaa}, 28)
	seedRetentionRows(t, db, poolKeyHash, currentEpoch)

	// A deferred header requires the epoch-10 mark snapshot (its producer's
	// leader-eligibility basis). Pin retention there.
	const pinnedEpoch = uint64(10)
	mgr.SetPoolSnapshotRetentionGuard(fixedFloorGuard(pinnedEpoch, true))

	require.NoError(
		t,
		mgr.cleanupOldSnapshots(context.Background(), currentEpoch),
	)

	// Pool snapshots from the pinned epoch up to current must survive, even
	// though 10..24 are below the default currentEpoch-3 (25) window.
	for epoch := pinnedEpoch; epoch <= currentEpoch; epoch++ {
		snapshots, err := meta.GetPoolStakeSnapshotsByEpoch(
			epoch, models.PoolStakeSnapshotTypeMark, nil,
		)
		require.NoError(t, err, "get pool stake snapshots %d", epoch)
		require.Len(
			t,
			snapshots,
			1,
			"pinned pool_stake_snapshot for epoch %d must be retained",
			epoch,
		)
	}

	// Snapshots strictly below the pin are still pruned.
	for epoch := range pinnedEpoch {
		snapshots, err := meta.GetPoolStakeSnapshotsByEpoch(
			epoch, models.PoolStakeSnapshotTypeMark, nil,
		)
		require.NoError(t, err, "get pool stake snapshots %d", epoch)
		require.Empty(
			t,
			snapshots,
			"pool_stake_snapshot below the pin (epoch %d) must be pruned",
			epoch,
		)
	}

	// The reward window is NOT widened by the pin: reward_stake_input keeps the
	// default currentEpoch-3 retention, so an epoch below it (but at/above the
	// pin) is still pruned there.
	const firstRewardRetained = currentEpoch - 3
	stakeInputs, err := meta.GetRewardStakeInputs(pinnedEpoch, nil)
	require.NoError(t, err)
	require.Empty(
		t,
		stakeInputs,
		"reward_stake_input at the pinned epoch must still be pruned (pin covers pool snapshots only)",
	)
	retainedInputs, err := meta.GetRewardStakeInputs(firstRewardRetained, nil)
	require.NoError(t, err)
	require.Len(
		t,
		retainedInputs,
		1,
		"reward_stake_input inside the default window must be retained",
	)
}

// TestCleanupOldSnapshotsRetentionFloorAboveWindowIsNoop verifies the pin only
// ever widens retention: a floor at/above the default currentEpoch-3 boundary
// changes nothing, and the default pruning still applies.
func TestCleanupOldSnapshotsRetentionFloorAboveWindowIsNoop(t *testing.T) {
	db := setupTestDB(t)
	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	meta := db.Metadata()

	const currentEpoch = uint64(10)
	const firstRetainedEpoch = currentEpoch - 3
	poolKeyHash := bytes.Repeat([]byte{0xaa}, 28)
	seedRetentionRows(t, db, poolKeyHash, currentEpoch)

	// Floor above the default window: must not resurrect pruning below it.
	mgr.SetPoolSnapshotRetentionGuard(fixedFloorGuard(currentEpoch, true))

	require.NoError(
		t,
		mgr.cleanupOldSnapshots(context.Background(), currentEpoch),
	)

	for epoch := range firstRetainedEpoch {
		snapshots, err := meta.GetPoolStakeSnapshotsByEpoch(
			epoch, models.PoolStakeSnapshotTypeMark, nil,
		)
		require.NoError(t, err, "get pool stake snapshots %d", epoch)
		require.Empty(
			t,
			snapshots,
			"epoch %d below the default window must still be pruned",
			epoch,
		)
	}
}

// TestCleanupOldSnapshotsRetentionDepthCapBounds proves the hard backstop
// (issue #3727, finding 5): even when the retention floor would pin a very old
// epoch, cleanupOldSnapshots never retains more than poolSnapshotRetentionMaxDepth
// epochs BELOW the current epoch of pool snapshots (the boundary epoch
// current-MaxDepth is retained, so the retained span is MaxDepth+1 epochs
// inclusive), so a stuck deferred header cannot pin them without bound.
func TestCleanupOldSnapshotsRetentionDepthCapBounds(t *testing.T) {
	db := setupTestDB(t)
	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	meta := db.Metadata()

	const currentEpoch = uint64(40)
	poolKeyHash := bytes.Repeat([]byte{0xaa}, 28)
	seedRetentionRows(t, db, poolKeyHash, currentEpoch)

	// A floor far below the cap: without the backstop this would retain epoch
	// 2 upward. The cap must clamp retention to currentEpoch - MaxDepth.
	mgr.SetPoolSnapshotRetentionGuard(fixedFloorGuard(2, true))
	require.NoError(
		t,
		mgr.cleanupOldSnapshots(context.Background(), currentEpoch),
	)

	firstRetained := currentEpoch - poolSnapshotRetentionMaxDepth
	for epoch := uint64(0); epoch < firstRetained; epoch++ {
		snaps, err := meta.GetPoolStakeSnapshotsByEpoch(
			epoch, models.PoolStakeSnapshotTypeMark, nil,
		)
		require.NoError(t, err, "epoch %d", epoch)
		require.Empty(
			t,
			snaps,
			"epoch %d below the depth cap must be pruned despite the low floor",
			epoch,
		)
	}
	for epoch := firstRetained; epoch <= currentEpoch; epoch++ {
		snaps, err := meta.GetPoolStakeSnapshotsByEpoch(
			epoch, models.PoolStakeSnapshotTypeMark, nil,
		)
		require.NoError(t, err, "epoch %d", epoch)
		require.Len(
			t,
			snaps,
			1,
			"epoch %d within the depth cap must be retained",
			epoch,
		)
	}
}
