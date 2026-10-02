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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// setupTestDBWithStorageMode mirrors setupTestDB (calculator_test.go) but
// pins the storage mode, so retention tests can compare CORE vs API mode
// pruning of reward_account_output.
func setupTestDBWithStorageMode(
	t *testing.T,
	storageMode string,
) *database.Database {
	t.Helper()
	tmpDir := t.TempDir()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     tmpDir,
		StorageMode: storageMode,
	})
	require.NoError(t, err, "create database")

	return db
}

// TestCleanupOldSnapshotsCoreModePrunesRewardAccountOutput pins that CORE
// storage mode's retention behavior is unchanged: both
// reward_stake_input and reward_account_output are pruned to the same
// rotation/reward-replay window. Also pins pool_stake_snapshot's own
// CORE-mode window (/: no test previously exercised the
// apiStorageMode branch cleanupOldSnapshots gained for this table) -- CORE
// mode's pool-snapshot pruning is unchanged from before that commit.
func TestCleanupOldSnapshotsCoreModePrunesRewardAccountOutput(t *testing.T) {
	t.Parallel()

	db := setupTestDBWithStorageMode(t, types.StorageModeCore)
	require.Equal(t, types.StorageModeCore, db.StorageMode())
	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	meta := db.Metadata()

	const currentEpoch = uint64(10)
	const firstRetainedEpoch = currentEpoch - 3
	poolKeyHash := bytes.Repeat([]byte{0x11}, 28)

	seedRetentionRows(t, db, poolKeyHash, currentEpoch)
	require.NoError(
		t,
		mgr.cleanupOldSnapshots(context.Background(), currentEpoch),
	)

	for epoch := range firstRetainedEpoch {
		accountOutputs, err := meta.GetRewardAccountOutputs(epoch, nil)
		require.NoError(t, err, "get reward account outputs %d", epoch)
		require.Empty(
			t,
			accountOutputs,
			"core mode must prune reward_account_output for epoch %d",
			epoch,
		)
		stakeInputs, err := meta.GetRewardStakeInputs(epoch, nil)
		require.NoError(t, err, "get reward stake inputs %d", epoch)
		require.Empty(
			t,
			stakeInputs,
			"core mode must prune reward_stake_input for epoch %d",
			epoch,
		)
		poolSnapshots, err := meta.GetPoolStakeSnapshotsByEpoch(
			epoch, models.PoolStakeSnapshotTypeMark, nil,
		)
		require.NoError(t, err, "get pool stake snapshots %d", epoch)
		require.Empty(
			t,
			poolSnapshots,
			"core mode must prune pool_stake_snapshot for epoch %d",
			epoch,
		)
	}
	for epoch := firstRetainedEpoch; epoch <= currentEpoch; epoch++ {
		accountOutputs, err := meta.GetRewardAccountOutputs(epoch, nil)
		require.NoError(t, err, "get reward account outputs %d", epoch)
		require.Len(
			t,
			accountOutputs,
			1,
			"core mode retains reward_account_output inside the window for epoch %d",
			epoch,
		)
		poolSnapshots, err := meta.GetPoolStakeSnapshotsByEpoch(
			epoch, models.PoolStakeSnapshotTypeMark, nil,
		)
		require.NoError(t, err, "get pool stake snapshots %d", epoch)
		require.Len(
			t,
			poolSnapshots,
			1,
			"core mode retains pool_stake_snapshot inside the window for epoch %d",
			epoch,
		)
	}
}

// TestCleanupOldSnapshotsAPIModeRetainsRewardAccountOutput is the
// regression test: in API storage mode, reward_account_output must be
// retained WITHOUT BOUND (so the Blockfrost account reward-history endpoint
// can serve an account's full history), while reward_stake_input still
// cannot be kept and continues to be pruned to the rotation/reward-replay
// window exactly as in core mode. Also the / regression test:
// pool_stake_snapshot must likewise be retained WITHOUT BOUND in API mode,
// so a from-genesis historical Acquire pinned well outside the ordinary
// 3-epoch window can still be validated
// (VerifyPointQueryable's stake-retention check) and answered
// (GetStakeDistribution/GetPoolDistr2) -- confirmed live before this test
// existed, but never previously pinned by an automated test.
func TestCleanupOldSnapshotsAPIModeRetainsRewardAccountOutput(t *testing.T) {
	t.Parallel()

	db := setupTestDBWithStorageMode(t, types.StorageModeAPI)
	require.Equal(t, types.StorageModeAPI, db.StorageMode())
	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	meta := db.Metadata()

	const currentEpoch = uint64(10)
	const firstRetainedEpoch = currentEpoch - 3
	poolKeyHash := bytes.Repeat([]byte{0x22}, 28)

	seedRetentionRows(t, db, poolKeyHash, currentEpoch)
	require.NoError(
		t,
		mgr.cleanupOldSnapshots(context.Background(), currentEpoch),
	)

	// reward_account_output and pool_stake_snapshot both survive for every
	// epoch, including those outside the rotation/reward-replay window.
	for epoch := uint64(0); epoch <= currentEpoch; epoch++ {
		accountOutputs, err := meta.GetRewardAccountOutputs(epoch, nil)
		require.NoError(t, err, "get reward account outputs %d", epoch)
		require.Len(
			t,
			accountOutputs,
			1,
			"API mode must retain reward_account_output for epoch %d",
			epoch,
		)
		poolSnapshots, err := meta.GetPoolStakeSnapshotsByEpoch(
			epoch, models.PoolStakeSnapshotTypeMark, nil,
		)
		require.NoError(t, err, "get pool stake snapshots %d", epoch)
		require.Len(
			t,
			poolSnapshots,
			1,
			"API mode must retain pool_stake_snapshot for epoch %d",
			epoch,
		)
	}

	// reward_stake_input is still pruned to the same window as core mode.
	for epoch := range firstRetainedEpoch {
		stakeInputs, err := meta.GetRewardStakeInputs(epoch, nil)
		require.NoError(t, err, "get reward stake inputs %d", epoch)
		require.Empty(
			t,
			stakeInputs,
			"API mode must still prune reward_stake_input for epoch %d",
			epoch,
		)
	}
	for epoch := firstRetainedEpoch; epoch <= currentEpoch; epoch++ {
		stakeInputs, err := meta.GetRewardStakeInputs(epoch, nil)
		require.NoError(t, err, "get reward stake inputs %d", epoch)
		require.Len(
			t,
			stakeInputs,
			1,
			"reward_stake_input for epoch %d is inside the retained window",
			epoch,
		)
	}
}

// TestCleanupOldSnapshotsKoiosParityRetentionUnbounded is the
// regression test: enabling SetRewardAccountOutputRetentionUnbounded on a
// CORE-mode database (the koios-parity observer's node.go wiring) must retain
// reward_account_output without bound, exactly like API storage mode, instead
// of pruning it to the 4-epoch rotation/reward-replay window. Without this,
// the observer's own network-bound epoch validation routinely falls behind
// chain progression during a catch-up sync and reads an epoch's
// reward_account_output rows only after cleanupOldSnapshots has already
// deleted them, making every koios-parity account check fail permanently with
// a row that genuinely no longer exists.
func TestCleanupOldSnapshotsKoiosParityRetentionUnbounded(t *testing.T) {
	t.Parallel()

	db := setupTestDBWithStorageMode(t, types.StorageModeCore)
	require.Equal(t, types.StorageModeCore, db.StorageMode())
	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	mgr.SetRewardAccountOutputRetentionUnbounded(true)
	require.True(t, mgr.RewardAccountOutputRetentionUnbounded())
	meta := db.Metadata()

	const currentEpoch = uint64(10)
	const firstRetainedEpoch = currentEpoch - 3
	poolKeyHash := bytes.Repeat([]byte{0x44}, 28)

	seedRetentionRows(t, db, poolKeyHash, currentEpoch)
	require.NoError(
		t,
		mgr.cleanupOldSnapshots(context.Background(), currentEpoch),
	)

	// reward_account_output survives for every epoch, including those
	// outside the rotation/reward-replay window, exactly like API mode.
	for epoch := uint64(0); epoch <= currentEpoch; epoch++ {
		accountOutputs, err := meta.GetRewardAccountOutputs(epoch, nil)
		require.NoError(t, err, "get reward account outputs %d", epoch)
		require.Len(
			t,
			accountOutputs,
			1,
			"koios-parity retention must retain reward_account_output for epoch %d",
			epoch,
		)
	}

	// reward_stake_input is still pruned to the same window as core mode.
	for epoch := range firstRetainedEpoch {
		stakeInputs, err := meta.GetRewardStakeInputs(epoch, nil)
		require.NoError(t, err, "get reward stake inputs %d", epoch)
		require.Empty(
			t,
			stakeInputs,
			"koios-parity retention must still prune reward_stake_input for epoch %d",
			epoch,
		)
	}
	for epoch := firstRetainedEpoch; epoch <= currentEpoch; epoch++ {
		stakeInputs, err := meta.GetRewardStakeInputs(epoch, nil)
		require.NoError(t, err, "get reward stake inputs %d", epoch)
		require.Len(
			t,
			stakeInputs,
			1,
			"reward_stake_input for epoch %d is inside the retained window",
			epoch,
		)
	}
}

// TestDeleteRewardStateAfterSlotUnaffectedByAPIModeRetention is the rollback
// correctness check: retaining reward_account_output without
// bound in API storage mode must not stop a rollback from removing rows
// captured above the rollback point. DeleteRewardStateAfterSlot is
// unconditional (it does not read storage mode at all), so this pins that
// behavior directly rather than relying on that being true by omission.
func TestDeleteRewardStateAfterSlotUnaffectedByAPIModeRetention(t *testing.T) {
	t.Parallel()

	db := setupTestDBWithStorageMode(t, types.StorageModeAPI)
	meta := db.Metadata()

	poolKeyHash := bytes.Repeat([]byte{0x33}, 28)
	const throughEpoch = uint64(5)
	seedRetentionRows(t, db, poolKeyHash, throughEpoch)

	// seedRetentionRows uses boundarySlot := epoch * 432000; roll back to a
	// slot inside epoch 3's boundary so epochs 0-2 predate the rollback slot
	// and epochs 3-5 postdate it.
	rollbackSlot := uint64(3)*432000 - 1
	require.NoError(t, meta.DeleteRewardStateAfterSlot(rollbackSlot, nil))

	for epoch := range uint64(3) {
		outputs, err := meta.GetRewardAccountOutputs(epoch, nil)
		require.NoError(t, err, "get reward account outputs %d", epoch)
		require.Len(
			t,
			outputs,
			1,
			"epoch %d predates the rollback slot and must survive",
			epoch,
		)
	}
	for epoch := uint64(3); epoch <= throughEpoch; epoch++ {
		outputs, err := meta.GetRewardAccountOutputs(epoch, nil)
		require.NoError(t, err, "get reward account outputs %d", epoch)
		require.Empty(
			t,
			outputs,
			"epoch %d is above the rollback slot and must be removed even in API mode",
			epoch,
		)
	}
}

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
// Mithril-imported nodes exposed. Rows that scale with delegator count stay
// bounded to the rotation/reward-replay window, while the three tables that
// scale with epoch or pool count — epoch_summary, reward_snapshot,
// reward_pool_input — are kept for the life of the database, so historical
// closed-epoch comparison has per-epoch aggregates and a per-pool reward basis
// to compare against (and a missing summary keeps meaning "never captured").
func TestCleanupOldSnapshotsRetainsEpochSummaries(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{{
		EpochId:       6,
		StartSlot:     0,
		LengthInSlots: 100,
	}, {
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
	require.NotNil(t, stored.LeiosKeyRegistrationEpoch)
	require.Equal(t, uint64(7), *stored.LeiosKeyRegistrationEpoch)
}

func TestRotateSnapshotsPreservesLeiosKeyWhenImportedAgeIsUnknown(
	t *testing.T,
) {
	t.Parallel()

	db := setupTestDB(t)
	// A Mithril registration has an import slot but no source registration
	// slot. Even when retained epoch history maps that synthetic slot, it must
	// not restart the voting-key TTL.
	seedEpochs(t, db, []models.Epoch{{
		EpochId:       6,
		StartSlot:     0,
		LengthInSlots: 100,
	}, {
		EpochId:       7,
		StartSlot:     100,
		LengthInSlots: 100,
	}})

	poolKeyHash := bytes.Repeat([]byte{0x41}, 28)
	publicKey := bytes.Repeat([]byte{0x51}, 96)
	proof := bytes.Repeat([]byte{0x61}, 48)
	pool := &models.Pool{
		PoolKeyHash:             append([]byte(nil), poolKeyHash...),
		VrfKeyHash:              bytes.Repeat([]byte{0x71}, 32),
		LeiosKeyPublic:          append([]byte(nil), publicKey...),
		LeiosKeyPossessionProof: append([]byte(nil), proof...),
	}
	registration := &models.PoolRegistration{
		PoolKeyHash:                    append([]byte(nil), poolKeyHash...),
		VrfKeyHash:                     bytes.Repeat([]byte{0x71}, 32),
		AddedSlot:                      50,
		LeiosKeyPublic:                 append([]byte(nil), publicKey...),
		LeiosKeyPossessionProof:        append([]byte(nil), proof...),
		LeiosKeyRegistrationAgeUnknown: true,
	}
	require.NoError(t, db.ImportPool(nil, pool, registration))

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

	stored, err := db.Metadata().GetPoolStakeSnapshot(
		8,
		models.PoolStakeSnapshotTypeMark,
		poolKeyHash,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, stored)
	require.Equal(t, publicKey, stored.LeiosKeyPublic,
		"the effective key bytes must survive a missing epoch-to-slot mapping")
	require.Equal(t, proof, stored.LeiosKeyPossessionProof)
	require.Nil(t, stored.LeiosKeyRegistrationEpoch,
		"unknown registration age must remain distinguishable from an absent key")
}

// TestCleanupOldSnapshotsRetentionFloorRetainsDeferredHeaderEpochs is the
// snapshot-side regression guard. When a queued/deferred header
// still needs an older epoch's mark snapshot for leader validation, the
// retention-floor provider reports that epoch and cleanupOldSnapshots must keep
// the pool_stake_snapshot rows at/above it instead of pruning them at the
// default currentEpoch-3 boundary — otherwise the deferred header would read
// the pruned rows back as a zero-stake "pool absent" answer. The reward-state
// retention window is unaffected: only pool snapshots are pinned.
func TestCleanupOldSnapshotsRetentionFloorRetainsDeferredHeaderEpochs(
	t *testing.T,
) {
	t.Parallel()

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
	t.Parallel()

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

// TestCleanupOldSnapshotsRetentionDepthCapBounds proves the hard backstop:
// even when the retention floor would pin a very old epoch, cleanupOldSnapshots
// never retains more than poolSnapshotRetentionMaxDepth epochs BELOW the
// current epoch of pool snapshots (the boundary epoch current-MaxDepth is
// retained, so the retained span is MaxDepth+1 epochs inclusive), so a stuck
// deferred header cannot pin them without bound.
func TestCleanupOldSnapshotsRetentionDepthCapBounds(t *testing.T) {
	t.Parallel()

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
	for epoch := range firstRetained {
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
