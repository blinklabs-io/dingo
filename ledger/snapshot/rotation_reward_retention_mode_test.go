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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	dbtest "github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/event"
)

// setupTestDBWithStorageMode mirrors setupTestDB (calculator_test.go) but
// pins the storage mode, so retention tests can compare CORE vs API mode
// pruning of reward_account_output (dingo #1875).
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
// storage mode's retention behavior is unchanged by dingo #1875: both
// reward_stake_input and reward_account_output are pruned to the same
// rotation/reward-replay window.
func TestCleanupOldSnapshotsCoreModePrunesRewardAccountOutput(t *testing.T) {
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
	}
}

// TestCleanupOldSnapshotsAPIModeRetainsRewardAccountOutput is the dingo #1875
// regression test: in API storage mode, reward_account_output must be
// retained WITHOUT BOUND (so the Blockfrost account reward-history endpoint
// can serve an account's full history), while reward_stake_input still
// cannot be kept and continues to be pruned to the rotation/reward-replay
// window exactly as in core mode.
func TestCleanupOldSnapshotsAPIModeRetainsRewardAccountOutput(t *testing.T) {
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

	// reward_account_output survives for every epoch, including those
	// outside the rotation/reward-replay window.
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

// TestDeleteRewardStateAfterSlotUnaffectedByAPIModeRetention is the rollback
// correctness check for dingo #1875: retaining reward_account_output without
// bound in API storage mode must not stop a rollback from removing rows
// captured above the rollback point. DeleteRewardStateAfterSlot is
// unconditional (it does not read storage mode at all), so this pins that
// behavior directly rather than relying on that being true by omission.
func TestDeleteRewardStateAfterSlotUnaffectedByAPIModeRetention(t *testing.T) {
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
