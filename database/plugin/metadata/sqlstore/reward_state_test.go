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

package sqlstore

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func TestRewardAccountOutputsExcludeUncreditedRows(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	stakingKey := bytes.Repeat([]byte{0x11}, 28)
	poolKey := bytes.Repeat([]byte{0x22}, 28)
	outputs := []*models.RewardAccountOutput{
		{
			Epoch: 1, StakingKey: stakingKey, PoolKeyHash: poolKey,
			RewardType: "member", Amount: 10, Spendable: true,
		},
		{
			Epoch: 2, StakingKey: stakingKey, PoolKeyHash: poolKey,
			RewardType: "member", Amount: 20, Spendable: false,
		},
		{
			Epoch: 3, StakingKey: stakingKey, PoolKeyHash: poolKey,
			RewardType: "member", Amount: 30, Spendable: true,
			Guarded: true,
		},
	}
	require.NoError(t, store.SaveRewardAccountOutputs(outputs, nil))

	rows, err := store.GetRewardAccountOutputsByCredential(
		0,
		stakingKey,
		100,
		0,
		"asc",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, uint64(1), rows[0].Epoch)
	require.False(t, rows[0].Guarded)

	count, err := store.CountRewardAccountOutputsByCredential(
		0,
		stakingKey,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	all, err := store.GetRewardAccountOutputs(3, nil)
	require.NoError(t, err)
	require.Len(t, all, 1)
	require.True(t, all[0].Guarded)
}

func TestSaveRewardAccountOutputsBatchesAndAssignsIDs(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	outputs := make([]*models.RewardAccountOutput, 250)
	for index := range outputs {
		outputs[index] = &models.RewardAccountOutput{
			Epoch:       uint64(index),
			StakingKey:  []byte{byte(index), 0x11},
			PoolKeyHash: []byte{0x22, byte(index)},
			RewardType:  "member",
			Amount:      types.Uint64(index + 1),
			Spendable:   true,
		}
	}
	require.NoError(t, store.SaveRewardAccountOutputs(outputs, nil))
	for _, output := range outputs {
		require.NotZero(t, output.ID)
	}
	rows, err := store.GetRewardAccountOutputs(249, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	// Replaying the same natural keys updates in place and retains IDs.
	ids := make([]uint, len(outputs))
	for index, output := range outputs {
		ids[index] = output.ID
	}
	require.NoError(t, store.SaveRewardAccountOutputs(outputs, nil))
	for index, output := range outputs {
		require.Equal(t, ids[index], output.ID)
	}
	duplicateA := &models.RewardAccountOutput{
		Epoch: 500, StakingKey: []byte{1}, PoolKeyHash: []byte{2},
		RewardType: "member", Amount: 1, Spendable: true,
	}
	duplicateB := &models.RewardAccountOutput{
		Epoch: 500, StakingKey: []byte{1}, PoolKeyHash: []byte{2},
		RewardType: "member", Amount: 2, Spendable: true,
	}
	require.NoError(
		t,
		store.SaveRewardAccountOutputs(
			[]*models.RewardAccountOutput{duplicateA, duplicateB},
			nil,
		),
	)
	require.Equal(t, duplicateA.ID, duplicateB.ID)
	rows, err = store.GetRewardAccountOutputs(500, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, types.Uint64(2), rows[0].Amount)
}

func TestRewardSeedFailureRoundTripAndRollback(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	require.NoError(t, store.SaveRewardSeedFailure(
		10, "mark", "pool has no reward account", 100, nil,
	))
	reason, err := store.GetRewardSeedFailure(10, "mark", nil)
	require.NoError(t, err)
	require.Equal(t, "pool has no reward account", reason)
	require.NoError(t, store.SaveRewardSeedFailure(
		12, "mark", "pool has no reward account", 50, nil,
	))
	require.NoError(t, store.SaveRewardSeedFailure(
		12, "mark", "pool has no parameters", 200, nil,
	))
	require.NoError(t, store.SaveRewardSeedFailure(
		11, "mark", "missing parameters", 200, nil,
	))
	require.NoError(t, store.DeleteRewardStateAfterSlot(150, nil))
	reason, err = store.GetRewardSeedFailure(10, "mark", nil)
	require.NoError(t, err)
	require.Equal(t, "pool has no reward account", reason)
	reason, err = store.GetRewardSeedFailure(12, "mark", nil)
	require.NoError(t, err)
	require.Equal(t, "pool has no reward account", reason)
	reason, err = store.GetRewardSeedFailure(11, "mark", nil)
	require.NoError(t, err)
	require.Empty(t, reason)
}

// TestImportedBlockCountRollbackRemovesBothTables pins the rollback cleanup for
// the imported block counts. The per-pool rows and the epoch total row are
// deleted by separate statements in DeleteRewardStateAfterSlot, and either one
// surviving alone is worse than both surviving: an epoch total without its rows
// fails the stored-total check on every read, and rows without a total read as
// an epoch nothing was imported for. Rows left above a rollback slot would also
// pair counts from a reverted anchor with the trust boundary of the surviving
// one, which is the disjointness the reward merge assumes.
func TestImportedBlockCountRollbackRemovesBothTables(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	keptPool := bytes.Repeat([]byte{0x01}, 28)
	rolledPool := bytes.Repeat([]byte{0x02}, 28)

	require.NoError(t, store.SaveImportedPoolBlockCounts(
		[]models.ImportedPoolBlockCount{
			{
				PoolKeyHash:    keptPool,
				Epoch:          10,
				BlocksProduced: 7,
				CapturedSlot:   100,
			},
			{
				PoolKeyHash:    rolledPool,
				Epoch:          11,
				BlocksProduced: 9,
				CapturedSlot:   200,
			},
		},
		nil,
	))
	require.NoError(t, store.SaveImportedEpochBlockTotal(10, 7, 100, nil))
	require.NoError(t, store.SaveImportedEpochBlockTotal(11, 9, 200, nil))

	require.NoError(t, store.DeleteRewardStateAfterSlot(150, nil))

	counts, total, known, err := store.GetImportedPoolBlockCounts(10, nil)
	require.NoError(t, err)
	require.True(t, known, "an epoch captured below the rollback slot survives")
	require.Equal(t, uint64(7), total)
	require.Equal(t, map[string]uint64{string(keptPool): 7}, counts)

	counts, total, known, err = store.GetImportedPoolBlockCounts(11, nil)
	require.NoError(t, err,
		"neither table may keep a row captured above the rollback slot")
	require.False(t, known)
	require.Zero(t, total)
	require.Empty(t, counts)

	// Asserted against the table rather than through
	// GetImportedPoolBlockCounts, which answers "unknown" from the missing
	// total row alone and never looks at the per-pool rows. Reading it
	// through the accessor would pass with the per-pool delete removed.
	var rolledRows int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM imported_pool_block_count WHERE epoch = 11",
	).Scan(&rolledRows))
	require.Zero(t, rolledRows,
		"per-pool rows captured above the rollback slot are deleted too")
}
