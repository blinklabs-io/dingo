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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
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

func TestRewardAccountGuardedQueryUsesIndex(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	rows, err := store.writeDB.Query(`
EXPLAIN QUERY PLAN
SELECT staking_key, pool_key_hash, reward_type, id, epoch, credential_tag,
       amount, spendable, guarded, captured_slot, boundary_slot
FROM reward_account_output
WHERE credential_tag = ? AND staking_key = ?
  AND spendable = TRUE AND guarded = FALSE
ORDER BY epoch ASC, pool_key_hash ASC, reward_type ASC
LIMIT ? OFFSET ?`,
		0,
		bytes.Repeat([]byte{0x11}, 28),
		100,
		0,
	)
	require.NoError(t, err)
	defer rows.Close()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		details = append(details, detail)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(t, details)
	plan := strings.Join(details, "\n")
	require.Contains(
		t,
		plan,
		"idx_reward_account_output_credential_spendable_guarded",
	)
	require.Contains(t, plan, "guarded=?")
	require.NotContains(t, strings.ToUpper(plan), "SCAN REWARD_ACCOUNT_OUTPUT")
}

func TestStakeCalculationVersionRoundTrip(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	poolKey := bytes.Repeat([]byte{0x33}, 28)
	poolSnapshot := &models.PoolStakeSnapshot{
		Epoch:              10,
		SnapshotType:       models.PoolStakeSnapshotTypeMark,
		PoolKeyHash:        poolKey,
		CalculationVersion: models.RewardStakeCalculationVersion,
	}
	require.NoError(t, store.SavePoolStakeSnapshot(poolSnapshot, nil))
	gotPool, err := store.GetPoolStakeSnapshot(
		10,
		models.PoolStakeSnapshotTypeMark,
		poolKey,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, gotPool)
	require.Equal(
		t,
		models.RewardStakeCalculationVersion,
		gotPool.CalculationVersion,
	)

	rewardSnapshot := &models.RewardSnapshot{
		Epoch:              10,
		SnapshotType:       models.PoolStakeSnapshotTypeMark,
		CalculationVersion: models.RewardStakeCalculationVersion,
	}
	require.NoError(t, store.SaveRewardSnapshot(rewardSnapshot, nil))
	gotReward, err := store.GetRewardSnapshot(
		10,
		models.PoolStakeSnapshotTypeMark,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, gotReward)
	require.Equal(
		t,
		models.RewardStakeCalculationVersion,
		gotReward.CalculationVersion,
	)
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

func TestV1Alpha1AddressTransactionIndex(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)
	var count int
	require.NoError(t, store.writeDB.QueryRow(`
SELECT COUNT(*)
FROM pragma_index_info('idx_addr_tx_stake_position')
WHERE (seqno = 0 AND name = 'credential_tag')
   OR (seqno = 1 AND name = 'staking_key')
   OR (seqno = 2 AND name = 'slot')
   OR (seqno = 3 AND name = 'tx_index')
   OR (seqno = 4 AND name = 'payment_key')`).Scan(&count))
	require.Equal(t, 5, count)
}
