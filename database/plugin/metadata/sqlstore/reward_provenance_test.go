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
