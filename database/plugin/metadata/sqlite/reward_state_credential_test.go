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

package sqlite

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
)

// TestGetRewardAccountOutputsByCredential covers an account with reward rows
// spanning more than one epoch and pool, verifying the credential filter,
// default ascending order, the pool_key_hash/reward_type tie-break within an
// epoch, and pagination. This is the query backing the Blockfrost account
// reward-history endpoint (dingo #1875).
func TestGetRewardAccountOutputsByCredential(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	stakingKey := rewardStateTestHash(0x01)
	otherStakingKey := rewardStateTestHash(0x02)
	poolA := rewardStateTestHash(0xaa)
	poolB := rewardStateTestHash(0xbb)

	require.NoError(t, store.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{Epoch: 1, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: poolA, RewardType: "member", Amount: 100},
		{Epoch: 2, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: poolB, RewardType: "member", Amount: 200},
		{Epoch: 3, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: poolA, RewardType: "leader", Amount: 300},
		{Epoch: 3, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: poolB, RewardType: "member", Amount: 50},
		// Different credential: must never leak into the results below.
		{Epoch: 1, CredentialTag: 0, StakingKey: otherStakingKey, PoolKeyHash: poolA, RewardType: "member", Amount: 999},
	}, nil))

	count, err := store.CountRewardAccountOutputsByCredential(0, stakingKey, nil)
	require.NoError(t, err)
	require.Equal(t, 4, count)

	rows, err := store.GetRewardAccountOutputsByCredential(
		0, stakingKey, 100, 0, "asc", nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 4)
	require.Equal(t, uint64(1), rows[0].Epoch)
	require.Equal(t, uint64(2), rows[1].Epoch)
	// Epoch 3 has two rows; pool_key_hash breaks the tie.
	require.Equal(t, uint64(3), rows[2].Epoch)
	require.Equal(t, poolA, rows[2].PoolKeyHash)
	require.Equal(t, "leader", rows[2].RewardType)
	require.Equal(t, uint64(3), rows[3].Epoch)
	require.Equal(t, poolB, rows[3].PoolKeyHash)

	// Descending order reverses by epoch.
	descRows, err := store.GetRewardAccountOutputsByCredential(
		0, stakingKey, 100, 0, "desc", nil,
	)
	require.NoError(t, err)
	require.Len(t, descRows, 4)
	require.Equal(t, uint64(3), descRows[0].Epoch)
	require.Equal(t, uint64(1), descRows[3].Epoch)

	// Pagination: limit 2, offset 2 returns the second page in ascending order.
	page2, err := store.GetRewardAccountOutputsByCredential(
		0, stakingKey, 2, 2, "asc", nil,
	)
	require.NoError(t, err)
	require.Len(t, page2, 2)
	require.Equal(t, rows[2], page2[0])
	require.Equal(t, rows[3], page2[1])
}

// TestGetRewardAccountOutputsByCredentialEmpty covers a credential with no
// reward_account_output rows: the query must return an empty slice and a
// zero count, not an error.
func TestGetRewardAccountOutputsByCredentialEmpty(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	stakingKey := rewardStateTestHash(0x03)

	count, err := store.CountRewardAccountOutputsByCredential(0, stakingKey, nil)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	rows, err := store.GetRewardAccountOutputsByCredential(
		0, stakingKey, 100, 0, "asc", nil,
	)
	require.NoError(t, err)
	require.Empty(t, rows)
}

// TestDeleteRewardStakeInputBeforeEpoch verifies the API storage-mode
// retention path: only reward_stake_input rows are removed, and
// reward_account_output rows for the same epoch are left untouched.
func TestDeleteRewardStakeInputBeforeEpoch(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	poolKeyHash := rewardStateTestHash(0xcc)
	stakingKey := rewardStateTestHash(0x04)

	require.NoError(t, store.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{Epoch: 1, PoolKeyHash: poolKeyHash, StakingKey: stakingKey, Stake: 1, Registered: true},
		{Epoch: 2, PoolKeyHash: poolKeyHash, StakingKey: stakingKey, Stake: 1, Registered: true},
	}, nil))
	require.NoError(t, store.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{Epoch: 1, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: poolKeyHash, RewardType: "member", Amount: 1},
		{Epoch: 2, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: poolKeyHash, RewardType: "member", Amount: 2},
	}, nil))

	require.NoError(t, store.DeleteRewardStakeInputBeforeEpoch(2, nil))

	stakeInputs1, err := store.GetRewardStakeInputs(1, nil)
	require.NoError(t, err)
	require.Empty(t, stakeInputs1, "reward_stake_input for epoch 1 must be pruned")

	stakeInputs2, err := store.GetRewardStakeInputs(2, nil)
	require.NoError(t, err)
	require.Len(t, stakeInputs2, 1, "reward_stake_input for epoch 2 is inside the window")

	// reward_account_output must survive for BOTH epochs: this function never
	// touches that table.
	accountOutputs1, err := store.GetRewardAccountOutputs(1, nil)
	require.NoError(t, err)
	require.Len(t, accountOutputs1, 1, "reward_account_output must not be pruned by this function")

	accountOutputs2, err := store.GetRewardAccountOutputs(2, nil)
	require.NoError(t, err)
	require.Len(t, accountOutputs2, 1)
}
