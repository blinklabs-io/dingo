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

//go:build dingo_extra_plugins

package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
)

// TestGetRewardAccountOutputsByCredentialPostgres exercises the
// credential-filtered reward-history query (backing the Blockfrost account
// reward-history endpoint, dingo #1875) against postgres, covering pagination
// and ordering.
func TestGetRewardAccountOutputsByCredentialPostgres(t *testing.T) {
	store := newTestPostgresStore(t)
	t.Cleanup(func() { _ = store.Close() })
	db := store.DB()

	stakingKey := testHash28("reward-history-cred")
	poolA := testHash28("reward-history-pool-a")
	poolB := testHash28("reward-history-pool-b")

	t.Cleanup(func() {
		_ = db.Where("staking_key = ?", stakingKey).
			Delete(&models.RewardAccountOutput{}).Error
	})

	require.NoError(t, store.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{Epoch: 1, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: poolA, RewardType: "member", Amount: 100, Spendable: true},
		{Epoch: 2, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: poolB, RewardType: "member", Amount: 200, Spendable: true},
	}, nil))

	count, err := store.CountRewardAccountOutputsByCredential(0, stakingKey, nil)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	rows, err := store.GetRewardAccountOutputsByCredential(
		0, stakingKey, 1, 1, "asc", nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, uint64(2), rows[0].Epoch)
}

// TestGetRewardAccountOutputsByCredentialExcludesNonSpendablePostgres pins
// Finding 1 against postgres: a row whose reward was never actually
// credited (Spendable = false, e.g. a credential that deregistered before
// its reward's payout boundary) must be absent from both the returned rows
// and the count.
func TestGetRewardAccountOutputsByCredentialExcludesNonSpendablePostgres(t *testing.T) {
	store := newTestPostgresStore(t)
	t.Cleanup(func() { _ = store.Close() })
	db := store.DB()

	stakingKey := testHash28("reward-history-nonspendable-cred")
	pool := testHash28("reward-history-nonspendable-pool")

	t.Cleanup(func() {
		_ = db.Where("staking_key = ?", stakingKey).
			Delete(&models.RewardAccountOutput{}).Error
	})

	require.NoError(t, store.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{Epoch: 10, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: pool, RewardType: "member", Amount: 1_000_000, Spendable: true},
		{Epoch: 11, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: pool, RewardType: "member", Amount: 9_999_999, Spendable: false},
	}, nil))

	count, err := store.CountRewardAccountOutputsByCredential(0, stakingKey, nil)
	require.NoError(t, err)
	require.Equal(t, 1, count, "the non-spendable row must not be counted")

	rows, err := store.GetRewardAccountOutputsByCredential(
		0, stakingKey, 100, 0, "asc", nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1, "the non-spendable row must be absent from the results")
	require.Equal(t, uint64(10), rows[0].Epoch)
}

// TestDeleteRewardStakeInputBeforeEpochPostgres verifies the API storage-mode
// retention path on postgres: reward_stake_input is pruned while
// reward_account_output is left untouched.
func TestDeleteRewardStakeInputBeforeEpochPostgres(t *testing.T) {
	store := newTestPostgresStore(t)
	t.Cleanup(func() { _ = store.Close() })
	db := store.DB()

	poolKeyHash := testHash28("reward-history-retention-pool")
	stakingKey := testHash28("reward-history-retention-cred")

	t.Cleanup(func() {
		_ = db.Where("staking_key = ?", stakingKey).
			Delete(&models.RewardStakeInput{}).Error
		_ = db.Where("staking_key = ?", stakingKey).
			Delete(&models.RewardAccountOutput{}).Error
	})

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
	for _, row := range stakeInputs1 {
		require.NotEqual(t, stakingKey, row.StakingKey, "epoch 1 reward_stake_input must be pruned")
	}

	accountOutputs1, err := store.GetRewardAccountOutputs(1, nil)
	require.NoError(t, err)
	found := false
	for _, row := range accountOutputs1 {
		if string(row.StakingKey) == string(stakingKey) {
			found = true
		}
	}
	require.True(t, found, "reward_account_output for epoch 1 must survive")
}
