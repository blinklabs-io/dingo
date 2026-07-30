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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"github.com/blinklabs-io/dingo/database/models"
)

// explainRewardAccountOutputsByCredentialPlan runs EXPLAIN QUERY PLAN for the
// exact query GetAccountOutputsByCredential issues and returns the "detail"
// column of every plan row. SQLite's EXPLAIN QUERY PLAN output has columns
// (id, parent, notused, detail); only detail is needed here.
func explainRewardAccountOutputsByCredentialPlan(
	t *testing.T,
	db *gorm.DB,
) []string {
	t.Helper()
	var plan []struct {
		Detail string `gorm:"column:detail"`
	}
	require.NoError(t, db.Raw(
		`EXPLAIN QUERY PLAN SELECT * FROM reward_account_output
		 WHERE credential_tag = ? AND staking_key = ? AND spendable = ?
		 ORDER BY epoch ASC, pool_key_hash ASC, reward_type ASC
		 LIMIT ? OFFSET ?`,
		0, rewardStateTestHash(0x01), true, 100, 0,
	).Scan(&plan).Error)
	details := make([]string, 0, len(plan))
	for _, row := range plan {
		details = append(details, row.Detail)
	}
	return details
}

// TestGetRewardAccountOutputsByCredential covers an account with reward rows
// spanning more than one epoch and pool, verifying the credential filter,
// default ascending order, the pool_key_hash/reward_type tie-break within an
// epoch, and pagination. This is the query backing the Blockfrost account
// reward-history endpoint (dingo #1875). All rows here are spendable; see
// TestGetRewardAccountOutputsByCredentialExcludesNonSpendable for the
// Finding 1 regression coverage.
func TestGetRewardAccountOutputsByCredential(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	stakingKey := rewardStateTestHash(0x01)
	otherStakingKey := rewardStateTestHash(0x02)
	poolA := rewardStateTestHash(0xaa)
	poolB := rewardStateTestHash(0xbb)

	require.NoError(t, store.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{Epoch: 1, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: poolA, RewardType: "member", Amount: 100, Spendable: true},
		{Epoch: 2, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: poolB, RewardType: "member", Amount: 200, Spendable: true},
		{Epoch: 3, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: poolA, RewardType: "leader", Amount: 300, Spendable: true},
		{Epoch: 3, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: poolB, RewardType: "member", Amount: 50, Spendable: true},
		// Different credential: must never leak into the results below.
		{Epoch: 1, CredentialTag: 0, StakingKey: otherStakingKey, PoolKeyHash: poolA, RewardType: "member", Amount: 999, Spendable: true},
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

// TestGetRewardAccountOutputsByCredentialUsesIndex pins that
// idx_reward_account_output_credential_spendable (credential_tag,
// staking_key, spendable, epoch, pool_key_hash, reward_type) is what makes
// GetRewardAccountOutputsByCredential affordable once dingo #1875 lets
// reward_account_output grow without bound in API storage mode: without a
// credential-leading index, the existing
// idx_reward_account_output_epoch_cred_pool_type index (which leads with
// epoch) cannot serve the credential_tag/staking_key/spendable predicate, and
// SQLite falls back to a full index/table scan — the query returns the same
// rows either way, so a plain correctness test would not catch an index
// being dropped or renamed out from under this query. This asserts on the
// query plan itself so that regression fails loudly instead of silently
// degrading to O(table size) per request. It also pins that spendable is
// part of the index's search key (not just a post-filter), which is what
// keeps the Finding 1 spendable=true filter a pure index seek.
func TestGetRewardAccountOutputsByCredentialUsesIndex(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	require.NoError(t, store.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{
			Epoch: 1, CredentialTag: 0, StakingKey: rewardStateTestHash(0x01),
			PoolKeyHash: rewardStateTestHash(0xaa), RewardType: "member",
			Amount: 1, Spendable: true,
		},
	}, nil))

	plan := explainRewardAccountOutputsByCredentialPlan(t, store.DB())
	require.NotEmpty(t, plan)

	sawCredentialIndex := false
	for _, detail := range plan {
		upper := strings.ToUpper(detail)
		require.NotContains(
			t, upper, "SCAN REWARD_ACCOUNT_OUTPUT",
			"query must not fall back to a full scan: %v", plan,
		)
		if strings.Contains(detail, "idx_reward_account_output_credential_spendable") {
			sawCredentialIndex = true
			// The search key itself must include spendable, not just
			// credential_tag/staking_key, or the filter is a
			// seek-plus-filter rather than a pure index range scan.
			require.Contains(
				t, detail, "spendable=?",
				"expected spendable in the index search key, got: %v", detail,
			)
		}
	}
	require.True(
		t, sawCredentialIndex,
		"expected the query plan to use idx_reward_account_output_credential_spendable, got: %v",
		plan,
	)
}

// TestGetRewardAccountOutputsByCredentialExcludesNonSpendable pins Finding 1:
// a row whose reward was never actually credited (Spendable = false, e.g. a
// credential that deregistered before its reward's payout boundary — see
// finalizePrecomputedRewardOutputs in ledger/reward_calculation.go) must be
// absent from both the returned rows and the count. Before this fix the
// query filtered only on credential, so this row would have been returned
// and counted as a reward the account received, even though it was never
// paid.
func TestGetRewardAccountOutputsByCredentialExcludesNonSpendable(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	stakingKey := rewardStateTestHash(0x06)
	pool := rewardStateTestHash(0xdd)

	require.NoError(t, store.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{Epoch: 10, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: pool, RewardType: "member", Amount: 1_000_000, Spendable: true},
		// Deregistered before the payout boundary: never credited.
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
	require.Equal(t, uint64(1_000_000), uint64(rows[0].Amount))
	require.True(t, rows[0].Spendable)
}
