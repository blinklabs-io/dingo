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

// TestGetRewardAccountOutputsByCredentialExcludesGuarded pins dingo #3021: a
// row whose reward was withheld by the CIP-0163 reward-crediting guard
// (Guarded = true, ledger/reward_calculation.go rewardOutputGuarded /
// applyGuardedFlagToAccountOutputs) must be absent from both the returned
// rows and the count, the same way a Spendable = false row already is
// (TestGetRewardAccountOutputsByCredentialExcludesNonSpendable). A guarded
// row keeps Spendable = true -- it is not a deregistration -- so this is a
// distinct code path from that existing test, not a duplicate of it: without
// the guarded filter, this row would previously have passed the
// spendable = true check and been reported as received even though it was
// never credited.
func TestGetRewardAccountOutputsByCredentialExcludesGuarded(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	stakingKey := rewardStateTestHash(0x08)
	pool := rewardStateTestHash(0xee)

	require.NoError(t, store.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{Epoch: 20, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: pool, RewardType: "member", Amount: 1_000_000, Spendable: true, Guarded: false},
		// CIP-0163-expired reward account: guarded, but still nominally
		// spendable (it was not deregistered).
		{Epoch: 21, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: pool, RewardType: "leader", Amount: 9_999_999, Spendable: true, Guarded: true},
	}, nil))

	count, err := store.CountRewardAccountOutputsByCredential(0, stakingKey, nil)
	require.NoError(t, err)
	require.Equal(t, 1, count, "the guarded row must not be counted")

	rows, err := store.GetRewardAccountOutputsByCredential(
		0, stakingKey, 100, 0, "asc", nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1, "the guarded row must be absent from the results")
	require.Equal(t, uint64(20), rows[0].Epoch)
	require.Equal(t, uint64(1_000_000), uint64(rows[0].Amount))
	require.True(t, rows[0].Spendable)
	require.False(t, rows[0].Guarded)
}

// TestGetRewardAccountOutputsByCredentialExcludesNonSpendableAndGuardedTogether
// covers an account with both withholding reasons present at once (a
// deregistered/non-spendable row and a separate CIP-0163-guarded row)
// alongside a normal credited row, pinning that both filters apply
// simultaneously and independently.
func TestGetRewardAccountOutputsByCredentialExcludesNonSpendableAndGuardedTogether(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	stakingKey := rewardStateTestHash(0x09)
	pool := rewardStateTestHash(0xef)

	require.NoError(t, store.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{Epoch: 30, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: pool, RewardType: "member", Amount: 10, Spendable: true, Guarded: false},
		{Epoch: 31, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: pool, RewardType: "member", Amount: 20, Spendable: false, Guarded: false},
		{Epoch: 32, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: pool, RewardType: "leader", Amount: 30, Spendable: true, Guarded: true},
	}, nil))

	count, err := store.CountRewardAccountOutputsByCredential(0, stakingKey, nil)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	rows, err := store.GetRewardAccountOutputsByCredential(
		0, stakingKey, 100, 0, "asc", nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, uint64(30), rows[0].Epoch)
	require.Equal(t, uint64(10), uint64(rows[0].Amount))
}

// explainRewardAccountOutputsByCredentialGuardedPlan runs EXPLAIN QUERY PLAN
// for the actual guarded-aware query GetAccountOutputsByCredential issues
// (credential_tag/staking_key/spendable/guarded), mirroring
// explainRewardAccountOutputsByCredentialPlan in
// reward_state_credential_test.go for the pre-existing spendable-only query.
func explainRewardAccountOutputsByCredentialGuardedPlan(
	t *testing.T,
	db *gorm.DB,
) []string {
	t.Helper()
	var plan []struct {
		Detail string `gorm:"column:detail"`
	}
	require.NoError(t, db.Raw(
		`EXPLAIN QUERY PLAN SELECT * FROM reward_account_output
		 WHERE credential_tag = ? AND staking_key = ? AND spendable = ? AND guarded = ?
		 ORDER BY epoch ASC, pool_key_hash ASC, reward_type ASC
		 LIMIT ? OFFSET ?`,
		0, rewardStateTestHash(0x0a), true, false, 100, 0,
	).Scan(&plan).Error)
	details := make([]string, 0, len(plan))
	for _, row := range plan {
		details = append(details, row.Detail)
	}
	return details
}

// TestGetRewardAccountOutputsByCredentialGuardedUsesIndex pins that
// idx_reward_account_output_credential_spendable_guarded (credential_tag,
// staking_key, spendable, guarded, epoch, pool_key_hash, reward_type) is what
// makes the guarded-aware query an index seek rather than an index seek plus
// a per-row guarded check, the same reason
// TestGetRewardAccountOutputsByCredentialUsesIndex pins the equivalent for
// spendable. Both indexes coexist on RewardAccountOutput (dingo #3021 adds
// this one without dropping or renaming the pre-existing
// idx_reward_account_output_credential_spendable, since that index's
// continued existence after a plain AutoMigrate is itself pinned by
// TestMigrateRewardAccountOutputCredentialIndex and
// TestMigrateRewardAccountOutputCredentialSpendableIndex in
// database/models); this asserts the query planner picks the new,
// guarded-aware one for the actual query so a future index rename, drop, or
// narrowing fails loudly instead of silently degrading to a seek-plus-filter.
func TestGetRewardAccountOutputsByCredentialGuardedUsesIndex(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	require.NoError(t, store.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{
			Epoch: 1, CredentialTag: 0, StakingKey: rewardStateTestHash(0x0a),
			PoolKeyHash: rewardStateTestHash(0xaa), RewardType: "member",
			Amount: 1, Spendable: true, Guarded: false,
		},
	}, nil))

	plan := explainRewardAccountOutputsByCredentialGuardedPlan(t, store.DB())
	require.NotEmpty(t, plan)

	sawGuardedIndex := false
	for _, detail := range plan {
		upper := strings.ToUpper(detail)
		require.NotContains(
			t, upper, "SCAN REWARD_ACCOUNT_OUTPUT",
			"query must not fall back to a full scan: %v", plan,
		)
		if strings.Contains(
			detail, "idx_reward_account_output_credential_spendable_guarded",
		) {
			sawGuardedIndex = true
			require.Contains(
				t, detail, "guarded=?",
				"expected guarded in the index search key, got: %v", detail,
			)
			require.Contains(
				t, detail, "spendable=?",
				"expected spendable in the index search key, got: %v", detail,
			)
		}
	}
	require.True(
		t, sawGuardedIndex,
		"expected the query plan to use idx_reward_account_output_credential_spendable_guarded, got: %v",
		plan,
	)
	require.NotContains(
		t, strings.Join(plan, " | "), "USE TEMP B-TREE",
		"the ascending case must be served entirely from the index: %v", plan,
	)
}
