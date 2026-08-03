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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blockfrost

import (
	"bytes"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRewardHistoryStakeAddress builds a valid bech32 stake address for a
// key-hash staking credential, matching the shape parseStakeAddress expects
// (empty payment part, non-zero staking key hash).
func newRewardHistoryStakeAddress(
	t *testing.T,
	stakingKeyHash []byte,
) string {
	t.Helper()
	addr, err := stakeAddressFromCredential(
		lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.CredentialHash(stakingKeyHash),
		},
		lcommon.AddressNetworkTestnet,
	)
	require.NoError(t, err)
	return addr
}

// TestAccountRewardHistoryMultiEpochMultiPool covers an account with reward
// rows spanning more than one epoch and more than one pool (including a pool
// owner's leader reward alongside a member reward in the same epoch), and
// verifies the default (ascending) ordering plus the Blockfrost field
// mapping, including the "type" enum value.
//
// Stored Epoch is the reward-calculation snapshot epoch, not the earned
// epoch Blockfrost reports (dingo #1875 review finding 2): the reward
// computed from snapshot epoch S is credited at the boundary into S+3, which
// is when it becomes spendable, and cardano-db-sync (what Blockfrost serves
// this endpoint from) models spendable_epoch = earned_epoch + 2, so
// earned_epoch = S+1. Row ordering is still driven by the stored (snapshot)
// epoch; only the reported value is shifted.
func TestAccountRewardHistoryMultiEpochMultiPool(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)

	stakingKey := bytes.Repeat([]byte{0x01}, 28)
	poolA := bytes.Repeat([]byte{0xaa}, 28)
	poolB := bytes.Repeat([]byte{0xbb}, 28)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		CredentialTag: 0,
		StakingKey:    stakingKey,
		Active:        true,
	}))

	require.NoError(t, store.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{
			Epoch:         1,
			CredentialTag: 0,
			StakingKey:    stakingKey,
			PoolKeyHash:   poolA,
			RewardType:    "member",
			Amount:        100,
			Spendable:     true,
		},
		{
			Epoch:         2,
			CredentialTag: 0,
			StakingKey:    stakingKey,
			PoolKeyHash:   poolB,
			RewardType:    "member",
			Amount:        200,
			Spendable:     true,
		},
		{
			Epoch:         3,
			CredentialTag: 0,
			StakingKey:    stakingKey,
			PoolKeyHash:   poolA,
			RewardType:    "leader",
			Amount:        300,
			Spendable:     true,
		},
		{
			Epoch:         3,
			CredentialTag: 0,
			StakingKey:    stakingKey,
			PoolKeyHash:   poolB,
			RewardType:    "member",
			Amount:        50,
			Spendable:     true,
		},
	}, nil))

	stakeAddress := newRewardHistoryStakeAddress(t, stakingKey)
	rows, total, err := adapter.AccountRewardHistory(
		stakeAddress,
		PaginationParams{Count: 100, Page: 1, Order: "asc"},
	)
	require.NoError(t, err)
	require.Equal(t, 4, total)
	require.Len(t, rows, 4)

	poolAID := lcommon.PoolId(lcommon.NewBlake2b224(poolA)).String()
	poolBID := lcommon.PoolId(lcommon.NewBlake2b224(poolB)).String()

	// Reported epochs are stored+1 (snapshot epoch 1, 2, 3 -> earned epoch
	// 2, 3, 4).
	assert.Equal(t, AccountRewardHistoryInfo{
		Epoch: 2, Amount: "100", PoolID: poolAID, Type: "member",
	}, rows[0])
	assert.Equal(t, AccountRewardHistoryInfo{
		Epoch: 3, Amount: "200", PoolID: poolBID, Type: "member",
	}, rows[1])
	// Stored epoch 3 has two rows; pool_key_hash breaks the tie (poolA <
	// poolB). Both report earned epoch 4.
	assert.Equal(t, AccountRewardHistoryInfo{
		Epoch: 4, Amount: "300", PoolID: poolAID, Type: "leader",
	}, rows[2])
	assert.Equal(t, AccountRewardHistoryInfo{
		Epoch: 4, Amount: "50", PoolID: poolBID, Type: "member",
	}, rows[3])
}

// TestAccountRewardHistoryPaginationAndOrder verifies that Count/Page/Order
// are applied against the full reward history rather than just returning
// everything, and that "desc" reverses the epoch ordering. Stored epochs are
// 1-5; reported epochs are stored+1 (2-6, see the earned-epoch note on
// TestAccountRewardHistoryMultiEpochMultiPool), but pagination and ordering
// are still driven by the stored (snapshot) epoch.
func TestAccountRewardHistoryPaginationAndOrder(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)

	stakingKey := bytes.Repeat([]byte{0x02}, 28)
	pool := bytes.Repeat([]byte{0xcc}, 28)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		CredentialTag: 0,
		StakingKey:    stakingKey,
		Active:        true,
	}))

	outputs := make([]*models.RewardAccountOutput, 0, 5)
	for epoch := uint64(1); epoch <= 5; epoch++ {
		outputs = append(outputs, &models.RewardAccountOutput{
			Epoch:         epoch,
			CredentialTag: 0,
			StakingKey:    stakingKey,
			PoolKeyHash:   pool,
			RewardType:    "member",
			Amount:        types.Uint64(epoch * 10),
			Spendable:     true,
		})
	}
	require.NoError(t, store.SaveRewardAccountOutputs(outputs, nil))

	stakeAddress := newRewardHistoryStakeAddress(t, stakingKey)

	// Page 2 of 2-per-page, ascending: stored epochs 3 and 4 -> reported 4
	// and 5.
	rows, total, err := adapter.AccountRewardHistory(
		stakeAddress,
		PaginationParams{Count: 2, Page: 2, Order: "asc"},
	)
	require.NoError(t, err)
	assert.Equal(t, 5, total)
	require.Len(t, rows, 2)
	assert.Equal(t, int32(4), rows[0].Epoch)
	assert.Equal(t, int32(5), rows[1].Epoch)

	// Descending order, first page: stored epochs 5 and 4 -> reported 6 and
	// 5.
	rows, total, err = adapter.AccountRewardHistory(
		stakeAddress,
		PaginationParams{Count: 2, Page: 1, Order: "desc"},
	)
	require.NoError(t, err)
	assert.Equal(t, 5, total)
	require.Len(t, rows, 2)
	assert.Equal(t, int32(6), rows[0].Epoch)
	assert.Equal(t, int32(5), rows[1].Epoch)

	// A page beyond the available rows returns an empty slice but still
	// reports the true total.
	rows, total, err = adapter.AccountRewardHistory(
		stakeAddress,
		PaginationParams{Count: 2, Page: 10, Order: "asc"},
	)
	require.NoError(t, err)
	assert.Equal(t, 5, total)
	assert.Empty(t, rows)
}

// TestAccountRewardHistoryExcludesNonSpendableReward pins review finding 1:
// a reward_account_output row with Spendable = false was never actually
// credited to the account (applyStakeRewardApplication in
// ledger/reward_calculation.go skips crediting it and instead accumulates
// the amount into the epoch's unspendable total), typically because the
// credential deregistered before the reward's payout boundary
// (finalizePrecomputedRewardOutputs persists that as a permanent
// spendable=false row). Reporting it here would overstate both the reward
// list and the total. This mirrors the maintainer's reproduction: one
// spendable row (amount 1000000) and one non-spendable row (amount
// 9999999) for the same credential; only the spendable row and its amount
// may appear.
func TestAccountRewardHistoryExcludesNonSpendableReward(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)

	stakingKey := bytes.Repeat([]byte{0x07}, 28)
	pool := bytes.Repeat([]byte{0xff}, 28)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		CredentialTag: 0,
		StakingKey:    stakingKey,
		Active:        true,
	}))

	require.NoError(t, store.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{
			Epoch: 10, CredentialTag: 0, StakingKey: stakingKey,
			PoolKeyHash: pool, RewardType: "member",
			Amount: 1_000_000, Spendable: true,
		},
		{
			Epoch: 11, CredentialTag: 0, StakingKey: stakingKey,
			PoolKeyHash: pool, RewardType: "member",
			Amount: 9_999_999, Spendable: false,
		},
	}, nil))

	stakeAddress := newRewardHistoryStakeAddress(t, stakingKey)
	rows, total, err := adapter.AccountRewardHistory(
		stakeAddress,
		PaginationParams{Count: 100, Page: 1, Order: "asc"},
	)
	require.NoError(t, err)
	require.Equal(
		t, 1, total,
		"the non-spendable row must not be counted in the total",
	)
	require.Len(
		t, rows, 1,
		"the non-spendable row must be absent from the response",
	)
	assert.Equal(t, "1000000", rows[0].Amount)

	var summed uint64
	for _, row := range rows {
		amount, err := strconv.ParseUint(row.Amount, 10, 64)
		require.NoError(t, err)
		summed += amount
	}
	assert.Equal(
		t, uint64(1_000_000), summed,
		"summed reward history must equal only what was actually credited",
	)
}

// TestAccountRewardHistoryExcludesGuardedReward is the dingo #3021 companion
// to TestAccountRewardHistoryExcludesNonSpendableReward: a
// reward_account_output row withheld by the CIP-0163 reward-crediting guard
// (rewardOutputGuarded / applyGuardedFlagToAccountOutputs in
// ledger/reward_calculation.go) is persisted with Guarded = true but keeps
// Spendable = true -- it was never deregistered, just CIP-0163-expired as of
// the reward's snapshot epoch -- so the Spendable-only filter #3015 added
// does not catch it on its own. This fixture deliberately keeps every row's
// Spendable = true (unlike the non-spendable case above) so the guarded
// column is what is actually under test.
func TestAccountRewardHistoryExcludesGuardedReward(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)

	stakingKey := bytes.Repeat([]byte{0x08}, 28)
	pool := bytes.Repeat([]byte{0xfe}, 28)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		CredentialTag: 0,
		StakingKey:    stakingKey,
		Active:        true,
	}))

	require.NoError(t, store.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{
			Epoch: 10, CredentialTag: 0, StakingKey: stakingKey,
			PoolKeyHash: pool, RewardType: "member",
			Amount: 1_000_000, Spendable: true, Guarded: false,
		},
		// CIP-0163-guarded: the reward account was expired as of the reward's
		// snapshot epoch, so the guard skipped crediting it, but it was never
		// deregistered, so Spendable stays true.
		{
			Epoch: 11, CredentialTag: 0, StakingKey: stakingKey,
			PoolKeyHash: pool, RewardType: "leader",
			Amount: 9_999_999, Spendable: true, Guarded: true,
		},
	}, nil))

	stakeAddress := newRewardHistoryStakeAddress(t, stakingKey)
	rows, total, err := adapter.AccountRewardHistory(
		stakeAddress,
		PaginationParams{Count: 100, Page: 1, Order: "asc"},
	)
	require.NoError(t, err)
	require.Equal(
		t, 1, total,
		"the guarded row must not be counted in the total",
	)
	require.Len(
		t, rows, 1,
		"the guarded row must be absent from the response",
	)
	assert.Equal(t, "1000000", rows[0].Amount)

	var summed uint64
	for _, row := range rows {
		amount, err := strconv.ParseUint(row.Amount, 10, 64)
		require.NoError(t, err)
		summed += amount
	}
	assert.Equal(
		t, uint64(1_000_000), summed,
		"summed reward history must equal only what was actually credited",
	)
}

// TestAccountRewardHistorySumMatchesAccountRewardsSum pins the user-visible
// contract both TestAccountRewardHistoryExcludesNonSpendableReward (dingo
// #1875 review finding 1) and TestAccountRewardHistoryExcludesGuardedReward
// (dingo #3021) motivate: for an account with a withheld reward -- for
// either reason -- the sum of amounts /accounts/{stake_address}/rewards
// reports must equal rewards_sum on /accounts/{stake_address}, because both
// numbers are supposed to describe the same thing (what the account actually
// received). Account.Reward (rewards_sum's source, via db.Account) is set
// only by AddAccountRewardByCredential, the same crediting primitive
// applyStakeRewardApplication calls, so setting it directly here to the sum
// of only the credited rows mirrors what a real reward application would
// have produced without needing to run the full ledger pipeline.
func TestAccountRewardHistorySumMatchesAccountRewardsSum(t *testing.T) {
	adapter, store, db := newDBBackedAdapter(t)

	stakingKey := bytes.Repeat([]byte{0x09}, 28)
	pool := bytes.Repeat([]byte{0xfd}, 28)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		CredentialTag: 0,
		StakingKey:    stakingKey,
		Active:        true,
	}))

	require.NoError(t, store.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{
			Epoch: 10, CredentialTag: 0, StakingKey: stakingKey,
			PoolKeyHash: pool, RewardType: "member",
			Amount: 1_000_000, Spendable: true, Guarded: false,
		},
		{
			Epoch: 11, CredentialTag: 0, StakingKey: stakingKey,
			PoolKeyHash: pool, RewardType: "member",
			Amount: 500_000, Spendable: false, Guarded: false,
		},
		{
			Epoch: 12, CredentialTag: 0, StakingKey: stakingKey,
			PoolKeyHash: pool, RewardType: "leader",
			Amount: 250_000, Spendable: true, Guarded: true,
		},
	}, nil))

	// Only the first row was actually credited; the other two were withheld
	// (for two different reasons) exactly as applyStakeRewardApplication
	// would have left them. AddAccountRewardByCredential requires the
	// account to be active to find it.
	require.NoError(t, db.AddAccountRewardByCredential(
		0, stakingKey, 1_000_000, 100, nil, nil,
	))
	// Deactivate afterward: newDBBackedAdapter supplies no CardanoNodeConfig
	// (see its doc comment), so Account's active-epoch lookup (SlotToEpoch)
	// has no epoch cache to resolve against and only runs when Active is
	// true. Activation status is orthogonal to the sum invariant under test.
	require.NoError(t, store.DB().
		Model(&models.Account{}).
		Where("credential_tag = ? AND staking_key = ?", 0, stakingKey).
		Update("active", false).Error)

	stakeAddress := newRewardHistoryStakeAddress(t, stakingKey)
	rows, _, err := adapter.AccountRewardHistory(
		stakeAddress,
		PaginationParams{Count: 100, Page: 1, Order: "asc"},
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	var summed uint64
	for _, row := range rows {
		amount, err := strconv.ParseUint(row.Amount, 10, 64)
		require.NoError(t, err)
		summed += amount
	}

	account, err := adapter.Account(stakeAddress)
	require.NoError(t, err)
	rewardsSum, err := strconv.ParseUint(account.RewardsSum, 10, 64)
	require.NoError(t, err)

	assert.Equal(
		t, rewardsSum, summed,
		"the sum of /rewards must equal rewards_sum on /accounts/{stake_address}",
	)
	assert.Equal(t, uint64(1_000_000), summed)
}

// TestAccountRewardHistoryEmptyForRegisteredAccount covers a registered
// account with no reward history: the endpoint must distinguish this from an
// unknown account by returning an empty slice and zero total, not an error.
func TestAccountRewardHistoryEmptyForRegisteredAccount(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)

	stakingKey := bytes.Repeat([]byte{0x03}, 28)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		CredentialTag: 0,
		StakingKey:    stakingKey,
		Active:        true,
	}))

	stakeAddress := newRewardHistoryStakeAddress(t, stakingKey)
	rows, total, err := adapter.AccountRewardHistory(
		stakeAddress,
		PaginationParams{Count: 100, Page: 1, Order: "asc"},
	)
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, rows)
}

// TestAccountRewardHistoryUnknownAccount covers a well-formed stake address
// with no backing account row: the endpoint must surface an error rather
// than a misleadingly empty history.
func TestAccountRewardHistoryUnknownAccount(t *testing.T) {
	adapter, _, _ := newDBBackedAdapter(t)

	stakingKey := bytes.Repeat([]byte{0x04}, 28)
	stakeAddress := newRewardHistoryStakeAddress(t, stakingKey)

	_, _, err := adapter.AccountRewardHistory(
		stakeAddress,
		PaginationParams{Count: 100, Page: 1, Order: "asc"},
	)
	require.Error(t, err)
}

// TestAccountRewardHistoryDatabaseError verifies that a query failure against
// the reward_account_output backing store surfaces as an error instead of a
// silently empty/successful response.
func TestAccountRewardHistoryDatabaseError(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)

	stakingKey := bytes.Repeat([]byte{0x05}, 28)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		CredentialTag: 0,
		StakingKey:    stakingKey,
		Active:        true,
	}))

	require.NoError(
		t, store.DB().Exec("DROP TABLE reward_account_output").Error,
	)

	stakeAddress := newRewardHistoryStakeAddress(t, stakingKey)
	_, _, err := adapter.AccountRewardHistory(
		stakeAddress,
		PaginationParams{Count: 100, Page: 1, Order: "asc"},
	)
	require.ErrorContains(t, err, "count account reward history")
}

// TestAccountRewardHistoryTypeMapping verifies the reward_type -> Blockfrost
// "type" enum handling: case-insensitivity, pass-through of a reward type
// outside the recognized allow-list (blockfrostRewardTypes), and that
// exactly the unrecognized value triggers the warn-and-pass-through path —
// recognized values must not. A plain strings.ToLower would satisfy the
// Type assertions below on its own, so what actually pins
// blockfrostRewardTypes as a real allow-list (rather than the no-op mapping
// review finding 3 identified) is the slog assertions: recognized values
// produce no warning, and "some_future_type" produces exactly one.
func TestAccountRewardHistoryTypeMapping(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)

	stakingKey := bytes.Repeat([]byte{0x06}, 28)
	pool := bytes.Repeat([]byte{0xee}, 28)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		CredentialTag: 0,
		StakingKey:    stakingKey,
		Active:        true,
	}))

	require.NoError(t, store.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{
			Epoch: 1, CredentialTag: 0, StakingKey: stakingKey,
			PoolKeyHash: pool, RewardType: "LEADER", Amount: 1, Spendable: true,
		},
		{
			Epoch: 2, CredentialTag: 0, StakingKey: stakingKey,
			PoolKeyHash: pool, RewardType: "Member", Amount: 2, Spendable: true,
		},
		{
			Epoch: 3, CredentialTag: 0, StakingKey: stakingKey,
			PoolKeyHash: pool, RewardType: "pool_deposit_refund", Amount: 3,
			Spendable: true,
		},
		{
			Epoch: 4, CredentialTag: 0, StakingKey: stakingKey,
			PoolKeyHash: pool, RewardType: "some_future_type", Amount: 4,
			Spendable: true,
		},
	}, nil))

	// Capture slog output for the duration of the call so the warn path can
	// be asserted on directly, rather than relying on the Type value alone
	// (which a plain strings.ToLower would also produce).
	var logBuf bytes.Buffer
	prevLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, nil)))
	t.Cleanup(func() { slog.SetDefault(prevLogger) })

	stakeAddress := newRewardHistoryStakeAddress(t, stakingKey)
	rows, total, err := adapter.AccountRewardHistory(
		stakeAddress,
		PaginationParams{Count: 100, Page: 1, Order: "asc"},
	)
	require.NoError(t, err)
	require.Equal(t, 4, total)
	require.Len(t, rows, 4)

	assert.Equal(t, "leader", rows[0].Type)
	assert.Equal(t, "member", rows[1].Type)
	assert.Equal(t, "pool_deposit_refund", rows[2].Type)
	// Unrecognized value still passes through lowercased rather than being
	// dropped.
	assert.Equal(t, "some_future_type", rows[3].Type)

	logOutput := logBuf.String()
	assert.Equal(
		t, 1, strings.Count(logOutput, "level=WARN"),
		"expected exactly one warning (for some_future_type), got log: %s",
		logOutput,
	)
	assert.Contains(t, logOutput, "some_future_type")
	assert.NotContains(t, logOutput, "reward_type=leader")
	assert.NotContains(t, logOutput, "reward_type=member")
	assert.NotContains(t, logOutput, "reward_type=pool_deposit_refund")
}
