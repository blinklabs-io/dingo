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

	assert.Equal(t, AccountRewardHistoryInfo{
		Epoch: 1, Amount: "100", PoolID: poolAID, Type: "member",
	}, rows[0])
	assert.Equal(t, AccountRewardHistoryInfo{
		Epoch: 2, Amount: "200", PoolID: poolBID, Type: "member",
	}, rows[1])
	// Epoch 3 has two rows; pool_key_hash breaks the tie (poolA < poolB).
	assert.Equal(t, AccountRewardHistoryInfo{
		Epoch: 3, Amount: "300", PoolID: poolAID, Type: "leader",
	}, rows[2])
	assert.Equal(t, AccountRewardHistoryInfo{
		Epoch: 3, Amount: "50", PoolID: poolBID, Type: "member",
	}, rows[3])
}

// TestAccountRewardHistoryPaginationAndOrder verifies that Count/Page/Order
// are applied against the full reward history rather than just returning
// everything, and that "desc" reverses the epoch ordering.
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

	// Page 2 of 2-per-page, ascending: epochs 3 and 4.
	rows, total, err := adapter.AccountRewardHistory(
		stakeAddress,
		PaginationParams{Count: 2, Page: 2, Order: "asc"},
	)
	require.NoError(t, err)
	assert.Equal(t, 5, total)
	require.Len(t, rows, 2)
	assert.Equal(t, int32(3), rows[0].Epoch)
	assert.Equal(t, int32(4), rows[1].Epoch)

	// Descending order, first page: epochs 5 and 4.
	rows, total, err = adapter.AccountRewardHistory(
		stakeAddress,
		PaginationParams{Count: 2, Page: 1, Order: "desc"},
	)
	require.NoError(t, err)
	assert.Equal(t, 5, total)
	require.Len(t, rows, 2)
	assert.Equal(t, int32(5), rows[0].Epoch)
	assert.Equal(t, int32(4), rows[1].Epoch)

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
// "type" enum mapping, including case-insensitivity and pass-through of a
// reward type dingo does not yet model.
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
	assert.Equal(t, "some_future_type", rows[3].Type)
}
