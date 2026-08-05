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

package blockfrost

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

func newRewardHistoryStakeAddress(
	t *testing.T,
	stakingKeyHash []byte,
) string {
	t.Helper()
	address, err := stakeAddressFromCredential(
		lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.CredentialHash(stakingKeyHash),
		},
		lcommon.AddressNetworkTestnet,
	)
	require.NoError(t, err)
	return address
}

func TestAccountRewardHistoryExcludesNonSpendableReward(t *testing.T) {
	adapter, _, db := newDBBackedAdapter(t)
	stakingKey := bytes.Repeat([]byte{0x07}, 28)
	poolKey := bytes.Repeat([]byte{0xff}, 28)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		CredentialTag: 0,
		StakingKey:    stakingKey,
		Active:        true,
	}))
	require.NoError(t, db.Metadata().SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{Epoch: 10, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: poolKey,
			RewardType: "member", Amount: 1_000_000, Spendable: true},
		{Epoch: 11, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: poolKey,
			RewardType: "member", Amount: 9_999_999, Spendable: false},
	}, nil))

	rows, total, err := adapter.AccountRewardHistory(
		newRewardHistoryStakeAddress(t, stakingKey),
		PaginationParams{Count: 100, Page: 1, Order: "asc"},
	)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, rows, 1)
	require.Equal(t, "1000000", rows[0].Amount)
}

func TestAccountRewardHistoryExcludesGuardedReward(t *testing.T) {
	adapter, _, db := newDBBackedAdapter(t)
	stakingKey := bytes.Repeat([]byte{0x08}, 28)
	poolKey := bytes.Repeat([]byte{0xfe}, 28)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		CredentialTag: 0,
		StakingKey:    stakingKey,
		Active:        true,
	}))
	require.NoError(t, db.Metadata().SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{Epoch: 10, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: poolKey,
			RewardType: "member", Amount: 1_000_000, Spendable: true},
		{Epoch: 11, CredentialTag: 0, StakingKey: stakingKey, PoolKeyHash: poolKey,
			RewardType: "leader", Amount: 9_999_999, Spendable: true, Guarded: true},
	}, nil))

	rows, total, err := adapter.AccountRewardHistory(
		newRewardHistoryStakeAddress(t, stakingKey),
		PaginationParams{Count: 100, Page: 1, Order: "asc"},
	)
	require.NoError(t, err)
	require.Equal(t, 1, total)
	require.Len(t, rows, 1)
	require.Equal(t, "1000000", rows[0].Amount)
}
