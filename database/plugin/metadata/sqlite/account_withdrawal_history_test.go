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
	"bytes"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestAccountWithdrawalHistoryOrdersByTransactionSlot(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	key := bytes.Repeat([]byte{0x31}, 28)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: key, Active: true, Reward: 100, CreatedSlot: 1, AddedSlot: 1,
	}))
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey, lcommon.AddressNetworkTestnet, nil, key,
	)
	require.NoError(t, err)
	for i, slot := range []uint64{20, 10} {
		tx := &mockTransaction{
			hash:    lcommon.Blake2b256{byte(i + 1)},
			isValid: true,
			withdrawals: map[*lcommon.Address]*big.Int{
				&address: big.NewInt(int64(i + 1)),
			},
		}
		require.NoError(t, store.SetTransaction(
			tx,
			ocommon.Point{
				Slot: slot,
				Hash: bytes.Repeat([]byte{byte(slot)}, 32),
			},
			0,
			nil,
			true,
			nil,
			0,
		))
	}
	asc, err := store.GetAccountWithdrawalHistoryByCredential(
		0,
		key,
		10,
		0,
		"asc",
		nil,
	)
	require.NoError(t, err)
	require.Len(t, asc, 2)
	require.Equal(t, []uint64{10, 20}, []uint64{asc[0].TxSlot, asc[1].TxSlot})
	desc, err := store.GetAccountWithdrawalHistoryByCredential(
		0,
		key,
		10,
		0,
		"desc",
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, []uint64{20, 10}, []uint64{desc[0].TxSlot, desc[1].TxSlot})
}
