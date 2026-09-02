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
	"database/sql"
	"encoding/binary"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedStakeCredentialUtxos registers a stake credential's account row, then
// seeds numUtxos live UTxOs against it (each at a distinct payment address
// and slot, so ascending/descending order is unambiguous), returning the
// bech32 stake address and the underlying staking key bytes.
func seedStakeCredentialUtxos(
	t *testing.T,
	adapter *NodeAdapter,
	raw *sql.DB,
	db *database.Database,
	numUtxos int,
) (string, []byte) {
	t.Helper()
	stakeKey := bytes.Repeat([]byte{0x77}, lcommon.AddressHashSize)
	stakeAddr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeKey,
	)
	require.NoError(t, err)
	require.NoError(t, adapter.ledgerState.Database().CreateAccount(
		nil,
		&models.Account{StakingKey: stakeKey, Active: true},
	))

	for i := range numUtxos {
		payment := make([]byte, lcommon.AddressHashSize)
		binary.BigEndian.PutUint32(payment, uint32(i)+1)
		txHash := make([]byte, 32)
		binary.BigEndian.PutUint32(txHash[28:], uint32(i)+1)
		slot := uint64(i) + 1
		amount := (uint64(i) + 1) * 1_000_000
		insertAdapterTransaction(t, raw, &models.Transaction{
			Hash:       txHash,
			Slot:       slot,
			BlockIndex: 0,
			Outputs: []models.Utxo{{
				TxId:       txHash,
				OutputIdx:  0,
				PaymentKey: payment,
				StakingKey: stakeKey,
				AddedSlot:  slot,
				Amount:     types.Uint64(amount),
			}},
		})
		addr, err := lcommon.NewAddressFromParts(
			lcommon.AddressTypeKeyKey,
			lcommon.AddressNetworkTestnet,
			payment,
			stakeKey,
		)
		require.NoError(t, err)
		storePointerOutputCbor(t, db, txHash, 0, addr, amount)
	}
	return stakeAddr.String(), stakeKey
}

// TestNodeAdapterAccountUTXOsLargeResultSetPagination proves AccountUTXOs
// pages a large stake account's UTxOs without materializing more than the
// requested window (see dingo/3520): it exercises a large result set, an
// ascending and a descending page, and an out-of-range page landing past
// the end.
func TestNodeAdapterAccountUTXOsLargeResultSetPagination(t *testing.T) {
	adapter, raw, db := newDBBackedAdapter(t)
	const total = 250
	stakeAddr, _ := seedStakeCredentialUtxos(t, adapter, raw, db, total)

	t.Run("ascending page stops at the requested window", func(t *testing.T) {
		items, gotTotal, err := adapter.AccountUTXOs(
			stakeAddr,
			PaginationParams{Count: 10, Page: 3, Order: PaginationOrderAsc},
		)
		require.NoError(t, err)
		assert.Equal(t, total, gotTotal)
		require.Len(t, items, 10)
		// Page 3 with count 10 is items 21..30 (slot order == insertion
		// order); item 21's amount is (21 * 1_000_000).
		assert.Equal(t, "21000000", items[0].Amount[0].Quantity)
		assert.Equal(t, "30000000", items[len(items)-1].Amount[0].Quantity)
	})

	t.Run("descending page returns newest first", func(t *testing.T) {
		items, gotTotal, err := adapter.AccountUTXOs(
			stakeAddr,
			PaginationParams{Count: 10, Page: 1, Order: PaginationOrderDesc},
		)
		require.NoError(t, err)
		assert.Equal(t, total, gotTotal)
		require.Len(t, items, 10)
		assert.Equal(t, "250000000", items[0].Amount[0].Quantity)
		assert.Equal(t, "241000000", items[len(items)-1].Amount[0].Quantity)
	})

	t.Run(
		"a page past the end is empty but reports the real total",
		func(t *testing.T) {
			items, gotTotal, err := adapter.AccountUTXOs(
				stakeAddr,
				PaginationParams{
					Count: 100,
					Page:  4,
					Order: PaginationOrderAsc,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, total, gotTotal)
			assert.Empty(t, items)
		},
	)

	t.Run(
		"a page far beyond the address history is empty, not an error",
		func(t *testing.T) {
			items, gotTotal, err := adapter.AccountUTXOs(
				stakeAddr,
				PaginationParams{
					Count: MaxPaginationCount,
					Page:  MaxPaginationPage,
					Order: PaginationOrderAsc,
				},
			)
			require.NoError(t, err)
			assert.Equal(t, total, gotTotal)
			assert.Empty(t, items)
		},
	)
}

// TestNodeAdapterAccountUTXOsEmpty proves a registered stake credential with
// no live UTxOs returns an empty page and a zero total rather than an error.
func TestNodeAdapterAccountUTXOsEmpty(t *testing.T) {
	adapter, raw, db := newDBBackedAdapter(t)
	stakeAddr, _ := seedStakeCredentialUtxos(t, adapter, raw, db, 0)

	items, total, err := adapter.AccountUTXOs(
		stakeAddr,
		PaginationParams{Count: 100, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	assert.Zero(t, total)
	assert.Empty(t, items)
}
