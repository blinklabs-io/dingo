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
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodeAdapterAddressUTXOsLargeResultSetPagination seeds one address with
// a large number of live UTxOs and proves ascending and descending
// pagination both return the correct window. It exercises the windowed
// reverse in NodeAdapter.AddressUTXOs (adapter.go): descending pagination
// used to swap-reverse the address's entire UTxO history before slicing out
// a page; reversing only the requested window must produce the identical
// ordering for a large result set (see dingo/3520).
func TestNodeAdapterAddressUTXOsLargeResultSetPagination(t *testing.T) {
	adapter, raw, db := newDBBackedAdapter(t)

	payment := bytes.Repeat([]byte{0x99}, lcommon.AddressHashSize)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)

	const total = 250
	for i := range total {
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
				AddedSlot:  slot,
				Amount:     types.Uint64(amount),
			}},
		})
		storePointerOutputCbor(t, db, txHash, 0, addr, amount)
	}

	t.Run("ascending page stops at the requested window", func(t *testing.T) {
		items, total, err := adapter.AddressUTXOs(
			addr.String(),
			PaginationParams{Count: 10, Page: 3, Order: PaginationOrderAsc},
		)
		require.NoError(t, err)
		assert.Equal(t, 250, total)
		require.Len(t, items, 10)
		assert.Equal(t, "21000000", items[0].Amount[0].Quantity)
		assert.Equal(t, "30000000", items[len(items)-1].Amount[0].Quantity)
	})

	t.Run("descending page matches a full-history reverse", func(t *testing.T) {
		items, total, err := adapter.AddressUTXOs(
			addr.String(),
			PaginationParams{Count: 10, Page: 5, Order: PaginationOrderDesc},
		)
		require.NoError(t, err)
		assert.Equal(t, 250, total)
		require.Len(t, items, 10)
		// Descending page 5 (count 10) is the 41st-newest through the
		// 50th-newest UTxO: amounts 210000000 down to 201000000.
		assert.Equal(t, "210000000", items[0].Amount[0].Quantity)
		assert.Equal(t, "201000000", items[len(items)-1].Amount[0].Quantity)
	})

	t.Run("descending page past the end is empty", func(t *testing.T) {
		items, total, err := adapter.AddressUTXOs(
			addr.String(),
			PaginationParams{Count: 10, Page: 26, Order: PaginationOrderDesc},
		)
		require.NoError(t, err)
		assert.Equal(t, 250, total)
		assert.Empty(t, items)
	})
}

// TestNodeAdapterAddressUTXOsAssetsSurviveRefFetch proves native assets
// still attach to the returned page: AddressUTXOs now resolves its total
// via a reference-only scan (no assets loaded) and fetches full rows for
// just the requested page via UtxosByRefs, a different path than before
// (see dingo/3520) that must not drop asset data along the way.
func TestNodeAdapterAddressUTXOsAssetsSurviveRefFetch(t *testing.T) {
	adapter, store, db := newDBBackedAdapter(t)

	payment := bytes.Repeat([]byte{0x33}, lcommon.AddressHashSize)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		payment,
		nil,
	)
	require.NoError(t, err)
	policyID := bytes.Repeat([]byte{0x44}, lcommon.AddressHashSize)
	assetName := []byte("TOKEN")

	txHash := fill32(0x50)
	insertAdapterTransaction(t, store, &models.Transaction{
		Hash: txHash,
		Slot: 1,
		Outputs: []models.Utxo{{
			TxId:       txHash,
			OutputIdx:  0,
			PaymentKey: payment,
			AddedSlot:  1,
			Amount:     types.Uint64(1_000_000),
			Assets: []models.Asset{{
				PolicyId: policyID,
				Name:     assetName,
				Amount:   types.Uint64(9),
			}},
		}},
	})
	storePointerOutputCbor(t, db, txHash, 0, addr, 1_000_000)

	items, total, err := adapter.AddressUTXOs(
		addr.String(),
		PaginationParams{Count: 10, Page: 1, Order: PaginationOrderAsc},
	)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	require.Len(t, items, 1)
	require.Len(t, items[0].Amount, 2)
	assert.Equal(t, "lovelace", items[0].Amount[0].Unit)
	assert.Equal(
		t,
		hex.EncodeToString(policyID)+hex.EncodeToString(assetName),
		items[0].Amount[1].Unit,
	)
	assert.Equal(t, "9", items[0].Amount[1].Quantity)
}
