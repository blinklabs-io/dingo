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

package ledger

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

func TestUtxosByAddressCompleteEnforcesMaxResults(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0x5a}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	encoded, err := cbor.Encode(&shelley.ShelleyTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  1_000_000,
	})
	require.NoError(t, err)
	const total = 3
	for i := range total {
		txHash := bytes.Repeat([]byte{byte(i + 1)}, 32)
		require.NoError(t, db.CreateUtxo(t.Context(), nil, &models.Utxo{
			TxId:       txHash,
			OutputIdx:  0,
			PaymentKey: addr.PaymentKeyHash().Bytes(),
			Address:    []byte(addr.String()),
			AddedSlot:  uint64(i + 1),
			Amount:     types.Uint64(1_000_000),
			Cbor:       encoded,
		}))
		require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error {
			return db.Blob().SetUtxo(txn.Blob(), txHash, 0, encoded)
		}))
	}
	ls := &LedgerState{db: db}
	addrs := []lcommon.Address{addr}

	got, err := ls.UtxosByAddressComplete(t.Context(), addrs, total)
	require.NoError(t, err)
	require.Len(t, got, total)

	_, err = ls.UtxosByAddressComplete(t.Context(), addrs, total-1)
	require.ErrorIs(t, err, models.ErrTooManyUtxoResults)

	_, err = ls.UtxosByAddressComplete(t.Context(), addrs, 0)
	require.Error(t, err)
}
