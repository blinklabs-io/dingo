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
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestScanUtxosByAddressVisitsEveryOutput(t *testing.T) {
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
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(30, bytes.Repeat([]byte{0x03}, 32)),
		BlockNumber: 3,
	}
	require.NoError(t, db.SetTip(tip, nil))
	pattern, err := models.ExactUtxoAddressPattern(addr)
	require.NoError(t, err)
	ls := &LedgerState{db: db}

	seen := 0
	gotTip, err := ls.ScanUtxosByAddress(
		t.Context(),
		&models.UtxoWithOrderingQuery{
			AddressPatterns: []models.UtxoAddressPattern{pattern},
			Limit:           2,
		},
		10,
		1<<20,
		func(page []models.UtxoWithOrdering) error {
			require.LessOrEqual(t, len(page), 2)
			seen += len(page)
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, total, seen)
	require.Equal(t, tip, gotTip)
}
