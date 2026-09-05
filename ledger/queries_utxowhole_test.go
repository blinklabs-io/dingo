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

package ledger

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// utxoWholeQuery wraps the leaf query the way the wire delivers it.
func utxoWholeQuery() *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyUtxoWholeQuery{},
		},
	}
}

// seedBabbageUtxo inserts a Utxo metadata row plus a raw-CBOR blob entry for
// a Babbage-format output at addr, so the iterator's loadCbor path resolves
// a real decoded output on each row.
func seedBabbageUtxo(
	t *testing.T,
	db *database.Database,
	txIdSeed byte,
	outputIdx uint32,
	addr lcommon.Address,
	amount uint64,
) []byte {
	t.Helper()
	out := babbage.BabbageTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  mary.MaryTransactionOutputValue{Amount: amount},
	}
	cborBytes, err := cbor.Encode(&out)
	require.NoError(t, err)

	txId := bytes.Repeat([]byte{txIdSeed}, 32)
	txn := db.Transaction(true)
	defer txn.Release()
	require.NoError(t, db.CreateUtxo(txn, &models.Utxo{
		TxId:      txId,
		OutputIdx: outputIdx,
		AddedSlot: 100,
	}))
	blob := db.Blob()
	require.NotNil(t, blob)
	require.NoError(t, blob.SetUtxo(txn.Blob(), txId, outputIdx, cborBytes))
	require.NoError(t, txn.Commit())
	return txId
}

// TestQueryShelleyUtxoWhole_ReturnsLiveUtxos covers GetUTxOWhole against a
// small set of live UTxOs, proving the query decodes every row's address
// and amount correctly and keys the result by (tx hash, output index).
func TestQueryShelleyUtxoWhole_ReturnsLiveUtxos(t *testing.T) {
	db := newTestDB(t)

	addrA, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	addrB, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xBB}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)

	txIdA := seedBabbageUtxo(t, db, 0xA1, 0, addrA, 1_000_000)
	txIdB := seedBabbageUtxo(t, db, 0xB2, 1, addrB, 2_000_000)

	ls := newPoolDistr2Ledger(t, db)

	result, err := ls.Query(utxoWholeQuery())
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok, "expected the []any result wrapper")
	require.Len(t, arr, 1)

	utxos, ok := arr[0].(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	require.True(t, ok, "expected a UtxoId map, got %T", arr[0])
	require.Len(t, utxos, 2)

	outA, ok := utxos[olocalstatequery.UtxoId{
		Hash: ledger.NewBlake2b256(txIdA),
		Idx:  0,
	}]
	require.True(t, ok, "utxo A missing from the whole UTxO set")
	require.Equal(t, addrA.String(), outA.Address().String())
	require.Equal(t, uint64(1_000_000), outA.Amount().Uint64())

	outB, ok := utxos[olocalstatequery.UtxoId{
		Hash: ledger.NewBlake2b256(txIdB),
		Idx:  1,
	}]
	require.True(t, ok, "utxo B missing from the whole UTxO set")
	require.Equal(t, addrB.String(), outB.Address().String())
	require.Equal(t, uint64(2_000_000), outB.Amount().Uint64())
}

// TestQueryShelleyUtxoWhole_EmptyLedger covers a chain with no UTxOs at
// all: the query must return an empty, non-nil map rather than failing.
func TestQueryShelleyUtxoWhole_EmptyLedger(t *testing.T) {
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	result, err := ls.queryShelleyUtxoWhole()
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)

	utxos, ok := arr[0].(map[olocalstatequery.UtxoId]ledger.TransactionOutput)
	require.True(t, ok)
	require.Empty(t, utxos)
}

// TestDecodeUtxoWholeRowMalformedTxIdDoesNotPanic covers a defensive fix
// over the original implementation of this query: a row with a TxId
// shorter than 8 bytes (corrupt data, in practice unreachable through
// normal writes) whose stored CBOR fails to decode as a transaction output
// must surface that decode error rather than panicking on an out-of-range
// slice while formatting the error message.
//
// This calls decodeUtxoWholeRow directly with a hand-built row rather than
// going through a real database round trip: a driver's BLOB scan can hand
// back a slice with slack capacity beyond its logical length (observed
// with SQLite here), which happens not to panic on a bounded slice
// expression regardless of this fix -- exercising the fix for real
// requires a TxId slice whose capacity is provably exactly its length,
// which only a directly constructed value guarantees.
func TestDecodeUtxoWholeRowMalformedTxIdDoesNotPanic(t *testing.T) {
	shortTxId := make([]byte, 2, 2)
	shortTxId[0], shortTxId[1] = 0x01, 0x02
	u := &models.Utxo{
		TxId:      shortTxId,
		OutputIdx: 0,
		Cbor:      []byte{0xff},
	}

	require.NotPanics(t, func() {
		_, _, _ = decodeUtxoWholeRow(u)
	})
	_, _, err := decodeUtxoWholeRow(u)
	require.Error(t, err)
	require.Contains(t, err.Error(), "0102")
}
