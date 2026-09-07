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

// TestDecodeUtxoWholeCborMalformedCborSurfacesError covers a row whose
// resolved CBOR fails to decode as a transaction output: this must surface
// that decode error (with the ref's hex-encoded TxId in the message) rather
// than panicking. Unlike this query's previous row-at-a-time
// implementation, decodeUtxoWholeCbor's ref.TxId is a fixed-size
// database.UtxoRef.TxId ([32]byte, not a variable-length slice), so a
// too-short TxId can no longer reach this code at all -- the earlier
// defensive fix for that case (using hex.EncodeToString instead of a
// %x-formatted slice) is retained here since it's still correct, just no
// longer reachable through a malformed-length input.
func TestDecodeUtxoWholeCborMalformedCborSurfacesError(t *testing.T) {
	ref := database.UtxoRef{OutputIdx: 0}
	ref.TxId[0], ref.TxId[1] = 0x01, 0x02

	require.NotPanics(t, func() {
		_, _ = decodeUtxoWholeCbor(ref, []byte{0xff})
	})
	_, err := decodeUtxoWholeCbor(ref, []byte{0xff})
	require.Error(t, err)
	require.Contains(t, err.Error(), "0102")
}
