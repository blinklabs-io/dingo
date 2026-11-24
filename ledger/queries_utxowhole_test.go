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
	"context"
	"encoding/hex"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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

func utxoWholeCborQuery() *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyCborQuery{
				Query: &olocalstatequery.ShelleyUtxoWholeQuery{},
			},
		},
	}
}

func requireUtxoWholeMap(
	t *testing.T,
	result any,
) map[olocalstatequery.UtxoId]ledger.TransactionOutput {
	t.Helper()
	bounded, ok := result.(*utxoWholeResult)
	require.True(t, ok, "expected a bounded whole UTxO result, got %T", result)
	var wire []map[olocalstatequery.UtxoId]cbor.RawMessage
	_, err := cbor.Decode(bounded.encoded, &wire)
	require.NoError(t, err)
	require.Len(t, wire, 1)
	ret := make(
		map[olocalstatequery.UtxoId]ledger.TransactionOutput,
		len(wire[0]),
	)
	for id, raw := range wire[0] {
		output, err := ledger.NewTransactionOutputFromCbor(raw)
		require.NoError(t, err)
		ret[id] = output
	}
	return ret
}

// seedBabbageUtxo makes a Babbage-format output at addr live in db's
// LedgerDB set, with its body in the blob store, so a read resolves a real
// decoded output.
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
	fxSeedOutput(t, db, txId, outputIdx, 100, cborBytes)
	return txId
}

// seedBabbageUtxoCbor stores the body seedBabbageUtxo's output has, for a
// test that commits the output's lifecycle to the set itself.
func seedBabbageUtxoCbor(
	t *testing.T,
	db *database.Database,
	txIdSeed byte,
	outputIdx uint32,
	addr lcommon.Address,
	amount uint64,
) []byte {
	t.Helper()
	cborBytes, err := cbor.Encode(&babbage.BabbageTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  mary.MaryTransactionOutputValue{Amount: amount},
	})
	require.NoError(t, err)
	txId := bytes.Repeat([]byte{txIdSeed}, 32)
	require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error {
		return db.Blob().SetUtxo(txn.Blob(), txId, outputIdx, cborBytes)
	}))
	return txId
}

func mustAddressBytes(t *testing.T, addr lcommon.Address) []byte {
	t.Helper()
	encoded, err := addr.Bytes()
	require.NoError(t, err)
	return encoded
}

// TestQueryShelleyUtxoWhole_ReturnsLiveUtxos covers GetUTxOWhole against a
// small set of live UTxOs, proving the query decodes every row's address
// and amount correctly and keys the result by (tx hash, output index).
func TestQueryShelleyUtxoWhole_ReturnsLiveUtxos(t *testing.T) {
	t.Parallel()

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

	result, err := ls.Query(t.Context(), utxoWholeQuery(), QueryPoint{})
	require.NoError(t, err)
	utxos := requireUtxoWholeMap(t, result)
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

func TestQueryShelleyUtxoWhole_SerializesCompleteResults(t *testing.T) {
	// Not t.Parallel: swaps the package-level decodeUtxoWholeCborFunc seam.
	db := newTestDB(t)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	seedBabbageUtxo(t, db, 0xA1, 0, addr, 1_000_000)

	firstEntered := make(chan struct{})
	secondEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	original := decodeUtxoWholeCborFunc
	var calls atomic.Int64
	decodeUtxoWholeCborFunc = func(
		ref database.UtxoRef,
		cborBytes []byte,
	) (ledger.TransactionOutput, error) {
		switch calls.Add(1) {
		case 1:
			close(firstEntered)
			<-releaseFirst
		case 2:
			close(secondEntered)
		}
		return original(ref, cborBytes)
	}
	t.Cleanup(func() { decodeUtxoWholeCborFunc = original })

	ls := newPoolDistr2Ledger(t, db)
	type queryResult struct {
		value any
		err   error
	}
	results := make(chan queryResult, 2)
	runQuery := func() {
		value, err := ls.Query(t.Context(), utxoWholeQuery(), QueryPoint{})
		results <- queryResult{value: value, err: err}
	}
	go runQuery()
	testutil.RequireReceive(
		t,
		firstEntered,
		testutil.AsyncWait,
		"first whole-UTxO query must start",
	)
	go runQuery()
	testutil.RequireNoReceive(
		t,
		secondEntered,
		100*time.Millisecond,
		"second whole-UTxO query must wait for admission",
	)
	close(releaseFirst)
	testutil.RequireReceive(
		t,
		secondEntered,
		testutil.AsyncWait,
		"second whole-UTxO query must start after admission is released",
	)

	for range 2 {
		result := testutil.RequireReceive(
			t,
			results,
			testutil.AsyncWait,
			"whole-UTxO query must complete",
		)
		require.NoError(t, result.err)
		require.Len(
			t,
			requireUtxoWholeMap(t, result.value),
			1,
			"admission must not truncate the whole-UTxO response",
		)
	}
}

func TestQueryShelleyUtxoWhole_ADAOnlyUsesCardanoNodeEncoding(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	txID := seedBabbageUtxo(t, db, 0xA1, 0, addr, 1_000_000)

	ls := newPoolDistr2Ledger(t, db)
	result, err := ls.Query(t.Context(), utxoWholeQuery(), QueryPoint{})
	require.NoError(t, err)
	bounded, ok := result.(*utxoWholeResult)
	require.True(t, ok)

	var wire []map[olocalstatequery.UtxoId]cbor.RawMessage
	_, err = cbor.Decode(bounded.encoded, &wire)
	require.NoError(t, err)
	require.Len(t, wire, 1)
	encoded, ok := wire[0][olocalstatequery.UtxoId{
		Hash: ledger.NewBlake2b256(txID),
		Idx:  0,
	}]
	require.True(t, ok)
	require.Equal(
		t,
		"82581d60"+strings.Repeat("aa", lcommon.AddressHashSize)+"1a000f4240",
		hex.EncodeToString(encoded),
	)
}

// TestQueryShelleyUtxoWhole_ByronOutputUsesShelleyEncoding: a Byron output is
// stored with its address as an inline CBOR structure, and must be served with
// that structure wrapped in a byte string, as a Shelley-era client decodes it.
func TestQueryShelleyUtxoWhole_ByronOutputUsesShelleyEncoding(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	addr, err := lcommon.NewAddress(
		"Ae2tdPwUPEZKQuZh2UndEoTKEakMYHGNjJVYmNZgJk2qqgHouxDsA5oT83n",
	)
	require.NoError(t, err)
	addrBytes, err := addr.Bytes()
	require.NoError(t, err)
	byronOut, err := cbor.Encode(
		[]any{cbor.RawMessage(addrBytes), uint64(2_000_000)},
	)
	require.NoError(t, err)
	txID := bytes.Repeat([]byte{0xB1}, 32)
	fxSeedOutput(t, db, txID, 0, 100, byronOut)

	ls := newPoolDistr2Ledger(t, db)
	result, err := ls.Query(t.Context(), utxoWholeQuery(), QueryPoint{})
	require.NoError(t, err)
	bounded, ok := result.(*utxoWholeResult)
	require.True(t, ok)

	var wire []map[olocalstatequery.UtxoId]cbor.RawMessage
	_, err = cbor.Decode(bounded.encoded, &wire)
	require.NoError(t, err)
	require.Len(t, wire, 1)
	encoded, ok := wire[0][olocalstatequery.UtxoId{
		Hash: ledger.NewBlake2b256(txID),
		Idx:  0,
	}]
	require.True(t, ok)
	addrField, err := cbor.Encode(addrBytes)
	require.NoError(t, err)
	require.Equal(
		t,
		"82"+hex.EncodeToString(addrField)+"1a001e8480",
		hex.EncodeToString(encoded),
	)
	out, err := ledger.NewTransactionOutputFromCbor(encoded)
	require.NoError(t, err)
	require.Equal(t, addr.String(), out.Address().String())
	require.Equal(t, uint64(2_000_000), out.Amount().Uint64())
}

// TestQueryShelleyUtxoWhole_EmptyLedger covers a chain with no UTxOs at
// all: the query must return an empty, non-nil map rather than failing.
func TestQueryShelleyUtxoWhole_EmptyLedger(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	result, err := ls.queryShelleyUtxoWhole(t.Context(), QueryPoint{}, nil)
	require.NoError(t, err)
	utxos := requireUtxoWholeMap(t, result)
	require.Empty(t, utxos)
}

func TestQueryShelleyUtxoWhole_CachesCheckedEncoding(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAC}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	seedBabbageUtxo(t, db, 0xC2, 1, addr, 2_000_000)
	seedBabbageUtxo(t, db, 0xC1, 0, addr, 1_000_000)
	ls := newPoolDistr2Ledger(t, db)
	result, err := ls.queryShelleyUtxoWhole(t.Context(), QueryPoint{}, nil)
	require.NoError(t, err)
	bounded, ok := result.(*utxoWholeResult)
	require.True(t, ok)

	encodedByProtocol, err := cbor.Encode(&result)
	require.NoError(t, err)
	require.Equal(t, bounded.encoded, encodedByProtocol)

	utxos := requireUtxoWholeMap(t, result)
	canonical, err := cbor.Encode(&[]any{utxos})
	require.NoError(t, err)
	require.Equal(t, canonical, bounded.encoded)
}

func TestQueryShelleyUtxoWholeViaGetCBOR(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAC}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	seedBabbageUtxo(t, db, 0xC1, 0, addr, 1_000_000)
	ls := newPoolDistr2Ledger(t, db)

	direct, err := ls.Query(t.Context(), utxoWholeQuery(), QueryPoint{})
	require.NoError(t, err)
	bounded, ok := direct.(*utxoWholeResult)
	require.True(t, ok)

	wrapped, err := ls.Query(t.Context(), utxoWholeCborQuery(), QueryPoint{})
	require.NoError(t, err)
	values, ok := wrapped.([]any)
	require.True(t, ok)
	require.Len(t, values, 1)
	tag, ok := values[0].(cbor.Tag)
	require.True(t, ok)
	require.EqualValues(t, cbor.CborTagCbor, tag.Number)
	require.Equal(t, bounded.encoded[1:], tag.Content)
	content, ok := tag.Content.([]byte)
	require.True(t, ok)
	var decoded map[olocalstatequery.UtxoId]cbor.RawMessage
	_, err = cbor.Decode(content, &decoded)
	require.NoError(t, err)
	require.Len(t, decoded, 1)
}

func TestQueryShelleyUtxoWholeRejectsWrongLengthTransactionID(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xA1}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	out := babbage.BabbageTransactionOutput{
		OutputAddress: addr,
		OutputAmount:  mary.MaryTransactionOutputValue{Amount: 1_000_000},
	}
	cborBytes, err := cbor.Encode(&out)
	require.NoError(t, err)
	ls := newPoolDistr2Ledger(t, db)
	set := newQuerySetHistory()
	set.rawRows = []models.Utxo{{TxId: bytes.Repeat([]byte{0xA2}, 31), Cbor: cborBytes}}
	ls.config.UtxoSet = set
	result, err := ls.queryShelleyUtxoWhole(t.Context(), QueryPoint{}, nil)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "blake2b-256")
}

// TestQueryShelleyUtxoWhole_UnrecoverableRowFailsQuery covers a live UTxO
// reader whose CBOR cannot be resolved: the whole query
// must fail rather than silently return a reply missing that row.
// GetUTxOWhole's contract is every live UTxO, and node-parity compares this
// reply against a real cardano-node -- a silently short reply would read as a
// ledger divergence rather than the storage fault it actually is, exactly like
// IterateLiveUtxos' own loadCbor path already fails rather than omits.
func TestQueryShelleyUtxoWhole_UnrecoverableRowFailsQuery(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	addrA, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	seedBabbageUtxo(t, db, 0xA1, 0, addrA, 1_000_000)

	ls := newPoolDistr2Ledger(t, db)
	// A set whose scan fails partway stands in for a row the store cannot
	// read.
	set := newQuerySetHistory()
	set.readErr = database.ErrUtxoCborUnavailable
	ls.config.UtxoSet = set

	_, err = ls.queryShelleyUtxoWhole(t.Context(), QueryPoint{}, nil)
	require.ErrorIs(t, err, database.ErrUtxoCborUnavailable)
}

// TestQueryShelleyUtxoWhole_DecodePanicFailsQuery proves a panic while
// decoding an output becomes an ordinary query error identifiable as
// database.ErrTxnPanic. It injects the panic through decodeUtxoWholeCborFunc
// rather than depending on a CBOR byte sequence that panics the real decoder.
func TestQueryShelleyUtxoWhole_DecodePanicFailsQuery(t *testing.T) {
	// Not t.Parallel: swaps the package-level decodeUtxoWholeCborFunc seam.
	db := newTestDB(t)

	addrA, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	seedBabbageUtxo(t, db, 0xA1, 0, addrA, 1_000_000)

	original := decodeUtxoWholeCborFunc
	decodeUtxoWholeCborFunc = func(
		_ database.UtxoRef,
		_ []byte,
	) (ledger.TransactionOutput, error) {
		panic("simulated decode panic")
	}
	t.Cleanup(func() { decodeUtxoWholeCborFunc = original })

	ls := newPoolDistr2Ledger(t, db)

	var queryErr error
	require.NotPanics(t, func() {
		_, queryErr = ls.queryShelleyUtxoWhole(t.Context(), QueryPoint{}, nil)
	}, "a decode panic must not escape the query")
	require.Error(t, queryErr)
	require.Contains(t, queryErr.Error(), "panicked")
	require.ErrorIs(t, queryErr, database.ErrTxnPanic)
}

func TestQueryShelleyUtxoWhole_CancelsDuringDecode(t *testing.T) {
	// Not t.Parallel: swaps the package-level decodeUtxoWholeCborFunc seam.
	db := newTestDB(t)
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	const rowCount = 8
	for i := range rowCount {
		seedBabbageUtxo(
			t,
			db,
			byte(i),
			uint32(i),
			addr,
			1_000_000,
		) //nolint:gosec
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var decodedCount atomic.Int64
	original := decodeUtxoWholeCborFunc
	decodeUtxoWholeCborFunc = func(ref database.UtxoRef, cborBytes []byte) (ledger.TransactionOutput, error) {
		decodedCount.Add(1)
		cancel()
		return decodeUtxoWholeCbor(ref, cborBytes)
	}
	t.Cleanup(func() { decodeUtxoWholeCborFunc = original })
	ls := newPoolDistr2Ledger(t, db)
	errCh := make(chan error, 1)
	go func() {
		_, err := ls.queryShelleyUtxoWhole(ctx, QueryPoint{}, nil)
		errCh <- err
	}()
	err = testutil.RequireReceive(
		t,
		errCh,
		testutil.AsyncWait,
		"canceled UTxO query must return",
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(
		t,
		int64(1),
		decodedCount.Load(),
		"no output may be decoded after the cancellation",
	)
}

// TestQueryShelleyUtxoWhole_PinnedPointExcludesUtxoCreatedAfterIt covers
// the core pinning fix: a pin must not report a UTxO
// created after the pinned slot. Before this fix, queryShelleyUtxoWhole
// took no point argument at all and always answered from live state --
// confirmed live against a real Preview cardano-node during node-parity's
// periodic full checks: every UTxO a diverged check flagged as "present in
// Dingo, missing in cardano-node" had its own AddedSlot strictly after the
// check's pinned slot, because Dingo silently answered with newer data
// than what the pin claimed to represent.
func TestQueryShelleyUtxoWhole_PinnedPointExcludesUtxoCreatedAfterIt(
	t *testing.T,
) {
	t.Parallel()

	db := fxOpenDB(t)
	ls := newPoolDistr2Ledger(t, db)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1_000, repeatedBytes(32, 0x0B)),
	}, nil))

	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	// seedBabbageUtxo always creates its row at AddedSlot 100.
	txIdOld := seedBabbageUtxo(t, db, 0xA1, 0, addr, 1_000_000)

	newCbor := queryTestOutputCBOR(t, db, txIdOld, 0)
	txIdNew := bytes.Repeat([]byte{0xB2}, 32)
	require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error {
		return db.Blob().SetUtxo(txn.Blob(), txIdNew, 0, newCbor)
	}))

	attachQueryUtxoHistory(t, ls, map[uint64][]UtxoEvent{
		100: {{Added: []UtxoOutput{{
			Ref:  models.UtxoId{Hash: txIdOld, Idx: 0},
			Cbor: queryTestOutputCBOR(t, db, txIdOld, 0),
		}}}},
		500: {{Added: []UtxoOutput{{
			Ref:  models.UtxoId{Hash: txIdNew, Idx: 0},
			Cbor: queryTestOutputCBOR(t, db, txIdNew, 0),
		}}}},
	})

	result, err := ls.queryShelleyUtxoWhole(
		t.Context(),
		QueryPoint{Slot: 300},
		nil,
	)
	require.NoError(t, err)
	utxos := requireUtxoWholeMap(t, result)

	_, hasOld := utxos[olocalstatequery.UtxoId{
		Hash: ledger.NewBlake2b256(txIdOld), Idx: 0,
	}]
	require.True(t, hasOld, "a UTxO created before the pin must be reported")
	_, hasNew := utxos[olocalstatequery.UtxoId{
		Hash: ledger.NewBlake2b256(txIdNew), Idx: 0,
	}]
	require.False(
		t, hasNew,
		"a UTxO created after the pinned slot must not be reported -- "+
			"it did not exist yet as of that point",
	)
}

// TestQueryShelleyUtxoWhole_PinnedPointIncludesUtxoSpentAfterIt covers the
// other half of the same fix: a UTxO live at the pinned slot but spent
// later must still be reported, proving this answers "live as of the pin"
// rather than merely "created before the pin."
func TestQueryShelleyUtxoWhole_PinnedPointIncludesUtxoSpentAfterIt(
	t *testing.T,
) {
	t.Parallel()

	db := fxOpenDB(t)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1_000, repeatedBytes(32, 0x0B)),
	}, nil))

	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	// seedBabbageUtxo always creates its row at AddedSlot 100.
	txId := seedBabbageUtxoCbor(t, db, 0xC1, 0, addr, 5_000_000)

	ls := newPoolDistr2Ledger(t, db)
	attachQueryUtxoLifecycle(t, ls, db, txId, 0, 100, 500)

	pinned, err := ls.queryShelleyUtxoWhole(
		t.Context(),
		QueryPoint{Slot: 300},
		nil,
	)
	require.NoError(t, err)
	pinnedUtxos := requireUtxoWholeMap(t, pinned)
	_, stillLiveAtPin := pinnedUtxos[olocalstatequery.UtxoId{
		Hash: ledger.NewBlake2b256(txId), Idx: 0,
	}]
	require.True(
		t, stillLiveAtPin,
		"a UTxO spent after the pinned slot (500) was still live as of "+
			"the pin (300) and must be reported",
	)

	live, err := ls.queryShelleyUtxoWhole(t.Context(), QueryPoint{}, nil)
	require.NoError(t, err)
	liveUtxos := requireUtxoWholeMap(t, live)
	_, stillLiveNow := liveUtxos[olocalstatequery.UtxoId{
		Hash: ledger.NewBlake2b256(txId), Idx: 0,
	}]
	require.False(
		t, stillLiveNow,
		"the unpinned (live) query must correctly report it as spent now",
	)
}

// TestQueryShelleyUtxoWhole_RetentionWindow_TooOldRejected requires a pin
// older than the LedgerDB set's pruned UTxO history to be rejected with
// ErrUtxoAsOfUnavailable rather than answered from an incomplete historical
// set.
func TestQueryShelleyUtxoWhole_RetentionWindow_TooOldRejected(t *testing.T) {
	t.Parallel()

	db := fxOpenDB(t)
	ls := newPoolDistr2Ledger(t, db)
	// newPoolDistr2Ledger leaves CardanoNodeConfig nil, so
	// calculateStabilityWindow returns the default (50_000) regardless of
	// era -- matching TestQueryShelleyUtxoByTxIn_RetentionWindow_TooOldRejected's
	// identical setup.
	const tipSlot = 200_000
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(tipSlot, repeatedBytes(32, 0x0B)),
	}, nil))

	// floor = 200_000 - 50_000 = 150_000, where the node drops the set's
	// history. The oldest record kept is the block at 150_000, which
	// restores the set its parent at 100_000 left; one slot behind that
	// must reject.
	set := attachQueryUtxoHistory(t, ls, map[uint64][]UtxoEvent{
		100_000: {},
		150_000: {},
		200_000: {},
	})
	pruner, ok := set.(interface{ PruneHistory(slot uint64) error })
	require.True(t, ok, "the fixture's set must prune its history")
	require.NoError(t, pruner.PruneHistory(150_000))
	_, err := ls.queryShelleyUtxoWhole(
		t.Context(),
		QueryPoint{Slot: 99_999},
		nil,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrUtxoAsOfUnavailable)
}

// TestQueryShelleyUtxoWhole_RetentionWindow_AtFloor_Succeeds covers the
// exact floor slot itself, mirroring
// TestQueryShelleyUtxoByTxIn_RetentionWindow_AtFloor_Succeeds: a pin naming
// the floor slot exactly must be accepted, not rejected.
func TestQueryShelleyUtxoWhole_RetentionWindow_AtFloor_Succeeds(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	const tipSlot = 200_000
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(tipSlot, repeatedBytes(32, 0x0B)),
	}, nil))
	attachQueryUtxoHistory(t, ls, map[uint64][]UtxoEvent{
		150_000: {},
		200_000: {},
	})

	_, err := ls.queryShelleyUtxoWhole(
		t.Context(),
		QueryPoint{Slot: 150_000},
		nil,
	)
	require.NoError(t, err)
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
	t.Parallel()

	ref := database.UtxoRef{OutputIdx: 0}
	ref.TxId[0], ref.TxId[1] = 0x01, 0x02

	require.NotPanics(t, func() {
		_, _ = decodeUtxoWholeCbor(ref, []byte{0xff})
	})
	_, err := decodeUtxoWholeCbor(ref, []byte{0xff})
	require.Error(t, err)
	require.Contains(t, err.Error(), "0102")
}
