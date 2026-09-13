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
	"errors"
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

	result, err := ls.Query(utxoWholeQuery(), QueryPoint{})
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
	t.Parallel()

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

// TestQueryShelleyUtxoWhole_UnrecoverableRowFailsQuery covers a live UTxO
// row whose CBOR cannot be resolved even via recovery (no blob entry and no
// producer transaction metadata to reconstruct it from): the whole query
// must fail rather than silently return a reply missing that row.
// GetUTxOWhole's contract is every live UTxO, and node-parity (#1900)
// compares this reply against a real cardano-node -- a silently short reply
// would read as a ledger divergence rather than the storage fault it
// actually is, exactly like IterateLiveUtxos' own loadCbor path already
// fails rather than omits.
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

	// A live metadata row with no blob entry and no producer transaction to
	// recover from -- ResolveUtxoCborWithRecovery must confirm this as
	// ErrUtxoCborUnavailable rather than reconstructing it.
	unrecoverableTxId := bytes.Repeat([]byte{0xC3}, 32)
	txn := db.Transaction(true)
	require.NoError(t, db.CreateUtxo(txn, &models.Utxo{
		TxId:      unrecoverableTxId,
		OutputIdx: 0,
		AddedSlot: 100,
	}))
	require.NoError(t, txn.Commit())

	ls := newPoolDistr2Ledger(t, db)

	_, err = ls.queryShelleyUtxoWhole()
	require.Error(t, err)
}

// TestQueryShelleyUtxoWhole_WorkerPanicDoesNotCrashProcess is the
// regression test for a chrisguiney review finding on PR #4084: a panic
// during a worker's resolve or decode step used to escape the worker
// goroutine entirely and terminate the whole node process, where the
// previous sequential implementation (running inside IterateLiveUtxos'
// Txn.Do, whose own recover converts a panic to ErrTxnPanic) would have
// turned it into an ordinary query error instead. Uses the
// decodeUtxoWholeCborFunc seam to inject a real panic deterministically,
// rather than hunting for a specific CBOR byte sequence that happens to
// panic the real decoder.
func TestQueryShelleyUtxoWhole_WorkerPanicDoesNotCrashProcess(t *testing.T) {
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
		_, queryErr = ls.queryShelleyUtxoWhole()
	}, "a worker panic must not escape and crash the process")
	require.Error(t, queryErr)
	require.Contains(t, queryErr.Error(), "panicked")
	require.ErrorIs(
		t, queryErr, database.ErrTxnPanic,
		"a recovered worker panic must be identifiable via "+
			"errors.Is(err, database.ErrTxnPanic), matching the "+
			"sequential implementation's Txn.Do recovery contract "+
			"(cubic review)",
	)
}

// TestQueryShelleyUtxoWhole_AbortsEarlyOnFirstFailure is the regression
// test for a chrisguiney review finding on PR #4084: once one row's
// resolve fails, the previous implementation kept feeding every remaining
// row to the worker pool instead of stopping -- unlike the earlier
// sequential implementation, which aborted its whole traversal on the
// first failure via the error it returned from IterateLiveUtxos'
// callback.
//
// Every seeded row is otherwise identical and independently resolvable
// (no single row is "the" unrecoverable one): whichever row a worker
// happens to reach first fails immediately, and every row reached after
// that is slow but succeeds. This is deliberate, per a cubic review
// finding on an earlier version of this test that keyed the failure to a
// specific seeded row: IterateLiveUtxos issues its live-row scan with no
// ORDER BY, so which row a database iteration visits first is
// unspecified, not a contract this test may depend on. Keying the
// failure to "whichever row is first," rather than to a specific row's
// identity, makes the test's outcome independent of that unspecified
// order.
func TestQueryShelleyUtxoWhole_AbortsEarlyOnFirstFailure(t *testing.T) {
	// Not t.Parallel: swaps the package-level decodeUtxoWholeCborFunc seam.
	db := newTestDB(t)

	const rowCount = 100
	addr, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeKeyNone,
		lcommon.AddressNetworkTestnet,
		bytes.Repeat([]byte{0xAA}, lcommon.AddressHashSize),
		nil,
	)
	require.NoError(t, err)
	for i := range rowCount {
		seedBabbageUtxo(
			t, db, byte(i), uint32(i), addr, 1_000_000, //nolint:gosec
		)
	}

	var (
		decodedCount atomic.Int64
		failed       atomic.Bool
	)
	release := make(chan struct{})
	original := decodeUtxoWholeCborFunc
	decodeUtxoWholeCborFunc = func(
		ref database.UtxoRef,
		cborBytes []byte,
	) (ledger.TransactionOutput, error) {
		if failed.CompareAndSwap(false, true) {
			return nil, errors.New("simulated first-row failure")
		}
		// Block until the test releases this gate, rather than a
		// fixed-duration time.Sleep: the test only opens it once it has
		// deterministically observed (via utxoWholeAbortObservedFunc,
		// below) that queryShelleyUtxoWhole has already closed its
		// internal done channel, so nothing here depends on decode
		// speed or on a hand-tuned sleep duration that could silently
		// stop covering this if utxoWholeResolveWorkers or decode cost
		// ever changed (cubic review).
		<-release
		decodedCount.Add(1)
		return decodeUtxoWholeCbor(ref, cborBytes)
	}
	t.Cleanup(func() { decodeUtxoWholeCborFunc = original })

	abortObserved := make(chan struct{})
	originalAbortHook := utxoWholeAbortObservedFunc
	utxoWholeAbortObservedFunc = func() { close(abortObserved) }
	t.Cleanup(func() { utxoWholeAbortObservedFunc = originalAbortHook })

	ls := newPoolDistr2Ledger(t, db)

	errCh := make(chan error, 1)
	go func() {
		_, err := ls.queryShelleyUtxoWhole()
		errCh <- err
	}()

	// The CAS above guarantees exactly one decode call ever returns the
	// simulated failure, and that is always the first result the main
	// loop can receive (every other row is still parked on <-release),
	// so this always fires -- deterministically, not racing a clock.
	testutil.RequireReceive(
		t, abortObserved, 5*time.Second,
		"queryShelleyUtxoWhole must observe the abort",
	)
	close(release)

	err = testutil.RequireReceive(
		t, errCh, 5*time.Second, "queryShelleyUtxoWhole must return",
	)
	require.Error(t, err)
	// A bound near utxoWholeResolveWorkers, not just under rowCount: only
	// the rows already dispatched by the time the abort above fired
	// should ever reach the gated decode branch. Without the early
	// abort, every remaining row of the 99 still would (rowCount minus
	// the one that failed), so this stays a meaningful regression check
	// rather than one only a total mechanism removal could fail.
	require.Less(
		t, decodedCount.Load(), int64(rowCount/2),
		"early abort must stop the feeder well before every row is decoded",
	)
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
