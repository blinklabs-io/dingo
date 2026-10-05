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
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// leiosApplyTestProducerTx builds a Dijkstra endorser transaction with no
// inputs that produces two enterprise (payment-only) outputs, so later
// transactions have live UTxOs to consume.
func leiosApplyTestProducerTx(
	t *testing.T,
	seed byte,
) (cbor.RawMessage, lcommon.Transaction) {
	t.Helper()
	addr := append([]byte{0x60}, bytes.Repeat([]byte{seed}, 28)...)
	bodyCbor, err := cbor.Encode(map[uint]any{
		0: cbor.Tag{Number: 258, Content: []any{}},
		1: []any{
			map[uint]any{0: addr, 1: uint64(1_000_000)},
			map[uint]any{0: addr, 1: uint64(2_000_000)},
		},
		2: uint64(200_000),
	})
	require.NoError(t, err)
	return leiosApplyTestTxFromBody(t, bodyCbor)
}

// leiosApplyTestSpendingTx builds a Dijkstra endorser transaction consuming
// inTxId#inIdx and producing a single output.
func leiosApplyTestSpendingTx(
	t *testing.T,
	seed byte,
	inTxId []byte,
	inIdx uint64,
) (cbor.RawMessage, lcommon.Transaction) {
	t.Helper()
	addr := append([]byte{0x60}, bytes.Repeat([]byte{seed}, 28)...)
	bodyCbor, err := cbor.Encode(map[uint]any{
		0: cbor.Tag{Number: 258, Content: []any{[]any{inTxId, inIdx}}},
		1: []any{map[uint]any{0: addr, 1: uint64(500_000)}},
		// A distinct fee per seed keeps the transaction hashes distinct.
		2: uint64(200_000) + uint64(seed),
	})
	require.NoError(t, err)
	return leiosApplyTestTxFromBody(t, bodyCbor)
}

func leiosApplyTestTxFromBody(
	t *testing.T,
	bodyCbor []byte,
) (cbor.RawMessage, lcommon.Transaction) {
	t.Helper()
	txCbor, err := cbor.Encode([]any{
		cbor.RawMessage(bodyCbor),
		map[uint]any{},
		true,
		nil,
	})
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(gledger.TxTypeDijkstra, txCbor)
	require.NoError(t, err)
	return cbor.RawMessage(txCbor), tx
}

func TestBuildEndorserBlockBlobIndexesDijkstraBatchLevels(t *testing.T) {
	t.Parallel()

	tx := &dijkstra.DijkstraTransaction{
		Body: dijkstra.DijkstraTransactionBody{
			TxSubTransactions: cbor.NewSetType(
				[]dijkstra.DijkstraSubTransaction{{
					Body: dijkstra.DijkstraSubTransactionBody{},
				}},
				true,
			),
		},
		TxIsValid: true,
	}
	txCbor, err := tx.MarshalCBOR()
	require.NoError(t, err)
	tx, err = dijkstra.NewDijkstraTransactionFromCbor(txCbor)
	require.NoError(t, err)
	_, elems, err := decodeEndorserTxEnvelope(txCbor)
	require.NoError(t, err)
	levels := TransactionLevelsForApply(tx)
	require.Len(t, levels, 2)
	subBodies := tx.Body.TxSubTransactions.Items()
	require.Len(t, subBodies, 1)

	blob, offsets, err := buildEndorserBlockBlob(
		[]lcommon.Transaction{tx},
		[][]byte{[]byte(elems[0])},
		42,
		[32]byte(bytes.Repeat([]byte{0x42}, 32)),
	)
	require.NoError(t, err)
	require.Len(t, offsets.TxOffsets, len(levels))
	// Each stored range must be the body its key hashes, as BlockIndexer
	// records: the sub-transaction body, then the enclosing body element
	// rather than the whole [body, witnesses, isValid, aux] array.
	want := [][]byte{subBodies[0].Body.Cbor(), []byte(elems[0])}
	for idx, level := range levels {
		var hash [32]byte
		copy(hash[:], level.Hash().Bytes())
		offset, ok := offsets.TxOffsets[hash]
		require.True(t, ok)
		end := uint64(offset.ByteOffset) + uint64(offset.ByteLength)
		require.LessOrEqual(t, end, uint64(len(blob)))
		stored := blob[offset.ByteOffset:end]
		require.Equal(t, want[idx], stored, "level %d body range", idx)
		require.Equal(t, level.Hash(), lcommon.Blake2b256Hash(stored))
	}
	var enclosing [32]byte
	copy(enclosing[:], tx.Hash().Bytes())
	offset := offsets.TxOffsets[enclosing]
	body, err := dijkstra.NewDijkstraTransactionBodyFromCbor(
		blob[offset.ByteOffset : offset.ByteOffset+offset.ByteLength],
	)
	require.NoError(t, err)
	require.Equal(t, tx.Hash(), body.Id())
}

// leiosApplyTestApplyEndorserBlock applies one endorser block in its own
// database transaction, mirroring how ledgerProcessBlock applies the certified
// closure ahead of the ranking block's own transactions.
func leiosApplyTestApplyEndorserBlock(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	rbPoint ocommon.Point,
	rbBlockNumber uint64,
	ebSlot uint64,
	ebHash []byte,
	rawTxs ...cbor.RawMessage,
) (int, error) {
	t.Helper()
	applied := -1
	txn := db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		var err error
		applied, _, err = ls.applyEndorserBlock(
			txn,
			rbPoint,
			rbBlockNumber,
			ebSlot,
			ebHash,
			rawTxs,
		)
		return err
	})
	return applied, err
}

// leiosApplyTestUtxoState returns a produced UTxO's spender and deletion slot.
func leiosApplyTestUtxoState(
	t *testing.T,
	raw *sql.DB,
	txId []byte,
	outputIdx uint32,
) (spentBy []byte, deletedSlot uint64) {
	t.Helper()
	var spender []byte
	require.NoError(t, raw.QueryRow(`
SELECT spent_at_tx_id, deleted_slot FROM utxo
WHERE tx_id = ? AND output_idx = ?`,
		txId,
		outputIdx,
	).Scan(&spender, &deletedSlot))
	return spender, deletedSlot
}

// The Musashi/Haskell-conformant closure apply folds a certified endorser
// block's transactions onto the ledger without validation, mirroring the
// reference ledger's applyLeiosClosure (ruleApplyTxValidation ValidateNone in
// Ouroboros.Consensus.Shelley.Ledger.Leios). Two certified endorser blocks may
// therefore name the same input across blocks: for the reference the second
// consume is Map.delete on a missing key -- a no-op -- and the transaction's
// produced outputs are still added.
//
// This is the wedge reported as ("UTxO already spent" while
// applying the certified endorser block at ranking-block slot 1864040): the
// failing apply is the endorser block's, and the conflict is between two
// *different* certified transactions, so no transaction-hash dedup can address
// it. The tolerance lives in the metadata store (SetTransactionLeiosClosure)
// and is selected by BatchedTxIngestOpts.SkipConsumedInputRecovery; this test
// covers the ledger-side wiring that reaches it (applyEndorserBlock ->
// LedgerDelta.skipConsumedInputRecovery -> Database.SetTransactionWithOpts),
// which the store-level test in database/plugin/metadata/sqlite cannot reach.
func TestApplyEndorserBlockHaskellPathToleratesCrossEndorserDoubleConsume(
	t *testing.T,
) {
	t.Parallel()

	ls, db, gdb := newLeiosApplyTestLedger(t)
	// LeiosApplyEndorserBlockTxs defaults to false (Haskell-conformant).
	rawProducer, producerTx := leiosApplyTestProducerTx(t, 0xa1)
	require.Len(t, producerTx.Produced(), 2)
	producerHash := producerTx.Hash().Bytes()
	rawFirst, firstTx := leiosApplyTestSpendingTx(t, 0xb1, producerHash, 0)
	rawSecond, secondTx := leiosApplyTestSpendingTx(t, 0xb2, producerHash, 0)
	require.NotEqual(
		t,
		firstTx.Hash(),
		secondTx.Hash(),
		"the double-consume must come from two distinct transactions",
	)

	producerPoint := leiosApplyTestRankingPoint(0x11)
	firstPoint := leiosApplyTestRankingPoint(0x12)
	secondPoint := leiosApplyTestRankingPoint(0x13)

	applied, err := leiosApplyTestApplyEndorserBlock(
		t, ls, db, producerPoint, 1, 900, leiosApplyTestEbHash(0xa2),
		rawProducer,
	)
	require.NoError(t, err)
	require.Equal(t, 1, applied)

	applied, err = leiosApplyTestApplyEndorserBlock(
		t, ls, db, firstPoint, 2, 901, leiosApplyTestEbHash(0xb3),
		rawFirst,
	)
	require.NoError(t, err)
	require.Equal(t, 1, applied)
	spentBy, deletedSlot := leiosApplyTestUtxoState(t, gdb, producerHash, 0)
	require.Equal(t, firstTx.Hash().Bytes(), spentBy)
	require.Equal(t, firstPoint.Slot, deletedSlot)

	// The second certified endorser block re-consumes the same input. The
	// closure apply must fold it on instead of failing with ErrUtxoConflict.
	applied, err = leiosApplyTestApplyEndorserBlock(
		t, ls, db, secondPoint, 3, 902, leiosApplyTestEbHash(0xb4),
		rawSecond,
	)
	require.NoError(t, err)
	require.Equal(t, 1, applied)

	// The contested input stays consumed by the first certified transaction,
	// at the ranking-block slot that applied it.
	spentBy, deletedSlot = leiosApplyTestUtxoState(t, gdb, producerHash, 0)
	require.Equal(t, firstTx.Hash().Bytes(), spentBy)
	require.Equal(t, firstPoint.Slot, deletedSlot)

	// Absence case: the second transaction is present in the endorser block
	// and in no ranking block, so it must still be applied -- its row is
	// recorded and its produced output is live at its ranking block's slot.
	var storedSlot uint64
	require.NoError(t, gdb.QueryRow(`
SELECT slot FROM "transaction" WHERE hash = ?`,
		secondTx.Hash().Bytes(),
	).Scan(&storedSlot))
	require.Equal(t, secondPoint.Slot, storedSlot)
	spentBy, deletedSlot = leiosApplyTestUtxoState(
		t, gdb, secondTx.Hash().Bytes(), 0,
	)
	require.Empty(t, spentBy)
	require.Equal(t, uint64(0), deletedSlot)
	var addedSlot uint64
	require.NoError(t, gdb.QueryRow(`
SELECT added_slot FROM utxo WHERE tx_id = ? AND output_idx = 0`,
		secondTx.Hash().Bytes(),
	).Scan(&addedSlot))
	require.Equal(t, secondPoint.Slot, addedSlot)
}

// The tolerance is scoped to the Musashi closure apply. On the CIP-conformant
// path (LeiosApplyEndorserBlockTxs true) endorser transactions are applied with
// ranking-block semantics -- consumed-input recovery stays on and a conflicting
// consume is a hard error -- so a real double-spend still fails and the
// endorser block is refused.
func TestApplyEndorserBlockCIPPathRejectsCrossEndorserDoubleConsume(
	t *testing.T,
) {
	t.Parallel()

	ls, db, gdb := newLeiosApplyTestLedger(t)
	ls.config.LeiosApplyEndorserBlockTxs = true // CIP-conformant path
	rawProducer, producerTx := leiosApplyTestProducerTx(t, 0xc1)
	producerHash := producerTx.Hash().Bytes()
	rawFirst, firstTx := leiosApplyTestSpendingTx(t, 0xd1, producerHash, 0)
	rawSecond, secondTx := leiosApplyTestSpendingTx(t, 0xd2, producerHash, 0)

	firstPoint := leiosApplyTestRankingPoint(0x22)
	_, err := leiosApplyTestApplyEndorserBlock(
		t, ls, db, leiosApplyTestRankingPoint(0x21), 1, 910,
		leiosApplyTestEbHash(0xc2), rawProducer,
	)
	require.NoError(t, err)
	_, err = leiosApplyTestApplyEndorserBlock(
		t, ls, db, firstPoint, 2, 911, leiosApplyTestEbHash(0xd3), rawFirst,
	)
	require.NoError(t, err)

	_, err = leiosApplyTestApplyEndorserBlock(
		t, ls, db, leiosApplyTestRankingPoint(0x23), 3, 912,
		leiosApplyTestEbHash(0xd4), rawSecond,
	)
	require.ErrorIs(t, err, types.ErrUtxoConflict)
	var storageErr *leiosEndorserBlockStorageError
	require.ErrorAs(
		t,
		err,
		&storageErr,
		"a failure after storage mutation must abort the outer transaction",
	)

	// The aborted endorser block left no effects: the contested input is still
	// consumed by the first transaction and the rejected one has no row.
	spentBy, deletedSlot := leiosApplyTestUtxoState(t, gdb, producerHash, 0)
	require.Equal(t, firstTx.Hash().Bytes(), spentBy)
	require.Equal(t, firstPoint.Slot, deletedSlot)
	var rows int64
	require.NoError(t, gdb.QueryRow(`
SELECT COUNT(*) FROM "transaction" WHERE hash = ?`,
		secondTx.Hash().Bytes(),
	).Scan(&rows))
	require.Equal(t, int64(0), rows)
}

// A ranking block's own transactions are applied by a delta with the closure
// options off (ledger/state.go). Absence case for the closure tolerance: a
// transaction present only in the ranking block is applied normally and
// consumes its input, and a ranking-block transaction that re-consumes an
// input an earlier certified endorser-block transaction already spent is still
// rejected as a double-spend.
func TestRankingBlockDeltaKeepsHardConsumedInputConflict(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newLeiosApplyTestLedger(t)
	// LeiosApplyEndorserBlockTxs defaults to false (Haskell-conformant).
	rawProducer, producerTx := leiosApplyTestProducerTx(t, 0xe1)
	producerHash := producerTx.Hash().Bytes()
	rawClosure, closureTx := leiosApplyTestSpendingTx(t, 0xf1, producerHash, 0)
	// Spends the producer's second (still live) output.
	rawRankingOnly, rankingOnlyTx := leiosApplyTestSpendingTx(
		t, 0xf2, producerHash, 1,
	)
	// Re-spends the output the certified closure already consumed.
	rawConflict, conflictTx := leiosApplyTestSpendingTx(
		t, 0xf3, producerHash, 0,
	)

	closurePoint := leiosApplyTestRankingPoint(0x31)
	_, err := leiosApplyTestApplyEndorserBlock(
		t, ls, db, leiosApplyTestRankingPoint(0x30), 1, 920,
		leiosApplyTestEbHash(0xe2), rawProducer,
	)
	require.NoError(t, err)
	_, err = leiosApplyTestApplyEndorserBlock(
		t, ls, db, closurePoint, 2, 921, leiosApplyTestEbHash(0xf4),
		rawClosure,
	)
	require.NoError(t, err)

	// A transaction present only in the ranking block still applies.
	rankingPoint := leiosApplyTestRankingPoint(0x32)
	require.NoError(t, leiosApplyTestApplyRankingDelta(
		t, ls, db, rankingPoint, 3, rawRankingOnly, rankingOnlyTx,
	))
	spentBy, deletedSlot := leiosApplyTestUtxoState(t, gdb, producerHash, 1)
	require.Equal(t, rankingOnlyTx.Hash().Bytes(), spentBy)
	require.Equal(t, rankingPoint.Slot, deletedSlot)

	// A ranking-block transaction re-consuming the closure's input is a
	// double-spend and must be rejected.
	err = leiosApplyTestApplyRankingDelta(
		t, ls, db, leiosApplyTestRankingPoint(0x33), 4, rawConflict,
		conflictTx,
	)
	require.ErrorIs(t, err, types.ErrUtxoConflict)
	spentBy, deletedSlot = leiosApplyTestUtxoState(t, gdb, producerHash, 0)
	require.Equal(t, closureTx.Hash().Bytes(), spentBy)
	require.Equal(t, closurePoint.Slot, deletedSlot)
	var rows int64
	require.NoError(t, gdb.QueryRow(`
SELECT COUNT(*) FROM "transaction" WHERE hash = ?`,
		conflictTx.Hash().Bytes(),
	).Scan(&rows))
	require.Equal(t, int64(0), rows)
}

// leiosApplyTestApplyRankingDelta applies one transaction as a ranking-block
// delta, the way ledgerProcessBlock applies a block's own transactions: the
// closure options (skipConsumedInputRecovery) stay off. Ranking-block deltas
// normally take their offsets from database.BlockIndexer.ComputeOffsets over
// the block CBOR; the endorser-blob builder is reused here because the
// consumed-input path only requires that an offset exists for the transaction
// and each produced output.
func leiosApplyTestApplyRankingDelta(
	t *testing.T,
	ls *LedgerState,
	db *database.Database,
	rbPoint ocommon.Point,
	rbBlockNumber uint64,
	rawTx cbor.RawMessage,
	tx lcommon.Transaction,
) error {
	t.Helper()
	var elems []cbor.RawMessage
	_, err := cbor.Decode([]byte(rawTx), &elems)
	require.NoError(t, err)
	var ebHash [lcommon.Blake2b256Size]byte
	copy(ebHash[:], leiosApplyTestEbHash(0x00))
	_, offsets, err := buildEndorserBlockBlob(
		[]lcommon.Transaction{tx},
		// elems holds the decoded array of the caller-built rawTx, which the
		// require.NoError above proves decoded, so index 0 exists.
		//nolint:nilaway // rawTx decodes to a non-empty CBOR array
		[][]byte{[]byte(elems[0])},
		rbPoint.Slot,
		ebHash,
	)
	require.NoError(t, err)
	txn := db.Transaction(true)
	return txn.Do(func(txn *database.Txn) error {
		delta := NewLedgerDelta(
			rbPoint,
			uint(dijkstra.EraIdDijkstra),
			rbBlockNumber,
		)
		defer delta.Release()
		delta.Offsets = offsets
		delta.addTransaction(tx, 0)
		return delta.applyWithoutRecordingDonations(ls, txn)
	})
}

func newLeiosApplyTestLedger(
	t *testing.T,
) (*LedgerState, *database.Database, *sql.DB) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	return ls, db, raw
}

func leiosApplyTestTx(
	t *testing.T,
	seed byte,
) (cbor.RawMessage, []byte, lcommon.Transaction) {
	t.Helper()
	bodyCbor, err := cbor.Encode(map[uint]any{
		0: cbor.Tag{Number: 258, Content: []any{[]any{bytes.Repeat([]byte{seed}, 32), uint64(0)}}},
		1: []any{[]any{append([]byte{0x61}, make([]byte, 28)...), uint64(1000000)}},
		2: 200_000 + uint64(seed),
	})
	require.NoError(t, err)
	txCbor, err := cbor.Encode([]any{
		cbor.RawMessage(bodyCbor),
		map[uint]any{},
		true,
		nil,
	})
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(
		gledger.TxTypeDijkstra,
		txCbor,
	)
	require.NoError(t, err)
	return cbor.RawMessage(txCbor), bodyCbor, tx
}

func leiosApplyTestEbHash(seed byte) []byte {
	return bytes.Repeat([]byte{seed}, lcommon.Blake2b256Size)
}

func leiosApplyTestRankingPoint(seed byte) ocommon.Point {
	return ocommon.Point{
		Slot: 10_000 + uint64(seed),
		Hash: bytes.Repeat([]byte{seed}, lcommon.Blake2b256Size),
	}
}

func requireLeiosApplyTestTxCount(
	t *testing.T,
	raw *sql.DB,
	want int64,
) {
	t.Helper()
	var got int64
	require.NoError(t, raw.QueryRow(
		`SELECT COUNT(*) FROM "transaction"`,
	).Scan(&got))
	require.Equal(t, want, got)
}

func requireLeiosApplyTestEndorserBlob(
	t *testing.T,
	db *database.Database,
	slot uint64,
	hash []byte,
	want []byte,
) {
	t.Helper()
	txn := db.BlobTxn(false)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		got, _, err := db.Blob().GetBlock(txn.Blob(), slot, hash)
		if err != nil {
			return err
		}
		require.Equal(t, want, got)
		return nil
	}))
}

func leiosApplyTestBlob(body []byte, tx lcommon.Transaction) []byte {
	blob := append([]byte(nil), body...)
	for _, utxo := range tx.Produced() {
		blob = append(blob, utxo.Output.Cbor()...)
	}
	return blob
}

func TestApplyEndorserBlockAppliesTransaction(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newLeiosApplyTestLedger(t)
	ls.config.LeiosApplyEndorserBlockTxs = true // CIP-conformant path
	rawTx, bodyCbor, tx := leiosApplyTestTx(t, 0x01)

	const ebSlot = uint64(200)
	ebHash := leiosApplyTestEbHash(0x22)
	applied := -1
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		applied, _, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x33),
			1,
			ebSlot,
			ebHash,
			[]cbor.RawMessage{rawTx},
		)
		return err
	}))

	require.Equal(t, 1, applied)
	requireLeiosApplyTestTxCount(t, gdb, 1)
	requireLeiosApplyTestEndorserBlob(t, db, ebSlot, ebHash, leiosApplyTestBlob(bodyCbor, tx))
	// The transaction is recorded under the ranking block's point.
	var got int64
	require.NoError(t, gdb.QueryRow(
		`SELECT COUNT(*) FROM "transaction" WHERE hash = ?`,
		tx.Hash().Bytes(),
	).Scan(&got))
	require.Equal(t, int64(1), got)
}

func TestApplyEndorserBlockAppliesMultipleTransactions(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newLeiosApplyTestLedger(t)
	ls.config.LeiosApplyEndorserBlockTxs = true // CIP-conformant path
	rawTx1, body1, tx1 := leiosApplyTestTx(t, 0x02)
	rawTx2, body2, tx2 := leiosApplyTestTx(t, 0x03)

	const ebSlot = uint64(300)
	ebHash := leiosApplyTestEbHash(0x44)
	applied := -1
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		applied, _, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x55),
			1,
			ebSlot,
			ebHash,
			[]cbor.RawMessage{rawTx1, rawTx2},
		)
		return err
	}))

	require.Equal(t, 2, applied)
	requireLeiosApplyTestTxCount(t, gdb, 2)
	// The blob contains each body followed by its produced output CBOR.
	want := append(leiosApplyTestBlob(body1, tx1), leiosApplyTestBlob(body2, tx2)...)
	requireLeiosApplyTestEndorserBlob(t, db, ebSlot, ebHash, want)
}

func TestApplyEndorserBlockDeduplicatesCIPTransactions(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newLeiosApplyTestLedger(t)
	ls.config.LeiosApplyEndorserBlockTxs = true // CIP-conformant path
	rawTx1, body1, tx1 := leiosApplyTestTx(t, 0x04)
	rawTx2, _, _ := leiosApplyTestTx(t, 0x05)

	appliedFirst := -1
	appliedSameTxnDuplicate := -1
	appliedSecondUnique := -1
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		appliedFirst, _, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x81),
			1,
			500,
			leiosApplyTestEbHash(0x82),
			[]cbor.RawMessage{rawTx1, rawTx1},
		)
		if err != nil {
			return err
		}
		appliedSameTxnDuplicate, _, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x83),
			2,
			501,
			leiosApplyTestEbHash(0x84),
			[]cbor.RawMessage{rawTx1},
		)
		if err != nil {
			return err
		}
		appliedSecondUnique, _, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x85),
			3,
			502,
			leiosApplyTestEbHash(0x86),
			[]cbor.RawMessage{rawTx2},
		)
		return err
	}))

	require.Equal(t, 1, appliedFirst)
	require.Equal(t, 0, appliedSameTxnDuplicate)
	require.Equal(t, 1, appliedSecondUnique)
	requireLeiosApplyTestTxCount(t, gdb, 2)
	requireLeiosApplyTestEndorserBlob(
		t,
		db,
		500,
		leiosApplyTestEbHash(0x82),
		leiosApplyTestBlob(body1, tx1),
	)

	appliedCommittedDuplicate := -1
	txn = db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		appliedCommittedDuplicate, _, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x87),
			4,
			503,
			leiosApplyTestEbHash(0x88),
			[]cbor.RawMessage{rawTx1},
		)
		return err
	}))
	require.Equal(t, 0, appliedCommittedDuplicate)
	requireLeiosApplyTestTxCount(t, gdb, 2)
}

// On the Haskell-conformant path (Musashi prototype) the endorser block's
// transactions are applied to the ledger with their full effects, matching the
// reference ledger's applyLeiosClosure (ValidateNone), and its metadata and blob
// are stored. Previously this path stored metadata only and did not apply the
// transactions, which diverged the UTxO set from the reference.
func TestApplyEndorserBlockHaskellPathAppliesTransactions(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newLeiosApplyTestLedger(t)
	// LeiosApplyEndorserBlockTxs defaults to false (Haskell-conformant).
	rawTx, bodyCbor, tx := leiosApplyTestTx(t, 0x06)

	const ebSlot = uint64(400)
	ebHash := leiosApplyTestEbHash(0x66)
	applied := -1
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		var err error
		applied, _, err = ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x77),
			1,
			ebSlot,
			ebHash,
			[]cbor.RawMessage{rawTx},
		)
		return err
	}))

	// The transaction is applied and recorded under the ranking block's point,
	// and the endorser blob is stored.
	require.Equal(t, 1, applied)
	requireLeiosApplyTestTxCount(t, gdb, 1)
	var gotTx models.Transaction
	require.NoError(t, gdb.QueryRow(
		`SELECT slot FROM "transaction" WHERE hash = ?`,
		tx.Hash().Bytes(),
	).Scan(&gotTx.Slot))
	require.Equal(t, leiosApplyTestRankingPoint(0x77).Slot, gotTx.Slot)
	requireLeiosApplyTestEndorserBlob(t, db, ebSlot, ebHash, leiosApplyTestBlob(bodyCbor, tx))
}

// leiosApplyTestTxWithOutput builds a Dijkstra endorser transaction that
// produces a single output to an enterprise (payment-only) testnet address, so
// tests can assert the produced UTxO is applied to the store.
func leiosApplyTestTxWithOutput(
	t *testing.T,
	seed byte,
) (cbor.RawMessage, lcommon.Transaction) {
	t.Helper()
	// Enterprise testnet address: header byte 0x60 + 28-byte payment key hash.
	addr := append([]byte{0x60}, bytes.Repeat([]byte{seed}, 28)...)
	bodyCbor, err := cbor.Encode(map[uint]any{
		0: cbor.Tag{Number: 258, Content: []any{}},
		1: []any{ // outputs
			map[uint]any{
				0: addr,
				1: uint64(1_000_000),
			},
		},
		2: uint64(200_000), // fee
	})
	require.NoError(t, err)
	txCbor, err := cbor.Encode([]any{
		cbor.RawMessage(bodyCbor),
		map[uint]any{},
		true,
		nil,
	})
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(gledger.TxTypeDijkstra, txCbor)
	require.NoError(t, err)
	return cbor.RawMessage(txCbor), tx
}

// The Haskell-conformant path applies endorser-block transactions with their
// full UTxO effects: an endorser transaction's produced output becomes a live
// UTxO in the store, stamped at the ranking block's slot so it rolls back with
// the ranking block. This is the fix that keeps the UTxO set — and the stake
// distribution derived from it — complete, matching the reference ledger;
// recording metadata only left the produced outputs missing, which zeroed
// delegator stake and drove the "pool has no stake in epoch snapshot" rejection.
func TestApplyEndorserBlockHaskellPathProducesUtxo(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newLeiosApplyTestLedger(t)
	// LeiosApplyEndorserBlockTxs defaults to false (Haskell-conformant).
	rawTx, tx := leiosApplyTestTxWithOutput(t, 0x6a)
	require.NotEmpty(t, tx.Produced(), "test tx must produce an output")

	rbPoint := leiosApplyTestRankingPoint(0x79)
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		applied, _, err := ls.applyEndorserBlock(
			txn,
			rbPoint,
			1,
			420,
			leiosApplyTestEbHash(0x6b),
			[]cbor.RawMessage{rawTx},
		)
		if err != nil {
			return err
		}
		require.Equal(t, 1, applied)
		return nil
	}))

	// The endorser transaction's produced output is a live UTxO stamped at the
	// ranking block's slot (rollback-safe) and not marked spent.
	var utxo models.Utxo
	require.NoError(t, gdb.QueryRow(`
SELECT added_slot, deleted_slot FROM utxo WHERE tx_id = ?`,
		tx.Hash().Bytes(),
	).Scan(&utxo.AddedSlot, &utxo.DeletedSlot))
	require.Equal(t, rbPoint.Slot, utxo.AddedSlot)
	require.Equal(t, uint64(0), utxo.DeletedSlot)
}

// The Haskell-conformant path commits the endorser-block blob in its own blob
// transaction, which the shared batch transaction's snapshot predates. Reading
// an endorser-produced output back through that batch after the shared block
// cache has evicted the blob must therefore use a fresh snapshot, which only
// applyEndorserBlock's separate-commit mark enables.
func TestApplyEndorserBlockHaskellPathResolvesProducedUtxoAfterCacheEviction(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir:     t.TempDir(),
		CacheConfig: database.CborCacheConfig{BlockLRUEntries: 1},
	})
	require.NoError(t, err)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	rawTx, tx := leiosApplyTestTxWithOutput(t, 0x6c)
	require.NotEmpty(t, tx.Produced(), "test tx must produce an output")

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		_, _, err := ls.applyEndorserBlock(
			txn,
			leiosApplyTestRankingPoint(0x7a),
			1,
			430,
			leiosApplyTestEbHash(0x6d),
			[]cbor.RawMessage{rawTx},
		)
		if err != nil {
			return err
		}
		// A later block commit evicts the endorser block from the one-entry
		// shared cache, so the read below reaches the blob store.
		require.NoError(t, db.SetGenesisCbor(
			431,
			leiosApplyTestEbHash(0x6e),
			[]byte{0x01},
			nil,
		))
		got, err := db.CborCache().ResolveUtxoCbor(tx.Hash().Bytes(), 0, txn)
		require.NoError(t, err)
		require.Equal(t, tx.Produced()[0].Output.Cbor(), got)
		return nil
	}))
}

func TestApplyEndorserBlockHaskellPathDeduplicatesMetadata(t *testing.T) {
	t.Parallel()

	ls, db, gdb := newLeiosApplyTestLedger(t)
	rawTx, bodyCbor, tx := leiosApplyTestTx(t, 0x07)
	firstPoint := leiosApplyTestRankingPoint(0x91)
	replayPoint := leiosApplyTestRankingPoint(0x93)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		_, _, err := ls.applyEndorserBlock(
			txn,
			firstPoint,
			1,
			600,
			leiosApplyTestEbHash(0x92),
			[]cbor.RawMessage{rawTx, rawTx},
		)
		return err
	}))

	txn = db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		_, _, err := ls.applyEndorserBlock(
			txn,
			replayPoint,
			2,
			601,
			leiosApplyTestEbHash(0x94),
			[]cbor.RawMessage{rawTx},
		)
		return err
	}))

	requireLeiosApplyTestTxCount(t, gdb, 1)
	var gotTx models.Transaction
	require.NoError(t, gdb.QueryRow(`
SELECT slot, block_index FROM "transaction" WHERE hash = ?`,
		tx.Hash().Bytes(),
	).Scan(&gotTx.Slot, &gotTx.BlockIndex))
	require.Equal(t, firstPoint.Slot, gotTx.Slot)
	require.Equal(t, uint32(0), gotTx.BlockIndex)
	requireLeiosApplyTestEndorserBlob(
		t,
		db,
		600,
		leiosApplyTestEbHash(0x92),
		append(leiosApplyTestBlob(bodyCbor, tx), leiosApplyTestBlob(bodyCbor, tx)...),
	)
	requireLeiosApplyTestEndorserBlob(
		t,
		db,
		601,
		leiosApplyTestEbHash(0x94),
		leiosApplyTestBlob(bodyCbor, tx),
	)
}

// leiosTestHash returns a distinct 32-byte hash whose bytes are all b, usable
// as both a map key (string form) and an endorser-block hash.
func leiosTestHash(b byte) []byte {
	return bytes.Repeat([]byte{b}, lcommon.Blake2b256Size)
}

func leiosTestRaw(t *testing.T, value any) cbor.RawMessage {
	t.Helper()
	raw, err := cbor.Encode(value)
	require.NoError(t, err)
	return cbor.RawMessage(raw)
}

func leiosTestCertifiedBlockPair(
	t *testing.T,
) (*dijkstra.DijkstraBlock, *dijkstra.DijkstraBlock, lcommon.Blake2b256) {
	t.Helper()
	ebHash := lcommon.NewBlake2b256(leiosTestHash(0xE1))
	parent := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 1,
					Slot:        100,
				},
			},
			LeiosHeaderExtension: []cbor.RawMessage{
				leiosTestRaw(t, false),
				leiosTestRaw(t, []any{ebHash.Bytes(), uint64(4096)}),
			},
		},
	}
	certifier := &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: 2,
					Slot:        140,
					PrevHash:    parent.Hash(),
				},
			},
			LeiosHeaderExtension: []cbor.RawMessage{
				leiosTestRaw(t, true),
				{0xf6},
			},
		},
	}
	return parent, certifier, ebHash
}

func leiosTestEnableCertifiedBlock(
	t *testing.T,
	ls *LedgerState,
	certifier *dijkstra.DijkstraBlock,
) {
	t.Helper()
	certifier.BlockBody.LeiosCertificate = &dijkstra.DijkstraLeiosCertificate{
		Signers:             []byte{0x80},
		AggregatedSignature: make([]byte, 48),
	}
	ls.config.ValidateLeiosCertificate = func(uint64, []byte, []byte, []byte) error {
		return nil
	}
	ls.consensus.Store(&consensusSnapshot{
		epochCache: []models.Epoch{{
			EpochId:       5,
			StartSlot:     0,
			LengthInSlots: 200,
		}},
	})
}

func TestEnsureReferencedEndorserBlocksRequiresCertifiedMusashiClosure(
	t *testing.T,
) {
	t.Parallel()

	parent, certifier, ebHash := leiosTestCertifiedBlockPair(t)
	available := false
	ls := &LedgerState{
		config: LedgerStateConfig{
			EndorserBlockProvider: func(
				hash []byte,
				_ uint64,
			) ([]cbor.RawMessage, bool) {
				return nil, available && bytes.Equal(hash, ebHash.Bytes())
			},
			// A zero wait disables best-effort announcement waiting. It must
			// not disable the certified-closure consistency check.
			EndorserBlockWaitSlots: 0,
		},
	}
	leiosTestEnableCertifiedBlock(t, ls, certifier)

	err := ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{parent, certifier},
	)
	require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)

	available = true
	require.NoError(t, ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{parent, certifier},
	))

	ls.config.EndorserBlockProvider = nil
	err = ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{parent, certifier},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
	require.Contains(t, err.Error(), "no endorser block provider configured")
}

// TestEnsureReferencedEndorserBlocksRejectsProviderResultAtWrongSlot is the
// P1 regression from review, adapted for EndorserBlockProviderFunc's slot
// parameter: ensureReferencedEndorserBlocks (via endorserBlockAvailableAt)
// must pass the certified reference's own required slot to the provider, not
// some other slot, so the provider resolves exactly the (slot, hash)
// occurrence the reference needs rather than whichever one happens to be
// cached for the hash. The manifest is content-addressed, so the same hash
// can legitimately be a distinct occurrence at another slot; a provider that
// only holds that other occurrence must correctly report unavailable when
// asked about this one.
func TestEnsureReferencedEndorserBlocksRejectsProviderResultAtWrongSlot(
	t *testing.T,
) {
	t.Parallel()

	parent, certifier, ebHash := leiosTestCertifiedBlockPair(t)
	ls := &LedgerState{
		config: LedgerStateConfig{
			EndorserBlockProvider: func(
				hash []byte,
				slot uint64,
			) ([]cbor.RawMessage, bool) {
				// Only holds a different occurrence of this hash, at a slot
				// other than the one the certified reference
				// (parent.SlotNumber(), 100) actually requires.
				if !bytes.Equal(hash, ebHash.Bytes()) ||
					slot != parent.SlotNumber()+1 {
					return nil, false
				}
				return nil, true
			},
			EndorserBlockWaitSlots: 0,
		},
	}
	leiosTestEnableCertifiedBlock(t, ls, certifier)

	err := ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{parent, certifier},
	)
	require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
}

func TestEnsureReferencedEndorserBlocksKeepsCIPAnnouncementsBestEffort(
	t *testing.T,
) {
	t.Parallel()

	parent, certifier, _ := leiosTestCertifiedBlockPair(t)
	ls := &LedgerState{
		config: LedgerStateConfig{
			EndorserBlockProvider: func(
				[]byte,
				uint64,
			) ([]cbor.RawMessage, bool) {
				return nil, false
			},
			EndorserBlockWaitSlots:     0,
			LeiosApplyEndorserBlockTxs: true,
		},
	}
	leiosTestEnableCertifiedBlock(t, ls, certifier)

	require.NoError(t, ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{parent, certifier},
	))
}

func TestEnsureReferencedEndorserBlocksRejectsUnresolvedCertifyingParent(
	t *testing.T,
) {
	t.Parallel()

	_, certifier, _ := leiosTestCertifiedBlockPair(t)
	ls := &LedgerState{
		config: LedgerStateConfig{
			EndorserBlockProvider: func(
				[]byte,
				uint64,
			) ([]cbor.RawMessage, bool) {
				return nil, false
			},
		},
	}
	leiosTestEnableCertifiedBlock(t, ls, certifier)

	err := ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{certifier},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
	require.Contains(t, err.Error(), "resolve certified parent announcement")
}

// TestLeiosBackfillerSpawnDedupsByHashAndSlotIndependently is the concurrency
// regression from review: the manifest is content-addressed, so the same
// hash can legitimately be required at two different slots at once. Deduping
// in-flight fetches by hash alone let a still-in-flight fetch for one slot
// silently suppress spawn for a different slot of the same hash; awaitFetch's
// "not in flight" skip-fast then fired the moment the *first* slot's fetch
// cleared the shared key, leaving the second slot's requirement never fetched
// at all.
func TestLeiosBackfillerSpawnDedupsByHashAndSlotIndependently(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var calls []uint64
	release := make(chan struct{})

	hash := lcommon.NewBlake2b256(leiosTestHash(0xAB))
	b := &leiosBackfiller{
		fetch: func(_ context.Context, slot uint64, _ []byte) error {
			mu.Lock()
			calls = append(calls, slot)
			mu.Unlock()
			<-release
			return nil
		},
		provider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		sem:    make(chan struct{}, leiosBackfillConcurrency),
	}
	defer close(release)

	callCount := func() int {
		mu.Lock()
		defer mu.Unlock()
		return len(calls)
	}

	// Slot 100's fetch starts and blocks inside fetch (simulating a live
	// in-flight network request).
	b.spawn(context.Background(), leiosEbRef{slot: 100, hash: hash})
	require.Eventually(
		t,
		func() bool { return callCount() == 1 },
		testutil.AsyncWait,
		time.Millisecond,
	)

	// Slot 200 requires the same hash while slot 100's fetch is still in
	// flight. It must be dispatched independently, not suppressed.
	b.spawn(context.Background(), leiosEbRef{slot: 200, hash: hash})
	require.Eventually(
		t,
		func() bool { return callCount() == 2 },
		testutil.AsyncWait,
		time.Millisecond,
	)

	mu.Lock()
	require.ElementsMatch(t, []uint64{100, 200}, calls)
	mu.Unlock()
}

// TestLeiosBackfillerFetchOnceDedupsWithSpawnInFlight verifies that the
// mandatory retry path observes the best-effort fetch marker for the same
// (slot, hash) reference instead of starting a redundant fetch.
func TestLeiosBackfillerFetchOnceDedupsWithSpawnInFlight(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	calls := 0
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	fetchDone := make(chan struct{})

	hash := lcommon.NewBlake2b256(leiosTestHash(0xEE))
	r := leiosEbRef{slot: 100, hash: hash}
	b := &leiosBackfiller{
		fetch: func(_ context.Context, _ uint64, _ []byte) error {
			mu.Lock()
			calls++
			mu.Unlock()
			started <- struct{}{}
			<-release
			return nil
		},
		provider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		sem:    make(chan struct{}, leiosBackfillConcurrency),
	}

	b.spawn(t.Context(), r)
	testutil.RequireReceive(
		t,
		started,
		testutil.AsyncWait,
		"spawn fetch never started",
	)
	go func() {
		defer close(fetchDone)
		_ = b.fetchOnce(t.Context(), r, time.Millisecond)
	}()
	testutil.RequireNoReceive(
		t,
		started,
		200*time.Millisecond,
		"fetchOnce started a redundant fetch for spawn's in-flight reference",
	)

	mu.Lock()
	require.Equal(t, 1, calls)
	mu.Unlock()
	close(release)
	testutil.RequireReceive(
		t,
		fetchDone,
		testutil.AsyncWait,
		"fetchOnce did not finish",
	)
}

// TestLeiosBackfillerAwaitFetchDoesNotSkipFastOnDifferentSlotCompletion is the
// companion regression targeting awaitFetch directly: with both slots' fetches
// genuinely in flight at once, slot 100 finishing (and clearing its own
// in-flight marker) must not make awaitFetch for slot 200 -- a different
// reference to the same hash -- skip-fast and report completion before slot
// 200's own fetch has actually finished.
func TestLeiosBackfillerAwaitFetchDoesNotSkipFastOnDifferentSlotCompletion(
	t *testing.T,
) {
	t.Parallel()

	var mu sync.Mutex
	completed := map[uint64]bool{}
	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	hash := lcommon.NewBlake2b256(leiosTestHash(0xCD))

	b := &leiosBackfiller{
		fetch: func(_ context.Context, slot uint64, _ []byte) error {
			switch slot {
			case 100:
				<-releaseA
			case 200:
				<-releaseB
			}
			mu.Lock()
			completed[slot] = true
			mu.Unlock()
			return nil
		},
		provider: func(_ []byte, slot uint64) ([]cbor.RawMessage, bool) {
			mu.Lock()
			defer mu.Unlock()
			if completed[slot] {
				return nil, true
			}
			return nil, false
		},
		logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		sem:    make(chan struct{}, leiosBackfillConcurrency),
	}

	// Both slots' fetches are genuinely in flight at once.
	b.spawn(context.Background(), leiosEbRef{slot: 100, hash: hash})
	b.spawn(context.Background(), leiosEbRef{slot: 200, hash: hash})

	// Slot 100 finishes first, while slot 200 is still in flight.
	close(releaseA)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return completed[100]
	}, testutil.AsyncWait, time.Millisecond)

	done := make(chan struct{})
	go func() {
		b.awaitFetch(
			t.Context(),
			leiosEbRef{slot: 200, hash: hash},
			time.Millisecond,
			leiosBackfillMaxWait,
		)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal(
			"awaitFetch for slot 200 returned before slot 200 actually completed",
		)
	case <-time.After(150 * time.Millisecond):
		// Still correctly waiting on slot 200's own fetch.
	}

	close(releaseB)
	select {
	case <-done:
	case <-time.After(testutil.AsyncWait):
		t.Fatal("awaitFetch for slot 200 did not return after it completed")
	}
}

// TestClassifyEndorserBlockFetchesKeepsDistinctSlotsOfSameHash is the
// companion regression to TestRequiredCertifiedEndorserBlocksKeepsDistinctSlots
// for classifyEndorserBlockFetches: two historical blocks announcing the
// same hash at different slots must both reach backfill, not collapse to
// one via the hash-only seen-map dedup.
func TestClassifyEndorserBlockFetchesKeepsDistinctSlotsOfSameHash(
	t *testing.T,
) {
	t.Parallel()

	sameHash := lcommon.NewBlake2b256(leiosTestHash(0xFE))
	hashX := leiosTestHash(0x11)
	hashY := leiosTestHash(0x22)
	infos := []leiosBlockInfo{
		{hash: string(hashX), slot: 100, announces: true, ebHash: sameHash},
		{hash: string(hashY), slot: 200, announces: true, ebHash: sameHash},
	}
	neverCached := func(leiosEbRef) bool { return false }

	// wallSlot 100_050 with waitSlots 100 puts both well into settled
	// backlog; certDrivenHistorical=false (CIP path) fetches every
	// referenced historical endorser block.
	backfill, tipWait := classifyEndorserBlockFetches(
		infos, nil, 100_050, true, 100, false, neverCached,
	)
	require.Empty(t, tipWait)
	require.ElementsMatch(t, []leiosEbRef{
		{slot: 100, hash: sameHash},
		{slot: 200, hash: sameHash},
	}, backfill)
}

// TestClassifyEndorserBlockFetches verifies the fetch policy: near the head,
// current announcements and certified parent announcements are both fetched;
// in the settled backlog only certified parent announcements are fetched and
// uncertified historical announcements are skipped.
func TestClassifyEndorserBlockFetches(t *testing.T) {
	t.Parallel()

	var (
		hashA = leiosTestHash(0xA1) // announces ebA (historical)
		hashC = leiosTestHash(0xC1) // certifies, parent = A (historical)
		hashD = leiosTestHash(0xD1) // announces ebB (near head)
		hashE = leiosTestHash(0xE1) // announces ebE (historical, uncertified)
		ebA   = lcommon.NewBlake2b256(leiosTestHash(0x0A))
		ebB   = lcommon.NewBlake2b256(leiosTestHash(0x0B))
		ebE   = lcommon.NewBlake2b256(leiosTestHash(0x0E))
	)
	infos := []leiosBlockInfo{
		{hash: string(hashA), slot: 100, announces: true, ebHash: ebA},
		{
			hash:      string(hashC),
			prevHash:  string(hashA),
			slot:      140,
			certifies: true,
		},
		{hash: string(hashD), slot: 100_000, announces: true, ebHash: ebB},
		{hash: string(hashE), slot: 200, announces: true, ebHash: ebE},
	}
	annByHash := map[string]leiosEbRef{
		string(hashA): {slot: 100, hash: ebA},
		string(hashD): {slot: 100_000, hash: ebB},
		string(hashE): {slot: 200, hash: ebE},
	}
	neverCached := func(leiosEbRef) bool { return false }

	// Haskell/cert-driven path. wallSlot 100050, waitSlots 100: slots 100/140/200
	// are settled backlog, slot 100000 is within the head window.
	backfill, tipWait := classifyEndorserBlockFetches(
		infos, annByHash, 100_050, true, 100, true, neverCached,
	)
	// Only the certified endorser block (ebA, via CertRB C's parent A) is
	// backfilled; the uncertified historical announcement (ebE) is skipped.
	require.Len(t, backfill, 1)
	require.Equal(t, ebA, backfill[0].hash)
	require.Equal(t, uint64(100), backfill[0].slot)
	// Only the near-head announcement (ebB) is fetched on announcement.
	require.Len(t, tipWait, 1)
	require.Equal(t, ebB, tipWait[0].hash)

	// prototype-2026w29 permits one near-head block to certify its parent's EB
	// and announce a new EB. Both references must be available, but only the
	// parent's EB is applied by the certifying block.
	nearParentHash := leiosTestHash(0xF1)
	nearCombinedHash := leiosTestHash(0xF2)
	nearParentEb := lcommon.NewBlake2b256(leiosTestHash(0x1F))
	nearCurrentEb := lcommon.NewBlake2b256(leiosTestHash(0x2F))
	backfill, tipWait = classifyEndorserBlockFetches(
		[]leiosBlockInfo{
			{
				hash:      string(nearParentHash),
				slot:      100_000,
				announces: true,
				ebHash:    nearParentEb,
			},
			{
				hash: string(
					nearCombinedHash,
				), prevHash: string(nearParentHash),
				slot: 100_001, announces: true, ebHash: nearCurrentEb, certifies: true,
			},
		},
		map[string]leiosEbRef{
			string(nearParentHash): {slot: 100_000, hash: nearParentEb},
		},
		100_050, true, 100, true, neverCached,
	)
	require.Empty(t, backfill)
	require.Len(t, tipWait, 2)
	require.ElementsMatch(
		t,
		[]lcommon.Blake2b256{nearParentEb, nearCurrentEb},
		[]lcommon.Blake2b256{tipWait[0].hash, tipWait[1].hash},
	)

	// A cached endorser block is not refetched.
	backfill, _ = classifyEndorserBlockFetches(
		infos, annByHash, 100_050, true, 100, true,
		func(r leiosEbRef) bool { return r.hash == ebA },
	)
	require.Empty(t, backfill)

	// CIP path (certDrivenHistorical=false): the settled backlog is
	// announcement-driven, so every referenced historical endorser block is
	// backfilled (ebA and ebE), not just certified ones, and the near-head
	// announcement (ebB) still goes to tipWait.
	backfill, tipWait = classifyEndorserBlockFetches(
		infos, annByHash, 100_050, true, 100, false, neverCached,
	)
	backfillHashes := make([]lcommon.Blake2b256, 0, len(backfill))
	for _, ref := range backfill {
		backfillHashes = append(backfillHashes, ref.hash)
	}
	require.ElementsMatch(
		t,
		[]lcommon.Blake2b256{ebA, ebE},
		backfillHashes,
	)
	require.Len(t, tipWait, 1)
	require.Equal(t, ebB, tipWait[0].hash)

	// With an unknown wall-clock slot every block is treated as near-head, so
	// all announcements fetch on announcement and none go to backfill.
	backfill, tipWait = classifyEndorserBlockFetches(
		infos, annByHash, 0, false, 100, true, neverCached,
	)
	require.Empty(t, backfill)
	require.Len(t, tipWait, 3) // ebA, ebB, ebE (all announcements)
}

func TestRequiredCertifiedEndorserBlocksKeepsDistinctSlots(t *testing.T) {
	t.Parallel()

	parentA := leiosTestHash(0xA2)
	parentB := leiosTestHash(0xB2)
	sharedHash := lcommon.NewBlake2b256(leiosTestHash(0xC2))
	required, err := requiredCertifiedEndorserBlocks(
		[]leiosBlockInfo{
			{prevHash: string(parentA), slot: 100, certifies: true},
			{prevHash: string(parentB), slot: 200, certifies: true},
		},
		map[string]leiosEbRef{
			string(parentA): {slot: 100, hash: sharedHash},
			string(parentB): {slot: 200, hash: sharedHash},
		},
		true,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, []leiosEbRef{
		{slot: 100, hash: sharedHash},
		{slot: 200, hash: sharedHash},
	}, required)
}

// leiosWaitTestSlotLen is the Shelley slot length these tests pin, so the
// slot-denominated diffusion window (EndorserBlockWaitSlots) converts to a
// wall-clock window short enough to assert on but long enough to separate
// "returned immediately" from "waited a window" without flaking.
const leiosWaitTestSlotLen = 20 * time.Millisecond

// leiosWaitTestWaitSlots matches the production default
// (CertifyByDeadlineSlots), so the window under test is
// leiosWaitTestWaitSlots * leiosWaitTestSlotLen = 400ms.
const leiosWaitTestWaitSlots = 20

const leiosWaitTestWindow = leiosWaitTestWaitSlots * leiosWaitTestSlotLen

// leiosWaitTestLongWaitSlots gives a 10s window, used by tests where the wait
// must be ended by something other than the deadline. It is far larger than
// any plausible scheduling delay on a loaded runner, so the deadline can never
// win the race against the event the test is actually exercising -- unlike a
// timer racing a short window, which is the classic flake in this file's
// shape. Nothing waits 10s: the wait ends as soon as that event fires.
const leiosWaitTestLongWaitSlots = 500

const leiosWaitTestLongWindow = leiosWaitTestLongWaitSlots * leiosWaitTestSlotLen

// withLeiosWaitTestSlotLength gives a bare-constructed LedgerState a Shelley
// slot length, which is what ensureReferencedEndorserBlocks converts the
// slot-denominated wait window with. Without it the wait is disabled outright
// and the timing assertions below would pass vacuously.
//
// It also pins the OTHER precondition every timing assertion in this file
// depends on: that the fixture's blocks are classified near-head rather than
// as settled backlog. classifyEndorserBlockFetches only calls a block
// historical when the wall-clock slot is KNOWN and more than
// EndorserBlockWaitSlots above it, and these fixtures deliberately leave
// ls.slotClock nil so CurrentSlot errors and wallKnown is false, which makes
// every block near-head no matter what slot the fixture uses. That is an
// invariant, not a coincidence: give one of these states a slot clock reading
// past the fixture slots and the near-head path stops being exercised --
// the shared-window test would skip the wait entirely (ls.leiosBackfill is
// nil) and the late-arrival test would reroute its closure to fetchRequired,
// so both would pass or fail for reasons unrelated to what they assert.
// Assert it here, once, where the fixture is established.
func withLeiosWaitTestSlotLength(t *testing.T, ls *LedgerState) {
	t.Helper()
	if _, err := ls.CurrentSlot(); err == nil {
		t.Fatal(
			"these tests require an unknown wall-clock slot so every " +
				"fixture block is classified near-head; a slot clock has " +
				"been added, so pin the fixture slots relative to it before " +
				"trusting any wait assertion in this file",
		)
	}
	ls.timeConverter = NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis: func() *shelley.ShelleyGenesis {
			return &shelley.ShelleyGenesis{
				SlotLength: lcommon.GenesisRat{
					Rat: big.NewRat(
						int64(leiosWaitTestSlotLen/time.Millisecond),
						1000,
					),
				},
			}
		},
	})
	ls.timeConverterOnce.Do(func() {})
}

// leiosWaitTestAnnouncingBlock builds a Dijkstra ranking block that announces
// ebHash and certifies nothing.
func leiosWaitTestAnnouncingBlock(
	t *testing.T,
	blockNumber, slot uint64,
	ebHash lcommon.Blake2b256,
) *dijkstra.DijkstraBlock {
	t.Helper()
	return &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: blockNumber,
					Slot:        slot,
				},
			},
			LeiosHeaderExtension: []cbor.RawMessage{
				leiosTestRaw(t, false),
				leiosTestRaw(t, []any{ebHash.Bytes(), uint64(4096)}),
			},
		},
	}
}

// TestEnsureReferencedEndorserBlocksDoesNotBlockOnUnreadAnnouncement is the
// apply-lag regression. On the Haskell-conformant (Musashi) path, ledger
// application of a ranking block reads only the certified closure announced by
// a certifying block's PARENT; a block's own announcement is never read when
// that block is applied. Blocking the single ledger pipeline on it stalled
// every block queued behind it for a whole diffusion window and then applied
// the block unchanged anyway, which is where the multi-second apply lag and
// the resulting stale-tip forge came from.
//
// Before the fix this returns after the full window; after it, immediately.
func TestEnsureReferencedEndorserBlocksDoesNotBlockOnUnreadAnnouncement(
	t *testing.T,
) {
	ebHash := lcommon.NewBlake2b256(leiosTestHash(0xA1))
	block := leiosWaitTestAnnouncingBlock(t, 1, 100, ebHash)

	var fetched, waitPolls atomic.Int64
	fetchedCh := make(chan struct{}, 1)
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			// Counting polls that come from INSIDE waitForEndorserBlock is
			// what makes this test's verdict event-driven: a gate that
			// blocked on this announcement must poll from there, and one
			// that correctly skips it never can.
			if leiosWaitTestPolledFromWait() {
				waitPolls.Add(1)
			}
			// The endorser block never arrives.
			return nil, false
		},
		EndorserBlockFetcher: func(
			_ context.Context,
			_ uint64,
			_ []byte,
		) error {
			fetched.Add(1)
			select {
			case fetchedCh <- struct{}{}:
			default:
			}
			return nil
		},
		EndorserBlockWaitSlots: leiosWaitTestWaitSlots,
		// Haskell-conformant path: application reads only certified closures.
		LeiosApplyEndorserBlockTxs: false,
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)
	withLeiosWaitTestSlotLength(t, ls)
	require.Equal(t, leiosWaitTestSlotLen, ls.shelleySlotLength())

	start := time.Now()
	require.NoError(t, ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{block},
	))
	elapsed := time.Since(start)
	// The load-bearing assertion is event-driven, not a stopwatch: if the gate
	// blocked on this announcement it would have polled from inside
	// waitForEndorserBlock. Asserting that directly cannot flake on a loaded
	// runner, whereas a short wall-clock budget has to cover goroutine
	// scheduling as well as the absence of a window.
	require.Zero(
		t,
		waitPolls.Load(),
		"apply gate blocked on an announcement ledger application never reads",
	)
	// Clock kept only as a loose backstop against a full window being spent
	// somewhere the poll counter cannot see. Generous on purpose.
	require.Less(
		t,
		elapsed,
		leiosWaitTestWindow,
		"apply gate spent a whole diffusion window",
	)

	// The announcement is prefetched in the background rather than dropped, so
	// it is cached before anything actually depends on it.
	select {
	case <-fetchedCh:
	case <-time.After(testutil.AsyncWait):
		t.Fatal("background by-point fetch was never dispatched")
	}
	require.Positive(t, fetched.Load())
}

// leiosWaitTestPolledFromWait reports whether the current call stack passes
// through waitForEndorserBlock, i.e. whether the endorser-block provider is
// being polled by the WAIT itself rather than by one of the gate's
// availability checks that run before the wait starts.
//
// A test that instead counts provider calls and flips availability on the Nth
// one is silently coupled to how many pre-wait checks the gate happens to
// make. If that count ever grows past the threshold, the endorser block
// becomes available before the wait is entered, the reference is treated as
// already cached, no wait happens at all -- and the test still passes, having
// stopped testing the thing it names. Keying on the caller instead makes the
// phase explicit and immune to that drift.
func leiosWaitTestPolledFromWait() bool {
	pcs := make([]uintptr, 64)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, "waitForEndorserBlock") {
			return true
		}
		if !more {
			return false
		}
	}
}

// TestEnsureReferencedEndorserBlocksWaitsForCertifiedClosureArrivingLate pins
// the other half of the contract: a certifying ranking block's closure IS read
// at apply time and committing without it would permanently omit the endorser
// block's effects, so that wait is load-bearing and is kept. An endorser block
// that lands part-way through the window must be picked up, not skipped.
//
// The endorser block is unavailable to every check the gate makes before the
// wait, and becomes available only on the wait's own Nth poll. That makes the
// arrival late by construction -- it cannot be satisfied by a pre-wait check,
// however many of those there are -- and it is driven by the wait's progress
// rather than the wall clock, so it cannot lose a race with the deadline on a
// loaded runner. The test then asserts that the arrival really was observed
// inside the wait, which is what stops it passing vacuously if the wait is
// skipped.
func TestEnsureReferencedEndorserBlocksWaitsForCertifiedClosureArrivingLate(
	t *testing.T,
) {
	parent, certifier, ebHash := leiosTestCertifiedBlockPair(t)
	const arrivalWaitPoll = 3
	var preWaitPolls, waitPolls atomic.Int64
	var arrivedInsideWait atomic.Bool
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func(
			hash []byte,
			slot uint64,
		) ([]cbor.RawMessage, bool) {
			if slot != parent.SlotNumber() {
				return nil, false
			}
			if string(hash) != string(ebHash.Bytes()) {
				return nil, false
			}
			// Once it has arrived it stays available to every caller, as a
			// real endorser block landing in the cache would -- including the
			// gate's mandatory-closure checks, which run after the wait and
			// outside its call stack.
			if arrivedInsideWait.Load() {
				return nil, true
			}
			if !leiosWaitTestPolledFromWait() {
				// Every check before the wait sees it missing, so the wait is
				// always entered and the arrival is always "late".
				preWaitPolls.Add(1)
				return nil, false
			}
			if waitPolls.Add(1) < arrivalWaitPoll {
				return nil, false
			}
			arrivedInsideWait.Store(true)
			return nil, true
		},
		EndorserBlockWaitSlots:     leiosWaitTestLongWaitSlots,
		LeiosApplyEndorserBlockTxs: false,
	}
	ls := &LedgerState{config: cfg}
	leiosTestEnableCertifiedBlock(t, ls, certifier)
	withLeiosWaitTestSlotLength(t, ls)

	start := time.Now()
	require.NoError(t, ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{parent, certifier},
	))
	require.True(
		t,
		arrivedInsideWait.Load(),
		"the closure must have been picked up by the wait; if it was never "+
			"polled from inside waitForEndorserBlock the gate did not wait "+
			"at all and this test would otherwise pass vacuously",
	)
	require.GreaterOrEqual(
		t,
		waitPolls.Load(),
		int64(arrivalWaitPoll),
		"mandatory certified closure must be waited for, not skipped",
	)
	require.Positive(
		t,
		preWaitPolls.Load(),
		"the gate is expected to check availability before waiting; if it "+
			"stops doing so this test's phase split needs revisiting",
	)
	require.Less(t, time.Since(start), leiosWaitTestLongWindow)
}

// TestEnsureReferencedEndorserBlocksSharesOneWindowAcrossMissingBlocks is the
// serial-stacking regression. The per-endorser-block waits are independent --
// none observes another's result -- so running them back to back charged the
// ledger pipeline one full diffusion window per missing endorser block. A
// batch referencing k missing endorser blocks cost k windows, which is the
// long tail of the measured apply stalls. They must share one window.
//
// The CIP-conformant path is used because every reference there is read at
// apply time, so all three stay blocking and only the concurrency changes.
func TestEnsureReferencedEndorserBlocksSharesOneWindowAcrossMissingBlocks(
	t *testing.T,
) {
	const missing = 3
	blocks := make([]gledger.Block, 0, missing)
	for i := range missing {
		blocks = append(blocks, leiosWaitTestAnnouncingBlock(
			t,
			uint64(i+1),
			uint64(100+i),
			lcommon.NewBlake2b256(leiosTestHash(byte(0xB0+i))),
		))
	}
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		EndorserBlockWaitSlots: leiosWaitTestWaitSlots,
		// CIP-conformant path: every announcement is read at apply time.
		LeiosApplyEndorserBlockTxs: true,
	}
	ls := &LedgerState{config: cfg}
	withLeiosWaitTestSlotLength(t, ls)

	start := time.Now()
	require.NoError(t, ls.ensureReferencedEndorserBlocks(
		t.Context(),
		blocks,
	))
	elapsed := time.Since(start)
	require.GreaterOrEqual(
		t,
		elapsed,
		leiosWaitTestWindow,
		"the window must still be honoured for references application reads",
	)
	require.Less(
		t,
		elapsed,
		2*leiosWaitTestWindow,
		"per-endorser-block waits stacked serially instead of sharing a window",
	)
}

// TestSplitTipWaitByApplyDependency pins the apply-path contract itself,
// independently of timing: on the CIP path every reference is read at apply
// time and stays blocking; on the Musashi path only the mandatory certified
// closures are read, and a block's own announcement is demoted to background
// prefetch.
func TestSplitTipWaitByApplyDependency(t *testing.T) {
	certified := leiosEbRef{
		slot: 100,
		hash: lcommon.NewBlake2b256(leiosTestHash(0xC1)),
	}
	announced := leiosEbRef{
		slot: 140,
		hash: lcommon.NewBlake2b256(leiosTestHash(0xC2)),
	}
	tipWait := []leiosEbRef{certified, announced}

	blocking, prefetch := splitTipWaitByApplyDependency(
		tipWait,
		[]leiosEbRef{certified},
		true,
	)
	require.Equal(t, []leiosEbRef{certified}, blocking)
	require.Equal(t, []leiosEbRef{announced}, prefetch)

	blocking, prefetch = splitTipWaitByApplyDependency(tipWait, nil, false)
	require.Equal(t, tipWait, blocking)
	require.Empty(t, prefetch)
}

// TestAwaitEndorserBlocksFetchesUpFront pins the second half of the wait fix:
// a reference the batch does block on has its by-point fetch dispatched up
// front, concurrently with the wait, instead of the wait polling passively and
// only falling back to a fetch once the whole diffusion window had already been
// spent. It also pins that an already-available reference costs neither a wait
// nor a fetch.
func TestAwaitEndorserBlocksFetchesUpFront(t *testing.T) {
	var cached atomic.Bool
	var fetches atomic.Int64
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, cached.Load()
		},
		EndorserBlockFetcher: func(
			_ context.Context,
			_ uint64,
			_ []byte,
		) error {
			fetches.Add(1)
			// The fetch is what makes the endorser block available; nothing
			// else will deliver it during this test.
			cached.Store(true)
			return nil
		},
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)
	ref := leiosEbRef{
		slot: 100,
		hash: lcommon.NewBlake2b256(leiosTestHash(0xD1)),
	}

	start := time.Now()
	ls.awaitEndorserBlocks(
		t.Context(),
		[]leiosEbRef{ref},
		leiosWaitTestWindow,
		time.Millisecond,
	)
	// The fetch count is the event that matters: it proves the by-point fetch
	// was dispatched up front rather than after the window. Elapsed time is a
	// loose backstop only, so a saturated scheduler cannot fail a correct wait.
	require.Equal(
		t,
		int64(1),
		fetches.Load(),
		"wait did not dispatch the by-point fetch",
	)
	require.Less(
		t,
		time.Since(start),
		leiosWaitTestWindow,
		"wait did not dispatch the by-point fetch until the window expired",
	)

	// Already cached: no second fetch, no wait.
	start = time.Now()
	ls.awaitEndorserBlocks(
		t.Context(),
		[]leiosEbRef{ref},
		leiosWaitTestWindow,
		time.Millisecond,
	)
	require.Equal(
		t,
		int64(1),
		fetches.Load(),
		"an already-cached reference must not be fetched again",
	)
	require.Less(t, time.Since(start), leiosWaitTestWindow)
}

// leiosWaitTestHistogram returns the sample count of
// dingo_metrics_leios_eb_wait_seconds for the given outcome label.
func leiosWaitTestHistogram(
	t *testing.T,
	reg *prometheus.Registry,
	outcome string,
) uint64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "dingo_metrics_leios_eb_wait_seconds" {
			continue
		}
		for _, m := range family.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == "outcome" &&
					label.GetValue() == outcome {
					return m.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	t.Fatalf("no eb wait histogram series for outcome %q", outcome)
	return 0
}

// TestLeiosEbWaitMetricsRecordOutcomeAndDuration covers the observability gap:
// the apply-path endorser-block wait had no metric at all, only an Info log,
// so a producer sitting in it for tens of seconds per block showed nothing in
// monitoring. Both outcomes are recorded, and both label values exist before
// any wait happens so a dashboard is not looking at an absent series.
func TestLeiosEbWaitMetricsRecordOutcomeAndDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	var available atomic.Bool
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EndorserBlockProvider: func(
				[]byte,
				uint64,
			) ([]cbor.RawMessage, bool) {
				return nil, available.Load()
			},
		},
	}
	ls.metrics.init(reg)

	// Every outcome series is pre-materialized at init, before any wait.
	require.Equal(
		t,
		4,
		promtestutil.CollectAndCount(ls.metrics.leiosEbWaitSeconds),
	)
	require.Zero(t, leiosWaitTestHistogram(t, reg, "arrived"))
	require.Zero(t, leiosWaitTestHistogram(t, reg, "timeout"))
	require.Zero(t, leiosWaitTestHistogram(t, reg, "cancelled"))
	require.Zero(t, leiosWaitTestHistogram(t, reg, "unavailable"))
	require.Zero(t, promtestutil.ToFloat64(ls.metrics.leiosEbWaitTimeouts))

	ebHash := lcommon.NewBlake2b256(leiosTestHash(0xE7))

	// Expiry: recorded as a timeout, on both the histogram and the counter.
	ls.waitForEndorserBlock(
		t.Context(),
		100,
		ebHash,
		leiosWaitTestWindow,
		time.Millisecond,
	)
	require.Equal(t, uint64(1), leiosWaitTestHistogram(t, reg, "timeout"))
	require.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(ls.metrics.leiosEbWaitTimeouts),
	)
	require.Zero(t, leiosWaitTestHistogram(t, reg, "arrived"))

	// Arrival: recorded as arrived, and does not increment the timeout
	// counter. The endorser block is made available by the provider's own
	// second call rather than by a timer racing the deadline, and the window
	// is long enough that the deadline cannot fire first regardless of load.
	available.Store(true)
	ls.waitForEndorserBlock(
		t.Context(),
		100,
		ebHash,
		leiosWaitTestLongWindow,
		time.Millisecond,
	)
	require.Equal(t, uint64(1), leiosWaitTestHistogram(t, reg, "arrived"))
	require.Equal(t, uint64(1), leiosWaitTestHistogram(t, reg, "timeout"))
	require.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(ls.metrics.leiosEbWaitTimeouts),
	)
}

// TestLeiosEbWaitCancellationIsNotCountedAsTimeout covers the review finding:
// the wait's context is a timeout CHILD of the block-processing context, so its
// Done also closes when the parent is cancelled -- node shutdown, or the pass
// being aborted and restarted. That is not a diffusion-window expiry, and
// counting it as one inflates the timeout rate exactly when a node is shutting
// down or restarting its pipeline, which is when the metric is most likely to
// be read.
//
// Neither case uses a timer. Cancellation is either already in effect before
// the wait starts, or is driven by the wait's own polling, and the window is
// large enough that the deadline cannot win either race on a loaded runner.
// A timer firing at a fraction of a short window is precisely the flake this
// avoids: if it landed late the outcome would be "timeout" and the assertions
// below would fail for reasons that have nothing to do with the code.
func TestLeiosEbWaitCancellationIsNotCountedAsTimeout(t *testing.T) {
	// cancelWhen selects how the parent context is cancelled relative to the
	// wait: before it starts, or from inside the availability poll once the
	// wait is already running.
	for name, cancelOnPoll := range map[string]int64{
		"cancelled before the wait starts": 0,
		"cancelled during the wait":        3,
	} {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			var polls atomic.Int64
			ls := &LedgerState{
				config: LedgerStateConfig{
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
					EndorserBlockProvider: func(
						[]byte,
						uint64,
					) ([]cbor.RawMessage, bool) {
						if cancelOnPoll > 0 &&
							polls.Add(1) == cancelOnPoll {
							cancel()
						}
						// Never arrives, so only a cancellation or the
						// window can end the wait.
						return nil, false
					},
				},
			}
			ls.metrics.init(reg)

			// Every outcome series exists before any wait.
			require.Equal(
				t,
				4,
				promtestutil.CollectAndCount(ls.metrics.leiosEbWaitSeconds),
			)

			if cancelOnPoll == 0 {
				cancel()
			}

			start := time.Now()
			ls.waitForEndorserBlock(
				ctx,
				100,
				lcommon.NewBlake2b256(leiosTestHash(0xE8)),
				leiosWaitTestLongWindow,
				time.Millisecond,
			)
			require.Less(
				t,
				time.Since(start),
				leiosWaitTestLongWindow,
				"the wait must end on parent cancellation, not run out the window",
			)

			require.Equal(
				t,
				uint64(1),
				leiosWaitTestHistogram(t, reg, "cancelled"),
			)
			require.Zero(t, leiosWaitTestHistogram(t, reg, "timeout"))
			require.Zero(t, leiosWaitTestHistogram(t, reg, "arrived"))
			require.Zero(
				t,
				promtestutil.ToFloat64(ls.metrics.leiosEbWaitTimeouts),
				"a cancelled pass must not be counted as a diffusion-window timeout",
			)
		})
	}
}

// TestLeiosEbWaitCancellationLeavesCallerBehaviourUnchanged pins that the new
// classification is only a classification: a cancelled pass still runs the
// mandatory-closure check, so it still fails the chunk when a certified
// closure is missing rather than silently committing without it.
func TestLeiosEbWaitCancellationLeavesCallerBehaviourUnchanged(t *testing.T) {
	parent, certifier, _ := leiosTestCertifiedBlockPair(t)
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EndorserBlockProvider: func(
				[]byte,
				uint64,
			) ([]cbor.RawMessage, bool) {
				return nil, false
			},
			EndorserBlockWaitSlots:     leiosWaitTestWaitSlots,
			LeiosApplyEndorserBlockTxs: false,
		},
	}
	leiosTestEnableCertifiedBlock(t, ls, certifier)
	withLeiosWaitTestSlotLength(t, ls)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := ls.ensureReferencedEndorserBlocks(
		ctx,
		[]gledger.Block{parent, certifier},
	)
	require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
}

// leiosWaitTestPolledFromGrace reports whether the provider is being polled by
// the post-window fetch wait (awaitInFlightEndorserFetches).
//
// It matches awaitInFlightEndorserFetches itself, NOT awaitFetch. awaitFetch is
// shared: the certificate-driven path reaches it through fetchOnce's dedup
// wait, so keying on it would make a cert-driven test report a grace phase that
// never ran, depending on whether a fetch happened to be in flight. The waits
// are dispatched from a goroutine per reference, and a closure carries its
// enclosing function's name in the stack (…awaitInFlightEndorserFetches.func1),
// so the enclosing frame is still visible from inside the poll.
func leiosWaitTestPolledFromGrace() bool {
	pcs := make([]uintptr, 64)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if strings.Contains(
			frame.Function,
			"awaitInFlightEndorserFetches",
		) {
			return true
		}
		if !more {
			return false
		}
	}
}

// TestEnsureReferencedEndorserBlocksAwaitsLateFetchOnCIPPath covers the
// regression the review found in the CIP-conformant path. Application there
// reads each ranking block's own announcement and nothing re-applies an
// endorser block that lands afterwards, so if the by-point fetch is still in
// flight when the diffusion window elapses, the ranking block is applied
// without the endorser-resident outputs and its spends fall through to the
// interim trust path permanently.
//
// The previous code got this right by accident: it issued a SYNCHRONOUS
// by-point fetch after the window, so however slow the fetch was it still
// populated the cache before the batch reached ledgerProcessBlock. Dispatching
// the fetch up front and asynchronously is better for latency but dropped that
// guarantee. The grace phase restores it.
//
// The fetch is released by the grace phase's own first poll rather than by a
// timer, so "slower than the window, faster than the grace" holds by
// construction and cannot flake: until the grace phase runs, the fetch cannot
// complete, so it is always later than the window.
func TestEnsureReferencedEndorserBlocksAwaitsLateFetchOnCIPPath(t *testing.T) {
	ebHash := lcommon.NewBlake2b256(leiosTestHash(0xF1))
	block := leiosWaitTestAnnouncingBlock(t, 1, 100, ebHash)

	// gracePollsBeforeRelease is chosen so the fetch cannot complete until the
	// wait has polled for materially longer than the soft-warn window: the poll
	// interval is a tenth of a slot, so the window is worth about
	// leiosWaitTestWaitSlots*10 polls and this is comfortably beyond it. A wait
	// bounded BY that window -- which is what this test exists to reject --
	// gives up before reaching this count, deterministically and regardless of
	// machine load, because it is counting the wait's own polls rather than
	// racing a clock.
	const gracePollsBeforeRelease = leiosWaitTestWaitSlots * 20

	release := make(chan struct{})
	var releaseOnce sync.Once
	var gracePolls atomic.Int64
	var cached, sawGracePhase, fetchCompleted atomic.Bool

	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func(
			hash []byte,
			slot uint64,
		) ([]cbor.RawMessage, bool) {
			if slot != 100 || string(hash) != string(ebHash.Bytes()) {
				return nil, false
			}
			if leiosWaitTestPolledFromGrace() {
				// The diffusion window has elapsed and the post-window wait is
				// running. Hold the fetch for long enough that a wait bounded
				// by one further window would have abandoned it.
				sawGracePhase.Store(true)
				if gracePolls.Add(1) >= gracePollsBeforeRelease {
					releaseOnce.Do(func() { close(release) })
				}
			}
			return nil, cached.Load()
		},
		EndorserBlockFetcher: func(
			ctx context.Context,
			_ uint64,
			_ []byte,
		) error {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Second):
				// Backstop so a regression that never runs the grace phase
				// fails the assertions below instead of hanging.
				return nil
			}
			cached.Store(true)
			fetchCompleted.Store(true)
			return nil
		},
		// CIP-conformant path: application reads the announcement.
		EndorserBlockWaitSlots:     leiosWaitTestWaitSlots,
		LeiosApplyEndorserBlockTxs: true,
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)
	withLeiosWaitTestSlotLength(t, ls)

	require.NoError(t, ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{block},
	))

	require.True(
		t,
		sawGracePhase.Load(),
		"the post-window grace phase must run on the CIP path",
	)
	require.True(t, fetchCompleted.Load(), "the in-flight fetch must finish")
	require.GreaterOrEqual(
		t,
		gracePolls.Load(),
		int64(gracePollsBeforeRelease),
		"the wait must outlast a single further diffusion window",
	)
	require.True(
		t,
		endorserBlockAvailableAt(
			ls.config.EndorserBlockProvider,
			ebHash.Bytes(),
			100,
		),
		"the endorser block must be cached before the batch is applied; "+
			"otherwise the ranking block commits without its endorser-resident "+
			"outputs and nothing ever re-applies them",
	)
}

// TestEnsureReferencedEndorserBlocksSkipsGraceOnCertDrivenPath pins that the
// grace is CIP-only, exercised against a MANDATORY certified closure so the
// blocking set is non-empty and the guard is what decides. On this path a
// missing closure is already retried by the bounded fetch that follows, so
// paying a second diffusion window here would add head-of-line blocking on the
// pipeline for nothing.
func TestEnsureReferencedEndorserBlocksSkipsGraceOnCertDrivenPath(
	t *testing.T,
) {
	parent, certifier, _ := leiosTestCertifiedBlockPair(t)

	var sawGracePhase atomic.Bool
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			if leiosWaitTestPolledFromGrace() {
				sawGracePhase.Store(true)
			}
			return nil, false
		},
		EndorserBlockFetcher: func(
			_ context.Context,
			_ uint64,
			_ []byte,
		) error {
			return nil
		},
		EndorserBlockWaitSlots:     leiosWaitTestWaitSlots,
		LeiosApplyEndorserBlockTxs: false,
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)
	leiosTestEnableCertifiedBlock(t, ls, certifier)
	withLeiosWaitTestSlotLength(t, ls)

	// The closure never arrives, so the mandatory check fails -- which is the
	// correct outcome and is what makes the grace pointless here.
	err := ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{parent, certifier},
	)
	require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
	require.False(
		t,
		sawGracePhase.Load(),
		"the post-window grace must not run on the certificate-driven path",
	)
}

// TestEnsureReferencedEndorserBlocksProceedsWhenCIPFetchFindsNothing pins the
// other arm of the CIP wait: when the by-point fetch finishes without caching
// -- no connected peer holds the endorser block -- application proceeds without
// it rather than failing the chunk.
//
// This is deliberate and is NOT a behaviour this change introduces: it is the
// long-standing semantics of the CIP path. Failing the chunk instead would turn
// an unfetchable endorser block into an unbounded pipeline retry, which is a
// wedge this codebase has hit before. The wait exists to stop us abandoning a
// fetch that was about to succeed, not to convert a genuine absence into a
// stall.
func TestEnsureReferencedEndorserBlocksProceedsWhenCIPFetchFindsNothing(
	t *testing.T,
) {
	ebHash := lcommon.NewBlake2b256(leiosTestHash(0xF3))
	block := leiosWaitTestAnnouncingBlock(t, 1, 100, ebHash)

	var fetches atomic.Int64
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		EndorserBlockFetcher: func(
			_ context.Context,
			_ uint64,
			_ []byte,
		) error {
			// Finishes promptly, having found nothing.
			fetches.Add(1)
			return errors.New("no peer holds this endorser block")
		},
		EndorserBlockWaitSlots:     leiosWaitTestWaitSlots,
		LeiosApplyEndorserBlockTxs: true,
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)
	withLeiosWaitTestSlotLength(t, ls)

	start := time.Now()
	require.NoError(t, ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{block},
	))
	// One diffusion window, then the fetch's own prompt failure -- not the
	// hard backstop.
	require.Less(t, time.Since(start), leiosTipFetchHardBound)
	require.Positive(t, fetches.Load())
	require.False(t, endorserBlockAvailableAt(
		ls.config.EndorserBlockProvider,
		ebHash.Bytes(),
		100,
	))
}

// TestLeiosGraceDetectorIgnoresSharedAwaitFetch pins the property that makes
// the certificate-driven test above meaningful rather than timing-dependent.
//
// awaitFetch is shared: the certificate-driven path reaches it through
// fetchOnce's dedup wait whenever a spawned fetch for the same reference is
// still in flight. A grace detector keyed on awaitFetch therefore reports a
// post-window wait that never ran, but only when that race happens to occur --
// so the cert-driven test would pass or fail depending on scheduling. Keying on
// awaitInFlightEndorserFetches makes it positive and deterministic, and this
// asserts exactly that: reached through awaitFetch alone, the detector is
// false.
func TestLeiosGraceDetectorIgnoresSharedAwaitFetch(t *testing.T) {
	var sawGrace atomic.Bool
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			if leiosWaitTestPolledFromGrace() {
				sawGrace.Store(true)
			}
			return nil, false
		},
		EndorserBlockFetcher: func(
			_ context.Context,
			_ uint64,
			_ []byte,
		) error {
			return nil
		},
	}
	b := newLeiosBackfiller(cfg)
	require.NotNil(t, b)

	b.awaitFetch(
		t.Context(),
		leiosEbRef{
			slot: 100,
			hash: lcommon.NewBlake2b256(leiosTestHash(0xF4)),
		},
		time.Millisecond,
		20*time.Millisecond,
	)

	require.False(
		t,
		sawGrace.Load(),
		"awaitFetch alone must not be mistaken for the post-window wait",
	)
}

// leiosWaitTestLogBuffer is a concurrency-safe log sink. The waits under test
// dispatch a goroutine per reference and the backfiller logs from its own
// fetch goroutine, so a bare bytes.Buffer is written from several goroutines
// at once -- which the race detector correctly flags.
type leiosWaitTestLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *leiosWaitTestLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *leiosWaitTestLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestFetchRequiredReportsCancellationNotBudgetExpiry covers the second site
// with the same defect the diffusion wait had: the retry budget is a timeout
// CHILD of the block-processing context, so its Done also closes when the
// PARENT is cancelled -- node shutdown, or the pass being aborted. Reporting
// that as "the retry budget elapsed" tells an operator that peers failed to
// serve the endorser block when nothing was asked of them, and it is loudest
// exactly when a node is shutting down.
func TestFetchRequiredReportsCancellationNotBudgetExpiry(t *testing.T) {
	var logs leiosWaitTestLogBuffer
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		EndorserBlockFetcher: func(
			_ context.Context,
			_ uint64,
			_ []byte,
		) error {
			return errors.New("no peer holds it")
		},
	}
	b := newLeiosBackfiller(cfg)
	require.NotNil(t, b)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := b.fetchRequired(
		ctx,
		leiosEbRef{
			slot: 100,
			hash: lcommon.NewBlake2b256(leiosTestHash(0xC7)),
		},
		time.Millisecond,
	)
	require.Error(t, err)
	require.NotContains(
		t,
		logs.String(),
		"certified leios endorser block fetch budget elapsed",
		"a cancelled pass must not be reported as peers failing to serve",
	)
	require.Contains(
		t,
		logs.String(),
		"certified leios endorser block fetch cancelled",
	)
}

// TestCIPFetchWaitReportsCancellationNotFailure covers the third site. On the
// CIP path a fetch that finishes without caching is reported as "could not be
// fetched", which is a real diagnosis -- unless the pass was cancelled, in
// which case nothing was learned about whether any peer holds the block and
// the line would be a false diagnosis emitted on every shutdown.
func TestCIPFetchWaitReportsCancellationNotFailure(t *testing.T) {
	var logs leiosWaitTestLogBuffer
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		EndorserBlockFetcher: func(
			ctx context.Context,
			_ uint64,
			_ []byte,
		) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)

	ref := leiosEbRef{
		slot: 100,
		hash: lcommon.NewBlake2b256(leiosTestHash(0xC8)),
	}
	ctx, cancel := context.WithCancel(t.Context())
	// Dispatch the fetch, then cancel: the fetch is in flight and will end
	// only because of the cancellation.
	ls.leiosBackfill.spawn(ctx, ref)
	cancel()

	ls.awaitInFlightEndorserFetches(
		ctx,
		[]leiosEbRef{ref},
		leiosWaitTestWindow,
		time.Millisecond,
		leiosTipFetchHardBound,
	)

	require.NotContains(
		t,
		logs.String(),
		"endorser block could not be fetched",
		"a cancelled pass must not be reported as an unfetchable endorser block",
	)
	require.Contains(
		t,
		logs.String(),
		"endorser block fetch cancelled before it completed",
	)
}

// TestCIPFetchWaitDoesNotWarnOnRoutineUnfetchableEndorserBlock pins the log
// level of the CIP path's most common non-cached outcome.
//
// awaitFetch returns for two reasons that this wait cannot otherwise tell
// apart: the all-peers fetch cleared its in-flight marker without caching (no
// peer holds this endorser block), or it neither cached nor cleared before the
// hard bound. Only the second is anomalous. The first is the expected,
// long-standing behaviour of this path -- the code this replaced logged its
// equivalent at Debug -- and on a CIP node where endorser blocks are routinely
// unfetchable, a WARN per reference turns normal operation into alertable
// volume.
func TestCIPFetchWaitDoesNotWarnOnRoutineUnfetchableEndorserBlock(
	t *testing.T,
) {
	var logs leiosWaitTestLogBuffer
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		// Fails immediately, the way a sweep that finds no peer holding the
		// block does: the in-flight marker clears and awaitFetch returns
		// without the hard bound ever being approached.
		EndorserBlockFetcher: func(context.Context, uint64, []byte) error {
			return errors.New("no peer holds it")
		},
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)

	ref := leiosEbRef{
		slot: 100,
		hash: lcommon.NewBlake2b256(leiosTestHash(0xC9)),
	}
	ls.leiosBackfill.spawn(t.Context(), ref)

	ls.awaitInFlightEndorserFetches(
		t.Context(),
		[]leiosEbRef{ref},
		leiosWaitTestWindow,
		time.Millisecond,
		leiosWaitTestLongWindow,
	)

	require.Contains(
		t,
		logs.String(),
		"endorser block could not be fetched",
		"the outcome must still be reported, just not at WARN",
	)
	require.NotContains(
		t,
		logs.String(),
		`"level":"WARN"`,
		"a fetch that swept every peer and found none holding the endorser "+
			"block is this path's expected outcome, not an alert",
	)
}

// TestCIPFetchWaitWarnsWhenAFetchNeitherCachesNorClears is the other half:
// the case that IS anomalous must stay at WARN. A fetch that neither caches
// nor clears its in-flight marker held the ledger pipeline for the whole hard
// bound and produced nothing, which is a wedged fetch rather than an absent
// endorser block.
func TestCIPFetchWaitWarnsWhenAFetchNeitherCachesNorClears(t *testing.T) {
	var logs leiosWaitTestLogBuffer
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		// Never returns while the wait runs, so the in-flight marker is still
		// set when awaitFetch gives up at the hard bound. Released by the
		// cleanup so the goroutine does not outlive the test.
		EndorserBlockFetcher: func(context.Context, uint64, []byte) error {
			<-release
			return errors.New("released")
		},
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)

	ref := leiosEbRef{
		slot: 100,
		hash: lcommon.NewBlake2b256(leiosTestHash(0xCA)),
	}
	// A live parent context, so the wait can only end at the hard bound --
	// never through the cancellation branch.
	ls.leiosBackfill.spawn(t.Context(), ref)

	ls.awaitInFlightEndorserFetches(
		t.Context(),
		[]leiosEbRef{ref},
		leiosWaitTestWindow,
		time.Millisecond,
		leiosWaitTestSlotLen,
	)

	require.Contains(
		t,
		logs.String(),
		"endorser block fetch neither completed nor cached within the hard bound",
	)
	require.Contains(
		t,
		logs.String(),
		`"level":"WARN"`,
		"a fetch wedged for the whole hard bound is worth an alert",
	)
}

// TestMandatoryFetchIsNotStarvedByBestEffortSpawns pins the reserved budget.
//
// spawn (best-effort) and fetchRequired (mandatory certified closure) used to
// share one semaphore. A best-effort fetch holds its slot for up to
// leiosBackfillMaxWait, so filling the semaphore with slow ones blocked a
// mandatory fetch behind them -- and this path now dispatches near-head
// prefetches through spawn as well, so that is far more reachable than when
// only historical backfill used it. A mandatory fetch must proceed regardless.
func TestMandatoryFetchIsNotStarvedByBestEffortSpawns(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	var mandatoryRan atomic.Bool
	spawned := make(chan struct{}, leiosBackfillConcurrency)

	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		EndorserBlockFetcher: func(
			_ context.Context,
			slot uint64,
			_ []byte,
		) error {
			if slot == 999 {
				// The mandatory one. Reaching here at all is the point.
				mandatoryRan.Store(true)
				return errors.New("no peer holds it")
			}
			// Best-effort: occupy the slot until the test ends.
			select {
			case spawned <- struct{}{}:
			default:
			}
			<-release
			return nil
		},
	}
	b := newLeiosBackfiller(cfg)
	require.NotNil(t, b)

	// Saturate the best-effort budget.
	for i := range leiosBackfillConcurrency {
		b.spawn(t.Context(), leiosEbRef{
			slot: uint64(100 + i),
			hash: lcommon.NewBlake2b256(leiosTestHash(byte(0xE0 + i))),
		})
	}
	for range leiosBackfillConcurrency {
		select {
		case <-spawned:
		case <-time.After(testutil.AsyncWait):
			t.Fatal("best-effort spawns never occupied the budget")
		}
	}

	// The mandatory fetch must not queue behind them.
	done := make(chan error, 1)
	go func() {
		done <- b.fetchRequired(
			t.Context(),
			leiosEbRef{
				slot: 999,
				hash: lcommon.NewBlake2b256(leiosTestHash(0xEF)),
			},
			time.Millisecond,
		)
	}()
	select {
	case <-done:
	case <-time.After(testutil.AsyncWait):
		t.Fatal(
			"mandatory certified fetch was starved by best-effort spawns; " +
				"it must have its own reserved budget",
		)
	}
	require.True(
		t,
		mandatoryRan.Load(),
		"the mandatory fetch never reached the fetcher",
	)
}

// TestCIPGraceUnavailableIsNotRecordedAsATimeout pins the grace phase's metric
// classification against the two ways inferring it after the fact goes wrong.
//
// Re-reading the cache and the context once awaitFetch has returned cannot
// tell a fetch that COMPLETED without caching (routine on a CIP node: no peer
// holds the block) from one that ran to the hard bound, so the routine case
// was recorded as a timeout -- inflating both the timeout histogram and
// dingo_metrics_leios_eb_wait_timeouts_total on every unfetchable block.
// awaitFetch now reports its own termination cause instead.
func TestCIPGraceUnavailableIsNotRecordedAsATimeout(t *testing.T) {
	reg := prometheus.NewRegistry()
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		// Completes immediately without caching: the in-flight marker clears
		// long before the hard bound, so nothing timed out.
		EndorserBlockFetcher: func(context.Context, uint64, []byte) error {
			return errors.New("no peer holds it")
		},
	}
	ls := &LedgerState{config: cfg}
	ls.metrics.init(reg)
	ls.leiosBackfill = newLeiosBackfiller(cfg)

	ref := leiosEbRef{
		slot: 100,
		hash: lcommon.NewBlake2b256(leiosTestHash(0xF1)),
	}
	ls.leiosBackfill.spawn(t.Context(), ref)

	ls.awaitInFlightEndorserFetches(
		t.Context(),
		[]leiosEbRef{ref},
		leiosWaitTestWindow,
		time.Millisecond,
		leiosWaitTestLongWindow,
	)

	require.Equal(
		t,
		uint64(1),
		leiosWaitTestHistogram(t, reg, "unavailable"),
		"a fetch that completed without caching is its own outcome",
	)
	require.Zero(
		t,
		leiosWaitTestHistogram(t, reg, "timeout"),
		"nothing timed out: the hard bound was never approached",
	)
	require.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.leiosEbWaitTimeouts),
		"the timeout counter must not move for a routine unfetchable block",
	)
}
