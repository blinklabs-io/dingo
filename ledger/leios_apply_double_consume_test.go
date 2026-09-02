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
	"database/sql"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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
		0: []any{[]any{inTxId, inIdx}},
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
// This is the wedge reported as issue #3643 ("UTxO already spent" while
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
