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

package sqlstore

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func producerTestInput(t *testing.T, hash []byte, idx uint32) lcommon.TransactionInput {
	t.Helper()
	in, err := mockledger.NewTransactionInputBuilder().
		WithTxId(hash).WithIndex(idx).Build()
	require.NoError(t, err)
	return in
}

func producerTestOutput(t *testing.T, lovelace uint64) lcommon.TransactionOutput {
	t.Helper()
	out, err := mockledger.NewTransactionOutputBuilder().
		WithAddress("addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp").
		WithLovelace(lovelace).Build()
	require.NoError(t, err)
	return out
}

// TestOutputsResolveToTheRowTheHashPathFinds applies a valid transaction, a
// phase-2 failure with a collateral return, and a spender of both plus an
// imported output, then checks that every reference resolves, through the
// producer and imported indexes, to the row the tx_id hash names, with the
// producing hash unchanged.
func TestOutputsResolveToTheRowTheHashPathFinds(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	valid := bytes32(0x71)
	failed := bytes32(0x72)
	imported := bytes32(0x73)
	spender := bytes32(0x74)

	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: imported, OutputIdx: 0, Amount: types.Uint64(5),
	}}, nil))

	validTx := mockledger.NewTransactionBuilder()
	validTx.WithId(valid)
	validTx.WithInputs(producerTestInput(t, bytes32(0x70), 0))
	validTx.WithOutputs(
		producerTestOutput(t, 2_000_000), producerTestOutput(t, 3_000_000),
	)
	validTx.WithValid(true)
	failedTx := mockledger.NewTransactionBuilder()
	failedTx.WithId(failed)
	failedTx.WithInputs(producerTestInput(t, bytes32(0x70), 1))
	failedTx.WithValid(false)
	spendTx := mockledger.NewTransactionBuilder()
	spendTx.WithId(spender)
	spendTx.WithInputs(
		producerTestInput(t, valid, 0),
		producerTestInput(t, failed, 1),
		producerTestInput(t, imported, 0),
	)
	spendTx.WithOutputs(producerTestOutput(t, 1))
	spendTx.WithValid(true)
	apply := func(i int, tx lcommon.Transaction) {
		require.NoError(t, store.SetTransaction(
			tx, ocommon.Point{Slot: uint64(10 + i), Hash: tx.Hash().Bytes()},
			0, nil, false, nil, 0,
		))
	}
	apply(0, validTx)
	apply(1, failedTx)
	// The mock builder never produces a collateral return, so write the one
	// the failed transaction would leave: index 1, after its one declared
	// output.
	var failedID uint
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT id FROM "transaction" WHERE hash = ?`, failed,
	).Scan(&failedID))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId: failed, OutputIdx: 1, Amount: types.Uint64(1_000_000),
		CollateralReturnForTxID: &failedID,
	}))
	apply(2, spendTx)

	refs := []models.UtxoId{
		{Hash: valid, Idx: 0},
		{Hash: valid, Idx: 1},
		{Hash: failed, Idx: 1},
		{Hash: imported, Idx: 0},
		{Hash: spender, Idx: 0},
	}
	for _, ref := range refs {
		var want int64
		require.NoError(t, store.writeDB.QueryRowContext(ctx,
			`SELECT id FROM utxo WHERE tx_id = (SELECT id FROM "transaction" WHERE hash = ?) AND output_idx = ?`,
			ref.Hash, ref.Idx,
		).Scan(&want), "%x#%d", ref.Hash, ref.Idx)
		got, err := store.GetUtxoIncludingSpent(ref.Hash, ref.Idx, nil)
		require.NoError(t, err)
		require.NotNil(t, got, "%x#%d", ref.Hash, ref.Idx)
		require.Equal(t, uint(want), got.ID, "%x#%d", ref.Hash, ref.Idx)
		require.Equal(t, ref.Hash, got.TxId)
	}
	// The collateral return is found at the index after the outputs and is
	// the failed transaction's own row.
	var returnFor int64
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT collateral_return_for_tx_id FROM utxo WHERE tx_id = (SELECT id FROM "transaction" WHERE hash = ?) AND output_idx = 1`,
		failed,
	).Scan(&returnFor))
	require.NotZero(t, returnFor)

	// Every input the spender names is spent by it, whichever identity the
	// row has.
	for _, ref := range []models.UtxoId{refs[0], refs[2], refs[3]} {
		var spentBy int64
		require.NoError(t, store.writeDB.QueryRow(
			`SELECT spent_at_tx_id FROM utxo WHERE tx_id = (SELECT id FROM "transaction" WHERE hash = ?) AND output_idx = ?`,
			ref.Hash, ref.Idx,
		).Scan(&spentBy), "%x#%d", ref.Hash, ref.Idx)
		var want int64
		require.NoError(t, store.writeDB.QueryRow(
			`SELECT id FROM "transaction" WHERE hash = ?`, spender,
		).Scan(&want))
		require.Equal(t, want, spentBy)
	}

	// No identity exists twice: re-importing the produced outputs and
	// re-applying the producer leave one row per reference.
	require.NoError(t, store.ImportUtxos([]models.Utxo{
		{TxId: valid, OutputIdx: 0, Amount: types.Uint64(1)},
		{TxId: imported, OutputIdx: 0, Amount: types.Uint64(5)},
	}, nil))
	var count int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM utxo WHERE (tx_id = (SELECT id FROM "transaction" WHERE hash = ?) AND output_idx = 0) OR (tx_id = (SELECT id FROM "transaction" WHERE hash = ?) AND output_idx = 0)`,
		valid, imported,
	).Scan(&count))
	require.Equal(t, 2, count, "an import duplicated a produced or imported output")
	require.NoError(t, store.SetTransaction(
		validTx, ocommon.Point{Slot: 10, Hash: validTx.Hash().Bytes()},
		0, nil, false, nil, 0,
	))
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM utxo WHERE tx_id = (SELECT id FROM "transaction" WHERE hash = ?)`, valid,
	).Scan(&count))
	require.Equal(t, 2, count, "re-applying a transaction duplicated its outputs")
}

// TestImportThenApplyKeepsOneRowPerOutput applies a producer over an output an
// import stored first. The import gives the producer an identity row and no
// application row; applying it reuses that row, adds its application row and
// leaves the output one row carrying the producer's id.
func TestImportThenApplyKeepsOneRowPerOutput(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	producer := bytes32(0x81)
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: producer, OutputIdx: 0, Amount: types.Uint64(2_000_000),
	}}, nil))
	var importedID int64
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT id FROM "transaction" WHERE hash = ?`, producer,
	).Scan(&importedID))
	var applied int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM applied_transaction WHERE tx_id = ?`, importedID,
	).Scan(&applied))
	require.Zero(t, applied, "an imported producer has no application row")

	tx := mockledger.NewTransactionBuilder()
	tx.WithId(producer)
	tx.WithInputs(producerTestInput(t, bytes32(0x80), 0))
	tx.WithOutputs(
		producerTestOutput(t, 2_000_000), producerTestOutput(t, 3_000_000),
	)
	tx.WithValid(true)
	require.NoError(t, store.SetTransaction(
		tx, ocommon.Point{Slot: 5, Hash: producer}, 0, nil, false, nil, 0,
	))

	var rows int
	var producerID int64
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*), MIN(tx_id) FROM utxo WHERE tx_id = ?`, importedID,
	).Scan(&rows, &producerID))
	require.Equal(t, 2, rows, "the replayed output must reuse the imported row")
	require.Equal(t, importedID, producerID)
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM applied_transaction WHERE tx_id = ?`, importedID,
	).Scan(&applied))
	require.Equal(t, 1, applied)
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM "transaction" WHERE hash = ?`, producer,
	).Scan(&rows))
	require.Equal(t, 1, rows, "applying must not add a second identity row")
}

// TestImportedProducerOverlayFallsBackAtEveryIndex applies, in a trusted
// batch, the producer of an output an import stored, both at an index the
// producer also writes and at one it does not. The producer's identity row
// already exists, so the overlay reports a conflict and writes nothing; the
// per-row path the ledger then takes reuses that identity, keeps the
// imported row at a shared index and adds the others beside it.
func TestImportedProducerOverlayFallsBackAtEveryIndex(t *testing.T) {
	t.Parallel()
	for name, importedIdx := range map[string]uint32{
		"shared_index": 1, "other_index": 5,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := newMigratedSQLiteStore(t)
			producer := bytes32(0x91)
			require.NoError(t, store.ImportUtxos([]models.Utxo{{
				TxId: producer, OutputIdx: importedIdx,
				Amount: types.Uint64(3_000_000),
			}}, nil))
			var importedRow, identity int64
			require.NoError(t, store.writeDB.QueryRow(
				`SELECT u.id, u.tx_id FROM utxo AS u
				 JOIN "transaction" AS t ON t.id = u.tx_id
				 WHERE t.hash = ? AND u.output_idx = ?`,
				producer, importedIdx,
			).Scan(&importedRow, &identity))

			tx := mockledger.NewTransactionBuilder()
			tx.WithId(producer)
			tx.WithInputs(producerTestInput(t, bytes32(0x90), 0))
			tx.WithOutputs(
				producerTestOutput(t, 2_000_000),
				producerTestOutput(t, 3_000_000),
			)
			tx.WithValid(true)
			point := ocommon.Point{Slot: 7, Hash: producer}
			apply := func(overlay bool) error {
				txn := store.Transaction(context.Background())
				defer func() { _ = txn.Rollback() }()
				acc := store.NewBatchAccumulator()
				if overlay {
					enabled, err := store.EnableBatchUtxoOverlay(
						acc, txn, []models.UtxoId{
							{Hash: bytes32(0x90), Idx: 0},
						}, nil,
					)
					require.NoError(t, err)
					require.True(t, enabled)
				}
				if err := store.SetTransactionBatched(
					tx, point, 0, nil, true, acc, txn, 0,
				); err != nil {
					return err
				}
				if err := store.FlushBatchStakeDeltas(acc, txn); err != nil {
					return err
				}
				if err := store.FlushBatch(acc, txn); err != nil {
					return err
				}
				return txn.Commit()
			}
			require.ErrorIs(
				t, apply(true), types.ErrBatchUtxoOverlayConflict,
			)
			var count int
			require.NoError(t, store.writeDB.QueryRow(
				"SELECT COUNT(*) FROM applied_transaction",
			).Scan(&count))
			require.Zero(t, count, "the failed overlay batch was written")

			require.NoError(t, apply(false))
			want := map[uint32]string{0: "2000000", 1: "3000000"}
			if importedIdx != 1 {
				want[importedIdx] = "3000000"
			}
			rows, err := store.writeDB.Query(
				`SELECT id, tx_id, output_idx, amount FROM utxo
				 WHERE deleted_slot = 0 ORDER BY output_idx`,
			)
			require.NoError(t, err)
			defer rows.Close()
			got := map[uint32]string{}
			for rows.Next() {
				var id, txID int64
				var idx uint32
				var amount string
				require.NoError(t, rows.Scan(&id, &txID, &idx, &amount))
				require.Equal(t, identity, txID, "output %d", idx)
				if idx == importedIdx {
					require.Equal(t, importedRow, id,
						"the imported row was replaced at %d", idx)
				}
				got[idx] = amount
			}
			require.NoError(t, rows.Err())
			require.Equal(t, want, got)
			require.NoError(t, store.writeDB.QueryRow(
				`SELECT COUNT(*) FROM "transaction" WHERE hash = ?`, producer,
			).Scan(&count))
			require.Equal(t, 1, count, "a second identity row was added")
			require.NoError(t, store.writeDB.QueryRow(
				`SELECT COUNT(*) FROM applied_transaction WHERE tx_id = ?`,
				identity,
			).Scan(&count))
			require.Equal(t, 1, count)
		})
	}
}

// TestOutputLookupsDoNotScan pins the plans of the reference lookups the apply
// path, rollback and the APIs run: each reaches utxo through the producer and
// imported indexes or the primary key, and reaches transaction through the
// hash index.
func TestOutputLookupsDoNotScan(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedRollbackUtxos(t, store, 20_000, 64, 2_656_808, 40)
	analyzeStore(t, store)
	hash := make([]byte, 32)
	for name, tc := range map[string]struct {
		query string
		args  []any
	}{
		"input_resolution": {
			sqliteUtxoSQL.getLiveByRef, []any{hash, int64(0)},
		},
		"including_spent": {
			sqliteUtxoSQL.getIncludingSpent,
			[]any{hash, int64(0)},
		},
		"spend_one": {
			consumeUtxoSQLiteReturningSQL,
			[]any{int64(5), int64(1), hash, int64(0)},
		},
		"spend_batch": {consumeUtxosBatchQuery(3, true), []any{
			int64(5), int64(1),
			hash, int64(0),
			hash, int64(1),
			hash, int64(2),
		}},
		"spend_state": {sqliteUtxoSQL.getSpendState, []any{hash, int64(0)}},
		"spend_state_hash": {
			sqliteUtxoSQL.getSpendStateHash, []any{hash, int64(0)},
		},
		"by_tx_hash": {
			sqliteRefs.deletionRows[2], []any{hash, hash},
		},
	} {
		plan := explainQueryPlan(t, store, tc.query, tc.args...)
		require.NotContains(t, plan, "SCAN utxo", "%s: %s", name, plan)
		require.NotContains(t, plan, "SCAN transaction", "%s: %s", name, plan)
		require.Contains(t, plan, "idx_utxo_tx_id_output_idx", "%s: %s", name, plan)
		require.Contains(t, plan, "idx_transaction_hash", "%s: %s", name, plan)
	}
}
