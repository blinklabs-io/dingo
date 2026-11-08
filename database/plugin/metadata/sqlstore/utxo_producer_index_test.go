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
		{Hash: valid, Idx: 0}, {Hash: valid, Idx: 1},
		{Hash: failed, Idx: 1}, {Hash: imported, Idx: 0},
		{Hash: spender, Idx: 0},
	}
	for _, ref := range refs {
		var want int64
		require.NoError(t, store.writeDB.QueryRowContext(ctx,
			"SELECT id FROM utxo WHERE tx_id = ? AND output_idx = ?",
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
		"SELECT collateral_return_for_tx_id FROM utxo WHERE tx_id = ? AND output_idx = 1",
		failed,
	).Scan(&returnFor))
	require.NotZero(t, returnFor)

	// Every input the spender names is spent by it, whichever identity the
	// row has.
	for _, ref := range []models.UtxoId{refs[0], refs[2], refs[3]} {
		var spentBy int64
		require.NoError(t, store.writeDB.QueryRow(
			"SELECT spent_by_tx_id FROM utxo WHERE tx_id = ? AND output_idx = ?",
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
		"SELECT COUNT(*) FROM utxo WHERE (tx_id = ? AND output_idx = 0) OR (tx_id = ? AND output_idx = 0)",
		valid, imported,
	).Scan(&count))
	require.Equal(t, 2, count, "an import duplicated a produced or imported output")
	require.NoError(t, store.SetTransaction(
		validTx, ocommon.Point{Slot: 10, Hash: validTx.Hash().Bytes()},
		0, nil, false, nil, 0,
	))
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM utxo WHERE tx_id = ?", valid,
	).Scan(&count))
	require.Equal(t, 2, count, "re-applying a transaction duplicated its outputs")
}

// TestImportedOutputIsHydratedByItsProducer replays a producer over an output
// an import stored first. The output must stay one row, now carrying the
// producer, rather than gain a second row under the producer index.
func TestImportedOutputIsHydratedByItsProducer(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	producer := bytes32(0x81)
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: producer, OutputIdx: 0, Amount: types.Uint64(2_000_000),
	}}, nil))
	tx := mockledger.NewTransactionBuilder()
	tx.WithId(producer)
	tx.WithInputs(producerTestInput(t, bytes32(0x80), 0))
	tx.WithOutputs(producerTestOutput(t, 2_000_000))
	tx.WithValid(true)
	require.NoError(t, store.SetTransaction(
		tx, ocommon.Point{Slot: 5, Hash: producer}, 0, nil, false, nil, 0,
	))
	var rows int
	var transactionID *int64
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*), MAX(transaction_id) FROM utxo WHERE tx_id = ?", producer,
	).Scan(&rows, &transactionID))
	require.Equal(t, 1, rows)
	require.NotNil(t, transactionID, "the producer must hydrate the imported row")
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
			sqliteUtxoSQL.getLiveByRef, []any{hash, int64(0), hash, int64(0)},
		},
		"including_spent": {
			sqliteUtxoSQL.getIncludingSpent,
			[]any{hash, int64(0), hash, int64(0)},
		},
		"spend_one": {consumeUtxoSQLiteReturningSQL,
			[]any{int64(5), int64(1), hash, int64(0), hash, int64(0)}},
		"spend_batch": {consumeUtxosBatchQuery(3, true), []any{
			int64(5), int64(1),
			hash, int64(0), hash, int64(0),
			hash, int64(1), hash, int64(1),
			hash, int64(2), hash, int64(2),
		}},
		"spend_state": {sqliteUtxoSQL.getSpendState, []any{hash, int64(0), hash, int64(0)}},
		"spend_state_hash": {
			sqliteUtxoSQL.getSpendStateHash, []any{hash, int64(0), hash, int64(0)},
		},
		"by_tx_hash": {
			sqliteRefs.deletionRows[2], []any{hash, hash, hash, hash},
		},
	} {
		plan := explainQueryPlan(t, store, tc.query, tc.args...)
		require.NotContains(t, plan, "SCAN utxo", "%s: %s", name, plan)
		require.NotContains(t, plan, "SCAN transaction", "%s: %s", name, plan)
		require.Contains(t, plan, "idx_utxo_producer_output", "%s: %s", name, plan)
		require.Contains(t, plan, "idx_utxo_imported_ref", "%s: %s", name, plan)
		require.Contains(t, plan, "idx_transaction_hash", "%s: %s", name, plan)
	}
}

// TestCrossIdentityProbeUsesTheImportedIndex pins the plan of the probe that
// finds an imported row for a produced output. Without the index named, SQLite
// searches idx_utxo_collateral_return_for_tx_id for NULL, which holds a row
// per output.
func TestCrossIdentityProbeUsesTheImportedIndex(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedRollbackUtxos(t, store, 20_000, 64, 2_656_808, 40)
	analyzeStore(t, store)
	hash := make([]byte, 32)
	plan := explainQueryPlan(
		t, store, importedProbeQuery(2), hash, hash,
	)
	require.Contains(t, plan, "idx_utxo_imported_ref", plan)
	require.NotContains(t, plan, "SCAN utxo", plan)
}

// TestDirectInsertsRefuseTheOtherIdentity covers the writers with no conflict
// clause: CreateUtxo refuses a produced output over an imported row and the
// reverse, as the unique hash index did, and AddUtxos leaves a produced output
// alone when it is declared again as an imported-kind row.
func TestDirectInsertsRefuseTheOtherIdentity(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	importedHash, producedHash := bytes32(0x91), bytes32(0x92)
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: importedHash, OutputIdx: 0, Amount: types.Uint64(1),
	}}, nil))
	var producer int64
	require.NoError(t, store.writeDB.QueryRow(
		`INSERT INTO "transaction" (hash, slot) VALUES (?, 5) RETURNING id`,
		producedHash,
	).Scan(&producer))
	id := uint(producer)
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId: producedHash, OutputIdx: 0, TransactionID: &id,
		Amount: types.Uint64(1),
	}))

	err := store.CreateUtxo(nil, &models.Utxo{
		TxId: importedHash, OutputIdx: 0, TransactionID: &id,
		Amount: types.Uint64(1),
	})
	require.ErrorIs(t, err, types.ErrUtxoConflict, "produced over imported")
	err = store.CreateUtxo(nil, &models.Utxo{
		TxId: producedHash, OutputIdx: 0, Amount: types.Uint64(1),
	})
	require.ErrorIs(t, err, types.ErrUtxoConflict, "imported over produced")

	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: producedHash, OutputIdx: 0, Amount: types.Uint64(1),
	}}, nil))
	var count int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM utxo WHERE tx_id IN (?, ?)",
		importedHash, producedHash,
	).Scan(&count))
	require.Equal(t, 2, count)
}
