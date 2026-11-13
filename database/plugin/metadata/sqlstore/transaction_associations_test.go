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
	"bytes"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func TestCollateralProductionApplyHydrateRollback(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", fmt.Sprintf("file:collateral_prod_%d?mode=memory&cache=shared", testStoreSequence.Add(1)))
	require.NoError(t, err)
	seedMigratedSQLite(t, db)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{WriteDB: db, Dialect: SQLiteDialect(), StorageMode: types.StorageModeAPI, Migrations: registry, MigrationLocker: migrations.NewProcessLocker()})
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	collateralProductionFlow(t, store, db)
}

func TestSetTransactionBatchedCommitAndRollback(t *testing.T) {
	t.Parallel()
	db, err := sql.Open(
		"sqlite",
		fmt.Sprintf(
			"file:batched_transactions_%d?mode=memory&cache=shared",
			testStoreSequence.Add(1),
		),
	)
	require.NoError(t, err)
	seedMigratedSQLite(t, db)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{
		WriteDB:         db,
		Dialect:         SQLiteDialect(),
		StorageMode:     types.StorageModeAPI,
		Migrations:      registry,
		MigrationLocker: migrations.NewProcessLocker(),
	})
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	makeTransaction := func(id byte) lcommon.Transaction {
		input, err := mockledger.NewTransactionInputBuilder().
			WithTxId(bytes.Repeat([]byte{id + 0x10}, 32)).
			WithIndex(0).
			Build()
		require.NoError(t, err)
		output, err := mockledger.NewTransactionOutputBuilder().
			WithAddress("addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp").
			WithLovelace(5_000_000).
			Build()
		require.NoError(t, err)
		transaction, err := mockledger.NewTransactionBuilder().
			WithId(bytes.Repeat([]byte{id}, 32)).
			WithInputs(input).
			WithOutputs(output).
			Build()
		require.NoError(t, err)
		return transaction
	}

	committed := []lcommon.Transaction{
		makeTransaction(1),
		makeTransaction(2),
	}
	commitTxn := store.Transaction(t.Context())
	commitBatch := store.NewBatchAccumulator()
	for index, transaction := range committed {
		require.NoError(t, store.SetTransactionBatched(
			transaction,
			ocommon.Point{Slot: uint64(index + 1), Hash: transaction.Hash().Bytes()},
			uint32(index), nil, false, commitBatch, commitTxn, 0,
		))
	}
	require.NoError(t, store.FlushBatch(commitBatch, commitTxn))
	require.NoError(t, commitTxn.Commit())

	rows, err := store.writeDB.Query(
		`SELECT t.id, t.hash, a.slot, a.block_index FROM "transaction" t
JOIN applied_transaction a ON a.tx_id = t.id ORDER BY t.id`,
	)
	require.NoError(t, err)
	defer rows.Close()
	for index, want := range committed {
		require.True(t, rows.Next())
		var (
			id         uint
			hash       []byte
			slot       uint64
			blockIndex uint32
		)
		require.NoError(t, rows.Scan(&id, &hash, &slot, &blockIndex))
		require.Equal(t, uint(index+1), id)
		require.Equal(t, want.Hash().Bytes(), hash)
		require.Equal(t, uint64(index+1), slot)
		require.Equal(t, uint32(index), blockIndex)
	}
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())

	rolledBack := []lcommon.Transaction{
		makeTransaction(3),
		makeTransaction(4),
	}
	rollbackTxn := store.Transaction(t.Context())
	rollbackBatch := store.NewBatchAccumulator()
	for index, transaction := range rolledBack {
		require.NoError(t, store.SetTransactionBatched(
			transaction,
			ocommon.Point{Slot: uint64(index + 10), Hash: transaction.Hash().Bytes()},
			uint32(index), nil, false, rollbackBatch, rollbackTxn, 0,
		))
	}
	require.NoError(t, rollbackTxn.Rollback())
	// Reset closes the accumulator's transaction-scoped prepared statement
	// after the caller rolls the SQL transaction back.
	rollbackBatch.Reset()

	var count int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM "transaction"`,
	).Scan(&count))
	require.Equal(t, len(committed), count)
}

// seedCollateralUtxo stores an output of the transaction with hash txHash,
// creating the identity row the output's tx_id names.
func seedCollateralUtxo(t *testing.T, store *Store, db *sql.DB, txHash []byte) {
	t.Helper()
	table := store.dialect.QuoteIdentifier("transaction")
	_, err := db.Exec(
		store.dialect.Rebind("INSERT INTO "+table+" (hash) VALUES (?)"), txHash,
	)
	require.NoError(t, err)
	_, err = db.Exec(store.dialect.Rebind(
		"INSERT INTO utxo (tx_id, output_idx, credential_tag, amount, payment_script) "+
			"VALUES ((SELECT id FROM "+table+" WHERE hash = ?), 0, 0, '1', FALSE)",
	), txHash)
	require.NoError(t, err)
}

// collateralProductionFlow runs against an already-migrated provider store;
// integration tests use it to exercise the same contract on every dialect.
func collateralProductionFlow(t *testing.T, store *Store, db *sql.DB) {
	t.Helper()
	input, err := mockledger.NewSimpleTransactionInput(bytes.Repeat([]byte{0xaa}, 32), 0)
	require.NoError(t, err)
	seedCollateralUtxo(t, store, db, input.Id().Bytes())
	makeTx := func(first byte) lcommon.Transaction {
		hash := bytes.Repeat([]byte{first}, 32)
		builder := mockledger.NewTransactionBuilder()
		builder.WithId(hash)
		builder.WithCollateral(input).WithValid(true)
		return builder
	}
	txA := makeTx(0xbb)
	txB := makeTx(0xcc)
	for _, item := range []struct {
		tx   lcommon.Transaction
		slot uint64
	}{{txA, 10}, {txB, 20}} {
		require.NoError(t, store.SetTransaction(item.tx, ocommon.Point{Slot: item.slot, Hash: item.tx.Hash().Bytes()}, 0, nil, false, nil, 0))
	}
	for _, hash := range [][]byte{txA.Hash().Bytes(), txB.Hash().Bytes()} {
		got, err := store.GetTransactionByHash(hash, nil)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Len(t, got.Collateral, 1)
	}
	// Both transactions are valid, so the collateral they name is recorded but
	// never consumed: only a phase-2 script failure spends collateral, and that
	// is the ledger's decision rather than the indexer's.
	var spentBy sql.NullInt64
	var deletedSlot sql.NullInt64
	require.NoError(t, db.QueryRow(store.dialect.Rebind(
		"SELECT spent_at_tx_id, deleted_slot FROM utxo WHERE tx_id = (SELECT id FROM "+
			store.dialect.QuoteIdentifier("transaction")+" WHERE hash = ?)",
	), input.Id().Bytes()).Scan(&spentBy, &deletedSlot))
	require.False(t, spentBy.Valid)
	require.Zero(t, deletedSlot.Int64)

	require.NoError(t, store.DeleteTransactionsAfterSlot(10, nil))
	var associationCount int
	require.NoError(t, db.QueryRow(store.dialect.Rebind(`SELECT COUNT(*) FROM utxo_collateral_edge e JOIN `+store.dialect.QuoteIdentifier("transaction")+` t ON t.id = e.tx_id WHERE t.hash = ?`), txA.Hash().Bytes()).Scan(&associationCount))
	require.Equal(t, 1, associationCount)
	require.NoError(t, db.QueryRow(store.dialect.Rebind(`SELECT COUNT(*) FROM utxo_collateral_edge e JOIN `+store.dialect.QuoteIdentifier("transaction")+` t ON t.id = e.tx_id WHERE t.hash = ?`), txB.Hash().Bytes()).Scan(&associationCount))
	require.Equal(t, 0, associationCount)
	got, err := store.GetTransactionByHash(txA.Hash().Bytes(), nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Len(t, got.Collateral, 1)
	gone, err := store.GetTransactionByHash(txB.Hash().Bytes(), nil)
	require.NoError(t, err)
	require.Nil(t, gone)
}

// TestCollateralRollbackOrderAcrossRestart rolls back two transactions that
// share one collateral input, later first, closing and reopening the on-disk
// store between steps. Each step asserts the association edge of both
// transactions, so a rollback that removed the shared input's association for
// every owner, or a restart that lost an edge, is observable.
func TestCollateralRollbackOrderAcrossRestart(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "collateral.db")
	dsn := "file:" + path +
		"?_pragma=synchronous(OFF)&_pragma=journal_mode(MEMORY)"
	open := func() (*Store, *sql.DB) {
		db, err := sql.Open("sqlite", dsn)
		require.NoError(t, err)
		// Only the first open starts from the template; a reopen must keep
		// the data the previous store wrote.
		if _, statErr := os.Stat(path); errors.Is(statErr, os.ErrNotExist) {
			seedMigratedSQLite(t, db)
		}
		registry, err := migrations.SQLiteRegistry()
		require.NoError(t, err)
		store, err := New(Config{
			WriteDB:         db,
			Dialect:         SQLiteDialect(),
			StorageMode:     types.StorageModeAPI,
			Migrations:      registry,
			MigrationLocker: migrations.NewProcessLocker(),
		})
		require.NoError(t, err)
		require.NoError(t, store.Start(t.Context()))
		return store, db
	}
	store, db := open()
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	reopen := func() {
		require.NoError(t, store.Close())
		require.NoError(t, db.Close())
		store, db = open()
	}

	input, err := mockledger.NewSimpleTransactionInput(
		bytes.Repeat([]byte{0xaa}, 32), 0,
	)
	require.NoError(t, err)
	seedCollateralUtxo(t, store, db, input.Id().Bytes())
	makeTx := func(id byte) lcommon.Transaction {
		builder := mockledger.NewTransactionBuilder()
		builder.WithId(bytes.Repeat([]byte{id}, 32))
		builder.WithCollateral(input).WithValid(true)
		return builder
	}
	txA, txB := makeTx(0xbb), makeTx(0xcc)
	for _, item := range []struct {
		tx   lcommon.Transaction
		slot uint64
	}{{txA, 10}, {txB, 20}} {
		require.NoError(t, store.SetTransaction(
			item.tx,
			ocommon.Point{Slot: item.slot, Hash: item.tx.Hash().Bytes()},
			0, nil, false, nil, 0,
		))
	}

	edges := func(tx lcommon.Transaction) int {
		var n int
		require.NoError(t, db.QueryRow(
			`SELECT COUNT(*) FROM utxo_collateral_edge e JOIN "transaction" t ON t.id = e.tx_id WHERE t.hash = ?`,
			tx.Hash().Bytes(),
		).Scan(&n))
		return n
	}
	requireEdges := func(step string, wantA, wantB int) {
		t.Helper()
		require.Equal(t, wantA, edges(txA), "%s: edge of the earlier transaction", step)
		require.Equal(t, wantB, edges(txB), "%s: edge of the later transaction", step)
	}

	requireEdges("applied", 1, 1)
	reopen()
	requireEdges("reopened after apply", 1, 1)

	require.NoError(t, store.DeleteTransactionsAfterSlot(10, nil))
	requireEdges("later rolled back", 1, 0)
	reopen()
	requireEdges("reopened after later rollback", 1, 0)
	got, err := store.GetTransactionByHash(txA.Hash().Bytes(), nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Len(t, got.Collateral, 1)

	require.NoError(t, store.DeleteTransactionsAfterSlot(9, nil))
	requireEdges("earlier rolled back", 0, 0)
	reopen()
	requireEdges("reopened after earlier rollback", 0, 0)
	gone, err := store.GetTransactionByHash(txA.Hash().Bytes(), nil)
	require.NoError(t, err)
	require.Nil(t, gone)
}

func TestCollateralInputsPreserveManyToManyOwnership(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	_, err := store.writeDB.Exec(`CREATE TABLE utxo (
id INTEGER PRIMARY KEY AUTOINCREMENT, tx_id INTEGER NOT NULL,
output_idx INTEGER NOT NULL, collateral_return_for_tx_id INTEGER,
spent_at_tx_id INTEGER, payment_key BLOB, staking_key BLOB,
credential_tag INTEGER, datum_hash BLOB, amount TEXT, payment_script BOOLEAN,
added_slot INTEGER, deleted_slot INTEGER)`)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(`CREATE TABLE "transaction" (
id INTEGER PRIMARY KEY AUTOINCREMENT, hash BLOB)`)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(`CREATE TABLE utxo_collateral_edge (
utxo_id INTEGER NOT NULL, tx_id INTEGER NOT NULL,
PRIMARY KEY (utxo_id, tx_id))`)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(`CREATE TABLE utxo_reference_edge (
utxo_id INTEGER NOT NULL, tx_id INTEGER NOT NULL,
PRIMARY KEY (utxo_id, tx_id))`)
	require.NoError(t, err)
	utxoID, err := hex.DecodeString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	require.NoError(t, err)
	secondUtxoID, err := hex.DecodeString("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
	require.NoError(t, err)
	const ownerA, ownerB = int64(1), int64(2)
	_, err = store.writeDB.Exec(
		"INSERT INTO utxo (tx_id, output_idx, amount, credential_tag, payment_script) VALUES (?, 0, '1', 0, FALSE)",
		testutil.TransactionID(t, store.writeDB, utxoID),
	)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(
		"INSERT INTO utxo (tx_id, output_idx, amount, credential_tag, payment_script) VALUES (?, 0, '1', 0, FALSE)",
		testutil.TransactionID(t, store.writeDB, secondUtxoID),
	)
	require.NoError(t, err)
	input, err := mockledger.NewSimpleTransactionInput(utxoID, 0)
	require.NoError(t, err)
	secondInput, err := mockledger.NewSimpleTransactionInput(secondUtxoID, 0)
	require.NoError(t, err)
	inputs := []lcommon.TransactionInput{input, secondInput}
	require.NoError(t, store.markTransactionUtxoReferences(
		t.Context(), store.writeDB, inputs,
		utxoCollateralEdgeTable, ownerA,
	))
	// Re-indexing is idempotent, while a second transaction retains its own edge.
	require.NoError(t, store.markTransactionUtxoReferences(
		t.Context(), store.writeDB, inputs,
		utxoCollateralEdgeTable, ownerA,
	))
	require.NoError(t, store.markTransactionUtxoReferences(
		t.Context(), store.writeDB, inputs,
		utxoCollateralEdgeTable, ownerB,
	))
	got, err := store.collateralInputsBatch(
		t.Context(), store.writeDB, []any{ownerA, ownerB},
	)
	require.NoError(t, err)
	require.Len(t, got["1"], 2)
	require.Len(t, got["2"], 2)
	require.Equal(t, got["1"][0].ID, got["2"][0].ID)
	require.Equal(t, got["1"][1].ID, got["2"][1].ID)
	var count int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM utxo_collateral_edge",
	).Scan(&count))
	require.Equal(t, 4, count)
	// Rollback cleanup is keyed by transaction, never by the shared UTxO.
	_, err = store.writeDB.Exec(
		"DELETE FROM utxo_collateral_edge WHERE tx_id = ?", ownerA,
	)
	require.NoError(t, err)
	got, err = store.collateralInputsBatch(t.Context(), store.writeDB, []any{ownerB})
	require.NoError(t, err)
	require.Len(t, got["2"], 2)
	require.NoError(t, store.markTransactionUtxoReferences(
		t.Context(), store.writeDB, inputs,
		utxoReferenceEdgeTable, ownerA,
	))
	require.NoError(t, store.markTransactionUtxoReferences(
		t.Context(), store.writeDB, inputs,
		utxoReferenceEdgeTable, ownerB,
	))
	references, err := store.referenceInputsBatch(
		t.Context(), store.writeDB, []any{ownerA, ownerB},
	)
	require.NoError(t, err)
	require.Len(t, references["1"], 2)
	require.Len(t, references["2"], 2)
	require.Equal(t, references["1"][0].ID, references["2"][0].ID)
	require.Equal(t, references["1"][1].ID, references["2"][1].ID)
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM utxo_reference_edge",
	).Scan(&count))
	require.Equal(t, 4, count)
}

func TestLoadUtxoAssetsBatchPreservesGrouping(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	testutil.InsertAssetHolding(
		t, store.writeDB, 10, []byte("p"), []byte("a"),
		"18446744073709551615",
	)
	testutil.InsertAssetHolding(
		t, store.writeDB, 20, []byte("p"), []byte("b"), "7",
	)
	utxos := map[string][]models.Utxo{
		"first":  {{ID: 10}},
		"second": {{ID: 20}},
	}
	require.NoError(
		t,
		store.loadUtxoAssetsBatch(t.Context(), store.writeDB, utxos),
	)
	first := utxos["first"]
	second := utxos["second"]
	if len(first) == 0 || len(second) == 0 {
		t.Fatal("expected hydrated UTxOs")
	}
	if len(first[0].Assets) == 0 {
		t.Fatal("expected first asset")
	}
	require.Equal(t, uint64(math.MaxUint64), uint64(first[0].Assets[0].Amount))
	if len(second[0].Assets) == 0 {
		t.Fatal("expected second asset")
	}
	require.Equal(t, uint64(7), uint64(second[0].Assets[0].Amount))
}

func TestLoadUtxoAssetsDeduplicatesIDsAcrossChunks(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	testutil.InsertAssetHolding(
		t, store.writeDB, 10, []byte("p"), []byte("a"), "1",
	)

	// Use enough repeated instances to force the same ID into two parameter
	// chunks.  Each instance still needs one asset, but the asset row must be
	// queried only once.
	utxos := make([]models.Utxo, 1000)
	for i := range utxos {
		utxos[i].ID = 10
	}
	pointers := make([]*models.Utxo, len(utxos))
	for i := range utxos {
		pointers[i] = &utxos[i]
	}
	require.NoError(
		t,
		store.loadUtxoAssets(t.Context(), store.writeDB, pointers),
	)
	for i := range utxos {
		require.Len(t, utxos[i].Assets, 1)
	}
}
