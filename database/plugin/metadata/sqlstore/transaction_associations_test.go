// Copyright 2026 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package sqlstore

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

func TestCollateralProductionApplyHydrateRollback(t *testing.T) {
	t.Parallel()
	db, err := sql.Open("sqlite", fmt.Sprintf("file:collateral_prod_%d?mode=memory&cache=shared", testStoreSequence.Add(1)))
	require.NoError(t, err)
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	store, err := New(Config{WriteDB: db, Dialect: SQLiteDialect(), StorageMode: types.StorageModeAPI, Migrations: registry, MigrationLocker: migrations.NewProcessLocker()})
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	collateralProductionFlow(t, store, db)
}

// collateralProductionFlow runs against an already-migrated provider store;
// integration tests use it to exercise the same contract on every dialect.
func collateralProductionFlow(t *testing.T, store *Store, db *sql.DB) {
	t.Helper()
	input, err := mockledger.NewSimpleTransactionInput(bytes.Repeat([]byte{0xaa}, 32), 0)
	require.NoError(t, err)
	_, err = db.Exec(store.dialect.Rebind("INSERT INTO utxo (tx_id, output_idx, credential_tag, amount, payment_script) VALUES (?, 0, 0, '1', FALSE)"), input.Id().Bytes())
	require.NoError(t, err)
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
		require.NoError(t, store.SetTransaction(item.tx, ocommon.Point{Slot: item.slot, Hash: item.tx.Hash().Bytes()}, 0, nil, false, nil))
	}
	for _, hash := range [][]byte{txA.Hash().Bytes(), txB.Hash().Bytes()} {
		got, err := store.GetTransactionByHash(hash, nil)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Len(t, got.Collateral, 1)
	}
	require.NoError(t, store.DeleteTransactionsAfterSlot(10, nil))
	var associationCount int
	require.NoError(t, db.QueryRow(store.dialect.Rebind("SELECT COUNT(*) FROM utxo_collateral_input WHERE transaction_hash = ?"), txA.Hash().Bytes()).Scan(&associationCount))
	require.Equal(t, 1, associationCount)
	require.NoError(t, db.QueryRow(store.dialect.Rebind("SELECT COUNT(*) FROM utxo_collateral_input WHERE transaction_hash = ?"), txB.Hash().Bytes()).Scan(&associationCount))
	require.Equal(t, 0, associationCount)
	got, err := store.GetTransactionByHash(txA.Hash().Bytes(), nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Len(t, got.Collateral, 1)
	gone, err := store.GetTransactionByHash(txB.Hash().Bytes(), nil)
	require.NoError(t, err)
	require.Nil(t, gone)
}

func TestCollateralInputsPreserveManyToManyOwnership(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	_, err := store.writeDB.Exec(`CREATE TABLE utxo (
transaction_id INTEGER, collateral_return_for_tx_id INTEGER, tx_id BLOB,
payment_key BLOB, staking_key BLOB, credential_tag INTEGER, datum_hash BLOB,
spent_at_tx_id BLOB, referenced_by_tx_id BLOB, collateral_by_tx_id BLOB,
id INTEGER PRIMARY KEY AUTOINCREMENT, added_slot INTEGER, deleted_slot INTEGER,
amount TEXT, output_idx INTEGER, payment_script BOOLEAN)`)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(`CREATE TABLE utxo_collateral_input (
utxo_id INTEGER NOT NULL, transaction_hash BLOB NOT NULL,
PRIMARY KEY (utxo_id, transaction_hash))`)
	require.NoError(t, err)
	utxoID, err := hex.DecodeString("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	require.NoError(t, err)
	ownerA, err := hex.DecodeString("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	require.NoError(t, err)
	ownerB, err := hex.DecodeString("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	require.NoError(t, err)
	_, err = store.writeDB.Exec(
		"INSERT INTO utxo (tx_id, output_idx, amount, credential_tag, payment_script) VALUES (?, 0, '1', 0, FALSE)",
		utxoID,
	)
	require.NoError(t, err)
	input, err := mockledger.NewSimpleTransactionInput(utxoID, 0)
	require.NoError(t, err)
	require.NoError(t, markTransactionUtxoReferences(
		t.Context(), store.writeDB, []lcommon.TransactionInput{input},
		"collateral_by_tx_id", ownerA,
	))
	// Re-indexing is idempotent, while a second transaction retains its own edge.
	require.NoError(t, markTransactionUtxoReferences(
		t.Context(), store.writeDB, []lcommon.TransactionInput{input},
		"collateral_by_tx_id", ownerA,
	))
	require.NoError(t, markTransactionUtxoReferences(
		t.Context(), store.writeDB, []lcommon.TransactionInput{input},
		"collateral_by_tx_id", ownerB,
	))
	got, err := store.collateralInputsBatch(
		t.Context(), store.writeDB, []any{ownerA, ownerB},
	)
	require.NoError(t, err)
	require.Len(t, got[string(ownerA)], 1)
	require.Len(t, got[string(ownerB)], 1)
	require.Equal(t, got[string(ownerA)][0].ID, got[string(ownerB)][0].ID)
	var count int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM utxo_collateral_input",
	).Scan(&count))
	require.Equal(t, 2, count)
	// Rollback cleanup is keyed by transaction, never by the shared UTxO.
	_, err = store.writeDB.Exec(
		"DELETE FROM utxo_collateral_input WHERE transaction_hash = ?", ownerA,
	)
	require.NoError(t, err)
	got, err = store.collateralInputsBatch(t.Context(), store.writeDB, []any{ownerB})
	require.NoError(t, err)
	require.Len(t, got[string(ownerB)], 1)
}

func TestLoadUtxoAssetsBatchPreservesGrouping(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	_, err := store.writeDB.Exec(`
CREATE TABLE asset (
 name BLOB, name_hex BLOB, policy_id BLOB, fingerprint BLOB,
 id INTEGER PRIMARY KEY, utxo_id INTEGER, amount TEXT
)`)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(`
INSERT INTO asset (name, name_hex, policy_id, fingerprint, id, utxo_id, amount)
VALUES ('a', '61', 'p', 'f1', 1, 10, '18446744073709551615'),
       ('b', '62', 'p', 'f2', 2, 20, '7')`)
	require.NoError(t, err)
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
	store := newTestStore(t)
	_, err := store.writeDB.Exec(`
CREATE TABLE asset (
 name BLOB, name_hex BLOB, policy_id BLOB, fingerprint BLOB,
 id INTEGER PRIMARY KEY, utxo_id INTEGER, amount TEXT
)`)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(`
INSERT INTO asset (name, name_hex, policy_id, fingerprint, id, utxo_id, amount)
VALUES ('a', '61', 'p', 'f1', 1, 10, '1')`)
	require.NoError(t, err)

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
