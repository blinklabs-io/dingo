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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

type utxoAddressPosition struct {
	TxID       int64
	OutputIdx  int64
	Slot       int64
	BlockIndex int64
}

func readUtxoAddressPosition(
	t *testing.T,
	store *Store,
	utxoID int64,
) utxoAddressPosition {
	t.Helper()
	var pos utxoAddressPosition
	require.NoError(t, store.writeDB.QueryRowContext(
		context.Background(),
		`SELECT tx_id, output_idx, slot, block_index
FROM utxo_address WHERE utxo_id = ?`, utxoID,
	).Scan(&pos.TxID, &pos.OutputIdx, &pos.Slot, &pos.BlockIndex))
	return pos
}

// seedProducedOutput stores a transaction applied at slot/blockIndex with one
// output at outputIdx, as the writer has it when it queues the address row.
func seedProducedOutput(
	t *testing.T,
	store *Store,
	txID, slot, blockIndex, outputIdx int64,
	appliedRow bool,
) int64 {
	t.Helper()
	ctx := context.Background()
	_, err := store.writeDB.ExecContext(ctx,
		`INSERT INTO "transaction" (id, hash) VALUES (?, ?)`,
		txID, []byte{byte(txID), 1, 2, 3},
	)
	require.NoError(t, err)
	if appliedRow {
		_, err = store.writeDB.ExecContext(ctx,
			`INSERT INTO applied_transaction (tx_id, slot, block_index)
VALUES (?, ?, ?)`, txID, slot, blockIndex,
		)
		require.NoError(t, err)
	}
	result, err := store.writeDB.ExecContext(ctx,
		`INSERT INTO utxo (tx_id, output_idx, amount, added_slot, deleted_slot)
VALUES (?, ?, '1', ?, 0)`, txID, outputIdx, slot,
	)
	require.NoError(t, err)
	id, err := result.LastInsertId()
	require.NoError(t, err)
	return id
}

func tableColumnNames(t *testing.T, store *Store, table string) []string {
	t.Helper()
	rows, err := store.writeDB.Query(
		`SELECT name FROM pragma_table_info(?) ORDER BY cid`, table,
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	return names
}

func indexColumnNames(t *testing.T, store *Store, index string) []string {
	t.Helper()
	rows, err := store.writeDB.Query(
		`SELECT name FROM pragma_index_info(?) ORDER BY seqno`, index,
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	return names
}

func tableIndexNames(t *testing.T, store *Store, table string) []string {
	t.Helper()
	rows, err := store.writeDB.Query(
		`SELECT name FROM sqlite_master
WHERE type = 'index' AND tbl_name = ? AND sql IS NOT NULL ORDER BY name`,
		table,
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	return names
}

func TestUtxoAddressReferencesDictionaryByID(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	require.NotContains(
		t, tableColumnNames(t, store, "utxo_address"), "address",
		"utxo_address repeats the address by value on every row",
	)
	require.Equal(
		t, []string{"idx_utxo_address_order"},
		tableIndexNames(t, store, "utxo_address"),
		"the order index is the only index on utxo_address",
	)
	require.Equal(
		t,
		[]string{
			"address_id", "slot", "block_index", "output_idx", "tx_id",
		},
		indexColumnNames(t, store, "idx_utxo_address_order"),
	)
}

func TestSetUtxoAddressSharesOneDictionaryRowPerAddress(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	first := seedProducedOutput(t, store, 1, 100, 0, 0, true)
	second := seedProducedOutput(t, store, 2, 101, 0, 0, true)
	other := seedProducedOutput(t, store, 3, 102, 0, 0, true)
	shared := []byte("addr_test1shared")
	require.NoError(t, store.SetUtxoAddress(uint(first), shared, nil))
	require.NoError(t, store.SetUtxoAddress(uint(second), shared, nil))
	require.NoError(t, store.SetUtxoAddress(
		uint(other), []byte("addr_test1other"), nil,
	))

	var distinct, ids int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM address`).Scan(&distinct))
	require.Equal(t, 2, distinct, "one dictionary row per distinct address")
	require.NoError(t, store.writeDB.QueryRow(`
SELECT COUNT(DISTINCT ua.address_id)
FROM utxo_address ua JOIN address a ON a.id = ua.address_id
WHERE ua.utxo_id IN (?, ?) AND a.address = ?`, first, second, shared,
	).Scan(&ids))
	require.Equal(t, 1, ids, "outputs at one address share its id")

	balance, err := store.GetUtxoBalanceByAddressAtSlot(
		shared, 1000, 0, nil,
	)
	require.NoError(t, err)
	require.Equal(t, int64(2), balance.UtxoCount)
	missing, err := store.GetUtxoBalanceByAddressAtSlot(
		[]byte("addr_test1never"), 1000, 0, nil,
	)
	require.NoError(t, err)
	require.Zero(t, missing.UtxoCount)
}

func TestUtxoAddressWriteReplacesAddressOnRewrite(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	id := seedProducedOutput(t, store, 1, 100, 0, 0, true)
	require.NoError(t, store.SetUtxoAddress(uint(id), []byte("addr_a"), nil))
	require.NoError(t, store.SetUtxoAddress(uint(id), []byte("addr_b"), nil))
	var got []byte
	require.NoError(t, store.writeDB.QueryRow(`
SELECT a.address FROM utxo_address ua
JOIN address a ON a.id = ua.address_id WHERE ua.utxo_id = ?`, id,
	).Scan(&got))
	require.Equal(t, []byte("addr_b"), got)
}

// TestRolledBackAddressWriteLeavesNoDictionaryRow pins the rollback rule: an
// address id is resolved inside the writing transaction, so a rolled back
// write must not leave an id that a later write would reuse for another
// address.
func TestRolledBackAddressWriteLeavesNoDictionaryRow(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	id := seedProducedOutput(t, store, 1, 100, 0, 0, true)
	txn := store.Transaction(t.Context())
	require.NoError(t, store.SetUtxoAddress(uint(id), []byte("addr_gone"), txn))
	require.NoError(t, txn.Rollback())
	require.NoError(t, store.SetUtxoAddress(uint(id), []byte("addr_kept"), nil))

	var count int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM address WHERE address = ?`,
		[]byte("addr_gone"),
	).Scan(&count))
	require.Zero(t, count)
	var got []byte
	require.NoError(t, store.writeDB.QueryRow(`
SELECT a.address FROM utxo_address ua
JOIN address a ON a.id = ua.address_id WHERE ua.utxo_id = ?`, id,
	).Scan(&got))
	require.Equal(t, []byte("addr_kept"), got)
}

// TestAddressPageUsesOrderIndex pins the plan of the exact-address page and
// count: the address id is looked up in the dictionary once and the page is
// read from idx_utxo_address_order without a sort.
func TestAddressPageUsesOrderIndex(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	query := &models.UtxoWithOrderingQuery{
		IndexedExactAddress: []byte("addr_test1plan"),
		Limit:               10,
	}
	from, predicate, args, err := utxoOrderingClauses(query, true)
	require.NoError(t, err)
	statement, args := utxoOrderingStatement(query, from, predicate, args, nil)
	plan := queryPlan(t, store.writeDB, statement, args...)
	require.Contains(t, plan, "idx_utxo_address_order", plan)
	require.False(t, strings.Contains(plan, "TEMP B-TREE"), plan)
	require.Contains(t, plan, "address_id=?", plan)
}

// TestOrderedUtxoAddressRowIsWrittenOnce counts the UPDATE statements that
// touch utxo_address while a flush writes an output whose position is known:
// the row must take its address id and position from its one INSERT.
func TestOrderedUtxoAddressRowIsWrittenOnce(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	ctx := context.Background()
	_, err := store.writeDB.ExecContext(ctx, `
CREATE TABLE utxo_address_updates (utxo_id INTEGER);
CREATE TRIGGER utxo_address_updated AFTER UPDATE ON utxo_address
BEGIN INSERT INTO utxo_address_updates VALUES (NEW.utxo_id); END;`)
	require.NoError(t, err)
	id := seedProducedOutput(t, store, 31, 500, 4, 1, true)
	txID := uint(31)
	blockIndex := uint32(4)
	var rows rowBatch
	queueUtxoAddress(&rows, id, []byte("addr_test1once"), &models.Utxo{
		TransactionID: &txID, OutputIdx: 1, AddedSlot: 500,
		BlockIndex: &blockIndex,
	})
	require.NoError(t, rows.flush(ctx, store.writeDB, store, 500))

	var updates int
	require.NoError(t, store.writeDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM utxo_address_updates`).Scan(&updates))
	require.Zero(t, updates, "the row is written by its INSERT alone")
	require.Equal(
		t, utxoAddressPosition{TxID: 31, OutputIdx: 1, Slot: 500, BlockIndex: 4},
		readUtxoAddressPosition(t, store, id),
	)
}
