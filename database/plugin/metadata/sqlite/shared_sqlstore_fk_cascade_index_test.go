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

package sqlite

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// utxoCascadeDeleteSQL is the statement SQLite runs to enforce
// fk_transaction_outputs, once for every "transaction" row the rollback
// deletes.
//
// It appears nowhere in the store: ON DELETE CASCADE is an action the engine
// takes, not a statement the code issues. It is spelled out here because that
// action is the whole cost of a deep rollback and because it is invisible from
// the parent statement -- EXPLAIN QUERY PLAN of
// DELETE FROM "transaction" WHERE slot > ? reports only the indexed search
// over "transaction" and says nothing about the child table it cascades into.
const utxoCascadeDeleteSQL = `DELETE FROM utxo WHERE transaction_id = ?`

// cascadeForeignKey is one ON DELETE CASCADE foreign key as the live schema
// declares it.
type cascadeForeignKey struct {
	table  string
	column string
	parent string
}

func (fk cascadeForeignKey) String() string {
	return fmt.Sprintf("%s.%s -> %s", fk.table, fk.column, fk.parent)
}

// newCoreModeSQLStore opens a core-mode store on its own data directory. Core
// is the mode a block producer or relay runs, and the mode the rollback sweep
// that motivates these tests runs in.
func newCoreModeSQLStore(t *testing.T) (*sqlstore.Store, *sql.DB) {
	t.Helper()
	store, writeDB, _, err := openSQLStore(
		Config{DataDir: t.TempDir()},
		metadata.ProviderDependencies{StorageMode: types.StorageModeCore},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() {
		// Logged rather than asserted: require calls FailNow, which stops
		// the remaining cleanup callbacks and leaks the t.TempDir removal
		// registered before this one.
		if err := store.Close(); err != nil {
			t.Logf("closing store: %v", err)
		}
	})
	return store, writeDB
}

// cascadeForeignKeys reads every ON DELETE CASCADE foreign key out of the
// schema the migrations produced, rather than restating a list that a later
// migration could add to without anyone revisiting this file.
func cascadeForeignKeys(t *testing.T, db *sql.DB) []cascadeForeignKey {
	t.Helper()
	rows, err := db.Query(`
SELECT m.name, fk."from", fk."table"
FROM sqlite_master m
JOIN pragma_foreign_key_list(m.name) fk
WHERE m.type = 'table' AND fk.on_delete = 'CASCADE'
ORDER BY m.name, fk."from"`)
	require.NoError(t, err)
	defer rows.Close()
	var out []cascadeForeignKey
	for rows.Next() {
		var fk cascadeForeignKey
		require.NoError(t, rows.Scan(&fk.table, &fk.column, &fk.parent))
		out = append(out, fk)
	}
	require.NoError(t, rows.Err())
	require.NotEmpty(
		t,
		out,
		"the schema must declare cascading foreign keys for this to test "+
			"anything",
	)
	return out
}

// coveringIndexes names the indexes whose leftmost column is column, which are
// the ones SQLite can use to answer the cascade's lookup. An index that
// mentions the column in any later position cannot.
func coveringIndexes(
	t *testing.T,
	db *sql.DB,
	table, column string,
) []string {
	t.Helper()
	rows, err := db.Query(`
SELECT il.name
FROM pragma_index_list(?) il
JOIN pragma_index_info(il.name) ii ON ii.seqno = 0
WHERE ii.name = ?`, table, column)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		out = append(out, name)
	}
	require.NoError(t, rows.Err())
	return out
}

// requireCascadeChildColumnsIndexed asserts that every cascading foreign key
// can be enforced by an index lookup rather than a scan of the child table.
func requireCascadeChildColumnsIndexed(
	t *testing.T,
	db *sql.DB,
	fks []cascadeForeignKey,
	when string,
) {
	t.Helper()
	for _, fk := range fks {
		require.NotEmpty(
			t,
			coveringIndexes(t, db, fk.table, fk.column),
			"%s: %s is ON DELETE CASCADE with no index on the child "+
				"column, so deleting one parent row scans all of %s",
			when,
			fk,
			fk.table,
		)
	}
}

// TestCascadeChildColumnsIndexedAfterCriticalRebuild is the manifest
// classification this file exists for.
//
// mithril/sync.go rebuilds only the critical subset of the deferred-index
// manifest before it marks the database ready, and the rollback sweep the node
// runs from its first reconciliation onward deletes "transaction" rows whose
// children cascade. A cascading child column left without an index at that
// point turns each deleted parent row into a full scan of the child table:
// measured on a preview relay, one 1,001-transaction rollback against a 3.2M
// row utxo table took 556s with idx_utxo_transaction_id absent and 0.095s with
// it present.
//
// The invariant is asserted over the schema's own foreign keys rather than
// over the one index that motivated it, so a new cascading foreign key whose
// child index is classified lazy fails here instead of in production.
func TestCascadeChildColumnsIndexedAfterCriticalRebuild(t *testing.T) {
	t.Parallel()
	store, db := newCoreModeSQLStore(t)
	fks := cascadeForeignKeys(t, db)
	require.Contains(
		t,
		fks,
		cascadeForeignKey{
			table:  "utxo",
			column: "transaction_id",
			parent: "transaction",
		},
		"the rollback's DELETE FROM \"transaction\" must still cascade "+
			"into utxo for this test to cover the sweep it describes",
	)
	requireCascadeChildColumnsIndexed(t, db, fks, "before deferring indexes")

	// The Mithril bootstrap sequence: drop the manifest for the bulk load,
	// then rebuild only the critical subset before the database is marked
	// ready. The lazy remainder is finished by later maintenance, and on a
	// database whose pending marker the sync's own ClearSyncState wiped,
	// never — so whatever the rollback path needs has to be in this
	// subset.
	require.NoError(t, store.DropDeferredIndexes())
	require.NoError(t, store.BuildCriticalDeferredIndexes())

	requireCascadeChildColumnsIndexed(
		t,
		db,
		fks,
		"after BuildCriticalDeferredIndexes",
	)
}

// seedUtxoRows adds count transactions, each owning two utxo rows, and runs
// ANALYZE so the planner assertions below see the statistics a live database
// has.
//
// Written as set-based SQL rather than through SetTransaction because the
// planner only needs representative table shapes, not faithful transaction
// contents.
func seedUtxoRows(t *testing.T, db *sql.DB, count int) {
	t.Helper()
	_, err := db.Exec(`
WITH RECURSIVE seq(n) AS (
    SELECT 0
    UNION ALL
    SELECT n + 1 FROM seq WHERE n + 1 < ?
)
INSERT INTO "transaction" (
    hash, block_hash, slot, type, fee, collateral_fee, ttl, block_index, valid
)
SELECT CAST(n AS BLOB), CAST(n AS BLOB), n, 0, '0', '0', '0', 0, TRUE
FROM seq`, count)
	require.NoError(t, err)
	for _, outputIdx := range []int{0, 1} {
		_, err := db.Exec(`
INSERT INTO utxo (
    transaction_id, tx_id, output_idx, payment_key, staking_key,
    credential_tag, amount, added_slot, deleted_slot
)
SELECT id, hash, ?, hash, hash, 0, '1000000', slot, 0
FROM "transaction"`, outputIdx)
		require.NoError(t, err)
	}
	_, err = db.Exec("ANALYZE")
	require.NoError(t, err)
}

// TestUtxoCascadeDeleteIndexedAfterCriticalRebuild pins the plan of the
// cascade itself.
//
// The negative case is asserted first: with the manifest dropped, the cascade
// scans utxo. Without it, a rebuild that restored nothing would still satisfy
// the positive assertion if the index had never been missing.
func TestUtxoCascadeDeleteIndexedAfterCriticalRebuild(t *testing.T) {
	t.Parallel()
	store, db := newCoreModeSQLStore(t)
	seedUtxoRows(t, db, 2000)

	require.NoError(t, store.DropDeferredIndexes())
	plan := queryPlan(t, db, utxoCascadeDeleteSQL, 1)
	require.Contains(
		t,
		plan,
		"SCAN utxo",
		"the dropped manifest must leave the cascade unindexed for this "+
			"test to have teeth:\n%s",
		plan,
	)

	require.NoError(t, store.BuildCriticalDeferredIndexes())
	plan = queryPlan(t, db, utxoCascadeDeleteSQL, 1)
	require.Contains(
		t,
		plan,
		"SEARCH utxo USING",
		"the rollback cascade must be an indexed search after the "+
			"critical rebuild:\n%s",
		plan,
	)
	require.Contains(
		t,
		plan,
		"INDEX idx_utxo_transaction_id (transaction_id=?)",
		"the cascade must resolve transaction_id through "+
			"idx_utxo_transaction_id:\n%s",
		plan,
	)
	require.NotContains(
		t,
		plan,
		"SCAN utxo",
		"the cascade must not scan utxo:\n%s",
		plan,
	)
}

// TestRollbackDeleteCascadesIntoUtxo grounds the plan assertion above: the
// cascade is on the rollback's path, so the index the manifest classifies is
// the one the rollback sweep depends on.
//
// Reproduces database/plugin/metadata/sqlstore/transaction_write.go's
// DELETE FROM "transaction" WHERE slot > ?, on a connection carrying the
// foreign_keys(1) pragma the store opens every connection with.
func TestRollbackDeleteCascadesIntoUtxo(t *testing.T) {
	t.Parallel()
	_, db := newCoreModeSQLStore(t)
	seedUtxoRows(t, db, 200)

	var enforced int
	require.NoError(
		t,
		db.QueryRow("PRAGMA foreign_keys").Scan(&enforced),
	)
	require.Equal(
		t,
		1,
		enforced,
		"the store opens every connection with foreign_keys(1); without "+
			"it the cascade this file measures would not run at all",
	)

	var before int
	require.NoError(
		t,
		db.QueryRow("SELECT COUNT(*) FROM utxo WHERE added_slot > 99").
			Scan(&before),
	)
	require.Positive(t, before)

	_, err := db.Exec(`DELETE FROM "transaction" WHERE slot > ?`, 99)
	require.NoError(t, err)

	var after int
	require.NoError(
		t,
		db.QueryRow("SELECT COUNT(*) FROM utxo WHERE added_slot > 99").
			Scan(&after),
	)
	require.Zero(
		t,
		after,
		"deleting the parent transactions must cascade into utxo",
	)
}
