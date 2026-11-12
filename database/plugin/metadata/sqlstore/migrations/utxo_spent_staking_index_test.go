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

package migrations_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

// utxoSpentStakingIndexVersion is the schema version that makes
// idx_utxo_deleted_staking_amount cover spent rows only.
const utxoSpentStakingIndexVersion = 60

// utxoSpentStakingIndexDB returns a database migrated through the version
// before the index became partial, holding live outputs with and without a
// stake credential and spent ones, and a function that runs the remaining
// versions.
func utxoSpentStakingIndexDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(registry), utxoSpentStakingIndexVersion)
	runTo := func(registry []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: registry,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		require.NoError(t, runner.Run(context.Background()))
	}
	runTo(registry[:utxoSpentStakingIndexVersion-1])
	for i, row := range []struct {
		deletedSlot int64
		script      bool
		staking     []byte
	}{
		{0, false, nil},
		{0, false, []byte{1}},
		{0, true, []byte{2}},
		{7, false, []byte{1}},
		{9, true, []byte{2}},
	} {
		_, err := db.Exec(
			"INSERT INTO utxo (tx_id, output_idx, credential_tag, "+
				"staking_key, added_slot, deleted_slot, amount, "+
				"payment_script) VALUES (?, 0, 0, ?, 1, ?, ?, ?)",
			[]byte{byte(i)}, row.staking, row.deletedSlot, "100", row.script,
		)
		require.NoError(t, err)
	}
	return db, func() { runTo(registry) }
}

func utxoRowsDigest(t *testing.T, db *sql.DB) string {
	t.Helper()
	var digest string
	require.NoError(t, db.QueryRow(
		"SELECT group_concat(hex(tx_id) || ':' || deleted_slot || ':' || "+
			"payment_script || ':' || amount || ':' || "+
			"coalesce(hex(staking_key), ''), ',') "+
			"FROM (SELECT * FROM utxo ORDER BY id)",
	).Scan(&digest))
	return digest
}

// indexEntryCount counts the rows an index holds from its own b-tree. A
// partial index cannot answer a query that does not imply its predicate, so
// the count reads the entries the b-tree stores rather than a table scan.
func indexEntryCount(t *testing.T, db *sql.DB, index string) int {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow(
		"SELECT ncell FROM dbstat WHERE name = ? AND aggregate = TRUE",
		index,
	).Scan(&count))
	return count
}

func TestUtxoSpentStakingIndexBecomesPartial(t *testing.T) {
	t.Parallel()
	db, upgrade := utxoSpentStakingIndexDB(t)
	const name = "idx_utxo_deleted_staking_amount"
	definition, ok := indexSQL(t, db, name)
	require.True(t, ok)
	require.NotContains(t, definition, "WHERE")
	require.Equal(t, 5, indexEntryCount(t, db, name))
	supplyBefore, ok := indexSQL(t, db, "idx_utxo_deleted_payment_script")
	require.True(t, ok)
	before := utxoRowsDigest(t, db)

	upgrade()

	definition, ok = indexSQL(t, db, name)
	require.True(t, ok)
	require.Contains(t, definition, "(deleted_slot, credential_tag, "+
		"staking_key, amount)")
	require.Contains(t, definition, "WHERE deleted_slot > 0")
	require.Equal(t, 2, indexEntryCount(t, db, name), "only the spent rows")
	require.Equal(t, before, utxoRowsDigest(t, db),
		"the upgrade must not change stored ledger state")
	supplyAfter, ok := indexSQL(t, db, "idx_utxo_deleted_payment_script")
	require.True(t, ok)
	require.Equal(t, supplyBefore, supplyAfter,
		"the script-supply index is not part of this upgrade")

	// The statement below is the one the rollback sweep issues; the rebuilt
	// index must still answer it.
	var detail string
	rows, err := db.Query(
		"EXPLAIN QUERY PLAN SELECT credential_tag, staking_key FROM utxo " +
			"WHERE deleted_slot > 5 AND deleted_slot > 0",
	)
	require.NoError(t, err)
	for rows.Next() {
		var id, parent, unused int
		var line string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &line))
		detail += line + "\n"
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Contains(t, detail, name)
}

// While a bulk-load rebuild is pending, the deferred rebuild owns this index.
// The upgrade must not build it early, and must not leave the full index
// behind for the rebuild to skip as already present.
func TestUtxoSpentStakingIndexLeftToPendingRebuild(t *testing.T) {
	t.Parallel()
	db, upgrade := utxoSpentStakingIndexDB(t)
	_, err := db.Exec(
		"INSERT INTO sync_state (sync_key, value) VALUES (?, ?)",
		deferred.SyncStateKey,
		deferred.SyncStateValue,
	)
	require.NoError(t, err)

	upgrade()

	_, ok := indexSQL(t, db, "idx_utxo_deleted_staking_amount")
	require.False(t, ok, "the index must be left to the deferred rebuild")
	_, ok = indexSQL(t, db, "idx_utxo_deleted_payment_script")
	require.True(t, ok, "the script-supply index is not part of this upgrade")
}

// MySQL has no partial index, so v60 replaces the four-column index with one
// on deleted_slot alone; the other engines run the partial-index backfill.
func TestUtxoSpentStakingIndexMigrationPerDialect(t *testing.T) {
	t.Parallel()
	mysql, err := migrations.MySQLRegistry()
	require.NoError(t, err)
	postgres, err := migrations.PostgresRegistry()
	require.NoError(t, err)
	sqlite, err := migrations.SQLiteRegistry()
	require.NoError(t, err)

	mysqlMigration := mysql[utxoSpentStakingIndexVersion-1]
	require.Equal(t, "utxo-spent-staking-index", mysqlMigration.Name)
	require.Nil(t, mysqlMigration.Backfill)
	require.Equal(t, []string{
		"DROP INDEX `idx_utxo_deleted_staking_amount` ON `utxo`",
		"CREATE INDEX `idx_utxo_deleted_staking_amount` ON `utxo`(`deleted_slot`)",
	}, mysqlMigration.SQL["mysql"].Expand)
	for _, statement := range mysqlMigration.SQL["mysql"].Expand {
		require.False(t, strings.Contains(strings.ToUpper(statement), "WHERE"),
			"MySQL cannot hold a partial index: %s", statement)
	}

	for _, registry := range [][]migrations.Migration{postgres, sqlite} {
		migration := registry[utxoSpentStakingIndexVersion-1]
		require.Equal(t, "utxo-spent-staking-index", migration.Name)
		require.NotNil(t, migration.Backfill)
	}
}
