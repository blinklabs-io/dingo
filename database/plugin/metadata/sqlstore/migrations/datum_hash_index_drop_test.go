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

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

func datumQueryPlan(t *testing.T, db *sql.DB, query string) string {
	t.Helper()
	rows, err := db.Query("EXPLAIN QUERY PLAN "+query, []byte{1, 2, 3})
	require.NoError(t, err)
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan = append(plan, detail)
	}
	require.NoError(t, rows.Err())
	return strings.Join(plan, "\n")
}

// TestDatumHashDuplicateIndexDrop upgrades a v62 database that holds datum
// rows and requires idx_datum_hash gone, the unique constraint's index and
// idx_datum_added_slot kept, every hash lookup planned on the unique index,
// the rows untouched, and a replay of the migration to be a no-op.
func TestDatumHashDuplicateIndexDrop(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, path, 62)
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	_, ok := indexSQL(t, db, "idx_datum_hash")
	require.True(t, ok, "v62 must still carry idx_datum_hash")
	for i := range 20 {
		_, err = db.Exec(
			`INSERT INTO datum (hash, raw_datum, added_slot) VALUES (?, ?, ?)`,
			[]byte{byte(i), 9, 9}, []byte{byte(i)}, 100+i,
		)
		require.NoError(t, err)
	}
	var before int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM datum`).Scan(&before))

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	run := func() {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: registry,
			Locker:   migrations.NewFileLocker(path + ".migrate.lock"),
		}
		require.NoError(t, runner.Run(context.Background()))
	}
	run()

	_, ok = indexSQL(t, db, "idx_datum_hash")
	require.False(t, ok, "idx_datum_hash must not survive the migration")
	_, ok = indexSQL(t, db, "idx_datum_added_slot")
	require.True(t, ok, "idx_datum_added_slot must survive")
	var autoindexes int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM sqlite_master
WHERE type = 'index' AND name = 'sqlite_autoindex_datum_1'`).Scan(&autoindexes))
	require.Equal(t, 1, autoindexes, "the unique constraint's index must remain")

	for _, query := range []string{
		`SELECT raw_datum FROM datum WHERE hash = ?`,
		`SELECT d.raw_datum FROM utxo u
LEFT JOIN datum d ON d.hash = u.datum_hash WHERE u.datum_hash = ?`,
	} {
		require.Contains(t, datumQueryPlan(t, db, query),
			"sqlite_autoindex_datum_1", query)
	}
	_, err = db.Exec(`INSERT INTO datum (hash, raw_datum, added_slot)
VALUES (?, ?, 1) ON CONFLICT (hash) DO NOTHING`, []byte{0, 9, 9}, []byte{7})
	require.NoError(t, err)
	var after int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM datum`).Scan(&after))
	require.Equal(t, before, after)

	_, err = db.Exec(`UPDATE schema_migrations
SET phase = 'expand', dirty = true, completed_at = NULL WHERE version = 63`)
	require.NoError(t, err)
	run()
	_, ok = indexSQL(t, db, "idx_datum_hash")
	require.False(t, ok)
}
