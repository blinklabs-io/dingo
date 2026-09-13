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

package conformance

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/require"
)

// newSqliteResetterTestDB creates a SQLite file with the given DDL applied and
// returns its path plus an open handle for assertions. The handle is separate
// from the resetter's own connection on purpose: the resetter holding its own
// pool for the manager's lifetime is the property being exercised.
func newSqliteResetterTestDB(t *testing.T, ddl ...string) (string, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(30000)")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	for _, stmt := range ddl {
		_, err := db.Exec(stmt)
		require.NoError(t, err, "apply ddl: %s", stmt)
	}
	return path, db
}

func sqliteRowCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	require.NoError(
		t,
		db.QueryRow(`SELECT COUNT(*) FROM "`+table+`"`).Scan(&n),
	)
	return n
}

// TestSqliteResetterTruncatesOnlyDirtyTables pins the same dirty-only
// contract the Postgres and MySQL resetters carry: a table nothing wrote to
// must not be touched, and the schema must survive.
func TestSqliteResetterTruncatesOnlyDirtyTables(t *testing.T) {
	path, db := newSqliteResetterTestDB(
		t,
		`CREATE TABLE dirty (id INTEGER PRIMARY KEY, v TEXT)`,
		`CREATE TABLE clean (id INTEGER PRIMARY KEY, v TEXT)`,
		`INSERT INTO dirty (v) VALUES ('x'), ('y')`,
	)

	resetter, err := newSqliteResetter(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resetter.Close() })

	// Record what truncate is actually asked to empty. Row counts alone
	// cannot show this: `clean` starts empty, so it reads 0 whether or not
	// the resetter touched it, and a regression that emptied every table
	// would pass on counts.
	var truncated []string
	inner := resetter.truncate
	resetter.truncate = func(
		ctx context.Context,
		db *sql.DB,
		qualified []string,
	) error {
		truncated = append(truncated, qualified...)
		return inner(ctx, db, qualified)
	}

	require.NoError(t, resetter.reset(context.Background()))

	require.Equal(
		t,
		[]string{`"dirty"`},
		truncated,
		"only the table holding rows may be truncated",
	)
	require.Equal(t, 0, sqliteRowCount(t, db, "dirty"))
	require.Equal(t, 0, sqliteRowCount(t, db, "clean"))

	// The schema must still exist: this path replaces a drop-and-recreate,
	// so a Reset that removed the tables would still read as "empty" here.
	var tables int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type='table' `+
			`AND name IN ('dirty','clean')`,
	).Scan(&tables))
	require.Equal(t, 2, tables, "reset must not drop the schema")
}

// TestSqliteResetterResetsAutoincrement is the reason this cannot be a plain
// DELETE. The close-and-reopen path this replaces recreated the file, so every
// AUTOINCREMENT counter restarted at 1; DELETE alone leaves sqlite_sequence
// intact and the next insert would continue from the previous vector's high
// water mark.
func TestSqliteResetterResetsAutoincrement(t *testing.T) {
	path, db := newSqliteResetterTestDB(
		t,
		`CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`,
		`INSERT INTO t (v) VALUES ('a'), ('b'), ('c')`,
	)

	var before int
	require.NoError(t, db.QueryRow(`SELECT MAX(id) FROM t`).Scan(&before))
	require.Equal(t, 3, before)

	resetter, err := newSqliteResetter(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resetter.Close() })
	require.NoError(t, resetter.reset(context.Background()))

	_, err = db.Exec(`INSERT INTO t (v) VALUES ('fresh')`)
	require.NoError(t, err)

	var after int
	require.NoError(t, db.QueryRow(`SELECT id FROM t`).Scan(&after))
	require.Equal(
		t,
		1,
		after,
		"AUTOINCREMENT must restart at 1, as the recreate path gave",
	)
}

// TestSqliteResetterClearsAutoincrementOfEmptiedTable covers the case the
// dirty-only optimization creates: a table emptied by an earlier reset holds
// no rows, so the row probe will not report it, yet its sqlite_sequence entry
// still advances the next insert. MySQL solves the same problem with
// extraDirty; SQLite must too.
func TestSqliteResetterClearsAutoincrementOfEmptiedTable(t *testing.T) {
	path, db := newSqliteResetterTestDB(
		t,
		`CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`,
		`INSERT INTO t (v) VALUES ('a'), ('b')`,
		// Emptied by hand, leaving sqlite_sequence advanced: exactly the
		// state a prior vector's reset would leave if only rows counted.
		`DELETE FROM t`,
	)

	var seq int
	require.NoError(t, db.QueryRow(
		`SELECT seq FROM sqlite_sequence WHERE name='t'`,
	).Scan(&seq))
	require.Equal(t, 2, seq, "precondition: sequence is advanced")

	resetter, err := newSqliteResetter(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resetter.Close() })
	require.NoError(t, resetter.reset(context.Background()))

	_, err = db.Exec(`INSERT INTO t (v) VALUES ('fresh')`)
	require.NoError(t, err)
	var after int
	require.NoError(t, db.QueryRow(`SELECT id FROM t`).Scan(&after))
	require.Equal(t, 1, after, "a row-empty table's sequence must still reset")
}

// TestSqliteResetterExcludesInternalAndMigrationTables keeps the resetter off
// SQLite's own bookkeeping and off the migration runner's ledger. Truncating
// schema_migrations would make the next construction re-run every migration,
// which is the cost this whole path exists to avoid.
func TestSqliteResetterExcludesInternalAndMigrationTables(t *testing.T) {
	path, db := newSqliteResetterTestDB(
		t,
		`CREATE TABLE schema_migrations (version TEXT PRIMARY KEY)`,
		`CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT)`,
		`INSERT INTO schema_migrations (version) VALUES ('v1')`,
		`INSERT INTO t (v) VALUES ('a')`,
	)

	resetter, err := newSqliteResetter(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resetter.Close() })

	// Both excluded names must actually exist in this database, or
	// NotContains below would pass for the wrong reason.
	for _, name := range []string{"schema_migrations", "sqlite_sequence"} {
		var found string
		require.NoError(
			t,
			db.QueryRow(
				`SELECT name FROM sqlite_master WHERE type='table' AND name=?`,
				name,
			).Scan(&found),
			"precondition: %s must exist to be meaningfully excluded", name,
		)
	}

	tables, err := resetter.cachedTables(context.Background())
	require.NoError(t, err)
	require.Contains(t, tables, "t")
	require.NotContains(t, tables, "schema_migrations")
	require.NotContains(t, tables, "sqlite_sequence")

	require.NoError(t, resetter.reset(context.Background()))
	require.Equal(
		t,
		1,
		sqliteRowCount(t, db, "schema_migrations"),
		"migration ledger must survive a reset",
	)
	require.Equal(t, 0, sqliteRowCount(t, db, "t"))
}

// TestSqliteResetterClearsDespiteForeignKeys pins deletion working regardless
// of parent/child ordering. Postgres gets this from TRUNCATE ... CASCADE;
// SQLite has no CASCADE on DELETE, so the resetter's own connection must not
// enforce foreign keys.
func TestSqliteResetterClearsDespiteForeignKeys(t *testing.T) {
	// Names chosen so listSqliteConformanceTables' ORDER BY name yields the
	// referenced table first: deleting a_parent while z_child still
	// references it is a foreign-key violation on an enforcing connection.
	// With the natural names the order is child-then-parent, which happens
	// not to violate anything and would make this test prove nothing.
	path, db := newSqliteResetterTestDB(
		t,
		`CREATE TABLE a_parent (id INTEGER PRIMARY KEY, v TEXT)`,
		`CREATE TABLE z_child (
			id INTEGER PRIMARY KEY,
			parent_id INTEGER NOT NULL REFERENCES a_parent(id)
		)`,
		`INSERT INTO a_parent (id, v) VALUES (1, 'p')`,
		`INSERT INTO z_child (id, parent_id) VALUES (1, 1)`,
	)

	resetter, err := newSqliteResetter(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resetter.Close() })

	require.NoError(t, resetter.reset(context.Background()))
	require.Equal(t, 0, sqliteRowCount(t, db, "a_parent"))
	require.Equal(t, 0, sqliteRowCount(t, db, "z_child"))
}

// TestSqliteResetterSkipsEntirelyWhenClean pins the no-op case: a run whose
// previous vector wrote nothing must issue no DELETE at all. This is what
// keeps the cost proportional to what a vector wrote.
func TestSqliteResetterSkipsEntirelyWhenClean(t *testing.T) {
	path, _ := newSqliteResetterTestDB(
		t,
		`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`,
	)
	resetter, err := newSqliteResetter(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resetter.Close() })

	var truncated [][]string
	inner := resetter.truncate
	resetter.truncate = func(
		ctx context.Context,
		db *sql.DB,
		qualified []string,
	) error {
		truncated = append(truncated, qualified)
		return inner(ctx, db, qualified)
	}

	require.NoError(t, resetter.reset(context.Background()))
	require.Empty(t, truncated, "a clean database must issue no DELETE")
}
