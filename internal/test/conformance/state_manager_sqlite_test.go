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
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// sqliteFileIdentity returns the metadata database's file identity, which
// distinguishes "emptied in place" from "deleted and recreated". The
// close-and-reopen path this change replaces removed the whole data directory
// on every Reset, so it produced a different file each time.
//
// os.SameFile rather than a syscall.Stat_t inode: Stat_t does not exist on
// Windows, and this package is built untagged by the Windows CI job.
func sqliteFileIdentity(t *testing.T, dataDir string) os.FileInfo {
	t.Helper()
	info, err := os.Stat(filepath.Join(dataDir, sqliteMetadataFileName))
	require.NoError(t, err, "metadata database must exist after reset")
	return info
}

// TestSqliteResetKeepsTheSameDatabaseFile is the regression test for the whole
// change. Reset must empty the backend in place rather than delete the data
// directory and rebuild it, because rebuilding re-runs every migration -- 260
// DDL statements, once per vector, ~315 vectors per replay.
//
// A different file after Reset means the recreate path ran.
func TestSqliteResetKeepsTheSameDatabaseFile(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := newDingoStateManagerAt(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })

	before := sqliteFileIdentity(t, dataDir)
	require.NoError(t, sm.Reset())
	after := sqliteFileIdentity(t, dataDir)

	require.True(
		t,
		os.SameFile(before, after),
		"Reset must empty the metadata database in place, not recreate it",
	)
}

// TestSqliteResetPreservesMigrationLedger pins the consequence that makes the
// in-place reset worth doing: schema_migrations survives, so the next
// construction has nothing to re-apply.
func TestSqliteResetPreservesMigrationLedger(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := newDingoStateManagerAt(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })

	resetter, err := newSqliteResetter(sqliteMetadataPath(dataDir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = resetter.Close() })

	var before int
	require.NoError(
		t,
		resetter.db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).
			Scan(&before),
	)
	require.Positive(t, before, "precondition: migrations were recorded")

	require.NoError(t, sm.Reset())

	var after int
	require.NoError(
		t,
		resetter.db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).
			Scan(&after),
	)
	require.Equal(t, before, after, "migration ledger must survive Reset")
}

// TestSqliteResetClearsBlobStore pins the half of Reset that the metadata
// truncate does not cover. The previous path cleared blobs incidentally, by
// removing the whole data directory; emptying only the metadata tables would
// silently leave a prior vector's UTxO CBOR readable.
func TestSqliteResetClearsBlobStore(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := newDingoStateManagerAt(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })

	key := []byte("conformance-reset-probe")
	value := []byte("stale vector bytes")

	blobStore := sm.db.Blob()
	require.NotNil(t, blobStore)
	// Registered before the assertions below: a failing Set or Commit would
	// otherwise abort the test with badger's single write transaction still
	// open, and every later blob write in this process would block on it.
	// Rollback after a successful Commit is a no-op.
	txn := blobStore.NewTransaction(true)
	t.Cleanup(func() { _ = txn.Rollback() })
	require.NoError(t, blobStore.Set(txn, key, value))
	require.NoError(t, txn.Commit())

	// Confirm it is actually readable before Reset, so a broken write cannot
	// make the post-Reset absence look like success.
	readTxn := sm.db.Blob().NewTransaction(false)
	t.Cleanup(func() { _ = readTxn.Rollback() })
	got, err := sm.db.Blob().Get(readTxn, key)
	require.NoError(t, err)
	require.Equal(t, value, got, "precondition: blob is readable")

	require.NoError(t, sm.Reset())

	afterTxn := sm.db.Blob().NewTransaction(false)
	t.Cleanup(func() { _ = afterTxn.Rollback() })
	_, err = sm.db.Blob().Get(afterTxn, key)
	require.Error(t, err, "Reset must clear the blob store")
}

// TestResetRunsEachWipeHookIndependently pins that Reset checks wipeMetadata
// and wipeBlob separately. They were nested, so a backend that set only
// wipeBlob would have silently skipped emptying its blob store; no backend
// does that today, which is exactly why the contract needs a test rather than
// a convention.
func TestResetRunsEachWipeHookIndependently(t *testing.T) {
	for _, tc := range []struct {
		name           string
		metadata, blob bool
	}{
		{name: "both", metadata: true, blob: true},
		{name: "metadata only", metadata: true},
		{name: "blob only", blob: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var metadataCalled, blobCalled bool
			m := &DingoStateManager{}
			if tc.metadata {
				m.wipeMetadata = func() error {
					metadataCalled = true
					return nil
				}
			}
			if tc.blob {
				m.wipeBlob = func() error {
					blobCalled = true
					return nil
				}
			}

			require.NoError(t, m.Reset())

			require.Equal(t, tc.metadata, metadataCalled, "wipeMetadata")
			require.Equal(t, tc.blob, blobCalled, "wipeBlob")
		})
	}
}

// TestSqliteResetPreservesAlonzoPParamsUnitMarker pins the one row a freshly
// migrated schema is not empty of. Migration v20 seeds the Alonzo
// protocol-parameter unit marker and database.checkAlonzoPParamsUnit fails
// closed when it is absent, so while node_settings_gate was in the managed
// set a Reset left the database in a state no construction accepts.
func TestSqliteResetPreservesAlonzoPParamsUnitMarker(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := newDingoStateManagerAt(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })

	resetter, err := newSqliteResetter(sqliteMetadataPath(dataDir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = resetter.Close() })

	require.NoError(t, sm.Reset())

	var value string
	require.NoError(
		t,
		resetter.db.QueryRow(
			`SELECT value FROM node_settings_gate WHERE name = ?`,
			nodesettings.AlonzoPParamsUnitGateName,
		).Scan(&value),
		"Reset must leave the migration-seeded Alonzo unit marker in place",
	)
	require.Equal(t, nodesettings.AlonzoPParamsUnitWordV1, value)
}

// TestSqliteReopenAfterResetSucceeds pins the consequence the marker exists
// for: a manager constructed against a database another manager already reset
// must still open. The remote backends share one database across every manager
// in the process (see mysqlProcessDatabase), so the corpus replay's first Reset
// used to make every later construction in the same test binary fail. It also
// covers the opposite error, since preserving the whole table instead fails
// here on the blob_store_id gate.
func TestSqliteReopenAfterResetSucceeds(t *testing.T) {
	dataDir := t.TempDir()
	m1, err := newDingoStateManagerAt(dataDir)
	require.NoError(t, err)

	require.NoError(t, m1.Reset())
	require.NoError(t, m1.Close())

	m2, err := newDingoStateManagerAt(dataDir)
	require.NoError(t, err)
	require.NoError(t, m2.Close())
}

// TestSqliteResetClearsBlobStoreIDGate pins the one node_settings_gate row a
// Reset must not preserve. The blob wipe discards the reserved key the
// identity is minted into, so a surviving gate would reject the next
// construction as a metadata store paired with a foreign blob store -- the
// failure TestSqliteReopenAfterResetSucceeds would then hit from the other
// direction.
func TestSqliteResetClearsBlobStoreIDGate(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := newDingoStateManagerAt(dataDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sm.Close() })

	resetter, err := newSqliteResetter(sqliteMetadataPath(dataDir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = resetter.Close() })

	blobStoreIDRows := func() int {
		t.Helper()
		var rows int
		require.NoError(t, resetter.db.QueryRow(
			`SELECT COUNT(*) FROM node_settings_gate WHERE name = 'blob_store_id'`,
		).Scan(&rows))
		return rows
	}
	require.Equal(t, 1, blobStoreIDRows(), "precondition: the gate is latched")

	require.NoError(t, sm.Reset())

	require.Zero(
		t,
		blobStoreIDRows(),
		"Reset discards the blob store's identity, so its gate must go too",
	)
}

// newSqliteResetterTestDB creates a SQLite file with the given DDL applied and
// returns its path plus an open handle for assertions. The handle is separate
// from the resetter's own connection on purpose: the resetter holding its own
// pool for the manager's lifetime is the property being exercised.
func newSqliteResetterTestDB(t *testing.T, ddl ...string) (string, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open(
		"sqlite",
		"file:"+path+"?_pragma=busy_timeout(30000)&"+
			"_pragma=journal_mode(MEMORY)&_pragma=synchronous(OFF)",
	)
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

// TestSqliteResetterRelaxesSynchronous pins the resetter's connection to
// synchronous=OFF (0). It runs one DELETE batch per vector against a throwaway
// database, so the driver's per-autocommit flush is pure cost. The store under
// test opens its own connections with its production settings.
func TestSqliteResetterRelaxesSynchronous(t *testing.T) {
	path, _ := newSqliteResetterTestDB(
		t,
		`CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)`,
	)
	resetter, err := newSqliteResetter(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = resetter.Close() })

	var synchronous int
	require.NoError(
		t,
		resetter.db.QueryRow("PRAGMA synchronous").Scan(&synchronous),
	)
	require.Zero(t, synchronous, "PRAGMA synchronous must be OFF (0)")
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
