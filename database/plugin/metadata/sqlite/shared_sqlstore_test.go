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
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

func newSharedSQLStore(
	t *testing.T,
) (*sqlstore.Store, *sql.DB) {
	t.Helper()
	store, writeDB, _, err := openSQLStore(
		Config{DataDir: t.TempDir()},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	return store, writeDB
}

func TestOpenSharedSQLStoreFilePoolsAndWAL(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, writeDB, readDB, err := openSQLStore(
		Config{MaxConnections: 3},
		metadata.ProviderDependencies{DataDir: dataDir},
	)
	require.NoError(t, err)
	require.NotSame(t, writeDB, readDB)
	require.Equal(t, 1, writeDB.Stats().MaxOpenConnections)
	require.Equal(t, 3, readDB.Stats().MaxOpenConnections)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	var journalMode string
	require.NoError(t, writeDB.QueryRow(
		"PRAGMA journal_mode",
	).Scan(&journalMode))
	require.Equal(t, "wal", journalMode)

	var migrationCount int
	require.NoError(t, readDB.QueryRow(
		"SELECT COUNT(*) FROM schema_migrations WHERE phase = 'complete'",
	).Scan(&migrationCount))
	// A fresh database runs every registered migration, so the count is taken
	// from the registry rather than written out: what is under test here is
	// that startup completed all of them, not how many there currently are.
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Equal(t, len(registry), migrationCount)

	size, err := store.DiskSize()
	require.NoError(t, err)
	require.Positive(t, size)
	require.FileExists(t, filepath.Join(dataDir, "metadata.sqlite"))
}

// TestDiskSizeDoesNotBlockOnBusyWriteConnection is the regression test for
// sqliteDiskSize querying through the write pool: writeDB has
// SetMaxOpenConns(1), so an open write transaction holds that pool's only
// connection until it commits or rolls back. DiskSize() (wired to
// dingo_database_sql_disk_bytes, scraped by Prometheus) must not share that
// pool, or a live write transaction stalls every scrape behind it.
// sqliteDiskSize now queries readDB, an independently sized pool, so this
// passes with an open write transaction held for the whole call.
func TestDiskSizeDoesNotBlockOnBusyWriteConnection(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, _, _, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{DataDir: dataDir},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	txn := store.Transaction(context.Background())
	t.Cleanup(func() {
		_ = txn.Rollback()
	})

	done := make(chan struct{})
	var (
		size        int64
		diskSizeErr error
	)
	go func() {
		defer close(done)
		size, diskSizeErr = store.DiskSize()
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal(
			"DiskSize blocked behind the write transaction's sole connection",
		)
	}
	require.NoError(t, diskSizeErr)
	require.Positive(t, size)
}

// TestDiskSizeDoesNotLeaveReadDBConnectionOpen is the regression test for
// sqliteDiskSize pinning a WAL reader open in readDB's shared pool. Live
// symptom this reproduces: perf's containers logged checkpointWAL's "a
// reader is still holding an old snapshot" warning every ~2 minutes, for as
// long as the process ran, after sqliteDiskSize started reading through
// readDB instead of writeDB; vanilla (never moved off writeDB) never logged
// it once. Reproduced directly against a live affected container: an
// external, independently-opened `sqlite3 metadata.sqlite "PRAGMA
// wal_checkpoint(TRUNCATE)"` returned the same busy=1 result dingo's own
// checkpointWAL was logging, confirming a real, OS-level WAL lock rather
// than an artifact of this process's own bookkeeping.
//
// checkpointWAL's PRAGMA wal_checkpoint(TRUNCATE) needs every connection
// other than its own dedicated one to be fully detached to complete the
// final truncation step -- not merely for no reader to hold a stale
// snapshot. Neither pool sets SetConnMaxIdleTime or SetConnMaxLifetime, so
// once a connection is idled into readDB's pool it stays attached
// indefinitely. Before the fix, sqliteDiskSize queried readDB directly: one
// DiskSize() call left a connection sitting in that pool that had not been
// there before and that nothing ever closes on its own, so from that point
// on every later WAL checkpoint(TRUNCATE) attempt had an attached connection
// to contend with. After the fix, sqliteDiskSize opens and closes its own
// dedicated connection per call, exactly like checkpointWAL already does, so
// readDB is never touched and never gains one.
func TestDiskSizeDoesNotLeaveReadDBConnectionOpen(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, _, readDB, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{DataDir: dataDir},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	// Force-evict whatever idle connection migrations left behind during
	// Start, so the assertion below reflects DiskSize's own effect rather
	// than incidental startup activity.
	readDB.SetMaxIdleConns(0)
	readDB.SetMaxIdleConns(DefaultMaxConnections)
	require.Zero(
		t,
		readDB.Stats().OpenConnections,
		"test setup: expected a clean readDB baseline before DiskSize",
	)

	// One DiskSize() call, mirroring a single Prometheus scrape of
	// dingo_database_sql_disk_bytes (see metrics.go).
	_, err = store.DiskSize()
	require.NoError(t, err)

	require.Zerof(
		t,
		readDB.Stats().OpenConnections,
		"DiskSize() left %d connection(s) open in readDB's shared pool; "+
			"a WAL checkpoint(TRUNCATE) attempt will report busy for as "+
			"long as any of them remain attached",
		readDB.Stats().OpenConnections,
	)
}

// TestOpenSharedSQLStoreWALAutocheckpoint pins the raised checkpoint
// threshold discussed at length in sqliteCommonPragmas' doc comment: a
// regression back to SQLite's compiled-in default of 1000 pages would
// silently reintroduce the checkpoint-driven write amplification that
// change fixed, without failing any functional test, since 1000 is itself a
// valid, working value.
func TestOpenSharedSQLStoreWALAutocheckpoint(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, writeDB, readDB, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{DataDir: dataDir},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	for name, db := range map[string]*sql.DB{"writeDB": writeDB, "readDB": readDB} {
		var pages int
		require.NoError(t, db.QueryRow(
			"PRAGMA wal_autocheckpoint",
		).Scan(&pages))
		require.Equalf(t, 10000, pages, "%s wal_autocheckpoint", name)
	}
}

func TestOpenSharedSQLStoreMemoryIsolation(t *testing.T) {
	t.Parallel()
	first, firstDB, _, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, first.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, first.Close())
	})
	second, secondDB, _, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, second.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, second.Close())
	})

	_, err = firstDB.Exec("CREATE TABLE isolation_marker (id INTEGER)")
	require.NoError(t, err)
	var count int
	require.NoError(t, secondDB.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master "+
			"WHERE type = 'table' AND name = 'isolation_marker'",
	).Scan(&count))
	require.Zero(t, count)
}
