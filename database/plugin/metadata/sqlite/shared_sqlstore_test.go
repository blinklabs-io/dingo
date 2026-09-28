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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
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

func TestSQLiteVacuumMaintenanceIsOptInAndConfigurable(t *testing.T) {
	t.Parallel()
	db, err := sqlstore.OpenDB(
		"sqlite",
		fmt.Sprintf(
			"file:sqlite_vacuum_config_%d?mode=memory&cache=shared",
			sharedMemoryDBSequence.Add(1),
		),
		"sqlite",
		false,
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	maintenance, interval, err := sqliteVacuum(db, 0)
	require.NoError(t, err)
	require.Nil(t, maintenance)
	require.Zero(t, interval)

	maintenance, interval, err = sqliteVacuum(db, 30)
	require.NoError(t, err)
	require.NotNil(t, maintenance)
	require.Equal(t, 30*time.Second, interval)
	require.NoError(t, maintenance(context.Background()))
}

func TestOpenSQLStoreRejectsVacuumIntervalOverflow(t *testing.T) {
	t.Parallel()
	_, _, _, err := openSQLStore(
		Config{VacuumIntervalSeconds: maxVacuumIntervalSeconds + 1},
		metadata.ProviderDependencies{},
	)
	require.ErrorContains(t, err, "vacuumIntervalSeconds exceeds maximum")
}

// diskSizeUntilComplete calls store.DiskSize until one call completes or
// deadline passes, returning the last result. Safe to call off the test
// goroutine: it never touches *testing.T.
//
// DiskSize bounds its own PRAGMA reads at sqliteDiskSizeQueryTimeout and
// reports a deadline rather than stalling a Prometheus scrape behind a slow
// connection. That budget is a production guarantee, not a test assumption,
// and it can legitimately expire while this package runs its tests in
// parallel under -race, each with its own SQLite database: one such run
// returned "SQLite page count: context deadline exceeded". Retrying keeps
// the assertion that DiskSize works without also asserting that five
// seconds is always enough on a loaded machine; a DiskSize that is wedged
// rather than slow never completes on any attempt.
func diskSizeUntilComplete(
	store *sqlstore.Store,
	deadline time.Time,
) (int64, error) {
	// Backoff, not synchronization: an error DiskSize returns immediately
	// -- a closed or missing database -- would otherwise spin hot until
	// the deadline.
	const retryPause = 50 * time.Millisecond
	for {
		size, err := store.DiskSize()
		if err == nil || !time.Now().Before(deadline) {
			return size, err
		}
		time.Sleep(retryPause)
	}
}

// requireDiskSize is diskSizeUntilComplete on the test goroutine, requiring
// that some call completes and reports a positive size.
func requireDiskSize(t *testing.T, store *sqlstore.Store) int64 {
	t.Helper()
	size, err := diskSizeUntilComplete(
		store,
		time.Now().Add(testutil.AsyncWait),
	)
	require.NoError(t, err, "no DiskSize call completed")
	require.Positive(t, size)
	return size
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

	requireDiskSize(t, store)
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
		size, diskSizeErr = diskSizeUntilComplete(
			store,
			time.Now().Add(testutil.AsyncWait),
		)
	}()

	// A failure deadline, not a latency budget. The property is that
	// DiskSize does not queue behind the write pool's only connection,
	// and a DiskSize that did queue behind it would not return late --
	// it would not return at all, because nothing rolls the transaction
	// back until cleanup. A generous deadline therefore costs a passing
	// run nothing (RequireReceive returns the instant DiskSize does) and
	// still catches the regression; a 2s one only added a second failure
	// mode, because opening a connection and reading two PRAGMAs takes
	// well over two seconds on a machine running the rest of this package
	// under -race in parallel.
	testutil.RequireReceive(
		t,
		done,
		testutil.AsyncWait,
		"DiskSize blocked behind the write transaction's sole connection",
	)
	require.NoError(
		t, diskSizeErr,
		"no DiskSize call completed while the write transaction held the "+
			"write pool's only connection",
	)
	require.Positive(t, size)
}

// TestDiskSizeDoesNotLeaveReadDBConnectionOpen is the regression test for
// sqliteDiskSize pinning an idle connection open in readDB's shared pool.
// Live symptom this is associated with: perf's containers logged
// checkpointWAL's "a reader is still holding an old snapshot" warning every
// ~2 minutes, for as long as the process ran, after sqliteDiskSize started
// reading through readDB instead of writeDB; vanilla (never moved off
// writeDB) never logged it once. Reproduced directly against a live
// affected container: an external, independently-opened `sqlite3
// metadata.sqlite "PRAGMA wal_checkpoint(TRUNCATE)"` returned the same
// busy=1 result dingo's own checkpointWAL was logging, and moving
// sqliteDiskSize off readDB onto its own dedicated connection made the
// warnings stop and the on-disk -wal file shrink.
//
// This test only proves the narrower, mechanical fact its name says: before
// the fix, one DiskSize() call left a connection sitting in readDB's pool
// that had not been there before and that nothing ever closes on its own
// (neither pool sets SetConnMaxIdleTime or SetConnMaxLifetime); after the
// fix, sqliteDiskSize opens and closes its own dedicated connection per
// call, exactly like checkpointWAL already does, so readDB is never touched
// and never gains one. It does not by itself show that an idle, otherwise
// unused connection blocks wal_checkpoint(TRUNCATE) -- see
// TestWALCheckpointTruncateIdleConnectionDoesNotBlock below, which tests
// that specific claim directly and finds it false: a connection that runs
// this same query pattern and is then left idle in the pool does not block
// a subsequent TRUNCATE against real WAL content, while an actual held read
// transaction does. The production mechanism connecting "DiskSize on
// readDB" to the persistent busy=1 symptom is not fully isolated by either
// test; the dedicated-connection fix is justified by the production
// before/after observation and by matching checkpointWAL's own existing
// design, not by a claim that mere pool attachment is sufficient to block a
// checkpoint.
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

	// A completed DiskSize() call, mirroring a Prometheus scrape of
	// dingo_database_sql_disk_bytes (see metrics.go). An attempt that hits
	// DiskSize's own query deadline is a scrape too and must leave readDB
	// just as untouched, so retrying until one completes does not weaken
	// the assertion below.
	requireDiskSize(t, store)

	require.Zerof(
		t,
		readDB.Stats().OpenConnections,
		"DiskSize() left %d connection(s) open in readDB's shared pool; "+
			"the pre-fix code path left one behind here indefinitely "+
			"(see this test's doc comment for what that was and was not "+
			"shown to cause)",
		readDB.Stats().OpenConnections,
	)
}

// TestWALCheckpointTruncateIdleConnectionDoesNotBlock settles, by direct
// reproduction against dingo's actual readDB DSN and pragmas, the claim
// disputed in review of the sqliteDiskSize fix above: does a connection that
// ran exactly sqliteDiskSize's old query pattern (two independent
// QueryRowContext(...).Scan(...) calls) and is then left idle in readDB's
// pool -- never closed -- block a separate connection's PRAGMA
// wal_checkpoint(TRUNCATE)?
//
// SQLite's own documentation for wal_checkpoint says RESTART and TRUNCATE
// block only on an active writer or a reader still using an old snapshot,
// not on a connection that is merely idle. This test confirms that is also
// true here: case "idle_finalized_connection" runs the old two-PRAGMA-read
// pattern, confirms (via OpenConnections) that a connection really is
// pinned in the pool afterward, and still observes busy=0 and a full
// truncation to zero bytes against real, substantial WAL content -- the
// same as the "no_other_connection" baseline. Case
// "held_read_transaction" is the positive control: an explicit,
// uncommitted read transaction on the same readDB pool does reproduce
// busy=1, proving this test can detect a real blocker when one exists.
//
// Each case also cross-checks internal consistency: a checkpoint's own
// reported log/checkpointed frame counts can legitimately read 0 with a
// large physical -wal file still on disk, when wal_autocheckpoint's PASSIVE
// checkpoints already backfilled every frame between writes (PASSIVE never
// truncates -- see checkpointInterval's doc comment above) -- so the
// assertions key off busy and the physical file size actually dropping to
// zero, not off log/checkpointed being nonzero.
func TestWALCheckpointTruncateIdleConnectionDoesNotBlock(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, writeDB, readDB, err := openSQLStore(
		Config{DataDir: dataDir},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	_, err = writeDB.Exec(
		`CREATE TABLE wal_checkpoint_repro (id INTEGER PRIMARY KEY, data BLOB)`,
	)
	require.NoError(t, err)
	blob := make([]byte, 4096)

	databasePath := filepath.Join(dataDir, "metadata.sqlite")
	databaseURI := sqliteFileURI(databasePath)
	walPath := databasePath + "-wal"

	// growWAL commits enough rows, one autocommit INSERT at a time (like
	// dingo's own chain-sync writes), to leave substantial physical WAL
	// content regardless of wal_autocheckpoint's PASSIVE backfilling.
	growWAL := func() {
		for range 500 {
			_, err := writeDB.Exec(
				`INSERT INTO wal_checkpoint_repro (data) VALUES (?)`,
				blob,
			)
			require.NoError(t, err)
		}
	}
	walSize := func() int64 {
		info, err := os.Stat(walPath)
		if err != nil {
			return 0
		}
		return info.Size()
	}

	// checkpoint opens a dedicated connection and issues exactly one
	// PRAGMA wal_checkpoint(TRUNCATE), mirroring checkpointWAL itself.
	checkpoint := func() (busy, log, checkpointed int) {
		t.Helper()
		db, err := sql.Open(
			"sqlite",
			fmt.Sprintf(
				"%s?_pragma=busy_timeout(%d)&_pragma=synchronous(OFF)",
				databaseURI,
				250,
			),
		)
		require.NoError(t, err)
		defer func() { require.NoError(t, db.Close()) }()
		db.SetMaxOpenConns(1)
		row := db.QueryRowContext(
			context.Background(),
			"PRAGMA wal_checkpoint(TRUNCATE)",
		)
		require.NoError(t, row.Scan(&busy, &log, &checkpointed))
		return busy, log, checkpointed
	}

	t.Run("no_other_connection", func(t *testing.T) {
		growWAL()
		require.Positive(t, walSize(), "need real WAL content to checkpoint")
		busy, _, _ := checkpoint()
		require.Zero(t, busy, "nothing else attached: TRUNCATE must not be busy")
		require.Zero(t, walSize(), "TRUNCATE must truncate the -wal file to 0 bytes")
	})

	t.Run("idle_finalized_connection", func(t *testing.T) {
		growWAL()
		require.Positive(t, walSize(), "need real WAL content to checkpoint")

		// sqliteDiskSize's old query pattern, run directly against readDB.
		var pageCount, pageSize int64
		require.NoError(
			t,
			readDB.QueryRowContext(context.Background(), "PRAGMA page_count").
				Scan(&pageCount),
		)
		require.NoError(
			t,
			readDB.QueryRowContext(context.Background(), "PRAGMA page_size").
				Scan(&pageSize),
		)
		require.Positive(
			t,
			readDB.Stats().OpenConnections,
			"test setup: the query above should leave a connection pinned in the pool",
		)

		busy, _, _ := checkpoint()
		require.Zerof(
			t,
			busy,
			"an idle, already-finalized connection (readDB.OpenConnections=%d) "+
				"must not block TRUNCATE",
			readDB.Stats().OpenConnections,
		)
		require.Zero(t, walSize(), "TRUNCATE must still truncate the -wal file to 0 bytes")
	})

	t.Run("held_read_transaction_positive_control", func(t *testing.T) {
		growWAL()
		before := walSize()
		require.Positive(t, before, "need real WAL content to checkpoint")

		tx, err := readDB.BeginTx(
			context.Background(),
			&sql.TxOptions{ReadOnly: true},
		)
		require.NoError(t, err)
		var count int64
		require.NoError(
			t,
			tx.QueryRowContext(
				context.Background(),
				"SELECT count(*) FROM wal_checkpoint_repro",
			).Scan(&count),
		)

		busy, _, _ := checkpoint()
		require.NotZero(
			t,
			busy,
			"a real, held, uncommitted read transaction must block TRUNCATE "+
				"(control: proves this test can detect a real blocker)",
		)
		require.Equal(
			t,
			before,
			walSize(),
			"a busy TRUNCATE must not have truncated the -wal file",
		)
		require.NoError(t, tx.Rollback())
	})
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
