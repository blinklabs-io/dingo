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
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

// newVacuumFixture opens a file-backed store whose database holds free pages
// left behind by a dropped table, plus a small table for concurrent writes.
func newVacuumFixture(
	t *testing.T,
) (dataDir string, writeDB, readDB *sql.DB) {
	t.Helper()
	dataDir = t.TempDir()
	store, writeDB, readDB, err := openSQLStore(
		Config{DataDir: dataDir},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	for _, statement := range []string{
		"CREATE TABLE vacuum_probe (payload BLOB)",
		"CREATE TABLE vacuum_writes (id INTEGER)",
		`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 200)
INSERT INTO vacuum_probe SELECT zeroblob(8192) FROM n`,
		"DROP TABLE vacuum_probe",
	} {
		_, err = writeDB.ExecContext(t.Context(), statement)
		require.NoError(t, err)
	}
	require.Positive(
		t,
		pragmaInt(t, readDB, "freelist_count"),
		"dropping the table must free pages",
	)
	return dataDir, writeDB, readDB
}

func pragmaInt(t *testing.T, db *sql.DB, pragma string) int {
	t.Helper()
	var value int
	require.NoError(
		t,
		db.QueryRowContext(t.Context(), "PRAGMA "+pragma).Scan(&value),
	)
	return value
}

// A ledger write that arrives while VACUUM is due waits for the write pool.
// A VACUUM on any other connection takes SQLite's database-wide lock while
// the pool believes it is idle, and a writer then fails with SQLITE_BUSY
// after its busy_timeout instead of queueing.
func TestSQLiteVacuumRunsThroughWritePool(t *testing.T) {
	t.Parallel()
	dataDir, writeDB, readDB := newVacuumFixture(t)
	vacuum, _, err := sqliteVacuum(writeDB, 1)
	require.NoError(t, err)

	// A probe with no busy timeout reports any database-wide write lock
	// held by someone else the moment it is taken.
	probeDB, err := sql.Open(
		"sqlite",
		sqliteFileURI(filepath.Join(dataDir, "metadata.sqlite"))+
			"?_pragma=busy_timeout(0)&_pragma=synchronous(OFF)",
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = probeDB.Close() })
	probe, err := probeDB.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = probe.Close() })
	writeLockHeld := func() bool {
		if _, err := probe.ExecContext(
			t.Context(),
			"BEGIN IMMEDIATE",
		); err != nil {
			return true
		}
		_, err := probe.ExecContext(t.Context(), "ROLLBACK")
		require.NoError(t, err)
		return false
	}

	held, err := writeDB.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = held.Close() })
	done := make(chan error, 1)
	go func() { done <- vacuum(context.Background()) }()

	require.Never(
		t,
		writeLockHeld,
		time.Second,
		2*time.Millisecond,
		"VACUUM took the database write lock while the write pool's only connection was in use",
	)
	require.Positive(t, pragmaInt(t, readDB, "freelist_count"))
	require.NoError(t, held.Close())
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("VACUUM did not finish once the write pool was free")
	}
	require.Zero(t, pragmaInt(t, readDB, "freelist_count"))
	require.Equal(t, 2, pragmaInt(t, readDB, "auto_vacuum"))
}

// Between incremental steps the write connection is back in the pool, so a
// queued ledger write completes before the reclaim does.
func TestSQLiteVacuumYieldsWritePoolBetweenChunks(t *testing.T) {
	t.Parallel()
	_, writeDB, readDB := newVacuumFixture(t)
	// Convert first so the loop under test is the incremental one.
	require.NoError(t, vacuumWith(t.Context(), writeDB, 1<<20, nil))
	_, err := writeDB.ExecContext(
		t.Context(),
		`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 100)
INSERT INTO vacuum_writes SELECT zeroblob(8192) FROM n`,
	)
	require.NoError(t, err)
	_, err = writeDB.ExecContext(t.Context(), "DELETE FROM vacuum_writes")
	require.NoError(t, err)
	free := pragmaInt(t, readDB, "freelist_count")

	var chunks int
	err = vacuumWith(t.Context(), writeDB, 16, func() {
		chunks++
		if chunks > 3 {
			return
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_, err := writeDB.ExecContext(
			ctx,
			"INSERT INTO vacuum_writes VALUES (?)",
			chunks,
		)
		require.NoError(t, err, "write between chunks must not wait")
	})
	require.NoError(t, err)
	require.GreaterOrEqual(t, free, 1)
	require.Greater(t, chunks, 3, "reclaim must proceed in several steps")
	require.Zero(t, pragmaInt(t, readDB, "freelist_count"))
}

func TestSQLiteVacuumStopsWhenWritesReplenishFreelist(t *testing.T) {
	t.Parallel()
	_, writeDB, readDB := newVacuumFixture(t)
	// Convert once so the test exercises only the incremental loop.
	require.NoError(t, vacuumWith(t.Context(), writeDB, 1<<20, nil))
	for _, statement := range []string{
		"CREATE TABLE vacuum_replenish (payload BLOB)",
		`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 200)
INSERT INTO vacuum_replenish SELECT zeroblob(8192) FROM n`,
		"DELETE FROM vacuum_replenish",
	} {
		_, err := writeDB.ExecContext(t.Context(), statement)
		require.NoError(t, err)
	}
	free := pragmaInt(t, readDB, "freelist_count")
	require.Positive(t, free)

	chunks := 0
	err := vacuumWith(t.Context(), writeDB, free, func() {
		if chunks > 0 {
			return
		}
		chunks++
		_, err := writeDB.ExecContext(
			t.Context(),
			fmt.Sprintf(`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < %d)
INSERT INTO vacuum_replenish SELECT zeroblob(8192) FROM n`, free+1),
		)
		require.NoError(t, err)
		_, err = writeDB.ExecContext(t.Context(), "DELETE FROM vacuum_replenish")
		require.NoError(t, err)
		require.GreaterOrEqual(t, pragmaInt(t, readDB, "freelist_count"), free)
	})
	require.ErrorContains(t, err, "incremental vacuum made no progress")
	require.Equal(t, 1, chunks)
}
