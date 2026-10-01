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
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

// A scheduled VACUUM that has to wait for SQLite's database-wide write lock
// waits on its own connection. Issued on the single-connection write pool it
// would hold that pool's only connection for the whole wait, and every ledger
// write would queue behind it.
func TestSQLiteVacuumWaitsOutsideWritePool(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	store, writeDB, readDB, err := openSQLStore(
		Config{DataDir: dataDir, VacuumIntervalSeconds: 1},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	for _, statement := range []string{
		"CREATE TABLE vacuum_probe (payload BLOB)",
		`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 200)
INSERT INTO vacuum_probe SELECT zeroblob(8192) FROM n`,
		"DROP TABLE vacuum_probe",
	} {
		_, err = writeDB.ExecContext(t.Context(), statement)
		require.NoError(t, err)
	}
	freelist := func() int {
		var pages int
		if err := readDB.QueryRowContext(
			t.Context(),
			"PRAGMA freelist_count",
		).Scan(&pages); err != nil {
			return -1
		}
		return pages
	}
	require.Positive(t, freelist(), "dropping the table must free pages")

	require.NoError(t, store.Start(t.Context()))
	// Hold the write lock from outside the pools so VACUUM has to wait.
	lockDB, err := sql.Open(
		"sqlite",
		sqliteFileURI(filepath.Join(dataDir, "metadata.sqlite"))+
			"?_pragma=busy_timeout(30000)",
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = lockDB.Close() })
	lock, err := lockDB.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = lock.Close() })
	_, err = lock.ExecContext(t.Context(), "BEGIN IMMEDIATE")
	require.NoError(t, err)

	// The first VACUUM is due 1 to 1.1 seconds after Start and then blocks on
	// the lock above; the window outlasts it.
	require.Never(
		t,
		func() bool { return writeDB.Stats().InUse > 0 },
		3*time.Second,
		10*time.Millisecond,
		"VACUUM held a write-pool connection while waiting for the lock",
	)

	_, err = lock.ExecContext(t.Context(), "ROLLBACK")
	require.NoError(t, err)
	require.Eventually(
		t,
		func() bool { return freelist() == 0 },
		30*time.Second,
		50*time.Millisecond,
		"VACUUM did not run once the lock was released",
	)
}
