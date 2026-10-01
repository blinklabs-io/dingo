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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

// The scheduled VACUUM must complete while every connection of the
// single-connection write pool is checked out. A VACUUM issued on that pool
// holds the only slot, so every ledger write queues behind it for as long as
// the rewrite takes.
func TestSQLiteVacuumRunsOutsideWritePool(t *testing.T) {
	t.Parallel()
	store, writeDB, readDB, err := openSQLStore(
		Config{DataDir: t.TempDir(), VacuumIntervalSeconds: 1},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(t.Context()))
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	_, err = writeDB.ExecContext(
		t.Context(),
		"CREATE TABLE vacuum_probe (payload BLOB)",
	)
	require.NoError(t, err)
	_, err = writeDB.ExecContext(
		t.Context(),
		`WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 200)
INSERT INTO vacuum_probe SELECT zeroblob(8192) FROM n`,
	)
	require.NoError(t, err)
	_, err = writeDB.ExecContext(t.Context(), "DROP TABLE vacuum_probe")
	require.NoError(t, err)
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

	writer, err := writeDB.Conn(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Close() })

	require.Eventually(
		t,
		func() bool { return freelist() == 0 },
		30*time.Second,
		50*time.Millisecond,
		"VACUUM did not run while the write pool was exhausted",
	)
}
