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

package migrations

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// Translating the registry for MySQL costs about half a second without the race
// detector and ~10s with it, and a dozen tests only read the result. The
// registries are immutable for those readers, so each dialect is built once per
// test binary.
var (
	cachedMySQLRegistry    = sync.OnceValues(MySQLRegistry)
	cachedPostgresRegistry = sync.OnceValues(PostgresRegistry)
)

// prefixBaselines holds database files migrated through the first n shipped
// SQLite versions, built incrementally so each version's DDL and backfill runs
// once per test binary however many tests need a database at that point.
var prefixBaselines struct {
	mu    sync.Mutex
	bytes [][]byte // bytes[n-1] is the file after registry[:n] completed
}

func prefixBaseline(tb testing.TB, n int) []byte {
	tb.Helper()
	registry, err := SQLiteRegistry()
	require.NoError(tb, err)
	require.LessOrEqual(tb, n, len(registry))
	require.Positive(tb, n)
	prefixBaselines.mu.Lock()
	defer prefixBaselines.mu.Unlock()
	for len(prefixBaselines.bytes) < n {
		next := len(prefixBaselines.bytes) + 1
		dir, err := os.MkdirTemp("", "dingo-migration-prefix-")
		require.NoError(tb, err)
		path := filepath.Join(dir, "metadata.sqlite")
		if next > 1 {
			require.NoError(tb, os.WriteFile(
				path, prefixBaselines.bytes[next-2], 0o600,
			))
		}
		db, err := sql.Open(
			"sqlite",
			"file:"+path+"?_pragma=foreign_keys(1)&"+testDBPragmas,
		)
		require.NoError(tb, err)
		runner := &Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: registry[:next],
			Locker:   NewProcessLocker(),
		}
		require.NoError(tb, runner.Run(context.Background()))
		require.NoError(tb, db.Close())
		data, err := os.ReadFile(path)
		require.NoError(tb, err)
		require.NoError(tb, os.RemoveAll(dir))
		prefixBaselines.bytes = append(prefixBaselines.bytes, data)
	}
	return prefixBaselines.bytes[n-1]
}

// SeedMigratedPrefix writes into path a database migrated through the first n
// shipped SQLite versions, the state an upgrade test starts from before it
// seeds legacy rows and runs the remaining versions. It is exported only so
// the external test package can share the cache.
func SeedMigratedPrefix(tb testing.TB, path string, n int) {
	tb.Helper()
	require.NoError(tb, os.WriteFile(path, prefixBaseline(tb, n), 0o600))
}

// cachedRegistryForDialect is registryForDialect through the per-binary caches
// above.
func cachedRegistryForDialect(dialect string) ([]Migration, error) {
	switch dialect {
	case "postgres":
		return cachedPostgresRegistry()
	case "mysql":
		return cachedMySQLRegistry()
	default:
		return registryForDialect(dialect)
	}
}
