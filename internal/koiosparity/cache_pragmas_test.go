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

package koiosparity

import (
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestOpenCacheUsesMetadataStoreSQLiteSettings pins the cache database to the
// pragma set of Dingo's own SQLite metadata store
// (sqliteCommonPragmas in database/plugin/metadata/sqlite/shared_sqlstore.go).
// The driver default, synchronous=FULL, flushes on every autocommit, and the
// cache commits once per fetched row batch.
//
// busy_timeout is the one deliberate difference: it stays at 5s because the
// concurrency tests in cache_concurrency_test.go are built around that bound.
func TestOpenCacheUsesMetadataStoreSQLiteSettings(t *testing.T) {
	t.Parallel()

	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })

	for _, want := range []struct {
		pragma string
		value  string
	}{
		{"journal_mode", "wal"},
		{"synchronous", "1"}, // NORMAL
		{"wal_autocheckpoint", "10000"},
		{"cache_size", "-50000"},
		{"foreign_keys", "1"},
		{"mmap_size", "268435456"},
		{"busy_timeout", "5000"},
	} {
		var got string
		require.NoError(
			t,
			cache.db.QueryRow("PRAGMA "+want.pragma).Scan(&got),
		)
		require.Equal(t, want.value, got, "PRAGMA %s", want.pragma)
	}
}

// TestOpenCacheWithRelaxedDurability pins the opt-in for throwaway caches:
// synchronous=OFF, with every other setting unchanged.
func TestOpenCacheWithRelaxedDurability(t *testing.T) {
	t.Parallel()

	cache, err := OpenCache(
		filepath.Join(t.TempDir(), "cache.db"),
		nil,
		WithRelaxedDurability(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })

	for _, want := range []struct {
		pragma string
		value  string
	}{
		{"synchronous", "0"}, // OFF
		{"journal_mode", "wal"},
		{"busy_timeout", "5000"},
		{"wal_autocheckpoint", "10000"},
	} {
		var got string
		require.NoError(
			t,
			cache.db.QueryRow("PRAGMA "+want.pragma).Scan(&got),
		)
		require.Equal(t, want.value, got, "PRAGMA %s", want.pragma)
	}
}

// openTestCache opens a cache whose durability is relaxed. Test caches are
// discarded with the test, so the per-commit flush is pure cost.
func openTestCache(path string, logger *slog.Logger) (*Cache, error) {
	return OpenCache(path, logger, WithRelaxedDurability())
}
