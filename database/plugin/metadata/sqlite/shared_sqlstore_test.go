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

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
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
	require.Equal(t, 1, migrationCount)

	size, err := store.DiskSize()
	require.NoError(t, err)
	require.Positive(t, size)
	require.FileExists(t, filepath.Join(dataDir, "metadata.sqlite"))
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
