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
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/stretchr/testify/require"
)

func TestSharedStoreDeferredIndexLifecycle(t *testing.T) {
	t.Parallel()
	store, writeDB, _, err := openSQLStore(
		Config{},
		metadata.ProviderDependencies{},
	)
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	require.True(t, sqliteIndexExists(t, writeDB, "idx_utxo_payment_key"))
	require.True(t, sqliteIndexExists(t, writeDB, "idx_asset_name_hex"))

	require.NoError(t, store.DropDeferredIndexes())
	pending, err := store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(t, pending)
	require.False(t, sqliteIndexExists(t, writeDB, "idx_utxo_payment_key"))
	require.False(t, sqliteIndexExists(t, writeDB, "idx_asset_name_hex"))

	require.NoError(t, store.BuildCriticalDeferredIndexes())
	require.True(t, sqliteIndexExists(t, writeDB, "idx_utxo_payment_key"))
	require.False(t, sqliteIndexExists(t, writeDB, "idx_asset_name_hex"))
	pending, err = store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.True(t, pending)

	require.NoError(t, store.BuildDeferredIndexes())
	require.True(t, sqliteIndexExists(t, writeDB, "idx_asset_name_hex"))
	pending, err = store.HasDeferredIndexesPending()
	require.NoError(t, err)
	require.False(t, pending)
}

func sqliteIndexExists(t *testing.T, db interface {
	QueryRow(string, ...any) *sql.Row
}, name string) bool {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?",
		name,
	).Scan(&count))
	return count == 1
}
