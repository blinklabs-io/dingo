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

package migrations_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

var utxoSpenderIndexes = map[string]string{
	"idx_utxo_spent_at_tx_id":      "spent_at_tx_id",
	"idx_utxo_collateral_by_tx_id": "collateral_by_tx_id",
	"idx_utxo_referenced_by_tx_id": "referenced_by_tx_id",
}

// spenderIndexDB returns a database migrated through the version before the
// spender indexes became partial, and a function running the rest.
func spenderIndexDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 50)
	runTo := func(registry []migrations.Migration) {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: registry,
			Locker: migrations.NewFileLocker(
				databasePath + ".migrate.lock",
			),
		}
		require.NoError(t, runner.Run(ctx))
	}
	runTo(registry[:49])
	return db, func() { runTo(registry) }
}

// indexSQL returns the stored definition of an index and whether it exists.
func indexSQL(t *testing.T, db *sql.DB, name string) (string, bool) {
	t.Helper()
	var definition string
	err := db.QueryRow(
		"SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
		name,
	).Scan(&definition)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false
	}
	require.NoError(t, err)
	return definition, true
}

func TestUtxoSpenderIndexesBecomePartial(t *testing.T) {
	t.Parallel()
	db, upgrade := spenderIndexDB(t)
	for name := range utxoSpenderIndexes {
		definition, ok := indexSQL(t, db, name)
		require.True(t, ok, name)
		require.NotContains(t, definition, "WHERE", name)
	}

	upgrade()

	for name, column := range utxoSpenderIndexes {
		definition, ok := indexSQL(t, db, name)
		require.True(t, ok, name)
		require.Contains(t, definition, "WHERE "+column+" IS NOT NULL",
			"%s must index only non-NULL rows: %s", name, definition)
	}
}

// While a bulk-load rebuild is pending, the deferred rebuild owns these
// indexes. The upgrade must not build them early, and must not leave a
// full-table index behind for the rebuild to skip as already present.
func TestUtxoSpenderIndexesLeftToPendingRebuild(t *testing.T) {
	t.Parallel()
	db, upgrade := spenderIndexDB(t)
	_, err := db.Exec(
		"INSERT INTO sync_state (sync_key, value) VALUES (?, ?)",
		deferred.SyncStateKey,
		deferred.SyncStateValue,
	)
	require.NoError(t, err)

	upgrade()

	for name := range utxoSpenderIndexes {
		_, ok := indexSQL(t, db, name)
		require.False(t, ok, "%s must be left to the deferred rebuild", name)
	}
}
