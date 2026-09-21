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
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// TestAssetNameHexColumnDropRemovesColumnAndIndex proves migration v19
// (asset-name-hex-column-drop, dingo#4464) drops both asset.name_hex and
// idx_asset_name_hex from a database migrated all the way through. Before
// this migration existed, a fully migrated (through v18) database still
// carried both: name_hex was a write-only column nothing filtered on.
func TestAssetNameHexColumnDropRemovesColumnAndIndex(t *testing.T) {
	t.Parallel()

	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   migrations.NewFileLocker(databasePath + ".migrate.lock"),
	}
	require.NoError(t, runner.Run(context.Background()))

	rows, err := db.Query(`PRAGMA table_info("asset")`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt any
		require.NoError(
			t,
			rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk),
		)
		require.NotEqual(
			t,
			"name_hex",
			name,
			"asset.name_hex must not survive migration v19",
		)
	}
	require.NoError(t, rows.Err())

	idxRows, err := db.Query(`PRAGMA index_list("asset")`)
	require.NoError(t, err)
	defer idxRows.Close()
	for idxRows.Next() {
		var seq int
		var name, origin string
		var unique, partial int
		require.NoError(
			t,
			idxRows.Scan(&seq, &name, &unique, &origin, &partial),
		)
		require.NotEqual(
			t,
			"idx_asset_name_hex",
			name,
			"idx_asset_name_hex must not survive migration v19",
		)
	}
	require.NoError(t, idxRows.Err())
}
