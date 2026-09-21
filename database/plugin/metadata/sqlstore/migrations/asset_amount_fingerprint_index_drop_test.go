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

// TestAssetAmountFingerprintIndexDropRemovesIndexes proves migration v20
// (asset-amount-fingerprint-index-drop, dingo#4598) drops idx_asset_amount
// and idx_asset_fingerprint from a database migrated all the way through,
// while leaving the asset.amount and asset.fingerprint columns themselves
// untouched -- unlike dingo#4482's asset.name_hex, both columns are still
// genuinely read and returned via the blockfrost/mesh API adapters. Before
// this migration existed, a fully migrated (through v19) database still
// carried both indexes: a real WAL-frame-churn measurement during genesis
// sync found neither backs any WHERE/JOIN/ORDER BY predicate anywhere in the
// tree (dingo#4464).
func TestAssetAmountFingerprintIndexDropRemovesIndexes(t *testing.T) {
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

	wantColumns := map[string]bool{"amount": false, "fingerprint": false}
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
		if _, ok := wantColumns[name]; ok {
			wantColumns[name] = true
		}
	}
	require.NoError(t, rows.Err())
	require.True(
		t,
		wantColumns["amount"],
		"asset.amount must survive migration v20 -- only its index is dropped",
	)
	require.True(
		t,
		wantColumns["fingerprint"],
		"asset.fingerprint must survive migration v20 -- only its index is dropped",
	)

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
			"idx_asset_amount",
			name,
			"idx_asset_amount must not survive migration v20",
		)
		require.NotEqual(
			t,
			"idx_asset_fingerprint",
			name,
			"idx_asset_fingerprint must not survive migration v20",
		)
	}
	require.NoError(t, idxRows.Err())
}
