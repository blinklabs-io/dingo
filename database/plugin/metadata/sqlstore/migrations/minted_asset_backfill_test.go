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
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// An upgraded database already holds mint/burn history. The migration must
// give every asset a minted_asset row pointing at its first event, across
// more than one backfill batch and without moving an asset first seen in an
// earlier batch when a later event of it arrives.
func TestMintedAssetBackfillRecordsFirstEvents(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, databasePath, 58)
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	policy := bytes.Repeat([]byte{0x01}, 28)
	// Event ids follow insertion order: A1 B2 A3 C4 B5 A6 empty-name7.
	for i, name := range []string{"A", "B", "A", "C", "B", "A", ""} {
		_, err := db.Exec(`
INSERT INTO asset_mint_burn (tx_hash, policy_id, name, fingerprint, slot, quantity, tx_index)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
			bytes.Repeat([]byte{byte(i + 1)}, 32), policy, []byte(name),
			[]byte("fp"), 10+i, "1", 0,
		)
		require.NoError(t, err)
	}

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 59)
	registry[58].BatchSize = 2
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker: migrations.NewFileLocker(
			databasePath + ".migrate.lock",
		),
	}
	require.NoError(t, runner.Run(ctx))

	rows, err := db.Query(
		`SELECT name, first_event_id FROM minted_asset ORDER BY first_event_id`,
	)
	require.NoError(t, err)
	defer rows.Close()
	got := map[string]int64{}
	var order []string
	for rows.Next() {
		var name []byte
		var first int64
		require.NoError(t, rows.Scan(&name, &first))
		got[string(name)] = first
		order = append(order, string(name))
	}
	require.NoError(t, rows.Err())
	require.Equal(t, map[string]int64{"A": 1, "B": 2, "C": 4, "": 7}, got)
	require.Equal(t, []string{"A", "B", "C", ""}, order)
}
