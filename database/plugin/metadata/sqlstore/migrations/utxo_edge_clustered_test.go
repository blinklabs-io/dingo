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
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

const edgeClusteredVersion = 74

// edgeClusteredDB returns a database migrated through the version before the
// rebuild, holding utxo rows 1..utxos and the given (tx_id, utxo_id) edges in
// both legacy edge tables.
func edgeClusteredDB(
	t *testing.T,
	utxos int,
	edges [][2]int64,
) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, path, edgeClusteredVersion-1)
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	for id := 1; id <= utxos; id++ {
		txID := make([]byte, 32)
		txID[0] = byte(id)
		_, err := db.Exec(`
INSERT INTO utxo (id, tx_id, output_idx, added_slot, deleted_slot, amount)
VALUES (?, ?, 0, 1, 0, '1')`, id, txID)
		require.NoError(t, err)
	}
	for _, edge := range edges {
		for _, table := range []string{
			"utxo_reference_edge", "utxo_collateral_edge",
		} {
			_, err := db.Exec(
				"INSERT INTO "+table+" (tx_id, utxo_id) VALUES (?, ?)",
				edge[0], edge[1],
			)
			require.NoError(t, err)
		}
	}
	return db, path
}

func runEdgeClustered(
	t *testing.T,
	db *sql.DB,
	path string,
	batchSize int,
) {
	t.Helper()
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	migration := &registry[edgeClusteredVersion-1]
	require.Equal(t, "utxo-edge-clustered", migration.Name)
	require.NotNil(t, migration.Backfill)
	if batchSize > 0 {
		migration.BatchSize = batchSize
	}
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   migrations.NewFileLocker(path + ".migrate.lock"),
	}
	require.NoError(t, runner.Run(context.Background()))
}

func edgeKeys(t *testing.T, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.Query(
		"SELECT tx_id, utxo_id FROM " + table + " ORDER BY tx_id, utxo_id",
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var out []string
	for rows.Next() {
		var tx, utxo int64
		require.NoError(t, rows.Scan(&tx, &utxo))
		out = append(out, fmt.Sprintf("%d/%d", tx, utxo))
	}
	require.NoError(t, rows.Err())
	return out
}

// TestUtxoEdgeClusteredKeepsEdgesAndDropsRowidLayout upgrades databases with
// edges spread over several batches and checks every edge survives, each
// table is a clustered table keyed by (tx_id, utxo_id), and the legacy
// tx_id index, the primary key autoindex and the copy tables are gone.
func TestUtxoEdgeClusteredKeepsEdgesAndDropsRowidLayout(t *testing.T) {
	t.Parallel()
	edges := [][2]int64{
		{7, 3}, {7, 1}, {2, 3}, {9, 2}, {2, 1}, {5, 3}, {5, 2},
	}
	for _, batchSize := range []int{0, 1, 2, 3, 7} {
		t.Run(fmt.Sprintf("batch_%d", batchSize), func(t *testing.T) {
			t.Parallel()
			db, path := edgeClusteredDB(t, 3, edges)
			runEdgeClustered(t, db, path, batchSize)
			want := []string{
				"2/1", "2/3", "5/2", "5/3", "7/1", "7/3", "9/2",
			}
			for _, table := range []string{
				"utxo_reference_edge", "utxo_collateral_edge",
			} {
				require.Equal(t, want, edgeKeys(t, db, table), table)
				var ddl string
				require.NoError(t, db.QueryRow(
					`SELECT sql FROM sqlite_master
WHERE type = 'table' AND name = ?`, table).Scan(&ddl))
				require.True(t, strings.Contains(ddl, "WITHOUT ROWID"), ddl)
				require.True(t, strings.Contains(
					ddl, "PRIMARY KEY (`tx_id`,`utxo_id`)"), ddl)
				require.Contains(t, ddl, "ON DELETE CASCADE")
			}
			var names []string
			rows, err := db.Query(`
SELECT name FROM sqlite_master
WHERE tbl_name LIKE 'utxo_%_edge%' AND type IN ('table', 'index')
ORDER BY name`)
			require.NoError(t, err)
			for rows.Next() {
				var name string
				require.NoError(t, rows.Scan(&name))
				names = append(names, name)
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			require.Equal(t, []string{
				"idx_utxo_collateral_edge_utxo_id",
				"idx_utxo_reference_edge_utxo_id",
				"utxo_collateral_edge",
				"utxo_reference_edge",
			}, names)
		})
	}
}

// TestUtxoEdgeClusteredCascadeStillDeletesEdges pins the utxo_id index's
// purpose: removing a utxo removes its edges.
func TestUtxoEdgeClusteredCascadeStillDeletesEdges(t *testing.T) {
	t.Parallel()
	db, path := edgeClusteredDB(t, 2, [][2]int64{{4, 1}, {4, 2}, {6, 1}})
	runEdgeClustered(t, db, path, 0)
	_, err := db.Exec("PRAGMA foreign_keys = ON")
	require.NoError(t, err)
	_, err = db.Exec("DELETE FROM utxo WHERE id = 1")
	require.NoError(t, err)
	require.Equal(t, []string{"4/2"}, edgeKeys(t, db, "utxo_reference_edge"))
	require.Equal(t, []string{"4/2"}, edgeKeys(t, db, "utxo_collateral_edge"))
}
