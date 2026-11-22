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
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

const blockNonceClusteredVersion = 78

type legacyBlockNonce struct {
	Hash       []byte
	Slot       any
	Nonce      []byte
	Checkpoint bool
}

func blockNonceClusteredDB(
	t *testing.T,
	rows []legacyBlockNonce,
) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, path, blockNonceClusteredVersion-1)
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	for _, row := range rows {
		_, err := db.Exec(`
INSERT INTO block_nonce (hash, slot, nonce, is_checkpoint) VALUES (?, ?, ?, ?)`,
			row.Hash, row.Slot, row.Nonce, row.Checkpoint)
		require.NoError(t, err)
	}
	return db, path
}

func runBlockNonceClustered(
	t *testing.T,
	db *sql.DB,
	path string,
	batchSize int,
) {
	t.Helper()
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	migration := &registry[blockNonceClusteredVersion-1]
	require.Equal(t, "block-nonce-clustered", migration.Name)
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

// TestBlockNonceClusteredKeepsRowsAndSlotOrder upgrades databases with blocks
// spread over several batches, including several blocks in one slot written
// out of hash order, and checks every row survives with the ordinal its
// legacy id order implies.
func TestBlockNonceClusteredKeepsRowsAndSlotOrder(t *testing.T) {
	t.Parallel()
	rows := []legacyBlockNonce{
		{Hash: []byte{0x90}, Slot: 5, Nonce: []byte{1}},
		{Hash: []byte{0x10}, Slot: 5, Nonce: []byte{2}, Checkpoint: true},
		{Hash: []byte{0x40}, Slot: 3, Nonce: []byte{3}},
		{Hash: []byte{0x20}, Slot: 5, Nonce: []byte{4}},
		{Hash: []byte{0x30}, Slot: 9, Nonce: []byte{5}},
		{Hash: []byte{0x60}, Slot: 3, Nonce: []byte{6}},
		{Hash: nil, Slot: 11, Nonce: []byte{7}},
		{Hash: []byte{0x70}, Slot: nil, Nonce: []byte{8}},
		{Hash: []byte{0x80}, Slot: 0, Nonce: []byte{9}},
	}
	want := []string{
		"0/80/0/09/false",
		"3/40/0/03/false", "3/60/1/06/false",
		"5/10/1/02/true", "5/20/2/04/false", "5/90/0/01/false",
		"9/30/0/05/false",
	}
	for _, batchSize := range []int{0, 1, 2, 3, 100} {
		t.Run(fmt.Sprintf("batch_%d", batchSize), func(t *testing.T) {
			t.Parallel()
			db, path := blockNonceClusteredDB(t, rows)
			runBlockNonceClustered(t, db, path, batchSize)

			out, err := db.Query(`
SELECT slot, hex(hash), ordinal, hex(nonce), is_checkpoint
FROM block_nonce ORDER BY slot, hash`)
			require.NoError(t, err)
			var got []string
			for out.Next() {
				var slot, ordinal int64
				var hash, nonce string
				var checkpoint bool
				require.NoError(t, out.Scan(
					&slot, &hash, &ordinal, &nonce, &checkpoint,
				))
				got = append(got, fmt.Sprintf(
					"%d/%s/%d/%s/%t", slot, hash, ordinal, nonce, checkpoint))
			}
			require.NoError(t, out.Err())
			require.NoError(t, out.Close())
			require.Equal(t, want, got)

			var ddl string
			require.NoError(t, db.QueryRow(`
SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'block_nonce'`,
			).Scan(&ddl))
			require.Contains(t, ddl, "WITHOUT ROWID")
			var secondary, copies int
			require.NoError(t, db.QueryRow(`
SELECT COUNT(*) FROM sqlite_master
WHERE type = 'index' AND tbl_name = 'block_nonce' AND sql IS NOT NULL`,
			).Scan(&secondary))
			require.Zero(t, secondary, "the key is the only b-tree")
			require.NoError(t, db.QueryRow(`
SELECT COUNT(*) FROM sqlite_master WHERE name = 'block_nonce_clustered'`,
			).Scan(&copies))
			require.Zero(t, copies)
		})
	}
}

func TestBlockNonceClusteredOnEmptyTable(t *testing.T) {
	t.Parallel()
	db, path := blockNonceClusteredDB(t, nil)
	runBlockNonceClustered(t, db, path, 0)
	var count int
	require.NoError(t, db.QueryRow(`
SELECT COUNT(*) FROM pragma_table_info('block_nonce') WHERE name = 'ordinal'`,
	).Scan(&count))
	require.Equal(t, 1, count)
}
