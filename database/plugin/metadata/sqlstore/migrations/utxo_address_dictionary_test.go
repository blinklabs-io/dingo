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

const utxoAddressDictionaryVersion = 76

// legacyUtxoAddress is one by-value utxo_address row of the schema before the
// dictionary. A nil Slot leaves the page position NULL, as a row written
// before the order columns existed holds it.
type legacyUtxoAddress struct {
	UtxoID  int64
	Address string
	Slot    *int64
}

func utxoAddressDictionaryDB(
	t *testing.T,
	rows []legacyUtxoAddress,
) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, path, utxoAddressDictionaryVersion-1)
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	for _, row := range rows {
		txID := make([]byte, 32)
		txID[0] = byte(row.UtxoID)
		_, err := db.Exec(`
INSERT INTO utxo (id, tx_id, output_idx, added_slot, deleted_slot, amount)
VALUES (?, ?, 0, 1, 0, '1')`, row.UtxoID, txID)
		require.NoError(t, err)
		var slot, blockIndex, outputIdx, txRow any
		if row.Slot != nil {
			slot, blockIndex = *row.Slot, row.UtxoID%5
			outputIdx, txRow = row.UtxoID%3, row.UtxoID*10
		}
		_, err = db.Exec(`
INSERT INTO utxo_address (utxo_id, address, tx_id, output_idx, slot, block_index)
VALUES (?, ?, ?, ?, ?, ?)`,
			row.UtxoID, []byte(row.Address), txRow, outputIdx, slot, blockIndex)
		require.NoError(t, err)
	}
	return db, path
}

func runUtxoAddressDictionary(
	t *testing.T,
	db *sql.DB,
	path string,
	batchSize int,
) {
	t.Helper()
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	migration := &registry[utxoAddressDictionaryVersion-1]
	require.Equal(t, "utxo-address-dictionary", migration.Name)
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

//go:fix inline
func int64Ptr(v int64) *int64 { return new(v) }

// TestUtxoAddressDictionaryKeepsRowsAndPositions upgrades databases with rows
// spread over several batches and checks each row keeps its address and page
// position, every distinct address is held once, and the by-value column and
// its indexes are gone.
func TestUtxoAddressDictionaryKeepsRowsAndPositions(t *testing.T) {
	t.Parallel()
	rows := []legacyUtxoAddress{
		{UtxoID: 1, Address: "addr_a", Slot: int64Ptr(10)},
		{UtxoID: 2, Address: "addr_b", Slot: int64Ptr(11)},
		{UtxoID: 4, Address: "addr_a"},
		{UtxoID: 7, Address: "addr_c", Slot: int64Ptr(12)},
		{UtxoID: 8, Address: "addr_b", Slot: int64Ptr(13)},
		{UtxoID: 9, Address: "addr_a", Slot: int64Ptr(14)},
	}
	for _, batchSize := range []int{0, 1, 2, 4, 6, 7} {
		t.Run(fmt.Sprintf("batch_%d", batchSize), func(t *testing.T) {
			t.Parallel()
			db, path := utxoAddressDictionaryDB(t, rows)
			runUtxoAddressDictionary(t, db, path, batchSize)

			var distinct int
			require.NoError(t, db.QueryRow(
				`SELECT COUNT(*) FROM address`).Scan(&distinct))
			require.Equal(t, 3, distinct, "one dictionary row per address")

			var columns []string
			colRows, err := db.Query(
				`SELECT name FROM pragma_table_info('utxo_address') ORDER BY cid`)
			require.NoError(t, err)
			for colRows.Next() {
				var name string
				require.NoError(t, colRows.Scan(&name))
				columns = append(columns, name)
			}
			require.NoError(t, colRows.Err())
			require.NoError(t, colRows.Close())
			require.Equal(t, []string{
				"utxo_id", "address_id", "tx_id", "output_idx", "slot",
				"block_index",
			}, columns)

			for _, want := range rows {
				var (
					address          []byte
					slot, blockIndex sql.NullInt64
					outputIdx, txRow sql.NullInt64
				)
				require.NoError(t, db.QueryRow(`
SELECT a.address, ua.slot, ua.block_index, ua.output_idx, ua.tx_id
FROM utxo_address ua JOIN address a ON a.id = ua.address_id
WHERE ua.utxo_id = ?`, want.UtxoID).Scan(
					&address, &slot, &blockIndex, &outputIdx, &txRow,
				))
				require.Equal(t, want.Address, string(address))
				if want.Slot == nil {
					require.False(t, slot.Valid, "NULL position stays NULL")
					require.False(t, txRow.Valid)
					continue
				}
				require.Equal(t, *want.Slot, slot.Int64)
				require.Equal(t, want.UtxoID%5, blockIndex.Int64)
				require.Equal(t, want.UtxoID%3, outputIdx.Int64)
				require.Equal(t, want.UtxoID*10, txRow.Int64)
			}

			var indexes []string
			idxRows, err := db.Query(`
SELECT name FROM sqlite_master
WHERE type = 'index' AND tbl_name = 'utxo_address' AND sql IS NOT NULL
ORDER BY name`)
			require.NoError(t, err)
			for idxRows.Next() {
				var name string
				require.NoError(t, idxRows.Scan(&name))
				indexes = append(indexes, name)
			}
			require.NoError(t, idxRows.Err())
			require.NoError(t, idxRows.Close())
			require.Equal(t, []string{"idx_utxo_address_order"}, indexes)

			var copies int
			require.NoError(t, db.QueryRow(`
SELECT COUNT(*) FROM sqlite_master
WHERE name = 'utxo_address_dictionary'`).Scan(&copies))
			require.Zero(t, copies, "the copy table is renamed away")
		})
	}
}

// TestUtxoAddressDictionaryCascadeStillDeletesRows pins that the rebuilt table
// keeps the cascade from utxo and never deletes dictionary rows.
func TestUtxoAddressDictionaryCascadeStillDeletesRows(t *testing.T) {
	t.Parallel()
	db, path := utxoAddressDictionaryDB(t, []legacyUtxoAddress{
		{UtxoID: 1, Address: "addr_a", Slot: int64Ptr(1)},
		{UtxoID: 2, Address: "addr_b", Slot: int64Ptr(2)},
	})
	runUtxoAddressDictionary(t, db, path, 0)
	_, err := db.Exec("PRAGMA foreign_keys = ON")
	require.NoError(t, err)
	_, err = db.Exec("DELETE FROM utxo WHERE id = 1")
	require.NoError(t, err)
	var rows, addresses int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM utxo_address`).Scan(&rows))
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM address`).Scan(&addresses))
	require.Equal(t, 1, rows)
	require.Equal(t, 2, addresses, "dictionary rows are never deleted")
}

func TestUtxoAddressDictionaryOnEmptyTable(t *testing.T) {
	t.Parallel()
	db, path := utxoAddressDictionaryDB(t, nil)
	runUtxoAddressDictionary(t, db, path, 0)
	var count int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM pragma_table_info('utxo_address')
WHERE name = 'address_id'`).Scan(&count))
	require.Equal(t, 1, count)
}
