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

// The migration seeds the aggregate with exactly what the dropped index used
// to serve, leaves every utxo row alone, and drops the index.
func TestScriptLockedSupplyMigrationSeedsAggregate(t *testing.T) {
	t.Parallel()
	for _, withScript := range []bool{true, false} {
		t.Run(fmt.Sprintf("script_outputs=%v", withScript), func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "metadata.sqlite")
			db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			registry, err := migrations.SQLiteRegistry()
			require.NoError(t, err)
			run := func(registry []migrations.Migration) {
				runner := migrations.Runner{
					DB:       db,
					Dialect:  "sqlite",
					Registry: registry,
					Locker:   migrations.NewFileLocker(path + ".migrate.lock"),
				}
				require.NoError(t, runner.Run(context.Background()))
			}
			run(registry[:64])
			_, ok := indexSQL(t, db, "idx_utxo_deleted_payment_script")
			require.True(t, ok)
			_, err = db.Exec(
				"INSERT INTO tip (hash, slot, block_number) VALUES (x'01', 4242, 7)",
			)
			require.NoError(t, err)
			type row struct {
				script  any
				deleted int
				amount  string
			}
			rows := []row{
				{true, 0, "5000000"},
				{true, 0, "9000000000000000"},
				{true, 90, "7000000"},
				{false, 0, "3000000"},
				{nil, 0, "11000000"},
			}
			if !withScript {
				rows = rows[2:]
			}
			var want uint64
			for i, r := range rows {
				_, err := db.Exec(
					"INSERT INTO utxo (tx_id, output_idx, added_slot, deleted_slot, amount, payment_script) VALUES (?, 0, 10, ?, ?, ?)",
					[]byte{byte(i + 1)},
					r.deleted,
					r.amount,
					r.script,
				)
				require.NoError(t, err)
				if r.script == true && r.deleted == 0 {
					var v uint64
					_, err := fmt.Sscan(r.amount, &v)
					require.NoError(t, err)
					want += v
				}
			}
			before := dumpUtxo(t, db)

			run(registry)

			var count int
			require.NoError(t, db.QueryRow(
				"SELECT COUNT(*) FROM script_locked_supply",
			).Scan(&count))
			if want == 0 {
				require.Zero(t, count)
			} else {
				var slot, amount int64
				require.NoError(t, db.QueryRow(
					"SELECT slot, amount FROM script_locked_supply",
				).Scan(&slot, &amount))
				require.Equal(t, 1, count)
				require.EqualValues(t, 4242, slot)
				require.Equal(t, want, uint64(amount))
			}
			_, ok = indexSQL(t, db, "idx_utxo_deleted_payment_script")
			require.False(t, ok)
			require.Equal(t, before, dumpUtxo(t, db))
			// A second run is a no-op.
			run(registry)
			var again int
			require.NoError(t, db.QueryRow(
				"SELECT COUNT(*) FROM script_locked_supply",
			).Scan(&again))
			require.Equal(t, count, again)
		})
	}
}

func dumpUtxo(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(
		"SELECT hex(tx_id), output_idx, added_slot, deleted_slot, amount, payment_script FROM utxo ORDER BY id",
	)
	require.NoError(t, err)
	defer rows.Close()
	var out []string
	for rows.Next() {
		var tx, amount string
		var idx, added, deleted int64
		var script sql.NullBool
		require.NoError(
			t,
			rows.Scan(&tx, &idx, &added, &deleted, &amount, &script),
		)
		out = append(out, fmt.Sprint(tx, idx, added, deleted, amount, script))
	}
	require.NoError(t, rows.Err())
	return out
}
