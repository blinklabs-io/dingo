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
)

// TestUtxoAddressOrderBackfillCopiesPagePosition covers the v71 upgrade: an
// address row takes the position of its producer's application row, or its
// output's added_slot and block index 0 when the producer was never applied.
func TestUtxoAddressOrderBackfillCopiesPagePosition(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, path, 70)
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx := context.Background()
	for _, statement := range []string{
		`INSERT INTO "transaction" (id, hash) VALUES (1, x'01'), (2, x'02')`,
		`INSERT INTO applied_transaction (tx_id, slot, block_index)
VALUES (1, 40, 3)`,
		`INSERT INTO utxo (id, tx_id, output_idx, added_slot, deleted_slot)
VALUES (10, 1, 5, 40, 0), (11, 2, 7, 25, 0)`,
		`INSERT INTO utxo_address (utxo_id, address)
VALUES (10, x'aa'), (11, x'aa')`,
	} {
		_, err := db.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   migrations.NewFileLocker(path + ".migrate.lock"),
	}
	require.NoError(t, runner.Run(ctx))

	type position struct{ txID, outputIdx, slot, blockIndex int64 }
	read := func(utxoID int64) position {
		var p position
		require.NoError(t, db.QueryRowContext(ctx,
			`SELECT tx_id, output_idx, slot, block_index
FROM utxo_address WHERE utxo_id = ?`, utxoID,
		).Scan(&p.txID, &p.outputIdx, &p.slot, &p.blockIndex))
		return p
	}
	require.Equal(t, position{1, 5, 40, 3}, read(10))
	require.Equal(t, position{2, 7, 25, 0}, read(11))

	var index string
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT name FROM sqlite_master
WHERE type = 'index' AND name = 'idx_utxo_address_order'`,
	).Scan(&index))
}
