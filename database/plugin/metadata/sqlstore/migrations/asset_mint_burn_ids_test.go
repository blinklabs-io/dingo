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

const mintBurnIDsVersion = 75

type legacyMintBurnRow struct {
	txHash any
	policy any
	name   any
	slot   int64
	qty    string
	txIdx  int64
}

// mintBurnIDsDB returns a database migrated through the version before the
// rebuild, holding transaction rows 1..txs (hash = 32 bytes of the id) and the
// given legacy mint and burn rows.
func mintBurnIDsDB(
	t *testing.T,
	txs int,
	rows []legacyMintBurnRow,
) (*sql.DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, path, mintBurnIDsVersion-1)
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	for id := 1; id <= txs; id++ {
		_, err := db.Exec(
			`INSERT INTO "transaction" (id, hash) VALUES (?, ?)`,
			id, mintBurnTxHash(id),
		)
		require.NoError(t, err)
	}
	for _, row := range rows {
		_, err := db.Exec(`
INSERT INTO asset_mint_burn
  (tx_hash, policy_id, name, fingerprint, slot, quantity, tx_index)
VALUES (?, ?, ?, ?, ?, ?, ?)`, row.txHash, row.policy, row.name,
			[]byte("fp"), row.slot, row.qty, row.txIdx)
		require.NoError(t, err)
	}
	return db, path
}

func mintBurnTxHash(id int) []byte {
	out := make([]byte, 32)
	out[0] = byte(id)
	return out
}

func runMintBurnIDs(t *testing.T, db *sql.DB, path string, batchSize int) {
	t.Helper()
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	migration := &registry[mintBurnIDsVersion-1]
	require.Equal(t, "asset-mint-burn-ids", migration.Name)
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

// TestAssetMintBurnIDsRebuildsByTransactionAndAsset upgrades a table holding
// one asset minted and burned in several transactions, the empty name, a NULL
// name, and rows naming no known transaction, and checks every row of a known
// transaction is kept against one identity per distinct pair, the rows of an
// unknown transaction are dropped, and the table is a clustered table keyed by
// (transaction_id, asset_id) without the by-value columns or their indexes.
func TestAssetMintBurnIDsRebuildsByTransactionAndAsset(t *testing.T) {
	t.Parallel()
	policy, other := assetSplitPolicy(0x01), assetSplitPolicy(0x02)
	legacy := []legacyMintBurnRow{
		{mintBurnTxHash(1), policy, []byte("tok"), 10, "5", 3},
		{mintBurnTxHash(2), policy, []byte("tok"), 20, "-2", 0},
		{mintBurnTxHash(2), policy, []byte{}, 20, "1", 0},
		{mintBurnTxHash(3), other, nil, 30, "7", 1},
		{mintBurnTxHash(9), policy, []byte("tok"), 40, "9", 1},
		{nil, policy, []byte("tok"), 50, "9", 1},
	}
	for _, batchSize := range []int{0, 1, 2, 4} {
		t.Run(fmt.Sprintf("batch_%d", batchSize), func(t *testing.T) {
			t.Parallel()
			db, path := mintBurnIDsDB(t, 3, legacy)
			runMintBurnIDs(t, db, path, batchSize)

			rows, err := db.Query(`
SELECT m.transaction_id, lower(hex(i.policy_id)), lower(hex(i.name)),
       m.slot, m.quantity, m.tx_index
FROM asset_mint_burn m JOIN asset_identity i ON i.id = m.asset_id
ORDER BY m.transaction_id, hex(i.policy_id), hex(i.name)`)
			require.NoError(t, err)
			var got []string
			for rows.Next() {
				var tx, slot, idx int64
				var pol, name, qty string
				require.NoError(t, rows.Scan(&tx, &pol, &name, &slot, &qty, &idx))
				got = append(got, fmt.Sprintf(
					"%d/%s/%s/%d/%s/%d", tx, pol, name, slot, qty, idx))
			}
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			require.Equal(t, []string{
				fmt.Sprintf("1/%x/%x/10/5/3", policy, "tok"),
				fmt.Sprintf("2/%x/%x/20/1/0", policy, ""),
				fmt.Sprintf("2/%x/%x/20/-2/0", policy, "tok"),
				fmt.Sprintf("3/%x/%x/30/7/1", other, ""),
			}, got)

			var ddl string
			require.NoError(t, db.QueryRow(`
SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'asset_mint_burn'`,
			).Scan(&ddl))
			require.True(t, strings.Contains(ddl, "WITHOUT ROWID"), ddl)
			require.Contains(t, ddl, "PRIMARY KEY (`transaction_id`,`asset_id`)")
			for _, column := range []string{"tx_hash", "policy_id", "name", "fingerprint"} {
				require.NotContains(t, ddl, "`"+column+"`")
			}
			var indexes []string
			idx, err := db.Query(`
SELECT name FROM sqlite_master
WHERE type = 'index' AND tbl_name = 'asset_mint_burn' ORDER BY name`)
			require.NoError(t, err)
			for idx.Next() {
				var name string
				require.NoError(t, idx.Scan(&name))
				indexes = append(indexes, name)
			}
			require.NoError(t, idx.Err())
			require.NoError(t, idx.Close())
			require.Equal(t, []string{
				"idx_asset_mint_burn_asset_id", "idx_asset_mint_burn_slot",
			}, indexes)
			var legacyTables int
			require.NoError(t, db.QueryRow(`
SELECT COUNT(*) FROM sqlite_master WHERE name LIKE 'asset_mint_burn_%'
AND type = 'table'`).Scan(&legacyTables))
			require.Zero(t, legacyTables)
		})
	}
}
