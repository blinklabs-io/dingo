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
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

const rewardOutputReshapeVersion = 56

func rewardOutputIndexNames(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.Query(`
SELECT name FROM sqlite_master
WHERE type = 'index' AND tbl_name = 'reward_account_output'
  AND name LIKE 'idx_%' ORDER BY name`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())
	return names
}

func rewardOutputRowDigest(t *testing.T, db *sql.DB) string {
	t.Helper()
	rows, err := db.Query(`
SELECT id, epoch, credential_tag, hex(staking_key), hex(pool_key_hash),
       reward_type, amount, spendable, guarded, captured_slot, boundary_slot,
       folded
FROM reward_account_output ORDER BY id`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	hash := sha256.New()
	var count int
	for rows.Next() {
		var id, epoch, tag, captured, boundary int64
		var key, pool, rewardType, amount string
		var spendable, guarded, folded bool
		require.NoError(t, rows.Scan(
			&id, &epoch, &tag, &key, &pool, &rewardType, &amount, &spendable,
			&guarded, &captured, &boundary, &folded,
		))
		fmt.Fprintln(hash, id, epoch, tag, key, pool, rewardType, amount,
			spendable, guarded, captured, boundary, folded)
		count++
	}
	require.NoError(t, rows.Err())
	require.Positive(t, count)
	return hex.EncodeToString(hash.Sum(nil))
}

// TestRewardAccountOutputIndexReshapeKeepsRows upgrades a database at the
// schema before the reshape, holding reward outputs in every flag state, and
// checks the rows are byte-identical afterwards, the old indexes are gone,
// the new ones exist, the unique key still rejects a duplicate, and the
// upsert the precompute issues still resolves its conflict target.
func TestRewardAccountOutputIndexReshapeKeepsRows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, databasePath, rewardOutputReshapeVersion-1)
	db, err := sql.Open("sqlite", "file:"+databasePath+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	require.Equal(t, []string{
		"idx_reward_account_output_boundary_slot",
		"idx_reward_account_output_captured_slot",
		"idx_reward_account_output_credential_pending",
		"idx_reward_account_output_credential_spendable",
		"idx_reward_account_output_credential_spendable_guarded",
		"idx_reward_account_output_epoch_cred_pool_type",
		"idx_reward_account_output_pending_round",
	}, rewardOutputIndexNames(t, db))

	for i := range 300 {
		key := make([]byte, 28)
		key[0], key[27] = byte(i), byte(i>>8)
		pool := make([]byte, 28)
		pool[0] = byte(i % 7)
		_, err := db.Exec(`
INSERT INTO reward_account_output (
    staking_key, pool_key_hash, reward_type, epoch, credential_tag, amount,
    spendable, guarded, captured_slot, boundary_slot, folded
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			key, pool, []string{"member", "leader"}[i%2], 1+i%4, i%2,
			fmt.Sprint(1000+i), i%5 != 0, i%11 == 3, 100+i%4, 200+i%4,
			i%3 == 0,
		)
		require.NoError(t, err)
	}
	before := rewardOutputRowDigest(t, db)

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   migrations.NewFileLocker(databasePath + ".migrate.lock"),
	}
	require.NoError(t, runner.Run(ctx))
	require.NoError(t, runner.Run(ctx), "replaying the upgrade is a no-op")

	require.Equal(t, before, rewardOutputRowDigest(t, db))
	require.Equal(t, []string{
		"idx_reward_account_output_captured_slot",
		"idx_reward_account_output_credential_epoch",
		"idx_reward_account_output_epoch_pool_cred_type",
		"idx_reward_account_output_pending_round",
	}, rewardOutputIndexNames(t, db))
	definition, ok := indexSQL(t, db,
		"idx_reward_account_output_epoch_pool_cred_type")
	require.True(t, ok)
	require.Contains(t, definition, "UNIQUE")

	key := bytes.Repeat([]byte{0xff}, 28)
	pool := make([]byte, 28)
	insert := `
INSERT INTO reward_account_output (
    staking_key, pool_key_hash, reward_type, epoch, credential_tag, amount,
    spendable, guarded, captured_slot, boundary_slot
) VALUES (?, ?, 'member', 1, 0, '5', TRUE, FALSE, 100, 200)`
	_, err = db.Exec(insert, key, pool)
	require.NoError(t, err)
	_, err = db.Exec(insert, key, pool)
	require.ErrorContains(t, err, "UNIQUE constraint failed")
	_, err = db.Exec(insert+`
ON CONFLICT (epoch, credential_tag, staking_key, pool_key_hash, reward_type)
DO UPDATE SET amount = excluded.amount`, key, pool)
	require.NoError(t, err)
}

func TestRewardAccountOutputIndexReshapeTranslatesForProviders(t *testing.T) {
	t.Parallel()

	postgres, err := migrations.PostgresRegistry()
	require.NoError(t, err)
	require.Equal(t, rewardOutputReshapeVersion, postgres[55].Version)
	postgresSQL := strings.Join(postgres[55].SQL["postgres"].Expand, "\n")
	require.Contains(t, postgresSQL, `CREATE INDEX IF NOT EXISTS "idx_reward_account_output_credential_epoch"`)
	require.Contains(t, postgresSQL, `CREATE UNIQUE INDEX IF NOT EXISTS "idx_reward_account_output_epoch_pool_cred_type"`)
	require.Contains(t, postgresSQL, `DROP INDEX IF EXISTS "idx_reward_account_output_epoch_cred_pool_type"`)
	require.Contains(t, postgresSQL, `DROP INDEX IF EXISTS "idx_reward_account_output_credential_pending"`)
	require.NotContains(t, postgresSQL, "`")

	mysql, err := migrations.MySQLRegistry()
	require.NoError(t, err)
	mysqlSQL := strings.Join(mysql[55].SQL["mysql"].Expand, "\n")
	require.Contains(t, mysqlSQL, "(`credential_tag`, `staking_key`(255), `epoch`, `pool_key_hash`(255),")
	require.Contains(t, mysqlSQL, "(`epoch`, `pool_key_hash`(255), `credential_tag`, `staking_key`(255),")
	require.Contains(t, mysqlSQL, "DROP INDEX `idx_reward_account_output_epoch_cred_pool_type`")
	require.NotContains(t, mysqlSQL, "credential_pending",
		"the covering index was never created on MySQL")
}
