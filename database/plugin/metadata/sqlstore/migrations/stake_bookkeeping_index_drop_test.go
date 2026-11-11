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
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/stretchr/testify/require"
)

func stakeBookkeepingRowDigest(t *testing.T, db *sql.DB) string {
	t.Helper()
	h := sha256.New()
	for _, table := range []string{
		"reward_live_stake", "account", "account_reward_delta",
		"reward_account_output", "reward_stake_input",
	} {
		rows, err := db.Query("SELECT * FROM " + table + " ORDER BY id")
		require.NoError(t, err)
		cols, err := rows.Columns()
		require.NoError(t, err)
		fmt.Fprintln(h, table, cols)
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			require.NoError(t, rows.Scan(ptrs...))
			fmt.Fprintln(h, vals...)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
	}
	return hex.EncodeToString(h.Sum(nil))
}

// TestStakeBookkeepingIndexDropKeepsRows upgrades a v58 database that holds
// rows in every table whose indexes v59 drops, and requires the rows to be
// byte-identical afterwards, the four indexes gone, the indexes that serve
// their readers present, and a replay of the migration to be a no-op.
func TestStakeBookkeepingIndexDropKeepsRows(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, path, 58)
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	dropped := map[string]string{
		"idx_reward_live_stake_updated_slot":  "reward_live_stake",
		"idx_account_reward_delta_tx_hash":    "account_reward_delta",
		"idx_account_reward_delta_withdrawal": "account_reward_delta",
		"idx_account_drep":                    "account",
	}
	kept := []string{
		"idx_reward_live_stake_cred",
		"idx_reward_live_stake_pool_order",
		"idx_account_reward_delta_w_tx_s_slot",
		"idx_account_reward_delta_credential",
		"idx_account_reward_delta_added_slot",
		"idx_account_drep_active_staking_key",
		"idx_account_active_pool_staking_key",
		"idx_reward_account_output_credential_epoch",
		"idx_reward_account_output_epoch_pool_cred_type",
		"idx_reward_stake_input_epoch_pool_cred",
		"idx_reward_stake_input_cred_epoch",
	}
	for name := range dropped {
		_, ok := indexSQL(t, db, name)
		require.True(t, ok, "v58 must still carry %s", name)
	}

	key := func(i int) []byte {
		sum := sha256.Sum256([]byte{byte(i)})
		return sum[:28]
	}
	for i := range 40 {
		_, err = db.Exec(`INSERT INTO reward_live_stake
(pool_key_hash, staking_key, credential_tag, utxo_stake, reward_stake,
 total_stake, registered, pool_delegation_slot, updated_slot, calculation_version)
VALUES (?, ?, 0, '10', '5', '15', 1, 7, ?, 3)`, key(i%5), key(i), 100+i)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO account
(staking_key, credential_tag, pool, drep, added_slot, created_slot, reward, drep_type, active)
VALUES (?, 0, ?, ?, ?, ?, '5', 0, 1)`, key(i), key(i%5), key(i%3), 100+i, 50)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO account_reward_delta
(staking_key, credential_tag, tx_hash, amount, previous_reward, added_slot, withdrawal, post_snapshot)
VALUES (?, 0, ?, '5', '0', ?, ?, 0)`, key(i), key(100+i), 100+i, i%2)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO reward_account_output
(staking_key, pool_key_hash, reward_type, epoch, credential_tag, amount,
 spendable, guarded, captured_slot, boundary_slot, folded)
VALUES (?, ?, 'member', ?, 0, '5', 1, 0, 90, 90, ?)`, key(i), key(i%5), i%4, i%2)
		require.NoError(t, err)
		_, err = db.Exec(`INSERT INTO reward_stake_input
(pool_key_hash, staking_key, epoch, credential_tag, stake, owner, registered,
 captured_slot, boundary_slot)
VALUES (?, ?, ?, 0, '10', 0, 1, 90, 90)`, key(i%5), key(i), i%4)
		require.NoError(t, err)
	}
	before := stakeBookkeepingRowDigest(t, db)

	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	run := func() {
		runner := migrations.Runner{
			DB:       db,
			Dialect:  "sqlite",
			Registry: registry,
			Locker:   migrations.NewFileLocker(path + ".migrate.lock"),
		}
		require.NoError(t, runner.Run(context.Background()))
	}
	run()

	for name := range dropped {
		_, ok := indexSQL(t, db, name)
		require.False(t, ok, "%s must not survive v59", name)
	}
	for _, name := range kept {
		_, ok := indexSQL(t, db, name)
		require.True(t, ok, "%s must survive v59", name)
	}
	require.Equal(t, before, stakeBookkeepingRowDigest(t, db),
		"v59 changed stored rows")

	_, err = db.Exec(`UPDATE schema_migrations
SET phase = 'expand', dirty = true, completed_at = NULL WHERE version = 59`)
	require.NoError(t, err)
	run()
	require.Equal(t, before, stakeBookkeepingRowDigest(t, db))
}
