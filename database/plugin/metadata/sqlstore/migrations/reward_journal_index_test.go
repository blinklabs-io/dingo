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

func TestRewardJournalCredentialIndexMigrationPreservesRows(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "metadata.sqlite")
	migrations.SeedMigratedPrefix(t, path, 71)
	db, err := sql.Open("sqlite", "file:"+path+"?"+testDBPragmas)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	// The seeded schema must predate the index swap, or the run below
	// applies nothing and every later assertion holds without it.
	for _, name := range []string{
		"idx_account_reward_delta_w_tx_s_slot",
		"idx_account_reward_delta_credential",
	} {
		_, ok := indexSQL(t, db, name)
		require.True(t, ok, "%s must exist before the upgrade", name)
	}
	_, replaced := indexSQL(
		t, db, "idx_account_reward_delta_credential_slot_kind_source",
	)
	require.False(t, replaced, "the upgrade must not be applied yet")

	for i := range 3 {
		_, err = db.Exec(`INSERT INTO account_reward_delta
(staking_key, credential_tag, tx_hash, amount, added_slot, withdrawal)
VALUES (?, ?, ?, '1', ?, ?)`,
			[]byte{byte(i + 1)}, i%2, []byte{byte(10 + i)}, 100+i, i%2,
		)
		require.NoError(t, err)
	}
	registry, err := migrations.SQLiteRegistry()
	require.NoError(t, err)
	runner := migrations.Runner{
		DB:       db,
		Dialect:  "sqlite",
		Registry: registry,
		Locker:   migrations.NewFileLocker(path + ".migrate.lock"),
	}
	require.NoError(t, runner.Run(context.Background()))

	for _, name := range []string{
		"idx_account_reward_delta_w_tx_s_slot",
		"idx_account_reward_delta_credential",
	} {
		_, ok := indexSQL(t, db, name)
		require.False(t, ok, "%s must be replaced", name)
	}
	_, ok := indexSQL(
		t, db, "idx_account_reward_delta_credential_slot_kind_source",
	)
	require.True(t, ok)
	rows, err := db.Query(`PRAGMA index_info(
'idx_account_reward_delta_credential_slot_kind_source')`)
	require.NoError(t, err)
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var seq, cid int
		var name string
		require.NoError(t, rows.Scan(&seq, &cid, &name))
		columns = append(columns, name)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{
		"credential_tag", "staking_key", "added_slot", "withdrawal", "tx_hash",
	}, columns)
	var count int
	require.NoError(t, db.QueryRow(
		`SELECT count(*) FROM account_reward_delta`,
	).Scan(&count))
	require.Equal(t, 3, count)
	_, err = db.Exec(`INSERT INTO account_reward_delta
(staking_key, credential_tag, tx_hash, amount, added_slot, withdrawal)
VALUES (?, ?, ?, '2', ?, ?)`, []byte{1}, 0, []byte{10}, 100, 0)
	require.Error(t, err, "the reordered key must retain journal uniqueness")
}
