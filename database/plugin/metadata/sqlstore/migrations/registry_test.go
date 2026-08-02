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

package migrations

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/glebarez/go-sqlite"
	"github.com/stretchr/testify/require"
)

func TestSQLiteRegistry(t *testing.T) {
	t.Parallel()
	registry, err := SQLiteRegistry()
	require.NoError(t, err)
	require.NoError(t, validateRegistry(registry, "sqlite"))
	require.Len(t, registry, 1)
	require.Equal(t, 1, registry[0].Version)
	require.Equal(t, "v1alpha1", registry[0].Name)
	require.GreaterOrEqual(t, len(registry[0].SQL["sqlite"].Expand), 302)
}

func TestSQLiteV1ColumnsDeriveCompleteContract(t *testing.T) {
	t.Parallel()
	columns, err := sqliteV1Columns()
	require.NoError(t, err)
	require.Greater(t, len(columns), 50)
	for table, tableColumns := range columns {
		require.NotEmpty(t, tableColumns, "table %q has no columns", table)
	}
}

func TestMySQLSchemaTranslationPrefixesBlobIndexes(t *testing.T) {
	expand, err := loadSQL("v1/sqlite/expand.sql")
	require.NoError(t, err)
	translated := translateSchemaSQL(expand, "mysql")
	for _, statement := range translated {
		if strings.Contains(statement, "idx_account_inactivity_activation_staking_key") {
			require.Contains(t, statement, "`staking_key`(255)")
		}
		if strings.Contains(statement, "idx_transaction_block_hash") {
			require.Contains(t, statement, "`block_hash`(255)")
		}
		if strings.Contains(statement, "idx_utxo_collateral_by_tx_id") {
			require.NotContains(t, statement, "`collateral_by_tx_id`(255)")
		}
	}
}

func TestPostgresSchemaTranslationUsesCompatibleIntegerTypes(t *testing.T) {
	t.Parallel()
	expand, err := loadSQL("v1/sqlite/expand.sql")
	require.NoError(t, err)
	translated := translateSchemaSQL(expand, "postgres")
	joined := strings.Join(translated, "\n")
	require.Contains(t, joined, "BIGSERIAL PRIMARY KEY")
	// SQLite INTEGER foreign-key columns must be widened alongside the
	// BIGSERIAL IDs they reference; PostgreSQL rejects an integer→bigint FK.
	require.NotRegexp(t, `(?i)\binteger\b`, joined)
	require.Contains(t, joined, "BIGINT")
}

func TestSplitSQL(t *testing.T) {
	t.Parallel()
	statements, err := splitSQL(`
		-- comment with ;
		CREATE TABLE thing (value TEXT DEFAULT ';');
		/* another ; */
		INSERT INTO thing VALUES ('it''s;fine');
	`)
	require.NoError(t, err)
	require.Equal(t, []string{
		"CREATE TABLE thing (value TEXT DEFAULT ';')",
		"INSERT INTO thing VALUES ('it''s;fine')",
	}, statements)
}

func TestRepairSQLiteV1IndexesDropsLegacyRewardDeltaIndexBeforeNormalizing(t *testing.T) {
	t.Parallel()
	db, err := sql.Open(
		"sqlite",
		"file:"+filepath.Join(t.TempDir(), "legacy.sqlite"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	_, err = db.Exec(`
CREATE TABLE account_reward_delta (
 staking_key BLOB NOT NULL,
 credential_tag INTEGER NOT NULL,
 tx_hash BLOB,
 amount TEXT NOT NULL,
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 added_slot INTEGER NOT NULL,
 withdrawal BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE UNIQUE INDEX idx_account_reward_delta_w_tx_s
 ON account_reward_delta(withdrawal, tx_hash, credential_tag, staking_key);
INSERT INTO account_reward_delta
 (staking_key, credential_tag, tx_hash, amount, added_slot)
 VALUES ('stake', 0, NULL, '1', 10), ('stake', 0, NULL, '2', 11);
CREATE TABLE block_nonce (
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 hash BLOB NOT NULL,
 slot INTEGER NOT NULL,
 nonce BLOB,
 is_checkpoint BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE UNIQUE INDEX hash_slot ON block_nonce(hash, slot);
CREATE TABLE account (id INTEGER PRIMARY KEY, staking_key BLOB);
`)
	require.NoError(t, err)
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	require.NoError(t, repairSQLiteV1Indexes(context.Background(), conn))

	var count int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM account_reward_delta WHERE tx_hash = X''",
	).Scan(&count))
	require.Equal(t, 2, count)
	var indexName string
	err = db.QueryRow(
		"SELECT name FROM pragma_index_list('account_reward_delta') WHERE name = ?",
		"idx_account_reward_delta_w_tx_s",
	).Scan(&indexName)
	require.ErrorIs(t, err, sql.ErrNoRows, fmt.Sprintf("legacy index still present: %s", indexName))
}
