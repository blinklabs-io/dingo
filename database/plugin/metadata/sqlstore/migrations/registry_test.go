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
		CREATE/* preserve token boundary */TABLE thing (value TEXT DEFAULT ';');
		/* another ; */
		INSERT INTO thing VALUES ('it''s;fine');
	`)
	require.NoError(t, err)
	require.Equal(t, []string{
		"CREATE TABLE thing (value TEXT DEFAULT ';')",
		"INSERT INTO thing VALUES ('it''s;fine')",
	}, statements)
}

func TestSplitSQLPreservesCommentTokenBoundaries(t *testing.T) {
	t.Parallel()
	statements, err := splitSQL("CREATE/*inline*/TABLE thing (id INTEGER); SELECT/*x*/1;")
	require.NoError(t, err)
	require.Equal(t, []string{
		"CREATE TABLE thing (id INTEGER)",
		"SELECT 1",
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
	_, err = conn.ExecContext(context.Background(), "PRAGMA foreign_keys = OFF")
	require.NoError(t, err)
	require.NoError(t, repairSQLiteV1Indexes(context.Background(), conn))

	var count int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM account_reward_delta WHERE tx_hash = X''",
	).Scan(&count))
	require.Equal(t, 2, count)
	var notNull int
	require.NoError(t, db.QueryRow(
		`SELECT "notnull" FROM pragma_table_info('account_reward_delta') WHERE name = 'tx_hash'`,
	).Scan(&notNull))
	require.Equal(t, 1, notNull)
	var indexName string
	err = db.QueryRow(
		"SELECT name FROM pragma_index_list('account_reward_delta') WHERE name = ?",
		"idx_account_reward_delta_w_tx_s",
	).Scan(&indexName)
	require.ErrorIs(t, err, sql.ErrNoRows, fmt.Sprintf("legacy index still present: %s", indexName))
}

func TestAdoptSQLiteV1RepairsLegacyCompatibilityState(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	registry, err := SQLiteRegistry()
	require.NoError(t, err)
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	_, err = conn.ExecContext(context.Background(), "PRAGMA foreign_keys = OFF")
	require.NoError(t, err)
	for _, statement := range registry[0].SQL["sqlite"].Expand {
		_, err = conn.ExecContext(context.Background(), statement)
		require.NoError(t, err)
	}
	// Simulate a pre-cascade table: retain its rows while removing the foreign
	// key definition. Adoption must purge the orphan and rebuild the released
	// constraint before v1 is recorded.
	_, err = conn.ExecContext(context.Background(), `
CREATE TABLE plutus_data_legacy (data BLOB, id INTEGER PRIMARY KEY AUTOINCREMENT, transaction_id INTEGER);
INSERT INTO plutus_data_legacy (data,id,transaction_id) SELECT data,id,transaction_id FROM plutus_data;
DROP TABLE plutus_data;
ALTER TABLE plutus_data_legacy RENAME TO plutus_data`)
	require.NoError(t, err)
	// Recreate the indexes and duplicate/orphan states emitted by supported
	// pre-v1 GORM startup paths.
	for _, index := range []string{
		"idx_pool_stake_epoch_pool", "idx_reward_live_stake_cred",
		"idx_drep_credential", "idx_vote_unique",
	} {
		_, err = conn.ExecContext(context.Background(), "DROP INDEX IF EXISTS `"+index+"`")
		require.NoError(t, err)
	}
	_, err = conn.ExecContext(context.Background(), `
CREATE UNIQUE INDEX idx_account_staking_key ON account(staking_key);
CREATE UNIQUE INDEX idx_drep_credential ON drep(credential);
CREATE UNIQUE INDEX idx_vote_unique ON governance_vote(proposal_id,voter_credential);
CREATE INDEX idx_reward_live_stake_pool ON reward_live_stake(pool_key_hash,total_stake);
CREATE INDEX idx_reward_account_output_credential ON reward_account_output(credential_tag,staking_key,epoch,pool_key_hash,reward_type);
CREATE INDEX idx_addr_tx_staking ON address_transaction(credential_tag,staking_key);
INSERT INTO pool_stake_snapshot
 (epoch,snapshot_type,pool_key_hash,total_stake,stake_denominator,delegator_count,captured_slot,calculation_version)
 VALUES (1,'mark',X'01','10','10',1,1,0),(1,'mark',X'01','11','11',1,2,0);
INSERT INTO reward_live_stake
 (pool_key_hash,staking_key,credential_tag,utxo_stake,reward_stake,total_stake,registered,updated_slot,calculation_version)
 VALUES (X'01',X'02',0,'1','2','3',TRUE,1,0),(X'01',X'02',0,'4','5','6',TRUE,2,0);
INSERT INTO account (staking_key,credential_tag,certificate_id,created_slot)
 VALUES (X'03',0,1,0);
INSERT INTO stake_registration (staking_key,credential_tag,certificate_id,added_slot)
 VALUES (X'03',0,1,123);
INSERT INTO plutus_data (transaction_id,data) VALUES (999,X'01')`)
	require.NoError(t, err)
	require.NoError(t, adoptSQLiteV1(context.Background(), conn, "sqlite"))
	for _, statement := range registry[0].SQL["sqlite"].Expand {
		_, err = conn.ExecContext(context.Background(), statement)
		require.NoError(t, err)
	}
	var count int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM pool_stake_snapshot WHERE epoch = 1 AND snapshot_type = 'mark' AND pool_key_hash = X'01'",
	).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM reward_live_stake WHERE credential_tag = 0 AND staking_key = X'02'",
	).Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM plutus_data WHERE transaction_id = 999",
	).Scan(&count))
	require.Zero(t, count)
	var foreignKeyCount int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM pragma_foreign_key_list('plutus_data')",
	).Scan(&foreignKeyCount))
	require.Equal(t, 1, foreignKeyCount)
	var createdSlot int64
	require.NoError(t, db.QueryRow(
		"SELECT created_slot FROM account WHERE staking_key = X'03'",
	).Scan(&createdSlot))
	require.Equal(t, int64(123), createdSlot)
	var indexCount int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM pragma_index_list('account') WHERE name = 'idx_account_staking_key'",
	).Scan(&indexCount))
	require.Zero(t, indexCount)
}

func TestAdoptSQLiteV1RejectsUnknownSchemaBeforeReferenceCopy(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	_, err := db.Exec("CREATE TABLE mystery (id INTEGER PRIMARY KEY)")
	require.NoError(t, err)
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	err = adoptSQLiteV1(context.Background(), conn, "sqlite")
	require.ErrorIs(t, err, ErrLegacySchema)
	var referenceTableCount int
	require.NoError(t, db.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'utxo_reference_input'",
	).Scan(&referenceTableCount))
	require.Zero(t, referenceTableCount)
}
