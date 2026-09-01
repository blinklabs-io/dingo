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
	require.Len(t, registry, 7)
	require.Equal(t, 1, registry[0].Version)
	require.Equal(t, "v1alpha1", registry[0].Name)
	require.GreaterOrEqual(t, len(registry[0].SQL["sqlite"].Expand), 303)
	require.Contains(
		t,
		registry[0].SQL["sqlite"].Expand,
		"CREATE INDEX IF NOT EXISTS `idx_pool_opcert_sequence_pool_sequence` ON `pool_opcert_sequence`(`pool_key_hash`,`sequence`)",
	)
	require.Equal(t, 2, registry[1].Version)
	require.Equal(t, "leios-key-registration", registry[1].Name)
	require.Contains(
		t,
		registry[1].SQL["sqlite"].Expand,
		"ALTER TABLE `pool` ADD COLUMN `leios_key_public` blob",
	)
	require.Equal(t, 3, registry[2].Version)
	require.Equal(t, "token-registry-metadata", registry[2].Name)
	require.Contains(
		t,
		registry[2].SQL["sqlite"].Expand,
		"CREATE UNIQUE INDEX IF NOT EXISTS `idx_token_registry_entry_subject`"+
			" ON `token_registry_entry`(`subject`)",
	)
	require.Equal(t, 4, registry[3].Version)
	require.Equal(t, "account-import-baseline", registry[3].Name)
	require.Len(t, registry[3].SQL["sqlite"].Expand, 2)
	require.Contains(
		t,
		registry[3].SQL["sqlite"].Expand[0],
		"CREATE TABLE IF NOT EXISTS `account_import_baseline`",
	)
	require.Contains(
		t,
		registry[3].SQL["sqlite"].Expand[1],
		"INSERT INTO `account_import_baseline`",
	)
	require.Equal(t, 5, registry[4].Version)
	require.Equal(t, "leios-snapshot-keys", registry[4].Name)
	require.Equal(t, []string{
		"ALTER TABLE `pool_stake_snapshot` ADD COLUMN `leios_key_public` blob",
		"ALTER TABLE `pool_stake_snapshot` ADD COLUMN `leios_key_possession_proof` blob",
	}, registry[4].SQL["sqlite"].Expand)
	require.Equal(t, 6, registry[5].Version)
	require.Equal(
		t,
		"governance-ratification-history",
		registry[5].Name,
	)
	require.Contains(
		t,
		registry[5].SQL["sqlite"].Expand[0],
		"CREATE TABLE IF NOT EXISTS `governance_proposal_ratification_history`",
	)
	require.Contains(
		t,
		registry[5].SQL["sqlite"].Expand[3],
		"INSERT INTO `governance_proposal_ratification_history`",
	)
	require.Equal(t, 7, registry[6].Version)
	require.Equal(t, "account-import-deposit", registry[6].Name)
	require.Equal(t, []string{
		"ALTER TABLE `account_import_baseline` ADD COLUMN `deposit_amount` text",
	}, registry[6].SQL["sqlite"].Expand)
}

// TestMySQLRegistryPrefixesAccountBaselinePrimaryKey guards the v4 migration's
// dialect hazard: `staking_key` is a blob in the composite primary key, and
// MySQL rejects a key over a VARBINARY column without a prefix length. The
// backfill statement in the same migration must not be rewritten at all.
func TestMySQLRegistryPrefixesAccountBaselinePrimaryKey(t *testing.T) {
	t.Parallel()
	registry, err := MySQLRegistry()
	require.NoError(t, err)
	expand := registry[3].SQL["mysql"].Expand
	require.Len(t, expand, 2)
	require.Contains(t, expand[0], "`staking_key` blob NOT NULL")
	require.Contains(t, expand[0], "`staking_key`(255)")
	require.NotContains(t, expand[1], "ON DUPLICATE KEY")
	require.Contains(t, expand[1], "LEFT JOIN `account_import_baseline`")
	// The certificate-history filters must survive as plain subqueries: they
	// read tables other than the INSERT's target, which is what keeps MySQL
	// from raising error 1093, and no key prefix belongs in a predicate.
	require.Equal(t, 10, strings.Count(expand[1], "NOT EXISTS (SELECT 1 FROM"))
	require.NotContains(t, expand[1], "cert.`staking_key`(255)")
}

// TestPostgresRegistryTypesAccountBaseline checks the v4 table picks up the
// PostgreSQL type translation, and that the backfill's identifiers are
// requoted rather than left as MySQL backticks.
func TestPostgresRegistryTypesAccountBaseline(t *testing.T) {
	t.Parallel()
	registry, err := PostgresRegistry()
	require.NoError(t, err)
	expand := registry[3].SQL["postgres"].Expand
	require.Len(t, expand, 2)
	require.Contains(t, expand[0], `"staking_key" BYTEA NOT NULL`)
	require.Contains(t, expand[0], `"active" BOOLEAN NOT NULL DEFAULT true`)
	require.NotContains(t, expand[0], "`")
	require.NotContains(t, expand[1], "`")
	require.Contains(t, expand[1], `LEFT JOIN "account_import_baseline"`)
	require.Equal(t, 10, strings.Count(expand[1], "NOT EXISTS (SELECT 1 FROM"))
	require.Contains(t, expand[1], `FROM "stake_vote_registration_delegation"`)
}

// TestMySQLRegistryPrefixesTokenRegistrySubjectIndex guards the token registry
// migration's one dialect hazard: `subject` is a TEXT column, and MySQL
// rejects a unique index on TEXT without a prefix length. The translation
// derives that prefix from the CREATE TABLE in the same migration, so this
// breaks if v3 ever stops carrying its own table definition.
func TestMySQLRegistryPrefixesTokenRegistrySubjectIndex(t *testing.T) {
	t.Parallel()
	registry, err := MySQLRegistry()
	require.NoError(t, err)
	require.Contains(
		t,
		registry[2].SQL["mysql"].Expand,
		"CREATE UNIQUE INDEX `idx_token_registry_entry_subject`"+
			" ON `token_registry_entry`(`subject`(255))",
	)
}

func TestMySQLRegistryPrefixesPoolOpCertSequenceIndex(t *testing.T) {
	t.Parallel()
	registry, err := MySQLRegistry()
	require.NoError(t, err)
	require.NoError(t, validateRegistry(registry, "mysql"))
	require.Len(t, registry, 7)
	require.Contains(
		t,
		registry[0].SQL["mysql"].Expand,
		"CREATE INDEX `idx_pool_opcert_sequence_pool_sequence` ON `pool_opcert_sequence`(`pool_key_hash`(255),`sequence`)",
	)
}

func TestRatificationHistoryMigrationTranslatesForProviders(t *testing.T) {
	t.Parallel()

	postgres, err := PostgresRegistry()
	require.NoError(t, err)
	postgresSQL := strings.Join(postgres[5].SQL["postgres"].Expand, "\n")
	require.Contains(t, postgresSQL, `"id" BIGSERIAL PRIMARY KEY`)
	require.Contains(t, postgresSQL, `"proposal_id" BIGINT NOT NULL`)
	require.NotContains(t, postgresSQL, "`")

	mysql, err := MySQLRegistry()
	require.NoError(t, err)
	mysqlSQL := strings.Join(mysql[5].SQL["mysql"].Expand, "\n")
	require.Contains(t, mysqlSQL, "`id` BIGINT AUTO_INCREMENT PRIMARY KEY")
	require.Contains(
		t,
		mysqlSQL,
		"FOREIGN KEY (`proposal_id`) REFERENCES `governance_proposal`(`id`)",
	)
	require.NotContains(t, mysqlSQL, "CREATE INDEX IF NOT EXISTS")
}

func TestMySQLSchemaTranslationPrefixesBlobIndexes(t *testing.T) {
	expand, err := loadSQL("v1/sqlite/expand.sql")
	require.NoError(t, err)
	translated := translateSchemaSQLInSchema(expand, "mysql", expand)
	for _, statement := range translated {
		if strings.Contains(
			statement,
			"idx_account_inactivity_activation_staking_key",
		) {
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
	translated := translateSchemaSQLInSchema(expand, "postgres", expand)
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
	statements, err := splitSQL(
		"CREATE/*inline*/TABLE thing (id INTEGER); SELECT/*x*/1;",
	)
	require.NoError(t, err)
	require.Equal(t, []string{
		"CREATE TABLE thing (id INTEGER)",
		"SELECT 1",
	}, statements)
}
