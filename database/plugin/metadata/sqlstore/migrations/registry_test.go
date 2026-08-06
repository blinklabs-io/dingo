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
	require.Len(t, registry, 1)
	require.Equal(t, 1, registry[0].Version)
	require.Equal(t, "v1alpha1", registry[0].Name)
	require.GreaterOrEqual(t, len(registry[0].SQL["sqlite"].Expand), 303)
	require.Contains(t, registry[0].SQL["sqlite"].Expand, "CREATE INDEX IF NOT EXISTS `idx_pool_opcert_sequence_pool_sequence` ON `pool_opcert_sequence`(`pool_key_hash`,`sequence`)")
}

func TestMySQLRegistryPrefixesPoolOpCertSequenceIndex(t *testing.T) {
	t.Parallel()
	registry, err := MySQLRegistry()
	require.NoError(t, err)
	require.NoError(t, validateRegistry(registry, "mysql"))
	require.Len(t, registry, 1)
	require.Contains(t, registry[0].SQL["mysql"].Expand, "CREATE INDEX `idx_pool_opcert_sequence_pool_sequence` ON `pool_opcert_sequence`(`pool_key_hash`(255),`sequence`)")
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
