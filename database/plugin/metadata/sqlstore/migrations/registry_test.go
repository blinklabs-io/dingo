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
	require.Len(t, registry, 2)
	require.Equal(t, 1, registry[0].Version)
	require.Equal(t, "v1alpha1", registry[0].Name)
	require.GreaterOrEqual(t, len(registry[0].SQL["sqlite"].Expand), 302)
	require.Equal(t, 2, registry[1].Version)
	require.Equal(t, "v2alpha1", registry[1].Name)
	// v2 adds an index and nothing else, so it ships no contract.sql; the
	// loader has to read that absence as empty rather than as an error.
	require.Empty(t, registry[1].SQL["sqlite"].Contract)
}

// TestMySQLTranslationPrefixesBlobIndexAddedByLaterVersion pins the schema
// context a later migration is translated against.
//
// Every CREATE TABLE lives in v1, and MySQL only learns which columns are
// blobs by reading them. v2 indexes pool_key_hash without creating the table
// it belongs to, so translating v2 against its own lone statement would leave
// the prefix length off and MySQL would reject the key. This fails if the
// registry ever goes back to translating a version in isolation.
func TestMySQLTranslationPrefixesBlobIndexAddedByLaterVersion(t *testing.T) {
	t.Parallel()
	registry, err := MySQLRegistry()
	require.NoError(t, err)
	require.Len(t, registry, 2)

	statements := registry[1].SQL["mysql"].Expand
	require.Len(t, statements, 1)
	require.Contains(t, statements[0], "`pool_key_hash`(255)")
	// The prefix belongs to the blob column alone; an integer column given one
	// would be a different error in the same place.
	require.Contains(t, statements[0], "`sequence`)")
	// MySQL has no CREATE INDEX IF NOT EXISTS; re-running is made safe by the
	// runner's duplicate-object tolerance instead.
	require.NotContains(t, statements[0], "IF NOT EXISTS")
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
