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

package sqlstore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgresRebind(t *testing.T) {
	t.Parallel()
	query := `SELECT ?, '?', "?", value -- ?
FROM things WHERE a = ? AND note = 'it''s ?' /* ? */ AND b = ?`
	require.Equal(
		t,
		`SELECT $1, '?', "?", value -- ?
FROM things WHERE a = $2 AND note = 'it''s ?' /* ? */ AND b = $3`,
		PostgresDialect().Rebind(query),
	)
}

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()
	require.Equal(t, `"a""b"`, SQLiteDialect().QuoteIdentifier(`a"b`))
	require.Equal(t, "`a``b`", MySQLDialect().QuoteIdentifier("a`b"))
}

func TestTranslateMySQLReservedIdentifiers(t *testing.T) {
	t.Parallel()
	query := `SELECT "transaction"."hash", "index" FROM "transaction"`
	require.Equal(
		t,
		"SELECT `transaction`.`hash`, `index` FROM `transaction`",
		translateMySQLReservedIdentifiers(query),
	)
}

func TestTranslateMySQLReservedIdentifiersPreservesLiteralsAndComments(
	t *testing.T,
) {
	t.Parallel()
	query := `SELECT '"not an identifier"', "transaction" -- "comment"
FROM "transaction" /* "comment" */`
	require.Equal(
		t,
		"SELECT '\"not an identifier\"', `transaction` -- \"comment\"\nFROM `transaction` /* \"comment\" */",
		translateMySQLReservedIdentifiers(query),
	)
}

func TestMySQLDeferredIndexDDLUsesPrefixes(t *testing.T) {
	t.Parallel()
	dialect := MySQLDialect()
	require.Equal(
		t,
		"CREATE INDEX `idx_utxo_deleted_payment_script` ON `utxo` (`deleted_slot`, `payment_script`, `amount`(255))",
		dialect.CreateIndexSQL(
			"idx_utxo_deleted_payment_script",
			"utxo",
			[]string{"deleted_slot", "payment_script", "amount"},
		),
	)
	require.Equal(t,
		"DROP INDEX `idx_utxo_deleted_payment_script` ON `utxo`",
		dialect.DropIndexSQL("idx_utxo_deleted_payment_script", "utxo"),
	)
	require.False(t, dialect.CanDropIndex("idx_utxo_spent_at_tx_id", "utxo"))
	require.True(t, dialect.CanDropIndex("idx_utxo_payment_key", "utxo"))
}

func TestMySQLDoNothingUsesAnInsertedColumn(t *testing.T) {
	t.Parallel()
	query := `INSERT INTO sync_state (sync_key, value) VALUES (?, ?)
ON CONFLICT (sync_key) DO NOTHING`
	got := translateMySQLUpsert(query)
	require.Contains(t, got, "ON DUPLICATE KEY UPDATE sync_key = sync_key")
	require.NotContains(t, got, "id = id")
}

func TestMySQLReturningTranslationQuotesReservedIdentifiers(t *testing.T) {
	t.Parallel()
	query := `INSERT INTO "transaction" (hash) VALUES (?) RETURNING id`
	base, _ := translateMySQLReturning(query)
	base = translateMySQLReservedIdentifiers(base)
	require.Contains(t, base, "INSERT INTO `transaction`")
}

func TestMySQLForeignKeyIndexErrorDetection(t *testing.T) {
	t.Parallel()
	require.True(t, isMySQLForeignKeyIndexError(
		fmt.Errorf(
			"Error 1553 (HY000): Cannot drop index: needed in a foreign key constraint",
		),
	))
	require.False(t, isMySQLForeignKeyIndexError(
		fmt.Errorf("Error 1553 (HY000): unrelated DDL failure"),
	))
}
