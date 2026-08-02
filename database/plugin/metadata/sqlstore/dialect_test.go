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

func TestMySQLDeferredIndexDDLUsesPrefixes(t *testing.T) {
	t.Parallel()
	dialect := MySQLDialect()
	require.Equal(t,
		"CREATE INDEX `idx_utxo_deleted_payment_script` ON `utxo` (`deleted_slot`, `payment_script`, `amount`(255))",
		dialect.CreateIndexSQL("idx_utxo_deleted_payment_script", "utxo", []string{"deleted_slot", "payment_script", "amount"}),
	)
	require.Equal(t,
		"DROP INDEX `idx_utxo_deleted_payment_script` ON `utxo`",
		dialect.DropIndexSQL("idx_utxo_deleted_payment_script", "utxo"),
	)
}
