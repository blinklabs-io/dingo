//go:build dingo_db_integration

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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The spender indexes are partial on Postgres after the schema migration and
// after a deferred drop and rebuild.
func TestPostgresUtxoSpenderIndexesPartial(t *testing.T) {
	dsn, schema := newPostgresIntegrationSchema(t)
	store := newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema)
	check := func(stage string) {
		t.Helper()
		for _, column := range spenderColumns {
			var definition string
			require.NoError(t, store.writeDB.QueryRow(`
SELECT indexdef FROM pg_indexes
WHERE schemaname = current_schema() AND indexname = $1`,
				"idx_utxo_"+column,
			).Scan(&definition))
			require.Contains(t, definition,
				"WHERE ("+column+" IS NOT NULL)",
				"%s: idx_utxo_%s must be partial: %s",
				stage, column, definition)
		}
	}
	check("after migration")
	require.NoError(t, store.DropDeferredIndexes())
	require.NoError(t, store.BuildDeferredIndexes())
	check("after rebuild")
}

// MySQL has no partial indexes and its foreign keys need a full index on
// each spender column, so the migration leaves those indexes alone.
func TestMySQLUtxoSpenderIndexesStayFull(t *testing.T) {
	dsn, database := newMySQLIntegrationDatabase(t)
	store := newIntegrationSQLStore(t, "mysql", dsn, "mysql", database)
	for _, column := range spenderColumns {
		var columns string
		require.NoError(t, store.writeDB.QueryRow(`
SELECT GROUP_CONCAT(column_name ORDER BY seq_in_index)
FROM information_schema.statistics
WHERE table_schema = DATABASE() AND table_name = 'utxo'
  AND index_name = ?`,
			"idx_utxo_"+column,
		).Scan(&columns))
		require.Equal(t, column, strings.ToLower(columns))
	}
}
