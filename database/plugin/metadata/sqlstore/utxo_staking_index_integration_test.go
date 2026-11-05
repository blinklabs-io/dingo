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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostgresUtxoStakingIndexesPartial(t *testing.T) {
	dsn, schema := newPostgresIntegrationSchema(t)
	store := newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema)
	check := func() {
		t.Helper()
		for _, name := range []string{"idx_utxo_staking_key", "idx_utxo_staking_deleted_amount"} {
			var definition string
			require.NoError(
				t,
				store.writeDB.QueryRow(`SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1`, name).
					Scan(&definition),
			)
			require.Contains(t, definition, "WHERE (staking_key IS NOT NULL)")
		}
	}
	check()
	require.NoError(t, store.DropDeferredIndexes())
	require.NoError(t, store.BuildDeferredIndexes())
	check()
}

func TestMySQLUtxoStakingIndexesStayFull(t *testing.T) {
	dsn, database := newMySQLIntegrationDatabase(t)
	store := newIntegrationSQLStore(t, "mysql", dsn, "mysql", database)
	check := func() {
		t.Helper()
		for name, want := range map[string]string{"idx_utxo_staking_key": "staking_key", "idx_utxo_staking_deleted_amount": "credential_tag,staking_key,deleted_slot,amount"} {
			var columns string
			require.NoError(
				t,
				store.writeDB.QueryRow(`SELECT GROUP_CONCAT(column_name ORDER BY seq_in_index) FROM information_schema.statistics WHERE table_schema = DATABASE() AND table_name = 'utxo' AND index_name = ?`, name).
					Scan(&columns),
			)
			require.Equal(t, want, columns)
		}
	}
	check()
	require.NoError(t, store.DropDeferredIndexes())
	require.NoError(t, store.BuildDeferredIndexes())
	check()
}
