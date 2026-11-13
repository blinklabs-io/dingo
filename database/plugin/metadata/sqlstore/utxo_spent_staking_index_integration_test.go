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
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// spentStakingIndexColumns returns the indexed columns of the spent-output
// index and, on PostgreSQL, its predicate.
func spentStakingIndexColumns(
	t *testing.T,
	store *Store,
	schema string,
) (string, string) {
	t.Helper()
	if store.dialect.Name() == "postgres" {
		var definition string
		require.NoError(t, store.writeDB.QueryRow(`SELECT indexdef FROM pg_indexes
WHERE schemaname = $1 AND indexname = $2`, schema, spentStakingIndex).Scan(&definition))
		_, rest, _ := strings.Cut(definition, "(")
		columns, predicate, _ := strings.Cut(rest, ")")
		return columns, strings.TrimSpace(predicate)
	}
	rows, err := store.writeDB.Query(`SELECT column_name
FROM information_schema.statistics
WHERE table_schema = DATABASE() AND table_name = 'utxo' AND index_name = ?
ORDER BY seq_in_index`, spentStakingIndex)
	require.NoError(t, err)
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var column string
		require.NoError(t, rows.Scan(&column))
		columns = append(columns, column)
	}
	require.NoError(t, rows.Err())
	return strings.Join(columns, ", "), ""
}

func testSpentStakingIndex(t *testing.T, store *Store, schema string) {
	t.Helper()
	wantColumns, wantPredicate := "deleted_slot", ""
	if store.dialect.Name() == "postgres" {
		wantColumns = "deleted_slot, credential_tag, staking_key, amount"
		wantPredicate = "WHERE (deleted_slot > 0)"
	}
	check := func(stage string) {
		t.Helper()
		columns, predicate := spentStakingIndexColumns(t, store, schema)
		require.Equal(t, wantColumns, columns, stage)
		require.Equal(t, wantPredicate, predicate, stage)
	}
	check("after migration")
	require.NoError(t, store.DropDeferredIndexes())
	require.NoError(t, store.BuildDeferredIndexes())
	check("after the deferred rebuild")

	// The rollback un-spend and the spent-output history read through the
	// statements that state deleted_slot > 0.
	for i, deleted := range []int64{0, 3, 7} {
		producer := fixtureIdentityRow(t, store, 0, []byte{byte(i + 1)})
		_, err := store.writeDB.Exec(store.dialect.Rebind(
			"INSERT INTO utxo (tx_id, output_idx, credential_tag, "+
				"staking_key, added_slot, deleted_slot, amount, "+
				"payment_script) VALUES (?, 0, 0, ?, 1, ?, '100', FALSE)",
		), producer, []byte{9}, deleted)
		require.NoError(t, err)
	}
	require.NoError(t, store.SetUtxosNotDeletedAfterSlot(5, nil))
	var spent []int64
	rows, err := store.writeDB.Query(
		"SELECT deleted_slot FROM utxo WHERE deleted_slot > 0",
	)
	require.NoError(t, err)
	for rows.Next() {
		var slot int64
		require.NoError(t, rows.Scan(&slot))
		spent = append(spent, slot)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, []int64{3}, spent, "only the output spent after slot 5 is un-spent")
	history, err := store.GetUtxosWithHistory(&models.UtxoHistoryQuery{
		MatchAllAddresses: true,
		Status:            models.UtxoHistoryStatusSpent,
		Limit:             10,
	}, nil)
	require.NoError(t, err)
	require.Len(t, history, 1)
	requireSpentRangeUsesIndex(t, store)
}

// requireSpentRangeUsesIndex plans the rollback un-spend and stake-reference
// statements over a table with many live rows and a few spent ones. PostgreSQL
// plans the bound-parameter form, which cannot prove deleted_slot > 0 on its
// own: the partial index is chosen because the statement states it, and the
// same statement without it must not be.
//
// idx_utxo_deleted_payment_script also leads with deleted_slot and holds every
// row, so the planner may pick it for a spent-row range. It is dropped from
// this throwaway schema so that the choice rests on the statement text alone.
func requireSpentRangeUsesIndex(t *testing.T, store *Store) {
	t.Helper()
	dropSupply := "DROP INDEX idx_utxo_deleted_payment_script"
	if store.dialect.Name() == "mysql" {
		dropSupply += " ON utxo"
	}
	_, err := store.writeDB.Exec(dropSupply)
	require.NoError(t, err)
	// Each output names its own producer's identity row.
	transaction := store.dialect.QuoteIdentifier("transaction")
	var seed []string
	if store.dialect.Name() == "postgres" {
		seed = []string{
			"INSERT INTO " + transaction + " (hash) " +
				"SELECT decode(lpad(to_hex(g), 64, '0'), 'hex') " +
				"FROM generate_series(1, 50000) g",
			"INSERT INTO utxo (tx_id, output_idx, credential_tag, " +
				"staking_key, added_slot, deleted_slot, amount, payment_script) " +
				"SELECT t.id, 0, 0, " +
				"decode(lpad(to_hex(g % 997), 56, '0'), 'hex'), g, " +
				"CASE WHEN g % 50 = 0 THEN g ELSE 0 END, '100', FALSE " +
				"FROM generate_series(1, 50000) g JOIN " + transaction +
				" t ON t.hash = decode(lpad(to_hex(g), 64, '0'), 'hex')",
		}
	} else {
		const series = "WITH RECURSIVE g(n) AS (SELECT 1 UNION ALL " +
			"SELECT n + 1 FROM g WHERE n < 50000) "
		seed = []string{
			"SET SESSION cte_max_recursion_depth = 100000",
			"INSERT INTO " + transaction + " (hash) " + series +
				"SELECT UNHEX(LPAD(HEX(n), 64, '0')) FROM g",
			"INSERT INTO utxo (tx_id, output_idx, credential_tag, " +
				"staking_key, added_slot, deleted_slot, amount, payment_script) " +
				series + "SELECT t.id, 0, 0, " +
				"UNHEX(LPAD(HEX(n % 997), 56, '0')), n, " +
				"IF(n % 50 = 0, n, 0), '100', FALSE FROM g JOIN " +
				transaction + " t ON t.hash = UNHEX(LPAD(HEX(n), 64, '0'))",
		}
	}
	for _, statement := range seed {
		_, err = store.writeDB.Exec(statement)
		require.NoError(t, err)
	}
	analyze := "ANALYZE TABLE utxo"
	if store.dialect.Name() == "postgres" {
		analyze = "ANALYZE utxo"
	}
	_, err = store.writeDB.Exec(analyze)
	require.NoError(t, err)

	// Each name is distinct: the pooled connection keeps its prepared
	// statements and the driver's own statement cache refers to them.
	prepared := 0
	explain := func(statement string) string {
		t.Helper()
		ctx := t.Context()
		conn, err := store.writeDB.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		var rows *sql.Rows
		if store.dialect.Name() == "postgres" {
			_, err = conn.ExecContext(ctx, "SET plan_cache_mode = force_generic_plan")
			require.NoError(t, err)
			name := fmt.Sprintf("spent_range_%d", prepared)
			prepared++
			_, err = conn.ExecContext(ctx, "PREPARE "+name+"(bigint) AS "+
				strings.Replace(statement, "?", "$1", 1))
			require.NoError(t, err)
			rows, err = conn.QueryContext(ctx, "EXPLAIN EXECUTE "+name+"(49000)")
			require.NoError(t, err)
		} else {
			rows, err = conn.QueryContext(ctx,
				"EXPLAIN "+strings.Replace(statement, "?", "49000", 1))
			require.NoError(t, err)
		}
		defer rows.Close()
		columns, err := rows.Columns()
		require.NoError(t, err)
		var out []string
		for rows.Next() {
			cells := make([]sql.NullString, len(columns))
			dest := make([]any, len(columns))
			for i := range cells {
				dest[i] = &cells[i]
			}
			require.NoError(t, rows.Scan(dest...))
			for _, cell := range cells {
				out = append(out, cell.String)
			}
		}
		require.NoError(t, rows.Err())
		return strings.Join(out, " | ")
	}
	for name, statement := range map[string]string{
		"un-spend":    utxoUnspendAfterSlotQuery,
		"stake sweep": utxoStakeRefsDeletedAfterSlotQuery,
	} {
		require.Contains(t, explain(statement), spentStakingIndex, name)
	}
	if store.dialect.Name() == "postgres" {
		without := strings.TrimSuffix(utxoStakeRefsDeletedAfterSlotQuery,
			" AND deleted_slot > 0")
		plan := explain(without)
		require.NotContains(t, plan, spentStakingIndex,
			"the bound parameter alone does not imply the index predicate")
		require.Contains(t, plan, "Seq Scan on utxo")
	}
}

func TestPostgresSpentStakingIndex(t *testing.T) {
	t.Parallel()
	dsn, schema := newPostgresIntegrationSchema(t)
	testSpentStakingIndex(
		t,
		newIntegrationSQLStore(t, "pgx", dsn, "postgres", schema),
		schema,
	)
}

func TestMySQLSpentStakingIndex(t *testing.T) {
	t.Parallel()
	dsn, database := newMySQLIntegrationDatabase(t)
	testSpentStakingIndex(
		t,
		newIntegrationSQLStore(t, "mysql", dsn, "mysql", database),
		database,
	)
}
