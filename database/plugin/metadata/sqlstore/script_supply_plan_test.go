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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The index replaced by the script-locked aggregate was the only
// deleted_slot-leading index with payment_script in it; every other query that
// the planner could send through it must still seek an index, with and without
// planner statistics, and none may name the dropped index.
func TestDroppedPaymentScriptIndexLeavesNoQueryOnAScan(t *testing.T) {
	t.Parallel()
	ids := "(?,?,?,?,?,?,?,?)"
	markQuery, _ := markUtxosDeletedQuery(
		[]int64{1, 2, 3, 4, 5, 6, 7, 8},
		1,
		false,
	)
	queries := map[string]string{
		"live lovelace sum":      `SELECT SUM(CAST(amount AS INTEGER)) FROM utxo WHERE deleted_slot = 0`,
		"live row iteration":     `SELECT tx_id FROM utxo WHERE deleted_slot = 0`,
		"consumed prune":         `SELECT id, tx_id, amount FROM utxo WHERE deleted_slot > 0 AND deleted_slot <= ? LIMIT ?`,
		"rollback unspend":       `UPDATE utxo SET deleted_slot = 0, spent_at_tx_id = NULL WHERE deleted_slot > ?`,
		"rollback stake refs":    utxoStakeRefsDeletedAfterSlotQuery,
		"bulk mark spent":        markQuery,
		"credential live sum":    `SELECT amount FROM utxo WHERE credential_tag = ? AND staking_key = ? AND deleted_slot = 0`,
		"batched spend":          consumeUtxosBatchQuery(4, true),
		"spend state by ref":     getUtxoSpendStateQuery,
		"live by ref":            getLiveUtxoByRefQuery,
		"delete by id":           `DELETE FROM utxo WHERE id IN ` + ids,
		"missing address repair": `SELECT id FROM utxo WHERE deleted_slot > ? AND id > ? ORDER BY id LIMIT ?`,
	}
	for _, analyzed := range []bool{false, true} {
		t.Run(fmt.Sprintf("analyzed=%v", analyzed), func(t *testing.T) {
			t.Parallel()
			store := newMigratedSQLiteStore(t)
			var index int
			require.NoError(t, store.writeDB.QueryRow(
				`SELECT COUNT(*) FROM sqlite_schema WHERE name = 'idx_utxo_deleted_payment_script'`,
			).Scan(&index))
			require.Zero(t, index)
			tx, err := store.writeDB.Begin()
			require.NoError(t, err)
			for i := range 4000 {
				var spent int
				if i%10 < 6 {
					spent = 100 + i/10
				}
				_, err := tx.Exec(
					`INSERT INTO utxo (tx_id, output_idx, payment_key, staking_key, credential_tag, added_slot, deleted_slot, amount, payment_script)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
					[]byte{byte(i), byte(i >> 8), 9},
					i%3,
					[]byte{byte(i % 50)},
					[]byte{byte(i % 40), 3},
					i%2,
					10+i/10,
					spent,
					fmt.Sprint(1_000_000+i),
					i%10 == 3,
				)
				require.NoError(t, err)
			}
			require.NoError(t, tx.Commit())
			if analyzed {
				_, err := store.writeDB.Exec("ANALYZE")
				require.NoError(t, err)
			}
			for name, query := range queries {
				plan := explainSQLPlan(t, store, query)
				require.NotContains(
					t,
					plan,
					"idx_utxo_deleted_payment_script",
					name,
				)
				require.NotContains(t, plan, "SCAN utxo", "%s: %s", name, plan)
			}
		})
	}
}

func explainSQLPlan(t *testing.T, store *Store, query string) string {
	t.Helper()
	if i := strings.Index(query, "\n"); strings.HasPrefix(query, "-- name:") &&
		i > 0 {
		query = query[i+1:]
	}
	rows, err := store.writeDB.QueryContext(
		context.Background(),
		"EXPLAIN QUERY PLAN "+query,
		make([]any, strings.Count(query, "?"))...,
	)
	require.NoError(t, err)
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &unused, &detail))
		plan = append(plan, detail)
	}
	require.NoError(t, rows.Err())
	return strings.Join(plan, " | ")
}

// DeleteUtxosAfterSlot reads the script-locked outputs it is about to remove
// on every rollback, so the read must range-search the added_slot index and
// visit only the rolled-back window, with and without planner statistics.
func TestScriptSupplyRollbackReadUsesAddedSlotIndex(t *testing.T) {
	t.Parallel()
	const rolledBackFrom = int64(2_656_808)
	for _, analyzed := range []bool{false, true} {
		t.Run(fmt.Sprintf("analyzed=%v", analyzed), func(t *testing.T) {
			t.Parallel()
			store := newMigratedSQLiteStore(t)
			seedRollbackUtxos(t, store, 2_000, 64, rolledBackFrom, 40)
			if analyzed {
				analyzeStore(t, store)
			}
			plan := queryPlan(
				t, store.writeDB, scriptSupplyAddedAfterSlotQuery, rolledBackFrom,
			)
			require.Contains(t, plan, "idx_utxo_added_slot (added_slot>?)", plan)
			require.NotContains(t, plan, "SCAN", plan)
		})
	}
}
