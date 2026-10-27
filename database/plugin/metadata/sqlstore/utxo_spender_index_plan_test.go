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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// spenderColumns are the nullable utxo columns naming the transaction that
// spent, collateralised, or referenced the output, each with its own index.
var spenderColumns = []string{
	"spent_at_tx_id",
	"collateral_by_tx_id",
	"referenced_by_tx_id",
}

// newImportedLedgerStatsStore returns a store whose planner statistics were
// gathered over a never-spent UTxO set, as after a ledger-state import, and
// which has since spent a few outputs through every spender column. The
// spends do not grow utxo enough for the incremental refresh to re-analyze it.
func newImportedLedgerStatsStore(t *testing.T) (*Store, []byte) {
	t.Helper()
	store := newFilePlannerStatsStore(t, prometheus.NewRegistry())
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := store.writeDB.Exec(query, args...)
		require.NoError(t, err, query)
	}
	exec(`
WITH RECURSIVE n(i) AS (
    SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 20000
)
INSERT INTO utxo
    (tx_id, output_idx, credential_tag, staking_key,
     added_slot, deleted_slot, amount)
SELECT randomblob(32), 0, i % 2, randomblob(28), 1, 0, '1' FROM n`)
	exec("ANALYZE")

	spender := []byte(fmt.Sprintf("%032d", 1))
	exec(
		`INSERT INTO "transaction" (hash, slot, block_index) VALUES (?, 10, 0)`,
		spender,
	)
	for i, column := range spenderColumns {
		exec(
			"UPDATE utxo SET "+column+" = ? WHERE id = ?",
			spender,
			i+1,
		)
	}
	require.NoError(t, store.refreshReadPool(context.Background()))
	return store, spender
}

func writeQueryPlan(
	t *testing.T,
	store *Store,
	query string,
	args ...any,
) string {
	t.Helper()
	rows, err := store.writeDB.QueryContext(
		context.Background(), "EXPLAIN QUERY PLAN "+query, args...,
	)
	require.NoError(t, err)
	var details []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notUsed, &detail))
		details = append(details, detail)
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	return strings.Join(details, "\n")
}

// Statistics gathered while every spender column was NULL recorded each
// spender index as one value covering the whole table, and the planner then
// walked idx_utxo_staking_deleted_amount for the rollback's one-hash stake
// lookup instead of searching the spender index. With the spender indexes
// holding only non-NULL rows, the same statistics describe an empty index
// and every one-hash rollback statement searches it.
func TestRollbackSpenderLookupsSearchIndexUnderImportStatistics(
	t *testing.T,
) {
	t.Parallel()
	store, spender := newImportedLedgerStatsStore(t)
	statements := map[string]string{
		"idx_utxo_spent_at_tx_id": `
SELECT DISTINCT credential_tag, staking_key FROM utxo
WHERE spent_at_tx_id IN (?)`,
	}
	for _, column := range spenderColumns {
		statements["idx_utxo_"+column+" update"] = "UPDATE utxo SET " +
			column + " = NULL WHERE " + column + " IN (?)"
	}
	for name, query := range statements {
		index := strings.TrimSuffix(name, " update")
		plan := writeQueryPlan(t, store, query, spender)
		require.NotContains(t, plan, "SCAN utxo",
			"%s must not scan utxo, plan:\n%s", name, plan)
		require.Contains(t, plan, index,
			"%s must search %s, plan:\n%s", name, index, plan)
	}
}
