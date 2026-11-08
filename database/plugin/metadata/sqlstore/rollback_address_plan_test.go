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

func TestRollbackAddressLookupUsesDeletedSlotRange(t *testing.T) {
	t.Parallel()
	s := newFilePlannerStatsStore(t, prometheus.NewRegistry())
	_, err := s.writeDB.Exec(`WITH RECURSIVE n(i) AS (
 SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<20000
) INSERT INTO utxo(tx_id,output_idx,added_slot,deleted_slot,amount)
SELECT randomblob(32),0,1,2,'1' FROM n`)
	require.NoError(t, err)
	_, err = s.writeDB.Exec(`INSERT INTO utxo(tx_id,output_idx,added_slot,deleted_slot,amount)
 VALUES(randomblob(32),0,1,101,'1'),(randomblob(32),0,1,102,'1'),(randomblob(32),0,1,103,'1')`)
	require.NoError(t, err)
	_, err = s.writeDB.Exec(`INSERT INTO utxo_address(utxo_id,address) VALUES(20002,x'01')`)
	require.NoError(t, err)
	_, err = s.writeDB.Exec("ANALYZE")
	require.NoError(t, err)
	// An import's stale statistics can describe all historical spends as one
	// deleted-slot value, so LIMIT/rowid ordering beats the useful range index.
	_, err = s.writeDB.Exec("UPDATE sqlite_stat1 SET stat='20003 20003 20003 20003' WHERE idx IN ('idx_utxo_deleted_payment_script','idx_utxo_deleted_staking_amount')")
	require.NoError(t, err)
	var hasStat4 int
	require.NoError(t, s.writeDB.QueryRow("SELECT count(*) FROM sqlite_master WHERE name='sqlite_stat4'").Scan(&hasStat4))
	if hasStat4 > 0 {
		_, err = s.writeDB.Exec("DELETE FROM sqlite_stat4 WHERE idx IN ('idx_utxo_deleted_payment_script','idx_utxo_deleted_staking_amount')")
		require.NoError(t, err)
	}
	require.NoError(t, s.refreshReadPool(context.Background()))
	query := spentUtxosMissingAddressAfterSlotSQL("sqlite")
	legacy := strings.ReplaceAll(query, "+utxo.id", "utxo.id")
	require.Contains(t, explainQueryPlan(t, s, legacy, 100, 0, 128), "SEARCH utxo USING INTEGER PRIMARY KEY", "fixture must reproduce the retained-history scan")
	plan := explainQueryPlan(t, s, query, 100, 0, 128)
	require.Contains(t, plan, "deleted_slot>?")
	require.NotContains(t, plan, "SEARCH utxo USING INTEGER PRIMARY KEY")
	require.NotContains(t, plan, "SCAN utxo")
	got, err := s.GetSpentUtxosMissingAddressAfterSlot(100, 0, 1, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, uint(20001), got[0].ID)
	next, err := s.GetSpentUtxosMissingAddressAfterSlot(100, got[0].ID, 128, nil)
	require.NoError(t, err)
	require.Len(t, next, 1)
	require.Equal(t, uint(20003), next[0].ID)
	empty, err := s.GetSpentUtxosMissingAddressAfterSlot(103, 0, 128, nil)
	require.NoError(t, err)
	require.Empty(t, empty)
	// A deferred index rebuild may leave no deleted-slot index. The query must
	// remain correct rather than turn a planner hint into an operational error.
	for _, name := range []string{"idx_utxo_deleted_payment_script", "idx_utxo_deleted_staking_amount"} {
		_, err = s.writeDB.Exec(fmt.Sprintf("DROP INDEX %s", name))
		require.NoError(t, err)
	}
	got, err = s.GetSpentUtxosMissingAddressAfterSlot(100, 0, 128, nil)
	require.NoError(t, err)
	require.Len(t, got, 2)
}

func TestRollbackAddressLookupKeepsNonSQLiteQueryShape(t *testing.T) {
	t.Parallel()
	for _, d := range []string{"postgres", "mysql"} {
		q := spentUtxosMissingAddressAfterSlotSQL(d)
		require.Contains(t, q, "AND utxo.id > ?")
		require.Contains(t, q, "ORDER BY utxo.id LIMIT ?")
		require.NotContains(t, q, "+utxo.id")
		require.NotContains(t, q, "INDEXED BY")
	}
}
