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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

func legacyMarkUtxosDeletedAtSlotQuery(
	ids []models.UtxoId,
	slot int64,
) (string, []any) {
	predicate, idArgs := utxoIDPredicate(ids)
	args := append([]any{slot}, idArgs...)
	return "UPDATE utxo SET deleted_slot = ? WHERE deleted_slot = 0 AND (" +
		predicate + ")", args
}

// TestMarkUtxosDeletedAtSlotUsesTxIDIndex pins the lookup plan before the
// method updates the selected primary keys. The former OR-of-pairs update
// prefers a deleted_slot index after a few terms, which turns a bounded
// batch into a scan of nearly every live UTxO. Selecting by tx_id and
// filtering output indexes in Go keeps the lookup on tx_id_output_idx.
func TestMarkUtxosDeletedAtSlotUsesTxIDIndex(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedStakeRefLookupUtxos(t, store, 50_000, 256)

	for _, nTerms := range []int{2, 4, 64, 400} {
		t.Run(fmt.Sprintf("terms=%d", nTerms), func(t *testing.T) {
			ids := make([]models.UtxoId, nTerms)
			for i := range ids {
				ids[i] = utxoIDAt(i * 7)
			}

			legacyQuery, legacyArgs := legacyMarkUtxosDeletedAtSlotQuery(
				ids,
				1,
			)
			legacyPlan := queryPlan(
				t,
				store.writeDB,
				legacyQuery,
				legacyArgs...,
			)
			t.Logf("legacy MarkUtxosDeletedAtSlot plan (n=%d): %s", nTerms, legacyPlan)

			txIDs, _ := distinctUtxoTxIDs(ids)
			query, args := utxoRowIDsByTxIDQuery(txIDs)
			newPlan := queryPlan(t, store.writeDB, query, args...)
			t.Logf("tx_id-IN mark lookup plan (n=%d): %s", nTerms, newPlan)

			require.Contains(t, newPlan, "tx_id_output_idx")
			require.NotContains(t, newPlan, "idx_utxo_deleted_payment_script")
			require.NotContains(t, newPlan, "idx_utxo_deleted_staking_amount")

			if nTerms >= 4 {
				require.Contains(
					t,
					legacyPlan,
					"idx_utxo_deleted",
					"expected the legacy update to fall back to a deleted-slot index: %s",
					legacyPlan,
				)
			}
		})
	}
}

// TestMarkUtxosDeletedAtSlotUpdatesOnlyRequestedLiveRows proves the indexed
// lookup does not widen a request to sibling outputs of the same transaction
// and leaves rows that were already spent untouched.
func TestMarkUtxosDeletedAtSlotUpdatesOnlyRequestedLiveRows(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	type row struct {
		txID        []byte
		outputIdx   uint32
		deletedSlot int64
	}
	rows := []row{
		{txID: []byte("transaction-a"), outputIdx: 0, deletedSlot: 0},
		{txID: []byte("transaction-a"), outputIdx: 1, deletedSlot: 0},
		{txID: []byte("transaction-b"), outputIdx: 0, deletedSlot: 0},
		{txID: []byte("transaction-c"), outputIdx: 0, deletedSlot: 9},
	}
	for i, row := range rows {
		_, err := store.writeDB.Exec(
			"INSERT INTO utxo (tx_id, output_idx, staking_key, credential_tag, "+
				"added_slot, deleted_slot, amount) VALUES (?, ?, ?, ?, ?, ?, ?)",
			row.txID,
			row.outputIdx,
			[]byte{byte(i + 1)},
			0,
			1,
			row.deletedSlot,
			"1000000",
		)
		require.NoError(t, err)
	}

	require.NoError(t, store.MarkUtxosDeletedAtSlot(
		nil,
		[]types.UtxoKey{
			{TxId: rows[0].txID, OutputIdx: rows[0].outputIdx},
			{TxId: rows[2].txID, OutputIdx: rows[2].outputIdx},
			{TxId: rows[3].txID, OutputIdx: rows[3].outputIdx},
			{TxId: []byte("missing"), OutputIdx: 0},
		},
		20,
	))

	want := []int64{20, 0, 20, 9}
	for i, row := range rows {
		var got int64
		require.NoError(t, store.writeDB.QueryRow(
			"SELECT deleted_slot FROM utxo WHERE tx_id = ? AND output_idx = ?",
			row.txID,
			row.outputIdx,
		).Scan(&got))
		require.Equal(t, want[i], got)
	}
}

// TestMarkUtxosDeletedAtSlotUpdatePlansOnPrimaryKey pins the plan of the
// statement MarkUtxosDeletedAtSlot actually executes -- markUtxosDeletedQuery
// builds the only UPDATE in that method -- rather than only the lookup that
// precedes it.
//
// The lookup fix alone left the update carrying "deleted_slot = 0", and with
// no sqlite_stat1 SQLite drives that form from
// idx_utxo_deleted_payment_script (deleted_slot=?) from two terms upwards,
// evaluating "id IN (...)" against every live row. That is issue #4067's
// whole-table pass moved from the first statement to the second. Statistics
// hide it, so this test must not run ANALYZE.
func TestMarkUtxosDeletedAtSlotUpdatePlansOnPrimaryKey(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedStakeRefLookupUtxos(t, store, 50_000, 256)

	for _, nRows := range []int{1, 2, 4, 64, 400, 998} {
		t.Run(fmt.Sprintf("rows=%d", nRows), func(t *testing.T) {
			rowIDs := make([]int64, nRows)
			for i := range rowIDs {
				rowIDs[i] = int64(i + 1)
			}

			query, args := markUtxosDeletedQuery(rowIDs, 1, false)
			plan := queryPlan(t, store.writeDB, query, args...)
			require.Contains(
				t, plan, "INTEGER PRIMARY KEY",
				"the update must resolve rows by primary key: %s", plan,
			)
			require.NotContains(t, plan, "idx_utxo_deleted")

			// Control: the guarded form this replaced, planned on the same
			// statistics-free database.
			guardedArgs := make([]any, 0, len(args))
			guardedArgs = append(guardedArgs, args...)
			guardedPlan := queryPlan(
				t,
				store.writeDB,
				strings.Replace(
					query,
					"WHERE id IN (",
					"WHERE deleted_slot = 0 AND id IN (",
					1,
				),
				guardedArgs...,
			)
			if nRows >= 2 {
				require.Contains(
					t, guardedPlan, "idx_utxo_deleted",
					"expected the guarded update to fall back to a "+
						"deleted-slot index: %s", guardedPlan,
				)
			}
		})
	}
}

func TestMarkUtxosDeletedQueryRetainsLivenessGuardForConcurrentWriters(t *testing.T) {
	t.Parallel()
	query, args := markUtxosDeletedQuery([]int64{1, 2}, 10, true)
	require.Equal(
		t,
		"UPDATE utxo SET deleted_slot = ? WHERE deleted_slot = 0 AND id IN (?,?)",
		query,
	)
	require.Equal(t, []any{int64(10), int64(1), int64(2)}, args)
}
