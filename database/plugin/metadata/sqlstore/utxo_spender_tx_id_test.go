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
	"database/sql"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

type spenderRow struct {
	deletedSlot int64
	spentBy     sql.NullInt64
	legacy      []byte
}

func readSpenderRow(t *testing.T, store *Store, txID []byte) spenderRow {
	t.Helper()
	var row spenderRow
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT deleted_slot, spent_by_tx_id, spent_at_tx_id FROM utxo
WHERE tx_id = ? AND output_idx = 0`, txID,
	).Scan(&row.deletedSlot, &row.spentBy, &row.legacy))
	return row
}

// TestSpendStoresSpenderTransactionID applies a transaction that spends an
// existing output through the per-row path and through the trusted batch
// overlay, then reads the spend back every way a reader can: the raw column,
// the model's spender hash, the spending transaction's inputs, and the
// rollback that restores the output.
func TestSpendStoresSpenderTransactionID(t *testing.T) {
	t.Parallel()
	for _, overlay := range []bool{false, true} {
		name := "per_row"
		if overlay {
			name = "overlay"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := newMigratedSQLiteStore(t)
			fx := buildSharedCredentialTx(t, 0x31)
			seedConsumedUtxo(t, store, fx)
			ctx := context.Background()

			txn := store.Transaction(ctx)
			acc := store.NewBatchAccumulator()
			if overlay {
				enabled, err := store.EnableBatchUtxoOverlay(
					acc, txn,
					[]models.UtxoId{{Hash: fx.consumedTxID, Idx: 0}}, nil,
				)
				require.NoError(t, err)
				require.True(t, enabled)
			}
			require.NoError(t, store.SetTransactionBatched(
				fx.tx, fx.point, 0, fx.certDeposits, false, acc, txn, 0,
			))
			require.NoError(t, store.FlushBatch(acc, txn))
			require.NoError(t, txn.Commit())

			var spenderID int64
			require.NoError(t, store.writeDB.QueryRow(
				`SELECT id FROM "transaction" WHERE hash = ?`,
				fx.tx.Hash().Bytes(),
			).Scan(&spenderID))

			row := readSpenderRow(t, store, fx.consumedTxID)
			require.Equal(t, int64(fx.point.Slot), row.deletedSlot)
			require.Equal(
				t, sql.NullInt64{Int64: spenderID, Valid: true}, row.spentBy,
				"spent_by_tx_id must be the spender's transaction id",
			)
			require.Nil(t, row.legacy, "legacy spender column must stay NULL")

			spent, err := store.GetUtxoIncludingSpent(fx.consumedTxID, 0, nil)
			require.NoError(t, err)
			require.NotNil(t, spent)
			require.Equal(
				t, fx.tx.Hash().Bytes(), []byte(spent.SpentAtTxId),
				"the model must still carry the spender's hash",
			)

			stored, err := store.GetTransactionByHash(
				fx.tx.Hash().Bytes(), nil,
			)
			require.NoError(t, err)
			require.NotNil(t, stored)
			require.Len(t, stored.Inputs, 1)
			require.Equal(t, fx.consumedTxID, stored.Inputs[0].TxId)

			require.NoError(t, store.DeleteTransactionsAfterSlot(
				fx.point.Slot-1, nil,
			))
			row = readSpenderRow(t, store, fx.consumedTxID)
			require.Zero(t, row.deletedSlot)
			require.False(t, row.spentBy.Valid, "rollback must clear the spender")
		})
	}
}

// TestSpenderQueriesSearchTheSpenderIndex pins the plans of the statements
// that read or clear the spender column: each must search
// idx_utxo_spent_by_tx_id, and the statements that resolve a spender hash
// must reach the transaction table by key rather than scan it.
func TestSpenderQueriesSearchTheSpenderIndex(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedRollbackUtxos(t, store, 20_000, 64, 2_656_808, 40)
	analyzeStore(t, store)

	for name, query := range map[string]string{
		"unspend": `UPDATE utxo SET spent_by_tx_id = NULL, deleted_slot = 0
WHERE spent_by_tx_id IN (?, ?, ?)`,
		"spender_inputs": `SELECT ` + sqliteUtxoColumns +
			` FROM utxo WHERE utxo.spent_by_tx_id IN (?, ?, ?)`,
	} {
		plan := explainQueryPlan(t, store, query, int64(1), int64(2), int64(3))
		require.Contains(
			t, plan, "USING INDEX idx_utxo_spent_by_tx_id (spent_by_tx_id=?)",
			"%s: %s", name, plan,
		)
		require.NotContains(t, plan, "SCAN utxo", "%s: %s", name, plan)
	}

	plan := explainQueryPlan(
		t, store, getUtxoIncludingSpentByRefQuery,
		make([]byte, 32), int64(0),
	)
	require.Contains(t, plan, "idx_utxo_producer_output", plan)
	require.NotContains(t, plan, "SCAN", plan)
}

// TestInsertWithUnknownSpenderFails requires an insert that names a spender
// hash with no transaction row to fail rather than store a spent output with
// no spender.
func TestInsertWithUnknownSpenderFails(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	err := store.CreateUtxo(nil, &models.Utxo{
		TxId:        bytes32(0x61),
		DeletedSlot: 5,
		SpentAtTxId: bytes32(0x62),
	})
	require.ErrorContains(t, err, "spender transaction")
	var count int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM utxo",
	).Scan(&count))
	require.Zero(t, count)
}

func bytes32(b byte) []byte {
	out := make([]byte, 32)
	for i := range out {
		out[i] = b
	}
	return out
}

// TestInputEdgeQueriesSearchTheEdgeIndex pins the plans of the edge reads and
// the rollback delete: each must search the transaction-id index and never
// scan an edge table or utxo.
func TestInputEdgeQueriesSearchTheEdgeIndex(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	for _, table := range []string{utxoCollateralEdgeTable, utxoReferenceEdgeTable} {
		for name, query := range map[string]string{
			"read": `SELECT ` + sqliteUtxoColumns + `, input.transaction_id
FROM utxo AS utxo JOIN ` + table + ` AS input ON input.utxo_id = utxo.id
WHERE input.transaction_id IN (?, ?) ORDER BY utxo.id`,
			"rollback": `DELETE FROM ` + table + ` WHERE transaction_id IN (?, ?)`,
		} {
			plan := explainQueryPlan(t, store, query, int64(1), int64(2))
			require.Contains(t, plan, "USING INDEX idx_"+table+"_tx", "%s %s: %s", table, name, plan)
			require.NotContains(t, plan, "SCAN "+table, "%s %s: %s", table, name, plan)
			require.NotContains(t, plan, "SCAN utxo", "%s %s: %s", table, name, plan)
		}
	}
}
