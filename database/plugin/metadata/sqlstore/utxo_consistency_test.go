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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// requireUtxoConsistencyCheck seeds a consistent ledger, then each kind of
// broken relationship in turn, and requires the report to count exactly it.
func requireUtxoConsistencyCheck(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()
	table := store.dialect.QuoteIdentifier("transaction")
	exec := func(query string, args ...any) {
		t.Helper()
		_, err := store.writeDB.Exec(store.dialect.Rebind(query), args...)
		require.NoError(t, err, query)
	}
	idOf := func(hash []byte) int64 {
		t.Helper()
		var id int64
		require.NoError(t, store.writeDB.QueryRow(
			store.dialect.Rebind("SELECT id FROM "+table+" WHERE hash = ?"), hash,
		).Scan(&id))
		return id
	}
	producerHash, spenderHash := bytes32(0xf1), bytes32(0xf2)
	exec("INSERT INTO "+table+" (hash, slot) VALUES (?, 5)", producerHash)
	exec("INSERT INTO "+table+" (hash, slot) VALUES (?, 6)", spenderHash)
	producer, spender := idOf(producerHash), idOf(spenderHash)
	producerID := uint(producer)
	addr := []byte("addr_test1consistency")
	for i := range 3 {
		require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
			TxId: producerHash, OutputIdx: uint32(i), TransactionID: &producerID,
			Amount: types.Uint64(1), Address: addr,
		}))
	}
	var first int64
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT MIN(id) FROM utxo").Scan(&first))
	exec("UPDATE utxo SET spent_by_tx_id = ?, deleted_slot = 6 WHERE id = ?",
		spender, first)
	exec("INSERT INTO utxo_reference_edge (utxo_id, transaction_id) VALUES (?, ?)",
		first+1, spender)
	exec("INSERT INTO utxo_collateral_edge (utxo_id, transaction_id) VALUES (?, ?)",
		first+1, spender)

	report, err := store.CheckUtxoConsistency(ctx)
	require.NoError(t, err)
	require.Equal(t, UtxoConsistencyReport{}, report)
	require.True(t, report.Consistent())

	exec("UPDATE utxo SET tx_id = ? WHERE id = ?", bytes32(0xf9), first+2)
	report, err = store.CheckUtxoConsistency(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), report.ProducerHashMismatches)
	require.Equal(t, first+2, report.ProducerHashMismatchSeen)
	require.False(t, report.Consistent())
	exec("UPDATE utxo SET tx_id = ? WHERE id = ?", producerHash, first+2)

	exec("INSERT INTO utxo_reference_edge (utxo_id, transaction_id) VALUES (?, ?)",
		first+2, 9999)
	exec("INSERT INTO utxo_collateral_edge (utxo_id, transaction_id) VALUES (?, ?)",
		first+2, 9998)
	// PostgreSQL and MySQL enforce the foreign keys to utxo and to the
	// producing transaction, so only SQLite can hold those orphans.
	wantCollateral := int64(1)
	if store.dialect.Name() == "sqlite" {
		exec("INSERT INTO utxo_collateral_edge (utxo_id, transaction_id) VALUES (?, ?)",
			9997, spender)
		wantCollateral = 2
	}
	report, err = store.CheckUtxoConsistency(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), report.OrphanReferenceEdges)
	require.Equal(t, first+2, report.OrphanReferenceEdgeSeen)
	require.Equal(t, wantCollateral, report.OrphanCollateralEdges)
	require.Equal(t, int64(0), report.ProducerHashMismatches)
	exec("DELETE FROM utxo_reference_edge WHERE transaction_id = ?", 9999)
	exec("DELETE FROM utxo_collateral_edge WHERE transaction_id = ? OR utxo_id = ?", 9998, 9997)

	exec("UPDATE utxo SET spent_by_tx_id = ? WHERE id = ?", 9996, first+1)
	report, err = store.CheckUtxoConsistency(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(1), report.OrphanSpenders)
	require.Equal(t, first+1, report.OrphanSpenderSeen)
	exec("UPDATE utxo SET spent_by_tx_id = NULL WHERE id = ?", first+1)

	if store.dialect.Name() == "sqlite" {
		exec("UPDATE utxo SET transaction_id = ? WHERE id = ?", 9995, first+2)
		report, err = store.CheckUtxoConsistency(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(1), report.OrphanProducers)
		require.Equal(t, first+2, report.OrphanProducerSeen)
	}
}

func TestSQLiteCheckUtxoConsistency(t *testing.T) {
	t.Parallel()
	requireUtxoConsistencyCheck(t, newMigratedSQLiteStore(t))
}
