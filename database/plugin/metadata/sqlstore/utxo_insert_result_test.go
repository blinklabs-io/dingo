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
	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSQLiteUtxoInsertResultKeepsConflictIdentity(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	require.NoError(t, store.withWriteTransaction(nil, func(db queryer, ctx context.Context) error {
		first := utxoInsertResultFixture(t, 1)
		created, err := store.insertUtxoModelChecked(ctx, db, first, true)
		require.NoError(t, err)
		require.True(t, created)
		require.NotZero(t, first.ID)
		other := utxoInsertResultFixture(t, 2)
		created, err = store.insertUtxoModelChecked(ctx, db, other, true)
		require.NoError(t, err)
		require.True(t, created)
		require.NotEqual(t, first.ID, other.ID)
		duplicate := utxoInsertResultFixture(t, 1)
		duplicate.Amount = 99
		created, err = store.insertUtxoModelChecked(ctx, db, duplicate, true)
		require.NoError(t, err)
		require.False(t, created, "a conflict must not report the preceding insert as a new output")
		require.Equal(t, first.ID, duplicate.ID)
		var amount string
		require.NoError(t, db.QueryRowContext(ctx, "SELECT amount FROM utxo WHERE id = ?", first.ID).Scan(&amount))
		require.Equal(t, "1000001", amount)
		_, err = store.insertUtxoModelChecked(ctx, db, duplicate, false)
		require.ErrorContains(t, err, "UNIQUE constraint failed")
		return nil
	}))
}

func TestSQLiteUtxoInsertResultSurvivesTriggerAndRollback(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	_, err := store.writeDB.Exec("CREATE TABLE output_audit (id INTEGER PRIMARY KEY AUTOINCREMENT, utxo_id INTEGER)")
	require.NoError(t, err)
	_, err = store.writeDB.Exec("INSERT INTO output_audit (id) VALUES (1000)")
	require.NoError(t, err)
	_, err = store.writeDB.Exec("CREATE TRIGGER audit_output AFTER INSERT ON utxo BEGIN INSERT INTO output_audit (utxo_id) VALUES (new.id); END")
	require.NoError(t, err)
	txn := store.Transaction(t.Context())
	t.Cleanup(func() { _ = txn.Rollback() })
	db, ctx, err := store.dbFromTxn(txn)
	require.NoError(t, err)
	output := utxoInsertResultFixture(t, 3)
	created, err := store.insertUtxoModelChecked(ctx, db, output, true)
	require.NoError(t, err)
	require.True(t, created)
	var id uint
	require.NoError(t, db.QueryRowContext(ctx, "SELECT utxo_id FROM output_audit WHERE id = 1001").Scan(&id))
	require.Equal(t, output.ID, id)
	require.NoError(t, txn.Rollback())
	require.Zero(t, tableCounts(t, store, nil, "utxo")["utxo"])
}

func TestSQLiteUtxoInsertResultHonorsCancellation(t *testing.T) {
	t.Parallel()
	store := newAPIModeSQLiteStore(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	created, err := store.insertUtxoModelChecked(ctx, store.writeDB, utxoInsertResultFixture(t, 4), true)
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, created)
	require.Zero(t, tableCounts(t, store, nil, "utxo")["utxo"])
}

func utxoInsertResultFixture(t *testing.T, i uint64) *models.Utxo {
	t.Helper()
	output := utxoForBenchmarkIteration(i)
	address, err := lcommon.NewAddressFromParts(lcommon.AddressTypeKeyNone, lcommon.AddressNetworkTestnet, output.PaymentKey, nil)
	require.NoError(t, err)
	output.Address = []byte(address.String())
	return output
}
