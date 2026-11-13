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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// fixtureIdentityRow writes the identity row of hash through the store's
// dialect and returns its row id. A fixture that inserts outputs or
// transactions directly names the producer by this id.
func fixtureIdentityRow(t *testing.T, store *Store, id int64, hash []byte) int64 {
	t.Helper()
	table := store.dialect.QuoteIdentifier("transaction")
	var err error
	if id == 0 {
		_, err = store.writeDB.Exec(store.dialect.Rebind(
			"INSERT INTO "+table+" (hash) VALUES (?)"), hash)
	} else {
		_, err = store.writeDB.Exec(store.dialect.Rebind(
			"INSERT INTO "+table+" (id, hash) VALUES (?, ?)"), id, hash)
	}
	require.NoError(t, err)
	require.NoError(t, store.writeDB.QueryRow(store.dialect.Rebind(
		"SELECT id FROM "+table+" WHERE hash = ?"), hash).Scan(&id))
	return id
}

func identityCount(t *testing.T, store *Store, hash []byte) int {
	t.Helper()
	var n int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM "transaction" WHERE hash = ?`, hash,
	).Scan(&n))
	return n
}

// TestRollbackDeletesAppliedIdentityAndKeepsImportedProducer applies a
// transaction that spends an imported output, then rolls it back. The
// transaction's application row, its identity row and its own output go; the
// imported output is unspent again and its producer, known only by an
// identity row, stays because that output still names it.
func TestRollbackDeletesAppliedIdentityAndKeepsImportedProducer(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	producer, spender := bytes32(0x91), bytes32(0x92)
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: producer, OutputIdx: 0, Amount: types.Uint64(2_000_000),
	}}, nil))

	tx := mockledger.NewTransactionBuilder()
	tx.WithId(spender)
	tx.WithInputs(producerTestInput(t, producer, 0))
	tx.WithOutputs(producerTestOutput(t, 1_500_000))
	tx.WithValid(true)
	require.NoError(t, store.SetTransaction(
		tx, ocommon.Point{Slot: 10, Hash: spender}, 0, nil, false, nil, 0,
	))
	require.Equal(t, 1, identityCount(t, store, spender))

	require.NoError(t, store.DeleteTransactionsAfterSlot(5, nil))

	require.Zero(t, identityCount(t, store, spender),
		"a rolled-back transaction's unreferenced identity row goes")
	require.Equal(t, 1, identityCount(t, store, producer),
		"the imported output still names its producer")
	var applied, outputs int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM applied_transaction`).Scan(&applied))
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM utxo`).Scan(&outputs))
	require.Zero(t, applied)
	require.Equal(t, 1, outputs, "only the imported output remains")
	var spentBy sql.NullInt64
	var deletedSlot int64
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT spent_at_tx_id, deleted_slot FROM utxo`,
	).Scan(&spentBy, &deletedSlot))
	require.False(t, spentBy.Valid)
	require.Zero(t, deletedSlot)

	var violations int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM pragma_foreign_key_check`).Scan(&violations))
	require.Zero(t, violations, "rollback must leave no dangling reference")
}

// TestIdentityOnlyRowsAreNotTransactions requires the transaction readers to
// see only applied transactions: the producer of an imported output has an
// identity row and no application row, and must neither be found by hash nor
// counted.
func TestIdentityOnlyRowsAreNotTransactions(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	producer := bytes32(0x93)
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: producer, OutputIdx: 0, Amount: types.Uint64(1_000_000),
	}}, nil))
	require.Equal(t, 1, identityCount(t, store, producer))

	found, err := store.GetTransactionByHash(producer, nil)
	require.NoError(t, err)
	require.Nil(t, found)
	_, ok, err := store.GetTransactionSlotByHash(producer, nil)
	require.NoError(t, err)
	require.False(t, ok)
}

// TestCreateUtxoLeavesTheCallersModelUnresolved requires the identity row id
// that CreateUtxo resolves for a hash-named output to stay off the caller's
// model: a later rollback can delete that row, and a model that kept its id
// would insert an output naming a transaction that no longer exists.
func TestCreateUtxoLeavesTheCallersModelUnresolved(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	utxo := &models.Utxo{
		TxId: bytes32(0x94), OutputIdx: 0, Amount: types.Uint64(1_000_000),
		AddedSlot: 7,
	}
	require.NoError(t, store.CreateUtxo(nil, utxo))
	require.Nil(t, utxo.TransactionID,
		"resolution belongs on a copy of the caller's model")
	require.Nil(t, utxo.SpentAtTxID)

	// The producer is rolled back and the same model creates the output
	// again, which must resolve a fresh identity row.
	_, err := store.writeDB.Exec(`DELETE FROM utxo`)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(`DELETE FROM "transaction"`)
	require.NoError(t, err)
	utxo.ID = 0
	require.NoError(t, store.CreateUtxo(nil, utxo))
	require.Equal(t, 1, identityCount(t, store, bytes32(0x94)))
}
