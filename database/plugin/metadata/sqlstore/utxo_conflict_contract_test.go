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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// requireCreateUtxoConflictContract pins the error CreateUtxo returns for an
// output that is already stored, whichever dialect enforces the uniqueness: an
// output created twice, by producer id or by hash, whether it was imported or
// produced.
func requireCreateUtxoConflictContract(t *testing.T, store *Store) {
	t.Helper()
	importedHash, producedHash := bytes32(0xa1), bytes32(0xa2)
	addr := []byte("addr_test1contract")
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: importedHash, OutputIdx: 0, Amount: types.Uint64(1), Address: addr,
	}}, nil))
	id := producerForDialect(t, store, producedHash)
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId: producedHash, OutputIdx: 0, TransactionID: &id,
		Amount: types.Uint64(1), Address: addr,
	}))

	err := store.CreateUtxo(nil, &models.Utxo{
		TxId: producedHash, OutputIdx: 0, TransactionID: &id,
		Amount: types.Uint64(1), Address: addr,
	})
	require.ErrorIs(t, err, types.ErrUtxoConflict, "produced twice")
	err = store.CreateUtxo(nil, &models.Utxo{
		TxId: producedHash, OutputIdx: 0, Amount: types.Uint64(1), Address: addr,
	})
	require.ErrorIs(t, err, types.ErrUtxoConflict, "created by hash over produced")
	err = store.CreateUtxo(nil, &models.Utxo{
		TxId: importedHash, OutputIdx: 0, Amount: types.Uint64(1), Address: addr,
	})
	require.ErrorIs(t, err, types.ErrUtxoConflict, "imported twice")
}

// producerForDialect inserts the identity row of hash and returns its id.
func producerForDialect(t *testing.T, store *Store, hash []byte) uint {
	t.Helper()
	table := store.dialect.QuoteIdentifier("transaction")
	_, err := store.writeDB.Exec(
		store.dialect.Rebind("INSERT INTO "+table+" (hash) VALUES (?)"), hash,
	)
	require.NoError(t, err)
	var id uint
	require.NoError(t, store.writeDB.QueryRow(
		store.dialect.Rebind("SELECT id FROM "+table+" WHERE hash = ?"), hash,
	).Scan(&id))
	return id
}

func TestSQLiteCreateUtxoConflictContract(t *testing.T) {
	t.Parallel()
	requireCreateUtxoConflictContract(t, newMigratedSQLiteStore(t))
}

// spent_at_tx_id is a foreign key to the transaction identity table, so a
// spend cannot name a transaction row that does not exist.
func TestSpentAtTxIDRequiresATransactionRow(t *testing.T) {
	t.Parallel()
	store, _ := newOverlayInsertStore(t, false, 0)
	producer := testutil.TransactionID(t, store.writeDB, bytes32(0xc6))
	_, err := store.writeDB.Exec(
		`INSERT INTO utxo (tx_id, output_idx, deleted_slot, added_slot, amount,
    spent_at_tx_id) VALUES (?, 0, 500, 3, '7', ?)`,
		producer, int64(987654),
	)
	require.ErrorContains(t, err, "FOREIGN KEY")
}

// ImportUtxos resolves producers in chunks. Outputs of producers an earlier
// chunk, an earlier call or an applied transaction already stored must each
// stay one row under one identity row.
func TestImportUtxosAcrossChunksKeepsOneIdentityPerProducer(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	const total = 3*importIdentityChunk + 5
	hash := func(i int) []byte {
		h := bytes32(0xd1)
		h[0], h[1] = byte(i%7), byte(i%7>>8)
		return h
	}
	var items []models.Utxo
	for i := range total {
		items = append(items, models.Utxo{
			TxId: hash(i), OutputIdx: uint32(i), Amount: types.Uint64(1),
		})
	}
	require.NoError(t, store.ImportUtxos(items, nil))
	require.NoError(t, store.ImportUtxos(items, nil))

	var rows, identities int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM utxo").Scan(&rows))
	require.Equal(t, total, rows, "one row per distinct output")
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM "transaction"`).Scan(&identities))
	require.Equal(t, 7, identities, "one identity row per producer hash")
	var applied int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM applied_transaction`).Scan(&applied))
	require.Zero(t, applied, "an import never applies a transaction")
}
