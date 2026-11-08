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
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// requireCreateUtxoConflictContract pins the error CreateUtxo returns for an
// output that is already stored, whichever dialect enforces the uniqueness: an
// output produced twice, a produced output over an imported row, and an
// imported row over a produced output.
func requireCreateUtxoConflictContract(t *testing.T, store *Store) {
	t.Helper()
	importedHash, producedHash := bytes32(0xa1), bytes32(0xa2)
	addr := []byte("addr_test1contract")
	require.NoError(t, store.ImportUtxos([]models.Utxo{{
		TxId: importedHash, OutputIdx: 0, Amount: types.Uint64(1), Address: addr,
	}}, nil))
	table := store.dialect.QuoteIdentifier("transaction")
	_, err := store.writeDB.Exec(
		store.dialect.Rebind(
			"INSERT INTO "+table+" (hash, slot) VALUES (?, 5)",
		),
		producedHash,
	)
	require.NoError(t, err)
	var producer int64
	require.NoError(t, store.writeDB.QueryRow(
		store.dialect.Rebind("SELECT id FROM "+table+" WHERE hash = ?"),
		producedHash,
	).Scan(&producer))
	id := uint(producer)
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId: producedHash, OutputIdx: 0, TransactionID: &id,
		Amount: types.Uint64(1), Address: addr,
	}))

	err = store.CreateUtxo(nil, &models.Utxo{
		TxId: producedHash, OutputIdx: 0, TransactionID: &id,
		Amount: types.Uint64(1), Address: addr,
	})
	require.ErrorIs(t, err, types.ErrUtxoConflict, "produced twice")
	err = store.CreateUtxo(nil, &models.Utxo{
		TxId: importedHash, OutputIdx: 0, TransactionID: &id,
		Amount: types.Uint64(1), Address: addr,
	})
	require.ErrorIs(t, err, types.ErrUtxoConflict, "produced over imported")
	err = store.CreateUtxo(nil, &models.Utxo{
		TxId: producedHash, OutputIdx: 0, Amount: types.Uint64(1), Address: addr,
	})
	require.ErrorIs(t, err, types.ErrUtxoConflict, "imported over produced")
	err = store.CreateUtxo(nil, &models.Utxo{
		TxId: importedHash, OutputIdx: 0, Amount: types.Uint64(1), Address: addr,
	})
	require.ErrorIs(t, err, types.ErrUtxoConflict, "imported twice")
}

func TestSQLiteCreateUtxoConflictContract(t *testing.T) {
	t.Parallel()
	requireCreateUtxoConflictContract(t, newMigratedSQLiteStore(t))
}

// A batch whose output collides with a stored output of the same producer
// and output index under a different tx_id fails with ErrUtxoConflict naming
// the output, not sql.ErrNoRows, and the implicit transaction leaves the
// batch's other outputs unwritten.
func TestUtxoBatchProducerCollisionFailsCleanly(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	producedHash := bytes32(0xb1)
	require.NoError(t, store.writeDB.QueryRow(
		`INSERT INTO "transaction" (hash, slot) VALUES (?, 5) RETURNING id`,
		producedHash,
	).Scan(new(int64)))
	var producer int64
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT id FROM "transaction" WHERE hash = ?`, producedHash,
	).Scan(&producer))
	id := uint(producer)
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId: producedHash, OutputIdx: 0, TransactionID: &id,
		Amount: types.Uint64(1),
	}))

	fresh := &models.Utxo{
		TxId: producedHash, OutputIdx: 5, TransactionID: &id,
		Amount: types.Uint64(1),
	}
	collision := &models.Utxo{
		TxId: bytes32(0xb9), OutputIdx: 0, TransactionID: &id,
		Amount: types.Uint64(1),
	}
	err := store.withWriteTransaction(nil, func(db queryer, ctx context.Context) error {
		_, err := store.insertUtxoModelsChecked(
			ctx, db, []*models.Utxo{fresh, collision}, true, nil,
		)
		return err
	})
	require.ErrorIs(t, err, types.ErrUtxoConflict)
	require.NotErrorIs(t, err, sql.ErrNoRows)
	require.ErrorContains(t, err, fmt.Sprintf("%x:0", collision.TxId))

	var count int
	require.NoError(t, store.writeDB.QueryRow("SELECT COUNT(*) FROM utxo").Scan(&count))
	require.Equal(t, 1, count, "the batch's fresh output must not be written")
}

// An output the legacy spender hash of which names no transaction row keeps
// deleted_slot but has no spender. Readers return it as a spent output with no
// spender hash; Kupo's match iterator drops spent outputs without one.
func TestUtxoSpentWithoutSpenderRowReadsAsSpentWithNoSpender(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	hash := bytes32(0xc5)
	_, err := store.writeDB.Exec(
		`INSERT INTO utxo (tx_id, output_idx, deleted_slot, added_slot, amount)
VALUES (?, 0, 500, 3, '7')`, hash,
	)
	require.NoError(t, err)

	utxo, err := store.GetUtxoIncludingSpent(hash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, utxo)
	require.Equal(t, uint64(500), utxo.DeletedSlot)
	require.Empty(t, utxo.SpentAtTxId)

	live, err := store.GetUtxo(hash, 0, nil)
	require.NoError(t, err)
	require.Nil(t, live, "a spent output is not live")
}
