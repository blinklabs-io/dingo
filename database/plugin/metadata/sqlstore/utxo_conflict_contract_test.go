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
	"strconv"
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

// A stored output at the producer and index an insert names, under another
// tx_id, is a different output: the insert conflicts instead of attaching its
// relations to that row.
func TestUtxoRepairRejectsStoredOutputWithAnotherTxID(t *testing.T) {
	t.Parallel()
	for _, n := range []int{1, 2} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			t.Parallel()
			store := newMigratedSQLiteStore(t)
			producedHash := bytes32(0xb3)
			id := producerFor(t, store, producedHash)
			_, err := store.writeDB.Exec(
				`INSERT INTO utxo (transaction_id, tx_id, output_idx,
    credential_tag, added_slot, deleted_slot, amount)
VALUES (?, ?, 0, 0, 1, 0, '1')`, id, bytes32(0xb4),
			)
			require.NoError(t, err)
			utxos := []*models.Utxo{{
				TxId: producedHash, OutputIdx: 0, TransactionID: &id,
				Amount: types.Uint64(1),
			}}
			if n == 2 {
				utxos = append(utxos, &models.Utxo{
					TxId: producedHash, OutputIdx: 5, TransactionID: &id,
					Amount: types.Uint64(1),
				})
			}
			err = store.withWriteTransaction(nil,
				func(db queryer, ctx context.Context) error {
					_, err := store.insertUtxoModelsChecked(
						ctx, db, utxos, true, nil,
					)
					return err
				},
			)
			require.ErrorIs(t, err, types.ErrUtxoConflict)
		})
	}
}

// Foreign keys are on for every store connection, so a legacy spender hash
// with no transaction row can only come from a database written before they
// were enforced.
func TestLegacySpenderHashRequiresATransactionRow(t *testing.T) {
	t.Parallel()
	store, _ := newOverlayInsertStore(t, false, 0)
	_, err := store.writeDB.Exec(
		`INSERT INTO utxo (tx_id, output_idx, deleted_slot, added_slot, amount,
    spent_at_tx_id) VALUES (?, 0, 500, 3, '7', ?)`,
		bytes32(0xc6), bytes32(0xc7),
	)
	require.ErrorContains(t, err, "FOREIGN KEY")
}

// An output whose legacy spender hash names no transaction row keeps
// deleted_slot but has no spender id. Readers return it as a spent output
// whose spender is the legacy hash; Kupo's match iterator drops spent outputs
// without a spender id.
func TestUtxoSpentWithoutSpenderRowReadsItsLegacySpenderHash(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	hash := bytes32(0xc5)
	// This store's connections do not enforce foreign keys, as a database
	// written before they were enforced did not.
	_, err := store.writeDB.Exec(
		`INSERT INTO utxo (tx_id, output_idx, deleted_slot, added_slot, amount,
    spent_at_tx_id) VALUES (?, 0, 500, 3, '7', ?)`,
		hash, bytes32(0xc8),
	)
	require.NoError(t, err)

	utxo, err := store.GetUtxoIncludingSpent(hash, 0, nil)
	require.NoError(t, err)
	require.NotNil(t, utxo)
	require.Equal(t, uint64(500), utxo.DeletedSlot)
	require.Equal(t, bytes32(0xc8), []byte(utxo.SpentAtTxId))

	live, err := store.GetUtxo(hash, 0, nil)
	require.NoError(t, err)
	require.Nil(t, live, "a spent output is not live")
}

// ImportUtxos probes for the other identity once per chunk of rows. Outputs
// already stored as produced outputs, in every chunk, and an output the same
// call repeats as the other kind, must not be inserted a second time.
func TestImportUtxosSkipsOutputsStoredUnderTheOtherIdentity(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	const total = 3*maxImportedProbeHashes + 5
	var producer int64
	require.NoError(t, store.writeDB.QueryRow(
		`INSERT INTO "transaction" (hash, slot) VALUES (?, 5) RETURNING id`,
		bytes32(0xd0),
	).Scan(&producer))
	id := uint(producer)
	hash := func(i int) []byte {
		h := bytes32(0xd1)
		h[0], h[1] = byte(i), byte(i>>8)
		return h
	}
	var items []models.Utxo
	produced := map[int]bool{}
	for i := range total {
		if i%maxImportedProbeHashes == 7 {
			produced[i] = true
			var own int64
			require.NoError(t, store.writeDB.QueryRow(
				`INSERT INTO "transaction" (hash, slot) VALUES (?, 5) RETURNING id`,
				hash(i),
			).Scan(&own))
			ownID := uint(own)
			require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
				TxId: hash(i), OutputIdx: uint32(i), TransactionID: &ownID,
				Amount: types.Uint64(1),
			}))
		}
		items = append(items, models.Utxo{
			TxId: hash(i), OutputIdx: uint32(i), Amount: types.Uint64(1),
		})
	}
	// The same call names one more output twice, as imported then produced.
	items = append(items,
		models.Utxo{TxId: hash(1000), OutputIdx: 9, Amount: types.Uint64(1)},
		models.Utxo{
			TxId: hash(1000), OutputIdx: 9, TransactionID: &id,
			Amount: types.Uint64(1),
		},
	)
	require.NoError(t, store.ImportUtxos(items, nil))

	var rows int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM utxo").Scan(&rows))
	require.Equal(t, total+1, rows, "one row per distinct output")
	var imported int
	require.NoError(t, store.writeDB.QueryRow(`SELECT COUNT(*) FROM utxo
WHERE transaction_id IS NULL AND collateral_return_for_tx_id IS NULL`).Scan(&imported))
	// The repeated output is imported first, then hydrated by its produced twin.
	require.Equal(t, total-len(produced), imported)
}
