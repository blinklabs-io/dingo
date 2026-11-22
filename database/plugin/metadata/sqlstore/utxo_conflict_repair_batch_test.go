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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func conflictRepairUtxo(seed int, assets bool) models.Utxo {
	utxo := *utxoForInsertCacheTest(byte(seed), uint32(seed%3), 2_000_000)
	utxo.TxId[30] = byte(seed >> 8)
	utxo.Address = fmt.Appendf(nil, "addr-%d", seed)
	if assets {
		utxo.Assets = []models.Asset{{
			Name:     []byte("tok"),
			PolicyId: bytes.Repeat([]byte{byte(seed)}, 28),
			Amount:   types.Uint64(uint64(seed) + 5),
		}}
	}
	return utxo
}

// producerFor returns the row id of the identity row of the transaction that
// produced the output with hash txID, creating the row when the hash is not
// known yet, as an import does for the producer of an imported output.
func producerFor(t *testing.T, store *Store, txID []byte) uint {
	t.Helper()
	return uint(testutil.TransactionID(t, store.writeDB, txID))
}

func replayConflicting(
	t *testing.T,
	store *Store,
	utxos []models.Utxo,
) []bool {
	t.Helper()
	ptrs := make([]*models.Utxo, len(utxos))
	for i := range utxos {
		utxos[i].Assets = append([]models.Asset(nil), utxos[i].Assets...)
		ptrs[i] = &utxos[i]
	}
	var inserted []bool
	var rows rowBatch
	require.NoError(t, store.withWriteTransaction(nil,
		func(db queryer, ctx context.Context) error {
			var err error
			inserted, err = store.insertUtxoModelsChecked(
				ctx, db, ptrs, true, &rows,
			)
			if err != nil {
				return err
			}
			return rows.flush(ctx, db, store.dialect.ParameterLimit())
		},
	))
	return inserted
}

// TestInsertUtxoModelsCheckedBatchesConflictRepair replays outputs that a
// ledger-state import already wrote. The outputs must stay one row each under
// the producer's id, and the import's address, pointer, reference-script, and
// asset rows must be intact afterwards.
func TestInsertUtxoModelsCheckedBatchesConflictRepair(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	store.storageMode = types.StorageModeAPI

	const count = utxoInsertBatchSize + 8
	imported := make([]models.Utxo, count)
	producers := make([]uint, count)
	replay := make([]models.Utxo, count)
	for i := range imported {
		imported[i] = conflictRepairUtxo(i+1, i%2 == 0)
		imported[i].ReferenceScriptHash = bytes.Repeat([]byte{byte(i + 1)}, 28)
		if i%5 == 0 {
			imported[i].Pointer = &models.UtxoPointer{
				Slot: uint64(i + 1), TxIndex: 2, CertIndex: 3,
			}
		}
		replay[i] = imported[i]
		producerID := producerFor(t, store, imported[i].TxId)
		producers[i] = producerID
		replay[i].TransactionID = &producerID
	}
	require.NoError(t, store.ImportUtxos(imported, nil))
	assetIDs := map[uint]uint{}
	rows, err := store.writeDB.Query(
		`SELECT asset_id, utxo_id FROM asset_holding`,
	)
	require.NoError(t, err)
	for rows.Next() {
		var id, utxoID uint
		require.NoError(t, rows.Scan(&id, &utxoID))
		assetIDs[utxoID] = id
	}
	require.NoError(t, rows.Close())
	require.Len(t, assetIDs, count/2)

	inserted := replayConflicting(t, store, replay)
	for i, created := range inserted {
		require.False(t, created, "output %d", i)
	}
	var rowCount int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM utxo`).Scan(&rowCount))
	require.Equal(t, count, rowCount, "a replay must not add rows")

	for i := range replay {
		got, err := store.GetUtxo(replay[i].TxId, replay[i].OutputIdx, nil)
		require.NoError(t, err)
		require.NotNil(t, got.TransactionID, "output %d", i)
		require.Equal(t, producers[i], *got.TransactionID, "output %d", i)
		var address []byte
		require.NoError(t, store.writeDB.QueryRow(
			`SELECT address FROM utxo_address WHERE utxo_id = ?`, got.ID,
		).Scan(&address))
		require.Equal(t, imported[i].Address, address)
		var script []byte
		require.NoError(t, store.writeDB.QueryRow(
			`SELECT script_hash FROM utxo_reference_script WHERE utxo_id = ?`,
			got.ID,
		).Scan(&script))
		require.Equal(t, imported[i].ReferenceScriptHash, script)
		var slot int64
		err = store.writeDB.QueryRow(
			`SELECT ptr_slot FROM utxo_pointer WHERE utxo_id = ?`, got.ID,
		).Scan(&slot)
		if imported[i].Pointer != nil {
			require.NoError(t, err)
			require.Equal(t, int64(imported[i].Pointer.Slot), slot)
		} else {
			require.ErrorIs(t, err, sql.ErrNoRows)
		}
		var assetID uint
		var amount string
		err = store.writeDB.QueryRow(
			`SELECT asset_id, amount FROM asset_holding WHERE utxo_id = ?`,
			got.ID,
		).Scan(&assetID, &amount)
		if i%2 == 0 {
			require.NoError(t, err)
			require.Equal(t, assetIDs[got.ID], assetID)
			require.Equal(t, fmt.Sprint(i+1+5), amount)
		} else {
			require.ErrorIs(t, err, sql.ErrNoRows)
		}
	}
}

// TestInsertUtxoModelsCheckedRepairsLinkedConflictsInFull covers conflicted
// outputs that are not import leftovers: an already-linked output keeps its
// link and has its relation rows restored, a collateral-return replay links
// the collateral side, and a duplicate within one batch is handled once.
func TestInsertUtxoModelsCheckedRepairsLinkedConflictsInFull(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	store.storageMode = types.StorageModeAPI

	linked := conflictRepairUtxo(1, true)
	unlinked := conflictRepairUtxo(2, true)
	collateral := conflictRepairUtxo(3, false)
	firstTx := producerFor(t, store, linked.TxId)
	secondTx := producerFor(t, store, unlinked.TxId)
	collateralTx := producerFor(t, store, collateral.TxId)
	linked.TransactionID = &firstTx
	require.NoError(t, store.ImportUtxos(
		[]models.Utxo{linked, unlinked, collateral}, nil,
	))
	_, err := store.writeDB.Exec(`DELETE FROM asset_holding`)
	require.NoError(t, err)

	replayLinked := linked
	replayLinked.TransactionID = &firstTx
	replayUnlinked := unlinked
	replayUnlinked.TransactionID = &secondTx
	replayCollateral := collateral
	replayCollateral.CollateralReturnForTxID = &collateralTx
	duplicate := unlinked
	duplicate.TransactionID = &secondTx
	inserted := replayConflicting(t, store, []models.Utxo{
		replayLinked, replayUnlinked, replayCollateral, duplicate,
	})
	require.Equal(t, []bool{false, false, false, false}, inserted)

	got, err := store.GetUtxo(linked.TxId, linked.OutputIdx, nil)
	require.NoError(t, err)
	require.Equal(t, firstTx, *got.TransactionID, "existing link kept")
	var assets int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM asset_holding WHERE utxo_id = ?`, got.ID,
	).Scan(&assets))
	require.Equal(t, 1, assets, "linked output's assets restored")

	got, err = store.GetUtxo(unlinked.TxId, unlinked.OutputIdx, nil)
	require.NoError(t, err)
	require.Equal(t, secondTx, *got.TransactionID)

	got, err = store.GetUtxo(collateral.TxId, collateral.OutputIdx, nil)
	require.NoError(t, err)
	require.Equal(t, collateralTx, *got.TransactionID)
	require.Equal(t, collateralTx, *got.CollateralReturnForTxID)
}

// TestInsertUtxoModelsCheckedDuplicateStoresOneRow replays two occurrences of
// one imported output in a batch. The output stays one row under its
// producer's id.
func TestInsertUtxoModelsCheckedDuplicateStoresOneRow(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	store.storageMode = types.StorageModeAPI

	imported := conflictRepairUtxo(1, true)
	other := conflictRepairUtxo(2, false)
	firstTx := producerFor(t, store, imported.TxId)
	otherTx := producerFor(t, store, other.TxId)
	require.NoError(t, store.ImportUtxos(
		[]models.Utxo{imported, other}, nil,
	))

	first := imported
	first.TransactionID = &firstTx
	between := other
	between.TransactionID = &otherTx
	later := imported
	later.TransactionID = &firstTx
	inserted := replayConflicting(t, store, []models.Utxo{
		first, between, later,
	})
	require.Equal(t, []bool{false, false, false}, inserted)

	var rows int
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM utxo WHERE tx_id = ? AND output_idx = ?`,
		firstTx, imported.OutputIdx,
	).Scan(&rows))
	require.Equal(t, 1, rows)
	got, err := store.GetUtxo(imported.TxId, imported.OutputIdx, nil)
	require.NoError(t, err)
	require.Equal(t, firstTx, *got.TransactionID)
}
