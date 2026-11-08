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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

func conflictRepairUtxo(seed int, assets bool) models.Utxo {
	utxo := *utxoForInsertCacheTest(byte(seed), uint32(seed%3), 2_000_000)
	utxo.TxId[30] = byte(seed >> 8)
	utxo.Address = fmt.Appendf(nil, "addr-%d", seed)
	if assets {
		utxo.Assets = []models.Asset{{
			Name:        []byte("tok"),
			PolicyId:    bytes.Repeat([]byte{byte(seed)}, 28),
			Fingerprint: fmt.Appendf(nil, "asset1-%d", seed),
			Amount:      types.Uint64(uint64(seed) + 5),
		}}
	}
	return utxo
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
// ledger-state import already wrote. The producing-transaction link must be
// filled in with one UPDATE per insert batch, and the import's address,
// pointer, reference-script, and asset rows must be intact afterwards.
func TestInsertUtxoModelsCheckedBatchesConflictRepair(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	store := newMigratedSQLiteStoreWithRegistry(t, reg)
	store.storageMode = types.StorageModeAPI

	const count = utxoInsertBatchSize + 8
	var producerID uint
	require.NoError(t, store.writeDB.QueryRow(
		`INSERT INTO "transaction" (hash) VALUES (?) RETURNING id`,
		bytes.Repeat([]byte{0xEE}, 32),
	).Scan(&producerID))

	imported := make([]models.Utxo, count)
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
		replay[i].TransactionID = &producerID
	}
	require.NoError(t, store.ImportUtxos(imported, nil))
	assetIDs := map[uint]uint{}
	rows, err := store.writeDB.Query(`SELECT id, utxo_id FROM asset`)
	require.NoError(t, err)
	for rows.Next() {
		var id, utxoID uint
		require.NoError(t, rows.Scan(&id, &utxoID))
		assetIDs[utxoID] = id
	}
	require.NoError(t, rows.Close())
	require.Len(t, assetIDs, count/2)

	before := counterValue(t, reg, "update")
	inserted := replayConflicting(t, store, replay)
	for i, created := range inserted {
		require.False(t, created, "output %d", i)
	}
	require.Equal(
		t,
		float64(2),
		counterValue(t, reg, "update")-before,
		"conflict repair should issue one UPDATE per insert batch",
	)

	for i := range replay {
		got, err := store.GetUtxo(replay[i].TxId, replay[i].OutputIdx, nil)
		require.NoError(t, err)
		require.NotNil(t, got.TransactionID, "output %d", i)
		require.Equal(t, producerID, *got.TransactionID, "output %d", i)
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
			`SELECT id, amount FROM asset WHERE utxo_id = ?`, got.ID,
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

	var firstTx, secondTx, collateralTx uint
	for i, dst := range []*uint{&firstTx, &secondTx, &collateralTx} {
		require.NoError(t, store.writeDB.QueryRow(
			`INSERT INTO "transaction" (hash) VALUES (?) RETURNING id`,
			bytes.Repeat([]byte{0xD0 + byte(i)}, 32),
		).Scan(dst))
	}
	linked := conflictRepairUtxo(1, true)
	linked.TransactionID = &firstTx
	unlinked := conflictRepairUtxo(2, true)
	collateral := conflictRepairUtxo(3, false)
	require.NoError(t, store.ImportUtxos(
		[]models.Utxo{linked, unlinked, collateral}, nil,
	))
	_, err := store.writeDB.Exec(
		`UPDATE utxo SET transaction_id = ? WHERE tx_id = ? AND output_idx = ?`,
		firstTx, linked.TxId, linked.OutputIdx,
	)
	require.NoError(t, err)
	_, err = store.writeDB.Exec(`DELETE FROM asset`)
	require.NoError(t, err)

	replayLinked := linked
	replayLinked.TransactionID = &secondTx
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
		`SELECT COUNT(*) FROM asset WHERE utxo_id = ?`, got.ID,
	).Scan(&assets))
	require.Equal(t, 1, assets, "linked output's assets restored")

	got, err = store.GetUtxo(unlinked.TxId, unlinked.OutputIdx, nil)
	require.NoError(t, err)
	require.Equal(t, secondTx, *got.TransactionID)

	got, err = store.GetUtxo(collateral.TxId, collateral.OutputIdx, nil)
	require.NoError(t, err)
	require.Nil(t, got.TransactionID)
	require.Equal(t, collateralTx, *got.CollateralReturnForTxID)
}

// TestRepairUnlinkedUtxosBatchQueryPlan checks that every arm of the batched
// repair is resolved through the unique tx_id_output_idx index, with and
// without planner statistics, so it never drives from a deleted_slot index.
func TestRepairUnlinkedUtxosBatchQueryPlan(t *testing.T) {
	skipPlanTestUnderRace(t)
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	seedSkewedUtxos(t, store, 3_000)
	ctx := context.Background()
	conn, err := store.writeDB.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()

	const keys = 5
	var args []any
	for range 2 {
		for i := range keys {
			args = append(args, skewedUtxoTxID(i*7), i%4, int64(1))
		}
	}
	for i := range keys {
		args = append(args, skewedUtxoTxID(i*7), i%4)
	}
	for _, stage := range []string{"no_stats", "analyzed"} {
		if stage == "analyzed" {
			_, err := conn.ExecContext(ctx, "ANALYZE")
			require.NoError(t, err)
		}
		plan := connQueryPlan(
			t, conn, repairUnlinkedUtxosBatchQuery(keys), args...,
		)
		require.NotContains(t, plan, "idx_utxo_deleted", stage+": "+plan)
		require.NotContains(t, plan, "SCAN utxo", stage+": "+plan)
		require.Equal(t, keys, bytes.Count(
			[]byte(plan),
			[]byte("USING INDEX tx_id_output_idx (tx_id=? AND output_idx=?)"),
		), stage+": "+plan)
	}
}
