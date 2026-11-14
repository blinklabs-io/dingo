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
	"github.com/stretchr/testify/require"
)

func utxoAddedSlotAndAssets(
	t *testing.T,
	store *Store,
	txID []byte,
	idx uint32,
) (addedSlot int64, assets int, found bool) {
	t.Helper()
	var id int64
	err := store.writeDB.QueryRow(
		`SELECT id, added_slot FROM utxo
WHERE tx_id = (SELECT id FROM "transaction" WHERE hash = ?)
  AND output_idx = ?`,
		txID, int64(idx),
	).Scan(&id, &addedSlot)
	if err != nil {
		return 0, 0, false
	}
	require.NoError(t, store.writeDB.QueryRow(
		`SELECT COUNT(*) FROM asset WHERE utxo_id = ?`, id,
	).Scan(&assets))
	return addedSlot, assets, true
}

// An output the snapshot holds is stamped with the anchor slot. Linking it to
// its producer never raises added_slot, so a rollback to the anchor or later
// leaves it and its assets in place; only a rollback below the producer's
// own slot removes it.
func TestRollbackKeepsImportedUtxoLinkedToProducer(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)

	const anchor, producerSlot = uint64(100), uint64(80)
	replayed := conflictRepairUtxo(1, true)
	replayed.AddedSlot = anchor
	recovered := conflictRepairUtxo(2, true)
	recovered.AddedSlot = anchor
	require.NoError(t, store.ImportUtxos(
		[]models.Utxo{replayed, recovered}, nil,
	))

	// Applying the producer block finds the stored row and links it without
	// restamping its slot.
	replay := replayed
	producerID := producerFor(t, store, replay.TxId)
	replay.TransactionID = &producerID
	replay.AddedSlot = producerSlot
	replayConflicting(t, store, []models.Utxo{replay})
	slot, _, _ := utxoAddedSlotAndAssets(t, store, replayed.TxId, replayed.OutputIdx)
	require.Equal(t, int64(anchor), slot)

	// Recovering the producer's output from the block store restamps the
	// row with the producer's slot, which is at or before the anchor.
	hydrate := recovered
	hydrateID := producerFor(t, store, hydrate.TxId)
	hydrate.TransactionID = &hydrateID
	hydrate.AddedSlot = producerSlot
	require.NoError(t, store.ImportUtxos([]models.Utxo{hydrate}, nil))
	slot, _, _ = utxoAddedSlotAndAssets(t, store, recovered.TxId, recovered.OutputIdx)
	require.Equal(t, int64(producerSlot), slot)

	require.NoError(t, store.DeleteUtxosAfterSlot(anchor, nil))
	for _, u := range []models.Utxo{replayed, recovered} {
		_, assets, found := utxoAddedSlotAndAssets(t, store, u.TxId, u.OutputIdx)
		require.True(t, found, "rollback to the anchor must keep the output")
		require.Equal(t, 1, assets)
	}

	require.NoError(t, store.DeleteUtxosAfterSlot(producerSlot-1, nil))
	for _, u := range []models.Utxo{replayed, recovered} {
		_, _, found := utxoAddedSlotAndAssets(t, store, u.TxId, u.OutputIdx)
		require.False(t, found, "rollback below the anchor removes the output")
	}
}
