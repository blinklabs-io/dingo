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

package database

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func supplyTestUtxo(
	id byte,
	slot, deleted, amount uint64,
	script bool,
) *models.Utxo {
	txID := make([]byte, 32)
	txID[0] = id
	return &models.Utxo{
		TxId:          txID,
		PaymentKey:    make([]byte, 28),
		AddedSlot:     slot,
		DeletedSlot:   deleted,
		Amount:        types.Uint64(amount),
		PaymentScript: script,
	}
}

func scriptSupply(t *testing.T, db *Database) uint64 {
	t.Helper()
	got, err := db.Metadata().GetScriptLockedSupply(nil)
	require.NoError(t, err)
	return got
}

// The rollback sweep drops the supply rows above the rollback slot and the
// restored UTxO rows agree with what remains.
func TestTruncateAfterSlotRestoresScriptLockedSupply(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	target := testIndexedBlock(50, 1, 0x15)
	require.NoError(t, db.BlockCreate(target, nil))
	for _, u := range []*models.Utxo{
		supplyTestUtxo(1, 40, 0, 300, true),
		supplyTestUtxo(2, 60, 0, 500, true),
		supplyTestUtxo(3, 60, 0, 900, false),
	} {
		require.NoError(t, db.Metadata().CreateUtxo(nil, u))
	}
	require.NoError(
		t,
		db.MetadataTxn(context.Background(), true).Do(func(txn *Txn) error {
			return db.Metadata().MarkUtxosDeletedAtSlot(
				txn.Metadata(),
				[]types.UtxoKey{{TxId: supplyTestUtxo(1, 0, 0, 0, true).TxId}},
				70,
			)
		}),
	)
	require.EqualValues(t, 500, scriptSupply(t, db))

	require.NoError(t, db.Transaction(context.Background(), true).Do(
		func(txn *Txn) error {
			_, _, err := db.TruncateAfterSlot(
				context.Background(),
				ocommon.Point{Slot: 50, Hash: target.Hash},
				0, txn,
			)
			return err
		},
	))
	require.EqualValues(t, 300, scriptSupply(t, db))
}

// Advancing the consumed-UTxO prune floor drops the supply rows below the
// base row at the floor.
func TestConsumedUtxoPruneFloorPrunesScriptLockedSupply(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{DataDir: dataDir})
	require.NoError(t, err)
	for _, u := range []*models.Utxo{
		supplyTestUtxo(1, 10, 0, 100, true),
		supplyTestUtxo(2, 20, 0, 200, true),
		supplyTestUtxo(3, 30, 0, 400, true),
		supplyTestUtxo(4, 5, 15, 50, false),
	} {
		require.NoError(t, db.Metadata().CreateUtxo(nil, u))
	}
	count := func() int {
		var n int
		raw, err := openMetadataSQLite(dataDir)
		require.NoError(t, err)
		defer raw.Close()
		require.NoError(t, raw.QueryRow(
			"SELECT COUNT(*) FROM script_locked_supply",
		).Scan(&n))
		return n
	}
	require.Equal(t, 3, count())
	pruned, err := db.UtxosDeleteConsumed(context.Background(), 25, 100, nil)
	require.NoError(t, err)
	require.Equal(t, 1, pruned)
	require.Equal(t, 2, count(), "rows at slots 20 and 30 remain")
	require.EqualValues(t, 700, scriptSupply(t, db))
}

// RebuildScriptLockedSupply replaces the aggregate with a scan, which is what
// a bulk load that inserts live outputs without the maintained primitives
// (ImportUtxos) relies on.
func TestRebuildScriptLockedSupplyCountsBulkImportedOutputs(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	db, err := newTestDatabase(t, &Config{DataDir: dataDir})
	require.NoError(t, err)
	require.NoError(t, db.Metadata().CreateUtxo(
		nil, supplyTestUtxo(1, 10, 0, 100, true),
	))
	require.NoError(t, db.MetadataTxn(context.Background(), true).Do(
		func(txn *Txn) error {
			return db.Metadata().ImportUtxos(
				[]models.Utxo{*supplyTestUtxo(2, 20, 0, 50, true)},
				txn.Metadata(),
			)
		},
	))
	require.EqualValues(t, 100, scriptSupply(t, db))

	require.NoError(
		t,
		db.RebuildScriptLockedSupply(context.Background(), 77, nil),
	)
	require.EqualValues(t, 150, scriptSupply(t, db))
	raw, err := openMetadataSQLite(dataDir)
	require.NoError(t, err)
	defer raw.Close()
	var slot, rows int64
	require.NoError(t, raw.QueryRow(
		"SELECT MIN(slot), COUNT(*) FROM script_locked_supply",
	).Scan(&slot, &rows))
	require.EqualValues(t, 77, slot)
	require.EqualValues(t, 1, rows)
}
