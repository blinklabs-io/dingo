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
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func supplyRows(t testing.TB, store *Store) map[int64]int64 {
	t.Helper()
	rows, err := store.writeDB.QueryContext(
		context.Background(), `SELECT slot, amount FROM script_locked_supply`,
	)
	require.NoError(t, err)
	defer rows.Close()
	out := map[int64]int64{}
	for rows.Next() {
		var slot, amount int64
		require.NoError(t, rows.Scan(&slot, &amount))
		out[slot] = amount
	}
	require.NoError(t, rows.Err())
	return out
}

func addSupply(t testing.TB, store *Store, slot uint64, delta int64) error {
	t.Helper()
	return store.withWriteTransaction(
		nil,
		func(db queryer, ctx context.Context) error {
			return store.addScriptLockedSupply(ctx, db, slot, delta)
		},
	)
}

func TestScriptLockedSupplyEmptyReadsZero(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	got, err := store.GetScriptLockedSupply(nil)
	require.NoError(t, err)
	require.Zero(t, got)
}

func TestScriptLockedSupplyOutOfOrderSlotsShiftLaterRows(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	require.NoError(t, addSupply(t, store, 10, 5))
	require.NoError(t, addSupply(t, store, 30, 7))
	require.NoError(t, addSupply(t, store, 20, 3))
	require.NoError(t, addSupply(t, store, 10, 1))
	require.Equal(
		t,
		map[int64]int64{10: 6, 20: 9, 30: 16},
		supplyRows(t, store),
	)
	got, err := store.GetScriptLockedSupply(nil)
	require.NoError(t, err)
	require.EqualValues(t, 16, got)
}

// A write older than every row has no base to sum from: the seed row already
// counts the output, so the delta only shifts the rows above it. Writing
// delta alone as a row would fail a debit and leave a credit as a total a
// rollback would then trust.
func TestScriptLockedSupplyBelowOldestRowShiftsWithoutARow(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	_, err := store.writeDB.Exec(
		`INSERT INTO script_locked_supply (slot, amount) VALUES (100, 1000)`,
	)
	require.NoError(t, err)

	require.NoError(t, addSupply(t, store, 50, -10))
	require.Equal(t, map[int64]int64{100: 990}, supplyRows(t, store))

	require.NoError(t, addSupply(t, store, 60, 4))
	require.Equal(t, map[int64]int64{100: 994}, supplyRows(t, store))

	require.ErrorContains(
		t, addSupply(t, store, 70, -995), "out of range",
	)
	require.Equal(t, map[int64]int64{100: 994}, supplyRows(t, store))
}

func TestScriptLockedSupplyRejectsOverflowAndUnderflow(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	_, err := store.writeDB.Exec(
		`INSERT INTO script_locked_supply (slot, amount) VALUES (10, ?)`,
		int64(math.MaxInt64-5),
	)
	require.NoError(t, err)
	require.ErrorContains(t, addSupply(t, store, 11, 10), "out of range")
	require.ErrorContains(t, addSupply(t, store, 10, 10), "out of range")
	require.Equal(
		t,
		map[int64]int64{10: math.MaxInt64 - 5},
		supplyRows(t, store),
	)
	require.ErrorContains(
		t, addSupply(t, store, 12, -(math.MaxInt64-4)), "out of range",
	)
	require.Equal(
		t,
		map[int64]int64{10: math.MaxInt64 - 5},
		supplyRows(t, store),
	)
}

func TestPruneScriptLockedSupplyKeepsBaseRow(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	for _, slot := range []int64{10, 20, 30, 40} {
		_, err := store.writeDB.Exec(
			`INSERT INTO script_locked_supply (slot, amount) VALUES (?, ?)`,
			slot, slot*100,
		)
		require.NoError(t, err)
	}
	require.NoError(t, store.PruneScriptLockedSupplyBefore(5, nil))
	require.Len(t, supplyRows(t, store), 4)
	require.NoError(t, store.PruneScriptLockedSupplyBefore(25, nil))
	require.Equal(
		t,
		map[int64]int64{20: 2000, 30: 3000, 40: 4000},
		supplyRows(t, store),
	)
	require.NoError(t, store.PruneScriptLockedSupplyBefore(40, nil))
	require.Equal(t, map[int64]int64{40: 4000}, supplyRows(t, store))
}

func insertRawUtxo(
	t testing.TB, store *Store, id byte, added, deleted int64,
	amount string, script bool,
) {
	t.Helper()
	_, err := store.writeDB.Exec(
		`INSERT INTO utxo (tx_id, output_idx, added_slot, deleted_slot, amount, payment_script)
VALUES (?, 0, ?, ?, ?, ?)`,
		[]byte{id},
		added,
		deleted,
		amount,
		script,
	)
	require.NoError(t, err)
}

// A rollback below the only aggregate row (a seed written at a later slot)
// rebuilds the base from the restored rows instead of reading zero.
func TestRollbackBelowSeedRowRebuildsFromRestoredRows(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	insertRawUtxo(t, store, 1, 10, 0, "400", true)
	insertRawUtxo(t, store, 2, 20, 90, "250", true)
	insertRawUtxo(t, store, 3, 20, 0, "999", false)
	insertRawUtxo(t, store, 4, 95, 0, "70", true)
	require.NoError(t, store.RebuildScriptLockedSupply(100, nil))
	require.Equal(t, map[int64]int64{100: 470}, supplyRows(t, store))

	require.NoError(t, store.DeleteUtxosAfterSlot(50, nil))
	require.NoError(t, store.SetUtxosNotDeletedAfterSlot(50, nil))

	require.Equal(t, map[int64]int64{50: 650}, supplyRows(t, store))
	require.EqualValues(t, 650, fullScanScriptLockedSupply(t, store))
}

func hash32(id byte) []byte {
	h := make([]byte, 32)
	h[0] = id
	return h
}

func scriptModel(
	id byte,
	slot uint64,
	amount uint64,
	script bool,
) *models.Utxo {
	return &models.Utxo{
		TxId:          hash32(id),
		OutputIdx:     0,
		PaymentKey:    lcommon.NewBlake2b224([]byte{id}).Bytes(),
		AddedSlot:     slot,
		Amount:        types.Uint64(amount),
		PaymentScript: script,
	}
}

// Every primitive that creates a live output or takes one out of the live
// set keeps the aggregate equal to a full scan: CreateUtxo, the single-row
// spend, the bulk mark, and a delete of a row that is still live.
func TestScriptLockedSupplyDirectUtxoPrimitives(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	check := func(step string, want uint64) {
		t.Helper()
		require.Equal(t, want, fullScanScriptLockedSupply(t, store), step)
		got, err := store.GetScriptLockedSupply(nil)
		require.NoError(t, err)
		require.Equal(t, want, got, step)
	}
	for i, amount := range []uint64{100, 200, 400, 800} {
		require.NoError(
			t,
			store.CreateUtxo(nil, scriptModel(byte(i+1), 10, amount, true)),
		)
	}
	require.NoError(t, store.CreateUtxo(nil, scriptModel(9, 10, 5000, false)))
	check("create", 1500)

	input := func(id byte) lcommon.TransactionInput {
		return overlayTxInput(t, hash32(id), 0)
	}
	require.NoError(t, store.SetUtxoDeletedAtSlot(input(1), 20, nil, nil))
	check("SetUtxoDeletedAtSlot", 1400)

	require.NoError(t, store.MarkUtxosDeletedAtSlot(
		nil,
		[]types.UtxoKey{
			{TxId: hash32(2), OutputIdx: 0},
			{TxId: hash32(9), OutputIdx: 0},
		},
		21,
	))
	check("MarkUtxosDeletedAtSlot", 1200)
	// Marking an already-spent output again changes nothing.
	require.NoError(t, store.MarkUtxosDeletedAtSlot(
		nil, []types.UtxoKey{{TxId: hash32(2), OutputIdx: 0}}, 22,
	))
	check("MarkUtxosDeletedAtSlot repeat", 1200)

	// A live row is removed at the tip slot.
	require.NoError(t, store.SetTip(ochainsync.Tip{
		Point: ocommon.Point{Slot: 30, Hash: hash32(0xEE)}, BlockNumber: 3,
	}, nil))
	require.NoError(
		t,
		store.DeleteUtxos(
			[]models.UtxoId{
				{Hash: hash32(3), Idx: 0},
				{Hash: hash32(1), Idx: 0},
			},
			nil,
		),
	)
	check("DeleteUtxos live and spent", 800)
}

// DeleteUtxos reads the rows it removes once, for the supply debit and for the
// stake credentials it refreshes, so a live row's stake must still leave.
func TestDeleteUtxosRefreshesStakeOfDeletedLiveRow(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	key := lcommon.NewBlake2b224([]byte("delete-utxos-stake")).Bytes()
	for id, amount := range map[byte]uint64{1: 100, 2: 200} {
		utxo := scriptModel(id, 10, amount, false)
		utxo.StakingKey = key
		require.NoError(t, store.CreateUtxo(nil, utxo))
	}
	require.NoError(t, store.SetTip(ochainsync.Tip{
		Point: ocommon.Point{Slot: 30, Hash: hash32(0xEE)}, BlockNumber: 3,
	}, nil))
	require.NoError(t, store.DeleteUtxos(
		[]models.UtxoId{{Hash: hash32(1), Idx: 0}},
		nil,
	))
	utxoStake, _, _, _ := readRewardLiveStake(
		t, store, models.NewStakeCredentialRef(0, key),
	)
	require.EqualValues(t, 200, utxoStake)
}
