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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlstore

import (
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

func TestSQLitePParamUpdateOrdering(t *testing.T) {
	t.Parallel()
	testPParamUpdateOrdering(t, newMigratedSQLiteStore(t))
}

// testPParamUpdateOrdering pins the storage contract classic update
// enactment relies on: GetPParamUpdates(e) returns the rows for e and e-1
// with their stored fields, and row IDs follow insertion order, including
// after a rollback deletes the newest rows, so (added slot, ID) is chain
// order for a genesis key's proposals within one slot.
func testPParamUpdateOrdering(t *testing.T, store *Store) {
	t.Helper()
	type row struct {
		genesis byte
		slot    uint64
		epoch   uint64
	}
	insert := func(r row) {
		require.NoError(t, store.SetPParamUpdate(
			[]byte{
				r.genesis,
			},
			[]byte{0xa1, 0x00, r.genesis},
			r.slot,
			r.epoch,
			nil,
		))
	}
	for _, r := range []row{
		{genesis: 4, slot: 200, epoch: 2},
		{genesis: 3, slot: 250, epoch: 3},
		{genesis: 1, slot: 300, epoch: 3},
		{genesis: 2, slot: 305, epoch: 3},
		{genesis: 1, slot: 305, epoch: 3},
		{genesis: 5, slot: 360, epoch: 4},
	} {
		insert(r)
	}
	read := func() []models.PParamUpdate {
		rows, err := store.GetPParamUpdates(3, nil)
		require.NoError(t, err)
		return rows
	}
	byInsertion := func(rows []models.PParamUpdate) []models.PParamUpdate {
		ret := append([]models.PParamUpdate(nil), rows...)
		for i := 1; i < len(ret); i++ {
			for j := i; j > 0 && ret[j].ID < ret[j-1].ID; j-- {
				ret[j], ret[j-1] = ret[j-1], ret[j]
			}
		}
		return ret
	}

	rows := byInsertion(read())
	require.Len(t, rows, 5)
	want := []row{
		{genesis: 4, slot: 200, epoch: 2},
		{genesis: 3, slot: 250, epoch: 3},
		{genesis: 1, slot: 300, epoch: 3},
		{genesis: 2, slot: 305, epoch: 3},
		{genesis: 1, slot: 305, epoch: 3},
	}
	for i, w := range want {
		require.Equal(t, []byte{w.genesis}, rows[i].GenesisHash, "row %d", i)
		require.Equal(
			t,
			[]byte{0xa1, 0x00, w.genesis},
			rows[i].Cbor,
			"row %d",
			i,
		)
		require.Equal(t, w.slot, rows[i].AddedSlot, "row %d", i)
		require.Equal(t, w.epoch, rows[i].Epoch, "row %d", i)
	}

	require.NoError(t, store.DeletePParamUpdatesAfterSlot(300, nil))
	survivors := byInsertion(read())
	require.Len(t, survivors, 3)
	insert(row{genesis: 2, slot: 305, epoch: 3})
	insert(row{genesis: 1, slot: 305, epoch: 3})
	rows = byInsertion(read())
	require.Len(t, rows, 5)
	require.Equal(t, survivors, rows[:3])
	require.Greater(t, rows[3].ID, survivors[2].ID)
	require.Equal(t, []byte{2}, rows[3].GenesisHash)
	require.Equal(t, []byte{1}, rows[4].GenesisHash)
	require.Greater(t, rows[4].ID, rows[3].ID)
}
