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

package sqlite

import (
	"bytes"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

func TestGetLiveUtxosBySlot(t *testing.T) {
	store := setupTestDB(t)

	// Three UTxOs added at slot 100: two live, one already spent.
	// Two UTxOs added at slot 200 (live).
	// One UTxO added at slot 300 (live).
	rows := []models.Utxo{
		{
			TxId:      bytes.Repeat([]byte{0x01}, 32),
			OutputIdx: 0,
			AddedSlot: 100,
			Amount:    types.Uint64(1_000_000),
		},
		{
			TxId:      bytes.Repeat([]byte{0x01}, 32),
			OutputIdx: 1,
			AddedSlot: 100,
			Amount:    types.Uint64(2_000_000),
		},
		{
			// Spent later — must be excluded from slot-100 results.
			TxId:        bytes.Repeat([]byte{0x02}, 32),
			OutputIdx:   0,
			AddedSlot:   100,
			DeletedSlot: 250,
			Amount:      types.Uint64(3_000_000),
		},
		{
			TxId:      bytes.Repeat([]byte{0x03}, 32),
			OutputIdx: 0,
			AddedSlot: 200,
			Amount:    types.Uint64(4_000_000),
		},
		{
			TxId:      bytes.Repeat([]byte{0x04}, 32),
			OutputIdx: 7,
			AddedSlot: 200,
			Amount:    types.Uint64(5_000_000),
		},
		{
			TxId:      bytes.Repeat([]byte{0x05}, 32),
			OutputIdx: 0,
			AddedSlot: 300,
			Amount:    types.Uint64(6_000_000),
		},
	}
	for i := range rows {
		require.NoError(t, store.DB().Create(&rows[i]).Error)
	}

	tests := []struct {
		name string
		slot uint64
		want []models.UtxoId
	}{
		{
			name: "two live at slot 100, one spent excluded",
			slot: 100,
			want: []models.UtxoId{
				{Hash: bytes.Repeat([]byte{0x01}, 32), Idx: 0},
				{Hash: bytes.Repeat([]byte{0x01}, 32), Idx: 1},
			},
		},
		{
			name: "two live at slot 200",
			slot: 200,
			want: []models.UtxoId{
				{Hash: bytes.Repeat([]byte{0x03}, 32), Idx: 0},
				{Hash: bytes.Repeat([]byte{0x04}, 32), Idx: 7},
			},
		},
		{
			name: "single live at slot 300",
			slot: 300,
			want: []models.UtxoId{
				{Hash: bytes.Repeat([]byte{0x05}, 32), Idx: 0},
			},
		},
		{
			name: "no UTxOs at unused slot",
			slot: 999,
			want: []models.UtxoId{},
		},
		{
			name: "spent-only slot returns nothing",
			// Slot 250 is when the spent UTxO was deleted but no UTxOs
			// were added at that slot.
			slot: 250,
			want: []models.UtxoId{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.GetLiveUtxosBySlot(tc.slot, nil)
			require.NoError(t, err)
			sortUtxoIds(got)
			sortUtxoIds(tc.want)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestGetLiveUtxosBySlotExcludesSpentAtSameSlot verifies that a UTxO
// spent in the same slot it was added is excluded.
func TestGetLiveUtxosBySlotExcludesSpentAtSameSlot(t *testing.T) {
	store := setupTestDB(t)

	live := models.Utxo{
		TxId:      bytes.Repeat([]byte{0xAA}, 32),
		OutputIdx: 0,
		AddedSlot: 500,
		Amount:    types.Uint64(1),
	}
	spentSameSlot := models.Utxo{
		TxId:        bytes.Repeat([]byte{0xBB}, 32),
		OutputIdx:   0,
		AddedSlot:   500,
		DeletedSlot: 500,
		Amount:      types.Uint64(1),
	}
	require.NoError(t, store.DB().Create(&live).Error)
	require.NoError(t, store.DB().Create(&spentSameSlot).Error)

	got, err := store.GetLiveUtxosBySlot(500, nil)
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Equal(t, live.TxId, got[0].Hash)
	assert.Equal(t, live.OutputIdx, got[0].Idx)
}

func sortUtxoIds(ids []models.UtxoId) {
	sort.Slice(ids, func(i, j int) bool {
		if c := bytes.Compare(ids[i].Hash, ids[j].Hash); c != 0 {
			return c < 0
		}
		return ids[i].Idx < ids[j].Idx
	})
}
