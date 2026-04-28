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
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// seedUtxos inserts n UTxOs whose TxIds are bytes.Repeat([]byte{i+1}, 32),
// OutputIdx 0, Amount = 1_000 * (i+1), AddedSlot 100.
func seedUtxos(t *testing.T, store *MetadataStoreSqlite, n int) [][]byte {
	t.Helper()
	ids := make([][]byte, n)
	for i := range n {
		id := bytes.Repeat([]byte{byte(i + 1)}, 32)
		ids[i] = id
		require.NoError(t, store.DB().Create(&models.Utxo{
			TxId:      id,
			OutputIdx: 0,
			Amount:    types.Uint64(uint64(1_000 * (i + 1))),
			AddedSlot: 100,
		}).Error)
	}
	return ids
}

// IterateLiveUtxos visits every live row exactly once and the
// per-row pointer carries the column values we expect.
func TestIterateLiveUtxos_VisitsEachLiveRow(t *testing.T) {
	store := setupTestDB(t)
	ids := seedUtxos(t, store, 5)

	seen := make(map[string]uint64)
	require.NoError(t, store.IterateLiveUtxos(nil, func(u *models.Utxo) error {
		seen[string(u.TxId)] = uint64(u.Amount)
		return nil
	}))

	require.Len(t, seen, len(ids))
	for i, id := range ids {
		assert.Equal(t, uint64(1_000*(i+1)), seen[string(id)],
			"amount for row %d", i)
	}
}

// IterateLiveUtxos skips rows already marked deleted.
func TestIterateLiveUtxos_SkipsDeleted(t *testing.T) {
	store := setupTestDB(t)
	ids := seedUtxos(t, store, 3)

	// Mark the middle row deleted.
	require.NoError(t, store.MarkUtxosDeletedAtSlot(nil, []types.UtxoKey{
		{TxId: ids[1], OutputIdx: 0},
	}, 999))

	var seen [][]byte
	require.NoError(t, store.IterateLiveUtxos(nil, func(u *models.Utxo) error {
		seen = append(seen, append([]byte(nil), u.TxId...))
		return nil
	}))

	sort.Slice(seen, func(i, j int) bool { return bytes.Compare(seen[i], seen[j]) < 0 })
	require.Len(t, seen, 2)
	assert.True(t, bytes.Equal(seen[0], ids[0]))
	assert.True(t, bytes.Equal(seen[1], ids[2]))
}

// Returning a non-nil error from the callback aborts iteration and
// propagates the error.
func TestIterateLiveUtxos_AbortOnError(t *testing.T) {
	store := setupTestDB(t)
	seedUtxos(t, store, 5)

	sentinel := errors.New("stop")
	visited := 0
	err := store.IterateLiveUtxos(nil, func(u *models.Utxo) error {
		visited++
		if visited == 2 {
			return sentinel
		}
		return nil
	})
	require.ErrorIs(t, err, sentinel)
	assert.Equal(t, 2, visited,
		"iteration must stop on the row whose callback returned an error")
}

// Iteration paging: setting more rows than one page returns them all.
func TestIterateLiveUtxos_Pagination(t *testing.T) {
	store := setupTestDB(t)
	const total = liveUtxoIterPageSize + 17
	for i := range total {
		// Distinct TxIds via a 4-byte int prefix so we don't collide
		// with the 32-byte size limit.
		id := make([]byte, 32)
		id[28] = byte(i >> 24)
		id[29] = byte(i >> 16)
		id[30] = byte(i >> 8)
		id[31] = byte(i)
		require.NoError(t, store.DB().Create(&models.Utxo{
			TxId: id, OutputIdx: 0, AddedSlot: 100, Amount: types.Uint64(1),
		}).Error)
	}
	count := 0
	require.NoError(t, store.IterateLiveUtxos(nil, func(*models.Utxo) error {
		count++
		return nil
	}))
	assert.Equal(t, total, count)
}

// MarkUtxosDeletedAtSlot writes deleted_slot for the matching live
// rows and is a no-op for refs that don't match anything.
func TestMarkUtxosDeletedAtSlot_MarksOnlyMatching(t *testing.T) {
	store := setupTestDB(t)
	ids := seedUtxos(t, store, 4)

	const atSlot uint64 = 555
	require.NoError(t, store.MarkUtxosDeletedAtSlot(nil, []types.UtxoKey{
		{TxId: ids[0], OutputIdx: 0},
		{TxId: ids[2], OutputIdx: 0},
		// Non-matching ref must be silently ignored.
		{TxId: bytes.Repeat([]byte{0xFF}, 32), OutputIdx: 7},
	}, atSlot))

	var rows []models.Utxo
	require.NoError(t, store.DB().Order("id").Find(&rows).Error)
	require.Len(t, rows, 4)
	assert.Equal(t, atSlot, rows[0].DeletedSlot)
	assert.Equal(t, uint64(0), rows[1].DeletedSlot)
	assert.Equal(t, atSlot, rows[2].DeletedSlot)
	assert.Equal(t, uint64(0), rows[3].DeletedSlot)
}

// Re-marking a row that's already deleted is a no-op (the WHERE
// clause filters deleted_slot == 0).
func TestMarkUtxosDeletedAtSlot_Idempotent(t *testing.T) {
	store := setupTestDB(t)
	ids := seedUtxos(t, store, 1)
	ref := types.UtxoKey{TxId: ids[0], OutputIdx: 0}

	require.NoError(t, store.MarkUtxosDeletedAtSlot(nil, []types.UtxoKey{ref}, 100))
	require.NoError(t, store.MarkUtxosDeletedAtSlot(nil, []types.UtxoKey{ref}, 200))

	var row models.Utxo
	require.NoError(t, store.DB().First(&row).Error)
	assert.Equal(t, uint64(100), row.DeletedSlot,
		"second call must not overwrite the original DeletedSlot")
}

// Empty refs slice is a no-op; no SQL is issued.
func TestMarkUtxosDeletedAtSlot_EmptyNoop(t *testing.T) {
	store := setupTestDB(t)
	ids := seedUtxos(t, store, 2)

	require.NoError(t, store.MarkUtxosDeletedAtSlot(nil, nil, 555))
	require.NoError(t, store.MarkUtxosDeletedAtSlot(nil, []types.UtxoKey{}, 555))

	var rows []models.Utxo
	require.NoError(t, store.DB().Order("id").Find(&rows).Error)
	require.Len(t, rows, len(ids))
	for _, r := range rows {
		assert.Equal(t, uint64(0), r.DeletedSlot)
	}
}
