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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

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

func TestGetUtxosBatchUsesTxIdOutputIndex(t *testing.T) {
	store := setupTestDB(t)

	refs := make([]UtxoRef, 0, 12)
	for i := range 12 {
		txID := bytes.Repeat([]byte{byte(i + 1)}, 32)
		outputIdx := uint32(i % 3) //nolint:gosec
		row := models.Utxo{
			TxId:      txID,
			OutputIdx: outputIdx,
			AddedSlot: uint64(i + 1), //nolint:gosec
			Amount:    types.Uint64(i + 1),
		}
		require.NoError(t, store.DB().Create(&row).Error)
		refs = append(refs, UtxoRef{TxId: txID, OutputIdx: outputIdx})
	}

	var capturedSQL string
	var capturedVars []any
	callbackName := "test:capture_get_utxos_batch_sql"
	require.NoError(t, store.ReadDB().Callback().Query().
		After("gorm:query").
		Register(callbackName, func(tx *gorm.DB) {
			if capturedSQL != "" {
				return
			}
			capturedSQL = tx.Statement.SQL.String()
			capturedVars = append([]any(nil), tx.Statement.Vars...)
		}))

	got, err := store.GetUtxosBatch(refs, nil)
	require.NoError(t, err)
	require.Len(t, got, len(refs))
	require.NotEmpty(t, capturedSQL)
	require.Contains(t, capturedSQL, "INDEXED BY "+utxoRefLookupIndex)

	planRows, err := store.DB().
		Raw(
			"EXPLAIN QUERY PLAN "+capturedSQL,
			capturedVars...,
		).Rows()
	require.NoError(t, err)
	defer planRows.Close()

	var details []string
	for planRows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(t, planRows.Scan(&id, &parent, &notUsed, &detail))
		details = append(details, detail)
	}
	require.NoError(t, planRows.Err())
	plan := strings.Join(details, "\n")
	assert.Contains(t, plan, utxoRefLookupIndex)
	assert.NotContains(t, plan, "idx_utxo_deleted_slot")
}

// TestGetUtxoAddressKeysBatchUsesSkinnyTxIdOutputIndexLookup verifies that the
// address-key lookup selects only the address-index fields and uses the UTxO ref
// index instead of a broader deleted-slot scan.
func TestGetUtxoAddressKeysBatchUsesSkinnyTxIdOutputIndexLookup(t *testing.T) {
	store := setupTestDB(t)

	refs := make([]UtxoRef, 0, 12)
	for i := range 12 {
		txID := bytes.Repeat([]byte{byte(i + 1)}, 32)
		outputIdx := uint32(i % 3) //nolint:gosec
		row := models.Utxo{
			TxId:       txID,
			OutputIdx:  outputIdx,
			AddedSlot:  uint64(i + 1), //nolint:gosec
			PaymentKey: bytes.Repeat([]byte{byte(0x40 + i)}, 28),
			StakingKey: bytes.Repeat([]byte{byte(0x70 + i)}, 28),
			Amount:     types.Uint64(i + 1),
		}
		require.NoError(t, store.DB().Create(&row).Error)
		refs = append(refs, UtxoRef{TxId: txID, OutputIdx: outputIdx})
	}

	var capturedSQL string
	var capturedVars []any
	callbackName := "test:capture_get_utxo_address_keys_batch_sql"
	require.NoError(t, store.ReadDB().Callback().Query().
		After("gorm:query").
		Register(callbackName, func(tx *gorm.DB) {
			if capturedSQL != "" {
				return
			}
			capturedSQL = tx.Statement.SQL.String()
			capturedVars = append([]any(nil), tx.Statement.Vars...)
		}))

	got, err := store.GetUtxoAddressKeysBatch(refs, nil)
	require.NoError(t, err)
	require.Len(t, got, len(refs))
	require.NotEmpty(t, capturedSQL)
	require.Contains(t, capturedSQL, "INDEXED BY "+utxoRefLookupIndex)
	require.Contains(t, capturedSQL, "`tx_id`")
	require.Contains(t, capturedSQL, "`output_idx`")
	require.Contains(t, capturedSQL, "`payment_key`")
	require.Contains(t, capturedSQL, "`staking_key`")
	require.NotContains(t, capturedSQL, "`amount`")
	require.NotContains(t, capturedSQL, "`datum_hash`")

	planRows, err := store.DB().
		Raw(
			"EXPLAIN QUERY PLAN "+capturedSQL,
			capturedVars...,
		).Rows()
	require.NoError(t, err)
	defer planRows.Close()

	var details []string
	for planRows.Next() {
		var id, parent, notUsed int
		var detail string
		require.NoError(t, planRows.Scan(&id, &parent, &notUsed, &detail))
		details = append(details, detail)
	}
	require.NoError(t, planRows.Err())
	plan := strings.Join(details, "\n")
	assert.Contains(t, plan, utxoRefLookupIndex)
	assert.NotContains(t, plan, "idx_utxo_deleted_slot")
}

// BenchmarkGetUtxoAddressKeysBatch compares the full UTxO batch lookup with
// the skinny address-key lookup used by API-mode backfill address indexing.
func BenchmarkGetUtxoAddressKeysBatch(b *testing.B) {
	store := setupTestDB(b)

	const rows = 512
	refs := make([]UtxoRef, 0, rows)
	for i := range rows {
		txID := bytes.Repeat([]byte{byte((i % 250) + 1)}, 32)
		txID[31] = byte(i / 250)
		outputIdx := uint32(i % 4) //nolint:gosec
		row := models.Utxo{
			TxId:       txID,
			OutputIdx:  outputIdx,
			AddedSlot:  uint64(i + 1), //nolint:gosec
			PaymentKey: bytes.Repeat([]byte{byte(0x20 + (i % 80))}, 28),
			StakingKey: bytes.Repeat([]byte{byte(0x80 + (i % 80))}, 28),
			Amount:     types.Uint64(i + 1),
			DatumHash:  bytes.Repeat([]byte{byte(0x40 + (i % 80))}, 32),
		}
		require.NoError(b, store.DB().Create(&row).Error)
		refs = append(refs, UtxoRef{TxId: txID, OutputIdx: outputIdx})
	}

	b.ReportAllocs()
	b.Run("full-utxo", func(b *testing.B) {
		for b.Loop() {
			got, err := store.GetUtxosBatch(refs, nil)
			if err != nil {
				b.Fatal(err)
			}
			if len(got) != len(refs) {
				b.Fatalf("got %d rows, want %d", len(got), len(refs))
			}
		}
	})
	b.Run("address-keys", func(b *testing.B) {
		for b.Loop() {
			got, err := store.GetUtxoAddressKeysBatch(refs, nil)
			if err != nil {
				b.Fatal(err)
			}
			if len(got) != len(refs) {
				b.Fatalf("got %d rows, want %d", len(got), len(refs))
			}
		}
	})
}

func sortUtxoIds(ids []models.UtxoId) {
	sort.Slice(ids, func(i, j int) bool {
		if c := bytes.Compare(ids[i].Hash, ids[j].Hash); c != 0 {
			return c < 0
		}
		return ids[i].Idx < ids[j].Idx
	})
}
