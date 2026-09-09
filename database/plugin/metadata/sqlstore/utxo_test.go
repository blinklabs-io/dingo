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

// TestDedupeUtxoIDs proves GetUtxosByRefs' input deduplication removes
// repeated (Hash, Idx) pairs, including a repeat that would otherwise land
// in a different 400-ref chunk, while preserving order of first occurrence
// and leaving distinct refs (including a same-hash-different-index pair)
// untouched (#392).
func TestDedupeUtxoIDs(t *testing.T) {
	hashA := []byte{0x01, 0x02, 0x03}
	hashB := []byte{0x04, 0x05, 0x06}

	ids := []models.UtxoId{
		{Hash: hashA, Idx: 0},
		{Hash: hashB, Idx: 0},
		{Hash: hashA, Idx: 0}, // duplicate of the first
		{Hash: hashA, Idx: 1}, // same hash, different index: distinct
	}

	got := dedupeUtxoIDs(ids)
	require.Equal(t, []models.UtxoId{
		{Hash: hashA, Idx: 0},
		{Hash: hashB, Idx: 0},
		{Hash: hashA, Idx: 1},
	}, got)
}

// TestDedupeUtxoIDs_CrossChunkDuplicate proves a duplicate ref is removed
// even when the two occurrences would fall into different 400-ref chunks
// inside GetUtxosByRefs.
func TestDedupeUtxoIDs_CrossChunkDuplicate(t *testing.T) {
	dup := models.UtxoId{Hash: []byte{0xAA}, Idx: 42}

	ids := make([]models.UtxoId, 0, 401)
	ids = append(ids, dup)
	for i := range 399 {
		ids = append(ids, models.UtxoId{
			Hash: []byte{byte(i), byte(i >> 8)},
			Idx:  uint32(i),
		})
	}
	// Placed at index 400, past the first 400-ref chunk boundary.
	ids = append(ids, dup)

	got := dedupeUtxoIDs(ids)
	require.Len(t, got, 400, "cross-chunk duplicate should be removed")

	count := 0
	for _, id := range got {
		if id.Idx == dup.Idx && string(id.Hash) == string(dup.Hash) {
			count++
		}
	}
	require.Equal(t, 1, count, "duplicate ref must appear exactly once")
}
