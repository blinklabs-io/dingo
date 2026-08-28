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
	"math"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
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

// TestAddUtxosRejectsOverflowAssetWithoutMutation covers the acceptance
// criterion that a rejected conversion must not mutate metadata: given a
// UTxO carrying a native-asset amount that overflows uint64,
// AddUtxos must fail and leave the utxo/asset tables empty, not insert a
// row with a silently wrapped amount.
func TestAddUtxosRejectsOverflowAssetWithoutMutation(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)

	var policyId lcommon.Blake2b224
	policyId[0] = 0xee
	overflowAmount := new(big.Int).SetUint64(math.MaxUint64)
	overflowAmount.Add(overflowAmount, big.NewInt(1))
	multiAsset := lcommon.NewMultiAsset[lcommon.MultiAssetTypeOutput](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeOutput{
			policyId: {
				cbor.NewByteString([]byte("asset")): overflowAmount,
			},
		},
	)

	utxo := ledger.Utxo{
		Id: shelley.NewShelleyTransactionInput(
			"0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
			0,
		),
		Output: &mary.MaryTransactionOutput{
			OutputAmount: mary.MaryTransactionOutputValue{
				Amount: 1_000_000,
				Assets: &multiAsset,
			},
		},
	}

	err := store.AddUtxos(
		[]models.UtxoSlot{{Utxo: utxo, Slot: 1}},
		nil,
	)
	require.Error(t, err)

	var utxoCount, assetCount int
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM utxo",
	).Scan(&utxoCount))
	require.NoError(t, store.writeDB.QueryRow(
		"SELECT COUNT(*) FROM asset",
	).Scan(&assetCount))
	require.Equal(t, 0, utxoCount)
	require.Equal(t, 0, assetCount)
}

// TestGetUtxosByAddressWithOrderingSkipAssets proves SkipAssets omits a
// row's native assets from the result without affecting which rows are
// returned. Callers that only need row identity or ordering (an
// exact-address candidate scan, a reference-only lookup) use this to avoid
// paying for asset joins that would be immediately discarded.
func TestGetUtxosByAddressWithOrderingSkipAssets(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)

	var policyId lcommon.Blake2b224
	policyId[0] = 0xaa
	multiAsset := lcommon.NewMultiAsset[lcommon.MultiAssetTypeOutput](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeOutput{
			policyId: {
				cbor.NewByteString([]byte("token")): big.NewInt(5),
			},
		},
	)
	utxo := ledger.Utxo{
		Id: shelley.NewShelleyTransactionInput(
			"0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20",
			0,
		),
		Output: &mary.MaryTransactionOutput{
			OutputAmount: mary.MaryTransactionOutputValue{
				Amount: 1_000_000,
				Assets: &multiAsset,
			},
		},
	}
	require.NoError(
		t,
		store.AddUtxos([]models.UtxoSlot{{Utxo: utxo, Slot: 1}}, nil),
	)

	withAssets, err := store.GetUtxosByAddressWithOrdering(
		&models.UtxoWithOrderingQuery{MatchAllAddresses: true},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, withAssets, 1)
	require.Len(t, withAssets[0].Assets, 1)

	skipped, err := store.GetUtxosByAddressWithOrdering(
		&models.UtxoWithOrderingQuery{
			MatchAllAddresses: true,
			SkipAssets:        true,
		},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, skipped, 1)
	require.Empty(t, skipped[0].Assets)
	// Row identity is unaffected by SkipAssets.
	require.Equal(t, withAssets[0].TxId, skipped[0].TxId)
}
