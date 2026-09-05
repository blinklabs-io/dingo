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
	"math"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
)

func TestGetUtxosWithHistoryFiltersAndHydratesLifecycle(t *testing.T) {
	t.Parallel()
	store := newManagementTestStore(t)

	createdHashes := [][]byte{
		bytes.Repeat([]byte{0x01}, 32),
		bytes.Repeat([]byte{0x02}, 32),
		bytes.Repeat([]byte{0x03}, 32),
	}
	createdBlockHashes := [][]byte{
		bytes.Repeat([]byte{0xa1}, 32),
		bytes.Repeat([]byte{0xa2}, 32),
		bytes.Repeat([]byte{0xa3}, 32),
	}
	spentHashes := [][]byte{
		bytes.Repeat([]byte{0x51}, 32),
		bytes.Repeat([]byte{0x61}, 32),
	}
	spentBlockHashes := [][]byte{
		bytes.Repeat([]byte{0xb1}, 32),
		bytes.Repeat([]byte{0xb2}, 32),
	}
	for i := range createdHashes {
		_, err := store.writeDB.Exec(`
INSERT INTO "transaction" (
    id, hash, block_hash, slot, block_index, type, fee, collateral_fee,
    ttl, valid
) VALUES (?, ?, ?, ?, ?, 0, '0', '0', '0', TRUE)`,
			i+1,
			createdHashes[i],
			createdBlockHashes[i],
			(i+1)*10,
			i+2,
		)
		require.NoError(t, err)
	}
	for i := range spentHashes {
		_, err := store.writeDB.Exec(`
INSERT INTO "transaction" (
    id, hash, block_hash, slot, block_index, type, fee, collateral_fee,
    ttl, valid
) VALUES (?, ?, ?, ?, 0, 0, '0', '0', '0', TRUE)`,
			i+11,
			spentHashes[i],
			spentBlockHashes[i],
			(i+5)*10,
		)
		require.NoError(t, err)
	}
	_, err := store.writeDB.Exec(`
INSERT INTO transaction_metadata_label (
    transaction_id, label, slot, cbor_value, json_value
) VALUES (?, ?, ?, ?, ?)`, 2, "42", 20, []byte{0x01}, "1")
	require.NoError(t, err)

	paymentA := bytes.Repeat([]byte{0xca}, lcommon.AddressHashSize)
	paymentB := bytes.Repeat([]byte{0xcb}, lcommon.AddressHashSize)
	policyID := bytes.Repeat([]byte{0xda}, lcommon.AddressHashSize)
	assetNames := [][]byte{[]byte("same"), []byte("same"), []byte("other")}
	for i := range createdHashes {
		transactionID := uint(i + 1)
		row := &models.Utxo{
			TransactionID: &transactionID,
			TxId:          createdHashes[i],
			PaymentKey:    paymentA,
			AddedSlot:     uint64((i + 1) * 10),
			Amount:        types.Uint64(1_000_000 + i),
			OutputIdx:     uint32(i),
			Assets: []models.Asset{{
				Name:     assetNames[i],
				PolicyId: policyID,
				Amount:   types.Uint64(i + 1),
			}},
		}
		if i == 2 {
			row.PaymentKey = paymentB
		}
		if i > 0 {
			row.SpentAtTxId = types.NullableHash(spentHashes[i-1])
			row.DeletedSlot = uint64((i + 4) * 10)
		}
		require.NoError(t, store.CreateUtxo(nil, row))
	}
	// Snapshot-imported outputs have no producing transaction row. The
	// historical query must retain their AddedSlot fallback and empty block
	// hash.
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       bytes.Repeat([]byte{0x04}, 32),
		PaymentKey: paymentB,
		AddedSlot:  40,
		Amount:     4_000_000,
		OutputIdx:  3,
	}))

	all, err := store.GetUtxosWithHistory(
		&models.UtxoHistoryQuery{MatchAllAddresses: true},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, all, 4)
	require.Equal(t, []byte{0x01, 0x02, 0x03, 0x04}, []byte{
		all[0].TxId[0], all[1].TxId[0], all[2].TxId[0], all[3].TxId[0],
	})
	require.Equal(t, uint64(10), all[0].TxSlot)
	require.Equal(t, uint32(2), all[0].TxBlockIndex)
	require.Equal(t, createdBlockHashes[0], all[0].CreatedBlockHash)
	require.Empty(t, all[0].SpentBlockHash)
	require.Len(t, all[0].Assets, 1)
	require.Equal(t, spentBlockHashes[0], all[1].SpentBlockHash)
	require.Equal(t, uint64(40), all[3].TxSlot)
	require.Empty(t, all[3].CreatedBlockHash)

	createdAfter, createdBefore := uint64(20), uint64(30)
	createdWindow, err := store.GetUtxosWithHistory(
		&models.UtxoHistoryQuery{
			MatchAllAddresses: true,
			CreatedAfter:      &createdAfter,
			CreatedBefore:     &createdBefore,
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, []byte{0x02, 0x03}, []byte{
		createdWindow[0].TxId[0], createdWindow[1].TxId[0],
	})

	spentAfter := uint64(51)
	spent, err := store.GetUtxosWithHistory(
		&models.UtxoHistoryQuery{
			MatchAllAddresses: true,
			SpentAfter:        &spentAfter,
		},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, spent, 1)
	require.Equal(t, byte(0x03), spent[0].TxId[0])

	unspent, err := store.GetUtxosWithHistory(
		&models.UtxoHistoryQuery{
			MatchAllAddresses: true,
			Status:            models.UtxoHistoryStatusUnspent,
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, []byte{0x01, 0x04}, []byte{
		unspent[0].TxId[0], unspent[1].TxId[0],
	})

	byAddress, err := store.GetUtxosWithHistory(
		&models.UtxoHistoryQuery{AddressPatterns: []models.UtxoAddressPattern{{
			PaymentPart: paymentA,
		}}},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, byAddress, 2)
	require.Equal(t, byte(0x01), byAddress[0].TxId[0])
	require.Equal(t, byte(0x02), byAddress[1].TxId[0])

	byAsset, err := store.GetUtxosWithHistory(
		&models.UtxoHistoryQuery{
			MatchAllAddresses: true,
			FilterByAsset:     true,
			AssetPolicyID:     policyID,
			AssetName:         []byte("same"),
		},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, byAsset, 2)
	require.Equal(t, byte(0x01), byAsset[0].TxId[0])
	require.Equal(t, byte(0x02), byAsset[1].TxId[0])

	metadataLabel := uint64(42)
	byMetadata, err := store.GetUtxosWithHistory(
		&models.UtxoHistoryQuery{
			MatchAllAddresses: true,
			MetadataLabel:     &metadataLabel,
		},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, byMetadata, 1)
	require.Equal(t, createdHashes[1], byMetadata[0].TxId)

	outputIndex := uint32(1)
	byRef, err := store.GetUtxosWithHistory(
		&models.UtxoHistoryQuery{
			MatchAllAddresses: true,
			TransactionID:     createdHashes[1],
			OutputIndex:       &outputIndex,
		},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, byRef, 1)
	require.Equal(t, createdHashes[1], byRef[0].TxId)

	descending, err := store.GetUtxosWithHistory(
		&models.UtxoHistoryQuery{
			MatchAllAddresses: true,
			Descending:        true,
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, []byte{0x04, 0x03, 0x02, 0x01}, []byte{
		descending[0].TxId[0],
		descending[1].TxId[0],
		descending[2].TxId[0],
		descending[3].TxId[0],
	})

	ascendingPage, err := store.GetUtxosWithHistory(
		&models.UtxoHistoryQuery{
			MatchAllAddresses: true,
			Limit:             2,
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, []byte{0x01, 0x02}, []byte{
		ascendingPage[0].TxId[0], ascendingPage[1].TxId[0],
	})
	ascendingPage, err = store.GetUtxosWithHistory(
		&models.UtxoHistoryQuery{
			MatchAllAddresses: true,
			After: &models.UtxoOrderingCursor{
				Slot:       ascendingPage[1].TxSlot,
				BlockIndex: ascendingPage[1].TxBlockIndex,
				OutputIdx:  ascendingPage[1].OutputIdx,
				TxId:       ascendingPage[1].TxId,
			},
			Limit: 2,
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, []byte{0x03, 0x04}, []byte{
		ascendingPage[0].TxId[0], ascendingPage[1].TxId[0],
	})

	descendingPage, err := store.GetUtxosWithHistory(
		&models.UtxoHistoryQuery{
			MatchAllAddresses: true,
			Descending:        true,
			Limit:             2,
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, []byte{0x04, 0x03}, []byte{
		descendingPage[0].TxId[0], descendingPage[1].TxId[0],
	})
	descendingPage, err = store.GetUtxosWithHistory(
		&models.UtxoHistoryQuery{
			MatchAllAddresses: true,
			Descending:        true,
			After: &models.UtxoOrderingCursor{
				Slot:       descendingPage[1].TxSlot,
				BlockIndex: descendingPage[1].TxBlockIndex,
				OutputIdx:  descendingPage[1].OutputIdx,
				TxId:       descendingPage[1].TxId,
			},
			Limit: 2,
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, []byte{0x02, 0x01}, []byte{
		descendingPage[0].TxId[0], descendingPage[1].TxId[0],
	})

	producerID := uint(1)
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		CollateralReturnForTxID: &producerID,
		TxId:                    createdHashes[0],
		PaymentKey:              paymentA,
		AddedSlot:               10,
		Amount:                  9_000_000,
		OutputIdx:               9,
	}))
	collateralIndex := uint32(9)
	collateralReturn, err := store.GetUtxosWithHistory(
		&models.UtxoHistoryQuery{
			MatchAllAddresses: true,
			TransactionID:     createdHashes[0],
			OutputIndex:       &collateralIndex,
		},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, collateralReturn, 1)
	require.Equal(t, uint64(10), collateralReturn[0].TxSlot)
	require.Equal(t, uint32(2), collateralReturn[0].TxBlockIndex)
	require.Equal(t, createdBlockHashes[0], collateralReturn[0].CreatedBlockHash)

	_, err = store.GetUtxosWithHistory(nil, nil)
	require.ErrorIs(t, err, models.ErrNilUtxoHistoryQuery)
	_, err = store.GetUtxosWithHistory(&models.UtxoHistoryQuery{
		MatchAllAddresses: true,
		Status:            models.UtxoHistoryStatus(255),
	}, nil)
	require.ErrorIs(t, err, models.ErrInvalidUtxoHistoryStatus)
}

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
