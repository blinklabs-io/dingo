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
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

const testAddress = "addr_test1qz2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzer3jcu5d8ps7zex2k2xt3uqxgjqnnj83ws8lhrn648jjxtwq2ytjqp"

func TestSetTransactionIdempotentlyStoresMultiAssetOutputs(t *testing.T) {
	store := setupTestDBWithMode(t, types.StorageModeAPI)
	txID := bytes.Repeat([]byte{0x35}, 32)
	policyID := bytes.Repeat([]byte{0xcd}, 28)
	assetName := []byte("INDY")
	output0 := mustTestOutput(t, policyID, assetName, 1)
	output1 := mustTestOutput(t, policyID, assetName, 2)
	tx := &mockTransaction{
		hash:    lcommon.NewBlake2b256(txID),
		isValid: true,
		produced: []lcommon.Utxo{
			{Id: mustTestInput(t, txID, 0), Output: output0},
			{Id: mustTestInput(t, txID, 1), Output: output1},
		},
		outputs: []lcommon.TransactionOutput{output0, output1},
	}
	point := ocommon.Point{
		Hash: bytes.Repeat([]byte{0x77}, 32),
		Slot: 12345,
	}

	require.NoError(t, store.SetTransaction(tx, point, 0, nil, nil))
	require.NoError(t, store.SetTransaction(tx, point, 0, nil, nil))

	requireUtxoAssetCounts(t, store, 2, 2)
}

func TestFlushBatchIdempotentlyStoresMultiAssetOutputs(t *testing.T) {
	store := setupTestDBWithMode(t, types.StorageModeAPI)
	txID := bytes.Repeat([]byte{0x46}, 32)
	policyID := bytes.Repeat([]byte{0xef}, 28)
	assetName := []byte("DAO")
	batch := NewBatchAccumulator()
	batch.AddUtxoOutput(models.Utxo{
		TxId:      txID,
		OutputIdx: 0,
		AddedSlot: 100,
		Amount:    10,
		Assets: []models.Asset{
			testAsset(policyID, assetName, 1),
		},
	})
	batch.AddUtxoOutput(models.Utxo{
		TxId:      txID,
		OutputIdx: 1,
		AddedSlot: 100,
		Amount:    20,
		Assets: []models.Asset{
			testAsset(policyID, assetName, 2),
		},
	})

	require.NoError(t, store.FlushBatch(batch, nil))
	require.NoError(t, store.FlushBatch(batch, nil))

	requireUtxoAssetCounts(t, store, 2, 2)
}

func mustTestInput(
	t *testing.T,
	txID []byte,
	idx uint32,
) lcommon.TransactionInput {
	t.Helper()
	input, err := mockledger.NewTransactionInputBuilder().
		WithTxId(txID).
		WithIndex(idx).
		Build()
	require.NoError(t, err)
	return input
}

func mustTestOutput(
	t *testing.T,
	policyID []byte,
	assetName []byte,
	amount uint64,
) lcommon.TransactionOutput {
	t.Helper()
	output, err := mockledger.NewTransactionOutputBuilder().
		WithAddress(testAddress).
		WithLovelace(2_000_000).
		WithAssets(mockledger.Asset{
			PolicyId:  policyID,
			AssetName: assetName,
			Amount:    amount,
		}).
		Build()
	require.NoError(t, err)
	return output
}

func testAsset(
	policyID []byte,
	name []byte,
	amount uint64,
) models.Asset {
	fingerprint := lcommon.NewAssetFingerprint(policyID, name)
	return models.Asset{
		Name:        name,
		NameHex:     []byte(hex.EncodeToString(name)),
		PolicyId:    policyID,
		Fingerprint: []byte(fingerprint.String()),
		Amount:      types.Uint64(amount),
	}
}

func requireUtxoAssetCounts(
	t *testing.T,
	store *MetadataStoreSqlite,
	wantUtxos int64,
	wantAssets int64,
) {
	t.Helper()
	var utxoCount int64
	require.NoError(
		t,
		store.DB().Model(&models.Utxo{}).Count(&utxoCount).Error,
	)
	require.Equal(t, wantUtxos, utxoCount)
	var assetCount int64
	require.NoError(
		t,
		store.DB().Model(&models.Asset{}).Count(&assetCount).Error,
	)
	require.Equal(t, wantAssets, assetCount)
	var zeroUtxoAssets int64
	require.NoError(
		t,
		store.DB().Model(&models.Asset{}).
			Where("utxo_id = 0").
			Count(&zeroUtxoAssets).Error,
	)
	require.Zero(t, zeroUtxoAssets)
}
