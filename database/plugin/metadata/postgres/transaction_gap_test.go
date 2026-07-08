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

//go:build dingo_extra_plugins

package postgres

import (
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestSetGapBlockTransactionHydratesSnapshotUtxoPostgres(t *testing.T) {
	store := newTestPostgresStore(t)

	txHash := lcommon.NewBlake2b256(
		[]byte("pg-gap-hydrate-snapshot-utxo-1"),
	)
	point := ocommon.Point{
		Hash: []byte("pg-gap-hydrate-block-hash-1"),
		Slot: 700001,
	}
	output := &mockTransactionOutput{amount: big.NewInt(1_241_280)}
	tx := &mockTransaction{
		hash:    txHash,
		isValid: true,
		produced: []lcommon.Utxo{
			{
				Id: mockTransactionInput{
					hash:  txHash,
					index: 0,
				},
				Output: output,
			},
		},
	}

	cleanup := func() {
		_ = store.DB().
			Where("tx_id = ?", txHash.Bytes()).
			Delete(&models.Utxo{}).Error
		_ = store.DB().
			Where("hash = ?", txHash.Bytes()).
			Delete(&models.Transaction{}).Error
	}
	cleanup()
	t.Cleanup(func() {
		cleanup()
		_ = store.Close()
	})

	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:      txHash.Bytes(),
		OutputIdx: 0,
		AddedSlot: point.Slot + 100,
		Amount:    types.Uint64(1_241_280),
	}).Error)

	require.NoError(t, store.SetGapBlockTransaction(tx, point, 0, nil))
	require.NoError(t, store.SetGapBlockTransaction(tx, point, 0, nil))

	var persistedTx models.Transaction
	require.NoError(t, store.DB().
		Where("hash = ?", txHash.Bytes()).
		Take(&persistedTx).Error)

	var persistedUtxo models.Utxo
	require.NoError(t, store.DB().
		Where("tx_id = ? AND output_idx = ?", txHash.Bytes(), 0).
		Take(&persistedUtxo).Error)
	require.NotNil(t, persistedUtxo.TransactionID)
	require.Equal(t, persistedTx.ID, *persistedUtxo.TransactionID)
	require.Equal(t, point.Slot, persistedUtxo.AddedSlot)
}
