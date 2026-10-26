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
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
)

// TestGetUtxosByAddressBoundedEnforcesMaxAssets spreads the asset load over
// several chunks with a small parameter limit, so the per-chunk LIMIT must
// carry the remaining budget across chunks for the bound to be exact.
func TestGetUtxosByAddressBoundedEnforcesMaxAssets(t *testing.T) {
	t.Parallel()
	store := newMigratedSQLiteStore(t)
	d := store.dialect.(dialect)
	d.parameterLimit = 3
	store.dialect = d

	paymentKey := bytes.Repeat([]byte{0x42}, 28)
	const utxoCount = 5
	const assetsPerUtxo = 2
	for i := range utxoCount {
		txId := make([]byte, 32)
		txId[31] = byte(i)
		assets := make([]models.Asset, 0, assetsPerUtxo)
		for j := range assetsPerUtxo {
			assets = append(assets, models.Asset{
				PolicyId: bytes.Repeat([]byte{0x01}, 28),
				Name:     []byte{byte(i), byte(j)},
				Amount:   1,
			})
		}
		require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
			TxId:       txId,
			OutputIdx:  0,
			PaymentKey: paymentKey,
			Address:    []byte("bounded-asset-address"),
			AddedSlot:  uint64(i + 1),
			Amount:     100,
			Assets:     assets,
		}))
	}
	limitSQLiteVariableNumber(t, store.writeDB, d.ParameterLimit())
	patterns := []models.UtxoAddressPattern{{PaymentPart: paymentKey}}
	const totalAssets = utxoCount * assetsPerUtxo

	got, err := store.GetUtxosByAddressBounded(
		patterns, utxoCount, totalAssets, nil,
	)
	require.NoError(t, err)
	require.Len(t, got, utxoCount)
	loaded := 0
	for i := range got {
		loaded += len(got[i].Assets)
	}
	require.Equal(t, totalAssets, loaded)

	_, err = store.GetUtxosByAddressBounded(
		patterns, utxoCount, totalAssets-1, nil,
	)
	require.ErrorIs(t, err, models.ErrTooManyUtxoResults)

	_, err = store.GetUtxosByAddressBounded(
		patterns, utxoCount-1, totalAssets, nil,
	)
	require.ErrorIs(t, err, models.ErrTooManyUtxoResults)

	for _, maxAssets := range []int{0, -1} {
		_, err = store.GetUtxosByAddressBounded(
			patterns, utxoCount, maxAssets, nil,
		)
		require.Error(t, err)
	}
}
