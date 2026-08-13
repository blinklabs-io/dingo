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
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

type utxoReadStore interface {
	CreateUtxo(types.Txn, *models.Utxo) error
	GetUtxo([]byte, uint32, types.Txn) (*models.Utxo, error)
	GetUtxoIncludingSpent([]byte, uint32, types.Txn) (*models.Utxo, error)
	GetUtxosAddedAfterSlot(uint64, types.Txn) ([]models.Utxo, error)
	GetLiveUtxosBySlot(uint64, types.Txn) ([]models.UtxoId, error)
	GetUtxosBySlot(uint64, types.Txn) ([]models.UtxoId, error)
	GetUtxosDeletedBeforeSlot(
		uint64,
		int,
		types.Txn,
	) ([]models.Utxo, error)
	GetUtxosByAddress(
		[]models.UtxoAddressPattern,
		types.Txn,
	) ([]models.Utxo, error)
	GetUtxosByAddressAtSlot(
		models.UtxoAddressPattern,
		uint64,
		types.Txn,
	) ([]models.Utxo, error)
	GetControlledAmountByCredential(uint8, []byte, types.Txn) (uint64, error)
	GetScriptLockedSupply(types.Txn) (uint64, error)
	GetUtxosByAssets([]byte, []byte, types.Txn) ([]models.Utxo, error)
	IterateLiveUtxos(types.Txn, func(*models.Utxo) error) error
}

type utxoReadState struct {
	live            *models.Utxo
	spentLiveLookup *models.Utxo
	spent           *models.Utxo
	added           []models.Utxo
	liveAtSlot      []models.UtxoId
	allAtSlot       []models.UtxoId
	deleted         []models.Utxo
	byAddress       []models.Utxo
	byAddressAtSlot []models.Utxo
	controlled      uint64
	scriptLocked    uint64
	byAsset         []models.Utxo
	iterated        []models.Utxo
}

func TestSharedSQLStoreUtxoReadParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	_ = exerciseUtxoReadStore(t, store)
}

func exerciseUtxoReadStore(t *testing.T, store utxoReadStore) utxoReadState {
	t.Helper()
	paymentKey := bytes.Repeat([]byte{0x11}, 28)
	stakingKey := bytes.Repeat([]byte{0x22}, 28)
	policyID := bytes.Repeat([]byte{0x33}, 28)
	assetName := []byte("asset")
	for _, utxo := range []*models.Utxo{
		{
			TxId:          []byte("tx-live"),
			OutputIdx:     0,
			PaymentKey:    paymentKey,
			StakingKey:    stakingKey,
			CredentialTag: 1,
			AddedSlot:     10,
			Amount:        100,
			Assets: []models.Asset{{
				Name: assetName, NameHex: []byte("6173736574"),
				PolicyId: policyID, Fingerprint: []byte("fingerprint"),
				Amount: 5,
			}},
		},
		{
			TxId:        []byte("tx-spent"),
			OutputIdx:   1,
			PaymentKey:  paymentKey,
			StakingKey:  stakingKey,
			AddedSlot:   10,
			DeletedSlot: 20,
			Amount:      50,
		},
		{
			TxId:          []byte("tx-script"),
			OutputIdx:     0,
			PaymentKey:    bytes.Repeat([]byte{0x44}, 28),
			AddedSlot:     30,
			Amount:        70,
			PaymentScript: true,
		},
	} {
		require.NoError(t, store.CreateUtxo(nil, utxo))
	}

	var ret utxoReadState
	var err error
	ret.live, err = store.GetUtxo([]byte("tx-live"), 0, nil)
	require.NoError(t, err)
	ret.spentLiveLookup, err = store.GetUtxo([]byte("tx-spent"), 1, nil)
	require.NoError(t, err)
	ret.spent, err = store.GetUtxoIncludingSpent(
		[]byte("tx-spent"),
		1,
		nil,
	)
	require.NoError(t, err)
	ret.added, err = store.GetUtxosAddedAfterSlot(15, nil)
	require.NoError(t, err)
	ret.liveAtSlot, err = store.GetLiveUtxosBySlot(10, nil)
	require.NoError(t, err)
	ret.allAtSlot, err = store.GetUtxosBySlot(10, nil)
	require.NoError(t, err)
	ret.deleted, err = store.GetUtxosDeletedBeforeSlot(25, 1, nil)
	require.NoError(t, err)
	pattern := models.UtxoAddressPattern{PaymentPart: paymentKey}
	ret.byAddress, err = store.GetUtxosByAddress(
		[]models.UtxoAddressPattern{pattern},
		nil,
	)
	require.NoError(t, err)
	ret.byAddressAtSlot, err = store.GetUtxosByAddressAtSlot(
		pattern,
		15,
		nil,
	)
	require.NoError(t, err)
	ret.controlled, err = store.GetControlledAmountByCredential(
		1,
		stakingKey,
		nil,
	)
	require.NoError(t, err)
	ret.scriptLocked, err = store.GetScriptLockedSupply(nil)
	require.NoError(t, err)
	ret.byAsset, err = store.GetUtxosByAssets(policyID, assetName, nil)
	require.NoError(t, err)
	require.NoError(t, store.IterateLiveUtxos(
		nil,
		func(utxo *models.Utxo) error {
			ret.iterated = append(ret.iterated, *utxo)
			return nil
		},
	))
	return ret
}
