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

type utxoMutationStore interface {
	CreateAccount(types.Txn, *models.Account) error
	CreateUtxo(types.Txn, *models.Utxo) error
	GetUtxoIncludingSpent(
		[]byte,
		uint32,
		types.Txn,
	) (*models.Utxo, error)
	DeleteUtxo(models.UtxoId, types.Txn) error
	DeleteUtxos([]models.UtxoId, types.Txn) error
	DeleteUtxosAfterSlot(uint64, types.Txn) error
	MarkUtxosDeletedAtSlot(types.Txn, []types.UtxoKey, uint64) error
	SetUtxosNotDeletedAfterSlot(uint64, types.Txn) error
	ImportUtxos([]models.Utxo, types.Txn) error
	GetLiveStakeInputsForPools(
		[][]byte,
		uint64,
		types.Txn,
	) ([]*models.RewardStakeInput, error)
}

type utxoMutationState struct {
	Marked           *models.Utxo
	Restored         *models.Utxo
	DeletedOne       *models.Utxo
	DeletedBatch     *models.Utxo
	DeletedAfterSlot *models.Utxo
	LiveStake        []*models.RewardStakeInput
	Imported         *models.Utxo
}

func TestSharedSQLStoreUtxoMutationParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	_ = exerciseUtxoMutationStore(t, store)
}

func exerciseUtxoMutationStore(
	t *testing.T,
	store utxoMutationStore,
) utxoMutationState {
	t.Helper()
	stakeKey := bytes.Repeat([]byte{0x71}, 28)
	pool := bytes.Repeat([]byte{0x72}, 28)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeKey,
		Pool:       pool, Reward: 5, Active: true, AddedSlot: 1,
	}))
	hashes := [][]byte{
		bytes.Repeat([]byte{0x81}, 32),
		bytes.Repeat([]byte{0x82}, 32),
		bytes.Repeat([]byte{0x83}, 32),
		bytes.Repeat([]byte{0x84}, 32),
		bytes.Repeat([]byte{0x85}, 32),
		bytes.Repeat([]byte{0x86}, 32),
	}
	for i := range hashes {
		if i == len(hashes)-1 {
			break
		}
		require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
			TxId: hashes[i], OutputIdx: uint32(i),
			StakingKey: stakeKey, Amount: types.Uint64(10 + i),
			AddedSlot: uint64(10 + i),
		}))
	}
	imported := models.Utxo{
		TxId: hashes[5], OutputIdx: 5, StakingKey: stakeKey,
		Amount: 25, AddedSlot: 15,
		Assets: []models.Asset{{
			Name: []byte("asset"), PolicyId: []byte("policy"), Amount: 2,
		}},
	}
	require.NoError(t, store.ImportUtxos([]models.Utxo{imported}, nil))
	require.NoError(t, store.ImportUtxos([]models.Utxo{imported}, nil))
	require.NoError(t, store.MarkUtxosDeletedAtSlot(
		nil,
		[]types.UtxoKey{{TxId: hashes[0], OutputIdx: 0}},
		20,
	))
	var ret utxoMutationState
	var err error
	ret.Marked, err = store.GetUtxoIncludingSpent(hashes[0], 0, nil)
	require.NoError(t, err)
	require.NoError(t, store.SetUtxosNotDeletedAfterSlot(19, nil))
	ret.Restored, err = store.GetUtxoIncludingSpent(hashes[0], 0, nil)
	require.NoError(t, err)
	require.NoError(t, store.DeleteUtxo(
		models.UtxoId{Hash: hashes[1], Idx: 1},
		nil,
	))
	ret.DeletedOne, err = store.GetUtxoIncludingSpent(hashes[1], 1, nil)
	require.NoError(t, err)
	require.NoError(t, store.DeleteUtxos(
		[]models.UtxoId{
			{Hash: hashes[2], Idx: 2},
			{Hash: []byte("missing"), Idx: 9},
		},
		nil,
	))
	ret.DeletedBatch, err = store.GetUtxoIncludingSpent(hashes[2], 2, nil)
	require.NoError(t, err)
	require.NoError(t, store.DeleteUtxosAfterSlot(13, nil))
	ret.DeletedAfterSlot, err = store.GetUtxoIncludingSpent(
		hashes[4],
		4,
		nil,
	)
	require.NoError(t, err)
	ret.LiveStake, err = store.GetLiveStakeInputsForPools(
		[][]byte{pool},
		0,
		nil,
	)
	require.NoError(t, err)
	ret.Imported, err = store.GetUtxoIncludingSpent(hashes[5], 5, nil)
	require.NoError(t, err)
	return ret
}
