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
	"math/big"
	"net"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type poolStore interface {
	ImportPool(*models.Pool, *models.PoolRegistration, types.Txn) error
	GetPool(lcommon.PoolKeyHash, bool, types.Txn) (*models.Pool, error)
	GetPoolByVrfKeyHash([]byte, types.Txn) (*models.Pool, error)
	GetPools([]lcommon.PoolKeyHash, types.Txn) ([]models.Pool, error)
	UpdatePoolOpCertSequence(
		lcommon.PoolKeyHash,
		uint64,
		uint64,
		types.Txn,
	) error
	LatestPoolOpCertSequence(
		lcommon.PoolKeyHash,
		types.Txn,
	) (uint64, bool, error)
	LatestPoolOpCertSequenceAtOrBefore(
		lcommon.PoolKeyHash,
		uint64,
		types.Txn,
	) (uint64, bool, error)
	GetPoolBlockIssuersInSlotRange(
		uint64,
		uint64,
		types.Txn,
	) ([]models.PoolOpCertSequence, error)
	CountPoolBlocksInSlotRange(
		[]lcommon.PoolKeyHash,
		uint64,
		uint64,
		types.Txn,
	) (map[string]uint64, uint64, error)
	SetEpoch(
		uint64,
		uint64,
		[]byte,
		[]byte,
		[]byte,
		[]byte,
		uint,
		uint,
		uint,
		types.Txn,
	) error
	SetTip(ochainsync.Tip, types.Txn) error
	GetActivePoolKeyHashes(types.Txn) ([][]byte, error)
	GetActivePoolKeyHashesAtSlot(uint64, types.Txn) ([][]byte, error)
	RetirePools(types.Txn, [][]byte, uint64, uint64) error
	GetRetiringPools(uint64, types.Txn) ([]models.PoolRetiringRow, error)
	CreateAccount(types.Txn, *models.Account) error
	CreateUtxo(types.Txn, *models.Utxo) error
	GetStakeByPool([]byte, types.Txn) (uint64, uint64, error)
	GetStakeByPools(
		[][]byte,
		types.Txn,
	) (map[string]uint64, map[string]uint64, error)
}

type poolState struct {
	Pool                  *models.Pool
	ByVRF                 *models.Pool
	Pools                 []models.Pool
	Missing               *models.Pool
	Sequence              uint64
	SequenceSet           bool
	HistoricalSequence    uint64
	HistoricalSequenceSet bool
	Issuers               []models.PoolOpCertSequence
	Counts                map[string]uint64
	Total                 uint64
	Active                [][]byte
	ActiveAtSlot          [][]byte
	Retiring              []models.PoolRetiringRow
	Stake                 uint64
	Delegators            uint64
	StakeMap              map[string]uint64
	DelegatorMap          map[string]uint64
}

func TestSharedSQLStorePoolParity(t *testing.T) {
	t.Parallel()
	store, _ := newSharedSQLStore(t)
	_ = exercisePoolStore(t, store)
}

func exercisePoolStore(t *testing.T, store poolStore) poolState {
	t.Helper()
	poolBytes := bytes.Repeat([]byte{0x91}, 28)
	poolKeyHash := lcommon.PoolKeyHash(
		lcommon.NewBlake2b224(poolBytes),
	)
	vrf := bytes.Repeat([]byte{0x92}, 32)
	ipv4 := net.IPv4(127, 0, 0, 1)
	pool := &models.Pool{
		PoolKeyHash: poolBytes, VrfKeyHash: vrf,
		RewardAccount:              bytes.Repeat([]byte{0x93}, 28),
		RewardAccountCredentialTag: 1,
		Pledge:                     1000, Cost: 50,
		Margin: &types.Rat{Rat: big.NewRat(1, 10)},
	}
	registration := &models.PoolRegistration{
		PoolKeyHash: poolBytes, VrfKeyHash: vrf,
		RewardAccount:              pool.RewardAccount,
		RewardAccountCredentialTag: 1,
		Pledge:                     1000, Cost: 50, AddedSlot: 10,
		DepositAmount: 500,
		Margin:        &types.Rat{Rat: big.NewRat(1, 10)},
		MetadataUrl:   "https://pool.example",
		MetadataHash:  []byte("metadata"),
		Owners: []models.PoolRegistrationOwner{{
			KeyHash: bytes.Repeat([]byte{0x94}, 28),
		}},
		Relays: []models.PoolRegistrationRelay{{
			Ipv4: &ipv4, Port: 3001,
		}},
	}
	require.NoError(t, store.ImportPool(pool, registration, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(
		poolKeyHash,
		2,
		20,
		nil,
	))
	require.NoError(t, store.UpdatePoolOpCertSequence(
		poolKeyHash,
		3,
		21,
		nil,
	))
	require.NoError(t, store.SetEpoch(
		2, 0, nil, nil, nil, nil, 0, 1, 100, nil,
	))
	require.NoError(t, store.SetTip(ochainsync.Tip{
		Point:       ocommon.Point{Slot: 25, Hash: []byte("tip")},
		BlockNumber: 1,
	}, nil))
	stakeKey := bytes.Repeat([]byte{0x95}, 28)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey: stakeKey, Pool: poolBytes, Active: true,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:       bytes.Repeat([]byte{0x96}, 32),
		StakingKey: stakeKey, Amount: 700, AddedSlot: 15,
	}))
	var ret poolState
	var err error
	ret.Pool, err = store.GetPool(poolKeyHash, true, nil)
	require.NoError(t, err)
	ret.ByVRF, err = store.GetPoolByVrfKeyHash(vrf, nil)
	require.NoError(t, err)
	ret.Pools, err = store.GetPools([]lcommon.PoolKeyHash{poolKeyHash}, nil)
	require.NoError(t, err)
	ret.Missing, err = store.GetPool(
		lcommon.PoolKeyHash(
			lcommon.NewBlake2b224(bytes.Repeat([]byte{0xff}, 28)),
		),
		true,
		nil,
	)
	require.NoError(t, err)
	ret.Sequence, ret.SequenceSet, err = store.LatestPoolOpCertSequence(
		poolKeyHash,
		nil,
	)
	require.NoError(t, err)
	ret.HistoricalSequence, ret.HistoricalSequenceSet, err =
		store.LatestPoolOpCertSequenceAtOrBefore(poolKeyHash, 20, nil)
	require.NoError(t, err)
	ret.Issuers, err = store.GetPoolBlockIssuersInSlotRange(20, 21, nil)
	require.NoError(t, err)
	ret.Counts, ret.Total, err = store.CountPoolBlocksInSlotRange(
		[]lcommon.PoolKeyHash{poolKeyHash},
		20,
		21,
		nil,
	)
	require.NoError(t, err)
	ret.Active, err = store.GetActivePoolKeyHashes(nil)
	require.NoError(t, err)
	ret.ActiveAtSlot, err = store.GetActivePoolKeyHashesAtSlot(25, nil)
	require.NoError(t, err)
	ret.Stake, ret.Delegators, err = store.GetStakeByPool(poolBytes, nil)
	require.NoError(t, err)
	ret.StakeMap, ret.DelegatorMap, err = store.GetStakeByPools(
		[][]byte{poolBytes},
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, store.RetirePools(
		nil,
		[][]byte{poolBytes},
		5,
		30,
	))
	ret.Retiring, err = store.GetRetiringPools(2, nil)
	require.NoError(t, err)
	return ret
}
