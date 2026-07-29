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

package blockfrost

import (
	"encoding/hex"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodeAdapterPoolDetailFullResponse seeds a pool with a real
// transaction-backed registration, an owner delegating its own stake back
// to the pool, a separate delegator, and observed block production across
// two epochs, then verifies every PoolDetailInfo field the adapter
// computes from real DB state.
func TestNodeAdapterPoolDetailFullResponse(t *testing.T) {
	adapter, store, db := newDBBackedAdapter(t)

	poolKeyHash := fill32(0x11)[:28]
	vrfKeyHash := fill32(0x22)
	rewardAccountHash := fill32(0x33)[:28]
	ownerKeyHash := fill32(0x44)[:28]
	delegatorStakingKey := fill32(0x55)[:28]

	pool := &models.Pool{
		PoolKeyHash:   poolKeyHash,
		VrfKeyHash:    vrfKeyHash,
		RewardAccount: rewardAccountHash,
		Pledge:        types.Uint64(5_000_000_000),
		Cost:          types.Uint64(340_000_000),
		Margin:        &types.Rat{Rat: big.NewRat(5, 100)},
		Registration: []models.PoolRegistration{
			{
				PoolKeyHash:   poolKeyHash,
				VrfKeyHash:    vrfKeyHash,
				RewardAccount: rewardAccountHash,
				Pledge:        types.Uint64(5_000_000_000),
				Cost:          types.Uint64(340_000_000),
				Margin:        &types.Rat{Rat: big.NewRat(5, 100)},
				AddedSlot:     1,
				CertificateID: 1,
			},
		},
	}
	require.NoError(t, store.DB().Create(pool).Error)
	// PoolRegistrationOwner.PoolID is a denormalized convenience column,
	// not the association GORM tracks (that's PoolRegistrationID), so it
	// is not auto-populated by a nested create and must be set explicitly
	// once the parent IDs exist.
	require.NoError(t, store.DB().Create(&models.PoolRegistrationOwner{
		KeyHash:            ownerKeyHash,
		PoolRegistrationID: pool.Registration[0].ID,
		PoolID:             pool.ID,
	}).Error)

	// Registration certificate history: a real transaction backing the
	// registration row's certificate_id = 1.
	regTxHash := fill32(0x66)
	require.NoError(t, store.DB().Create(&models.Transaction{
		ID: 1, Hash: regTxHash, Slot: 1, BlockIndex: 0,
	}).Error)
	require.NoError(t, store.DB().Exec(
		"INSERT INTO certs (id, transaction_id, cert_index, cert_type, slot) VALUES (1, 1, 0, 0, 1)",
	).Error)

	// A plain delegator (not an owner) with a live UTxO delegated to the
	// pool.
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: delegatorStakingKey,
		Pool:       poolKeyHash,
		Active:     true,
		AddedSlot:  1,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       fill32(0x77),
		OutputIdx:  0,
		StakingKey: delegatorStakingKey,
		Amount:     types.Uint64(6_900_000_000),
		AddedSlot:  1,
	}).Error)

	// The pool's own owner, also delegating its stake to the pool: this is
	// what live_pledge reports.
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: ownerKeyHash,
		Pool:       poolKeyHash,
		Active:     true,
		AddedSlot:  1,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       fill32(0x88),
		OutputIdx:  0,
		StakingKey: ownerKeyHash,
		Amount:     types.Uint64(5_000_000_001),
		AddedSlot:  1,
	}).Error)

	// Two blocks produced: one before the current epoch's start slot, one
	// within it, so blocks_minted (lifetime) and blocks_epoch (current
	// epoch only) diverge.
	pkh := lcommon.PoolKeyHash(poolKeyHash)
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 1, 10, nil))
	require.NoError(t, db.UpdatePoolOpCertSequence(pkh, 2, 500, nil))
	// ls.CurrentEpoch() is 0 in this lightweight harness (no ls.Start()),
	// so the adapter resolves "current epoch" to epoch_id 0 regardless of
	// slot; giving it a start_slot of 100 excludes the block at slot 10
	// from blocks_epoch while still counting it in the lifetime total.
	require.NoError(t, store.DB().Create(&models.Epoch{
		EpochId:       0,
		StartSlot:     100,
		LengthInSlots: 1000,
	}).Error)

	poolID := hex.EncodeToString(poolKeyHash)
	info, err := adapter.PoolDetail(poolID)
	require.NoError(t, err)

	assert.Equal(t, poolID, info.Hex)
	assert.Equal(t,
		lcommon.PoolId(lcommon.NewBlake2b224(poolKeyHash)).String(),
		info.PoolID,
	)
	assert.Equal(t, hex.EncodeToString(vrfKeyHash), info.VrfKey)
	assert.Equal(t, uint64(2), info.BlocksMinted)
	assert.Equal(t, uint64(1), info.BlocksEpoch)
	assert.Equal(t, "11900000001", info.LiveStake)
	assert.Equal(t, uint64(2), info.LiveDelegators)
	assert.Equal(t, "5000000000", info.DeclaredPledge)
	assert.Equal(t, "5000000001", info.LivePledge)
	assert.InDelta(t, 0.05, info.MarginCost, 0.0001)
	assert.Equal(t, "340000000", info.FixedCost)
	require.Len(t, info.Owners, 1)
	require.Len(t, info.Registration, 1)
	assert.Equal(t, hex.EncodeToString(regTxHash), info.Registration[0])
	assert.Empty(t, info.Retirement)
	assert.NotNil(t, info.Retirement)
	assert.Nil(t, info.CalidusKey)
	// Protocol parameters are never loaded in this lightweight harness
	// (ls.Start() is not called), so nOpt is unavailable and saturation
	// degrades to zero rather than the request failing outright.
	assert.Zero(t, info.LiveSaturation)
	// Active stake/size are likewise zero: no pool_stake_snapshot row was
	// seeded for this test, which is a legitimate "not captured yet" state
	// distinct from a query failure.
	assert.Equal(t, "0", info.ActiveStake)
}

// TestNodeAdapterPoolDetailBech32AndHexSameResult is the ID-format
// acceptance criterion: bech32 and hex forms of the same pool key hash must
// resolve to an identical PoolDetailInfo.
func TestNodeAdapterPoolDetailBech32AndHexSameResult(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)

	poolKeyHash := fill32(0x99)[:28]
	pool := &models.Pool{
		PoolKeyHash: poolKeyHash,
		VrfKeyHash:  fill32(0xaa),
		Registration: []models.PoolRegistration{
			{PoolKeyHash: poolKeyHash, AddedSlot: 1},
		},
	}
	require.NoError(t, store.DB().Create(pool).Error)

	hexID := hex.EncodeToString(poolKeyHash)
	bech32ID := lcommon.PoolId(lcommon.NewBlake2b224(poolKeyHash)).String()
	require.NotEqual(t, hexID, bech32ID)

	byHex, err := adapter.PoolDetail(hexID)
	require.NoError(t, err)
	byBech32, err := adapter.PoolDetail(bech32ID)
	require.NoError(t, err)

	assert.Equal(t, byHex, byBech32)
	assert.Equal(t, bech32ID, byHex.PoolID)
	assert.Equal(t, hexID, byHex.Hex)
}

// TestNodeAdapterPoolDetailInvalidID covers a malformed pool ID (neither
// valid bech32 nor 56-character hex).
func TestNodeAdapterPoolDetailInvalidID(t *testing.T) {
	adapter, _, _ := newDBBackedAdapter(t)

	_, err := adapter.PoolDetail("not-a-pool-id")
	require.ErrorIs(t, err, ErrInvalidPoolID)
}

// TestNodeAdapterPoolDetailNotFound covers a well-formed pool ID with no
// matching pool row.
func TestNodeAdapterPoolDetailNotFound(t *testing.T) {
	adapter, _, _ := newDBBackedAdapter(t)

	missing := hex.EncodeToString(fill32(0xfe)[:28])
	_, err := adapter.PoolDetail(missing)
	require.ErrorIs(t, err, models.ErrPoolNotFound)
}

// TestNodeAdapterPoolDetailDatabaseFailure guards against a backing-store
// failure being silently swallowed into an incomplete success response: a
// broken stake query must surface as an error.
func TestNodeAdapterPoolDetailDatabaseFailure(t *testing.T) {
	adapter, store, _ := newDBBackedAdapter(t)

	poolKeyHash := fill32(0xcc)[:28]
	pool := &models.Pool{
		PoolKeyHash: poolKeyHash,
		VrfKeyHash:  fill32(0xdd),
		Registration: []models.PoolRegistration{
			{PoolKeyHash: poolKeyHash, AddedSlot: 1},
		},
	}
	require.NoError(t, store.DB().Create(pool).Error)

	// Break the store so GetStakeByPools fails; the failure must surface
	// instead of a partial/zeroed response.
	require.NoError(t, store.DB().Exec("DROP TABLE account").Error)

	_, err := adapter.PoolDetail(hex.EncodeToString(poolKeyHash))
	require.ErrorContains(t, err, "get live stake")
}
