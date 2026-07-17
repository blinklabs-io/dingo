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

package ledger

import (
	"encoding/binary"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/rewards"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestApplyStakeRewardsUsesDelayedRewardState(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		performanceEpoch    = uint64(2)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(400)
	)
	poolKey := rewardCalcHash(0x11)
	rewardAccount := rewardCalcHash(0x22)
	member := rewardCalcHash(0x33)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)

	require.NoError(t, meta.SetEpoch(0, 1, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil))
	require.NoError(t, meta.SetEpoch(100, performanceEpoch, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil))
	require.NoError(t, meta.SetEpoch(200, potsEpoch, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil))
	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			poolID,
			i+1,
			140+i,
			nil,
		))
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      rewardSnapshotEpoch,
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:                     500,
			Cost:                       1_000,
			DelegatedStake:             1_000,
			OwnerStake:                 500,
			DelegatorCount:             2,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    rewardAccount,
			Stake:         500,
			Owner:         true,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    member,
			Stake:         500,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	gormDB := rewardCalcGormDB(t, db)
	pool := models.Pool{PoolKeyHash: poolKey}
	require.NoError(t, gormDB.Create(&pool).Error)
	require.NoError(t, gormDB.Create(&models.PoolRegistration{
		PoolID:      pool.ID,
		PoolKeyHash: poolKey,
		AddedSlot:   0,
	}).Error)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: rewardAccount,
		Pool:       poolKey,
		Active:     true,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: member,
		Pool:       poolKey,
		Active:     true,
	}))
	rewardCalcSeedStakeCert(
		t,
		db,
		1,
		rewardAccount,
		0,
		250,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		2,
		member,
		0,
		250,
		uint(lcommon.CertificateTypeStakeRegistration),
	)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, newEpoch, boundarySlot)
	}))

	rewardOwner, err := db.GetAccountByCredential(0, rewardAccount, false, nil)
	require.NoError(t, err)
	require.NotNil(t, rewardOwner)
	require.Equal(t, uint64(46_283), uint64(rewardOwner.Reward))

	rewardMember, err := db.GetAccountByCredential(0, member, false, nil)
	require.NoError(t, err)
	require.NotNil(t, rewardMember)
	require.Equal(t, uint64(37_049), uint64(rewardMember.Reward))

	liveInputs, err := meta.GetRewardStakeInputsForPools(
		[][]byte{poolKey},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, liveInputs, 2)
	liveStakeByKey := make(map[string]uint64, len(liveInputs))
	for _, input := range liveInputs {
		liveStakeByKey[string(input.StakingKey)] = uint64(input.Stake)
	}
	require.Equal(t, uint64(46_283), liveStakeByKey[string(rewardAccount)])
	require.Equal(t, uint64(37_049), liveStakeByKey[string(member)])

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(99_916_668), uint64(state.Reserves))
	require.Equal(t, uint64(0), uint64(state.Treasury))

	pots, err := meta.GetRewardAdaPots(potsEpoch, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	require.Equal(t, uint64(1_000_000), uint64(pots.Rewards))

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(t, uint64(83_333), uint64(poolOutputs[0].TotalReward))

	accountOutputs, err := meta.GetRewardAccountOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 2)

	var deltas int64
	require.NoError(t, rewardCalcGormDB(t, db).
		Model(&models.AccountRewardDelta{}).
		Where("added_slot = ?", boundarySlot).
		Count(&deltas).Error)
	require.Equal(t, int64(2), deltas)

	txn = db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, newEpoch, boundarySlot)
	}))

	rewardOwner, err = db.GetAccountByCredential(0, rewardAccount, false, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(46_283), uint64(rewardOwner.Reward))
	rewardMember, err = db.GetAccountByCredential(0, member, false, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(37_049), uint64(rewardMember.Reward))
	liveInputs, err = meta.GetRewardStakeInputsForPools(
		[][]byte{poolKey},
		nil,
	)
	require.NoError(t, err)
	require.Len(t, liveInputs, 2)
	liveStakeByKey = make(map[string]uint64, len(liveInputs))
	for _, input := range liveInputs {
		liveStakeByKey[string(input.StakingKey)] = uint64(input.Stake)
	}
	require.Equal(t, uint64(46_283), liveStakeByKey[string(rewardAccount)])
	require.Equal(t, uint64(37_049), liveStakeByKey[string(member)])
	require.NoError(t, rewardCalcGormDB(t, db).
		Model(&models.AccountRewardDelta{}).
		Where("added_slot = ?", boundarySlot).
		Count(&deltas).Error)
	require.Equal(t, int64(2), deltas)
}

func TestApplyStakeRewardsAggregatesSharedRewardAccountBalance(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		performanceEpoch    = uint64(2)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(400)
	)
	poolA := rewardCalcHash(0x14)
	poolB := rewardCalcHash(0x15)
	sharedRewardAccount := rewardCalcHash(0x16)
	memberA := rewardCalcHash(0x17)
	memberB := rewardCalcHash(0x18)
	var poolIDA lcommon.PoolKeyHash
	var poolIDB lcommon.PoolKeyHash
	copy(poolIDA[:], poolA)
	copy(poolIDB[:], poolB)

	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)

	require.NoError(t, meta.SetEpoch(100, performanceEpoch, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil))
	require.NoError(t, meta.SetEpoch(200, potsEpoch, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil))
	for i := range uint64(5) {
		require.NoError(t, db.UpdatePoolOpCertSequence(poolIDA, i+1, 140+i, nil))
		require.NoError(t, db.UpdatePoolOpCertSequence(poolIDB, i+1, 150+i, nil))
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 2_000,
		TotalPoolCount:   2,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      rewardSnapshotEpoch,
			PoolKeyHash:                poolA,
			RewardAccount:              sharedRewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Cost:                       1_000,
			DelegatedStake:             1_000,
			DelegatorCount:             1,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
		{
			Epoch:                      rewardSnapshotEpoch,
			PoolKeyHash:                poolB,
			RewardAccount:              sharedRewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Cost:                       1_000,
			DelegatedStake:             1_000,
			DelegatorCount:             1,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:        rewardSnapshotEpoch,
			PoolKeyHash:  poolA,
			StakingKey:   memberA,
			Stake:        1_000,
			Registered:   true,
			CapturedSlot: 100,
			BoundarySlot: 100,
		},
		{
			Epoch:        rewardSnapshotEpoch,
			PoolKeyHash:  poolB,
			StakingKey:   memberB,
			Stake:        1_000,
			Registered:   true,
			CapturedSlot: 100,
			BoundarySlot: 100,
		},
	}, nil))
	for _, stakingKey := range [][]byte{sharedRewardAccount, memberA, memberB} {
		require.NoError(t, db.CreateAccount(nil, &models.Account{
			StakingKey: stakingKey,
			Active:     true,
		}))
	}

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, newEpoch, boundarySlot)
	}))

	accountOutputs, err := meta.GetRewardAccountOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	var sharedLeaderOutputs int
	var sharedLeaderTotal uint64
	for _, output := range accountOutputs {
		if string(output.StakingKey) != string(sharedRewardAccount) {
			continue
		}
		require.Equal(t, string(rewards.RewardTypeLeader), output.RewardType)
		sharedLeaderOutputs++
		sharedLeaderTotal += uint64(output.Amount)
	}
	require.Equal(t, 2, sharedLeaderOutputs)
	require.Greater(t, sharedLeaderTotal, uint64(0))

	account, err := db.GetAccountByCredential(0, sharedRewardAccount, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.Equal(t, sharedLeaderTotal, uint64(account.Reward))
}

func TestCalculateStakeRewardsRejectsPersistedStakeInputMismatch(t *testing.T) {
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	poolKey := rewardCalcHash(0x4a)
	rewardAccount := rewardCalcHash(0x5a)

	result := rewardCalcGormDB(t, db).
		Model(&models.RewardStakeInput{}).
		Where(
			"epoch = ? AND pool_key_hash = ? AND staking_key = ?",
			uint64(1),
			poolKey,
			rewardAccount,
		).
		Update("stake", types.Uint64(499))
	require.NoError(t, result.Error)
	require.Equal(t, int64(1), result.RowsAffected)

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reward stake input total mismatch")
	require.False(t, ok)
	require.Nil(t, app)
}

func TestCalculateStakeRewardsRejectsPersistedOwnerStakeInputMismatch(t *testing.T) {
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	poolKey := rewardCalcHash(0x4a)

	result := rewardCalcGormDB(t, db).
		Model(&models.RewardPoolInput{}).
		Where("epoch = ? AND pool_key_hash = ?", uint64(1), poolKey).
		Update("owner_stake", types.Uint64(499))
	require.NoError(t, result.Error)
	require.Equal(t, int64(1), result.RowsAffected)

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reward owner stake input total mismatch")
	require.False(t, ok)
	require.Nil(t, app)
}

func TestCalculateStakeRewardsRejectsPersistedDelegatorCountMismatch(t *testing.T) {
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	poolKey := rewardCalcHash(0x4a)

	result := rewardCalcGormDB(t, db).
		Model(&models.RewardPoolInput{}).
		Where("epoch = ? AND pool_key_hash = ?", uint64(1), poolKey).
		Update("delegator_count", uint64(1))
	require.NoError(t, result.Error)
	require.Equal(t, int64(1), result.RowsAffected)

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "total delegator count")
	require.False(t, ok)
	require.Nil(t, app)
}

func TestCalculateStakeRewardsRejectsPersistedPoolCountMismatch(t *testing.T) {
	ls, db := seedRewardPrecomputeTimingState(t, 7)

	result := rewardCalcGormDB(t, db).
		Model(&models.RewardSnapshot{}).
		Where("epoch = ? AND snapshot_type = ?", uint64(1), "mark").
		Update("total_pool_count", uint64(2))
	require.NoError(t, result.Error)
	require.Equal(t, int64(1), result.RowsAffected)

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match snapshot pool count")
	require.False(t, ok)
	require.Nil(t, app)
}

func TestCalculateStakeRewardsRejectsPersistedPoolInputSnapshotSlotMismatch(t *testing.T) {
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	poolKey := rewardCalcHash(0x4a)

	result := rewardCalcGormDB(t, db).
		Model(&models.RewardPoolInput{}).
		Where("epoch = ? AND pool_key_hash = ?", uint64(1), poolKey).
		Update("captured_slot", uint64(99))
	require.NoError(t, result.Error)
	require.Equal(t, int64(1), result.RowsAffected)

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reward pool input captured slot")
	require.False(t, ok)
	require.Nil(t, app)
}

func TestCalculateStakeRewardsRejectsPersistedStakeInputSnapshotSlotMismatch(t *testing.T) {
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	poolKey := rewardCalcHash(0x4a)
	member := rewardCalcHash(0x6a)

	result := rewardCalcGormDB(t, db).
		Model(&models.RewardStakeInput{}).
		Where(
			"epoch = ? AND pool_key_hash = ? AND staking_key = ?",
			uint64(1),
			poolKey,
			member,
		).
		Update("boundary_slot", uint64(99))
	require.NoError(t, result.Error)
	require.Equal(t, int64(1), result.RowsAffected)

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reward stake input boundary slot")
	require.False(t, ok)
	require.Nil(t, app)
}

func TestValidateRewardCalculatorInputsRejectsMalformedPoolInput(t *testing.T) {
	poolKey := rewardCalcHash(0x4a)
	rewardAccount := rewardCalcHash(0x5a)
	member := rewardCalcHash(0x6a)
	snapshot := &models.RewardSnapshot{
		TotalActiveStake: 100,
		TotalPoolCount:   1,
		TotalDelegators:  1,
		CapturedSlot:     10,
		BoundarySlot:     20,
	}
	validPoolInput := func() *models.RewardPoolInput {
		return &models.RewardPoolInput{
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			DelegatedStake:             100,
			OwnerStake:                 10,
			DelegatorCount:             1,
			CapturedSlot:               10,
			BoundarySlot:               20,
		}
	}
	stakeInputs := []*models.RewardStakeInput{
		{
			PoolKeyHash:  poolKey,
			StakingKey:   member,
			Stake:        100,
			CapturedSlot: 10,
			BoundarySlot: 20,
		},
	}

	for _, tc := range []struct {
		name      string
		mutate    func(*models.RewardPoolInput)
		wantError string
	}{
		{
			name: "missing margin",
			mutate: func(input *models.RewardPoolInput) {
				input.Margin = nil
			},
			wantError: "reward pool input margin is missing",
		},
		{
			name: "missing margin rat",
			mutate: func(input *models.RewardPoolInput) {
				input.Margin = &types.Rat{}
			},
			wantError: "reward pool input margin is missing",
		},
		{
			name: "negative margin",
			mutate: func(input *models.RewardPoolInput) {
				input.Margin = &types.Rat{Rat: big.NewRat(-1, 10)}
			},
			wantError: "reward pool input margin outside [0,1]",
		},
		{
			name: "margin above one",
			mutate: func(input *models.RewardPoolInput) {
				input.Margin = &types.Rat{Rat: big.NewRat(11, 10)}
			},
			wantError: "reward pool input margin outside [0,1]",
		},
		{
			name: "invalid reward account length",
			mutate: func(input *models.RewardPoolInput) {
				input.RewardAccount = input.RewardAccount[:27]
			},
			wantError: "invalid reward pool input reward account",
		},
		{
			name: "invalid reward account credential tag",
			mutate: func(input *models.RewardPoolInput) {
				input.RewardAccountCredentialTag = 2
			},
			wantError: "invalid reward pool input reward account",
		},
		{
			name: "owner stake above delegated",
			mutate: func(input *models.RewardPoolInput) {
				input.OwnerStake = 101
			},
			wantError: "reward pool input owner stake",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			poolInput := validPoolInput()
			tc.mutate(poolInput)
			err := validateRewardCalculatorInputs(
				snapshot,
				[]*models.RewardPoolInput{poolInput},
				stakeInputs,
			)
			require.ErrorContains(t, err, tc.wantError)
		})
	}
}

func TestCalculateStakeRewardsRejectsUnknownPersistedStakeInputPool(t *testing.T) {
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	unknownPool := rewardCalcHash(0x7a)
	stakingKey := rewardCalcHash(0x8a)

	require.NoError(t, rewardCalcGormDB(t, db).Create(&models.RewardStakeInput{
		Epoch:         1,
		PoolKeyHash:   unknownPool,
		CredentialTag: 0,
		StakingKey:    stakingKey,
		Stake:         1,
		Registered:    true,
		CapturedSlot:  100,
		BoundarySlot:  100,
	}).Error)

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.calculateStakeRewardApplication(
		txn,
		4,
		1_200,
		1_200,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reward stake input for unknown pool")
	require.False(t, ok)
	require.Nil(t, app)
}

func TestApplyStakeRewardsUsesPrecomputedOutputs(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		performanceEpoch    = uint64(2)
		potsEpoch           = uint64(3)
		precomputeSlot      = uint64(300)
		boundarySlot        = uint64(400)
	)
	poolKey := rewardCalcHash(0x17)
	rewardAccount := rewardCalcHash(0x27)
	member := rewardCalcHash(0x37)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)

	require.NoError(t, meta.SetEpoch(100, performanceEpoch, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil))
	require.NoError(t, meta.SetEpoch(200, potsEpoch, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil))
	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			poolID,
			i+1,
			140+i,
			nil,
		))
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: precomputeSlot,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      rewardSnapshotEpoch,
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:                     500,
			Cost:                       1_000,
			DelegatedStake:             1_000,
			OwnerStake:                 500,
			DelegatorCount:             2,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    rewardAccount,
			Stake:         500,
			Owner:         true,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    member,
			Stake:         500,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: rewardAccount,
		Active:     true,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: member,
		Active:     true,
	}))
	rewardCalcSeedStakeCert(
		t,
		db,
		21,
		rewardAccount,
		0,
		250,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		22,
		member,
		0,
		250,
		uint(lcommon.CertificateTypeStakeRegistration),
	)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.precomputeStakeRewards(txn, newEpoch, precomputeSlot, boundarySlot)
	}))

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(t, precomputeSlot, poolOutputs[0].CapturedSlot)
	require.Equal(t, boundarySlot, poolOutputs[0].BoundarySlot)

	accountOutputs, err := meta.GetRewardAccountOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 2)
	for _, output := range accountOutputs {
		require.Equal(t, precomputeSlot, output.CapturedSlot)
		require.Equal(t, boundarySlot, output.BoundarySlot)
	}

	rewardOwner, err := db.GetAccountByCredential(0, rewardAccount, false, nil)
	require.NoError(t, err)
	require.NotNil(t, rewardOwner)
	require.Equal(t, uint64(0), uint64(rewardOwner.Reward))

	txn = db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, newEpoch, boundarySlot)
	}))

	rewardOwner, err = db.GetAccountByCredential(0, rewardAccount, false, nil)
	require.NoError(t, err)
	require.NotNil(t, rewardOwner)
	require.Equal(t, uint64(46_283), uint64(rewardOwner.Reward))

	rewardMember, err := db.GetAccountByCredential(0, member, false, nil)
	require.NoError(t, err)
	require.NotNil(t, rewardMember)
	require.Equal(t, uint64(37_049), uint64(rewardMember.Reward))
}

func TestApplyPrecomputedStakeRewardsChecksFinalAccountRegistration(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		performanceEpoch    = uint64(2)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(400)
	)
	poolKey := rewardCalcHash(0x19)
	rewardAccount := rewardCalcHash(0x29)
	member := rewardCalcHash(0x39)

	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)

	require.NoError(t, meta.SetEpoch(100, performanceEpoch, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil))
	require.NoError(t, meta.SetEpoch(200, potsEpoch, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil))
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      rewardSnapshotEpoch,
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:                     500,
			Cost:                       1_000,
			DelegatedStake:             1_000,
			OwnerStake:                 500,
			DelegatorCount:             2,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    rewardAccount,
			Stake:         500,
			Owner:         true,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    member,
			Stake:         500,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:             rewardSnapshotEpoch,
			PoolKeyHash:       poolKey,
			OptimalReward:     83_333,
			TotalReward:       83_333,
			LeaderReward:      46_283,
			MemberRewardTotal: 37_049,
			OwnerStake:        500,
			Undistributed:     1,
			CapturedSlot:      300,
			BoundarySlot:      boundarySlot,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{
			Epoch:         rewardSnapshotEpoch,
			CredentialTag: 0,
			StakingKey:    rewardAccount,
			PoolKeyHash:   poolKey,
			RewardType:    string(rewards.RewardTypeLeader),
			Amount:        46_283,
			Spendable:     true,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
		{
			Epoch:         rewardSnapshotEpoch,
			CredentialTag: 0,
			StakingKey:    member,
			PoolKeyHash:   poolKey,
			RewardType:    string(rewards.RewardTypeMember),
			Amount:        37_049,
			Spendable:     true,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
	}, nil))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: rewardAccount,
		Active:     true,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: member,
		Active:     true,
	}))
	rewardCalcSetAccountActive(t, db, member, false)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, newEpoch, boundarySlot)
	}))

	rewardOwner, err := db.GetAccountByCredential(0, rewardAccount, false, nil)
	require.NoError(t, err)
	require.NotNil(t, rewardOwner)
	require.Equal(t, uint64(46_283), uint64(rewardOwner.Reward))

	rewardMember, err := db.GetAccountByCredential(0, member, true, nil)
	require.NoError(t, err)
	require.NotNil(t, rewardMember)
	require.Equal(t, uint64(0), uint64(rewardMember.Reward))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(99_916_668), uint64(state.Reserves))
	require.Equal(t, uint64(37_049), uint64(state.Treasury))

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(t, uint64(37_049), uint64(poolOutputs[0].Unspendable))

	accountOutputs, err := meta.GetRewardAccountOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 2)
	for _, output := range accountOutputs {
		if string(output.StakingKey) == string(member) {
			require.False(t, output.Spendable)
		}
	}
}

func TestApplyStakeRewardsDoesNotMergeCredentialTags(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		performanceEpoch    = uint64(2)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(400)
	)
	poolKey := rewardCalcHash(0x21)
	sharedStakeHash := rewardCalcHash(0x22)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)

	require.NoError(t, meta.SetEpoch(100, performanceEpoch, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil))
	require.NoError(t, meta.SetEpoch(200, potsEpoch, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil))
	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			poolID,
			i+1,
			140+i,
			nil,
		))
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 500,
		TotalPoolCount:   1,
		TotalDelegators:  1,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      rewardSnapshotEpoch,
			PoolKeyHash:                poolKey,
			RewardAccount:              sharedStakeHash,
			RewardAccountCredentialTag: 1,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:                     500,
			Cost:                       1_000,
			DelegatedStake:             500,
			OwnerStake:                 500,
			DelegatorCount:             1,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    sharedStakeHash,
			Stake:         500,
			Owner:         true,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    sharedStakeHash,
		CredentialTag: 0,
		Reward:        7,
		Active:        true,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    sharedStakeHash,
		CredentialTag: 1,
		Active:        true,
	}))
	rewardCalcSetAccountActiveByCredential(t, db, 1, sharedStakeHash, false)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, newEpoch, boundarySlot)
	}))

	keyAccount, err := db.GetAccountByCredential(0, sharedStakeHash, false, nil)
	require.NoError(t, err)
	require.NotNil(t, keyAccount)
	require.Equal(t, uint64(7), uint64(keyAccount.Reward))

	scriptAccount, err := db.GetAccountByCredential(1, sharedStakeHash, true, nil)
	require.NoError(t, err)
	require.NotNil(t, scriptAccount)
	require.False(t, scriptAccount.Active)
	require.Equal(t, uint64(0), uint64(scriptAccount.Reward))

	accountOutputs, err := meta.GetRewardAccountOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 1)
	require.Equal(t, uint8(1), accountOutputs[0].CredentialTag)
	require.Equal(t, sharedStakeHash, accountOutputs[0].StakingKey)
	require.False(t, accountOutputs[0].Spendable)
	require.Greater(t, uint64(accountOutputs[0].Amount), uint64(0))

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(t, accountOutputs[0].Amount, poolOutputs[0].Unspendable)

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, accountOutputs[0].Amount, state.Treasury)
}

func TestPrecomputedStakeRewardsFinalEligibilityDoesNotMergeCredentialTags(t *testing.T) {
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	poolKey := rewardCalcHash(0x4a)
	sharedStakeHash := rewardCalcHash(0x77)
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      100,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    sharedStakeHash,
		CredentialTag: 0,
		Reward:        7,
		Active:        true,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey:    sharedStakeHash,
		CredentialTag: 1,
		Active:        true,
	}))
	rewardCalcSetAccountActiveByCredential(t, db, 1, sharedStakeHash, false)
	// Replace the seed's multi-delegator pool with a coherent single-member pool
	// whose only non-owner delegator is the shared script credential (tag 1). Its
	// persisted member reward (100) must equal the real member-reward formula so
	// the precompute-reuse amount check accepts it: with cost 0, margin 0, and the
	// member holding the entire 1000 delegated stake, MemberReward = TotalReward =
	// 100. The account is inactive, so that reward is unspendable and flows to the
	// treasury without merging into the shared credential's active tag-0 account.
	gormDB := rewardCalcGormDB(t, db)
	require.NoError(t, gormDB.
		Where("epoch = ? AND pool_key_hash = ?", rewardSnapshotEpoch, poolKey).
		Delete(&models.RewardStakeInput{}).Error)
	require.NoError(t, gormDB.
		Where("epoch = ? AND pool_key_hash = ?", rewardSnapshotEpoch, poolKey).
		Delete(&models.RewardPoolInput{}).Error)
	require.NoError(t, gormDB.
		Model(&models.RewardSnapshot{}).
		Where(
			"epoch = ? AND snapshot_type = ?",
			rewardSnapshotEpoch,
			"mark",
		).
		Updates(map[string]any{
			"total_active_stake": types.Uint64(1_000),
			"total_delegators":   uint64(1),
		}).Error)
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      rewardSnapshotEpoch,
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardCalcHash(0x5a),
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(0, 1)},
			Cost:                       0,
			DelegatedStake:             1_000,
			OwnerStake:                 0,
			DelegatorCount:             1,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 1,
			StakingKey:    sharedStakeHash,
			Stake:         1_000,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:             rewardSnapshotEpoch,
			PoolKeyHash:       poolKey,
			TotalReward:       100,
			LeaderReward:      0,
			MemberRewardTotal: 100,
			OwnerStake:        0,
			CapturedSlot:      300,
			BoundarySlot:      boundarySlot,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{
			Epoch:         rewardSnapshotEpoch,
			CredentialTag: 1,
			StakingKey:    sharedStakeHash,
			PoolKeyHash:   poolKey,
			RewardType:    string(rewards.RewardTypeMember),
			Amount:        100,
			Spendable:     true,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
	}, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, newEpoch, boundarySlot)
	}))

	keyAccount, err := db.GetAccountByCredential(0, sharedStakeHash, false, nil)
	require.NoError(t, err)
	require.NotNil(t, keyAccount)
	require.Equal(t, uint64(7), uint64(keyAccount.Reward))

	scriptAccount, err := db.GetAccountByCredential(1, sharedStakeHash, true, nil)
	require.NoError(t, err)
	require.NotNil(t, scriptAccount)
	require.False(t, scriptAccount.Active)
	require.Equal(t, uint64(0), uint64(scriptAccount.Reward))

	accountOutputs, err := meta.GetRewardAccountOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 1)
	require.Equal(t, uint8(1), accountOutputs[0].CredentialTag)
	require.Equal(t, sharedStakeHash, accountOutputs[0].StakingKey)
	require.False(t, accountOutputs[0].Spendable)

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(t, uint64(100), uint64(poolOutputs[0].Unspendable))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(100), uint64(state.Treasury))
}

func TestPrecomputedStakeRewardsRequireCompletePoolOutputs(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(400)
	)

	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedStakeRewardsRejectOutputsWithoutStakeInputs(t *testing.T) {
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	poolKey := rewardCalcHash(0x4a)

	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			TotalReward:   10,
			Undistributed: 10,
			OwnerStake:    500,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
	}, nil))
	require.NoError(t, rewardCalcGormDB(t, db).
		Where("epoch = ?", rewardSnapshotEpoch).
		Delete(&models.RewardStakeInput{}).Error)

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedStakeRewardsRejectOutputsWithoutPoolInputs(t *testing.T) {
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	poolKey := rewardCalcHash(0x4a)

	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			TotalReward:   10,
			Undistributed: 10,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
	}, nil))
	require.NoError(t, rewardCalcGormDB(t, db).
		Where("epoch = ?", rewardSnapshotEpoch).
		Delete(&models.RewardPoolInput{}).Error)

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedStakeRewardsRejectExtraPoolOutputs(t *testing.T) {
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	poolKey := rewardCalcHash(0x4a)
	stalePoolKey := rewardCalcHash(0x4b)

	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			TotalReward:   10,
			Undistributed: 10,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   stalePoolKey,
			TotalReward:   10,
			Undistributed: 10,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
	}, nil))

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedStakeRewardsRejectPoolOutputOutsideSnapshotInputs(t *testing.T) {
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	stalePoolKey := rewardCalcHash(0x4b)

	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   stalePoolKey,
			TotalReward:   10,
			Undistributed: 10,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
	}, nil))

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedStakeRewardsRejectPoolOutputOwnerStakeMismatch(t *testing.T) {
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	poolKey := rewardCalcHash(0x4a)

	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			TotalReward:   10,
			OwnerStake:    499,
			Undistributed: 10,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
	}, nil))

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedStakeRewardsRequireCompleteAccountOutputs(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(400)
	)
	poolKey := rewardCalcHash(0x18)

	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			TotalReward:   83_333,
			Undistributed: 1,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
	}, nil))

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedStakeRewardsRejectOutputsOutsideApplicationBoundary(t *testing.T) {
	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
		capturedSlot        = uint64(300)
	)

	for _, tc := range []struct {
		name                string
		poolCapturedSlot    uint64
		poolBoundarySlot    uint64
		accountCapturedSlot uint64
		accountBoundarySlot uint64
	}{
		{
			name:                "pool boundary slot zero",
			poolCapturedSlot:    capturedSlot,
			poolBoundarySlot:    0,
			accountCapturedSlot: capturedSlot,
			accountBoundarySlot: boundarySlot,
		},
		{
			name:                "pool captured after boundary",
			poolCapturedSlot:    boundarySlot + 1,
			poolBoundarySlot:    boundarySlot,
			accountCapturedSlot: capturedSlot,
			accountBoundarySlot: boundarySlot,
		},
		{
			name:                "account boundary slot zero",
			poolCapturedSlot:    capturedSlot,
			poolBoundarySlot:    boundarySlot,
			accountCapturedSlot: capturedSlot,
			accountBoundarySlot: 0,
		},
		{
			name:                "account captured after boundary",
			poolCapturedSlot:    capturedSlot,
			poolBoundarySlot:    boundarySlot,
			accountCapturedSlot: boundarySlot + 1,
			accountBoundarySlot: boundarySlot,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ls, db := seedRewardPrecomputeTimingState(t, 7)
			meta := db.Metadata()
			poolKey := rewardCalcHash(0x4a)
			member := rewardCalcHash(0x6a)

			require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
				Epoch:        potsEpoch,
				Reserves:     100_000_000,
				Rewards:      1_000,
				CapturedSlot: capturedSlot,
			}, nil))
			require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
				{
					Epoch:             rewardSnapshotEpoch,
					PoolKeyHash:       poolKey,
					TotalReward:       100,
					MemberRewardTotal: 100,
					OwnerStake:        500,
					CapturedSlot:      tc.poolCapturedSlot,
					BoundarySlot:      tc.poolBoundarySlot,
				},
			}, nil))
			require.NoError(t, meta.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
				{
					Epoch:         rewardSnapshotEpoch,
					CredentialTag: 0,
					StakingKey:    member,
					PoolKeyHash:   poolKey,
					RewardType:    string(rewards.RewardTypeMember),
					Amount:        100,
					Spendable:     true,
					CapturedSlot:  tc.accountCapturedSlot,
					BoundarySlot:  tc.accountBoundarySlot,
				},
			}, nil))

			txn := db.Transaction(false)
			defer func() { _ = txn.Rollback() }()
			app, ok, err := ls.precomputedStakeRewardApplication(
				txn,
				newEpoch,
				boundarySlot,
			)
			require.NoError(t, err)
			require.False(t, ok)
			require.Nil(t, app)
		})
	}
}

func TestPrecomputedRewardOutputsRequirePerPoolAccountTotals(t *testing.T) {
	poolA := rewardCalcHash(0x19)
	poolB := rewardCalcHash(0x1a)
	accountA := rewardCalcHash(0x1b)
	accountB := rewardCalcHash(0x1c)

	ok, err := precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash:       poolA,
				TotalReward:       100,
				MemberRewardTotal: 90,
				Undistributed:     10,
			},
			{
				PoolKeyHash:       poolB,
				TotalReward:       50,
				MemberRewardTotal: 50,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      140,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash:   poolA,
				TotalReward:   100,
				Undistributed: 10,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      90,
			},
			{
				PoolKeyHash: poolB,
				StakingKey:  accountB,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      0,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash:       poolA,
				TotalReward:       100,
				Undistributed:     100,
				LeaderReward:      100,
				MemberRewardTotal: 0,
			},
		},
		nil,
		true,
	)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash:       poolA,
				TotalReward:       100,
				MemberRewardTotal: 90,
				Undistributed:     10,
			},
			{
				PoolKeyHash:       poolB,
				TotalReward:       50,
				MemberRewardTotal: 50,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      90,
			},
			{
				PoolKeyHash: poolB,
				StakingKey:  accountB,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      50,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestPrecomputedRewardOutputsRequirePoolBreakdownTotals(t *testing.T) {
	poolA := rewardCalcHash(0x19)
	leaderA := rewardCalcHash(0x1b)
	memberA := rewardCalcHash(0x1c)

	ok, err := precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash:       poolA,
				TotalReward:       100,
				Undistributed:     10,
				LeaderReward:      50,
				MemberRewardTotal: 40,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  leaderA,
				RewardType:  string(rewards.RewardTypeLeader),
				Amount:      40,
			},
			{
				PoolKeyHash: poolA,
				StakingKey:  memberA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      50,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash:       poolA,
				TotalReward:       100,
				Undistributed:     10,
				LeaderReward:      100,
				MemberRewardTotal: 0,
			},
		},
		nil,
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash:       poolA,
				TotalReward:       100,
				Undistributed:     10,
				LeaderReward:      50,
				MemberRewardTotal: 40,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  leaderA,
				RewardType:  string(rewards.RewardTypeLeader),
				Amount:      50,
			},
			{
				PoolKeyHash: poolA,
				StakingKey:  memberA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      40,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestPrecomputedRewardOutputsRejectMalformedRows(t *testing.T) {
	poolA := rewardCalcHash(0x19)
	accountA := rewardCalcHash(0x1b)

	ok, err := precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash: poolA[:27],
				TotalReward: 90,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      90,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash: poolA,
				TotalReward: 90,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA,
				RewardType:  "unknown",
				Amount:      90,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash: poolA,
				TotalReward: 90,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA[:27],
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      90,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash:   poolA,
				TotalReward:   90,
				Undistributed: 90,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      0,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestPrecomputedRewardOutputsRejectDuplicateAccountIdentities(t *testing.T) {
	poolA := rewardCalcHash(0x19)
	accountA := rewardCalcHash(0x1b)

	ok, err := precomputedRewardOutputsComplete(
		[]*models.RewardPoolOutput{
			{
				PoolKeyHash: poolA,
				TotalReward: 90,
			},
		},
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      40,
			},
			{
				PoolKeyHash: poolA,
				StakingKey:  accountA,
				RewardType:  string(rewards.RewardTypeMember),
				Amount:      50,
			},
		},
		false,
	)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestPrecomputedRewardAccountOutputsMatchPoolInputs(t *testing.T) {
	poolA := rewardCalcHash(0x19)
	poolB := rewardCalcHash(0x1a)
	rewardAccount := rewardCalcHash(0x1b)
	otherAccount := rewardCalcHash(0x1c)

	poolInputs := []*models.RewardPoolInput{
		{
			PoolKeyHash:                poolA,
			RewardAccountCredentialTag: 0,
			RewardAccount:              rewardAccount,
		},
	}

	// With no stake inputs there is nothing to prove pool membership, so a
	// member reward output cannot be validated and the precomputed outputs are
	// rejected (the leader output alone would match).
	require.False(t, precomputedRewardAccountOutputsMatchInputs(
		poolInputs,
		nil,
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash:   poolA,
				CredentialTag: 0,
				StakingKey:    rewardAccount,
				RewardType:    string(rewards.RewardTypeLeader),
				Amount:        10,
			},
			{
				PoolKeyHash:   poolA,
				CredentialTag: 0,
				StakingKey:    otherAccount,
				RewardType:    string(rewards.RewardTypeMember),
				Amount:        20,
			},
		},
	))

	require.False(t, precomputedRewardAccountOutputsMatchInputs(
		poolInputs,
		nil,
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash:   poolA,
				CredentialTag: 0,
				StakingKey:    otherAccount,
				RewardType:    string(rewards.RewardTypeLeader),
				Amount:        10,
			},
		},
	))

	require.False(t, precomputedRewardAccountOutputsMatchInputs(
		poolInputs,
		nil,
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash:   poolB,
				CredentialTag: 0,
				StakingKey:    otherAccount,
				RewardType:    string(rewards.RewardTypeMember),
				Amount:        20,
			},
		},
	))
}

func TestPrecomputedRewardAccountOutputsMatchStakeInputs(t *testing.T) {
	poolA := rewardCalcHash(0x19)
	rewardAccount := rewardCalcHash(0x1b)
	memberAccount := rewardCalcHash(0x1c)
	nonMemberAccount := rewardCalcHash(0x1d)
	ownerAccount := rewardCalcHash(0x1e)

	poolInputs := []*models.RewardPoolInput{
		{
			PoolKeyHash:                poolA,
			RewardAccountCredentialTag: 0,
			RewardAccount:              rewardAccount,
		},
	}
	stakeInputs := []*models.RewardStakeInput{
		{
			PoolKeyHash:   poolA,
			CredentialTag: 0,
			StakingKey:    memberAccount,
			Stake:         100,
		},
		{
			PoolKeyHash:   poolA,
			CredentialTag: 0,
			StakingKey:    ownerAccount,
			Stake:         200,
			Owner:         true,
		},
	}

	require.True(t, precomputedRewardAccountOutputsMatchInputs(
		poolInputs,
		stakeInputs,
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash:   poolA,
				CredentialTag: 0,
				StakingKey:    rewardAccount,
				RewardType:    string(rewards.RewardTypeLeader),
				Amount:        10,
			},
			{
				PoolKeyHash:   poolA,
				CredentialTag: 0,
				StakingKey:    memberAccount,
				RewardType:    string(rewards.RewardTypeMember),
				Amount:        20,
			},
		},
	))

	require.False(t, precomputedRewardAccountOutputsMatchInputs(
		poolInputs,
		stakeInputs,
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash:   poolA,
				CredentialTag: 0,
				StakingKey:    nonMemberAccount,
				RewardType:    string(rewards.RewardTypeMember),
				Amount:        20,
			},
		},
	))

	require.False(t, precomputedRewardAccountOutputsMatchInputs(
		poolInputs,
		stakeInputs,
		[]*models.RewardAccountOutput{
			{
				PoolKeyHash:   poolA,
				CredentialTag: 0,
				StakingKey:    ownerAccount,
				RewardType:    string(rewards.RewardTypeMember),
				Amount:        20,
			},
		},
	))
}

func TestPrecomputedRewardAccountAmountsMatchInputs(t *testing.T) {
	poolA := rewardCalcHash(0x40)
	rewardAccount := rewardCalcHash(0x41)
	memberA := rewardCalcHash(0x42)
	memberB := rewardCalcHash(0x43)

	const (
		poolReward   = uint64(1000)
		cost         = uint64(0)
		delegated    = uint64(300)
		stakeA       = uint64(200)
		stakeB       = uint64(100)
		leaderReward = uint64(100)
	)
	zeroMargin := new(big.Rat)
	wantA, err := rewards.MemberReward(poolReward, cost, zeroMargin, stakeA, delegated)
	require.NoError(t, err)
	wantB, err := rewards.MemberReward(poolReward, cost, zeroMargin, stakeB, delegated)
	require.NoError(t, err)
	// A non-uniform split is what makes a within-pool redistribution detectable;
	// if the shares were equal, swapping them would be invisible.
	require.NotEqual(t, wantA, wantB)

	poolInputs := []*models.RewardPoolInput{
		{
			PoolKeyHash:                poolA,
			RewardAccountCredentialTag: 0,
			RewardAccount:              rewardAccount,
			Cost:                       types.Uint64(cost),
			DelegatedStake:             types.Uint64(delegated),
		},
	}
	poolOutputs := []*models.RewardPoolOutput{
		{
			PoolKeyHash:       poolA,
			TotalReward:       types.Uint64(poolReward),
			LeaderReward:      types.Uint64(leaderReward),
			MemberRewardTotal: types.Uint64(wantA + wantB),
		},
	}
	stakeInputs := []*models.RewardStakeInput{
		{PoolKeyHash: poolA, CredentialTag: 0, StakingKey: memberA, Stake: types.Uint64(stakeA)},
		{PoolKeyHash: poolA, CredentialTag: 0, StakingKey: memberB, Stake: types.Uint64(stakeB)},
	}

	leaderOut := func(amt uint64) *models.RewardAccountOutput {
		return &models.RewardAccountOutput{
			PoolKeyHash:   poolA,
			CredentialTag: 0,
			StakingKey:    rewardAccount,
			RewardType:    string(rewards.RewardTypeLeader),
			Amount:        types.Uint64(amt),
		}
	}
	memberOut := func(key []byte, amt uint64) *models.RewardAccountOutput {
		return &models.RewardAccountOutput{
			PoolKeyHash:   poolA,
			CredentialTag: 0,
			StakingKey:    key,
			RewardType:    string(rewards.RewardTypeMember),
			Amount:        types.Uint64(amt),
		}
	}

	check := func(outs []*models.RewardAccountOutput) bool {
		return precomputedRewardAccountAmountsMatchInputs(
			poolInputs, poolOutputs, stakeInputs, outs,
		)
	}

	// The correct per-recipient split is accepted.
	require.True(t, check([]*models.RewardAccountOutput{
		leaderOut(leaderReward), memberOut(memberA, wantA), memberOut(memberB, wantB),
	}))

	// Redistribution within the pool: member A absorbs B's share and B's row is
	// dropped. Per-pool and per-type totals are unchanged, so this is exactly the
	// case the membership and completeness checks miss; the amount check rejects it.
	require.False(t, check([]*models.RewardAccountOutput{
		leaderOut(leaderReward), memberOut(memberA, wantA+wantB),
	}))

	// Both members present but with each other's amounts (aggregate identical).
	require.False(t, check([]*models.RewardAccountOutput{
		leaderOut(leaderReward), memberOut(memberA, wantB), memberOut(memberB, wantA),
	}))

	// A tampered leader amount is rejected (pinned to the pool output).
	require.False(t, check([]*models.RewardAccountOutput{
		leaderOut(leaderReward + 1), memberOut(memberA, wantA), memberOut(memberB, wantB),
	}))

	// A member output whose credential has no stake input is rejected.
	require.False(t, check([]*models.RewardAccountOutput{
		memberOut(memberA, wantA), memberOut(rewardCalcHash(0x44), 1),
	}))

	// Guard the invariant this fix relies on: the pre-existing membership check
	// accepts the redistributed set, so the amount check is the only gate that
	// catches it.
	require.True(t, precomputedRewardAccountOutputsMatchInputs(
		poolInputs, stakeInputs,
		[]*models.RewardAccountOutput{
			leaderOut(leaderReward), memberOut(memberA, wantA+wantB),
		},
	))
}

func TestSaveStakeRewardOutputsReplacesEpochRows(t *testing.T) {
	_, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()
	const rewardSnapshotEpoch = uint64(9)
	poolA := rewardCalcHash(0x41)
	poolB := rewardCalcHash(0x42)
	accountA := rewardCalcHash(0x51)
	accountB := rewardCalcHash(0x52)

	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:       rewardSnapshotEpoch,
			PoolKeyHash: poolA,
			TotalReward: 10,
		},
		{
			Epoch:       rewardSnapshotEpoch,
			PoolKeyHash: poolB,
			TotalReward: 20,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{
			Epoch:         rewardSnapshotEpoch,
			CredentialTag: 0,
			StakingKey:    accountA,
			PoolKeyHash:   poolA,
			RewardType:    string(rewards.RewardTypeMember),
			Amount:        10,
			Spendable:     true,
		},
		{
			Epoch:         rewardSnapshotEpoch,
			CredentialTag: 0,
			StakingKey:    accountB,
			PoolKeyHash:   poolB,
			RewardType:    string(rewards.RewardTypeMember),
			Amount:        20,
			Spendable:     true,
		},
	}, nil))

	txn := db.Transaction(true)
	defer func() { _ = txn.Rollback() }()
	require.NoError(t, saveStakeRewardOutputs(
		meta,
		txn.Metadata(),
		&stakeRewardApplication{
			epochs: stakeRewardEpochs{snapshot: rewardSnapshotEpoch},
			poolOutputs: []*models.RewardPoolOutput{
				{
					Epoch:       rewardSnapshotEpoch,
					PoolKeyHash: poolA,
					TotalReward: 10,
				},
			},
			accountOutputs: []*models.RewardAccountOutput{
				{
					Epoch:         rewardSnapshotEpoch,
					CredentialTag: 0,
					StakingKey:    accountA,
					PoolKeyHash:   poolA,
					RewardType:    string(rewards.RewardTypeMember),
					Amount:        10,
					Spendable:     true,
				},
			},
		},
	))
	require.NoError(t, txn.Commit())

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(t, poolA, poolOutputs[0].PoolKeyHash)

	accountOutputs, err := meta.GetRewardAccountOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 1)
	require.Equal(t, accountA, accountOutputs[0].StakingKey)
}

func TestPrecomputedStakeRewardsRejectPoolOutputsAboveAvailableRewards(t *testing.T) {
	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)

	poolKey := rewardCalcHash(0x4a)

	t.Run("exact fit", func(t *testing.T) {
		ls, db := seedRewardPrecomputeTimingState(t, 7)
		meta := db.Metadata()
		require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
			Epoch:        potsEpoch,
			Reserves:     100_000_000,
			Rewards:      1_000_000,
			CapturedSlot: 300,
		}, nil))
		require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
			{
				Epoch:         rewardSnapshotEpoch,
				PoolKeyHash:   poolKey,
				TotalReward:   1_000_000,
				OwnerStake:    500,
				Undistributed: 1_000_000,
				CapturedSlot:  300,
				BoundarySlot:  boundarySlot,
			},
		}, nil))

		txn := db.Transaction(false)
		defer func() { _ = txn.Rollback() }()
		app, ok, err := ls.precomputedStakeRewardApplication(
			txn,
			newEpoch,
			boundarySlot,
		)
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, app)
		require.Equal(t, uint64(1_000_000), app.availableRewards)
		require.Equal(t, uint64(1_000_000), app.undistributed)
	})

	t.Run("above available", func(t *testing.T) {
		ls, db := seedRewardPrecomputeTimingState(t, 7)
		meta := db.Metadata()
		require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
			Epoch:        potsEpoch,
			Reserves:     100_000_000,
			Rewards:      1_000_000,
			CapturedSlot: 300,
		}, nil))
		require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
			{
				Epoch:         rewardSnapshotEpoch,
				PoolKeyHash:   poolKey,
				TotalReward:   1_000_001,
				Undistributed: 1_000_001,
				CapturedSlot:  300,
				BoundarySlot:  boundarySlot,
			},
		}, nil))

		txn := db.Transaction(false)
		defer func() { _ = txn.Rollback() }()
		app, ok, err := ls.precomputedStakeRewardApplication(
			txn,
			newEpoch,
			boundarySlot,
		)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, app)
	})
}

func TestPrecomputedStakeRewardsRejectPoolInputsMismatchingSnapshot(t *testing.T) {
	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)

	poolKey := rewardCalcHash(0x4a)
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 999,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  7,
	}, nil))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      1_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:        rewardSnapshotEpoch,
			PoolKeyHash:  poolKey,
			CapturedSlot: 300,
			BoundarySlot: boundarySlot,
		},
	}, nil))

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedRewardPoolInputsRejectMalformedRows(t *testing.T) {
	poolKey := rewardCalcHash(0x4a)
	rewardAccount := rewardCalcHash(0x5a)
	snapshot := &models.RewardSnapshot{
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     101,
	}
	validInput := func() *models.RewardPoolInput {
		return &models.RewardPoolInput{
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			DelegatedStake:             1_000,
			OwnerStake:                 500,
			DelegatorCount:             2,
			CapturedSlot:               100,
			BoundarySlot:               101,
		}
	}

	valid, err := precomputedRewardPoolInputsMatchSnapshot(
		snapshot,
		[]*models.RewardPoolInput{validInput()},
	)
	require.NoError(t, err)
	require.True(t, valid)

	for _, tc := range []struct {
		name   string
		mutate func(*models.RewardPoolInput)
	}{
		{
			name: "invalid reward account length",
			mutate: func(input *models.RewardPoolInput) {
				input.RewardAccount = input.RewardAccount[:27]
			},
		},
		{
			name: "invalid reward account credential tag",
			mutate: func(input *models.RewardPoolInput) {
				input.RewardAccountCredentialTag = 2
			},
		},
		{
			name: "nil margin",
			mutate: func(input *models.RewardPoolInput) {
				input.Margin = nil
			},
		},
		{
			name: "nil margin rat",
			mutate: func(input *models.RewardPoolInput) {
				input.Margin = &types.Rat{}
			},
		},
		{
			name: "negative margin",
			mutate: func(input *models.RewardPoolInput) {
				input.Margin = &types.Rat{Rat: big.NewRat(-1, 10)}
			},
		},
		{
			name: "margin above one",
			mutate: func(input *models.RewardPoolInput) {
				input.Margin = &types.Rat{Rat: big.NewRat(11, 10)}
			},
		},
		{
			name: "owner stake above delegated stake",
			mutate: func(input *models.RewardPoolInput) {
				input.OwnerStake = 1_001
			},
		},
		{
			name: "delegator count mismatch",
			mutate: func(input *models.RewardPoolInput) {
				input.DelegatorCount = 1
			},
		},
		{
			name: "captured slot mismatch",
			mutate: func(input *models.RewardPoolInput) {
				input.CapturedSlot = 99
			},
		},
		{
			name: "boundary slot mismatch",
			mutate: func(input *models.RewardPoolInput) {
				input.BoundarySlot = 102
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := validInput()
			tc.mutate(input)
			ok, err := precomputedRewardPoolInputsMatchSnapshot(
				snapshot,
				[]*models.RewardPoolInput{input},
			)
			require.NoError(t, err)
			require.False(t, ok)
		})
	}

	snapshot.TotalPoolCount = 2
	ok, err := precomputedRewardPoolInputsMatchSnapshot(
		snapshot,
		[]*models.RewardPoolInput{validInput()},
	)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestPrecomputedStakeRewardsRejectImpossibleRewardPot(t *testing.T) {
	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	poolKey := rewardCalcHash(0x4a)

	for _, tc := range []struct {
		name     string
		reserves uint64
		fees     uint64
		rewards  uint64
	}{
		{
			name:     "below fees",
			reserves: 100_000_000,
			fees:     200,
			rewards:  100,
		},
		{
			name:     "incentives exceed reserves",
			reserves: 999,
			fees:     0,
			rewards:  1_000,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ls, db := seedRewardPrecomputeTimingState(t, 7)
			meta := db.Metadata()
			require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
				Epoch:        potsEpoch,
				Reserves:     types.Uint64(tc.reserves),
				Fees:         types.Uint64(tc.fees),
				Rewards:      types.Uint64(tc.rewards),
				CapturedSlot: 300,
			}, nil))
			require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
				{
					Epoch:        rewardSnapshotEpoch,
					PoolKeyHash:  poolKey,
					CapturedSlot: 300,
					BoundarySlot: boundarySlot,
				},
			}, nil))

			txn := db.Transaction(false)
			defer func() { _ = txn.Rollback() }()
			app, ok, err := ls.precomputedStakeRewardApplication(
				txn,
				newEpoch,
				boundarySlot,
			)
			require.NoError(t, err)
			require.False(t, ok)
			require.Nil(t, app)
		})
	}
}

func TestPrecomputeStakeRewardsWaitsForPreBabbagePrefilterSlot(t *testing.T) {
	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		capturedSlot        = uint64(300)
		boundarySlot        = uint64(1_200)
	)

	t.Run("pre-babbage waits", func(t *testing.T) {
		ls, db := seedRewardPrecomputeTimingState(t, 6)
		meta := db.Metadata()
		prefilterSlot, err := ls.rewardPrefilterSlot(meta, nil, potsEpoch)
		require.NoError(t, err)
		require.Greater(t, prefilterSlot, capturedSlot)

		txn := db.Transaction(true)
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			return ls.precomputeStakeRewards(
				txn,
				newEpoch,
				capturedSlot,
				boundarySlot,
			)
		}))

		poolOutputs, err := meta.GetRewardPoolOutputs(
			rewardSnapshotEpoch,
			nil,
		)
		require.NoError(t, err)
		require.Empty(t, poolOutputs)
		accountOutputs, err := meta.GetRewardAccountOutputs(
			rewardSnapshotEpoch,
			nil,
		)
		require.NoError(t, err)
		require.Empty(t, accountOutputs)
		pots, err := meta.GetRewardAdaPots(potsEpoch, nil)
		require.NoError(t, err)
		require.NotNil(t, pots)
		require.Equal(t, uint64(0), uint64(pots.Rewards))
		ls.rewardPrecomputeMu.Lock()
		retry := ls.rewardPrecomputeRetry
		ls.rewardPrecomputeMu.Unlock()
		require.NotNil(t, retry)
		require.Equal(t, prefilterSlot, retry.cutoffSlot)
		require.Equal(t, newEpoch-1, retry.epochEvent.NewEpoch)

		actualCapturedSlot := prefilterSlot + 7
		ls.maybeQueueStakeRewardPrecomputeRetry(actualCapturedSlot)
		ls.rewardPrecomputeWG.Wait()
		poolOutputs, err = meta.GetRewardPoolOutputs(
			rewardSnapshotEpoch,
			nil,
		)
		require.NoError(t, err)
		require.Len(t, poolOutputs, 1)
		require.Equal(t, actualCapturedSlot, poolOutputs[0].CapturedSlot)
	})

	t.Run("pre-babbage precomputes at first just-right slot", func(t *testing.T) {
		ls, db := seedRewardPrecomputeTimingState(t, 6)
		meta := db.Metadata()
		prefilterSlot, err := ls.rewardPrefilterSlot(meta, nil, potsEpoch)
		require.NoError(t, err)
		rewardCalcSeedStakeCert(
			t,
			db,
			21,
			rewardCalcHash(0x5a),
			0,
			prefilterSlot-1,
			uint(lcommon.CertificateTypeStakeRegistration),
		)
		rewardCalcSeedStakeCert(
			t,
			db,
			22,
			rewardCalcHash(0x6a),
			0,
			prefilterSlot-1,
			uint(lcommon.CertificateTypeStakeRegistration),
		)

		txn := db.Transaction(true)
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			return ls.precomputeStakeRewards(
				txn,
				newEpoch,
				prefilterSlot,
				boundarySlot,
			)
		}))

		poolOutputs, err := meta.GetRewardPoolOutputs(
			rewardSnapshotEpoch,
			nil,
		)
		require.NoError(t, err)
		require.Len(t, poolOutputs, 1)
		accountOutputs, err := meta.GetRewardAccountOutputs(
			rewardSnapshotEpoch,
			nil,
		)
		require.NoError(t, err)
		require.Len(t, accountOutputs, 2)
		pots, err := meta.GetRewardAdaPots(potsEpoch, nil)
		require.NoError(t, err)
		require.NotNil(t, pots)
		require.Equal(t, uint64(100_000), uint64(pots.Rewards))
	})

	t.Run("babbage precomputes immediately", func(t *testing.T) {
		ls, db := seedRewardPrecomputeTimingState(t, 7)
		meta := db.Metadata()
		prefilterSlot, err := ls.rewardPrefilterSlot(meta, nil, potsEpoch)
		require.NoError(t, err)
		require.Greater(t, prefilterSlot, capturedSlot)

		txn := db.Transaction(true)
		require.NoError(t, txn.Do(func(txn *database.Txn) error {
			return ls.precomputeStakeRewards(
				txn,
				newEpoch,
				capturedSlot,
				boundarySlot,
			)
		}))

		poolOutputs, err := meta.GetRewardPoolOutputs(
			rewardSnapshotEpoch,
			nil,
		)
		require.NoError(t, err)
		require.Len(t, poolOutputs, 1)
		accountOutputs, err := meta.GetRewardAccountOutputs(
			rewardSnapshotEpoch,
			nil,
		)
		require.NoError(t, err)
		require.Len(t, accountOutputs, 2)
		pots, err := meta.GetRewardAdaPots(potsEpoch, nil)
		require.NoError(t, err)
		require.NotNil(t, pots)
		require.Equal(t, uint64(100_000), uint64(pots.Rewards))
	})
}

func TestRewardPrecomputeEpochTransitionStoresNextBoundaryOutputs(t *testing.T) {
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	const (
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		eventBoundarySlot   = uint64(200)
		applicationBoundary = uint64(1_200)
	)

	ls.handleRewardPrecomputeEpochTransition(event.NewEvent(
		event.EpochTransitionEventType,
		event.EpochTransitionEvent{
			PreviousEpoch: 2,
			NewEpoch:      3,
			BoundarySlot:  eventBoundarySlot,
			EpochNonce:    []byte{0x01},
			SnapshotSlot:  eventBoundarySlot - 1,
		},
	))
	ls.rewardPrecomputeWG.Wait()

	poolOutputs, err := meta.GetRewardPoolOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(t, eventBoundarySlot, poolOutputs[0].CapturedSlot)
	require.Equal(t, applicationBoundary, poolOutputs[0].BoundarySlot)

	accountOutputs, err := meta.GetRewardAccountOutputs(
		rewardSnapshotEpoch,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 2)
	for _, output := range accountOutputs {
		require.Equal(t, eventBoundarySlot, output.CapturedSlot)
		require.Equal(t, applicationBoundary, output.BoundarySlot)
	}

	pots, err := meta.GetRewardAdaPots(potsEpoch, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	require.Equal(t, uint64(100_000), uint64(pots.Rewards))
}

func TestPrecomputedStakeRewardsRejectEarlyPreBabbageOutputs(t *testing.T) {
	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		capturedSlot        = uint64(300)
		boundarySlot        = uint64(1_200)
	)

	seedOutputs := func(t *testing.T, db *database.Database) {
		t.Helper()
		meta := db.Metadata()
		poolKey := rewardCalcHash(0x4a)
		rewardAccount := rewardCalcHash(0x5a)
		member := rewardCalcHash(0x6a)

		require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
			Epoch:        potsEpoch,
			Reserves:     100_000_000,
			Rewards:      1_000_000,
			CapturedSlot: 200,
		}, nil))
		require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
			{
				Epoch:             rewardSnapshotEpoch,
				PoolKeyHash:       poolKey,
				TotalReward:       83_333,
				LeaderReward:      46_283,
				MemberRewardTotal: 37_049,
				OwnerStake:        500,
				Undistributed:     1,
				CapturedSlot:      capturedSlot,
				BoundarySlot:      boundarySlot,
			},
		}, nil))
		require.NoError(t, meta.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
			{
				Epoch:         rewardSnapshotEpoch,
				CredentialTag: 0,
				StakingKey:    rewardAccount,
				PoolKeyHash:   poolKey,
				RewardType:    string(rewards.RewardTypeLeader),
				Amount:        46_283,
				Spendable:     true,
				CapturedSlot:  capturedSlot,
				BoundarySlot:  boundarySlot,
			},
			{
				Epoch:         rewardSnapshotEpoch,
				CredentialTag: 0,
				StakingKey:    member,
				PoolKeyHash:   poolKey,
				RewardType:    string(rewards.RewardTypeMember),
				Amount:        37_049,
				Spendable:     true,
				CapturedSlot:  capturedSlot,
				BoundarySlot:  boundarySlot,
			},
		}, nil))
	}

	t.Run("pre-babbage rejects", func(t *testing.T) {
		ls, db := seedRewardPrecomputeTimingState(t, 6)
		prefilterSlot, err := ls.rewardPrefilterSlot(
			db.Metadata(),
			nil,
			potsEpoch,
		)
		require.NoError(t, err)
		require.Greater(t, prefilterSlot, capturedSlot)
		seedOutputs(t, db)

		txn := db.Transaction(false)
		defer func() { _ = txn.Rollback() }()
		app, ok, err := ls.precomputedStakeRewardApplication(
			txn,
			newEpoch,
			boundarySlot,
		)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, app)
	})

	t.Run("babbage accepts", func(t *testing.T) {
		ls, db := seedRewardPrecomputeTimingState(t, 7)
		prefilterSlot, err := ls.rewardPrefilterSlot(
			db.Metadata(),
			nil,
			potsEpoch,
		)
		require.NoError(t, err)
		require.Greater(t, prefilterSlot, capturedSlot)
		seedOutputs(t, db)

		txn := db.Transaction(false)
		defer func() { _ = txn.Rollback() }()
		app, ok, err := ls.precomputedStakeRewardApplication(
			txn,
			newEpoch,
			boundarySlot,
		)
		require.NoError(t, err)
		require.True(t, ok)
		require.NotNil(t, app)
		require.True(t, app.precomputed)
	})
}

func TestPrecomputedStakeRewardsRejectMissingBabbageLeaderOutput(t *testing.T) {
	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	poolKey := rewardCalcHash(0x4a)
	member := rewardCalcHash(0x6a)

	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		Rewards:      100_000,
		CapturedSlot: 200,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:             rewardSnapshotEpoch,
			PoolKeyHash:       poolKey,
			TotalReward:       83_333,
			LeaderReward:      46_283,
			MemberRewardTotal: 37_049,
			OwnerStake:        500,
			Undistributed:     46_284,
			CapturedSlot:      300,
			BoundarySlot:      boundarySlot,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{
			Epoch:         rewardSnapshotEpoch,
			CredentialTag: 0,
			StakingKey:    member,
			PoolKeyHash:   poolKey,
			RewardType:    string(rewards.RewardTypeMember),
			Amount:        37_049,
			Spendable:     true,
			CapturedSlot:  300,
			BoundarySlot:  boundarySlot,
		},
	}, nil))

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	app, ok, err := ls.precomputedStakeRewardApplication(
		txn,
		newEpoch,
		boundarySlot,
	)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, app)
}

func TestPrecomputedStakeRewardsCheckPreBabbageMissingLeaderPrefilter(t *testing.T) {
	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)

	for _, tc := range []struct {
		name                    string
		registerRewardAtRUPD    bool
		expectPrecomputedReward bool
	}{
		{
			name:                    "inactive at RUPD accepts missing leader",
			expectPrecomputedReward: true,
		},
		{
			name:                 "registered at RUPD rejects missing leader",
			registerRewardAtRUPD: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ls, db := seedRewardPrecomputeTimingState(t, 6)
			meta := db.Metadata()
			poolKey := rewardCalcHash(0x4a)
			rewardAccount := rewardCalcHash(0x5a)
			member := rewardCalcHash(0x6a)

			prefilterSlot, err := ls.rewardPrefilterSlot(
				meta,
				nil,
				potsEpoch,
			)
			require.NoError(t, err)
			rewardCalcSetAccountActive(t, db, rewardAccount, false)
			if tc.registerRewardAtRUPD {
				rewardCalcSeedStakeCert(
					t,
					db,
					41,
					rewardAccount,
					0,
					prefilterSlot-1,
					uint(lcommon.CertificateTypeStakeRegistration),
				)
			}

			require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
				Epoch:        potsEpoch,
				Reserves:     100_000_000,
				Rewards:      100_000,
				CapturedSlot: 200,
			}, nil))
			require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
				{
					Epoch:             rewardSnapshotEpoch,
					PoolKeyHash:       poolKey,
					TotalReward:       83_333,
					LeaderReward:      46_283,
					MemberRewardTotal: 37_049,
					OwnerStake:        500,
					Undistributed:     46_284,
					CapturedSlot:      prefilterSlot,
					BoundarySlot:      boundarySlot,
				},
			}, nil))
			require.NoError(t, meta.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
				{
					Epoch:         rewardSnapshotEpoch,
					CredentialTag: 0,
					StakingKey:    member,
					PoolKeyHash:   poolKey,
					RewardType:    string(rewards.RewardTypeMember),
					Amount:        37_049,
					Spendable:     true,
					CapturedSlot:  prefilterSlot,
					BoundarySlot:  boundarySlot,
				},
			}, nil))

			txn := db.Transaction(false)
			defer func() { _ = txn.Rollback() }()
			app, ok, err := ls.precomputedStakeRewardApplication(
				txn,
				newEpoch,
				boundarySlot,
			)
			require.NoError(t, err)
			require.Equal(t, tc.expectPrecomputedReward, ok)
			if tc.expectPrecomputedReward {
				require.NotNil(t, app)
				require.True(t, app.precomputed)
			} else {
				require.Nil(t, app)
			}
		})
	}
}

func TestApplyStakeRewardsUsesRewardUpdatePrefilterAccountHistory(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		performanceEpoch    = uint64(2)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(400)
	)
	poolKey := rewardCalcHash(0x44)
	rewardAccount := rewardCalcHash(0x55)
	member := rewardCalcHash(0x66)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    6,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)

	require.NoError(t, meta.SetEpoch(100, performanceEpoch, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil))
	require.NoError(t, meta.SetEpoch(200, potsEpoch, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil))
	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			poolID,
			i+1,
			140+i,
			nil,
		))
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  6,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      rewardSnapshotEpoch,
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:                     500,
			Cost:                       1_000,
			DelegatedStake:             1_000,
			OwnerStake:                 500,
			DelegatorCount:             2,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    rewardAccount,
			Stake:         500,
			Owner:         true,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    member,
			Stake:         500,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: rewardAccount,
		Active:     true,
	}))
	rewardCalcSetAccountActive(t, db, rewardAccount, false)

	rewardCalcSeedStakeCert(
		t,
		db,
		11,
		rewardAccount,
		0,
		250,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		12,
		rewardAccount,
		0,
		350,
		uint(lcommon.CertificateTypeStakeDeregistration),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		13,
		member,
		0,
		150,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		14,
		member,
		0,
		250,
		uint(lcommon.CertificateTypeStakeDeregistration),
	)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, newEpoch, boundarySlot)
	}))

	accountOutputs, err := meta.GetRewardAccountOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 1)
	require.Equal(t, rewardAccount, accountOutputs[0].StakingKey)
	require.False(t, accountOutputs[0].Spendable)
	require.Greater(t, uint64(accountOutputs[0].Amount), uint64(0))

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(t, uint64(accountOutputs[0].Amount), uint64(poolOutputs[0].Unspendable))
	require.Greater(t, uint64(poolOutputs[0].Undistributed), uint64(0))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(accountOutputs[0].Amount), uint64(state.Treasury))

	rewardOwner, err := db.GetAccountByCredential(0, rewardAccount, true, nil)
	require.NoError(t, err)
	require.NotNil(t, rewardOwner)
	require.Equal(t, uint64(0), uint64(rewardOwner.Reward))
}

func TestApplyStakeRewardsPrefilterUsesBeginningOfRUPDSlot(t *testing.T) {
	ls, db := seedRewardPrecomputeTimingState(t, 6)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(1_200)
	)
	rewardAccount := rewardCalcHash(0x5a)
	member := rewardCalcHash(0x6a)
	prefilterSlot, err := ls.rewardPrefilterSlot(meta, nil, potsEpoch)
	require.NoError(t, err)

	rewardCalcSetAccountActive(t, db, rewardAccount, false)
	rewardCalcSetAccountActive(t, db, member, false)
	rewardCalcSeedStakeCert(
		t,
		db,
		31,
		rewardAccount,
		0,
		prefilterSlot-1,
		uint(lcommon.CertificateTypeStakeRegistration),
	)
	rewardCalcSeedStakeCert(
		t,
		db,
		32,
		member,
		0,
		prefilterSlot,
		uint(lcommon.CertificateTypeStakeRegistration),
	)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, newEpoch, boundarySlot)
	}))

	accountOutputs, err := meta.GetRewardAccountOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 1)
	require.Equal(t, rewardAccount, accountOutputs[0].StakingKey)
	require.False(t, accountOutputs[0].Spendable)

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	require.Equal(t, accountOutputs[0].Amount, poolOutputs[0].LeaderReward)
	require.Equal(
		t,
		uint64(poolOutputs[0].TotalReward-poolOutputs[0].LeaderReward),
		uint64(poolOutputs[0].Undistributed),
	)
	require.Equal(t, accountOutputs[0].Amount, poolOutputs[0].Unspendable)

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(accountOutputs[0].Amount), uint64(state.Treasury))
}

func TestApplyStakeRewardsAccountsEmptySnapshotPots(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		newEpoch            = uint64(4)
		rewardSnapshotEpoch = uint64(1)
		performanceEpoch    = uint64(2)
		potsEpoch           = uint64(3)
		boundarySlot        = uint64(400)
	)

	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(1, 5),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)

	require.NoError(t, meta.SetEpoch(100, performanceEpoch, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil))
	require.NoError(t, meta.SetEpoch(200, potsEpoch, nil, nil, nil, nil, eras.ShelleyEraDesc.Id, 1, 100, nil))
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Treasury:     10,
		Reserves:     100_000_000,
		Fees:         2_000,
		CapturedSlot: 300,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:           rewardSnapshotEpoch,
		SnapshotType:    "mark",
		CapturedSlot:    100,
		BoundarySlot:    100,
		ProtocolVersion: 7,
	}, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyStakeRewards(txn, newEpoch, boundarySlot)
	}))

	state, err := meta.GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(100_001_600), uint64(state.Reserves))
	require.Equal(t, uint64(410), uint64(state.Treasury))

	pots, err := meta.GetRewardAdaPots(potsEpoch, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	require.Equal(t, uint64(2_000), uint64(pots.Rewards))
}

func TestStakeRewardEpochsForNewEpochMatchDelayedUpdate(t *testing.T) {
	for _, newEpoch := range []uint64{0, 1, 2} {
		_, ok := stakeRewardEpochsForNewEpoch(newEpoch)
		require.False(t, ok, "epoch %d has no delayed reward update", newEpoch)
	}

	epochs, ok := stakeRewardEpochsForNewEpoch(4)
	require.True(t, ok)
	require.Equal(t, stakeRewardEpochs{
		snapshot:    1,
		performance: 2,
		pots:        3,
	}, epochs)

	epochs, ok = stakeRewardEpochsForNewEpoch(211)
	require.True(t, ok)
	require.Equal(t, stakeRewardEpochs{
		snapshot:    208,
		performance: 209,
		pots:        210,
	}, epochs)
}

func TestRewardParametersUseRUPDCalculationEpochLength(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		performanceEpoch = uint64(2)
		potsEpoch        = uint64(3)
	)
	performancePParams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(1, 2),
		ProtocolMajor:    7,
		ProtocolMinor:    0,
	}
	calculationPParams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(1, 5),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
		ProtocolMinor:    0,
	}
	performancePParamsCbor, err := cbor.Encode(performancePParams)
	require.NoError(t, err)
	calculationPParamsCbor, err := cbor.Encode(calculationPParams)
	require.NoError(t, err)

	require.NoError(t, meta.SetEpoch(
		100,
		performanceEpoch,
		nil,
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		100,
		nil,
	))
	require.NoError(t, meta.SetEpoch(
		200,
		potsEpoch,
		nil,
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		1_000,
		nil,
	))
	require.NoError(t, db.SetPParams(
		performancePParamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, db.SetPParams(
		calculationPParamsCbor,
		200,
		potsEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))

	txn := db.Transaction(false)
	defer func() { _ = txn.Rollback() }()
	_, params, performanceDecentralization, err := ls.rewardParameters(
		txn,
		performanceEpoch,
		potsEpoch,
		&models.RewardAdaPots{Reserves: 100_000_000},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(1_000), params.EpochLength)
	require.Equal(t, big.NewRat(1, 5), params.TreasuryExpansion)
	require.Equal(t, big.NewRat(0, 1), params.Decentralization)
	require.Equal(t, big.NewRat(1, 2), performanceDecentralization)
}

func TestRewardParametersBabbageDefaultsDecentralizationAndForgoesPrefilter(t *testing.T) {
	ls, _ := newRewardCalculationTestLedger(t)
	pparams := &babbage.BabbageProtocolParameters{
		NOpt:          10,
		A0:            rewardCalcRat(1, 2),
		Rho:           rewardCalcRat(1, 100),
		Tau:           rewardCalcRat(0, 1),
		ProtocolMajor: 7,
		ProtocolMinor: 0,
	}

	params, err := rewardParametersFromPParams(
		pparams,
		ls.config.CardanoNodeConfig,
		100,
	)
	require.NoError(t, err)
	require.NotNil(t, params.Decentralization)
	require.Zero(t, params.Decentralization.Sign())
	require.Equal(t, uint64(7), params.ProtocolMajorVersion)
	require.False(t, params.RequiresRewardPrefilter())
}

func TestRewardParametersRejectIncompletePParams(t *testing.T) {
	ls, _ := newRewardCalculationTestLedger(t)
	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
		ProtocolMinor:    0,
	}

	_, err := rewardParametersFromPParams(
		pparams,
		ls.config.CardanoNodeConfig,
		100,
	)
	require.ErrorIs(t, err, rewards.ErrInvalidParameters)
	require.ErrorContains(t, err, "missing treasury expansion")
}

func TestRewardBlockCountsTotalIncludesPoolsOutsideSnapshot(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const performanceEpoch = uint64(2)
	poolKey := rewardCalcHash(0x71)
	otherPoolKey := rewardCalcHash(0x72)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)
	var otherPoolID lcommon.PoolKeyHash
	copy(otherPoolID[:], otherPoolKey)

	require.NoError(t, meta.SetEpoch(
		100,
		performanceEpoch,
		nil,
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		100,
		nil,
	))
	for i := range uint64(3) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			poolID,
			i+1,
			120+i,
			nil,
		))
	}
	for i := range uint64(2) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			otherPoolID,
			i+1,
			130+i,
			nil,
		))
	}

	counts, total, err := ls.rewardBlockCounts(
		meta,
		nil,
		performanceEpoch,
		[]*models.RewardPoolInput{
			{PoolKeyHash: poolKey},
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(3), counts[string(poolKey)])
	require.Equal(t, uint64(5), total)
}

func TestRewardBlockCountsSkipsOverlaySlots(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const performanceEpoch = uint64(2)
	poolKey := rewardCalcHash(0x73)
	otherPoolKey := rewardCalcHash(0x74)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)
	var otherPoolID lcommon.PoolKeyHash
	copy(otherPoolID[:], otherPoolKey)

	require.NoError(t, meta.SetEpoch(
		100,
		performanceEpoch,
		nil,
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		100,
		nil,
	))
	for _, slot := range []uint64{100, 101, 102, 103} {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			poolID,
			slot,
			slot,
			nil,
		))
	}
	require.NoError(t, db.UpdatePoolOpCertSequence(
		otherPoolID,
		105,
		105,
		nil,
	))

	decentralization := big.NewRat(1, 2)
	require.True(t, rewardIsOverlaySlot(100, decentralization, 100))
	require.False(t, rewardIsOverlaySlot(100, decentralization, 101))
	require.True(t, rewardIsOverlaySlot(100, decentralization, 102))
	require.False(t, rewardIsOverlaySlot(100, decentralization, 103))

	counts, total, err := ls.rewardBlockCounts(
		meta,
		nil,
		performanceEpoch,
		[]*models.RewardPoolInput{
			{PoolKeyHash: poolKey},
		},
		decentralization,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(2), counts[string(poolKey)])
	require.Equal(t, uint64(3), total)
}

func TestRewardPrefilterSlotUsesRUPDRandomnessWindow(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()
	require.NoError(t, meta.SetEpoch(
		1_000,
		3,
		nil,
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		1_000,
		nil,
	))

	slot, err := ls.rewardPrefilterSlot(meta, nil, 3)
	require.NoError(t, err)
	require.Equal(t, uint64(1_401), slot)
	require.Equal(t, uint64(300), ls.nonceStabilityWindow(eras.ShelleyEraDesc.Id))
	require.Equal(t, uint64(400), ls.rewardUpdateStabilityWindow())
}

func TestProcessEpochRolloverSnapshotEventUsesProtocolMajor(t *testing.T) {
	ls, db := newRewardCalculationTestLedger(t)
	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    7,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		0,
		0,
		eras.ShelleyEraDesc.Id,
		nil,
	))

	var got event.EpochTransitionEvent
	ls.SetEpochBoundarySnapshotHook(func(
		_ *database.Txn,
		evt event.EpochTransitionEvent,
	) error {
		got = evt
		return nil
	})

	txn := db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		_, err := ls.processEpochRollover(
			txn,
			models.Epoch{
				EpochId:       0,
				StartSlot:     0,
				SlotLength:    1,
				LengthInSlots: 100,
				EraId:         eras.ShelleyEraDesc.Id,
			},
			eras.ShelleyEraDesc,
			pparams,
		)
		return err
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), got.NewEpoch)
	require.Equal(t, uint64(100), got.BoundarySlot)
	require.Equal(t, uint64(99), got.SnapshotSlot)
	require.Equal(t, uint(7), got.ProtocolVersion)
}

// TestPrecomputeStakeRewardsAsyncPathMatchesSingleTransactionPath verifies
// that splitting the async EventBus precompute path
// (precomputeStakeRewardsAfterEpochTransition) into a read-only calculation
// transaction plus a short write transaction produces byte-identical reward
// output rows and ADA pots to the single read-write-transaction
// precomputeStakeRewards path used directly by other tests in this file.
// This guards the read/write split introduced to stop the async precompute
// from holding SQLite's single writer for the entire calculation.
func TestPrecomputeStakeRewardsAsyncPathMatchesSingleTransactionPath(t *testing.T) {
	const (
		rewardSnapshotEpoch = uint64(1)
		potsEpoch           = uint64(3)
		eventBoundarySlot   = uint64(200)
		applicationBoundary = uint64(1_200)
		newEpoch            = uint64(4)
	)

	// Reference: the original single read-write-transaction path.
	lsRef, dbRef := seedRewardPrecomputeTimingState(t, 7)
	metaRef := dbRef.Metadata()
	refTxn := dbRef.Transaction(true)
	require.NoError(t, refTxn.Do(func(txn *database.Txn) error {
		return lsRef.precomputeStakeRewards(
			txn,
			newEpoch,
			eventBoundarySlot,
			applicationBoundary,
		)
	}))
	refPoolOutputs, err := metaRef.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	refAccountOutputs, err := metaRef.GetRewardAccountOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	refPots, err := metaRef.GetRewardAdaPots(potsEpoch, nil)
	require.NoError(t, err)

	// Under test: the split async EventBus path, driven the same way the
	// EventBus itself would (handleRewardPrecomputeEpochTransition).
	lsSplit, dbSplit := seedRewardPrecomputeTimingState(t, 7)
	metaSplit := dbSplit.Metadata()
	lsSplit.handleRewardPrecomputeEpochTransition(event.NewEvent(
		event.EpochTransitionEventType,
		event.EpochTransitionEvent{
			PreviousEpoch: 2,
			NewEpoch:      3,
			BoundarySlot:  eventBoundarySlot,
			EpochNonce:    []byte{0x01},
			SnapshotSlot:  eventBoundarySlot - 1,
		},
	))
	lsSplit.rewardPrecomputeWG.Wait()
	splitPoolOutputs, err := metaSplit.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	splitAccountOutputs, err := metaSplit.GetRewardAccountOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	splitPots, err := metaSplit.GetRewardAdaPots(potsEpoch, nil)
	require.NoError(t, err)

	require.NotEmpty(t, refPoolOutputs)
	require.NotEmpty(t, refAccountOutputs)
	require.Len(t, splitPoolOutputs, len(refPoolOutputs))
	require.Len(t, splitAccountOutputs, len(refAccountOutputs))

	require.Equal(
		t,
		rewardCalcNormalizePoolOutputs(refPoolOutputs),
		rewardCalcNormalizePoolOutputs(splitPoolOutputs),
	)
	require.Equal(
		t,
		rewardCalcNormalizeAccountOutputs(refAccountOutputs),
		rewardCalcNormalizeAccountOutputs(splitAccountOutputs),
	)
	require.NotNil(t, refPots)
	require.NotNil(t, splitPots)
	require.Equal(t, refPots.Rewards, splitPots.Rewards)
}

// rewardCalcNormalizePoolOutputs strips the auto-increment ID (which is an
// artifact of insert order into a particular database, not part of the
// calculated result) so reward pool output rows from two independently
// seeded databases can be compared for equality.
func rewardCalcNormalizePoolOutputs(
	outputs []*models.RewardPoolOutput,
) []models.RewardPoolOutput {
	ret := make([]models.RewardPoolOutput, len(outputs))
	for i, output := range outputs {
		normalized := *output
		normalized.ID = 0
		ret[i] = normalized
	}
	return ret
}

// rewardCalcNormalizeAccountOutputs is the RewardAccountOutput counterpart
// of rewardCalcNormalizePoolOutputs.
func rewardCalcNormalizeAccountOutputs(
	outputs []*models.RewardAccountOutput,
) []models.RewardAccountOutput {
	ret := make([]models.RewardAccountOutput, len(outputs))
	for i, output := range outputs {
		normalized := *output
		normalized.ID = 0
		ret[i] = normalized
	}
	return ret
}

// TestStakeRewardPrecomputeSnapshotGuardOK exercises the guard that protects
// the write phase of the split async precompute
// (precomputeStakeRewardsAfterEpochTransition) from persisting a computed
// stakeRewardApplication whose reward_snapshot row has moved since the
// read-only calculation ran. It runs the real read phase
// (precomputeStakeRewardsCalculate) to obtain an app the same way the
// production code does, then simulates the world changing between the read
// and write phases (a rollback replacing or removing the mark snapshot) and
// checks that the guard rejects the stale app -- so a caller honoring it
// never persists a stale result -- while still accepting a matching,
// unchanged snapshot.
func TestStakeRewardPrecomputeSnapshotGuardOK(t *testing.T) {
	const (
		rewardSnapshotEpoch = uint64(1)
		newEpoch            = uint64(4)
		capturedSlot        = uint64(200)
		boundarySlot        = uint64(1_200)
	)

	calculate := func(
		t *testing.T,
		ls *LedgerState,
		db *database.Database,
	) *stakeRewardApplication {
		t.Helper()
		var app *stakeRewardApplication
		readTxn := db.Transaction(false)
		require.NoError(t, readTxn.Do(func(txn *database.Txn) error {
			computed, ok, err := ls.precomputeStakeRewardsCalculate(
				txn,
				newEpoch,
				capturedSlot,
				boundarySlot,
			)
			require.NoError(t, err)
			require.True(t, ok)
			app = computed
			return nil
		}))
		require.NotNil(t, app)
		return app
	}

	t.Run("matching snapshot passes and is safe to persist", func(t *testing.T) {
		ls, db := seedRewardPrecomputeTimingState(t, 7)
		meta := db.Metadata()
		app := calculate(t, ls, db)
		require.Equal(t, uint64(100), app.snapshotCapturedSlot)
		require.Equal(t, uint64(100), app.snapshotBoundarySlot)

		writeTxn := db.Transaction(true)
		require.NoError(t, writeTxn.Do(func(txn *database.Txn) error {
			ok, err := stakeRewardPrecomputeSnapshotGuardOK(
				meta,
				txn.Metadata(),
				app,
			)
			require.NoError(t, err)
			require.True(t, ok)
			return ls.saveStakeRewardPrecompute(
				meta,
				txn.Metadata(),
				app,
				newEpoch,
				capturedSlot,
				boundarySlot,
			)
		}))

		poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
		require.NoError(t, err)
		require.Len(t, poolOutputs, 1)
		accountOutputs, err := meta.GetRewardAccountOutputs(rewardSnapshotEpoch, nil)
		require.NoError(t, err)
		require.Len(t, accountOutputs, 2)
	})

	t.Run("snapshot replaced between read and write is dropped", func(t *testing.T) {
		ls, db := seedRewardPrecomputeTimingState(t, 7)
		meta := db.Metadata()
		app := calculate(t, ls, db)

		// Simulate the world moving on between the read and write phases: a
		// rollback replaces the mark snapshot for the same epoch with
		// different captured/boundary slots (e.g. it was recaptured at a
		// different point after a reorg).
		require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
			Epoch:            rewardSnapshotEpoch,
			SnapshotType:     "mark",
			TotalActiveStake: 1_000,
			TotalPoolCount:   1,
			TotalDelegators:  2,
			CapturedSlot:     555,
			BoundarySlot:     555,
			ProtocolVersion:  7,
		}, nil))

		writeTxn := db.Transaction(true)
		require.NoError(t, writeTxn.Do(func(txn *database.Txn) error {
			ok, err := stakeRewardPrecomputeSnapshotGuardOK(
				meta,
				txn.Metadata(),
				app,
			)
			require.NoError(t, err)
			require.False(t, ok)
			return nil
		}))

		poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
		require.NoError(t, err)
		require.Empty(t, poolOutputs)
		accountOutputs, err := meta.GetRewardAccountOutputs(rewardSnapshotEpoch, nil)
		require.NoError(t, err)
		require.Empty(t, accountOutputs)
	})

	t.Run("snapshot removed between read and write is dropped", func(t *testing.T) {
		ls, db := seedRewardPrecomputeTimingState(t, 7)
		meta := db.Metadata()
		app := calculate(t, ls, db)

		// Simulate a rollback deleting the snapshot outright.
		require.NoError(t, meta.DeleteRewardStateAfterSlot(0, nil))

		writeTxn := db.Transaction(true)
		require.NoError(t, writeTxn.Do(func(txn *database.Txn) error {
			ok, err := stakeRewardPrecomputeSnapshotGuardOK(
				meta,
				txn.Metadata(),
				app,
			)
			require.NoError(t, err)
			require.False(t, ok)
			return nil
		}))

		poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
		require.NoError(t, err)
		require.Empty(t, poolOutputs)
	})

	t.Run("rollback generation changed between read and write is dropped", func(t *testing.T) {
		ls, db := seedRewardPrecomputeTimingState(t, 7)
		meta := db.Metadata()
		app := calculate(t, ls, db)

		// The Mark snapshot is deliberately unchanged. The generation represents
		// a rollback-sensitive input outside that row (blocks, pots, pparams, or
		// certificate history) changing after calculation.
		ls.rewardInputGeneration.Add(1)

		writeTxn := db.Transaction(true)
		require.NoError(t, writeTxn.Do(func(txn *database.Txn) error {
			ok, err := stakeRewardPrecomputeSnapshotGuardOK(
				meta,
				txn.Metadata(),
				app,
			)
			require.NoError(t, err)
			require.False(t, ok)
			return nil
		}))
	})
}

func TestRewardPrefilterAccountsSkipsHistoryWhenNotRequired(t *testing.T) {
	activeAccounts := map[string]struct{}{"active": {}}
	accounts, err := rewardPrefilterAccounts(
		rewardAccountHistoryMustNotRun{},
		nil,
		nil,
		nil,
		100,
		false,
		activeAccounts,
	)
	require.NoError(t, err)
	require.Equal(t, activeAccounts, accounts)
}

type rewardAccountHistoryMustNotRun struct{}

func (rewardAccountHistoryMustNotRun) GetAccountsActiveAtSlot(
	[]models.StakeCredentialRef,
	uint64,
	types.Txn,
) (map[string]struct{}, error) {
	panic("reward account history query must not run")
}

// TestStakeRewardPrecomputeSnapshotGuardRejectsSameSlotContentChange verifies
// the deferred-persist guard rejects a mark snapshot that keeps identical
// captured/boundary slots but whose content changed between the read and write
// phases (a rollback re-deriving the same-slot snapshot could produce this).
// The guard compares snapshot content (epoch nonce and totals), not just slot
// numbers, so a slot-only check would have wrongly persisted stale rewards.
func TestStakeRewardPrecomputeSnapshotGuardRejectsSameSlotContentChange(
	t *testing.T,
) {
	const (
		rewardSnapshotEpoch = uint64(1)
		newEpoch            = uint64(4)
		capturedSlot        = uint64(200)
		boundarySlot        = uint64(1_200)
	)

	ls, db := seedRewardPrecomputeTimingState(t, 7)
	meta := db.Metadata()

	var app *stakeRewardApplication
	readTxn := db.Transaction(false)
	require.NoError(t, readTxn.Do(func(txn *database.Txn) error {
		computed, ok, err := ls.precomputeStakeRewardsCalculate(
			txn,
			newEpoch,
			capturedSlot,
			boundarySlot,
		)
		require.NoError(t, err)
		require.True(t, ok)
		app = computed
		return nil
	}))
	require.NotNil(t, app)
	require.Equal(t, uint64(100), app.snapshotCapturedSlot)
	require.Equal(t, uint64(100), app.snapshotBoundarySlot)

	// Re-save the mark snapshot with identical captured/boundary slots but a
	// different active-stake total and epoch nonce, mimicking a rollback that
	// re-derived the same-slot snapshot with different content. A slot-only
	// guard would accept this and persist the stale precomputed rewards.
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 2_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		EpochNonce:       []byte{0xAB, 0xCD},
		ProtocolVersion:  7,
	}, nil))
	changedSnapshot, err := meta.GetRewardSnapshot(
		rewardSnapshotEpoch,
		"mark",
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, changedSnapshot)
	require.Equal(t, types.Uint64(2_000), changedSnapshot.TotalActiveStake)
	require.Equal(t, []byte{0xAB, 0xCD}, changedSnapshot.EpochNonce)

	writeTxn := db.Transaction(true)
	require.NoError(t, writeTxn.Do(func(txn *database.Txn) error {
		ok, err := stakeRewardPrecomputeSnapshotGuardOK(
			meta,
			txn.Metadata(),
			app,
		)
		require.NoError(t, err)
		require.False(t, ok)
		return nil
	}))

	poolOutputs, err := meta.GetRewardPoolOutputs(rewardSnapshotEpoch, nil)
	require.NoError(t, err)
	require.Empty(t, poolOutputs)
}

func newRewardCalculationTestLedger(
	t *testing.T,
) (*LedgerState, *database.Database) {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.1,
		"epochLength": 100,
		"maxLovelaceSupply": 100010000,
		"securityParam": 10,
		"slotLength": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	db, err := database.New(&database.Config{
		BlobPlugin:     "badger",
		MetadataPlugin: "sqlite",
		DataDir:        "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() }) //nolint:errcheck

	return &LedgerState{
		db:         db,
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}, db
}

func seedRewardPrecomputeTimingState(
	t *testing.T,
	protocolMajor uint,
) (*LedgerState, *database.Database) {
	t.Helper()
	ls, db := newRewardCalculationTestLedger(t)
	meta := db.Metadata()

	const (
		rewardSnapshotEpoch = uint64(1)
		performanceEpoch    = uint64(2)
		potsEpoch           = uint64(3)
	)
	poolKey := rewardCalcHash(0x4a)
	rewardAccount := rewardCalcHash(0x5a)
	member := rewardCalcHash(0x6a)
	var poolID lcommon.PoolKeyHash
	copy(poolID[:], poolKey)

	pparams := &shelley.ShelleyProtocolParameters{
		NOpt:             10,
		A0:               rewardCalcRat(1, 2),
		Rho:              rewardCalcRat(1, 100),
		Tau:              rewardCalcRat(0, 1),
		Decentralization: rewardCalcRat(0, 1),
		ProtocolMajor:    protocolMajor,
		ProtocolMinor:    0,
	}
	pparamsCbor, err := cbor.Encode(pparams)
	require.NoError(t, err)

	require.NoError(t, meta.SetEpoch(
		100,
		performanceEpoch,
		nil,
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		100,
		nil,
	))
	require.NoError(t, meta.SetEpoch(
		200,
		potsEpoch,
		nil,
		nil,
		nil,
		nil,
		eras.ShelleyEraDesc.Id,
		1,
		1_000,
		nil,
	))
	for i := range uint64(10) {
		require.NoError(t, db.UpdatePoolOpCertSequence(
			poolID,
			i+1,
			140+i,
			nil,
		))
	}
	require.NoError(t, db.SetPParams(
		pparamsCbor,
		100,
		performanceEpoch,
		eras.ShelleyEraDesc.Id,
		nil,
	))
	require.NoError(t, meta.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        potsEpoch,
		Reserves:     100_000_000,
		CapturedSlot: 200,
	}, nil))
	require.NoError(t, meta.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:            rewardSnapshotEpoch,
		SnapshotType:     "mark",
		TotalActiveStake: 1_000,
		TotalPoolCount:   1,
		TotalDelegators:  2,
		CapturedSlot:     100,
		BoundarySlot:     100,
		ProtocolVersion:  protocolMajor,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      rewardSnapshotEpoch,
			PoolKeyHash:                poolKey,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			Pledge:                     500,
			Cost:                       1_000,
			DelegatedStake:             1_000,
			OwnerStake:                 500,
			DelegatorCount:             2,
			CapturedSlot:               100,
			BoundarySlot:               100,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    rewardAccount,
			Stake:         500,
			Owner:         true,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
		{
			Epoch:         rewardSnapshotEpoch,
			PoolKeyHash:   poolKey,
			CredentialTag: 0,
			StakingKey:    member,
			Stake:         500,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  100,
		},
	}, nil))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: rewardAccount,
		Active:     true,
	}))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: member,
		Active:     true,
	}))
	return ls, db
}

func rewardCalcHash(fill byte) []byte {
	ret := make([]byte, 28)
	for i := range ret {
		ret[i] = fill
	}
	return ret
}

func rewardCalcRat(num int64, denom int64) *cbor.Rat {
	return &cbor.Rat{Rat: big.NewRat(num, denom)}
}

type rewardCalcMetadataDB interface {
	DB() *gorm.DB
}

func rewardCalcGormDB(t *testing.T, db *database.Database) *gorm.DB {
	t.Helper()
	provider, ok := db.Metadata().(rewardCalcMetadataDB)
	require.True(t, ok, "metadata store should expose DB() for test seeding")
	return provider.DB()
}

func rewardCalcSetAccountActive(
	t *testing.T,
	db *database.Database,
	stakingKey []byte,
	active bool,
) {
	t.Helper()
	rewardCalcSetAccountActiveByCredential(t, db, 0, stakingKey, active)
}

func rewardCalcSetAccountActiveByCredential(
	t *testing.T,
	db *database.Database,
	credentialTag uint8,
	stakingKey []byte,
	active bool,
) {
	t.Helper()
	require.NoError(t, rewardCalcGormDB(t, db).
		Model(&models.Account{}).
		Where("credential_tag = ? AND staking_key = ?", credentialTag, stakingKey).
		Update("active", active).Error)
}

func rewardCalcSeedStakeCert(
	t *testing.T,
	db *database.Database,
	id uint,
	stakingKey []byte,
	credentialTag uint8,
	slot uint64,
	certType uint,
) {
	t.Helper()
	gormDB := rewardCalcGormDB(t, db)
	hash := make([]byte, 32)
	binary.BigEndian.PutUint64(hash[24:], uint64(id))
	require.NoError(t, gormDB.Create(&models.Transaction{
		ID:         id,
		Hash:       hash,
		Slot:       slot,
		BlockIndex: 0,
	}).Error)
	require.NoError(t, gormDB.Create(&models.Certificate{
		ID:            id,
		TransactionID: id,
		CertIndex:     0,
		Slot:          slot,
		CertType:      certType,
	}).Error)
	switch certType {
	case uint(lcommon.CertificateTypeStakeRegistration):
		require.NoError(t, gormDB.Create(&models.StakeRegistration{
			ID:            id,
			StakingKey:    stakingKey,
			CredentialTag: credentialTag,
			CertificateID: id,
			AddedSlot:     slot,
		}).Error)
	case uint(lcommon.CertificateTypeStakeDeregistration):
		require.NoError(t, gormDB.Create(&models.StakeDeregistration{
			ID:            id,
			StakingKey:    stakingKey,
			CredentialTag: credentialTag,
			CertificateID: id,
			AddedSlot:     slot,
		}).Error)
	default:
		t.Fatalf("unsupported cert type %d", certType)
	}
}
