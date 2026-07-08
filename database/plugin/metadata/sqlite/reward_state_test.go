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
	"encoding/binary"
	"math/big"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

func TestRewardAdaPotsCRUD(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	pots := &models.RewardAdaPots{
		Epoch:        42,
		Treasury:     1,
		Reserves:     2,
		Fees:         3,
		Rewards:      4,
		CapturedSlot: 12345,
	}
	require.NoError(t, store.SaveRewardAdaPots(pots, nil))

	got, err := store.GetRewardAdaPots(42, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(1), uint64(got.Treasury))
	require.Equal(t, uint64(2), uint64(got.Reserves))
	require.Equal(t, uint64(3), uint64(got.Fees))
	require.Equal(t, uint64(4), uint64(got.Rewards))
	require.Equal(t, uint64(12345), got.CapturedSlot)

	pots.Treasury = 10
	pots.CapturedSlot = 12346
	require.NoError(t, store.SaveRewardAdaPots(pots, nil))

	got, err = store.GetRewardAdaPots(42, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(10), uint64(got.Treasury))
	require.Equal(t, uint64(12346), got.CapturedSlot)

	got, err = store.GetRewardAdaPots(99, nil)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestRewardSnapshotCRUD(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	snapshot := &models.RewardSnapshot{
		Epoch:            42,
		SnapshotType:     "mark",
		TotalActiveStake: 100,
		TotalPoolCount:   2,
		TotalDelegators:  5,
		CapturedSlot:     12344,
		BoundarySlot:     12345,
		EpochNonce:       []byte{0x01, 0x02},
		ProtocolVersion:  8,
	}
	require.NoError(t, store.SaveRewardSnapshot(snapshot, nil))

	got, err := store.GetRewardSnapshot(42, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(100), uint64(got.TotalActiveStake))
	require.Equal(t, uint64(2), got.TotalPoolCount)
	require.Equal(t, uint64(5), got.TotalDelegators)
	require.Equal(t, uint64(12344), got.CapturedSlot)
	require.Equal(t, uint64(12345), got.BoundarySlot)
	require.Equal(t, []byte{0x01, 0x02}, got.EpochNonce)
	require.Equal(t, uint(8), got.ProtocolVersion)

	snapshot.TotalActiveStake = 200
	snapshot.TotalPoolCount = 3
	require.NoError(t, store.SaveRewardSnapshot(snapshot, nil))

	got, err = store.GetRewardSnapshot(42, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(200), uint64(got.TotalActiveStake))
	require.Equal(t, uint64(3), got.TotalPoolCount)

	got, err = store.GetRewardSnapshot(42, "go", nil)
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestRewardPoolInputsCRUD(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	blocksProduced := uint64(12)
	totalBlocks := uint64(100)
	poolA := rewardStateTestHash(0xaa)
	poolB := rewardStateTestHash(0xbb)
	inputs := []*models.RewardPoolInput{
		{
			Epoch:                      42,
			PoolKeyHash:                poolA,
			BlocksProduced:             &blocksProduced,
			TotalBlocksInEpoch:         &totalBlocks,
			Pledge:                     1_000,
			DelegatedStake:             2_000,
			OwnerStake:                 500,
			Cost:                       340,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 20)},
			RewardAccount:              rewardStateTestHash(0x01),
			RewardAccountCredentialTag: 1,
			DelegatorCount:             10,
			CapturedSlot:               12344,
			BoundarySlot:               12345,
		},
		{
			Epoch:          42,
			PoolKeyHash:    poolB,
			Pledge:         3_000,
			DelegatedStake: 4_000,
			OwnerStake:     1_500,
			Cost:           500,
			Margin:         &types.Rat{Rat: big.NewRat(3, 100)},
			RewardAccount:  rewardStateTestHash(0x02),
			DelegatorCount: 20,
			CapturedSlot:   12344,
			BoundarySlot:   12345,
		},
	}
	require.NoError(t, store.SaveRewardPoolInputs(inputs, nil))

	got, err := store.GetRewardPoolInputs(42, nil)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, poolA, got[0].PoolKeyHash)
	require.NotNil(t, got[0].BlocksProduced)
	require.NotNil(t, got[0].TotalBlocksInEpoch)
	require.Equal(t, uint64(12), *got[0].BlocksProduced)
	require.Equal(t, uint64(100), *got[0].TotalBlocksInEpoch)
	require.Equal(t, uint64(1_000), uint64(got[0].Pledge))
	require.Equal(t, uint64(2_000), uint64(got[0].DelegatedStake))
	require.Equal(t, uint64(500), uint64(got[0].OwnerStake))
	require.Equal(t, uint64(340), uint64(got[0].Cost))
	require.Equal(t, "1/20", got[0].Margin.String())
	require.Equal(t, rewardStateTestHash(0x01), got[0].RewardAccount)
	require.Equal(t, uint8(1), got[0].RewardAccountCredentialTag)
	require.Equal(t, uint64(10), got[0].DelegatorCount)
	require.Equal(t, poolB, got[1].PoolKeyHash)
	require.Nil(t, got[1].BlocksProduced)
	require.Nil(t, got[1].TotalBlocksInEpoch)
	require.Equal(t, uint64(3_000), uint64(got[1].Pledge))
	require.Equal(t, uint64(4_000), uint64(got[1].DelegatedStake))
	require.Equal(t, uint64(1_500), uint64(got[1].OwnerStake))
	require.Equal(t, uint64(500), uint64(got[1].Cost))
	require.Equal(t, "3/100", got[1].Margin.String())
	require.Equal(t, rewardStateTestHash(0x02), got[1].RewardAccount)
	require.Equal(t, uint8(0), got[1].RewardAccountCredentialTag)
	require.Equal(t, uint64(20), got[1].DelegatorCount)
	require.Equal(t, uint64(12344), got[1].CapturedSlot)
	require.Equal(t, uint64(12345), got[1].BoundarySlot)

	updatedBlocksProduced := uint64(13)
	require.NoError(t, store.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:                      42,
			PoolKeyHash:                poolA,
			BlocksProduced:             &updatedBlocksProduced,
			TotalBlocksInEpoch:         &totalBlocks,
			Pledge:                     1_100,
			DelegatedStake:             2_200,
			OwnerStake:                 550,
			Cost:                       350,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
			RewardAccount:              rewardStateTestHash(0x03),
			RewardAccountCredentialTag: 1,
			DelegatorCount:             11,
			CapturedSlot:               12346,
			BoundarySlot:               12347,
		},
	}, nil))

	got, err = store.GetRewardPoolInputs(42, nil)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.NotNil(t, got[0].BlocksProduced)
	require.Equal(t, uint64(13), *got[0].BlocksProduced)
	require.Equal(t, uint64(1_100), uint64(got[0].Pledge))
	require.Equal(t, uint64(2_200), uint64(got[0].DelegatedStake))
	require.Equal(t, uint64(550), uint64(got[0].OwnerStake))
	require.Equal(t, uint64(350), uint64(got[0].Cost))
	require.Equal(t, "1/10", got[0].Margin.String())
	require.Equal(t, rewardStateTestHash(0x03), got[0].RewardAccount)
	require.Equal(t, uint8(1), got[0].RewardAccountCredentialTag)
	require.Equal(t, uint64(11), got[0].DelegatorCount)
	require.Equal(t, uint64(12346), got[0].CapturedSlot)
	require.Equal(t, uint64(12347), got[0].BoundarySlot)

	got, err = store.GetRewardPoolInputs(99, nil)
	require.NoError(t, err)
	require.Empty(t, got)
	require.NoError(t, store.SaveRewardPoolInputs(nil, nil))
}

func TestRewardStakeInputsCRUD(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	poolA := rewardStateTestHash(0xaa)
	stakeA := rewardStateTestHash(0x01)
	stakeB := rewardStateTestHash(0x02)
	inputs := []*models.RewardStakeInput{
		{
			Epoch:         42,
			PoolKeyHash:   poolA,
			StakingKey:    stakeA,
			CredentialTag: 1,
			Stake:         1_000,
			Owner:         true,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  101,
		},
		{
			Epoch:         42,
			PoolKeyHash:   poolA,
			StakingKey:    stakeB,
			CredentialTag: 0,
			Stake:         2_000,
			Registered:    true,
			CapturedSlot:  100,
			BoundarySlot:  101,
		},
	}
	require.NoError(t, store.SaveRewardStakeInputs(inputs, nil))

	got, err := store.GetRewardStakeInputs(42, nil)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, stakeB, got[0].StakingKey)
	require.Equal(t, uint64(2_000), uint64(got[0].Stake))
	require.False(t, got[0].Owner)
	require.Equal(t, stakeA, got[1].StakingKey)
	require.Equal(t, uint64(1_000), uint64(got[1].Stake))
	require.True(t, got[1].Owner)
	require.True(t, got[1].Registered)

	inputs[0].Stake = 1_500
	inputs[0].Owner = false
	require.NoError(t, store.SaveRewardStakeInputs(inputs[:1], nil))

	got, err = store.GetRewardStakeInputs(42, nil)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Equal(t, uint64(1_500), uint64(got[1].Stake))
	require.False(t, got[1].Owner)

	got, err = store.GetRewardStakeInputs(99, nil)
	require.NoError(t, err)
	require.Empty(t, got)
	require.NoError(t, store.SaveRewardStakeInputs(nil, nil))
}

func TestRewardOutputsCRUD(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	poolA := rewardStateTestHash(0xaa)
	stakeA := rewardStateTestHash(0x01)
	poolOutputs := []*models.RewardPoolOutput{
		{
			Epoch:               42,
			PoolKeyHash:         poolA,
			ApparentPerformance: &types.Rat{Rat: big.NewRat(3, 2)},
			OptimalReward:       10_000,
			TotalReward:         9_000,
			LeaderReward:        4_000,
			MemberRewardTotal:   4_999,
			OwnerStake:          1_000,
			Undistributed:       1,
			Unspendable:         0,
			CapturedSlot:        100,
			BoundarySlot:        101,
		},
	}
	require.NoError(t, store.SaveRewardPoolOutputs(poolOutputs, nil))

	gotPools, err := store.GetRewardPoolOutputs(42, nil)
	require.NoError(t, err)
	require.Len(t, gotPools, 1)
	require.Equal(t, "3/2", gotPools[0].ApparentPerformance.String())
	require.Equal(t, uint64(9_000), uint64(gotPools[0].TotalReward))
	require.Equal(t, uint64(1), uint64(gotPools[0].Undistributed))

	poolOutputs[0].TotalReward = 9_500
	poolOutputs[0].Unspendable = 10
	require.NoError(t, store.SaveRewardPoolOutputs(poolOutputs, nil))
	gotPools, err = store.GetRewardPoolOutputs(42, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(9_500), uint64(gotPools[0].TotalReward))
	require.Equal(t, uint64(10), uint64(gotPools[0].Unspendable))

	accountOutputs := []*models.RewardAccountOutput{
		{
			Epoch:         42,
			CredentialTag: 0,
			StakingKey:    stakeA,
			PoolKeyHash:   poolA,
			RewardType:    "member",
			Amount:        4_999,
			Spendable:     true,
			CapturedSlot:  100,
			BoundarySlot:  101,
		},
	}
	require.NoError(t, store.SaveRewardAccountOutputs(accountOutputs, nil))

	gotAccounts, err := store.GetRewardAccountOutputs(42, nil)
	require.NoError(t, err)
	require.Len(t, gotAccounts, 1)
	require.Equal(t, stakeA, gotAccounts[0].StakingKey)
	require.Equal(t, uint64(4_999), uint64(gotAccounts[0].Amount))
	require.True(t, gotAccounts[0].Spendable)

	accountOutputs[0].Amount = 5_001
	accountOutputs[0].Spendable = false
	require.NoError(t, store.SaveRewardAccountOutputs(accountOutputs, nil))
	gotAccounts, err = store.GetRewardAccountOutputs(42, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(5_001), uint64(gotAccounts[0].Amount))
	require.False(t, gotAccounts[0].Spendable)
}

func TestGetRewardStakeInputsAtSlot(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestHash(0xaa)
	poolB := rewardStateTestHash(0xbb)
	stakeA := rewardStateTestHash(0x01)
	stakeB := rewardStateTestHash(0x02)

	requireRewardStatePoolRegistration(t, store, poolA, 0)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeA,
		CredentialTag: 1,
		Pool:          poolA,
		Reward:        2,
		Active:        true,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeB,
		Pool:       poolB,
		Reward:     100,
		Active:     true,
	}).Error)

	for idx, utxo := range []models.Utxo{
		{
			TxId:          rewardStateTestTxHash(0x11),
			OutputIdx:     0,
			StakingKey:    stakeA,
			CredentialTag: 1,
			Amount:        5,
			AddedSlot:     10,
		},
		{
			TxId:          rewardStateTestTxHash(0x22),
			OutputIdx:     0,
			StakingKey:    stakeA,
			CredentialTag: 1,
			Amount:        7,
			AddedSlot:     20,
		},
		{
			TxId:          rewardStateTestTxHash(0x33),
			OutputIdx:     0,
			StakingKey:    stakeA,
			CredentialTag: 1,
			Amount:        11,
			AddedSlot:     10,
			DeletedSlot:   15,
		},
		{
			TxId:          rewardStateTestTxHash(0x44),
			OutputIdx:     0,
			StakingKey:    stakeA,
			CredentialTag: 1,
			Amount:        13,
			AddedSlot:     10,
			DeletedSlot:   25,
		},
		{
			TxId:          rewardStateTestTxHash(0x55),
			OutputIdx:     0,
			StakingKey:    stakeA,
			CredentialTag: 1,
			Amount:        17,
			AddedSlot:     30,
		},
	} {
		utxo.OutputIdx = uint32(idx)
		require.NoError(t, store.DB().Create(&utxo).Error)
	}

	require.NoError(t, store.RebuildRewardLiveStake(20, nil))

	inputs, err := store.GetRewardStakeInputsAtSlot(
		[][]byte{poolA},
		20,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, poolA, inputs[0].PoolKeyHash)
	require.Equal(t, stakeA, inputs[0].StakingKey)
	require.Equal(t, uint8(1), inputs[0].CredentialTag)
	require.Equal(t, uint64(31), uint64(inputs[0].Stake))
	require.True(t, inputs[0].Registered)
}

func TestGetRewardStakeInputsAtSlotDeduplicatesPoolHashChunks(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestHash(0xaa)
	stakeA := rewardStateTestHash(0x01)

	requireRewardStatePoolRegistration(t, store, poolA, 0)
	require.NoError(t, store.DB().Create(&models.RewardLiveStake{
		PoolKeyHash:        poolA,
		StakingKey:         stakeA,
		CredentialTag:      0,
		TotalStake:         5,
		Registered:         true,
		PoolDelegationSlot: 0,
	}).Error)

	poolKeyHashes := make([][]byte, sqliteBindVarLimit+1)
	for i := range poolKeyHashes {
		hash := make([]byte, 28)
		hash[0] = 0xfe
		hash[1] = byte(i)
		hash[2] = byte(i >> 8)
		poolKeyHashes[i] = hash
	}
	poolKeyHashes[0] = poolA
	poolKeyHashes[sqliteBindVarLimit] = append([]byte(nil), poolA...)

	inputs, err := store.GetRewardStakeInputsAtSlot(
		poolKeyHashes,
		20,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, poolA, inputs[0].PoolKeyHash)
	require.Equal(t, stakeA, inputs[0].StakingKey)
	require.Equal(t, uint64(5), uint64(inputs[0].Stake))
}

func TestRewardLiveStakeTracksAccountUtxoAndRewardChanges(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestHash(0xaa)
	stakeA := rewardStateTestHash(0x01)

	requireRewardStatePoolRegistration(t, store, poolA, 0)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey:    stakeA,
		CredentialTag: 0,
		Pool:          poolA,
		Reward:        3,
		Active:        true,
		AddedSlot:     10,
	}))

	inputs, err := store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 10, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(3), uint64(inputs[0].Stake))

	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:          rewardStateTestTxHash(0x10),
		OutputIdx:     0,
		StakingKey:    stakeA,
		CredentialTag: 0,
		Amount:        5,
		AddedSlot:     11,
	}))
	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 11, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(8), uint64(inputs[0].Stake))

	require.NoError(t, store.AddAccountRewardByCredential(
		0,
		stakeA,
		7,
		12,
		rewardStateTestTxHash(0x12),
		nil,
	))
	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 12, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(15), uint64(inputs[0].Stake))

	require.NoError(t, store.ApplyAccountRewardWithdrawal(
		0,
		stakeA,
		10,
		13,
		rewardStateTestTxHash(0x13),
		nil,
	))
	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 13, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(5), uint64(inputs[0].Stake))

	require.NoError(t, store.DeleteAccountRewardsAfterSlot(11, nil))
	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 11, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(8), uint64(inputs[0].Stake))
}

// TestRewardLiveStakeCreditDeltaIsExactAcrossTwoCredits proves the
// AddAccountRewardByCredential delta-update path (a single UPDATE of
// reward_stake/total_stake, added as an efficiency fix for the epoch-boundary
// reward rollover which credits ~every delegator's account) computes exact
// totals across two sequential credits, and leaves every other
// reward_live_stake column untouched. It also cross-checks the result
// against a full RebuildRewardLiveStake recompute from source tables.
func TestRewardLiveStakeCreditDeltaIsExactAcrossTwoCredits(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestHash(0xaa)
	stakeA := rewardStateTestHash(0x77)

	requireRewardStatePoolRegistration(t, store, poolA, 0)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey:    stakeA,
		CredentialTag: 0,
		Pool:          poolA,
		Reward:        1_000,
		Active:        true,
		AddedSlot:     10,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:          rewardStateTestTxHash(0x77),
		OutputIdx:     0,
		StakingKey:    stakeA,
		CredentialTag: 0,
		Amount:        2_000_000,
		AddedSlot:     10,
	}))

	// Sanity: the row already exists before crediting, so both credits below
	// exercise the delta-update path rather than falling back to a full
	// refresh (which only happens when no row exists yet).
	var seeded models.RewardLiveStake
	require.NoError(t, store.DB().Where(
		"credential_tag = ? AND staking_key = ?", 0, stakeA,
	).First(&seeded).Error)
	require.Equal(t, types.Uint64(1_000), seeded.RewardStake)
	require.Equal(t, types.Uint64(2_000_000), seeded.UtxoStake)
	require.Equal(t, types.Uint64(2_001_000), seeded.TotalStake)

	const credit1 = uint64(500_000_000)
	const credit2 = uint64(123_456_789)
	require.NoError(t, store.AddAccountRewardByCredential(
		0, stakeA, credit1, 11, rewardStateTestTxHash(0x01), nil,
	))
	require.NoError(t, store.AddAccountRewardByCredential(
		0, stakeA, credit2, 12, rewardStateTestTxHash(0x02), nil,
	))

	wantReward := types.Uint64(1_000 + credit1 + credit2)
	wantTotal := types.Uint64(2_000_000 + 1_000 + credit1 + credit2)

	var got models.RewardLiveStake
	require.NoError(t, store.DB().Where(
		"credential_tag = ? AND staking_key = ?", 0, stakeA,
	).First(&got).Error)
	require.Equal(
		t, wantReward, got.RewardStake,
		"reward_stake must equal the exact sum of both credits",
	)
	require.Equal(
		t, wantTotal, got.TotalStake,
		"total_stake must equal the exact sum of utxo_stake and both credits",
	)
	require.Equal(
		t, types.Uint64(2_000_000), got.UtxoStake,
		"utxo_stake must be untouched by the reward-credit delta",
	)
	require.Equal(
		t, poolA, got.PoolKeyHash,
		"pool delegation must be untouched by the reward-credit delta",
	)
	require.True(
		t, got.Registered,
		"registered flag must be untouched by the reward-credit delta",
	)
	require.Equal(
		t, uint64(12), got.UpdatedSlot,
		"updated_slot must reflect the latest credit's slot",
	)

	// Cross-check against ground truth: a full rebuild from source tables
	// must land on the exact same totals the delta path produced.
	require.NoError(t, store.RebuildRewardLiveStake(12, nil))
	var rebuilt models.RewardLiveStake
	require.NoError(t, store.DB().Where(
		"credential_tag = ? AND staking_key = ?", 0, stakeA,
	).First(&rebuilt).Error)
	require.Equal(t, wantReward, rebuilt.RewardStake)
	require.Equal(t, wantTotal, rebuilt.TotalStake)
}

func TestRewardLiveStakeCreditDeltaRejectsTotalStakeOverflow(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestHash(0xaa)
	stakeA := rewardStateTestHash(0x78)

	requireRewardStatePoolRegistration(t, store, poolA, 0)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey:    stakeA,
		CredentialTag: 0,
		Pool:          poolA,
		Reward:        types.Uint64(^uint64(0) - 10),
		Active:        true,
		AddedSlot:     10,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:          rewardStateTestTxHash(0x78),
		OutputIdx:     0,
		StakingKey:    stakeA,
		CredentialTag: 0,
		Amount:        8,
		AddedSlot:     10,
	}))

	err := store.AddAccountRewardByCredential(
		0, stakeA, 5, 11, rewardStateTestTxHash(0x01), nil,
	)
	require.ErrorContains(t, err, "reward live stake overflow")

	account, err := store.GetAccountByCredential(0, stakeA, false, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	require.Equal(t, types.Uint64(^uint64(0)-10), account.Reward)

	var live models.RewardLiveStake
	require.NoError(t, store.DB().Where(
		"credential_tag = ? AND staking_key = ?", 0, stakeA,
	).First(&live).Error)
	require.Equal(t, types.Uint64(^uint64(0)-2), live.TotalStake)
	require.Equal(t, types.Uint64(^uint64(0)-10), live.RewardStake)

	var deltaCount int64
	require.NoError(t, store.DB().Model(&models.AccountRewardDelta{}).
		Where("credential_tag = ? AND staking_key = ?", 0, stakeA).
		Count(&deltaCount).Error)
	require.Zero(t, deltaCount)
}

func TestRewardLiveStakeIncludesExistingUtxosWhenAccountRegisters(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestHash(0xaa)
	stakeA := rewardStateTestHash(0x51)

	requireRewardStatePoolRegistration(t, store, poolA, 0)
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:          rewardStateTestTxHash(0x51),
		OutputIdx:     0,
		StakingKey:    stakeA,
		CredentialTag: 0,
		Amount:        11,
		AddedSlot:     10,
	}))
	inputs, err := store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 10, nil)
	require.NoError(t, err)
	require.Empty(t, inputs)

	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey:    stakeA,
		CredentialTag: 0,
		Pool:          poolA,
		Reward:        5,
		Active:        true,
		AddedSlot:     11,
	}))

	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 11, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, poolA, inputs[0].PoolKeyHash)
	require.Equal(t, stakeA, inputs[0].StakingKey)
	require.Equal(t, uint64(16), uint64(inputs[0].Stake))
	require.True(t, inputs[0].Registered)
}

func TestRewardLiveStakeRebuildKeepsUtxosWithoutAccounts(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestHash(0xaa)
	stakeA := rewardStateTestHash(0x52)

	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:          rewardStateTestTxHash(0x52),
		OutputIdx:     0,
		StakingKey:    stakeA,
		CredentialTag: 0,
		Amount:        17,
		AddedSlot:     10,
	}))
	require.NoError(t, store.RebuildRewardLiveStake(11, nil))

	var live models.RewardLiveStake
	require.NoError(t, store.DB().
		Where("credential_tag = ? AND staking_key = ?", 0, stakeA).
		First(&live).Error)
	require.Empty(t, live.PoolKeyHash)
	require.False(t, live.Registered)
	require.Equal(t, uint64(17), uint64(live.UtxoStake))
	require.Equal(t, uint64(0), uint64(live.RewardStake))
	require.Equal(t, uint64(17), uint64(live.TotalStake))
	require.Equal(t, uint64(11), live.UpdatedSlot)

	inputs, err := store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 11, nil)
	require.NoError(t, err)
	require.Empty(t, inputs)
}

func TestRewardLiveStakeRejectsInvalidTouchedCredential(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	err := refreshRewardLiveStakeAggregate(
		store.DB(),
		models.NewStakeCredentialRef(2, rewardStateTestHash(0x91)),
		10,
	)
	require.ErrorContains(t, err, "invalid reward live stake credential tag")

	shortKey := rewardStateTestHash(0x92)[:27]
	err = refreshRewardLiveStakeAggregate(
		store.DB(),
		models.NewStakeCredentialRef(0, shortKey),
		10,
	)
	require.ErrorContains(
		t,
		err,
		"invalid reward live stake credential length",
	)
}

func TestRewardLiveStakeRebuildRejectsInvalidSourceCredential(t *testing.T) {
	t.Parallel()

	t.Run("account tag", func(t *testing.T) {
		t.Parallel()
		store := setupTestDB(t)
		require.NoError(t, store.DB().Create(&models.Account{
			StakingKey:    rewardStateTestHash(0x93),
			CredentialTag: 2,
			AddedSlot:     10,
		}).Error)

		err := store.RebuildRewardLiveStake(10, nil)
		require.ErrorContains(
			t,
			err,
			"invalid account credentials",
		)
	})

	t.Run("account length", func(t *testing.T) {
		t.Parallel()
		store := setupTestDB(t)
		require.NoError(t, store.DB().Create(&models.Account{
			StakingKey:    rewardStateTestHash(0x94)[:27],
			CredentialTag: 0,
			AddedSlot:     10,
		}).Error)

		err := store.RebuildRewardLiveStake(10, nil)
		require.ErrorContains(
			t,
			err,
			"invalid account credentials",
		)
	})

	t.Run("utxo tag", func(t *testing.T) {
		t.Parallel()
		store := setupTestDB(t)
		require.NoError(t, store.DB().Create(&models.Utxo{
			TxId:          rewardStateTestTxHash(0x95),
			OutputIdx:     0,
			StakingKey:    rewardStateTestHash(0x95),
			CredentialTag: 2,
			Amount:        1,
			AddedSlot:     10,
		}).Error)

		err := store.RebuildRewardLiveStake(10, nil)
		require.ErrorContains(t, err, "invalid UTxO credentials")
	})

	t.Run("utxo length", func(t *testing.T) {
		t.Parallel()
		store := setupTestDB(t)
		require.NoError(t, store.DB().Create(&models.Utxo{
			TxId:          rewardStateTestTxHash(0x96),
			OutputIdx:     0,
			StakingKey:    rewardStateTestHash(0x96)[:27],
			CredentialTag: 0,
			Amount:        1,
			AddedSlot:     10,
		}).Error)

		err := store.RebuildRewardLiveStake(10, nil)
		require.ErrorContains(t, err, "invalid UTxO credentials")
	})
}

func TestRewardLiveStakeMovesWhenDelegationChanges(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestHash(0xaa)
	poolB := rewardStateTestHash(0xbb)
	stakeA := rewardStateTestHash(0x61)

	requireRewardStatePoolRegistration(t, store, poolA, 0)
	requireRewardStatePoolRegistration(t, store, poolB, 0)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey:    stakeA,
		CredentialTag: 0,
		Pool:          poolA,
		Active:        true,
		AddedSlot:     10,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:          rewardStateTestTxHash(0x61),
		OutputIdx:     0,
		StakingKey:    stakeA,
		CredentialTag: 0,
		Amount:        11,
		AddedSlot:     11,
	}))

	inputs, err := store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 11, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(11), uint64(inputs[0].Stake))
	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolB}, 11, nil)
	require.NoError(t, err)
	require.Empty(t, inputs)

	account, err := store.GetAccountByCredential(0, stakeA, true, nil)
	require.NoError(t, err)
	require.NotNil(t, account)
	account.Pool = poolB
	account.AddedSlot = 12
	require.NoError(t, saveAccount(account, store.DB()))

	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 12, nil)
	require.NoError(t, err)
	require.Empty(t, inputs)
	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolB}, 12, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, poolB, inputs[0].PoolKeyHash)
	require.Equal(t, stakeA, inputs[0].StakingKey)
	require.Equal(t, uint64(11), uint64(inputs[0].Stake))
}

func TestRewardLiveStakeKeepsDelegationAcrossPoolRegistration(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestHash(0xaa)
	stakeA := rewardStateTestHash(0x71)

	pool := models.Pool{PoolKeyHash: poolA}
	require.NoError(t, store.DB().Create(&pool).Error)
	require.NoError(t, store.DB().Create(&models.PoolRegistration{
		PoolID:      pool.ID,
		PoolKeyHash: poolA,
		AddedSlot:   20,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey:    stakeA,
		CredentialTag: 0,
		Pool:          poolA,
		Active:        true,
		AddedSlot:     10,
	}).Error)
	require.NoError(t, store.DB().Create(&models.StakeDelegation{
		StakingKey:    stakeA,
		CredentialTag: 0,
		PoolKeyHash:   poolA,
		AddedSlot:     10,
	}).Error)
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:          rewardStateTestTxHash(0x71),
		OutputIdx:     0,
		StakingKey:    stakeA,
		CredentialTag: 0,
		Amount:        11,
		AddedSlot:     11,
	}))
	require.NoError(t, store.RebuildRewardLiveStake(20, nil))

	inputs, err := store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 20, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, stakeA, inputs[0].StakingKey)
	require.Equal(t, uint64(11), uint64(inputs[0].Stake))

	require.NoError(t, store.DB().Create(&models.StakeDelegation{
		StakingKey:    stakeA,
		CredentialTag: 0,
		PoolKeyHash:   poolA,
		AddedSlot:     21,
	}).Error)
	require.NoError(t, store.RebuildRewardLiveStake(21, nil))

	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 21, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, stakeA, inputs[0].StakingKey)
	require.Equal(t, uint64(11), uint64(inputs[0].Stake))
}

func TestRewardLiveStakeIgnoresPoolRegistrationCertificateOrder(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestHash(0xaa)
	stakeBefore := rewardStateTestHash(0x71)
	stakeEqual := rewardStateTestHash(0x72)
	stakeAfter := rewardStateTestHash(0x73)

	pool := models.Pool{PoolKeyHash: poolA}
	require.NoError(t, store.DB().Create(&pool).Error)
	tx := models.Transaction{
		Hash:       rewardStateTestTxHash(0x70),
		Slot:       30,
		BlockIndex: 2,
		Valid:      true,
	}
	require.NoError(t, store.DB().Create(&tx).Error)
	cert := models.Certificate{
		TransactionID: tx.ID,
		Slot:          30,
		CertIndex:     5,
		CertType:      uint(lcommon.CertificateTypePoolRegistration),
	}
	require.NoError(t, store.DB().Create(&cert).Error)
	require.NoError(t, store.DB().Create(&models.PoolRegistration{
		PoolID:        pool.ID,
		PoolKeyHash:   poolA,
		CertificateID: cert.ID,
		AddedSlot:     30,
	}).Error)

	require.NoError(t, store.DB().Create(&[]models.RewardLiveStake{
		{
			PoolKeyHash:              poolA,
			StakingKey:               stakeBefore,
			Registered:               true,
			TotalStake:               11,
			PoolDelegationSlot:       30,
			PoolDelegationBlockIndex: 2,
			PoolDelegationCertIndex:  4,
		},
		{
			PoolKeyHash:              poolA,
			StakingKey:               stakeEqual,
			Registered:               true,
			TotalStake:               13,
			PoolDelegationSlot:       30,
			PoolDelegationBlockIndex: 2,
			PoolDelegationCertIndex:  5,
		},
		{
			PoolKeyHash:              poolA,
			StakingKey:               stakeAfter,
			Registered:               true,
			TotalStake:               17,
			PoolDelegationSlot:       30,
			PoolDelegationBlockIndex: 3,
			PoolDelegationCertIndex:  0,
		},
	}).Error)

	inputs, err := store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 30, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 3)
	require.Equal(t, stakeBefore, inputs[0].StakingKey)
	require.Equal(t, uint64(11), uint64(inputs[0].Stake))
	require.Equal(t, stakeEqual, inputs[1].StakingKey)
	require.Equal(t, uint64(13), uint64(inputs[1].Stake))
	require.Equal(t, stakeAfter, inputs[2].StakingKey)
	require.Equal(t, uint64(17), uint64(inputs[2].Stake))
}

func TestCountPoolBlocksInSlotRangeTotalIncludesUnrequestedPools(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestPoolKeyHash(0xaa)
	poolB := rewardStateTestPoolKeyHash(0xbb)

	require.NoError(t, store.UpdatePoolOpCertSequence(poolA, 1, 10, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(poolA, 2, 11, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(poolB, 1, 12, nil))
	require.NoError(t, store.UpdatePoolOpCertSequence(poolB, 2, 30, nil))

	counts, total, err := store.CountPoolBlocksInSlotRange(
		[]lcommon.PoolKeyHash{poolA},
		10,
		20,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, uint64(3), total)
	require.Equal(t, uint64(2), counts[string(poolA.Bytes())])
	_, ok := counts[string(poolB.Bytes())]
	require.False(t, ok)
}

func TestRewardLiveStakeTracksUtxoDeleteAndRollback(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestHash(0xaa)
	stakeA := rewardStateTestHash(0x01)
	stakeB := rewardStateTestHash(0x02)

	requireRewardStatePoolRegistration(t, store, poolA, 0)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey:    stakeA,
		CredentialTag: 0,
		Pool:          poolA,
		Active:        true,
		AddedSlot:     10,
	}))
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey:    stakeB,
		CredentialTag: 1,
		Pool:          poolA,
		Active:        true,
		AddedSlot:     10,
	}))
	txA := rewardStateTestTxHash(0x20)
	txB := rewardStateTestTxHash(0x21)
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:          txA,
		OutputIdx:     0,
		StakingKey:    stakeA,
		CredentialTag: 0,
		Amount:        5,
		AddedSlot:     11,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:          txB,
		OutputIdx:     0,
		StakingKey:    stakeB,
		CredentialTag: 1,
		Amount:        7,
		AddedSlot:     12,
	}))

	inputs, err := store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 12, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 2)

	require.NoError(t, store.MarkUtxosDeletedAtSlot(
		nil,
		[]types.UtxoKey{{TxId: txA, OutputIdx: 0}},
		13,
	))
	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 13, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	stakes := map[string]uint64{}
	for _, input := range inputs {
		stakes[string([]byte{input.CredentialTag})+string(input.StakingKey)] = uint64(input.Stake)
	}
	require.Equal(t, uint64(7), stakes[string([]byte{1})+string(stakeB)])

	require.NoError(t, store.SetUtxosNotDeletedAfterSlot(12, nil))
	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 12, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 2)
	stakes = map[string]uint64{}
	for _, input := range inputs {
		stakes[string([]byte{input.CredentialTag})+string(input.StakingKey)] = uint64(input.Stake)
	}
	require.Equal(t, uint64(5), stakes[string([]byte{0})+string(stakeA)])

	require.NoError(t, store.DeleteUtxosAfterSlot(11, nil))
	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 11, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	stakes = map[string]uint64{}
	for _, input := range inputs {
		stakes[string([]byte{input.CredentialTag})+string(input.StakingKey)] = uint64(input.Stake)
	}
	require.Equal(t, uint64(5), stakes[string([]byte{0})+string(stakeA)])
}

func TestRewardLiveStakeExcludesInactiveAccounts(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestHash(0xaa)
	activeKey := rewardStateTestHash(0x01)
	inactiveKey := rewardStateTestHash(0x02)

	requireRewardStatePoolRegistration(t, store, poolA, 0)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: activeKey,
		Pool:       poolA,
		AddedSlot:  10,
		Active:     true,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: inactiveKey,
		Pool:       poolA,
		AddedSlot:  10,
		Active:     true,
	}).Error)
	require.NoError(t, store.DB().
		Model(&models.Account{}).
		Where("staking_key = ?", inactiveKey).
		Update("active", false).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       rewardStateTestTxHash(0x41),
		OutputIdx:  0,
		StakingKey: activeKey,
		Amount:     7,
		AddedSlot:  11,
	}).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       rewardStateTestTxHash(0x42),
		OutputIdx:  0,
		StakingKey: inactiveKey,
		Amount:     15,
		AddedSlot:  11,
	}).Error)
	require.NoError(t, store.RebuildRewardLiveStake(11, nil))

	inputs, err := store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 11, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, activeKey, inputs[0].StakingKey)
	require.Equal(t, uint64(7), uint64(inputs[0].Stake))
}

func TestRewardLiveStakeReactivationDoesNotReuseStaleDelegation(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestHash(0xaa)
	stakeA := rewardStateTestHash(0x03)

	require.NoError(t, store.DB().Create(&models.Account{
		StakingKey: stakeA,
		Pool:       poolA,
		AddedSlot:  10,
	}).Error)
	require.NoError(t, store.DB().
		Model(&models.Account{}).
		Where("staking_key = ?", stakeA).
		Update("active", false).Error)
	require.NoError(t, store.DB().Create(&models.Utxo{
		TxId:       rewardStateTestTxHash(0x43),
		OutputIdx:  0,
		StakingKey: stakeA,
		Amount:     7,
		AddedSlot:  11,
	}).Error)
	require.NoError(t, store.RebuildRewardLiveStake(11, nil))

	inputs, err := store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 11, nil)
	require.NoError(t, err)
	require.Empty(t, inputs)

	account, err := store.getOrCreateAccount(0, stakeA, nil)
	require.NoError(t, err)
	account.AddedSlot = 12
	require.NoError(t, saveAccount(account, store.DB()))

	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 12, nil)
	require.NoError(t, err)
	require.Empty(t, inputs)

	got, err := store.GetAccountByCredential(0, stakeA, true, nil)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.True(t, got.Active)
	require.Empty(t, got.Pool)
}

func TestRewardLiveStakeTracksTransactionRollbackInputRestore(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestHash(0xaa)
	stakeA := rewardStateTestHash(0x01)
	txA := rewardStateTestTxHash(0x31)
	spendTx := rewardStateTestTxHash(0x32)

	requireRewardStatePoolRegistration(t, store, poolA, 0)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey:    stakeA,
		CredentialTag: 0,
		Pool:          poolA,
		Active:        true,
		AddedSlot:     10,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TxId:          txA,
		OutputIdx:     0,
		StakingKey:    stakeA,
		CredentialTag: 0,
		Amount:        5,
		AddedSlot:     11,
	}))
	require.NoError(t, store.DB().Create(&models.Transaction{
		Hash: spendTx,
		Slot: 20,
	}).Error)
	require.NoError(t, store.DB().Model(&models.Utxo{}).
		Where("tx_id = ? AND output_idx = ?", txA, 0).
		Updates(map[string]any{
			"spent_at_tx_id": spendTx,
			"deleted_slot":   uint64(20),
		}).Error)
	require.NoError(t, store.RebuildRewardLiveStake(20, nil))

	inputs, err := store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 20, nil)
	require.NoError(t, err)
	require.Empty(t, inputs)

	require.NoError(t, store.DeleteTransactionsAfterSlot(10, nil))
	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 10, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(5), uint64(inputs[0].Stake))
}

func TestRewardLiveStakeTracksTransactionRollbackOutputDelete(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestHash(0xaa)
	stakeOutput := rewardStateTestHash(0x01)
	stakeCollateralReturn := rewardStateTestHash(0x02)
	txHash := rewardStateTestTxHash(0x41)

	requireRewardStatePoolRegistration(t, store, poolA, 0)
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey:    stakeOutput,
		CredentialTag: 0,
		Pool:          poolA,
		Active:        true,
		AddedSlot:     1,
	}))
	require.NoError(t, store.CreateAccount(nil, &models.Account{
		StakingKey:    stakeCollateralReturn,
		CredentialTag: 1,
		Pool:          poolA,
		Active:        true,
		AddedSlot:     1,
	}))
	tx := models.Transaction{
		Hash: txHash,
		Slot: 20,
	}
	require.NoError(t, store.DB().Create(&tx).Error)
	txID := tx.ID
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		TransactionID: &txID,
		TxId:          txHash,
		OutputIdx:     0,
		StakingKey:    stakeOutput,
		CredentialTag: 0,
		Amount:        5,
		AddedSlot:     20,
	}))
	require.NoError(t, store.CreateUtxo(nil, &models.Utxo{
		CollateralReturnForTxID: &txID,
		TxId:                    txHash,
		OutputIdx:               1,
		StakingKey:              stakeCollateralReturn,
		CredentialTag:           1,
		Amount:                  7,
		AddedSlot:               20,
	}))

	inputs, err := store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 20, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 2)
	stakes := make(map[string]uint64, len(inputs))
	for _, input := range inputs {
		ref := models.NewStakeCredentialRef(
			input.CredentialTag,
			input.StakingKey,
		)
		stakes[ref.MapKey()] = uint64(input.Stake)
	}
	require.Equal(t, uint64(5), stakes[models.NewStakeCredentialRef(0, stakeOutput).MapKey()])
	require.Equal(
		t,
		uint64(7),
		stakes[models.NewStakeCredentialRef(1, stakeCollateralReturn).MapKey()],
	)

	require.NoError(t, store.DeleteTransactionsAfterSlot(10, nil))

	inputs, err = store.GetRewardStakeInputsAtSlot([][]byte{poolA}, 10, nil)
	require.NoError(t, err)
	require.Empty(t, inputs)

	var remainingUtxos int64
	require.NoError(t, store.DB().Model(&models.Utxo{}).
		Where("tx_id = ?", txHash).
		Count(&remainingUtxos).Error)
	require.Zero(t, remainingUtxos)
}

func TestDeleteRewardStateAfterSlot(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestHash(0xaa)
	poolB := rewardStateTestHash(0xbb)

	require.NoError(t, store.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        1,
		CapturedSlot: 10,
	}, nil))
	require.NoError(t, store.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        2,
		CapturedSlot: 20,
	}, nil))
	require.NoError(t, store.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        3,
		Rewards:      33,
		CapturedSlot: 10,
	}, nil))
	require.NoError(t, store.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        4,
		Rewards:      44,
		CapturedSlot: 10,
	}, nil))
	require.NoError(t, store.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:        1,
		SnapshotType: "mark",
		CapturedSlot: 10,
		BoundarySlot: 11,
	}, nil))
	require.NoError(t, store.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:        2,
		SnapshotType: "mark",
		CapturedSlot: 14,
		BoundarySlot: 16,
	}, nil))
	require.NoError(t, store.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:          1,
			PoolKeyHash:    poolA,
			CapturedSlot:   10,
			BoundarySlot:   11,
			DelegatedStake: 1,
		},
		{
			Epoch:          2,
			PoolKeyHash:    poolB,
			CapturedSlot:   14,
			BoundarySlot:   16,
			DelegatedStake: 1,
		},
	}, nil))
	require.NoError(t, store.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:        1,
			PoolKeyHash:  poolA,
			TotalReward:  1,
			CapturedSlot: 10,
			BoundarySlot: 11,
		},
		{
			Epoch:        2,
			PoolKeyHash:  poolB,
			TotalReward:  1,
			CapturedSlot: 14,
			BoundarySlot: 16,
		},
	}, nil))
	require.NoError(t, store.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{
			Epoch:         1,
			CredentialTag: 0,
			StakingKey:    rewardStateTestHash(0x01),
			PoolKeyHash:   poolA,
			RewardType:    "member",
			Amount:        1,
			Spendable:     true,
			CapturedSlot:  10,
			BoundarySlot:  11,
		},
		{
			Epoch:         2,
			CredentialTag: 0,
			StakingKey:    rewardStateTestHash(0x02),
			PoolKeyHash:   poolB,
			RewardType:    "member",
			Amount:        1,
			Spendable:     true,
			CapturedSlot:  14,
			BoundarySlot:  16,
		},
	}, nil))

	require.NoError(t, store.DeleteRewardStateAfterSlot(15, nil))

	pots, err := store.GetRewardAdaPots(1, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	pots, err = store.GetRewardAdaPots(2, nil)
	require.NoError(t, err)
	require.Nil(t, pots)
	pots, err = store.GetRewardAdaPots(3, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	require.Equal(t, uint64(33), uint64(pots.Rewards))
	pots, err = store.GetRewardAdaPots(4, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	require.Equal(t, uint64(0), uint64(pots.Rewards))

	snapshot, err := store.GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	snapshot, err = store.GetRewardSnapshot(2, "mark", nil)
	require.NoError(t, err)
	require.Nil(t, snapshot)

	inputs, err := store.GetRewardPoolInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	inputs, err = store.GetRewardPoolInputs(2, nil)
	require.NoError(t, err)
	require.Empty(t, inputs)

	poolOutputs, err := store.GetRewardPoolOutputs(1, nil)
	require.NoError(t, err)
	require.Len(t, poolOutputs, 1)
	poolOutputs, err = store.GetRewardPoolOutputs(2, nil)
	require.NoError(t, err)
	require.Empty(t, poolOutputs)

	accountOutputs, err := store.GetRewardAccountOutputs(1, nil)
	require.NoError(t, err)
	require.Len(t, accountOutputs, 1)
	accountOutputs, err = store.GetRewardAccountOutputs(2, nil)
	require.NoError(t, err)
	require.Empty(t, accountOutputs)
}

func TestDeleteRewardStateBeforeEpoch(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	require.NoError(t, store.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        1,
		CapturedSlot: 10,
	}, nil))
	require.NoError(t, store.SaveRewardAdaPots(&models.RewardAdaPots{
		Epoch:        2,
		CapturedSlot: 20,
	}, nil))
	require.NoError(t, store.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:        1,
		SnapshotType: "mark",
		CapturedSlot: 10,
		BoundarySlot: 11,
	}, nil))
	require.NoError(t, store.SaveRewardSnapshot(&models.RewardSnapshot{
		Epoch:        2,
		SnapshotType: "mark",
		CapturedSlot: 20,
		BoundarySlot: 21,
	}, nil))
	require.NoError(t, store.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:          1,
			PoolKeyHash:    rewardStateTestHash(0xaa),
			CapturedSlot:   10,
			BoundarySlot:   11,
			DelegatedStake: 1,
		},
		{
			Epoch:          2,
			PoolKeyHash:    rewardStateTestHash(0xbb),
			CapturedSlot:   20,
			BoundarySlot:   21,
			DelegatedStake: 1,
		},
	}, nil))

	require.NoError(t, store.DeleteRewardStateBeforeEpoch(2, nil))

	pots, err := store.GetRewardAdaPots(1, nil)
	require.NoError(t, err)
	require.Nil(t, pots)
	pots, err = store.GetRewardAdaPots(2, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)

	snapshot, err := store.GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.Nil(t, snapshot)
	snapshot, err = store.GetRewardSnapshot(2, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, snapshot)

	inputs, err := store.GetRewardPoolInputs(1, nil)
	require.NoError(t, err)
	require.Empty(t, inputs)
	inputs, err = store.GetRewardPoolInputs(2, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
}

func TestGetPoolRegistrationsAtSlot(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolA := rewardStateTestHash(0xaa)
	poolB := rewardStateTestHash(0xbb)

	poolRows := []*models.Pool{
		{PoolKeyHash: poolA},
		{PoolKeyHash: poolB},
	}
	for _, pool := range poolRows {
		require.NoError(t, store.DB().Create(pool).Error)
	}

	require.NoError(t, store.DB().Create(&models.PoolRegistration{
		PoolID:      poolRows[0].ID,
		PoolKeyHash: poolA,
		AddedSlot:   10,
		Pledge:      1_000,
		Cost:        340,
		Margin:      &types.Rat{Rat: big.NewRat(1, 20)},
	}).Error)
	require.NoError(t, store.DB().Create(&models.PoolRegistration{
		PoolID:      poolRows[0].ID,
		PoolKeyHash: poolA,
		AddedSlot:   20,
		Pledge:      2_000,
		Cost:        500,
		Margin:      &types.Rat{Rat: big.NewRat(1, 10)},
	}).Error)
	require.NoError(t, store.DB().Create(&models.PoolRegistration{
		PoolID:      poolRows[1].ID,
		PoolKeyHash: poolB,
		AddedSlot:   40,
		Pledge:      3_000,
		Cost:        600,
		Margin:      &types.Rat{Rat: big.NewRat(3, 100)},
	}).Error)

	registrations, err := store.GetPoolRegistrationsAtSlot(
		[]lcommon.PoolKeyHash{
			rewardStateTestPoolKeyHash(0xaa),
			rewardStateTestPoolKeyHash(0xbb),
		},
		15,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, registrations, 1)
	require.Equal(t, poolA, registrations[0].PoolKeyHash)
	require.Equal(t, uint64(1_000), uint64(registrations[0].Pledge))
	require.Equal(t, uint64(340), uint64(registrations[0].Cost))
	require.Equal(t, "1/20", registrations[0].Margin.String())

	registrations, err = store.GetPoolRegistrationsAtSlot(
		[]lcommon.PoolKeyHash{rewardStateTestPoolKeyHash(0xaa)},
		20,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, registrations, 1)
	require.Equal(t, uint64(2_000), uint64(registrations[0].Pledge))
	require.Equal(t, uint64(500), uint64(registrations[0].Cost))
	require.Equal(t, "1/10", registrations[0].Margin.String())
}

func TestGetPoolRegistrationsAtSlotHydratesOwnersAcrossChunks(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	total := sqliteBindVarLimit + 1
	poolHashes := make([]lcommon.PoolKeyHash, 0, total)
	ownerByPool := make(map[string][]byte, total)
	for i := range total {
		var poolHash lcommon.PoolKeyHash
		binary.BigEndian.PutUint64(poolHash[20:], uint64(i+1))
		var ownerHash lcommon.PoolKeyHash
		ownerHash[0] = 0x99
		binary.BigEndian.PutUint64(ownerHash[20:], uint64(i+1))

		pool := &models.Pool{PoolKeyHash: poolHash[:]}
		require.NoError(t, store.DB().Create(pool).Error)
		registration := &models.PoolRegistration{
			PoolID:      pool.ID,
			PoolKeyHash: poolHash[:],
			AddedSlot:   10,
			Pledge:      1_000,
			Cost:        340,
			Margin:      &types.Rat{Rat: big.NewRat(1, 20)},
		}
		require.NoError(t, store.DB().Create(registration).Error)
		require.NoError(t, store.DB().Create(&models.PoolRegistrationOwner{
			PoolID:             pool.ID,
			PoolRegistrationID: registration.ID,
			KeyHash:            ownerHash[:],
		}).Error)

		poolHashes = append(poolHashes, poolHash)
		ownerByPool[string(poolHash[:])] = append([]byte(nil), ownerHash[:]...)
	}

	registrations, err := store.GetPoolRegistrationsAtSlot(
		poolHashes,
		10,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, registrations, total)
	for _, registration := range registrations {
		require.Len(t, registration.Owners, 1)
		require.Equal(
			t,
			ownerByPool[string(registration.PoolKeyHash)],
			registration.Owners[0].KeyHash,
		)
	}
}

func TestGetPoolRegistrationsAtSlotTieBreaksSameAddedSlot(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)
	poolKeyHash := rewardStateTestHash(0xcc)
	poolHash := rewardStateTestPoolKeyHash(0xcc)

	pools := []*models.Pool{
		{PoolKeyHash: rewardStateTestHash(0xc1)},
		{PoolKeyHash: rewardStateTestHash(0xc2)},
		{PoolKeyHash: rewardStateTestHash(0xc3)},
	}
	for _, pool := range pools {
		require.NoError(t, store.DB().Create(pool).Error)
	}

	txLowBlock := &models.Transaction{
		Hash:       rewardStateTestHash(0x11),
		Slot:       50,
		BlockIndex: 1,
		Valid:      true,
	}
	txHighBlock := &models.Transaction{
		Hash:       rewardStateTestHash(0x22),
		Slot:       50,
		BlockIndex: 2,
		Valid:      true,
	}
	txHighCert := &models.Transaction{
		Hash:       rewardStateTestHash(0x33),
		Slot:       50,
		BlockIndex: 2,
		Valid:      true,
	}
	require.NoError(t, store.DB().Create(txLowBlock).Error)
	require.NoError(t, store.DB().Create(txHighBlock).Error)
	require.NoError(t, store.DB().Create(txHighCert).Error)

	certLowBlock := &models.Certificate{
		TransactionID: txLowBlock.ID,
		Slot:          50,
		CertIndex:     99,
		CertType:      uint(lcommon.CertificateTypePoolRegistration),
	}
	certHighBlockLowCert := &models.Certificate{
		TransactionID: txHighBlock.ID,
		Slot:          50,
		CertIndex:     1,
		CertType:      uint(lcommon.CertificateTypePoolRegistration),
	}
	certHighBlockHighCert := &models.Certificate{
		TransactionID: txHighCert.ID,
		Slot:          50,
		CertIndex:     2,
		CertType:      uint(lcommon.CertificateTypePoolRegistration),
	}
	require.NoError(t, store.DB().Create(certLowBlock).Error)
	require.NoError(t, store.DB().Create(certHighBlockLowCert).Error)
	require.NoError(t, store.DB().Create(certHighBlockHighCert).Error)

	for _, reg := range []*models.PoolRegistration{
		{
			PoolID:        pools[0].ID,
			PoolKeyHash:   poolKeyHash,
			CertificateID: certLowBlock.ID,
			AddedSlot:     50,
			Pledge:        1_000,
			Cost:          100,
			Margin:        &types.Rat{Rat: big.NewRat(1, 100)},
		},
		{
			PoolID:        pools[1].ID,
			PoolKeyHash:   poolKeyHash,
			CertificateID: certHighBlockHighCert.ID,
			AddedSlot:     50,
			Pledge:        3_000,
			Cost:          300,
			Margin:        &types.Rat{Rat: big.NewRat(3, 100)},
		},
		{
			PoolID:        pools[2].ID,
			PoolKeyHash:   poolKeyHash,
			CertificateID: certHighBlockLowCert.ID,
			AddedSlot:     50,
			Pledge:        2_000,
			Cost:          200,
			Margin:        &types.Rat{Rat: big.NewRat(2, 100)},
		},
	} {
		require.NoError(t, store.DB().Create(reg).Error)
	}

	registrations, err := store.GetPoolRegistrationsAtSlot(
		[]lcommon.PoolKeyHash{poolHash},
		50,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, registrations, 1)
	require.Equal(t, uint64(3_000), uint64(registrations[0].Pledge))
	require.Equal(t, uint64(300), uint64(registrations[0].Cost))
	require.Equal(t, "3/100", registrations[0].Margin.String())
}

func rewardStateTestHash(fill byte) []byte {
	hash := make([]byte, 28)
	for i := range hash {
		hash[i] = fill
	}
	return hash
}

func requireRewardStatePoolRegistration(
	t *testing.T,
	store *MetadataStoreSqlite,
	poolKeyHash []byte,
	slot uint64,
) {
	t.Helper()
	pool := models.Pool{PoolKeyHash: append([]byte(nil), poolKeyHash...)}
	require.NoError(t, store.DB().
		Where("pool_key_hash = ?", poolKeyHash).
		FirstOrCreate(&pool).Error)
	require.NoError(t, store.DB().Create(&models.PoolRegistration{
		PoolID:      pool.ID,
		PoolKeyHash: append([]byte(nil), poolKeyHash...),
		AddedSlot:   slot,
	}).Error)
}

func rewardStateTestTxHash(fill byte) []byte {
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = fill
	}
	return hash
}

func rewardStateTestPoolKeyHash(fill byte) lcommon.PoolKeyHash {
	var hash lcommon.PoolKeyHash
	for i := range hash {
		hash[i] = fill
	}
	return hash
}

// TestGetPoolRegistrationsEffectiveForEpoch verifies the SNAP-time pool
// params selection: re-registrations during the ended epoch are future
// params and excluded, fresh registrations during the epoch take effect
// immediately (earliest cert), an executed retirement makes a later in-epoch
// registration fresh, and a cancelled retirement keeps the latest pre-epoch
// cert effective.
func TestGetPoolRegistrationsEffectiveForEpoch(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	// Ended epoch 5 spans slots [100, 199]; snapshot at slot 199.
	const (
		epochStartSlot = uint64(100)
		endedEpoch     = uint64(5)
		snapshotSlot   = uint64(199)
	)

	type regSpec struct {
		fill   byte
		slot   uint64
		pledge uint64
	}
	type retSpec struct {
		fill  byte
		slot  uint64
		epoch uint64
	}
	regs := []regSpec{
		// poolA: re-registered mid-epoch — future params, keep slot-10 cert.
		{0xa1, 10, 1_000},
		{0xa1, 150, 2_000},
		// poolB: fresh during the epoch — earliest in-epoch cert wins,
		// the second in-epoch cert is future params.
		{0xb2, 120, 3_000},
		{0xb2, 160, 4_000},
		// poolC: retirement executed at the boundary into epoch 4, then
		// re-registered during epoch 5 — fresh, in-epoch cert wins.
		{0xc3, 20, 5_000},
		{0xc3, 130, 6_000},
		// poolD: retirement cancelled by a later pre-epoch registration —
		// latest pre-epoch cert wins.
		{0xd4, 20, 7_000},
		{0xd4, 40, 8_000},
		// poolE: retirement scheduled for a future epoch — not executed,
		// pre-epoch cert wins.
		{0xe5, 20, 9_000},
	}
	rets := []retSpec{
		{0xc3, 30, 4},
		{0xd4, 25, 4},
		{0xe5, 90, 6},
	}

	poolIDs := make(map[byte]uint)
	for _, fill := range []byte{0xa1, 0xb2, 0xc3, 0xd4, 0xe5} {
		pool := &models.Pool{PoolKeyHash: rewardStateTestHash(fill)}
		require.NoError(t, store.DB().Create(pool).Error)
		poolIDs[fill] = pool.ID
	}
	for _, reg := range regs {
		require.NoError(t, store.DB().Create(&models.PoolRegistration{
			PoolID:      poolIDs[reg.fill],
			PoolKeyHash: rewardStateTestHash(reg.fill),
			AddedSlot:   reg.slot,
			Pledge:      types.Uint64(reg.pledge),
			Margin:      &types.Rat{Rat: big.NewRat(1, 100)},
		}).Error)
	}
	for _, ret := range rets {
		require.NoError(t, store.DB().Create(&models.PoolRetirement{
			PoolID:      poolIDs[ret.fill],
			PoolKeyHash: rewardStateTestHash(ret.fill),
			AddedSlot:   ret.slot,
			Epoch:       ret.epoch,
		}).Error)
	}

	registrations, err := store.GetPoolRegistrationsEffectiveForEpoch(
		[]lcommon.PoolKeyHash{
			rewardStateTestPoolKeyHash(0xa1),
			rewardStateTestPoolKeyHash(0xb2),
			rewardStateTestPoolKeyHash(0xc3),
			rewardStateTestPoolKeyHash(0xd4),
			rewardStateTestPoolKeyHash(0xe5),
		},
		epochStartSlot,
		endedEpoch,
		snapshotSlot,
		nil,
	)
	require.NoError(t, err)
	pledgeByPool := make(map[byte]uint64, len(registrations))
	for _, registration := range registrations {
		pledgeByPool[registration.PoolKeyHash[0]] = uint64(registration.Pledge)
	}
	require.Equal(t, map[byte]uint64{
		0xa1: 1_000,
		0xb2: 3_000,
		0xc3: 6_000,
		0xd4: 8_000,
		0xe5: 9_000,
	}, pledgeByPool)
}

func TestGetPoolRegistrationsEffectiveForEpochChunksSqliteBinds(t *testing.T) {
	t.Parallel()
	store := setupTestDB(t)

	total := sqliteBindVarLimit*2 + 1
	poolHashes := make([]lcommon.PoolKeyHash, 0, total)
	pledgeByPool := make(map[string]uint64, total)
	for i := range total {
		var poolHash lcommon.PoolKeyHash
		binary.BigEndian.PutUint64(poolHash[20:], uint64(i+1))
		pool := &models.Pool{PoolKeyHash: poolHash[:]}
		require.NoError(t, store.DB().Create(pool).Error)
		pledge := uint64(i + 1)
		require.NoError(t, store.DB().Create(&models.PoolRegistration{
			PoolID:      pool.ID,
			PoolKeyHash: poolHash[:],
			AddedSlot:   10,
			Pledge:      types.Uint64(pledge),
			Margin:      &types.Rat{Rat: big.NewRat(1, 100)},
		}).Error)
		poolHashes = append(poolHashes, poolHash)
		pledgeByPool[string(poolHash[:])] = pledge
	}

	registrations, err := store.GetPoolRegistrationsEffectiveForEpoch(
		poolHashes,
		100,
		5,
		199,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, registrations, total)
	for _, registration := range registrations {
		require.Equal(
			t,
			pledgeByPool[string(registration.PoolKeyHash)],
			uint64(registration.Pledge),
		)
	}
}
