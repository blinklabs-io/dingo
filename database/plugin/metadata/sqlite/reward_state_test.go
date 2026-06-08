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
			Epoch:              42,
			PoolKeyHash:        poolA,
			BlocksProduced:     &blocksProduced,
			TotalBlocksInEpoch: &totalBlocks,
			Pledge:             1_000,
			DelegatedStake:     2_000,
			Cost:               340,
			Margin:             &types.Rat{Rat: big.NewRat(1, 20)},
			DelegatorCount:     10,
			CapturedSlot:       12344,
			BoundarySlot:       12345,
		},
		{
			Epoch:          42,
			PoolKeyHash:    poolB,
			Pledge:         3_000,
			DelegatedStake: 4_000,
			Cost:           500,
			Margin:         &types.Rat{Rat: big.NewRat(3, 100)},
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
	require.Equal(t, uint64(340), uint64(got[0].Cost))
	require.Equal(t, "1/20", got[0].Margin.String())
	require.Equal(t, uint64(10), got[0].DelegatorCount)
	require.Nil(t, got[1].BlocksProduced)
	require.Nil(t, got[1].TotalBlocksInEpoch)

	updatedBlocksProduced := uint64(13)
	require.NoError(t, store.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:              42,
			PoolKeyHash:        poolA,
			BlocksProduced:     &updatedBlocksProduced,
			TotalBlocksInEpoch: &totalBlocks,
			Pledge:             1_100,
			DelegatedStake:     2_200,
			Cost:               350,
			Margin:             &types.Rat{Rat: big.NewRat(1, 10)},
			DelegatorCount:     11,
			CapturedSlot:       12346,
			BoundarySlot:       12347,
		},
	}, nil))

	got, err = store.GetRewardPoolInputs(42, nil)
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.NotNil(t, got[0].BlocksProduced)
	require.Equal(t, uint64(13), *got[0].BlocksProduced)
	require.Equal(t, uint64(1_100), uint64(got[0].Pledge))
	require.Equal(t, uint64(2_200), uint64(got[0].DelegatedStake))
	require.Equal(t, uint64(350), uint64(got[0].Cost))
	require.Equal(t, "1/10", got[0].Margin.String())
	require.Equal(t, uint64(11), got[0].DelegatorCount)
	require.Equal(t, uint64(12346), got[0].CapturedSlot)
	require.Equal(t, uint64(12347), got[0].BoundarySlot)

	got, err = store.GetRewardPoolInputs(99, nil)
	require.NoError(t, err)
	require.Empty(t, got)
	require.NoError(t, store.SaveRewardPoolInputs(nil, nil))
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

	require.NoError(t, store.DeleteRewardStateAfterSlot(15, nil))

	pots, err := store.GetRewardAdaPots(1, nil)
	require.NoError(t, err)
	require.NotNil(t, pots)
	pots, err = store.GetRewardAdaPots(2, nil)
	require.NoError(t, err)
	require.Nil(t, pots)

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

func rewardStateTestPoolKeyHash(fill byte) lcommon.PoolKeyHash {
	var hash lcommon.PoolKeyHash
	for i := range hash {
		hash[i] = fill
	}
	return hash
}
