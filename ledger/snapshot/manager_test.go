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

package snapshot

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
)

// TestCaptureGenesisSnapshot_PostMithril verifies that after a Mithril
// bootstrap (where slot 0 has no pools but later epochs exist), the
// snapshot manager seeds the Mark/Set/Go window for the current epoch.
// Without this, leader election at epoch N queries epoch N-2 and finds
// pool_stake=0.
func TestCaptureGenesisSnapshot_PostMithril(t *testing.T) {
	db, sqliteStore := setupTestDB(t)
	gormDB := sqliteStore.DB()

	// Simulate post-Mithril state: epoch 0 exists (from ledger state
	// import) but has no pool data at slot 0. The latest epoch is 150.
	for _, e := range []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
		{EpochId: 148, StartSlot: 63936000, LengthInSlots: 432000},
		{EpochId: 149, StartSlot: 64368000, LengthInSlots: 432000},
		{EpochId: 150, StartSlot: 64800000, LengthInSlots: 432000},
	} {
		require.NoError(t, gormDB.Create(&e).Error)
	}

	// Seed a pool registered at a post-Mithril slot with delegations
	poolHash := []byte("poolM_12345678901234567890AB")
	seedPoolAndDelegations(t, sqliteStore, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{
			stakingKey:  []byte("alice_staking_key_1234567890"),
			utxoAmounts: []types.Uint64{50000000},
		},
	}, 64800000) // registered at epoch 150's start slot

	eventBus := event.NewEventBus(nil, nil)
	mgr := NewManager(db, eventBus, nil)

	err := mgr.CaptureGenesisSnapshot(context.Background())
	require.NoError(t, err)

	// Leader election for epoch 150 queries epoch 148 (N-2).
	// Verify that mark snapshots exist for the full window.
	for _, epoch := range []uint64{0, 148, 149, 150} {
		snapshot, sErr := db.Metadata().GetPoolStakeSnapshot(
			epoch, "mark", poolHash, nil,
		)
		require.NoError(t, sErr, "epoch %d lookup should not error", epoch)
		require.NotNil(t, snapshot,
			"epoch %d must have a mark snapshot after Mithril bootstrap",
			epoch)
		require.NotZero(t, snapshot.TotalStake,
			"epoch %d snapshot must have non-zero stake", epoch)
	}
}

// TestCaptureGenesisSnapshot_FreshSync verifies that on a fresh sync
// (no Mithril), only epoch 0 gets a snapshot and no extra epochs are
// seeded.
func TestCaptureGenesisSnapshot_FreshSync(t *testing.T) {
	db, sqliteStore := setupTestDB(t)
	gormDB := sqliteStore.DB()

	// Fresh sync: only epoch 0 exists
	epoch := models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		LengthInSlots: 432000,
	}
	require.NoError(t, gormDB.Create(&epoch).Error)

	// Seed a pool at slot 0
	poolHash := []byte("poolG_12345678901234567890AB")
	seedPoolAndDelegations(t, sqliteStore, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{
			stakingKey:  []byte("bob___staking_key_1234567890"),
			utxoAmounts: []types.Uint64{10000000},
		},
	}, 0)

	eventBus := event.NewEventBus(nil, nil)
	mgr := NewManager(db, eventBus, nil)

	err := mgr.CaptureGenesisSnapshot(context.Background())
	require.NoError(t, err)

	// Epoch 0 should have a snapshot
	snapshot, err := db.Metadata().GetPoolStakeSnapshot(
		0, "mark", poolHash, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, snapshot, "epoch 0 must have a snapshot")
	require.NotZero(t, snapshot.TotalStake)

	// No spurious snapshots for epochs that don't exist
	snapshot2, err := db.Metadata().GetPoolStakeSnapshot(
		1, "mark", poolHash, nil,
	)
	require.NoError(t, err)
	require.Nil(t, snapshot2,
		"epoch 1 should not have a snapshot on fresh sync")
}

func TestHandleEpochTransitionPersistsRewardStateInputs(t *testing.T) {
	db, sqliteStore := setupTestDB(t)
	gormDB := sqliteStore.DB()

	require.NoError(t, gormDB.Create(&models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		LengthInSlots: 432000,
	}).Error)

	poolHash := []byte("poolR_12345678901234567890AB")
	seedPoolAndDelegations(t, sqliteStore, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{
			stakingKey:  []byte("reward_staking_key_123456789"),
			utxoAmounts: []types.Uint64{50_000_000},
		},
	}, 500)
	var pool models.Pool
	require.NoError(t, gormDB.Where("pool_key_hash = ?", poolHash).First(&pool).Error)
	require.NoError(t, gormDB.Create(&models.PoolRegistration{
		PoolID:      pool.ID,
		PoolKeyHash: poolHash,
		AddedSlot:   2000,
		Pledge:      2_000_000,
		Cost:        500_000_000,
		Margin:      &types.Rat{Rat: big.NewRat(1, 10)},
	}).Error)
	require.NoError(t, gormDB.Create(&models.PoolOpCertSequence{
		PoolKeyHash: poolHash,
		Slot:        600,
		Sequence:    1,
	}).Error)
	require.NoError(t, gormDB.Create(&models.PoolOpCertSequence{
		PoolKeyHash: []byte("other_12345678901234567890AB"),
		Slot:        700,
		Sequence:    1,
	}).Error)
	require.NoError(t, gormDB.Create(&models.PoolOpCertSequence{
		PoolKeyHash: poolHash,
		Slot:        432000,
		Sequence:    2,
	}).Error)

	eventBus := event.NewEventBus(nil, nil)
	mgr := NewManager(db, eventBus, nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432000,
		EpochNonce:      []byte{0x01, 0x02, 0x03},
		ProtocolVersion: 8,
		SnapshotSlot:    1000,
	}
	require.NoError(t, mgr.handleEpochTransition(context.Background(), evt))

	rewardSnapshot, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, rewardSnapshot)
	require.Equal(t, uint64(50_000_000), uint64(rewardSnapshot.TotalActiveStake))
	require.Equal(t, uint64(1), rewardSnapshot.TotalPoolCount)
	require.Equal(t, uint64(1), rewardSnapshot.TotalDelegators)
	require.Equal(t, uint64(1000), rewardSnapshot.CapturedSlot)
	require.Equal(t, uint64(432000), rewardSnapshot.BoundarySlot)
	require.Equal(t, []byte{0x01, 0x02, 0x03}, rewardSnapshot.EpochNonce)
	require.Equal(t, uint(8), rewardSnapshot.ProtocolVersion)

	inputs, err := db.Metadata().GetRewardPoolInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, poolHash, inputs[0].PoolKeyHash)
	require.NotNil(t, inputs[0].BlocksProduced)
	require.Equal(t, uint64(1), *inputs[0].BlocksProduced)
	require.NotNil(t, inputs[0].TotalBlocksInEpoch)
	require.Equal(t, uint64(2), *inputs[0].TotalBlocksInEpoch)
	require.Equal(t, uint64(1_000_000), uint64(inputs[0].Pledge))
	require.Equal(t, uint64(50_000_000), uint64(inputs[0].DelegatedStake))
	require.Equal(t, uint64(340_000_000), uint64(inputs[0].Cost))
	require.Equal(t, "1/100", inputs[0].Margin.String())
	require.Equal(t, uint64(1), inputs[0].DelegatorCount)
	require.Equal(t, uint64(1000), inputs[0].CapturedSlot)
	require.Equal(t, uint64(432000), inputs[0].BoundarySlot)
}

// TestCaptureGenesisSnapshot_NoPools verifies that when no pools exist
// at all, no snapshots are created and no error is returned.
func TestCaptureGenesisSnapshot_NoPools(t *testing.T) {
	db, sqliteStore := setupTestDB(t)
	gormDB := sqliteStore.DB()

	// Epochs exist but no pools
	for _, e := range []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
		{EpochId: 10, StartSlot: 4320000, LengthInSlots: 432000},
	} {
		require.NoError(t, gormDB.Create(&e).Error)
	}

	eventBus := event.NewEventBus(nil, nil)
	mgr := NewManager(db, eventBus, nil)

	err := mgr.CaptureGenesisSnapshot(context.Background())
	require.NoError(t, err, "no pools should not be an error")
}

// TestCaptureGenesisSnapshot_SmallEpoch verifies correct behavior when
// the current epoch is less than 2 (edge case for the offset loop).
func TestCaptureGenesisSnapshot_SmallEpoch(t *testing.T) {
	db, sqliteStore := setupTestDB(t)
	gormDB := sqliteStore.DB()

	// Simulate Mithril bootstrap to epoch 1 (no pools at slot 0)
	for _, e := range []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
		{EpochId: 1, StartSlot: 432000, LengthInSlots: 432000},
	} {
		require.NoError(t, gormDB.Create(&e).Error)
	}

	poolHash := []byte("poolS_12345678901234567890AB")
	seedPoolAndDelegations(t, sqliteStore, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{
			stakingKey:  []byte("eve___staking_key_1234567890"),
			utxoAmounts: []types.Uint64{25000000},
		},
	}, 432000) // registered at epoch 1

	eventBus := event.NewEventBus(nil, nil)
	mgr := NewManager(db, eventBus, nil)

	err := mgr.CaptureGenesisSnapshot(context.Background())
	require.NoError(t, err)

	// Epoch 0 and 1 should both have snapshots
	for _, epoch := range []uint64{0, 1} {
		snapshot, sErr := db.Metadata().GetPoolStakeSnapshot(
			epoch, "mark", poolHash, nil,
		)
		require.NoError(t, sErr, "epoch %d lookup", epoch)
		require.NotNil(t, snapshot,
			"epoch %d must have a snapshot", epoch)
	}
}
