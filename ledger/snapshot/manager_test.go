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

// TestCaptureGenesisSnapshot_PostMithrilAutoVoteFlagOnlyOnCurrentEpoch
// asserts the CIP-1694 reward-account auto-vote resolution gate on
// the post-Mithril seeding loop: live Pool/Account state at bootstrap
// time matches only the current epoch's boundary, so only the
// currentEpochId mark row should land with
// RewardAccountAutoVoteResolved=true. The N-1 and N-2 mark rows seeded
// in the same loop represent older boundaries — resolving them would
// freeze today's delegation map into a historical snapshot, so those
// rows must keep Resolved=false and the tally must treat them as
// implicit no.
func TestCaptureGenesisSnapshot_PostMithrilAutoVoteFlagOnlyOnCurrentEpoch(t *testing.T) {
	db, sqliteStore := setupTestDB(t)
	gormDB := sqliteStore.DB()

	for _, e := range []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
		{EpochId: 148, StartSlot: 63936000, LengthInSlots: 432000},
		{EpochId: 149, StartSlot: 64368000, LengthInSlots: 432000},
		{EpochId: 150, StartSlot: 64800000, LengthInSlots: 432000},
	} {
		require.NoError(t, gormDB.Create(&e).Error)
	}

	poolHash := []byte("poolM_12345678901234567890CD")
	rewardAccount := []byte("rewardCD_12345678901234567890")
	seedPoolAndDelegations(t, sqliteStore, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{
			stakingKey:  []byte("aliceCD_staking_key_1234567890"),
			utxoAmounts: []types.Uint64{50000000},
		},
	}, 64800000)

	// Overwrite the pool's reward account to a credential we control
	// and seed an AlwaysAbstain delegation for that credential. The
	// resolver, if it runs, will produce Abstain. We then verify it
	// only ran for the currentEpoch row.
	require.NoError(t, gormDB.Model(&models.Pool{}).
		Where("pool_key_hash = ?", poolHash).
		Update("reward_account", rewardAccount).Error)
	require.NoError(t, gormDB.Create(&models.Account{
		StakingKey: rewardAccount,
		DrepType:   models.DrepTypeAlwaysAbstain,
		AddedSlot:  64800000,
		Active:     true,
	}).Error)

	eventBus := event.NewEventBus(nil, nil)
	mgr := NewManager(db, eventBus, nil)

	require.NoError(t, mgr.CaptureGenesisSnapshot(context.Background()))

	cases := []struct {
		epoch        uint64
		wantResolved bool
		wantAutoVote uint8
		note         string
	}{
		{
			epoch:        150,
			wantResolved: true,
			wantAutoVote: models.PoolRewardAccountAutoVoteAbstain,
			note:         "current epoch: live state == boundary, resolver runs",
		},
		{
			epoch:        149,
			wantResolved: false,
			wantAutoVote: models.PoolRewardAccountAutoVoteNone,
			note:         "N-1 seed: live state too new, resolver skipped",
		},
		{
			epoch:        148,
			wantResolved: false,
			wantAutoVote: models.PoolRewardAccountAutoVoteNone,
			note:         "N-2 seed: live state too new, resolver skipped",
		},
		{
			epoch:        0,
			wantResolved: false,
			wantAutoVote: models.PoolRewardAccountAutoVoteNone,
			note: "epoch 0 in post-Mithril bootstrap is a historical " +
				"boundary, not the current one — live state must not " +
				"be frozen onto the genesis row",
		},
	}
	for _, tc := range cases {
		snapshot, sErr := db.Metadata().GetPoolStakeSnapshot(
			tc.epoch, "mark", poolHash, nil,
		)
		require.NoError(t, sErr, "epoch %d", tc.epoch)
		require.NotNil(t, snapshot, "epoch %d snapshot must exist", tc.epoch)
		require.Equal(
			t, tc.wantResolved, snapshot.RewardAccountAutoVoteResolved,
			"epoch %d (%s): RewardAccountAutoVoteResolved mismatch",
			tc.epoch, tc.note,
		)
		require.Equal(
			t, tc.wantAutoVote, snapshot.RewardAccountAutoVote,
			"epoch %d (%s): RewardAccountAutoVote mismatch",
			tc.epoch, tc.note,
		)
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
	// Fresh-sync: live state IS the genesis boundary, so the
	// CIP-1694 reward-account auto-vote resolver runs and the row
	// must come out flagged Resolved. Symmetric with the
	// post-Mithril case where epoch 0 stays unresolved.
	require.True(
		t,
		snapshot.RewardAccountAutoVoteResolved,
		"fresh-sync epoch 0 row must be resolved",
	)

	// No spurious snapshots for epochs that don't exist
	snapshot2, err := db.Metadata().GetPoolStakeSnapshot(
		1, "mark", poolHash, nil,
	)
	require.NoError(t, err)
	require.Nil(t, snapshot2,
		"epoch 1 should not have a snapshot on fresh sync")
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
