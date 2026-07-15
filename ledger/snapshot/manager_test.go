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
	"bytes"
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// TestCaptureGenesisSnapshot_PostMithril verifies that after a Mithril
// bootstrap (where slot 0 has no pools but later epochs exist), the
// snapshot manager seeds the recent historical window for the current epoch.
func TestCaptureGenesisSnapshot_PostMithril(t *testing.T) {
	db := setupTestDB(t)

	// Simulate post-Mithril state: epoch 0 exists (from ledger state
	// import) but has no pool data at slot 0. The latest epoch is 150.
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
		{EpochId: 148, StartSlot: 63936000, LengthInSlots: 432000},
		{EpochId: 149, StartSlot: 64368000, LengthInSlots: 432000},
		{EpochId: 150, StartSlot: 64800000, LengthInSlots: 432000},
	})

	// Seed a pool registered at a post-Mithril slot with delegations
	poolHash := []byte("poolM_12345678901234567890AB")
	seedPoolAndDelegations(t, db, poolHash, []struct {
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

	// Verify that mark snapshots exist for the full recent window.
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

	// Historical post-Mithril seed rows are stake snapshots only: without
	// a historical backfill, copying the live reward aggregate into older
	// epochs would create bogus reward-calculation inputs.
	for _, epoch := range []uint64{0, 148, 149} {
		rewardSnapshot, sErr := db.Metadata().GetRewardSnapshot(
			epoch, "mark", nil,
		)
		require.NoError(t, sErr, "epoch %d reward snapshot lookup", epoch)
		require.Nil(t, rewardSnapshot,
			"epoch %d must not persist historical reward inputs", epoch)

		poolInputs, pErr := db.Metadata().GetRewardPoolInputs(epoch, nil)
		require.NoError(t, pErr, "epoch %d reward pool inputs", epoch)
		require.Empty(t, poolInputs,
			"epoch %d must not persist historical pool inputs", epoch)

		stakeInputs, iErr := db.Metadata().GetRewardStakeInputs(epoch, nil)
		require.NoError(t, iErr, "epoch %d reward stake inputs", epoch)
		require.Empty(t, stakeInputs,
			"epoch %d must not persist historical stake inputs", epoch)
	}

	rewardSnapshot, err := db.Metadata().GetRewardSnapshot(150, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, rewardSnapshot,
		"current post-Mithril seed row keeps reward snapshot inputs")
	poolInputs, err := db.Metadata().GetRewardPoolInputs(150, nil)
	require.NoError(t, err)
	require.NotEmpty(t, poolInputs,
		"current post-Mithril seed row keeps pool reward inputs")
	stakeInputs, err := db.Metadata().GetRewardStakeInputs(150, nil)
	require.NoError(t, err)
	require.NotEmpty(t, stakeInputs,
		"current post-Mithril seed row keeps stake reward inputs")
}

func TestCaptureGenesisSnapshot_PostMithrilSkipsExistingWindow(t *testing.T) {
	db := setupTestDB(t)

	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
		{EpochId: 148, StartSlot: 63936000, LengthInSlots: 432000},
		{EpochId: 149, StartSlot: 64368000, LengthInSlots: 432000},
		{EpochId: 150, StartSlot: 64800000, LengthInSlots: 432000},
	})

	poolHash := []byte("poolM_12345678901234567890EF")
	seedPoolAndDelegations(t, db, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{
			stakingKey:  []byte("aliceEF_staking_key_12345678"),
			utxoAmounts: []types.Uint64{50000000},
		},
	}, 64800000)

	for _, epoch := range []uint64{148, 149, 150} {
		require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
			&models.PoolStakeSnapshot{
				Epoch:          epoch,
				SnapshotType:   "mark",
				PoolKeyHash:    poolHash,
				TotalStake:     types.Uint64(50000000),
				DelegatorCount: 1,
				CapturedSlot:   64800000,
			},
			nil,
		))
	}

	eventBus := event.NewEventBus(nil, nil)
	mgr := NewManager(db, eventBus, nil)

	require.NoError(t, mgr.CaptureGenesisSnapshot(context.Background()))

	snapshot, err := db.Metadata().GetPoolStakeSnapshot(
		0,
		"mark",
		poolHash,
		nil,
	)
	require.NoError(t, err)
	require.Nil(t, snapshot, "existing post-Mithril window should skip epoch-0 reseed")
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
	db := setupTestDB(t)

	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
		{EpochId: 148, StartSlot: 63936000, LengthInSlots: 432000},
		{EpochId: 149, StartSlot: 64368000, LengthInSlots: 432000},
		{EpochId: 150, StartSlot: 64800000, LengthInSlots: 432000},
	})

	poolHash := []byte("poolM_12345678901234567890CD")
	rewardAccount := bytes.Repeat([]byte{0xcd}, 28)
	seedPoolAndDelegationsWithRewardAccount(t, db, poolHash, rewardAccount, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{
			stakingKey:  bytes.Repeat([]byte{0xac}, 28),
			utxoAmounts: []types.Uint64{50000000},
		},
	}, 64800000)

	// Seed an AlwaysAbstain delegation for the pool reward account. The
	// resolver, if it runs, will produce Abstain. We then verify it only
	// ran for the currentEpoch row.
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: rewardAccount,
		DrepType:   models.DrepTypeAlwaysAbstain,
		AddedSlot:  64800000,
		Active:     true,
	}))

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
	db := setupTestDB(t)

	// Fresh sync: only epoch 0 exists
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	// Seed a pool at slot 0
	poolHash := []byte("poolG_12345678901234567890AB")
	seedPoolAndDelegations(t, db, poolHash, []struct {
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
	rewardSnapshot, err := db.Metadata().GetRewardSnapshot(0, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, rewardSnapshot,
		"fresh-sync epoch 0 row must persist reward snapshot inputs")
	poolInputs, err := db.Metadata().GetRewardPoolInputs(0, nil)
	require.NoError(t, err)
	require.NotEmpty(t, poolInputs,
		"fresh-sync epoch 0 row must persist pool reward inputs")
	stakeInputs, err := db.Metadata().GetRewardStakeInputs(0, nil)
	require.NoError(t, err)
	require.NotEmpty(t, stakeInputs,
		"fresh-sync epoch 0 row must persist stake reward inputs")

	// No spurious snapshots for epochs that don't exist
	snapshot2, err := db.Metadata().GetPoolStakeSnapshot(
		1, "mark", poolHash, nil,
	)
	require.NoError(t, err)
	require.Nil(t, snapshot2,
		"epoch 1 should not have a snapshot on fresh sync")
}

func TestHandleEpochTransitionPersistsRewardStateInputs(t *testing.T) {
	db := setupTestDB(t)

	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	poolHash := []byte("poolR_12345678901234567890AB")
	seedPoolAndDelegations(t, db, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{
			stakingKey:  []byte("reward_staking_key_123456789"),
			utxoAmounts: []types.Uint64{50_000_000},
		},
	}, 500)
	gormDB := snapshotGormDB(t, db)
	var pool models.Pool
	require.NoError(t, gormDB.Where("pool_key_hash = ?", poolHash).First(&pool).Error)
	var effectiveRegistration models.PoolRegistration
	require.NoError(t, gormDB.Where(
		"pool_id = ? AND added_slot = ?", pool.ID, 500,
	).First(&effectiveRegistration).Error)
	rewardAccount := []byte("reward_account_1234567890123")
	require.Len(t, rewardAccount, 28)
	require.NoError(t, gormDB.Model(&effectiveRegistration).Updates(map[string]any{
		"reward_account":                rewardAccount,
		"reward_account_credential_tag": uint8(1),
	}).Error)
	require.NoError(t, gormDB.Create(&models.PoolRegistrationOwner{
		PoolRegistrationID: effectiveRegistration.ID,
		PoolID:             pool.ID,
		KeyHash:            []byte("reward_staking_key_123456789"),
	}).Error)
	// Reward balance is part of the historical Mark stake and therefore of
	// owner stake as well.
	require.NoError(t, db.AddAccountRewardByCredential(
		0,
		[]byte("reward_staking_key_123456789"),
		7_000_000,
		600,
		nil,
		nil,
	))
	require.NoError(t, gormDB.Create(&models.PoolRegistration{
		PoolID:      pool.ID,
		PoolKeyHash: poolHash,
		// This in-epoch re-registration is future parameters and must not
		// replace the registration active at the start of epoch 0.
		AddedSlot: 750,
		Pledge:    2_000_000,
		Cost:      500_000_000,
		Margin:    &types.Rat{Rat: big.NewRat(1, 10)},
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
	require.Equal(t, uint64(57_000_000), uint64(rewardSnapshot.TotalActiveStake))
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
	require.Equal(t, uint64(57_000_000), uint64(inputs[0].DelegatedStake))
	require.Equal(t, uint64(57_000_000), uint64(inputs[0].OwnerStake))
	require.Equal(t, rewardAccount, inputs[0].RewardAccount)
	require.Equal(t, uint8(1), inputs[0].RewardAccountCredentialTag)
	require.Equal(t, uint64(340_000_000), uint64(inputs[0].Cost))
	require.Equal(t, "1/100", inputs[0].Margin.String())
	require.Equal(t, uint64(1), inputs[0].DelegatorCount)
	require.Equal(t, uint64(1000), inputs[0].CapturedSlot)
	require.Equal(t, uint64(432000), inputs[0].BoundarySlot)
}

func TestCaptureMarkSnapshotReplacesPriorPoolSet(t *testing.T) {
	db := setupTestDB(t)
	require.NoError(t, snapshotGormDB(t, db).Create(&models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		LengthInSlots: 1_000,
	}).Error)
	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:          7,
			SnapshotType:   models.PoolStakeSnapshotTypeMark,
			PoolKeyHash:    make([]byte, 28),
			TotalStake:     1,
			DelegatorCount: 1,
			CapturedSlot:   100,
		}, nil,
	))

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	require.NoError(t, mgr.captureMarkSnapshot(
		context.Background(),
		event.EpochTransitionEvent{
			NewEpoch:     7,
			SnapshotSlot: 200,
			BoundarySlot: 200,
		},
	))

	rows, err := db.Metadata().GetPoolStakeSnapshotsByEpoch(
		7, models.PoolStakeSnapshotTypeMark, nil,
	)
	require.NoError(t, err)
	require.Empty(t, rows)
}

func TestHandleEpochTransitionCapturesSelfDelegatedOwnerStake(t *testing.T) {
	db := setupTestDB(t)

	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	poolHash := bytes.Repeat([]byte{0x11}, 28)
	ownerKey := bytes.Repeat([]byte{0x22}, 28)
	memberKey := bytes.Repeat([]byte{0x33}, 28)
	rewardAccount := bytes.Repeat([]byte{0x44}, 28)
	require.NoError(t, db.ImportPool(
		nil,
		&models.Pool{
			PoolKeyHash: poolHash,
			VrfKeyHash:  bytes.Repeat([]byte{0x44}, 32),
			Pledge:      60,
			Cost:        5,
			Margin:      &types.Rat{Rat: big.NewRat(1, 20)},
		},
		&models.PoolRegistration{
			PoolKeyHash:                poolHash,
			VrfKeyHash:                 bytes.Repeat([]byte{0x44}, 32),
			AddedSlot:                  500,
			Pledge:                     60,
			Cost:                       5,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 20)},
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Owners: []models.PoolRegistrationOwner{
				{KeyHash: ownerKey},
			},
		},
	))
	for _, account := range []models.Account{
		{
			StakingKey: ownerKey,
			Pool:       poolHash,
			AddedSlot:  500,
			Active:     true,
		},
		{
			StakingKey: memberKey,
			Pool:       poolHash,
			AddedSlot:  500,
			Active:     true,
		},
	} {
		require.NoError(t, db.CreateAccount(nil, &account))
	}
	for i, utxo := range []models.Utxo{
		{
			TxId:       bytes.Repeat([]byte{0x55}, 32),
			OutputIdx:  0,
			StakingKey: ownerKey,
			Amount:     70,
			AddedSlot:  500,
		},
		{
			TxId:       bytes.Repeat([]byte{0x66}, 32),
			OutputIdx:  0,
			StakingKey: memberKey,
			Amount:     30,
			AddedSlot:  500,
		},
	} {
		utxo.OutputIdx = uint32(i)
		require.NoError(t, db.CreateUtxo(nil, &utxo))
	}

	eventBus := event.NewEventBus(nil, nil)
	mgr := NewManager(db, eventBus, nil)
	require.NoError(t, mgr.handleEpochTransition(
		context.Background(),
		event.EpochTransitionEvent{
			PreviousEpoch:   0,
			NewEpoch:        1,
			BoundarySlot:    432000,
			ProtocolVersion: 8,
			SnapshotSlot:    1000,
		},
	))

	poolInputs, err := db.Metadata().GetRewardPoolInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, poolInputs, 1)
	require.Equal(t, uint64(100), uint64(poolInputs[0].DelegatedStake))
	require.Equal(t, uint64(70), uint64(poolInputs[0].OwnerStake))
	require.Equal(t, uint64(2), poolInputs[0].DelegatorCount)

	stakeInputs, err := db.Metadata().GetRewardStakeInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, stakeInputs, 2)
	ownerRows := 0
	for _, input := range stakeInputs {
		if bytes.Equal(input.StakingKey, ownerKey) {
			require.True(t, input.Owner)
			require.Equal(t, uint64(70), uint64(input.Stake))
			ownerRows++
			continue
		}
		require.False(t, input.Owner)
		require.Equal(t, memberKey, input.StakingKey)
		require.Equal(t, uint64(30), uint64(input.Stake))
	}
	require.Equal(t, 1, ownerRows)
}

func TestHandleEpochTransitionDoesNotTreatScriptCredentialAsOwner(t *testing.T) {
	db := setupTestDB(t)

	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	poolHash := bytes.Repeat([]byte{0x11}, 28)
	ownerHash := bytes.Repeat([]byte{0x22}, 28)
	rewardAccount := bytes.Repeat([]byte{0x33}, 28)
	require.NoError(t, db.ImportPool(
		nil,
		&models.Pool{
			PoolKeyHash: poolHash,
			VrfKeyHash:  bytes.Repeat([]byte{0x44}, 32),
			Pledge:      60,
			Cost:        5,
			Margin:      &types.Rat{Rat: big.NewRat(1, 20)},
		},
		&models.PoolRegistration{
			PoolKeyHash:                poolHash,
			VrfKeyHash:                 bytes.Repeat([]byte{0x44}, 32),
			AddedSlot:                  500,
			Pledge:                     60,
			Cost:                       5,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 20)},
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 0,
			Owners: []models.PoolRegistrationOwner{
				{KeyHash: ownerHash},
			},
		},
	))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		CredentialTag: 1,
		StakingKey:    ownerHash,
		Pool:          poolHash,
		AddedSlot:     500,
		Active:        true,
	}))
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:          bytes.Repeat([]byte{0x55}, 32),
		OutputIdx:     0,
		CredentialTag: 1,
		StakingKey:    ownerHash,
		Amount:        90,
		AddedSlot:     500,
	}))

	eventBus := event.NewEventBus(nil, nil)
	mgr := NewManager(db, eventBus, nil)
	require.NoError(t, mgr.handleEpochTransition(
		context.Background(),
		event.EpochTransitionEvent{
			PreviousEpoch:   0,
			NewEpoch:        1,
			BoundarySlot:    432000,
			ProtocolVersion: 8,
			SnapshotSlot:    1000,
		},
	))

	poolInputs, err := db.Metadata().GetRewardPoolInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, poolInputs, 1)
	require.Equal(t, uint64(90), uint64(poolInputs[0].DelegatedStake))
	require.Equal(t, uint64(0), uint64(poolInputs[0].OwnerStake))
	require.Equal(t, uint64(1), poolInputs[0].DelegatorCount)

	stakeInputs, err := db.Metadata().GetRewardStakeInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, stakeInputs, 1)
	require.Equal(t, uint8(1), stakeInputs[0].CredentialTag)
	require.Equal(t, ownerHash, stakeInputs[0].StakingKey)
	require.False(t, stakeInputs[0].Owner)
	require.Equal(t, uint64(90), uint64(stakeInputs[0].Stake))
}

func TestValidateRewardStakeInputTotals(t *testing.T) {
	var poolA lcommon.PoolKeyHash
	var poolB lcommon.PoolKeyHash
	copy(poolA[:], bytes.Repeat([]byte{0x11}, len(poolA)))
	copy(poolB[:], bytes.Repeat([]byte{0x22}, len(poolB)))
	stakeA := bytes.Repeat([]byte{0x31}, len(poolA))
	stakeB := bytes.Repeat([]byte{0x32}, len(poolA))
	stakeC := bytes.Repeat([]byte{0x33}, len(poolA))

	t.Run("valid", func(t *testing.T) {
		err := validateRewardStakeInputTotals(&StakeDistribution{
			PoolStakes: map[lcommon.PoolKeyHash]uint64{
				poolA: 100,
				poolB: 50,
			},
			StakeInputs: []StakeInput{
				{PoolKeyHash: poolA[:], StakingKey: stakeA, Stake: 40},
				{PoolKeyHash: poolA[:], StakingKey: stakeB, Stake: 60},
				{PoolKeyHash: poolB[:], StakingKey: stakeC, Stake: 50},
			},
		})
		require.NoError(t, err)
	})

	t.Run("mismatch", func(t *testing.T) {
		err := validateRewardStakeInputTotals(&StakeDistribution{
			PoolStakes: map[lcommon.PoolKeyHash]uint64{
				poolA: 100,
			},
			StakeInputs: []StakeInput{
				{PoolKeyHash: poolA[:], StakingKey: stakeA, Stake: 99},
			},
		})
		require.ErrorContains(t, err, "reward stake input total mismatch")
	})

	t.Run("unknown pool", func(t *testing.T) {
		err := validateRewardStakeInputTotals(&StakeDistribution{
			PoolStakes: map[lcommon.PoolKeyHash]uint64{
				poolA: 100,
			},
			StakeInputs: []StakeInput{
				{PoolKeyHash: poolA[:], StakingKey: stakeA, Stake: 100},
				{PoolKeyHash: poolB[:], StakingKey: stakeB, Stake: 1},
			},
		})
		require.ErrorContains(t, err, "unknown pool")
	})

	t.Run("invalid credential length", func(t *testing.T) {
		err := validateRewardStakeInputTotals(&StakeDistribution{
			PoolStakes: map[lcommon.PoolKeyHash]uint64{
				poolA: 100,
			},
			StakeInputs: []StakeInput{
				{PoolKeyHash: poolA[:], StakingKey: stakeA[:27], Stake: 100},
			},
		})
		require.ErrorContains(t, err, "invalid reward stake input credential length")
	})

	t.Run("invalid credential tag", func(t *testing.T) {
		err := validateRewardStakeInputTotals(&StakeDistribution{
			PoolStakes: map[lcommon.PoolKeyHash]uint64{
				poolA: 100,
			},
			StakeInputs: []StakeInput{
				{
					PoolKeyHash:   poolA[:],
					CredentialTag: 2,
					StakingKey:    stakeA,
					Stake:         100,
				},
			},
		})
		require.ErrorContains(t, err, "invalid reward stake input credential tag")
	})
}

func TestRewardInputsRejectMissingPoolRegistration(t *testing.T) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})
	var poolKey lcommon.PoolKeyHash
	copy(poolKey[:], bytes.Repeat([]byte{0x11}, len(poolKey)))
	stakeKey := bytes.Repeat([]byte{0x31}, len(poolKey))

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	_, _, err := mgr.rewardInputs(
		1,
		&StakeDistribution{
			Slot: 100,
			PoolStakes: map[lcommon.PoolKeyHash]uint64{
				poolKey: 100,
			},
			StakeInputs: []StakeInput{
				{
					PoolKeyHash: poolKey[:],
					StakingKey:  stakeKey,
					Stake:       100,
					Registered:  true,
				},
			},
		},
		event.EpochTransitionEvent{BoundarySlot: 200},
		db.Metadata(),
		nil,
	)
	require.ErrorContains(t, err, "missing pool registration")
}

func TestRewardInputsRejectInvalidRewardAccountLength(t *testing.T) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})
	poolHash := bytes.Repeat([]byte{0x11}, 28)
	var poolKey lcommon.PoolKeyHash
	copy(poolKey[:], poolHash)
	stakeKey := bytes.Repeat([]byte{0x31}, len(poolKey))

	require.NoError(t, db.ImportPool(
		nil,
		&models.Pool{PoolKeyHash: poolHash},
		&models.PoolRegistration{
			PoolKeyHash:   poolHash,
			AddedSlot:     50,
			RewardAccount: []byte{0x01, 0x02},
		},
	))

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	_, _, err := mgr.rewardInputs(
		1,
		&StakeDistribution{
			Slot: 100,
			PoolStakes: map[lcommon.PoolKeyHash]uint64{
				poolKey: 100,
			},
			StakeInputs: []StakeInput{
				{
					PoolKeyHash: poolKey[:],
					StakingKey:  stakeKey,
					Stake:       100,
					Registered:  true,
				},
			},
		},
		event.EpochTransitionEvent{BoundarySlot: 200},
		db.Metadata(),
		nil,
	)
	require.ErrorContains(t, err, "invalid reward account length")
}

func TestRewardInputsRejectInvalidRewardAccountCredentialTag(t *testing.T) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})
	poolHash := bytes.Repeat([]byte{0x11}, 28)
	var poolKey lcommon.PoolKeyHash
	copy(poolKey[:], poolHash)
	stakeKey := bytes.Repeat([]byte{0x31}, len(poolKey))
	rewardAccount := bytes.Repeat([]byte{0x41}, len(poolKey))

	require.NoError(t, db.ImportPool(
		nil,
		&models.Pool{PoolKeyHash: poolHash},
		&models.PoolRegistration{
			PoolKeyHash:                poolHash,
			AddedSlot:                  50,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 2,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
		},
	))

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	_, _, err := mgr.rewardInputs(
		1,
		&StakeDistribution{
			Slot: 100,
			PoolStakes: map[lcommon.PoolKeyHash]uint64{
				poolKey: 100,
			},
			StakeInputs: []StakeInput{
				{
					PoolKeyHash: poolKey[:],
					StakingKey:  stakeKey,
					Stake:       100,
					Registered:  true,
				},
			},
		},
		event.EpochTransitionEvent{BoundarySlot: 200},
		db.Metadata(),
		nil,
	)
	require.ErrorContains(t, err, "invalid reward account credential tag")
}

func TestRewardInputsRejectMissingPoolMargin(t *testing.T) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})
	poolHash := bytes.Repeat([]byte{0x11}, 28)
	var poolKey lcommon.PoolKeyHash
	copy(poolKey[:], poolHash)
	stakeKey := bytes.Repeat([]byte{0x31}, len(poolKey))
	rewardAccount := bytes.Repeat([]byte{0x41}, len(poolKey))

	require.NoError(t, db.ImportPool(
		nil,
		&models.Pool{PoolKeyHash: poolHash},
		&models.PoolRegistration{
			PoolKeyHash:   poolHash,
			AddedSlot:     50,
			RewardAccount: rewardAccount,
		},
	))

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	_, _, err := mgr.rewardInputs(
		1,
		&StakeDistribution{
			Slot: 100,
			PoolStakes: map[lcommon.PoolKeyHash]uint64{
				poolKey: 100,
			},
			StakeInputs: []StakeInput{
				{
					PoolKeyHash: poolKey[:],
					StakingKey:  stakeKey,
					Stake:       100,
					Registered:  true,
				},
			},
		},
		event.EpochTransitionEvent{BoundarySlot: 200},
		db.Metadata(),
		nil,
	)
	require.ErrorContains(t, err, "missing pool margin")
}

func TestRewardInputsRejectInvalidPoolOwnerKeyHashLength(t *testing.T) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})
	poolHash := bytes.Repeat([]byte{0x11}, 28)
	var poolKey lcommon.PoolKeyHash
	copy(poolKey[:], poolHash)
	stakeKey := bytes.Repeat([]byte{0x31}, len(poolKey))
	rewardAccount := bytes.Repeat([]byte{0x41}, len(poolKey))

	require.NoError(t, db.ImportPool(
		nil,
		&models.Pool{PoolKeyHash: poolHash},
		&models.PoolRegistration{
			PoolKeyHash:   poolHash,
			AddedSlot:     50,
			RewardAccount: rewardAccount,
			Margin:        &types.Rat{Rat: big.NewRat(1, 10)},
			Owners: []models.PoolRegistrationOwner{
				{KeyHash: []byte{0x01, 0x02}},
			},
		},
	))

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	_, _, err := mgr.rewardInputs(
		1,
		&StakeDistribution{
			Slot: 100,
			PoolStakes: map[lcommon.PoolKeyHash]uint64{
				poolKey: 100,
			},
			StakeInputs: []StakeInput{
				{
					PoolKeyHash: poolKey[:],
					StakingKey:  stakeKey,
					Stake:       100,
					Registered:  true,
				},
			},
		},
		event.EpochTransitionEvent{BoundarySlot: 200},
		db.Metadata(),
		nil,
	)
	require.ErrorContains(t, err, "invalid pool owner key hash length")
}

func TestHandleEpochTransitionKeepsBoundaryCapturedSnapshot(t *testing.T) {
	db := setupTestDB(t)

	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	poolHash := []byte("poolB_12345678901234567890AB")
	stakingKey := bytes.Repeat([]byte{0xbc}, 28)
	seedPoolAndDelegations(t, db, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{
			stakingKey:  stakingKey,
			utxoAmounts: []types.Uint64{50_000_000},
		},
	}, 500)

	eventBus := event.NewEventBus(nil, nil)
	mgr := NewManager(db, eventBus, nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432000,
		EpochNonce:      []byte{0x01, 0x02, 0x03},
		ProtocolVersion: 8,
		SnapshotSlot:    431999,
	}

	txn := db.Transaction(true)
	require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
		context.Background(),
		txn,
		evt,
	))
	require.NoError(t, txn.Commit())

	txId := make([]byte, 32)
	copy(txId, []byte("late_boundary_utxo_123456789012"))
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:       txId,
		OutputIdx:  0,
		StakingKey: stakingKey,
		Amount:     25_000_000,
		AddedSlot:  432000,
	}))

	require.NoError(t, mgr.handleEpochTransition(context.Background(), evt))

	rewardSnapshot, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, rewardSnapshot)
	require.Equal(t, uint64(50_000_000), uint64(rewardSnapshot.TotalActiveStake))

	inputs, err := db.Metadata().GetRewardStakeInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(50_000_000), uint64(inputs[0].Stake))
}

func TestHandleEpochTransitionRefreshesProvisionalSlotSnapshot(t *testing.T) {
	db := setupTestDB(t)

	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	poolHash := []byte("poolP_12345678901234567890AB")
	stakingKey := bytes.Repeat([]byte{0xfc}, 28)
	seedPoolAndDelegations(t, db, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{
			stakingKey:  stakingKey,
			utxoAmounts: []types.Uint64{50_000_000},
		},
	}, 500)

	eventBus := event.NewEventBus(nil, nil)
	mgr := NewManager(db, eventBus, nil)
	slotEvt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432000,
		ProtocolVersion: 8,
		SnapshotSlot:    431999,
	}
	require.NoError(t, mgr.handleEpochTransition(context.Background(), slotEvt))

	txId := make([]byte, 32)
	copy(txId, []byte("refresh_boundary_utxo_1234567890"))
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:       txId,
		OutputIdx:  0,
		StakingKey: stakingKey,
		Amount:     25_000_000,
		AddedSlot:  432000,
	}))

	blockEvt := slotEvt
	blockEvt.EpochNonce = []byte{0x04, 0x05, 0x06}
	require.NoError(t, mgr.handleEpochTransition(context.Background(), blockEvt))

	rewardSnapshot, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, rewardSnapshot)
	require.Equal(t, []byte{0x04, 0x05, 0x06}, rewardSnapshot.EpochNonce)
	require.Equal(t, uint64(75_000_000), uint64(rewardSnapshot.TotalActiveStake))
}

func TestHandleEpochTransitionReplacesStaleSnapshotRows(t *testing.T) {
	db := setupTestDB(t)

	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	poolA := bytes.Repeat([]byte{0xaa}, 28)
	poolB := bytes.Repeat([]byte{0xbb}, 28)
	stakeA := bytes.Repeat([]byte{0x01}, 28)
	stakeB := bytes.Repeat([]byte{0x02}, 28)
	seedPoolAndDelegations(t, db, poolA, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{
			stakingKey:  stakeA,
			utxoAmounts: []types.Uint64{50_000_000},
		},
	}, 500)

	meta := db.Metadata()
	require.NoError(t, meta.SavePoolStakeSnapshot(&models.PoolStakeSnapshot{
		Epoch:          1,
		SnapshotType:   "mark",
		PoolKeyHash:    poolB,
		TotalStake:     25_000_000,
		DelegatorCount: 1,
		CapturedSlot:   999,
	}, nil))
	require.NoError(t, meta.SaveRewardPoolInputs([]*models.RewardPoolInput{
		{
			Epoch:          1,
			PoolKeyHash:    poolB,
			DelegatedStake: 25_000_000,
			DelegatorCount: 1,
			CapturedSlot:   999,
			BoundarySlot:   432000,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardStakeInputs([]*models.RewardStakeInput{
		{
			Epoch:        1,
			PoolKeyHash:  poolB,
			StakingKey:   stakeB,
			Stake:        25_000_000,
			Registered:   true,
			CapturedSlot: 999,
			BoundarySlot: 432000,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardPoolOutputs([]*models.RewardPoolOutput{
		{
			Epoch:             1,
			PoolKeyHash:       poolB,
			TotalReward:       100,
			LeaderReward:      40,
			MemberRewardTotal: 60,
			CapturedSlot:      1000,
			BoundarySlot:      432000,
		},
	}, nil))
	require.NoError(t, meta.SaveRewardAccountOutputs([]*models.RewardAccountOutput{
		{
			Epoch:        1,
			PoolKeyHash:  poolB,
			StakingKey:   stakeB,
			RewardType:   "member",
			Amount:       60,
			Spendable:    true,
			CapturedSlot: 1000,
			BoundarySlot: 432000,
		},
	}, nil))

	eventBus := event.NewEventBus(nil, nil)
	mgr := NewManager(db, eventBus, nil)
	require.NoError(t, mgr.handleEpochTransition(
		context.Background(),
		event.EpochTransitionEvent{
			PreviousEpoch:   0,
			NewEpoch:        1,
			BoundarySlot:    432000,
			EpochNonce:      []byte{0x01, 0x02, 0x03},
			ProtocolVersion: 8,
			SnapshotSlot:    431999,
		},
	))

	poolSnapshots, err := meta.GetPoolStakeSnapshotsByEpoch(1, "mark", nil)
	require.NoError(t, err)
	require.Len(t, poolSnapshots, 1)
	require.Equal(t, poolA, poolSnapshots[0].PoolKeyHash)

	poolInputs, err := meta.GetRewardPoolInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, poolInputs, 1)
	require.Equal(t, poolA, poolInputs[0].PoolKeyHash)

	stakeInputs, err := meta.GetRewardStakeInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, stakeInputs, 1)
	require.Equal(t, stakeA, stakeInputs[0].StakingKey)
	require.Equal(t, uint64(50_000_000), uint64(stakeInputs[0].Stake))

	poolOutputs, err := meta.GetRewardPoolOutputs(1, nil)
	require.NoError(t, err)
	require.Empty(t, poolOutputs)
	accountOutputs, err := meta.GetRewardAccountOutputs(1, nil)
	require.NoError(t, err)
	require.Empty(t, accountOutputs)
}

// TestCaptureGenesisSnapshot_NoPools verifies that when no pools exist
// at all, no snapshots are created and no error is returned.
func TestCaptureGenesisSnapshot_NoPools(t *testing.T) {
	db := setupTestDB(t)

	// Epochs exist but no pools
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
		{EpochId: 10, StartSlot: 4320000, LengthInSlots: 432000},
	})

	eventBus := event.NewEventBus(nil, nil)
	mgr := NewManager(db, eventBus, nil)

	err := mgr.CaptureGenesisSnapshot(context.Background())
	require.NoError(t, err, "no pools should not be an error")
}

// TestCaptureGenesisSnapshot_SmallEpoch verifies correct behavior when
// the current epoch is less than 2 (edge case for the offset loop).
func TestCaptureGenesisSnapshot_SmallEpoch(t *testing.T) {
	db := setupTestDB(t)

	// Simulate Mithril bootstrap to epoch 1 (no pools at slot 0)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
		{EpochId: 1, StartSlot: 432000, LengthInSlots: 432000},
	})

	poolHash := []byte("poolS_12345678901234567890AB")
	seedPoolAndDelegations(t, db, poolHash, []struct {
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

// TestHandleEpochTransitionSkipsPoolWithMissingMargin exercises the full
// epoch-rollover path (not a direct rewardInputs call) with two pools: one
// with a fully valid registration, and one whose registration is missing
// its margin (e.g. simulating a legacy/partial import that never recorded
// this field). Before the fix, rewardInputs would hard-error on the bad
// pool's missing margin and that error would propagate out of
// handleEpochTransition, wedging the epoch transition for every pool, not
// just the bad one. This proves the transition now succeeds, the bad pool
// is excluded only from reward-calculation inputs, the good pool's reward
// inputs are unaffected, and the ground-truth stake reporting
// (PoolStakeSnapshot/EpochSummary) still reflects both pools' real stake.
func TestHandleEpochTransitionSkipsPoolWithMissingMargin(t *testing.T) {
	db := setupTestDB(t)

	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	// Good pool: fully valid registration via the normal test helper.
	goodPoolHash := []byte("poolG_12345678901234567890AB")
	goodStakingKey := bytes.Repeat([]byte{0x21}, 28)
	seedPoolAndDelegations(t, db, goodPoolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{
			stakingKey:  goodStakingKey,
			utxoAmounts: []types.Uint64{50_000_000},
		},
	}, 500)

	// Bad pool: active (registered before the boundary, never retired)
	// and carrying real delegated stake, but its registration is
	// missing the margin field, as a legacy/partial import might leave
	// it.
	badPoolHash := bytes.Repeat([]byte{0x77}, 28)
	badStakingKey := bytes.Repeat([]byte{0x88}, 28)
	require.NoError(t, db.ImportPool(
		nil,
		&models.Pool{PoolKeyHash: badPoolHash},
		&models.PoolRegistration{
			PoolKeyHash:   badPoolHash,
			AddedSlot:     500,
			RewardAccount: bytes.Repeat([]byte{0x99}, 28),
			// Margin intentionally left nil/missing.
		},
	))
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: badStakingKey,
		Pool:       badPoolHash,
		AddedSlot:  500,
		Active:     true,
	}))
	badTxId := make([]byte, 32)
	copy(badTxId, []byte("bad_pool_delegator_utxo_1234567"))
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:       badTxId,
		OutputIdx:  0,
		StakingKey: badStakingKey,
		Amount:     30_000_000,
		AddedSlot:  500,
	}))

	eventBus := event.NewEventBus(nil, nil)
	mgr := NewManager(db, eventBus, nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432000,
		EpochNonce:      []byte{0x01, 0x02, 0x03},
		ProtocolVersion: 8,
		SnapshotSlot:    431999,
	}

	// The epoch transition must succeed even though one pool's
	// registration is missing its margin: it must not wedge every
	// other pool's reward-input capture.
	require.NoError(t, mgr.handleEpochTransition(context.Background(), evt))

	rewardSnapshot, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, rewardSnapshot)
	// Only the good pool's stake counts toward the reward snapshot.
	require.Equal(
		t,
		uint64(50_000_000),
		uint64(rewardSnapshot.TotalActiveStake),
	)
	require.Equal(t, uint64(1), rewardSnapshot.TotalPoolCount)

	poolInputs, err := db.Metadata().GetRewardPoolInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, poolInputs, 1)
	require.Equal(t, goodPoolHash, poolInputs[0].PoolKeyHash)

	stakeInputs, err := db.Metadata().GetRewardStakeInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, stakeInputs, 1)
	require.Equal(t, goodStakingKey, stakeInputs[0].StakingKey)

	// The observed stake distribution (used for leader election and
	// reporting, independent of reward eligibility) must still reflect
	// BOTH pools: skipping a pool from reward inputs must not erase it
	// from ground-truth snapshot data.
	epochSummary, err := db.Metadata().GetEpochSummary(1, nil)
	require.NoError(t, err)
	require.NotNil(t, epochSummary)
	require.Equal(
		t,
		uint64(80_000_000),
		uint64(epochSummary.TotalActiveStake),
	)
	require.Equal(t, uint64(2), epochSummary.TotalPoolCount)

	badPoolSnapshot, err := db.Metadata().GetPoolStakeSnapshot(
		1, "mark", badPoolHash, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, badPoolSnapshot)
	require.Equal(t, uint64(30_000_000), uint64(badPoolSnapshot.TotalStake))
}

// TestRewardInputsSkippingDegradedPoolsExcludesOnlyBadPools is a more
// direct test of the retry/skip loop itself: two pools have distinct
// degraded registration conditions (missing registration entirely, and an
// invalid reward-account credential tag) alongside one fully valid pool.
// It proves rewardInputsSkippingDegradedPools tolerates more than one bad
// pool in the same distribution, excludes exactly those pools, and leaves
// the good pool's inputs and the returned (reward-input) distribution's
// totals correctly reduced to match.
func TestRewardInputsSkippingDegradedPoolsExcludesOnlyBadPools(t *testing.T) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	goodPoolHash := bytes.Repeat([]byte{0x11}, 28)
	goodStakeKey := bytes.Repeat([]byte{0x21}, 28)
	rewardAccount := bytes.Repeat([]byte{0x41}, 28)
	require.NoError(t, db.ImportPool(
		nil,
		&models.Pool{PoolKeyHash: goodPoolHash},
		&models.PoolRegistration{
			PoolKeyHash:   goodPoolHash,
			AddedSlot:     50,
			RewardAccount: rewardAccount,
			Margin:        &types.Rat{Rat: big.NewRat(1, 10)},
		},
	))

	// Bad pool 1: no registration at all.
	missingRegPoolHash := bytes.Repeat([]byte{0x22}, 28)
	missingRegStakeKey := bytes.Repeat([]byte{0x32}, 28)

	// Bad pool 2: registered, but with an invalid reward account
	// credential tag.
	badTagPoolHash := bytes.Repeat([]byte{0x33}, 28)
	badTagStakeKey := bytes.Repeat([]byte{0x43}, 28)
	require.NoError(t, db.ImportPool(
		nil,
		&models.Pool{PoolKeyHash: badTagPoolHash},
		&models.PoolRegistration{
			PoolKeyHash:                badTagPoolHash,
			AddedSlot:                  50,
			RewardAccount:              rewardAccount,
			RewardAccountCredentialTag: 2,
			Margin:                     &types.Rat{Rat: big.NewRat(1, 10)},
		},
	))

	var goodPoolKey, missingRegPoolKey, badTagPoolKey lcommon.PoolKeyHash
	copy(goodPoolKey[:], goodPoolHash)
	copy(missingRegPoolKey[:], missingRegPoolHash)
	copy(badTagPoolKey[:], badTagPoolHash)

	distribution := &StakeDistribution{
		Slot: 100,
		PoolStakes: map[lcommon.PoolKeyHash]uint64{
			goodPoolKey:       100,
			missingRegPoolKey: 200,
			badTagPoolKey:     300,
		},
		DelegatorCount: map[lcommon.PoolKeyHash]uint64{
			goodPoolKey:       1,
			missingRegPoolKey: 1,
			badTagPoolKey:     1,
		},
		TotalStake: 600,
		TotalPools: 3,
		StakeInputs: []StakeInput{
			{
				PoolKeyHash: goodPoolHash,
				StakingKey:  goodStakeKey,
				Stake:       100,
				Registered:  true,
			},
			{
				PoolKeyHash: missingRegPoolHash,
				StakingKey:  missingRegStakeKey,
				Stake:       200,
				Registered:  true,
			},
			{
				PoolKeyHash: badTagPoolHash,
				StakingKey:  badTagStakeKey,
				Stake:       300,
				Registered:  true,
			},
		},
	}

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	poolInputs, stakeInputs, effective, err := mgr.rewardInputsSkippingDegradedPools(
		1,
		distribution,
		event.EpochTransitionEvent{BoundarySlot: 200},
		db.Metadata(),
		nil,
	)
	require.NoError(t, err)

	require.Len(t, poolInputs, 1)
	require.Equal(t, goodPoolHash, poolInputs[0].PoolKeyHash)

	require.Len(t, stakeInputs, 1)
	require.Equal(t, goodStakeKey, stakeInputs[0].StakingKey)

	require.Equal(t, uint64(100), sumPoolStakes(effective.PoolStakes))
	require.Len(t, effective.PoolStakes, 1)
	require.Contains(t, effective.PoolStakes, goodPoolKey)

	// The original distribution passed in must be untouched: only the
	// working copy used for reward inputs is filtered.
	require.Len(t, distribution.PoolStakes, 3)
	require.Equal(t, uint64(600), distribution.TotalStake)
}

// TestFallbackCaptureRespectsAuthoritativeMarkSnapshot is a regression test
// for a TOCTOU race between the two mark-snapshot capture paths at an epoch
// boundary:
//
//  1. The authoritative path, CaptureEpochBoundarySnapshot, runs
//     synchronously inside the epoch-rollover write transaction at the exact
//     SNAP point.
//  2. A fallback, event-driven path (captureMarkSnapshot, invoked by
//     handleEpochTransition off a wall-clock slot-clock tick) computes the
//     live distribution and writes it separately.
//
// In production, handleEpochTransition checks
// authoritativeMarkRewardSnapshotExists before calling captureMarkSnapshot,
// but that check is a fresh, out-of-transaction read: near the tip, the
// slot-clock event can fire and pass that check *before* the authoritative
// write commits, then captureMarkSnapshot spends time scanning the live
// aggregate (during which the authoritative write commits), and its own
// write would land afterward, clobbering the authoritative rows.
//
// This test exercises captureMarkSnapshot directly (bypassing
// handleEpochTransition's outer check entirely) to prove that the guard now
// lives where it actually matters: inside saveSnapshot/saveSnapshotInTxn,
// re-checked as the first statement of the write transaction that performs
// the deletes.
func TestFallbackCaptureRespectsAuthoritativeMarkSnapshot(t *testing.T) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	poolHash := []byte("poolX_12345678901234567890AB")
	stakingKey := bytes.Repeat([]byte{0xaa}, 28)
	seedPoolAndDelegations(t, db, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{
			stakingKey:  stakingKey,
			utxoAmounts: []types.Uint64{50_000_000},
		},
	}, 500)

	eventBus := event.NewEventBus(nil, nil)
	mgr := NewManager(db, eventBus, nil)

	authoritativeEvt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432000,
		EpochNonce:      []byte{0x01, 0x02, 0x03},
		ProtocolVersion: 8,
		SnapshotSlot:    431999,
	}

	// Authoritative capture: synchronous, inside the rollover's write
	// transaction, at the exact SNAP point.
	txn := db.Transaction(true)
	require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
		context.Background(),
		txn,
		authoritativeEvt,
	))
	require.NoError(t, txn.Commit())

	before, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, before)
	require.Equal(t, uint64(50_000_000), uint64(before.TotalActiveStake))
	require.Equal(t, uint64(431999), before.CapturedSlot)
	require.Equal(t, []byte{0x01, 0x02, 0x03}, before.EpochNonce)

	// More live state lands after the authoritative commit — exactly what
	// the fallback's live-aggregate scan would pick up if it ran later.
	txId := make([]byte, 32)
	copy(txId, []byte("post_boundary_utxo_1234567890AB"))
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:       txId,
		OutputIdx:  0,
		StakingKey: stakingKey,
		Amount:     25_000_000,
		AddedSlot:  432000,
	}))

	// Fallback capture, called directly with a nil-nonce event matching the
	// real slot-clock event shape, simulating the fallback path reaching its
	// write after the authoritative commit landed.
	fallbackEvt := authoritativeEvt
	fallbackEvt.EpochNonce = nil
	require.NoError(t, mgr.captureMarkSnapshot(context.Background(), fallbackEvt))

	after, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, after)
	require.Equal(t, uint64(50_000_000), uint64(after.TotalActiveStake),
		"fallback capture must not clobber the authoritative snapshot's stake total")
	require.Equal(t, uint64(431999), after.CapturedSlot,
		"fallback capture must not overwrite the authoritative captured slot")
	require.Equal(t, []byte{0x01, 0x02, 0x03}, after.EpochNonce,
		"fallback capture must not erase the authoritative epoch nonce")

	afterInputs, err := db.Metadata().GetRewardStakeInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, afterInputs, 1,
		"fallback capture must not add the post-boundary stake input")
	require.Equal(t, uint64(50_000_000), uint64(afterInputs[0].Stake))

	poolSnapshot, err := db.Metadata().GetPoolStakeSnapshot(
		1, "mark", poolHash, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, poolSnapshot)
	require.Equal(t, uint64(50_000_000), uint64(poolSnapshot.TotalStake),
		"fallback capture must not clobber the authoritative pool stake snapshot")
}

// TestAuthoritativeCaptureOverwritesFallbackSnapshot is the inverse of
// TestFallbackCaptureRespectsAuthoritativeMarkSnapshot: when the fallback
// (event-driven) capture lands first — the normal, non-racy case where the
// wall-clock tick fires and there is no concurrent authoritative writer —
// the authoritative CaptureEpochBoundarySnapshot path must still be free to
// overwrite those provisional rows when it later runs at the exact SNAP
// point. The in-transaction guard added to close the TOCTOU race must only
// block the fallback direction, never this one.
func TestAuthoritativeCaptureOverwritesFallbackSnapshot(t *testing.T) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	poolHash := []byte("poolY_12345678901234567890AB")
	stakingKey := bytes.Repeat([]byte{0xbb}, 28)
	seedPoolAndDelegations(t, db, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{
			stakingKey:  stakingKey,
			utxoAmounts: []types.Uint64{10_000_000},
		},
	}, 500)

	eventBus := event.NewEventBus(nil, nil)
	mgr := NewManager(db, eventBus, nil)

	fallbackEvt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432000,
		EpochNonce:      nil,
		ProtocolVersion: 8,
		SnapshotSlot:    431999,
	}

	// Fallback capture lands first (the normal case: the slot-clock tick
	// fires with no concurrent authoritative writer).
	require.NoError(t, mgr.captureMarkSnapshot(context.Background(), fallbackEvt))

	fallbackSnap, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, fallbackSnap)
	require.Equal(t, uint64(10_000_000), uint64(fallbackSnap.TotalActiveStake))
	require.Empty(t, fallbackSnap.EpochNonce)

	// More stake lands before the authoritative SNAP point actually runs.
	txId := make([]byte, 32)
	copy(txId, []byte("authoritative_utxo_1234567890AB"))
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:       txId,
		OutputIdx:  0,
		StakingKey: stakingKey,
		Amount:     40_000_000,
		AddedSlot:  432000,
	}))

	authoritativeEvt := fallbackEvt
	authoritativeEvt.EpochNonce = []byte{0x0a, 0x0b, 0x0c}

	txn := db.Transaction(true)
	require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
		context.Background(),
		txn,
		authoritativeEvt,
	))
	require.NoError(t, txn.Commit())

	after, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, after)
	require.Equal(t, uint64(50_000_000), uint64(after.TotalActiveStake),
		"authoritative capture must overwrite the fallback-written snapshot")
	require.Equal(t, []byte{0x0a, 0x0b, 0x0c}, after.EpochNonce,
		"authoritative capture must persist the real epoch nonce")

	poolSnapshot, err := db.Metadata().GetPoolStakeSnapshot(
		1, "mark", poolHash, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, poolSnapshot)
	require.Equal(t, uint64(50_000_000), uint64(poolSnapshot.TotalStake),
		"authoritative capture must overwrite the fallback pool stake snapshot")
}
