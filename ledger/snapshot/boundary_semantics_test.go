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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// TestCaptureEpochBoundaryUsesSnapPointStake proves the authoritative capture
// persists the stake read at the SNAP point rather than whatever the live
// aggregate holds when the snapshot row is finally written.
//
// cardano-ledger runs SNAP before POOLREAP and before governance enactment, so
// the mark snapshot must not contain the reward-account credits those later rules
// apply at the boundary slot. Here the live aggregate is mutated between the two
// phases, standing in for exactly those credits; the persisted snapshot must
// still show the SNAP-point value.
func TestCaptureEpochBoundaryUsesSnapPointStake(t *testing.T) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	poolHash := []byte("poolSNAP_1234567890123456789")
	stakingKey := bytes.Repeat([]byte{0x5a}, 28)
	seedPoolAndDelegations(t, db, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{40_000_000}},
	}, 500)

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432_000,
		EpochNonce:      []byte{0x0a, 0x0b},
		ProtocolVersion: 8,
		SnapshotSlot:    431_999,
	}

	txn := db.Transaction(true)
	// SNAP point: stake read before any post-SNAP boundary rule runs.
	require.NoError(t, mgr.ComputeEpochBoundarySnapshot(
		context.Background(), txn, evt,
	))
	// Post-SNAP boundary credit (POOLREAP refund / MIR / treasury withdrawal /
	// proposal refund), applied inside the same rollover transaction. It raises
	// the live reward aggregate at the boundary slot, which is what the capture
	// used to absorb.
	require.NoError(t, db.AddPostSnapshotAccountRewardByCredential(
		0,
		stakingKey,
		1_000_000,
		evt.BoundarySlot,
		bytes.Repeat([]byte{0xc1}, 32),
		txn,
	))
	// End of rollover: persist, now that the new epoch row would exist.
	require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
		context.Background(), txn, evt,
	))
	require.NoError(t, txn.Commit())

	poolSnapshot, err := db.Metadata().GetPoolStakeSnapshot(
		1, "mark", poolHash, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, poolSnapshot)
	require.Equal(t, uint64(40_000_000), uint64(poolSnapshot.TotalStake),
		"mark snapshot must hold SNAP-point stake, not post-SNAP boundary credits")

	rewardSnapshot, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.NotNil(t, rewardSnapshot)
	require.True(t, rewardSnapshot.Authoritative)
	require.Equal(t, uint64(40_000_000), uint64(rewardSnapshot.TotalActiveStake),
		"reward basis must hold SNAP-point stake too")

	inputs, err := db.Metadata().GetRewardStakeInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 1)
	require.Equal(t, uint64(40_000_000), uint64(inputs[0].Stake))
}

// TestCaptureEpochBoundaryMissingSnapHookUsesHistoricalStake proves that the
// persist fallback cannot use the live aggregate merely because the database
// tip is still at/before the snapshot slot. A missing SNAP hook is exactly the
// failure mode where the fallback runs after a post-SNAP boundary credit.
func TestCaptureEpochBoundaryMissingSnapHookUsesHistoricalStake(t *testing.T) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	poolHash := []byte("poolFALLBACK_123456789012345")
	stakingKey := bytes.Repeat([]byte{0x5d}, 28)
	seedPoolAndDelegations(t, db, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{40_000_000}},
	}, 500)

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	evt := event.EpochTransitionEvent{
		PreviousEpoch:   0,
		NewEpoch:        1,
		BoundarySlot:    432_000,
		EpochNonce:      []byte{0x0c, 0x0d},
		ProtocolVersion: 8,
		SnapshotSlot:    431_999,
	}

	txn := db.Transaction(true)
	require.NoError(t, db.AddPostSnapshotAccountRewardByCredential(
		0,
		stakingKey,
		1_000_000,
		evt.BoundarySlot,
		bytes.Repeat([]byte{0xc3}, 32),
		txn,
	))
	// Do not call ComputeEpochBoundarySnapshot: this simulates a missing or
	// failed SNAP read. The fallback runs after the credit in this transaction.
	require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
		context.Background(), txn, evt,
	))
	require.NoError(t, txn.Commit())

	poolSnapshot, err := db.Metadata().GetPoolStakeSnapshot(
		1, "mark", poolHash, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, poolSnapshot)
	require.Equal(t, uint64(40_000_000), uint64(poolSnapshot.TotalStake),
		"missing SNAP hook must reconstruct pre-credit stake, not read live aggregate")
}

// TestCaptureEpochBoundaryIgnoresStaleSnapPointStake proves the SNAP-point
// handoff is bound to the transaction that produced it: a distribution left
// behind by a rolled-back rollover is discarded even when the retry has the
// same boundary identity, and the capture reconstructs the historical SNAP
// value rather than attaching the stale live aggregate.
func TestCaptureEpochBoundaryIgnoresStaleSnapPointStake(t *testing.T) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
		{EpochId: 1, StartSlot: 432000, LengthInSlots: 432000},
	})

	poolHash := []byte("poolSTALE_123456789012345678")
	stakingKey := bytes.Repeat([]byte{0x5b}, 28)
	seedPoolAndDelegations(t, db, poolHash, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{40_000_000}},
	}, 500)
	// Commit a post-SNAP credit before the abandoned transaction. The live
	// aggregate is now 55M, while boundary-aware reconstruction must subtract
	// the credit and recover the 40M SNAP value.
	creditTxn := db.Transaction(true)
	require.NoError(t, db.AddPostSnapshotAccountRewardByCredential(
		0,
		stakingKey,
		15_000_000,
		432_000,
		bytes.Repeat([]byte{0xc2}, 32),
		creditTxn,
	))
	require.NoError(t, creditTxn.Commit())

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	abandoned := event.EpochTransitionEvent{
		PreviousEpoch: 0,
		NewEpoch:      1,
		BoundarySlot:  432_000,
		SnapshotSlot:  431_999,
	}
	txn := db.Transaction(true)
	require.NoError(t, mgr.ComputeEpochBoundarySnapshot(
		context.Background(), txn, abandoned,
	))
	require.NoError(t, txn.Rollback())

	// Retry the exact same boundary. Matching only the event fields would
	// incorrectly reuse the abandoned 55M live read here.
	next := abandoned
	txn = db.Transaction(true)
	require.NoError(t, mgr.CaptureEpochBoundarySnapshot(
		context.Background(), txn, next,
	))
	require.NoError(t, txn.Commit())

	poolSnapshot, err := db.Metadata().GetPoolStakeSnapshot(
		1, "mark", poolHash, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, poolSnapshot)
	require.Equal(t, uint64(40_000_000), uint64(poolSnapshot.TotalStake),
		"fallback must reconstruct the boundary rather than reuse a rolled-back SNAP read")
}

// TestCalculateStakeDistributionDedupesCredentialAcrossPools proves the
// duplicate-credential collapse actually reaches the snapshot aggregates.
//
// reward_live_stake is unique on (credential_tag, staking_key), so a credential
// cannot legitimately hold stake under two pools. Rows seeded before that index
// was unique can, and such a duplicate previously made the per-credential reward
// inputs disagree with the per-pool aggregate and crashed reward application at an
// epoch rollover. The dedupe existed, but only inside a throwaway validation copy,
// so PoolStakes, TotalStake, DelegatorCount — and from them PoolStakeSnapshot and
// EpochSummary — still double-counted the credential.
func TestCalculateStakeDistributionDedupesCredentialAcrossPools(t *testing.T) {
	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	poolA := []byte("poolDUPA_1234567890123456789")
	poolB := []byte("poolDUPB_1234567890123456789")
	stakingKey := bytes.Repeat([]byte{0x5c}, 28)
	seedPoolAndDelegations(t, db, poolA, []struct {
		stakingKey  []byte
		utxoAmounts []types.Uint64
	}{
		{stakingKey: stakingKey, utxoAmounts: []types.Uint64{10}},
	}, 100)
	seedPoolAndDelegations(t, db, poolB, nil, 100)

	gormDB := snapshotGormDB(t, db)
	// Reproduce a pre-unique-index database: drop the constraint, then add the
	// duplicate credential row it would now reject.
	require.NoError(t, gormDB.Exec(
		"DROP INDEX IF EXISTS idx_reward_live_stake_cred",
	).Error)
	require.NoError(t, gormDB.Create(&models.RewardLiveStake{
		CredentialTag:      0,
		StakingKey:         stakingKey,
		PoolKeyHash:        poolB,
		UtxoStake:          types.Uint64(10),
		TotalStake:         types.Uint64(10),
		Registered:         true,
		PoolDelegationSlot: 100,
		UpdatedSlot:        100,
	}).Error)

	var rows int64
	require.NoError(t, gormDB.Model(&models.RewardLiveStake{}).
		Where("staking_key = ?", stakingKey).Count(&rows).Error)
	require.Equal(t, int64(2), rows, "fixture must hold the duplicate rows")

	calc := NewCalculator(db)
	txn := db.Transaction(false)
	defer func() { _ = txn.Commit() }()
	dist, err := calc.calculateStakeDistributionInTxn(
		context.Background(), txn, 100, 0,
	)
	require.NoError(t, err)

	var keyA, keyB lcommon.PoolKeyHash
	copy(keyA[:], poolA)
	copy(keyB[:], poolB)
	require.Equal(t, uint64(10), dist.TotalStake,
		"a duplicated credential must contribute its stake exactly once")
	require.Len(t, dist.StakeInputs, 1)
	require.Equal(t, uint64(1), dist.DelegatorCount[keyA]+dist.DelegatorCount[keyB],
		"a duplicated credential must be counted as one delegator")
	require.Equal(t, uint64(10), dist.PoolStakes[keyA]+dist.PoolStakes[keyB])
	require.Len(t, dist.PoolStakes, 1,
		"only the retained assignment may hold the credential's stake")
}

// TestCalculateEpochBoundaryFallbackHalvesAgree covers the fallback capture
// mixing sources. When live state has advanced past the boundary the fallback
// reconstructs the leader-election pool totals historically; the per-credential
// reward basis must come from that same reconstruction rather than from the live
// reward aggregate, which has no slot predicate. Otherwise one mark snapshot
// carries a boundary-accurate pool total against post-boundary per-credential
// stake, and nothing compares the two.
func TestCalculateEpochBoundaryFallbackHalvesAgree(t *testing.T) {
	for _, test := range []struct {
		name             string
		expiryEpoch      uint64
		inactivityPeriod uint64
	}{
		{name: "CIP-0163 gate off"},
		{
			name:             "CIP-0163 gate on",
			expiryEpoch:      3,
			inactivityPeriod: 2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := setupTestDB(t)
			seedEpochs(t, db, []models.Epoch{
				{EpochId: 0, StartSlot: 0, LengthInSlots: 100},
				{EpochId: 1, StartSlot: 100, LengthInSlots: 100},
				{EpochId: 2, StartSlot: 200, LengthInSlots: 100},
				{EpochId: 3, StartSlot: 300, LengthInSlots: 100},
			})

			poolHash := bytes.Repeat([]byte{0xb1}, 28)
			stakingKey := bytes.Repeat([]byte{0x5d}, 28)
			seedPoolAndDelegations(t, db, poolHash, []struct {
				stakingKey  []byte
				utxoAmounts []types.Uint64
			}{
				{stakingKey: stakingKey, utxoAmounts: []types.Uint64{50}},
			}, 100)

			// Make the live aggregate disagree with the historical
			// reconstruction, standing in for stake that moved after the
			// boundary.
			require.NoError(t, snapshotGormDB(t, db).
				Model(&models.RewardLiveStake{}).
				Where("staking_key = ?", stakingKey).
				Update("total_stake", types.Uint64(75)).Error)
			// Tip past the snapshot slot selects the historical fallback.
			require.NoError(t, db.SetTip(ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 150,
					Hash: bytes.Repeat([]byte{0x0f}, 32),
				},
				BlockNumber: 2,
			}, nil))

			calc := NewCalculator(db)
			txn := db.Transaction(false)
			defer func() { _ = txn.Commit() }()
			dist, err := calc.calculateBoundaryStakeDistributionInTxn(
				context.Background(),
				txn,
				100,
				101,
				test.expiryEpoch,
				test.inactivityPeriod,
			)
			require.NoError(t, err)

			var pool lcommon.PoolKeyHash
			copy(pool[:], poolHash)
			var inputSum uint64
			for _, input := range dist.StakeInputs {
				require.Equal(t, poolHash, input.PoolKeyHash)
				inputSum += input.Stake
			}
			require.Equal(t, uint64(50), dist.PoolStakes[pool],
				"leader-election total stays slot-accurate")
			require.Equal(t, dist.PoolStakes[pool], inputSum,
				"the reward basis must sum to the leader-election pool total")
		})
	}
}
