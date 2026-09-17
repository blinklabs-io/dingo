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

// TestRewardSnapshotActiveStakeKeepsDegradedPoolStake pins the sigma_a
// denominator written by buildRewardStateInputs.
//
// cardano-ledger derives ssTotalActiveStake from every registered credential
// that carries a delegation, independently of which pools are present in
// ssStakePoolsSnapShot (Cardano.Ledger.State.SnapShots.mkSnapShot over
// Cardano.Ledger.State.Stake.resolveInstantStake, whose own comment reads
// "active stake means any stake credential that is registered and delegated to
// a stake pool"). A pool whose registration cannot be resolved therefore earns
// nothing while its delegators keep contributing to that denominator.
//
// Deriving RewardSnapshot.TotalActiveStake from the post-exclusion pool set
// instead shrinks the denominator, which raises sigma_a for every surviving
// pool, lowers its apparent performance, and under-credits every member and
// leader reward the node reconstructs. TotalPoolCount and TotalDelegators
// describe the reward_pool_input rows actually written and stay reduced.
func TestRewardSnapshotActiveStakeKeepsDegradedPoolStake(t *testing.T) {
	t.Parallel()

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

	// Degraded pool: delegated to, but with no resolvable registration.
	degradedPoolHash := bytes.Repeat([]byte{0x22}, 28)
	degradedStakeKey := bytes.Repeat([]byte{0x32}, 28)

	var goodPoolKey, degradedPoolKey lcommon.PoolKeyHash
	copy(goodPoolKey[:], goodPoolHash)
	copy(degradedPoolKey[:], degradedPoolHash)

	const goodStake = uint64(7_000_000_000)
	const degradedStake = uint64(3_000_000_000)

	distribution := &StakeDistribution{
		Slot: 100,
		PoolStakes: map[lcommon.PoolKeyHash]uint64{
			goodPoolKey:     goodStake,
			degradedPoolKey: degradedStake,
		},
		DelegatorCount: map[lcommon.PoolKeyHash]uint64{
			goodPoolKey:     1,
			degradedPoolKey: 1,
		},
		TotalStake: goodStake + degradedStake,
		TotalPools: 2,
		StakeInputs: []StakeInput{
			{
				PoolKeyHash: goodPoolHash,
				StakingKey:  goodStakeKey,
				Stake:       goodStake,
				Registered:  true,
			},
			{
				PoolKeyHash: degradedPoolHash,
				StakingKey:  degradedStakeKey,
				Stake:       degradedStake,
				Registered:  true,
			},
		},
	}

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	bundle, err := mgr.buildRewardStateInputs(
		1,
		models.PoolStakeSnapshotTypeMark,
		distribution,
		event.EpochTransitionEvent{BoundarySlot: 200},
		db.Metadata(),
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, bundle)

	// The degraded pool earns nothing: no reward_pool_input row, and no
	// reward_stake_input row for its delegator.
	require.Len(t, bundle.poolInputs, 1)
	require.Equal(t, goodPoolHash, bundle.poolInputs[0].PoolKeyHash)
	require.Len(t, bundle.stakeInputs, 1)
	require.Equal(t, goodStakeKey, bundle.stakeInputs[0].StakingKey)
	require.Equal(t, uint64(1), bundle.snapshot.TotalPoolCount)
	require.Equal(t, uint64(1), bundle.snapshot.TotalDelegators)

	// Its delegator's stake nonetheless stays in the sigma_a denominator.
	require.Equal(
		t,
		goodStake+degradedStake,
		uint64(bundle.snapshot.TotalActiveStake),
	)
}

// TestSaveSnapshotKeepsDegradedPoolStakeInPoolAndEpochRows is the
// pool_stake_snapshot/epoch_summary companion to
// TestRewardSnapshotActiveStakeKeepsDegradedPoolStake, and pins the
// DATABASE.md claim for those tables: saveSnapshotInTxn builds
// pool_stake_snapshot and epoch_summary from the caller's unmodified
// distribution (rotation.go's loop over distribution.PoolStakes and the
// EpochSummary literal that follows it), not from the post-exclusion copy
// buildRewardStateInputs consumes internally to populate reward_pool_input.
// A pool excluded from reward_pool_input for degraded registration data
// therefore still gets its own pool_stake_snapshot row carrying its full
// stake, and epoch_summary's totals still count it.
func TestSaveSnapshotKeepsDegradedPoolStakeInPoolAndEpochRows(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	seedEpochs(t, db, []models.Epoch{
		{EpochId: 0, StartSlot: 0, LengthInSlots: 432000},
	})

	goodPoolHash := bytes.Repeat([]byte{0x13}, 28)
	goodStakeKey := bytes.Repeat([]byte{0x23}, 28)
	rewardAccount := bytes.Repeat([]byte{0x43}, 28)
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

	// Degraded pool: delegated to, but with no resolvable registration.
	degradedPoolHash := bytes.Repeat([]byte{0x24}, 28)
	degradedStakeKey := bytes.Repeat([]byte{0x34}, 28)

	var goodPoolKey, degradedPoolKey lcommon.PoolKeyHash
	copy(goodPoolKey[:], goodPoolHash)
	copy(degradedPoolKey[:], degradedPoolHash)

	const goodStake = uint64(7_000_000_000)
	const degradedStake = uint64(3_000_000_000)

	distribution := &StakeDistribution{
		Slot: 100,
		PoolStakes: map[lcommon.PoolKeyHash]uint64{
			goodPoolKey:     goodStake,
			degradedPoolKey: degradedStake,
		},
		DelegatorCount: map[lcommon.PoolKeyHash]uint64{
			goodPoolKey:     1,
			degradedPoolKey: 1,
		},
		TotalStake: goodStake + degradedStake,
		TotalPools: 2,
		StakeInputs: []StakeInput{
			{
				PoolKeyHash: goodPoolHash,
				StakingKey:  goodStakeKey,
				Stake:       goodStake,
				Registered:  true,
			},
			{
				PoolKeyHash: degradedPoolHash,
				StakingKey:  degradedStakeKey,
				Stake:       degradedStake,
				Registered:  true,
			},
		},
	}

	mgr := NewManager(db, event.NewEventBus(nil, nil), nil)
	saved, err := mgr.saveSnapshot(
		context.Background(),
		1,
		models.PoolStakeSnapshotTypeMark,
		distribution,
		event.EpochTransitionEvent{BoundarySlot: 200},
		false,
		true,
		false,
	)
	require.NoError(t, err)
	require.True(t, saved)

	// reward_pool_input excludes the degraded pool.
	poolInputs, err := db.Metadata().GetRewardPoolInputs(1, nil)
	require.NoError(t, err)
	require.Len(t, poolInputs, 1)
	require.Equal(t, goodPoolHash, poolInputs[0].PoolKeyHash)

	// pool_stake_snapshot still carries the degraded pool's own row with its
	// full stake.
	degraded, err := db.Metadata().GetPoolStakeSnapshot(
		1, models.PoolStakeSnapshotTypeMark, degradedPoolHash, nil,
	)
	require.NoError(t, err)
	require.NotNil(t, degraded)
	require.Equal(t, degradedStake, uint64(degraded.TotalStake))

	// epoch_summary's totals describe the full boundary, not just the
	// surviving pool set reward_pool_input holds.
	summary, err := db.Metadata().GetEpochSummary(1, nil)
	require.NoError(t, err)
	require.NotNil(t, summary)
	require.Equal(t, goodStake+degradedStake, uint64(summary.TotalActiveStake))
	require.Equal(t, uint64(2), summary.TotalPoolCount)
	require.Equal(t, uint64(2), summary.TotalDelegators)
}
