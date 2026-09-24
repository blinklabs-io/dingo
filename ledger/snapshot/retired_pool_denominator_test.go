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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// retirePool marks a pool retired as of the given epoch, the state POOLREAP
// leaves behind and the state GetActivePoolKeyHashesAtSlot excludes.
func retirePool(
	t *testing.T,
	db *database.Database,
	poolKeyHash []byte,
	epoch uint64,
	addedSlot uint64,
) {
	t.Helper()
	_, err := snapshotSQLDB(t, db).Exec(`
INSERT INTO pool_retirement (pool_key_hash, certificate_id, pool_id, epoch, added_slot)
SELECT ?, 0, id, ?, ? FROM pool WHERE pool_key_hash = ?`,
		poolKeyHash, epoch, addedSlot, poolKeyHash,
	)
	require.NoError(t, err)
}

// TestStakeDistributionKeepsRetiredPoolStakeInDenominator pins the sigma_a
// denominator against the defect behind dingo #4660.
//
// cardano-ledger's ssTotalActiveStake sums every registered credential holding
// a delegation and never consults the stake-pool set, so a credential still
// delegated to a pool that has left the active set counts towards the
// denominator even though the pool earns nothing. Enumerating the snapshot
// from the active pool set alone drops that stake, which raises sigma_a for
// every surviving pool and under-credits every reward on the node by the
// dropped share -- uniformly, regardless of pool size or saturation, and
// invisibly to a check that compares the pool rows against the total, because
// the omission shortens both sides equally.
func TestStakeDistributionKeepsRetiredPoolStakeInDenominator(t *testing.T) {
	t.Parallel()

	db := setupTestDB(t)
	seedSnapshotEpoch(t, db)

	activePool := bytes.Repeat([]byte{0xa1}, 28)
	retiredPool := bytes.Repeat([]byte{0xb2}, 28)
	seedPoolAndDelegations(t, db, activePool, nil, 100)
	seedPoolAndDelegations(t, db, retiredPool, nil, 100)
	// seedSnapshotEpoch puts slot 1000 in epoch 10, so a retirement recorded
	// for epoch 10 is already in effect at the snapshot slot.
	retirePool(t, db, retiredPool, 10, 100)

	seedRewardLiveStake(t, snapshotSQLDB(t, db), []models.RewardLiveStake{
		{
			CredentialTag:      0,
			StakingKey:         bytes.Repeat([]byte{0x01}, 28),
			PoolKeyHash:        activePool,
			TotalStake:         types.Uint64(700),
			Registered:         true,
			PoolDelegationSlot: 100,
		},
		{
			CredentialTag:      0,
			StakingKey:         bytes.Repeat([]byte{0x02}, 28),
			PoolKeyHash:        retiredPool,
			TotalStake:         types.Uint64(300),
			Registered:         true,
			PoolDelegationSlot: 100,
		},
	})

	calc := NewCalculator(db)
	txn := db.Transaction(false)
	defer func() { _ = txn.Commit() }()
	dist, err := calc.calculateStakeDistributionInTxn(
		context.Background(), txn, 1000, 0, 0,
	)
	require.NoError(t, err)

	var active, retired lcommon.PoolKeyHash
	copy(active[:], activePool)
	copy(retired[:], retiredPool)

	// The retired pool earns nothing, so it gets no bucket and no reward input.
	require.NotContains(t, dist.PoolStakes, retired)
	require.Equal(t, uint64(700), dist.PoolStakes[active])
	require.Equal(t, uint64(700), dist.TotalStake)
	require.Len(t, dist.StakeInputs, 1)

	// Its delegator's stake still counts towards the denominator.
	require.Equal(t, uint64(1000), dist.TotalActiveStake)

	// And the reward-stake view carries that denominator rather than
	// re-deriving it from the pool buckets it does not contain.
	reward, err := rewardStakeDistribution(dist)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), rewardTotalActiveStake(reward))
	require.Equal(
		t,
		uint64(300),
		rewardTotalActiveStake(reward)-sumPoolStakes(reward.PoolStakes),
	)
}

// TestRewardTotalActiveStakeFallsBackToBuckets covers a distribution built
// outside the calculator, or restored from a capture predating the
// credential-first total: the pool-bucket sum is the floor, never zero.
func TestRewardTotalActiveStakeFallsBackToBuckets(t *testing.T) {
	t.Parallel()

	var pool lcommon.PoolKeyHash
	pool[0] = 0x01
	dist := &StakeDistribution{
		PoolStakes: map[lcommon.PoolKeyHash]uint64{pool: 500},
	}
	require.Equal(t, uint64(500), rewardTotalActiveStake(dist))

	dist.TotalActiveStake = 900
	require.Equal(t, uint64(900), rewardTotalActiveStake(dist))
}
