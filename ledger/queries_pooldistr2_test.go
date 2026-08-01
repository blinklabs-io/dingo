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
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// poolDistr2Query wraps the leaf query the way the wire delivers it, with an
// empty pool filter, which the ledger reads as "every pool".
func poolDistr2Query() *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyPoolDistr2Query{
				Type: olocalstatequery.QueryTypeShelleyPoolDistr2,
			},
		},
	}
}

// seedPoolDistr2Fixture registers a pool with a known VRF hash and gives it
// stake in the snapshot leader election reads.
func seedPoolDistr2Fixture(
	t *testing.T,
	db *database.Database,
	poolKeyHash []byte,
	vrfKeyHash []byte,
	stake uint64,
	snapshotEpoch uint64,
) lcommon.PoolKeyHash {
	t.Helper()
	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolKeyHash))
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{PoolKeyHash: pkh.Bytes(), VrfKeyHash: vrfKeyHash},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  vrfKeyHash,
			AddedSlot:   1,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))
	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:        snapshotEpoch,
			SnapshotType: snapshotTypeMark,
			PoolKeyHash:  pkh.Bytes(),
			TotalStake:   dbtypes.Uint64(stake),
			CapturedSlot: 1,
		},
		nil,
	))
	return pkh
}

// TestQueryShelleyPoolDistr2_ReportsStakeFractionAndVrf covers GetPoolDistr2,
// which cardano-cli sends while computing a leadership schedule.
//
// The reply has to agree with the distribution the node itself elects leaders
// from, or an operator checking their schedule against the node would be told
// they lead slots they do not. Both are therefore read from the mark snapshot
// at praos.StakeSnapshotEpoch rather than from live stake.
func TestQueryShelleyPoolDistr2_ReportsStakeFractionAndVrf(t *testing.T) {
	db := newTestDB(t)

	vrfA := make([]byte, 32)
	for i := range vrfA {
		vrfA[i] = 0xAA
	}
	vrfB := make([]byte, 32)
	for i := range vrfB {
		vrfB[i] = 0xBB
	}
	poolA := make([]byte, 28)
	for i := range poolA {
		poolA[i] = 0x11
	}
	poolB := make([]byte, 28)
	for i := range poolB {
		poolB[i] = 0x22
	}

	// The ledger state under test reports epoch 0, and leader election reads
	// the snapshot for the preceding epoch, which at epoch 0 is epoch 0.
	const snapshotEpoch = 0
	pkhA := seedPoolDistr2Fixture(t, db, poolA, vrfA, 3_000_000, snapshotEpoch)
	pkhB := seedPoolDistr2Fixture(t, db, poolB, vrfB, 1_000_000, snapshotEpoch)

	ls := &LedgerState{db: db}
	ls.publishSnapshotsLocked()

	result, err := ls.Query(poolDistr2Query())
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok, "expected the []any result wrapper")
	require.Len(t, arr, 1)

	distr, ok := arr[0].(olocalstatequery.PoolDistr2Result)
	require.True(t, ok, "expected a PoolDistr2Result, got %T", arr[0])

	assert.Equal(t, uint64(4_000_000), distr.TotalActiveStake,
		"total active stake is the sum over the snapshot")

	entryA, ok := distr.Pools[lcommon.PoolId(pkhA)]
	require.True(t, ok, "pool A missing from the distribution")
	assert.Equal(t, uint64(3_000_000), entryA.TotalPoolStake)
	require.NotNil(t, entryA.StakeFraction)
	assert.Equal(t, int64(3), entryA.StakeFraction.Num().Int64())
	assert.Equal(t, int64(4), entryA.StakeFraction.Denom().Int64())
	assert.Equal(t, vrfA, entryA.VrfHash[:],
		"the VRF hash is what a caller checks their own key against")

	entryB, ok := distr.Pools[lcommon.PoolId(pkhB)]
	require.True(t, ok, "pool B missing from the distribution")
	assert.Equal(t, uint64(1_000_000), entryB.TotalPoolStake)
}

// TestQueryShelleyPoolDistr2_ZeroTotalStakeDoesNotDivide covers an epoch whose
// snapshot holds no stake at all, which is the state a fresh chain is in
// before its first snapshot is taken. Dividing by the total would panic.
func TestQueryShelleyPoolDistr2_ZeroTotalStakeDoesNotDivide(t *testing.T) {
	db := newTestDB(t)
	ls := &LedgerState{db: db}
	ls.publishSnapshotsLocked()

	result, err := ls.Query(poolDistr2Query())
	require.NoError(t, err,
		"an empty snapshot reports an empty distribution, not an error")
	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)

	distr, ok := arr[0].(olocalstatequery.PoolDistr2Result)
	require.True(t, ok)
	assert.Zero(t, distr.TotalActiveStake)
	assert.Empty(t, distr.Pools)
}

// TestQueryShelleyPoolDistr2_RejectsPoolWithoutRegistration covers a pool that
// holds snapshot stake but has no registration on record.
//
// Such a pool cannot be given a VRF key hash, and dropping it silently is
// worse than it sounds: TotalActiveStake is summed over the whole snapshot and
// still counts that pool's stake, so the reported fractions would sum to less
// than one with nothing in the reply saying so. A caller would compute a
// leadership schedule against a denominator covering stake it cannot see.
//
// The state is a database inconsistency rather than a routine case, so it
// fails loudly instead of producing a quietly wrong distribution.
func TestQueryShelleyPoolDistr2_RejectsPoolWithoutRegistration(t *testing.T) {
	db := newTestDB(t)

	orphan := make([]byte, 28)
	for i := range orphan {
		orphan[i] = 0x77
	}
	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(orphan))
	// Stake in the snapshot, but no pool or registration row to match it.
	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:        0,
			SnapshotType: snapshotTypeMark,
			PoolKeyHash:  pkh.Bytes(),
			TotalStake:   dbtypes.Uint64(5_000_000),
			CapturedSlot: 1,
		},
		nil,
	))

	ls := &LedgerState{db: db}
	ls.publishSnapshotsLocked()

	_, err := ls.Query(poolDistr2Query())
	require.ErrorIs(t, err, ErrPoolDistrUnregisteredPool,
		"a pool with stake but no registration must not be dropped silently")
}
