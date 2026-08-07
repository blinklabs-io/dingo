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
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// repeatedBytes builds a fixture hash of n bytes all set to b, the same shape
// the GetPoolDistr2 fixtures use.
func repeatedBytes(n int, b byte) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

// seedUnregisteredPoolStake gives a pool stake in the snapshot without the pool
// or registration rows that would let a VRF key hash be resolved for it.
func seedUnregisteredPoolStake(
	t *testing.T,
	db *database.Database,
	poolKeyHash []byte,
	stake uint64,
	snapshotEpoch uint64,
) lcommon.PoolKeyHash {
	t.Helper()
	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolKeyHash))
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

// TestPoolStakeDistribution_OrdersPoolsByKeyHash covers the ordering contract
// the exported distribution adds over queryShelleyPoolDistr2's own result.
//
// GetPoolDistr2 returns a map, so the order its pools were assembled in is
// invisible to it. UTxO RPC's StakePoolDistribution is a repeated field, so the
// same assembly order becomes part of the response a client sees. Ranging a Go
// map is deliberately randomised, so without an explicit sort two identical
// requests against an unchanged snapshot return the pools in different orders.
// Sorting by pool key hash is what makes the reply a function of the state
// alone.
func TestPoolStakeDistribution_OrdersPoolsByKeyHash(t *testing.T) {
	db := newTestDB(t)

	const snapshotEpoch = 0
	// Seeded high key hash first so a passing test cannot be explained by the
	// pools happening to come back in insertion order.
	pkhC := seedPoolDistr2Fixture(
		t, db,
		repeatedBytes(28, 0xCC), repeatedBytes(32, 0x03),
		1_000_000, snapshotEpoch,
	)
	pkhA := seedPoolDistr2Fixture(
		t, db,
		repeatedBytes(28, 0xAA), repeatedBytes(32, 0x01),
		2_000_000, snapshotEpoch,
	)
	pkhB := seedPoolDistr2Fixture(
		t, db,
		repeatedBytes(28, 0xBB), repeatedBytes(32, 0x02),
		1_000_000, snapshotEpoch,
	)

	ls := newPoolDistr2Ledger(t, db)

	dist, err := ls.PoolStakeDistribution(nil)
	require.NoError(t, err)
	require.NotNil(t, dist)
	require.Len(t, dist.Pools, 3)

	assert.Equal(t, uint64(4_000_000), dist.TotalActiveStake)
	assert.Equal(t, uint64(snapshotEpoch), dist.SnapshotEpoch)

	got := make([]lcommon.PoolKeyHash, 0, len(dist.Pools))
	for _, pool := range dist.Pools {
		got = append(got, pool.PoolKeyHash)
	}
	assert.Equal(t, []lcommon.PoolKeyHash{pkhA, pkhB, pkhC}, got,
		"pools must come back ordered by key hash, not by map iteration")

	// Repeating the read must produce the same order. A single call cannot
	// distinguish a real sort from a map that happened to range in order.
	again, err := ls.PoolStakeDistribution(nil)
	require.NoError(t, err)
	require.Equal(t, dist.Pools, again.Pools)
}

// TestPoolStakeDistribution_ReportsStakeFractionAndVrf covers the values each
// entry carries. The fraction is a share of the whole snapshot's total and the
// VRF key hash is the one block validation will hold the pool to, both
// inherited from queryShelleyPoolDistr2 rather than recomputed here.
func TestPoolStakeDistribution_ReportsStakeFractionAndVrf(t *testing.T) {
	db := newTestDB(t)

	vrfA := repeatedBytes(32, 0xAA)
	const snapshotEpoch = 0
	pkhA := seedPoolDistr2Fixture(
		t, db, repeatedBytes(28, 0x11), vrfA, 3_000_000, snapshotEpoch,
	)
	seedPoolDistr2Fixture(
		t, db,
		repeatedBytes(28, 0x22), repeatedBytes(32, 0xBB),
		1_000_000, snapshotEpoch,
	)

	ls := newPoolDistr2Ledger(t, db)

	dist, err := ls.PoolStakeDistribution(nil)
	require.NoError(t, err)
	require.Len(t, dist.Pools, 2)

	entryA := dist.Pools[0]
	require.Equal(t, pkhA, entryA.PoolKeyHash)
	assert.Equal(t, uint64(3_000_000), entryA.Stake)
	require.NotNil(t, entryA.StakeFraction)
	assert.Equal(t, 0, entryA.StakeFraction.Cmp(big.NewRat(3, 4)),
		"fraction is a share of the whole snapshot total")
	assert.Equal(t, vrfA, entryA.VrfKeyHash[:])

	// The property the distribution exists to preserve: reported fractions are
	// shares of one total, so an unfiltered reply's fractions sum to one.
	sum := new(big.Rat)
	for _, pool := range dist.Pools {
		sum.Add(sum, pool.StakeFraction.Rat)
	}
	assert.Equal(t, 0, sum.Cmp(big.NewRat(1, 1)),
		"unfiltered fractions must sum to one, got %s", sum)
}

// TestPoolStakeDistribution_CarriesTheTipItWasReadAt covers the Tip field.
//
// Callers that report a "state as of" point need the point the stake rows were
// read at. Comparing Tip against a GetTip taken after the call proves nothing:
// with no writer in between the two agree under any implementation, including
// one that sampled the tip separately. So the tip is set to a known point
// first and asserted exactly, then moved and asserted again -- a Tip that was
// hardcoded, left at its zero value, or captured once would fail one of the
// two.
//
// The stronger property, that the tip and the stake rows come from the same
// transaction, is not observable from outside a single call: both are taken
// from one epochAtTip(txn) and the call returns before anything else can
// interleave. It is held by construction here and asserted at the UTxO RPC
// layer, where TestReadState_LedgerTipComesFromTheDistributionRead checks the
// handler renders this point and never samples the live tip.
func TestPoolStakeDistribution_CarriesTheTipItWasReadAt(t *testing.T) {
	db := newTestDB(t)
	seedPoolDistr2Fixture(
		t, db,
		repeatedBytes(28, 0x11), repeatedBytes(32, 0xAA),
		3_000_000, 0,
	)
	ls := newPoolDistr2Ledger(t, db)

	first := ochainsync.Tip{
		Point:       ocommon.NewPoint(111, repeatedBytes(32, 0x0A)),
		BlockNumber: 7,
	}
	require.NoError(t, db.SetTip(first, nil))

	dist, err := ls.PoolStakeDistribution(nil)
	require.NoError(t, err)
	require.NotNil(t, dist)
	assert.Equal(t, first.Point.Slot, dist.Tip.Point.Slot)
	assert.Equal(t, first.Point.Hash, dist.Tip.Point.Hash)

	// Advancing the chain must move the reported point.
	second := ochainsync.Tip{
		Point:       ocommon.NewPoint(222, repeatedBytes(32, 0x0B)),
		BlockNumber: 8,
	}
	require.NoError(t, db.SetTip(second, nil))

	again, err := ls.PoolStakeDistribution(nil)
	require.NoError(t, err)
	require.NotNil(t, again)
	assert.Equal(t, second.Point.Slot, again.Tip.Point.Slot)
	assert.Equal(t, second.Point.Hash, again.Tip.Point.Hash)

	// The earlier result is a snapshot, not a live view of the chain.
	assert.Equal(t, first.Point.Slot, dist.Tip.Point.Slot)
	assert.Equal(t, first.Point.Hash, dist.Tip.Point.Hash)
}

// TestPoolStakeDistribution_FilterReportsOnlyRequestedPools covers the bounded
// read. The filter selects which pools are reported; it does not change what
// they are a share of, so a filtered reply's fractions sum to less than one.
func TestPoolStakeDistribution_FilterReportsOnlyRequestedPools(t *testing.T) {
	db := newTestDB(t)

	const snapshotEpoch = 0
	pkhA := seedPoolDistr2Fixture(
		t, db,
		repeatedBytes(28, 0x11), repeatedBytes(32, 0xAA),
		3_000_000, snapshotEpoch,
	)
	seedPoolDistr2Fixture(
		t, db,
		repeatedBytes(28, 0x22), repeatedBytes(32, 0xBB),
		1_000_000, snapshotEpoch,
	)

	ls := newPoolDistr2Ledger(t, db)

	dist, err := ls.PoolStakeDistribution([]lcommon.PoolKeyHash{pkhA})
	require.NoError(t, err)
	require.Len(t, dist.Pools, 1)
	assert.Equal(t, pkhA, dist.Pools[0].PoolKeyHash)
	assert.Equal(t, uint64(4_000_000), dist.TotalActiveStake,
		"the filter must not renormalise the total")
	assert.Equal(t, 0, dist.Pools[0].StakeFraction.Cmp(big.NewRat(3, 4)))
}

// TestPoolStakeDistribution_OmitsPoolWithoutRegistration mirrors
// queryShelleyPoolDistr2's handling of a pool that holds snapshot stake but has
// no registration on record: it cannot be given a VRF key hash, so it is left
// out and the rest of the distribution is still served.
func TestPoolStakeDistribution_OmitsPoolWithoutRegistration(t *testing.T) {
	db := newTestDB(t)

	const snapshotEpoch = 0
	pkhA := seedPoolDistr2Fixture(
		t, db,
		repeatedBytes(28, 0x11), repeatedBytes(32, 0xAA),
		3_000_000, snapshotEpoch,
	)
	seedUnregisteredPoolStake(
		t,
		db,
		repeatedBytes(28, 0x22),
		1_000_000,
		snapshotEpoch,
	)

	ls := newPoolDistr2Ledger(t, db)

	dist, err := ls.PoolStakeDistribution(nil)
	require.NoError(t, err)
	require.Len(t, dist.Pools, 1, "the unregistered pool is omitted")
	assert.Equal(t, pkhA, dist.Pools[0].PoolKeyHash)
	assert.Equal(t, uint64(4_000_000), dist.TotalActiveStake,
		"the omitted pool's stake stays in the total")
}

// TestPoolStakeDistribution_EmptySnapshotDoesNotDivide covers an epoch whose
// snapshot holds no stake at all, the state a fresh chain is in before its
// first snapshot is taken. Dividing by the total would panic.
func TestPoolStakeDistribution_EmptySnapshotDoesNotDivide(t *testing.T) {
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	dist, err := ls.PoolStakeDistribution(nil)
	require.NoError(t, err)
	require.NotNil(t, dist)
	assert.Empty(t, dist.Pools)
}
