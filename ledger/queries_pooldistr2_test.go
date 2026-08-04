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
	"github.com/blinklabs-io/gouroboros/cbor"
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

	// The property the query exists to preserve: the reported fractions are
	// shares of the same total the reply carries, so they sum to one. Checking
	// each fraction alone would not catch a pool being dropped while its stake
	// stayed in the total.
	sum := new(big.Rat).Add(entryA.StakeFraction.Rat, entryB.StakeFraction.Rat)
	assert.Equal(t, 0, sum.Cmp(big.NewRat(1, 1)),
		"reported fractions must sum to one over the snapshot, got %s", sum)
}

// poolDistr2QueryFor wraps the leaf query with a pool filter, the form
// cardano-cli sends when it wants specific pools rather than the whole
// distribution.
func poolDistr2QueryFor(
	pools ...lcommon.PoolKeyHash,
) *olocalstatequery.BlockQuery {
	ids := make([]lcommon.PoolId, 0, len(pools))
	for _, pkh := range pools {
		ids = append(ids, lcommon.PoolId(pkh))
	}
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyPoolDistr2Query{
				Type:  olocalstatequery.QueryTypeShelleyPoolDistr2,
				Pools: []cbor.SetType[lcommon.PoolId]{cbor.NewSetType(ids, false)},
			},
		},
	}
}

// TestQueryShelleyPoolDistr2_FilterReportsOnlyRequestedPools covers a query
// carrying a pool filter.
//
// The filter selects which pools are reported; it does not change what they are
// a share of. TotalActiveStake stays the whole snapshot's total and each
// fraction stays a share of it, so a filtered reply's fractions sum to less
// than one -- renormalising them over the requested pools would tell a caller
// their pool leads more slots than the node will grant it.
func TestQueryShelleyPoolDistr2_FilterReportsOnlyRequestedPools(t *testing.T) {
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

	const snapshotEpoch = 0
	pkhA := seedPoolDistr2Fixture(t, db, poolA, vrfA, 3_000_000, snapshotEpoch)
	pkhB := seedPoolDistr2Fixture(t, db, poolB, vrfB, 1_000_000, snapshotEpoch)

	ls := &LedgerState{db: db}
	ls.publishSnapshotsLocked()

	result, err := ls.Query(poolDistr2QueryFor(pkhA))
	require.NoError(t, err)
	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	distr, ok := arr[0].(olocalstatequery.PoolDistr2Result)
	require.True(t, ok)

	require.Len(t, distr.Pools, 1, "only the requested pool is reported")
	entryA, ok := distr.Pools[lcommon.PoolId(pkhA)]
	require.True(t, ok, "the requested pool must be present")
	assert.Equal(t, uint64(3_000_000), entryA.TotalPoolStake)
	assert.Equal(t, vrfA, entryA.VrfHash[:])
	_, ok = distr.Pools[lcommon.PoolId(pkhB)]
	assert.False(t, ok, "an unrequested pool must not be reported")

	assert.Equal(t, uint64(4_000_000), distr.TotalActiveStake,
		"the total stays the whole snapshot's, not the filtered subset's")
	require.NotNil(t, entryA.StakeFraction)
	assert.Equal(t, 0, entryA.StakeFraction.Cmp(big.NewRat(3, 4)),
		"the fraction stays a share of the whole snapshot, got %s",
		entryA.StakeFraction.Rat)
}

// TestQueryShelleyPoolDistr2_FilterOmitsPoolAbsentFromSnapshot covers a filter
// naming a pool the snapshot has no row for.
//
// Absent from the distribution and holding zero stake in it are different
// answers: the Haskell node restricts the distribution to the requested keys,
// so a pool that is not in it comes back missing rather than at zero. Reporting
// it at zero would also route a registered-but-unstaked pool into the
// unregistered-pool check, turning a routine "not in this snapshot" into a
// failed query.
func TestQueryShelleyPoolDistr2_FilterOmitsPoolAbsentFromSnapshot(t *testing.T) {
	db := newTestDB(t)

	vrfA := make([]byte, 32)
	for i := range vrfA {
		vrfA[i] = 0xAA
	}
	poolA := make([]byte, 28)
	for i := range poolA {
		poolA[i] = 0x11
	}
	// A pool with neither a registration nor a snapshot row -- the shape of a
	// key a caller asks about that this chain knows nothing of.
	unknown := make([]byte, 28)
	for i := range unknown {
		unknown[i] = 0x99
	}
	unknownPkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(unknown))

	const snapshotEpoch = 0
	pkhA := seedPoolDistr2Fixture(t, db, poolA, vrfA, 3_000_000, snapshotEpoch)

	ls := &LedgerState{db: db}
	ls.publishSnapshotsLocked()

	result, err := ls.Query(poolDistr2QueryFor(pkhA, unknownPkh))
	require.NoError(t, err,
		"a pool the snapshot does not hold is omitted, not an error")
	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	distr, ok := arr[0].(olocalstatequery.PoolDistr2Result)
	require.True(t, ok)

	require.Len(t, distr.Pools, 1)
	_, ok = distr.Pools[lcommon.PoolId(pkhA)]
	assert.True(t, ok, "the pool the snapshot holds is still reported")
	_, ok = distr.Pools[lcommon.PoolId(unknownPkh)]
	assert.False(t, ok, "a pool absent from the snapshot is not reported")
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
	// One, not zero: the ledger types this field as a NonZero Coin, so a zero
	// total is not decodable by the client at all.
	assert.Equal(t, uint64(1), distr.TotalActiveStake)
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

// TestQueryShelleyPoolDistr2_PrefersRegistrationVrfKey covers a pool whose
// denormalized VRF hash disagrees with its newest registration.
//
// A pool that re-registers with a new VRF key can leave the copy on the pool
// row behind. Reporting that copy would have cardano-cli check leadership
// against a key the producer no longer uses, so the registration in force is
// what the reply carries.
func TestQueryShelleyPoolDistr2_PrefersRegistrationVrfKey(t *testing.T) {
	db := newTestDB(t)

	staleVrf := make([]byte, 32)
	for i := range staleVrf {
		staleVrf[i] = 0xDD
	}
	currentVrf := make([]byte, 32)
	for i := range currentVrf {
		currentVrf[i] = 0xEE
	}
	poolKeyHash := make([]byte, 28)
	for i := range poolKeyHash {
		poolKeyHash[i] = 0x33
	}
	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolKeyHash))

	// The pool row keeps the superseded key; the registration carries the one
	// in force.
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{PoolKeyHash: pkh.Bytes(), VrfKeyHash: staleVrf},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  currentVrf,
			AddedSlot:   2,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))
	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:        0,
			SnapshotType: snapshotTypeMark,
			PoolKeyHash:  pkh.Bytes(),
			TotalStake:   dbtypes.Uint64(1_000_000),
			CapturedSlot: 1,
		},
		nil,
	))

	ls := &LedgerState{db: db}
	ls.publishSnapshotsLocked()

	result, err := ls.Query(poolDistr2Query())
	require.NoError(t, err)
	arr, _ := result.([]any)
	require.Len(t, arr, 1)
	distr, ok := arr[0].(olocalstatequery.PoolDistr2Result)
	require.True(t, ok)

	entry, ok := distr.Pools[lcommon.PoolId(pkh)]
	require.True(t, ok, "pool missing from the distribution")
	assert.Equal(t, currentVrf, entry.VrfHash[:],
		"the registration in force decides the VRF key, not the pool row copy")
}

// TestQueryShelleyPoolDistr2_VrfKeyMatchesHeaderValidation is the property that
// decides which registration the reply should carry, and it is the reason this
// query does not pick the registration that was in force when the snapshot was
// taken.
//
// A leadership schedule is only useful if the node that produced it will accept
// the blocks it promises, so the key the reply names has to be the key a block
// must carry to get past verifyRegisteredVrfKey.
//
// The expected value is therefore taken from a real block header that the
// validator is made to accept, not from the resolution helper the query itself
// calls -- asserting against that helper would only restate the query's own
// implementation and would hold even if the helper returned the wrong key.
func TestQueryShelleyPoolDistr2_VrfKeyMatchesHeaderValidation(t *testing.T) {
	tb := createTestBlock(t, [32]byte{91}, 0, tamperNone)
	ls, db := newEligibilityTestLedger(t, tb.epochNonce)

	headerVrfKey, ok, err := headerVrfKeyFromBodyCbor(tb.block.Header())
	require.NoError(t, err)
	require.True(t, ok)
	require.NotEmpty(t, headerVrfKey)
	// What the chain will hold this block's producer to.
	acceptedVrfHash := lcommon.Blake2b256Hash(headerVrfKey)

	issuerHash := tb.block.IssuerVkey().Hash()
	pkh := lcommon.PoolKeyHash(issuerHash)

	// The registration carries the key the header uses; the pool row keeps a
	// superseded copy, so a reply built from the wrong one is distinguishable
	// from a reply built from the right one.
	supersededVrf := make([]byte, 32)
	for i := range supersededVrf {
		supersededVrf[i] = 0x01
	}
	require.NotEqual(t, supersededVrf, acceptedVrfHash.Bytes())
	require.NoError(t, db.Metadata().ImportPool(
		&models.Pool{PoolKeyHash: pkh.Bytes(), VrfKeyHash: supersededVrf},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  acceptedVrfHash.Bytes(),
			AddedSlot:   1,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
		nil,
	))

	// The premise: this block passes the validator. Whatever key that took is
	// the key an operator's schedule has to be computed against.
	require.NoError(t, ls.verifyRegisteredVrfKey(tb.block),
		"fixture must be a block the validator accepts")

	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:        0,
			SnapshotType: snapshotTypeMark,
			PoolKeyHash:  pkh.Bytes(),
			TotalStake:   dbtypes.Uint64(2_000_000),
			CapturedSlot: 1,
		},
		nil,
	))

	result, err := ls.Query(poolDistr2Query())
	require.NoError(t, err)
	arr, _ := result.([]any)
	require.Len(t, arr, 1)
	distr, ok := arr[0].(olocalstatequery.PoolDistr2Result)
	require.True(t, ok)
	entry, ok := distr.Pools[lcommon.PoolId(pkh)]
	require.True(t, ok, "pool missing from the distribution")

	assert.Equal(t, acceptedVrfHash.Bytes(), entry.VrfHash[:],
		"the reply must name the VRF key of a header the validator accepts")
}
