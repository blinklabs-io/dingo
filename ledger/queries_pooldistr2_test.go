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
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	dbtypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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

// newPoolDistr2Ledger builds the ledger state these tests query.
//
// The handler logs the pools it cannot report, and NewLedgerState defaults a
// nil logger so the rest of the ledger need not guard every call. These tests
// construct LedgerState directly and so have to supply one themselves.
func newPoolDistr2Ledger(
	t *testing.T,
	db *database.Database,
) *LedgerState {
	t.Helper()
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()
	return ls
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

// decodePoolDistr2Result round-trips a handler's returned []any through CBOR
// the way the wire actually does: encoded by the server exactly as
// protocol/localstatequery/server.go encodes it, then decoded by the real
// gouroboros client-side type. Client.GetPoolDistr2 decodes straight into a
// PoolDistr2Result with no wrapping, so asserting against that decoded value
// (rather than a raw type assertion on the handler's own []any) is what would
// have caught the handler double-wrapping its result before it shipped.
func decodePoolDistr2Result(
	t *testing.T,
	result any,
) olocalstatequery.PoolDistr2Result {
	t.Helper()
	encoded, err := cbor.Encode(&result)
	require.NoError(t, err)
	var decoded olocalstatequery.PoolDistr2Result
	_, err = cbor.Decode(encoded, &decoded)
	require.NoError(t, err)
	return decoded
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

	ls := newPoolDistr2Ledger(t, db)

	result, err := ls.Query(poolDistr2Query())
	require.NoError(t, err)
	distr := decodePoolDistr2Result(t, result)

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

	ls := newPoolDistr2Ledger(t, db)

	result, err := ls.Query(poolDistr2QueryFor(pkhA))
	require.NoError(t, err)
	distr := decodePoolDistr2Result(t, result)

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

	ls := newPoolDistr2Ledger(t, db)

	result, err := ls.Query(poolDistr2QueryFor(pkhA, unknownPkh))
	require.NoError(t, err,
		"a pool the snapshot does not hold is omitted, not an error")
	distr := decodePoolDistr2Result(t, result)

	require.Len(t, distr.Pools, 1)
	_, ok := distr.Pools[lcommon.PoolId(pkhA)]
	assert.True(t, ok, "the pool the snapshot holds is still reported")
	_, ok = distr.Pools[lcommon.PoolId(unknownPkh)]
	assert.False(t, ok, "a pool absent from the snapshot is not reported")
}

// TestQueryShelleyPoolDistr2_ZeroTotalStakeDoesNotDivide covers an epoch whose
// snapshot holds no stake at all, which is the state a fresh chain is in
// before its first snapshot is taken. Dividing by the total would panic.
func TestQueryShelleyPoolDistr2_ZeroTotalStakeDoesNotDivide(t *testing.T) {
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	result, err := ls.Query(poolDistr2Query())
	require.NoError(t, err,
		"an empty snapshot reports an empty distribution, not an error")
	distr := decodePoolDistr2Result(t, result)
	// One, not zero: the ledger types this field as a NonZero Coin, so a zero
	// total is not decodable by the client at all.
	assert.Equal(t, uint64(1), distr.TotalActiveStake)
	assert.Empty(t, distr.Pools)
}

// TestQueryShelleyPoolDistr2_OmitsPoolWithoutRegistrationRatherThanAborting
// covers a pool that holds snapshot stake but has no registration on record.
//
// Such a pool cannot be given a VRF key hash, and reporting one of zeroes would
// read as a real key, so it is left out. What matters is that leaving it out is
// all that happens: the rest of the distribution is still served.
//
// Returning an error instead would not fail this one query. The LocalStateQuery
// server propagates a query error as a protocol error, so the node drops the
// connection and cardano-cli reports only a closed bearer -- which is exactly
// the opaque failure #2997 was filed for. Because the unfiltered form of this
// query covers every pool in the snapshot, one unregistered pool anywhere on
// the chain would take leadership-schedule down for every operator.
//
// The fixture therefore pairs the orphan with a healthy pool and asserts the
// healthy one still answers. Asserting only the orphan's absence would pass
// just as well against a handler that returned nothing at all.
func TestQueryShelleyPoolDistr2_OmitsPoolWithoutRegistrationRatherThanAborting(
	t *testing.T,
) {
	db := newTestDB(t)

	orphan := make([]byte, 28)
	for i := range orphan {
		orphan[i] = 0x77
	}
	orphanPkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(orphan))
	// Stake in the snapshot, but no pool or registration row to match it.
	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:        0,
			SnapshotType: snapshotTypeMark,
			PoolKeyHash:  orphanPkh.Bytes(),
			TotalStake:   dbtypes.Uint64(5_000_000),
			CapturedSlot: 1,
		},
		nil,
	))

	healthy := make([]byte, 28)
	for i := range healthy {
		healthy[i] = 0x88
	}
	healthyVrf := make([]byte, 32)
	for i := range healthyVrf {
		healthyVrf[i] = 0x99
	}
	healthyPkh := seedPoolDistr2Fixture(
		t, db, healthy, healthyVrf, 5_000_000, 0,
	)

	ls := newPoolDistr2Ledger(t, db)

	result, err := ls.Query(poolDistr2Query())
	require.NoError(t, err,
		"an unregistered pool must not abort the protocol and drop the "+
			"client's connection")
	distr := decodePoolDistr2Result(t, result)

	_, present := distr.Pools[lcommon.PoolId(orphanPkh)]
	assert.False(t, present,
		"a pool with no registration has no VRF key to report, so it is "+
			"omitted rather than given one of zeroes")

	entry, present := distr.Pools[lcommon.PoolId(healthyPkh)]
	require.True(t, present,
		"the rest of the distribution must still be served")
	assert.Equal(t, uint64(5_000_000), entry.TotalPoolStake)
	// The omitted pool's stake stays in the total, so this pool's own fraction
	// is unchanged by the omission -- which is why omitting is safe for a
	// caller checking its own leadership.
	assert.Equal(t, uint64(10_000_000), distr.TotalActiveStake,
		"the total is summed over the whole snapshot, including the pool "+
			"that could not be reported")
	require.NotNil(t, entry.StakeFraction)
	assert.Equal(t, 0, entry.StakeFraction.Cmp(big.NewRat(1, 2)),
		"the reported pool's own fraction is its stake over the unchanged "+
			"total, got %s", entry.StakeFraction.Rat)
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

	ls := newPoolDistr2Ledger(t, db)

	result, err := ls.Query(poolDistr2Query())
	require.NoError(t, err)
	distr := decodePoolDistr2Result(t, result)

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
	distr := decodePoolDistr2Result(t, result)
	entry, ok := distr.Pools[lcommon.PoolId(pkh)]
	require.True(t, ok, "pool missing from the distribution")

	assert.Equal(t, acceptedVrfHash.Bytes(), entry.VrfHash[:],
		"the reply must name the VRF key of a header the validator accepts")
}

// TestQueryShelleyPoolDistr2_EpochComesFromTheTransactionNotTheSnapshot pins
// where the snapshot epoch is read from.
//
// The in-memory consensus snapshot is the cheaper source, but it is published
// after the database write that advances the chain, so it and a transaction
// opened separately can sit on opposite sides of an epoch boundary. Deriving
// the epoch from the snapshot and then reading the stake rows from the
// transaction pairs an epoch number with a distribution belonging to a
// different one -- and the result is a plausible distribution rather than an
// absent one, so nothing downstream can tell.
//
// This is the same pairing GetChainDepState resolves by reading its tip and
// epoch from the one transaction, and it is resolved the same way here.
//
// The fixture makes the two sources disagree outright rather than trying to
// land a real boundary mid-query: the epoch row covering the tip says 5 while
// the in-memory snapshot says 9. Each names a different mark snapshot, and only
// one of them is consistent with the state the query acquired.
func TestQueryShelleyPoolDistr2_EpochComesFromTheTransactionNotTheSnapshot(
	t *testing.T,
) {
	db := newTestDB(t)

	const (
		epochStart  uint64 = 5000
		epochLength uint64 = 1000
		tipSlot     uint64 = 5500
		// What the transaction sees. Leader election reads the preceding
		// epoch's mark snapshot, so this resolves to epoch 4.
		txnEpoch      uint64 = 5
		txnSnapshot   uint64 = 4
		staleEpoch    uint64 = 9
		staleSnapshot uint64 = 8
	)

	vrfKey := make([]byte, 32)
	for i := range vrfKey {
		vrfKey[i] = 0xCC
	}
	poolKey := make([]byte, 28)
	for i := range poolKey {
		poolKey[i] = 0x33
	}

	// The same pool holds different stake in the two snapshots, so the reply
	// names which one was read without needing a second pool to tell them
	// apart.
	pkh := seedPoolDistr2Fixture(
		t, db, poolKey, vrfKey, 3_000_000, txnSnapshot,
	)
	require.NoError(t, db.Metadata().SavePoolStakeSnapshot(
		&models.PoolStakeSnapshot{
			Epoch:        staleSnapshot,
			SnapshotType: snapshotTypeMark,
			PoolKeyHash:  pkh.Bytes(),
			TotalStake:   dbtypes.Uint64(7_000_000),
			CapturedSlot: 1,
		},
		nil,
	))

	require.NoError(t, db.Metadata().SetEpoch(
		epochStart,        // slot
		txnEpoch,          // epoch
		nil,               // nonce
		nil,               // evolvingNonce
		nil,               // candidateNonce
		nil,               // lastEpochBlockNonce
		0,                 // era
		1,                 // slotLength
		uint(epochLength), // lengthInSlots
		nil,               // txn
	))
	require.NoError(t, db.SetTip(
		ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, make([]byte, 32)),
		},
		nil,
	))

	ls := newPoolDistr2Ledger(t, db)
	// The snapshot the query must NOT read the epoch from. Republished after
	// the helper's own publication so this value is the one on record.
	ls.currentEpoch = models.Epoch{EpochId: staleEpoch}
	ls.publishSnapshotsLocked()

	result, err := ls.Query(poolDistr2Query())
	require.NoError(t, err)
	distr := decodePoolDistr2Result(t, result)

	entry, ok := distr.Pools[lcommon.PoolId(pkh)]
	require.True(t, ok, "pool missing from the distribution")
	assert.Equal(t, uint64(3_000_000), entry.TotalPoolStake,
		"the distribution must come from the epoch the acquired transaction "+
			"is in, not the one the in-memory snapshot had reached")
	assert.Equal(t, uint64(3_000_000), distr.TotalActiveStake,
		"the total has to be summed over that same snapshot, or the "+
			"fractions are shares of a denominator from another epoch")
}

// TestQueryShelleyPoolDistr2_TotalMatchesRowsWhenSummaryIsReady covers the
// total-active-stake read on the path a synced node actually takes.
//
// The per-pool stakes come from pool_stake_snapshot, but the total does not:
// GetTotalActiveStake short-circuits to epoch_summary.total_active_stake
// whenever that row exists with snapshot_ready set, and only falls back to
// summing the snapshot rows when it does not. Two tables, and the reply's
// fractions are meaningful only while they agree -- reading both under one
// transaction makes the pair consistent, not equal.
//
// Every other test here leaves epoch_summary empty and so silently exercises
// the SUM fallback, which is the path a synced node never takes. This one
// writes the row.
//
// The two agree by construction rather than by luck, and it is worth recording
// why. Snapshot rotation writes both in one transaction from one calculation:
// epoch_summary.total_active_stake is distribution.TotalStake, the running sum
// of the same distribution.PoolStakes the rows are built from. They could only
// drift if the rows were removed while the summary stayed, which is exactly
// what cleanupOldSnapshots does -- but it prunes below currentEpoch-3 and
// deliberately retains epoch_summary for the life of the database, while this
// query reads praos.StakeSnapshotEpoch (currentEpoch-1). The epoch it asks
// about is always inside the retained window. This test is what would fail if
// either of those bounds moved.
func TestQueryShelleyPoolDistr2_TotalMatchesRowsWhenSummaryIsReady(
	t *testing.T,
) {
	db := newTestDB(t)

	vrfA := make([]byte, 32)
	for i := range vrfA {
		vrfA[i] = 0xA1
	}
	vrfB := make([]byte, 32)
	for i := range vrfB {
		vrfB[i] = 0xB1
	}
	poolA := make([]byte, 28)
	for i := range poolA {
		poolA[i] = 0x41
	}
	poolB := make([]byte, 28)
	for i := range poolB {
		poolB[i] = 0x42
	}

	const snapshotEpoch = 0
	pkhA := seedPoolDistr2Fixture(t, db, poolA, vrfA, 3_000_000, snapshotEpoch)
	pkhB := seedPoolDistr2Fixture(t, db, poolB, vrfB, 1_000_000, snapshotEpoch)

	// What rotation writes beside the rows: the same total, marked ready, which
	// is what makes GetTotalActiveStake prefer it over summing.
	require.NoError(t, db.Metadata().SaveEpochSummary(
		&models.EpochSummary{
			Epoch:            snapshotEpoch,
			TotalActiveStake: dbtypes.Uint64(4_000_000),
			TotalPoolCount:   2,
			TotalDelegators:  0,
			BoundarySlot:     1,
			SnapshotReady:    true,
		},
		nil,
	))

	ls := newPoolDistr2Ledger(t, db)

	result, err := ls.Query(poolDistr2Query())
	require.NoError(t, err)
	distr := decodePoolDistr2Result(t, result)

	assert.Equal(t, uint64(4_000_000), distr.TotalActiveStake,
		"the total must equal the sum of the snapshot rows the per-pool "+
			"stakes came from, whichever source served it")

	entryA, ok := distr.Pools[lcommon.PoolId(pkhA)]
	require.True(t, ok, "pool A missing from the distribution")
	entryB, ok := distr.Pools[lcommon.PoolId(pkhB)]
	require.True(t, ok, "pool B missing from the distribution")

	// The invariant the query exists to preserve, asserted against the summary
	// rather than against a total this same reply just summed for itself.
	require.NotNil(t, entryA.StakeFraction)
	require.NotNil(t, entryB.StakeFraction)
	sum := new(big.Rat).Add(entryA.StakeFraction.Rat, entryB.StakeFraction.Rat)
	assert.Equal(t, 0, sum.Cmp(big.NewRat(1, 1)),
		"fractions taken over the summary's total must still sum to one, "+
			"got %s", sum)
}
