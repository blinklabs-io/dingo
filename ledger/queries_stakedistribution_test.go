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
	"bytes"
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
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

// decodeStakeDistributionResult round-trips a handler's returned []any
// through CBOR the way the wire actually does: encoded by the server exactly
// as protocol/localstatequery/server.go encodes it, then decoded by the real
// gouroboros client-side type. Client.GetStakeDistribution decodes straight
// into a StakeDistributionResult with no wrapping, so asserting against that
// decoded value (rather than a raw type assertion on the handler's own
// []any) is what would have caught the handler double-wrapping its result
// before it shipped.
func decodeStakeDistributionResult(
	t *testing.T,
	result any,
) olocalstatequery.StakeDistributionResult {
	t.Helper()
	encoded, err := cbor.Encode(&result)
	require.NoError(t, err)
	var decoded olocalstatequery.StakeDistributionResult
	_, err = cbor.Decode(encoded, &decoded)
	require.NoError(t, err)
	return decoded
}

// stakeDistributionQuery wraps the leaf query the way the wire delivers
// it. GetStakeDistribution has no pool filter on the wire, unlike
// GetPoolDistr2 (poolDistr2Query in queries_pooldistr2_test.go).
func stakeDistributionQuery() *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyStakeDistributionQuery{},
		},
	}
}

// seedLiveStakeFixture registers a pool with a known VRF hash and gives it
// stake via a real delegated account and UTxO -- the data
// GetStakeDistribution's live reconstruction (ledger/snapshot's Calculator)
// actually reads after blinklabs-io/dingo#4152's fix, unlike
// seedPoolDistr2Fixture (queries_pooldistr2_test.go), which only writes a
// PoolStakeSnapshot row: the periodic mark/set/go snapshot GetPoolDistr2
// reads, and GetStakeDistribution no longer does.
func seedLiveStakeFixture(
	t *testing.T,
	db *database.Database,
	poolKeyHash []byte,
	vrfKeyHash []byte,
	stake uint64,
	slot uint64,
) lcommon.PoolKeyHash {
	t.Helper()
	pkh := lcommon.PoolKeyHash(lcommon.NewBlake2b224(poolKeyHash))
	require.NoError(t, db.ImportPool(
		nil,
		&models.Pool{PoolKeyHash: pkh.Bytes(), VrfKeyHash: vrfKeyHash},
		&models.PoolRegistration{
			PoolKeyHash: pkh.Bytes(),
			VrfKeyHash:  vrfKeyHash,
			AddedSlot:   slot,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
	))
	stakingKey := make([]byte, 28)
	copy(stakingKey, poolKeyHash)
	require.NoError(t, db.CreateAccount(nil, &models.Account{
		StakingKey: stakingKey,
		Pool:       pkh.Bytes(),
		AddedSlot:  slot,
		Active:     true,
	}))
	txId := make([]byte, 32)
	copy(txId, poolKeyHash)
	require.NoError(t, db.CreateUtxo(nil, &models.Utxo{
		TxId:       txId,
		OutputIdx:  0,
		StakingKey: stakingKey,
		Amount:     dbtypes.Uint64(stake),
		AddedSlot:  slot,
	}))
	return pkh
}

// TestQueryShelleyStakeDistribution_ReportsFractionAndVrf covers
// GetStakeDistribution's live reconstruction: unlike GetPoolDistr2
// (queryShelleyPoolDistr2), which reads the periodic mark snapshot, this
// query reads live stake (blinklabs-io/dingo#4152), so its fixture is real
// delegated accounts and UTxOs rather than a PoolStakeSnapshot row.
func TestQueryShelleyStakeDistribution_ReportsFractionAndVrf(t *testing.T) {
	t.Parallel()

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

	pkhA := seedLiveStakeFixture(t, db, poolA, vrfA, 3_000_000, 1)
	pkhB := seedLiveStakeFixture(t, db, poolB, vrfB, 1_000_000, 1)

	// Genesis/circulation is configured explicitly (matching a real running
	// node, which always has both -- see totalCirculatingSupply's doc
	// comment) so this test's fraction assertions are pinned to a known
	// circulation (4_000_000, with nothing in reserves) rather than
	// depending on totalCirculatingSupply's own fallback path, which
	// answers from the mark-snapshot total-active-stake table -- a
	// different, unrelated number now that this query's own numerator is
	// live stake, not the mark snapshot (blinklabs-io/dingo#4152).
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.1,
		"epochLength": 100,
		"maxLovelaceSupply": 4000000,
		"securityParam": 10,
		"slotLength": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	require.NoError(t, db.Metadata().SetNetworkState(0, 0, 1, nil))

	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = cfg
	seedEpochs(t, ls, map[uint64]uint64{0: 0})
	tipHash := bytes.Repeat([]byte{0xEF}, 32)
	seedBlockAtSlot(t, ls, 1, tipHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1, tipHash),
	}, nil))

	result, err := ls.Query(stakeDistributionQuery(), QueryPoint{})
	require.NoError(t, err)
	dist := decodeStakeDistributionResult(t, result)
	require.Len(t, dist.Results, 2)

	entryA, ok := dist.Results[lcommon.PoolId(pkhA)]
	require.True(t, ok, "pool A missing from the distribution")
	require.NotNil(t, entryA.StakeFraction)
	assert.Equal(t, int64(3), entryA.StakeFraction.Num().Int64())
	assert.Equal(t, int64(4), entryA.StakeFraction.Denom().Int64())
	assert.Equal(t, vrfA, entryA.VrfHash[:],
		"the VRF hash is what a caller checks their own key against")

	entryB, ok := dist.Results[lcommon.PoolId(pkhB)]
	require.True(t, ok, "pool B missing from the distribution")
	require.NotNil(t, entryB.StakeFraction)
	assert.Equal(t, int64(1), entryB.StakeFraction.Num().Int64())
	assert.Equal(t, int64(4), entryB.StakeFraction.Denom().Int64())
	assert.Equal(t, vrfB, entryB.VrfHash[:])
}

// stakeDistributionCborQuery wraps the leaf query in GetCBOR (Shelley
// sub-query 9, ShelleyCborQuery), matching poolDistr2CborQuery in
// queries_pooldistr2_test.go.
func stakeDistributionCborQuery() *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyCborQuery{
				Query: &olocalstatequery.ShelleyStakeDistributionQuery{},
			},
		},
	}
}

// TestQueryShelleyStakeDistribution_ViaGetCBOR covers GetStakeDistribution
// wrapped in the GetCBOR combinator (queryShelleyCbor), mirroring
// TestQueryShelleyPoolDistr2_ViaGetCBOR. StakeDistributionResult is a
// one-field cbor.StructAsArray struct (unlike PoolDistr2Result's two
// fields), which is exactly the shape queryShelleyCbor's own doc comment
// says it treats differently (unwrapping a single-element inner result
// rather than keeping it as a one-element array) -- this proves whether
// that special case is actually correct for a genuine one-field
// StructAsArray result, or whether it strips a wrapping layer the real
// client-side type still expects.
func TestQueryShelleyStakeDistribution_ViaGetCBOR(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)

	vrfA := make([]byte, 32)
	for i := range vrfA {
		vrfA[i] = 0xAA
	}
	poolA := make([]byte, 28)
	for i := range poolA {
		poolA[i] = 0x11
	}
	pkhA := seedLiveStakeFixture(t, db, poolA, vrfA, 3_000_000, 1)

	ls := newPoolDistr2Ledger(t, db)
	seedEpochs(t, ls, map[uint64]uint64{0: 0})
	tipHash := bytes.Repeat([]byte{0xEF}, 32)
	seedBlockAtSlot(t, ls, 1, tipHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1, tipHash),
	}, nil))

	result, err := ls.Query(stakeDistributionCborQuery(), QueryPoint{})
	require.NoError(
		t,
		err,
		"GetCBOR-wrapped GetStakeDistribution must not error",
	)

	arr, ok := result.([]any)
	require.True(t, ok, "expected the []any result wrapper")
	require.Len(t, arr, 1)
	tag, ok := arr[0].(cbor.Tag)
	require.True(t, ok, "expected a tag-24 CBOR.Tag, got %T", arr[0])
	assert.EqualValues(t, cbor.CborTagCbor, tag.Number)

	content, ok := tag.Content.([]byte)
	require.True(
		t,
		ok,
		"tag content must be raw CBOR bytes, got %T",
		tag.Content,
	)

	// The tag-24 content must decode via the same real client-side type a
	// direct (non-GetCBOR) GetStakeDistribution reply does: proof that
	// GetCBOR carries the identical value, just CBOR-in-CBOR encoded.
	var dist olocalstatequery.StakeDistributionResult
	_, err = cbor.Decode(content, &dist)
	require.NoError(
		t,
		err,
		"tag-24 content must decode as a StakeDistributionResult",
	)

	entryA, ok := dist.Results[lcommon.PoolId(pkhA)]
	require.True(t, ok, "pool missing from the GetCBOR-wrapped distribution")
	require.NotNil(t, entryA.StakeFraction)
	assert.Equal(t, vrfA, entryA.VrfHash[:])
}

// TestQueryShelleyStakeDistribution_UsesCirculationNotGetPoolDistr2sTotal
// covers blinklabs-io/dingo#3824: a live devnet run against a real
// cardano-node found GetStakeDistribution's reported fraction inflated 2x,
// because it shared GetPoolDistr2's denominator (sum of delegated stake).
// Confirmed with real cardano-node's own raw wire bytes (not just the
// decoded fraction) that its GetStakeDistribution reply genuinely uses total
// circulation instead -- a real, deliberate difference between the two
// queries, not a bug in either one. GetPoolDistr2's own denominator is
// correct as sum-of-delegated (matching real cardano-ledger's
// calculatePoolDistr/SnapShot.ssTotalActiveStake) and must not change --
// this test proves the two queries now correctly disagree on the same
// underlying data, rather than being kept artificially consistent.
//
// The two queries also now read from different sources for the numerator
// itself (blinklabs-io/dingo#4152: GetPoolDistr2 from the mark snapshot,
// GetStakeDistribution from live stake), so this fixture seeds both a
// PoolStakeSnapshot row (for GetPoolDistr2) and a live delegated
// account/UTxO (for GetStakeDistribution) naming the same two pools with
// the same stake, to isolate the denominator difference this test is
// actually about from the (separately tested) numerator-source difference.
func TestQueryShelleyStakeDistribution_UsesCirculationNotGetPoolDistr2sTotal(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)

	const snapshotEpoch = 0
	poolAHash := repeatedBytes(28, 0xAA)
	poolBHash := repeatedBytes(28, 0xBB)
	vrfA := repeatedBytes(32, 0x01)
	vrfB := repeatedBytes(32, 0x02)
	pkhA := seedPoolDistr2Fixture(
		t,
		db,
		poolAHash,
		vrfA,
		1_000_000,
		snapshotEpoch,
	)
	seedPoolDistr2Fixture(t, db, poolBHash, vrfB, 1_000_000, snapshotEpoch)
	seedLiveStakeFixture(t, db, poolAHash, vrfA, 1_000_000, 1)
	seedLiveStakeFixture(t, db, poolBHash, vrfB, 1_000_000, 1)

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.1,
		"epochLength": 100,
		"maxLovelaceSupply": 8000000,
		"securityParam": 10,
		"slotLength": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	// Half the genesis supply sits in reserves, undelegated: circulation is
	// 8_000_000 - 4_000_000 = 4_000_000, twice the 2_000_000 the two pools
	// hold delegated between them.
	require.NoError(t, db.Metadata().SetNetworkState(0, 4_000_000, 1, nil))

	ls := newPoolDistr2Ledger(t, db)
	ls.config.CardanoNodeConfig = cfg
	seedEpochs(t, ls, map[uint64]uint64{0: 0})
	tipHash := bytes.Repeat([]byte{0xEF}, 32)
	seedBlockAtSlot(t, ls, 1, tipHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(1, tipHash),
	}, nil))

	// GetPoolDistr2 must be unaffected: still sum-of-delegated (2_000_000),
	// so each pool is 1/2.
	poolDistr2Result, err := ls.Query(poolDistr2Query(), QueryPoint{})
	require.NoError(t, err)
	poolDistr2 := decodePoolDistr2Result(t, poolDistr2Result)
	entryA2, ok := poolDistr2.Pools[lcommon.PoolId(pkhA)]
	require.True(t, ok)
	assert.Equal(t, 0, entryA2.StakeFraction.Cmp(big.NewRat(1, 2)),
		"GetPoolDistr2 must keep using sum-of-delegated stake as its total")
	assert.Equal(t, uint64(2_000_000), poolDistr2.TotalActiveStake)

	// GetStakeDistribution must use circulation (4_000_000) instead, so each
	// pool is 1/4 -- not 1/2.
	stakeDistResult, err := ls.Query(stakeDistributionQuery(), QueryPoint{})
	require.NoError(t, err)
	stakeDist := decodeStakeDistributionResult(t, stakeDistResult)
	entryA, ok := stakeDist.Results[lcommon.PoolId(pkhA)]
	require.True(t, ok)
	require.NotNil(t, entryA.StakeFraction)
	assert.Equal(t, int64(1), entryA.StakeFraction.Num().Int64())
	assert.Equal(t, int64(4), entryA.StakeFraction.Denom().Int64(),
		"GetStakeDistribution must use total circulation, not "+
			"GetPoolDistr2's sum-of-delegated total")
}

// TestQueryShelleyStakeDistribution_PinnedAtLiveTip_Succeeds covers
// node-parity's own usage (blinklabs-io/dingo#1900): Check pins every
// query, including GetStakeDistribution, to whatever point the two nodes
// just agreed was live -- so a pin naming exactly the current tip must
// succeed rather than being rejected as an unsupported historical pin.
// This asserts only that the call succeeds and returns a fraction, not a
// specific value: the fixture leaves config.CardanoNodeConfig nil, so
// totalCirculatingSupply answers from its totalActiveStake fallback here
// rather than genesis-derived circulation -- the accept-vs-reject behavior
// under test does not depend on which denominator path answered it. See
// TestQueryShelleyStakeDistribution_UsesCirculationNotGetPoolDistr2sTotal
// above for a test that does configure genesis/network_state and asserts
// the resulting fraction.
func TestQueryShelleyStakeDistribution_PinnedAtLiveTip_Succeeds(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	poolAHash := repeatedBytes(28, 0x11)
	vrfA := repeatedBytes(32, 0xAA)
	pkhA := seedLiveStakeFixture(t, db, poolAHash, vrfA, 3_000_000, 50)
	ls := newPoolDistr2Ledger(t, db)
	seedEpochs(t, ls, map[uint64]uint64{0: 0})

	tipHash := bytes.Repeat([]byte{0xEF}, 32)
	seedBlockAtSlot(t, ls, 50, tipHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(50, tipHash),
	}, nil))

	result, err := ls.Query(
		stakeDistributionQuery(),
		QueryPoint{Slot: 50, Hash: tipHash},
	)
	require.NoError(
		t,
		err,
		"a pin naming exactly the live tip must be answered",
	)
	dist := decodeStakeDistributionResult(t, result)
	entryA, ok := dist.Results[lcommon.PoolId(pkhA)]
	require.True(t, ok)
	require.NotNil(t, entryA.StakeFraction)
}

// TestQueryShelleyStakeDistribution_PinnedBehindLiveTip_UsesHistoricalCirculatingSupply
// covers the real #382 fix this handler no longer works around: a pin naming
// a real, on-chain point behind the live tip is now answered, and with the
// circulating supply that was actually true at that point
// (GetNetworkStateAsOfSlot), not whatever reserves are live now. Reserves
// are seeded to a deliberately different value at the pinned slot than at
// the live tip, so a query that silently fell back to live reserves (the
// bug this closes) would report the wrong fraction rather than merely
// erroring -- a stronger check than "does it return an error."
func TestQueryShelleyStakeDistribution_PinnedBehindLiveTip_UsesHistoricalCirculatingSupply(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	poolAHash := repeatedBytes(28, 0x11)
	vrfA := repeatedBytes(32, 0xAA)
	pkhA := seedLiveStakeFixture(t, db, poolAHash, vrfA, 1_000_000, 100)
	ls := newPoolDistr2Ledger(t, db)
	seedEpochs(t, ls, map[uint64]uint64{0: 0, 100: 1, 200: 2})

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.1,
		"epochLength": 100,
		"maxLovelaceSupply": 8000000,
		"securityParam": 10,
		"slotLength": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	ls.config.CardanoNodeConfig = cfg

	pastHash := bytes.Repeat([]byte{0xAB}, 32)
	tipHash := bytes.Repeat([]byte{0xCD}, 32)
	seedBlockAtSlot(t, ls, 100, pastHash)
	seedBlockAtSlot(t, ls, 200, tipHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(200, tipHash),
	}, nil))

	// As of slot 100 (the pinned point): reserves 4_000_000, so circulation
	// is 8_000_000 - 4_000_000 = 4_000_000 and pool A's 1_000_000 is 1/4.
	require.NoError(t, db.Metadata().SetNetworkState(0, 4_000_000, 100, nil))
	// As of slot 200 (the live tip, seeded later so it is the latest row):
	// reserves drop to 2_000_000, so live circulation is 6_000_000 and pool
	// A's own fraction would be 1/6 if this handler wrongly used live
	// reserves for the slot-100 pin instead of the historical row above.
	require.NoError(t, db.Metadata().SetNetworkState(0, 2_000_000, 200, nil))

	result, err := ls.Query(
		stakeDistributionQuery(),
		QueryPoint{Slot: 100, Hash: pastHash},
	)
	require.NoError(t, err, "a real historical pin must now be answered")
	dist := decodeStakeDistributionResult(t, result)
	entryA, ok := dist.Results[lcommon.PoolId(pkhA)]
	require.True(t, ok)
	require.NotNil(t, entryA.StakeFraction)
	assert.Equal(t, int64(1), entryA.StakeFraction.Num().Int64())
	assert.Equal(
		t, int64(4), entryA.StakeFraction.Denom().Int64(),
		"must use slot 100's own reserves (4_000_000), not the live "+
			"tip's (2_000_000)",
	)

	// The live (unpinned) query is unaffected: it still reads the latest
	// NetworkState row, giving pool A a 1/6 fraction against 6_000_000
	// circulation.
	liveResult, err := ls.Query(stakeDistributionQuery(), QueryPoint{})
	require.NoError(t, err)
	liveDist := decodeStakeDistributionResult(t, liveResult)
	liveEntryA, ok := liveDist.Results[lcommon.PoolId(pkhA)]
	require.True(t, ok)
	require.NotNil(t, liveEntryA.StakeFraction)
	assert.Equal(t, int64(1), liveEntryA.StakeFraction.Num().Int64())
	assert.Equal(t, int64(6), liveEntryA.StakeFraction.Denom().Int64())
}

// TestQueryShelleyStakeDistribution_PinnedBeforeAnyNetworkStateRow_Rejected
// covers a pin older than every recorded network_state row: unlike
// TestQueryShelleyStakeDistribution_PinnedBehindLiveTip_UsesHistoricalCirculatingSupply,
// there is no historical reserves row to answer from, so this must fail
// with ErrHistoricalStateUnavailable rather than silently fall back to
// totalActiveStake -- a different, non-equivalent total (see
// totalCirculatingSupply's doc comment) that would look like a plausible
// answer instead of an honest rejection.
func TestQueryShelleyStakeDistribution_PinnedBeforeAnyNetworkStateRow_Rejected(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	seedLiveStakeFixture(
		t, db, repeatedBytes(28, 0x11), repeatedBytes(32, 0xAA),
		1_000_000, 100,
	)
	ls := newPoolDistr2Ledger(t, db)
	seedEpochs(t, ls, map[uint64]uint64{0: 0})

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"activeSlotsCoeff": 0.1,
		"epochLength": 100,
		"maxLovelaceSupply": 8000000,
		"securityParam": 10,
		"slotLength": 1,
		"systemStart": "2022-10-25T00:00:00Z"
	}`)))
	ls.config.CardanoNodeConfig = cfg

	pastHash := bytes.Repeat([]byte{0xAB}, 32)
	tipHash := bytes.Repeat([]byte{0xCD}, 32)
	seedBlockAtSlot(t, ls, 100, pastHash)
	seedBlockAtSlot(t, ls, 200, tipHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(200, tipHash),
	}, nil))

	// The only network_state row is at slot 200 -- after the slot-100 pin,
	// so GetNetworkStateAsOfSlot(100) finds nothing.
	require.NoError(t, db.Metadata().SetNetworkState(0, 2_000_000, 200, nil))

	_, err := ls.Query(
		stakeDistributionQuery(),
		QueryPoint{Slot: 100, Hash: pastHash},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrHistoricalStateUnavailable)
}

// TestQueryShelleyStakeDistribution_PinnedVrfKeyUsesSlotNotLatestRegistration
// covers a CodeRabbit finding on blinklabs-io/dingo#4237: a pool that
// re-registers with a new VRF key after a pinned query's slot must still be
// reported with the key it held at that slot, not its current one. Before
// this fix, poolVrfKeyHashes always resolved through
// registeredPoolVrfKeyHash's unbounded "latest registration" rule (needed for
// live callers, since dingo validates incoming blocks against a producer's
// current key), which a pinned historical query has no business using -- it
// would pair pool A's slot-100 stake with the VRF key it only registered at
// slot 150, a key dingo itself would not have accepted from that pool as of
// slot 100.
func TestQueryShelleyStakeDistribution_PinnedVrfKeyUsesSlotNotLatestRegistration(
	t *testing.T,
) {
	t.Parallel()

	db := newTestDB(t)
	poolAHash := repeatedBytes(28, 0x11)
	vrfOld := repeatedBytes(32, 0xAA)
	vrfNew := repeatedBytes(32, 0xBB)

	pkhA := seedLiveStakeFixture(t, db, poolAHash, vrfOld, 1_000_000, 1)
	// Re-registers the same pool with a new VRF key at slot 150 -- after the
	// slot-100 pin below, but before the live tip.
	require.NoError(t, db.ImportPool(
		nil,
		&models.Pool{PoolKeyHash: poolAHash, VrfKeyHash: vrfNew},
		&models.PoolRegistration{
			PoolKeyHash: poolAHash,
			VrfKeyHash:  vrfNew,
			AddedSlot:   150,
			Pledge:      dbtypes.Uint64(1),
			Cost:        dbtypes.Uint64(1),
		},
	))

	ls := newPoolDistr2Ledger(t, db)
	seedEpochs(t, ls, map[uint64]uint64{0: 0, 100: 1, 200: 2})

	pastHash := bytes.Repeat([]byte{0xAB}, 32)
	tipHash := bytes.Repeat([]byte{0xCD}, 32)
	seedBlockAtSlot(t, ls, 100, pastHash)
	seedBlockAtSlot(t, ls, 200, tipHash)
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(200, tipHash),
	}, nil))
	require.NoError(t, db.Metadata().SetNetworkState(0, 2_000_000, 100, nil))

	result, err := ls.Query(
		stakeDistributionQuery(),
		QueryPoint{Slot: 100, Hash: pastHash},
	)
	require.NoError(t, err)
	dist := decodeStakeDistributionResult(t, result)
	entryA, ok := dist.Results[lcommon.PoolId(pkhA)]
	require.True(t, ok, "pool must still be reported at the pinned slot")
	assert.Equal(t, vrfOld, entryA.VrfHash[:],
		"pinned slot 100 predates the slot-150 re-registration, so the "+
			"key in force then (vrfOld) must be reported, not the pool's "+
			"current key (vrfNew)")

	// The live (unpinned) query is unaffected: it must report the pool's
	// current key, since dingo validates incoming blocks against it.
	liveResult, err := ls.Query(stakeDistributionQuery(), QueryPoint{})
	require.NoError(t, err)
	liveDist := decodeStakeDistributionResult(t, liveResult)
	liveEntryA, ok := liveDist.Results[lcommon.PoolId(pkhA)]
	require.True(t, ok)
	assert.Equal(t, vrfNew, liveEntryA.VrfHash[:])
}

// TestQueryShelleyStakeDistribution_EmptySnapshot covers a chain with no
// stake yet (and no epoch data synced at all -- a completely fresh
// database): the query must return an empty, non-nil map rather than
// failing (blinklabs-io/dingo#4152: the live reconstruction's
// getActivePoolsAtSlot returns types.ErrNoEpochData in exactly this case,
// which queryShelleyStakeDistribution must treat the same way
// queryLedgerPeerSnapshot already does -- an empty result, not an error).
func TestQueryShelleyStakeDistribution_EmptySnapshot(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	result, err := ls.queryShelleyStakeDistribution(QueryPoint{}, nil)
	require.NoError(t, err)
	dist := decodeStakeDistributionResult(t, result)
	assert.Empty(t, dist.Results)
}
