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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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

// TestQueryShelleyStakeDistribution_ReportsFractionAndVrf covers
// GetStakeDistribution, which reads from the same PoolStakeDistribution
// helper as GetPoolDistr2 (queryShelleyPoolDistr2), so the two queries
// cannot silently disagree about the same chain's stake distribution.
func TestQueryShelleyStakeDistribution_ReportsFractionAndVrf(t *testing.T) {
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

	// The ledger state under test reports epoch 0, and leader election
	// reads the snapshot for the preceding epoch, which at epoch 0 is
	// epoch 0 (see queryShelleyPoolDistr2's own fixture for the same
	// reasoning).
	const snapshotEpoch = 0
	pkhA := seedPoolDistr2Fixture(t, db, poolA, vrfA, 3_000_000, snapshotEpoch)
	pkhB := seedPoolDistr2Fixture(t, db, poolB, vrfB, 1_000_000, snapshotEpoch)

	ls := newPoolDistr2Ledger(t, db)

	result, err := ls.Query(stakeDistributionQuery())
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
	db := newTestDB(t)

	vrfA := make([]byte, 32)
	for i := range vrfA {
		vrfA[i] = 0xAA
	}
	poolA := make([]byte, 28)
	for i := range poolA {
		poolA[i] = 0x11
	}
	const snapshotEpoch = 0
	pkhA := seedPoolDistr2Fixture(t, db, poolA, vrfA, 3_000_000, snapshotEpoch)

	ls := newPoolDistr2Ledger(t, db)

	result, err := ls.Query(stakeDistributionCborQuery())
	require.NoError(t, err, "GetCBOR-wrapped GetStakeDistribution must not error")

	arr, ok := result.([]any)
	require.True(t, ok, "expected the []any result wrapper")
	require.Len(t, arr, 1)
	tag, ok := arr[0].(cbor.Tag)
	require.True(t, ok, "expected a tag-24 CBOR.Tag, got %T", arr[0])
	assert.EqualValues(t, cbor.CborTagCbor, tag.Number)

	content, ok := tag.Content.([]byte)
	require.True(t, ok, "tag content must be raw CBOR bytes, got %T", tag.Content)

	// The tag-24 content must decode via the same real client-side type a
	// direct (non-GetCBOR) GetStakeDistribution reply does: proof that
	// GetCBOR carries the identical value, just CBOR-in-CBOR encoded.
	var dist olocalstatequery.StakeDistributionResult
	_, err = cbor.Decode(content, &dist)
	require.NoError(t, err, "tag-24 content must decode as a StakeDistributionResult")

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
func TestQueryShelleyStakeDistribution_UsesCirculationNotGetPoolDistr2sTotal(
	t *testing.T,
) {
	db := newTestDB(t)

	const snapshotEpoch = 0
	pkhA := seedPoolDistr2Fixture(
		t, db,
		repeatedBytes(28, 0xAA), repeatedBytes(32, 0x01),
		1_000_000, snapshotEpoch,
	)
	seedPoolDistr2Fixture(
		t, db,
		repeatedBytes(28, 0xBB), repeatedBytes(32, 0x02),
		1_000_000, snapshotEpoch,
	)

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

	// GetPoolDistr2 must be unaffected: still sum-of-delegated (2_000_000),
	// so each pool is 1/2.
	poolDistr2Result, err := ls.Query(poolDistr2Query())
	require.NoError(t, err)
	poolDistr2 := decodePoolDistr2Result(t, poolDistr2Result)
	entryA2, ok := poolDistr2.Pools[lcommon.PoolId(pkhA)]
	require.True(t, ok)
	assert.Equal(t, 0, entryA2.StakeFraction.Cmp(big.NewRat(1, 2)),
		"GetPoolDistr2 must keep using sum-of-delegated stake as its total")
	assert.Equal(t, uint64(2_000_000), poolDistr2.TotalActiveStake)

	// GetStakeDistribution must use circulation (4_000_000) instead, so each
	// pool is 1/4 -- not 1/2.
	stakeDistResult, err := ls.Query(stakeDistributionQuery())
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

// TestQueryShelleyStakeDistribution_EmptySnapshot covers a chain with no
// stake snapshot yet: the query must return an empty, non-nil map rather
// than failing.
func TestQueryShelleyStakeDistribution_EmptySnapshot(t *testing.T) {
	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)

	result, err := ls.queryShelleyStakeDistribution()
	require.NoError(t, err)
	dist := decodeStakeDistributionResult(t, result)
	assert.Empty(t, dist.Results)
}
