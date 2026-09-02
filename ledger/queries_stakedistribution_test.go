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
