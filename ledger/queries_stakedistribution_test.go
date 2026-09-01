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

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	arr, ok := result.([]any)
	require.True(t, ok, "expected the []any result wrapper")
	require.Len(t, arr, 1)

	dist, ok := arr[0].(olocalstatequery.StakeDistributionResult)
	require.True(t, ok, "expected a StakeDistributionResult, got %T", arr[0])
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
	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)

	dist, ok := arr[0].(olocalstatequery.StakeDistributionResult)
	require.True(t, ok)
	assert.Empty(t, dist.Results)
}
