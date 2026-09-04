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

	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// protocolParamsQuery wraps GetCurrentProtocolParams the way the wire
// delivers it, matching poolDistr2Query/stakeDistributionQuery in the
// neighboring query test files.
func protocolParamsQuery() *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyCurrentProtocolParamsQuery{},
		},
	}
}

// TestInjectedSyntheticV2CostModel_DetectsHardForkBabbagesDefault covers the
// actual code path this session found responsible for blinklabs-io/dingo#3825:
// HardForkBabbage fabricates a PlutusV2 cost model whenever the previous
// era's params don't have one -- real for any Alonzo genesis, since the
// AlonzoGenesisCostModels format predates PlutusV2 entirely and never has a
// slot for it.
func TestInjectedSyntheticV2CostModel_DetectsHardForkBabbagesDefault(t *testing.T) {
	prev := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}
	after, err := eras.HardForkBabbage(nil, prev)
	require.NoError(t, err)

	assert.True(t, injectedSyntheticV2CostModel(prev, after))
}

// TestInjectedSyntheticV2CostModel_FalseWhenAlreadyPresent covers a pparams
// value that already carries a real (non-fabricated) PlutusV2 entry before
// the transition -- HardForkBabbage's own guard (`if _, hasV2 :=
// ret.CostModels[1]; !hasV2`) leaves it untouched, so nothing was injected.
func TestInjectedSyntheticV2CostModel_FalseWhenAlreadyPresent(t *testing.T) {
	realV2 := []int64{9, 9, 9}
	prev := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}, 1: realV2},
	}
	after, err := eras.HardForkBabbage(nil, prev)
	require.NoError(t, err)

	assert.False(t, injectedSyntheticV2CostModel(prev, after))
}

// TestInjectedSyntheticV2CostModel_FalseWhenValueIsNotTheKnownDefault covers
// a hypothetical newly-added key 1 whose value does not match
// eras.DefaultPlutusV2CostModel -- only the exact known fabricated value
// counts as synthetic, not "any new key 1."
func TestInjectedSyntheticV2CostModel_FalseWhenValueIsNotTheKnownDefault(t *testing.T) {
	before := &babbage.BabbageProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}
	after := &babbage.BabbageProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}, 1: {999}},
	}

	assert.False(t, injectedSyntheticV2CostModel(before, after))
}

// TestWithoutSyntheticV2CostModel_RemovesKeyWithoutMutatingOriginal covers
// the query-boundary filter: when synthetic is true, the returned value
// omits PlutusV2 while every other key survives, and the original pparams
// (still reachable from internal validation state) is never mutated.
func TestWithoutSyntheticV2CostModel_RemovesKeyWithoutMutatingOriginal(t *testing.T) {
	original := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{
			0: {1, 1, 1},
			1: {2, 2, 2},
			2: {3, 3, 3},
		},
	}

	filtered := withoutSyntheticV2CostModel(original, true)

	fp, ok := filtered.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.NotContains(t, fp.CostModels, uint(1))
	assert.Equal(t, []int64{1, 1, 1}, fp.CostModels[0])
	assert.Equal(t, []int64{3, 3, 3}, fp.CostModels[2])

	// The original, still reachable from ls.currentPParams / the published
	// snapshot for internal script validation, must be untouched.
	assert.Contains(t, original.CostModels, uint(1))
	assert.Equal(t, []int64{2, 2, 2}, original.CostModels[1])
}

// TestWithoutSyntheticV2CostModel_NoOpWhenNotSynthetic covers the common
// case: once real data has been observed (or none was ever fabricated),
// the filter must return the value unchanged, identical pointer included,
// so a caller reading it sees the exact same struct internal validation
// uses.
func TestWithoutSyntheticV2CostModel_NoOpWhenNotSynthetic(t *testing.T) {
	pp := &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{0: {1}, 1: {2}, 2: {3}},
	}

	got := withoutSyntheticV2CostModel(pp, false)

	assert.Same(t, pp, got)
}

// TestQueryShelleyCurrentProtocolParams_OmitsSyntheticV2CostModel is the
// end-to-end regression test for blinklabs-io/dingo#3825: confirmed against
// a real cardano-node's raw wire bytes (captured via a temporary diagnostic,
// decoded with the real client-side type, independent of any display-layer
// bug) that on a chain which has never received a real PlutusV2
// cost-model update, a real cardano-node's GetCurrentProtocolParams reply
// has no PlutusV2 entry at all -- while Dingo's internal state always
// carries HardForkBabbage's fabricated one, needed for real script
// validation. The LocalStateQuery reply must match the real node's
// observable behavior; internal validation must not be affected.
func TestQueryShelleyCurrentProtocolParams_OmitsSyntheticV2CostModel(t *testing.T) {
	ls := newPoolDistr2Ledger(t, newTestDB(t))
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{
			0: {1, 1, 1},
			1: eras.DefaultPlutusV2CostModel,
			2: {3, 3, 3},
		},
	}
	ls.syntheticV2CostModel = true
	ls.publishSnapshotsLocked()

	result, err := ls.Query(protocolParamsQuery())
	require.NoError(t, err)

	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	pp, ok := arr[0].(*conway.ConwayProtocolParameters)
	require.True(t, ok)

	assert.NotContains(t, pp.CostModels, uint(1),
		"the reply must omit the synthetic PlutusV2 cost model")
	assert.Contains(t, pp.CostModels, uint(0))
	assert.Contains(t, pp.CostModels, uint(2))

	// Internal validation state must be completely unaffected by the query.
	internal, ok := ls.currentPParams.(*conway.ConwayProtocolParameters)
	require.True(t, ok)
	assert.Contains(t, internal.CostModels, uint(1),
		"internal state must keep the default for real script validation")
}

// TestQueryShelleyCurrentProtocolParams_IncludesRealV2CostModel covers the
// other half: once real governance data has cleared the synthetic marker
// (LedgerState.syntheticV2CostModel == false), the reply must include
// whatever is actually in CostModels -- including a value that happens to
// equal the known synthetic default, since real governance re-affirming
// that exact value is still real data, not still a guess.
func TestQueryShelleyCurrentProtocolParams_IncludesRealV2CostModel(t *testing.T) {
	ls := newPoolDistr2Ledger(t, newTestDB(t))
	ls.currentEra = eras.ConwayEraDesc
	ls.currentPParams = &conway.ConwayProtocolParameters{
		CostModels: map[uint][]int64{
			0: {1, 1, 1},
			1: eras.DefaultPlutusV2CostModel,
			2: {3, 3, 3},
		},
	}
	ls.syntheticV2CostModel = false
	ls.publishSnapshotsLocked()

	result, err := ls.Query(protocolParamsQuery())
	require.NoError(t, err)

	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	pp, ok := arr[0].(*conway.ConwayProtocolParameters)
	require.True(t, ok)

	assert.Contains(t, pp.CostModels, uint(1))
	assert.Equal(t, eras.DefaultPlutusV2CostModel, pp.CostModels[1])
}
