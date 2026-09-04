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
	olocalstatequery "github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

func currentProtocolParamsQuery() *olocalstatequery.BlockQuery {
	return &olocalstatequery.BlockQuery{
		Query: &olocalstatequery.ShelleyQuery{
			Query: &olocalstatequery.ShelleyCurrentProtocolParamsQuery{},
		},
	}
}

// TestCurrentProtocolParamsQueryOmitsSyntheticV2 keeps the internal fallback
// available for script validation but omits it from the wire reply. The
// transition is constructed through the same transitionToEra path used by
// ledger startup and block processing, so this test also covers provenance
// tracking rather than merely testing a map-copy helper.
func TestCurrentProtocolParamsQueryOmitsSyntheticV2(t *testing.T) {
	ls := newPoolDistr2Ledger(t, newTestDB(t))
	ls.activeEras = eras.ErasWithDijkstra
	ls.currentEra = eras.AlonzoEraDesc
	previous := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}
	ls.currentPParams = previous

	transition, err := ls.transitionToEra(
		nil,
		eras.BabbageEraDesc.Id,
		0,
		0,
		previous,
	)
	require.NoError(t, err)
	ls.applyEraTransition(transition)
	ls.publishSnapshotsLocked()

	result, err := ls.Query(currentProtocolParamsQuery())
	require.NoError(t, err)
	values, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, values, 1)
	replied, ok := values[0].(*babbage.BabbageProtocolParameters)
	require.True(t, ok)

	require.Contains(t, replied.CostModels, uint(0),
		"a genuine PlutusV1 model must survive the reply")
	require.NotContains(t, replied.CostModels, uint(1),
		"HardForkBabbage's synthetic PlutusV2 model must be omitted")
	require.Contains(t, previous.CostModels, uint(0))

	internal, ok := ls.currentPParams.(*babbage.BabbageProtocolParameters)
	require.True(t, ok)
	require.Contains(t, internal.CostModels, uint(1),
		"internal validation must retain the synthetic model")
}

// TestCurrentProtocolParamsQueryIncludesExistingV2 verifies that a real
// PlutusV2 model present before the Babbage transition is not classified as a
// hard-fork fallback.
func TestCurrentProtocolParamsQueryIncludesExistingV2(t *testing.T) {
	ls := newPoolDistr2Ledger(t, newTestDB(t))
	ls.activeEras = eras.ErasWithDijkstra
	ls.currentEra = eras.AlonzoEraDesc
	previous := &alonzo.AlonzoProtocolParameters{
		CostModels: map[uint][]int64{
			0: {1, 2, 3},
			1: {4, 5, 6},
		},
	}
	ls.currentPParams = previous

	transition, err := ls.transitionToEra(
		nil,
		eras.BabbageEraDesc.Id,
		0,
		0,
		previous,
	)
	require.NoError(t, err)
	ls.applyEraTransition(transition)
	ls.publishSnapshotsLocked()

	result, err := ls.Query(currentProtocolParamsQuery())
	require.NoError(t, err)
	values := result.([]any)
	replied := values[0].(*babbage.BabbageProtocolParameters)
	require.Equal(t, []int64{4, 5, 6}, replied.CostModels[1])
}
