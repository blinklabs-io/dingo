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

package blockfrost

import (
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

func TestProtocolParamsInfoFromNativeDijkstra(t *testing.T) {
	t.Parallel()
	pp := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MinFeeA:      44,
			MinFeeB:      155381,
			MaxTxSize:    16384,
			MinPoolCost:  170000000,
			MaxValueSize: 5000,
			ProtocolVersion: common.ProtocolParametersProtocolVersion{
				Major: dijkstra.MinProtocolVersionDijkstra,
			},
			CostModels: map[uint][]int64{
				2: {1, 2, 3},
				3: {4, 5, 6},
			},
			DRepInactivityPeriod:       20,
			MinFeeRefScriptCostPerByte: rat(15, 1),
		},
		RefScriptCostStride: 25600,
	}

	info, err := protocolParamsInfoFromNative(pp, 900)
	require.NoError(t, err)
	require.Equal(t, uint64(900), info.Epoch)
	require.Equal(t, 44, info.MinFeeA)
	require.Equal(t, "170000000", info.MinPoolCost)
	require.Equal(t, int(dijkstra.MinProtocolVersionDijkstra), info.ProtocolMajorVer)
	require.NotNil(t, info.DRepActivity)
	require.Equal(t, "20", *info.DRepActivity)
	require.NotNil(t, info.MinFeeRefScriptCostPerByte)
	require.InDelta(t, 15.0, *info.MinFeeRefScriptCostPerByte, 1e-9)
	require.NotNil(t, info.CostModelsRaw)
	require.Equal(
		t,
		map[string][]int64{
			"PlutusV3": {1, 2, 3},
			"PlutusV4": {4, 5, 6},
		},
		*info.CostModelsRaw,
	)
}
