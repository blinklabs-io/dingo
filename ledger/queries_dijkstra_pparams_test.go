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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dijkstraQueryPParams carries a synthetic PlutusV2 cost model, which the
// reply must drop, beside the Plutus V4 cost model and Dijkstra-only fields
// it must keep.
func dijkstraQueryPParams(stride uint32) *dijkstra.DijkstraProtocolParameters {
	ratPtr := func(n, d int64) *cbor.Rat { return &cbor.Rat{Rat: big.NewRat(n, d)} }
	return &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: *conwayPParamsWithCostModels(
			map[uint][]int64{
				1: eras.DefaultPlutusV2CostModel,
				2: {3, 3, 3},
				3: {4, 4, 4},
			},
		),
		MaxRefScriptSizePerBlock: 1048576,
		MaxRefScriptSizePerTx:    204800,
		RefScriptCostStride:      stride,
		RefScriptCostMultiplier:  ratPtr(6, 5),
		MaxPledgeLeverage:        ratPtr(10, 1),
		MinPoolMargin:            ratPtr(1, 100),
	}
}

// requireDijkstraQueryReply checks the reply as a client decodes it from the
// wire: the Dijkstra type, without the synthetic PlutusV2 entry, with every
// Dijkstra-only field intact.
func requireDijkstraQueryReply(
	t *testing.T,
	result any,
	stride uint32,
) {
	t.Helper()
	arr, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, arr, 1)
	pp, ok := arr[0].(*dijkstra.DijkstraProtocolParameters)
	require.True(t, ok, "reply is %T", arr[0])
	encoded, err := cbor.Encode(pp)
	require.NoError(t, err)
	decodedAny, err := eras.DecodePParamsDijkstra(encoded)
	require.NoError(t, err)
	decoded, ok := decodedAny.(*dijkstra.DijkstraProtocolParameters)
	require.True(t, ok)
	assert.NotContains(t, decoded.CostModels, uint(1))
	assert.Equal(t, []int64{3, 3, 3}, decoded.CostModels[2])
	assert.Equal(t, []int64{4, 4, 4}, decoded.CostModels[3])
	assert.Equal(t, uint32(1048576), decoded.MaxRefScriptSizePerBlock)
	assert.Equal(t, uint32(204800), decoded.MaxRefScriptSizePerTx)
	assert.Equal(t, stride, decoded.RefScriptCostStride)
	require.NotNil(t, decoded.RefScriptCostMultiplier)
	assert.Zero(t, decoded.RefScriptCostMultiplier.Cmp(big.NewRat(6, 5)))
	require.NotNil(t, decoded.MaxPledgeLeverage)
	assert.Zero(t, decoded.MaxPledgeLeverage.Cmp(big.NewRat(10, 1)))
	require.NotNil(t, decoded.MinPoolMargin)
	assert.Zero(t, decoded.MinPoolMargin.Cmp(big.NewRat(1, 100)))
}

func TestQueryShelleyCurrentProtocolParams_DijkstraLive(t *testing.T) {
	t.Parallel()

	ls := newPoolDistr2Ledger(t, newTestDB(t))
	ls.currentEra = eras.DijkstraEraDesc
	ls.currentPParams = dijkstraQueryPParams(25600)
	ls.syntheticV2CostModel = true
	ls.publishSnapshotsLocked()

	result, err := ls.Query(protocolParamsQuery(), QueryPoint{})
	require.NoError(t, err)
	requireDijkstraQueryReply(t, result, 25600)

	internal, ok := ls.currentPParams.(*dijkstra.DijkstraProtocolParameters)
	require.True(t, ok)
	assert.Contains(t, internal.CostModels, uint(1),
		"validation keeps the synthetic PlutusV2 cost model")
}

func TestQueryShelleyCurrentProtocolParams_DijkstraPersistedRow(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	ls := newPoolDistr2Ledger(t, db)
	ls.currentEra = eras.DijkstraEraDesc
	ls.currentPParams = dijkstraQueryPParams(51200)
	ls.currentEpoch = models.Epoch{EpochId: 6}
	ls.publishSnapshotsLocked()

	dijkstraEraId := uint(eras.DijkstraEraDesc.Id)
	require.NoError(t, ls.db.SetEpoch(
		300, 3, nil, nil, nil, nil, dijkstraEraId, 1, 100, nil,
	))
	require.NoError(t, ls.db.SetEpoch(
		600, 6, nil, nil, nil, nil, dijkstraEraId, 1, 100, nil,
	))
	historicalCbor, err := cbor.Encode(dijkstraQueryPParams(25600))
	require.NoError(t, err)
	require.NoError(t, ls.db.SetPParams(
		historicalCbor, 300, 3, dijkstraEraId, nil,
	))
	require.NoError(t, db.SetTip(ochainsync.Tip{
		Point: ocommon.NewPoint(650, repeatedBytes(32, 0x0B)),
	}, nil))

	result, err := ls.queryShelleyCurrentProtocolParams(
		QueryPoint{Slot: 350}, nil,
	)
	require.NoError(t, err)
	requireDijkstraQueryReply(t, result, 25600)
}
