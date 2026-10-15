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

package utxorpc

import (
	"context"
	"math/big"
	"testing"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
)

func TestReadParams_Dijkstra(t *testing.T) {
	rat := func(n, d int64) cbor.Rat { return cbor.Rat{Rat: big.NewRat(n, d)} }
	ratPtr := func(n, d int64) *cbor.Rat { return &cbor.Rat{Rat: big.NewRat(n, d)} }
	pp := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MinFeeA:     44,
			MinFeeB:     155381,
			MinPoolCost: 170000000,
			A0:          ratPtr(3, 10),
			Rho:         ratPtr(3, 1000),
			Tau:         ratPtr(1, 5),
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: dijkstra.MinProtocolVersionDijkstra,
			},
			CostModels: map[uint][]int64{
				2: {1, 2, 3},
				3: {4, 5, 6},
			},
			ExecutionCosts: lcommon.ExUnitPrice{
				MemPrice:  ratPtr(577, 10000),
				StepPrice: ratPtr(721, 10000000),
			},
			MinFeeRefScriptCostPerByte: ratPtr(15, 1),
			PoolVotingThresholds: conway.PoolVotingThresholds{
				MotionNoConfidence:    rat(51, 100),
				CommitteeNormal:       rat(51, 100),
				CommitteeNoConfidence: rat(51, 100),
				HardForkInitiation:    rat(51, 100),
				PpSecurityGroup:       rat(51, 100),
			},
			DRepVotingThresholds: conway.DRepVotingThresholds{
				MotionNoConfidence:    rat(67, 100),
				CommitteeNormal:       rat(67, 100),
				CommitteeNoConfidence: rat(60, 100),
				UpdateToConstitution:  rat(75, 100),
				HardForkInitiation:    rat(60, 100),
				PpNetworkGroup:        rat(67, 100),
				PpEconomicGroup:       rat(67, 100),
				PpTechnicalGroup:      rat(67, 100),
				PpGovGroup:            rat(75, 100),
				TreasuryWithdrawal:    rat(67, 100),
			},
		},
		RefScriptCostStride: 25600,
	}
	stub := &shelleyLedgerStub{
		byronLedgerStub: byronLedgerStub{
			tip: ochainsync.Tip{
				Point: ocommon.NewPoint(42, []byte{0xab, 0xcd}),
			},
		},
		pparams: pp,
	}
	srv := newByronQueryServer(t, stub)

	out, err := srv.ReadParams(
		context.Background(),
		connect.NewRequest(&query.ReadParamsRequest{}),
	)

	require.NoError(t, err)
	cardanoParams := out.Msg.GetValues().GetCardano()
	require.NotNil(t, cardanoParams)
	assert.Equal(t, int64(44), cardanoParams.GetMinFeeCoefficient().GetInt())
	assert.Equal(
		t,
		uint64(dijkstra.MinProtocolVersionDijkstra),
		uint64(cardanoParams.GetProtocolVersion().GetMajor()),
	)
	costModels := cardanoParams.GetCostModels()
	assert.Equal(t, []int64{1, 2, 3}, costModels.GetPlutusV3().GetValues())
	assert.Equal(t, []int64{4, 5, 6}, costModels.GetPlutusV4().GetValues())
	assert.Equal(
		t,
		int32(15),
		cardanoParams.GetMinFeeScriptRefCostPerByte().GetNumerator(),
	)
	assert.Equal(t, uint64(42), out.Msg.GetLedgerTip().GetSlot())
}
