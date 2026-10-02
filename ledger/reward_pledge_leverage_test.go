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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	mockledger "github.com/blinklabs-io/ouroboros-mock/ledger"
	"github.com/stretchr/testify/require"
)

// pledgeLeverageTestEpoch is one persisted epoch for the rewardParameters
// CIP-50 source tests. A nil leverage on a Dijkstra epoch is the enacted
// "unset" value; leverage is ignored for a Conway epoch.
type pledgeLeverageTestEpoch struct {
	era      eras.EraDesc
	leverage *cbor.Rat
}

func pledgeLeverageTestPParams(
	t *testing.T,
	epoch pledgeLeverageTestEpoch,
) []byte {
	t.Helper()
	conwayPParams := mockledger.NewMockConwayProtocolParams()
	conwayPParams.NOpt = 10
	conwayPParams.A0 = rewardCalcRat(1, 2)
	conwayPParams.Rho = rewardCalcRat(1, 100)
	conwayPParams.Tau = rewardCalcRat(0, 1)
	var pparams any
	switch epoch.era.Id {
	case eras.ConwayEraDesc.Id:
		conwayPParams.ProtocolVersion = lcommon.ProtocolParametersProtocolVersion{Major: 10}
		pparams = &conwayPParams
	case eras.DijkstraEraDesc.Id:
		conwayPParams.ProtocolVersion = lcommon.ProtocolParametersProtocolVersion{Major: 12}
		pparams = &dijkstra.DijkstraProtocolParameters{
			ConwayProtocolParameters:  conwayPParams,
			RefScriptCostMultiplier:   rewardCalcRat(1, 1),
			MaxPledgeLeverage:         epoch.leverage,
			LeiosQuorumStakeThreshold: rewardCalcRat(1, 2),
			CommitteeStakeCoverage:    rewardCalcRat(1, 2),
			QuorumStakeThreshold:      rewardCalcRat(1, 2),
		}
	default:
		t.Fatalf("unsupported test era %s", epoch.era.Name)
	}
	ret, err := cbor.Encode(pparams)
	require.NoError(t, err)
	return ret
}

// TestRewardParametersPledgeLeverageSource pins which protocol parameters
// supply CIP-50 L through the rewardParameters entry point. cardano-ledger's
// startStep reads ppMaxPledgeLeverageG from prevPParams, the parameters in
// force over the performance epoch; at the Conway-to-Dijkstra boundary those
// have been upgraded with the Dijkstra genesis value, which is also what the
// first Dijkstra epoch carries. The operator override applies only when
// neither epoch is Dijkstra. An enacted zero is a real cap, not "unset".
func TestRewardParametersPledgeLeverageSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		performance pledgeLeverageTestEpoch
		calculation pledgeLeverageTestEpoch
		wantEnabled bool
		want        *big.Rat
	}{
		{
			name:        "pre-Dijkstra round uses the operator override",
			performance: pledgeLeverageTestEpoch{era: eras.ConwayEraDesc},
			calculation: pledgeLeverageTestEpoch{era: eras.ConwayEraDesc},
			wantEnabled: true,
			want:        big.NewRat(100, 1),
		},
		{
			name:        "first Dijkstra round with an unset value ignores the override",
			performance: pledgeLeverageTestEpoch{era: eras.ConwayEraDesc},
			calculation: pledgeLeverageTestEpoch{era: eras.DijkstraEraDesc},
		},
		{
			name:        "first Dijkstra round enforces an enacted zero",
			performance: pledgeLeverageTestEpoch{era: eras.ConwayEraDesc},
			calculation: pledgeLeverageTestEpoch{
				era:      eras.DijkstraEraDesc,
				leverage: rewardCalcRat(0, 1),
			},
			wantEnabled: true,
			want:        big.NewRat(0, 1),
		},
		{
			name: "Dijkstra round reads the performance epoch value",
			performance: pledgeLeverageTestEpoch{
				era:      eras.DijkstraEraDesc,
				leverage: rewardCalcRat(3, 1),
			},
			calculation: pledgeLeverageTestEpoch{
				era:      eras.DijkstraEraDesc,
				leverage: rewardCalcRat(7, 1),
			},
			wantEnabled: true,
			want:        big.NewRat(3, 1),
		},
		{
			name:        "Dijkstra round with an unset performance value ignores later enactment and the override",
			performance: pledgeLeverageTestEpoch{era: eras.DijkstraEraDesc},
			calculation: pledgeLeverageTestEpoch{
				era:      eras.DijkstraEraDesc,
				leverage: rewardCalcRat(7, 1),
			},
		},
		{
			name: "Dijkstra round enforces an enacted zero",
			performance: pledgeLeverageTestEpoch{
				era:      eras.DijkstraEraDesc,
				leverage: rewardCalcRat(0, 1),
			},
			calculation: pledgeLeverageTestEpoch{era: eras.DijkstraEraDesc},
			wantEnabled: true,
			want:        big.NewRat(0, 1),
		},
		{
			name: "Dijkstra round keeps a fractional value exact",
			performance: pledgeLeverageTestEpoch{
				era:      eras.DijkstraEraDesc,
				leverage: rewardCalcRat(1, 3),
			},
			calculation: pledgeLeverageTestEpoch{era: eras.DijkstraEraDesc},
			wantEnabled: true,
			want:        big.NewRat(1, 3),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ls, db := newRewardCalculationTestLedger(t)
			ls.activeEras = append(
				append([]eras.EraDesc(nil), eras.Eras...),
				eras.DijkstraEraDesc,
			)
			ls.config.PledgeLeverageEnabled = true
			ls.config.PledgeLeverage = 100
			meta := db.Metadata()
			require.NoError(t, meta.SetEpoch(
				100, 2, nil, nil, nil, nil, tc.performance.era.Id, 1, 100, nil,
			))
			require.NoError(t, meta.SetEpoch(
				200, 3, nil, nil, nil, nil, tc.calculation.era.Id, 1, 100, nil,
			))
			require.NoError(t, db.SetPParams(
				pledgeLeverageTestPParams(t, tc.performance),
				100, 2, tc.performance.era.Id, nil,
			))
			require.NoError(t, db.SetPParams(
				pledgeLeverageTestPParams(t, tc.calculation),
				200, 3, tc.calculation.era.Id, nil,
			))

			txn := db.Transaction(false)
			defer func() { _ = txn.Rollback() }()
			_, params, _, err := ls.rewardParameters(
				txn, 2, 3, &models.RewardAdaPots{Reserves: 100_000_000},
			)
			require.NoError(t, err)
			require.Equal(t, tc.wantEnabled, params.PledgeLeverageEnabled)
			if tc.want == nil {
				require.Nil(t, params.PledgeLeverage)
				return
			}
			require.NotNil(t, params.PledgeLeverage)
			require.Zero(
				t,
				tc.want.Cmp(params.PledgeLeverage),
				"want L=%s, got %s",
				tc.want.RatString(),
				params.PledgeLeverage.RatString(),
			)
		})
	}
}
