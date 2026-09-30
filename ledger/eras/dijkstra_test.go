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

package eras_test

import (
	"math/big"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/ledger/eras"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/stretchr/testify/require"
)

func TestHardForkDijkstraSkipsEmptyGenesis(t *testing.T) {
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		cfg.LoadDijkstraGenesisFromReader(strings.NewReader("{}")),
	)

	prev := &conway.ConwayProtocolParameters{
		MinCommitteeSize:        7,
		GovActionDeposit:        42,
		DRepDeposit:             84,
		GovActionValidityPeriod: 99,
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 10,
			Minor: 0,
		},
		CostModels: map[uint][]int64{0: {1, 2, 3}},
	}

	got, err := eras.HardForkDijkstra(cfg, prev)
	require.NoError(t, err)
	dijkstraPParams, ok := got.(*dijkstra.DijkstraProtocolParameters)
	require.True(t, ok)
	require.Equal(t, uint(7), dijkstraPParams.MinCommitteeSize)
	require.Equal(t, uint64(42), dijkstraPParams.GovActionDeposit)
	require.Equal(t, uint64(84), dijkstraPParams.DRepDeposit)
	require.Equal(t, uint64(99), dijkstraPParams.GovActionValidityPeriod)
	require.Equal(
		t,
		uint(dijkstra.MinProtocolVersionDijkstra),
		dijkstraPParams.ProtocolVersion.Major,
	)
	if dijkstraPParams.CostModels == nil || prev.CostModels == nil ||
		dijkstraPParams.CostModels[0] == nil || prev.CostModels[0] == nil {
		t.Fatal("expected cost models")
	}
	dijkstraPParams.CostModels[0][0] = 9
	require.Equal(t, []int64{1, 2, 3}, prev.CostModels[0])
}

func hardForkDijkstraFromGenesisJSON(
	t *testing.T,
	genesisJSON string,
) *dijkstra.DijkstraProtocolParameters {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{}
	require.NoError(
		t,
		cfg.LoadDijkstraGenesisFromReader(strings.NewReader(genesisJSON)),
	)
	prev := &conway.ConwayProtocolParameters{
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 10,
		},
	}
	got, err := eras.HardForkDijkstra(cfg, prev)
	require.NoError(t, err)
	pparams, ok := got.(*dijkstra.DijkstraProtocolParameters)
	require.True(t, ok)
	return pparams
}

func requireConwayRefScriptDefaults(
	t *testing.T,
	pparams *dijkstra.DijkstraProtocolParameters,
) {
	t.Helper()
	require.Equal(t, uint32(25_600), pparams.RefScriptCostStride)
	require.NotNil(t, pparams.RefScriptCostMultiplier)
	require.Zero(t, pparams.RefScriptCostMultiplier.Cmp(big.NewRat(6, 5)))
	require.Equal(t, uint32(200*1024), pparams.MaxRefScriptSizePerTx)
	require.Equal(t, uint32(1024*1024), pparams.MaxRefScriptSizePerBlock)
}

func TestHardForkDijkstraEmptyGenesisKeepsRefScriptFeeDefaults(t *testing.T) {
	t.Parallel()
	pparams := hardForkDijkstraFromGenesisJSON(t, "{}")
	requireConwayRefScriptDefaults(t, pparams)
}

func TestHardForkDijkstraCarriesFieldsSkippedByEmptyCheck(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		genesis string
		check   func(*testing.T, *dijkstra.DijkstraProtocolParameters)
	}{
		{
			name:    "plutusV4CostModel",
			genesis: `{"plutusV4CostModel":[1,2,3]}`,
			check: func(t *testing.T, p *dijkstra.DijkstraProtocolParameters) {
				require.Equal(t, []int64{1, 2, 3}, p.CostModels[3])
			},
		},
		{
			name:    "maxPledgeLeverage",
			genesis: `{"maxPledgeLeverage":5}`,
			check: func(t *testing.T, p *dijkstra.DijkstraProtocolParameters) {
				require.NotNil(t, p.MaxPledgeLeverage)
				require.Zero(t, p.MaxPledgeLeverage.Cmp(big.NewRat(5, 1)))
			},
		},
		{
			name:    "minPoolMargin",
			genesis: `{"minPoolMargin":0.1}`,
			check: func(t *testing.T, p *dijkstra.DijkstraProtocolParameters) {
				require.NotNil(t, p.MinPoolMargin)
				require.Zero(t, p.MinPoolMargin.Cmp(big.NewRat(1, 10)))
			},
		},
		{
			name:    "leiosCommitteeSize",
			genesis: `{"leiosCommitteeSize":7}`,
			check: func(t *testing.T, p *dijkstra.DijkstraProtocolParameters) {
				require.Equal(t, uint16(7), p.LeiosCommitteeSize)
			},
		},
		{
			name:    "maxEndorserBlockTxsSize",
			genesis: `{"maxEndorserBlockTxsSize":1234}`,
			check: func(t *testing.T, p *dijkstra.DijkstraProtocolParameters) {
				require.Equal(t, uint32(1234), p.MaxEndorserBlockTxsSize)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pparams := hardForkDijkstraFromGenesisJSON(t, tc.genesis)
			tc.check(t, pparams)
			requireConwayRefScriptDefaults(t, pparams)
		})
	}
}
