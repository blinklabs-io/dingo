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
	require.NoError(t, cfg.LoadDijkstraGenesisFromReader(strings.NewReader("{}")))

	prev := &conway.ConwayProtocolParameters{
		MinCommitteeSize:        7,
		GovActionDeposit:        42,
		DRepDeposit:             84,
		GovActionValidityPeriod: 99,
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 10,
			Minor: 0,
		},
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
}
