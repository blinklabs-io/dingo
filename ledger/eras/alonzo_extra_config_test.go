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
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/stretchr/testify/require"
)

const extraConfigAlonzoFixture = "testdata/musashi-alonzo-genesis-extraconfig.json"

// extraConfigNodeConfig loads the Musashi node config and swaps in the Alonzo
// genesis fixture. The fixture carries extraConfig cost models of 332, 332
// and 350 parameters, while the Conway genesis carries a shorter PlutusV3
// model.
func extraConfigNodeConfig(
	t *testing.T,
	alonzoGenesis string,
) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg, err := cardano.NewCardanoNodeConfigFromFile(
		"../../config/cardano/musashi/config.json",
	)
	require.NoError(t, err)
	require.NoError(
		t,
		cfg.LoadAlonzoGenesisFromReader(strings.NewReader(alonzoGenesis)),
	)
	return cfg
}

func readExtraConfigFixture(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(extraConfigAlonzoFixture)
	require.NoError(t, err)
	return string(data)
}

// hardForkToConway runs the Alonzo, Babbage and Conway transitions in order.
func hardForkToConway(
	t *testing.T,
	cfg *cardano.CardanoNodeConfig,
) *conway.ConwayProtocolParameters {
	t.Helper()
	var pp lcommon.ProtocolParameters = &mary.MaryProtocolParameters{}
	for _, fn := range []func(
		*cardano.CardanoNodeConfig,
		lcommon.ProtocolParameters,
	) (lcommon.ProtocolParameters, error){
		eras.HardForkAlonzo,
		eras.HardForkBabbage,
		eras.HardForkConway,
	} {
		var err error
		pp, err = fn(cfg, pp)
		require.NoError(t, err)
	}
	return pp.(*conway.ConwayProtocolParameters)
}

func TestHardForkConwayAppliesAlonzoExtraConfigCostModels(t *testing.T) {
	cfg := extraConfigNodeConfig(t, readExtraConfigFixture(t))
	extra := cfg.AlonzoGenesis().ExtraConfig.CostModels
	require.Len(t, extra["PlutusV3"], 350)
	// The Conway genesis V3 model differs from extraConfig; otherwise the
	// assertions below could not tell which one won.
	require.NotEqual(
		t,
		len(cfg.ConwayGenesis().PlutusV3CostModel),
		len(extra["PlutusV3"]),
	)

	pp := hardForkToConway(t, cfg)

	require.Equal(t, extra["PlutusV1"], pp.CostModels[0])
	require.Equal(t, extra["PlutusV2"], pp.CostModels[1])
	require.Equal(t, extra["PlutusV3"], pp.CostModels[2])
	require.Len(t, pp.CostModels[0], 332)
	require.Len(t, pp.CostModels[1], 332)
	require.Len(t, pp.CostModels[2], 350)
}

func TestHardForkDijkstraKeepsAlonzoExtraConfigCostModels(t *testing.T) {
	cfg := extraConfigNodeConfig(t, readExtraConfigFixture(t))
	extra := cfg.AlonzoGenesis().ExtraConfig.CostModels
	conwayPP := hardForkToConway(t, cfg)

	got, err := eras.HardForkDijkstra(cfg, conwayPP)
	require.NoError(t, err)
	pp := got.(*dijkstra.DijkstraProtocolParameters)

	require.Equal(t, extra["PlutusV1"], pp.CostModels[0])
	require.Equal(t, extra["PlutusV2"], pp.CostModels[1])
	require.Equal(t, extra["PlutusV3"], pp.CostModels[2])
}

func TestHardForkConwayWithoutExtraConfigKeepsGenesisCostModels(
	t *testing.T,
) {
	cfg := extraConfigNodeConfig(t, `{"costModels":{}}`)
	require.Nil(t, cfg.AlonzoGenesis().ExtraConfig)

	pp := hardForkToConway(t, cfg)

	require.Equal(t, cfg.ConwayGenesis().PlutusV3CostModel, pp.CostModels[2])
	require.Contains(t, pp.CostModels, uint(1))
}

func TestHardForkRejectsShortExtraConfigCostModel(t *testing.T) {
	var genesis map[string]any
	require.NoError(
		t,
		json.Unmarshal([]byte(readExtraConfigFixture(t)), &genesis),
	)
	extra := genesis["extraConfig"].(map[string]any)
	models := extra["costModels"].(map[string]any)
	models["PlutusV3"] = []int64{1, 2, 3}
	short, err := json.Marshal(genesis)
	require.NoError(t, err)
	cfg := extraConfigNodeConfig(t, string(short))

	var pp lcommon.ProtocolParameters = &mary.MaryProtocolParameters{}
	_, err = eras.HardForkAlonzo(cfg, pp)
	require.ErrorContains(t, err, "insufficient param count")

	// A later era must reject it too when the Alonzo step was skipped.
	prev := &babbage.BabbageProtocolParameters{ProtocolMajor: 8}
	_, err = eras.HardForkConway(cfg, prev)
	require.ErrorContains(t, err, "insufficient param count")
}
