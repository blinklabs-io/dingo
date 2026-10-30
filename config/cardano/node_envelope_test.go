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

package cardano

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeDijkstraV2Config places the DijkstraNet version 2 envelope config next
// to the DijkstraNet genesis files and returns the config path.
func writeDijkstraV2Config(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, name := range []string{
		"byron-genesis.json",
		"shelley-genesis.json",
		"alonzo-genesis.json",
		"conway-genesis.json",
		"dijkstra-genesis.json",
	} {
		data, err := os.ReadFile(filepath.Join("dijkstra", name))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0o644))
	}
	data, err := os.ReadFile(filepath.Join("testdata-envelope", "dijkstra-v2-config.json"))
	require.NoError(t, err)
	configPath := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(configPath, data, 0o644))
	return configPath
}

func TestCardanoNodeConfigEnvelopeV2Dijkstra(t *testing.T) {
	t.Parallel()

	cfg, err := NewCardanoNodeConfigFromFile(writeDijkstraV2Config(t))
	require.NoError(t, err)

	// Configuration.Protocol
	assert.Equal(t, "byron-genesis.json", cfg.ByronGenesisFile)
	assert.Equal(t,
		"245e5b28a31c6bc3e8b78f1223294ae09e57b6d2543b51aba7cf9dadff161238",
		cfg.ByronGenesisHash)
	assert.Equal(t, "shelley-genesis.json", cfg.ShelleyGenesisFile)
	assert.Equal(t,
		"b47eb627b52bf9cfe5ffd865658b47b5747a088d150a9c27216de374e836c385",
		cfg.ShelleyGenesisHash)
	assert.Equal(t, "alonzo-genesis.json", cfg.AlonzoGenesisFile)
	assert.Equal(t,
		"387a7c4880477ce7b128566fa7f9f9ed99ee04476084e9f6332b6d42d907faab",
		cfg.AlonzoGenesisHash)
	assert.Equal(t, "conway-genesis.json", cfg.ConwayGenesisFile)
	assert.Equal(t,
		"e18ab8efc363ad170142706723568f1520a57f0dc05354e1d458041da18a3704",
		cfg.ConwayGenesisHash)

	// Configuration.Testing
	assert.Equal(t, "dijkstra-genesis.json", cfg.DijkstraGenesisFile)
	assert.Equal(t,
		"ad58e282eff1f48c241faf6319f88ed54843821627a6be69470f05d7904e4601",
		cfg.DijkstraGenesisHash)
	require.NotNil(t, cfg.ExperimentalHardForksEnabled)
	assert.True(t, *cfg.ExperimentalHardForksEnabled)
	for name, epoch := range map[string]*uint64{
		"Shelley": cfg.TestShelleyHardForkAtEpoch,
		"Allegra": cfg.TestAllegraHardForkAtEpoch,
		"Mary":    cfg.TestMaryHardForkAtEpoch,
		"Alonzo":  cfg.TestAlonzoHardForkAtEpoch,
		"Babbage": cfg.TestBabbageHardForkAtEpoch,
		"Conway":  cfg.TestConwayHardForkAtEpoch,
	} {
		require.NotNil(t, epoch, name)
		assert.Equal(t, uint64(0), *epoch, name)
	}
	assert.Nil(t, cfg.TestDijkstraHardForkAtEpoch)

	// Configuration.Consensus
	assert.Equal(t, "GenesisMode", cfg.ConsensusMode)

	// The genesis documents named in the nested sections were loaded and
	// matched their declared hashes.
	assert.NotNil(t, cfg.ByronGenesis())
	assert.NotNil(t, cfg.ShelleyGenesis())
	assert.NotNil(t, cfg.AlonzoGenesis())
	assert.NotNil(t, cfg.ConwayGenesis())
	assert.NotNil(t, cfg.DijkstraGenesis())
}

func TestCardanoNodeConfigEnvelopeV2MatchesFlat(t *testing.T) {
	t.Parallel()

	v2, err := NewCardanoNodeConfigFromFile(writeDijkstraV2Config(t))
	require.NoError(t, err)
	flat, err := NewCardanoNodeConfigFromFile(filepath.Join("dijkstra", "config.json"))
	require.NoError(t, err)

	assert.Equal(t, flat.ByronGenesisHash, v2.ByronGenesisHash)
	assert.Equal(t, flat.ShelleyGenesisHash, v2.ShelleyGenesisHash)
	assert.Equal(t, flat.AlonzoGenesisHash, v2.AlonzoGenesisHash)
	assert.Equal(t, flat.ConwayGenesisHash, v2.ConwayGenesisHash)
	assert.Equal(t, flat.DijkstraGenesisHash, v2.DijkstraGenesisHash)
	assert.Equal(t, flat.ShelleyGenesis().NetworkMagic, v2.ShelleyGenesis().NetworkMagic)
}

func TestCardanoNodeConfigEnvelopeV2Network(t *testing.T) {
	t.Parallel()

	cfg, err := NewCardanoNodeConfigFromReader(strings.NewReader(`{
  "Version": 2,
  "Configuration": {
    "Network": {
      "PeerSharing": false,
      "DeadlineTargetNumberOfRootPeers": 7,
      "DeadlineTargetNumberOfKnownPeers": 80,
      "DeadlineTargetNumberOfEstablishedPeers": 30,
      "DeadlineTargetNumberOfActivePeers": 15,
      "SyncTargetNumberOfActivePeers": 99
    },
    "HermodTracing": "tracing.json"
  }
}`))
	require.NoError(t, err)
	require.NotNil(t, cfg.PeerSharing)
	assert.False(t, *cfg.PeerSharing)
	assert.Equal(t, 7, cfg.TargetNumberOfRootPeers)
	assert.Equal(t, 80, cfg.TargetNumberOfKnownPeers)
	assert.Equal(t, 30, cfg.TargetNumberOfEstablishedPeers)
	assert.Equal(t, 15, cfg.TargetNumberOfActivePeers)
}

func TestCardanoNodeConfigEnvelopeVersionRejected(t *testing.T) {
	t.Parallel()

	for name, body := range map[string]string{
		"future":      `{"Version": 3, "Configuration": {}}`,
		"version 1":   `{"Version": 1, "Configuration": {}}`,
		"string":      `{"Version": "2", "Configuration": {}}`,
		"missing":     `{"Configuration": {"Protocol": {}}}`,
		"no config":   `{"Version": 2}`,
		"bad config":  `{"Version": 2, "Configuration": []}`,
		"bad section": `{"Version": 2, "Configuration": {"Protocol": "x"}}`,
	} {
		_, err := NewCardanoNodeConfigFromReader(strings.NewReader(body))
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "cardano node config", name)
	}
	_, err := NewCardanoNodeConfigFromReader(
		strings.NewReader(`{"Version": 3, "Configuration": {}}`),
	)
	require.ErrorContains(t, err, "unsupported config Version 3")
}

// A flat config's top-level keys keep loading as before; a flat file has no
// Version or Configuration key.
func TestCardanoNodeConfigFlatUnchanged(t *testing.T) {
	t.Parallel()

	cfg, err := NewCardanoNodeConfigFromReader(strings.NewReader(`{
  "ConsensusMode": "PraosMode",
  "ExperimentalHardForksEnabled": true,
  "TestConwayHardForkAtEpoch": 0,
  "TargetNumberOfRootPeers": 5,
  "Protocol": "Cardano"
}`))
	require.NoError(t, err)
	assert.Equal(t, "PraosMode", cfg.ConsensusMode)
	require.NotNil(t, cfg.ExperimentalHardForksEnabled)
	assert.True(t, *cfg.ExperimentalHardForksEnabled)
	require.NotNil(t, cfg.TestConwayHardForkAtEpoch)
	assert.Equal(t, 5, cfg.TargetNumberOfRootPeers)
}
