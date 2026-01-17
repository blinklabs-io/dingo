// Copyright 2024 Blink Labs Software
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
	"bytes"
	"path"
	"reflect"
	"testing"
)

const (
	testDataDir = "testdata"
)

var expectedCardanoNodeConfig = &CardanoNodeConfig{
	path:               testDataDir,
	AlonzoGenesisFile:  "alonzo-genesis.json",
	AlonzoGenesisHash:  "7e94a15f55d1e82d10f09203fa1d40f8eede58fd8066542cf6566008068ed874",
	ByronGenesisFile:   "byron-genesis.json",
	ByronGenesisHash:   "83de1d7302569ad56cf9139a41e2e11346d4cb4a31c00142557b6ab3fa550761",
	ConwayGenesisFile:  "conway-genesis.json",
	ConwayGenesisHash:  "9cc5084f02e27210eacba47af0872e3dba8946ad9460b6072d793e1d2f3987ef",
	ShelleyGenesisFile: "shelley-genesis.json",
	ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
}

func TestCardanoNodeConfig(t *testing.T) {
	tmpPath := path.Join(
		testDataDir,
		"config.json",
	)
	cfg, err := NewCardanoNodeConfigFromFile(tmpPath)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// Create temp config without parsed genesis files to make comparison easier
	tmpCfg := *cfg
	tmpCfg.byronGenesis = nil
	tmpCfg.shelleyGenesis = nil
	tmpCfg.alonzoGenesis = nil
	tmpCfg.conwayGenesis = nil
	if !reflect.DeepEqual(&tmpCfg, expectedCardanoNodeConfig) {
		t.Fatalf(
			"did not get expected object\n     got: %#v\n  wanted: %#v\n",
			tmpCfg,
			expectedCardanoNodeConfig,
		)
	}
	t.Run("Byron genesis", func(t *testing.T) {
		g := cfg.ByronGenesis()
		if g == nil {
			t.Fatalf("got nil instead of ByronGenesis")
		}
	})
	t.Run("Shelley genesis", func(t *testing.T) {
		g := cfg.ShelleyGenesis()
		if g == nil {
			t.Fatalf("got nil instead of ShelleyGenesis")
		}
	})
	t.Run("Alonzo genesis", func(t *testing.T) {
		g := cfg.AlonzoGenesis()
		if g == nil {
			t.Fatalf("got nil instead of AlonzoGenesis")
		}
	})
	t.Run("Conway genesis", func(t *testing.T) {
		g := cfg.ConwayGenesis()
		if g == nil {
			t.Fatalf("got nil instead of ConwayGenesis")
		}
	})
}

func TestCardanoNodeConfigMissingGenesisHashes(t *testing.T) {
	cfgBytes := []byte(`{
  "AlonzoGenesisFile": "alonzo-genesis.json",
  "ByronGenesisFile": "byron-genesis.json",
  "ConwayGenesisFile": "conway-genesis.json",
  "ShelleyGenesisFile": "shelley-genesis.json"
}`)
	cfg, err := NewCardanoNodeConfigFromReader(bytes.NewReader(cfgBytes))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	cfg.path = testDataDir
	if err := cfg.loadGenesisConfigs(); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if cfg.ByronGenesisHash != expectedCardanoNodeConfig.ByronGenesisHash {
		t.Fatalf(
			"unexpected Byron genesis hash: got %s, wanted %s",
			cfg.ByronGenesisHash,
			expectedCardanoNodeConfig.ByronGenesisHash,
		)
	}
	if cfg.ShelleyGenesisHash != expectedCardanoNodeConfig.ShelleyGenesisHash {
		t.Fatalf(
			"unexpected Shelley genesis hash: got %s, wanted %s",
			cfg.ShelleyGenesisHash,
			expectedCardanoNodeConfig.ShelleyGenesisHash,
		)
	}
	if cfg.AlonzoGenesisHash != expectedCardanoNodeConfig.AlonzoGenesisHash {
		t.Fatalf(
			"unexpected Alonzo genesis hash: got %s, wanted %s",
			cfg.AlonzoGenesisHash,
			expectedCardanoNodeConfig.AlonzoGenesisHash,
		)
	}
	if cfg.ConwayGenesisHash != expectedCardanoNodeConfig.ConwayGenesisHash {
		t.Fatalf(
			"unexpected Conway genesis hash: got %s, wanted %s",
			cfg.ConwayGenesisHash,
			expectedCardanoNodeConfig.ConwayGenesisHash,
		)
	}
}
