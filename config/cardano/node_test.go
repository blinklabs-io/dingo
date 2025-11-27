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

package cardano_test

import (
	"path"
	"testing"

	"github.com/blinklabs-io/dingo/config/cardano"
)

const (
	testDataDir = "testdata"
)

func TestCardanoNodeConfig(t *testing.T) {
	tmpPath := path.Join(
		testDataDir,
		"config.json",
	)
	cfg, err := cardano.NewCardanoNodeConfigFromFile(tmpPath)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// Check that genesis files are accessible
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
		if g.NetworkId != "Testnet" {
			t.Errorf("expected NetworkId 'Testnet', got %q", g.NetworkId)
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
	// Validate config fields are populated
	t.Run("Config fields", func(t *testing.T) {
		if cfg.ShelleyGenesisFile == "" {
			t.Error("expected ShelleyGenesisFile to be set")
		}
		if cfg.ByronGenesisFile == "" {
			t.Error("expected ByronGenesisFile to be set")
		}
	})
}
