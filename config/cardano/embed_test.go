// Copyright 2025 Blink Labs Software
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
	"io/fs"
	"testing"
)

func TestNewCardanoNodeConfigFromEmbedFS(t *testing.T) {
	t.Parallel()

	// Test loading config from embedded filesystem
	cfg, err := NewCardanoNodeConfigFromEmbedFS(
		EmbeddedConfigFS,
		"preview/config.json",
	)
	if err != nil {
		t.Fatalf("failed to load cardano config from embedded FS: %v", err)
	}

	if cfg == nil {
		t.Fatal("expected non-nil config")
	}

	// Verify that the config has expected values
	if cfg.ShelleyGenesisFile == "" {
		t.Error("expected ShelleyGenesisFile to be set")
	}

	if cfg.ByronGenesisFile == "" {
		t.Error("expected ByronGenesisFile to be set")
	}

	// Verify genesis configs were actually loaded
	if cfg.ShelleyGenesis() == nil {
		t.Error("expected ShelleyGenesis to be loaded")
	}

	if cfg.ByronGenesis() == nil {
		t.Error("expected ByronGenesis to be loaded")
	}
}

func TestNewCardanoNodeConfigFromEmbedFS_InvalidPath(t *testing.T) {
	t.Parallel()

	// Test loading config from embedded filesystem with invalid path
	_, err := NewCardanoNodeConfigFromEmbedFS(
		EmbeddedConfigFS,
		"nonexistent/config.json",
	)
	if err == nil {
		t.Fatal("expected error for invalid path")
	}
}

func TestLoadCardanoNodeConfigWithFallbackNormalizesEmbedPath(t *testing.T) {
	t.Parallel()

	cfg, err := LoadCardanoNodeConfigWithFallback(
		`preview\config.json`,
		"preview",
		EmbeddedConfigFS,
	)
	if err != nil {
		t.Fatalf("failed to load cardano config from embedded FS: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
}

func TestEmbedFS_ListFiles(t *testing.T) {
	t.Parallel()

	// Test that embedded FS contains expected files in preview directory
	previewEntries, err := EmbeddedConfigFS.ReadDir("preview")
	if err != nil {
		t.Fatalf("failed to read preview directory: %v", err)
	}

	if len(previewEntries) == 0 {
		t.Fatal("expected preview directory to contain files")
	}

	expectedFiles := []string{
		"config.json",
		"byron-genesis.json",
		"shelley-genesis.json",
		"alonzo-genesis.json",
		"conway-genesis.json",
	}
	foundFiles := make(map[string]bool)

	for _, entry := range previewEntries {
		foundFiles[entry.Name()] = true
	}

	for _, expectedFile := range expectedFiles {
		if !foundFiles[expectedFile] {
			t.Errorf("expected to find %s in preview directory", expectedFile)
		}
	}
}

func TestEmbedFS_AllNetworks(t *testing.T) {
	t.Parallel()

	networks := []string{"preview", "preprod", "mainnet", "devnet"}
	expectedFiles := []string{
		"config.json",
		"byron-genesis.json",
		"shelley-genesis.json",
		"alonzo-genesis.json",
		"conway-genesis.json",
	}

	for _, network := range networks {
		t.Run(network, func(t *testing.T) {
			t.Parallel()
			entries, err := EmbeddedConfigFS.ReadDir(network)
			if err != nil {
				t.Fatalf("failed to read %s directory: %v", network, err)
			}
			if len(entries) == 0 {
				t.Fatalf("expected %s directory to contain files", network)
			}
			foundFiles := make(map[string]bool)
			for _, entry := range entries {
				foundFiles[entry.Name()] = true
			}
			for _, expectedFile := range expectedFiles {
				if !foundFiles[expectedFile] {
					t.Errorf(
						"expected to find %s in %s directory",
						expectedFile,
						network,
					)
				}
			}
			// Verify config can be loaded
			cfg, err := NewCardanoNodeConfigFromEmbedFS(
				EmbeddedConfigFS,
				network+"/config.json",
			)
			if err != nil {
				t.Fatalf("failed to load %s config: %v", network, err)
			}
			if cfg == nil {
				t.Fatalf("expected non-nil config for %s", network)
			}
			if cfg.ShelleyGenesis() == nil {
				t.Errorf("expected ShelleyGenesis to be loaded for %s", network)
			}
			if cfg.ByronGenesis() == nil {
				t.Errorf("expected ByronGenesis to be loaded for %s", network)
			}
		})
	}
}

func TestEmbedFS_PrimeTestnet(t *testing.T) {
	t.Parallel()

	const network = "prime-testnet"
	expectedFiles := []string{
		"configuration.yaml",
		"topology.json",
		"genesis/byron/genesis.json",
		"genesis/shelley/genesis.json",
		"genesis/shelley/genesis.alonzo.json",
		"genesis/shelley/genesis.conway.json",
	}

	for _, file := range expectedFiles {
		if _, err := EmbeddedConfigFS.Open(network + "/" + file); err != nil {
			t.Errorf("expected to find %s/%s: %v", network, file, err)
		}
	}

	cfg, err := NewCardanoNodeConfigFromEmbedFS(
		EmbeddedConfigFS,
		network+"/configuration.yaml",
	)
	if err != nil {
		t.Fatalf("failed to load %s config: %v", network, err)
	}
	if cfg.ShelleyGenesis() == nil {
		t.Fatal("expected ShelleyGenesis to be loaded")
	}
	if cfg.ByronGenesis() == nil {
		t.Fatal("expected ByronGenesis to be loaded")
	}
}

// TestEmbeddedConfigPathReachesEveryNetwork is the defect the helper exists
// for: every caller that has only a network name derived the config path by
// appending "/config.json", and prime-testnet's upstream config is named
// configuration.yaml, so that network could not be started from the embedded
// filesystem at all.
//
// Enumerating the embedded directories rather than listing names keeps a
// network added to the //go:embed directive under this check automatically --
// the same property bin/config-parity.sh relies on.
func TestEmbeddedConfigPathReachesEveryNetwork(t *testing.T) {
	t.Parallel()

	entries, err := fs.ReadDir(EmbeddedConfigFS, ".")
	if err != nil {
		t.Fatalf("reading the embedded root: %v", err)
	}
	networks := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		networks++
		network := entry.Name()
		t.Run(network, func(t *testing.T) {
			t.Parallel()
			cfg, err := LoadCardanoNodeConfigWithFallback(
				EmbeddedConfigPath(network),
				network,
				EmbeddedConfigFS,
			)
			if err != nil {
				t.Fatalf(
					"%s is not loadable at the derived path %q: %v",
					network, EmbeddedConfigPath(network), err,
				)
			}
			if cfg.ShelleyGenesis() == nil {
				t.Error("expected ShelleyGenesis to be loaded")
			}
		})
	}
	if networks == 0 {
		t.Fatal("the embedded filesystem contains no network directories")
	}
}

// TestEmbeddedConfigPathFallsBackToConfigJSON covers the name reported for a
// network that is not embedded at all. Returning the conventional name means
// the caller's error names the file it expected, rather than whichever
// candidate happened to be tried last.
func TestEmbeddedConfigPathFallsBackToConfigJSON(t *testing.T) {
	t.Parallel()

	if got, want := EmbeddedConfigPath("no-such-network"),
		"no-such-network/config.json"; got != want {
		t.Errorf("EmbeddedConfigPath = %q, want %q", got, want)
	}
}
