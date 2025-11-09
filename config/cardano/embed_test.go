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
	"testing"
)

func TestNewCardanoNodeConfigFromEmbedFS(t *testing.T) {
	// Test loading config from embedded filesystem
	cfg, err := NewCardanoNodeConfigFromEmbedFS(
		EmbeddedConfigPreviewNetworkFS,
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
	// Test loading config from embedded filesystem with invalid path
	_, err := NewCardanoNodeConfigFromEmbedFS(
		EmbeddedConfigPreviewNetworkFS,
		"nonexistent/config.json",
	)
	if err == nil {
		t.Fatal("expected error for invalid path")
	}
}

func TestEmbedFS_ListFiles(t *testing.T) {
	// Test that embedded FS contains expected files in preview directory
	previewEntries, err := EmbeddedConfigPreviewNetworkFS.ReadDir("preview")
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
