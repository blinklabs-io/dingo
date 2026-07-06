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

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeStrictTestConfig(t *testing.T, yamlContent string) string {
	t.Helper()
	tmpFile := filepath.Join(t.TempDir(), "test-dingo.yaml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return tmpFile
}

func TestLoad_UnknownTopLevelFieldRejected(t *testing.T) {
	resetGlobalConfig()
	tmpFile := writeStrictTestConfig(t, `
network: "preview"
totallyUnknownField: 123
`)
	_, err := LoadConfig(tmpFile)
	if err == nil {
		t.Fatal("expected error for unknown top-level field, got nil")
	}
	if !strings.Contains(err.Error(), "totallyUnknownField") {
		t.Errorf("error %q does not mention the unknown field", err.Error())
	}
}

func TestLoad_UnknownFieldInWrappedConfigSectionRejected(t *testing.T) {
	resetGlobalConfig()
	tmpFile := writeStrictTestConfig(t, `
config:
  network: "preview"
  totallyUnknownField: 123
`)
	_, err := LoadConfig(tmpFile)
	if err == nil {
		t.Fatal("expected error for unknown field in wrapped config section, got nil")
	}
	if !strings.Contains(err.Error(), "totallyUnknownField") {
		t.Errorf("error %q does not mention the unknown field", err.Error())
	}
}

func TestLoad_FlatSiblingPluginSectionsStillLoad(t *testing.T) {
	resetGlobalConfig()
	tmpFile := writeStrictTestConfig(t, `
network: "preview"
database:
  blob:
    plugin: badger
  metadata:
    plugin: sqlite
`)
	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.BlobPlugin != "badger" {
		t.Errorf("BlobPlugin = %q, want badger", cfg.BlobPlugin)
	}
	if cfg.MetadataPlugin != "sqlite" {
		t.Errorf("MetadataPlugin = %q, want sqlite", cfg.MetadataPlugin)
	}
}

func TestLoad_WrappedConfigSectionWithSiblingsStillLoads(t *testing.T) {
	resetGlobalConfig()
	tmpFile := writeStrictTestConfig(t, `
config:
  network: "preview"
database:
  blob:
    plugin: badger
`)
	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Network != "preview" {
		t.Errorf("Network = %q, want preview", cfg.Network)
	}
	if cfg.BlobPlugin != "badger" {
		t.Errorf("BlobPlugin = %q, want badger", cfg.BlobPlugin)
	}
}

func TestLoad_EmptyConfigFileStillLoads(t *testing.T) {
	resetGlobalConfig()
	tmpFile := writeStrictTestConfig(t, "")
	if _, err := LoadConfig(tmpFile); err != nil {
		t.Fatalf("LoadConfig(empty file): %v", err)
	}
}

func TestLoad_ExampleConfigFileStillLoads(t *testing.T) {
	resetGlobalConfig()
	if _, err := LoadConfig("../../dingo.yaml.example"); err != nil {
		t.Fatalf("LoadConfig(dingo.yaml.example): %v", err)
	}
}
