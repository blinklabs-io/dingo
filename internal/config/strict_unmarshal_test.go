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

// TestLoad_UnknownTopLevelSiblingOfWrappedConfigRejected guards against a
// typo'd root-level sibling (e.g. "databse" instead of "database") next to
// a valid "config:" section silently vanishing. Before this check, only
// tempCfg.Config was strict-decoded; the root document itself was decoded
// leniently, so an unrecognized sibling was dropped with no error and the
// intended section (e.g. database plugin settings) silently fell back to
// defaults instead of applying.
func TestLoad_UnknownTopLevelSiblingOfWrappedConfigRejected(t *testing.T) {
	resetGlobalConfig()
	tmpFile := writeStrictTestConfig(t, `
config:
  network: "preview"
databse:
  blob:
    plugin: badger
`)
	_, err := LoadConfig(tmpFile)
	if err == nil {
		t.Fatal("expected error for unknown top-level sibling of config:, got nil")
	}
	if !strings.Contains(err.Error(), "databse") {
		t.Errorf("error %q does not mention the unknown sibling key", err.Error())
	}
}

// TestLoad_UnknownDatabaseChildKeyRejected guards against a typo'd key
// directly under "database:" (e.g. "blbo" instead of "blob") silently
// vanishing. tempConfig's databaseConfig struct only declares Blob/
// Metadata fields and is decoded leniently, so before this check an
// unrecognized child key was dropped with no error and the intended
// plugin config silently fell back to defaults instead of applying.
func TestLoad_UnknownDatabaseChildKeyRejected(t *testing.T) {
	resetGlobalConfig()
	tmpFile := writeStrictTestConfig(t, `
network: "preview"
database:
  blbo:
    plugin: badger
`)
	_, err := LoadConfig(tmpFile)
	if err == nil {
		t.Fatal("expected error for unknown key under database:, got nil")
	}
	if !strings.Contains(err.Error(), "blbo") {
		t.Errorf("error %q does not mention the unknown key", err.Error())
	}
}

// TestLoad_UnknownDatabaseChildKeyRejectedInWrappedMode is the wrapped-mode
// ("config:" section present) counterpart of
// TestLoad_UnknownDatabaseChildKeyRejected, since "database:" is a sibling
// of "config:" in both modes.
func TestLoad_UnknownDatabaseChildKeyRejectedInWrappedMode(t *testing.T) {
	resetGlobalConfig()
	tmpFile := writeStrictTestConfig(t, `
config:
  network: "preview"
database:
  metadtaa:
    plugin: sqlite
`)
	_, err := LoadConfig(tmpFile)
	if err == nil {
		t.Fatal("expected error for unknown key under database:, got nil")
	}
	if !strings.Contains(err.Error(), "metadtaa") {
		t.Errorf("error %q does not mention the unknown key", err.Error())
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
