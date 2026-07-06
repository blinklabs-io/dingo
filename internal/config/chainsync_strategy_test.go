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
	"testing"
)

func TestLoad_InvalidChainsyncStrategyRejected(t *testing.T) {
	resetGlobalConfig()
	tmpFile := filepath.Join(t.TempDir(), "dingo.yaml")
	yamlContent := "network: preview\nchainsync:\n  strategy: bogus\n"
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := LoadConfig(tmpFile); err == nil {
		t.Fatal("expected error for invalid chainsync.strategy, got nil")
	}
}

func TestLoad_ValidChainsyncStrategyAccepted(t *testing.T) {
	resetGlobalConfig()
	tmpFile := filepath.Join(t.TempDir(), "dingo.yaml")
	yamlContent := "network: preview\nchainsync:\n  strategy: parallel\n"
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Chainsync.Strategy != "parallel" {
		t.Errorf("Chainsync.Strategy = %q, want parallel", cfg.Chainsync.Strategy)
	}
}
