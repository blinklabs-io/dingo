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

	"github.com/spf13/cobra"
)

// TestStrictFlags_DefaultOff verifies the new fail-fast toggles default to
// false, preserving the historical warn-and-continue behavior unless an
// operator opts in.
func TestStrictFlags_DefaultOff(t *testing.T) {
	resetGlobalConfig()
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.StrictLeaderEligibility {
		t.Error("StrictLeaderEligibility should default to false")
	}
	if cfg.StrictSlotClock {
		t.Error("StrictSlotClock should default to false")
	}
}

// TestStrictFlags_YAML verifies the toggles are read from YAML.
func TestStrictFlags_YAML(t *testing.T) {
	resetGlobalConfig()
	tmpFile := filepath.Join(t.TempDir(), "dingo.yaml")
	body := "strictLeaderEligibility: true\nstrictSlotClock: true\n"
	if err := os.WriteFile(tmpFile, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if !cfg.StrictLeaderEligibility {
		t.Error("StrictLeaderEligibility should be true from YAML")
	}
	if !cfg.StrictSlotClock {
		t.Error("StrictSlotClock should be true from YAML")
	}
}

// TestStrictFlags_CLI verifies the toggles are settable via CLI flags.
func TestStrictFlags_CLI(t *testing.T) {
	resetGlobalConfig()
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.ParseFlags([]string{
		"--strict-leader-eligibility",
		"--strict-slot-clock",
	}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}
	if err := ApplyFlags(cmd, cfg); err != nil {
		t.Fatalf("ApplyFlags: %v", err)
	}
	if !cfg.StrictLeaderEligibility {
		t.Error("StrictLeaderEligibility should be true from CLI flag")
	}
	if !cfg.StrictSlotClock {
		t.Error("StrictSlotClock should be true from CLI flag")
	}
}
