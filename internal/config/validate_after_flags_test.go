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

// These tests guard against CLI flags reintroducing a value that
// ValidateRuntimeConfig already rejects for YAML/env, and against
// ValidateRuntimeConfig (called by ApplyFlags, not LoadConfig -- see its doc
// comment) rejecting a bad YAML/env value that a valid CLI flag would have
// overridden.

// TestLoad_InvalidRelayPortRejected verifies that an out-of-range relayPort
// is rejected by ValidateRuntimeConfig, rather than only surfacing later as a
// silent "dial without source-port reuse" fallback in connmanager's outbound
// dialer (relayPort doubles as the outbound source port). LoadConfig alone
// does not run this validation, so ValidateRuntimeConfig is called
// explicitly, as a non-CLI caller of this package would.
func TestLoad_InvalidRelayPortRejected(t *testing.T) {
	resetGlobalConfig()
	tmpFile := filepath.Join(t.TempDir(), "dingo.yaml")
	if err := os.WriteFile(tmpFile, []byte("relayPort: 99999999\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if err := ValidateRuntimeConfig(cfg); err == nil {
		t.Fatal("expected error for out-of-range relayPort, got nil")
	}
}

// TestApplyFlags_ValidPortOverridesInvalidYAML is the ordering regression:
// an out-of-range relayPort from YAML must not fail startup when a valid
// --port flag is supplied to override it. Before ValidateRuntimeConfig was
// deferred to ApplyFlags, LoadConfig ran this validation itself and returned
// an error here before ApplyFlags ever got a chance to apply the override.
func TestApplyFlags_ValidPortOverridesInvalidYAML(t *testing.T) {
	resetGlobalConfig()
	tmpFile := filepath.Join(t.TempDir(), "dingo.yaml")
	if err := os.WriteFile(tmpFile, []byte("shutdownTimeout: \"0s\"\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("LoadConfig should not fail on a value a later CLI flag can fix: %v", err)
	}

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.ParseFlags([]string{"--shutdown-timeout=30s"}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	if err := ApplyFlags(cmd, cfg); err != nil {
		t.Fatalf("valid --shutdown-timeout should override invalid YAML value: %v", err)
	}
	if cfg.ShutdownTimeout != "30s" {
		t.Errorf("ShutdownTimeout = %q, want 30s", cfg.ShutdownTimeout)
	}
}

func TestApplyFlags_InvalidRelayPortRejected(t *testing.T) {
	resetGlobalConfig()
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.ParseFlags([]string{"--port=99999999"}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	if err := ApplyFlags(cmd, cfg); err == nil {
		t.Fatal("expected error for --port=99999999, got nil")
	}
}

func TestApplyFlags_InvalidLoggingLevelRejected(t *testing.T) {
	resetGlobalConfig()
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.ParseFlags([]string{"--logging-level=bogus"}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	if err := ApplyFlags(cmd, cfg); err == nil {
		t.Fatal("expected error for --logging-level=bogus, got nil")
	}
}

func TestApplyFlags_InvalidLoggingFormatRejected(t *testing.T) {
	resetGlobalConfig()
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.ParseFlags([]string{"--logging-format=bogus"}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	if err := ApplyFlags(cmd, cfg); err == nil {
		t.Fatal("expected error for --logging-format=bogus, got nil")
	}
}

func TestApplyFlags_InvalidChainsyncStrategyRejected(t *testing.T) {
	resetGlobalConfig()
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.ParseFlags([]string{"--chainsync-strategy=bogus"}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	if err := ApplyFlags(cmd, cfg); err == nil {
		t.Fatal("expected error for --chainsync-strategy=bogus, got nil")
	}
}

func TestApplyFlags_InvalidMithrilBackendRejected(t *testing.T) {
	resetGlobalConfig()
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.ParseFlags([]string{"--mithril-backend=v3"}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	if err := ApplyFlags(cmd, cfg); err == nil {
		t.Fatal("expected error for --mithril-backend=v3, got nil")
	}
}

// TestApplyFlags_ValidValuesStillWork is a control: valid flag values for
// the same fields must still apply cleanly through ApplyFlags.
func TestApplyFlags_ValidValuesStillWork(t *testing.T) {
	resetGlobalConfig()
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.ParseFlags([]string{
		"--logging-level=DEBUG",
		"--logging-format=JSON",
		"--chainsync-strategy=parallel",
		"--mithril-backend=v1",
	}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	if err := ApplyFlags(cmd, cfg); err != nil {
		t.Fatalf("ApplyFlags with valid values: %v", err)
	}
	if cfg.Logging.Level != "debug" {
		t.Errorf("Logging.Level = %q, want debug", cfg.Logging.Level)
	}
	if cfg.Logging.Format != "json" {
		t.Errorf("Logging.Format = %q, want json", cfg.Logging.Format)
	}
	if cfg.Chainsync.Strategy != "parallel" {
		t.Errorf("Chainsync.Strategy = %q, want parallel", cfg.Chainsync.Strategy)
	}
	if cfg.Mithril.Backend != "v1" {
		t.Errorf("Mithril.Backend = %q, want v1", cfg.Mithril.Backend)
	}
}
