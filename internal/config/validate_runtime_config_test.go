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

// These tests cover fields whose invalid values used to be ignored (falling
// back to a default) or surfaced only much later at runtime, rather than
// failing at config load. See validateRuntimeConfig.

func loadYAML(t *testing.T, body string) error {
	t.Helper()
	resetGlobalConfig()
	tmpFile := filepath.Join(t.TempDir(), "dingo.yaml")
	if err := os.WriteFile(tmpFile, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	_, err := LoadConfig(tmpFile)
	return err
}

// TestLoad_OutOfRangePortsRejected verifies that every listen/connect port
// above 65535 is rejected at load, rather than surfacing later as a bind
// failure.
func TestLoad_OutOfRangePortsRejected(t *testing.T) {
	for _, key := range []string{
		"privatePort",
		"utxorpcPort",
		"barkPort",
		"metricsPort",
		"debugPort",
		"blockfrostPort",
		"meshPort",
	} {
		t.Run(key, func(t *testing.T) {
			if err := loadYAML(t, key+": 70000\n"); err == nil {
				t.Fatalf("expected error for out-of-range %s, got nil", key)
			}
		})
	}
}

// TestLoad_MidnightPortOutOfRangeRejected covers the nested midnight.port.
func TestLoad_MidnightPortOutOfRangeRejected(t *testing.T) {
	if err := loadYAML(t, "midnight:\n  port: 70000\n"); err == nil {
		t.Fatal("expected error for out-of-range midnight.port, got nil")
	}
}

// TestLoad_ValidPortsAccepted is a control: in-range ports (including 0 to
// disable) load cleanly.
func TestLoad_ValidPortsAccepted(t *testing.T) {
	if err := loadYAML(t, "metricsPort: 0\nutxorpcPort: 65535\n"); err != nil {
		t.Fatalf("valid ports rejected: %v", err)
	}
}

// TestLoad_InvalidStorageModeRejected verifies an invalid storage mode from
// YAML fails at load instead of falling through to the node-startup check.
func TestLoad_InvalidStorageModeRejected(t *testing.T) {
	if err := loadYAML(t, "storageMode: coree\n"); err == nil {
		t.Fatal("expected error for invalid storageMode, got nil")
	}
}

func TestLoad_ValidStorageModeAccepted(t *testing.T) {
	for _, mode := range []string{"core", "api", "API", "Core"} {
		if err := loadYAML(t, "storageMode: "+mode+"\n"); err != nil {
			t.Fatalf("valid storageMode %q rejected: %v", mode, err)
		}
	}
}

// TestLoad_InvalidDurationStringsRejected verifies malformed duration
// strings fail at load rather than being silently ignored (and the default
// used) at the point they are eventually parsed.
func TestLoad_InvalidDurationStringsRejected(t *testing.T) {
	cases := map[string]string{
		"shutdownTimeout":             "shutdownTimeout: \"30x\"\n",
		"ledgerCatchupTimeout":        "ledgerCatchupTimeout: \"nope\"\n",
		"chainsync.stallTimeout":      "chainsync:\n  stallTimeout: \"5min\"\n",
		"mithril.downloadIdleTimeout": "mithril:\n  downloadIdleTimeout: \"soon\"\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if err := loadYAML(t, body); err == nil {
				t.Fatalf("expected error for invalid %s, got nil", name)
			}
		})
	}
}

func TestLoad_ValidDurationStringsAccepted(t *testing.T) {
	body := "shutdownTimeout: \"45s\"\n" +
		"ledgerCatchupTimeout: \"10m\"\n" +
		"chainsync:\n  stallTimeout: \"90s\"\n" +
		"mithril:\n  downloadIdleTimeout: \"2m\"\n"
	if err := loadYAML(t, body); err != nil {
		t.Fatalf("valid duration strings rejected: %v", err)
	}
}

// TestApplyFlags_OutOfRangeMetricsPortRejected guards the flag path: a bad
// --metrics-port must be rejected by the re-validation at the end of
// ApplyFlags, not just on the YAML/env path.
func TestApplyFlags_OutOfRangeMetricsPortRejected(t *testing.T) {
	resetGlobalConfig()
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.ParseFlags([]string{"--metrics-port=70000"}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}
	if err := ApplyFlags(cmd, cfg); err == nil {
		t.Fatal("expected error for --metrics-port=70000, got nil")
	}
}
