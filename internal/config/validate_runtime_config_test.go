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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
)

// These tests cover fields whose invalid values used to be ignored (falling
// back to a default) or surfaced only much later at runtime, rather than
// failing at config load. See ValidateRuntimeConfig.
//
// LoadConfig itself does not run this validation (see ValidateRuntimeConfig's
// doc comment: a valid CLI flag, applied afterward by ApplyFlags, must be
// able to override a semantically invalid YAML/env value). loadYAML calls
// ValidateRuntimeConfig explicitly after LoadConfig, exercising it the same
// way a non-CLI caller of this package would.

func loadYAML(t *testing.T, body string) error {
	t.Helper()
	resetGlobalConfig()
	tmpFile := filepath.Join(t.TempDir(), "dingo.yaml")
	if err := os.WriteFile(tmpFile, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		return err
	}
	return ValidateRuntimeConfig(cfg)
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

// TestLoad_StorageModeNormalized verifies a mixed-case/whitespace value is
// normalized at load, so it behaves like the CLI path instead of passing
// load and then failing the case-sensitive check at node startup.
func TestLoad_StorageModeNormalized(t *testing.T) {
	resetGlobalConfig()
	tmpFile := filepath.Join(t.TempDir(), "dingo.yaml")
	if err := os.WriteFile(tmpFile, []byte("storageMode: \"  API  \"\n"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if err := ValidateRuntimeConfig(cfg); err != nil {
		t.Fatalf("ValidateRuntimeConfig: %v", err)
	}
	if cfg.StorageMode != "api" {
		t.Errorf("StorageMode = %q, want normalized %q", cfg.StorageMode, "api")
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

// TestLoad_NonPositiveDurationsRejected verifies that zero and negative
// values for the positive-only timeout fields are rejected at load, rather
// than being accepted and then silently replaced with a default by their
// consumers (configuredShutdownTimeout / chainsync StallTimeout).
func TestLoad_NonPositiveDurationsRejected(t *testing.T) {
	fields := map[string]string{
		"shutdownTimeout":        "shutdownTimeout: %q\n",
		"ledgerCatchupTimeout":   "ledgerCatchupTimeout: %q\n",
		"chainsync.stallTimeout": "chainsync:\n  stallTimeout: %q\n",
	}
	for _, val := range []string{"0s", "0", "-5s", "-1m"} {
		for name, tmpl := range fields {
			t.Run(name+"="+val, func(t *testing.T) {
				body := fmt.Sprintf(tmpl, val)
				if err := loadYAML(t, body); err == nil {
					t.Fatalf(
						"expected error for non-positive %s=%q, got nil",
						name, val,
					)
				}
			})
		}
	}
}

// TestLoad_DownloadIdleTimeoutNonPositiveAccepted verifies the documented
// non-positive semantics of mithril.downloadIdleTimeout are preserved: "0"
// means "use the default" and a negative duration disables idle detection,
// so neither is rejected.
func TestLoad_DownloadIdleTimeoutNonPositiveAccepted(t *testing.T) {
	for _, val := range []string{"0s", "-1s", "-30m"} {
		body := fmt.Sprintf("mithril:\n  downloadIdleTimeout: %q\n", val)
		if err := loadYAML(t, body); err != nil {
			t.Fatalf(
				"downloadIdleTimeout=%q should be accepted, got: %v",
				val, err,
			)
		}
	}
}

// TestLoad_NonPositiveDurationEnvRejected covers the environment-variable
// input path for the positive-only timeout fields. ValidateRuntimeConfig is
// called explicitly since LoadConfig alone no longer runs semantic
// validation (see loadYAML's doc comment above).
func TestLoad_NonPositiveDurationEnvRejected(t *testing.T) {
	t.Run("stallTimeout", func(t *testing.T) {
		resetGlobalConfig()
		t.Setenv("DINGO_CHAINSYNC_STALL_TIMEOUT", "-5s")
		cfg, err := LoadConfig("")
		if err != nil {
			t.Fatalf("LoadConfig: %v", err)
		}
		if err := ValidateRuntimeConfig(cfg); err == nil {
			t.Fatal("expected error for negative stall timeout via env, got nil")
		}
	})
	t.Run("shutdownTimeout", func(t *testing.T) {
		resetGlobalConfig()
		t.Setenv("CARDANO_SHUTDOWN_TIMEOUT", "0s")
		cfg, err := LoadConfig("")
		if err != nil {
			t.Fatalf("LoadConfig: %v", err)
		}
		if err := ValidateRuntimeConfig(cfg); err == nil {
			t.Fatal("expected error for zero shutdown timeout via env, got nil")
		}
	})
	t.Run("ledgerCatchupTimeout", func(t *testing.T) {
		resetGlobalConfig()
		t.Setenv("DINGO_LEDGER_CATCHUP_TIMEOUT", "0s")
		cfg, err := LoadConfig("")
		if err != nil {
			t.Fatalf("LoadConfig: %v", err)
		}
		if err := ValidateRuntimeConfig(cfg); err == nil {
			t.Fatal("expected error for zero ledger catchup timeout via env, got nil")
		}
	})
}

// TestApplyFlags_NonPositiveDurationRejected covers the CLI-flag input path.
func TestApplyFlags_NonPositiveDurationRejected(t *testing.T) {
	for _, tc := range []struct {
		flag string
		val  string
	}{
		{"--chainsync-stall-timeout", "0s"},
		{"--shutdown-timeout", "-10s"},
		{"--ledger-catchup-timeout", "0"},
	} {
		t.Run(tc.flag+"="+tc.val, func(t *testing.T) {
			resetGlobalConfig()
			cfg, err := LoadConfig("")
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			cmd := &cobra.Command{Use: "dingo"}
			RegisterFlags(cmd)
			if err := cmd.ParseFlags([]string{tc.flag + "=" + tc.val}); err != nil {
				t.Fatalf("failed to parse flags: %v", err)
			}
			if err := ApplyFlags(cmd, cfg); err == nil {
				t.Fatalf("expected error for %s=%s, got nil", tc.flag, tc.val)
			}
		})
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
