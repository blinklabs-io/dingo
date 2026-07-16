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

func TestDefaultLoggingConfig(t *testing.T) {
	d := DefaultLoggingConfig()
	if d.Format != "text" {
		t.Errorf("default Format = %q, want text", d.Format)
	}
	if d.Level != "info" {
		t.Errorf("default Level = %q, want info", d.Level)
	}
}

func TestLoad_LoggingEnvVars(t *testing.T) {
	resetGlobalConfig()
	// Avoid picking up a real ~/.dingo/dingo.yaml on the dev machine.
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DINGO_LOGGING_FORMAT", "json")
	t.Setenv("DINGO_LOGGING_LEVEL", "warn")

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Logging.Format != "json" {
		t.Errorf("Logging.Format = %q, want json", cfg.Logging.Format)
	}
	if cfg.Logging.Level != "warn" {
		t.Errorf("Logging.Level = %q, want warn", cfg.Logging.Level)
	}
}

// TestLoad_InvalidLoggingLevelRejected verifies ValidateRuntimeConfig rejects
// an invalid logging.level. LoadConfig alone does not run this validation --
// see ValidateRuntimeConfig's doc comment -- so it is called explicitly here,
// as a non-CLI caller of this package would.
func TestLoad_InvalidLoggingLevelRejected(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	yamlContent := "logging:\n  level: bogus\n"
	tmpFile := filepath.Join(t.TempDir(), "dingo.yaml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if err := ValidateRuntimeConfig(cfg); err == nil {
		t.Fatal("expected error for invalid logging.level, got nil")
	}
}

func TestLoad_InvalidLoggingLevelViaEnvRejected(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DINGO_LOGGING_LEVEL", "bogus")
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if err := ValidateRuntimeConfig(cfg); err == nil {
		t.Fatal("expected error for invalid DINGO_LOGGING_LEVEL, got nil")
	}
}

func TestLoad_InvalidLoggingFormatRejected(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	yamlContent := "logging:\n  format: bogus\n"
	tmpFile := filepath.Join(t.TempDir(), "dingo.yaml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if err := ValidateRuntimeConfig(cfg); err == nil {
		t.Fatal("expected error for invalid logging.format, got nil")
	}
}

func TestLoad_LoggingFromYAML(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	// Ensure a developer's exported DINGO_LOGGING_* env vars cannot override
	// the YAML values under test. t.Setenv records the originals for restore;
	// os.Unsetenv then removes them for the duration of the test.
	t.Setenv("DINGO_LOGGING_FORMAT", "")
	os.Unsetenv("DINGO_LOGGING_FORMAT")
	t.Setenv("DINGO_LOGGING_LEVEL", "")
	os.Unsetenv("DINGO_LOGGING_LEVEL")

	yamlContent := "logging:\n  format: json\n  level: error\n"
	tmpFile := filepath.Join(t.TempDir(), "dingo.yaml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Logging.Format != "json" {
		t.Errorf("Logging.Format = %q, want json", cfg.Logging.Format)
	}
	if cfg.Logging.Level != "error" {
		t.Errorf("Logging.Level = %q, want error", cfg.Logging.Level)
	}
}
