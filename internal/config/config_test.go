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

package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func resetGlobalConfig() {
	globalConfig = &Config{
		MempoolCapacity:      1048576,
		EvictionWatermark:    0.90,
		RejectionWatermark:   0.95,
		BindAddr:             "0.0.0.0",
		CardanoConfig:        "", // Will be set dynamically based on network
		DatabasePath:         ".dingo",
		SocketPath:           "dingo.socket",
		IntersectTip:         false,
		ValidateHistorical:   false,
		Network:              "preview",
		MetricsPort:          12798,
		PrivateBindAddr:      "127.0.0.1",
		PrivatePort:          3002,
		RelayPort:            3001,
		UtxorpcPort:          0,
		Topology:             "",
		TlsCertFilePath:      "",
		TlsKeyFilePath:       "",
		BlobPlugin:           DefaultBlobPlugin,
		MetadataPlugin:       DefaultMetadataPlugin,
		RunMode:              RunModeServe,
		ImmutableDbPath:      "",
		ShutdownTimeout:      DefaultShutdownTimeout,
		LedgerCatchupTimeout: DefaultLedgerCatchupTimeout,
		DatabaseWorkers:      5,
		DatabaseQueueSize:    50,
		Mithril: MithrilConfig{
			Enabled:            true,
			CleanupAfterLoad:   true,
			VerifyCertificates: true,
		},
	}
}

func TestLoad_CompareFullStruct(t *testing.T) {
	resetGlobalConfig()
	yamlContent := `
badgerCacheSize: 8388608
mempoolCapacity: 2097152
bindAddr: "127.0.0.1"
cardanoConfig: "./cardano/preview/config.json"
databasePath: ".dingo"
socketPath: "env.socket"
intersectTip: true
network: "preview"
metricsPort: 8088
privateBindAddr: "127.0.0.1"
privatePort: 8000
relayPort: 4000
utxorpcPort: 9940
topology: ""
tlsCertFilePath: "cert1.pem"
tlsKeyFilePath: "key1.pem"
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-dingo.yaml")

	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	defer os.Remove(tmpFile)

	expected := &Config{
		MempoolCapacity:      2097152,
		EvictionWatermark:    0.90,
		RejectionWatermark:   0.95,
		BindAddr:             "127.0.0.1",
		CardanoConfig:        "./cardano/preview/config.json",
		DatabasePath:         ".dingo",
		SocketPath:           "env.socket",
		IntersectTip:         true,
		ValidateHistorical:   false,
		Network:              "preview",
		MetricsPort:          8088,
		PrivateBindAddr:      "127.0.0.1",
		PrivatePort:          8000,
		RelayPort:            4000,
		UtxorpcPort:          9940, // explicit override from YAML
		Topology:             "",
		TlsCertFilePath:      "cert1.pem",
		TlsKeyFilePath:       "key1.pem",
		BlobPlugin:           DefaultBlobPlugin,
		MetadataPlugin:       DefaultMetadataPlugin,
		RunMode:              RunModeServe,
		ImmutableDbPath:      "",
		ShutdownTimeout:      DefaultShutdownTimeout,
		LedgerCatchupTimeout: DefaultLedgerCatchupTimeout,
		DatabaseWorkers:      5,
		DatabaseQueueSize:    50,
		Mithril: MithrilConfig{
			Enabled:            true,
			CleanupAfterLoad:   true,
			VerifyCertificates: true,
		},
	}

	actual, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf(
			"Loaded config does not match expected.\nActual: %+v\nExpected: %+v",
			actual,
			expected,
		)
	}
}
func TestLoad_WithoutConfigFile_UsesDefaults(t *testing.T) {
	resetGlobalConfig()

	// Without Config file
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	// Expected is the updated default values from globalConfig
	expected := &Config{
		MempoolCapacity:      1048576,
		EvictionWatermark:    0.90,
		RejectionWatermark:   0.95,
		BindAddr:             "0.0.0.0",
		CardanoConfig:        "preview/config.json", // Set dynamically based on network
		DatabasePath:         ".dingo",
		SocketPath:           "dingo.socket",
		IntersectTip:         false,
		ValidateHistorical:   false,
		Network:              "preview",
		MetricsPort:          12798,
		PrivateBindAddr:      "127.0.0.1",
		PrivatePort:          3002,
		RelayPort:            3001,
		UtxorpcPort:          0,
		Topology:             "",
		TlsCertFilePath:      "",
		TlsKeyFilePath:       "",
		BlobPlugin:           DefaultBlobPlugin,
		MetadataPlugin:       DefaultMetadataPlugin,
		RunMode:              RunModeServe,
		ImmutableDbPath:      "",
		ShutdownTimeout:      DefaultShutdownTimeout,
		LedgerCatchupTimeout: DefaultLedgerCatchupTimeout,
		DatabaseWorkers:      5,
		DatabaseQueueSize:    50,
		Mithril: MithrilConfig{
			Enabled:            true,
			CleanupAfterLoad:   true,
			VerifyCertificates: true,
		},
	}

	if !reflect.DeepEqual(cfg, expected) {
		t.Errorf(
			"config mismatch without file:\nExpected: %+v\nGot:      %+v",
			expected,
			cfg,
		)
	}
}

func TestLoad_WithRunModeConfig(t *testing.T) {
	resetGlobalConfig()

	// Test with dev mode in config file
	yamlContent := `
runMode: "dev"
network: "preview"
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-run-mode.yaml")

	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if cfg.RunMode != RunModeDev {
		t.Errorf("expected RunMode to be 'dev', got: %v", cfg.RunMode)
	}
	if !cfg.RunMode.IsDevMode() {
		t.Error("expected IsDevMode() to return true for 'dev' mode")
	}
}

func TestRunMode_Validation(t *testing.T) {
	tests := []struct {
		mode  RunMode
		valid bool
	}{
		{RunModeServe, true},
		{RunModeLoad, true},
		{RunModeDev, true},
		{"", true}, // empty is valid (defaults to serve)
		{"invalid", false},
	}
	for _, tt := range tests {
		if got := tt.mode.Valid(); got != tt.valid {
			t.Errorf(
				"RunMode(%q).Valid() = %v, want %v",
				tt.mode,
				got,
				tt.valid,
			)
		}
	}
}

func TestRunMode_IsDevMode(t *testing.T) {
	tests := []struct {
		mode      RunMode
		isDevMode bool
	}{
		{RunModeServe, false},
		{RunModeLoad, false},
		{RunModeDev, true},
		{"", false},
	}
	for _, tt := range tests {
		if got := tt.mode.IsDevMode(); got != tt.isDevMode {
			t.Errorf(
				"RunMode(%q).IsDevMode() = %v, want %v",
				tt.mode,
				got,
				tt.isDevMode,
			)
		}
	}
}

func TestLoad_WithLoadModeConfig(t *testing.T) {
	resetGlobalConfig()

	yamlContent := `
runMode: "load"
immutableDbPath: "/path/to/immutable"
network: "preview"
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-load-mode.yaml")

	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	// Set dev mode to avoid topology loading issues during test
	globalConfig.RunMode = RunModeDev

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if cfg.RunMode != RunModeLoad {
		t.Errorf("expected RunMode to be 'load', got: %v", cfg.RunMode)
	}
	if cfg.ImmutableDbPath != "/path/to/immutable" {
		t.Errorf(
			"expected ImmutableDbPath to be '/path/to/immutable', got: %v",
			cfg.ImmutableDbPath,
		)
	}
}

func TestLoadConfig_EmbeddedDefaults(t *testing.T) {
	resetGlobalConfig()

	// Test loading config without any file (should use defaults including embedded path)
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("expected no error loading default config, got: %v", err)
	}

	// Should use embedded path for CardanoConfig
	expected := "preview/config.json"
	if cfg.CardanoConfig != expected {
		t.Errorf(
			"expected CardanoConfig to be %q, got %q",
			expected,
			cfg.CardanoConfig,
		)
	}

	// Should have other default values
	if cfg.Network != "preview" {
		t.Errorf("expected Network to be 'preview', got %q", cfg.Network)
	}

	if cfg.RelayPort != 3001 {
		t.Errorf("expected RelayPort to be 3001, got %d", cfg.RelayPort)
	}
}

func TestLoadConfig_MainnetNetwork(t *testing.T) {
	resetGlobalConfig()
	globalConfig.Network = "mainnet"

	// Test loading config with non-preview network uses /opt/cardano path
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("expected no error for non-preview network, got: %v", err)
	}

	// Should use /opt/cardano path for non-preview networks
	expected := "/opt/cardano/mainnet/config.json"
	if cfg.CardanoConfig != expected {
		t.Errorf(
			"expected CardanoConfig to be %q, got %q",
			expected,
			cfg.CardanoConfig,
		)
	}

	if cfg.Network != "mainnet" {
		t.Errorf("expected Network to be 'mainnet', got %q", cfg.Network)
	}
}

func TestLoadConfig_DevnetNetwork(t *testing.T) {
	resetGlobalConfig()
	globalConfig.Network = "devnet"
	globalConfig.RunMode = RunModeDev // Set dev mode to avoid topology issues

	// Test loading config with devnet network uses /opt/cardano path
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("expected no error for devnet network, got: %v", err)
	}

	// Should use /opt/cardano path for devnet network
	expected := "/opt/cardano/devnet/config.json"
	if cfg.CardanoConfig != expected {
		t.Errorf(
			"expected CardanoConfig to be %q, got %q",
			expected,
			cfg.CardanoConfig,
		)
	}

	if cfg.Network != "devnet" {
		t.Errorf("expected Network to be 'devnet', got %q", cfg.Network)
	}
}

func TestLoadConfig_UnsupportedNetworkWithUserConfig(t *testing.T) {
	resetGlobalConfig()
	globalConfig.Network = "unsupported"
	globalConfig.CardanoConfig = "/custom/path/config.json"
	globalConfig.RunMode = RunModeDev // Set dev mode to avoid topology issues

	// Test that unsupported network works if user provides CardanoConfig
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf(
			"expected no error when user provides CardanoConfig, got: %v",
			err,
		)
	}

	if cfg.CardanoConfig != "/custom/path/config.json" {
		t.Errorf(
			"expected CardanoConfig to be user-provided path, got %q",
			cfg.CardanoConfig,
		)
	}
}

func TestLoadConfig_WatermarkValidation(t *testing.T) {
	tests := []struct {
		name       string
		eviction   float64
		rejection  float64
		wantErr    bool
		errContain string
	}{
		{
			name:      "defaults when both zero",
			eviction:  0,
			rejection: 0,
			wantErr:   false,
		},
		{
			name:      "default eviction when zero with explicit rejection",
			eviction:  0,
			rejection: 0.95,
			wantErr:   false,
		},
		{
			name:      "default rejection when zero with explicit eviction",
			eviction:  0.80,
			rejection: 0,
			wantErr:   false,
		},
		{
			name:      "valid custom values",
			eviction:  0.75,
			rejection: 0.85,
			wantErr:   false,
		},
		{
			name:      "rejection at exactly 1.0",
			eviction:  0.90,
			rejection: 1.0,
			wantErr:   false,
		},
		{
			name:       "eviction negative",
			eviction:   -0.1,
			rejection:  0.95,
			wantErr:    true,
			errContain: "invalid evictionWatermark",
		},
		{
			name:       "rejection negative",
			eviction:   0.90,
			rejection:  -0.5,
			wantErr:    true,
			errContain: "invalid rejectionWatermark",
		},
		{
			name:       "eviction above 1",
			eviction:   1.5,
			rejection:  0.95,
			wantErr:    true,
			errContain: "invalid evictionWatermark",
		},
		{
			name:       "rejection above 1",
			eviction:   0.90,
			rejection:  1.1,
			wantErr:    true,
			errContain: "invalid rejectionWatermark",
		},
		{
			name:       "eviction equals rejection",
			eviction:   0.90,
			rejection:  0.90,
			wantErr:    true,
			errContain: "must be less than",
		},
		{
			name:       "eviction greater than rejection",
			eviction:   0.95,
			rejection:  0.90,
			wantErr:    true,
			errContain: "must be less than",
		},
		{
			name:       "eviction at exactly 1.0",
			eviction:   1.0,
			rejection:  0.95,
			wantErr:    true,
			errContain: "invalid evictionWatermark",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetGlobalConfig()
			globalConfig.EvictionWatermark = tt.eviction
			globalConfig.RejectionWatermark = tt.rejection
			globalConfig.RunMode = RunModeDev

			_, err := LoadConfig("")
			if tt.wantErr {
				if err == nil {
					t.Fatalf(
						"expected error containing %q, got nil",
						tt.errContain,
					)
				}
				if tt.errContain != "" {
					if got := err.Error(); !strings.Contains(
						got,
						tt.errContain,
					) {
						t.Errorf(
							"error %q should contain %q",
							got,
							tt.errContain,
						)
					}
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestLoad_DatabaseSection(t *testing.T) {
	resetGlobalConfig()
	yamlContent := `
database:
  blob:
    plugin: "badger"
    badger:
      data-dir: "/tmp/badger"
      block-cache-size: 1000000
    gcs:
      bucket: "test-bucket"
  metadata:
    plugin: "sqlite"
    sqlite:
      db-path: "/tmp/test.db"
`
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-dingo.yaml")

	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	defer os.Remove(tmpFile)

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.BlobPlugin != "badger" {
		t.Errorf("expected BlobPlugin to be 'badger', got %q", cfg.BlobPlugin)
	}

	if cfg.MetadataPlugin != "sqlite" {
		t.Errorf(
			"expected MetadataPlugin to be 'sqlite', got %q",
			cfg.MetadataPlugin,
		)
	}
}

func TestLoadConfig_NetworkNameValidation(t *testing.T) {
	validTests := []struct {
		name           string
		network        string
		wantConfigPath string
	}{
		{
			name:           "preview",
			network:        "preview",
			wantConfigPath: "preview/config.json",
		},
		{
			name:           "mainnet",
			network:        "mainnet",
			wantConfigPath: "/opt/cardano/mainnet/config.json",
		},
		{
			name:           "preprod",
			network:        "preprod",
			wantConfigPath: "/opt/cardano/preprod/config.json",
		},
		{
			name:           "hyphenated name",
			network:        "my-devnet",
			wantConfigPath: "/opt/cardano/my-devnet/config.json",
		},
		{
			name:           "underscore name",
			network:        "test_net",
			wantConfigPath: "/opt/cardano/test_net/config.json",
		},
	}

	for _, tt := range validTests {
		t.Run("valid/"+tt.name, func(t *testing.T) {
			resetGlobalConfig()
			globalConfig.Network = tt.network
			globalConfig.RunMode = RunModeDev

			cfg, err := LoadConfig("")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if cfg.Network != tt.network {
				t.Errorf(
					"Network = %q, want %q",
					cfg.Network,
					tt.network,
				)
			}

			if cfg.CardanoConfig != tt.wantConfigPath {
				t.Errorf(
					"CardanoConfig = %q, want %q",
					cfg.CardanoConfig,
					tt.wantConfigPath,
				)
			}
		})
	}

	invalidTests := []struct {
		name    string
		network string
	}{
		{
			name:    "parent directory traversal",
			network: "../etc",
		},
		{
			name:    "deep traversal",
			network: "../../etc",
		},
		{
			name:    "forward slash",
			network: "foo/bar",
		},
		{
			name:    "bare dot-dot",
			network: "..",
		},
		{
			name:    "backslash",
			network: "foo\\bar",
		},
		{
			name:    "absolute path",
			network: "/etc/passwd",
		},
		{
			name:    "dot prefix",
			network: ".hidden",
		},
		{
			name:    "empty string",
			network: "",
		},
		{
			name:    "hyphen prefix",
			network: "-bad",
		},
		{
			name:    "underscore prefix",
			network: "_bad",
		},
	}

	for _, tt := range invalidTests {
		t.Run("invalid/"+tt.name, func(t *testing.T) {
			resetGlobalConfig()
			globalConfig.Network = tt.network
			globalConfig.RunMode = RunModeDev

			_, err := LoadConfig("")
			if err == nil {
				t.Fatal(
					"expected error for invalid network name, got nil",
				)
			}

			if !strings.Contains(err.Error(), "invalid network name") {
				t.Errorf(
					"error %q should contain %q",
					err.Error(),
					"invalid network name",
				)
			}
		})
	}
}

func TestLoad_APIPorts(t *testing.T) {
	resetGlobalConfig()
	yamlContent := `
blockfrostPort: 8080
utxorpcPort: 9090
meshPort: 8081
network: "preview"
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-api-ports.yaml")

	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.BlockfrostPort != 8080 {
		t.Errorf(
			"expected BlockfrostPort to be 8080, got %d",
			cfg.BlockfrostPort,
		)
	}
	if cfg.UtxorpcPort != 9090 {
		t.Errorf(
			"expected UtxorpcPort to be 9090, got %d",
			cfg.UtxorpcPort,
		)
	}
	if cfg.MeshPort != 8081 {
		t.Errorf(
			"expected MeshPort to be 8081, got %d",
			cfg.MeshPort,
		)
	}
}

func TestLoad_APIPortsDefault(t *testing.T) {
	resetGlobalConfig()
	globalConfig.RunMode = RunModeDev

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.BlockfrostPort != 0 {
		t.Errorf(
			"expected BlockfrostPort default to be 0, got %d",
			cfg.BlockfrostPort,
		)
	}
	if cfg.UtxorpcPort != 0 {
		t.Errorf(
			"expected UtxorpcPort default to be 0, got %d",
			cfg.UtxorpcPort,
		)
	}
	if cfg.MeshPort != 0 {
		t.Errorf(
			"expected MeshPort default to be 0, got %d",
			cfg.MeshPort,
		)
	}
}

func TestLoad_StorageMode(t *testing.T) {
	resetGlobalConfig()
	yamlContent := `
storageMode: "api"
network: "preview"
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-storage-mode.yaml")

	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.StorageMode != "api" {
		t.Errorf("expected StorageMode to be 'api', got %q", cfg.StorageMode)
	}
}

func TestLoad_StorageModeDefault(t *testing.T) {
	resetGlobalConfig()
	globalConfig.RunMode = RunModeDev

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.StorageMode != "" {
		t.Errorf("expected StorageMode default to be empty, got %q", cfg.StorageMode)
	}
}
