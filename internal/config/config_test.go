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
	"time"

	"github.com/blinklabs-io/dingo/topology"
)

func resetGlobalConfig() {
	midnightYAMLFields = nil
	globalConfig = &Config{
		// MempoolCapacity left as the zero sentinel; ApplyDefaults
		// fills it in from RunMode after all sources are merged.
		MempoolCapacity:             0,
		MempoolImplementation:       DefaultMempoolImplementation,
		EvictionWatermark:           0.90,
		RejectionWatermark:          0.95,
		BindAddr:                    "0.0.0.0",
		CardanoConfig:               "", // Will be set dynamically based on network
		DatabasePath:                ".dingo",
		SocketPath:                  "dingo.socket",
		IntersectTip:                false,
		ValidateHistorical:          false,
		Network:                     "preview",
		MetricsPort:                 12798,
		PrivateBindAddr:             "127.0.0.1",
		PrivatePort:                 3002,
		RelayPort:                   3001,
		UtxorpcPort:                 9090,
		CORSAllowedOrigins:          []string{"*"},
		BlockfrostPort:              3000,
		MeshPort:                    8080,
		Topology:                    "",
		TlsCertFilePath:             "",
		TlsKeyFilePath:              "",
		BlobPlugin:                  DefaultBlobPlugin,
		MetadataPlugin:              DefaultMetadataPlugin,
		RunMode:                     RunModeServe,
		StartEra:                    StartEraDefault,
		ImmutableDbPath:             "",
		ShutdownTimeout:             DefaultShutdownTimeout,
		LedgerCatchupTimeout:        DefaultLedgerCatchupTimeout,
		DatabaseWorkers:             5,
		DatabaseQueueSize:           50,
		BackfillBatchSize:           100,
		GenesisBootstrap:            DefaultGenesisBootstrapConfig(),
		HistoryExpiry:               DefaultHistoryExpiryConfig(),
		Midnight:                    DefaultMidnightConfig(),
		ForgeSyncToleranceSlots:     DefaultForgeSyncToleranceSlots,
		ForgeStaleGapThresholdSlots: DefaultForgeStaleGapThresholdSlots,
		Mithril: MithrilConfig{
			Enabled:            true,
			CleanupAfterLoad:   true,
			VerifyCertificates: true,
		},
	}
	globalTopologyConfig = &topology.TopologyConfig{}
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
databaseWorkers: 11
databaseQueueSize: 77
backfillBatchSize: 200
immutableDbPath: "/tmp/immutable"
shutdownTimeout: "45s"
ledgerCatchupTimeout: "90m"
topology: ""
tlsCertFilePath: "cert1.pem"
tlsKeyFilePath: "key1.pem"
genesisBootstrap:
  enabled: false
  windowSlots: 4321
  promotionMinDiversityGroups: 4
midnight:
  port: 50052
  host: "127.0.0.1"
  cnightPolicyId: "policy1"
  cnightAssetName: "434e49474854"
  mappingValidatorAddress: "addr_mapping"
  authTokenAssetName: "auth"
  committeeCandidateAddress: "addr_candidate"
  technicalCommitteeAddress: "addr_technical"
  technicalCommitteePolicyId: "policy_technical"
  councilAddress: "addr_council"
  councilPolicyId: "policy_council"
  permissionedCandidatePolicy: "policy_permissioned"
mithril:
  enabled: false
  aggregatorUrl: "https://mithril.example.net"
  downloadDir: "/tmp/mithril"
  downloadIdleTimeout: "5m"
  downloadMaxIdleRetries: 9
  cleanupAfterLoad: false
  verifyCertificates: false
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-dingo.yaml")

	t.Setenv("DINGO_FORGE_SYNC_TOLERANCE_SLOTS", "321")
	t.Setenv("DINGO_FORGE_STALE_GAP_THRESHOLD_SLOTS", "654")

	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	defer os.Remove(tmpFile)

	expected := &Config{
		MempoolCapacity:       2097152,
		MempoolImplementation: DefaultMempoolImplementation,
		EvictionWatermark:     0.90,
		RejectionWatermark:    0.95,
		BindAddr:              "127.0.0.1",
		CardanoConfig:         "./cardano/preview/config.json",
		DatabasePath:          ".dingo",
		SocketPath:            "env.socket",
		IntersectTip:          true,
		ValidateHistorical:    false,
		Network:               "preview",
		MetricsPort:           8088,
		PrivateBindAddr:       "127.0.0.1",
		PrivatePort:           8000,
		RelayPort:             4000,
		UtxorpcPort:           9940, // explicit override from YAML
		CORSAllowedOrigins:    []string{"*"},
		BlockfrostPort:        3000, // default
		MeshPort:              8080, // default
		Topology:              "",
		TlsCertFilePath:       "cert1.pem",
		TlsKeyFilePath:        "key1.pem",
		BlobPlugin:            DefaultBlobPlugin,
		MetadataPlugin:        DefaultMetadataPlugin,
		RunMode:               RunModeServe,
		StartEra:              StartEraDefault,
		ImmutableDbPath:       "/tmp/immutable",
		ShutdownTimeout:       "45s",
		LedgerCatchupTimeout:  "90m",
		DatabaseWorkers:       11,
		DatabaseQueueSize:     77,
		BackfillBatchSize:     200,
		GenesisBootstrap: GenesisBootstrapConfig{
			Enabled:                     false,
			WindowSlots:                 4321,
			PromotionMinDiversityGroups: 4,
		},
		HistoryExpiry: DefaultHistoryExpiryConfig(),
		Midnight: MidnightConfig{
			Port:                        50052,
			Host:                        "127.0.0.1",
			CNightPolicyID:              "policy1",
			CNightAssetName:             "434e49474854",
			MappingValidatorAddress:     "addr_mapping",
			AuthTokenAssetName:          "auth",
			CommitteeCandidateAddress:   "addr_candidate",
			TechnicalCommitteeAddress:   "addr_technical",
			TechnicalCommitteePolicyID:  "policy_technical",
			CouncilAddress:              "addr_council",
			CouncilPolicyID:             "policy_council",
			PermissionedCandidatePolicy: "policy_permissioned",
		},
		ForgeSyncToleranceSlots:     321,
		ForgeStaleGapThresholdSlots: 654,
		Mithril: MithrilConfig{
			Enabled:                false,
			AggregatorURL:          "https://mithril.example.net",
			DownloadDir:            "/tmp/mithril",
			DownloadIdleTimeout:    "5m",
			DownloadMaxIdleRetries: 9,
			CleanupAfterLoad:       false,
			VerifyCertificates:     false,
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
	// LoadConfig only parses and merges; derived defaults (runMode,
	// mempool capacity, watermarks) are filled in afterwards
	cfg.ApplyDefaults()

	// Expected is the updated default values from globalConfig
	expected := &Config{
		MempoolCapacity:       1048576,
		MempoolImplementation: DefaultMempoolImplementation,
		EvictionWatermark:     0.90,
		RejectionWatermark:    0.95,
		BindAddr:              "0.0.0.0",
		CardanoConfig:         "", // Resolved by consumers using cfg.Network
		DatabasePath:          ".dingo",
		SocketPath:            "dingo.socket",
		IntersectTip:          false,
		ValidateHistorical:    false,
		Network:               "preview",
		MetricsPort:           12798,
		PrivateBindAddr:       "127.0.0.1",
		PrivatePort:           3002,
		RelayPort:             3001,
		UtxorpcPort:           9090,
		CORSAllowedOrigins:    []string{"*"},
		BlockfrostPort:        3000,
		MeshPort:              8080,
		Topology:              "",
		TlsCertFilePath:       "",
		TlsKeyFilePath:        "",
		BlobPlugin:            DefaultBlobPlugin,
		MetadataPlugin:        DefaultMetadataPlugin,
		RunMode:               RunModeServe,
		StartEra:              StartEraDefault,
		ImmutableDbPath:       "",
		ShutdownTimeout:       DefaultShutdownTimeout,
		LedgerCatchupTimeout:  DefaultLedgerCatchupTimeout,
		DatabaseWorkers:       5,
		DatabaseQueueSize:     50,
		BackfillBatchSize:     100,
		GenesisBootstrap:      DefaultGenesisBootstrapConfig(),
		HistoryExpiry:         DefaultHistoryExpiryConfig(),
		Midnight: func() MidnightConfig {
			m := midnightNetworkDefaults["preview"]
			m.Port = DefaultMidnightConfig().Port
			m.Host = DefaultMidnightConfig().Host
			return m
		}(),
		ForgeSyncToleranceSlots:     DefaultForgeSyncToleranceSlots,
		ForgeStaleGapThresholdSlots: DefaultForgeStaleGapThresholdSlots,
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

func TestLoad_GenesisBootstrapEnvVars(t *testing.T) {
	resetGlobalConfig()
	globalConfig.RunMode = RunModeDev

	t.Setenv("DINGO_GENESIS_BOOTSTRAP_ENABLED", "false")
	t.Setenv("DINGO_GENESIS_BOOTSTRAP_WINDOW_SLOTS", "1234")
	t.Setenv(
		"DINGO_GENESIS_BOOTSTRAP_PROMOTION_MIN_DIVERSITY_GROUPS",
		"6",
	)

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if cfg.GenesisBootstrap.Enabled {
		t.Fatal("expected GenesisBootstrap.Enabled to be false")
	}
	if cfg.GenesisBootstrap.WindowSlots != 1234 {
		t.Fatalf(
			"expected GenesisBootstrap.WindowSlots to be 1234, got %d",
			cfg.GenesisBootstrap.WindowSlots,
		)
	}
	if cfg.GenesisBootstrap.PromotionMinDiversityGroups != 6 {
		t.Fatalf(
			"expected GenesisBootstrap.PromotionMinDiversityGroups to be 6, got %d",
			cfg.GenesisBootstrap.PromotionMinDiversityGroups,
		)
	}
}

func TestLoad_HistoryExpiryConfig(t *testing.T) {
	resetGlobalConfig()
	globalConfig.RunMode = RunModeDev

	yamlContent := `
historyExpiry:
  enabled: true
  frequency: 15m
`
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "history-expiry.yaml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if !cfg.HistoryExpiry.Enabled {
		t.Fatal("expected HistoryExpiry.Enabled to be true")
	}
	if cfg.HistoryExpiry.Frequency != 15*time.Minute {
		t.Fatalf(
			"expected HistoryExpiry.Frequency to be 15m, got %s",
			cfg.HistoryExpiry.Frequency,
		)
	}
}

func TestLoad_ChainsyncStrategyConfig(t *testing.T) {
	resetGlobalConfig()
	globalConfig.RunMode = RunModeDev

	yamlContent := `
chainsync:
  strategy: parallel
`
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "chainsync-strategy.yaml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if cfg.Chainsync.Strategy != "parallel" {
		t.Fatalf(
			"expected Chainsync.Strategy to be parallel, got %q",
			cfg.Chainsync.Strategy,
		)
	}
}

func TestLoad_ChainsyncStrategyEnvVar(t *testing.T) {
	resetGlobalConfig()
	globalConfig.RunMode = RunModeDev

	t.Setenv("DINGO_CHAINSYNC_STRATEGY", "round-robin")

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if cfg.Chainsync.Strategy != "round-robin" {
		t.Fatalf(
			"expected Chainsync.Strategy to be round-robin, got %q",
			cfg.Chainsync.Strategy,
		)
	}
}

func TestLoad_HistoryExpiryEnvVars(t *testing.T) {
	resetGlobalConfig()
	globalConfig.RunMode = RunModeDev

	t.Setenv("DINGO_HISTORY_EXPIRY_ENABLED", "true")
	t.Setenv("DINGO_HISTORY_EXPIRY_FREQUENCY", "45m")

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if !cfg.HistoryExpiry.Enabled {
		t.Fatal("expected HistoryExpiry.Enabled to be true")
	}
	if cfg.HistoryExpiry.Frequency != 45*time.Minute {
		t.Fatalf(
			"expected HistoryExpiry.Frequency to be 45m, got %s",
			cfg.HistoryExpiry.Frequency,
		)
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
		{RunModeLeios, true},
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
		{RunModeLeios, false},
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

func TestStartEra_Validation(t *testing.T) {
	tests := []struct {
		era   StartEra
		valid bool
	}{
		{StartEraDefault, true},
		{StartEraDijkstra, true},
		{"invalid", false},
	}
	for _, tt := range tests {
		if got := tt.era.Valid(); got != tt.valid {
			t.Errorf(
				"StartEra(%q).Valid() = %v, want %v",
				tt.era,
				got,
				tt.valid,
			)
		}
	}
}

func TestLoad_WithStartEraConfig(t *testing.T) {
	resetGlobalConfig()

	yamlContent := `
startEra: "dijkstra"
network: "preview"
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-start-era.yaml")

	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if cfg.StartEra != StartEraDijkstra {
		t.Errorf("expected StartEra to be 'dijkstra', got: %v", cfg.StartEra)
	}
	if !cfg.StartEra.IsDijkstra() {
		t.Error("expected IsDijkstra() to return true for 'dijkstra'")
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

	// CardanoConfig is no longer set by LoadConfig; consumers resolve it
	if cfg.CardanoConfig != "" {
		t.Errorf(
			"expected CardanoConfig to be empty, got %q",
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

	// Topology is resolved separately from LoadConfig, once the merged
	// configuration is final (see cmd/dingo)
	if _, err := LoadTopologyConfig(); err != nil {
		t.Fatalf("failed to load topology: %v", err)
	}
	topologyConfig := GetTopologyConfig()
	if topologyConfig.PeerSnapshotFile != "peer-snapshot.json" {
		t.Fatalf(
			"expected embedded topology to reference peer-snapshot.json, got %q",
			topologyConfig.PeerSnapshotFile,
		)
	}
	if topologyConfig.PeerSnapshot == nil {
		t.Fatal("expected embedded topology to load peer snapshot")
	}
	if !topologyConfig.PeerSnapshot.HasRelays() {
		t.Fatal("expected embedded peer snapshot to contain relays")
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

	// CardanoConfig is no longer set by LoadConfig; consumers resolve it
	if cfg.CardanoConfig != "" {
		t.Errorf(
			"expected CardanoConfig to be empty, got %q",
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

	// CardanoConfig is no longer set by LoadConfig; consumers resolve it
	if cfg.CardanoConfig != "" {
		t.Errorf(
			"expected CardanoConfig to be empty, got %q",
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

// TestWatermarkDefaultingAndValidation covers the post-merge pipeline
// for the mempool watermarks: ApplyDefaults fills unset (zero) values
// and validate rejects out-of-range ones. LoadConfig itself no longer
// judges watermark values, so a CLI flag can still override a bad YAML
// value before validation.
func TestWatermarkDefaultingAndValidation(t *testing.T) {
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

			cfg, err := LoadConfig("")
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			cfg.ApplyDefaults()
			err = cfg.validate(cfg.RunMode, minUnprivilegedPort)
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

// Database plugin config must fail loudly when YAML values have the wrong shape.
// These cases guard against silently falling back to zero-value plugin config.
func TestLoad_DatabaseSectionInvalidPluginConfigTypes(t *testing.T) {
	tests := []struct {
		name       string
		yaml       string
		errContain string
	}{
		{
			name: "blob plugin selector is not string",
			yaml: `
database:
  blob:
    plugin: 123
`,
			errContain: "blob plugin name must be a string",
		},
		{
			name: "metadata plugin selector is not string",
			yaml: `
database:
  metadata:
    plugin: true
`,
			errContain: "metadata plugin name must be a string",
		},
		{
			name: "blob plugin config is not map",
			yaml: `
database:
  blob:
    plugin: "badger"
    badger: "/tmp/badger"
`,
			errContain: `blob plugin config "badger" must be a map`,
		},
		{
			name: "metadata plugin config is not map",
			yaml: `
database:
  metadata:
    plugin: "sqlite"
    sqlite: "/tmp/test.db"
`,
			errContain: `metadata plugin config "sqlite" must be a map`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetGlobalConfig()
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "test-dingo.yaml")
			if err := os.WriteFile(tmpFile, []byte(tt.yaml), 0644); err != nil {
				t.Fatalf("failed to write config file: %v", err)
			}

			_, err := LoadConfig(tmpFile)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.errContain)
			}
			if !strings.Contains(err.Error(), tt.errContain) {
				t.Fatalf(
					"error %q should contain %q",
					err.Error(),
					tt.errContain,
				)
			}
		})
	}
}

func TestNetworkNameValidation(t *testing.T) {
	validTests := []struct {
		name    string
		network string
	}{
		{
			name:    "preview",
			network: "preview",
		},
		{
			name:    "mainnet",
			network: "mainnet",
		},
		{
			name:    "preprod",
			network: "preprod",
		},
		{
			name:    "hyphenated name",
			network: "my-devnet",
		},
		{
			name:    "underscore name",
			network: "test_net",
		},
		{
			// An empty network must load: Validate() enforces that
			// network or networkMagic is set, so a networkMagic-only
			// configuration is legal at the LoadConfig layer.
			name:    "empty network for magic-only configs",
			network: "",
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

			// CardanoConfig is resolved by consumers, not LoadConfig
			if cfg.CardanoConfig != "" {
				t.Errorf(
					"CardanoConfig = %q, want empty",
					cfg.CardanoConfig,
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

			// LoadConfig only parses and merges; a CLI flag may still
			// replace the network name, so the traversal guard runs in
			// validate on the final value.
			cfg, err := LoadConfig("")
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			err = cfg.validate(cfg.RunMode, minUnprivilegedPort)
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

// TestLoadConfig_NetworkMagicOnly is a regression test for the
// networkMagic-without-network contract: a YAML config with an empty
// network and a custom networkMagic must survive both LoadConfig and
// validation, since Validate() accepts either network or networkMagic.
func TestLoadConfig_NetworkMagicOnly(t *testing.T) {
	resetGlobalConfig()
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-dingo.yaml")
	yamlContent := "network: \"\"\nnetworkMagic: 42\n"
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Network != "" {
		t.Errorf("Network = %q, want empty", cfg.Network)
	}
	if cfg.NetworkMagic != 42 {
		t.Errorf("NetworkMagic = %d, want 42", cfg.NetworkMagic)
	}
	if err := cfg.validate(cfg.RunMode, minUnprivilegedPort); err != nil {
		t.Errorf("validation rejected magic-only config: %v", err)
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

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.BlockfrostPort != 3000 {
		t.Errorf(
			"expected BlockfrostPort default to be 3000, got %d",
			cfg.BlockfrostPort,
		)
	}
	if cfg.UtxorpcPort != 9090 {
		t.Errorf(
			"expected UtxorpcPort default to be 9090, got %d",
			cfg.UtxorpcPort,
		)
	}
	if cfg.MeshPort != 8080 {
		t.Errorf(
			"expected MeshPort default to be 8080, got %d",
			cfg.MeshPort,
		)
	}
}

func TestLoad_MidnightConfig(t *testing.T) {
	resetGlobalConfig()
	yamlContent := `
midnight:
  port: 50060
  host: "127.0.0.2"
  cnightPolicyId: "cnight-policy"
  cnightAssetName: "434e49474854"
  mappingValidatorAddress: "addr_mapping"
  authTokenAssetName: "auth-token"
  committeeCandidateAddress: "addr_candidate"
  technicalCommitteeAddress: "addr_technical"
  technicalCommitteePolicyId: "technical-policy"
  councilAddress: "addr_council"
  councilPolicyId: "council-policy"
  permissionedCandidatePolicy: "permissioned-policy"
network: "preview"
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-midnight.yaml")

	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	expected := MidnightConfig{
		Port:                        50060,
		Host:                        "127.0.0.2",
		CNightPolicyID:              "cnight-policy",
		CNightAssetName:             "434e49474854",
		MappingValidatorAddress:     "addr_mapping",
		AuthTokenAssetName:          "auth-token",
		CommitteeCandidateAddress:   "addr_candidate",
		TechnicalCommitteeAddress:   "addr_technical",
		TechnicalCommitteePolicyID:  "technical-policy",
		CouncilAddress:              "addr_council",
		CouncilPolicyID:             "council-policy",
		PermissionedCandidatePolicy: "permissioned-policy",
	}
	if cfg.Midnight != expected {
		t.Fatalf(
			"expected Midnight config %+v, got %+v",
			expected,
			cfg.Midnight,
		)
	}
}

func TestLoad_MidnightEnvOverridesYAML(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("DINGO_MIDNIGHT_PORT", "50070")
	t.Setenv("DINGO_MIDNIGHT_HOST", "127.0.0.3")
	yamlContent := `
midnight:
  port: 50060
  host: "127.0.0.2"
network: "preview"
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-midnight-env.yaml")

	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.Midnight.Port != 50070 {
		t.Fatalf("expected env midnight port 50070, got %d", cfg.Midnight.Port)
	}
	if cfg.Midnight.Host != "127.0.0.3" {
		t.Fatalf("expected env midnight host 127.0.0.3, got %q", cfg.Midnight.Host)
	}
}

func TestLoad_MidnightAddressAndPolicyFieldsAreYAMLOnly(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("DINGO_MIDNIGHT_CNIGHT_POLICY_ID", "env-policy")
	t.Setenv("DINGO_MIDNIGHT_MAPPING_VALIDATOR_ADDRESS", "env-address")
	yamlContent := `
midnight:
  cnightPolicyId: "yaml-policy"
  mappingValidatorAddress: "yaml-address"
network: "preprod"
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-midnight-yaml-only.yaml")

	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if cfg.Midnight.CNightPolicyID != "yaml-policy" {
		t.Fatalf(
			"expected YAML cnight policy to win, got %q",
			cfg.Midnight.CNightPolicyID,
		)
	}
	if cfg.Midnight.MappingValidatorAddress != "yaml-address" {
		t.Fatalf(
			"expected YAML mapping validator address to win, got %q",
			cfg.Midnight.MappingValidatorAddress,
		)
	}
}

func TestLoad_MidnightNetworkDefaults(t *testing.T) {
	tests := []struct {
		network  string
		expected MidnightConfig
	}{
		{
			network:  "mainnet",
			expected: midnightNetworkDefaults["mainnet"],
		},
		{
			network:  "preview",
			expected: midnightNetworkDefaults["preview"],
		},
	}
	for _, tc := range tests {
		t.Run(tc.network, func(t *testing.T) {
			resetGlobalConfig()
			yamlContent := "network: \"" + tc.network + "\"\n"
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "dingo.yaml")
			if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
				t.Fatalf("write config: %v", err)
			}
			cfg, err := LoadConfig(tmpFile)
			if err != nil {
				t.Fatalf("load config: %v", err)
			}
			got := cfg.Midnight
			// Port and Host come from DefaultMidnightConfig, not network defaults.
			got.Port = 0
			got.Host = ""
			want := tc.expected
			want.Port = 0
			want.Host = ""
			if got != want {
				t.Fatalf("network %q: expected %+v, got %+v", tc.network, want, got)
			}
		})
	}
}

func TestLoad_MidnightExplicitOverridesNetworkDefault(t *testing.T) {
	resetGlobalConfig()
	yamlContent := `
network: "preview"
midnight:
  cnightPolicyId: "explicit-override"
`
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "dingo.yaml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("write config: %v", err)
	}
	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Midnight.CNightPolicyID != "explicit-override" {
		t.Fatalf("expected explicit override, got %q", cfg.Midnight.CNightPolicyID)
	}
	// Other fields should still get the network default.
	if cfg.Midnight.CouncilPolicyID != midnightNetworkDefaults["preview"].CouncilPolicyID {
		t.Fatalf(
			"expected network default for CouncilPolicyID, got %q",
			cfg.Midnight.CouncilPolicyID,
		)
	}
}

func TestLoad_OffchainMetadataConfig(t *testing.T) {
	resetGlobalConfig()
	yamlContent := `
offchainMetadata:
  interval: 2m
  requestTimeout: 10s
  userAgent: "dingo-test/1"
  ipfsGatewayUrl: "https://ipfs.example.test/ipfs/"
  batchSize: 7
  maxBytes: 65536
  allowPrivateAddresses: true
network: "preview"
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-offchain-metadata.yaml")

	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	expected := OffchainMetadataConfig{
		Interval:              2 * time.Minute,
		RequestTimeout:        10 * time.Second,
		UserAgent:             "dingo-test/1",
		IPFSGatewayURL:        "https://ipfs.example.test/ipfs/",
		BatchSize:             7,
		MaxBytes:              65536,
		AllowPrivateAddresses: true,
	}
	if cfg.OffchainMetadata != expected {
		t.Fatalf(
			"expected off-chain metadata config %+v, got %+v",
			expected,
			cfg.OffchainMetadata,
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

// TestLoad_MempoolCapacityMode covers MempoolCapacity defaulting based
// on RunMode and the priority of an explicit YAML override.
func TestLoad_MempoolCapacityMode(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		expected int64
	}{
		{
			name:     "praos serve default",
			yaml:     "runMode: \"serve\"\n",
			expected: DefaultMempoolCapacityPraos,
		},
		{
			name:     "leios default raises to 25 MiB",
			yaml:     "runMode: \"leios\"\n",
			expected: DefaultMempoolCapacityLeios,
		},
		{
			name:     "explicit value wins under leios",
			yaml:     "runMode: \"leios\"\nmempoolCapacity: 5242880\n",
			expected: 5242880,
		},
		{
			name:     "explicit value wins under serve",
			yaml:     "runMode: \"serve\"\nmempoolCapacity: 5242880\n",
			expected: 5242880,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resetGlobalConfig()
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "test-mempool-mode.yaml")
			if err := os.WriteFile(tmpFile, []byte(tc.yaml), 0644); err != nil {
				t.Fatalf("write config: %v", err)
			}
			cfg, err := LoadConfig(tmpFile)
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			cfg.ApplyDefaults()
			if cfg.MempoolCapacity != tc.expected {
				t.Errorf(
					"MempoolCapacity: got %d, want %d",
					cfg.MempoolCapacity, tc.expected,
				)
			}
		})
	}
}

func TestLoad_WithLeiosVotingConfig(t *testing.T) {
	resetGlobalConfig()

	yamlContent := `
runMode: "leios"
network: "preview"
leiosVoteSigningKeyFile: "/keys/leios-vote.skey"
leiosVoterPublicKeys:
  "aabbcc": "ddeeff"
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-leios-voting.yaml")

	err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if cfg.LeiosVoteSigningKeyFile != "/keys/leios-vote.skey" {
		t.Errorf(
			"expected LeiosVoteSigningKeyFile to be '/keys/leios-vote.skey', got: %q",
			cfg.LeiosVoteSigningKeyFile,
		)
	}
	if cfg.LeiosVoterPublicKeys["aabbcc"] != "ddeeff" {
		t.Errorf(
			"expected LeiosVoterPublicKeys['aabbcc'] to be 'ddeeff', got: %v",
			cfg.LeiosVoterPublicKeys,
		)
	}
}

func TestLoad_LeiosVotingEnvVars(t *testing.T) {
	resetGlobalConfig()

	t.Setenv("DINGO_LEIOS_VOTE_SIGNING_KEY_FILE", "/env/leios-vote.skey")
	t.Setenv("DINGO_LEIOS_VOTER_PUBLIC_KEYS", "aabbcc:ddeeff")

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if cfg.LeiosVoteSigningKeyFile != "/env/leios-vote.skey" {
		t.Errorf(
			"expected LeiosVoteSigningKeyFile to be '/env/leios-vote.skey', got: %q",
			cfg.LeiosVoteSigningKeyFile,
		)
	}
	if cfg.LeiosVoterPublicKeys["aabbcc"] != "ddeeff" {
		t.Errorf(
			"expected LeiosVoterPublicKeys['aabbcc'] to be 'ddeeff', got: %v",
			cfg.LeiosVoterPublicKeys,
		)
	}
}
