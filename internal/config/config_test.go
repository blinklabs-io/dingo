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
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	hostplugin "github.com/blinklabs-io/dingo/plugin"
	"github.com/blinklabs-io/dingo/topology"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func resetGlobalConfig() {
	midnightYAMLFields = nil
	globalConfig = &Config{
		Plugins:                           defaultPluginsConfig(),
		BindAddr:                          "0.0.0.0",
		CardanoConfig:                     "", // Will be set dynamically based on network
		DatabasePath:                      ".dingo",
		SocketPath:                        "dingo.socket",
		IntersectTip:                      false,
		ValidateHistorical:                true,
		StrictUtxoValidation:              true,
		Network:                           "preview",
		MetricsPort:                       12798,
		DebugBindAddr:                     DefaultDebugBindAddr,
		PrivateBindAddr:                   "127.0.0.1",
		PrivatePort:                       3002,
		RelayPort:                         3001,
		CORSAllowedOrigins:                []string{"*"},
		Topology:                          "",
		TlsCertFilePath:                   "",
		TlsKeyFilePath:                    "",
		RunMode:                           RunModeServe,
		StartEra:                          StartEraDefault,
		ImmutableDbPath:                   "",
		ShutdownTimeout:                   DefaultShutdownTimeout,
		LedgerCatchupTimeout:              DefaultLedgerCatchupTimeout,
		DatabaseWorkers:                   5,
		DatabaseQueueSize:                 50,
		BackfillBatchSize:                 100,
		GenesisBootstrap:                  DefaultGenesisBootstrapConfig(),
		HistoryExpiry:                     DefaultHistoryExpiryConfig(),
		KoiosParity:                       DefaultKoiosParityConfig(),
		Midnight:                          DefaultMidnightConfig(),
		ForgeSyncToleranceSlots:           DefaultForgeSyncToleranceSlots,
		ForgeStaleGapThresholdSlots:       DefaultForgeStaleGapThresholdSlots,
		ForgeHeaderFrontierToleranceSlots: DefaultForgeHeaderFrontierToleranceSlots,
		Mithril: MithrilConfig{
			Enabled:            true,
			CleanupAfterLoad:   true,
			VerifyCertificates: true,
		},
	}
	globalTopologyConfig = &topology.TopologyConfig{}
}

func unsetDebugBindAddrEnv(t *testing.T) {
	t.Helper()
	// Preserve the caller's environment while ensuring config tests that
	// exercise defaults or YAML precedence do not inherit this override.
	t.Setenv("DINGO_DEBUG_BIND_ADDR", "")
	require.NoError(t, os.Unsetenv("DINGO_DEBUG_BIND_ADDR"))
}

// unsetForgeGateEnv clears the forge-gate overrides so tests that assert the
// built-in defaults cannot inherit a value from the caller's environment.
// LoadConfig runs envconfig AFTER the YAML merge, so an exported
// DINGO_FORGE_* variable silently overrides both the fixture and the default,
// and the assertion then fails for a reason unrelated to the code under test.
func unsetForgeGateEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"DINGO_FORGE_HEADER_FRONTIER_TOLERANCE_SLOTS",
	} {
		// t.Setenv registers the restore; Unsetenv then removes it for the
		// duration of the test, which is what envconfig must not see.
		t.Setenv(k, "")
		require.NoError(t, os.Unsetenv(k))
	}
}

func TestLoad_CompareFullStruct(t *testing.T) {
	resetGlobalConfig()
	unsetDebugBindAddrEnv(t)
	unsetForgeGateEnv(t)
	yamlContent := `
plugins:
  mempool:
    provider: fifo
    config:
      capacity: 2097152
      evictionWatermark: 0.90
      rejectionWatermark: 0.95
  api:
    utxorpc:
      provider: builtin
      config:
        port: 9940
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

	expectedPlugins := defaultPluginsConfig()
	expectedPlugins.Mempool.Config["capacity"] = 2097152
	expectedPlugins.Mempool.Config["evictionWatermark"] = 0.90
	expectedPlugins.Mempool.Config["rejectionWatermark"] = 0.95
	expectedPlugins.API.Utxorpc.Config["port"] = 9940
	expected := &Config{
		Plugins:              expectedPlugins,
		BindAddr:             "127.0.0.1",
		CardanoConfig:        "./cardano/preview/config.json",
		DatabasePath:         ".dingo",
		SocketPath:           "env.socket",
		IntersectTip:         true,
		ValidateHistorical:   true,
		StrictUtxoValidation: true,
		Network:              "preview",
		MetricsPort:          8088,
		DebugBindAddr:        DefaultDebugBindAddr,
		PrivateBindAddr:      "127.0.0.1",
		PrivatePort:          8000,
		RelayPort:            4000,
		CORSAllowedOrigins:   []string{"*"},
		Topology:             "",
		TlsCertFilePath:      "cert1.pem",
		TlsKeyFilePath:       "key1.pem",
		RunMode:              RunModeServe,
		StartEra:             StartEraDefault,
		ImmutableDbPath:      "/tmp/immutable",
		ShutdownTimeout:      "45s",
		LedgerCatchupTimeout: "90m",
		DatabaseWorkers:      11,
		DatabaseQueueSize:    77,
		BackfillBatchSize:    200,
		GenesisBootstrap: GenesisBootstrapConfig{
			Enabled:                     false,
			WindowSlots:                 4321,
			PromotionMinDiversityGroups: 4,
		},
		HistoryExpiry: DefaultHistoryExpiryConfig(),
		KoiosParity:   DefaultKoiosParityConfig(),
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
		// Not set by the fixture's YAML/env, so ApplyDefaults fills it.
		ForgeHeaderFrontierToleranceSlots: DefaultForgeHeaderFrontierToleranceSlots,
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

func TestDefaultMempoolProviderIsFIFO(t *testing.T) {
	if got := defaultPluginsConfig().Mempool.Provider; got != "fifo" {
		t.Fatalf("default mempool provider = %q, want fifo", got)
	}
}

func TestLoad_DAGMempoolProvider(t *testing.T) {
	resetGlobalConfig()
	tmpFile := filepath.Join(t.TempDir(), "dag-mempool.yaml")
	if err := os.WriteFile(
		tmpFile,
		[]byte("plugins:\n  mempool:\n    provider: dag\n"),
		0644,
	); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Plugins.Mempool.Provider; got != "dag" {
		t.Fatalf("mempool provider = %q, want dag", got)
	}
}

func TestLoad_WithoutConfigFile_UsesDefaults(t *testing.T) {
	resetGlobalConfig()
	unsetDebugBindAddrEnv(t)
	unsetForgeGateEnv(t)

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
		Plugins: func() PluginsConfig {
			plugins := defaultPluginsConfig()
			plugins.Mempool.Config["capacity"] = int64(1048576)
			return plugins
		}(),
		BindAddr:             "0.0.0.0",
		CardanoConfig:        "", // Resolved by consumers using cfg.Network
		DatabasePath:         ".dingo",
		SocketPath:           "dingo.socket",
		IntersectTip:         false,
		ValidateHistorical:   true,
		StrictUtxoValidation: true,
		Network:              "preview",
		MetricsPort:          12798,
		DebugBindAddr:        DefaultDebugBindAddr,
		PrivateBindAddr:      "127.0.0.1",
		PrivatePort:          3002,
		RelayPort:            3001,
		CORSAllowedOrigins:   []string{"*"},
		Topology:             "",
		TlsCertFilePath:      "",
		TlsKeyFilePath:       "",
		RunMode:              RunModeServe,
		StartEra:             StartEraDefault,
		ImmutableDbPath:      "",
		ShutdownTimeout:      DefaultShutdownTimeout,
		LedgerCatchupTimeout: DefaultLedgerCatchupTimeout,
		DatabaseWorkers:      5,
		DatabaseQueueSize:    50,
		BackfillBatchSize:    100,
		GenesisBootstrap:     DefaultGenesisBootstrapConfig(),
		HistoryExpiry:        DefaultHistoryExpiryConfig(),
		KoiosParity:          DefaultKoiosParityConfig(),
		Midnight: func() MidnightConfig {
			m := midnightNetworkDefaults["preview"]
			m.Port = DefaultMidnightConfig().Port
			m.Host = DefaultMidnightConfig().Host
			return m
		}(),
		ForgeSyncToleranceSlots:           DefaultForgeSyncToleranceSlots,
		ForgeStaleGapThresholdSlots:       DefaultForgeStaleGapThresholdSlots,
		ForgeHeaderFrontierToleranceSlots: DefaultForgeHeaderFrontierToleranceSlots,
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
	t.Setenv("DINGO_GENESIS_BOOTSTRAP_CORROBORATION_PEERS", "3")

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
	if cfg.GenesisBootstrap.CorroborationPeers != 3 {
		t.Fatalf(
			"expected GenesisBootstrap.CorroborationPeers to be 3, got %d",
			cfg.GenesisBootstrap.CorroborationPeers,
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
// for the mempool watermarks: ApplyDefaults fills unset values while
// preserving an explicit zero eviction watermark, and validate rejects
// out-of-range values. LoadConfig itself no longer
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
			name:      "defaults rejection when both zero",
			eviction:  0,
			rejection: 0,
			wantErr:   false,
		},
		{
			name:      "preserves disabled eviction with explicit rejection",
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
			eviction:  0.5,
			rejection: 1.0,
			wantErr:   false,
		},
		{
			name:      "eviction disabled",
			eviction:  0.0,
			rejection: 1.0,
			wantErr:   false,
		},
		{
			name:       "eviction negative",
			eviction:   -0.1,
			rejection:  0.95,
			wantErr:    true,
			errContain: "invalid plugins.mempool.config.evictionWatermark",
		},
		{
			name:       "rejection negative",
			eviction:   0.0,
			rejection:  -0.5,
			wantErr:    true,
			errContain: "invalid plugins.mempool.config.rejectionWatermark",
		},
		{
			name:       "eviction above 1",
			eviction:   1.5,
			rejection:  0.95,
			wantErr:    true,
			errContain: "invalid plugins.mempool.config.evictionWatermark",
		},
		{
			name:       "rejection above 1",
			eviction:   0.90,
			rejection:  1.1,
			wantErr:    true,
			errContain: "invalid plugins.mempool.config.rejectionWatermark",
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
			errContain: "invalid plugins.mempool.config.evictionWatermark",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetGlobalConfig()
			globalConfig.Plugins.Mempool.Config["evictionWatermark"] = tt.eviction
			globalConfig.Plugins.Mempool.Config["rejectionWatermark"] = tt.rejection
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

func TestApplyDefaultsPreservesExplicitEvictionDisable(t *testing.T) {
	resetGlobalConfig()
	globalConfig.Plugins.Mempool.Config["evictionWatermark"] = 0.0
	globalConfig.Plugins.Mempool.Config["rejectionWatermark"] = 1.0

	cfg, err := LoadConfig("")
	require.NoError(t, err)
	cfg.ApplyDefaults()
	_, evictionWatermark, rejectionWatermark := cfg.MempoolSettings()
	assert.Zero(t, evictionWatermark)
	assert.Equal(t, 1.0, rejectionWatermark)
	require.NoError(t, cfg.validate(cfg.RunMode, minUnprivilegedPort))
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

	_, err = LoadConfig(tmpFile)
	if err == nil ||
		!strings.Contains(err.Error(), "field database not found") {
		t.Fatalf("expected legacy database section rejection, got %v", err)
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
			errContain: "field database not found",
		},
		{
			name: "metadata plugin selector is not string",
			yaml: `
database:
  metadata:
    plugin: true
`,
			errContain: "field database not found",
		},
		{
			name: "blob plugin config is not map",
			yaml: `
database:
  blob:
    plugin: "badger"
    badger: "/tmp/badger"
`,
			errContain: "field database not found",
		},
		{
			name: "metadata plugin config is not map",
			yaml: `
database:
  metadata:
    plugin: "sqlite"
    sqlite: "/tmp/test.db"
`,
			errContain: "field database not found",
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
plugins:
  api:
    blockfrost:
      provider: builtin
      config:
        port: 8080
    utxorpc:
      provider: builtin
      config:
        port: 9090
    mesh:
      provider: builtin
      config:
        port: 8081
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

	if port := APIPluginPort(cfg.Plugins.API.Blockfrost); port != 8080 {
		t.Errorf(
			"expected Blockfrost port to be 8080, got %d",
			port,
		)
	}
	if port := APIPluginPort(cfg.Plugins.API.Utxorpc); port != 9090 {
		t.Errorf(
			"expected Utxorpc port to be 9090, got %d",
			port,
		)
	}
	if port := APIPluginPort(cfg.Plugins.API.Mesh); port != 8081 {
		t.Errorf(
			"expected Mesh port to be 8081, got %d",
			port,
		)
	}
}

func TestLoad_APIPortCompatibilityEnvironment(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DINGO_BLOCKFROST_PORT", "3100")
	t.Setenv("DINGO_MESH_PORT", "8181")
	t.Setenv("DINGO_UTXORPC_PORT", "9191")

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	if err := os.WriteFile(configFile, nil, 0o600); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	cfg, err := LoadConfig(configFile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if port := APIPluginPort(cfg.Plugins.API.Blockfrost); port != 3100 {
		t.Fatalf("Blockfrost port = %d, want 3100", port)
	}
	if port := APIPluginPort(cfg.Plugins.API.Mesh); port != 8181 {
		t.Fatalf("Mesh port = %d, want 8181", port)
	}
	if port := APIPluginPort(cfg.Plugins.API.Utxorpc); port != 9191 {
		t.Fatalf("Utxorpc port = %d, want 9191", port)
	}
}

func TestLoad_CanonicalAPIPortEnvironmentOverridesCompatibility(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DINGO_UTXORPC_PORT", "9191")
	t.Setenv("DINGO_PLUGINS_API_UTXORPC_CONFIG_PORT", "9292")

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	if err := os.WriteFile(configFile, nil, 0o600); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	cfg, err := LoadConfig(configFile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	if port := APIPluginPort(cfg.Plugins.API.Utxorpc); port != 9292 {
		t.Fatalf("Utxorpc port = %d, want canonical value 9292", port)
	}
}

func TestLoad_InvalidAPIPortCompatibilityEnvironment(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DINGO_UTXORPC_PORT", "not-a-port")

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	if err := os.WriteFile(configFile, nil, 0o600); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	_, err := LoadConfig(configFile)
	if err == nil || !strings.Contains(err.Error(), "DINGO_UTXORPC_PORT") {
		t.Fatalf("expected invalid compatibility port error, got %v", err)
	}
}

func TestLoad_APIPortsDefault(t *testing.T) {
	resetGlobalConfig()

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if port := APIPluginPort(cfg.Plugins.API.Blockfrost); port != 3000 {
		t.Errorf(
			"expected BlockfrostPort default to be 3000, got %d",
			port,
		)
	}
	if port := APIPluginPort(cfg.Plugins.API.Utxorpc); port != 9090 {
		t.Errorf(
			"expected UtxorpcPort default to be 9090, got %d",
			port,
		)
	}
	if port := APIPluginPort(cfg.Plugins.API.Mesh); port != 8080 {
		t.Errorf(
			"expected MeshPort default to be 8080, got %d",
			port,
		)
	}
}

func TestLoad_MidnightConfig(t *testing.T) {
	resetGlobalConfig()
	yamlContent := `
midnight:
  enabled: true
  serverEnabled: true
  reflectionEnabled: true
  allowInsecureRemote: true
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
		Enabled:                     true,
		ServerEnabled:               true,
		ReflectionEnabled:           true,
		AllowInsecureRemote:         true,
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
	t.Setenv("DINGO_MIDNIGHT_SERVER_ENABLED", "true")
	t.Setenv("DINGO_MIDNIGHT_REFLECTION_ENABLED", "true")
	t.Setenv("DINGO_MIDNIGHT_ALLOW_INSECURE_REMOTE", "true")
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
	if !cfg.Midnight.ServerEnabled || !cfg.Midnight.ReflectionEnabled ||
		!cfg.Midnight.AllowInsecureRemote {
		t.Fatalf(
			"expected environment to enable Midnight server policy: %+v",
			cfg.Midnight,
		)
	}
	if cfg.Midnight.Host != "127.0.0.3" {
		t.Fatalf(
			"expected env midnight host 127.0.0.3, got %q",
			cfg.Midnight.Host,
		)
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
				t.Fatalf(
					"network %q: expected %+v, got %+v",
					tc.network,
					want,
					got,
				)
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
		t.Fatalf(
			"expected explicit override, got %q",
			cfg.Midnight.CNightPolicyID,
		)
	}
	// Other fields should still get the network default.
	if cfg.Midnight.CouncilPolicyID != midnightNetworkDefaults["preview"].CouncilPolicyID {
		t.Fatalf(
			"expected network default for CouncilPolicyID, got %q",
			cfg.Midnight.CouncilPolicyID,
		)
	}
}

// TestReapplyMidnightNetworkDefaults_ResumedNetwork exercises the exported
// wrapper settingsresolve.Apply calls directly: a caller that changes
// cfg.Network after LoadConfig/ApplyFlags have already derived Midnight
// defaults for the old network must see every network-derived constant
// move to the new network once ReapplyMidnightNetworkDefaults runs.
func TestReapplyMidnightNetworkDefaults_ResumedNetwork(t *testing.T) {
	resetGlobalConfig()
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Network != "preview" {
		t.Fatalf("expected initial network preview, got %q", cfg.Network)
	}
	if cfg.Midnight.CNightPolicyID != midnightNetworkDefaults["preview"].CNightPolicyID {
		t.Fatalf(
			"expected preview Midnight default before resume, got %q",
			cfg.Midnight.CNightPolicyID,
		)
	}
	// preview sets CommitteeCandidateAddress but mainnet does not, so this
	// also proves the stale preview value is cleared, not just left
	// alongside a filled-in mainnet value.
	if cfg.Midnight.CommitteeCandidateAddress == "" {
		t.Fatal(
			"expected preview Midnight default to set CommitteeCandidateAddress",
		)
	}

	previousNetwork := cfg.Network
	cfg.Network = "mainnet" // simulates a caller resuming Network late
	ReapplyMidnightNetworkDefaults(cfg, previousNetwork)

	if cfg.Midnight.CNightPolicyID != midnightNetworkDefaults["mainnet"].CNightPolicyID {
		t.Fatalf(
			"expected mainnet Midnight policy default after resume, got %q",
			cfg.Midnight.CNightPolicyID,
		)
	}
	if cfg.Midnight.CouncilPolicyID != midnightNetworkDefaults["mainnet"].CouncilPolicyID {
		t.Fatalf(
			"expected mainnet Midnight council policy default after resume, got %q",
			cfg.Midnight.CouncilPolicyID,
		)
	}
	if cfg.Midnight.CommitteeCandidateAddress != "" {
		t.Fatalf(
			"expected stale preview CommitteeCandidateAddress to be cleared, got %q",
			cfg.Midnight.CommitteeCandidateAddress,
		)
	}
}

// TestReapplyMidnightNetworkDefaults_PreservesExplicitYAML mirrors
// TestApplyFlags_NetworkOverridePreservesExplicitMidnightYAML, but exercises
// ReapplyMidnightNetworkDefaults directly rather than through ApplyFlags:
// an operator-set Midnight field must survive a resumed network change even
// when its value happens to equal the previous network's default (the
// coincidental case clearMidnightNetworkDefaults must not clear).
func TestReapplyMidnightNetworkDefaults_PreservesExplicitYAML(t *testing.T) {
	resetGlobalConfig()
	previewPolicy := midnightNetworkDefaults["preview"].CNightPolicyID
	yamlContent := `
network: "preview"
midnight:
  cnightPolicyId: "` + previewPolicy + `"
`
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "dingo.yaml")
	if err := os.WriteFile(configFile, []byte(yamlContent), 0o600); err != nil {
		t.Fatalf("failed to write temp config file: %v", err)
	}
	cfg, err := LoadConfig(configFile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	previousNetwork := cfg.Network
	cfg.Network = "mainnet" // simulates a caller resuming Network late
	ReapplyMidnightNetworkDefaults(cfg, previousNetwork)

	if cfg.Midnight.CNightPolicyID != previewPolicy {
		t.Fatalf(
			"expected explicit Midnight YAML policy to be preserved, got %q",
			cfg.Midnight.CNightPolicyID,
		)
	}
	if cfg.Midnight.CouncilPolicyID != midnightNetworkDefaults["mainnet"].CouncilPolicyID {
		t.Fatalf(
			"expected remaining Midnight defaults to switch to mainnet, got %q",
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
		t.Errorf(
			"expected StorageMode default to be empty, got %q",
			cfg.StorageMode,
		)
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
			yaml:     "runMode: \"leios\"\nplugins:\n  mempool:\n    provider: default\n    config:\n      capacity: 5242880\n      evictionWatermark: 0.90\n      rejectionWatermark: 0.95\n",
			expected: 5242880,
		},
		{
			name:     "explicit value wins under serve",
			yaml:     "runMode: \"serve\"\nplugins:\n  mempool:\n    provider: default\n    config:\n      capacity: 5242880\n      evictionWatermark: 0.90\n      rejectionWatermark: 0.95\n",
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
			capacity, _, _ := cfg.MempoolSettings()
			if capacity != tc.expected {
				t.Errorf(
					"MempoolCapacity: got %d, want %d",
					capacity, tc.expected,
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
}

func TestLoad_LeiosVotingEnvVars(t *testing.T) {
	resetGlobalConfig()

	t.Setenv("DINGO_LEIOS_VOTE_SIGNING_KEY_FILE", "/env/leios-vote.skey")

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
}

func TestLoad_RejectsRetiredLeiosVoterPublicKeysYAML(t *testing.T) {
	resetGlobalConfig()
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "retired-leios-voter-keys.yaml")
	require.NoError(t, os.WriteFile(tmpFile, []byte(`
runMode: "leios"
leiosVoterPublicKeys:
  "aabbcc": "ddeeff"
`), 0o600))

	_, err := LoadConfig(tmpFile)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field leiosVoterPublicKeys not found")
}

func TestLoad_RejectsRetiredLeiosVoterPublicKeysEnv(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("DINGO_LEIOS_VOTER_PUBLIC_KEYS", "aabbcc:ddeeff")

	_, err := LoadConfig("")
	require.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"DINGO_LEIOS_VOTER_PUBLIC_KEYS is no longer supported",
	)
}

// GetConfig hands out snapshots, so nested plugin config values must be
// duplicated. Copying only the top level would let a caller mutate a nested
// mapping or sequence that globalConfig and every other snapshot still share.
func TestClonePluginSelectionDeepCopiesNestedValues(t *testing.T) {
	nestedMap := map[string]any{"inner": "original"}
	nestedSlice := []any{"first"}
	deeper := map[string]any{"list": []any{map[string]any{"k": "v"}}}
	selection := hostplugin.Selection{
		Provider: "builtin",
		Config: map[string]any{
			"scalar": 1,
			"map":    nestedMap,
			"slice":  nestedSlice,
			"deep":   deeper,
		},
	}

	clone := clonePluginSelection(selection)

	// Mutating the clone must not reach the original.
	clone.Config["map"].(map[string]any)["inner"] = "mutated"
	clone.Config["slice"].([]any)[0] = "mutated"
	clone.Config["deep"].(map[string]any)["list"].([]any)[0].(map[string]any)["k"] = "mutated"

	assert.Equal(t, "original", nestedMap["inner"],
		"nested map must not be shared with the clone")
	assert.Equal(t, "first", nestedSlice[0],
		"nested slice must not be shared with the clone")
	assert.Equal(
		t,
		"v",
		deeper["list"].([]any)[0].(map[string]any)["k"],
		"deeply nested values must not be shared with the clone",
	)
	assert.Equal(t, 1, clone.Config["scalar"])
}

// A yaml.v2-style map[any]any value is deep-copied as well.
func TestDeepCopyPluginValueHandlesInterfaceKeyedMaps(t *testing.T) {
	original := map[any]any{"key": map[any]any{"inner": "original"}}

	clone, ok := deepCopyPluginValue(original).(map[any]any)
	require.True(t, ok)
	inner, ok := clone["key"].(map[any]any)
	require.True(t, ok)
	inner["inner"] = "mutated"

	assert.Equal(
		t,
		"original",
		original["key"].(map[any]any)["inner"],
	)
}

// GetConfig must not return a snapshot that shares nested plugin config values
// with globalConfig.
func TestGetConfigSnapshotDoesNotShareNestedPluginConfig(t *testing.T) {
	configMu.Lock()
	prev := globalConfig
	globalConfig = cloneConfig(prev)
	globalConfig.Plugins.Mempool.Config = map[string]any{
		"nested": map[string]any{"inner": "original"},
	}
	configMu.Unlock()
	t.Cleanup(func() {
		configMu.Lock()
		globalConfig = prev
		configMu.Unlock()
	})

	snapshot := GetConfig()
	require.NotNil(t, snapshot)
	nested, ok := snapshot.Plugins.Mempool.Config["nested"].(map[string]any)
	require.True(t, ok)
	nested["inner"] = "mutated"

	configMu.RLock()
	defer configMu.RUnlock()
	assert.Equal(
		t,
		"original",
		globalConfig.Plugins.Mempool.Config["nested"].(map[string]any)["inner"],
		"mutating a snapshot must not reach globalConfig",
	)
}

// exampleConfigPath returns the path to the repo's bundled
// dingo.yaml.example, independent of the working directory the test binary
// runs from.
func exampleConfigPath() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(
		filepath.Dir(thisFile),
		"..",
		"..",
		"dingo.yaml.example",
	)
}

// TestLoad_ExampleConfigParses guards against regressions like #3169, where
// a single mis-indented line in dingo.yaml.example (the default config
// shipped to operators) produced a YAML syntax error on startup with no
// indication of which field was affected. Any change to dingo.yaml.example
// must keep it loadable as-is.
func TestLoad_ExampleConfigParses(t *testing.T) {
	resetGlobalConfig()

	path := exampleConfigPath()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("dingo.yaml.example not found at %s: %v", path, err)
	}

	cfg, err := LoadConfig(path)
	require.NoError(
		t,
		err,
		"dingo.yaml.example must parse cleanly as shipped",
	)
	require.NotNil(t, cfg)
}

// TestLoad_ParseErrorIncludesConfigFilePath guards the error-message
// behavior introduced alongside TestLoad_ExampleConfigParses: every parse
// failure path in LoadConfig must name the resolved config file so a
// regression that drops configFile from one of the error wraps is caught.
func TestLoad_ParseErrorIncludesConfigFilePath(t *testing.T) {
	t.Run("invalid YAML syntax", func(t *testing.T) {
		resetGlobalConfig()

		tmpDir := t.TempDir()
		tmpFile := filepath.Join(tmpDir, "bad-syntax.yaml")
		yamlContent := "network: mainnet\n  badIndent: true\n"

		err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
		if err != nil {
			t.Fatalf("failed to write config file: %v", err)
		}

		_, err = LoadConfig(tmpFile)
		require.Error(t, err)
		assert.Contains(t, err.Error(), tmpFile)
	})

	t.Run("wrapped config section decode error", func(t *testing.T) {
		resetGlobalConfig()

		tmpDir := t.TempDir()
		tmpFile := filepath.Join(tmpDir, "bad-wrapped-decode.yaml")
		yamlContent := `
config:
  network: mainnet
  unknownField: true
`

		err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
		if err != nil {
			t.Fatalf("failed to write config file: %v", err)
		}

		_, err = LoadConfig(tmpFile)
		require.Error(t, err)
		assert.Contains(t, err.Error(), tmpFile)
	})

	t.Run("wrapped config section is nil", func(t *testing.T) {
		resetGlobalConfig()

		tmpDir := t.TempDir()
		tmpFile := filepath.Join(tmpDir, "nil-config-section.yaml")
		yamlContent := "config:\n"

		err := os.WriteFile(tmpFile, []byte(yamlContent), 0644)
		if err != nil {
			t.Fatalf("failed to write config file: %v", err)
		}

		_, err = LoadConfig(tmpFile)
		require.Error(t, err)
		expected := fmt.Sprintf(
			"config section in %s must be a mapping",
			tmpFile,
		)
		assert.Equal(t, expected, err.Error())
	})
}
