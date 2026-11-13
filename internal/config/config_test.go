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
		Plugins:                          defaultPluginsConfig(),
		BindAddr:                         "0.0.0.0",
		CardanoConfig:                    "", // Will be set dynamically based on network
		DatabasePath:                     ".dingo",
		SocketPath:                       "dingo.socket",
		IntersectTip:                     false,
		ValidateHistorical:               true,
		StrictUtxoValidation:             true,
		Network:                          "preview",
		MetricsPort:                      12798,
		HealthPort:                       DefaultHealthPort,
		HealthReadyGapSlots:              DefaultHealthReadyGapSlots,
		DebugBindAddr:                    DefaultDebugBindAddr,
		MetricsBindAddr:                  DefaultMetricsBindAddr,
		PrivateBindAddr:                  "127.0.0.1",
		PrivatePort:                      0,
		RelayPort:                        3001,
		CORSAllowedOrigins:               []string{"*"},
		Topology:                         "",
		TlsCertFilePath:                  "",
		TlsKeyFilePath:                   "",
		RunMode:                          RunModeServe,
		StartEra:                         StartEraDefault,
		ImmutableDbPath:                  "",
		ShutdownTimeout:                  DefaultShutdownTimeout,
		LedgerCatchupTimeout:             DefaultLedgerCatchupTimeout,
		DatabaseWorkers:                  5,
		DatabaseQueueSize:                50,
		BackfillBatchSize:                200,
		GenesisBootstrap:                 DefaultGenesisBootstrapConfig(),
		HistoryExpiry:                    DefaultHistoryExpiryConfig(),
		KoiosParity:                      DefaultKoiosParityConfig(),
		Midnight:                         DefaultMidnightConfig(),
		ForgeSyncToleranceSlots:          DefaultForgeSyncToleranceSlots,
		ForgeStaleGapThresholdSlots:      DefaultForgeStaleGapThresholdSlots,
		ForgeUpstreamStalenessSlots:      DefaultForgeUpstreamStalenessSlots,
		ForgeAppliedTipStalenessSlots:    DefaultForgeAppliedTipStalenessSlots,
		ForgeEndorserBlockStalenessSlots: DefaultForgeEndorserBlockStalenessSlots,
		ForgeEBMaxTxRefs:                 forgeEBCapDefault(DefaultForgeEBMaxTxRefs),
		ForgeEBMaxBytes:                  forgeEBCapDefault(DefaultForgeEBMaxBytes),
		ForgeEBSelectionReserve:          DefaultForgeEBSelectionReserve,
		Mithril: MithrilConfig{
			WindowChunks:       DefaultMithrilWindowChunks,
			BatchChunks:        DefaultMithrilBatchChunks,
			DownloadWorkers:    DefaultMithrilDownloadWorkers,
			MinGapChunks:       DefaultMithrilMinGapChunks,
			CheckInterval:      DefaultMithrilCheckInterval,
			RetryBackoff:       DefaultMithrilRetryBackoff,
			CleanupAfterLoad:   true,
			VerifyCertificates: true,
		},
		// Fail closed: mirrors newDefaultConfig's own ValidateForgedBlock
		// default so this test-only reset does not silently
		// diverge from what an operator actually gets. Unlike the several
		// fields above left at their zero value on purpose (StorageMode,
		// Cache, Chainsync, SlotsPerKESPeriod, ...), so tests can observe
		// their own fill-in-if-empty defaulting logic in isolation,
		// ValidateForgedBlock's production default is an unconditional
		// literal with no separate fill-in step to test around.
		ValidateForgedBlock: true,
		// Mirrors newDefaultConfig: the apply-ahead work is on by default.
		LedgerPrefetchAheadEnabled: true,
		LedgerValidateAheadEnabled: true,
	}
	globalTopologyConfig = &topology.TopologyConfig{}
}

func unsetDebugBindAddrEnv(t *testing.T) {
	t.Helper()
	unsetEnv(t, "DINGO_DEBUG_BIND_ADDR")
}

func unsetMetricsBindAddrEnv(t *testing.T) {
	t.Helper()
	unsetEnv(t, "DINGO_METRICS_BIND_ADDR")
}

// unsetEnv preserves the caller's environment while ensuring config tests
// that exercise defaults or YAML precedence do not inherit an override.
func unsetEnv(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")
	require.NoError(t, os.Unsetenv(key))
}

// unsetForgeGateEnv clears the forge-gate overrides so tests that assert the
// built-in defaults cannot inherit a value from the caller's environment.
// LoadConfig runs envconfig AFTER the YAML merge, so an exported
// DINGO_FORGE_* variable silently overrides both the fixture and the default,
// and the assertion then fails for a reason unrelated to the code under test.
//
// Both spellings of every forge-gate knob are cleared. LoadConfig calls
// envconfig.Process("cardano", cfg), so the name envconfig looks up FIRST is
// the prefixed CARDANO_DINGO_... form; the bare DINGO_... name in the
// envconfig struct tag is only the alt name it falls back to. Guarding one
// spelling leaves the other able to override the value under test.
func unsetForgeGateEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"DINGO_FORGE_PRIMARY_CHAIN_TIP_TOLERANCE_SLOTS",
		"CARDANO_DINGO_FORGE_PRIMARY_CHAIN_TIP_TOLERANCE_SLOTS",
		"DINGO_FORGE_SYNC_TOLERANCE_SLOTS",
		"CARDANO_DINGO_FORGE_SYNC_TOLERANCE_SLOTS",
		"DINGO_FORGE_STALE_GAP_THRESHOLD_SLOTS",
		"CARDANO_DINGO_FORGE_STALE_GAP_THRESHOLD_SLOTS",
		"DINGO_FORGE_UPSTREAM_STALENESS_SLOTS",
		"CARDANO_DINGO_FORGE_UPSTREAM_STALENESS_SLOTS",
		"DINGO_FORGE_APPLIED_TIP_STALENESS_SLOTS",
		"CARDANO_DINGO_FORGE_APPLIED_TIP_STALENESS_SLOTS",
		"DINGO_FORGE_ENDORSER_BLOCK_STALENESS_SLOTS",
		"CARDANO_DINGO_FORGE_ENDORSER_BLOCK_STALENESS_SLOTS",
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
	unsetMetricsBindAddrEnv(t)
	unsetForgeGateEnv(t)
	unsetGenesisCSJEnv(t)
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
  downloadMaxBytes: 4294967296
  cleanupAfterLoad: false
  verifyCertificates: false
`

	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "test-dingo.yaml")

	t.Setenv("DINGO_FORGE_SYNC_TOLERANCE_SLOTS", "321")
	t.Setenv("DINGO_FORGE_STALE_GAP_THRESHOLD_SLOTS", "654")
	t.Setenv("DINGO_FORGE_EB_MAX_TX_REFS", "777")
	t.Setenv("DINGO_FORGE_EB_MAX_BYTES", "888888")
	t.Setenv("DINGO_FORGE_EB_SELECTION_RESERVE", "750ms")

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
		HealthPort:           DefaultHealthPort,
		HealthReadyGapSlots:  DefaultHealthReadyGapSlots,
		DebugBindAddr:        DefaultDebugBindAddr,
		MetricsBindAddr:      DefaultMetricsBindAddr,
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
			CorroborationPeers:          0,
			// Unset in the file, so the defaults survive.
			LimitOnPatienceEnabled: true,
			ChainSyncJumping:       true,
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
		// These come from the globalConfig clone seeded by resetGlobalConfig,
		// not from ApplyDefaults: this test never calls it, and LoadConfig
		// only parses and merges. Both staleness bounds are 0 there because
		// 0 means "disabled" for them rather than "unset".
		ForgeUpstreamStalenessSlots:      DefaultForgeUpstreamStalenessSlots,
		ForgeAppliedTipStalenessSlots:    DefaultForgeAppliedTipStalenessSlots,
		ForgeEndorserBlockStalenessSlots: DefaultForgeEndorserBlockStalenessSlots,
		ValidateForgedBlock:              true,
		LedgerPrefetchAheadEnabled:       true,
		LedgerValidateAheadEnabled:       true,
		ForgeEBMaxTxRefs:                 forgeEBCapDefault(777),
		ForgeEBMaxBytes:                  forgeEBCapDefault(888888),
		ForgeEBSelectionReserve:          750 * time.Millisecond,
		Mithril: MithrilConfig{
			Enabled:                false,
			WindowChunks:           DefaultMithrilWindowChunks,
			BatchChunks:            DefaultMithrilBatchChunks,
			DownloadWorkers:        DefaultMithrilDownloadWorkers,
			MinGapChunks:           DefaultMithrilMinGapChunks,
			CheckInterval:          DefaultMithrilCheckInterval,
			RetryBackoff:           DefaultMithrilRetryBackoff,
			AggregatorURL:          "https://mithril.example.net",
			DownloadDir:            "/tmp/mithril",
			DownloadIdleTimeout:    "5m",
			DownloadMaxIdleRetries: 9,
			DownloadMaxBytes:       4294967296,
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

// The NtC TCP listener is opt-in: resetGlobalConfig is a hand-kept fixture, so
// the default is pinned on the constructor itself.
func TestDefaultConfigDisablesNtCTCPListener(t *testing.T) {
	t.Parallel()
	if port := newDefaultConfig().PrivatePort; port != 0 {
		t.Fatalf("expected PrivatePort 0 by default, got %d", port)
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

func TestLoad_MidnightEnvOverridesYAML(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("DINGO_MIDNIGHT_SERVER_ENABLED", "true")
	t.Setenv("DINGO_MIDNIGHT_REFLECTION_ENABLED", "true")
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
	if !cfg.Midnight.ServerEnabled || !cfg.Midnight.ReflectionEnabled {
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
			name:     "musashi default raises to 25 MiB",
			yaml:     "runMode: \"serve\"\nnetwork: \"musashi\"\n",
			expected: DefaultMempoolCapacityLeios,
		},
		{
			name:     "explicit praos-sized value wins on musashi",
			yaml:     "network: \"musashi\"\nplugins:\n  mempool:\n    provider: default\n    config:\n      capacity: 1048576\n      evictionWatermark: 0.90\n      rejectionWatermark: 0.95\n",
			expected: DefaultMempoolCapacityPraos,
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

func TestCloneConfigDoesNotShareTracingPools(t *testing.T) {
	cfg := &Config{TracingAlwaysSamplePools: []string{"a", "b"}}
	clone := cloneConfig(cfg)
	clone.TracingAlwaysSamplePools[0] = "mutated"
	assert.Equal(t, "a", cfg.TracingAlwaysSamplePools[0])
}

func TestCloneConfigDoesNotShareBlockProducerPools(t *testing.T) {
	cfg := &Config{BlockProducerPools: []BlockProducerPool{
		{ShelleyVRFKey: "a.vrf.skey"},
	}}
	clone := cloneConfig(cfg)
	clone.BlockProducerPools[0].ShelleyVRFKey = "mutated"
	assert.Equal(t, "a.vrf.skey", cfg.BlockProducerPools[0].ShelleyVRFKey)
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

// TestLoad_ExampleConfigParses guards against regressions where
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

func TestConfigSnapshotsDoNotShareRegistryCredentials(t *testing.T) {
	// Not t.Parallel: replaces the process-global configuration.
	configMu.Lock()
	previous := globalConfig
	globalConfig = cloneConfig(previous)
	globalConfig.TokenRegistry.HeaderSecrets = map[string]string{
		"X-API-Key": "original",
	}
	configMu.Unlock()
	t.Cleanup(
		func() { configMu.Lock(); globalConfig = previous; configMu.Unlock() },
	)
	snapshot := GetConfig()
	snapshot.TokenRegistry.HeaderSecrets["X-API-Key"] = "changed"
	snapshot.TokenRegistry.HeaderSecrets["new"] = "value"
	require.Equal(
		t,
		map[string]string{"X-API-Key": "original"},
		GetConfig().TokenRegistry.HeaderSecrets,
	)
}

func TestCloneConfigIsolatesMCP(t *testing.T) {
	t.Parallel()
	cfg := &Config{Plugins: defaultPluginsConfig()}
	cfg.Plugins.API.Mcp.Config["tls"] = map[string]any{"enabled": true}
	clone := cloneConfig(cfg)
	clone.Plugins.API.Mcp.Config["port"] = 8088
	clone.Plugins.API.Mcp.Config["authToken"] = "override"
	clone.Plugins.API.Mcp.Config["tls"].(map[string]any)["enabled"] = false
	require.Equal(t, 0, cfg.Plugins.API.Mcp.Config["port"])
	require.NotContains(t, cfg.Plugins.API.Mcp.Config, "authToken")
	require.Equal(
		t,
		true,
		cfg.Plugins.API.Mcp.Config["tls"].(map[string]any)["enabled"],
	)
}

// TestDefaultMithrilServerPortIsUnshared guards running `dingo mithril serve`
// beside a node with default settings: both bind the shared bindAddr, and the
// two commands are validated separately, so a shared default port would only
// fail at bind time.
func TestDefaultMithrilServerPortIsUnshared(t *testing.T) {
	t.Parallel()
	cfg := newDefaultConfig()
	api := cfg.Plugins.API
	for name, port := range map[string]uint{
		"relay":      cfg.RelayPort,
		"private":    cfg.PrivatePort,
		"metrics":    cfg.MetricsPort,
		"health":     cfg.HealthPort,
		"midnight":   cfg.Midnight.Port,
		"blockfrost": APIPluginPort(api.Blockfrost),
		"kupo":       APIPluginPort(api.Kupo),
		"mesh":       APIPluginPort(api.Mesh),
		"utxorpc":    APIPluginPort(api.Utxorpc),
		"mcp":        APIPluginPort(api.Mcp),
	} {
		assert.NotEqual(t, port, cfg.Mithril.Server.Port, name)
	}
}

// unsetGenesisCSJEnv clears both spellings envconfig reads for
// GenesisBootstrap.ChainSyncJumping: the nested container's derived prefix
// form, checked first, and the bare tag it falls back to.
func unsetGenesisCSJEnv(t *testing.T) {
	t.Helper()
	unsetEnv(t, "CARDANO_GENESISBOOTSTRAP_DINGO_GENESIS_CSJ")
	unsetEnv(t, "DINGO_GENESIS_CSJ")
}

func TestLoad_GenesisChainSyncJumpingEnv(t *testing.T) {
	// Not t.Parallel: LoadConfig and resetGlobalConfig replace process-global
	// configuration state.
	unsetGenesisCSJEnv(t)
	resetGlobalConfig()
	globalConfig.RunMode = RunModeDev
	cfg, err := LoadConfig("")
	require.NoError(t, err)
	require.True(t, cfg.GenesisBootstrap.ChainSyncJumping, "on by default")

	t.Setenv("DINGO_GENESIS_CSJ", "false")
	resetGlobalConfig()
	globalConfig.RunMode = RunModeDev
	cfg, err = LoadConfig("")
	require.NoError(t, err)
	require.False(t, cfg.GenesisBootstrap.ChainSyncJumping)
}
