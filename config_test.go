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

package dingo

import (
	"context"
	"runtime"
	"testing"
	"time"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageModeValid(t *testing.T) {
	tests := []struct {
		mode  StorageMode
		valid bool
	}{
		{StorageModeCore, true},
		{StorageModeAPI, true},
		{"", false},
		{"invalid", false},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.valid, tt.mode.Valid(), "mode=%q", tt.mode)
	}
}

func TestStorageModeIsAPI(t *testing.T) {
	assert.False(t, StorageModeCore.IsAPI())
	assert.True(t, StorageModeAPI.IsAPI())
}

func TestWithStorageMode(t *testing.T) {
	cfg := NewConfig()

	// Apply API mode
	WithStorageMode(StorageModeAPI)(&cfg)
	assert.Equal(t, string(StorageModeAPI), cfg.StorageMode())

	// Apply core mode
	WithStorageMode(StorageModeCore)(&cfg)
	assert.Equal(t, string(StorageModeCore), cfg.StorageMode())
}

func TestWithMidnightConfig(t *testing.T) {
	cfg := NewConfig()
	midnightCfg := MidnightConfig{
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
	}

	WithMidnightConfig(midnightCfg)(&cfg)

	midnight := cfg.Midnight()
	assert.Equal(t, uint(50052), midnight.Port)
	assert.Equal(t, "127.0.0.1", midnight.Host)
	assert.Equal(t, "policy1", midnight.CNightPolicyID)
}

func TestExperimentalDijkstraEnabled(t *testing.T) {
	tests := []struct {
		name     string
		setup    func() Config
		expected bool
	}{
		{
			name:     "default",
			setup:    func() Config { return NewConfig() },
			expected: false,
		},
		{
			name: "leios run mode",
			setup: func() Config {
				return NewConfig(WithRunMode("leios"))
			},
			expected: true,
		},
		{
			name: "dijkstra start era",
			setup: func() Config {
				return NewConfig(WithStartEra("dijkstra"))
			},
			expected: true,
		},
		{
			name: "leios and dijkstra",
			setup: func() Config {
				return NewConfig(
					WithRunMode("leios"),
					WithStartEra("dijkstra"),
				)
			},
			expected: true,
		},
		{
			// `dingo -n musashi` sets the network name but leaves run
			// mode at its default; the Musashi testnet still requires the
			// Dijkstra era table to follow the chain.
			name: "musashi network by name",
			setup: func() Config {
				return NewConfig(WithNetwork("musashi"))
			},
			expected: true,
		},
		{
			// Same network selected via its magic (e.g. --network-magic
			// 164) with no network name.
			name: "musashi network by magic",
			setup: func() Config {
				return NewConfig(WithNetworkMagic(164))
			},
			expected: true,
		},
		{
			name: "non-musashi network stays disabled",
			setup: func() Config {
				return NewConfig(
					WithNetwork("preview"),
					WithNetworkMagic(2),
				)
			},
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.setup()
			assert.Equal(
				t,
				tt.expected,
				cfg.experimentalDijkstraEnabled(),
			)
		})
	}
}

// TestExperimentalLeiosNetworkingEnabled locks the decoupling between the
// Dijkstra ledger era and the Leios node-to-node mini-protocols: the musashi
// network enables the Dijkstra era so the chain can be followed, and now also
// opens leios-notify / leios-fetch. The standalone leios-votes protocol stays
// gated off for prototype interop.
func TestExperimentalLeiosNetworkingEnabled(t *testing.T) {
	tests := []struct {
		name              string
		setup             func() Config
		expectNetworking  bool
		expectDijkstraEra bool
	}{
		{
			name:              "default",
			setup:             func() Config { return NewConfig() },
			expectNetworking:  false,
			expectDijkstraEra: false,
		},
		{
			name: "leios run mode enables both",
			setup: func() Config {
				return NewConfig(WithRunMode("leios"))
			},
			expectNetworking:  true,
			expectDijkstraEra: true,
		},
		{
			name: "dijkstra start era enables both",
			setup: func() Config {
				return NewConfig(WithStartEra("dijkstra"))
			},
			expectNetworking:  true,
			expectDijkstraEra: true,
		},
		{
			// `dingo -n musashi`: the Musashi testnet enables both the
			// Dijkstra era and the Leios mini-protocols (leios-notify /
			// leios-fetch).
			name: "musashi network enables both",
			setup: func() Config {
				return NewConfig(WithNetwork("musashi"))
			},
			expectNetworking:  true,
			expectDijkstraEra: true,
		},
		{
			name: "musashi network by magic enables both",
			setup: func() Config {
				return NewConfig(WithNetworkMagic(164))
			},
			expectNetworking:  true,
			expectDijkstraEra: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.setup()
			assert.Equal(
				t,
				tt.expectNetworking,
				cfg.experimentalLeiosNetworkingEnabled(),
				"leios networking",
			)
			assert.Equal(
				t,
				tt.expectDijkstraEra,
				cfg.experimentalDijkstraEnabled(),
				"dijkstra era",
			)
		})
	}
}

func TestPeerGovernorOptionsIgnoreNonPositiveValues(t *testing.T) {
	cfg := NewConfig()

	// Apply options with non-positive values
	WithMinHotPeers(-1)(&cfg)
	WithReconcileInterval(-1 * time.Minute)(&cfg)
	WithInactivityTimeout(-5 * time.Minute)(&cfg)
	WithMaxConnectionsPerIP(-2)(&cfg)
	WithMaxInboundConns(0)(&cfg)

	// These should remain at default (zero) since non-positive values are ignored
	assert.Zero(t, cfg.MinHotPeers())
	assert.Zero(t, cfg.ReconcileInterval())
	assert.Zero(t, cfg.InactivityTimeout())
	assert.Zero(t, cfg.MaxConnectionsPerIP())
	assert.Zero(t, cfg.MaxInboundConns())
}

func TestPeerGovernorOptionsApplyPositiveValues(t *testing.T) {
	cfg := NewConfig()

	WithMinHotPeers(3)(&cfg)
	WithReconcileInterval(30 * time.Second)(&cfg)
	WithInactivityTimeout(2 * time.Minute)(&cfg)
	WithMaxConnectionsPerIP(4)(&cfg)
	WithMaxInboundConns(25)(&cfg)

	assert.Equal(t, 3, cfg.MinHotPeers())
	assert.Equal(t, 30*time.Second, cfg.ReconcileInterval())
	assert.Equal(t, 2*time.Minute, cfg.InactivityTimeout())
	assert.Equal(t, 4, cfg.MaxConnectionsPerIP())
	assert.Equal(t, 25, cfg.MaxInboundConns())
}

// TestUpdateRTSMetrics verifies the pure-function mapping from
// runtime.MemStats fields to the four cardano_node_metrics_RTS_* gauges.
// Specifically exercises the NumGC - NumForcedGC subtraction so a future
// typo that inverts the operands is caught immediately.
func TestUpdateRTSMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)
	m := &rtsMetrics{
		gcLiveBytes: factory.NewGauge(
			prometheus.GaugeOpts{Name: "test_live"},
		),
		gcHeapBytes: factory.NewGauge(
			prometheus.GaugeOpts{Name: "test_heap"},
		),
		gcMajorNum: factory.NewGauge(
			prometheus.GaugeOpts{Name: "test_major"},
		),
		gcMinorNum: factory.NewGauge(
			prometheus.GaugeOpts{Name: "test_minor"},
		),
	}
	stats := &runtime.MemStats{
		HeapAlloc:   1024,
		HeapSys:     4096,
		Sys:         8192,
		NumGC:       10,
		NumForcedGC: 3,
	}

	updateRTSMetrics(m, stats)

	require.Equal(t, float64(1024), promtestutil.ToFloat64(m.gcLiveBytes))
	require.Equal(t, float64(4096), promtestutil.ToFloat64(m.gcHeapBytes))
	require.Equal(t, float64(3), promtestutil.ToFloat64(m.gcMajorNum))
	// 10 total - 3 forced = 7 automatic
	require.Equal(t, float64(7), promtestutil.ToFloat64(m.gcMinorNum))
}

// TestRunRTSMetricsUpdater_Lifecycle verifies the background updater
// populates the gauges after its initial prime and exits cleanly when
// the context is cancelled.
func TestRunRTSMetricsUpdater_Lifecycle(t *testing.T) {
	reg := prometheus.NewRegistry()
	n := &Node{config: Config{promRegistry: reg}}
	n.registerRTSMetrics()
	require.NotNil(t, n.rtsMetrics, "registerRTSMetrics must populate n.rtsMetrics")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		n.runRTSMetricsUpdater(ctx, 5*time.Millisecond)
		close(done)
	}()

	// Wait for the initial prime (or first tick) to populate real values.
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(n.rtsMetrics.gcHeapBytes) > 0
	}, 2*time.Second, 10*time.Millisecond, "gcHeapBytes should be populated by the updater")

	cancel()
	testutil.RequireReceive(
		t,
		done,
		2*time.Second,
		"updater should exit after ctx cancel",
	)
}

func TestWithLeiosVoteSigningKeyFile(t *testing.T) {
	cfg := NewConfig()
	assert.Equal(t, "", cfg.LeiosVoteSigningKeyFile())
	WithLeiosVoteSigningKeyFile("/keys/leios-vote.skey")(&cfg)
	assert.Equal(t, "/keys/leios-vote.skey", cfg.LeiosVoteSigningKeyFile())
}

func TestWithLeiosVoterPublicKeys(t *testing.T) {
	cfg := NewConfig()
	assert.Nil(t, cfg.LeiosVoterPublicKeys())
	keys := map[string]string{"aabbcc": "ddeeff"}
	WithLeiosVoterPublicKeys(keys)(&cfg)
	assert.Equal(
		t,
		map[string]string{"aabbcc": "ddeeff"},
		cfg.LeiosVoterPublicKeys(),
	)
	// The option copies the map: later caller mutations must not
	// change live config
	keys["aabbcc"] = "mutated"
	assert.Equal(t, "ddeeff", cfg.LeiosVoterPublicKeys()["aabbcc"])
}

// TestConfigConvergence verifies that the public and internal configs are
// properly converged with a single source of truth. This addresses GitHub
// issue #2277.
func TestConfigConvergence(t *testing.T) {
	t.Run("NewConfig creates config with proper defaults", func(t *testing.T) {
		cfg := NewConfig()

		// Verify defaults are applied
		assert.Equal(t, 0.90, cfg.EvictionWatermark())
		assert.Equal(t, 0.95, cfg.RejectionWatermark())
		assert.Equal(t, "0.0.0.0", cfg.BindAddr())
		assert.Equal(t, "core", cfg.StorageMode())
		assert.Equal(t, internalconfig.RunModeServe, cfg.RunMode())
		assert.Equal(t, []string{"*"}, cfg.CORSAllowedOrigins())

		// Verify nested config defaults
		cache := cfg.Cache()
		assert.Equal(t, 50000, cache.HotUtxoEntries)
		assert.Equal(t, 10000, cache.HotTxEntries)

		chainsync := cfg.Chainsync()
		assert.Equal(t, 3, chainsync.MaxClients)
		assert.Equal(t, "2m", chainsync.StallTimeout)
		assert.Equal(t, "primary", chainsync.Strategy)
	})

	t.Run("With* functions modify single source of truth", func(t *testing.T) {
		cfg := NewConfig(
			WithNetwork("mainnet"),
			WithNetworkMagic(764824073),
			WithDatabasePath("/custom/data"),
			WithBindAddr("127.0.0.1"),
			WithMempoolCapacity(5242880),
			WithEvictionWatermark(0.88),
			WithRejectionWatermark(0.93),
			WithRunMode("dev"),
			WithStorageMode(StorageModeAPI),
			WithValidateHistorical(true),
			WithPeerTargets(200, 75, 30),
		)

		// Verify all values are accessible through accessor methods
		assert.Equal(t, "mainnet", cfg.Network())
		assert.Equal(t, uint32(764824073), cfg.NetworkMagic())
		assert.Equal(t, "/custom/data", cfg.DatabasePath())
		assert.Equal(t, "127.0.0.1", cfg.BindAddr())
		assert.Equal(t, int64(5242880), cfg.MempoolCapacity())
		assert.Equal(t, 0.88, cfg.EvictionWatermark())
		assert.Equal(t, 0.93, cfg.RejectionWatermark())
		assert.Equal(t, internalconfig.RunModeDev, cfg.RunMode())
		assert.Equal(t, "api", cfg.StorageMode())
		assert.True(t, cfg.ValidateHistorical())
		assert.Equal(t, 200, cfg.TargetNumberOfKnownPeers())
		assert.Equal(t, 75, cfg.TargetNumberOfEstablishedPeers())
		assert.Equal(t, 30, cfg.TargetNumberOfActivePeers())
	})

	t.Run("NewConfigFromInternal preserves all values", func(t *testing.T) {
		// Create internal config with specific values
		internal := &internalconfig.Config{
			Network:                        "preview",
			NetworkMagic:                   2,
			DatabasePath:                   "/data/preview",
			SocketPath:                     "/run/cardano.sock",
			MempoolCapacity:                2097152,
			EvictionWatermark:              0.87,
			RejectionWatermark:             0.94,
			RunMode:                        internalconfig.RunModeLoad,
			StorageMode:                    "api",
			TargetNumberOfKnownPeers:       250,
			TargetNumberOfEstablishedPeers: 80,
			TargetNumberOfActivePeers:      35,
			BlockProducer:                  true,
			ValidateHistorical:             true,
			IntersectTip:                   true,
		}

		// Convert to public config
		public, err := NewConfigFromInternal(internal, nil, nil, nil, nil)
		assert.NoError(t, err)

		// Verify all values are preserved and accessible
		assert.Equal(t, "preview", public.Network())
		assert.Equal(t, uint32(2), public.NetworkMagic())
		assert.Equal(t, "/data/preview", public.DatabasePath())
		assert.Equal(t, "/run/cardano.sock", public.SocketPath())
		assert.Equal(t, int64(2097152), public.MempoolCapacity())
		assert.Equal(t, 0.87, public.EvictionWatermark())
		assert.Equal(t, 0.94, public.RejectionWatermark())
		assert.Equal(t, internalconfig.RunModeLoad, public.RunMode())
		assert.Equal(t, "api", public.StorageMode())
		assert.Equal(t, 250, public.TargetNumberOfKnownPeers())
		assert.Equal(t, 80, public.TargetNumberOfEstablishedPeers())
		assert.Equal(t, 35, public.TargetNumberOfActivePeers())
		assert.True(t, public.BlockProducer())
		assert.True(t, public.ValidateHistorical())
		assert.True(t, public.IntersectTip())
	})

	t.Run("Setter methods work correctly", func(t *testing.T) {
		cfg := NewConfig()

		// Use setter methods
		cfg.SetStorageMode("api")
		cfg.SetTargetNumberOfKnownPeers(300)
		cfg.SetTargetNumberOfEstablishedPeers(90)
		cfg.SetTargetNumberOfActivePeers(40)

		// Verify changes are reflected
		assert.Equal(t, "api", cfg.StorageMode())
		assert.Equal(t, 300, cfg.TargetNumberOfKnownPeers())
		assert.Equal(t, 90, cfg.TargetNumberOfEstablishedPeers())
		assert.Equal(t, 40, cfg.TargetNumberOfActivePeers())
	})

	t.Run("Adding new config field has single path", func(t *testing.T) {
		// This test documents the pattern for adding a new config field.
		// When adding a field:
		// 1. Add it to internal/config/config.go with YAML/env tags
		// 2. Add accessor method to Config in config.go
		// 3. Add With* option function in config.go (if settable)
		// 4. Tests automatically work with accessor methods

		// Example: verify a few fields follow this pattern
		cfg := NewConfig(
			WithBlockfrostPort(8090),
			WithUtxorpcPort(50051),
			WithMeshPort(8091),
		)

		// Accessor methods provide the interface
		assert.Equal(t, uint(8090), cfg.BlockfrostPort())
		assert.Equal(t, uint(50051), cfg.UtxorpcPort())
		assert.Equal(t, uint(8091), cfg.MeshPort())
	})

	t.Run("NewConfigFromInternal returns error for invalid chainsync.stallTimeout", func(t *testing.T) {
		// Test that invalid chainsync.stallTimeout causes fail-fast behavior
		internal := &internalconfig.Config{
			Chainsync: internalconfig.ChainsyncConfig{
				StallTimeout: "invalid-duration",
			},
		}

		_, err := NewConfigFromInternal(internal, nil, nil, nil, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid chainsync stall timeout")
	})

	t.Run("NewConfigFromInternal preserves tracing settings", func(t *testing.T) {
		// Test that Tracing and TracingStdout are carried over from internal config
		internal := &internalconfig.Config{
			Tracing:       true,
			TracingStdout: true,
		}

		cfg, err := NewConfigFromInternal(internal, nil, nil, nil, nil)
		assert.NoError(t, err)
		assert.True(t, cfg.Tracing())
		assert.True(t, cfg.TracingStdout())
	})
}

// TestConfigAccessorCompleteness verifies that accessor methods exist for
// all commonly-used configuration fields, catching drift between internal
// config struct and public accessor API.
func TestConfigAccessorCompleteness(t *testing.T) {
	internal := &internalconfig.Config{
		// Set various fields to non-default values
		Network:                       "testnet",
		NetworkMagic:                  42,
		DatabasePath:                  "/db",
		SocketPath:                    "/sock",
		CardanoConfig:                 "/cfg",
		Topology:                      "/topo",
		BlobPlugin:                    "s3",
		MetadataPlugin:                "postgres",
		BindAddr:                      "1.2.3.4",
		PrivateBindAddr:               "127.0.0.1",
		RelayPort:                     3001,
		PrivatePort:                   3002,
		MetricsPort:                   8080,
		DebugPort:                     6060,
		BlockfrostPort:                8090,
		UtxorpcPort:                   50051,
		MeshPort:                      8091,
		BarkPort:                      8092,
		BarkBaseUrl:                   "http://bark",
		TlsCertFilePath:               "/cert",
		TlsKeyFilePath:                "/key",
		IntersectTip:                  true,
		ValidateHistorical:            true,
		ShutdownTimeout:               "45s",
		LedgerCatchupTimeout:          "60m",
		MempoolCapacity:               999,
		EvictionWatermark:             0.77,
		RejectionWatermark:            0.88,
		RunMode:                       internalconfig.RunModeDev,
		StartEra:                      internalconfig.StartEraDijkstra,
		ImmutableDbPath:               "/immut",
		StorageMode:                   "api",
		DatabaseWorkers:               8,
		DatabaseQueueSize:             2000,
		BackfillBatchSize:             200,
		BlockProducer:                 true,
		ShelleyVRFKey:                 "/vrf",
		ShelleyKESKey:                 "/kes",
		ShelleyOperationalCertificate: "/cert",
		ForgeSyncToleranceSlots:       50,
		ForgeStaleGapThresholdSlots:   500,
		ValidateForgedBlock:           true,
		LeiosVoteSigningKeyFile:       "/leios",
		SlotsPerKESPeriod:             100,
		MaxKESEvolutions:              50,
	}

	cfg, err := NewConfigFromInternal(internal, nil, nil, nil, nil)
	assert.NoError(t, err)

	// Verify all fields have working accessor methods
	assert.Equal(t, "testnet", cfg.Network())
	assert.Equal(t, uint32(42), cfg.NetworkMagic())
	assert.Equal(t, "/db", cfg.DatabasePath())
	assert.Equal(t, "/sock", cfg.SocketPath())
	assert.Equal(t, "/cfg", cfg.CardanoConfig())
	assert.Equal(t, "/topo", cfg.Topology())
	assert.Equal(t, "s3", cfg.BlobPlugin())
	assert.Equal(t, "postgres", cfg.MetadataPlugin())
	assert.Equal(t, "1.2.3.4", cfg.BindAddr())
	assert.Equal(t, "127.0.0.1", cfg.PrivateBindAddr())
	assert.Equal(t, uint(3001), cfg.RelayPort())
	assert.Equal(t, uint(3002), cfg.PrivatePort())
	assert.Equal(t, uint(8080), cfg.MetricsPort())
	assert.Equal(t, uint(6060), cfg.DebugPort())
	assert.Equal(t, uint(8090), cfg.BlockfrostPort())
	assert.Equal(t, uint(50051), cfg.UtxorpcPort())
	assert.Equal(t, uint(8091), cfg.MeshPort())
	assert.Equal(t, uint(8092), cfg.BarkPort())
	assert.Equal(t, "http://bark", cfg.BarkBaseUrl())
	assert.Equal(t, "/cert", cfg.TlsCertFilePath())
	assert.Equal(t, "/key", cfg.TlsKeyFilePath())
	assert.True(t, cfg.IntersectTip())
	assert.True(t, cfg.ValidateHistorical())
	assert.Equal(t, "45s", cfg.ShutdownTimeout())
	assert.Equal(t, "60m", cfg.LedgerCatchupTimeout())
	assert.Equal(t, int64(999), cfg.MempoolCapacity())
	assert.Equal(t, 0.77, cfg.EvictionWatermark())
	assert.Equal(t, 0.88, cfg.RejectionWatermark())
	assert.Equal(t, internalconfig.RunModeDev, cfg.RunMode())
	assert.Equal(t, internalconfig.StartEraDijkstra, cfg.StartEra())
	assert.Equal(t, "/immut", cfg.ImmutableDbPath())
	assert.Equal(t, "api", cfg.StorageMode())
	assert.Equal(t, 8, cfg.DatabaseWorkers())
	assert.Equal(t, 2000, cfg.DatabaseQueueSize())
	assert.Equal(t, 200, cfg.BackfillBatchSize())
	assert.True(t, cfg.BlockProducer())
	assert.Equal(t, "/vrf", cfg.ShelleyVRFKey())
	assert.Equal(t, "/kes", cfg.ShelleyKESKey())
	assert.Equal(t, "/cert", cfg.ShelleyOperationalCertificate())
	assert.Equal(t, uint64(50), cfg.ForgeSyncToleranceSlots())
	assert.Equal(t, uint64(500), cfg.ForgeStaleGapThresholdSlots())
	assert.True(t, cfg.ValidateForgedBlock())
	assert.Equal(t, "/leios", cfg.LeiosVoteSigningKeyFile())
	assert.Equal(t, uint64(100), cfg.SlotsPerKESPeriod())
	assert.Equal(t, uint64(50), cfg.MaxKESEvolutions())
}
