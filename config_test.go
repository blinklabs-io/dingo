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
	"reflect"
	"runtime"
	"testing"
	"time"

	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
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
	cfg := &Config{}

	// Default should be zero value (empty string)
	assert.Equal(t, StorageMode(""), cfg.storageMode)

	// Apply API mode
	WithStorageMode(StorageModeAPI)(cfg)
	assert.Equal(t, StorageModeAPI, cfg.storageMode)

	// Apply core mode
	WithStorageMode(StorageModeCore)(cfg)
	assert.Equal(t, StorageModeCore, cfg.storageMode)
}

func TestNewConfigMempoolCapacityDefaultsFromRunMode(t *testing.T) {
	tests := []struct {
		name     string
		runMode  string
		expected int64
	}{
		{
			name:     "default",
			expected: int64(internalconfig.DefaultMempoolCapacityPraos),
		},
		{
			name:     "serve",
			runMode:  string(internalconfig.RunModeServe),
			expected: int64(internalconfig.DefaultMempoolCapacityPraos),
		},
		{
			name:     "leios",
			runMode:  string(internalconfig.RunModeLeios),
			expected: int64(internalconfig.DefaultMempoolCapacityLeios),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig(WithRunMode(tt.runMode))
			selection := cfg.pluginSelections[plugin.CapabilityMempool]
			assert.Equal(t, tt.expected, selection.Config["capacity"])
		})
	}
}

func TestNewConfigPreservesExplicitMempoolCapacity(t *testing.T) {
	const capacity = int64(42)
	cfg := NewConfig(
		WithRunMode(string(internalconfig.RunModeLeios)),
		WithPluginSelection(plugin.CapabilityMempool, plugin.Selection{
			Provider: "default",
			Config:   map[string]any{"capacity": capacity},
		}),
	)

	selection := cfg.pluginSelections[plugin.CapabilityMempool]
	assert.Equal(t, capacity, selection.Config["capacity"])
}

func TestNewConfigDefaultsBuiltInMempoolCapacity(t *testing.T) {
	for _, provider := range []string{"default", "fifo", "dag"} {
		t.Run(provider, func(t *testing.T) {
			cfg := NewConfig(WithPluginSelection(
				plugin.CapabilityMempool,
				plugin.Selection{Provider: provider},
			))
			selection := cfg.pluginSelections[plugin.CapabilityMempool]
			assert.Equal(
				t,
				int64(internalconfig.DefaultMempoolCapacityPraos),
				selection.Config["capacity"],
			)
		})
	}
}

func TestNewConfigDoesNotDefaultCustomMempoolConfig(t *testing.T) {
	cfg := NewConfig(
		WithRunMode(string(internalconfig.RunModeLeios)),
		WithPluginSelection(plugin.CapabilityMempool, plugin.Selection{
			Provider: "custom",
			Config:   map[string]any{},
		}),
	)

	selection := cfg.pluginSelections[plugin.CapabilityMempool]
	assert.Empty(t, selection.Config)
}

func TestWithPluginSelectionSnapshotsConfig(t *testing.T) {
	const originalCapacity = int64(2)
	values := []any{"original"}
	nested := map[string]any{"values": values}
	config := map[string]any{
		"capacity": originalCapacity,
		"nested":   nested,
	}
	cfg := NewConfig(WithPluginSelection(
		plugin.CapabilityMempool,
		plugin.Selection{Provider: "default", Config: config},
	))

	config["capacity"] = int64(3)
	config["extra"] = true
	nested["extra"] = true
	values[0] = "mutated"

	selection := cfg.pluginSelections[plugin.CapabilityMempool]
	assert.Equal(t, originalCapacity, selection.Config["capacity"])
	assert.NotContains(t, selection.Config, "extra")
	snapshotNested := selection.Config["nested"].(map[string]any)
	assert.NotContains(t, snapshotNested, "extra")
	assert.Equal(t, "original", snapshotNested["values"].([]any)[0])
}

func TestNewValidatesMinPoolMargin(t *testing.T) {
	tests := []struct {
		name    string
		margin  uint
		wantErr bool
	}{
		{name: "disabled", margin: 0},
		{name: "maximum", margin: 10_000},
		{name: "above maximum", margin: 10_001, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig(
				WithMinPoolMargin(tt.margin),
				WithNetworkMagic(1),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp",
					ListenAddress: "127.0.0.1:0",
				}),
				WithPrometheusRegistry(prometheus.NewRegistry()),
			)
			n, err := New(cfg)
			if tt.wantErr {
				require.ErrorContains(t, err, "min pool margin")
				return
			}
			require.NoError(t, err)
			// New starts the event bus' background goroutines; Stop releases them.
			t.Cleanup(func() { _ = n.Stop() })
		})
	}
}

func TestWithMidnightConfig(t *testing.T) {
	cfg := &Config{}
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

	WithMidnightConfig(midnightCfg)(cfg)

	assert.Equal(t, midnightCfg, cfg.midnight)
}

// TestSyncCompatFieldsMidnightEnabled is a regression test for a bug where
// syncCompatFields's mirror of internalconfig.MidnightConfig into the root
// MidnightConfig (used by node.go's indexer-start gate) omitted Enabled,
// leaving it permanently false and silently disabling the Midnight indexer
// even with midnight.enabled: true configured.
func TestSyncCompatFieldsMidnightEnabled(t *testing.T) {
	cfg := NewConfig()
	cfg.cfg.Midnight.Enabled = true

	cfg.syncCompatFields()

	assert.True(
		t,
		cfg.midnight.Enabled,
		"mirrored MidnightConfig.Enabled must reflect the loaded config",
	)
}

// TestSyncCompatFieldsMidnightAllFieldsMirrored guards against the same bug
// class recurring for any future MidnightConfig field: it sets every field
// of the internal internalconfig.MidnightConfig to a non-zero value, runs
// syncCompatFields, and fails if any same-named field on the mirrored root
// MidnightConfig was left at its zero value. This is what should have
// caught the missing Enabled field before it shipped.
func TestSyncCompatFieldsMidnightAllFieldsMirrored(t *testing.T) {
	src := internalconfig.MidnightConfig{
		Enabled:                     true,
		Port:                        50099,
		Host:                        "127.0.0.1",
		CNightPolicyID:              "policy1",
		CNightAssetName:             "assetname1",
		MappingValidatorAddress:     "addr_mapping",
		AuthTokenPolicyID:           "policy_auth",
		AuthTokenAssetName:          "asset_auth",
		CommitteeCandidateAddress:   "addr_candidate",
		TechnicalCommitteeAddress:   "addr_technical",
		TechnicalCommitteePolicyID:  "policy_technical",
		CouncilAddress:              "addr_council",
		CouncilPolicyID:             "policy_council",
		PermissionedCandidatePolicy: "policy_permissioned",
	}

	// Sanity-check the fixture itself: every field set above must be
	// non-zero, or the comparison loop below could pass by accident on a
	// field nobody actually exercised.
	srcVal := reflect.ValueOf(src)
	for i := range srcVal.NumField() {
		f := srcVal.Field(i)
		require.False(
			t,
			f.IsZero(),
			"test fixture field %s must be non-zero",
			srcVal.Type().Field(i).Name,
		)
	}

	cfg := NewConfig()
	cfg.cfg.Midnight = src
	cfg.syncCompatFields()

	gotVal := reflect.ValueOf(cfg.midnight)
	gotType := gotVal.Type()
	for i := range gotVal.NumField() {
		name := gotType.Field(i).Name
		srcField := srcVal.FieldByName(name)
		if !srcField.IsValid() {
			// Field exists only on the mirror; nothing in the source to
			// compare against.
			continue
		}
		assert.Equal(
			t,
			srcField.Interface(),
			gotVal.Field(i).Interface(),
			"mirrored MidnightConfig.%s does not match source "+
				"internalconfig.MidnightConfig.%s after syncCompatFields",
			name,
			name,
		)
	}
}

func TestConfigValidatePledgeLeverage(t *testing.T) {
	tests := []struct {
		name     string
		enabled  bool
		leverage uint
		wantErr  bool
	}{
		{name: "disabled ignores zero", leverage: 0},
		{
			name:     "enabled rejects zero",
			enabled:  true,
			leverage: 0,
			wantErr:  true,
		},
		{name: "enabled accepts minimum", enabled: true, leverage: 1},
		{name: "enabled accepts typical value", enabled: true, leverage: 100},
		{name: "enabled accepts maximum", enabled: true, leverage: 10_000},
		{
			name:     "enabled rejects above maximum",
			enabled:  true,
			leverage: 10_001,
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig(
				WithNetworkMagic(1),
				WithPrometheusRegistry(prometheus.NewRegistry()),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp",
					ListenAddress: "127.0.0.1:0",
				}),
				WithPledgeLeverage(tt.enabled, tt.leverage),
			)
			n, err := New(cfg)
			if tt.wantErr {
				require.ErrorContains(t, err, "pledge leverage")
				return
			}
			require.NoError(t, err)
			// New starts the event bus' background goroutines; Stop releases them.
			t.Cleanup(func() { _ = n.Stop() })
		})
	}
}

func TestWithFullPotRewards(t *testing.T) {
	cfg := &Config{}
	WithFullPotRewards(true)(cfg)
	assert.True(t, cfg.fullPotRewardsEnabled)
	WithFullPotRewards(false)(cfg)
	assert.False(t, cfg.fullPotRewardsEnabled)
}

func TestFullPotRewardsStandardNetworkValidation(t *testing.T) {
	tests := []struct {
		name    string
		opts    []ConfigOptionFunc
		wantErr string
	}{
		{
			name: "rejects standard network by name",
			opts: []ConfigOptionFunc{
				WithNetwork("preview"),
			},
			wantErr: "full pot rewards are not permitted on standard network \"preview\"",
		},
		{
			name: "rejects standard network by magic",
			opts: []ConfigOptionFunc{
				WithNetwork("private-preview-mirror"),
				WithNetworkMagic(2),
			},
			wantErr: "full pot rewards are not permitted on standard network \"preview\"",
		},
		{
			name: "allows standard network with unsafe opt-in",
			opts: []ConfigOptionFunc{
				WithNetwork("preview"),
				WithUnsafeFullPotRewardsOnStandardNetworks(true),
			},
		},
		{
			name: "allows custom network",
			opts: []ConfigOptionFunc{
				WithNetwork("private-net"),
				WithNetworkMagic(9_999),
			},
		},
		{
			name: "allows devnet",
			opts: []ConfigOptionFunc{
				WithNetwork("devnet"),
				WithNetworkMagic(42),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := []ConfigOptionFunc{
				WithPrometheusRegistry(prometheus.NewRegistry()),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp",
					ListenAddress: "127.0.0.1:0",
				}),
				WithFullPotRewards(true),
			}
			opts = append(opts, tt.opts...)
			n, err := New(NewConfig(opts...))
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			// New starts the event bus' background goroutines; Stop releases them.
			t.Cleanup(func() { _ = n.Stop() })
		})
	}
}

func TestWithDelegatorInactivity(t *testing.T) {
	cfg := &Config{}
	WithDelegatorInactivity(true, 90)(cfg)
	assert.True(t, cfg.delegatorInactivityEnabled)
	assert.Equal(t, uint64(90), cfg.delegatorInactivity)
	WithDelegatorInactivity(false, 0)(cfg)
	assert.False(t, cfg.delegatorInactivityEnabled)
	assert.Zero(t, cfg.delegatorInactivity)
}

func TestExperimentalDijkstraEnabled(t *testing.T) {
	tests := []struct {
		name     string
		cfg      Config
		expected bool
	}{
		{name: "default", cfg: Config{}, expected: false},
		{
			name: "leios run mode",
			cfg: Config{cfg: &internalconfig.Config{
				RunMode: internalconfig.RunModeLeios,
			}},
			expected: true,
		},
		{
			name: "dijkstra start era",
			cfg: Config{cfg: &internalconfig.Config{
				StartEra: internalconfig.StartEraDijkstra,
			}},
			expected: true,
		},
		{
			name: "leios and dijkstra",
			cfg: Config{cfg: &internalconfig.Config{
				RunMode:  internalconfig.RunModeLeios,
				StartEra: internalconfig.StartEraDijkstra,
			},
			},
			expected: true,
		},
		{
			// `dingo -n musashi` sets the network name but leaves run
			// mode at its default; the Musashi testnet still requires the
			// Dijkstra era table to follow the chain.
			name: "musashi network by name",
			cfg: Config{cfg: &internalconfig.Config{
				Network: "musashi",
			}},
			expected: true,
		},
		{
			// Same network selected via its magic (e.g. --network-magic
			// 164) with no network name.
			name: "musashi network by magic",
			cfg: Config{cfg: &internalconfig.Config{
				NetworkMagic: 164,
			}},
			expected: true,
		},
		{
			name: "non-musashi network stays disabled",
			cfg: Config{cfg: &internalconfig.Config{
				Network:      "preview",
				NetworkMagic: 2,
			}},
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(
				t,
				tt.expected,
				tt.cfg.experimentalDijkstraEnabled(),
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
		cfg               Config
		expectNetworking  bool
		expectDijkstraEra bool
	}{
		{
			name:              "default",
			cfg:               Config{},
			expectNetworking:  false,
			expectDijkstraEra: false,
		},
		{
			name: "leios run mode enables both",
			cfg: Config{cfg: &internalconfig.Config{
				RunMode: internalconfig.RunModeLeios,
			}},
			expectNetworking:  true,
			expectDijkstraEra: true,
		},
		{
			name: "dijkstra start era enables both",
			cfg: Config{cfg: &internalconfig.Config{
				StartEra: internalconfig.StartEraDijkstra,
			},
			},
			expectNetworking:  true,
			expectDijkstraEra: true,
		},
		{
			// `dingo -n musashi`: the Musashi testnet enables both the
			// Dijkstra era and the Leios mini-protocols (leios-notify /
			// leios-fetch).
			name: "musashi network enables both",
			cfg: Config{cfg: &internalconfig.Config{
				Network: "musashi",
			}},
			expectNetworking:  true,
			expectDijkstraEra: true,
		},
		{
			name: "musashi network by magic enables both",
			cfg: Config{cfg: &internalconfig.Config{
				NetworkMagic: 164,
			}},
			expectNetworking:  true,
			expectDijkstraEra: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(
				t,
				tt.expectNetworking,
				tt.cfg.experimentalLeiosNetworkingEnabled(),
				"leios networking",
			)
			assert.Equal(
				t,
				tt.expectDijkstraEra,
				tt.cfg.experimentalDijkstraEnabled(),
				"dijkstra era",
			)
		})
	}
}

func TestPeerGovernorOptionsIgnoreNonPositiveValues(t *testing.T) {
	cfg := &Config{cfg: &internalconfig.Config{}}

	WithMinHotPeers(-1)(cfg)
	WithReconcileInterval(-1 * time.Minute)(cfg)
	WithInactivityTimeout(-5 * time.Minute)(cfg)
	WithMaxConnectionsPerIP(-2)(cfg)
	WithMaxInboundConns(0)(cfg)

	assert.Zero(t, cfg.cfg.MinHotPeers)
	assert.Zero(t, cfg.cfg.ReconcileInterval)
	assert.Zero(t, cfg.cfg.InactivityTimeout)
	assert.Zero(t, cfg.cfg.MaxConnectionsPerIP)
	assert.Zero(t, cfg.cfg.MaxInboundConns)
}

func TestPeerGovernorOptionsApplyPositiveValues(t *testing.T) {
	cfg := &Config{cfg: &internalconfig.Config{}}

	WithMinHotPeers(3)(cfg)
	WithReconcileInterval(30 * time.Second)(cfg)
	WithInactivityTimeout(2 * time.Minute)(cfg)
	WithMaxConnectionsPerIP(4)(cfg)
	WithMaxInboundConns(25)(cfg)

	assert.Equal(t, 3, cfg.cfg.MinHotPeers)
	assert.Equal(t, 30*time.Second, cfg.cfg.ReconcileInterval)
	assert.Equal(t, 2*time.Minute, cfg.cfg.InactivityTimeout)
	assert.Equal(t, 4, cfg.cfg.MaxConnectionsPerIP)
	assert.Equal(t, 25, cfg.cfg.MaxInboundConns)
}

// TestWithGenesisCorroborationPeers covers the public programmatic API path for
// the Genesis corroboration threshold. A negative value is stored as-is on the
// Config; the chain selector fails closed on it (clamps to 1) rather than
// disabling the security gate — see chainselection.NewChainSelector and
// TestGenesisNegativeCorroborationFailsClosed. node.go passes this field to
// ChainSelectorConfig.MinCorroboratingPeers.
func TestWithGenesisCorroborationPeers(t *testing.T) {
	cfg := &Config{cfg: &internalconfig.Config{}}
	WithGenesisCorroborationPeers(3)(cfg)
	assert.Equal(t, 3, cfg.cfg.GenesisBootstrap.CorroborationPeers)

	WithGenesisCorroborationPeers(0)(cfg)
	assert.Zero(t, cfg.cfg.GenesisBootstrap.CorroborationPeers)

	WithGenesisCorroborationPeers(-1)(cfg)
	assert.Equal(t, -1, cfg.cfg.GenesisBootstrap.CorroborationPeers)
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
	require.NotNil(
		t,
		n.rtsMetrics,
		"registerRTSMetrics must populate n.rtsMetrics",
	)

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
	cfg := &Config{cfg: &internalconfig.Config{}}
	assert.Equal(t, "", cfg.cfg.LeiosVoteSigningKeyFile)
	WithLeiosVoteSigningKeyFile("/keys/leios-vote.skey")(cfg)
	assert.Equal(t, "/keys/leios-vote.skey", cfg.cfg.LeiosVoteSigningKeyFile)
}

func TestWithLeiosVoterPublicKeys(t *testing.T) {
	cfg := &Config{cfg: &internalconfig.Config{}}
	assert.Nil(t, cfg.cfg.LeiosVoterPublicKeys)
	keys := map[string]string{"aabbcc": "ddeeff"}
	WithLeiosVoterPublicKeys(keys)(cfg)
	assert.Equal(
		t,
		map[string]string{"aabbcc": "ddeeff"},
		cfg.cfg.LeiosVoterPublicKeys,
	)
	// The option copies the map: later caller mutations must not
	// change live config
	keys["aabbcc"] = "mutated"
	assert.Equal(t, "ddeeff", cfg.cfg.LeiosVoterPublicKeys["aabbcc"])
}
