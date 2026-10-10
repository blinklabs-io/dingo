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
	"net/http"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/promutil"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestForgeStalenessBoundsAreOperatorTunable covers the config plumbing for
// the three opt-in forge staleness bounds.
//
// Each hop is asserted separately, because a break in any one of them leaves
// the knob silently inert: the loaded config carries the value, the node
// Config snapshot copies it out of the loaded config, and the accessor
// reports it.
//
// IMPORTANT: this covers the NewConfigFromInternal path only. The binary does
// NOT take it -- internal/node.buildDingoConfig composes dingo.Config via
// dingo.NewConfig from an explicit With... list, and a field missing from that
// list is dropped no matter how green this test is. The runtime composition
// path is covered by TestBuildDingoConfigWiresForgeTolerances in
// internal/node; presence of a field at each layer is not wiring.
func TestForgeStalenessBoundsAreOperatorTunable(t *testing.T) {
	t.Run("explicit values survive every hop", func(t *testing.T) {
		loaded := &internalconfig.Config{
			ForgeUpstreamStalenessSlots:      41,
			ForgeAppliedTipStalenessSlots:    42,
			ForgeEndorserBlockStalenessSlots: 43,
		}
		c := &Config{cfg: loaded}
		// syncCompatFields is what the loaded-config constructor runs to
		// project the parsed config onto the fields the node reads.
		c.syncCompatFields()

		require.Equal(t, uint64(41), c.ForgeUpstreamStalenessSlots())
		require.Equal(t, uint64(41), c.forgeUpstreamStalenessSlots)
		require.Equal(t, uint64(42), c.ForgeAppliedTipStalenessSlots())
		require.Equal(t, uint64(42), c.forgeAppliedTipStalenessSlots)
		require.Equal(t, uint64(43), c.ForgeEndorserBlockStalenessSlots())
		require.Equal(
			t,
			uint64(43),
			c.forgeEndorserBlockStalenessSlots,
			"the node Config snapshot the forger reads must carry it",
		)
	})

	t.Run("option funcs set them", func(t *testing.T) {
		c := NewConfig(
			WithForgeUpstreamStalenessSlots(11),
			WithForgeAppliedTipStalenessSlots(12),
			WithForgeEndorserBlockStalenessSlots(13),
		)
		require.Equal(t, uint64(11), c.ForgeUpstreamStalenessSlots())
		require.Equal(t, uint64(12), c.ForgeAppliedTipStalenessSlots())
		require.Equal(t, uint64(13), c.ForgeEndorserBlockStalenessSlots())

		c.syncCompatFields()
		require.Equal(t, uint64(11), c.forgeUpstreamStalenessSlots)
		require.Equal(t, uint64(12), c.forgeAppliedTipStalenessSlots)
		require.Equal(t, uint64(13), c.forgeEndorserBlockStalenessSlots)
	})

	// All three are opt-in. ApplyDefaults must leave them at 0, because 0
	// means "disabled" for them rather than "unset": a default-on bound on any
	// of the three refuses leader slots during ordinary operation.
	t.Run("defaults leave every bound disabled", func(t *testing.T) {
		loaded := internalconfig.Config{}
		loaded.ApplyDefaults()

		require.Zero(t, loaded.ForgeUpstreamStalenessSlots)
		require.Zero(t, loaded.ForgeAppliedTipStalenessSlots)
		require.Zero(
			t,
			loaded.ForgeEndorserBlockStalenessSlots,
			"the endorser-block bound gates a network-stage watermark "+
				"against the local applied tip; defaulting it on would "+
				"withhold leader slots with every local indicator healthy",
		)
		require.Zero(
			t,
			uint64(internalconfig.DefaultForgeEndorserBlockStalenessSlots),
			"the documented default must not drift silently",
		)
	})

	t.Run(
		"explicit values are not overwritten by defaults",
		func(t *testing.T) {
			loaded := internalconfig.Config{
				ForgeUpstreamStalenessSlots:      7,
				ForgeAppliedTipStalenessSlots:    8,
				ForgeEndorserBlockStalenessSlots: 9,
			}
			loaded.ApplyDefaults()

			require.Equal(t, uint64(7), loaded.ForgeUpstreamStalenessSlots)
			require.Equal(t, uint64(8), loaded.ForgeAppliedTipStalenessSlots)
			require.Equal(t, uint64(9), loaded.ForgeEndorserBlockStalenessSlots)
		},
	)
}

// TestConfigValidateRejectsByronNetworkMagicMismatch pins that
// a loaded genesis network magic must be cross-checked against
// the requested network. configValidate already cross-checks the Shelley
// genesis's NetworkMagic against the configured/requested network magic;
// this proves the same cross-check applies to the Byron genesis's own
// ProtocolConsts.ProtocolMagic field, which previously loaded and was used
// for Byron-era validation without ever being compared against the
// configured network.
func TestConfigValidateRejectsByronNetworkMagicMismatch(t *testing.T) {
	const shelleyMagic = 42

	shelleyGenesisJSON := `{
		"networkMagic": ` + strconv.Itoa(shelleyMagic) + `,
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"slotsPerKESPeriod": 129600,
		"maxKESEvolutions": 62,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	tests := []struct {
		name               string
		byronProtocolMagic int
		wantErr            string
	}{
		{
			name:               "mismatched Byron protocol magic is rejected",
			byronProtocolMagic: shelleyMagic + 1,
			wantErr:            "doesn't match value from Byron genesis",
		},
		{
			name:               "matching Byron protocol magic is accepted",
			byronProtocolMagic: shelleyMagic,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			byronGenesisJSON := `{
				"avvmDistr": {},
				"blockVersionData": {
					"heavyDelThd":"300000000000","maxBlockSize":"2000000",
					"maxHeaderSize":"2000000","maxProposalSize":"700",
					"maxTxSize":"4096","mpcThd":"20000000000000",
					"scriptVersion":0,"slotDuration":"20000",
					"softforkRule":{"initThd":"900000000000000","minThd":"600000000000000","thdDecrement":"50000000000000"},
					"txFeePolicy":{"multiplier":"43946000000","summand":"155381000000000"},
					"unlockStakeEpoch":"18446744073709551615","updateImplicit":"10000",
					"updateProposalThd":"100000000000000","updateVoteThd":"1000000000000"
				},
				"startTime": 1666656000,
				"bootStakeholders": {}, "heavyDelegation": {}, "nonAvvmBalances": {},
				"protocolConsts": {"k": 108, "protocolMagic": ` + strconv.Itoa(
				tt.byronProtocolMagic,
			) + `}
			}`

			nodeCfg := &cardano.CardanoNodeConfig{}
			require.NoError(
				t,
				nodeCfg.LoadShelleyGenesisFromReader(
					strings.NewReader(shelleyGenesisJSON),
				),
			)
			require.NoError(
				t,
				nodeCfg.LoadByronGenesisFromReader(
					strings.NewReader(byronGenesisJSON),
				),
			)

			cfg := NewConfig(
				WithPrometheusRegistry(prometheus.NewRegistry()),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp",
					ListenAddress: "127.0.0.1:0",
				}),
				WithNetworkMagic(shelleyMagic),
				WithCardanoNodeConfig(nodeCfg),
			)
			n, err := New(cfg)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { _ = n.Stop() })
		})
	}
}

// TestConfigPopulateNetworkMagicSyncsCompatField is a regression test for the
// devnet handshake blocker: a config built by network NAME (magic left 0, the
// production path from internal/node) must resolve the compat networkMagic
// field that the ouroboros handshake reads. configPopulateNetworkMagic resolves
// the canonical cfg.NetworkMagic, but the handshake reads Config.networkMagic
// (set only by syncCompatFields at construction time, before the name was
// resolved). If the compat field stays 0, gouroboros refuses every connection
// with "invalid network magic value provided: 0". This affects any network
// started by name (devnet, preview, ...), not just devnet.
func TestConfigPopulateNetworkMagicSyncsCompatField(t *testing.T) {
	t.Parallel()

	cases := []struct {
		network string
		want    uint32
	}{
		{"devnet", 42},
		{"preview", 2},
	}
	for _, tc := range cases {
		t.Run(tc.network, func(t *testing.T) {
			cfg := NewConfig(WithNetwork(tc.network))
			n := &Node{config: cfg}
			if err := n.configPopulateNetworkMagic(); err != nil {
				t.Fatalf("configPopulateNetworkMagic: %v", err)
			}
			if got := n.config.cfg.NetworkMagic; got != tc.want {
				t.Fatalf("cfg.NetworkMagic = %d, want %d", got, tc.want)
			}
			// The compat field the ouroboros handshake actually reads.
			if got := n.config.networkMagic; got != tc.want {
				t.Fatalf(
					"config.networkMagic = %d, want %d (handshake would get 0)",
					got, tc.want,
				)
			}
		})
	}
}

// This checks configuration acceptance, not socket binding. The removed
// startup gate rejected these configurations; Node.Run binding has separate
// provider-dependency coverage in TestNodeRunPublicAPIsUseSharedBindAddress.
func TestProgrammaticPublicAPIConfigAcceptsRemoteAddresses(t *testing.T) {
	t.Parallel()
	for _, bind := range []string{"0.0.0.0", "::", "192.0.2.10"} {
		t.Run(bind, func(t *testing.T) {
			cfg := NewConfig(
				WithStorageMode(StorageModeAPI),
				WithBindAddr(bind),
				WithNetworkMagic(42),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp",
					ListenAddress: "127.0.0.1:0",
				}),
			)
			node := &Node{config: cfg}
			require.NoError(t, node.configValidate())
		})
	}
}

func TestStorageModeValid(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	assert.False(t, StorageModeCore.IsAPI())
	assert.True(t, StorageModeAPI.IsAPI())
}

func TestWithStorageMode(t *testing.T) {
	t.Parallel()

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

// TestWithRootPeerTarget verifies the public option preserves default,
// explicit, and unlimited root-peer target representations.
func TestWithRootPeerTarget(t *testing.T) {
	t.Parallel()

	for _, target := range []int{0, 12, -1} {
		cfg := NewConfig(WithRootPeerTarget(target))
		if got := cfg.TargetNumberOfRootPeers(); got != target {
			t.Fatalf("expected root-peer target %d, got %d", target, got)
		}
	}
}

func TestNewConfigMempoolCapacityDefaultsFromRunMode(t *testing.T) {
	t.Parallel()

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

func TestNewConfigPublicBindAddressDefaultsToWildcard(t *testing.T) {
	cfg := NewConfig(
		WithRunMode(string(internalconfig.RunModeDev)),
	)

	assert.Equal(t, "0.0.0.0", cfg.BindAddr())
	assert.Equal(t, cfg.BindAddr(), cfg.bindAddr)

	cfg = NewConfig(WithBindAddr("192.0.2.10"))
	assert.Equal(t, "192.0.2.10", cfg.BindAddr())
	assert.Equal(t, "192.0.2.10", cfg.bindAddr)
}

func TestNewConfigPreservesExplicitMempoolCapacity(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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

// TestNewConfigDefaultsValidationFlags ensures programmatic configuration
// preserves the fail-closed validation defaults of the standard config loader.
func TestNewConfigDefaultsValidationFlags(t *testing.T) {
	cfg := NewConfig()
	assert.True(t, cfg.cfg.ValidateHistorical)
	assert.True(t, cfg.validateHistorical)
	assert.True(t, cfg.cfg.StrictUtxoValidation)
	assert.True(t, cfg.strictUtxoValidation)
	assert.True(t, cfg.cfg.ValidateForgedBlock)
	assert.True(t, cfg.validateForgedBlock)
}

func TestWithPluginSelectionSnapshotsConfig(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	cfg := &Config{}
	midnightCfg := MidnightConfig{
		Enabled:                     true,
		ServerEnabled:               true,
		ReflectionEnabled:           true,
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
	t.Parallel()

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
	t.Parallel()

	src := internalconfig.MidnightConfig{
		Enabled:                     true,
		ServerEnabled:               true,
		ReflectionEnabled:           true,
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
	t.Parallel()

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
	t.Parallel()

	cfg := &Config{}
	WithFullPotRewards(true)(cfg)
	assert.True(t, cfg.fullPotRewardsEnabled)
	WithFullPotRewards(false)(cfg)
	assert.False(t, cfg.fullPotRewardsEnabled)
}

func TestFullPotRewardsStandardNetworkValidation(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	cfg := &Config{}
	WithDelegatorInactivity(true, 90)(cfg)
	assert.True(t, cfg.delegatorInactivityEnabled)
	assert.Equal(t, uint64(90), cfg.delegatorInactivity)
	WithDelegatorInactivity(false, 0)(cfg)
	assert.False(t, cfg.delegatorInactivityEnabled)
	assert.Zero(t, cfg.delegatorInactivity)
}

func TestExperimentalDijkstraEnabled(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	cfg := &Config{cfg: &internalconfig.Config{}}

	WithMinHotPeers(-1)(cfg)
	WithReconcileInterval(-1 * time.Minute)(cfg)
	WithInactivityTimeout(-5 * time.Minute)(cfg)
	WithMaxConnectionsPerIP(-2)(cfg)
	WithMaxInboundConns(0)(cfg)
	WithMaxNtCConns(-3)(cfg)
	WithMaxNtCConnectionsPerIP(0)(cfg)
	WithMaxTrustedLocalNtCConns(-1)(cfg)

	assert.Zero(t, cfg.cfg.MinHotPeers)
	assert.Zero(t, cfg.cfg.ReconcileInterval)
	assert.Zero(t, cfg.cfg.InactivityTimeout)
	assert.Zero(t, cfg.cfg.MaxConnectionsPerIP)
	assert.Zero(t, cfg.cfg.MaxInboundConns)
	assert.Zero(t, cfg.cfg.MaxNtCConns)
	assert.Zero(t, cfg.cfg.MaxNtCConnectionsPerIP)
	assert.Zero(t, cfg.cfg.MaxTrustedLocalNtCConns)
}

func TestPeerGovernorOptionsApplyPositiveValues(t *testing.T) {
	t.Parallel()

	cfg := &Config{cfg: &internalconfig.Config{}}

	WithMinHotPeers(3)(cfg)
	WithReconcileInterval(30 * time.Second)(cfg)
	WithInactivityTimeout(2 * time.Minute)(cfg)
	WithMaxConnectionsPerIP(4)(cfg)
	WithMaxInboundConns(25)(cfg)
	WithMaxNtCConns(30)(cfg)
	WithMaxNtCConnectionsPerIP(6)(cfg)
	WithMaxTrustedLocalNtCConns(8)(cfg)

	assert.Equal(t, 3, cfg.cfg.MinHotPeers)
	assert.Equal(t, 30*time.Second, cfg.cfg.ReconcileInterval)
	assert.Equal(t, 2*time.Minute, cfg.cfg.InactivityTimeout)
	assert.Equal(t, 4, cfg.cfg.MaxConnectionsPerIP)
	assert.Equal(t, 25, cfg.cfg.MaxInboundConns)
	assert.Equal(t, 30, cfg.cfg.MaxNtCConns)
	assert.Equal(t, 6, cfg.cfg.MaxNtCConnectionsPerIP)
	assert.Equal(t, 8, cfg.cfg.MaxTrustedLocalNtCConns)

	cfg.syncCompatFields()
	assert.Equal(t, 30, cfg.maxNtCConns)
	assert.Equal(t, 6, cfg.maxNtCConnectionsPerIP)
	assert.Equal(t, 8, cfg.maxTrustedLocalNtCConns)
}

func TestWithSkipRewardLiveStakeBackfillCheck(t *testing.T) {
	t.Parallel()
	cfg := &Config{cfg: &internalconfig.Config{}}
	WithSkipRewardLiveStakeBackfillCheck(true)(cfg)
	assert.True(t, cfg.cfg.SkipRewardLiveStakeBackfillCheck)
	cfg.syncCompatFields()
	assert.True(t, cfg.skipRewardLiveStakeBackfillCheck)
}

// TestWithGenesisCorroborationPeers covers the public programmatic API path for
// the Genesis corroboration threshold. A negative value is stored as-is on the
// Config; the chain selector fails closed on it (clamps to 1) rather than
// disabling the security gate — see chainselection.NewChainSelector and
// TestGenesisNegativeCorroborationFailsClosed. node.go passes this field to
// ChainSelectorConfig.MinCorroboratingPeers.
func TestWithGenesisCorroborationPeers(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	reg := prometheus.NewRegistry()
	n := &Node{config: Config{promRegistry: reg}}
	n.registerRTSMetrics(promutil.NewRegistration(reg))
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
	t.Parallel()

	cfg := &Config{cfg: &internalconfig.Config{}}
	assert.Equal(t, "", cfg.cfg.LeiosVoteSigningKeyFile)
	WithLeiosVoteSigningKeyFile("/keys/leios-vote.skey")(cfg)
	assert.Equal(t, "/keys/leios-vote.skey", cfg.cfg.LeiosVoteSigningKeyFile)
}

// TestWithKoiosParityAccountsNilDefaultsToEnabled locks in
// KoiosParityConfig.Accounts's *bool semantics: an unset (nil) Accounts
// pointer must default to enabled (true), matching
// internalconfig.DefaultKoiosParityConfig's own Accounts: true default. A
// plain bool field here would make "caller never set this" indistinguishable
// from an explicit opt-out, silently disabling per-account checking.
func TestWithKoiosParityAccountsNilDefaultsToEnabled(t *testing.T) {
	t.Parallel()

	cfg := NewConfig(WithKoiosParity(KoiosParityConfig{
		Enabled:  true,
		Accounts: nil,
	}))

	assert.True(
		t,
		cfg.cfg.KoiosParity.Accounts,
		"a nil Accounts pointer must resolve to enabled",
	)
	require.NotNil(
		t,
		cfg.koiosParity.Accounts,
		"syncCompatFields must always mirror a non-nil Accounts pointer",
	)
	assert.True(t, *cfg.koiosParity.Accounts)
}

// TestWithKoiosParityAccountsExplicitFalseDisablesEndToEnd proves an explicit
// pointer-to-false actually disables per-account checking end to
// end: through WithKoiosParity's resolution into the internal config
// (internalconfig.KoiosParityConfig.Accounts, a plain bool), and through
// syncCompatFields's mirror back into the exported root
// KoiosParityConfig.Accounts *bool that
// node_koiosparity.go's startKoiosParityObserver reads (with its own
// defensive nil-check, per KoiosParityConfig's doc comment).
func TestWithKoiosParityAccountsExplicitFalseDisablesEndToEnd(t *testing.T) {
	t.Parallel()

	disabled := false
	cfg := NewConfig(WithKoiosParity(KoiosParityConfig{
		Enabled:  true,
		Accounts: &disabled,
	}))

	assert.False(t, cfg.cfg.KoiosParity.Accounts)
	require.NotNil(t, cfg.koiosParity.Accounts)
	assert.False(t, *cfg.koiosParity.Accounts)
}

// TestTokenRegistryConfigReachesRuntimeFromYAML covers the path YAML, env, and
// CLI all land on: internal config only. The syncer reads the runtime mirror,
// so without syncCompatFields carrying it across, an operator's tokenRegistry
// block would parse and then be silently ignored.
func TestTokenRegistryConfigReachesRuntimeFromYAML(t *testing.T) {
	t.Parallel()

	cfg, err := NewConfigFromInternal(
		&internalconfig.Config{
			TokenRegistry: internalconfig.TokenRegistryConfig{
				Enabled:               true,
				SourceURL:             "https://mirror.example.test/reg.tar.gz",
				Interval:              2 * time.Hour,
				RequestTimeout:        9 * time.Minute,
				UserAgent:             "custom-agent/9",
				HeaderSecrets:         map[string]string{"Authorization": "Bearer x"},
				MaxBytes:              123,
				MaxDecompressedBytes:  456,
				MaxEntryBytes:         45,
				MaxArchiveEntries:     67,
				MaxAcceptedEntries:    34,
				MaxBatchBytes:         89,
				StoreLogos:            true,
				AllowPrivateAddresses: true,
			},
		},
		nil, nil, nil, nil,
	)
	require.NoError(t, err)

	require.True(t, cfg.tokenRegistry.Enabled)
	require.Equal(
		t,
		"https://mirror.example.test/reg.tar.gz",
		cfg.tokenRegistry.SourceURL,
	)
	require.Equal(t, 2*time.Hour, cfg.tokenRegistry.Interval)
	require.Equal(t, 9*time.Minute, cfg.tokenRegistry.RequestTimeout)
	require.Equal(t, "custom-agent/9", cfg.tokenRegistry.UserAgent)
	require.Equal(
		t,
		map[string]string{"Authorization": "Bearer x"},
		cfg.tokenRegistry.Headers,
	)
	require.Equal(t, int64(123), cfg.tokenRegistry.MaxBytes)
	require.Equal(t, int64(456), cfg.tokenRegistry.MaxDecompressedBytes)
	require.Equal(t, int64(45), cfg.tokenRegistry.MaxEntryBytes)
	require.Equal(t, 67, cfg.tokenRegistry.MaxArchiveEntries)
	require.Equal(t, 34, cfg.tokenRegistry.MaxAcceptedEntries)
	require.Equal(t, int64(89), cfg.tokenRegistry.MaxBatchBytes)
	require.True(t, cfg.tokenRegistry.StoreLogos)
	require.True(t, cfg.tokenRegistry.AllowPrivateAddresses)
}

// TestTokenRegistryConfigDisabledByDefault pins the deliberate default: the
// mainnet registry is a roughly 240MB download, so an upgrade must not start
// one on its own.
func TestTokenRegistryConfigDisabledByDefault(t *testing.T) {
	t.Parallel()

	cfg := NewConfig()

	require.False(t, cfg.tokenRegistry.Enabled)
	require.False(t, cfg.TokenRegistry().Enabled)
}

// TestWithTokenRegistryConfigPreservesHTTPClient guards the one field that
// cannot round-trip through internal config: the programmatic HTTP client is
// runtime-only, and syncCompatFields runs after options are applied.
func TestWithTokenRegistryConfigPreservesHTTPClient(t *testing.T) {
	t.Parallel()

	client := &http.Client{}

	cfg := NewConfig(WithTokenRegistryConfig(TokenRegistryConfig{
		Enabled:              true,
		HTTPClient:           client,
		UserAgent:            "programmatic/1",
		MaxDecompressedBytes: 456,
		MaxArchiveEntries:    67,
		MaxAcceptedEntries:   34,
		MaxBatchBytes:        89,
	}))

	require.Same(t, client, cfg.tokenRegistry.HTTPClient)
	require.True(t, cfg.tokenRegistry.Enabled)
	require.Equal(t, "programmatic/1", cfg.tokenRegistry.UserAgent)
	require.Equal(t, "programmatic/1", cfg.TokenRegistry().UserAgent)
	require.Equal(t, int64(456), cfg.TokenRegistry().MaxDecompressedBytes)
	require.Equal(t, 67, cfg.TokenRegistry().MaxArchiveEntries)
	require.Equal(t, 34, cfg.TokenRegistry().MaxAcceptedEntries)
	require.Equal(t, int64(89), cfg.TokenRegistry().MaxBatchBytes)
}

func TestNewConfigPlannerStatsRefreshDefaultsOnAndCanBeDisabled(t *testing.T) {
	t.Parallel()
	cfg := NewConfig()
	assert.True(t, cfg.plannerStatsRefreshEnabled())
	off := NewConfig(WithPlannerStatsRefresh(false))
	assert.False(t, off.plannerStatsRefreshEnabled())
	// A hand-built Config has no internal config and must not start the
	// refresh.
	assert.False(t, (&Config{}).plannerStatsRefreshEnabled())
}
