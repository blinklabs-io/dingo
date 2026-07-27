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
	"errors"
	"io/fs"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/api/blockfrost"
	"github.com/blinklabs-io/dingo/api/mesh"
	"github.com/blinklabs-io/dingo/api/utxorpc"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type apiProbeConfig struct {
	Port uint `yaml:"port"`
}

type apiLifecycleProbe struct {
	starts   atomic.Int32
	stops    atomic.Int32
	startErr error
}

func (p *apiLifecycleProbe) instance() plugin.Instance {
	return plugin.Lifecycle{
		StartFunc: func(context.Context) error {
			p.starts.Add(1)
			return p.startErr
		},
		StopFunc: func(context.Context) error {
			p.stops.Add(1)
			return nil
		},
	}
}

func registerAPIProbe(
	t *testing.T,
	host *plugin.Host,
	capability plugin.Capability,
	name string,
	probe *apiLifecycleProbe,
) {
	t.Helper()
	descriptor := plugin.Descriptor{Capability: capability, Name: name}
	var err error
	switch capability {
	case plugin.CapabilityAPIUtxorpc:
		err = plugin.Register(
			host,
			descriptor,
			func() apiProbeConfig { return apiProbeConfig{} },
			func(
				context.Context,
				apiProbeConfig,
				utxorpc.ProviderDependencies,
			) (string, plugin.Instance, error) {
				return name, probe.instance(), nil
			},
		)
	case plugin.CapabilityAPIBlockfrost:
		err = plugin.Register(
			host,
			descriptor,
			func() apiProbeConfig { return apiProbeConfig{} },
			func(
				context.Context,
				apiProbeConfig,
				blockfrost.ProviderDependencies,
			) (string, plugin.Instance, error) {
				return name, probe.instance(), nil
			},
		)
	case plugin.CapabilityAPIMesh:
		err = plugin.Register(
			host,
			descriptor,
			func() apiProbeConfig { return apiProbeConfig{} },
			func(
				context.Context,
				apiProbeConfig,
				mesh.ProviderDependencies,
			) (string, plugin.Instance, error) {
				return name, probe.instance(), nil
			},
		)
	default:
		t.Fatalf("unsupported API capability %s", capability)
	}
	require.NoError(t, err)
}

func newAPIPluginRuntimeNode(t *testing.T) *Node {
	t.Helper()
	cardanoCfg := newNodeTestCardanoNodeCfg(t)
	n, err := New(NewConfig(
		WithDatabasePath(t.TempDir()),
		WithCardanoNodeConfig(cardanoCfg),
		WithNetworkMagic(cardanoCfg.ShelleyGenesis().NetworkMagic),
		WithPrometheusRegistry(prometheus.NewRegistry()),
		WithStorageMode(StorageModeAPI),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
		WithMidnightConfig(MidnightConfig{Port: 0}),
		WithShutdownTimeout(5*time.Second),
	))
	require.NoError(t, err)
	// New validates the production requirement for at least one listener.
	// Runtime plugin tests do not need a network socket, so remove it after
	// validation to keep these tests hermetic in restricted environments.
	n.config.listeners = nil
	return n
}

func selectAPIProbe(
	n *Node,
	capability plugin.Capability,
	name string,
	port uint,
) {
	n.config.pluginSelections[capability] = plugin.Selection{
		Provider: name,
		Config:   map[string]any{"port": port},
	}
}

// newAPISelectionNode builds a Node whose only plugin selection is sel under
// the Blockfrost API capability, for exercising apiPluginSelection.
func newAPISelectionNode(sel plugin.Selection) *Node {
	return &Node{
		config: Config{
			pluginSelections: map[plugin.Capability]plugin.Selection{
				plugin.CapabilityAPIBlockfrost: sel,
			},
		},
	}
}

// TestAPIPluginSelectionPortDecoding covers apiPluginSelection's port decoder.
// The port arrives inside a map[string]any and can be produced by YAML decode
// (int/float64), the environment compatibility shim (uint64), or an in-code
// selection (uint), so every accepted numeric type and every guard is checked.
func TestAPIPluginSelectionPortDecoding(t *testing.T) {
	t.Run("uses capability default when port absent", func(t *testing.T) {
		n := newAPISelectionNode(
			plugin.Selection{Provider: "builtin", Config: map[string]any{}},
		)
		_, port, err := n.apiPluginSelection(plugin.CapabilityAPIBlockfrost)
		require.NoError(t, err)
		assert.Equal(t, uint(3000), port)
	})

	t.Run("uses capability default when config is nil", func(t *testing.T) {
		n := newAPISelectionNode(plugin.Selection{Provider: "builtin"})
		_, port, err := n.apiPluginSelection(plugin.CapabilityAPIBlockfrost)
		require.NoError(t, err)
		assert.Equal(t, uint(3000), port)
	})

	accepted := []struct {
		name  string
		value any
		want  uint
	}{
		{"int", int(3100), 3100},
		{"uint", uint(3101), 3101},
		{"uint64", uint64(3102), 3102},
		{"int64", int64(3103), 3103},
		{"float64", float64(3104), 3104},
		{"zero (disables the API)", int(0), 0},
		{"max port", int(65535), 65535},
		{"int64 max port", int64(65535), 65535},
	}
	for _, tc := range accepted {
		t.Run("accepts port as "+tc.name, func(t *testing.T) {
			n := newAPISelectionNode(plugin.Selection{
				Provider: "builtin",
				Config:   map[string]any{"port": tc.value},
			})
			_, port, err := n.apiPluginSelection(
				plugin.CapabilityAPIBlockfrost,
			)
			require.NoError(t, err)
			assert.Equal(t, tc.want, port)
		})
	}

	rejected := []struct {
		name  string
		value any
	}{
		{"negative int", int(-1)},
		{"negative int64", int64(-1)},
		{"negative float64", float64(-1)},
		{"fractional float64", float64(3000.5)},
		{"int above 65535", int(65536)},
		{"uint64 above 65535", uint64(70000)},
		{"string", "3000"},
		{"bool", true},
	}
	for _, tc := range rejected {
		t.Run("rejects port as "+tc.name, func(t *testing.T) {
			n := newAPISelectionNode(plugin.Selection{
				Provider: "builtin",
				Config:   map[string]any{"port": tc.value},
			})
			_, _, err := n.apiPluginSelection(
				plugin.CapabilityAPIBlockfrost,
			)
			require.Error(t, err)
		})
	}
}

// TestAPIPluginSelectionErrors covers the selection-level guards: an empty
// provider and a capability absent from the selection map are both errors.
func TestAPIPluginSelectionErrors(t *testing.T) {
	t.Run("empty provider is rejected", func(t *testing.T) {
		n := newAPISelectionNode(plugin.Selection{
			Provider: "",
			Config:   map[string]any{"port": 3000},
		})
		_, _, err := n.apiPluginSelection(plugin.CapabilityAPIBlockfrost)
		require.Error(t, err)
	})

	t.Run("missing capability is rejected", func(t *testing.T) {
		n := &Node{
			config: Config{
				pluginSelections: map[plugin.Capability]plugin.Selection{},
			},
		}
		_, _, err := n.apiPluginSelection(plugin.CapabilityAPIBlockfrost)
		require.Error(t, err)
	})
}

// TestAPIPluginSelectionDefaultPortPerCapability verifies each API capability
// falls back to its own default port when no port is configured.
func TestAPIPluginSelectionDefaultPortPerCapability(t *testing.T) {
	want := map[plugin.Capability]uint{
		plugin.CapabilityAPIBlockfrost: 3000,
		plugin.CapabilityAPIMesh:       8080,
		plugin.CapabilityAPIUtxorpc:    9090,
	}
	for capability, wantPort := range want {
		n := &Node{
			config: Config{
				pluginSelections: map[plugin.Capability]plugin.Selection{
					capability: {
						Provider: "builtin",
						Config:   map[string]any{},
					},
				},
			},
		}
		_, port, err := n.apiPluginSelection(capability)
		require.NoErrorf(t, err, "capability %s", capability)
		assert.Equalf(t, wantPort, port, "capability %s", capability)
	}
}

func TestNodeRunSkipsZeroPortAPIProviders(t *testing.T) {
	n := newAPIPluginRuntimeNode(t)
	probes := map[plugin.Capability]*apiLifecycleProbe{
		plugin.CapabilityAPIUtxorpc:    {},
		plugin.CapabilityAPIBlockfrost: {},
		plugin.CapabilityAPIMesh:       {},
	}
	for capability, probe := range probes {
		registerAPIProbe(t, n.pluginHost, capability, "probe", probe)
		selectAPIProbe(n, capability, "probe", 0)
	}
	// Force a deterministic failure after the API startup section so Run
	// returns without requiring an external shutdown signal. Reaching block
	// producer validation proves all three zero-port decisions were exercised.
	n.config.blockProducer = true

	require.ErrorIs(
		t,
		n.Run(context.Background()),
		fs.ErrNotExist,
	)
	for capability, probe := range probes {
		assert.Zero(
			t,
			probe.starts.Load(),
			"capability %s started with port 0",
			capability,
		)
		assert.Zero(
			t,
			probe.stops.Load(),
			"capability %s stopped despite never starting",
			capability,
		)
	}
}

func TestNodeRunAPIStartupFailureCleansUpStartedProviders(t *testing.T) {
	n := newAPIPluginRuntimeNode(t)
	utxorpcProbe := &apiLifecycleProbe{}
	blockfrostProbe := &apiLifecycleProbe{
		startErr: errors.New("injected API startup failure"),
	}
	meshProbe := &apiLifecycleProbe{}
	registerAPIProbe(
		t,
		n.pluginHost,
		plugin.CapabilityAPIUtxorpc,
		"probe",
		utxorpcProbe,
	)
	registerAPIProbe(
		t,
		n.pluginHost,
		plugin.CapabilityAPIBlockfrost,
		"probe",
		blockfrostProbe,
	)
	registerAPIProbe(
		t,
		n.pluginHost,
		plugin.CapabilityAPIMesh,
		"probe",
		meshProbe,
	)
	selectAPIProbe(n, plugin.CapabilityAPIUtxorpc, "probe", 19090)
	selectAPIProbe(n, plugin.CapabilityAPIBlockfrost, "probe", 13000)
	selectAPIProbe(n, plugin.CapabilityAPIMesh, "probe", 0)

	err := n.Run(context.Background())
	require.ErrorContains(t, err, "injected API startup failure")
	assert.Equal(t, int32(1), utxorpcProbe.starts.Load())
	assert.Equal(t, int32(1), utxorpcProbe.stops.Load())
	assert.Equal(t, int32(1), blockfrostProbe.starts.Load())
	assert.Equal(t, int32(1), blockfrostProbe.stops.Load())
	assert.Zero(t, meshProbe.starts.Load())
	assert.Zero(t, meshProbe.stops.Load())
}
