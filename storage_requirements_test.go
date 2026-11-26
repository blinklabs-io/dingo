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
	"testing"

	"github.com/blinklabs-io/dingo/plugin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func storageRequirementsTestConfig(opts ...ConfigOptionFunc) Config {
	return NewConfig(append([]ConfigOptionFunc{
		WithPrometheusRegistry(prometheus.NewRegistry()),
		WithNetworkMagic(1),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp", ListenAddress: "127.0.0.1:0",
		}),
	}, opts...)...)
}

func TestNewRefusesEnabledConsumersAboveStorageTier(t *testing.T) {
	t.Parallel()

	n, err := New(storageRequirementsTestConfig(
		WithStorageMode(StorageModeCore),
		WithMidnightConfig(MidnightConfig{Enabled: true}),
		WithTokenRegistryConfig(TokenRegistryConfig{Enabled: true}),
	))
	require.Nil(t, n)
	require.ErrorContains(t, err, `storage mode "core"`)
	require.ErrorContains(t, err, `CIP-26 registry requires "api"`)
	require.ErrorContains(t, err, `Midnight indexer requires "api"`)
}

func TestNewRefusesEnabledAPIProviderAboveStorageTier(t *testing.T) {
	t.Parallel()

	n, err := New(storageRequirementsTestConfig(
		WithStorageMode(StorageModeCore),
		WithBlockfrostPort(13000),
	))
	require.Nil(t, n)
	require.ErrorContains(t, err, `api.blockfrost/builtin requires "api"`)
}

func TestNewEnablesDeclaredStorageFeatures(t *testing.T) {
	t.Parallel()

	n, err := New(storageRequirementsTestConfig(
		WithStorageMode(StorageModeCore),
	))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, n.Stop()) })

	require.Equal(t, []plugin.StorageFeature{
		plugin.StorageFeatureUtxorpc,
	}, n.config.storageFeatures)
}

func TestAPIStorageTierEnablesEveryDeclaredFeature(t *testing.T) {
	t.Parallel()

	n, err := New(storageRequirementsTestConfig(
		WithStorageMode(StorageModeAPI),
	))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, n.Stop()) })

	require.Equal(t, []plugin.StorageFeature{
		plugin.StorageFeatureBark,
		plugin.StorageFeatureBlockfrost,
		plugin.StorageFeatureCIP26,
		plugin.StorageFeatureKupo,
		plugin.StorageFeatureMCP,
		plugin.StorageFeatureMesh,
		plugin.StorageFeatureMidnight,
		plugin.StorageFeatureOffchainMetadata,
		plugin.StorageFeatureUtxorpc,
	}, n.config.storageFeatures)
}

func TestAPIProviderEnabledDoesNotRequireBuiltInFeature(t *testing.T) {
	t.Parallel()

	external := plugin.Descriptor{Storage: plugin.StorageRequirements{
		Tier:     plugin.StorageTierCore,
		Features: []plugin.StorageFeature{"custom-feature"},
	}}
	apiTier := plugin.Descriptor{Storage: plugin.StorageRequirements{
		Tier:     plugin.StorageTierAPI,
		Features: []plugin.StorageFeature{plugin.StorageFeatureBlockfrost},
	}}
	builtin := plugin.Selection{Provider: "builtin"}
	custom := plugin.Selection{Provider: "custom"}
	blockfrostPort := defaultAPIPort(plugin.CapabilityAPIBlockfrost)
	utxorpcPort := defaultAPIPort(plugin.CapabilityAPIUtxorpc)

	// A core-tier external provider declaring only its own feature starts.
	require.True(t, apiProviderEnabledFor(
		StorageModeCore, plugin.CapabilityAPIUtxorpc, custom, external,
		utxorpcPort,
	))
	// Port zero disables any provider.
	require.False(t, apiProviderEnabledFor(
		StorageModeCore, plugin.CapabilityAPIUtxorpc, custom, external, 0,
	))
	// An API-tier built-in at its default port stays dormant in core mode.
	require.False(t, apiProviderEnabledFor(
		StorageModeCore, plugin.CapabilityAPIBlockfrost, builtin, apiTier,
		blockfrostPort,
	))
	require.True(t, apiProviderEnabledFor(
		StorageModeAPI, plugin.CapabilityAPIBlockfrost, builtin, apiTier,
		blockfrostPort,
	))
}

func TestAPIProviderEnabledFollowsStoragePlan(t *testing.T) {
	t.Parallel()

	n, err := New(storageRequirementsTestConfig(
		WithStorageMode(StorageModeCore),
	))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, n.Stop()) })

	utxorpc, port, err := n.apiPluginSelection(plugin.CapabilityAPIUtxorpc)
	require.NoError(t, err)
	require.True(t, n.apiProviderEnabled(
		plugin.CapabilityAPIUtxorpc, utxorpc, port,
	))
	blockfrost, port, err := n.apiPluginSelection(
		plugin.CapabilityAPIBlockfrost,
	)
	require.NoError(t, err)
	require.False(t, n.apiProviderEnabled(
		plugin.CapabilityAPIBlockfrost, blockfrost, port,
	))
}
