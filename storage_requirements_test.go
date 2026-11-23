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
	require.Equal(t, []string{"utxorpc"}, n.databaseConfig().StorageFeatures)
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
