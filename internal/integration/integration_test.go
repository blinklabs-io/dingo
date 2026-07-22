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

package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/database"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestPluginSystemIntegration(t *testing.T) {
	host, err := internalplugins.NewHost()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, host.Stop(context.Background())) })
	providers := host.Providers()
	require.NotEmpty(t, providers)
	require.Contains(t, providers, plugin.Descriptor{
		Capability: plugin.CapabilityStorageBlob,
		Name:       "badger", Description: "BadgerDB local key-value store",
	})
	require.Contains(t, providers, plugin.Descriptor{
		Capability: plugin.CapabilityStorageMetadata,
		Name:       "sqlite", Description: "SQLite relational database",
	})

	runtime, err := internalplugins.OpenDatabase(
		context.Background(),
		&database.Config{DataDir: t.TempDir()},
		internalplugins.StorageSelections{
			Blob:     plugin.Selection{Provider: "badger", Config: map[string]any{}},
			Metadata: plugin.Selection{Provider: "sqlite", Config: map[string]any{}},
		},
		internalplugins.StorageDependencies{DataDir: t.TempDir()},
	)
	require.NoError(t, err)
	require.NotNil(t, runtime.Database.Blob())
	require.NotNil(t, runtime.Database.Metadata())
	require.NoError(t, runtime.Close(context.Background()))
	require.NoError(t, runtime.Close(context.Background()))
}

func TestNodeShutdownIntegration(t *testing.T) {
	cfg := dingo.NewConfig(
		dingo.WithDatabasePath(t.TempDir()),
		dingo.WithLogger(slog.New(slog.NewJSONHandler(io.Discard, nil))),
		dingo.WithPrometheusRegistry(prometheus.NewRegistry()),
		dingo.WithNetworkMagic(764824073),
		dingo.WithShutdownTimeout(5*time.Second),
		dingo.WithListeners(dingo.ListenerConfig{
			ListenNetwork: "tcp", ListenAddress: "127.0.0.1:0",
		}),
	)
	node, err := dingo.New(cfg)
	require.NoError(t, err)
	require.NoError(t, node.Stop())
	require.NoError(t, node.Stop())
}

func TestStorageBackends(t *testing.T) {
	for _, dataDir := range []string{"", t.TempDir()} {
		t.Run(fmt.Sprintf("dir-%t", dataDir != ""), func(t *testing.T) {
			runtime, err := internalplugins.OpenDatabase(
				context.Background(),
				&database.Config{DataDir: dataDir},
				internalplugins.StorageSelections{
					Blob:     plugin.Selection{Provider: "badger", Config: map[string]any{}},
					Metadata: plugin.Selection{Provider: "sqlite", Config: map[string]any{}},
				},
				internalplugins.StorageDependencies{DataDir: dataDir},
			)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, runtime.Close(context.Background())) })

			blocks, err := loadBlockData(10)
			require.NoError(t, err)
			for i := range 10 {
				txn := runtime.Database.Transaction(true)
				key := fmt.Appendf(nil, "block-%d", i)
				require.NoError(t, txn.DB().Blob().Set(txn.Blob(), key, blocks[i]))
				require.NoError(t, txn.Commit())
			}
		})
	}
}
