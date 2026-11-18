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
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

type plannerStartupGuard struct {
	entered chan error
	release chan struct{}
	inspect func() error
	once    sync.Once
}

type plannerStartupStore struct {
	*sqlstore.Store
	gate *plannerStartupGuard
}

func (s *plannerStartupStore) OptimizePlannerStatsContext(
	ctx context.Context, trigger string,
) (types.PlannerStatsResult, error) {
	// Block before taking SQLite's write connection, so database contention
	// cannot conceal a core node that starts its ledger asynchronously.
	s.gate.once.Do(func() {
		s.gate.entered <- s.gate.inspect()
		select {
		case <-s.gate.release:
		case <-ctx.Done():
		}
	})
	return s.Store.OptimizePlannerStatsContext(ctx, trigger)
}

func TestNodePlannerStartupOrdering(t *testing.T) {
	t.Parallel()
	for _, mode := range []StorageMode{StorageModeAPI, StorageModeCore} {
		t.Run(string(mode), func(t *testing.T) {
			t.Parallel()
			cardanoCfg := newNodeTestCardanoNodeCfg(t)
			var apiPort uint
			if mode.IsAPI() {
				apiPort = freeLoopbackPort(t)
			}
			n, err := New(NewConfig(
				WithDatabasePath(t.TempDir()),
				WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
				WithCardanoNodeConfig(cardanoCfg),
				WithNetworkMagic(cardanoCfg.ShelleyGenesis().NetworkMagic),
				WithPrometheusRegistry(prometheus.NewRegistry()),
				WithNetwork("preview"),
				WithStorageMode(mode),
				WithIndexerLayout("shared"),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp", ListenAddress: "127.0.0.1:0",
				}),
				WithBlockfrostPort(apiPort),
				WithPluginSelection(plugin.CapabilityStorageBlob,
					plugin.Selection{Provider: "badger",
						Config: testutil.BadgerBlobConfig()}),
			))
			require.NoError(t, err)
			n.config.listeners = nil
			n.config.pluginSelections[plugin.CapabilityStorageMetadata] =
				plugin.Selection{Provider: "planner-startup-test"}
			guard := &plannerStartupGuard{
				entered: make(chan error, 1), release: make(chan struct{}),
				inspect: func() error {
					if !mode.IsAPI() {
						if n.wallClockLedger.Load() != nil {
							return fmt.Errorf(
								"core ledger started before planner pass",
							)
						}
						return nil
					}
					client := &http.Client{Timeout: 5 * time.Second}
					req, err := http.NewRequestWithContext(t.Context(),
						http.MethodGet,
						fmt.Sprintf("http://127.0.0.1:%d/health", apiPort), nil)
					if err != nil {
						return err
					}
					resp, err := client.Do(req)
					if err != nil {
						return fmt.Errorf(
							"API not serving before planner pass: %w",
							err,
						)
					}
					defer resp.Body.Close()
					if resp.StatusCode != http.StatusOK {
						return fmt.Errorf(
							"API health status: %d",
							resp.StatusCode,
						)
					}
					return nil
				},
			}
			require.NoError(t, plugin.Register(n.pluginHost,
				plugin.Descriptor{Capability: plugin.CapabilityStorageMetadata,
					Name: "planner-startup-test", DurableClose: true},
				func() sqlite.Config { return sqlite.Config{} },
				func(_ context.Context, cfg sqlite.Config,
					deps metadata.ProviderDependencies,
				) (*plannerStartupStore, plugin.Instance, error) {
					store, err := sqlite.NewSQLStore(cfg, deps)
					if err != nil {
						return nil, nil, err
					}
					return &plannerStartupStore{Store: store, gate: guard},
						plugin.Lifecycle{StartFunc: store.Start,
							StopFunc: store.CloseContext}, nil
				}))
			runErr := make(chan error, 1)
			release := sync.OnceFunc(func() { close(guard.release) })
			go func() { runErr <- n.Run(t.Context()) }()
			t.Cleanup(func() {
				release()
				require.NoError(t, n.Stop())
				require.NoError(t, testutil.RequireReceive(
					t, runErr, testutil.AsyncWait, "node shutdown",
				))
			})
			require.NoError(t, testutil.RequireReceive(
				t, guard.entered, testutil.AsyncWait, "planner startup pass",
			))
			if mode.IsAPI() {
				require.Eventually(
					t,
					func() bool {
						if !n.startupLifecycleMu.TryLock() {
							return false
						}
						n.startupLifecycleMu.Unlock()
						return true
					},
					10*time.Second,
					10*time.Millisecond,
					"API startup must finish while planner maintenance is blocked",
				)
			} else {
				require.Never(t, func() bool {
					return n.wallClockLedger.Load() != nil
				}, time.Second, 10*time.Millisecond,
					"core block processing must wait for planner completion")
				release()
				requireStartupComplete(t, n)
				require.NotNil(t, n.wallClockLedger.Load())
			}
		})
	}
}
