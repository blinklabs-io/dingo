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

package utxorpc

import (
	"context"
	"log/slog"
	"net"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

func newProviderHost(t *testing.T) *plugin.Host {
	t.Helper()
	host := plugin.NewHost()
	require.NoError(t, RegisterProvider(host))
	t.Cleanup(func() {
		require.NoError(t, host.Stop(context.Background()))
	})
	return host
}

func freeLoopbackPort(t *testing.T) uint {
	t.Helper()
	_, portStr, err := net.SplitHostPort(testutil.FreePort(t))
	require.NoError(t, err)
	port, err := strconv.ParseUint(portStr, 10, 16)
	require.NoError(t, err)
	return uint(port)
}

// providerDeps builds provider dependencies over a test-owned EventBus.
// The bus is permanently shut down via t.Cleanup(bus.Close) rather than
// bus.Stop -- Stop leaves the worker pool restartable (for a component
// that reuses one EventBus across a stop/start cycle), which would leak
// its four workers for the rest of the test run instead of releasing them.
func providerDeps(t *testing.T) ProviderDependencies {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	return ProviderDependencies{
		Logger:   slog.Default(),
		EventBus: bus,
		Host:     "127.0.0.1",
	}
}

func TestProviderRejectsPartialTLSPair(t *testing.T) {
	host := newProviderHost(t)

	_, err := plugin.Resolve[*Utxorpc](
		t.Context(),
		host,
		plugin.CapabilityAPIUtxorpc,
		"builtin",
		map[string]any{
			"port": freeLoopbackPort(t),
			"tls": map[string]any{
				"mode":         "server",
				"certFilePath": "/only/cert.pem",
			},
		},
		providerDeps(t),
	)

	require.Error(t, err)
	require.ErrorContains(t, err, "plugins.api.utxorpc.config.tls")
	require.ErrorContains(t, err, "must both be set")
}
