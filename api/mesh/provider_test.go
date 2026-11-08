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

package mesh

import (
	"context"
	"maps"
	"net"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

// newProviderHost registers the built-in Mesh provider on a fresh host.
func newProviderHost(t *testing.T) *plugin.Host {
	t.Helper()
	host := plugin.NewHost()
	require.NoError(t, RegisterProvider(host))
	t.Cleanup(func() {
		require.NoError(t, host.Stop(context.Background()))
	})
	return host
}

// providerDeps builds provider dependencies over test doubles.
func providerDeps(deps *testDeps) ProviderDependencies {
	return ProviderDependencies{
		LedgerState:  deps.ledger,
		Database:     deps.database,
		Chain:        deps.chain,
		Mempool:      deps.mempool,
		Host:         "127.0.0.1",
		Network:      testNetwork,
		NetworkMagic: testNetworkMagic,
		GenesisHash:  testGenesisHash,
	}
}

// freeLoopbackPort reserves and releases a loopback port, returning it.
// The port can be claimed before the provider binds it, so callers that
// expect resolution to succeed go through resolveOnFreePort.
func freeLoopbackPort(t *testing.T) uint {
	t.Helper()
	_, portStr, err := net.SplitHostPort(testutil.FreePort(t))
	require.NoError(t, err)
	port, err := strconv.ParseUint(portStr, 10, 16)
	require.NoError(t, err)
	return uint(port)
}

// resolveOnFreePort resolves the Mesh provider on a free loopback port,
// retrying on a lost race for the port. Resolve starts the instance, so
// a port claimed between reservation and bind surfaces as a resolution
// error rather than a test failure worth reporting. It is
// resolveOnFreePortWithConfig with no extra config fields.
func resolveOnFreePort(
	t *testing.T,
	host *plugin.Host,
	deps ProviderDependencies,
) *Server {
	t.Helper()
	return resolveOnFreePortWithConfig(t, host, deps, nil)
}

// resolveOnFreePortWithConfig is resolveOnFreePort with additional
// provider config fields (e.g. "tls") merged alongside "port".
func resolveOnFreePortWithConfig(
	t *testing.T,
	host *plugin.Host,
	deps ProviderDependencies,
	extra map[string]any,
) *Server {
	t.Helper()
	var lastErr error
	for range testutil.BindAttempts {
		cfg := map[string]any{"port": freeLoopbackPort(t)}
		maps.Copy(cfg, extra)
		srv, err := plugin.Resolve[*Server](
			t.Context(),
			host,
			plugin.CapabilityAPIMesh,
			"builtin",
			cfg,
			deps,
		)
		if err != nil {
			lastErr = err
			continue
		}
		return srv
	}
	t.Fatalf(
		"could not resolve the Mesh provider in %d attempts: %v",
		testutil.BindAttempts, lastErr,
	)
	return nil
}

// TestProviderBuildsListenAddress covers the host/port composition: the
// server must listen on the address the node's config asks for.
func TestProviderBuildsListenAddress(t *testing.T) {
	t.Parallel()

	host := newProviderHost(t)
	deps := newTestDeps()

	srv := resolveOnFreePort(t, host, providerDeps(deps))

	// The address is the configured host joined to the configured
	// port, not a default or a bare port.
	wantHost, wantPort, err := net.SplitHostPort(
		srv.config.ListenAddress,
	)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", wantHost)
	require.NotEmpty(t, wantPort)
	// Resolve starts the instance, so the port must be accepting.
	require.True(t, portAccepts(srv.config.ListenAddress))
}

// TestProviderRejectsInvalidDependencies asserts a misconfigured node
// fails at plugin resolution, before a listener is opened.
func TestProviderRejectsInvalidDependencies(t *testing.T) {
	t.Parallel()

	host := newProviderHost(t)
	pd := providerDeps(newTestDeps())
	pd.GenesisHash = ""

	srv, err := plugin.Resolve[*Server](
		t.Context(),
		host,
		plugin.CapabilityAPIMesh,
		"builtin",
		map[string]any{"port": freeLoopbackPort(t)},
		pd,
	)

	require.Error(t, err)
	require.Nil(t, srv)
	require.Contains(t, err.Error(), "GenesisHash")
}

// TestProviderStopClosesListener asserts the host's shutdown path stops
// the Mesh listener, so a capability restart can rebind the port.
func TestProviderStopClosesListener(t *testing.T) {
	t.Parallel()

	host := plugin.NewHost()
	require.NoError(t, RegisterProvider(host))
	deps := newTestDeps()

	srv := resolveOnFreePort(t, host, providerDeps(deps))
	addr := srv.config.ListenAddress
	require.True(t, portAccepts(addr))

	require.NoError(t, host.Stop(t.Context()))

	require.False(t, portAccepts(addr))
}

// TestProviderRejectsPartialTLSPair asserts a provider config with tls
// mode "server" and only one of certFilePath/keyFilePath set fails
// resolution -- before any listener is opened -- with an error naming
// the full provider config path, not just "tls".
func TestProviderRejectsPartialTLSPair(t *testing.T) {
	t.Parallel()

	host := newProviderHost(t)

	_, err := plugin.Resolve[*Server](
		t.Context(),
		host,
		plugin.CapabilityAPIMesh,
		"builtin",
		map[string]any{
			"port": freeLoopbackPort(t),
			"tls": map[string]any{
				"mode":         "server",
				"certFilePath": "/only/cert.pem",
			},
		},
		providerDeps(newTestDeps()),
	)

	require.Error(t, err)
	require.ErrorContains(t, err, "plugins.api.mesh.config.tls")
	require.ErrorContains(t, err, "must both be set")
}
