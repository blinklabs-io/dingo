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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
		LedgerState:         deps.ledger,
		Database:            deps.database,
		Chain:               deps.chain,
		Mempool:             deps.mempool,
		Host:                "127.0.0.1",
		Network:             testNetwork,
		NetworkMagic:        testNetworkMagic,
		GenesisHash:         testGenesisHash,
		GenesisStartTimeSec: testGenesisStartTimeSec,
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
// provider config fields (e.g. "tls"/"auth") merged alongside "port".
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

// TestRegisterProviderDescriptor asserts the provider is advertised
// under the capability and name the node's configuration selects.
func TestRegisterProviderDescriptor(t *testing.T) {
	host := newProviderHost(t)

	var found *plugin.Descriptor
	for _, d := range host.Providers() {
		if d.Capability == plugin.CapabilityAPIMesh {
			found = &d
			break
		}
	}

	require.NotNil(t, found)
	require.Equal(t, "builtin", found.Name)
	require.NotEmpty(t, found.Description)
}

// TestRegisterProviderRejectsNilHost asserts registration fails loudly
// rather than silently leaving the capability unavailable.
func TestRegisterProviderRejectsNilHost(t *testing.T) {
	require.Error(t, RegisterProvider(nil))
}

// TestProviderBuildsListenAddress covers the host/port composition: the
// server must listen on the address the node's config asks for.
func TestProviderBuildsListenAddress(t *testing.T) {
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

// TestProviderDefaultPort pins the port a deployment gets when its
// configuration omits one. Resolving with no port would exercise the
// default end to end but would bind 8080 on the test host, so assert
// the defaults the plugin host is handed instead -- RegisterProvider
// passes providerDefaults itself, so a change to the default port is a
// change to what this asserts.
func TestProviderDefaultPort(t *testing.T) {
	require.Equal(t, uint(8080), defaultProviderPort)
	require.Equal(
		t,
		ProviderConfig{Port: defaultProviderPort},
		providerDefaults(),
	)
}

// TestProviderPropagatesDependencies asserts the node-supplied network
// identity and CORS policy reach the server rather than being dropped
// in the provider wiring.
func TestProviderPropagatesDependencies(t *testing.T) {
	host := newProviderHost(t)
	deps := newTestDeps()
	pd := providerDeps(deps)
	pd.CORSAllowedOrigins = []string{"https://wallet.example"}

	srv := resolveOnFreePort(t, host, pd)

	require.Equal(t, testNetwork, srv.config.Network)
	require.Equal(t, testNetworkMagic, srv.config.NetworkMagic)
	require.Equal(t, testGenesisHash, srv.config.GenesisHash)
	require.Equal(
		t,
		testGenesisStartTimeSec,
		srv.config.GenesisStartTimeSec,
	)
	require.Equal(
		t,
		[]string{"https://wallet.example"},
		srv.config.CORSAllowedOrigins,
	)
	require.Same(t, deps.chain, srv.config.Chain)
	require.Same(t, deps.database, srv.config.Database)
	require.Same(t, deps.ledger, srv.config.LedgerState)
	require.Same(t, deps.mempool, srv.config.Mempool)
}

// TestProviderRejectsInvalidDependencies asserts a misconfigured node
// fails at plugin resolution, before a listener is opened.
func TestProviderRejectsInvalidDependencies(t *testing.T) {
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

// TestProviderRejectsInvalidAuthMode asserts an unrecognized auth.mode is
// rejected at resolution, with an error naming the full provider config
// path.
func TestProviderRejectsInvalidAuthMode(t *testing.T) {
	host := newProviderHost(t)

	_, err := plugin.Resolve[*Server](
		t.Context(),
		host,
		plugin.CapabilityAPIMesh,
		"builtin",
		map[string]any{
			"port": freeLoopbackPort(t),
			"auth": map[string]any{"mode": "bogus"},
		},
		providerDeps(newTestDeps()),
	)

	require.Error(t, err)
	require.ErrorContains(t, err, "plugins.api.mesh.config.auth")
	require.ErrorContains(t, err, "invalid mode")
}

// TestProviderPropagatesTLSAndAuth asserts a valid provider tls/auth
// config reaches the server's resolved (EffectiveTLS/EffectiveAuth)
// settings.
func TestProviderPropagatesTLSAndAuth(t *testing.T) {
	host := newProviderHost(t)
	certPath, keyPath := testutil.GenerateTestTLSCertKey(t)

	srv := resolveOnFreePortWithConfig(
		t, host, providerDeps(newTestDeps()),
		map[string]any{
			"tls": map[string]any{
				"mode":         "server",
				"certFilePath": certPath,
				"keyFilePath":  keyPath,
			},
			"auth": map[string]any{
				"mode":  "token",
				"token": "shared-secret",
			},
		},
	)

	require.True(t, srv.config.TLS.Enabled)
	require.Equal(t, certPath, srv.config.TLS.CertFilePath)
	require.Equal(t, keyPath, srv.config.TLS.KeyFilePath)
	require.True(t, srv.config.Auth.Enabled)
	require.Equal(t, "shared-secret", srv.config.Auth.Token)
}
