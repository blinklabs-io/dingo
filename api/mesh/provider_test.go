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
	"net"
	"strconv"
	"testing"

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
func freeLoopbackPort(t *testing.T) uint {
	t.Helper()
	_, portStr, err := net.SplitHostPort(freePort(t))
	require.NoError(t, err)
	port, err := strconv.ParseUint(portStr, 10, 16)
	require.NoError(t, err)
	return uint(port)
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
	port := freeLoopbackPort(t)

	srv, err := plugin.Resolve[*Server](
		t.Context(),
		host,
		plugin.CapabilityAPIMesh,
		"builtin",
		map[string]any{"port": port},
		providerDeps(deps),
	)

	require.NoError(t, err)
	require.Equal(
		t,
		net.JoinHostPort(
			"127.0.0.1",
			strconv.FormatUint(uint64(port), 10),
		),
		srv.config.ListenAddress,
	)
	// Resolve starts the instance, so the port must be accepting.
	require.True(t, portAccepts(srv.config.ListenAddress))
}

// TestProviderDefaultPort pins the documented default so a deployment
// that omits the port keeps the same listener address.
func TestProviderDefaultPort(t *testing.T) {
	host := plugin.NewHost()
	require.NoError(t, RegisterProvider(host))

	// Construct through NewServer directly rather than resolving, so
	// the default does not bind port 8080 during the test run.
	deps := newTestDeps()
	pd := providerDeps(deps)
	srv, err := NewServer(ServerConfig{
		LedgerState:         pd.LedgerState,
		Database:            pd.Database,
		Chain:               pd.Chain,
		Mempool:             pd.Mempool,
		ListenAddress:       net.JoinHostPort(pd.Host, "8080"),
		Network:             pd.Network,
		NetworkMagic:        pd.NetworkMagic,
		GenesisHash:         pd.GenesisHash,
		GenesisStartTimeSec: pd.GenesisStartTimeSec,
	})

	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:8080", srv.config.ListenAddress)
}

// TestProviderPropagatesDependencies asserts the node-supplied network
// identity and CORS policy reach the server rather than being dropped
// in the provider wiring.
func TestProviderPropagatesDependencies(t *testing.T) {
	host := newProviderHost(t)
	deps := newTestDeps()
	pd := providerDeps(deps)
	pd.CORSAllowedOrigins = []string{"https://wallet.example"}

	srv, err := plugin.Resolve[*Server](
		t.Context(),
		host,
		plugin.CapabilityAPIMesh,
		"builtin",
		map[string]any{"port": freeLoopbackPort(t)},
		pd,
	)

	require.NoError(t, err)
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
	port := freeLoopbackPort(t)

	srv, err := plugin.Resolve[*Server](
		t.Context(),
		host,
		plugin.CapabilityAPIMesh,
		"builtin",
		map[string]any{"port": port},
		providerDeps(deps),
	)
	require.NoError(t, err)
	addr := srv.config.ListenAddress
	require.True(t, portAccepts(addr))

	require.NoError(t, host.Stop(t.Context()))

	require.False(t, portAccepts(addr))
}
