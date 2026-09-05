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

package blockfrost

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

// providerDeps builds provider dependencies over a test double, with the
// shared API bind address composition would hand down.
func providerDeps() ProviderDependencies {
	return ProviderDependencies{Node: &mockNode{}, Host: "127.0.0.1"}
}

// resolveOnFreePortWithConfig resolves the built-in Blockfrost provider on
// a free loopback port with extra config fields (e.g. "tls"/"auth")
// merged alongside "port", retrying on a lost race for the port. It is
// resolveOnFreePortWithDeps over the default dependencies.
func resolveOnFreePortWithConfig(
	t *testing.T,
	host *plugin.Host,
	extra map[string]any,
) *Blockfrost {
	t.Helper()
	return resolveOnFreePortWithDeps(t, host, providerDeps(), extra)
}

// resolveOnFreePortWithDeps is resolveOnFreePortWithConfig with the
// provider dependencies supplied by the caller, for tests that need a
// specific shared Host to resolve a per-provider override against.
func resolveOnFreePortWithDeps(
	t *testing.T,
	host *plugin.Host,
	deps ProviderDependencies,
	extra map[string]any,
) *Blockfrost {
	t.Helper()
	var lastErr error
	for range testutil.BindAttempts {
		cfg := map[string]any{"port": freeLoopbackPort(t)}
		maps.Copy(cfg, extra)
		srv, err := plugin.Resolve[*Blockfrost](
			t.Context(),
			host,
			plugin.CapabilityAPIBlockfrost,
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
		"could not resolve the Blockfrost provider in %d attempts: %v",
		testutil.BindAttempts, lastErr,
	)
	return nil
}

func TestRegisterProviderDescriptor(t *testing.T) {
	host := newProviderHost(t)

	var found *plugin.Descriptor
	for _, d := range host.Providers() {
		if d.Capability == plugin.CapabilityAPIBlockfrost {
			found = &d
			break
		}
	}

	require.NotNil(t, found)
	require.Equal(t, "builtin", found.Name)
	require.NotEmpty(t, found.Description)
}

func TestRegisterProviderRejectsNilHost(t *testing.T) {
	require.Error(t, RegisterProvider(nil))
}

func TestProviderRejectsPartialTLSPair(t *testing.T) {
	host := newProviderHost(t)

	_, err := plugin.Resolve[*Blockfrost](
		t.Context(),
		host,
		plugin.CapabilityAPIBlockfrost,
		"builtin",
		map[string]any{
			"port": freeLoopbackPort(t),
			"tls": map[string]any{
				"mode":        "server",
				"keyFilePath": "/only/key.pem",
			},
		},
		ProviderDependencies{Node: &mockNode{}, Host: "127.0.0.1"},
	)

	require.Error(t, err)
	require.ErrorContains(t, err, "plugins.api.blockfrost.config.tls")
	require.ErrorContains(t, err, "must both be set")
}

func TestProviderRejectsInvalidAuthMode(t *testing.T) {
	host := newProviderHost(t)

	_, err := plugin.Resolve[*Blockfrost](
		t.Context(),
		host,
		plugin.CapabilityAPIBlockfrost,
		"builtin",
		map[string]any{
			"port": freeLoopbackPort(t),
			"auth": map[string]any{"mode": "bogus"},
		},
		ProviderDependencies{Node: &mockNode{}, Host: "127.0.0.1"},
	)

	require.Error(t, err)
	require.ErrorContains(t, err, "plugins.api.blockfrost.config.auth")
	require.ErrorContains(t, err, "invalid mode")
}

func TestProviderPropagatesTLSAndAuth(t *testing.T) {
	host := newProviderHost(t)
	certPath, keyPath := testutil.GenerateTestTLSCertKey(t)

	srv := resolveOnFreePortWithConfig(t, host, map[string]any{
		"tls": map[string]any{
			"mode":         "server",
			"certFilePath": certPath,
			"keyFilePath":  keyPath,
		},
		"auth": map[string]any{
			"mode":  "token",
			"token": "shared-secret",
		},
	})

	require.True(t, srv.config.TLS.Enabled)
	require.Equal(t, certPath, srv.config.TLS.CertFilePath)
	require.Equal(t, keyPath, srv.config.TLS.KeyFilePath)
	require.True(t, srv.config.Auth.Enabled)
	require.Equal(t, "shared-secret", srv.config.Auth.Token)
}

// TestProviderHostOverridesSharedDefault asserts the per-plugin
// `plugins.api.blockfrost.config.host` override wins over the shared API
// bind address composition hands down (issue #3498).
func TestProviderHostOverridesSharedDefault(t *testing.T) {
	host := newProviderHost(t)
	deps := providerDeps()
	deps.Host = "0.0.0.0"

	srv := resolveOnFreePortWithDeps(
		t, host, deps, map[string]any{"host": "127.0.0.1"},
	)

	hostPart, _, err := net.SplitHostPort(srv.config.ListenAddress)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", hostPart)
}
