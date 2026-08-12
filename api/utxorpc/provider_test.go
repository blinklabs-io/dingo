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
	"maps"
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
	_, portStr, err := net.SplitHostPort(freePort(t))
	require.NoError(t, err)
	port, err := strconv.ParseUint(portStr, 10, 16)
	require.NoError(t, err)
	return uint(port)
}

func providerDeps() ProviderDependencies {
	return ProviderDependencies{
		Logger:   slog.Default(),
		EventBus: event.NewEventBus(nil, nil),
		Host:     "127.0.0.1",
	}
}

// resolveOnFreePortWithConfig resolves the built-in UTxO RPC provider on a
// free loopback port with extra config fields (e.g. "tls"/"auth") merged
// alongside "port", retrying on a lost race for the port.
func resolveOnFreePortWithConfig(
	t *testing.T,
	host *plugin.Host,
	extra map[string]any,
) *Utxorpc {
	t.Helper()
	var lastErr error
	for range bindAttempts {
		cfg := map[string]any{"port": freeLoopbackPort(t)}
		maps.Copy(cfg, extra)
		srv, err := plugin.Resolve[*Utxorpc](
			t.Context(),
			host,
			plugin.CapabilityAPIUtxorpc,
			"builtin",
			cfg,
			providerDeps(),
		)
		if err != nil {
			lastErr = err
			continue
		}
		return srv
	}
	t.Fatalf(
		"could not resolve the UTxO RPC provider in %d attempts: %v",
		bindAttempts, lastErr,
	)
	return nil
}

func TestRegisterProviderDescriptor(t *testing.T) {
	host := newProviderHost(t)

	var found *plugin.Descriptor
	for _, d := range host.Providers() {
		if d.Capability == plugin.CapabilityAPIUtxorpc {
			found = &d
			break
		}
	}

	require.NotNil(t, found)
	require.Equal(t, "builtin", found.Name)
	require.NotEmpty(t, found.Description)
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
		providerDeps(),
	)

	require.Error(t, err)
	require.ErrorContains(t, err, "plugins.api.utxorpc.config.tls")
	require.ErrorContains(t, err, "must both be set")
}

func TestProviderRejectsInvalidAuthMode(t *testing.T) {
	host := newProviderHost(t)

	_, err := plugin.Resolve[*Utxorpc](
		t.Context(),
		host,
		plugin.CapabilityAPIUtxorpc,
		"builtin",
		map[string]any{
			"port": freeLoopbackPort(t),
			"auth": map[string]any{"mode": "bogus"},
		},
		providerDeps(),
	)

	require.Error(t, err)
	require.ErrorContains(t, err, "plugins.api.utxorpc.config.auth")
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
