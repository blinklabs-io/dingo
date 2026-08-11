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
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/api/blockfrost"
	"github.com/blinklabs-io/dingo/api/mesh"
	"github.com/blinklabs-io/dingo/api/utxorpc"
	"github.com/blinklabs-io/dingo/internal/apiauth"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// capturedAPISecurity is the subset of a provider's ProviderDependencies
// this file's regression test cares about, normalized across all three
// built-in API providers so utxorpc/blockfrost/mesh can be asserted on with
// one shared comparison.
type capturedAPISecurity struct {
	tlsCertFilePath   string
	tlsKeyFilePath    string
	authMode          string
	authTokenFilePath string
}

// apiSecurityCaptures collects one capturedAPISecurity per built-in API
// provider capability, guarded by a mutex since plugin.ResolveProvider
// documents no ordering guarantee across capabilities.
type apiSecurityCaptures struct {
	mu           sync.Mutex
	byCapability map[plugin.Capability]capturedAPISecurity
}

func newAPISecurityCaptures() *apiSecurityCaptures {
	return &apiSecurityCaptures{
		byCapability: make(map[plugin.Capability]capturedAPISecurity),
	}
}

func (c *apiSecurityCaptures) record(
	capability plugin.Capability,
	got capturedAPISecurity,
) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byCapability[capability] = got
}

func (c *apiSecurityCaptures) get(
	capability plugin.Capability,
) (capturedAPISecurity, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	got, ok := c.byCapability[capability]
	return got, ok
}

// registerAPISecurityCapturingProbe registers a no-op provider under name
// for capability that records the TLS/auth fields it was actually handed in
// its ProviderDependencies, so a test can compare what a real provider would
// have been configured with.
func registerAPISecurityCapturingProbe(
	t *testing.T,
	host *plugin.Host,
	capability plugin.Capability,
	name string,
	captures *apiSecurityCaptures,
) {
	t.Helper()
	descriptor := plugin.Descriptor{Capability: capability, Name: name}
	noop := func() plugin.Instance { return plugin.Lifecycle{} }
	var err error
	switch capability {
	case plugin.CapabilityAPIUtxorpc:
		err = plugin.Register(
			host, descriptor,
			func() apiProbeConfig { return apiProbeConfig{} },
			func(
				_ context.Context,
				_ apiProbeConfig,
				deps utxorpc.ProviderDependencies,
			) (string, plugin.Instance, error) {
				captures.record(capability, capturedAPISecurity{
					tlsCertFilePath:   deps.TLSCertFilePath,
					tlsKeyFilePath:    deps.TLSKeyFilePath,
					authMode:          string(deps.AuthMode),
					authTokenFilePath: deps.AuthTokenFilePath,
				})
				return name, noop(), nil
			},
		)
	case plugin.CapabilityAPIBlockfrost:
		err = plugin.Register(
			host, descriptor,
			func() apiProbeConfig { return apiProbeConfig{} },
			func(
				_ context.Context,
				_ apiProbeConfig,
				deps blockfrost.ProviderDependencies,
			) (string, plugin.Instance, error) {
				captures.record(capability, capturedAPISecurity{
					tlsCertFilePath:   deps.TLSCertFilePath,
					tlsKeyFilePath:    deps.TLSKeyFilePath,
					authMode:          string(deps.AuthMode),
					authTokenFilePath: deps.AuthTokenFilePath,
				})
				return name, noop(), nil
			},
		)
	case plugin.CapabilityAPIMesh:
		err = plugin.Register(
			host, descriptor,
			func() apiProbeConfig { return apiProbeConfig{} },
			func(
				_ context.Context,
				_ apiProbeConfig,
				deps mesh.ProviderDependencies,
			) (string, plugin.Instance, error) {
				captures.record(capability, capturedAPISecurity{
					tlsCertFilePath:   deps.TLSCertFilePath,
					tlsKeyFilePath:    deps.TLSKeyFilePath,
					authMode:          string(deps.AuthMode),
					authTokenFilePath: deps.AuthTokenFilePath,
				})
				return name, noop(), nil
			},
		)
	default:
		t.Fatalf("unsupported API capability %s", capability)
	}
	require.NoError(t, err)
}

// TestReinitializeAPIServersResolvesSameAPISecurityAsRun is the regression
// test for the bug where reinitializeAPIServers (the live Restore/Truncate
// reinit path, node_lifecycle.go) wired utxorpc's TLS off the deprecated
// root-level fields directly, gave blockfrost and mesh no TLS at all, and
// set no auth fields on any of the three -- instead of resolving the
// dingo #2996/#2998 api.tls/api.auth policy the way Run() does.
//
// It configures a top-level api.tls/api.auth policy that is deliberately
// different from the deprecated root-level TLS fields, then calls
// reinitializeAPIServers directly and asserts every provider's captured
// ProviderDependencies match n.resolveAPISecurity's resolution for that
// provider's selection -- the exact helper Run() also calls (node.go) -- and
// do NOT match the stale root-level fields a pre-fix reinit would have used
// instead.
func TestReinitializeAPIServersResolvesSameAPISecurityAsRun(t *testing.T) {
	n, _ := newLiveLifecycleTestNode(t, 5)

	mp, err := mempool.NewFIFO(mempool.MempoolConfig{
		Logger:          n.config.logger,
		Validator:       n.ledgerState,
		MempoolCapacity: 1024 * 1024,
	})
	require.NoError(t, err)
	require.NoError(t, mp.Start(context.Background()))
	t.Cleanup(func() { _ = mp.Stop(context.Background()) })
	n.mempool = mp

	n.config.storageMode = StorageModeAPI
	n.config.midnight = MidnightConfig{Port: 0}
	n.config.pluginSelections = map[plugin.Capability]plugin.Selection{
		plugin.CapabilityAPIUtxorpc: {
			Provider: "security-probe",
			Config:   map[string]any{"port": uint(19090)},
		},
		plugin.CapabilityAPIBlockfrost: {
			Provider: "security-probe",
			Config:   map[string]any{"port": uint(13000)},
		},
		plugin.CapabilityAPIMesh: {
			Provider: "security-probe",
			Config:   map[string]any{"port": uint(18080)},
		},
	}

	// Deliberately distinct from api.tls/api.auth below, so a reinit that
	// (pre-fix) wired these raw fields instead of resolving through
	// internal/config is caught by the "does not equal legacy" assertion.
	n.config.cfg.TlsCertFilePath = "/legacy/root.crt"
	n.config.cfg.TlsKeyFilePath = "/legacy/root.key"
	n.config.cfg.API = internalconfig.APIConfig{
		TLS: internalconfig.APITLSPolicy{
			Mode:         "server",
			CertFilePath: "/cfg/api-tls.crt",
			KeyFilePath:  "/cfg/api-tls.key",
		},
		Auth: internalconfig.APIAuthPolicy{
			Mode:          "token",
			TokenFilePath: "/cfg/api-auth.token",
		},
	}

	captures := newAPISecurityCaptures()
	for _, capability := range []plugin.Capability{
		plugin.CapabilityAPIUtxorpc,
		plugin.CapabilityAPIBlockfrost,
		plugin.CapabilityAPIMesh,
	} {
		registerAPISecurityCapturingProbe(
			t, n.pluginHost, capability, "security-probe", captures,
		)
	}

	require.NoError(t, n.reinitializeAPIServers())
	t.Cleanup(func() {
		for _, capability := range []plugin.Capability{
			plugin.CapabilityAPIUtxorpc,
			plugin.CapabilityAPIBlockfrost,
			plugin.CapabilityAPIMesh,
		} {
			_ = n.pluginHost.StopCapability(context.Background(), capability)
		}
	})

	for _, capability := range []plugin.Capability{
		plugin.CapabilityAPIUtxorpc,
		plugin.CapabilityAPIBlockfrost,
		plugin.CapabilityAPIMesh,
	} {
		got, ok := captures.get(capability)
		require.Truef(t, ok, "capability %s never resolved", capability)

		// This is exactly what Run() would have computed for the same
		// selection, since Run() and reinitializeAPIServers both now call
		// this one shared helper (node.go) instead of resolving
		// separately.
		want := n.resolveAPISecurity(n.config.pluginSelections[capability])
		assert.Equalf(t, capturedAPISecurity{
			tlsCertFilePath:   want.TLSCertFilePath,
			tlsKeyFilePath:    want.TLSKeyFilePath,
			authMode:          want.AuthMode,
			authTokenFilePath: want.AuthTokenFilePath,
		}, got, "capability %s", capability)

		assert.NotEqualf(
			t, n.config.cfg.TlsCertFilePath, got.tlsCertFilePath,
			"capability %s: reinit leaked the deprecated root-level TLS "+
				"cert instead of resolving api.tls",
			capability,
		)
		assert.NotEmptyf(
			t, got.authMode,
			"capability %s: reinit set no auth mode at all",
			capability,
		)
		assert.Equalf(
			t, string(apiauth.ModeToken), got.authMode,
			"capability %s", capability,
		)
		assert.Equalf(
			t, "/cfg/api-auth.token", got.authTokenFilePath,
			"capability %s", capability,
		)
	}
}
