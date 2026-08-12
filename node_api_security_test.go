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
	"time"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAPIProviderConfigMergesTopLevelDefault asserts a provider selection
// with no tls/auth of its own inherits the shared api.tls/api.auth
// policy.
func TestAPIProviderConfigMergesTopLevelDefault(t *testing.T) {
	cfg := Config{
		apiConfig: internalconfig.APIConfig{
			TLS: apiconfig.TLSPolicy{
				Mode:         new("server"),
				CertFilePath: new("/shared/cert.pem"),
				KeyFilePath:  new("/shared/key.pem"),
			},
		},
	}
	selection := plugin.Selection{
		Provider: "builtin",
		Config:   map[string]any{"port": uint(3000)},
	}

	merged, err := cfg.apiProviderConfig(
		plugin.CapabilityAPIBlockfrost, selection,
	)
	require.NoError(t, err)

	tlsPolicy, err := apiconfig.DecodeTLSPolicy(merged.Config)
	require.NoError(t, err)
	effective, err := tlsPolicy.Resolve("test")
	require.NoError(t, err)
	assert.True(t, effective.Enabled)
	assert.Equal(t, "/shared/cert.pem", effective.CertFilePath)
	assert.Equal(t, "/shared/key.pem", effective.KeyFilePath)
}

// TestAPIProviderConfigProviderOverrideWins asserts an explicit provider
// field beats the shared top-level default for that field only.
func TestAPIProviderConfigProviderOverrideWins(t *testing.T) {
	cfg := Config{
		apiConfig: internalconfig.APIConfig{
			TLS: apiconfig.TLSPolicy{
				Mode:         new("server"),
				CertFilePath: new("/shared/cert.pem"),
				KeyFilePath:  new("/shared/key.pem"),
			},
		},
	}
	selection := plugin.Selection{
		Provider: "builtin",
		Config: map[string]any{
			"port": uint(3000),
			"tls": map[string]any{
				"certFilePath": "/provider/cert.pem",
			},
		},
	}

	merged, err := cfg.apiProviderConfig(
		plugin.CapabilityAPIBlockfrost, selection,
	)
	require.NoError(t, err)

	tlsPolicy, err := apiconfig.DecodeTLSPolicy(merged.Config)
	require.NoError(t, err)
	effective, err := tlsPolicy.Resolve("test")
	require.NoError(t, err)
	assert.True(t, effective.Enabled)
	assert.Equal(t, "/provider/cert.pem", effective.CertFilePath)
	// keyFilePath falls through to the shared default.
	assert.Equal(t, "/shared/key.pem", effective.KeyFilePath)
}

// TestAPIProviderConfigExplicitDisableOverridesInherited asserts a
// provider can turn off an inherited auth policy explicitly.
func TestAPIProviderConfigExplicitDisableOverridesInherited(t *testing.T) {
	cfg := Config{
		apiConfig: internalconfig.APIConfig{
			Auth: apiconfig.AuthPolicy{
				Mode:  new("token"),
				Token: new("shared-secret"),
			},
		},
	}
	selection := plugin.Selection{
		Provider: "builtin",
		Config: map[string]any{
			"port": uint(8080),
			"auth": map[string]any{"mode": "disabled"},
		},
	}

	merged, err := cfg.apiProviderConfig(plugin.CapabilityAPIMesh, selection)
	require.NoError(t, err)

	authPolicy, err := apiconfig.DecodeAuthPolicy(merged.Config)
	require.NoError(t, err)
	effective, err := authPolicy.Resolve("test")
	require.NoError(t, err)
	assert.False(t, effective.Enabled)
}

// TestLegacyUtxorpcTLSPolicyIsUtxorpcOnly asserts the legacy root
// tlsCertFilePath/tlsKeyFilePath compatibility fields feed only UTxORPC's
// default TLS policy, never Blockfrost's or Mesh's -- promoting them to
// every API provider would silently switch previously-plaintext listeners
// to TLS on upgrade for any deployment that had set them (see
// legacyUtxorpcTLSPolicy's own doc comment).
func TestLegacyUtxorpcTLSPolicyIsUtxorpcOnly(t *testing.T) {
	cfg := Config{
		tlsCertFilePath: "/legacy/cert.pem",
		tlsKeyFilePath:  "/legacy/key.pem",
	}
	selection := plugin.Selection{
		Provider: "builtin",
		Config:   map[string]any{"port": uint(9090)},
	}

	for _, tc := range []struct {
		capability   plugin.Capability
		wantEnabled  bool
		wantCertPath string
	}{
		{plugin.CapabilityAPIUtxorpc, true, "/legacy/cert.pem"},
		{plugin.CapabilityAPIBlockfrost, false, ""},
		{plugin.CapabilityAPIMesh, false, ""},
	} {
		merged, err := cfg.apiProviderConfig(tc.capability, selection)
		require.NoErrorf(t, err, "capability %s", tc.capability)
		tlsPolicy, err := apiconfig.DecodeTLSPolicy(merged.Config)
		require.NoErrorf(t, err, "capability %s", tc.capability)
		effective, err := tlsPolicy.Resolve("test")
		require.NoErrorf(t, err, "capability %s", tc.capability)
		assert.Equalf(
			t, tc.wantEnabled, effective.Enabled,
			"capability %s", tc.capability,
		)
		assert.Equalf(
			t, tc.wantCertPath, effective.CertFilePath,
			"capability %s", tc.capability,
		)
	}
}

// TestLegacyUtxorpcTLSPolicyYieldsToExplicitPolicy asserts the shared
// api.tls default and any provider-level override both still take
// precedence over the legacy compatibility fields for UTxORPC, matching
// the canonical-over-compatibility precedence used elsewhere (e.g.
// applyAPIPortCompatibilityEnvironment).
func TestLegacyUtxorpcTLSPolicyYieldsToExplicitPolicy(t *testing.T) {
	cfg := Config{
		tlsCertFilePath: "/legacy/cert.pem",
		tlsKeyFilePath:  "/legacy/key.pem",
		apiConfig: internalconfig.APIConfig{
			TLS: apiconfig.TLSPolicy{Mode: new("disabled")},
		},
	}
	selection := plugin.Selection{
		Provider: "builtin",
		Config:   map[string]any{"port": uint(9090)},
	}

	merged, err := cfg.apiProviderConfig(
		plugin.CapabilityAPIUtxorpc, selection,
	)
	require.NoError(t, err)
	tlsPolicy, err := apiconfig.DecodeTLSPolicy(merged.Config)
	require.NoError(t, err)
	effective, err := tlsPolicy.Resolve("test")
	require.NoError(t, err)
	assert.False(t, effective.Enabled)
}

// TestNewRejectsInvalidMergedAPITLSPolicy asserts a partial certificate/
// key pair in the shared api.tls default is rejected at New(), before any
// listener starts -- not merely logged or deferred to Start() time.
func TestNewRejectsInvalidMergedAPITLSPolicy(t *testing.T) {
	cardanoCfg := newNodeTestCardanoNodeCfg(t)
	_, err := New(NewConfig(
		WithDatabasePath(t.TempDir()),
		WithCardanoNodeConfig(cardanoCfg),
		WithNetworkMagic(cardanoCfg.ShelleyGenesis().NetworkMagic),
		WithPrometheusRegistry(prometheus.NewRegistry()),
		WithStorageMode(StorageModeAPI),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
		WithMidnightConfig(MidnightConfig{Port: 0}),
		WithShutdownTimeout(5*time.Second),
		WithAPIConfig(internalconfig.APIConfig{
			TLS: apiconfig.TLSPolicy{
				Mode:         new("server"),
				CertFilePath: new("/only/cert.pem"),
			},
		}),
	))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "config.tls")
	assert.Contains(t, err.Error(), "must both be set")
}

// TestNewRejectsInvalidAPIAuthMode asserts an invalid auth mode is
// likewise rejected at New().
func TestNewRejectsInvalidAPIAuthMode(t *testing.T) {
	cardanoCfg := newNodeTestCardanoNodeCfg(t)
	_, err := New(NewConfig(
		WithDatabasePath(t.TempDir()),
		WithCardanoNodeConfig(cardanoCfg),
		WithNetworkMagic(cardanoCfg.ShelleyGenesis().NetworkMagic),
		WithPrometheusRegistry(prometheus.NewRegistry()),
		WithStorageMode(StorageModeAPI),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
		WithMidnightConfig(MidnightConfig{Port: 0}),
		WithShutdownTimeout(5*time.Second),
		WithAPIConfig(internalconfig.APIConfig{
			Auth: apiconfig.AuthPolicy{Mode: new("bogus")},
		}),
	))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "config.auth")
	assert.Contains(t, err.Error(), "invalid mode")
}
