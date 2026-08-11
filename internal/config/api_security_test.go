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

package config

import (
	"maps"
	"testing"

	hostplugin "github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveAPISecurityDefaults(t *testing.T) {
	sec := ResolveAPISecurity(APIConfig{}, hostplugin.Selection{
		Config: map[string]any{"port": 3000},
	})
	assert.Equal(t, ResolvedAPISecurity{
		TLSMode:  "off",
		AuthMode: "none",
	}, sec)
}

func TestResolveAPISecurityTopLevelDefaultsApplyToEveryProvider(t *testing.T) {
	apiPolicy := APIConfig{
		TLS: APITLSPolicy{
			Mode:         "server",
			CertFilePath: "/run/secrets/api.crt",
			KeyFilePath:  "/run/secrets/api.key",
		},
		Auth: APIAuthPolicy{
			Mode:          "token",
			TokenFilePath: "/run/secrets/api-token",
		},
	}
	for _, selection := range []hostplugin.Selection{
		{Config: map[string]any{"port": 3000}},
		{Config: map[string]any{"port": 8080}},
		{Config: map[string]any{"port": 9090}},
	} {
		sec := ResolveAPISecurity(apiPolicy, selection)
		assert.Equal(t, ResolvedAPISecurity{
			TLSMode:           "server",
			TLSCertFilePath:   "/run/secrets/api.crt",
			TLSKeyFilePath:    "/run/secrets/api.key",
			AuthMode:          "token",
			AuthTokenFilePath: "/run/secrets/api-token",
		}, sec)
	}
}

func TestResolveAPISecurityPartialProviderOverride(t *testing.T) {
	apiPolicy := APIConfig{
		TLS: APITLSPolicy{
			Mode:         "server",
			CertFilePath: "/run/secrets/api.crt",
			KeyFilePath:  "/run/secrets/api.key",
		},
	}
	// Only certFilePath is overridden; mode and keyFilePath must still be
	// inherited from the top-level policy field-by-field, not replaced as a
	// whole object.
	selection := hostplugin.Selection{
		Config: map[string]any{
			"port": 3000,
			"tls": map[string]any{
				"certFilePath": "/run/secrets/blockfrost.crt",
			},
		},
	}
	sec := ResolveAPISecurity(apiPolicy, selection)
	assert.Equal(t, ResolvedAPISecurity{
		TLSMode:         "server",
		TLSCertFilePath: "/run/secrets/blockfrost.crt",
		TLSKeyFilePath:  "/run/secrets/api.key",
		AuthMode:        "none",
	}, sec)
}

func TestResolveAPISecurityExplicitDisable(t *testing.T) {
	apiPolicy := APIConfig{
		Auth: APIAuthPolicy{
			Mode:          "token",
			TokenFilePath: "/run/secrets/api-token",
		},
	}
	// A provider can affirmatively disable an inherited auth policy.
	selection := hostplugin.Selection{
		Config: map[string]any{
			"port": 8080,
			"auth": map[string]any{"mode": "none"},
		},
	}
	sec := ResolveAPISecurity(apiPolicy, selection)
	assert.Equal(t, "none", sec.AuthMode)
	assert.Empty(
		t,
		sec.AuthTokenFilePath,
		"disabled auth must not leak the inherited token path",
	)
}

func TestResolveAPISecurityEmptyProviderFieldDoesNotOverride(t *testing.T) {
	apiPolicy := APIConfig{
		TLS: APITLSPolicy{
			Mode:         "server",
			CertFilePath: "/run/secrets/api.crt",
			KeyFilePath:  "/run/secrets/api.key",
		},
	}
	// An explicit empty string is treated the same as an absent field: it
	// must not blank out the inherited value.
	selection := hostplugin.Selection{
		Config: map[string]any{
			"tls": map[string]any{"certFilePath": ""},
		},
	}
	sec := ResolveAPISecurity(apiPolicy, selection)
	assert.Equal(t, "/run/secrets/api.crt", sec.TLSCertFilePath)
}

func TestResolveAPISecurityDeterministicRegardlessOfMapOrder(t *testing.T) {
	apiPolicy := APIConfig{
		TLS:  APITLSPolicy{Mode: "server", CertFilePath: "c", KeyFilePath: "k"},
		Auth: APIAuthPolicy{Mode: "token", TokenFilePath: "t"},
	}
	base := map[string]any{
		"port": 3000,
		"tls":  map[string]any{"certFilePath": "override-cert"},
		"auth": map[string]any{"mode": "none"},
	}
	// Constructing the same logical config from freshly built maps (Go map
	// iteration order is randomized) must produce an identical result every
	// time.
	var first ResolvedAPISecurity
	for i := range 20 {
		clone := map[string]any{}
		maps.Copy(clone, base)
		sec := ResolveAPISecurity(
			apiPolicy,
			hostplugin.Selection{Config: clone},
		)
		if i == 0 {
			first = sec
			continue
		}
		require.Equal(t, first, sec)
	}
}

func TestEffectiveAPIPolicyLegacyFallback(t *testing.T) {
	t.Run("legacy fields fill an entirely unset api.tls", func(t *testing.T) {
		cfg := &Config{
			TlsCertFilePath: "/etc/dingo/legacy.crt",
			TlsKeyFilePath:  "/etc/dingo/legacy.key",
		}
		policy := cfg.EffectiveAPIPolicy()
		assert.Equal(t, APITLSPolicy{
			Mode:         "server",
			CertFilePath: "/etc/dingo/legacy.crt",
			KeyFilePath:  "/etc/dingo/legacy.key",
		}, policy.TLS)
	})

	t.Run(
		"api.tls set takes full precedence over legacy fields",
		func(t *testing.T) {
			cfg := &Config{
				TlsCertFilePath: "/etc/dingo/legacy.crt",
				TlsKeyFilePath:  "/etc/dingo/legacy.key",
				API: APIConfig{
					TLS: APITLSPolicy{Mode: "off"},
				},
			}
			policy := cfg.EffectiveAPIPolicy()
			assert.Equal(t, APITLSPolicy{Mode: "off"}, policy.TLS)
		},
	)

	t.Run("no legacy fields and no api.tls stays disabled", func(t *testing.T) {
		cfg := &Config{}
		policy := cfg.EffectiveAPIPolicy()
		assert.Equal(t, APITLSPolicy{}, policy.TLS)
	})
}

// TestValidateAPISecurityPartialMergeAcrossProviders exercises the
// "invalid merged certificate/key pair" acceptance scenario end to end via
// Config.validate: the top-level policy alone supplies only a certificate,
// leaving every provider's merged TLS pair incomplete except the one that
// fills the gap with its own override.
func TestValidateAPISecurityPartialMergeAcrossProviders(t *testing.T) {
	cfg := validTestConfig()
	cfg.API.TLS = APITLSPolicy{
		Mode:         "server",
		CertFilePath: "/run/secrets/api.crt",
	}
	cfg.Plugins.API.Mesh.Config["tls"] = map[string]any{
		"keyFilePath": "/run/secrets/mesh.key",
	}

	err := cfg.validate(cfg.RunMode, minUnprivilegedPort)
	require.Error(t, err)
	msg := err.Error()
	assert.Contains(t, msg, "plugins.api.utxorpc.config.tls")
	assert.Contains(t, msg, "plugins.api.blockfrost.config.tls")
	assert.NotContains(
		t,
		msg,
		"plugins.api.mesh.config.tls",
		"mesh supplied the missing keyFilePath itself and must not be "+
			"flagged",
	)
}
