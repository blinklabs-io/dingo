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

package apiconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMergeTLSFieldByField(t *testing.T) {
	base := TLSPolicy{
		Mode:         new(string(TLSModeServer)),
		CertFilePath: new("/base/cert.pem"),
		KeyFilePath:  new("/base/key.pem"),
	}
	// Overriding only certFilePath must not blow away the inherited
	// keyFilePath -- this is the "per-field merge" requirement from
	// dingo#2998.
	override := TLSPolicy{CertFilePath: new("/override/cert.pem")}
	merged := MergeTLS(base, override)
	require.NotNil(t, merged.Mode)
	assert.Equal(t, string(TLSModeServer), *merged.Mode)
	require.NotNil(t, merged.CertFilePath)
	assert.Equal(t, "/override/cert.pem", *merged.CertFilePath)
	require.NotNil(t, merged.KeyFilePath)
	assert.Equal(t, "/base/key.pem", *merged.KeyFilePath)
}

func TestMergeTLSExplicitDisableOverridesInheritedEnable(t *testing.T) {
	base := TLSPolicy{
		Mode:         new(string(TLSModeServer)),
		CertFilePath: new("/base/cert.pem"),
		KeyFilePath:  new("/base/key.pem"),
	}
	override := TLSPolicy{Mode: new(string(TLSModeDisabled))}
	merged := MergeTLS(base, override)
	effective, err := merged.Resolve("test")
	require.NoError(t, err)
	assert.False(t, effective.Enabled)
}

func TestMergeTLSUnsetInheritsBaseUnchanged(t *testing.T) {
	base := TLSPolicy{
		Mode:         new(string(TLSModeServer)),
		CertFilePath: new("/base/cert.pem"),
		KeyFilePath:  new("/base/key.pem"),
	}
	merged := MergeTLS(base, TLSPolicy{})
	effective, err := merged.Resolve("test")
	require.NoError(t, err)
	assert.True(t, effective.Enabled)
	assert.Equal(t, "/base/cert.pem", effective.CertFilePath)
	assert.Equal(t, "/base/key.pem", effective.KeyFilePath)
}

func TestTLSResolveDefaultsToDisabled(t *testing.T) {
	effective, err := TLSPolicy{}.Resolve("plugins.api.mesh.config.tls")
	require.NoError(t, err)
	assert.Equal(t, EffectiveTLS{}, effective)
}

func TestTLSResolvePartialPairFails(t *testing.T) {
	_, err := TLSPolicy{
		Mode:         new(string(TLSModeServer)),
		CertFilePath: new("/only/cert.pem"),
	}.Resolve("plugins.api.blockfrost.config.tls")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plugins.api.blockfrost.config.tls")
	assert.Contains(t, err.Error(), "must both be set")
}

func TestTLSResolveServerRequiresCertAndKey(t *testing.T) {
	_, err := TLSPolicy{Mode: new(string(TLSModeServer))}.Resolve("api.tls")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "required")
}

func TestTLSResolveInvalidMode(t *testing.T) {
	_, err := TLSPolicy{Mode: new("bogus")}.Resolve("api.tls")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid mode")
}

func TestMergeProviderConfigPrecedenceProviderOverTopLevelOverLegacy(
	t *testing.T,
) {
	legacyTLS := TLSPolicy{
		Mode:         new(string(TLSModeServer)),
		CertFilePath: new("/legacy/cert.pem"),
		KeyFilePath:  new("/legacy/key.pem"),
	}
	apiTLS := TLSPolicy{
		Mode:         new(string(TLSModeServer)),
		CertFilePath: new("/toplevel/cert.pem"),
		KeyFilePath:  new("/toplevel/key.pem"),
	}
	raw := map[string]any{
		"port": 3000,
		"tls": map[string]any{
			"certFilePath": "/provider/cert.pem",
		},
	}
	merged, err := MergeProviderConfig(raw, legacyTLS, apiTLS)
	require.NoError(t, err)
	// Untouched keys pass through.
	assert.Equal(t, 3000, merged["port"])
	providerTLS, err := DecodeTLSPolicy(merged)
	require.NoError(t, err)
	effective, err := providerTLS.Resolve("test")
	require.NoError(t, err)
	assert.True(t, effective.Enabled)
	// certFilePath: explicit provider override wins.
	assert.Equal(t, "/provider/cert.pem", effective.CertFilePath)
	// keyFilePath: not set by provider, falls through to the top-level
	// default (not the legacy value, since top-level is higher priority).
	assert.Equal(t, "/toplevel/key.pem", effective.KeyFilePath)
}

func TestMergeProviderConfigLegacyOnlyAppliesWhenNothingElseSet(t *testing.T) {
	legacyTLS := TLSPolicy{
		Mode:         new(string(TLSModeServer)),
		CertFilePath: new("/legacy/cert.pem"),
		KeyFilePath:  new("/legacy/key.pem"),
	}
	merged, err := MergeProviderConfig(
		map[string]any{"port": 9090},
		legacyTLS,
		TLSPolicy{},
	)
	require.NoError(t, err)
	providerTLS, err := DecodeTLSPolicy(merged)
	require.NoError(t, err)
	effective, err := providerTLS.Resolve("test")
	require.NoError(t, err)
	assert.True(t, effective.Enabled)
	assert.Equal(t, "/legacy/cert.pem", effective.CertFilePath)
}

func TestMergeProviderConfigDoesNotMutateInput(t *testing.T) {
	raw := map[string]any{
		"tls": map[string]any{"certFilePath": "/provider/cert.pem"},
	}
	apiTLS := TLSPolicy{Mode: new(string(TLSModeServer))}
	_, err := MergeProviderConfig(raw, TLSPolicy{}, apiTLS)
	require.NoError(t, err)
	// raw's own "tls" section must be untouched by the merge.
	tlsSection, ok := raw["tls"].(map[string]any)
	require.True(t, ok)
	assert.Len(t, tlsSection, 1)
	assert.Equal(t, "/provider/cert.pem", tlsSection["certFilePath"])
}

func TestMergeProviderConfigDeterministicRegardlessOfInputMapOrder(
	t *testing.T,
) {
	legacyTLS := TLSPolicy{}
	apiTLS := TLSPolicy{
		Mode:         new(string(TLSModeServer)),
		CertFilePath: new("/a/cert.pem"),
		KeyFilePath:  new("/a/key.pem"),
	}

	// Two maps built by inserting keys in different orders (Go map literals
	// don't guarantee iteration order, but constructing them differently
	// here exercises the same risk MergeProviderConfig must avoid: any
	// dependency on map iteration order rather than named-field merging).
	rawA := map[string]any{}
	rawA["port"] = 8080
	rawA["tls"] = map[string]any{"certFilePath": "/provider/cert.pem"}

	rawB := map[string]any{}
	rawB["tls"] = map[string]any{"certFilePath": "/provider/cert.pem"}
	rawB["port"] = 8080

	mergedA, err := MergeProviderConfig(rawA, legacyTLS, apiTLS)
	require.NoError(t, err)
	mergedB, err := MergeProviderConfig(rawB, legacyTLS, apiTLS)
	require.NoError(t, err)

	tlsA, err := DecodeTLSPolicy(mergedA)
	require.NoError(t, err)
	tlsB, err := DecodeTLSPolicy(mergedB)
	require.NoError(t, err)
	effA, err := tlsA.Resolve("test")
	require.NoError(t, err)
	effB, err := tlsB.Resolve("test")
	require.NoError(t, err)
	assert.Equal(t, effA, effB)
}
