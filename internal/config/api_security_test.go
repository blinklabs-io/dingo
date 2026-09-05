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
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLoad_APITLSAuthYAMLDefaults covers the top-level api.tls/api.auth
// section: a YAML-only configuration populates it, with the per-provider
// plugins.api.* selections left untouched (the merge into each provider's
// own config happens at node composition, not here -- see node.go's
// apiProviderConfig).
func TestLoad_APITLSAuthYAMLDefaults(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(
		"api:\n"+
			"  tls:\n"+
			"    mode: server\n"+
			"    certFilePath: /run/secrets/api.crt\n"+
			"    keyFilePath: /run/secrets/api.key\n"+
			"  auth:\n"+
			"    mode: token\n"+
			"    tokenFilePath: /run/secrets/api-token\n",
	), 0o600))

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)

	require.NotNil(t, cfg.API.TLS.Mode)
	assert.Equal(t, "server", *cfg.API.TLS.Mode)
	require.NotNil(t, cfg.API.TLS.CertFilePath)
	assert.Equal(t, "/run/secrets/api.crt", *cfg.API.TLS.CertFilePath)
	require.NotNil(t, cfg.API.TLS.KeyFilePath)
	assert.Equal(t, "/run/secrets/api.key", *cfg.API.TLS.KeyFilePath)
	require.NotNil(t, cfg.API.Auth.Mode)
	assert.Equal(t, "token", *cfg.API.Auth.Mode)
	require.NotNil(t, cfg.API.Auth.TokenFilePath)
	assert.Equal(
		t, "/run/secrets/api-token", *cfg.API.Auth.TokenFilePath,
	)
}

// TestLoad_APITLSEnvironmentOverridesYAML covers source precedence
// (env over YAML) for the new api.tls fields, mirroring
// TestMempoolProviderSourcePrecedence's pattern for the existing plugin
// selection fields.
func TestLoad_APITLSEnvironmentOverridesYAML(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DINGO_API_TLS_MODE", "disabled")
	t.Setenv("DINGO_API_TLS_CERT_FILE_PATH", "/env/cert.pem")
	t.Setenv("DINGO_API_TLS_KEY_FILE_PATH", "/env/key.pem")

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(
		"api:\n"+
			"  tls:\n"+
			"    mode: server\n"+
			"    certFilePath: /yaml/cert.pem\n"+
			"    keyFilePath: /yaml/key.pem\n",
	), 0o600))

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)

	require.NotNil(t, cfg.API.TLS.Mode)
	assert.Equal(t, "disabled", *cfg.API.TLS.Mode, "environment overrides YAML")
	require.NotNil(t, cfg.API.TLS.CertFilePath)
	assert.Equal(t, "/env/cert.pem", *cfg.API.TLS.CertFilePath)
}

// TestLoad_APIAuthTokenHasNoEnvironmentBinding pins
// apiconfig.AuthPolicy.Token's documented security property: an inline
// secret is settable only via YAML, never via an environment variable
// (unlike every other api.auth/api.tls field). Without the `ignored:"true"`
// struct tag, envconfig.Process would still auto-derive and honor
// CARDANO_API_AUTH_TOKEN even with no explicit `envconfig` tag present --
// omitting the tag alone does not suppress the binding.
func TestLoad_APIAuthTokenHasNoEnvironmentBinding(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("CARDANO_API_AUTH_TOKEN", "leaked-secret")

	cfg, err := LoadConfig("")
	require.NoError(t, err)

	assert.Nil(
		t, cfg.API.Auth.Token,
		"api.auth.token must never be settable via environment variable",
	)
}

// TestApplyFlags_APITLSCLIOverridesEnvironment covers the top of the
// source precedence chain: a CLI flag beats both environment and YAML.
func TestApplyFlags_APITLSCLIOverridesEnvironment(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DINGO_API_TLS_MODE", "disabled")

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(
		"api:\n  tls:\n    mode: server\n",
	), 0o600))
	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)
	require.NotNil(t, cfg.API.TLS.Mode)
	assert.Equal(t, "disabled", *cfg.API.TLS.Mode)

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	require.NoError(t, cmd.ParseFlags([]string{"--api-tls-mode=server"}))
	require.NoError(t, ApplyFlags(cmd, cfg))

	require.NotNil(t, cfg.API.TLS.Mode)
	assert.Equal(t, "server", *cfg.API.TLS.Mode, "CLI overrides environment")
}

// TestLoad_APIAuthCLIExplicitDisable covers representing an explicit
// "disabled" as distinct from "unset": a CLI flag can turn off an
// environment/YAML-inherited auth mode rather than merely leaving it at
// its default.
func TestLoad_APIAuthCLIExplicitDisable(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(
		"api:\n  auth:\n    mode: token\n    tokenFilePath: /run/secrets/api-token\n",
	), 0o600))
	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	require.NoError(t, cmd.ParseFlags([]string{"--api-auth-mode=disabled"}))
	require.NoError(t, ApplyFlags(cmd, cfg))

	require.NotNil(t, cfg.API.Auth.Mode)
	assert.Equal(t, "disabled", *cfg.API.Auth.Mode)
	// The inherited tokenFilePath is untouched -- disabling mode does not
	// require also clearing the (now irrelevant) credential fields.
	require.NotNil(t, cfg.API.Auth.TokenFilePath)
	assert.Equal(t, "/run/secrets/api-token", *cfg.API.Auth.TokenFilePath)
}

// TestLoad_APIProviderConfigPerFieldOverride is the end-to-end shape from
// dingo#2998's issue body: a shared top-level api.tls default plus a
// provider-level override of only one nested field. It exercises the real
// LoadConfig YAML path together with apiconfig.MergeProviderConfig (the
// same merge node.go's apiProviderConfig performs at composition), rather
// than re-deriving the same fields by hand, so a regression in either
// layer's field names would be caught here.
func TestLoad_APIProviderConfigPerFieldOverride(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(
		"api:\n"+
			"  tls:\n"+
			"    mode: server\n"+
			"    certFilePath: /shared/cert.pem\n"+
			"    keyFilePath: /shared/key.pem\n"+
			"plugins:\n"+
			"  api:\n"+
			"    blockfrost:\n"+
			"      provider: builtin\n"+
			"      config:\n"+
			"        port: 3000\n"+
			"        tls:\n"+
			"          certFilePath: /blockfrost/cert.pem\n",
	), 0o600))

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)

	merged, err := apiconfig.MergeProviderConfig(
		cfg.Plugins.API.Blockfrost.Config,
		apiconfig.TLSPolicy{},
		cfg.API.TLS,
		cfg.API.Auth,
	)
	require.NoError(t, err)
	tlsPolicy, err := apiconfig.DecodeTLSPolicy(merged)
	require.NoError(t, err)
	effective, err := tlsPolicy.Resolve("test")
	require.NoError(t, err)

	assert.True(t, effective.Enabled)
	// certFilePath: the provider's own explicit override wins.
	assert.Equal(t, "/blockfrost/cert.pem", effective.CertFilePath)
	// keyFilePath: not set by the provider, inherited from the top-level
	// default -- overriding one nested field must not blow away the
	// other.
	assert.Equal(t, "/shared/key.pem", effective.KeyFilePath)
}

// TestLoad_APIProviderConfigExplicitDisable covers a provider explicitly
// opting out of an inherited top-level policy.
func TestLoad_APIProviderConfigExplicitDisable(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(
		"api:\n"+
			"  auth:\n"+
			"    mode: token\n"+
			"    token: shared-secret\n"+
			"plugins:\n"+
			"  api:\n"+
			"    mesh:\n"+
			"      provider: builtin\n"+
			"      config:\n"+
			"        port: 8080\n"+
			"        auth:\n"+
			"          mode: disabled\n",
	), 0o600))

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)

	merged, err := apiconfig.MergeProviderConfig(
		cfg.Plugins.API.Mesh.Config,
		apiconfig.TLSPolicy{},
		cfg.API.TLS,
		cfg.API.Auth,
	)
	require.NoError(t, err)
	authPolicy, err := apiconfig.DecodeAuthPolicy(merged)
	require.NoError(t, err)
	effective, err := authPolicy.Resolve("test")
	require.NoError(t, err)

	assert.False(t, effective.Enabled)
}

// TestLoad_APIProviderConfigInvalidMergedTLSPair covers the "invalid
// merged certificate/key pair" acceptance criterion: a top-level
// certFilePath with a provider-level keyFilePath removal (impossible to
// express by omission, so this uses an explicit provider override that
// still leaves the pair incomplete) fails Resolve with a path-qualified
// error.
func TestLoad_APIProviderConfigInvalidMergedTLSPair(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(
		"api:\n"+
			"  tls:\n"+
			"    mode: server\n"+
			"    certFilePath: /shared/cert.pem\n"+
			"plugins:\n"+
			"  api:\n"+
			"    utxorpc:\n"+
			"      provider: builtin\n"+
			"      config:\n"+
			"        port: 9090\n",
	), 0o600))

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)

	merged, err := apiconfig.MergeProviderConfig(
		cfg.Plugins.API.Utxorpc.Config,
		apiconfig.TLSPolicy{},
		cfg.API.TLS,
		cfg.API.Auth,
	)
	require.NoError(t, err)
	tlsPolicy, err := apiconfig.DecodeTLSPolicy(merged)
	require.NoError(t, err)
	_, err = tlsPolicy.Resolve("plugins.api.utxorpc.config.tls")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plugins.api.utxorpc.config.tls")
	assert.Contains(t, err.Error(), "must both be set")
}

// TestValidate_InvalidAPITLSMode covers the fail-fast top-level mode
// enum check: a typo in api.tls.mode is rejected once, with a clear
// message, by Validate rather than only surfacing later from each of the
// four API providers that would otherwise inherit it.
func TestValidate_InvalidAPITLSMode(t *testing.T) {
	resetGlobalConfig()
	cfg := GetConfig()
	cfg.ApplyDefaults()
	bogus := "bogus"
	cfg.API.TLS.Mode = &bogus

	err := cfg.Validate(RunModeLoad)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "api.tls.mode")
	assert.Contains(t, err.Error(), "invalid mode")
}

// TestValidate_InvalidAPIAuthMode is TestValidate_InvalidAPITLSMode's
// auth counterpart.
func TestValidate_InvalidAPIAuthMode(t *testing.T) {
	resetGlobalConfig()
	cfg := GetConfig()
	cfg.ApplyDefaults()
	bogus := "bogus"
	cfg.API.Auth.Mode = &bogus

	err := cfg.Validate(RunModeLoad)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "api.auth.mode")
	assert.Contains(t, err.Error(), "invalid mode")
}

// TestGetConfigSnapshotDoesNotShareAPIPolicy asserts GetConfig's snapshot
// deep-copies api.tls/api.auth pointer fields, matching the existing
// nested-plugin-config isolation guarantee: mutating one snapshot's
// policy must not be visible through another.
func TestGetConfigSnapshotDoesNotShareAPIPolicy(t *testing.T) {
	resetGlobalConfig()
	mode := "server"
	globalConfig.API.TLS.Mode = &mode

	snapshotA := GetConfig()
	snapshotB := GetConfig()
	require.NotNil(t, snapshotA.API.TLS.Mode)
	require.NotNil(t, snapshotB.API.TLS.Mode)
	require.NotSame(t, snapshotA.API.TLS.Mode, snapshotB.API.TLS.Mode)

	disabled := "disabled"
	snapshotA.API.TLS.Mode = &disabled
	assert.Equal(t, "server", *snapshotB.API.TLS.Mode)
}
