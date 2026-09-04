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

package config

import (
	"os"
	"path/filepath"
	"testing"

	hostplugin "github.com/blinklabs-io/dingo/plugin"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

// unsetAPIExposureEnv keeps tests that exercise the API exposure defaults
// from inheriting an operator's own overrides, the same way
// unsetDebugBindAddrEnv does for the pprof listener.
func unsetAPIExposureEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"DINGO_API_BIND_ADDR",
		"DINGO_CORS_ALLOWED_ORIGINS",
	} {
		t.Setenv(name, "")
		require.NoError(t, os.Unsetenv(name))
	}
}

// TestAPIBindAddressDefaultsToLoopback pins the fix for #3498: the
// Blockfrost/Mesh/UTxO-RPC listeners bind loopback by default, and do so
// independently of bindAddr, which stays on the wildcard the relay/NtN
// and metrics listeners need.
func TestAPIBindAddressDefaultsToLoopback(t *testing.T) {
	resetGlobalConfig()
	unsetDebugBindAddrEnv(t)
	unsetAPIExposureEnv(t)
	t.Setenv("HOME", t.TempDir())

	cfg, err := LoadConfig("")
	require.NoError(t, err)
	cfg.ApplyDefaults()

	require.Equal(t, "0.0.0.0", cfg.BindAddr)
	require.Equal(t, DefaultAPIBindAddr, cfg.APIBindAddr)
	require.Equal(t, "127.0.0.1", cfg.APIBindAddr)
}

// TestAPIBindAddressDoesNotInheritBindAddr asserts that widening the
// public bind address for the relay does not silently widen the API
// listeners with it. This is the whole point of a separate field.
func TestAPIBindAddressDoesNotInheritBindAddr(t *testing.T) {
	resetGlobalConfig()
	unsetDebugBindAddrEnv(t)
	unsetAPIExposureEnv(t)
	t.Setenv("HOME", t.TempDir())
	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(
		configFile,
		[]byte("bindAddr: 0.0.0.0\n"),
		0o600,
	))

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)
	cfg.ApplyDefaults()

	require.Equal(t, "0.0.0.0", cfg.BindAddr)
	require.Equal(t, DefaultAPIBindAddr, cfg.APIBindAddr)
}

// TestAPIBindAddressExplicitOverridePrecedence asserts an operator can
// widen the API listeners on purpose, through YAML, the environment, or
// the CLI, in the repository's usual CLI > env > YAML > default order.
func TestAPIBindAddressExplicitOverridePrecedence(t *testing.T) {
	resetGlobalConfig()
	unsetDebugBindAddrEnv(t)
	unsetAPIExposureEnv(t)
	t.Setenv("HOME", t.TempDir())
	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(
		configFile,
		[]byte("apiBindAddr: 127.0.0.2\n"),
		0o600,
	))

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.2", cfg.APIBindAddr)

	t.Setenv("DINGO_API_BIND_ADDR", "127.0.0.3")
	cfg, err = LoadConfig(configFile)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.3", cfg.APIBindAddr)

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	require.NoError(t, cmd.ParseFlags([]string{
		"--api-bind-addr=0.0.0.0",
	}))
	require.NoError(t, ApplyFlags(cmd, cfg))
	require.Equal(t, "0.0.0.0", cfg.APIBindAddr)
}

// TestCORSAllowedOriginsDefaultsToDisabled pins the second half of
// #3498: the shared CORS default no longer echoes every origin back.
// httpcors treats an empty list as "send no CORS headers".
func TestCORSAllowedOriginsDefaultsToDisabled(t *testing.T) {
	resetGlobalConfig()
	unsetDebugBindAddrEnv(t)
	unsetAPIExposureEnv(t)
	t.Setenv("HOME", t.TempDir())

	cfg, err := LoadConfig("")
	require.NoError(t, err)
	cfg.ApplyDefaults()

	require.Empty(t, cfg.CORSAllowedOrigins)
}

// TestCORSAllowedOriginsWildcardStaysExplicit asserts an operator who
// wants the old behavior still gets it by asking for it.
func TestCORSAllowedOriginsWildcardStaysExplicit(t *testing.T) {
	resetGlobalConfig()
	unsetDebugBindAddrEnv(t)
	unsetAPIExposureEnv(t)
	t.Setenv("HOME", t.TempDir())
	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(
		configFile,
		[]byte("corsAllowedOrigins:\n  - \"*\"\n"),
		0o600,
	))

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)
	cfg.ApplyDefaults()
	require.Equal(t, []string{"*"}, cfg.CORSAllowedOrigins)

	t.Setenv("DINGO_CORS_ALLOWED_ORIGINS", "https://wallet.example")
	cfg, err = LoadConfig(configFile)
	require.NoError(t, err)
	require.Equal(t, []string{"https://wallet.example"}, cfg.CORSAllowedOrigins)

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	require.NoError(t, cmd.ParseFlags([]string{
		"--cors-allowed-origins=https://explorer.example",
	}))
	require.NoError(t, ApplyFlags(cmd, cfg))
	require.Equal(
		t,
		[]string{"https://explorer.example"},
		cfg.CORSAllowedOrigins,
	)
}

// TestAPIPluginHostPerProviderOverride asserts a per-plugin host beats
// the shared apiBindAddr default, so one listener can be exposed without
// widening the other two.
func TestAPIPluginHostPerProviderOverride(t *testing.T) {
	selection := hostplugin.Selection{
		Provider: "builtin",
		Config:   map[string]any{"port": uint(3000), "host": "0.0.0.0"},
	}
	require.Equal(
		t,
		"0.0.0.0",
		APIPluginHost(selection, DefaultAPIBindAddr),
	)

	noHost := hostplugin.Selection{
		Provider: "builtin",
		Config:   map[string]any{"port": uint(3000)},
	}
	require.Equal(
		t,
		DefaultAPIBindAddr,
		APIPluginHost(noHost, DefaultAPIBindAddr),
	)
}
