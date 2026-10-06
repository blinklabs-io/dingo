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

	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestLoadMCPAuthCompatibility(t *testing.T) {
	// Not t.Parallel: LoadConfig publishes process-global configuration and t.Setenv changes the environment.
	for _, tc := range []struct {
		name      string
		canonical *string
		want      string
	}{
		{name: "alias", want: "00123:true secret"},
		{name: "canonical", canonical: new("canonical-token"), want: "canonical-token"},
		{name: "explicit-empty-canonical", canonical: new(""), want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetGlobalConfig()
			t.Setenv("DINGO_MCP_AUTH_TOKEN", "00123:true secret")
			if tc.canonical != nil {
				t.Setenv(
					"DINGO_PLUGINS_API_MCP_CONFIG_AUTH_TOKEN",
					*tc.canonical,
				)
			} else {
				old, ok := os.LookupEnv("DINGO_PLUGINS_API_MCP_CONFIG_AUTH_TOKEN")
				require.NoError(t, os.Unsetenv("DINGO_PLUGINS_API_MCP_CONFIG_AUTH_TOKEN"))
				t.Cleanup(func() {
					if ok {
						require.NoError(t, os.Setenv("DINGO_PLUGINS_API_MCP_CONFIG_AUTH_TOKEN", old))
					}
				})
			}
			path := filepath.Join(t.TempDir(), "dingo.yaml")
			require.NoError(
				t,
				os.WriteFile(
					path,
					[]byte(
						"plugins:\n  api:\n    mcp:\n      config:\n        authToken: yaml-token\n",
					),
					0600,
				),
			)
			cfg, err := LoadConfig(path)
			require.NoError(t, err)
			encoded, err := yaml.Marshal(cfg.Plugins.API.Mcp.Config)
			require.NoError(t, err)
			var resolved struct {
				AuthToken string `yaml:"authToken"`
			}
			require.NoError(t, yaml.Unmarshal(encoded, &resolved))
			require.Equal(t, tc.want, resolved.AuthToken)
		})
	}
}

func TestMCPPortRequiresExplicitOptIn(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		env  []string
		want uint
	}{
		{name: "default"},
		{name: "legacy opt-in", env: []string{"DINGO_MCP_PORT=8088"}, want: 8088},
		{name: "canonical opt-in", env: []string{"DINGO_PLUGINS_API_MCP_CONFIG_PORT=8088"}, want: 8088},
		{name: "canonical disable overrides alias", env: []string{"DINGO_MCP_PORT=8088", "DINGO_PLUGINS_API_MCP_CONFIG_PORT=0"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := &Config{Plugins: defaultPluginsConfig()}
			require.NoError(t, applyAPIPortCompatibilityEnvironment(cfg, tc.env))
			require.NoError(
				t,
				plugin.ApplyEnvironment(plugin.CapabilityAPIMcp, &cfg.Plugins.API.Mcp, tc.env),
			)
			require.Equal(t, tc.want, APIPluginPort(cfg.Plugins.API.Mcp))
		})
	}
}
