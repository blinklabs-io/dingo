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
	"testing"

	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

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
			require.NoError(
				t,
				applyAPIPortCompatibilityEnvironment(cfg, tc.env),
			)
			require.NoError(
				t,
				plugin.ApplyEnvironment(
					plugin.CapabilityAPIMcp,
					&cfg.Plugins.API.Mcp,
					tc.env,
				),
			)
			require.Equal(t, tc.want, APIPluginPort(cfg.Plugins.API.Mcp))
		})
	}
}
