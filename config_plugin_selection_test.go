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
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// apiCapabilities is every API capability the platform supports, derived from
// plugin.AllCapabilities rather than listed here. A capability added to the
// platform without a WithPluginSelection case and a syncCompatFields map entry
// is unreachable from configuration; deriving the list is what makes the tests
// below audit that class rather than a hand-maintained sample of it.
func apiCapabilities() []plugin.Capability {
	var ret []plugin.Capability
	for _, capability := range plugin.AllCapabilities() {
		if strings.HasPrefix(string(capability), "api.") {
			ret = append(ret, capability)
		}
	}
	return ret
}

// TestWithPluginSelectionAppliesEveryCapability pins that
// WithPluginSelection's switch handles every capability. A missing case makes
// the option a silent no-op: the selection is discarded, the capability never
// reaches pluginSelections, and apiPluginSelection then fails with "plugin
// selection is missing for capability <name>" for every node, whether or not
// that API was configured.
func TestWithPluginSelectionAppliesEveryCapability(t *testing.T) {
	t.Parallel()

	for _, capability := range plugin.AllCapabilities() {
		t.Run(string(capability), func(t *testing.T) {
			t.Parallel()

			cfg := NewConfig(
				WithPluginSelection(capability, plugin.Selection{
					Provider: "test-provider",
					Config:   map[string]any{"port": 4242},
				}),
			)
			selection, ok := cfg.pluginSelections[capability]
			require.Truef(
				t,
				ok,
				"pluginSelections has no entry for %s: "+
					"WithPluginSelection has no case for it, or "+
					"syncCompatFields omits it from the map",
				capability,
			)
			assert.Equal(t, "test-provider", selection.Provider)
			assert.Equal(
				t,
				map[string]any{"port": 4242},
				selection.Config,
			)
		})
	}
}

// TestAPIPluginSelectionResolvesEveryAPICapability covers the consumer side:
// Node.Run and reinitializeAPIServers call apiPluginSelection for each API
// capability before the port gate that makes it optional, so an unwired
// capability fails a node that never enabled that API.
func TestAPIPluginSelectionResolvesEveryAPICapability(t *testing.T) {
	t.Parallel()

	for _, capability := range apiCapabilities() {
		t.Run(string(capability), func(t *testing.T) {
			t.Parallel()

			cfg := NewConfig(
				WithPluginSelection(capability, plugin.Selection{
					Provider: "builtin",
					Config:   map[string]any{"port": 0},
				}),
			)
			n := &Node{config: cfg}
			_, port, err := n.apiPluginSelection(capability)
			require.NoErrorf(
				t,
				err,
				"apiPluginSelection(%s) must resolve a configured selection",
				capability,
			)
			assert.Equal(t, uint(0), port)
		})
	}
}

// TestNewConfigDefaultsResolveEveryAPICapability covers the path Node.Run and
// reinitializeAPIServers take on a node that configured no API at all.
// apiPluginSelection rejects an empty Provider as well as a missing map entry,
// and it runs before the port gate that makes each API optional, so a
// capability left out of NewConfig's defaults fails every node rather than
// only one that enabled that API.
func TestNewConfigDefaultsResolveEveryAPICapability(t *testing.T) {
	t.Parallel()

	n := &Node{config: NewConfig()}
	for _, capability := range apiCapabilities() {
		t.Run(string(capability), func(t *testing.T) {
			t.Parallel()

			selection, _, err := n.apiPluginSelection(capability)
			require.NoErrorf(
				t,
				err,
				"a default node must resolve %s",
				capability,
			)
			assert.NotEmpty(t, selection.Provider)
		})
	}
}
