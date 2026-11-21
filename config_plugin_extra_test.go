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

	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

func TestWithPluginSelectionStoresUnknownCapability(t *testing.T) {
	t.Parallel()

	cfg := NewConfig(
		WithPluginSelection("example.widget/v0", plugin.Selection{
			Provider: "basic",
			Config:   map[string]any{"greeting": "hi"},
		}),
	)
	// Selections carry no contract version, so a lookup at any version of the
	// name finds it.
	for _, capability := range []plugin.Capability{
		"example.widget/v0", "example.widget/v3",
	} {
		selection, ok := cfg.pluginSelection(capability)
		require.True(t, ok, capability)
		require.Equal(t, "basic", selection.Provider)
		require.Equal(t, map[string]any{"greeting": "hi"}, selection.Config)
	}
	_, ok := cfg.pluginSelection("example.other/v0")
	require.False(t, ok)
}

func TestWithPluginSelectionIgnoresMalformedCapability(t *testing.T) {
	t.Parallel()

	cfg := NewConfig(
		WithPluginSelection("Example.Widget", plugin.Selection{Provider: "x"}),
	)
	require.Empty(t, cfg.cfg.Plugins.Extra)
}

func TestPluginSelectionLookupFindsBuiltIns(t *testing.T) {
	t.Parallel()

	cfg := NewConfig(
		WithPluginSelection(plugin.CapabilityMempool, plugin.Selection{
			Provider: "fifo",
		}),
	)
	selection, ok := cfg.pluginSelection(plugin.CapabilityMempool)
	require.True(t, ok)
	require.Equal(t, "fifo", selection.Provider)
	require.Empty(t, cfg.cfg.Plugins.Extra, "built-ins never land in Extra")
}

func TestNewRejectsSelectionForCapabilityHostDoesNotDeclare(t *testing.T) {
	t.Parallel()

	_, err := New(NewConfig(
		WithPluginSelection("example.widget/v0", plugin.Selection{
			Provider: "basic",
		}),
	))
	require.ErrorContains(t, err, "invalid plugin selection")
	require.ErrorContains(t, err, "example.widget")
}
