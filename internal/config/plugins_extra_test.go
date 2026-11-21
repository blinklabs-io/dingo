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
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	hostplugin "github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func decodePlugins(t *testing.T, doc string) (PluginsConfig, error) {
	t.Helper()
	cfg := defaultPluginsConfig()
	err := yaml.Unmarshal([]byte(doc), &cfg)
	return cfg, err
}

func TestPluginsYAMLNestsUnknownCapabilityUnderNameSegments(t *testing.T) {
	t.Parallel()

	cfg, err := decodePlugins(t, `
storage:
  blob:
    provider: other
example:
  widget:
    provider: basic
    config:
      greeting: hi
cardano:
  query:
    provider: remote
`)
	require.NoError(t, err)
	require.Equal(t, "other", cfg.Storage.Blob.Provider)
	require.Equal(
		t,
		defaultPluginsConfig().Storage.Metadata,
		cfg.Storage.Metadata,
	)
	require.Equal(t, map[string]hostplugin.Selection{
		"example.widget": {
			Provider: "basic",
			Config:   map[string]any{"greeting": "hi"},
		},
		"cardano.query": {Provider: "remote"},
	}, cfg.Extra)
}

func TestPluginsYAMLBuiltInsDecodeWithoutExtras(t *testing.T) {
	t.Parallel()

	cfg, err := decodePlugins(t, `
mempool:
  provider: dag
api:
  kupo:
    provider: kupo
    config:
      port: 1442
`)
	require.NoError(t, err)
	require.Nil(t, cfg.Extra)
	require.Equal(t, "dag", cfg.Mempool.Provider)
	require.Equal(t, 1442, cfg.API.Kupo.Config["port"])
}

func TestPluginsYAMLStrictness(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"typo in built-in selection key": "storage:\n  blob:\n    provder: x\n",
		"unknown key beside built-ins":   "bogus: 1\n",
		"scalar under unknown namespace": "bogus:\n  foo: 1\n",
		"typo in extra selection key":    "example:\n  widget:\n    provider: a\n    cnfig: {}\n",
		"typo key in extra namespace":    "example:\n  widget:\n    providr: a\n",
		"built-in without a field":       "storage:\n  immutable:\n    provider: x\n",
		"uppercase capability segment":   "Example:\n  widget:\n    provider: a\n",
		"plugins is a scalar":            "5\n",
	}
	for name, doc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := decodePlugins(t, doc)
			require.Error(t, err)
		})
	}
}

func TestLoadConfigExtraPluginSelectionPrecedence(t *testing.T) {
	// Not t.Parallel: LoadConfig and resetGlobalConfig replace process-global
	// configuration state, and t.Setenv is process-wide.
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DINGO_PLUGINS_EXAMPLE_WIDGET_PROVIDER", "env-provider")
	t.Setenv("DINGO_PLUGINS_EXAMPLE_WIDGET_CONFIG_SUFFIX", "from-env")
	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(`plugins:
  example:
    widget:
      provider: yaml-provider
      config:
        greeting: from-yaml
        suffix: from-yaml
`), 0o600))
	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)
	require.Equal(t, hostplugin.Selection{
		Provider: "env-provider",
		Config: map[string]any{
			"greeting": "from-yaml",
			"suffix":   "from-env",
		},
	}, cfg.Plugins.Extra["example.widget"])

	cloned := cloneConfig(cfg)
	cloned.Plugins.Extra["example.widget"].Config["greeting"] = "changed"
	require.Equal(
		t,
		"from-yaml",
		cfg.Plugins.Extra["example.widget"].Config["greeting"],
	)
}

func TestConfigLogRedactsExtraPluginSelections(t *testing.T) {
	t.Parallel()

	cfg := &Config{Plugins: defaultPluginsConfig()}
	cfg.Plugins.Extra = map[string]hostplugin.Selection{
		"example.widget": {
			Provider: "basic",
			Config: map[string]any{
				"password": "SENTINEL-EXTRA-PASSWORD",
				"opaque":   "SENTINEL-EXTRA-UNCLASSIFIED",
				"port":     8080,
			},
		},
	}
	var buf bytes.Buffer
	slog.New(slog.NewTextHandler(&buf, nil)).Info("cfg", "config", cfg)
	out := buf.String()
	require.NotContains(t, out, "SENTINEL-EXTRA-PASSWORD")
	require.NotContains(t, out, "SENTINEL-EXTRA-UNCLASSIFIED")
	require.Contains(t, out, "basic")
	require.Contains(t, out, "8080")
}
