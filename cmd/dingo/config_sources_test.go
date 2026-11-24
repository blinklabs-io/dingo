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

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestMergeConfigSourcesReadsSecretFiles(t *testing.T) {
	// Not t.Parallel: LoadConfig and ApplyFlags publish process-global
	// configuration and t.Setenv changes the environment.
	previous := config.GetConfig()
	t.Cleanup(func() { config.PublishConfig(previous) })
	t.Setenv("HOME", t.TempDir())
	for _, name := range []string{
		"DINGO_KOIOS_PARITY_API_KEY",
		"DINGO_KOIOS_PARITY_API_KEY_FILE",
		"CARDANO_KOIOSPARITY_DINGO_KOIOS_PARITY_API_KEY",
		"CARDANO_KOIOSPARITY_DINGO_KOIOS_PARITY_API_KEY_FILE",
	} {
		t.Setenv(name, "")
		require.NoError(t, os.Unsetenv(name))
	}
	dir := t.TempDir()
	require.NoError(t, os.Chmod(dir, 0o700))
	keyFile := filepath.Join(dir, "koios-key")
	require.NoError(t, os.WriteFile(keyFile, []byte("from-file\n"), 0o600))
	configFile := filepath.Join(dir, "dingo.yaml")
	require.NoError(t, os.WriteFile(
		configFile,
		[]byte("koiosParity:\n  apiKey: from-yaml\n"),
		0o600,
	))

	root := &cobra.Command{Use: "dingo"}
	config.RegisterFlags(root)
	require.NoError(t, root.ParseFlags([]string{
		"--koios-parity-api-key-file=" + keyFile,
	}))
	cfg, err := mergeConfigSources(root, configFile)
	require.NoError(t, err)
	require.Equal(t, "from-file", cfg.KoiosParity.APIKey)
	require.Empty(t, cfg.KoiosParity.APIKeyFile)
	// Readers of the process-wide snapshot must see the resolved value, not
	// the file path the merge consumed.
	published := config.GetConfig()
	require.Equal(t, "from-file", published.KoiosParity.APIKey)
	require.Empty(t, published.KoiosParity.APIKeyFile)
}

func TestBlockPipelineDefaultsPreserveExplicitSources(t *testing.T) {
	// Not t.Parallel: config merging publishes process-global configuration
	// and the environment is process-global.
	tests := []struct {
		name  string
		yaml  string
		env   string
		flags []string
		want  bool
	}{
		{name: "core default", want: true},
		{name: "api default", yaml: "storageMode: api\n"},
		{name: "musashi default", yaml: "network: musashi\n"},
		{name: "yaml false", yaml: "blockPipelineEnabled: false\n"},
		{name: "environment false", env: "false"},
		{
			name:  "flag false",
			flags: []string{"--block-pipeline-enabled=false"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			previous := config.GetConfig()
			t.Cleanup(func() { config.PublishConfig(previous) })
			t.Setenv("HOME", t.TempDir())
			for _, name := range []string{
				"DINGO_BLOCK_PIPELINE_ENABLED",
				"CARDANO_DINGO_BLOCK_PIPELINE_ENABLED",
			} {
				t.Setenv(name, "")
				require.NoError(t, os.Unsetenv(name))
			}
			if tt.env != "" {
				t.Setenv("DINGO_BLOCK_PIPELINE_ENABLED", tt.env)
			}

			configFile := filepath.Join(t.TempDir(), "dingo.yaml")
			require.NoError(
				t,
				os.WriteFile(configFile, []byte(tt.yaml), 0o600),
			)
			root := &cobra.Command{Use: "dingo"}
			config.RegisterFlags(root)
			require.NoError(t, root.ParseFlags(tt.flags))
			cfg, err := mergeConfigSources(root, configFile)
			require.NoError(t, err)
			cfg.ApplyDefaults()
			if cfg.BlockPipelineEnabled != tt.want {
				t.Fatalf(
					"block pipeline enabled = %t, want %t",
					cfg.BlockPipelineEnabled,
					tt.want,
				)
			}
		})
	}
}
