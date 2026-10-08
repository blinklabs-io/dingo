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
	t.Setenv("HOME", t.TempDir())
	for _, name := range []string{
		"DINGO_KOIOS_PARITY_API_KEY",
		"DINGO_KOIOS_PARITY_API_KEY_FILE",
	} {
		t.Setenv(name, "")
		require.NoError(t, os.Unsetenv(name))
	}
	dir := t.TempDir()
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
}
