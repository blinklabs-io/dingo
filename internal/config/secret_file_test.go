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

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

const (
	koiosAPIKeyEnv     = "DINGO_KOIOS_PARITY_API_KEY"
	koiosAPIKeyFileEnv = "DINGO_KOIOS_PARITY_API_KEY_FILE"
)

// resolveKoiosAPIKey runs the cmd/dingo merge order -- YAML, environment,
// flags, then file resolution -- and returns the resolved Koios API key.
func resolveKoiosAPIKey(
	t *testing.T,
	yamlBody string,
	env map[string]string,
	args []string,
) (*Config, error) {
	t.Helper()
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	for _, name := range []string{koiosAPIKeyEnv, koiosAPIKeyFileEnv} {
		t.Setenv(name, "")
		require.NoError(t, os.Unsetenv(name))
	}
	for name, value := range env {
		t.Setenv(name, value)
	}
	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(yamlBody), 0o600))
	cfg, err := LoadConfig(configFile)
	if err != nil {
		return nil, err
	}
	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	require.NoError(t, cmd.ParseFlags(args))
	if err := ApplyFlags(cmd, cfg); err != nil {
		return nil, err
	}
	if err := cfg.ResolveSecretFiles(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func writeSecret(t *testing.T, value string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "secret")
	require.NoError(t, os.WriteFile(path, []byte(value+"\n"), 0o600))
	return path
}

func TestKoiosParityAPIKeyFilePrecedence(t *testing.T) {
	// Not t.Parallel: LoadConfig and ApplyFlags publish process-global
	// configuration and t.Setenv changes the environment.
	yamlFile := writeSecret(t, "yaml-file")
	envFile := writeSecret(t, "env-file")
	flagFile := writeSecret(t, "flag-file")
	koios := func(body string) string { return "koiosParity:\n" + body }
	for _, tc := range []struct {
		name    string
		yaml    string
		env     map[string]string
		args    []string
		want    string
		wantErr string
	}{
		{
			name: "yaml file",
			yaml: koios("  apiKeyFile: " + yamlFile + "\n"),
			want: "yaml-file",
		},
		{
			name:    "yaml literal and file",
			yaml:    koios("  apiKey: yaml\n  apiKeyFile: " + yamlFile + "\n"),
			wantErr: "koiosParity.apiKey",
		},
		{
			name: "env file replaces yaml literal",
			yaml: koios("  apiKey: yaml\n"),
			env:  map[string]string{koiosAPIKeyFileEnv: envFile},
			want: "env-file",
		},
		{
			name: "env literal replaces yaml file",
			yaml: koios("  apiKeyFile: " + yamlFile + "\n"),
			env:  map[string]string{koiosAPIKeyEnv: "env"},
			want: "env",
		},
		{
			name: "env literal and file",
			env: map[string]string{
				koiosAPIKeyEnv:     "env",
				koiosAPIKeyFileEnv: envFile,
			},
			wantErr: koiosAPIKeyFileEnv,
		},
		{
			name: "flag literal replaces env file",
			env:  map[string]string{koiosAPIKeyFileEnv: envFile},
			args: []string{"--koios-parity-api-key=flag"},
			want: "flag",
		},
		{
			name: "flag file replaces env literal",
			env:  map[string]string{koiosAPIKeyEnv: "env"},
			args: []string{"--koios-parity-api-key-file=" + flagFile},
			want: "flag-file",
		},
		{
			name: "flag literal and file",
			args: []string{
				"--koios-parity-api-key=flag",
				"--koios-parity-api-key-file=" + flagFile,
			},
			wantErr: "--koios-parity-api-key-file",
		},
		{
			name: "missing file",
			args: []string{
				"--koios-parity-api-key-file=" +
					filepath.Join(t.TempDir(), "missing"),
			},
			wantErr: "koiosParity.apiKeyFile",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := resolveKoiosAPIKey(t, tc.yaml, tc.env, tc.args)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, cfg.KoiosParity.APIKey)
			require.Empty(t, cfg.KoiosParity.APIKeyFile)
		})
	}
}

func TestResolveSecretFilesIsIdempotent(t *testing.T) {
	t.Parallel()
	cfg := &Config{}
	cfg.KoiosParity.APIKeyFile = writeSecret(t, "once")
	require.NoError(t, cfg.ResolveSecretFiles())
	require.NoError(t, cfg.ResolveSecretFiles())
	require.Equal(t, "once", cfg.KoiosParity.APIKey)
}
