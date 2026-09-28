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

// TestLoadConfig_LeavesProvenanceEmpty pins that LoadConfig itself never
// populates provenance, even though env/YAML values it merges still take
// effect. TestLoad_CompareFullStruct (config_test.go) DeepEquals the whole
// *Config LoadConfig returns against a hand-built struct literal; a
// literal cannot populate an unexported field, so LoadConfig populating
// provenance would break that test. Provenance is instead recorded by the
// separate RecordSourceProvenance, called after LoadConfig (see
// cmd/dingo/main.go).
func TestLoadConfig_LeavesProvenanceEmpty(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("CARDANO_NETWORK", "preprod")

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(
		t,
		os.WriteFile(configFile, []byte("network: mainnet\n"), 0o600),
	)

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)
	// The env var still resolves the value normally...
	require.Equal(t, "preprod", cfg.Network)
	// ...but LoadConfig alone must not have recorded why.
	require.Empty(t, cfg.Provenance())
}

// TestGatedFieldEnvProvenance pins the actual environment variable name(s)
// that control each gated field, as derived by envVarCandidatesForField.
// For EVERY gated field, it exercises EVERY candidate that function
// returns — not just one hand-picked name — and asserts both that the
// candidate actually changed the field's value AND that provenance
// recorded SourceEnv for it. That "every candidate must genuinely apply"
// invariant is what catches a wrong derived name: a review previously
// found envVarCandidatesForField returning a name for
// HistoryExpiry.Enabled that envconfig never actually honours (see
// TestHistoryExpiryEnabledEnv_FlatFormDoesNotApply for the regression
// guard on that specific wrong name).
func TestGatedFieldEnvProvenance(t *testing.T) {
	tests := []struct {
		field string
		value string
		got   func(cfg *Config) any
		want  any
	}{
		{
			field: "Network",
			value: "mainnet",
			got:   func(cfg *Config) any { return cfg.Network },
			want:  "mainnet",
		},
		{
			field: "NetworkMagic",
			value: "764824073",
			got:   func(cfg *Config) any { return cfg.NetworkMagic },
			want:  uint32(764824073),
		},
		{
			field: "ValidateHistorical",
			value: "false",
			got:   func(cfg *Config) any { return cfg.ValidateHistorical },
			want:  false,
		},
		{
			field: "StrictUtxoValidation",
			value: "false",
			got:   func(cfg *Config) any { return cfg.StrictUtxoValidation },
			want:  false,
		},
		{
			field: "StartEra",
			value: "dijkstra",
			got:   func(cfg *Config) any { return string(cfg.StartEra) },
			want:  "dijkstra",
		},
		{
			field: "StorageMode",
			value: "api",
			got:   func(cfg *Config) any { return cfg.StorageMode },
			want:  "api",
		},
		{
			field: "HistoryExpiry.Enabled",
			value: "true",
			got:   func(cfg *Config) any { return cfg.HistoryExpiry.Enabled },
			want:  true,
		},
		{
			field: "PledgeLeverageEnabled",
			value: "true",
			got:   func(cfg *Config) any { return cfg.PledgeLeverageEnabled },
			want:  true,
		},
		{
			field: "PledgeLeverage",
			value: "42",
			got:   func(cfg *Config) any { return cfg.PledgeLeverage },
			want:  uint(42),
		},
		{
			field: "FullPotRewardsEnabled",
			value: "true",
			got:   func(cfg *Config) any { return cfg.FullPotRewardsEnabled },
			want:  true,
		},
		{
			field: "DelegatorInactivityEnabled",
			value: "true",
			got:   func(cfg *Config) any { return cfg.DelegatorInactivityEnabled },
			want:  true,
		},
		{
			field: "DelegatorInactivity",
			value: "500",
			got:   func(cfg *Config) any { return cfg.DelegatorInactivity },
			want:  uint64(500),
		},
		{
			field: "MinPoolMargin",
			value: "150",
			got:   func(cfg *Config) any { return cfg.MinPoolMargin },
			want:  uint(150),
		},
	}

	for _, tt := range tests {
		candidates := envVarCandidatesForField(tt.field)
		require.NotEmpty(
			t, candidates,
			"no env var candidates derived for %s", tt.field,
		)
		for _, envVar := range candidates {
			t.Run(tt.field+"/"+envVar, func(t *testing.T) {
				resetGlobalConfig()
				t.Setenv("HOME", t.TempDir())
				t.Setenv(envVar, tt.value)

				configFile := filepath.Join(t.TempDir(), "dingo.yaml")
				require.NoError(t, os.WriteFile(configFile, nil, 0o600))

				cfg, err := LoadConfig(configFile)
				require.NoError(t, err)
				require.Equal(
					t, tt.want, tt.got(cfg),
					"candidate env var %s=%s did not set field %s;"+
						" this candidate is wrong",
					envVar, tt.value, tt.field,
				)
				require.NoError(t, cfg.RecordSourceProvenance(configFile))
				prov := cfg.Provenance()
				require.True(
					t, prov.IsExplicit(tt.field),
					"expected %s to be explicit via candidate env var %s",
					tt.field, envVar,
				)
				require.Equal(t, SourceEnv, prov[tt.field])
			})
		}
	}
}

// TestHistoryExpiryEnabledEnv_FlatFormDoesNotApply is a regression guard
// for the wrong candidate a review caught: envconfig prefixes a field
// nested under a container with no envconfig tag of its own using that
// CONTAINER's own derived key ("CARDANO_HISTORYEXPIRY") as the prefix, not
// a flat "CARDANO_" + the leaf's envconfig tag directly.
// "CARDANO_DINGO_HISTORY_EXPIRY_ENABLED" is therefore not a name
// envconfig.Process ever honours: setting it must change nothing and must
// not be recorded as explicit. The false-explicit direction (this one)
// is the dangerous one — a later gate check would otherwise trust that an
// operator set this field when nothing actually happened.
func TestHistoryExpiryEnabledEnv_FlatFormDoesNotApply(t *testing.T) {
	require.NotContains(
		t,
		envVarCandidatesForField("HistoryExpiry.Enabled"),
		"CARDANO_DINGO_HISTORY_EXPIRY_ENABLED",
	)

	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("CARDANO_DINGO_HISTORY_EXPIRY_ENABLED", "true")

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(configFile, nil, 0o600))

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)
	require.False(
		t, cfg.HistoryExpiry.Enabled,
		"the flat form must not set the value",
	)

	require.NoError(t, cfg.RecordSourceProvenance(configFile))
	require.False(
		t, cfg.Provenance().IsExplicit("HistoryExpiry.Enabled"),
	)
}

func TestApplyFlags_RecordsFlagProvenanceOnlyForChangedFlags(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(configFile, nil, 0o600))
	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)
	require.NoError(t, cfg.RecordSourceProvenance(configFile))

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	require.NoError(t, cmd.ParseFlags([]string{"--network=mainnet"}))
	require.NoError(t, ApplyFlags(cmd, cfg))

	prov := cfg.Provenance()
	require.True(t, prov.IsExplicit("Network"))
	require.Equal(t, SourceFlag, prov["Network"])
	// --network-magic was never passed, so it must not be marked explicit.
	require.False(t, prov.IsExplicit("NetworkMagic"))
}

func TestRecordSourceProvenance_YAMLTopLevelShape(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(
		t,
		os.WriteFile(configFile, []byte("network: mainnet\n"), 0o600),
	)

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)
	require.Equal(t, "mainnet", cfg.Network)
	require.NoError(t, cfg.RecordSourceProvenance(configFile))

	prov := cfg.Provenance()
	require.Equal(t, SourceYAML, prov["Network"])
	require.False(t, prov.IsExplicit("NetworkMagic"))
}

func TestRecordSourceProvenance_YAMLNestedConfigShape(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	yamlContent := "config:\n  network: mainnet\n  storageMode: api\n"
	require.NoError(
		t,
		os.WriteFile(configFile, []byte(yamlContent), 0o600),
	)

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)
	require.Equal(t, "mainnet", cfg.Network)
	require.Equal(t, "api", cfg.StorageMode)
	require.NoError(t, cfg.RecordSourceProvenance(configFile))

	prov := cfg.Provenance()
	require.Equal(t, SourceYAML, prov["Network"])
	require.Equal(t, SourceYAML, prov["StorageMode"])
}

func TestRecordSourceProvenance_NestedField(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	yamlContent := "historyExpiry:\n  enabled: true\n"
	require.NoError(
		t,
		os.WriteFile(configFile, []byte(yamlContent), 0o600),
	)

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)
	require.True(t, cfg.HistoryExpiry.Enabled)
	require.NoError(t, cfg.RecordSourceProvenance(configFile))

	prov := cfg.Provenance()
	require.Equal(t, SourceYAML, prov["HistoryExpiry.Enabled"])
}

func TestRecordSourceProvenance_NoConfigFileIsNotAnError(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("CARDANO_NETWORK", "preprod")

	cfg, err := LoadConfig("")
	require.NoError(t, err)
	require.Equal(t, "preprod", cfg.Network)

	require.NoError(t, cfg.RecordSourceProvenance(""))
	prov := cfg.Provenance()
	require.Equal(t, SourceEnv, prov["Network"])
}

func TestProvenance_EnvWinsOverYAML(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("CARDANO_NETWORK", "preprod")

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(
		t,
		os.WriteFile(configFile, []byte("network: mainnet\n"), 0o600),
	)

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)
	require.Equal(t, "preprod", cfg.Network)
	require.NoError(t, cfg.RecordSourceProvenance(configFile))
	require.Equal(t, SourceEnv, cfg.Provenance()["Network"])
}

func TestApplyFlags_FlagWinsOverEnvAndYAMLProvenance(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("CARDANO_NETWORK", "preprod")

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(
		t,
		os.WriteFile(configFile, []byte("network: preview\n"), 0o600),
	)

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)
	require.Equal(t, "preprod", cfg.Network)
	require.NoError(t, cfg.RecordSourceProvenance(configFile))
	require.Equal(t, SourceEnv, cfg.Provenance()["Network"])

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	require.NoError(t, cmd.ParseFlags([]string{"--network=mainnet"}))
	require.NoError(t, ApplyFlags(cmd, cfg))

	require.Equal(t, "mainnet", cfg.Network)
	require.Equal(t, SourceFlag, cfg.Provenance()["Network"])
}

func TestCloneConfig_CopiesProvenanceMapWithoutAliasing(t *testing.T) {
	cfg := &Config{}
	cfg.recordProvenance("Network", SourceFlag)

	clone := cloneConfig(cfg)
	clone.recordProvenance("Network", SourceYAML)
	clone.recordProvenance("StorageMode", SourceEnv)

	require.Equal(t, SourceFlag, cfg.Provenance()["Network"])
	require.False(t, cfg.Provenance().IsExplicit("StorageMode"))
	require.Equal(t, SourceYAML, clone.Provenance()["Network"])
	require.Equal(t, SourceEnv, clone.Provenance()["StorageMode"])
}

func TestCloneConfig_NilProvenanceStaysNil(t *testing.T) {
	cfg := &Config{}
	clone := cloneConfig(cfg)
	require.Nil(t, clone.provenance)
	require.Empty(t, clone.Provenance())
}
