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
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLoad_APITLSYAMLDefaults covers the top-level api.tls section: a
// YAML-only configuration populates it, with the per-provider
// plugins.api.* selections left untouched (the merge into each provider's
// own config happens at node composition, not here -- see node.go's
// apiProviderConfig).
func TestLoad_APITLSYAMLDefaults(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(
		"api:\n"+
			"  tls:\n"+
			"    mode: server\n"+
			"    certFilePath: /run/secrets/api.crt\n"+
			"    keyFilePath: /run/secrets/api.key\n",
	), 0o600))

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)

	require.NotNil(t, cfg.API.TLS.Mode)
	assert.Equal(t, "server", *cfg.API.TLS.Mode)
	require.NotNil(t, cfg.API.TLS.CertFilePath)
	assert.Equal(t, "/run/secrets/api.crt", *cfg.API.TLS.CertFilePath)
	require.NotNil(t, cfg.API.TLS.KeyFilePath)
	assert.Equal(t, "/run/secrets/api.key", *cfg.API.TLS.KeyFilePath)
}

// TestLoad_APITLSEnvironmentOverridesYAML covers source precedence
// (env over YAML) for the new api.tls fields, mirroring
// TestMempoolProviderSourcePrecedence's pattern for the existing plugin
// selection fields.
func TestLoad_APITLSEnvironmentOverridesYAML(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DINGO_API_TLS_MODE", "disabled")
	t.Setenv("DINGO_API_TLS_CERT_FILE_PATH", "/env/cert.pem")
	t.Setenv("DINGO_API_TLS_KEY_FILE_PATH", "/env/key.pem")

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(
		"api:\n"+
			"  tls:\n"+
			"    mode: server\n"+
			"    certFilePath: /yaml/cert.pem\n"+
			"    keyFilePath: /yaml/key.pem\n",
	), 0o600))

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)

	require.NotNil(t, cfg.API.TLS.Mode)
	assert.Equal(t, "disabled", *cfg.API.TLS.Mode, "environment overrides YAML")
	require.NotNil(t, cfg.API.TLS.CertFilePath)
	assert.Equal(t, "/env/cert.pem", *cfg.API.TLS.CertFilePath)
}

// TestApplyFlags_APITLSCLIOverridesEnvironment covers the top of the
// source precedence chain: a CLI flag beats both environment and YAML.
func TestApplyFlags_APITLSCLIOverridesEnvironment(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DINGO_API_TLS_MODE", "disabled")

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(
		"api:\n  tls:\n    mode: server\n",
	), 0o600))
	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)
	require.NotNil(t, cfg.API.TLS.Mode)
	assert.Equal(t, "disabled", *cfg.API.TLS.Mode)

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	require.NoError(t, cmd.ParseFlags([]string{"--api-tls-mode=server"}))
	require.NoError(t, ApplyFlags(cmd, cfg))

	require.NotNil(t, cfg.API.TLS.Mode)
	assert.Equal(t, "server", *cfg.API.TLS.Mode, "CLI overrides environment")
}

// TestLoad_APIProviderConfigPerFieldOverride is the end-to-end shape from
// issue body: a shared top-level api.tls default plus a
// provider-level override of only one nested field. It exercises the real
// LoadConfig YAML path together with apiconfig.MergeProviderConfig (the
// same merge node.go's apiProviderConfig performs at composition), rather
// than re-deriving the same fields by hand, so a regression in either
// layer's field names would be caught here.
func TestLoad_APIProviderConfigPerFieldOverride(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(
		"api:\n"+
			"  tls:\n"+
			"    mode: server\n"+
			"    certFilePath: /shared/cert.pem\n"+
			"    keyFilePath: /shared/key.pem\n"+
			"plugins:\n"+
			"  api:\n"+
			"    blockfrost:\n"+
			"      provider: builtin\n"+
			"      config:\n"+
			"        port: 3000\n"+
			"        tls:\n"+
			"          certFilePath: /blockfrost/cert.pem\n",
	), 0o600))

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)

	merged, err := apiconfig.MergeProviderConfig(
		cfg.Plugins.API.Blockfrost.Config,
		apiconfig.TLSPolicy{},
		cfg.API.TLS,
	)
	require.NoError(t, err)
	tlsPolicy, err := apiconfig.DecodeTLSPolicy(merged)
	require.NoError(t, err)
	effective, err := tlsPolicy.Resolve("test")
	require.NoError(t, err)

	assert.True(t, effective.Enabled)
	// certFilePath: the provider's own explicit override wins.
	assert.Equal(t, "/blockfrost/cert.pem", effective.CertFilePath)
	// keyFilePath: not set by the provider, inherited from the top-level
	// default -- overriding one nested field must not blow away the
	// other.
	assert.Equal(t, "/shared/key.pem", effective.KeyFilePath)
}

// TestLoad_APIProviderConfigInvalidMergedTLSPair covers the "invalid
// merged certificate/key pair" acceptance criterion: a top-level
// certFilePath with a provider-level keyFilePath removal (impossible to
// express by omission, so this uses an explicit provider override that
// still leaves the pair incomplete) fails Resolve with a path-qualified
// error.
func TestLoad_APIProviderConfigInvalidMergedTLSPair(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())

	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte(
		"api:\n"+
			"  tls:\n"+
			"    mode: server\n"+
			"    certFilePath: /shared/cert.pem\n"+
			"plugins:\n"+
			"  api:\n"+
			"    utxorpc:\n"+
			"      provider: builtin\n"+
			"      config:\n"+
			"        port: 9090\n",
	), 0o600))

	cfg, err := LoadConfig(configFile)
	require.NoError(t, err)

	merged, err := apiconfig.MergeProviderConfig(
		cfg.Plugins.API.Utxorpc.Config,
		apiconfig.TLSPolicy{},
		cfg.API.TLS,
	)
	require.NoError(t, err)
	tlsPolicy, err := apiconfig.DecodeTLSPolicy(merged)
	require.NoError(t, err)
	_, err = tlsPolicy.Resolve("plugins.api.utxorpc.config.tls")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plugins.api.utxorpc.config.tls")
	assert.Contains(t, err.Error(), "must both be set")
}

// TestValidate_InvalidAPITLSMode covers the fail-fast top-level mode
// enum check: a typo in api.tls.mode is rejected once, with a clear
// message, by Validate rather than only surfacing later from each of the
// four API providers that would otherwise inherit it.
func TestValidate_InvalidAPITLSMode(t *testing.T) {
	resetGlobalConfig()
	cfg := GetConfig()
	cfg.ApplyDefaults()
	bogus := "bogus"
	cfg.API.TLS.Mode = &bogus

	err := cfg.Validate(RunModeLoad)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "api.tls.mode")
	assert.Contains(t, err.Error(), "invalid mode")
}

// TestGetConfigSnapshotDoesNotShareAPIPolicy asserts GetConfig's snapshot
// deep-copies api.tls pointer fields, matching the existing
// nested-plugin-config isolation guarantee: mutating one snapshot's
// policy must not be visible through another.
func TestGetConfigSnapshotDoesNotShareAPIPolicy(t *testing.T) {
	resetGlobalConfig()
	mode := "server"
	globalConfig.API.TLS.Mode = &mode

	snapshotA := GetConfig()
	snapshotB := GetConfig()
	require.NotNil(t, snapshotA.API.TLS.Mode)
	require.NotNil(t, snapshotB.API.TLS.Mode)
	require.NotSame(t, snapshotA.API.TLS.Mode, snapshotB.API.TLS.Mode)

	disabled := "disabled"
	snapshotA.API.TLS.Mode = &disabled
	assert.Equal(t, "server", *snapshotB.API.TLS.Mode)
}

// TestForgeEBCapsUnsetTakeTheDefault and its explicit-zero counterpart pin
// the distinction a plain uint64 cannot express: an operator who never
// mentioned the cap gets the backstop, and one who wrote 0 gets the cap
// switched off, as both the flag help and the ForgerConfig contract
// promise.
func TestForgeEBCapsUnsetTakeTheDefault(t *testing.T) {
	c := &Config{}
	c.ApplyDefaults()

	if c.ForgeEBMaxTxRefs == nil {
		t.Fatal("unset forgeEbMaxTxRefs must take the default, got nil")
	}
	if got := *c.ForgeEBMaxTxRefs; got != DefaultForgeEBMaxTxRefs {
		t.Fatalf("forgeEbMaxTxRefs = %d, want %d", got, DefaultForgeEBMaxTxRefs)
	}
	if c.ForgeEBMaxBytes == nil {
		t.Fatal("unset forgeEbMaxBytes must take the default, got nil")
	}
	if got := *c.ForgeEBMaxBytes; got != DefaultForgeEBMaxBytes {
		t.Fatalf("forgeEbMaxBytes = %d, want %d", got, DefaultForgeEBMaxBytes)
	}
}

func TestForgeEBCapsExplicitZeroDisablesThem(t *testing.T) {
	zero := uint64(0)
	c := &Config{ForgeEBMaxTxRefs: &zero, ForgeEBMaxBytes: &zero}
	c.ApplyDefaults()

	if c.ForgeEBMaxTxRefs == nil || *c.ForgeEBMaxTxRefs != 0 {
		t.Fatalf(
			"explicit zero forgeEbMaxTxRefs must survive ApplyDefaults, got %v",
			c.ForgeEBMaxTxRefs,
		)
	}
	if c.ForgeEBMaxBytes == nil || *c.ForgeEBMaxBytes != 0 {
		t.Fatalf(
			"explicit zero forgeEbMaxBytes must survive ApplyDefaults, got %v",
			c.ForgeEBMaxBytes,
		)
	}
}

// TestForgeEBCapDefaultsArePinned guards the copy of these numbers that
// ledger/forging keeps for the embedder path. internal/config cannot
// import ledger/forging (it would cycle), so the values are declared twice
// on purpose; ledger/forging asserts the same literals from its side.
func TestForgeEBCapDefaultsArePinned(t *testing.T) {
	if DefaultForgeEBMaxTxRefs != 20000 {
		t.Fatalf("forgeEbMaxTxRefs default drifted: %d", DefaultForgeEBMaxTxRefs)
	}
	if DefaultForgeEBMaxBytes != 25165824 {
		t.Fatalf("forgeEbMaxBytes default drifted: %d", DefaultForgeEBMaxBytes)
	}
}

// TestForgeEBSelectionReserveUnsetTakesTheDefault covers the third state
// this field can be in. Unlike the caps, a reserve of zero cannot mean
// "disabled": it would leave the ranking block no time at all, so zero can
// only mean the operator never mentioned it.
func TestForgeEBSelectionReserveUnsetTakesTheDefault(t *testing.T) {
	c := &Config{}
	c.ApplyDefaults()

	if c.ForgeEBSelectionReserve != DefaultForgeEBSelectionReserve {
		t.Fatalf(
			"forgeEbSelectionReserve = %s, want %s",
			c.ForgeEBSelectionReserve,
			DefaultForgeEBSelectionReserve,
		)
	}
}

func TestForgeEBSelectionReserveKeepsAConfiguredValue(t *testing.T) {
	c := &Config{ForgeEBSelectionReserve: 750 * time.Millisecond}
	c.ApplyDefaults()

	if c.ForgeEBSelectionReserve != 750*time.Millisecond {
		t.Fatalf(
			"configured forgeEbSelectionReserve must survive ApplyDefaults, got %s",
			c.ForgeEBSelectionReserve,
		)
	}
}

// TestForgeEBSelectionReserveDefaultIsPinned is the internal/config half
// of the same two-sided pin as TestForgeEBCapDefaultsArePinned: the
// forging package keeps its own copy of this number for embedders that
// never touch internal/config.
func TestForgeEBSelectionReserveDefaultIsPinned(t *testing.T) {
	if DefaultForgeEBSelectionReserve != 300*time.Millisecond {
		t.Fatalf(
			"forgeEbSelectionReserve default drifted: %s",
			DefaultForgeEBSelectionReserve,
		)
	}
}

// TestGetConfigSnapshotDoesNotAliasForgeEBCaps: GetConfig hands out a
// snapshot, and callers treat it as their own. Pointer fields make that
// promise easy to break -- writing through a snapshot's cap would change
// the process-wide configuration every later reader sees, and race with
// them while doing it.
func TestGetConfigSnapshotDoesNotAliasForgeEBCaps(t *testing.T) {
	resetGlobalConfig()

	snapshot := GetConfig()
	if snapshot.ForgeEBMaxTxRefs == nil || snapshot.ForgeEBMaxBytes == nil {
		t.Fatal("snapshot must carry the default caps")
	}
	*snapshot.ForgeEBMaxTxRefs = 1
	*snapshot.ForgeEBMaxBytes = 2

	fresh := GetConfig()
	if got := *fresh.ForgeEBMaxTxRefs; got != DefaultForgeEBMaxTxRefs {
		t.Fatalf("global forgeEbMaxTxRefs changed to %d", got)
	}
	if got := *fresh.ForgeEBMaxBytes; got != DefaultForgeEBMaxBytes {
		t.Fatalf("global forgeEbMaxBytes changed to %d", got)
	}
}

// TestForgeEBCapFlagDefaultsShowTheEffectiveValue: --help is where an
// operator learns what omitting a flag does. Registering 0 there said
// "uncapped" while omitting the flag actually applies the backstop, which
// is the opposite of the truth for the one number that bounds a forged
// endorser block.
func TestForgeEBCapFlagDefaultsShowTheEffectiveValue(t *testing.T) {
	resetGlobalConfig()

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)

	for name, want := range map[string]string{
		"forge-eb-max-tx-refs":       "20000",
		"forge-eb-max-bytes":         "25165824",
		"forge-eb-selection-reserve": "300ms",
	} {
		flag := cmd.PersistentFlags().Lookup(name)
		if flag == nil {
			t.Fatalf("flag %q is not registered", name)
		}
		if flag.DefValue != want {
			t.Fatalf("flag %q default = %q, want %q", name, flag.DefValue, want)
		}
	}
}

// TestForgeEBCapFlagsPreserveAnExplicitZero: the effective-default help
// text must not cost the operator the ability to switch a cap off, which
// is what a zero on the command line means.
func TestForgeEBCapFlagsPreserveAnExplicitZero(t *testing.T) {
	resetGlobalConfig()

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.PersistentFlags().Parse([]string{
		"--forge-eb-max-tx-refs=0",
		"--forge-eb-max-bytes=0",
	}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	cfg := &Config{}
	if err := ApplyFlags(cmd, cfg); err != nil {
		t.Fatalf("apply flags: %v", err)
	}
	cfg.ApplyDefaults()

	if cfg.ForgeEBMaxTxRefs == nil || *cfg.ForgeEBMaxTxRefs != 0 {
		t.Fatalf(
			"--forge-eb-max-tx-refs=0 must disable the cap, got %v",
			cfg.ForgeEBMaxTxRefs,
		)
	}
	if cfg.ForgeEBMaxBytes == nil || *cfg.ForgeEBMaxBytes != 0 {
		t.Fatalf(
			"--forge-eb-max-bytes=0 must disable the cap, got %v",
			cfg.ForgeEBMaxBytes,
		)
	}
}

// TestForgeEBSelectionReserveFlagIsApplied closes the CLI half of the
// reserve's path: flag -> Config -> (buildDingoConfig, covered in
// internal/node) -> forger.
func TestForgeEBSelectionReserveFlagIsApplied(t *testing.T) {
	resetGlobalConfig()

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.PersistentFlags().Parse([]string{
		"--forge-eb-selection-reserve=750ms",
	}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	cfg := &Config{}
	if err := ApplyFlags(cmd, cfg); err != nil {
		t.Fatalf("apply flags: %v", err)
	}
	cfg.ApplyDefaults()

	if cfg.ForgeEBSelectionReserve != 750*time.Millisecond {
		t.Fatalf(
			"forgeEbSelectionReserve = %s, want 750ms",
			cfg.ForgeEBSelectionReserve,
		)
	}
}

// TestForgeEBCapsExplicitZeroSurvivesTheLoadPipeline drives an explicit
// zero through each source the node reads, in the order cmd/dingo applies
// them (LoadConfig, ApplyFlags, ApplyDefaults). LoadConfig starts from a
// configuration that already holds the default caps, so the zero has to
// displace a non-zero value on the way in and then survive ApplyDefaults;
// the unit tests above cover each step alone, this one covers the chain.
func TestForgeEBCapsExplicitZeroSurvivesTheLoadPipeline(t *testing.T) {
	for _, tc := range []struct {
		name  string
		yaml  string
		env   map[string]string
		flags []string
	}{
		{
			name: "yaml",
			yaml: "forgeEbMaxTxRefs: 0\nforgeEbMaxBytes: 0\n",
		},
		{
			name: "env",
			env: map[string]string{
				"DINGO_FORGE_EB_MAX_TX_REFS": "0",
				"DINGO_FORGE_EB_MAX_BYTES":   "0",
			},
		},
		{
			name: "cli",
			flags: []string{
				"--forge-eb-max-tx-refs=0",
				"--forge-eb-max-bytes=0",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetGlobalConfig()
			t.Cleanup(resetGlobalConfig)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}

			configFile := filepath.Join(t.TempDir(), "dingo.yaml")
			if err := os.WriteFile(
				configFile,
				[]byte(tc.yaml),
				0o600,
			); err != nil {
				t.Fatalf("write config file: %v", err)
			}

			cmd := &cobra.Command{Use: "dingo"}
			RegisterFlags(cmd)

			cfg, err := LoadConfig(configFile)
			if err != nil {
				t.Fatalf("load config: %v", err)
			}

			if err := cmd.PersistentFlags().Parse(tc.flags); err != nil {
				t.Fatalf("parse flags: %v", err)
			}
			if err := ApplyFlags(cmd, cfg); err != nil {
				t.Fatalf("apply flags: %v", err)
			}
			cfg.ApplyDefaults()

			if cfg.ForgeEBMaxTxRefs == nil || *cfg.ForgeEBMaxTxRefs != 0 {
				t.Fatalf(
					"explicit zero forgeEbMaxTxRefs from %s must disable the cap, got %s",
					tc.name,
					forgeEBCapString(cfg.ForgeEBMaxTxRefs),
				)
			}
			if cfg.ForgeEBMaxBytes == nil || *cfg.ForgeEBMaxBytes != 0 {
				t.Fatalf(
					"explicit zero forgeEbMaxBytes from %s must disable the cap, got %s",
					tc.name,
					forgeEBCapString(cfg.ForgeEBMaxBytes),
				)
			}
		})
	}
}

func forgeEBCapString(v *uint64) string {
	if v == nil {
		return "nil"
	}
	return strconv.FormatUint(*v, 10)
}

// TestHealthPortDefaultsToDefaultHealthPort pins the health listener's
// defaults on newDefaultConfig, which is the only place they come from.
// NewHealthServer skips the listener entirely at HealthPort 0 and
// ApplyDefaults has no fill-in step for that field, so losing the literal
// disables the probe outright -- and resetGlobalConfig's separately
// maintained copy (config_test.go) seeds every other test in this package,
// so none of them would notice. Same blind spot as
// TestValidateForgedBlockDefaultsToTrue (flags_test.go).
func TestHealthPortDefaultsToDefaultHealthPort(t *testing.T) {
	defaults := newDefaultConfig()
	require.Equal(
		t,
		uint(DefaultHealthPort),
		defaults.HealthPort,
		"newDefaultConfig is the only source of the health listener port",
	)
	require.Equal(
		t,
		uint(DefaultHealthReadyGapSlots),
		defaults.HealthReadyGapSlots,
	)

	// ApplyDefaults refills HealthReadyGapSlots but deliberately not
	// HealthPort, so it cannot stand in as a second source.
	zeroed := newDefaultConfig()
	zeroed.HealthPort = 0
	zeroed.HealthReadyGapSlots = 0
	zeroed.ApplyDefaults()
	assert.Zero(t, zeroed.HealthPort)
	assert.Equal(
		t,
		uint(DefaultHealthReadyGapSlots),
		zeroed.HealthReadyGapSlots,
	)
}

func TestDefaultLoggingConfig(t *testing.T) {
	d := DefaultLoggingConfig()
	if d.Format != "text" {
		t.Errorf("default Format = %q, want text", d.Format)
	}
	if d.Level != "info" {
		t.Errorf("default Level = %q, want info", d.Level)
	}
}

func TestLoad_LoggingEnvVars(t *testing.T) {
	resetGlobalConfig()
	// Avoid picking up a real ~/.dingo/dingo.yaml on the dev machine.
	t.Setenv("HOME", t.TempDir())
	t.Setenv("DINGO_LOGGING_FORMAT", "json")
	t.Setenv("DINGO_LOGGING_LEVEL", "warn")

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Logging.Format != "json" {
		t.Errorf("Logging.Format = %q, want json", cfg.Logging.Format)
	}
	if cfg.Logging.Level != "warn" {
		t.Errorf("Logging.Level = %q, want warn", cfg.Logging.Level)
	}
}

func TestLoad_LoggingFromYAML(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	// Ensure a developer's exported DINGO_LOGGING_* env vars cannot override
	// the YAML values under test. t.Setenv records the originals for restore;
	// os.Unsetenv then removes them for the duration of the test.
	t.Setenv("DINGO_LOGGING_FORMAT", "")
	os.Unsetenv("DINGO_LOGGING_FORMAT")
	t.Setenv("DINGO_LOGGING_LEVEL", "")
	os.Unsetenv("DINGO_LOGGING_LEVEL")

	yamlContent := "logging:\n  format: json\n  level: error\n"
	tmpFile := filepath.Join(t.TempDir(), "dingo.yaml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadConfig(tmpFile)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Logging.Format != "json" {
		t.Errorf("Logging.Format = %q, want json", cfg.Logging.Format)
	}
	if cfg.Logging.Level != "error" {
		t.Errorf("Logging.Level = %q, want error", cfg.Logging.Level)
	}
}

// defaultMithrilBackendAtInit captures the production default before
// any test mutates or resets globalConfig.
var defaultMithrilBackendAtInit = globalConfig.Mithril.Backend

func TestMithrilBackendDefault(t *testing.T) {
	assert.Equal(
		t,
		"v2",
		defaultMithrilBackendAtInit,
		"default Mithril backend should be v2",
	)
}

func writeMithrilBackendTestConfig(t *testing.T, yamlContent string) string {
	t.Helper()
	tmpFile := filepath.Join(t.TempDir(), "test-dingo.yaml")
	require.NoError(t, os.WriteFile(tmpFile, []byte(yamlContent), 0o644))
	return tmpFile
}

func TestMithrilBackendYAML(t *testing.T) {
	resetGlobalConfig()
	tmpFile := writeMithrilBackendTestConfig(t, `
network: "preview"
mithril:
  backend: "v1"
`)
	cfg, err := LoadConfig(tmpFile)
	require.NoError(t, err)
	assert.Equal(t, "v1", cfg.Mithril.Backend)
}

func TestMithrilBackendEnvOverride(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("DINGO_MITHRIL_BACKEND", "v1")
	tmpFile := writeMithrilBackendTestConfig(t, `
network: "preview"
mithril:
  backend: "v2"
`)
	cfg, err := LoadConfig(tmpFile)
	require.NoError(t, err)
	assert.Equal(
		t,
		"v1",
		cfg.Mithril.Backend,
		"env var should override YAML",
	)
}

func TestMithrilPinnedDigestYAMLEnvAndCLI(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("DINGO_MITHRIL_PINNED_DIGEST", "env-digest")
	tmpFile := writeMithrilBackendTestConfig(t, `
network: "preview"
mithril:
  pinnedDigest: "yaml-digest"
`)
	cfg, err := LoadConfig(tmpFile)
	require.NoError(t, err)
	assert.Equal(t, "env-digest", cfg.Mithril.PinnedDigest)

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	require.NoError(
		t, cmd.ParseFlags([]string{"--mithril-pinned-digest=cli-digest"}),
	)
	require.NoError(t, ApplyFlags(cmd, cfg))
	assert.Equal(t, "cli-digest", cfg.Mithril.PinnedDigest)
}

// TestMusashiNetworkIdentityConflict pins the rule that decides whether a
// configuration may enable the Musashi prototype's consensus/ledger trust
// bypasses. The prototype network is identified by name ("musashi") or by
// magic (164); a configuration that mixes one of those with a *different*
// predefined network is a conflict and must be rejected, because otherwise a
// node an operator believes is on preview/preprod runs with validation off.
func TestMusashiNetworkIdentityConflict(t *testing.T) {
	tests := []struct {
		name         string
		network      string
		networkMagic uint32
		wantConflict string
	}{
		// Unambiguous prototype identities: allowed.
		{name: "name only", network: "musashi"},
		{
			name:         "name and matching magic",
			network:      "musashi",
			networkMagic: 164,
		},
		{name: "magic only", networkMagic: 164},
		{
			name:         "custom name with prototype magic",
			network:      "musashi-mirror",
			networkMagic: 164,
		},

		// Unambiguous non-prototype identities: allowed (bypasses stay off).
		{name: "preview by name", network: "preview"},
		{
			name:         "preview by name and magic",
			network:      "preview",
			networkMagic: 2,
		},
		{
			name:         "preprod by name and magic",
			network:      "preprod",
			networkMagic: 1,
		},
		{name: "mainnet", network: "mainnet", networkMagic: 764824073},
		{name: "devnet", network: "devnet", networkMagic: 42},
		{
			name:         "custom private net",
			network:      "private-net",
			networkMagic: 9999,
		},
		{name: "unset"},

		// Devnet is excluded from the conflict set, for the same reason
		// FullPotRewardsStandardNetwork excludes it: it is a local test
		// network, not a production-like profile worth refusing to start for.
		// These two cases are the only ones that reach either devnet guard —
		// plain "devnet"/42 carries no Musashi half and returns before them.
		{
			name:         "devnet name with prototype magic",
			network:      "devnet",
			networkMagic: 164,
		},
		{
			name:         "prototype name with devnet magic",
			network:      "musashi",
			networkMagic: 42,
		},

		// Conflicts: a standard network wearing the prototype's magic.
		{
			name:         "preview name with prototype magic",
			network:      "preview",
			networkMagic: 164,
			wantConflict: "preview",
		},
		{
			name:         "preprod name with prototype magic",
			network:      "preprod",
			networkMagic: 164,
			wantConflict: "preprod",
		},
		{
			name:         "mainnet name with prototype magic",
			network:      "mainnet",
			networkMagic: 164,
			wantConflict: "mainnet",
		},

		// Conflicts: the prototype name wearing a standard network's magic.
		// This is the sharper direction — the handshake uses the magic, so
		// the node actually joins preview while trusting prototype rules.
		{
			name:         "prototype name with preview magic",
			network:      "musashi",
			networkMagic: 2,
			wantConflict: "preview",
		},
		{
			name:         "prototype name with preprod magic",
			network:      "musashi",
			networkMagic: 1,
			wantConflict: "preprod",
		},
		{
			name:         "prototype name with mainnet magic",
			network:      "musashi",
			networkMagic: 764824073,
			wantConflict: "mainnet",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := MusashiNetworkIdentityConflict(
				tt.network,
				tt.networkMagic,
			)
			if tt.wantConflict == "" {
				assert.False(t, ok, "unexpected conflict %q", got)
				return
			}
			require.True(t, ok, "expected a conflict")
			assert.Equal(t, tt.wantConflict, got)
		})
	}
}

// TestMusashiPrototypeNetwork pins which identities count as the prototype
// network at all. Preview and preprod must never qualify.
//
// Note the asymmetry between the two kinds of half-match, which is the whole
// point of the rule: a half-match is a misconfiguration only when the *other*
// half names a different predefined network. A custom name on magic 164, or
// the "musashi" name on an unregistered magic, is a private prototype
// deployment rather than a mistake, so it still counts as the prototype
// network — magic 164 *is* Musashi, whatever an operator calls it locally.
func TestMusashiPrototypeNetwork(t *testing.T) {
	tests := []struct {
		name         string
		network      string
		networkMagic uint32
		want         bool
	}{
		{name: "name only", network: "musashi", want: true},
		{
			name:         "name and magic",
			network:      "musashi",
			networkMagic: 164,
			want:         true,
		},
		{name: "magic only", networkMagic: 164, want: true},
		{name: "preview", network: "preview", networkMagic: 2},
		{name: "preprod", network: "preprod", networkMagic: 1},
		{name: "mainnet", network: "mainnet", networkMagic: 764824073},
		{name: "devnet", network: "devnet", networkMagic: 42},
		{name: "unset"},
		// Half-matches against a *custom* identity are private prototype
		// deployments (e.g. a Musashi mirror), not misconfigurations, so they
		// remain the prototype network.
		{
			name:         "custom name with prototype magic",
			network:      "musashi-mirror",
			networkMagic: 164,
			want:         true,
		},
		{
			name:         "prototype name with unregistered magic",
			network:      "musashi",
			networkMagic: 9999,
			want:         true,
		},
		// Consequence of the devnet exclusion: because neither pairing is a
		// conflict, both still resolve to the prototype network and would run
		// with the bypasses. That is acceptable precisely because devnet is a
		// local test network.
		{
			name:         "devnet name with prototype magic",
			network:      "devnet",
			networkMagic: 164,
			want:         true,
		},
		{
			name:         "prototype name with devnet magic",
			network:      "musashi",
			networkMagic: 42,
			want:         true,
		},
		// A conflicting identity is not the prototype network: the bypasses
		// must stay off even if startup validation was never run.
		{
			name:         "preview with prototype magic",
			network:      "preview",
			networkMagic: 164,
		},
		{
			name:         "prototype name with preview magic",
			network:      "musashi",
			networkMagic: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(
				t,
				tt.want,
				MusashiPrototypeNetwork(tt.network, tt.networkMagic),
			)
		})
	}
}

// TestValidateRejectsMusashiIdentityConflict proves the conflict is refused at
// startup configuration validation, not merely defused at the wiring site.
func TestValidateRejectsMusashiIdentityConflict(t *testing.T) {
	tests := []struct {
		name         string
		network      string
		networkMagic uint32
		wantErr      string
	}{
		{
			name:         "preview with prototype magic",
			network:      "preview",
			networkMagic: 164,
			// The message names both configured fields, so the operator can
			// see which of the two they need to change.
			wantErr: `network identity conflict: network "preview" with ` +
				`networkMagic 164 identifies both the "preview" network and ` +
				`the Musashi prototype network`,
		},
		{
			name:         "preprod with prototype magic",
			network:      "preprod",
			networkMagic: 164,
			wantErr: `network identity conflict: network "preprod" with ` +
				`networkMagic 164 identifies both the "preprod" network and ` +
				`the Musashi prototype network`,
		},
		{
			// The reverse direction must not blame "preview", which the
			// operator never configured: it reports musashi/2 as supplied.
			name:         "prototype name with preview magic",
			network:      "musashi",
			networkMagic: 2,
			wantErr: `network identity conflict: network "musashi" with ` +
				`networkMagic 2 identifies both the "preview" network and ` +
				`the Musashi prototype network`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.Network = tt.network
			cfg.NetworkMagic = tt.networkMagic
			err := cfg.Validate(RunModeServe)
			require.Error(t, err)
			assert.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestValidateAllowsUnambiguousNetworks guards against the new rule rejecting
// legitimate configurations, including Musashi itself.
func TestValidateAllowsUnambiguousNetworks(t *testing.T) {
	for _, tt := range []struct {
		name         string
		network      string
		networkMagic uint32
	}{
		{name: "musashi by name", network: "musashi"},
		{name: "musashi by name and magic", network: "musashi", networkMagic: 164},
		{name: "musashi by magic only", network: "", networkMagic: 164},
		{name: "preview", network: "preview", networkMagic: 2},
		{name: "preprod", network: "preprod", networkMagic: 1},
		{name: "devnet", network: "devnet", networkMagic: 42},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validTestConfig()
			cfg.Network = tt.network
			cfg.NetworkMagic = tt.networkMagic
			require.NoError(t, cfg.Validate(RunModeServe))
		})
	}
}

func TestValidateTracingSampleRatio(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		ratio   float64
		wantErr bool
	}{
		{name: "zero", ratio: 0},
		{name: "fraction", ratio: 0.25},
		{name: "one", ratio: 1},
		{name: "negative", ratio: -0.1, wantErr: true},
		{name: "above one", ratio: 1.5, wantErr: true},
		{name: "nan", ratio: math.NaN(), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := newDefaultConfig()
			cfg.TracingSampleRatio = tc.ratio
			err := cfg.Validate(RunModeServe)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "tracingSampleRatio")
				return
			}
			if err != nil {
				assert.NotContains(t, err.Error(), "tracingSampleRatio")
			}
		})
	}
}
