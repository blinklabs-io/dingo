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
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

func TestRegisterFlags_CoversAllExportedConfigFields(t *testing.T) {
	resetGlobalConfig()

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)

	specFields := map[string]string{}
	yamlOnlyFields := map[string]struct{}{
		"Midnight.CNightPolicyID":              {},
		"Midnight.CNightAssetName":             {},
		"Midnight.MappingValidatorAddress":     {},
		"Midnight.AuthTokenPolicyID":           {},
		"Midnight.AuthTokenAssetName":          {},
		"Midnight.CommitteeCandidateAddress":   {},
		"Midnight.TechnicalCommitteeAddress":   {},
		"Midnight.TechnicalCommitteePolicyID":  {},
		"Midnight.CouncilAddress":              {},
		"Midnight.CouncilPolicyID":             {},
		"Midnight.PermissionedCandidatePolicy": {},
	}
	for _, spec := range flagSpecs {
		if prev, ok := specFields[spec.field]; ok {
			t.Fatalf(
				"duplicate flag spec for field %q: %q and %q",
				spec.field, prev, spec.name,
			)
		}
		specFields[spec.field] = spec.name
		if cmd.PersistentFlags().Lookup(spec.name) == nil {
			t.Fatalf(
				"flag %q for field %q is not registered",
				spec.name, spec.field,
			)
		}
	}

	leafFields := map[string]struct{}{}
	collectExportedLeafFields(reflect.TypeFor[Config](), "", leafFields)

	for fieldPath := range leafFields {
		if _, ok := yamlOnlyFields[fieldPath]; ok {
			continue
		}
		if _, ok := specFields[fieldPath]; !ok {
			t.Fatalf("config field %q has no flag spec", fieldPath)
		}
	}
	for fieldPath := range specFields {
		if _, ok := leafFields[fieldPath]; !ok {
			t.Fatalf(
				"flag spec field %q does not exist on Config",
				fieldPath,
			)
		}
	}
}

func TestApplyFlags_PriorityOrderFlagsOverrideEnv(t *testing.T) {
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("CARDANO_MEMPOOL_CAPACITY", "123456")
	t.Setenv("DINGO_DATABASE_WORKERS", "9")
	t.Setenv("DINGO_BACKFILL_BATCH_SIZE", "50")
	t.Setenv("DINGO_HISTORY_EXPIRY_FREQUENCY", "15m")
	t.Setenv("DINGO_OFFCHAIN_METADATA_MAX_BYTES", "1024")
	t.Setenv(
		"DINGO_OFFCHAIN_METADATA_IPFS_GATEWAY_URL",
		"https://env.example/ipfs/",
	)
	t.Setenv("DINGO_MIDNIGHT_PORT", "50070")
	t.Setenv("DINGO_MIDNIGHT_HOST", "127.0.0.3")

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "dingo.yaml")
	if err := os.WriteFile(configFile, []byte(""), 0o600); err != nil {
		t.Fatalf("failed to write temp config file: %v", err)
	}

	cfg, err := LoadConfig(configFile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}
	if cfg.MempoolCapacity != 123456 {
		t.Fatalf(
			"expected env var to set mempoolCapacity=123456, got %d",
			cfg.MempoolCapacity,
		)
	}
	if cfg.DatabaseWorkers != 9 {
		t.Fatalf(
			"expected env var to set databaseWorkers=9, got %d",
			cfg.DatabaseWorkers,
		)
	}
	if cfg.BackfillBatchSize != 50 {
		t.Fatalf(
			"expected env var to set backfillBatchSize=50, got %d",
			cfg.BackfillBatchSize,
		)
	}
	if cfg.HistoryExpiry.Frequency.String() != "15m0s" {
		t.Fatalf(
			"expected env var to set history expiry frequency=15m, got %s",
			cfg.HistoryExpiry.Frequency,
		)
	}
	if cfg.OffchainMetadata.MaxBytes != 1024 {
		t.Fatalf(
			"expected env var to set offchain metadata max bytes=1024, got %d",
			cfg.OffchainMetadata.MaxBytes,
		)
	}
	if cfg.OffchainMetadata.IPFSGatewayURL != "https://env.example/ipfs/" {
		t.Fatalf(
			"expected env var to set offchain metadata IPFS gateway, got %q",
			cfg.OffchainMetadata.IPFSGatewayURL,
		)
	}
	if cfg.Midnight.Port != 50070 {
		t.Fatalf(
			"expected env var to set midnight port=50070, got %d",
			cfg.Midnight.Port,
		)
	}
	if cfg.Midnight.Host != "127.0.0.3" {
		t.Fatalf(
			"expected env var to set midnight host, got %q",
			cfg.Midnight.Host,
		)
	}

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.ParseFlags([]string{
		"--mempool-capacity=7890",
		"--data-dir=/tmp/override",
		"--backfill-batch-size=200",
		"--history-expiry-enabled=true",
		"--history-expiry-frequency=30m",
		"--offchain-metadata-max-bytes=2048",
		"--offchain-metadata-ipfs-gateway-url=https://flag.example/ipfs/",
		"--midnight-port=50080",
		"--midnight-host=127.0.0.4",
	}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	if err := ApplyFlags(cmd, cfg); err != nil {
		t.Fatalf("failed to apply flags: %v", err)
	}

	if cfg.MempoolCapacity != 7890 {
		t.Fatalf(
			"expected flag to override env mempoolCapacity to 7890, got %d",
			cfg.MempoolCapacity,
		)
	}
	if cfg.DatabaseWorkers != 9 {
		t.Fatalf(
			"expected unchanged databaseWorkers to stay env value 9, got %d",
			cfg.DatabaseWorkers,
		)
	}
	if cfg.DatabasePath != "/tmp/override" {
		t.Fatalf(
			"expected --data-dir to set databasePath, got %q",
			cfg.DatabasePath,
		)
	}
	if cfg.BackfillBatchSize != 200 {
		t.Fatalf(
			"expected --backfill-batch-size to set backfillBatchSize=200, got %d",
			cfg.BackfillBatchSize,
		)
	}
	if !cfg.HistoryExpiry.Enabled {
		t.Fatal("expected --history-expiry-enabled to enable history expiry")
	}
	if cfg.HistoryExpiry.Frequency.String() != "30m0s" {
		t.Fatalf(
			"expected --history-expiry-frequency to set 30m, got %s",
			cfg.HistoryExpiry.Frequency,
		)
	}
	if cfg.OffchainMetadata.MaxBytes != 2048 {
		t.Fatalf(
			"expected flag to override offchain metadata max bytes to 2048, got %d",
			cfg.OffchainMetadata.MaxBytes,
		)
	}
	if cfg.OffchainMetadata.IPFSGatewayURL != "https://flag.example/ipfs/" {
		t.Fatalf(
			"expected flag to override offchain metadata IPFS gateway, got %q",
			cfg.OffchainMetadata.IPFSGatewayURL,
		)
	}
	if cfg.Midnight.Port != 50080 {
		t.Fatalf(
			"expected flag to override midnight port to 50080, got %d",
			cfg.Midnight.Port,
		)
	}
	if cfg.Midnight.Host != "127.0.0.4" {
		t.Fatalf(
			"expected flag to override midnight host, got %q",
			cfg.Midnight.Host,
		)
	}
}

func TestRegisterFlags_MidnightAddressAndPolicyFieldsAreYAMLOnly(t *testing.T) {
	resetGlobalConfig()

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)

	yamlOnlyFlags := []string{
		"midnight-cnight-policy-id",
		"midnight-cnight-asset-name",
		"midnight-mapping-validator-address",
		"midnight-auth-token-policy-id",
		"midnight-auth-token-asset-name",
		"midnight-committee-candidate-address",
		"midnight-technical-committee-address",
		"midnight-technical-committee-policy-id",
		"midnight-council-address",
		"midnight-council-policy-id",
		"midnight-permissioned-candidate-policy",
	}
	for _, flag := range yamlOnlyFlags {
		if cmd.PersistentFlags().Lookup(flag) != nil {
			t.Fatalf("expected --%s to be YAML/env only", flag)
		}
	}
}

func TestApplyFlags_NetworkOverrideReappliesMidnightDefaults(t *testing.T) {
	resetGlobalConfig()

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}
	if cfg.Network != "preview" {
		t.Fatalf("expected initial network preview, got %q", cfg.Network)
	}
	if cfg.Midnight.CNightPolicyID != midnightNetworkDefaults["preview"].CNightPolicyID {
		t.Fatalf(
			"expected preview Midnight default before flags, got %q",
			cfg.Midnight.CNightPolicyID,
		)
	}

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.ParseFlags([]string{"--network=mainnet"}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	if err := ApplyFlags(cmd, cfg); err != nil {
		t.Fatalf("failed to apply flags: %v", err)
	}

	if cfg.Network != "mainnet" {
		t.Fatalf("expected network mainnet, got %q", cfg.Network)
	}
	if cfg.Midnight.CNightPolicyID != midnightNetworkDefaults["mainnet"].CNightPolicyID {
		t.Fatalf(
			"expected mainnet Midnight policy default, got %q",
			cfg.Midnight.CNightPolicyID,
		)
	}
	if cfg.Midnight.CouncilPolicyID != midnightNetworkDefaults["mainnet"].CouncilPolicyID {
		t.Fatalf(
			"expected mainnet Midnight council policy default, got %q",
			cfg.Midnight.CouncilPolicyID,
		)
	}
}

func TestApplyFlags_NetworkOverridePreservesExplicitMidnightYAML(t *testing.T) {
	resetGlobalConfig()
	previewPolicy := midnightNetworkDefaults["preview"].CNightPolicyID
	yamlContent := `
network: "preview"
midnight:
  cnightPolicyId: "` + previewPolicy + `"
`
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "dingo.yaml")
	if err := os.WriteFile(configFile, []byte(yamlContent), 0o600); err != nil {
		t.Fatalf("failed to write temp config file: %v", err)
	}
	cfg, err := LoadConfig(configFile)
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.ParseFlags([]string{"--network=mainnet"}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	if err := ApplyFlags(cmd, cfg); err != nil {
		t.Fatalf("failed to apply flags: %v", err)
	}

	if cfg.Midnight.CNightPolicyID != previewPolicy {
		t.Fatalf(
			"expected explicit Midnight YAML policy to be preserved, got %q",
			cfg.Midnight.CNightPolicyID,
		)
	}
	if cfg.Midnight.CouncilPolicyID != midnightNetworkDefaults["mainnet"].CouncilPolicyID {
		t.Fatalf(
			"expected remaining Midnight defaults to switch to mainnet, got %q",
			cfg.Midnight.CouncilPolicyID,
		)
	}
}

func TestApplyFlags_NetworkMagicRejectsOverflow(t *testing.T) {
	resetGlobalConfig()

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	overflow := uint64(math.MaxUint32) + 1
	if err := cmd.ParseFlags([]string{
		"--network-magic=" + strconv.FormatUint(overflow, 10),
	}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	cfg := &Config{}
	err := ApplyFlags(cmd, cfg)
	if err == nil {
		t.Fatalf("expected overflow error, got nil")
	}
}

// TestTopologyResolvesFromNetworkFlag pins topology resolution to the
// final merged configuration: LoadTopologyConfig runs only after
// ApplyFlags (see cmd/dingo), so a --network override determines which
// network's topology is loaded.
func TestTopologyResolvesFromNetworkFlag(t *testing.T) {
	resetGlobalConfig()

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}
	if cfg.Network != "preview" {
		t.Fatalf("expected default network preview, got %q", cfg.Network)
	}

	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.ParseFlags([]string{"--network=preprod"}); err != nil {
		t.Fatalf("failed to parse flags: %v", err)
	}

	if err := ApplyFlags(cmd, cfg); err != nil {
		t.Fatalf("failed to apply flags: %v", err)
	}

	if cfg.Network != "preprod" {
		t.Fatalf("expected network preprod, got %q", cfg.Network)
	}
	if _, err := LoadTopologyConfig(); err != nil {
		t.Fatalf("failed to load topology: %v", err)
	}
	topologyConfig := GetTopologyConfig()
	if topologyConfig.PeerSnapshot == nil {
		t.Fatal("expected topology load to use the preprod peer snapshot")
	}
	if topologyConfig.PeerSnapshot.NetworkMagic != 1 {
		t.Fatalf(
			"expected preprod peer snapshot network magic 1, got %d",
			topologyConfig.PeerSnapshot.NetworkMagic,
		)
	}
}

// loadConfigThroughPipeline runs the full cmd/dingo configuration
// pipeline — LoadConfig, ApplyFlags, ApplyDefaults, validate, and
// topology resolution — on the given YAML and CLI arguments and returns
// the merged config alongside the validation/topology result, mirroring
// PersistentPreRunE in cmd/dingo.
func loadConfigThroughPipeline(
	t *testing.T,
	yaml string,
	args []string,
) (*Config, error) {
	t.Helper()
	resetGlobalConfig()
	t.Setenv("HOME", t.TempDir())
	configFile := filepath.Join(t.TempDir(), "dingo.yaml")
	if err := os.WriteFile(configFile, []byte(yaml), 0o600); err != nil {
		t.Fatalf("failed to write temp config file: %v", err)
	}
	cfg, err := LoadConfig(configFile)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cmd := &cobra.Command{Use: "dingo"}
	RegisterFlags(cmd)
	if err := cmd.ParseFlags(args); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	if err := ApplyFlags(cmd, cfg); err != nil {
		t.Fatalf("ApplyFlags: %v", err)
	}
	cfg.ApplyDefaults()
	if err := cfg.validate(cfg.RunMode, minUnprivilegedPort); err != nil {
		return cfg, err
	}
	if _, err := LoadTopologyConfig(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// TestPipeline_FlagOverridesInvalidYAMLRunMode is a precedence
// regression test: a higher-precedence CLI flag must be able to replace
// an invalid YAML runMode, so LoadConfig cannot reject the value before
// flags are merged.
func TestPipeline_FlagOverridesInvalidYAMLRunMode(t *testing.T) {
	cfg, err := loadConfigThroughPipeline(
		t,
		"runMode: \"bogus\"\n",
		[]string{"--run-mode=serve"},
	)
	if err != nil {
		t.Fatalf("expected valid config after flag override, got: %v", err)
	}
	if cfg.RunMode != RunModeServe {
		t.Errorf("RunMode = %q, want %q", cfg.RunMode, RunModeServe)
	}
}

// TestPipeline_InvalidYAMLRunModeRejectedWithoutOverride pins the other
// side of the precedence fix: with no flag override, the invalid YAML
// value must still be rejected — by Validate on the merged config, not
// by LoadConfig.
func TestPipeline_InvalidYAMLRunModeRejectedWithoutOverride(t *testing.T) {
	_, err := loadConfigThroughPipeline(t, "runMode: \"bogus\"\n", nil)
	if err == nil || !strings.Contains(err.Error(), "invalid runMode") {
		t.Fatalf("expected invalid runMode error, got: %v", err)
	}
}

// TestPipeline_MempoolCapacityDefaultsFromFlagRunMode is a defaulting
// regression test: the run-mode-derived MempoolCapacity default must be
// chosen from the final merged mode, including one set only by a CLI
// flag.
func TestPipeline_MempoolCapacityDefaultsFromFlagRunMode(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		expected int64
	}{
		{
			name:     "leios via flag",
			args:     []string{"--run-mode=leios"},
			expected: DefaultMempoolCapacityLeios,
		},
		{
			name:     "serve via flag",
			args:     []string{"--run-mode=serve"},
			expected: DefaultMempoolCapacityPraos,
		},
		{
			name:     "no mode configured anywhere",
			args:     nil,
			expected: DefaultMempoolCapacityPraos,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := loadConfigThroughPipeline(t, "", tc.args)
			if err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
			if cfg.MempoolCapacity != tc.expected {
				t.Errorf(
					"MempoolCapacity = %d, want %d",
					cfg.MempoolCapacity, tc.expected,
				)
			}
		})
	}
}

// TestPipeline_NetworkFlagRepairsInvalidYAMLNetwork is a precedence
// regression test for topology ordering: a traversal-shaped YAML
// network must not abort config loading or topology resolution before
// a --network flag replaces it. Without the override the same value
// must still be rejected — by Validate, not LoadConfig.
func TestPipeline_NetworkFlagRepairsInvalidYAMLNetwork(t *testing.T) {
	yaml := "network: \"../bad\"\n"

	cfg, err := loadConfigThroughPipeline(
		t, yaml, []string{"--network=preview"},
	)
	if err != nil {
		t.Fatalf("expected valid config after flag override, got: %v", err)
	}
	if cfg.Network != "preview" {
		t.Errorf("Network = %q, want %q", cfg.Network, "preview")
	}

	_, err = loadConfigThroughPipeline(t, yaml, nil)
	if err == nil || !strings.Contains(err.Error(), "invalid network name") {
		t.Fatalf("expected invalid network name error, got: %v", err)
	}
}

// TestPipeline_TopologyFlagRepairsMissingYAMLTopology pins the same
// ordering for the topology file itself: a missing YAML topology path
// must not abort before a --topology flag replaces it, and must still
// fail topology resolution without the override.
func TestPipeline_TopologyFlagRepairsMissingYAMLTopology(t *testing.T) {
	yaml := "topology: \"/nonexistent/topology.json\"\n"
	validTopology := filepath.Join(t.TempDir(), "topology.json")
	if err := os.WriteFile(
		validTopology,
		[]byte(`{"localRoots": [], "publicRoots": []}`),
		0o600,
	); err != nil {
		t.Fatalf("failed to write topology file: %v", err)
	}

	cfg, err := loadConfigThroughPipeline(
		t, yaml, []string{"--topology=" + validTopology},
	)
	if err != nil {
		t.Fatalf("expected valid config after flag override, got: %v", err)
	}
	if cfg.Topology != validTopology {
		t.Errorf("Topology = %q, want %q", cfg.Topology, validTopology)
	}

	_, err = loadConfigThroughPipeline(t, yaml, nil)
	if err == nil || !strings.Contains(err.Error(), "topology") {
		t.Fatalf("expected topology load error, got: %v", err)
	}
}

// TestPipeline_NegativeHistoryExpiryFrequencyRejected pins the
// explicit-negative-frequency contract for every configuration source:
// ApplyDefaults only fills in an unset (zero) historyExpiry.frequency,
// so a configured negative value must survive defaulting and fail
// validation instead of silently starting the expiry worker on the
// one-hour default cadence.
func TestPipeline_NegativeHistoryExpiryFrequencyRejected(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		env  map[string]string
		args []string
	}{
		{
			name: "yaml",
			yaml: "historyExpiry:\n  enabled: true\n  frequency: -1s\n",
		},
		{
			name: "environment",
			env: map[string]string{
				"DINGO_HISTORY_EXPIRY_ENABLED":   "true",
				"DINGO_HISTORY_EXPIRY_FREQUENCY": "-1s",
			},
		},
		{
			name: "flag",
			args: []string{
				"--history-expiry-enabled=true",
				"--history-expiry-frequency=-1s",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			cfg, err := loadConfigThroughPipeline(t, tc.yaml, tc.args)
			if err == nil ||
				!strings.Contains(err.Error(), "invalid historyExpiry.frequency") {
				t.Fatalf(
					"expected invalid historyExpiry.frequency error, got: %v",
					err,
				)
			}
			if cfg.HistoryExpiry.Frequency != -time.Second {
				t.Errorf(
					"Frequency = %s, want -1s preserved through defaulting",
					cfg.HistoryExpiry.Frequency,
				)
			}
		})
	}
}

// TestPipeline_AggregatesErrorsAcrossSettings pins error aggregation on
// the merged config: LoadConfig no longer fails on the first bad value,
// so every problem must surface together in the single Validate pass.
func TestPipeline_AggregatesErrorsAcrossSettings(t *testing.T) {
	yaml := "runMode: \"bogus\"\n" +
		"evictionWatermark: 2.0\n" +
		"blockProducer: true\n" +
		"chainsync:\n  strategy: \"fastest\"\n"
	_, err := loadConfigThroughPipeline(t, yaml, nil)
	if err == nil {
		t.Fatal("expected validation errors, got nil")
	}
	for _, want := range []string{
		"invalid runMode",
		"invalid evictionWatermark",
		"blockProducer enabled but missing required key paths",
		"invalid chainsync.strategy",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error should contain %q, got: %v", want, err)
		}
	}
}

func collectExportedLeafFields(
	t reflect.Type,
	prefix string,
	out map[string]struct{},
) {
	for field := range t.Fields() {
		if !field.IsExported() {
			continue
		}
		path := field.Name
		if prefix != "" {
			path = prefix + "." + field.Name
		}
		if field.Type.Kind() == reflect.Struct {
			collectExportedLeafFields(field.Type, path, out)
			continue
		}
		out[path] = struct{}{}
	}
}
