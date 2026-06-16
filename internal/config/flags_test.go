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
	"testing"

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
	yamlContent := `
network: "preview"
midnight:
  cnightPolicyId: "yaml-policy"
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

	if cfg.Midnight.CNightPolicyID != "yaml-policy" {
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

func TestApplyFlags_ReloadsTopologyForNetworkFlag(t *testing.T) {
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
	topologyConfig := GetTopologyConfig()
	if topologyConfig.PeerSnapshot == nil {
		t.Fatal("expected topology reload to load preprod peer snapshot")
	}
	if topologyConfig.PeerSnapshot.NetworkMagic != 1 {
		t.Fatalf(
			"expected preprod peer snapshot network magic 1, got %d",
			topologyConfig.PeerSnapshot.NetworkMagic,
		)
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
