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

package settingsresolve_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database/dbinfo"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/internal/config"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/settingsresolve"
	"github.com/blinklabs-io/dingo/plugin"
	"github.com/stretchr/testify/require"
)

// isolateConfigSnapshot snapshots the process-wide configuration state
// (config.GetConfig()) on entry and republishes that exact snapshot on
// cleanup, so no test in this file can observe another test's
// settingsresolve.Apply publish regardless of run order.
//
// settingsresolve.Apply publishes its result via config.PublishConfig, the
// same process-wide snapshot LoadConfig and ApplyFlags publish to in a
// real process -- so within a single test binary that snapshot persists
// across tests in this file exactly the way it persists across commands
// in a real process. Before Apply published anything this was not a
// hazard; now it is a real one, reproduced independently: with this
// isolation removed, TestApplyAllowsExplicitPrune successfully publishing
// an explicit SourceFlag provenance for StorageMode, immediately followed
// by TestApplyResumesAPIStorageMode (which never sets StorageMode's
// provenance itself, since it means to test the default case), makes that
// second test's cfg := config.GetConfig() inherit the stale explicit
// provenance and silently stop guarding against pruning an api database to
// core -- exactly the destructive failure mode this feature exists to
// prevent, now happening only under an unlucky test order. Per-test
// SetProvenanceForTest(..., SourceDefault) resets (kept below on the tests
// that already had them, for documentation and belt-and-braces) fix this
// for the tests that have one, but are easy to forget on the next test
// someone adds; this helper makes the leakage structurally impossible
// instead by resetting the entire snapshot, not just the one field a given
// test happens to care about. Call it at the top of every test in this
// file that calls Apply or reads config.GetConfig() directly.
func isolateConfigSnapshot(t *testing.T) {
	t.Helper()
	saved := config.GetConfig()
	t.Cleanup(func() {
		config.PublishConfig(saved)
	})
}

// seedDatabase opens a real sqlite-backed metadata store in a fresh
// directory, persists gates directly into node_settings_gate, and closes
// it again -- reproducing exactly the state a real prior `dingo` start
// would have left behind, without going through settingsresolve.Apply
// itself (which is what these tests exercise).
func seedDatabase(t *testing.T, gates map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	ctx := context.Background()

	host, err := internalplugins.NewHost()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = host.Stop(ctx)
	})

	store, err := plugin.Resolve[metadata.MetadataStore](
		ctx,
		host,
		plugin.CapabilityStorageMetadata,
		"sqlite",
		nil,
		metadata.ProviderDependencies{
			DataDir:        dir,
			StorageMode:    "core",
			MaxConnections: 1,
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = store.Close()
	})

	require.NoError(t, store.SetNodeSettingsGates(
		nodesettings.Values(gates), 0, 0,
	))
	require.NoError(t, dbinfo.Write(dir, dbinfo.Info{
		FormatVersion:  dbinfo.CurrentFormatVersion,
		MetadataPlugin: "sqlite",
	}))
	return dir
}

func TestApplyResumesNetworkFromDefault(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := seedDatabase(t, map[string]string{"network": "preprod"})
	cfg := config.GetConfig()
	cfg.DatabasePath = dir
	cfg.Network = "preview" // the built-in default
	require.NoError(t, settingsresolve.Apply(cfg))
	require.Equal(t, "preprod", cfg.Network)
}

func TestApplyRejectsExplicitConflict(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := seedDatabase(t, map[string]string{"network": "preprod"})
	cfg := config.GetConfig()
	cfg.DatabasePath = dir
	cfg.Network = "preview"
	cfg.SetProvenanceForTest("Network", config.SourceFlag)
	err := settingsresolve.Apply(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "network")
	require.Contains(t, err.Error(), "preprod")
}

func TestApplyResumesAPIStorageMode(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := seedDatabase(t, map[string]string{"storage_mode": "api"})
	cfg := config.GetConfig()
	cfg.DatabasePath = dir
	cfg.StorageMode = "core" // the built-in default
	require.NoError(t, settingsresolve.Apply(cfg))
	require.Equal(
		t, "api", cfg.StorageMode,
		"a bare start must not silently prune an api database to core",
	)
}

func TestApplyAllowsExplicitPrune(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := seedDatabase(t, map[string]string{"storage_mode": "api"})
	cfg := config.GetConfig()
	cfg.DatabasePath = dir
	cfg.StorageMode = "core"
	cfg.SetProvenanceForTest("StorageMode", config.SourceFlag)
	require.NoError(t, settingsresolve.Apply(cfg))
	require.Equal(t, "core", cfg.StorageMode)
}

func TestApplyOnMissingDatabaseIsNoOp(t *testing.T) {
	isolateConfigSnapshot(t)
	cfg := config.GetConfig()
	cfg.DatabasePath = filepath.Join(t.TempDir(), "does-not-exist")
	cfg.Network = "preview"
	require.NoError(t, settingsresolve.Apply(cfg))
	require.Equal(t, "preview", cfg.Network)
}

// TestApplyOnEmptyDatabaseDirIsNoOp pins the fix for a real regression found
// running the real binary: `dingo database restore` into an existing-but-
// empty target directory (a pre-created container/k8s volume mount, or the
// `rm -rf $DATADIR/*` recovery workflow) failed with "target data directory
// is not empty", because Apply gated only on os.Stat succeeding and
// readPersistedGateValues resolving a metadata provider runs its migration
// registry as a side effect of merely starting it -- creating a database in
// the directory lifecycle.RestoreValidated's requireEmptyOrAbsent check then
// rejected. This asserts both halves of the fix: Apply must still succeed
// as a no-op, AND the directory must still be empty afterward -- the bug was
// entirely in the side effect, not the return value, so a test that only
// checked for a nil error would not have caught it.
func TestApplyOnEmptyDatabaseDirIsNoOp(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := t.TempDir() // exists (like a pre-mounted volume), but empty
	cfg := config.GetConfig()
	cfg.DatabasePath = dir
	cfg.Network = "preview"
	require.NoError(t, settingsresolve.Apply(cfg))
	require.Equal(t, "preview", cfg.Network)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(
		t,
		entries,
		"Apply must not create anything in an existing-but-empty data directory",
	)
}

func TestApplyRejectsMetadataPluginMismatch(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := t.TempDir()
	require.NoError(t, dbinfo.Write(dir, dbinfo.Info{
		FormatVersion: 1, MetadataPlugin: "postgres",
	}))
	cfg := config.GetConfig()
	cfg.DatabasePath = dir
	cfg.Plugins.Storage.Metadata.Provider = "sqlite"
	err := settingsresolve.Apply(cfg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "postgres")
}

// TestApplyFailsOnIncompleteSidecar pins the P1 half of the dbinfo fix that
// actually matters: dbinfo.Read alone returning ErrIncompleteSidecar for a
// sidecar missing its plugin name changes nothing on its own, because
// checkMetadataPluginSidecar previously treated every Read error --
// including this one -- as advisory (Warn and proceed). A bare "dingo" run
// against a postgres-provisioned directory whose sidecar never got its
// plugin name written would then fall through here, resolve the configured
// sqlite provider, and run its migrations -- creating a fresh, empty
// database beside the real one, exactly the outcome the sidecar exists to
// prevent. This asserts Apply now fails closed instead.
func TestApplyFailsOnIncompleteSidecar(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := t.TempDir()
	require.NoError(t, dbinfo.Write(dir, dbinfo.Info{
		FormatVersion:  dbinfo.CurrentFormatVersion,
		MetadataPlugin: "",
	}))
	cfg := config.GetConfig()
	cfg.DatabasePath = dir
	cfg.Plugins.Storage.Metadata.Provider = "sqlite"
	err := settingsresolve.Apply(cfg)
	require.Error(t, err)
}

// TestApplyProceedsOnUnreadableSidecar pins the other half of the same
// fix, in the opposite direction: a sidecar this build cannot read for a
// reason unrelated to ErrIncompleteSidecar -- here, an unrecognised newer
// FormatVersion -- must keep today's deliberate forward-compatibility
// behavior of warning and proceeding, not fail startup. Only the
// incomplete-sidecar case changes.
func TestApplyProceedsOnUnreadableSidecar(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := seedDatabase(t, map[string]string{"network": "preprod"})
	require.NoError(t, dbinfo.Write(dir, dbinfo.Info{
		FormatVersion:  dbinfo.CurrentFormatVersion + 1,
		MetadataPlugin: "sqlite",
	}))
	cfg := config.GetConfig()
	cfg.DatabasePath = dir
	cfg.Network = "preview"
	require.NoError(t, settingsresolve.Apply(cfg))
	require.Equal(t, "preprod", cfg.Network)
}

// TestApplyResumesNetworkFromDefaultPublishesToGetConfig pins a real bug
// found by running the end-to-end scenario against the real binary: Apply
// overriding cfg.Network was not enough on its own. cmd/dingo's
// LoadTopologyConfig (and anything else that calls config.GetConfig)
// reads the process-wide configuration snapshot, not the *Config pointer
// threaded through main.go's PersistentPreRunE -- LoadConfig and
// ApplyFlags already publish their own results to that snapshot, but
// settingsresolve.Apply did not, so a bare resume left topology resolving
// against the stale, pre-override network while every other consumer of
// cfg saw the resumed one. Concretely: the node dialed the *previous*
// default network's relays while handshaking with the *resumed* network's
// magic, and could not sync with anything. This asserts the fix directly:
// config.GetConfig() must reflect the override, not just the cfg pointer
// Apply was given.
//
// SetProvenanceForTest(..., SourceDefault) isolates this test from
// whatever a previous test in this file may have already published to the
// shared process-wide snapshot via config.GetConfig()/PublishConfig --
// TestApplyAllowsExplicitPrune above, for one, publishes an explicit
// StorageMode provenance, and Network/StorageMode fields visible through
// config.GetConfig() persist across tests in this package the same way
// they persist across commands in a real process. This test's own
// assertion must hold regardless of what ran before it, not by relying on
// file declaration order.
func TestApplyResumesNetworkFromDefaultPublishesToGetConfig(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := seedDatabase(t, map[string]string{"network": "preprod"})
	cfg := config.GetConfig()
	cfg.DatabasePath = dir
	cfg.Network = "preview" // the built-in default
	cfg.SetProvenanceForTest("Network", config.SourceDefault)

	require.NoError(t, settingsresolve.Apply(cfg))
	require.Equal(t, "preprod", cfg.Network)
	require.Equal(
		t, "preprod", config.GetConfig().Network,
		"Apply must publish its override, not just set it on the cfg pointer",
	)
}

// TestApplyResumesAPIStorageModePublishesToGetConfig is the storage-mode
// equivalent of TestApplyResumesNetworkFromDefaultPublishesToGetConfig
// above: same bug class, different gate.
func TestApplyResumesAPIStorageModePublishesToGetConfig(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := seedDatabase(t, map[string]string{"storage_mode": "api"})
	cfg := config.GetConfig()
	cfg.DatabasePath = dir
	cfg.StorageMode = "core" // the built-in default
	cfg.SetProvenanceForTest("StorageMode", config.SourceDefault)

	require.NoError(t, settingsresolve.Apply(cfg))
	require.Equal(t, "api", cfg.StorageMode)
	require.Equal(
		t, "api", config.GetConfig().StorageMode,
		"Apply must publish its override, not just set it on the cfg pointer",
	)
}

// TestApplyReDerivesMidnightDefaultsOnResumedNetwork pins the fix for the
// bug this task exists to close: settingsresolve.Apply resuming cfg.Network
// from a persisted gate did not re-derive the network-keyed Midnight
// constants (internal/config's midnightNetworkDefaults), so an operator
// with Midnight enabled who leaves Network at its built-in default kept the
// *previous default network's* constants after a bare resume, even though
// cfg.Network itself correctly became the resumed network. config.LoadConfig
// reproduces exactly what a bare `dingo` invocation has already done by the
// time Apply runs: Network at its built-in default (preview) with preview's
// Midnight constants already filled in.
func TestApplyReDerivesMidnightDefaultsOnResumedNetwork(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := seedDatabase(t, map[string]string{"network": "mainnet"})

	cfg, err := config.LoadConfig("")
	require.NoError(t, err)
	require.Equal(t, "preview", cfg.Network)
	cfg.Midnight.Enabled = true
	previewPolicy := cfg.Midnight.CNightPolicyID
	require.NotEmpty(t, previewPolicy)
	// preview sets CommitteeCandidateAddress but mainnet does not (see the
	// ground-truth load below), so this also proves the stale preview value
	// gets cleared, not just left alongside a filled-in mainnet value.
	require.NotEmpty(t, cfg.Midnight.CommitteeCandidateAddress)
	cfg.DatabasePath = dir

	require.NoError(t, settingsresolve.Apply(cfg))
	require.Equal(t, "mainnet", cfg.Network)

	// Ground truth for mainnet's Midnight constants, loaded independently
	// the same way a fresh `dingo -n mainnet` would derive them.
	tmpDir := t.TempDir()
	mainnetYAML := filepath.Join(tmpDir, "mainnet.yaml")
	require.NoError(
		t,
		os.WriteFile(mainnetYAML, []byte("network: mainnet\n"), 0o600),
	)
	mainnetCfg, err := config.LoadConfig(mainnetYAML)
	require.NoError(t, err)

	require.Equal(
		t,
		mainnetCfg.Midnight.CNightPolicyID,
		cfg.Midnight.CNightPolicyID,
	)
	require.NotEqual(t, previewPolicy, cfg.Midnight.CNightPolicyID)
	require.Equal(
		t,
		mainnetCfg.Midnight.CouncilPolicyID,
		cfg.Midnight.CouncilPolicyID,
	)
	require.Equal(
		t,
		mainnetCfg.Midnight.TechnicalCommitteeAddress,
		cfg.Midnight.TechnicalCommitteeAddress,
	)
	require.Empty(
		t,
		cfg.Midnight.CommitteeCandidateAddress,
		"stale preview CommitteeCandidateAddress must be cleared, not carried over to mainnet",
	)
	require.True(
		t,
		cfg.Midnight.Enabled,
		"Midnight.Enabled must not be touched by Apply",
	)
}

// TestApplyPreservesExplicitMidnightFieldOnResumedNetwork is the
// explicit-value-preservation half of the same fix: an operator-configured
// Midnight value (set via the YAML config file) must survive a resumed
// network change untouched, while every other, unconfigured Midnight
// constant still moves to the resumed network's defaults.
func TestApplyPreservesExplicitMidnightFieldOnResumedNetwork(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := seedDatabase(t, map[string]string{"network": "mainnet"})

	baseline, err := config.LoadConfig("")
	require.NoError(t, err)
	previewPolicy := baseline.Midnight.CNightPolicyID
	require.NotEmpty(t, previewPolicy)

	tmpDir := t.TempDir()
	yamlFile := filepath.Join(tmpDir, "dingo.yaml")
	require.NoError(t, os.WriteFile(yamlFile, []byte(
		"midnight:\n  enabled: true\n  cnightPolicyId: \""+previewPolicy+"\"\n",
	), 0o600))

	cfg, err := config.LoadConfig(yamlFile)
	require.NoError(t, err)
	require.Equal(t, "preview", cfg.Network)
	require.True(t, cfg.Midnight.Enabled)
	require.Equal(t, previewPolicy, cfg.Midnight.CNightPolicyID)
	cfg.DatabasePath = dir

	require.NoError(t, settingsresolve.Apply(cfg))
	require.Equal(t, "mainnet", cfg.Network)

	// The explicit YAML value must survive the resumed network change
	// untouched, even though it happens to equal preview's own default.
	require.Equal(
		t,
		previewPolicy,
		cfg.Midnight.CNightPolicyID,
		"explicit midnight.cnightPolicyId must not be clobbered by a resumed network change",
	)

	// Every other, unconfigured Midnight field must still move to the
	// resumed network's defaults.
	mainnetYAML := filepath.Join(tmpDir, "mainnet.yaml")
	require.NoError(
		t,
		os.WriteFile(mainnetYAML, []byte("network: mainnet\n"), 0o600),
	)
	mainnetCfg, err := config.LoadConfig(mainnetYAML)
	require.NoError(t, err)
	require.Equal(
		t,
		mainnetCfg.Midnight.CouncilPolicyID,
		cfg.Midnight.CouncilPolicyID,
	)
	require.NotEqual(
		t,
		baseline.Midnight.CouncilPolicyID,
		cfg.Midnight.CouncilPolicyID,
	)
}
