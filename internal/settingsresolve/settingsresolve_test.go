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
	"database/sql"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/dbinfo"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlstore/migrations"
	"github.com/blinklabs-io/dingo/internal/config"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/settingsresolve"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
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

// seedDatabase leaves a data directory as this release does after a prior
// start: a migrated metadata store, and the node settings in the dbinfo file.
func seedDatabase(t *testing.T, gates map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	template, err := dbtest.MetadataTemplateBytes()
	require.NoError(t, err)
	require.NoError(t, dbtest.SeedMetadataTemplateBytes(dir, template))
	values := nodesettings.Values{"metadata_plugin": "sqlite"}
	maps.Copy(values, gates)
	require.NoError(t, dbinfo.WriteGates(dir, values))
	return dir
}

// seedReleasedDatabase leaves a data directory as v0.94.2 does: the node
// settings in the metadata store, and a format 1 dbinfo file naming the
// plugin.
func seedReleasedDatabase(t *testing.T, gates map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	ctx := context.Background()
	template, err := dbtest.MetadataTemplateBytes()
	require.NoError(t, err)
	require.NoError(t, dbtest.SeedMetadataTemplateBytes(dir, template))

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

	require.NoError(t, store.SetNodeSettingsGates(context.Background(),
		nodesettings.Values(gates), 0, 0,
	))
	require.NoError(t, dbinfo.Write(dir, dbinfo.Info{
		FormatVersion:  dbinfo.ReleasedFormatVersion,
		MetadataPlugin: "sqlite",
	}))
	return dir
}

// A data directory v0.94.2 left resumes from the settings in its metadata
// store, which the first open by this release has not moved yet.
func TestApplyResumesNetworkFromAReleasedDatabase(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := seedReleasedDatabase(t, map[string]string{"network": "preprod"})
	cfg := config.GetConfig()
	cfg.DatabasePath = dir
	cfg.Network = "preview"
	require.NoError(t, settingsresolve.Apply(cfg))
	require.Equal(t, "preprod", cfg.Network)
}

// A data directory this release opened resumes from its dbinfo file without
// opening any store, so it reads no node settings table.
func TestApplyResumesWithoutOpeningAStore(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := t.TempDir()
	db, err := dbtest.NewDatabaseWithOptions(t, dbtest.Options{
		Config: &database.Config{
			DataDir:        dir,
			StorageMode:    "core",
			Network:        "preprod",
			BlobPlugin:     "badger",
			MetadataPlugin: "sqlite",
		},
	})
	require.NoError(t, err)
	require.NoError(t, dbtest.CloseDatabase(db))

	opened := false
	defer settingsresolve.SetNewHost(func() (*plugin.Host, error) {
		opened = true
		return nil, errors.New("the read must not open a store")
	})()
	cfg := config.GetConfig()
	cfg.DatabasePath = dir
	cfg.Network = "preview"
	cfg.Plugins.Storage.Metadata.Provider = "sqlite"
	require.NoError(t, settingsresolve.Apply(cfg))
	require.Equal(t, "preprod", cfg.Network)
	require.False(t, opened)
}

// No test in this package calls t.Parallel: settingsresolve.Apply ends in
// config.PublishConfig, which replaces internal/config's process-global
// globalConfig, and the assertions here read it back.
func TestApplyResumesNetworkFromDefault(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := seedDatabase(t, map[string]string{"network": "preprod"})
	cfg := config.GetConfig()
	cfg.DatabasePath = dir
	cfg.Network = "preview" // the built-in default
	require.NoError(t, settingsresolve.Apply(cfg))
	require.Equal(t, "preprod", cfg.Network)
}

func TestApplyRejectsMalformedCarriedGateValues(t *testing.T) {
	cases := []struct {
		name  string
		gate  string
		value string
	}{
		{
			name:  "pledge leverage",
			gate:  "pledge_leverage",
			value: "on:not-a-number",
		},
		{
			name:  "delegator inactivity",
			gate:  "delegator_inactivity",
			value: "on:not-a-number",
		},
		{
			name:  "minimum pool margin",
			gate:  "min_pool_margin",
			value: "on:not-a-number",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isolateConfigSnapshot(t)
			dir := seedDatabase(t, map[string]string{tc.gate: tc.value})
			cfg := config.GetConfig()
			cfg.DatabasePath = dir
			err := settingsresolve.Apply(cfg)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.gate)
		})
	}
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
// the directory lifecycle.RestoreValidated's empty-target check then
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
		FormatVersion:  dbinfo.ReleasedFormatVersion,
		MetadataPlugin: "",
	}))
	cfg := config.GetConfig()
	cfg.DatabasePath = dir
	cfg.Plugins.Storage.Metadata.Provider = "sqlite"
	err := settingsresolve.Apply(cfg)
	require.Error(t, err)
}

// TestApplyProceedsOnUnreadableSidecar: a dbinfo file this build cannot
// read, here one of a newer format, is not Apply's to report. It warns,
// resolves nothing and leaves the open that follows to refuse the file.
func TestApplyProceedsOnUnreadableSidecar(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := seedDatabase(t, map[string]string{"network": "preprod"})
	require.NoError(t, dbinfo.Write(dir, dbinfo.Info{
		FormatVersion: dbinfo.CurrentFormatVersion + 1,
	}))
	cfg := config.GetConfig()
	cfg.DatabasePath = dir
	cfg.Network = "preview"
	require.NoError(t, settingsresolve.Apply(cfg))
	require.Equal(t, "preview", cfg.Network)
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

// On the IndexerDB layout the consensus store persists core while the
// configuration says api, because the IndexerDB is the API store. A restart
// must compare the store against the consensus mode, not the configured one.
func TestApplyAcceptsAnAPINodeRestart(t *testing.T) {
	for name, source := range map[string]config.Source{
		"configured":   config.SourceFlag,
		"by default":   config.SourceDefault,
		"from the env": config.SourceEnv,
	} {
		t.Run(name, func(t *testing.T) {
			isolateConfigSnapshot(t)
			dir := seedDatabase(t, map[string]string{
				"storage_mode":   "core",
				"indexer_layout": config.IndexerLayoutIndexerDB,
			})
			cfg := config.GetConfig()
			cfg.DatabasePath = dir
			cfg.StorageMode = "api"
			cfg.SetProvenanceForTest("StorageMode", source)
			require.NoError(t, settingsresolve.Apply(cfg))
			require.Equal(t, "api", cfg.StorageMode,
				"the configured mode is the IndexerDB's and stays api")
		})
	}
}

// A IndexerDB layout still refuses to move a consensus store back from core.
func TestApplyWithIndexerDBStillRefusesTheWrongNetwork(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := seedDatabase(t, map[string]string{
		"storage_mode": "core", "network": "preprod",
	})
	cfg := config.GetConfig()
	cfg.DatabasePath = dir
	cfg.StorageMode = "api"
	cfg.Network = "preview"
	cfg.SetProvenanceForTest("Network", config.SourceFlag)
	require.Error(t, settingsresolve.Apply(cfg))
}

// serverAsSQLiteHost registers SQLite stores under a server provider's name:
// the configured store in dir, its consensus namespace in dir/core.
func serverAsSQLiteHost(provider string) func() (*plugin.Host, error) {
	return func() (*plugin.Host, error) {
		host := plugin.NewHost()
		err := plugin.Register(
			host,
			plugin.Descriptor{
				Capability: plugin.CapabilityStorageMetadata,
				Name:       provider,
			},
			func() struct{} { return struct{}{} },
			func(_ context.Context, _ struct{}, deps metadata.ProviderDependencies) (*sqlstore.Store, plugin.Instance, error) {
				dir := deps.DataDir
				if deps.CoreNamespace {
					dir = filepath.Join(dir, "core")
				}
				store, err := sqlite.NewSQLStore(sqlite.Config{DataDir: dir}, deps)
				if err != nil {
					return nil, nil, err
				}
				return store, plugin.Lifecycle{
					StartFunc: store.Start,
					StopFunc:  store.CloseContext,
				}, nil
			},
		)
		return host, err
	}
}

// A v0.94.2 API store interrupted during its metadata backfill is refused
// before the store is opened; one that has since been latched as core keeps
// no API history to finish and resolves as any other.
func TestApplyRefusesAnInterruptedBackfillOfAnAPIStore(t *testing.T) {
	isolateConfigSnapshot(t)
	for mode, refused := range map[string]bool{"api": true, "core": false} {
		dir := seedReleasedDatabase(t, map[string]string{"storage_mode": mode})
		raw, err := sql.Open("sqlite", filepath.Join(dir, "metadata.sqlite"))
		require.NoError(t, err)
		_, err = raw.Exec(
			`INSERT INTO sync_state(sync_key,value) VALUES('sync_status','backfill')`,
		)
		require.NoError(t, err)
		require.NoError(t, raw.Close())
		cfg := config.GetConfig()
		cfg.DatabasePath = dir
		cfg.Plugins.Storage.Metadata.Provider = "sqlite"
		err = settingsresolve.Apply(cfg)
		if refused {
			require.ErrorIs(t, err, settingsresolve.ErrInterruptedBackfill, mode)
			continue
		}
		require.NoError(t, err, mode)
	}
}

// Once the node settings have moved into the dbinfo file, this release has
// opened the metadata store and upgraded its schema, so v0.94.2 can no longer
// finish a backfill there and startup does not read the store again.
func TestApplyReadsNoReleasedStoreOnceTheSettingsMoved(t *testing.T) {
	isolateConfigSnapshot(t)
	dir := seedReleasedDatabase(t, map[string]string{"storage_mode": "api"})
	raw, err := sql.Open("sqlite", filepath.Join(dir, "metadata.sqlite"))
	require.NoError(t, err)
	_, err = raw.Exec(
		`INSERT INTO sync_state(sync_key,value) VALUES('sync_status','backfill')`,
	)
	require.NoError(t, err)
	require.NoError(t, raw.Close())
	require.NoError(t, dbinfo.WriteGates(dir, nodesettings.Values{
		"metadata_plugin": "sqlite",
		"storage_mode":    "api",
	}))
	cfg := config.GetConfig()
	cfg.DatabasePath = dir
	cfg.StorageMode = "api"
	cfg.Plugins.Storage.Metadata.Provider = "sqlite"

	require.NoError(t, settingsresolve.Apply(cfg))
}

// A v0.94.2 API store interrupted during its metadata backfill is refused by
// the open of its provider, before that open migrates the schema: on a
// PostgreSQL or MySQL store, and on a SQLite store outside the data directory,
// none of which the read of the data directory's SQLite file covers. The
// store is left as v0.94.2 wrote it, which that release still opens.
func TestApplyRefusesAnInterruptedBackfillBeforeTheProviderMigrates(
	t *testing.T,
) {
	isolateConfigSnapshot(t)
	for _, tc := range []struct {
		name     string
		provider string
		external bool
	}{
		{name: "postgres", provider: "postgres"},
		{name: "mysql", provider: "mysql"},
		{name: "sqlite outside the data directory", provider: "sqlite", external: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			storeDir := dir
			cfg := config.GetConfig()
			cfg.DatabasePath = dir
			cfg.StorageMode = "api"
			cfg.Plugins.Storage.Metadata = plugin.Selection{Provider: tc.provider}
			if tc.external {
				storeDir = t.TempDir()
				cfg.Plugins.Storage.Metadata.Config = map[string]any{
					"dataDir": storeDir,
				}
				require.NoError(t, os.Mkdir(filepath.Join(dir, "blob"), 0o755))
			} else {
				defer settingsresolve.SetNewHost(
					serverAsSQLiteHost(tc.provider),
				)()
			}
			require.NoError(t, dbtest.SeedReleasedMetadata(storeDir, "backfill"))
			before, err := dbtest.MetadataFingerprint(storeDir)
			require.NoError(t, err)

			err = settingsresolve.Apply(cfg)
			require.ErrorIs(t, err, settingsresolve.ErrInterruptedBackfill)
			require.ErrorContains(t, err, "start dingo v0.94.2")

			after, err := dbtest.MetadataFingerprint(storeDir)
			require.NoError(t, err)
			require.Equal(t, before, after,
				"the refusal changed the released store")
			status, err := dbtest.ReleasedMetadataSyncStatus(storeDir)
			require.NoError(t, err, "v0.94.2 no longer opens the store")
			require.Equal(t, "backfill", status)

			// The same store with its backfill finished upgrades.
			require.NoError(t, os.RemoveAll(storeDir))
			require.NoError(t, dbtest.SeedReleasedMetadata(storeDir, ""))
			require.NoError(t, settingsresolve.Apply(cfg))
			_, err = dbtest.ReleasedMetadataSyncStatus(storeDir)
			require.ErrorIs(t, err, migrations.ErrNewerSchema)
		})
	}
}
