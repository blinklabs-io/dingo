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
	"context"
	"database/sql"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/internal/config"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	"github.com/blinklabs-io/dingo/mithril"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger/alonzo"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"
)

// TestCheckSyncStateRepairsLegacyAlonzoPParamsUnit covers the open that
// reaches a legacy database first. serveRun runs checkSyncState before
// node.Run, so the repair's genesis input has to be resolved by
// openConfiguredDatabase from the configuration alone: when it is not, this
// preflight — not the node's own later open, which does resolve it — is what
// aborts with the resync instruction the in-place repair exists to remove.
func TestCheckSyncStateRepairsLegacyAlonzoPParamsUnit(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()
	cfg := &config.Config{
		RunMode:      config.RunModeServe,
		StorageMode:  "core",
		Network:      "preview",
		DatabasePath: dir,
		Plugins:      testStoragePlugins(),
	}

	word := cardano.AlonzoLovelacePerUtxoWord(
		nil,
		cfg.CardanoConfig,
		cfg.Network,
	)
	require.NotZero(
		t,
		word,
		"the embedded preview config must supply an Alonzo genesis word",
	)

	// Seed a database in the shape a pre-gouroboros-v0.205.7 release left
	// behind: one Alonzo row holding the lossy quotient, and the
	// conservative marker migration v20 writes for it.
	seedMigratedMetadata(t, cfg)
	runtime, err := openConfiguredDatabase(context.Background(), cfg, logger, 1)
	require.NoError(t, err)
	params := alonzo.AlonzoProtocolParameters{AdaPerUtxoByte: word / 8}
	encoded, err := cbor.Encode(&params)
	require.NoError(t, err)
	require.NoError(t, runtime.Database.SetPParams(
		encoded, 0, 0, alonzo.EraIdAlonzo, nil,
	))
	require.NoError(t, runtime.Close(context.Background()))

	sqlDB, err := sql.Open(
		"sqlite",
		filepath.Join(dir, "metadata.sqlite")+"?_pragma=synchronous(OFF)",
	)
	require.NoError(t, err)
	_, err = sqlDB.Exec(
		`UPDATE node_settings_gate SET value = ? WHERE name = ?`,
		nodesettings.AlonzoPParamsUnitLegacyByteV0,
		nodesettings.AlonzoPParamsUnitGateName,
	)
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	require.NoError(t, checkSyncState(cfg, logger))

	reopened, err := openConfiguredDatabase(
		context.Background(),
		cfg,
		logger,
		1,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close(context.Background()) })
	gates, err := reopened.Database.Metadata().
		GetNodeSettingsGates(context.Background())
	require.NoError(t, err)
	require.Equal(
		t,
		nodesettings.AlonzoPParamsUnitWordV1,
		gates[nodesettings.AlonzoPParamsUnitGateName],
	)
}

// TestServeCoreModeRepairsMissingCascadeIndex covers the composition serveRun
// actually takes: a core-mode configuration reaches repairDeferredIndexes,
// which is the only deferred-index entry point on that path.
//
// The state under test is the one two Mithril-bootstrapped preview nodes were
// found in: idx_utxo_spent_at_tx_id absent with no pending marker to record
// it, so every rollback's delete of a transaction row scanned utxo once
// per deleted transaction row.
func TestServeCoreModeRepairsMissingCascadeIndex(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()
	cfg := &config.Config{
		DatabasePath:    dir,
		DatabaseWorkers: 1,
		Plugins:         testStoragePlugins(),
	}
	require.False(
		t,
		effectiveStorageMode(cfg).IsAPI(),
		"serveRun sends this configuration down the core-mode branch, "+
			"which is the branch repairDeferredIndexes is wired into",
	)

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: dir,
		Logger:  logger,
	})
	require.NoError(t, err)
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec("DROP INDEX IF EXISTS idx_utxo_spent_at_tx_id")
	require.NoError(t, err)
	_, err = raw.Exec(
		"DELETE FROM sync_state WHERE sync_key = ?",
		deferred.SyncStateKey,
	)
	require.NoError(t, err)
	require.NoError(t, dbtest.CloseDatabase(db))

	require.NoError(t, repairDeferredIndexes(t.Context(), cfg, logger))

	var count int
	require.NoError(t, raw.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?",
		"idx_utxo_spent_at_tx_id",
	).Scan(&count))
	require.Equal(
		t,
		1,
		count,
		"core-mode serve must restore the rollback cascade index before "+
			"the node starts",
	)
}

func TestMithrilRewardRepairConfig(t *testing.T) {
	t.Parallel()

	for _, backend := range []string{"", mithril.BackendV2} {
		t.Run("backend="+backend, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Mithril.Backend = backend
			cfg.Mithril.PinnedDigest = "pinned-digest"

			repairCfg, err := mithrilRewardRepairConfig(cfg)
			require.NoError(t, err)
			require.Equal(t, mithril.BackendV2, repairCfg.Mithril.Backend)
			require.Equal(t, "pinned-digest", repairCfg.Mithril.PinnedDigest)
		})
	}

	cfg := &config.Config{}
	cfg.Mithril.Backend = "v1"
	_, err := mithrilRewardRepairConfig(cfg)
	require.ErrorContains(t, err, "requires backend")
}

func TestMithrilRewardRepairNetwork(t *testing.T) {
	t.Parallel()

	name, err := mithrilRewardRepairNetwork(
		&config.Config{NetworkMagic: 764824073},
	)
	require.NoError(t, err)
	require.Equal(t, "mainnet", name)

	_, err = mithrilRewardRepairNetwork(&config.Config{NetworkMagic: 987654321})
	require.ErrorContains(t, err, "cannot resolve")
}

func TestCheckSyncStateAllowsInterruptedRewardRepairToResume(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &config.Config{
		RunMode:      config.RunModeServe,
		StorageMode:  "core",
		Network:      "preview",
		DatabasePath: t.TempDir(),
		Plugins:      testStoragePlugins(),
	}
	seedMigratedMetadata(t, cfg)
	runtime, err := openConfiguredDatabase(context.Background(), cfg, logger, 1)
	require.NoError(t, err)
	require.NoError(t, runtime.Database.SetSyncState(
		"sync_status", syncStatusInProgress, nil,
	))
	require.NoError(t, runtime.Database.SetSyncState(
		mithril.RewardStateRepairPendingKey, "1", nil,
	))
	require.NoError(t, runtime.Database.SetSyncState(
		mithril.RewardStateRepairActiveKey, "1", nil,
	))
	require.NoError(t, runtime.Close(context.Background()))

	require.NoError(t, checkSyncState(cfg, logger),
		"serve must resume an interrupted in-place repair before node startup")
}

func TestCheckSyncStateAllowsTheNodesOwnReimportToResume(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &config.Config{
		RunMode:      config.RunModeServe,
		StorageMode:  "core",
		Network:      "preview",
		DatabasePath: t.TempDir(),
		Plugins:      testStoragePlugins(),
	}
	seedMigratedMetadata(t, cfg)
	runtime, err := openConfiguredDatabase(context.Background(), cfg, logger, 1)
	require.NoError(t, err)
	require.NoError(t, runtime.Database.SetSyncState(
		"sync_status", syncStatusInProgress, nil,
	))
	require.NoError(t, runtime.Database.SetSyncState(
		database.LedgerReimportSyncKey, "1", nil,
	))
	require.NoError(t, runtime.Close(context.Background()))

	require.NoError(t, checkSyncState(cfg, logger),
		"serve must let the node run its own interrupted re-import")

	// The same status without the re-import mark is still an incomplete sync.
	runtime, err = openConfiguredDatabase(context.Background(), cfg, logger, 1)
	require.NoError(t, err)
	require.NoError(
		t,
		runtime.Database.DeleteSyncState(database.LedgerReimportSyncKey, nil),
	)
	require.NoError(t, runtime.Close(context.Background()))
	require.ErrorContains(
		t,
		checkSyncState(cfg, logger),
		"incomplete sync detected",
	)
}

func TestCheckSyncStateDoesNotIgnoreOtherInterruptedSyncForRewardRepair(
	t *testing.T,
) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &config.Config{
		RunMode:      config.RunModeServe,
		StorageMode:  "core",
		Network:      "preview",
		DatabasePath: t.TempDir(),
		Plugins:      testStoragePlugins(),
	}
	seedMigratedMetadata(t, cfg)
	runtime, err := openConfiguredDatabase(context.Background(), cfg, logger, 1)
	require.NoError(t, err)
	require.NoError(t, runtime.Database.SetSyncState(
		"sync_status", syncStatusInProgress, nil,
	))
	require.NoError(t, runtime.Database.SetSyncState(
		mithril.RewardStateRepairPendingKey, "1", nil,
	))
	require.NoError(t, runtime.Close(context.Background()))

	err = checkSyncState(cfg, logger)
	require.ErrorContains(t, err, "incomplete sync detected")
}

func TestRetryMithrilRewardStateRepairWaitsForNewSnapshot(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	attempts := 0
	err := retryMithrilRewardStateRepair(
		context.Background(),
		logger,
		time.Nanosecond,
		func() error {
			attempts++
			if attempts == 1 {
				return mithril.ErrRewardStateRepairWaitingForSnapshot
			}
			return nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, 2, attempts)
}

func TestRetryMithrilRewardStateRepairStopsOnOtherErrors(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	wantErr := context.DeadlineExceeded
	attempts := 0
	err := retryMithrilRewardStateRepair(
		context.Background(), logger, time.Nanosecond,
		func() error {
			attempts++
			return wantErr
		},
	)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, 1, attempts)
}

// TestCheckSyncStateDevModeAgreesWithTheNodeOpen starts a dev-mode node twice
// over one database: the preflight open, then the open node.Run performs once
// it has applied the dev-mode override. storage_mode only moves api to core,
// so a preflight that opened the consensus store as api was refused on the
// second start, after the node had latched it as core.
func TestCheckSyncStateDevModeAgreesWithTheNodeOpen(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &config.Config{
		RunMode:      config.RunModeDev,
		StorageMode:  "core",
		Network:      "preview",
		DatabasePath: t.TempDir(),
		Plugins:      testStoragePlugins(),
	}
	seedMigratedMetadata(t, cfg)
	for start := range 2 {
		preflight := *cfg
		require.NoError(t, checkSyncState(&preflight, logger), "start %d", start)

		run := *cfg
		run.ApplyRunModeOverrides(run.RunMode)
		require.True(t, run.SplitIndexer(), "dev mode serves the APIs")
		mode := run.ConsensusStorageMode()
		runtime, err := internalplugins.OpenDatabase(
			t.Context(),
			&database.Config{
				DataDir: run.DatabasePath, Logger: logger,
				StorageMode: mode, Network: run.Network,
			},
			internalplugins.ConfiguredStorageSelections(&run),
			internalplugins.StorageDependencies{
				DataDir: run.DatabasePath, RunMode: string(run.RunMode),
				StorageMode: mode, Logger: logger, GenesisStakingEmpty: true,
			},
		)
		require.NoError(t, err, "start %d", start)
		require.NoError(t, runtime.RecoveryError())
		gates, err := runtime.Database.Metadata().
			GetNodeSettingsGates(t.Context())
		require.NoError(t, err)
		require.Equal(t, "core", gates["storage_mode"])
		require.NoError(t, runtime.Close(context.WithoutCancel(t.Context())))
	}
}

// With mithril.enabled the node syncs an empty database itself, so serve
// accepts one; an interrupted manual sync still has to be finished first.
func TestServeAcceptsInNodeModeFreshDB(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &config.Config{
		RunMode:      config.RunModeServe,
		StorageMode:  "core",
		Network:      "preview",
		DatabasePath: t.TempDir(),
		Plugins:      testStoragePlugins(),
	}
	cfg.Mithril.Enabled = true
	seedMigratedMetadata(t, cfg)
	require.NoError(t, checkSyncState(cfg, logger),
		"an empty database must pass with node-managed Mithril")

	runtime, err := openConfiguredDatabase(context.Background(), cfg, logger, 1)
	require.NoError(t, err)
	require.NoError(t, runtime.Database.SetSyncState(
		"sync_status", syncStatusInProgress, nil,
	))
	require.NoError(t, runtime.Close(context.Background()))
	require.ErrorContains(t, checkSyncState(cfg, logger),
		"incomplete sync detected")
}

// The node's own interrupted snapshot restore is the node's to resume in
// snapshot mode. In any other mode, or with the delegator-inactivity gate
// that keeps the node off snapshots, it is refused like a manual sync.
func TestServeAcceptsInterruptedSnapshotRestore(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &config.Config{
		RunMode:      config.RunModeServe,
		StorageMode:  "core",
		Network:      "preview",
		DatabasePath: t.TempDir(),
		Plugins:      testStoragePlugins(),
	}
	cfg.Mithril.Enabled = true
	cfg.HistoryExpiry.Enabled = true

	seedMigratedMetadata(t, cfg)
	runtime, err := openConfiguredDatabase(context.Background(), cfg, logger, 1)
	require.NoError(t, err)
	require.NoError(t, runtime.Database.SetSyncState(
		"sync_status", syncStatusInProgress, nil,
	))
	require.NoError(t, runtime.Close(context.Background()))
	require.ErrorContains(t, checkSyncState(cfg, logger),
		"incomplete sync detected", "a manual sync's interruption passed")

	runtime, err = openConfiguredDatabase(context.Background(), cfg, logger, 1)
	require.NoError(t, err)
	require.NoError(t, runtime.Database.SetSyncState(
		"mithril_snapshot_restore_active", "1", nil,
	))
	require.NoError(t, runtime.Close(context.Background()))
	require.NoError(t, checkSyncState(cfg, logger))

	// Refused elsewhere, but pointed at the in-node resume: no sync
	// command can finish the node's restore.
	cfg.DelegatorInactivityEnabled = true
	err = checkSyncState(cfg, logger)
	require.ErrorContains(t, err, "node's own interrupted Mithril snapshot")
	require.NotContains(t, err.Error(), "dingo sync")
	cfg.DelegatorInactivityEnabled = false

	cfg.HistoryExpiry.Enabled = false
	err = checkSyncState(cfg, logger)
	require.ErrorContains(t, err, "node's own interrupted Mithril snapshot",
		"stream mode resumed a snapshot restore")
	require.NotContains(t, err.Error(), "dingo sync")
}
