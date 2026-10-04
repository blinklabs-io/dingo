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

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/nodesettings"
	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/plugin/metadata/deferred"
	"github.com/blinklabs-io/dingo/internal/config"
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
	gates, err := reopened.Database.Metadata().GetNodeSettingsGates()
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
// found in: idx_utxo_transaction_id absent with no pending marker to record
// it, so every rollback's DELETE FROM "transaction" cascade scanned utxo once
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
	_, err = raw.Exec("DROP INDEX IF EXISTS idx_utxo_transaction_id")
	require.NoError(t, err)
	_, err = raw.Exec(
		"DELETE FROM sync_state WHERE sync_key = ?",
		deferred.SyncStateKey,
	)
	require.NoError(t, err)
	require.NoError(t, dbtest.CloseDatabase(db))

	require.NoError(t, repairDeferredIndexes(cfg, logger))

	var count int
	require.NoError(t, raw.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?",
		"idx_utxo_transaction_id",
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

	name, err := mithrilRewardRepairNetwork(&config.Config{NetworkMagic: 764824073})
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

func TestCheckSyncStateDoesNotIgnoreOtherInterruptedSyncForRewardRepair(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := &config.Config{
		RunMode:      config.RunModeServe,
		StorageMode:  "core",
		Network:      "preview",
		DatabasePath: t.TempDir(),
		Plugins:      testStoragePlugins(),
	}
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

// TestEffectiveStorageMode pins effectiveStorageMode's dev-mode override so
// preflight callers and internal/node.Run's own "dev mode always uses API
// storage" upgrade never disagree about which mode a dev-mode config
// actually runs with.
func TestEffectiveStorageMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		runMode config.RunMode
		mode    string
		want    dingo.StorageMode
	}{
		{
			"dev mode upgrades core to api",
			config.RunModeDev,
			"core",
			dingo.StorageModeAPI,
		},
		{
			"dev mode leaves api as api",
			config.RunModeDev,
			"api",
			dingo.StorageModeAPI,
		},
		{
			"dev mode upgrades unset mode to api",
			config.RunModeDev,
			"",
			dingo.StorageModeAPI,
		},
		{
			"serve mode leaves core alone",
			config.RunModeServe,
			"core",
			dingo.StorageModeCore,
		},
		{
			"serve mode leaves api alone",
			config.RunModeServe,
			"api",
			dingo.StorageModeAPI,
		},
		{
			// internal/node.Run normalizes an unset mode to core before
			// WithStorageMode, so this helper must return core too, not "".
			// It returned "" before, and converged with Run only because
			// database.New applies its own empty-to-core default -- leaving
			// this helper's contract resting on a third component's default.
			"serve mode normalizes unset mode to core",
			config.RunModeServe,
			"",
			dingo.StorageModeCore,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := effectiveStorageMode(&config.Config{
				RunMode:     tt.runMode,
				StorageMode: tt.mode,
			})
			require.Equal(t, tt.want, got)
		})
	}
}

// TestCheckSyncStateDevModeAgreesWithLaterAPIModeOpen is a regression test
// for a reachable startup failure: validate.go exempts midnight.enabled
// from its storageMode-must-be-api check in dev mode, on the assumption
// that internal/node.Run's own "dev mode always uses API storage" override
// makes the contradiction moot. But serveRun's preflight (checkSyncState)
// opens the database before node.Run ever runs, and used to do so with the
// raw configured storage mode ("core" here) rather than the mode node.Run
// was about to upgrade to. That latched storage_mode="core" as a node
// settings gate; storage_mode is a LatchEnum that only ever moves
// api-to-core, never back, so node.Run's subsequent api-mode open of the
// same database would then fail enforcement.
//
// With effectiveStorageMode applied consistently, checkSyncState's open
// already uses "api" for a dev-mode config, so the gate it latches matches
// what every later open (including the one this test performs directly,
// standing in for node.Run's) needs.
func TestCheckSyncStateDevModeAgreesWithLaterAPIModeOpen(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()
	cfg := &config.Config{
		RunMode:      config.RunModeDev,
		StorageMode:  "core",
		Network:      "preview",
		DatabasePath: dir,
		Plugins:      testStoragePlugins(),
	}

	// Preflight open, exactly as serveRun performs it before node.Run runs.
	require.NoError(t, checkSyncState(cfg, logger))

	// Stand-in for node.Run's own database open, once it has upgraded to
	// API mode. Reuses openConfiguredDatabase (which node.go's real open
	// path also ultimately composes the same storage config through) so
	// this exercises the same effective-mode computation both call sites
	// share.
	runtime, err := openConfiguredDatabase(context.Background(), cfg, logger, 1)
	require.NoError(t, err)
	require.NoError(t, runtime.RecoveryError())
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })

	gates, err := runtime.Database.Metadata().GetNodeSettingsGates()
	require.NoError(t, err)
	require.Equal(
		t,
		"api",
		gates["storage_mode"],
		"dev mode's preflight open must have already latched api, not core",
	)
}

func TestNodeServicesFollowTheConfiguration(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{}
	require.Empty(t, nodeServices(cfg))

	cfg.Mithril.Signer.Enabled = true
	require.Len(t, nodeServices(cfg), 1)
}

func TestResumeBackfillFinalizesStatsBeforeClearingSync(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{
		RunMode:           config.RunModeServe,
		StorageMode:       "api",
		Network:           "preview",
		DatabasePath:      t.TempDir(),
		Plugins:           testStoragePlugins(),
		BackfillBatchSize: 100,
		DatabaseWorkers:   1,
	}
	logger := slog.New(slog.DiscardHandler)
	runtime, err := openConfiguredDatabase(t.Context(), cfg, logger, 1)
	require.NoError(t, err)
	require.NoError(
		t,
		runtime.Database.Metadata().
			SetBackfillCheckpoint(&models.BackfillCheckpoint{Phase: "metadata", Completed: true, UpdatedAt: time.Now().UTC()}, nil),
	)
	require.NoError(
		t,
		runtime.Database.SetSyncState("sync_status", syncStatusBackfill, nil),
	)
	raw, err := dbtest.RawSQLiteMetadata(t, runtime.Database)
	require.NoError(t, err)
	_, err = raw.Exec(
		`CREATE TRIGGER fail_stats_marker BEFORE INSERT ON sync_state WHEN NEW.sync_key='metadata_planner_stats_backfill' BEGIN SELECT RAISE(ABORT,'marker interrupted'); END`,
	)
	require.NoError(t, err)
	require.NoError(t, runtime.Close(t.Context()))
	require.ErrorContains(
		t,
		resumeBackfill(t.Context(), cfg, logger),
		"marker interrupted",
	)
	var status string
	require.NoError(
		t,
		raw.QueryRow("SELECT value FROM sync_state WHERE sync_key='sync_status'").
			Scan(&status),
	)
	require.Equal(t, syncStatusBackfill, status)
	_, err = raw.Exec("DROP TRIGGER fail_stats_marker")
	require.NoError(t, err)
	require.NoError(t, resumeBackfill(t.Context(), cfg, logger))
	var count int
	require.NoError(
		t,
		raw.QueryRow("SELECT COUNT(*) FROM sync_state WHERE sync_key='sync_status'").
			Scan(&count),
	)
	require.Zero(t, count)
	require.NoError(
		t,
		raw.QueryRow("SELECT value FROM sync_state WHERE sync_key=?", metadata.PlannerStatsBackfillSyncKey).
			Scan(&status),
	)
	require.NotEmpty(t, status)
}
