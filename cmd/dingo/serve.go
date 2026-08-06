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
	"errors"
	"fmt"
	"log/slog"
	"os"

	dingo "github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/node"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/mithril"
	"github.com/spf13/cobra"
)

func serveRun(
	cmd *cobra.Command, _ []string, cfg *config.Config,
) {
	logger := commonRun(cfg)

	// Check for an in-progress sync. If the "sync_status" key in
	// the sync_state table holds a non-empty value, a previous sync
	// did not complete. The user must finish (or re-run) the sync
	// before starting the node.
	if err := checkSyncState(cfg, logger); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	// CIP-0163: refuse to serve a Mithril-bootstrapped database with the
	// delegator-inactivity gate enabled. This closes the "run 'mithril sync'
	// with the gate off, then restart with it on" path that Guard 1 in the
	// sync command cannot see; a bootstrapped node cannot reproduce a
	// genesis-synced node's expiration state.
	if err := checkMithrilInactivityCompat(cfg, logger); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}

	// Historical metadata backfill is only needed for API mode.
	// API mode rebuilds any pending deferred indexes at the end of
	// resumeBackfill; Core mode has no backfill step, so it falls
	// through to the explicit repair below. Uses effectiveStorageMode,
	// not the raw configured value, so this choice agrees with the mode
	// openConfiguredDatabase actually opened above and the mode node.Run
	// is about to run with (see effectiveStorageMode's doc comment).
	if effectiveStorageMode(cfg).IsAPI() {
		if err := resumeBackfill(
			cmd.Context(), cfg, logger,
		); err != nil {
			slog.Error(
				"backfill resume failed",
				"error", err,
			)
			os.Exit(1)
		}
	} else {
		// Core mode: if a Mithril sync interrupted between drop
		// and rebuild, repair before exposing the node so
		// secondary-index-backed queries return correct
		// cardinalities.
		if err := repairDeferredIndexes(cfg, logger); err != nil {
			slog.Error(
				"deferred-index repair failed",
				"error", err,
			)
			os.Exit(1)
		}
	}

	// Run node
	if err := node.Run(cfg, logger); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
}

func checkSyncState(
	cfg *config.Config,
	logger *slog.Logger,
) error {
	runtime, err := openConfiguredDatabase(
		context.Background(), cfg, logger, 1,
	)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	db, runtimeErr := runtimeDatabase(runtime)
	if runtimeErr != nil {
		return runtimeErr
	}
	defer runtime.Close(context.Background()) //nolint:contextcheck
	if recoveryErr := runtime.RecoveryError(); recoveryErr != nil {
		// A commit-timestamp mismatch is recoverable downstream in
		// node.Run. We only need to read sync_status here, which
		// works on the partially-initialised db handle returned with
		// the error.
		var cte database.CommitTimestampError
		if !errors.As(recoveryErr, &cte) {
			return fmt.Errorf("opening database: %w", recoveryErr)
		}
		logger.Warn(
			"sync state check observed commit timestamp mismatch; "+
				"deferring recovery to node startup",
			"metadata_timestamp", cte.MetadataTimestamp,
			"blob_timestamp", cte.BlobTimestamp,
		)
	}

	val, err := db.GetSyncState("sync_status", nil)
	if err != nil {
		return fmt.Errorf("checking sync state: %w", err)
	}
	if val == "" || val == syncStatusBackfill {
		// metadata_indexes_pending can still be set if a prior
		// run dropped the deferred indexes and crashed before
		// the rebuild. The repair is handled below (Core mode)
		// or inside resumeBackfill (API mode) so the operator
		// does not have to re-bootstrap just to recreate
		// secondary indexes.
		return nil
	}
	return fmt.Errorf(
		"incomplete sync detected (sync_status=%q). "+
			"Run 'dingo sync' (or 'dingo sync --mithril' for "+
			"Mithril bootstrap) to resume before starting the node",
		val,
	)
}

// checkMithrilInactivityCompat refuses to start a node that has the CIP-0163
// delegator-inactivity gate enabled on a database populated by a Mithril
// bootstrap. Reward-account expiration state is dingo-only and absent from the
// cardano-ledger Mithril snapshot, and cannot be reconstructed after import, so
// serving such a database would diverge from a genesis-synced node. A no-op
// when the gate is off or the database was not Mithril-bootstrapped.
func checkMithrilInactivityCompat(
	cfg *config.Config,
	logger *slog.Logger,
) error {
	if !cfg.DelegatorInactivityEnabled {
		return nil
	}
	runtime, err := openConfiguredDatabase(
		context.Background(), cfg, logger, 1,
	)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	db, runtimeErr := runtimeDatabase(runtime)
	if runtimeErr != nil {
		return runtimeErr
	}
	defer runtime.Close(context.Background()) //nolint:contextcheck
	if recoveryErr := runtime.RecoveryError(); recoveryErr != nil {
		// A commit-timestamp mismatch is recovered downstream in node.Run;
		// the marker read works on the partially-initialised handle.
		var cte database.CommitTimestampError
		if !errors.As(recoveryErr, &cte) {
			return fmt.Errorf("opening database: %w", recoveryErr)
		}
	}

	bootstrapped, err := mithril.WasBootstrapped(db)
	if err != nil {
		return fmt.Errorf("checking mithril bootstrap marker: %w", err)
	}
	if bootstrapped {
		return errMithrilInactivityIncompatible()
	}
	return nil
}

// repairDeferredIndexes rebuilds any deferred metadata indexes left
// outstanding by a prior interrupted bulk-load run.
func repairDeferredIndexes(
	cfg *config.Config, logger *slog.Logger,
) error {
	runtime, err := openConfiguredDatabase(
		context.Background(), cfg, logger, cfg.DatabaseWorkers,
	)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	db, runtimeErr := runtimeDatabase(runtime)
	if runtimeErr != nil {
		return runtimeErr
	}
	defer runtime.Close(context.Background()) //nolint:contextcheck
	if recoveryErr := runtime.RecoveryError(); recoveryErr != nil {
		var cte database.CommitTimestampError
		if !errors.As(recoveryErr, &cte) {
			return fmt.Errorf("opening database: %w", recoveryErr)
		}
	}
	return node.RepairDeferredIndexes(db, logger)
}

// resumeBackfill checks whether metadata backfill is needed and
// runs it to completion before the node starts serving.
func resumeBackfill(
	ctx context.Context,
	cfg *config.Config,
	logger *slog.Logger,
) error {
	// Load Cardano node config for pparams and nonce
	// computation during backfill.
	cardanoConfigPath := cfg.CardanoConfig
	network := cfg.Network
	if cardanoConfigPath == "" {
		if network == "" {
			network = "preview"
		}
		cardanoConfigPath = network + "/config.json"
	}
	nodeCfg, nodeCfgErr := cardano.LoadCardanoNodeConfigWithFallback(
		cardanoConfigPath,
		network,
		cardano.EmbeddedConfigFS,
	)
	if nodeCfgErr != nil {
		logger.Warn(
			"could not load cardano node config, "+
				"backfill will skip pparams seeding "+
				"and nonce computation",
			"component", "backfill",
			"error", nodeCfgErr,
		)
	}

	runtime, err := openConfiguredDatabase(
		ctx, cfg, logger, cfg.DatabaseWorkers,
	)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	db, runtimeErr := runtimeDatabase(runtime)
	if runtimeErr != nil {
		return runtimeErr
	}
	defer runtime.Close(context.Background()) //nolint:contextcheck
	if recoveryErr := runtime.RecoveryError(); recoveryErr != nil {
		// Backfill writes through full transactions which heal a
		// commit-timestamp mismatch as it makes progress, and
		// node.Run will run a full recovery pass afterwards. So a
		// recoverable mismatch should not block the resume.
		var cte database.CommitTimestampError
		if !errors.As(recoveryErr, &cte) {
			return fmt.Errorf("opening database: %w", recoveryErr)
		}
		logger.Warn(
			"backfill observed commit timestamp mismatch; "+
				"continuing — backfill writes will heal it",
			"component", "backfill",
			"metadata_timestamp", cte.MetadataTimestamp,
			"blob_timestamp", cte.BlobTimestamp,
		)
	}

	bf := node.NewBackfill(db, nodeCfg, logger)
	if err := bf.SetBatchSize(cfg.BackfillBatchSize); err != nil {
		return fmt.Errorf(
			"invalid BackfillBatchSize %d in SetBatchSize: %w",
			cfg.BackfillBatchSize,
			err,
		)
	}
	bf.DisableNonceComputation()
	needed, err := bf.NeedsBackfill()
	if err != nil {
		return fmt.Errorf("checking backfill state: %w", err)
	}
	if !needed {
		// Still rebuild deferred indexes if a prior bulk-load
		// run left them pending — the previous backfill may
		// have completed but crashed before the critical rebuild
		// ran. The lazy remainder is handled by background
		// maintenance after the API starts.
		if err := node.RepairCriticalDeferredIndexes(db, logger); err != nil {
			return err
		}
		return clearBackfillSyncStatus(db)
	}

	if err := node.RunPlannerStats(db, logger); err != nil {
		return fmt.Errorf("running planner statistics before backfill: %w", err)
	}

	// Enable bulk-load optimizations for the backfill.
	cleanup := node.WithBulkLoadPragmas(db, logger)
	defer cleanup()

	logger.Info(
		"running metadata backfill",
		"component", "backfill",
	)
	if err := bf.Run(ctx); err != nil {
		return fmt.Errorf("backfill: %w", err)
	}
	// Rebuild critical deferred indexes before clearing sync_status so a
	// crash between the two leaves both markers set and the next
	// startup re-runs the rebuild.
	if err := node.RepairCriticalDeferredIndexes(db, logger); err != nil {
		return err
	}
	if err := clearBackfillSyncStatus(db); err != nil {
		return err
	}
	return nil
}

// effectiveStorageMode returns the storage mode this run will actually end
// up with once internal/node.Run applies its own dev-mode override ("dev
// mode always uses API storage for full transaction metadata"), so any
// preflight step that opens the database before node.Run gets to run --
// openConfiguredDatabase chief among them -- opens it in the same mode
// node.Run will, rather than whatever was configured.
//
// Without this, a dev-mode config with storageMode: core (permitted through
// by validate.go's dev-mode exemption for midnight.enabled, since dev mode
// is documented to upgrade to API before the contradiction would matter)
// would have this preflight open a database in core mode first. That
// latches storage_mode="core" as a node settings gate, and storage_mode is
// a LatchEnum that only ever moves api-to-core, never back -- so the very
// next open, from node.Run in its already-upgraded api mode, would fail
// enforcement against the gate this preflight just recorded.
func effectiveStorageMode(cfg *config.Config) dingo.StorageMode {
	mode := dingo.StorageMode(cfg.StorageMode)
	// internal/node.Run normalizes an unset mode to core before it reaches
	// WithStorageMode, so do the same here rather than returning "". The
	// values happen to converge today only because database.New applies its
	// own empty-to-core default, and leaning on that would leave this
	// helper's contract -- return exactly what node.Run will open with --
	// dependent on a third component's default staying put. storage_mode is
	// a one-way latch, so a future divergence here is not the kind that
	// merely looks untidy.
	if mode == "" {
		mode = dingo.StorageModeCore
	}
	if cfg.RunMode.IsDevMode() && !mode.IsAPI() {
		return dingo.StorageModeAPI
	}
	return mode
}

func openConfiguredDatabase(
	ctx context.Context,
	cfg *config.Config,
	logger *slog.Logger,
	maxConnections int,
) (*internalplugins.DatabaseRuntime, error) {
	storageMode := string(effectiveStorageMode(cfg))
	return internalplugins.OpenDatabase(
		ctx,
		&database.Config{
			DataDir: cfg.DatabasePath, Logger: logger,
			StorageMode:    storageMode,
			Network:        cfg.Network,
			NetworkMagic:   cfg.NetworkMagic,
			StartEra:       string(cfg.StartEra),
			BlobPlugin:     cfg.Plugins.Storage.Blob.Provider,
			MetadataPlugin: cfg.Plugins.Storage.Metadata.Provider,
		},
		internalplugins.StorageSelections{
			Blob:     cfg.Plugins.Storage.Blob,
			Metadata: cfg.Plugins.Storage.Metadata,
		},
		internalplugins.StorageDependencies{
			DataDir: cfg.DatabasePath, RunMode: string(cfg.RunMode),
			StorageMode: storageMode, MaxConnections: maxConnections,
			Logger: logger,
		},
	)
}

func runtimeDatabase(
	runtime *internalplugins.DatabaseRuntime,
) (*database.Database, error) {
	if runtime == nil || runtime.Database == nil {
		return nil, errors.New("database runtime did not provide a database")
	}
	return runtime.Database, nil
}

func clearBackfillSyncStatus(db *database.Database) error {
	status, err := db.GetSyncState("sync_status", nil)
	if err != nil {
		return fmt.Errorf("reading sync status: %w", err)
	}
	if status != syncStatusBackfill {
		return nil
	}
	if err := db.DeleteSyncState("sync_status", nil); err != nil {
		return fmt.Errorf("clearing backfill sync status: %w", err)
	}
	return nil
}

func serveCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run as a node",
		Run: func(cmd *cobra.Command, args []string) {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				slog.Error("no config found in context")
				os.Exit(1)
			}
			serveRun(cmd, args, cfg)
		},
	}
	return cmd
}
