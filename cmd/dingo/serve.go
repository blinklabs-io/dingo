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
	"time"

	dingo "github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/blinklabs-io/dingo/internal/nodemithril"
	"github.com/blinklabs-io/dingo/internal/nodestorage"
	internalplugins "github.com/blinklabs-io/dingo/internal/plugins"
	"github.com/blinklabs-io/dingo/mithril"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/spf13/cobra"
)

func serveRun(
	cmd *cobra.Command, _ []string, cfg *config.Config,
) error {
	logger, err := commonRun(cmd.Context(), cfg)
	if err != nil {
		return err
	}
	applyDebugProfilingRates(logger, os.Stderr, cfg)

	// Check for an in-progress sync. If the "sync_status" key in
	// the sync_state table holds a non-empty value, a previous sync
	// did not complete. The user must finish (or re-run) the sync
	// before starting the node.
	if err := checkSyncState(cfg, logger); err != nil {
		return err
	}
	// CIP-0163: refuse to serve a Mithril-bootstrapped database with the
	// delegator-inactivity gate enabled. This closes the "run 'mithril sync'
	// with the gate off, then restart with it on" path that Guard 1 in the
	// sync command cannot see; a bootstrapped node cannot reproduce a
	// genesis-synced node's expiration state.
	if err := checkMithrilInactivityCompat(cfg, logger); err != nil {
		return err
	}
	if err := repairPendingMithrilRewardState(
		cmd.Context(), cfg, logger,
	); err != nil {
		return err
	}

	// Run node. The node runs Mithril catch-up itself under mithril.enabled;
	// nodemithril supplies the mithril package it cannot import.
	if err := node.Run(
		cmd.Context(), cfg, logger,
		dingo.WithMithrilCatchUp(nodemithril.New),
		dingo.WithMithrilLedgerReimport(nodemithril.NewReimporter),
	); err != nil {
		return err
	}
	return nil
}

// checkSyncState refuses to serve a database a manual sync left incomplete.
// An empty sync_status passes in every mode, an empty database included:
// with mithril.enabled the node then syncs it from Mithril or Ouroboros
// itself. So does the backfill status, which a store that left API mode
// during a v0.94.2 metadata backfill still carries: a core store keeps no
// history to backfill, and the migration refuses one that is still API. So do
// an interrupted reward-state repair, which serve finishes, and, in Mithril
// snapshot mode,
// the node's own interrupted snapshot restore, which the node resumes. Any
// other value is an interrupted `dingo sync` or `dingo mithril sync`, which
// only that command can finish, so it refuses whatever mithril.enabled says.
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

	val, err := db.GetChainState(database.SyncStatusKey, nil)
	if err != nil {
		return fmt.Errorf("checking sync state: %w", err)
	}
	if val == "" || val == syncStatusBackfill {
		return nil
	}
	pendingRepair, err := mithril.RewardStateRepairPending(db)
	if err != nil {
		return err
	}
	repairActive, err := mithril.RewardStateRepairActive(db)
	if err != nil {
		return err
	}
	if val == syncStatusInProgress && pendingRepair && repairActive {
		return nil
	}
	// The node runs its own ledger-state re-import again at start.
	ownReimport, err := mithril.ReimportInterrupted(db)
	if err != nil {
		return err
	}
	if ownReimport {
		return nil
	}
	// The node resumes its own snapshot restore, but only in the mode that
	// started it, and not with the delegator-inactivity gate it could never
	// have started under. No sync command can finish it, so any other
	// configuration is told how to let the node do so.
	ownRestore, err := mithril.SnapshotRestoreInterrupted(db)
	if err != nil {
		return err
	}
	if ownRestore {
		if mithril.SelectMode(
			cfg.Mithril.Enabled, cfg.HistoryExpiry.Enabled,
		) == mithril.ModeSnapshot && !cfg.DelegatorInactivityEnabled {
			return nil
		}
		return errors.New(
			"the database holds the node's own interrupted Mithril " +
				"snapshot restore, which only the node finishes: start " +
				"it with mithril.enabled and historyExpiry.enabled set " +
				"and delegatorInactivityEnabled off to resume the " +
				"restore, or remove the database directory to sync " +
				"from scratch",
		)
	}
	return fmt.Errorf(
		"incomplete sync detected (sync_status=%q). "+
			"Run 'dingo sync' (or 'dingo sync --mithril' for "+
			"Mithril bootstrap) to resume before starting the node",
		val,
	)
}

// repairPendingMithrilRewardState reconciles a legacy bootstrapped database
// against a certified artifact before node startup. Mithril's catch-up path
// verifies the existing chain intersection before writing, rolls the LedgerDB
// set back to the artifact's ledger state through the set's journal and
// imports that state over it. A local chain tail beyond the artifact is kept
// for ordinary ledger replay after the selected state passes the stable-anchor
// check. If no artifact reaches that anchor, or the set's journal does not
// reach the artifact's state, the durable repair marker remains and this
// function keeps the node offline instead of serving inconsistent rewards.
func repairPendingMithrilRewardState(
	ctx context.Context,
	cfg *config.Config,
	logger *slog.Logger,
) error {
	pending, err := mithrilRewardRepairPending(ctx, cfg, logger)
	if err != nil || !pending {
		return err
	}
	logger.Warn(
		"reconciling legacy Mithril reward state before serving",
		"component", "node",
	)
	repairCfg, err := mithrilRewardRepairConfig(cfg)
	if err != nil {
		return err
	}
	network, err := mithrilRewardRepairNetwork(repairCfg)
	if err != nil {
		return err
	}
	stopTracing := startStandaloneTracing(ctx, repairCfg, logger)
	err = retryMithrilRewardStateRepair(
		ctx,
		logger,
		mithrilRewardRepairRetryInterval,
		func() error {
			return runMithrilSyncForRewardRepair(
				ctx, repairCfg, logger, network,
			)
		},
	)
	stopTracing()
	if err != nil {
		return fmt.Errorf("repairing legacy Mithril reward state: %w", err)
	}
	pending, err = mithrilRewardRepairPending(ctx, cfg, logger)
	if err != nil {
		return err
	}
	if pending {
		return errors.New(
			"mithril reward-state repair did not reconcile the existing database; " +
				"the database is preserved and node startup is blocked until a " +
				"certified snapshot's ledger state can be imported over it",
		)
	}
	return nil
}

func mithrilRewardRepairConfig(cfg *config.Config) (*config.Config, error) {
	if cfg.Mithril.Backend != "" &&
		cfg.Mithril.Backend != mithril.BackendV2 {
		return nil, fmt.Errorf(
			"mithril reward-state repair requires backend %q; configured backend is %q",
			mithril.BackendV2, cfg.Mithril.Backend,
		)
	}
	repairCfg := *cfg
	repairCfg.Mithril.Backend = mithril.BackendV2
	return &repairCfg, nil
}

func mithrilRewardRepairNetwork(cfg *config.Config) (string, error) {
	if cfg.Network != "" {
		return cfg.Network, nil
	}
	network, ok := ouroboros.NetworkByNetworkMagic(cfg.NetworkMagic)
	if !ok {
		return "", fmt.Errorf(
			"cannot resolve Mithril reward-state repair network from network magic %d",
			cfg.NetworkMagic,
		)
	}
	return network.Name, nil
}

const mithrilRewardRepairRetryInterval = 5 * time.Minute

func retryMithrilRewardStateRepair(
	ctx context.Context,
	logger *slog.Logger,
	interval time.Duration,
	repair func() error,
) error {
	for {
		err := repair()
		if err == nil || !errors.Is(
			err,
			mithril.ErrRewardStateRepairWaitingForSnapshot,
		) {
			return err
		}
		logger.Warn(
			"latest certified snapshot's ledger state cannot be imported yet; "+
				"preserving the database and retrying in-place repair",
			"component", "node",
			"retry_after", mithrilRewardRepairRetryInterval,
			"error", err,
		)
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func mithrilRewardRepairPending(
	ctx context.Context,
	cfg *config.Config,
	logger *slog.Logger,
) (bool, error) {
	runtime, err := openConfiguredDatabase(
		ctx, cfg, logger, 1,
	)
	if err != nil {
		return false, fmt.Errorf(
			"opening database for reward repair check: %w",
			err,
		)
	}
	db, err := runtimeDatabase(runtime)
	if err != nil {
		return false, err
	}
	defer runtime.Close(context.WithoutCancel(ctx))
	return mithril.RewardStateRepairPending(db)
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

	bootstrapped, err := mithril.WasBootstrapped(db)
	if err != nil {
		return fmt.Errorf("checking mithril bootstrap marker: %w", err)
	}
	if bootstrapped {
		return errMithrilInactivityIncompatible()
	}
	return nil
}

// effectiveStorageMode returns the storage mode of the consensus store, which
// every preflight step that opens the database before node.Run must open it
// in. The store is core in every run mode, dev mode included: API rows live in
// the IndexerDB, which the node opens in API mode itself. storage_mode is a
// latch that only moves api to core, so a preflight that opened the store as
// api would be refused on the start after the node latched it as core.
func effectiveStorageMode(*config.Config) dingo.StorageMode {
	return dingo.StorageModeCore
}

func openConfiguredDatabase(
	ctx context.Context,
	cfg *config.Config,
	logger *slog.Logger,
	maxConnections int,
) (*internalplugins.DatabaseRuntime, error) {
	genesisStakingEmpty, err := internalplugins.ResolveGenesisStakingEmpty(
		nil, cfg.CardanoConfig, cfg.Network, cfg.NetworkMagic,
	)
	if err != nil {
		return nil, fmt.Errorf("loading cardano node config: %w", err)
	}
	storageMode := string(effectiveStorageMode(cfg))
	dbConfig := &database.Config{
		DataDir: cfg.DatabasePath, Logger: logger,
		StorageMode:    storageMode,
		Network:        cfg.Network,
		NetworkMagic:   cfg.NetworkMagic,
		StartEra:       string(cfg.StartEra),
		BlobPlugin:     cfg.Plugins.Storage.Blob.Provider,
		MetadataPlugin: cfg.Plugins.Storage.Metadata.Provider,
		AlonzoLovelacePerUtxoWord: cardano.AlonzoLovelacePerUtxoWord(
			nil, cfg.CardanoConfig, cfg.Network,
		),
	}
	return internalplugins.OpenDatabase(
		ctx,
		dbConfig,
		internalplugins.ConfiguredStorageSelections(cfg),
		internalplugins.StorageDependencies{
			GenesisStakingEmpty: genesisStakingEmpty,
			DataDir:             cfg.DatabasePath, RunMode: string(cfg.RunMode),
			StorageMode: storageMode, MaxConnections: maxConnections,
			Logger: logger, TracingEnabled: cfg.Tracing,
			MigrateReleased: nodestorage.MigrateReleased(cfg, dbConfig, nil, logger),
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

func serveCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run as a node",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				return errors.New("no config found in context")
			}
			return serveRun(cmd, args, cfg)
		},
	}
	return cmd
}
