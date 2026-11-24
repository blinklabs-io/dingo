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
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/blinklabs-io/dingo/internal/version"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/spf13/cobra"
)

// newCLIDestinationRegistry builds the set of cloud destination schemes
// (s3, gcs) available to the offline `dingo database` commands, explicitly
// at this composition boundary rather than through a process-global
// registry populated by each scheme's own package.
func newCLIDestinationRegistry() *lifecycle.DestinationRegistry {
	registry := lifecycle.NewDestinationRegistry()
	lifecycle.RegisterBuiltinDestinations(registry)
	return registry
}

// databaseCommand is the parent for the offline database lifecycle
// maintenance commands. Each subcommand operates directly against the
// configured data directory (like `load`/`mithril`) and must not be run
// against a data directory a `dingo serve` process currently has open.
// newDatabaseService builds the lifecycle service the offline commands share.
// On the split layout a truncate rolls the IndexerDB back through the same
// writer the node runs.
func newDatabaseService(
	cfg *config.Config,
	registry *lifecycle.DestinationRegistry,
	logger *slog.Logger,
) *dblifecycle.Service {
	svc := dblifecycle.NewService(cfg, registry, logger)
	svc.SetIndexerRollbackFactory(func(
		consensus, indexer *database.Database,
	) (dblifecycle.IndexerRoller, error) {
		return node.NewIndexerService(dingo.IndexerWriterDeps{
			Consensus:                  consensus,
			Indexer:                    indexer,
			Logger:                     logger,
			MaxReadyLagBlocks:          cfg.Indexer.MaxReadyLagBlocks,
			DelegatorInactivityEnabled: cfg.DelegatorInactivityEnabled,
		})
	})
	return svc
}

func databaseCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "database",
		Short: "Database snapshot, restore, and truncate maintenance commands",
	}
	cmd.AddCommand(databaseSnapshotCommand())
	cmd.AddCommand(databaseRestoreCommand())
	cmd.AddCommand(databaseTruncateCommand())
	return cmd
}

func databaseSnapshotCommand() *cobra.Command {
	var (
		destDir    string
		sclsOutput string
	)
	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "Capture a native database snapshot or export SCLS",
		Long: `Capture a point-in-time native database snapshot at --dir, or export
the live UTxO set as an SCLS (CIP-0165) parity file at --scls. Exactly one
output must be selected. The configured data directory must not be open in a
running node.

An SCLS export reads one snapshot of the live UTxO set and persists no
checkpoint. It stages the file as <scls>.partial, verifies it in full, and
hard-links it into place without replacing an existing file. A failure or an
interrupt removes the staged file. The output directory must support hard
links. A staged file left by a killed export is refused and must be removed
once no export is running.

Only a Conway state is exported, and only the utxo/v0 namespace. The file
declares itself a parity export in its manifest (profile=parity,
bootstrap=false): it is not a bootstrap artifact and a node cannot be
restarted from it.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				return errors.New("no config found in context")
			}
			logger, err := commonRun(cmd.Context(), cfg)
			if err != nil {
				return err
			}
			return runDatabaseSnapshot(
				cmd, cfg, logger, destDir, sclsOutput,
			)
		},
	}
	cmd.Flags().StringVar(
		&destDir,
		"dir",
		"",
		"destination directory for the snapshot (must not already exist)",
	)
	cmd.Flags().StringVar(
		&sclsOutput,
		"scls",
		"",
		"SCLS parity file to create (must not already exist)",
	)
	cmd.MarkFlagsMutuallyExclusive("dir", "scls")
	cmd.MarkFlagsOneRequired("dir", "scls")
	return cmd
}

func runDatabaseSnapshot(
	cmd *cobra.Command,
	cfg *config.Config,
	logger *slog.Logger,
	destDir, sclsOutput string,
) error {
	if sclsOutput != "" {
		res, err := exportSCLS(cmd, cfg.DatabasePath, sclsOutput)
		if err != nil {
			return fmt.Errorf("snapshot SCLS: %w", err)
		}
		fmt.Printf(
			"SCLS parity export written to %s\n"+
				"era=%s slot=%d block=%s namespaces=%s entries=%d root=%x\n",
			sclsOutput,
			strings.ToLower(ledgerstate.EraName(res.Era)),
			res.Slot,
			hex.EncodeToString(res.BlockHash),
			strings.Join(res.Namespaces, ","),
			res.Entries,
			res.Root[:],
		)
		return nil
	}

	svc := newDatabaseService(
		cfg,
		newCLIDestinationRegistry(),
		logger,
	)
	manifest, err := svc.Snapshot(cmd.Context(), destDir, "", "")
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	fmt.Printf(
		"Snapshot written to %s (tip slot=%d, block=%d)\n",
		destDir,
		manifest.TipSlot,
		manifest.TipBlockNumber,
	)
	return nil
}

func databaseRestoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore <snapshot-dir>",
		Short: "Restore the database from a snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				return errors.New("no config found in context")
			}
			logger, err := commonRun(cmd.Context(), cfg)
			if err != nil {
				return err
			}
			svc := newDatabaseService(
				cfg,
				newCLIDestinationRegistry(),
				logger,
			)

			manifest, err := svc.Restore(cmd.Context(), args[0])
			if err != nil {
				return fmt.Errorf("restore: %w", err)
			}
			fmt.Printf(
				"Database restored to %s (tip slot=%d, block=%d)\n",
				cfg.DatabasePath,
				manifest.TipSlot,
				manifest.TipBlockNumber,
			)
			return nil
		},
	}
	return cmd
}

func databaseTruncateCommand() *cobra.Command {
	var (
		slot        uint64
		hash        string
		blockNumber uint64
	)
	cmd := &cobra.Command{
		Use:   "truncate",
		Short: "Truncate the database to a target point (slot, block hash, or block number)",
		Long: `Truncate the database to a target point, identified by exactly one of
--slot, --hash, or --block-number. The target block becomes the new chain
tip; every block and metadata row added after it is removed. Unlike a
normal chain rollback, this does not reject a target beyond the configured
security parameter — it is intended for disaster recovery scenarios (see
CIP-0135) where the chain must be rewound further than Ouroboros Praos's
built-in rollback limit allows. The resulting database is resync-ready
from the target point.

A target older than what this database's own routine retention has already
pruned is refused rather than silently producing a corrupt resync-ready
state: this command checks the target against the recorded Mithril trust
boundary and the consumed-UTxO retention floor before touching anything. A
refusal means the local database no longer holds enough history to safely
resync from that point; a fully synced peer's snapshot is the recovery
path, not a shallower local target.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				return errors.New("no config found in context")
			}
			target := dblifecycle.TruncateTarget{}
			if cmd.Flags().Changed("slot") {
				target.Slot = &slot
			}
			if cmd.Flags().Changed("hash") {
				decoded, err := hex.DecodeString(hash)
				if err != nil {
					return fmt.Errorf("invalid --hash: %w", err)
				}
				target.Hash = decoded
			}
			if cmd.Flags().Changed("block-number") {
				target.BlockNumber = &blockNumber
			}
			logger, err := commonRun(cmd.Context(), cfg)
			if err != nil {
				return err
			}
			// Truncate never resolves a cloud destination, so a nil
			// registry here is fine — see DestinationRegistry's doc
			// comment.
			svc := newDatabaseService(cfg, nil, logger)

			blocksRemoved, err := svc.Truncate(cmd.Context(), target)
			if err != nil {
				return fmt.Errorf("truncate: %w", err)
			}
			fmt.Printf(
				"Database truncated successfully (%d block(s) removed).\n",
				blocksRemoved,
			)
			return nil
		},
	}
	cmd.Flags().Uint64Var(&slot, "slot", 0, "truncate target slot")
	cmd.Flags().StringVar(&hash, "hash", "", "truncate target block hash (hex)")
	cmd.Flags().
		Uint64Var(&blockNumber, "block-number", 0, "truncate target block number")
	return cmd
}

// exportSCLS opens the set under dataDir, exports one snapshot of it and
// releases both on every path out.
func exportSCLS(
	cmd *cobra.Command,
	dataDir, output string,
) (ledgerstate.SCLSExportResult, error) {
	store, err := dblifecycle.OpenUtxoSet(dataDir)
	if err != nil {
		return ledgerstate.SCLSExportResult{}, err
	}
	if store == nil {
		return ledgerstate.SCLSExportResult{}, fmt.Errorf(
			"no live UTxO set under %s", dataDir,
		)
	}
	defer store.Close()
	src, err := ledgerstate.NewSCLSSource(store)
	if err != nil {
		return ledgerstate.SCLSExportResult{}, err
	}
	defer src.Close()
	return ledgerstate.ExportSCLS(cmd.Context(), ledgerstate.SCLSExportConfig{
		Source: src,
		Output: output,
		Tool:   "dingo " + version.GetVersionString(),
	})
}
