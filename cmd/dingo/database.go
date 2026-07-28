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
	"os/signal"
	"syscall"

	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
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
	var destDir string
	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "Capture a point-in-time snapshot of the database",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				return errors.New("no config found in context")
			}
			if destDir == "" {
				return errors.New("--dir is required")
			}
			logger := commonRun(cfg)
			svc := dblifecycle.NewService(cfg, newCLIDestinationRegistry(), logger)
			manifest, err := svc.Snapshot(cmd.Context(), destDir)
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
		},
	}
	cmd.Flags().StringVar(
		&destDir,
		"dir",
		"",
		"destination directory for the snapshot (must not already exist)",
	)
	return cmd
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
			logger := commonRun(cfg)
			svc := dblifecycle.NewService(cfg, newCLIDestinationRegistry(), logger)

			// Restore can run for a long time against a large database,
			// and Cobra's default cmd.Context() is a plain
			// context.Background() with no signal handling wired in
			// anywhere above this command -- without this, an operator's
			// Ctrl+C (SIGINT) or a SIGTERM would not cancel ctx at all,
			// leaving Restore no way to notice the interrupt and return
			// cleanly (only the default Go runtime behavior of killing
			// the process outright, skipping every deferred cleanup).
			// database/lifecycle.RestoreValidated's staging-directory-
			// plus-atomic-rename design already ensures the configured
			// data directory itself is left untouched either way, but a
			// signal-aware context here is what lets a well-behaved
			// interrupt (this one) actually be observed by Restore and
			// fail fast, matching how internal/node/node.go's serve path
			// installs the same signal handling for the live node.
			ctx, stop := signal.NotifyContext(
				cmd.Context(), syscall.SIGINT, syscall.SIGTERM,
			)
			defer stop()

			manifest, err := svc.Restore(ctx, args[0])
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
from the target point.`,
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
			logger := commonRun(cfg)
			// Truncate never resolves a cloud destination, so a nil
			// registry here is fine — see DestinationRegistry's doc
			// comment.
			svc := dblifecycle.NewService(cfg, nil, logger)
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
