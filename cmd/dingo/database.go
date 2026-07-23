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

	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/dblifecycle"
	"github.com/spf13/cobra"
)

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
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				return errors.New("no config found in context")
			}
			if destDir == "" {
				return errors.New("--dir is required")
			}
			logger := commonRun(cfg)
			svc := dblifecycle.NewService(cfg, logger)
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
			svc := dblifecycle.NewService(cfg, logger)
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
from the target point.`,
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
			svc := dblifecycle.NewService(cfg, logger)
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
