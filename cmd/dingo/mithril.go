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
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/dingo/mithril"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

func mithrilCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mithril",
		Short: "Mithril snapshot management commands",
	}

	cmd.AddCommand(mithrilListCommand())
	cmd.AddCommand(mithrilShowCommand())
	cmd.AddCommand(mithrilSyncCommand())

	return cmd
}

func mithrilListCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List available Mithril snapshots",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				return errors.New("no config found in context")
			}

			network := cfg.Network
			if network == "" {
				network = "preview"
			}

			aggregatorURL := cfg.Mithril.AggregatorURL
			if aggregatorURL == "" {
				var err error
				aggregatorURL, err = mithril.AggregatorURLForNetwork(
					network,
				)
				if err != nil {
					return err
				}
			}

			client := mithril.NewClient(aggregatorURL)
			snapshots, err := client.ListSnapshots(
				cmd.Context(),
			)
			if err != nil {
				return fmt.Errorf("listing snapshots: %w", err)
			}

			if len(snapshots) == 0 {
				fmt.Println("No snapshots available.")
				return nil
			}

			fmt.Printf(
				"%-16s  %-8s  %-8s  %12s  %s\n",
				"DIGEST",
				"EPOCH",
				"IMMUT#",
				"SIZE",
				"CREATED",
			)
			for _, s := range snapshots {
				digest := s.Digest
				if len(digest) > 16 {
					digest = digest[:16]
				}
				created := s.CreatedAt
				if len(created) > 19 {
					created = created[:19]
				}
				fmt.Printf(
					"%-16s  %-8d  %-8d  %12s  %s\n",
					digest,
					s.Beacon.Epoch,
					s.Beacon.ImmutableFileNumber,
					humanBytes(s.Size),
					created,
				)
			}

			return nil
		},
	}
	return cmd
}

func mithrilShowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show <digest>",
		Short: "Show details of a specific Mithril snapshot",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				return errors.New("no config found in context")
			}

			network := cfg.Network
			if network == "" {
				network = "preview"
			}

			aggregatorURL := cfg.Mithril.AggregatorURL
			if aggregatorURL == "" {
				var err error
				aggregatorURL, err = mithril.AggregatorURLForNetwork(
					network,
				)
				if err != nil {
					return err
				}
			}

			client := mithril.NewClient(aggregatorURL)
			snapshot, err := client.GetSnapshot(
				cmd.Context(),
				args[0],
			)
			if err != nil {
				return fmt.Errorf("getting snapshot: %w", err)
			}

			fmt.Printf("Digest:                %s\n", snapshot.Digest)
			fmt.Printf("Network:               %s\n", snapshot.Network)
			fmt.Printf(
				"Epoch:                 %d\n",
				snapshot.Beacon.Epoch,
			)
			fmt.Printf(
				"Immutable File Number: %d\n",
				snapshot.Beacon.ImmutableFileNumber,
			)
			fmt.Printf("Size:                  %s\n", humanBytes(snapshot.Size))
			fmt.Printf(
				"Certificate Hash:      %s\n",
				snapshot.CertificateHash,
			)
			fmt.Printf(
				"Compression:           %s\n",
				snapshot.CompressionAlgorithm,
			)
			fmt.Printf(
				"Cardano Node Version:  %s\n",
				snapshot.CardanoNodeVersion,
			)
			fmt.Printf("Created:               %s\n", snapshot.CreatedAt)
			fmt.Println("Locations:")
			for _, loc := range snapshot.Locations {
				fmt.Printf("  - %s\n", loc)
			}

			return nil
		},
	}
	return cmd
}

func mithrilSyncCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Download latest Mithril snapshot and load into database",
		Long: `Download the latest Mithril snapshot from the aggregator,
extract it, and load the ImmutableDB blocks into the local database.
This is the fastest way to bootstrap a new node.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				return errors.New("no config found in context")
			}

			logger := commonRun()

			network := cfg.Network
			if network == "" {
				network = "preview"
			}

			return runMithrilSync(cmd.Context(), cfg, logger, network)
		},
	}
	return cmd
}

func runMithrilSync(
	ctx context.Context,
	cfg *config.Config,
	logger *slog.Logger,
	network string,
) error {
	aggregatorURL := cfg.Mithril.AggregatorURL
	if aggregatorURL == "" {
		var err error
		aggregatorURL, err = mithril.AggregatorURLForNetwork(
			network,
		)
		if err != nil {
			return err
		}
	}

	// Default download directory to a deterministic path within
	// the database directory. This allows re-runs to find and
	// reuse previously downloaded/extracted snapshot data.
	downloadDir := cfg.Mithril.DownloadDir
	if downloadDir == "" && cfg.DatabasePath != "" {
		downloadDir = filepath.Join(
			cfg.DatabasePath, ".mithril-cache",
		)
	}

	result, err := mithril.Bootstrap(
		ctx,
		mithril.BootstrapConfig{
			Network:                network,
			AggregatorURL:          aggregatorURL,
			DownloadDir:            downloadDir,
			CleanupAfterLoad:       cfg.Mithril.CleanupAfterLoad,
			VerifyCertificateChain: cfg.Mithril.VerifyCertificates,
			Logger:                 logger,
			OnProgress: func(p mithril.DownloadProgress) {
				if p.TotalBytes > 0 {
					logger.Info(
						fmt.Sprintf(
							"download progress: %.1f%% (%s / %s) at %s/s",
							p.Percent,
							humanBytes(p.BytesDownloaded),
							humanBytes(p.TotalBytes),
							humanBytes(int64(p.BytesPerSecond)),
						),
						"component", "mithril",
					)
				}
			},
		},
	)
	if err != nil {
		return fmt.Errorf("mithril bootstrap failed: %w", err)
	}

	// Load Cardano node config for epoch parameter resolution
	cardanoConfigPath := cfg.CardanoConfig
	if cardanoConfigPath == "" {
		cardanoConfigPath = network + "/config.json"
	}
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardanoConfigPath,
		network,
		cardano.EmbeddedConfigPreviewNetworkFS,
	)
	if err != nil {
		return fmt.Errorf("loading cardano node config: %w", err)
	}

	// Open database once and reuse for both import and ImmutableDB load
	db, err := database.New(&database.Config{
		DataDir:        cfg.DatabasePath,
		Logger:         logger,
		BlobPlugin:     cfg.BlobPlugin,
		MetadataPlugin: cfg.MetadataPlugin,
		MaxConnections: cfg.DatabaseWorkers,
	})
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	// Import ledger state and copy blocks in parallel.
	// Ledger state goes to metadata (SQLite), blocks go to the blob
	// store (Badger) — completely independent data stores.
	var loadResult *node.LoadBlobsResult
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if err := importLedgerState(
			gctx, db, logger, nodeCfg, result,
		); err != nil {
			return fmt.Errorf("importing ledger state: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		logger.Info(
			"loading ImmutableDB blocks into blob store",
			"component", "mithril",
			"immutable_dir", result.ImmutableDir,
		)
		var loadErr error
		loadResult, loadErr = node.LoadBlobsWithDB(
			cfg, logger, result.ImmutableDir, db,
		)
		if loadErr != nil {
			return fmt.Errorf("loading ImmutableDB: %w", loadErr)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	// Update metadata tip to match the actual chain tip (last block
	// in the blob store). The ledger state tip may reference a block
	// beyond the ImmutableDB (in the volatile storage), so we need
	// to align the metadata tip with what's actually stored.
	recentBlocks, err := database.BlocksRecent(db, 1)
	if err != nil {
		return fmt.Errorf("reading chain tip: %w", err)
	}
	if len(recentBlocks) > 0 {
		chainTip := recentBlocks[0]
		if err := db.SetTip(
			ochainsync.Tip{
				Point: ocommon.Point{
					Slot: chainTip.Slot,
					Hash: chainTip.Hash,
				},
				BlockNumber: chainTip.Number,
			},
			nil,
		); err != nil {
			return fmt.Errorf("updating metadata tip: %w", err)
		}
		var blocksCopied int
		if loadResult != nil {
			blocksCopied = loadResult.BlocksCopied
		}
		logger.Info(
			"metadata tip updated to chain tip",
			"component", "mithril",
			"slot", chainTip.Slot,
			"blocks_loaded", blocksCopied,
		)
	}

	// Clean up temporary files after a successful complete load.
	if cfg.Mithril.CleanupAfterLoad {
		result.Cleanup(logger)
	}

	logger.Info(
		"Mithril bootstrap complete",
		"component", "mithril",
		"epoch", result.Snapshot.Beacon.Epoch,
		"immutable_file_number", result.Snapshot.Beacon.ImmutableFileNumber,
	)

	return nil
}

// humanBytes formats a byte count in a human-readable form.
func humanBytes(b int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case b >= gb:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// syncCommand creates the "sync" command with --mithril flag at the
// root level for convenience.
func syncCommand() *cobra.Command {
	var useMithril bool
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Synchronize node data",
		Long: `Synchronize the node database. Use --mithril to bootstrap
from a Mithril snapshot for fast initial sync.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !useMithril {
				fmt.Fprintln(
					os.Stderr,
					"Please specify a sync method. Use --mithril for Mithril snapshot sync.",
				)
				return cmd.Usage()
			}

			cfg := config.FromContext(cmd.Context())
			if cfg == nil {
				return errors.New("no config found in context")
			}

			logger := commonRun()

			network := cfg.Network
			if network == "" {
				network = "preview"
			}

			return runMithrilSync(cmd.Context(), cfg, logger, network)
		},
	}
	cmd.Flags().BoolVar(
		&useMithril,
		"mithril",
		false,
		"use Mithril snapshot for fast initial sync",
	)
	return cmd
}

// importLedgerState finds, parses, and imports the ledger state
// from the extracted Mithril snapshot. It searches both the main
// extract directory and the ancillary directory for the state file.
func importLedgerState(
	ctx context.Context,
	db *database.Database,
	logger *slog.Logger,
	nodeCfg *cardano.CardanoNodeConfig,
	result *mithril.BootstrapResult,
) error {
	// Search for ledger state: prefer ancillary dir, fall back to
	// main extract dir.
	searchDirs := []string{}
	if result.AncillaryDir != "" {
		searchDirs = append(searchDirs, result.AncillaryDir)
	}
	searchDirs = append(searchDirs, result.ExtractDir)

	var lstatePath string
	var searchDir string
	for _, dir := range searchDirs {
		path, findErr := ledgerstate.FindLedgerStateFile(dir)
		if findErr == nil {
			lstatePath = path
			searchDir = dir
			break
		}
		logger.Debug(
			"ledger state not found in directory",
			"component", "mithril",
			"dir", dir,
			"error", findErr,
		)
	}

	if lstatePath == "" {
		logger.Warn(
			"no ledger state file found in snapshot, "+
				"skipping ledger state import",
			"component", "mithril",
		)
		return nil
	}

	logger.Info(
		"found ledger state file",
		"component", "mithril",
		"path", lstatePath,
	)

	// Parse the snapshot
	state, err := ledgerstate.ParseSnapshot(lstatePath)
	if err != nil {
		return fmt.Errorf("parsing ledger state: %w", err)
	}

	// Check for UTxO-HD tvar file (UTxOs stored separately)
	tvarPath := ledgerstate.FindUTxOTableFile(searchDir)
	if tvarPath != "" {
		state.UTxOTablePath = tvarPath
		logger.Info(
			"found UTxO table file (UTxO-HD format)",
			"component", "mithril",
			"path", tvarPath,
		)
	}

	if state.Tip == nil {
		return errors.New(
			"parsed ledger state has no tip (Origin snapshot)",
		)
	}

	nonceHex := "neutral"
	if len(state.EpochNonce) > 0 {
		nonceHex = hex.EncodeToString(state.EpochNonce)
	}
	logger.Info(
		"parsed ledger state",
		"component", "mithril",
		"era", ledgerstate.EraName(state.EraIndex),
		"epoch", state.Epoch,
		"slot", state.Tip.Slot,
		"era_bound_slot", state.EraBoundSlot,
		"era_bound_epoch", state.EraBoundEpoch,
		"epoch_nonce", nonceHex,
	)

	// Build import key for resume tracking
	importKey := ""
	if result.Snapshot != nil && result.Snapshot.Digest != "" {
		digest := result.Snapshot.Digest
		if len(digest) > 16 {
			digest = digest[:16]
		}
		importKey = fmt.Sprintf(
			"%s:%d",
			digest,
			state.Tip.Slot,
		)
	}

	// Import the ledger state
	return ledgerstate.ImportLedgerState(
		ctx,
		ledgerstate.ImportConfig{
			Database:          db,
			State:             state,
			Logger:            logger,
			CardanoNodeConfig: nodeCfg,
			ImportKey:         importKey,
			OnProgress: func(p ledgerstate.ImportProgress) {
				logger.Info(
					p.Description,
					"component", "mithril",
					"stage", p.Stage,
				)
			},
		},
	)
}
