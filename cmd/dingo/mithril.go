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
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	dingo "github.com/blinklabs-io/dingo"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/governance"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/dingo/mithril"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
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

// resolveAggregatorURL returns the Mithril aggregator URL from
// config, falling back to the network default.
func resolveAggregatorURL(
	cfgURL string,
	network string,
) (string, error) {
	if cfgURL != "" {
		return cfgURL, nil
	}
	url, err := mithril.AggregatorURLForNetwork(network)
	if err != nil {
		return "", fmt.Errorf(
			"resolving aggregator URL for network %s: %w",
			network, err,
		)
	}
	return url, nil
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

			aggregatorURL, err := resolveAggregatorURL(
				cfg.Mithril.AggregatorURL, network,
			)
			if err != nil {
				return err
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
					mithril.HumanBytes(s.Size),
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

			aggregatorURL, err := resolveAggregatorURL(
				cfg.Mithril.AggregatorURL, network,
			)
			if err != nil {
				return err
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
			fmt.Printf("Size:                  %s\n", mithril.HumanBytes(snapshot.Size))
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
		RunE: mithrilSyncRunE,
	}
	return cmd
}

// mithrilSyncRunE is the shared RunE for both the "mithril sync"
// subcommand and the "sync --mithril" convenience command.
func mithrilSyncRunE(
	cmd *cobra.Command,
	_ []string,
) error {
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
}

func runMithrilSync(
	ctx context.Context,
	cfg *config.Config,
	logger *slog.Logger,
	network string,
) error {
	cardanoConfigPath := cfg.CardanoConfig
	if cardanoConfigPath == "" {
		cardanoConfigPath = filepath.Join(network, "config.json")
	}
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		cardanoConfigPath,
		network,
		cardano.EmbeddedConfigFS,
	)
	if err != nil {
		return fmt.Errorf("loading cardano node config: %w", err)
	}

	aggregatorURL, err := resolveAggregatorURL(
		cfg.Mithril.AggregatorURL, network,
	)
	if err != nil {
		return err
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
			GenesisVerificationKey: nodeCfg.MithrilGenesisVerificationKey,
			AncillaryVerificationKey: nodeCfg.
				MithrilGenesisAncillaryVerificationKey,
			Logger: logger,
			OnProgress: func() func(mithril.DownloadProgress) {
				const progressLogInterval = 10 * time.Second
				const progressLogPercentStep = 5.0

				lastLogTime := time.Time{}
				lastLoggedPercent := -progressLogPercentStep

				return func(p mithril.DownloadProgress) {
					if p.TotalBytes <= 0 {
						return
					}
					now := time.Now()
					if !lastLogTime.IsZero() &&
						now.Sub(lastLogTime) < progressLogInterval &&
						p.Percent < 100 &&
						(p.Percent-lastLoggedPercent) < progressLogPercentStep {
						return
					}
					logger.Info(
						fmt.Sprintf(
							"download progress: %.1f%% (%s / %s) at %s/s",
							p.Percent,
							mithril.HumanBytes(p.BytesDownloaded),
							mithril.HumanBytes(p.TotalBytes),
							mithril.HumanBytes(int64(p.BytesPerSecond)),
						),
						"component", "mithril",
					)
					lastLogTime = now
					lastLoggedPercent = p.Percent
				}
			}(),
		},
	)
	if err != nil {
		return fmt.Errorf("mithril bootstrap failed: %w", err)
	}

	// Open database once and reuse for both import and ImmutableDB load
	db, err := database.New(&database.Config{
		DataDir:        cfg.DatabasePath,
		Logger:         logger,
		BlobPlugin:     cfg.BlobPlugin,
		RunMode:        string(cfg.RunMode),
		MetadataPlugin: cfg.MetadataPlugin,
		MaxConnections: cfg.DatabaseWorkers,
		StorageMode:    cfg.StorageMode,
	})
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	// Mark sync as in-progress so `dingo serve` can detect an
	// incomplete sync and refuse to start.
	if err := db.SetSyncState("sync_status", "in_progress", nil); err != nil {
		return fmt.Errorf("marking sync in-progress: %w", err)
	}

	// For API mode, mark an incomplete backfill checkpoint BEFORE any
	// blob writes. If the process dies between writing blocks and
	// node.Backfill.Run() creating its own initial checkpoint, the next
	// startup would see blocks but no checkpoint and skip the backfill.
	// Pre-seeding the checkpoint here ensures NeedsBackfill() returns
	// the correct answer at any interruption point.
	if dingo.StorageMode(cfg.StorageMode).IsAPI() {
		now := time.Now()
		if err := db.Metadata().SetBackfillCheckpoint(
			&models.BackfillCheckpoint{
				Phase:     node.BackfillPhase,
				StartedAt: now,
				UpdatedAt: now,
			},
			nil,
		); err != nil {
			return fmt.Errorf(
				"marking backfill in-progress: %w", err,
			)
		}
	}
	// On error, log a message guiding the user to re-run.
	syncComplete := false
	defer func() {
		if !syncComplete {
			logger.Error(
				"sync did not complete; "+
					"re-run 'dingo mithril sync' to resume",
				"component", "mithril",
			)
		}
	}()

	// Enable bulk-load optimizations before launching parallel
	// goroutines so both importLedgerState and LoadBlobsWithDB
	// share the same pragma settings without racing.
	defer node.WithBulkLoadPragmas(db, logger)()

	// Import ledger state and copy blocks in parallel.
	// Ledger state goes to metadata (SQLite), blocks go to the blob
	// store (Badger) — completely independent data stores.
	var loadResult *node.LoadBlobsResult
	var ledgerStateSlot uint64
	var ledgerStateHash []byte
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		slot, hash, importErr := importLedgerState(
			gctx, db, logger, nodeCfg, result,
		)
		if importErr != nil {
			return fmt.Errorf("importing ledger state: %w", importErr)
		}
		ledgerStateSlot = slot
		ledgerStateHash = hash
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
			gctx, cfg, logger, result.ImmutableDir, db,
		)
		if loadErr != nil {
			return fmt.Errorf("loading ImmutableDB: %w", loadErr)
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	// Fetch volatile blocks between the ImmutableDB tip and the
	// ledger state tip. The Mithril snapshot's UTxO set is at the
	// ledger state tip, but the ImmutableDB only has blocks up to
	// an earlier point. We must close this gap so the node has a
	// continuous chain matching the UTxO state.
	recentBlocks, err := database.BlocksRecent(db, 1)
	if err != nil {
		return fmt.Errorf("reading chain tip: %w", err)
	}
	immutableTipSlot := uint64(0)
	if loadResult != nil {
		immutableTipSlot = loadResult.ImmutableTipSlot
	}
	// When the snapshot's ledger state lands exactly on an immutable
	// boundary, there is no gap to fill — but stale volatile blocks
	// from a prior run can still sit above it. Drop them now so the
	// trust boundary below lands on ledgerStateSlot instead of the
	// stale block slot.
	if len(recentBlocks) > 0 &&
		recentBlocks[0].Slot > immutableTipSlot &&
		immutableTipSlot == ledgerStateSlot {
		logger.Info(
			"removing stale volatile blocks above ledger state tip",
			"component", "mithril",
			"stored_tip_slot", recentBlocks[0].Slot,
			"ledger_state_slot", ledgerStateSlot,
		)
		if cleanupErr := deleteBlobBlocksAboveSlot(
			db, immutableTipSlot,
		); cleanupErr != nil {
			return fmt.Errorf(
				"removing stale volatile blocks above slot %d: %w",
				immutableTipSlot, cleanupErr,
			)
		}
		recentBlocks, err = database.BlocksRecent(db, 1)
		if err != nil {
			return fmt.Errorf(
				"reading chain tip after stale volatile cleanup: %w",
				err,
			)
		}
	}
	if len(recentBlocks) > 0 &&
		recentBlocks[0].Slot > immutableTipSlot &&
		immutableTipSlot < ledgerStateSlot {
		resumeGapEnd := min(recentBlocks[0].Slot, ledgerStateSlot)
		logger.Info(
			"processing stored volatile gap blocks from blob store",
			"component", "mithril",
			"immutable_tip_slot", immutableTipSlot,
			"stored_tip_slot", recentBlocks[0].Slot,
			"resume_gap_end_slot", resumeGapEnd,
		)
		storedGapBlocks, err := loadGapBlocksFromBlob(
			db,
			immutableTipSlot+1,
			resumeGapEnd,
		)
		if err != nil {
			return fmt.Errorf(
				"loading stored gap blocks from blob store: %w",
				err,
			)
		}
		immutableTip, err := database.BlockBeforeSlot(
			db,
			immutableTipSlot+1,
		)
		if err != nil {
			return fmt.Errorf(
				"reading immutable tip for stored gap validation: %w",
				err,
			)
		}
		if immutableTip.Slot != immutableTipSlot {
			return fmt.Errorf(
				"reading immutable tip for stored gap validation: expected slot %d, got slot %d",
				immutableTipSlot,
				immutableTip.Slot,
			)
		}
		// Only enforce the terminal-hash check against ledgerStateHash
		// when the stored gap reaches the ledger state tip. A partial
		// stored gap is still useful: continuity-validate it and let
		// the volatile fetch path below pull the remainder.
		var validateErr error
		if resumeGapEnd == ledgerStateSlot {
			validateErr = validateStoredGapBlocks(
				storedGapBlocks,
				immutableTip,
				ledgerStateHash,
			)
		} else {
			validateErr = validateStoredGapContinuity(
				storedGapBlocks,
				immutableTip,
			)
		}
		if validateErr != nil {
			logger.Warn(
				"stored volatile gap blocks failed continuity check, refetching from relay",
				"component", "mithril",
				"immutable_tip_slot", immutableTipSlot,
				"resume_gap_end_slot", resumeGapEnd,
				"ledger_state_slot", ledgerStateSlot,
				"ledger_state_hash", hex.EncodeToString(ledgerStateHash),
				"error", validateErr,
			)
			// Drop the rejected blob blocks so neither the upcoming
			// BlocksRecent query nor any slot-ordered iterator can
			// resurface them as the chain tip after the relay refetch.
			if cleanupErr := deleteBlobBlocksAboveSlot(
				db, immutableTipSlot,
			); cleanupErr != nil {
				return fmt.Errorf(
					"removing rejected gap blocks above slot %d: %w",
					immutableTipSlot, cleanupErr,
				)
			}
			recentBlocks = []models.Block{immutableTip}
		} else {
			storedTipSlot := recentBlocks[0].Slot
			if storedTipSlot > resumeGapEnd {
				logger.Info(
					"removing stored volatile blocks beyond ledger state tip",
					"component", "mithril",
					"stored_tip_slot", storedTipSlot,
					"resume_gap_end_slot", resumeGapEnd,
				)
				if cleanupErr := deleteBlobBlocksAboveSlot(
					db, resumeGapEnd,
				); cleanupErr != nil {
					return fmt.Errorf(
						"removing stored gap blocks above slot %d: %w",
						resumeGapEnd,
						cleanupErr,
					)
				}
			}
			if resumeGapEnd < ledgerStateSlot {
				logger.Info(
					"accepted partial stored volatile gap; remainder will be fetched from relay",
					"component", "mithril",
					"immutable_tip_slot", immutableTipSlot,
					"resume_gap_end_slot", resumeGapEnd,
					"ledger_state_slot", ledgerStateSlot,
				)
			}
			if err := processGapBlocks(
				ctx,
				db,
				logger,
				storedGapBlocks,
			); err != nil {
				return fmt.Errorf(
					"processing stored gap block transactions: %w",
					err,
				)
			}
			recentBlocks, err = database.BlocksRecent(db, 1)
			if err != nil {
				return fmt.Errorf(
					"reading chain tip after gap resume: %w",
					err,
				)
			}
		}
	}
	if len(recentBlocks) > 0 && ledgerStateSlot > recentBlocks[0].Slot {
		immutableTip := recentBlocks[0]
		logger.Info(
			"fetching volatile blocks to close gap",
			"component", "mithril",
			"immutable_tip_slot", immutableTip.Slot,
			"ledger_state_slot", ledgerStateSlot,
		)
		gapBlocks, fetchErr := fetchGapBlocks(
			ctx, logger, network,
			ocommon.NewPoint(immutableTip.Slot, immutableTip.Hash),
			ocommon.NewPoint(ledgerStateSlot, ledgerStateHash),
		)
		if fetchErr != nil {
			return fmt.Errorf("fetching volatile blocks: %w", fetchErr)
		}
		// Store gap blocks to the blob store, then index
		// their metadata. These two steps are not atomic:
		// if processGapBlocks fails, re-running the sync
		// will re-fetch and re-store the blocks. Both
		// BlockCreate and SetTransaction use idempotent
		// writes, so re-processing is safe.
		for _, block := range gapBlocks {
			if err := db.BlockCreate(block, nil); err != nil {
				return fmt.Errorf("storing volatile block: %w", err)
			}
		}
		if err := processGapBlocks(ctx, db, logger, gapBlocks); err != nil {
			return fmt.Errorf("processing gap block transactions: %w", err)
		}
		logger.Info(
			"volatile blocks stored",
			"component", "mithril",
			"count", len(gapBlocks),
		)
	}

	// Backfill historical metadata if storage mode is API.
	// This replays all stored blocks to populate transaction
	// records needed for API queries (Blockfrost, UTxO RPC).
	if dingo.StorageMode(cfg.StorageMode).IsAPI() {
		logger.Info(
			"backfilling historical metadata for API mode",
			"component", "mithril",
		)
		bf := node.NewBackfill(db, nodeCfg, logger)
		if err := bf.Run(ctx); err != nil {
			return fmt.Errorf("backfill: %w", err)
		}
	}

	// Set the metadata tip to the latest block in the blob store.
	// After gap blocks are stored, this should match the ledger
	// state tip, giving the node a consistent starting point.
	recentBlocks, err = database.BlocksRecent(db, 1)
	if err != nil {
		return fmt.Errorf("reading final chain tip: %w", err)
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
			"metadata tip set",
			"component", "mithril",
			"slot", chainTip.Slot,
			"blocks_loaded", blocksCopied,
		)
	}

	// Record the trust boundary as the chain tip AFTER gap closure,
	// not the Mithril snapshot slot. Gap blocks between the snapshot
	// slot and chain tip were imported via SetGapBlockTransaction
	// (no UTxO tracking), so chainsync replay must skip them too.
	// Fall back to the snapshot slot when no gap blocks were fetched.
	trustBoundarySlot := ledgerStateSlot
	if len(recentBlocks) > 0 {
		trustBoundarySlot = recentBlocks[0].Slot
	}

	// Clean up ephemeral sync state and record the Mithril trust
	// boundary atomically. Both must succeed together: if cleanup
	// succeeds but the boundary write fails, dingo serve would
	// start without the trust boundary and hit the replay bug.
	txn := db.MetadataTxn(true)
	if err := txn.Do(func(txn *database.Txn) error {
		if err := db.ClearSyncState(txn); err != nil {
			return fmt.Errorf("cleaning up sync state: %w", err)
		}
		if err := db.SetSyncState(
			"mithril_ledger_slot",
			strconv.FormatUint(trustBoundarySlot, 10),
			txn,
		); err != nil {
			return fmt.Errorf(
				"recording mithril ledger slot: %w", err,
			)
		}
		return nil
	}); err != nil {
		return err
	}
	syncComplete = true

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
					"Please specify a sync method."+
						" Use --mithril for"+
						" Mithril snapshot sync.",
				)
				_ = cmd.Usage()
				return errors.New("no sync method specified")
			}
			return mithrilSyncRunE(cmd, args)
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
) (ledgerStateSlot uint64, ledgerStateHash []byte, err error) {
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
		return 0, nil, nil
	}

	logger.Info(
		"found ledger state file",
		"component", "mithril",
		"path", lstatePath,
	)

	// Parse the snapshot
	state, err := ledgerstate.ParseSnapshot(lstatePath)
	if err != nil {
		return 0, nil, fmt.Errorf("parsing ledger state: %w", err)
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
		return 0, nil, errors.New(
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
	if err := ledgerstate.ImportLedgerState(
		ctx,
		ledgerstate.ImportConfig{
			Database:  db,
			State:     state,
			Logger:    logger,
			ImportKey: importKey,
			EpochLength: epochLengthFromConfig(
				nodeCfg,
			),
			OnProgress: func(p ledgerstate.ImportProgress) {
				attrs := []any{
					"component", "mithril",
					"stage", p.Stage,
				}
				msg := p.Description
				var pct float64
				switch {
				case p.Percent > 0:
					pct = p.Percent
				case p.Total > 0:
					pct = float64(p.Current) /
						float64(p.Total) * 100
				}
				if pct > 0 {
					attrs = append(
						attrs,
						"progress",
						fmt.Sprintf("%.1f%%", pct),
					)
				}
				if p.Total > 0 {
					attrs = append(
						attrs,
						"current", p.Current,
						"total", p.Total,
					)
				} else if p.Current > 0 {
					attrs = append(
						attrs, "current", p.Current,
					)
				}
				logger.Info(msg, attrs...)
			},
		},
	); err != nil {
		return 0, nil, fmt.Errorf("importing ledger state: %w", err)
	}
	return state.Tip.Slot, state.Tip.BlockHash, nil
}

// fetchGapBlocks connects to network relay peers and fetches blocks
// between start and end (inclusive). The start block is skipped since
// it already exists in the blob store. This closes the gap between
// the ImmutableDB tip and the ledger state tip after Mithril bootstrap.
// It tries each bootstrap peer in order, falling back to the next on
// failure.
func fetchGapBlocks(
	ctx context.Context,
	logger *slog.Logger,
	network string,
	start ocommon.Point,
	end ocommon.Point,
) ([]models.Block, error) {
	// Resolve network info for magic and bootstrap peers
	netInfo, ok := ouroboros.NetworkByName(network)
	if !ok {
		return nil, fmt.Errorf("unknown network: %s", network)
	}
	if len(netInfo.BootstrapPeers) == 0 {
		return nil, fmt.Errorf(
			"no bootstrap peers for network %s", network,
		)
	}

	var lastErr error
	for i, peer := range netInfo.BootstrapPeers {
		peerAddr := net.JoinHostPort(
			peer.Address,
			strconv.FormatUint(uint64(peer.Port), 10),
		)

		blocks, err := fetchGapBlocksFromPeer(
			ctx, logger, netInfo.NetworkMagic,
			peerAddr, start, end,
		)
		if err == nil {
			return blocks, nil
		}

		lastErr = err
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		logger.Warn(
			"failed to fetch gap blocks from peer, "+
				"trying next",
			"component", "mithril",
			"peer", peerAddr,
			"peer_index", i,
			"total_peers", len(netInfo.BootstrapPeers),
			"error", err,
		)
	}

	return nil, fmt.Errorf(
		"all %d bootstrap peers failed: %w",
		len(netInfo.BootstrapPeers),
		lastErr,
	)
}

// fetchGapBlocksFromPeer fetches gap blocks from a single peer.
func fetchGapBlocksFromPeer(
	ctx context.Context,
	logger *slog.Logger,
	networkMagic uint32,
	peerAddr string,
	start ocommon.Point,
	end ocommon.Point,
) ([]models.Block, error) {
	logger.Info(
		"connecting to relay for volatile blocks",
		"component", "mithril",
		"peer", peerAddr,
	)

	// Collect blocks via callbacks. The start block is skipped
	// because it already exists in the blob store.
	var mu sync.Mutex
	var blocks []models.Block
	done := make(chan struct{}, 1)

	blockFunc := func(
		_ blockfetch.CallbackContext,
		blockType uint,
		block gledger.Block,
	) error {
		if block.SlotNumber() <= start.Slot {
			return nil
		}
		b := models.Block{
			Slot:     block.SlotNumber(),
			Hash:     block.Hash().Bytes(),
			Cbor:     block.Cbor(),
			Number:   block.BlockNumber(),
			Type:     blockType,
			PrevHash: block.PrevHash().Bytes(),
		}
		mu.Lock()
		blocks = append(blocks, b)
		mu.Unlock()
		return nil
	}

	batchDoneFunc := func(_ blockfetch.CallbackContext) error {
		select {
		case done <- struct{}{}:
		default:
		}
		return nil
	}

	// Establish TCP connection to the relay peer
	dialer := net.Dialer{Timeout: 30 * time.Second}
	tcpConn, err := dialer.DialContext(ctx, "tcp", peerAddr)
	if err != nil {
		return nil, fmt.Errorf(
			"connecting to peer %s: %w", peerAddr, err,
		)
	}

	// Create ouroboros connection with blockfetch protocol
	oConn, err := ouroboros.NewConnection(
		ouroboros.WithConnection(tcpConn),
		ouroboros.WithNetworkMagic(networkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(true),
		ouroboros.WithBlockFetchConfig(
			blockfetch.NewConfig(
				blockfetch.WithBlockFunc(blockFunc),
				blockfetch.WithBatchDoneFunc(batchDoneFunc),
				blockfetch.WithBatchStartTimeout(
					30*time.Second,
				),
				blockfetch.WithBlockTimeout(60*time.Second),
			),
		),
	)
	if err != nil {
		tcpConn.Close()
		return nil, fmt.Errorf(
			"creating ouroboros connection: %w", err,
		)
	}
	defer oConn.Close()

	// Request block range [start, end] inclusive
	if err := oConn.BlockFetch().Client.GetBlockRange(
		start, end,
	); err != nil {
		return nil, fmt.Errorf(
			"block range request failed: %w", err,
		)
	}

	// Wait for batch completion with a safety timeout
	// in case the peer disconnects without triggering
	// batchDoneFunc (e.g. protocol error, peer doesn't
	// have the range). Also monitor the ouroboros
	// connection's error channel for immediate failure
	// detection on peer disconnect or protocol error.
	timer := time.NewTimer(5 * time.Minute)
	defer timer.Stop()
	select {
	case <-done:
		// All blocks received
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-oConn.ErrorChan():
		// The peer may close the connection right after
		// sending the BatchDone message. The connection
		// monitor can fire before the protocol handler
		// processes BatchDone. Wait briefly for
		// batchDoneFunc to complete before treating the
		// error as genuine.
		graceTimer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-done:
			graceTimer.Stop()
		case <-graceTimer.C:
			return nil, fmt.Errorf(
				"ouroboros connection error during "+
					"block fetch: %w",
				err,
			)
		}
	case <-timer.C:
		return nil, errors.New(
			"timed out waiting for gap blocks from relay",
		)
	}

	logger.Info(
		"fetched volatile blocks from relay",
		"component", "mithril",
		"count", len(blocks),
		"peer", peerAddr,
	)

	return blocks, nil
}

// deleteBlobBlocksAboveSlot removes every block from the blob store
// whose slot is greater than the given threshold AND drops any
// metadata indexed for those blocks (transactions, UTxOs, governance
// proposals/votes), and restores any UTxOs that the rejected gap
// blocks had marked spent. Used to clean up rejected gap blocks from
// a previous failed Mithril resume so the next BlocksRecent query and
// any slot-ordered iterator (chainsync replay, etc.) cannot resurface
// them, and so leftover metadata from the discarded fork cannot
// shadow the refetched chain.
func deleteBlobBlocksAboveSlot(
	db *database.Database,
	slot uint64,
) error {
	if slot == ^uint64(0) {
		return nil
	}
	iter := db.BlocksInRange(slot+1, ^uint64(0))
	var stale []ocommon.Point
	for {
		next, err := iter.NextRaw()
		if err != nil {
			iter.Close()
			return fmt.Errorf(
				"iterating stale gap blocks above slot %d: %w",
				slot, err,
			)
		}
		if next == nil {
			break
		}
		stale = append(stale, ocommon.Point{
			Slot: next.Slot,
			Hash: append([]byte(nil), next.Hash...),
		})
	}
	iter.Close()
	if len(stale) == 0 {
		return nil
	}
	txn := db.Transaction(true)
	return txn.Do(func(txn *database.Txn) error {
		for _, p := range stale {
			block, err := database.BlockByPointTxn(txn, p)
			if err != nil {
				if errors.Is(err, models.ErrBlockNotFound) {
					continue
				}
				return fmt.Errorf(
					"lookup stale gap block at slot %d: %w",
					p.Slot, err,
				)
			}
			if err := database.BlockDeleteTxn(txn, block); err != nil {
				return fmt.Errorf(
					"delete stale gap block at slot %d: %w",
					p.Slot, err,
				)
			}
		}
		// Drop metadata indexed for the rejected blocks. Without this
		// the refetched chain would inherit stale UTxO consumption,
		// duplicate tx records, and orphan governance proposals/votes
		// from the previous run's gap-block processing.
		if err := db.UtxosDeleteRolledback(slot, txn); err != nil {
			return fmt.Errorf(
				"delete rolled-back UTxOs above slot %d: %w",
				slot, err,
			)
		}
		if err := db.TransactionsDeleteRolledback(slot, txn); err != nil {
			return fmt.Errorf(
				"delete rolled-back transactions above slot %d: %w",
				slot, err,
			)
		}
		if err := db.UtxosUnspend(slot, txn); err != nil {
			return fmt.Errorf(
				"restore UTxOs spent above slot %d: %w",
				slot, err,
			)
		}
		if err := db.DeleteGovernanceVotesAfterSlot(
			slot, txn,
		); err != nil {
			return fmt.Errorf(
				"delete rolled-back governance votes above slot %d: %w",
				slot, err,
			)
		}
		if err := db.DeleteGovernanceProposalsAfterSlot(
			slot, txn,
		); err != nil {
			return fmt.Errorf(
				"delete rolled-back governance proposals above slot %d: %w",
				slot, err,
			)
		}
		return nil
	})
}

func loadGapBlocksFromBlob(
	db *database.Database,
	startSlot uint64,
	endSlot uint64,
) ([]models.Block, error) {
	if startSlot > endSlot {
		return nil, nil
	}
	iter := db.BlocksInRange(startSlot, endSlot)
	defer iter.Close()
	var ret []models.Block
	for {
		next, err := iter.NextRaw()
		if err != nil {
			return nil, fmt.Errorf(
				"iterate blob blocks in range [%d,%d]: %w",
				startSlot,
				endSlot,
				err,
			)
		}
		if next == nil {
			break
		}
		ret = append(ret, models.Block{
			Slot:     next.Slot,
			Hash:     append([]byte(nil), next.Hash...),
			PrevHash: append([]byte(nil), next.PrevHash...),
			Cbor:     append([]byte(nil), next.Cbor...),
			Number:   next.Height,
			Type:     next.BlockType,
		})
	}
	return ret, nil
}

// validateStoredGapContinuity verifies the stored gap is non-empty,
// chains from immutableTip, and is internally hash-linked. It does not
// check the terminal block hash, so it is safe to use for partial gaps
// that don't yet reach the ledger state tip.
func validateStoredGapContinuity(
	blocks []models.Block,
	immutableTip models.Block,
) error {
	if len(blocks) == 0 {
		return fmt.Errorf(
			"stored volatile gap is empty after immutable tip slot %d",
			immutableTip.Slot,
		)
	}
	if !bytes.Equal(blocks[0].PrevHash, immutableTip.Hash) {
		return fmt.Errorf(
			"stored gap first block at slot %d prev hash %x does not match immutable tip slot %d hash %x",
			blocks[0].Slot,
			blocks[0].PrevHash,
			immutableTip.Slot,
			immutableTip.Hash,
		)
	}
	for i := 1; i < len(blocks); i++ {
		if bytes.Equal(blocks[i].PrevHash, blocks[i-1].Hash) {
			continue
		}
		return fmt.Errorf(
			"stored gap block at slot %d prev hash %x does not match previous block slot %d hash %x",
			blocks[i].Slot,
			blocks[i].PrevHash,
			blocks[i-1].Slot,
			blocks[i-1].Hash,
		)
	}
	return nil
}

func validateStoredGapBlocks(
	blocks []models.Block,
	immutableTip models.Block,
	ledgerStateHash []byte,
) error {
	if err := validateStoredGapContinuity(blocks, immutableTip); err != nil {
		return err
	}
	last := blocks[len(blocks)-1]
	if !bytes.Equal(last.Hash, ledgerStateHash) {
		return fmt.Errorf(
			"stored gap terminal block at slot %d hash %x does not match ledger state hash %x",
			last.Slot,
			last.Hash,
			ledgerStateHash,
		)
	}
	return nil
}

// processGapBlocks computes byte offsets and stores transaction
// metadata for gap blocks that were fetched from a relay peer.
// BlockCreate only writes raw CBOR to the blob store; this function
// adds the offset data and metadata records needed for UTxO resolution
// and transaction lookups.
func processGapBlocks(
	ctx context.Context,
	db *database.Database,
	logger *slog.Logger,
	blocks []models.Block,
) error {
	if len(blocks) == 0 {
		return nil
	}
	epochs, err := db.GetEpochs(nil)
	if err != nil {
		return fmt.Errorf("loading epochs for gap blocks: %w", err)
	}
	conwayPParamsCache := make(map[uint64]*conway.ConwayProtocolParameters)
	for _, block := range blocks {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("cancelled: %w", err)
		}
		// Gap blocks were already parsed and body-hash validated by the
		// blockfetch client when fetched from the relay. We only need a
		// second decode here to extract transactions and offsets.
		parsedBlock, err := gledger.NewBlockFromCbor(
			block.Type,
			block.Cbor,
			lcommon.VerifyConfig{SkipBodyHashValidation: true},
		)
		if err != nil {
			return fmt.Errorf(
				"parsing gap block at slot %d: %w",
				block.Slot, err,
			)
		}
		txs := parsedBlock.Transactions()
		if len(txs) == 0 {
			continue
		}
		indexer := database.NewBlockIndexer(
			block.Slot, block.Hash,
		)
		offsets, err := indexer.ComputeOffsets(
			block.Cbor, parsedBlock,
		)
		if err != nil {
			return fmt.Errorf(
				"computing offsets for gap block at slot %d: %w",
				block.Slot, err,
			)
		}
		point := ocommon.NewPoint(block.Slot, block.Hash)
		epoch, err := gapBlockEpoch(epochs, block.Slot)
		if err != nil {
			return fmt.Errorf(
				"resolving epoch for gap block at slot %d: %w",
				block.Slot,
				err,
			)
		}
		blockConwayPParams := (*conway.ConwayProtocolParameters)(nil)
		if epoch.EraId == conway.EraIdConway {
			cached, ok := conwayPParamsCache[epoch.EpochId]
			if !ok {
				pparams, err := db.GetPParams(
					epoch.EpochId,
					eras.ConwayEraDesc.DecodePParamsFunc,
					nil,
				)
				if err != nil {
					return fmt.Errorf(
						"loading protocol parameters for gap block at slot %d (epoch %d): %w",
						block.Slot,
						epoch.EpochId,
						err,
					)
				}
				if pparams != nil {
					cached, ok = pparams.(*conway.ConwayProtocolParameters)
					if !ok {
						return fmt.Errorf(
							"unexpected protocol params %T for gap block at slot %d (epoch %d)",
							pparams,
							block.Slot,
							epoch.EpochId,
						)
					}
				}
				conwayPParamsCache[epoch.EpochId] = cached
			}
			blockConwayPParams = cached
		}
		if err := processGapBlockTransactions(
			db,
			point,
			txs,
			offsets,
			epoch.EpochId,
			blockConwayPParams,
		); err != nil {
			return fmt.Errorf(
				"processing gap block at slot %d: %w",
				block.Slot, err,
			)
		}
		logger.Debug(
			"processed gap block transactions",
			"component", "mithril",
			"slot", block.Slot,
			"tx_count", len(txs),
		)
	}
	return nil
}

func processGapBlockTransactions(
	db *database.Database,
	point ocommon.Point,
	txs []lcommon.Transaction,
	offsets *database.BlockIngestionResult,
	epochId uint64,
	conwayPParams *conway.ConwayProtocolParameters,
) error {
	txn := db.Transaction(true)
	defer txn.Release()
	for i, tx := range txs {
		// Gap blocks are already reflected in the Mithril snapshot's
		// UTxO set, so input UTxOs are already consumed. Store the TX
		// record and blob offsets without re-consuming inputs.
		if err := db.SetGapBlockTransaction(
			tx,
			point,
			uint32(i), // #nosec G115 -- tx index within a block
			offsets,
			txn,
		); err != nil {
			return fmt.Errorf("storing TX: %w", err)
		}
		if !tx.IsValid() {
			continue
		}
		hasGovernance := len(tx.ProposalProcedures()) > 0 ||
			len(tx.VotingProcedures()) > 0
		if !hasGovernance {
			continue
		}
		if conwayPParams == nil {
			return errors.New(
				"missing Conway protocol parameters for governance gap block processing",
			)
		}
		if err := governance.ProcessProposals(
			tx,
			point,
			epochId,
			conwayPParams.GovActionValidityPeriod,
			db,
			txn,
		); err != nil {
			return fmt.Errorf(
				"processing governance proposals: %w",
				err,
			)
		}
		if err := governance.ProcessVotes(
			tx,
			point,
			epochId,
			conwayPParams.DRepInactivityPeriod,
			db,
			txn,
		); err != nil {
			return fmt.Errorf(
				"processing governance votes: %w",
				err,
			)
		}
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

// gapBlockEpoch resolves the epoch containing slot from a slice of
// epochs ordered by ascending StartSlot (as returned by db.GetEpochs).
// It enforces the upper bound StartSlot + LengthInSlots so a slot past
// the last known epoch surfaces an error rather than silently binding
// to the final entry.
func gapBlockEpoch(
	epochs []models.Epoch,
	slot uint64,
) (models.Epoch, error) {
	for i := len(epochs) - 1; i >= 0; i-- {
		if slot < epochs[i].StartSlot {
			continue
		}
		end := epochs[i].StartSlot + uint64(epochs[i].LengthInSlots)
		if epochs[i].LengthInSlots > 0 && slot >= end {
			return models.Epoch{}, fmt.Errorf(
				"slot %d is past the end of the last known epoch %d (slots %d..%d)",
				slot,
				epochs[i].EpochId,
				epochs[i].StartSlot,
				end,
			)
		}
		return epochs[i], nil
	}
	return models.Epoch{}, fmt.Errorf("no epoch found for slot %d", slot)
}

// epochLengthFromConfig returns an EpochLengthFunc that resolves
// era parameters from the Cardano node config.
func epochLengthFromConfig(
	nodeCfg *cardano.CardanoNodeConfig,
) ledgerstate.EpochLengthFunc {
	if nodeCfg == nil {
		return nil
	}
	return func(eraId uint) (uint, uint, error) {
		eraDesc := eras.GetEraById(eraId)
		if eraDesc == nil || eraDesc.EpochLengthFunc == nil {
			return 0, 0, fmt.Errorf(
				"unknown era %d", eraId,
			)
		}
		return eraDesc.EpochLengthFunc(nodeCfg)
	}
}
