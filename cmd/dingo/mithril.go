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

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledgerstate"
	"github.com/blinklabs-io/dingo/mithril"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
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

	// Mark sync as in-progress so `dingo serve` can detect an
	// incomplete sync and refuse to start.
	if err := db.SetSyncState("sync_status", "in_progress", nil); err != nil {
		return fmt.Errorf("marking sync in-progress: %w", err)
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

	// Post-process ImmutableDB blocks to compute UTxO byte
	// offsets. The ImmutableDB load (copyBlocksRaw) only stores
	// block CBOR and header metadata. This step re-reads the
	// ImmutableDB files, extracts transaction output byte ranges,
	// and stores offset references in the blob store so that
	// UtxoByRef() can extract UTxO CBOR on-demand from blocks.
	logger.Info(
		"post-processing UTxO offsets from ImmutableDB",
		"component", "mithril",
	)
	if err := postProcessUtxoOffsets(
		ctx, db, logger, result.ImmutableDir,
	); err != nil {
		return fmt.Errorf(
			"post-processing UTxO offsets: %w", err,
		)
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

	// Clean up all sync_state entries.
	// These are ephemeral and only used by the sync process itself.
	// Set syncComplete after success so the deferred error message
	// fires if cleanup fails (the user needs to re-run or manually
	// clear sync state so `dingo serve` will start).
	if err := db.ClearSyncState(nil); err != nil {
		return fmt.Errorf("cleaning up sync state: %w", err)
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
				logger.Info(
					p.Description,
					"component", "mithril",
					"stage", p.Stage,
				)
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
	for _, block := range blocks {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("cancelled: %w", err)
		}
		parsedBlock, err := gledger.NewBlockFromCbor(
			block.Type, block.Cbor,
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
		if err := func() error {
			txn := db.Transaction(true)
			defer txn.Release()
			// Gap blocks are already reflected in the Mithril
			// snapshot's ledger state, so cert deposits are not
			// needed. Pass an empty (non-nil) map to avoid the
			// nil-certDeposits hard error in SetTransaction —
			// deposit-bearing certs will store deposit=0 which
			// is acceptable since the snapshot state is
			// authoritative.
			emptyDeposits := map[int]uint64{}
			for i, tx := range txs {
				if err := db.SetTransaction(
					tx, point, uint32(i), // #nosec G115 -- tx index within a block
					0, nil, emptyDeposits, offsets, txn,
				); err != nil {
					return fmt.Errorf(
						"storing TX: %w", err,
					)
				}
			}
			if err := txn.Commit(); err != nil {
				return fmt.Errorf(
					"commit transaction: %w", err,
				)
			}
			return nil
		}(); err != nil {
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

// postProcessUtxoOffsets iterates all ImmutableDB blocks and
// computes byte offset references for every transaction output.
// This enables UtxoByRef() to extract UTxO CBOR on-demand from
// the source block stored in the blob store.
//
// Fast path (Shelley+): uses ExtractTransactionOffsets for
// structural CBOR parsing without full block decode, then
// computes tx hashes via blake2b-256 on body CBOR.
//
// Fallback (Byron and other eras): parses the full block via
// NewBlockFromCbor and locates output CBOR via byte search.
func postProcessUtxoOffsets(
	ctx context.Context,
	db *database.Database,
	logger *slog.Logger,
	immutableDir string,
) error {
	blob := db.Blob()
	if blob == nil {
		return errors.New("blob store not available")
	}

	imm, err := immutable.New(immutableDir)
	if err != nil {
		return fmt.Errorf(
			"opening immutable DB: %w", err,
		)
	}

	iter, err := imm.BlocksFromPoint(ocommon.Point{})
	if err != nil {
		return fmt.Errorf(
			"getting block iterator: %w", err,
		)
	}
	defer iter.Close()

	const batchBlocks = 100
	var processedBlocks, totalUtxos int
	txn := db.BlobTxn(true)
	blocksInBatch := 0
	// Deferred rollback ensures the active transaction is cleaned
	// up if the loop body panics. Rollback after Commit is a no-op
	// in Badger, so this is safe on the normal path.
	defer func() { txn.Rollback() }() //nolint:errcheck

	for {
		if err := ctx.Err(); err != nil {
			txn.Rollback() //nolint:errcheck
			return fmt.Errorf("cancelled: %w", err)
		}
		next, err := iter.Next()
		if err != nil {
			txn.Rollback() //nolint:errcheck
			return fmt.Errorf("reading block: %w", err)
		}
		if next == nil {
			break
		}

		// Skip EBBs (epoch boundary blocks have no
		// transactions).
		if next.IsEbb {
			processedBlocks++
			continue
		}

		var blockHash [32]byte
		copy(blockHash[:], next.Hash)
		blobTxn := txn.Blob()

		// Try fast path: structural CBOR parsing without
		// full block decode (works for Shelley+ eras).
		offsets, extractErr := lcommon.ExtractTransactionOffsets(
			next.Cbor,
		)
		if extractErr == nil {
			for _, txLoc := range offsets.Transactions {
				bodyEnd := txLoc.Body.Offset +
					txLoc.Body.Length
				if int(bodyEnd) > len(next.Cbor) {
					txn.Rollback() //nolint:errcheck
					return fmt.Errorf(
						"block at slot %d: body "+
							"range [%d:%d] exceeds "+
							"block size %d",
						next.Slot,
						txLoc.Body.Offset,
						bodyEnd,
						len(next.Cbor),
					)
				}
				bodyBytes := next.Cbor[txLoc.Body.Offset:bodyEnd]
				txHash := lcommon.Blake2b256Hash(bodyBytes)

				for i, outLoc := range txLoc.Outputs {
					if outLoc.Length == 0 {
						continue
					}
					offset := database.CborOffset{
						BlockSlot:  next.Slot,
						BlockHash:  blockHash,
						ByteOffset: outLoc.Offset,
						ByteLength: outLoc.Length,
					}
					offsetData := database.EncodeUtxoOffset(
						&offset,
					)
					// #nosec G115
					if err := blob.SetUtxo(
						blobTxn,
						txHash[:],
						uint32(i),
						offsetData,
					); err != nil {
						txn.Rollback() //nolint:errcheck
						return fmt.Errorf(
							"storing UTxO offset: %w",
							err,
						)
					}
					totalUtxos++
				}
			}
		} else {
			// Fallback: full block parsing for Byron
			// and other eras where structural extraction
			// fails.
			parsedBlock, parseErr := gledger.NewBlockFromCbor(
				next.Type, next.Cbor,
			)
			if parseErr != nil {
				txn.Rollback() //nolint:errcheck
				return fmt.Errorf(
					"parsing block at slot %d: %w",
					next.Slot, parseErr,
				)
			}
			seenCbor := make(map[string]int)
			for _, tx := range parsedBlock.Transactions() {
				txHash := tx.Hash()
				var txHashArr [32]byte
				copy(txHashArr[:], txHash.Bytes())
				produced := tx.Produced()
				for _, utxo := range produced {
					outCbor := utxo.Output.Cbor()
					if len(outCbor) == 0 {
						txn.Rollback() //nolint:errcheck
						return fmt.Errorf(
							"block at slot %d: "+
								"output %d has "+
								"no CBOR",
							next.Slot,
							utxo.Id.Index(),
						)
					}
					// Search the raw block for the nth
					// occurrence of this output's CBOR.
					// NOTE: this searches all block
					// sections (header, witnesses, etc.),
					// not just outputs. A false match in
					// a non-output section would produce
					// a wrong offset. In practice this is
					// safe for Byron blocks because output
					// CBOR (unique base58 addresses +
					// amounts) is extremely unlikely to
					// appear verbatim elsewhere. Shelley+
					// blocks use the structural fast path.
					key := string(outCbor)
					nth := seenCbor[key]
					seenCbor[key]++
					pos := findNthOccurrence(
						next.Cbor, outCbor, nth,
					)
					if pos < 0 {
						txn.Rollback() //nolint:errcheck
						return fmt.Errorf(
							"block at slot %d: "+
								"output %d CBOR "+
								"not found",
							next.Slot,
							utxo.Id.Index(),
						)
					}
					offset := database.CborOffset{
						BlockSlot: next.Slot,
						BlockHash: blockHash,
						// #nosec G115
						ByteOffset: uint32(pos),
						// #nosec G115
						ByteLength: uint32(
							len(outCbor),
						),
					}
					offsetData := database.EncodeUtxoOffset(
						&offset,
					)
					if err := blob.SetUtxo(
						blobTxn,
						txHashArr[:],
						utxo.Id.Index(),
						offsetData,
					); err != nil {
						txn.Rollback() //nolint:errcheck
						return fmt.Errorf(
							"storing UTxO offset: %w",
							err,
						)
					}
					totalUtxos++
				}
			}
		}

		processedBlocks++
		blocksInBatch++

		if blocksInBatch >= batchBlocks {
			if err := txn.Commit(); err != nil {
				return fmt.Errorf(
					"committing UTxO offsets: %w",
					err,
				)
			}
			blocksInBatch = 0
			// Check for cancellation between batches, before
			// opening a new transaction that would leak.
			if err := ctx.Err(); err != nil {
				return fmt.Errorf("cancelled: %w", err)
			}
			txn = db.BlobTxn(true)
		}

		if processedBlocks > 0 && processedBlocks%50000 == 0 {
			logger.Info(
				"UTxO offset progress",
				"component", "mithril",
				"blocks", processedBlocks,
				"utxos", totalUtxos,
			)
		}
	}

	// Commit remaining batch; the deferred rollback handles
	// cleanup when there is nothing to commit.
	if blocksInBatch > 0 {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf(
				"committing final UTxO offsets: %w",
				err,
			)
		}
	}

	logger.Info(
		"UTxO offset post-processing complete",
		"component", "mithril",
		"blocks_processed", processedBlocks,
		"utxo_offsets_stored", totalUtxos,
	)

	return nil
}

// findNthOccurrence returns the byte offset of the nth (0-indexed)
// occurrence of target within data, or -1 if not found.
func findNthOccurrence(data, target []byte, n int) int {
	if len(target) == 0 || n < 0 {
		return -1
	}
	off := 0
	for range n {
		idx := bytes.Index(data[off:], target)
		if idx < 0 {
			return -1
		}
		off += idx + len(target)
	}
	idx := bytes.Index(data[off:], target)
	if idx < 0 {
		return -1
	}
	return off + idx
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
