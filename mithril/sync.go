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

package mithril

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/blinklabs-io/dingo/ledgerstate"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"golang.org/x/sync/errgroup"
)

// Sync-status values stored under the "sync_status" key. A complete sync clears
// the key; "backfill" means servable-but-still-backfilling; "in_progress" means
// an incomplete sync that must be re-run before serving.
const (
	syncStatusInProgress = "in_progress"
	syncStatusBackfill   = "backfill"
)

// SyncPhase identifies a stage of a Mithril bootstrap.
type SyncPhase string

const (
	PhaseBootstrap     SyncPhase = "bootstrap"
	PhaseLedgerImport  SyncPhase = "ledger_import"
	PhaseImmutableCopy SyncPhase = "immutable_copy"
	PhaseGapBlocks     SyncPhase = "gap_blocks"
	PhasePostLedger    SyncPhase = "post_ledger_state"
	PhaseBackfill      SyncPhase = "backfill"
	PhaseIndexRebuild  SyncPhase = "index_rebuild"
	PhaseComplete      SyncPhase = "complete"
)

// SyncProgress is a flat, dependency-free progress report emitted during Sync.
// It deliberately exposes no internal/node or ledgerstate types so external
// embedders can consume it without importing dingo internals.
type SyncProgress struct {
	Phase           SyncPhase
	Active          bool    // true while the phase is active (begin + every mid-phase tick); false only marks the phase end
	BytesDownloaded int64   // download phase
	TotalBytes      int64   // download phase
	BytesPerSecond  float64 // download/copy/backfill rate
	CurrentSlot     uint64
	TipSlot         uint64
	Percent         float64
	Count           int    // generic counter (gap blocks, blocks copied)
	Total           int    // Total complements Count for non-byte progress (e.g. ledger-import items).
	Description     string // free-form (era/stage label)
}

// SyncProgressFunc receives progress updates. It must be fast and non-blocking.
type SyncProgressFunc func(SyncProgress)

// SyncConfig is the input to a full Mithril bootstrap of a node database.
type SyncConfig struct {
	Network                string                     // "mainnet" | "preprod" | "preview"
	DataDir                string                     // node database path
	StorageMode            string                     // "api" | "core"
	CardanoNodeConfig      *cardano.CardanoNodeConfig // genesis + Mithril verification keys; if nil, loaded from EmbeddedConfigFS for Network
	CardanoConfigPath      string                     // optional explicit config.json path (else "<network>/config.json")
	Backend                string                     // Mithril artifact backend; same semantics as BootstrapConfig.Backend (empty selects v2)
	AggregatorURL          string                     // optional; defaults per-network
	DownloadDir            string                     // optional; defaults to <DataDir>/.mithril-cache
	DownloadIdleTimeout    string                     // optional; passed to BootstrapConfig
	DownloadMaxIdleRetries int                        // must be >= 0
	VerifyCertChain        bool
	CleanupAfterLoad       bool
	BlobPlugin             string // database plugin selection (match the node's)
	MetadataPlugin         string
	RunMode                string
	BackfillBatchSize      int
	DatabaseWorkers        int
	Logger                 *slog.Logger     // optional; defaults to slog.Default()
	OnProgress             SyncProgressFunc // optional
}

// SyncResult summarises a completed bootstrap.
type SyncResult struct {
	Snapshot   *SnapshotListItem
	LedgerSlot uint64
}

// emit calls cfg.OnProgress if set.
func (cfg SyncConfig) emit(p SyncProgress) {
	if cfg.OnProgress != nil {
		cfg.OnProgress(p)
	}
}

// isAPIMode returns true when storageMode is "api" (case-insensitive).
func isAPIMode(m string) bool {
	return strings.EqualFold(m, "api")
}

// pctOf returns cur/total*100, or 0 if total <= 0.
func pctOf(cur, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return float64(cur) / float64(total) * 100
}

// markSyncInProgress marks the database as an incomplete sync (so `dingo serve`
// refuses to start) and, for API mode, pre-seeds the backfill checkpoint. It is
// called at the first actual write rather than before bootstrap, so a bootstrap
// that fails before writing anything leaves an existing healthy database
// untouched. Idempotent: safe to call more than once per sync.
func markSyncInProgress(db *database.Database, storageMode string) error {
	if err := db.SetSyncState(
		"sync_status", syncStatusInProgress, nil,
	); err != nil {
		return fmt.Errorf("marking sync in-progress: %w", err)
	}
	if isAPIMode(storageMode) {
		if err := ensureMithrilBackfillCheckpoint(db); err != nil {
			return fmt.Errorf("marking backfill in-progress: %w", err)
		}
	}
	return nil
}

// Sync performs a full Mithril bootstrap of the database at cfg.DataDir:
// download + verify + extract a snapshot, import ledger state and immutable
// blocks, close the volatile gap, backfill metadata, and mark the sync
// complete. The resulting database is servable by dingo.Node.Run.
func Sync(ctx context.Context, cfg SyncConfig) (SyncResult, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	cfg.Backend = normalizeBackend(cfg.Backend)

	network := cfg.Network

	// Load or reuse Cardano node config.
	nodeCfg := cfg.CardanoNodeConfig
	if nodeCfg == nil {
		cardanoConfigPath := cfg.CardanoConfigPath
		if cardanoConfigPath == "" {
			cardanoConfigPath = filepath.Join(network, "config.json")
		}
		var err error
		nodeCfg, err = cardano.LoadCardanoNodeConfigWithFallback(
			cardanoConfigPath,
			network,
			cardano.EmbeddedConfigFS,
		)
		if err != nil {
			return SyncResult{}, fmt.Errorf("loading cardano node config: %w", err)
		}
	}

	aggregatorURL := cfg.AggregatorURL
	if aggregatorURL == "" {
		var err error
		aggregatorURL, err = AggregatorURLForNetwork(network)
		if err != nil {
			return SyncResult{}, fmt.Errorf(
				"resolving aggregator URL for network %s: %w",
				network, err,
			)
		}
	}

	// Default download directory to a deterministic path within
	// the database directory. This allows re-runs to find and
	// reuse previously downloaded/extracted snapshot data.
	downloadDir := cfg.DownloadDir
	if downloadDir == "" && cfg.DataDir != "" {
		downloadDir = filepath.Join(
			cfg.DataDir, ".mithril-cache",
		)
	}

	downloadIdleTimeout, err := parseOptionalDuration(
		"Mithril download idle timeout",
		cfg.DownloadIdleTimeout,
	)
	if err != nil {
		return SyncResult{}, err
	}
	if cfg.DownloadMaxIdleRetries < 0 {
		return SyncResult{}, fmt.Errorf(
			"invalid Mithril download max idle retries %d: must be >= 0",
			cfg.DownloadMaxIdleRetries,
		)
	}

	// Open the database before bootstrap so the immutable copy can overlap the
	// download: chunks are copied into the blob store in contiguous order as
	// they finish downloading, instead of waiting for the whole download.
	db, err := database.New(&database.Config{
		DataDir:        cfg.DataDir,
		Logger:         logger,
		BlobPlugin:     cfg.BlobPlugin,
		RunMode:        cfg.RunMode,
		MetadataPlugin: cfg.MetadataPlugin,
		MaxConnections: cfg.DatabaseWorkers,
		StorageMode:    cfg.StorageMode,
		Network:        cfg.Network,
	})
	if err != nil {
		// Tolerate a recoverable commit-timestamp mismatch from a previously
		// interrupted run; mithril import heals it on forward progress.
		var cte database.CommitTimestampError
		if errors.As(err, &cte) && db != nil {
			logger.Warn(
				"opened database with commit timestamp mismatch; "+
					"continuing mithril sync — import will heal it",
				"component", "mithril",
				"metadata_timestamp", cte.MetadataTimestamp,
				"blob_timestamp", cte.BlobTimestamp,
			)
		} else {
			return SyncResult{}, fmt.Errorf("opening database: %w", err)
		}
	}
	defer db.Close()

	// The sync-in-progress marker and the API backfill checkpoint are NOT set
	// here: setting them before bootstrap would leave an existing healthy
	// database permanently marked incomplete (blocking `dingo serve`) if the
	// bootstrap fails before anything is written. They are set by
	// markSyncInProgress at the first actual write instead (the pipelined copy
	// below, and again before the post-bootstrap import as a backstop).

	// Pipelined copy consumer. Bootstrap invokes onChunkContiguous for each
	// immutable file number in contiguous order as it finishes downloading; we
	// copy that contiguous prefix into the blob store while later chunks are
	// still downloading. The sequencer drives this from a single in-order
	// consumer goroutine, so the closure needs no locking.
	copyCm, err := chain.NewManager(db, nil)
	if err != nil {
		return SyncResult{}, fmt.Errorf("loading chain manager: %w", err)
	}
	copyChain := copyCm.PrimaryChain()
	if copyChain == nil {
		return SyncResult{}, errors.New("primary chain not available")
	}
	var pipeImm *immutable.ImmutableDb
	var pipeLastChunk uint64
	pipeCopied := false
	const pipelineCopyChunkStride = 64
	onChunkContiguous := func(immutableDir string, num uint64) error {
		if pipeImm == nil {
			var nerr error
			if pipeImm, nerr = immutable.New(immutableDir); nerr != nil {
				return fmt.Errorf(
					"opening immutable DB for pipelined copy: %w", nerr,
				)
			}
		}
		// Throttle: copy every N contiguous chunks rather than on each one;
		// the post-bootstrap copy finishes whatever remainder is left.
		if pipeCopied && num < pipeLastChunk+pipelineCopyChunkStride {
			return nil
		}
		maxSlot, ok, serr := pipeImm.LastSlotInChunk(num)
		if serr != nil {
			return fmt.Errorf(
				"pipelined copy bound for chunk %d: %w", num, serr,
			)
		}
		if !ok {
			return nil
		}
		// First blob write: mark the sync in-progress now (not before
		// bootstrap), so a bootstrap that fails before any write leaves an
		// existing healthy database untouched.
		if !pipeCopied {
			if merr := markSyncInProgress(db, cfg.StorageMode); merr != nil {
				return merr
			}
		}
		if _, _, cerr := node.CopyImmutableBlobsBounded(
			ctx, logger, pipeImm, copyChain, maxSlot, nil,
		); cerr != nil {
			return fmt.Errorf(
				"pipelined copy to slot %d: %w", maxSlot, cerr,
			)
		}
		pipeLastChunk = num
		pipeCopied = true
		return nil
	}

	cfg.emit(SyncProgress{Phase: PhaseBootstrap, Active: true})
	result, err := Bootstrap(
		ctx,
		BootstrapConfig{
			OnChunkContiguous:      onChunkContiguous,
			Network:                network,
			Backend:                cfg.Backend,
			AggregatorURL:          aggregatorURL,
			DownloadDir:            downloadDir,
			CleanupAfterLoad:       cfg.CleanupAfterLoad,
			VerifyCertificateChain: cfg.VerifyCertChain,
			GenesisVerificationKey: nodeCfg.MithrilGenesisVerificationKey,
			AncillaryVerificationKey: nodeCfg.
				MithrilGenesisAncillaryVerificationKey,
			Logger:                 logger,
			DownloadIdleTimeout:    downloadIdleTimeout,
			DownloadMaxIdleRetries: cfg.DownloadMaxIdleRetries,
			OnProgress: func() func(DownloadProgress) {
				const progressLogInterval = 10 * time.Second
				const progressLogPercentStep = 5.0

				lastLogTime := time.Time{}
				lastLoggedPercent := -progressLogPercentStep

				return func(p DownloadProgress) {
					cfg.emit(SyncProgress{
						Phase:           PhaseBootstrap,
						Active:          true,
						BytesDownloaded: p.BytesDownloaded,
						TotalBytes:      p.TotalBytes,
						BytesPerSecond:  p.BytesPerSecond,
						Percent:         pctOf(p.BytesDownloaded, p.TotalBytes),
					})
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
							HumanBytes(p.BytesDownloaded),
							HumanBytes(p.TotalBytes),
							HumanBytes(int64(p.BytesPerSecond)),
						),
						"component", "mithril",
					)
					lastLogTime = now
					lastLoggedPercent = p.Percent
				}
			}(),
		},
	)
	cfg.emit(SyncProgress{Phase: PhaseBootstrap, Active: false})
	if err != nil {
		return SyncResult{}, fmt.Errorf("mithril bootstrap failed: %w", err)
	}

	// Backstop the in-progress marker for paths that did not pipeline a copy
	// (for example the v1 backend, which has no per-chunk hook). Idempotent
	// when the pipelined copy already set it. Set after a successful bootstrap
	// and before any post-bootstrap write or deferred-index drop.
	if err := markSyncInProgress(db, cfg.StorageMode); err != nil {
		return SyncResult{}, err
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

	// Drop the deferred-index manifest BEFORE inserting any rows
	// so secondary indexes are not maintained during ledger-state
	// import, immutable blob load, or API backfill. The rebuild is
	// triggered explicitly below — before updateMithrilReadyState
	// clears sync_status — so a crash between drop and rebuild
	// leaves sync_status set and triggers the recovery path on
	// the next startup.
	deferredIndexes := node.WithDeferredIndexes(db, logger)

	// Import ledger state and copy blocks in parallel.
	// Ledger state goes to metadata (SQLite), blocks go to the blob
	// store (Badger) — completely independent data stores.
	var loadResult *node.LoadBlobsResult
	var ledgerStateSlot uint64
	var ledgerStateHash []byte
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		cfg.emit(SyncProgress{Phase: PhaseLedgerImport, Active: true})
		defer cfg.emit(SyncProgress{Phase: PhaseLedgerImport, Active: false})
		slot, hash, importErr := importLedgerState(
			gctx, db, logger, nodeCfg, result,
			func(p ledgerstate.ImportProgress) {
				cfg.emit(SyncProgress{
					Phase:       PhaseLedgerImport,
					Active:      true,
					Percent:     p.Percent,
					Count:       p.Current,
					Total:       p.Total,
					Description: p.Stage,
				})
			},
		)
		if importErr != nil {
			return fmt.Errorf("importing ledger state: %w", importErr)
		}
		ledgerStateSlot = slot
		ledgerStateHash = hash
		if len(hash) > 0 {
			cfg.emit(SyncProgress{Phase: PhaseLedgerImport, Active: true, CurrentSlot: slot})
		}
		return nil
	})

	g.Go(func() error {
		cfg.emit(SyncProgress{Phase: PhaseImmutableCopy, Active: true})
		defer cfg.emit(SyncProgress{Phase: PhaseImmutableCopy, Active: false})
		logger.Info(
			"loading ImmutableDB blocks into blob store",
			"component", "mithril",
			"immutable_dir", result.ImmutableDir,
		)
		var loadErr error
		loadResult, loadErr = node.LoadBlobsWithDB(
			gctx, nil, logger, result.ImmutableDir, db,
			node.WithLoadBlobsProgress(func(p node.LoadBlobsProgress) {
				cfg.emit(SyncProgress{
					Phase:          PhaseImmutableCopy,
					Active:         true,
					Count:          p.BlocksCopied,
					CurrentSlot:    p.CurrentSlot,
					TipSlot:        p.TipSlot,
					Percent:        p.Percent,
					BytesPerSecond: p.BlocksPerSecond,
				})
			}),
		)
		if loadErr != nil {
			return fmt.Errorf("loading ImmutableDB: %w", loadErr)
		}
		if loadResult != nil {
			cfg.emit(SyncProgress{
				Phase:       PhaseImmutableCopy,
				Active:      true,
				Count:       loadResult.BlocksCopied,
				CurrentSlot: loadResult.ImmutableTipSlot,
				TipSlot:     loadResult.ImmutableTipSlot,
				Percent:     100,
			})
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		return SyncResult{}, err
	}

	// The pipelined copy stores produced-UTxO offsets per block but does not
	// advance the offsets-complete marker, and the post-bootstrap copy only
	// marks it when it copied at least one block (so it is skipped when the
	// pipeline already copied the entire immutable DB). Record it explicitly
	// for the full immutable tip so a later API backfill or resume does not
	// redo offset work the copy already performed. Idempotent.
	if loadResult != nil {
		if err := node.MarkImmutableUtxoOffsetsComplete(
			db, loadResult.ImmutableTipSlot,
		); err != nil {
			return SyncResult{}, fmt.Errorf(
				"recording immutable UTxO offsets complete: %w", err,
			)
		}
	}

	// Fetch volatile blocks between the ImmutableDB tip and the
	// ledger state tip. The Mithril snapshot's UTxO set is at the
	// ledger state tip, but the ImmutableDB only has blocks up to
	// an earlier point. We must close this gap so the node has a
	// continuous chain matching the UTxO state.
	recentBlocks, err := database.BlocksRecent(db, 1)
	if err != nil {
		return SyncResult{}, fmt.Errorf("reading chain tip: %w", err)
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
			return SyncResult{}, fmt.Errorf(
				"removing stale volatile blocks above slot %d: %w",
				immutableTipSlot, cleanupErr,
			)
		}
		recentBlocks, err = database.BlocksRecent(db, 1)
		if err != nil {
			return SyncResult{}, fmt.Errorf(
				"reading chain tip after stale volatile cleanup: %w",
				err,
			)
		}
	}
	if len(recentBlocks) > 0 &&
		recentBlocks[0].Slot > immutableTipSlot &&
		immutableTipSlot < ledgerStateSlot {
		cfg.emit(SyncProgress{Phase: PhaseGapBlocks, Active: true})
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
			return SyncResult{}, fmt.Errorf(
				"loading stored gap blocks from blob store: %w",
				err,
			)
		}
		immutableTip, err := database.BlockBeforeSlot(
			db,
			immutableTipSlot+1,
		)
		if err != nil {
			return SyncResult{}, fmt.Errorf(
				"reading immutable tip for stored gap validation: %w",
				err,
			)
		}
		if immutableTip.Slot != immutableTipSlot {
			return SyncResult{}, fmt.Errorf(
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
				return SyncResult{}, fmt.Errorf(
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
					return SyncResult{}, fmt.Errorf(
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
			cfg.emit(SyncProgress{Phase: PhaseGapBlocks, Active: true, Count: len(storedGapBlocks)})
			if err := processGapBlocks(
				ctx,
				db,
				logger,
				storedGapBlocks,
			); err != nil {
				return SyncResult{}, fmt.Errorf(
					"processing stored gap block transactions: %w",
					err,
				)
			}
			recentBlocks, err = database.BlocksRecent(db, 1)
			if err != nil {
				return SyncResult{}, fmt.Errorf(
					"reading chain tip after gap resume: %w",
					err,
				)
			}
		}
		cfg.emit(SyncProgress{Phase: PhaseGapBlocks, Active: false})
	}
	if len(recentBlocks) > 0 && ledgerStateSlot > recentBlocks[0].Slot {
		cfg.emit(SyncProgress{Phase: PhaseGapBlocks, Active: true})
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
			return SyncResult{}, fmt.Errorf("fetching volatile blocks: %w", fetchErr)
		}
		cfg.emit(SyncProgress{Phase: PhaseGapBlocks, Active: true, Count: len(gapBlocks)})
		// Store gap blocks to the blob store, then index
		// their metadata. These two steps are not atomic:
		// if processGapBlocks fails, re-running the sync
		// will re-fetch and re-store the blocks. Both
		// BlockCreate and SetTransaction use idempotent
		// writes, so re-processing is safe.
		for _, block := range gapBlocks {
			if err := db.BlockCreate(block, nil); err != nil {
				return SyncResult{}, fmt.Errorf("storing volatile block: %w", err)
			}
		}
		if err := processGapBlocks(ctx, db, logger, gapBlocks); err != nil {
			return SyncResult{}, fmt.Errorf("processing gap block transactions: %w", err)
		}
		logger.Info(
			"volatile blocks stored",
			"component", "mithril",
			"count", len(gapBlocks),
		)
		cfg.emit(SyncProgress{Phase: PhaseGapBlocks, Active: false})
	}
	// Skip post-ledger-state processing when no ledger state was imported.
	// importLedgerState returns (0, nil, nil) for a snapshot with no ledger
	// state file. In that case there is no ledger-state tip to chain from,
	// and validateStoredGapContinuity would compare the first stored block's
	// PrevHash against a nil hash and always fail.
	if len(ledgerStateHash) > 0 {
		cfg.emit(SyncProgress{Phase: PhasePostLedger, Active: true})
		if err := processPostLedgerStateBlocks(
			ctx,
			db,
			logger,
			ledgerStateSlot,
			ledgerStateHash,
		); err != nil {
			return SyncResult{}, err
		}
		cfg.emit(SyncProgress{Phase: PhasePostLedger, Active: false})
	}

	if isAPIMode(cfg.StorageMode) {
		if err := updateMithrilReadyState(
			db, logger, loadResult, ledgerStateSlot, ledgerStateHash,
			syncStatusBackfill, false,
		); err != nil {
			return SyncResult{}, err
		}
	}

	// Backfill historical metadata if storage mode is API.
	// This replays all stored blocks to populate transaction
	// records needed for API queries (Blockfrost, UTxO RPC).
	if isAPIMode(cfg.StorageMode) {
		cfg.emit(SyncProgress{Phase: PhaseBackfill, Active: true})
		logger.Info(
			"backfilling historical metadata for API mode",
			"component", "mithril",
		)
		if err := node.RunPlannerStats(db, logger); err != nil {
			return SyncResult{}, fmt.Errorf("running planner statistics before backfill: %w", err)
		}
		bf := node.NewBackfill(db, nodeCfg, logger)
		if err := bf.SetBatchSize(cfg.BackfillBatchSize); err != nil {
			return SyncResult{}, fmt.Errorf(
				"invalid backfill batch size %d in SetBatchSize: %w",
				cfg.BackfillBatchSize,
				err,
			)
		}
		bf.DisableNonceComputation()
		bf.SetProgressFunc(func(p node.BackfillProgress) {
			cfg.emit(SyncProgress{
				Phase:          PhaseBackfill,
				Active:         true,
				CurrentSlot:    p.Slot,
				TipSlot:        p.TipSlot,
				Percent:        p.ProgressPercent,
				BytesPerSecond: p.BlocksPerSecond,
				Count:          p.ProcessedBlocks,
			})
		})
		if err := bf.Run(ctx); err != nil {
			return SyncResult{}, fmt.Errorf("backfill: %w", err)
		}
		cfg.emit(SyncProgress{Phase: PhaseBackfill, Active: false})
	}

	// Rebuild critical deferred indexes BEFORE clearing sync_status so a
	// crash here leaves sync_status set (serve refuses to start)
	// and metadata_indexes_pending set (next mithril sync rebuilds
	// before re-running anything else).
	cfg.emit(SyncProgress{Phase: PhaseIndexRebuild, Active: true})
	indexRebuildStart := time.Now()
	if err := deferredIndexes.BuildCritical(); err != nil {
		cfg.emit(SyncProgress{Phase: PhaseIndexRebuild, Active: false})
		return SyncResult{}, err
	}
	indexRebuildElapsed := time.Since(indexRebuildStart)
	cfg.emit(SyncProgress{Phase: PhaseIndexRebuild, Active: true, Description: indexRebuildElapsed.String()})
	cfg.emit(SyncProgress{Phase: PhaseIndexRebuild, Active: false})
	logger.Info(
		"critical deferred metadata indexes rebuilt; lazy indexes deferred to maintenance",
		"component", "mithril",
		"duration", indexRebuildElapsed,
	)

	if err := updateMithrilReadyState(
		db, logger, loadResult, ledgerStateSlot, ledgerStateHash,
		"", true,
	); err != nil {
		return SyncResult{}, err
	}
	syncComplete = true
	cfg.emit(SyncProgress{Phase: PhaseComplete, Active: true})

	// Clean up temporary files after a successful complete load.
	if cfg.CleanupAfterLoad {
		result.Cleanup(logger)
	}

	logger.Info(
		"Mithril bootstrap complete",
		"component", "mithril",
		"epoch", result.Snapshot.Beacon.Epoch,
		"immutable_file_number", result.Snapshot.Beacon.ImmutableFileNumber,
		"index_rebuild_elapsed", indexRebuildElapsed,
		"lazy_index_rebuild_mode", "maintenance",
	)

	return SyncResult{Snapshot: result.Snapshot, LedgerSlot: ledgerStateSlot}, nil
}

// NeedsSync reports whether the database at cfg.DataDir requires a (re)sync —
// i.e. it is empty or its sync_status indicates an incomplete sync.
func NeedsSync(cfg SyncConfig) (bool, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	db, err := database.New(&database.Config{
		DataDir:        cfg.DataDir,
		Logger:         logger,
		BlobPlugin:     cfg.BlobPlugin,
		RunMode:        cfg.RunMode,
		MetadataPlugin: cfg.MetadataPlugin,
		MaxConnections: 1,
		StorageMode:    cfg.StorageMode,
		Network:        cfg.Network,
	})
	if err != nil {
		var cte database.CommitTimestampError
		if !errors.As(err, &cte) || db == nil {
			return false, fmt.Errorf("opening database: %w", err)
		}
	}
	defer db.Close()
	val, err := db.GetSyncState("sync_status", nil)
	if err != nil {
		return false, fmt.Errorf("checking sync state: %w", err)
	}
	if val == "" {
		// An empty sync_status is ambiguous: a brand-new database that has
		// never been synced and a database whose sync completed (which clears
		// the key) both report "". Distinguish them by chain presence — a
		// completed sync has stored blocks while a fresh database has none, so
		// an empty database still needs a sync.
		recent, err := database.BlocksRecent(db, 1)
		if err != nil {
			return false, fmt.Errorf("checking chain data: %w", err)
		}
		return len(recent) == 0, nil
	}
	return val != syncStatusBackfill, nil
}

// parseOptionalDuration parses an optional duration string.
// Returns (0, nil) when value is empty.
func parseOptionalDuration(
	name string,
	value string,
) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", name, value, err)
	}
	return duration, nil
}
