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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/database/models"
	datatypes "github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/node"
	"github.com/blinklabs-io/dingo/ledgerstate"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// syncKeySnapshotRestoreActive marks a sync_status "in_progress" as the node's
// own snapshot restore, which the node resumes itself, rather than an
// interrupted `dingo mithril sync`, which only that command may finish. It is
// part of the ephemeral sync lifecycle: written at the restore's first
// database write and removed by the completion clear.
const syncKeySnapshotRestoreActive = "mithril_snapshot_restore_active"

// Values of syncKeySnapshotRestoreActive: how far an interrupted restore got,
// which decides how it may resume.
const (
	// restorePhaseBlocks: blocks may be stored, no ledger row yet.
	restorePhaseBlocks = "blocks"
	// restorePhaseDiscard: the restore was re-pinned to another artifact
	// and the blocks stored for the old one are being deleted; a resume
	// deletes them again before loading.
	restorePhaseDiscard = "discard"
	// restorePhaseLedger: the ledger import has begun, so the database may
	// hold the pinned artifact's partly imported ledger rows, and only
	// that artifact can finish it.
	restorePhaseLedger = "ledger"
)

// syncKeySnapshotImportKey records the ledger-state import checkpoint key of
// a restore in the ledger phase, so its progress can be read. Ephemeral like
// the restore marker.
const syncKeySnapshotImportKey = "mithril_snapshot_import_key"

// ErrSnapshotRestoreUnrecoverable reports an interrupted restore that cannot
// be finished in place: the aggregator no longer serves the artifact its
// partly imported ledger state came from. Retrying cannot help. Every row in
// the database was written by the restore, so removing the database and
// restoring afresh is the recovery.
var ErrSnapshotRestoreUnrecoverable = errors.New(
	"mithril: interrupted snapshot restore cannot be finished",
)

// snapshotDownloadSubdir is where a restore downloads under the data
// directory when no download directory is configured.
const snapshotDownloadSubdir = "mithril-snapshot"

// maxCutoffProbes bounds how many immutable files a restore downloads, one at
// a time and from the certified tip down, looking for the file that holds the
// history-expiry cutoff. A stability window spans about six files on every
// Cardano network, so the bound is only reached on a chain with a much
// longer window; the files below the lowest probe are then loaded too.
const maxCutoffProbes = 16

// errStoredChainOffCertified reports stored blocks a resumed restore cannot
// place on the chain of the artifact it is restoring.
var errStoredChainOffCertified = errors.New(
	"stored blocks are not on the certified chain",
)

// SnapshotStartup is what a restore at node startup does with a database.
type SnapshotStartup int

const (
	// SnapshotStartupFresh is an empty database: no sync in progress, no
	// stored block and no ledger tip. A restore starts from scratch.
	SnapshotStartupFresh SnapshotStartup = iota
	// SnapshotStartupResume is a restore of the node's own that was
	// interrupted. A restore resumes it against the artifact it pinned.
	SnapshotStartupResume
	// SnapshotStartupPopulated is anything else: a database with chain data,
	// or one another sync left incomplete. Nothing is restored over it.
	SnapshotStartupPopulated
)

// SnapshotStartupState classifies db for a restore at node startup.
func SnapshotStartupState(
	ctx context.Context,
	db *database.Database,
) (SnapshotStartup, error) {
	status, err := db.GetSyncState("sync_status", nil)
	if err != nil {
		return 0, fmt.Errorf("reading sync_status: %w", err)
	}
	if status != "" {
		own, err := SnapshotRestoreInterrupted(db)
		if err != nil {
			return 0, err
		}
		if own {
			return SnapshotStartupResume, nil
		}
		return SnapshotStartupPopulated, nil
	}
	recent, err := database.BlocksRecent(ctx, db, 1)
	if err != nil {
		return 0, fmt.Errorf("reading chain tip: %w", err)
	}
	if len(recent) > 0 {
		return SnapshotStartupPopulated, nil
	}
	tip, err := db.GetTip(nil)
	if err != nil {
		return 0, fmt.Errorf("reading ledger tip: %w", err)
	}
	if tip.Point.Slot != 0 || len(tip.Point.Hash) != 0 {
		return SnapshotStartupPopulated, nil
	}
	return SnapshotStartupFresh, nil
}

// SnapshotRestoreInterrupted reports whether db holds a snapshot restore the
// node itself started and did not finish.
func SnapshotRestoreInterrupted(db *database.Database) (bool, error) {
	status, err := db.GetSyncState("sync_status", nil)
	if err != nil {
		return false, fmt.Errorf("reading sync_status: %w", err)
	}
	if status != syncStatusInProgress {
		return false, nil
	}
	active, err := db.GetSyncState(syncKeySnapshotRestoreActive, nil)
	if err != nil {
		return false, fmt.Errorf("reading mithril snapshot restore flag: %w", err)
	}
	return active != "", nil
}

// gapFetcher fetches the blocks after start up to and including end.
type gapFetcher func(
	ctx context.Context,
	logger *slog.Logger,
	network string,
	start ocommon.Point,
	end ocommon.Point,
) ([]models.Block, error)

// SnapshotConfig configures RunSnapshotBackfill.
type SnapshotConfig struct {
	// Feed carries the aggregator, trust keys and download limits. The
	// ancillary verification key is required. StartImmutable is ignored:
	// the restore derives the range from the ledger state. WindowChunks
	// bounds the probed files kept on disk for the load, and separately
	// each feed that loads the rest, so the trios on disk stay within it
	// except when the cutoff lies below the probe bound: the feed loading
	// the files below the probes then runs while the kept probes wait, for
	// a peak of twice WindowChunks. Zero keeps up to maxCutoffProbes probed
	// files.
	Feed ChunkFeedConfig
	// DB is the empty database to restore into, or the one holding the
	// restore to resume.
	DB *database.Database
	// StabilityWindow is the history-expiry retention, in slots: every block
	// at or above the ledger state's slot minus it is loaded.
	StabilityWindow uint64
	// CardanoNodeConfig supplies era epoch lengths to the ledger import.
	CardanoNodeConfig *cardano.CardanoNodeConfig
	// DataDir is the node's data directory. The restore downloads under it
	// when Feed.Bootstrap.DownloadDir is empty. One of the two is required.
	DataDir    string
	Logger     *slog.Logger
	OnProgress SyncProgressFunc

	fetchGap gapFetcher
	// firstWrite, when set, runs inside the transaction of the restore's
	// first write, after its writes; an error aborts that transaction.
	firstWrite func(*database.Txn) error
	// afterProbes, when set, runs once the probes are done and before the
	// first write.
	afterProbes func()
}

func (cfg SnapshotConfig) emit(p SyncProgress) {
	if cfg.OnProgress != nil {
		cfg.OnProgress(p)
	}
}

// pinnedArtifactPruned reports whether the aggregator no longer serves the
// pinned artifact: it answered 404 for the artifact itself, and answers 404
// again when asked once more. A 404 for anything else, a proxy's passing 404
// or an unreachable aggregator is not pruning.
func pinnedArtifactPruned(
	ctx context.Context,
	client *Client,
	digest string,
	err error,
) bool {
	if !errors.Is(err, errPinnedArtifactNotServed) {
		return false
	}
	_, confirm := client.GetCardanoDatabaseSnapshot(ctx, digest)
	return isAggregatorNotFound(confirm)
}

// RunSnapshotBackfill restores a Mithril snapshot into an empty database for
// a node that expires history, and returns the imported ledger state's point.
//
// It verifies the latest artifact's certificate chain, downloads the
// immutable files from the certified tip down to the one holding the
// history-expiry cutoff (ledger slot minus StabilityWindow), and downloads and
// verifies the ancillary ledger state against its signed manifest. Blocks
// between the certified tip and a ledger state beyond it are fetched from a
// relay. All of that happens before the first database write, so a failure
// there leaves the database exactly as it was.
//
// It then loads the blocks, imports the ledger state, records the Mithril
// trust boundary and the bootstrap marker, and clears the sync state, in the
// order `dingo mithril sync` does. A failure from the first write on leaves
// the restore resumable: SnapshotStartupState reports it as
// SnapshotStartupResume, and a later call resumes it against the same
// artifact. When the aggregator no longer serves that artifact, a restore
// that stored only blocks discards them and starts afresh on the latest one;
// one whose ledger import had begun fails with
// ErrSnapshotRestoreUnrecoverable, since importing another artifact over the
// rows already written is not known to leave none of them behind. Only a
// 404 for the pinned artifact itself, confirmed by a second request, counts
// as no longer served.
//
// An error caused by a source not answering usefully (see
// ErrSnapshotRestoreUnavailable) wraps that sentinel, whichever phase it
// came from.
//
// Probed files wait for the load on disk, not in memory; at most
// WindowChunks of them are kept, the rest re-downloaded by the load.
func RunSnapshotBackfill(
	ctx context.Context,
	cfg SnapshotConfig,
) (point ocommon.Point, err error) {
	defer func() {
		if err != nil && ctx.Err() == nil &&
			!errors.Is(err, ErrSnapshotRestoreUnrecoverable) &&
			isSourceUnavailable(err) {
			err = fmt.Errorf("%w: %w", ErrSnapshotRestoreUnavailable, err)
		}
	}()
	if cfg.DB == nil {
		return ocommon.Point{}, errors.New("snapshot restore requires a database")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Feed.Bootstrap.Logger == nil {
		cfg.Feed.Bootstrap.Logger = cfg.Logger
	}
	logger := cfg.Logger
	db := cfg.DB

	state, err := SnapshotStartupState(ctx, db)
	if err != nil {
		return ocommon.Point{}, err
	}
	if state == SnapshotStartupPopulated {
		return ocommon.Point{}, errors.New(
			"snapshot restore requires an empty database",
		)
	}
	resume := state == SnapshotStartupResume
	phase := ""
	if resume {
		phase, err = db.GetSyncState(syncKeySnapshotRestoreActive, nil)
		if err != nil {
			return ocommon.Point{}, fmt.Errorf(
				"reading mithril snapshot restore phase: %w", err,
			)
		}
	} else if err := clearStrayRestoreKeys(db); err != nil {
		return ocommon.Point{}, err
	}

	bcfg, url, err := prepareFeedConfig(cfg.Feed.Bootstrap)
	if err != nil {
		return ocommon.Point{}, err
	}
	if strings.TrimSpace(bcfg.AncillaryVerificationKey) == "" {
		return ocommon.Point{}, errors.New(
			"snapshot restore requires an ancillary verification key",
		)
	}
	if bcfg.DownloadDir == "" {
		if cfg.DataDir == "" {
			return ocommon.Point{}, errors.New(
				"snapshot restore requires a download or data directory",
			)
		}
		bcfg.DownloadDir = filepath.Join(cfg.DataDir, snapshotDownloadSubdir)
	}
	downloadDir := bcfg.DownloadDir
	if err := sweepStaleFeedDirs(downloadDir, logger); err != nil {
		return ocommon.Point{}, err
	}

	var pin pinnedArtifact
	if resume {
		var ok bool
		pin, ok, err = getPinnedArtifact(db)
		if err != nil {
			return ocommon.Point{}, err
		}
		if !ok {
			return ocommon.Point{}, errNoArtifactPin
		}
		if err := pin.validateForRun(BackendV2, bcfg.Network); err != nil {
			return ocommon.Point{}, err
		}
		bcfg.PinnedDigest = pin.Digest
	}
	client := newMithrilClient(url, bcfg.AllowInsecureHTTP)
	artifact, err := selectVerifiedArtifact(ctx, bcfg, client)
	repinned := false
	if resume && errors.Is(err, errPinnedArtifactNotServed) &&
		!pinnedArtifactPruned(ctx, client, pin.Digest, err) {
		// The 404 did not repeat: something between the node and the
		// aggregator, not the aggregator pruning the artifact.
		return ocommon.Point{}, fmt.Errorf(
			"%w: %w", ErrSnapshotRestoreUnavailable, err,
		)
	}
	if resume && errors.Is(err, errPinnedArtifactNotServed) {
		// Aggregators prune old artifacts, so a resume can outlive its
		// pin. Once the ledger import has begun, the rows it wrote belong
		// to that artifact, and no import of another one is known to
		// remove all of them (governance proposals among them).
		if phase == restorePhaseLedger {
			return ocommon.Point{}, fmt.Errorf(
				"%w: the aggregator no longer serves artifact %s, whose "+
					"ledger state the restore had begun importing: %w",
				ErrSnapshotRestoreUnrecoverable, pin.Digest, err,
			)
		}
		// With only blocks stored, they are discarded and the restore
		// starts afresh on the latest artifact.
		logger.Warn(
			"mithril: the artifact the interrupted restore pinned is no "+
				"longer served, restarting on the latest",
			"component", "mithril",
			"pinned_snapshot_hash", pin.Digest,
			"restore_phase", phase,
			"error", err,
		)
		bcfg.PinnedDigest = ""
		artifact, err = selectVerifiedArtifact(ctx, bcfg, client)
		repinned = true
	}
	if err != nil {
		return ocommon.Point{}, err
	}
	snapshot := &SnapshotListItem{SnapshotBase: SnapshotBase{
		Digest:          artifact.Hash,
		Network:         artifact.Network,
		Beacon:          artifact.Beacon,
		CertificateHash: artifact.CertificateHash,
	}}
	if resume && !repinned {
		if err := pin.verifyResolved(snapshot); err != nil {
			return ocommon.Point{}, err
		}
	}
	nextPhase := phase
	switch {
	case !resume:
		nextPhase = restorePhaseBlocks
	case repinned, phase == restorePhaseDiscard:
		nextPhase = restorePhaseDiscard
	case phase != restorePhaseLedger:
		nextPhase = restorePhaseBlocks
	}
	if len(artifact.Ancillary.Locations) == 0 {
		return ocommon.Point{}, errors.New(
			"snapshot restore requires ancillary locations",
		)
	}
	logger.Info(
		"mithril: restoring snapshot",
		"component", "mithril",
		"snapshot_hash", artifact.Hash,
		"immutable_file_number", artifact.Beacon.ImmutableFileNumber,
		"resume", resume,
		"repinned", repinned,
	)
	// The digest list and the ancillary archive are this restore's one-shot
	// downloads, held to the same location policy and budgets as a bootstrap's.
	// The block feeds below bring their own.
	dcfg, closeArtifactClient, err := withOneShotDownloadClient(
		bcfg, url, downloadDir, true,
	)
	if err != nil {
		return ocommon.Point{}, err
	}
	defer closeArtifactClient()
	digests, err := fetchVerifiedDigests(
		ctx, client, dcfg, artifact, downloadDir,
	)
	if err != nil {
		return ocommon.Point{}, err
	}

	keep := maxCutoffProbes
	if cfg.Feed.WindowChunks > 0 {
		keep = min(keep, cfg.Feed.WindowChunks)
	}
	probes, err := newSnapshotProbes(bcfg, client, artifact, digests, logger, keep)
	if err != nil {
		return ocommon.Point{}, err
	}
	defer probes.close()
	target := artifact.Beacon.ImmutableFileNumber
	certifiedTip, err := probes.certifiedTip(ctx, target)
	if err != nil {
		return ocommon.Point{}, err
	}
	slotLimit := func() (uint64, error) {
		return ancillarySlotLimit(cfg.CardanoNodeConfig, certifiedTip.Slot)
	}
	if cfg.fetchGap == nil {
		cfg.fetchGap = func(
			ctx context.Context,
			logger *slog.Logger,
			network string,
			start ocommon.Point,
			end ocommon.Point,
		) ([]models.Block, error) {
			maxGapBlocks, err := ancillaryBlockLimit(
				cfg.CardanoNodeConfig,
			)
			if err != nil {
				return nil, err
			}
			return fetchGapBlocks(
				ctx, logger, network, start, end, maxGapBlocks,
			)
		}
	}
	if resume && !repinned {
		if err := pin.verifyCertifiedTip(certifiedTip.Slot); err != nil {
			return ocommon.Point{}, err
		}
	}

	ancillary, ancillaryDigests, archive, err := downloadAncillaryV2(
		ctx, dcfg, artifact, downloadDir,
	)
	defer removeAncillary(logger, ancillary, archive)
	if err != nil {
		return ocommon.Point{}, fmt.Errorf(
			"verified Mithril ancillary data unavailable: %w", err,
		)
	}
	result := &BootstrapResult{
		Snapshot:          snapshot,
		AncillaryDir:      ancillary.Path(),
		AncillaryRoot:     ancillary.Root(),
		AncillaryVerified: true,
		AncillaryDigests:  ancillaryDigests,
	}
	prepared, err := prepareLedgerStateImport(
		logger, result, certifiedTip.Slot, slotLimit,
	)
	if err != nil {
		return ocommon.Point{}, fmt.Errorf("preparing ledger state: %w", err)
	}
	defer prepared.Close()
	ledgerPoint := ocommon.NewPoint(
		prepared.state.Tip.Slot, prepared.state.Tip.BlockHash,
	)
	if ledgerPoint.Slot > certifiedTip.Slot && !prepared.beyondCertifiedTip {
		return ocommon.Point{}, fmt.Errorf(
			"selected ledger state slot %d is past certified ImmutableDB tip slot %d",
			ledgerPoint.Slot, certifiedTip.Slot,
		)
	}
	var gap []models.Block
	if ledgerPoint.Slot > certifiedTip.Slot {
		gap, err = cfg.fetchGap(
			ctx, logger, bcfg.Network, certifiedTip, ledgerPoint,
		)
		if err != nil {
			return ocommon.Point{}, fmt.Errorf("fetching volatile blocks: %w", err)
		}
		if err := validateCompleteGapBlocks(
			gap, certifiedTip, ledgerPoint,
		); err != nil {
			return ocommon.Point{}, fmt.Errorf(
				"validating volatile blocks: %w", err,
			)
		}
	}

	// From the artifact being restored alone: a same-artifact resume
	// stored its blocks from this same file, and a re-pin discards them.
	cutoffSlot := ledgerPoint.Slot - min(ledgerPoint.Slot, cfg.StabilityWindow)
	start, found, err := probes.cutoff(ctx, target, cutoffSlot)
	if err != nil {
		return ocommon.Point{}, err
	}
	logger.Info(
		"mithril: snapshot history range",
		"component", "mithril",
		"ledger_slot", ledgerPoint.Slot,
		"cutoff_slot", cutoffSlot,
		"first_immutable", start,
		"target_immutable", target,
	)
	if cfg.afterProbes != nil {
		cfg.afterProbes()
	}

	// Everything above is read-only. The first write records, in one
	// transaction, the artifact to resume against, that the restore is the
	// node's own, and that a sync is in progress, so no interruption leaves
	// one without the others.
	if !resume || repinned {
		if err := db.MetadataTxn(ctx, true).Do(func(txn *database.Txn) error {
			if err := setPinnedArtifactTxn(db, pinnedArtifact{
				Backend:             BackendV2,
				Network:             artifact.Network,
				Digest:              artifact.Hash,
				Epoch:               artifact.Beacon.Epoch,
				ImmutableFileNumber: artifact.Beacon.ImmutableFileNumber,
				CertificateHash:     artifact.CertificateHash,
				CertifiedTipSlot:    certifiedTip.Slot,
				CertifiedTipSlotSet: true,
			}, txn); err != nil {
				return err
			}
			if err := db.SetSyncState(
				syncKeySnapshotRestoreActive, nextPhase, txn,
			); err != nil {
				return fmt.Errorf(
					"marking mithril snapshot restore active: %w", err,
				)
			}
			if err := db.SetSyncState(
				"sync_status", syncStatusInProgress, txn,
			); err != nil {
				return fmt.Errorf("marking sync in-progress: %w", err)
			}
			if cfg.firstWrite != nil {
				return cfg.firstWrite(txn)
			}
			return nil
		}); err != nil {
			return ocommon.Point{}, err
		}
	}
	if nextPhase == restorePhaseDiscard {
		if err := discardStoredBlocks(ctx, db, logger); err != nil {
			return ocommon.Point{}, err
		}
		nextPhase = restorePhaseBlocks
	}
	storedTip, err := storedChainTip(ctx, db)
	if err != nil {
		return ocommon.Point{}, err
	}
	cleanupBulkLoadPragmas := node.WithBulkLoadPragmas(db, logger)
	defer func() {
		if err := cleanupBulkLoadPragmas(); err != nil {
			logger.Error("failed to restore normal settings", "error", err)
		}
	}()
	deferredIndexes := node.WithDeferredIndexes(ctx, db, logger)

	loaded, tipOnChain, err := loadSnapshotBlocks(
		ctx, cfg, bcfg, probes, start, found, target,
	)
	if err != nil {
		return ocommon.Point{}, err
	}
	if storedTip != nil && !tipOnChain && !blocksContain(gap, *storedTip) {
		return ocommon.Point{}, fmt.Errorf(
			"%w: stored tip at slot %d", errStoredChainOffCertified,
			storedTip.Slot,
		)
	}

	if nextPhase == restorePhaseBlocks {
		if err := db.MetadataTxn(ctx, true).Do(func(txn *database.Txn) error {
			if err := db.SetSyncState(
				syncKeySnapshotImportKey,
				ledgerImportKey(artifact.Hash, ledgerPoint.Slot), txn,
			); err != nil {
				return err
			}
			return db.SetSyncState(
				syncKeySnapshotRestoreActive, restorePhaseLedger, txn,
			)
		}); err != nil {
			return ocommon.Point{}, fmt.Errorf(
				"marking mithril snapshot ledger import: %w", err,
			)
		}
	}
	cfg.emit(SyncProgress{Phase: PhaseLedgerImport, Active: true})
	_, _, _, err = importPreparedLedgerState(
		ctx, db, logger, cfg.CardanoNodeConfig, result, prepared, false,
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
	cfg.emit(SyncProgress{Phase: PhaseLedgerImport, Active: false})
	if err != nil {
		return ocommon.Point{}, fmt.Errorf("importing ledger state: %w", err)
	}
	// The block copy stores produced-UTxO offsets only outside Core mode, so
	// only then may the marker claim them; a later API backfill builds the
	// ones a Core restore left out.
	if db.StorageMode() != datatypes.StorageModeCore {
		if err := node.MarkImmutableUtxoOffsetsComplete(
			db, certifiedTip.Slot,
		); err != nil {
			return ocommon.Point{}, fmt.Errorf(
				"recording immutable UTxO offsets complete: %w", err,
			)
		}
	}
	if len(gap) > 0 {
		// Both writes are idempotent, so a resumed restore repeats them.
		for _, block := range gap {
			if err := db.BlockCreate(block, nil); err != nil {
				return ocommon.Point{}, fmt.Errorf(
					"storing volatile block: %w", err,
				)
			}
		}
		if err := processGapBlocks(ctx, db, logger, gap); err != nil {
			return ocommon.Point{}, fmt.Errorf(
				"processing gap block transactions: %w", err,
			)
		}
	}
	if err := setStableMithrilLedgerTip(
		ctx, db, ledgerPoint.Slot, ledgerPoint.Hash,
	); err != nil {
		return ocommon.Point{}, err
	}
	if _, err := finishMithrilImport(
		ctx, db, logger, cfg.emit, deferredIndexes, cleanupBulkLoadPragmas,
		&node.LoadBlobsResult{
			BlocksCopied:     loaded,
			ImmutableTipSlot: certifiedTip.Slot,
		},
		ledgerPoint.Slot, ledgerPoint.Hash, true,
	); err != nil {
		return ocommon.Point{}, err
	}
	// After the completion clear, which would remove it. The trust boundary
	// the clear left in place already makes WasBootstrapped true, so a failed
	// write here costs a later catch-up its start hint, not correctness.
	if err := setImmutableImportMarker(db, target); err != nil {
		logger.Warn(
			"failed to record Mithril immutable-import marker",
			"component", "mithril",
			"error", err,
		)
	}
	cfg.emit(SyncProgress{Phase: PhaseComplete, Active: true})
	logger.Info(
		"mithril: snapshot restore complete",
		"component", "mithril",
		"ledger_slot", ledgerPoint.Slot,
		"blocks_loaded", loaded,
		"first_immutable", start,
		"immutable_file_number", target,
	)
	return ledgerPoint, nil
}

// discardStoredBlocks deletes every stored block, for a restore re-pinned to
// another artifact. Only blocks, and outside Core mode their produced-output
// offsets, were written before the ledger import, and the import overwrites
// the entry of every output it holds, so the blocks alone have to go. The restore marker
// stays restorePhaseDiscard until the deletion is done, so an interruption
// repeats it; the deletion is idempotent.
func discardStoredBlocks(
	ctx context.Context,
	db *database.Database,
	logger *slog.Logger,
) error {
	recent, err := database.BlocksRecent(ctx, db, 1)
	if err != nil {
		return fmt.Errorf("reading chain tip: %w", err)
	}
	if len(recent) > 0 {
		deleted, err := lifecycle.DeleteBlocksAfter(
			ctx, db, 0, recent[0].ID, 0,
		)
		if err != nil {
			return fmt.Errorf("discarding stored blocks: %w", err)
		}
		logger.Info(
			"mithril: discarded blocks stored for the previous artifact",
			"component", "mithril",
			"blocks", deleted,
		)
	}
	if err := db.SetSyncState(
		syncKeySnapshotRestoreActive, restorePhaseBlocks, nil,
	); err != nil {
		return fmt.Errorf("marking mithril snapshot restore active: %w", err)
	}
	return nil
}

// SnapshotRestoreProgress returns a token that changes whenever an
// interrupted restore advances: its phase, its stored chain tip, or its
// ledger import checkpoint. Comparing it across a failed attempt tells an
// attempt that got further from one that did not.
func SnapshotRestoreProgress(
	ctx context.Context,
	db *database.Database,
) (string, error) {
	phase, err := db.GetSyncState(syncKeySnapshotRestoreActive, nil)
	if err != nil {
		return "", fmt.Errorf("reading mithril snapshot restore phase: %w", err)
	}
	tip, err := storedChainTip(ctx, db)
	if err != nil {
		return "", err
	}
	tipText := "none"
	if tip != nil {
		tipText = fmt.Sprintf("%d.%x", tip.Slot, tip.Hash)
	}
	checkpoint := "none"
	key, err := db.GetSyncState(syncKeySnapshotImportKey, nil)
	if err != nil {
		return "", fmt.Errorf("reading mithril snapshot import key: %w", err)
	}
	if key != "" {
		cp, err := db.Metadata().GetImportCheckpoint(key, nil)
		if err != nil {
			return "", fmt.Errorf("reading import checkpoint: %w", err)
		}
		if cp != nil {
			checkpoint = cp.Phase
		}
	}
	return phase + "|" + tipText + "|" + checkpoint, nil
}

// clearStrayRestoreKeys removes restore keys a fresh database can still hold:
// the restore writes them only together with sync_status, but a database
// written by an earlier build may hold them alone, and a later interrupted
// manual sync would then read as the node's own restore.
func clearStrayRestoreKeys(db *database.Database) error {
	for _, key := range []string{
		syncKeySnapshotRestoreActive, syncKeyPinnedArtifact,
		syncKeySnapshotImportKey,
	} {
		if err := db.DeleteSyncState(key, nil); err != nil {
			return fmt.Errorf("clearing stray %s: %w", key, err)
		}
	}
	return nil
}

// storedChainTip returns the highest stored block's point, or nil when no
// block is stored.
func storedChainTip(
	ctx context.Context,
	db *database.Database,
) (*ocommon.Point, error) {
	recent, err := database.BlocksRecent(ctx, db, 1)
	if err != nil {
		return nil, fmt.Errorf("reading chain tip: %w", err)
	}
	if len(recent) == 0 {
		return nil, nil
	}
	p := ocommon.NewPoint(recent[0].Slot, recent[0].Hash)
	return &p, nil
}

func blocksContain(blocks []models.Block, p ocommon.Point) bool {
	for _, b := range blocks {
		if b.Slot == p.Slot && bytes.Equal(b.Hash, p.Hash) {
			return true
		}
	}
	return false
}

// removeAncillary releases and deletes the ancillary download. The ledger
// state it held has either been imported or will be downloaded again.
func removeAncillary(
	logger *slog.Logger,
	tree *vettedDir,
	archive *bootstrapFile,
) {
	if tree != nil {
		if err := cleanupVettedDir(tree); err != nil {
			logger.Warn(
				"mithril: removing ancillary data",
				"component", "mithril",
				"error", err,
			)
		}
	}
	if archive != nil {
		if err := archive.cleanup(); err != nil {
			logger.Warn(
				"mithril: removing ancillary archive",
				"component", "mithril",
				"error", err,
			)
		}
	}
}

// snapshotProbes downloads single immutable files of one verified artifact,
// each through its own window-of-one feed, and keeps the lowest of them on
// disk for the load, at most keep at a time. Only the slots a probe found are
// held in memory.
type snapshotProbes struct {
	bcfg     BootstrapConfig
	client   *Client
	artifact *CardanoDatabaseSnapshot
	digests  map[string]string
	logger   *slog.Logger
	keep     int

	storeDir  string
	store     *bootstrapDir
	storeRoot *os.Root
	probed    map[uint64]probeResult
	onDisk    map[uint64]struct{}
}

// probeResult is what a probe learned of one file.
type probeResult struct {
	empty     bool
	firstSlot uint64
	last      ocommon.Point
}

func newSnapshotProbes(
	bcfg BootstrapConfig,
	client *Client,
	artifact *CardanoDatabaseSnapshot,
	digests map[string]string,
	logger *slog.Logger,
	keep int,
) (*snapshotProbes, error) {
	if err := os.MkdirAll(bcfg.DownloadDir, extractDirMode); err != nil {
		return nil, fmt.Errorf("creating download directory: %w", err)
	}
	// Named as a feed's directory is, so a crashed restore's probes are
	// swept with the feeds'.
	dir, store, err := createBootstrapDir(
		bcfg.DownloadDir,
		chunkFeedDirPrefix+truncateDigest(artifact.Hash)+"-*",
	)
	if err != nil {
		return nil, fmt.Errorf("creating probe directory: %w", err)
	}
	_, root, err := openImmutableRoot(filepath.Join(dir, "extract"))
	if err != nil {
		_ = store.cleanup()
		return nil, err
	}
	return &snapshotProbes{
		bcfg:      bcfg,
		client:    client,
		artifact:  artifact,
		digests:   digests,
		logger:    logger,
		keep:      max(keep, 1),
		storeDir:  dir,
		store:     store,
		storeRoot: root,
		probed:    map[uint64]probeResult{},
		onDisk:    map[uint64]struct{}{},
	}, nil
}

func (p *snapshotProbes) close() {
	if err := p.storeRoot.Close(); err != nil {
		p.logger.Warn(
			"mithril: closing probe directory",
			"component", "mithril", "error", err,
		)
	}
	if err := p.store.cleanup(); err != nil {
		p.logger.Warn(
			"mithril: removing probe directory",
			"component", "mithril", "error", err,
		)
	}
}

func (p *snapshotProbes) probe(
	ctx context.Context,
	num uint64,
) (probeResult, error) {
	if res, ok := p.probed[num]; ok {
		return res, nil
	}
	// Evicted before the download, so the trios on disk never exceed keep.
	// The probes descend, so the highest is evicted and the ones kept are
	// the lowest, the first the ascending load needs.
	if len(p.onDisk) >= p.keep {
		highest := uint64(0)
		for n := range p.onDisk {
			highest = max(highest, n)
		}
		if err := p.drop(highest); err != nil {
			return probeResult{}, err
		}
	}
	cfg := ChunkFeedConfig{
		Bootstrap:      p.bcfg,
		StartImmutable: num,
		// A window of one: num is never released, so the feed downloads it
		// and nothing after it.
		WindowChunks: 1,
		Workers:      1,
		RawBlocks:    true,
	}
	feed, err := newVerifiedChunkFeed(
		ctx, cfg, p.bcfg, p.client, p.artifact, p.digests,
	)
	if err != nil {
		return probeResult{}, err
	}
	defer closeFeed(feed, p.logger)
	chunk, err := feed.Next(ctx)
	if err != nil {
		return probeResult{}, fmt.Errorf("probing immutable file %05d: %w", num, err)
	}
	res := probeResult{empty: len(chunk.Raw) == 0}
	if !res.empty {
		last := chunk.Raw[len(chunk.Raw)-1]
		res.firstSlot = chunk.Raw[0].Slot
		res.last = ocommon.NewPoint(last.Slot, last.Hash)
	}
	// The trio moves out of the feed's directory before the feed is closed
	// and removes it. Moved by name, so it is re-verified against the
	// certified digests when the load reads it back.
	src := filepath.Join(feed.extractDir, "immutable")
	dst := filepath.Join(p.storeDir, "extract", "immutable")
	for _, ext := range immutableFileExtensions {
		name := fmt.Sprintf("%05d.%s", num, ext)
		if err := os.Rename(
			filepath.Join(src, name), filepath.Join(dst, name),
		); err != nil {
			removeImmutableTrio(p.storeRoot, num)
			return probeResult{}, fmt.Errorf(
				"keeping probed immutable file %s: %w", name, err,
			)
		}
	}
	p.onDisk[num] = struct{}{}
	p.probed[num] = res
	return res, nil
}

func (p *snapshotProbes) drop(num uint64) error {
	if err := removeImmutableTrioErr(p.storeRoot, num); err != nil {
		return fmt.Errorf("removing probed immutable file %05d: %w", num, err)
	}
	delete(p.onDisk, num)
	return nil
}

// readBack returns the blocks of a kept file, read through the certified
// digests, and deletes the file.
func (p *snapshotProbes) readBack(num uint64) ([]immutable.Block, error) {
	imm, err := immutable.NewFromRootVerified(p.storeRoot, p.digests, "")
	if err != nil {
		return nil, fmt.Errorf("opening probed immutable files: %w", err)
	}
	iter, err := imm.BlocksInChunk(num)
	if err != nil {
		return nil, fmt.Errorf("reading probed immutable file %05d: %w", num, err)
	}
	var blocks []immutable.Block
	for {
		next, err := iter.Next()
		if err != nil {
			_ = iter.Close()
			return nil, fmt.Errorf(
				"reading probed immutable file %05d: %w", num, err,
			)
		}
		if next == nil {
			break
		}
		blocks = append(blocks, *next)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return blocks, p.drop(num)
}

// certifiedTip returns the last block of the highest non-empty certified
// file at or below target.
func (p *snapshotProbes) certifiedTip(
	ctx context.Context,
	target uint64,
) (ocommon.Point, error) {
	for num := target; ; num-- {
		if len(p.probed) >= maxCutoffProbes {
			return ocommon.Point{}, fmt.Errorf(
				"no block in the top %d certified immutable files",
				maxCutoffProbes,
			)
		}
		res, err := p.probe(ctx, num)
		if err != nil {
			return ocommon.Point{}, err
		}
		if !res.empty {
			return res.last, nil
		}
		if num == 0 {
			return ocommon.Point{}, errors.New(
				"certified immutable files hold no blocks",
			)
		}
	}
}

// cutoff returns the lowest immutable file history expiry keeps: the highest
// file whose first block is at or below cutoffSlot, so every block at or
// above cutoffSlot is in it or above it. found is false when the probe bound
// was reached first; start is then the lowest file probed, and the files
// below it must be loaded as well.
func (p *snapshotProbes) cutoff(
	ctx context.Context,
	target uint64,
	cutoffSlot uint64,
) (start uint64, found bool, err error) {
	for num := target; ; num-- {
		if _, ok := p.probed[num]; !ok && len(p.probed) >= maxCutoffProbes {
			p.logger.Warn(
				"mithril: history-expiry cutoff is below the probed files, "+
					"loading every certified file below them",
				"component", "mithril",
				"cutoff_slot", cutoffSlot,
				"lowest_probed_immutable", num+1,
			)
			return num + 1, false, nil
		}
		res, err := p.probe(ctx, num)
		if err != nil {
			return 0, false, err
		}
		if !res.empty && res.firstSlot <= cutoffSlot {
			return num, true, nil
		}
		if num == 0 {
			p.logger.Info(
				"mithril: history-expiry cutoff precedes the first "+
					"certified block, keeping all history",
				"component", "mithril",
				"cutoff_slot", cutoffSlot,
			)
			return 0, true, nil
		}
	}
}

// loadSnapshotBlocks stores the certified blocks from file start, or from
// file 0 when the cutoff was not found, through target, and returns how many
// it stored and whether the chain's tip before the load is on the certified
// chain. Blocks at or below that tip were stored by an interrupted run and
// are skipped; the chain rejects a block that does not extend its tip.
func loadSnapshotBlocks(
	ctx context.Context,
	cfg SnapshotConfig,
	bcfg BootstrapConfig,
	probes *snapshotProbes,
	start uint64,
	found bool,
	target uint64,
) (int, bool, error) {
	cm, err := chain.NewManager(ctx, cfg.DB, nil)
	if err != nil {
		return 0, false, fmt.Errorf("loading chain manager: %w", err)
	}
	c := cm.PrimaryChain()
	if c == nil {
		return 0, false, errors.New("primary chain not available")
	}
	cfg.emit(SyncProgress{Phase: PhaseImmutableCopy, Active: true})
	defer cfg.emit(SyncProgress{Phase: PhaseImmutableCopy, Active: false})
	tip := c.Tip().Point
	atOrigin := tip.Slot == 0 && len(tip.Hash) == 0
	// The tip is on the certified chain once a certified block equals it or
	// the chain accepts a certified block as its child.
	tipOnChain := atOrigin
	loaded := 0
	store := func(blocks []immutable.Block) error {
		skip := 0
		for !atOrigin && skip < len(blocks) && blocks[skip].Slot <= tip.Slot {
			if blocks[skip].Slot == tip.Slot {
				if !bytes.Equal(blocks[skip].Hash, tip.Hash) {
					return fmt.Errorf(
						"%w: a different block holds the stored tip's slot %d",
						errStoredChainOffCertified, tip.Slot,
					)
				}
				tipOnChain = true
			}
			skip++
		}
		n, err := node.CopyImmutableBlocks(ctx, c, blocks[skip:])
		loaded += n
		if n > 0 {
			tipOnChain = true
		}
		if err != nil && !tipOnChain && skip < len(blocks) {
			// The first certified block after the stored tip does not
			// extend it.
			return fmt.Errorf(
				"%w: stored tip at slot %d: %w",
				errStoredChainOffCertified, tip.Slot, err,
			)
		}
		cfg.emit(SyncProgress{
			Phase: PhaseImmutableCopy, Active: true, Count: loaded,
		})
		return err
	}
	if !found && start > 0 {
		if err := loadFeedRange(
			ctx, cfg, bcfg, probes, 0, start, store,
		); err != nil {
			return loaded, tipOnChain, err
		}
	}
	num := start
	for ; num <= target; num++ {
		if _, ok := probes.onDisk[num]; !ok {
			break
		}
		blocks, err := probes.readBack(num)
		if err != nil {
			return loaded, tipOnChain, err
		}
		if err := store(blocks); err != nil {
			return loaded, tipOnChain, err
		}
	}
	if num <= target {
		if err := loadFeedRange(
			ctx, cfg, bcfg, probes, num, target+1, store,
		); err != nil {
			return loaded, tipOnChain, err
		}
	}
	return loaded, tipOnChain, nil
}

// loadFeedRange stores the certified files from first up to, not including,
// end through a feed whose window bounds the trios on disk.
func loadFeedRange(
	ctx context.Context,
	cfg SnapshotConfig,
	bcfg BootstrapConfig,
	probes *snapshotProbes,
	first, end uint64,
	store func([]immutable.Block) error,
) error {
	feed, err := newVerifiedChunkFeed(ctx, ChunkFeedConfig{
		Bootstrap:      bcfg,
		StartImmutable: first,
		WindowChunks:   cfg.Feed.WindowChunks,
		Workers:        cfg.Feed.Workers,
		RawBlocks:      true,
	}, bcfg, probes.client, probes.artifact, probes.digests)
	if err != nil {
		return err
	}
	defer closeFeed(feed, cfg.Logger)
	for {
		chunk, err := feed.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if chunk.Num >= end {
			return nil
		}
		if err := store(chunk.Raw); err != nil {
			return err
		}
		if err := feed.Release(chunk.Num); err != nil {
			return err
		}
	}
}
