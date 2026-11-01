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
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ErrStaleBatch reports that the applier's tip moved between the moment a
// batch was prepared and the moment it would have been applied, so the batch
// was discarded unapplied. A BatchApplier must return an error satisfying
// errors.Is(err, ErrStaleBatch) in that case.
var ErrStaleBatch = errors.New("mithril: applied tip moved under the batch")

// maxStreamRestarts bounds how many times one RunStreamCatchUp call replays
// from a moved tip before giving up. A tip that keeps moving means another
// writer is active, and the caller is better served syncing from peers.
const maxStreamRestarts = 8

// streamRestartBackoff is the wait before the nth replay, times n, capped at
// maxStreamRestartBackoff.
const (
	streamRestartBackoff    = 50 * time.Millisecond
	maxStreamRestartBackoff = 500 * time.Millisecond
)

// streamDownloadSubdir is where, under StreamConfig.DataDir, a run downloads
// when no download directory is configured.
const streamDownloadSubdir = "mithril-stream"

// DefaultStreamNoProgressTimeout is how long a held run may go without
// committing a batch before it gives up. A batch closes after at most
// BatchChunks files or MaxBatchBlocks blocks, minutes of download even for
// dense mainnet files on a slow link, so a longer gap means a stalled or
// degraded aggregator, and the node is better served syncing from peers than
// holding them off any longer.
const DefaultStreamNoProgressTimeout = 5 * time.Minute

// ErrStreamStalled reports a held run that committed no batch within its
// no-progress timeout.
var ErrStreamStalled = errors.New(
	"mithril: stream catch-up committed no batch within its no-progress timeout",
)

// BatchApplier applies certified blocks to the ledger. The mithril package
// cannot import the ledger, so the node adapts its ledger state to this.
type BatchApplier interface {
	// Tip returns the point of the last applied block, or the zero point for
	// an empty ledger.
	Tip() ocommon.Point
	// ApplyTrustedBatch applies blocks, which extend expected, as one batch,
	// and returns the new tip. When the tip is no longer expected it applies
	// nothing and returns an error satisfying errors.Is(err, ErrStaleBatch).
	ApplyTrustedBatch(
		ctx context.Context,
		expected ocommon.Point,
		blocks []ledger.Block,
	) (ocommon.Point, error)
}

// StreamConfig configures RunStreamCatchUp.
type StreamConfig struct {
	// Feed configures each chunk feed the run opens. Its StartImmutable is
	// ignored: the run derives the start from the ledger tip. An empty
	// Bootstrap.DownloadDir selects a directory under DataDir. The download
	// directory must belong to this run alone: stale feed directories in it
	// are deleted before each feed opens.
	Feed ChunkFeedConfig
	// DataDir is the node's data directory. The run downloads under it when
	// Feed.Bootstrap.DownloadDir is empty, so a crash leaves nothing in the
	// OS temp directory that no later run would clean up. One of the two is
	// required.
	DataDir string
	// ChunkSlots is the number of slots each immutable file spans: the Byron
	// epoch length, 10*k from the Byron genesis, for every Cardano network.
	// It turns the ledger tip's slot into an estimate of the tip's file. Zero
	// means no estimate, and the file is found by bisection.
	ChunkSlots uint64
	// BatchChunks is the most immutable files whose last block one batch may
	// carry. Zero or negative means one.
	BatchChunks int
	// MaxBatchBlocks is the most blocks one ApplyTrustedBatch call carries;
	// a chunk with more is split across batches. Zero or negative means no
	// limit.
	MaxBatchBlocks int
	// DB holds the immutable-import marker the run resumes from and advances.
	DB *database.Database
	// Applier is the ledger the blocks are applied to. Its tip must be on the
	// certified chain.
	Applier BatchApplier
	Logger  *slog.Logger
	// Hold, when set, is called once, after the run has verified its
	// artifact and digest list and located the applier's tip, and before it
	// re-reads the tip to apply anything; the run calls the returned release
	// on every exit, after its no-progress watchdog has stopped, so a release
	// may take longer than NoProgressTimeout. Everything that asks the aggregator about the artifact
	// happens before it, so a slow or failing aggregator delays only the
	// unheld half. Nil holds nothing.
	Hold func() (release func())
	// NoProgressTimeout ends a run, with ErrStreamStalled, once it has held
	// for this long without committing a batch. Zero or negative selects
	// DefaultStreamNoProgressTimeout.
	NoProgressTimeout time.Duration
	// chunkTaken, when set, is called with each chunk's number once the feed
	// has returned it decoded, on the goroutine that decoded it. Tests use it
	// to see when decoding happens.
	chunkTaken func(num uint64)
}

// StreamResult reports where a stream catch-up stopped.
type StreamResult struct {
	// Tip is the applier's tip when the run ended.
	Tip ocommon.Point
	// Immutable is the highest immutable file whose blocks are all committed,
	// as recorded in the import marker. It is zero when no marker was written.
	Immutable uint64
	// Restarts counts replays after the applier's tip moved under a batch.
	Restarts int
}

// RunStreamCatchUp streams the certified immutable files above the local
// chain into the applier, in batches, until the certified target.
//
// The run starts at the immutable file after the import marker, or at file 0
// with no marker. Certified blocks at or below the applier's tip are skipped,
// and the tip must be found on the certified chain before anything is
// applied: either as one of those blocks, as the parent of the first block
// after it, or, when the tip is above every certified block, as a descendant
// of the last one (a no-op). A tip found nowhere fails with
// ErrNotOnCertifiedChain and nothing is written.
//
// A chunk is released, and recorded in the marker, only once its last block
// has committed. When the applier reports ErrStaleBatch the feed is closed
// and the run starts over from the applier's new tip.
//
// Stream catch-up imports no ledger snapshot, so it never records the Mithril
// trust boundary, and its marker does not make WasBootstrapped true.
//
// Callers must treat every non-nil error, whatever it wraps, as a reason to
// sync from peers instead: the run stops at a committed batch boundary, so
// the ledger is always consistent when it returns.
func RunStreamCatchUp(
	ctx context.Context,
	cfg StreamConfig,
) (StreamResult, error) {
	if cfg.DB == nil {
		return StreamResult{}, errors.New("stream catch-up requires a database")
	}
	if cfg.Applier == nil {
		return StreamResult{}, errors.New(
			"stream catch-up requires a batch applier",
		)
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Feed.Bootstrap.Logger == nil {
		cfg.Feed.Bootstrap.Logger = cfg.Logger
	}
	cfg.BatchChunks = max(cfg.BatchChunks, 1)
	cfg.MaxBatchBlocks = max(cfg.MaxBatchBlocks, 0)
	if cfg.Feed.Bootstrap.DownloadDir == "" {
		if cfg.DataDir == "" {
			return StreamResult{}, errors.New(
				"stream catch-up requires a download or data directory",
			)
		}
		cfg.Feed.Bootstrap.DownloadDir = filepath.Join(
			cfg.DataDir, streamDownloadSubdir,
		)
	}

	if err := sweepStaleFeedDirs(
		cfg.Feed.Bootstrap.DownloadDir, cfg.Logger,
	); err != nil {
		return StreamResult{}, err
	}
	src, err := openStreamSource(ctx, cfg)
	if err != nil {
		return StreamResult{}, err
	}
	// Locating the tip before the hold costs the held phase nothing when
	// the tip has not left the located file by then: its probes are kept.
	if tip := cfg.Applier.Tip(); tip.Slot != 0 || len(tip.Hash) != 0 {
		if _, _, err := src.locate(ctx, cfg, tip); err != nil {
			return StreamResult{Tip: tip}, err
		}
	}

	if cfg.Hold != nil {
		release := cfg.Hold()
		// Deferred before the watchdog's stop so it runs after it: a
		// release may rebuild indexes for longer than the timeout, and
		// the watchdog must not fire on that.
		defer release()
	}
	timeout := cfg.NoProgressTimeout
	if timeout <= 0 {
		timeout = DefaultStreamNoProgressTimeout
	}
	runCtx, stopWatchdog, progress := startStreamWatchdog(ctx, timeout)
	defer stopWatchdog()
	res, err := runStreamRestarts(runCtx, cfg, src, progress)
	if err != nil && ctx.Err() == nil &&
		errors.Is(context.Cause(runCtx), ErrStreamStalled) {
		err = fmt.Errorf("%w (%s): %w", ErrStreamStalled, timeout, err)
	}
	return res, err
}

// startStreamWatchdog returns a context that is cancelled with
// ErrStreamStalled once timeout passes without a call to progress.
func startStreamWatchdog(
	ctx context.Context,
	timeout time.Duration,
) (context.Context, func(), func()) {
	runCtx, cancel := context.WithCancelCause(ctx)
	beat := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-beat:
				// Since Go 1.23 Reset also discards an expiry not yet
				// received.
				timer.Reset(timeout)
			case <-timer.C:
				cancel(ErrStreamStalled)
				return
			}
		}
	}()
	stop := func() {
		cancel(context.Canceled)
		<-done
	}
	progress := func() {
		select {
		case beat <- struct{}{}:
		default:
		}
	}
	return runCtx, stop, progress
}

// runStreamRestarts runs passes until one ends other than on a stale tip.
func runStreamRestarts(
	ctx context.Context,
	cfg StreamConfig,
	src *streamSource,
	progress func(),
) (StreamResult, error) {
	restarts := 0
	for {
		res, err := runStreamOnce(ctx, cfg, src, progress)
		res.Restarts = restarts
		if !errors.Is(err, ErrStaleBatch) {
			return res, err
		}
		if restarts >= maxStreamRestarts {
			return res, fmt.Errorf(
				"stream catch-up gave up after %d restarts: %w",
				restarts, err,
			)
		}
		restarts++
		cfg.Feed.Bootstrap.Metrics.staleRestart()
		cfg.Logger.Info(
			"stream catch-up: applied tip moved, replaying from the new tip",
			"component", "mithril",
			"restarts", restarts,
		)
		// A tip that moved once is likely still moving; give the other
		// writer a moment before locating it again.
		timer := time.NewTimer(min(
			time.Duration(restarts)*streamRestartBackoff,
			maxStreamRestartBackoff,
		))
		select {
		case <-ctx.Done():
			timer.Stop()
			return res, ctx.Err()
		case <-timer.C:
		}
	}
}

// streamRun is one pass over a feed, from the tip the applier had when it
// started.
type streamRun struct {
	cfg StreamConfig
	// progress is called after every committed batch.
	progress func()
	feed     *ChunkFeed
	tip      ocommon.Point

	// seeking is set until the local tip has been found on the certified
	// chain; blocks are skipped, never applied, while it is.
	seeking bool
	local   ocommon.Point
	// lastSeen is the last certified block skipped while seeking.
	lastSeen *ocommon.Point

	marked    bool
	committed uint64 // the marker's value, when marked

	batch []ledger.Block
	// batchChunks counts the chunks whose last block is in the batch.
	batchChunks int
	// done is the highest chunk all of whose blocks are committed or in the
	// batch, when doneSet.
	done    uint64
	doneSet bool
	// outstanding lists the chunks Next returned and Release has not.
	outstanding []uint64
}

// streamSource is the verified artifact one RunStreamCatchUp call streams
// from. Its certificate chain and digest list are verified once, when it is
// opened, and every feed of the call, tip probes and stale-tip replays
// included, opens on them without asking the aggregator again.
type streamSource struct {
	bcfg     BootstrapConfig
	client   *Client
	artifact *CardanoDatabaseSnapshot
	digests  map[string]string
	// probes keeps the block positions of every file a locate probed, for
	// the whole call, so the held locate and each replay's reuse them.
	probes map[uint64][]probeBlock
}

func openStreamSource(
	ctx context.Context,
	cfg StreamConfig,
) (*streamSource, error) {
	bcfg, url, err := prepareFeedConfig(cfg.Feed.Bootstrap)
	if err != nil {
		return nil, err
	}
	client := newMithrilClient(url, bcfg.AllowInsecureHTTP)
	artifact, err := selectVerifiedArtifact(ctx, bcfg, client)
	if err != nil {
		return nil, err
	}
	dir := bcfg.DownloadDir
	if err := os.MkdirAll(dir, extractDirMode); err != nil {
		return nil, fmt.Errorf("creating download directory: %w", err)
	}
	dcfg, closeDigestClient, err := withOneShotDownloadClient(
		bcfg, url, dir, false,
	)
	if err != nil {
		return nil, err
	}
	digests, err := fetchVerifiedDigests(ctx, client, dcfg, artifact, dir)
	closeDigestClient()
	if err != nil {
		return nil, err
	}
	return &streamSource{
		bcfg:     bcfg,
		client:   client,
		artifact: artifact,
		digests:  digests,
		probes:   map[uint64][]probeBlock{},
	}, nil
}

// locate finds the file the stream starts at for tip, which is not the
// origin, or ahead when tip is above every certified block.
func (s *streamSource) locate(
	ctx context.Context,
	cfg StreamConfig,
	tip ocommon.Point,
) (start uint64, ahead bool, err error) {
	marker, marked, err := getImmutableImportMarker(cfg.DB)
	if err != nil {
		return 0, false, err
	}
	loc := &tipLocator{
		cfg:    cfg,
		src:    s,
		tip:    tip,
		target: s.target(),
		probes: s.probes,
	}
	hint, hinted := ImmutableFileForSlot(tip.Slot, cfg.ChunkSlots)
	if !hinted && marked {
		hint, hinted = marker+1, true
	}
	return loc.locate(ctx, hint, hinted)
}

// target is the last certified immutable file of the source's artifact.
func (s *streamSource) target() uint64 {
	return s.artifact.Beacon.ImmutableFileNumber
}

// feed opens a chunk feed on the source's verified artifact and digests.
func (s *streamSource) feed(
	ctx context.Context,
	cfg ChunkFeedConfig,
) (*ChunkFeed, error) {
	return newVerifiedChunkFeed(
		ctx, cfg, s.bcfg, s.client, s.artifact, s.digests,
	)
}

func runStreamOnce(
	ctx context.Context,
	cfg StreamConfig,
	src *streamSource,
	progress func(),
) (StreamResult, error) {
	marker, marked, err := getImmutableImportMarker(cfg.DB)
	if err != nil {
		return StreamResult{}, err
	}
	tip := cfg.Applier.Tip()
	origin := tip.Slot == 0 && len(tip.Hash) == 0
	res := StreamResult{Tip: tip, Immutable: marker}
	target := CertifiedTarget{Immutable: src.target()}

	// The start comes from the ledger tip; the marker only records stream
	// progress and can lag the tip (Ouroboros moved it) or lead it (a
	// truncate), so it is at most a hint.
	var start uint64
	if !origin {
		var ahead bool
		start, ahead, err = src.locate(ctx, cfg, tip)
		if err != nil {
			return res, err
		}
		if ahead {
			cfg.Logger.Info(
				"stream catch-up: local chain already covers the certified "+
					"target",
				"component", "mithril",
				"tip_slot", tip.Slot,
				"target_immutable", target.Immutable,
			)
			if err := setStreamImportMarker(cfg.DB, target.Immutable); err != nil {
				return res, err
			}
			res.Immutable = target.Immutable
			return res, nil
		}
	}
	// Files below start are on the ledger in full; the start file is not.
	if marked && marker >= start {
		cfg.Logger.Warn(
			"stream catch-up: import marker is above the ledger tip; "+
				"resetting it to the ledger",
			"component", "mithril",
			"import_marker", marker,
			"tip_slot", tip.Slot,
			"tip_immutable", start,
		)
		if start == 0 {
			err = cfg.DB.DeleteSyncState(syncKeyImmutableMax, nil)
		} else {
			err = setImmutableImportMarker(cfg.DB, start-1)
		}
		if err != nil {
			return res, fmt.Errorf("resetting import marker: %w", err)
		}
		marker, marked = start-1, start > 0
		res.Immutable = marker
		if !marked {
			res.Immutable = 0
		}
	}
	if start > target.Immutable {
		// The tip is the last certified block.
		if err := setStreamImportMarker(cfg.DB, target.Immutable); err != nil {
			return res, err
		}
		res.Immutable = target.Immutable
		return res, nil
	}

	feedCfg := cfg.Feed
	feedCfg.StartImmutable = start
	// The feed goes before a restart opens the next one: two would hold two
	// windows of trios on disk at once.
	feed, err := src.feed(ctx, feedCfg)
	if err != nil {
		return res, err
	}
	defer closeFeed(feed, cfg.Logger)

	r := &streamRun{
		cfg:       cfg,
		progress:  progress,
		feed:      feed,
		tip:       tip,
		seeking:   !origin,
		local:     tip,
		marked:    marked,
		committed: marker,
	}
	err = r.run(ctx)
	res.Tip = r.tip
	if r.marked {
		res.Immutable = r.committed
	}
	if err != nil {
		return res, err
	}
	if r.seeking {
		// The locate step found the tip in the start file, which holds the
		// same certified bytes, so this means the aggregator changed them.
		return res, fmt.Errorf(
			"local tip slot %d block %x was not in immutable file %05d on "+
				"the second read: %w",
			tip.Slot, tip.Hash, start, ErrNotOnCertifiedChain,
		)
	}
	return res, nil
}

func closeFeed(feed *ChunkFeed, logger *slog.Logger) {
	if err := feed.Close(); err != nil {
		logger.Warn(
			"stream catch-up: closing chunk feed",
			"component", "mithril",
			"error", err,
		)
	}
}

// sweepStaleFeedDirs removes the feed directories a crashed run left in dir.
// Only a feed removes its own directory, on Close, so after a crash nothing
// else would, and each would hold up to a window of trios forever. Only
// directories named the way NewChunkFeed names them are touched.
func sweepStaleFeedDirs(dir string, logger *slog.Logger) error {
	parent, err := os.OpenRoot(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("opening download directory: %w", err)
	}
	defer parent.Close() //nolint:errcheck // primary sweep error is returned
	entries, err := fs.ReadDir(parent.FS(), ".")
	if err != nil {
		return fmt.Errorf("listing download directory: %w", err)
	}
	for _, entry := range entries {
		// ReadDir does not follow links, so a link is never a directory here.
		if !entry.IsDir() || !chunkFeedDirPattern.MatchString(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("inspecting stale chunk feed directory: %w", err)
		}
		root, err := parent.OpenRoot(entry.Name())
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("opening stale chunk feed directory: %w", err)
		}
		opened, err := root.Stat(".")
		if err != nil || !os.SameFile(info, opened) {
			_ = root.Close()
			return errors.Join(
				fmt.Errorf("%w: stale chunk feed directory changed", errBootstrapCleanupIdentity),
				err,
			)
		}
		path := filepath.Join(dir, entry.Name())
		if err := cleanupRetainedDir(
			parent, &root, opened, entry.Name(), path,
		); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return fmt.Errorf("removing stale chunk feed directory: %w", err)
		}
		logger.Info(
			"stream catch-up: removed a stale chunk feed directory",
			"component", "mithril",
			"path", path,
		)
	}
	return nil
}

// tipLocator finds the immutable file holding the ledger tip by downloading
// single files, each through its own verified feed.
type tipLocator struct {
	cfg    StreamConfig
	src    *streamSource
	tip    ocommon.Point
	target uint64
	// probes keeps only each probed file's block positions, not its decoded
	// blocks, so a long bisection holds a few hundred bytes per file.
	probes map[uint64][]probeBlock
}

// probeBlock is what locating the tip reads of a certified block.
type probeBlock struct {
	point ocommon.Point
	prev  []byte
}

// tipRelation places the ledger tip relative to one immutable file.
type tipRelation int

const (
	tipInFile    tipRelation = iota // in the file, or the parent of its first
	tipBelowFile                    // before the file's first block
	tipAboveFile                    // after the file's last block
	fileEmpty                       // the file holds no blocks
)

// probe returns the block positions of file num, downloading it once.
func (l *tipLocator) probe(
	ctx context.Context,
	num uint64,
) ([]probeBlock, error) {
	if blocks, ok := l.probes[num]; ok {
		return blocks, nil
	}
	cfg := l.cfg.Feed
	cfg.StartImmutable = num
	// A window of one: the feed downloads num and nothing after it, because
	// num is never released.
	cfg.WindowChunks = 1
	cfg.Workers = 1
	feed, err := l.src.feed(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer closeFeed(feed, l.cfg.Logger)
	chunk, err := feed.Next(ctx)
	if err != nil {
		return nil, fmt.Errorf("probing immutable file %05d: %w", num, err)
	}
	blocks := make([]probeBlock, 0, len(chunk.Blocks))
	for _, b := range chunk.Blocks {
		blocks = append(blocks, probeBlock{
			point: blockPoint(b),
			prev:  b.PrevHash().Bytes(),
		})
	}
	l.probes[num] = blocks
	return blocks, nil
}

// classify places the tip relative to file num. With the tip in the file it
// also returns the file the stream starts at: the next one when the tip is
// the file's last block.
func (l *tipLocator) classify(
	ctx context.Context,
	num uint64,
) (tipRelation, uint64, error) {
	blocks, err := l.probe(ctx, num)
	if err != nil {
		return 0, 0, err
	}
	if len(blocks) == 0 {
		return fileEmpty, 0, nil
	}
	for i, b := range blocks {
		p := b.point
		if p.Slot == l.tip.Slot && bytes.Equal(p.Hash, l.tip.Hash) {
			if i == len(blocks)-1 {
				return tipInFile, num + 1, nil
			}
			return tipInFile, num, nil
		}
	}
	first, last := blocks[0], blocks[len(blocks)-1]
	switch {
	case bytes.Equal(first.prev, l.tip.Hash):
		return tipInFile, num, nil
	case l.tip.Slot < first.point.Slot:
		return tipBelowFile, 0, nil
	case l.tip.Slot > last.point.Slot:
		return tipAboveFile, 0, nil
	}
	return 0, 0, fmt.Errorf(
		"local tip slot %d block %x is not in immutable file %05d, which "+
			"certifies slots %d to %d: %w",
		l.tip.Slot, l.tip.Hash, num, first.point.Slot, last.point.Slot,
		ErrNotOnCertifiedChain,
	)
}

// locate returns the file the stream starts at, or ahead when the tip is
// above every certified block and descends from the last one. The hint, when
// given, is probed first; a miss narrows a bisection over the certified file
// numbers, so a wrong hint costs about log2 of the file count in probes.
func (l *tipLocator) locate(
	ctx context.Context,
	hint uint64,
	hinted bool,
) (start uint64, ahead bool, err error) {
	lo, hi := uint64(0), l.target
	// step narrows [lo, hi] by probing file num; done reports a located tip.
	step := func(num uint64) (done bool, err error) {
		rel, next, err := l.classify(ctx, num)
		if err != nil {
			return false, err
		}
		switch rel {
		case tipInFile:
			start = next
			return true, nil
		case tipAboveFile:
			lo = num + 1
		case tipBelowFile:
			if num == 0 {
				return false, fmt.Errorf(
					"local tip slot %d block %x is below the first certified "+
						"block: %w",
					l.tip.Slot, l.tip.Hash, ErrNotOnCertifiedChain,
				)
			}
			hi = num - 1
		case fileEmpty:
			// Placement needs a block; look upward for one, and give up on
			// the upper part if there is none.
			for up := num + 1; up <= hi; up++ {
				rel, next, err := l.classify(ctx, up)
				if err != nil {
					return false, err
				}
				switch rel {
				case fileEmpty:
					continue
				case tipInFile:
					start = next
					return true, nil
				case tipAboveFile:
					lo = up + 1
					return false, nil
				case tipBelowFile:
				}
				// Below a block above num, with nothing in between: the tip
				// is below num as well.
				break
			}
			if num == 0 {
				return false, fmt.Errorf(
					"local tip slot %d block %x is below the first certified "+
						"block: %w",
					l.tip.Slot, l.tip.Hash, ErrNotOnCertifiedChain,
				)
			}
			hi = num - 1
		}
		return false, nil
	}
	if hinted {
		done, err := step(min(hint, l.target))
		if err != nil || done {
			return start, false, err
		}
	}
	for lo <= hi {
		done, err := step(lo + (hi-lo)/2)
		if err != nil || done {
			return start, false, err
		}
	}
	if lo <= l.target {
		return 0, false, fmt.Errorf(
			"local tip slot %d block %x falls between immutable files %05d "+
				"and %05d: %w",
			l.tip.Slot, l.tip.Hash, hi, lo, ErrNotOnCertifiedChain,
		)
	}
	return 0, true, l.verifyAhead(ctx)
}

// verifyAhead checks that a tip above every certified block descends from
// the last one.
func (l *tipLocator) verifyAhead(ctx context.Context) error {
	var last *ocommon.Point
	for num := l.target; ; num-- {
		blocks, err := l.probe(ctx, num)
		if err != nil {
			return err
		}
		if len(blocks) > 0 {
			p := blocks[len(blocks)-1].point
			last = &p
			break
		}
		if num == 0 {
			break
		}
	}
	if last == nil {
		return fmt.Errorf(
			"local tip slot %d block %x: no certified block to locate it "+
				"against: %w",
			l.tip.Slot, l.tip.Hash, ErrNotOnCertifiedChain,
		)
	}
	localBlock, err := database.BlockByHash(ctx, l.cfg.DB, l.tip.Hash)
	if err != nil {
		if errors.Is(err, models.ErrBlockNotFound) {
			return fmt.Errorf(
				"local tip slot %d block %x is not stored locally: %w",
				l.tip.Slot, l.tip.Hash, ErrNotOnCertifiedChain,
			)
		}
		return fmt.Errorf("reading local tip block: %w", err)
	}
	err = verifyLocalDescendsFromArtifactTip(
		ctx, l.cfg.DB, localBlock, *last, l.cfg.Logger,
	)
	if errors.Is(err, errCatchUpLocalAhead) {
		return nil
	}
	if err == nil {
		return errors.New("catch-up: ancestry check returned no verdict")
	}
	return err
}

func (r *streamRun) run(ctx context.Context) error {
	window := r.cfg.Feed.WindowChunks
	// Up to one batch of chunks decodes while the batch before it applies.
	ahead := startDecodeAhead(
		ctx, r.feed, r.cfg.BatchChunks, r.cfg.MaxBatchBlocks,
		r.cfg.chunkTaken,
	)
	defer ahead.stop()
	for {
		chunk, err := ahead.next()
		if errors.Is(err, io.EOF) {
			return r.flush(ctx)
		}
		if err != nil {
			return err
		}
		blocks := chunk.Blocks
		if r.seeking {
			blocks, err = r.seek(chunk)
			if err != nil {
				return err
			}
			if r.seeking {
				// Nothing to commit and no marker yet: the files only cost
				// disk, and the window needs the slot.
				if err := r.feed.Release(chunk.Num); err != nil {
					return err
				}
				continue
			}
			// Every chunk below this one is on the local chain.
			if chunk.Num > 0 {
				r.markDone(chunk.Num - 1)
			}
		}
		r.outstanding = append(r.outstanding, chunk.Num)
		for len(blocks) > 0 {
			take := len(blocks)
			if r.cfg.MaxBatchBlocks > 0 {
				take = min(take, r.cfg.MaxBatchBlocks-len(r.batch))
			}
			r.batch = append(r.batch, blocks[:take]...)
			blocks = blocks[take:]
			if len(blocks) == 0 {
				break
			}
			// The batch is full and the chunk goes on: commit what is
			// there, leaving the chunk outstanding and out of the marker.
			if err := r.flush(ctx); err != nil {
				return err
			}
		}
		r.markDone(chunk.Num)
		r.batchChunks++
		// Next cannot return a chunk the window holds back, so a batch
		// spanning the whole window is committed before asking for another.
		if r.batchChunks >= r.cfg.BatchChunks ||
			(r.cfg.MaxBatchBlocks > 0 &&
				len(r.batch) >= r.cfg.MaxBatchBlocks) ||
			(window > 0 && len(r.outstanding) >= window) {
			if err := r.flush(ctx); err != nil {
				return err
			}
		}
	}
}

// seek skips the chunk's blocks up to the local tip and returns the rest.
// It clears r.seeking once the tip is found.
func (r *streamRun) seek(chunk VerifiedChunk) ([]ledger.Block, error) {
	for i, b := range chunk.Blocks {
		p := blockPoint(b)
		switch {
		case p.Slot == r.local.Slot && bytes.Equal(p.Hash, r.local.Hash):
			r.seeking = false
			return chunk.Blocks[i+1:], nil
		case bytes.Equal(b.PrevHash().Bytes(), r.local.Hash):
			// The tip is the last block of an earlier chunk, which this run
			// did not download.
			r.seeking = false
			return chunk.Blocks[i:], nil
		case p.Slot <= r.local.Slot:
			// An epoch boundary block shares its slot with the block after
			// it, so an equal slot with another hash is not yet a fork.
			r.lastSeen = &p
		default:
			return nil, fmt.Errorf(
				"local tip slot %d block %x is not in immutable file %05d, "+
					"which certifies slot %d block %x after it: %w",
				r.local.Slot, r.local.Hash, chunk.Num, p.Slot, p.Hash,
				ErrNotOnCertifiedChain,
			)
		}
	}
	return nil, nil
}

// markDone records that chunk num and every chunk below it are committed or
// in the batch.
func (r *streamRun) markDone(num uint64) {
	if !r.doneSet || num > r.done {
		r.done, r.doneSet = num, true
	}
}

// flush commits the batch, then records the chunks it completed in the
// marker, then releases them, in that order: a chunk's trio and its marker
// entry describe committed blocks only.
func (r *streamRun) flush(ctx context.Context) error {
	if len(r.batch) > 0 {
		started := time.Now()
		tip, err := r.cfg.Applier.ApplyTrustedBatch(ctx, r.tip, r.batch)
		if err != nil {
			return fmt.Errorf(
				"applying %d certified blocks after slot %d: %w",
				len(r.batch), r.tip.Slot, err,
			)
		}
		r.cfg.Feed.Bootstrap.Metrics.observeBatchCommit(time.Since(started))
		r.progress()
		r.tip = tip
		// A fresh slice, not a truncation: the applier may keep the one it
		// was given.
		r.batch = nil
	}
	r.batchChunks = 0
	if err := r.writeMarker(); err != nil {
		return err
	}
	kept := r.outstanding[:0]
	for _, num := range r.outstanding {
		if !r.doneSet || num > r.done {
			kept = append(kept, num)
			continue
		}
		if err := r.feed.Release(num); err != nil {
			return err
		}
		r.cfg.Feed.Bootstrap.Metrics.chunkApplied()
	}
	r.outstanding = kept
	return nil
}

func (r *streamRun) writeMarker() error {
	if !r.doneSet || (r.marked && r.done <= r.committed) {
		return nil
	}
	if err := setStreamImportMarker(r.cfg.DB, r.done); err != nil {
		return err
	}
	r.committed, r.marked = r.done, true
	return nil
}

// blockPoint returns the chain point of b.
func blockPoint(b ledger.Block) ocommon.Point {
	return ocommon.NewPoint(b.SlotNumber(), b.Hash().Bytes())
}
