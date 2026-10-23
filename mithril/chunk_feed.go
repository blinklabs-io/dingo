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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"golang.org/x/sync/semaphore"
)

// ErrChunkFeedClosed is returned by ChunkFeed methods called after Close.
var ErrChunkFeedClosed = errors.New("mithril: chunk feed is closed")

// ErrChunkNotDelivered is returned by Release for a chunk number that Next
// has not yet returned, including numbers outside the feed's range. Deleting
// such a trio would pull files out from under a download that may still be
// writing them.
var ErrChunkNotDelivered = errors.New(
	"mithril: chunk was not delivered by this feed",
)

// chunkFeedDirPrefix starts the name of each feed's own directory beneath the
// download directory; chunkFeedDirPattern matches the whole name, which is
// the prefix, the truncated artifact digest, and os.MkdirTemp's numeric
// suffix.
const chunkFeedDirPrefix = "chunkfeed-"

var chunkFeedDirPattern = regexp.MustCompile(
	`^` + chunkFeedDirPrefix + `[0-9a-f]{1,16}-[0-9]+$`,
)

// ChunkFeedConfig configures NewChunkFeed.
type ChunkFeedConfig struct {
	// Bootstrap carries the network, aggregator, trust keys, download limits
	// and DownloadDir. VerifyCertificateChain and AncillaryVerificationKey
	// are ignored: a feed always verifies the certificate chain and never
	// reads the ancillary archive. An empty DownloadDir makes the feed create
	// and remove its own temporary directory.
	Bootstrap BootstrapConfig
	// StartImmutable is the lowest immutable file number to deliver. A value
	// above the certified target yields an empty feed.
	StartImmutable uint64
	// WindowChunks bounds how many chunk trios may exist on disk at once.
	// Zero or negative means unbounded. Trios wait in the window undecoded:
	// a chunk is decoded only when Next returns it, so decoded blocks in
	// memory are bounded by what the consumer holds, not by the window.
	WindowChunks int
	// Workers is the number of concurrent downloads. Zero or negative selects
	// the bootstrap default. It is capped at WindowChunks when that is set,
	// since a worker beyond the window could never start.
	Workers int
	// RawBlocks delivers each chunk's blocks as read from the verified trio,
	// in VerifiedChunk.Raw, instead of decoding them into VerifiedChunk.Blocks.
	RawBlocks bool
	// keep makes the feed Bootstrap's whole-artifact downloader rather than a
	// streaming source; see feedKeep.
	keep *feedKeep
}

// feedKeep configures a feed that downloads into a caller's stable directory
// and leaves the verified trios there after Close, for a caller that loads the
// extracted tree afterwards. A trio already in place that matches the
// certified digests is reused rather than fetched, which is how an interrupted
// manual sync resumes. The feed delivers each chunk's number only: nothing is
// read back or decoded, since the caller reads the tree itself. It requires
// already-verified digests and an unbounded window, and Release must not be
// called, since it deletes the trio the caller is keeping.
type feedKeep struct {
	extractDir string
	archiveDir string
	// onFetched, when set, runs on a worker goroutine with each chunk's
	// extracted size, whether downloaded or reused.
	onFetched func(bytes int64)
}

// CertifiedTarget identifies the last immutable file the aggregator's
// certificate covers.
type CertifiedTarget struct {
	// Immutable is the highest certified immutable file number.
	Immutable uint64
	// Tip is the last block of that file. The certificate does not carry it,
	// so it is zero from LatestCertifiedTarget and, on a feed, is filled in
	// once Next has returned the last chunk (and stays zero if that file
	// holds no blocks).
	Tip ocommon.Point
}

// VerifiedChunk is one immutable file whose trio matched the certified digest
// list before it was decoded.
type VerifiedChunk struct {
	Num    uint64
	Blocks []ledger.Block
	// Raw holds the chunk's blocks undecoded, when the feed was configured
	// with RawBlocks; Blocks is then empty.
	Raw []immutable.Block
}

// ChunkFeed downloads the certified immutable files of one Mithril artifact
// and hands them out in ascending order.
//
// Workers only download and verify trios; Next decodes the chunk it returns,
// reading it back through the digest-checked database. Decoded blocks
// therefore exist only for chunks the consumer has taken, never for the
// verified trios waiting in the window.
//
// Chunks are fetched in parallel but only inside a window that starts at the
// lowest chunk the consumer has not yet released, so a slow consumer stalls
// the downloads instead of filling the disk. The window is positional rather
// than a plain count of slots: with a count, workers could fill every slot
// with chunks above a slow one, and the consumer would then wait on a chunk
// that can never start.
type ChunkFeed struct {
	cfg       BootstrapConfig
	logger    *slog.Logger
	artifact  *CardanoDatabaseSnapshot
	locations []*CardanoDatabaseLocation
	digests   map[string]string

	first, last uint64
	window      uint64 // zero means unbounded
	empty       bool
	raw         bool
	keep        *feedKeep

	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup

	tempDir    string // set only when the feed created its own download dir
	feedDir    string // this feed's own directory beneath the download dir
	extractDir string
	archiveDir string
	immDir     string
	immRoot    *os.Root
	imm        *immutable.ImmutableDb
	transport  interface{ CloseIdleConnections() }
	inflight   *semaphore.Weighted
	weight     int64

	mu        sync.Mutex
	cond      *sync.Cond
	nextStart uint64              // next chunk to hand to a worker
	lowest    uint64              // lowest chunk not yet released
	nextOut   uint64              // next chunk Next returns
	ready     map[uint64]struct{} // verified trios on disk, not yet taken
	failed    map[uint64]error
	released  map[uint64]struct{} // released chunks above lowest
	halted    bool                // a chunk failed; start no more
	err       error               // sticky
	closed    bool
	closeErr  error
	target    CertifiedTarget
	// decoding counts Next calls decoding outside f.mu. Add runs under f.mu
	// only while the feed is open, so Close, which marks it closed first,
	// can wait for them before removing the files they read.
	decoding sync.WaitGroup
}

// prepareFeedConfig applies the same validation and defaults as Bootstrap and
// resolves the aggregator URL.
func prepareFeedConfig(cfg BootstrapConfig) (BootstrapConfig, string, error) {
	if err := (DownloadConfig{MaxBytes: cfg.DownloadMaxBytes}).Validate(); err != nil {
		return cfg, "", err
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if normalizeBackend(cfg.Backend) != BackendV2 {
		return cfg, "", fmt.Errorf(
			"unsupported Mithril backend %q (expected %q)",
			cfg.Backend, BackendV2,
		)
	}
	cfg.Backend = BackendV2
	cfg.VerifyCertificateChain = true
	if strings.TrimSpace(cfg.GenesisVerificationKey) == "" {
		return cfg, "", errors.New(
			"verified Mithril chunk feed requires a genesis verification key",
		)
	}
	if _, err := ParseVerificationKey(cfg.GenesisVerificationKey); err != nil {
		return cfg, "", fmt.Errorf(
			"parsing Mithril genesis verification key: %w", err,
		)
	}
	url := cfg.AggregatorURL
	if url == "" {
		var err error
		url, err = AggregatorURLForNetwork(cfg.Network)
		if err != nil {
			return cfg, "", fmt.Errorf("resolving aggregator URL: %w", err)
		}
	}
	// A feed is pinned by its target, never resumed from a recorded artifact.
	cfg.PinnedDigest = ""
	return cfg, url, nil
}

// selectVerifiedArtifact resolves the latest artifact for cfg's network and
// authenticates it against its certificate chain.
func selectVerifiedArtifact(
	ctx context.Context,
	cfg BootstrapConfig,
	client *Client,
) (*CardanoDatabaseSnapshot, error) {
	artifact, err := selectArtifactV2(ctx, cfg, client)
	if err != nil {
		if cfg.PinnedDigest != "" && isAggregatorNotFound(err) {
			return nil, fmt.Errorf("%w: %w", errPinnedArtifactNotServed, err)
		}
		return nil, err
	}
	if len(artifact.Immutables.Locations) == 0 {
		return nil, fmt.Errorf(
			"no immutable locations in Cardano database snapshot %s",
			artifact.Hash,
		)
	}
	if err := verifyArtifactCertificateV2(ctx, cfg, client, artifact); err != nil {
		return nil, err
	}
	return artifact, nil
}

// LatestCertifiedTarget reports the newest certified immutable file number
// after verifying the artifact and its certificate chain. It fetches no digest
// list and no immutable data; the returned Tip is zero because nothing
// certified names it without the data.
//
// Without cfg.CertificateCache every call walks and STM-verifies the whole
// chain to genesis, one request per certificate plus the stake distributions,
// which on mainnet is hundreds of requests. With a cache, a call whose leaf
// was already accepted costs only the artifact requests, and a new leaf costs
// one request per certificate added since plus the stake distributions; that
// is what makes it fit to poll.
func LatestCertifiedTarget(
	ctx context.Context,
	cfg BootstrapConfig,
) (CertifiedTarget, error) {
	cfg, url, err := prepareFeedConfig(cfg)
	if err != nil {
		return CertifiedTarget{}, err
	}
	artifact, err := selectVerifiedArtifact(
		ctx, cfg, newMithrilClient(url, cfg.AllowInsecureHTTP),
	)
	if err != nil {
		return CertifiedTarget{}, err
	}
	return CertifiedTarget{Immutable: artifact.Beacon.ImmutableFileNumber}, nil
}

// NewChunkFeed verifies the latest artifact's certificate chain and its digest
// list against the certified merkle root, then starts downloading. Nothing is
// downloaded from an immutable location before both checks pass. ctx bounds
// the feed's whole lifetime, not just construction; the caller must Close it.
func NewChunkFeed(
	ctx context.Context,
	cfg ChunkFeedConfig,
) (*ChunkFeed, error) {
	bcfg, url, err := prepareFeedConfig(cfg.Bootstrap)
	if err != nil {
		return nil, err
	}
	client := newMithrilClient(url, bcfg.AllowInsecureHTTP)
	artifact, err := selectVerifiedArtifact(ctx, bcfg, client)
	if err != nil {
		return nil, err
	}
	return newVerifiedChunkFeed(ctx, cfg, bcfg, client, artifact, nil)
}

// newVerifiedChunkFeed opens a feed on an artifact whose certificate chain
// selectVerifiedArtifact has already verified. digests, when not nil, is the
// artifact's digest list already verified against its certified merkle root;
// nil fetches and verifies it here. bcfg is cfg.Bootstrap as
// prepareFeedConfig returned it.
func newVerifiedChunkFeed(
	ctx context.Context,
	cfg ChunkFeedConfig,
	bcfg BootstrapConfig,
	client *Client,
	artifact *CardanoDatabaseSnapshot,
	digests map[string]string,
) (*ChunkFeed, error) {
	var err error
	if cfg.keep != nil {
		if digests == nil {
			return nil, errors.New(
				"kept chunk feed requires a verified digest list",
			)
		}
		// Bootstrap's logger already carries the artifact identity.
		cfg.WindowChunks = 0
	} else {
		bcfg.Logger = bcfg.Logger.With(
			"network", artifact.Network,
			"snapshot_hash", artifact.Hash,
			"snapshot_epoch", artifact.Beacon.Epoch,
			"snapshot_immutable_file_number",
			artifact.Beacon.ImmutableFileNumber,
			"artifact_backend", BackendV2,
		)
	}

	f := &ChunkFeed{
		cfg:       bcfg,
		logger:    bcfg.Logger,
		artifact:  artifact,
		first:     cfg.StartImmutable,
		last:      artifact.Beacon.ImmutableFileNumber,
		raw:       cfg.RawBlocks,
		keep:      cfg.keep,
		nextStart: cfg.StartImmutable,
		lowest:    cfg.StartImmutable,
		nextOut:   cfg.StartImmutable,
		ready:     map[uint64]struct{}{},
		failed:    map[uint64]error{},
		released:  map[uint64]struct{}{},
		target: CertifiedTarget{
			Immutable: artifact.Beacon.ImmutableFileNumber,
		},
	}
	f.cond = sync.NewCond(&f.mu)
	if cfg.WindowChunks > 0 {
		f.window = uint64(cfg.WindowChunks)
	}
	for i := range artifact.Immutables.Locations {
		if artifact.Immutables.Locations[i].URITemplate != "" {
			f.locations = append(f.locations, &artifact.Immutables.Locations[i])
		}
	}
	if len(f.locations) == 0 && (cfg.keep != nil || f.first <= f.last) {
		return nil, errors.New(
			"no usable immutable archive locations (URI template missing)",
		)
	}
	if cfg.keep != nil {
		// Opened before the empty check: the caller reads the directory even
		// when there is nothing to download into it.
		f.extractDir = cfg.keep.extractDir
		f.archiveDir = cfg.keep.archiveDir
		f.immDir, f.immRoot, err = openImmutableRoot(f.extractDir)
		if err != nil {
			return nil, err
		}
		f.digests = digests
	}
	if f.first > f.last {
		f.empty = true
		f.ctx, f.cancel = context.WithCancelCause(ctx)
		return f, nil
	}
	if err := f.setDownloadClient(cfg, client.aggregatorURL); err != nil {
		return nil, err
	}
	if cfg.keep != nil {
		f.start(ctx, cfg)
		return f, nil
	}

	downloadDir := bcfg.DownloadDir
	if downloadDir == "" {
		downloadDir, err = os.MkdirTemp("", "dingo-mithril-feed-*")
		if err != nil {
			return nil, fmt.Errorf("creating temp directory: %w", err)
		}
		f.tempDir = downloadDir
	}
	ok := false
	defer func() {
		if !ok {
			_ = f.removeFiles()
		}
	}()

	f.digests = digests
	if f.digests == nil {
		f.digests, err = fetchVerifiedDigests(
			ctx, client, f.cfg, artifact, downloadDir,
		)
		// The digest archive is only needed to build the map, and a failed
		// fetch can leave it behind in a caller-supplied directory.
		removeDigestsCache(artifact, downloadDir)
		if err != nil {
			return nil, err
		}
	}

	// A directory unique to this feed, so a second feed on the same artifact
	// and download directory neither shares nor deletes its trios, and so the
	// trios on disk are exactly the ones the window accounts for.
	if err := os.MkdirAll(downloadDir, extractDirMode); err != nil {
		return nil, fmt.Errorf("creating download directory: %w", err)
	}
	f.feedDir, err = os.MkdirTemp(
		downloadDir, chunkFeedDirPrefix+truncateDigest(artifact.Hash)+"-*",
	)
	if err != nil {
		return nil, fmt.Errorf("creating chunk feed directory: %w", err)
	}
	f.extractDir = filepath.Join(f.feedDir, "extract")
	f.archiveDir = filepath.Join(f.feedDir, "archives")
	_, f.immRoot, err = openImmutableRoot(f.extractDir)
	if err != nil {
		return nil, err
	}
	f.imm, err = immutable.NewFromRootVerified(f.immRoot, f.digests, "")
	if err != nil {
		return nil, fmt.Errorf("opening certified ImmutableDB: %w", err)
	}
	ok = true
	f.start(ctx, cfg)
	return f, nil
}

// workerCount is the number of download workers the feed runs.
func (f *ChunkFeed) workerCount(cfg ChunkFeedConfig) uint64 {
	workers := uint64(immutableDownloadWorkers)
	if cfg.Workers > 0 {
		workers = uint64(cfg.Workers)
	}
	if f.window > 0 {
		workers = min(workers, f.window)
	}
	return min(workers, f.last-f.first+1)
}

// setDownloadClient gives the feed the client its downloads use. A kept feed
// is a bootstrap's own immutable download, so it shares the bootstrap's client
// and its archives stay charged to the bootstrap's download budget. Any other
// feed gets its own client, budgeted for its digest list and the chunks it can
// fetch. Such a feed drops the cross-archive extraction budget: its window,
// not a running total, bounds the disk it uses, and a running total would cap
// the history it can stream at the free space it started with.
func (f *ChunkFeed) setDownloadClient(
	cfg ChunkFeedConfig,
	aggregatorURL string,
) error {
	if f.keep != nil && f.cfg.httpClient != nil {
		return nil
	}
	if f.keep == nil {
		f.cfg.extractionBudget = nil
	}
	chunks := f.last - f.first + 1
	immutableBytes, ok := multiplyWithinInt64(
		f.cfg.objectMaxBytes(immutableArchiveMaxBytes), chunks,
	)
	if !ok {
		return errArtifactDownloadBudget
	}
	budget, err := newArtifactDownloadBudget(
		chunks+1,
		immutableBytes,
		f.cfg.objectMaxBytes(digestListMaxBytes),
		0,
	)
	if err != nil {
		return err
	}
	httpClient, transport, err := newArtifactDownloadClient(
		aggregatorURL,
		f.cfg.AllowInsecureHTTP,
		int(f.workerCount(cfg)), // #nosec G115 -- bounded by immutableDownloadWorkers or cfg.Workers
		budget,
	)
	if err != nil {
		return err
	}
	f.cfg.downloadBudget = budget
	f.cfg.httpClient = httpClient
	f.transport = transport
	return nil
}

// start launches the download workers; the caller must then Close the feed.
func (f *ChunkFeed) start(
	ctx context.Context,
	cfg ChunkFeedConfig,
) {
	workers := f.workerCount(cfg)
	f.cfg.immutableDigests = f.digests
	f.inflight, f.weight = immutableDownloadBudget(f.cfg)
	if f.cfg.decoderBudget == nil {
		f.cfg.decoderBudget = newDecoderMemoryBudget()
	}

	feedCtx, cancel := context.WithCancelCause(ctx)
	f.ctx, f.cancel = feedCtx, cancel
	// Blocked workers and consumers wait on the condition variable, which
	// knows nothing about contexts; cancellation has to wake them.
	context.AfterFunc(feedCtx, f.wake)
	f.wg.Add(int(workers)) // #nosec G115 -- bounded by the chunk count
	for range workers {
		go f.worker()
	}
}

func (f *ChunkFeed) wake() {
	f.mu.Lock()
	f.cond.Broadcast()
	f.mu.Unlock()
}

// Target returns the certified target, including the tip once Next has
// returned the last chunk.
func (f *ChunkFeed) Target() CertifiedTarget {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.target
}

// claim blocks until a chunk may start downloading and returns its number.
// It returns false when the worker should exit.
func (f *ChunkFeed) claim() (uint64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for {
		if f.closed || f.halted || f.ctx.Err() != nil ||
			f.nextStart > f.last {
			return 0, false
		}
		if f.window == 0 || f.nextStart-f.lowest < f.window {
			num := f.nextStart
			f.nextStart++
			f.reportOccupancy()
			return num, true
		}
		f.cond.Wait()
	}
}

func (f *ChunkFeed) worker() {
	defer f.wg.Done()
	for {
		num, ok := f.claim()
		if !ok {
			return
		}
		err := f.fetch(num)
		f.mu.Lock()
		switch {
		case f.closed:
			// Nobody will read it, and Close removes the files.
		case err != nil:
			f.failed[num] = err
			f.halted = true
		default:
			f.ready[num] = struct{}{}
		}
		f.cond.Broadcast()
		f.mu.Unlock()
	}
}

// fetch downloads and verifies one chunk's trio. fetchVerifiedImmutableTrio
// verifies it before anything reads it; Next decodes it later.
func (f *ChunkFeed) fetch(num uint64) error {
	bytes, err := f.download(num)
	if err != nil {
		return err
	}
	if f.keep != nil && f.keep.onFetched != nil {
		f.keep.onFetched(bytes)
	}
	return nil
}

// take decodes chunk num, which Next has just claimed, outside f.mu. The
// decode reads the certified bytes back through the digest-checked database,
// so a trio changed on disk since its download is refused here.
func (f *ChunkFeed) take(num uint64) (VerifiedChunk, error) {
	defer f.decoding.Done()
	if f.keep != nil {
		return VerifiedChunk{Num: num}, nil
	}
	chunk, tip, err := f.decode(num)
	f.mu.Lock()
	defer f.mu.Unlock()
	if err != nil {
		removeImmutableTrio(f.immRoot, num)
		err = fmt.Errorf("decoding immutable file %05d: %w", num, err)
		if f.err == nil && !f.closed {
			f.err = err
		}
		f.cond.Broadcast()
		return VerifiedChunk{}, err
	}
	if num == f.last && tip != nil {
		f.target.Tip = *tip
	}
	return chunk, nil
}

// download fetches the trio for num and returns its extracted size.
func (f *ChunkFeed) download(num uint64) (int64, error) {
	if err := f.ctx.Err(); err != nil {
		return 0, err
	}
	archiveErr := &ImmutableArchiveError{
		ArtifactHash:        f.artifact.Hash,
		Epoch:               f.artifact.Beacon.Epoch,
		ImmutableFileNumber: num,
		Locations:           len(f.locations),
	}
	if f.keep != nil {
		bytes, cached, err := reuseCachedImmutableTrio(
			f.cfg.Logger, f.immRoot, num, f.digests, archiveErr,
		)
		if err != nil || cached {
			return bytes, err
		}
	}
	// Each in-flight download is charged at its size limit, as in the
	// bootstrap pool, since the real size is unknown until it arrives.
	if err := f.inflight.Acquire(f.ctx, f.weight); err != nil {
		return 0, err
	}
	defer f.inflight.Release(f.weight)
	// Per-archive extraction is too chatty for the main log at mainnet
	// scale; progress is aggregated by the caller instead.
	bytes, err := fetchVerifiedImmutableTrio(
		f.ctx, f.cfg, slog.New(slog.DiscardHandler), f.locations, num,
		f.archiveDir, f.extractDir, f.immRoot, f.digests, archiveErr,
	)
	if err == nil {
		f.cfg.Metrics.chunkDownloaded()
	}
	return bytes, err
}

func (f *ChunkFeed) decode(
	num uint64,
) (VerifiedChunk, *ocommon.Point, error) {
	iter, err := f.imm.BlocksInChunk(num)
	if err != nil {
		return VerifiedChunk{}, nil, err
	}
	defer iter.Close()
	verifyCfg := lcommon.VerifyConfig{SkipBodyHashValidation: true}
	var blocks []ledger.Block
	var raws []immutable.Block
	var tip *ocommon.Point
	for {
		if err := f.ctx.Err(); err != nil {
			return VerifiedChunk{}, nil, err
		}
		raw, err := iter.Next()
		if err != nil {
			return VerifiedChunk{}, nil, err
		}
		if raw == nil {
			break
		}
		if f.raw {
			raws = append(raws, *raw)
			point := ocommon.NewPoint(raw.Slot, raw.Hash)
			tip = &point
			continue
		}
		block, err := models.DecodeBlockCbor(raw.Type, raw.Cbor, verifyCfg)
		if err != nil {
			return VerifiedChunk{}, nil, fmt.Errorf(
				"decoding block at slot %d: %w", raw.Slot, err,
			)
		}
		blocks = append(blocks, block)
		point := ocommon.NewPoint(raw.Slot, raw.Hash)
		tip = &point
	}
	return VerifiedChunk{Num: num, Blocks: blocks, Raw: raws}, tip, nil
}

// Next returns the next chunk in ascending order, blocking until it has been
// downloaded and verified, and decodes it. A decode failure, a trio changed
// on disk among them, fails the feed. It returns io.EOF after the certified
// target. The
// first chunk failure is returned once every lower chunk has been delivered,
// and is returned by every later call. The consumer must Release the lowest
// outstanding chunk before Next can return the chunk WindowChunks positions
// ahead of it; until then Next blocks until ctx ends. A cancelled ctx fails
// only that call, and does not interrupt a decode already started;
// cancelling the feed's own context fails the feed with its error.
func (f *ChunkFeed) Next(ctx context.Context) (VerifiedChunk, error) {
	stop := context.AfterFunc(ctx, f.wake)
	defer stop()
	f.mu.Lock()
	unlocked := false
	defer func() {
		if !unlocked {
			f.mu.Unlock()
		}
	}()
	for {
		switch {
		case f.closed:
			return VerifiedChunk{}, ErrChunkFeedClosed
		case f.err != nil:
			return VerifiedChunk{}, f.err
		case f.nextOut > f.last:
			return VerifiedChunk{}, io.EOF
		}
		if _, ok := f.ready[f.nextOut]; ok {
			num := f.nextOut
			delete(f.ready, num)
			f.nextOut++
			f.decoding.Add(1)
			unlocked = true
			f.mu.Unlock()
			return f.take(num)
		}
		if err := f.ctx.Err(); err != nil {
			f.err = err
			return VerifiedChunk{}, err
		}
		if err, ok := f.failed[f.nextOut]; ok {
			f.err = err
			return VerifiedChunk{}, err
		}
		if err := ctx.Err(); err != nil {
			return VerifiedChunk{}, err
		}
		f.cond.Wait()
	}
}

// Release deletes the trio for chunk num and lets the window advance past it.
//
// A chunk can be released only after Next has returned it: any other number,
// including one outside the feed's range, fails with ErrChunkNotDelivered. A
// repeat release of a chunk already released is a no-op. Chunks may be
// released in any order; the window advances over the contiguous released
// prefix.
func (f *ChunkFeed) Release(num uint64) error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return ErrChunkFeedClosed
	}
	if num < f.first || num >= f.nextOut {
		f.mu.Unlock()
		return fmt.Errorf("%w: %05d", ErrChunkNotDelivered, num)
	}
	if _, dup := f.released[num]; dup || num < f.lowest {
		f.mu.Unlock()
		return nil
	}
	f.mu.Unlock()
	// The files go first and the window moves only once they are gone: a
	// worker woken by the advance starts a chunk at once, and a delete that
	// failed (or had not happened yet) would then leave one trio too many on
	// disk. A failed delete leaves the chunk unreleased so it can be retried.
	if err := removeImmutableTrioErr(f.immRoot, num); err != nil {
		return fmt.Errorf("removing chunk %05d: %w", num, err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, dup := f.released[num]; dup || num < f.lowest {
		return nil
	}
	f.released[num] = struct{}{}
	for {
		if _, ok := f.released[f.lowest]; !ok {
			break
		}
		delete(f.released, f.lowest)
		f.lowest++
	}
	f.reportOccupancy()
	f.cond.Broadcast()
	return nil
}

// reportOccupancy publishes how many trios the window holds: claimed by a
// worker and not yet released. Only a windowed feed reports it, since an
// unbounded one has no window to fill. Callers hold f.mu.
func (f *ChunkFeed) reportOccupancy() {
	if f.window > 0 {
		f.cfg.Metrics.setWindowOccupancy(f.nextStart - f.lowest)
	}
}

// Close stops the feed, waits for every worker to exit, and removes the
// files it created. It is safe to call more than once and from any
// goroutine; a blocked Next returns ErrChunkFeedClosed.
func (f *ChunkFeed) Close() error {
	f.mu.Lock()
	if f.closed {
		err := f.closeErr
		f.mu.Unlock()
		return err
	}
	f.closed = true
	f.cond.Broadcast()
	f.mu.Unlock()
	f.cancel(ErrChunkFeedClosed)
	f.wg.Wait()
	f.decoding.Wait()
	err := f.removeFiles()
	f.mu.Lock()
	f.closeErr = err
	if f.window > 0 {
		f.cfg.Metrics.setWindowOccupancy(0)
	}
	f.mu.Unlock()
	return err
}

// removeFiles releases the directory handle and deletes everything the feed
// created. It must run only after the workers have stopped.
func (f *ChunkFeed) removeFiles() error {
	var errs []error
	if f.transport != nil {
		f.transport.CloseIdleConnections()
	}
	// Closed first so Windows can remove the tree beneath it.
	if f.immRoot != nil {
		errs = append(errs, f.immRoot.Close())
	}
	for _, dir := range []string{f.feedDir, f.tempDir} {
		if dir != "" {
			errs = append(errs, os.RemoveAll(dir))
		}
	}
	return errors.Join(errs...)
}
