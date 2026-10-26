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

package dingo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database"
	internalconfig "github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/promutil"
	"github.com/blinklabs-io/dingo/plugin"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
)

// TrustedBatchApplier applies certified blocks to the node's ledger for their
// effects. It is the node's ledger state as node-managed Mithril sees it.
type TrustedBatchApplier interface {
	// Tip returns the point of the last applied block, or the zero point
	// for an empty ledger.
	Tip() ocommon.Point
	// ApplyTrustedBatch applies blocks, which extend expected, as one batch
	// and returns the new tip. It returns an error satisfying
	// errors.Is(err, ledger.ErrStaleBatch), and applies nothing, when the
	// tip is no longer expected.
	ApplyTrustedBatch(
		ctx context.Context,
		expected ocommon.Point,
		blocks []gledger.Block,
	) (ocommon.Point, error)
}

// MithrilDecision is what a check weighs to choose a sync source.
type MithrilDecision struct {
	// LocalImmutable is the immutable file holding the ledger tip.
	LocalImmutable uint64
	// TargetImmutable is the aggregator's highest certified immutable file.
	TargetImmutable uint64
	// OuroborosBPS is the blocks per second Ouroboros applied over the last
	// check interval. Zero means unknown.
	OuroborosBPS float64
	// MithrilBPS is the blocks per second the last Mithril catch-up applied.
	// Zero means unknown.
	MithrilBPS float64
	// MinGapChunks is mithril.minGapChunks.
	MinGapChunks uint64
}

// MithrilCatchUp is the Mithril half of node-managed Mithril catch-up: the
// aggregator, the decision rule and the stream runner. The node supplies the
// other half: when to check, holding Ouroboros off the ledger during a run,
// and falling back. The mithril package depends on internal/node, which
// composes this package, so it cannot be imported here, and the composition
// layer passes an implementation through WithMithrilCatchUp.
type MithrilCatchUp interface {
	// SnapshotMode reports whether the node expires history, so a fresh
	// database restores a snapshot rather than streaming from genesis.
	SnapshotMode() bool
	// CertifiedImmutable returns the aggregator's highest certified
	// immutable file number, verified against its certificate chain.
	CertifiedImmutable(ctx context.Context) (uint64, error)
	// ImmutableForSlot returns the immutable file holding slot.
	ImmutableForSlot(slot uint64) uint64
	// UseMithril reports whether d favours a Mithril catch-up.
	UseMithril(d MithrilDecision) bool
	// CatchUp streams the certified immutable files above the applier's
	// tip into it. It prepares unheld (artifact, certificate and digest
	// verification, locating the tip) and then calls hold, once, which holds
	// network ingest and pauses chain ingest so applier is the ledger's only
	// writer; it calls the release hold returns before it returns. It must
	// apply nothing before hold. Every non-nil error means: sync from
	// Ouroboros instead. Batches committed before an error stay applied.
	CatchUp(
		ctx context.Context,
		db *database.Database,
		applier TrustedBatchApplier,
		hold func() (release func()),
	) error
	// SnapshotState classifies db for the restore snapshot mode runs at
	// startup.
	SnapshotState(
		ctx context.Context,
		db *database.Database,
	) (MithrilSnapshotState, error)
	// RestoreSnapshot restores the certified ledger state into db, which
	// SnapshotState reported fresh or interrupted, with the block history
	// history expiry keeps, and returns the restored ledger state's point.
	// Every non-nil error leaves db either still fresh or an interrupted
	// restore that a later call resumes.
	RestoreSnapshot(
		ctx context.Context,
		db *database.Database,
	) (ocommon.Point, error)
	// SnapshotProgress returns a token that changes whenever an
	// interrupted restore in db advances, so an attempt that got further
	// can be told from one that did not.
	SnapshotProgress(ctx context.Context, db *database.Database) (string, error)
}

// ErrMithrilRestoreUnrecoverable is wrapped by a RestoreSnapshot error that
// no retry can clear: the restore cannot be finished on this database.
var ErrMithrilRestoreUnrecoverable = errors.New(
	"mithril snapshot restore cannot be finished",
)

// ErrMithrilRestoreUnavailable is wrapped by a RestoreSnapshot error caused
// by a source the restore reads from (the aggregator, a download location, a
// relay) not answering usefully. It says nothing about the database, so it
// never counts as a resume failing to get further.
var ErrMithrilRestoreUnavailable = errors.New(
	"mithril snapshot restore source unavailable",
)

// MithrilSnapshotState is what snapshot mode does with the database at
// startup.
type MithrilSnapshotState int

const (
	// MithrilSnapshotFresh is an empty database: no sync in progress, no
	// stored block and no ledger tip. Snapshot mode restores into it.
	MithrilSnapshotFresh MithrilSnapshotState = iota
	// MithrilSnapshotInterrupted is a restore of the node's own that did
	// not finish. Snapshot mode resumes it.
	MithrilSnapshotInterrupted
	// MithrilSnapshotPopulated is any other database. Snapshot mode leaves
	// it to stream catch-up and Ouroboros.
	MithrilSnapshotPopulated
)

// MithrilCatchUpParams is what the node knows that a MithrilCatchUp needs.
type MithrilCatchUpParams struct {
	Mithril       internalconfig.MithrilConfig
	HistoryExpiry bool
	// StorageMode is the mode the node runs in, after run-mode overrides
	// such as dev mode forcing API storage.
	StorageMode       StorageMode
	CardanoNodeConfig *cardano.CardanoNodeConfig
	Network           string
	NetworkMagic      uint32
	DataDir           string
	Logger            *slog.Logger
	// PromRegistry is the node's metrics registry, nil when it has none.
	// Collectors registered on it last for the node's lifetime.
	PromRegistry prometheus.Registerer
}

// MithrilCatchUpFactory builds the MithrilCatchUp for a node. It returns nil
// when the configuration leaves Mithril to the manual `dingo mithril sync`.
type MithrilCatchUpFactory func(MithrilCatchUpParams) (MithrilCatchUp, error)

// mithrilLedger is what the coordinator uses of a *ledger.LedgerState.
type mithrilLedger interface {
	Tip() ochainsync.Tip
	HoldNetworkIngest() (release func())
	PauseChainIngest() (resume func())
	DrainTrustedPrune(ctx context.Context) error
	ApplyTrustedBatch(
		ctx context.Context,
		expected ocommon.Point,
		blocks []gledger.Block,
	) (ocommon.Point, error)
}

// ledgerTrustedApplier presents a mithrilLedger as a TrustedBatchApplier.
type ledgerTrustedApplier struct {
	ls mithrilLedger
}

func (a ledgerTrustedApplier) Tip() ocommon.Point {
	return a.ls.Tip().Point
}

func (a ledgerTrustedApplier) ApplyTrustedBatch(
	ctx context.Context,
	expected ocommon.Point,
	blocks []gledger.Block,
) (ocommon.Point, error) {
	return a.ls.ApplyTrustedBatch(ctx, expected, blocks)
}

// mithrilLive is the node's current ledger and database. A live Restore or
// Truncate replaces both, so it is read under liveLifecycleMu and never
// kept past the cycle or run that read it.
type mithrilLive struct {
	ledger mithrilLedger
	db     *database.Database
	// tuning is nil when nothing tunes the database for a run.
	tuning       mithrilCatchUpTuning
	peerTipSlot  uint64
	peerTipKnown bool
}

// minMithrilRateBlocks is the fewest blocks a successful run must apply for
// its rate to replace the measured Mithril rate. A short run is dominated by
// certificate verification and the first download, so its rate says little
// about a long catch-up.
const minMithrilRateBlocks = 1000

// mithrilTipSample is a ledger tip and when it was read.
type mithrilTipSample struct {
	ledger mithrilLedger
	tip    ochainsync.Tip
	at     time.Time
}

// mithrilCoordinator decides, at startup and every check interval, whether
// to catch up from Mithril, and runs the catch-up when it should.
//
// A run prepares while Ouroboros keeps syncing, then is the ledger's single
// writer while it applies: it holds network ingest, so Ouroboros stores
// nothing, then pauses chain ingest, so the ledger's own loop applies
// nothing. Both come back, in reverse order, on every exit. Any failure, a
// held run that stops committing included, falls back to Ouroboros and
// suppresses Mithril for the retry backoff; nothing here stops the node.
type mithrilCoordinator struct {
	source MithrilCatchUp
	cfg    internalconfig.MithrilConfig
	logger *slog.Logger
	now    func() time.Time
	// metrics is nil without a metrics registry.
	metrics *mithrilNodeMetrics
	// lifecycle is the node's liveLifecycleMu. live is called only under it.
	lifecycle sync.Locker
	live      func() mithrilLive

	// runMu guards the run in progress, which abortRun cancels.
	runMu     sync.Mutex
	runCancel context.CancelCauseFunc
	runDone   chan struct{}

	// The rest is touched only by the goroutine calling cycle.
	suppressedUntil time.Time
	mithrilBPS      float64
	sample          mithrilTipSample
}

// run checks once at startup and then every check interval until ctx ends.
func (c *mithrilCoordinator) run(ctx context.Context) {
	c.cycle(ctx)
	ticker := time.NewTicker(c.cfg.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.cycle(ctx)
		}
	}
}

// cycle makes one source decision and, when Mithril wins, one catch-up.
func (c *mithrilCoordinator) cycle(ctx context.Context) {
	now := c.now()
	if now.Before(c.suppressedUntil) {
		return
	}
	c.lifecycle.Lock()
	if ctx.Err() != nil {
		c.lifecycle.Unlock()
		return
	}
	live := c.live()
	var tip ochainsync.Tip
	if live.ledger != nil {
		tip = live.ledger.Tip()
	}
	c.lifecycle.Unlock()
	if live.ledger == nil {
		return
	}
	ouroborosBPS := c.ouroborosRate(live.ledger, tip, now)

	// The probe runs before anything is held, so an unreachable aggregator
	// costs Ouroboros nothing.
	target, err := c.source.CertifiedImmutable(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		c.fallback(
			mithrilFallbackProbe, "probing the certified Mithril target", err,
		)
		return
	}
	d := MithrilDecision{
		LocalImmutable:  c.source.ImmutableForSlot(tip.Point.Slot),
		TargetImmutable: target,
		OuroborosBPS:    ouroborosBPS,
		MithrilBPS:      c.mithrilBPS,
		MinGapChunks:    c.cfg.MinGapChunks,
	}
	useMithril := c.source.UseMithril(d)
	source := mithrilSourceOuroboros
	if useMithril {
		source = mithrilSourceMithril
	}
	attrs := []any{
		"component", "mithril",
		"source", source,
		"local_immutable", d.LocalImmutable,
		"target_immutable", d.TargetImmutable,
		"ouroboros_bps", d.OuroborosBPS,
		"mithril_bps", d.MithrilBPS,
		"min_gap_chunks", d.MinGapChunks,
	}
	if live.peerTipKnown {
		attrs = append(attrs, "peer_tip_slot", live.peerTipSlot)
	}
	c.metrics.decision(source)
	// A caught-up node makes this decision every check; only a gap worth
	// weighing is worth the Info line.
	level := slog.LevelInfo
	if !useMithril &&
		d.TargetImmutable < d.LocalImmutable+max(d.MinGapChunks, 1) {
		level = slog.LevelDebug
	}
	c.logger.Log(ctx, level, "mithril: sync source decision", attrs...)
	if !useMithril {
		return
	}
	gap := d.TargetImmutable - min(d.LocalImmutable, d.TargetImmutable)
	largeGap := gap >= mithrilTunedCatchUpMinChunks
	c.catchUp(ctx, live.ledger, mithrilTuningScope{
		largeGap: largeGap,
		catchUpIndexes: largeGap && mithrilCatchUpIndexesWorthDropping(
			gap, d.LocalImmutable, tip.BlockNumber,
		),
	})
}

// ouroborosRate returns the blocks per second the ledger tip moved since the
// previous check, or zero when that is unknown: on the first check, after a
// Mithril run, or on a ledger a live restore rebuilt.
func (c *mithrilCoordinator) ouroborosRate(
	ls mithrilLedger,
	tip ochainsync.Tip,
	now time.Time,
) float64 {
	prev := c.sample
	c.sample = mithrilTipSample{ledger: ls, tip: tip, at: now}
	if prev.ledger != ls || !now.After(prev.at) ||
		tip.BlockNumber <= prev.tip.BlockNumber {
		return 0
	}
	return float64(tip.BlockNumber-prev.tip.BlockNumber) /
		now.Sub(prev.at).Seconds()
}

// catchUp runs one Mithril catch-up of gap immutable files on expect, the
// ledger the decision was made on.
func (c *mithrilCoordinator) catchUp(
	ctx context.Context,
	expect mithrilLedger,
	scope mithrilTuningScope,
) {
	runCtx, live, finish, ok := c.beginRun(ctx, expect)
	if !ok {
		return
	}
	defer finish()
	run, err := c.stream(runCtx, ctx, live, scope)
	applied := run.after.BlockNumber - min(
		run.before.BlockNumber, run.after.BlockNumber,
	)
	switch {
	case err == nil:
		if applied >= minMithrilRateBlocks && run.end.After(run.start) {
			c.mithrilBPS = float64(applied) /
				run.end.Sub(run.start).Seconds()
		}
		// Blocks a run applied are not Ouroboros', so the next rate
		// starts here.
		c.sample = mithrilTipSample{
			ledger: live.ledger, tip: run.after, at: run.end,
		}
		c.logger.Info(
			"mithril: catch-up complete, handing back to Ouroboros",
			"component", "mithril",
			"tip_slot", run.after.Point.Slot,
			"blocks", applied,
			"mithril_bps", c.mithrilBPS,
		)
	case ctx.Err() != nil:
		c.sample = mithrilTipSample{}
	case runCtx.Err() != nil:
		// A live Restore or Truncate aborted the run to rebuild the
		// ledger. Mithril did not fail, so the next check may run again,
		// on the rebuilt ledger. The sample would pin the old ledger.
		c.sample = mithrilTipSample{}
		c.logger.Info(
			"mithril: catch-up aborted for a database lifecycle operation",
			"component", "mithril",
			"tip_slot", run.after.Point.Slot,
		)
	default:
		c.sample = mithrilTipSample{
			ledger: live.ledger, tip: run.after, at: run.end,
		}
		c.fallback(mithrilFallbackCatchUp, "catching up from Mithril", err)
	}
}

// stream runs the catch-up, handing it the hold it takes once prepared: hold
// network ingest, then pause chain ingest, then tune the database. The
// release undoes the hold in reverse; it also runs when the catch-up returns
// without calling it, a panic included.
//
// The release first rebuilds the catch-up droppable indexes, because the
// rollback and chain selection Ouroboros resumes read them. It does so on
// nodeCtx, not ctx, so a run that failed or stalled still rebuilds them, and
// only a node shutdown cancels the rebuild, leaving it to startup repair. A
// run a live Restore or Truncate aborted (errMithrilLifecycleAbort) skips
// it, or stops it if the abort arrives mid-rebuild, and the operation's
// reinitialization repairs them before chain ingest.
//
// Then, on the same context, it drains the consumed-UTxO cleanup the trusted
// batches still owe (DrainTrustedPrune), which batches run only every few
// batches, so Ouroboros resumes without that backlog. A lifecycle abort skips
// or stops the drain as it does the rebuild: the operation replaces or
// rewrites the database the drain would prune.
//
// The run's end is stamped before the rebuild, so the Mithril rate is the
// rate of applying, not of rebuilding. The rest of the tuning is undone
// after the release, so the lazy rebuild holds neither Ouroboros nor the
// stream's no-progress timeout.
func (c *mithrilCoordinator) stream(
	ctx context.Context,
	nodeCtx context.Context,
	live mithrilLive,
	scope mithrilTuningScope,
) (mithrilRun, error) {
	var (
		run     mithrilRun
		held    bool
		release = func() {}
		tuned   mithrilTuned
	)
	hold := func() func() {
		if held {
			return release
		}
		held = true
		releaseIngest := live.ledger.HoldNetworkIngest()
		resume := live.ledger.PauseChainIngest()
		release = sync.OnceFunc(func() {
			defer releaseIngest()
			defer resume()
			run.end = c.now()
			if errors.Is(context.Cause(ctx), errMithrilLifecycleAbort) {
				if tuned.restore != nil {
					c.logger.Info(
						"mithril: skipping the catch-up index rebuild for a "+
							"database lifecycle operation; it repairs them "+
							"before chain ingest",
						"component", "mithril",
					)
				}
				return
			}
			// A lifecycle abort that arrives during the rebuild or the
			// drain stops it too, so abortRun never waits one out.
			releaseCtx, stopRelease := context.WithCancelCause(nodeCtx)
			defer stopRelease(nil)
			stopWatch := context.AfterFunc(ctx, func() {
				if cause := context.Cause(ctx); errors.Is(
					cause, errMithrilLifecycleAbort,
				) {
					stopRelease(cause)
				}
			})
			defer stopWatch()
			if tuned.restore != nil {
				tuned.restore(releaseCtx)
			}
			// Ouroboros resumes on a ledger with no consumed-UTxO cleanup
			// still owed by the trusted batches, as when every batch ran
			// it.
			if err := live.ledger.DrainTrustedPrune(
				releaseCtx,
			); err != nil && releaseCtx.Err() == nil {
				c.logger.Warn(
					"mithril: consumed UTxO cleanup after catch-up failed; "+
						"the periodic cleanup reclaims the rows",
					"component", "mithril",
					"error", err,
				)
			}
		})
		if live.tuning != nil {
			tuned = live.tuning.begin(ctx, scope)
		}
		// Timed from here: preparing, and waiting for the hold and the
		// pause, is not Mithril's throughput.
		run.start, run.before = c.now(), live.ledger.Tip()
		return release
	}
	defer func() {
		release()
		if tuned.end != nil {
			tuned.end(ctx)
		}
	}()
	err := c.source.CatchUp(
		ctx, live.db, ledgerTrustedApplier{ls: live.ledger}, hold,
	)
	run.after = live.ledger.Tip()
	if run.end.IsZero() {
		run.end = c.now()
	}
	if !held {
		// Nothing was applied; the tip moved only by Ouroboros.
		run.start, run.before = run.end, run.after
	}
	return run, err
}

// mithrilRun is the ledger tip and clock around one catch-up.
type mithrilRun struct {
	start, end    time.Time
	before, after ochainsync.Tip
}

// beginRun registers a run under the lifecycle lock, so a Restore or
// Truncate that takes the lock afterwards finds it to abort, and one that
// held the lock first has already replaced the ledger, which is then not
// expect and the run is skipped.
func (c *mithrilCoordinator) beginRun(
	ctx context.Context,
	expect mithrilLedger,
) (context.Context, mithrilLive, func(), bool) {
	c.lifecycle.Lock()
	defer c.lifecycle.Unlock()
	if ctx.Err() != nil {
		return nil, mithrilLive{}, nil, false
	}
	live := c.live()
	if live.ledger == nil || live.ledger != expect {
		return nil, mithrilLive{}, nil, false
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	c.runMu.Lock()
	c.runCancel, c.runDone = cancel, done
	c.runMu.Unlock()
	return runCtx, live, func() {
		cancel(nil)
		c.runMu.Lock()
		c.runCancel, c.runDone = nil, nil
		c.runMu.Unlock()
		close(done)
	}, true
}

// errMithrilLifecycleAbort is the cause a live Restore or Truncate cancels a
// run with. Such a run skips the in-hold index rebuild: the operation is
// about to replace or rewrite the database under it, and its
// reinitialization repairs the critical indexes of whatever database it
// resumes on before chain ingest, so nothing multi-minute sits inside the
// operation's bounded quiesce.
var errMithrilLifecycleAbort = errors.New(
	"mithril: catch-up aborted for a database lifecycle operation",
)

// abortRun cancels the run in progress, if any, and waits until it has
// released network ingest and resumed chain ingest. Called with
// liveLifecycleMu held, it leaves no run on the ledger about to be replaced,
// and none can start until the lock is released.
func (c *mithrilCoordinator) abortRun() {
	c.runMu.Lock()
	cancel, done := c.runCancel, c.runDone
	c.runMu.Unlock()
	if cancel == nil {
		return
	}
	cancel(errMithrilLifecycleAbort)
	<-done
}

// fallback logs a Mithril failure and suppresses Mithril for the retry
// backoff. Ouroboros is already syncing: nothing is held at this point.
// reason is the bounded label it is counted under.
func (c *mithrilCoordinator) fallback(reason, stage string, err error) {
	c.metrics.fallback(reason)
	c.suppressedUntil = c.now().Add(c.cfg.RetryBackoff)
	c.logger.Warn(
		"mithril: "+stage+" failed, falling back to Ouroboros",
		"component", "mithril",
		"error", err,
		"retry_after", c.cfg.RetryBackoff,
	)
}

// newMithrilSource builds the Mithril half of node-managed Mithril, or
// returns nil when the configuration does not select it. A configuration the
// Mithril half rejects is logged and leaves the node on Ouroboros alone.
func (n *Node) newMithrilSource() MithrilCatchUp {
	factory := n.config.mithrilCatchUp
	if factory == nil {
		return nil
	}
	storageMode := n.config.storageMode
	if storageMode == "" {
		storageMode = StorageModeCore
	}
	source, err := factory(MithrilCatchUpParams{
		Mithril:           n.config.cfg.Mithril,
		HistoryExpiry:     n.config.historyExpiry.Enabled,
		CardanoNodeConfig: n.config.cardanoNodeConfig,
		StorageMode:       storageMode,
		Network:           n.config.network,
		NetworkMagic:      n.config.networkMagic,
		DataDir:           n.config.dataDir,
		Logger:            n.config.logger,
		// The source outlives every live Restore or Truncate rebuild, so
		// its metrics must not be in the set those rebuilds unregister.
		PromRegistry: n.retainedComponentPromRegistry(),
	})
	if err != nil {
		n.mithrilMetrics.fallback(mithrilFallbackConfig)
		n.config.logger.Error(
			"mithril: node-managed catch-up disabled, syncing from Ouroboros",
			"component", "mithril",
			"error", err,
		)
		return nil
	}
	return source
}

// startMithrilCoordinator starts node-managed Mithril catch-up on the source
// newMithrilSource built, if any. stop cancels the coordinator and waits for
// it to exit; it must not be called with liveLifecycleMu held.
func (n *Node) startMithrilCoordinator(ctx context.Context) (stop func()) {
	stop = func() {}
	source := n.mithrilSource
	if source == nil {
		return stop
	}
	c := &mithrilCoordinator{
		source:          source,
		cfg:             n.config.cfg.Mithril,
		logger:          n.config.logger,
		now:             time.Now,
		metrics:         n.mithrilMetrics,
		lifecycle:       &n.liveLifecycleMu,
		live:            n.mithrilLive,
		suppressedUntil: n.mithrilSuppressedUntil,
	}
	n.liveLifecycleMu.Lock()
	n.mithrilCoordinator = c
	n.liveLifecycleMu.Unlock()
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.run(loopCtx)
	}()
	return func() {
		cancel()
		<-done
	}
}

// maxSnapshotRestoreAttempts is how many attempts in a row the startup
// restore may fail after it began writing without getting any further, per
// process start, before the node stops retrying. An attempt that failed
// because a source was unavailable (ErrMithrilRestoreUnavailable) is not
// counted: an aggregator outage is retried after the backoff for as long as
// it lasts. An attempt that advanced
// the restore, by phase, stored blocks or ledger import checkpoint, starts
// the count again. Only a failure that leaves the database fresh falls back
// to Ouroboros; the rest leave a partly restored database, so a restore that
// keeps failing on it in the same place has to end the startup rather than
// hold the node off-live with nothing to show for it.
const maxSnapshotRestoreAttempts = 5

// freshSnapshotRestoreRetries and freshSnapshotRestoreBackoff bound how long
// the startup restore keeps retrying a fresh database whose restore failed
// because a source was unavailable, before falling back to Ouroboros. Falling
// back is costly for good: once Ouroboros stores a block the database is no
// longer fresh, so snapshot mode never restores it, and the node instead
// downloads and replays the whole history that history expiry exists to
// skip. A few minutes of retries ride out an aggregator blip without holding
// a node whose aggregator is really down off the network for long.
const (
	freshSnapshotRestoreRetries = 3
	freshSnapshotRestoreBackoff = time.Minute
)

// Values of the dingo_mithril_snapshot_restore_state gauge.
const (
	mithrilRestoreStateIdle      = 0
	mithrilRestoreStateRestoring = 1
	mithrilRestoreStateStuck     = 2
	// mithrilRestoreStateWaiting is a resume backing off because the
	// Mithril source did not answer usefully.
	mithrilRestoreStateWaiting = 3
)

// mithrilRestoreStatus is the startup snapshot restore's progress as the
// readiness probe and the restore-state gauge report it.
type mithrilRestoreStatus struct {
	// failures counts the attempts in a row that failed after writing
	// without advancing the restore. Non-zero means stuck.
	failures int
	// waiting is set while the loop backs off after a source-unavailable
	// failure that made no progress. Such failures never count toward
	// failures, so this is the only mark of a resume the source holds up.
	waiting bool
	lastErr string
	// nextAttempt is when the loop tries again; zero when not backing off.
	nextAttempt time.Time
}

// errMithrilRestoreInProgress and errMithrilRestoreStuck are the readiness
// answers while the startup restore runs, and once it has failed without
// advancing and is retrying.
var (
	errMithrilRestoreInProgress = errors.New(
		"mithril snapshot restore in progress",
	)
	errMithrilRestoreStuck   = errors.New("mithril snapshot restore stuck")
	errMithrilRestoreWaiting = errors.New(
		"mithril snapshot restore waiting on source",
	)
)

// retryDetail renders the last error and the time of the next attempt.
func (st *mithrilRestoreStatus) retryDetail() string {
	detail := "last: " + st.lastErr
	if !st.nextAttempt.IsZero() {
		detail += ", next attempt at " + st.nextAttempt.UTC().Format(time.RFC3339)
	}
	return detail
}

// MithrilSnapshotRestoreReady reports the startup snapshot restore holding
// the node off-live, or nil when none is running. It tells a restore at work
// from one that keeps failing without advancing.
func (n *Node) MithrilSnapshotRestoreReady() error {
	status := n.mithrilRestore.Load()
	if status == nil {
		return nil
	}
	if status.waiting {
		return fmt.Errorf(
			"%w: %s", errMithrilRestoreWaiting, status.retryDetail(),
		)
	}
	if status.failures == 0 {
		return errMithrilRestoreInProgress
	}
	return fmt.Errorf(
		"%w: %d of %d attempts failed without progress, %s",
		errMithrilRestoreStuck, status.failures,
		maxSnapshotRestoreAttempts, status.retryDetail(),
	)
}

// reportMithrilRestore publishes the startup restore's status to the
// readiness probe and the restore-state gauge; nil means no restore runs.
func (n *Node) reportMithrilRestore(status *mithrilRestoreStatus) {
	n.mithrilRestore.Store(status)
	state := mithrilRestoreStateIdle
	switch {
	case status == nil:
	case status.waiting:
		state = mithrilRestoreStateWaiting
	case status.failures == 0:
		state = mithrilRestoreStateRestoring
	default:
		state = mithrilRestoreStateStuck
	}
	if n.mithrilRestoreGauge != nil {
		n.mithrilRestoreGauge.Set(float64(state))
	}
}

// mithrilRestoreRecovery is the operator's way out of a restore the node
// cannot finish. The fresh-database check held before the restore's first
// write, so every row in the node's storage is the restore's own. The local
// plugins keep their stores under the data directory; a remote one keeps
// them where its own configuration points, which removing the data directory
// does not reach.
func mithrilRestoreRecovery(dataDir, metadataPlugin, blobPlugin string) string {
	local := func(plugin, def string) bool {
		return plugin == "" || plugin == def
	}
	if local(metadataPlugin, "sqlite") && local(blobPlugin, "badger") {
		return "every row in the database was written by the restore, so " +
			"remove the data directory " + dataDir + " and start the node " +
			"again to restore afresh"
	}
	stores := make([]string, 0, 2)
	if !local(metadataPlugin, "sqlite") {
		stores = append(stores, "drop every table of the "+metadataPlugin+
			" metadata database the node is configured with")
	}
	if !local(blobPlugin, "badger") {
		stores = append(stores, "delete every object in the "+blobPlugin+
			" blob store bucket and prefix the node is configured with")
	}
	return "every row in the node's storage was written by the restore, " +
		"so " + strings.Join(stores, ", ") + ", remove the data directory " +
		dataDir + ", and start the node again to restore afresh"
}

// checkSnapshotRestoreSettings refuses to start, before the node settings
// gate persists anything, on the node's own interrupted snapshot restore
// with the CIP-0163 delegator-inactivity gate on. The restore cannot finish
// with it on, and the gate latches once enforced, so refusing here leaves
// turning it back off possible. This is a settings conflict, not a Mithril
// failure.
func (n *Node) checkSnapshotRestoreSettings(ctx context.Context) error {
	if n.mithrilSource == nil || !n.mithrilSource.SnapshotMode() ||
		!n.config.delegatorInactivityEnabled {
		return nil
	}
	state, err := n.mithrilSource.SnapshotState(ctx, n.db)
	if err != nil {
		return fmt.Errorf("mithril: reading snapshot restore state: %w", err)
	}
	if state != MithrilSnapshotInterrupted {
		return nil
	}
	return fmt.Errorf(
		"node settings: the database holds the node's own interrupted "+
			"Mithril snapshot restore, which cannot be finished with "+
			"delegatorInactivityEnabled (CIP-0163) set; start the node "+
			"with it off to finish the restore (a restored database "+
			"refuses the gate afterwards, as any Mithril-bootstrapped one "+
			"does), or remove the data directory %s to sync from genesis "+
			"with it on",
		n.config.dataDir,
	)
}

// mithrilSnapshotStartup runs snapshot mode's startup restore on the node's
// database. It is called before the chain manager and the ledger state are
// built, so they are built on whatever the restore leaves.
func (n *Node) mithrilSnapshotStartup(ctx context.Context) error {
	if n.mithrilSource == nil || !n.mithrilSource.SnapshotMode() {
		return nil
	}
	s := &mithrilSnapshotStartup{
		source:  n.mithrilSource,
		db:      n.db,
		dataDir: n.config.dataDir,
		metadataPlugin: n.config.pluginSelections[plugin.CapabilityStorageMetadata].
			Provider,
		blobPlugin: n.config.pluginSelections[plugin.CapabilityStorageBlob].
			Provider,
		backoff:             n.config.cfg.Mithril.RetryBackoff,
		delegatorInactivity: n.config.delegatorInactivityEnabled,
		logger:              n.config.logger,
		now:                 time.Now,
		wait:                waitContext,
		report:              n.reportMithrilRestore,
		metrics:             n.mithrilMetrics,
	}
	defer n.reportMithrilRestore(nil)
	until, err := s.run(ctx)
	n.mithrilSuppressedUntil = until
	return err
}

// mithrilSnapshotStartup restores a snapshot into an empty database, or
// resumes the node's own interrupted restore, before the node goes live.
//
// A restore that fails before writing anything leaves the database empty. A
// source being unavailable is retried freshSnapshotRestoreRetries times,
// freshSnapshotRestoreBackoff apart, first; then, or at once for any other
// failure, the node syncs the database from genesis over Ouroboros, and the
// coordinator holds off for the retry backoff as after any Mithril failure. A restore that
// fails after writing leaves a resumable, partly restored database that
// Ouroboros cannot extend from genesis and no existing machinery can empty
// again, so it is resumed after the retry backoff instead, up to
// maxSnapshotRestoreAttempts attempts in a row that get no further, and then
// fails the startup with the recovery spelled out. So does a restore the
// source reports unrecoverable, at once. The readiness probe tells the two waits apart.
//
// A fresh database is not restored with the CIP-0163 delegator-inactivity
// gate on, the same refusal `dingo mithril sync` makes: the snapshot carries
// no reward-account expiration state. The node syncs from genesis instead.
type mithrilSnapshotStartup struct {
	source  MithrilCatchUp
	db      *database.Database
	dataDir string
	// metadataPlugin and blobPlugin name the storage providers, for the
	// recovery advice.
	metadataPlugin      string
	blobPlugin          string
	backoff             time.Duration
	delegatorInactivity bool
	logger              *slog.Logger
	now                 func() time.Time
	wait                func(context.Context, time.Duration) error
	report              func(*mithrilRestoreStatus)
	metrics             *mithrilNodeMetrics
}

// run returns when the database may be opened by the rest of the node, and
// the time the coordinator should hold off until. An error means the node
// cannot start on this database.
func (s *mithrilSnapshotStartup) run(
	ctx context.Context,
) (suppressUntil time.Time, err error) {
	failures := 0
	freshRetries := 0
	// waiting carries the source-unavailable status across the next
	// attempt, so the gauge does not flap back to restoring until an
	// attempt gets further.
	waiting := false
	for {
		state, err := s.source.SnapshotState(ctx, s.db)
		if err != nil {
			return time.Time{}, fmt.Errorf(
				"mithril: reading snapshot restore state: %w", err,
			)
		}
		switch state {
		case MithrilSnapshotPopulated:
			return time.Time{}, nil
		case MithrilSnapshotFresh:
			if s.delegatorInactivity {
				s.logger.Warn(
					"mithril: not restoring a snapshot with "+
						"delegatorInactivityEnabled (CIP-0163) set; "+
						"syncing from genesis",
					"component", "mithril",
				)
				return time.Time{}, nil
			}
		case MithrilSnapshotInterrupted:
		}
		s.logger.Info(
			"mithril: restoring a snapshot before going live",
			"component", "mithril",
			"resume", state == MithrilSnapshotInterrupted,
		)
		if failures == 0 && !waiting {
			s.report(&mithrilRestoreStatus{})
		}
		before, beforeErr := s.source.SnapshotProgress(ctx, s.db)
		tip, err := s.source.RestoreSnapshot(ctx, s.db)
		if err == nil {
			s.logger.Info(
				"mithril: snapshot restored, going live",
				"component", "mithril",
				"ledger_slot", tip.Slot,
			)
			return time.Time{}, nil
		}
		if ctx.Err() != nil {
			return time.Time{}, fmt.Errorf(
				"mithril: snapshot restore: %w", ctx.Err(),
			)
		}
		if errors.Is(err, ErrMithrilRestoreUnrecoverable) {
			return time.Time{}, fmt.Errorf(
				"mithril: snapshot restore: %w; %s",
				err, s.recovery(),
			)
		}
		after, stateErr := s.source.SnapshotState(ctx, s.db)
		if stateErr == nil && after == MithrilSnapshotFresh {
			if errors.Is(err, ErrMithrilRestoreUnavailable) &&
				freshRetries < freshSnapshotRestoreRetries {
				freshRetries++
				s.logger.Warn(
					"mithril: snapshot restore failed before writing, "+
						"retrying before falling back to Ouroboros",
					"component", "mithril",
					"error", err,
					"retry", freshRetries,
					"of", freshSnapshotRestoreRetries,
					"retry_after", freshSnapshotRestoreBackoff,
				)
				if err := s.wait(ctx, freshSnapshotRestoreBackoff); err != nil {
					return time.Time{}, fmt.Errorf(
						"mithril: snapshot restore: %w", err,
					)
				}
				continue
			}
			s.logger.Warn(
				"mithril: snapshot restore failed, falling back to "+
					"Ouroboros from genesis; this abandons snapshot mode "+
					"for this database, which will download and replay "+
					"the full chain history",
				"component", "mithril",
				"error", err,
				"retry_after", s.backoff,
			)
			s.metrics.fallback(mithrilFallbackSnapshotRestore)
			return s.now().Add(s.backoff), nil
		}
		progressed := false
		if beforeErr == nil {
			afterProgress, afterErr := s.source.SnapshotProgress(ctx, s.db)
			progressed = afterErr == nil && afterProgress != before
		}
		unavailable := errors.Is(err, ErrMithrilRestoreUnavailable)
		switch {
		case progressed:
			failures = 0
		case unavailable:
			// The aggregator did not answer usefully; the database is no
			// more stuck than before.
		default:
			failures++
		}
		waiting = unavailable && !progressed
		s.report(&mithrilRestoreStatus{
			failures:    failures,
			waiting:     waiting,
			lastErr:     internalconfig.RedactCredentialsInText(err.Error()),
			nextAttempt: s.now().Add(s.backoff),
		})
		if failures >= maxSnapshotRestoreAttempts {
			return time.Time{}, fmt.Errorf(
				"mithril: snapshot restore failed %d times in a row after "+
					"it began writing without getting further, last: %w; %s",
				failures, err, s.recovery(),
			)
		}
		if stateErr != nil {
			s.logger.Error(
				"mithril: snapshot restore failed and the database state "+
					"is unknown; retrying after the retry backoff",
				"component", "mithril",
				"error", err,
				"state_error", stateErr,
				"failures_without_progress", failures,
				"retry_after", s.backoff,
			)
		} else {
			s.logger.Error(
				"mithril: snapshot restore failed after writing; resuming "+
					"it after the retry backoff",
				"component", "mithril",
				"error", err,
				"progressed", progressed,
				"source_unavailable", unavailable,
				"failures_without_progress", failures,
				"retry_after", s.backoff,
			)
		}
		if err := s.wait(ctx, s.backoff); err != nil {
			return time.Time{}, fmt.Errorf("mithril: snapshot restore: %w", err)
		}
	}
}

func (s *mithrilSnapshotStartup) recovery() string {
	return mithrilRestoreRecovery(s.dataDir, s.metadataPlugin, s.blobPlugin)
}

// waitContext waits for d or until ctx ends, and returns ctx's error if it
// ended first.
func waitContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// mithrilLive returns the node's current ledger and database. Callers hold
// liveLifecycleMu.
func (n *Node) mithrilLive() mithrilLive {
	if n.ledgerState == nil || n.db == nil {
		return mithrilLive{}
	}
	live := mithrilLive{
		ledger: n.ledgerState,
		db:     n.db,
		tuning: nodeMithrilTuning{
			db:      n.db,
			planner: n.plannerStatsMgr,
			logger:  n.config.logger,
		},
	}
	if n.chainsyncState != nil {
		live.peerTipSlot, live.peerTipKnown = n.chainsyncState.PeerTipSlot()
	}
	return live
}

// abortMithrilRun aborts a node-managed Mithril run in progress, for a live
// Restore, Truncate or shutdown, which hold liveLifecycleMu.
func (n *Node) abortMithrilRun() {
	if n.mithrilCoordinator != nil {
		n.mithrilCoordinator.abortRun()
	}
}

// Label values of dingo_mithril_source_decisions_total and
// dingo_mithril_fallbacks_total. Both label sets are fixed here so the series
// count stays bounded.
const (
	mithrilSourceMithril   = "mithril"
	mithrilSourceOuroboros = "ouroboros"

	// mithrilFallbackConfig: the Mithril half rejected the configuration.
	mithrilFallbackConfig = "config"
	// mithrilFallbackProbe: the certified target could not be read.
	mithrilFallbackProbe = "probe"
	// mithrilFallbackCatchUp: a stream catch-up run failed.
	mithrilFallbackCatchUp = "catch_up"
	// mithrilFallbackSnapshotRestore: a startup snapshot restore failed
	// before writing, so the node syncs from genesis.
	mithrilFallbackSnapshotRestore = "snapshot_restore"
)

// mithrilNodeMetrics counts the coordinator's source decisions and its
// fallbacks to Ouroboros. Its methods are safe on nil and then record
// nothing.
type mithrilNodeMetrics struct {
	decisions *prometheus.CounterVec
	fallbacks *prometheus.CounterVec
}

func newMithrilNodeMetrics(r *promutil.Registration) *mithrilNodeMetrics {
	m := &mithrilNodeMetrics{
		decisions: promutil.Register(r, prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dingo_mithril_source_decisions_total",
				Help: "Node-managed Mithril sync source decisions, by the " +
					"source chosen.",
			},
			[]string{"source"},
		)),
		fallbacks: promutil.Register(r, prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dingo_mithril_fallbacks_total",
				Help: "Node-managed Mithril failures that left the node " +
					"syncing from Ouroboros, by reason.",
			},
			[]string{"reason"},
		)),
	}
	// Every series exists from the start, at zero, so a rate over it is
	// defined before the first event.
	for _, source := range []string{
		mithrilSourceMithril, mithrilSourceOuroboros,
	} {
		m.decisions.WithLabelValues(source)
	}
	for _, reason := range []string{
		mithrilFallbackConfig, mithrilFallbackProbe,
		mithrilFallbackCatchUp, mithrilFallbackSnapshotRestore,
	} {
		m.fallbacks.WithLabelValues(reason)
	}
	return m
}

func (m *mithrilNodeMetrics) decision(source string) {
	if m == nil {
		return
	}
	m.decisions.WithLabelValues(source).Inc()
}

func (m *mithrilNodeMetrics) fallback(reason string) {
	if m == nil {
		return
	}
	m.fallbacks.WithLabelValues(reason).Inc()
}
