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

package nodeparity

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
)

// FullCheckReason is a stable, low-cardinality label for why an incremental
// run fell back to a full Check -- suitable for a Prometheus label, unlike a
// free-text log message.
type FullCheckReason string

const (
	FullCheckStartup         FullCheckReason = "startup"
	FullCheckInterval        FullCheckReason = "interval"
	FullCheckEpochTransition FullCheckReason = "epoch_transition"
	FullCheckRollback        FullCheckReason = "rollback"
	FullCheckMismatch        FullCheckReason = "mismatch"
)

// IncrementalCursor is the sequential-mode persisted state: the last block
// both nodes were confirmed (by a full or incremental comparison) to agree
// on, plus enough bookkeeping to decide when the next full checkpoint is
// due. Persisted as plain JSON via LoadCursor/SaveCursor so a process
// restart resumes from here rather than needing a fresh full baseline every
// time -- though RunIncremental always re-validates with one full Check on
// startup regardless (see its doc comment), so a stale or corrupt cursor
// file can never be silently trusted past that point without at least an
// attempt to confirm its point is still reachable (see
// buildStartupCursor/pointReachable); a point that is no longer reachable
// falls back to a fresh baseline rather than being trusted.
type IncrementalCursor struct {
	Tip                  Tip    `json:"tip"`
	Epoch                int    `json:"epoch"`
	BlocksSinceFullCheck uint64 `json:"blocksSinceFullCheck"`
}

// LoadCursor reads a persisted IncrementalCursor from path. A missing file is
// not an error -- it returns (nil, nil), the expected shape for "no prior
// run", so the caller can distinguish that from a real read/parse failure
// and fall back to a fresh full baseline only in the former case.
func LoadCursor(path string) (*IncrementalCursor, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read cursor file %s: %w", path, err)
	}
	var cursor IncrementalCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return nil, fmt.Errorf("parse cursor file %s: %w", path, err)
	}
	return &cursor, nil
}

// SaveCursor persists cursor to path as JSON, via a temp-file-plus-rename so
// a crash or a concurrent read never observes a half-written file -- this
// runs once per block in the steady state, so a torn write on every process
// kill is not a theoretical concern.
func SaveCursor(path string, cursor *IncrementalCursor) error {
	data, err := json.MarshalIndent(cursor, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal cursor: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write cursor temp file %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename cursor file %s: %w", path, err)
	}
	return nil
}

// cursorState synchronizes access to one IncrementalCursor across the two
// goroutines that read and mutate it concurrently once a full check runs in
// the background (see fullCheckWorker): the ChainSync callback goroutine
// (advancing the cursor block by block, or resetting it on a rollback) and
// the full-check worker goroutine (reading a snapshot to check against, and
// clearing BlocksSinceFullCheck once its check completes). Every method
// persists the result under the same lock it mutates under, so a reader
// (LoadCursor, or an operator inspecting the file) never observes a state
// change without also observing its corresponding write to disk.
type cursorState struct {
	mu   sync.Mutex
	path string
	cur  IncrementalCursor
}

func newCursorState(path string, initial IncrementalCursor) *cursorState {
	return &cursorState{path: path, cur: initial}
}

// snapshot returns a copy of the current cursor, safe to read or pass
// around without further locking.
func (s *cursorState) snapshot() IncrementalCursor {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur
}

// advance records a newly-validated block's point (and its epoch, when
// known -- epoch is left unchanged when negative, queryIncrementalHalf's
// signal for "the epoch lookup itself failed"), increments
// BlocksSinceFullCheck, persists the result, and returns the post-update
// snapshot so the caller can decide whether a checkpoint is due without a
// second lock round trip.
func (s *cursorState) advance(tip Tip, epoch int) (IncrementalCursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur.Tip = tip
	s.cur.BlocksSinceFullCheck++
	if epoch >= 0 {
		s.cur.Epoch = epoch
	}
	cur := s.cur
	return cur, SaveCursor(s.path, &cur)
}

// setRollback moves the cursor back to point and persists it.
// BlockNumber is left zero (a rollback point does not carry one); the next
// advance call, for the fork's own block at or after this point, repopulates
// it.
func (s *cursorState) setRollback(point Tip) (IncrementalCursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur.Tip = point
	cur := s.cur
	return cur, SaveCursor(s.path, &cur)
}

// resetFullCheckCounter zeroes BlocksSinceFullCheck and persists it, once a
// full check completes (successfully or not -- see fullCheckWorker's doc
// comment on why even a failed attempt still resets the countdown). It
// leaves Tip/Epoch untouched: by the time a full check finishes, the
// ChainSync callback goroutine may have already advanced them well past the
// point the check was pinned to, and this must not clobber that progress.
func (s *cursorState) resetFullCheckCounter() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur.BlocksSinceFullCheck = 0
	cur := s.cur
	return SaveCursor(s.path, &cur)
}

// fullCheckRequest is one dispatch to a fullCheckWorker.
type fullCheckRequest struct {
	reason FullCheckReason
	at     Tip
}

// fullCheckWorker runs full checks one at a time, in its own goroutine,
// decoupled from whichever ChainSync callback requested one -- a full
// check's multi-minute duration must never block that callback.
//
// Blocking a ChainSync callback for that long was tried first and found
// live, against a real node, to be actively harmful, not just slow: the
// callback's own connection sits idle (no ChainSync traffic in either
// direction) for the whole duration, long enough to trip gouroboros's mux
// read timeout and tear the session down out from under the still-running
// callback. RunIncremental's own reconnect loop then starts a *new* session
// while the *old* callback goroutine is still running the full check it was
// part way through -- two goroutines now touching the same cursor
// concurrently, a real data race, with the added risk that the old,
// abandoned goroutine's eventual write could silently roll the persisted
// cursor backward after the new session has already advanced it.
//
// Requests coalesce to at most one pending at a time (a buffered channel of
// size 1): if a full check is already running or already queued when
// another is requested, the new request is dropped rather than queued --
// the condition that asked for it (a checkpoint interval, an epoch
// transition, or a per-block mismatch) will simply re-evaluate against
// whatever cursor state exists once the in-flight one finishes, rather than
// piling up redundant checks behind a fast-moving chain.
type fullCheckWorker struct {
	cfg     IncrementalConfig
	cursor  *cursorState
	pending chan fullCheckRequest
	done    chan struct{}
}

// startFullCheckWorker starts the worker's background goroutine and returns
// immediately. The worker runs until ctx is cancelled; call stop afterward
// to wait for it to actually exit.
func startFullCheckWorker(
	ctx context.Context, cfg IncrementalConfig, cursor *cursorState,
) *fullCheckWorker {
	w := &fullCheckWorker{
		cfg:     cfg,
		cursor:  cursor,
		pending: make(chan fullCheckRequest, 1),
		done:    make(chan struct{}),
	}
	go w.run(ctx)
	return w
}

// request dispatches a full check pinned to at, for the given reason.
// Non-blocking: see fullCheckWorker's doc comment on why a request is
// dropped, not queued, when one is already pending or in flight.
func (w *fullCheckWorker) request(reason FullCheckReason, at Tip) {
	select {
	case w.pending <- fullCheckRequest{reason: reason, at: at}:
	default:
	}
}

// fullCheckSucceeded reports whether a full Check outcome represents a
// genuinely completed comparison (matched or diverged -- either way, a real
// answer was obtained at the pinned point) rather than one that learned
// nothing (a dial/query error, or a discarded/Skipped cycle). Only the
// former should reset BlocksSinceFullCheck: resetting it on every attempt
// regardless of outcome (the prior behavior) silently delayed the next
// legitimate interval checkpoint by up to a full --full-check-interval's
// worth of blocks even though no check had actually completed
// (blinklabs-io/dingo#1900 incremental-mode audit finding).
func fullCheckSucceeded(result *CheckResult, err error) bool {
	return err == nil && result != nil && !result.Skipped
}

// run is the worker's whole lifetime: wait for a request, run it to
// completion (bounded by cfg.FullCheckTimeout, derived from ctx so shutdown
// cancels an in-flight check promptly), report it, and -- only once the
// check actually completed trustworthily (fullCheckSucceeded) -- reset the
// shared cursor's checkpoint countdown, then wait for the next request.
func (w *fullCheckWorker) run(ctx context.Context) {
	defer close(w.done)
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-w.pending:
			fullCheckCtx, cancel := context.WithTimeout(
				ctx,
				w.cfg.FullCheckTimeout,
			)
			result, err := Check(
				fullCheckCtx, w.cfg.DingoAddr, w.cfg.CardanoAddr, w.cfg.Magic,
				&req.at,
			)
			cancel()
			w.cfg.OnFullCheck(req.reason, result, err)
			// A failed (err != nil) or discarded (result.Skipped) attempt
			// learned nothing and must not reset the countdown -- see
			// fullCheckSucceeded's doc comment. decideFullCheckReason will
			// simply re-request a full check on the very next block for as
			// long as BlocksSinceFullCheck's own threshold (or another
			// trigger) stays due, and request's own coalescing (see its doc
			// comment) keeps that from piling up redundant requests behind a
			// fast-moving chain.
			if !fullCheckSucceeded(result, err) {
				continue
			}
			if err := w.cursor.resetFullCheckCounter(); err != nil {
				w.cfg.Logger.Warn(
					"nodeparity: could not persist cursor after full check",
					"error", err,
				)
			}
		}
	}
}

// stop waits for the worker's goroutine to exit. Call after ctx (the one
// passed to startFullCheckWorker) is cancelled; otherwise this blocks
// forever.
func (w *fullCheckWorker) stop() {
	<-w.done
}

// IncrementalConfig configures RunIncremental. FullCheckInterval and
// CursorFile are required to be positive/non-empty; RunIncremental validates
// this itself so a caller cannot start a loop that can never checkpoint or
// resume.
type IncrementalConfig struct {
	DingoAddr         string
	CardanoAddr       string
	Magic             uint32
	FullCheckInterval uint64
	CursorFile        string
	// FullCheckTimeout bounds every full Check this package triggers
	// (startup baseline, interval/rollback/epoch-transition/mismatch
	// checkpoints) -- distinct from, and far longer than, blockCheckTimeout,
	// which only ever bounds the lightweight per-block delta query. A full
	// Check's paginated whole-UTxO walk measured roughly 5 minutes against a
	// real Preview-scale node (see ARCHITECTURE.md); a timeout sized for the
	// per-block query would self-cancel every triggered full check via its
	// own context deadline. Defaults to DefaultFullCheckTimeout when zero.
	FullCheckTimeout time.Duration
	Logger           *slog.Logger
	// OnFullCheck is called with each full Check's outcome, including the
	// startup baseline -- so a caller (cmd/node-parity) can log and record
	// metrics for it exactly like watch's full mode already does, without
	// this package depending on cmd/node-parity's logging/metrics types.
	OnFullCheck func(reason FullCheckReason, result *CheckResult, err error)
	// OnBlockCheck is called after every incrementally-validated block,
	// matched or diverged, so a caller can log and record metrics per block.
	OnBlockCheck func(tip Tip, diff Diff)
	// OnBlockCheckError is called whenever incrementalSession ends with an
	// error -- a per-block LocalStateQuery failure (the failure class this
	// exists for: previously such errors reached only a log line, never any
	// metric, so the NodeParityCheckErrors alert could not see them at all;
	// blinklabs-io/dingo#1900 incremental-mode audit finding), a dial
	// failure, or any other reason the session dropped -- right before
	// RunIncremental logs it and reconnects with backoff. Unlike OnBlockCheck
	// (one call per successfully-validated block), this is one call per
	// ended session, so a caller (cmd/node-parity) can record it via the same
	// check-error metric runWatchCycle already uses for any other Check
	// failure. Never called for a clean shutdown (ctx cancelled).
	OnBlockCheckError func(err error)
}

// blockCheckTimeout bounds each block's Acquire-and-query round trip against
// both nodes. A single block's delta touches only the handful of UTxOs it
// consumed/produced, not the whole ledger, so this is far shorter than
// runWatchCycle's full-mode timeout -- a block that cannot be compared this
// quickly indicates a stuck peer, not genuine work in progress.
const blockCheckTimeout = 30 * time.Second

// DefaultFullCheckTimeout is IncrementalConfig.FullCheckTimeout's default,
// comfortably above the ~5 minute full-check walk time measured against a
// real Preview-scale node (see ARCHITECTURE.md).
const DefaultFullCheckTimeout = 10 * time.Minute

// RunIncremental runs sequential, per-block ledger-state validation: unlike
// Check (which pins to whatever point is current and compares the whole
// ledger state), this validates block N, then N+1, then N+2 in strict chain
// order, comparing only the UTxOs each block actually touches (its
// transactions' consumed inputs and produced outputs) plus that block's
// point-pinned protocol parameters -- not stake distribution, which Dingo can
// only answer when pinned exactly at its live tip (see queryProtocolParams's
// doc comment), never true for a per-block walk that is behind tip by
// design; stake-distribution divergence is instead caught by this mode's own
// periodic full checkpoints (--full-check-interval), which do pin at live
// tip. It runs until ctx is cancelled.
//
// It always starts by running one full Check pinned at the live tip,
// regardless of whether cfg.CursorFile already holds a prior cursor: this is
// the trust-reestablishing check a process start needs (the two nodes might
// have been restarted, resynced, or diverged during any downtime), and the
// only point at which Dingo can currently answer a full whole-ledger-state
// comparison at all (see queryProtocolParams's doc comment). That fresh
// baseline result is always reported via cfg.OnFullCheck, but it is not
// unconditionally what the cursor resumes from: when a prior cursor exists
// and both nodes can still Acquire its own point (buildStartupCursor,
// pointReachable), RunIncremental resumes sequential validation from there
// instead, so blocks that landed during any downtime are individually
// compared rather than silently skipped for the fresh baseline's own,
// typically-later point (blinklabs-io/dingo#1900 incremental-mode audit
// finding). Only when there is no prior cursor, or its point is no longer
// reachable (pruned, or a fork), does the cursor fall back to the fresh
// baseline's own point. Either way, BlocksSinceFullCheck is always seeded
// from any prior cursor file, so a restart mid-interval does not reset the
// checkpoint countdown.
//
// Rollbacks (chainsync.WithRollBackwardFunc) move the cursor back to the
// reported point and trigger a fresh full check there (FullCheckRollback):
// the ChainSync protocol itself resumes delivering RollForward blocks from
// the new fork automatically once a rollback is reported, so nothing else
// needs to change to "replay forward" -- the existing per-block loop simply
// continues from the rolled-back cursor.
func RunIncremental(ctx context.Context, cfg IncrementalConfig) error {
	if cfg.FullCheckInterval == 0 {
		return errors.New("FullCheckInterval must be positive")
	}
	if cfg.CursorFile == "" {
		return errors.New("CursorFile is required for incremental mode")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.OnFullCheck == nil {
		cfg.OnFullCheck = func(FullCheckReason, *CheckResult, error) {}
	}
	if cfg.OnBlockCheck == nil {
		cfg.OnBlockCheck = func(Tip, Diff) {}
	}
	if cfg.OnBlockCheckError == nil {
		cfg.OnBlockCheckError = func(error) {}
	}
	if cfg.FullCheckTimeout <= 0 {
		cfg.FullCheckTimeout = DefaultFullCheckTimeout
	}
	if err := os.MkdirAll(filepath.Dir(cfg.CursorFile), 0o700); err != nil &&
		!errors.Is(err, os.ErrExist) {
		return fmt.Errorf(
			"create cursor file directory: %w", err,
		)
	}

	baseline, err := establishBaseline(ctx, cfg)
	if err != nil {
		return err
	}
	cursor := newCursorState(cfg.CursorFile, *baseline)

	worker := startFullCheckWorker(ctx, cfg, cursor)
	defer worker.stop()

	backoff := watcherMinBackoff
	for ctx.Err() == nil {
		established, sessionErr := incrementalSession(ctx, cfg, cursor, worker)
		if ctx.Err() != nil {
			return nil //nolint:nilerr // shutdown via ctx, not the session's own error
		}
		if established {
			backoff = nextBackoff(backoff, true)
		}
		reportSessionEnd(cfg, sessionErr, backoff, cursor.snapshot().Tip.Slot)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = nextBackoff(backoff, false)
	}
	return nil
}

// reportSessionEnd logs one incrementalSession ending and, when sessionErr
// is non-nil, calls cfg.OnBlockCheckError so a caller can record it via a
// check-error metric the same way any other Check failure would be -- see
// IncrementalConfig.OnBlockCheckError's doc comment for why this needed its
// own callback (RunIncremental's existing OnBlockCheck only ever fires for a
// block that was actually decoded and queried, and OnFullCheck only for this
// mode's own full checkpoints, so neither one previously saw this failure
// class at all). RunIncremental's caller already excludes the clean-shutdown
// case (ctx cancelled) before calling this, so sessionErr here is always a
// real failure the operator should be able to see in metrics, not just logs.
func reportSessionEnd(
	cfg IncrementalConfig,
	sessionErr error,
	backoff time.Duration,
	cursorSlot uint64,
) {
	if sessionErr != nil {
		cfg.OnBlockCheckError(sessionErr)
	}
	cfg.Logger.Warn(
		"nodeparity: incremental session ended, reconnecting",
		"error", sessionErr, "backoff", backoff,
		"cursor_slot", cursorSlot,
	)
}

// establishBaseline runs the mandatory startup full Check pinned at the live
// tip (see RunIncremental's doc comment), retrying with the same backoff a
// dropped ChainSync session uses until it produces a trustworthy
// (non-skipped) result, since incremental mode has nothing safe to build on
// otherwise. It seeds BlocksSinceFullCheck from any prior cursor file so a
// restart mid-interval does not reset the checkpoint countdown, and hands
// the fresh result plus any prior cursor to buildStartupCursor, which
// decides whether the returned cursor actually resumes from the prior
// cursor's own point or falls back to this fresh baseline's -- see its doc
// comment.
func establishBaseline(
	ctx context.Context, cfg IncrementalConfig,
) (*IncrementalCursor, error) {
	priorBlocksSinceFullCheck := uint64(0)
	var prior *IncrementalCursor
	if p, err := LoadCursor(cfg.CursorFile); err != nil {
		cfg.Logger.Warn(
			"nodeparity: could not read prior cursor file, starting fresh",
			"error", err,
		)
	} else if p != nil {
		prior = p
		priorBlocksSinceFullCheck = p.BlocksSinceFullCheck
	}

	backoff := watcherMinBackoff
	for {
		// Always pinned to nil (the live tip), regardless of whether a prior
		// cursor exists: this is the mandatory trust-reestablishing check
		// RunIncremental's doc comment describes, and Dingo cannot answer a
		// full whole-ledger-state comparison (stake distribution in
		// particular) at any point other than its own live tip -- see
		// queryProtocolParams's doc comment. Which point the returned cursor
		// actually resumes from is decided separately, in buildStartupCursor
		// below.
		// Bounded by cfg.FullCheckTimeout, the same as every other full
		// Check this package triggers (fullCheckWorker.run) -- Dial
		// deliberately disables both the mux segment-read and
		// LocalStateQuery query timeouts on this trusted NtC channel (see
		// its doc comment), so nothing else stops a peer that accepts the
		// connection and then stalls mid-query from hanging this retry
		// loop indefinitely instead of timing out and retrying with
		// backoff like every other failure mode here already does
		// (blinklabs-io/dingo#4183 review).
		checkCtx, cancel := context.WithTimeout(ctx, cfg.FullCheckTimeout)
		result, err := Check(
			checkCtx,
			cfg.DingoAddr,
			cfg.CardanoAddr,
			cfg.Magic,
			nil,
		)
		cancel()
		cfg.OnFullCheck(FullCheckStartup, result, err)
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("startup baseline: %w", ctxErr)
		}
		if err == nil && result != nil && !result.Skipped {
			return buildStartupCursor(
				ctx,
				cfg,
				result,
				prior,
				priorBlocksSinceFullCheck,
			)
		}
		cfg.Logger.Warn(
			"nodeparity: startup baseline check did not produce a trustworthy result, retrying",
			"error",
			err,
			"skipped",
			result != nil && result.Skipped,
			"backoff",
			backoff,
		)
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("startup baseline: %w", ctx.Err())
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, watcherMaxBackoff)
	}
}

// buildStartupCursor decides the actual IncrementalCursor RunIncremental
// resumes from once a trustworthy live-tip baseline result is in hand: a
// prior cursor's own persisted point, when one exists and both nodes can
// still Acquire it (pointReachable) -- so blocks that landed during any
// downtime are not silently skipped, the fix for blinklabs-io/dingo#1900's
// incremental-mode audit finding that a restart always discarded the saved
// point for the fresh baseline's own -- or the fresh baseline result's own
// point otherwise (no prior cursor, or its point is no longer reachable:
// pruned, or a fork moved it off one or both nodes' chains). Either way,
// result's own live-tip comparison (already reported via OnFullCheck by the
// caller) still ran and still established today's trust in the tool; only
// the point RunIncremental's ChainSync session actually resumes from
// differs.
//
// When result itself shows a real divergence (not Skipped -- callers only
// reach this function for a non-skipped result -- but a non-empty Diff),
// the returned cursor is not resuming from a confirmed-agreed point either
// way: this is logged distinctly (not just via OnFullCheck's own routine
// per-cycle report) so an operator watching logs can tell a session started
// life already disputed, rather than only discovering that by reading the
// same "ledger state diverged" line every other full check produces
// (blinklabs-io/dingo#1900 incremental-mode audit finding).
func buildStartupCursor(
	ctx context.Context,
	cfg IncrementalConfig,
	result *CheckResult,
	prior *IncrementalCursor,
	priorBlocksSinceFullCheck uint64,
) (*IncrementalCursor, error) {
	if !result.Diff.Empty() {
		cfg.Logger.Warn(
			"nodeparity: startup baseline itself diverged -- the incremental cursor begins on a disputed point, not a confirmed-agreed one",
			"slot",
			result.Tip.Slot,
			"hash",
			result.Tip.Hash,
			"diff_count",
			result.Diff.Count(),
		)
	}

	cursorTip := result.Tip
	liveEpoch, epochErr := queryEpochAt(
		ctx,
		cfg.CardanoAddr,
		cfg.Magic,
		cursorTip,
	)
	if epochErr != nil {
		// The epoch number is only used to detect a later transition and
		// trigger an extra checkpoint -- losing it at startup is not fatal to
		// establishing the baseline itself, so this degrades to "no
		// transition detected until the next full check" rather than
		// blocking startup entirely. It also makes resuming from a prior
		// cursor unsafe below (same-epoch cannot be confirmed), so a fresh
		// baseline is used regardless of whether a prior cursor exists.
		cfg.Logger.Warn(
			"nodeparity: could not read starting epoch, epoch-transition checkpoints disabled until the next full check",
			"error",
			epochErr,
		)
		liveEpoch = -1
	}
	epoch := liveEpoch

	switch {
	case prior == nil:
		// No prior cursor at all -- nothing to resume from.
	case !pointReachable(ctx, cfg, prior.Tip):
		cfg.Logger.Warn(
			"nodeparity: prior cursor's point is no longer reachable (pruned, or a fork), falling back to a fresh baseline at the live tip -- blocks between the old point and the new baseline will not be individually compared",
			"prior_slot",
			prior.Tip.Slot,
			"prior_hash",
			prior.Tip.Hash,
		)
	case liveEpoch < 0 || prior.Epoch < 0 || prior.Epoch != liveEpoch:
		// Resuming replays every block between the prior point and the live
		// tip through the per-block delta check, which only queries protocol
		// parameters (not stake distribution -- see queryProtocolParams's
		// doc comment) pinned at each block's own point. Dingo's
		// protocol-parameters handler only tolerates a pinned point within
		// the live tip's *current* epoch (queryShelleyCurrentProtocolParams,
		// ErrHistoricalStateUnavailable otherwise) -- so replaying across an
		// epoch boundary would make every one of those queries fail the same
		// way, the session erroring out on the very first replayed block and
		// never actually making the progress resuming is meant to provide.
		// A prior cursor whose own epoch cannot be confirmed to still match
		// the live one (either epoch is unknown, or they genuinely differ)
		// therefore falls back to the fresh live-tip baseline instead --
		// safe, at the cost of not resuming across downtime long enough to
		// span an epoch boundary (Cardano's ~5 day epoch comfortably covers
		// routine restarts and brief outages).
		cfg.Logger.Warn(
			"nodeparity: prior cursor's epoch cannot be confirmed to match the live tip's, falling back to a fresh baseline at the live tip -- incremental mode cannot yet resume across an epoch boundary",
			"prior_slot",
			prior.Tip.Slot,
			"prior_epoch",
			prior.Epoch,
			"live_epoch",
			liveEpoch,
		)
	default:
		cfg.Logger.Info(
			"nodeparity: resuming incremental validation from prior cursor",
			"slot", prior.Tip.Slot, "hash", prior.Tip.Hash,
			"baseline_slot", result.Tip.Slot,
		)
		cursorTip = prior.Tip
		epoch = prior.Epoch
	}

	cursor := &IncrementalCursor{
		Tip:                  cursorTip,
		Epoch:                epoch,
		BlocksSinceFullCheck: priorBlocksSinceFullCheck,
	}
	if err := SaveCursor(cfg.CursorFile, cursor); err != nil {
		return nil, fmt.Errorf("save initial cursor: %w", err)
	}
	return cursor, nil
}

// pointReachable reports whether both dingo and cardano-node can still
// Acquire at, as a cheap way to validate a persisted cursor's point before
// resuming incremental validation from it -- without running a full
// whole-ledger-state comparison there, which Dingo cannot do at a non-live
// point anyway (see queryProtocolParams's doc comment). queryEpochAt is
// reused as the probe query since GetEpochNo has no retention window and
// honors any point still on the chain (ledger/queries.go
// queryShelleyEpochNo), so an Acquire failure here means the point is
// genuinely gone (pruned, or rolled off the chain by a fork), not merely
// that some other, more restricted query would have rejected it.
func pointReachable(ctx context.Context, cfg IncrementalConfig, at Tip) bool {
	if _, err := queryEpochAt(ctx, cfg.DingoAddr, cfg.Magic, at); err != nil {
		return false
	}
	if _, err := queryEpochAt(ctx, cfg.CardanoAddr, cfg.Magic, at); err != nil {
		return false
	}
	return true
}

// queryEpochAt opens a short-lived connection to addr and reads the epoch
// number at the given point. queryShelleyEpochNo (ledger/queries.go) honors
// an acquired point with no retention window, unlike protocol parameters, so
// this is safe to call at any already-established point.
func queryEpochAt(
	ctx context.Context, addr string, magic uint32, at Tip,
) (int, error) {
	conn, err := Dial(ctx, addr, magic)
	if err != nil {
		return 0, err
	}
	defer conn.Close() //nolint:errcheck
	lsq := conn.LocalStateQuery()
	if lsq == nil || lsq.Client == nil {
		return 0, errors.New("LocalStateQuery client unavailable")
	}
	point, err := at.point()
	if err != nil {
		return 0, err
	}
	if err := lsq.Client.Acquire(&point); err != nil {
		return 0, fmt.Errorf("acquire point: %w", err)
	}
	defer lsq.Client.Release() //nolint:errcheck
	return lsq.Client.GetEpochNo()
}

// incrementalSession runs one ChainSync connection to cfg.CardanoAddr from
// dial to teardown, syncing from cursor.Tip and validating each block as it
// arrives. cardano-node is used as the block-content source (not dingo)
// because its block bytes are the trusted reference this whole tool
// compares against; either node's copy of a given block would decode
// identically since block content is consensus-critical and
// canonical -- this choice is about which node a decode bug should never be
// attributed to, not about which one has the "real" bytes.
//
// It reports whether the session got as far as an established sync (so the
// caller's reconnect backoff resets the same way Watcher's does), and the
// reason it ended.
func incrementalSession(
	ctx context.Context,
	cfg IncrementalConfig,
	cursor *cursorState,
	worker *fullCheckWorker,
) (established bool, err error) {
	startPoint, err := cursor.snapshot().Tip.point()
	if err != nil {
		return false, fmt.Errorf("cursor point: %w", err)
	}

	// One persistent connection to dingo for this whole session's per-block
	// LocalStateQuery calls -- re-Acquired per block, never re-dialed. This
	// (plus reusing the ChainSync connection below for cardano-node's own
	// per-block queries, rather than dialing a second connection to it too)
	// replaces what checkBlockDelta used to do: dial two brand-new NtC
	// connections for every single block. That per-block dial churn was
	// tested live against a real dingo and found to correlate with
	// "protocol is shutting down" disconnects -- rare (roughly 1 in 8
	// blocks) against a freshly-bootstrapped node, but roughly 9x more
	// frequent against the same node after hours of sustained connection
	// churn, strongly suggesting the churn itself contributes to it. A
	// session-lifetime connection here does not eliminate every possible
	// "protocol is shutting down" (that class of failure predates and is
	// independent of this specific pattern -- see QueryReferenceUTxOSnapshot's
	// doc comment), but removes thousands of unnecessary handshakes per
	// hour that were making it worse.
	dingoConn, err := Dial(ctx, cfg.DingoAddr, cfg.Magic)
	if err != nil {
		return false, fmt.Errorf("dial dingo: %w", err)
	}
	defer dingoConn.Close() //nolint:errcheck

	proto := protoFromAddr(cfg.CardanoAddr)
	dialCtx, cancelDial := context.WithTimeout(ctx, dialTimeout)
	defer cancelDial()
	rawConn, dialErr := dialRaw(dialCtx, proto, cfg.CardanoAddr)
	if dialErr != nil {
		return false, fmt.Errorf(
			"dial %s %s: %w",
			proto,
			cfg.CardanoAddr,
			dialErr,
		)
	}
	stopDialCancel := context.AfterFunc(
		dialCtx, func() { rawConn.Close() },
	) //nolint:errcheck

	blockErrChan := make(chan error, 1)
	reportBlockErr := func(err error) {
		select {
		case blockErrChan <- err:
		default:
		}
	}

	// cardanoConn is predeclared and assigned (not :=) below so the
	// RollForward/RollBackward closures, built as part of the same
	// ouroboros.New call that produces it, capture this variable by
	// reference: they only ever run after Sync starts, well after
	// cardanoConn holds its real value. Its LocalStateQuery half is reused
	// for cardano-node's own per-block queries -- Ouroboros multiplexes
	// ChainSync and LocalStateQuery over one connection by design, so this
	// needs no separate dial to cardano-node at all.
	var cardanoConn *ouroboros.Connection
	cardanoConn, connErr := ouroboros.New(
		ouroboros.WithConnection(rawConn),
		ouroboros.WithNetworkMagic(cfg.Magic),
		ouroboros.WithNodeToNode(false),
		// This connection is held open for the whole incremental session
		// (ChainSync waits indefinitely for the next block, reused for
		// cardano-node's own per-block LocalStateQuery calls, and also for
		// whatever periodic full checkpoint this session triggers) --
		// confirmed live that gouroboros' default 120s segment-read
		// timeout and 180s LocalStateQuery QueryTimeout both fire on this
		// connection during ordinary operation, tearing the session down
		// and forcing a reconnect (dial.go/watch.go's identical fix
		// covers the same failure mode on their own connections). Same
		// trusted-channel rationale as both of those.
		ouroboros.WithMuxerSegmentReadTimeout(0),
		ouroboros.WithLocalStateQueryConfig(localstatequery.NewConfig(
			localstatequery.WithQueryTimeout(0),
			localstatequery.WithMaxReadBufferSize(2*1024*1024*1024),
		)),
		ouroboros.WithChainSyncConfig(chainsync.NewConfig(
			chainsync.WithRollForwardFunc(
				func(
					_ chainsync.CallbackContext,
					blockType uint,
					blockData any,
					_ chainsync.Tip,
				) error {
					block, ok := blockData.(lcommon.Block)
					if !ok {
						err := fmt.Errorf(
							"unexpected roll-forward payload type %T for block type %d",
							blockData,
							blockType,
						)
						reportBlockErr(err)
						return err
					}
					if err := handleIncrementalBlock(
						ctx, cfg, cursor, worker, dingoConn, cardanoConn, block,
					); err != nil {
						reportBlockErr(err)
						return err
					}
					return nil
				},
			),
			chainsync.WithRollBackwardFunc(
				func(
					_ chainsync.CallbackContext,
					point pcommon.Point,
					_ chainsync.Tip,
				) error {
					if err := handleIncrementalRollback(cfg, cursor, worker, point); err != nil {
						reportBlockErr(err)
						return err
					}
					return nil
				},
			),
		)),
	)
	stopDialCancel()
	if connErr != nil {
		rawConn.Close() //nolint:errcheck
		return false, fmt.Errorf("ouroboros.New: %w", connErr)
	}
	defer cardanoConn.Close() //nolint:errcheck

	stopOnCancel := context.AfterFunc(
		ctx,
		func() { cardanoConn.Close() },
	) //nolint:errcheck
	defer stopOnCancel()

	cs := cardanoConn.ChainSync()
	if cs == nil || cs.Client == nil {
		return false, errors.New("ChainSync client unavailable")
	}
	if syncErr := cs.Client.Sync([]pcommon.Point{startPoint}); syncErr != nil {
		return false, fmt.Errorf("start chainsync: %w", syncErr)
	}

	select {
	case <-ctx.Done():
		return true, nil
	case err := <-blockErrChan:
		return true, err
	case sessionErr, ok := <-cardanoConn.ErrorChan():
		if !ok {
			return true, errors.New("connection closed")
		}
		return true, sessionErr
	}
}

// decideFullCheckReason is handleIncrementalBlock's trigger decision, pulled
// out as a pure function so it can be tested directly against every
// combination of inputs -- including the interval and epoch-transition
// triggers, which the live testnet used to develop this package could not
// exercise on its own: blinklabs-io/dingo#3854 (open at the time of
// writing) makes the stake-distribution comparison diverge on effectively
// every block, so a live run's mismatch trigger always fires first and
// preempts both of the others before their own conditions are ever reached.
//
// diffEmpty is the block's own delta-check result; epoch and beforeEpoch are
// this block's freshly-queried epoch and the cursor's epoch before this
// block (both a caller supplies as -1 when the epoch lookup failed, per
// checkBlockDelta's contract, which decideFullCheckReason treats as "unknown,
// do not report a transition"); blocksSinceFullCheck and fullCheckInterval
// are the values checkpointing this block would compare. Priority order
// matches the switch statement this replaced: a mismatch always wins over an
// epoch transition, which always wins over a plain interval checkpoint --
// deliberate, since a mismatch is the most actionable of the three and an
// operator investigating one should not have it overwritten in the log by a
// merely-due interval checkpoint that happened to line up with the same
// block.
func decideFullCheckReason(
	diffEmpty bool,
	epoch, beforeEpoch int,
	blocksSinceFullCheck, fullCheckInterval uint64,
) (reason FullCheckReason, due bool) {
	epochChanged := epoch >= 0 && beforeEpoch >= 0 && epoch != beforeEpoch
	switch {
	case !diffEmpty:
		return FullCheckMismatch, true
	case epochChanged:
		return FullCheckEpochTransition, true
	case blocksSinceFullCheck >= fullCheckInterval:
		return FullCheckInterval, true
	default:
		return "", false
	}
}

// handleIncrementalBlock validates one block's UTxO delta plus point-pinned
// protocol parameters (not stake distribution -- see checkBlockDelta's doc
// comment) against both nodes, advances and persists the cursor, and
// dispatches a full checkpoint (run in the
// background by worker, never blocking this function or the ChainSync
// session it runs on -- see fullCheckWorker's doc comment) when one is due:
// by interval, an epoch transition, or this block's own mismatch.
func handleIncrementalBlock(
	ctx context.Context,
	cfg IncrementalConfig,
	cursor *cursorState,
	worker *fullCheckWorker,
	dingoConn, cardanoConn *ouroboros.Connection,
	block lcommon.Block,
) error {
	cycleCtx, cancel := context.WithTimeout(ctx, blockCheckTimeout)
	defer cancel()

	tip := Tip{
		Slot:        block.SlotNumber(),
		Hash:        hex.EncodeToString(block.Hash().Bytes()),
		BlockNumber: block.BlockNumber(),
	}

	diff, epoch, err := checkBlockDelta(
		cycleCtx,
		dingoConn,
		cardanoConn,
		tip,
		block,
	)
	if err != nil {
		return fmt.Errorf(
			"block delta check at slot %d (block %d): %w",
			tip.Slot, tip.BlockNumber, err,
		)
	}
	cfg.OnBlockCheck(tip, diff)

	before := cursor.snapshot()
	after, err := cursor.advance(tip, epoch)
	if err != nil {
		return fmt.Errorf("save cursor: %w", err)
	}

	if reason, due := decideFullCheckReason(
		diff.Empty(), epoch, before.Epoch, after.BlocksSinceFullCheck,
		cfg.FullCheckInterval,
	); due {
		worker.request(reason, after.Tip)
	}
	return nil
}

// handleIncrementalRollback moves the cursor back to point and dispatches a
// full checkpoint there (run in the background by worker -- see
// fullCheckWorker's doc comment). The ChainSync client resumes sending
// RollForward messages from the new fork on its own once this returns, so
// nothing else is needed to "replay forward" -- handleIncrementalBlock's
// normal per-block path picks up from here.
//
// A RollBackward to exactly the cursor's own current point is not a real
// rollback: the ChainSync protocol reports one as the first message after
// any Sync/FindIntersect, confirming the negotiated reading position, before
// real RollForward messages follow -- this fires on every session start
// (including every reconnect after a dropped connection, not just process
// startup) with the same point the caller just asked to Sync from. Treating
// it as a genuine rollback would dispatch a full checkpoint at zero
// information gain (the point is already trusted -- it is either the
// just-established startup baseline, or a point the previous session's own
// per-block validation already confirmed) every single time a session
// starts.
func handleIncrementalRollback(
	cfg IncrementalConfig,
	cursor *cursorState,
	worker *fullCheckWorker,
	point pcommon.Point,
) error {
	hash := hex.EncodeToString(point.Hash)
	current := cursor.snapshot()
	if point.Slot == current.Tip.Slot && hash == current.Tip.Hash {
		return nil
	}

	rollbackTip := Tip{Slot: point.Slot, Hash: hash}
	after, err := cursor.setRollback(rollbackTip)
	if err != nil {
		return fmt.Errorf("save cursor: %w", err)
	}
	cfg.Logger.Warn(
		"nodeparity: rollback reported, resetting cursor",
		"slot", point.Slot, "hash", hash,
	)
	worker.request(FullCheckRollback, after.Tip)
	return nil
}

// checkBlockDelta validates one block's UTxO delta (its transactions'
// consumed inputs and produced outputs) plus point-pinned protocol
// parameters, against both dingo and cardano-node pinned to the block's own
// point. It deliberately does not compare stake distribution: Dingo's
// GetStakeDistribution handler (ledger/queries_stakedistribution.go) only
// answers when the pinned point equals its own live tip, which a per-block
// walk is behind by design -- querying it here reliably stalled every
// incremental session against a real Dingo node, since the very next block's
// query would fail the same way forever, the cursor never advancing
// (blinklabs-io/dingo#1900 incremental-mode audit finding). Stake
// distribution is still compared by this mode's periodic full checkpoints
// (--full-check-interval), which pin at live tip via the shared Check()
// path and so can query it safely. It returns the negative epoch number
// (-1) when the epoch query itself failed, distinct from a real epoch, so
// the caller does not mistake a failed lookup for "epoch 0" or otherwise
// spuriously detect a transition.
func checkBlockDelta(
	ctx context.Context,
	dingoConn, cardanoConn *ouroboros.Connection,
	tip Tip,
	block lcommon.Block,
) (Diff, int, error) {
	// queryIncrementalHalf's Acquire/query calls are synchronous gouroboros
	// client calls with no per-call timeout or context of their own -- when
	// checkBlockDelta dialed a fresh, short-lived connection per block, the
	// dial's own ctx-cancel-closes-the-connection handling (see Dial) bounded
	// them for free. Now that dingoConn/cardanoConn are persistent,
	// session-lifetime connections dialed once against the long-lived outer
	// ctx, that protection is gone unless reinstated here: close both
	// connections if ctx (the caller's blockCheckTimeout-bounded cycleCtx)
	// expires before this function returns, unblocking whichever call is
	// stuck. Closing both, not just the slow one, is deliberate: a query
	// that cannot complete within blockCheckTimeout indicates a stuck peer,
	// not genuine work in progress, and the caller already treats any
	// checkBlockDelta error as ending the whole incremental session (see
	// handleIncrementalBlock) -- both connections need redialing on the next
	// reconnect regardless of which one was actually slow.
	stop := context.AfterFunc(ctx, func() {
		dingoConn.Close()   //nolint:errcheck
		cardanoConn.Close() //nolint:errcheck
	})
	defer stop()

	consumed, produced := blockUtxoDelta(block)

	dingoHalf, err := queryIncrementalHalf(dingoConn, tip, consumed, produced)
	if err != nil {
		return Diff{}, -1, fmt.Errorf("dingo query: %w", err)
	}
	cardanoHalf, err := queryIncrementalHalf(
		cardanoConn,
		tip,
		consumed,
		produced,
	)
	if err != nil {
		return Diff{}, -1, fmt.Errorf("cardano-node query: %w", err)
	}

	// Protocol params only -- not diffProtocolParamsAndStake -- since
	// queryIncrementalHalf never queries stake distribution for this
	// per-block cycle; see its doc comment.
	diff := diffProtocolParams(dingoHalf.snapshot, cardanoHalf.snapshot)
	diff.UTxO = append(
		diff.UTxO,
		diffBlockUtxoDelta(
			consumed,
			produced,
			dingoHalf.utxo,
			cardanoHalf.utxo,
		)...,
	)

	epoch := -1
	if dingoHalf.epoch == cardanoHalf.epoch {
		epoch = dingoHalf.epoch
	}
	return diff, epoch, nil
}

// blockUtxoDelta extracts every transaction's applied (validity-aware --
// Consumed/Produced already account for IsValid, unlike the raw
// Inputs/Outputs on TransactionBody) consumed inputs and produced outputs
// across a whole block.
func blockUtxoDelta(
	block lcommon.Block,
) (consumed []lcommon.TransactionInput, produced []lcommon.Utxo) {
	for _, tx := range block.Transactions() {
		consumed = append(consumed, tx.Consumed()...)
		produced = append(produced, tx.Produced()...)
	}
	return consumed, produced
}

// incrementalHalf is one node's answer for a single block's check: the
// point-pinned protocol-params-only snapshot (StakeDistribution and
// UTxOEntries both left nil -- see queryProtocolParams's doc comment for why
// stake distribution is never queried here, and this cycle never asks for a
// whole UTxO set either), the resolved UTxO entries for exactly this
// block's touched refs, and the epoch at this point.
type incrementalHalf struct {
	snapshot *Snapshot
	utxo     map[string]string
	epoch    int
}

// queryIncrementalHalf runs the one LocalStateQuery session a single node
// needs for one block's incremental check: protocol params (not stake
// distribution -- see queryProtocolParams's doc comment for why that cannot
// be part of this per-block cycle), epoch number, and exactly the UTxO refs
// this block touched (consumed and produced together, in one batched
// GetUTxOByTxIn call) -- all pinned to tip. A consumed ref legitimately has
// no entry in the result (that is what "correctly spent" looks like);
// diffBlockUtxoDelta interprets absence, not this function.
func queryIncrementalHalf(
	conn *ouroboros.Connection,
	tip Tip,
	consumed []lcommon.TransactionInput,
	produced []lcommon.Utxo,
) (incrementalHalf, error) {
	lsq := conn.LocalStateQuery()
	if lsq == nil || lsq.Client == nil {
		return incrementalHalf{}, errors.New(
			"LocalStateQuery client unavailable",
		)
	}
	client := lsq.Client
	point, err := tip.point()
	if err != nil {
		return incrementalHalf{}, err
	}
	if err := client.Acquire(&point); err != nil {
		return incrementalHalf{}, fmt.Errorf("acquire point: %w", err)
	}
	defer client.Release() //nolint:errcheck

	// Protocol params only, not stake distribution: Dingo's
	// GetStakeDistribution handler only answers when the pinned point
	// equals its live tip (ledger/queries_stakedistribution.go), which is
	// never true for a per-block walk that is behind tip by design --
	// querying it here permanently stalled every incremental session against
	// a real Dingo node (blinklabs-io/dingo#1900 incremental-mode audit
	// finding: 4339 identical failures in a row, cursor never advancing).
	// Stake-distribution divergence is still caught by incremental mode's
	// periodic full checkpoints (--full-check-interval), which pin at live
	// tip via the shared Check() path and so can query it safely.
	ppProto, err := queryProtocolParams(client)
	if err != nil {
		return incrementalHalf{}, err
	}

	refs := blockRefsToQuery(consumed, produced)
	utxo, err := queryUTxOByRefs(client, refs)
	if err != nil {
		return incrementalHalf{}, err
	}

	epoch, err := client.GetEpochNo()
	if err != nil {
		return incrementalHalf{}, fmt.Errorf("epoch query: %w", err)
	}

	return incrementalHalf{
		snapshot: &Snapshot{ProtocolParams: ppProto},
		utxo:     utxo,
		epoch:    epoch,
	}, nil
}

// blockRefsToQuery converts a block's consumed inputs and produced outputs
// into the ref list queryUTxOByRefs (and so GetUTxOByTxIn) expects,
// deduplicating so an intra-block spend-and-recreate of the same TxIn (not
// possible for a real TxIn, which is unique per transaction output, but kept
// defensive since two different transactions in one block could still both
// name the same ref if one produces what another consumes within the same
// block) is queried once, not twice.
func blockRefsToQuery(
	consumed []lcommon.TransactionInput, produced []lcommon.Utxo,
) []localstatequery.UtxoId {
	seen := make(map[string]struct{}, len(consumed)+len(produced))
	refs := make([]localstatequery.UtxoId, 0, len(consumed)+len(produced))
	add := func(id lcommon.TransactionInput) {
		key := fmt.Sprintf("%s#%d", id.Id().String(), id.Index())
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		refs = append(
			refs,
			localstatequery.UtxoId{Hash: id.Id(), Idx: int(id.Index())},
		)
	}
	for _, in := range consumed {
		add(in)
	}
	for _, utxo := range produced {
		add(utxo.Id)
	}
	return refs
}

// diffBlockUtxoDelta compares one block's expected UTxO delta against what
// both nodes actually report at that block's point: every consumed ref must
// be absent from both (a lingering entry means that node failed to mark it
// spent), and every produced ref must be present in both with matching
// canonical content (a real cardano-node is the reference this whole tool
// compares against, so "present in a, missing in b" for a produced ref is
// reported the same as a content mismatch -- both mean dingo disagrees with
// the reference on a freshly-created output) -- except a produced ref that
// is missing from both because it was also consumed within this same block
// (created by one transaction, spent by a later one): that is the correct,
// expected outcome, not a divergence, so it must not be reported (see the
// produced loop below; blinklabs-io/dingo#1900 incremental-mode audit
// finding -- an intra-block create-then-spend previously produced a false
// "missing from both" line).
func diffBlockUtxoDelta(
	consumed []lcommon.TransactionInput,
	produced []lcommon.Utxo,
	dingoEntries, cardanoEntries map[string]string,
) []string {
	var lines []string
	seenConsumed := make(map[string]struct{}, len(consumed))
	for _, in := range consumed {
		key := fmt.Sprintf("%s#%d", in.Id().String(), in.Index())
		if _, dup := seenConsumed[key]; dup {
			continue
		}
		seenConsumed[key] = struct{}{}
		if val, ok := dingoEntries[key]; ok {
			lines = append(lines, fmt.Sprintf(
				"utxo %s should be spent but is still present in dingo: %s",
				key, val,
			))
		}
		if val, ok := cardanoEntries[key]; ok {
			lines = append(lines, fmt.Sprintf(
				"utxo %s should be spent but is still present in cardano-node: %s",
				key,
				val,
			))
		}
	}
	seenProduced := make(map[string]struct{}, len(produced))
	for _, utxo := range produced {
		key := fmt.Sprintf("%s#%d", utxo.Id.Id().String(), utxo.Id.Index())
		if _, dup := seenProduced[key]; dup {
			continue
		}
		seenProduced[key] = struct{}{}
		dingoVal, dingoOK := dingoEntries[key]
		cardanoVal, cardanoOK := cardanoEntries[key]
		_, consumedInBlock := seenConsumed[key]
		switch {
		case !dingoOK && !cardanoOK && consumedInBlock:
			// Created and spent within this same block: correctly absent
			// from both nodes' live answers, not a divergence. seenConsumed
			// is fully populated by the loop above before this one runs, so
			// this is safe to check unconditionally here.
			continue
		case !dingoOK && !cardanoOK:
			lines = append(lines, fmt.Sprintf(
				"utxo %s should have been created but is missing from both dingo and cardano-node",
				key,
			))
		case !dingoOK:
			lines = append(lines, fmt.Sprintf(
				"utxo %s present in cardano-node, missing in dingo: %s",
				key, cardanoVal,
			))
		case !cardanoOK:
			lines = append(lines, fmt.Sprintf(
				"utxo %s present in dingo, missing in cardano-node: %s",
				key, dingoVal,
			))
		case dingoVal != cardanoVal:
			lines = append(lines, fmt.Sprintf(
				"utxo %s differs: %s (dingo) vs %s (cardano-node)",
				key, dingoVal, cardanoVal,
			))
		}
	}
	return lines
}

// dialRaw is Dial's raw-socket-dial half, factored out so incrementalSession
// can attach its own ChainSync callback config to ouroboros.New -- Dial
// itself takes no such config, since Check's use of it never needs one.
func dialRaw(ctx context.Context, proto, addr string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, proto, addr)
}
