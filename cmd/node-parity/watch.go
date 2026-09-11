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
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/blinklabs-io/dingo/internal/nodeparity"
	"github.com/spf13/cobra"
)

// defaultFallbackInterval is the safety-net cadence: watch is normally
// triggered by real block activity, not this clock, but a check still runs
// on this schedule regardless, in case a watcher's ChainSync subscription
// silently stalls without erroring. Full mode only -- incremental mode has
// no equivalent flag, since its ChainSync session already drives every
// block deterministically and a stalled session is instead caught by its own
// reconnect-with-backoff loop. Independent of defaultCheckTimeout below --
// see its own doc comment for why the two must not be conflated.
const defaultFallbackInterval = 2 * time.Minute

// defaultCheckTimeout bounds one full-mode check cycle (runWatchCycle),
// separately from defaultFallbackInterval above. These two used to be the
// same value (--fallback-interval doubling as the per-cycle deadline), which
// made full mode fail by default out of the box: a real whole-UTxO
// comparison against a Preview-scale node measured 7-9+ minutes end to end
// (see ARCHITECTURE.md), far longer than --fallback-interval's own 2m
// default, so runWatchCycle self-cancelled via its own context deadline
// before a comparison could ever complete -- indistinguishable in the logs
// from the unrelated node-side "protocol is shutting down" failure this tool
// was originally built to route around (blinklabs-io/dingo#1900 audit
// finding). 20 minutes gives comfortable headroom above the measured
// 7-9 minute walk without --fallback-interval also needing to grow to match
// (it keeps its own, much smaller, "also check on this schedule regardless
// of block activity" meaning -- see its own flag help).
const defaultCheckTimeout = 20 * time.Minute

// defaultFullCheckInterval is incremental mode's default checkpoint cadence:
// how many blocks of sequential per-block validation run between full,
// whole-ledger-state comparisons. 1000 blocks is roughly 5-6 hours at
// Cardano's ~20s block time -- frequent enough to catch a slow-accumulating
// divergence a per-block delta check would never see (it only ever compares
// the handful of UTxOs one block touches), without running a multi-minute
// full check often enough to dominate the loop's own pace.
const defaultFullCheckInterval = 1000

func watchCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "watch",
		Short: "Run a check cycle every time either node produces a new block",
		Long: `Runs a comparison cycle triggered by real chain activity instead of a
clock, in one of two modes selected by --mode:

--mode=incremental (the default) validates one block at a time, in strict
chain order (block N, then N+1, then N+2, ...), comparing only the UTxOs
each block's transactions actually consumed or produced rather than the
whole ledger state -- far cheaper per cycle, at the cost of never
independently re-deriving the parts of the ledger a single block's delta
cannot reveal on its own. It always starts with one full check to
establish a trusted baseline (this doubles as the "full check on restart"
case), persists its sequential cursor to --cursor-file (required) so a
restart resumes rather than re-baselining from scratch, and periodically
re-runs a full check anyway (--full-check-interval blocks, or immediately
on a rollback, an epoch transition, or any incremental block's own
mismatch) as a checkpoint against whatever a single block's delta cannot
catch by itself.

--mode=full instead runs the same whole-ledger-state comparison as 'check'
every time either node's tip changes: it follows both nodes' ChainSync
feeds and reacts within a fraction of a second of a new block landing,
rather than waiting out a fixed interval and missing everything that
happened in between. --fallback-interval also runs a check on that
schedule regardless of block activity, purely as a safety net in case a
watcher's subscription silently stalls without erroring; --check-timeout
separately bounds how long any single check cycle (fallback-triggered or
block-triggered) may run before it is treated as failed -- a real
whole-UTxO comparison against a Preview-scale node measured 7-9+ minutes,
so --check-timeout's default is comfortably above that, independent of
--fallback-interval's own, much smaller default. Chosen over incremental
mode only when every triggered check needs the strongest available
guarantee (stake distribution and protocol params compared fresh, not just
relied on from an earlier checkpoint) -- at the cost of skipping whatever
blocks land during a multi-minute whole-UTxO query, which incremental
mode's per-block coverage does not.

Logs each cycle's outcome and, when --metrics-addr is set, exposes
Prometheus counters for completed cycles, skipped cycles, and per-field
divergences (both modes), plus incremental-specific counters for
--mode=incremental.

docs/dashboards/alerts.yaml's NodeParityNotChecking rule assumes
--fallback-interval stays comfortably under its own fixed 10m window (an
order of magnitude, as with the 2m default, is a safe margin); a much
larger value can make that alert misfire during an otherwise healthy
full-mode run. The same rule also assumes one check cycle itself finishes
within that 10m window: since --check-timeout (not --fallback-interval) now
bounds a single cycle's duration, a --check-timeout raised well past 10m to
accommodate a larger UTxO-set scale can make a single genuinely slow (not
stuck) cycle -- which reports nothing to Prometheus until it completes --
look identical to the tool being stuck. Widen that rule's window to match if
--check-timeout is raised significantly above its default. That rule does
not cover incremental mode.`,
		Args: cobra.NoArgs,
		RunE: watchRun,
	}
	cmd.Flags().Duration(
		"fallback-interval", defaultFallbackInterval,
		"full mode only: also run a check on this schedule regardless of block activity, as a safety net",
	)
	cmd.Flags().Duration(
		"check-timeout", defaultCheckTimeout,
		"full mode only: how long a single check cycle may take before it is treated as failed -- must comfortably exceed how long a full comparison actually takes against the target network's UTxO-set scale (measured at 7-9+ minutes against a Preview-scale node), independent of --fallback-interval",
	)
	cmd.Flags().String(
		"mode", "incremental",
		"comparison mode: 'incremental' (default; sequential per-block UTxO delta, with periodic full checkpoints) or 'full' (whole ledger state, triggered per block)",
	)
	cmd.Flags().Uint64(
		"full-check-interval", defaultFullCheckInterval,
		"incremental mode only: run a full checkpoint comparison every N blocks",
	)
	cmd.Flags().String(
		"cursor-file", "",
		"incremental mode only (required): path to persist the sequential parity cursor across restarts",
	)
	cmd.Flags().Duration(
		"full-check-timeout", nodeparity.DefaultFullCheckTimeout,
		"incremental mode only: how long a triggered full checkpoint (startup, interval, rollback, epoch transition, or mismatch) may take -- must comfortably exceed how long a full comparison actually takes against the target network's UTxO-set scale, not just one block's delta",
	)
	return cmd
}

func watchRun(cmd *cobra.Command, _ []string) error {
	network, err := requireNetwork()
	if err != nil {
		return err
	}
	if err := requireAddrs(); err != nil {
		return err
	}
	magic, err := networkMagic(network)
	if err != nil {
		return err
	}
	mode, _ := cmd.Flags().GetString("mode")

	// Every flag this command accepts is validated here, before
	// newParityMetrics runs below: that call registers into the real
	// Prometheus default registerer (a genuine side effect, not just a
	// local computation), so a caller that passed an invalid mode-specific
	// flag must never reach it -- otherwise a single invocation that fails
	// on, say, a missing --cursor-file would still have registered a full
	// set of metrics collectors, leaving a second invocation in the same
	// process (a retry, or a test) to fail on a duplicate registration
	// instead of on the same clear validation error.
	var runMode func(ctx context.Context, logger *slog.Logger, metrics *parityMetrics) error
	switch mode {
	case "full":
		fallbackInterval, _ := cmd.Flags().GetDuration("fallback-interval")
		if fallbackInterval <= 0 {
			return errors.New("--fallback-interval must be positive")
		}
		checkTimeout, _ := cmd.Flags().GetDuration("check-timeout")
		if checkTimeout <= 0 {
			return errors.New("--check-timeout must be positive")
		}
		runMode = func(
			ctx context.Context, logger *slog.Logger, metrics *parityMetrics,
		) error {
			return watchRunFull(
				ctx,
				magic,
				fallbackInterval,
				checkTimeout,
				logger,
				metrics,
			)
		}
	case "incremental":
		fullCheckInterval, _ := cmd.Flags().GetUint64("full-check-interval")
		if fullCheckInterval == 0 {
			return errors.New("--full-check-interval must be positive")
		}
		cursorFile, _ := cmd.Flags().GetString("cursor-file")
		if cursorFile == "" {
			return errors.New(
				"--cursor-file is required for --mode=incremental",
			)
		}
		fullCheckTimeout, _ := cmd.Flags().GetDuration("full-check-timeout")
		if fullCheckTimeout <= 0 {
			return errors.New("--full-check-timeout must be positive")
		}
		runMode = func(
			ctx context.Context, logger *slog.Logger, metrics *parityMetrics,
		) error {
			return watchRunIncremental(
				ctx, magic, fullCheckInterval, fullCheckTimeout, cursorFile,
				logger, metrics,
			)
		}
	default:
		return fmt.Errorf(
			"--mode must be 'full' or 'incremental', got %q",
			mode,
		)
	}

	logger := slog.Default()
	ctx := cmd.Context()

	metrics := newParityMetrics(network)
	if globalFlags.metricsAddr != "" {
		metricsServer, err := serveMetrics(globalFlags.metricsAddr, logger)
		if err != nil {
			return err
		}
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(
				context.Background(), 5*time.Second,
			)
			defer cancel()
			_ = metricsServer.Shutdown(shutdownCtx) //nolint:errcheck
		}()
	}

	return runMode(ctx, logger, metrics)
}

// watchRunFull is watch's original, unchanged behavior: react to either
// node's tip changing, with --fallback-interval as a backstop. fallbackInterval
// and checkTimeout are already validated positive by the caller -- see
// checkTimeout's own doc comment (defaultCheckTimeout) for why this is a
// separate parameter from fallbackInterval rather than reusing it as
// runWatchCycle's per-cycle deadline the way this used to.
func watchRunFull(
	ctx context.Context,
	magic uint32,
	fallbackInterval, checkTimeout time.Duration,
	logger *slog.Logger,
	metrics *parityMetrics,
) error {
	watcherLog := func(format string, args ...any) {
		logger.Warn(fmt.Sprintf(format, args...))
	}
	dingoWatcher := nodeparity.WatchBlocks(
		ctx, globalFlags.dingoAddr, magic, watcherLog,
	)
	defer dingoWatcher.Close()
	cardanoWatcher := nodeparity.WatchBlocks(
		ctx, globalFlags.cardanoAddr, magic, watcherLog,
	)
	defer cardanoWatcher.Close()

	logger.Info("node-parity: watch started",
		"mode", "full",
		"dingo_addr", globalFlags.dingoAddr,
		"cardano_addr", globalFlags.cardanoAddr,
		"fallback_interval", fallbackInterval,
		"check_timeout", checkTimeout,
	)

	fallback := time.NewTimer(fallbackInterval)
	defer fallback.Stop()

	for {
		runWatchCycle(
			ctx,
			checkTimeout,
			globalFlags.dingoAddr,
			globalFlags.cardanoAddr,
			magic,
			logger,
			metrics,
		)

		resetFallbackTimer(fallback, fallbackInterval)

		select {
		case <-ctx.Done():
			logger.Info("node-parity: watch stopped")
			return nil
		case <-dingoWatcher.Events:
		case <-cardanoWatcher.Events:
		case <-fallback.C:
		}
	}
}

// watchRunIncremental runs nodeparity.RunIncremental, wiring its callbacks
// to the same logger and metrics full mode uses (see
// handleIncrementalFullCheck/handleIncrementalBlockCheck).
// fullCheckInterval and cursorFile are already validated by the caller.
func watchRunIncremental(
	ctx context.Context,
	magic uint32,
	fullCheckInterval uint64,
	fullCheckTimeout time.Duration,
	cursorFile string,
	logger *slog.Logger,
	metrics *parityMetrics,
) error {
	logger.Info("node-parity: watch started",
		"mode", "incremental",
		"dingo_addr", globalFlags.dingoAddr,
		"cardano_addr", globalFlags.cardanoAddr,
		"full_check_interval", fullCheckInterval,
		"full_check_timeout", fullCheckTimeout,
		"cursor_file", cursorFile,
	)

	return nodeparity.RunIncremental(ctx, nodeparity.IncrementalConfig{
		DingoAddr:         globalFlags.dingoAddr,
		CardanoAddr:       globalFlags.cardanoAddr,
		Magic:             magic,
		FullCheckInterval: fullCheckInterval,
		FullCheckTimeout:  fullCheckTimeout,
		CursorFile:        cursorFile,
		Logger:            logger,
		OnFullCheck: func(
			reason nodeparity.FullCheckReason,
			result *nodeparity.CheckResult,
			err error,
		) {
			handleIncrementalFullCheck(reason, result, err, logger, metrics)
		},
		OnBlockCheck: func(tip nodeparity.Tip, diff nodeparity.Diff) {
			handleIncrementalBlockCheck(tip, diff, logger, metrics)
		},
		OnBlockCheckError: func(err error) {
			handleIncrementalSessionError(err, metrics)
		},
	})
}

// resetFallbackTimer rearms fallback for the next cycle. If fallback had
// already fired (Stop returns false) -- a check ran long enough to overrun
// it -- the next one is scheduled immediately (Reset(0)) instead of a full
// fresh interval, so an overrun cycle doesn't compound into "at least 2x
// --fallback-interval between checks." Otherwise fallback was stopped
// before firing, and a normal full-interval reset is correct.
func resetFallbackTimer(fallback *time.Timer, interval time.Duration) {
	if !fallback.Stop() {
		select {
		case <-fallback.C:
		default:
		}
		fallback.Reset(0)
		return
	}
	fallback.Reset(interval)
}

// runWatchCycle runs one Check, bounded by timeout, and hands its outcome
// to handleCheckResult. Splitting the network call from the outcome-
// handling logic keeps the latter unit-testable without a live node.
//
// Check's own context-cancellation handling (see internal/nodeparity's
// Dial) only reacts to ctx itself being cancelled -- process shutdown via
// signal.NotifyContext, in this CLI. Without a per-cycle bound, a peer
// that accepts a connection and then stops responding mid-query would
// block Check (and so this whole watch loop, which calls it synchronously)
// indefinitely: the fallback timer that exists specifically to guarantee
// activity within --fallback-interval can never fire again once the loop
// itself is the thing stuck, since nothing schedules a fresh cycle until
// this one returns. Bounding each cycle by timeout guarantees a stuck cycle
// self-aborts (recorded via checkErrorsTotal, same as any other Check
// error) rather than wedging the loop forever.
//
// timeout is --check-timeout, not --fallback-interval: the two used to be
// the same value, which made a genuinely slow (but not stuck) full
// comparison self-cancel via this very deadline before it could ever
// complete -- a real whole-UTxO walk measured 7-9+ minutes against a
// Preview-scale node, far longer than --fallback-interval's own 2m default
// (see defaultCheckTimeout's doc comment; blinklabs-io/dingo#1900 audit
// finding). --check-timeout is sized for that real cost; --fallback-interval
// keeps its own, separate meaning of "also trigger a check on this schedule
// regardless of block activity."
func runWatchCycle(
	ctx context.Context,
	timeout time.Duration,
	dingoAddr, cardanoAddr string,
	magic uint32,
	logger *slog.Logger,
	metrics *parityMetrics,
) {
	cycleCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := nodeparity.Check(
		cycleCtx,
		dingoAddr,
		cardanoAddr,
		magic,
		nil,
	)
	handleCheckResult(result, err, logger, metrics)
}

// handleCheckResult logs and records one Check outcome. Errors from Check
// itself (a dial or query failure, as opposed to a discarded cycle) are
// logged and otherwise swallowed: a watch loop must keep running through a
// transient node hiccup rather than exit and stop watching entirely.
func handleCheckResult(
	result *nodeparity.CheckResult,
	err error,
	logger *slog.Logger,
	metrics *parityMetrics,
) {
	// Guard on result being nil, not err: that's the value dereferenced
	// below, and Check's contract (a nil result on any error path) makes
	// the two equivalent in practice, but nil-checking the pointer that
	// actually gets used is what a static nil-flow check can verify.
	if result == nil {
		metrics.recordCheckError()
		logger.Warn("node-parity: check error", "error", err)
		return
	}
	if result.Skipped {
		metrics.recordSkip(result.SkipReason)
		logger.Warn("node-parity: check skipped", "reason", result.SkipDetail)
		return
	}
	metrics.recordCheck(result.Diff)
	if result.Diff.Empty() {
		logger.Info("node-parity: check matched",
			"slot", result.Tip.Slot, "block", result.Tip.BlockNumber,
		)
		return
	}
	logger.Warn("node-parity: ledger state diverged",
		"slot", result.Tip.Slot, "block", result.Tip.BlockNumber,
		"diff", strings.Join(result.Diff.Lines(), "; "),
	)
}

// handleIncrementalFullCheck logs and records one of incremental mode's full
// checkpoint cycles (startup baseline, periodic interval, rollback, epoch
// transition, or a per-block mismatch's own follow-up). Reuses
// handleCheckResult for the outcome itself so a full check looks the same in
// logs/metrics regardless of which mode triggered it, then additionally
// records which reason triggered this particular one.
func handleIncrementalFullCheck(
	reason nodeparity.FullCheckReason,
	result *nodeparity.CheckResult,
	err error,
	logger *slog.Logger,
	metrics *parityMetrics,
) {
	metrics.recordFullCheckTrigger(string(reason))
	handleCheckResult(result, err, logger, metrics)
}

// handleIncrementalBlockCheck logs and records one incremental per-block
// cycle's outcome. Unlike handleCheckResult, there is no "skipped" case here
// (RunIncremental only calls this after a block was actually decoded and
// queried) and no separate error path (a query failure for one block ends
// the whole ChainSync session instead, surfacing through
// RunIncremental's own reconnect-and-retry loop, not through this callback).
func handleIncrementalBlockCheck(
	tip nodeparity.Tip,
	diff nodeparity.Diff,
	logger *slog.Logger,
	metrics *parityMetrics,
) {
	metrics.recordIncrementalBlock(diff)
	if diff.Empty() {
		logger.Info("node-parity: incremental block matched",
			"slot", tip.Slot, "block", tip.BlockNumber,
		)
		return
	}
	logger.Warn("node-parity: incremental block diverged",
		"slot", tip.Slot, "block", tip.BlockNumber,
		"diff", strings.Join(diff.Lines(), "; "),
	)
}

// handleIncrementalSessionError records one incremental session ending in
// error (a per-block LocalStateQuery failure, a dial failure, or any other
// reason the session dropped) via the same checkErrorsTotal counter full
// mode's runWatchCycle/handleCheckResult already use for any other Check
// failure -- see nodeparity.IncrementalConfig.OnBlockCheckError's doc
// comment for why this needed its own callback: RunIncremental's existing
// OnBlockCheck only ever fires for a block that was actually decoded and
// queried, and OnFullCheck only for this mode's own full checkpoints, so
// neither one previously let this failure class reach
// node_parity_check_errors_total at all (blinklabs-io/dingo#1900
// incremental-mode audit finding). RunIncremental itself already logs the
// error and reconnects with backoff (reportSessionEnd), so this only records
// the metric.
func handleIncrementalSessionError(err error, metrics *parityMetrics) {
	metrics.recordCheckError()
}
