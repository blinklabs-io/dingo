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
// silently stalls without erroring.
const defaultFallbackInterval = 2 * time.Minute

func watchCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "watch",
		Short: "Run a check cycle every time either node produces a new block",
		Long: `Runs the same comparison cycle as 'check', triggered by real chain
activity instead of a clock: it follows both nodes' ChainSync feeds and
runs a check the moment either one's tip changes, so it reacts within a
fraction of a second of a new block landing rather than waiting out a
fixed interval and missing everything that happened in between.

--fallback-interval also runs a check on that schedule regardless of block
activity, purely as a safety net in case a watcher's subscription silently
stalls without erroring.

Logs each cycle's outcome and, when --metrics-addr is set, exposes
Prometheus counters for completed cycles, skipped cycles, and per-field
divergences.`,
		RunE: watchRun,
	}
	cmd.Flags().Duration(
		"fallback-interval", defaultFallbackInterval,
		"also run a check on this schedule regardless of block activity, as a safety net",
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
	fallbackInterval, _ := cmd.Flags().GetDuration("fallback-interval")
	if fallbackInterval <= 0 {
		return errors.New("--fallback-interval must be positive")
	}

	logger := slog.Default()
	ctx := cmd.Context()

	metrics := newParityMetrics(network)
	if globalFlags.metricsAddr != "" {
		metricsServer := serveMetrics(globalFlags.metricsAddr, logger)
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(
				context.Background(), 5*time.Second,
			)
			defer cancel()
			_ = metricsServer.Shutdown(shutdownCtx) //nolint:errcheck
		}()
	}

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
		"network", network,
		"dingo_addr", globalFlags.dingoAddr,
		"cardano_addr", globalFlags.cardanoAddr,
		"fallback_interval", fallbackInterval,
	)

	fallback := time.NewTimer(fallbackInterval)
	defer fallback.Stop()

	for {
		runWatchCycle(
			globalFlags.dingoAddr,
			globalFlags.cardanoAddr,
			magic,
			logger,
			metrics,
		)

		if !fallback.Stop() {
			select {
			case <-fallback.C:
			default:
			}
		}
		fallback.Reset(fallbackInterval)

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

// runWatchCycle runs one Check and hands its outcome to handleCheckResult.
// Splitting the network call from the outcome-handling logic keeps the
// latter unit-testable without a live node.
func runWatchCycle(
	dingoAddr, cardanoAddr string,
	magic uint32,
	logger *slog.Logger,
	metrics *parityMetrics,
) {
	result, err := nodeparity.Check(dingoAddr, cardanoAddr, magic)
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
