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
	"errors"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/nodeparity"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testLogger returns a logger writing plain text into buf, so a test can
// assert on what got logged the same way an operator would read it.
func testLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

// TestHandleCheckResult_Error covers a Check call that itself failed (a
// dial or query error, not a discarded cycle): handleCheckResult must log
// it as a warning, record neither a completed check nor a skip --
// nodeparity.Check never returned a result to record anything about -- and
// record it via checkErrorsTotal instead, so a run of persistent check
// errors (e.g. a misconfigured address) is distinguishable from the tool
// simply not attempting anything (see NodeParityCheckErrors in
// docs/dashboards/alerts.yaml).
func TestHandleCheckResult_Error(t *testing.T) {
	metrics, _ := newTestParityMetrics(t)
	logger, buf := testLogger()

	handleCheckResult(
		nil,
		errors.New("dial dingo: connection refused"),
		logger,
		metrics,
	)

	assert.Contains(t, buf.String(), "check error")
	assert.Contains(t, buf.String(), "connection refused")
	assert.Equal(t, float64(0), promtestutil.ToFloat64(metrics.checksTotal))
	assert.Equal(
		t, float64(1), promtestutil.ToFloat64(metrics.checkErrorsTotal),
	)
}

// TestHandleCheckResult_Skipped covers a discarded cycle (the two nodes
// never held a stable common tip): handleCheckResult must record it via
// recordSkip under its specific reason code, log the human-readable
// detail, and -- critically -- must not increment checksTotal, so a
// skipped cycle can never be mistaken for a clean match downstream.
func TestHandleCheckResult_Skipped(t *testing.T) {
	metrics, _ := newTestParityMetrics(t)
	logger, buf := testLogger()

	handleCheckResult(&nodeparity.CheckResult{
		Skipped:    true,
		SkipReason: nodeparity.SkipTipMismatch,
		SkipDetail: "tips did not match: dingo at slot 1, cardano-node at slot 2",
	}, nil, logger, metrics)

	assert.Contains(t, buf.String(), "check skipped")
	assert.Contains(t, buf.String(), "tips did not match")
	assert.Equal(t, float64(0), promtestutil.ToFloat64(metrics.checksTotal))
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			metrics.checksSkippedTotal.WithLabelValues(
				nodeparity.SkipTipMismatch,
			),
		),
	)
	assert.Equal(
		t, float64(0), promtestutil.ToFloat64(metrics.checkErrorsTotal),
		"a skipped cycle is not a check error",
	)
}

// TestHandleCheckResult_Matched covers a completed, clean comparison:
// handleCheckResult must record it as a completed check (recordCheck) and
// log it at Info level (a match is routine, not a warning), without
// recording any skip.
func TestHandleCheckResult_Matched(t *testing.T) {
	metrics, _ := newTestParityMetrics(t)
	logger, buf := testLogger()

	handleCheckResult(&nodeparity.CheckResult{
		Tip: nodeparity.Tip{Slot: 12345, BlockNumber: 678},
	}, nil, logger, metrics)

	assert.Contains(t, buf.String(), "check matched")
	assert.Contains(t, buf.String(), "12345")
	assert.Equal(t, float64(1), promtestutil.ToFloat64(metrics.checksTotal))
	assert.Equal(
		t,
		float64(0),
		promtestutil.ToFloat64(
			metrics.checksSkippedTotal.WithLabelValues(
				nodeparity.SkipTipMismatch,
			),
		),
	)
	assert.Equal(
		t, float64(0), promtestutil.ToFloat64(metrics.checkErrorsTotal),
		"a clean match is not a check error",
	)
}

// TestHandleCheckResult_Diverged covers a completed comparison that found
// a real difference: handleCheckResult must still record it as a
// completed check (a divergence is a completed check with a non-empty
// result, not a skip) via recordCheck -- which is what actually bumps the
// per-field divergence counters -- and log it at Warn level with the diff
// content included, so an operator scanning logs sees what diverged
// without cross-referencing metrics.
func TestHandleCheckResult_Diverged(t *testing.T) {
	metrics, _ := newTestParityMetrics(t)
	logger, buf := testLogger()

	handleCheckResult(&nodeparity.CheckResult{
		Tip: nodeparity.Tip{Slot: 999, BlockNumber: 42},
		Diff: nodeparity.Diff{
			ProtocolParamsDiff: "protocol parameters differ",
		},
	}, nil, logger, metrics)

	assert.Contains(t, buf.String(), "ledger state diverged")
	assert.Contains(t, buf.String(), "protocol parameters differ")
	assert.Equal(t, float64(1), promtestutil.ToFloat64(metrics.checksTotal))
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			metrics.divergenceTotal.WithLabelValues("protocol_params"),
		),
	)
}

// TestWatchCommand_FallbackIntervalMustBePositive covers --fallback-interval's
// validation: watchRun must reject a zero or negative value before it ever
// starts a Watcher, the same way a zero poll interval would spin in a
// tight loop forever. Uses addresses that would fail fast if dialed, but
// the point of this test is that dialing never happens at all -- the
// validation error must return first.
func TestWatchCommand_FallbackIntervalMustBePositive(t *testing.T) {
	withGlobalFlags(t, "preview", "127.0.0.1:1", "127.0.0.1:1")

	cmd := watchCommand()
	cmd.SetContext(context.Background())
	require.NoError(t, cmd.Flags().Set("fallback-interval", "0s"))

	err := watchRun(cmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--fallback-interval must be positive")
}

// TestResetFallbackTimer_NotYetFiredGetsFullInterval covers the ordinary
// case: a cycle finishes well within --fallback-interval, so Stop()
// successfully cancels the still-pending timer, and the next one is armed
// for a full fresh interval.
func TestResetFallbackTimer_NotYetFiredGetsFullInterval(t *testing.T) {
	const interval = 200 * time.Millisecond
	fallback := time.NewTimer(time.Hour) // won't fire during this test
	t.Cleanup(func() { fallback.Stop() })

	start := time.Now()
	resetFallbackTimer(fallback, interval)

	select {
	case <-fallback.C:
		// A loose bound, not InDelta(50ms): elapsed includes real timer-
		// delivery and scheduler latency on top of the nominal interval,
		// which a tight tolerance can exceed under -race on a loaded CI
		// runner. The property that actually matters is "rearmed for
		// approximately the full interval, not near-instantly and not for
		// something drastically longer" -- matching the generous
		// (seconds-scale) tolerances internal/test/testutil's own helpers
		// use for exactly this reason.
		elapsed := time.Since(start)
		assert.GreaterOrEqual(
			t, elapsed, interval,
			"an unfired timer must not fire before the full interval elapses",
		)
		assert.Less(
			t,
			elapsed,
			interval+2*time.Second,
			"an unfired timer must be rearmed for the full interval, not something drastically longer",
		)
	case <-time.After(interval + 3*time.Second):
		t.Fatal("fallback timer never fired after being reset")
	}
}

// TestResetFallbackTimer_AlreadyFiredSchedulesImmediately covers a check
// that overran --fallback-interval: by the time resetFallbackTimer runs,
// the timer has already fired on its own (Stop returns false). The next
// cycle must be scheduled immediately (Reset(0)), not after another full
// interval, so an overrun doesn't compound into "at least 2x
// --fallback-interval between checks."
func TestResetFallbackTimer_AlreadyFiredSchedulesImmediately(t *testing.T) {
	const interval = time.Hour // would fail the test if used here by mistake
	fallback := time.NewTimer(10 * time.Millisecond)
	t.Cleanup(func() { fallback.Stop() })

	// Wait for a genuine fire, observed via the channel: Stop() only
	// reliably reports "already fired" (false) once the fire has actually
	// been delivered, not merely once its deadline has passed in wall-clock
	// terms -- Go's runtime doesn't necessarily process an idle timer's
	// firing the instant its duration elapses (confirmed directly: a 10ms
	// timer's Stop() still returned true, "not yet fired," after sleeping
	// 50ms with nothing ever touching its channel). Draining the channel
	// ourselves first, simulating a check that ran long enough to overrun
	// the fallback, puts the timer in the same already-fired, drained
	// state resetFallbackTimer must handle.
	<-fallback.C

	done := make(chan struct{})
	go func() {
		resetFallbackTimer(fallback, interval)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("resetFallbackTimer blocked draining an already-fired timer")
	}

	select {
	case <-fallback.C:
	case <-time.After(time.Second):
		t.Fatal(
			"an already-fired fallback timer must be rescheduled immediately, not after a full fresh interval",
		)
	}
}

// TestRunWatchCycle_BoundedByTimeoutAgainstStalledPeer covers the fix for a
// stuck watch loop: without a per-cycle timeout, a peer that accepts a
// connection and then never responds would block Check (and so this whole
// loop, which calls it synchronously) indefinitely, since Check's own
// context-cancellation handling only reacts to ctx itself being cancelled
// (process shutdown), not a per-cycle bound -- silently defeating the
// fallback timer's guarantee of activity within --fallback-interval, since
// nothing schedules a fresh cycle until the stuck one returns.
// runWatchCycle must self-abort within timeout+margin against a stalled
// dingo-addr peer, recording it as a check error like any other Check
// failure, rather than hanging until the caller's own ctx cancels.
func TestRunWatchCycle_BoundedByTimeoutAgainstStalledPeer(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		// Accept and hold the connection open without ever writing to it,
		// so the handshake never completes unless bounded by the cycle
		// timeout below.
		t.Cleanup(func() { _ = conn.Close() })
	}()

	metrics, _ := newTestParityMetrics(t)
	logger, buf := testLogger()

	const timeout = 500 * time.Millisecond
	done := make(chan struct{})
	go func() {
		runWatchCycle(
			context.Background(), timeout,
			listener.Addr().String(), "127.0.0.1:1", 42,
			logger, metrics,
		)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout + 5*time.Second):
		t.Fatal(
			"runWatchCycle did not return within timeout+margin against a stalled peer",
		)
	}
	assert.Contains(t, buf.String(), "check error")
	assert.Equal(
		t, float64(1), promtestutil.ToFloat64(metrics.checkErrorsTotal),
	)
}

// TestWatchCommand_CheckTimeoutMustBePositive covers --check-timeout's own
// validation, the same way --fallback-interval's is validated: a zero or
// negative value would make every full-mode cycle self-cancel instantly.
func TestWatchCommand_CheckTimeoutMustBePositive(t *testing.T) {
	withGlobalFlags(t, "preview", "127.0.0.1:1", "127.0.0.1:1")

	cmd := watchCommand()
	cmd.SetContext(context.Background())
	require.NoError(t, cmd.Flags().Set("check-timeout", "0s"))

	err := watchRun(cmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--check-timeout must be positive")
}

// TestWatchRunFull_ChecksAreBoundedByCheckTimeoutNotFallbackInterval is a
// regression test for blinklabs-io/dingo#1900's audit finding: full mode's
// default --fallback-interval (2m) used to double as runWatchCycle's own
// per-cycle deadline, so a genuinely slow (not stuck) full comparison --
// measured at 7-9+ minutes against a Preview-scale node -- self-cancelled
// long before it could complete, making full mode fail by default out of
// the box. --check-timeout must now bound the cycle instead, independent of
// the much shorter --fallback-interval: a cycle against a peer that never
// responds must survive past --fallback-interval without being recorded as
// a check error, and only fail once --check-timeout itself elapses.
func TestWatchRunFull_ChecksAreBoundedByCheckTimeoutNotFallbackInterval(
	t *testing.T,
) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		// Accept and hold the connection open without ever writing to it, so
		// the handshake never completes on its own.
		t.Cleanup(func() { _ = conn.Close() })
	}()

	withGlobalFlags(t, "preview", listener.Addr().String(), "127.0.0.1:1")
	metrics, _ := newTestParityMetrics(t)
	logger, _ := testLogger()

	const fallbackInterval = 100 * time.Millisecond
	const checkTimeout = 1500 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = watchRunFull(
			ctx,
			42,
			fallbackInterval,
			checkTimeout,
			logger,
			metrics,
		)
		close(done)
	}()

	// Comfortably past fallbackInterval but well before checkTimeout: if the
	// cycle were (incorrectly) bounded by fallbackInterval, this would
	// already observe a recorded check error.
	time.Sleep(fallbackInterval + 400*time.Millisecond)
	assert.Equal(
		t,
		float64(0),
		promtestutil.ToFloat64(metrics.checkErrorsTotal),
		"a stalled cycle must not be cut off at --fallback-interval; it must run until --check-timeout",
	)

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("watchRunFull did not stop after its context was cancelled")
	}
}

// TestHandleIncrementalSessionError_RecordsCheckError covers the wiring for
// blinklabs-io/dingo#1900's incremental-mode audit finding: a per-block
// query failure ending an incrementalSession must be recorded via the same
// checkErrorsTotal counter any other Check failure uses, so the existing
// NodeParityCheckErrors alert rule can actually see this failure class.
func TestHandleIncrementalSessionError_RecordsCheckError(t *testing.T) {
	metrics, _ := newTestParityMetrics(t)

	handleIncrementalSessionError(errors.New("dingo query: boom"), metrics)

	assert.Equal(
		t, float64(1), promtestutil.ToFloat64(metrics.checkErrorsTotal),
	)
}

// TestWatchCommand_ModeMustBeFullOrIncremental covers --mode's validation:
// only "full" and "incremental" are accepted, matching --network's own
// closed-set style (main_test.go's TestRequireNetwork) rather than passing
// an arbitrary string through to dispatch logic that would otherwise fail
// less clearly deeper in the call stack.
func TestWatchCommand_ModeMustBeFullOrIncremental(t *testing.T) {
	withGlobalFlags(t, "preview", "127.0.0.1:1", "127.0.0.1:1")

	cmd := watchCommand()
	cmd.SetContext(context.Background())
	require.NoError(t, cmd.Flags().Set("mode", "bogus"))

	err := watchRun(cmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--mode must be 'full' or 'incremental'")
}

// TestWatchCommand_IncrementalRequiresCursorFile covers --mode=incremental's
// own required flag: unlike full mode (whose --fallback-interval has a sane
// default), incremental mode cannot run at all without somewhere to persist
// its cursor, so this must be validated before anything dials either node.
func TestWatchCommand_IncrementalRequiresCursorFile(t *testing.T) {
	withGlobalFlags(t, "preview", "127.0.0.1:1", "127.0.0.1:1")

	cmd := watchCommand()
	cmd.SetContext(context.Background())
	require.NoError(t, cmd.Flags().Set("mode", "incremental"))

	err := watchRun(cmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--cursor-file is required")
}

// TestWatchCommand_IncrementalRequiresPositiveFullCheckInterval covers
// --full-check-interval's validation: a zero value would mean incremental
// mode never advances past forcing a full checkpoint every single block,
// defeating the point of the mode, so it is rejected the same way
// --fallback-interval's zero is rejected for full mode.
func TestWatchCommand_IncrementalRequiresPositiveFullCheckInterval(
	t *testing.T,
) {
	withGlobalFlags(t, "preview", "127.0.0.1:1", "127.0.0.1:1")

	cmd := watchCommand()
	cmd.SetContext(context.Background())
	require.NoError(t, cmd.Flags().Set("mode", "incremental"))
	require.NoError(t, cmd.Flags().Set(
		"cursor-file", filepath.Join(t.TempDir(), "cursor.json"),
	))
	require.NoError(t, cmd.Flags().Set("full-check-interval", "0"))

	err := watchRun(cmd, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--full-check-interval must be positive")
}

// TestHandleIncrementalBlockCheck_Matched and
// TestHandleIncrementalBlockCheck_Diverged cover the incremental per-block
// outcome handler the same way TestHandleCheckResult_Matched/_Diverged
// cover full mode's: logged at the right level, and recorded via
// incrementalBlocksTotal/incrementalMismatchTotal rather than full mode's
// checksTotal.
func TestHandleIncrementalBlockCheck_Matched(t *testing.T) {
	metrics, _ := newTestParityMetrics(t)
	logger, buf := testLogger()

	handleIncrementalBlockCheck(
		nodeparity.Tip{Slot: 111, BlockNumber: 22},
		nodeparity.Diff{},
		logger, metrics,
	)

	assert.Contains(t, buf.String(), "incremental block matched")
	assert.Equal(
		t, float64(1), promtestutil.ToFloat64(metrics.incrementalBlocksTotal),
	)
	assert.Equal(
		t, float64(0), promtestutil.ToFloat64(metrics.incrementalMismatchTotal),
	)
}

func TestHandleIncrementalBlockCheck_Diverged(t *testing.T) {
	metrics, _ := newTestParityMetrics(t)
	logger, buf := testLogger()

	handleIncrementalBlockCheck(
		nodeparity.Tip{Slot: 111, BlockNumber: 22},
		nodeparity.Diff{
			UTxO: []string{
				"utxo abc#0 should be spent but is still present in dingo: x",
			},
		},
		logger,
		metrics,
	)

	assert.Contains(t, buf.String(), "incremental block diverged")
	assert.Contains(t, buf.String(), "should be spent")
	assert.Equal(
		t, float64(1), promtestutil.ToFloat64(metrics.incrementalBlocksTotal),
	)
	assert.Equal(
		t, float64(1), promtestutil.ToFloat64(metrics.incrementalMismatchTotal),
	)
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(metrics.divergenceTotal.WithLabelValues("utxo")),
		"an incremental mismatch must also count toward the shared divergenceTotal series",
	)
}

// TestHandleIncrementalFullCheck_RecordsReason covers the reason label
// incremental mode's own full checkpoints are recorded under, distinct from
// full mode's per-block-triggered checks (which never call this function at
// all).
func TestHandleIncrementalFullCheck_RecordsReason(t *testing.T) {
	metrics, _ := newTestParityMetrics(t)
	logger, _ := testLogger()

	handleIncrementalFullCheck(
		nodeparity.FullCheckEpochTransition,
		&nodeparity.CheckResult{Tip: nodeparity.Tip{Slot: 1}},
		nil, logger, metrics,
	)

	assert.Equal(
		t, float64(1),
		promtestutil.ToFloat64(
			metrics.fullCheckTriggersTotal.WithLabelValues("epoch_transition"),
		),
	)
	assert.Equal(
		t, float64(0),
		promtestutil.ToFloat64(
			metrics.fullCheckTriggersTotal.WithLabelValues("rollback"),
		),
		"only the reason actually passed must be incremented",
	)
	assert.Equal(t, float64(1), promtestutil.ToFloat64(metrics.checksTotal))
}
