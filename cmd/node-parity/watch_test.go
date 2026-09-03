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
		SkipReason: nodeparity.SkipTipAdvanced,
		SkipDetail: "tip advanced during the query round trip",
	}, nil, logger, metrics)

	assert.Contains(t, buf.String(), "check skipped")
	assert.Contains(t, buf.String(), "tip advanced during the query round trip")
	assert.Equal(t, float64(0), promtestutil.ToFloat64(metrics.checksTotal))
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			metrics.checksSkippedTotal.WithLabelValues(
				nodeparity.SkipTipAdvanced,
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
				nodeparity.SkipTipAdvanced,
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
		elapsed := time.Since(start)
		assert.InDelta(
			t, interval, elapsed, float64(50*time.Millisecond),
			"an unfired timer must be rearmed for the full interval",
		)
	case <-time.After(interval + time.Second):
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
