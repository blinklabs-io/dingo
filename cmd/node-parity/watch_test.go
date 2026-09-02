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
	"testing"

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
// it as a warning and record neither a completed check nor a skip --
// nodeparity.Check never returned a result to record anything about.
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
