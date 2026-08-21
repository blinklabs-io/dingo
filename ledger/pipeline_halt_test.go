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

package ledger

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newPipelineLoopLedger(t *testing.T) *LedgerState {
	t.Helper()
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	return ls
}

// TestLedgerProcessBlocksStopsRetryingOnUnrepairableFailure covers the terminal
// half of issue #3261. Recovery raises errHaltLedgerPipeline once it has
// established that no local replay can change a block's verdict; the restart
// loop must then stop rather than restart into the same block forever, and must
// leave a terminal signal behind for an operator.
func TestLedgerProcessBlocksStopsRetryingOnUnrepairableFailure(t *testing.T) {
	ls := newPipelineLoopLedger(t)

	var attempts atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		ls.ledgerProcessBlocksWithAttempt(
			t.Context(),
			func(context.Context) error {
				attempts.Add(1)
				return fmt.Errorf(
					"process block batch: %w",
					errHaltLedgerPipeline,
				)
			},
		)
	}()
	testutil.RequireReceive(
		t,
		done,
		10*time.Second,
		"an unrepairable validation failure must stop the ledger pipeline",
	)

	assert.Equal(
		t,
		int64(1),
		attempts.Load(),
		"a halted pipeline must not run another attempt",
	)
	assert.Equal(
		t,
		1.0,
		promtestutil.ToFloat64(ls.metrics.pipelineHalted),
		"a halted pipeline must report its terminal state",
	)
}

// TestLedgerProcessBlocksKeepsRetryingRecoverableFailures is the negative case:
// an ordinary failure must keep restarting the pipeline. Treating every failure
// as terminal would turn a transient database or peer problem into an outage.
func TestLedgerProcessBlocksKeepsRetryingRecoverableFailures(t *testing.T) {
	ls := newPipelineLoopLedger(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var attempts atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		ls.ledgerProcessBlocksWithAttempt(
			ctx,
			func(context.Context) error {
				attempts.Add(1)
				return errors.New("transient read failure")
			},
		)
	}()
	testutil.WaitForCondition(
		t,
		func() bool { return attempts.Load() >= 3 },
		10*time.Second,
		"a recoverable failure must keep restarting the pipeline",
	)
	assert.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.pipelineHalted),
		"a retrying pipeline must not report itself halted",
	)

	cancel()
	testutil.RequireReceive(
		t,
		done,
		10*time.Second,
		"the pipeline loop must exit when its context is cancelled",
	)
}

// TestPipelineStuckAnnouncementStaysVisible covers the third ask of issue
// #3261. The stuck condition was announced at ERROR exactly once and then only
// at WARN, so a node that had stopped following the chain looked quiet to
// log-level alerting for as long as it stayed wedged.
func TestPipelineStuckAnnouncementStaysVisible(t *testing.T) {
	for consecutive := range noProgressStuckThreshold {
		assert.False(
			t,
			pipelineStuckShouldAnnounce(consecutive),
			"restart %d is not stuck yet and must not announce",
			consecutive,
		)
	}
	assert.True(
		t,
		pipelineStuckShouldAnnounce(noProgressStuckThreshold),
		"the transition into stuck must be announced",
	)

	announcements := 0
	for consecutive := noProgressStuckThreshold; consecutive <= noProgressStuckThreshold+
		4*noProgressStuckReannounceInterval; consecutive++ {
		if pipelineStuckShouldAnnounce(consecutive) {
			announcements++
		}
	}
	assert.Equal(
		t,
		5,
		announcements,
		"a persistently stuck pipeline must keep announcing itself at a fixed cadence",
	)
}

// TestResetMithrilBoundaryRejectionsRequiresAppliedTipProgress verifies that
// the trust-window tally is keyed on applied ledger progress rather than on a
// reported failing-block identity. Replay can report changing failures while
// rebuilding to the same applied tip; those are still one non-converging run.
func TestResetMithrilBoundaryRejectionsRequiresAppliedTipProgress(
	t *testing.T,
) {
	ls := &LedgerState{}

	rejections, exhausted := ls.observeMithrilBoundaryRejection(500)
	require.Equal(t, 1, rejections)
	require.False(t, exhausted)

	// Replaying to the same or an older applied tip is not progress.
	ls.resetMithrilBoundaryRejections(500)
	rejections, exhausted = ls.observeMithrilBoundaryRejection(499)
	require.Equal(t, 2, rejections)
	require.False(t, exhausted)

	// Advancing the applied high-water mark is real progress and starts a
	// later recovery run with a fresh budget.
	ls.resetMithrilBoundaryRejections(501)
	rejections, exhausted = ls.observeMithrilBoundaryRejection(501)
	assert.Equal(t, 1, rejections)
	assert.False(t, exhausted)

	// Every scheduled rewind depth refused, plus the capped retry the
	// schedule settles on: the legal rewind space is exhausted.
	for rejections < maxMithrilBoundaryRecoveryRejections {
		rejections, exhausted = ls.observeMithrilBoundaryRejection(501)
		require.False(
			t,
			exhausted,
			"%d refusals is still inside the bound",
			rejections,
		)
	}
	_, exhausted = ls.observeMithrilBoundaryRejection(501)
	require.True(
		t,
		exhausted,
		"the tally must be exhausted once every legal rewind depth has been refused",
	)
}
