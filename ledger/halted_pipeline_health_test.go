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
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/blinklabs-io/dingo/internal/health"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
)

const (
	haltedPipelineTipSlot  = uint64(500)
	haltedPipelineGapLimit = uint64(100)
)

// probeStatus serves path on mux and returns the HTTP status code.
func probeStatus(t *testing.T, mux http.Handler, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

// assertHaltedPipelineHealth halts the ledger pipeline with halt, then drives
// real slot ticks past the frozen tip and checks the probes against the tip
// gap the ledger reports: /healthz stays 200 (documented: a halted pipeline
// is not a liveness failure), while /readyz is 200 only while the gap is
// within the configured bound.
func assertHaltedPipelineHealth(
	t *testing.T,
	ls *LedgerState,
	ticks chan SlotTick,
	gap *atomic.Uint64,
	reported chan uint64,
	runHalted func(),
) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		ls.handleSlotTicks(context.Background())
	}()
	t.Cleanup(func() {
		close(ticks)
		<-done
	})

	runHalted()
	require.Equal(
		t, 1.0, promtestutil.ToFloat64(ls.metrics.pipelineHalted),
		"the pipeline must be halted",
	)

	mux := health.NewMux(
		nil,
		func() (uint64, bool) { return gap.Load(), true },
		haltedPipelineGapLimit,
	)
	tick := func(slot uint64) {
		ticks <- SlotTick{Slot: slot}
		testutil.RequireReceive(
			t, reported, testutil.AsyncWait, "slot tick must report a gap",
		)
	}

	// Within the bound the node still reads ready: the gap is wall slot
	// minus the frozen tip, so a recently halted node is not yet stale.
	tick(haltedPipelineTipSlot + haltedPipelineGapLimit)
	assert.Equal(t, http.StatusOK, probeStatus(t, mux, health.PathLive))
	assert.Equal(t, http.StatusOK, probeStatus(t, mux, health.PathReady),
		"readyz must be 200 while the tip gap is within the bound")

	// Past the bound the halted node must stop reporting ready, while
	// liveness is unchanged.
	tick(haltedPipelineTipSlot + haltedPipelineGapLimit + 1)
	assert.Equal(t, http.StatusOK, probeStatus(t, mux, health.PathLive),
		"healthz must stay 200 with a halted pipeline")
	assert.Equal(t, http.StatusOK, probeStatus(t, mux, health.PathHealth))
	assert.Equal(t, http.StatusServiceUnavailable,
		probeStatus(t, mux, health.PathReady),
		"readyz must be 503 once the halted tip trails the wall clock "+
			"by more than the bound")
	assert.Equal(t, haltedPipelineTipSlot, ls.Tip().Point.Slot,
		"a halted pipeline must not advance the tip")
}

func newHaltedPipelineLedger(t *testing.T) (
	*LedgerState, chan SlotTick, *atomic.Uint64, chan uint64,
) {
	t.Helper()
	var gap atomic.Uint64
	reported := make(chan uint64, 4)
	ls, ticks, _ := newTipGapTestLedgerState(
		t, haltedPipelineTipSlot, func(g uint64) {
			gap.Store(g)
			reported <- g
		},
	)
	return ls, ticks, &gap, reported
}

// TestHaltedLedgerPipelineHealthEndpointsAgreeWithProgress covers a pipeline
// halted by errHaltLedgerPipeline.
func TestHaltedLedgerPipelineHealthEndpointsAgreeWithProgress(t *testing.T) {
	t.Parallel()
	ls, ticks, gap, reported := newHaltedPipelineLedger(t)
	assertHaltedPipelineHealth(t, ls, ticks, gap, reported, func() {
		ls.ledgerProcessBlocksWithAttempt(
			t.Context(),
			func(context.Context) error { return errHaltLedgerPipeline },
		)
	})
}

// TestStuckLedgerPipelineHealthEndpointsAgreeWithProgress covers a pipeline
// halted by repeated restarts that never move the tip (the no-progress
// threshold), under a fake clock so the restart backoff costs no wall time.
func TestStuckLedgerPipelineHealthEndpointsAgreeWithProgress(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ls, ticks, gap, reported := newHaltedPipelineLedger(t)
		attempts := 0
		assertHaltedPipelineHealth(t, ls, ticks, gap, reported, func() {
			ls.ledgerProcessBlocksWithAttempt(
				t.Context(),
				func(context.Context) error {
					attempts++
					return errors.Join(errRestartLedgerPipeline, errors.New("same tip"))
				},
			)
		})
		assert.Greater(t, attempts, noProgressStuckThreshold)
	})
}
