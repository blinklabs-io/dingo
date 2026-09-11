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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// newTipGapTestLedgerState builds the smallest LedgerState that can run
// handleSlotTicks: initialised metrics, a published tip snapshot, and a slot
// tick channel. reachedTip stays false, so the loop takes its catch-up
// `continue` immediately after the tip-gap update this test is about.
func newTipGapTestLedgerState(
	t *testing.T,
	tipSlot uint64,
	report ReportTipGapFunc,
) (*LedgerState, chan SlotTick, *stateMetrics) {
	t.Helper()

	ticks := make(chan SlotTick, 1)
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
			ReportTipGapFunc: report,
		},
		slotTickChan: ticks,
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(tipSlot, []byte("tip")),
	}
	ls.publishSnapshotsLocked()
	return ls, ticks, &ls.metrics
}

func gaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	t.Helper()
	var metric dto.Metric
	require.NoError(t, gauge.Write(&metric))
	require.NotNil(t, metric.Gauge)
	return metric.Gauge.GetValue()
}

// TestHandleSlotTicksReportsTipGap pins the producer end of the readiness
// signal: every slot tick hands the health reporter the same wall-clock-to-tip
// distance it publishes as dingo_tip_gap_slots. Reading it from the ledger
// rather than scraping Prometheus is what lets /readyz work with the metrics
// listener disabled.
func TestHandleSlotTicksReportsTipGap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tipSlot  uint64
		tickSlot uint64
		want     uint64
	}{
		{
			name:     "tip behind wall clock",
			tipSlot:  500,
			tickSlot: 1750,
			want:     1250,
		},
		{name: "tip at wall clock", tipSlot: 900, tickSlot: 900, want: 0},
		// A tip ahead of the slot clock is not a negative gap.
		{name: "tip ahead of wall clock", tipSlot: 900, tickSlot: 880, want: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			reported := make(chan uint64, 4)
			ls, ticks, metrics := newTipGapTestLedgerState(
				t,
				test.tipSlot,
				func(gap uint64) { reported <- gap },
			)

			done := make(chan struct{})
			go func() {
				ls.handleSlotTicks()
				close(done)
			}()

			ticks <- SlotTick{Slot: test.tickSlot}

			select {
			case got := <-reported:
				assert.Equal(t, test.want, got)
			case <-time.After(10 * time.Second):
				t.Fatal("slot tick did not report a tip gap")
			}

			close(ticks)
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("handleSlotTicks did not return")
			}

			// The reported value and the exported gauge must not drift.
			assert.Equal(
				t,
				float64(test.want),
				gaugeValue(t, metrics.tipGapSlots),
			)
		})
	}
}

// A nil reporter is the configuration every ledger test and every
// non-node caller uses; it must not panic.
func TestHandleSlotTicksToleratesNilTipGapReporter(t *testing.T) {
	t.Parallel()

	ls, ticks, _ := newTipGapTestLedgerState(t, 100, nil)
	done := make(chan struct{})
	go func() {
		ls.handleSlotTicks()
		close(done)
	}()
	ticks <- SlotTick{Slot: 200}
	close(ticks)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("handleSlotTicks did not return")
	}
}
