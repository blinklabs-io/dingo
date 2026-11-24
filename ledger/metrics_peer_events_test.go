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
	"log/slog"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func histogramSnapshot(t *testing.T, h prometheus.Histogram) *dto.Histogram {
	t.Helper()
	var m dto.Metric
	require.NoError(t, h.Write(&m))
	return m.GetHistogram()
}

// An applied peer-driven rollback is observed with its depth in blocks; a
// no-op rollback to the current tip is not counted.
func TestPeerRollbackAppliedObservesDepth(t *testing.T) {
	t.Parallel()

	f := newBehindPeerFixture(t)
	hist := f.ls.metrics.peerRollbackDepthBlocks

	f.sendHeader(1, "")
	f.sendChainsync(ChainsyncEvent{
		Rollback: true,
		Point:    f.pointAtDepth(0),
		Tip:      f.peerTipAhead(),
	})
	require.Zero(t, histogramSnapshot(t, hist).GetSampleCount())

	f.sendChainsync(ChainsyncEvent{
		Rollback: true,
		Point:    f.pointAtDepth(2),
		Tip:      f.peerTipAhead(),
	})
	require.Equal(t, f.pointAtDepth(2), f.ls.chain.Tip().Point)
	snap := histogramSnapshot(t, hist)
	require.Equal(t, uint64(1), snap.GetSampleCount())
	require.Equal(t, float64(2), snap.GetSampleSum())
}

// rollbackHook runs fn once, when the handler logs that it is about to roll
// back, which is after the pre-rollback tip sample and before the chain
// mutation.
type rollbackHook struct {
	slog.Handler
	fn   func()
	done bool
}

func (h *rollbackHook) Handle(ctx context.Context, r slog.Record) error {
	if !h.done && strings.HasPrefix(r.Message, "ledger: rolling back to") {
		h.done = true
		h.fn()
	}
	return h.Handler.Handle(ctx, r)
}

// The observed depth is the number of blocks the chain rollback removed, even
// when a concurrent add advances the tip between the handler's tip sample and
// the rollback.
func TestPeerRollbackDepthCountsBlocksRemovedUnderChainLock(t *testing.T) {
	t.Parallel()

	f := newBehindPeerFixture(t)
	tip := f.blocks[len(f.blocks)-1]
	hook := &rollbackHook{
		Handler: f.ls.config.Logger.Handler(),
		fn: func() {
			require.NoError(t, f.ls.chain.AddRawBlocks(
				context.Background(),
				[]chain.RawBlock{{
					Slot:        tip.Slot + 10,
					Hash:        testHashBytes("concurrent-add"),
					BlockNumber: tip.BlockNumber + 1,
					Type:        1,
					PrevHash:    tip.Hash,
					Cbor:        []byte{0x80},
				}},
			))
		},
	}
	f.ls.config.Logger = slog.New(hook)

	f.sendHeader(1, "")
	f.sendChainsync(ChainsyncEvent{
		Rollback: true,
		Point:    f.pointAtDepth(0),
		Tip:      f.peerTipAhead(),
	})
	f.sendChainsync(ChainsyncEvent{
		Rollback: true,
		Point:    f.pointAtDepth(2),
		Tip:      f.peerTipAhead(),
	})

	require.True(t, hook.done)
	snap := histogramSnapshot(t, f.ls.metrics.peerRollbackDepthBlocks)
	require.Equal(t, f.pointAtDepth(2), f.ls.chain.Tip().Point)
	require.Equal(t, uint64(1), snap.GetSampleCount())
	require.Equal(t, float64(3), snap.GetSampleSum())
}

// A rollback the node refuses (here: past K from a divergent peer) applies
// nothing and must not be counted.
func TestPeerRollbackRefusedIsNotObserved(t *testing.T) {
	t.Parallel()

	f := newBehindPeerFixture(t)
	f.sendChainsync(ChainsyncEvent{
		Rollback: true,
		Point:    f.pointAtDepth(12),
		Tip:      f.peerTipAhead(),
	})
	require.Zero(
		t,
		histogramSnapshot(t, f.ls.metrics.peerRollbackDepthBlocks).
			GetSampleCount(),
	)
}

func TestHeaderVerificationDeferredCounter(t *testing.T) {
	t.Parallel()

	var m stateMetrics
	// Safe before init, like the other metric helpers.
	m.incHeaderVerificationDeferred()
	reg := prometheus.NewRegistry()
	m.init(reg)
	m.incHeaderVerificationDeferred()
	m.incHeaderVerificationDeferred()
	require.Equal(
		t,
		float64(2),
		syncMetricValue(
			t, reg, "dingo_ledger_header_verification_deferred_total",
		),
	)
}
