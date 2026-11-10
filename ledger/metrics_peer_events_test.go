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
	"bytes"
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
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

// The sweep exports the floor it persisted and the rows it deleted.
func TestConsumedUtxoSweepMetrics(t *testing.T) {
	t.Parallel()

	fixture := seedRetentionReconstructionFixture(t)
	reg := prometheus.NewRegistry()
	fixture.ls.metrics.init(reg)
	require.NoError(
		t,
		fixture.db.MarkUtxosDeletedAtSlot(
			context.Background(),
			nil,
			[]types.UtxoKey{
				{TxId: bytes.Repeat([]byte{0x11}, 32), OutputIdx: 0},
			},
			150,
		),
	)
	pruned, floor, err := fixture.ls.pruneConsumedUtxosUpTo(
		context.Background(), 200, 50, 1000,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		float64(floor),
		promtestutil.ToFloat64(fixture.ls.metrics.consumedUtxoPruneFloorSlot),
	)
	require.Equal(
		t,
		float64(pruned),
		promtestutil.ToFloat64(fixture.ls.metrics.consumedUtxoSweepRowsDeleted),
	)
	require.Positive(t, pruned)
}
