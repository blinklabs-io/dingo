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
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// syncMetricValue returns the current value of the named single-series
// counter or gauge, failing the test when it is not exported.
func syncMetricValue(
	t *testing.T,
	reg *prometheus.Registry,
	name string,
) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		require.Len(
			t, family.GetMetric(), 1, "%s must be a single series", name,
		)
		m := family.GetMetric()[0]
		require.Empty(t, m.GetLabel(), "%s must carry no labels", name)
		if m.GetCounter() != nil {
			return m.GetCounter().GetValue()
		}
		return m.GetGauge().GetValue()
	}
	require.Failf(t, "metric not exported", "%s", name)
	return 0
}

// newSyncMetricsLedger builds a ledger around a chain holding headerCount
// queued headers, with a blockfetch dispatch that always succeeds, and
// exports its sync metrics on a private registry.
func newSyncMetricsLedger(
	t *testing.T,
	headerCount int,
) (*LedgerState, *prometheus.Registry) {
	t.Helper()
	testChain, _ := buildDeepCatchupChain(t, headerCount)
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				ouroboros.ConnectionId,
				ocommon.Point,
				ocommon.Point,
			) (uint64, error) {
				return 1, nil
			},
		},
	}
	ls.publishSnapshotsLocked()
	reg := prometheus.NewRegistry()
	ls.metrics.init(reg)
	ls.registerSyncGauges(reg)
	return ls, reg
}

func TestSyncMetricsExportHeaderQueueLengthAndCapacity(t *testing.T) {
	t.Parallel()
	ls, reg := newSyncMetricsLedger(t, 3)

	require.Equal(t, float64(3), syncMetricValue(
		t, reg, "dingo_chain_header_queue_length",
	))
	require.Equal(
		t,
		float64(ls.chain.MaxQueuedHeaders()),
		syncMetricValue(t, reg, "dingo_chain_header_queue_capacity"),
	)
	require.Equal(t, float64(0), syncMetricValue(
		t, reg, "dingo_chain_header_queue_full_total",
	))
}

// The counter is fed from the chain's own rejection, so a header refused on
// any path is counted. Driven through the ledger a real chain manager is
// wired into, since that wiring is what NewLedgerState installs.
func TestSyncMetricsCountHeaderQueueFullRejections(t *testing.T) {
	t.Parallel()
	fixture := newChainsyncRollbackFixture(t)
	reg := prometheus.NewRegistry()
	fixture.ls.metrics.init(reg)
	fixture.ls.registerSyncGauges(reg)
	limit := fixture.ls.chain.MaxQueuedHeaders()
	prevHash := fixture.currentTip.Point.Hash
	prevBlockNumber := fixture.currentTip.BlockNumber
	add := func(i int) error {
		h := mockHeader{
			hash: lcommon.NewBlake2b256(
				testHashBytes(fmt.Sprintf("queue-full-%d", i)),
			),
			prevHash:    lcommon.NewBlake2b256(prevHash),
			blockNumber: prevBlockNumber + 1,
			slot:        fixture.currentTip.Point.Slot + uint64(i) + 1,
		}
		err := fixture.ls.chain.AddBlockHeader(context.Background(), h)
		if err == nil {
			prevHash = h.Hash().Bytes()
			prevBlockNumber = h.BlockNumber()
		}
		return err
	}
	for i := range limit {
		require.NoError(t, add(i))
	}
	require.Equal(t, float64(0), syncMetricValue(
		t, reg, "dingo_chain_header_queue_full_total",
	))
	require.Equal(t, float64(limit), syncMetricValue(
		t, reg, "dingo_chain_header_queue_length",
	))

	require.ErrorIs(t, add(limit), chain.ErrHeaderQueueFull)

	require.Equal(t, float64(1), syncMetricValue(
		t, reg, "dingo_chain_header_queue_full_total",
	))
}

func TestSyncMetricsExportBatchInFlight(t *testing.T) {
	t.Parallel()
	ls, reg := newSyncMetricsLedger(t, 3)
	connId := testChainsyncConnId(6401, 3001)
	require.Zero(t, syncMetricValue(
		t, reg, "dingo_ledger_blockfetch_batch_in_flight",
	))

	ls.chainsyncBlockfetchMutex.Lock()
	require.NoError(t, ls.startQueuedBlockfetchLocked(connId, nil))
	require.NotNil(t, ls.chainsyncBlockfetchReadyChan)
	require.Equal(t, float64(1), syncMetricValue(
		t, reg, "dingo_ledger_blockfetch_batch_in_flight",
	))

	ls.blockfetchRequestRangeCleanup()
	ls.chainsyncBlockfetchMutex.Unlock()
	require.Nil(t, ls.chainsyncBlockfetchReadyChan)
	require.Zero(t, syncMetricValue(
		t, reg, "dingo_ledger_blockfetch_batch_in_flight",
	))
}

func TestSyncMetricsExportContinuationPending(t *testing.T) {
	t.Parallel()
	ls, reg := newSyncMetricsLedger(t, 3)
	connId := testChainsyncConnId(6402, 3001)
	require.Zero(t, syncMetricValue(
		t, reg, "dingo_ledger_blockfetch_continuation_pending",
	))

	// The continuation worker needs the blockfetch mutex, which the
	// scheduling caller owns, so the pending window stays open until it is
	// released.
	ls.chainsyncBlockfetchMutex.Lock()
	ls.startQueuedBlockfetchFromEventLocked(connId, connId, "test")
	require.Equal(t, float64(1), syncMetricValue(
		t, reg, "dingo_ledger_blockfetch_continuation_pending",
	))
	ls.chainsyncBlockfetchMutex.Unlock()

	testutil.WaitForCondition(t, func() bool {
		return syncMetricValue(
			t, reg, "dingo_ledger_blockfetch_continuation_pending",
		) == 0
	}, testutil.AsyncWait, "continuation must clear the pending gauge")
	ls.blockfetchContinuationWG.Wait()
	require.Equal(t, float64(1), syncMetricValue(
		t, reg, "dingo_ledger_blockfetch_batch_in_flight",
	), "the started continuation is a batch in flight")
}

func TestSyncMetricsExportBlockfetchConnectionSelected(t *testing.T) {
	t.Parallel()
	ls, reg := newSyncMetricsLedger(t, 3)
	connId := testChainsyncConnId(6403, 3001)
	require.Zero(t, syncMetricValue(
		t, reg, "dingo_ledger_blockfetch_connection_selected",
	))

	ls.chainsyncBlockfetchMutex.Lock()
	require.NoError(t, ls.startQueuedBlockfetchOnLocked(connId, nil))
	ls.chainsyncBlockfetchMutex.Unlock()
	require.Equal(t, float64(1), syncMetricValue(
		t, reg, "dingo_ledger_blockfetch_connection_selected",
	))

	ls.handleConnectionClosedEvent(event.NewEvent(
		ConnectionClosedEventType,
		ConnectionClosedEvent{ConnectionId: connId},
	))
	require.Zero(t, syncMetricValue(
		t, reg, "dingo_ledger_blockfetch_connection_selected",
	))
}

func TestSyncMetricsExportLastBlockAdded(t *testing.T) {
	t.Parallel()
	f := newSiblingFixture(t)
	reg := prometheus.NewRegistry()
	f.ls.metrics.init(reg)
	f.ls.registerSyncGauges(reg)
	require.Zero(t, syncMetricValue(
		t, reg, "dingo_ledger_last_block_added_timestamp_seconds",
	))
	continuation := newSiblingTestBlock(
		t,
		f.rival.BlockNumber()+1,
		f.rival.SlotNumber()+10,
		f.rival.Hash(),
		0x44,
		0x44,
		1,
	)
	stageInFlightBlockfetchBatch(t, f.ls, continuation)
	require.Zero(t, syncMetricValue(
		t, reg, "dingo_ledger_last_block_added_timestamp_seconds",
	), "a received but unapplied block is not an added block")
	before := time.Now().Add(-time.Second)

	require.NoError(t, f.ls.flushPendingBlockfetchBlocksDeferred(nil))

	require.Equal(
		t,
		continuation.Hash().Bytes(),
		f.ls.chain.Tip().Point.Hash,
	)
	require.Greater(t, syncMetricValue(
		t, reg, "dingo_ledger_last_block_added_timestamp_seconds",
	), float64(before.Unix()))
}
