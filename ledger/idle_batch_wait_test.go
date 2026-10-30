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
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestLedgerReadChainIteratorAppliesWhenNoQueuedHeaderCanExtendBatch covers
// a catch-up pass whose chain has no queued header left: every admitted
// header's block is already on the chain, so no block can follow without a
// new header admission and a fetch round trip. The pass must apply what it
// gathered instead of spending the coalescing wait.
//
// The retry interval is minutes, so a pass that enters the wait is still
// asleep when the receive deadline expires.
func TestLedgerReadChainIteratorAppliesWhenNoQueuedHeaderCanExtendBatch(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level gatherCoalesce* seams.
	shrinkGatherCoalesceRetryInterval(t, 10*time.Minute, 10)

	block, point := buildDecodableTestBlock(t, 10, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &scriptedGapLedgerReadIterator{
		ctx:    ctx,
		script: []*chain.ChainIteratorResult{{Point: point, Block: block}},
	}

	ls := &LedgerState{
		chain:  &chain.Chain{},
		config: LedgerStateConfig{Logger: testLogger()},
	}
	ls.advanceUpstreamTipSlot(farUpstreamTipSlot)
	require.Zero(t, ls.chain.HeaderCount())

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	result := testutil.RequireReceive(
		t, resultCh, 10*time.Second,
		"a pass with no queued header left waited for blocks that cannot "+
			"arrive instead of applying the block it gathered",
	)
	require.NoError(t, result.err)
	require.Len(t, result.blocks, 1)
	close(result.done)
}

// TestLedgerReadChainIteratorCoalescesWhileHeadersRemainQueued is the other
// side: while a queued header's block is still to come, gaps are bridged as
// before and the pass gathers every block into one batch.
func TestLedgerReadChainIteratorCoalescesWhileHeadersRemainQueued(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level gatherCoalesce* seams.
	shrinkGatherCoalesceRetryInterval(t, time.Millisecond, 100)

	block1, point1 := buildDecodableTestBlock(t, 10, 1)
	block2, point2 := buildDecodableTestBlock(t, 20, 2)
	block3, point3 := buildDecodableTestBlock(t, 30, 3)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &scriptedGapLedgerReadIterator{
		ctx: ctx,
		script: []*chain.ChainIteratorResult{
			{Point: point1, Block: block1},
			nil, nil,
			{Point: point2, Block: block2},
			nil,
			{Point: point3, Block: block3},
		},
	}

	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(context.Background(),
		mockHeader{
			hash:        lcommon.NewBlake2b256([]byte("queued")),
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 1,
			slot:        40,
		}))
	ls := &LedgerState{
		chain:  testChain,
		config: LedgerStateConfig{Logger: testLogger()},
	}
	ls.advanceUpstreamTipSlot(farUpstreamTipSlot)

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	result := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered a batch",
	)
	require.NoError(t, result.err)
	require.Len(t, result.blocks, 3,
		"gaps must still coalesce while a queued header can extend the batch")
	close(result.done)
}

type idleFlushTimer struct {
	delay    time.Duration
	callback func()
}

type idleFlushHarness struct {
	ls       *LedgerState
	conn     ouroboros.ConnectionId
	clock    atomic.Int64
	base     time.Time
	timers   chan idleFlushTimer
	requests chan queuedFlushTestRequest
	prevHash lcommon.Blake2b256
	next     uint64
	first    ocommon.Point
	last     ocommon.Point
}

func newIdleFlushHarness(t *testing.T) *idleFlushHarness {
	t.Helper()
	h := &idleFlushHarness{
		conn: ouroboros.ConnectionId{
			LocalAddr:  &net.TCPAddr{Port: 6000},
			RemoteAddr: &net.TCPAddr{Port: 3001},
		},
		base:     time.Unix(1_000_000, 0),
		timers:   make(chan idleFlushTimer, 16),
		requests: make(chan queuedFlushTestRequest, 4),
		prevHash: lcommon.NewBlake2b256(nil),
		next:     1,
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.ls = &LedgerState{
		ctx:   ctx,
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &h.conn
			},
			BlockfetchRequestRangeFunc: func(
				peer ouroboros.ConnectionId,
				start, end ocommon.Point,
			) (uint64, error) {
				h.requests <- queuedFlushTestRequest{peer, start, end}
				return 1, nil
			},
		},
	}
	h.ls.queuedBlockfetchNow = func() time.Time {
		return h.base.Add(time.Duration(h.clock.Load()))
	}
	h.ls.queuedBlockfetchSchedule = func(
		delay time.Duration, callback func(),
	) func() {
		h.timers <- idleFlushTimer{delay: delay, callback: callback}
		return func() {}
	}
	t.Cleanup(func() {
		h.ls.closed.Store(true)
		cancel()
		h.ls.stopQueuedBlockfetchFlush()
		h.ls.blockfetchContinuationWG.Wait()
		h.ls.chainsyncBlockfetchMutex.Lock()
		if h.ls.chainsyncBlockfetchTimeoutTimer != nil {
			h.ls.chainsyncBlockfetchTimeoutTimer.Stop()
		}
		h.ls.chainsyncBlockfetchMutex.Unlock()
	})
	return h
}

func (h *idleFlushHarness) at(offset time.Duration) {
	h.clock.Store(int64(offset))
}

// admit delivers the next header far behind the peer tip, so it queues below
// the batch threshold and arms the accumulation flush.
func (h *idleFlushHarness) admit(t *testing.T) {
	t.Helper()
	i := h.next
	h.next++
	hash := lcommon.NewBlake2b256(fmt.Appendf(nil, "idle-hdr-%d", i))
	point := ocommon.NewPoint(i, hash.Bytes())
	require.NoError(t, h.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: h.conn,
		Point:        point,
		BlockHeader: mockHeader{
			hash: hash, prevHash: h.prevHash, blockNumber: i, slot: i,
		},
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(1280, []byte("tip")),
			BlockNumber: 1280,
		},
	}))
	if i == 1 {
		h.first = point
	}
	h.last, h.prevHash = point, hash
}

func (h *idleFlushHarness) fire(t *testing.T, timer idleFlushTimer) {
	t.Helper()
	timer.callback()
	h.ls.blockfetchContinuationWG.Wait()
}

// TestQueuedBlockfetchFlushFiresOnQuietHeaderInflow covers a queue that stops
// growing: one header admitted with nothing in flight and no further header
// behind it. The flush must not hold that header for the full maximum wait.
func TestQueuedBlockfetchFlushFiresOnQuietHeaderInflow(t *testing.T) {
	t.Parallel()
	h := newIdleFlushHarness(t)
	h.at(0)
	h.admit(t)
	require.Equal(t, 1, h.ls.chain.HeaderCount())
	timer := testutil.RequireReceive(t, h.timers, time.Second,
		"header admission must arm the accumulation flush")
	require.Less(t, timer.delay, queuedBlockfetchMaxWait,
		"a single quiet header must not wait out the full maximum")

	h.at(timer.delay)
	h.fire(t, timer)
	request := testutil.RequireReceive(t, h.requests, time.Second,
		"a queue with no header inflow must fetch once the stream is idle")
	require.Equal(t, h.conn, request.conn)
	require.Equal(t, h.first, request.start)
	require.Equal(t, h.first, request.end)
	require.Empty(t, h.timers)
}

// TestQueuedBlockfetchFlushKeepsAccumulatingSteadyInflow keeps the batching
// the flush exists for: while headers keep arriving the queue is not flushed
// at the idle interval, and the first deadline still bounds the whole
// accumulation however steady the stream.
func TestQueuedBlockfetchFlushKeepsAccumulatingSteadyInflow(t *testing.T) {
	t.Parallel()
	h := newIdleFlushHarness(t)
	h.at(0)
	h.admit(t)
	timer := testutil.RequireReceive(t, h.timers, time.Second,
		"header admission must arm the accumulation flush")

	step := queuedBlockfetchIdleWait / 2
	due := timer.delay
	nextHeader := step
	for {
		for nextHeader < due {
			h.at(nextHeader)
			h.admit(t)
			nextHeader += step
		}
		h.at(due)
		h.fire(t, timer)
		if due >= queuedBlockfetchMaxWait {
			break
		}
		require.Empty(t, h.requests,
			"a growing queue must keep accumulating past the idle interval")
		timer = testutil.RequireReceive(t, h.timers, time.Second,
			"an early wake on a growing queue must re-arm the flush")
		due += timer.delay
		require.LessOrEqual(t, due, queuedBlockfetchMaxWait,
			"re-arming must never push past the first deadline")
	}
	request := testutil.RequireReceive(t, h.requests, time.Second,
		"a steady stream must still fetch at the first deadline")
	require.Equal(t, h.first, request.start)
	require.Equal(t, h.last, request.end)
	require.Empty(t, h.timers)
}
