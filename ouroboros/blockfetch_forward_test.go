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

package ouroboros

import (
	"sync"
	"testing"
	"time"

	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/internal/test/testutil"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros_conn "github.com/blinklabs-io/gouroboros/connection"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// blockfetchForwardTestBound is how long a blockfetch callback is given to
// return. The callbacks never wait on the ledger, so this is generous for a
// CI host.
const blockfetchForwardTestBound = 2 * time.Second

// stopEventBusBounded stops bus and fails the test if Stop does not return
// promptly. A forwarder goroutine deliberately left blocked in
// EventBus.Publish (this test's whole point) must be released by Stop, the
// same guarantee TestBlockfetchDrainDefersChainUpdatePastLedgerMutex relies
// on; an unbounded t.Cleanup(bus.Stop) would otherwise hang the test binary
// with no diagnostic if that guarantee ever regressed.
func stopEventBusBounded(t *testing.T, bus *event.EventBus) {
	t.Helper()
	stopped := make(chan struct{})
	go func() {
		bus.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(blockfetchForwardTestBound):
		t.Error(
			"EventBus.Stop did not return: it failed to release the " +
				"forwarder goroutine parked on the stalled subscriber",
		)
	}
}

// TestBlockfetchClientBlockNeverBlocksOnStalledLedgerConsumer pins that
// blockfetchClientBlock, which runs on the gouroboros blockfetch receive
// goroutine the muxer read loop depends on, returns while the lossless
// ledger.blockfetch subscriber is stalled with a full buffer. Publishing
// inline would block the first call indefinitely.
func TestBlockfetchClientBlockNeverBlocksOnStalledLedgerConsumer(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { stopEventBusBounded(t, bus) })

	// Lossless subscriber, buffer 1, deliberately never drained -- mirrors
	// ls.subscribeBlockfetchEvents' real buffer/backpressure policy for
	// ledger.BlockfetchEventType.
	subId, ch := bus.SubscribeWithBufferPolicy(
		ledger.BlockfetchEventType,
		1,
		event.SubscriberBackpressureBlock,
	)
	require.NotZero(t, subId)
	require.NotNil(t, ch)

	// Fill the single buffer slot so any further synchronous Publish blocks.
	// Confirm the stall is real, exactly as
	// TestBlockfetchDrainDefersChainUpdatePastLedgerMutex does for
	// chain.update: a broken test setup here would pass for the wrong
	// reason.
	bus.Publish(
		ledger.BlockfetchEventType,
		event.NewEvent(ledger.BlockfetchEventType, ledger.BlockfetchEvent{}),
	)
	directBlocked := make(chan struct{})
	go func() {
		bus.Publish(
			ledger.BlockfetchEventType,
			event.NewEvent(
				ledger.BlockfetchEventType,
				ledger.BlockfetchEvent{},
			),
		)
		close(directBlocked)
	}()
	select {
	case <-directBlocked:
		t.Fatal(
			"inline Publish did not block on the stalled subscriber; " +
				"the test cannot exercise the deadlock condition",
		)
	case <-time.After(300 * time.Millisecond):
	}

	reg := prometheus.NewRegistry()
	o := newOuroboros(OuroborosConfig{EventBus: bus, PromRegistry: reg})

	blocks, err := testfixtures.GenerateConwayChain(2)
	require.NoError(t, err)
	require.Len(t, blocks, 2)

	connId := testConnId()
	ctx := blockfetch.CallbackContext{ConnectionId: connId, RequestId: 1}

	callReturned := func(blockIdx int) bool {
		done := make(chan error, 1)
		go func() {
			block := blocks[blockIdx]
			done <- o.blockfetchClientBlock(ctx, uint(block.Type()), block)
		}()
		select {
		case err := <-done:
			require.NoError(t, err)
			return true
		case <-time.After(blockfetchForwardTestBound):
			return false
		}
	}

	require.True(
		t,
		callReturned(0),
		"blockfetchClientBlock blocked on a stalled ledger.blockfetch "+
			"consumer: it must enqueue and return, never publish inline",
	)
	require.True(
		t,
		callReturned(1),
		"a second blockfetchClientBlock call also blocked: the forward "+
			"queue must keep accepting while its single forwarder "+
			"goroutine is parked delivering the first event",
	)

	// Both blocks are accounted for as in-flight: the first is stuck mid
	// hand-off inside the forwarder's own (now-blocked) Publish call, the
	// second is still queued behind it. Neither has reached the ledger.
	wantBytes := float64(len(blocks[0].Cbor()) + len(blocks[1].Cbor()))
	require.Eventually(t, func() bool {
		return promtestutil.ToFloat64(o.blockfetchMetrics.inFlightBlocks) == 2
	}, blockfetchForwardTestBound, 5*time.Millisecond,
		"expected 2 blockfetch events in flight",
	)
	require.Equal(
		t,
		wantBytes,
		promtestutil.ToFloat64(o.blockfetchMetrics.inFlightBytes),
		"in-flight bytes must account for both undelivered blocks",
	)

	// Drain the subscriber channel: the forwarder's blocked Publish call can
	// now complete, and it goes on to deliver the second queued event too.
	// In-flight bytes/blocks must return to zero once the ledger consumer
	// catches up, so the backlog drains rather than being deferred. A continuous
	// blocking drain (not a fixed receive count) is required: the channel
	// also still holds the priming event from the stall check above and
	// contends with the still-parked directBlocked goroutine's own publish,
	// so the exact number of sends ahead of the two blockfetch events is not
	// fixed.
	drainDone := make(chan struct{})
	defer close(drainDone)
	go func() {
		for {
			select {
			case <-ch:
			case <-drainDone:
				return
			}
		}
	}()

	require.Eventually(
		t,
		func() bool {
			return promtestutil.ToFloat64(
				o.blockfetchMetrics.inFlightBlocks,
			) == 0
		},
		blockfetchForwardTestBound,
		5*time.Millisecond,
		"in-flight blocks must drain to zero once the ledger consumer catches up",
	)
	require.Equal(
		t,
		float64(0),
		promtestutil.ToFloat64(o.blockfetchMetrics.inFlightBytes),
		"in-flight bytes must drain to zero alongside the blocks",
	)
}

// TestBlockfetchForwardKeepsPerConnectionOrderUnderStall pins the
// single-forwarder invariant that gives each connection FIFO delivery. The
// first publish on each connection is held inside the forwarder while the
// rest of the range, including BatchDone, is enqueued; no enqueue may start a
// second forwarder for a connection that already has one, and once released
// each connection must deliver its blocks in order followed by BatchDone.
func TestBlockfetchForwardKeepsPerConnectionOrderUnderStall(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { stopEventBusBounded(t, bus) })
	blocks, err := testfixtures.GenerateConwayChain(3)
	require.NoError(t, err)
	conns := []ouroboros_conn.ConnectionId{
		testConnIdWithPort(4001),
		testConnIdWithPort(4002),
	}
	total := len(conns) * (len(blocks) + 1)
	_, ch := bus.SubscribeWithBufferPolicy(
		ledger.BlockfetchEventType,
		total,
		event.SubscriberBackpressureBlock,
	)
	o := newOuroboros(OuroborosConfig{
		EventBus:     bus,
		PromRegistry: prometheus.NewRegistry(),
	})

	var mu sync.Mutex
	spawns := make(map[string]int)
	held := make(map[string]bool)
	entered := make(map[string]chan struct{})
	for _, connId := range conns {
		entered[connId.String()] = make(chan struct{})
	}
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)
	o.blockfetchForwardSpawn = func(
		connId ouroboros_conn.ConnectionId,
		run func(ouroboros_conn.ConnectionId),
	) {
		mu.Lock()
		spawns[connId.String()]++
		mu.Unlock()
		go run(connId)
	}
	o.blockfetchForwardBeforePublish = func(
		connId ouroboros_conn.ConnectionId,
		_ event.Event,
	) {
		key := connId.String()
		mu.Lock()
		first := !held[key]
		held[key] = true
		mu.Unlock()
		if first {
			close(entered[key])
			<-release
		}
	}

	callbacks := func(connId ouroboros_conn.ConnectionId, from int, done bool) {
		ctx := blockfetch.CallbackContext{ConnectionId: connId, RequestId: 1}
		for _, block := range blocks[from:] {
			require.NoError(
				t,
				o.blockfetchClientBlock(ctx, uint(block.Type()), block),
			)
			if from == 0 {
				return
			}
		}
		if done {
			require.NoError(t, o.blockfetchClientRangeDone(ctx, nil))
		}
	}
	for _, connId := range conns {
		callbacks(connId, 0, false)
		testutil.RequireReceive(
			t,
			entered[connId.String()],
			blockfetchForwardTestBound,
			"forwarder did not reach its first publish",
		)
	}
	for _, connId := range conns {
		callbacks(connId, 1, true)
	}
	mu.Lock()
	for _, connId := range conns {
		require.Equal(
			t,
			1,
			spawns[connId.String()],
			"connection %s: an enqueue started a second forwarder while "+
				"the first was still publishing",
			connId.String(),
		)
	}
	mu.Unlock()
	releaseAll()

	want := make([]string, 0, len(blocks)+1)
	for _, block := range blocks {
		want = append(want, block.Hash().String())
	}
	want = append(want, "BatchDone")
	got := make(map[string][]string)
	for range total {
		evt := testutil.RequireReceive(
			t,
			ch,
			blockfetchForwardTestBound,
			"forwarded blockfetch event",
		)
		e, ok := evt.Data.(ledger.BlockfetchEvent)
		require.True(t, ok)
		key := e.ConnectionId.String()
		if e.BatchDone {
			got[key] = append(got[key], "BatchDone")
		} else {
			got[key] = append(got[key], e.Block.Hash().String())
		}
	}
	for _, connId := range conns {
		require.Equal(
			t,
			want,
			got[connId.String()],
			"connection %s: events must arrive in enqueue order",
			connId.String(),
		)
	}
}

// blockfetchForwardOverflowFixture holds a connection's first publish inside
// its forwarder, so every later event stays queued, and records the
// connections closed for reaching a forward queue bound.
type blockfetchForwardOverflowFixture struct {
	o       *Ouroboros
	ch      <-chan event.Event
	connId  ouroboros_conn.ConnectionId
	entered chan struct{}
	release func()
	closed  chan ouroboros_conn.ConnectionId
}

func newBlockfetchForwardOverflowFixture(
	t *testing.T,
) *blockfetchForwardOverflowFixture {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { stopEventBusBounded(t, bus) })
	_, ch := bus.SubscribeWithBufferPolicy(
		ledger.BlockfetchEventType,
		64,
		event.SubscriberBackpressureBlock,
	)
	f := &blockfetchForwardOverflowFixture{
		o: newOuroboros(OuroborosConfig{
			EventBus:     bus,
			PromRegistry: prometheus.NewRegistry(),
		}),
		ch:      ch,
		connId:  testConnId(),
		entered: make(chan struct{}),
		closed:  make(chan ouroboros_conn.ConnectionId, 4),
	}
	release := make(chan struct{})
	var releaseOnce sync.Once
	f.release = func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(f.release)
	var holdOnce sync.Once
	f.o.blockfetchForwardBeforePublish = func(
		ouroboros_conn.ConnectionId,
		event.Event,
	) {
		holdOnce.Do(func() {
			close(f.entered)
			<-release
		})
	}
	f.o.blockfetchForwardClose = func(connId ouroboros_conn.ConnectionId) {
		f.closed <- connId
	}
	return f
}

// deliver runs fn as a blockfetch receive callback and fails the test if it
// does not return within blockfetchForwardTestBound.
func (f *blockfetchForwardOverflowFixture) deliver(
	t *testing.T,
	fn func() error,
) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(blockfetchForwardTestBound):
		t.Fatal("blockfetch receive callback blocked on the forward queue")
	}
}

func (f *blockfetchForwardOverflowFixture) block(
	t *testing.T,
	block gledger.Block,
) {
	t.Helper()
	ctx := blockfetch.CallbackContext{ConnectionId: f.connId, RequestId: 1}
	f.deliver(t, func() error {
		return f.o.blockfetchClientBlock(ctx, uint(block.Type()), block)
	})
}

func (f *blockfetchForwardOverflowFixture) overflows() float64 {
	return promtestutil.ToFloat64(f.o.blockfetchMetrics.forwardOverflows)
}

func (f *blockfetchForwardOverflowFixture) inFlight() (float64, float64) {
	return promtestutil.ToFloat64(f.o.blockfetchMetrics.inFlightBytes),
		promtestutil.ToFloat64(f.o.blockfetchMetrics.inFlightBlocks)
}

// TestBlockfetchForwardEventLimitClosesConnection pins the overflow contract
// of the per-connection forward queue: while the forwarder is held inside a
// publish, enqueues up to the event bound are accepted, the enqueue that
// would exceed it returns without blocking, discards the queued events,
// closes the connection, and every later event for the connection is dropped
// until the close is handled, after which the connection id is usable again.
func TestBlockfetchForwardEventLimitClosesConnection(t *testing.T) {
	t.Parallel()

	f := newBlockfetchForwardOverflowFixture(t)
	f.o.blockfetchForwardMaxEvents = 3
	blocks, err := testfixtures.GenerateConwayChain(5)
	require.NoError(t, err)

	f.block(t, blocks[0])
	testutil.RequireReceive(
		t,
		f.entered,
		blockfetchForwardTestBound,
		"forwarder did not reach its first publish",
	)
	for _, block := range blocks[1:4] {
		f.block(t, block)
	}
	require.Equal(t, float64(0), f.overflows(),
		"events within the bound must be queued, not rejected")
	inBytes, inBlocks := f.inFlight()
	require.Equal(t, float64(4), inBlocks)

	f.block(t, blocks[4])
	require.Equal(t, float64(1), f.overflows(),
		"the enqueue exceeding the event bound must overflow the queue")
	require.Equal(t, f.connId, testutil.RequireReceive(
		t,
		f.closed,
		blockfetchForwardTestBound,
		"overflowed connection was not closed",
	))
	inBytes, inBlocks = f.inFlight()
	require.Equal(t, float64(1), inBlocks,
		"queued events must be discarded, leaving only the one in publish")
	require.Equal(t, float64(len(blocks[0].Cbor())), inBytes)

	f.deliver(t, func() error {
		return f.o.blockfetchClientRangeDone(
			blockfetch.CallbackContext{ConnectionId: f.connId, RequestId: 1},
			nil,
		)
	})
	_, inBlocks = f.inFlight()
	require.Equal(t, float64(1), inBlocks,
		"an event for an overflowed connection must be dropped")
	require.Equal(t, float64(1), f.overflows(),
		"a dropped event must not count as a second overflow")

	f.release()
	evt := testutil.RequireReceive(
		t,
		f.ch,
		blockfetchForwardTestBound,
		"held event was not delivered",
	)
	e, ok := evt.Data.(ledger.BlockfetchEvent)
	require.True(t, ok)
	require.Equal(t, blocks[0].Hash().String(), e.Block.Hash().String())
	require.Eventually(t, func() bool {
		_, n := f.inFlight()
		return n == 0
	}, blockfetchForwardTestBound, 5*time.Millisecond)
	select {
	case evt := <-f.ch:
		t.Fatalf("discarded event reached the ledger: %+v", evt.Data)
	default:
	}

	f.o.HandleConnClosedEvent(event.NewEvent(
		connmanager.ConnectionClosedEventType,
		connmanager.ConnectionClosedEvent{ConnectionId: f.connId},
	))
	f.block(t, blocks[1])
	evt = testutil.RequireReceive(
		t,
		f.ch,
		blockfetchForwardTestBound,
		"event after the close was not delivered",
	)
	e, ok = evt.Data.(ledger.BlockfetchEvent)
	require.True(t, ok)
	require.Equal(t, blocks[1].Hash().String(), e.Block.Hash().String())
}

// TestBlockfetchForwardByteLimitClosesConnection pins the byte bound: an
// enqueue that would take the queued bytes past it overflows the queue even
// when the event bound has room.
func TestBlockfetchForwardByteLimitClosesConnection(t *testing.T) {
	t.Parallel()

	f := newBlockfetchForwardOverflowFixture(t)
	blocks, err := testfixtures.GenerateConwayChain(3)
	require.NoError(t, err)
	f.o.blockfetchForwardMaxBytes = len(blocks[0].Cbor()) +
		len(blocks[1].Cbor())

	f.block(t, blocks[0])
	testutil.RequireReceive(
		t,
		f.entered,
		blockfetchForwardTestBound,
		"forwarder did not reach its first publish",
	)
	f.block(t, blocks[1])
	require.Equal(t, float64(0), f.overflows(),
		"bytes up to the bound must be queued, not rejected")
	f.block(t, blocks[2])
	require.Equal(t, float64(1), f.overflows(),
		"the enqueue exceeding the byte bound must overflow the queue")
	require.Equal(t, f.connId, testutil.RequireReceive(
		t,
		f.closed,
		blockfetchForwardTestBound,
		"overflowed connection was not closed",
	))
	inBytes, inBlocks := f.inFlight()
	require.Equal(t, float64(1), inBlocks)
	require.Equal(t, float64(len(blocks[0].Cbor())), inBytes)
}
