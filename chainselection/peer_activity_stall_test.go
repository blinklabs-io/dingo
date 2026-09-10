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

package chainselection

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// peerActivityStallBuffer stands in for the production
// event.DefaultSubscriberBuffer (1024) used by the node's
// chainselection.peer_activity subscription. The mechanism under test is
// buffer-size independent: a handler that stops returning stops draining, and
// the buffer only decides how long that takes to become visible. In the
// blinklabs-io/dingo#3550 Preview run the 1024-slot buffer took 12h31m of
// keepalive traffic to fill, which is exactly why the buffer is shrunk here
// rather than the events slowed down.
const peerActivityStallBuffer = 4

// blockedConsumer is a downstream EventBus consumer that parks inside its
// handler until the test releases it, modelling the ledger's
// chainselection.chain_switch subscriber blocked on ls.chainsyncMutex while
// the chainsync pipeline is not making progress.
type blockedConsumer struct {
	release   chan struct{}
	closeOnce sync.Once
	delivered atomic.Int64
	entered   atomic.Int64
}

func newBlockedConsumer(t *testing.T) *blockedConsumer {
	t.Helper()
	return &blockedConsumer{release: make(chan struct{})}
}

func (b *blockedConsumer) handle(event.Event) {
	b.entered.Add(1)
	<-b.release
	b.delivered.Add(1)
}

func (b *blockedConsumer) unblock() {
	b.closeOnce.Do(func() { close(b.release) })
}

// newStalledSelectionFixture builds a ChainSelector with one tracked best peer
// and a downstream chainselection.chain_selection consumer that never returns.
// chain_selection is published by publishSelectionEvents on every accepted
// TouchPeerActivity that has a best peer, so it is the deterministic member of
// the same synchronous fan-out that also carries chain_switch.
func newStalledSelectionFixture(
	t *testing.T,
) (*event.EventBus, *ChainSelector, *blockedConsumer, ouroboros.ConnectionId) {
	t.Helper()

	bus := event.NewEventBus(nil, nil)
	consumer := newBlockedConsumer(t)
	// LIFO cleanup: the consumer is released before the bus is stopped, or
	// EventBus.shutdown would wait forever on the parked dispatch goroutine.
	t.Cleanup(bus.Stop)
	t.Cleanup(consumer.unblock)

	cs := NewChainSelector(ChainSelectorConfig{
		EventBus:          bus,
		StaleTipThreshold: time.Hour,
	})

	connId := newTestConnectionId(1)
	cs.UpdatePeerTip(connId, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("best")},
		BlockNumber: 50,
	}, nil)
	cs.EvaluateAndSwitch()
	require.NotNil(t, cs.GetBestPeer(), "fixture needs a selected best peer")

	// Subscribed only after the setup publishes above have drained, so the
	// fixture itself cannot park on its own blocked consumer.
	//
	// SubscriberBackpressureBlock models a downstream that stays blocked:
	// under the default Detach the wait ends at event.channelDeliveryTimeout
	// and the subscriber is dropped, which is what the 92b2e3e6 build in the
	// issue did not do at all and is a different failure (permanent silent
	// loss) rather than the stall under test here.
	bus.SubscribeFuncWithBufferPolicy(
		ChainSelectionEventType,
		1,
		event.SubscriberBackpressureBlock,
		consumer.handle,
	)
	return bus, cs, consumer, connId
}

// A blocked downstream consumer must not stop the internal
// chainselection.peer_activity subscriber from draining. The Preview run in
// blinklabs-io/dingo#3550 shows the opposite: the handler stopped returning,
// its 1024-slot buffer filled over the next 12h31m, and from then on every
// keepalive response parked a protocol goroutine inside EventBus.Publish
// (ouroboros/keepalive.go) with 299 of them blocked by the end of the log.
func TestPeerActivityHandlerKeepsDrainingWhileDownstreamConsumerBlocks(
	t *testing.T,
) {
	bus, cs, consumer, connId := newStalledSelectionFixture(t)

	var handled atomic.Int64
	// Mirrors the node's subscribeChainSelectorEvents wiring for
	// chainselection.peer_activity, with the blocking policy so a stalled
	// handler is measured rather than the detach timer.
	bus.SubscribeFuncWithBufferPolicy(
		PeerActivityEventType,
		peerActivityStallBuffer,
		event.SubscriberBackpressureBlock,
		func(evt event.Event) {
			cs.HandlePeerActivityEvent(evt)
			handled.Add(1)
		},
	)

	// Enough touches to overrun the subscriber buffer several times over.
	const touches = peerActivityStallBuffer * 4
	published := make(chan struct{})
	go func() {
		defer close(published)
		for range touches {
			bus.Publish(
				PeerActivityEventType,
				event.NewEvent(
					PeerActivityEventType,
					PeerActivityEvent{ConnectionId: connId},
				),
			)
		}
	}()

	// The negative case: a blocked downstream must not park the keepalive-side
	// publishers. Every publish has to return.
	testutil.RequireReceive(t, published, 10*time.Second,
		"keepalive publishers parked on the peer_activity subscriber: a "+
			"blocked downstream consumer must not park protocol goroutines",
	)
	testutil.WaitForCondition(t, func() bool {
		return handled.Load() == touches
	}, 10*time.Second,
		"the internal peer_activity subscriber stopped draining while a "+
			"downstream consumer was blocked",
	)
	require.Zero(t, consumer.delivered.Load(),
		"fixture invariant: the downstream consumer is still blocked",
	)

	// Releasing the downstream consumer must still deliver the selection
	// events the activity path produced: they are deferred, not discarded.
	consumer.unblock()
	testutil.WaitForCondition(t, func() bool {
		return consumer.delivered.Load() > 0
	}, 10*time.Second,
		"selection events produced by the activity path were never delivered",
	)
}

// Deferring publication must not reorder chain switches: a subscriber that
// acts on chainselection.chain_switch (the ledger repoints its chainsync
// cursor at NewConnectionId) ends up on the wrong peer if an older switch is
// delivered after a newer one.
//
// What this pins is publisher order, which is the whole of what the lane
// promises. All eight switches are decided and published from this goroutine,
// so decision order and publish order coincide here. They do not in general:
// every producer decides under cs.mutex and publishes after releasing it, so
// two goroutines can decide in one order and enqueue in the other, and no lane
// prevents that (wolf31o2 review). See publishSelection.
func TestChainSwitchEventsPreservePublishOrder(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)

	_, evtCh := bus.SubscribeWithBuffer(ChainSwitchEventType, 64)

	cs := NewChainSelector(ChainSelectorConfig{
		EventBus:          bus,
		StaleTipThreshold: time.Hour,
	})

	connA := newTestConnectionId(1)
	connB := newTestConnectionId(2)

	const switches = 8
	want := make([]ouroboros.ConnectionId, 0, switches)
	blockNumber := uint64(50)
	for i := range switches {
		conn := connA
		if i%2 == 1 {
			conn = connB
		}
		blockNumber++
		cs.UpdatePeerTip(conn, ochainsync.Tip{
			Point: ocommon.Point{
				Slot: 100 + blockNumber,
				Hash: []byte(conn.String()),
			},
			BlockNumber: blockNumber,
		}, nil)
		cs.EvaluateAndSwitch()
		want = append(want, conn)
	}

	got := make([]ouroboros.ConnectionId, 0, switches)
	for range switches {
		evt := testutil.RequireReceive(t, evtCh, 10*time.Second,
			"chain switch event was never delivered",
		)
		data, ok := evt.Data.(ChainSwitchEvent)
		require.True(t, ok, "expected ChainSwitchEvent, got %T", evt.Data)
		got = append(got, data.NewConnectionId)
	}
	require.Equal(t, want, got,
		"chain switch events must be delivered in publish order",
	)
}
