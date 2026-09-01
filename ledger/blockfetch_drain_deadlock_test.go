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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// newChainUpdateEvent builds a throwaway chain.update event for saturating the
// bus in the deadlock regression below.
func newChainUpdateEvent() event.Event {
	return event.NewEvent(chain.ChainUpdateEventType, chain.ChainBlockEvent{})
}

// TestBlockfetchDrainDefersChainUpdatePastLedgerMutex is the regression guard
// for the chainsync/blockfetch drain deadlock (blinklabs-io/dingo preview
// freeze), rewritten to exercise the lane-saturation path the six-block test
// could not reach.
//
// The ledger drains fetched blocks via flushPendingBlockfetchBlocks while
// holding chainsyncBlockfetchMutex. If that drain publishes chain.update
// inline, a terminal chain.update subscriber that stops draining stalls the
// publish WITH the mutex held; handleEventChainsync then blocks acquiring the
// same mutex, the ledger.chainsync buffer fills, and the node deadlocks.
//
// This test puts the bus into the exact state that traps BOTH previously
// attempted inline publishers:
//
//   - a lossless chain.update subscriber whose one buffer slot is filled and
//     never drained, so a synchronous Publish (the original code) blocks; and
//   - the ordered chain.update lane filled to capacity, so a PublishOrdered
//     (the rejected under-lock fix) also blocks -- this is the saturation the
//     maintainer flagged, which a handful of blocks never reaches.
//
// With the bus in that state the drain must still return promptly, because the
// fix hands each block's chain.update back to the ledger's pendingPublishes to
// publish AFTER the mutex is released rather than publishing it inline. Both
// older approaches would block here and fail the timeout.
func TestBlockfetchDrainDefersChainUpdatePastLedgerMutex(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	// Stop releases the goroutines parked on the stalled subscriber / full
	// lane at the end of the test. Run it under a bounded wait: if Stop's
	// shutdown path ever regresses and fails to release those parked
	// publishers, an unbounded t.Cleanup(eventBus.Stop) would hang the whole
	// `go test` binary in Stop with no diagnostic. Fail the test instead so
	// the regression is visible.
	t.Cleanup(func() {
		stopped := make(chan struct{})
		go func() {
			eventBus.Stop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			t.Error(
				"eventBus.Stop did not return within 10s: it failed to " +
					"release the publishers parked on the stalled subscriber / " +
					"full ordered lane (shutdown backpressure-release regressed)",
			)
		}
	})

	// Terminal chain.update subscriber, buffer 1, deliberately never drained.
	// Lossless (SubscriberBackpressureBlock) means a full buffer blocks the
	// publisher forever rather than dropping -- the stalled-subscriber
	// condition behind the freeze.
	subId, ch := eventBus.SubscribeWithBufferPolicy(
		chain.ChainUpdateEventType,
		1,
		event.SubscriberBackpressureBlock,
	)
	require.NotZero(t, subId)
	require.NotNil(t, ch)

	// Fill the subscriber's single buffer slot so any further synchronous
	// Publish blocks. Confirm the stall is real: a second inline Publish must
	// not complete.
	eventBus.Publish(chain.ChainUpdateEventType, newChainUpdateEvent())
	directBlocked := make(chan struct{})
	go func() {
		eventBus.Publish(chain.ChainUpdateEventType, newChainUpdateEvent())
		close(directBlocked)
	}()
	select {
	case <-directBlocked:
		t.Fatal(
			"inline Publish did not block on the stalled subscriber; " +
				"the test cannot exercise the deadlock condition",
		)
	case <-time.After(500 * time.Millisecond):
	}

	// Saturate the ordered chain.update lane to capacity. The lane worker
	// parks on the stalled subscriber, so every enqueued event stays in the
	// lane; once it is full a PublishOrdered blocks too.
	go func() {
		for range event.OrderedQueueSize + 8 {
			eventBus.PublishOrdered(
				chain.ChainUpdateEventType,
				newChainUpdateEvent(),
			)
		}
	}()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			50*time.Millisecond,
		)
		defer cancel()
		// A bounded publish that cannot enqueue reports false: the lane is
		// full.
		return !eventBus.PublishOrderedContext(
			ctx,
			chain.ChainUpdateEventType,
			newChainUpdateEvent(),
		)
	}, 10*time.Second, 20*time.Millisecond,
		"ordered chain.update lane never reached capacity",
	)

	// Real primary chain wired to the saturated bus, plus a minimal ledger
	// state -- enough for the blockfetch drain path.
	cm, err := chain.NewManager(nil, eventBus)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.NotNil(t, c)

	ls := &LedgerState{
		chain: c,
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: eventBus,
		},
	}

	blocks, err := testfixtures.GenerateConwayChain(1)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	ls.pendingBlockfetchEvents = []BlockfetchEvent{
		{
			Block: blocks[0],
			Point: ocommon.Point{
				Slot: blocks[0].SlotNumber(),
				Hash: blocks[0].Hash().Bytes(),
			},
		},
	}

	// THE FIX. flushPendingBlockfetchBlocksDeferred runs on the mutex-holding
	// drain path. It must add the block and return promptly, queueing the
	// chain.update on pubs instead of publishing it into the saturated bus.
	// Under the original synchronous Publish, or the rejected under-lock
	// PublishOrdered, this call would block forever on the stalled subscriber
	// / full lane and the timeout below would fire.
	var pubs pendingPublishes
	drained := make(chan error, 1)
	go func() { drained <- ls.flushPendingBlockfetchBlocksDeferred(&pubs) }()
	select {
	case drainErr := <-drained:
		require.NoError(t, drainErr)
	case <-time.After(10 * time.Second):
		t.Fatal(
			"flushPendingBlockfetchBlocks blocked under a saturated " +
				"chain.update lane: the block's chain.update must be deferred " +
				"past chainsyncBlockfetchMutex, not published inline",
		)
	}

	// The chain.update was deferred onto the caller's queue, not published:
	// nothing reached the saturated bus under the lock.
	require.Len(t, pubs.events, 1)
	require.Equal(
		t,
		event.EventType(chain.ChainUpdateEventType),
		pubs.events[0].evt.Type,
	)
	// The block really was added to the chain (the drain did its job, it just
	// did not publish).
	require.Equal(t, blocks[0].SlotNumber(), c.Tip().Point.Slot)
}
