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

package chain_test

import (
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestDeferredAddAndRollbackDoNotPublish is the chain-package half of the
// chainsync/blockfetch drain deadlock fix.
//
// The ledger calls into the chain while holding chainsyncBlockfetchMutex /
// chainsyncMutex. To keep a backpressured publish from stalling under those
// locks, the mutex-holding paths use AddBlockWithPointDeferred and
// RollbackDeferred, which return the chain.update event(s) for the ledger to
// publish AFTER the mutex is released (see ledger.pendingPublishes) instead of
// publishing inline.
//
// This test installs a lossless (SubscriberBackpressureBlock) chain.update
// subscriber whose single buffer slot is filled and never drained: any inline
// Publish would block on it forever. It then drives blocks and a rollback
// through the deferred methods and requires each to return promptly and to
// publish nothing. A regression that published inline from these methods would
// block on the stalled subscriber and fail the timeout, or would leak an event
// onto the subscriber channel and fail the "published nothing" check.
func TestDeferredAddAndRollbackDoNotPublish(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	t.Cleanup(eventBus.Stop)

	subId, ch := eventBus.SubscribeWithBufferPolicy(
		chain.ChainUpdateEventType,
		1,
		event.SubscriberBackpressureBlock,
	)
	require.NotZero(t, subId)
	require.NotNil(t, ch)

	// Fill the subscriber's only buffer slot; from here an inline Publish
	// blocks.
	eventBus.Publish(
		chain.ChainUpdateEventType,
		event.NewEvent(chain.ChainUpdateEventType, chain.ChainBlockEvent{}),
	)

	cm, err := chain.NewManager(nil, eventBus)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.NotNil(t, c)

	blocks, err := testfixtures.GenerateConwayChain(2)
	require.NoError(t, err)
	require.Len(t, blocks, 2)

	pointOf := func(i int) ocommon.Point {
		return ocommon.Point{
			Slot: blocks[i].SlotNumber(),
			Hash: blocks[i].Hash().Bytes(),
		}
	}

	// Add both blocks through the deferred API. Each call must return promptly
	// (it never touches the bus) and hand back a populated chain.update event.
	for i := range blocks {
		type result struct {
			evt event.Event
			err error
		}
		done := make(chan result, 1)
		go func() {
			evt, addErr := c.AddBlockWithPointDeferred(blocks[i], pointOf(i), nil)
			done <- result{evt: evt, err: addErr}
		}()
		select {
		case r := <-done:
			require.NoError(t, r.err)
			require.Equal(
				t,
				event.EventType(chain.ChainUpdateEventType),
				r.evt.Type,
				"deferred add must return the chain.update event to publish",
			)
		case <-time.After(5 * time.Second):
			t.Fatalf(
				"AddBlockWithPointDeferred(%d) blocked: it must return the "+
					"event, not publish it inline under a stalled subscriber",
				i,
			)
		}
	}
	require.Equal(t, blocks[1].SlotNumber(), c.Tip().Point.Slot)

	// Roll back the newest block through the deferred API. It must also return
	// promptly and hand back its chain.update event(s).
	rbDone := make(chan []event.Event, 1)
	rbErr := make(chan error, 1)
	go func() {
		evts, err := c.RollbackDeferred(pointOf(0))
		rbErr <- err
		rbDone <- evts
	}()
	select {
	case err := <-rbErr:
		require.NoError(t, err)
		evts := <-rbDone
		require.NotEmpty(t, evts)
	case <-time.After(5 * time.Second):
		t.Fatal(
			"RollbackDeferred blocked: it must return the events, not publish " +
				"them inline under a stalled subscriber",
		)
	}
	require.Equal(t, blocks[0].SlotNumber(), c.Tip().Point.Slot)

	// Nothing beyond the single pre-fill event may have reached the subscriber:
	// the deferred methods published nothing.
	select {
	case <-ch:
		// the pre-fill event
	default:
		t.Fatal("expected the pre-fill event in the subscriber buffer")
	}
	select {
	case extra := <-ch:
		t.Fatalf(
			"deferred add/rollback published %v to the chain.update "+
				"subscriber; they must defer publication to the caller",
			extra.Type,
		)
	default:
	}
}
