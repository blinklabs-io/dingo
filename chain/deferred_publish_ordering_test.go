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
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// publishMode selects how a handler publishes the chain.update it deferred while
// holding its mutex, so one scenario can model both the pre-fix and post-fix
// ledger callers exactly.
type publishMode int

const (
	// perHandlerFlush is the pre-fix caller: each handler publishes ITS OWN
	// returned chain.update event(s) from its OWN pendingPublishes after
	// releasing its mutex. The two handlers' queues are independent, so a
	// handler that mutated the chain later can still flush first.
	perHandlerFlush publishMode = iota
	// sharedSequencer is the fix: each handler drains the chain-level
	// sequencer (chain.PublishPendingChainUpdates). Whichever handler flushes
	// first publishes the WHOLE queue in enqueue (== mutation) order, so the
	// two handlers can no longer invert.
	sharedSequencer
)

// observedUpdate labels a published chain.update by kind so the test can assert
// the block-add update and the rollback update come out in mutation order.
type observedUpdate string

const (
	updateAdd      observedUpdate = "add"
	updateRollback observedUpdate = "rollback"
)

// runDeferredOrderingScenario drives the exact concurrency wolf31o2 flagged: a
// blockfetch-style deferred add and a chainsync-style deferred rollback, each
// mutating the primary chain under its OWN mutex (the real handlers hold two
// different mutexes -- chainsyncBlockfetchMutex and chainsyncMutex -- so they do
// not mutually exclude), then publishing their deferred chain.update AFTER
// releasing that mutex.
//
// The interleaving is pinned deterministically with channels, no sleeps:
//
//  1. the add mutates the chain first (tip -> block C), enqueuing update "add";
//  2. the rollback then mutates the chain (tip C -> block B, discarding C),
//     enqueuing update "rollback"; so the true chain-mutation order is
//     [add, rollback];
//  3. the rollback PUBLISHES first, then the add publishes.
//
// Step 3 is the inversion trigger: the handler that mutated second publishes
// first. With perHandlerFlush each handler emits only its own event, so the
// subscriber observes [rollback, add] -- the reverse of the mutation order.
// With sharedSequencer the rollback's flush drains the shared queue in
// enqueue order and emits [add, rollback]; the add's later flush finds the
// queue already empty.
//
// It returns the chain.update kinds in the exact order the subscriber received
// them.
func runDeferredOrderingScenario(
	t *testing.T,
	mode publishMode,
) []observedUpdate {
	t.Helper()

	eventBus := event.NewEventBus(nil, nil)
	t.Cleanup(eventBus.Stop)

	cm, err := chain.NewManager(nil, eventBus)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.NotNil(t, c)

	blocks, err := testfixtures.GenerateConwayChain(3)
	require.NoError(t, err)
	require.Len(t, blocks, 3)

	pointOf := func(i int) ocommon.Point {
		return ocommon.Point{
			Slot: blocks[i].SlotNumber(),
			Hash: blocks[i].Hash().Bytes(),
		}
	}

	// Seed the chain up to block B (blocks[1]) BEFORE subscribing, so the
	// scenario's subscriber records only the add/rollback under test and not
	// the seed updates.
	require.NoError(t, c.AddBlock(blocks[0], nil))
	require.NoError(t, c.AddBlock(blocks[1], nil))
	require.Equal(t, blocks[1].SlotNumber(), c.Tip().Point.Slot)

	// Lossless subscriber with generous buffer: it never drops and preserves
	// per-subscriber delivery order, so the slice it records is the true
	// publish order.
	subId, ch := eventBus.SubscribeWithBufferPolicy(
		chain.ChainUpdateEventType,
		16,
		event.SubscriberBackpressureBlock,
	)
	require.NotZero(t, subId)

	classify := func(evt event.Event) (observedUpdate, bool) {
		switch evt.Data.(type) {
		case chain.ChainBlockEvent:
			return updateAdd, true
		case chain.ChainRollbackEvent:
			return updateRollback, true
		default:
			return "", false
		}
	}

	// Two independent mutexes standing in for chainsyncBlockfetchMutex
	// (add) and chainsyncMutex (rollback): the point of the bug is that these
	// are DIFFERENT locks, so the two handlers' critical sections and their
	// deferred publishes are not serialized against each other.
	var addMutex, rollbackMutex sync.Mutex

	addMutated := make(chan struct{})        // add finished its chain mutation
	rollbackPublished := make(chan struct{}) // rollback finished publishing

	var wg sync.WaitGroup
	wg.Add(2)

	// Blockfetch-style deferred add: extend the chain with block C
	// (blocks[2]) on top of block B.
	go func() {
		defer wg.Done()
		addMutex.Lock()
		addEvt, addErr := c.AddBlockWithPointDeferred(
			blocks[2],
			pointOf(2),
			nil,
		)
		addMutex.Unlock()
		require.NoError(t, addErr)
		require.Equal(
			t,
			event.EventType(chain.ChainUpdateEventType),
			addEvt.Type,
		)
		// The chain was mutated first; signal the rollback to proceed.
		close(addMutated)
		// Publish only AFTER the rollback has published, forcing the
		// second-mutating handler to publish first.
		<-rollbackPublished
		switch mode {
		case perHandlerFlush:
			eventBus.Publish(addEvt.Type, addEvt)
		case sharedSequencer:
			c.PublishPendingChainUpdates()
		}
	}()

	// Chainsync-style deferred rollback: rewind block C, back to block B.
	go func() {
		defer wg.Done()
		<-addMutated // ensure the add mutated the chain first
		rollbackMutex.Lock()
		rbEvts, rbErr := c.RollbackDeferred(pointOf(1))
		rollbackMutex.Unlock()
		require.NoError(t, rbErr)
		require.NotEmpty(t, rbEvts)
		switch mode {
		case perHandlerFlush:
			for _, evt := range rbEvts {
				eventBus.Publish(evt.Type, evt)
			}
		case sharedSequencer:
			c.PublishPendingChainUpdates()
		}
		close(rollbackPublished)
	}()

	wg.Wait()
	// The chain really ended at block B: the add was applied then rolled back.
	require.Equal(t, blocks[1].SlotNumber(), c.Tip().Point.Slot)

	// Collect exactly the two chain.update events the scenario publishes.
	var got []observedUpdate
	deadline := time.After(5 * time.Second)
	for len(got) < 2 {
		select {
		case evt := <-ch:
			if kind, ok := classify(evt); ok {
				got = append(got, kind)
			}
		case <-deadline:
			t.Fatalf(
				"timed out: observed %d of 2 chain.update events (%v)",
				len(got), got,
			)
		}
	}
	// No third chain.update may follow (exactly-once, no duplicate drain).
	select {
	case evt := <-ch:
		if kind, ok := classify(evt); ok {
			t.Fatalf("unexpected extra chain.update published: %s", kind)
		}
	case <-time.After(200 * time.Millisecond):
	}
	return got
}

// TestDeferredChainUpdatesPublishInChainMutationOrder is the regression guard
// for wolf31o2's blocking review (ledger/chainsync.go:531): deferred chain.update
// publication must follow chain-mutation order across the blockfetch-add and
// chainsync-rollback handlers, which hold different mutexes and previously
// flushed independent pendingPublishes queues.
//
// The shared chain-level sequencer makes the handler that flushes first publish
// the whole queue in enqueue (== mutation) order, so the observed order is
// [add, rollback] -- the order the chain was actually mutated -- regardless of
// which handler flushes first.
//
// To see this FAIL against the pre-fix per-instance-queue behaviour, change the
// mode below to perHandlerFlush: the second-mutating rollback flushes its own
// queue first and the subscriber observes [rollback, add], the inverse of the
// chain-mutation order. TestDeferredPerHandlerFlushInvertsAcrossHandlers pins
// that inversion so this guard is not vacuous.
func TestDeferredChainUpdatesPublishInChainMutationOrder(t *testing.T) {
	got := runDeferredOrderingScenario(t, sharedSequencer)
	require.Equal(
		t,
		[]observedUpdate{updateAdd, updateRollback},
		got,
		"deferred chain.update publication must match chain-mutation order "+
			"[add, rollback] across the two handlers",
	)
}

// TestDeferredPerHandlerFlushInvertsAcrossHandlers proves the concurrency the
// fix addresses is real, not hypothetical: with the pre-fix per-handler flush,
// the identical interleaving publishes the rollback BEFORE the add even though
// the chain was mutated add-then-rollback. This is the exact ordering that
// drove the chain.update subscriber's block apply/undo notifications out of
// order, and it is what the shared sequencer (asserted above) prevents.
func TestDeferredPerHandlerFlushInvertsAcrossHandlers(t *testing.T) {
	got := runDeferredOrderingScenario(t, perHandlerFlush)
	require.Equal(
		t,
		[]observedUpdate{updateRollback, updateAdd},
		got,
		"per-handler flush is expected to invert publish order relative to "+
			"chain-mutation order; if this no longer inverts the guard test "+
			"has stopped exercising the race",
	)
}
