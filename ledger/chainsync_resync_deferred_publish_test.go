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
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
)

// These two tests close the gap the converted rollback tests left open: they
// all pass a nil *pendingPublishes, which takes pendingPublishes.add's
// immediate-publish branch, so none of them can tell a deferred publish from
// an inline one. Reverting requestChainsyncResync's pending.add(...) back to a
// direct ls.config.EventBus.Publish(...) leaves the whole ledger suite green
// because nothing exercises the deferred path with a real queue and a real
// subscriber that needs the held mutex.
//
// Each test below hands requestChainsyncResync a NON-nil queue while holding a
// guarded mutex, with a ChainsyncResyncEventType subscriber that reaches for
// that same mutex from its handler — exactly the cycle pendingPublishes.go
// documents (the real subscriber is RecoverAfterLocalRollback, which takes
// chainsyncMutex and nests chainsyncBlockfetchMutex under it). With the fix the
// event is queued and only published after the unlock, so it completes. Revert
// the publish to inline and it parks forever: the subscriber buffer is full and
// its only reader is the handler waiting for the mutex the publisher still
// holds. The 5s guard turns that hang into a failure instead of wedging the
// whole test binary.

// runResyncDeferredPublishScenario drives requestChainsyncResync under the
// mutex selected by lock/unlock, with a resync subscriber whose handler takes
// that same mutex. It fails if the publish happens inline (deadlock) rather
// than being deferred until after the unlock.
func runResyncDeferredPublishScenario(
	t *testing.T,
	mu *sync.Mutex,
	mutexName string,
) {
	t.Helper()

	bus := event.NewEventBus(nil, nil)
	defer bus.Stop()

	ls := &LedgerState{
		config: LedgerStateConfig{EventBus: bus},
	}

	// entered is signalled from inside the handler, before it blocks on the
	// mutex. Receiving from it proves the dispatch goroutine has pulled an
	// event off the channel and is now committed to a handler call — so it
	// will not read another event until that handler returns, which it cannot
	// until the publisher releases the mutex.
	entered := make(chan struct{}, 4)
	// SubscriberBackpressureBlock models a lossless subscriber (the production
	// resync subscriber is one): a full buffer parks the publisher forever
	// rather than detaching after a timeout, which is what makes an inline
	// publish a true deadlock instead of an eventually-dropped event. Buffer 1
	// is the smallest that lets one priming event occupy the reader while a
	// second fills the channel, so the next publish has nowhere to go.
	bus.SubscribeFuncWithBufferPolicy(
		event.ChainsyncResyncEventType,
		1,
		event.SubscriberBackpressureBlock,
		func(event.Event) {
			entered <- struct{}{}
			mu.Lock()
			mu.Unlock()
		},
	)

	connId := testChainsyncConnId(6000, 7000)
	primer := func() event.Event {
		return event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{ConnectionId: connId},
		)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		mu.Lock()
		// First primer: consumed by the dispatch goroutine, whose handler
		// then parks on the mutex we hold. Waiting on entered guarantees it
		// has left the channel before the buffer is filled below.
		bus.Publish(event.ChainsyncResyncEventType, primer())
		<-entered
		// Second primer: fills the now-empty single buffer slot. The reader
		// is still parked in the first handler, so this event just sits there
		// and the subscriber has no free capacity left.
		bus.Publish(event.ChainsyncResyncEventType, primer())
		// With the fix this queues onto pending and returns immediately.
		// Reverted to an inline ls.config.EventBus.Publish it blocks here: the
		// buffer is full and its only reader is the parked handler.
		var pending pendingPublishes
		ls.requestChainsyncResync(connId, "test resync", &pending)
		mu.Unlock()
		// Only reached once the publish did NOT happen under the lock.
		pending.flush()
	}()

	select {
	case <-done:
		// Deferred: the queued event was published after the unlock, the
		// handler took the mutex, and everything drained.
	case <-time.After(5 * time.Second):
		t.Fatalf(
			"requestChainsyncResync published ChainsyncResyncEventType inline"+
				" while holding %s: the publish parked on a full subscriber"+
				" buffer whose handler was waiting for that same mutex"+
				" (deadlock). Queue it with pendingPublishes and flush after"+
				" the unlock — see pending_publish.go.",
			mutexName,
		)
	}
}

// TestRequestChainsyncResyncDefersPublishUnderChainsyncMutex fails (hangs to
// the 5s guard) if requestChainsyncResync publishes inline while chainsyncMutex
// is held, and passes when the publish is deferred through pendingPublishes.
func TestRequestChainsyncResyncDefersPublishUnderChainsyncMutex(t *testing.T) {
	ls := &LedgerState{}
	runResyncDeferredPublishScenario(t, &ls.chainsyncMutex, "chainsyncMutex")
}

// TestRequestChainsyncResyncDefersPublishUnderChainsyncBlockfetchMutex is the
// same guard for the blockfetch lock: the resync subscriber nests
// chainsyncBlockfetchMutex under chainsyncMutex, so holding the blockfetch lock
// alone across an inline publish deadlocks the same way.
func TestRequestChainsyncResyncDefersPublishUnderChainsyncBlockfetchMutex(
	t *testing.T,
) {
	ls := &LedgerState{}
	runResyncDeferredPublishScenario(
		t, &ls.chainsyncBlockfetchMutex, "chainsyncBlockfetchMutex",
	)
}
