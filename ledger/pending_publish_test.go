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
	"github.com/stretchr/testify/require"
)

func TestPendingPublishesFlushesInOrder(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	var mu sync.Mutex
	var got []string
	done := make(chan struct{})
	bus.SubscribeFunc(LedgerErrorEventType, func(evt event.Event) {
		e, ok := evt.Data.(LedgerErrorEvent)
		if !ok {
			return
		}
		mu.Lock()
		got = append(got, e.Operation)
		if len(got) == 2 {
			close(done)
		}
		mu.Unlock()
	})

	var pending pendingPublishes
	for _, op := range []string{"first", "second"} {
		pending.add(bus, LedgerErrorEventType, event.NewEvent(
			LedgerErrorEventType,
			LedgerErrorEvent{Operation: op},
		))
	}

	mu.Lock()
	require.Empty(t, got, "nothing is published before flush")
	mu.Unlock()

	pending.flush()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("queued events were not delivered")
	}
	mu.Lock()
	require.Equal(t, []string{"first", "second"}, got,
		"events are published in the order they were queued")
	mu.Unlock()

	// flush is not re-entrant: a second call must not republish.
	pending.flush()
	mu.Lock()
	require.Len(t, got, 2, "flush clears the queue")
	mu.Unlock()
}

func TestPendingPublishesIgnoresNilBus(t *testing.T) {
	var pending pendingPublishes
	pending.add(nil, LedgerErrorEventType, event.NewEvent(
		LedgerErrorEventType,
		LedgerErrorEvent{Operation: "dropped"},
	))
	require.Empty(t, pending.events,
		"a nil EventBus is ignored, matching the call sites' nil checks")
	pending.flush() // must not panic
}

// The deadlock this type exists to prevent: a subscriber that needs the
// publisher's lock, reached while that lock is held and the subscriber's
// buffer is full. Publishing directly would park both sides forever;
// queueing and flushing after the unlock lets the subscriber drain.
func TestPendingPublishesBreaksLockCycle(t *testing.T) {
	bus := event.NewEventBus(nil, nil)
	defer bus.Close()

	var chainsyncMutex sync.Mutex
	handled := make(chan struct{}, 8)

	// The subscriber needs the same lock the publisher holds, exactly as
	// ChainsyncResyncEventType's subscriber does via
	// RecoverAfterLocalRollback.
	bus.SubscribeFuncWithBuffer(
		event.ChainsyncResyncEventType, 1,
		func(event.Event) {
			chainsyncMutex.Lock()
			chainsyncMutex.Unlock() //nolint:staticcheck // lock/unlock is the point
			handled <- struct{}{}
		},
	)

	newEvt := func(op string) event.Event {
		return event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{Reason: op},
		)
	}

	finished := make(chan struct{})
	go func() {
		defer close(finished)
		var pending pendingPublishes
		defer pending.flush() // runs after the unlock below
		chainsyncMutex.Lock()
		defer chainsyncMutex.Unlock()
		// Enough to overrun the subscriber's buffer while it is parked on
		// the lock we hold. Publishing these directly is the deadlock.
		for i := range 4 {
			pending.add(
				bus,
				event.ChainsyncResyncEventType,
				newEvt(string(rune('a'+i))),
			)
		}
	}()

	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("publisher deadlocked while holding the lock")
	}

	for range 4 {
		select {
		case <-handled:
		case <-time.After(10 * time.Second):
			t.Fatal("subscriber did not drain after the lock was released")
		}
	}
}
