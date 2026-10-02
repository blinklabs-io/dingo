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

package event

import (
	"sync"
	"testing"
	"time"
)

func (s *blockingSubscriber) Deliver(Event) error {
	s.startOnce.Do(func() {
		close(s.deliverStarted)
	})
	<-s.releaseDeliver
	s.doneOnce.Do(func() {
		close(s.deliverDone)
	})
	return nil
}

func (s *blockingSubscriber) Close() {
	s.closeCalled.Store(true)
}

// TestCloseDoesNotDeadlockWithFullChannel verifies that Close
// completes promptly even when the channel buffer is full and a
// concurrent Publish is in progress.
//
// The property is about a Publish parked on a *full* buffer racing Close.
// How large that buffer is does not change the interleaving, only how long
// each attempt spends filling it before the interesting part begins --
// subscribing at EventQueueSize meant every one of the 500 attempts
// published 100,000 events first, which made this single test the event
// package's floor at 49.3s of a 51.1s run, and left the race window a
// vanishing fraction of each attempt.
//
// So the attempt count that hunts the interleaving now fills a small
// buffer, and a few attempts still run at the production queue size so
// that path keeps its coverage.
func TestCloseDoesNotDeadlockWithFullChannel(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		buffer   int
		attempts int
	}{
		{name: "small buffer", buffer: 8, attempts: 500},
		{name: "production queue size", buffer: EventQueueSize, attempts: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for range tc.attempts {
				closeDeadlockAttempt(t, tc.buffer)
			}
		})
	}
}

// closeDeadlockAttempt runs one Close-versus-Publish attempt against a
// subscriber whose buffer it first fills, and fails if Close does not
// complete.
func closeDeadlockAttempt(t *testing.T, buffer int) {
	t.Helper()

	eb := NewEventBus(nil, nil)
	typ := EventType("close.deadlock.test")
	subId, ch := eb.SubscribeWithBuffer(typ, buffer)

	// Fill the buffer.
	for range buffer {
		eb.Publish(typ, NewEvent(typ, "fill"))
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// Concurrent publisher that keeps trying to publish.
	go func() {
		defer wg.Done()
		for range 50 {
			eb.Publish(typ, NewEvent(typ, "storm"))
		}
	}()

	// Concurrent unsubscribe (triggers Close).
	go func() {
		defer wg.Done()
		eb.Unsubscribe(typ, subId)
	}()

	// Drain channel so it eventually closes.
	go func() {
		for range ch { //nolint:revive
		}
	}()

	// wg.Wait must complete. If Close deadlocks this will
	// hang and the test will time out.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: Close/Publish blocked for 5s")
	}

	eb.Stop()
}
