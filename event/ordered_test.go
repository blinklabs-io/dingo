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

// collectOrdered drains n events from ch and returns their int payloads in
// arrival order.
func collectOrdered(t *testing.T, ch <-chan Event, n int) []int {
	t.Helper()
	got := make([]int, 0, n)
	deadline := time.After(10 * time.Second)
	for len(got) < n {
		select {
		case evt := <-ch:
			v, ok := evt.Data.(int)
			if !ok {
				t.Fatalf("unexpected payload type %T", evt.Data)
			}
			got = append(got, v)
		case <-deadline:
			t.Fatalf("timed out after %d of %d events", len(got), n)
		}
	}
	return got
}

func requireAscending(t *testing.T, got []int) {
	t.Helper()
	for i := range got {
		if got[i] != i {
			t.Fatalf(
				"event %d delivered out of order: got %d, want %d (sequence %v)",
				i,
				got[i],
				i,
				got,
			)
		}
	}
}

// TestPublishOrderedPreservesPublisherOrder is the core ordering contract:
// events published to one event type from one goroutine reach a subscriber in
// publish order. PublishAsync cannot promise this because AsyncWorkerPoolSize
// workers drain the shared queue concurrently and race each other into
// Publish; a single 200-event run reordered 3-8 of them. See
// blinklabs-io/dingo#2287.
func TestPublishOrderedPreservesPublisherOrder(t *testing.T) {
	const n = 500
	eb := NewEventBus(nil, nil)
	t.Cleanup(eb.Close)

	subId, ch := eb.SubscribeWithBuffer("ordered.seq", n)
	if subId == 0 {
		t.Fatal("subscribe failed")
	}

	for i := range n {
		if !eb.PublishOrdered("ordered.seq", NewEvent("ordered.seq", i)) {
			t.Fatalf("PublishOrdered returned false at %d", i)
		}
	}
	requireAscending(t, collectOrdered(t, ch, n))
}

// TestPublishOrderedPreservesOrderUnderConcurrentAsyncTraffic keeps the shared
// async pool busy with an unrelated event type while the ordered lane runs, so
// a regression that routed ordered events back onto the shared pool is caught
// rather than passing on an idle bus.
func TestPublishOrderedPreservesOrderUnderConcurrentAsyncTraffic(
	t *testing.T,
) {
	const n = 500
	eb := NewEventBus(nil, nil)
	t.Cleanup(eb.Close)

	subId, ch := eb.SubscribeWithBuffer("ordered.seq", n)
	if subId == 0 {
		t.Fatal("subscribe failed")
	}
	// Unrelated async traffic occupying the shared worker pool.
	noiseDone := make(chan struct{})
	eb.SubscribeFunc("ordered.noise", func(Event) {})
	go func() {
		defer close(noiseDone)
		for i := range n * 4 {
			eb.PublishAsync("ordered.noise", NewEvent("ordered.noise", i))
		}
	}()

	for i := range n {
		if !eb.PublishOrdered("ordered.seq", NewEvent("ordered.seq", i)) {
			t.Fatalf("PublishOrdered returned false at %d", i)
		}
	}
	requireAscending(t, collectOrdered(t, ch, n))
	<-noiseDone
}

// TestPublishOrderedWaitsForCapacityRatherThanDropping holds the no-drop
// guarantee documented for the rest of the bus: a full ordered lane
// backpressures its publisher instead of discarding events.
func TestPublishOrderedWaitsForCapacityRatherThanDropping(t *testing.T) {
	// More events than the lane buffer can hold, with a subscriber that
	// only starts draining once the publisher is demonstrably parked.
	total := OrderedQueueSize + 64
	eb := NewEventBus(nil, nil)
	t.Cleanup(eb.Close)

	release := make(chan struct{})
	var mu sync.Mutex
	got := make([]int, 0, total)
	done := make(chan struct{})
	eb.SubscribeFuncWithBuffer("ordered.full", 1, func(evt Event) {
		<-release
		mu.Lock()
		got = append(got, evt.Data.(int))
		if len(got) == total {
			close(done)
		}
		mu.Unlock()
	})

	published := make(chan struct{})
	go func() {
		defer close(published)
		for i := range total {
			if !eb.PublishOrdered("ordered.full", NewEvent("ordered.full", i)) {
				t.Errorf("PublishOrdered returned false at %d", i)
				return
			}
		}
	}()

	// The publisher must still be parked: nothing is draining yet.
	select {
	case <-published:
		t.Fatal("publisher completed without backpressure from a full lane")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		mu.Lock()
		n := len(got)
		mu.Unlock()
		t.Fatalf(
			"only %d of %d events delivered: lane dropped events",
			n,
			total,
		)
	}
	<-published
	mu.Lock()
	defer mu.Unlock()
	requireAscending(t, got)
}

// TestPublishOrderedReturnsFalseWhenStopped mirrors PublishAsync's contract:
// a stopped or closed bus reports the failed publish rather than accepting an
// event nothing will deliver.
func TestPublishOrderedReturnsFalseWhenStopped(t *testing.T) {
	eb := NewEventBus(nil, nil)
	eb.Close()
	if eb.PublishOrdered("ordered.stopped", NewEvent("ordered.stopped", 0)) {
		t.Fatal("PublishOrdered returned true on a closed bus")
	}
}

// TestPublishOrderedPreservesOrderAfterStopRestart covers the reusable-bus
// path: Stop tears the lanes down, and the next publish must rebuild them
// bound to the new stop channel rather than to the torn-down one.
func TestPublishOrderedPreservesOrderAfterStopRestart(t *testing.T) {
	const n = 200
	eb := NewEventBus(nil, nil)
	t.Cleanup(eb.Close)

	subId, ch := eb.SubscribeWithBuffer("ordered.restart", n)
	if subId == 0 {
		t.Fatal("subscribe failed")
	}
	for i := range n {
		if !eb.PublishOrdered(
			"ordered.restart",
			NewEvent("ordered.restart", i),
		) {
			t.Fatalf("PublishOrdered returned false at %d", i)
		}
	}
	requireAscending(t, collectOrdered(t, ch, n))

	eb.Stop()

	subId, ch = eb.SubscribeWithBuffer("ordered.restart", n)
	if subId == 0 {
		t.Fatal("subscribe after restart failed")
	}
	for i := range n {
		if !eb.PublishOrdered(
			"ordered.restart",
			NewEvent("ordered.restart", i),
		) {
			t.Fatalf("PublishOrdered returned false at %d after restart", i)
		}
	}
	requireAscending(t, collectOrdered(t, ch, n))
}

// TestPublishOrderedIsolatesEventTypes checks that one lane's slow subscriber
// cannot stall an unrelated lane. Per-type lanes are what make this true; the
// shared async pool explicitly does not promise it.
func TestPublishOrderedIsolatesEventTypes(t *testing.T) {
	const n = 100
	eb := NewEventBus(nil, nil)
	t.Cleanup(eb.Close)

	block := make(chan struct{})
	eb.SubscribeFuncWithBuffer("ordered.slow", 1, func(Event) { <-block })
	t.Cleanup(func() { close(block) })

	fastId, fastCh := eb.SubscribeWithBuffer("ordered.fast", n)
	if fastId == 0 {
		t.Fatal("subscribe failed")
	}
	// Park the slow lane's worker and fill its buffer.
	for i := range 4 {
		eb.PublishOrdered("ordered.slow", NewEvent("ordered.slow", i))
	}
	for i := range n {
		if !eb.PublishOrdered("ordered.fast", NewEvent("ordered.fast", i)) {
			t.Fatalf("PublishOrdered returned false at %d", i)
		}
	}
	requireAscending(t, collectOrdered(t, fastCh, n))
}
