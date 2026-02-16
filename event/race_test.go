package event

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestPublishUnsubscribeRace attempts to reproduce the race between Publish
// and Unsubscribe/Stop where a send on a channel could hit a concurrently
// closing channel. The test runs many iterations to probabilistically
// surface races; the implementation should be deterministic and not panic.
func TestPublishUnsubscribeRace(t *testing.T) {
	const iters = 1000
	for range iters {
		eb := NewEventBus(nil, nil)
		typ := EventType("race.test")

		// Subscribe a channel-backed subscriber
		subId, ch := eb.Subscribe(typ)

		var wg sync.WaitGroup
		wg.Add(3)

		// Publisher goroutine
		go func() {
			defer wg.Done()
			// Publish many events to increase chance of overlapping with close
			for j := range 10 {
				eb.Publish(typ, NewEvent(typ, j))
			}
		}()

		// Concurrently unsubscribe/stop the bus
		go func() {
			defer wg.Done()
			// Unsubscribe the subscriber and Stop the bus concurrently
			eb.Unsubscribe(typ, subId)
			eb.Stop()
		}()

		// Drain channel until closed or timeout (no timeout here; Publish/Close should finish)
		go func() {
			defer wg.Done()
			for range ch {
			}
		}()

		wg.Wait()
	}
}

// TestSubscribeFuncStopRace tests the race condition where SubscribeFunc could
// call subscriberWg.Add(1) after Stop() has started Wait() with counter=0,
// which would panic or leave goroutines blocked forever. The fix ensures that
// SubscribeFunc holds stopMu.RLock through Add(1), preventing Stop from
// proceeding to Wait() until all pending subscriptions complete.
func TestSubscribeFuncStopRace(t *testing.T) {
	const iters = 1000
	for range iters {
		eb := NewEventBus(nil, nil)
		typ := EventType("race.subscribefunc.stop")

		var wg sync.WaitGroup
		var successfulSubscribes atomic.Int32

		// Spawn multiple SubscribeFunc goroutines concurrently
		for range 5 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				subId := eb.SubscribeFunc(typ, func(Event) {})
				if subId != 0 {
					successfulSubscribes.Add(1)
				}
			}()
		}

		// Concurrently call Stop
		wg.Add(1)
		go func() {
			defer wg.Done()
			eb.Stop()
		}()

		wg.Wait()
		// If we get here without panic, the race is handled correctly.
		// Some SubscribeFunc calls may have succeeded (subId != 0) and
		// their goroutines should have been properly shut down by Stop.
	}
}

// TestPublishDoesNotBlockOnFullChannel verifies that Publish returns
// promptly even when a subscriber's channel buffer is completely full.
// Before the non-blocking send fix, this scenario would deadlock:
// Deliver() held mu.RLock while blocking on ch<-, and Close() would
// block trying to acquire mu.Lock.
func TestPublishDoesNotBlockOnFullChannel(t *testing.T) {
	eb := NewEventBus(nil, nil)
	typ := EventType("deadlock.test")

	_, ch := eb.Subscribe(typ)

	// Fill the subscriber's channel buffer completely.
	for range EventQueueSize {
		eb.Publish(typ, NewEvent(typ, "fill"))
	}

	// This next Publish must complete without blocking. With the old
	// blocking send this would hang forever (deadlock). Use a channel
	// + require.Eventually to detect the hang.
	done := make(chan struct{})
	go func() {
		defer close(done)
		eb.Publish(typ, NewEvent(typ, "overflow"))
	}()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 2*time.Second, 5*time.Millisecond,
		"Publish should not block when subscriber channel is full",
	)

	// Drain the channel and verify we got EventQueueSize events
	// (the overflow event was dropped).
	drained := 0
	for drained < EventQueueSize {
		select {
		case <-ch:
			drained++
		default:
			t.Fatalf(
				"expected %d buffered events, got %d",
				EventQueueSize, drained,
			)
		}
	}

	// No extra event should be in the channel.
	select {
	case <-ch:
		t.Fatal("overflow event should have been dropped")
	default:
		// expected
	}

	eb.Stop()
}

// TestCloseDoesNotDeadlockWithFullChannel verifies that Close
// completes promptly even when the channel buffer is full and a
// concurrent Publish is in progress.
func TestCloseDoesNotDeadlockWithFullChannel(t *testing.T) {
	const iters = 500
	for range iters {
		eb := NewEventBus(nil, nil)
		typ := EventType("close.deadlock.test")
		subId, ch := eb.Subscribe(typ)

		// Fill the buffer.
		for range EventQueueSize {
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
			for range ch {
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
}
