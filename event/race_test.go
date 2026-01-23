package event

import (
	"sync"
	"sync/atomic"
	"testing"
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
