package event

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// TestUnsubscribeAndWaitStillWaitsAfterConcurrentPlainUnsubscribe guards
// against comment-28's original bug: unsubscribe only found the
// subscriber to Close/wait on via e.subscribers, which holds exactly one
// entry per subId -- so whichever of two concurrent calls for the same
// subId ran first (here, a plain Unsubscribe) removed that entry, leaving
// a second, concurrent UnsubscribeAndWait call for the identical subId
// with nothing to find, and it returned immediately without ever calling
// waitDone. This reproduces exactly that ordering (a completed plain
// Unsubscribe for a subId whose handler is still in flight, immediately
// followed by UnsubscribeAndWait for the same subId) and confirms
// UnsubscribeAndWait still blocks until the handler actually finishes.
func TestUnsubscribeAndWaitStillWaitsAfterConcurrentPlainUnsubscribe(t *testing.T) {
	eb := NewEventBus(nil, nil)
	defer eb.Stop()
	typ := EventType("race.unsubscribe-and-wait")

	handlerStarted := make(chan struct{})
	proceed := make(chan struct{})
	subId := eb.SubscribeFunc(typ, func(Event) {
		close(handlerStarted)
		<-proceed
	})

	eb.Publish(typ, NewEvent(typ, nil))
	testutil.RequireReceive(
		t, handlerStarted, time.Second, "handler must start",
	)

	// Plain Unsubscribe for this subId completes first (never waits), while
	// the handler above is still blocked in-flight.
	eb.Unsubscribe(typ, subId)

	waitDone := make(chan struct{})
	go func() {
		eb.UnsubscribeAndWait(typ, subId)
		close(waitDone)
	}()

	testutil.RequireNoReceive(
		t, waitDone, 150*time.Millisecond,
		"UnsubscribeAndWait must still block on the in-flight handler even "+
			"though a concurrent plain Unsubscribe for the same subId "+
			"already ran",
	)

	close(proceed)
	testutil.RequireReceive(
		t, waitDone, time.Second,
		"UnsubscribeAndWait must return once the handler finishes",
	)
}

// TestUnsubscribeIgnoresMismatchedEventType guards against comment-56's
// bug: channelSubsById is keyed by subId alone, with no eventType
// dimension, so Unsubscribe/UnsubscribeAndWait called with a subId that's
// valid but registered under a DIFFERENT eventType than the one passed in
// used to still find and close that subscriber via channelSubsById, even
// though the first, eventType-scoped lookup (e.subscribers[eventType])
// correctly found nothing. This calls Unsubscribe for a real subscriber's
// subId but under an unrelated eventType, and confirms the subscriber is
// unaffected -- still receives events -- until it's unsubscribed under
// its own, correct eventType.
func TestUnsubscribeIgnoresMismatchedEventType(t *testing.T) {
	eb := NewEventBus(nil, nil)
	defer eb.Stop()

	const wrongType EventType = "race.mismatch.wrong"
	const realType EventType = "race.mismatch.real"

	subId, ch := eb.Subscribe(realType)

	// subId is valid, but registered under realType, not wrongType -- this
	// call must find and affect nothing.
	eb.Unsubscribe(wrongType, subId)

	eb.Publish(realType, NewEvent(realType, "still-subscribed"))
	// Deliberately not testutil.RequireReceive: a closed channel is
	// always immediately ready to receive its zero value, so a plain
	// single-value receive would "succeed" here regardless of whether
	// the buggy mismatched-type Unsubscribe above actually closed the
	// channel -- checking ok (and the payload) is what actually tells
	// a real delivery apart from reading a channel Close already
	// closed out from under this subscriber.
	select {
	case evt, ok := <-ch:
		require.True(
			t, ok,
			"the channel must not be closed by an Unsubscribe call for "+
				"its subId under an unrelated eventType",
		)
		require.Equal(t, "still-subscribed", evt.Data)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the still-subscribed event")
	}

	// A real Unsubscribe (matching eventType) closes the channel -- so
	// this checks for that closure directly (a zero-value, ok=false
	// receive), rather than via RequireNoReceive: a closed channel is
	// always immediately ready to receive, which RequireNoReceive would
	// otherwise (correctly, per its own contract) report as "a value was
	// received" even though no real event was ever published to it.
	eb.Unsubscribe(realType, subId)
	_, ok := <-ch
	require.False(
		t, ok,
		"the channel must be closed once unsubscribed under its own eventType",
	)
}

// TestStopClearsPlainSubscribeEntriesFromChannelSubsById guards against
// comment-57's original leak: shutdown (run by both Stop and Close)
// closed every subscriber but never removed a plain Subscribe/
// SubscribeWithBuffer subscriber's channelSubsById entry -- a
// SubscribeFunc dispatch goroutine self-removes its own entry as it
// exits (and subscriberWg.Wait() inside shutdown already blocks until
// every one of them has), but a plain-Subscribe channel has no such
// goroutine, and unsubscribe() only clears an entry when a caller
// explicitly calls Unsubscribe/UnsubscribeAndWait for it -- which
// shutdown does not do on a caller's behalf. Left alone, an EventBus
// reused across repeated Stop()/resubscribe cycles (Stop supports
// exactly that, restarting its async workers) would accumulate an
// ever-growing set of abandoned entries, one per cycle's forgotten
// plain-Subscribe calls.
func TestStopClearsPlainSubscribeEntriesFromChannelSubsById(t *testing.T) {
	eb := NewEventBus(nil, nil)
	defer eb.Stop()

	const typ EventType = "race.channelsubsbyid.leak"
	_, _ = eb.Subscribe(typ)

	eb.mu.RLock()
	before := len(eb.channelSubsById)
	eb.mu.RUnlock()
	require.Equal(
		t, 1, before,
		"the plain Subscribe call must register itself in channelSubsById",
	)

	eb.Stop()

	eb.mu.RLock()
	after := len(eb.channelSubsById)
	eb.mu.RUnlock()
	require.Zero(
		t, after,
		"Stop must clear a plain Subscribe subscriber's channelSubsById "+
			"entry, not leak it across restarts",
	)
}

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
			wg.Go(func() {
				subId := eb.SubscribeFunc(typ, func(Event) {})
				if subId != 0 {
					successfulSubscribes.Add(1)
				}
			})
		}

		// Concurrently call Stop
		wg.Go(func() {
			eb.Stop()
		})

		wg.Wait()
		// If we get here without panic, the race is handled correctly.
		// Some SubscribeFunc calls may have succeeded (subId != 0) and
		// their goroutines should have been properly shut down by Stop.
	}
}

type blockingSubscriber struct {
	deliverStarted chan struct{}
	releaseDeliver chan struct{}
	deliverDone    chan struct{}
	closeCalled    atomic.Bool
	startOnce      sync.Once
	doneOnce       sync.Once
}

func newBlockingSubscriber() *blockingSubscriber {
	return &blockingSubscriber{
		deliverStarted: make(chan struct{}),
		releaseDeliver: make(chan struct{}),
		deliverDone:    make(chan struct{}),
	}
}

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

// TestStopWaitsForInFlightPublish verifies that Stop cannot close subscribers
// and return while a Publish call is still delivering to a subscriber.
func TestStopWaitsForInFlightPublish(t *testing.T) {
	eb := NewEventBus(nil, nil)
	typ := EventType("race.publish.stop.wait")
	sub := newBlockingSubscriber()
	require.NotZero(t, eb.RegisterSubscriber(typ, sub))

	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		eb.Publish(typ, NewEvent(typ, "blocked"))
	}()

	select {
	case <-sub.deliverStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish did not enter subscriber Deliver")
	}

	stopDone := make(chan struct{})
	stopStarted := make(chan struct{})
	go func() {
		close(stopStarted)
		defer close(stopDone)
		eb.Stop()
	}()
	<-stopStarted

	select {
	case <-stopDone:
		t.Fatal("Stop returned while Publish was still in flight")
	case <-time.After(25 * time.Millisecond):
		// Expected: Stop is blocked behind the in-flight Publish.
	}

	close(sub.releaseDeliver)

	select {
	case <-publishDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish did not complete after subscriber was released")
	}
	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not complete after in-flight Publish completed")
	}

	require.True(t, sub.closeCalled.Load(), "Stop should close subscriber")
	select {
	case <-sub.deliverDone:
	default:
		t.Fatal("subscriber Close happened before Deliver completed")
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

	_, ch := eb.SubscribeWithBuffer(typ, EventQueueSize)

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
		subId, ch := eb.SubscribeWithBuffer(typ, EventQueueSize)

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
