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
// against a real bug: unsubscribe only found the
// subscriber to Close/wait on via e.subscribers, which holds exactly one
// entry per subId -- so whichever of two concurrent calls for the same
// subId ran first (here, a plain Unsubscribe) removed that entry, leaving
// a second, concurrent UnsubscribeAndWait call for the identical subId
// with nothing to find, and it returned immediately without ever calling
// waitDone. This reproduces exactly that ordering (a completed plain
// Unsubscribe for a subId whose handler is still in flight, immediately
// followed by UnsubscribeAndWait for the same subId) and confirms
// UnsubscribeAndWait still blocks until the handler actually finishes.
func TestUnsubscribeAndWaitStillWaitsAfterConcurrentPlainUnsubscribe(
	t *testing.T,
) {
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

// TestUnsubscribeIgnoresMismatchedEventType guards against a real
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
// a real leak: shutdown (run by both Stop and Close)
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

// TestPublishBlocksOnFullChannelUntilDrained verifies that Publish applies
// backpressure when a subscriber's channel buffer is full, and that the event
// is delivered once capacity appears rather than dropped. Regression test for
// blinklabs-io/dingo#2932. Close() must still be able to run against an
// in-flight blocked send without deadlocking, which is why the blocked send
// wakes on the subscriber's close signal.
func TestPublishBlocksOnFullChannelUntilDrained(t *testing.T) {
	eb := NewEventBus(nil, nil)
	typ := EventType("backpressure.test")

	const buffer = 64
	_, ch := eb.SubscribeWithBuffer(typ, buffer)

	// Fill the subscriber's channel buffer completely.
	for i := range buffer {
		eb.Publish(typ, NewEvent(typ, i))
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		eb.Publish(typ, NewEvent(typ, "overflow"))
	}()

	select {
	case <-done:
		t.Fatal("Publish returned while the subscriber buffer was full")
	case <-time.After(50 * time.Millisecond):
		// Expected: the publisher is backpressured.
	}

	// Draining releases the publisher and the event must arrive.
	drained := make([]any, 0, buffer+1)
	for range buffer {
		drained = append(drained, (<-ch).Data)
	}

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 2*time.Second, 5*time.Millisecond,
		"Publish should complete once subscriber capacity is available",
	)

	select {
	case evt := <-ch:
		drained = append(drained, evt.Data)
	case <-time.After(time.Second):
		t.Fatal("event that was backpressured never arrived")
	}

	require.Len(t, drained, buffer+1, "no event may be dropped")
	require.Equal(t, "overflow", drained[buffer])

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

// TestSubscribeFuncDoneVisibleBeforeSubIdPublished guards against a real
// bug: subscribeInternal published chSub into channelSubsById while still
// holding e.mu, but chSub.done for a SubscribeFuncWithBuffer subscriber was
// only set afterwards, in SubscribeFuncWithBuffer, after subscribeInternal
// had already returned and released e.mu. That left a window where a
// subId was visible in channelSubsById with done still nil, and two
// problems followed: (1) a concurrent Unsubscribe/UnsubscribeAndWait call
// for that exact subId (e.g. the next sequential ID, which is entirely
// predictable since subIds increment by one) reads done == nil as "no
// dispatch goroutine exists for this subscriber" and deletes the
// channelSubsById entry and returns without ever waiting -- defeating
// UnsubscribeAndWait's entire purpose; and (2) the write to chSub.done in
// SubscribeFuncWithBuffer and unsubscribe's reads of it were not
// synchronized by any shared lock, i.e. a genuine data race.
//
// This runs many iterations of SubscribeFuncWithBuffer racing against
// UnsubscribeAndWait for the predicted next subId, while a concurrent
// checker goroutine continuously scans channelSubsById (under e.mu, the
// same lock both the subscribe and unsubscribe paths use) asserting that
// every entry present there always has a non-nil done -- which must hold
// at every instant once the fix keeps the map publish and the done
// assignment inside the same e.mu critical section. Run with -race: with
// the old ordering restored, this both trips the invariant check below
// and is reliably flagged by the race detector as an unsynchronized
// read/write of chSub.done.
func TestSubscribeFuncDoneVisibleBeforeSubIdPublished(t *testing.T) {
	eb := NewEventBus(nil, nil)
	defer eb.Stop()
	typ := EventType("race.subscribefunc.done-visibility")

	var invariantViolated atomic.Bool
	stopChecker := make(chan struct{})
	checkerDone := make(chan struct{})
	go func() {
		defer close(checkerDone)
		for {
			select {
			case <-stopChecker:
				return
			default:
			}
			eb.mu.RLock()
			for _, chSub := range eb.channelSubsById {
				if chSub.done == nil {
					invariantViolated.Store(true)
				}
			}
			eb.mu.RUnlock()
		}
	}()

	const iterations = 300
	for range iterations {
		eb.mu.RLock()
		predictedSubId := eb.lastSubId + 1
		eb.mu.RUnlock()

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			eb.SubscribeFuncWithBuffer(
				typ,
				DefaultSubscriberBuffer,
				func(Event) {},
			)
		}()
		go func() {
			defer wg.Done()
			eb.UnsubscribeAndWait(typ, predictedSubId)
		}()
		wg.Wait()
	}

	close(stopChecker)
	<-checkerDone

	require.False(
		t, invariantViolated.Load(),
		"a channelSubsById entry for a SubscribeFuncWithBuffer subscriber "+
			"must never be observable with done == nil; the map publish "+
			"raced ahead of done initialization",
	)
}
