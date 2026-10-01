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
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestAsyncWorkerDropsQueuedEventAfterStop(t *testing.T) {
	t.Parallel()

	const attempts = 128
	const testEvtType EventType = "test.async.stop"

	for attempt := range attempts {
		eb := &EventBus{
			subscribers: make(
				map[EventType]map[EventSubscriberId]Subscriber,
			),
			subscriberSnapshots: make(map[EventType][]subscriberEntry),
			asyncQueue:          make(chan asyncEvent, 1),
			stopCh:              make(chan struct{}),
		}
		sub := newChannelSubscriber(testEvtType, 1, nil)
		eb.subscriberSnapshots[testEvtType] = []subscriberEntry{
			{
				id:         1,
				sub:        sub,
				channelSub: sub,
				kind:       "in-memory",
			},
		}
		eb.asyncQueue <- asyncEvent{
			eventType: testEvtType,
			event:     NewEvent(testEvtType, attempt),
		}
		close(eb.stopCh)

		eb.asyncWg.Add(1)
		go eb.asyncWorker()

		workerDone := make(chan struct{})
		go func() {
			eb.asyncWg.Wait()
			close(workerDone)
		}()

		select {
		case <-workerDone:
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for async worker shutdown")
		}

		select {
		case evt := <-sub.ch:
			t.Fatalf(
				"queued async event was delivered after stop on attempt %d: %v",
				attempt,
				evt,
			)
		default:
		}
	}
}

// TestDeliverWaitsForCapacityThenDelivers is the core no-loss property: a
// delivery into a full buffer parks until a slot frees, then lands.
func TestDeliverWaitsForCapacityThenDelivers(t *testing.T) {
	t.Parallel()

	sub := newChannelSubscriber("test", 1, nil)
	require.NoError(t, sub.Deliver(NewEvent("test", "first")))

	blocked := make(chan struct{})
	sub.onBlocked = func() { close(blocked) }
	done := make(chan error, 1)
	go func() {
		done <- sub.Deliver(NewEvent("test", "second"))
	}()
	testutil.RequireReceive(
		t,
		blocked,
		testutil.AsyncWait,
		"delivery never reached the full-buffer wait",
	)
	require.Len(t, sub.ch, cap(sub.ch), "the delivery buffer must be full")
	select {
	case <-done:
		t.Fatal("Deliver returned while the buffer was full")
	default:
	}

	require.Equal(t, "first", (<-sub.ch).Data)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Deliver did not complete after capacity was freed")
	}
	require.Equal(t, "second", (<-sub.ch).Data)
}

// TestDeliverUnblocksOnClose is the constraint the original non-blocking send
// existed to satisfy: Close must not deadlock behind an in-flight Deliver.
// Deliver holds mu.RLock while waiting, so Close has to signal waiters before
// it takes mu.Lock.
func TestDeliverUnblocksOnClose(t *testing.T) {
	t.Parallel()

	sub := newChannelSubscriber("test", 1, nil)
	require.NoError(t, sub.Deliver(NewEvent("test", "fill")))

	blocked := make(chan struct{})
	sub.onBlocked = func() { close(blocked) }
	done := make(chan error, 1)
	go func() {
		done <- sub.Deliver(NewEvent("test", "blocked"))
	}()
	testutil.RequireReceive(
		t,
		blocked,
		testutil.AsyncWait,
		"delivery never reached the full-buffer wait",
	)
	require.Len(t, sub.ch, cap(sub.ch), "the delivery buffer must be full")
	select {
	case <-done:
		t.Fatal("Deliver returned while the buffer was full")
	default:
	}

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		sub.Close()
	}()

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close deadlocked behind a blocked Deliver")
	}

	select {
	case err := <-done:
		// Deliver swallows the closed error so Publish does not treat a
		// shutting-down subscriber as a delivery failure.
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Deliver did not return after Close")
	}
}

// TestDeliverBlockingUnblocksOnClose is the same property for the variant
// PublishBlocking uses, which must surface the closed error so PublishBlocking
// can report ErrEventBusStopped.
func TestDeliverBlockingUnblocksOnClose(t *testing.T) {
	t.Parallel()

	sub := newChannelSubscriber("test", 1, nil)
	require.NoError(t, sub.DeliverBlocking(NewEvent("test", "fill")))

	blocked := make(chan struct{})
	sub.onBlocked = func() { close(blocked) }
	done := make(chan error, 1)
	go func() {
		done <- sub.DeliverBlocking(NewEvent("test", "blocked"))
	}()
	testutil.RequireReceive(
		t,
		blocked,
		testutil.AsyncWait,
		"delivery never reached the full-buffer wait",
	)
	require.Len(t, sub.ch, cap(sub.ch), "the delivery buffer must be full")
	select {
	case <-done:
		t.Fatal("DeliverBlocking returned while the buffer was full")
	default:
	}

	sub.Close()

	select {
	case err := <-done:
		require.ErrorIs(t, err, errChannelSubscriberClosed)
	case <-time.After(2 * time.Second):
		t.Fatal("DeliverBlocking did not return after Close")
	}
}

// TestCloseRaceWithBlockedDelivers stresses the window between a waiting send
// and close(ch). A send that resumes after the channel is closed would panic.
func TestCloseRaceWithBlockedDelivers(t *testing.T) {
	t.Parallel()

	const iters = 500
	const senders = 8
	for range iters {
		sub := newChannelSubscriber("test", 1, nil)
		require.NoError(t, sub.Deliver(NewEvent("test", "fill")))

		var wg sync.WaitGroup
		for i := range senders {
			wg.Go(func() {
				_ = sub.Deliver(NewEvent("test", i))
			})
		}
		wg.Go(func() {
			// Free a slot so at least one sender resumes concurrently
			// with Close.
			<-sub.ch
		})
		wg.Go(sub.Close)

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("blocked Deliver and Close deadlocked")
		}
	}
}

// TestDeliverStallWarning verifies operators still get a signal when a
// subscriber stops draining. Backpressure is normal under load, so the warning
// is emitted only after a delivery has been parked for a full interval, and it
// repeats at most once per interval rather than once per event.
// Not t.Parallel: this and the other tests here swap the package-level
// deliveryStallWarnInterval / channelDeliveryTimeout tunables, which every
// concurrently running event-bus test in this package would observe.
func TestDeliverStallWarning(t *testing.T) {
	origInterval := deliveryStallWarnInterval
	deliveryStallWarnInterval = 20 * time.Millisecond
	t.Cleanup(func() { deliveryStallWarnInterval = origInterval })

	var buf lockedBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))
	sub := newChannelSubscriber("test", 1, logger)
	require.NoError(t, sub.Deliver(NewEvent("test.stalled", "fill")))

	done := make(chan error, 1)
	go func() {
		done <- sub.Deliver(NewEvent("test.stalled", "blocked"))
	}()

	require.Eventually(t, func() bool {
		return strings.Contains(buf.String(), "event delivery stalled")
	}, 2*time.Second, 5*time.Millisecond,
		"a stalled delivery should be reported",
	)
	require.Contains(t, buf.String(), "test.stalled", "warning names the type")

	// The delivery still completes once capacity appears.
	<-sub.ch
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("stalled Deliver did not complete after capacity was freed")
	}
	require.Equal(t, "blocked", (<-sub.ch).Data)
}

// TestDeliverDoesNotWarnWhenCapacityIsAvailable guards against reintroducing
// the per-event log spam .
func TestDeliverDoesNotWarnWhenCapacityIsAvailable(t *testing.T) {
	origInterval := deliveryStallWarnInterval
	deliveryStallWarnInterval = 20 * time.Millisecond
	t.Cleanup(func() { deliveryStallWarnInterval = origInterval })

	var buf lockedBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))
	sub := newChannelSubscriber("test", 4, logger)

	for i := range 1000 {
		require.NoError(t, sub.Deliver(NewEvent("test.quiet", i)))
		<-sub.ch
	}
	require.Empty(t, buf.String(), "steady-state delivery must not log")
}

// TestDeliverAfterCloseReturnsClosed keeps the post-close contract explicit:
// DeliverBlocking reports the closed subscriber, Deliver swallows it.
func TestDeliverAfterCloseReturnsClosed(t *testing.T) {
	t.Parallel()

	sub := newChannelSubscriber("test", 1, nil)
	sub.Close()

	require.True(
		t,
		errors.Is(
			sub.DeliverBlocking(NewEvent("test", "x")),
			errChannelSubscriberClosed,
		),
	)
	require.NoError(t, sub.Deliver(NewEvent("test", "x")))
}

// TestPublishDetachesStalledSubscriberWithoutReorderingHealthyDelivery proves
// the EventBus boundary rather than channelSubscriber in isolation. A dead
// subscriber cannot hold a topic forever; healthy subscribers still receive
// every event accepted for them in the publisher's order.
func TestPublishDetachesStalledSubscriberWithoutReorderingHealthyDelivery(
	t *testing.T,
) {
	originalTimeout := channelDeliveryTimeout
	channelDeliveryTimeout = 20 * time.Millisecond
	t.Cleanup(func() { channelDeliveryTimeout = originalTimeout })

	const eventType EventType = "test.stalled.detach"
	eb := NewEventBus(nil, nil)
	t.Cleanup(eb.Stop)

	_, stalled := eb.SubscribeWithBuffer(eventType, 1)
	_, healthy := eb.SubscribeWithBuffer(eventType, 2)

	eb.Publish(eventType, NewEvent(eventType, 0))
	require.Equal(t, 0, (<-healthy).Data)

	published := make(chan struct{})
	go func() {
		defer close(published)
		eb.Publish(eventType, NewEvent(eventType, 1))
	}()

	select {
	case <-published:
	case <-time.After(time.Second):
		t.Fatal("stalled subscriber was not detached within the delivery bound")
	}
	require.Equal(t, 1, (<-healthy).Data)

	// The stalled channel keeps only work accepted before detachment, then
	// closes. The second event was never accepted by that subscriber.
	require.Equal(t, 0, (<-stalled).Data)
	_, open := <-stalled
	require.False(t, open, "stalled subscriber should be detached and closed")
}

// TestPublishBlockingReportsStalledSubscriber verifies the synchronous API
// exposes the lifecycle boundary instead of silently treating a detached
// subscriber as a successful delivery.
func TestPublishBlockingReportsStalledSubscriber(t *testing.T) {
	originalTimeout := channelDeliveryTimeout
	channelDeliveryTimeout = 20 * time.Millisecond
	t.Cleanup(func() { channelDeliveryTimeout = originalTimeout })

	const eventType EventType = "test.stalled.blocking"
	eb := NewEventBus(nil, nil)
	t.Cleanup(eb.Stop)

	_, stalled := eb.SubscribeWithBuffer(eventType, 1)
	eb.Publish(eventType, NewEvent(eventType, "first"))

	errCh := make(chan error, 1)
	go func() {
		errCh <- eb.PublishBlocking(eventType, NewEvent(eventType, "second"))
	}()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, ErrEventSubscriberStalled)
	case <-time.After(time.Second):
		t.Fatal(
			"PublishBlocking did not return after stalled-subscriber timeout",
		)
	}

	require.Equal(t, "first", (<-stalled).Data)
	_, open := <-stalled
	require.False(t, open, "stalled subscriber should be closed")
}

// TestLosslessSubscribeFuncDoesNotDetach verifies the policy used by
// ordering-critical ledger streams. A full callback queue must apply
// backpressure until the handler drains it, rather than silently removing the
// subscriber after the ordinary delivery timeout.
func TestLosslessSubscribeFuncDoesNotDetach(t *testing.T) {
	originalTimeout := channelDeliveryTimeout
	channelDeliveryTimeout = 20 * time.Millisecond
	t.Cleanup(func() { channelDeliveryTimeout = originalTimeout })

	const eventType EventType = "test.lossless.callback"
	eb := NewEventBus(nil, nil)
	t.Cleanup(eb.Stop)

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	eb.SubscribeFuncWithBufferPolicy(
		eventType,
		1,
		SubscriberBackpressureBlock,
		func(evt Event) {
			if evt.Data == "first" {
				once.Do(func() { close(started) })
				<-release
			}
		},
	)

	eb.Publish(eventType, NewEvent(eventType, "first"))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("lossless callback did not begin")
	}
	eb.Publish(eventType, NewEvent(eventType, "second"))

	published := make(chan error, 1)
	go func() {
		published <- eb.PublishBlocking(eventType, NewEvent(eventType, "third"))
	}()
	select {
	case err := <-published:
		t.Fatalf("lossless callback detached while full: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-published:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("lossless callback did not resume after draining")
	}
}

// TestStalledSubscriberDetachmentReportsWarningAndMetric makes the bounded
// path observable with the production defaults: detachment emits a warning and
// increments the delivery-timeout metric even though the periodic stall warning
// interval is longer than the normal delivery timeout.
func TestStalledSubscriberDetachmentReportsWarningAndMetric(t *testing.T) {
	originalTimeout := channelDeliveryTimeout
	channelDeliveryTimeout = 20 * time.Millisecond
	t.Cleanup(func() { channelDeliveryTimeout = originalTimeout })

	const eventType EventType = "test.stalled.observability"
	var buf lockedBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))
	registry := prometheus.NewRegistry()
	eb := NewEventBus(registry, logger)
	t.Cleanup(eb.Stop)

	_, _ = eb.SubscribeWithBuffer(eventType, 1)
	eb.Publish(eventType, NewEvent(eventType, "first"))
	eb.Publish(eventType, NewEvent(eventType, "second"))

	require.Contains(
		t,
		buf.String(),
		"event subscriber detached after delivery timeout",
	)
	require.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			eb.metrics.deliveryTimeouts.WithLabelValues(string(eventType)),
		),
	)
}

// TestConcurrentPublishBlockingReportsStalledSubscriber preserves the cause
// for every publisher already parked on one subscription when the first timer
// detaches it. Returning nil to the later publishers would falsely report an
// accepted delivery that never reached their subscriber.
func TestConcurrentPublishBlockingReportsStalledSubscriber(t *testing.T) {
	originalTimeout := channelDeliveryTimeout
	channelDeliveryTimeout = 20 * time.Millisecond
	t.Cleanup(func() { channelDeliveryTimeout = originalTimeout })

	const eventType EventType = "test.stalled.concurrent"
	const publishers = 8
	eb := NewEventBus(nil, nil)
	t.Cleanup(eb.Stop)

	_, _ = eb.SubscribeWithBuffer(eventType, 1)
	eb.Publish(eventType, NewEvent(eventType, "first"))

	results := make(chan error, publishers)
	for range publishers {
		go func() {
			results <- eb.PublishBlocking(
				eventType,
				NewEvent(eventType, "blocked"),
			)
		}()
	}
	for range publishers {
		select {
		case err := <-results:
			require.ErrorIs(t, err, ErrEventSubscriberStalled)
		case <-time.After(time.Second):
			t.Fatal("blocked publisher did not receive the detachment cause")
		}
	}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// mockSubscriber returns an error on Deliver to simulate a failing remote client.
type mockSubscriber struct {
	closed bool
}

func (m *mockSubscriber) Deliver(evt Event) error {
	return fmt.Errorf("deliver failed")
}

func (m *mockSubscriber) Close() {
	m.closed = true
}

func TestDeliverFailureUnregisters(t *testing.T) {
	t.Parallel()

	// Create a bus without metrics
	eb := NewEventBus(nil, nil)
	// Register mock subscriber
	sub := &mockSubscriber{}
	subId := eb.RegisterSubscriber("test.fail", sub)
	if subId == 0 {
		t.Fatalf("expected non-zero sub id")
	}
	// Publish event should cause deliver failure and unregister
	eb.Publish("test.fail", NewEvent("test.fail", "x"))
	// After publish, subscriber map for event type should not contain subId
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	if subs, ok := eb.subscribers["test.fail"]; ok {
		if _, exists := subs[subId]; exists {
			t.Fatalf("expected subscriber to be removed after deliver failure")
		}
	}
	if !sub.closed {
		t.Fatalf(
			"expected subscriber Close() to be called after deliver failure",
		)
	}
}

// TestChannelSubscriberDeliverWaitsForCapacity verifies that
// channelSubscriber.Deliver waits for buffer capacity instead of dropping the
// event. Regression: the non-blocking send
// this replaces silently discarded events under sustained load.
func TestChannelSubscriberDeliverWaitsForCapacity(t *testing.T) {
	t.Parallel()

	const bufferSize = 5
	sub := newChannelSubscriber("test", bufferSize, nil)

	// Fill the buffer completely
	for i := range bufferSize {
		err := sub.Deliver(NewEvent("test", i))
		if err != nil {
			t.Fatalf("unexpected error on buffered deliver: %v", err)
		}
	}

	// Deliver to the full buffer must wait rather than drop.
	blocked := make(chan struct{})
	sub.onBlocked = func() { close(blocked) }
	done := make(chan error, 1)
	go func() {
		done <- sub.Deliver(NewEvent("test", "overflow"))
	}()
	testutil.RequireReceive(
		t,
		blocked,
		testutil.AsyncWait,
		"delivery never reached the full-buffer wait",
	)
	if len(sub.ch) != cap(sub.ch) {
		t.Fatal("expected the delivery buffer to be full")
	}
	select {
	case <-done:
		t.Fatal("Deliver returned while the buffer was full; event was dropped")
	default:
	}

	// Draining one slot releases the waiting Deliver.
	first := <-sub.ch

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error after capacity freed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Deliver did not complete after buffer capacity was freed")
	}

	// Every event is accounted for: the drained one, the rest of the
	// original batch, and the event that had to wait.
	got := []any{first.Data}
	for range bufferSize {
		select {
		case evt := <-sub.ch:
			got = append(got, evt.Data)
		default:
			t.Fatalf("expected %d events, only got %d", bufferSize+1, len(got))
		}
	}
	want := []any{0, 1, 2, 3, 4, "overflow"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("event %d: expected %v, got %v", i, want[i], got[i])
		}
	}
}

// TestChannelSubscriberDeliverAfterClose verifies that Deliver to a closed
// subscriber returns nil (not a panic) and does not block.
func TestChannelSubscriberDeliverAfterClose(t *testing.T) {
	t.Parallel()

	sub := newChannelSubscriber("test", 5, nil)
	sub.Close()

	done := make(chan error, 1)
	go func() {
		done <- sub.Deliver(NewEvent("test", "after-close"))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Deliver after Close should return nil, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Deliver blocked after Close")
	}
}

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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

// TestStopWaitsForInFlightPublish verifies that Stop cannot close subscribers
// and return while a Publish call is still delivering to a subscriber.
func TestStopWaitsForInFlightPublish(t *testing.T) {
	t.Parallel()

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
//. Close must still be able to run against an
// in-flight blocked send without deadlocking, which is why the blocked send
// wakes on the subscriber's close signal.
func TestPublishBlocksOnFullChannelUntilDrained(t *testing.T) {
	t.Parallel()

	eb := NewEventBus(nil, nil)
	typ := EventType("backpressure.test")

	const buffer = 64
	subId, ch := eb.SubscribeWithBuffer(typ, buffer)

	// Set onBlocked immediately after subscribing, before any publisher can
	// reach this subscriber: onBlocked's documented contract (event.go) is
	// that it is set once at subscribe time. Setting it after the fill loop
	// below would write it unsynchronized while it is readable under
	// deliverWait's c.mu.RLock, and would read channelSubsById without
	// e.mu.
	blocked := make(chan struct{})
	eb.channelSubsById[subId].onBlocked = func() { close(blocked) }

	// Fill the subscriber's channel buffer completely.
	for i := range buffer {
		eb.Publish(typ, NewEvent(typ, i))
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		eb.Publish(typ, NewEvent(typ, "overflow"))
	}()

	// Synchronize on deliverWait's full-buffer wait path rather than on the
	// overflow goroutine merely starting: the goroutine can pause before
	// Publish reaches its blocking send, which would let the checks below
	// pass without proving delivery blocked.
	testutil.RequireReceive(
		t,
		blocked,
		testutil.AsyncWait,
		"delivery never reached the full-buffer wait",
	)
	require.Len(t, ch, cap(ch), "the subscriber buffer must be full")
	select {
	case <-done:
		t.Fatal("Publish returned while the subscriber buffer was full")
	default:
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
	t.Parallel()

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

// A stalled subscriber must still be reported often enough to be actionable,
// and the report must say how many publishers are parked on it -- that count
// is what distinguishes ordinary backpressure from a wedged subscriber.
func TestDeliverStallWarningReportsBlockedPublishers(t *testing.T) {
	origInterval := deliveryStallWarnInterval
	deliveryStallWarnInterval = 20 * time.Millisecond
	t.Cleanup(func() { deliveryStallWarnInterval = origInterval })

	var buf lockedBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))
	sub := newChannelSubscriber("test", 1, logger)
	require.NoError(t, sub.Deliver(NewEvent("test.stalled", "fill")))

	const blockedPublishers = 3
	var wg sync.WaitGroup
	for i := range blockedPublishers {
		wg.Go(func() {
			_ = sub.Deliver(NewEvent("test.stalled", i))
		})
	}

	// Every warning carries the field, so asserting the key is present
	// proves nothing about the number. Wait for all publishers to park, then
	// require the reported count to be the number actually parked.
	require.Eventually(t, func() bool {
		return sub.stallWaiters.Load() == blockedPublishers
	}, 2*time.Second, time.Millisecond,
		"every publisher should park on the stalled subscriber",
	)
	want := fmt.Sprintf("blocked_publishers=%d", blockedPublishers)
	require.Eventually(t, func() bool {
		return strings.Contains(buf.String(), want)
	}, 2*time.Second, 5*time.Millisecond,
		"the warning should report the number of publishers actually parked",
	)

	sub.Close()
	requirePublishersUnpark(t, &wg)
}

// requirePublishersUnpark fails unless every parked publisher returns, using
// the repository channel helper rather than a hand-rolled select so the
// timeout contract is the shared one.
func requirePublishersUnpark(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		wg.Wait()
	}()
	testutil.RequireReceive(
		t,
		drained,
		5*time.Second,
		"blocked publishers did not unpark after Close",
	)
}
