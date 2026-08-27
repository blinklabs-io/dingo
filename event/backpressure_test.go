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
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// The tests in this file cover blinklabs-io/dingo#2932: channelSubscriber must
// wait for buffer capacity rather than dropping events, without reintroducing
// the Close() deadlock that the original non-blocking send avoided.

// TestDeliverWaitsForCapacityThenDelivers is the core no-loss property: a
// delivery into a full buffer parks until a slot frees, then lands.
func TestDeliverWaitsForCapacityThenDelivers(t *testing.T) {
	sub := newChannelSubscriber("test", 1, nil)
	require.NoError(t, sub.Deliver(NewEvent("test", "first")))

	done := make(chan error, 1)
	go func() {
		done <- sub.Deliver(NewEvent("test", "second"))
	}()

	select {
	case <-done:
		t.Fatal("Deliver returned while the buffer was full")
	case <-time.After(50 * time.Millisecond):
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
	sub := newChannelSubscriber("test", 1, nil)
	require.NoError(t, sub.Deliver(NewEvent("test", "fill")))

	done := make(chan error, 1)
	go func() {
		done <- sub.Deliver(NewEvent("test", "blocked"))
	}()

	// Make sure Deliver is actually parked before closing.
	select {
	case <-done:
		t.Fatal("Deliver returned while the buffer was full")
	case <-time.After(50 * time.Millisecond):
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
	sub := newChannelSubscriber("test", 1, nil)
	require.NoError(t, sub.DeliverBlocking(NewEvent("test", "fill")))

	done := make(chan error, 1)
	go func() {
		done <- sub.DeliverBlocking(NewEvent("test", "blocked"))
	}()

	select {
	case <-done:
		t.Fatal("DeliverBlocking returned while the buffer was full")
	case <-time.After(50 * time.Millisecond):
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
// the per-event log spam from blinklabs-io/dingo#1556.
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
		t.Fatal("PublishBlocking did not return after stalled-subscriber timeout")
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
