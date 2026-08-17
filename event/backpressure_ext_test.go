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

package event_test

import (
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// The tests in this file cover blinklabs-io/dingo#2932: neither the
// per-subscriber channel nor the shared async queue may discard events under
// sustained load, and backpressure must not wedge shutdown.

// TestPublishDeliversEveryEventUnderBackpressure is the headline regression:
// a producer far faster than its subscriber loses nothing.
func TestPublishDeliversEveryEventUnderBackpressure(t *testing.T) {
	const testEvtType event.EventType = "test.nodrop.publish"
	const eventCount = 5000

	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	// Deliberately tiny buffer: the producer spends nearly all its time
	// backpressured.
	_, subCh := eb.SubscribeWithBuffer(testEvtType, 8)

	received := make(chan []int, 1)
	go func() {
		got := make([]int, 0, eventCount)
		for evt := range subCh {
			v, ok := evt.Data.(int)
			if !ok {
				continue
			}
			got = append(got, v)
			if len(got) == eventCount {
				received <- got
				return
			}
		}
		received <- got
	}()

	for i := range eventCount {
		eb.Publish(testEvtType, event.NewEvent(testEvtType, i))
	}

	got := testutil.RequireReceive(
		t,
		received,
		10*time.Second,
		"subscriber did not receive every published event",
	)
	require.Len(t, got, eventCount, "no event may be dropped")
	for i, v := range got {
		require.Equal(t, i, v, "events must arrive in publish order")
	}
}

// TestPublishAsyncDeliversEveryEventUnderBackpressure covers the second drop
// site: the shared async queue. This is the ledger.tx shape from the issue.
func TestPublishAsyncDeliversEveryEventUnderBackpressure(t *testing.T) {
	const testEvtType event.EventType = "test.nodrop.async"
	const eventCount = 5000

	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	_, subCh := eb.SubscribeWithBuffer(testEvtType, 8)

	received := make(chan int, 1)
	go func() {
		count := 0
		for range subCh {
			count++
			if count == eventCount {
				received <- count
				return
			}
		}
		received <- count
	}()

	for i := range eventCount {
		require.True(
			t,
			eb.PublishAsync(testEvtType, event.NewEvent(testEvtType, i)),
			"PublishAsync must not drop event %d", i,
		)
	}

	require.Equal(
		t,
		eventCount,
		testutil.RequireReceive(
			t,
			received,
			10*time.Second,
			"subscriber did not receive every async-published event",
		),
	)
}

// TestPublishAsyncBlocksWhenQueueFull verifies PublishAsync waits for queue
// capacity instead of returning false and discarding the event.
func TestPublishAsyncBlocksWhenQueueFull(t *testing.T) {
	const testEvtType event.EventType = "test.async.full"

	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	// One-slot subscriber that nobody drains yet. The async workers park on
	// it, and the shared queue then fills behind them.
	_, subCh := eb.SubscribeWithBuffer(testEvtType, 1)

	// Enough to fill the subscriber buffer, occupy every async worker, and
	// saturate the queue.
	total := event.AsyncQueueSize + event.AsyncWorkerPoolSize + 8
	enqueued := make(chan bool, 1)
	go func() {
		for i := range total {
			if !eb.PublishAsync(testEvtType, event.NewEvent(testEvtType, i)) {
				enqueued <- false
				return
			}
		}
		enqueued <- true
	}()

	testutil.RequireNoReceive(
		t,
		enqueued,
		250*time.Millisecond,
		"PublishAsync should block once the async queue is full",
	)

	// Draining lets everything through; nothing was discarded.
	drained := make(chan int, 1)
	go func() {
		count := 0
		for range subCh {
			count++
			if count == total {
				drained <- count
				return
			}
		}
		drained <- count
	}()

	require.True(
		t,
		testutil.RequireReceive(
			t,
			enqueued,
			10*time.Second,
			"PublishAsync did not complete after the subscriber drained",
		),
		"PublishAsync must not drop events",
	)
	require.Equal(
		t,
		total,
		testutil.RequireReceive(
			t,
			drained,
			10*time.Second,
			"not every async event was delivered",
		),
	)
}

// TestPublishUnblocksOnStop is the shutdown-deadlock regression: a producer
// parked on a full subscriber must not prevent Stop from completing.
func TestPublishUnblocksOnStop(t *testing.T) {
	const testEvtType event.EventType = "test.backpressure.stop"

	eb := event.NewEventBus(nil, nil)
	_, _ = eb.SubscribeWithBuffer(testEvtType, 1)
	eb.Publish(testEvtType, event.NewEvent(testEvtType, "fill"))

	published := make(chan struct{})
	go func() {
		defer close(published)
		eb.Publish(testEvtType, event.NewEvent(testEvtType, "blocked"))
	}()

	testutil.RequireNoReceive(
		t,
		published,
		50*time.Millisecond,
		"Publish returned before Stop despite a full subscriber buffer",
	)

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		eb.Stop()
	}()

	testutil.RequireReceive(
		t,
		stopped,
		5*time.Second,
		"Stop deadlocked behind a backpressured Publish",
	)
	testutil.RequireReceive(
		t,
		published,
		5*time.Second,
		"Publish did not return after Stop",
	)
}

// TestPublishUnblocksOnUnsubscribe verifies a backpressured producer is
// released when the slow subscriber goes away.
func TestPublishUnblocksOnUnsubscribe(t *testing.T) {
	const testEvtType event.EventType = "test.backpressure.unsubscribe"

	eb := event.NewEventBus(nil, nil)
	defer eb.Stop()

	subId, _ := eb.SubscribeWithBuffer(testEvtType, 1)
	eb.Publish(testEvtType, event.NewEvent(testEvtType, "fill"))

	published := make(chan struct{})
	go func() {
		defer close(published)
		eb.Publish(testEvtType, event.NewEvent(testEvtType, "blocked"))
	}()

	testutil.RequireNoReceive(
		t,
		published,
		50*time.Millisecond,
		"Publish returned despite a full subscriber buffer",
	)

	eb.Unsubscribe(testEvtType, subId)

	testutil.RequireReceive(
		t,
		published,
		5*time.Second,
		"Publish did not return after the slow subscriber unsubscribed",
	)
}

// TestPublishAsyncUnblocksOnStop covers the same shutdown property for a
// producer parked on a full async queue.
func TestPublishAsyncUnblocksOnStop(t *testing.T) {
	const testEvtType event.EventType = "test.async.stop.unblock"

	eb := event.NewEventBus(nil, nil)
	_, _ = eb.SubscribeWithBuffer(testEvtType, 1)

	// Fill the subscriber, park every async worker, then saturate the queue.
	total := event.AsyncQueueSize + event.AsyncWorkerPoolSize + 8
	result := make(chan bool, 1)
	go func() {
		for i := range total {
			if !eb.PublishAsync(testEvtType, event.NewEvent(testEvtType, i)) {
				result <- false
				return
			}
		}
		result <- true
	}()

	testutil.RequireNoReceive(
		t,
		result,
		250*time.Millisecond,
		"PublishAsync should block once the async queue is full",
	)

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		eb.Stop()
	}()

	testutil.RequireReceive(
		t,
		stopped,
		5*time.Second,
		"Stop deadlocked behind a backpressured PublishAsync",
	)
	require.False(
		t,
		testutil.RequireReceive(
			t,
			result,
			5*time.Second,
			"PublishAsync did not return after Stop",
		),
		"PublishAsync should report failure only because the bus stopped",
	)
}

// TestCloseCompletesWithBlockedProducers checks the permanent-shutdown path as
// well as the reusable Stop path.
func TestCloseCompletesWithBlockedProducers(t *testing.T) {
	const testEvtType event.EventType = "test.backpressure.close"

	eb := event.NewEventBus(nil, nil)
	_, _ = eb.SubscribeWithBuffer(testEvtType, 1)
	eb.Publish(testEvtType, event.NewEvent(testEvtType, "fill"))

	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			eb.Publish(testEvtType, event.NewEvent(testEvtType, i))
		})
	}

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		eb.Close()
	}()
	testutil.RequireReceive(
		t,
		closed,
		5*time.Second,
		"Close deadlocked behind backpressured publishers",
	)

	producersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(producersDone)
	}()
	testutil.RequireReceive(
		t,
		producersDone,
		5*time.Second,
		"backpressured publishers did not return after Close",
	)
}

// TestBackpressureMetrics asserts the drop counter is gone and replaced by
// counters that report waiting instead of loss.
func TestBackpressureMetrics(t *testing.T) {
	const testEvtType event.EventType = "test.metrics.backpressure"

	reg := prometheus.NewRegistry()
	eb := event.NewEventBus(reg, nil)
	defer eb.Stop()

	_, subCh := eb.SubscribeWithBuffer(testEvtType, 1)
	eb.Publish(testEvtType, event.NewEvent(testEvtType, "fill"))

	published := make(chan struct{})
	go func() {
		defer close(published)
		eb.Publish(testEvtType, event.NewEvent(testEvtType, "blocked"))
	}()

	require.Eventually(t, func() bool {
		return counterValue(
			t,
			reg,
			"event_delivery_blocked_total",
			map[string]string{
				"type": string(testEvtType),
				"kind": "in-memory",
			},
		) >= 1
	}, 2*time.Second, 5*time.Millisecond,
		"a backpressured delivery should be counted",
	)

	<-subCh
	testutil.RequireReceive(
		t,
		published,
		5*time.Second,
		"Publish did not complete after capacity was freed",
	)

	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "event_delivery_errors_total" {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				require.NotEqual(
					t,
					"async-dropped",
					label.GetValue(),
					"nothing may be reported as dropped",
				)
			}
		}
	}
}

// TestAsyncEnqueueBlockedMetric asserts a saturated async queue is reported as
// waiting rather than as a dropped event.
func TestAsyncEnqueueBlockedMetric(t *testing.T) {
	const testEvtType event.EventType = "test.metrics.async"

	reg := prometheus.NewRegistry()
	eb := event.NewEventBus(reg, nil)
	_, subCh := eb.SubscribeWithBuffer(testEvtType, 1)

	total := event.AsyncQueueSize + event.AsyncWorkerPoolSize + 8
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range total {
			eb.PublishAsync(testEvtType, event.NewEvent(testEvtType, i))
		}
	}()

	require.Eventually(t, func() bool {
		return counterValue(
			t,
			reg,
			"event_async_enqueue_blocked_total",
			map[string]string{
				"type": string(testEvtType),
			},
		) >= 1
	}, 5*time.Second, 10*time.Millisecond,
		"a backpressured async enqueue should be counted",
	)

	go func() {
		for range subCh {
		}
	}()
	testutil.RequireReceive(
		t,
		done,
		10*time.Second,
		"PublishAsync did not complete after the subscriber drained",
	)
	eb.Stop()
}

// counterValue returns the value of a labeled counter in reg, or -1 when the
// metric is absent.
func counterValue(
	t *testing.T,
	reg prometheus.Gatherer,
	name string,
	labels map[string]string,
) float64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			got := make(map[string]string, len(metric.GetLabel()))
			for _, label := range metric.GetLabel() {
				got[label.GetName()] = label.GetValue()
			}
			if maps.Equal(got, labels) {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return -1
}
