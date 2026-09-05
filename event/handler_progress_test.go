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
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

const handlerProgressTestType EventType = "test.handler_progress"

// A subscriber whose handler has stopped returning is invisible until its
// buffer fills, because the only stall report the bus has is raised by a
// publisher that had to wait for capacity. In blinklabs-io/dingo#3550 the
// chainselection.peer_activity handler stopped returning at 18:02 and the
// first "event delivery stalled" line appeared 12h31m later, once 1024
// keepalive events had piled up behind it. The handler being stuck is the
// fact worth reporting, and it is knowable immediately.
func TestSubscriberHandlerStuckIsReportedBeforeBufferFills(t *testing.T) {
	origInterval := handlerProgressWarnInterval
	handlerProgressWarnInterval = 20 * time.Millisecond
	t.Cleanup(func() { handlerProgressWarnInterval = origInterval })

	var buf lockedBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))
	bus := NewEventBus(nil, logger)

	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	// Close, not Stop: Stop restarts the worker pool, and the restarted
	// watchdog would read handlerProgressWarnInterval concurrently with this
	// test's cleanup restoring it.
	t.Cleanup(bus.Close)
	t.Cleanup(unblock)

	// A buffer far larger than the number of events published: nothing here
	// ever waits for capacity, so the existing full-buffer stall warning
	// cannot fire.
	bus.SubscribeFuncWithBuffer(
		handlerProgressTestType,
		1024,
		func(Event) { <-release },
	)

	bus.Publish(
		handlerProgressTestType,
		NewEvent(handlerProgressTestType, "stuck"),
	)

	testutil.WaitForConditionWithInterval(t, func() bool {
		return strings.Contains(
			buf.String(),
			"event subscriber handler not making progress",
		)
	}, 5*time.Second, time.Millisecond,
		"a handler that stopped returning must be reported without waiting "+
			"for its buffer to fill",
	)
	require.NotContains(t, buf.String(), "event delivery stalled",
		"the buffer never filled, so this must be the handler report",
	)
	require.Contains(t, buf.String(),
		"type="+string(handlerProgressTestType),
		"the report must name the subscriber's event type",
	)

	unblock()

	// A handler that returns must clear the condition rather than keep
	// reporting for the life of the subscription.
	warnings := strings.Count(
		buf.String(),
		"event subscriber handler not making progress",
	)
	testutil.WaitForCondition(t, func() bool {
		return bus.StuckHandlerCount() == 0
	}, 5*time.Second, "the handler returned, so nothing should still be stuck")
	settled := strings.Count(
		buf.String(),
		"event subscriber handler not making progress",
	)
	require.GreaterOrEqual(t, settled, warnings)

	before := strings.Count(
		buf.String(),
		"event subscriber handler not making progress",
	)
	testutil.RequireNoReceive(
		t,
		handlerProgressWarningsAfter(t, &buf, before),
		200*time.Millisecond,
		"the report must stop once the handler is making progress again",
	)
}

// handlerProgressWarningsAfter reports on its channel as soon as the warning
// count rises above want, so the caller can assert that it does not.
func handlerProgressWarningsAfter(
	t *testing.T,
	buf *lockedBuffer,
	want int,
) <-chan int {
	t.Helper()
	ch := make(chan int, 1)
	done := make(chan struct{})
	t.Cleanup(func() { close(done) })
	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				got := strings.Count(
					buf.String(),
					"event subscriber handler not making progress",
				)
				if got > want {
					select {
					case ch <- got:
					default:
					}
					return
				}
			}
		}
	}()
	return ch
}

// The report has to say which subscriber is stuck and for how long, or an
// operator cannot tell a wedged internal consumer from ordinary slow work.
func TestStuckHandlerReportIdentifiesSubscriber(t *testing.T) {
	origInterval := handlerProgressWarnInterval
	handlerProgressWarnInterval = 20 * time.Millisecond
	t.Cleanup(func() { handlerProgressWarnInterval = origInterval })

	var buf lockedBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))
	bus := NewEventBus(nil, logger)

	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	// Close, not Stop: Stop restarts the worker pool, and the restarted
	// watchdog would read handlerProgressWarnInterval concurrently with this
	// test's cleanup restoring it.
	t.Cleanup(bus.Close)
	t.Cleanup(unblock)

	bus.SubscribeFuncWithBufferPolicy(
		handlerProgressTestType,
		8,
		SubscriberBackpressureBlock,
		func(Event) { <-release },
	)
	bus.Publish(
		handlerProgressTestType,
		NewEvent(handlerProgressTestType, "stuck"),
	)

	testutil.WaitForConditionWithInterval(t, func() bool {
		return bus.StuckHandlerCount() == 1 &&
			strings.Contains(
				buf.String(),
				"event subscriber handler not making progress",
			)
	}, 5*time.Second, time.Millisecond,
		"the bus should report exactly one stuck handler",
	)
	logs := buf.String()
	require.Contains(t, logs, "stuck_for=")
	require.Contains(t, logs, "queued=")
	require.Contains(t, logs, "buffer=8")
}

// A watchdog tick that races the handler's return must not silence the report
// for the invocation that follows it.
//
// handlerStuckFor can load the running invocation's start time and then, before
// the tick writes its rate-limit stamp, have that handler return and the
// dispatch loop begin the next event. The stamp then lands while a different
// invocation is running. Keying it to the invocation it actually described is
// what keeps it from suppressing that invocation's own first report — a stamp
// cleared on each begin cannot, because the racing write happens after the
// clear.
func TestStuckHandlerRateLimitIsPerInvocation(t *testing.T) {
	const interval = time.Second

	var buf lockedBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))
	c := newChannelSubscriber(handlerProgressTestType, 1, logger)

	first := time.Now()
	c.beginHandler(first)
	firstWarn := first.Add(interval)
	require.True(t, c.warnStuckHandler(firstWarn, interval))
	require.Equal(t, 1, stuckHandlerWarnings(&buf))

	// Replay the losing interleaving. The tick above sampled `first`; the
	// handler returned and the dispatch loop began the next event before the
	// tick's stamp was written, so the stamp is applied here, after
	// beginHandler.
	second := first.Add(interval / 2)
	c.endHandler()
	c.beginHandler(second)
	c.handlerWarnedAt.Store(firstWarn.UnixNano())
	c.handlerWarnedFor.Store(first.UnixNano())

	// The second invocation has now been stuck for a full interval of its
	// own and must be reported, even though the stale stamp is younger than
	// one interval.
	require.True(t, c.warnStuckHandler(second.Add(interval), interval))
	require.Equal(t, 2, stuckHandlerWarnings(&buf),
		"a report stamped for an invocation that already returned must not "+
			"suppress the first report for the one running now",
	)

	// The rate limit still holds within one invocation.
	require.True(
		t,
		c.warnStuckHandler(second.Add(interval+interval/2), interval),
	)
	require.Equal(t, 2, stuckHandlerWarnings(&buf),
		"repeat reports for the same invocation stay rate-limited",
	)
}

func stuckHandlerWarnings(buf *lockedBuffer) int {
	return strings.Count(
		buf.String(),
		"event subscriber handler not making progress",
	)
}

// event_subscriber_handler_stalled_total only ever moves on a stall, so a
// healthy bus would export no series for it at all and a query could not tell
// "nothing here has stalled" from "this subscription does not exist" — which
// is the distinction an operator watching for a wedged internal consumer
// needs. Registering a SubscribeFunc subscription materializes its zero.
func TestHandlerStallSeriesExistsBeforeAnyStall(t *testing.T) {
	const funcType EventType = "test.handler_stall.func"
	const chanType EventType = "test.handler_stall.chan"

	registry := prometheus.NewRegistry()
	eb := NewEventBus(registry, nil)
	t.Cleanup(eb.Close)

	require.Equal(
		t,
		0,
		promtestutil.CollectAndCount(eb.metrics.handlerStalls),
		"nothing is subscribed yet",
	)

	eb.SubscribeFunc(funcType, func(Event) {})
	require.Equal(
		t,
		1,
		promtestutil.CollectAndCount(eb.metrics.handlerStalls),
		"a SubscribeFunc subscription must publish its zero",
	)

	// A Subscribe channel's read loop is not owned by the bus, so the
	// watchdog never reports it. A series for it would be permanently zero
	// and would misrepresent the subscription as observed.
	_, _ = eb.Subscribe(chanType)
	require.Equal(
		t,
		1,
		promtestutil.CollectAndCount(eb.metrics.handlerStalls),
		"a channel subscription is not observable and gets no series",
	)

	require.Equal(
		t,
		float64(0),
		promtestutil.ToFloat64(
			eb.metrics.handlerStalls.WithLabelValues(string(funcType)),
		),
	)
}
