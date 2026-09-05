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
