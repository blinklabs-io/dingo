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

import "time"

// handlerProgressWarnInterval is how long a SubscribeFunc handler may stay
// inside a single event before the subscription is reported as not making
// progress, and how often that report repeats while it stays there.
// Overridden in tests.
//
// Each EventBus snapshots it once, in NewEventBus, into
// handlerProgressInterval. The watchdog is a long-lived background goroutine,
// so re-reading a package variable on every tick would race any test that
// overrides it against every bus an earlier test left running.
var handlerProgressWarnInterval = 30 * time.Second

// beginHandler records that this subscription's dispatch goroutine has entered
// its handler. Called immediately before the handler runs, and paired with
// endHandler.
//
// It deliberately does not touch the watchdog's rate-limit stamp. Clearing it
// here would race the watchdog: a tick that read the previous invocation's
// start time can write its stamp after this store, and the cleared field would
// then be re-set for an invocation that has just begun. The stamp instead
// carries the start time it belongs to, so a stale one cannot match.
func (c *channelSubscriber) beginHandler(now time.Time) {
	c.handlerStartedAt.Store(now.UnixNano())
}

// endHandler records that the handler returned. Clearing the start time is
// what makes a subscription that is merely busy indistinguishable from an idle
// one: only a handler that has not returned is reported.
func (c *channelSubscriber) endHandler() {
	c.handlerStartedAt.Store(0)
}

// handlerStuckFor reports how long the current handler invocation has been
// running, which invocation that is (its unix-nano start time, 0 when none),
// and whether one is running at all.
func (c *channelSubscriber) handlerStuckFor(
	now time.Time,
) (time.Duration, int64, bool) {
	started := c.handlerStartedAt.Load()
	if started == 0 {
		return 0, 0, false
	}
	elapsed := now.Sub(time.Unix(0, started))
	if elapsed < 0 {
		return 0, started, true
	}
	return elapsed, started, true
}

// warnStuckHandler reports a subscription whose handler has not returned, at
// most once per interval per subscription. Returns true when the subscription
// is currently stuck, whether or not this call logged.
//
// Called only from handlerProgressWatchdog, which is what makes the unlocked
// read-then-write of the two stamp fields safe: a bus runs one watchdog at a
// time, and nothing else writes them.
func (c *channelSubscriber) warnStuckHandler(
	now time.Time,
	interval time.Duration,
) bool {
	elapsed, started, running := c.handlerStuckFor(now)
	if !running || elapsed < interval {
		return false
	}
	// Rate-limit per invocation, not per subscription: a stamp left behind
	// for an invocation that has since returned names a different start
	// time, so it cannot suppress the first report for the current one.
	if c.handlerWarnedFor.Load() == started {
		lastWarn := c.handlerWarnedAt.Load()
		if lastWarn != 0 && now.Sub(time.Unix(0, lastWarn)) < interval {
			return true
		}
	}
	c.handlerWarnedAt.Store(now.UnixNano())
	c.handlerWarnedFor.Store(started)
	if c.logger != nil {
		c.logger.Warn(
			"event subscriber handler not making progress",
			"type", c.eventType,
			"stuck_for", elapsed.Truncate(time.Millisecond),
			"queued", len(c.ch),
			"buffer", cap(c.ch),
			"blocked_publishers", c.stallWaiters.Load(),
		)
	}
	return true
}

// StuckHandlerCount reports how many SubscribeFunc subscriptions currently
// have a handler that has been running longer than handlerProgressWarnInterval.
//
// Exported so the node can surface the condition alongside its other health
// signals: a required internal consumer that has stopped returning is a node
// fault, and waiting for its buffer to fill turns a 30-second symptom into a
// half-day one (blinklabs-io/dingo#3550).
func (e *EventBus) StuckHandlerCount() int {
	if e == nil {
		return 0
	}
	now := time.Now()
	count := 0
	for _, sub := range e.channelSubscriberSnapshot() {
		if elapsed, _, running := sub.handlerStuckFor(now); running &&
			elapsed >= e.handlerProgressInterval {
			count++
		}
	}
	return count
}

// observeHandlerProgress materializes the zero-valued handler-stall series for
// an event type the watchdog can observe. The counter is only ever incremented
// by a stall, so without this a healthy bus exports no series at all for the
// type and a Prometheus query cannot tell "no handler for this type has ever
// stalled" from "no handler for this type was ever registered" -- the two
// answers an operator most needs to distinguish.
//
// The series is per event type, so subscriptions to one type share it, and it
// outlives their unsubscribe like any other counter. Called for the
// SubscribeFunc paths only: a Subscribe channel's read loop is not owned by
// the bus, so the watchdog never reports it and a series for it would always
// read zero.
func (e *EventBus) observeHandlerProgress(eventType EventType) {
	if e.metrics == nil {
		return
	}
	e.metrics.handlerStalls.WithLabelValues(string(eventType)).Add(0)
}

// channelSubscriberSnapshot copies the live channel subscribers out from under
// e.mu so the watchdog never holds the bus lock while logging.
func (e *EventBus) channelSubscriberSnapshot() []*channelSubscriber {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if len(e.channelSubsById) == 0 {
		return nil
	}
	subs := make([]*channelSubscriber, 0, len(e.channelSubsById))
	for _, sub := range e.channelSubsById {
		subs = append(subs, sub)
	}
	return subs
}

// handlerProgressWatchdog reports subscriptions whose handler has stopped
// returning.
//
// The existing stall report is raised by a publisher that had to wait for
// buffer capacity, so it can only fire once the buffer is already full. That
// makes the time to first signal a function of the buffer size and the event
// rate rather than of the fault: the chainselection.peer_activity handler in
// blinklabs-io/dingo#3550 stopped returning 12h31m before its 1024-slot buffer
// filled and said so. This watchdog observes the handler itself, so the report
// arrives one interval after the handler stops making progress no matter how
// much headroom the buffer has.
//
// stopCh is passed in rather than read from e: a Stop/restart cycle swaps
// e.stopCh, and this worker must watch the generation it was started under.
func (e *EventBus) handlerProgressWatchdog(stopCh chan struct{}) {
	defer e.asyncWg.Done()
	interval := e.handlerProgressInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case now := <-ticker.C:
			for _, sub := range e.channelSubscriberSnapshot() {
				if sub.warnStuckHandler(now, interval) && e.metrics != nil {
					e.metrics.handlerStalls.WithLabelValues(
						string(sub.eventType),
					).Inc()
				}
			}
		}
	}
}
