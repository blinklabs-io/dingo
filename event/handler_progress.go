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
// The rate limiter is cleared here as well as in endHandler: each invocation
// gets its own grace period, and clearing on both edges means a watchdog tick
// that races endHandler cannot leave a stale timestamp behind that suppresses
// the report for the next event.
func (c *channelSubscriber) beginHandler(now time.Time) {
	c.handlerWarnedAt.Store(0)
	c.handlerStartedAt.Store(now.UnixNano())
}

// endHandler records that the handler returned. Clearing the start time is
// what makes a subscription that is merely busy indistinguishable from an idle
// one: only a handler that has not returned is reported.
func (c *channelSubscriber) endHandler() {
	c.handlerStartedAt.Store(0)
	c.handlerWarnedAt.Store(0)
}

// handlerStuckFor reports how long the current handler invocation has been
// running, and whether one is running at all.
func (c *channelSubscriber) handlerStuckFor(
	now time.Time,
) (time.Duration, bool) {
	started := c.handlerStartedAt.Load()
	if started == 0 {
		return 0, false
	}
	elapsed := now.Sub(time.Unix(0, started))
	if elapsed < 0 {
		return 0, true
	}
	return elapsed, true
}

// warnStuckHandler reports a subscription whose handler has not returned, at
// most once per interval per subscription. Returns true when the subscription
// is currently stuck, whether or not this call logged.
func (c *channelSubscriber) warnStuckHandler(
	now time.Time,
	interval time.Duration,
) bool {
	elapsed, running := c.handlerStuckFor(now)
	if !running || elapsed < interval {
		return false
	}
	// Rate-limit per subscription. endHandler resets this, so a handler that
	// recovers starts from a clean slate rather than staying suppressed.
	lastWarn := c.handlerWarnedAt.Load()
	if lastWarn != 0 && now.Sub(time.Unix(0, lastWarn)) < interval {
		return true
	}
	if !c.handlerWarnedAt.CompareAndSwap(lastWarn, now.UnixNano()) {
		return true
	}
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
		if elapsed, running := sub.handlerStuckFor(now); running &&
			elapsed >= e.handlerProgressInterval {
			count++
		}
	}
	return count
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
