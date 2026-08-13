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
)

// The stall warning is rate-limited per delivery, which bounds it to one line
// per interval per *blocked publisher*. That is not a bound at all when the
// publishers are what is numerous: a wedged subscriber with N goroutines parked
// on it emits N lines per interval, forever.
//
// This is not hypothetical. A node-to-client peer reconnecting in a tight loop
// parked publishers on connmanager.conn_closed faster than they drained and
// produced 7.7 million identical warnings in a 40-minute run -- enough to bury
// the one signal an operator needs to see, and to make the logs themselves a
// second problem. The bound has to be per subscriber.
func TestDeliverStallWarningIsRateLimitedPerSubscriber(t *testing.T) {
	origInterval := deliveryStallWarnInterval
	deliveryStallWarnInterval = 20 * time.Millisecond
	t.Cleanup(func() { deliveryStallWarnInterval = origInterval })

	var buf lockedBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))
	sub := newChannelSubscriber("test", 1, logger)
	require.NoError(t, sub.Deliver(NewEvent("test.stalled", "fill")))

	// Park many publishers on the subscriber at once.
	const blockedPublishers = 40
	var wg sync.WaitGroup
	for i := range blockedPublishers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sub.Deliver(NewEvent("test.stalled", i))
		}()
	}

	require.Eventually(t, func() bool {
		return strings.Contains(buf.String(), "event delivery stalled")
	}, 2*time.Second, 5*time.Millisecond,
		"a stalled subscriber should still be reported",
	)

	// Wait until the warning has repeated a few times, which means several
	// intervals have elapsed. A per-subscriber limit yields one line per
	// interval, so the count here tracks intervals. A per-delivery limit
	// yields one line per interval *per parked publisher*, so the count
	// tracks publishers instead and overshoots immediately.
	const observedRepeats = 3
	require.Eventually(t, func() bool {
		return countStallWarnings(buf.String()) >= observedRepeats
	}, 2*time.Second, time.Millisecond,
		"the warning should repeat while the stall continues",
	)

	got := countStallWarnings(buf.String())
	t.Logf("stall warnings after %d repeats: %d (publishers=%d)",
		observedRepeats, got, blockedPublishers)
	require.Less(t, got, blockedPublishers,
		"stall warnings scaled with the number of blocked publishers "+
			"(%d warnings, %d publishers): the rate limit must be per "+
			"subscriber, not per delivery", got, blockedPublishers,
	)

	// Release the parked publishers so the test does not leak goroutines.
	sub.Close()
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		wg.Wait()
	}()
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked publishers did not unpark after Close")
	}
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

	var wg sync.WaitGroup
	for i := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sub.Deliver(NewEvent("test.stalled", i))
		}()
	}

	require.Eventually(t, func() bool {
		return strings.Contains(buf.String(), "blocked_publishers")
	}, 2*time.Second, 5*time.Millisecond,
		"the warning should report how many publishers are parked",
	)

	sub.Close()
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		wg.Wait()
	}()
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("blocked publishers did not unpark after Close")
	}
}

func countStallWarnings(logs string) int {
	return strings.Count(logs, "event delivery stalled")
}
