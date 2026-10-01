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
// Not t.Parallel: swaps the package-level deliveryStallWarnInterval.
func TestDeliverStallWarningIsRateLimitedPerSubscriber(t *testing.T) {
	origInterval := deliveryStallWarnInterval
	deliveryStallWarnInterval = 20 * time.Millisecond
	t.Cleanup(func() { deliveryStallWarnInterval = origInterval })

	// Compare warning volume at two very different publisher counts over the
	// same observation condition. This is a ratio rather than an absolute
	// count on purpose: any assertion that samples "how many warnings by
	// now" races the burst, because a per-delivery limiter emits all of its
	// warnings within a single interval and the sample can land part-way
	// through. What cannot be faked by timing is the *scaling* -- a
	// per-subscriber bound is independent of publisher count, a per-delivery
	// bound is proportional to it.
	const fewPublishers = 4
	const manyPublishers = 40
	const observedRepeats = 3

	few := stallWarningsForPublishers(t, fewPublishers, observedRepeats)
	many := stallWarningsForPublishers(t, manyPublishers, observedRepeats)
	t.Logf("stall warnings: %d publishers -> %d, %d publishers -> %d",
		fewPublishers, few, manyPublishers, many)

	// A per-delivery limit multiplies the volume by the publisher ratio
	// (10x here). A per-subscriber limit leaves it flat, so a generous 3x
	// allowance still separates them decisively.
	require.LessOrEqual(t, many, few*3,
		"stall warnings scaled with the number of blocked publishers "+
			"(%d publishers -> %d warnings, %d -> %d): the rate limit must "+
			"be per subscriber, not per delivery",
		fewPublishers, few, manyPublishers, many,
	)
}

// stallWarningsForPublishers parks publishers on a subscriber that never
// drains, waits until the stall warning has repeated observedRepeats times,
// and reports how many warnings were emitted by that point.
func stallWarningsForPublishers(
	t *testing.T,
	publishers int,
	observedRepeats int,
) int {
	t.Helper()

	var buf lockedBuffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelWarn,
	}))
	sub := newChannelSubscriber("test", 1, logger)
	require.NoError(t, sub.Deliver(NewEvent("test.stalled", "fill")))

	var wg sync.WaitGroup
	for i := range publishers {
		wg.Go(func() {
			_ = sub.Deliver(NewEvent("test.stalled", i))
		})
	}
	// Park everyone before the first interval elapses, so both runs are
	// measured from the same starting condition.
	require.Eventually(t, func() bool {
		return sub.stallWaiters.Load() == int64(publishers)
	}, 2*time.Second, time.Millisecond,
		"every publisher should park on the stalled subscriber",
	)
	require.Eventually(t, func() bool {
		return countStallWarnings(buf.String()) >= observedRepeats
	}, 5*time.Second, time.Millisecond,
		"the warning should repeat while the stall continues",
	)
	got := countStallWarnings(buf.String())

	sub.Close()
	requirePublishersUnpark(t, &wg)
	return got
}

func countStallWarnings(logs string) int {
	return strings.Count(logs, "event delivery stalled")
}
