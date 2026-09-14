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

package ouroboros

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChainsyncFindIntersectRateLimiter_NormalUse(t *testing.T) {
	t.Parallel()

	rl := newChainsyncFindIntersectRateLimiter(200, 1000)

	// A well-behaved client's occasional, in-bounds FindIntersect requests
	// stay well within the burst.
	assert.True(t, rl.Allow(100), "first request should be allowed")
	assert.True(t, rl.Allow(100), "second request should be allowed")
	assert.True(t, rl.Allow(100), "third request should be allowed")
}

func TestChainsyncFindIntersectRateLimiter_BoundaryAtBurst(t *testing.T) {
	t.Parallel()

	rl := newChainsyncFindIntersectRateLimiter(200, 1000)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rl.nowFunc = func() time.Time { return now }

	// A single request exactly at the burst (matching
	// chainsyncMaxFindIntersectPoints) must be allowed in full.
	assert.True(
		t,
		rl.Allow(1000),
		"a request exactly at the burst must be allowed",
	)
	// The very next point, with no time elapsed, must be rejected: the
	// budget is now exhausted.
	assert.False(
		t,
		rl.Allow(1),
		"a request one point past the exhausted burst must be rejected",
	)
}

func TestChainsyncFindIntersectRateLimiter_RepeatedRequestsExhaustBudget(
	t *testing.T,
) {
	t.Parallel()

	rl := newChainsyncFindIntersectRateLimiter(200, 1000)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rl.nowFunc = func() time.Time { return now }

	// Several smaller, individually in-bounds requests must draw from the
	// same cumulative budget as one large request.
	for i := range 4 {
		assert.True(
			t,
			rl.Allow(250),
			"request %d within the cumulative budget should be allowed",
			i,
		)
	}
	assert.False(
		t,
		rl.Allow(1),
		"a request after the cumulative budget is spent must be rejected",
	)
}

func TestChainsyncFindIntersectRateLimiter_Recovery(t *testing.T) {
	t.Parallel()

	rl := newChainsyncFindIntersectRateLimiter(200, 1000)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rl.nowFunc = func() time.Time { return now }

	assert.True(t, rl.Allow(1000), "burst allowed")
	assert.False(t, rl.Allow(1), "should be over budget")

	// Advance time: the budget refills at the configured rate.
	now = now.Add(1 * time.Second) // +200 points at rate=200/s
	assert.True(
		t,
		rl.Allow(200),
		"should allow a request within the refilled budget",
	)
	assert.False(
		t,
		rl.Allow(1),
		"should reject again once the refill is spent",
	)
}

func TestChainsyncFindIntersectRateLimiter_ZeroPointRequestAlwaysAllowed(
	t *testing.T,
) {
	t.Parallel()

	rl := newChainsyncFindIntersectRateLimiter(200, 1000)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rl.nowFunc = func() time.Time { return now }

	require.True(t, rl.Allow(1000), "exhaust the budget")
	// An empty (post-deduplication) point list costs nothing and must
	// never be rejected by the budget, matching how GetIntersectPoint
	// treats an empty list.
	assert.True(
		t,
		rl.Allow(0),
		"a zero-point request must always be allowed",
	)
}
