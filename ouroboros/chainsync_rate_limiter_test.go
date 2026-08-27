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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChainsyncFindIntersectRateLimiter_NormalUse(t *testing.T) {
	rl := newChainsyncFindIntersectRateLimiter(200, 1000)
	peer := testConnIdWithPort(4001)

	// A well-behaved client's occasional, in-bounds FindIntersect requests
	// stay well within the burst.
	assert.True(t, rl.Allow(peer, 100), "first request should be allowed")
	assert.True(t, rl.Allow(peer, 100), "second request should be allowed")
	assert.True(t, rl.Allow(peer, 100), "third request should be allowed")
}

func TestChainsyncFindIntersectRateLimiter_BoundaryAtBurst(t *testing.T) {
	rl := newChainsyncFindIntersectRateLimiter(200, 1000)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rl.nowFunc = func() time.Time { return now }
	peer := testConnIdWithPort(4001)

	// A single request exactly at the burst (matching
	// chainsyncMaxFindIntersectPoints) must be allowed in full.
	assert.True(
		t,
		rl.Allow(peer, 1000),
		"a request exactly at the burst must be allowed",
	)
	// The very next point, with no time elapsed, must be rejected: the
	// budget is now exhausted.
	assert.False(
		t,
		rl.Allow(peer, 1),
		"a request one point past the exhausted burst must be rejected",
	)
}

func TestChainsyncFindIntersectRateLimiter_RepeatedRequestsExhaustBudget(
	t *testing.T,
) {
	rl := newChainsyncFindIntersectRateLimiter(200, 1000)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rl.nowFunc = func() time.Time { return now }
	peer := testConnIdWithPort(4001)

	// Several smaller, individually in-bounds requests must draw from the
	// same cumulative budget as one large request.
	for i := range 4 {
		assert.True(
			t,
			rl.Allow(peer, 250),
			"request %d within the cumulative budget should be allowed",
			i,
		)
	}
	assert.False(
		t,
		rl.Allow(peer, 1),
		"a request after the cumulative budget is spent must be rejected",
	)
}

func TestChainsyncFindIntersectRateLimiter_PerPeerIsolation(t *testing.T) {
	rl := newChainsyncFindIntersectRateLimiter(200, 1000)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rl.nowFunc = func() time.Time { return now }

	peerA := testConnIdWithPort(4001)
	peerB := testConnIdWithPort(4002)

	assert.True(t, rl.Allow(peerA, 1000), "peer A burst allowed")
	assert.False(t, rl.Allow(peerA, 1), "peer A should be over budget")

	// Peer B has an independent budget, unaffected by peer A's requests.
	assert.True(t, rl.Allow(peerB, 1000), "peer B should be unaffected")
	assert.False(t, rl.Allow(peerB, 1), "peer B should now be over budget")
}

func TestChainsyncFindIntersectRateLimiter_Recovery(t *testing.T) {
	rl := newChainsyncFindIntersectRateLimiter(200, 1000)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rl.nowFunc = func() time.Time { return now }
	peer := testConnIdWithPort(4001)

	assert.True(t, rl.Allow(peer, 1000), "burst allowed")
	assert.False(t, rl.Allow(peer, 1), "should be over budget")

	// Advance time: the budget refills at the configured rate.
	now = now.Add(1 * time.Second) // +200 points at rate=200/s
	assert.True(
		t,
		rl.Allow(peer, 200),
		"should allow a request within the refilled budget",
	)
	assert.False(
		t,
		rl.Allow(peer, 1),
		"should reject again once the refill is spent",
	)
}

func TestChainsyncFindIntersectRateLimiter_ZeroPointRequestAlwaysAllowed(
	t *testing.T,
) {
	rl := newChainsyncFindIntersectRateLimiter(200, 1000)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rl.nowFunc = func() time.Time { return now }
	peer := testConnIdWithPort(4001)

	require.True(t, rl.Allow(peer, 1000), "exhaust the budget")
	// An empty (post-deduplication) point list costs nothing and must
	// never be rejected by the budget, matching how GetIntersectPoint
	// treats an empty list.
	assert.True(
		t,
		rl.Allow(peer, 0),
		"a zero-point request must always be allowed",
	)
}

func TestChainsyncFindIntersectRateLimiter_RemovePeer(t *testing.T) {
	rl := newChainsyncFindIntersectRateLimiter(200, 1000)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rl.nowFunc = func() time.Time { return now }
	peer := testConnIdWithPort(4001)

	require.True(t, rl.Allow(peer, 1000), "burst allowed")
	require.False(t, rl.Allow(peer, 1), "should be over budget")

	rl.RemovePeer(peer)

	// Removing the peer drops its bucket; the next request starts with a
	// fresh burst rather than inheriting the exhausted state.
	assert.True(
		t,
		rl.Allow(peer, 1000),
		"a fresh bucket should be created after RemovePeer",
	)
}

func TestNewOuroboros_ChainsyncFindIntersectLimiterAlwaysEnabled(t *testing.T) {
	// Unlike TxSubmission, FindIntersect is entirely peer-driven rather than
	// paced by our own request loop, so this limiter is not configurable
	// off; it must always be present.
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})

	require.NotNil(t, o.chainsyncFindIntersectLimiter)
	assert.Equal(
		t,
		float64(chainsyncFindIntersectBudgetRate),
		o.chainsyncFindIntersectLimiter.rate,
	)
	assert.Equal(
		t,
		chainsyncFindIntersectBudgetBurst,
		o.chainsyncFindIntersectLimiter.burst,
	)
}

func TestHandleConnClosedEvent_CleansUpChainsyncFindIntersectLimiter(
	t *testing.T,
) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := newOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})

	peer := testConnIdWithPort(4001)

	o.chainsyncFindIntersectLimiter.Allow(peer, 1)
	_, exists := o.chainsyncFindIntersectLimiter.peers.Load(connIdKey(peer))
	require.True(t, exists, "peer should exist in the limiter")

	evt := event.NewEvent(
		connmanager.ConnectionClosedEventType,
		connmanager.ConnectionClosedEvent{
			ConnectionId: peer,
		},
	)
	o.HandleConnClosedEvent(evt)

	_, exists = o.chainsyncFindIntersectLimiter.peers.Load(connIdKey(peer))
	assert.False(
		t,
		exists,
		"peer should be removed from the limiter after connection closed event",
	)
}
