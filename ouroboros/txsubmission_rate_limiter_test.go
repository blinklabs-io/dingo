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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	ouroboros_conn "github.com/blinklabs-io/gouroboros/connection"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testConnIdWithPort creates a ConnectionId with a unique remote port
// for testing.
func testConnIdWithPort(
	port int,
) ouroboros_conn.ConnectionId {
	return ouroboros_conn.ConnectionId{
		LocalAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: 3001,
		},
		RemoteAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: port,
		},
	}
}

func TestTokenBucket_BasicAllow(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tb := newTokenBucket(10, 20, now)

	// Should allow up to burst size
	assert.True(
		t,
		tb.allow(10, now),
		"should allow 10 tokens from burst of 20",
	)
	assert.True(
		t,
		tb.allow(10, now),
		"should allow another 10 tokens from burst of 20",
	)
	// Burst exhausted
	assert.False(
		t,
		tb.allow(1, now),
		"should reject when tokens exhausted",
	)
}

func TestTokenBucket_Refill(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tb := newTokenBucket(10, 20, now)

	// Exhaust all tokens
	assert.True(t, tb.allow(20, now), "should allow burst")
	assert.False(
		t,
		tb.allow(1, now),
		"should reject when exhausted",
	)

	// Advance time by 1 second: refill 10 tokens
	later := now.Add(1 * time.Second)
	assert.True(
		t,
		tb.allow(10, later),
		"should allow 10 tokens after 1s refill",
	)
	assert.False(
		t,
		tb.allow(1, later),
		"should reject after consuming refilled tokens",
	)
}

func TestTokenBucket_RefillCap(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tb := newTokenBucket(10, 20, now)

	// Exhaust all tokens
	assert.True(t, tb.allow(20, now), "should allow burst")

	// Advance time by 10 seconds: refill would be 100 but capped
	// at 20
	later := now.Add(10 * time.Second)
	assert.True(
		t,
		tb.allow(20, later),
		"should allow max burst after long refill",
	)
	assert.False(
		t,
		tb.allow(1, later),
		"should reject since bucket only holds burst",
	)
}

func TestTokenBucket_PartialConsumption(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tb := newTokenBucket(10, 20, now)

	// Consume some tokens
	assert.True(t, tb.allow(5, now), "should allow 5 of 20")
	assert.True(t, tb.allow(5, now), "should allow another 5")
	assert.True(t, tb.allow(5, now), "should allow another 5")
	assert.True(t, tb.allow(5, now), "should allow last 5")
	assert.False(t, tb.allow(1, now), "should reject at 0 tokens")
}

func TestTxSubmissionRateLimiter_NormalRate(t *testing.T) {
	rl := newTxSubmissionRateLimiter(30, 60)
	peer := testConnIdWithPort(4001)

	// Normal submission rate: 10 txs at a time, well within burst
	assert.True(
		t,
		rl.Allow(peer, 10),
		"normal batch of 10 should be allowed",
	)
	assert.True(
		t,
		rl.Allow(peer, 10),
		"second batch of 10 should be allowed",
	)
	assert.True(
		t,
		rl.Allow(peer, 10),
		"third batch of 10 should be allowed",
	)
}

func TestTxSubmissionRateLimiter_ExcessiveRate(t *testing.T) {
	rl := newTxSubmissionRateLimiter(10, 20)
	// Override time function for deterministic testing
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rl.nowFunc = func() time.Time { return now }

	peer := testConnIdWithPort(4001)

	// Exhaust the burst
	assert.True(
		t,
		rl.Allow(peer, 10),
		"first batch should be allowed",
	)
	assert.True(
		t,
		rl.Allow(peer, 10),
		"second batch should be allowed (burst=20)",
	)
	// Now the bucket is empty
	assert.False(
		t,
		rl.Allow(peer, 1),
		"should reject when rate limit exceeded",
	)
}

func TestTxSubmissionRateLimiter_PerPeerIsolation(t *testing.T) {
	rl := newTxSubmissionRateLimiter(10, 20)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rl.nowFunc = func() time.Time { return now }

	peerA := testConnIdWithPort(4001)
	peerB := testConnIdWithPort(4002)

	// Exhaust peer A's bucket
	assert.True(t, rl.Allow(peerA, 20), "peer A burst allowed")
	assert.False(
		t,
		rl.Allow(peerA, 1),
		"peer A should be rate limited",
	)

	// Peer B should still have a full bucket
	assert.True(
		t,
		rl.Allow(peerB, 20),
		"peer B should be unaffected by peer A",
	)
	assert.False(
		t,
		rl.Allow(peerB, 1),
		"peer B should now be rate limited",
	)
}

func TestTxSubmissionRateLimiter_Recovery(t *testing.T) {
	rl := newTxSubmissionRateLimiter(10, 20)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mu := sync.Mutex{}
	rl.nowFunc = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}

	peer := testConnIdWithPort(4001)

	// Exhaust the bucket
	assert.True(t, rl.Allow(peer, 20), "burst allowed")
	assert.False(
		t,
		rl.Allow(peer, 1),
		"should be rate limited",
	)

	// Advance time: tokens should refill
	mu.Lock()
	now = now.Add(2 * time.Second) // +20 tokens at rate=10/s
	mu.Unlock()

	assert.True(
		t,
		rl.Allow(peer, 10),
		"should allow after refill",
	)
}

func TestTxSubmissionRateLimiter_RemovePeer(t *testing.T) {
	rl := newTxSubmissionRateLimiter(10, 20)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rl.nowFunc = func() time.Time { return now }

	peer := testConnIdWithPort(4001)

	// Exhaust the bucket
	assert.True(t, rl.Allow(peer, 20), "burst allowed")
	assert.False(
		t,
		rl.Allow(peer, 1),
		"should be rate limited",
	)

	// Remove peer and verify a new bucket is created
	rl.RemovePeer(peer)
	assert.True(
		t,
		rl.Allow(peer, 20),
		"should have fresh bucket after removal",
	)
}

func TestTxSubmissionRateLimiter_ConcurrentAccess(t *testing.T) {
	rl := newTxSubmissionRateLimiter(1000, 2000)
	peer := testConnIdWithPort(4001)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Just exercise the rate limiter concurrently;
			// we don't assert results since timing varies
			rl.Allow(peer, 1)
		}()
	}
	wg.Wait()
	// If the test reaches here without data races (with -race),
	// the concurrent access is safe
}

func TestNewOuroboros_DefaultRateLimit(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := NewOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})

	require.NotNil(
		t,
		o.txSubmissionRateLimiter,
		"rate limiter should be initialized with default config",
	)
	assert.Equal(
		t,
		float64(DefaultMaxTxSubmissionsPerSecond),
		o.txSubmissionRateLimiter.rate,
		"rate should use default",
	)
	assert.Equal(
		t,
		float64(DefaultMaxTxSubmissionsPerSecond)*2,
		o.txSubmissionRateLimiter.burst,
		"burst should be 2x default rate",
	)
}

func TestNewOuroboros_CustomRateLimit(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := NewOuroboros(OuroborosConfig{
		Logger:                    logger,
		EventBus:                  event.NewEventBus(nil, logger),
		MaxTxSubmissionsPerSecond: 50,
	})

	require.NotNil(t, o.txSubmissionRateLimiter)
	assert.Equal(
		t,
		float64(50),
		o.txSubmissionRateLimiter.rate,
		"rate should use custom value",
	)
	assert.Equal(
		t,
		float64(100),
		o.txSubmissionRateLimiter.burst,
		"burst should be 2x custom rate",
	)
}

func TestNewOuroboros_DisabledRateLimit(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := NewOuroboros(OuroborosConfig{
		Logger:                    logger,
		EventBus:                  event.NewEventBus(nil, logger),
		MaxTxSubmissionsPerSecond: -1,
	})

	assert.Nil(
		t,
		o.txSubmissionRateLimiter,
		"rate limiter should be nil when disabled",
	)
}

func TestHandleConnClosedEvent_CleansUpRateLimiter(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := NewOuroboros(OuroborosConfig{
		Logger:   logger,
		EventBus: event.NewEventBus(nil, logger),
	})

	peer := testConnIdWithPort(4001)

	// Create rate limiter state for this peer
	o.txSubmissionRateLimiter.Allow(peer, 1)

	// Verify the peer exists in the rate limiter
	o.txSubmissionRateLimiter.mu.Lock()
	_, exists := o.txSubmissionRateLimiter.peers[peer]
	o.txSubmissionRateLimiter.mu.Unlock()
	require.True(t, exists, "peer should exist in rate limiter")

	// Simulate a connection closed event through the actual handler
	evt := event.NewEvent(
		connmanager.ConnectionClosedEventType,
		connmanager.ConnectionClosedEvent{
			ConnectionId: peer,
		},
	)
	o.HandleConnClosedEvent(evt)

	// Verify cleanup
	o.txSubmissionRateLimiter.mu.Lock()
	_, exists = o.txSubmissionRateLimiter.peers[peer]
	o.txSubmissionRateLimiter.mu.Unlock()
	assert.False(
		t,
		exists,
		"peer should be removed from rate limiter after connection closed event",
	)
}

func TestTxSubmissionRateLimiter_CustomRatePlumbing(t *testing.T) {
	// Verify that a custom MaxTxSubmissionsPerSecond value is
	// correctly plumbed through to the rate limiter with the
	// expected rate and burst (2x rate).
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	o := NewOuroboros(OuroborosConfig{
		Logger:                    logger,
		EventBus:                  event.NewEventBus(nil, logger),
		MaxTxSubmissionsPerSecond: 10,
	})

	require.NotNil(t, o.txSubmissionRateLimiter)
	assert.Equal(
		t,
		float64(10),
		o.txSubmissionRateLimiter.rate,
		"rate should match custom config value",
	)
	assert.Equal(
		t,
		float64(20),
		o.txSubmissionRateLimiter.burst,
		"burst should be 2x the custom rate",
	)
}

func TestTokenBucket_ZeroTokenRequest(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tb := newTokenBucket(10, 20, now)

	// Zero tokens should always be allowed
	assert.True(
		t,
		tb.allow(0, now),
		"zero tokens should always be allowed",
	)

	// Exhaust the bucket
	assert.True(t, tb.allow(20, now), "consume all tokens")

	// Zero tokens should still be allowed even when bucket is empty
	assert.True(
		t,
		tb.allow(0, now),
		"zero tokens should be allowed even when empty",
	)
}

func TestTokenBucket_WaitDuration(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tb := newTokenBucket(10, 20, now)

	// Tokens available - no wait needed
	assert.Equal(
		t,
		time.Duration(0),
		tb.waitDuration(10, now),
		"should not wait when tokens are available",
	)

	// Exhaust the bucket
	assert.True(t, tb.allow(20, now), "consume all tokens")

	// Need 10 tokens at rate 10/s = 1 second wait
	assert.Equal(
		t,
		1*time.Second,
		tb.waitDuration(10, now),
		"should wait 1s for 10 tokens at rate 10/s",
	)

	// Need 20 tokens at rate 10/s = 2 second wait
	assert.Equal(
		t,
		2*time.Second,
		tb.waitDuration(20, now),
		"should wait 2s for 20 tokens at rate 10/s",
	)
}

func TestTxSubmissionRateLimiter_WaitDuration(t *testing.T) {
	rl := newTxSubmissionRateLimiter(10, 20)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rl.nowFunc = func() time.Time { return now }

	peer := testConnIdWithPort(4001)

	// Unknown peer returns zero wait
	assert.Equal(
		t,
		time.Duration(0),
		rl.WaitDuration(peer, 10),
		"unknown peer should return zero wait",
	)

	// Create peer and exhaust tokens
	assert.True(t, rl.Allow(peer, 20), "consume all tokens")

	// Should report wait duration
	assert.True(
		t,
		rl.WaitDuration(peer, 10) > 0,
		"should report positive wait when exhausted",
	)
}

func TestTxSubmissionRateLimiter_MultiplePeersIndependent(
	t *testing.T,
) {
	rl := newTxSubmissionRateLimiter(10, 20)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mu := sync.Mutex{}
	rl.nowFunc = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}

	// Create 5 peers
	peers := make([]ouroboros_conn.ConnectionId, 5)
	for i := range peers {
		peers[i] = testConnIdWithPort(5000 + i)
	}

	// Each peer should have their own independent bucket
	for i, peer := range peers {
		assert.True(
			t,
			rl.Allow(peer, 20),
			"peer %d should have full burst", i,
		)
		assert.False(
			t,
			rl.Allow(peer, 1),
			"peer %d should be rate limited", i,
		)
	}

	// Advance time and verify all peers recover independently
	mu.Lock()
	now = now.Add(1 * time.Second) // +10 tokens for each peer
	mu.Unlock()

	for i, peer := range peers {
		assert.True(
			t,
			rl.Allow(peer, 10),
			"peer %d should recover after refill", i,
		)
	}
}
