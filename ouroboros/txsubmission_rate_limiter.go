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
	"sync"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
)

// DefaultMaxTxSubmissionsPerSecond is the default maximum number of
// transaction submissions accepted per peer per second.
const DefaultMaxTxSubmissionsPerSecond = 30

// tokenBucket implements a simple token bucket rate limiter.
// Tokens are replenished at a fixed rate up to a maximum burst.
type tokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64   // tokens per second
	lastRefill time.Time // last time tokens were refilled
}

// newTokenBucket creates a new token bucket with the given rate
// (tokens per second) and burst size.
func newTokenBucket(
	rate float64,
	burst float64,
	now time.Time,
) *tokenBucket {
	return &tokenBucket{
		tokens:     burst,
		maxTokens:  burst,
		refillRate: rate,
		lastRefill: now,
	}
}

// refill replenishes tokens based on elapsed time. Must be called
// with tb.mu held.
func (tb *tokenBucket) refill(now time.Time) {
	elapsed := now.Sub(tb.lastRefill).Seconds()
	if elapsed > 0 {
		tb.tokens += elapsed * tb.refillRate
		if tb.tokens > tb.maxTokens {
			tb.tokens = tb.maxTokens
		}
		tb.lastRefill = now
	}
}

// allow checks whether n tokens can be consumed. It refills tokens
// based on elapsed time and then attempts to consume n tokens.
// Returns true if the tokens were consumed, false if rate limited.
func (tb *tokenBucket) allow(n float64, now time.Time) bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill(now)

	// Check if enough tokens are available
	if tb.tokens < n {
		return false
	}
	tb.tokens -= n
	return true
}

// waitDuration returns how long to wait before n tokens will be
// available. Returns 0 if tokens are already available.
func (tb *tokenBucket) waitDuration(
	n float64,
	now time.Time,
) time.Duration {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill(now)

	if tb.tokens >= n {
		return 0
	}
	deficit := n - tb.tokens
	seconds := deficit / tb.refillRate
	return time.Duration(seconds * float64(time.Second))
}

// txSubmissionRateLimiter manages per-peer rate limiting for
// the TxSubmission mini-protocol.
type txSubmissionRateLimiter struct {
	mu      sync.Mutex
	peers   map[ouroboros.ConnectionId]*tokenBucket
	rate    float64 // tokens per second per peer
	burst   float64 // max burst per peer
	nowFunc func() time.Time
}

// newTxSubmissionRateLimiter creates a new per-peer rate limiter.
// rate is the sustained transactions per second allowed per peer.
// burst is the maximum burst size (typically 2x-3x the rate to
// allow small batches).
func newTxSubmissionRateLimiter(
	rate float64,
	burst float64,
) *txSubmissionRateLimiter {
	return &txSubmissionRateLimiter{
		peers:   make(map[ouroboros.ConnectionId]*tokenBucket),
		rate:    rate,
		burst:   burst,
		nowFunc: time.Now,
	}
}

// Allow checks whether n transaction submissions from the given
// peer are allowed. Returns true if the submissions are within
// the rate limit, false if they should be rejected.
func (rl *txSubmissionRateLimiter) Allow(
	connId ouroboros.ConnectionId,
	n int,
) bool {
	rl.mu.Lock()
	bucket, ok := rl.peers[connId]
	if !ok {
		bucket = newTokenBucket(rl.rate, rl.burst, rl.nowFunc())
		rl.peers[connId] = bucket
	}
	rl.mu.Unlock()

	return bucket.allow(float64(n), rl.nowFunc())
}

// WaitDuration returns how long to wait before n transactions from
// the given peer will be allowed.
func (rl *txSubmissionRateLimiter) WaitDuration(
	connId ouroboros.ConnectionId,
	n int,
) time.Duration {
	rl.mu.Lock()
	bucket, ok := rl.peers[connId]
	rl.mu.Unlock()

	if !ok {
		return 0
	}
	return bucket.waitDuration(float64(n), rl.nowFunc())
}

// RemovePeer removes rate limiting state for the given connection.
// This should be called when a connection is closed.
func (rl *txSubmissionRateLimiter) RemovePeer(
	connId ouroboros.ConnectionId,
) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	delete(rl.peers, connId)
}
