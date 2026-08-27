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

// chainsyncFindIntersectRateLimiter bounds the database lookup work a single
// ChainSync peer connection can trigger via repeated FindIntersect requests.
// Unlike the point-count cap in chainsyncServerFindIntersect, which only
// rejects a single oversized request, this tracks cumulative work across a
// connection's lifetime: a peer that repeatedly resends smaller, in-bounds
// point lists is bounded the same way as one that sends occasional large
// ones. Cost is charged per point actually looked up (post-deduplication),
// so resending duplicate points cannot inflate the charge.
//
// Reuses the tokenBucket implementation from txsubmission_rate_limiter.go
// and the sync.Map-per-peer shape for lock-free reads on the hot path.
type chainsyncFindIntersectRateLimiter struct {
	peers   sync.Map // map[string]*tokenBucket keyed by connIdKey
	rate    float64  // points per second per peer
	burst   float64  // max burst per peer
	nowFunc func() time.Time
}

// newChainsyncFindIntersectRateLimiter creates a new per-peer FindIntersect
// work-budget limiter. rate is the sustained points-per-second budget per
// peer; burst is the maximum immediately available budget.
func newChainsyncFindIntersectRateLimiter(
	rate float64,
	burst float64,
) *chainsyncFindIntersectRateLimiter {
	return &chainsyncFindIntersectRateLimiter{
		rate:    rate,
		burst:   burst,
		nowFunc: time.Now,
	}
}

// Allow reports whether n points of FindIntersect lookup work from the given
// peer are within budget, consuming that budget if so.
func (rl *chainsyncFindIntersectRateLimiter) Allow(
	connId ouroboros.ConnectionId,
	n int,
) bool {
	key := connIdKey(connId)
	val, ok := rl.peers.Load(key)
	if !ok {
		bucket := newTokenBucket(rl.rate, rl.burst, rl.nowFunc())
		val, _ = rl.peers.LoadOrStore(key, bucket)
	}
	return val.(*tokenBucket).allow(float64(n), rl.nowFunc())
}

// RemovePeer removes rate limiting state for the given connection. This
// should be called when a connection is closed.
func (rl *chainsyncFindIntersectRateLimiter) RemovePeer(
	connId ouroboros.ConnectionId,
) {
	rl.peers.Delete(connIdKey(connId))
}
