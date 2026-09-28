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

import "time"

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
// and one token bucket owned by the ChainSync protocol instance.
type chainsyncFindIntersectRateLimiter struct {
	bucket  *tokenBucket
	nowFunc func() time.Time
}

// newChainsyncFindIntersectRateLimiter creates a new per-peer FindIntersect
// work-budget limiter. rate is the sustained points-per-second budget per
// peer; burst is the maximum immediately available budget.
func newChainsyncFindIntersectRateLimiter(
	rate float64,
	burst float64,
) *chainsyncFindIntersectRateLimiter {
	rl := &chainsyncFindIntersectRateLimiter{
		nowFunc: time.Now,
	}
	// Start with a zero timestamp so the first Allow call initializes the
	// bucket clock from nowFunc. This keeps the clock injectable for tests.
	rl.bucket = newTokenBucket(rate, burst, time.Time{})
	return rl
}

// Allow reports whether n points of FindIntersect lookup work from the given
// peer are within budget, consuming that budget if so.
func (rl *chainsyncFindIntersectRateLimiter) Allow(
	n int,
) bool {
	return rl.bucket.allow(float64(n), rl.nowFunc())
}
