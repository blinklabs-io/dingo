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

package utxorpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"connectrpc.com/connect"
)

// Defaults for the FollowTip historical replay bounds on UtxorpcConfig.
const (
	DefaultMaxConcurrentReplays     = 4
	DefaultMaxReplaysPerClient      = 1
	DefaultReplayBlocksPerSecond    = 1000
	DefaultMaxReplayBlocksPerClient = 20_000_000
	DefaultReplayBudgetWindow       = 24 * time.Hour
)

// replayLimiter bounds the work FollowTip spends serving blocks that are
// already behind the tip. Live tip following never touches it.
//
// Three independent limits apply: a cap on streams that are replaying at once,
// process-wide and per client (refused, never queued), a per-client block rate
// that slows replay down without ending it, and a per-client block count per
// window that ends a replay once spent. A client is the remote IPv4 address
// or IPv6 /64 prefix (see clientKey), so clients behind one proxy share a
// budget, as with streamLimiter. The per-client slot cap is what stops one
// client from holding every process-wide slot: the stream cap admits many
// streams per client, and a stream holds its slot while it waits on pacing or
// on a client that stopped reading.
type replayLimiter struct {
	maxSlots          int
	maxSlotsPerClient int
	active            int
	activeByClient    map[string]int
	rate              float64
	burst             float64
	budget            int64
	window            time.Duration
	mu                sync.Mutex
	clients           map[string]*replayClient
	nextSweep         time.Time

	// now and sleep are seams for tests; production uses the wall clock.
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error
}

// replayClient is one client's rate bucket and window block count. tokens may
// go negative: a charge reserves its token immediately and the caller waits
// out the debt, so concurrent streams of one client are serialized fairly.
type replayClient struct {
	tokens      float64
	last        time.Time
	windowStart time.Time
	used        int64
}

func newReplayLimiter(
	maxConcurrent int,
	maxPerClient int,
	blocksPerSecond int,
	budget int64,
	window time.Duration,
) *replayLimiter {
	return &replayLimiter{
		maxSlots:          maxConcurrent,
		maxSlotsPerClient: maxPerClient,
		activeByClient:    make(map[string]int),
		rate:              float64(blocksPerSecond),
		burst:             float64(max(blocksPerSecond, 1)),
		budget:            budget,
		window:            window,
		clients:           make(map[string]*replayClient),
		now:               time.Now,
		sleep:             sleepContext,
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// acquireSlot reserves one of the concurrent replay slots for the client at
// peerAddr without waiting.
func (l *replayLimiter) acquireSlot(peerAddr string) (func(), error) {
	key := clientKey(peerAddr)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active >= l.maxSlots {
		return nil, connect.NewError(
			connect.CodeResourceExhausted,
			fmt.Errorf(
				"too many concurrent historical replays: limit is %d",
				l.maxSlots,
			),
		)
	}
	if l.activeByClient[key] >= l.maxSlotsPerClient {
		return nil, connect.NewError(
			connect.CodeResourceExhausted,
			fmt.Errorf(
				"per-client concurrent historical replay limit of %d reached",
				l.maxSlotsPerClient,
			),
		)
	}
	l.active++
	l.activeByClient[key]++
	return sync.OnceFunc(func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.active--
		if l.activeByClient[key]--; l.activeByClient[key] == 0 {
			delete(l.activeByClient, key)
		}
	}), nil
}

// charge accounts one replayed block to the client at peerAddr. It returns
// ResourceExhausted once the client has spent its window budget, and
// otherwise waits as long as the client's block rate requires.
func (l *replayLimiter) charge(ctx context.Context, peerAddr string) error {
	key := clientKey(peerAddr)
	l.mu.Lock()
	now := l.now()
	l.sweepLocked(now)
	c := l.clients[key]
	if c == nil {
		c = &replayClient{tokens: l.burst, last: now, windowStart: now}
		l.clients[key] = c
	}
	if now.Sub(c.windowStart) >= l.window {
		c.windowStart = now
		c.used = 0
	}
	if c.used >= l.budget {
		l.mu.Unlock()
		return connect.NewError(
			connect.CodeResourceExhausted,
			errors.New("client historical replay block budget exhausted"),
		)
	}
	c.used++
	if elapsed := now.Sub(c.last); elapsed > 0 {
		c.tokens = min(l.burst, c.tokens+elapsed.Seconds()*l.rate)
	}
	c.last = now
	c.tokens--
	var wait time.Duration
	if c.tokens < 0 {
		wait = time.Duration(-c.tokens / l.rate * float64(time.Second))
	}
	l.mu.Unlock()
	if wait <= 0 {
		return nil
	}
	return l.sleep(ctx, wait)
}

// sweepLocked drops clients whose window has lapsed and whose rate bucket has
// refilled, so the map does not grow with every host that ever replayed.
func (l *replayLimiter) sweepLocked(now time.Time) {
	if now.Before(l.nextSweep) {
		return
	}
	l.nextSweep = now.Add(l.window)
	for key, c := range l.clients {
		if now.Sub(c.windowStart) >= l.window &&
			c.tokens+now.Sub(c.last).Seconds()*l.rate >= l.burst {
			delete(l.clients, key)
		}
	}
}
