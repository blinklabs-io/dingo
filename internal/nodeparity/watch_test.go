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

package nodeparity

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unreachableAddr binds a TCP listener on an OS-assigned free port and
// immediately closes it, returning that address. Nothing is listening
// there afterward, so dialing it fails fast and deterministically --
// unlike a hardcoded port number, this can never collide with something
// already running on the test host.
func unreachableAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// TestNextBackoff_DoublesUntilCap covers the reconnect delay's growth for a
// session that never gets established (a node that will not talk to us at
// all): each failure must double the previous delay, and the delay must
// never exceed watcherMaxBackoff no matter how many failures accumulate.
func TestNextBackoff_DoublesUntilCap(t *testing.T) {
	backoff := watcherMinBackoff
	seen := []time.Duration{backoff}
	for range 10 {
		backoff = nextBackoff(backoff, false)
		seen = append(seen, backoff)
	}
	for i := 1; i < len(seen); i++ {
		prev, cur := seen[i-1], seen[i]
		wantDoubled := min(2*prev, watcherMaxBackoff)
		assert.Equal(t, wantDoubled, cur, "step %d", i)
		assert.LessOrEqual(t, cur, watcherMaxBackoff)
	}
	assert.Equal(
		t, watcherMaxBackoff, seen[len(seen)-1],
		"backoff must have reached the cap well within 10 doublings",
	)
}

// TestNextBackoff_ResetsOnEstablished covers the other half of the
// reconnect policy: a session that got as far as following the chain and
// then dropped resets to the minimum delay on its next attempt, regardless
// of how large the backoff had grown -- that failure mode looks like a
// node restart, not a node that refuses to talk to us, so it should be
// retried quickly.
func TestNextBackoff_ResetsOnEstablished(t *testing.T) {
	grown := nextBackoff(
		nextBackoff(nextBackoff(watcherMinBackoff, false), false),
		false,
	)
	require.Greater(
		t,
		grown,
		watcherMinBackoff,
		"precondition: backoff must have grown",
	)

	reset := nextBackoff(grown, true)
	assert.Equal(t, watcherMinBackoff, reset)
}

// TestBlockEventSignal_CoalescesBursts covers the coalescing behavior
// newBlockEventSignal exists for: several notify calls in a row, with
// nothing draining the channel in between, must leave exactly one pending
// event -- not block, not panic, and not queue up a backlog that would
// make a slow consumer process stale bursts one at a time.
func TestBlockEventSignal_CoalescesBursts(t *testing.T) {
	events, notify := newBlockEventSignal()
	for range 5 {
		notify()
	}
	assert.Len(
		t,
		events,
		1,
		"a burst of notifies must coalesce to one pending event",
	)
}

// TestBlockEventSignal_DeliversAgainAfterDrain covers the other half of
// the coalescing contract: coalescing must not turn into permanently
// dropping events. Once a caller drains the pending event, the next
// notify must deliver a fresh one.
func TestBlockEventSignal_DeliversAgainAfterDrain(t *testing.T) {
	events, notify := newBlockEventSignal()
	notify()
	<-events // drain
	assert.Empty(t, events, "channel must be empty right after drain")

	notify()
	assert.Len(t, events, 1, "a notify after drain must deliver a new event")
}

// TestBlockEventSignal_NotifyNeverBlocks covers the property that makes
// this safe to call from a ChainSync callback: notify must never block the
// caller, even under a sustained flood with nobody ever reading Events.
// A ChainSync callback that blocked here would stall the whole protocol
// session, not just this watcher.
func TestBlockEventSignal_NotifyNeverBlocks(t *testing.T) {
	_, notify := newBlockEventSignal()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 1000 {
			notify()
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("notify blocked under a flood with no reader draining Events")
	}
}

// TestWatchBlocks_CloseStopsPromptly covers Watcher lifecycle management
// against a node that will never accept a connection: WatchBlocks starts a
// background reconnect loop immediately, and Close must cancel it and wait
// for that goroutine to actually exit, rather than returning while it is
// still running (which would leak the goroutine) or hanging forever
// waiting on a connection that will never succeed.
func TestWatchBlocks_CloseStopsPromptly(t *testing.T) {
	addr := unreachableAddr(t)
	w := WatchBlocks(context.Background(), addr, 2, nil)

	done := make(chan struct{})
	go func() {
		w.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal(
			"Watcher.Close did not return promptly against an unreachable address",
		)
	}
}

// TestWatchBlocks_RetriesOnUnreachableAddr covers the actual retry
// behavior end to end (short of a real ChainSync server, which would
// require new shared mock infrastructure this package does not add
// locally): pointed at an address nothing is listening on, the watcher
// must keep attempting to reconnect on its own, logging each attempt,
// rather than giving up after the first failure.
func TestWatchBlocks_RetriesOnUnreachableAddr(t *testing.T) {
	addr := unreachableAddr(t)

	var mu sync.Mutex
	attempts := 0
	logf := func(string, ...any) {
		mu.Lock()
		attempts++
		mu.Unlock()
	}

	w := WatchBlocks(context.Background(), addr, 2, logf)
	defer w.Close()

	testutil.WaitForCondition(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts >= 2
	}, 3*time.Second, "watcher must keep retrying a connection that never succeeds")
}
