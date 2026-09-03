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
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol/chainsync"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
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

// TestWatchBlocks_CloseStopsPromptlyAgainstUnresponsivePeer covers a peer
// that completes the real NtC handshake and then stalls before replying to
// the first ChainSync request (GetCurrentTip's FindIntersect) -- a dead or
// hung node, as opposed to TestWatchBlocks_CloseStopsPromptly's "nothing is
// listening at all" case, and distinct from a peer that never completes
// the handshake at all (that phase is bounded by dialTimeout, not this
// path -- see TestDial_HandshakeStallIsBoundedByDialTimeout's watch.go
// counterpart, watchSession's own dialCtx-scoped closer). GetCurrentTip and
// Sync are synchronous protocol calls with no per-call timeout of their
// own: each blocks until the peer replies or the connection is closed out
// from under it. Without watchSession closing the connection the instant
// its context is cancelled, Close would block for as long as the peer
// keeps the socket open (in production, indefinitely), rather than
// returning promptly. Using a fake peer that never completes the
// handshake at all would only exercise the earlier dial-time closer, not
// this one -- a regression that removed this later closer would still
// pass such a test.
func TestWatchBlocks_CloseStopsPromptlyAgainstUnresponsivePeer(t *testing.T) {
	const magic = 42
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	findIntersectCalled := make(chan struct{})
	stall := make(chan struct{}) // never closed: FindIntersect never replies
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		oconn, err := ouroboros.New(
			ouroboros.WithConnection(conn),
			ouroboros.WithServer(true),
			ouroboros.WithNetworkMagic(magic),
			ouroboros.WithChainSyncConfig(chainsync.NewConfig(
				chainsync.WithFindIntersectFunc(
					func(
						_ chainsync.CallbackContext, _ []pcommon.Point,
					) (pcommon.Point, chainsync.Tip, error) {
						close(findIntersectCalled)
						<-stall
						return pcommon.Point{}, chainsync.Tip{}, nil
					},
				),
			)),
		)
		if err != nil {
			return
		}
		defer oconn.Close() //nolint:errcheck
		<-oconn.ErrorChan()
	}()

	w := WatchBlocks(context.Background(), listener.Addr().String(), magic, nil)

	// Wait for GetCurrentTip's FindIntersect to actually reach the server,
	// not just for the handshake to finish: cancelling immediately after
	// the handshake races the dial-time closer (stopDialCancel, scoped to
	// dialCtx) against ouroboros.New's own return, since dialCtx is a
	// child of this same ctx -- which can tear down the raw connection
	// through that earlier closer instead of the later one (stopOnCancel)
	// this test exists to cover, making the test pass even with
	// stopOnCancel removed. Waiting for FindIntersect guarantees
	// stopDialCancel has already deregistered by the time Close runs.
	select {
	case <-findIntersectCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("server never observed the watcher's GetCurrentTip request")
	}

	done := make(chan struct{})
	go func() {
		w.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal(
			"Watcher.Close did not return promptly against a peer that " +
				"completed the handshake and then never replied to GetCurrentTip",
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

// TestWatchBlocks_StalledHandshakeTriggersReconnectWithinDialTimeout covers
// a peer that accepts the connection and then never completes the NtC
// handshake, with no external cancellation (matching a real Watcher, whose
// ctx normally only cancels on process shutdown). watchSession's own
// dialCtx-scoped closer must still bound this phase by dialTimeout, the
// same as Dial's identical fix (TestDial_HandshakeStallIsBoundedByDialTimeout):
// regression test for a bug where this closer was registered against the
// long-lived watcher ctx instead, leaving a stalled handshake unbounded
// except by the watcher's own eventual shutdown.
func TestWatchBlocks_StalledHandshakeTriggersReconnectWithinDialTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		// Accept and hold the connection open without ever writing to it,
		// so ouroboros.New blocks in the handshake indefinitely unless
		// watchSession's dialCtx-scoped closer bounds it.
		t.Cleanup(func() { _ = conn.Close() })
	}()

	var mu sync.Mutex
	var logs []string
	logf := func(format string, args ...any) {
		mu.Lock()
		logs = append(logs, fmt.Sprintf(format, args...))
		mu.Unlock()
	}

	w := WatchBlocks(context.Background(), listener.Addr().String(), 42, logf)
	defer w.Close()

	testutil.WaitForCondition(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(logs) >= 1
	}, dialTimeout+5*time.Second, "watcher must log a reconnect attempt within dialTimeout+margin against a stalled handshake, with no external cancellation")
}
