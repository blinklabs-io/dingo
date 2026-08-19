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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package apilistener

import (
	"context"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// --- helpers ------------------------------------------------------------

// portAccepts reports whether a TCP connection to addr succeeds.
func portAccepts(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// newListener returns a Listener named for tests, with a discarding logger.
func newListener() *Listener {
	return New("Test API", nil)
}

// publish registers a bare http.Server on addr, matching how each API package
// publishes the server it built.
func publish(
	l *Listener, addr string,
) (*http.Server, chan struct{}, error) {
	return l.Publish(func() *http.Server {
		return &http.Server{Addr: addr} //nolint:gosec // test server
	})
}

// startOnFreePort publishes and binds a server on a free loopback port,
// retrying on a lost race for the port, and returns the Listener with the
// address it bound. The caller owns shutdown.
func startOnFreePort(t *testing.T) (*Listener, string) {
	t.Helper()
	var lastErr error
	for range testutil.BindAttempts {
		addr := testutil.FreePort(t)
		l := newListener()
		srv, bindDone, err := publish(l, addr)
		require.NoError(t, err)
		if err := l.Bind(srv, bindDone, apiconfig.EffectiveTLS{}); err != nil {
			lastErr = err
			continue
		}
		return l, addr
	}
	t.Fatalf(
		"could not bind a free loopback port in %d attempts: %v",
		testutil.BindAttempts, lastErr,
	)
	return nil, ""
}

// stop runs the full Stop sequence an API server's Stop performs.
func stop(ctx context.Context, l *Listener) error {
	job, inFlight := l.Take()
	if job == nil {
		return l.AwaitTeardown(ctx, inFlight)
	}
	return l.Shutdown(ctx, job, Graceful)
}

// --- publication --------------------------------------------------------

// TestPublishRejectsASecondServer asserts a Listener holds one server at a
// time, which is what makes a second Start fail rather than strand the first
// server's socket with nothing left able to reach it.
func TestPublishRejectsASecondServer(t *testing.T) {
	l := newListener()
	_, _, err := publish(l, "127.0.0.1:0")
	require.NoError(t, err)

	_, _, err = publish(l, "127.0.0.1:0")
	require.ErrorContains(t, err, "already started")
}

// TestUnpublishOnlyClearsTheCurrentServer asserts a failed Start does not
// discard a server that an overlapping Stop or restart already replaced.
func TestUnpublishOnlyClearsTheCurrentServer(t *testing.T) {
	l := newListener()
	current, _, err := publish(l, "127.0.0.1:0")
	require.NoError(t, err)

	// A stale server from an earlier, already-detached Start.
	l.Unpublish(&http.Server{Addr: "127.0.0.1:0"}) //nolint:gosec // test server

	l.mu.Lock()
	defer l.mu.Unlock()
	require.Same(
		t, current, l.srv,
		"Unpublish must not clear a server it does not own",
	)
}

// TestUnpublishClearsTheBindChannelWithTheServer asserts the fields are
// cleared as a set. A cleared server paired with a surviving bind channel
// would leave the next Publish's Take handing out a stale channel.
func TestUnpublishClearsTheBindChannelWithTheServer(t *testing.T) {
	l := newListener()
	srv, _, err := publish(l, "127.0.0.1:0")
	require.NoError(t, err)

	l.Unpublish(srv)

	l.mu.Lock()
	defer l.mu.Unlock()
	require.Nil(t, l.srv)
	require.Nil(t, l.bindDone)
	require.Nil(t, l.ln)
}

// --- the defect ---------------------------------------------------------

// TestShutdownClosesAListenerServeNeverRegistered is the defect this package
// exists for, pinned deterministically rather than by racing a real bind.
//
// http.Server.Shutdown closes only the listeners Serve has registered. Bind
// hands the socket to Serve in a goroutine it does not wait for, so a Stop
// landing in that window finds a server with nothing registered -- and
// Shutdown alone leaves the port bound after Stop returns.
func TestShutdownClosesAListenerServeNeverRegistered(t *testing.T) {
	l := newListener()
	addr := testutil.FreePort(t)
	_, bindDone, err := publish(l, addr)
	require.NoError(t, err)

	// Stands in for Bind having opened and published the socket, with Serve
	// not yet registered on it.
	ln, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	l.mu.Lock()
	l.ln = ln
	l.mu.Unlock()
	close(bindDone)

	require.NoError(t, stop(t.Context(), l))

	require.False(
		t, portAccepts(addr),
		"Stop must release a socket Serve never registered",
	)
}

// TestStopReleasesPortBeforeServeRegisters is the same defect through the real
// bind path, where the window is narrow and the failure probabilistic. It
// complements the deterministic test above rather than replacing it.
func TestStopReleasesPortBeforeServeRegisters(t *testing.T) {
	for i := range 100 {
		l, addr := startOnFreePort(t)

		require.NoError(t, stop(t.Context(), l))

		require.False(
			t, portAccepts(addr),
			"listener still accepting when Stop returned "+
				"(iteration %d)", i,
		)
	}
}

// --- bind ---------------------------------------------------------------

// TestBindReleasesListenerWhenServerAlreadyDetached covers the window between
// Publish and Bind recording the listener. A Stop landing inside it detaches
// the server, so Take later hands back a nil server and shutdownServer never
// runs -- meaning Bind must not leave its own socket bound, and must not
// overwrite the listener of whichever server is current now.
func TestBindReleasesListenerWhenServerAlreadyDetached(t *testing.T) {
	l := newListener()
	addr := testutil.FreePort(t)

	// Stands in for the server a concurrent restart already published; it must
	// survive this call untouched.
	currentListener, err := net.Listen("tcp", testutil.FreePort(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = currentListener.Close() })
	current := &http.Server{ //nolint:gosec // test server
		Addr: currentListener.Addr().String(),
	}
	l.mu.Lock()
	l.srv = current
	l.ln = currentListener
	l.mu.Unlock()

	// The detached server this Bind call is bringing up.
	detached := &http.Server{Addr: addr} //nolint:gosec // test server
	bindDone := make(chan struct{})
	require.NoError(
		t, l.Bind(detached, bindDone, apiconfig.EffectiveTLS{}),
	)
	testutil.RequireReceive(
		t, bindDone, time.Second,
		"Bind must signal that the bind settled",
	)

	require.False(
		t, portAccepts(addr),
		"Bind must not leave a stopped server's port bound",
	)
	l.mu.Lock()
	defer l.mu.Unlock()
	require.Same(
		t, current, l.srv,
		"Bind must not disturb the current server",
	)
	require.Same(
		t, currentListener, l.ln,
		"Bind must not overwrite the current server's listener",
	)
}

// TestBindSignalsBindDoneOnKeypairFailure asserts the bind channel is closed
// even when Bind fails before it ever reaches net.Listen. A Stop waiting on
// that channel would otherwise block until its context expired, waiting on a
// bind that had already given up.
func TestBindSignalsBindDoneOnKeypairFailure(t *testing.T) {
	l := newListener()
	srv, bindDone, err := publish(l, testutil.FreePort(t))
	require.NoError(t, err)

	missing := filepath.Join(t.TempDir(), "absent")
	err = l.Bind(srv, bindDone, apiconfig.EffectiveTLS{
		Enabled:      true,
		CertFilePath: missing,
		KeyFilePath:  missing,
	})

	require.ErrorContains(t, err, "failed to load TLS keypair")
	testutil.RequireReceive(
		t, bindDone, time.Second,
		"a failed keypair load must still signal the bind settled",
	)
}

// TestBindSignalsBindDoneOnListenFailure asserts the same for a lost port
// race, which is the failure an operator actually hits.
func TestBindSignalsBindDoneOnListenFailure(t *testing.T) {
	occupied, err := net.Listen("tcp", testutil.FreePort(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = occupied.Close() })

	l := newListener()
	srv, bindDone, err := publish(l, occupied.Addr().String())
	require.NoError(t, err)

	err = l.Bind(srv, bindDone, apiconfig.EffectiveTLS{})

	require.ErrorContains(t, err, "failed to listen for Test API server")
	testutil.RequireReceive(
		t, bindDone, time.Second,
		"a failed bind must still signal the bind settled",
	)
}

// --- shutdown coordination ----------------------------------------------

// TestShutdownWaitsForAnInFlightBind asserts Stop does not report the server
// down while a Bind call is still between net.Listen and releasing its socket.
// Detaching the server is what makes that bind close its own listener, so
// without waiting here Stop could return -- and a caller could rebind the same
// port -- while the old socket was still open.
func TestShutdownWaitsForAnInFlightBind(t *testing.T) {
	l := newListener()

	// Stands in for a published server whose bind has not finished yet.
	bindDone := make(chan struct{})
	l.mu.Lock()
	l.srv = &http.Server{
		Addr: testutil.FreePort(t),
	} //nolint:gosec // test server
	l.bindDone = bindDone
	l.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	require.ErrorIs(
		t, stop(ctx, l), context.DeadlineExceeded,
		"Stop must wait for the in-flight bind rather than returning",
	)

	// Once the bind settles, Stop completes.
	close(bindDone)
	require.NoError(t, stop(t.Context(), l))
}

// TestShutdownTearsDownEvenWhenTheBindWaitTimesOut asserts a Stop whose
// context expires mid-wait still releases the socket it detached. The detach is
// what makes Stop the only remaining reference to that listener, so returning
// the wait error without tearing down would leave the port bound with nothing
// left able to close it.
func TestShutdownTearsDownEvenWhenTheBindWaitTimesOut(t *testing.T) {
	l := newListener()
	addr := testutil.FreePort(t)
	ln, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	// A published listener plus a bind that never settles.
	l.mu.Lock()
	l.srv = &http.Server{Addr: addr} //nolint:gosec // test server
	l.ln = ln
	l.bindDone = make(chan struct{})
	l.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, stop(ctx, l), context.DeadlineExceeded)

	require.False(
		t, portAccepts(addr),
		"Stop must release the socket it detached even when the bind "+
			"wait times out",
	)
}

// TestStopWaitsForATeardownItLost asserts the loser of the Take race does not
// report the server down early. A server's Stop and its context monitor both
// detach; only one wins, and a Stop that returned nil while the winner was
// still releasing the port would let an immediate restart fail to bind.
func TestStopWaitsForATeardownItLost(t *testing.T) {
	l := newListener()

	// Stands in for another caller having already detached the server and
	// still being mid-teardown.
	teardown := make(chan struct{})
	l.mu.Lock()
	l.srv = nil
	l.teardown = teardown
	l.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	require.ErrorIs(
		t, stop(ctx, l), context.DeadlineExceeded,
		"Stop must wait for the teardown it lost rather than returning nil",
	)

	close(teardown)
	require.NoError(t, stop(t.Context(), l))
}

// TestAwaitTeardownPrefersACompletedTeardown asserts a finished teardown is
// never reported as a timeout. When the completion channel and the context are
// both ready, select picks at random, so the loop is what makes the absence of
// a recheck fail rather than flake.
func TestAwaitTeardownPrefersACompletedTeardown(t *testing.T) {
	l := newListener()
	done := make(chan struct{})
	close(done)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for i := range 200 {
		require.NoError(
			t, l.AwaitTeardown(ctx, done),
			"a completed teardown must not be reported as a timeout "+
				"(iteration %d)", i,
		)
	}
}

// TestTimedOutTeardownDoesNotSignalCompletionEarly asserts a Stop whose bind
// wait times out does not mark the teardown complete. Bind still owns a socket
// that Stop cannot close, so a second caller waiting on the teardown has to
// keep waiting rather than read it as "the port is free".
func TestTimedOutTeardownDoesNotSignalCompletionEarly(t *testing.T) {
	l := newListener()
	bindDone := make(chan struct{})

	l.mu.Lock()
	l.srv = &http.Server{
		Addr: testutil.FreePort(t),
	} //nolint:gosec // test server
	l.bindDone = bindDone
	l.mu.Unlock()

	// First caller detaches and times out waiting for the bind.
	stopCtx, cancelStop := context.WithTimeout(
		context.Background(), 100*time.Millisecond,
	)
	defer cancelStop()
	require.ErrorIs(t, stop(stopCtx, l), context.DeadlineExceeded)

	// Second caller lost the detach and must not be told the teardown is done.
	loserCtx, cancelLoser := context.WithTimeout(
		context.Background(), 100*time.Millisecond,
	)
	defer cancelLoser()
	require.ErrorIs(
		t, stop(loserCtx, l), context.DeadlineExceeded,
		"a teardown blocked on an in-flight bind must not report completion",
	)

	// Once the bind settles the teardown is genuinely complete.
	close(bindDone)
	require.NoError(t, stop(t.Context(), l))
}

// TestStopOnAnUnstartedListenerIsClean asserts Stop before Start is not an
// error. The node stops capabilities it may never have started.
func TestStopOnAnUnstartedListenerIsClean(t *testing.T) {
	require.NoError(t, stop(t.Context(), newListener()))
}

// --- the contract callers rely on ---------------------------------------

// TestListenerIsReusableAfterShutdown asserts a completed Stop leaves the
// Listener able to bring another server up on the same address. This is what a
// capability restart does -- see reinitializeAPIServers in node_lifecycle.go --
// and it is the reason releasing the port has to be part of what Stop waits
// for rather than something Serve gets around to later.
func TestListenerIsReusableAfterShutdown(t *testing.T) {
	l, addr := startOnFreePort(t)
	require.NoError(t, stop(t.Context(), l))

	srv, bindDone, err := publish(l, addr)
	require.NoError(t, err)
	require.NoError(
		t, l.Bind(srv, bindDone, apiconfig.EffectiveTLS{}),
		"rebinding after a clean Stop must succeed",
	)
	require.NoError(t, stop(t.Context(), l))
}

// TestConcurrentBindStopNeverLeavesThePortBound hammers the interleavings the
// individual tests each pin one of: a bind racing Stop, two Stops racing each
// other, and a rebind on the same address immediately after.
//
// The invariant is the one every caller relies on: once Stop returns without an
// error, the address is free, so the next bind on it must succeed.
//
// What this does NOT cover: the paths that need a bind still in flight when a
// wait expires. A real bind settles far too quickly for that, so a stalled bind
// has to be constructed. Those live in
// TestShutdownTearsDownEvenWhenTheBindWaitTimesOut,
// TestStopWaitsForATeardownItLost, and
// TestTimedOutTeardownDoesNotSignalCompletionEarly. Do not read a pass here as
// covering them.
func TestConcurrentBindStopNeverLeavesThePortBound(t *testing.T) {
	addr := testutil.FreePort(t)

	for i := range 60 {
		l := newListener()

		// Three-way contention on purpose: the bind, and two Stops. Two Stops
		// matter -- one of them loses Take and has to wait on the winner's
		// teardown, which is the path where a premature completion signal
		// turns into a false "the port is free".
		var wg sync.WaitGroup
		stopErrs := make([]error, 2)
		wg.Add(3)
		go func() {
			defer wg.Done()
			srv, bindDone, err := publish(l, addr)
			if err != nil {
				return
			}
			_ = l.Bind(srv, bindDone, apiconfig.EffectiveTLS{})
		}()
		for slot := range stopErrs {
			go func() {
				defer wg.Done()
				stopErrs[slot] = stop(t.Context(), l)
			}()
		}
		wg.Wait()

		// Every Stop that returned nil made the same promise, so the strictest
		// reading applies: if any of them reported clean, the port must be free.
		if stopErrs[0] != nil && stopErrs[1] != nil {
			// Both reported a timeout, which is honest: the callers were told
			// the port may still be held, so neither is licensed to rebind.
			continue
		}
		require.NoError(
			t, stop(t.Context(), l),
			"a second Stop must stay clean (iteration %d)", i,
		)
		require.False(
			t, portAccepts(addr),
			"Stop returned nil but the port is still accepting "+
				"(iteration %d)", i,
		)

		// The contract Stop's nil return promises: the address is rebindable.
		next := newListener()
		nextSrv, bindDone, err := publish(next, addr)
		require.NoError(t, err)
		require.NoError(
			t, next.Bind(nextSrv, bindDone, apiconfig.EffectiveTLS{}),
			"rebinding after a clean Stop must succeed (iteration %d)", i,
		)
		require.NoError(t, stop(t.Context(), next))
	}
}
