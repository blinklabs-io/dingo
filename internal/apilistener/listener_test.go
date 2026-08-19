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

// publishBound is publish for a server that stands in for one already brought
// up: it marks the bind settled, so a Stop is not left waiting on a bind that
// will never happen. Tests that want a bind still in flight set bindDone
// themselves.
func publishBound(
	l *Listener, addr string,
) (*http.Server, error) {
	srv, bindDone, err := publish(l, addr)
	if err != nil {
		return nil, err
	}
	close(bindDone)
	return srv, nil
}

// stopNow runs the Stop sequence under a bounded context, so a hang in the
// teardown path fails the test instead of stalling the suite until go test's
// own timeout fires.
func stopNow(t *testing.T, l *Listener) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	return stop(ctx, l)
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
		if _, err := l.Bind(
			srv, bindDone, apiconfig.EffectiveTLS{},
		); err != nil {
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

	require.NoError(t, stopNow(t, l))

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

		require.NoError(t, stopNow(t, l))

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
	served, err := l.Bind(detached, bindDone, apiconfig.EffectiveTLS{})
	require.NoError(t, err)
	require.False(t, served)
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
	_, err = l.Bind(srv, bindDone, apiconfig.EffectiveTLS{
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

	_, err = l.Bind(srv, bindDone, apiconfig.EffectiveTLS{})

	require.ErrorContains(t, err, "failed to listen for Test API server")
	testutil.RequireReceive(
		t, bindDone, time.Second,
		"a failed bind must still signal the bind settled",
	)
}

// TestTakeIfIgnoresAServerItDoesNotOwn asserts a context monitor cannot tear
// down a server its own Start never published. Start's monitor outlives the
// server it was launched for -- it sits on ctx.Done() until the caller's
// context ends, which may be long after that server was stopped and a restart
// published another one on the same Listener. An unconditional detach there
// would shut the replacement down.
func TestTakeIfIgnoresAServerItDoesNotOwn(t *testing.T) {
	l := newListener()
	first, err := publishBound(l, "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, stopNow(t, l))

	// The restart, on the same Listener.
	second, err := publishBound(l, "127.0.0.1:0")
	require.NoError(t, err)

	job, inFlight := l.TakeIf(first)

	require.Nil(t, job, "the first server's monitor must not detach the second")
	require.Nil(
		t,
		inFlight,
		"a teardown belonging to another server is not this caller's to wait on",
	)
	require.Same(
		t, second, l.Server(),
		"the replacement must still be published",
	)
}

// TestTakeIfDetachesItsOwnServer asserts the identity check does not defeat the
// case it exists to serve: a monitor whose server is still the current one
// still tears it down.
func TestTakeIfDetachesItsOwnServer(t *testing.T) {
	l := newListener()
	srv, err := publishBound(l, "127.0.0.1:0")
	require.NoError(t, err)

	job, _ := l.TakeIf(srv)

	require.NotNil(t, job, "a monitor must detach the server it published")
	require.Nil(t, l.Server())
}

// TestTakeIfHandsBackNoTeardownWhenItsServerIsGone covers the case where the
// monitor's server has been detached but its teardown is still running, so
// l.srv is nil rather than pointing at a replacement. The identity check has
// to come first: a caller landing here must be told there is nothing of its own
// left, not handed a teardown that belongs to whoever detached it. Waiting on
// that would block a monitor on an unrelated shutdown.
func TestTakeIfHandsBackNoTeardownWhenItsServerIsGone(t *testing.T) {
	l := newListener()
	srv, err := publishBound(l, "127.0.0.1:0")
	require.NoError(t, err)

	// Another caller detached it and is still tearing it down.
	winner, _ := l.Take()
	require.NotNil(t, winner)

	job, inFlight := l.TakeIf(srv)

	require.Nil(t, job)
	require.Nil(
		t, inFlight,
		"a monitor whose server is gone has nothing of its own to wait on",
	)

	// Take, by contrast, is the loser of a genuine race and must wait.
	_, loserWait := l.Take()
	require.NotNil(
		t, loserWait,
		"Take must still hand the loser the winner's teardown",
	)
}

// TestBindReportsLostPublication asserts Bind tells its caller when it closed
// the socket instead of serving it, so Start does not log that a listener came
// up when a concurrent Stop means none did.
func TestBindReportsLostPublication(t *testing.T) {
	l := newListener()
	addr := testutil.FreePort(t)

	// Stands in for a Stop that detached between Publish and Bind.
	detached := &http.Server{Addr: addr} //nolint:gosec // test server
	bindDone := make(chan struct{})

	served, err := l.Bind(detached, bindDone, apiconfig.EffectiveTLS{})

	require.NoError(t, err, "losing the publication is not a bind failure")
	require.False(
		t, served,
		"Bind must report that it closed the socket rather than serving it",
	)
	require.False(t, portAccepts(addr))
}

// TestBindReportsServed asserts the reporting side that Start's success log
// depends on.
func TestBindReportsServed(t *testing.T) {
	l := newListener()
	srv, bindDone, err := publish(l, testutil.FreePort(t))
	require.NoError(t, err)

	served, err := l.Bind(srv, bindDone, apiconfig.EffectiveTLS{})
	t.Cleanup(func() { _ = stopNow(t, l) })

	require.NoError(t, err)
	require.True(t, served)
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
	require.NoError(t, stopNow(t, l))
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
	require.NoError(t, stopNow(t, l))
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
	require.NoError(t, stopNow(t, l))
}

// TestStopOnAnUnstartedListenerIsClean asserts Stop before Start is not an
// error. The node stops capabilities it may never have started.
func TestStopOnAnUnstartedListenerIsClean(t *testing.T) {
	require.NoError(t, stopNow(t, newListener()))
}

// --- the contract callers rely on ---------------------------------------

// TestListenerIsReusableAfterShutdown asserts a completed Stop leaves the
// Listener able to bring another server up on the same address. This is what a
// capability restart does -- see reinitializeAPIServers in node_lifecycle.go --
// and it is the reason releasing the port has to be part of what Stop waits
// for rather than something Serve gets around to later.
func TestListenerIsReusableAfterShutdown(t *testing.T) {
	l, addr := startOnFreePort(t)
	require.NoError(t, stopNow(t, l))

	srv, bindDone, err := publish(l, addr)
	require.NoError(t, err)
	served, err := l.Bind(srv, bindDone, apiconfig.EffectiveTLS{})
	require.NoError(t, err, "rebinding after a clean Stop must succeed")
	require.True(t, served)
	require.NoError(t, stopNow(t, l))
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
			_, _ = l.Bind(srv, bindDone, apiconfig.EffectiveTLS{})
		}()
		for slot := range stopErrs {
			go func() {
				defer wg.Done()
				stopErrs[slot] = stopNow(t, l)
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
			t, stopNow(t, l),
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
		_, err = next.Bind(nextSrv, bindDone, apiconfig.EffectiveTLS{})
		require.NoError(
			t, err,
			"rebinding after a clean Stop must succeed (iteration %d)", i,
		)
		require.NoError(t, stopNow(t, next))
	}
}
