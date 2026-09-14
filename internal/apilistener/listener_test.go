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
	return l.Stop(ctx, Graceful)
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
// would leave the next Publish's take handing out a stale channel.
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

	require.ErrorIs(
		t, ln.Close(), net.ErrClosed,
		"Stop must close the original socket Serve never registered",
	)
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

// TestMatchedTakeIgnoresAServerItDoesNotOwn asserts a context monitor cannot tear
// down a server its own Start never published. Start's monitor outlives the
// server it was launched for -- it sits on ctx.Done() until the caller's
// context ends, which may be long after that server was stopped and a restart
// published another one on the same Listener. An unconditional detach there
// would shut the replacement down.
func TestMatchedTakeIgnoresAServerItDoesNotOwn(t *testing.T) {
	l := newListener()
	first, err := publishBound(l, "127.0.0.1:0")
	require.NoError(t, err)
	require.NoError(t, stopNow(t, l))

	// The restart, on the same Listener.
	second, err := publishBound(l, "127.0.0.1:0")
	require.NoError(t, err)

	job, inFlight := l.take(first)

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

// TestMatchedTakeDetachesItsOwnServer asserts the identity check does not defeat the
// case it exists to serve: a monitor whose server is still the current one
// still tears it down.
func TestMatchedTakeDetachesItsOwnServer(t *testing.T) {
	l := newListener()
	srv, err := publishBound(l, "127.0.0.1:0")
	require.NoError(t, err)

	job, _ := l.take(srv)

	require.NotNil(t, job, "a monitor must detach the server it published")
	require.Nil(t, l.Server())
}

// TestMatchedTakeHandsBackNoTeardownWhenItsServerIsGone covers the case where the
// monitor's server has been detached but its teardown is still running, so
// l.srv is nil rather than pointing at a replacement. The identity check has
// to come first: a caller landing here must be told there is nothing of its own
// left, not handed a teardown that belongs to whoever detached it. Waiting on
// that would block a monitor on an unrelated shutdown.
func TestMatchedTakeHandsBackNoTeardownWhenItsServerIsGone(t *testing.T) {
	l := newListener()
	srv, err := publishBound(l, "127.0.0.1:0")
	require.NoError(t, err)

	// Another caller detached it and is still tearing it down.
	winner, _ := l.take(nil)
	require.NotNil(t, winner)

	job, inFlight := l.take(srv)

	require.Nil(t, job)
	require.Nil(
		t, inFlight,
		"a monitor whose server is gone has nothing of its own to wait on",
	)

	// An unmatched take, by contrast, is the loser of a genuine race and
	// must wait.
	_, loserWait := l.take(nil)
	require.NotNil(
		t, loserWait,
		"an unmatched take must still hand the loser the winner's teardown",
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

// TestServeEnteredAfterShutdownStaysQuiet pins the residual window Bind cannot
// close: a Stop can detach and tear down between Bind recording the listener
// and its goroutine entering Serve, so Serve is handed an already-closed socket
// it never serves.
//
// What matters is that the window is inert. Serve reports ErrServerClosed --
// the one value the goroutine's error filter deliberately drops -- so the case
// produces no spurious error log, and the port is released either way. The only
// visible artifact is Start having already logged that the listener came up.
// Closing the window entirely would need Serve to signal that it registered the
// listener, which net/http does not expose, and would still lose to a Stop
// landing an instant later.
func TestServeEnteredAfterShutdownStaysQuiet(t *testing.T) {
	addr := testutil.FreePort(t)
	ln, err := net.Listen("tcp", addr)
	require.NoError(t, err)
	srv := &http.Server{Addr: addr} //nolint:gosec // test server

	// The teardown, landing before Serve is entered.
	require.NoError(t, srv.Shutdown(context.Background()))
	require.NoError(t, ln.Close())

	serveErr := srv.Serve(ln)

	require.ErrorIs(
		t, serveErr, http.ErrServerClosed,
		"Serve entered after Shutdown must stay within the filtered error",
	)
	require.False(t, portAccepts(addr), "the port must still be released")
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

// TestStopWaitsForATeardownItLost asserts the loser of the detach race does not
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
			t, l.awaitTeardown(ctx, done),
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
		// The contract Stop's nil return promises: the address is rebindable.
		// Rebinding is the externally observable assertion here. Closure of
		// the original listener object is checked directly above, without
		// confusing a concurrently rebound address for the old listener.
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

// --- the start gate -----------------------------------------------------

// TestStopWaitsForAStartStillInFlight is the defect the start gate exists for.
//
// A start is only visible to the rest of the protocol once it has published a
// server, and everything before that -- reading config, building a handler
// chain, being descheduled -- is a window in which a Stop finds nothing to
// detach. Without the gate that Stop returns nil, telling its caller the
// server is down, and the start it could not see goes on to bind the port
// behind it. Waiting is the only correct answer: the start is about to produce
// exactly the server this Stop was asked to take down.
//
// Pinned by the wait rather than by a race: with the gate held and no start
// ever completing, a bounded Stop must time out.
func TestStopWaitsForAStartStillInFlight(t *testing.T) {
	l := newListener()
	startDone, err := l.BeginStart()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	require.ErrorIs(
		t, stop(ctx, l), context.DeadlineExceeded,
		"Stop must wait out a start still in flight rather than reporting "+
			"the server down",
	)

	// Once the start finishes, Stop completes.
	l.EndStart(startDone)
	require.NoError(t, stopNow(t, l))
}

// TestStopTakesDownAServerPublishedWhileItWaited is the consequence the gate
// prevents, stated as the port rather than as the wait: the server a Stop
// could not see when it arrived is still the server it has to take down.
//
// The gate is what orders this deterministically. It is held before the Stop
// is launched, so the publish and bind below are the ones that would otherwise
// have landed behind a Stop that had already returned.
func TestStopTakesDownAServerPublishedWhileItWaited(t *testing.T) {
	l := newListener()
	addr := testutil.FreePort(t)
	startDone, err := l.BeginStart()
	require.NoError(t, err)

	stopErr := make(chan error, 1)
	go func() { stopErr <- stopNow(t, l) }()
	testutil.RequireNoReceive(
		t, stopErr, 200*time.Millisecond,
		"Stop must not report the server down while a start is in flight",
	)

	// The start this Stop is waiting on, completing normally.
	srv, bindDone, err := publish(l, addr)
	require.NoError(t, err)
	served, err := l.Bind(srv, bindDone, apiconfig.EffectiveTLS{})
	require.NoError(t, err)
	require.True(t, served)
	l.EndStart(startDone)

	require.NoError(
		t,
		testutil.RequireReceive(
			t, stopErr, 5*time.Second, "Stop must complete once the start does",
		),
	)
	require.False(
		t, portAccepts(addr),
		"Stop must take down the server brought up by the start it waited on",
	)
}

// TestBeginStartRejectsASecondStart asserts the gate admits one start at a
// time, and is released for the next one.
func TestBeginStartRejectsASecondStart(t *testing.T) {
	l := newListener()
	first, err := l.BeginStart()
	require.NoError(t, err)

	_, err = l.BeginStart()
	require.ErrorContains(t, err, "start already in progress")

	l.EndStart(first)
	second, err := l.BeginStart()
	require.NoError(
		t, err, "the gate must be available again once a start returns",
	)
	l.EndStart(second)
}

// TestEndStartOnlyReleasesTheGateItHolds asserts a start that never took the
// gate cannot release the one held by the start that beat it. Every Start
// defers EndStart before it can fail, including the failure that is losing the
// gate, so this path is taken on every rejected concurrent start.
func TestEndStartOnlyReleasesTheGateItHolds(t *testing.T) {
	l := newListener()
	held, err := l.BeginStart()
	require.NoError(t, err)

	// The rejected start holds no gate at all.
	l.EndStart(nil)
	// And a gate belonging to nobody is not this Listener's to clear.
	l.EndStart(make(chan struct{}))

	l.mu.Lock()
	current := l.startDone
	l.mu.Unlock()
	require.True(
		t, current == held,
		"EndStart must not release a gate it does not hold",
	)

	l.EndStart(held)
	l.mu.Lock()
	defer l.mu.Unlock()
	require.Nil(t, l.startDone)
}

// --- the context monitor ------------------------------------------------

// TestWatchExitsWhenItsServerIsStopped is the second defect this package
// exists to close, and the reason the monitor lives here rather than in each
// API package.
//
// Every production caller passes the node context, which stays live across a
// capability restart and for the rest of the process. A monitor waiting only
// on that context therefore outlives the server it was launched for, holding
// the whole stopped http.Server, its handler chain, and through that the
// database those handlers were built over. There is one per Start, and every
// live Restore or Truncate performs another Start, so the retained set grows
// with operator actions and includes the database each restore just replaced.
func TestWatchExitsWhenItsServerIsStopped(t *testing.T) {
	l := newListener()
	srv, err := publishBound(l, "127.0.0.1:0")
	require.NoError(t, err)

	// Deliberately not cancelled until after the assertion: the context is
	// what a monitor must NOT be relying on to be released.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exited := l.Watch(ctx, srv, Graceful)

	require.NoError(t, stopNow(t, l))

	testutil.RequireReceive(
		t, exited, 5*time.Second,
		"the monitor must exit when its server is torn down rather than "+
			"holding it until the node context ends",
	)
}

// TestWatchExitsWhenAFailedStartUnpublishes covers the other way a server
// leaves the Listener: a Start whose bind failed clears it, and the monitor it
// already launched has nothing left to watch.
func TestWatchExitsWhenAFailedStartUnpublishes(t *testing.T) {
	l := newListener()
	srv, _, err := publish(l, "127.0.0.1:0")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exited := l.Watch(ctx, srv, Graceful)

	l.Unpublish(srv)

	testutil.RequireReceive(
		t, exited, 5*time.Second,
		"the monitor must exit when its server is unpublished",
	)
}

// TestWatchShutsDownOnContextCancellation asserts closing the leak did not
// cost the monitor its job: a cancelled context still releases the port,
// whatever the server's own Stop is doing.
func TestWatchShutsDownOnContextCancellation(t *testing.T) {
	l, addr := startOnFreePort(t)
	srv := l.Server()
	require.NotNil(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	exited := l.Watch(ctx, srv, Graceful)
	cancel()

	testutil.RequireReceive(
		t, exited, 5*time.Second, "the monitor must run and then exit",
	)
	require.Nil(t, l.Server(), "the monitor must detach the server it watched")
	require.False(
		t, portAccepts(addr),
		"a cancelled context must release the listening socket",
	)
}

// TestWatchOnAReplacedServerExitsImmediately asserts a monitor launched for a
// server that was already detached does not wait on a channel nobody will
// close. Reachable when a context is cancelled between Publish and Watch.
func TestWatchOnAReplacedServerExitsImmediately(t *testing.T) {
	l := newListener()
	stale := &http.Server{Addr: "127.0.0.1:0"} //nolint:gosec // test server

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exited := l.Watch(ctx, stale, Graceful)

	testutil.RequireReceive(
		t, exited, 5*time.Second,
		"a monitor with no server of its own must not wait",
	)
	require.Nil(t, l.Server())
}
