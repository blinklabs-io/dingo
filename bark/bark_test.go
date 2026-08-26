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

package bark

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// newTestBark builds a Bark wired to db, with no listening server (Start is
// never called), for exercising Acquire/PauseDB/ResumeDB directly.
func newTestBark(t *testing.T, db *database.Database) *Bark {
	t.Helper()
	b, err := NewBark(BarkConfig{DB: db, Port: 1})
	require.NoError(t, err)
	return b
}

func TestBarkListenAddrSupportsIPv6(t *testing.T) {
	require.Equal(t, "[::1]:9091", barkListenAddr("::1", 9091))
	require.Equal(t, "127.0.0.1:9091", barkListenAddr("127.0.0.1", 9091))
}

func TestBarkServerTimeoutsSupportStreaming(t *testing.T) {
	testCases := []struct {
		name   string
		useTLS bool
	}{
		{name: "cleartext"},
		{name: "TLS", useTLS: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			serverCtx, cancelServer := context.WithCancel(t.Context())
			cfg := BarkConfig{
				DB:   newTestDB(t),
				Host: "127.0.0.1",
				Port: freeTCPPort(t),
			}
			if testCase.useTLS {
				cfg.TlsCertFilePath, cfg.TlsKeyFilePath = writeTestTLSCertKey(t)
			}

			b, err := NewBark(cfg)
			require.NoError(t, err)
			t.Cleanup(func() {
				cancelServer()
				require.Eventually(t, func() bool {
					return b.Addr() == ""
				}, 5*time.Second, 10*time.Millisecond,
					"Bark server must finish context-triggered shutdown")
				require.NoError(t, b.Stop(context.Background()))
			})
			require.NoError(t, b.Start(serverCtx))

			b.mu.Lock()
			server := b.server
			b.mu.Unlock()
			require.NotNil(t, server)
			require.Zero(t, server.WriteTimeout,
				"streaming responses must not have an overall write deadline")
			require.Equal(t, 60*time.Second, server.ReadHeaderTimeout)
			require.Equal(t, DefaultRequestReadTimeout, server.ReadTimeout)
			require.Equal(t, 120*time.Second, server.IdleTimeout)
		})
	}
}

// TestAcquireReturnsCurrentDB verifies the ordinary, uncontended path: a
// database was set at construction time, so Acquire hands it back with a
// working release func.
func TestAcquireReturnsCurrentDB(t *testing.T) {
	db := newTestDB(t)
	b := newTestBark(t, db)

	got, release, err := b.Acquire()
	require.NoError(t, err)
	require.NotNil(t, release)
	require.Same(t, db, got)
	release()
}

// TestAcquireFailsFastWhilePaused guards against a real bug:
// a live Restore/Truncate closes the old *database.Database out from under
// any in-flight Bark request that's still holding a stale pointer to it —
// anywhere from a confusing internal error (sqlite) to an outright panic
// (Badger). PauseDB must make new Acquire calls fail immediately with
// ErrDBUnavailable, rather than either blocking (which would stall a
// request for the whole restore/truncate duration) or handing out a
// pointer that's about to be closed.
func TestAcquireFailsFastWhilePaused(t *testing.T) {
	db := newTestDB(t)
	b := newTestBark(t, db)

	paused := make(chan struct{})
	go func() {
		b.PauseDB()
		close(paused)
	}()
	testutil.RequireReceive(t, paused, time.Second, "PauseDB should complete "+
		"immediately when there is no in-flight Acquire holding the gate")

	_, release, err := b.Acquire()
	require.ErrorIs(t, err, ErrDBUnavailable)
	require.Nil(t, release)
}

// TestPauseDBWaitsForInFlightAcquire verifies PauseDB doesn't return — and
// therefore a caller closing the database it guards doesn't proceed — until
// every Acquire holder in flight when it was called has released.
func TestPauseDBWaitsForInFlightAcquire(t *testing.T) {
	db := newTestDB(t)
	b := newTestBark(t, db)

	_, release, err := b.Acquire()
	require.NoError(t, err)

	pauseDone := make(chan struct{})
	go func() {
		b.PauseDB()
		close(pauseDone)
	}()

	testutil.RequireNoReceive(t, pauseDone, 150*time.Millisecond,
		"PauseDB must wait for the in-flight Acquire to release before "+
			"closing the pause gate")

	release()
	testutil.RequireReceive(t, pauseDone, time.Second, "PauseDB should "+
		"complete promptly once the in-flight Acquire releases")
}

// TestResumeDBPublishesNewDBAndUnpauses verifies ResumeDB both republishes
// a (possibly different) *database.Database for future Acquire calls and
// releases the pause PauseDB put in place.
func TestResumeDBPublishesNewDBAndUnpauses(t *testing.T) {
	oldDB := newTestDB(t)
	newDB := newTestDB(t)
	b := newTestBark(t, oldDB)

	b.PauseDB()
	_, _, err := b.Acquire()
	require.ErrorIs(t, err, ErrDBUnavailable)

	b.ResumeDB(newDB)

	got, release, err := b.Acquire()
	require.NoError(t, err)
	require.Same(t, newDB, got)
	release()
}

// TestAddrClearsAfterStop guards against a real bug: Stop
// (and the ctx-cancellation-triggered auto-shutdown goroutine Start
// starts) reset b.server to nil but left b.listenerAddr pointing at the
// now-closed listener's address, so Addr() kept returning that stale,
// no-longer-valid address after the server had actually stopped — instead
// of "", the same as before Start was ever called. A caller polling Addr()
// to tell "is bark actually listening right now" (or reusing it after a
// stop/restart cycle) could be misled into believing a dead address was
// still live.
func TestAddrClearsAfterStop(t *testing.T) {
	db := newTestDB(t)
	b, err := NewBark(
		BarkConfig{DB: db, Host: "127.0.0.1", Port: freeTCPPort(t)},
	)
	require.NoError(t, err)
	require.Empty(t, b.Addr(), "Addr must be empty before Start is ever called")

	require.NoError(t, b.Start(context.Background()))
	require.NotEmpty(
		t,
		b.Addr(),
		"Addr must be populated once Start has bound the listener",
	)

	require.NoError(t, b.Stop(context.Background()))
	require.Empty(
		t,
		b.Addr(),
		"Addr must be cleared, not stale, once the server has stopped",
	)
}

// TestAddrClearsAfterStopTimesOut guards against a real bug: Stop
// only cleared b.server/b.listenerAddr in the branch where
// server.Shutdown returned nil, so a Stop call whose ctx deadline was hit
// before an active connection finished draining (Shutdown returns
// ctx.Err() in that case) left Addr() reporting the old listener address
// as if the server were still live. It isn't: http.Server.Shutdown closes
// every listener essentially immediately, before it even starts waiting
// on active connections to drain, so the listener is already gone
// regardless of whether Shutdown's wait times out or completes.
//
// To force that timeout path, this holds a connection open in a
// non-idle state (mid-request, so Shutdown's closeIdleConns can't reap
// it) and calls Stop with a deadline far shorter than that connection
// will ever take to finish. Whether the OS/runtime has actually
// finished accepting that connection and registered it as non-idle by
// the moment Stop runs is scheduling-dependent and not something this
// process can observe directly (no hook exists to poll it) -- so rather
// than pad a fixed guess with time.Sleep, this retries the whole
// dial-write-Stop sequence against a fresh listener/connection a bounded
// number of times until Stop actually reports the timeout it's supposed
// to, the same way a flaky-by-nature OS-timing assertion is made
// reliable by giving it several independent chances instead of one
// arbitrarily-padded one.
func TestAddrClearsAfterStopTimesOut(t *testing.T) {
	const maxAttempts = 20
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		db := newTestDB(t)
		b, err := NewBark(
			BarkConfig{DB: db, Host: "127.0.0.1", Port: freeTCPPort(t)},
		)
		require.NoError(t, err)
		require.NoError(t, b.Start(context.Background()))
		addr := b.Addr()
		require.NotEmpty(t, addr)

		// A raw connection that has sent a partial request line and
		// nothing more: the server has accepted it and is actively trying
		// to read the rest of the request (net/http.StateActive), so
		// Shutdown's closeIdleConns cannot close it out from under the
		// wait -- unlike a fully idle keep-alive connection, which
		// Shutdown reaps immediately regardless of ctx.
		conn, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		_, err = conn.Write([]byte("GET / HTTP/1.1\r\n"))
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(
			context.Background(),
			20*time.Millisecond,
		)
		stopErr := b.Stop(ctx)
		cancel()
		_ = conn.Close()
		_ = db.Close()

		if stopErr == nil {
			// The connection wasn't registered as non-idle in time this
			// attempt (a scheduling artifact, not the behavior under
			// test) -- Stop succeeded normally, so Addr() being cleared
			// here would be true regardless of the fix. Retry with a
			// fresh server instead of asserting on an inconclusive run.
			continue
		}

		require.Empty(
			t, b.Addr(),
			"Addr must be cleared even when Stop's Shutdown call times "+
				"out, since the listener is already closed either way",
		)
		return
	}
	t.Fatalf(
		"Stop never reported a Shutdown timeout in %d attempts -- "+
			"could not exercise the timeout path this test guards",
		maxAttempts,
	)
}

// TestAddrClearsWhenStartContextIsCancelled is TestAddrClearsAfterStop's
// counterpart for the OTHER shutdown path: Start's own ctx being
// cancelled directly (not a separate call to Stop) triggers the same
// auto-shutdown goroutine, which must clear listenerAddr the same way.
func TestAddrClearsWhenStartContextIsCancelled(t *testing.T) {
	db := newTestDB(t)
	b, err := NewBark(
		BarkConfig{DB: db, Host: "127.0.0.1", Port: freeTCPPort(t)},
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, b.Start(ctx))
	require.NotEmpty(t, b.Addr())

	cancel()
	testutil.WaitForCondition(t, func() bool {
		return b.Addr() == ""
	}, 5*time.Second, "Addr must clear once Start's ctx cancellation finishes shutting the server down")
}

// TestStopDoesNotDeadlockWithInFlightAcquire guards against a real
// deadlock: Stop holds b.mu for the entire duration of its blocking
// server.Shutdown call, which itself waits for in-flight requests to
// finish. If Acquire also needed b.mu to read config.DB (as it used to),
// a request whose handler calls Acquire — exactly the kind of in-flight
// request Shutdown is waiting to drain — could never make progress: Stop
// holds b.mu waiting for the handler to finish, while the handler's
// Acquire call blocks waiting for b.mu that Stop won't release until the
// handler finishes. Classic lock-ordering deadlock.
//
// This wires up a real http.Server (bypassing Start, which would use
// Bark's own gRPC handlers) with a handler that blocks until signaled,
// then calls Acquire. It uses b.mu.TryLock polling to deterministically
// wait until Stop has actually taken b.mu (and is therefore blocked
// inside Shutdown) before letting the handler proceed to call Acquire —
// otherwise the handler could race ahead and acquire first, never
// exercising the deadlock window at all. A bounded timeout on every wait
// makes a regression fail loudly instead of hanging the test suite.
func TestStopDoesNotDeadlockWithInFlightAcquire(t *testing.T) {
	db := newTestDB(t)
	b := newTestBark(t, db)

	entered := make(chan struct{})
	proceed := make(chan struct{})
	acquireDone := make(chan error, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-proceed
		_, release, err := b.Acquire()
		if release != nil {
			release()
		}
		acquireDone <- err
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := &http.Server{
		Handler: mux,
	} //nolint:gosec // test-only server, no real timeouts needed

	b.mu.Lock()
	b.server = server
	b.listenerAddr = ln.Addr()
	b.mu.Unlock()

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = server.Serve(ln)
	}()

	reqDone := make(chan struct{})
	go func() {
		defer close(reqDone)
		resp, getErr := http.Get(
			"http://" + ln.Addr().String() + "/",
		) //nolint:noctx,gosec // test-only request to a loopback test server
		if getErr == nil {
			_ = resp.Body.Close()
		}
	}()

	testutil.RequireReceive(
		t, entered, 2*time.Second,
		"handler should have been entered by the in-flight request",
	)

	stopDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stopDone <- b.Stop(ctx)
	}()

	// Wait until b.mu is actually held (presumably by the Stop goroutine
	// above, blocked inside Shutdown waiting for our handler) before
	// letting the handler proceed to call Acquire. Stop holds b.mu for
	// its entire body, so once TryLock starts failing it stays failing
	// until Stop returns.
	testutil.WaitForCondition(t, func() bool {
		if b.mu.TryLock() {
			b.mu.Unlock()
			return false
		}
		return true
	}, 2*time.Second, "Stop should take b.mu before/while blocking in Shutdown")

	close(proceed)

	select {
	case err := <-acquireDone:
		require.NoError(
			t,
			err,
			"Acquire should succeed for the in-flight request",
		)
	case <-time.After(3 * time.Second):
		t.Fatal(
			"Acquire did not return -- deadlocked behind Stop's b.mu hold " +
				"while Stop's Shutdown waited for this very request to finish",
		)
	}

	select {
	case err := <-stopDone:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Stop did not return after the in-flight request completed")
	}

	testutil.RequireReceive(
		t,
		reqDone,
		2*time.Second,
		"client request should complete",
	)
	testutil.RequireReceive(
		t,
		serveDone,
		2*time.Second,
		"Serve should return once Shutdown completes",
	)
}
