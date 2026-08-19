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

package mesh

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// --- construction validation -------------------------------------------

// TestNewServerRequiresDependencies asserts every dependency the
// handlers dereference is validated up front, so a misconfigured node
// fails at startup rather than on the first request.
func TestNewServerRequiresDependencies(t *testing.T) {
	tests := map[string]func(*ServerConfig){
		"missing chain": func(c *ServerConfig) {
			c.Chain = nil
		},
		"missing database": func(c *ServerConfig) {
			c.Database = nil
		},
		"missing ledger state": func(c *ServerConfig) {
			c.LedgerState = nil
		},
		"missing mempool": func(c *ServerConfig) {
			c.Mempool = nil
		},
		"missing network": func(c *ServerConfig) {
			c.Network = ""
		},
		"missing genesis hash": func(c *ServerConfig) {
			c.GenesisHash = ""
		},
		"non-hex genesis hash": func(c *ServerConfig) {
			c.GenesisHash = "not-hex"
		},
		"zero genesis start time": func(c *ServerConfig) {
			c.GenesisStartTimeSec = 0
		},
		"negative genesis start time": func(c *ServerConfig) {
			c.GenesisStartTimeSec = -1
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			deps := newTestDeps()
			cfg := ServerConfig{
				LedgerState:         deps.ledger,
				Database:            deps.database,
				Chain:               deps.chain,
				Mempool:             deps.mempool,
				Network:             testNetwork,
				NetworkMagic:        testNetworkMagic,
				GenesisHash:         testGenesisHash,
				GenesisStartTimeSec: testGenesisStartTimeSec,
			}
			mutate(&cfg)

			srv, err := NewServer(cfg)

			require.Error(t, err)
			require.Nil(t, srv)
			require.Contains(t, err.Error(), "mesh:")
		})
	}
}

// TestNewServerDefaults covers the optional configuration: a nil logger
// and an empty listen address must not leave the server unusable.
func TestNewServerDefaults(t *testing.T) {
	deps := newTestDeps()
	srv, err := NewServer(ServerConfig{
		LedgerState:         deps.ledger,
		Database:            deps.database,
		Chain:               deps.chain,
		Mempool:             deps.mempool,
		Network:             testNetwork,
		GenesisHash:         testGenesisHash,
		GenesisStartTimeSec: testGenesisStartTimeSec,
	})

	require.NoError(t, err)
	require.NotNil(t, srv.logger)
	require.Equal(t, defaultListenAddr, srv.config.ListenAddress)
}

// TestNewServerAddressNetworkFollowsMagic asserts the address network
// used for every derived address is chosen from the network magic.
func TestNewServerAddressNetworkFollowsMagic(t *testing.T) {
	deps := newTestDeps()

	testnet := newTestServer(t, deps)
	mainnet := newTestServer(
		t, deps,
		func(c *ServerConfig) { c.NetworkMagic = mainnetMagic },
	)

	require.Equal(t, uint8(0), testnet.addrNetworkID)
	require.Equal(t, uint8(1), mainnet.addrNetworkID)
}

// --- listener lifecycle -------------------------------------------------

// startOnFreePort starts a server on a free loopback port, retrying on
// a lost race for the port, and returns it with the address it bound.
// The caller owns shutdown.
//
// Each attempt gets its own cancellable context. Start launches the
// context-monitor goroutine before it binds, so a failed bind returns
// an error while leaving that goroutine parked on the context; Stop
// does not release it, because the goroutine waits on the context
// rather than on server state. Cancelling the failed attempt's context
// retires its goroutine immediately instead of holding one per retry
// until the test ends. The surviving attempt's context stays a child of
// the caller's, so cancelling that still shuts the server down.
func startOnFreePort(
	t *testing.T,
	ctx context.Context,
	deps *testDeps,
	opts ...serverOption,
) (*Server, string) {
	t.Helper()
	var lastErr error
	for range testutil.BindAttempts {
		addr := testutil.FreePort(t)
		attemptOpts := make([]serverOption, 0, len(opts)+1)
		attemptOpts = append(attemptOpts, opts...)
		attemptOpts = append(
			attemptOpts,
			func(c *ServerConfig) { c.ListenAddress = addr },
		)
		srv := newTestServer(t, deps, attemptOpts...)
		attemptCtx, cancel := context.WithCancel(ctx)
		if err := srv.Start(attemptCtx); err != nil {
			cancel()
			lastErr = err
			continue
		}
		t.Cleanup(cancel)
		return srv, addr
	}
	t.Fatalf(
		"could not bind a free loopback port in %d attempts: %v",
		testutil.BindAttempts, lastErr,
	)
	return nil, ""
}

// startTestServer starts a server on a free port and stops it when the
// test ends, returning the base URL.
func startTestServer(
	t *testing.T,
	deps *testDeps,
	opts ...serverOption,
) (*Server, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	srv, addr := startOnFreePort(t, ctx, deps, opts...)
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer stopCancel()
		require.NoError(t, srv.Stop(stopCtx))
	})

	return srv, "http://" + addr
}

func TestServerServesRequests(t *testing.T) {
	deps := newTestDeps()
	_, baseURL := startTestServer(t, deps)

	body, err := json.Marshal(MetadataRequest{})
	require.NoError(t, err)
	resp, err := http.Post(
		baseURL+"/network/list",
		"application/json",
		bytes.NewReader(body),
	)
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Cleanup(func() { _ = resp.Body.Close() })

	require.Equal(t, http.StatusOK, resp.StatusCode)
	var decoded NetworkListResponse
	require.NoError(
		t, json.NewDecoder(resp.Body).Decode(&decoded),
	)
	require.Len(t, decoded.NetworkIdentifiers, 1)
}

// TestServerBindFailure covers a listen address already in use: Start
// must report the failure instead of silently running without a
// listener.
func TestServerBindFailure(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = occupied.Close() })

	srv := newTestServer(
		t,
		newTestDeps(),
		func(c *ServerConfig) {
			c.ListenAddress = occupied.Addr().String()
		},
	)

	err = srv.Start(t.Context())

	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to listen")
	// A failed start must leave the server restartable.
	require.Nil(t, srv.httpServer)
}

// TestServerDoubleStart asserts a second Start is refused rather than
// leaking the first listener.
func TestServerDoubleStart(t *testing.T) {
	srv, _ := startTestServer(t, newTestDeps())

	err := srv.Start(t.Context())

	require.ErrorContains(t, err, "already started")
}

// TestServerStopIsIdempotent asserts Stop on a server that was never
// started, or stopped twice, is a no-op rather than an error.
func TestServerStopIsIdempotent(t *testing.T) {
	srv := newTestServer(t, newTestDeps())

	require.NoError(t, srv.Stop(t.Context()))
	require.NoError(t, srv.Stop(t.Context()))
}

// TestServerGracefulShutdown asserts Stop closes the listener so the
// port stops accepting connections, and that it has done so by the time
// Stop returns rather than some time afterwards.
func TestServerGracefulShutdown(t *testing.T) {
	srv, addr := startOnFreePort(
		t, t.Context(), newTestDeps(),
	)

	stopCtx, cancel := context.WithTimeout(
		t.Context(), 5*time.Second,
	)
	defer cancel()
	require.NoError(t, srv.Stop(stopCtx))

	require.False(
		t, portAccepts(addr),
		"listener still accepting after Stop",
	)
}

// TestServerStopReleasesPortBeforeServeRegisters covers the window
// between startServer binding the socket and the goroutine it launches
// reaching http.Server.Serve: Shutdown closes only the listeners Serve
// registered, so in that window Stop used to return with the port still
// bound -- which the capability restart in node_lifecycle.go then fails
// to rebind. Stopping straight after Start lands in the window often
// but not every time, so the assertion is repeated.
func TestServerStopReleasesPortBeforeServeRegisters(t *testing.T) {
	for i := range 100 {
		srv, addr := startOnFreePort(
			t, t.Context(), newTestDeps(),
		)

		require.NoError(t, srv.Stop(t.Context()))

		require.False(
			t, portAccepts(addr),
			"listener still accepting when Stop returned "+
				"(iteration %d)", i,
		)
	}
}

// TestStartServerReleasesListenerWhenServerAlreadyDetached covers the
// window between Start publishing s.httpServer and startServer recording
// the listener. A Stop landing inside it detaches the server, so takeServer
// later hands back a nil server and shutdownServer never runs -- meaning
// startServer must not leave its own socket bound, and must not overwrite
// the listener of whichever server is current now.
func TestStartServerReleasesListenerWhenServerAlreadyDetached(t *testing.T) {
	srv := newTestServer(t, newTestDeps())
	addr := testutil.FreePort(t)

	// Stands in for the server a concurrent restart already published;
	// it must survive this call untouched.
	currentListener, err := net.Listen("tcp", testutil.FreePort(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = currentListener.Close() })
	current := &http.Server{Addr: currentListener.Addr().String()}
	srv.mu.Lock()
	srv.httpServer = current
	srv.listener = currentListener
	srv.mu.Unlock()

	// The detached server this startServer call is bringing up.
	detached := &http.Server{Addr: addr}
	bindDone := make(chan struct{})
	require.NoError(t, srv.startServer(detached, bindDone))
	testutil.RequireReceive(
		t, bindDone, time.Second,
		"startServer must signal that the bind settled",
	)

	require.False(
		t, portAccepts(addr),
		"startServer must not leave a stopped server's port bound",
	)
	srv.mu.Lock()
	defer srv.mu.Unlock()
	require.Same(
		t, current, srv.httpServer,
		"startServer must not disturb the current server",
	)
	require.Same(
		t, currentListener, srv.listener,
		"startServer must not overwrite the current server's listener",
	)
}

// TestStopWaitsForAnInFlightBind asserts Stop does not report the server
// down while a startServer call is still between net.Listen and releasing
// its socket. Detaching the server is what makes that bind close its own
// listener, so without waiting here Stop could return -- and a caller could
// rebind the same port -- while the old socket was still open.
func TestStopWaitsForAnInFlightBind(t *testing.T) {
	srv := newTestServer(t, newTestDeps())

	// Stands in for Start having published a server and a bind that has
	// not finished yet.
	bindDone := make(chan struct{})
	srv.mu.Lock()
	srv.httpServer = &http.Server{Addr: testutil.FreePort(t)}
	srv.bindDone = bindDone
	srv.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	err := srv.Stop(ctx)
	require.ErrorIs(
		t, err, context.DeadlineExceeded,
		"Stop must wait for the in-flight bind rather than returning",
	)

	// Once the bind settles, Stop completes.
	close(bindDone)
	require.NoError(t, srv.Stop(t.Context()))
}

// TestStopTearsDownEvenWhenTheBindWaitTimesOut asserts a Stop whose context
// expires mid-wait still releases the socket it detached. The detach is what
// makes Stop the only remaining reference to that listener, so returning the
// wait error without tearing down would leave the port bound with nothing left
// able to close it.
func TestStopTearsDownEvenWhenTheBindWaitTimesOut(t *testing.T) {
	srv := newTestServer(t, newTestDeps())
	addr := testutil.FreePort(t)
	ln, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	// A published listener plus a bind that never settles.
	srv.mu.Lock()
	srv.httpServer = &http.Server{Addr: addr}
	srv.listener = ln
	srv.bindDone = make(chan struct{})
	srv.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, srv.Stop(ctx), context.DeadlineExceeded)

	require.False(
		t, portAccepts(addr),
		"Stop must release the socket it detached even when the bind "+
			"wait times out",
	)
}

// TestStopWaitsForATeardownItLost asserts the loser of the takeServer race
// does not report the server down early. Stop and the context monitor both
// detach; only one wins, and a Stop that returned nil while the winner was
// still releasing the port would let an immediate restart fail to bind.
func TestStopWaitsForATeardownItLost(t *testing.T) {
	srv := newTestServer(t, newTestDeps())

	// Stands in for another caller having already detached the server and
	// still being mid-teardown.
	teardown := make(chan struct{})
	srv.mu.Lock()
	srv.httpServer = nil
	srv.teardown = teardown
	srv.mu.Unlock()

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	require.ErrorIs(
		t, srv.Stop(ctx), context.DeadlineExceeded,
		"Stop must wait for the teardown it lost rather than returning nil",
	)

	close(teardown)
	require.NoError(t, srv.Stop(t.Context()))
}

// TestAwaitTeardownPrefersACompletedTeardown asserts a finished teardown is
// never reported as a timeout. When the completion channel and the context are
// both ready, select picks at random, so the loop is what makes the absence of
// a recheck fail rather than flake.
func TestAwaitTeardownPrefersACompletedTeardown(t *testing.T) {
	done := make(chan struct{})
	close(done)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	for i := range 200 {
		require.NoError(
			t, awaitTeardown(ctx, done),
			"a completed teardown must not be reported as a timeout "+
				"(iteration %d)", i,
		)
	}
}

// TestTimedOutTeardownDoesNotSignalCompletionEarly asserts a Stop whose bind
// wait times out does not mark the teardown complete. startServer still owns a
// socket that Stop cannot close, so a second caller waiting on the teardown has
// to keep waiting rather than read it as "the port is free".
func TestTimedOutTeardownDoesNotSignalCompletionEarly(t *testing.T) {
	srv := newTestServer(t, newTestDeps())
	bindDone := make(chan struct{})

	srv.mu.Lock()
	srv.httpServer = &http.Server{Addr: testutil.FreePort(t)}
	srv.bindDone = bindDone
	srv.mu.Unlock()

	// First caller detaches and times out waiting for the bind.
	stopCtx, cancelStop := context.WithTimeout(
		context.Background(),
		100*time.Millisecond,
	)
	defer cancelStop()
	require.ErrorIs(t, srv.Stop(stopCtx), context.DeadlineExceeded)

	// Second caller lost the detach and must not be told the teardown is done.
	loserCtx, cancelLoser := context.WithTimeout(
		context.Background(),
		100*time.Millisecond,
	)
	defer cancelLoser()
	require.ErrorIs(
		t, srv.Stop(loserCtx), context.DeadlineExceeded,
		"a teardown blocked on an in-flight bind must not report completion",
	)

	// Once the bind settles the teardown is genuinely complete.
	close(bindDone)
	require.NoError(t, srv.Stop(t.Context()))
}

// TestConcurrentStartStopNeverLeavesThePortBound hammers the interleavings the
// individual lifecycle tests each pin one of: Start racing Stop, Stop racing the
// context monitor, and a restart on the same address immediately after.
//
// The invariant is the one every caller relies on: once Stop returns without an
// error, the address is free, so the next Start on it must succeed.
//
// What this does NOT cover, verified by running it against the earlier buggy
// revisions, where it passed: the paths that need a bind still in flight when a
// wait expires. A real bind settles far too quickly for that, so a stalled bind
// has to be constructed. Those live in
// TestStopTearsDownEvenWhenTheBindWaitTimesOut,
// TestStopWaitsForATeardownItLost, and
// TestTimedOutTeardownDoesNotSignalCompletionEarly, each checked against the
// defect it names. Do not read a pass here as covering them.
func TestConcurrentStartStopNeverLeavesThePortBound(t *testing.T) {
	addr := testutil.FreePort(t)

	for i := range 60 {
		srv := newTestServer(
			t, newTestDeps(),
			func(c *ServerConfig) { c.ListenAddress = addr },
		)
		ctx, cancel := context.WithCancel(context.Background())

		// Four-way contention on purpose: Start, two Stops, and the context
		// monitor. Two Stops matter — one of them loses takeServer and has to
		// wait on the winner's teardown, which is the path where a premature
		// completion signal turns into a false "the port is free".
		var wg sync.WaitGroup
		stopErrs := make([]error, 2)
		wg.Add(4)
		go func() {
			defer wg.Done()
			_ = srv.Start(ctx)
		}()
		for slot := range stopErrs {
			go func() {
				defer wg.Done()
				stopErrs[slot] = srv.Stop(t.Context())
			}()
		}
		go func() {
			defer wg.Done()
			cancel()
		}()
		wg.Wait()

		// Every Stop that returned nil made the same promise, so the strictest
		// reading applies: if any of them reported clean, the port must be free.
		stopErr := errors.Join(stopErrs...)
		if stopErr != nil && stopErrs[0] != nil && stopErrs[1] != nil {
			// A reported timeout is honest: the caller was told the port may
			// still be held, so it is not licensed to rebind.
			continue
		}
		require.NoError(
			t, srv.Stop(t.Context()),
			"a second Stop must stay clean (iteration %d)", i,
		)
		require.False(
			t, portAccepts(addr),
			"Stop returned nil but the port is still accepting "+
				"(iteration %d)", i,
		)

		// The contract Stop's nil return promises: the address is rebindable.
		next := newTestServer(
			t, newTestDeps(),
			func(c *ServerConfig) { c.ListenAddress = addr },
		)
		nextCtx, cancelNext := context.WithCancel(context.Background())
		require.NoError(
			t, next.Start(nextCtx),
			"rebinding after a clean Stop must succeed (iteration %d)", i,
		)
		require.NoError(t, next.Stop(t.Context()))
		cancelNext()
		_ = stopErr
	}
}

// TestServerShutdownOnContextCancel asserts cancelling the context
// passed to Start shuts the listener down, which is how the node stops
// the API during its own shutdown.
func TestServerShutdownOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	_, addr := startOnFreePort(t, ctx, newTestDeps())

	cancel()

	testutil.WaitForCondition(
		t,
		func() bool { return !portAccepts(addr) },
		5*time.Second,
		"listener still accepting after context cancel",
	)
}

// portAccepts reports whether a TCP connection to addr succeeds.
func portAccepts(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// --- CORS ---------------------------------------------------------------

// TestServerCORSPreflight covers browser access: a preflight from an
// allowed origin succeeds, and one from any other origin is refused.
func TestServerCORSPreflight(t *testing.T) {
	const allowed = "https://wallet.example"
	_, baseURL := startTestServer(
		t,
		newTestDeps(),
		func(c *ServerConfig) {
			c.CORSAllowedOrigins = []string{allowed}
		},
	)

	t.Run("allowed origin", func(t *testing.T) {
		resp := preflight(t, baseURL, allowed)
		t.Cleanup(func() { _ = resp.Body.Close() })

		require.Equal(t, http.StatusNoContent, resp.StatusCode)
		require.Equal(
			t,
			allowed,
			resp.Header.Get("Access-Control-Allow-Origin"),
		)
	})

	t.Run("disallowed origin", func(t *testing.T) {
		resp := preflight(t, baseURL, "https://evil.example")
		t.Cleanup(func() { _ = resp.Body.Close() })

		require.Equal(t, http.StatusForbidden, resp.StatusCode)
		require.Empty(
			t,
			resp.Header.Get("Access-Control-Allow-Origin"),
		)
	})
}

// TestServerCORSDisabledByDefault asserts no CORS headers are emitted
// when no origins are configured, so a browser cannot read responses
// from an unconfigured deployment.
func TestServerCORSDisabledByDefault(t *testing.T) {
	_, baseURL := startTestServer(t, newTestDeps())

	resp := preflight(t, baseURL, "https://wallet.example")
	t.Cleanup(func() { _ = resp.Body.Close() })

	require.Empty(
		t, resp.Header.Get("Access-Control-Allow-Origin"),
	)
}

// preflight issues a CORS preflight request against /network/list.
func preflight(
	t *testing.T,
	baseURL string,
	origin string,
) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodOptions,
		baseURL+"/network/list",
		nil,
	)
	require.NoError(t, err)
	req.Header.Set("Origin", origin)
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	return resp
}

// --- request bounds -----------------------------------------------------

// TestRequestBodyLimit covers the 1 MiB request cap: an oversized body
// is rejected as an invalid request rather than being buffered whole.
func TestRequestBodyLimit(t *testing.T) {
	h := newTestHandler(t, newTestDeps())
	oversized := `{"network_identifier":{"blockchain":"cardano",` +
		`"network":"preview"},"metadata":{"pad":"` +
		strings.Repeat("a", maxRequestBody) + `"}}`

	rec := postRaw(t, h, "/network/status", oversized)

	requireMeshError(
		t, rec, ErrInvalidRequest, http.StatusBadRequest,
	)
}

// TestRequestBodyAtLimitIsAccepted pins the accepting side of the cap
// at the exact boundary: a body of precisely maxRequestBody bytes must
// still be served, so a regression that tightens the limit is caught
// rather than hidden behind a comfortably small request.
func TestRequestBodyAtLimitIsAccepted(t *testing.T) {
	h := newTestHandler(t, newTestDeps())
	const prefix = `{"network_identifier":{"blockchain":"cardano",` +
		`"network":"preview"},"metadata":{"pad":"`
	const suffix = `"}}`
	padded := prefix +
		strings.Repeat("a", maxRequestBody-len(prefix)-len(suffix)) +
		suffix
	require.Len(t, padded, maxRequestBody)

	rec := postRaw(t, h, "/network/status", padded)

	decodeResponse[NetworkStatusResponse](t, rec)
}

// TestServerTimeoutsAreConfigured pins the listener timeouts, which
// bound how long a slow or idle client can hold a connection.
func TestServerTimeoutsAreConfigured(t *testing.T) {
	srv, _ := startTestServer(t, newTestDeps())

	srv.mu.Lock()
	httpServer := srv.httpServer
	srv.mu.Unlock()

	require.NotNil(t, httpServer)
	require.Equal(
		t, 60*time.Second, httpServer.ReadHeaderTimeout,
	)
	require.Equal(t, 30*time.Second, httpServer.WriteTimeout)
	require.Equal(t, 120*time.Second, httpServer.IdleTimeout)
}

// --- routing ------------------------------------------------------------

// TestUnknownRouteIsNotFound asserts an unregistered path does not fall
// through to a handler.
func TestUnknownRouteIsNotFound(t *testing.T) {
	h := newTestHandler(t, newTestDeps())

	rec := postRaw(t, h, "/does/not/exist", "{}")

	require.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRoutesRejectNonPost asserts every Mesh endpoint is POST-only, as
// the Rosetta specification requires.
func TestRoutesRejectNonPost(t *testing.T) {
	h := newTestHandler(t, newTestDeps())
	paths := append(
		[]string{"/network/list"}, networkValidatedRoutes()...,
	)

	for _, path := range paths {
		for _, method := range []string{
			http.MethodGet, http.MethodPut, http.MethodDelete,
		} {
			req := newRequest(t, method, path)
			rec := recordRequest(h, req)

			require.Equal(
				t,
				http.StatusMethodNotAllowed,
				rec.Code,
				"%s %s", method, path,
			)
		}
	}
}
