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

package mesh

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/mempool"
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	"github.com/stretchr/testify/require"
)

// --- construction validation -------------------------------------------

// TestNewServerRequiresDependencies asserts every dependency the
// handlers dereference is validated up front, so a misconfigured node
// fails at startup rather than on the first request.
func TestNewServerRequiresDependencies(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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

	// A failed start must leave the server restartable rather than holding a
	// server it never brought up, so a retry once the address frees up is not
	// refused as "already started".
	require.NoError(t, occupied.Close())
	require.NoError(t, srv.Start(t.Context()))
	require.NoError(t, srv.Stop(t.Context()))
}

// TestServerDoubleStart asserts a second Start is refused rather than
// leaking the first listener.
func TestServerDoubleStart(t *testing.T) {
	t.Parallel()

	srv, _ := startTestServer(t, newTestDeps())

	err := srv.Start(t.Context())

	require.ErrorContains(t, err, "already started")
}

// TestServerStopIsIdempotent asserts Stop on a server that was never
// started, or stopped twice, is a no-op rather than an error.
func TestServerStopIsIdempotent(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, newTestDeps())

	require.NoError(t, srv.Stop(t.Context()))
	require.NoError(t, srv.Stop(t.Context()))
}

// TestServerGracefulShutdown asserts Stop releases the listener before it
// returns by immediately starting a replacement on the same address.
func TestServerGracefulShutdown(t *testing.T) {
	t.Parallel()

	srv, addr := startOnFreePort(
		t, t.Context(), newTestDeps(),
	)

	stopCtx, cancel := context.WithTimeout(
		t.Context(), 5*time.Second,
	)
	defer cancel()
	require.NoError(t, srv.Stop(stopCtx))

	restarted := newTestServer(
		t,
		newTestDeps(),
		func(c *ServerConfig) { c.ListenAddress = addr },
	)
	require.NoError(t, restarted.Start(t.Context()))
	require.NoError(t, restarted.Stop(t.Context()))
}

// TestConcurrentStartStopLeavesTheMeshServerStopped exercises Mesh's Start,
// competing Stops, and context monitor. The listener package checks closure of
// the owned socket directly; this test checks that Mesh unpublishes its server.
func TestConcurrentStartStopLeavesTheMeshServerStopped(t *testing.T) {
	t.Parallel()

	for i := range 60 {
		srv := newTestServer(
			t, newTestDeps(),
			func(c *ServerConfig) { c.ListenAddress = "127.0.0.1:0" },
		)
		ctx, cancel := context.WithCancel(context.Background())

		var (
			wg       sync.WaitGroup
			startErr error
			stopErrs = make([]error, 2)
		)
		wg.Add(4)
		go func() {
			defer wg.Done()
			startErr = srv.Start(ctx)
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

		require.NoError(t, startErr, "iteration %d", i)
		require.NoError(t, stopErrs[0], "first Stop, iteration %d", i)
		require.NoError(t, stopErrs[1], "second Stop, iteration %d", i)
		require.NoError(
			t, srv.Stop(t.Context()),
			"a second Stop must stay clean (iteration %d)", i,
		)
		require.Nil(
			t, srv.listener.Server(),
			"a clean Stop must unpublish the Mesh server (iteration %d)", i,
		)
	}
}

// TestServerShutdownOnContextCancel asserts cancelling the context
// passed to Start shuts the listener down, which is how the node stops
// the API during its own shutdown.
func TestServerShutdownOnContextCancel(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
// ReadTimeout is the backstop for a request whose body no handler
// reads, which the per-request deadline in decodeRequest never sees.
func TestServerTimeoutsAreConfigured(t *testing.T) {
	t.Parallel()

	srv, _ := startTestServer(t, newTestDeps())

	httpServer := srv.listener.Server()

	require.NotNil(t, httpServer)
	require.Equal(
		t, 60*time.Second, httpServer.ReadHeaderTimeout,
	)
	require.Equal(
		t, listenerReadTimeout, httpServer.ReadTimeout,
	)
	require.Positive(t, httpServer.ReadTimeout)
	require.Equal(t, 30*time.Second, httpServer.WriteTimeout)
	require.Equal(t, 120*time.Second, httpServer.IdleTimeout)
}

// TestDefaultRequestBodyTimeoutIsApplied asserts a server built
// without an explicit body deadline still gets one, so the bound
// cannot be lost by composition code that never sets it.
func TestDefaultRequestBodyTimeoutIsApplied(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, newTestDeps())

	require.Equal(
		t, defaultRequestBodyTimeout, srv.config.requestBodyTimeout,
	)
}

// --- routing ------------------------------------------------------------

// TestUnknownRouteIsNotFound asserts an unregistered path does not fall
// through to a handler.
func TestUnknownRouteIsNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t, newTestDeps())

	rec := postRaw(t, h, "/does/not/exist", "{}")

	require.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRoutesRejectNonPost asserts every Mesh endpoint is POST-only, as
// the Rosetta specification requires.
func TestRoutesRejectNonPost(t *testing.T) {
	t.Parallel()

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

// TestStartIsRefusedWhileAnotherStartHoldsTheGate pins the start gate this
// package is wired to, so BeginStart/EndStart cannot be removed from Start
// without the suite failing.
func TestStartIsRefusedWhileAnotherStartHoldsTheGate(t *testing.T) {
	t.Parallel()

	srv := newTestServer(
		t, newTestDeps(),
		func(c *ServerConfig) { c.ListenAddress = testutil.FreePort(t) },
	)

	held, err := srv.listener.BeginStart()
	require.NoError(t, err)

	err = srv.Start(t.Context())
	require.ErrorContains(
		t, err, "start already in progress",
		"Start must take the listener's start gate before publishing",
	)
	require.Nil(
		t, srv.listener.Server(),
		"a refused Start must not publish a server",
	)

	srv.listener.EndStart(held)
	require.NoError(
		t, srv.Start(t.Context()),
		"the gate must be available again once the holder releases it",
	)
	require.NoError(t, srv.Stop(t.Context()))
}

// The oversized half of the request bounds is covered at the handler by
// TestRequestBodyLimit and TestRequestBodyAtLimitIsAccepted. The cases
// here need a real socket, because a stalled or truncated body cannot
// be expressed through net/http's client or an httptest recorder: both
// always deliver a complete body.

// testBodyTimeout is short enough to keep the stalled-client case fast
// while staying far above the loopback round trip it measures against.
const testBodyTimeout = 250 * time.Millisecond

// respondWithin bounds how long a bounded handler may take to answer.
// It has to clear loopback and CI scheduling noise, but it also has to
// stay well under both listenerReadTimeout and any plausible
// regression of the body deadline: an allowance of tens of seconds
// would pass a deadline that had grown to seconds, or been dropped
// onto the listener backstop, which is the regression these cases
// exist to catch.
const respondWithin = 20 * testBodyTimeout

// withRequestBodyTimeout sets the per-request body deadline so a test
// need not wait out the production default.
func withRequestBodyTimeout(d time.Duration) serverOption {
	return func(c *ServerConfig) { c.requestBodyTimeout = d }
}

// dialTestServer opens a raw connection to a running test server, so a
// test can control exactly how much of a request body reaches it.
func dialTestServer(t *testing.T, baseURL string) net.Conn {
	t.Helper()
	addr := strings.TrimPrefix(baseURL, "http://")
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// writePartialRequest sends a complete request head declaring
// contentLength, followed by only the supplied prefix of the body.
func writePartialRequest(
	t *testing.T,
	conn net.Conn,
	path string,
	contentLength int,
	bodyPrefix string,
) {
	t.Helper()
	head := fmt.Sprintf(
		"POST %s HTTP/1.1\r\n"+
			"Host: mesh.test\r\n"+
			"Content-Type: application/json\r\n"+
			"Content-Length: %d\r\n"+
			"\r\n",
		path, contentLength,
	)
	_, err := io.WriteString(conn, head+bodyPrefix)
	require.NoError(t, err)
}

// readMeshResponse reads one HTTP response from conn, failing the test
// if none arrives within limit. A handler still waiting on the body
// therefore fails as a read timeout here rather than hanging the test
// binary until the package deadline.
func readMeshResponse(
	t *testing.T,
	conn net.Conn,
	limit time.Duration,
) (*http.Response, []byte) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(limit)))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	require.NoError(
		t, err,
		"no response within %s: the handler is still waiting on the body",
		limit,
	)
	t.Cleanup(func() { _ = resp.Body.Close() })
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp, body
}

// requireInvalidRequest asserts the wire response is the existing Mesh
// invalid-request error, so a bounded body read reports the error
// callers already handle rather than a new one.
func requireInvalidRequest(
	t *testing.T,
	resp *http.Response,
	body []byte,
) {
	t.Helper()
	require.Equal(
		t, http.StatusBadRequest, resp.StatusCode,
		"unexpected status, body: %s", body,
	)
	var got Error
	require.NoError(t, json.Unmarshal(body, &got))
	require.Equal(t, ErrInvalidRequest.Code, got.Code)
	require.Equal(t, ErrInvalidRequest.Message, got.Message)
	require.Equal(t, ErrInvalidRequest.Retriable, got.Retriable)
}

// TestRequestBodyStalledClientIsBounded covers the slow-client case: a
// client that sends a complete request head and then stops partway
// through the declared body must not hold the handler indefinitely.
// The byte cap cannot fire here, because the client never sends enough
// bytes to reach it.
func TestRequestBodyStalledClientIsBounded(t *testing.T) {
	t.Parallel()

	_, baseURL := startTestServer(
		t, newTestDeps(), withRequestBodyTimeout(testBodyTimeout),
	)

	conn := dialTestServer(t, baseURL)
	// Declares far more body than it sends, then goes quiet without
	// closing: the connection stays open and silent indefinitely.
	start := time.Now()
	writePartialRequest(
		t, conn, "/network/list", 4096,
		`{"network_identifier":{"blockchain":"cardano"`,
	)

	resp, body := readMeshResponse(t, conn, respondWithin)
	elapsed := time.Since(start)

	requireInvalidRequest(t, resp, body)
	// The deadline is what ended this read. An answer arriving before
	// it could not have come from waiting on the body, so it would
	// mean the request failed for some other reason and the case was
	// no longer exercising the bound it names.
	require.GreaterOrEqual(t, elapsed, testBodyTimeout)
}

// TestRequestBodyTruncatedIsRejected covers a body shorter than its
// declared Content-Length, where the client half-closes instead of
// stalling. The read ends in an unexpected EOF rather than a deadline,
// and must produce the same invalid-request error.
func TestRequestBodyTruncatedIsRejected(t *testing.T) {
	t.Parallel()

	_, baseURL := startTestServer(
		t, newTestDeps(), withRequestBodyTimeout(testBodyTimeout),
	)

	conn := dialTestServer(t, baseURL)
	writePartialRequest(
		t, conn, "/network/list", 4096,
		`{"network_identifier":{"blockchain":"cardano"`,
	)
	tcpConn, ok := conn.(*net.TCPConn)
	require.True(t, ok)
	require.NoError(t, tcpConn.CloseWrite())

	resp, body := readMeshResponse(t, conn, respondWithin)

	requireInvalidRequest(t, resp, body)
}

// TestRequestBodyNormalRequestUnaffected is the control: a well-formed
// request served under the same short deadline must still succeed, so
// the bound cannot be satisfied by rejecting everything.
func TestRequestBodyNormalRequestUnaffected(t *testing.T) {
	t.Parallel()

	_, baseURL := startTestServer(
		t, newTestDeps(), withRequestBodyTimeout(testBodyTimeout),
	)

	raw, err := json.Marshal(MetadataRequest{})
	require.NoError(t, err)
	resp, err := http.Post(
		baseURL+"/network/list",
		"application/json",
		bytes.NewReader(raw),
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

// A complete first value does not complete the declared HTTP body.
func TestRequestBodyStalledAfterJSONIsRejected(t *testing.T) {
	_, baseURL := startTestServer(
		t, newTestDeps(), withRequestBodyTimeout(testBodyTimeout),
	)
	conn := dialTestServer(t, baseURL)
	writePartialRequest(t, conn, "/network/list", 4096, "{}")
	resp, body := readMeshResponse(t, conn, respondWithin)
	requireInvalidRequest(t, resp, body)
}

// Fixed network identity shared by every test in the package so that
// requests can be built without threading configuration through each
// helper. The magic deliberately differs from mainnetMagic so the
// default server under test uses testnet-prefixed addresses.
const (
	testNetwork      = "preview"
	testNetworkMagic = uint32(2)
	testGenesisHash  = "268ae601af9f5e0d5e0a3e8f8a3a19d0a2ac6b93" +
		"b6f5d8e3c8fbc9d1a2b3c4d5"
	testGenesisStartTimeSec = int64(1666656000)
)

// --- dependency doubles -------------------------------------------------
//
// Each double implements one of the package-local dependency interfaces
// from node_interface.go. Behavior is supplied per test through function
// fields; a nil field means "return the zero value", which keeps tests
// that only care about one dependency free of unrelated setup.

// fakeChain is a MeshChain returning a fixed tip.
type fakeChain struct {
	tip ochainsync.Tip
}

func (f *fakeChain) Tip() ochainsync.Tip { return f.tip }

// fakeDatabase is a MeshDatabase whose lookups are supplied per test.
type fakeDatabase struct {
	blockByHash    func(hash []byte) (models.Block, error)
	blockByIndex   func(idx uint64) (models.Block, error)
	txByHash       func(hash []byte) (*models.Transaction, error)
	txsByBlockHash func(hash []byte) ([]models.Transaction, error)
}

func (f *fakeDatabase) BlockByHash(
	hash []byte,
) (models.Block, error) {
	if f.blockByHash == nil {
		return models.Block{}, models.ErrBlockNotFound
	}
	return f.blockByHash(hash)
}

func (f *fakeDatabase) BlockByIndex(
	idx uint64,
) (models.Block, error) {
	if f.blockByIndex == nil {
		return models.Block{}, models.ErrBlockNotFound
	}
	return f.blockByIndex(idx)
}

func (f *fakeDatabase) GetTransactionByHash(
	hash []byte,
) (*models.Transaction, error) {
	if f.txByHash == nil {
		return nil, nil
	}
	return f.txByHash(hash)
}

func (f *fakeDatabase) GetTransactionsByBlockHash(
	hash []byte,
) ([]models.Transaction, error) {
	if f.txsByBlockHash == nil {
		return nil, nil
	}
	return f.txsByBlockHash(hash)
}

// fakeLedgerState is a MeshLedgerState with per-test behavior.
type fakeLedgerState struct {
	pparams     lcommon.ProtocolParameters
	slotToTime  func(slot uint64) (time.Time, error)
	utxos       func(addrs []lcommon.Address) ([]models.Utxo, error)
	utxosAtSlot func(
		addr lcommon.Address,
		slot uint64,
	) ([]models.Utxo, error)
}

func (f *fakeLedgerState) GetCurrentPParams() lcommon.ProtocolParameters {
	return f.pparams
}

func (f *fakeLedgerState) GetCurrentPParamsForReporting() lcommon.ProtocolParameters {
	return f.pparams
}

func (f *fakeLedgerState) SlotToTime(
	slot uint64,
) (time.Time, error) {
	if f.slotToTime == nil {
		// Mirror the ledger's Shelley-era 1s slots so handlers that
		// do not exercise the fallback path get stable timestamps.
		return time.Unix(
			testGenesisStartTimeSec+int64(slot), 0,
		).UTC(), nil
	}
	return f.slotToTime(slot)
}

func (f *fakeLedgerState) UtxosByAddress(
	addrs []lcommon.Address,
) ([]models.Utxo, error) {
	if f.utxos == nil {
		return nil, nil
	}
	return f.utxos(addrs)
}

func (f *fakeLedgerState) UtxosByAddressAtSlot(
	addr lcommon.Address,
	slot uint64,
) ([]models.Utxo, error) {
	if f.utxosAtSlot == nil {
		return nil, nil
	}
	return f.utxosAtSlot(addr, slot)
}

// submittedTx records one accepted MeshMempool.AddTransaction call.
type submittedTx struct {
	txType  uint
	txBytes []byte
}

// fakeMempool is a MeshMempool backed by an in-memory transaction list.
// Submissions are recorded so tests can assert what reached the mempool.
type fakeMempool struct {
	mu        sync.Mutex
	txs       []mempool.MempoolTransaction
	addErr    error
	submitted []submittedTx
}

func (f *fakeMempool) AddTransaction(
	txType uint,
	txBytes []byte,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.addErr != nil {
		return f.addErr
	}
	f.submitted = append(f.submitted, submittedTx{
		txType:  txType,
		txBytes: bytes.Clone(txBytes),
	})
	return nil
}

func (f *fakeMempool) GetTransaction(
	hash string,
) (mempool.MempoolTransaction, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, tx := range f.txs {
		if tx.Hash == hash {
			return tx, true
		}
	}
	return mempool.MempoolTransaction{}, false
}

func (f *fakeMempool) Transactions() []mempool.MempoolTransaction {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append(
		[]mempool.MempoolTransaction(nil), f.txs...,
	)
}

func (f *fakeMempool) submissions() []submittedTx {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]submittedTx(nil), f.submitted...)
}

// --- server construction ------------------------------------------------

// testDeps bundles the doubles handed to a test server so a test can
// reach into them after wiring.
type testDeps struct {
	chain    *fakeChain
	database *fakeDatabase
	ledger   *fakeLedgerState
	mempool  *fakeMempool
}

// newTestDeps returns doubles with no configured behavior.
func newTestDeps() *testDeps {
	return &testDeps{
		chain:    &fakeChain{},
		database: &fakeDatabase{},
		ledger:   &fakeLedgerState{},
		mempool:  &fakeMempool{},
	}
}

// serverOption mutates the ServerConfig before NewServer is called.
type serverOption func(*ServerConfig)

// newTestServer builds a Server over the supplied doubles. It fails the
// test if construction fails, so callers testing validation errors should
// call NewServer directly.
func newTestServer(
	t *testing.T,
	deps *testDeps,
	opts ...serverOption,
) *Server {
	t.Helper()
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
	for _, opt := range opts {
		opt(&cfg)
	}
	srv, err := NewServer(cfg)
	require.NoError(t, err)
	return srv
}

// newTestHandler returns the routed handler for a server built over the
// supplied doubles, plus the doubles themselves.
func newTestHandler(
	t *testing.T,
	deps *testDeps,
	opts ...serverOption,
) http.Handler {
	t.Helper()
	srv := newTestServer(t, deps, opts...)
	mux := http.NewServeMux()
	srv.registerRoutes(mux)
	return mux
}

// --- request helpers ----------------------------------------------------

// testNetworkID returns the NetworkIdentifier accepted by a test server.
func testNetworkID() *NetworkIdentifier {
	return &NetworkIdentifier{
		Blockchain: blockchain,
		Network:    testNetwork,
	}
}

// postJSON marshals body and posts it to path.
func postJSON(
	t *testing.T,
	h http.Handler,
	path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	return postRaw(t, h, path, string(raw))
}

// postRaw posts an unmarshaled request body, for malformed-input cases.
func postRaw(
	t *testing.T,
	h http.Handler,
	path string,
	body string,
) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(
		http.MethodPost, path, bytes.NewBufferString(body),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// decodeResponse decodes a successful JSON response into T, asserting
// the 200 status first so a failure reports the endpoint's error body.
func decodeResponse[T any](
	t *testing.T,
	rec *httptest.ResponseRecorder,
) *T {
	t.Helper()
	require.Equal(
		t, http.StatusOK, rec.Code,
		"unexpected status, body: %s", rec.Body.String(),
	)
	require.Equal(
		t,
		"application/json",
		rec.Header().Get("Content-Type"),
	)
	var out T
	require.NoError(
		t, json.Unmarshal(rec.Body.Bytes(), &out),
	)
	return &out
}

// requireMeshError asserts that the response is the given Mesh error
// with the given HTTP status, and returns the decoded error so callers
// can inspect its details.
func requireMeshError(
	t *testing.T,
	rec *httptest.ResponseRecorder,
	want *Error,
	wantStatus int,
) *Error {
	t.Helper()
	require.Equal(
		t, wantStatus, rec.Code,
		"unexpected status, body: %s", rec.Body.String(),
	)
	var got Error
	require.NoError(
		t, json.Unmarshal(rec.Body.Bytes(), &got),
	)
	require.Equal(t, want.Code, got.Code)
	require.Equal(t, want.Message, got.Message)
	require.Equal(t, want.Retriable, got.Retriable)
	return &got
}

// --- fixture builders ---------------------------------------------------

// mustDecodeHex decodes a hex string or fails the test.
func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

// hexString hex-encodes bytes for comparison against response fields.
func hexString(b []byte) string { return hex.EncodeToString(b) }

// testHash returns a deterministic 32-byte hash seeded by b.
func testHash(b byte) []byte {
	h := make([]byte, 32)
	for i := range h {
		h[i] = b
	}
	return h
}

// testKeyHash returns a deterministic 28-byte credential hash.
func testKeyHash(b byte) []byte {
	h := make([]byte, 28)
	for i := range h {
		h[i] = b
	}
	return h
}

// testAddress builds a bech32 testnet address from raw credentials.
func testAddress(
	t *testing.T,
	addrType uint8,
	payment []byte,
	staking []byte,
) string {
	t.Helper()
	addr, err := lcommon.NewAddressFromParts(
		addrType,
		lcommon.AddressNetworkTestnet,
		payment,
		staking,
	)
	require.NoError(t, err)
	return addr.String()
}

// testUtxo builds a UTxO owned by a key-only testnet address.
func testUtxo(
	txID []byte,
	idx uint32,
	lovelace uint64,
	paymentKey []byte,
	assets []models.Asset,
) models.Utxo {
	return models.Utxo{
		TxId:       txID,
		OutputIdx:  idx,
		Amount:     types.Uint64(lovelace),
		PaymentKey: paymentKey,
		Assets:     assets,
	}
}

// testAsset builds a native asset holding.
func testAsset(
	policyID []byte,
	name []byte,
	amount uint64,
) models.Asset {
	return models.Asset{
		PolicyId: policyID,
		Name:     name,
		Amount:   types.Uint64(amount),
	}
}

// --- transaction fixtures -----------------------------------------------

// testTxBody encodes a minimal Conway transaction body spending the
// given inputs into the given outputs. It mirrors the body shape
// produced by /construction/payloads so fixtures and the construction
// flow stay in agreement.
func testTxBody(
	t *testing.T,
	inputs []shelley.ShelleyTransactionInput,
	outputs []babbage.BabbageTransactionOutput,
	fee uint64,
) []byte {
	t.Helper()
	body := conway.ConwayTransactionBody{
		TxInputs: conway.NewConwayTransactionInputSet(
			inputs,
		),
		TxOutputs: outputs,
		TxFee:     fee,
	}
	bodyCbor, err := cbor.Encode(&body)
	require.NoError(t, err)
	return bodyCbor
}

// testSignedTx wraps an encoded body in the signed-transaction envelope
// [body, witness_set, is_valid, auxiliary_data].
func testSignedTx(
	t *testing.T,
	bodyCbor []byte,
	witnesses []lcommon.VkeyWitness,
) []byte {
	t.Helper()
	signed := []any{
		cbor.RawMessage(bodyCbor),
		map[int]any{0: witnesses},
		true,
		nil,
	}
	txCbor, err := cbor.Encode(signed)
	require.NoError(t, err)
	return txCbor
}

// testOutput builds a lovelace-only transaction output.
func testOutput(
	t *testing.T,
	address string,
	lovelace uint64,
) babbage.BabbageTransactionOutput {
	t.Helper()
	addr, err := lcommon.NewAddress(address)
	require.NoError(t, err)
	return babbage.BabbageTransactionOutput{
		OutputAddress: addr,
		OutputAmount: mary.MaryTransactionOutputValue{
			Amount: lovelace,
		},
	}
}

// testKeyPair returns a deterministic ed25519 key pair. The seed byte
// keeps separate signers distinguishable within a test.
func testKeyPair(
	t *testing.T,
	seed byte,
) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	seedBytes := make([]byte, ed25519.SeedSize)
	for i := range seedBytes {
		seedBytes[i] = seed
	}
	priv := ed25519.NewKeyFromSeed(seedBytes)
	pub, ok := priv.Public().(ed25519.PublicKey)
	require.True(t, ok)
	return pub, priv
}

// testSignerSeed is the key seed testSimpleSignedTx signs with. Tests
// asserting on the resulting signer identity derive the key from this
// constant rather than repeating the value.
const testSignerSeed = byte(0x42)

// testSimpleSignedTx builds a signed single-input, single-output
// transaction and returns its CBOR alongside the decoded transaction.
func testSimpleSignedTx(
	t *testing.T,
	address string,
) ([]byte, gledger.Transaction) {
	t.Helper()
	pub, _ := testKeyPair(t, testSignerSeed)
	bodyCbor := testTxBody(
		t,
		[]shelley.ShelleyTransactionInput{
			shelley.NewShelleyTransactionInput(
				hexString(testHash(0xd1)), 0,
			),
		},
		[]babbage.BabbageTransactionOutput{
			testOutput(t, address, 1_000_000),
		},
		170_000,
	)
	txCbor := testSignedTx(
		t,
		bodyCbor,
		[]lcommon.VkeyWitness{
			{
				Vkey:      pub,
				Signature: make([]byte, 64),
			},
		},
	)
	txType, err := gledger.DetermineTransactionType(txCbor)
	require.NoError(t, err)
	tx, err := gledger.NewTransactionFromCbor(txType, txCbor)
	require.NoError(t, err)
	return txCbor, tx
}

// testTxBodyWithCerts encodes a Conway body carrying certificates
// alongside a single input and output, for exercising the certificate
// paths in the operation converter.
func testTxBodyWithCerts(
	t *testing.T,
	address string,
	certs []lcommon.CertificateWrapper,
) []byte {
	t.Helper()
	body := conway.ConwayTransactionBody{
		TxInputs: conway.NewConwayTransactionInputSet(
			[]shelley.ShelleyTransactionInput{
				shelley.NewShelleyTransactionInput(
					hexString(testHash(0xd2)), 0,
				),
			},
		),
		TxOutputs: []babbage.BabbageTransactionOutput{
			testOutput(t, address, 1_000_000),
		},
		TxFee:          170_000,
		TxCertificates: certs,
	}
	bodyCbor, err := cbor.Encode(&body)
	require.NoError(t, err)
	return bodyCbor
}

// ed25519Sign signs message with an ed25519 private key.
func ed25519Sign(priv []byte, message []byte) []byte {
	return ed25519.Sign(ed25519.PrivateKey(priv), message)
}

// testPParams returns Conway protocol parameters with every rational
// field populated, so conversion to the utxorpc representation (which
// rejects nil rationals) succeeds.
func testPParams(
	minFeeA uint,
	minFeeB uint,
) *conway.ConwayProtocolParameters {
	rat := func(num, denom int64) *cbor.Rat {
		return &cbor.Rat{Rat: big.NewRat(num, denom)}
	}
	return &conway.ConwayProtocolParameters{
		MinFeeA:            minFeeA,
		MinFeeB:            minFeeB,
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
		KeyDeposit:         2_000_000,
		PoolDeposit:        500_000_000,
		MaxEpoch:           18,
		NOpt:               150,
		A0:                 rat(3, 10),
		Rho:                rat(3, 1000),
		Tau:                rat(2, 10),
		ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
			Major: 10,
		},
		MinPoolCost:    170_000_000,
		AdaPerUtxoByte: 4310,
		ExecutionCosts: lcommon.ExUnitPrice{
			MemPrice:  rat(577, 10000),
			StepPrice: rat(721, 10000000),
		},
		MaxTxExUnits: lcommon.ExUnits{
			Memory: 14_000_000,
			Steps:  10_000_000_000,
		},
		MaxBlockExUnits: lcommon.ExUnits{
			Memory: 62_000_000,
			Steps:  20_000_000_000,
		},
		MaxValueSize:               5000,
		CollateralPercentage:       150,
		MaxCollateralInputs:        3,
		MinFeeRefScriptCostPerByte: rat(15, 1),
	}
}

// newRequest builds a request with no body for method assertions.
func newRequest(
	t *testing.T,
	method string,
	path string,
) *http.Request {
	t.Helper()
	return httptest.NewRequest(method, path, nil)
}

// recordRequest serves req against h and returns the recorder.
func recordRequest(
	h http.Handler,
	req *http.Request,
) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// newDiscardWriter returns a writer that drops everything written to
// it, for silencing component loggers in tests.
func newDiscardWriter() io.Writer { return io.Discard }

func TestMeshAnonymousPlaintextTLSAndCORS(t *testing.T) {
	t.Parallel()
	const origin = "https://wallet.example"
	cert, key := testutil.GenerateTestTLSCertKey(t)
	for _, tc := range []struct {
		name string
		tls  apiconfig.EffectiveTLS
		url  string
		cli  *http.Client
	}{
		{"plaintext", apiconfig.EffectiveTLS{}, "http://", http.DefaultClient},
		{"tls", apiconfig.EffectiveTLS{Enabled: true, CertFilePath: cert, KeyFilePath: key}, "https://", testutil.InsecureHTTPClient()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			t.Cleanup(cancel)
			client := *tc.cli
			client.Timeout = 5 * time.Second
			srv, addr := startOnFreePort(
				t,
				ctx,
				newTestDeps(),
				func(c *ServerConfig) { c.TLS = tc.tls; c.CORSAllowedOrigins = []string{origin} },
			)
			t.Cleanup(func() {
				stopCtx, stopCancel := context.WithTimeout(
					context.Background(),
					5*time.Second,
				)
				defer stopCancel()
				require.NoError(t, srv.Stop(stopCtx))
			})
			body := `{"network_identifier":{"blockchain":"cardano","network":"testnet"}}`
			req, err := http.NewRequestWithContext(
				ctx,
				http.MethodPost,
				tc.url+addr+"/network/list",
				strings.NewReader(body),
			)
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Origin", origin)
			resp, err := client.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(
				t,
				origin,
				resp.Header.Get("Access-Control-Allow-Origin"),
			)
			preflight, err := http.NewRequestWithContext(
				ctx,
				http.MethodOptions,
				tc.url+addr+"/network/list",
				nil,
			)
			require.NoError(t, err)
			preflight.Header.Set("Origin", origin)
			preflight.Header.Set(
				"Access-Control-Request-Method",
				http.MethodPost,
			)
			preflight.Header.Set(
				"Access-Control-Request-Headers",
				"Content-Type",
			)
			corsResp, err := client.Do(preflight)
			require.NoError(t, err)
			defer corsResp.Body.Close()
			require.Equal(t, http.StatusNoContent, corsResp.StatusCode)
			require.Equal(
				t,
				origin,
				corsResp.Header.Get("Access-Control-Allow-Origin"),
			)
		})
	}
}
