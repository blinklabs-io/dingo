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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The oversized half of the request bounds is covered at the handler by
// TestRequestBodyLimit and TestRequestBodyAtLimitIsAccepted. The cases
// here need a real socket, because a stalled or truncated body cannot
// be expressed through net/http's client or an httptest recorder: both
// always deliver a complete body.

// testBodyTimeout is short enough to keep the stalled-client case fast
// while staying far above the loopback round trip it measures against.
const testBodyTimeout = 250 * time.Millisecond

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
	_, baseURL := startTestServer(
		t, newTestDeps(), withRequestBodyTimeout(testBodyTimeout),
	)

	conn := dialTestServer(t, baseURL)
	// Declares far more body than it sends, then goes quiet without
	// closing: the connection stays open and silent indefinitely.
	writePartialRequest(
		t, conn, "/network/list", 4096,
		`{"network_identifier":{"blockchain":"cardano"`,
	)

	resp, body := readMeshResponse(t, conn, 30*time.Second)

	requireInvalidRequest(t, resp, body)
}

// TestRequestBodyTruncatedIsRejected covers a body shorter than its
// declared Content-Length, where the client half-closes instead of
// stalling. The read ends in an unexpected EOF rather than a deadline,
// and must produce the same invalid-request error.
func TestRequestBodyTruncatedIsRejected(t *testing.T) {
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

	resp, body := readMeshResponse(t, conn, 30*time.Second)

	requireInvalidRequest(t, resp, body)
}

// TestRequestBodyNormalRequestUnaffected is the control: a well-formed
// request served under the same short deadline must still succeed, so
// the bound cannot be satisfied by rejecting everything.
func TestRequestBodyNormalRequestUnaffected(t *testing.T) {
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
