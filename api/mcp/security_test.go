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

package mcp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"net"

	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestSecurityMiddlewareAuth(t *testing.T) {
	t.Parallel()

	nextHandler := http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		},
	)

	// Case 1: No auth required
	unprotected := SecurityMiddleware("", 0, 0, nil, nextHandler)
	req := httptest.NewRequest("GET", "/mcp", nil)
	rec := httptest.NewRecorder()
	unprotected.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Case 2: Auth required
	token := "my-secret-test-token"
	protected := SecurityMiddleware(token, 0, 0, nil, nextHandler)

	// Request with missing token
	req = httptest.NewRequest("GET", "/mcp", nil)
	rec = httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	// Request with wrong token
	req = httptest.NewRequest("GET", "/mcp", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec = httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	// Request with correct Bearer token
	req = httptest.NewRequest("GET", "/mcp", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec = httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Request with correct X-API-Key token
	req = httptest.NewRequest("GET", "/mcp", nil)
	req.Header.Set("X-API-Key", token)
	rec = httptest.NewRecorder()
	protected.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestSecurityMiddlewareRateLimiting(t *testing.T) {
	t.Parallel()

	nextHandler := http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		},
	)

	// Rate limit: 1 rps, burst: 2
	limited := SecurityMiddleware("", 1.0, 2, nil, nextHandler)

	// First 2 requests (within burst) should succeed
	for range 2 {
		req := httptest.NewRequest("GET", "/mcp", nil)
		req.RemoteAddr = "192.0.2.1:1234"
		rec := httptest.NewRecorder()
		limited.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	}

	// Third immediate request should be rate-limited (429)
	req := httptest.NewRequest("GET", "/mcp", nil)
	req.RemoteAddr = "192.0.2.1:1234"
	rec := httptest.NewRecorder()
	limited.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusTooManyRequests, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("Retry-After"))
}

func TestSecurityMiddlewareCORS(t *testing.T) {
	t.Parallel()

	nextHandler := http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		},
	)

	handler := SecurityMiddleware(
		"",
		0,
		0,
		[]string{"http://localhost:5173"},
		nextHandler,
	)

	// Preflight OPTIONS
	req := httptest.NewRequest("OPTIONS", "/mcp", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(
		t,
		"http://localhost:5173",
		rec.Header().Get("Access-Control-Allow-Origin"),
	)
	assert.Contains(t, rec.Header().Get("Access-Control-Allow-Methods"), "POST")
}

func TestSecurityMiddlewareBrowserOrigins(t *testing.T) {
	t.Parallel()
	for _, allowed := range [][]string{nil, {"*"}, {"http://trusted.example"}} {
		for _, path := range []string{"/", "/mcp", "/mcp/", "/sse", "/sse/", "/healthz"} {
			for _, method := range []string{"GET", "POST", "OPTIONS"} {
				for _, origin := range []string{"https://evil.example", "null", "", "http://trusted.example/", "http://trusted.example https://evil.example"} {
					invoked := false
					handler := SecurityMiddleware(
						"",
						0,
						0,
						allowed,
						http.HandlerFunc(
							func(http.ResponseWriter, *http.Request) { invoked = true },
						),
					)
					req := httptest.NewRequest(
						method,
						"http://localhost:8088"+path,
						nil,
					)
					req.Header.Set("Origin", origin)
					rec := httptest.NewRecorder()
					handler.ServeHTTP(rec, req)
					require.Equal(
						t,
						http.StatusForbidden,
						rec.Code,
						"%s %s %q",
						method,
						path,
						origin,
					)
					require.False(t, invoked)
					require.Empty(
						t,
						rec.Header().Get("Access-Control-Allow-Origin"),
					)
				}
			}
		}
	}
	for _, origin := range []string{"", "http://localhost:8088", "http://trusted.example"} {
		handler := SecurityMiddleware(
			"secret",
			0,
			0,
			[]string{"http://trusted.example"},
			http.HandlerFunc(
				func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) },
			),
		)
		req := httptest.NewRequest("POST", "http://localhost:8088/mcp", nil)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, 401, rec.Code)
		req.Header.Set("Authorization", "Bearer secret")
		rec = httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, 200, rec.Code)
		req.Header.Add("Origin", "http://trusted.example")
		if origin != "" {
			rec = httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			require.Equal(t, 403, rec.Code)
		}
	}
}

func TestServerRejectsReboundOrigin(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"/mcp", "/sse", "/"} {
		server, err := NewServer(
			DefaultProviderConfig(),
			ProviderDependencies{},
			apiconfig.EffectiveTLS{},
			"0.0.0.0:8088",
		)
		require.NoError(t, err)
		req := httptest.NewRequest(
			"POST",
			"http://evil.example:8088"+path,
			strings.NewReader(
				`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}`,
			),
		)
		req.Header.Set("Origin", "http://evil.example:8088")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req = req.WithContext(
			context.WithValue(
				req.Context(),
				http.LocalAddrContextKey,
				&net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 8088},
			),
		)
		rec := httptest.NewRecorder()
		server.handler().ServeHTTP(rec, req)
		require.Equal(t, 403, rec.Code)
		require.Empty(t, rec.Header().Get("Mcp-Session-Id"))
	}
}

func TestClientLimiterEvictsIdleEntries(t *testing.T) {
	t.Parallel()
	limiter := newClientLimiter(rate.Limit(1), 1)
	first := limiter.getLimiter("old")
	limiter.lastSeen["old"] = time.Now().Add(-time.Hour)
	limiter.nextCleanup = time.Time{}
	limiter.getLimiter("new")
	require.NotContains(t, limiter.limiters, "old")
	require.NotSame(t, first, limiter.getLimiter("old"))
}

func TestMCPRejectsReboundHostWithoutOrigin(t *testing.T) {
	t.Parallel()
	server := &Server{
		mcpServer: mcp.NewServer(
			&mcp.Implementation{Name: "test", Version: "1"},
			nil,
		),
	}
	ts := httptest.NewServer(server.handler())
	t.Cleanup(ts.Close)
	for _, path := range []string{"/mcp", "/sse", "/"} {
		req, err := http.NewRequestWithContext(
			t.Context(),
			http.MethodGet,
			ts.URL+path,
			nil,
		)
		require.NoError(t, err)
		req.Host = "evil.example"
		resp, err := ts.Client().Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusForbidden, resp.StatusCode)
	}
}

func TestSanitizeText(t *testing.T) {
	t.Parallel()

	// Zero-width and RTL override stripping
	malicious := "Hello\u202EWorld\u200BTest\uFEFF!"
	sanitized := SanitizeExternalString(malicious)
	assert.NotContains(t, sanitized, "\u202E")
	assert.NotContains(t, sanitized, "\u200B")
	assert.NotContains(t, sanitized, "\uFEFF")
	assert.Contains(t, sanitized, "HelloWorldTest!")

	// Markdown pipe and newline escaping in cell
	cell := "First\nSecond|Third"
	formatted := FormatCell(cell)
	assert.NotContains(t, formatted, "\n")
	assert.Contains(t, formatted, `\|`)

	// External string wrapper
	tagged := WrapUntrustedChainData("evil prompt injection")
	assert.True(t, strings.HasPrefix(tagged, "<untrusted_chain_data>"))
	assert.True(t, strings.HasSuffix(tagged, "</untrusted_chain_data>"))
}

func TestFormatCell(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "NULL", FormatCell(nil))
	assert.Equal(t, "123", FormatCell(int64(123)))
	assert.Equal(t, "12.34", FormatCell(float64(12.34)))
	assert.Equal(t, "true", FormatCell(true))
	assert.Equal(t, "false", FormatCell(false))
	assert.Equal(t, "0x", FormatCell([]byte{}))
	assert.Equal(t, "0x68656c6c6f", FormatCell([]byte("hello")))
	assert.Equal(t, "0xdeadbeef", FormatCell([]byte{0xde, 0xad, 0xbe, 0xef}))

	// Large blob preview
	largeBlob := make([]byte, 100)
	for i := range largeBlob {
		largeBlob[i] = byte(i)
	}
	blobFormatted := FormatCell(largeBlob)
	assert.Contains(t, blobFormatted, "bytes: 100")
	assert.Contains(t, blobFormatted, "...")

	// String truncation
	longString := strings.Repeat("x", 200)
	assert.Equal(t, strings.Repeat("x", 128)+"...", FormatCell(longString))

	// Markdown pipe and newline escaping
	assert.Equal(t, "a \\| b c", FormatCell("a | b\nc"))

	// Large default type truncation
	largeDefault := strings.Repeat("z", 200)
	formattedDefault := FormatCell([]rune(largeDefault))
	assert.True(t, strings.HasSuffix(formattedDefault, "..."))
	assert.Equal(t, 128+3, len(formattedDefault))
}

func TestFormatMarkdownTable(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "*(empty result)*\n", FormatMarkdownTable(nil, nil))
	assert.Equal(t, "*(empty result)*\n", FormatMarkdownTable([]string{}, nil))

	tbl := FormatMarkdownTable([]string{"ColA", "ColB"}, [][]string{{"1", "2"}})
	assert.Contains(t, tbl, "| ColA | ColB |")
	assert.Contains(t, tbl, "| 1 | 2 |")
}

func TestFormatHash(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "<nil>", formatHash(nil))
	assert.Equal(t, "abc", formatHash("abc"))
	assert.Equal(t, "0102", formatHash([]byte{1, 2}))
	assert.Equal(t, "123", formatHash(123))
}
