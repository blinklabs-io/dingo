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

package kupo

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const corsTestOrigin = "https://app.example"

func newCORSTestServer() *Server {
	return New(
		Config{
			ListenAddress:      ":0",
			CORSAllowedOrigins: []string{corsTestOrigin},
		},
		&pagedNode{total: 5},
		nil,
	)
}

func corsRequest(
	server *Server,
	method, target, origin string,
	header map[string]string,
) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	req.Header.Set("Origin", origin)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	server.handler().ServeHTTP(rec, req)
	return rec
}

func exposedHeaders(rec *httptest.ResponseRecorder) map[string]bool {
	ret := map[string]bool{}
	for _, value := range rec.Header().Values("Access-Control-Expose-Headers") {
		for name := range strings.SplitSeq(value, ",") {
			ret[http.CanonicalHeaderKey(strings.TrimSpace(name))] = true
		}
	}
	return ret
}

// TestCORSExposesKupoResponseHeaders pins that a browser script on an
// allowed origin can read every non-safelisted header the Kupo API sets.
func TestCORSExposesKupoResponseHeaders(t *testing.T) {
	t.Parallel()

	rec := corsRequest(
		newCORSTestServer(), http.MethodGet, "/matches",
		corsTestOrigin, nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NotEmpty(t, rec.Header().Get("X-Most-Recent-Checkpoint"))
	require.Equal(t, corsTestOrigin,
		rec.Header().Get("Access-Control-Allow-Origin"))
	exposed := exposedHeaders(rec)
	for _, name := range []string{
		"ETag",
		"Retry-After",
		"X-Block-Header-Hash",
		"X-Most-Recent-Checkpoint",
	} {
		require.True(t, exposed[http.CanonicalHeaderKey(name)], name)
	}
}

func TestCORSDisallowedOriginGetsNoCORSHeaders(t *testing.T) {
	t.Parallel()

	rec := corsRequest(
		newCORSTestServer(), http.MethodGet, "/matches",
		"https://other.example", nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)
	for name := range rec.Header() {
		require.False(
			t,
			strings.HasPrefix(name, "Access-Control-"),
			name,
		)
	}
}

func TestCORSPreflightIsAnsweredBeforeKupoRoutes(t *testing.T) {
	t.Parallel()

	server := newCORSTestServer()
	allowed := corsRequest(server, http.MethodOptions, "/matches",
		corsTestOrigin, map[string]string{
			"Access-Control-Request-Method": http.MethodGet,
		})
	require.Equal(t, http.StatusNoContent, allowed.Code)
	require.Equal(t, corsTestOrigin,
		allowed.Header().Get("Access-Control-Allow-Origin"))
	require.Equal(t, "GET, HEAD, POST, OPTIONS",
		allowed.Header().Get("Access-Control-Allow-Methods"))

	refused := corsRequest(server, http.MethodOptions, "/matches",
		"https://other.example", map[string]string{
			"Access-Control-Request-Method": http.MethodGet,
		})
	require.Equal(t, http.StatusForbidden, refused.Code)
}
