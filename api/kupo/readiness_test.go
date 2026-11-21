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
	"regexp"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifyRoute(t *testing.T) {
	t.Parallel()
	cases := []struct {
		method, path string
		kind         indexerdb.Kind
		gated        bool
	}{
		{"GET", "/matches/*", indexerdb.KindCurrent, true},
		{"GET", "/v1/matches/addr1", indexerdb.KindCurrent, true},
		{"DELETE", "/matches/*", 0, false},
		{"GET", "/datums/abcd", indexerdb.KindHistory, true},
		{"GET", "/scripts/abcd", indexerdb.KindHistory, true},
		{"GET", "/metadata/100", indexerdb.KindHistory, true},
		{"GET", "/checkpoints", indexerdb.KindHistory, true},
		{"GET", "/v1/checkpoints/100", indexerdb.KindHistory, true},
		{"HEAD", "/matches/*", indexerdb.KindCurrent, true},
		{"GET", "/health", 0, false},
		{"GET", "/metrics", 0, false},
		{"GET", "/patterns", 0, false},
		{"PUT", "/patterns/*", 0, false},
	}
	for _, tc := range cases {
		kind, gated := classifyRoute(
			httptest.NewRequest(tc.method, tc.path, nil),
		)
		assert.Equal(t, tc.gated, gated, "%s %s gated", tc.method, tc.path)
		if tc.gated {
			assert.Equal(t, tc.kind, kind, "%s %s kind", tc.method, tc.path)
		}
	}
}

func TestHandlerGatesOnReadiness(t *testing.T) {
	t.Parallel()
	s := New(Config{}, nil, nil)
	s.ready = func(indexerdb.Kind) (bool, error) { return false, nil }
	h := s.handler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/matches/*", nil))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("Retry-After"))
}

func TestEveryRegisteredRouteIsClassified(t *testing.T) {
	t.Parallel()
	patterns := RoutePatterns()
	require.NotEmpty(t, patterns)
	wildcard := regexp.MustCompile(`\{[^}]*\}`)
	for _, pattern := range patterns {
		method, path, ok := strings.Cut(pattern, " ")
		require.True(t, ok, pattern)
		_, _, known := classifyPath(
			method,
			wildcard.ReplaceAllString(path, "x"),
		)
		assert.True(
			t,
			known,
			"route %q has no readiness classification",
			pattern,
		)
	}
}
