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

package blockfrost

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

type routeClass struct {
	method string
	path   string
	kind   indexerdb.Kind
	gated  bool
}

func TestClassifyRoute(t *testing.T) {
	t.Parallel()
	cases := []routeClass{
		{"GET", "/", 0, false},
		{"GET", "/health", 0, false},
		{"GET", "/api/v0/health", 0, false},
		{"GET", "/api/v0/health/clock", 0, false},
		{"GET", "/api/v0/network", indexerdb.KindCurrent, true},
		{"GET", "/api/v0/network/eras", indexerdb.KindCurrent, true},
		{"GET", "/api/v0/genesis", 0, false},
		{"POST", "/api/v0/tx/submit", 0, false},
		{"POST", "/api/v0/utils/txs/evaluate", 0, false},
		{"GET", "/api/v0/mempool", 0, false},
		{"GET", "/api/v0/mempool/addresses/addr1", 0, false},
		{"GET", "/api/v0/accounts/stake1", indexerdb.KindCurrent, true},
		{"GET", "/api/v0/accounts/stake1/rewards", indexerdb.KindCurrent, true},
		{"GET", "/api/v0/addresses/addr1/utxos", indexerdb.KindCurrent, true},
		{"GET", "/api/v0/assets/abcd", indexerdb.KindCurrent, true},
		{"GET", "/api/v0/pools/pool1", indexerdb.KindCurrent, true},
		{"GET", "/api/v0/pools/extended", indexerdb.KindCurrent, true},
		{"GET", "/api/v0/governance/dreps", indexerdb.KindCurrent, true},
		{"GET", "/api/v0/epochs/latest", indexerdb.KindCurrent, true},
		{
			"GET",
			"/api/v0/epochs/latest/parameters",
			indexerdb.KindCurrent,
			true,
		},
		{"GET", "/api/v0/epochs/400", indexerdb.KindHistory, true},
		{"GET", "/api/v0/epochs/400/blocks", indexerdb.KindHistory, true},
		{"GET", "/api/v0/blocks/latest", indexerdb.KindHistory, true},
		{"GET", "/api/v0/blocks/100/txs", indexerdb.KindHistory, true},
		{"GET", "/api/v0/txs/abcd/utxos", indexerdb.KindHistory, true},
		{"GET", "/api/v0/scripts/abcd", indexerdb.KindHistory, true},
		{"GET", "/api/v0/metadata/txs/labels", indexerdb.KindHistory, true},
	}
	for _, tc := range cases {
		r := httptest.NewRequest(tc.method, tc.path, nil)
		kind, gated := classifyRoute(r)
		assert.Equal(t, tc.gated, gated, "%s %s gated", tc.method, tc.path)
		if tc.gated {
			assert.Equal(t, tc.kind, kind, "%s %s kind", tc.method, tc.path)
		}
	}
}

func TestHandlerGatesOnReadiness(t *testing.T) {
	t.Parallel()
	b := New(BlockfrostConfig{}, nil, nil)
	b.ready = func(kind indexerdb.Kind) (bool, error) {
		return kind == indexerdb.KindHistory, nil
	}
	h := b.handler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(
		http.MethodGet, "/api/v0/accounts/stake1", nil,
	))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("Retry-After"))

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestEveryRegisteredRouteIsClassified(t *testing.T) {
	t.Parallel()
	patterns := RoutePatterns()
	require.NotEmpty(t, patterns)
	wildcard := regexp.MustCompile(`\{[^}]*\}`)
	for _, pattern := range patterns {
		_, path, ok := strings.Cut(pattern, " ")
		if !ok {
			path = pattern
		}
		if path == "/" {
			continue // the catch-all answers 404 for every unknown path
		}
		path = wildcard.ReplaceAllString(path, "x")
		if path == "/x" {
			path = "/"
		}
		_, _, known := classifyPath(path)
		assert.True(
			t,
			known,
			"route %q has no readiness classification",
			pattern,
		)
	}
}
