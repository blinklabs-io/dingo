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

package health_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blinklabs-io/dingo/internal/health"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func gap(v uint64) health.TipGapFunc {
	return func() (uint64, bool) { return v, true }
}

func unknownGap() (uint64, bool) { return 0, false }

func TestEvaluate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tipGap    health.TipGapFunc
		tolerance uint64
		wantReady bool
	}{
		{name: "nil source", tipGap: nil, tolerance: 100},
		{name: "no tip yet", tipGap: unknownGap, tolerance: 100},
		{name: "caught up", tipGap: gap(0), tolerance: 100, wantReady: true},
		{
			name:      "at tolerance",
			tipGap:    gap(100),
			tolerance: 100,
			wantReady: true,
		},
		{name: "past tolerance", tipGap: gap(101), tolerance: 100},
		{name: "initial sync", tipGap: gap(90_000_000), tolerance: 1000},
		// Zero tolerance is rejected in config; if one reaches here it must
		// still admit an exact-tip node rather than nothing at all.
		{
			name:      "zero tolerance at tip",
			tipGap:    gap(0),
			tolerance: 0,
			wantReady: true,
		},
		{name: "zero tolerance behind", tipGap: gap(1), tolerance: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := health.Evaluate(test.tipGap, test.tolerance)
			assert.True(t, got.Live, "Evaluate always reports live")
			assert.Equal(t, test.wantReady, got.Ready)
			if test.wantReady {
				assert.Empty(t, got.Reason)
			} else {
				assert.NotEmpty(
					t,
					got.Reason,
					"an unready verdict must say why",
				)
			}
		})
	}
}

func decode(t *testing.T, rec *httptest.ResponseRecorder) health.Status {
	t.Helper()
	var status health.Status
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &status))
	return status
}

func TestMuxProbeCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		tipGap     health.TipGapFunc
		wantCode   int
		wantStatus string
	}{
		{"health alias live", health.PathHealth, unknownGap, 200, "ok"},
		{"healthz live", health.PathLive, unknownGap, 200, "ok"},
		{"readyz unready", health.PathReady, unknownGap, 503, "unhealthy"},
		{"readyz frozen tip", health.PathReady, gap(5000), 503, "unhealthy"},
		{"readyz ready", health.PathReady, gap(3), 200, "ok"},
		{"health alias frozen tip", health.PathHealth, gap(5000), 200, "ok"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			mux := health.NewMux(test.tipGap, 1000)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(
				rec,
				httptest.NewRequest(http.MethodGet, test.path, nil),
			)
			assert.Equal(t, test.wantCode, rec.Code)
			assert.Equal(
				t,
				"application/json",
				rec.Header().Get("Content-Type"),
			)
			assert.Equal(t, test.wantStatus, decode(t, rec).Status)
		})
	}
}

func TestMuxRejectsWrites(t *testing.T) {
	t.Parallel()

	mux := health.NewMux(gap(0), 1000)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(
		rec,
		httptest.NewRequest(http.MethodPost, health.PathReady, nil),
	)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.Equal(t, "GET, HEAD", rec.Header().Get("Allow"))
}

// The health listener serves probes and nothing else: no metrics, no pprof,
// no API surface leaks onto it through the default mux.
func TestMuxServesOnlyProbePaths(t *testing.T) {
	t.Parallel()

	mux := health.NewMux(gap(0), 1000)
	for _, path := range []string{
		"/",
		"/metrics",
		"/debug/pprof/",
		"/api/v1/status",
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		assert.Equal(t, http.StatusNotFound, rec.Code, "path %s", path)
	}
}

// HEAD is what some load-balancer health checks send; it must carry the
// status code without a body.
func TestMuxHeadCarriesStatusWithoutBody(t *testing.T) {
	t.Parallel()

	mux := health.NewMux(unknownGap, 1000)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(
		rec,
		httptest.NewRequest(http.MethodHead, health.PathReady, nil),
	)
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Empty(t, rec.Body.String())
}
