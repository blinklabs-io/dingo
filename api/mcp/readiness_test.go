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

package mcp

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database/indexerdb"
	"github.com/blinklabs-io/dingo/internal/apiconfig"
	"github.com/stretchr/testify/require"
)

func TestHandlerGatesDataRoutesUntilIndexerReady(t *testing.T) {
	t.Parallel()
	var ready atomic.Bool
	server, err := NewServer(
		t.Context(),
		DefaultProviderConfig(),
		ProviderDependencies{
			Ready: func(indexerdb.Kind) (bool, error) {
				return ready.Load(), nil
			},
		},
		apiconfig.EffectiveTLS{},
		"127.0.0.1:0",
	)
	require.NoError(t, err)
	h := server.handler()

	for _, path := range []string{"/mcp", "/sse", "/"} {
		req := httptest.NewRequest(
			http.MethodPost, path, strings.NewReader("{}"),
		)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code, path)
		require.Equal(t, "5", rec.Header().Get("Retry-After"), path)
	}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "health is never gated")

	ready.Store(true)
	req = httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader("{}"))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.NotEqual(t, http.StatusServiceUnavailable, rec.Code)
}
