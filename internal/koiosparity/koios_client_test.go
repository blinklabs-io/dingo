// Copyright 2025 Blink Labs Software
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

package koiosparity

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newTestKoiosClient builds a client pointed at a test server, using an API
// key so retries use the shorter keyed backoff tier.
func newTestKoiosClient(baseURL string) *KoiosClient {
	return &KoiosClient{
		baseURL: baseURL,
		apiKey:  "testkey",
		http:    &http.Client{Timeout: 5 * time.Second},
	}
}

func TestGetRetriesOn503ThenSucceeds(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("<html>503 Service Unavailable</html>"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"epoch_no":1}]`))
	}))
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	resp, err := k.get(context.Background(), "/tip", -1, -1)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.EqualValues(t, 2, attempts.Load())
}

func TestGetFailsAfterExhausting503Retries(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("no server available"))
	}))
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	_, err := k.get(context.Background(), "/tip", -1, -1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "server error")
	require.EqualValues(t, koiosMaxRetries, attempts.Load())
}

func TestGetDoesNotRetryOnQuotaExceeded(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte("Exceeded Tier Limit"))
	}))
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	_, err := k.get(context.Background(), "/tip", -1, -1)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "tier quota exceeded"))
	require.EqualValues(t, 1, attempts.Load())
}
