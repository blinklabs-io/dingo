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

// newTestKoiosClient builds a client pointed at a test server. The burst
// limiter is disabled (limit 0) so retries are not slowed by the sliding window.
func newTestKoiosClient(baseURL string) *KoiosClient {
	return &KoiosClient{
		baseURL: baseURL,
		apiKey:  "testkey",
		http:    &http.Client{Timeout: 5 * time.Second},
		limiter: newBurstLimiter(0, koiosBurstWindow),
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

func TestGetDoesNotRetryOnDailyQuotaExceeded(t *testing.T) {
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
	require.True(t, strings.Contains(err.Error(), "daily tier quota exceeded"))
	require.EqualValues(t, 1, attempts.Load())
}

func TestGetRetriesBurst429HonoringRetryAfter(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("Too many requests"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"epoch_no":1}]`))
	}))
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	start := time.Now()
	resp, err := k.get(context.Background(), "/tip", -1, -1)
	elapsed := time.Since(start)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.EqualValues(t, 2, attempts.Load())
	// Must have waited at least the Retry-After second (not the old 2s keyed backoff).
	require.GreaterOrEqual(t, elapsed, time.Second)
}

func TestRetryAfterDelayFallsBackToBurstCooldown(t *testing.T) {
	require.Equal(t, koiosBurstCooldown, retryAfterDelay(nil))
	resp := &http.Response{Header: make(http.Header)}
	require.Equal(t, koiosBurstCooldown, retryAfterDelay(resp))
	resp.Header.Set("Retry-After", "42")
	require.Equal(t, 42*time.Second, retryAfterDelay(resp))
}

func TestIsDailyQuotaExceeded(t *testing.T) {
	require.True(t, isDailyQuotaExceeded("Exceeded Tier Limit"))
	require.True(t, isDailyQuotaExceeded("error: Exceeded Tier Limit\n"))
	require.False(t, isDailyQuotaExceeded("Too many requests"))
	require.False(t, isDailyQuotaExceeded(""))
}

func TestRationalsEqual(t *testing.T) {
	require.True(t, rationalsEqual("0.1", "1/10"))
	require.True(t, rationalsEqual("1/20", "0.05"))
	require.False(t, rationalsEqual("0.1", "0.2"))
	require.False(t, rationalsEqual("not-a-number", "1/10"))
}
