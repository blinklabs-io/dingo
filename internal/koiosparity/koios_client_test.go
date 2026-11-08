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
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPostRejectsOversizedResponseAsPermanentNeverRetried proves the
// response-size bound (readBodyLimited/koiosMaxResponseBytes):
// a response exceeding the cap must fail with an error that classifies as
// ErrKoiosPermanent — never retried within the call (a response that big
// won't get smaller on retry) and never automatically retried on a future
// fetch run either.
func TestPostRejectsOversizedResponseAsPermanentNeverRetried(t *testing.T) {
	t.Parallel()

	var requestCount atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount.Add(1)
			w.WriteHeader(http.StatusOK)
			// Stream just past the cap without holding it all in memory
			// twice: koiosMaxResponseBytes+1 zero bytes is already a
			// deliberately oversized response for this test's purposes.
			chunk := make([]byte, 1<<20) // 1 MiB
			written := 0
			for written < koiosMaxResponseBytes+1 {
				n, werr := w.Write(chunk)
				written += n
				if werr != nil {
					return
				}
			}
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	_, err := k.GetAccountRewardHistory(
		context.Background(), []string{"stake1x"}, 100,
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrKoiosPermanent))
	require.True(t, errors.Is(err, errKoiosResponseTooLarge))
	require.EqualValues(
		t,
		1,
		requestCount.Load(),
		"an oversized response must never be retried",
	)
}

// newTestKoiosClient builds a client pointed at a test server. The burst
// limiter is disabled (limit 0) so retries are not slowed by the sliding window.
func newTestKoiosClient(baseURL string) *KoiosClient {
	return &KoiosClient{
		baseURL:                baseURL,
		apiKey:                 "testkey",
		http:                   &http.Client{Timeout: 5 * time.Second},
		limiter:                newBurstLimiter(0, koiosBurstWindow),
		koios408MaxRetries:     koios408MaxRetriesDefault,
		koios408InitialBackoff: koios408InitialBackoffDefault,
		koios408MaxBackoff:     koios408MaxBackoffDefault,
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestKoiosTransportErrorsRedactURLUserinfo(t *testing.T) {
	const (
		secretUsername = "SENTINEL-URL-USERNAME"
		secretPassword = "SENTINEL-URL-PASSWORD"
		safeCause      = "connection refused by test transport"
	)

	for _, tc := range []struct {
		name string
		do   func(*KoiosClient) error
		want string
	}{
		{
			name: "GET",
			do: func(k *KoiosClient) error {
				_, err := k.get(context.Background(), "/tip", -1, -1)
				return err
			},
			want: "koios GET /tip",
		},
		{
			name: "POST",
			do: func(k *KoiosClient) error {
				_, err := k.post(
					context.Background(),
					"/account_reward_history",
					map[string]any{"_epoch_no": 1},
				)
				return err
			},
			want: "koios POST /account_reward_history",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			k := newTestKoiosClient(
				"https://" + secretUsername + ":" + secretPassword +
					"@koios.example/api/v1",
			)
			k.http.Transport = roundTripFunc(
				func(*http.Request) (*http.Response, error) {
					return nil, errors.New(safeCause)
				},
			)

			err := tc.do(k)
			require.Error(t, err)
			assert.NotContains(t, err.Error(), secretUsername)
			assert.NotContains(t, err.Error(), secretPassword)
			assert.Contains(t, err.Error(), tc.want)
			assert.Contains(t, err.Error(), safeCause)
		})
	}
}

func TestKoiosRequestBuildErrorsRedactURLUserinfo(t *testing.T) {
	const (
		secretUsername = "SENTINEL-BUILD-USERNAME"
		secretPassword = "SENTINEL-BUILD-PASSWORD"
	)

	k := newTestKoiosClient(
		"https://" + secretUsername + ":" + secretPassword +
			"@koios.example/api/v1",
	)
	_, err := k.GetPoolEpochHistory(
		context.Background(),
		"pool1\nbad",
		1,
	)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), secretUsername)
	assert.NotContains(t, err.Error(), secretPassword)
	assert.Contains(t, err.Error(), "build request")
	assert.Contains(t, err.Error(), "invalid control character in URL")
}

func TestKoiosRestrictedDialerRejectsPrivateDNSAnswer(t *testing.T) {
	t.Parallel()

	var dialed atomic.Bool
	dialer := &koiosRestrictedDialer{
		lookupIPAddr: func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("10.0.0.7")}}, nil
		},
		dialContext: func(context.Context, string, string) (net.Conn, error) {
			dialed.Store(true)
			return nil, errors.New("unexpected dial")
		},
	}

	_, err := dialer.DialContext(
		context.Background(), "tcp", "public-looking.example:443",
	)
	require.Error(t, err)
	assert.False(t, dialed.Load(),
		"a private DNS answer must be rejected before any connection")
}

func TestKoiosRestrictedDialerRejectsMixedPublicAndPrivateDNSAnswers(
	t *testing.T,
) {
	t.Parallel()

	var dialed atomic.Bool
	dialer := &koiosRestrictedDialer{
		lookupIPAddr: func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{
				{IP: net.ParseIP("93.184.216.34")},
				{IP: net.ParseIP("127.0.0.1")},
			}, nil
		},
		dialContext: func(context.Context, string, string) (net.Conn, error) {
			dialed.Store(true)
			return nil, errors.New("unexpected dial")
		},
	}

	_, err := dialer.DialContext(
		context.Background(), "tcp", "rebinding.example:443",
	)
	require.Error(t, err)
	assert.False(t, dialed.Load(),
		"one blocked DNS answer must reject the whole resolution set")
}

func TestKoiosRestrictedDialerRejectsSpecialUseIPv6DNSAnswer(t *testing.T) {
	t.Parallel()

	for _, ip := range []string{
		"64:ff9b::7f00:1",
		"100:0:0:1::1",
		"2620:4f:8000::1",
	} {
		t.Run(ip, func(t *testing.T) {
			t.Parallel()

			var dialed atomic.Bool
			dialer := &koiosRestrictedDialer{
				lookupIPAddr: func(context.Context, string) ([]net.IPAddr, error) {
					return []net.IPAddr{{IP: net.ParseIP(ip)}}, nil
				},
				dialContext: func(context.Context, string, string) (net.Conn, error) {
					dialed.Store(true)
					return nil, errors.New("unexpected dial")
				},
			}

			_, err := dialer.DialContext(
				context.Background(), "tcp", "translated.example:443",
			)
			require.Error(t, err)
			assert.False(t, dialed.Load())
		})
	}
}

func TestNewKoiosClientTransportEnforcesResolvedAddressPolicy(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	blockedClient, err := NewKoiosClient("preview", "", "", false, false)
	require.NoError(t, err)
	blockedClient.baseURL = srv.URL
	_, err = blockedClient.http.Get(srv.URL)
	require.Error(t, err)
	require.Contains(t, err.Error(), "private or special-use address")

	allowedClient, err := NewKoiosClient("preview", "", "", false, true)
	require.NoError(t, err)
	allowedClient.baseURL = srv.URL
	resp, err := allowedClient.http.Get(srv.URL)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
}

func TestKoiosRestrictedDialerDialsValidatedPublicIP(t *testing.T) {
	t.Parallel()

	var target string
	dialer := &koiosRestrictedDialer{
		lookupIPAddr: func(context.Context, string) ([]net.IPAddr, error) {
			return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
		},
		dialContext: func(_ context.Context, _, address string) (net.Conn, error) {
			target = address
			client, server := net.Pipe()
			server.Close()
			return client, nil
		},
	}

	conn, err := dialer.DialContext(
		context.Background(), "tcp", "mirror.example:443",
	)
	require.NoError(t, err)
	require.NoError(t, conn.Close())
	assert.Equal(t, "93.184.216.34:443", target)
}

func TestGetRetriesOn503ThenSucceeds(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if attempts.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("<html>503 Service Unavailable</html>"))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"epoch_no":1}]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	resp, err := k.get(context.Background(), "/tip", -1, -1)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.EqualValues(t, 2, attempts.Load())
}

func TestGetRetriesOnBodyReadFailure(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if attempts.Add(1) == 1 {
				// Declare a body longer than what's actually written so the
				// server aborts the connection mid-transfer, forcing the
				// client's io.ReadAll to fail with io.ErrUnexpectedEOF — a
				// transient failure distinct from a non-2xx status or a
				// connect-time transport error.
				w.Header().Set("Content-Length", "1000")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[{"epoch_no":1}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"epoch_no":1}]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	resp, err := k.get(context.Background(), "/tip", -1, -1)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.JSONEq(t, `[{"epoch_no":1}]`, string(resp.Body))
	require.EqualValues(t, 2, attempts.Load())
}

func TestGetFailsAfterExhausting503Retries(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("no server available"))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	_, err := k.get(context.Background(), "/tip", -1, -1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "server error")
	require.EqualValues(t, koiosMaxRetries, attempts.Load())
}

func TestGetDoesNotRetryOnDailyQuotaExceeded(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("Exceeded Tier Limit"))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	_, err := k.get(context.Background(), "/tip", -1, -1)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "daily tier quota exceeded"))
	require.EqualValues(t, 1, attempts.Load())
	require.ErrorIs(
		t,
		err,
		ErrKoiosPermanent,
		"daily quota exhaustion must be classified permanent so Fetch aborts rather than retrying",
	)
}

// TestGetDoesNotRetryOnAuthFailure covers the "hard 4xx" class (401/403 and
// similar) that get() never retries: unlike burst-429/5xx, which are retried
// internally and only reach the caller once exhausted, these reach the caller
// on the very first attempt and must be marked permanent so Fetch aborts the
// whole run instead of recording a misleading, isolated per-epoch failure.
func TestGetDoesNotRetryOnAuthFailure(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("invalid API key"))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	_, err := k.get(context.Background(), "/tip", -1, -1)
	require.Error(t, err)
	require.EqualValues(t, 1, attempts.Load(), "a 401 must not be retried")
	require.ErrorIs(t, err, ErrKoiosPermanent)
}

// TestGetExhausted503IsNotPermanent guards against over-classifying: a
// transient failure that exhausts its retries (e.g. sustained 503s) must
// remain an isolated, resumable per-epoch failure, not a hard abort.
func TestGetExhausted503IsNotPermanent(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("no server available"))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	_, err := k.get(context.Background(), "/tip", -1, -1)
	require.Error(t, err)
	require.False(
		t,
		errors.Is(err, ErrKoiosPermanent),
		"an exhausted transient 503 must remain retryable/isolated, not permanent",
	)
}

func TestGetRetriesBurst429HonoringRetryAfter(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if attempts.Add(1) == 1 {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte("Too many requests"))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"epoch_no":1}]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	start := time.Now()
	resp, err := k.get(context.Background(), "/tip", -1, -1)
	elapsed := time.Since(start)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.EqualValues(t, 2, attempts.Load())
	// Must have waited at least the Retry-After second (not the old 2s keyed backoff).
	require.GreaterOrEqual(t, elapsed, time.Second)
}

func TestRetryAfterDelayFallsBackToBurstCooldown(t *testing.T) {
	t.Parallel()

	require.Equal(t, koiosBurstCooldown, retryAfterDelay(nil))
	resp := &http.Response{Header: make(http.Header)}
	require.Equal(t, koiosBurstCooldown, retryAfterDelay(resp))
	resp.Header.Set("Retry-After", "42")
	require.Equal(t, 42*time.Second, retryAfterDelay(resp))
}

func TestGetPoolFirstActiveEpochsTakesMinAcrossUpdates(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			// pool1re-registered: two updates, must take the earlier (500), not the
			// current/latest one (1400) that /pool_list alone would report.
			// pool1once: a single registration, no re-registration.
			// pool1nullupdate: a null active_epoch_no row that must not corrupt the result.
			_, _ = w.Write([]byte(`[
			{"pool_id_bech32":"pool1re-registered","active_epoch_no":1400},
			{"pool_id_bech32":"pool1re-registered","active_epoch_no":500},
			{"pool_id_bech32":"pool1once","active_epoch_no":1356},
			{"pool_id_bech32":"pool1nullupdate","active_epoch_no":null}
		]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	first, err := k.GetPoolFirstActiveEpochs(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(500), first["pool1re-registered"])
	require.Equal(t, uint64(1356), first["pool1once"])
	_, ok := first["pool1nullupdate"]
	require.False(t, ok)
}

func TestFilteredKoiosResponsesRequireRequestedEpoch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		call func(*KoiosClient) error
	}{
		{
			name: "epoch_info",
			path: "/epoch_info",
			call: func(k *KoiosClient) error {
				_, err := k.GetEpochInfo(context.Background(), 10)
				return err
			},
		},
		{
			name: "totals",
			path: "/totals",
			call: func(k *KoiosClient) error {
				_, err := k.GetTotals(context.Background(), 10)
				return err
			},
		},
		{
			name: "pool_history",
			path: "/pool_history",
			call: func(k *KoiosClient) error {
				_, err := k.GetPoolEpochHistory(
					context.Background(),
					"pool1test",
					10,
				)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv := httptest.NewServer(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					assert.Equal(t, test.path, r.URL.Path)
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`[{"epoch_no":11}]`))
				}),
			)
			defer srv.Close()

			err := test.call(newTestKoiosClient(srv.URL))
			require.ErrorContains(t, err, "requested epoch 10")
		})
	}
}

func TestFilteredKoiosResponsesRejectMultipleRows(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"epoch_no":10},{"epoch_no":10}]`))
		}),
	)
	defer srv.Close()

	_, err := newTestKoiosClient(srv.URL).GetEpochInfo(context.Background(), 10)
	require.ErrorContains(t, err, "got 2 row(s)")
}

func TestRationalsEqual(t *testing.T) {
	t.Parallel()

	require.True(t, rationalsEqual("0.1", "1/10"))
	require.True(t, rationalsEqual("1/20", "0.05"))
	require.False(t, rationalsEqual("0.1", "0.2"))
	require.False(t, rationalsEqual("not-a-number", "1/10"))
}

// TestPostRetriesOn503ThenSucceeds mirrors TestGetRetriesOn503ThenSucceeds
// for post(), proving the POST path shares the same transient-retry
// classification as get() despite needing to rebuild its body each attempt.
func TestPostRetriesOn503ThenSucceeds(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodPost, r.Method)
			if attempts.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("no server available"))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"stake_address":"stake1x"}]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	resp, err := k.post(
		context.Background(),
		"/account_reward_history",
		map[string]any{
			"_stake_addresses": []string{"stake1x"},
			"_epoch_no":        100,
		},
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.EqualValues(t, 2, attempts.Load())
}

func TestPostDoesNotRetryOnAuthFailure(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("unauthorized"))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	_, err := k.post(
		context.Background(),
		"/account_reward_history",
		map[string]any{},
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrKoiosPermanent))
	require.EqualValues(t, 1, attempts.Load())
}

func TestPostDoesNotRetryOnDailyQuotaExceeded(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte("Exceeded Tier Limit"))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	_, err := k.post(
		context.Background(),
		"/account_reward_history",
		map[string]any{},
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrKoiosPermanent))
	require.EqualValues(t, 1, attempts.Load())
}

func TestGetAllAccountAddressesPaginates(t *testing.T) {
	t.Parallel()

	var reqs []string
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqs = append(reqs, r.Header.Get("Range"))
			if len(reqs) == 1 {
				w.Header().Set("Content-Range", "0-999/1001")
				w.WriteHeader(http.StatusPartialContent)
				items := make([]string, koiosPageSize)
				for i := range items {
					items[i] = `{"stake_address":"stake1addr` + strings.Repeat(
						"a",
						1,
					) + `"}`
				}
				_, _ = w.Write([]byte("[" + strings.Join(items, ",") + "]"))
				return
			}
			w.Header().Set("Content-Range", "1000-1000/1001")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"stake_address":"stake1addrlast"}]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	addrs, err := k.GetAllAccountAddresses(context.Background())
	require.NoError(t, err)
	require.Len(t, reqs, 2)
	require.Contains(t, addrs, "stake1addrlast")
}

func TestGetAccountRewardHistoryEmptyAddressesNoRequest(t *testing.T) {
	t.Parallel()

	called := false
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	items, err := k.GetAccountRewardHistory(context.Background(), nil, 100)
	require.NoError(t, err)
	require.Nil(t, items)
	require.False(t, called)
}

// TestGetTxInfosReturnsResultsInRequestOrder proves GetTxInfos reorders
// Koios's response to match the caller's requested order, not whatever
// order Koios happened to return -- required so a caller applying
// dependent transactions (a UTxO created by one hash, spent by a later
// one in the same request) does so in chain order.
func TestGetTxInfosReturnsResultsInRequestOrder(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			// Deliberately reversed vs. the request order below.
			_, _ = w.Write([]byte(`[
				{"tx_hash":"bbb","inputs":[],"outputs":[]},
				{"tx_hash":"aaa","inputs":[],"outputs":[]}
			]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	items, err := k.GetTxInfos(context.Background(), []string{"aaa", "bbb"})
	require.NoError(t, err)
	require.Len(t, items, 2)
	require.Equal(t, "aaa", items[0].TxHash)
	require.Equal(t, "bbb", items[1].TxHash)
}

// TestGetTxInfosBatchesBySetTxInfoLimits proves GetTxInfos splits its
// request by the client's configured batch size, so a host that accepts
// larger bodies gets fewer, larger requests.
func TestGetTxInfosBatchesBySetTxInfoLimits(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var sizes []int
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var body struct {
				TxHashes []string `json:"_tx_hashes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			sizes = append(sizes, len(body.TxHashes))
			mu.Unlock()
			items := make([]map[string]any, 0, len(body.TxHashes))
			for _, h := range body.TxHashes {
				items = append(items, map[string]any{
					"tx_hash": h, "inputs": []any{}, "outputs": []any{},
				})
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(items)
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	require.NoError(t, k.SetTxInfoLimits(3, 2))
	hashes := []string{"a1", "a2", "a3", "a4", "a5", "a6", "a7"}
	items, err := k.GetTxInfos(context.Background(), hashes)
	require.NoError(t, err)
	require.Len(t, items, len(hashes))
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []int{3, 3, 1}, sizes)
}

func TestSetTxInfoLimitsBounds(t *testing.T) {
	t.Parallel()

	k := newTestKoiosClient("http://127.0.0.1:1")
	require.Equal(t, KoiosTxInfoBatchSize, k.TxInfoBatchSize())
	require.Equal(t, KoiosTxInfoConcurrency, k.TxInfoConcurrency())

	// Koios answers at most koiosPageSize rows, so a batch that large could
	// come back silently cut short.
	require.Error(t, k.SetTxInfoLimits(koiosPageSize, 4))
	require.Error(t, k.SetTxInfoLimits(0, 4))
	require.Error(t, k.SetTxInfoLimits(250, 0))
	require.Error(t, k.SetTxInfoLimits(250, KoiosTxInfoMaxConcurrency+1))
	require.Equal(t, KoiosTxInfoBatchSize, k.TxInfoBatchSize(),
		"a rejected setting leaves the client unchanged")

	require.NoError(t, k.SetTxInfoLimits(koiosPageSize-1, 4))
	require.Equal(t, koiosPageSize-1, k.TxInfoBatchSize())
	require.Equal(t, 4, k.TxInfoConcurrency())

	require.NoError(t, k.SetTxInfoLimits(50, KoiosTxInfoMaxConcurrency))
	require.Equal(t, KoiosTxInfoMaxConcurrency, k.TxInfoConcurrency())
}

// TestGetTxInfosErrorsOnIncompleteResponse proves that a transaction hash
// the caller asked for but Koios omitted from the response must fail
// loudly, not be silently treated as
// "no changes for that hash" -- the from-genesis UTxO reconstruction can't
// tell the difference between "this hash has no inputs/outputs" and "this
// hash's real inputs/outputs were silently dropped," so it must never
// proceed on an incomplete response.
func TestGetTxInfosErrorsOnIncompleteResponse(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			// Only "aaa" of the two requested hashes comes back.
			_, _ = w.Write([]byte(`[{"tx_hash":"aaa","inputs":[],"outputs":[]}]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	_, err := k.GetTxInfos(context.Background(), []string{"aaa", "bbb"})
	require.ErrorIs(t, err, ErrTxInfoMissing,
		"the caller fetches a missing hash again, so it must be identifiable")
	require.ErrorContains(t, err, "bbb")
}

// TestGetTxInfosErrorsOnDuplicateResult proves a defensively-unexpected
// duplicate tx_hash in Koios's response (which would otherwise silently
// pick one and lose data from the other) fails loudly instead.
func TestGetTxInfosErrorsOnDuplicateResult(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[
				{"tx_hash":"aaa","inputs":[],"outputs":[]},
				{"tx_hash":"aaa","inputs":[],"outputs":[]}
			]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	_, err := k.GetTxInfos(context.Background(), []string{"aaa"})
	require.Error(t, err)
	require.ErrorContains(t, err, "duplicate")
	require.NotErrorIs(t, err, ErrTxInfoMissing)
}

// fakeRoundTripper is a http.RoundTripper that is deliberately not
// *http.Transport, standing in for whatever a future caller might install as
// http.DefaultTransport.
type fakeRoundTripper struct{}

func (fakeRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("fakeRoundTripper: not implemented")
}

func TestNewKoiosTransportIgnoresAmbientInsecureTLSConfig(t *testing.T) {
	// Not t.Parallel: swaps the process-global http.DefaultTransport.
	original := http.DefaultTransport
	http.DefaultTransport = &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // deliberately poisoned process-global fixture
	}
	t.Cleanup(func() { http.DefaultTransport = original })

	transport := newKoiosTransport(
		koiosDialTimeout,
		koiosDialKeepAlive,
		koiosTLSHandshakeTimeout,
		koiosResponseHeaderTimeout,
		koiosExpectContinueTimeout,
		false,
	)
	assert.Nil(t, transport.TLSClientConfig)
}

func TestNewKoiosTransportIgnoresAmbientTLSNextProto(t *testing.T) {
	// Not t.Parallel: swaps the process-global http.DefaultTransport.
	original := http.DefaultTransport
	http.DefaultTransport = &http.Transport{
		TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{
			"h2": func(string, *tls.Conn) http.RoundTripper {
				return fakeRoundTripper{}
			},
		},
	}
	t.Cleanup(func() { http.DefaultTransport = original })

	transport := newKoiosTransport(
		koiosDialTimeout,
		koiosDialKeepAlive,
		koiosTLSHandshakeTimeout,
		koiosResponseHeaderTimeout,
		koiosExpectContinueTimeout,
		false,
	)
	assert.Nil(t, transport.TLSNextProto)
}

func newTestKoiosClientFast408(baseURL string, maxRetries int) *KoiosClient {
	k := newTestKoiosClient(baseURL)
	k.koios408MaxRetries = maxRetries
	k.koios408InitialBackoff = time.Millisecond
	k.koios408MaxBackoff = 5 * time.Millisecond
	return k
}

func TestGetRetries408ThenSucceeds(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if attempts.Add(1) <= 2 {
				w.WriteHeader(http.StatusRequestTimeout)
				_, _ = w.Write(
					[]byte(
						"<html><body><h1>408 Request Time-out</h1></body></html>",
					),
				)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"epoch_no":1}]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClientFast408(srv.URL, 5)
	resp, err := k.get(context.Background(), "/tip", -1, -1)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.EqualValues(t, 3, attempts.Load())
}

func TestGetFailsAfterExhausting408RetriesReturnsPermanent(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			w.WriteHeader(http.StatusRequestTimeout)
			_, _ = w.Write(
				[]byte(
					"<html><body><h1>408 Request Time-out</h1></body></html>",
				),
			)
		}),
	)
	defer srv.Close()

	k := newTestKoiosClientFast408(srv.URL, 4)
	_, err := k.get(context.Background(), "/tip", -1, -1)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrKoiosPermanent))
	require.EqualValues(t, 4, attempts.Load())
}

func TestGet408RetriesDoNotConsumeThe5xxBudget(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch attempts.Add(1) {
			case 1, 2:
				w.WriteHeader(http.StatusRequestTimeout)
			case 3:
				w.WriteHeader(http.StatusServiceUnavailable)
			default:
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[{"epoch_no":1}]`))
			}
		}),
	)
	defer srv.Close()

	k := newTestKoiosClientFast408(srv.URL, 16)
	resp, err := k.get(context.Background(), "/tip", -1, -1)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.EqualValues(t, 4, attempts.Load())
}

func TestGetDoesNotRetryOn404(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("not found"))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	_, err := k.get(context.Background(), "/tip", -1, -1)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrKoiosPermanent))
	require.EqualValues(t, 1, attempts.Load(), "a 404 must not be retried")
}

func TestPostRetries408ThenSucceeds(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodPost, r.Method)
			if attempts.Add(1) <= 2 {
				w.WriteHeader(http.StatusRequestTimeout)
				_, _ = w.Write(
					[]byte(
						"<html><body><h1>408 Request Time-out</h1></body></html>",
					),
				)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[{"stake_address":"stake1x"}]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClientFast408(srv.URL, 5)
	resp, err := k.post(
		context.Background(),
		"/account_reward_history",
		map[string]any{
			"_stake_addresses": []string{"stake1x"},
			"_epoch_no":        100,
		},
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.EqualValues(t, 3, attempts.Load())
}

func TestPostFailsAfterExhausting408RetriesReturnsPermanent(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			w.WriteHeader(http.StatusRequestTimeout)
			_, _ = w.Write(
				[]byte(
					"<html><body><h1>408 Request Time-out</h1></body></html>",
				),
			)
		}),
	)
	defer srv.Close()

	k := newTestKoiosClientFast408(srv.URL, 4)
	_, err := k.post(
		context.Background(),
		"/account_reward_history",
		map[string]any{},
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrKoiosPermanent))
	require.EqualValues(t, 4, attempts.Load())
}

func TestPost408RetriesDoNotConsumeThe5xxBudget(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch attempts.Add(1) {
			case 1, 2:
				w.WriteHeader(http.StatusRequestTimeout)
			case 3:
				w.WriteHeader(http.StatusServiceUnavailable)
			default:
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[{"stake_address":"stake1x"}]`))
			}
		}),
	)
	defer srv.Close()

	k := newTestKoiosClientFast408(srv.URL, 16)
	resp, err := k.post(
		context.Background(),
		"/account_reward_history",
		map[string]any{"_stake_addresses": []string{"stake1x"}},
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.EqualValues(t, 4, attempts.Load())
}
