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
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPostRejectsOversizedResponseAsPermanentNeverRetried proves dingo
// #3099's response-size bound (readBodyLimited/koiosMaxResponseBytes):
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

func TestNewKoiosClientWiresPrivateAddressGuard(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	blocked := &http.Client{Transport: newKoiosTransport(
		koiosDialTimeout, koiosDialKeepAlive, koiosTLSHandshakeTimeout,
		koiosResponseHeaderTimeout, koiosExpectContinueTimeout, false,
	)}
	_, err := blocked.Get(srv.URL)
	require.Error(t, err)

	allowed := &http.Client{Transport: newKoiosTransport(
		koiosDialTimeout, koiosDialKeepAlive, koiosTLSHandshakeTimeout,
		koiosResponseHeaderTimeout, koiosExpectContinueTimeout, true,
	)}
	resp, err := allowed.Get(srv.URL)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NoError(t, resp.Body.Close())
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

func TestIsDailyQuotaExceeded(t *testing.T) {
	t.Parallel()

	require.True(t, isDailyQuotaExceeded("Exceeded Tier Limit"))
	require.True(t, isDailyQuotaExceeded("error: Exceeded Tier Limit\n"))
	require.False(t, isDailyQuotaExceeded("Too many requests"))
	require.False(t, isDailyQuotaExceeded(""))
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

func TestGetAccountRewardHistoryDecodesResponse(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodPost, r.Method)
			require.Equal(t, "/account_reward_history", r.URL.Path)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[
				{"stake_address":"stake1x","earned_epoch":100,"spendable_epoch":102,"amount":"1000000","type":"member","pool_id_bech32":null},
				{"stake_address":"stake1y","earned_epoch":100,"spendable_epoch":102,"amount":"5000000","type":"leader","pool_id_bech32":"pool1abc"}
			]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	items, err := k.GetAccountRewardHistory(
		context.Background(),
		[]string{"stake1x", "stake1y"},
		100,
	)
	require.NoError(t, err)
	require.Len(t, items, 2)
	require.Equal(t, "member", items[0].Type)
	require.Nil(t, items[0].PoolIDBech32)
	require.Equal(t, "leader", items[1].Type)
	require.NotNil(t, items[1].PoolIDBech32)
	require.Equal(t, "pool1abc", *items[1].PoolIDBech32)
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
	require.Error(t, err)
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
}

// TestNewKoiosTransportResponseHeaderTimeout proves newKoiosTransport's
// ResponseHeaderTimeout is actually enforced by the transport itself, not
// merely present as a struct field: a server that accepts the connection but
// blocks well past the configured ResponseHeaderTimeout before writing
// anything must fail the request in roughly that bound, not in the far
// longer overall http.Client.Timeout also configured on the same client.
//
// This is the phase http.DefaultTransport leaves unbounded (see
// newKoiosTransport's doc comment): dial and TLS handshake already have
// independent defaults, so this test is the one that would fail today
// against the pre-fix client, which relied solely on the coarse top-level
// Timeout to catch a stuck server.
func TestNewKoiosTransportResponseHeaderTimeout(t *testing.T) {
	t.Parallel()

	const (
		responseHeaderTimeout = 200 * time.Millisecond
		serverDelay           = 3 * time.Second
		clientTimeout         = 10 * time.Second // must stay >> responseHeaderTimeout
	)

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Simulate a server that accepts the connection and then never
			// writes anything for far longer than responseHeaderTimeout:
			// no headers, no body, just silence on an already-open
			// connection.
			time.Sleep(serverDelay)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("too late"))
		}),
	)
	defer srv.Close()

	client := &http.Client{
		// Deliberately far larger than responseHeaderTimeout: if the
		// request only failed once this fired, this test would prove
		// nothing beyond what the client already did before this change.
		Timeout: clientTimeout,
		Transport: newKoiosTransport(
			koiosDialTimeout,
			koiosDialKeepAlive,
			koiosTLSHandshakeTimeout,
			responseHeaderTimeout,
			koiosExpectContinueTimeout,
			true,
		),
	}

	start := time.Now()
	resp, err := client.Get(srv.URL) //nolint:noctx // deliberately no context; client.Timeout/transport timeouts are exactly what's under test
	elapsed := time.Since(start)
	if resp != nil {
		resp.Body.Close()
	}

	require.Error(
		t,
		err,
		"a server silent past ResponseHeaderTimeout must fail the request",
	)
	assert.Contains(
		t,
		err.Error(),
		"timeout awaiting response headers",
		"failure must be attributable to ResponseHeaderTimeout specifically",
	)
	assert.Less(
		t,
		elapsed,
		serverDelay,
		"must fail before the server ever responds",
	)
	assert.Less(
		t,
		elapsed,
		clientTimeout,
		"must fail well before the unrelated, much larger client-level Timeout",
	)
}

// TestNewKoiosTransportDialTimeout proves newKoiosTransport's DialContext
// timeout is enforced: connecting to an address that never completes the TCP
// handshake (nothing there to accept or refuse it) must fail within roughly
// the configured dial timeout, not the far longer client-level Timeout.
//
// A non-routable address (RFC 5737 TEST-NET-1, reserved for documentation
// and guaranteed never to route) stands in for "a firewall silently drops
// SYN/ACK": the dial attempt neither succeeds nor is immediately refused, so
// only an explicit dial timeout bounds it.
//
// Three properties distinguish this from a coincidentally-fast, differently
// caused failure:
//
//  1. The proxy environment variables net/http's ProxyFromEnvironment (part
//     of http.DefaultTransport, which newKoiosTransport clones) consults are
//     cleared for the duration of the test. An HTTP_PROXY/http_proxy set in
//     the test's execution environment would make the client dial the proxy
//     instead of 192.0.2.1:81, so the request could fail fast (or succeed)
//     through the proxy without ever exercising the 300ms dial bound at all
//     — passing the test for the wrong reason, and inconsistently depending
//     on the environment it runs in.
//  2. The returned error must classify as a dial timeout specifically (a
//     net.Error whose Timeout() reports true), not merely "some error" — a
//     fast "connection refused" or "network unreachable" would satisfy the
//     old require.Error(...) assertion without ever hitting the 300ms bound
//     (confirmed by contrast: dialing a closed local port instead returns a
//     non-timeout "connection refused" with Timeout() false). This dial
//     timeout does not reliably chain os.ErrDeadlineExceeded — verified
//     empirically to surface it in most but not all runs, depending on
//     which of two internal error paths net.Dialer's deadline hits — so
//     Timeout() is the classification actually asserted here.
//  3. elapsed is bounded tightly against dialTimeout itself, not merely
//     against the unrelated, far larger client-level Timeout. Measured
//     dial-timeout failures land within a few milliseconds of dialTimeout
//     (300ms); a bound that only excludes clientTimeout (10s) would not
//     distinguish this from, say, http.DefaultTransport's own 30s dial
//     default happening to lose a race against something else.
func TestNewKoiosTransportDialTimeout(t *testing.T) {
	// Not t.Parallel: t.Setenv (below) forbids it, and the whole point of
	// clearing these process-wide proxy env vars is to isolate this test
	// from whatever the ambient environment has set.

	// Cleared rather than left to the ambient environment: any of these set
	// in the test's execution environment would redirect the dial through a
	// proxy instead of exercising DialContext's own timeout (see doc comment
	// point 1).
	for _, key := range []string{
		"HTTP_PROXY", "http_proxy",
		"HTTPS_PROXY", "https_proxy",
		"NO_PROXY", "no_proxy",
	} {
		t.Setenv(key, "")
	}

	const (
		dialTimeout   = 300 * time.Millisecond
		clientTimeout = 10 * time.Second // must stay >> dialTimeout
	)

	client := &http.Client{
		Timeout: clientTimeout,
		Transport: newKoiosTransport(
			dialTimeout,
			koiosDialKeepAlive,
			koiosTLSHandshakeTimeout,
			koiosResponseHeaderTimeout,
			koiosExpectContinueTimeout,
			true,
		),
	}

	start := time.Now()
	//nolint:noctx // deliberately no context; the transport's own dial timeout is exactly what's under test
	resp, err := client.Get("http://192.0.2.1:81/")
	elapsed := time.Since(start)
	if resp != nil {
		resp.Body.Close()
	}

	require.Error(t, err, "a black-holed dial target must fail the request")

	var netErr net.Error
	require.ErrorAs(
		t,
		err,
		&netErr,
		"failure must be a net.Error, not some other error shape",
	)
	assert.True(
		t,
		netErr.Timeout(),
		"failure must classify as a timeout, not e.g. a fast refusal/unreachable error",
	)

	assert.Less(
		t,
		elapsed,
		clientTimeout,
		"must fail well before the unrelated, much larger client-level Timeout",
	)
	assert.Less(
		t,
		elapsed,
		2*dialTimeout,
		"must fail close to the configured dial timeout itself, not merely "+
			"before the far larger client-level Timeout or DefaultTransport's "+
			"own 30s dial default",
	)
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

// TestNewKoiosTransportDoesNotConsultNonHTTPDefaultTransport proves the
// security-owned transport does not depend on the process-global default's
// concrete type or behavior.
func TestNewKoiosTransportDoesNotConsultNonHTTPDefaultTransport(
	t *testing.T,
) {
	// Not t.Parallel: swaps the process-global http.DefaultTransport.
	original := http.DefaultTransport
	http.DefaultTransport = fakeRoundTripper{}
	t.Cleanup(func() { http.DefaultTransport = original })

	require.NotPanics(t, func() {
		transport := newKoiosTransport(
			koiosDialTimeout,
			koiosDialKeepAlive,
			koiosTLSHandshakeTimeout,
			koiosResponseHeaderTimeout,
			koiosExpectContinueTimeout,
			false,
		)
		require.NotNil(t, transport)
		assert.NotNil(
			t,
			transport.DialContext,
			"security-owned transport must get the configured DialContext",
		)
		assert.True(t, transport.ForceAttemptHTTP2)
		assert.Equal(t, 100, transport.MaxIdleConns)
		assert.Equal(t, koiosIdleConnTimeout, transport.IdleConnTimeout)
		assert.Equal(
			t,
			koiosTLSHandshakeTimeout,
			transport.TLSHandshakeTimeout,
		)
		assert.Equal(
			t,
			koiosResponseHeaderTimeout,
			transport.ResponseHeaderTimeout,
		)
		assert.Equal(
			t,
			koiosExpectContinueTimeout,
			transport.ExpectContinueTimeout,
		)
	})
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
