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

package koiosparity

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newAccountListServer serves a one-page /account_list and counts the requests
// it answers, so a test can assert how many times the universe was crawled.
func newAccountListServer(
	t *testing.T,
	calls *atomic.Int32,
	addrs ...string,
) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/account_list" {
				http.NotFound(w, r)
				return
			}
			calls.Add(1)
			var sb strings.Builder
			sb.WriteByte('[')
			for i, addr := range addrs {
				if i > 0 {
					sb.WriteByte(',')
				}
				fmt.Fprintf(&sb, `{"stake_address":%q}`, addr)
			}
			sb.WriteByte(']')
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(sb.String()))
		}),
	)
	t.Cleanup(srv.Close)
	return srv
}

func newUniverseTestClient(srv *httptest.Server) *KoiosClient {
	return &KoiosClient{
		baseURL: srv.URL,
		http:    &http.Client{Timeout: 2 * time.Second},
		limiter: newBurstLimiter(0, koiosBurstWindow),
	}
}

// TestResolveKoiosAccountUniverseCachedReusesCrawlAcrossEpochs is the point of
// the cache. The crawl is 304 sequential /account_list requests on Preview, and
// paying it once per epoch is why the in-process observer could not keep pace
// with a syncing node (dingo #3796). A second epoch whose end time the cached
// crawl already covers must not touch Koios again.
func TestResolveKoiosAccountUniverseCachedReusesCrawlAcrossEpochs(t *testing.T) {
	var calls atomic.Int32
	srv := newAccountListServer(t, &calls, "stake_test1a", "stake_test1b")
	koios := newUniverseTestClient(srv)
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	epochEnd := time.Now().Add(-time.Hour)
	logger := slog.New(slog.DiscardHandler)

	first, err := ResolveKoiosAccountUniverseCached(
		context.Background(), koios, cache, "preview", epochEnd, logger,
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"stake_test1a", "stake_test1b"}, first)
	require.Equal(t, int32(1), calls.Load())

	// A later epoch that also ended before the crawl is fully covered by it.
	second, err := ResolveKoiosAccountUniverseCached(
		context.Background(), koios, cache, "preview",
		epochEnd.Add(30*time.Minute), logger,
	)
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.Equal(t, int32(1), calls.Load(),
		"a cached crawl covering the epoch must not be re-fetched")

	// Control: the uncached resolver the observer used to call directly does
	// crawl again, so the counter above is measuring what it claims to.
	_, err = ResolveKoiosAccountUniverse(context.Background(), koios)
	require.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load())
}

// TestResolveKoiosAccountUniverseCachedRefreshesForNewerEpoch is the other
// half. An account that earned a reward in an epoch registered before that
// epoch ended, so a crawl taken before the epoch closed may be missing one and
// cannot be reused — a short universe silently skips accounts, which reads as
// a pass.
func TestResolveKoiosAccountUniverseCachedRefreshesForNewerEpoch(t *testing.T) {
	var calls atomic.Int32
	srv := newAccountListServer(t, &calls, "stake_test1a")
	koios := newUniverseTestClient(srv)
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	logger := slog.New(slog.DiscardHandler)
	_, err = ResolveKoiosAccountUniverseCached(
		context.Background(), koios, cache, "preview",
		time.Now().Add(-time.Hour), logger,
	)
	require.NoError(t, err)
	require.Equal(t, int32(1), calls.Load())

	// An epoch that closed after the crawl was taken.
	_, err = ResolveKoiosAccountUniverseCached(
		context.Background(), koios, cache, "preview",
		time.Now().Add(time.Hour), logger,
	)
	require.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load(),
		"a crawl older than the epoch's close must be refreshed")
}

// TestAccountUniverseCacheRoundTrip pins the storage contract the resolver
// relies on: a save replaces the previous set wholesale rather than merging
// into it, so a shrinking universe cannot leave a stale address behind.
func TestAccountUniverseCacheRoundTrip(t *testing.T) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	addrs, fetchedAt, cached, err := cache.GetAccountUniverse("preview")
	require.NoError(t, err)
	assert.Empty(t, addrs)
	assert.False(t, cached, "nothing cached yet")
	assert.True(t, fetchedAt.IsZero())

	first := time.Now().Add(-time.Hour).UTC().Truncate(time.Second)
	require.NoError(t, cache.SaveAccountUniverse(
		"preview", []string{"stake_test1a", "stake_test1b"}, first,
	))
	require.NoError(t, cache.SaveAccountUniverse(
		"preprod", []string{"stake_test1z"}, first,
	))

	addrs, fetchedAt, cached, err = cache.GetAccountUniverse("preview")
	require.NoError(t, err)
	assert.True(t, cached)
	assert.Equal(t, []string{"stake_test1a", "stake_test1b"}, addrs)
	assert.WithinDuration(t, first, fetchedAt, time.Second)

	second := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, cache.SaveAccountUniverse(
		"preview", []string{"stake_test1b"}, second,
	))
	addrs, fetchedAt, cached, err = cache.GetAccountUniverse("preview")
	require.NoError(t, err)
	assert.True(t, cached)
	assert.Equal(t, []string{"stake_test1b"}, addrs,
		"a save replaces the set rather than merging into it")
	assert.WithinDuration(t, second, fetchedAt, time.Second)

	other, _, cached, err := cache.GetAccountUniverse("preprod")
	require.NoError(t, err)
	assert.True(t, cached)
	assert.Equal(t, []string{"stake_test1z"}, other,
		"networks are stored independently")
}

// TestResolveKoiosAccountUniverseCachedWithEmptyCrawl covers a network whose
// /account_list is legitimately empty. Presence is recorded separately from the
// address rows, so an empty crawl is still a cached crawl and a later epoch it
// covers does not pay for it again.
func TestResolveKoiosAccountUniverseCachedWithEmptyCrawl(t *testing.T) {
	var calls atomic.Int32
	srv := newAccountListServer(t, &calls)
	koios := newUniverseTestClient(srv)
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	epochEnd := time.Now().Add(-time.Hour)
	logger := slog.New(slog.DiscardHandler)
	for range 2 {
		addrs, err := ResolveKoiosAccountUniverseCached(
			context.Background(), koios, cache, "preview", epochEnd, logger,
		)
		require.NoError(t, err)
		assert.Empty(t, addrs)
	}
	assert.Equal(t, int32(1), calls.Load(),
		"an empty crawl is still a cached crawl")
}

// TestResolveKoiosAccountUniverseCachedRefusesUnboundedReuse covers an epoch
// whose end time the cache does not carry. There is then nothing to measure the
// crawl against, and reusing it anyway could skip an account that registered
// between the crawl and the epoch's close — a short universe reads as a pass.
func TestResolveKoiosAccountUniverseCachedRefusesUnboundedReuse(t *testing.T) {
	var calls atomic.Int32
	srv := newAccountListServer(t, &calls, "stake_test1a")
	koios := newUniverseTestClient(srv)
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck
	logger := slog.New(slog.DiscardHandler)

	for range 2 {
		_, err = ResolveKoiosAccountUniverseCached(
			context.Background(), koios, cache, "preview", time.Time{}, logger,
		)
		require.NoError(t, err)
	}
	assert.Equal(t, int32(2), calls.Load(),
		"with no bound to measure against, the crawl is not reused")

	assert.False(t, accountUniverseFresh(time.Time{}, time.Now()),
		"no crawl is never fresh")
	assert.False(t, accountUniverseFresh(time.Now(), time.Time{}),
		"no bound is never fresh")
	assert.True(t, accountUniverseFresh(
		time.Now(), time.Now().Add(-time.Minute),
	))
}
