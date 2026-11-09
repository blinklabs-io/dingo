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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeAccountList serves /account_list over a fixed address list the way the
// production endpoint does: Range-offset windows over the default order, and
// nothing else. A request carrying an order or filter is refused, because the
// real host takes tens of seconds to answer one.
type fakeAccountList struct {
	mu       sync.Mutex
	addrs    []string
	starts   []int
	requests int
	// onPage runs before the nth request is answered and may change addrs
	// through setAddrs.
	onPage func(request int, r *http.Request)
}

func newFakeAccountList(n int) *fakeAccountList {
	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("stake1u%06d", i)
	}
	return &fakeAccountList{addrs: addrs}
}

func (f *fakeAccountList) setAddrs(addrs []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addrs = addrs
}

func (f *fakeAccountList) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.addrs...)
}

func (f *fakeAccountList) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/account_list" {
			http.NotFound(w, r)
			return
		}
		if r.URL.Query().Get("order") != "" ||
			r.URL.Query().Get("stake_address") != "" {
			http.Error(w, "ordered page too slow", http.StatusGatewayTimeout)
			return
		}
		parts := strings.SplitN(r.Header.Get("Range"), "-", 2)
		start, _ := strconv.Atoi(parts[0])
		end, _ := strconv.Atoi(parts[1])
		f.mu.Lock()
		f.requests++
		n := f.requests
		f.starts = append(f.starts, start)
		hook := f.onPage
		f.mu.Unlock()
		if hook != nil {
			hook(n, r)
		}
		rows := f.snapshot()
		start = min(start, len(rows))
		page := rows[start:min(end+1, len(rows))]
		var sb strings.Builder
		sb.WriteByte('[')
		for i, a := range page {
			if i > 0 {
				sb.WriteByte(',')
			}
			fmt.Fprintf(&sb, `{"stake_address":%q}`, a)
		}
		sb.WriteByte(']')
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sb.String()))
	})
}

func (f *fakeAccountList) seenStarts() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.starts...)
}

// interruptedCrawl runs a crawl that is cancelled while its third page is
// being requested, leaving two persisted pages, and returns the cache path.
func interruptedCrawl(
	t *testing.T,
	fake *fakeAccountList,
	koios *KoiosClient,
	epochEnd time.Time,
) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake.onPage = func(n int, r *http.Request) {
		if n == 3 {
			cancel()
			// Hold the answer until the client has seen the cancellation. A
			// short final page delivered first would let the crawl finish
			// before the context is checked again.
			<-r.Context().Done()
		}
	}
	path := filepath.Join(t.TempDir(), "cache.db")
	cache, err := openTestCache(path, nil)
	require.NoError(t, err)
	_, err = ResolveKoiosAccountUniverseCached(
		ctx, koios, cache, "preview", epochEnd,
		slog.New(slog.DiscardHandler),
	)
	require.Error(t, err, "the interrupted crawl must report failure")
	require.NoError(t, cache.Close())
	fake.onPage = nil
	return path
}

// TestAccountUniverseCrawlResumesAfterInterruption restarts the process
// mid-crawl and requires the second run to continue from the persisted offset
// instead of walking from the start.
func TestAccountUniverseCrawlResumesAfterInterruption(t *testing.T) {
	t.Parallel()

	fake := newFakeAccountList(2*koiosPageSize + 500)
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	koios := newUniverseTestClient(srv)
	epochEnd := time.Now().Add(-time.Hour)
	path := interruptedCrawl(t, fake, koios, epochEnd)

	cache, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck
	addrs, err := ResolveKoiosAccountUniverseCached(
		context.Background(), koios, cache, "preview", epochEnd,
		slog.New(slog.DiscardHandler),
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, fake.snapshot(), addrs,
		"resumed crawl must return every account exactly once")
	assert.Len(t, addrs, 2*koiosPageSize+500)

	// Pages 1 and 2 cover rows 0..1999; the resumed request starts one row
	// early to check alignment.
	starts := fake.seenStarts()
	require.GreaterOrEqual(t, len(starts), 4)
	assert.Equal(t, 1999, starts[3],
		"the restarted crawl must continue at the persisted offset, "+
			"not from the start (range starts requested: %v)", starts)
	assert.Equal(t, 1, countInts(starts, 0),
		"pages persisted before the interruption must not be refetched")
}

func countInts(vals []int, want int) int {
	n := 0
	for _, v := range vals {
		if v == want {
			n++
		}
	}
	return n
}

// TestAccountUniverseCrawlDetectsDriftOnResume shifts the list while the
// crawl is interrupted. Resuming at the saved offset would repeat and skip
// accounts, so the crawl has to notice and start over.
func TestAccountUniverseCrawlDetectsDriftOnResume(t *testing.T) {
	t.Parallel()

	fake := newFakeAccountList(2*koiosPageSize + 500)
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	koios := newUniverseTestClient(srv)
	epochEnd := time.Now().Add(-time.Hour)
	path := interruptedCrawl(t, fake, koios, epochEnd)

	shifted := append([]string{"stake1u-new-0", "stake1u-new-1"}, fake.snapshot()...)
	fake.setAddrs(shifted)
	before := len(fake.seenStarts())

	cache, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck
	addrs, err := ResolveKoiosAccountUniverseCached(
		context.Background(), koios, cache, "preview", epochEnd,
		slog.New(slog.DiscardHandler),
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, shifted, addrs,
		"a shifted list must be recrawled whole, with nothing skipped")
	assert.Len(t, addrs, len(shifted))
	starts := fake.seenStarts()[before:]
	assert.Equal(t, []int{1999, 0}, starts[:2],
		"the shift is found by the overlap row, then the crawl restarts at 0")
}

func countRows(t *testing.T, c *Cache, table, network string) int {
	t.Helper()
	var n int
	require.NoError(t, c.db.QueryRow(
		`SELECT COUNT(*) FROM `+table+` WHERE network = ?`, network,
	).Scan(&n))
	return n
}

// TestAccountUniversePartialCrawlIsNeverAUniverse checks that an interrupted
// crawl is invisible to every consumer of the universe, and that a resolver
// whose Koios is still failing returns an error rather than the addresses
// gathered so far.
func TestAccountUniversePartialCrawlIsNeverAUniverse(t *testing.T) {
	t.Parallel()

	fake := newFakeAccountList(2*koiosPageSize + 10)
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	koios := newUniverseTestClient(srv)
	epochEnd := time.Now().Add(-time.Hour)
	path := interruptedCrawl(t, fake, koios, epochEnd)
	cache, err := openTestCache(path, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	addrs, _, cached, err := cache.GetAccountUniverse("preview")
	require.NoError(t, err)
	assert.False(t, cached, "a partial crawl must not read as a universe")
	assert.Empty(t, addrs)
	progress, err := cache.GetAccountUniverseCrawl("preview")
	require.NoError(t, err)
	require.NotNil(t, progress)
	assert.Equal(t, 2, progress.Pages)
	assert.Equal(t, 2*koiosPageSize, progress.Addresses)
	assert.Equal(t, 2*koiosPageSize, progress.Offset)
	assert.Equal(t, fake.addrs[2*koiosPageSize-1], progress.LastAddress)

	failing := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
		},
	))
	defer failing.Close()
	got, err := ResolveKoiosAccountUniverseCached(
		context.Background(), newUniverseTestClient(failing), cache,
		"preview", epochEnd, slog.New(slog.DiscardHandler),
	)
	require.Error(t, err)
	assert.Nil(t, got)
	_, _, cached, err = cache.GetAccountUniverse("preview")
	require.NoError(t, err)
	assert.False(t, cached)
}

// TestAccountUniverseCrawlCompletionIsPersisted checks the last page's commit:
// the universe becomes visible with the crawl's start time, the partial rows
// are gone, and a restarted process reuses it without calling Koios.
func TestAccountUniverseCrawlCompletionIsPersisted(t *testing.T) {
	t.Parallel()

	fake := newFakeAccountList(koiosPageSize + 1)
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	koios := newUniverseTestClient(srv)
	path := filepath.Join(t.TempDir(), "cache.db")
	logger := slog.New(slog.DiscardHandler)
	epochEnd := time.Now().Add(-time.Hour)

	cache, err := openTestCache(path, nil)
	require.NoError(t, err)
	before := time.Now().Add(-time.Second)
	addrs, err := ResolveKoiosAccountUniverseCached(
		context.Background(), koios, cache, "preview", epochEnd, logger,
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, fake.snapshot(), addrs)
	assert.Zero(t, countRows(t, cache, "koios_account_universe_partial", "preview"))
	assert.Zero(t, countRows(t, cache, "koios_account_universe_crawl", "preview"))
	require.NoError(t, cache.Close())

	cache, err = openTestCache(path, nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck
	stored, fetchedAt, cached, err := cache.GetAccountUniverse("preview")
	require.NoError(t, err)
	require.True(t, cached)
	assert.ElementsMatch(t, fake.snapshot(), stored)
	assert.True(t, fetchedAt.After(before), "stamped with the crawl's start")
	requests := len(fake.seenStarts())
	_, err = ResolveKoiosAccountUniverseCached(
		context.Background(), koios, cache, "preview", epochEnd, logger,
	)
	require.NoError(t, err)
	assert.Equal(t, requests, len(fake.seenStarts()),
		"a completed universe is not crawled again")
}

// TestAccountUniverseCrawlEmptyList covers a list with no accounts at all:
// there is no page to carry the start time, yet the crawl still completes.
func TestAccountUniverseCrawlEmptyList(t *testing.T) {
	t.Parallel()

	fake := newFakeAccountList(0)
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	cache, err := openTestCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	addrs, err := ResolveKoiosAccountUniverseCached(
		context.Background(), newUniverseTestClient(srv), cache, "preview",
		time.Now().Add(-time.Hour), slog.New(slog.DiscardHandler),
	)
	require.NoError(t, err)
	assert.Empty(t, addrs)
	_, _, cached, err := cache.GetAccountUniverse("preview")
	require.NoError(t, err)
	assert.True(t, cached)
}

// TestAccountUniverseCrawlDiscardsUnusableProgress covers the two reasons a
// persisted crawl is not resumed: it was taken against another query, or it
// started before the epoch being checked ended.
func TestAccountUniverseCrawlDiscardsUnusableProgress(t *testing.T) {
	t.Parallel()

	for name, tc := range map[string]struct {
		query   string
		started time.Time
	}{
		"other query": {
			"/account_list?select=stake_address&order=stake_address.asc",
			time.Now(),
		},
		"started before epoch end": {
			accountListQuery, time.Now().Add(-48 * time.Hour),
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fake := newFakeAccountList(300)
			srv := httptest.NewServer(fake.handler())
			defer srv.Close()
			cache, err := openTestCache(
				filepath.Join(t.TempDir(), "cache.db"), nil,
			)
			require.NoError(t, err)
			defer cache.Close() //nolint:errcheck
			require.NoError(t, cache.SaveAccountUniverseCrawlPage(
				"preview", tc.query, tc.started,
				[]string{"stake1ustale"}, 1, "stake1ustale", 1,
			))

			addrs, err := ResolveKoiosAccountUniverseCached(
				context.Background(), newUniverseTestClient(srv), cache,
				"preview", time.Now().Add(-time.Hour),
				slog.New(slog.DiscardHandler),
			)
			require.NoError(t, err)
			assert.ElementsMatch(t, fake.snapshot(), addrs,
				"rows from the discarded crawl must not leak into the universe")
			assert.Equal(t, []int{0}, fake.seenStarts())
		})
	}
}

// TestAccountListUsesLongPageTimeout checks that /account_list pages get the
// longer timeout: a deep offset page legitimately takes longer than the
// general request bound, and failing it would leave the crawl unable to pass
// that page.
func TestAccountListUsesLongPageTimeout(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(300 * time.Millisecond)
			_, _ = w.Write([]byte(`[{"stake_address":"stake1ua"}]`))
		},
	))
	defer srv.Close()
	k := newUniverseTestClient(srv)
	k.http = &http.Client{
		Timeout:   50 * time.Millisecond,
		Transport: &http.Transport{},
	}
	k.accountListHTTP = &http.Client{Timeout: 5 * time.Second}

	addrs, err := k.GetAllAccountAddresses(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"stake1ua"}, addrs)
}

// TestWalkAccountListDetectsMisalignedOverlap drives the walk directly: a
// resume point whose last address is not at the row before the offset must be
// reported as drift, not consumed.
func TestWalkAccountListDetectsMisalignedOverlap(t *testing.T) {
	t.Parallel()

	fake := newFakeAccountList(50)
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	err := newUniverseTestClient(srv).walkAccountList(
		context.Background(),
		accountListPosition{Offset: 10, LastAddress: "stake1u-not-there"},
		func([]string, accountListPosition, int) error {
			t.Error("a misaligned page must not be delivered")
			return nil
		},
	)
	require.ErrorIs(t, err, errAccountListDrifted)
}

type countingTransport struct {
	next http.RoundTripper
	n    atomic.Int32
}

func (c *countingTransport) RoundTrip(
	r *http.Request,
) (*http.Response, error) {
	c.n.Add(1)
	return c.next.RoundTrip(r)
}

// TestAccountListHonoursWrappedTransport checks that a client whose transport
// was wrapped still routes /account_list through the wrapper rather than
// through the long-timeout client's own transport.
func TestAccountListHonoursWrappedTransport(t *testing.T) {
	t.Parallel()

	fake := newFakeAccountList(3)
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	k := newUniverseTestClient(srv)
	wrapped := &countingTransport{next: http.DefaultTransport}
	k.http.Transport = wrapped
	k.accountListHTTP = &http.Client{Timeout: 5 * time.Second}

	addrs, err := k.GetAllAccountAddresses(context.Background())
	require.NoError(t, err)
	assert.Len(t, addrs, 3)
	assert.Equal(t, int32(1), wrapped.n.Load())
}

// TestWalkAccountListAdvancesPastEmptyAddressRows serves a full first page
// whose rows all carry an empty stake_address. The walk must still move its
// offset past those rows; refetching the same window would never end.
func TestWalkAccountListAdvancesPastEmptyAddressRows(t *testing.T) {
	t.Parallel()

	fake := newFakeAccountList(koiosPageSize + 3)
	addrs := fake.snapshot()
	for i := range koiosPageSize {
		addrs[i] = ""
	}
	fake.setAddrs(addrs)
	srv := httptest.NewServer(fake.handler())
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var got []string
	var last accountListPosition
	err := newUniverseTestClient(srv).walkAccountList(
		ctx, accountListPosition{},
		func(page []string, next accountListPosition, _ int) error {
			got = append(got, page...)
			last = next
			return nil
		},
	)
	require.NoError(t, err)
	assert.Equal(t, addrs[koiosPageSize:], got)
	assert.Equal(t, []int{0, koiosPageSize}, fake.seenStarts())
	assert.Equal(t, accountListPosition{
		Offset:      koiosPageSize + 3,
		LastAddress: addrs[koiosPageSize+2],
	}, last)
}
