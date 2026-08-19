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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/stretchr/testify/require"
)

// TestFetchAccountRewardsForEpochChunksAndCommits proves the address universe
// is split into koiosAccountChunkSize-sized requests and every returned row
// is committed atomically, with coverage marked complete.
func TestFetchAccountRewardsForEpochChunksAndCommits(t *testing.T) {
	var reqCount atomic.Int32
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqCount.Add(1)
			var body struct {
				StakeAddresses []string `json:"_stake_addresses"`
				EpochNo        uint64   `json:"_epoch_no"`
			}
			// require.* must never run inside an HTTP handler goroutine: a
			// failure calls t.FailNow, which is only valid on the goroutine
			// running the test function itself — here it would abort the
			// handler without writing a response, hanging the client until
			// timeout instead of failing cleanly. Use t.Errorf plus an
			// explicit error response instead.
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode request body: %v", err)
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if body.EpochNo != 100 {
				t.Errorf("unexpected epoch_no: got %d, want 100", body.EpochNo)
				http.Error(w, "unexpected epoch_no", http.StatusBadRequest)
				return
			}
			var sb strings.Builder
			sb.WriteByte('[')
			for i, addr := range body.StakeAddresses {
				if i > 0 {
					sb.WriteByte(',')
				}
				fmt.Fprintf(
					&sb,
					`{"stake_address":%q,"earned_epoch":100,"amount":"1000000","type":"member"}`,
					addr,
				)
			}
			sb.WriteByte(']')
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(sb.String()))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	// koiosAccountChunkSize addresses + 1 extra to force two chunks/requests.
	addrs := make([]string, koiosAccountChunkSize+1)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("stake1addr%d", i)
	}

	fetched, err := FetchAccountRewardsForEpoch(
		context.Background(),
		k,
		cache,
		"preview",
		100,
		addrs,
		time.Time{},
		0,
		nil,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		len(addrs),
		fetched,
	) // one reward row per requested address
	require.EqualValues(t, 2, reqCount.Load())

	cov, err := cache.GetAccountCoverage("preview", 100)
	require.NoError(t, err)
	require.True(t, cov.Complete)
	require.Equal(t, len(addrs), cov.RequestedCount)
	require.Equal(t, len(addrs), cov.FetchedCount)
}

// TestFetchAccountRewardsForEpochEmptyUniverseCommitsComplete proves an empty
// address universe still commits a complete coverage record rather than
// leaving the epoch perpetually "not fetched".
func TestFetchAccountRewardsForEpochEmptyUniverseCommitsComplete(t *testing.T) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	k := newTestKoiosClient("http://unused.invalid")
	fetched, err := FetchAccountRewardsForEpoch(
		context.Background(),
		k,
		cache,
		"preview",
		100,
		nil,
		time.Time{},
		0,
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 0, fetched)

	cov, err := cache.GetAccountCoverage("preview", 100)
	require.NoError(t, err)
	require.True(t, cov.Complete)
	require.Equal(t, 0, cov.RequestedCount)
}

// TestFetchAccountRewardsForEpochTransientChunkFailureCommitsNothing proves a
// single failed chunk aborts the whole fetch (nothing committed, coverage
// left absent) rather than committing a partial, silently-"complete" result.
func TestFetchAccountRewardsForEpochTransientChunkFailureCommitsNothing(
	t *testing.T,
) {
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("no server available"))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	_, err = FetchAccountRewardsForEpoch(
		context.Background(),
		k,
		cache,
		"preview",
		100,
		[]string{"stake1a"},
		time.Time{},
		0,
		nil,
	)
	require.Error(t, err)
	require.False(t, errors.Is(err, ErrKoiosPermanent))

	_, covErr := cache.GetAccountCoverage("preview", 100)
	require.Error(t, covErr) // no coverage row committed at all
}

// TestFetchAccountRewardsForEpochPermanentErrorAbortsImmediately proves a
// permanent Koios error (daily quota/auth) is returned unwrapped (still
// classified permanent) so callers abort rather than retry.
func TestFetchAccountRewardsForEpochPermanentErrorAbortsImmediately(
	t *testing.T,
) {
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte("unauthorized"))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	_, err = FetchAccountRewardsForEpoch(
		context.Background(),
		k,
		cache,
		"preview",
		100,
		[]string{"stake1a"},
		time.Time{},
		0,
		nil,
	)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrKoiosPermanent))
}

// TestFetchAccountRewardsForEpochZeroRowsWithinGraceLeavesIncomplete proves a
// just-closed epoch (EpochEndTime within graceHours of now) whose #3097
// account fetch returns zero rows across the whole address universe is left
// with coverage incomplete rather than permanently accepted as "zero
// accounts earned rewards" — Koios's own /account_reward_history publishing
// lag is far more likely, and GetEpochsMissingAccountCoverage never
// re-selects an epoch whose coverage row already reports complete=1 (see
// FetchAccountRewardsForEpoch's doc comment).
func TestFetchAccountRewardsForEpochZeroRowsWithinGraceLeavesIncomplete(
	t *testing.T,
) {
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	fetched, err := FetchAccountRewardsForEpoch(
		context.Background(),
		k,
		cache,
		"preview",
		100,
		[]string{"stake1a"},
		time.Now().Add(-time.Hour), // closed 1h ago
		24,                         // graceHours
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 0, fetched)

	cov, err := cache.GetAccountCoverage("preview", 100)
	require.NoError(t, err)
	require.False(
		t,
		cov.Complete,
		"a zero-row result within the grace window must not be accepted as final",
	)
	require.Equal(t, 0, cov.FetchedCount)
}

// TestFetchAccountRewardsForEpochZeroRowsPastGraceMarksComplete proves a
// zero-row result is accepted as final (complete=true) once the grace window
// has elapsed, so a genuinely empty epoch is not retried forever.
func TestFetchAccountRewardsForEpochZeroRowsPastGraceMarksComplete(
	t *testing.T,
) {
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	fetched, err := FetchAccountRewardsForEpoch(
		context.Background(),
		k,
		cache,
		"preview",
		100,
		[]string{"stake1a"},
		time.Now().Add(-100*time.Hour), // closed long ago
		24,                             // graceHours
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, 0, fetched)

	cov, err := cache.GetAccountCoverage("preview", 100)
	require.NoError(t, err)
	require.True(
		t,
		cov.Complete,
		"a zero-row result past the grace window must be accepted as final",
	)
}

// TestFetchAccountRewardsForEpochZeroRowsGraceDisabledMarksComplete proves
// graceHours=0 disables the lag gate entirely (immediate complete=true on a
// zero-row result, matching this function's pre-grace-gate behavior), for
// any caller that explicitly passes 0.
func TestFetchAccountRewardsForEpochZeroRowsGraceDisabledMarksComplete(
	t *testing.T,
) {
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	_, err = FetchAccountRewardsForEpoch(
		context.Background(),
		k,
		cache,
		"preview",
		100,
		[]string{"stake1a"},
		time.Now().Add(-time.Hour),
		0, // graceHours disabled
		nil,
	)
	require.NoError(t, err)

	cov, err := cache.GetAccountCoverage("preview", 100)
	require.NoError(t, err)
	require.True(t, cov.Complete)
}

// TestFetchEpochAccountsWithAddrsLooksUpEpochEndTimeFromCache proves
// FetchEpochAccountsWithAddrs resolves epochEndTime from the epoch's
// already-committed koios_epoch_info row (rather than requiring the caller
// to pass it explicitly), so the grace-window gate above actually applies
// end-to-end through the caller every real Fetch/Observer path uses.
func TestFetchEpochAccountsWithAddrsLooksUpEpochEndTimeFromCache(t *testing.T) {
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	const network = "preview"
	const epoch = uint64(100)
	require.NoError(t, cache.CommitEpochData(KoiosEpochInfo{
		Network:      network,
		Epoch:        epoch,
		ActiveStake:  "1",
		EpochEndTime: time.Now().Add(-time.Hour), // just closed
		FetchedAt:    time.Now(),
	}, nil, nil))

	_, err = FetchEpochAccountsWithAddrs(
		context.Background(),
		k,
		cache,
		network,
		epoch,
		nil,
		[]string{"stake1a"},
		24, // graceHours
		nil,
	)
	require.NoError(t, err)

	cov, err := cache.GetAccountCoverage(network, epoch)
	require.NoError(t, err)
	require.False(
		t,
		cov.Complete,
		"the epoch's cached EpochEndTime must be looked up and used to gate the zero-row result",
	)
}

// TestBuildAccountAddressUniverseUnionsKoiosAndDingo proves the address
// universe is the union of Koios's list and Dingo's own committed
// reward_account_output addresses, not either alone — using a real DingoDB
// (RewardParitySource) against the same glebarez/sqlite fixture schema
// dingo_db_test.go/check_test.go use, per this tool's "no local mocks"
// testing convention.
func TestBuildAccountAddressUniverseUnionsKoiosAndDingo(t *testing.T) {
	dingo, gdb := openTestDingoDB(t)
	defer dingo.Close() //nolint:errcheck

	stakingKey := testPoolKeyHash(t, 0x33)
	require.NoError(t, gdb.Create(&models.RewardAccountOutput{
		Epoch:         99,
		StakingKey:    stakingKey,
		PoolKeyHash:   testPoolKeyHash(t, 0x22),
		RewardType:    "member",
		CredentialTag: 0,
		Amount:        types.Uint64(1000000),
		Spendable:     true,
	}).Error)

	wantAddr, err := StakeAddressFromCredential(stakingKey, 0)
	require.NoError(t, err)

	koiosAddrs := []string{"stake1koiosonly"}
	universe, err := BuildAccountAddressUniverse(
		context.Background(),
		dingo,
		99,
		koiosAddrs,
	)
	require.NoError(t, err)
	require.Len(t, universe, 2)
	require.Contains(t, universe, "stake1koiosonly")
	require.Contains(t, universe, wantAddr)
}

// TestBuildAccountAddressUniverseNilSourceIsKoiosOnly proves a nil source
// (no Dingo DB access configured) still returns Koios's list alone rather
// than erroring.
func TestBuildAccountAddressUniverseNilSourceIsKoiosOnly(t *testing.T) {
	universe, err := BuildAccountAddressUniverse(
		context.Background(),
		nil,
		99,
		[]string{"stake1a", "stake1b"},
	)
	require.NoError(t, err)
	require.Len(t, universe, 2)
}

func TestChunkAddresses(t *testing.T) {
	chunks := chunkAddresses([]string{"a", "b", "c", "d", "e"}, 2)
	require.Len(t, chunks, 3)
	require.Equal(t, []string{"a", "b"}, chunks[0])
	require.Equal(t, []string{"c", "d"}, chunks[1])
	require.Equal(t, []string{"e"}, chunks[2])
	require.Nil(t, chunkAddresses(nil, 2))
}

// TestFetchAccountRewardsForEpochStopsDispatchingAfterFirstChunkError guards
// against the dispatcher race flagged in review: `select { case
// <-fetchCtx.Done(): ...; case sem <- struct{}{}: }` can nondeterministically
// choose the semaphore branch even after a concurrently-running chunk's
// error has already called cancel(), because Done() and a buffered
// sem<-struct{} can both be simultaneously ready — Go's select does not
// prioritize between ready cases. Without a recheck after acquiring the
// semaphore, the dispatch loop could launch one or more further chunk
// workers after the epoch's reference set is already known to be doomed.
//
// This exercises the real dispatch loop under genuine concurrent execution
// (a real *KoiosClient against a real httptest.Server, real goroutines,
// real channel operations) rather than a hand-rolled reimplementation of the
// same select/semaphore shape, so it validates the actual production code
// path, not a model of it:
//
//   - The erroring chunk is identified by *content* (poisonAddr, placed in
//     the very first koiosAccountChunkSize addresses so it is always part of
//     chunk 0, always dispatched in the initial accountFetchConcurrency-sized
//     batch) rather than "whichever request happens to reach the server
//     first" — with arrival-order identification, *any* of the 30 chunks
//     could win the race to fail, so the count measured could include any
//     number of otherwise-successful chunks that legitimately cascaded
//     through before the unlucky one happened to run, with or without the
//     fix — not a useful signal either way.
//   - Every other chunk's response is held behind a test-controlled gate
//     that only this test ever closes, and only once the production
//     dispatch loop's own test seam (afterChunkCancelForTest) has reported —
//     via a channel close, not a fixed sleep margin — that cancel() has
//     actually already been called for the poison chunk's error. This
//     bounds every possible source of a freed semaphore slot (the poison
//     chunk's own release, and every other chunk's release once the gate
//     opens) to strictly after cancellation, so requestCount
//     — checked only once FetchAccountRewardsForEpoch has fully returned —
//     can never legitimately exceed accountFetchConcurrency; anything more
//     means a chunk was dispatched using a slot that only became available
//     post-cancellation. It can legitimately be *less*, since an
//     already-dispatched chunk still waiting on the rate limiter or
//     http.Client.Do for one of the gated chunks aborts immediately once
//     fetchCtx is cancelled, without ever completing its request.
//
// The exact nanosecond-scale race between a `close(fetchCtx.Done())` and an
// immediately-following semaphore release in the same goroutine did not
// reproduce empirically against this Go runtime even under sustained -race
// stress testing during this test's development (a mutex-guarded
// context.Context's Err() appears, in practice, to make the specific
// same-goroutine sequencing safe well before the release notification can
// reach a parked select) — but the recheck is correct, necessary per Go's
// documented select semantics for the general case, and costs nothing; this
// test's job is to confirm the dispatcher never violates the invariant under
// real adversarial concurrent execution, not to force that one specific
// interleaving to occur.
func TestFetchAccountRewardsForEpochStopsDispatchingAfterFirstChunkError(
	t *testing.T,
) {
	const totalChunks = 30
	const poisonAddr = "stake1poison"

	// cancelled is closed exactly once, synchronously, from
	// afterChunkCancelForTest — the production dispatch loop's own
	// cancel()-path test seam (see fetch_accounts.go) — so this test can
	// wait for the real cancellation to have happened deterministically
	// instead of inferring it from a fixed sleep margin, which would be
	// timing-dependent and could flake under load (per CLAUDE.md's no
	// time.Sleep-for-synchronization rule).
	cancelled := make(chan struct{})
	var cancelledOnce sync.Once
	afterChunkCancelForTest = func() {
		cancelledOnce.Do(func() { close(cancelled) })
	}
	t.Cleanup(func() { afterChunkCancelForTest = nil })

	var requestCount atomic.Int32
	poisonSeen := make(chan struct{})
	gate := make(chan struct{})

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount.Add(1)
			body, _ := io.ReadAll(r.Body)
			if strings.Contains(string(body), poisonAddr) {
				// 401 is a permanent, non-retried failure (see
				// koios_client.go's get()/post(): only 429 bursts and 5xx
				// are retried internally) — a retryable status here (e.g.
				// 503) would make the client itself resend this very same
				// chunk's request rather than surfacing an error once.
				close(poisonSeen)
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte("invalid API key"))
				return
			}
			// Every other request — including any chunk that should never
			// have been dispatched — blocks here until the test explicitly
			// opens the gate, well after cancel() is guaranteed to have
			// already fired. Nothing about reaching this point depends on
			// dispatch order or timing beyond "this request is not the
			// poison one."
			<-gate
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		}),
	)
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	addrs := make([]string, totalChunks*koiosAccountChunkSize)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("stake1addr%d", i)
	}
	addrs[0] = poisonAddr // guarantees poisonAddr is part of chunk 0

	fetchDone := make(chan error, 1)
	go func() {
		_, fetchErr := FetchAccountRewardsForEpoch(
			context.Background(),
			k,
			cache,
			"preview",
			100,
			addrs,
			time.Time{},
			0,
			nil,
		)
		fetchDone <- fetchErr
	}()

	select {
	case <-poisonSeen:
	case <-time.After(10 * time.Second):
		t.Fatal("poison chunk was never dispatched")
	}
	select {
	case <-cancelled:
	case <-time.After(10 * time.Second):
		t.Fatal("fetchCtx was never cancelled after the poison chunk's error")
	}
	close(gate)

	var fetchErr error
	select {
	case fetchErr = <-fetchDone:
	case <-time.After(10 * time.Second):
		t.Fatal(
			"FetchAccountRewardsForEpoch never returned after the gate opened",
		)
	}
	require.Error(t, fetchErr)
	require.ErrorIs(t, fetchErr, ErrKoiosPermanent)

	// requestCount can legitimately be *less* than accountFetchConcurrency:
	// once fetchCtx is cancelled, an already-dispatched chunk still waiting
	// on koios.limiter.wait/http.Client.Do for one of the other, gated
	// chunks aborts immediately without ever completing its request, so not
	// every one of the initial batch is guaranteed to reach the server. It
	// must never be *more*: every request beyond the poison chunk's own
	// initial batch would have to have used a slot that only ever became
	// available after fetchCtx was already cancelled — see the doc comment
	// for why this holds for requests arriving both before and after the
	// gate opens.
	require.LessOrEqual(
		t,
		requestCount.Load(),
		int32(accountFetchConcurrency),
		"no chunk may ever be dispatched using a semaphore slot freed after "+
			"fetchCtx was already cancelled",
	)
}
