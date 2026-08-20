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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// decodeAccountRewardHistoryRequest decodes one /account_reward_history POST
// body the same way the production endpoint does. Runs inside the
// httptest.Server's own per-connection goroutine, never the test's own
// goroutine, so it uses assert (records a failure, keeps going) rather than
// require (which calls t.FailNow()/runtime.Goexit — only valid from the
// goroutine that started the test; from here it would just exit this one
// handler goroutine, potentially hiding the failure's real location). A
// decode failure here also naturally surfaces to the caller anyway: the
// handler goes on to build a response from a zero-value/partial body, which
// the outer test-goroutine assertions (on FetchAccountRewardsForEpoch's own
// return value) already catch.
func decodeAccountRewardHistoryRequest(
	t *testing.T,
	r *http.Request,
) (addrs []string, epoch uint64) {
	t.Helper()
	var body struct {
		StakeAddresses []string `json:"_stake_addresses"`
		EpochNo        uint64   `json:"_epoch_no"`
	}
	assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
	return body.StakeAddresses, body.EpochNo
}

func writeAccountRewardHistoryRows(w http.ResponseWriter, addrs []string) {
	items := make([]KoiosAccountRewardHistoryItem, len(addrs))
	for i, a := range addrs {
		items[i] = KoiosAccountRewardHistoryItem{
			StakeAddress: a,
			EarnedEpoch:  100,
			Amount:       "1000",
			Type:         "member",
		}
	}
	body, err := json.Marshal(items)
	if err != nil {
		panic(err)
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// TestFetchAccountRewardsForEpochResumesOnlyUndoneChunksAfterRestart proves
// dingo #3099's checkpointing: a chunk that already committed to
// koios_account_checked/koios_account_fetch_staged_rows on a prior,
// interrupted call is never re-requested on a resumed call — only the
// chunk(s) that never succeeded are retried. #3097's original
// implementation had no such checkpoint, so every chunk (including
// already-succeeded ones) was re-fetched from scratch on every retry.
func TestFetchAccountRewardsForEpochResumesOnlyUndoneChunksAfterRestart(
	t *testing.T,
) {
	const poisonAddr = "stake9POISON" // sorts after every "stake1addrNNN"

	var recovered atomic.Bool
	var requestCount atomic.Int32

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount.Add(1)
			addrs, epoch := decodeAccountRewardHistoryRequest(t, r)
			assert.Equal(
				t,
				uint64(100),
				epoch,
			) // handler goroutine — see decodeAccountRewardHistoryRequest's doc comment

			if slices.Contains(addrs, poisonAddr) && !recovered.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("unavailable"))
				return
			}
			writeAccountRewardHistoryRows(w, addrs)
		}),
	)
	defer srv.Close()

	k := &KoiosClient{
		baseURL: srv.URL,
		http:    &http.Client{Timeout: 2 * time.Second},
		limiter: newBurstLimiter(0, koiosBurstWindow),
	}
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	const numNormal = 249
	addrs := make([]string, numNormal+1)
	for i := range numNormal {
		addrs[i] = fmt.Sprintf("stake1addr%03d", i)
	}
	addrs[numNormal] = poisonAddr // 250 addresses total -> chunks of 100,100,50+poison

	_, err = FetchAccountRewardsForEpoch(
		t.Context(), k, cache, "preview", 100, addrs, time.Time{}, 0, nil,
	)
	require.Error(
		t,
		err,
		"the poisoned chunk's 503 must surface as a real, uncommitted failure",
	)
	require.False(t, errors.Is(err, ErrKoiosPermanent))

	_, covErr := cache.GetAccountCoverage("preview", 100)
	require.Error(t, covErr, "nothing may commit while any chunk is unresolved")

	afterFirstRun := requestCount.Load()
	// 2 successful chunks (1 request each) + the poisoned chunk retried
	// koiosMaxRetries times before post() gives up (5xx is retried
	// internally) = 5.
	require.EqualValues(
		t,
		5,
		afterFirstRun,
		"2 successful chunks + 3 retries on the poisoned chunk",
	)

	// "Restart": Koios recovers, caller retries with the identical universe.
	recovered.Store(true)
	fetched, err := FetchAccountRewardsForEpoch(
		t.Context(), k, cache, "preview", 100, addrs, time.Time{}, 0, nil,
	)
	require.NoError(t, err)

	afterSecondRun := requestCount.Load()
	require.EqualValues(
		t,
		afterFirstRun+1,
		afterSecondRun,
		"only the previously-failed chunk is re-requested; the other two chunks' "+
			"checkpointed progress must be reused, not re-fetched",
	)
	// Only the resumed chunk's addresses are freshly checkpointed by this
	// call — fetched is per-call, not cumulative (mirrors #3097's original
	// per-call semantics, preserved by this rewrite).
	require.Equal(t, len(addrs)-200, fetched)

	cov, err := cache.GetAccountCoverage("preview", 100)
	require.NoError(t, err)
	require.NotNil(t, cov)
	require.True(t, cov.Complete)
	require.Equal(t, len(addrs), cov.RequestedCount)
	require.Equal(t, len(addrs), cov.FetchedCount)
}

// TestFetchAccountRewardsForEpochInvalidatesOnlyChangedChunksOnUniverseChange
// proves content-addressed chunk hashing: when the requested universe
// changes between an interrupted attempt and a resumed one, only the chunk(s)
// whose exact address grouping changed are invalidated and refetched — an
// unaffected chunk's checkpointed progress survives.
func TestFetchAccountRewardsForEpochInvalidatesOnlyChangedChunksOnUniverseChange(
	t *testing.T,
) {
	const poisonAddr = "stake9POISON"

	var recovered atomic.Bool
	var requestCount atomic.Int32

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount.Add(1)
			addrs, _ := decodeAccountRewardHistoryRequest(t, r)
			if slices.Contains(addrs, poisonAddr) && !recovered.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("unavailable"))
				return
			}
			writeAccountRewardHistoryRows(w, addrs)
		}),
	)
	defer srv.Close()

	k := &KoiosClient{
		baseURL: srv.URL,
		http:    &http.Client{Timeout: 2 * time.Second},
		limiter: newBurstLimiter(0, koiosBurstWindow),
	}
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	buildAddrs := func(chunk1Marker string) []string {
		addrs := make([]string, 250)
		for i := range 100 {
			addrs[i] = fmt.Sprintf("stake1addr%03d", i) // chunk 0: [000,100)
		}
		for i := 100; i < 200; i++ {
			addrs[i] = fmt.Sprintf("stake1addr%03d", i) // chunk 1: [100,200)
		}
		addrs[150] = chunk1Marker // perturb chunk 1's content only
		for i := 200; i < 249; i++ {
			addrs[i] = fmt.Sprintf(
				"stake1addr%03d",
				i,
			) // chunk 2: [200,249)+poison
		}
		addrs[249] = poisonAddr
		return addrs
	}

	firstUniverse := buildAddrs("stake1addr150")
	// The replacement must sort into the exact same position (between
	// "stake1addr149" and "stake1addr151") as a prefix-extension of the
	// original — anything sorting differently (e.g. an unrelated string)
	// would shift every later chunk's boundary too, since sorting is global
	// across the whole universe, not scoped per labeled "chunk".
	_, err = FetchAccountRewardsForEpoch(
		t.Context(),
		k,
		cache,
		"preview",
		200,
		firstUniverse,
		time.Time{},
		0,
		nil,
	)
	require.Error(t, err, "the poisoned chunk 2 must abort the first attempt")

	afterFirstRun := requestCount.Load()
	// 2 successful chunks (1 request each) + the poisoned chunk retried
	// koiosMaxRetries times before post() gives up = 5.
	require.EqualValues(t, 5, afterFirstRun)

	// Resume with chunk 1's content changed (a different address swapped in
	// at the same position) and chunk 2's poison now recovered.
	recovered.Store(true)
	secondUniverse := buildAddrs("stake1addr150X")
	fetched, err := FetchAccountRewardsForEpoch(
		t.Context(),
		k,
		cache,
		"preview",
		200,
		secondUniverse,
		time.Time{},
		0,
		nil,
	)
	require.NoError(t, err)

	afterSecondRun := requestCount.Load()
	require.EqualValues(
		t,
		afterFirstRun+2,
		afterSecondRun,
		"chunk 0 is untouched by the universe change and must be skipped; "+
			"only changed chunk 1 and previously-failed chunk 2 are re-requested",
	)
	require.Equal(t, len(secondUniverse)-100, fetched)

	cov, err := cache.GetAccountCoverage("preview", 200)
	require.NoError(t, err)
	require.NotNil(t, cov)
	require.True(t, cov.Complete)
	require.Equal(t, len(secondUniverse), cov.RequestedCount)
}

// TestFetchAccountRewardsForEpochDoesNotTrustEmptyCheckpointWithinGraceWindow
// proves a real bug fix: a chunk that checkpointed with zero rows while
// still within the grace window must be re-dispatched on a later retry
// (still within the window), not skipped as "already done" — otherwise a
// retry could never notice that Koios has since published real data, and
// the epoch would eventually commit complete=true with a stale, empty
// result once graceHours elapses, silently losing rewards Koios published
// in the meantime.
func TestFetchAccountRewardsForEpochDoesNotTrustEmptyCheckpointWithinGraceWindow(
	t *testing.T,
) {
	var hasData atomic.Bool
	var requestCount atomic.Int32

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount.Add(1)
			addrs, _ := decodeAccountRewardHistoryRequest(t, r)
			if !hasData.Load() {
				// Koios hasn't published yet: a legitimate empty response, not
				// an error.
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[]`))
				return
			}
			writeAccountRewardHistoryRows(w, addrs)
		}),
	)
	defer srv.Close()

	k := &KoiosClient{
		baseURL: srv.URL,
		http:    &http.Client{Timeout: 2 * time.Second},
		limiter: newBurstLimiter(0, koiosBurstWindow),
	}
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	addrs := []string{"stake1a", "stake1b", "stake1c"}
	epochEndTime := time.Now()
	const graceHours = 24

	// First attempt: Koios hasn't published yet — the chunk succeeds with
	// zero rows, checkpoints, but the epoch correctly stays incomplete
	// because we're still within the grace window.
	_, err = FetchAccountRewardsForEpoch(
		t.Context(),
		k,
		cache,
		"preview",
		700,
		addrs,
		epochEndTime,
		graceHours,
		nil,
	)
	require.NoError(t, err)
	require.EqualValues(t, 1, requestCount.Load())

	cov, err := cache.GetAccountCoverage("preview", 700)
	require.NoError(t, err)
	require.NotNil(t, cov)
	require.False(
		t,
		cov.Complete,
		"a zero-row result within the grace window must leave the epoch incomplete",
	)

	// Koios has since published real data. Retry, still within the grace
	// window (epochEndTime unchanged, no real time has meaningfully passed).
	hasData.Store(true)
	_, err = FetchAccountRewardsForEpoch(
		t.Context(),
		k,
		cache,
		"preview",
		700,
		addrs,
		epochEndTime,
		graceHours,
		nil,
	)
	require.NoError(t, err)
	require.EqualValues(
		t,
		2,
		requestCount.Load(),
		"the previously-empty, still-within-grace chunk must be re-dispatched, "+
			"not trusted as already done",
	)

	cov, err = cache.GetAccountCoverage("preview", 700)
	require.NoError(t, err)
	require.NotNil(t, cov)
	require.True(t, cov.Complete)
	require.Equal(
		t,
		len(addrs),
		cov.FetchedCount,
		"the real data Koios published in the meantime must actually be captured",
	)
}

// TestFetchAccountRewardsForEpochRequiresEveryChunkCurrentBeforeComplete
// proves the fix for a mixed-chunk variant of the grace-window trust bug:
// one chunk has genuinely non-empty rows while another, in the very same
// call, is still empty-and-lagging. Naively checking only "did the aggregate
// staged-row total end up non-zero" would let the non-empty chunk alone
// satisfy that check and mark the whole epoch complete=true — permanently
// missing whatever the still-lagging chunk's real answer turns out to be
// later, since a completed epoch is never re-fetched. complete must instead
// require every planned chunk to have a currently-real result before
// treating grace-window completion as valid.
func TestFetchAccountRewardsForEpochRequiresEveryChunkCurrentBeforeComplete(
	t *testing.T,
) {
	const hasDataAddr = "stake1has-data"
	const noDataAddr = "stake1no-data"

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			addrs, _ := decodeAccountRewardHistoryRequest(t, r)
			if slices.Contains(addrs, noDataAddr) {
				// Koios hasn't published this address's reward yet — a
				// legitimate empty response, not an error, and never recovers
				// within this test.
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`[]`))
				return
			}
			writeAccountRewardHistoryRows(w, addrs)
		}),
	)
	defer srv.Close()

	k := &KoiosClient{
		baseURL: srv.URL,
		http:    &http.Client{Timeout: 2 * time.Second},
		limiter: newBurstLimiter(0, koiosBurstWindow),
	}
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	// FetchEpochAccountsWithAddrs looks up EpochEndTime from koios_epoch_info
	// (see its own doc comment) — commit one so the grace-hours gate is
	// actually active rather than disabled by an unknown end time.
	require.NoError(t, cache.CommitEpochData(KoiosEpochInfo{
		Network:      "preview",
		Epoch:        800,
		ActiveStake:  "1",
		EpochEndTime: time.Now().Add(-time.Hour), // just closed
		FetchedAt:    time.Now(),
	}, nil, nil))

	// chunkSize=1 forces each address into its own chunk, so one chunk gets
	// real rows and the other stays empty within the same call.
	_, err = FetchEpochAccountsWithAddrs(
		t.Context(), k, cache, "preview", 800, nil,
		[]string{hasDataAddr, noDataAddr}, 24, 1, 0, false, nil,
	)
	require.NoError(t, err)

	cov, err := cache.GetAccountCoverage("preview", 800)
	require.NoError(t, err)
	require.NotNil(t, cov)
	require.False(
		t,
		cov.Complete,
		"one chunk having real rows must not let the epoch look complete "+
			"while another chunk is still empty-and-lagging within the grace window",
	)

	// Partial rows are still recorded (matching #3097's original "record
	// progress, gate trust via the separate coverage flag" design) — what
	// matters is that cov.Complete above stays false, so a future check
	// never mistakes this for a fully verified reference set.
	rewards, err := cache.GetAccountRewardsForEpoch("preview", 800)
	require.NoError(t, err)
	require.Len(t, rewards, 1)
	require.Equal(t, hasDataAddr, rewards[0].StakeAddress)
}

// TestFetchAccountRewardsForEpochEmptyUniverseInvalidatesPriorCheckpointData
// proves a universe that shrinks to empty (e.g. Dingo's reward_account_output
// rows for this stake epoch were pruned/rolled back between attempts) does
// not leave stale checkpoint data behind. Without invalidating it, a later
// accountLifecycleMismatches call would read koios_account_checked as if
// those addresses were still part of this epoch's current, correctly-empty
// universe — reporting phantom zero-reward/lifecycle findings for addresses
// that no longer belong to this epoch at all.
func TestFetchAccountRewardsForEpochEmptyUniverseInvalidatesPriorCheckpointData(
	t *testing.T,
) {
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			addrs, _ := decodeAccountRewardHistoryRequest(t, r)
			writeAccountRewardHistoryRows(w, addrs)
		}),
	)
	defer srv.Close()

	k := &KoiosClient{
		baseURL: srv.URL,
		http:    &http.Client{Timeout: 2 * time.Second},
		limiter: newBurstLimiter(0, koiosBurstWindow),
	}
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	// First attempt: a real, non-empty universe fully succeeds and
	// checkpoints.
	_, err = FetchAccountRewardsForEpoch(
		t.Context(), k, cache, "preview", 850,
		[]string{"stake1old1", "stake1old2"}, time.Time{}, 0, nil,
	)
	require.NoError(t, err)
	done, err := cache.GetDoneAccountChunkHashes("preview", 850)
	require.NoError(t, err)
	require.NotEmpty(
		t,
		done,
		"test setup sanity check: the first attempt must have checkpointed",
	)

	// Second attempt: the universe has shrunk to empty.
	_, err = FetchAccountRewardsForEpoch(
		t.Context(), k, cache, "preview", 850, nil, time.Time{}, 0, nil,
	)
	require.NoError(t, err)

	cov, err := cache.GetAccountCoverage("preview", 850)
	require.NoError(t, err)
	require.NotNil(t, cov)
	require.True(t, cov.Complete)
	require.Equal(t, 0, cov.RequestedCount)

	universe, err := cache.GetAccountUniverseForEpoch("preview", 850)
	require.NoError(t, err)
	require.Empty(
		t,
		universe,
		"the old universe's checked markers must not survive once the universe is empty",
	)
	zeroReward, err := cache.GetZeroRewardAccountsForEpoch("preview", 850)
	require.NoError(t, err)
	require.Empty(t, zeroReward)
}

// TestFetchAccountRewardsForEpochForceRefreshBypassesRetainedCheckpoints
// proves --force-refresh (forceRefresh=true) actually goes back to Koios
// instead of silently reusing retained checkpoint state: rerunning with an
// unchanged address universe and forceRefresh=false must not re-request any
// already-done chunk (the existing resume behavior), but the same rerun with
// forceRefresh=true must re-request every chunk, since an operator running
// --force-refresh is explicitly trying to repair suspected stale/corrupt
// cached data.
func TestFetchAccountRewardsForEpochForceRefreshBypassesRetainedCheckpoints(
	t *testing.T,
) {
	var requestCount atomic.Int32

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount.Add(1)
			addrs, _ := decodeAccountRewardHistoryRequest(t, r)
			writeAccountRewardHistoryRows(w, addrs)
		}),
	)
	defer srv.Close()

	k := &KoiosClient{
		baseURL: srv.URL,
		http:    &http.Client{Timeout: 2 * time.Second},
		limiter: newBurstLimiter(0, koiosBurstWindow),
	}
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	addrs := []string{"stake1addr000", "stake1addr001", "stake1addr002"}

	_, err = fetchAccountRewardsForEpoch(
		t.Context(), k, cache, "preview", 950, addrs, time.Time{}, 0,
		1, 0, false, nil, // chunkSize 1 -> 3 chunks, 3 requests
	)
	require.NoError(t, err)
	afterFirstRun := requestCount.Load()
	require.EqualValues(t, 3, afterFirstRun)

	// Rerun, unchanged universe, forceRefresh=false: every chunk is already
	// checkpointed "done", so no new requests are made.
	_, err = fetchAccountRewardsForEpoch(
		t.Context(), k, cache, "preview", 950, addrs, time.Time{}, 0,
		1, 0, false, nil,
	)
	require.NoError(t, err)
	require.EqualValues(
		t,
		afterFirstRun,
		requestCount.Load(),
		"an unchanged universe without forceRefresh must reuse every checkpointed chunk",
	)

	// Rerun again, unchanged universe, forceRefresh=true: every chunk must be
	// re-requested despite matching the retained checkpoints exactly.
	_, err = fetchAccountRewardsForEpoch(
		t.Context(), k, cache, "preview", 950, addrs, time.Time{}, 0,
		1, 0, true, nil,
	)
	require.NoError(t, err)
	require.EqualValues(
		t,
		afterFirstRun+3,
		requestCount.Load(),
		"forceRefresh=true must re-request every chunk even when the universe is unchanged",
	)

	cov, err := cache.GetAccountCoverage("preview", 950)
	require.NoError(t, err)
	require.NotNil(t, cov)
	require.True(t, cov.Complete)
	require.Equal(t, len(addrs), cov.RequestedCount)
}

// TestFetchAccountRewardsForEpochFailedForceRefreshPreservesUntouchedCheckpoints
// proves the fix for a real data-loss hazard: an earlier version of
// forceRefresh invalidated (deleted) every existing chunk's checkpoint data
// up front, before dispatch, so an interrupted/partially-failing
// --force-refresh attempt could leave koios_account_checked/
// koios_account_fetch_staged_rows wiped or partial. forceRefresh must instead
// leave a chunk's existing checkpoint alone until that specific chunk's own
// re-fetch actually succeeds — proved below by failAddr's untouched
// checkpoint surviving. Separately, since a partial force-refresh failure
// still leaves koios_account_checked holding a mix of freshly-refreshed and
// untouched pre-refresh chunks (two snapshots that were never valid
// together), the pre-existing complete=true coverage row from before this
// attempt must be downgraded to complete=false (Cache.MarkAccountCoverageIncomplete)
// rather than left as-is — otherwise compareEpochAccounts/
// accountLifecycleMismatches would trust that mixed state as a genuinely
// complete epoch.
func TestFetchAccountRewardsForEpochFailedForceRefreshPreservesUntouchedCheckpoints(
	t *testing.T,
) {
	const goodAddr = "stake1addrGOOD"
	const failAddr = "stake1addrFAIL"

	var refreshing atomic.Bool

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			addrs, _ := decodeAccountRewardHistoryRequest(t, r)
			if slices.Contains(addrs, failAddr) && refreshing.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("unavailable"))
				return
			}
			writeAccountRewardHistoryRows(w, addrs)
		}),
	)
	defer srv.Close()

	k := &KoiosClient{
		baseURL: srv.URL,
		http:    &http.Client{Timeout: 100 * time.Millisecond},
		limiter: newBurstLimiter(0, koiosBurstWindow),
	}
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	addrs := []string{goodAddr, failAddr}

	// First, normal run: both chunks succeed and commit a complete epoch.
	_, err = fetchAccountRewardsForEpoch(
		t.Context(), k, cache, "preview", 960, addrs, time.Time{}, 0,
		1, 0, false, nil,
	)
	require.NoError(t, err)
	covBefore, err := cache.GetAccountCoverage("preview", 960)
	require.NoError(t, err)
	require.NotNil(t, covBefore)
	require.True(t, covBefore.Complete)
	doneBefore, err := cache.GetDoneAccountChunkHashes("preview", 960)
	require.NoError(t, err)
	require.Len(t, doneBefore, 2, "test setup sanity check")

	// Force-refresh: goodAddr's chunk re-fetches successfully, failAddr's
	// chunk fails outright. (goodAddr genuinely being re-requested is already
	// covered by TestFetchAccountRewardsForEpochForceRefreshBypassesRetainedCheckpoints's
	// request-count assertions — not repeated here via a response-value
	// check, since both chunks dispatch concurrently and there is no
	// ordering guarantee between goodAddr's checkpoint save and failAddr's
	// cancellation.)
	refreshing.Store(true)
	_, err = fetchAccountRewardsForEpoch(
		t.Context(), k, cache, "preview", 960, addrs, time.Time{}, 0,
		1, 0, true, nil,
	)
	require.Error(t, err, "failAddr's chunk must surface as a real failure")

	// The epoch's coverage row must be downgraded to complete=false — the
	// mixed post-refresh checkpoint state (goodAddr refreshed, failAddr
	// untouched) is never a valid snapshot to trust, even though
	// CommitAccountRewardsForEpoch itself was never reached on this partial
	// failure. RequestedCount is otherwise unchanged (MarkAccountCoverageIncomplete
	// only ever flips the complete flag).
	covAfter, err := cache.GetAccountCoverage("preview", 960)
	require.NoError(t, err)
	require.NotNil(t, covAfter)
	require.False(
		t,
		covAfter.Complete,
		"a partial force-refresh failure must downgrade coverage to incomplete",
	)
	require.Equal(t, covBefore.RequestedCount, covAfter.RequestedCount)

	// failAddr's chunk never re-succeeded, so its prior, still-valid
	// checkpoint must survive untouched rather than being deleted up front.
	doneAfter, err := cache.GetDoneAccountChunkHashes("preview", 960)
	require.NoError(t, err)
	require.Len(
		t,
		doneAfter,
		2,
		"failAddr's untouched-by-the-refresh chunk checkpoint must still be present",
	)
}

// TestFetchAccountRewardsForEpochIncompleteCoverageAfterForceRefreshSelfHeals
// proves the other half of Cache.MarkAccountCoverageIncomplete's contract:
// once a partial --force-refresh failure downgrades an epoch's coverage to
// complete=false, a subsequent plain (non-force-refresh) call restores
// complete=true on its own — because failAddr's chunk from before the
// refresh was left untouched (never deleted), it's still checkpointed with
// real rows, so the normal doneHashes/hashesWithRows resume logic already
// trusts it without needing to re-contact Koios for it at all. This is
// exactly why forceRefresh never pre-invalidates existing checkpoint data:
// doing so would have made this self-heal impossible without another
// explicit --force-refresh, since failAddr's old, still-valid data would
// have been destroyed rather than preserved.
func TestFetchAccountRewardsForEpochIncompleteCoverageAfterForceRefreshSelfHeals(
	t *testing.T,
) {
	const goodAddr = "stake1addrGOOD2"
	const failAddr = "stake1addrFAIL2"

	var failAddrRequests atomic.Int32
	var failNextFailAddrRequest atomic.Bool

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			addrs, _ := decodeAccountRewardHistoryRequest(t, r)
			if slices.Contains(addrs, failAddr) {
				failAddrRequests.Add(1)
				if failNextFailAddrRequest.Load() {
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = w.Write([]byte("unavailable"))
					return
				}
			}
			writeAccountRewardHistoryRows(w, addrs)
		}),
	)
	defer srv.Close()

	k := &KoiosClient{
		baseURL: srv.URL,
		http:    &http.Client{Timeout: 100 * time.Millisecond},
		limiter: newBurstLimiter(0, koiosBurstWindow),
	}
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	addrs := []string{goodAddr, failAddr}

	// First, normal run: both chunks succeed and commit a complete epoch.
	_, err = fetchAccountRewardsForEpoch(
		t.Context(), k, cache, "preview", 970, addrs, time.Time{}, 0,
		1, 0, false, nil,
	)
	require.NoError(t, err)
	require.EqualValues(t, 1, failAddrRequests.Load())

	// Force-refresh: failAddr's chunk fails (post()'s own internal retries on
	// a 5xx mean this bumps the counter by more than one), downgrading
	// coverage.
	failNextFailAddrRequest.Store(true)
	_, err = fetchAccountRewardsForEpoch(
		t.Context(), k, cache, "preview", 970, addrs, time.Time{}, 0,
		1, 0, true, nil,
	)
	require.Error(t, err)
	afterForceRefresh := failAddrRequests.Load()
	require.Greater(t, afterForceRefresh, int32(1))
	cov, err := cache.GetAccountCoverage("preview", 970)
	require.NoError(t, err)
	require.False(t, cov.Complete, "test setup sanity check")

	// A subsequent plain (non-force-refresh) call must self-heal — restoring
	// complete=true — without ever re-requesting failAddr's chunk, since its
	// pre-refresh checkpoint was preserved, not deleted.
	_, err = fetchAccountRewardsForEpoch(
		t.Context(), k, cache, "preview", 970, addrs, time.Time{}, 0,
		1, 0, false, nil,
	)
	require.NoError(t, err)
	require.EqualValues(
		t,
		afterForceRefresh,
		failAddrRequests.Load(),
		"the self-heal must trust failAddr's preserved checkpoint, not re-request it",
	)

	covHealed, err := cache.GetAccountCoverage("preview", 970)
	require.NoError(t, err)
	require.True(
		t,
		covHealed.Complete,
		"a subsequent normal fetch must self-heal the incomplete coverage "+
			"left behind by a failed force-refresh",
	)
}

// TestFetchAccountRewardsForEpochMegaScenario is the combined exercise the
// issue asks for directly: large synthetic snapshot plus injected timeout,
// rate-limit, truncated-response, duplicate-page, and restart failures, all
// layered onto one run/resume cycle against dingo #3099's checkpointed
// rewrite of #3097's fetchAccountRewardsForEpoch — rather than each failure
// mode tested only in isolation.
//
// Roles are assigned by chunk content (each chunk's first address), not
// server-arrival order: fetchAccountRewardsForEpoch cancels every in-flight
// chunk the instant any one errors, so a fast-failing chunk racing among the
// very first dispatched batch could otherwise starve slower, otherwise-
// healthy chunks of a chance to complete at all. Placing all five injected
// failures in the last chunks (indices numAccounts/chunkSize-5 and up) lets
// every earlier, ordinary chunk complete and checkpoint deterministically
// before dispatch ever reaches the failing ones.
func TestFetchAccountRewardsForEpochMegaScenario(t *testing.T) {
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	const numAccounts = 240 // chunk size 10 -> 24 chunks
	const chunkSize = 10
	addrs := make([]string, numAccounts)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("stake1addr%04d", i)
	}

	const (
		roleTimeout = iota
		roleRateLimit
		roleTruncated
		roleDuplicate
		roleOutage
		roleNormal
	)

	// The last 5 chunks (indices 19-23) get the special roles, by content —
	// deterministic regardless of dispatch/arrival timing.
	roleForFirstAddr := make(map[string]int, 5)
	specialRoles := []int{
		roleTimeout,
		roleRateLimit,
		roleTruncated,
		roleDuplicate,
		roleOutage,
	}
	firstSpecialChunk := numAccounts/chunkSize - len(specialRoles)
	for i, role := range specialRoles {
		chunkIdx := firstSpecialChunk + i
		roleForFirstAddr[addrs[chunkIdx*chunkSize]] = role
	}

	var mu sync.Mutex
	attempts := map[string]int{}
	var phaseRecovered atomic.Bool

	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			addrs, _ := decodeAccountRewardHistoryRequest(t, r)
			key := addrs[0]

			role, special := roleForFirstAddr[key]
			if !special {
				role = roleNormal
			}
			mu.Lock()
			attempts[key]++
			attempt := attempts[key]
			mu.Unlock()

			recovered := phaseRecovered.Load()

			switch role {
			case roleTimeout:
				if !recovered {
					time.Sleep(300 * time.Millisecond)
				}
			case roleRateLimit:
				if attempt == 1 {
					w.Header().Set("Retry-After", "1")
					w.WriteHeader(http.StatusTooManyRequests)
					_, _ = w.Write([]byte("rate limited"))
					return
				}
			case roleTruncated:
				if !recovered {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(
						[]byte(
							`[{"stake_address":"` + key + `","earned_epoch":`,
						),
					)
					return
				}
			case roleDuplicate:
				// One genuine, isolated duplicate for the chunk's first address
				// only, with the SAME amount as the primary row (a true
				// duplicate, not a conflicting value) — never an error; must be
				// preserved, not retried or rejected.
				items := make([]KoiosAccountRewardHistoryItem, 0, len(addrs)+1)
				for i, a := range addrs {
					items = append(items, KoiosAccountRewardHistoryItem{
						StakeAddress: a, EarnedEpoch: 100, Amount: "1000", Type: "member",
					})
					if i == 0 {
						items = append(items, KoiosAccountRewardHistoryItem{
							StakeAddress: a, EarnedEpoch: 100, Amount: "1000", Type: "member",
						})
					}
				}
				body, marshalErr := json.Marshal(items)
				assert.NoError(
					t,
					marshalErr,
				) // handler goroutine — see decodeAccountRewardHistoryRequest's doc comment
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(body)
				return
			case roleOutage:
				if !recovered {
					w.WriteHeader(http.StatusServiceUnavailable)
					_, _ = w.Write([]byte("unavailable"))
					return
				}
			}
			writeAccountRewardHistoryRows(w, addrs)
		}),
	)
	defer srv.Close()

	k := &KoiosClient{
		baseURL: srv.URL,
		http:    &http.Client{Timeout: 100 * time.Millisecond},
		limiter: newBurstLimiter(0, koiosBurstWindow),
	}

	// First run: timeout/truncated/outage chunks fail; rate-limit recovers
	// via retry; duplicate-page and every normal chunk succeed and
	// checkpoint.
	_, err = fetchAccountRewardsForEpoch(
		t.Context(),
		k,
		cache,
		"preview",
		300,
		addrs,
		time.Time{},
		0,
		10,
		0,
		false,
		nil,
	)
	require.Error(
		t,
		err,
		"timeout/truncated/outage chunks must surface as a real error",
	)

	_, covErr := cache.GetAccountCoverage("preview", 300)
	require.Error(
		t,
		covErr,
		"must never be falsely complete after a partial failure",
	)

	doneAfterRun1, err := cache.GetDoneAccountChunkHashes("preview", 300)
	require.NoError(t, err)
	// The 19 ordinary chunks (indices 0-18) dispatch and complete well before
	// dispatch ever reaches the 5 special-role chunks (indices 19-23,
	// gated behind accountFetchConcurrency concurrent slots), so all 19 must
	// have checkpointed by the time the fast-failing truncated-response
	// chunk cancels the rest. Asserted as a lower bound, not an exact count,
	// since exactly how many of the 5 special chunks also race to
	// completion before cancellation propagates is not guaranteed.
	require.GreaterOrEqual(
		t,
		len(doneAfterRun1),
		19,
		"every ordinary chunk dispatched before the failing ones must have checkpointed",
	)
	require.Less(
		t,
		len(doneAfterRun1),
		24,
		"at least one chunk must still be outstanding after a real failure",
	)

	// "Restart": Koios recovers, rerun resumes.
	phaseRecovered.Store(true)
	checked, err := fetchAccountRewardsForEpoch(
		t.Context(),
		k,
		cache,
		"preview",
		300,
		addrs,
		time.Time{},
		0,
		10,
		0,
		false,
		nil,
	)
	require.NoError(t, err)
	require.Greater(
		t,
		checked,
		0,
		"the resume call must freshly fetch at least the previously-failed chunks",
	)

	cov, err := cache.GetAccountCoverage("preview", 300)
	require.NoError(t, err)
	require.NotNil(t, cov)
	require.True(t, cov.Complete)
	require.Equal(t, numAccounts, cov.RequestedCount)

	rewards, err := cache.GetAccountRewardsForEpoch("preview", 300)
	require.NoError(t, err)

	counts := make(map[string]int, len(rewards))
	for _, r := range rewards {
		counts[r.StakeAddress+"|"+r.RewardType]++
	}
	var duplicateKeys int
	for _, c := range counts {
		if c > 1 {
			duplicateKeys++
			require.Equal(
				t,
				2,
				c,
				"the injected duplicate must survive as exactly two rows",
			)
		}
	}
	require.Equal(
		t,
		1,
		duplicateKeys,
		"exactly one (address, reward_type) key must show the injected duplicate",
	)
}

// TestFetchAccountRewardsForEpochRequestBodyNeverExceedsConfiguredMaxBytes
// proves chunkAddressesByCountAndSize's byte budget accounts for the fixed
// {"_stake_addresses":[...],"_epoch_no":N} JSON envelope, not just the
// address array itself — without reserving that overhead, the real request
// body could exceed a small configured --account-chunk-max-bytes by a fixed
// amount on every single chunk.
func TestFetchAccountRewardsForEpochRequestBodyNeverExceedsConfiguredMaxBytes(
	t *testing.T,
) {
	const chunkMaxBytes = 100 // deliberately tiny, to make the envelope's
	// fixed overhead a large fraction of the budget rather than negligible.

	// Chunks dispatch concurrently (accountFetchConcurrency workers), so the
	// handler runs on multiple goroutines at once — guard the shared max
	// with a mutex rather than a plain int.
	var mu sync.Mutex
	var maxBodyLen int
	// Both checks below run inside the httptest.Server's own per-connection
	// goroutine (see decodeAccountRewardHistoryRequest's doc comment for why
	// that means assert, not require: FailNow/Goexit is only valid from the
	// goroutine that started the test). A failure here still surfaces via
	// the outer FetchEpochAccountsWithAddrs assertion below, since a broken
	// decode leaves decoded.StakeAddresses empty/zero-valued rather than
	// panicking.
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			mu.Lock()
			if len(body) > maxBodyLen {
				maxBodyLen = len(body)
			}
			mu.Unlock()
			var decoded struct {
				StakeAddresses []string `json:"_stake_addresses"`
			}
			assert.NoError(t, json.Unmarshal(body, &decoded))
			writeAccountRewardHistoryRows(w, decoded.StakeAddresses)
		}),
	)
	defer srv.Close()

	k := &KoiosClient{
		baseURL: srv.URL,
		http:    &http.Client{Timeout: 2 * time.Second},
		limiter: newBurstLimiter(0, koiosBurstWindow),
	}
	cache, err := OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
	require.NoError(t, err)
	defer cache.Close() //nolint:errcheck

	addrs := make([]string, 50)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("stake1addr%03d", i)
	}

	_, err = FetchEpochAccountsWithAddrs(
		t.Context(), k, cache, "preview", 900, nil, addrs, 0,
		1_000_000, chunkMaxBytes, false, nil,
	)
	require.NoError(t, err)
	mu.Lock()
	got := maxBodyLen
	mu.Unlock()
	require.Greater(t, got, 0, "at least one request must have been sent")
	require.LessOrEqual(
		t,
		got,
		chunkMaxBytes,
		"the real encoded request body, envelope included, must never exceed --account-chunk-max-bytes",
	)
}

// TestGetAccountRewardHistoryRejectsSuspiciouslyFullResponse proves the
// page-safety guard: /account_reward_history is not Range-paginated in any
// working sense (verified live against preview — repeated requests with
// different Range values return the same first-window rows rather than
// paging further), so a response landing at the koiosPageSize row ceiling
// must hard-error rather than be accepted as a complete, trustworthy answer.
func TestGetAccountRewardHistoryRejectsSuspiciouslyFullResponse(t *testing.T) {
	srv := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			addrs, _ := decodeAccountRewardHistoryRequest(t, r)
			n := koiosPageSize
			if len(addrs) > 0 && addrs[0] == "few" {
				n = koiosPageSize - 1
			}
			items := make([]KoiosAccountRewardHistoryItem, n)
			for i := range items {
				items[i] = KoiosAccountRewardHistoryItem{
					StakeAddress: fmt.Sprintf(
						"stake1x%d",
						i,
					), EarnedEpoch: 100, Amount: "1", Type: "member",
				}
			}
			body, err := json.Marshal(items)
			assert.NoError(
				t,
				err,
			) // handler goroutine — see decodeAccountRewardHistoryRequest's doc comment
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)
		}),
	)
	defer srv.Close()

	k := &KoiosClient{
		baseURL: srv.URL,
		http:    &http.Client{Timeout: 2 * time.Second},
		limiter: newBurstLimiter(0, koiosBurstWindow),
	}

	_, err := k.GetAccountRewardHistory(t.Context(), []string{"many"}, 100)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrKoiosPermanent))

	items, err := k.GetAccountRewardHistory(t.Context(), []string{"few"}, 100)
	require.NoError(t, err)
	require.Len(t, items, koiosPageSize-1)
}
