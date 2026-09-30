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

package nodeparity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/allegra"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openTestCache opens a real, temp-file-backed koiosparity.Cache -- the same
// convention internal/koiosparity's own tests use (e.g.
// account_universe_cache_test.go) -- rather than a fake, so this proves
// behavior against the real Cache/OpenCache implementation dingo's embedded
// koios-parity observer also uses, including its WAL/busy_timeout setup.
func openTestCache(t *testing.T) *koiosparity.Cache {
	t.Helper()
	cache, err := koiosparity.OpenCache(
		filepath.Join(t.TempDir(), "cache.db"),
		nil,
		koiosparity.WithRelaxedDurability(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cache.Close() })
	return cache
}

// countingKoiosServer wraps handler with a request counter, so a test can
// assert exactly how many live Koios calls a cache hit/miss actually made --
// stronger than asserting on comparison output alone, which a cache bug
// could still coincidentally leave looking correct.
func countingKoiosServer(
	t *testing.T, handler http.HandlerFunc,
) (url string, count *atomic.Int32) {
	t.Helper()
	count = &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			count.Add(1)
			handler(w, r)
		},
	))
	t.Cleanup(srv.Close)
	return srv.URL, count
}

// TestCheckProtocolParams_CacheHitSkipsLiveKoiosCall proves a
// koios_epoch_params row already cached for (network, epoch) is used
// outright: CheckProtocolParams must never call Koios's /epoch_params at all
// once the cache already has an answer for this epoch, since a closed
// epoch's protocol parameters are immutable Koios-side (matching
// UpsertEpochParams's own doc comment in internal/koiosparity/cache.go).
//
// Reverting CheckProtocolParams's cache-hit branch (checking cache before
// ever calling koios.GetEpochParams) in place would make this test's Koios
// fake receive a request and fail the reqCount assertion below.
func TestCheckProtocolParams_CacheHitSkipsLiveKoiosCall(t *testing.T) {
	const magic = 764824073
	const epoch = uint64(107)

	lsq := newWiringFakeLSQServer()
	lsq.setEra(int(shelley.EraIdShelley))
	lsq.setProtocolParams(newWiringShelleyProtocolParams())

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	lsq.serve(t, listener, magic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := dialWiringClient(t, ctx, listener.Addr().String(), magic)

	koiosURL, reqCount := countingKoiosServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	koios, err := NewKoiosClient("preview", "", koiosURL, true, true)
	require.NoError(t, err)

	cache := openTestCache(t)
	require.NoError(t, cache.UpsertEpochParams(koiosparity.KoiosEpochParams{
		Network:   "preview",
		Epoch:     epoch,
		Era:       "Shelley",
		FetchedAt: time.Now().UTC(),
	}))

	_, err = CheckProtocolParams(ctx, client, koios, cache, "preview", epoch)
	require.NoError(t, err)
	require.Equal(t, int32(0), reqCount.Load(),
		"a koios_epoch_params cache hit must never call live Koios")
}

// TestCheckProtocolParams_CacheMissFetchesOnceAndPersists proves the other
// half of the cache contract: a miss still calls Koios exactly as before
// (one live request), and the freshly-fetched row is written back so a
// second call for the same (network, epoch) becomes a cache hit -- no second
// live request.
//
// Reverting the write-back call (UpsertEpochParams after a live fetch) in
// place leaves cache.GetEpochParams empty after the first call, so the
// second CheckProtocolParams call also misses and reqCount reaches 2,
// failing this test's final assertion.
func TestCheckProtocolParams_CacheMissFetchesOnceAndPersists(t *testing.T) {
	const magic = 764824073
	const epoch = uint64(108)

	lsq := newWiringFakeLSQServer()
	lsq.setEra(int(shelley.EraIdShelley))
	lsq.setProtocolParams(newWiringShelleyProtocolParams())

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	lsq.serve(t, listener, magic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	koiosURL, reqCount := countingKoiosServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/epoch_params" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"epoch_no":108,"era":"Shelley"}]`))
	})
	koios, err := NewKoiosClient("preview", "", koiosURL, true, true)
	require.NoError(t, err)

	cache := openTestCache(t)

	client1 := dialWiringClient(t, ctx, listener.Addr().String(), magic)
	_, err = CheckProtocolParams(ctx, client1, koios, cache, "preview", epoch)
	require.NoError(t, err)
	require.Equal(t, int32(1), reqCount.Load(),
		"a cache miss must fetch live Koios exactly once")

	cached, err := cache.GetEpochParams("preview", epoch)
	require.NoError(t, err, "the live fetch must be written back to the cache")
	require.Equal(t, "Shelley", cached.Era)

	client2 := dialWiringClient(t, ctx, listener.Addr().String(), magic)
	_, err = CheckProtocolParams(ctx, client2, koios, cache, "preview", epoch)
	require.NoError(t, err)
	require.Equal(t, int32(1), reqCount.Load(),
		"the second call for the same epoch must be served from cache, not a second live request")
}

// TestCheckStakeDistribution_CacheHitSkipsLiveKoiosCall proves a
// koios_pool_epoch row already cached for (network, epoch, pool) is used
// outright: CheckStakeDistribution must never call Koios's /pool_history for
// that pool once the cache already has an answer.
//
// Reverting the cache-hit branch in place would make the koios fake below
// receive a request and fail the reqCount assertion.
func TestCheckStakeDistribution_CacheHitSkipsLiveKoiosCall(t *testing.T) {
	const magic = 764824073
	const epoch = uint64(500)
	const dingoStake = uint64(1_000_000)

	var poolID ledger.PoolId
	poolID[0] = 0xAB
	bech32 := poolID.String()

	lsq := newWiringFakeLSQServer()
	lsq.setPoolDistr(&localstatequery.PoolDistr2Result{
		Pools: map[ledger.PoolId]localstatequery.PoolDistr2IndividualStake{
			poolID: {
				StakeFraction:  &cbor.Rat{Rat: big.NewRat(1, 1)},
				TotalPoolStake: dingoStake,
			},
		},
		TotalActiveStake: dingoStake,
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	lsq.serve(t, listener, magic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := dialWiringClient(t, ctx, listener.Addr().String(), magic)

	koiosURL, reqCount := countingKoiosServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	koios, err := NewKoiosClient("preview", "", koiosURL, true, true)
	require.NoError(t, err)

	cache := openTestCache(t)
	// compareTotalActiveStake (dingo#4321) also reads epoch_info, so seed it
	// too: this test's contract is that a cached POOL row causes no
	// pool_history call, not that the stake check has only one cache
	// dependency. Left unseeded it would fetch epoch_info live and the
	// request count below would no longer be measuring what it claims.
	require.NoError(t, cache.UpsertEpochInfo(koiosparity.KoiosEpochInfo{
		Network:     "preview",
		Epoch:       epoch,
		ActiveStake: "1000000",
		FetchedAt:   time.Now().UTC(),
	}))
	require.NoError(t, cache.UpsertPoolEpoch(koiosparity.KoiosPoolEpoch{
		Network:     "preview",
		Epoch:       epoch,
		PoolBech32:  bech32,
		ActiveStake: "1000000",
		FetchedAt:   time.Now().UTC(),
	}))

	mismatches, err := CheckStakeDistribution(ctx, client, koios, cache, "preview", epoch)
	require.NoError(t, err)
	require.Empty(t, mismatches,
		"dingo and the cached koios row agree, so no mismatch should be reported")
	require.Equal(t, int32(0), reqCount.Load(),
		"a koios_pool_epoch cache hit must never call live Koios")
}

// TestCheckStakeDistribution_CacheMissFetchesOnceAndPersists mirrors
// TestCheckProtocolParams_CacheMissFetchesOnceAndPersists for the stake side:
// a miss fetches live once and persists the result, so a second call for the
// same (network, epoch, pool) is served from cache.
func TestCheckStakeDistribution_CacheMissFetchesOnceAndPersists(t *testing.T) {
	const magic = 764824073
	const epoch = uint64(501)
	const dingoStake = uint64(2_000_000)

	var poolID ledger.PoolId
	poolID[0] = 0xCD
	bech32 := poolID.String()

	lsq := newWiringFakeLSQServer()
	lsq.setPoolDistr(&localstatequery.PoolDistr2Result{
		Pools: map[ledger.PoolId]localstatequery.PoolDistr2IndividualStake{
			poolID: {
				StakeFraction:  &cbor.Rat{Rat: big.NewRat(1, 1)},
				TotalPoolStake: dingoStake,
			},
		},
		TotalActiveStake: dingoStake,
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	lsq.serve(t, listener, magic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	koiosURL, reqCount := countingKoiosServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/pool_history" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`[{"epoch_no":501,"active_stake":"2000000"}]`))
	})
	koios, err := NewKoiosClient("preview", "", koiosURL, true, true)
	require.NoError(t, err)

	cache := openTestCache(t)
	// Seed epoch_info: compareTotalActiveStake (dingo#4321) reads it too,
	// and this test measures pool_history caching specifically. Without it
	// the fake server 404s the epoch_info fetch, which correctly is not
	// cached and so retries, and the request count stops measuring what
	// this test claims.
	require.NoError(t, cache.UpsertEpochInfo(koiosparity.KoiosEpochInfo{
		Network:     "preview",
		Epoch:       epoch,
		ActiveStake: "2000000",
		FetchedAt:   time.Now().UTC(),
	}))

	client1 := dialWiringClient(t, ctx, listener.Addr().String(), magic)
	mismatches, err := CheckStakeDistribution(ctx, client1, koios, cache, "preview", epoch)
	require.NoError(t, err)
	require.Empty(t, mismatches)
	require.Equal(t, int32(1), reqCount.Load(),
		"a cache miss must fetch live Koios exactly once")

	cachedRows, err := cache.GetAllPoolsForEpoch("preview", epoch)
	require.NoError(t, err)
	require.Len(t, cachedRows, 1, "the live fetch must be written back to the cache")
	require.Equal(t, bech32, cachedRows[0].PoolBech32)
	require.Equal(t, "2000000", cachedRows[0].ActiveStake)

	client2 := dialWiringClient(t, ctx, listener.Addr().String(), magic)
	_, err = CheckStakeDistribution(ctx, client2, koios, cache, "preview", epoch)
	require.NoError(t, err)
	require.Equal(t, int32(1), reqCount.Load(),
		"the second call for the same epoch must be served from cache, not a second live request")
}

// txInfoRequestHashes decodes the "_tx_hashes" array out of a /tx_info POST
// body, so a test can assert on exactly which hashes reached live Koios --
// the whole point of batch-aware caching is that a partially-cached chunk
// asks for the misses ONLY, which a plain request count cannot distinguish
// from re-fetching the whole chunk.
func txInfoRequestHashes(t *testing.T, r *http.Request) []string {
	t.Helper()
	var payload struct {
		TxHashes []string `json:"_tx_hashes"`
	}
	require.NoError(t, json.NewDecoder(r.Body).Decode(&payload))
	return payload.TxHashes
}

// txInfoHandler answers a /tx_info POST with one synthetic item per
// requested hash (an output paying "addr_<hash>" for 1000000 lovelace), and
// records every request's hash list into requested.
func txInfoHandler(
	t *testing.T, requested *[][]string, mu *sync.Mutex,
) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tx_info" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		hashes := txInfoRequestHashes(t, r)
		mu.Lock()
		*requested = append(*requested, hashes)
		mu.Unlock()
		items := make([]koiosparity.KoiosTxInfoItem, 0, len(hashes))
		for _, h := range hashes {
			items = append(items, syntheticTxInfo(h))
		}
		body, err := json.Marshal(items)
		require.NoError(t, err)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}
}

// syntheticTxInfo builds the one canonical fake /tx_info item every test
// below uses for a hash, so a cached row and a live answer for the same hash
// are byte-identical and a test asserting on content cannot accidentally
// pass just because the two sources happened to differ visibly.
func syntheticTxInfo(hash string) koiosparity.KoiosTxInfoItem {
	out := koiosparity.KoiosTxInfoOutput{
		TxHash:  hash,
		TxIndex: 0,
		Value:   "1000000",
		AssetList: []koiosparity.KoiosTxInfoAsset{
			{PolicyID: "aa", AssetName: "bb", Quantity: "7"},
		},
	}
	out.PaymentAddr.Bech32 = "addr_" + hash
	return koiosparity.KoiosTxInfoItem{
		TxHash: hash,
		Inputs: []koiosparity.KoiosTxInfoUtxoRef{
			{TxHash: "spent_" + hash, TxIndex: 1},
		},
		Outputs: []koiosparity.KoiosTxInfoOutput{out},
	}
}

// TestFetchTxInfosCached_FullCacheHitSkipsLiveKoiosCall proves a chunk whose
// every transaction is already in koios_tx_info costs no live Koios request
// at all -- the UTxO half of the from-genesis walk reaching the same
// cache-first behavior CheckProtocolParams/CheckStakeDistribution already
// have (blinklabs-io/dingo#1900).
//
// Reverting fetchTxInfosCached's cache lookup in place (calling
// koios.GetTxInfos on the full chunk) makes the fake below receive a request
// and fails the reqCount assertion.
func TestFetchTxInfosCached_FullCacheHitSkipsLiveKoiosCall(t *testing.T) {
	ctx := context.Background()
	hashes := []string{"aaa", "bbb"}

	koiosURL, reqCount := countingKoiosServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	koios, err := NewKoiosClient("preview", "", koiosURL, true, true)
	require.NoError(t, err)

	cache := openTestCache(t)
	want := []koiosparity.KoiosTxInfoItem{
		syntheticTxInfo("aaa"), syntheticTxInfo("bbb"),
	}
	require.NoError(t, cache.UpsertTxInfos("preview", want, time.Now().UTC()))

	got, err := fetchTxInfosCached(ctx, koios, cache, "preview", hashes)
	require.NoError(t, err)
	require.Equal(t, int32(0), reqCount.Load(),
		"a fully cached tx_info chunk must never call live Koios")
	require.Equal(t, want, got,
		"cached items must round-trip identically to what was stored")
}

// TestFetchTxInfosCached_PartialHitFetchesOnlyMissingHashes is the assertion
// that matters most for a BATCH endpoint: a chunk with one cached and one
// uncached transaction must ask Koios for the uncached hash ONLY, not
// re-fetch the whole chunk, and must merge the two sources back into
// request order (which the from-genesis walk depends on, since a UTxO
// created by one transaction can be spent by a later one in the same chunk).
//
// Reverting fetchTxInfosCached to an all-or-nothing check (any miss =>
// fetch the full chunk) leaves the cached hash in the request body and fails
// the requested-hashes assertion below, even though the request COUNT would
// be unchanged.
func TestFetchTxInfosCached_PartialHitFetchesOnlyMissingHashes(t *testing.T) {
	ctx := context.Background()

	var mu sync.Mutex
	var requested [][]string
	koiosURL, reqCount := countingKoiosServer(t, txInfoHandler(t, &requested, &mu))
	koios, err := NewKoiosClient("preview", "", koiosURL, true, true)
	require.NoError(t, err)

	cache := openTestCache(t)
	require.NoError(t, cache.UpsertTxInfos(
		"preview",
		[]koiosparity.KoiosTxInfoItem{syntheticTxInfo("cached1")},
		time.Now().UTC(),
	))

	chunk := []string{"cached1", "fresh1", "fresh2"}
	got, err := fetchTxInfosCached(ctx, koios, cache, "preview", chunk)
	require.NoError(t, err)
	require.Equal(t, int32(1), reqCount.Load(),
		"a partially cached chunk must make exactly one live request")

	mu.Lock()
	sent := requested
	mu.Unlock()
	require.Len(t, sent, 1)
	require.Equal(t, []string{"fresh1", "fresh2"}, sent[0],
		"only the uncached hashes may reach live Koios")

	require.Equal(t, []koiosparity.KoiosTxInfoItem{
		syntheticTxInfo("cached1"),
		syntheticTxInfo("fresh1"),
		syntheticTxInfo("fresh2"),
	}, got, "cached and freshly-fetched items must merge in request order")

	// The freshly-fetched pair must have been written back, so replaying the
	// same chunk -- exactly what a node-parity restart from genesis does --
	// costs nothing.
	got2, err := fetchTxInfosCached(ctx, koios, cache, "preview", chunk)
	require.NoError(t, err)
	require.Equal(t, got, got2)
	require.Equal(t, int32(1), reqCount.Load(),
		"a replay of an already-fetched chunk must make no further live request")
}

// TestFetchTxInfosCached_NilCacheStillFetchesLive proves the cache is purely
// additive: a run without a cache (--koios-cache-path unset) behaves exactly
// as it did before, fetching the whole chunk live.
func TestFetchTxInfosCached_NilCacheStillFetchesLive(t *testing.T) {
	ctx := context.Background()

	var mu sync.Mutex
	var requested [][]string
	koiosURL, reqCount := countingKoiosServer(t, txInfoHandler(t, &requested, &mu))
	koios, err := NewKoiosClient("preview", "", koiosURL, true, true)
	require.NoError(t, err)

	got, err := fetchTxInfosCached(ctx, koios, nil, "preview", []string{"x", "y"})
	require.NoError(t, err)
	require.Equal(t, int32(1), reqCount.Load())
	require.Equal(t, []koiosparity.KoiosTxInfoItem{
		syntheticTxInfo("x"), syntheticTxInfo("y"),
	}, got)
}

// TestFetchTxInfosCached_LiveFetchErrorIsNotSwallowed proves a Koios failure
// on the uncached remainder still fails the whole call, so
// applyTxInfoResults keeps re-baselining and marking the epoch tainted
// instead of silently applying only the cached half of a chunk -- the
// partially-applied reconstruction would be exactly the silent data loss
// KoiosClient.GetTxInfos' all-or-nothing contract exists to prevent.
func TestFetchTxInfosCached_LiveFetchErrorIsNotSwallowed(t *testing.T) {
	ctx := context.Background()

	koiosURL, _ := countingKoiosServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	koios, err := NewKoiosClient("preview", "", koiosURL, true, true)
	require.NoError(t, err)

	cache := openTestCache(t)
	require.NoError(t, cache.UpsertTxInfos(
		"preview",
		[]koiosparity.KoiosTxInfoItem{syntheticTxInfo("cached1")},
		time.Now().UTC(),
	))

	_, err = fetchTxInfosCached(
		ctx, koios, cache, "preview", []string{"cached1", "missing1"},
	)
	require.Error(t, err,
		"a live failure for the uncached part of a chunk must fail the whole call")
}

// TestCheckStakeDistributionReportsATotalShortfall pins the call site, not
// just the helper: compareTotalActiveStake is only useful if
// CheckStakeDistribution actually calls it, and a helper-only test stays
// green when the call is deleted -- the same unpinned-call-site gap raised
// on #4319.
//
// Dingo reports one pool; the cached Koios epoch total covers two. That is
// dingo#4321's failure mode: the per-pool loop finds nothing wrong, because
// it never asks about a pool Dingo did not report.
func TestCheckStakeDistributionReportsATotalShortfall(t *testing.T) {
	const magic = 764824073
	const epoch = uint64(502)
	const dingoStake = uint64(1_000_000)

	var poolID ledger.PoolId
	poolID[0] = 0xCD
	bech32 := poolID.String()

	lsq := newWiringFakeLSQServer()
	lsq.setPoolDistr(&localstatequery.PoolDistr2Result{
		Pools: map[ledger.PoolId]localstatequery.PoolDistr2IndividualStake{
			poolID: {
				StakeFraction:  &cbor.Rat{Rat: big.NewRat(1, 1)},
				TotalPoolStake: dingoStake,
			},
		},
		TotalActiveStake: dingoStake,
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	lsq.serve(t, listener, magic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := dialWiringClient(t, ctx, listener.Addr().String(), magic)

	koiosURL, _ := countingKoiosServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	koios, err := NewKoiosClient("preview", "", koiosURL, true, true)
	require.NoError(t, err)

	cache := openTestCache(t)
	// The pool Dingo reported agrees exactly, so the per-pool loop is silent.
	require.NoError(t, cache.UpsertPoolEpoch(koiosparity.KoiosPoolEpoch{
		Network:     "preview",
		Epoch:       epoch,
		PoolBech32:  bech32,
		ActiveStake: "1000000",
		FetchedAt:   time.Now().UTC(),
	}))
	// Koios's epoch-wide total is twice that: a second pool exists that
	// Dingo never reported.
	require.NoError(t, cache.UpsertEpochInfo(koiosparity.KoiosEpochInfo{
		Network:     "preview",
		Epoch:       epoch,
		ActiveStake: "2000000",
		FetchedAt:   time.Now().UTC(),
	}))

	mismatches, err := CheckStakeDistribution(ctx, client, koios, cache, "preview", epoch)
	require.NoError(t, err)
	require.Len(t, mismatches, 1,
		"the missing pool must be reported via the epoch total")
	require.Equal(t, ReasonTotalActiveStakeMismatch, mismatches[0].Reason)
	require.Equal(t, int64(-1_000_000), mismatches[0].DiffLovelace,
		"the shortfall equals the unreported pool's stake")
	require.False(t, mismatches[0].KoiosFault,
		"a shortfall is a real divergence, not a Koios-side fault")
}

// TestCheckStakeDistributionKeepsPoolFindingsWhenTheEpochTotalFails is the
// blocker raised in review on #4781, and the reason the epoch_info failure
// is a KoiosFault mismatch rather than an error return.
//
// An error return discards every per-pool mismatch CheckStakeDistribution
// has already collected. from-genesis's recordEpoch then logs only "stake
// distribution check did not run" and never inspects StakeMismatches, so a
// divergence the base branch reports plainly would be hidden by an outage
// of the endpoint this branch added. The check meant to close a blind spot
// would have opened a worse one.
//
// Dingo reports 1,000,000 for a pool Koios has cached at 900,000, so the
// per-pool half finds a real divergence with no network call; /epoch_info
// then fails. Both must survive: the pool mismatch as a real finding, the
// fetch failure as a fault that leaves the epoch unverified.
func TestCheckStakeDistributionKeepsPoolFindingsWhenTheEpochTotalFails(t *testing.T) {
	const magic = 764824073
	const epoch = uint64(504)
	const dingoStake = uint64(1_000_000)

	var poolID ledger.PoolId
	poolID[0] = 0x5A
	bech32 := poolID.String()

	lsq := newWiringFakeLSQServer()
	lsq.setPoolDistr(&localstatequery.PoolDistr2Result{
		Pools: map[ledger.PoolId]localstatequery.PoolDistr2IndividualStake{
			poolID: {
				StakeFraction:  &cbor.Rat{Rat: big.NewRat(1, 1)},
				TotalPoolStake: dingoStake,
			},
		},
		TotalActiveStake: dingoStake,
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	lsq.serve(t, listener, magic)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := dialWiringClient(t, ctx, listener.Addr().String(), magic)

	koiosURL, _ := countingKoiosServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	koios, err := NewKoiosClient("preview", "", koiosURL, true, true)
	require.NoError(t, err)

	cache := openTestCache(t)
	// Cached and disagreeing: the per-pool half finds this without a request.
	require.NoError(t, cache.UpsertPoolEpoch(koiosparity.KoiosPoolEpoch{
		Network:     "preview",
		Epoch:       epoch,
		PoolBech32:  bech32,
		ActiveStake: "900000",
		FetchedAt:   time.Now().UTC(),
	}))
	// No epoch_info row, so the total check goes to the failing Koios above.

	mismatches, err := CheckStakeDistribution(
		ctx, client, koios, cache, "preview", epoch,
	)
	require.NoError(t, err,
		"an epoch_info outage must not fail the whole check and throw away "+
			"the per-pool findings")

	var pool, fault *StakeMismatch
	for i := range mismatches {
		if mismatches[i].KoiosFault {
			fault = &mismatches[i]
		} else {
			pool = &mismatches[i]
		}
	}

	require.NotNil(t, pool,
		"the real per-pool divergence must survive the epoch_info failure")
	require.Equal(t, bech32, pool.PoolIDBech32)
	require.Equal(t, int64(100_000), pool.DiffLovelace)

	require.NotNil(t, fault,
		"the epoch_info failure must still be reported, or the epoch reads "+
			"as fully verified when the total check never ran")
	require.Empty(t, fault.PoolIDBech32)
	require.Contains(t, fault.Reason, ReasonKoiosEpochInfoUnavailable)
}

// TestApplyResolvedEra guards against CheckProtocolParams ignoring a
// GetCurrentEra error and falling back to ProtocolParamsFromNative's
// type-inferred era guess, which cannot tell Shelley and Allegra apart.
// queryHardFork's HardForkCurrentEraQuery case returns errEpochNotResolved
// for a pinned point no epoch row covers, instead of silently answering era
// 0, so GetCurrentEra can fail -- and swallowing that error in an Allegra
// epoch would leave the wrong guessed era in place, producing a false
// pparams_era mismatch (CompareEpochProtocolParams/DetermineStatus)
// reported as "ledger state diverged from Koios" for what was actually a
// failed query, not a real divergence.
//
// Exercised directly against applyResolvedEra with plain values rather
// than over a real localstatequery.Client: driving the regression through
// a fake wire server and asserting on exactly how many HardForkCurrentEraQuery
// calls occurred would make the test depend on gouroboros's internal call
// count for GetCurrentProtocolParams -- an implementation detail of a
// third-party client, not this package's contract -- and that assumption
// does not hold on every CI runner.
func TestApplyResolvedEra(t *testing.T) {
	t.Run("era query error fails, does not fall back to a guess", func(t *testing.T) {
		dingoParams := &koiosparity.DingoProtocolParams{
			EraID:   uint(shelley.EraIdShelley),
			EraName: "shelley",
		}
		err := applyResolvedEra(dingoParams, -1, errors.New("boom"))
		require.Error(t, err)
		// The pre-existing type-inferred guess must survive untouched --
		// this is what "not silently swallowed" means in practice: the
		// caller sees the error and never trusts these fields.
		require.Equal(t, uint(shelley.EraIdShelley), dingoParams.EraID)
		require.Equal(t, "shelley", dingoParams.EraName)
	})

	t.Run("resolved era overwrites an ambiguous guess", func(t *testing.T) {
		// ProtocolParamsFromNative's ambiguous guess: Allegra's params
		// type is a type alias for Shelley's, so the type switch alone
		// guesses "shelley" even in an Allegra epoch.
		dingoParams := &koiosparity.DingoProtocolParams{
			EraID:   uint(shelley.EraIdShelley),
			EraName: "shelley",
		}
		err := applyResolvedEra(dingoParams, int(allegra.EraIdAllegra), nil)
		require.NoError(t, err)
		require.Equal(t, uint(allegra.EraIdAllegra), dingoParams.EraID)
		require.Equal(t, "Allegra", dingoParams.EraName)
	})

	t.Run("unknown era ID leaves the existing guess in place without erroring", func(t *testing.T) {
		dingoParams := &koiosparity.DingoProtocolParams{
			EraID:   uint(shelley.EraIdShelley),
			EraName: "shelley",
		}
		err := applyResolvedEra(dingoParams, 999, nil)
		require.NoError(t, err)
		require.Equal(t, uint(shelley.EraIdShelley), dingoParams.EraID)
		require.Equal(t, "shelley", dingoParams.EraName)
	})
}

func TestNewKoiosClientRejectsMainnet(t *testing.T) {
	if _, err := NewKoiosClient("mainnet", "", "", false, false); err == nil {
		t.Fatal("expected an error for network \"mainnet\", got nil")
	}
	for _, network := range []string{"preview", "preprod"} {
		if _, err := NewKoiosClient(network, "", "", false, false); err != nil {
			t.Fatalf("network %q: unexpected error: %v", network, err)
		}
	}
}

// TestStakeDiffLovelaceIsExact proves stakeDiffLovelace has no tolerance at
// all: both GetPoolDistr2's TotalPoolStake and Koios's pool_history
// active_stake are exact integers with nothing to round between two
// independent computations, so even a 1-lovelace difference must be
// reported, not absorbed. Real observed Preview values (100 trillion
// lovelace per pool), not toy numbers, so a comparison that only worked by
// coincidence at small values would still be caught.
func TestStakeDiffLovelaceIsExact(t *testing.T) {
	const dingoStake = 100_000_000_000_000

	diff, kind := stakeDiffLovelace(dingoStake, "100000000000000")
	if kind != stakeDiffOK {
		t.Fatal("rejected a valid decimal lovelace string")
	}
	if diff != 0 {
		t.Fatalf("identical amounts produced a nonzero diff: %d", diff)
	}

	// A 1-lovelace difference is real and exact integers have nothing to
	// round -- it must be reported, not treated as noise.
	diff, kind = stakeDiffLovelace(dingoStake, "100000000000001")
	if kind != stakeDiffOK {
		t.Fatal("rejected a valid decimal lovelace string")
	}
	if diff != -1 {
		t.Fatalf("a real 1-lovelace difference was not reported exactly: got %d, want -1", diff)
	}

	// A real divergence -- Koios reporting a materially different value --
	// must be caught.
	diff, kind = stakeDiffLovelace(dingoStake, "190000000000000")
	if kind != stakeDiffOK {
		t.Fatal("rejected a valid decimal lovelace string")
	}
	if diff != -90_000_000_000_000 {
		t.Fatalf("a real ~90%% stake divergence was not reported exactly: got %d", diff)
	}

	if _, kind := stakeDiffLovelace(dingoStake, "not-a-number"); kind != stakeDiffUnparseableKoios {
		t.Fatalf("expected stakeDiffUnparseableKoios for an unparseable koios value, got %v", kind)
	}

	// Two independently-reported zero-stake pools must compare as an exact
	// match.
	diff, kind = stakeDiffLovelace(0, "0")
	if kind != stakeDiffOK || diff != 0 {
		t.Fatalf("zero vs zero: kind=%v diff=%v, want stakeDiffOK diff=0", kind, diff)
	}
}

// TestStakeDiffLovelaceOverflowIsNotAKoiosFault guards stakeDiffLovelace's
// IsInt64 branch, which fires when both values parse but their difference
// is too large to represent -- reachable not just from a corrupted koios
// string, but from Dingo itself reporting an impossible TotalPoolStake
// (Cardano's entire max supply fits comfortably inside int64's range, so
// this only happens if dingoStake itself is implausible). Conflating this
// with the unparseable-koios-string case would label it a Koios fault and
// silently drop it from the mismatch count, hiding a genuine Dingo-side bug
// as if it were unremarkable Koios noise. dingoStake is deliberately set
// well beyond Cardano's real max supply (45 billion ADA / 4.5e16 lovelace)
// to force the overflow while koiosStakeStr itself parses cleanly.
func TestStakeDiffLovelaceOverflowIsNotAKoiosFault(t *testing.T) {
	const implausibleDingoStake = math.MaxUint64

	diff, kind := stakeDiffLovelace(implausibleDingoStake, "0")
	if kind != stakeDiffOverflow {
		t.Fatalf("expected stakeDiffOverflow for a Dingo-side implausible "+
			"stake, got kind=%v diff=%v", kind, diff)
	}
}

// TestEvaluatePoolStake pins CheckStakeDistribution's actual per-pool
// decision -- the StakeMismatch (or nil) it returns for the pool's real
// caller, not just stakeDiffLovelace's own return values in isolation.
// TestStakeDiffLovelaceOverflowIsNotAKoiosFault alone does not prove this
// function still builds the right StakeMismatch for an overflowing dingo
// stake: reverting its stakeDiffOverflow case to the "same as unparseable,
// KoiosFault true" shape would leave that test green.
func TestEvaluatePoolStake(t *testing.T) {
	t.Run("no koios row, zero dingo stake: both sides agree, no mismatch", func(t *testing.T) {
		got := evaluatePoolStake("pool1new", 0, nil)
		assert.Nil(t, got)
	})

	t.Run("no koios row, nonzero dingo stake: a real mismatch", func(t *testing.T) {
		got := evaluatePoolStake("pool1missing", 100, nil)
		require.NotNil(t, got)
		assert.False(t, got.KoiosFault)
		assert.Equal(t, "no koios pool_history row for nonzero dingo stake", got.Reason)
	})

	t.Run("unparseable koios value: a koios fault, excluded from mismatch counting by callers", func(t *testing.T) {
		got := evaluatePoolStake("pool1fault", 100, &koiosparity.KoiosPoolHistoryItem{
			ActiveStake: "not-a-number",
		})
		require.NotNil(t, got)
		assert.True(t, got.KoiosFault)
		assert.Equal(t, "unparseable koios active_stake value", got.Reason)
	})

	t.Run("dingo-side overflow: a real mismatch, not a koios fault", func(t *testing.T) {
		got := evaluatePoolStake("pool1overflow", math.MaxUint64, &koiosparity.KoiosPoolHistoryItem{
			ActiveStake: "0",
		})
		require.NotNil(t, got,
			"an implausible dingo stake must still be reported as a mismatch")
		assert.False(t, got.KoiosFault,
			"a dingo-side overflow must not be excluded from the mismatch count as if it were koios noise")
		assert.Equal(
			t,
			"stake difference too large to represent -- dingo's reported stake is implausible",
			got.Reason,
		)
	})

	t.Run("real numeric divergence", func(t *testing.T) {
		got := evaluatePoolStake("pool1diverge", 100, &koiosparity.KoiosPoolHistoryItem{
			ActiveStake: "50",
		})
		require.NotNil(t, got)
		assert.False(t, got.KoiosFault)
		assert.Empty(t, got.Reason)
		assert.Equal(t, int64(50), got.DiffLovelace)
	})

	t.Run("exact match: no mismatch", func(t *testing.T) {
		got := evaluatePoolStake("pool1match", 100, &koiosparity.KoiosPoolHistoryItem{
			ActiveStake: "100",
		})
		assert.Nil(t, got)
	})
}

// TestUTxODiffDetectsRealMismatches proves UTxODiff in all three directions:
// identical sets report no difference, a deliberately injected missing/extra
// ref is caught precisely, and a ref present on both sides with disagreeing
// content is reported as a "differs" entry rather than being missed.
func TestUTxODiffDetectsRealMismatches(t *testing.T) {
	dingo := UTxOSet{
		"4843cf2e582b2f9ce37600e5ab4cc678991f988f8780fed05407f9537f7712bd#0": "addr1abc|1000000",
		"e3ca57e8f323265742a8f4e79ff9af884c9ff8719bd4f7788adaea4c33ba07b6#0": "addr1def|2000000",
	}
	identical := UTxOSet{
		"4843cf2e582b2f9ce37600e5ab4cc678991f988f8780fed05407f9537f7712bd#0": "addr1abc|1000000",
		"e3ca57e8f323265742a8f4e79ff9af884c9ff8719bd4f7788adaea4c33ba07b6#0": "addr1def|2000000",
	}
	if missing, extra, differs := UTxODiff(identical, dingo); len(missing) != 0 || len(extra) != 0 || len(differs) != 0 {
		t.Fatalf("identical sets reported a difference: missing=%v extra=%v differs=%v", missing, extra, differs)
	}

	koiosReconstruction := UTxOSet{
		"4843cf2e582b2f9ce37600e5ab4cc678991f988f8780fed05407f9537f7712bd#0": "addr1abc|9999999",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa#0": "addr1zzz|3000000",
	}
	missing, extra, differs := UTxODiff(koiosReconstruction, dingo)
	if len(missing) != 1 || missing[0] != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa#0" {
		t.Fatalf("expected exactly the injected missing ref, got %v", missing)
	}
	if len(extra) != 1 || extra[0] != "e3ca57e8f323265742a8f4e79ff9af884c9ff8719bd4f7788adaea4c33ba07b6#0" {
		t.Fatalf("expected exactly the injected extra ref, got %v", extra)
	}
	if len(differs) != 1 {
		t.Fatalf("expected exactly one content mismatch, got %v", differs)
	}
}

// TestUTxOChangesAppliesInputsAndOutputs proves the running reconstruction
// correctly adds new outputs (with their full canonical content, not just
// their ref) and removes spent inputs from Koios's own tx_info data.
func TestUTxOChangesAppliesInputsAndOutputs(t *testing.T) {
	set := UTxOSet{
		"spent0000000000000000000000000000000000000000000000000000000000#0": "addr1spent|500000",
	}
	txInfos := []koiosparity.KoiosTxInfoItem{
		{
			TxHash: "newtx000000000000000000000000000000000000000000000000000000000",
			Inputs: []koiosparity.KoiosTxInfoUtxoRef{
				{TxHash: "spent0000000000000000000000000000000000000000000000000000000000", TxIndex: 0},
			},
			Outputs: []koiosparity.KoiosTxInfoOutput{
				{
					TxHash:  "newtx000000000000000000000000000000000000000000000000000000000",
					TxIndex: 0,
					PaymentAddr: struct {
						Bech32 string `json:"bech32"`
					}{Bech32: "addr1new0"},
					Value: "1000000",
				},
				{
					TxHash:  "newtx000000000000000000000000000000000000000000000000000000000",
					TxIndex: 1,
					PaymentAddr: struct {
						Bech32 string `json:"bech32"`
					}{Bech32: "addr1new1"},
					Value: "2000000",
				},
			},
		},
	}
	UTxOChanges(set, txInfos)

	if _, ok := set["spent0000000000000000000000000000000000000000000000000000000000#0"]; ok {
		t.Fatal("spent input was not removed from the set")
	}
	if set["newtx000000000000000000000000000000000000000000000000000000000#0"] != "addr1new0|1000000" ||
		set["newtx000000000000000000000000000000000000000000000000000000000#1"] != "addr1new1|2000000" {
		t.Fatalf("new outputs were not added with correct canonical content: %v", set)
	}
	if len(set) != 2 {
		t.Fatalf("expected exactly 2 live refs after the change, got %d: %v", len(set), set)
	}
}

// TestUTxOChangesPhase2InvalidUsesCollateral proves the reconstruction
// follows what the ledger actually did with a phase-2-invalid transaction,
// not what its body asked for.
//
// Applying such a transaction's body inputs and outputs -- which is what
// reading Inputs/Outputs directly does -- removes refs the ledger never
// spent and adds outputs it never created, while missing the collateral it
// did consume and the collateral return it did produce. Against Dingo's own
// answer that shows up as a false missing/extra pair on every one of those
// refs, persisting until the next re-baseline, and Dingo's collateral
// handling is never actually compared. incremental.go's blockUtxoDelta draws
// the same distinction via gouroboros' Transaction.Consumed()/Produced().
//
// Reverting UTxOChanges to iterate info.Inputs/info.Outputs makes this fail.
func TestUTxOChangesPhase2InvalidUsesCollateral(t *testing.T) {
	const (
		bodyInput  = "bodyin#0"
		collInput  = "collin#7"
		collReturn = "failtx#2"
	)
	set := UTxOSet{
		bodyInput: "addr1body|500000",
		collInput: "addr1coll|9000000",
	}

	invalid := koiosparity.KoiosTxInfoItem{
		TxHash: "failtx",
		Inputs: []koiosparity.KoiosTxInfoUtxoRef{{TxHash: "bodyin", TxIndex: 0}},
		Outputs: []koiosparity.KoiosTxInfoOutput{
			{TxHash: "failtx", TxIndex: 0, Value: "400000"},
			{TxHash: "failtx", TxIndex: 1, Value: "100000"},
		},
		CollateralInputs: []koiosparity.KoiosTxInfoUtxoRef{
			{TxHash: "collin", TxIndex: 7},
		},
		CollateralOutput: &koiosparity.KoiosTxInfoOutput{
			TxHash: "failtx", TxIndex: 2, Value: "8500000",
		},
		PlutusContracts: []koiosparity.KoiosTxInfoPlutusContract{
			{ValidContract: new(false)},
		},
	}

	UTxOChanges(set, []koiosparity.KoiosTxInfoItem{invalid})

	assert.Contains(
		t, set, bodyInput,
		"a phase-2-invalid transaction's body inputs are NOT spent",
	)
	assert.NotContains(
		t, set, collInput,
		"a phase-2-invalid transaction's collateral IS consumed",
	)
	assert.NotContains(
		t, set, "failtx#0",
		"a phase-2-invalid transaction's body outputs are NOT created",
	)
	assert.NotContains(t, set, "failtx#1")
	require.Contains(
		t, set, collReturn,
		"a phase-2-invalid transaction's collateral return IS created",
	)
	assert.Equal(
		t,
		koiosparity.CanonicalKoiosUTxOEntry(*invalid.CollateralOutput),
		set[collReturn],
	)
}

// TestUTxOChangesValidTxWithCollateralUsesBody is the other half: a
// transaction that declares collateral but passes phase-2 validation applies
// its body, and its declared collateral return is never created. Koios
// reports collateral_inputs and collateral_output for these too, so keying
// off their presence rather than the validity verdict would corrupt every
// successful script transaction.
func TestUTxOChangesValidTxWithCollateralUsesBody(t *testing.T) {
	set := UTxOSet{"bodyin#0": "addr1body|500000", "collin#7": "addr1coll|9000000"}

	valid := koiosparity.KoiosTxInfoItem{
		TxHash:  "oktx",
		Inputs:  []koiosparity.KoiosTxInfoUtxoRef{{TxHash: "bodyin", TxIndex: 0}},
		Outputs: []koiosparity.KoiosTxInfoOutput{{TxHash: "oktx", TxIndex: 0, Value: "400000"}},
		CollateralInputs: []koiosparity.KoiosTxInfoUtxoRef{
			{TxHash: "collin", TxIndex: 7},
		},
		CollateralOutput: &koiosparity.KoiosTxInfoOutput{
			TxHash: "oktx", TxIndex: 1, Value: "8500000",
		},
		PlutusContracts: []koiosparity.KoiosTxInfoPlutusContract{
			{ValidContract: new(true)},
		},
	}

	UTxOChanges(set, []koiosparity.KoiosTxInfoItem{valid})

	assert.NotContains(t, set, "bodyin#0", "a valid transaction spends its body inputs")
	assert.Contains(t, set, "collin#7", "a valid transaction does not touch its collateral")
	assert.Contains(t, set, "oktx#0", "a valid transaction creates its body outputs")
	assert.NotContains(
		t, set, "oktx#1",
		"a valid transaction's declared collateral return is never created",
	)
}

// wiringFakeLSQServer is a minimal real gouroboros LocalStateQuery server
// (no ChainSync configured -- neither test under it ever calls ChainSync,
// matching dial.go's own client-side omission) answering exactly the three
// query types CheckProtocolParams and CheckStakeDistribution need:
// HardForkCurrentEraQuery, ShelleyCurrentProtocolParamsQuery, and
// ShelleyPoolDistr2Query.
type wiringFakeLSQServer struct {
	mu    sync.Mutex
	eraID int
	// eraErr, once set (setEraErr), replaces every future
	// HardForkCurrentEraQuery reply with itself -- see
	// TestCheckProtocolParams_FailsWhenEraQueryFails for why this
	// necessarily also fails GetCurrentProtocolParams's own embedded era
	// lookup, not only CheckProtocolParams's later explicit GetCurrentEra
	// call.
	eraErr         error
	protocolParams *shelley.ShelleyProtocolParameters
	poolDistr      *localstatequery.PoolDistr2Result

	// killConnOnNextPoolDistr, when set (killNextPoolDistr), closes the
	// connection a ShelleyPoolDistr2Query arrives on instead of answering it
	// -- reproducing dingo#1900's confirmed live failure shape (the shared
	// connection between CheckProtocolParams and CheckStakeDistribution
	// dying mid-sequence) directly, rather than fabricating an
	// application-level error a real server could never actually send this
	// way: gouroboros's client only ever returns protocol.ErrProtocolShuttingDown
	// (or a raw EOF/closed-connection error) once its own connection is
	// already gone (dial.go's doc comment), never as a decoded query reply.
	// activeConn is the connection the most recent query arrived on, so the
	// handler can close exactly that one.
	killConnOnNextPoolDistr atomic.Bool
	// killAllPoolDistr, when true, closes the connection on every single
	// ShelleyPoolDistr2Query -- unlike killConnOnNextPoolDistr, this never
	// self-clears, simulating sustained connection churn a bounded retry
	// budget cannot outlast (as opposed to the one-off death
	// killNextPoolDistr models).
	killAllPoolDistr atomic.Bool
	activeConn       atomic.Pointer[ouroboros.Connection]
}

// killNextPoolDistr arms killConnOnNextPoolDistr -- see that field's doc
// comment.
func (s *wiringFakeLSQServer) killNextPoolDistr() {
	s.killConnOnNextPoolDistr.Store(true)
}

// alwaysKillPoolDistr arms killAllPoolDistr -- see that field's doc comment.
func (s *wiringFakeLSQServer) alwaysKillPoolDistr() {
	s.killAllPoolDistr.Store(true)
}

// newWiringFakeLSQServer defaults eraID to Conway, matching koios_check.go's
// own doc comment that stake distribution has no era-specific decode path --
// only the protocol-params test needs to override it to exercise the
// Shelley/Allegra ambiguity.
func newWiringFakeLSQServer() *wiringFakeLSQServer {
	return &wiringFakeLSQServer{eraID: int(conway.EraIdConway)}
}

func (s *wiringFakeLSQServer) setEra(eraID int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eraID = eraID
}

func (s *wiringFakeLSQServer) setEraErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.eraErr = err
}

func (s *wiringFakeLSQServer) setProtocolParams(pp *shelley.ShelleyProtocolParameters) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.protocolParams = pp
}

func (s *wiringFakeLSQServer) setPoolDistr(pd *localstatequery.PoolDistr2Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.poolDistr = pd
}

func (s *wiringFakeLSQServer) snapshot() (
	int, error, *shelley.ShelleyProtocolParameters, *localstatequery.PoolDistr2Result,
) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.eraID, s.eraErr, s.protocolParams, s.poolDistr
}

// config builds the localstatequery.Config a real gouroboros server uses to
// answer GetCurrentEra, GetCurrentProtocolParams, and GetPoolDistr2 --
// mirroring incremental_harness_test.go's fakeLSQState.config() dispatch
// convention (BlockQuery -> HardForkQuery/ShelleyQuery -> leaf query type).
func (s *wiringFakeLSQServer) config() localstatequery.Config {
	return localstatequery.NewConfig(
		localstatequery.WithAcquireFunc(
			func(
				_ localstatequery.CallbackContext,
				_ localstatequery.AcquireTarget,
				_ bool,
			) error {
				return nil
			},
		),
		localstatequery.WithQueryFunc(
			func(
				_ localstatequery.CallbackContext,
				q localstatequery.QueryWrapper,
			) (any, error) {
				eraID, eraErr, pp, poolDistr := s.snapshot()
				block, ok := q.Query.(*localstatequery.BlockQuery)
				if !ok {
					return nil, fmt.Errorf("unexpected top-level query %T", q.Query)
				}
				switch inner := block.Query.(type) {
				case *localstatequery.HardForkQuery:
					switch inner.Query.(type) {
					case *localstatequery.HardForkCurrentEraQuery:
						if eraErr != nil {
							return nil, eraErr
						}
						return eraID, nil
					default:
						return nil, fmt.Errorf("unexpected hardfork query %T", inner.Query)
					}
				case *localstatequery.ShelleyQuery:
					switch inner.Query.(type) {
					case *localstatequery.ShelleyCurrentProtocolParamsQuery:
						if pp == nil {
							return nil, fmt.Errorf("wiringFakeLSQServer: no protocol params configured")
						}
						return []any{pp}, nil
					case *localstatequery.ShelleyPoolDistr2Query:
						if s.killAllPoolDistr.Load() || s.killConnOnNextPoolDistr.CompareAndSwap(true, false) {
							if conn := s.activeConn.Load(); conn != nil {
								_ = conn.Close()
							}
							return nil, errors.New(
								"wiringFakeLSQServer: connection killed for test",
							)
						}
						if poolDistr == nil {
							return localstatequery.PoolDistr2Result{
								Pools: map[ledger.PoolId]localstatequery.PoolDistr2IndividualStake{},
							}, nil
						}
						return *poolDistr, nil
					default:
						return nil, fmt.Errorf("unexpected shelley query %T", inner.Query)
					}
				default:
					return nil, fmt.Errorf("unexpected block query %T", block.Query)
				}
			},
		),
		localstatequery.WithReleaseFunc(
			func(_ localstatequery.CallbackContext) error { return nil },
		),
	)
}

// serve starts this fake as a real NtC LocalStateQuery server on listener.
func (s *wiringFakeLSQServer) serve(t *testing.T, listener net.Listener, magic uint32) {
	t.Helper()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				oconn, err := ouroboros.New(
					ouroboros.WithConnection(conn),
					ouroboros.WithServer(true),
					ouroboros.WithNetworkMagic(magic),
					ouroboros.WithNodeToNode(false),
					ouroboros.WithLocalStateQueryConfig(s.config()),
				)
				if err != nil {
					_ = conn.Close()
					return
				}
				s.activeConn.Store(oconn)
				defer oconn.Close() //nolint:errcheck
				<-oconn.ErrorChan()
			}()
		}
	}()
}

// dialWiringClient dials addr and Acquires the volatile tip, returning a
// real *localstatequery.Client ready for CheckProtocolParams/
// CheckStakeDistribution -- both documented as needing an already-Acquired
// client.
func dialWiringClient(
	t *testing.T, ctx context.Context, addr string, magic uint32,
) *localstatequery.Client {
	t.Helper()
	conn, err := Dial(ctx, addr, magic)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	lsq := conn.LocalStateQuery()
	require.NotNil(t, lsq)
	require.NotNil(t, lsq.Client)
	require.NoError(t, lsq.Client.AcquireVolatileTip())
	return lsq.Client
}

// newWiringShelleyProtocolParams returns a fully-populated
// *shelley.ShelleyProtocolParameters -- every *cbor.Rat field non-nil, since
// cbor.Rat.MarshalCBOR panics on a nil underlying *big.Rat rather than
// encoding it as CBOR null (matching incremental_harness_test.go's own
// newFakeProtocolParams doc comment on the identical hazard for Conway).
func newWiringShelleyProtocolParams() *shelley.ShelleyProtocolParameters {
	return &shelley.ShelleyProtocolParameters{
		MinFeeA:            44,
		MinFeeB:            155381,
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
		KeyDeposit:         2000000,
		PoolDeposit:        500000000,
		MaxEpoch:           18,
		NOpt:               100,
		A0:                 &cbor.Rat{Rat: big.NewRat(3, 10)},
		Rho:                &cbor.Rat{Rat: big.NewRat(3, 1000)},
		Tau:                &cbor.Rat{Rat: big.NewRat(1, 5)},
		Decentralization:   &cbor.Rat{Rat: big.NewRat(0, 1)},
		ProtocolMajor:      3,
		ProtocolMinor:      0,
		MinUtxoValue:       1000000,
		MinPoolCost:        340000000,
	}
}

// TestCheckProtocolParams_AppliesWireResolvedEraOverAmbiguousGuess drives
// CheckProtocolParams itself -- not applyResolvedEra directly -- over a real
// *localstatequery.Client against a real gouroboros LocalStateQuery server,
// and a real *koiosparity.KoiosClient against an httptest Koios fake,
// exploiting the exact ambiguity applyResolvedEra exists to resolve:
// allegra.AllegraProtocolParameters is a type alias for
// shelley.ShelleyProtocolParameters, so a client decoding Allegra-era
// protocol params gets back a value ProtocolParamsFromNative's type switch
// alone cannot distinguish from genuine Shelley -- it always guesses
// "Shelley" (see koios_check.go's own doc comment on this exact scenario).
//
// The fake LocalStateQuery server reports the wire-authoritative era as
// Allegra (HardForkCurrentEraQuery) while replying to
// GetCurrentProtocolParams with Shelley-shaped params (the same value
// gouroboros would decode for either era). The fake Koios server reports
// "Allegra" for /epoch_params. If CheckProtocolParams's call to
// applyResolvedEra actually wires the authoritative era into the comparison,
// dingoParams.EraName becomes "Allegra" and CompareEpochProtocolParams finds
// no pparams_era disagreement. If that call site is bypassed, the ambiguous
// "Shelley" guess survives untouched and a real pparams_era
// CategoryValueMismatch appears, which this test fails on.
func TestCheckProtocolParams_AppliesWireResolvedEraOverAmbiguousGuess(t *testing.T) {
	const magic = 764824073
	const epoch = uint64(107)

	lsq := newWiringFakeLSQServer()
	lsq.setEra(int(allegra.EraIdAllegra))
	lsq.setProtocolParams(newWiringShelleyProtocolParams())

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	lsq.serve(t, listener, magic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := dialWiringClient(t, ctx, listener.Addr().String(), magic)

	koiosSrv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/epoch_params" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(
				w, `[{"epoch_no":%d,"era":"Allegra"}]`, epoch,
			)
		},
	))
	t.Cleanup(koiosSrv.Close)

	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
	require.NoError(t, err)

	mismatches, err := CheckProtocolParams(ctx, client, koios, nil, "preview", epoch)
	require.NoError(t, err)
	for _, m := range mismatches {
		if m.Field == "pparams_era" {
			t.Fatalf(
				"unexpected pparams_era mismatch: dingo=%q koios=%q -- "+
					"CheckProtocolParams did not apply the wire-resolved "+
					"era over ProtocolParamsFromNative's ambiguous "+
					"type-inferred guess",
				m.DingoValue, m.KoiosValue,
			)
		}
	}
}

// TestCheckStakeDistribution_DetectsRealPoolStakeDivergence drives
// CheckStakeDistribution itself -- not evaluatePoolStake directly -- over a
// real *localstatequery.Client's GetPoolDistr2 reply and a real
// *koiosparity.KoiosClient's /pool_history reply, with the two sides
// deliberately disagreeing on one pool's active stake. If
// CheckStakeDistribution's per-pool goroutine still calls evaluatePoolStake
// on the real GetPoolDistr2/pool_history data, the disagreement surfaces as
// a StakeMismatch with the exact expected fields. If that call site is
// bypassed (the reverted call site this test proves against), the real
// divergence is silently dropped and this test fails on an empty result.
func TestCheckStakeDistribution_DetectsRealPoolStakeDivergence(t *testing.T) {
	const magic = 764824073
	const epoch = uint64(500)
	const dingoStake = uint64(1_000_000)
	const koiosStake = "400000"

	var poolID ledger.PoolId
	poolID[0] = 0xAB
	poolID[27] = 0xCD
	bech32 := poolID.String()

	lsq := newWiringFakeLSQServer()
	lsq.setPoolDistr(&localstatequery.PoolDistr2Result{
		Pools: map[ledger.PoolId]localstatequery.PoolDistr2IndividualStake{
			poolID: {
				StakeFraction:  &cbor.Rat{Rat: big.NewRat(1, 2)},
				TotalPoolStake: dingoStake,
			},
		},
		TotalActiveStake: dingoStake,
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	lsq.serve(t, listener, magic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := dialWiringClient(t, ctx, listener.Addr().String(), magic)

	koiosSrv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/pool_history":
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(
					w,
					`[{"epoch_no":%d,"active_stake":"%s"}]`,
					epoch, koiosStake,
				)
			case "/epoch_info":
				// Matches what Dingo reports, so compareTotalActiveStake
				// stays silent and the one mismatch asserted below is
				// unambiguously the injected per-pool divergence.
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(
					w,
					`[{"epoch_no":%d,"active_stake":"%d"}]`,
					epoch, dingoStake,
				)
			default:
				w.WriteHeader(http.StatusNotFound)
			}
		},
	))
	t.Cleanup(koiosSrv.Close)

	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
	require.NoError(t, err)

	mismatches, err := CheckStakeDistribution(ctx, client, koios, nil, "preview", epoch)
	require.NoError(t, err)
	require.Len(
		t, mismatches, 1,
		"expected exactly the injected dingo/koios stake divergence, got %+v",
		mismatches,
	)
	got := mismatches[0]
	assert.Equal(t, bech32, got.PoolIDBech32)
	assert.Equal(t, dingoStake, got.DingoStake)
	assert.Equal(t, koiosStake, got.KoiosStake)
	assert.Equal(t, int64(600_000), got.DiffLovelace)
	assert.Empty(t, got.Reason)
	assert.False(t, got.KoiosFault)
}

// TestCheckProtocolParams_FailsWhenEraQueryFails drives CheckProtocolParams
// itself over a real *localstatequery.Client whose wire-level
// HardForkCurrentEraQuery always fails, proving CheckProtocolParams returns
// an error with nil mismatches -- never a result built on
// ProtocolParamsFromNative's ambiguous type-inferred guess -- when Dingo's
// era cannot be resolved at all. TestApplyResolvedEra already pins
// applyResolvedEra itself (the helper `if err := applyResolvedEra(...); err
// != nil` at koios_check.go delegates to) with plain values.
//
// This test does NOT isolate that call site specifically: a wire-level
// HardForkCurrentEraQuery failure necessarily fails
// client.GetCurrentProtocolParams's own embedded era lookup first
// (gouroboros's Client.getCurrentEra caches the resolved era in c.currentEra
// only after a successful lookup, and GetCurrentProtocolParams calls it
// internally before CheckProtocolParams ever reaches its own explicit
// GetCurrentEra call), so this test actually observes CheckProtocolParams
// failing at "dingo protocol params query", one line above applyResolvedEra's
// own call site. Confirmed by temporarily deleting that call site's error
// check (`_ = eraID; _ = eraErr` in place of it): this test still passed
// unchanged, proving it does not regression-guard that specific line.
//
// There is no way to make only the second, explicit call fail over a real
// wire connection, and this is not merely a limitation of this test's own
// server setup: gouroboros's Client.getCurrentEra returns its cached
// c.currentEra immediately, with no wire round trip, whenever
// c.currentEra > -1, and sets it only after a query that succeeds -- never
// on failure, and never reset afterward. CheckProtocolParams's explicit
// GetCurrentEra call is only ever reached once GetCurrentProtocolParams has
// already returned successfully on that same client, which is only
// possible once its own internal getCurrentEra call has already succeeded
// and cached a value. So whenever the explicit call executes, it is
// provably a cache hit -- it cannot fail on a real client, full stop, not
// just in this test's fake-server configuration. Reaching this call site's
// error branch at all requires decoupling the two outcomes with a test
// double; see TestCheckProtocolParams_PropagatesExplicitEraQueryError,
// which does that via protocolParamsClient/fakeProtocolParamsClient and is
// the test that actually regression-guards this specific line. This test
// is kept anyway because it still pins a real, adjacent contract
// (CheckProtocolParams never fabricates a result once era resolution is
// broken) that nothing else exercises over a real wire connection.
func TestCheckProtocolParams_FailsWhenEraQueryFails(t *testing.T) {
	const magic = 764824073
	const epoch = uint64(107)

	lsq := newWiringFakeLSQServer()
	lsq.setEraErr(errors.New("boom: era query failed"))
	lsq.setProtocolParams(newWiringShelleyProtocolParams())

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	lsq.serve(t, listener, magic)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := dialWiringClient(t, ctx, listener.Addr().String(), magic)

	koiosSrv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		},
	))
	t.Cleanup(koiosSrv.Close)

	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
	require.NoError(t, err)

	mismatches, err := CheckProtocolParams(ctx, client, koios, nil, "preview", epoch)
	require.Error(t, err)
	assert.Nil(t, mismatches)
}

// fakeProtocolParamsClient implements protocolParamsClient with fully
// independent GetCurrentProtocolParams/GetCurrentEra outcomes -- something
// no real *localstatequery.Client can offer, since it only ever resolves
// its era once per connection and caches it on success (see
// protocolParamsClient's own doc comment in koios_check.go). It exists
// solely for TestCheckProtocolParams_PropagatesExplicitEraQueryError.
type fakeProtocolParamsClient struct {
	pp     lcommon.ProtocolParameters
	eraID  int
	eraErr error
}

func (f *fakeProtocolParamsClient) GetCurrentProtocolParams() (lcommon.ProtocolParameters, error) {
	return f.pp, nil
}

func (f *fakeProtocolParamsClient) GetCurrentEra() (int, error) {
	return f.eraID, f.eraErr
}

// TestCheckProtocolParams_PropagatesExplicitEraQueryError pins
// koios_check.go's `eraID, eraErr := client.GetCurrentEra()` /
// `if err := applyResolvedEra(dingoParams, eraID, eraErr); err != nil`
// call site directly -- the exact thing
// TestCheckProtocolParams_FailsWhenEraQueryFails's own doc comment proves it
// cannot reach over a real wire connection, because a real
// *localstatequery.Client can never let GetCurrentProtocolParams succeed
// while a later GetCurrentEra on that same client fails (its era cache is
// set only on success and never re-queries the wire once set).
// fakeProtocolParamsClient breaks that coupling: GetCurrentProtocolParams
// always succeeds here, independent of eraErr.
//
// Reverting the call site to ignore eraErr (for example replacing it with
// a hardcoded nil while keeping a `_ = eraErr` no-op so it still compiles)
// makes applyResolvedEra apply the fake's eraID unconditionally.
// CheckProtocolParams then proceeds past era resolution, and Koios's
// /epoch_params 404 is recorded as an ordinary KoiosFault mismatch rather
// than a hard error -- CheckProtocolParams returns (mismatches, nil)
// instead of (nil, err), and require.Error below fails.
func TestCheckProtocolParams_PropagatesExplicitEraQueryError(t *testing.T) {
	const epoch = uint64(107)

	client := &fakeProtocolParamsClient{
		pp:     newWiringShelleyProtocolParams(),
		eraID:  int(conway.EraIdConway),
		eraErr: errors.New("boom: explicit era query failed"),
	}

	koiosSrv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
		},
	))
	t.Cleanup(koiosSrv.Close)

	koios, err := NewKoiosClient("preview", "", koiosSrv.URL, true, true)
	require.NoError(t, err)

	mismatches, err := CheckProtocolParams(
		context.Background(), client, koios, nil, "preview", epoch,
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "explicit era query failed")
	assert.Nil(t, mismatches)
}
