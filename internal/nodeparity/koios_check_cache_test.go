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

// Proves node-parity's Koios-backed checks (CheckProtocolParams,
// CheckStakeDistribution, and the UTxO half's fetchTxInfosCached) actually
// read and write the shared
// internal/koiosparity.Cache -- blinklabs-io/dingo#1900's explicit
// requirement that node-parity stop calling live Koios fresh for every
// single epoch with nothing ever saved for reuse.
//
// Each test drives the real function against a real *koiosparity.Cache
// backed by a temp-file SQLite database (matching internal/koiosparity's own
// test convention, e.g. account_universe_cache_test.go) and a real Koios
// httptest fake instrumented with a request counter, so "the cache was
// actually consulted/written" is proven by request counts, not by inspecting
// comparison output that a cache bug could still coincidentally produce.

import (
	"context"
	"encoding/json"
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
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/gouroboros/protocol/localstatequery"
	"github.com/stretchr/testify/require"
)

// openTestCache opens a real, temp-file-backed koiosparity.Cache -- the same
// convention internal/koiosparity's own tests use (e.g.
// account_universe_cache_test.go) -- rather than a fake, so this proves
// behavior against the real Cache/OpenCache implementation dingo's embedded
// koios-parity observer also uses, including its WAL/busy_timeout setup.
func openTestCache(t *testing.T) *koiosparity.Cache {
	t.Helper()
	cache, err := koiosparity.OpenCache(filepath.Join(t.TempDir(), "cache.db"), nil)
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
	koios, err := NewKoiosClient("preview", "", koiosURL, true)
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
	koios, err := NewKoiosClient("preview", "", koiosURL, true)
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
	koios, err := NewKoiosClient("preview", "", koiosURL, true)
	require.NoError(t, err)

	cache := openTestCache(t)
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
	koios, err := NewKoiosClient("preview", "", koiosURL, true)
	require.NoError(t, err)

	cache := openTestCache(t)

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
	koios, err := NewKoiosClient("preview", "", koiosURL, true)
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
	koios, err := NewKoiosClient("preview", "", koiosURL, true)
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
	koios, err := NewKoiosClient("preview", "", koiosURL, true)
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
	koios, err := NewKoiosClient("preview", "", koiosURL, true)
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
