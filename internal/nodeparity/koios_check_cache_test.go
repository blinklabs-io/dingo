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
// CheckStakeDistribution) actually read and write the shared
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
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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
