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

package ouroboros

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

// benchConwayBlockFixture and benchConwayHeaderFixture mirror
// conwayBlockFixtureBytes/conwayHeaderFixtureBytes but accept testing.TB so
// the same real fixture loading works from both Test and Benchmark
// functions.
func benchConwayBlockFixture(b *testing.B) (blockType uint, raw []byte) {
	b.Helper()
	blocks, err := testfixtures.GenerateConwayChain(1)
	if err != nil {
		b.Fatalf("generate fixture: %v", err)
	}
	if len(blocks) != 1 {
		b.Fatalf("expected one generated block, got %d", len(blocks))
	}
	return uint(blocks[0].Type()), blocks[0].Cbor()
}

func benchConwayHeaderFixture(b *testing.B) (headerType uint, raw []byte) {
	b.Helper()
	blocks, err := testfixtures.GenerateConwayChain(1)
	if err != nil {
		b.Fatalf("generate fixture: %v", err)
	}
	if len(blocks) != 1 {
		b.Fatalf("expected one generated block, got %d", len(blocks))
	}
	return uint(blocks[0].Type()), blocks[0].Header().Cbor()
}

// --- Blocks -----------------------------------------------------------

// BenchmarkBlockDecodeDirect is the pre-#489 baseline: decode every delivery
// directly, no cache, no hashing, no locking. Every other block benchmark
// below should be read relative to this number.
func BenchmarkBlockDecodeDirect(b *testing.B) {
	o := testOuroborosForDecodeCache(b)
	blockType, raw := benchConwayBlockFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := o.decodeBlockfetchBlock(blockType, raw); err != nil {
			b.Fatalf("decode: %v", err)
		}
	}
}

// BenchmarkBlockDecodeCacheAllDuplicate is the best case for the cache: every
// delivery is byte-identical (the real-world "several peers relay the same
// block" scenario). Only the first call should actually decode.
func BenchmarkBlockDecodeCacheAllDuplicate(b *testing.B) {
	o := testOuroborosForDecodeCache(b)
	blockType, raw := benchConwayBlockFixture(b)
	decodeFn := func() (gledger.Block, error) {
		return o.decodeBlockfetchBlock(blockType, raw)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		// Hash inside the timed loop: the real entry point
		// (blockfetchClientBlockRaw) hashes every delivery's bytes before
		// the cache lookup, even when -- as here -- the bytes are
		// byte-identical to the previous delivery. Precomputing the key
		// once outside the loop would omit that per-delivery cost from the
		// reported numbers.
		key := hashDecodeInput(blockType, raw)
		if _, err, _ := o.blockDecodeCache.getOrDecodeSizedWithErrorRetention(key, 0, true, decodeFn); err != nil {
			b.Fatalf("decode: %v", err)
		}
	}
}

// BenchmarkBlockDecodeCacheAllUnique is the worst case for the cache: every
// delivery is a genuine miss (no duplication ever happens), so this measures
// the pure tax the cache adds -- hashing plus locking plus bookkeeping --
// with zero benefit, on top of the same real decode cost every iteration.
// Compare directly against BenchmarkBlockDecodeDirect: the delta is the cost
// of adding this cache when duplicates never occur.
func BenchmarkBlockDecodeCacheAllUnique(b *testing.B) {
	o := testOuroborosForDecodeCache(b)
	blockType, raw := benchConwayBlockFixture(b)
	decodeFn := func() (gledger.Block, error) {
		return o.decodeBlockfetchBlock(blockType, raw)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		key := hashDecodeInput(blockType, raw)
		// Perturb the key per iteration so every call is a genuine miss,
		// while decodeFn still does the same real decode work each time --
		// isolating cache overhead from decode cost.
		key[0] ^= byte(i)
		key[1] ^= byte(i >> 8)
		key[2] ^= byte(i >> 16)
		if _, err, _ := o.blockDecodeCache.getOrDecodeSizedWithErrorRetention(key, 0, true, decodeFn); err != nil {
			b.Fatalf("decode: %v", err)
		}
	}
}

// BenchmarkBlockDecodeConcurrentDirect is the concurrent pre-#489 baseline:
// many simulated peer connections decoding in parallel with no shared state
// at all, so it should scale cleanly with GOMAXPROCS.
func BenchmarkBlockDecodeConcurrentDirect(b *testing.B) {
	o := testOuroborosForDecodeCache(b)
	blockType, raw := benchConwayBlockFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// testing.TB's FailNow/Fatal family must only be called from the
			// goroutine running the benchmark, not from a RunParallel worker
			// goroutine (it would only runtime.Goexit that one goroutine,
			// not reliably fail the benchmark). Error/Errorf are safe from
			// any goroutine.
			if _, err := o.decodeBlockfetchBlock(blockType, raw); err != nil {
				b.Errorf("decode: %v", err)
			}
		}
	})
}

// BenchmarkBlockDecodeConcurrentCacheAllUnique is the concurrency-specific
// worst case: many simulated peer connections all hitting the shared cache
// lock at once, with every call a genuine miss (no benefit from caching at
// all). This isolates lock contention cost under real concurrent peer load,
// which BenchmarkBlockDecodeCacheAllUnique (single-goroutine) cannot show.
func BenchmarkBlockDecodeConcurrentCacheAllUnique(b *testing.B) {
	o := testOuroborosForDecodeCache(b)
	blockType, raw := benchConwayBlockFixture(b)
	decodeFn := func() (gledger.Block, error) {
		return o.decodeBlockfetchBlock(blockType, raw)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var counter atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// atomic: RunParallel's callback runs concurrently across
			// goroutines, so a plain counter++ here is a data race.
			n := counter.Add(1)
			key := hashDecodeInput(blockType, raw)
			key[0] ^= byte(n)
			key[1] ^= byte(n >> 8)
			key[2] ^= byte(n >> 16)
			key[3] ^= byte(n >> 24)
			if _, err, _ := o.blockDecodeCache.getOrDecodeSizedWithErrorRetention(key, 0, true, decodeFn); err != nil {
				b.Errorf("decode: %v", err)
			}
		}
	})
}

// --- Headers ------------------------------------------------------------
//
// Headers are the case most likely to be a losing trade (small, cheap to
// decode, so the hashing+locking tax is proportionally larger).

func BenchmarkHeaderDecodeDirect(b *testing.B) {
	o := testOuroborosForDecodeCache(b)
	headerType, raw := benchConwayHeaderFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := o.decodeChainsyncHeader(headerType, raw); err != nil {
			b.Fatalf("decode: %v", err)
		}
	}
}

func BenchmarkHeaderDecodeCacheAllDuplicate(b *testing.B) {
	o := testOuroborosForDecodeCache(b)
	headerType, raw := benchConwayHeaderFixture(b)
	decodeFn := func() (gledger.BlockHeader, error) {
		return o.decodeChainsyncHeader(headerType, raw)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		// Hash inside the timed loop -- see BenchmarkBlockDecodeCacheAllDuplicate.
		key := hashDecodeInput(headerType, raw)
		if _, err, _ := o.headerDecodeCache.getOrDecodeSizedWithErrorRetention(key, 0, true, decodeFn); err != nil {
			b.Fatalf("decode: %v", err)
		}
	}
}

func BenchmarkHeaderDecodeCacheAllUnique(b *testing.B) {
	o := testOuroborosForDecodeCache(b)
	headerType, raw := benchConwayHeaderFixture(b)
	decodeFn := func() (gledger.BlockHeader, error) {
		return o.decodeChainsyncHeader(headerType, raw)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		key := hashDecodeInput(headerType, raw)
		key[0] ^= byte(i)
		key[1] ^= byte(i >> 8)
		key[2] ^= byte(i >> 16)
		if _, err, _ := o.headerDecodeCache.getOrDecodeSizedWithErrorRetention(key, 0, true, decodeFn); err != nil {
			b.Fatalf("decode: %v", err)
		}
	}
}

func BenchmarkHeaderDecodeConcurrentDirect(b *testing.B) {
	o := testOuroborosForDecodeCache(b)
	headerType, raw := benchConwayHeaderFixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// See BenchmarkBlockDecodeConcurrentDirect on why Errorf, not
			// Fatalf, is required inside a RunParallel worker goroutine.
			if _, err := o.decodeChainsyncHeader(headerType, raw); err != nil {
				b.Errorf("decode: %v", err)
			}
		}
	})
}

func BenchmarkHeaderDecodeConcurrentCacheAllUnique(b *testing.B) {
	o := testOuroborosForDecodeCache(b)
	headerType, raw := benchConwayHeaderFixture(b)
	decodeFn := func() (gledger.BlockHeader, error) {
		return o.decodeChainsyncHeader(headerType, raw)
	}
	b.ReportAllocs()
	b.ResetTimer()
	var counter atomic.Int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// atomic: see BenchmarkBlockDecodeConcurrentCacheAllUnique.
			n := counter.Add(1)
			key := hashDecodeInput(headerType, raw)
			key[0] ^= byte(n)
			key[1] ^= byte(n >> 8)
			key[2] ^= byte(n >> 16)
			key[3] ^= byte(n >> 24)
			if _, err, _ := o.headerDecodeCache.getOrDecodeSizedWithErrorRetention(key, 0, true, decodeFn); err != nil {
				b.Errorf("decode: %v", err)
			}
		}
	})
}

// BenchmarkBlockDecodeCacheMixedRatio sits between the two extremes already
// covered above (BenchmarkBlockDecodeCacheAllDuplicate: 100% duplicate,
// BenchmarkBlockDecodeCacheAllUnique: 0% duplicate). Real peer traffic is
// neither: a handful of distinct in-flight blocks, each delivered by several
// peers. This cycles through 5 distinct keys (derived from the same real
// block bytes) so 4 out of every 5 calls are a cache hit, giving a more
// representative estimate of steady-state overhead/benefit than either pure
// extreme does alone.
func BenchmarkBlockDecodeCacheMixedRatio(b *testing.B) {
	o := testOuroborosForDecodeCache(b)
	blockType, raw := benchConwayBlockFixture(b)
	decodeFn := func() (gledger.Block, error) {
		return o.decodeBlockfetchBlock(blockType, raw)
	}
	// Five distinct real inputs (copies of the fixture bytes, each with one
	// byte perturbed), hashed inside the timed loop below -- not five
	// precomputed keys derived by flipping a byte of an already-hashed
	// digest. The real entry point always hashes the actual delivery bytes
	// it was just handed, so this measures that per-delivery hashing cost
	// on genuinely different byte content, matching production instead of
	// precomputing the (cheaper) key lookup alone before ResetTimer.
	const distinctInputs = 5
	rawVariants := make([][]byte, distinctInputs)
	for i := range rawVariants {
		variant := make([]byte, len(raw))
		copy(variant, raw)
		variant[0] ^= byte(i)
		rawVariants[i] = variant
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		key := hashDecodeInput(blockType, rawVariants[i%distinctInputs])
		if _, err, _ := o.blockDecodeCache.getOrDecodeSizedWithErrorRetention(key, 0, true, decodeFn); err != nil {
			b.Fatalf("decode: %v", err)
		}
	}
}

func TestDecodeCacheConcurrentHash(t *testing.T) {
	generators := map[string]func(
		uint64, common.Blake2b256, uint64, uint64, int,
	) ([]gledger.Block, error){
		"shelley":  fixtures.GenerateShelleyChain,
		"allegra":  fixtures.GenerateAllegraChain,
		"mary":     fixtures.GenerateMaryChain,
		"alonzo":   fixtures.GenerateAlonzoChain,
		"babbage":  fixtures.GenerateBabbageChain,
		"conway":   fixtures.GenerateConwayChain,
		"dijkstra": fixtures.GenerateDijkstraChain,
	}
	for name, generate := range generators {
		t.Run(name, func(t *testing.T) {
			blocks, err := generate(1, common.Blake2b256{}, 2, 20, 1)
			require.NoError(t, err)
			require.Len(t, blocks, 1)
			block := blocks[0]
			testDecodedHashPublication(t, 0, uint(block.Type()),
				block.Cbor(), block.Header().Cbor(), block.Hash())
		})
	}
	t.Run("byron_ebb", func(t *testing.T) {
		raw := byronEbbFixtureCbor(t)
		block, err := gledger.NewBlockFromCbor(gledger.BlockTypeByronEbb, raw)
		require.NoError(t, err)
		testDecodedHashPublication(t, 0, gledger.BlockTypeByronEbb,
			raw, block.Header().Cbor(), block.Hash())
		t.Run("full_block_header_fallback", func(t *testing.T) {
			testDecodedHashPublication(t, 0, gledger.BlockTypeByronEbb,
				raw, raw, block.Hash())
		})
	})
	t.Run("musashi", func(t *testing.T) {
		raw := readHexFixture(t, musashiType7BlockFixture)
		headerRaw := readHexFixture(t, musashiType7HeaderFixture)
		oracle := testOuroborosForDecodeCache(t)
		oracle.config.NetworkMagic = musashiNetworkMagic
		block, err := oracle.decodeBlockfetchBlock(gledger.BlockTypeConway, raw)
		require.NoError(t, err)
		testDecodedHashPublication(t, musashiNetworkMagic,
			gledger.BlockTypeConway, raw, headerRaw, block.Hash())
	})
}

func testDecodedHashPublication(
	t *testing.T,
	networkMagic uint32,
	blockType uint,
	blockRaw, headerRaw []byte,
	expected common.Blake2b256,
) {
	t.Helper()
	t.Run("header", func(t *testing.T) {
		ob := testOuroborosForDecodeCache(t)
		ob.config.NetworkMagic = networkMagic
		testConcurrentCachedHash(t, ob.headerDecodeCache,
			hashDecodeInput(blockType, headerRaw),
			func() (gledger.BlockHeader, error) {
				return ob.decodeChainsyncHeader(blockType, headerRaw)
			}, expected)
	})
	t.Run("block", func(t *testing.T) {
		ob := testOuroborosForDecodeCache(t)
		ob.config.NetworkMagic = networkMagic
		testConcurrentCachedHash(t, ob.blockDecodeCache,
			hashDecodeInput(blockType, blockRaw),
			func() (gledger.Block, error) {
				return ob.decodeBlockfetchBlock(blockType, blockRaw)
			}, expected)
	})
}

func testConcurrentCachedHash[T gledger.BlockHeader](
	t *testing.T,
	cache *decodeCache[T],
	key decodeCacheKey,
	decode func() (T, error),
	expected common.Blake2b256,
) {
	t.Helper()
	const readers = 8
	decodeStarted := make(chan struct{})
	releaseDecode := make(chan struct{})
	startHash := make(chan struct{})
	ready := make(chan struct{}, readers)
	done := make(chan struct{}, readers)
	finishDecode := sync.OnceFunc(func() { close(releaseDecode) })
	beginHash := sync.OnceFunc(func() { close(startHash) })
	defer finishDecode()
	defer beginHash()
	var decodeCalls atomic.Int64
	values := make([]T, readers)
	errors := make([]error, readers)
	hashes := make([]common.Blake2b256, readers)
	for reader := range readers {
		go func() {
			values[reader], errors[reader], _ = cache.getOrDecodeSizedWithErrorRetention(key, 0, true, func() (T, error) {
				if decodeCalls.Add(1) == 1 {
					close(decodeStarted)
				}
				<-releaseDecode
				return decode()
			})
			ready <- struct{}{}
			<-startHash
			if errors[reader] == nil {
				hashes[reader] = values[reader].Hash()
			}
			done <- struct{}{}
		}()
	}
	testutil.RequireReceive(t, decodeStarted, 5*time.Second, "decode leader")
	require.Eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		return len(cache.inFlight[key]) == readers-1
	}, 5*time.Second, time.Millisecond, "all followers must wait for decode")
	finishDecode()
	for range readers {
		testutil.RequireReceive(t, ready, 5*time.Second, "cache result")
	}
	beginHash()
	for range readers {
		testutil.RequireReceive(t, done, 5*time.Second, "concurrent hash")
	}
	for reader := range readers {
		require.NoError(t, errors[reader])
		require.Same(t, values[0], values[reader])
		require.Equal(t, expected, hashes[reader])
		if block, ok := any(values[reader]).(gledger.Block); ok {
			require.Equal(t, expected, block.Header().Hash())
		}
	}
	cached, err, decoded := cache.getOrDecodeSizedWithErrorRetention(key, 0, true, decode)
	require.NoError(t, err)
	require.False(t, decoded)
	require.Same(t, values[0], cached)
	require.Equal(t, expected, cached.Hash())
	require.EqualValues(t, 1, decodeCalls.Load())
	cache.mu.Lock()
	defer cache.mu.Unlock()
	require.Empty(t, cache.inFlight)
}
