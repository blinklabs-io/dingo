package ouroboros

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/stretchr/testify/require"
)

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
			values[reader], errors[reader], _ = cache.getOrDecode(
				key, func() (T, error) {
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
	cached, err, decoded := cache.getOrDecode(key, decode)
	require.NoError(t, err)
	require.False(t, decoded)
	require.Same(t, values[0], cached)
	require.Equal(t, expected, cached.Hash())
	require.EqualValues(t, 1, decodeCalls.Load())
	cache.mu.Lock()
	defer cache.mu.Unlock()
	require.Empty(t, cache.inFlight)
}
