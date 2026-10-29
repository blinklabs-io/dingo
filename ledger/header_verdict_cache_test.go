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

package ledger

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/stretchr/testify/require"
)

func TestStatelessHeaderCryptoVerdictSharedAcrossCalls(t *testing.T) {
	t.Parallel()

	tb := createTestBlock(t, [32]byte{61}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	var runs atomic.Int64
	ls.statelessHeaderCryptoRunHook = func() { runs.Add(1) }
	block := headerOnlyBlock{header: tb.block.Header(), peerRelative: true}
	epoch := models.Epoch{EpochId: 5}

	for range 5 {
		require.NoError(
			t,
			ls.verifyBlockHeaderStatelessCryptoWithEpochNonce(
				block,
				epoch,
				tb.epochNonce,
			),
		)
	}
	require.Equal(
		t,
		int64(1),
		runs.Load(),
		"the same header and nonce must be verified once, not once per call",
	)
}

func TestStatelessHeaderCryptoVerdictKeyedByNonceAndHeader(t *testing.T) {
	t.Parallel()

	tb := createTestBlock(t, [32]byte{62}, 0, tamperNone)
	other := createTestBlock(t, [32]byte{63}, 0, tamperNone)
	tampered := createTestBlock(t, [32]byte{62}, 0, tamperVRFProof)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	var runs atomic.Int64
	ls.statelessHeaderCryptoRunHook = func() { runs.Add(1) }
	epoch := models.Epoch{EpochId: 5}
	verify := func(b *realBabbageBlock, nonce []byte) error {
		return ls.verifyBlockHeaderStatelessCryptoWithEpochNonce(
			headerOnlyBlock{header: b.Header(), peerRelative: true},
			epoch,
			nonce,
		)
	}

	require.NoError(t, verify(tb.block, tb.epochNonce))
	require.Equal(t, int64(1), runs.Load())

	wrongNonce := append([]byte(nil), tb.epochNonce...)
	wrongNonce[0] ^= 0xff
	require.Error(t, verify(tb.block, wrongNonce), "wrong nonce must not hit")
	require.Equal(t, int64(2), runs.Load())

	require.NotEqual(t, tb.block.Header().Hash(), tampered.block.Header().Hash())
	require.Error(t, verify(tampered.block, tb.epochNonce))
	require.Equal(t, int64(3), runs.Load())
	require.Error(t, verify(tampered.block, tb.epochNonce))
	require.Equal(t, int64(4), runs.Load(), "failures must not be cached")

	_ = verify(other.block, tb.epochNonce)
	require.Equal(t, int64(5), runs.Load(), "a different header must not hit")

	require.NoError(t, verify(tb.block, tb.epochNonce))
	require.Equal(t, int64(5), runs.Load(), "original verdict must survive")
}

func TestStatelessHeaderCryptoVerdictNotSharedWithoutHeaderCbor(t *testing.T) {
	t.Parallel()

	// Verification reads the header body CBOR and the decoded signature, while
	// the header hash covers the stored header CBOR. A header without stored
	// CBOR hashes the empty input, so every such header would share one key.
	good := createTestBlock(t, [32]byte{65}, 0, tamperNone)
	bad := createTestBlock(t, [32]byte{65}, 0, tamperVRFProof)
	ls, _ := newEligibilityTestLedger(t, good.epochNonce)
	epoch := models.Epoch{EpochId: 5}
	withoutHeaderCbor := func(b *realBabbageBlock) headerOnlyBlock {
		return headerOnlyBlock{
			header: &babbage.BabbageBlockHeader{
				Body:      b.header.Body,
				Signature: b.header.Signature,
			},
			peerRelative: true,
		}
	}
	goodBlock := withoutHeaderCbor(good.block)
	badBlock := withoutHeaderCbor(bad.block)
	require.Empty(t, goodBlock.Header().Cbor())
	require.Equal(t, goodBlock.Header().Hash(), badBlock.Header().Hash())

	require.NoError(
		t,
		ls.verifyBlockHeaderStatelessCryptoWithEpochNonce(
			goodBlock,
			epoch,
			good.epochNonce,
		),
	)
	require.Error(
		t,
		ls.verifyBlockHeaderStatelessCryptoWithEpochNonce(
			badBlock,
			epoch,
			good.epochNonce,
		),
		"a header without stored CBOR must not be answered from the cache",
	)
}

func TestStatelessHeaderVerdictCacheBoundedAndConcurrent(t *testing.T) {
	t.Parallel()

	var cache statelessHeaderVerdictCache
	nonce := make([]byte, 32)
	var wg sync.WaitGroup
	for worker := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range statelessHeaderVerdictCacheLimit {
				var hash [32]byte
				hash[0] = byte(worker)
				hash[1] = byte(i)
				hash[2] = byte(i >> 8)
				cache.add(hash[:], nonce)
				_ = cache.contains(hash[:], nonce)
			}
		}()
	}
	wg.Wait()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	require.LessOrEqual(
		t,
		len(cache.entries),
		statelessHeaderVerdictCacheLimit,
	)
	require.Equal(t, len(cache.entries), cache.order.Len())
}

func BenchmarkStatelessHeaderCryptoVerdict(b *testing.B) {
	tb := createTestBlock(b, [32]byte{64}, 0, tamperNone)
	ls := &LedgerState{
		config: LedgerStateConfig{
			CardanoNodeConfig: newHighFreqShelleyGenesisCfg(b),
		},
	}
	block := headerOnlyBlock{header: tb.block.Header(), peerRelative: true}
	epoch := models.Epoch{EpochId: 5}
	verify := func(b *testing.B) {
		if err := ls.verifyBlockHeaderStatelessCryptoWithEpochNonce(
			block,
			epoch,
			tb.epochNonce,
		); err != nil {
			b.Fatal(err)
		}
	}
	b.Run("uncached", func(b *testing.B) {
		for b.Loop() {
			ls.statelessHeaderVerdicts = statelessHeaderVerdictCache{}
			verify(b)
		}
	})
	b.Run("shared", func(b *testing.B) {
		verify(b)
		for b.Loop() {
			verify(b)
		}
	})
}
