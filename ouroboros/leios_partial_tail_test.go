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
	"context"
	"slices"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	"github.com/stretchr/testify/require"
)

// diffusingBlockTxsRequester simulates the relay near the live tip: it has only
// diffused the first `available` transactions of the endorser block, so it
// serves requested indices below that watermark and nothing above it. Raising
// `available` between fetch attempts models the relay finishing diffusion
// before it re-offers the block. Each served transaction's CBOR encodes its
// absolute index so callers can verify ordering, and every requested index is
// recorded so a test can prove a resumed fetch asks only for the missing tail.
type diffusingBlockTxsRequester struct {
	available int
	calls     int
	requested []int
}

func (r *diffusingBlockTxsRequester) BlockTxsRequest(
	_ context.Context,
	point ocommon.Point,
	bitmaps map[uint16]uint64,
) (protocol.Message, error) {
	r.calls++
	requested := leiosBitmapTxIndices(bitmaps)
	slices.Sort(requested)
	r.requested = append(r.requested, requested...)
	served := map[uint16]uint64{}
	txs := make([]cbor.RawMessage, 0, len(requested))
	for _, idx := range requested {
		if idx >= r.available {
			continue
		}
		served[uint16(idx/64)] |= 1 << uint(
			63-(idx%64),
		) // MSB-first, see leiosWindowNeededMask
		enc, err := cbor.Encode(idx)
		if err != nil {
			return nil, err
		}
		txs = append(txs, cbor.RawMessage(enc))
	}
	return leiosfetch.NewMsgBlockTxsFull(point, served, txs), nil
}

// A fetch that runs out of diffused transactions must retain what it already
// holds against the cached endorser block instead of discarding it. Before
// this, the partial prefix was dropped on the floor and the next offer
// re-fetched the whole block from scratch (issue #2629).
func TestFetchLeiosEbTxsRetainsPartialTailOnIncompleteFetch(t *testing.T) {
	const txCount = 100
	const diffused = 40
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 7, txCount)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))

	requester := &diffusingBlockTxsRequester{available: diffused}
	txs, err := o.fetchLeiosEbTxsBatched(requester, point, txCount, nil)
	require.Error(t, err)
	requireTxsInIndexOrder(t, txs, diffused)

	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.False(t, data.completeTxCache())
	require.Equal(
		t,
		diffused,
		data.partialTxCount(),
		"partially fetched endorser block was discarded",
	)
	// What was retained is the diffused prefix itself, in index order, so the
	// next attempt resumes rather than re-fetching it.
	requireTxsInIndexOrder(t, leiosCollectTxs(data.partialTxs), diffused)
}

// A re-offer of the same endorser block must fetch only the still-missing
// transactions and complete the cached entry, rather than re-fetching the
// transactions dingo already holds.
func TestFetchLeiosEbTxsCompletesPartialTailOnReoffer(t *testing.T) {
	const txCount = 100
	const diffused = 40
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 11, txCount)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))

	first := &diffusingBlockTxsRequester{available: diffused}
	_, err := o.fetchLeiosEbTxsBatched(first, point, txCount, nil)
	require.Error(t, err)

	// The relay finished diffusing and re-offers the block.
	second := &diffusingBlockTxsRequester{available: txCount}
	txs, err := o.fetchLeiosEbTxsBatched(second, point, txCount, nil)
	require.NoError(t, err)
	requireTxsInIndexOrder(t, txs, txCount)

	require.NotEmpty(t, second.requested)
	require.Equal(
		t,
		diffused,
		slices.Min(second.requested),
		"re-offer re-fetched transactions already held",
	)
	require.Equal(t, txCount-1, slices.Max(second.requested))

	// Completing the block stores it through the unchanged path, so the
	// existing tip gate applies it exactly as a single-attempt fetch would.
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, txs))
	slot, gotTxs, ok := o.EndorserBlockTxsByHash(point.Hash)
	require.True(t, ok)
	require.Equal(t, point.Slot, slot)
	requireTxsInIndexOrder(t, gotTxs, txCount)

	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.True(t, data.completeTxCache())
	require.Zero(
		t,
		data.partialTxCount(),
		"completed endorser block still retains partial-fetch state",
	)
}

// The relay offers each endorser block on every connection, so a manifest-only
// store routinely lands after another connection has fetched part of the
// transaction set. It must not drop the retained partial: doing so would send
// the next re-offer back to a from-scratch fetch. This mirrors the existing
// no-clobber invariant for a complete transaction set.
func TestStoreLeiosEndorserBlockManifestKeepsPartialTail(t *testing.T) {
	const txCount = 100
	const diffused = 40
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 13, txCount)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))

	requester := &diffusingBlockTxsRequester{available: diffused}
	_, err := o.fetchLeiosEbTxsBatched(requester, point, txCount, nil)
	require.Error(t, err)

	for range 3 {
		require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))
		data, ok := o.lookupLeiosEndorserBlock(point.Hash)
		require.True(t, ok)
		require.Equal(
			t,
			diffused,
			data.partialTxCount(),
			"redundant manifest store dropped the retained partial tail",
		)
	}
}

// Two connections can fetch overlapping parts of the same endorser block. The
// retained partial is a union, so neither attempt's progress is lost and the
// block completes once their combined coverage is whole.
func TestRetainLeiosPartialTxsUnionsAcrossAttempts(t *testing.T) {
	const txCount = 100
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 17, txCount)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))

	head := make([]cbor.RawMessage, txCount)
	tail := make([]cbor.RawMessage, txCount)
	for i := range txCount {
		enc, err := cbor.Encode(i)
		require.NoError(t, err)
		if i < 60 {
			head[i] = cbor.RawMessage(enc)
		}
		if i >= 40 {
			tail[i] = cbor.RawMessage(enc)
		}
	}
	o.retainLeiosPartialTxs(point.Hash, head, nil)
	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.Equal(t, 60, data.partialTxCount())

	o.retainLeiosPartialTxs(point.Hash, tail, nil)
	data, ok = o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.Equal(t, txCount, data.partialTxCount())

	// A fetch seeded from the union needs no further transactions from the
	// relay at all.
	requester := &diffusingBlockTxsRequester{available: 0}
	txs, err := o.fetchLeiosEbTxsBatched(requester, point, txCount, nil)
	require.NoError(t, err)
	requireTxsInIndexOrder(t, txs, txCount)
	require.Zero(t, requester.calls)
}

// Retention is scoped to endorser blocks dingo is actually tracking: a partial
// for an unknown hash is dropped rather than growing the cache.
func TestRetainLeiosPartialTxsIgnoresUnknownBlock(t *testing.T) {
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	o.retainLeiosPartialTxs([]byte{0xde, 0xad}, []cbor.RawMessage{
		mustCbor(t, "tx0"),
	}, nil)
	_, ok := o.lookupLeiosEndorserBlock([]byte{0xde, 0xad})
	require.False(t, ok)
}

// An endorser block that never completes must not stay resident forever. The
// relay offers each block on every connection and every one of those offers
// re-stores the manifest, rebuilding the cache entry with a fresh insertedAt.
// Carrying the retained partial across that store must not also restart the
// block's ten-minute lifetime: the entry now holds transaction bodies rather
// than just a manifest, and a steady trickle of re-offers would otherwise keep
// refreshing it just before expiry, so it would never be pruned.
func TestStoreLeiosEndorserBlockPartialDoesNotRefreshCacheTTL(t *testing.T) {
	const txCount = 100
	const diffused = 40
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 19, txCount)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))

	requester := &diffusingBlockTxsRequester{available: diffused}
	_, err := o.fetchLeiosEbTxsBatched(requester, point, txCount, nil)
	require.Error(t, err)

	// Age the entry to just short of its TTL, the window in which a re-offer
	// would otherwise reset the clock before pruning can evict it.
	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	aged := time.Now().Add(-leiosEndorserBlockCacheTTL + 2*time.Second)
	o.leiosMu.Lock()
	data.insertedAt = aged
	o.leiosMu.Unlock()

	for range 3 {
		require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))
	}
	data, ok = o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.Equal(
		t,
		diffused,
		data.partialTxCount(),
		"redundant manifest store dropped the retained partial tail",
	)
	require.WithinDuration(
		t,
		aged,
		data.insertedAt,
		time.Second,
		"re-offer restarted the cache lifetime of an incomplete endorser block",
	)

	// Completing the block does refresh it: it is now a servable entry with
	// the same lifetime any freshly fetched endorser block gets.
	full := make([]cbor.RawMessage, txCount)
	for i := range full {
		enc, err := cbor.Encode(i)
		require.NoError(t, err)
		full[i] = cbor.RawMessage(enc)
	}
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, full))
	data, ok = o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.True(t, data.completeTxCache())
	require.Zero(t, data.partialTxCount())
	require.WithinDuration(t, time.Now(), data.insertedAt, time.Minute)
}
