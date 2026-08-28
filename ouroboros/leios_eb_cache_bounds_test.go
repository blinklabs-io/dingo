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
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	"github.com/stretchr/testify/require"
)

// fixedBlockRequester returns a fixed manifest body for every BlockRequest,
// simulating a leios-fetch server's response to a MsgBlockOffer-driven fetch.
type fixedBlockRequester struct {
	blockRaw []byte
	calls    int
}

func (r *fixedBlockRequester) BlockRequest(
	_ context.Context,
	_ ocommon.Point,
) (protocol.Message, error) {
	r.calls++
	return leiosfetch.NewMsgBlock(cbor.RawMessage(r.blockRaw)), nil
}

// withLowerLeiosEndorserBlockCacheBudgets temporarily lowers the byte-budget
// vars so a test can exercise per-entry rejection or aggregate eviction
// without allocating hundreds of megabytes of test data. Restored via
// t.Cleanup so other tests keep the production defaults.
func withLowerLeiosEndorserBlockCacheBudgets(
	t *testing.T,
	maxEntryBytes, maxBytes int,
) {
	t.Helper()
	origEntry, origTotal := leiosEndorserBlockCacheMaxEntryBytes,
		leiosEndorserBlockCacheMaxBytes
	leiosEndorserBlockCacheMaxEntryBytes = maxEntryBytes
	leiosEndorserBlockCacheMaxBytes = maxBytes
	t.Cleanup(func() {
		leiosEndorserBlockCacheMaxEntryBytes = origEntry
		leiosEndorserBlockCacheMaxBytes = origTotal
	})
}

// Valid case: the fetched manifest's length matches what the offer declared,
// so the fetch is accepted and the bytes are returned unchanged.
func TestFetchAndValidateLeiosEbManifestAcceptsMatchingSize(t *testing.T) {
	_, blockRaw := testLeiosEndorserBlockRaw(t, 1)
	client := &fixedBlockRequester{blockRaw: blockRaw}

	got, err := fetchAndValidateLeiosEbManifest(
		context.Background(),
		client,
		ocommon.Point{Slot: 1},
		uint64(len(blockRaw)),
	)
	require.NoError(t, err)
	require.Equal(t, []byte(blockRaw), got)
	require.Equal(t, 1, client.calls)
}

// Mismatched case: a peer that declares one size in its offer and serves a
// body of a different length is rejected rather than cached.
func TestFetchAndValidateLeiosEbManifestRejectsSizeMismatch(t *testing.T) {
	_, blockRaw := testLeiosEndorserBlockRaw(t, 1)
	client := &fixedBlockRequester{blockRaw: blockRaw}

	got, err := fetchAndValidateLeiosEbManifest(
		context.Background(),
		client,
		ocommon.Point{Slot: 1},
		uint64(len(blockRaw))+1,
	)
	require.ErrorContains(t, err, "size mismatch")
	require.Nil(t, got)
}

// Oversized case: an entry whose retained bytes (manifest plus transaction
// bodies) exceed the per-entry budget is rejected rather than cached, and any
// previously cached (smaller) entry for the same hash is left untouched.
func TestStoreLeiosEndorserBlockRejectsOversizedEntry(t *testing.T) {
	withLowerLeiosEndorserBlockCacheBudgets(t, 1<<10, 1<<20) // 1 KiB / 1 MiB
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 7000, 1)
	oversizedTx := cbor.RawMessage(make([]byte, 2<<10)) // 2 KiB > 1 KiB cap

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	err := o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		[]cbor.RawMessage{oversizedTx},
	)
	require.ErrorContains(t, err, "exceeds max")

	_, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.False(t, ok)
}

// Eviction case: once the aggregate byte budget is exceeded, the cache evicts
// the oldest-inserted entries first -- the same policy already used for the
// entry-count cap -- until the remaining entries fit the budget.
func TestLeiosEndorserBlockCacheEvictsOldestFirstAtByteBudget(t *testing.T) {
	// Each entry retains one ~600-byte transaction body; five fit comfortably
	// under a 1500-byte aggregate budget, but not all five at once.
	withLowerLeiosEndorserBlockCacheBudgets(t, 1<<20, 1500)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})

	const entries = 5
	points := make([]ocommon.Point, entries)
	for i := range entries {
		point, blockRaw := testLeiosEndorserBlockRaw(t, i+1)
		points[i] = point
		tx := cbor.RawMessage(make([]byte, 600))
		require.NoError(
			t,
			o.storeLeiosEndorserBlock(point, blockRaw, []cbor.RawMessage{tx}),
		)
	}

	o.leiosMu.RLock()
	totalBytes := 0
	for _, data := range o.leiosEndorserBlocks {
		totalBytes += data.approxBytes()
	}
	o.leiosMu.RUnlock()
	require.LessOrEqual(t, totalBytes, 1500)

	// The oldest entries were evicted; the most recently stored one survives.
	_, ok := o.lookupLeiosEndorserBlock(points[0].Hash)
	require.False(t, ok)
	_, ok = o.lookupLeiosEndorserBlock(points[entries-1].Hash)
	require.True(t, ok)
}

// Eviction correctness: eviction order must follow actual insertion order
// (seq), not the wall-clock insertedAt captured before leiosMu is acquired. A
// delayed goroutine can win the lock later while still carrying an earlier
// insertedAt than a goroutine that actually inserted first; sorting by seq
// instead keeps the truly-older entry as the eviction victim.
func TestLeiosEndorserBlockCacheEvictionOrdersBySeqNotInsertedAt(t *testing.T) {
	withLowerLeiosEndorserBlockCacheBudgets(t, 1<<20, 300)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	tx := func() cbor.RawMessage { return cbor.RawMessage(make([]byte, 200)) }

	firstPoint, firstRaw := testLeiosEndorserBlockRaw(t, 1)
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(firstPoint, firstRaw, []cbor.RawMessage{tx()}),
	)

	// Simulate the race directly: the entry inserted first (and so holding
	// the lower seq) is given a later wall-clock insertedAt than the entry
	// about to be inserted second.
	o.leiosMu.Lock()
	first := o.leiosEndorserBlocks[leiosBlockKey(firstPoint.Hash)]
	require.NotNil(t, first)
	first.insertedAt = time.Now().Add(time.Hour)
	o.leiosMu.Unlock()

	secondPoint, secondRaw := testLeiosEndorserBlockRaw(t, 2)
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			secondPoint,
			secondRaw,
			[]cbor.RawMessage{tx()},
		),
	)

	// Both entries together exceed the 300-byte aggregate budget, forcing one
	// eviction. Despite "first" appearing newest by insertedAt, it holds the
	// lower seq (it was actually inserted first) and must be the one evicted.
	_, firstStillCached := o.lookupLeiosEndorserBlock(firstPoint.Hash)
	_, secondStillCached := o.lookupLeiosEndorserBlock(secondPoint.Hash)
	require.False(t, firstStillCached)
	require.True(t, secondStillCached)
}

// Oversized case, partial-retention path: retainLeiosPartialTxs publishes
// merged partialTxs directly rather than through storeLeiosEndorserBlock, so
// it needs its own per-entry byte-budget check -- otherwise a peer dribbling
// enough small partial responses across repeated fetch attempts could grow an
// entry past the budget without ever going through that check.
func TestRetainLeiosPartialTxsRejectsMergeOverEntryByteBudget(t *testing.T) {
	withLowerLeiosEndorserBlockCacheBudgets(t, 1500, 1<<20) // 1.5 KiB / 1 MiB
	const txCount = 4
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 21, txCount)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))

	// A first partial, well under the budget, is retained normally.
	small := make([]cbor.RawMessage, txCount)
	small[0] = cbor.RawMessage(make([]byte, 500))
	o.retainLeiosPartialTxs(point.Hash, small, nil)
	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.Equal(t, 1, data.partialTxCount())

	// A second partial that would push the merged entry over the per-entry
	// byte budget is rejected -- the existing (smaller) partial survives.
	big := make([]cbor.RawMessage, txCount)
	big[1] = cbor.RawMessage(make([]byte, 5000))
	o.retainLeiosPartialTxs(point.Hash, big, nil)

	data, ok = o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.Equal(t, 1, data.partialTxCount())
	require.LessOrEqual(t, data.approxBytes(), 1500)
}
