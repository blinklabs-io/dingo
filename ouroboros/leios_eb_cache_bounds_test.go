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
