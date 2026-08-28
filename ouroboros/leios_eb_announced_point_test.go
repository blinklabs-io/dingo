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
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// testDijkstraAnnouncementHeaderRawFor builds a ranking-block header
// announcing ebHash/ebSize at the given slot, mirroring
// testDijkstraAnnouncementHeaderRaw but for a caller-supplied endorser-block
// identity so tests can bind an announcement to a real, independently
// computed endorser-block hash.
func testDijkstraAnnouncementHeaderRawFor(
	t *testing.T,
	slot uint64,
	ebHash lcommon.Blake2b256,
	ebSize uint64,
) []byte {
	t.Helper()
	_, blockRaw := testDijkstraBlockRaw(t, int(slot))
	var components []cbor.RawMessage
	_, err := cbor.Decode(blockRaw, &components)
	require.NoError(t, err)
	require.Len(t, components, 2)

	var headerTop []cbor.RawMessage
	_, err = cbor.Decode(components[0], &headerTop)
	require.NoError(t, err)
	require.Len(t, headerTop, 2)
	var headerBody []cbor.RawMessage
	_, err = cbor.Decode(headerTop[0], &headerBody)
	require.NoError(t, err)
	headerBody = append(
		headerBody,
		mustCbor(t, false),
		mustCbor(t, []any{ebHash.Bytes(), ebSize}),
	)
	headerTop[0], err = cbor.Encode(headerBody)
	require.NoError(t, err)
	headerRaw, err := cbor.Encode(headerTop)
	require.NoError(t, err)
	return headerRaw
}

// recordTestLeiosAnnouncement decodes headerRaw and records it as an
// announcement, mirroring the `record` helper in
// TestLeiosNotifyBlockAnnouncementIsConsumedAndDeduplicated.
func recordTestLeiosAnnouncement(
	t *testing.T,
	o *Ouroboros,
	headerRaw []byte,
) {
	t.Helper()
	header, err := gdijkstra.NewDijkstraBlockHeaderFromCbor(headerRaw)
	require.NoError(t, err)
	ebHash, ebSize, ok := header.LeiosAnnouncement()
	require.True(t, ok)
	require.NoError(
		t,
		o.recordLeiosAnnouncement(headerRaw, ebHash, ebSize, header, "test", false),
	)
}

// TestStoreLeiosEndorserBlockRejectsPointConflictingWithAnnouncement covers
// issue #3513: an endorser-block entry must be bound to the point its
// announcement vouched for, not accepted on a first-writer-wins basis from
// whichever connection offers it first.
func TestStoreLeiosEndorserBlockRejectsPointConflictingWithAnnouncement(
	t *testing.T,
) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 7)
	var ebHash lcommon.Blake2b256
	copy(ebHash[:], point.Hash)

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	recordTestLeiosAnnouncement(
		t,
		o,
		testDijkstraAnnouncementHeaderRawFor(
			t,
			point.Slot,
			ebHash,
			uint64(len(blockRaw)),
		),
	)

	// A connection offering the real, correctly-hashed body for this
	// endorser-block hash, but at a slot other than the one its announcement
	// declared, must be rejected -- this is the very first store attempted
	// for the hash, so nothing but the announcement can catch the conflict.
	conflicting := ocommon.Point{Slot: point.Slot + 1, Hash: point.Hash}
	err := o.storeLeiosEndorserBlock(conflicting, blockRaw, nil)
	require.ErrorContains(t, err, "does not match announced point")
	_, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.False(t, ok, "a rejected store must not poison the cache")
}

// TestStoreLeiosEndorserBlockAcceptsAnnouncedPointAndIsIdempotent covers the
// companion acceptance criteria: a store matching the announced point
// succeeds, and retransmitting the identical store (as every connection
// offering the block does) remains idempotent.
func TestStoreLeiosEndorserBlockAcceptsAnnouncedPointAndIsIdempotent(
	t *testing.T,
) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 11)
	var ebHash lcommon.Blake2b256
	copy(ebHash[:], point.Hash)

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	recordTestLeiosAnnouncement(
		t,
		o,
		testDijkstraAnnouncementHeaderRawFor(
			t,
			point.Slot,
			ebHash,
			uint64(len(blockRaw)),
		),
	)

	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))
	// Simulates a second connection re-offering the identical, correctly
	// bound endorser block: retransmission of a valid entry must not error.
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))

	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.Equal(t, point.Slot, data.point.Slot)
}

// TestStoreLeiosEndorserBlockCrossConnectionConflictIsRejectedRegardlessOfOrder
// covers cross-connection conflicts: whichever connection's offer is stored
// first, a later offer for the same hash at a different slot is rejected,
// and the originally-bound entry is unharmed.
func TestStoreLeiosEndorserBlockCrossConnectionConflictIsRejectedRegardlessOfOrder(
	t *testing.T,
) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 13)
	var ebHash lcommon.Blake2b256
	copy(ebHash[:], point.Hash)
	conflicting := ocommon.Point{Slot: point.Slot + 5, Hash: point.Hash}

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	recordTestLeiosAnnouncement(
		t,
		o,
		testDijkstraAnnouncementHeaderRawFor(
			t,
			point.Slot,
			ebHash,
			uint64(len(blockRaw)),
		),
	)

	// Connection A stores the correctly-bound entry first.
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))
	// Connection B later offers the same hash at a conflicting slot.
	err := o.storeLeiosEndorserBlock(conflicting, blockRaw, nil)
	require.Error(t, err)

	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.Equal(
		t,
		point.Slot,
		data.point.Slot,
		"the conflicting later offer must not overwrite the bound entry",
	)
}
