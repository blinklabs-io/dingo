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
		o.recordLeiosAnnouncement(
			headerRaw,
			ebHash,
			ebSize,
			header,
			"test",
			false,
		),
	)
}

// announceTestEndorserBlock records the announcement binding ebHash to slot.
func announceTestEndorserBlock(
	t *testing.T,
	o *Ouroboros,
	slot uint64,
	ebHash lcommon.Blake2b256,
	ebSize int,
) {
	t.Helper()
	recordTestLeiosAnnouncement(
		t,
		o,
		testDijkstraAnnouncementHeaderRawFor(t, slot, ebHash, uint64(ebSize)),
	)
}

func testEbHash(point ocommon.Point) lcommon.Blake2b256 {
	var ebHash lcommon.Blake2b256
	copy(ebHash[:], point.Hash)
	return ebHash
}

// TestStoreLeiosEndorserBlockRejectsPointConflictingWithAnnouncement covers
// issue #3513: an endorser-block entry must be bound to the point its
// announcement vouched for, not accepted on a first-writer-wins basis from
// whichever connection offers it first.
func TestStoreLeiosEndorserBlockRejectsPointConflictingWithAnnouncement(
	t *testing.T,
) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 7)

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	announceTestEndorserBlock(
		t,
		o,
		point.Slot,
		testEbHash(point),
		len(blockRaw),
	)

	// A connection offering the real, correctly-hashed body for this
	// endorser-block hash, but at a slot other than the one its announcement
	// declared, must be rejected -- this is the very first store attempted
	// for the hash, so nothing but the announcement can catch the conflict.
	conflicting := ocommon.Point{Slot: point.Slot + 1, Hash: point.Hash}
	err := o.storeLeiosEndorserBlock(
		conflicting,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	)
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

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	announceTestEndorserBlock(
		t,
		o,
		point.Slot,
		testEbHash(point),
		len(blockRaw),
	)

	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))
	// Simulates a second connection re-offering the identical, correctly
	// bound endorser block: retransmission of a valid entry must not error.
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))

	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.Equal(t, point.Slot, data.point.Slot)
	require.True(t, data.slotVerified)
}

// TestStoreLeiosEndorserBlockCrossConnectionConflictIsRejectedRegardlessOfOrder
// covers cross-connection conflicts: whichever connection's offer is stored
// first, a later offer for the same hash at a different slot is rejected,
// and the originally-bound entry is unharmed.
func TestStoreLeiosEndorserBlockCrossConnectionConflictIsRejectedRegardlessOfOrder(
	t *testing.T,
) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 13)
	conflicting := ocommon.Point{Slot: point.Slot + 5, Hash: point.Hash}

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	announceTestEndorserBlock(
		t,
		o,
		point.Slot,
		testEbHash(point),
		len(blockRaw),
	)

	// Connection A stores the correctly-bound entry first.
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))
	// Connection B later offers the same hash at a conflicting slot.
	err := o.storeLeiosEndorserBlock(
		conflicting,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	)
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

// TestPeerOfferedStoreWithheldUntilAnnouncementBindsIt is the reverse-order
// regression: the relay -- and dingo's own forge path -- queue the block offer
// before the ranking-block announcement, so an authentic manifest is routinely
// stored while no announcement exists yet. Nothing keyed on the peer-supplied
// slot may be published until an announcement corroborates it.
func TestPeerOfferedStoreWithheldUntilAnnouncementBindsIt(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 41)

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	votes := &fakeLeiosVoteHandler{}
	o.leiosVotes = votes

	// Offer arrives first, before any announcement.
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))
	require.Empty(
		t,
		votes.ebs,
		"an unverified peer-supplied slot must not drive vote emission",
	)
	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok, "the block is still cached so its txs can be fetched")
	require.False(t, data.slotVerified)

	// The matching announcement then binds it and releases publication.
	announceTestEndorserBlock(
		t,
		o,
		point.Slot,
		testEbHash(point),
		len(blockRaw),
	)
	data, ok = o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.True(t, data.slotVerified)
	require.Len(t, votes.ebs, 1)
	require.Equal(t, point.Slot, votes.ebs[0].slot)
}

// TestPeerOfferedStoreUnderFabricatedSlotIsEvictedByAnnouncement is the core
// issue #3513 attack in its store-first ordering: a peer offers an authentic,
// correctly-hashed manifest under a slot of its choosing before the genuine
// announcement arrives. The fabricated slot must never be voted on, and the
// poisoned entry must not survive the announcement that contradicts it.
func TestPeerOfferedStoreUnderFabricatedSlotIsEvictedByAnnouncement(
	t *testing.T,
) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 41)
	fabricated := ocommon.Point{Slot: 42, Hash: point.Hash}

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	votes := &fakeLeiosVoteHandler{}
	o.leiosVotes = votes

	require.NoError(t, o.storeLeiosEndorserBlock(
		fabricated,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))
	require.Empty(
		t,
		votes.ebs,
		"a fabricated slot must not reach the vote handler",
	)

	// The genuine announcement for slot 41 contradicts the cached entry.
	announceTestEndorserBlock(
		t,
		o,
		point.Slot,
		testEbHash(point),
		len(blockRaw),
	)

	if data, ok := o.lookupLeiosEndorserBlock(point.Hash); ok {
		require.NotEqual(
			t,
			fabricated.Slot,
			data.point.Slot,
			"the entry bound to the fabricated slot must not survive",
		)
	}
	require.Empty(t, votes.ebs)

	// The ledger must not be handed the fabricated slot either.
	_, _, provOk := o.EndorserBlockTxsByHash(point.Hash)
	require.False(t, provOk)
}

// TestEndorserBlockTxsByHashWithholdsUnverifiedSlotFromLedger guards the
// ledger-facing consumer directly: it keys the endorser blob it persists on
// this slot, so a complete-but-unbound entry must read as unavailable.
func TestEndorserBlockTxsByHashWithholdsUnverifiedSlotFromLedger(
	t *testing.T,
) {
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 21, 1)
	txsRaw := []cbor.RawMessage{mustCbor(t, "tx0")}

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		txsRaw,
		leiosStorePeerOffered,
	))

	data, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.True(
		t,
		data.completeTxCache(),
		"the transaction set is whole; only the slot binding is missing",
	)
	_, _, provOk := o.EndorserBlockTxsByHash(point.Hash)
	require.False(
		t,
		provOk,
		"a complete but unverified entry must not reach the ledger",
	)

	announceTestEndorserBlock(
		t,
		o,
		point.Slot,
		testEbHash(point),
		len(blockRaw),
	)
	slot, _, provOk := o.EndorserBlockTxsByHash(point.Hash)
	require.True(t, provOk)
	require.Equal(t, point.Slot, slot)
}
