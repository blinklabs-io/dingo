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
	"testing"
	"time"

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

// TestEndorserBlockTxsByHashAvailableAfterDBReload is the store -> drain ->
// clear-memory -> provider regression from review: a verified, persisted
// endorser block reloaded from the blob store after the in-memory cache is
// cleared must remain available to the ledger provider immediately, not
// only after some later event happens to re-verify it. loadLeiosEBFromDB
// must reconstruct the reload as already bound, since the blob store is
// only ever written from a verified entry in the first place.
func TestEndorserBlockTxsByHashAvailableAfterDBReload(t *testing.T) {
	tx0, ref0 := testLeiosManifestTx(t, 0)
	blockRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref0},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(55, lcommon.Blake2b256Hash(blockRaw).Bytes())
	txsRaw := []cbor.RawMessage{tx0}

	o := newTestOuroborosWithLeiosDB(t)
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		txsRaw,
		leiosStoreAuthoritative,
	))

	// Endorser-block persistence is asynchronous; drain the writer so the
	// blob store reflects the stored block before forcing a DB reload.
	o.StopLeiosPersistWriter()
	o.leiosMu.Lock()
	o.leiosEndorserBlocks = make(map[string]*leiosEndorserBlockData)
	o.leiosMu.Unlock()

	slot, gotTxs, ok := o.EndorserBlockTxsByHash(point.Hash)
	require.True(
		t,
		ok,
		"a reloaded, previously-verified entry must be immediately available",
	)
	require.Equal(t, point.Slot, slot)
	require.Equal(t, txsRaw, gotTxs)
}

// TestBindLeiosEndorserBlockSlotDoesNotMutateSharedEntry guards the copy-on-
// write invariant this file otherwise depends on throughout: lookupLeiosEndorserBlock
// hands out a pointer after leiosMu is released, and readers use it without
// the lock, so bindLeiosEndorserBlockSlot must publish a distinct copy on
// verification rather than flipping slotVerified on the pointer a caller
// already holds. Concurrent unlocked reads of that already-held pointer must
// see it unmodified.
func TestBindLeiosEndorserBlockSlotDoesNotMutateSharedEntry(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 63)

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))

	held, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.False(t, held.slotVerified)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 1000 {
			_ = held.slotVerified
		}
	}()
	go func() {
		defer wg.Done()
		o.bindLeiosEndorserBlockSlot(point.Hash, point.Slot)
	}()
	wg.Wait()

	require.False(
		t,
		held.slotVerified,
		"the pointer a caller already held must never be mutated",
	)
	fresh, ok := o.lookupLeiosEndorserBlock(point.Hash)
	require.True(t, ok)
	require.True(t, fresh.slotVerified, "a fresh lookup sees the published copy")
}

// TestStoreAndAnnouncementRaceAlwaysEndsVerified is the coordinated store /
// announcement regression from review: whichever of a peer-offered store and
// its matching announcement runs first, the entry must end up verified.
// recordLeiosAnnouncement's reconciliation runs at most once per distinct
// announcement, so if it can run before the store inserts its entry (seeing
// nothing to reconcile) while the store's own announcement check ran before
// the announcement was recorded (seeing nothing to bind to), the entry is
// stuck unverified forever. Run across many independent hashes concurrently
// under -race to exercise both interleavings.
func TestStoreAndAnnouncementRaceAlwaysEndsVerified(t *testing.T) {
	const n = 64
	o := newOuroboros(OuroborosConfig{EnableLeios: true})

	points := make([]ocommon.Point, n)
	blocks := make([][]byte, n)
	headers := make([][]byte, n)
	for i := range n {
		point, blockRaw := testLeiosEndorserBlockRaw(t, 100+i)
		points[i] = point
		blocks[i] = blockRaw
		headers[i] = testDijkstraAnnouncementHeaderRawFor(
			t,
			point.Slot,
			testEbHash(point),
			uint64(len(blockRaw)),
		)
	}

	var wg sync.WaitGroup
	wg.Add(2 * n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			_ = o.storeLeiosEndorserBlock(
				points[i],
				blocks[i],
				nil,
				leiosStorePeerOffered,
			)
		}(i)
		go func(i int) {
			defer wg.Done()
			recordTestLeiosAnnouncement(t, o, headers[i])
		}(i)
	}
	wg.Wait()

	for i := range n {
		data, ok := o.lookupLeiosEndorserBlock(points[i].Hash)
		require.True(t, ok)
		require.True(
			t,
			data.slotVerified,
			"entry %d must end up verified regardless of race order",
			i,
		)
	}
}

// TestLeiosAnnouncedSlotIgnoresExpiredBinding is the idle-expiry regression
// from review: leiosAnnouncementSlots is only actively pruned as a side
// effect of a *new* announcement being accepted (pruneLeiosAnnouncements), so
// on an otherwise-idle node a binding can sit long past the acceptance window
// pruneLeiosAnnouncements itself enforces. A lookup that does not also check
// that age would treat a stale, long-expired binding as still authoritative
// -- rejecting a legitimate later offer or announcement for the same hash as
// a conflict instead of leaving it merely unverified, the same as a hash
// with no binding at all.
func TestLeiosAnnouncedSlotIgnoresExpiredBinding(t *testing.T) {
	ledger := &fakeLeiosAnnouncementLedger{
		// SlotToTime always answers as if the binding's slot occurred long
		// enough ago to have aged out of leiosNotifyMaxAnnouncementAge.
		slotTime: time.Now().Add(-2 * leiosNotifyMaxAnnouncementAge),
	}
	o := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LeiosAnnouncementLedger: ledger,
	})

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 200, 1)
	ebHash := testEbHash(point)
	announceTestEndorserBlock(t, o, point.Slot, ebHash, len(blockRaw))

	slot, ok := o.leiosAnnouncedSlotLocked(point.Hash)
	require.False(
		t,
		ok,
		"an expired binding must read as unknown, not as a live conflict",
	)
	require.Zero(t, slot)

	// A later offer for the same hash at an unrelated slot must be accepted
	// (left unverified, pending its own announcement) rather than rejected
	// against the stale binding.
	later := ocommon.Point{Slot: point.Slot + 100_000, Hash: point.Hash}
	require.NoError(t, o.storeLeiosEndorserBlock(
		later,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))
}

// TestFetchEndorserBlockByPointRejectsStaleReloadedSlot is the P1 regression
// from the second review round: a hash persisted (and so already verified)
// under one slot must not be silently accepted as satisfying a later,
// authoritative request for the same hash at a different slot. The manifest
// is content-addressed, so the same hash can legitimately recur at a
// different slot; loadLeiosEBFromDB trusts a reload's persisted slot as
// verified for whatever occurrence wrote it, but FetchEndorserBlockByPoint
// must still compare that slot against the one it was actually asked about
// before treating the reload as already satisfying the request.
func TestFetchEndorserBlockByPointRejectsStaleReloadedSlot(t *testing.T) {
	tx0, ref0 := testLeiosManifestTx(t, 0)
	blockRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref0},
	}.MarshalCBOR()
	require.NoError(t, err)
	hash := lcommon.Blake2b256Hash(blockRaw).Bytes()
	staleSlot := uint64(41)
	authoritativeSlot := uint64(42)

	o := newTestOuroborosWithLeiosDB(t)
	require.NoError(t, o.storeLeiosEndorserBlock(
		ocommon.NewPoint(staleSlot, hash),
		blockRaw,
		[]cbor.RawMessage{tx0},
		leiosStoreAuthoritative,
	))

	// Endorser-block persistence is asynchronous; drain the writer so the
	// blob store reflects the stale-slot store, then clear the in-memory
	// cache so the next lookup must reload from the blob store.
	o.StopLeiosPersistWriter()
	o.leiosMu.Lock()
	o.leiosEndorserBlocks = make(map[string]*leiosEndorserBlockData)
	o.leiosMu.Unlock()

	// o.connManager is nil, so a genuine cache miss (or a correctly-rejected
	// stale reload) must return an error here, not silently succeed --
	// there is no way to actually fetch anything without a connection.
	err = o.FetchEndorserBlockByPoint(authoritativeSlot, hash)
	require.Error(
		t,
		err,
		"must not silently accept a reload bound to a different slot",
	)
	// Without a connection to actually re-fetch and re-persist, the blob
	// store's single-slot-per-hash record is unchanged, so a fully
	// independent EndorserBlockTxsByHash query may still report the stale
	// slot -- there is nothing here that could have corrected it. The
	// contract this test guards is narrower: FetchEndorserBlockByPoint
	// itself must not have claimed the authoritative slot was satisfied.

	// Once a real fetch *does* succeed (simulated here directly), the
	// authoritative store must override the stale entry rather than being
	// rejected by it, and the blob's single record for this hash is
	// corrected going forward.
	require.NoError(t, o.storeLeiosEndorserBlock(
		ocommon.NewPoint(authoritativeSlot, hash),
		blockRaw,
		[]cbor.RawMessage{tx0},
		leiosStoreAuthoritative,
	))
	slot, _, ok := o.EndorserBlockTxsByHash(hash)
	require.True(t, ok)
	require.Equal(t, authoritativeSlot, slot)
}
