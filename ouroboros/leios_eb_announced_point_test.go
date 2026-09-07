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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
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

// recordTestLeiosAnnouncementNoFail is recordTestLeiosAnnouncement's
// error-returning twin, for use from a worker goroutine: require's t.FailNow
// is documented as unsafe to call from any goroutine other than the one
// running the test function, so a concurrent caller must collect the error
// and assert on it back on the test goroutine instead (cubic review).
func recordTestLeiosAnnouncementNoFail(o *Ouroboros, headerRaw []byte) error {
	header, err := gdijkstra.NewDijkstraBlockHeaderFromCbor(headerRaw)
	if err != nil {
		return err
	}
	ebHash, ebSize, ok := header.LeiosAnnouncement()
	if !ok {
		return errors.New("header carries no leios announcement")
	}
	return o.recordLeiosAnnouncement(
		headerRaw,
		ebHash,
		ebSize,
		header,
		"test",
		false,
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

// TestStoreLeiosEndorserBlockAcceptsDifferentSlotOfSameHashWhileFirstIsLive
// is the wolf31o2 regression: the manifest is content-addressed, so the same
// hash can be a live, independently required occurrence at more than one
// slot at once (two elections producing an identical transaction-reference
// set), and both must be independently storable and verifiable through
// their own announcements. Rejecting the second occurrence just because a
// live announcement already exists for the hash at a different slot would
// drop that occurrence's offer/fetch and endorser data for whichever ranking
// block referenced it (wolf31o2 review; issue #3513).
func TestStoreLeiosEndorserBlockAcceptsDifferentSlotOfSameHashWhileFirstIsLive(
	t *testing.T,
) {
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 7, 1)
	second := ocommon.Point{Slot: point.Slot + 1, Hash: point.Hash}
	txsRaw := []cbor.RawMessage{mustCbor(t, "tx0")}

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	announceTestEndorserBlock(
		t,
		o,
		point.Slot,
		testEbHash(point),
		len(blockRaw),
	)
	// A second, independent announcement of the same hash at a different
	// slot, while the first is still live (no expiry involved at all).
	announceTestEndorserBlock(
		t,
		o,
		second.Slot,
		testEbHash(point),
		len(blockRaw),
	)

	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		txsRaw,
		leiosStorePeerOffered,
	))
	require.NoError(t, o.storeLeiosEndorserBlock(
		second,
		blockRaw,
		txsRaw,
		leiosStorePeerOffered,
	))

	firstData, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(
		t,
		firstData.slotVerified,
		"the first occurrence must bind to its own announcement",
	)

	secondData, ok := o.lookupLeiosEndorserBlock(second.Slot, second.Hash)
	require.True(t, ok)
	require.True(
		t,
		secondData.slotVerified,
		"the second occurrence must independently bind to its own announcement",
	)

	require.NotSame(
		t,
		firstData,
		secondData,
		"the two live occurrences must be tracked independently, not collapsed into one",
	)

	// Both occurrences must be independently available to the ledger
	// provider at once -- the concrete "offer/fetch and endorser data become
	// available" property wolf31o2's review asked for.
	_, ok = o.EndorserBlockTxsByHash(point.Hash, point.Slot)
	require.True(t, ok, "the first occurrence must reach the ledger")
	_, ok = o.EndorserBlockTxsByHash(second.Hash, second.Slot)
	require.True(t, ok, "the second occurrence must reach the ledger too")
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

	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.Equal(t, point.Slot, data.point.Slot)
	require.True(t, data.slotVerified)
}

// TestStoreLeiosEndorserBlockCrossConnectionDifferentSlotsCoexistRegardlessOfOrder
// covers cross-connection arrival order for two live occurrences of the same
// hash: whichever connection's offer is stored first, a later offer for the
// same hash at a different, independently announced slot is accepted as its
// own occurrence rather than rejected, and neither disturbs the other
// (wolf31o2 review; issue #3513). Table-driven over both arrival orders --
// cubic's review flagged that only testing point-then-second would leave a
// regression specific to the reverse order undetected.
func TestStoreLeiosEndorserBlockCrossConnectionDifferentSlotsCoexistRegardlessOfOrder(
	t *testing.T,
) {
	for _, secondFirst := range []bool{false, true} {
		name := "first-then-second"
		if secondFirst {
			name = "second-then-first"
		}
		t.Run(name, func(t *testing.T) {
			point, blockRaw := testLeiosEndorserBlockRaw(t, 13)
			second := ocommon.Point{Slot: point.Slot + 5, Hash: point.Hash}

			o := newOuroboros(OuroborosConfig{EnableLeios: true})
			announceTestEndorserBlock(
				t,
				o,
				point.Slot,
				testEbHash(point),
				len(blockRaw),
			)
			announceTestEndorserBlock(
				t,
				o,
				second.Slot,
				testEbHash(point),
				len(blockRaw),
			)

			store := func(p ocommon.Point) {
				require.NoError(t, o.storeLeiosEndorserBlock(
					p,
					blockRaw,
					nil,
					leiosStorePeerOffered,
				))
			}
			if secondFirst {
				store(second)
				store(point)
			} else {
				store(point)
				store(second)
			}

			data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
			require.True(t, ok)
			require.Equal(
				t,
				point.Slot,
				data.point.Slot,
				"the first occurrence must be unharmed by the second's arrival",
			)
			require.True(t, data.slotVerified)
			secondData, ok := o.lookupLeiosEndorserBlock(
				second.Slot,
				second.Hash,
			)
			require.True(t, ok)
			require.True(t, secondData.slotVerified)
		})
	}
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
	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
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
	data, ok = o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(t, data.slotVerified)
	require.Len(t, votes.ebs, 1)
	require.Equal(t, point.Slot, votes.ebs[0].slot)
}

// TestPeerOfferedStoreUnderFabricatedSlotStaysPermanentlyUnverified is the
// core issue #3513 attack in its store-first ordering: a peer offers an
// authentic, correctly-hashed manifest under a slot of its choosing before
// the genuine announcement arrives. The fabricated slot must never be voted
// on or reach the ledger. Unlike the pre-composite-key design, the genuine
// announcement for the real slot does not evict the fabricated entry -- they
// are now independent (slot, hash) occurrences -- so the fabricated entry
// simply sits cached but permanently unverified (until its own TTL prunes
// it), which is exactly as inert as if it had been evicted: nothing keyed on
// its slot is ever published.
func TestPeerOfferedStoreUnderFabricatedSlotStaysPermanentlyUnverified(
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

	// The genuine announcement is for a different slot; it has nothing to do
	// with the fabricated occurrence's own (slot, hash) key.
	announceTestEndorserBlock(
		t,
		o,
		point.Slot,
		testEbHash(point),
		len(blockRaw),
	)

	fabricatedData, ok := o.lookupLeiosEndorserBlock(
		fabricated.Slot,
		fabricated.Hash,
	)
	require.True(
		t,
		ok,
		"the fabricated entry is not evicted by an unrelated announcement",
	)
	require.False(
		t,
		fabricatedData.slotVerified,
		"the fabricated slot must never become verified",
	)
	require.Empty(t, votes.ebs)

	// The ledger must not be handed the fabricated slot either.
	_, provOk := o.EndorserBlockTxsByHash(fabricated.Hash, fabricated.Slot)
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

	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(
		t,
		data.completeTxCache(),
		"the transaction set is whole; only the slot binding is missing",
	)
	_, provOk := o.EndorserBlockTxsByHash(point.Hash, point.Slot)
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
	_, provOk = o.EndorserBlockTxsByHash(point.Hash, point.Slot)
	require.True(t, provOk)
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

	gotTxs, ok := o.EndorserBlockTxsByHash(point.Hash, point.Slot)
	require.True(
		t,
		ok,
		"a reloaded, previously-verified entry must be immediately available",
	)
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

	held, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
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
	fresh, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(
		t,
		fresh.slotVerified,
		"a fresh lookup sees the published copy",
	)
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

	// Collected here rather than asserted inside the goroutines below: require
	// (and t.FailNow, which it calls on failure) is documented as unsafe to
	// invoke from any goroutine other than the one running the test function
	// (cubic review).
	announceErrs := make([]error, n)
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
			announceErrs[i] = recordTestLeiosAnnouncementNoFail(o, headers[i])
		}(i)
	}
	wg.Wait()

	for i := range n {
		require.NoError(t, announceErrs[i], "announcement %d", i)
	}
	for i := range n {
		data, ok := o.lookupLeiosEndorserBlock(points[i].Slot, points[i].Hash)
		require.True(t, ok)
		require.True(
			t,
			data.slotVerified,
			"entry %d must end up verified regardless of race order",
			i,
		)
	}
}

// TestLeiosAnnouncementBindsSlotIgnoresExpiredBinding is the idle-expiry
// regression from review: leiosAnnouncementSlots is only actively pruned as a
// side effect of a *new* announcement being accepted (pruneLeiosAnnouncements),
// so on an otherwise-idle node a binding can sit long past the acceptance
// window pruneLeiosAnnouncements itself enforces.
// leiosAnnouncementBindsSlotLocked must not treat a stale, long-expired
// binding as still live -- a peer-offered store for that same slot must be
// left merely unverified, the same as a hash with no binding at all.
func TestLeiosAnnouncementBindsSlotIgnoresExpiredBinding(t *testing.T) {
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

	require.False(
		t,
		o.leiosAnnouncementBindsSlotLocked(point.Hash, point.Slot),
		"an expired binding must not verify a store for the same slot",
	)

	// A peer-offered store for that same, now-expired slot must be accepted
	// but left unverified rather than rejected -- the expired binding reads
	// as unknown, not as a live conflict.
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))
	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.False(t, data.slotVerified)
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
	err = o.FetchEndorserBlockByPoint(context.Background(), authoritativeSlot, hash)
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
	_, ok := o.EndorserBlockTxsByHash(hash, authoritativeSlot)
	require.True(t, ok)
}

// TestStoreLeiosEndorserBlockAuthoritativeAndAnnouncedOccurrencesCoexist
// covers a gap symmetric to the stale-cache-entry cases above: an
// authoritative occurrence of a hash at one slot, and a live announcement
// for the same hash at a different slot, are two independent, equally valid
// occurrences (the manifest is content-addressed) and neither blocks the
// other. A peer-offered store matching its own live announcement must
// succeed and coexist with the authoritative entry, not be rejected as if
// the authoritative source's slot were the hash's only valid one (issue
// #3513 review; wolf31o2 review).
func TestStoreLeiosEndorserBlockAuthoritativeAndAnnouncedOccurrencesCoexist(
	t *testing.T,
) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 300)
	ebHash := testEbHash(point)
	announced := ocommon.Point{Slot: point.Slot - 50, Hash: point.Hash}

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	// A live announcement binds this hash to an older, unrelated slot.
	announceTestEndorserBlock(t, o, announced.Slot, ebHash, len(blockRaw))

	// The ledger (or the local forge path) authoritatively establishes the
	// hash at a different slot; it is unaffected by the unrelated
	// announcement.
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		nil,
		leiosStoreAuthoritative,
	))

	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.Equal(t, point.Slot, data.point.Slot)
	require.True(t, data.slotVerified)

	// A peer-offered store matching its own live announcement must succeed
	// and become independently verified, coexisting with the authoritative
	// entry above rather than being rejected by it.
	require.NoError(t, o.storeLeiosEndorserBlock(
		announced,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))
	announcedData, ok := o.lookupLeiosEndorserBlock(
		announced.Slot,
		announced.Hash,
	)
	require.True(t, ok)
	require.True(t, announcedData.slotVerified)
}

// TestEndorserBlockTxHashesByHashWithholdsUnverifiedSlot is the second review
// round's comment 2 companion to
// TestEndorserBlockTxsByHashWithholdsUnverifiedSlotFromLedger:
// EndorserBlockTxHashesByHash feeds the forge loop's post-certificate mempool
// exclusion list, so a complete-but-unbound entry must read as unavailable
// there too, not just from the tx-body provider.
func TestEndorserBlockTxHashesByHashWithholdsUnverifiedSlot(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 22, 1)
	txsRaw := []cbor.RawMessage{mustCbor(t, "tx0")}

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		txsRaw,
		leiosStorePeerOffered,
	))

	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(t, data.completeTxCache())
	_, ok = o.EndorserBlockTxHashesByHash(point.Hash, point.Slot)
	require.False(
		t,
		ok,
		"a complete but unverified entry must not reach the forge loop",
	)

	announceTestEndorserBlock(
		t,
		o,
		point.Slot,
		testEbHash(point),
		len(blockRaw),
	)
	hashes, ok := o.EndorserBlockTxHashesByHash(point.Hash, point.Slot)
	require.True(t, ok)
	require.Len(t, hashes, 1)
}

// TestLeiosClosureCompleteLockedWithholdsUnverifiedEntry is the closure-wait
// half of the second review round's comment 2: a closure that is complete but
// not yet slot-verified must not report ready via
// leiosClosureCompleteLocked/waitForLeiosEndorserClosure, and a waiter
// registered on it must stay parked until bindLeiosEndorserBlockSlot
// corroborates the slot -- otherwise the node-to-client merge path (which
// waits on this same closure) could consume an unverified slot the same way
// EndorserBlockTxsByHash could before issue #3513.
func TestLeiosClosureCompleteLockedWithholdsUnverifiedEntry(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 71)

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		[]cbor.RawMessage{mustCbor(t, "tx0")},
		leiosStorePeerOffered,
	))
	data, ok := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.True(t, ok)
	require.True(t, data.completeTxCache())
	require.False(t, data.slotVerified)

	// The already-cached fast path must not report a complete-but-unverified
	// closure as ready.
	quickCtx, quickCancel := context.WithTimeout(
		context.Background(),
		200*time.Millisecond,
	)
	defer quickCancel()
	require.False(
		t,
		o.waitForLeiosEndorserClosure(quickCtx, point.Slot, point.Hash),
	)

	// A waiter registered while the entry is complete-but-unverified must
	// stay parked -- nothing signals it at store time -- until the slot is
	// corroborated.
	// The wait context is intentionally much longer than every assertion
	// below: a passing RequireReceive must be caused by the promotion's
	// explicit wakeup, not by this context happening to expire around the
	// same time.
	result := make(chan bool, 1)
	go func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second,
		)
		defer cancel()
		result <- o.waitForLeiosEndorserClosure(ctx, point.Slot, point.Hash)
	}()
	testutil.WaitForCondition(
		t,
		func() bool {
			o.leiosMu.RLock()
			defer o.leiosMu.RUnlock()
			return len(
				o.leiosClosureWaiters[leiosBlockKey(point.Slot, point.Hash)],
			) > 0
		},
		2*time.Second,
		"closure waiter to register",
	)
	testutil.RequireNoReceive(
		t,
		result,
		300*time.Millisecond,
		"a complete but unverified closure must not wake a waiter",
	)

	// bindLeiosEndorserBlockSlot corroborating the slot must wake the parked
	// waiter itself -- the store above never will, since the entry was
	// already complete before the binding arrived.
	o.bindLeiosEndorserBlockSlot(point.Hash, point.Slot)
	require.True(
		t,
		testutil.RequireReceive(
			t,
			result,
			500*time.Millisecond,
			"closure wait to resolve once the slot is verified",
		),
	)
}

// lockProbingVoteHandler's HandleEndorserBlock acquires leiosAnnouncementsMu
// itself before delegating, the way a real handler could legitimately need
// to (e.g. to cross-check announcement state). If bindLeiosEndorserBlockSlot's
// publish step still ran while recordLeiosAnnouncement held that same lock,
// this self-deadlocks instead of merely looking suspicious.
type lockProbingVoteHandler struct {
	*fakeLeiosVoteHandler
	o *Ouroboros
}

func (l *lockProbingVoteHandler) HandleEndorserBlock(
	slot uint64,
	ebHash lcommon.Blake2b256,
) {
	l.o.leiosAnnouncementsMu.Lock()
	l.o.leiosAnnouncementsMu.Unlock()
	l.fakeLeiosVoteHandler.HandleEndorserBlock(slot, ebHash)
}

// TestRecordLeiosAnnouncementPublishesAfterReleasingAnnouncementsLock is the
// regression for the cubic P2 finding: bindLeiosEndorserBlockSlot's promotion
// used to publish (vote emission, pipeline observation, persistence enqueue)
// while recordLeiosAnnouncement still held leiosAnnouncementsMu, a lock
// shared by every concurrent announcement. A vote handler that itself needs
// that lock would then deadlock. The goroutine here is bounded by a timeout
// so a regression shows up as a clean test failure rather than a hung test
// binary; recordTestLeiosAnnouncementNoFail (not recordTestLeiosAnnouncement)
// keeps require calls off that goroutine (cubic review).
func TestRecordLeiosAnnouncementPublishesAfterReleasingAnnouncementsLock(
	t *testing.T,
) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 250)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	votes := &lockProbingVoteHandler{
		fakeLeiosVoteHandler: &fakeLeiosVoteHandler{},
		o:                    o,
	}
	o.leiosVotes = votes

	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		nil,
		leiosStorePeerOffered,
	))

	headerRaw := testDijkstraAnnouncementHeaderRawFor(
		t,
		point.Slot,
		testEbHash(point),
		uint64(len(blockRaw)),
	)
	errCh := make(chan error, 1)
	go func() {
		errCh <- recordTestLeiosAnnouncementNoFail(o, headerRaw)
	}()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal(
			"recordLeiosAnnouncement deadlocked: publish must run after " +
				"releasing leiosAnnouncementsMu",
		)
	}
	require.Len(t, votes.ebs, 1)
}
