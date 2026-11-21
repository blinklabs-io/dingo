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
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/ledger"
	gouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	"github.com/stretchr/testify/require"
)

const testLeiosOfferAnnouncedSize = 1234

// leiosOfferNode is an Ouroboros with two LeiosNotify peers whose
// announcements and offers are delivered through the production handler.
type leiosOfferNode struct {
	t              *testing.T
	o              *Ouroboros
	ledger         *fakeLeiosAnnouncementLedger
	first, second  *gouroboros.Connection
	firstDone      chan any
	secondDone     chan any
	immutableSlot  atomic.Uint64
	immutableFound atomic.Bool
	immutableErr   atomic.Bool
}

func newLeiosOfferNode(t *testing.T) *leiosOfferNode {
	t.Helper()
	o, first, second := newHeldManifestOfferPeers(t)
	n := &leiosOfferNode{
		t:          t,
		o:          o,
		first:      first,
		second:     second,
		firstDone:  make(chan any),
		secondDone: make(chan any),
		ledger: &fakeLeiosAnnouncementLedger{
			currentSlot: 10_000,
			slotTime:    time.Now().Add(-time.Minute),
			staleness:   ledger.LeiosAnnouncementFreshOCIN,
		},
	}
	o.leiosAnnouncementLedger = n.ledger
	o.leiosImmutableSlot = func(context.Context) (uint64, bool, error) {
		if n.immutableErr.Load() {
			return 0, false, errors.New("immutable point unavailable")
		}
		return n.immutableSlot.Load(), n.immutableFound.Load(), nil
	}
	t.Cleanup(func() {
		close(n.firstDone)
		close(n.secondDone)
	})
	return n
}

func (n *leiosOfferNode) ctx(
	conn *gouroboros.Connection,
) oleiosnotify.CallbackContext {
	done := n.firstDone
	if conn == n.second {
		done = n.secondDone
	}
	return oleiosnotify.CallbackContext{
		ConnectionId:       conn.Id(),
		ConnectionDoneChan: done,
	}
}

func leiosOfferPoint(slot uint64) ocommon.Point {
	return ocommon.NewPoint(slot, []byte(fmt.Sprintf("%032d", slot)))
}

func (n *leiosOfferNode) announcement(
	point ocommon.Point,
) *oleiosnotify.MsgBlockAnnouncement {
	n.t.Helper()
	return oleiosnotify.NewMsgBlockAnnouncement(
		uint(gdijkstra.EraIdDijkstra),
		testDijkstraAnnouncementHeaderRawFor(
			n.t,
			point.Slot,
			lcommon.NewBlake2b256(point.Hash),
			testLeiosOfferAnnouncedSize,
		),
	)
}

func (n *leiosOfferNode) announce(
	conn *gouroboros.Connection,
	point ocommon.Point,
) {
	n.t.Helper()
	require.NoError(
		n.t,
		n.o.leiosnotifyClientNotification(n.ctx(conn), n.announcement(point)),
	)
}

func (n *leiosOfferNode) offerBody(
	conn *gouroboros.Connection,
	point ocommon.Point,
	size uint64,
) error {
	return n.o.leiosnotifyClientNotification(
		n.ctx(conn), oleiosnotify.NewMsgBlockOffer(point, size),
	)
}

func (n *leiosOfferNode) offerClosure(
	conn *gouroboros.Connection,
	point ocommon.Point,
) error {
	return n.o.leiosnotifyClientNotification(
		n.ctx(conn), oleiosnotify.NewMsgBlockTxsOffer(point),
	)
}

// requireNothingCommitted checks that a refused offer left no trace in the
// shared structures an accepted offer would have written to.
func (n *leiosOfferNode) requireNothingCommitted() {
	n.t.Helper()
	require.Zero(n.t, pendingLeiosOfferCount(n.o), "offer remembered")
	n.o.leiosBackfillSourcesMu.Lock()
	sources := len(n.o.leiosBackfillSources)
	n.o.leiosBackfillSourcesMu.Unlock()
	require.Zero(n.t, sources, "backfill source recorded")
	n.o.leiosMu.RLock()
	cached := len(n.o.leiosEndorserBlocks)
	n.o.leiosMu.RUnlock()
	require.Zero(n.t, cached, "cache intent recorded")
	require.Zero(
		n.t,
		n.o.leiosFetchGuardFor(n.first.Id()).inflight.Load(),
		"fetch started",
	)
}

func TestLeiosOfferWithoutOwnAnnouncementEndsConnection(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"body", "closure"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			n := newLeiosOfferNode(t)
			point := leiosOfferPoint(200)
			// The other peer's announcement makes the election observed
			// node-wide; it is still not this peer's announcement.
			n.announce(n.second, point)
			require.True(t, n.o.leiosElectionObserved(point))

			var err error
			if kind == "body" {
				err = n.offerBody(n.first, point, 100)
			} else {
				err = n.offerClosure(n.first, point)
			}
			require.ErrorIs(t, err, errLeiosOfferUnannounced)
			require.ErrorIs(t, err, errLeiosInvalidOffer)
			n.requireNothingCommitted()

			// The peer that did announce is not affected.
			if kind == "body" {
				require.NoError(t, n.offerBody(n.second, point, 100))
			} else {
				require.NoError(t, n.offerClosure(n.second, point))
			}
		})
	}
}

func TestLeiosOfferWithoutAnnouncementAtThatHashEndsConnection(t *testing.T) {
	t.Parallel()
	n := newLeiosOfferNode(t)
	announced := leiosOfferPoint(210)
	n.announce(n.first, announced)
	otherHash := ocommon.NewPoint(announced.Slot, leiosOfferPoint(211).Hash)
	otherSlot := ocommon.NewPoint(announced.Slot+1, announced.Hash)
	for _, point := range []ocommon.Point{otherHash, otherSlot} {
		require.ErrorIs(
			t, n.offerBody(n.first, point, 100), errLeiosOfferUnannounced,
		)
		require.ErrorIs(
			t, n.offerClosure(n.first, point), errLeiosOfferUnannounced,
		)
	}
	require.NoError(t, n.offerBody(n.first, announced, 100))
}

func TestLeiosOffersAfterOwnAnnouncementAreAccepted(t *testing.T) {
	t.Parallel()
	n := newLeiosOfferNode(t)
	bodyFirst := leiosOfferPoint(220)
	closureFirst := leiosOfferPoint(221)
	n.announce(n.first, bodyFirst)
	n.announce(n.first, closureFirst)
	require.NoError(t, n.offerBody(n.first, bodyFirst, 100))
	require.NoError(t, n.offerClosure(n.first, bodyFirst))
	require.NoError(t, n.offerClosure(n.first, closureFirst))
	require.NoError(t, n.offerBody(n.first, closureFirst, 100))
}

func TestLeiosRepeatedOfferEndsConnection(t *testing.T) {
	t.Parallel()
	n := newLeiosOfferNode(t)
	point := leiosOfferPoint(230)
	otherSlot := ocommon.NewPoint(point.Slot+1, point.Hash)
	n.announce(n.first, point)
	n.announce(n.first, otherSlot)

	require.NoError(t, n.offerBody(n.first, point, 100))
	err := n.offerBody(n.first, point, 100)
	require.ErrorIs(t, err, errLeiosOfferRepeated)
	require.ErrorIs(t, err, errLeiosInvalidOffer)
	// The point is the key: the same hash at another slot is its own offer,
	// and the closure is independent of the body.
	require.NoError(t, n.offerBody(n.first, otherSlot, 100))
	require.NoError(t, n.offerClosure(n.first, point))
	require.ErrorIs(
		t, n.offerClosure(n.first, point), errLeiosOfferRepeated,
	)
	require.ErrorIs(
		t, n.offerBody(n.first, otherSlot, 100), errLeiosOfferRepeated,
	)
}

// A repeat is reported as a repeat, ahead of the size and announcement rules
// that the same message would also break.
func TestLeiosRepeatedOfferIsCheckedBeforeSize(t *testing.T) {
	t.Parallel()
	n := newLeiosOfferNode(t)
	point := leiosOfferPoint(231)
	n.announce(n.first, point)
	require.NoError(t, n.offerBody(n.first, point, 100))
	err := n.offerBody(n.first, point, 0)
	require.ErrorIs(t, err, errLeiosOfferRepeated)
	require.NotErrorIs(t, err, errLeiosBlockOfferSize)
}

// An offer is recorded when it is accepted, whether or not the node then acts
// on it: one the host budget drops still counts, so repeating it is refused.
func TestLeiosOfferDroppedByBudgetStillCountsAsOffered(t *testing.T) {
	t.Parallel()
	n := newLeiosOfferNode(t)
	point := leiosOfferPoint(232)
	n.announce(n.first, point)
	key := leiosOfferBudgetKey(n.first.Id())
	for range leiosOfferMaxPerPeer {
		n.o.admitLeiosOffer(key)
	}
	require.False(t, n.o.admitLeiosOffer(key))
	require.NoError(t, n.offerBody(n.first, point, 100))
	n.requireNothingCommitted()
	require.ErrorIs(
		t, n.offerBody(n.first, point, 100), errLeiosOfferRepeated,
	)
}

func TestLeiosOfferBelowImmutableSlotEndsConnection(t *testing.T) {
	t.Parallel()
	n := newLeiosOfferNode(t)
	const immutable = 300
	points := map[uint64]ocommon.Point{}
	for _, slot := range []uint64{immutable - 1, immutable, immutable + 1} {
		points[slot] = leiosOfferPoint(slot)
		n.announce(n.first, points[slot])
	}
	n.immutableSlot.Store(immutable)
	n.immutableFound.Store(true)

	below := points[immutable-1]
	for _, err := range []error{
		n.offerBody(n.first, below, 100),
		n.offerClosure(n.first, below),
	} {
		require.ErrorIs(t, err, errLeiosOfferTooOld)
		require.ErrorIs(t, err, errLeiosInvalidOffer)
	}
	n.requireNothingCommitted()
	// The immutable slot itself and anything above it is current.
	for _, slot := range []uint64{immutable, immutable + 1} {
		require.NoError(t, n.offerBody(n.first, points[slot], 100))
		require.NoError(t, n.offerClosure(n.first, points[slot]))
	}
}

// Too old is checked first: an offer below the immutable slot is reported as
// that whatever else is wrong with it.
func TestLeiosOfferTooOldIsCheckedBeforeOtherRules(t *testing.T) {
	t.Parallel()
	n := newLeiosOfferNode(t)
	n.immutableSlot.Store(400)
	n.immutableFound.Store(true)
	point := leiosOfferPoint(399)
	require.ErrorIs(t, n.offerBody(n.first, point, 0), errLeiosOfferTooOld)
	require.ErrorIs(t, n.offerClosure(n.first, point), errLeiosOfferTooOld)
}

func TestLeiosOfferWithoutKnownImmutableSlotIsNotTooOld(t *testing.T) {
	t.Parallel()
	n := newLeiosOfferNode(t)
	early := leiosOfferPoint(1)
	other := leiosOfferPoint(2)
	n.announce(n.first, early)
	n.announce(n.first, other)
	// No immutable point yet.
	require.NoError(t, n.offerBody(n.first, early, 100))
	// An unresolvable one prunes nothing either.
	n.immutableErr.Store(true)
	require.NoError(t, n.offerBody(n.first, other, 100))
}

func TestLeiosBlockOfferSizeBounds(t *testing.T) {
	t.Parallel()
	n := newLeiosOfferNode(t)
	for idx, size := range []uint64{
		0,
		leiosMaxEndorserBlockBytes + 1,
		1 << 40,
	} {
		point := leiosOfferPoint(uint64(500 + idx))
		n.announce(n.first, point)
		err := n.offerBody(n.first, point, size)
		require.ErrorIs(t, err, errLeiosBlockOfferSize, "size %d", size)
		require.ErrorIs(t, err, errLeiosInvalidOffer)
	}
	n.requireNothingCommitted()
	for idx, size := range []uint64{1, leiosMaxEndorserBlockBytes} {
		point := leiosOfferPoint(uint64(600 + idx))
		n.announce(n.first, point)
		require.NoError(t, n.offerBody(n.first, point, size), "size %d", size)
	}
	// The closure offer carries no size.
	point := leiosOfferPoint(610)
	n.announce(n.first, point)
	require.NoError(t, n.offerClosure(n.first, point))
}

// The size is checked before the announcement, as the reference does: a peer
// that is wrong on both is reported for the size.
func TestLeiosBlockOfferSizeIsCheckedBeforeAnnouncement(t *testing.T) {
	t.Parallel()
	n := newLeiosOfferNode(t)
	err := n.offerBody(n.first, leiosOfferPoint(620), 0)
	require.ErrorIs(t, err, errLeiosBlockOfferSize)
	require.NotErrorIs(t, err, errLeiosOfferUnannounced)
}

// A refused offer ends only the connection that sent it: the other peer's
// state is untouched, and a peer that reconnects starts with nothing.
func TestLeiosInvalidOfferAffectsOnlyOffendingConnection(t *testing.T) {
	t.Parallel()
	n := newLeiosOfferNode(t)
	point := leiosOfferPoint(700)
	n.announce(n.first, point)
	n.announce(n.second, point)
	require.NoError(t, n.offerBody(n.second, point, 100))

	require.NoError(t, n.offerBody(n.first, point, 100))
	require.ErrorIs(
		t, n.offerBody(n.first, point, 100), errLeiosOfferRepeated,
	)
	// The other connection keeps its announcements and its own offers: its
	// closure is still acceptable, its body is still a repeat.
	require.NoError(t, n.offerClosure(n.second, point))
	require.ErrorIs(
		t, n.offerBody(n.second, point, 100), errLeiosOfferRepeated,
	)

	// The same connection identity with a new lifetime is a new peer.
	reconnected := oleiosnotify.CallbackContext{
		ConnectionId:       n.first.Id(),
		ConnectionDoneChan: make(chan any),
	}
	err := n.o.leiosnotifyClientNotification(
		reconnected, oleiosnotify.NewMsgBlockOffer(point, 100),
	)
	require.ErrorIs(t, err, errLeiosOfferUnannounced)
}

// Announcements this node could not judge for a local reason still count as
// the peer's, so the peer's next offer is not mistaken for an unannounced one;
// one found invalid, and one this node made itself, do not.
func TestLeiosPeerAnnouncementAccounting(t *testing.T) {
	t.Parallel()
	probe := newTestOuroborosWithLeiosDB(t)
	header, err := decodeLeiosAnnouncementHeader(
		testDijkstraAnnouncementHeaderRaw(t),
	)
	require.NoError(t, err)
	deferredErr := probe.ledgerState.ValidateChainSelectionHeaderCrypto(header)
	require.True(t, ledger.IsHeaderVerificationDeferred(deferredErr))

	newNode := func() (*Ouroboros, *fakeLeiosAnnouncementLedger) {
		fake := &fakeLeiosAnnouncementLedger{
			currentSlot: 10_000,
			slotTime:    time.Now().Add(-time.Minute),
			staleness:   ledger.LeiosAnnouncementFreshOCIN,
		}
		return newOuroboros(OuroborosConfig{
			EnableLeios:             true,
			LeiosAnnouncementLedger: fake,
		}), fake
	}
	headerFor := func(point ocommon.Point) []byte {
		return testDijkstraAnnouncementHeaderRawFor(
			t,
			point.Slot,
			lcommon.NewBlake2b256(point.Hash),
			testLeiosOfferAnnouncedSize,
		)
	}
	offered := func(o *Ouroboros, source string, done <-chan any, point ocommon.Point) error {
		return o.checkLeiosPeerOffer(source, done, point, 100, false)
	}

	t.Run("already known from another peer", func(t *testing.T) {
		t.Parallel()
		o, _ := newNode()
		point := leiosOfferPoint(800)
		doneA, doneB := make(chan any), make(chan any)
		require.NoError(
			t,
			o.acceptLeiosAnnouncementInternal(
				headerFor(point),
				"a",
				true,
				doneA,
			),
		)
		require.NoError(
			t,
			o.acceptLeiosAnnouncementInternal(
				headerFor(point),
				"b",
				true,
				doneB,
			),
		)
		require.NoError(t, offered(o, "b", doneB, point))
	})
	t.Run("deferred verification", func(t *testing.T) {
		t.Parallel()
		o, fake := newNode()
		fake.err = deferredErr
		point := leiosOfferPoint(801)
		done := make(chan any)
		require.Error(
			t,
			o.acceptLeiosAnnouncementInternal(
				headerFor(point),
				"a",
				true,
				done,
			),
		)
		require.NoError(t, offered(o, "a", done, point))
	})
	t.Run("invalid", func(t *testing.T) {
		t.Parallel()
		o, fake := newNode()
		fake.err = errors.New("invalid header")
		point := leiosOfferPoint(802)
		done := make(chan any)
		require.Error(
			t,
			o.acceptLeiosAnnouncementInternal(
				headerFor(point),
				"a",
				true,
				done,
			),
		)
		require.ErrorIs(
			t,
			offered(o, "a", done, point),
			errLeiosOfferUnannounced,
		)
	})
	t.Run("local", func(t *testing.T) {
		t.Parallel()
		o, _ := newNode()
		point := leiosOfferPoint(803)
		require.NoError(
			t,
			o.acceptLeiosAnnouncement(
				headerFor(point),
				leiosLocalAnnouncementSource,
			),
		)
		require.ErrorIs(
			t,
			offered(o, leiosLocalAnnouncementSource, nil, point),
			errLeiosOfferUnannounced,
		)
	})
}

// An offer remembered while its election is unobserved was checked when it
// arrived; releasing it must not count it a second time.
func TestLeiosRememberedOfferIsReplayedWithoutRecheck(t *testing.T) {
	t.Parallel()
	probe := newTestOuroborosWithLeiosDB(t)
	header, err := decodeLeiosAnnouncementHeader(
		testDijkstraAnnouncementHeaderRaw(t),
	)
	require.NoError(t, err)
	deferredErr := probe.ledgerState.ValidateChainSelectionHeaderCrypto(header)
	require.True(t, ledger.IsHeaderVerificationDeferred(deferredErr))

	n := newLeiosOfferNode(t)
	n.ledger.err = deferredErr
	point := leiosOfferPoint(900)
	require.NoError(t, n.o.leiosnotifyClientNotification(
		n.ctx(n.first), n.announcement(point),
	))
	require.NoError(t, n.offerBody(n.first, point, 100))
	require.Equal(t, 1, pendingLeiosOfferCount(n.o))
	require.Zero(t, n.o.leiosFetchGuardFor(n.first.Id()).inflight.Load())

	n.ledger.err = nil
	n.o.retryDeferredLeiosAnnouncements()
	require.Zero(t, pendingLeiosOfferCount(n.o))
	require.Equal(
		t,
		int32(1),
		n.o.leiosFetchGuardFor(n.first.Id()).inflight.Load(),
		"the remembered offer must be released to its peer",
	)
	// Remembering did not forgive the offer.
	require.ErrorIs(
		t, n.offerBody(n.first, point, 100), errLeiosOfferRepeated,
	)
}

func TestLeiosPeerOfferRecordsAreBoundedByAnnouncements(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	done := make(chan any)
	total := uint64(leiosPeerMaxAnnouncements + 100)
	for slot := uint64(1); slot <= total; slot++ {
		point := leiosOfferPoint(slot)
		o.recordLeiosPeerAnnouncement("peer", done, slot, point.Hash)
		require.NoError(
			t,
			o.checkLeiosPeerOffer("peer", done, point, 100, false),
		)
	}
	o.leiosPeerOffersMu.Lock()
	state := o.leiosPeerOffers["peer"]
	announced, offered := len(state.announced), len(state.offered)
	o.leiosPeerOffersMu.Unlock()
	require.Equal(t, leiosPeerMaxAnnouncements, announced)
	require.LessOrEqual(t, offered, leiosPeerMaxAnnouncements)
	require.ErrorIs(
		t,
		o.checkLeiosPeerOffer("peer", done, leiosOfferPoint(1), 100, false),
		errLeiosOfferUnannounced,
	)
	require.ErrorIs(
		t,
		o.checkLeiosPeerOffer("peer", done, leiosOfferPoint(total), 100, false),
		errLeiosOfferRepeated,
	)
}
