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
	"fmt"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

// newDeferringLeiosOuroboros returns an Ouroboros whose announcement ledger
// defers header verification until the returned ledger's err is cleared, and
// a function producing a distinct announcement header per slot.
func newDeferringLeiosOuroboros(
	t *testing.T,
) (*Ouroboros, *fakeLeiosAnnouncementLedger, func(slot uint64) []byte) {
	t.Helper()
	probe := newTestOuroborosWithLeiosDB(t)
	header, err := decodeLeiosAnnouncementHeader(
		testDijkstraAnnouncementHeaderRaw(t),
	)
	require.NoError(t, err)
	deferredErr := probe.ledgerState.ValidateChainSelectionHeaderCrypto(header)
	require.True(t, ledger.IsHeaderVerificationDeferred(deferredErr))

	announcementLedger := &fakeLeiosAnnouncementLedger{
		currentSlot: 10_000,
		slotTime:    time.Now().Add(-time.Minute),
		staleness:   ledger.LeiosAnnouncementFreshOCIN,
		err:         deferredErr,
	}
	o := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LeiosAnnouncementLedger: announcementLedger,
	})
	headerFor := func(slot uint64) []byte {
		return testDijkstraAnnouncementHeaderRawFor(
			t,
			slot,
			lcommon.NewBlake2b256(
				[]byte(fmt.Sprintf("%032d", slot)),
			),
			1234,
		)
	}
	return o, announcementLedger, headerFor
}

func deferredLeiosAnnouncementsFrom(o *Ouroboros, source string) int {
	o.leiosDeferredMu.Lock()
	defer o.leiosDeferredMu.Unlock()
	count := 0
	for _, announcement := range o.leiosDeferredAnnouncements {
		if announcement.source == source {
			count++
		}
	}
	return count
}

// TestDeferredLeiosAnnouncementsOneSourceCannotFillTheCap shows that the
// shared deferral cap is not monopolized by one connection: a peer offering
// more deferred headers than the cap leaves room for another peer's.
func TestDeferredLeiosAnnouncementsOneSourceCannotFillTheCap(t *testing.T) {
	t.Parallel()

	o, _, headerFor := newDeferringLeiosOuroboros(t)
	for slot := uint64(1); slot <= leiosMaxDeferredAnnouncements+10; slot++ {
		require.Error(t, o.acceptLeiosAnnouncement(headerFor(slot), "peer-a"))
	}
	require.Less(
		t,
		deferredLeiosAnnouncementsFrom(o, "peer-a"),
		leiosMaxDeferredAnnouncements,
		"one source must not hold every deferral slot",
	)

	require.Error(t, o.acceptLeiosAnnouncement(headerFor(5_000), "peer-b"))
	require.Equal(t, 1, deferredLeiosAnnouncementsFrom(o, "peer-b"))
}

// TestDeferredLeiosAnnouncementsStayBoundedAndDrain shows that the retained
// set never exceeds the cap however many sources defer, that dropped
// announcements are not retained later, and that a successful retry empties
// the set and frees the capacity for new deferrals.
func TestDeferredLeiosAnnouncementsStayBoundedAndDrain(t *testing.T) {
	t.Parallel()

	o, announcementLedger, headerFor := newDeferringLeiosOuroboros(t)
	const sources = leiosMaxDeferredAnnouncements + 20
	for slot := uint64(1); slot <= sources; slot++ {
		require.Error(
			t,
			o.acceptLeiosAnnouncement(headerFor(slot), fmt.Sprintf("peer-%d", slot)),
		)
	}
	o.leiosDeferredMu.Lock()
	retained := len(o.leiosDeferredAnnouncements)
	o.leiosDeferredMu.Unlock()
	require.Equal(t, leiosMaxDeferredAnnouncements, retained)

	// A retry that still defers keeps every entry and adds none.
	o.retryDeferredLeiosAnnouncements()
	o.leiosDeferredMu.Lock()
	require.Len(t, o.leiosDeferredAnnouncements, leiosMaxDeferredAnnouncements)
	o.leiosDeferredMu.Unlock()

	// Once verification can proceed, a retry accepts every retained
	// announcement and releases its slot.
	deferredErr := announcementLedger.err
	announcementLedger.err = nil
	o.retryDeferredLeiosAnnouncements()
	o.leiosDeferredMu.Lock()
	require.Empty(t, o.leiosDeferredAnnouncements)
	o.leiosDeferredMu.Unlock()
	o.leiosAnnouncementsMu.Lock()
	require.Len(t, o.leiosAnnouncements, leiosMaxDeferredAnnouncements)
	o.leiosAnnouncementsMu.Unlock()

	// The capacity is reusable.
	announcementLedger.err = deferredErr
	require.Error(t, o.acceptLeiosAnnouncement(headerFor(9_000), "peer-late"))
	require.Equal(t, 1, deferredLeiosAnnouncementsFrom(o, "peer-late"))
}

// TestHandleConnClosedEventDropsDeferredLeiosAnnouncements shows that a closed
// connection's deferred announcements do not outlive it and keep consuming the
// shared cap, while another connection's are untouched.
func TestHandleConnClosedEventDropsDeferredLeiosAnnouncements(t *testing.T) {
	t.Parallel()

	o, _, headerFor := newDeferringLeiosOuroboros(t)
	closing := testConnIdWithPort(4001)
	surviving := testConnIdWithPort(4002)
	closingKey := leiosConnectionIdString(closing)
	survivingKey := leiosConnectionIdString(surviving)
	require.Error(t, o.acceptLeiosAnnouncement(headerFor(1), closingKey))
	require.Error(t, o.acceptLeiosAnnouncement(headerFor(2), closingKey))
	require.Error(t, o.acceptLeiosAnnouncement(headerFor(3), survivingKey))

	o.HandleConnClosedEvent(event.NewEvent(
		connmanager.ConnectionClosedEventType,
		connmanager.ConnectionClosedEvent{ConnectionId: closing},
	))

	require.Zero(t, deferredLeiosAnnouncementsFrom(o, closingKey))
	require.Equal(t, 1, deferredLeiosAnnouncementsFrom(o, survivingKey))
}
