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
	"time"

	"github.com/blinklabs-io/dingo/ledger"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	"github.com/stretchr/testify/require"
)

// testDijkstraHeaderAnnouncing is testDijkstraAnnouncementHeaderRaw for a
// given endorser block: a slot-1 ranking-block header announcing ebRaw.
func testDijkstraHeaderAnnouncing(t *testing.T, ebRaw []byte) []byte {
	t.Helper()
	_, blockRaw := testDijkstraBlockRaw(t, 1)
	var components []cbor.RawMessage
	_, err := cbor.Decode(blockRaw, &components)
	require.NoError(t, err)
	var headerTop []cbor.RawMessage
	_, err = cbor.Decode(components[0], &headerTop)
	require.NoError(t, err)
	var headerBody []cbor.RawMessage
	_, err = cbor.Decode(headerTop[0], &headerBody)
	require.NoError(t, err)
	headerBody = append(
		headerBody[:10],
		mustCbor(t, false),
		mustCbor(t, []any{
			lcommon.Blake2b256Hash(ebRaw).Bytes(),
			uint64(len(ebRaw)),
		}),
	)
	headerTop[0], err = cbor.Encode(headerBody)
	require.NoError(t, err)
	headerRaw, err := cbor.Encode(headerTop)
	require.NoError(t, err)
	return headerRaw
}

// drainLeiosNotify returns every entry the leios-notify server would send
// connKey, in order.
func drainLeiosNotify(
	o *Ouroboros,
	connKey string,
) []*leiosForgedEBEntry {
	var entries []*leiosForgedEBEntry
	for {
		entry, _ := o.leiosEBLog.next(connKey)
		if entry == nil {
			return entries
		}
		entries = append(entries, entry)
		o.leiosEBLog.complete(connKey, nil, true)
	}
}

func newForgedOfferOrderOuroboros(t *testing.T) *Ouroboros {
	t.Helper()
	o := newOuroboros(OuroborosConfig{
		EnableLeios: true,
		LeiosAnnouncementLedger: &fakeLeiosAnnouncementLedger{
			currentSlot: 10,
			slotTime:    time.Now().Add(-time.Minute),
			staleness:   ledger.LeiosAnnouncementFreshOCIN,
		},
	})
	t.Cleanup(func() { _ = o.Close() })
	return o
}

// A peer may offer only an endorser block it has itself announced: the
// reference node answers a LeiosNotify block or closure offer for an endorser
// block the peer never announced to it by disconnecting the peer
// (ExnLeiosBlockOfferWithoutAnnouncement and
// ExnLeiosClosureOfferWithoutAnnouncement in ouroboros-consensus
// LeiosDemoLogic). The forger stores and broadcasts its endorser block before
// the ranking block announcing it is built and adopted, so the offers must
// wait for that announcement and follow it on the wire.
func TestForgedEndorserBlockOffersFollowItsAnnouncement(t *testing.T) {
	t.Parallel()
	o := newForgedOfferOrderOuroboros(t)
	const conn = "peer"
	o.leiosEBLog.registerConn(conn, nil, nil)

	ebPoint, ebRaw := testLeiosEndorserBlockRaw(t, 1)
	require.NoError(t, o.BroadcastEndorserBlock(
		ebPoint.Slot,
		ebPoint.Hash,
		ebRaw,
		nil,
	))
	require.Empty(t, drainLeiosNotify(o, conn),
		"nothing may be offered before the announcing header is sent")

	o.EnqueueLeiosBlockAnnouncement(testDijkstraHeaderAnnouncing(t, ebRaw))
	entries := drainLeiosNotify(o, conn)
	require.Len(t, entries, 3)
	require.NotNil(t, entries[0].announcement, "announcement first")
	require.NotNil(t, entries[1].point, "then the block offer")
	require.Equal(t, ebPoint, *entries[1].point)
	require.Equal(t, uint64(len(ebRaw)), entries[1].size)
	require.NotNil(t, entries[2].txOffer, "then the closure offer")
	require.Equal(t, ebPoint, *entries[2].txOffer)

	// A peer acting on the offer fetches the manifest by the offered point.
	resp, err := o.leiosfetchServerBlockRequest(
		oleiosfetch.CallbackContext{},
		*entries[1].point,
	)
	require.NoError(t, err)
	block, ok := resp.(*oleiosfetch.MsgBlock)
	require.True(t, ok)
	require.Equal(t, []byte(ebRaw), []byte(block.BlockRaw))

	// A repeated announcement does not offer the block a second time.
	o.EnqueueLeiosBlockAnnouncement(testDijkstraHeaderAnnouncing(t, ebRaw))
	require.Empty(t, drainLeiosNotify(o, conn))
}

// A forged endorser block whose ranking block never reaches the announcement
// stage -- self-validation or adoption failed -- is never offered.
func TestUnannouncedForgedEndorserBlockIsNeverOffered(t *testing.T) {
	t.Parallel()
	o := newForgedOfferOrderOuroboros(t)
	const conn = "peer"
	o.leiosEBLog.registerConn(conn, nil, nil)

	ebPoint, ebRaw := testLeiosEndorserBlockRaw(t, 1)
	require.NoError(t, o.BroadcastEndorserBlock(
		ebPoint.Slot, ebPoint.Hash, ebRaw, nil,
	))
	_, otherRaw := testLeiosEndorserBlockRaw(t, 2)
	o.EnqueueLeiosBlockAnnouncement(testDijkstraHeaderAnnouncing(t, otherRaw))
	for _, entry := range drainLeiosNotify(o, conn) {
		require.Nil(t, entry.point)
		require.Nil(t, entry.txOffer)
	}
}

// The pending offers are bounded: forged endorser blocks that were never
// announced do not accumulate.
func TestPendingForgedEndorserBlockOffersAreBounded(t *testing.T) {
	t.Parallel()
	o := newForgedOfferOrderOuroboros(t)
	for slot := range uint64(3 * leiosMaxPendingForgedOffers) {
		point := ocommon.NewPoint(slot+1, lcommon.Blake2b256Hash(
			[]byte{byte(slot), byte(slot >> 8)},
		).Bytes())
		o.queueForgedEndorserBlockOffers(point, 10)
	}
	o.leiosAnnouncementsMu.Lock()
	defer o.leiosAnnouncementsMu.Unlock()
	require.Len(t, o.leiosPendingForgedOffers, leiosMaxPendingForgedOffers)
	for _, pending := range o.leiosPendingForgedOffers {
		require.Greater(t,
			pending.point.Slot,
			uint64(2*leiosMaxPendingForgedOffers),
			"the newest forged endorser blocks are kept",
		)
	}
}
