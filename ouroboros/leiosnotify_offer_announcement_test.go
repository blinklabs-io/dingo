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

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	"github.com/stretchr/testify/require"
)

// relayOfferFixture is a complete endorser block with one transaction.
type relayOfferFixture struct {
	point  ocommon.Point
	ebRaw  []byte
	txsRaw []cbor.RawMessage
}

func newRelayOfferFixture(t *testing.T, slot uint64) relayOfferFixture {
	t.Helper()
	txRaw := testDijkstraTx(t, 0)
	ebRaw, err := cbor.Encode(&lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{{
			TransactionHash: lcommon.Blake2b256Hash(txRaw),
			TransactionSize: uint32(
				len(txRaw),
			), //nolint:gosec // short test transaction
		}},
	})
	require.NoError(t, err)
	return relayOfferFixture{
		point:  ocommon.NewPoint(slot, lcommon.Blake2b256Hash(ebRaw).Bytes()),
		ebRaw:  ebRaw,
		txsRaw: []cbor.RawMessage{cbor.RawMessage(txRaw)},
	}
}

// newRelayReloadOuroboros returns a node backed by a database whose slot
// timing resolves, which a relayed announcement needs to carry its expiry.
func newRelayReloadOuroboros(t *testing.T) *Ouroboros {
	t.Helper()
	o := newTestOuroborosWithLeiosDB(t)
	o.leiosAnnouncementLedger = &fakeLeiosAnnouncementLedger{
		slotTime: time.Now(),
	}
	return o
}

// drainRelaySends delivers everything queued for connKey and returns the
// messages in send order.
func drainRelaySends(o *Ouroboros, connKey string) []any {
	var sent []any
	for {
		entry, _ := o.leiosEBLog.next(connKey)
		if entry == nil {
			return sent
		}
		sent = append(sent, leiosForgedEBOffer(entry))
		o.leiosEBLog.complete(connKey, nil, true)
	}
}

// requireAnnouncementBeforeOffers asserts that sent opens with the
// announcement and then carries the body offer and the closure offer.
func requireAnnouncementBeforeOffers(
	t *testing.T,
	fixture relayOfferFixture,
	sent []any,
) {
	t.Helper()
	require.Len(t, sent, 3, "announcement, body offer, closure offer")
	_, ok := sent[0].(*oleiosnotify.MsgBlockAnnouncement)
	require.True(t, ok, "the announcement must be sent before any offer")
	body, ok := sent[1].(*oleiosnotify.MsgBlockOffer)
	require.True(t, ok)
	require.Equal(t, fixture.point, body.Point)
	closure, ok := sent[2].(*oleiosnotify.MsgBlockTxsOffer)
	require.True(t, ok)
	require.Equal(t, fixture.point, closure.Point)
}

// requireNoOffers asserts that nothing offering an endorser block was sent.
func requireNoOffers(t *testing.T, sent []any) {
	t.Helper()
	for _, msg := range sent {
		switch msg.(type) {
		case *oleiosnotify.MsgBlockOffer, *oleiosnotify.MsgBlockTxsOffer:
			require.Fail(
				t,
				"offer sent on a connection with no announcement for the block",
			)
		}
	}
}

func TestLeiosRelayBackfillWithoutAnnouncementOffersNothing(t *testing.T) {
	t.Parallel()
	fixture := newRelayOfferFixture(t, 42)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	o.leiosEBLog.registerConn("downstream", nil, nil)

	require.NoError(t, o.storeLeiosEndorserBlock(
		fixture.point, fixture.ebRaw, fixture.txsRaw, leiosStoreBackfill,
	))
	requireNoOffers(t, drainRelaySends(o, "downstream"))

	// The announcement arriving later releases the offers, behind itself.
	announceTestEndorserBlock(
		t, o, fixture.point.Slot,
		lcommon.Blake2b256Hash(fixture.ebRaw), len(fixture.ebRaw),
	)
	requireAnnouncementBeforeOffers(
		t, fixture, drainRelaySends(o, "downstream"),
	)
}

func TestLeiosRelayBackfillWithAnnouncementSendsItFirst(t *testing.T) {
	t.Parallel()
	fixture := newRelayOfferFixture(t, 42)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	o.leiosEBLog.registerConn("downstream", nil, nil)
	announceTestEndorserBlock(
		t, o, fixture.point.Slot,
		lcommon.Blake2b256Hash(fixture.ebRaw), len(fixture.ebRaw),
	)

	require.NoError(t, o.storeLeiosEndorserBlock(
		fixture.point, fixture.ebRaw, fixture.txsRaw, leiosStoreBackfill,
	))
	requireAnnouncementBeforeOffers(
		t, fixture, drainRelaySends(o, "downstream"),
	)
}

func TestLeiosRelayReloadedBlockWithoutAnnouncementOffersNothing(
	t *testing.T,
) {
	t.Parallel()
	fixture := newRelayOfferFixture(t, 472)
	o := newRelayReloadOuroboros(t)
	o.leiosEBLog.registerConn("downstream", nil, nil)
	require.NoError(t, o.leiosDatabase().SetLeiosEB(
		fixture.point.Slot, fixture.point.Hash, fixture.ebRaw, fixture.txsRaw,
	))

	o.markLeiosEndorserBlockRelayOfferFrom(fixture.point, "offer-source")
	data, ok := o.lookupLeiosEndorserBlock(
		fixture.point.Slot, fixture.point.Hash,
	)
	require.True(t, ok)
	require.True(t, data.completeTxCache())
	requireNoOffers(t, drainRelaySends(o, "downstream"))

	announceTestEndorserBlock(
		t, o, fixture.point.Slot,
		lcommon.Blake2b256Hash(fixture.ebRaw), len(fixture.ebRaw),
	)
	requireAnnouncementBeforeOffers(
		t, fixture, drainRelaySends(o, "downstream"),
	)
}

func TestLeiosRelayReloadedBlockCarriesRecordedAnnouncement(t *testing.T) {
	t.Parallel()
	fixture := newRelayOfferFixture(t, 472)
	o := newRelayReloadOuroboros(t)
	o.leiosEBLog.registerConn("downstream", nil, nil)
	announceTestEndorserBlock(
		t, o, fixture.point.Slot,
		lcommon.Blake2b256Hash(fixture.ebRaw), len(fixture.ebRaw),
	)
	require.NoError(t, o.leiosDatabase().SetLeiosEB(
		fixture.point.Slot, fixture.point.Hash, fixture.ebRaw, fixture.txsRaw,
	))

	o.markLeiosEndorserBlockRelayOfferFrom(fixture.point, "offer-source")
	_, ok := o.lookupLeiosEndorserBlock(fixture.point.Slot, fixture.point.Hash)
	require.True(t, ok)
	requireAnnouncementBeforeOffers(
		t, fixture, drainRelaySends(o, "downstream"),
	)
}

func TestLeiosRelayBackfillTakesSelectedChainAnnouncement(t *testing.T) {
	t.Parallel()
	fixture := newRelayOfferFixture(t, 42)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	o.leiosEBLog.registerConn("downstream", nil, nil)
	header := testLeiosAnnouncingHeader(
		t, fixture.point, uint64(len(fixture.ebRaw)),
	)
	release, err := o.retainSelectedLeiosHeader(header)
	require.NoError(t, err)
	defer release()

	require.NoError(t, o.storeLeiosEndorserBlock(
		fixture.point, fixture.ebRaw, fixture.txsRaw, leiosStoreBackfill,
	))
	sent := drainRelaySends(o, "downstream")
	requireAnnouncementBeforeOffers(t, fixture, sent)
	announcement, ok := sent[0].(*oleiosnotify.MsgBlockAnnouncement)
	require.True(t, ok)
	require.Equal(t, header.Cbor(), []byte(announcement.BlockHeaderRaw))
}

// The announcement path reloads a persisted block before it takes
// leiosAnnouncementsMu, and the reload queues the relay offers, which look the
// announcement up under that mutex. Reloading with it held would deadlock.
func TestLeiosRelayAnnouncementReloadingIntentOffersBehindIt(t *testing.T) {
	t.Parallel()
	fixture := newRelayOfferFixture(t, 472)
	o := newRelayReloadOuroboros(t)
	o.leiosEBLog.registerConn("downstream", nil, nil)
	require.NoError(t, o.leiosDatabase().SetLeiosEB(
		fixture.point.Slot, fixture.point.Hash, fixture.ebRaw, fixture.txsRaw,
	))
	o.markLeiosEndorserBlockRelayOfferFrom(fixture.point, "offer-source")

	headerRaw := testDijkstraAnnouncementHeaderRawFor(
		t, fixture.point.Slot,
		lcommon.Blake2b256Hash(fixture.ebRaw), uint64(len(fixture.ebRaw)),
	)
	recorded := make(chan error, 1)
	go func() {
		recorded <- recordTestLeiosAnnouncementNoFail(o, headerRaw)
	}()
	require.NoError(t, testutil.RequireReceive(
		t, recorded, 5*time.Second, "announcement recording deadlocked",
	))
	requireAnnouncementBeforeOffers(
		t, fixture, drainRelaySends(o, "downstream"),
	)
}
