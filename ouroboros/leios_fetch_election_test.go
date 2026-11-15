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
	"net"
	"sync/atomic"
	"testing"

	gouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	gdijkstra "github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	"github.com/stretchr/testify/require"
)

// observeLeiosElectionForTest marks point's election as observed without
// running the announcement path, for tests whose subject is an offer made after
// the election.
func (o *Ouroboros) observeLeiosElectionForTest(point ocommon.Point) {
	o.leiosAnnouncementsMu.Lock()
	defer o.leiosAnnouncementsMu.Unlock()
	if o.leiosAnnouncementSlots == nil {
		o.leiosAnnouncementSlots = make(map[string]map[uint64]struct{})
	}
	hash := string(point.Hash)
	if o.leiosAnnouncementSlots[hash] == nil {
		o.leiosAnnouncementSlots[hash] = make(map[uint64]struct{})
	}
	o.leiosAnnouncementSlots[hash][point.Slot] = struct{}{}
}

// testLeiosAnnouncingHeader returns a ranking-block header at point.Slot that
// announces the endorser block point.Hash.
func testLeiosAnnouncingHeader(
	t *testing.T,
	point ocommon.Point,
	size uint64,
) *gdijkstra.DijkstraBlockHeader {
	t.Helper()
	_, blockRaw := testDijkstraBlockRaw(t, int(point.Slot)) // #nosec G115
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
		mustCbor(t, []any{point.Hash, size}),
	)
	headerTop[0], err = cbor.Encode(headerBody)
	require.NoError(t, err)
	headerRaw, err := cbor.Encode(headerTop)
	require.NoError(t, err)
	header, err := gdijkstra.NewDijkstraBlockHeaderFromCbor(headerRaw)
	require.NoError(t, err)
	require.Equal(t, point.Slot, header.SlotNumber())
	return header
}

// announceLeiosElectionForTest observes point's election through the
// production announcement path.
func announceLeiosElectionForTest(
	t *testing.T,
	o *Ouroboros,
	point ocommon.Point,
	size uint64,
) {
	t.Helper()
	header := testLeiosAnnouncingHeader(t, point, size)
	ebHash, ebSize, ok := header.LeiosAnnouncement()
	require.True(t, ok)
	require.NoError(t, o.recordLeiosAnnouncement(
		header.Cbor(), header.Cbor(), ebHash, ebSize, header, "test", false,
	))
}

type countingLeiosTxOfferRequester struct {
	blockRaw      []byte
	blockRequests atomic.Int32
	txsRequests   atomic.Int32
}

func (c *countingLeiosTxOfferRequester) BlockRequest(
	context.Context,
	ocommon.Point,
) (protocol.Message, error) {
	c.blockRequests.Add(1)
	return &leiosfetch.MsgBlock{BlockRaw: c.blockRaw}, nil
}

func (c *countingLeiosTxOfferRequester) BlockTxsRequest(
	context.Context,
	ocommon.Point,
	map[uint16]uint64,
) (protocol.Message, error) {
	c.txsRequests.Add(1)
	return nil, context.Canceled
}

func TestLeiosManifestOfferFetchesOnlyAfterElectionObserved(t *testing.T) {
	t.Parallel()
	o, first, second := newHeldManifestOfferPeers(t)
	firstGuard := o.leiosFetchGuardFor(first.Id())
	secondGuard := o.leiosFetchGuardFor(second.Id())
	point, raw := testLeiosEndorserBlockRaw(t, 200)
	offer := oleiosnotify.NewMsgBlockOffer(point, uint64(len(raw)))

	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: first.Id()}, offer,
	))
	require.Zero(
		t,
		firstGuard.inflight.Load()+secondGuard.inflight.Load(),
		"an offer for an unobserved election must not start a manifest fetch",
	)

	announceLeiosElectionForTest(t, o, point, uint64(len(raw)))
	require.Equal(
		t,
		int32(1),
		firstGuard.inflight.Load(),
		"the offering peer is fetched from once the election is observed",
	)
	require.Zero(t, secondGuard.inflight.Load())
}

func TestLeiosTxsOfferFetchesOnlyAfterElectionObserved(t *testing.T) {
	t.Parallel()
	o, first, second := newHeldManifestOfferPeers(t)
	firstGuard := o.leiosFetchGuardFor(first.Id())
	secondGuard := o.leiosFetchGuardFor(second.Id())
	point, raw := testLeiosEndorserBlockRaw(t, 201)

	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: first.Id()},
		oleiosnotify.NewMsgBlockTxsOffer(point),
	))
	require.Zero(
		t,
		firstGuard.inflight.Load(),
		"a transaction offer for an unobserved election must not start a fetch",
	)

	announceLeiosElectionForTest(t, o, point, uint64(len(raw)))
	require.Equal(t, int32(1), firstGuard.inflight.Load())
	require.Zero(t, secondGuard.inflight.Load())
}

func TestLeiosOfferFetchesAfterSelectedHeaderObserved(t *testing.T) {
	t.Parallel()
	o, first, _ := newHeldManifestOfferPeers(t)
	firstGuard := o.leiosFetchGuardFor(first.Id())
	point, raw := testLeiosEndorserBlockRaw(t, 204)
	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: first.Id()},
		oleiosnotify.NewMsgBlockOffer(point, uint64(len(raw))),
	))
	require.Zero(t, firstGuard.inflight.Load())

	release, err := o.retainSelectedLeiosHeader(
		testLeiosAnnouncingHeader(t, point, uint64(len(raw))),
	)
	require.NoError(t, err)
	defer release()
	require.Equal(t, int32(1), firstGuard.inflight.Load())
}

func TestLeiosUnelectedOfferStaysUnfetchedAfterOtherElection(t *testing.T) {
	t.Parallel()
	o, first, _ := newHeldManifestOfferPeers(t)
	firstGuard := o.leiosFetchGuardFor(first.Id())
	offered, raw := testLeiosEndorserBlockRaw(t, 205)
	other, otherRaw := testLeiosEndorserBlockRaw(t, 206)
	require.NoError(t, o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: first.Id()},
		oleiosnotify.NewMsgBlockOffer(offered, uint64(len(raw))),
	))
	announceLeiosElectionForTest(t, o, other, uint64(len(otherRaw)))
	require.Zero(
		t,
		firstGuard.inflight.Load(),
		"an election for another point must not release this offer",
	)
}

func TestFetchLeiosTxOfferRequiresObservedElection(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{
		EnableLeios:        true,
		EnableLeiosTxFetch: true,
	})
	point, raw := testLeiosEndorserBlockRaw(t, 202)
	client := &countingLeiosTxOfferRequester{blockRaw: raw}

	require.False(t, o.fetchLeiosTxOffer(
		context.Background(), client, "peer", point,
	))
	require.Zero(t, client.blockRequests.Load(),
		"no manifest request without an observed election")
	require.Zero(t, client.txsRequests.Load())
	_, cached := o.lookupLeiosEndorserBlock(point.Slot, point.Hash)
	require.False(t, cached)

	announceLeiosElectionForTest(t, o, point, uint64(len(raw)))
	o.fetchLeiosTxOffer(context.Background(), client, "peer", point)
	require.Equal(t, int32(1), client.blockRequests.Load(),
		"the manifest is fetched once the election is observed")
}

func TestLeiosElectionObservedBindsOneSlot(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	point, raw := testLeiosEndorserBlockRaw(t, 203)
	require.False(t, o.leiosElectionObserved(point))
	announceLeiosElectionForTest(t, o, point, uint64(len(raw)))
	require.True(t, o.leiosElectionObserved(point))
	require.False(t, o.leiosElectionObserved(
		ocommon.NewPoint(point.Slot+1, point.Hash),
	), "the election binds one slot, not every slot of the hash")
}

func testPendingOfferConn(port int) oleiosnotify.CallbackContext {
	return oleiosnotify.CallbackContext{ConnectionId: gouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port},
	}}
}

func pendingLeiosOfferCount(o *Ouroboros) (total int) {
	o.leiosPendingOffersMu.Lock()
	defer o.leiosPendingOffersMu.Unlock()
	for _, offers := range o.leiosPendingOffers {
		total += len(offers)
	}
	return total
}

func TestLeiosPendingOffersAreBounded(t *testing.T) {
	t.Parallel()
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	victim := testPendingOfferConn(1)
	victimPoint, _ := testLeiosEndorserBlockRaw(t, 1)
	o.rememberLeiosOffer(
		victim, oleiosnotify.NewMsgBlockTxsOffer(victimPoint), victimPoint, true,
	)

	flooder := testPendingOfferConn(2)
	for idx := range 5 * leiosBackfillMaxPointsPerConn {
		point, _ := testLeiosEndorserBlockRaw(t, 100+idx)
		o.rememberLeiosOffer(
			flooder, oleiosnotify.NewMsgBlockTxsOffer(point), point, true,
		)
	}
	require.Equal(
		t,
		leiosBackfillMaxPointsPerConn+1,
		pendingLeiosOfferCount(o),
		"one connection holds at most its per-connection cap and displaces only its own",
	)
	o.leiosPendingOffersMu.Lock()
	_, victimKept := o.leiosPendingOffers[leiosBlockKey(
		victimPoint.Slot, victimPoint.Hash,
	)]
	o.leiosPendingOffersMu.Unlock()
	require.True(t, victimKept)

	shared, _ := testLeiosEndorserBlockRaw(t, 9000)
	for port := range 3 * leiosBackfillMaxSourcesPerPoint {
		o.rememberLeiosOffer(
			testPendingOfferConn(10_000+port),
			oleiosnotify.NewMsgBlockTxsOffer(shared),
			shared,
			true,
		)
	}
	o.leiosPendingOffersMu.Lock()
	perPoint := len(o.leiosPendingOffers[leiosBlockKey(shared.Slot, shared.Hash)])
	o.leiosPendingOffersMu.Unlock()
	require.Equal(t, leiosBackfillMaxSourcesPerPoint, perPoint)
}
