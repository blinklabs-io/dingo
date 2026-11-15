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
	"net"
	"testing"
	"time"

	gouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	"github.com/stretchr/testify/require"
)

const (
	relayLeadTestImmutable = 10_000
	relayLeadTestLead      = 1_200
)

type relayLeadFixture struct {
	o   *Ouroboros
	ctx leiosnotify.CallbackContext
}

func newRelayLeadFixture(
	t *testing.T,
	immutable func(context.Context) (uint64, bool, error),
) *relayLeadFixture {
	t.Helper()
	o := newOuroboros(OuroborosConfig{LeiosMinOfferLead: relayLeadTestLead})
	o.leiosImmutableSlot = immutable
	server := leiosnotify.NewServer(protocol.ProtocolOptions{}, nil)
	t.Cleanup(server.Stop)
	done := make(chan any)
	t.Cleanup(func() { close(done) })
	ctx := leiosnotify.CallbackContext{
		Server: server,
		ConnectionId: gouroboros.ConnectionId{
			LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
			RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
		},
		ConnectionDoneChan: done,
	}
	o.leiosEBLog.registerConn(leiosConnectionIdString(ctx.ConnectionId), server, nil)
	return &relayLeadFixture{o: o, ctx: ctx}
}

func fixedImmutable(
	slot uint64,
	found bool,
) func(context.Context) (uint64, bool, error) {
	return func(context.Context) (uint64, bool, error) {
		return slot, found, nil
	}
}

func relayLeadPoint(slot uint64, id byte) ocommon.Point {
	hash := make([]byte, lcommon.Blake2b256Size)
	hash[0] = id
	return ocommon.NewPoint(slot, hash)
}

// firstMessage sends one entry followed by a sentinel vote and reports which
// message the first RequestNext returned. A vote is never gated, so the
// sentinel is what a suppressed offer yields to.
func (f *relayLeadFixture) firstMessage(
	t *testing.T,
	entry leiosForgedEBEntry,
) protocol.Message {
	t.Helper()
	f.o.leiosEBLog.append(entry)
	f.o.EnqueueLeiosPrototypeVote(lcommon.LeiosPrototypeVote{
		AnnouncingRbHash: lcommon.NewBlake2b256([]byte("sentinel")),
		VoterId:          1,
		VoteSignature:    make([]byte, lcommon.LeiosBlsSignatureSize),
	})
	type result struct {
		msg protocol.Message
		err error
	}
	ch := make(chan result, 1)
	go func() {
		msg, err := f.o.leiosnotifyServerRequestNext(f.ctx)
		ch <- result{msg, err}
	}()
	select {
	case r := <-ch:
		require.NoError(t, r.err)
		return r.msg
	case <-time.After(5 * time.Second):
		t.Fatal("request-next did not return")
		return nil
	}
}

func offerEntries(point ocommon.Point) map[string]leiosForgedEBEntry {
	blockPoint := point
	txPoint := point
	return map[string]leiosForgedEBEntry{
		"block offer": {point: &blockPoint, size: 100},
		"txs offer":   {txOffer: &txPoint},
	}
}

func TestLeiosNotifyOfferLeadBoundary(t *testing.T) {
	t.Parallel()
	floor := uint64(relayLeadTestImmutable + relayLeadTestLead)
	cases := []struct {
		name string
		slot uint64
		sent bool
	}{
		{"below immutable tip", relayLeadTestImmutable - 1, false},
		{"at immutable tip", relayLeadTestImmutable, false},
		{"one below tip plus lead", floor - 1, false},
		{"exactly tip plus lead", floor, true},
		{"one above tip plus lead", floor + 1, true},
	}
	for _, tc := range cases {
		for shape, entry := range offerEntries(relayLeadPoint(tc.slot, 1)) {
			t.Run(tc.name+"/"+shape, func(t *testing.T) {
				t.Parallel()
				f := newRelayLeadFixture(
					t,
					fixedImmutable(relayLeadTestImmutable, true),
				)
				msg := f.firstMessage(t, entry)
				_, isVote := msg.(*leiosnotify.MsgVotesOffer)
				if tc.sent {
					require.False(t, isVote, "offer must be sent before the vote")
					return
				}
				require.True(
					t,
					isVote,
					"offer below the lead must be dropped, got %T",
					msg,
				)
			})
		}
	}
}

func TestLeiosNotifyOfferLeadDropsWithDependencyHeader(t *testing.T) {
	t.Parallel()
	f := newRelayLeadFixture(t, fixedImmutable(relayLeadTestImmutable, true))
	slot := uint64(relayLeadTestImmutable)
	point := relayLeadPoint(slot, 2)
	f.o.leiosEndorserBlocks = map[string]*leiosEndorserBlockData{
		leiosBlockKey(point.Slot, point.Hash): {
			point:      point,
			cacheKeys:  []string{leiosBlockKey(point.Slot, point.Hash)},
			insertedAt: time.Now(),
			blockRaw:   []byte{1},
		},
	}
	entry := leiosForgedEBEntry{
		point:            &point,
		size:             1,
		dependencyHeader: []byte("header"),
	}
	// An entry carrying a dependency header is preceded by its announcement,
	// which is not an endorser-block offer and is always sent.
	msg := f.firstMessage(t, entry)
	require.IsType(t, &leiosnotify.MsgBlockAnnouncement{}, msg)
	f.o.leiosnotifyServerResponseSent(f.ctx, msg, nil)
	next, err := f.o.leiosnotifyServerRequestNext(f.ctx)
	require.NoError(t, err)
	require.IsType(
		t,
		&leiosnotify.MsgVotesOffer{},
		next,
		"gated offer must yield to the vote",
	)
}

func TestLeiosNotifyOfferLeadDoesNotGateOtherEntries(t *testing.T) {
	t.Parallel()
	f := newRelayLeadFixture(t, fixedImmutable(relayLeadTestImmutable, true))
	msg := f.firstMessage(t, leiosForgedEBEntry{announcement: []byte("hdr")})
	require.IsType(t, &leiosnotify.MsgBlockAnnouncement{}, msg)
}

func TestLeiosNotifyOfferLeadNoImmutablePointUsesOrigin(t *testing.T) {
	t.Parallel()
	below := relayLeadPoint(relayLeadTestLead-1, 3)
	f := newRelayLeadFixture(t, fixedImmutable(0, false))
	_, isVote := f.firstMessage(
		t,
		offerEntries(below)["block offer"],
	).(*leiosnotify.MsgVotesOffer)
	require.True(t, isVote, "an offer below the lead is dropped at Origin")

	at := relayLeadPoint(relayLeadTestLead, 4)
	f = newRelayLeadFixture(t, fixedImmutable(0, false))
	msg := f.firstMessage(t, offerEntries(at)["block offer"])
	require.IsType(t, &leiosnotify.MsgBlockOffer{}, msg)
}

func TestLeiosNotifyOfferLeadImmutableLookupFailureDrops(t *testing.T) {
	t.Parallel()
	f := newRelayLeadFixture(
		t,
		func(context.Context) (uint64, bool, error) {
			return 0, false, errors.New("lookup failed")
		},
	)
	point := relayLeadPoint(1_000_000, 5)
	_, isVote := f.firstMessage(
		t,
		offerEntries(point)["block offer"],
	).(*leiosnotify.MsgVotesOffer)
	require.True(t, isVote)
}

func TestLeiosNotifyOfferLeadDisabledWithoutLedger(t *testing.T) {
	t.Parallel()
	f := newRelayLeadFixture(t, nil)
	point := relayLeadPoint(1, 6)
	msg := f.firstMessage(t, offerEntries(point)["block offer"])
	require.IsType(t, &leiosnotify.MsgBlockOffer{}, msg)
}
