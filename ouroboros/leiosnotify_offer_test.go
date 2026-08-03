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

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	"github.com/stretchr/testify/require"
)

// A forged endorser block must be offered as a MsgBlockOffer whose MessageType
// is set. A bare struct literal leaves MessageType at 0, which the gouroboros
// leios-notify state machine rejects in the Busy state ("not allowed in current
// protocol state Busy"), so the EB is never offered, fetched, voted on, or
// certified.
func TestLeiosForgedEBOfferSetsBlockOfferType(t *testing.T) {
	point := ocommon.Point{Slot: 42, Hash: []byte("eb-hash")}
	entry := &leiosForgedEBEntry{point: &point, size: 1234}

	msg := leiosForgedEBOffer(entry)
	require.NotNil(t, msg)
	require.Equal(t, uint8(oleiosnotify.MessageTypeBlockOffer), msg.Type())

	offer, ok := msg.(*oleiosnotify.MsgBlockOffer)
	require.True(t, ok)
	require.Equal(t, point, offer.Point)
	require.Equal(t, uint64(1234), offer.Size)
}

// A locally emitted vote must be offered as a MsgVotesOffer with its type set.
func TestLeiosForgedEBOfferSetsVotesOfferType(t *testing.T) {
	vote := lcommon.LeiosPrototypeVote{
		AnnouncingRbHash: lcommon.NewBlake2b256([]byte("announcing-rb")),
		VoterId:          7,
		VoteSignature:    make([]byte, lcommon.LeiosBlsSignatureSize),
	}
	entry := &leiosForgedEBEntry{vote: &vote}

	msg := leiosForgedEBOffer(entry)
	require.NotNil(t, msg)
	require.Equal(t, uint8(oleiosnotify.MessageTypeVotesOffer), msg.Type())

	offer, ok := msg.(*oleiosnotify.MsgVotesOffer)
	require.True(t, ok)
	require.Equal(t, []lcommon.LeiosPrototypeVote{vote}, offer.PrototypeVotes)
}

// An empty entry yields no offer.
func TestLeiosForgedEBOfferEmptyEntryNil(t *testing.T) {
	require.Nil(t, leiosForgedEBOffer(&leiosForgedEBEntry{}))
}

func TestLeiosForgedEBOfferAnnouncement(t *testing.T) {
	raw := []byte{0x82, 0x01, 0x02}
	msg := leiosForgedEBOffer(&leiosForgedEBEntry{announcement: raw})
	require.Equal(t, uint8(oleiosnotify.MessageTypeBlockAnnouncement), msg.Type())
	announcement, ok := msg.(*oleiosnotify.MsgBlockAnnouncement)
	require.True(t, ok)
	require.Equal(t, raw, []byte(announcement.BlockHeaderRaw))
}
