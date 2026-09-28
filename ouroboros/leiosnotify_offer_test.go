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
	t.Parallel()

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

func TestLeiosForgedEBOfferSetsTransactionOfferType(t *testing.T) {
	t.Parallel()

	point := ocommon.Point{Slot: 42, Hash: []byte("eb-hash")}
	msg := leiosForgedEBOffer(&leiosForgedEBEntry{txOffer: &point})
	require.NotNil(t, msg)
	require.Equal(t, uint8(oleiosnotify.MessageTypeBlockTxsOffer), msg.Type())
	offer, ok := msg.(*oleiosnotify.MsgBlockTxsOffer)
	require.True(t, ok)
	require.Equal(t, point, offer.Point)
}

func TestLeiosRelayForwardsVerifiedManifestAndCompleteTransactionsOnce(t *testing.T) {
	t.Parallel()

	txRaw, err := cbor.Encode([]cbor.RawMessage{mustCbor(t, "tx-body")})
	require.NoError(t, err)
	ebRaw, err := cbor.Encode(&lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{{
			TransactionHash: lcommon.Blake2b256Hash(txRaw),
			TransactionSize: uint16(len(txRaw)), //nolint:gosec // short test transaction
		}},
	})
	require.NoError(t, err)
	ebHash := lcommon.Blake2b256Hash(ebRaw)
	point := ocommon.NewPoint(42, ebHash.Bytes())

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	o.leiosEBLog.registerConn("downstream", nil, nil)

	require.NoError(t, o.storeLeiosEndorserBlock(
		point, ebRaw, nil, leiosStorePeerOffered,
	))
	entry, _ := o.leiosEBLog.next("downstream")
	require.Nil(t, entry, "unverified peer slots must not be relayed")

	announceTestEndorserBlock(t, o, point.Slot, ebHash, len(ebRaw))
	manifest, _ := o.leiosEBLog.next("downstream")
	require.NotNil(t, manifest)
	manifestMessage, ok := leiosForgedEBOffer(manifest).(*oleiosnotify.MsgBlockOffer)
	require.True(t, ok)
	require.Equal(t, point, manifestMessage.Point)
	o.leiosEBLog.complete("downstream", nil, true)

	entry, _ = o.leiosEBLog.next("downstream")
	require.Nil(t, entry, "transaction offer must wait for complete bodies")

	txsRaw := []cbor.RawMessage{cbor.RawMessage(txRaw)}
	require.NoError(t, o.storeLeiosEndorserBlock(
		point, ebRaw, txsRaw, leiosStorePeerOffered,
	))
	transactions, _ := o.leiosEBLog.next("downstream")
	require.NotNil(t, transactions)
	transactionMessage, ok := leiosForgedEBOffer(transactions).(*oleiosnotify.MsgBlockTxsOffer)
	require.True(t, ok)
	require.Equal(t, point, transactionMessage.Point)
	o.leiosEBLog.complete("downstream", nil, true)

	require.NoError(t, o.storeLeiosEndorserBlock(
		point, ebRaw, txsRaw, leiosStorePeerOffered,
	))
	entry, _ = o.leiosEBLog.next("downstream")
	require.Nil(t, entry, "repeated peer offers must not be re-enqueued")
}

func TestLeiosRelayOffersExistingCompleteCacheOnPeerOffer(t *testing.T) {
	t.Parallel()

	txRaw, err := cbor.Encode([]cbor.RawMessage{mustCbor(t, "tx-body")})
	require.NoError(t, err)
	ebRaw, err := cbor.Encode(&lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{{
			TransactionHash: lcommon.Blake2b256Hash(txRaw),
			TransactionSize: uint16(len(txRaw)), //nolint:gosec // short test transaction
		}},
	})
	require.NoError(t, err)
	ebHash := lcommon.Blake2b256Hash(ebRaw)
	point := ocommon.NewPoint(42, ebHash.Bytes())
	txsRaw := []cbor.RawMessage{cbor.RawMessage(txRaw)}

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	t.Cleanup(func() { require.NoError(t, o.Close()) })
	o.leiosEBLog.registerConn("downstream", nil, nil)
	require.NoError(t, o.storeLeiosEndorserBlock(
		point, ebRaw, txsRaw, leiosStoreAuthoritative,
	))

	o.markLeiosEndorserBlockRelayOffer(point)
	manifest, _ := o.leiosEBLog.next("downstream")
	require.NotNil(t, manifest)
	_, ok := leiosForgedEBOffer(manifest).(*oleiosnotify.MsgBlockOffer)
	require.True(t, ok)
	o.leiosEBLog.complete("downstream", nil, true)
	transactions, _ := o.leiosEBLog.next("downstream")
	require.NotNil(t, transactions)
	_, ok = leiosForgedEBOffer(transactions).(*oleiosnotify.MsgBlockTxsOffer)
	require.True(t, ok)
	o.leiosEBLog.complete("downstream", nil, true)

	o.markLeiosEndorserBlockRelayOffer(point)
	entry, _ := o.leiosEBLog.next("downstream")
	require.Nil(t, entry, "a cache hit must not duplicate an existing offer")
}

func TestLeiosForgedEBLogOffersManifestBeforeTransactions(t *testing.T) {
	t.Parallel()

	log := newLeiosForgedEBLog()
	log.registerConn("peer", nil, nil)
	point := ocommon.Point{Slot: 42, Hash: []byte("eb-hash")}
	log.append(leiosForgedEBEntry{point: &point, size: 1234})
	log.append(leiosForgedEBEntry{txOffer: &point})

	manifest, _ := log.next("peer")
	require.Equal(t,
		uint8(oleiosnotify.MessageTypeBlockOffer),
		leiosForgedEBOffer(manifest).Type(),
	)
	log.complete("peer", nil, true)

	transactions, _ := log.next("peer")
	require.Equal(t,
		uint8(oleiosnotify.MessageTypeBlockTxsOffer),
		leiosForgedEBOffer(transactions).Type(),
	)
}

// A locally emitted vote must be offered as a MsgVotesOffer with its type set.
func TestLeiosForgedEBOfferSetsVotesOfferType(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	require.Nil(t, leiosForgedEBOffer(&leiosForgedEBEntry{}))
}

func TestLeiosForgedEBOfferAnnouncement(t *testing.T) {
	t.Parallel()

	raw := []byte{0x82, 0x01, 0x02}
	msg := leiosForgedEBOffer(&leiosForgedEBEntry{announcement: raw})
	require.NotNil(t, msg)
	require.Equal(
		t,
		uint8(oleiosnotify.MessageTypeBlockAnnouncement),
		msg.Type(),
	)
	announcement, ok := msg.(*oleiosnotify.MsgBlockAnnouncement)
	require.True(t, ok)
	require.Equal(t, raw, []byte(announcement.BlockHeaderRaw))
}
