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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	gouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	oleiosnotify "github.com/blinklabs-io/gouroboros/protocol/leiosnotify"
	oleiosvotes "github.com/blinklabs-io/gouroboros/protocol/leiosvotes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type handledVote struct {
	connKey string
	vote    lcommon.LeiosVote
}

type handledPrototypeVote struct {
	connKey string
	vote    lcommon.LeiosPrototypeVote
}

type handledEb struct {
	slot   uint64
	rbHash lcommon.Blake2b256
	ebHash lcommon.Blake2b256
}

type fakeLeiosVoteHandler struct {
	mu             sync.Mutex
	votes          []handledVote
	prototypeVotes []handledPrototypeVote
	nextVotes      []lcommon.LeiosVote
	nextErr        error
	nextRequests   []uint64
	rawVotes       []cbor.RawMessage
	requestedIds   []lcommon.LeiosVoteId
	ebs            []handledEb
	removed        []string
	waitStarted    chan struct{}
}

func (f *fakeLeiosVoteHandler) HandleVote(
	connKey string,
	vote lcommon.LeiosVote,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.votes = append(f.votes, handledVote{connKey: connKey, vote: vote})
	return nil
}

func (f *fakeLeiosVoteHandler) HandlePrototypeVote(
	connKey string,
	vote lcommon.LeiosPrototypeVote,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.prototypeVotes = append(
		f.prototypeVotes,
		handledPrototypeVote{connKey: connKey, vote: vote},
	)
	return nil
}

func (f *fakeLeiosVoteHandler) ObserveAnnouncement(
	_ uint64,
	_ lcommon.Blake2b256,
	_ lcommon.Blake2b256,
) {
}

func (f *fakeLeiosVoteHandler) NextVotes(
	done <-chan struct{},
	connKey string,
	count uint64,
) ([]lcommon.LeiosVote, error) {
	f.mu.Lock()
	f.nextRequests = append(f.nextRequests, count)
	waitStarted := f.waitStarted
	nextErr := f.nextErr
	nextVotes := f.nextVotes
	f.mu.Unlock()
	if waitStarted != nil {
		close(waitStarted)
		<-done
		return nil, errors.New("vote request canceled")
	}
	if nextErr != nil {
		return nil, nextErr
	}
	return nextVotes, nil
}

func (f *fakeLeiosVoteHandler) VotesByIds(
	ids []lcommon.LeiosVoteId,
) []cbor.RawMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requestedIds = append(f.requestedIds, ids...)
	return f.rawVotes
}

func (f *fakeLeiosVoteHandler) HandleValidatedEndorserBlock(
	slot uint64,
	rbHash lcommon.Blake2b256,
	ebHash lcommon.Blake2b256,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ebs = append(f.ebs, handledEb{
		slot: slot, rbHash: rbHash, ebHash: ebHash,
	})
}

func requireHandledEndorserBlock(
	t *testing.T,
	handler *fakeLeiosVoteHandler,
	want handledEb,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		handler.mu.Lock()
		defer handler.mu.Unlock()
		return len(handler.ebs) == 1 && handler.ebs[0] == want
	}, time.Second, time.Millisecond)
}

func (f *fakeLeiosVoteHandler) RemoveConnection(connKey string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, connKey)
}

func testLeiosVote(voterId uint64) lcommon.LeiosVote {
	return lcommon.LeiosVote{
		SlotNo:            123,
		EndorserBlockHash: lcommon.NewBlake2b256([]byte("eb")),
		VoterId:           voterId,
		VoteSignature:     make([]byte, lcommon.LeiosBlsSignatureSize),
	}
}

func TestLeiosVotesServerRequestNextUnavailableWithoutHandler(t *testing.T) {
	t.Parallel()

	// This test exercises the vote notification plumbing without enabling the
	// semantic transaction gate, which requires a real ledger snapshot.
	o := newOuroboros(OuroborosConfig{EnableLeios: false})
	votes, err := o.leiosvotesServerRequestNext(
		oleiosvotes.CallbackContext{},
		3,
	)
	require.ErrorIs(t, err, errLeiosVotesUnavailable)
	assert.Nil(t, votes)
}

func TestLeiosVotesServerRequestNextDelegates(t *testing.T) {
	t.Parallel()

	// Keep this focused on occurrence binding; semantic validation is covered
	// separately with an injected ledger validator.
	o := newOuroboros(OuroborosConfig{EnableLeios: false})
	handler := &fakeLeiosVoteHandler{
		nextVotes: []lcommon.LeiosVote{
			testLeiosVote(0),
			testLeiosVote(1),
		},
	}
	o.leiosVotes = handler

	votes, err := o.leiosvotesServerRequestNext(
		oleiosvotes.CallbackContext{},
		2,
	)
	require.NoError(t, err)
	require.Len(t, votes, 2)
	assert.Equal(t, uint64(0), votes[0].VoterId)
	assert.Equal(t, uint64(1), votes[1].VoterId)
	assert.Equal(t, []uint64{2}, handler.nextRequests)
}

func TestLeiosVotesServerRequestNextRejectsOversizedCount(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{EnableLeios: false})
	handler := &fakeLeiosVoteHandler{}
	o.leiosVotes = handler

	votes, err := o.leiosvotesServerRequestNext(
		oleiosvotes.CallbackContext{},
		oleiosvotes.MaxRequestNextCount+1,
	)
	require.Error(t, err)
	assert.Nil(t, votes)
	handler.mu.Lock()
	defer handler.mu.Unlock()
	assert.Empty(t, handler.nextRequests)
}

func TestLeiosVotesServerRequestNextCanceledByProtocolStop(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	started := make(chan struct{})
	o.leiosVotes = &fakeLeiosVoteHandler{waitStarted: started}
	serverConfig := oleiosvotes.NewConfig()
	server := oleiosvotes.NewServer(
		protocol.ProtocolOptions{},
		&serverConfig,
	)
	result := make(chan error, 1)
	go func() {
		_, err := o.leiosvotesServerRequestNext(
			oleiosvotes.CallbackContext{Server: server},
			1,
		)
		result <- err
	}()
	<-started
	server.Stop()

	select {
	case err := <-result:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("vote request remained blocked after protocol stop")
	}
}

func TestLeiosVotesServerRequestNextCanceledOnConnectionClose(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	started := make(chan struct{})
	o.leiosVotes = &fakeLeiosVoteHandler{waitStarted: started}
	connId := newTestConnId("127.0.0.1:3000", "127.0.0.2:3001")
	connectionDone := make(chan any)
	result := make(chan error, 1)
	go func() {
		_, err := o.leiosvotesServerRequestNext(
			oleiosvotes.CallbackContext{
				ConnectionId:       connId,
				ConnectionDoneChan: connectionDone,
			},
			1,
		)
		result <- err
	}()
	<-started
	close(connectionDone)

	select {
	case err := <-result:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("vote request remained blocked after connection close")
	}
}

func TestLeiosVotesClientVoteDelegates(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	handler := &fakeLeiosVoteHandler{}
	o.leiosVotes = handler

	vote := testLeiosVote(7)
	require.NoError(
		t,
		o.leiosvotesClientVote(oleiosvotes.CallbackContext{}, vote),
	)
	require.Len(t, handler.votes, 1)
	assert.Equal(t, uint64(7), handler.votes[0].vote.VoterId)
}

func TestLeiosNotifyPrototypeVoteDelegates(t *testing.T) {
	t.Parallel()

	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	conn, err := gouroboros.New()
	require.NoError(t, err)
	require.True(t, cm.AddConnection(conn, false, "127.0.0.1:1234"))
	defer func() {
		conn.ErrorChan() <- errors.New("test connection closed")
	}()
	handler := &fakeLeiosVoteHandler{}
	o := newOuroboros(OuroborosConfig{
		ConnManager: cm,
		EnableLeios: true,
	})
	o.leiosVotes = handler
	vote := lcommon.LeiosPrototypeVote{
		AnnouncingRbHash: lcommon.NewBlake2b256([]byte("announcing-rb")),
		VoterId:          7,
		VoteSignature:    make([]byte, lcommon.LeiosBlsSignatureSize),
	}

	err = o.leiosnotifyClientNotification(
		oleiosnotify.CallbackContext{ConnectionId: conn.Id()},
		oleiosnotify.NewMsgVotesOfferPrototype(
			[]lcommon.LeiosPrototypeVote{vote},
		),
	)
	require.NoError(t, err)
	require.Len(t, handler.prototypeVotes, 1)
	assert.Equal(
		t,
		leiosConnectionIdString(conn.Id()),
		handler.prototypeVotes[0].connKey,
	)
	assert.Equal(t, vote, handler.prototypeVotes[0].vote)
}

func TestLeiosVotesClientVoteWithoutHandlerLogsOnly(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.leiosvotesClientVote(
			oleiosvotes.CallbackContext{},
			testLeiosVote(7),
		),
	)
}

func TestLeiosFetchServerVotesRequestDelegates(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	raw := mustCbor(t, "vote-cbor")
	handler := &fakeLeiosVoteHandler{
		rawVotes: []cbor.RawMessage{raw},
	}
	o.leiosVotes = handler

	ids := []oleiosfetch.MsgVotesRequestVoteId{
		{SlotNo: 123, VoterId: 0},
		{SlotNo: 123, VoterId: 9},
	}
	msg, err := o.leiosfetchServerVotesRequest(
		oleiosfetch.CallbackContext{},
		ids,
	)
	require.NoError(t, err)
	votesMsg, ok := msg.(*oleiosfetch.MsgVotes)
	require.True(t, ok)
	require.Len(t, votesMsg.VotesRaw, 1)
	assert.Equal(t, []lcommon.LeiosVoteId(ids), handler.requestedIds)
}

func TestLeiosFetchServerVotesRequestWithoutHandler(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	msg, err := o.leiosfetchServerVotesRequest(
		oleiosfetch.CallbackContext{},
		[]oleiosfetch.MsgVotesRequestVoteId{{SlotNo: 1, VoterId: 2}},
	)
	require.NoError(t, err)
	votesMsg, ok := msg.(*oleiosfetch.MsgVotes)
	require.True(t, ok)
	assert.Empty(t, votesMsg.VotesRaw)
	wire, err := cbor.Encode(votesMsg)
	require.NoError(t, err)
	assert.Equal(t, []byte{0x82, 0x0b, 0x80}, wire)
}

func TestStoreLeiosEndorserBlockNotifiesVoteHandler(t *testing.T) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 10, 1)
	txsRaw := []cbor.RawMessage{testDijkstraTx(t, 0)}
	o := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LeiosAnnouncementLedger: &fakeLeiosAnnouncementLedger{},
	})
	defer func() { require.NoError(t, o.Close()) }()
	handler := &fakeLeiosVoteHandler{}
	o.leiosVotes = handler

	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			txsRaw,
			leiosStoreAuthoritative,
		),
	)
	require.Empty(t, handler.ebs)
	rbHash := announceTestEndorserBlock(
		t, o, point.Slot, testEbHash(point), len(blockRaw),
	)
	requireHandledEndorserBlock(t, handler, handledEb{
		slot: point.Slot, rbHash: rbHash, ebHash: testEbHash(point),
	})
}

// A peer must not be able to make us vote for the same EB hash under a
// different slot by replaying the same EB bytes with a different point. An
// authoritative source (unlike a peer) may still override a mismatched entry
// -- covered separately in tests_test.go -- since the
// manifest is content-addressed and the same hash can legitimately recur at
// a different slot; this guards the peer-offered side of that distinction.
// TestStoreLeiosEndorserBlockDifferentSlotOfSameHashStaysUnverifiedBeforeVote
// is the multi-slot-aware successor to the old
// "...RejectsSlotMismatchBeforeVote" test: a peer-offered store for the same
// hash at a different, unannounced slot is no longer rejected -- the
// manifest is content-addressed, so that occurrence can be independently
// legitimate -- but it must stay unverified and must
// not trigger a second vote for a slot nothing has corroborated.
func TestStoreLeiosEndorserBlockDifferentSlotOfSameHashStaysUnverifiedBeforeVote(
	t *testing.T,
) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 10, 1)
	txsRaw := []cbor.RawMessage{testDijkstraTx(t, 0)}
	o := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LeiosAnnouncementLedger: &fakeLeiosAnnouncementLedger{},
	})
	defer func() { require.NoError(t, o.Close()) }()
	handler := &fakeLeiosVoteHandler{}
	o.leiosVotes = handler

	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			txsRaw,
			leiosStoreAuthoritative,
		),
	)
	rbHash := announceTestEndorserBlock(
		t, o, point.Slot, testEbHash(point), len(blockRaw),
	)
	requireHandledEndorserBlock(t, handler, handledEb{
		slot: point.Slot, rbHash: rbHash, ebHash: testEbHash(point),
	})

	otherSlotPoint := point
	otherSlotPoint.Slot++
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			otherSlotPoint,
			blockRaw,
			txsRaw,
			leiosStorePeerOffered,
		),
	)
	data, ok := o.lookupLeiosEndorserBlock(
		otherSlotPoint.Slot,
		otherSlotPoint.Hash,
	)
	require.True(t, ok)
	require.False(
		t,
		data.slotVerified,
		"an unannounced occurrence at a different slot must not be trusted",
	)
	handler.mu.Lock()
	defer handler.mu.Unlock()
	require.Len(t, handler.ebs, 1,
		"the unverified second occurrence must not trigger a second vote")
}

func TestLeiosAnnouncementBindsCompleteCachedBlockAtNewSlot(t *testing.T) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 10, 1)
	txsRaw := []cbor.RawMessage{testDijkstraTx(t, 0)}
	o := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LeiosAnnouncementLedger: &fakeLeiosAnnouncementLedger{},
	})
	defer func() { require.NoError(t, o.Close()) }()
	handler := &fakeLeiosVoteHandler{}
	o.leiosVotes = handler

	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		txsRaw,
		leiosStoreAuthoritative,
	))

	announcedPoint := point
	announcedPoint.Slot++
	_, ok := o.lookupLeiosEndorserBlock(
		announcedPoint.Slot,
		announcedPoint.Hash,
	)
	require.False(t, ok)
	require.Empty(t, handler.ebs)

	rbHash := announceTestEndorserBlock(
		t,
		o,
		announcedPoint.Slot,
		testEbHash(announcedPoint),
		len(blockRaw),
	)

	data, ok := o.lookupLeiosEndorserBlock(
		announcedPoint.Slot,
		announcedPoint.Hash,
	)
	require.True(t, ok)
	require.True(t, data.slotVerified)
	require.NotEmpty(t, data.announcementHeaderRaw)
	require.Equal(t, txsRaw, data.txsRaw)
	requireHandledEndorserBlock(t, handler, handledEb{
		slot:   announcedPoint.Slot,
		rbHash: rbHash,
		ebHash: testEbHash(announcedPoint),
	})
}

func TestLeiosAnnouncementCompletesExistingRelayIntent(t *testing.T) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRawWithRefs(t, 20, 1)
	txsRaw := []cbor.RawMessage{testDijkstraTx(t, 0)}
	o := newOuroboros(OuroborosConfig{
		EnableLeios:             true,
		LeiosAnnouncementLedger: &fakeLeiosAnnouncementLedger{},
	})
	defer func() { require.NoError(t, o.Close()) }()
	handler := &fakeLeiosVoteHandler{}
	o.leiosVotes = handler

	require.NoError(t, o.storeLeiosEndorserBlock(
		point,
		blockRaw,
		txsRaw,
		leiosStoreAuthoritative,
	))

	announcedPoint := point
	announcedPoint.Slot++
	o.markLeiosEndorserBlockRelayOffer(announcedPoint)
	o.markLeiosEndorserBlockRelayOfferFrom(announcedPoint, "peer-a")
	o.leiosMu.RLock()
	intent := o.leiosEndorserBlocks[leiosBlockKey(
		announcedPoint.Slot,
		announcedPoint.Hash,
	)]
	o.leiosMu.RUnlock()
	require.NotNil(t, intent)
	require.False(t, intent.completeTxCache())
	require.False(t, intent.slotVerified)
	require.True(t, intent.relayOfferRequested)
	require.Equal(t, "peer-a", intent.relayOfferSource)
	require.Empty(t, handler.ebs)

	rbHash := announceTestEndorserBlock(
		t,
		o,
		announcedPoint.Slot,
		testEbHash(announcedPoint),
		len(blockRaw),
	)

	data, ok := o.lookupLeiosEndorserBlock(
		announcedPoint.Slot,
		announcedPoint.Hash,
	)
	require.True(t, ok)
	require.True(t, data.completeTxCache())
	require.True(t, data.slotVerified)
	require.NotEmpty(t, data.announcementHeaderRaw)
	require.True(t, data.relayOfferRequested)
	requireHandledEndorserBlock(t, handler, handledEb{
		slot:   announcedPoint.Slot,
		rbHash: rbHash,
		ebHash: testEbHash(announcedPoint),
	})
	require.Equal(t, "peer-a", data.relayOfferSource)
	require.Equal(t, txsRaw, data.txsRaw)
}

func TestStoreLeiosEndorserBlockWithoutHandler(t *testing.T) {
	t.Parallel()

	point, blockRaw := testLeiosEndorserBlockRaw(t, 11)
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			blockRaw,
			nil,
			leiosStoreAuthoritative,
		),
	)
}

func TestLeiosVotesClientRequestSizeIsIncremental(t *testing.T) {
	t.Parallel()

	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	cfg := oleiosvotes.NewConfig(o.leiosvotesClientConnOpts()...)
	require.Equal(t, uint64(1), cfg.RequestNextCount)
}
