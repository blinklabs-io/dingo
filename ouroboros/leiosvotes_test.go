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
	"sync"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	oleiosvotes "github.com/blinklabs-io/gouroboros/protocol/leiosvotes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type handledVote struct {
	connKey string
	vote    lcommon.LeiosVote
}

type handledEb struct {
	slot   uint64
	ebHash lcommon.Blake2b256
}

type fakeLeiosVoteHandler struct {
	mu           sync.Mutex
	votes        []handledVote
	nextVotes    []lcommon.LeiosVote
	nextErr      error
	nextRequests []uint64
	rawVotes     []cbor.RawMessage
	requestedIds []lcommon.LeiosVoteId
	ebs          []handledEb
	removed      []string
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

func (f *fakeLeiosVoteHandler) NextVotes(
	done <-chan struct{},
	connKey string,
	count uint64,
) ([]lcommon.LeiosVote, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextRequests = append(f.nextRequests, count)
	if f.nextErr != nil {
		return nil, f.nextErr
	}
	return f.nextVotes, nil
}

func (f *fakeLeiosVoteHandler) VotesByIds(
	ids []lcommon.LeiosVoteId,
) []cbor.RawMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requestedIds = append(f.requestedIds, ids...)
	return f.rawVotes
}

func (f *fakeLeiosVoteHandler) HandleEndorserBlock(
	slot uint64,
	ebHash lcommon.Blake2b256,
) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ebs = append(f.ebs, handledEb{slot: slot, ebHash: ebHash})
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
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	votes, err := o.leiosvotesServerRequestNext(
		oleiosvotes.CallbackContext{},
		3,
	)
	require.ErrorIs(t, err, errLeiosVotesUnavailable)
	assert.Nil(t, votes)
}

func TestLeiosVotesServerRequestNextDelegates(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	handler := &fakeLeiosVoteHandler{
		nextVotes: []lcommon.LeiosVote{
			testLeiosVote(0),
			testLeiosVote(1),
		},
	}
	o.LeiosVotes = handler

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

func TestLeiosVotesClientVoteDelegates(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	handler := &fakeLeiosVoteHandler{}
	o.LeiosVotes = handler

	vote := testLeiosVote(7)
	require.NoError(
		t,
		o.leiosvotesClientVote(oleiosvotes.CallbackContext{}, vote),
	)
	require.Len(t, handler.votes, 1)
	assert.Equal(t, uint64(7), handler.votes[0].vote.VoterId)
}

func TestLeiosVotesClientVoteWithoutHandlerLogsOnly(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(
		t,
		o.leiosvotesClientVote(
			oleiosvotes.CallbackContext{},
			testLeiosVote(7),
		),
	)
}

func TestLeiosFetchServerVotesRequestDelegates(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	raw := mustCbor(t, "vote-cbor")
	handler := &fakeLeiosVoteHandler{
		rawVotes: []cbor.RawMessage{raw},
	}
	o.LeiosVotes = handler

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
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	msg, err := o.leiosfetchServerVotesRequest(
		oleiosfetch.CallbackContext{},
		[]oleiosfetch.MsgVotesRequestVoteId{{SlotNo: 1, VoterId: 2}},
	)
	require.ErrorIs(t, err, errLeiosVotesUnavailable)
	assert.Nil(t, msg)
}

func TestStoreLeiosEndorserBlockNotifiesVoteHandler(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 10)
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	handler := &fakeLeiosVoteHandler{}
	o.LeiosVotes = handler

	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))
	require.Len(t, handler.ebs, 1)
	assert.Equal(t, uint64(10), handler.ebs[0].slot)
	assert.Equal(t, point.Hash, handler.ebs[0].ebHash.Bytes())
}

// A peer must not be able to make us vote for the same EB hash under a
// different slot by replaying the same EB bytes with a different point.
func TestStoreLeiosEndorserBlockRejectsSlotMismatchBeforeVote(
	t *testing.T,
) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 10)
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	handler := &fakeLeiosVoteHandler{}
	o.LeiosVotes = handler

	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))

	mismatchedPoint := point
	mismatchedPoint.Slot++
	err := o.storeLeiosEndorserBlock(mismatchedPoint, blockRaw, nil)
	require.ErrorContains(
		t,
		err,
		"leios endorser block cache: point slot mismatch",
	)
	require.Len(t, handler.ebs, 1)
	assert.Equal(t, uint64(10), handler.ebs[0].slot)
}

func TestStoreLeiosEndorserBlockWithoutHandler(t *testing.T) {
	point, blockRaw := testLeiosEndorserBlockRaw(t, 11)
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	require.NoError(t, o.storeLeiosEndorserBlock(point, blockRaw, nil))
}

func TestLeiosVotesClientRequestSizeIsIncremental(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	cfg := oleiosvotes.NewConfig(o.leiosvotesClientConnOpts()...)
	require.Equal(t, uint64(1), cfg.RequestNextCount)
}
