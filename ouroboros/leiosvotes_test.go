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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	oleiosvotes "github.com/blinklabs-io/gouroboros/protocol/leiosvotes"
	"github.com/stretchr/testify/require"
)

func testLeiosVote(slot uint64, hashFill byte, voterId uint64) oleiosvotes.Vote {
	signature := make([]byte, lcommon.LeiosBlsSignatureSize)
	for idx := range signature {
		signature[idx] = byte(idx) ^ hashFill
	}
	return oleiosvotes.Vote{
		SlotNo:            slot,
		EndorserBlockHash: lcommon.NewBlake2b256([]byte{hashFill}),
		VoterId:           voterId,
		VoteSignature:     signature,
	}
}

type leiosVoteWaitResult struct {
	votes []oleiosvotes.Vote
	err   error
}

func TestLeiosVotesClientRequestSizeIsIncremental(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	cfg := oleiosvotes.NewConfig(o.leiosvotesClientConnOpts()...)
	require.Equal(t, uint64(1), cfg.RequestNextCount)
}

func TestLeiosVotesServerRequestNextUsesCursor(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	vote1 := testLeiosVote(10, 0xaa, 1)
	vote2 := testLeiosVote(11, 0xbb, 2)
	require.NoError(t, o.storeLeiosVote(vote1))
	require.NoError(t, o.storeLeiosVote(vote2))

	got, err := o.leiosvotesServerRequestNext(
		oleiosvotes.CallbackContext{},
		1,
	)
	require.NoError(t, err)
	require.Equal(t, []oleiosvotes.Vote{vote1}, got)

	got, err = o.leiosvotesServerRequestNext(
		oleiosvotes.CallbackContext{},
		1,
	)
	require.NoError(t, err)
	require.Equal(t, []oleiosvotes.Vote{vote2}, got)

	got, err = o.leiosvotesServerRequestNext(
		oleiosvotes.CallbackContext{},
		1,
	)
	require.ErrorIs(t, err, errLeiosVotesUnavailable)
	require.Nil(t, got)
}

func TestLeiosVotesRequestNextDoesNotAdvanceCursorOnShortRead(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	vote1 := testLeiosVote(10, 0xaa, 1)
	vote2 := testLeiosVote(11, 0xbb, 2)
	require.NoError(t, o.storeLeiosVote(vote1))

	got, err := o.leiosvotesServerRequestNext(
		oleiosvotes.CallbackContext{},
		2,
	)
	require.ErrorIs(t, err, errLeiosVotesUnavailable)
	require.Nil(t, got)

	require.NoError(t, o.storeLeiosVote(vote2))
	got, err = o.leiosvotesServerRequestNext(
		oleiosvotes.CallbackContext{},
		2,
	)
	require.NoError(t, err)
	require.Equal(t, []oleiosvotes.Vote{vote1, vote2}, got)
}

func TestLeiosVotesRequestNextWaitsForCachedVote(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	done := make(chan struct{})
	resultCh := make(chan leiosVoteWaitResult, 1)
	connId := oleiosvotes.CallbackContext{}.ConnectionId

	go func() {
		got, err := o.waitLeiosVotesForRequestNext(connId, 1, done)
		resultCh <- leiosVoteWaitResult{votes: got, err: err}
	}()

	testutil.RequireNoReceive(
		t,
		resultCh,
		50*time.Millisecond,
		"empty cache should leave the leios-votes pull outstanding",
	)
	vote := testLeiosVote(10, 0xaa, 1)
	require.NoError(t, o.storeLeiosVote(vote))
	result := testutil.RequireReceive(
		t,
		resultCh,
		2*time.Second,
		"leios-votes waiter should wake when a vote is cached",
	)
	require.NoError(t, result.err)
	require.Equal(t, []oleiosvotes.Vote{vote}, result.votes)
}

func TestLeiosVotesRequestNextWaitStopsOnDone(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	done := make(chan struct{})
	resultCh := make(chan leiosVoteWaitResult, 1)
	connId := oleiosvotes.CallbackContext{}.ConnectionId

	go func() {
		got, err := o.waitLeiosVotesForRequestNext(connId, 1, done)
		resultCh <- leiosVoteWaitResult{votes: got, err: err}
	}()

	close(done)
	result := testutil.RequireReceive(
		t,
		resultCh,
		2*time.Second,
		"leios-votes waiter should stop when the protocol is done",
	)
	require.ErrorIs(t, result.err, protocol.ErrProtocolShuttingDown)
	require.Nil(t, result.votes)
}

func TestLeiosVoteCacheDedupesFullVoteIdentity(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	vote := testLeiosVote(10, 0xaa, 1)
	require.NoError(t, o.storeLeiosVote(vote))
	require.NoError(t, o.storeLeiosVote(vote))

	got, err := o.leiosvotesServerRequestNext(
		oleiosvotes.CallbackContext{},
		2,
	)
	require.ErrorIs(t, err, errLeiosVotesUnavailable)
	require.Nil(t, got)

	got, err = o.leiosvotesServerRequestNext(
		oleiosvotes.CallbackContext{},
		1,
	)
	require.NoError(t, err)
	require.Equal(t, []oleiosvotes.Vote{vote}, got)
}

func TestLeiosVoteCacheAllowsSameVoterDifferentEndorserBlock(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	vote1 := testLeiosVote(10, 0xaa, 1)
	vote2 := testLeiosVote(10, 0xbb, 1)
	require.NoError(t, o.storeLeiosVote(vote1))
	require.NoError(t, o.storeLeiosVote(vote2))

	got, err := o.leiosvotesServerRequestNext(
		oleiosvotes.CallbackContext{},
		2,
	)
	require.NoError(t, err)
	require.Equal(t, []oleiosvotes.Vote{vote1, vote2}, got)
}

func TestLeiosVoteCacheRejectsInvalidVote(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	err := o.storeLeiosVote(oleiosvotes.Vote{
		SlotNo:            10,
		EndorserBlockHash: lcommon.NewBlake2b256([]byte{0xaa}),
		VoterId:           1,
		VoteSignature:     []byte{0x01},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "VoteSignature")
}

func TestLeiosFetchServerVotesRequestReturnsCachedVotes(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	matching := testLeiosVote(10, 0xaa, 1)
	other := testLeiosVote(10, 0xbb, 2)
	require.NoError(t, o.storeLeiosVote(matching))
	require.NoError(t, o.storeLeiosVote(other))

	msg, err := o.leiosfetchServerVotesRequest(
		oleiosfetch.CallbackContext{},
		[]oleiosfetch.MsgVotesRequestVoteId{{
			SlotNo:  matching.SlotNo,
			VoterId: matching.VoterId,
		}},
	)
	require.NoError(t, err)
	resp, ok := msg.(*oleiosfetch.MsgVotes)
	require.True(t, ok)
	votes, err := resp.DecodeVotes()
	require.NoError(t, err)
	require.Len(t, votes, 1)
	require.Equal(t, matching.SlotNo, votes[0].SlotNo)
	require.Equal(t, matching.EndorserBlockHash, votes[0].EndorserBlockHash)
	require.Equal(t, matching.VoterId, votes[0].VoterId)
	require.Equal(t, matching.VoteSignature, votes[0].VoteSignature)
}

func TestLeiosVoteLookupExpiresStaleEntries(t *testing.T) {
	o := NewOuroboros(OuroborosConfig{EnableLeios: true})
	vote := testLeiosVote(10, 0xaa, 1)
	require.NoError(t, o.storeLeiosVote(vote))

	o.leiosMu.Lock()
	cached := o.leiosVotes[leiosVoteKey(vote)]
	if cached != nil {
		cached.insertedAt = time.Now().
			Add(-leiosVoteCacheTTL - time.Second)
	}
	o.leiosMu.Unlock()
	require.NotNil(t, cached)

	got, err := o.leiosvotesServerRequestNext(
		oleiosvotes.CallbackContext{},
		1,
	)
	require.True(t, errors.Is(err, errLeiosVotesUnavailable))
	require.Nil(t, got)

	o.leiosMu.RLock()
	cacheEntries := len(o.leiosVotes)
	o.leiosMu.RUnlock()
	require.Zero(t, cacheEntries)
}
