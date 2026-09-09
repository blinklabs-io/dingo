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

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/cbor"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	"github.com/stretchr/testify/require"
)

func TestLeiosFetchVoteRequestBound(t *testing.T) {
	for _, count := range []int{0, 1, 1000, 1001} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			o := newOuroboros(OuroborosConfig{EnableLeios: true})
			handler := &fakeLeiosVoteHandler{rawVotes: []cbor.RawMessage{mustCbor(t, "vote")}}
			o.leiosVotes = handler
			ids := make([]oleiosfetch.MsgVotesRequestVoteId, count)
			msg, err := o.leiosfetchServerVotesRequest(oleiosfetch.CallbackContext{}, ids)
			if count > 1000 {
				require.ErrorContains(t, err, "vote ID request exceeds limit")
				require.Nil(t, msg)
				require.Empty(t, handler.requestedIds, "oversized request reached vote manager")
				return
			}
			require.NoError(t, err)
			require.Len(t, handler.requestedIds, count)
			require.Len(t, msg.(*oleiosfetch.MsgVotes).VotesRaw, 1)
		})
	}
}

func TestLeiosFetchVoteRequestBoundOnWire(t *testing.T) {
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	handler := &fakeLeiosVoteHandler{rawVotes: []cbor.RawMessage{mustCbor(t, "vote")}}
	o.leiosVotes = handler
	peer := newLeiosFetchServerPeer(t, o)
	peer.send(
		t,
		oleiosfetch.ProtocolId,
		oleiosfetch.NewMsgVotesRequest(make([]oleiosfetch.MsgVotesRequestVoteId, 1000)),
	)
	segment := peer.readResponse(t, 5*time.Second)
	msg, err := oleiosfetch.NewMsgFromCbor(oleiosfetch.MessageTypeVotes, segment.Payload)
	require.NoError(t, err)
	require.Len(t, msg.(*oleiosfetch.MsgVotes).VotesRaw, 1)
	peer.send(
		t,
		oleiosfetch.ProtocolId,
		oleiosfetch.NewMsgVotesRequest(make([]oleiosfetch.MsgVotesRequestVoteId, 1001)),
	)
	err = testutil.RequireReceive(t, peer.errChan, 5*time.Second, "oversized vote request rejected")
	require.ErrorContains(t, err, "vote ID request exceeds limit")
	handler.mu.Lock()
	defer handler.mu.Unlock()
	require.Len(t, handler.requestedIds, 1000, "only the accepted request reaches the manager")
}

func TestLeiosFetchLargeVoteRequestWithoutManager(t *testing.T) {
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	peer := newLeiosFetchServerPeer(t, o)
	peer.send(
		t,
		oleiosfetch.ProtocolId,
		oleiosfetch.NewMsgVotesRequest(make([]oleiosfetch.MsgVotesRequestVoteId, 1001)),
	)
	segment := peer.readResponse(t, 5*time.Second)
	require.Equal(t, []byte{0x82, oleiosfetch.MessageTypeVotes, 0x80}, segment.Payload)
	// A second request demonstrates that the unavailable-manager response
	// returned protocol agency instead of closing or parking the bearer.
	peer.send(t, oleiosfetch.ProtocolId, oleiosfetch.NewMsgVotesRequest(nil))
	segment = peer.readResponse(t, 5*time.Second)
	require.Equal(t, []byte{0x82, oleiosfetch.MessageTypeVotes, 0x80}, segment.Payload)
}
