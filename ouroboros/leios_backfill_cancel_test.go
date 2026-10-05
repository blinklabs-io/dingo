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
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/stretchr/testify/require"
)

// newBackfillCancelOuroboros wires one leios-fetch connection whose peer
// accepts one request of the given message type (none when requestType is
// negative) and never answers it.
func newBackfillCancelOuroboros(
	t *testing.T,
	requestType int,
) (*Ouroboros, *connmanager.ConnectionManager, <-chan error) {
	t.Helper()
	conversation := leiosFetchHandshake()
	if requestType >= 0 {
		conversation = append(
			conversation,
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: uint(requestType),
			},
		)
	}
	conn, done := newLeiosFetchConversation(t, conversation)
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	require.True(t, cm.AddConnection(conn, false, "peer"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})
	o := newOuroboros(OuroborosConfig{ConnManager: cm, EnableLeios: true})
	return o, cm, done
}

func requireBackfillConnUnpenalized(
	t *testing.T,
	o *Ouroboros,
	cm *connmanager.ConnectionManager,
) {
	t.Helper()
	for _, connId := range cm.LeiosFetchConnectionIds() {
		g := o.leiosFetchGuardFor(connId)
		require.False(
			t,
			g.inCooldown(time.Now()),
			"caller cancellation must not cool down a healthy connection",
		)
		require.False(t, g.isProtocolDead())
	}
}

// TestFetchEndorserBlockByPointCancelledBeforeStartIsNotAPeerFailure shows that
// a caller context cancelled before the call returns its error, sends the peer
// nothing, and leaves the connection unpenalized.
func TestFetchEndorserBlockByPointCancelledBeforeStartIsNotAPeerFailure(
	t *testing.T,
) {
	t.Parallel()

	o, cm, done := newBackfillCancelOuroboros(t, -1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := o.FetchEndorserBlockByPoint(
		ctx,
		3412,
		make([]byte, lcommon.Blake2b256Size),
	)
	require.ErrorIs(t, err, context.Canceled)
	requireBackfillConnUnpenalized(t, o, cm)
	requireLeiosFetchConversationDone(t, done)
}

// TestFetchEndorserBlockByPointCancelledInFlightReturnsPromptly shows that
// cancelling the caller while a peer holds a request unanswered ends the whole
// call with the cancellation, well inside the fetch budget, and leaves the
// connection unpenalized: cancellation is not a peer failure. It covers both
// the manifest request and the transaction request that follows a cached
// manifest.
func TestFetchEndorserBlockByPointCancelledInFlightReturnsPromptly(
	t *testing.T,
) {
	t.Parallel()

	t.Run("manifest request", func(t *testing.T) {
		t.Parallel()
		o, cm, received := newBackfillCancelOuroboros(
			t, leiosfetch.MessageTypeBlockRequest,
		)
		cancelBackfillInFlight(t, o, cm, received, ocommon.NewPoint(
			3412, make([]byte, lcommon.Blake2b256Size),
		))
	})
	t.Run("transaction request", func(t *testing.T) {
		t.Parallel()
		o, cm, received := newBackfillCancelOuroboros(
			t, leiosfetch.MessageTypeBlockTxsRequest,
		)
		_, ref := testLeiosManifestTx(t, 0x34)
		manifestRaw, err := lcommon.LeiosEndorserBlock{
			TransactionReferences: []lcommon.LeiosTransactionReference{ref},
		}.MarshalCBOR()
		require.NoError(t, err)
		point := ocommon.NewPoint(
			3412, lcommon.Blake2b256Hash(manifestRaw).Bytes(),
		)
		require.NoError(t, o.storeLeiosEndorserBlock(
			point, manifestRaw, nil, leiosStoreAuthoritative,
		))
		cancelBackfillInFlight(t, o, cm, received, point)
	})
}

func cancelBackfillInFlight(
	t *testing.T,
	o *Ouroboros,
	cm *connmanager.ConnectionManager,
	received <-chan error,
	point ocommon.Point,
) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result := make(chan error, 1)
	go func() {
		result <- o.FetchEndorserBlockByPoint(ctx, point.Slot, point.Hash)
	}()
	// The conversation ends only after the peer has received the request.
	requireLeiosFetchConversationDone(t, received)
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("cancelled backfill fetch did not return")
	}
	requireBackfillConnUnpenalized(t, o, cm)
}
