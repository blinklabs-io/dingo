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
	"fmt"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	gouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/stretchr/testify/require"
)

const abandonedLeiosFetchError = "leios-fetch request slot awaiting abandoned response"

func newLeiosFetchConversation(
	t *testing.T,
	conversation []ouroboros_mock.ConversationEntry,
) (*gouroboros.Connection, <-chan error) {
	t.Helper()
	mockConn := ouroboros_mock.NewConnection(
		ouroboros_mock.ProtocolRoleClient,
		conversation,
	)
	mockErrCh := make(chan error, 1)
	go func() {
		err := <-mockConn.(*ouroboros_mock.Connection).ErrorChan()
		if err != nil {
			mockErrCh <- fmt.Errorf("mock connection: %w", err)
		}
		close(mockErrCh)
	}()

	conn, err := gouroboros.New(
		gouroboros.WithConnection(mockConn),
		gouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
		gouroboros.WithNodeToNode(true),
	)
	require.NoError(t, err)

	return conn, mockErrCh
}

func requireLeiosFetchConversationDone(t *testing.T, mockErrCh <-chan error) {
	t.Helper()
	select {
	case err := <-mockErrCh:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("mock leios-fetch conversation did not finish")
	}
}

func leiosFetchHandshake() []ouroboros_mock.ConversationEntry {
	return []ouroboros_mock.ConversationEntry{
		ouroboros_mock.ConversationEntryHandshakeRequestGeneric,
		ouroboros_mock.ConversationEntryHandshakeNtNResponse,
	}
}

// TestLeiosBlockTxsAbandonedSlotFailsOverAndBecomesAvailable reproduces the
// Dingo #3622 path. A response lost after the peer accepts a BlockTxsRequest
// leaves the request slot abandoned. The same connection must fail in bounded
// time so backfill can move to another peer, whose MsgBlockTxs response then
// makes the certified endorser block available to both ledger and serving.
func TestLeiosBlockTxsAbandonedSlotFailsOverAndBecomesAvailable(t *testing.T) {
	tx, ref := testLeiosManifestTx(t, 0x22)
	manifestRaw, err := lcommon.LeiosEndorserBlock{
		TransactionReferences: []lcommon.LeiosTransactionReference{ref},
	}.MarshalCBOR()
	require.NoError(t, err)
	point := ocommon.NewPoint(
		3622,
		lcommon.Blake2b256Hash(manifestRaw).Bytes(),
	)
	bitmap := map[uint16]uint64{0: 1 << 63}

	abandonedConn, abandonedDone := newLeiosFetchConversation(
		t,
		append(
			leiosFetchHandshake(),
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockTxsRequest,
			},
		),
	)
	healthyConn, healthyDone := newLeiosFetchConversation(
		t,
		append(
			leiosFetchHandshake(),
			ouroboros_mock.ConversationEntryInput{
				ProtocolId:  leiosfetch.ProtocolId,
				MessageType: leiosfetch.MessageTypeBlockTxsRequest,
			},
			ouroboros_mock.ConversationEntryOutput{
				ProtocolId: leiosfetch.ProtocolId,
				IsResponse: true,
				Messages: []protocol.Message{
					leiosfetch.NewMsgBlockTxsFull(
						point,
						bitmap,
						[]cbor.RawMessage{tx},
					),
				},
			},
		),
	)
	cm := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{},
	)
	require.True(t, cm.AddConnection(abandonedConn, false, "abandoned"))
	require.True(t, cm.AddConnection(healthyConn, false, "healthy"))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		require.NoError(t, cm.Stop(ctx))
	})

	o := newOuroboros(OuroborosConfig{
		ConnManager: cm,
		EnableLeios: true,
	})
	require.NoError(
		t,
		o.storeLeiosEndorserBlock(
			point,
			manifestRaw,
			nil,
			leiosStoreAuthoritative,
		),
	)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		50*time.Millisecond,
	)
	resp, err := abandonedConn.LeiosFetch().Client.BlockTxsRequest(
		ctx,
		point,
		bitmap,
	)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, resp)

	deadline := time.Now().Add(1500 * time.Millisecond)
	txs, err := o.fetchLeiosEbTxsBatchedUntil(
		abandonedConn.LeiosFetch().Client,
		point,
		1,
		manifestRaw,
		deadline,
	)
	require.Empty(t, txs)
	require.EqualError(t, err, abandonedLeiosFetchError)

	// Make the poisoned connection the first backfill candidate. Its bounded
	// abandoned-slot error must cool it down and let the healthy peer complete.
	o.leiosFetchGuardFor(abandonedConn.Id()).markFetchOK()
	require.NoError(t, o.FetchEndorserBlockByPoint(point.Slot, point.Hash))
	require.True(
		t,
		o.leiosFetchGuardFor(abandonedConn.Id()).inCooldown(time.Now()),
		"abandoned peer was not recorded as failed",
	)
	require.True(
		t,
		o.leiosFetchGuardFor(healthyConn.Id()).recentlySucceeded(
			time.Now(),
			leiosBackfillAffinityWindow,
		),
		"healthy peer was not recorded as successful",
	)

	ledgerTxs, ok := o.EndorserBlockTxsByHash(point.Hash, point.Slot)
	require.True(t, ok, "ledger provider still reports EB unavailable")
	require.Equal(t, []cbor.RawMessage{tx}, ledgerTxs)

	served, err := o.leiosfetchServerBlockTxsRequest(
		leiosfetch.CallbackContext{},
		point,
		bitmap,
	)
	require.NoError(t, err)
	servedTxs, ok := served.(*leiosfetch.MsgBlockTxs)
	require.True(t, ok)
	require.Equal(t, []cbor.RawMessage{tx}, servedTxs.TxsRaw)

	requireLeiosFetchConversationDone(t, abandonedDone)
	requireLeiosFetchConversationDone(t, healthyDone)
}
