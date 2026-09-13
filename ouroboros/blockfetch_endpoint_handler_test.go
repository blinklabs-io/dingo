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
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	gouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	oblockfetch "github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/gouroboros/protocol/handshake"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/stretchr/testify/require"
)

// TestBlockfetchServerRequestRangeRejectsInvalidEnd exercises the handler
// path that validates the end point after accepting a valid start point. The
// sender-level tests cannot cover this branch because they bypass the handler.
func TestBlockfetchServerRequestRangeRejectsInvalidEnd(t *testing.T) {
	ledgerState := newTestLedgerState(t)
	blocks, err := testfixtures.GenerateConwayChain(3)
	require.NoError(t, err)
	for _, block := range blocks {
		require.NoError(t, ledgerState.Chain().AddBlock(block, nil))
	}
	// Keep block 1 as an actual rolled-back point. Chain.FromPoint rejects
	// this point through its membership check even though BlockByPoint can
	// still resolve the historical block from the manager cache.
	require.NoError(t, ledgerState.Chain().Rollback(ocommon.NewPoint(
		blocks[0].SlotNumber(),
		blocks[0].Hash().Bytes(),
	)))

	point := func(index int) ocommon.Point {
		return ocommon.NewPoint(
			blocks[index].SlotNumber(),
			blocks[index].Hash().Bytes(),
		)
	}
	start := point(0)
	validEnd := point(0)
	missingEndHash := make([]byte, 32)
	copy(missingEndHash, "missing-end")
	cases := []struct {
		name string
		end  ocommon.Point
	}{
		{
			name: "missing end",
			end:  ocommon.NewPoint(validEnd.Slot, missingEndHash),
		},
		{
			name: "rolled-back fork end",
			end:  point(1),
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			bus := event.NewEventBus(nil, logger)
			t.Cleanup(bus.Close)
			manager := connmanager.NewConnectionManager(
				connmanager.ConnectionManagerConfig{
					Logger:   logger,
					EventBus: bus,
				},
			)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(
					context.Background(),
					5*time.Second,
				)
				defer cancel()
				require.NoError(t, manager.Stop(ctx))
			})
			o := newOuroboros(OuroborosConfig{
				Logger: logger, EventBus: bus, ConnManager: manager,
			})
			o.ledgerState = ledgerState
			peer := newRegisteredBlockfetchServerPeer(t, o)

			// Prove the asynchronous sender is reachable through this exact
			// handler and connection before testing its endpoint rejection.
			assertValidBatch := func() {
				t.Helper()
				peer.send(t, oblockfetch.ProtocolId,
					oblockfetch.NewMsgRequestRange(start, validEnd))
				segment := peer.readResponse(t, 5*time.Second)
				require.Equal(
					t,
					oblockfetch.ProtocolId,
					segment.GetProtocolId(),
				)
				require.Equal(
					t,
					[]byte{0x81, oblockfetch.MessageTypeStartBatch},
					segment.Payload,
				)
				segment = peer.readResponse(t, 5*time.Second)
				require.Equal(
					t,
					oblockfetch.ProtocolId,
					segment.GetProtocolId(),
				)
				var msg oblockfetch.MsgBlock
				_, err := cbor.Decode(segment.Payload, &msg)
				require.NoError(t, err)
				require.Equal(
					t,
					uint8(oblockfetch.MessageTypeBlock),
					msg.Type(),
				)
				wrapped, err := cbor.Encode(
					[]any{blocks[0].Type(), cbor.RawMessage(blocks[0].Cbor())},
				)
				require.NoError(t, err)
				require.Equal(t, wrapped, msg.WrappedBlock)
				segment = peer.readResponse(t, 5*time.Second)
				require.Equal(
					t,
					oblockfetch.ProtocolId,
					segment.GetProtocolId(),
				)
				require.Equal(
					t,
					[]byte{0x81, oblockfetch.MessageTypeBatchDone},
					segment.Payload,
				)
			}
			assertValidBatch()
			peer.send(
				t,
				oblockfetch.ProtocolId,
				oblockfetch.NewMsgRequestRange(start, test.end),
			)
			segment := peer.readResponse(t, 5*time.Second)
			require.Equal(t, oblockfetch.ProtocolId, segment.GetProtocolId())
			require.Equal(
				t,
				[]byte{0x81, oblockfetch.MessageTypeNoBlocks},
				segment.Payload,
				"invalid end must be rejected before StartBatch",
			)
			// Rejection must also leave the same connection able to serve.
			assertValidBatch()
		})
	}
}

// newRegisteredBlockfetchServerPeer uses the shared handshake fixture and
// existing raw-wire peer with a full connection, as the asynchronous handler
// resolves that connection through the manager before sending StartBatch.
func newRegisteredBlockfetchServerPeer(
	t *testing.T,
	o *Ouroboros,
) *muxerServerPeer {
	t.Helper()
	serverPipe, peerPipe := net.Pipe()
	t.Cleanup(func() {
		_ = serverPipe.Close()
		_ = peerPipe.Close()
	})
	require.NoError(t, peerPipe.SetWriteDeadline(time.Now().Add(5*time.Second)))
	peer := &muxerServerPeer{peerConn: peerPipe}
	cfg, err := oblockfetch.NewConfig(o.blockfetchServerConnOpts()...)
	require.NoError(t, err)
	type result struct {
		conn *gouroboros.Connection
		err  error
	}
	ready := make(chan result, 1)
	go func() {
		conn, err := gouroboros.New(
			gouroboros.WithConnection(serverPipe),
			gouroboros.WithServer(true),
			gouroboros.WithNodeToNode(true),
			gouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
			gouroboros.WithLogger(o.config.Logger),
			gouroboros.WithBlockFetchConfig(cfg),
		)
		ready <- result{conn: conn, err: err}
	}()
	peer.send(t, handshake.ProtocolId,
		ouroboros_mock.ConversationEntryHandshakeRequestOutput.Messages[0])
	segment := peer.readResponse(t, 5*time.Second)
	require.Equal(t, uint16(handshake.ProtocolId), segment.GetProtocolId())
	var accepted handshake.MsgAcceptVersion
	_, err = cbor.Decode(segment.Payload, &accepted)
	require.NoError(t, err)
	require.Equal(t, uint8(handshake.MessageTypeAcceptVersion), accepted.Type())
	select {
	case r := <-ready:
		require.NoError(t, r.err)
		t.Cleanup(func() { _ = r.conn.Close() })
		require.True(t, o.connManager.AddConnection(
			r.conn, true, r.conn.Id().RemoteAddr.String()))
	case <-time.After(5 * time.Second):
		t.Fatal("server connection did not finish its handshake")
	}
	return peer
}
