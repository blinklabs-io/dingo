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

package peergov

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/blinklabs-io/gouroboros/protocol/handshake"
	ouroboros_mock "github.com/blinklabs-io/ouroboros-mock"
	"github.com/stretchr/testify/require"
)

// newMockNtNHandshakeConn returns the remote end of an NtN connection that
// accepts the handshake and advertises the given peer-sharing mode.
func newMockNtNHandshakeConn(peerSharing bool) net.Conn {
	mode := uint(protocol.PeerSharingModeNoPeerSharing)
	if peerSharing {
		mode = protocol.PeerSharingModePeerSharingPublic
	}
	response := ouroboros_mock.ConversationEntryOutput{
		ProtocolId: handshake.ProtocolId,
		IsResponse: true,
		Messages: []protocol.Message{
			handshake.NewMsgAcceptVersion(
				ouroboros_mock.MockProtocolVersionNtN,
				protocol.VersionDataNtN13andUp{
					VersionDataNtN11to12: protocol.VersionDataNtN11to12{
						CborNetworkMagic:                       ouroboros_mock.MockNetworkMagic,
						CborInitiatorAndResponderDiffusionMode: protocol.DiffusionModeInitiatorOnly,
						CborPeerSharing:                        mode,
						CborQuery:                              protocol.QueryModeDisabled,
					},
				},
			),
		},
	}
	return ouroboros_mock.NewConnection(
		ouroboros_mock.ProtocolRoleClient,
		[]ouroboros_mock.ConversationEntry{
			ouroboros_mock.ConversationEntryHandshakeRequestGeneric,
			response,
		},
	)
}

// newMockNtNConnection returns a handshaken NtN connection whose peer
// advertises the given peer-sharing mode.
func newMockNtNConnection(
	t *testing.T,
	peerSharing bool,
) *ouroboros.Connection {
	t.Helper()
	oConn, err := ouroboros.New(
		ouroboros.WithConnection(newMockNtNHandshakeConn(peerSharing)),
		ouroboros.WithNetworkMagic(ouroboros_mock.MockNetworkMagic),
		ouroboros.WithNodeToNode(true),
		ouroboros.WithKeepAlive(false),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = oConn.Close() })
	return oConn
}

func TestAddPeer_GossipPeerNotSharableBeforeConnection(t *testing.T) {
	t.Parallel()
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	require.NoError(t, pg.AddPeer("44.0.0.1:3001", PeerSourceP2PGossip))
	peers := pg.GetPeers()
	require.Len(t, peers, 1)
	require.False(t, peers[0].Sharable)
}

func TestRecordOutboundConnected_SharableAfterSuccessfulConnection(
	t *testing.T,
) {
	t.Parallel()
	tests := []struct {
		name        string
		source      PeerSource
		peerSharing bool
		want        bool
	}{
		{"gossip peer sharing enabled", PeerSourceP2PGossip, true, true},
		{"gossip peer sharing disabled", PeerSourceP2PGossip, false, true},
		{"ledger peer sharing disabled", PeerSourceP2PLedger, false, true},
		{
			"topology peer keeps configured flag",
			PeerSourceTopologyLocalRoot, true, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			peer := &Peer{
				Address: "44.0.0.1:3001",
				Source:  tt.source,
				State:   PeerStateCold,
			}
			peer.recordOutboundConnected(
				newMockNtNConnection(t, tt.peerSharing),
				time.Now(),
			)
			require.True(t, peer.EverConnected)
			require.True(t, peer.hasClientConnection())
			require.Equal(t, tt.want, peer.Sharable)
		})
	}
}

// TestCreateOutboundConnection_MarksDiscoveredPeerSharable drives the real
// outbound dial path, so it fails if the success branch stops recording the
// connection through recordOutboundConnected.
func TestCreateOutboundConnection_MarksDiscoveredPeerSharable(t *testing.T) {
	t.Parallel()
	for _, source := range []PeerSource{
		PeerSourceP2PGossip,
		PeerSourceP2PLedger,
	} {
		t.Run(source.String(), func(t *testing.T) {
			t.Parallel()
			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			connMgr := connmanager.NewConnectionManager(
				connmanager.ConnectionManagerConfig{
					Logger: logger,
					OutboundConnOpts: []ouroboros.ConnectionOptionFunc{
						ouroboros.WithNetworkMagic(
							ouroboros_mock.MockNetworkMagic,
						),
						ouroboros.WithNodeToNode(true),
						ouroboros.WithKeepAlive(false),
					},
					OutboundDialer: func(
						context.Context,
						string,
					) (net.Conn, error) {
						return newMockNtNHandshakeConn(false), nil
					},
				},
			)
			pg := NewPeerGovernor(PeerGovernorConfig{
				Logger:      logger,
				EventBus:    newMockEventBus(),
				ConnManager: connMgr,
			})
			peer := &Peer{
				Address:           "44.0.0.1:3001",
				NormalizedAddress: "44.0.0.1:3001",
				Source:            source,
				State:             PeerStateCold,
			}
			pg.mu.Lock()
			pg.ctx = t.Context()
			pg.stopCh = make(chan struct{})
			pg.peers = []*Peer{peer}
			pg.mu.Unlock()
			t.Cleanup(func() {
				_ = pg.Stop(context.Background())
				_ = connMgr.Stop(context.Background())
			})

			pg.createOutboundConnection(peer, false)

			pg.mu.Lock()
			everConnected := peer.EverConnected
			sharable := peer.Sharable
			pg.mu.Unlock()
			require.True(t, everConnected)
			require.True(
				t,
				sharable,
				"a discovered peer is sharable once an outbound dial succeeds",
			)
		})
	}
}

func TestDiscoverLedgerPeers_NotSharableBeforeConnection(t *testing.T) {
	t.Parallel()
	ipv4 := net.ParseIP("44.0.0.1")
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays:      []PoolRelay{{IPv4: &ipv4, Port: 3001}},
			currentSlot: 1000,
		},
	})
	pg.discoverLedgerPeers()
	peers := pg.GetPeers()
	require.Len(t, peers, 1)
	require.Equal(t, PeerSource(PeerSourceP2PLedger), peers[0].Source)
	require.False(
		t,
		peers[0].Sharable,
		"a registered relay is not sharable before an outbound connection",
	)
}

func TestHandleInboundConnection_EphemeralAddressNotSharable(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{Logger: logger},
	)
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:      logger,
		ConnManager: connManager,
	})
	oConn := newMockNtNConnection(t, true)
	connId := oConn.Id()
	address := connId.RemoteAddr.String()
	require.True(t, connManager.AddConnection(oConn, true, address))

	pg.handleInboundConnectionEvent(event.Event{
		Type: connmanager.InboundConnectionEventType,
		Data: connmanager.InboundConnectionEvent{
			ConnectionId:         connId,
			LocalAddr:            connId.LocalAddr,
			RemoteAddr:           connId.RemoteAddr,
			NormalizedRemoteAddr: connmanager.NormalizePeerAddr(address),
			IsDuplex:             true,
		},
	})

	peers := pg.GetPeers()
	require.Len(t, peers, 1)
	require.Equal(t, PeerSource(PeerSourceInboundConn), peers[0].Source)
	require.NotNil(
		t,
		peers[0].Connection,
		"the handshake data must have been read for the entry",
	)
	require.True(t, peers[0].Connection.VersionData.PeerSharing())
	require.False(
		t,
		peers[0].Sharable,
		"an inbound remote address is not proven to be a listening address",
	)
}
