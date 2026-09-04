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
	"bytes"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	gconnection "github.com/blinklabs-io/gouroboros/connection"
	"github.com/blinklabs-io/gouroboros/muxer"
	"github.com/blinklabs-io/gouroboros/protocol"
	"github.com/stretchr/testify/require"
)

// muxerServer is the subset of a gouroboros protocol server
// (*blockfetch.Server, *leiosfetch.Server, ...) that muxerServerPeer needs to
// start and stop. Every protocol package's Server embeds *protocol.Protocol,
// which defines both methods and is promoted onto the Server, so any of them
// satisfies this interface unchanged.
type muxerServer interface {
	Start()
	Stop()
}

// muxerServerPeer drives a real Dingo server-side protocol implementation
// (blockfetch, leios-fetch, ...) over a real net.Pipe/muxer pair, so
// assertions are about what Dingo actually puts on the wire rather than
// about what a callback returns directly. Each protocol still builds its own
// Config/Server -- gouroboros gives each protocol package distinct concrete
// types with no shared constructor -- but the net.Pipe/muxer plumbing and the
// send/readResponse wire mechanics below are identical across protocols, so
// this type is shared; each protocol-specific test file supplies only its
// own NewConfig/NewServer call (see newLeiosFetchServerPeer,
// newBlockfetchServerPeer).
type muxerServerPeer struct {
	peerConn net.Conn
	errChan  chan error
	muxer    *muxer.Muxer
}

// newMuxerServerPeer creates the net.Pipe pair and muxer, and returns the
// protocol.ProtocolOptions every protocol-specific *Server constructor needs
// (blockfetch.NewServer, leiosfetch.NewServer, ...) alongside the peer side.
// Build the protocol's Config/Server from opts, then call peer.start with it.
func newMuxerServerPeer(
	t *testing.T,
) (opts protocol.ProtocolOptions, peer *muxerServerPeer) {
	t.Helper()
	serverConn, peerConn := net.Pipe()
	m := muxer.New(serverConn)
	errChan := make(chan error, 4)
	opts = protocol.ProtocolOptions{
		ConnectionId: gconnection.ConnectionId{
			LocalAddr:  serverConn.LocalAddr(),
			RemoteAddr: serverConn.RemoteAddr(),
		},
		ErrorChan: errChan,
		Muxer:     m,
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	peer = &muxerServerPeer{peerConn: peerConn, errChan: errChan, muxer: m}
	t.Cleanup(func() {
		m.Stop()
		_ = serverConn.Close()
		_ = peerConn.Close()
	})
	return opts, peer
}

// start starts the caller's protocol server, then the muxer -- gouroboros
// requires the server to register itself with the muxer before the muxer
// starts dispatching -- and arranges for the server to stop during
// t.Cleanup. t.Cleanup runs LIFO, and this is registered after
// newMuxerServerPeer's own cleanup, so server.Stop still runs before the
// muxer/connection teardown that call registered, preserving the original
// stop order.
func (p *muxerServerPeer) start(t *testing.T, server muxerServer) {
	t.Helper()
	server.Start()
	p.muxer.Start()
	t.Cleanup(server.Stop)
}

// send writes msg to the server as a request segment for the given protocol.
func (p *muxerServerPeer) send(
	t *testing.T,
	protocolId uint16,
	msg protocol.Message,
) {
	t.Helper()
	data, err := cbor.Encode(msg)
	require.NoError(t, err)
	segment := muxer.NewSegment(protocolId, data, false)
	require.NotNil(t, segment)
	buf := &bytes.Buffer{}
	require.NoError(
		t,
		binary.Write(buf, binary.BigEndian, segment.SegmentHeader),
	)
	_, err = buf.Write(segment.Payload)
	require.NoError(t, err)
	_, err = p.peerConn.Write(buf.Bytes())
	require.NoError(t, err)
}

// readResponse reads one response segment, bounded by timeout so a request
// the server leaves pending fails the test instead of hanging it.
func (p *muxerServerPeer) readResponse(
	t *testing.T,
	timeout time.Duration,
) *muxer.Segment {
	t.Helper()
	require.NoError(t, p.peerConn.SetReadDeadline(time.Now().Add(timeout)))
	header := muxer.SegmentHeader{}
	require.NoError(t, binary.Read(p.peerConn, binary.BigEndian, &header))
	payload := make([]byte, header.PayloadLength)
	_, err := io.ReadFull(p.peerConn, payload)
	require.NoError(t, err)
	return &muxer.Segment{SegmentHeader: header, Payload: payload}
}
