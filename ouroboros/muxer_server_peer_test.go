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
	"errors"
	"io"
	"log/slog"
	"net"
	"slices"
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
	// pending holds segment payload bytes read but not yet returned by
	// readMessage, and pendingProtocolId the protocol those bytes belong to.
	pending           []byte
	pendingProtocolId uint16
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

// readMessage returns the protocol ID and encoded bytes of the next single
// protocol message, bounded by timeout.
//
// A muxer segment boundary is not a message boundary. gouroboros' protocol
// send loop drains everything already queued into one payload buffer and
// emits it as a single segment (up to maxMessagesPerSegment messages), and
// splits a payload larger than muxer.SegmentMaxPayloadLength across several
// segments. So whenever the server queues a second message before the send
// loop has decided the boundary for the first, both messages arrive in one
// segment; comparing a whole segment payload against one encoded message is
// therefore racy. readMessage reassembles the byte stream and hands back
// exactly one CBOR message per call, which is what the protocol actually
// guarantees.
//
// Do not mix readMessage and readResponse on the same peer: readMessage
// buffers whatever a segment carried past the message it returns, and
// readResponse would read the connection past that buffer.
func (p *muxerServerPeer) readMessage(
	t *testing.T,
	timeout time.Duration,
) (uint16, []byte) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if len(p.pending) > 0 {
			var raw cbor.RawMessage
			n, err := cbor.Decode(p.pending, &raw)
			switch {
			case err == nil:
				// Cap the returned slice so a later append for a
				// continuation segment cannot write into it.
				msg := p.pending[:n:n]
				p.pending = p.pending[n:]
				return p.pendingProtocolId, msg
			case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
				// Message split across segments; read the rest below.
			default:
				require.NoError(t, err, "decoding buffered segment payload")
			}
		}
		remaining := time.Until(deadline)
		require.Positive(
			t,
			remaining,
			"timed out waiting for a complete protocol message",
		)
		segment := p.readResponse(t, remaining)
		if len(p.pending) == 0 {
			p.pendingProtocolId = segment.GetProtocolId()
		} else {
			require.Equal(
				t,
				p.pendingProtocolId,
				segment.GetProtocolId(),
				"segment for a different protocol split a buffered message",
			)
		}
		p.pending = append(p.pending, segment.Payload...)
	}
}

// encodeSegment returns the wire bytes of one raw segment, so a test can
// present exactly the framing gouroboros' send loop is allowed to produce.
func encodeSegment(t *testing.T, protocolId uint16, payload []byte) []byte {
	t.Helper()
	segment := muxer.NewSegment(protocolId, payload, true)
	require.NotNil(t, segment)
	buf := &bytes.Buffer{}
	require.NoError(
		t,
		binary.Write(buf, binary.BigEndian, segment.SegmentHeader),
	)
	_, err := buf.Write(segment.Payload)
	require.NoError(t, err)
	return buf.Bytes()
}

// TestMuxerServerPeerReadMessage pins the framing readMessage exists for: a
// segment boundary is not a message boundary, so a segment may carry several
// messages and a message may span several segments. Asserting on whole
// segment payloads made
// TestBlockfetchServerRequestRangeRejectsInvalidEnd flaky whenever the
// blockfetch send loop batched StartBatch and the first block body together.
func TestMuxerServerPeerReadMessage(t *testing.T) {
	const protocolId = uint16(3)
	first, err := cbor.Encode([]any{uint(2)})
	require.NoError(t, err)
	second, err := cbor.Encode(
		[]any{uint(4), cbor.NewByteString(bytes.Repeat([]byte{0xab}, 64))},
	)
	require.NoError(t, err)
	third, err := cbor.Encode([]any{uint(5)})
	require.NoError(t, err)

	for _, test := range []struct {
		name     string
		segments [][]byte
	}{
		{
			name:     "one message per segment",
			segments: [][]byte{first, second, third},
		},
		{
			name: "all messages batched into one segment",
			segments: [][]byte{
				slices.Concat(first, second, third),
			},
		},
		{
			name: "message split across segments",
			segments: [][]byte{
				slices.Concat(first, second[:10]),
				slices.Concat(second[10:], third),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			serverConn, peerConn := net.Pipe()
			t.Cleanup(func() {
				_ = serverConn.Close()
				_ = peerConn.Close()
			})
			peer := &muxerServerPeer{peerConn: peerConn}
			// Encode on the test goroutine: only the blocking writes
			// belong in the writer, which cannot call require.
			wire := make([][]byte, 0, len(test.segments))
			for _, payload := range test.segments {
				wire = append(
					wire,
					encodeSegment(t, protocolId, payload),
				)
			}
			written := make(chan error, 1)
			go func() {
				for _, segment := range wire {
					if _, err := serverConn.Write(segment); err != nil {
						written <- err
						return
					}
				}
				written <- nil
			}()
			for _, want := range [][]byte{first, second, third} {
				gotProtocolId, got := peer.readMessage(t, 5*time.Second)
				require.Equal(t, protocolId, gotProtocolId)
				require.Equal(t, want, got)
			}
			select {
			case err := <-written:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("segment writer did not finish")
			}
		})
	}
}
