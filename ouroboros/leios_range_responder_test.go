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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/muxer"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	oleiosfetch "github.com/blinklabs-io/gouroboros/protocol/leiosfetch"
	"github.com/stretchr/testify/require"
)

// leiosFetchServerPeer drives Dingo's real leios-fetch server config
// (leiosfetchServerConnOpts, instrumentation wrappers included) over a real
// muxer, so the assertions are about what Dingo actually puts on the wire
// rather than about what a callback returns.
type leiosFetchServerPeer struct {
	o        *Ouroboros
	peerConn net.Conn
	errChan  chan error
}

func newLeiosFetchServerPeer(
	t *testing.T,
	o *Ouroboros,
) *leiosFetchServerPeer {
	t.Helper()
	serverConn, peerConn := net.Pipe()
	m := muxer.New(serverConn)
	errChan := make(chan error, 4)
	cfg := oleiosfetch.NewConfig(o.leiosfetchServerConnOpts()...)
	server := oleiosfetch.NewServer(
		protocol.ProtocolOptions{
			ConnectionId: gconnection.ConnectionId{
				LocalAddr:  serverConn.LocalAddr(),
				RemoteAddr: serverConn.RemoteAddr(),
			},
			ErrorChan: errChan,
			Muxer:     m,
			Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
		&cfg,
	)
	server.Start()
	m.Start()
	t.Cleanup(func() {
		server.Protocol.Stop()
		m.Stop()
		_ = serverConn.Close()
		_ = peerConn.Close()
	})
	return &leiosFetchServerPeer{o: o, peerConn: peerConn, errChan: errChan}
}

// send writes msg to the server as a leios-fetch request segment.
func (p *leiosFetchServerPeer) send(t *testing.T, msg protocol.Message) {
	t.Helper()
	data, err := cbor.Encode(msg)
	require.NoError(t, err)
	segment := muxer.NewSegment(oleiosfetch.ProtocolId, data, false)
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
// that the server leaves pending fails the test instead of hanging it.
func (p *leiosFetchServerPeer) readResponse(
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

// TestLeiosFetchBlockRangeRequestIsDeclined is the Dingo-owned half of issue
// #3623. Dingo registers a BlockRangeRequestFunc but does not serve ranges.
// gouroboros reads a nil return from that callback as "an async process was
// started that will send NextBlockAndTxsInRange / LastBlockAndTxsInRange", so
// returning nil without sending anything left this server holding leios-fetch
// agency in StateBlockRange forever.
//
// A peer in that state is wedged permanently: its protocol send loop waits on
// agency the state map only returns when the missing response arrives, so it
// can never issue another leios-fetch request on the connection and has no way
// to detect the condition. Dingo must decline observably instead.
//
// There is no absence reply for a range request, so declining means a
// connection-level protocol error. That error is what this asserts, bounded by
// a timeout because the defect being fixed is a park.
func TestLeiosFetchBlockRangeRequestIsDeclined(t *testing.T) {
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	peer := newLeiosFetchServerPeer(t, o)

	peer.send(t, oleiosfetch.NewMsgBlockRangeRequest(
		ocommon.NewPoint(3623, []byte{0x01, 0x02}),
		ocommon.NewPoint(3700, []byte{0x03, 0x04}),
	))

	select {
	case err := <-peer.errChan:
		require.Error(t, err)
		require.Contains(t, err.Error(), "block range")
	case <-time.After(5 * time.Second):
		t.Fatal(
			"leios-fetch BlockRangeRequest was left pending instead of declined",
		)
	}
}

// TestLeiosFetchUnavailableBlockTxsAnswersNoBlockTxs is the absence case for
// the test above: a request Dingo cannot satisfy but CAN answer must still be
// answered on the wire, not escalated into a connection error. This keeps the
// range fix from being read as "decline anything unavailable".
func TestLeiosFetchUnavailableBlockTxsAnswersNoBlockTxs(t *testing.T) {
	o := newOuroboros(OuroborosConfig{EnableLeios: true})
	peer := newLeiosFetchServerPeer(t, o)

	// No endorser block is stored, so the callback reports
	// ErrBlockTxsNotFound and the server answers MsgNoBlockTxs.
	peer.send(t, oleiosfetch.NewMsgBlockTxsRequest(
		ocommon.NewPoint(3623, make([]byte, lcommon.Blake2b256Size)),
		map[uint16]uint64{0: 1 << 63},
	))

	segment := peer.readResponse(t, 5*time.Second)
	require.True(t, segment.IsResponse())
	require.Equal(t, oleiosfetch.ProtocolId, segment.GetProtocolId())
	require.Equal(
		t,
		[]byte{0x81, oleiosfetch.MessageTypeNoBlockTxs},
		segment.Payload,
	)
	select {
	case err := <-peer.errChan:
		require.NoError(t, err, "unavailable EB txs must not fail the bearer")
	default:
	}
}
