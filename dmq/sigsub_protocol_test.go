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

package dmq

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/connection"
	"github.com/blinklabs-io/gouroboros/muxer"
	"github.com/blinklabs-io/gouroboros/protocol"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sigID(b byte) []byte { return fill(b, 32) }

func ids(bs ...byte) [][]byte {
	out := make([][]byte, 0, len(bs))
	for _, b := range bs {
		out = append(out, sigID(b))
	}
	return out
}

func idsAndSizes(bs ...byte) []ocommon.MessageIDAndSize {
	out := make([]ocommon.MessageIDAndSize, 0, len(bs))
	for _, b := range bs {
		out = append(out, ocommon.MessageIDAndSize{
			MessageID: sigID(b), SizeInBytes: 400,
		})
	}
	return out
}

// sigMsgWithID is a message whose ID is the given byte repeated.
func sigMsgWithID(b byte) ocommon.DmqMessage {
	msg, _ := goldenMessage()
	msg.MessageID = sigID(b)
	msg.Payload.MessageID = sigID(b)
	return msg
}

func TestValidateIDRequest(t *testing.T) {
	t.Parallel()
	out := ids(1, 2, 3)
	tests := []struct {
		name        string
		outstanding [][]byte
		req         idRequest
		limit       int
		wantErr     string
	}{
		{"blocking with nothing outstanding", nil,
			idRequest{blocking: true, req: 5}, 10, ""},
		{"blocking that acknowledges everything", out,
			idRequest{blocking: true, ack: 3, req: 5}, 10, ""},
		{"non-blocking with IDs left", out,
			idRequest{ack: 1, req: 2}, 10, ""},
		{"non-blocking that only acknowledges", out,
			idRequest{ack: 1}, 10, ""},
		{"acknowledging more than is outstanding", out,
			idRequest{ack: 4, req: 1}, 10, "acknowledgement 4 exceeds 3"},
		{"blocking that asks for nothing", nil,
			idRequest{blocking: true}, 10, "invalid message ID request counts"},
		{"non-blocking that does nothing", out,
			idRequest{}, 10, "invalid message ID request counts"},
		{"blocking with IDs left", out,
			idRequest{blocking: true, ack: 1, req: 1}, 10,
			"blocking request leaves 2"},
		{"non-blocking with nothing left", out,
			idRequest{ack: 3, req: 1}, 10,
			"non-blocking request leaves no message IDs"},
		{"exceeding the limit", out,
			idRequest{ack: 1, req: 9}, 10, "would leave 11"},
		{"reaching the limit", out,
			idRequest{ack: 1, req: 8}, 10, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateIDRequest(tt.outstanding, tt.req, tt.limit)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			assert.ErrorIs(t, err, protocol.ErrProtocolViolationRequestExceeded)
		})
	}
}

func TestReconcileIDs(t *testing.T) {
	t.Parallel()
	t.Run("acknowledges a prefix and appends the reply", func(t *testing.T) {
		t.Parallel()
		next, err := reconcileIDs(
			ids(1, 2, 3), idRequest{ack: 2, req: 2},
			idsAndSizes(4, 5), 10,
		)
		require.NoError(t, err)
		assert.Equal(t, ids(3, 4, 5), next)
	})
	t.Run("a short reply is fine", func(t *testing.T) {
		t.Parallel()
		next, err := reconcileIDs(
			ids(1), idRequest{ack: 0, req: 5}, nil, 10,
		)
		require.NoError(t, err)
		assert.Equal(t, ids(1), next)
	})
	t.Run("more than was asked", func(t *testing.T) {
		t.Parallel()
		_, err := reconcileIDs(
			nil, idRequest{blocking: true, req: 1}, idsAndSizes(1, 2), 10,
		)
		require.ErrorIs(t, err, protocol.ErrProtocolViolationRequestExceeded)
	})
	t.Run("empty reply to a blocking request", func(t *testing.T) {
		t.Parallel()
		_, err := reconcileIDs(
			nil, idRequest{blocking: true, req: 1}, nil, 10,
		)
		require.ErrorIs(t, err, protocol.ErrProtocolViolationInvalidMessage)
	})
	t.Run("repeating an outstanding ID", func(t *testing.T) {
		t.Parallel()
		_, err := reconcileIDs(
			ids(1, 2), idRequest{ack: 0, req: 1}, idsAndSizes(2), 10,
		)
		require.ErrorIs(t, err, protocol.ErrProtocolViolationInvalidMessage)
	})
	t.Run("repeating an ID within the reply", func(t *testing.T) {
		t.Parallel()
		_, err := reconcileIDs(
			nil, idRequest{blocking: true, req: 2}, idsAndSizes(1, 1), 10,
		)
		require.ErrorIs(t, err, protocol.ErrProtocolViolationInvalidMessage)
	})
	t.Run("an acknowledged ID may come back", func(t *testing.T) {
		t.Parallel()
		next, err := reconcileIDs(
			ids(1), idRequest{blocking: true, ack: 1, req: 1},
			idsAndSizes(1), 10,
		)
		require.NoError(t, err)
		assert.Equal(t, ids(1), next)
	})
}

func TestRequestedSigsOutstanding(t *testing.T) {
	t.Parallel()
	out := ids(1, 2, 3)
	_, err := requestedSigsOutstanding(out, ids(1, 3))
	require.NoError(t, err)
	_, err = requestedSigsOutstanding(out, nil)
	require.NoError(t, err)
	_, err = requestedSigsOutstanding(out, ids(9))
	require.ErrorIs(t, err, protocol.ErrProtocolViolationInvalidMessage)
	_, err = requestedSigsOutstanding(out, ids(1, 1))
	require.ErrorIs(t, err, protocol.ErrProtocolViolationInvalidMessage)
	_, err = requestedSigsOutstanding(out, ids(1, 2, 3, 4))
	require.ErrorIs(t, err, protocol.ErrProtocolViolationRequestExceeded)
}

func TestValidateSigReply(t *testing.T) {
	t.Parallel()
	req, err := requestedSigsOutstanding(ids(1, 2, 3), ids(1, 2))
	require.NoError(t, err)
	one, two, nine := sigMsgWithID(1), sigMsgWithID(2), sigMsgWithID(9)

	require.NoError(t, validateSigReply(req,
		[]ocommon.DmqMessage{one, two}))
	require.NoError(t, validateSigReply(req, []ocommon.DmqMessage{two}))
	require.NoError(t, validateSigReply(req, nil))
	require.ErrorIs(t, validateSigReply(nil, nil),
		protocol.ErrProtocolViolationInvalidMessage)
	require.ErrorIs(t, validateSigReply(req, []ocommon.DmqMessage{nine}),
		protocol.ErrProtocolViolationInvalidMessage)
	require.ErrorIs(t, validateSigReply(req, []ocommon.DmqMessage{one, one}),
		protocol.ErrProtocolViolationInvalidMessage)
	require.ErrorIs(t,
		validateSigReply(req, []ocommon.DmqMessage{one, two, one}),
		protocol.ErrProtocolViolationRequestExceeded)
}

// sigPair is a pulling side and a serving side joined over a real muxer, with
// no handshake, so the protocol can be driven message by message.
type sigPair struct {
	puller    *sigPuller
	server    *sigServer
	pullerErr chan error
	serverErr chan error
}

func newSigPair(
	t *testing.T,
	pullerCfg sigPullerConfig,
	serverCfg sigServerConfig,
) *sigPair {
	t.Helper()
	c1, c2 := tcpPair(t)
	m1, m2 := muxer.New(c1), muxer.New(c2)
	pair := &sigPair{
		pullerErr: make(chan error, 8),
		serverErr: make(chan error, 8),
	}
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	done := make(chan any)
	newOpts := func(
		m *muxer.Muxer,
		errs chan error,
		local, remote net.Addr,
	) protocol.ProtocolOptions {
		return protocol.ProtocolOptions{
			ConnectionId: connection.ConnectionId{
				LocalAddr: local, RemoteAddr: remote,
			},
			ConnectionDoneChan: done,
			Muxer:              m,
			Logger:             logger,
			ErrorChan:          errs,
			Mode:               protocol.ProtocolModeNodeToNode,
			Version:            protocol.ProtocolVersionDMQNtN2,
		}
	}
	o1 := newOpts(m1, pair.pullerErr, c1.LocalAddr(), c1.RemoteAddr())
	o2 := newOpts(m2, pair.serverErr, c2.LocalAddr(), c2.RemoteAddr())
	if pullerCfg.maxUnacked == 0 {
		pullerCfg.maxUnacked = 10
	}
	if serverCfg.maxUnacked == 0 {
		serverCfg.maxUnacked = 10
	}
	pair.puller = newSigPuller(o1, pullerCfg)
	pair.server = newSigServer(o2, serverCfg)
	t.Cleanup(func() {
		pair.puller.Stop()
		pair.server.Stop()
		m1.Stop()
		m2.Stop()
		_ = c1.Close()
		_ = c2.Close()
	})
	pair.server.EnsureRegistered()
	pair.puller.Start()
	pair.server.Start()
	m1.SetDiffusionMode(muxer.DiffusionModeInitiator)
	m2.SetDiffusionMode(muxer.DiffusionModeResponder)
	m1.Start()
	m2.Start()
	return pair
}

type idRequestSeen struct {
	blocking bool
	ack, req uint16
}

// recorder collects what a pair's callbacks report.
type recorder struct {
	idRequests  chan idRequestSeen
	sigRequests chan [][]byte
	dones       chan struct{}
	idReplies   chan []ocommon.MessageIDAndSize
	sigReplies  chan []ocommon.DmqMessage
}

func newRecorder() *recorder {
	return &recorder{
		idRequests:  make(chan idRequestSeen, 8),
		sigRequests: make(chan [][]byte, 8),
		dones:       make(chan struct{}, 8),
		idReplies:   make(chan []ocommon.MessageIDAndSize, 8),
		sigReplies:  make(chan []ocommon.DmqMessage, 8),
	}
}

func (r *recorder) pair(t *testing.T) *sigPair {
	t.Helper()
	return newSigPair(t,
		sigPullerConfig{
			onReplyIDs:  func(i []ocommon.MessageIDAndSize) { r.idReplies <- i },
			onReplySigs: func(m []ocommon.DmqMessage) { r.sigReplies <- m },
		},
		sigServerConfig{
			onRequestIDs: func(b bool, a, q uint16) {
				r.idRequests <- idRequestSeen{b, a, q}
			},
			onRequestSigs: func(i [][]byte) { r.sigRequests <- i },
			onDone:        func() { r.dones <- struct{}{} },
		},
	)
}

func recv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(peerWait):
		require.FailNow(t, "timed out waiting for "+what)
		var none T
		return none
	}
}

func TestSigProtocolPullsAndServes(t *testing.T) {
	t.Parallel()
	rec := newRecorder()
	pair := rec.pair(t)

	require.NoError(t, pair.puller.RequestIDs(true, 0, 5))
	assert.Equal(
		t,
		idRequestSeen{true, 0, 5},
		recv(t, rec.idRequests, "the request"),
	)
	require.NoError(t, pair.server.ReplyIDs(idsAndSizes(1, 2, 3)))
	got := recv(t, rec.idReplies, "the IDs")
	require.Len(t, got, 3)

	require.NoError(t, pair.puller.RequestSigs(ids(1, 3)))
	assert.Equal(t, ids(1, 3), recv(t, rec.sigRequests, "the message request"))
	require.NoError(t, pair.server.ReplySigs(
		[]ocommon.DmqMessage{sigMsgWithID(1), sigMsgWithID(3)},
	))
	msgs := recv(t, rec.sigReplies, "the messages")
	require.Len(t, msgs, 2)
	assert.Equal(t, sigID(1), msgs[0].ID())

	// Acknowledging all three makes the next request blocking again.
	require.NoError(t, pair.puller.RequestIDs(true, 3, 5))
	assert.Equal(
		t,
		idRequestSeen{true, 3, 5},
		recv(t, rec.idRequests, "the next request"),
	)

	require.NoError(t, pair.puller.Done())
	_ = pair
}

// A server may answer a blocking request with MsgReplyNoIDs, which
// acknowledges and announces nothing.
func TestSigProtocolReplyNoIDsToABlockingRequest(t *testing.T) {
	t.Parallel()
	rec := newRecorder()
	pair := rec.pair(t)

	require.NoError(t, pair.puller.RequestIDs(true, 0, 5))
	recv(t, rec.idRequests, "the request")
	require.NoError(t, pair.server.ReplyIDs(idsAndSizes(1)))
	recv(t, rec.idReplies, "the IDs")

	require.NoError(t, pair.puller.RequestIDs(true, 1, 5))
	recv(t, rec.idRequests, "the second request")
	require.NoError(t, pair.server.ReplyNoIDs())
	assert.Empty(t, recv(t, rec.idReplies, "the empty reply"))

	// The acknowledgement counted, so the window is empty again.
	require.NoError(t, pair.puller.RequestIDs(true, 0, 5))
	assert.Equal(
		t,
		idRequestSeen{true, 0, 5},
		recv(t, rec.idRequests, "the third request"),
	)
}

// A non-blocking request may be answered with an empty list, and then the
// window is as it was.
func TestSigProtocolEmptyReplyToANonBlockingRequest(t *testing.T) {
	t.Parallel()
	rec := newRecorder()
	pair := rec.pair(t)

	require.NoError(t, pair.puller.RequestIDs(true, 0, 5))
	recv(t, rec.idRequests, "the request")
	require.NoError(t, pair.server.ReplyIDs(idsAndSizes(1)))
	recv(t, rec.idReplies, "the IDs")

	require.NoError(t, pair.puller.RequestIDs(false, 0, 2))
	recv(t, rec.idRequests, "the non-blocking request")
	require.NoError(t, pair.server.ReplyIDs(nil))
	assert.Empty(t, recv(t, rec.idReplies, "the empty reply"))

	// ID 1 is still outstanding, so it can be fetched.
	require.NoError(t, pair.puller.RequestSigs(ids(1)))
	assert.Equal(t, ids(1), recv(t, rec.sigRequests, "the message request"))
}

func TestSigProtocolServerRefusesWhatTheRulesForbid(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		send func(*sigPair)
		want error
	}{
		{"a blocking request with IDs outstanding", func(p *sigPair) {
			_ = p.puller.SendMessage(newMsgRequestIDs(true, 0, 1))
		}, protocol.ErrProtocolViolationRequestExceeded},
		{"acknowledging more than is outstanding", func(p *sigPair) {
			_ = p.puller.SendMessage(newMsgRequestIDs(false, 9, 1))
		}, protocol.ErrProtocolViolationRequestExceeded},
		{"a request for nothing", func(p *sigPair) {
			_ = p.puller.SendMessage(newMsgRequestIDs(false, 0, 0))
		}, protocol.ErrProtocolViolationRequestExceeded},
		{"asking beyond the window", func(p *sigPair) {
			_ = p.puller.SendMessage(newMsgRequestIDs(false, 0, 10))
		}, protocol.ErrProtocolViolationRequestExceeded},
		{"messages that were never announced", func(p *sigPair) {
			_ = p.puller.SendMessage(newMsgRequestSigs(ids(9)))
		}, protocol.ErrProtocolViolationInvalidMessage},
		{"the same message twice", func(p *sigPair) {
			_ = p.puller.SendMessage(newMsgRequestSigs(ids(1, 1)))
		}, protocol.ErrProtocolViolationInvalidMessage},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := newRecorder()
			pair := rec.pair(t)
			// Announce IDs 1 and 2, so a window of two is outstanding.
			require.NoError(t, pair.puller.RequestIDs(true, 0, 5))
			recv(t, rec.idRequests, "the request")
			require.NoError(t, pair.server.ReplyIDs(idsAndSizes(1, 2)))
			recv(t, rec.idReplies, "the IDs")

			tt.send(pair)
			err := recv(t, pair.serverErr, "the violation")
			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestSigProtocolPullerRefusesWhatTheRulesForbid(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		setup func(*sigPair, *recorder)
		reply func(*sigPair)
		want  error
	}{
		{"more IDs than were asked", func(p *sigPair, r *recorder) {
			require.NoError(t, p.puller.RequestIDs(true, 0, 1))
			recv(t, r.idRequests, "the request")
		}, func(p *sigPair) {
			_ = p.server.SendMessage(newMsgReplyIDs(idsAndSizes(1, 2)))
		}, protocol.ErrProtocolViolationRequestExceeded},
		{"nothing for a blocking request", func(p *sigPair, r *recorder) {
			require.NoError(t, p.puller.RequestIDs(true, 0, 1))
			recv(t, r.idRequests, "the request")
		}, func(p *sigPair) {
			_ = p.server.SendMessage(newMsgReplyIDs(nil))
		}, protocol.ErrProtocolViolationInvalidMessage},
		{"a repeated ID", func(p *sigPair, r *recorder) {
			require.NoError(t, p.puller.RequestIDs(true, 0, 2))
			recv(t, r.idRequests, "the request")
		}, func(p *sigPair) {
			_ = p.server.SendMessage(newMsgReplyIDs(idsAndSizes(1, 1)))
		}, protocol.ErrProtocolViolationInvalidMessage},
		{"a message that was not asked for", func(p *sigPair, r *recorder) {
			require.NoError(t, p.puller.RequestIDs(true, 0, 2))
			recv(t, r.idRequests, "the request")
			require.NoError(t, p.server.ReplyIDs(idsAndSizes(1, 2)))
			recv(t, r.idReplies, "the IDs")
			require.NoError(t, p.puller.RequestSigs(ids(1)))
			recv(t, r.sigRequests, "the message request")
		}, func(p *sigPair) {
			_ = p.server.SendMessage(
				newMsgReplySigs([]ocommon.DmqMessage{sigMsgWithID(2)}),
			)
		}, protocol.ErrProtocolViolationInvalidMessage},
		{"more messages than were asked for", func(p *sigPair, r *recorder) {
			require.NoError(t, p.puller.RequestIDs(true, 0, 2))
			recv(t, r.idRequests, "the request")
			require.NoError(t, p.server.ReplyIDs(idsAndSizes(1, 2)))
			recv(t, r.idReplies, "the IDs")
			require.NoError(t, p.puller.RequestSigs(ids(1)))
			recv(t, r.sigRequests, "the message request")
		}, func(p *sigPair) {
			_ = p.server.SendMessage(newMsgReplySigs(
				[]ocommon.DmqMessage{sigMsgWithID(1), sigMsgWithID(2)},
			))
		}, protocol.ErrProtocolViolationRequestExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := newRecorder()
			pair := rec.pair(t)
			tt.setup(pair, rec)
			tt.reply(pair)
			err := recv(t, pair.pullerErr, "the violation")
			require.True(t, errors.Is(err, tt.want), "got %v", err)
		})
	}
}

// dmq-node's pulling side pipelines: it sends its next message before the
// reply to the last one, as if it already knew the reply. The serving side
// must hold such a message until it has sent its reply, then handle it.
func TestSigProtocolServerToleratesAPipeliningPuller(t *testing.T) {
	t.Parallel()
	rec := newRecorder()
	pipelined := sigStateMap(sigTimeouts{}, false)
	for _, st := range []protocol.State{sigStateIDsNonBlocking, sigStateSigs} {
		entry := pipelined[st]
		entry.AllowPipelinedSend = true
		entry.PipelinedMessageTypes = []uint8{
			sigMsgRequestIDs,
			sigMsgRequestSigs,
		}
		pipelined[st] = entry
	}
	pair := newSigPair(t,
		sigPullerConfig{
			stateMap:    pipelined,
			onReplyIDs:  func(i []ocommon.MessageIDAndSize) { rec.idReplies <- i },
			onReplySigs: func(m []ocommon.DmqMessage) { rec.sigReplies <- m },
		},
		sigServerConfig{
			onRequestIDs: func(b bool, a, q uint16) {
				rec.idRequests <- idRequestSeen{b, a, q}
			},
			onRequestSigs: func(i [][]byte) { rec.sigRequests <- i },
		},
	)

	require.NoError(t, pair.puller.RequestIDs(true, 0, 5))
	recv(t, rec.idRequests, "the request")
	require.NoError(t, pair.server.ReplyIDs(idsAndSizes(1, 2)))
	recv(t, rec.idReplies, "the IDs")

	// A non-blocking request, and straight behind it a request for message 1,
	// which the puller already holds the ID of.
	require.NoError(t, pair.puller.RequestIDs(false, 0, 2))
	pair.puller.mu.Lock()
	pair.puller.pendingSigs = &sigRequest{
		ids: map[string]struct{}{string(sigID(1)): {}},
	}
	pair.puller.mu.Unlock()
	require.NoError(t, pair.puller.SendMessage(newMsgRequestSigs(ids(1))))

	recv(t, rec.idRequests, "the non-blocking request")
	select {
	case <-rec.sigRequests:
		require.FailNow(t, "the pipelined request ran before the reply")
	case <-time.After(200 * time.Millisecond):
	}
	require.NoError(t, pair.server.ReplyIDs(nil))
	assert.Equal(t, ids(1), recv(t, rec.sigRequests, "the pipelined request"))
	select {
	case err := <-pair.serverErr:
		require.FailNow(t, "the pipelined request was refused", "%v", err)
	default:
	}
}

func TestSigProtocolDoneReachesTheServingSide(t *testing.T) {
	t.Parallel()
	rec := newRecorder()
	pair := rec.pair(t)
	require.NoError(t, pair.puller.Done())
	recv(t, rec.dones, "the Done")
	waitFor(t, "the serving side is finished", pair.server.IsDone)
	waitFor(t, "the pulling side is finished", pair.puller.IsDone)
}

// The pulling side is the connection's initiator and holds agency in Idle; the
// serving side is the responder. dmq-node's version 2 assigns them this way,
// the reverse of TxSubmission2, and a pair of Dingo stacks would agree on
// either assignment, so it is pinned here.
func TestSigProtocolRolesAndAgencyMatchDmqNode(t *testing.T) {
	t.Parallel()
	pair := newRecorder().pair(t)
	assert.Equal(t, protocol.ProtocolRoleClient, pair.puller.Role())
	assert.Equal(t, protocol.ProtocolRoleServer, pair.server.Role())
	assert.Equal(t, sigProtocolID, uint16(11))

	stateMap := sigStateMap(sigTimeouts{}, true)
	assert.Equal(t, protocol.AgencyClient, stateMap[sigStateIdle].Agency)
	for _, st := range []protocol.State{
		sigStateIDsBlocking, sigStateIDsNonBlocking, sigStateSigs,
	} {
		assert.Equal(t, protocol.AgencyServer, stateMap[st].Agency, st.Name)
	}
	assert.Equal(t, protocol.AgencyNone, stateMap[sigStateDone].Agency)
	// MsgReplyNoIDs answers a blocking request only.
	for st, entry := range map[protocol.State]protocol.StateMapEntry{
		sigStateIDsBlocking:    stateMap[sigStateIDsBlocking],
		sigStateIDsNonBlocking: stateMap[sigStateIDsNonBlocking],
	} {
		var allowsNoIDs bool
		for _, tr := range entry.Transitions {
			allowsNoIDs = allowsNoIDs || tr.MsgType == sigMsgReplyNoIDs
		}
		assert.Equal(t, st == sigStateIDsBlocking, allowsNoIDs, st.Name)
	}
}

// Version 2 has no Init state: the pulling side's first message is a request,
// sent the moment the protocol starts, with no Init before it and none awaited.
// A flow that needed an Init first would leave this request queued forever.
func TestSigProtocolStartsInIdleWithoutAnInit(t *testing.T) {
	t.Parallel()
	rec := newRecorder()
	pair := rec.pair(t)

	require.NoError(t, pair.puller.RequestIDs(true, 0, 5))
	assert.Equal(t, idRequestSeen{true, 0, 5},
		recv(t, rec.idRequests, "the first request, with no Init before it"))
	require.NoError(t, pair.server.ReplyIDs(idsAndSizes(1)))
	recv(t, rec.idReplies, "the first reply")

	stateMap := sigStateMap(sigTimeouts{}, true)
	idle := stateMap[sigStateIdle]
	for _, tr := range idle.Transitions {
		assert.NotZero(t, tr.MsgType, "no transition out of Idle is an Init")
	}
	for state := range stateMap {
		assert.NotEqual(t, "Init", state.Name)
	}
}

// The pulling side gives up on a peer that does not reply within the limit for
// the state, as dmq-node does of its peers.
func TestSigProtocolPullerEnforcesItsTimeouts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		limit sigTimeouts
		send  func(*sigPair, *recorder)
	}{
		{"a blocking request", sigTimeouts{blocking: 150 * time.Millisecond},
			func(p *sigPair, _ *recorder) {
				require.NoError(t, p.puller.RequestIDs(true, 0, 5))
			}},
		{
			"a request for message bodies",
			sigTimeouts{sigs: 150 * time.Millisecond},
			func(p *sigPair, r *recorder) {
				require.NoError(t, p.puller.RequestIDs(true, 0, 5))
				recv(t, r.idRequests, "the request")
				require.NoError(t, p.server.ReplyIDs(idsAndSizes(1)))
				recv(t, r.idReplies, "the IDs")
				require.NoError(t, p.puller.RequestSigs(ids(1)))
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := newRecorder()
			pair := newSigPair(t,
				sigPullerConfig{
					timeouts:   tt.limit,
					onReplyIDs: func(i []ocommon.MessageIDAndSize) { rec.idReplies <- i },
				},
				sigServerConfig{
					onRequestIDs: func(b bool, a, q uint16) {
						rec.idRequests <- idRequestSeen{b, a, q}
					},
					onRequestSigs: func(i [][]byte) { rec.sigRequests <- i },
				},
			)
			tt.send(pair, rec)
			// The serving side never answers, so the pulling side times out.
			require.Error(t, recv(t, pair.pullerErr, "the timeout"))
		})
	}
}

func TestSigStateMapBoundsWhatIsReceivedInEveryState(t *testing.T) {
	t.Parallel()
	for _, pipelined := range []bool{false, true} {
		sm := sigStateMap(sigTimeouts{}, pipelined)
		assert.Equal(
			t, sigRequestByteLimit,
			sm[sigStateIdle].PendingMessageByteLimit,
			"Idle, pipelined=%v", pipelined,
		)
		for _, st := range []protocol.State{
			sigStateIDsBlocking, sigStateIDsNonBlocking, sigStateSigs,
		} {
			assert.Equal(
				t, sigReplyByteLimit, sm[st].PendingMessageByteLimit,
				"state %v, pipelined=%v", st, pipelined,
			)
		}
	}
}

// A request over the limit ends the connection at the serving side, whatever
// the pulling side's own limit, before the request is decoded or checked
// against the window.
func TestSigProtocolServerRefusesAnOversizedRequest(t *testing.T) {
	t.Parallel()
	rec := newRecorder()
	lifted := sigStateMap(sigTimeouts{}, false)
	idle := lifted[sigStateIdle]
	idle.PendingMessageByteLimit = 0
	lifted[sigStateIdle] = idle
	pair := newSigPair(t,
		sigPullerConfig{stateMap: lifted},
		sigServerConfig{
			onRequestIDs:  func(bool, uint16, uint16) {},
			onRequestSigs: func(i [][]byte) { rec.sigRequests <- i },
		},
	)

	// More IDs than 256 KiB holds, none of them announced.
	big := make([][]byte, sigRequestByteLimit/34+1)
	for i := range big {
		big[i] = sigID(byte(i))
	}
	_ = pair.puller.SendMessage(newMsgRequestSigs(big))

	err := recv(t, pair.serverErr, "the refusal")
	require.ErrorContains(t, err, "limit")
	assert.NotErrorIs(t, err, protocol.ErrProtocolViolationInvalidMessage)
	select {
	case <-rec.sigRequests:
		require.FailNow(t, "the request reached the callback")
	default:
	}
}
