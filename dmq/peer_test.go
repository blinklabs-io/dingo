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
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const peerWait = 10 * time.Second

// tcpPair returns the two ends of a loopback TCP connection.
func tcpPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- conn
	}()
	dialed, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	listened, ok := <-accepted
	require.True(t, ok)
	t.Cleanup(func() {
		_ = dialed.Close()
		_ = listened.Close()
	})
	return dialed, listened
}

type attachResult struct {
	peer *PeerConn
	err  error
}

// attachBoth connects a and b over loopback, a dialing and b listening, and
// returns what each side's AttachPeer returned.
func attachBoth(t *testing.T, a, b *Stack) (attachResult, attachResult) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), peerWait)
	defer cancel()
	dialedConn, listenedConn := tcpPair(t)
	listened := make(chan attachResult, 1)
	go func() {
		p, err := b.AttachPeer(ctx, listenedConn, false)
		listened <- attachResult{p, err}
	}()
	p, err := a.AttachPeer(ctx, dialedConn, true)
	return attachResult{p, err}, <-listened
}

// attachPair is attachBoth for a connection that must succeed.
func attachPair(t *testing.T, a, b *Stack) (*PeerConn, *PeerConn) {
	t.Helper()
	ra, rb := attachBoth(t, a, b)
	require.NoError(t, ra.err)
	require.NoError(t, rb.err)
	return ra.peer, rb.peer
}

func waitFor(t *testing.T, msg string, cond func() bool) {
	t.Helper()
	testutil.WaitForCondition(t, cond, peerWait, msg)
}

// idedMessage is newTestMessage with its message ID set, as the pool stores it.
func idedMessage(
	t *testing.T,
	body string,
	expiresAt uint32,
) ocommon.DmqMessage {
	t.Helper()
	msg := newTestMessage([]byte(body), expiresAt)
	require.NoError(t, msg.SetComputedMessageID())
	return msg
}

func addMessages(t *testing.T, s *Stack, prefix string, n int) {
	t.Helper()
	for i := range n {
		added, err := s.pool.Add(newTestMessage(
			[]byte(fmt.Sprintf("%s-%d", prefix, i)), soonExpiry(),
		))
		require.NoError(t, err)
		require.True(t, added)
	}
}

func closed(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

func requireClosed(t *testing.T, msg string, done <-chan struct{}) {
	t.Helper()
	waitFor(t, msg, func() bool { return closed(done) })
}

func counter(
	t *testing.T,
	reg *prometheus.Registry,
	name string,
) func() float64 {
	return func() float64 { return metricValue(t, reg, name, "") }
}

func TestPeerExchangesMessagesBothWays(t *testing.T) {
	t.Parallel()
	a, regA := newTestStack(t, StackConfig{})
	b, regB := newTestStack(t, StackConfig{})
	addMessages(t, a, "from-a", 3)

	pa, pb := attachPair(t, a, b)
	require.Equal(t, uint16(2), pa.Version())
	require.Equal(t, uint16(2), pb.Version())
	waitFor(t, "b receives a's messages", func() bool {
		return b.pool.Len() == 3
	})
	// b's pool was empty when a first asked, so this reaches a through a
	// blocking request that waits for the message to arrive.
	addMessages(t, b, "from-b", 1)
	waitFor(t, "a receives b's message", func() bool {
		return a.pool.Len() == 4
	})

	// Each body crossed exactly once: nothing came back to the side it
	// started from.
	waitFor(t, "a sent its three", func() bool {
		return counter(t, regA, "dingo_dmq_messages_sent_total")() == 3
	})
	waitFor(t, "b sent its one", func() bool {
		return counter(t, regB, "dingo_dmq_messages_sent_total")() == 1
	})
	require.Equal(t, 1.0, counter(t, regA, "dingo_dmq_peers")())
	require.Equal(t, 1.0, counter(t, regB, "dingo_dmq_peers")())
	require.False(t, closed(pa.Done()))
	require.False(t, closed(pb.Done()))
}

func TestPeerOffersOnlyVersionTwo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		a, b []uint16
	}{
		{"both offer the default", nil, nil},
		{"one side also offers version 1", []uint16{1, 2}, nil},
		{"the other side also offers version 1", nil, []uint16{1, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a, _ := newTestStack(t, StackConfig{})
			b, _ := newTestStack(t, StackConfig{})
			if tt.a != nil {
				a.peerTune.versions = tt.a
			}
			if tt.b != nil {
				b.peerTune.versions = tt.b
			}
			addMessages(t, a, "m", 2)

			pa, pb := attachPair(t, a, b)
			// Version 2 has no Init state; the pull has to work without one.
			require.Equal(t, uint16(2), pa.Version())
			require.Equal(t, uint16(2), pb.Version())
			waitFor(t, "b receives a's messages", func() bool {
				return b.pool.Len() == 2
			})
		})
	}
}

func TestPeerRefusesWithNoCommonVersion(t *testing.T) {
	t.Parallel()
	a, _ := newTestStack(t, StackConfig{})
	b, _ := newTestStack(t, StackConfig{})
	a.peerTune.versions = []uint16{1}
	b.peerTune.versions = []uint16{2}
	a.peerTune.handshakeTimeout = 3 * time.Second
	b.peerTune.handshakeTimeout = 3 * time.Second

	ra, rb := attachBoth(t, a, b)
	require.Error(t, ra.err)
	require.Error(t, rb.err)
}

func TestPeerRefusesNetworkMagicMismatch(t *testing.T) {
	t.Parallel()
	a, _ := newTestStack(t, StackConfig{})
	b, _ := newTestStack(t, StackConfig{})
	b.cfg.NetworkMagic = testDMQMagic + 1
	a.peerTune.handshakeTimeout = 3 * time.Second
	b.peerTune.handshakeTimeout = 3 * time.Second

	ra, rb := attachBoth(t, a, b)
	require.Error(t, ra.err)
	require.Error(t, rb.err)
	require.Zero(t, metricValue(t, newRegistryOf(a), "dingo_dmq_peers", ""))
}

// newRegistryOf is the registry a test stack registered its metrics with.
func newRegistryOf(s *Stack) *prometheus.Registry {
	reg, ok := s.cfg.PromRegistry.(*prometheus.Registry)
	if !ok {
		panic("test stack registry is not a *prometheus.Registry")
	}
	return reg
}

// A connection with no network address cannot be logged by gouroboros, so it
// is refused and closed before anything is built on it.
func TestAttachPeerRefusesAConnectionWithoutAddresses(t *testing.T) {
	t.Parallel()
	a, _ := newTestStack(t, StackConfig{})
	conn := &addresslessConn{}

	_, err := a.AttachPeer(t.Context(), conn, true)
	require.ErrorContains(t, err, "no network address")
	require.True(t, conn.closed)
	require.Zero(t, metricValue(t, newRegistryOf(a), "dingo_dmq_peers", ""))
}

type addresslessConn struct {
	net.Conn
	closed bool
}

func (c *addresslessConn) LocalAddr() net.Addr  { return nil }
func (c *addresslessConn) RemoteAddr() net.Addr { return nil }
func (c *addresslessConn) Close() error {
	c.closed = true
	return nil
}

func TestPeerHandshakeTimesOut(t *testing.T) {
	t.Parallel()
	a, _ := newTestStack(t, StackConfig{})
	a.peerTune.handshakeTimeout = 100 * time.Millisecond
	dialed, _ := tcpPair(t)

	_, err := a.AttachPeer(t.Context(), dialed, true)
	require.Error(t, err)
	require.Zero(t, metricValue(t, newRegistryOf(a), "dingo_dmq_peers", ""))
}

// With IDs left unfetched the next request must be non-blocking: a blocking
// one is only allowed once everything announced has been acknowledged.
func TestPeerAsksNonBlockingWhileCatchingUp(t *testing.T) {
	t.Parallel()
	a, _ := newTestStack(t, StackConfig{})
	b, _ := newTestStack(t, StackConfig{})
	b.peerTune.idBatch = 4
	b.peerTune.fetchMessages = 2
	addMessages(t, a, "m", 4)

	_, pb := attachPair(t, a, b)
	waitFor(t, "b receives every message", func() bool {
		return b.pool.Len() == 4
	})
	waitFor(t, "b asks blocking and non-blocking", func() bool {
		return pb.blockingRequests.Load() >= 1 &&
			pb.nonBlockingRequests.Load() >= 1
	})
	require.False(t, closed(pb.Done()), "a violation would have closed it")
}

func TestPeerAcknowledgementsKeepTheWindowOpen(t *testing.T) {
	t.Parallel()
	a, _ := newTestStack(t, StackConfig{})
	b, _ := newTestStack(t, StackConfig{})
	b.peerTune.maxUnacked = 4
	b.peerTune.idBatch = 2
	b.peerTune.fetchMessages = 2
	addMessages(t, a, "m", 10)

	pa, pb := attachPair(t, a, b)
	waitFor(t, "b receives all ten past a window of four", func() bool {
		return b.pool.Len() == 10
	})
	require.False(t, closed(pa.Done()))
	require.False(t, closed(pb.Done()))
	require.NoError(t, pb.Err())
}

func TestPeerSkipsMessagesAlreadyHeld(t *testing.T) {
	t.Parallel()
	a, regA := newTestStack(t, StackConfig{})
	b, regB := newTestStack(t, StackConfig{})
	shared := newTestMessage([]byte("shared"), soonExpiry())
	for _, s := range []*Stack{a, b} {
		_, err := s.pool.Add(shared)
		require.NoError(t, err)
	}

	pa, pb := attachPair(t, a, b)
	// A message sent after the connection is up proves the exchange is live,
	// and that the shared one was settled before it.
	addMessages(t, a, "later", 1)
	waitFor(t, "b receives the later message", func() bool {
		return b.pool.Len() == 2
	})
	// The sent counter is bumped on the replying goroutine after the reply is
	// queued, so b can hold the message before a counts it. Teardown waits for
	// those goroutines, which makes the counters final.
	pa.Close()
	requireClosed(t, "b's side closes", pb.Done())
	require.Equal(t, 1.0, counter(t, regA, "dingo_dmq_messages_sent_total")())
	require.Zero(t, counter(t, regB, "dingo_dmq_messages_sent_total")())
}

func TestPeerNeverAnnouncesBackWhatThePeerAnnounced(t *testing.T) {
	t.Parallel()
	s, _ := newTestStack(t, StackConfig{})
	p := &PeerConn{
		stack:     s,
		logger:    s.logger,
		cursorKey: "unit",
		known:     newIDSet(8),
	}
	t.Cleanup(func() { s.pool.RemovePeer(p.cursorKey) })
	told := idedMessage(t, "told", soonExpiry())
	fresh := idedMessage(t, "fresh", soonExpiry())
	for _, m := range []ocommon.DmqMessage{told, fresh} {
		_, err := s.pool.Add(m)
		require.NoError(t, err)
	}
	p.known.add(told.ID())

	ids := p.nextMessageIDs(10)
	require.Len(t, ids, 1)
	got, ok := s.pool.Get(ids[0].MessageID)
	require.True(t, ok)
	require.Equal(t, []byte("fresh"), got.Payload.MessageBody)
	require.NotZero(t, ids[0].SizeInBytes)
}

func TestPeerIgnoresExpiredMessagesWhenAnnouncing(t *testing.T) {
	t.Parallel()
	now := time.Now()
	s, _ := newTestStack(t, StackConfig{Now: func() time.Time { return now }})
	p := &PeerConn{
		stack:     s,
		logger:    s.logger,
		cursorKey: "unit",
		known:     newIDSet(8),
	}
	t.Cleanup(func() { s.pool.RemovePeer(p.cursorKey) })
	// #nosec G115 -- test fixture timestamps
	soon := uint32(now.Add(time.Minute).Unix())
	_, err := s.pool.Add(newTestMessage([]byte("soon"), soon))
	require.NoError(t, err)
	_, err = s.pool.Add(newTestMessage([]byte("later"), soonExpiry()))
	require.NoError(t, err)

	now = now.Add(2 * time.Minute)
	ids := p.nextMessageIDs(10)
	require.Len(t, ids, 1)
	got, ok := s.pool.Get(ids[0].MessageID)
	require.True(t, ok)
	require.Equal(t, []byte("later"), got.Payload.MessageBody)
}

// A message that fails validation is a protocol violation: the connection
// ends, and the refusal is counted under its reason.
func TestPeerInvalidMessageEndsTheConnection(t *testing.T) {
	t.Parallel()
	a, regA := newTestStack(t, StackConfig{})
	b, _ := newTestStack(t, StackConfig{})
	// The pool does not apply the message TTL, so b can hold a message that
	// a's admission refuses for expiring too far ahead.
	// #nosec G115 -- test fixture timestamp
	tooFar := uint32(time.Now().Add(2 * time.Hour).Unix())
	_, err := b.pool.Add(newTestMessage([]byte("too-far"), tooFar))
	require.NoError(t, err)

	pa, pb := attachPair(t, a, b)
	requireClosed(t, "a ends the connection", pa.Done())
	require.ErrorContains(t, pa.Err(), "invalid message")
	requireClosed(t, "b notices", pb.Done())
	require.Zero(t, a.pool.Len())
	require.Equal(
		t,
		1.0,
		metricValue(t, regA, "dingo_dmq_validation_failures_total", "invalid"),
	)
	waitFor(t, "peer count returns to zero", func() bool {
		return counter(t, regA, "dingo_dmq_peers")() == 0
	})
}

// A peer that sends a message outside CIP-0137's sizes is a violator: dmq-node
// would have refused to decode it, so this stack does not take it.
func TestPeerMessageOutsideTheSizeLimitsEndsTheConnection(t *testing.T) {
	t.Parallel()
	a, regA := newTestStack(t, StackConfig{
		MessageBodyMin: DefaultMessageBodyMin,
		MessageBodyMax: DefaultMessageBodyMax,
	})
	// b has no size limit of its own to speak of, so it holds a short message.
	b, _ := newTestStack(t, StackConfig{})
	addMessages(t, b, "short", 1)

	pa, _ := attachPair(t, a, b)
	requireClosed(t, "a ends the connection", pa.Done())
	require.ErrorContains(t, pa.Err(), "body is")
	require.Zero(t, a.pool.Len())
	require.Equal(
		t,
		1.0,
		metricValue(t, regA, "dingo_dmq_validation_failures_total", "invalid"),
	)
}

// dmq-node ends a connection whose blocking request goes unanswered for 20
// seconds, so a serving side with nothing to announce says so after a short
// wait and the peer asks again. The connection has to survive that cycle, and a
// message that arrives between requests still has to reach the peer.
func TestPeerAnswersABlockingRequestWithNoIDsWhenNothingArrives(t *testing.T) {
	t.Parallel()
	a, _ := newTestStack(t, StackConfig{})
	b, _ := newTestStack(t, StackConfig{})
	a.peerTune.blockingReplyWait = 100 * time.Millisecond

	pa, pb := attachPair(t, a, b)
	// b's blocking request to a goes unanswered by content again and again,
	// each time answered with no IDs, and b asks again.
	waitFor(t, "b asks for IDs three times over", func() bool {
		return pb.blockingRequests.Load() >= 3
	})
	require.False(t, closed(pb.Done()), "no violation: %v", pb.Err())
	require.False(t, closed(pa.Done()), "no violation: %v", pa.Err())
	require.Zero(t, b.pool.Len())

	addMessages(t, a, "late", 1)
	waitFor(t, "b receives the late message", func() bool {
		return b.pool.Len() == 1
	})
}

// The defaults are dmq-node's own limits for the three states, which is what
// the live run showed it enforces, and the serving side answers well inside
// the blocking one.
func TestDefaultPeerTuningMatchesDmqNodeTimeLimits(t *testing.T) {
	t.Parallel()
	tune := defaultPeerTuning()
	require.Equal(t, 20*time.Second, tune.blockingTimeout)
	require.Equal(t, 10*time.Second, tune.nonBlockingTimeout)
	require.Equal(t, 10*time.Second, tune.sigsTimeout)
	require.Less(t, tune.blockingReplyWait, tune.blockingTimeout)
	require.Equal(t, []uint16{2}, tune.versions)
}

func TestPeerCloseReleasesItsState(t *testing.T) {
	t.Parallel()
	a, regA := newTestStack(t, StackConfig{})
	b, regB := newTestStack(t, StackConfig{})
	addMessages(t, a, "m", 2)

	pa, pb := attachPair(t, a, b)
	waitFor(t, "b receives a's messages", func() bool {
		return b.pool.Len() == 2
	})
	a.pool.peersMu.Lock()
	_, tracked := a.pool.peers[pa.cursorKey]
	a.pool.peersMu.Unlock()
	require.True(t, tracked, "a keeps a cursor for the open peer")

	pa.Close()
	require.True(t, closed(pa.Done()))
	require.NoError(t, pa.Err())
	// The remote hanging up is how a connection ends, not a failure.
	requireClosed(t, "b notices the hang-up", pb.Done())
	require.NoError(t, pb.Err())
	a.pool.peersMu.Lock()
	_, tracked = a.pool.peers[pa.cursorKey]
	a.pool.peersMu.Unlock()
	require.False(t, tracked, "closing releases the cursor")
	waitFor(t, "both peer counts return to zero", func() bool {
		return counter(t, regA, "dingo_dmq_peers")() == 0 &&
			counter(t, regB, "dingo_dmq_peers")() == 0
	})
}

// A side that closes a socket while unread data is queued for it makes the
// kernel reset the connection instead of finishing it. The other side's muxer
// then reads ECONNRESET rather than EOF, which is still the remote hanging up.
func TestPeerRemoteResetIsNotAFailure(t *testing.T) {
	t.Parallel()
	a, _ := newTestStack(t, StackConfig{})
	b, _ := newTestStack(t, StackConfig{})
	pa, pb := attachPair(t, a, b)

	tcp, ok := pa.conn.(*net.TCPConn)
	require.True(t, ok)
	require.NoError(t, tcp.SetLinger(0))
	pa.Close()
	requireClosed(t, "b notices the reset", pb.Done())
	require.NoError(t, pb.Err())
}

func TestStackStopClosesPeers(t *testing.T) {
	t.Parallel()
	a, regA := newTestStack(t, StackConfig{})
	b, _ := newTestStack(t, StackConfig{})
	pa, pb := attachPair(t, a, b)

	ctx, cancel := context.WithTimeout(t.Context(), peerWait)
	defer cancel()
	require.NoError(t, a.Stop(ctx))
	require.True(t, closed(pa.Done()))
	requireClosed(t, "the remote notices", pb.Done())
	require.Zero(t, metricValue(t, regA, "dingo_dmq_peers", ""))

	dialed, listened := tcpPair(t)
	_, err := a.AttachPeer(ctx, dialed, true)
	require.ErrorContains(t, err, "stopped")
	// The refused connection was closed, not leaked.
	require.NoError(t, listened.SetReadDeadline(time.Now().Add(peerWait)))
	_, err = listened.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)
}

func TestPeerKeepAliveRunsOnProtocolTwelve(t *testing.T) {
	t.Parallel()
	require.Equal(t, uint16(12), keepAliveProtocolID)

	a, _ := newTestStack(t, StackConfig{})
	b, _ := newTestStack(t, StackConfig{})
	a.peerTune.keepAlivePeriod = 5 * time.Millisecond
	b.peerTune.keepAlivePeriod = 5 * time.Millisecond

	pa, pb := attachPair(t, a, b)
	waitFor(t, "both sides see several keep-alive replies", func() bool {
		return pa.keepAlive.replies.Load() >= 3 &&
			pb.keepAlive.replies.Load() >= 3
	})
	require.False(t, closed(pa.Done()))
	require.False(t, closed(pb.Done()))
}

// A node that dials without offering both roles only pulls: its initiator
// instances run, so the listener serves it and nothing flows the other way.
func TestPeerInitiatorOnlyDialerOnlyPulls(t *testing.T) {
	t.Parallel()
	a, _ := newTestStack(t, StackConfig{})
	b, _ := newTestStack(t, StackConfig{})
	a.peerTune.initiatorOnly = true
	addMessages(t, a, "from-a", 1)
	addMessages(t, b, "from-b", 1)

	pa, pb := attachPair(t, a, b)
	waitFor(t, "a receives b's message", func() bool {
		return a.pool.Len() == 2
	})
	// A second message from b is a later event on the same connection, so by
	// the time a has it, a's own message has had every chance to reach b.
	addMessages(t, b, "more-from-b", 1)
	waitFor(t, "a receives the second message", func() bool {
		return a.pool.Len() == 3
	})
	require.Equal(t, 2, b.pool.Len(), "nothing reached the listener")
	require.False(t, closed(pa.Done()))
	require.False(t, closed(pb.Done()))
}

// A peer that ends its pulling side no longer reads from this stack, so the
// cursor held for it is released.
func TestPeerDoneReleasesTheCursor(t *testing.T) {
	t.Parallel()
	s, _ := newTestStack(t, StackConfig{})
	p := &PeerConn{
		stack:     s,
		logger:    s.logger,
		cursorKey: "unit",
		known:     newIDSet(8),
	}
	addMessages(t, s, "m", 1)
	_, ok := s.pool.NextForPeer(p.cursorKey)
	require.True(t, ok)
	hasCursor := func() bool {
		s.pool.peersMu.Lock()
		defer s.pool.peersMu.Unlock()
		_, tracked := s.pool.peers[p.cursorKey]
		return tracked
	}
	require.True(t, hasCursor())

	p.onDone()
	require.False(t, hasCursor())
}

// A message announced and then delivered by another peer before the fetch is
// settled without being asked for.
func TestPeerNextFetchSkipsMessagesThatArrivedMeanwhile(t *testing.T) {
	t.Parallel()
	s, _ := newTestStack(t, StackConfig{})
	p := &PeerConn{stack: s, logger: s.logger, tune: defaultPeerTuning()}
	held := idedMessage(t, "held", soonExpiry())
	_, err := s.pool.Add(held)
	require.NoError(t, err)
	missing := make([]byte, 32)
	missing[0] = 9
	window := []announced{
		{id: held.ID(), size: 100},
		{id: missing, size: 100},
	}

	picked := p.nextFetch(window)
	require.Equal(t, []int{1}, picked)
	require.True(t, window[0].handled, "the held message is settled")
	require.False(t, window[1].handled)
}

// The fetch honours the message and byte limits, but always takes the first
// message so one oversized message is not stuck forever.
func TestPeerNextFetchHonoursItsLimits(t *testing.T) {
	t.Parallel()
	s, _ := newTestStack(t, StackConfig{})
	id := func(b byte) []byte {
		out := make([]byte, 32)
		out[0] = b
		return out
	}
	window := []announced{
		{id: id(1), size: 600},
		{id: id(2), size: 600},
		{id: id(3), size: 100},
	}
	tests := []struct {
		name       string
		messages   int
		bytes      uint64
		window     []announced
		wantPicked []int
	}{
		{"message limit", 2, 1 << 20, window, []int{0, 1}},
		{"byte limit", 10, 1000, window, []int{0}},
		{"oversized first message", 10, 100, window, []int{0}},
		{"all fit", 10, 1 << 20, window, []int{0, 1, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tune := defaultPeerTuning()
			tune.fetchMessages = tt.messages
			tune.fetchBytes = tt.bytes
			p := &PeerConn{stack: s, logger: s.logger, tune: tune}
			win := append([]announced(nil), tt.window...)
			require.Equal(t, tt.wantPicked, p.nextFetch(win))
		})
	}
}

// Only the pulling side can end the protocol. The serving side then stops
// being asked, while the other direction keeps working.
func TestPeerPullingSideDoneEndsOneDirectionOnly(t *testing.T) {
	t.Parallel()
	a, _ := newTestStack(t, StackConfig{})
	b, _ := newTestStack(t, StackConfig{})
	gate := make(chan struct{})
	b.peerTune.pullGate = gate

	pa, pb := attachPair(t, a, b)
	require.Equal(t, uint16(2), pb.Version())
	// b's pulling loop is held back, so its protocol is still in Idle, which
	// is the one state the pulling side may end it from.
	require.NoError(t, pb.puller.Done())
	waitFor(t, "a's serving side sees the Done", pa.server.IsDone)
	close(gate)
	requireClosed(t, "b's pulling loop ends", pb.pullDone)
	require.Zero(t, pb.blockingRequests.Load()+pb.nonBlockingRequests.Load(),
		"nothing is requested once the protocol is done")

	// a still pulls from b, so the connection is up and b serves it.
	addMessages(t, b, "from-b", 1)
	waitFor(t, "a receives b's message", func() bool {
		return a.pool.Len() == 1
	})
	// b no longer pulls: a message a gets now never reaches b.
	addMessages(t, a, "from-a", 1)
	addMessages(t, b, "later-from-b", 1)
	waitFor(t, "a receives the later message", func() bool {
		return a.pool.Len() == 3
	})
	require.Equal(t, 2, b.pool.Len())
	require.False(t, closed(pa.Done()))
	require.False(t, closed(pb.Done()))
	require.NoError(t, pb.Err())
}

// A message the peer sends is held to the stack's own authentication, not only
// to the TTL rule: one that fails it is a violation.
func TestPeerMessageFailingAuthenticationEndsTheConnection(t *testing.T) {
	t.Parallel()
	a, regA := newTestStack(t, StackConfig{
		Authenticator: newNoStakeAuthenticator(t),
	})
	b, _ := newTestStack(t, StackConfig{})
	addMessages(t, b, "unauthenticated", 1)

	pa, _ := attachPair(t, a, b)
	requireClosed(t, "a ends the connection", pa.Done())
	require.Error(t, pa.Err())
	require.Zero(t, a.pool.Len())
	require.Equal(
		t,
		1.0,
		metricValue(t, regA, "dingo_dmq_validation_failures_total", "invalid"),
	)
}

func TestIDSetForgetsTheOldestOnceFull(t *testing.T) {
	t.Parallel()
	set := newIDSet(2)
	id := func(b byte) []byte {
		out := make([]byte, 32)
		out[0] = b
		return out
	}
	set.add(id(1))
	set.add(id(2))
	set.add(id(2))
	require.True(t, set.has(id(1)))
	require.True(t, set.has(id(2)))
	set.add(id(3))
	assert.False(t, set.has(id(1)), "the oldest is forgotten")
	assert.True(t, set.has(id(2)))
	assert.True(t, set.has(id(3)))
	set.add(id(4))
	assert.False(t, set.has(id(2)))
	assert.True(t, set.has(id(3)))
	assert.True(t, set.has(id(4)))
}

func TestIDSetIgnoresMalformedIDs(t *testing.T) {
	t.Parallel()
	set := newIDSet(4)
	set.add([]byte("short"))
	set.add(nil)
	assert.False(t, set.has([]byte("short")))
	assert.False(t, set.has(nil))
	assert.Empty(t, set.set)
}

func TestMessageMempoolHas(t *testing.T) {
	t.Parallel()
	s, _ := newTestStack(t, StackConfig{})
	msg := idedMessage(t, "held", soonExpiry())
	_, err := s.pool.Add(msg)
	require.NoError(t, err)

	assert.True(t, s.pool.Has(msg.ID()))
	assert.False(t, s.pool.Has(make([]byte, 32)))
	assert.False(t, s.pool.Has([]byte("short")))
	assert.False(t, s.pool.Has(nil))
}
