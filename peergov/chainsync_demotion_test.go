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
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var chainsyncDemotionReasons = []string{
	CloseReasonPatienceExhausted,
	CloseReasonEagernessStandoffLost,
}

func closePeerWithReason(pg *PeerGovernor, peer *Peer, reason string) {
	closePeerWithReasonAndError(pg, peer, reason, nil)
}

func closePeerWithReasonAndError(
	pg *PeerGovernor,
	peer *Peer,
	reason string,
	closeErr error,
) {
	connId := outboundTestConnId()
	pg.mu.Lock()
	peer.Connection = &PeerConnection{Id: connId, IsClient: true}
	peer.State = PeerStateHot
	// The session outlived the stable-connection threshold, which on its own
	// resets the backoff to zero.
	peer.ConnectedAt = time.Now().Add(-10 * minStableConnectionDuration)
	pg.mu.Unlock()
	pg.handleConnectionClosedEvent(event.NewEvent(
		connmanager.ConnectionClosedEventType,
		connmanager.ConnectionClosedEvent{
			ConnectionId:     connId,
			Error:            closeErr,
			LocalCloseReason: reason,
		},
	))
}

// A peer closed for exhausting the Limit on Patience or losing a Limit on
// Eagerness standoff is not redialed at once after a long session, even when
// the hot pool is critically low: an immediate redial lets it pin the limit
// again with its first fork header.
func TestHandleConnectionClosedEvent_ChainsyncDemotionDelaysRedial(
	t *testing.T,
) {
	t.Parallel()
	for _, reason := range chainsyncDemotionReasons {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			pg := NewPeerGovernor(PeerGovernorConfig{
				Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			})
			peer := &Peer{
				Address:           "192.168.12.101:3003",
				NormalizedAddress: "192.168.12.101:3003",
				Source:            PeerSourceP2PGossip,
				// Suppress the reconnect goroutine; only the accounting is
				// checked.
				Reconnecting: true,
			}
			pg.mu.Lock()
			pg.peers = []*Peer{peer}
			pg.mu.Unlock()

			closePeerWithReason(pg, peer, reason)

			pg.mu.Lock()
			got := peer.ReconnectDelay
			stalledAt := peer.LastChainsyncStall
			pg.mu.Unlock()
			assert.GreaterOrEqual(t, got,
				chainsyncDemotionReconnectDelay-chainsyncDemotionJitter)
			assert.Less(t, got,
				chainsyncDemotionReconnectDelay+chainsyncDemotionJitter)
			assert.False(t, stalledAt.IsZero(),
				"the demoted peer must rank behind alternates")
		})
	}
}

// An owner-requested close for any other reason after a long session keeps
// the stable-session reset.
func TestHandleConnectionClosedEvent_UnclassifiedLocalCloseResetsBackoff(
	t *testing.T,
) {
	t.Parallel()
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	peer := &Peer{
		Address:           "192.168.12.101:3003",
		NormalizedAddress: "192.168.12.101:3003",
		Source:            PeerSourceP2PGossip,
		Reconnecting:      true,
		ReconnectDelay:    8 * time.Second,
	}
	pg.mu.Lock()
	pg.peers = []*Peer{peer}
	pg.mu.Unlock()

	closePeerWithReason(pg, peer, "stalled_active_connection")

	pg.mu.Lock()
	defer pg.mu.Unlock()
	assert.Zero(t, peer.ReconnectDelay)
}

// A demoted peer's slot goes to a known alternate at once instead of waiting
// for the next reconcile.
func TestHandleConnectionClosedEvent_ChainsyncDemotionDialsColdAlternate(
	t *testing.T,
) {
	t.Parallel()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: logger,
		ConnManager: connmanager.NewConnectionManager(
			connmanager.ConnectionManagerConfig{Logger: logger},
		),
	})
	demoted := &Peer{
		Address:           "192.168.12.101:3003",
		NormalizedAddress: "192.168.12.101:3003",
		Source:            PeerSourceP2PGossip,
		Reconnecting:      true,
	}
	pg.mu.Lock()
	pg.ctx = t.Context()
	pg.stopCh = make(chan struct{})
	pg.peers = []*Peer{
		demoted,
		{
			// Nothing listens here; the dial fails fast and the reconnect
			// loop records the attempt.
			Address:           "127.0.0.1:1",
			NormalizedAddress: "127.0.0.1:1",
			Source:            PeerSourceP2PGossip,
			State:             PeerStateCold,
		},
	}
	pg.mu.Unlock()
	t.Cleanup(func() { _ = pg.Stop(context.Background()) })

	closePeerWithReason(
		pg,
		demoted,
		CloseReasonEagernessStandoffLost,
	)

	require.Eventually(
		t,
		func() bool {
			pg.mu.Lock()
			defer pg.mu.Unlock()
			if idx := pg.peerIndexByAddress("127.0.0.1:1"); idx != -1 {
				alternate := pg.peers[idx]
				return alternate.Reconnecting || alternate.ReconnectCount > 0
			}
			_, denied := pg.denyList["127.0.0.1:1"]
			return denied
		},
		5*time.Second,
		10*time.Millisecond,
		"a cold alternate must be dialed when the hot peer is demoted",
	)
}

func newDemotionTestGovernor(t *testing.T) (*PeerGovernor, *Peer) {
	t.Helper()
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: newMockEventBus(),
	})
	seedTopologyPeer(
		pg,
		"44.0.0.1:3001",
		"44.0.0.1:3001",
		"local-root-0",
		PeerSourceTopologyLocalRoot,
	)
	pg.mu.Lock()
	peer := pg.peers[0]
	// Suppress the reconnect goroutine; only the accounting is checked.
	peer.Reconnecting = true
	pg.mu.Unlock()
	return pg, peer
}

func publishInboundFrom(pg *PeerGovernor, addr string) {
	localAddr, _ := net.ResolveTCPAddr("tcp", "44.0.0.9:3001")
	remoteAddr, _ := net.ResolveTCPAddr("tcp", addr)
	pg.handleInboundConnectionEvent(event.Event{
		Type: connmanager.InboundConnectionEventType,
		Data: connmanager.InboundConnectionEvent{
			ConnectionId: ouroboros.ConnectionId{
				LocalAddr:  localAddr,
				RemoteAddr: remoteAddr,
			},
			LocalAddr:            localAddr,
			RemoteAddr:           remoteAddr,
			NormalizedRemoteAddr: connmanager.NormalizePeerAddr(addr),
		},
	})
}

// A demoted peer that dials this node back proves only that it is reachable,
// which is not why it was demoted, so the inbound arrival keeps the delay.
func TestInboundArrivalKeepsChainsyncDemotionBackoff(t *testing.T) {
	t.Parallel()
	pg, peer := newDemotionTestGovernor(t)
	pg.afterFunc = func(time.Duration, func()) func() bool {
		return func() bool { return true }
	}
	closePeerWithReason(
		pg,
		peer,
		CloseReasonEagernessStandoffLost,
	)

	publishInboundFrom(pg, "44.0.0.1:3001")

	pg.mu.Lock()
	defer pg.mu.Unlock()
	assert.GreaterOrEqual(t, peer.ReconnectDelay,
		chainsyncDemotionReconnectDelay-chainsyncDemotionJitter)
}

// A connection a demoted peer opens during its demotion stays open but is
// withheld from chain selection, and is released when the demotion ends:
// otherwise its first fork header pins the Limit on Eagerness again.
func TestChainsyncDemotionWithholdsUpstreamUntilItEnds(t *testing.T) {
	t.Parallel()
	pg, peer := newDemotionTestGovernor(t)
	var release func()
	var releaseAfter time.Duration
	pg.afterFunc = func(d time.Duration, f func()) func() bool {
		releaseAfter, release = d, f
		return func() bool { return true }
	}
	closePeerWithReason(
		pg,
		peer,
		CloseReasonPatienceExhausted,
	)
	require.NotNil(t, release, "the end of the demotion must be scheduled")

	pg.mu.Lock()
	assert.Equal(t, peer.ReconnectDelay, releaseAfter)
	peer.Connection = &PeerConnection{
		Id:       outboundTestConnId(),
		IsClient: true,
	}
	pg.withholdDeniedUpstreamLocked(peer)
	assert.True(t, peer.Connection.UpstreamWithheld)
	assert.False(t, chainSelectionEligible(
		peer.Source,
		pg.selectionConnLocked(peer),
	))
	// The demotion has ended by the time the timer fires.
	peer.ChainsyncDemotedUntil = time.Now().Add(-time.Second)
	pg.mu.Unlock()

	release()

	pg.mu.Lock()
	defer pg.mu.Unlock()
	assert.False(t, peer.Connection.UpstreamWithheld)
	assert.True(t, chainSelectionEligible(
		peer.Source,
		pg.selectionConnLocked(peer),
	))
}

// The connection manager records the local reason before teardown, so a
// transport error that races it does not cancel the demotion.
func TestChainsyncDemotionSurvivesTransportError(t *testing.T) {
	t.Parallel()
	pg, peer := newDemotionTestGovernor(t)
	pg.afterFunc = func(time.Duration, func()) func() bool {
		return func() bool { return true }
	}
	closePeerWithReasonAndError(
		pg,
		peer,
		CloseReasonEagernessStandoffLost,
		io.ErrUnexpectedEOF,
	)

	pg.mu.Lock()
	defer pg.mu.Unlock()
	assert.GreaterOrEqual(t, peer.ReconnectDelay,
		chainsyncDemotionReconnectDelay-chainsyncDemotionJitter)
	assert.True(t, chainsyncDemoted(peer, time.Now()))
}

// Stop cancels a pending demotion release, and a release that already fired
// does nothing once the governor has stopped.
func TestStopCancelsChainsyncDemotionRelease(t *testing.T) {
	t.Parallel()
	pg, peer := newDemotionTestGovernor(t)
	var release func()
	stopped := false
	pg.afterFunc = func(_ time.Duration, f func()) func() bool {
		release = f
		return func() bool { stopped = true; return true }
	}
	closePeerWithReason(pg, peer, CloseReasonPatienceExhausted)
	require.NotNil(t, release)
	pg.mu.Lock()
	peer.Connection = &PeerConnection{
		Id:               outboundTestConnId(),
		IsClient:         true,
		UpstreamWithheld: true,
	}
	peer.ChainsyncDemotedUntil = time.Now().Add(-time.Second)
	pg.mu.Unlock()

	require.NoError(t, pg.Stop(context.Background()))
	release()

	assert.True(t, stopped, "Stop must cancel the release timer")
	pg.mu.Lock()
	defer pg.mu.Unlock()
	assert.True(t, peer.Connection.UpstreamWithheld,
		"a release after Stop must not change the stopped governor")
}

// Recovery redials skip a demoted peer, so its slot goes to an alternate.
func TestRedialCandidatesSkipDemotedPeer(t *testing.T) {
	t.Parallel()
	pg, peer := newDemotionTestGovernor(t)
	pg.mu.Lock()
	defer pg.mu.Unlock()
	peer.Reconnecting = false
	require.Contains(t, pg.redialCandidatesLocked(), peer)

	peer.ChainsyncDemotedUntil = time.Now().Add(time.Minute)

	assert.NotContains(t, pg.redialCandidatesLocked(), peer)
}

// A reconnect worker that was already running when the peer was demoted
// waits out the demotion before dialing.
func TestOutboundDialWaitsOutChainsyncDemotion(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: logger,
		ConnManager: connmanager.NewConnectionManager(
			connmanager.ConnectionManagerConfig{Logger: logger},
		),
	})
	peer := &Peer{
		// Nothing listens here, so a dial fails at once and is counted.
		Address:               "127.0.0.1:1",
		NormalizedAddress:     "127.0.0.1:1",
		Source:                PeerSourceTopologyLocalRoot,
		ChainsyncDemotedUntil: time.Now().Add(time.Hour),
	}
	pg.mu.Lock()
	pg.ctx = t.Context()
	pg.stopCh = make(chan struct{})
	pg.peers = []*Peer{peer}
	pg.mu.Unlock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		pg.createOutboundConnection(peer, false)
	}()

	require.Eventually(t, func() bool {
		pg.mu.Lock()
		defer pg.mu.Unlock()
		return peer.Reconnecting
	}, 5*time.Second, time.Millisecond)
	require.Never(t, func() bool {
		pg.mu.Lock()
		defer pg.mu.Unlock()
		return peer.ReconnectCount > 0
	}, 300*time.Millisecond, 5*time.Millisecond,
		"a demoted peer must not be dialed")

	require.NoError(t, pg.Stop(context.Background()))
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the reconnect worker did not stop")
	}
}

// An outbound connection that completes while the peer is demoted is kept
// but withheld from chain selection.
func TestOutboundConnectionDuringDemotionIsWithheld(t *testing.T) {
	t.Parallel()
	pg, peer := newDemotionTestGovernor(t)
	conn, err := ouroboros.NewConnection()
	require.NoError(t, err)
	pg.mu.Lock()
	defer pg.mu.Unlock()
	peer.ChainsyncDemotedUntil = time.Now().Add(time.Minute)

	pg.attachOutboundConnLocked(peer, conn, time.Now())

	require.NotNil(t, peer.Connection)
	assert.True(t, peer.Connection.UpstreamWithheld)
}

// A real full-duplex inbound connection from a demoted topology peer stays
// open but is not a chain selection source until the demotion ends.
func TestRealInboundArrivalDuringChainsyncDemotionIsWithheld(t *testing.T) {
	t.Parallel()
	f := newWithholdFixture(t, PeerSourceTopologyLocalRoot)
	var release func()
	f.pg.afterFunc = func(_ time.Duration, fn func()) func() bool {
		release = fn
		return func() bool { return true }
	}
	f.pg.mu.Lock()
	peer := f.pg.peers[0]
	peer.Reconnecting = true
	f.pg.mu.Unlock()
	closePeerWithReason(f.pg, peer, CloseReasonEagernessStandoffLost)
	require.NotNil(t, release)

	f.pg.handleInboundConnectionEvent(f.arrival)

	require.NotNil(t, f.pg.GetPeers()[0].Connection,
		"the connection stays open for downstream use")
	assert.False(t, f.pg.IsChainSelectionEligible(f.connId))
	assert.False(t, nextEligibilityOf(t, f).Eligible)

	f.pg.mu.Lock()
	peer.ChainsyncDemotedUntil = time.Now().Add(-time.Second)
	f.pg.mu.Unlock()
	release()

	assert.True(t, f.pg.IsChainSelectionEligible(f.connId))
	assert.True(t, nextEligibilityOf(t, f).Eligible)
}

// nextEligibilityOf skips events for connections other than the fixture's,
// such as the closed connection that set up the demotion.
func nextEligibilityOf(
	t *testing.T,
	f *withholdFixture,
) PeerEligibilityChangedEvent {
	t.Helper()
	for {
		evt := f.nextEligibility(t)
		if evt.ConnectionId.String() == f.connId.String() {
			return evt
		}
	}
}

// A demotion still in force when the governor restarts gets its release
// scheduled again: Stop cancelled the first one.
func TestStartReschedulesActiveDemotionRelease(t *testing.T) {
	t.Parallel()
	pg, peer := newDemotionTestGovernor(t)
	var mu sync.Mutex
	var scheduled []time.Duration
	pg.afterFunc = func(d time.Duration, _ func()) func() bool {
		mu.Lock()
		defer mu.Unlock()
		scheduled = append(scheduled, d)
		return func() bool { return true }
	}
	closePeerWithReason(pg, peer, CloseReasonPatienceExhausted)
	require.NoError(t, pg.Stop(context.Background()))

	require.NoError(t, pg.Start(t.Context()))
	t.Cleanup(func() { _ = pg.Stop(context.Background()) })

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, scheduled, 2)
	assert.Positive(t, scheduled[1])
	assert.LessOrEqual(t, scheduled[1], scheduled[0])
}
