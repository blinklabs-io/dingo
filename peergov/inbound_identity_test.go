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
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newInboundIdentityGovernor(t *testing.T) *PeerGovernor {
	t.Helper()
	return NewPeerGovernor(PeerGovernorConfig{
		Logger:          slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:        event.NewEventBus(nil, nil),
		PromRegistry:    prometheus.NewRegistry(),
		InboundCooldown: 5 * time.Minute,
		DenyDuration:    time.Minute,
	})
}

func inboundArrival(t *testing.T, remote string) event.Event {
	t.Helper()
	localAddr, err := net.ResolveTCPAddr("tcp", "44.0.0.9:3001")
	require.NoError(t, err)
	remoteAddr, err := net.ResolveTCPAddr("tcp", remote)
	require.NoError(t, err)
	return event.Event{
		Type: connmanager.InboundConnectionEventType,
		Data: connmanager.InboundConnectionEvent{
			ConnectionId: ouroboros.ConnectionId{
				LocalAddr:  localAddr,
				RemoteAddr: remoteAddr,
			},
			LocalAddr:  localAddr,
			RemoteAddr: remoteAddr,
			NormalizedRemoteAddr: connmanager.NormalizePeerAddr(
				remoteAddr.String(),
			),
		},
	}
}

func deniedInboundCount(pg *PeerGovernor) float64 {
	return promtestutil.ToFloat64(
		pg.metrics.inboundLifecycle.WithLabelValues("denied"),
	)
}

// Denying an inbound peer must not turn its host away: the peer may be a
// downstream consumer that dials us from a fresh source port, and refusing
// the connection cuts it off. Inbound-only records are never a chain
// selection source, so there is no upstream role to withhold from them.
func TestInboundHostDenialDoesNotRefuseArrival(t *testing.T) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	seedTopologyPeer(
		pg, "44.0.0.1:3001", "44.0.0.1:3001",
		"local-root-0", PeerSourceTopologyLocalRoot,
	)
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(44, 0, 0, 9), Port: 3001},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(44, 0, 0, 1), Port: 3001},
	}
	pg.mu.Lock()
	pg.peers[0].Connection = &PeerConnection{Id: connId, IsClient: true}
	pg.mu.Unlock()
	first := inboundArrival(t, "44.0.0.1:51000")
	pg.handleInboundConnectionEvent(first)
	require.Equal(t, 1, len(pg.GetPeers()))

	pg.DenyPeer("44.0.0.1:3001", time.Minute)

	for port := 51001; port < 51006; port++ {
		pg.handleInboundConnectionEvent(
			inboundArrival(t, fmt.Sprintf("44.0.0.1:%d", port)),
		)
	}
	assert.Equal(
		t,
		float64(0),
		deniedInboundCount(pg),
		"a host denied on one source port must still be admitted on another",
	)
	assert.Equal(t, 1, len(pg.GetPeers()), "the host keeps one record")
	assert.Equal(
		t,
		"local-root-0",
		pg.GetPeers()[0].InboundTopologyMatch,
		"the denied topology peer's new-port arrival remains admitted",
	)
	peers := pg.GetPeers()
	require.NotNil(t, peers[0].Connection)
	assert.True(
		t,
		sameConnectionId(connId, peers[0].Connection.Id),
		"host-scoped denial must leave the existing peer connection intact",
	)
}

// A host that flaps is not refused either: refusal also cuts off a
// downstream that reconnects because this node closed its sessions.
func TestInboundFlappingHostIsNotRefused(t *testing.T) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address:                "44.0.0.1:51000",
		NormalizedAddress:      "44.0.0.1:51000",
		Source:                 PeerSourceInboundConn,
		State:                  PeerStateCold,
		FirstSeen:              time.Now().Add(-time.Hour),
		LastInboundDisconnect:  time.Now(),
		InboundShortLivedCount: 3,
	})
	pg.mu.Unlock()

	pg.handleInboundConnectionEvent(inboundArrival(t, "44.0.0.1:51001"))

	assert.Equal(t, float64(0), deniedInboundCount(pg))
	peers := pg.GetPeers()
	require.Equal(t, 1, len(peers))
	assert.False(
		t,
		peers[0].InboundConnectedAt.IsZero(),
		"the arrival must be attributed to the host's record",
	)
}

// A reconnect from a new source port reuses the host's disconnected record, so
// its short-session history is not reset by port rotation.
func TestInboundRecordReuseKeepsShortSessionHistory(t *testing.T) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address:                "44.0.0.1:51000",
		NormalizedAddress:      "44.0.0.1:51000",
		Source:                 PeerSourceInboundConn,
		State:                  PeerStateCold,
		FirstSeen:              time.Now().Add(-time.Hour),
		LastInboundDisconnect:  time.Now().Add(-5 * time.Second),
		InboundShortLivedCount: 1,
	})
	pg.mu.Unlock()

	pg.handleInboundConnectionEvent(inboundArrival(t, "44.0.0.1:51001"))

	peers := pg.GetPeers()
	require.Equal(t, 1, len(peers))
	assert.Equal(t, uint32(1), peers[0].InboundShortLivedCount)
}

func closeInbound(
	t *testing.T,
	pg *PeerGovernor,
	arrival event.Event,
	closeErr error,
) {
	t.Helper()
	connId := arrival.Data.(connmanager.InboundConnectionEvent).ConnectionId
	pg.mu.Lock()
	pg.peers[0].Connection = &PeerConnection{Id: connId, IsClient: true}
	pg.mu.Unlock()
	pg.handleConnectionClosedEvent(event.Event{
		Type: connmanager.ConnectionClosedEventType,
		Data: connmanager.ConnectionClosedEvent{
			ConnectionId: connId,
			Error:        closeErr,
		},
	})
}

// A peer that completes the handshake and disconnects reaches this node as a
// close with no error, because the connection layer suppresses the close
// error when every protocol is still idle. Those sessions are the flapping
// pattern and must accumulate.
func TestInboundShortSessionCleanCloseIsCounted(t *testing.T) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	for port := 51000; port < 51004; port++ {
		arrival := inboundArrival(t, fmt.Sprintf("44.0.0.1:%d", port))
		pg.handleInboundConnectionEvent(arrival)
		closeInbound(t, pg, arrival, nil)
	}
	peers := pg.GetPeers()
	require.Equal(t, 1, len(peers))
	assert.Equal(t, uint32(4), peers[0].InboundShortLivedCount)
}

// A short session the peer ended with an error counts as well.
func TestInboundShortSessionEndedByPeerErrorIsCounted(t *testing.T) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	for port := 51000; port < 51003; port++ {
		arrival := inboundArrival(t, fmt.Sprintf("44.0.0.1:%d", port))
		pg.handleInboundConnectionEvent(arrival)
		closeInbound(t, pg, arrival, io.ErrUnexpectedEOF)
	}
	peers := pg.GetPeers()
	require.Equal(t, 1, len(peers))
	assert.Equal(t, uint32(3), peers[0].InboundShortLivedCount)
}

// Pruning a flapping warm peer keeps the host cooldown and escalation even
// when its next connection uses a different source port.
func TestInboundFlappingPruneKeepsHostHistory(t *testing.T) {
	t.Parallel()
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:          event.NewEventBus(nil, nil),
		PromRegistry:      prometheus.NewRegistry(),
		InboundPruneAfter: time.Minute,
		InboundCooldown:   2 * time.Minute,
		DenyDuration:      30 * time.Second,
	})
	now := time.Now()
	pg.mu.Lock()
	pg.peers = []*Peer{{
		Address:                "44.0.0.1:51000",
		NormalizedAddress:      "44.0.0.1:51000",
		Source:                 PeerSourceInboundConn,
		State:                  PeerStateWarm,
		FirstSeen:              now.Add(-time.Hour),
		LastActivity:           now.Add(-time.Hour),
		LastInboundDisconnect:  now.Add(-30 * time.Second),
		InboundShortLivedCount: 4,
	}}
	pg.mu.Unlock()
	pg.reconcile(t.Context())
	require.Equal(t, 0, len(pg.GetPeers()), "flapping peer is pruned")

	pg.handleInboundConnectionEvent(inboundArrival(t, "44.0.0.1:51001"))
	assert.Equal(t, float64(1), deniedInboundCount(pg))
	assert.Empty(t, pg.GetPeers())

	pg.mu.Lock()
	hostKey := inboundFlapHostKey("44.0.0.1:51001")
	history := pg.inboundFlapHistory[hostKey]
	require.Equal(t, uint32(4), history.shortLivedCount)
	history.cooldownUntil = time.Now().Add(-time.Second)
	pg.inboundFlapHistory[hostKey] = history
	pg.mu.Unlock()

	arrival := inboundArrival(t, "44.0.0.1:51002")
	pg.handleInboundConnectionEvent(arrival)
	peers := pg.GetPeers()
	require.Len(t, peers, 1)
	assert.Equal(t, uint32(4), peers[0].InboundShortLivedCount)
	pg.mu.Lock()
	flapping, _ := pg.inboundFlappingStateLocked(pg.peers[0], time.Now())
	pg.mu.Unlock()
	assert.False(t, flapping, "the admitted session can prove stability")
	closeInbound(t, pg, arrival, nil)
	peers = pg.GetPeers()
	require.Len(t, peers, 1)
	assert.Equal(t, uint32(5), peers[0].InboundShortLivedCount)

}

// A denied topology peer that reconnects from a new source port keeps its
// connection but is not a chain selection source; an undenied one is.
func TestWithholdDeniedUpstreamKeepsConnectionOutOfChainSelection(
	t *testing.T,
) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	seedTopologyPeer(
		pg, "44.0.0.1:3001", "44.0.0.1:3001",
		"local-root-0", PeerSourceTopologyLocalRoot,
	)
	arrival := inboundArrival(t, "44.0.0.1:51001")
	connId := arrival.Data.(connmanager.InboundConnectionEvent).ConnectionId
	attach := func() {
		pg.mu.Lock()
		defer pg.mu.Unlock()
		pg.peers[0].Connection = &PeerConnection{Id: connId, IsClient: true}
		pg.withholdDeniedUpstreamLocked(pg.peers[0])
	}

	attach()
	assert.True(t, pg.IsChainSelectionEligible(connId), "control: not denied")

	pg.DenyPeer("44.0.0.1:3001", time.Minute)
	attach()
	assert.False(t, pg.IsChainSelectionEligible(connId))
	assert.NotNil(t, pg.peers[0].Connection, "the connection stays open")
}

// A denied topology peer's arrival is admitted rather than closed.
func TestInboundTopologyDenialDoesNotRefuseArrival(t *testing.T) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	seedTopologyPeer(
		pg, "44.0.0.1:3001", "44.0.0.1:3001",
		"local-root-0", PeerSourceTopologyLocalRoot,
	)
	pg.DenyPeer("44.0.0.1:3001", time.Minute)

	pg.handleInboundConnectionEvent(inboundArrival(t, "44.0.0.1:51001"))

	assert.Equal(t, float64(0), deniedInboundCount(pg))
	assert.False(t, pg.GetPeers()[0].InboundConnectedAt.IsZero())
}
