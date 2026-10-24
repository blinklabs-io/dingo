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

func connectInboundIdentityPeer(
	pg *PeerGovernor,
	idx int,
	remoteIP net.IP,
	remotePort int,
) ouroboros.ConnectionId {
	connId := ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{
			IP:   net.IPv4(44, 0, 0, 9),
			Port: 3001,
		},
		RemoteAddr: &net.TCPAddr{IP: remoteIP, Port: remotePort},
	}
	pg.mu.Lock()
	defer pg.mu.Unlock()
	pg.peers[idx].Connection = &PeerConnection{Id: connId, IsClient: true}
	pg.withholdDeniedUpstreamLocked(pg.peers[idx])
	return connId
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
	require.Equal(t, 2, len(pg.GetPeers()))

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
	assert.Equal(t, 7, len(pg.GetPeers()), "each transport identity is retained")
	peers := pg.GetPeers()
	for _, peer := range peers {
		assert.Empty(t, peer.InboundTopologyMatch,
			"same-host arrivals must not inherit topology privileges")
	}
	require.NotNil(t, peers[0].Connection)
	assert.True(
		t,
		sameConnectionId(connId, peers[0].Connection.Id),
		"host-scoped denial must leave the existing peer connection intact",
	)
}

// A disconnected inbound peer does not capture an unrelated arrival from a
// different source port on the same NAT gateway.
func TestInboundDisconnectedPeerDoesNotCaptureSharedNATArrival(t *testing.T) {
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
	require.Equal(t, 2, len(peers))
	assert.False(
		t,
		peers[1].InboundConnectedAt.IsZero(),
		"the arrival must get an identity distinct from another NAT peer",
	)
}

// A reconnect from the same transport identity reuses its disconnected record,
// so its short-session history remains effective.
func TestInboundExactRecordReuseKeepsShortSessionHistory(t *testing.T) {
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

	pg.handleInboundConnectionEvent(inboundArrival(t, "44.0.0.1:51000"))

	peers := pg.GetPeers()
	require.Equal(t, 1, len(peers))
	assert.Equal(t, uint32(1), peers[0].InboundShortLivedCount)
}

func closeInbound(
	t *testing.T,
	pg *PeerGovernor,
	peerIdx int,
	arrival event.Event,
	closeErr error,
) {
	t.Helper()
	connId := arrival.Data.(connmanager.InboundConnectionEvent).ConnectionId
	pg.mu.Lock()
	pg.peers[peerIdx].Connection = &PeerConnection{Id: connId, IsClient: true}
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
	for range 4 {
		arrival := inboundArrival(t, "44.0.0.1:51000")
		pg.handleInboundConnectionEvent(arrival)
		closeInbound(t, pg, 0, arrival, nil)
	}
	peers := pg.GetPeers()
	require.Equal(t, 1, len(peers))
	assert.Equal(t, uint32(4), peers[0].InboundShortLivedCount)
}

// A short session the peer ended with an error counts as well.
func TestInboundShortSessionEndedByPeerErrorIsCounted(t *testing.T) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	for range 3 {
		arrival := inboundArrival(t, "44.0.0.1:51000")
		pg.handleInboundConnectionEvent(arrival)
		closeInbound(t, pg, 0, arrival, io.ErrUnexpectedEOF)
	}
	peers := pg.GetPeers()
	require.Equal(t, 1, len(peers))
	assert.Equal(t, uint32(3), peers[0].InboundShortLivedCount)
}

func TestInboundShortSessionPortRotationHitsHostBackstop(t *testing.T) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	limit := pg.inboundHostChurnLimit()
	hostKey := inboundFlapHostKey("44.0.0.1:51000")
	pg.mu.Lock()
	pg.inboundHostChurn[hostKey] = inboundHostChurnRecord{
		shortLivedCount: uint32(limit - 1),
		windowStarted:   time.Now(),
		expiresAt:       time.Now().Add(pg.config.InboundCooldown),
	}
	pg.mu.Unlock()

	arrival := inboundArrival(t, "44.0.0.1:51000")
	pg.handleInboundConnectionEvent(arrival)
	closeInbound(t, pg, 0, arrival, nil)
	pg.handleInboundConnectionEvent(inboundArrival(t, "44.0.0.1:51001"))

	assert.Equal(t, float64(1), deniedInboundCount(pg))
	require.Len(t, pg.GetPeers(), 1)
}

func TestInboundIPv6RotationWithinPrefixHitsHostBackstop(t *testing.T) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	limit := pg.inboundHostChurnLimit()
	hostKey := inboundFlapHostKey("[2001:db8:1::1]:51000")
	pg.mu.Lock()
	pg.inboundHostChurn[hostKey] = inboundHostChurnRecord{
		shortLivedCount: uint32(limit - 1),
		windowStarted:   time.Now(),
		expiresAt:       time.Now().Add(pg.config.InboundCooldown),
	}
	pg.mu.Unlock()

	arrival := inboundArrival(t, "[2001:db8:1::2]:51001")
	pg.handleInboundConnectionEvent(arrival)
	closeInbound(t, pg, 0, arrival, nil)
	pg.handleInboundConnectionEvent(
		inboundArrival(t, "[2001:db8:1::1]:51002"),
	)

	assert.Equal(t, float64(1), deniedInboundCount(pg))
	require.Len(t, pg.GetPeers(), 1)
}

func TestInboundHostChurnActiveCooldownSurvivesWindowExpiry(t *testing.T) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	now := time.Now()
	hostKey := inboundFlapHostKey("44.0.0.1:51000")
	windowStarted := now.Add(-2 * pg.config.InboundCooldown)
	pg.inboundHostChurn[hostKey] = inboundHostChurnRecord{
		shortLivedCount: uint32(pg.inboundHostChurnLimit()),
		windowStarted:   windowStarted,
		cooldownUntil:   now.Add(pg.config.InboundCooldown / 2),
		expiresAt:       now.Add(pg.config.InboundCooldown / 2),
	}

	pg.rememberInboundHostChurnLocked(&Peer{
		NormalizedAddress: "44.0.0.1:51000",
	}, now)

	record := pg.inboundHostChurn[hostKey]
	assert.Equal(t, windowStarted, record.windowStarted)
	assert.Equal(
		t,
		uint32(pg.inboundHostChurnLimit()+1),
		record.shortLivedCount,
	)
	assert.Equal(t, now.Add(pg.config.InboundCooldown), record.cooldownUntil)
}

func TestInboundHostChurnCapacityFailsClosed(t *testing.T) {
	t.Parallel()
	now := time.Now()
	pg := newInboundIdentityGovernor(t)
	pg.mu.Lock()
	for i := range pg.maxPeerListSize() {
		pg.inboundHostChurn[fmt.Sprintf("host-%d", i)] =
			inboundHostChurnRecord{expiresAt: now.Add(time.Minute)}
	}
	pg.mu.Unlock()

	pg.handleInboundConnectionEvent(inboundArrival(t, "44.0.0.1:51000"))
	assert.Equal(t, float64(1), deniedInboundCount(pg))
	assert.Empty(t, pg.GetPeers())

	pg.mu.Lock()
	expired := pg.inboundHostChurn["host-0"]
	expired.expiresAt = time.Now().Add(-time.Second)
	pg.inboundHostChurn["host-0"] = expired
	pg.mu.Unlock()
	pg.handleInboundConnectionEvent(inboundArrival(t, "44.0.0.2:51000"))
	assert.Equal(t, float64(1), deniedInboundCount(pg))
	require.Len(t, pg.GetPeers(), 1)

	topologyPG := newInboundIdentityGovernor(t)
	topologyPG.mu.Lock()
	for i := range topologyPG.maxPeerListSize() {
		topologyPG.inboundHostChurn[fmt.Sprintf("host-%d", i)] =
			inboundHostChurnRecord{expiresAt: now.Add(time.Minute)}
	}
	topologyPG.mu.Unlock()
	seedTopologyPeer(
		topologyPG, "44.0.0.3:3001", "44.0.0.3:3001",
		"local-root-0", PeerSourceTopologyLocalRoot,
	)
	topologyPG.handleInboundConnectionEvent(
		inboundArrival(t, "44.0.0.3:3001"),
	)
	assert.Equal(t, float64(0), deniedInboundCount(topologyPG))
	require.Len(t, topologyPG.GetPeers(), 1)
}

func TestInboundStableNATPeerDoesNotSpendHostBackstop(t *testing.T) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	arrival := inboundArrival(t, "44.0.0.1:51000")
	pg.handleInboundConnectionEvent(arrival)
	pg.mu.Lock()
	pg.peers[0].InboundConnectedAt = time.Now().Add(
		-minStableConnectionDuration,
	)
	pg.mu.Unlock()
	closeInbound(t, pg, 0, arrival, nil)

	pg.mu.Lock()
	_, tracked := pg.inboundHostChurn[inboundFlapHostKey("44.0.0.1:51000")]
	pg.mu.Unlock()
	assert.False(t, tracked)
	pg.handleInboundConnectionEvent(inboundArrival(t, "44.0.0.1:51001"))
	assert.Equal(t, float64(0), deniedInboundCount(pg))
}

// Pruning a flapping warm peer cools down that transport identity without
// refusing another peer that arrives through the same NAT gateway.
func TestInboundFlappingPruneDoesNotCooldownSharedNAT(t *testing.T) {
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
	assert.Equal(t, float64(0), deniedInboundCount(pg))
	require.Len(t, pg.GetPeers(), 1)

	pg.handleInboundConnectionEvent(inboundArrival(t, "44.0.0.1:51000"))
	assert.Equal(t, float64(1), deniedInboundCount(pg))
	require.Len(t, pg.GetPeers(), 1)

	pg.mu.Lock()
	flapKey := "44.0.0.1:51000"
	history := pg.inboundFlapHistory[flapKey]
	require.Equal(t, uint32(4), history.shortLivedCount)
	history.cooldownUntil = time.Now().Add(-time.Second)
	pg.inboundFlapHistory[flapKey] = history
	pg.denyList["44.0.0.1:51000"] = time.Now().Add(-time.Second)
	pg.mu.Unlock()

	arrival := inboundArrival(t, "44.0.0.1:51000")
	pg.handleInboundConnectionEvent(arrival)
	peers := pg.GetPeers()
	require.Len(t, peers, 2)
	assert.Equal(t, uint32(4), peers[1].InboundShortLivedCount)
	pg.mu.Lock()
	flapping, _ := pg.inboundFlappingStateLocked(pg.peers[1], time.Now())
	pg.mu.Unlock()
	assert.False(t, flapping, "the admitted session can prove stability")
	closeInbound(t, pg, 1, arrival, nil)
	peers = pg.GetPeers()
	require.Len(t, peers, 2)
	assert.Equal(t, uint32(5), peers[1].InboundShortLivedCount)
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

// A denial on a topology peer's configured address does not follow the host
// to another source port: the arrival is a fresh inbound identity, admitted
// rather than closed, and never inherits the topology peer's identity.
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
	peers := pg.GetPeers()
	require.Len(t, peers, 2)
	assert.True(t, peers[0].InboundConnectedAt.IsZero())
	assert.Empty(t, peers[1].InboundTopologyMatch)
	assert.True(t, PeerSourceInboundConn == peers[1].Source)
}

// A peer denied while connected stays withheld from chain selection, and a
// reconnect from a new source port is a separate inbound-only identity that is
// never chain-selection eligible, so selection recovers on the honest peer.
func TestDeniedPeerStaysWithheldAcrossSourcePorts(t *testing.T) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	seedTopologyPeer(
		pg, "44.0.0.1:3001", "44.0.0.1:3001",
		"bad-root", PeerSourceTopologyPublicRoot,
	)
	seedTopologyPeer(
		pg, "44.0.0.2:3001", "44.0.0.2:3001",
		"honest-root", PeerSourceTopologyPublicRoot,
	)
	badId := connectInboundIdentityPeer(
		pg, 0, net.IPv4(44, 0, 0, 1), 51000,
	)
	honestId := connectInboundIdentityPeer(
		pg, 1, net.IPv4(44, 0, 0, 2), 3001,
	)
	require.True(t, pg.IsChainSelectionEligible(badId), "control")
	require.True(t, pg.IsChainSelectionEligible(honestId), "control")

	pg.DenyPeer(badId.RemoteAddr.String(), time.Minute)
	assert.False(t, pg.IsChainSelectionEligible(badId))

	for port := 51001; port < 51004; port++ {
		arrival := inboundArrival(t, fmt.Sprintf("44.0.0.1:%d", port))
		pg.handleInboundConnectionEvent(arrival)
		reconnectId := arrival.Data.(connmanager.InboundConnectionEvent).ConnectionId
		assert.False(
			t,
			pg.IsChainSelectionEligible(reconnectId),
			"reconnect from source port %d is not an upstream",
			port,
		)
		assert.False(t, pg.IsChainSelectionEligible(badId))
		assert.True(
			t,
			pg.IsChainSelectionEligible(honestId),
			"the honest peer remains the eligible upstream",
		)
	}
}

func TestStalePortDenialDoesNotCollapseSharedTopologyHost(t *testing.T) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	seedTopologyPeer(
		pg, "44.0.0.1:3001", "44.0.0.1:3001",
		"first-root", PeerSourceTopologyPublicRoot,
	)
	seedTopologyPeer(
		pg, "44.0.0.1:3002", "44.0.0.1:3002",
		"second-root", PeerSourceTopologyPublicRoot,
	)
	first := connectInboundIdentityPeer(pg, 0, net.IPv4(44, 0, 0, 1), 51001)
	second := connectInboundIdentityPeer(pg, 1, net.IPv4(44, 0, 0, 1), 51002)

	pg.DenyPeer("44.0.0.1:51000", time.Minute)

	assert.True(t, pg.IsChainSelectionEligible(first))
	assert.True(t, pg.IsChainSelectionEligible(second))
}

func TestStalePortDenialDoesNotDenyUnrelatedPeerOnSharedHost(t *testing.T) {
	t.Parallel()
	pg := newInboundIdentityGovernor(t)
	seedTopologyPeer(
		pg, "44.0.0.1:3001", "44.0.0.1:3001",
		"bad-root", PeerSourceTopologyPublicRoot,
	)
	pg.peers = append(pg.peers, &Peer{
		Address:           "44.0.0.1:3002",
		NormalizedAddress: "44.0.0.1:3002",
		Source:            PeerSourceP2PGossip,
	})
	bad := connectInboundIdentityPeer(pg, 0, net.IPv4(44, 0, 0, 1), 51001)
	unrelated := connectInboundIdentityPeer(
		pg, 1, net.IPv4(44, 0, 0, 1), 51002,
	)

	pg.DenyPeer("44.0.0.1:51000", time.Minute)

	assert.True(t, pg.IsChainSelectionEligible(bad))
	assert.True(t, pg.IsChainSelectionEligible(unrelated))
}
