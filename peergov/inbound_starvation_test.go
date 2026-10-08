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

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func starvationConnId(i int) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 3001},
		RemoteAddr: &net.TCPAddr{
			IP:   net.IPv4(10, 1, byte(i/250), byte(i%250+1)),
			Port: 3001,
		},
	}
}

func starvationPeer(
	i int,
	source PeerSource,
	state PeerState,
	isClient bool,
) *Peer {
	addr := fmt.Sprintf("10.1.%d.%d:3001", i/250, i%250+1)
	return &Peer{
		Address:           addr,
		NormalizedAddress: addr,
		Source:            source,
		State:             state,
		FirstSeen:         time.Now(),
		Connection: &PeerConnection{
			Id:       starvationConnId(i),
			IsClient: isClient,
		},
	}
}

func newStarvationGovernor(cfg PeerGovernorConfig) *PeerGovernor {
	cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	cfg.PromRegistry = prometheus.NewRegistry()
	return NewPeerGovernor(cfg)
}

func hotCountBySource(pg *PeerGovernor, source PeerSource) int {
	pg.mu.Lock()
	defer pg.mu.Unlock()
	n := 0
	for _, p := range pg.peers {
		if p != nil && p.Source == source && p.State == PeerStateHot {
			n++
		}
	}
	return n
}

// Starting a chainsync client marks the peer hot; that must not push the
// hot count past the active target or the per-source quota.
func TestSetPeerHotByConnIdRespectsActiveTarget(t *testing.T) {
	t.Parallel()
	pg := newStarvationGovernor(PeerGovernorConfig{
		TargetNumberOfActivePeers: 3,
		ActivePeersGossipQuota:    20,
	})
	const n = 10
	pg.mu.Lock()
	for i := range n {
		pg.peers = append(
			pg.peers,
			starvationPeer(i, PeerSourceP2PGossip, PeerStateWarm, true),
		)
	}
	pg.mu.Unlock()
	for i := range n {
		pg.SetPeerHotByConnId(starvationConnId(i))
	}
	assert.Equal(
		t, 3, hotCountBySource(pg, PeerSourceP2PGossip),
		"chainsync start must not promote past TargetNumberOfActivePeers",
	)
}

func TestSetPeerHotByConnIdRespectsSourceQuota(t *testing.T) {
	t.Parallel()
	pg := newStarvationGovernor(PeerGovernorConfig{
		TargetNumberOfActivePeers: 20,
		ActivePeersGossipQuota:    2,
	})
	const n = 10
	pg.mu.Lock()
	for i := range n {
		pg.peers = append(
			pg.peers,
			starvationPeer(i, PeerSourceP2PGossip, PeerStateWarm, true),
		)
	}
	pg.mu.Unlock()
	for i := range n {
		pg.SetPeerHotByConnId(starvationConnId(i))
	}
	assert.Equal(
		t, 2, hotCountBySource(pg, PeerSourceP2PGossip),
		"chainsync start must not promote past ActivePeersGossipQuota",
	)
}

func TestSetPeerHotByConnIdRespectsInboundHotQuota(t *testing.T) {
	t.Parallel()
	pg := newStarvationGovernor(PeerGovernorConfig{
		TargetNumberOfActivePeers: 20,
		InboundHotQuota:           2,
	})
	const n = 10
	pg.mu.Lock()
	for i := range n {
		p := starvationPeer(i, PeerSourceInboundConn, PeerStateWarm, true)
		p.PerformanceScore = 1
		p.FirstSeen = time.Now().Add(-time.Hour)
		p.ChainSyncLastUpdate = time.Now()
		p.TipSlotDeltaInit = true
		pg.peers = append(pg.peers, p)
	}
	pg.mu.Unlock()
	for i := range n {
		pg.SetPeerHotByConnId(starvationConnId(i))
	}
	assert.Equal(
		t, 2, hotCountBySource(pg, PeerSourceInboundConn),
		"chainsync start must not promote past InboundHotQuota",
	)
}

// Inbound hot peers must not consume the outbound active target, and a
// local root must still be promoted when the target is full.
func TestSetPeerHotByConnIdLocalRootAndInboundIndependence(t *testing.T) {
	t.Parallel()
	pg := newStarvationGovernor(PeerGovernorConfig{
		TargetNumberOfActivePeers: 1,
		ActivePeersGossipQuota:    20,
	})
	pg.mu.Lock()
	pg.peers = []*Peer{
		starvationPeer(0, PeerSourceP2PGossip, PeerStateHot, true),
		starvationPeer(1, PeerSourceTopologyLocalRoot, PeerStateWarm, true),
	}
	pg.mu.Unlock()
	pg.SetPeerHotByConnId(starvationConnId(1))
	assert.Equal(
		t, 1, hotCountBySource(pg, PeerSourceTopologyLocalRoot),
		"local roots are never held back by the active target",
	)
}

// Outbound peers at/over target must not cause inbound peers to be closed.
func TestEnforcePeerLimitsNeverPrunesInbound(t *testing.T) {
	t.Parallel()
	pg := newStarvationGovernor(PeerGovernorConfig{
		TargetNumberOfActivePeers:      2,
		TargetNumberOfEstablishedPeers: 3,
		TargetNumberOfKnownPeers:       500,
	})
	const inbound = 30
	pg.mu.Lock()
	idx := 0
	for range 2 {
		pg.peers = append(
			pg.peers,
			starvationPeer(idx, PeerSourceP2PGossip, PeerStateHot, true),
		)
		idx++
	}
	for range 3 {
		pg.peers = append(
			pg.peers,
			starvationPeer(idx, PeerSourceP2PGossip, PeerStateWarm, true),
		)
		idx++
	}
	for i := range inbound {
		state := PeerStateWarm
		if i < 6 {
			state = PeerStateHot
		}
		pg.peers = append(
			pg.peers,
			starvationPeer(idx, PeerSourceInboundConn, state, false),
		)
		idx++
	}
	removed := 0
	pg.enforcePeerLimits(&removed)
	remainingInbound := 0
	for _, p := range pg.peers {
		if p != nil && p.Source == PeerSourceInboundConn {
			remainingInbound++
		}
	}
	remainingOutbound := len(pg.peers) - remainingInbound
	pg.mu.Unlock()

	assert.Equal(t, inbound, remainingInbound,
		"inbound peers must not be removed for outbound targets")
	assert.Equal(t, 5, remainingOutbound,
		"outbound peers are at target and must stay")
	assert.Zero(t, removed)
	assert.Zero(t, testutil.ToFloat64(
		pg.metrics.inboundPrunedByReason.WithLabelValues("limit_exceeded"),
	))
}

// When outbound is over target, only outbound peers are removed.
func TestEnforcePeerLimitsRemovesOutboundOnlyWhenOverTarget(t *testing.T) {
	t.Parallel()
	pg := newStarvationGovernor(PeerGovernorConfig{
		TargetNumberOfActivePeers:      2,
		TargetNumberOfEstablishedPeers: 3,
		TargetNumberOfKnownPeers:       500,
	})
	pg.mu.Lock()
	for i := range 6 {
		pg.peers = append(
			pg.peers,
			starvationPeer(i, PeerSourceP2PGossip, PeerStateHot, true),
		)
	}
	for i := range 10 {
		pg.peers = append(
			pg.peers,
			starvationPeer(100+i, PeerSourceInboundConn, PeerStateWarm, false),
		)
	}
	removed := 0
	pg.enforcePeerLimits(&removed)
	hotOut, inbound := 0, 0
	for _, p := range pg.peers {
		switch {
		case p == nil:
		case p.Source == PeerSourceInboundConn:
			inbound++
		case p.State == PeerStateHot:
			hotOut++
		}
	}
	pg.mu.Unlock()
	require.Equal(t, 10, inbound)
	assert.Equal(t, 2, hotOut)
	assert.Equal(t, 4, removed)
}
