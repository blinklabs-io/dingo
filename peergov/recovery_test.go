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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression tests for the silent chain-stall wedge: connection recovery
// was edge-triggered only (one-shot reconnect on the close event), so a
// node that lost its last upstream connection could never get it back.

func TestPeerGovernor_GossipChurn_SkipsLastEligibleUpstream(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		GossipChurnPercent: 1.0,
		MinScoreThreshold:  0.3,
	})
	pg.peers = []*Peer{
		{
			Address:          "gossip1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.5,
			Connection:       &PeerConnection{IsClient: true},
		},
	}

	pg.gossipChurn()

	assert.Equal(
		t,
		PeerStateHot,
		pg.peers[0].State,
		"last eligible upstream must not be churned",
	)
	assert.NotNil(
		t,
		pg.peers[0].Connection,
		"last eligible upstream connection must stay open",
	)
}

func TestPeerGovernor_GossipChurn_KeepsOneUpstreamWhenChurningAll(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		GossipChurnPercent: 1.0,
		MinScoreThreshold:  0.3,
	})
	pg.peers = []*Peer{
		{
			Address:          "gossip1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.2,
			Connection:       &PeerConnection{IsClient: true},
		},
		{
			Address:          "gossip2:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.9,
			Connection:       &PeerConnection{IsClient: true},
		},
	}

	pg.gossipChurn()

	// Lowest-scoring peer churns first; the survivor must keep its
	// connection even though the churn percentage requested both.
	assert.Equal(t, PeerStateCold, pg.peers[0].State)
	assert.Nil(t, pg.peers[0].Connection)
	assert.Equal(t, PeerStateHot, pg.peers[1].State)
	assert.NotNil(
		t,
		pg.peers[1].Connection,
		"churn must never close the last remaining upstream connection",
	)
}

func TestPeerGovernor_GossipChurn_ChurnsGossipWhenTopologyUpstreamExists(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		GossipChurnPercent: 1.0,
		MinScoreThreshold:  0.3,
	})
	pg.peers = []*Peer{
		{
			Address:          "gossip1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.5,
			Connection:       &PeerConnection{IsClient: true},
		},
		{
			Address:          "root1:3001",
			Source:           PeerSourceTopologyLocalRoot,
			State:            PeerStateHot,
			PerformanceScore: 0.9,
			Connection:       &PeerConnection{IsClient: true},
		},
	}

	pg.gossipChurn()

	assert.Equal(
		t,
		PeerStateCold,
		pg.peers[0].State,
		"gossip peer should still churn while another upstream exists",
	)
	assert.Equal(t, PeerStateHot, pg.peers[1].State)
	assert.NotNil(t, pg.peers[1].Connection)
}

func TestPeerGovernor_RedialCandidates_TopologyPeersWithoutConnection(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: newMockEventBus(),
	})
	pg.peers = []*Peer{
		{
			Address:           "root1:3001",
			NormalizedAddress: "root1:3001",
			Source:            PeerSourceTopologyLocalRoot,
			State:             PeerStateCold,
		},
		{
			Address:           "public1:3001",
			NormalizedAddress: "public1:3001",
			Source:            PeerSourceTopologyPublicRoot,
			State:             PeerStateCold,
		},
		{
			Address:           "root2:3001",
			NormalizedAddress: "root2:3001",
			Source:            PeerSourceTopologyLocalRoot,
			State:             PeerStateHot,
			Connection:        &PeerConnection{IsClient: true},
		},
	}

	pg.mu.Lock()
	candidates := pg.redialCandidatesLocked()
	pg.mu.Unlock()

	addrs := make([]string, 0, len(candidates))
	for _, peer := range candidates {
		addrs = append(addrs, peer.Address)
	}
	assert.ElementsMatch(
		t,
		[]string{"root1:3001", "public1:3001"},
		addrs,
		"disconnected topology peers must be redial candidates; connected ones must not",
	)
}

func TestPeerGovernor_RedialCandidates_SkipsDeniedAndReconnecting(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: newMockEventBus(),
	})
	pg.peers = []*Peer{
		{
			Address:           "denied:3001",
			NormalizedAddress: "denied:3001",
			Source:            PeerSourceTopologyLocalRoot,
			State:             PeerStateCold,
		},
		{
			Address:           "reconnecting:3001",
			NormalizedAddress: "reconnecting:3001",
			Source:            PeerSourceTopologyLocalRoot,
			State:             PeerStateCold,
			Reconnecting:      true,
		},
	}
	pg.denyList["denied:3001"] = time.Now().Add(time.Hour)

	pg.mu.Lock()
	candidates := pg.redialCandidatesLocked()
	pg.mu.Unlock()

	assert.Empty(
		t,
		candidates,
		"denied peers and peers with an active reconnect goroutine must not be redialed",
	)
}

func TestPeerGovernor_RedialCandidates_BootstrapGatedOnBootstrapExit(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: newMockEventBus(),
	})
	pg.peers = []*Peer{
		{
			Address:           "bootstrap1:3001",
			NormalizedAddress: "bootstrap1:3001",
			Source:            PeerSourceTopologyBootstrapPeer,
			State:             PeerStateCold,
		},
	}

	pg.mu.Lock()
	candidates := pg.redialCandidatesLocked()
	pg.mu.Unlock()
	require.Len(
		t,
		candidates,
		1,
		"bootstrap peers should be redialed while bootstrap is active",
	)

	pg.mu.Lock()
	pg.bootstrapExited = true
	candidates = pg.redialCandidatesLocked()
	pg.mu.Unlock()
	assert.Empty(
		t,
		candidates,
		"bootstrap peers must not be redialed after bootstrap exit",
	)
}

func TestPeerGovernor_RedialCandidates_EmergencyOnlyWhenNoEligibleUpstream(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: newMockEventBus(),
	})
	pg.peers = []*Peer{
		{
			Address:           "gossip1:3001",
			NormalizedAddress: "gossip1:3001",
			Source:            PeerSourceP2PGossip,
			State:             PeerStateCold,
		},
		{
			Address:           "ledger1:3001",
			NormalizedAddress: "ledger1:3001",
			Source:            PeerSourceP2PLedger,
			State:             PeerStateCold,
		},
	}

	// No eligible upstream at all: gossip/ledger peers become
	// emergency redial candidates.
	pg.mu.Lock()
	candidates := pg.redialCandidatesLocked()
	pg.mu.Unlock()
	addrs := make([]string, 0, len(candidates))
	for _, peer := range candidates {
		addrs = append(addrs, peer.Address)
	}
	assert.ElementsMatch(
		t,
		[]string{"gossip1:3001", "ledger1:3001"},
		addrs,
		"gossip/ledger peers must be redialed when the node has no upstream left",
	)

	// With a healthy upstream present, churned gossip/ledger peers
	// must stay cold so churn keeps working as designed.
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address:           "root1:3001",
		NormalizedAddress: "root1:3001",
		Source:            PeerSourceTopologyLocalRoot,
		State:             PeerStateHot,
		Connection:        &PeerConnection{IsClient: true},
	})
	candidates = pg.redialCandidatesLocked()
	pg.mu.Unlock()
	assert.Empty(
		t,
		candidates,
		"gossip/ledger peers must not be redialed while an eligible upstream exists",
	)
}

func TestPeerGovernor_RedialCandidates_EmergencyCapped(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: newMockEventBus(),
	})
	for _, addr := range []string{
		"gossip1:3001",
		"gossip2:3001",
		"gossip3:3001",
		"gossip4:3001",
		"gossip5:3001",
	} {
		pg.peers = append(pg.peers, &Peer{
			Address:           addr,
			NormalizedAddress: addr,
			Source:            PeerSourceP2PGossip,
			State:             PeerStateCold,
		})
	}

	pg.mu.Lock()
	candidates := pg.redialCandidatesLocked()
	pg.mu.Unlock()

	assert.Len(
		t,
		candidates,
		maxEmergencyRedialsPerReconcile,
		"emergency redials must be capped per reconcile cycle",
	)
}

func TestPeerGovernor_RedialCandidates_SkipsValencySatisfiedTopologyPeer(
	t *testing.T,
) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: newMockEventBus(),
	})
	pg.peers = []*Peer{
		{
			Address:           "root1:3001",
			NormalizedAddress: "root1:3001",
			Source:            PeerSourceTopologyLocalRoot,
			State:             PeerStateCold,
			GroupID:           "g1",
			Valency:           1,
		},
		{
			// Same topology group already satisfied by a reusable
			// inbound duplex connection.
			Address:           "root1-inbound:3001",
			NormalizedAddress: "root1-inbound:3001",
			Source:            PeerSourceTopologyLocalRoot,
			State:             PeerStateHot,
			GroupID:           "g1",
			Valency:           1,
			Connection:        &PeerConnection{IsClient: true},
			InboundDuplex:     true,
		},
	}

	pg.mu.Lock()
	candidates := pg.redialCandidatesLocked()
	pg.mu.Unlock()

	assert.Empty(
		t,
		candidates,
		"topology peers whose group valency is satisfied by inbound duplex must not be redialed",
	)
}

func TestPeerGovernor_Reconcile_RedialsDisconnectedTopologyPeer(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{Logger: logger},
	)
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:      logger,
		EventBus:    newMockEventBus(),
		ConnManager: connManager,
	})
	pg.mu.Lock()
	pg.ctx = t.Context()
	pg.stopCh = make(chan struct{})
	pg.peers = []*Peer{
		{
			// Nothing listens here; the dial fails fast and the
			// reconnect loop records the attempt.
			Address:           "127.0.0.1:1",
			NormalizedAddress: "127.0.0.1:1",
			Source:            PeerSourceTopologyLocalRoot,
			State:             PeerStateCold,
		},
	}
	pg.mu.Unlock()
	t.Cleanup(pg.Stop)

	pg.reconcile(t.Context())

	require.Eventually(t, func() bool {
		pg.mu.Lock()
		defer pg.mu.Unlock()
		return pg.peers[0].Reconnecting || pg.peers[0].ReconnectCount > 0
	}, 5*time.Second, 10*time.Millisecond,
		"reconcile must spawn an outbound connection attempt for a disconnected topology peer")
}

func TestPeerGovernor_Reconcile_RedialsGossipPeerWhenNoUpstream(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{Logger: logger},
	)
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:      logger,
		EventBus:    newMockEventBus(),
		ConnManager: connManager,
	})
	pg.mu.Lock()
	pg.ctx = t.Context()
	pg.stopCh = make(chan struct{})
	pg.peers = []*Peer{
		{
			Address:           "127.0.0.1:1",
			NormalizedAddress: "127.0.0.1:1",
			Source:            PeerSourceP2PGossip,
			State:             PeerStateCold,
		},
	}
	pg.mu.Unlock()
	t.Cleanup(pg.Stop)

	pg.reconcile(t.Context())

	require.Eventually(t, func() bool {
		pg.mu.Lock()
		defer pg.mu.Unlock()
		return pg.peers[0].Reconnecting || pg.peers[0].ReconnectCount > 0
	}, 5*time.Second, 10*time.Millisecond,
		"reconcile must redial a known gossip peer when the node has no upstream left")
}
