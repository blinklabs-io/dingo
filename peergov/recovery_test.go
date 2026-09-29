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
	"bytes"
	"context"
	"io"
	"log/slog"
	"strings"
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

// When the node is reduced to a single eligible upstream, gossip churn
// skips demoting it on every interval. The skip itself is correct, but the
// condition persists, so the INFO line must be emitted only on entry into
// that state, not on every churn cycle, otherwise it spams the log
// indefinitely (observed once every GossipChurnInterval).
func TestPeerGovernor_GossipChurn_LastEligibleUpstreamSkipLogThrottled(
	t *testing.T,
) {
	var logBuf bytes.Buffer
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(&logBuf, nil)),
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

	const skipMsg = "skipping demotion of last eligible upstream peer"
	for range 5 {
		pg.gossipChurn()
	}

	if got := strings.Count(logBuf.String(), skipMsg); got != 1 {
		t.Fatalf(
			"skip log emitted %d times across 5 churn cycles, want 1 (on entry only)",
			got,
		)
	}
	// Behavior must be unchanged: the last upstream is still protected.
	assert.Equal(t, PeerStateHot, pg.peers[0].State)
	assert.NotNil(t, pg.peers[0].Connection)
}

func TestPeerGovernor_GossipChurn_KeepsOneUpstreamWhenChurningAll(
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
		// A warm replacement so the demotion below is not also blocked by
		// the "no promotable replacement" guard (dingo#4783): this test
		// is specifically about the last-eligible-upstream protection.
		{
			Address:          "ledger1:3001",
			Source:           PeerSourceP2PLedger,
			State:            PeerStateWarm,
			PerformanceScore: 0.5,
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
		// A warm replacement so the demotion below is not also blocked by
		// the "no promotable replacement" guard (dingo#4783): this test
		// is specifically about churn still operating with a topology
		// upstream present.
		{
			Address:          "gossip2:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateWarm,
			PerformanceScore: 0.5,
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

	// With a healthy upstream present but the hot set still far below
	// MinHotPeers and no warm candidates to close that gap, cold
	// gossip/ledger peers must still become redial candidates
	// (dingo#4783): a single eligible upstream is not "healthy" when the
	// configured hot-peer target is 10 and nothing else is in flight to
	// reach it.
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
	addrs = make([]string, 0, len(candidates))
	for _, peer := range candidates {
		addrs = append(addrs, peer.Address)
	}
	assert.ElementsMatch(
		t,
		[]string{"gossip1:3001", "ledger1:3001"},
		addrs,
		"gossip/ledger peers must still be redialed when the hot set sits below MinHotPeers with no warm replacements available",
	)

	// Once the hot set reaches MinHotPeers, the deficit-driven trigger no
	// longer applies and gossip/ledger peers stay cold as before.
	pg.mu.Lock()
	pg.config.MinHotPeers = 1
	candidates = pg.redialCandidatesLocked()
	pg.mu.Unlock()
	assert.Empty(
		t,
		candidates,
		"gossip/ledger peers must not be redialed once an eligible upstream satisfies MinHotPeers",
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
	t.Cleanup(func() { _ = pg.Stop(context.Background()) })

	pg.reconcile(t.Context())

	require.Eventually(
		t,
		func() bool {
			pg.mu.Lock()
			defer pg.mu.Unlock()
			return pg.peers[0].Reconnecting || pg.peers[0].ReconnectCount > 0
		},
		5*time.Second,
		10*time.Millisecond,
		"reconcile must spawn an outbound connection attempt for a disconnected topology peer",
	)
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
	t.Cleanup(func() { _ = pg.Stop(context.Background()) })

	pg.reconcile(t.Context())

	require.Eventually(
		t,
		func() bool {
			pg.mu.Lock()
			defer pg.mu.Unlock()
			return pg.peers[0].Reconnecting || pg.peers[0].ReconnectCount > 0
		},
		5*time.Second,
		10*time.Millisecond,
		"reconcile must redial a known gossip peer when the node has no upstream left",
	)
}

// TestPeerGovernor_Reconcile_RedialsColdGossipPeerBelowMinHotPeers is the
// dingo#4783 regression for the redial side: before this fix, gossip/ledger
// peers were only ever redialed in the zero-eligible-upstream emergency.
// A node with one eligible upstream but a hot set well below MinHotPeers,
// and no warm candidates to close that gap, never dialed any of its
// remaining cold known peers at all -- it just sat there until the single
// upstream also dropped. Reconcile must now dial cold known peers whenever
// the hot set is short of MinHotPeers and the warm pool cannot cover the
// deficit, not only when the node has zero upstreams left.
func TestPeerGovernor_Reconcile_RedialsColdGossipPeerBelowMinHotPeers(
	t *testing.T,
) {
	t.Parallel()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	connManager := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{Logger: logger},
	)
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:      logger,
		EventBus:    newMockEventBus(),
		ConnManager: connManager,
		MinHotPeers: 3,
	})
	pg.mu.Lock()
	pg.ctx = t.Context()
	pg.stopCh = make(chan struct{})
	pg.peers = []*Peer{
		// One eligible upstream: the zero-upstream emergency trigger does
		// not apply here.
		{
			Address:           "root1:3001",
			NormalizedAddress: "root1:3001",
			Source:            PeerSourceTopologyLocalRoot,
			State:             PeerStateHot,
			Connection:        &PeerConnection{IsClient: true},
		},
		// A cold known gossip peer with nothing warm available to
		// promote instead.
		{
			// Nothing listens here; the dial fails fast and the
			// reconnect loop records the attempt.
			Address:           "127.0.0.1:1",
			NormalizedAddress: "127.0.0.1:1",
			Source:            PeerSourceP2PGossip,
			State:             PeerStateCold,
		},
	}
	pg.mu.Unlock()
	t.Cleanup(func() { _ = pg.Stop(context.Background()) })

	pg.reconcile(t.Context())

	require.Eventually(
		t,
		func() bool {
			pg.mu.Lock()
			defer pg.mu.Unlock()
			return pg.peers[1].Reconnecting || pg.peers[1].ReconnectCount > 0
		},
		5*time.Second,
		10*time.Millisecond,
		"reconcile must redial a cold known gossip peer when the hot set is below MinHotPeers, even with an eligible upstream present",
	)

	pg.mu.Lock()
	rootStillHot := pg.peers[0].State == PeerStateHot
	rootStillConnected := pg.peers[0].Connection != nil
	pg.mu.Unlock()
	assert.True(
		t,
		rootStillHot,
		"the existing eligible upstream must be undisturbed",
	)
	assert.True(
		t,
		rootStillConnected,
		"the existing eligible upstream must keep its connection",
	)
}

// A warm gossip peer holding only a responder-side inbound connection
// cannot be promoted by reconcile's refill (it requires a client
// connection) and would be demoted by reconcile's inactivity check if
// promoted. It must therefore not count as covering a hot-set deficit.
func TestPeerGovernor_RedialCandidates_ResponderOnlyWarmPeerDoesNotCoverDeficit(
	t *testing.T,
) {
	t.Parallel()
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:    newMockEventBus(),
		MinHotPeers: 3,
	})
	pg.peers = []*Peer{
		{
			Address:           "root1:3001",
			NormalizedAddress: "root1:3001",
			Source:            PeerSourceTopologyLocalRoot,
			State:             PeerStateHot,
			Connection:        &PeerConnection{IsClient: true},
		},
		{
			Address:           "inbound1:3001",
			NormalizedAddress: "inbound1:3001",
			Source:            PeerSourceP2PGossip,
			State:             PeerStateWarm,
			PerformanceScore:  0.9,
			Connection:        &PeerConnection{IsClient: false},
		},
		{
			Address:           "inbound2:3001",
			NormalizedAddress: "inbound2:3001",
			Source:            PeerSourceP2PGossip,
			State:             PeerStateWarm,
			PerformanceScore:  0.9,
			Connection:        &PeerConnection{IsClient: false},
		},
		{
			Address:           "gossip1:3001",
			NormalizedAddress: "gossip1:3001",
			Source:            PeerSourceP2PGossip,
			State:             PeerStateCold,
		},
	}

	pg.mu.Lock()
	candidates := pg.redialCandidatesLocked()
	pg.mu.Unlock()
	addrs := make([]string, 0, len(candidates))
	for _, peer := range candidates {
		addrs = append(addrs, peer.Address)
	}
	assert.Equal(
		t,
		[]string{"gossip1:3001"},
		addrs,
		"responder-only warm peers must not suppress the hot_deficit redial",
	)
}

// Churn must not demote a hot client upstream when the only warm
// "replacement" holds a responder-side connection that cannot serve as
// an upstream once promoted.
func TestPeerGovernor_GossipChurn_ResponderOnlyWarmPeerIsNotAReplacement(
	t *testing.T,
) {
	t.Parallel()
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		GossipChurnPercent: 1.0,
		MinScoreThreshold:  0.3,
	})
	pg.peers = []*Peer{
		{
			Address:    "root1:3001",
			Source:     PeerSourceTopologyLocalRoot,
			State:      PeerStateHot,
			Connection: &PeerConnection{IsClient: true},
		},
		{
			Address:          "gossip1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateHot,
			PerformanceScore: 0.8,
			Connection:       &PeerConnection{IsClient: true},
		},
		{
			Address:          "inbound1:3001",
			Source:           PeerSourceP2PGossip,
			State:            PeerStateWarm,
			PerformanceScore: 0.9,
			Connection:       &PeerConnection{IsClient: false},
		},
	}

	pg.gossipChurn()

	states := make(map[string]PeerState, len(pg.peers))
	for _, peer := range pg.peers {
		states[peer.Address] = peer.State
	}
	assert.Equal(
		t,
		PeerStateHot,
		states["gossip1:3001"],
		"the hot client upstream must not be churned out for a responder-only peer",
	)
	assert.Equal(
		t,
		PeerStateWarm,
		states["inbound1:3001"],
		"a responder-only warm peer must not be promoted to hot",
	)
}

// Peers churn just dropped for scoring below MinScoreThreshold must not
// take the hot_deficit redial budget ahead of better cold peers, or a
// failing peer cycles cold, warm, hot on every churn interval.
func TestPeerGovernor_RedialCandidates_HotDeficitSkipsChurnedLowScorers(
	t *testing.T,
) {
	t.Parallel()
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		GossipChurnPercent: 1.0,
		MinScoreThreshold:  0.3,
		MinHotPeers:        3,
	})
	pg.peers = []*Peer{
		{
			Address:           "root1:3001",
			NormalizedAddress: "root1:3001",
			Source:            PeerSourceTopologyLocalRoot,
			State:             PeerStateHot,
			Connection:        &PeerConnection{IsClient: true},
		},
		observedScorePeer(
			"bad1:3001",
			PeerSourceP2PGossip,
			PeerStateHot,
			false,
		),
		observedScorePeer(
			"bad2:3001",
			PeerSourceP2PGossip,
			PeerStateHot,
			false,
		),
		{
			Address:           "fresh1:3001",
			NormalizedAddress: "fresh1:3001",
			Source:            PeerSourceP2PGossip,
			State:             PeerStateCold,
		},
		observedScorePeer(
			"good1:3001",
			PeerSourceP2PLedger,
			PeerStateCold,
			true,
		),
	}

	pg.gossipChurn()
	pg.mu.Lock()
	require.Less(t, pg.peers[1].PerformanceScore, pg.config.MinScoreThreshold)
	require.Equal(t, PeerStateCold, pg.peers[1].State)
	require.Equal(t, PeerStateCold, pg.peers[2].State)
	candidates := pg.redialCandidatesLocked()
	pg.mu.Unlock()

	addrs := make([]string, 0, len(candidates))
	for _, peer := range candidates {
		addrs = append(addrs, peer.Address)
	}
	assert.Equal(
		t,
		[]string{"good1:3001", "fresh1:3001"},
		addrs,
		"hot_deficit redial must skip observed below-threshold peers and prefer higher scores",
	)
}

// observedScorePeer returns a peer whose score comes from real
// observations, so agePeerScoresLocked recomputes rather than resets it.
func observedScorePeer(
	addr string,
	source PeerSource,
	state PeerState,
	good bool,
) *Peer {
	peer := &Peer{
		Address:                 addr,
		NormalizedAddress:       addr,
		Source:                  source,
		State:                   state,
		ScoreLastUpdate:         time.Now(),
		BlockFetchLatencyInit:   true,
		BlockFetchSuccessInit:   true,
		ConnectionStabilityInit: true,
		HeaderArrivalRateInit:   true,
		TipSlotDeltaInit:        true,
		BlockFetchLatencyMs:     10_000,
		TipSlotDelta:            1_000_000,
	}
	if good {
		peer.BlockFetchLatencyMs = 10
		peer.BlockFetchSuccessRate = 1
		peer.ConnectionStability = 1
		peer.HeaderArrivalRate = 100
		peer.TipSlotDelta = 0
	}
	if state != PeerStateCold {
		peer.Connection = &PeerConnection{IsClient: true}
	}
	peer.UpdatePeerScore()
	return peer
}

// When no eligible upstream remains any peer is better than none, so
// below-threshold peers stay redial candidates, ranked after the rest.
func TestPeerGovernor_RedialCandidates_ZeroUpstreamRanksLowScorersLast(
	t *testing.T,
) {
	t.Parallel()
	observed := time.Now()
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:          newMockEventBus(),
		MinScoreThreshold: 0.3,
	})
	for _, peer := range []*Peer{
		{Address: "bad1:3001", PerformanceScore: 0.1, ScoreLastUpdate: observed},
		{Address: "bad2:3001", PerformanceScore: 0.2, ScoreLastUpdate: observed},
		{Address: "fresh1:3001"},
		{Address: "good1:3001", PerformanceScore: 0.9, ScoreLastUpdate: observed},
	} {
		peer.NormalizedAddress = peer.Address
		peer.Source = PeerSourceP2PGossip
		peer.State = PeerStateCold
		pg.peers = append(pg.peers, peer)
	}

	pg.mu.Lock()
	candidates := pg.redialCandidatesLocked()
	pg.mu.Unlock()

	addrs := make([]string, 0, len(candidates))
	for _, peer := range candidates {
		addrs = append(addrs, peer.Address)
	}
	assert.Equal(
		t,
		[]string{"good1:3001", "fresh1:3001", "bad2:3001"},
		addrs,
		"zero_upstream redial must still use below-threshold peers, after the rest",
	)
}
