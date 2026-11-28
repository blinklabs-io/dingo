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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newBlockedDialGovernor returns a started governor whose outbound dials
// block until the returned release function runs or the test ends, so a
// dial the governor starts keeps its peer's Reconnecting flag set. The flag
// is set under p.mu before the dial goroutine exists, which makes "a dial
// was started" observable right after the call that started it.
func newBlockedDialGovernor(
	t *testing.T,
	cfg PeerGovernorConfig,
) *PeerGovernor {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	releaseCh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseCh) }) }
	connMgr := connmanager.NewConnectionManager(
		connmanager.ConnectionManagerConfig{
			Logger: logger,
			OutboundDialer: func(
				ctx context.Context,
				_ string,
			) (net.Conn, error) {
				select {
				case <-releaseCh:
				case <-ctx.Done():
				}
				return nil, io.EOF
			},
		},
	)
	cfg.Logger = logger
	cfg.EventBus = newMockEventBus()
	cfg.PromRegistry = prometheus.NewRegistry()
	cfg.ConnManager = connMgr
	pg := NewPeerGovernor(cfg)
	pg.mu.Lock()
	pg.ctx = t.Context()
	pg.stopCh = make(chan struct{})
	pg.mu.Unlock()
	t.Cleanup(func() {
		_ = pg.Stop(context.Background())
		_ = connMgr.Stop(context.Background())
	})
	// Registered last, so it runs first: Stop waits for the dial workers.
	t.Cleanup(release)
	return pg
}

// reconnectingLocked reports whether the peer at address has a dial reserved.
func (p *PeerGovernor) reconnectingLocked(address string) bool {
	idx := p.peerIndexByAddress(address)
	return idx != -1 && p.peers[idx].Reconnecting
}

func requireDialStarted(
	t *testing.T,
	pg *PeerGovernor,
	address string,
	want bool,
	msg string,
) {
	t.Helper()
	pg.mu.Lock()
	defer pg.mu.Unlock()
	require.NotEqual(t, -1, pg.peerIndexByAddress(address),
		"%s: peer %s must be known", msg, address)
	require.Equal(t, want, pg.reconnectingLocked(address), msg)
}

func TestAddPeer_DialsOnlyBelowEstablishedTarget(t *testing.T) {
	t.Parallel()
	pg := newBlockedDialGovernor(t, PeerGovernorConfig{
		TargetNumberOfEstablishedPeers: 2,
		TargetNumberOfKnownPeers:       100,
	})
	pg.mu.Lock()
	pg.peers = append(pg.peers,
		starvationPeer(0, PeerSourceP2PGossip, PeerStateWarm, true),
	)
	pg.mu.Unlock()

	require.NoError(t, pg.AddPeer("44.0.0.1:3001", PeerSourceP2PGossip))
	requireDialStarted(t, pg, "44.0.0.1:3001", true,
		"one established peer is below the target of two: dial")

	// The dial in flight counts toward the target.
	require.NoError(t, pg.AddPeer("44.0.0.2:3001", PeerSourceP2PGossip))
	requireDialStarted(t, pg, "44.0.0.2:3001", false,
		"an established peer plus a dial in flight reach the target: no dial")

	// A topology peer is operator-configured and always dialled.
	require.NoError(t,
		pg.AddPeer("44.0.0.3:3001", PeerSourceTopologyPublicRoot))
	requireDialStarted(t, pg, "44.0.0.3:3001", true,
		"topology peers are dialled whatever the established count")
}

func TestAddLedgerPeer_DialsOnlyBelowEstablishedTarget(t *testing.T) {
	t.Parallel()
	pg := newBlockedDialGovernor(t, PeerGovernorConfig{
		TargetNumberOfEstablishedPeers: 1,
		TargetNumberOfKnownPeers:       100,
	})
	pg.mu.Lock()
	pg.peers = append(pg.peers,
		starvationPeer(0, PeerSourceP2PGossip, PeerStateWarm, true),
	)
	pg.mu.Unlock()

	require.True(t, pg.addLedgerPeerContext(
		t.Context(), "44.0.0.1:3001", 0, false, "",
	))
	requireDialStarted(t, pg, "44.0.0.1:3001", false,
		"established peers are at the target: a ledger peer is added cold")

	pg.mu.Lock()
	pg.config.TargetNumberOfEstablishedPeers = 2
	pg.mu.Unlock()
	require.True(t, pg.addLedgerPeerContext(
		t.Context(), "44.0.0.2:3001", 0, false, "",
	))
	requireDialStarted(t, pg, "44.0.0.2:3001", true,
		"below the target a ledger peer is dialled")
}

func TestStartOutboundConnections_DialsOnlyBelowEstablishedTarget(
	t *testing.T,
) {
	t.Parallel()
	pg := newBlockedDialGovernor(t, PeerGovernorConfig{
		TargetNumberOfEstablishedPeers: 2,
		TargetNumberOfKnownPeers:       100,
	})
	topo := &Peer{
		Address:           "44.0.0.1:3001",
		NormalizedAddress: "44.0.0.1:3001",
		Source:            PeerSourceTopologyLocalRoot,
		State:             PeerStateCold,
	}
	g1 := &Peer{
		Address:           "44.0.0.2:3001",
		NormalizedAddress: "44.0.0.2:3001",
		Source:            PeerSourceP2PGossip,
		State:             PeerStateCold,
	}
	g2 := &Peer{
		Address:           "44.0.0.3:3001",
		NormalizedAddress: "44.0.0.3:3001",
		Source:            PeerSourceP2PGossip,
		State:             PeerStateCold,
	}
	pg.mu.Lock()
	pg.peers = []*Peer{topo, g1, g2}
	pg.mu.Unlock()

	pg.startOutboundConnections()

	requireDialStarted(t, pg, topo.Address, true,
		"a topology peer is dialled at start")
	requireDialStarted(t, pg, g1.Address, true,
		"the topology dial leaves room for one more under the target")
	requireDialStarted(t, pg, g2.Address, false,
		"held peers past the established target are not dialled at start")
}

// A warm peer over the established target is demoted to cold and stays
// known; offering its address again (peer sharing and the ledger round do
// every cycle) must not dial it.
func TestEnforcePeerLimits_DemotesExcessWarmAndKeepsPeerKnown(t *testing.T) {
	t.Parallel()
	// Four established peers (two warm gossip, one hot, one warm topology)
	// over a target of three: one warm gossip peer is demoted.
	pg := newBlockedDialGovernor(t, PeerGovernorConfig{
		TargetNumberOfEstablishedPeers: 3,
		TargetNumberOfKnownPeers:       100,
	})
	keep := starvationPeer(0, PeerSourceP2PGossip, PeerStateWarm, true)
	keep.PerformanceScore = 0.9
	drop := starvationPeer(1, PeerSourceP2PGossip, PeerStateWarm, true)
	drop.PerformanceScore = 0.1
	// AddPeer below refuses private addresses.
	drop.Address = "44.0.0.9:3001"
	drop.NormalizedAddress = drop.Address
	hot := starvationPeer(2, PeerSourceP2PGossip, PeerStateHot, true)
	topo := starvationPeer(3, PeerSourceTopologyPublicRoot, PeerStateWarm, true)
	pg.mu.Lock()
	pg.peers = []*Peer{keep, drop, hot, topo}
	removed := 0
	events := pg.enforcePeerLimits(&removed, new([]deferredClose))
	pg.mu.Unlock()

	assert.Zero(t, removed, "the limit demotes, it does not remove")
	pg.mu.Lock()
	idx := pg.peerIndexByAddress(drop.NormalizedAddress)
	var dropState PeerState
	var dropConn *PeerConnection
	var dropDenied bool
	if idx != -1 {
		dropState = pg.peers[idx].State
		dropConn = pg.peers[idx].Connection
		dropDenied = pg.isPeerDeniedLocked(pg.peers[idx])
	}
	pg.mu.Unlock()
	require.NotEqual(t, -1, idx, "the demoted peer must stay known")
	assert.Equal(t, PeerStateCold, dropState)
	assert.Nil(t, dropConn)
	assert.False(t, dropDenied, "a demoted peer is not denied")
	assert.Equal(t, PeerStateWarm, keep.State, "the better peer stays warm")
	assert.Equal(t, PeerStateHot, hot.State, "hot peers are not demoted here")
	assert.Equal(t, PeerStateWarm, topo.State,
		"topology peers are never demoted for the limit")
	var demoted, removedEvents int
	for _, evt := range events {
		switch evt.eventType {
		case PeerDemotedEventType:
			demoted++
		case PeerRemovedEventType:
			removedEvents++
		}
	}
	assert.Equal(t, 1, demoted)
	assert.Zero(t, removedEvents)

	// Discovery offers the demoted peer's address again.
	require.NoError(t, pg.AddPeer(drop.Address, PeerSourceP2PGossip))
	requireDialStarted(t, pg, drop.NormalizedAddress, false,
		"a peer demoted for the limit is not redialled by the next round")
}

// The established target counts hot and warm outbound peers, the set the
// dial gate counts, so hot peers leave fewer warm slots. Only non-topology
// warm peers are demoted, lowest score first; inbound peers do not count.
func TestEnforcePeerLimits_EstablishedLimitCountsHotPeers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		target  int
		warm    int
		demoted int
	}{
		{name: "two hot and two warm over a target of three", target: 3,
			warm: 2, demoted: 1},
		{name: "two hot and one warm at a target of three", target: 3,
			warm: 1, demoted: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pg := newStarvationGovernor(PeerGovernorConfig{
				TargetNumberOfEstablishedPeers: tc.target,
				TargetNumberOfKnownPeers:       100,
			})
			peers := []*Peer{
				starvationPeer(0, PeerSourceP2PGossip, PeerStateHot, true),
				starvationPeer(1, PeerSourceP2PLedger, PeerStateHot, true),
				starvationPeer(2, PeerSourceInboundConn, PeerStateWarm, false),
			}
			warm := make([]*Peer, 0, tc.warm)
			for i := range tc.warm {
				w := starvationPeer(3+i, PeerSourceP2PGossip, PeerStateWarm,
					true)
				w.PerformanceScore = float64(i+1) / 10
				warm = append(warm, w)
			}
			peers = append(peers, warm...)
			pg.mu.Lock()
			pg.peers = peers
			removed := 0
			pg.enforcePeerLimits(&removed, new([]deferredClose))
			pg.mu.Unlock()

			assert.Zero(t, removed)
			assert.Equal(t, PeerStateHot, peers[0].State)
			assert.Equal(t, PeerStateHot, peers[1].State)
			assert.Equal(t, PeerStateWarm, peers[2].State,
				"inbound peers are outside the outbound target")
			for i, w := range warm {
				want := PeerStateWarm
				if i < tc.demoted {
					want = PeerStateCold
				}
				assert.Equal(t, want, w.State, "warm peer %d", i)
			}
		})
	}
}

// A peer held back by the gate must be dialled once established peers fall
// below the target, even while the hot set meets MinHotPeers and no
// emergency redial applies; otherwise the node stays below the target.
func TestReconcile_DialsKnownColdPeerBelowEstablishedTarget(t *testing.T) {
	t.Parallel()
	pg := newBlockedDialGovernor(t, PeerGovernorConfig{
		TargetNumberOfEstablishedPeers: 3,
		TargetNumberOfActivePeers:      1,
		TargetNumberOfKnownPeers:       100,
		MinHotPeers:                    1,
	})
	hot := starvationPeer(0, PeerSourceP2PGossip, PeerStateHot, true)
	hot.LastActivity = time.Now()
	warm1 := starvationPeer(1, PeerSourceP2PGossip, PeerStateWarm, true)
	warm2 := starvationPeer(2, PeerSourceP2PGossip, PeerStateWarm, true)
	pg.mu.Lock()
	pg.bootstrapExited = true
	pg.peers = []*Peer{hot, warm1, warm2}
	pg.mu.Unlock()

	const held = "44.0.0.1:3001"
	require.NoError(t, pg.AddPeer(held, PeerSourceP2PGossip))
	requireDialStarted(t, pg, held, false,
		"established peers are at the target: the peer is held cold")

	pg.mu.Lock()
	idx := pg.peerIndexByAddress(held)
	require.NotEqual(t, -1, idx)
	pg.peers = []*Peer{hot, pg.peers[idx]}
	pg.mu.Unlock()

	pg.reconcile(t.Context())
	requireDialStarted(t, pg, held, true,
		"one established peer of three: reconcile dials the held peer")
}

// The fill's deficit counts dials still in flight. Without that, a second
// reconcile before the first fill dial completes dials another peer past the
// target, the warm limit then demotes one, and the next fill dials it again:
// the connect/disconnect loop the target exists to prevent.
func TestReconcile_FillCountsDialsInFlight(t *testing.T) {
	t.Parallel()
	pg := newBlockedDialGovernor(t, PeerGovernorConfig{
		TargetNumberOfEstablishedPeers: 3,
		TargetNumberOfActivePeers:      1,
		TargetNumberOfKnownPeers:       100,
		MinHotPeers:                    1,
	})
	hot := starvationPeer(0, PeerSourceP2PGossip, PeerStateHot, true)
	hot.LastActivity = time.Now()
	warm1 := starvationPeer(1, PeerSourceP2PGossip, PeerStateWarm, true)
	warm2 := starvationPeer(2, PeerSourceP2PGossip, PeerStateWarm, true)
	pg.mu.Lock()
	pg.bootstrapExited = true
	pg.peers = []*Peer{hot, warm1, warm2}
	pg.mu.Unlock()

	held := []string{"44.0.0.1:3001", "44.0.0.2:3001"}
	for _, addr := range held {
		require.NoError(t, pg.AddPeer(addr, PeerSourceP2PGossip))
		requireDialStarted(t, pg, addr, false,
			"established peers are at the target: the peer is held cold")
	}

	pg.mu.Lock()
	kept := []*Peer{hot, warm1}
	for _, addr := range held {
		idx := pg.peerIndexByAddress(addr)
		require.NotEqual(t, -1, idx)
		kept = append(kept, pg.peers[idx])
	}
	pg.peers = kept
	pg.mu.Unlock()

	dialing := func() int {
		pg.mu.Lock()
		defer pg.mu.Unlock()
		n := 0
		for _, addr := range held {
			if pg.reconnectingLocked(addr) {
				n++
			}
		}
		return n
	}

	pg.reconcile(t.Context())
	require.Equal(t, 1, dialing(),
		"two established peers of three: reconcile dials one held peer")

	pg.reconcile(t.Context())
	assert.Equal(t, 1, dialing(),
		"the dial in flight fills the deficit: no second dial")
}
