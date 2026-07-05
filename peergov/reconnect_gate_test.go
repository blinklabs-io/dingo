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

// deadDialAddress is a loopback address with nothing listening, so an outbound
// dial to it fails fast and drives the reconnect fail path.
const deadDialAddress = "127.0.0.1:1"

// newReconnectGateTestGovernor wires a PeerGovernor with a real connection
// manager whose dials to deadDialAddress fail fast, so the outbound reconnect
// gate can be exercised end to end.
func newReconnectGateTestGovernor(t *testing.T, threshold int) *PeerGovernor {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:   logger,
		EventBus: newMockEventBus(),
		ConnManager: connmanager.NewConnectionManager(
			connmanager.ConnectionManagerConfig{Logger: logger},
		),
		MaxReconnectFailureThreshold: threshold,
		DenyDuration:                 30 * time.Minute,
	})
	pg.mu.Lock()
	pg.ctx = t.Context()
	pg.stopCh = make(chan struct{})
	pg.mu.Unlock()
	t.Cleanup(pg.Stop)
	return pg
}

// setupReconnectGateTest installs a target peer at the dead dial address and,
// when withUpstream is set, a connected chain-selection-eligible upstream so
// the node is not stranded. The gate only drops never-connected discovered or
// public-root peers while eligible upstreams remain. Returns the target peer.
func setupReconnectGateTest(
	pg *PeerGovernor,
	source PeerSource,
	everConnected bool,
	withUpstream bool,
) *Peer {
	target := &Peer{
		Address:           deadDialAddress,
		NormalizedAddress: deadDialAddress,
		Source:            source,
		State:             PeerStateCold,
		EverConnected:     everConnected,
	}
	peers := []*Peer{target}
	if withUpstream {
		peers = append(peers, &Peer{
			Address:           "203.0.113.10:3001",
			NormalizedAddress: "203.0.113.10:3001",
			Source:            PeerSourceTopologyLocalRoot,
			State:             PeerStateHot,
			Connection:        &PeerConnection{IsClient: true},
		})
	}
	pg.mu.Lock()
	pg.peers = peers
	pg.mu.Unlock()
	return target
}

func peersContainAddress(peers []*Peer, address string) bool {
	for _, peer := range peers {
		if peer != nil && peer.NormalizedAddress == address {
			return true
		}
	}
	return false
}

// A discovered (peer-share gossip) peer that has never connected must be
// dropped and denied after its first failed dial when other upstreams remain.
func TestCreateOutboundConnection_DropsNeverConnectedGossipPeer(t *testing.T) {
	pg := newReconnectGateTestGovernor(t, 0)
	target := setupReconnectGateTest(pg, PeerSourceP2PGossip, false, true)

	go pg.createOutboundConnection(target)

	require.Eventually(t, func() bool {
		pg.mu.Lock()
		defer pg.mu.Unlock()
		_, denied := pg.denyList[deadDialAddress]
		return !peersContainAddress(pg.peers, deadDialAddress) && denied
	}, 5*time.Second, 10*time.Millisecond,
		"never-connected gossip peer must be dropped and denied after a failed dial")
}

// A public-root peer that has never connected must likewise be dropped, even
// though it is a topology-sourced peer.
func TestCreateOutboundConnection_DropsNeverConnectedPublicRootPeer(t *testing.T) {
	pg := newReconnectGateTestGovernor(t, 0)
	target := setupReconnectGateTest(pg, PeerSourceTopologyPublicRoot, false, true)

	go pg.createOutboundConnection(target)

	require.Eventually(t, func() bool {
		pg.mu.Lock()
		defer pg.mu.Unlock()
		_, denied := pg.denyList[deadDialAddress]
		return !peersContainAddress(pg.peers, deadDialAddress) && denied
	}, 5*time.Second, 10*time.Millisecond,
		"never-connected public-root peer must be dropped and denied after a failed dial")
}

// A local-root peer that has never connected is trusted and must keep retrying,
// never dropped or denied, even while other upstreams exist.
func TestCreateOutboundConnection_RetainsNeverConnectedLocalRootPeer(t *testing.T) {
	pg := newReconnectGateTestGovernor(t, 0)
	target := setupReconnectGateTest(pg, PeerSourceTopologyLocalRoot, false, true)

	go pg.createOutboundConnection(target)

	require.Eventually(t, func() bool {
		pg.mu.Lock()
		defer pg.mu.Unlock()
		return peersContainAddress(pg.peers, deadDialAddress) && target.ReconnectCount > 0
	}, 5*time.Second, 10*time.Millisecond,
		"never-connected local-root peer must keep retrying, not be dropped")

	pg.mu.Lock()
	_, denied := pg.denyList[deadDialAddress]
	pg.mu.Unlock()
	assert.False(t, denied, "local-root peer must never be denied by the never-connected gate")
}

// A discovered peer that connected at least once must keep retrying on a later
// failure; a transient loss is worth recovering. A high failure threshold keeps
// the unrelated fail-fast path from interfering with the assertion.
func TestCreateOutboundConnection_RetainsEverConnectedGossipPeer(t *testing.T) {
	pg := newReconnectGateTestGovernor(t, 1000)
	target := setupReconnectGateTest(pg, PeerSourceP2PGossip, true, true)

	go pg.createOutboundConnection(target)

	require.Eventually(t, func() bool {
		pg.mu.Lock()
		defer pg.mu.Unlock()
		return peersContainAddress(pg.peers, deadDialAddress) && target.ReconnectCount > 0
	}, 5*time.Second, 10*time.Millisecond,
		"gossip peer that connected once must keep retrying on a later failure, not be dropped")

	pg.mu.Lock()
	_, denied := pg.denyList[deadDialAddress]
	pg.mu.Unlock()
	assert.False(t, denied, "ever-connected gossip peer must not be denied by the never-connected gate")
}

// When the node has no eligible upstream, a never-connected gossip peer is the
// only lead back onto the network and must be kept and retried rather than
// dropped, preserving the anti-stranding emergency redial path.
func TestCreateOutboundConnection_RetainsNeverConnectedGossipPeerWhenNoUpstream(t *testing.T) {
	pg := newReconnectGateTestGovernor(t, 1000)
	target := setupReconnectGateTest(pg, PeerSourceP2PGossip, false, false)

	go pg.createOutboundConnection(target)

	require.Eventually(t, func() bool {
		pg.mu.Lock()
		defer pg.mu.Unlock()
		return peersContainAddress(pg.peers, deadDialAddress) && target.ReconnectCount > 0
	}, 5*time.Second, 10*time.Millisecond,
		"never-connected gossip peer must be retried when the node has no upstream left")

	pg.mu.Lock()
	_, denied := pg.denyList[deadDialAddress]
	pg.mu.Unlock()
	assert.False(t, denied, "last-lead gossip peer must not be denied when no upstream remains")
}

// A peer can gain a client-capable inbound connection while an outbound dial is
// still in flight. If that outbound dial then fails, the never-connected gate
// must not remove the now-healthy upstream.
func TestNeverConnectedDropGate_RetainsCurrentClientConnection(t *testing.T) {
	pg := newReconnectGateTestGovernor(t, 0)
	target := setupReconnectGateTest(pg, PeerSourceP2PGossip, false, true)

	pg.mu.Lock()
	target.Connection = &PeerConnection{IsClient: true}
	drop := pg.shouldDropNeverConnectedPeerAfterDialFailureLocked(target)
	pg.mu.Unlock()

	assert.False(t, drop, "current client-capable connection must suppress the never-connected drop")
}
