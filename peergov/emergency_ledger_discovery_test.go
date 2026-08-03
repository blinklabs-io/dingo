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
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// addEligibleUpstreamPeers appends n connected, chain-selection-eligible
// upstream peers (client connections from a topology source).
func addEligibleUpstreamPeers(pg *PeerGovernor, n int) {
	pg.mu.Lock()
	for i := range n {
		addr := "10.10.0." + strconv.Itoa(i+1) + ":3001"
		pg.peers = append(pg.peers, &Peer{
			Address:           addr,
			NormalizedAddress: addr,
			Source:            PeerSourceTopologyLocalRoot,
			State:             PeerStateHot,
			Connection:        &PeerConnection{IsClient: true},
		})
	}
	pg.mu.Unlock()
}

func countPeersBySource(pg *PeerGovernor, src PeerSource) int {
	pg.mu.Lock()
	defer pg.mu.Unlock()
	n := 0
	for _, peer := range pg.peers {
		if peer != nil && peer.Source == src {
			n++
		}
	}
	return n
}

func fiftyLedgerRelays() []PoolRelay {
	relays := make([]PoolRelay, 50)
	for i := range relays {
		ip := net.ParseIP("44.0." + strconv.Itoa(i/2) + "." + strconv.Itoa(i%2+1))
		relays[i] = PoolRelay{IPv4: &ip, Port: 3001}
	}
	return relays
}

// The node is "urgent" for ledger-peer replenishment while it has fewer
// connected upstreams than its hot-peer target, and never when discovery is
// disabled.
func TestLedgerPeersUrgent(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MinHotPeers:      10,
		LedgerPeerTarget: 20,
	})
	minHot := pg.config.MinHotPeers

	assert.True(t, pg.ledgerPeersUrgent(),
		"a node with no connected upstreams must be urgent")

	addEligibleUpstreamPeers(pg, minHot-1)
	assert.True(t, pg.ledgerPeersUrgent(),
		"still urgent while below the hot-peer target")

	addEligibleUpstreamPeers(pg, 1) // now at minHot
	assert.False(t, pg.ledgerPeersUrgent(),
		"not urgent once the hot-peer target is met")

	pgDisabled := NewPeerGovernor(PeerGovernorConfig{
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MinHotPeers:      10,
		LedgerPeerTarget: -1, // discovery disabled
	})
	assert.False(t, pgDisabled.ledgerPeersUrgent(),
		"ledger discovery disabled is never urgent")
}

// When the node is peer-starved, discovery must ignore the (hourly) refresh
// interval and replenish immediately, so a collapsed pool never wedges the
// node while relays are still available.
func TestDiscoverLedgerPeers_EmergencyBypassesRefreshInterval(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		MinHotPeers:        10,
		LedgerPeerTarget:   5,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays:      fiftyLedgerRelays(),
			currentSlot: 1000,
		},
	})
	// Refreshed 1 minute ago: within the hourly interval (normal gate would
	// block) but past the 30s emergency interval.
	pg.lastLedgerPeerRefresh.Store(time.Now().Add(-1 * time.Minute).UnixNano())
	// No upstreams -> urgent -> emergency cadence bypasses the hourly gate.
	pg.discoverLedgerPeers()

	assert.Equal(t, 5, countPeersBySource(pg, PeerSourceP2PLedger),
		"urgent node must replenish ledger peers despite a recent refresh")
}

// When the known ledger-peer target is already full of unusable peers, urgent
// discovery must still add fresh candidates instead of returning early.
func TestDiscoverLedgerPeers_EmergencyBypassesSatisfiedTarget(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		MinHotPeers:        10,
		LedgerPeerTarget:   2,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays:      fiftyLedgerRelays(),
			currentSlot: 1000,
		},
	})
	pg.mu.Lock()
	pg.peers = append(pg.peers,
		&Peer{
			Address:           "44.0.0.1:3001",
			NormalizedAddress: "44.0.0.1:3001",
			Source:            PeerSourceP2PLedger,
			State:             PeerStateCold,
		},
		&Peer{
			Address:           "44.0.0.2:3001",
			NormalizedAddress: "44.0.0.2:3001",
			Source:            PeerSourceP2PLedger,
			State:             PeerStateCold,
		},
	)
	pg.mu.Unlock()
	assert.Equal(t, 0, pg.ledgerPeerDeficit(),
		"known ledger-peer target should appear satisfied")
	// Refreshed 1 minute ago: within the hourly interval but past the default
	// emergency interval, so only the urgent path may proceed.
	pg.lastLedgerPeerRefresh.Store(time.Now().Add(-1 * time.Minute).UnixNano())

	pg.discoverLedgerPeers()

	assert.Greater(t, countPeersBySource(pg, PeerSourceP2PLedger), 2,
		"urgent discovery must add fresh ledger peers even when target appears satisfied")
}

func TestDiscoverLedgerPeers_EmergencyLogField(t *testing.T) {
	var logBuf bytes.Buffer
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(&logBuf, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		MinHotPeers:        10,
		LedgerPeerTarget:   1,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays:      fiftyLedgerRelays(),
			currentSlot: 1000,
		},
	})

	pg.discoverLedgerPeers()

	assert.Contains(t, logBuf.String(), `"emergency":true`,
		"emergency ledger discovery log should include emergency=true")
}

// A healthy node (at its hot-peer target) must NOT bypass the refresh
// interval: a recent refresh suppresses discovery as normal.
func TestDiscoverLedgerPeers_NoBypassWhenNotUrgent(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		MinHotPeers:        10,
		LedgerPeerTarget:   5,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays:      fiftyLedgerRelays(),
			currentSlot: 1000,
		},
	})
	addEligibleUpstreamPeers(pg, pg.config.MinHotPeers) // at hot target: not urgent
	// Refreshed 1 minute ago: inside the hourly interval, so a non-urgent node
	// must not discover (no emergency bypass applies).
	pg.lastLedgerPeerRefresh.Store(time.Now().Add(-1 * time.Minute).UnixNano())

	pg.discoverLedgerPeers()

	assert.Equal(t, 0, countPeersBySource(pg, PeerSourceP2PLedger),
		"non-urgent node must respect the refresh interval and add no ledger peers")
}
