// Copyright 2025 Blink Labs Software
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
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiscoverLedgerPeers_BoundedByTarget(t *testing.T) {
	// Provide 50 relays but set target to 5
	relays := make([]PoolRelay, 50)
	for i := range relays {
		ip := net.ParseIP("44.0.0." + strconv.Itoa(i+1))
		relays[i] = PoolRelay{IPv4: &ip, Port: 3001}
	}

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		LedgerPeerTarget:   5,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays:      relays,
			currentSlot: 1000,
		},
	})

	pg.discoverLedgerPeers()

	// Should add exactly 5 peers, not all 50
	assert.Len(t, pg.peers, 5)
	for _, peer := range pg.peers {
		assert.Equal(t, PeerSource(PeerSourceP2PLedger), peer.Source)
	}
}

func TestDiscoverLedgerPeers_TargetAlreadySatisfied(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		LedgerPeerTarget:   2,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays: func() []PoolRelay {
				r := make([]PoolRelay, 3)
				for i := range r {
					ip := net.ParseIP("44.0.1." + strconv.Itoa(i+1))
					r[i] = PoolRelay{IPv4: &ip, Port: 3001}
				}
				return r
			}(),
			currentSlot: 1000,
		},
	})

	// First discovery: adds 2 to reach target
	pg.discoverLedgerPeers()
	assert.Len(t, pg.peers, 2)

	// Reset refresh timestamp
	pg.lastLedgerPeerRefresh.Store(
		time.Now().Add(-2 * time.Hour).UnixNano(),
	)

	// Second discovery: target already satisfied, should not add more
	pg.discoverLedgerPeers()
	assert.Len(t, pg.peers, 2)
}

func TestDiscoverLedgerPeers_PartialRefill(t *testing.T) {
	ip1 := net.ParseIP("44.0.0.1")
	ip2 := net.ParseIP("44.0.0.2")
	ip3 := net.ParseIP("44.0.0.3")

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		LedgerPeerTarget:   3,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays: []PoolRelay{
				{IPv4: &ip1, Port: 3001},
				{IPv4: &ip2, Port: 3001},
				{IPv4: &ip3, Port: 3001},
			},
			currentSlot: 1000,
		},
	})

	// Fill to target
	pg.discoverLedgerPeers()
	require.Len(t, pg.peers, 3)

	// Simulate peer removal (disconnect/churn)
	pg.mu.Lock()
	pg.peers = pg.peers[:1] // Keep only 1 peer
	pg.mu.Unlock()

	// Reset refresh timestamp
	pg.lastLedgerPeerRefresh.Store(
		time.Now().Add(-2 * time.Hour).UnixNano(),
	)

	// Discovery should refill: deficit is 3 - 1 = 2.
	// The kept peer matches exactly one candidate (dedup), and the
	// other two distinct candidates are added, bringing total to 3.
	pg.discoverLedgerPeers()

	ledgerCount := 0
	for _, peer := range pg.peers {
		if peer != nil && peer.Source == PeerSourceP2PLedger {
			ledgerCount++
		}
	}
	assert.Equal(t, 3, ledgerCount)
}

func TestDiscoverLedgerPeers_NegativeTargetDisables(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		LedgerPeerTarget:   -1, // Explicitly disabled
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays: []PoolRelay{
				{Hostname: "relay.example.com", Port: 3001},
			},
			currentSlot: 1000,
		},
	})

	pg.discoverLedgerPeers()

	// With a negative target, deficit is 0, so no peers should be added
	assert.Len(t, pg.peers, 0)
}

func TestDiscoverLedgerPeers_DefaultTarget(t *testing.T) {
	// Verify default target is applied
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		UseLedgerAfterSlot: 0,
		LedgerPeerProvider: &mockLedgerPeerProvider{
			currentSlot: 1000,
		},
	})

	assert.Equal(t, defaultLedgerPeerTarget, pg.config.LedgerPeerTarget)
}

func TestDiscoverLedgerPeers_PeerCapInteraction(t *testing.T) {
	// Set a very low peer cap and a higher ledger target
	relays := make([]PoolRelay, 20)
	for i := range relays {
		ip := net.ParseIP("44.0.0." + strconv.Itoa(i+1))
		relays[i] = PoolRelay{IPv4: &ip, Port: 3001}
	}

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:                 newMockEventBus(),
		UseLedgerAfterSlot:       0,
		LedgerPeerTarget:         15,
		TargetNumberOfKnownPeers: 5, // Peer cap = max(2*5, 200) = 200
		LedgerPeerProvider: &mockLedgerPeerProvider{
			relays:      relays,
			currentSlot: 1000,
		},
	})

	pg.discoverLedgerPeers()

	// Should respect ledger target (15), not the peer cap (200)
	assert.Len(t, pg.peers, 15)
}

func TestLedgerPeerDeficit(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerPeerTarget: 5,
	})

	// No peers yet, full deficit
	assert.Equal(t, 5, pg.ledgerPeerDeficit())

	// Add some ledger peers
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Source:            PeerSourceP2PLedger,
		Address:           "44.0.0.1:3001",
		NormalizedAddress: "44.0.0.1:3001",
	})
	pg.peers = append(pg.peers, &Peer{
		Source:            PeerSourceP2PLedger,
		Address:           "44.0.0.2:3001",
		NormalizedAddress: "44.0.0.2:3001",
	})
	// Add a gossip peer at a ledger-known address, so it still counts
	// toward the ledger target via ledgerKnownAddrs.
	pg.peers = append(pg.peers, &Peer{
		Source:            PeerSourceP2PGossip,
		Address:           "44.0.0.3:3001",
		NormalizedAddress: "44.0.0.3:3001",
	})
	pg.ledgerKnownAddrs["44.0.0.3:3001"] = struct{}{}
	pg.mu.Unlock()

	assert.Equal(t, 2, pg.ledgerPeerDeficit())
}

func TestLedgerPeerDeficit_Satisfied(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerPeerTarget: 2,
	})

	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Source:            PeerSourceP2PLedger,
		Address:           "44.0.0.1:3001",
		NormalizedAddress: "44.0.0.1:3001",
	})
	pg.peers = append(pg.peers, &Peer{
		Source:            PeerSourceP2PLedger,
		Address:           "44.0.0.2:3001",
		NormalizedAddress: "44.0.0.2:3001",
	})
	pg.peers = append(pg.peers, &Peer{
		Source:            PeerSourceP2PLedger,
		Address:           "44.0.0.3:3001",
		NormalizedAddress: "44.0.0.3:3001",
	})
	pg.mu.Unlock()

	// Already exceeds target, deficit should be 0
	assert.Equal(t, 0, pg.ledgerPeerDeficit())
}

func TestFlattenRelayCandidates(t *testing.T) {
	ip4 := net.ParseIP("44.0.0.1")
	ip6 := net.ParseIP("2001:db8::1")

	relays := []PoolRelay{
		{Hostname: "relay.example.com", Port: 3001},
		{IPv4: &ip4, Port: 3002},
		{IPv6: &ip6, Port: 3003},
		{IPv4: &ip4, IPv6: &ip6, Port: 3004}, // Multiple addresses
	}

	candidates := flattenRelayCandidates(relays)

	// relay.example.com:3001, 44.0.0.1:3002, [2001:db8::1]:3003,
	// 44.0.0.1:3004, [2001:db8::1]:3004
	assert.Len(t, candidates, 5)
}

func TestFlattenRelayCandidates_Empty(t *testing.T) {
	candidates := flattenRelayCandidates(nil)
	assert.Empty(t, candidates)

	candidates = flattenRelayCandidates([]PoolRelay{})
	assert.Empty(t, candidates)
}

func TestDedupeRelayCandidates(t *testing.T) {
	candidates := dedupeRelayCandidates([]string{
		"relay.example.com:3001",
		"44.0.0.1:3001",
		"relay.example.com:3001",
		"[2001:db8::1]:3001",
		"44.0.0.1:3001",
	})

	assert.Equal(t, []string{
		"relay.example.com:3001",
		"44.0.0.1:3001",
		"[2001:db8::1]:3001",
	}, candidates)
}

func TestCountLedgerPeersLocked(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	pg.mu.Lock()
	pg.peers = []*Peer{
		{Source: PeerSourceP2PLedger},
		{
			Source:            PeerSourceP2PGossip,
			NormalizedAddress: "44.0.0.10:3001",
		},
		{Source: PeerSourceP2PLedger},
		nil, // nil entries should be skipped
		{Source: PeerSourceTopologyLocalRoot},
		{Source: PeerSourceP2PLedger},
	}
	pg.ledgerKnownAddrs["44.0.0.10:3001"] = struct{}{}
	count := pg.countLedgerPeersLocked()
	pg.mu.Unlock()
	assert.Equal(t, 4, count)
}
