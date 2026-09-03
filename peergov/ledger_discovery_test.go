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
	"context"
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/topology"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadPeerSnapshotSeedsLedgerPeers(t *testing.T) {
	snapshot := &topology.PeerSnapshotConfig{
		Point: topology.PeerSnapshotPoint{BlockPointSlot: 42},
		BigLedgerPools: []topology.PeerSnapshotLedgerPool{
			{
				Relays: []topology.TopologyConfigP2PAccessPoint{
					{Address: "44.0.0.1", Port: 3001},
					{Address: "44.0.0.2", Port: 3001},
					{Address: "44.0.0.3", Port: 3001},
				},
			},
		},
	}
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:         newMockEventBus(),
		LedgerPeerTarget: 2,
	})

	added := pg.LoadPeerSnapshot(context.Background(), snapshot)

	require.Equal(t, 2, added)
	require.Len(t, pg.peers, 2)
	for _, peer := range pg.peers {
		require.NotNil(t, peer)
		assert.Equal(t, PeerSource(PeerSourceP2PLedger), peer.Source)
		assert.Equal(t, PeerStateCold, peer.State)
		_, known := pg.ledgerKnownAddrs[peer.NormalizedAddress]
		assert.True(t, known)
	}
}

func TestLoadPeerSnapshotConvertsRelayShapes(t *testing.T) {
	snapshot := &topology.PeerSnapshotConfig{
		BigLedgerPools: []topology.PeerSnapshotLedgerPool{
			{
				Relays: []topology.TopologyConfigP2PAccessPoint{
					{Address: "relay.example.com", Port: 3002},
					{Address: "44.0.1.1", Port: 3003},
					{Address: "2001:db8::1", Port: 3004},
				},
			},
		},
	}

	relays := PoolRelaysFromPeerSnapshot(snapshot)

	require.Len(t, relays, 3)
	assert.Equal(t, "relay.example.com", relays[0].Hostname)
	assert.Equal(t, uint(3002), relays[0].Port)
	require.NotNil(t, relays[1].IPv4)
	assert.Equal(t, "44.0.1.1", relays[1].IPv4.String())
	assert.Equal(t, uint(3003), relays[1].Port)
	require.NotNil(t, relays[2].IPv6)
	assert.Equal(t, "2001:db8::1", relays[2].IPv6.String())
	assert.Equal(t, uint(3004), relays[2].Port)
}

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
		MinHotPeers:        2,
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
	pg.mu.Lock()
	for _, peer := range pg.peers {
		peer.State = PeerStateHot
		peer.Connection = &PeerConnection{IsClient: true}
	}
	pg.mu.Unlock()

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
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
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

// TestPruneLedgerKnownAddrsLocked_RemovesStaleEntries verifies that an
// address recorded in ledgerKnownAddrs is dropped once no retained peer
// carries it any longer, so a relay that disappears from ledger state (or a
// peer that leaves the peer list for any other reason) does not grow the map
// forever across discovery rounds.
func TestPruneLedgerKnownAddrsLocked_RemovesStaleEntries(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Source:            PeerSourceP2PLedger,
		Address:           "44.0.0.1:3001",
		NormalizedAddress: "44.0.0.1:3001",
	})
	// "44.0.0.2:3001" was ledger-known but its peer already left p.peers
	// (deny, capacity, or reconnect-failure eviction).
	pg.ledgerKnownAddrs["44.0.0.1:3001"] = struct{}{}
	pg.ledgerKnownAddrs["44.0.0.2:3001"] = struct{}{}

	pg.pruneLedgerKnownAddrsLocked()

	_, stillLive := pg.ledgerKnownAddrs["44.0.0.1:3001"]
	_, stale := pg.ledgerKnownAddrs["44.0.0.2:3001"]
	pg.mu.Unlock()

	assert.True(t, stillLive, "address backed by a retained peer must survive")
	assert.False(t, stale, "address with no retained peer must be pruned")
}

// TestPeerGovernor_Reconcile_PrunesStaleLedgerKnownAddr is the same
// reconciliation exercised through the public reconcile loop rather than the
// locked helper directly, and covers repeated reconcile passes settling on a
// stable, pruned map instead of erroring or re-adding the stale entry.
func TestPeerGovernor_Reconcile_PrunesStaleLedgerKnownAddr(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})

	pg.mu.Lock()
	pg.ledgerKnownAddrs["44.0.0.9:3001"] = struct{}{}
	pg.mu.Unlock()

	pg.reconcile(t.Context())
	pg.mu.Lock()
	_, known := pg.ledgerKnownAddrs["44.0.0.9:3001"]
	pg.mu.Unlock()
	assert.False(t, known)

	// Repeated reconcile passes over an already-pruned map must stay stable.
	pg.reconcile(t.Context())
	pg.mu.Lock()
	assert.Empty(t, pg.ledgerKnownAddrs)
	pg.mu.Unlock()
}

// TestDiscoverLedgerPeers_ReconcilesAgainstCurrentRelaySet verifies the
// on-chain half of ledgerKnownAddrs reconciliation: an address whose pool
// deregisters or rotates its relay must stop counting toward
// LedgerPeerTarget once it is no longer part of the ledger provider's
// current result, even though the peer that address originally matched
// (added from a non-ledger source) stays connected. This is distinct from
// pruneLedgerKnownAddrsLocked, which only reacts to the peer itself leaving
// p.peers; reconcileLedgerKnownAddrs reacts to the chain's own relay list
// changing while the peer is untouched.
func TestDiscoverLedgerPeers_ReconcilesAgainstCurrentRelaySet(t *testing.T) {
	provider := &mockLedgerPeerProvider{
		relays: []PoolRelay{{Hostname: "44.0.0.9", Port: 3001}},
	}
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerPeerProvider: provider,
		LedgerPeerTarget:   1,
		DisableOutbound:    true,
	})
	require.NoError(t, pg.AddPeer("44.0.0.9:3001", PeerSourceP2PGossip))

	pg.discoverLedgerPeers()
	pg.mu.Lock()
	_, knownBefore := pg.ledgerKnownAddrs["44.0.0.9:3001"]
	countBefore := pg.countLedgerPeersLocked()
	pg.mu.Unlock()
	require.True(t, knownBefore)
	require.Equal(
		t,
		1,
		countBefore,
		"gossip peer matching a currently listed relay counts toward the ledger target",
	)

	// The pool moves its registration to a different relay: "44.0.0.9" is no
	// longer part of the ledger's current relay set, even though the
	// gossip-sourced peer at that address stays connected.
	provider.relays = []PoolRelay{{Hostname: "44.0.0.99", Port: 3001}}
	pg.lastLedgerPeerRefresh.Store(0) // force past the refresh-interval gate
	pg.discoverLedgerPeers()

	pg.mu.Lock()
	_, stillKnown := pg.ledgerKnownAddrs["44.0.0.9:3001"]
	gossipPeerRetained := pg.peerIndexByAddress("44.0.0.9:3001") != -1
	pg.mu.Unlock()

	assert.False(t, stillKnown,
		"a delisted relay's association must be reconciled away")
	assert.True(
		t,
		gossipPeerRetained,
		"the peer itself must remain connected; only its ledger association is pruned",
	)
}

// TestDiscoverLedgerPeers_ReAssociatesRelistedRelay is the replacement
// counterpart to TestDiscoverLedgerPeers_ReconcilesAgainstCurrentRelaySet: a
// relay that drops out of the ledger's relay set and later reappears in it
// (a pool moving back, or simply being seen again on a later round) must
// have its ledgerKnownAddrs association restored, not left permanently
// stale from the round it was pruned.
func TestDiscoverLedgerPeers_ReAssociatesRelistedRelay(t *testing.T) {
	provider := &mockLedgerPeerProvider{
		relays: []PoolRelay{{Hostname: "44.0.0.9", Port: 3001}},
	}
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerPeerProvider: provider,
		LedgerPeerTarget:   1,
		DisableOutbound:    true,
	})
	require.NoError(t, pg.AddPeer("44.0.0.9:3001", PeerSourceP2PGossip))

	// Round 1: relay is listed, gossip peer counts toward the target.
	pg.discoverLedgerPeers()
	pg.mu.Lock()
	_, knownRound1 := pg.ledgerKnownAddrs["44.0.0.9:3001"]
	pg.mu.Unlock()
	require.True(t, knownRound1)

	// Round 2: the pool moves its registration elsewhere; the association
	// is pruned (covered by TestDiscoverLedgerPeers_ReconcilesAgainstCurrentRelaySet).
	provider.relays = []PoolRelay{{Hostname: "44.0.0.99", Port: 3001}}
	pg.lastLedgerPeerRefresh.Store(0)
	pg.discoverLedgerPeers()
	pg.mu.Lock()
	_, knownRound2 := pg.ledgerKnownAddrs["44.0.0.9:3001"]
	pg.mu.Unlock()
	require.False(
		t,
		knownRound2,
		"precondition: association must be pruned first",
	)

	// Round 3: the original relay reappears in the ledger's relay set
	// alongside the other one. The still-connected gossip peer at that
	// address must be re-associated, replacing the pruned entry.
	provider.relays = []PoolRelay{
		{Hostname: "44.0.0.9", Port: 3001},
		{Hostname: "44.0.0.99", Port: 3001},
	}
	pg.lastLedgerPeerRefresh.Store(0)
	pg.discoverLedgerPeers()

	pg.mu.Lock()
	_, knownRound3 := pg.ledgerKnownAddrs["44.0.0.9:3001"]
	gossipPeerRetained := pg.peerIndexByAddress("44.0.0.9:3001") != -1
	pg.mu.Unlock()
	assert.True(t, knownRound3,
		"a re-listed relay must be re-associated with its retained peer")
	assert.True(t, gossipPeerRetained,
		"the original gossip peer must still be the one holding the address")
}

// TestReconcileLedgerKnownAddrs_EmptyCandidatesIsNoop verifies that an empty
// candidate set (GetPoolRelays returning zero addresses without an error)
// leaves ledgerKnownAddrs untouched rather than wiping it: that response
// shape is not expected on a live chain, so treating it as "every relay was
// delisted" would be actively harmful for no benefit.
func TestReconcileLedgerKnownAddrs_EmptyCandidatesIsNoop(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	pg.mu.Lock()
	pg.ledgerKnownAddrs["44.0.0.9:3001"] = struct{}{}
	pg.mu.Unlock()

	pg.reconcileLedgerKnownAddrs(nil)

	pg.mu.Lock()
	_, known := pg.ledgerKnownAddrs["44.0.0.9:3001"]
	pg.mu.Unlock()
	assert.True(t, known)
}

// TestReconcileLedgerKnownAddrs_RepeatedCallsAreIdempotent covers the
// repeated-operation case directly against reconcileLedgerKnownAddrs: running
// the same candidate set through it twice must not change the outcome, and
// a subsequent call with a shrunk candidate set must prune exactly the
// addresses that dropped out.
func TestReconcileLedgerKnownAddrs_RepeatedCallsAreIdempotent(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	pg.mu.Lock()
	pg.ledgerKnownAddrs["44.0.0.1:3001"] = struct{}{}
	pg.ledgerKnownAddrs["44.0.0.2:3001"] = struct{}{}
	pg.mu.Unlock()

	candidates := []string{"44.0.0.1:3001", "44.0.0.2:3001"}
	pg.reconcileLedgerKnownAddrs(candidates)
	pg.reconcileLedgerKnownAddrs(candidates)
	pg.mu.Lock()
	assert.Len(t, pg.ledgerKnownAddrs, 2)
	pg.mu.Unlock()

	pg.reconcileLedgerKnownAddrs([]string{"44.0.0.1:3001"})
	pg.mu.Lock()
	_, keptKnown := pg.ledgerKnownAddrs["44.0.0.1:3001"]
	_, droppedKnown := pg.ledgerKnownAddrs["44.0.0.2:3001"]
	pg.mu.Unlock()
	assert.True(t, keptKnown)
	assert.False(t, droppedKnown)
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
			Address:           "44.0.0.10:3001",
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
