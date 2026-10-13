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
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/topology"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingResolver installs a lookupIPAddr stub that counts calls and returns
// a fixed result, restoring the previous resolver on cleanup.
func countingResolver(
	t *testing.T,
	ips []net.IP,
	err error,
) *atomic.Int64 {
	t.Helper()
	calls := new(atomic.Int64)
	old := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		calls.Add(1)
		return ips, err
	}
	t.Cleanup(func() { lookupIPAddr = old })
	return calls
}

func discardGovernor() *PeerGovernor {
	return NewPeerGovernor(PeerGovernorConfig{
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus: newMockEventBus(),
	})
}

func nxdomain(host string) error {
	return errors.New("lookup " + host + ": no such host")
}

// A relay address that already belongs to a known peer must not be resolved
// again. Ledger discovery re-offers the full relay set every round, so
// resolving before the exists check re-resolves every healthy connected peer
// and every dead hostname on every round.
func TestAddLedgerPeer_KnownPeerSkipsDNSResolution(t *testing.T) {
	calls := countingResolver(t, nil, nxdomain("relay.example.com"))
	pg := discardGovernor()
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address:           "relay.example.com:3001",
		NormalizedAddress: "44.0.0.7:3001",
		Source:            PeerSourceTopologyLocalRoot,
		State:             PeerStateCold,
	})
	pg.mu.Unlock()

	require.False(t, pg.addLedgerPeer("relay.example.com:3001"),
		"an already-known relay must not be added twice")
	assert.Equal(t, int64(0), calls.Load(),
		"an already-known ledger relay must not be re-resolved")

	pg.mu.Lock()
	// ledgerKnownAddrs is keyed on normalizeAddress(peer.Address), not the
	// peer's resolved NormalizedAddress; see
	// addLedgerPeerContext/countLedgerPeersLocked. Here the peer's Address
	// is exactly the candidate string, so the key happens to read the same
	// either way.
	_, known := pg.ledgerKnownAddrs["relay.example.com:3001"]
	count := pg.countLedgerPeersLocked()
	pg.mu.Unlock()
	assert.True(t, known)
	assert.Equal(t, 1, count,
		"the retained peer must count toward the ledger target")
}

// Peer.Address is stored verbatim, so a topology or gossip peer can hold the
// same relay hostname under different casing. Matching it case-sensitively
// adds a second peer for one relay, which AddPeer already avoids by
// normalizing both sides.
func TestAddLedgerPeer_KnownPeerMatchedCaseInsensitively(t *testing.T) {
	// A different IP than the known peer's, so a missed match here is not
	// rescued by the post-resolution check either.
	calls := countingResolver(t, []net.IP{net.ParseIP("44.0.0.8")}, nil)
	pg := discardGovernor()
	pg.mu.Lock()
	pg.peers = append(pg.peers, &Peer{
		Address:           "Relay.Example.com:3001",
		NormalizedAddress: "44.0.0.7:3001",
		Source:            PeerSourceTopologyLocalRoot,
		State:             PeerStateCold,
	})
	pg.mu.Unlock()

	require.False(t, pg.addLedgerPeer("relay.example.com:3001"),
		"the same relay under different casing must not be added twice")
	assert.Equal(t, int64(0), calls.Load(),
		"a case-insensitive match must be decided without DNS")

	pg.mu.Lock()
	peerCount := len(pg.peers)
	// ledgerKnownAddrs is keyed on normalizeAddress(peer.Address), not the
	// peer's resolved NormalizedAddress; see
	// addLedgerPeerContext/countLedgerPeersLocked. normalizeAddress only
	// lowercases a hostname, so "Relay.Example.com:3001" and the candidate's
	// "relay.example.com:3001" produce the same key here.
	_, known := pg.ledgerKnownAddrs["relay.example.com:3001"]
	count := pg.countLedgerPeersLocked()
	pg.mu.Unlock()
	assert.Equal(t, 1, peerCount, "no duplicate peer for the same relay")
	assert.True(t, known)
	assert.Equal(t, 1, count,
		"the retained peer must count toward the ledger target")
}

// A relay hostname already on the deny list must not be resolved. Deny
// entries for unresolvable hostnames are keyed on the lowercased hostname,
// which is exactly what the pre-resolution check can compare against.
func TestAddLedgerPeer_DeniedHostnameSkipsDNSResolution(t *testing.T) {
	calls := countingResolver(t, nil, nxdomain("dead.example.com"))
	pg := discardGovernor()
	pg.mu.Lock()
	pg.denyList["dead.example.com:3001"] = time.Now().
		Add(defaultDenyDuration)
	pg.mu.Unlock()

	require.False(t, pg.addLedgerPeer("DEAD.example.com:3001"),
		"a denied relay must not be added")
	assert.Equal(t, int64(0), calls.Load(),
		"a denied ledger relay must not be resolved")
}

// The negative case for the two above: an unknown, undenied relay must still
// be resolved and added, so the reordering cannot silently stop discovery.
func TestAddLedgerPeer_UnknownRelayStillResolves(t *testing.T) {
	calls := countingResolver(t, []net.IP{net.ParseIP("44.0.0.9")}, nil)
	pg := discardGovernor()

	require.True(t, pg.addLedgerPeer("fresh.example.com:3001"))
	assert.Equal(t, int64(1), calls.Load(),
		"a fresh relay hostname must still be resolved once")

	pg.mu.Lock()
	defer pg.mu.Unlock()
	require.Len(t, pg.peers, 1)
	assert.Equal(t, "44.0.0.9:3001", pg.peers[0].NormalizedAddress,
		"the peer must be keyed on the resolved address")
}

// A hostname that failed to resolve is cached as a negative result, so the
// next discovery round skips the lookup entirely rather than repeating it.
func TestResolveLedgerDiscoveryAddress_NegativeCacheSuppressesLookups(
	t *testing.T,
) {
	calls := countingResolver(t, nil, nxdomain("dead.example.com"))
	pg := discardGovernor()
	ctx := context.Background()

	first := pg.resolveLedgerDiscoveryAddress(ctx, "dead.example.com:3001")
	second := pg.resolveLedgerDiscoveryAddress(ctx, "dead.example.com:3001")

	assert.Equal(t, "dead.example.com:3001", first,
		"a failed resolution still falls back to the bare hostname")
	assert.Equal(t, first, second,
		"a cached failure must return the same fallback address")
	assert.Equal(t, int64(1), calls.Load(),
		"a cached resolution failure must not be looked up again")
}

// The negative cache is bounded in time: once an entry expires the hostname
// is resolved for real again, so a relay that starts resolving recovers
// instead of staying pinned as dead.
func TestResolveLedgerDiscoveryAddress_NegativeCacheExpiresAndRecovers(
	t *testing.T,
) {
	failing := new(atomic.Bool)
	failing.Store(true)
	calls := new(atomic.Int64)
	old := lookupIPAddr
	lookupIPAddr = func(_ context.Context, _ string) ([]net.IP, error) {
		calls.Add(1)
		if failing.Load() {
			return nil, nxdomain("flaky.example.com")
		}
		return []net.IP{net.ParseIP("44.0.0.4")}, nil
	}
	t.Cleanup(func() { lookupIPAddr = old })

	pg := discardGovernor()
	ctx := context.Background()
	require.Equal(t, "flaky.example.com:3001",
		pg.resolveLedgerDiscoveryAddress(ctx, "flaky.example.com:3001"))
	require.Equal(t, int64(1), calls.Load())

	pg.negativeDNSMu.Lock()
	pg.negativeDNS["flaky.example.com"] = time.Now().Add(-time.Second)
	pg.negativeDNSMu.Unlock()
	failing.Store(false)

	got := pg.resolveLedgerDiscoveryAddress(ctx, "flaky.example.com:3001")
	assert.Equal(t, int64(2), calls.Load(),
		"an expired negative-cache entry must be re-resolved")
	assert.Equal(t, "44.0.0.4:3001", got,
		"a recovered hostname must resolve to its real address")

	pg.negativeDNSMu.Lock()
	_, cached := pg.negativeDNS["flaky.example.com"]
	pg.negativeDNSMu.Unlock()
	assert.False(t, cached,
		"a successful resolution must leave no cached failure behind")
}

// The cache is bounded in size: pool-published relay hostnames are untrusted
// input, so an unbounded map would be a memory sink.
func TestNegativeDNSCacheIsBounded(t *testing.T) {
	countingResolver(t, nil, errors.New("no such host"))
	pg := discardGovernor()
	ctx := context.Background()

	for i := range negativeDNSCacheMaxEntries + 64 {
		host := "dead" + strconv.Itoa(i) + ".example.com"
		pg.resolveLedgerDiscoveryAddress(ctx, host+":3001")
	}

	pg.negativeDNSMu.Lock()
	size := len(pg.negativeDNS)
	pg.negativeDNSMu.Unlock()
	assert.LessOrEqual(t, size, negativeDNSCacheMaxEntries,
		"the negative DNS cache must stay bounded")
}

// A pool publishing a dead relay hostname is a fact about the chain, not an
// operator-actionable fault, so it must not be logged at WARN.
func TestResolveLedgerDiscoveryAddress_FailureNotLoggedAtWarn(t *testing.T) {
	countingResolver(t, nil, nxdomain("dead.example.com"))

	var infoBuf bytes.Buffer
	pgInfo := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(&infoBuf, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		})),
	})
	pgInfo.resolveLedgerDiscoveryAddress(
		context.Background(),
		"dead.example.com:3001",
	)
	assert.NotContains(t, infoBuf.String(),
		"failed to resolve ledger relay hostname",
		"a dead pool-published relay must not warn the operator")

	var debugBuf bytes.Buffer
	pgDebug := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(&debugBuf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
	})
	pgDebug.resolveLedgerDiscoveryAddress(
		context.Background(),
		"other-dead.example.com:3001",
	)
	assert.Contains(t, debugBuf.String(),
		"failed to resolve ledger relay hostname",
		"the failure must stay observable at debug level")
}

// TestAddLedgerPeer_HostnameCandidateCountsPeerAddedByIP covers a peer added
// under its resolved IP (as topology config commonly does) later matched by
// a ledger candidate published as a hostname that resolves to that same IP.
// ledgerKnownAddrs must key on the retained peer's own address, not the
// candidate's hostname form, or the peer silently stops counting toward
// LedgerPeerTarget and reconcileLedgerKnownAddrs cannot find it either
// (both derive their lookup key from peer.Address, which here is the IP,
// never the hostname).
func TestAddLedgerPeer_HostnameCandidateCountsPeerAddedByIP(t *testing.T) {
	countingResolver(t, []net.IP{net.ParseIP("44.0.0.50")}, nil)
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerPeerTarget: 1,
	})
	require.NoError(t, pg.AddPeer("44.0.0.50:3001", PeerSourceP2PGossip))

	added := pg.addLedgerPeer("relay.example.com:3001")
	require.False(t, added, "existing peer must not be duplicated")

	pg.mu.Lock()
	count := pg.countLedgerPeersLocked()
	peerKey := pg.normalizeAddress("44.0.0.50:3001")
	pg.mu.Unlock()
	assert.Equal(
		t,
		1,
		count,
		"a peer added under its IP must count when a hostname candidate resolves to it",
	)

	// The relay is still listed under its hostname form: reconciliation
	// must not prune the association.
	pg.reconcileLedgerKnownAddrs([]string{"relay.example.com:3001"})
	pg.mu.Lock()
	_, stillKnown := pg.ledgerKnownAddrs[peerKey]
	pg.mu.Unlock()
	assert.True(t, stillKnown,
		"the association must survive reconciliation while still listed")

	// The pool moves away from that hostname entirely.
	pg.reconcileLedgerKnownAddrs([]string{"other.example.com:3001"})
	pg.mu.Lock()
	_, known := pg.ledgerKnownAddrs[peerKey]
	pg.mu.Unlock()
	assert.False(t, known,
		"the association must be pruned once the hostname is delisted")
}

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
	provider := &countingLedgerPeerProvider{}
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EventBus:           newMockEventBus(),
		UseLedgerAfterSlot: 0,
		LedgerPeerTarget:   -1, // Explicitly disabled
		LedgerPeerProvider: provider,
	})

	pg.discoverLedgerPeers()

	// With a negative target, deficit is 0, so no peers should be added.
	assert.Len(t, pg.peers, 0)
	// A negative target disables ledger discovery outright: it must return
	// before ever calling the provider, not merely add zero peers after
	// fetching and reconciling on every refresh interval for no benefit.
	assert.Equal(t, int32(0), provider.calls.Load(),
		"a disabled node must never call the ledger peer provider")
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
	pg.ledgerKnownAddrs["44.0.0.3:3001"] = "44.0.0.3:3001"
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
	pg.ledgerKnownAddrs["44.0.0.1:3001"] = "44.0.0.1:3001"
	pg.ledgerKnownAddrs["44.0.0.2:3001"] = "44.0.0.2:3001"

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
	pg.ledgerKnownAddrs["44.0.0.9:3001"] = "44.0.0.9:3001"
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

// TestDiscoverLedgerPeers_ReconcilesEvenWhenTargetSatisfiedAndNotUrgent
// verifies reconciliation runs on the discovery cadence even in the common,
// healthy steady state: LedgerPeerTarget already satisfied and the node not
// short of upstreams (ledgerPeersUrgent false). discoverLedgerPeersContext
// used to return before ever fetching relays or reconciling in exactly this
// case, so a delisted relay's stale association could never be reconciled
// away as long as the target stayed satisfied — the association itself was
// what kept it looking satisfied, permanently masking the real deficit left
// behind. TestDiscoverLedgerPeers_ReconcilesAgainstCurrentRelaySet does not
// catch this: its default MinHotPeers makes ledgerPeersUrgent true, which
// bypassed the old early return for an unrelated reason.
func TestDiscoverLedgerPeers_ReconcilesEvenWhenTargetSatisfiedAndNotUrgent(
	t *testing.T,
) {
	provider := &mockLedgerPeerProvider{
		relays: []PoolRelay{{Hostname: "44.0.0.9", Port: 3001}},
	}
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerPeerProvider: provider,
		LedgerPeerTarget:   1,
		MinHotPeers:        1,
		DisableOutbound:    true,
	})
	require.NoError(t, pg.AddPeer("44.0.0.9:3001", PeerSourceP2PGossip))
	// Give the gossip peer a client connection so it counts as an eligible
	// upstream, making ledgerPeersUrgent false without a real dial
	// (DisableOutbound prevents one anyway).
	pg.mu.Lock()
	pg.peers[0].Connection = &PeerConnection{IsClient: true}
	pg.mu.Unlock()

	pg.discoverLedgerPeers()

	require.False(t, pg.ledgerPeersUrgent(), "precondition: must not be urgent")
	require.LessOrEqual(
		t,
		pg.ledgerPeerDeficit(),
		0,
		"precondition: target must already be satisfied",
	)
	pg.mu.Lock()
	_, knownBefore := pg.ledgerKnownAddrs["44.0.0.9:3001"]
	pg.mu.Unlock()
	require.True(t, knownBefore)

	// The pool moves its registration elsewhere. The target-satisfied,
	// not-urgent state from above is exactly the condition that used to
	// skip reconciliation entirely.
	provider.relays = []PoolRelay{{Hostname: "44.0.0.99", Port: 3001}}
	pg.lastLedgerPeerRefresh.Store(0) // force past the refresh-interval gate
	pg.discoverLedgerPeers()

	pg.mu.Lock()
	_, stillKnown := pg.ledgerKnownAddrs["44.0.0.9:3001"]
	pg.mu.Unlock()
	assert.False(t, stillKnown,
		"a delisted relay's association must be reconciled away even when "+
			"the target is already satisfied and the node is not urgent")
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
	pg.ledgerKnownAddrs["44.0.0.9:3001"] = "44.0.0.9:3001"
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
	pg.ledgerKnownAddrs["44.0.0.1:3001"] = "44.0.0.1:3001"
	pg.ledgerKnownAddrs["44.0.0.2:3001"] = "44.0.0.2:3001"
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
	pg.ledgerKnownAddrs["44.0.0.10:3001"] = "44.0.0.10:3001"
	count := pg.countLedgerPeersLocked()
	pg.mu.Unlock()
	assert.Equal(t, 4, count)
}

func newStrandedSetGovernor(cfg PeerGovernorConfig) *PeerGovernor {
	cfg.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	cfg.EventBus = newMockEventBus()
	return NewPeerGovernor(cfg)
}

func addFailedColdPeer(pg *PeerGovernor, addr string, src PeerSource) {
	pg.mu.Lock()
	defer pg.mu.Unlock()
	pg.peers = append(pg.peers, &Peer{
		Address:           addr,
		NormalizedAddress: addr,
		Source:            src,
		State:             PeerStateCold,
		Reconnecting:      true,
		ReconnectCount:    pg.config.MaxReconnectFailureThreshold + 1,
	})
}

// reconcile must not delete the last known peers of a node that has no
// eligible upstream: a removed peer is not restored when its deny entry
// expires, so the known set would stay empty.
func TestReconcile_KeepsFailedNonTopologyPeerWhenNoUpstream(t *testing.T) {
	t.Parallel()
	pg := newStrandedSetGovernor(PeerGovernorConfig{
		MaxReconnectFailureThreshold: 1,
	})
	addFailedColdPeer(pg, "44.0.0.1:3001", PeerSourceP2PLedger)

	pg.reconcile(t.Context())

	pg.mu.Lock()
	defer pg.mu.Unlock()
	assert.True(t, peersContainAddress(pg.peers, "44.0.0.1:3001"),
		"last-lead peer must survive reconcile with no upstream")
	assert.NotContains(t, pg.denyList, "44.0.0.1:3001",
		"last-lead peer must not be denied with no upstream")
}

// With an eligible upstream the cold-peer removal still applies.
func TestReconcile_RemovesFailedNonTopologyPeerWhenUpstreamRemains(
	t *testing.T,
) {
	t.Parallel()
	pg := newStrandedSetGovernor(PeerGovernorConfig{
		MaxReconnectFailureThreshold: 1,
	})
	addEligibleUpstreamPeers(pg, 1)
	addFailedColdPeer(pg, "44.0.0.1:3001", PeerSourceP2PLedger)

	pg.reconcile(t.Context())

	pg.mu.Lock()
	defer pg.mu.Unlock()
	assert.False(t, peersContainAddress(pg.peers, "44.0.0.1:3001"))
	assert.Contains(t, pg.denyList, "44.0.0.1:3001")
}

func testSnapshot() *topology.PeerSnapshotConfig {
	return &topology.PeerSnapshotConfig{
		BigLedgerPools: []topology.PeerSnapshotLedgerPool{{
			Relays: []topology.TopologyConfigP2PAccessPoint{
				{Address: "44.0.0.1", Port: 3001},
				{Address: "44.0.0.2", Port: 3001},
				{Address: "44.0.0.3", Port: 3001},
				{Address: "44.0.0.4", Port: 3001},
				{Address: "44.0.0.5", Port: 3001},
			},
		}},
	}
}

// collapseLedgerPeers simulates a correlated failure that removed every
// snapshot-derived peer and denied its address.
func collapseLedgerPeers(pg *PeerGovernor) map[string]bool {
	pg.mu.Lock()
	defer pg.mu.Unlock()
	denied := make(map[string]bool)
	for _, peer := range pg.peers {
		denied[peer.NormalizedAddress] = true
		pg.denyList[peer.NormalizedAddress] = time.Now().Add(time.Hour)
	}
	pg.peers = nil
	pg.ledgerKnownAddrs = make(map[string]string)
	return denied
}

// Below UseLedgerAfterSlot the ledger provider cannot answer, but an urgent
// node must still refill from the peer snapshot's unused candidates.
func TestDiscoverLedgerPeers_UrgentBelowSlotRefillsFromSnapshot(t *testing.T) {
	t.Parallel()
	provider := &countingLedgerPeerProvider{}
	pg := newStrandedSetGovernor(PeerGovernorConfig{
		UseLedgerAfterSlot: 1000,
		LedgerPeerTarget:   2,
		LedgerPeerProvider: provider,
	})
	require.Equal(
		t,
		2,
		pg.LoadPeerSnapshot(context.Background(), testSnapshot()),
	)
	denied := collapseLedgerPeers(pg)

	pg.discoverLedgerPeersContext(t.Context())

	pg.mu.Lock()
	defer pg.mu.Unlock()
	require.Len(t, pg.peers, 2,
		"urgent discovery must refill from unused snapshot candidates")
	for _, peer := range pg.peers {
		assert.Equal(t, PeerSource(PeerSourceP2PLedger), peer.Source)
		assert.False(t, denied[peer.NormalizedAddress],
			"refill must not re-add a denied address")
	}
	assert.Zero(t, provider.calls.Load(),
		"the ledger provider must not be consulted below UseLedgerAfterSlot")
}

// A node with enough upstreams is not urgent, so the gate still holds.
func TestDiscoverLedgerPeers_NotUrgentBelowSlotDoesNotRefill(t *testing.T) {
	t.Parallel()
	pg := newStrandedSetGovernor(PeerGovernorConfig{
		UseLedgerAfterSlot: 1000,
		LedgerPeerTarget:   2,
		LedgerPeerProvider: &countingLedgerPeerProvider{},
	})
	require.Equal(
		t,
		2,
		pg.LoadPeerSnapshot(context.Background(), testSnapshot()),
	)
	collapseLedgerPeers(pg)
	addEligibleUpstreamPeers(pg, pg.config.MinHotPeers)

	pg.discoverLedgerPeersContext(t.Context())

	assert.Equal(t, 0, countPeersBySource(pg, PeerSourceP2PLedger))
}

type slotLedgerPeerProvider struct {
	slot  atomic.Uint64
	calls atomic.Int32
}

func (p *slotLedgerPeerProvider) GetPoolRelays(ctx context.Context) ([]PoolRelay, error) {
	p.calls.Add(1)
	return nil, nil
}

func (p *slotLedgerPeerProvider) CurrentSlot() uint64 {
	return p.slot.Load()
}

// A snapshot refill below UseLedgerAfterSlot must not delay the first ledger
// query once the threshold is reached: that query is the node's switch from
// the static snapshot to live relay registrations.
func TestDiscoverLedgerPeers_SnapshotRefillDoesNotDelayFirstLedgerQuery(
	t *testing.T,
) {
	t.Parallel()
	provider := &slotLedgerPeerProvider{}
	provider.slot.Store(1)
	pg := newStrandedSetGovernor(PeerGovernorConfig{
		UseLedgerAfterSlot:                 1000,
		LedgerPeerTarget:                   2,
		LedgerPeerProvider:                 provider,
		EmergencyLedgerPeerRefreshInterval: time.Hour,
		LedgerPeerRefreshInterval:          2 * time.Hour,
	})
	require.Equal(
		t,
		2,
		pg.LoadPeerSnapshot(context.Background(), testSnapshot()),
	)
	collapseLedgerPeers(pg)

	pg.discoverLedgerPeersContext(t.Context())
	require.Zero(t, provider.calls.Load())
	require.Equal(t, 2, countPeersBySource(pg, PeerSourceP2PLedger),
		"urgent discovery must refill from the snapshot below the slot")

	provider.slot.Store(1000)
	pg.discoverLedgerPeersContext(t.Context())
	assert.Equal(t, int32(1), provider.calls.Load(),
		"first ledger query above the slot must not wait out a snapshot round")
	assert.Equal(t, int64(1), int64(pg.emergencyRefreshRounds.Load()),
		"snapshot rounds must not escalate the ledger emergency backoff")
}

// Recovering below UseLedgerAfterSlot must reset the emergency backoff, as it
// does above the slot, so a later collapse starts at the base cadence.
func TestDiscoverLedgerPeers_RecoveryBelowSlotResetsEmergencyBackoff(
	t *testing.T,
) {
	t.Parallel()
	pg := newStrandedSetGovernor(PeerGovernorConfig{
		UseLedgerAfterSlot: 1000,
		LedgerPeerTarget:   2,
		LedgerPeerProvider: &countingLedgerPeerProvider{},
	})
	require.Equal(
		t,
		2,
		pg.LoadPeerSnapshot(context.Background(), testSnapshot()),
	)
	collapseLedgerPeers(pg)
	pg.discoverLedgerPeersContext(t.Context())
	require.Equal(t, uint32(1), pg.emergencyRefreshRounds.Load())

	addEligibleUpstreamPeers(pg, pg.config.MinHotPeers)
	pg.discoverLedgerPeersContext(t.Context())

	assert.Zero(t, pg.emergencyRefreshRounds.Load(),
		"recovery below the slot must reset the emergency backoff")
}
