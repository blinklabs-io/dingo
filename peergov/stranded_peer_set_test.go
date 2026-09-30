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
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/topology"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func (p *slotLedgerPeerProvider) GetPoolRelays() ([]PoolRelay, error) {
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
