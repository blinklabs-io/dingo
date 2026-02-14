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
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddPeer_RejectsGossipPeersAtCap(t *testing.T) {
	// Use a small TargetNumberOfKnownPeers so the cap is
	// defaultMinPeerListCap (200).
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		TargetNumberOfKnownPeers: 5, // cap = max(2*5, 200) = 200
	})
	cap := pg.maxPeerListSize()
	require.Equal(t, defaultMinPeerListCap, cap)

	// Fill peer list to exactly the cap with gossip peers
	for i := range cap {
		addr := fmt.Sprintf("10.0.%d.%d:%d", i/256, i%256, 3000)
		err := pg.AddPeer(addr, PeerSourceP2PGossip)
		require.NoError(t, err, "peer %d should be accepted", i)
	}
	require.Len(t, pg.GetPeers(), cap)

	// The next gossip peer should be rejected
	err := pg.AddPeer("10.99.99.99:3000", PeerSourceP2PGossip)
	assert.ErrorIs(t, err, ErrPeerListFull)

	// Peer count should not have increased
	assert.Len(t, pg.GetPeers(), cap)
}

func TestAddPeer_RejectsLedgerPeersAtCap(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		TargetNumberOfKnownPeers: 5,
	})
	cap := pg.maxPeerListSize()

	// Fill peer list to the cap
	for i := range cap {
		addr := fmt.Sprintf("10.0.%d.%d:%d", i/256, i%256, 3000)
		err := pg.AddPeer(addr, PeerSourceP2PGossip)
		require.NoError(t, err)
	}

	// Ledger peers should also be rejected at cap
	err := pg.AddPeer("10.99.99.99:3000", PeerSourceP2PLedger)
	assert.ErrorIs(t, err, ErrPeerListFull)
	assert.Len(t, pg.GetPeers(), cap)
}

func TestAddPeer_AcceptsTopologyPeersAtCap(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		TargetNumberOfKnownPeers: 5,
	})
	cap := pg.maxPeerListSize()

	// Fill peer list to the cap
	for i := range cap {
		addr := fmt.Sprintf("10.0.%d.%d:%d", i/256, i%256, 3000)
		err := pg.AddPeer(addr, PeerSourceP2PGossip)
		require.NoError(t, err)
	}
	require.Len(t, pg.GetPeers(), cap)

	// Topology peers (all three types) should still be accepted
	topologySources := []PeerSource{
		PeerSourceTopologyLocalRoot,
		PeerSourceTopologyPublicRoot,
		PeerSourceTopologyBootstrapPeer,
	}
	for i, source := range topologySources {
		addr := fmt.Sprintf("172.16.0.%d:3000", i+1)
		err := pg.AddPeer(addr, source)
		assert.NoError(t, err,
			"topology peer with source %v should be accepted at cap",
			source,
		)
	}

	// Peer count should have grown beyond the cap
	assert.Len(t, pg.GetPeers(), cap+len(topologySources))
}

func TestAddPeer_NormalOperationWithinCap(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		TargetNumberOfKnownPeers: 5,
	})

	// Add a mix of peer sources, well under cap
	sources := []struct {
		addr   string
		source PeerSource
	}{
		{"10.0.0.1:3000", PeerSourceP2PGossip},
		{"10.0.0.2:3000", PeerSourceP2PLedger},
		{"10.0.0.3:3000", PeerSourceInboundConn},
		{"10.0.0.4:3000", PeerSourceTopologyLocalRoot},
		{"10.0.0.5:3000", PeerSourceTopologyPublicRoot},
	}

	for _, s := range sources {
		err := pg.AddPeer(s.addr, s.source)
		assert.NoError(t, err, "peer %s should be accepted", s.addr)
	}
	assert.Len(t, pg.GetPeers(), len(sources))
}

func TestMaxPeerListSize_UsesDoubleKnownPeersWhenLarger(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		TargetNumberOfKnownPeers: 150, // cap = max(2*150, 200) = 300
	})
	assert.Equal(t, 300, pg.maxPeerListSize())
}

func TestMaxPeerListSize_UsesMinCapWhenSmaller(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		TargetNumberOfKnownPeers: 50, // cap = max(2*50, 200) = 200
	})
	assert.Equal(t, defaultMinPeerListCap, pg.maxPeerListSize())
}

func TestAddPeer_InboundRejectedAtCap(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		TargetNumberOfKnownPeers: 5,
	})
	cap := pg.maxPeerListSize()

	// Fill peer list to the cap
	for i := range cap {
		addr := fmt.Sprintf("10.0.%d.%d:%d", i/256, i%256, 3000)
		err := pg.AddPeer(addr, PeerSourceP2PGossip)
		require.NoError(t, err)
	}

	// Inbound connection peer should also be rejected at cap
	err := pg.AddPeer("10.99.99.99:3000", PeerSourceInboundConn)
	assert.ErrorIs(t, err, ErrPeerListFull)
	assert.Len(t, pg.GetPeers(), cap)
}

func TestAddLedgerPeer_RejectedAtCap(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:                   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		TargetNumberOfKnownPeers: 5,
	})
	cap := pg.maxPeerListSize()

	// Fill peer list to the cap via AddPeer
	for i := range cap {
		addr := fmt.Sprintf("10.0.%d.%d:%d", i/256, i%256, 3000)
		err := pg.AddPeer(addr, PeerSourceP2PGossip)
		require.NoError(t, err)
	}

	// addLedgerPeer should return false when at cap
	added := pg.addLedgerPeer("10.99.99.99:3000")
	assert.False(t, added, "ledger peer should be rejected at cap")
	assert.Len(t, pg.GetPeers(), cap)
}
