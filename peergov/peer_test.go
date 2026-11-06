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
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaxPeerListSize_UsesDoubleKnownPeersWhenLarger(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		TargetNumberOfKnownPeers: 150, // cap = max(2*150, 200) = 300
	})
	assert.Equal(t, 300, pg.maxPeerListSize())
}

func TestMaxPeerListSize_UsesMinCapWhenSmaller(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		TargetNumberOfKnownPeers: 50, // cap = max(2*50, 200) = 200
	})
	assert.Equal(t, defaultMinPeerListCap, pg.maxPeerListSize())
}

// TestAddLedgerPeer_ExistingPeerCountsTowardLedgerTarget verifies that a
// ledger relay already retained from another source satisfies the target
// without adding a duplicate peer.
func TestAddLedgerPeer_ExistingPeerCountsTowardLedgerTarget(t *testing.T) {
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		LedgerPeerTarget: 1,
	})
	const address = "44.99.99.99:3000"
	require.NoError(t, pg.AddPeer(address, PeerSourceP2PGossip))

	added := pg.addLedgerPeer(address)
	assert.False(t, added, "duplicate ledger peer should be rejected")
	_, retained := pg.ledgerKnownAddrs[address]
	assert.True(t, retained, "retained peer should be marked ledger-known")
	assert.Zero(t, pg.ledgerPeerDeficit(),
		"retained ledger relay should satisfy the ledger target")
}

func TestAddPeer_PeerListCap(t *testing.T) {
	t.Parallel()
	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(
			slog.NewJSONHandler(io.Discard, nil),
		),
		TargetNumberOfKnownPeers: 5, // cap = max(2*5, 200) = 200
	})
	cap := pg.maxPeerListSize()
	require.Equal(t, defaultMinPeerListCap, cap)
	for i := range cap {
		addr := fmt.Sprintf("44.0.%d.%d:%d", i/256, i%256, 3000)
		err := pg.AddPeer(addr, PeerSourceP2PGossip)
		require.NoError(t, err, "peer %d should be accepted", i)
	}
	require.Len(t, pg.GetPeers(), cap)

	for _, source := range []PeerSource{
		PeerSourceP2PGossip,
		PeerSourceP2PLedger,
		PeerSourceInboundConn,
	} {
		err := pg.AddPeer("44.99.99.99:3000", source)
		assert.ErrorIs(t, err, ErrPeerListFull, "source %v", source)
		assert.Len(t, pg.GetPeers(), cap)
	}

	added := pg.addLedgerPeer("44.99.99.99:3000")
	assert.False(t, added, "ledger peer should be rejected at cap")
	assert.Len(t, pg.GetPeers(), cap)
	_, retained := pg.ledgerKnownAddrs["44.99.99.99:3000"]
	assert.False(t, retained, "rejected ledger peer must not be retained")

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
	assert.Len(t, pg.GetPeers(), cap+len(topologySources))
}
