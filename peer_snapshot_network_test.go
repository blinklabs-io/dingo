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

package dingo

import (
	"testing"

	"github.com/blinklabs-io/dingo/topology"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// peerSnapshotTopology builds a topology carrying a peer snapshot for the
// given network magic, plus a configured bootstrap peer. The bootstrap peer
// matters because a node that accepts the snapshot drops it.
func peerSnapshotTopology(snapshotMagic uint32) *topology.TopologyConfig {
	return &topology.TopologyConfig{
		BootstrapPeers: []topology.TopologyConfigP2PBootstrapPeer{
			{Address: "backup.example", Port: 3001},
		},
		PeerSnapshot: &topology.PeerSnapshotConfig{
			NetworkMagic: snapshotMagic,
			LedgerPools: []topology.PeerSnapshotLedgerPool{
				{
					Relays: []topology.TopologyConfigP2PAccessPoint{
						{Address: "relay.example", Port: 3001},
					},
				},
			},
		},
	}
}

// TestPeerSnapshotFromAnotherNetworkRejected proves a peer snapshot naming a
// different network cannot start the node.
//
// The snapshot's relays replace the configured bootstrap peers during Genesis
// selection, so accepting a foreign snapshot points the node at another
// network's relays and discards the only addresses that could have worked.
// Each of those relays is then denied at the handshake on a network-magic
// mismatch, which leaves the node with no peers at all and no way back to the
// bootstrap list — a failure that looks like a network outage rather than the
// misconfiguration it is.
func TestPeerSnapshotFromAnotherNetworkRejected(t *testing.T) {
	tests := []struct {
		name          string
		network       string
		snapshotMagic uint32
		wantErr       string
	}{
		{
			name:          "preview node given a mainnet snapshot",
			network:       "preview",
			snapshotMagic: 764824073,
			wantErr:       "peer snapshot network mismatch",
		},
		{
			name:          "mainnet node given a preprod snapshot",
			network:       "mainnet",
			snapshotMagic: 1,
			wantErr:       "peer snapshot network mismatch",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(NewConfig(
				WithPrometheusRegistry(prometheus.NewRegistry()),
				WithListeners(ListenerConfig{
					ListenNetwork: "tcp",
					ListenAddress: "127.0.0.1:0",
				}),
				WithNetwork(tt.network),
				WithTopologyConfig(peerSnapshotTopology(tt.snapshotMagic)),
			))
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestPeerSnapshotMatchingNetworkAccepted is the negative case: a snapshot for
// the node's own network must still start, or the check would break every
// Genesis bootstrap it is meant to protect.
func TestPeerSnapshotMatchingNetworkAccepted(t *testing.T) {
	n, err := New(NewConfig(
		WithPrometheusRegistry(prometheus.NewRegistry()),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
		WithNetwork("preview"),
		WithTopologyConfig(peerSnapshotTopology(2)),
	))
	require.NoError(t, err)
	require.NotNil(t, n)
}

// TestPeerSnapshotWithoutNetworkMagicAccepted covers a snapshot that omits the
// field. Zero is "unspecified" rather than a network, and no real network uses
// it, so a hand-written or older snapshot must not be rejected on the strength
// of an absent field.
func TestPeerSnapshotWithoutNetworkMagicAccepted(t *testing.T) {
	n, err := New(NewConfig(
		WithPrometheusRegistry(prometheus.NewRegistry()),
		WithListeners(ListenerConfig{
			ListenNetwork: "tcp",
			ListenAddress: "127.0.0.1:0",
		}),
		WithNetwork("preview"),
		WithTopologyConfig(peerSnapshotTopology(0)),
	))
	require.NoError(t, err)
	require.NotNil(t, n)
}
