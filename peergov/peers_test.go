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

import "testing"

// TestChainSelectionEligible_FiltersRandomInboundSource ensures that inbound
// connections from peers we don't know about (PeerSourceInboundConn) are not
// treated as chain-selection sources even when the connection is a full-duplex
// client. This preserves the protection added in #1699 against random
// downstream peers polluting chain selection while still allowing topology
// and P2P-discovered peers who happen to dial us first to drive chain sync.
func TestChainSelectionEligible_FiltersRandomInboundSource(t *testing.T) {
	duplexClient := &PeerConnection{IsClient: true}
	responderOnly := &PeerConnection{IsClient: false}

	cases := []struct {
		name   string
		source PeerSource
		conn   *PeerConnection
		want   bool
	}{
		{
			name:   "topology local root duplex is eligible",
			source: PeerSourceTopologyLocalRoot,
			conn:   duplexClient,
			want:   true,
		},
		{
			name:   "topology public root duplex is eligible",
			source: PeerSourceTopologyPublicRoot,
			conn:   duplexClient,
			want:   true,
		},
		{
			name:   "topology bootstrap duplex is eligible",
			source: PeerSourceTopologyBootstrapPeer,
			conn:   duplexClient,
			want:   true,
		},
		{
			name:   "p2p gossip duplex is eligible",
			source: PeerSourceP2PGossip,
			conn:   duplexClient,
			want:   true,
		},
		{
			name:   "p2p ledger duplex is eligible",
			source: PeerSourceP2PLedger,
			conn:   duplexClient,
			want:   true,
		},
		{
			name:   "random inbound-only peer is not eligible",
			source: PeerSourceInboundConn,
			conn:   duplexClient,
			want:   false,
		},
		{
			name:   "responder-only connection is not eligible",
			source: PeerSourceTopologyLocalRoot,
			conn:   responderOnly,
			want:   false,
		},
		{
			name:   "nil connection is not eligible",
			source: PeerSourceTopologyLocalRoot,
			conn:   nil,
			want:   false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := chainSelectionEligible(tc.source, tc.conn)
			if got != tc.want {
				t.Fatalf(
					"chainSelectionEligible(%v, %+v) = %v, want %v",
					tc.source,
					tc.conn,
					got,
					tc.want,
				)
			}
		})
	}
}
