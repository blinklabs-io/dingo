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

package ouroboros

import (
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"

	"github.com/blinklabs-io/dingo/peergov"
	opeersharing "github.com/blinklabs-io/gouroboros/protocol/peersharing"
	"github.com/stretchr/testify/require"
)

func TestPeerSharingConfigSetsLocalDisabledFromNodeConfig(t *testing.T) {
	tests := []struct {
		name              string
		peerSharing       bool
		wantLocalDisabled bool
	}{
		{
			name:              "disabled",
			peerSharing:       false,
			wantLocalDisabled: true,
		},
		{
			name:              "enabled",
			peerSharing:       true,
			wantLocalDisabled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o := newOuroboros(OuroborosConfig{
				PeerSharing: tt.peerSharing,
			})

			cfg := o.peerSharingConfig()

			require.Equal(t, tt.wantLocalDisabled, cfg.LocalDisabled)
			require.False(t, cfg.RemoteDisabled)
			require.NotNil(t, cfg.ShareRequestFunc)
		})
	}
}

// TestPeerSharingShareRequestBoundsValidPeers verifies that malformed peers
// are skipped without consuming the requested reply count and that every
// returned peer has a valid IP address and port.
func TestPeerSharingShareRequestBoundsValidPeers(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	peerGov := peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
		Logger:          logger,
		DisableOutbound: true,
	})
	// Unsharable peers must not appear in the response.
	require.NoError(
		t,
		peerGov.AddPeer("10.0.0.1:3001", peergov.PeerSourceTopologyLocalRoot),
	)
	// Exercise every validation failure in the sharing adapter: an address
	// without host:port structure, a split address whose host is not an IP,
	// and a port outside the uint16 wire range.
	require.NoError(t, peerGov.AddPeer("malformed", peergov.PeerSourceP2PGossip))
	require.NoError(
		t,
		peerGov.AddPeer("[44.0.0.9%invalid]:3001", peergov.PeerSourceP2PGossip),
	)
	require.NoError(
		t,
		peerGov.AddPeer("44.0.0.9:70000", peergov.PeerSourceP2PGossip),
	)
	require.NoError(
		t,
		peerGov.AddPeer("44.0.0.1:3001", peergov.PeerSourceP2PGossip),
	)
	require.NoError(
		t,
		peerGov.AddPeer(
			"[2001:4860:4860::8888]:3002",
			peergov.PeerSourceP2PGossip,
		),
	)
	require.NoError(
		t,
		peerGov.AddPeer("44.0.0.2:3003", peergov.PeerSourceP2PGossip),
	)

	o := newOuroboros(OuroborosConfig{Logger: logger})
	o.peerGov = peerGov

	tests := []struct {
		name     string
		amount   int
		expected []string
	}{
		{name: "negative", amount: -1},
		{name: "zero", amount: 0},
		{name: "one", amount: 1, expected: []string{"44.0.0.1:3001"}},
		{
			name:   "exact available subset",
			amount: 2,
			expected: []string{
				"44.0.0.1:3001",
				"[2001:4860:4860::8888]:3002",
			},
		},
		{
			name:   "more than available",
			amount: 10,
			expected: []string{
				"44.0.0.1:3001",
				"[2001:4860:4860::8888]:3002",
				"44.0.0.2:3003",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peers, err := o.peersharingShareRequest(
				opeersharing.CallbackContext{},
				tt.amount,
			)
			require.NoError(t, err)
			require.Len(t, peers, len(tt.expected))
			for i, expected := range tt.expected {
				host, port, err := net.SplitHostPort(expected)
				require.NoError(t, err)
				require.True(t, peers[i].IP.Equal(net.ParseIP(host)))
				require.Equal(t, port, strconv.Itoa(int(peers[i].Port)))
				_, err = peers[i].MarshalCBOR()
				require.NoError(t, err, "every returned peer must serialize")
			}
		})
	}
}

// TestPeerSharingShareRequestWithoutGovernor verifies that peer sharing is
// safe during startup before the peer governor has been wired.
func TestPeerSharingShareRequestWithoutGovernor(t *testing.T) {
	o := newOuroboros(OuroborosConfig{})

	peers, err := o.peersharingShareRequest(
		opeersharing.CallbackContext{},
		1,
	)
	require.NoError(t, err)
	require.Empty(t, peers)
}
