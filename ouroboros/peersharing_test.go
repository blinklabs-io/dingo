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

// TestPeerSharingConfigRegistersShareRequestFuncOnce verifies that
// opeersharing.Config's single ShareRequestFunc slot is wired to the real
// peer-sharing response handler regardless of internal wiring order:
// invoking cfg.ShareRequestFunc against a populated peer governor must
// return that governor's sharable peers, not an empty no-op result. This
// guards against reintroducing a second WithShareRequestFunc registration
// whose relative order would silently decide which callback answers
// incoming ShareRequest messages.
func TestPeerSharingConfigRegistersShareRequestFuncOnce(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	peerGov := peergov.NewPeerGovernor(peergov.PeerGovernorConfig{
		Logger:          logger,
		DisableOutbound: true,
	})
	require.NoError(
		t,
		peerGov.AddPeer("44.0.0.1:3001", peergov.PeerSourceP2PGossip),
	)

	o := newOuroboros(OuroborosConfig{Logger: logger, PeerSharing: true})
	o.peerGov = peerGov

	cfg := o.peerSharingConfig()
	require.NotNil(t, cfg.ShareRequestFunc)

	peers, err := cfg.ShareRequestFunc(opeersharing.CallbackContext{}, 1)
	require.NoError(t, err)
	require.Len(t, peers, 1)
	require.True(t, peers[0].IP.Equal(net.ParseIP("44.0.0.1")))
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

// mkPeerAddr builds a peer-sharing reply entry from an IP literal and port.
// An unparsable literal yields a nil IP so malformed-entry cases can be
// expressed in the same table as valid ones.
func mkPeerAddr(ip string, port uint16) opeersharing.PeerAddress {
	return opeersharing.PeerAddress{IP: net.ParseIP(ip), Port: port}
}

// TestPeerSharingReplyBoundsRequestedCount verifies that a peer-sharing reply
// is bounded by the number of peers we asked for. A remote that answers a
// 5-peer request with a frame-sized reply must not turn into a frame-sized
// batch of peer-governor candidates, each of which costs a DNS resolution and
// a linear dedup scan in peergov.AddPeer.
func TestPeerSharingReplyBoundsRequestedCount(t *testing.T) {
	o := newOuroboros(OuroborosConfig{})

	// A reply far larger than any request we make.
	oversized := make([]opeersharing.PeerAddress, 0, 512)
	for i := range 512 {
		oversized = append(
			oversized,
			mkPeerAddr(
				net.IPv4(44, 0, byte(i/256), byte(i%256)).String(),
				3001,
			),
		)
	}

	tests := []struct {
		name      string
		reply     []opeersharing.PeerAddress
		requested int
		wantLen   int
		wantFirst string
	}{
		{
			name:      "oversized reply truncated to request",
			reply:     oversized,
			requested: defaultPeersToRequest,
			wantLen:   defaultPeersToRequest,
			wantFirst: "44.0.0.0:3001",
		},
		{
			name:      "exact boundary",
			reply:     oversized[:defaultPeersToRequest],
			requested: defaultPeersToRequest,
			wantLen:   defaultPeersToRequest,
			wantFirst: "44.0.0.0:3001",
		},
		{
			name:      "fewer than requested",
			reply:     oversized[:2],
			requested: defaultPeersToRequest,
			wantLen:   2,
			wantFirst: "44.0.0.0:3001",
		},
		{
			name:      "empty reply",
			reply:     nil,
			requested: defaultPeersToRequest,
			wantLen:   0,
		},
		{
			name:      "zero requested",
			reply:     oversized,
			requested: 0,
			wantLen:   0,
		},
		{
			name:      "negative requested",
			reply:     oversized,
			requested: -1,
			wantLen:   0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addrs := o.peerSharingReplyAddresses(tt.reply, tt.requested)
			require.Len(t, addrs, tt.wantLen)
			if tt.wantFirst != "" {
				require.Equal(t, tt.wantFirst, addrs[0])
			}
		})
	}
}

// TestPeerSharingReplyRejectsAddressClasses verifies that no non-globally-
// routable or malformed entry becomes a peer-governor candidate. Each case
// pairs the rejected entry with a valid public IPv4 and IPv6 control, so a
// helper that rejected everything could not pass.
func TestPeerSharingReplyRejectsAddressClasses(t *testing.T) {
	o := newOuroboros(OuroborosConfig{})

	const (
		controlV4 = "44.0.0.1:3001"
		controlV6 = "[2001:4860:4860::8888]:3002"
	)
	controls := []opeersharing.PeerAddress{
		mkPeerAddr("44.0.0.1", 3001),
		mkPeerAddr("2001:4860:4860::8888", 3002),
	}

	tests := []struct {
		name     string
		rejected opeersharing.PeerAddress
	}{
		{"ipv4 loopback", mkPeerAddr("127.0.0.1", 3001)},
		{"ipv4 private 10/8", mkPeerAddr("10.0.0.1", 3001)},
		{"ipv4 private 172.16/12", mkPeerAddr("172.16.0.1", 3001)},
		{"ipv4 private 192.168/16", mkPeerAddr("192.168.1.1", 3001)},
		{"ipv4 link-local", mkPeerAddr("169.254.0.1", 3001)},
		{"ipv4 multicast", mkPeerAddr("224.0.0.1", 3001)},
		{"ipv4 unspecified", mkPeerAddr("0.0.0.0", 3001)},
		{"ipv4 cgnat shared space", mkPeerAddr("100.64.0.1", 3001)},
		{"ipv4 ietf protocol assignments", mkPeerAddr("192.0.0.1", 3001)},
		{"ipv4 benchmarking", mkPeerAddr("198.18.0.1", 3001)},
		{"ipv4 reserved future use", mkPeerAddr("240.0.0.1", 3001)},
		{"ipv4 broadcast", mkPeerAddr("255.255.255.255", 3001)},
		{"ipv6 discard only", mkPeerAddr("100::1", 3001)},
		{"ipv4 this network", mkPeerAddr("0.0.0.1", 3001)},
		{"ipv4 deprecated 6to4 anycast", mkPeerAddr("192.88.99.1", 3001)},
		{"ipv6 benchmarking", mkPeerAddr("2001:2::1", 3001)},
		{"ipv6 local-use translation", mkPeerAddr("64:ff9b:1::1", 3001)},
		{"ipv6 orchid deprecated", mkPeerAddr("2001:10::1", 3001)},
		{"ipv6 loopback", mkPeerAddr("::1", 3001)},
		{"ipv6 unspecified", mkPeerAddr("::", 3001)},
		{"ipv6 unique local", mkPeerAddr("fd00::1", 3001)},
		{"ipv6 link-local", mkPeerAddr("fe80::1", 3001)},
		{"ipv6 multicast", mkPeerAddr("ff02::1", 3001)},
		{"nil ip", opeersharing.PeerAddress{IP: nil, Port: 3001}},
		{
			"wrong length ip",
			opeersharing.PeerAddress{
				IP:   net.IP{1, 2, 3},
				Port: 3001,
			},
		},
		{"zero port", mkPeerAddr("44.0.0.9", 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reply := append(
				[]opeersharing.PeerAddress{tt.rejected},
				controls...,
			)
			addrs := o.peerSharingReplyAddresses(reply, len(reply))
			require.Equal(t, []string{controlV4, controlV6}, addrs)
		})
	}
}

// TestPeerSharingReplyEmitsResolvableLiterals verifies that every accepted
// address is an IP literal. peergov.isRoutableAddr treats a host that does not
// parse as an IP as a routable hostname, so a malformed entry rendered as
// "<nil>:3001" would be accepted there and then sent to a DNS lookup.
func TestPeerSharingReplyEmitsResolvableLiterals(t *testing.T) {
	o := newOuroboros(OuroborosConfig{})

	reply := []opeersharing.PeerAddress{
		{IP: nil, Port: 3001},
		{IP: net.IP{1, 2, 3}, Port: 3001},
		mkPeerAddr("44.0.0.1", 3001),
	}
	for _, addr := range o.peerSharingReplyAddresses(reply, len(reply)) {
		host, _, err := net.SplitHostPort(addr)
		require.NoError(t, err)
		require.NotNil(
			t,
			net.ParseIP(host),
			"every emitted candidate must be an IP literal, got %q",
			addr,
		)
	}
}

func TestPeerSharingReplyCollectsRequestedValidAddresses(t *testing.T) {
	o := newOuroboros(OuroborosConfig{})

	addrs := o.peerSharingReplyAddresses([]opeersharing.PeerAddress{
		{IP: net.ParseIP("10.0.0.1"), Port: 3001},
		mkPeerAddr("44.0.0.1", 3001),
		mkPeerAddr("2001:4860:4860::8888", 3002),
	}, 2)

	require.Equal(t, []string{
		"44.0.0.1:3001",
		"[2001:4860:4860::8888]:3002",
	}, addrs)
}
