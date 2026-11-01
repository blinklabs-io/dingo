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
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

// zoneDroppingLookup mimics net.DefaultResolver for a scoped IPv6 literal: it
// answers with the address and no zone, which is what lookupIPAddr returns
// because it keeps only IPAddr.IP. It records every host it is asked about.
func zoneDroppingLookup(t *testing.T, calls *[]string) {
	t.Helper()
	old := lookupIPAddr
	lookupIPAddr = func(_ context.Context, host string) ([]net.IP, error) {
		*calls = append(*calls, host)
		return []net.IP{net.ParseIP("fe80::1")}, nil
	}
	t.Cleanup(func() { lookupIPAddr = old })
}

func TestIsRoutableAddr_ScopedIPv6(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		address  string
		routable bool
	}{
		{"scoped link-local", "[fe80::1%eth0]:3001", false},
		{"scoped link-local no port", "fe80::1%eth0", false},
		{"scoped loopback", "[::1%lo]:3001", false},
		// A zone names an interface on this host. No publicly routable
		// peer needs one, and accepting it would let zone variants of one
		// address occupy separate peer entries.
		{"scoped global", "[2001:4860:4860::8888%eth0]:3001", false},
		{"scoped IPv4-mapped", "[::ffff:8.8.8.8%eth0]:3001", false},
		{"unscoped link-local", "[fe80::1]:3001", false},
		{"unscoped loopback", "[::1]:3001", false},
		{"unscoped global", "[2001:4860:4860::8888]:3001", true},
		{"hostname", "relay.example.com:3001", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.routable, isRoutableAddr(tt.address))
		})
	}
}

// Not t.Parallel: swaps the package-level lookupIPAddr seam.
// A scoped literal must be dialed with its zone: connect(2) rejects a
// link-local destination without one ("invalid argument").
func TestResolveDialAddress_ScopedIPv6LiteralUnchanged(t *testing.T) {
	var calls []string
	zoneDroppingLookup(t, &calls)

	pg := newDialSpreadGovernor()
	const addr = "[fe80::1%eth0]:3001"
	assert.Equal(t, addr, pg.resolveDialAddress(context.Background(), addr))
	assert.Empty(t, calls, "an IP literal must not be resolved")
}

// Not t.Parallel: swaps the package-level lookupIPAddr seam.
func TestResolveAddress_ScopedIPv6LiteralKeepsZone(t *testing.T) {
	var calls []string
	zoneDroppingLookup(t, &calls)

	pg := newDialSpreadGovernor()
	assert.Equal(
		t,
		"[fe80::1%eth0]:3001",
		pg.resolveAddress("[FE80::0:1%eth0]:3001"),
	)
	assert.Empty(t, calls, "an IP literal must not be resolved")
}

func TestNormalizeAddress_ScopedIPv6Canonical(t *testing.T) {
	t.Parallel()
	pg := newDialSpreadGovernor()
	tests := []struct {
		input    string
		expected string
	}{
		{"[FE80::0:1%eth0]:3001", "[fe80::1%eth0]:3001"},
		// Interface names are case-sensitive, so the zone is kept as given.
		{"[fe80::1%Eth0]:3001", "[fe80::1%Eth0]:3001"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, pg.normalizeAddress(tt.input))
		})
	}
}

// Not t.Parallel: swaps the package-level lookupIPAddr seam.
// The gossip rejection must not depend on what the resolver does with a
// scoped literal.
func TestAddPeer_RejectsScopedIPv6GossipPeer(t *testing.T) {
	old := lookupIPAddr
	lookupIPAddr = func(context.Context, string) ([]net.IP, error) {
		return nil, errors.New("lookup failed")
	}
	t.Cleanup(func() { lookupIPAddr = old })

	pg := NewPeerGovernor(PeerGovernorConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	for _, addr := range []string{
		"[fe80::1%eth0]:3001",
		"[2001:4860:4860::8888%eth0]:3001",
	} {
		err := pg.AddPeer(addr, PeerSourceP2PGossip)
		assert.ErrorIs(t, err, ErrUnroutableAddress, addr)
	}
	assert.Empty(t, pg.GetPeers())
}
