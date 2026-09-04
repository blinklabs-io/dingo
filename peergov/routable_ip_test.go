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
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIsRoutableIP pins the routability policy shared by gossip, ledger, and
// peer-sharing candidates. The accepted cases are as load-bearing as the
// rejected ones: RFC 5737 and RFC 3849 addresses stay accepted pending #3792,
// so a tightening that breaks the fixtures in ledger_dial_security_test.go and
// elsewhere fails here first, with the reason and the tracking issue.
func TestIsRoutableIP(t *testing.T) {
	tests := []struct {
		name string
		ip   string
		want bool
	}{
		// Covered by net.IP's own class predicates.
		{"ipv4 public", "44.0.0.1", true},
		{"ipv4 loopback", "127.0.0.1", false},
		{"ipv4 private 10/8", "10.0.0.1", false},
		{"ipv4 private 172.16/12", "172.16.0.1", false},
		{"ipv4 private 192.168/16", "192.168.1.1", false},
		{"ipv4 link-local", "169.254.0.1", false},
		{"ipv4 multicast", "224.0.0.1", false},
		{"ipv4 unspecified", "0.0.0.0", false},
		{"ipv6 public", "2001:4860:4860::8888", true},
		{"ipv6 loopback", "::1", false},
		{"ipv6 unique local", "fd00::1", false},
		{"ipv6 link-local", "fe80::1", false},
		{"ipv6 multicast", "ff02::1", false},
		{"ipv6 unspecified", "::", false},

		// Reported as global unicast by net.IP, rejected here because they
		// reach nothing or reach a host we did not intend.
		{"ipv4 cgnat shared space", "100.64.0.1", false},
		{"ipv4 cgnat upper bound", "100.127.255.255", false},
		{"ipv4 ietf protocol assignments", "192.0.0.1", false},
		// Rejected with the rest of the block although IANA marks these two
		// as globally reachable: PCP anycast (RFC 7723) and TURN anycast
		// (RFC 8155) are never Cardano relays.
		{"ipv4 pcp anycast", "192.0.0.9", false},
		{"ipv4 turn anycast", "192.0.0.10", false},
		{"ipv4 benchmarking", "198.18.0.1", false},
		{"ipv4 reserved future use", "240.0.0.1", false},
		{"ipv4 broadcast", "255.255.255.255", false},
		{"ipv6 discard only", "100::1", false},
		{"ipv4 this network", "0.0.0.1", false},
		{"ipv4 deprecated 6to4 anycast", "192.88.99.1", false},
		{"ipv6 benchmarking", "2001:2::1", false},
		{"ipv6 local-use translation", "64:ff9b:1::1", false},
		{"ipv6 orchid deprecated", "2001:10::1", false},

		// IANA marks these Globally Reachable, so they must stay accepted.
		// They sit next to rejected ranges and would be easy to sweep up.
		{"ipv4 as112", "192.31.196.1", true},
		{"ipv4 amt", "192.52.193.1", true},
		{"ipv6 as112", "2001:4:112::1", true},
		{"ipv6 amt", "2001:3::1", true},
		{"ipv4 translation nat64", "64:ff9b::1", true},

		// Just outside the rejected ranges, to prove the bounds.
		{"ipv4 below cgnat", "100.63.255.255", true},
		{"ipv4 above cgnat", "100.128.0.0", true},
		{"ipv4 below benchmarking", "198.17.255.255", true},
		{"ipv4 above benchmarking", "198.20.0.0", true},
		{"ipv4 below reserved", "239.255.255.255", false}, // multicast
		{"ipv4 outside protocol assignments", "192.0.1.1", true},
		{"ipv4 top of this network", "0.255.255.255", false},
		{"ipv4 above this network", "1.0.0.0", true},
		{"ipv4 below 6to4 anycast", "192.88.98.255", true},
		{"ipv4 top of 6to4 anycast", "192.88.99.255", false},
		{"ipv4 above 6to4 anycast", "192.88.100.0", true},
		{
			"ipv6 below benchmarking",
			"2001:1:ffff:ffff:ffff:ffff:ffff:ffff",
			true,
		},
		{"ipv6 top of benchmarking", "2001:2:0:ffff:ffff:ffff:ffff:ffff", false},
		{"ipv6 above benchmarking", "2001:2:1::", true},
		{"ipv6 above local-use translation", "64:ff9b:2::", true},
		{
			"ipv6 below orchid",
			"2001:f:ffff:ffff:ffff:ffff:ffff:ffff",
			true,
		},
		{
			"ipv6 top of orchid",
			"2001:1f:ffff:ffff:ffff:ffff:ffff:ffff",
			false,
		},
		{"ipv6 orchidv2", "2001:20::1", false},

		// Accepted pending the decision in #3792: not routed anywhere, and
		// used as public stand-ins across ~19 files' fixtures.
		{"ipv4 test-net-1", "192.0.2.1", true},
		{"ipv4 test-net-2", "198.51.100.1", true},
		{"ipv4 test-net-3", "203.0.113.1", true},
		{"ipv6 documentation", "2001:db8::1", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ip := net.ParseIP(tt.ip)
			require.NotNil(t, ip, "test case address must parse")
			require.Equal(t, tt.want, IsRoutableIP(ip))
		})
	}
}

// TestIsRoutableIPMalformed covers values that never come off the wire but
// would read as routable if the length were not checked: every net.IP class
// predicate reports false for a length other than 4 or 16.
func TestIsRoutableIPMalformed(t *testing.T) {
	require.False(t, IsRoutableIP(nil))
	require.False(t, IsRoutableIP(net.IP{}))
	require.False(t, IsRoutableIP(net.IP{1, 2, 3}))
	require.False(t, IsRoutableIP(make(net.IP, 5)))
}

// TestIsRoutableIPUnmapsV4 verifies that an IPv4-mapped IPv6 address is
// matched against the IPv4 prefixes. Without the unmap it would miss every
// one of them and be accepted.
func TestIsRoutableIPUnmapsV4(t *testing.T) {
	mapped := net.ParseIP("::ffff:100.64.0.1")
	require.NotNil(t, mapped)
	require.Len(t, mapped, net.IPv6len, "want the 16-byte mapped form")
	require.False(t, IsRoutableIP(mapped))
}
