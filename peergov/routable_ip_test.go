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
// rejected ones: RFC 5737 and RFC 3849 addresses stay accepted because this
// package uses them as stand-ins for public addresses, so a future tightening
// that breaks ledger_dial_security_test.go fails here first with the reason.
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
		{"ipv4 benchmarking", "198.18.0.1", false},
		{"ipv4 reserved future use", "240.0.0.1", false},
		{"ipv4 broadcast", "255.255.255.255", false},
		{"ipv6 discard only", "100::1", false},

		// Just outside the rejected ranges, to prove the bounds.
		{"ipv4 below cgnat", "100.63.255.255", true},
		{"ipv4 above cgnat", "100.128.0.0", true},
		{"ipv4 below benchmarking", "198.17.255.255", true},
		{"ipv4 above benchmarking", "198.20.0.0", true},
		{"ipv4 below reserved", "239.255.255.255", false}, // multicast
		{"ipv4 outside protocol assignments", "192.0.1.1", true},

		// Deliberately accepted: not routed anywhere, and used as public
		// stand-ins by this package's tests.
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
