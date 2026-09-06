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
	"strings"
	"testing"
)

func FuzzNormalizeAddress(f *testing.F) {
	f.Add("")
	f.Add("Example.COM:3001")
	f.Add("[0:0:0:0:0:0:0:1]:3001")
	f.Add("malformed:address:with:colons")

	f.Fuzz(func(t *testing.T, address string) {
		var p PeerGovernor
		normalized := p.normalizeAddress(address)
		if p.normalizeAddress(normalized) != normalized {
			t.Fatalf("normalizeAddress is not idempotent: %q -> %q",
				normalized,
				p.normalizeAddress(normalized),
			)
		}

		host, port, err := net.SplitHostPort(address)
		if err != nil {
			if normalized != strings.ToLower(address) {
				t.Fatalf(
					"malformed address normalized to %q, want lowercase %q",
					normalized,
					strings.ToLower(address),
				)
			}
			return
		}
		normalizedHost, normalizedPort, err := net.SplitHostPort(normalized)
		if err != nil {
			t.Fatalf("normalized address is not host:port: %q", normalized)
		}
		if normalizedPort != port {
			t.Fatalf("normalized port = %q, want %q", normalizedPort, port)
		}
		if net.ParseIP(host) == nil && normalizedHost != strings.ToLower(host) {
			t.Fatalf(
				"normalized hostname = %q, want %q",
				normalizedHost,
				strings.ToLower(host),
			)
		}
	})
}

func FuzzAddressHost(f *testing.F) {
	f.Add("")
	f.Add("Example.COM:3001")
	f.Add("[0:0:0:0:0:0:0:1]:3001")

	f.Fuzz(func(t *testing.T, address string) {
		host := addressHost(address)
		inputHost, _, err := net.SplitHostPort(address)
		if err != nil {
			if host != "" {
				t.Fatalf(
					"addressHost(%q) = %q, want empty on parse failure",
					address,
					host,
				)
			}
			return
		}
		if ip := net.ParseIP(inputHost); ip != nil {
			if host != ip.String() {
				t.Fatalf("addressHost IP = %q, want %q", host, ip.String())
			}
			return
		}
		if host != strings.ToLower(inputHost) {
			t.Fatalf(
				"addressHost hostname = %q, want %q",
				host,
				strings.ToLower(inputHost),
			)
		}
	})
}

// FuzzIsRoutableAddr checks the string wrapper, not the routability policy
// itself: whatever host isRoutableAddr extracts must be judged exactly as
// IsRoutableIP judges it, and a host that is not an IP literal must be
// accepted as a hostname. The policy's own address classes are enumerated in
// TestIsRoutableIP, so restating them here would only duplicate that table and
// drift from it — which is what happened when unreachablePrefixes was added.
func FuzzIsRoutableAddr(f *testing.F) {
	f.Add("")
	f.Add("127.0.0.1:3001")
	f.Add("10.0.0.1:3001")
	f.Add("8.8.8.8:3001")
	f.Add("relay.example.com:3001")
	// Seeds inside unreachablePrefixes, which net.IP reports as global
	// unicast. Without these the corpus never reaches that branch.
	f.Add("100.64.0.1:3001")
	f.Add("192.0.0.1:3001")
	f.Add("198.18.0.1:3001")
	f.Add("240.0.0.1:3001")
	f.Add("[100::1]:3001")
	f.Add("[::ffff:100.64.0.1]:3001")

	f.Fuzz(func(t *testing.T, address string) {
		routable := isRoutableAddr(address)
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			host = address
		}
		ip := net.ParseIP(host)
		if ip == nil {
			if !routable {
				t.Fatalf(
					"hostname or malformed address %q should be treated as routable",
					address,
				)
			}
			return
		}
		if want := IsRoutableIP(ip); routable != want {
			t.Fatalf(
				"isRoutableAddr(%q)=%v disagrees with IsRoutableIP(%v)=%v",
				address,
				routable,
				ip,
				want,
			)
		}
	})
}
