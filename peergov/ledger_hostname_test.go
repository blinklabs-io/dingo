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

func TestIsResolvableHost(t *testing.T) {
	longLabel := strings.Repeat("a", 64)
	okLabel := strings.Repeat("a", 63)
	longName := strings.Repeat("a.", 127) + "bcd" // 257 bytes

	for _, tc := range []struct {
		host string
		want bool
		why  string
	}{
		// The address observed on a preview block producer (issue #2018).
		{"--pool-relay-port", false, "leading hyphen label"},
		{"", false, "empty"},
		{".", false, "root only"},
		{"relay..example.com", false, "empty inner label"},
		{"-relay.example.com", false, "label starts with a hyphen"},
		{"relay-.example.com", false, "label ends with a hyphen"},
		{longLabel + ".example.com", false, "label over 63 bytes"},
		{longName, false, "name over 253 bytes"},
		{"relay example.com", false, "space"},
		{"relay/example.com", false, "slash"},
		{"relay:3001", false, "colon is not part of a host"},

		{"relay.example.com", true, "ordinary name"},
		{"relay.example.com.", true, "absolute form"},
		{okLabel + ".example.com", true, "label at the 63-byte limit"},
		{"relay-1.example.com", true, "interior hyphen"},
		{"_relay.example.com", true, "underscore is accepted deliberately"},
		{"192.0.2.1", true, "IPv4 literal"},
		{"2001:db8::1", true, "IPv6 literal"},
		{"a", true, "single label"},
	} {
		if got := IsResolvableHost(tc.host); got != tc.want {
			t.Errorf(
				"IsResolvableHost(%q) = %v, want %v (%s)",
				tc.host, got, tc.want, tc.why,
			)
		}
	}
}

// TestPoolRelayAddressesDropsUnresolvableHostname is the regression guard for
// the reported behaviour: the malformed hostname must not become a candidate
// address, while the relay's usable IP addresses still do.
func TestPoolRelayAddressesDropsUnresolvableHostname(t *testing.T) {
	ipv4 := net.ParseIP("192.0.2.10")
	relay := PoolRelay{
		Hostname: "--pool-relay-port",
		IPv4:     &ipv4,
		Port:     3001,
	}
	got := relay.Addresses()
	for _, addr := range got {
		if strings.Contains(addr, "--pool-relay-port") {
			t.Errorf("Addresses() returned the malformed hostname: %q", addr)
		}
	}
	if len(got) != 1 || got[0] != "192.0.2.10:3001" {
		t.Errorf("Addresses() = %v, want only the IPv4 address", got)
	}
}

func TestPoolRelayAddressesKeepsValidHostname(t *testing.T) {
	relay := PoolRelay{Hostname: "relay.example.com", Port: 4200}
	got := relay.Addresses()
	if len(got) != 1 || got[0] != "relay.example.com:4200" {
		t.Errorf("Addresses() = %v, want the hostname preserved", got)
	}
}

// TestFlattenRelayCandidatesDropsUnresolvableHostname covers the discovery
// path itself, which is where the address reached addLedgerPeer.
func TestFlattenRelayCandidatesDropsUnresolvableHostname(t *testing.T) {
	ipv4 := net.ParseIP("198.51.100.7")
	candidates := flattenRelayCandidates([]PoolRelay{
		{Hostname: "--pool-relay-port", Port: 3001},
		{Hostname: "relay.example.com", Port: 3001},
		{IPv4: &ipv4, Port: 3001},
	})
	want := []string{"relay.example.com:3001", "198.51.100.7:3001"}
	if len(candidates) != len(want) {
		t.Fatalf("flattenRelayCandidates() = %v, want %v", candidates, want)
	}
	for i := range want {
		if candidates[i] != want[i] {
			t.Errorf("candidate %d = %q, want %q", i, candidates[i], want[i])
		}
	}
}
