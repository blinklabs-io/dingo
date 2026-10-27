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

package utxorpc

import (
	"net"
	"net/netip"
)

// clientKey names the client a peer address belongs to; every per-client
// budget in this package keys on it. An IPv4 address, including an
// IPv4-mapped IPv6 one, is its own client. An IPv6 address stands for its
// /64, the smallest prefix a network normally delegates to one subscriber, so
// a caller cannot multiply its allowance by rotating addresses inside its
// prefix. Zones are dropped. Input that is not an IP address keys as the host
// part, or the raw string when it has no port.
func clientKey(peerAddr string) string {
	host := peerAddr
	if h, _, err := net.SplitHostPort(peerAddr); err == nil {
		host = h
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return host
	}
	addr = addr.WithZone("").Unmap()
	if addr.Is4() {
		return addr.String()
	}
	prefix, err := addr.Prefix(64)
	if err != nil {
		return host
	}
	return prefix.String()
}
