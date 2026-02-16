// Copyright 2025 Blink Labs Software
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

package connmanager

import "net"

// ipKeyFromAddr extracts a rate-limit key from a net.Addr. For IPv4
// addresses the key is the bare IP string.  For IPv6 addresses the key
// is the /64 prefix so that an attacker rotating within a single /64
// subnet is still rate-limited as one source. Non-TCP/UDP addresses
// (e.g. Unix sockets) return an empty string and are exempt from
// rate limiting.
func ipKeyFromAddr(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		// Address may not have a port (e.g. Unix sockets)
		return ""
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return ""
	}
	// IPv4 or IPv4-mapped IPv6: use the full address as the key
	if ip4 := ip.To4(); ip4 != nil {
		return ip4.String()
	}
	// IPv6: mask to /64 prefix to handle subnet rotation
	mask := net.CIDRMask(64, 128)
	return ip.Mask(mask).String() + "/64"
}

// acquireIPSlot attempts to reserve a connection slot for the given IP
// key. It returns true if the connection is allowed, false if the
// per-IP limit has been reached.
func (c *ConnectionManager) acquireIPSlot(ipKey string) bool {
	if ipKey == "" {
		return true // exempt (e.g. Unix sockets)
	}
	c.ipConnsMutex.Lock()
	defer c.ipConnsMutex.Unlock()
	if c.ipConns[ipKey] >= c.config.MaxConnectionsPerIP {
		return false
	}
	c.ipConns[ipKey]++
	return true
}

// releaseIPSlot decrements the connection count for the given IP key.
func (c *ConnectionManager) releaseIPSlot(ipKey string) {
	if ipKey == "" {
		return
	}
	c.ipConnsMutex.Lock()
	defer c.ipConnsMutex.Unlock()
	c.ipConns[ipKey]--
	if c.ipConns[ipKey] <= 0 {
		delete(c.ipConns, ipKey)
	}
}

// ipConnCount returns the current connection count for an IP key.
// Exported for testing only.
func (c *ConnectionManager) IPConnCount(ipKey string) int {
	c.ipConnsMutex.Lock()
	defer c.ipConnsMutex.Unlock()
	return c.ipConns[ipKey]
}
