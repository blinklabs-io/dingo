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

package peergov

import (
	"net"
	"strconv"
	"strings"
)

// SyncProgressProvider defines an interface for querying sync progress.
// This allows the peer governor to check sync progress without directly
// depending on the chain or node packages.
type SyncProgressProvider interface {
	// SyncProgress returns the current sync progress as a value between 0 and 1.
	// A value of 1.0 indicates fully synced.
	// A value of 0.0 indicates just started or unknown.
	SyncProgress() float64
}

// LedgerPeerProvider defines an interface for querying ledger peer information.
// This allows the peer governor to discover peers from on-chain stake pool
// relay registrations without directly depending on the ledger package.
//
// Ledger peer discovery enables a node to find peers by reading stake pool
// relay information from the blockchain itself. This is the primary mechanism
// for peer discovery in Cardano's decentralized network, complementing
// topology-based and gossip-based peer discovery.
//
// The provider is queried periodically (controlled by LedgerPeerRefreshInterval)
// once the chain tip has passed UseLedgerAfterSlot. This allows nodes to
// initially sync using bootstrap/topology peers before transitioning to
// ledger-based discovery.
type LedgerPeerProvider interface {
	// GetPoolRelays returns all active pool relays from the ledger.
	// A pool is considered active if it has a registration certificate and
	// either no retirement certificate, or a retirement epoch in the future.
	// Returns an empty slice if no relays are available.
	// Returns an error if the underlying data cannot be read.
	GetPoolRelays() ([]PoolRelay, error)

	// CurrentSlot returns the current chain tip slot number.
	// This is used to check if UseLedgerAfterSlot threshold has been reached.
	CurrentSlot() uint64
}

// PoolRelay represents a stake pool relay for peer discovery.
// This corresponds to the relay information in a pool registration certificate.
//
// Cardano supports three relay types:
//   - SingleHostAddress: Direct IP address(es) with port
//   - SingleHostName: DNS hostname with port
//   - MultiHostName: DNS hostname without port (not commonly used)
//
// At least one of Hostname, IPv4, or IPv6 must be set.
type PoolRelay struct {
	// Hostname is the DNS hostname of the relay (for SingleHostName relays)
	Hostname string

	// IPv4 is the IPv4 address of the relay (for SingleHostAddress relays)
	IPv4 *net.IP

	// IPv6 is the IPv6 address of the relay (for SingleHostAddress relays)
	IPv6 *net.IP

	// Port is the port number of the relay. If 0, defaults to 3001.
	Port uint
}

// Addresses returns the network addresses for this relay.
// For hostname relays, returns "hostname:port".
// For IP relays, returns "ip:port" for each configured IP address.
func (r PoolRelay) Addresses() []string {
	var addresses []string
	portStr := formatPort(r.Port)

	// A hostname that cannot resolve is not a peer. The ledger carries
	// whatever an operator registered, and a value like "--pool-relay-port"
	// becomes a dial attempt, four connection failures and a half-hour deny
	// entry before anything notices it was never an address (issue #2018).
	if IsResolvableHost(r.Hostname) {
		addresses = append(addresses, net.JoinHostPort(r.Hostname, portStr))
	}
	if r.IPv4 != nil && len(*r.IPv4) > 0 {
		addresses = append(
			addresses,
			net.JoinHostPort(r.IPv4.String(), portStr),
		)
	}
	if r.IPv6 != nil && len(*r.IPv6) > 0 {
		addresses = append(
			addresses,
			net.JoinHostPort(r.IPv6.String(), portStr),
		)
	}

	return addresses
}

// IsResolvableHost reports whether host could be resolved to an address: an IP
// literal, or a syntactically valid DNS name.
//
// It is deliberately permissive about what a DNS name may contain, because the
// two failure modes are not symmetric. Rejecting a name that would have
// resolved costs a real relay; admitting one that cannot resolve costs a dial,
// its retries and a deny entry. So this rejects only what no resolver could
// answer -- a label that is empty, over-long, or starts or ends with a hyphen,
// a name over 253 bytes, or a byte outside the set operators actually use --
// and leaves anything else to DNS.
//
// Underscores are accepted. They are invalid in a host name under RFC 1123 but
// appear in real zones, and a resolver will attempt them.
func IsResolvableHost(host string) bool {
	if host == "" {
		return false
	}
	if net.ParseIP(host) != nil {
		return true
	}
	// A single trailing dot is the valid absolute form.
	name := strings.TrimSuffix(host, ".")
	if name == "" || len(name) > maxDNSNameLen {
		return false
	}
	for label := range strings.SplitSeq(name, ".") {
		if !isValidDNSLabel(label) {
			return false
		}
	}
	return true
}

func isValidDNSLabel(label string) bool {
	if label == "" || len(label) > maxDNSLabelLen {
		return false
	}
	if label[0] == '-' || label[len(label)-1] == '-' {
		return false
	}
	for i := range len(label) {
		c := label[i]
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}

const (
	// maxDNSNameLen is the longest name a resolver will accept, excluding the
	// root label.
	maxDNSNameLen = 253
	// maxDNSLabelLen is the longest single label.
	maxDNSLabelLen = 63
)

// formatPort converts a port number to a string.
// Returns the default Cardano port "3001" if port is 0.
func formatPort(port uint) string {
	if port == 0 {
		return "3001" // Default Cardano port
	}
	return strconv.FormatUint(uint64(port), 10)
}
