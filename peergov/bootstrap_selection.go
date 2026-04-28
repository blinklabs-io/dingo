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
	"strings"
)

func (p *PeerGovernor) bootstrapPromotionEnabled() bool {
	return p.config.BootstrapPromotionEnabled == nil ||
		*p.config.BootstrapPromotionEnabled
}

func (p *PeerGovernor) bootstrapPromotionMinDiversityGroups() int {
	if p.config.BootstrapPromotionMinDiversityGroups <= 0 {
		return defaultBootstrapPromotionMinDiversityGroups
	}
	return p.config.BootstrapPromotionMinDiversityGroups
}

// isBootstrapPromotionHistoricalPeer returns true for topology peers that are
// likely to have historical chain data available during bootstrap.
func (p *PeerGovernor) isBootstrapPromotionHistoricalPeer(peer *Peer) bool {
	return peer != nil && p.isTopologyPeer(peer.Source)
}

// peerDiversityGroup derives a group label used to diversify hot promotions.
// Topology peers use their configured GroupID. Other peers fall back to an IP
// prefix or a normalized hostname.
func (p *PeerGovernor) peerDiversityGroup(peer *Peer) string {
	if peer == nil {
		return ""
	}
	if peer.GroupID != "" {
		return "group:" + peer.GroupID
	}

	address := peer.NormalizedAddress
	if address == "" {
		address = peer.Address
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}
	host = strings.ToLower(host)

	ip := net.ParseIP(host)
	if ip == nil {
		return "host:" + host
	}

	if v4 := ip.To4(); v4 != nil {
		masked := v4.Mask(net.CIDRMask(24, 32))
		return "ipv4:" + masked.String() + "/24"
	}

	masked := ip.Mask(net.CIDRMask(64, 128))
	if masked == nil {
		return "ipv6:" + ip.String()
	}
	return "ipv6:" + masked.String() + "/64"
}
