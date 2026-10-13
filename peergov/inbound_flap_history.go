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
	"time"
)

type inboundFlapRecord struct {
	shortLivedCount uint32
	cooldownUntil   time.Time
	expiresAt       time.Time
}

type inboundHostChurnRecord struct {
	shortLivedCount uint32
	windowStarted   time.Time
	cooldownUntil   time.Time
	expiresAt       time.Time
}

func inboundFlapHostKey(address string) string {
	host := addressHost(address)
	if host == "" {
		return ""
	}
	host, _, _ = strings.Cut(host, "%")
	ip := net.ParseIP(host)
	if ip == nil {
		return strings.ToLower(host)
	}
	if ip4 := ip.To4(); ip4 != nil {
		return ip4.String()
	}
	return ip.Mask(net.CIDRMask(64, 128)).String() + "/64"
}

func (p *PeerGovernor) inboundHostChurnLimit() int {
	return p.maxPeerListSize() / 2
}

// rememberInboundFlapLocked retains the escalation across a pruned peer
// record. The retained history is bounded by the peer-list cap and expires
// after one more admission window following the cooldown.
func (p *PeerGovernor) rememberInboundFlapLocked(
	peer *Peer,
	now time.Time,
	cooldown time.Duration,
) {
	key := peer.NormalizedAddress
	if key == "" {
		return
	}
	p.cleanupInboundFlapHistoryLocked(now)
	if _, exists := p.inboundFlapHistory[key]; !exists &&
		len(p.inboundFlapHistory) >= p.maxPeerListSize() {
		return
	}
	until := now.Add(cooldown)
	p.inboundFlapHistory[key] = inboundFlapRecord{
		shortLivedCount: peer.InboundShortLivedCount,
		cooldownUntil:   until,
		expiresAt:       until.Add(p.config.InboundCooldown),
	}
}

// rememberInboundHostChurnLocked records short sessions by host as a
// high-volume abuse backstop. Tuple history remains authoritative for ordinary
// flap cooldowns, so unrelated peers behind one NAT do not share escalation.
func (p *PeerGovernor) rememberInboundHostChurnLocked(
	peer *Peer,
	now time.Time,
) {
	key := inboundFlapHostKey(peer.NormalizedAddress)
	if key == "" {
		return
	}
	p.cleanupInboundHostChurnLocked(now)
	record, exists := p.inboundHostChurn[key]
	if !exists && len(p.inboundHostChurn) >= p.maxPeerListSize() {
		return
	}
	if !exists || record.windowStarted.IsZero() ||
		(!now.Before(record.cooldownUntil) &&
			now.Sub(record.windowStarted) >= p.config.InboundCooldown) {
		record = inboundHostChurnRecord{windowStarted: now}
	}
	record.shortLivedCount++
	record.expiresAt = record.windowStarted.Add(p.config.InboundCooldown)
	if int(record.shortLivedCount) >= p.inboundHostChurnLimit() {
		record.cooldownUntil = now.Add(p.config.InboundCooldown)
		record.expiresAt = record.cooldownUntil
	}
	p.inboundHostChurn[key] = record
}

func (p *PeerGovernor) cleanupInboundHostChurnLocked(now time.Time) {
	for key, record := range p.inboundHostChurn {
		if !now.Before(record.expiresAt) {
			delete(p.inboundHostChurn, key)
		}
	}
}

func (p *PeerGovernor) cleanupInboundFlapHistoryLocked(now time.Time) {
	for key, record := range p.inboundFlapHistory {
		if !now.Before(record.expiresAt) {
			delete(p.inboundFlapHistory, key)
		}
	}
}
