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

import "time"

type inboundFlapRecord struct {
	shortLivedCount uint32
	cooldownUntil   time.Time
	expiresAt       time.Time
}

// inboundFlapHostKey keeps ephemeral source ports out of the identity used
// for inbound flapping. The prefix separates it from address deny keys.
func inboundFlapHostKey(address string) string {
	if host := addressHost(address); host != "" {
		return "inbound-host:" + host
	}
	return ""
}

// rememberInboundFlapLocked retains the escalation across a pruned peer
// record. The retained history is bounded by the peer-list cap and expires
// after one more admission window following the cooldown.
func (p *PeerGovernor) rememberInboundFlapLocked(
	peer *Peer,
	now time.Time,
	cooldown time.Duration,
) {
	key := inboundFlapHostKey(peer.NormalizedAddress)
	if key == "" {
		return
	}
	p.cleanupInboundFlapHistoryLocked(now)
	if _, exists := p.inboundFlapHistory[key]; !exists &&
		len(p.inboundFlapHistory) >= p.maxPeerListSize() {
		var oldestKey string
		var oldestExpiry time.Time
		for candidate, record := range p.inboundFlapHistory {
			if oldestKey == "" || record.expiresAt.Before(oldestExpiry) {
				oldestKey, oldestExpiry = candidate, record.expiresAt
			}
		}
		delete(p.inboundFlapHistory, oldestKey)
	}
	until := now.Add(cooldown)
	p.inboundFlapHistory[key] = inboundFlapRecord{
		shortLivedCount: peer.InboundShortLivedCount,
		cooldownUntil:   until,
		expiresAt:       until.Add(p.config.InboundCooldown),
	}
}

func (p *PeerGovernor) cleanupInboundFlapHistoryLocked(now time.Time) {
	for key, record := range p.inboundFlapHistory {
		if !now.Before(record.expiresAt) {
			delete(p.inboundFlapHistory, key)
		}
	}
}
