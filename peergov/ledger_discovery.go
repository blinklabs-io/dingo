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
	"time"
)

// discoverLedgerPeers discovers peers from on-chain stake pool relay registrations.
// This method is called during reconciliation if ledger peers are enabled.
//
// Selection is bounded: normal discovery adds only enough peers to reach
// LedgerPeerTarget. Emergency discovery may add one target-sized batch even
// when the known ledger-peer target is already satisfied, because stale or
// unusable peers must not block fresh relay candidates while the node is short
// of connected upstreams. Candidates are shuffled uniformly so no single pool
// dominates across refreshes.
func (p *PeerGovernor) discoverLedgerPeers() {
	// Check if ledger peer provider is configured
	if p.config.LedgerPeerProvider == nil {
		p.config.Logger.Debug(
			"ledger peer discovery skipped: provider is nil",
			"component", "peergov",
		)
		return
	}
	// Check UseLedgerAfterSlot threshold first (before claiming refresh)
	if p.config.UseLedgerAfterSlot < 0 {
		// Ledger peers are disabled
		return
	}
	if p.config.UseLedgerAfterSlot > 0 {
		currentSlot := p.config.LedgerPeerProvider.CurrentSlot()
		// Safe conversion: UseLedgerAfterSlot is already checked to be > 0
		useLedgerAfterSlot := uint64(p.config.UseLedgerAfterSlot) // #nosec G115
		if currentSlot < useLedgerAfterSlot {
			p.config.Logger.Debug(
				"ledger peers not yet enabled",
				"current_slot", currentSlot,
				"use_ledger_after_slot", p.config.UseLedgerAfterSlot,
			)
			return
		}
	}

	// Count existing ledger peers to determine how many we need.
	urgent := p.ledgerPeersUrgent()
	needed := p.ledgerPeerDeficit()
	p.config.Logger.Debug(
		"ledger peer discovery starting",
		"component", "peergov",
		"use_ledger_after_slot", p.config.UseLedgerAfterSlot,
		"needed", needed,
		"emergency", urgent,
	)
	if needed <= 0 && !urgent {
		p.config.Logger.Debug(
			"ledger peer target already satisfied",
			"target", p.config.LedgerPeerTarget,
			"emergency", urgent,
		)
		return
	}

	// Atomically check and claim the refresh to prevent concurrent discoveries.
	// Use CompareAndSwap to ensure only one goroutine proceeds. The normal
	// cadence is LedgerPeerRefreshInterval, but when the node is critically
	// short of connected upstreams it replenishes on a much shorter emergency
	// interval instead of waiting up to an hour: a node must never wedge on a
	// collapsed peer pool while the ledger still lists plenty of relays.
	refreshInterval := p.config.LedgerPeerRefreshInterval
	if urgent {
		refreshInterval = p.config.EmergencyLedgerPeerRefreshInterval
	}
	now := time.Now().UnixNano()
	lastRefresh := p.lastLedgerPeerRefresh.Load()
	if time.Duration(now-lastRefresh) < refreshInterval {
		return
	}
	if !p.lastLedgerPeerRefresh.CompareAndSwap(lastRefresh, now) {
		// Another goroutine claimed the refresh
		return
	}

	// Get pool relays from ledger
	relays, err := p.config.LedgerPeerProvider.GetPoolRelays()
	if err != nil {
		p.config.Logger.Error(
			"failed to get ledger peers",
			"error", err,
			"emergency", urgent,
		)
		// Reset timestamp to allow retry on next reconciliation cycle
		// rather than waiting for the full refresh interval
		p.lastLedgerPeerRefresh.Store(lastRefresh)
		return
	}

	candidates := dedupeRelayCandidates(flattenRelayCandidates(relays))
	extraAdds := 0
	if urgent && needed <= 0 {
		extraAdds = p.config.LedgerPeerTarget
	}
	addedCount := p.addLedgerRelays(relays, extraAdds)

	if addedCount > 0 {
		p.config.Logger.Info(
			"discovered ledger peers",
			"added", addedCount,
			"target", p.config.LedgerPeerTarget,
			"candidates", len(candidates),
			"emergency", urgent,
		)
	} else {
		p.config.Logger.Debug(
			"ledger peer discovery complete",
			"candidates", len(candidates),
			"new_peers", 0,
			"emergency", urgent,
		)
	}
}

// ledgerPeersUrgent reports whether the node is critically short of connected
// upstreams and must replenish ledger peers on the emergency cadence rather
// than waiting for the normal refresh interval. Ledger discovery must be
// enabled (target > 0) for this to apply. The threshold is the hot-peer
// target: while the node has fewer eligible upstreams than it wants hot
// peers, it keeps pulling fresh relays so it never gets stuck on a shrinking
// pool of bad peers.
func (p *PeerGovernor) ledgerPeersUrgent() bool {
	if p.config.LedgerPeerTarget <= 0 {
		return false
	}
	p.mu.Lock()
	upstreams := p.countEligibleUpstreamsLocked()
	p.mu.Unlock()
	return upstreams < p.config.MinHotPeers
}

// ledgerPeerDeficit returns how many more ledger peers are needed to reach
// the configured target. Returns 0 when the target is already satisfied.
func (p *PeerGovernor) ledgerPeerDeficit() int {
	target := p.config.LedgerPeerTarget
	if target <= 0 {
		return 0
	}
	p.mu.Lock()
	current := p.countLedgerPeersLocked()
	p.mu.Unlock()
	deficit := target - current
	if deficit < 0 {
		return 0
	}
	return deficit
}

// countLedgerPeersLocked returns the number of known peers that satisfy the
// ledger peer target. A peer counts if its source is PeerSourceP2PLedger or
// its address was seen during ledger discovery (covering peers that were
// already known from another source such as topology). Must be called with
// p.mu held.
func (p *PeerGovernor) countLedgerPeersLocked() int {
	count := 0
	for _, peer := range p.peers {
		if peer == nil {
			continue
		}
		if peer.Source == PeerSourceP2PLedger {
			count++
			continue
		}
		if _, ok := p.ledgerKnownAddrs[peer.NormalizedAddress]; ok {
			count++
		}
	}
	return count
}

// flattenRelayCandidates converts a slice of PoolRelays into a flat list
// of "host:port" address strings suitable for addLedgerPeer.
func flattenRelayCandidates(relays []PoolRelay) []string {
	// Pre-size: most relays have 1-2 addresses.
	candidates := make([]string, 0, len(relays)*2)
	for _, relay := range relays {
		candidates = append(candidates, relay.Addresses()...)
	}
	return candidates
}

func dedupeRelayCandidates(candidates []string) []string {
	if len(candidates) < 2 {
		return candidates
	}

	seen := make(map[string]struct{}, len(candidates))
	unique := candidates[:0]
	for _, candidate := range candidates {
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		unique = append(unique, candidate)
	}
	return unique
}

// addLedgerPeer adds a peer from ledger discovery with deduplication.
// Returns true if the peer was added, false if it already exists or is denied.
func (p *PeerGovernor) addLedgerPeer(address string) bool {
	// Resolve address (with DNS lookup) before acquiring lock to avoid
	// blocking while holding the mutex. Ledger relay hostnames are
	// attacker-supplied, and resolveLedgerDialTarget's fast path dials
	// whatever ends up here unchanged for the peer's whole lifetime, so this
	// (unlike resolveAddress) filters to a locally-dialable address family.
	normalized := p.resolveLedgerDiscoveryAddress(address)

	// Reject non-routable IPs (private, loopback, link-local, etc.)
	if !isRoutableAddr(normalized) {
		return false
	}
	var evt *pendingEvent
	added := false

	p.mu.Lock()

	// Check deny list
	if p.isDeniedLocked(normalized) || p.isDeniedLocked(p.normalizeAddress(address)) {
		p.mu.Unlock()
		return false
	}

	// Track this address as ledger-discovered so peers from other
	// sources at the same address count toward the ledger target.
	p.ledgerKnownAddrs[normalized] = struct{}{}

	// Check for existing peer using cached NormalizedAddress
	exists := false
	for _, peer := range p.peers {
		if peer == nil {
			continue
		}
		if peer.NormalizedAddress == normalized || peer.Address == address {
			exists = true
			break
		}
	}
	if exists {
		p.mu.Unlock()
		return false
	}

	// Enforce hard cap on peer list size
	if p.isAtPeerCapLocked() {
		p.config.Logger.Debug(
			"rejecting ledger peer: peer list at capacity",
			"address", address,
			"cap", p.maxPeerListSize(),
			"current", len(p.peers),
		)
		p.mu.Unlock()
		return false
	}

	// Add as new peer
	newPeer := &Peer{
		Address:           address,
		NormalizedAddress: normalized,
		Source:            PeerSourceP2PLedger,
		State:             PeerStateCold,
		Sharable:          true, // Ledger peers are public relays
		EMAAlpha:          p.config.EMAAlpha,
		FirstSeen:         time.Now(),
	}
	p.peers = append(p.peers, newPeer)
	p.updatePeerMetrics()

	if p.metrics != nil {
		p.metrics.increasedKnownPeers.Inc()
	}

	// Check if the governor is running (stopCh is set by Start)
	// and outbound connections are enabled before spawning.
	shouldConnect := p.stopCh != nil && !p.config.DisableOutbound
	evt = &pendingEvent{
		PeerAddedEventType,
		PeerStateChangeEvent{Address: address, Reason: "ledger"},
	}
	added = true
	p.mu.Unlock()

	// Publish event outside of lock to avoid deadlock
	p.publishEvent(evt.eventType, evt.data)

	// Spawn an outbound connection goroutine for the new peer.
	// Without this, ledger peers stay cold indefinitely since
	// startOutboundConnections only runs once at startup.
	if shouldConnect {
		go p.createOutboundConnection(newPeer)
	}

	return added
}
