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
	"math/rand/v2"
	"time"
)

// discoverLedgerPeers discovers peers from on-chain stake pool relay registrations.
// This method is called during reconciliation if ledger peers are enabled.
//
// Selection is bounded: only enough peers are added to reach LedgerPeerTarget.
// Candidates are shuffled uniformly so no single pool dominates across refreshes.
func (p *PeerGovernor) discoverLedgerPeers() {
	// Check if ledger peer provider is configured
	if p.config.LedgerPeerProvider == nil {
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
	needed := p.ledgerPeerDeficit()
	if needed <= 0 {
		p.config.Logger.Debug(
			"ledger peer target already satisfied",
			"target", p.config.LedgerPeerTarget,
		)
		return
	}

	// Atomically check and claim the refresh to prevent concurrent discoveries.
	// Use CompareAndSwap to ensure only one goroutine proceeds.
	now := time.Now().UnixNano()
	lastRefresh := p.lastLedgerPeerRefresh.Load()
	if time.Duration(now-lastRefresh) < p.config.LedgerPeerRefreshInterval {
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
		)
		// Reset timestamp to allow retry on next reconciliation cycle
		// rather than waiting for the full refresh interval
		p.lastLedgerPeerRefresh.Store(lastRefresh)
		return
	}

	// Flatten relays into candidate addresses, deduplicate them, and
	// shuffle so we do not always pick the same relays in provider-returned
	// order.
	candidates := dedupeRelayCandidates(flattenRelayCandidates(relays))
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	// Add candidates until we reach the target or exhaust the list.
	addedCount := 0
	for _, addr := range candidates {
		if p.ledgerPeerDeficit() <= 0 {
			break
		}
		if p.addLedgerPeer(addr) {
			addedCount++
		}
	}

	if addedCount > 0 {
		p.config.Logger.Info(
			"discovered ledger peers",
			"added", addedCount,
			"target", p.config.LedgerPeerTarget,
			"candidates", len(candidates),
		)
	} else {
		p.config.Logger.Debug(
			"ledger peer discovery complete",
			"candidates", len(candidates),
			"new_peers", 0,
		)
	}
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
	// blocking while holding the mutex
	normalized := p.resolveAddress(address)

	// Reject non-routable IPs (private, loopback, link-local, etc.)
	if !isRoutableAddr(normalized) {
		return false
	}
	var evt *pendingEvent
	added := false

	p.mu.Lock()

	// Check deny list
	if p.isDeniedLocked(normalized) {
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
