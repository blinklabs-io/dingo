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

import "time"

// discoverLedgerPeers discovers peers from on-chain stake pool relay registrations.
// This method is called during reconciliation if ledger peers are enabled.
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

	// Track how many peers we added
	addedCount := 0

	// Add each relay as a peer (with deduplication)
	for _, relay := range relays {
		addresses := relay.Addresses()
		for _, addr := range addresses {
			if p.addLedgerPeer(addr) {
				addedCount++
			}
		}
	}

	if addedCount > 0 {
		p.config.Logger.Info(
			"discovered ledger peers",
			"added", addedCount,
			"total_relays", len(relays),
		)
	} else {
		p.config.Logger.Debug(
			"ledger peer discovery complete",
			"total_relays", len(relays),
			"new_peers", 0,
		)
	}
}

// addLedgerPeer adds a peer from ledger discovery with deduplication.
// Returns true if the peer was added, false if it already exists or is denied.
func (p *PeerGovernor) addLedgerPeer(address string) bool {
	// Resolve address (with DNS lookup) before acquiring lock to avoid
	// blocking while holding the mutex
	normalized := p.resolveAddress(address)
	var evt *pendingEvent
	added := false

	p.mu.Lock()

	// Check deny list
	if p.isDeniedLocked(normalized) {
		p.mu.Unlock()
		return false
	}

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

	evt = &pendingEvent{
		PeerAddedEventType,
		PeerStateChangeEvent{Address: address, Reason: "ledger"},
	}
	added = true
	p.mu.Unlock()

	// Publish event outside of lock to avoid deadlock
	p.publishEvent(evt.eventType, evt.data)

	return added
}
