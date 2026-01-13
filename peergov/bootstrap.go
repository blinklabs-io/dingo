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

import "fmt"

// shouldExitBootstrap checks if conditions are met to exit bootstrap mode.
// Returns true if ANY of these conditions are met:
// 1. Current slot > UseLedgerAfterSlot (ledger peers are enabled)
// 2. Ledger peer count >= MinLedgerPeersForExit
// 3. Sync progress >= SyncProgressForExit
// Must be called with p.mu held.
func (p *PeerGovernor) shouldExitBootstrap() (bool, string) {
	// Already exited
	if p.bootstrapExited {
		return false, ""
	}

	// Check if we have any bootstrap peers to exit
	hasBootstrapPeers := false
	for _, peer := range p.peers {
		if peer != nil && peer.Source == PeerSourceTopologyBootstrapPeer {
			hasBootstrapPeers = true
			break
		}
	}
	if !hasBootstrapPeers {
		return false, ""
	}

	// Condition 1: Current slot > UseLedgerAfterSlot
	if p.config.UseLedgerAfterSlot > 0 && p.config.LedgerPeerProvider != nil {
		currentSlot := p.config.LedgerPeerProvider.CurrentSlot()
		// Safe conversion: UseLedgerAfterSlot is already checked to be > 0
		useLedgerAfterSlot := uint64(p.config.UseLedgerAfterSlot) // #nosec G115
		if currentSlot > useLedgerAfterSlot {
			return true, "slot threshold reached"
		}
	}

	// Condition 2: Ledger peer count >= MinLedgerPeersForExit
	if p.config.MinLedgerPeersForExit > 0 {
		ledgerPeerCount := 0
		for _, peer := range p.peers {
			if peer != nil && peer.Source == PeerSourceP2PLedger &&
				(peer.State == PeerStateWarm || peer.State == PeerStateHot) {
				ledgerPeerCount++
			}
		}
		if ledgerPeerCount >= p.config.MinLedgerPeersForExit {
			return true, fmt.Sprintf(
				"ledger peer count (%d) >= threshold (%d)",
				ledgerPeerCount,
				p.config.MinLedgerPeersForExit,
			)
		}
	}

	// Condition 3: Sync progress >= SyncProgressForExit
	if p.config.SyncProgressForExit > 0 &&
		p.config.SyncProgressProvider != nil {
		syncProgress := p.config.SyncProgressProvider.SyncProgress()
		if syncProgress >= p.config.SyncProgressForExit {
			return true, fmt.Sprintf(
				"sync progress (%.2f%%) >= threshold (%.2f%%)",
				syncProgress*100,
				p.config.SyncProgressForExit*100,
			)
		}
	}

	return false, ""
}

// exitBootstrap demotes all bootstrap peers to cold and marks bootstrap as exited.
// This closes connections to bootstrap peers and prevents them from being promoted.
// Acquires p.mu internally and publishes events after releasing it to avoid deadlock.
//
//nolint:unused // Used by tests
func (p *PeerGovernor) exitBootstrap(reason string) {
	p.mu.Lock()
	events := p.exitBootstrapLocked(reason)
	p.mu.Unlock()

	p.publishPendingEvents(events)
}

// exitBootstrapLocked exits bootstrap mode and returns pending events.
// Must be called with p.mu held.
func (p *PeerGovernor) exitBootstrapLocked(reason string) []pendingEvent {
	var events []pendingEvent
	if p.bootstrapExited {
		return events
	}

	demotedCount := 0
	for _, peer := range p.peers {
		if peer == nil || peer.Source != PeerSourceTopologyBootstrapPeer {
			continue
		}
		if peer.State == PeerStateHot || peer.State == PeerStateWarm {
			prevState := peer.State
			peer.State = PeerStateCold
			demotedCount++

			// Close the connection if it exists
			if peer.Connection != nil {
				if p.config.ConnManager != nil {
					conn := p.config.ConnManager.GetConnectionById(
						peer.Connection.Id,
					)
					if conn != nil {
						conn.Close()
					}
				}
				peer.Connection = nil
			}

			p.config.Logger.Info(
				"bootstrap exit: demoted peer to cold",
				"address", peer.Address,
				"previous_state", prevState,
			)
			if p.metrics != nil {
				p.metrics.churnDemotionsBySource.WithLabelValues(
					peer.Source.String(),
				).Inc()
			}
			events = append(events, pendingEvent{
				PeerDemotedEventType,
				PeerStateChangeEvent{
					Address: peer.Address,
					Reason:  "bootstrap exit",
				},
			})
		}
	}

	p.bootstrapExited = true
	p.config.Logger.Info(
		"exited bootstrap mode",
		"reason", reason,
		"demoted_peers", demotedCount,
	)

	events = append(events, pendingEvent{
		BootstrapExitedEventType,
		BootstrapExitedEvent{
			Reason:       reason,
			DemotedPeers: demotedCount,
		},
	})

	p.updatePeerMetrics()
	return events
}

// checkBootstrapRecovery checks if bootstrap peers should be re-enabled.
// This happens when:
// - AutoBootstrapRecovery is enabled
// - Hot peer count < MinHotPeers
// - No gossip or ledger peers are available as warm candidates
// Acquires p.mu internally and publishes events after releasing it to avoid deadlock.
//
//nolint:unused // Used by tests
func (p *PeerGovernor) checkBootstrapRecovery() {
	p.mu.Lock()
	events := p.checkBootstrapRecoveryLocked()
	p.mu.Unlock()

	p.publishPendingEvents(events)
}

// checkBootstrapRecoveryLocked checks if bootstrap peers should be re-enabled
// and returns pending events. Must be called with p.mu held.
func (p *PeerGovernor) checkBootstrapRecoveryLocked() []pendingEvent {
	events := make([]pendingEvent, 0, 1) // At most one recovery event

	// Only check if bootstrap was previously exited
	if !p.bootstrapExited {
		return events
	}

	// Check if auto-recovery is enabled
	// nil = use default (true), explicit false disables
	if p.config.AutoBootstrapRecovery != nil &&
		!*p.config.AutoBootstrapRecovery {
		return events
	}

	// Count hot peers
	hotCount := 0
	for _, peer := range p.peers {
		if peer != nil && peer.State == PeerStateHot {
			hotCount++
		}
	}

	// Only recover if we're below minimum hot peers
	if hotCount >= p.config.MinHotPeers {
		return events
	}

	// Check if any gossip or ledger peers are available as warm candidates
	hasWarmCandidates := false
	for _, peer := range p.peers {
		if peer == nil {
			continue
		}
		if peer.State == PeerStateWarm && peer.Connection != nil {
			if peer.Source == PeerSourceP2PGossip ||
				peer.Source == PeerSourceP2PLedger {
				hasWarmCandidates = true
				break
			}
		}
	}

	// If we have warm candidates from gossip/ledger, don't recover bootstrap
	if hasWarmCandidates {
		return events
	}

	// Re-enable bootstrap peers
	p.bootstrapExited = false
	p.config.Logger.Info(
		"re-enabling bootstrap peers due to low hot peer count",
		"hot_count", hotCount,
		"min_hot_peers", p.config.MinHotPeers,
	)

	events = append(events, pendingEvent{
		BootstrapRecoveryEventType,
		BootstrapRecoveryEvent{
			HotPeerCount: hotCount,
			MinHotPeers:  p.config.MinHotPeers,
		},
	})

	// Attempt to reconnect to bootstrap peers (only if ConnManager is available)
	if p.config.ConnManager != nil {
		for _, peer := range p.peers {
			if peer != nil && peer.Source == PeerSourceTopologyBootstrapPeer &&
				peer.State == PeerStateCold && peer.Connection == nil {
				go p.createOutboundConnection(peer)
			}
		}
	}

	return events
}

// isBootstrapPeer returns true if the peer is sourced from bootstrap topology.
func (p *PeerGovernor) isBootstrapPeer(peer *Peer) bool {
	return peer != nil && peer.Source == PeerSourceTopologyBootstrapPeer
}

// canPromoteBootstrapPeer returns true if bootstrap peers can be promoted.
// Bootstrap peers can only be promoted if bootstrap has not been exited.
// Must be called with p.mu held.
func (p *PeerGovernor) canPromoteBootstrapPeer() bool {
	return !p.bootstrapExited
}
