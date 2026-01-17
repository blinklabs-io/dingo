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
	"cmp"
	"slices"
	"time"
)

// PeerCounts holds peer counts by state for a given source or category.
type PeerCounts struct {
	Cold int
	Warm int
	Hot  int
}

// GroupCounts holds peer counts by state for a specific topology group.
type GroupCounts struct {
	GroupID     string
	Valency     uint // Target hot connections
	WarmValency uint // Target warm connections
	Cold        int
	Warm        int
	Hot         int
}

// peerSourcePriority returns a priority value for a peer source.
// Higher values = higher priority (less likely to be removed).
func (p *PeerGovernor) peerSourcePriority(source PeerSource) int {
	switch source {
	case PeerSourceTopologyLocalRoot,
		PeerSourceTopologyPublicRoot,
		PeerSourceTopologyBootstrapPeer:
		return peerPriorityTopology
	case PeerSourceP2PGossip:
		return peerPriorityGossip
	case PeerSourceP2PLedger:
		return peerPriorityLedger
	case PeerSourceInboundConn:
		return peerPriorityInbound
	default:
		return peerPriorityUnknown
	}
}

// isTopologyPeer returns true if the peer source is a topology source.
func (p *PeerGovernor) isTopologyPeer(source PeerSource) bool {
	switch source {
	case PeerSourceTopologyLocalRoot,
		PeerSourceTopologyPublicRoot,
		PeerSourceTopologyBootstrapPeer:
		return true
	default:
		return false
	}
}

// countPeersBySourceAndState returns a map of peer source to peer counts by state.
// This method must be called with p.mu held.
// nolint:unused // Exported for tests and future use in Phase 3+ churn timers
func (p *PeerGovernor) countPeersBySourceAndState() map[PeerSource]PeerCounts {
	counts := make(map[PeerSource]PeerCounts)
	for _, peer := range p.peers {
		if peer == nil {
			continue
		}
		c := counts[peer.Source]
		switch peer.State {
		case PeerStateCold:
			c.Cold++
		case PeerStateWarm:
			c.Warm++
		case PeerStateHot:
			c.Hot++
		}
		counts[peer.Source] = c
	}
	return counts
}

// getSourceCategory maps a peer source to its quota category.
// Returns "topology", "gossip", "ledger", or "other".
func (p *PeerGovernor) getSourceCategory(source PeerSource) string {
	switch source {
	case PeerSourceTopologyLocalRoot,
		PeerSourceTopologyPublicRoot,
		PeerSourceTopologyBootstrapPeer:
		return "topology"
	case PeerSourceP2PGossip:
		return "gossip"
	case PeerSourceP2PLedger:
		return "ledger"
	default:
		return "other"
	}
}

// getHotPeersByCategory returns the count of hot peers in each quota category.
// This method must be called with p.mu held.
func (p *PeerGovernor) getHotPeersByCategory() map[string]int {
	counts := make(map[string]int)
	for _, peer := range p.peers {
		if peer == nil || peer.State != PeerStateHot {
			continue
		}
		category := p.getSourceCategory(peer.Source)
		counts[category]++
	}
	return counts
}

// enforcePerSourceQuotas enforces the per-source quotas for hot peers.
// When a category exceeds its quota, lowest-scoring peers are demoted.
// Returns events that should be published after releasing the lock.
// This method must be called with p.mu held.
func (p *PeerGovernor) enforcePerSourceQuotas() []pendingEvent {
	events := make(
		[]pendingEvent,
		0,
		8,
	) // Pre-allocate for typical quota enforcement
	hotByCategory := p.getHotPeersByCategory()

	// Check each quota category
	events = append(events, p.enforceSourceQuota(
		"gossip",
		p.config.ActivePeersGossipQuota,
		hotByCategory["gossip"],
	)...)
	events = append(events, p.enforceSourceQuota(
		"ledger",
		p.config.ActivePeersLedgerQuota,
		hotByCategory["ledger"],
	)...)

	// Enforce topology quota with special rules (demote public roots first, never local roots)
	events = append(
		events,
		p.enforceTopologyQuota(hotByCategory["topology"])...)

	// Enforce per-group valency for topology peers
	events = append(events, p.enforceGroupValency()...)

	return events
}

// enforceTopologyQuota enforces the topology quota for hot peers.
// Unlike other sources, topology peers have special demotion rules:
// - Public roots are demoted to warm (keep connection alive)
// - Local roots are NEVER demoted (highest priority)
// This method must be called with p.mu held.
func (p *PeerGovernor) enforceTopologyQuota(currentCount int) []pendingEvent {
	var events []pendingEvent
	quota := p.config.ActivePeersTopologyQuota
	if quota <= 0 || currentCount <= quota {
		return events
	}

	excess := currentCount - quota
	p.config.Logger.Debug(
		"enforcing topology quota",
		"quota", quota,
		"current", currentCount,
		"excess", excess,
	)

	// Collect hot topology peers, but only public roots (local roots are never demoted)
	candidates := p.filterPeers(func(peer *Peer) bool {
		return peer.State == PeerStateHot &&
			peer.Source == PeerSourceTopologyPublicRoot
	})

	if len(candidates) == 0 {
		// Only local roots are hot, and we don't demote those
		return events
	}

	// Sort by score ascending (lowest score = demote first)
	slices.SortFunc(candidates, func(a, b *Peer) int {
		return cmp.Compare(a.PerformanceScore, b.PerformanceScore)
	})

	// Demote the lowest-scoring public roots to warm (not cold, to preserve connection)
	demoted := 0
	for i := 0; i < len(candidates) && demoted < excess; i++ {
		peer := candidates[i]
		peer.State = PeerStateWarm // Demote to warm, not cold
		demoted++
		p.config.Logger.Info(
			"demoted public root to warm due to topology quota",
			"address", peer.Address,
			"score", peer.PerformanceScore,
			"group", peer.GroupID,
		)
		events = append(events, pendingEvent{
			PeerDemotedEventType,
			PeerStateChangeEvent{
				Address: peer.Address,
				Reason:  "topology quota exceeded",
			},
		})
	}
	return events
}

// logValencyStatus logs the current valency status for all topology groups.
// This method must be called with p.mu held.
func (p *PeerGovernor) logValencyStatus() {
	groups := p.countPeersByGroup()
	if len(groups) == 0 {
		return
	}

	for groupID, gc := range groups {
		status := "at-target"
		if gc.Valency > 0 {
			// Hot counts are always non-negative
			hotCount := uint(gc.Hot) //nolint:gosec // Hot counts are always >= 0
			if hotCount < gc.Valency {
				status = "under-valency"
			} else if hotCount > gc.Valency {
				status = "over-valency"
			}
		}

		p.config.Logger.Debug(
			"topology group valency status",
			"group", groupID,
			"hot", gc.Hot,
			"warm", gc.Warm,
			"cold", gc.Cold,
			"valency", gc.Valency,
			"warmValency", gc.WarmValency,
			"status", status,
		)
	}
}

// enforceGroupValency ensures each topology group doesn't exceed its valency.
// Groups with valency > 0 will have excess hot peers demoted to warm.
// This method must be called with p.mu held.
func (p *PeerGovernor) enforceGroupValency() []pendingEvent {
	var events []pendingEvent
	groups := p.countPeersByGroup()

	for groupID, gc := range groups {
		// Hot counts are always non-negative, Valency is uint
		hotCount := uint(gc.Hot) //nolint:gosec // Hot counts are always >= 0
		if gc.Valency == 0 || hotCount <= gc.Valency {
			continue // No valency limit or under limit
		}

		excess := gc.Hot - int(gc.Valency) //nolint:gosec // Valency fits in int
		p.config.Logger.Debug(
			"enforcing group valency",
			"group", groupID,
			"valency", gc.Valency,
			"hot", gc.Hot,
			"excess", excess,
		)

		// Collect hot peers in this group, excluding local roots
		// (local roots are never demoted to preserve the topology invariant)
		candidates := p.filterPeers(func(peer *Peer) bool {
			return peer.State == PeerStateHot &&
				peer.GroupID == groupID &&
				peer.Source != PeerSourceTopologyLocalRoot
		})

		// Sort by score ascending (lowest score = demote first)
		slices.SortFunc(candidates, func(a, b *Peer) int {
			return cmp.Compare(a.PerformanceScore, b.PerformanceScore)
		})

		// Demote excess peers to warm
		demoted := 0
		for i := 0; i < len(candidates) && demoted < excess; i++ {
			peer := candidates[i]
			peer.State = PeerStateWarm
			demoted++
			p.config.Logger.Info(
				"demoted peer to warm due to group valency",
				"address", peer.Address,
				"group", groupID,
				"valency", gc.Valency,
				"score", peer.PerformanceScore,
			)
			events = append(events, pendingEvent{
				PeerDemotedEventType,
				PeerStateChangeEvent{
					Address: peer.Address,
					Reason:  "group valency exceeded",
				},
			})
		}
	}
	return events
}

// enforceSourceQuota demotes excess hot peers from a specific category.
// This method must be called with p.mu held.
func (p *PeerGovernor) enforceSourceQuota(
	category string,
	quota int,
	currentCount int,
) []pendingEvent {
	var events []pendingEvent
	if quota <= 0 || currentCount <= quota {
		return events
	}

	excess := currentCount - quota
	p.config.Logger.Debug(
		"enforcing source quota",
		"category", category,
		"quota", quota,
		"current", currentCount,
		"excess", excess,
	)

	// Collect hot peers in this category
	candidates := p.filterPeers(func(peer *Peer) bool {
		return peer.State == PeerStateHot &&
			p.getSourceCategory(peer.Source) == category
	})

	// Sort by score ascending (lowest score = demote first)
	slices.SortFunc(candidates, func(a, b *Peer) int {
		return cmp.Compare(a.PerformanceScore, b.PerformanceScore)
	})

	// Demote the lowest-scoring peers
	demoted := 0
	for i := 0; i < len(candidates) && demoted < excess; i++ {
		peer := candidates[i]
		peer.State = PeerStateWarm
		demoted++
		p.config.Logger.Info(
			"demoted peer to warm due to source quota",
			"address", peer.Address,
			"category", category,
			"score", peer.PerformanceScore,
		)
		events = append(events, pendingEvent{
			PeerDemotedEventType,
			PeerStateChangeEvent{
				Address: peer.Address,
				Reason:  "source quota exceeded",
			},
		})
	}
	return events
}

// countPeersByGroup returns counts of peers in each state grouped by GroupID.
// Only topology-sourced peers (local roots and public roots) have GroupIDs.
// This method must be called with p.mu held.
func (p *PeerGovernor) countPeersByGroup() map[string]*GroupCounts {
	groups := make(map[string]*GroupCounts)
	for _, peer := range p.peers {
		if peer == nil || peer.GroupID == "" {
			continue
		}
		gc, exists := groups[peer.GroupID]
		if !exists {
			gc = &GroupCounts{
				GroupID:     peer.GroupID,
				Valency:     peer.Valency,
				WarmValency: peer.WarmValency,
			}
			groups[peer.GroupID] = gc
		}
		switch peer.State {
		case PeerStateCold:
			gc.Cold++
		case PeerStateWarm:
			gc.Warm++
		case PeerStateHot:
			gc.Hot++
		}
	}
	return groups
}

// isGroupUnderHotValency returns true if the peer's group has fewer hot peers
// than its target valency. Returns false if peer has no GroupID.
// This method must be called with p.mu held.
func (p *PeerGovernor) isGroupUnderHotValency(
	peer *Peer,
	groups map[string]*GroupCounts,
) bool {
	if peer == nil || peer.GroupID == "" {
		return false
	}
	gc, exists := groups[peer.GroupID]
	if !exists {
		return false
	}
	// Hot counts are always non-negative
	return gc.Valency > 0 &&
		uint(gc.Hot) < gc.Valency //nolint:gosec // Hot counts are always >= 0
}

// isGroupOverHotValency returns true if the peer's group has more hot peers
// than its target valency. Returns false if peer has no GroupID or valency is 0.
// This method must be called with p.mu held.
func (p *PeerGovernor) isGroupOverHotValency(
	peer *Peer,
	groups map[string]*GroupCounts,
) bool {
	if peer == nil || peer.GroupID == "" {
		return false
	}
	gc, exists := groups[peer.GroupID]
	if !exists {
		return false
	}
	// Hot counts are always non-negative
	return gc.Valency > 0 &&
		uint(gc.Hot) > gc.Valency //nolint:gosec // Hot counts are always >= 0
}

// isInboundEligibleForHot checks if an inbound peer meets the requirements for
// hot promotion. Inbound peers have stricter requirements than other peers:
// - Must have score >= InboundHotScoreThreshold (default 0.6, higher than normal 0.3)
// - Must have been seen for at least InboundMinTenure (default 10 minutes)
// Returns true if the peer is eligible for hot promotion, false otherwise.
// For non-inbound peers, always returns true (they use the normal score threshold).
// This method must be called with p.mu held.
func (p *PeerGovernor) isInboundEligibleForHot(peer *Peer) bool {
	if peer == nil {
		return false
	}
	// Non-inbound peers always pass this check (they use normal thresholds)
	if peer.Source != PeerSourceInboundConn {
		return true
	}
	// Inbound peers must meet higher score threshold
	if peer.PerformanceScore < p.config.InboundHotScoreThreshold {
		return false
	}
	// Inbound peers must have sufficient tenure
	if peer.FirstSeen.IsZero() {
		return false
	}
	tenure := time.Since(peer.FirstSeen)
	return tenure >= p.config.InboundMinTenure
}

// redistributeUnusedSlots redistributes unused quota slots to other categories.
// If a category has fewer peers than its quota, the unused slots can be used
// by other categories (preference: gossip > ledger > topology).
// Returns the number of extra slots available for each category.
// This method must be called with p.mu held.
// nolint:unused // Exported for tests and future use in Phase 3+ promotion logic
func (p *PeerGovernor) redistributeUnusedSlots() map[string]int {
	hotByCategory := p.getHotPeersByCategory()

	// Calculate unused slots per category
	topologyUnused := max(
		0,
		p.config.ActivePeersTopologyQuota-hotByCategory["topology"],
	)
	gossipUnused := max(
		0,
		p.config.ActivePeersGossipQuota-hotByCategory["gossip"],
	)
	ledgerUnused := max(
		0,
		p.config.ActivePeersLedgerQuota-hotByCategory["ledger"],
	)

	totalUnused := topologyUnused + gossipUnused + ledgerUnused

	// Redistribute unused slots
	// Priority: gossip first, then ledger, then topology
	extraSlots := make(map[string]int)
	remaining := totalUnused

	// Gossip can borrow unused slots from topology and ledger
	gossipCanBorrow := topologyUnused + ledgerUnused
	extraSlots["gossip"] = min(gossipCanBorrow, remaining)
	remaining -= extraSlots["gossip"]

	// Ledger can borrow unused slots from topology
	ledgerCanBorrow := topologyUnused
	extraSlots["ledger"] = min(ledgerCanBorrow, remaining)
	_ = remaining // Suppress ineffassign - remaining is used for future topology borrowing

	// Topology generally doesn't borrow from others

	return extraSlots
}

// demotionTarget returns the target state when demoting a peer from hot,
// and whether demotion is allowed for this peer source.
// Different sources have different demotion behaviors:
// - TopologyLocalRoot: Never demote (canDemote=false)
// - TopologyPublicRoot: Demote to warm (keep connection alive)
// - TopologyBootstrapPeer: Demote to cold (close connection, can be fully churned)
// - Gossip/Ledger/Inbound: Demote to cold (close connection)
func (p *PeerGovernor) demotionTarget(source PeerSource) (target PeerState, canDemote bool) {
	switch source {
	case PeerSourceTopologyLocalRoot:
		// Local roots are never churned - they represent explicit operator configuration
		return PeerStateCold, false
	case PeerSourceTopologyPublicRoot:
		// Public roots demote to warm (keep connection for later promotion)
		return PeerStateWarm, true
	case PeerSourceTopologyBootstrapPeer:
		// Bootstrap peers demote to cold (can be fully churned out once sync is established)
		return PeerStateCold, true
	default:
		// Gossip, ledger, inbound, and unknown peers demote to cold (close connection)
		return PeerStateCold, true
	}
}
