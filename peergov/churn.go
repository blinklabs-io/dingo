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

// gossipChurn performs aggressive churn on gossip and ledger peers.
// It demotes the lowest-scoring hot peers to cold and promotes warm peers.
func (p *PeerGovernor) gossipChurn() {
	var events []pendingEvent

	p.mu.Lock()

	// Collect hot non-root peers (gossip, ledger)
	hotNonRoot := p.filterPeers(func(peer *Peer) bool {
		return peer.State == PeerStateHot &&
			(peer.Source == PeerSourceP2PGossip || peer.Source == PeerSourceP2PLedger)
	})
	if len(hotNonRoot) == 0 {
		p.mu.Unlock()
		return
	}

	// Count peers below minimum score threshold (always churn these)
	belowThreshold := 0
	for _, peer := range hotNonRoot {
		if peer.PerformanceScore < p.config.MinScoreThreshold {
			belowThreshold++
		}
	}

	// Calculate number to churn (always at least 1, and always include below-threshold peers)
	// Clamp to available peers to prevent overflow with misconfigured percentages
	churnCount := min(
		len(hotNonRoot),
		max(
			1,
			int(float64(len(hotNonRoot))*p.config.GossipChurnPercent),
			belowThreshold,
		),
	)

	// Sort by score ascending (lowest first)
	slices.SortFunc(hotNonRoot, func(a, b *Peer) int {
		return cmp.Compare(a.PerformanceScore, b.PerformanceScore)
	})

	// Demote the lowest-scoring peers
	demoted := 0
	for i := 0; i < len(hotNonRoot) && demoted < churnCount; i++ {
		peer := hotNonRoot[i]
		targetState, canDemote := p.demotionTarget(peer.Source)
		if !canDemote {
			continue // Never demote this peer (e.g., local roots)
		}
		peer.State = targetState

		// Close connection when demoting to cold
		if targetState == PeerStateCold && peer.Connection != nil {
			if p.config.ConnManager != nil {
				conn := p.config.ConnManager.GetConnectionById(
					peer.Connection.Id,
				)
				if conn != nil {
					conn.Close()
				}
			}
			peer.Connection = nil
			p.config.Logger.Debug(
				"gossip churn: closed connection for demoted peer",
				"address", peer.Address,
			)
		}

		demoted++
		p.config.Logger.Debug(
			"gossip churn: demoted peer",
			"address", peer.Address,
			"source", peer.Source.String(),
			"old_state", "hot",
			"new_state", targetState.String(),
			"score", peer.PerformanceScore,
			"reason", "gossip churn",
		)
		if p.metrics != nil {
			p.metrics.activeNonRootPeersDemotions.Inc()
			p.metrics.churnDemotionsBySource.WithLabelValues(
				peer.Source.String(),
			).Inc()
		}
		// Collect events to publish after releasing lock
		events = append(events, pendingEvent{
			PeerDemotedEventType,
			PeerStateChangeEvent{Address: peer.Address, Reason: "gossip churn"},
		})
		events = append(events, pendingEvent{
			PeerChurnEventType,
			PeerChurnEvent{
				Address:  peer.Address,
				Source:   peer.Source.String(),
				OldState: "hot",
				NewState: targetState.String(),
				Score:    peer.PerformanceScore,
				Reason:   "gossip churn",
			},
		})
	}

	// Now promote warm peers to fill slots
	promotionEvents := p.promoteWarmNonRootPeersLocked(demoted)
	events = append(events, promotionEvents...)
	p.updatePeerMetrics()
	p.mu.Unlock()

	// Publish all events outside of lock to avoid deadlock
	p.publishPendingEvents(events)
}

// publicRootChurn performs conservative churn on public root peers.
// It demotes the lowest-scoring hot public roots to warm and promotes warm ones.
func (p *PeerGovernor) publicRootChurn() {
	var events []pendingEvent

	p.mu.Lock()

	// Collect warm public roots BEFORE demotion (for later promotion)
	// This prevents promoting peers we just demoted
	warmPublicRoots := p.filterPeers(func(peer *Peer) bool {
		return peer.State == PeerStateWarm &&
			peer.Source == PeerSourceTopologyPublicRoot
	})

	// Collect hot public root peers
	hotPublicRoots := p.filterPeers(func(peer *Peer) bool {
		return peer.State == PeerStateHot &&
			peer.Source == PeerSourceTopologyPublicRoot
	})
	if len(hotPublicRoots) == 0 {
		p.mu.Unlock()
		return
	}

	// Count peers below minimum score threshold (always churn these)
	belowThreshold := 0
	for _, peer := range hotPublicRoots {
		if peer.PerformanceScore < p.config.MinScoreThreshold {
			belowThreshold++
		}
	}

	// Calculate number to churn (always at least 1, and always include below-threshold peers)
	// Clamp to available peers to prevent overflow with misconfigured percentages
	churnCount := min(
		len(hotPublicRoots),
		max(
			1,
			int(float64(len(hotPublicRoots))*p.config.PublicRootChurnPercent),
			belowThreshold,
		),
	)

	// Get group counts for valency-aware demotion
	groups := p.countPeersByGroup()

	// Sort by: 1) over-valency groups first, 2) score ascending (lowest first)
	slices.SortFunc(hotPublicRoots, func(a, b *Peer) int {
		aOver := p.isGroupOverHotValency(a, groups)
		bOver := p.isGroupOverHotValency(b, groups)
		// Over-valency peers should be demoted first
		if aOver && !bOver {
			return -1
		}
		if !aOver && bOver {
			return 1
		}
		// Same valency status: sort by score ascending (lowest first)
		return cmp.Compare(a.PerformanceScore, b.PerformanceScore)
	})

	// Demote the lowest-scoring peers to warm (prefer over-valency groups)
	demoted := 0
	for i := 0; i < len(hotPublicRoots) && demoted < churnCount; i++ {
		peer := hotPublicRoots[i]
		peer.State = PeerStateWarm // Public roots demote to warm, not cold
		demoted++
		// Update group counts after demotion
		if gc, exists := groups[peer.GroupID]; exists {
			gc.Hot--
			gc.Warm++
		}
		p.config.Logger.Debug(
			"public root churn: demoted peer to warm",
			"address", peer.Address,
			"source", peer.Source.String(),
			"old_state", "hot",
			"new_state", "warm",
			"score", peer.PerformanceScore,
			"group", peer.GroupID,
			"reason", "public root churn",
		)
		if p.metrics != nil {
			p.metrics.churnDemotionsBySource.WithLabelValues(
				peer.Source.String(),
			).Inc()
		}
		// Collect events to publish after releasing lock
		events = append(events, pendingEvent{
			PeerDemotedEventType,
			PeerStateChangeEvent{
				Address: peer.Address,
				Reason:  "public root churn",
			},
		})
		events = append(events, pendingEvent{
			PeerChurnEventType,
			PeerChurnEvent{
				Address:  peer.Address,
				Source:   peer.Source.String(),
				OldState: "hot",
				NewState: "warm",
				Score:    peer.PerformanceScore,
				Reason:   "public root churn",
			},
		})
	}

	// Promote from pre-existing warm public roots to fill slots
	promotionEvents := p.promoteFromWarmPublicRootsLocked(
		warmPublicRoots,
		demoted,
	)
	events = append(events, promotionEvents...)
	p.updatePeerMetrics()
	p.mu.Unlock()

	// Publish all events outside of lock to avoid deadlock
	p.publishPendingEvents(events)
}

// promoteWarmNonRootPeers promotes the highest-scoring warm non-root peers to hot.
//
//nolint:unused // Used by tests
func (p *PeerGovernor) promoteWarmNonRootPeers(count int) {
	p.mu.Lock()
	events := p.promoteWarmNonRootPeersLocked(count)
	p.mu.Unlock()

	// Publish events outside of lock to avoid deadlock
	p.publishPendingEvents(events)
}

// promoteWarmNonRootPeersLocked promotes the highest-scoring warm non-root peers to hot
// and returns pending events instead of publishing them directly.
// Must be called with p.mu held.
func (p *PeerGovernor) promoteWarmNonRootPeersLocked(count int) []pendingEvent {
	var events []pendingEvent
	if count <= 0 {
		return events
	}

	// Collect warm non-root peers
	warmNonRoot := p.filterPeers(func(peer *Peer) bool {
		return peer.State == PeerStateWarm &&
			(peer.Source == PeerSourceP2PGossip || peer.Source == PeerSourceP2PLedger)
	})
	if len(warmNonRoot) == 0 {
		return events
	}

	// Sort by score descending (highest first)
	slices.SortFunc(warmNonRoot, func(a, b *Peer) int {
		return cmp.Compare(b.PerformanceScore, a.PerformanceScore)
	})

	// Promote the highest-scoring peers
	promoted := 0
	for i := 0; i < len(warmNonRoot) && promoted < count; i++ {
		peer := warmNonRoot[i]
		// Only promote if connection exists and score is above threshold
		if peer.Connection == nil {
			continue // Skip peers without active connections
		}
		if peer.PerformanceScore >= p.config.MinScoreThreshold {
			peer.State = PeerStateHot
			peer.LastActivity = time.Now()
			promoted++
			p.config.Logger.Debug(
				"gossip churn: promoted peer to hot",
				"address", peer.Address,
				"source", peer.Source.String(),
				"old_state", "warm",
				"new_state", "hot",
				"score", peer.PerformanceScore,
				"reason", "gossip churn promotion",
			)
			if p.metrics != nil {
				p.metrics.warmNonRootPeersPromotions.Inc()
				p.metrics.churnPromotionsBySource.WithLabelValues(
					peer.Source.String(),
				).Inc()
			}
			events = append(events, pendingEvent{
				PeerPromotedEventType,
				PeerStateChangeEvent{
					Address: peer.Address,
					Reason:  "gossip churn promotion",
				},
			})
			events = append(events, pendingEvent{
				PeerChurnEventType,
				PeerChurnEvent{
					Address:  peer.Address,
					Source:   peer.Source.String(),
					OldState: "warm",
					NewState: "hot",
					Score:    peer.PerformanceScore,
					Reason:   "gossip churn promotion",
				},
			})
		}
	}
	return events
}

// promoteFromWarmPublicRoots promotes from a pre-collected list of warm public root peers.
// This is used by publicRootChurn to avoid promoting peers that were just demoted.
// Uses valency-aware promotion: prefers peers from groups under their valency target.
//
//nolint:unused // Used by tests
func (p *PeerGovernor) promoteFromWarmPublicRoots(
	warmPublicRoots []*Peer,
	count int,
) {
	p.mu.Lock()
	events := p.promoteFromWarmPublicRootsLocked(warmPublicRoots, count)
	p.mu.Unlock()

	// Publish events outside of lock to avoid deadlock
	p.publishPendingEvents(events)
}

// promoteFromWarmPublicRootsLocked promotes from a pre-collected list of warm public root peers
// and returns pending events instead of publishing them directly.
// This is used by publicRootChurn to avoid promoting peers that were just demoted.
// Uses valency-aware promotion: prefers peers from groups under their valency target.
// Must be called with p.mu held.
func (p *PeerGovernor) promoteFromWarmPublicRootsLocked(
	warmPublicRoots []*Peer,
	count int,
) []pendingEvent {
	var events []pendingEvent
	if count <= 0 || len(warmPublicRoots) == 0 {
		return events
	}

	// Get current group counts for valency-aware sorting
	groups := p.countPeersByGroup()

	// Sort by: 1) under-valency groups first, 2) score descending
	slices.SortFunc(warmPublicRoots, func(a, b *Peer) int {
		aUnder := p.isGroupUnderHotValency(a, groups)
		bUnder := p.isGroupUnderHotValency(b, groups)
		// Under-valency peers come first
		if aUnder && !bUnder {
			return -1
		}
		if !aUnder && bUnder {
			return 1
		}
		// Same valency status: sort by score descending
		return cmp.Compare(b.PerformanceScore, a.PerformanceScore)
	})

	// Promote the highest-priority peers
	promoted := 0
	for i := 0; i < len(warmPublicRoots) && promoted < count; i++ {
		peer := warmPublicRoots[i]
		// Only promote if connection exists and score is above threshold
		if peer.Connection == nil {
			continue // Skip peers without active connections
		}
		if peer.PerformanceScore >= p.config.MinScoreThreshold {
			peer.State = PeerStateHot
			peer.LastActivity = time.Now()
			promoted++
			// Update group counts after promotion
			if gc, exists := groups[peer.GroupID]; exists {
				gc.Hot++
				gc.Warm--
			}
			p.config.Logger.Debug(
				"public root churn: promoted peer to hot",
				"address", peer.Address,
				"source", peer.Source.String(),
				"old_state", "warm",
				"new_state", "hot",
				"score", peer.PerformanceScore,
				"group", peer.GroupID,
				"reason", "public root churn promotion",
			)
			if p.metrics != nil {
				p.metrics.churnPromotionsBySource.WithLabelValues(
					peer.Source.String(),
				).Inc()
			}
			events = append(events, pendingEvent{
				PeerPromotedEventType,
				PeerStateChangeEvent{
					Address: peer.Address,
					Reason:  "public root churn promotion",
				},
			})
			events = append(events, pendingEvent{
				PeerChurnEventType,
				PeerChurnEvent{
					Address:  peer.Address,
					Source:   peer.Source.String(),
					OldState: "warm",
					NewState: "hot",
					Score:    peer.PerformanceScore,
					Reason:   "public root churn promotion",
				},
			})
		}
	}
	return events
}
