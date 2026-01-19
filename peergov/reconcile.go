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

func (p *PeerGovernor) reconcile() {
	p.mu.Lock()

	p.config.Logger.Debug("starting peer reconcile")

	// Collect events to publish after releasing the lock (avoid deadlock)
	var events []pendingEvent

	// Cleanup expired deny list entries
	p.cleanupDenyList()

	// Check if we should exit bootstrap mode
	if shouldExit, reason := p.shouldExitBootstrap(); shouldExit {
		events = append(events, p.exitBootstrapLocked(reason)...)
	}

	// Check if we need to recover bootstrap peers
	events = append(events, p.checkBootstrapRecoveryLocked()...)

	// Track changes for metrics
	var coldPromotions, warmPromotions, warmDemotions, knownRemoved, activeIncreased, activeDecreased int

	// Demotion/Promotion Logic
	for i := len(p.peers) - 1; i >= 0; i-- {
		peer := p.peers[i]
		if peer == nil {
			continue
		}
		switch peer.State {
		case PeerStateHot:
			// Demote if inactive (no connection or last activity > timeout)
			if peer.Connection == nil ||
				time.Since(peer.LastActivity) > p.config.InactivityTimeout {
				p.peers[i].State = PeerStateWarm
				warmDemotions++
				activeDecreased++
				p.config.Logger.Info(
					"demoted peer to warm due to inactivity",
					"address",
					peer.Address,
				)
				events = append(events, pendingEvent{
					PeerDemotedEventType,
					PeerStateChangeEvent{
						Address: peer.Address,
						Reason:  "inactive",
					},
				})
			}
		case PeerStateWarm:
			// Do not promote warm peers here; collect them and perform
			// score-based promotion later. This avoids unconditional
			// promotion and lets the scoring policy decide which warm
			// peers to promote when ensuring MinHotPeers.
			// Note: warm peers remain warm unless promoted in scoring block below.
		case PeerStateCold:
			// Promote to warm if connection exists
			// Skip bootstrap peers if bootstrap has been exited
			if peer.Connection != nil {
				if p.isBootstrapPeer(peer) && !p.canPromoteBootstrapPeer() {
					// Bootstrap peer with connection but bootstrap exited
					// Close the connection and keep peer cold
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
						"prevented bootstrap peer promotion after exit",
						"address", peer.Address,
					)
				} else {
					p.peers[i].State = PeerStateWarm
					coldPromotions++
					p.config.Logger.Info(
						"promoted peer to warm",
						"address",
						peer.Address,
					)
					events = append(events, pendingEvent{
						PeerPromotedEventType,
						PeerStateChangeEvent{Address: peer.Address, Reason: "connection established"},
					})
				}
			} else if peer.ReconnectCount > p.config.MaxReconnectFailures {
				knownRemoved++
				p.config.Logger.Info(
					"removing failed peer",
					"address",
					peer.Address,
					"failures",
					peer.ReconnectCount,
				)
				events = append(events, pendingEvent{
					PeerRemovedEventType,
					PeerStateChangeEvent{Address: peer.Address, Reason: "excessive failures"},
				})
				// Remove from slice (safe while iterating backwards)
				p.peers = slices.Delete(p.peers, i, i+1)
			}
		}
	}

	// Ensure minimum hot peers (simple: promote more warm if needed)
	hotCount := 0
	for _, peer := range p.peers {
		if peer != nil && peer.State == PeerStateHot {
			hotCount++
		}
	}
	if hotCount < p.config.MinHotPeers {
		// Score-based selection: collect warm peers with connections, compute scores,
		// sort by valency priority then score, and promote top N required to reach MinHotPeers.
		candidates := []*Peer{}
		for _, peer := range p.peers {
			if peer != nil && peer.State == PeerStateWarm &&
				peer.Connection != nil {
				// Skip bootstrap peers if bootstrap has been exited
				if p.isBootstrapPeer(peer) && !p.canPromoteBootstrapPeer() {
					continue
				}
				peer.UpdatePeerScore()
				candidates = append(candidates, peer)
			}
		}

		// Get group counts for valency-aware sorting
		groups := p.countPeersByGroup()

		// Sort candidates: 1) under-valency groups first, 2) by score descending
		slices.SortFunc(candidates, func(a, b *Peer) int {
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

		needed := p.config.MinHotPeers - hotCount
		promoted := 0
		for i := 0; i < len(candidates) && promoted < needed; i++ {
			peer := candidates[i]
			// Check inbound peer eligibility (score threshold and tenure)
			if !p.isInboundEligibleForHot(peer) {
				p.config.Logger.Debug(
					"skipping inbound peer for hot promotion (requirements not met)",
					"address",
					peer.Address,
					"score",
					peer.PerformanceScore,
					"first_seen",
					peer.FirstSeen,
					"tenure_required",
					p.config.InboundMinTenure,
					"score_threshold",
					p.config.InboundHotScoreThreshold,
				)
				continue
			}
			peer.State = PeerStateHot
			peer.LastActivity = time.Now()
			warmPromotions++
			activeIncreased++
			promoted++
			// Update group counts after promotion
			if gc, exists := groups[peer.GroupID]; exists {
				gc.Hot++
				gc.Warm--
			}
			p.config.Logger.Info(
				"promoted peer to hot to meet minimum (score-based)",
				"address", peer.Address,
				"score", peer.PerformanceScore,
				"group", peer.GroupID,
			)
			events = append(events, pendingEvent{
				PeerPromotedEventType,
				PeerStateChangeEvent{
					Address: peer.Address,
					Reason:  "minimum hot peers (score)",
				},
			})
			hotCount++
		}
	}

	// Enforce per-source quotas for hot peers
	// This ensures no single source dominates the hot peer slots
	events = append(events, p.enforcePerSourceQuotas()...)

	// Collect QuotaStatusEvent with current hot peer distribution
	hotByCategory := p.getHotPeersByCategory()
	totalHot := hotByCategory["topology"] + hotByCategory["gossip"] +
		hotByCategory["ledger"] + hotByCategory["other"]
	events = append(events, pendingEvent{
		QuotaStatusEventType,
		QuotaStatusEvent{
			TopologyHot: hotByCategory["topology"],
			GossipHot:   hotByCategory["gossip"],
			LedgerHot:   hotByCategory["ledger"],
			OtherHot:    hotByCategory["other"],
			TotalHot:    totalHot,
		},
	})
	// Log quota status for debugging
	p.config.Logger.Debug(
		"quota status",
		"topology_hot", hotByCategory["topology"],
		"gossip_hot", hotByCategory["gossip"],
		"ledger_hot", hotByCategory["ledger"],
		"other_hot", hotByCategory["other"],
		"total_hot", totalHot,
		"topology_quota", p.config.ActivePeersTopologyQuota,
		"gossip_quota", p.config.ActivePeersGossipQuota,
		"ledger_quota", p.config.ActivePeersLedgerQuota,
	)

	// Log valency status for topology groups
	p.logValencyStatus()

	// Enforce overall peer targets by removing excess peers
	// Priority order (highest to lowest): Topology > Gossip > Ledger > Inbound > Unknown
	events = append(events, p.enforcePeerLimits(&knownRemoved)...)

	// Collect eligible peers for peer sharing
	// Copy peer data while holding lock to avoid race conditions
	var eligiblePeersCopy []Peer
	if p.config.PeerRequestFunc != nil {
		for _, peer := range p.peers {
			if peer != nil && peer.State == PeerStateHot &&
				peer.Connection != nil && peer.Source != PeerSourceTopologyLocalRoot {
				eligiblePeersCopy = append(eligiblePeersCopy, *peer)
			}
		}
	}

	// Update metrics
	p.updatePeerMetrics()
	if p.metrics != nil {
		p.metrics.coldPeersPromotions.Add(float64(coldPromotions))
		p.metrics.warmPeersPromotions.Add(float64(warmPromotions))
		p.metrics.warmPeersDemotions.Add(float64(warmDemotions))
		p.metrics.decreasedKnownPeers.Add(float64(knownRemoved))
		p.metrics.increasedActivePeers.Add(float64(activeIncreased))
		p.metrics.decreasedActivePeers.Add(float64(activeDecreased))
	}

	p.config.Logger.Debug(
		"peer reconcile completed",
		"changes",
		coldPromotions+warmPromotions+warmDemotions+knownRemoved,
	)

	// Peer Discovery via Peer Sharing (outside lock)
	p.mu.Unlock()

	// Publish all collected events after releasing the lock (avoid deadlock)
	p.publishPendingEvents(events)

	for i := range eligiblePeersCopy {
		addrs := p.config.PeerRequestFunc(&eligiblePeersCopy[i])
		for _, addr := range addrs {
			p.AddPeer(addr, PeerSourceP2PGossip)
		}
	}

	// Discover peers from ledger (stake pool relays)
	p.discoverLedgerPeers()
}

// enforcePeerLimits removes excess peers when targets are exceeded.
// It prioritizes keeping topology peers and removes lower-priority peers first.
// Returns events that should be published after releasing the lock.
// Must be called with p.mu held.
func (p *PeerGovernor) enforcePeerLimits(removedCount *int) []pendingEvent {
	var events []pendingEvent

	// Enforce active (hot) peer target
	if p.config.TargetNumberOfActivePeers > 0 {
		events = append(events, p.enforceStateLimit(
			PeerStateHot,
			p.config.TargetNumberOfActivePeers,
			removedCount,
		)...)
	}

	// Enforce established (warm) peer target
	if p.config.TargetNumberOfEstablishedPeers > 0 {
		events = append(events, p.enforceStateLimit(
			PeerStateWarm,
			p.config.TargetNumberOfEstablishedPeers,
			removedCount,
		)...)
	}

	// Enforce known (cold) peer target
	if p.config.TargetNumberOfKnownPeers > 0 {
		events = append(events, p.enforceStateLimit(
			PeerStateCold,
			p.config.TargetNumberOfKnownPeers,
			removedCount,
		)...)
	}

	return events
}

// enforceStateLimit removes excess peers in a given state.
// Returns events that should be published after releasing the lock.
// Must be called with p.mu held.
func (p *PeerGovernor) enforceStateLimit(
	state PeerState,
	limit int,
	removedCount *int,
) []pendingEvent {
	var events []pendingEvent

	// Collect peers in this state
	var peersInState []*Peer
	for _, peer := range p.peers {
		if peer != nil && peer.State == state {
			peersInState = append(peersInState, peer)
		}
	}

	// Check if we're over the limit
	excess := len(peersInState) - limit
	if excess <= 0 {
		return events
	}

	// Sort peers by removal priority (lowest priority first)
	// Priority order (highest to lowest):
	// - Topology peers (LocalRoot, PublicRoot, Bootstrap) - never remove
	// - Gossip peers
	// - Ledger peers
	// - Inbound peers
	// - Unknown peers
	slices.SortFunc(peersInState, func(a, b *Peer) int {
		// First compare by source priority (lower priority = remove first)
		aPriority := p.peerSourcePriority(a.Source)
		bPriority := p.peerSourcePriority(b.Source)
		if aPriority != bPriority {
			return cmp.Compare(aPriority, bPriority)
		}
		// Same priority: lower score = remove first
		return cmp.Compare(a.PerformanceScore, b.PerformanceScore)
	})

	// Remove excess peers (they're sorted lowest priority first)
	removed := 0
	for i := 0; i < len(peersInState) && removed < excess; i++ {
		peer := peersInState[i]

		// Never remove topology peers
		if p.isTopologyPeer(peer.Source) {
			continue
		}

		// Find and remove from main peer list
		for j := len(p.peers) - 1; j >= 0; j-- {
			if p.peers[j] == peer {
				p.config.Logger.Info(
					"removing peer due to limit exceeded",
					"address", peer.Address,
					"state", state,
					"source", peer.Source,
					"limit", limit,
				)
				// Close connection if peer has one (for warm/hot peers)
				if peer.Connection != nil && p.config.ConnManager != nil {
					conn := p.config.ConnManager.GetConnectionById(
						peer.Connection.Id,
					)
					if conn != nil {
						conn.Close()
					}
				}
				events = append(events, pendingEvent{
					PeerRemovedEventType,
					PeerStateChangeEvent{
						Address: peer.Address,
						Reason:  "limit exceeded",
					},
				})
				p.peers = slices.Delete(p.peers, j, j+1)
				removed++
				*removedCount++
				break
			}
		}
	}

	if removed > 0 {
		p.config.Logger.Debug(
			"enforced peer limit",
			"state", state,
			"limit", limit,
			"removed", removed,
		)
	}

	return events
}
