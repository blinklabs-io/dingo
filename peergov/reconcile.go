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
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/blinklabs-io/dingo/connmanager"
)

func (p *PeerGovernor) reconcile(ctx context.Context) {
	p.mu.Lock()
	now := time.Now()
	debugEnabled := p.config.Logger.Enabled(
		ctx,
		slog.LevelDebug,
	)

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
			// Demote on transport loss for all peers. For connected local roots,
			// do not age them out just because they are quiet: they are
			// explicitly configured peers and should only leave hot when the
			// connection is actually bad or gone.
			demoteForInactivity := !peer.hasClientConnection() ||
				(peer.Source != PeerSourceTopologyLocalRoot &&
					now.Sub(peer.LastActivity) > p.config.InactivityTimeout)
			if demoteForInactivity {
				p.peers[i].State = PeerStateWarm
				warmDemotions++
				activeDecreased++
				p.config.Logger.Debug(
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
			// peers to promote when ensuring the promotion target.
			// Note: warm peers remain warm unless promoted in scoring block below.
		case PeerStateCold:
			// Promote to warm if connection exists
			// Skip bootstrap peers if bootstrap has been exited
			if peer.Connection != nil {
				oldSource := peer.Source
				oldConn := clonePeerConnection(peer.Connection)
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
					events = p.appendChainSelectionEventsLocked(
						events,
						p.bootstrapExited,
						oldSource,
						oldConn,
						peer,
					)
				} else {
					p.peers[i].State = PeerStateWarm
					coldPromotions++
					p.config.Logger.Debug(
						"promoted peer to warm",
						"address",
						peer.Address,
					)
					events = append(events, pendingEvent{
						PeerPromotedEventType,
						PeerStateChangeEvent{Address: peer.Address, Reason: "connection established"},
					})
				}
			} else if peer.ReconnectCount >
				p.config.MaxReconnectFailureThreshold {
				if p.isTopologyPeer(peer.Source) {
					continue
				}
				p.denyList[peer.NormalizedAddress] = now.
					Add(p.config.DenyDuration)
				knownRemoved++
				p.config.Logger.Debug(
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
	// Trigger promotion when hot peers drop below the low-water mark
	// (MinHotPeers), then refill up to TargetNumberOfActivePeers.
	// When TargetNumberOfActivePeers is 0 (unlimited), fall back to
	// MinHotPeers as the refill goal.
	if hotCount < p.config.MinHotPeers {
		refillTarget := p.config.TargetNumberOfActivePeers
		if refillTarget <= 0 {
			refillTarget = p.config.MinHotPeers
		}
		type promotionCandidate struct {
			peer           *Peer
			underValency   bool
			historical     bool
			diversityGroup string
		}
		candidates := make([]promotionCandidate, 0, len(p.peers))
		for _, peer := range p.peers {
			if peer != nil && peer.State == PeerStateWarm &&
				peer.hasClientConnection() {
				// Skip bootstrap peers if bootstrap has been exited
				if p.isBootstrapPeer(peer) && !p.canPromoteBootstrapPeer() {
					continue
				}
				peer.UpdatePeerScore()
				candidates = append(candidates, promotionCandidate{
					peer:           peer,
					historical:     p.isBootstrapPromotionHistoricalPeer(peer),
					diversityGroup: p.peerDiversityGroup(peer),
				})
			}
		}

		// Get group counts for valency-aware sorting
		groups := p.countPeersByGroup()
		for i := range candidates {
			candidates[i].underValency = p.isGroupUnderHotValency(
				candidates[i].peer,
				groups,
			)
		}

		bootstrapPromotion := !p.bootstrapExited &&
			p.bootstrapPromotionEnabled()
		selectedGroups := make(map[string]struct{})
		if bootstrapPromotion {
			for _, peer := range p.peers {
				if peer == nil || peer.State != PeerStateHot {
					continue
				}
				if diversityGroup := p.peerDiversityGroup(peer); diversityGroup != "" {
					selectedGroups[diversityGroup] = struct{}{}
				}
			}
		}

		rankCandidates := func(a, b promotionCandidate) int {
			if bootstrapPromotion {
				if a.historical != b.historical {
					if a.historical {
						return -1
					}
					return 1
				}
				if len(selectedGroups) < p.bootstrapPromotionMinDiversityGroups() {
					_, aSeen := selectedGroups[a.diversityGroup]
					_, bSeen := selectedGroups[b.diversityGroup]
					aNew := a.diversityGroup != "" && !aSeen
					bNew := b.diversityGroup != "" && !bSeen
					if aNew != bNew {
						if aNew {
							return -1
						}
						return 1
					}
				}
			}
			if a.underValency != b.underValency {
				if a.underValency {
					return -1
				}
				return 1
			}
			// Prefer higher-priority sources when priorities differ; when two
			// sources share the same priority (e.g. two topology kinds), fall
			// through to score tie-breaking instead of returning 0 early.
			if c := cmp.Compare(
				p.peerSourcePriority(b.peer.Source),
				p.peerSourcePriority(a.peer.Source),
			); c != 0 {
				return c
			}
			if a.peer.PerformanceScore != b.peer.PerformanceScore {
				return cmp.Compare(
					b.peer.PerformanceScore,
					a.peer.PerformanceScore,
				)
			}
			return cmp.Compare(a.peer.Address, b.peer.Address)
		}

		// Initial sort
		slices.SortFunc(candidates, rankCandidates)

		needed := refillTarget - hotCount
		promoted := 0
		// Count actual hot inbound peers (not censusInboundCounts.Hot, which
		// excludes peers inside InboundProvisionalWindow) so quota cannot be
		// exceeded when tenure is shorter than that window.
		inboundHotHeld := 0
		for _, existing := range p.peers {
			if existing != nil &&
				existing.Source == PeerSourceInboundConn &&
				existing.State == PeerStateHot {
				inboundHotHeld++
			}
		}
		for i := 0; i < len(candidates) && promoted < needed; i++ {
			peer := candidates[i].peer
			if peer.Source == PeerSourceInboundConn &&
				inboundHotHeld >= p.config.InboundHotQuota {
				continue
			}
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
			peer.LastActivity = now
			warmPromotions++
			activeIncreased++
			promoted++
			if peer.Source == PeerSourceInboundConn {
				inboundHotHeld++
			}
			if bootstrapPromotion && candidates[i].diversityGroup != "" {
				selectedGroups[candidates[i].diversityGroup] = struct{}{}
			}
			// Update group counts after promotion
			if gc, exists := groups[peer.GroupID]; exists {
				gc.Hot++
				gc.Warm--
			}
			// Recompute valency priority only for the affected group, then
			// resort the remaining candidates for the next pick.
			if i+1 < len(candidates) {
				for j := i + 1; j < len(candidates); j++ {
					if candidates[j].peer.GroupID == peer.GroupID {
						candidates[j].underValency = p.isGroupUnderHotValency(
							candidates[j].peer,
							groups,
						)
					}
				}
				slices.SortFunc(candidates[i+1:], rankCandidates)
			}
			p.config.Logger.Debug(
				"promoted peer to hot (score-based)",
				"address", peer.Address,
				"score", peer.PerformanceScore,
				"group", peer.GroupID,
			)
			events = append(events, pendingEvent{
				PeerPromotedEventType,
				PeerStateChangeEvent{
					Address: peer.Address,
					Reason:  "target active peers (score)",
				},
			})
			hotCount++
		}
	}

	// Enforce per-source quotas for hot peers
	// This ensures no single source dominates the hot peer slots
	events = append(events, p.enforcePerSourceQuotas()...)

	// Prune idle/unhelpful inbound warm peers and apply cooldown for
	// flapping identities before generic state-limit pruning.
	events = append(events, p.pruneInboundWarmPeersLocked(now, &knownRemoved)...)

	// Log valency status for topology groups
	if debugEnabled {
		p.logValencyStatus()
	}

	// Enforce overall peer targets by removing excess peers
	// Priority order (highest to lowest): Topology > Gossip > Ledger > Inbound > Unknown
	events = append(events, p.enforcePeerLimits(&knownRemoved)...)

	// Collect QuotaStatusEvent with current hot peer distribution.
	// This runs after enforcePeerLimits so InboundPruned and held counts
	// reflect the final post-prune state for this reconcile cycle.
	hotByCategory := p.getHotPeersByCategory()
	// censusInboundCounts applies the provisional-window filter so the
	// event agrees with the Prometheus gauges.
	census := p.censusInboundCounts()
	// hotByCategory["other"] still includes inbound hot peers, so
	// subtract the filtered inbound hot count rather than the raw one
	// to keep TotalHot self-consistent with the event fields.
	otherHot := max(0, hotByCategory["other"]-census.Hot)
	totalHot := hotByCategory["topology"] + hotByCategory["gossip"] +
		hotByCategory["ledger"] + census.Hot + otherHot
	events = append(events, pendingEvent{
		QuotaStatusEventType,
		QuotaStatusEvent{
			InboundWarmTarget:      p.config.InboundWarmTarget,
			InboundHotQuota:        p.config.InboundHotQuota,
			InboundWarm:            census.Warm,
			InboundHot:             census.Hot,
			InboundPruned:          p.inboundPruned,
			InboundTopologyMatched: census.TopologyMatched,
			InboundDuplex:          census.Duplex,
			TopologyHot:            hotByCategory["topology"],
			GossipHot:              hotByCategory["gossip"],
			LedgerHot:              hotByCategory["ledger"],
			OtherHot:               otherHot,
			TotalHot:               totalHot,
		},
	})
	// Log quota status for debugging
	p.config.Logger.Debug(
		"quota status",
		"inbound_warm", census.Warm,
		"inbound_hot", census.Hot,
		"inbound_warm_target", p.config.InboundWarmTarget,
		"inbound_hot_quota", p.config.InboundHotQuota,
		"inbound_pruned", p.inboundPruned,
		"inbound_topology_matched", census.TopologyMatched,
		"inbound_duplex", census.Duplex,
		"topology_hot", hotByCategory["topology"],
		"gossip_hot", hotByCategory["gossip"],
		"ledger_hot", hotByCategory["ledger"],
		"other_hot", otherHot,
		"total_hot", totalHot,
		"topology_quota", p.config.ActivePeersTopologyQuota,
		"gossip_quota", p.config.ActivePeersGossipQuota,
		"ledger_quota", p.config.ActivePeersLedgerQuota,
	)

	// Collect eligible peers for peer sharing
	// Copy peer data while holding lock to avoid race conditions
	var eligiblePeersCopy []Peer
	if p.config.PeerRequestFunc != nil {
		for _, peer := range p.peers {
			if peer != nil && peer.State == PeerStateHot &&
				peer.Connection != nil {
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

	// Peer Discovery (outside lock)
	p.mu.Unlock()

	// Publish all collected events after releasing the lock (avoid deadlock)
	p.publishPendingEvents(events)

	// Discover peers from ledger (stake pool relays) before peer sharing,
	// which can block on unresponsive peers.
	p.discoverLedgerPeers()

	for i := range eligiblePeersCopy {
		addrs := p.config.PeerRequestFunc(&eligiblePeersCopy[i])
		for _, addr := range addrs {
			// Ignore error: reaching the peer list cap during gossip
			// discovery is expected and not actionable here.
			_ = p.AddPeer(addr, PeerSourceP2PGossip)
		}
	}
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

	type removalCandidate struct {
		idx      int
		peer     *Peer
		priority int
	}

	stateCount := 0
	candidates := make([]removalCandidate, 0, len(p.peers))
	for idx, peer := range p.peers {
		if peer == nil || peer.State != state {
			continue
		}
		stateCount++
		if p.isTopologyPeer(peer.Source) {
			continue
		}
		candidates = append(candidates, removalCandidate{
			idx:      idx,
			peer:     peer,
			priority: p.peerSourcePriority(peer.Source),
		})
	}

	// Check if we're over the limit
	excess := stateCount - limit
	if excess <= 0 {
		return events
	}

	removeCount := min(excess, len(candidates))
	if removeCount <= 0 {
		return events
	}

	// Sort removable peers by removal priority (lowest priority first)
	// Priority order (highest to lowest):
	// - Topology peers (LocalRoot, PublicRoot, Bootstrap) - never remove
	// - Gossip peers
	// - Ledger peers
	// - Inbound peers
	// - Unknown peers
	slices.SortFunc(candidates, func(a, b removalCandidate) int {
		// First compare by source priority (lower priority = remove first)
		if a.priority != b.priority {
			return cmp.Compare(a.priority, b.priority)
		}
		// Same priority: lower score = remove first
		return cmp.Compare(
			a.peer.PerformanceScore,
			b.peer.PerformanceScore,
		)
	})

	removeIdx := make([]bool, len(p.peers))
	removed := 0
	for i := range removeCount {
		candidate := candidates[i]
		peer := candidate.peer
		p.config.Logger.Debug(
			"removing peer due to limit exceeded",
			"address", peer.Address,
			"state", state,
			"source", peer.Source,
			"limit", limit,
		)
		if peer.Connection != nil && p.config.ConnManager != nil {
			conn := p.config.ConnManager.GetConnectionById(peer.Connection.Id)
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
		removeIdx[candidate.idx] = true
		removed++
		*removedCount++
		if peer.Source == PeerSourceInboundConn {
			p.inboundPruned++
			if p.metrics != nil {
				p.metrics.inboundPruned.Inc()
				p.metrics.inboundPrunedByReason.WithLabelValues("limit_exceeded").Inc()
			}
		}
	}

	if removed > 0 {
		originalPeers := p.peers
		kept := originalPeers[:0]
		for idx, peer := range originalPeers {
			if removeIdx[idx] {
				continue
			}
			kept = append(kept, peer)
		}
		for idx := len(kept); idx < len(originalPeers); idx++ {
			originalPeers[idx] = nil
		}
		p.peers = kept
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

func (p *PeerGovernor) pruneInboundWarmPeersLocked(
	now time.Time,
	removedCount *int,
) []pendingEvent {
	var events []pendingEvent
	for i := len(p.peers) - 1; i >= 0; i-- {
		peer := p.peers[i]
		if peer == nil || peer.Source != PeerSourceInboundConn || peer.State != PeerStateWarm {
			continue
		}
		shouldPrune, reason, reasonLabel, cooldownDuration, applyCooldown := p.inboundPruneDecisionLocked(peer, now)
		if !shouldPrune {
			continue
		}
		oldSource := peer.Source
		oldConn := clonePeerConnection(peer.Connection)
		if peer.Connection != nil {
			events = p.appendChainSelectionEventsLocked(
				events,
				p.bootstrapExited,
				oldSource,
				oldConn,
				nil,
			)
			events = append(events, pendingEvent{
				eventType: connmanager.ConnectionRecycleRequestedEventType,
				data: connmanager.ConnectionRecycleRequestedEvent{
					ConnectionId: peer.Connection.Id,
					ConnKey:      peer.NormalizedAddress,
					Reason:       reason,
				},
			})
		}
		if applyCooldown {
			p.denyList[peer.NormalizedAddress] = now.Add(cooldownDuration)
		}
		p.config.Logger.Info(
			"pruned inbound warm peer",
			"address", peer.Address,
			"reason", reason,
			"inbound_short_lived_count", peer.InboundShortLivedCount,
			"last_inbound_disconnect", peer.LastInboundDisconnect,
			"last_inbound_session_duration", peer.LastInboundSessionDuration,
			"prune_after", p.config.InboundPruneAfter,
			"cooldown_applied", applyCooldown,
			"cooldown_duration", cooldownDuration,
		)
		events = append(events, pendingEvent{
			eventType: PeerRemovedEventType,
			data: PeerStateChangeEvent{
				Address: peer.Address,
				Reason:  reason,
			},
		})
		p.peers = slices.Delete(p.peers, i, i+1)
		p.inboundPruned++
		*removedCount++
		if p.metrics != nil {
			p.metrics.inboundPruned.Inc()
			p.metrics.inboundPrunedByReason.WithLabelValues(reasonLabel).Inc()
		}
	}
	return events
}

func (p *PeerGovernor) inboundPruneDecisionLocked(
	peer *Peer,
	now time.Time,
) (
	shouldPrune bool,
	reason, reasonLabel string,
	cooldownDuration time.Duration,
	applyCooldown bool,
) {
	reason = "inbound idle or unhelpful past prune threshold"
	reasonLabel = "idle_unhelpful"
	if peer == nil || peer.Source != PeerSourceInboundConn || peer.State != PeerStateWarm {
		return false, "", "", 0, false
	}
	if flapping, multiplier := p.inboundFlappingStateLocked(peer, now); flapping {
		cooldownDuration = p.config.InboundCooldown * time.Duration(multiplier)
		// Keep cooldown at least as long as the normal deny duration.
		if cooldownDuration < p.config.DenyDuration {
			cooldownDuration = p.config.DenyDuration
		}
		reason = "inbound flapping cooldown"
		reasonLabel = "flapping_cooldown"
		applyCooldown = true
		return true, reason, reasonLabel, cooldownDuration, applyCooldown
	}
	lastSignal := peer.FirstSeen
	if peer.LastActivity.After(lastSignal) {
		lastSignal = peer.LastActivity
	}
	if peer.ChainSyncLastUpdate.After(lastSignal) {
		lastSignal = peer.ChainSyncLastUpdate
	}
	if peer.LastBlockFetchTime.After(lastSignal) {
		lastSignal = peer.LastBlockFetchTime
	}
	if peer.LastInboundDisconnect.After(lastSignal) {
		lastSignal = peer.LastInboundDisconnect
	}
	if peer.ConnectedAt.After(lastSignal) {
		lastSignal = peer.ConnectedAt
	}
	if lastSignal.IsZero() || now.Sub(lastSignal) < p.config.InboundPruneAfter {
		return false, reason, reasonLabel, cooldownDuration, applyCooldown
	}
	if p.isInboundEligibleForHot(peer) {
		return false, reason, reasonLabel, cooldownDuration, applyCooldown
	}
	return true, reason, reasonLabel, cooldownDuration, applyCooldown
}
