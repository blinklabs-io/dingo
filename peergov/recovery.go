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
	"cmp"
	"slices"
	"time"
)

// Connection recovery used to be edge-triggered only: the single
// reconnect goroutine spawned by handleConnectionClosedEvent was the
// only thing that ever redialed a known peer. Any path that missed
// that one chance (a close event that no longer matches the peer, an
// intentional churn close, a dial loop that exited because inbound
// connections satisfied valency) left the peer cold forever, and a
// node whose last upstream hit one of those paths silently stopped
// following the chain. The reconcile loop now level-triggers recovery
// through redialDisconnectedPeersLocked.

// maxEmergencyRedialsPerReconcile bounds how many gossip/ledger peers a
// single reconcile cycle will redial when the node has no eligible
// upstream connection left or the hot set sits below MinHotPeers.
const maxEmergencyRedialsPerReconcile = 3

// chainsyncStallRankWindow is how long after a ChainSync stall a peer ranks
// behind every other redial candidate.
const chainsyncStallRankWindow = 15 * time.Minute

// countEligibleUpstreamsLocked returns the number of peers whose current
// connection can feed chainsync ingress. Must be called with p.mu held.
func (p *PeerGovernor) countEligibleUpstreamsLocked() int {
	count := 0
	for _, peer := range p.peers {
		if peer == nil {
			continue
		}
		if chainSelectionState(
			p.bootstrapExited,
			peer.Source,
			p.selectionConnLocked(peer),
		).eligible {
			count++
		}
	}
	return count
}

// hotSetDeficitLocked returns how far the current hot count sits below
// MinHotPeers, floored at zero. Must be called with p.mu held.
func (p *PeerGovernor) hotSetDeficitLocked() int {
	return max(0, p.config.MinHotPeers-p.countHotPeersLocked())
}

// redialCandidatesLocked returns known peers that should get a new
// outbound connection attempt. Topology peers are always redialed: they
// are operator-configured and must converge back to connected.
// Gossip/ledger peers are redialed under a per-cycle budget when the
// node has no eligible upstream connection, or when the hot set is below
// MinHotPeers and the promotable warm pool cannot close that gap on its
// own. Outside those cases churn retires them as designed. The budget
// goes to the highest-scoring peers first; see redialRankCompare. Must be
// called with p.mu held.
func (p *PeerGovernor) redialCandidatesLocked() []*Peer {
	var candidates []*Peer
	nonRoot := make([]*Peer, 0, len(p.peers))
	eligibleUpstreams := p.countEligibleUpstreamsLocked()
	warmShortfall := p.hotSetDeficitLocked() >
		p.countPromotableWarmNonRootPeersLocked()

	emergencyBudget := 0
	trigger := ""
	switch {
	case eligibleUpstreams == 0:
		emergencyBudget = maxEmergencyRedialsPerReconcile
		trigger = "zero_upstream"
	case warmShortfall:
		emergencyBudget = maxEmergencyRedialsPerReconcile
		trigger = "hot_deficit"
	}
	for _, peer := range p.peers {
		if peer == nil || peer.Connection != nil || peer.Reconnecting {
			continue
		}
		if p.isPeerDeniedLocked(peer) {
			continue
		}
		switch peer.Source {
		case PeerSourceTopologyLocalRoot, PeerSourceTopologyPublicRoot:
			if p.inboundSatisfiesTopologyValencyLocked(peer) {
				continue
			}
		case PeerSourceTopologyBootstrapPeer:
			if !p.canPromoteBootstrapPeer() {
				continue
			}
		case PeerSourceP2PGossip, PeerSourceP2PLedger:
			if emergencyBudget == 0 {
				continue
			}
			// With an upstream still connected, a peer churn dropped for
			// its score would be promoted straight back by reconcile's
			// refill and churned again next interval. Leave it cold until
			// score aging lifts it back over the threshold.
			if trigger == "hot_deficit" && p.observedBelowThreshold(peer) {
				continue
			}
			nonRoot = append(nonRoot, peer)
			continue
		default:
			continue
		}
		candidates = append(candidates, peer)
	}
	slices.SortStableFunc(nonRoot, p.redialRankCompare)
	for _, peer := range nonRoot[:min(len(nonRoot), emergencyBudget)] {
		if p.metrics != nil {
			p.metrics.coldPeerRedialsByTrigger.WithLabelValues(
				trigger,
			).Inc()
		}
		candidates = append(candidates, peer)
	}
	return candidates
}

// observedBelowThreshold reports whether peer has scoring observations
// and a score below MinScoreThreshold. A never-observed peer keeps its
// zero initial score, which says nothing about its quality.
func (p *PeerGovernor) observedBelowThreshold(peer *Peer) bool {
	return !peer.ScoreLastUpdate.IsZero() &&
		peer.PerformanceScore < p.config.MinScoreThreshold
}

// recentlyStalled reports whether a connection to peer closed on a ChainSync
// stall within chainsyncStallRankWindow.
func recentlyStalled(peer *Peer) bool {
	return !peer.LastChainsyncStall.IsZero() &&
		time.Since(peer.LastChainsyncStall) < chainsyncStallRankWindow
}

// redialRankCompare orders gossip/ledger redial candidates: recently stalled
// peers last, then observed below-threshold peers, then by score descending.
func (p *PeerGovernor) redialRankCompare(a, b *Peer) int {
	aStalled := recentlyStalled(a)
	bStalled := recentlyStalled(b)
	if aStalled != bStalled {
		if aStalled {
			return 1
		}
		return -1
	}
	aBad := p.observedBelowThreshold(a)
	bBad := p.observedBelowThreshold(b)
	if aBad != bBad {
		if aBad {
			return 1
		}
		return -1
	}
	return cmp.Compare(b.PerformanceScore, a.PerformanceScore)
}

// redialDisconnectedPeersLocked spawns outbound connection attempts for
// redialCandidatesLocked. Must be called with p.mu held; the spawned
// goroutines acquire the lock themselves and are deduplicated by the
// per-peer Reconnecting flag inside createOutboundConnection.
func (p *PeerGovernor) redialDisconnectedPeersLocked() {
	if p.config.DisableOutbound ||
		p.config.ConnManager == nil ||
		p.stopCh == nil {
		return
	}
	for _, peer := range p.redialCandidatesLocked() {
		p.config.Logger.Info(
			"redialing disconnected peer",
			"address", peer.Address,
			"source", peer.Source.String(),
		)
		p.spawnOutboundConnectionLocked(peer)
	}
}
