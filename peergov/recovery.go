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
// upstream connection left.
const maxEmergencyRedialsPerReconcile = 3

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
			peer.Connection,
		).eligible {
			count++
		}
	}
	return count
}

// redialCandidatesLocked returns known peers that should get a new
// outbound connection attempt. Topology peers are always redialed:
// they are operator-configured and must converge back to connected.
// Gossip/ledger peers are only redialed when the node has no eligible
// upstream connection at all, capped per cycle, so normal churn still
// retires them as designed. Must be called with p.mu held.
func (p *PeerGovernor) redialCandidatesLocked() []*Peer {
	var candidates []*Peer
	emergencyBudget := 0
	if p.countEligibleUpstreamsLocked() == 0 {
		emergencyBudget = maxEmergencyRedialsPerReconcile
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
			emergencyBudget--
		default:
			continue
		}
		candidates = append(candidates, peer)
	}
	return candidates
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
		go p.createOutboundConnection(peer)
	}
}
