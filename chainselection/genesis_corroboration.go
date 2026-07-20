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

package chainselection

import (
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

// Genesis corroboration implements the Ouroboros Genesis trust property that a
// fast (shallow) block source is followed only while corroborated by
// independent peers. A candidate peer is "corroborated" when at least
// MinCorroboratingPeers other eligible, live, non-stale peers report the same
// recent block(s) within the Genesis density window (identical slot+hash in
// their observed frontier). An uncorroborated fast source — including a
// divergent source on a private fork that no honest peer reports — is denied
// chain selection so it stalls rather than steering the local chain.
//
// This is a corroboration gate, not full Ouroboros Genesis density-at-
// intersection with ChainSync Jumping. See ARCHITECTURE.md ("Ouroboros Genesis
// trust model") for the supported trust model and the explicitly deferred
// pieces.

// peerCanCorroborateLocked reports whether a peer is a valid corroboration
// witness: live, eligible, and non-stale. It deliberately omits the
// corroboration check itself (which would be circular) and the behind-best
// selection filters, so honest peers that are slightly behind the fast source
// still count as witnesses.
func (cs *ChainSelector) peerCanCorroborateLocked(
	connId ouroboros.ConnectionId,
	peerTip *PeerChainTip,
) bool {
	if peerTip == nil {
		return false
	}
	if cs.config.ConnectionLive != nil && !cs.config.ConnectionLive(connId) {
		return false
	}
	if !cs.isConnectionEligible(connId) {
		return false
	}
	if cs.isPeerTipStale(peerTip) {
		return false
	}
	return true
}

// corroboratingPeersLocked counts the distinct other eligible, live, non-stale
// peers whose observed frontier shares at least one identical (slot, hash)
// point with the candidate.
func (cs *ChainSelector) corroboratingPeersLocked(
	candidate ouroboros.ConnectionId,
	candidateTip *PeerChainTip,
) int {
	if candidateTip == nil {
		return 0
	}
	count := 0
	for connId, peerTip := range cs.peerTips {
		if connId == candidate {
			continue
		}
		if !cs.peerCanCorroborateLocked(connId, peerTip) {
			continue
		}
		if candidateTip.sharesObservedPoint(peerTip) {
			count++
		}
	}
	return count
}

// isPeerCorroboratedLocked reports whether a peer may drive Genesis chain
// selection. Corroboration only applies in Genesis mode with a positive
// MinCorroboratingPeers; otherwise every peer passes (density-only / Praos
// selection), preserving the historical default.
func (cs *ChainSelector) isPeerCorroboratedLocked(
	connId ouroboros.ConnectionId,
	peerTip *PeerChainTip,
) bool {
	if cs.mode != SelectionModeGenesis ||
		cs.config.MinCorroboratingPeers <= 0 {
		return true
	}
	return cs.corroboratingPeersLocked(connId, peerTip) >=
		cs.config.MinCorroboratingPeers
}

// rawDensityLeaderLocked returns the connection with the highest observed
// density among live/eligible/non-stale peers, ignoring corroboration. This is
// the fast source Genesis selection would pick on density alone; comparing it
// against corroboration surfaces a divergent/uncorroborated leader.
func (cs *ChainSelector) rawDensityLeaderLocked() (
	ouroboros.ConnectionId,
	uint64,
	bool,
) {
	window := cs.genesisWindowSlotsLocked()
	var leader ouroboros.ConnectionId
	var bestDensity uint64
	var bestBlock uint64
	found := false
	for connId, peerTip := range cs.peerTips {
		if !cs.peerCanCorroborateLocked(connId, peerTip) {
			continue
		}
		density := peerTip.observedDensity(window)
		block := peerTip.SelectionTip().BlockNumber
		if !found || density > bestDensity ||
			(density == bestDensity && block > bestBlock) {
			leader = connId
			bestDensity = density
			bestBlock = block
			found = true
		}
	}
	return leader, bestDensity, found
}

// genesisCorroborationFailureLocked returns a GenesisCorroborationFailedEvent
// when the densest Genesis fast source cannot be corroborated, deduped so a
// persistently uncorroborated leader emits once (not on every evaluation).
// Returns nil outside Genesis corroboration, when the leader is corroborated,
// or when the same failure was already reported.
func (cs *ChainSelector) genesisCorroborationFailureLocked() *event.Event {
	if cs.config.EventBus == nil ||
		cs.mode != SelectionModeGenesis ||
		cs.config.MinCorroboratingPeers <= 0 {
		cs.lastCorroborationFailedConn = nil
		return nil
	}
	leader, density, ok := cs.rawDensityLeaderLocked()
	if !ok {
		cs.lastCorroborationFailedConn = nil
		return nil
	}
	corroborators := cs.corroboratingPeersLocked(leader, cs.peerTips[leader])
	if corroborators >= cs.config.MinCorroboratingPeers {
		cs.lastCorroborationFailedConn = nil
		return nil
	}
	if cs.lastCorroborationFailedConn != nil &&
		*cs.lastCorroborationFailedConn == leader {
		return nil
	}
	leaderCopy := leader
	cs.lastCorroborationFailedConn = &leaderCopy
	cs.config.Logger.Warn(
		"genesis fast source lacks corroboration; denying chain selection",
		"connection_id", leader.String(),
		"observed_density", density,
		"corroborating_peers", corroborators,
		"required_peers", cs.config.MinCorroboratingPeers,
		"genesis_window_slots", cs.genesisWindowSlotsLocked(),
	)
	evt := event.NewEvent(
		GenesisCorroborationFailedEventType,
		GenesisCorroborationFailedEvent{
			ConnectionId:       leader,
			ObservedDensity:    density,
			CorroboratingPeers: corroborators,
			RequiredPeers:      cs.config.MinCorroboratingPeers,
			GenesisWindowSlots: cs.genesisWindowSlotsLocked(),
		},
	)
	return &evt
}

// GenesisPeerStatus is the per-peer Genesis observability snapshot.
type GenesisPeerStatus struct {
	ConnectionId       ouroboros.ConnectionId
	ObservedDensity    uint64
	CorroboratingPeers int
	Corroborated       bool
	Selectable         bool
}

// GenesisStatus is a point-in-time snapshot of Genesis selection state for
// observability (metrics, logs, operator tooling).
type GenesisStatus struct {
	Mode                  SelectionMode
	WindowSlots           uint64
	MinCorroboratingPeers int
	BestSource            *ouroboros.ConnectionId
	Peers                 []GenesisPeerStatus
}

// GenesisStatus returns a snapshot of current selection mode, Genesis window,
// selected fast source, and per-peer density/corroboration for observability.
func (cs *ChainSelector) GenesisStatus() GenesisStatus {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()

	window := cs.genesisWindowSlotsLocked()
	status := GenesisStatus{
		Mode:                  cs.mode,
		WindowSlots:           window,
		MinCorroboratingPeers: cs.config.MinCorroboratingPeers,
		Peers:                 make([]GenesisPeerStatus, 0, len(cs.peerTips)),
	}
	if cs.bestPeerConn != nil {
		best := *cs.bestPeerConn
		status.BestSource = &best
	}
	for connId, peerTip := range cs.peerTips {
		corroborators := cs.corroboratingPeersLocked(connId, peerTip)
		status.Peers = append(status.Peers, GenesisPeerStatus{
			ConnectionId:       connId,
			ObservedDensity:    peerTip.observedDensity(window),
			CorroboratingPeers: corroborators,
			Corroborated:       cs.isPeerCorroboratedLocked(connId, peerTip),
			Selectable:         cs.isPeerSelectableLocked(connId, peerTip, false),
		})
	}
	return status
}
