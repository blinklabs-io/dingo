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
	"context"
	"net"
	"time"

	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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
// This gate supplies the independent-peer trust check. Authoritative
// density-at-intersection is performed by ledger fork resolution; ChainSync
// Jumping remains separate deferred work. See ARCHITECTURE.md ("Ouroboros
// Genesis trust model").

// genesisCorroborationActiveLocked reports whether the Genesis corroboration
// gate is in effect (Genesis mode with a positive threshold). When false the
// selector behaves as before (density-only / Praos) and does not track the
// per-peer hash frontier used for corroboration.
func (cs *ChainSelector) genesisCorroborationActiveLocked() bool {
	return cs.mode == SelectionModeGenesis &&
		cs.config.MinCorroboratingPeers > 0
}

// GenesisCorroborationActive reports whether the Genesis corroboration gate is
// currently in effect. Callers use it to decide whether tip observation must be
// ordered synchronously with the apply-eligibility gate (see ShouldApplyIngress):
// the gate is only non-trivial while corroboration is active.
func (cs *ChainSelector) GenesisCorroborationActive() bool {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	return cs.genesisCorroborationActiveLocked()
}

// RecordDeliveredPeerTip keeps a bounded corroboration history separate from
// the admission frontier, whose contents can be discarded after rejection or
// rebuilt from a later intersection.
func (cs *ChainSelector) RecordDeliveredPeerTip(update PeerTipUpdateEvent) {
	if update.ObservedTip.Point.Slot == 0 &&
		len(update.ObservedTip.Point.Hash) == 0 {
		return
	}
	if !cs.peerTipAdmissionGenerationCurrent(update) {
		return
	}
	if cs.config.ConnectionLive != nil &&
		!cs.config.ConnectionLive(update.ConnectionId) {
		return
	}
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	if !cs.genesisCorroborationActiveLocked() {
		return
	}
	peerTip := cs.deliveredPeerTips[update.ConnectionId]
	if peerTip == nil {
		if len(cs.deliveredPeerTips) >= cs.maxTrackedPeers {
			return
		}
		peerTip = &PeerChainTip{
			ConnectionId: update.ConnectionId,
			nowFn:        cs.nowFn,
		}
		cs.deliveredPeerTips[update.ConnectionId] = peerTip
	}
	cs.applyPeerTipAdmissionUpdateLocked(peerTip, update)
	cs.signalIngressChange()
}

// RecordDeliveredPeerRollback trims the delivered history of a peer after a
// rollback. It touches only an existing delivered record: it never registers
// the connection, so it is safe for peers that are not admitted to selection.
func (cs *ChainSelector) RecordDeliveredPeerRollback(e PeerRollbackEvent) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	deliveredTip := cs.deliveredPeerTips[e.ConnectionId]
	if deliveredTip == nil {
		return
	}
	deliveredTip.ApplyRollback(e.Point, e.Tip)
	cs.signalIngressChange()
}

// peerLiveEligibleNonStaleLocked reports whether a peer passes the shared
// live/eligible/non-stale prerequisite used by both Genesis corroboration
// witness eligibility and normal selection eligibility. It is the single
// source of truth for those three checks.
func (cs *ChainSelector) peerLiveEligibleNonStaleLocked(
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
	return !cs.isPeerTipStale(peerTip)
}

// peerCanCorroborateLocked reports whether a peer is a valid corroboration
// witness: live, eligible, and non-stale. It deliberately omits the
// corroboration check itself (which would be circular) and the behind-best
// selection filters, so honest peers that are slightly behind the fast source
// still count as witnesses.
func (cs *ChainSelector) peerCanCorroborateLocked(
	connId ouroboros.ConnectionId,
	peerTip *PeerChainTip,
) bool {
	return cs.peerLiveEligibleNonStaleLocked(connId, peerTip)
}

// corroboratorIdentity returns the independence group used to de-duplicate
// corroboration witnesses, and whether the connection could be placed in one.
// With a PeerIdentity hook configured the group is the one it reports, which
// the node backs with peer governance's diversity group, so one operator's
// several addresses count once. A connection the hook cannot place has no
// identity: keying it by remote host instead would put it in a different
// namespace from its siblings, so an untracked connection and its tracked
// siblings would count as independent of each other. Without a hook,
// witnesses are grouped by remote host. Either is a lower bound on
// independence, not a guarantee.
func (cs *ChainSelector) corroboratorIdentity(
	connId ouroboros.ConnectionId,
) (string, bool) {
	if cs.config.PeerIdentity != nil {
		id := cs.config.PeerIdentity(connId)
		return id, id != ""
	}
	if connId.RemoteAddr == nil {
		return connId.String(), true
	}
	addr := connId.RemoteAddr.String()
	if host, _, err := net.SplitHostPort(addr); err == nil && host != "" {
		return host, true
	}
	return addr, true
}

// witnessSupportsSuffixLocked checks that the witness's delivered block number
// is consistent with the candidate's current point within k blocks. The point
// itself must match in confirmsRecentChain. Keepalive activity does not advance
// this delivered frontier.
func (cs *ChainSelector) witnessSupportsSuffixLocked(
	candidateTip *PeerChainTip,
	witnessTip *PeerChainTip,
) bool {
	return safeAddUint64(
		witnessTip.SelectionTip().BlockNumber,
		cs.securityParam,
	) >= candidateTip.SelectionTip().BlockNumber
}

// corroboratingPeersLocked counts the distinct witness identities whose chain
// confirms the candidate's recent chain within the Genesis window and reaches
// its suffix. Any witness sharing the candidate's own identity is excluded, so
// a Sybil fast source opening several connections cannot self-corroborate.
// A candidate or witness without an identity fails closed: the candidate has
// no corroborators and the witness does not count.
func (cs *ChainSelector) corroboratingPeersLocked(
	candidate ouroboros.ConnectionId,
	candidateTip *PeerChainTip,
) int {
	return cs.corroboratingPeersIncludingCandidatesLocked(
		candidate,
		candidateTip,
	)
}

func (cs *ChainSelector) corroboratingPeersIncludingCandidatesLocked(
	candidate ouroboros.ConnectionId,
	candidateTip *PeerChainTip,
) int {
	if candidateTip == nil {
		return 0
	}
	candidateIdentity, ok := cs.corroboratorIdentity(candidate)
	if !ok {
		return 0
	}
	witnesses := make(map[string]struct{})
	seen := make(map[ouroboros.ConnectionId]struct{},
		len(cs.peerTips)+len(cs.pendingPeerTips)+len(cs.deliveredPeerTips))
	countWitness := func(connId ouroboros.ConnectionId, peerTip *PeerChainTip) {
		if connId == candidate || !cs.peerCanCorroborateLocked(connId, peerTip) {
			return
		}
		if !candidateTip.confirmsRecentChainWithin(
			peerTip,
			cs.genesisWindowSlotsLocked(),
		) || !cs.witnessSupportsSuffixLocked(candidateTip, peerTip) {
			return
		}
		identity, ok := cs.corroboratorIdentity(connId)
		if !ok || identity == candidateIdentity {
			return
		}
		witnesses[identity] = struct{}{}
	}
	for connId, committedTip := range cs.peerTips {
		seen[connId] = struct{}{}
		peerTip := cs.latestCorroborationWitnessLocked(connId)
		if peerTip == nil {
			peerTip = committedTip
		}
		countWitness(connId, peerTip)
	}
	for connId := range cs.pendingPeerTips {
		if _, exists := seen[connId]; exists {
			continue
		}
		seen[connId] = struct{}{}
		peerTip := cs.latestCorroborationWitnessLocked(connId)
		countWitness(connId, peerTip)
	}
	for connId, peerTip := range cs.deliveredPeerTips {
		if _, exists := seen[connId]; exists {
			continue
		}
		countWitness(connId, peerTip)
	}
	return len(witnesses)
}

func (cs *ChainSelector) latestCorroborationWitnessLocked(
	connId ouroboros.ConnectionId,
) *PeerChainTip {
	if witness, ok := cs.forecastHorizonWitnesses[connId]; ok {
		return witness.peerTip
	}
	if witness := cs.deliveredPeerTips[connId]; witness != nil {
		return witness
	}
	return cs.latestPeerTipCandidateLocked(connId)
}

// frontierTrustedLocked reports whether a peer's delivered frontier may be used
// to exclude other candidates as behind. In Genesis mode the peer must be live,
// eligible and fresh and its chain must be corroborated by at least
// MinCorroboratingPeers independent witnesses, and by one even when the
// threshold is 0, so a single peer cannot define the frontier. Outside Genesis
// every eligible, non-stale peer's frontier counts.
func (cs *ChainSelector) frontierTrustedLocked(
	connId ouroboros.ConnectionId,
	peerTip *PeerChainTip,
) bool {
	if cs.mode != SelectionModeGenesis {
		return cs.isConnectionEligible(connId) && !cs.isPeerTipStale(peerTip)
	}
	return cs.peerLiveEligibleNonStaleLocked(connId, peerTip) &&
		cs.corroboratingPeersLocked(connId, peerTip) >=
			max(1, cs.config.MinCorroboratingPeers)
}

// isPeerCorroboratedLocked reports whether a peer may drive Genesis chain
// selection. Corroboration only applies in Genesis mode with a positive
// MinCorroboratingPeers; otherwise every peer passes (density-only / Praos
// selection), preserving the historical default.
func (cs *ChainSelector) isPeerCorroboratedLocked(
	connId ouroboros.ConnectionId,
	peerTip *PeerChainTip,
) bool {
	if !cs.genesisCorroborationActiveLocked() {
		return true
	}
	return cs.corroboratingPeersLocked(connId, peerTip) >=
		cs.config.MinCorroboratingPeers
}

// ShouldApplyIngress reports whether headers/blocks from connId may be applied
// to the ledger. It returns false only while Genesis corroboration is active
// (Genesis mode with a positive threshold) and the peer is not corroborated, so
// an uncorroborated or divergent fast source is OBSERVED — its tips still feed
// corroboration via PeerTipUpdateEvent — but its blocks are not applied and it
// cannot steer the ledger. Enforcing the stall here (ledger application) rather
// than only at peer selection is required because ingress is gated on peer-
// governance eligibility, independent of the selected best peer: clearing the
// active chainsync driver alone would not stop an eligible uncorroborated peer's
// headers from being applied.
//
// An unknown peer (no tips observed yet) is treated as uncorroborated and denied
// while the gate is active — fail closed until corroboration forms. Outside
// Genesis corroboration it returns true unless the peer's advertised tip is more
// than a Genesis window older than the wall clock, which is denied in Genesis
// mode regardless of corroboration (see advertisedTipStaleLocked).
func (cs *ChainSelector) ShouldApplyIngress(
	connId ouroboros.ConnectionId,
) bool {
	return cs.ingressDecision(connId) == ingressApply
}

type ingressVerdict int

const (
	ingressApply ingressVerdict = iota
	// ingressAwaitCorroboration: the peer is denied only because no
	// independent witness has delivered its latest header yet.
	ingressAwaitCorroboration
	ingressDeny
)

func (cs *ChainSelector) ingressDecision(
	connId ouroboros.ConnectionId,
) ingressVerdict {
	releaseGeneration, generationCurrent := cs.pinCurrentHeaderCryptoGeneration()
	if !generationCurrent {
		if cs.GenesisCorroborationActive() {
			// The ledger is publishing a new snapshot; the verdict is only
			// unavailable for the moment.
			return ingressAwaitCorroboration
		}
		return ingressDeny
	}
	defer releaseGeneration()
	var observedPoint ocommon.Point
	var observedBlock uint64
	var localTip ochainsync.Tip
	verdict := func() ingressVerdict {
		cs.mutex.RLock()
		defer cs.mutex.RUnlock()
		peerTip := cs.latestPeerTipCandidateLocked(connId)
		if peerTip == nil {
			peerTip = cs.peerTips[connId]
		}
		// A peer whose advertised tip is older than the wall clock by more than a
		// Genesis window serves an abandoned chain. Its headers must not enter the
		// ledger's shared header queue, where they would outlive its selection and
		// make honest peers' headers look like a persistent fork.
		if _, active := cs.staleTipRuleWallSlotLocked(); active {
			// Tip observation is ordered before this gate. If admission rejected
			// that observation, there is no tip against which to apply the stale
			// bound, so accepting the header would bypass the rule.
			if peerTip == nil || cs.advertisedTipStaleLocked(peerTip) {
				return ingressDeny
			}
		}
		if !cs.genesisCorroborationActiveLocked() {
			return ingressApply
		}
		if peerTip == nil {
			return ingressDeny
		}
		if cs.corroboratingPeersIncludingCandidatesLocked(connId, peerTip) >=
			cs.config.MinCorroboratingPeers {
			return ingressApply
		}
		observedPoint = clonePoint(peerTip.ObservedTip.Point)
		observedBlock = peerTip.ObservedTip.BlockNumber
		localTip = cs.localTip
		return ingressAwaitCorroboration
	}()
	if verdict != ingressAwaitCorroboration {
		return verdict
	}
	if cs.pointOnAppliedChain(observedPoint, observedBlock, localTip) {
		return ingressApply
	}
	return ingressAwaitCorroboration
}

func (cs *ChainSelector) pointOnAppliedChain(
	point ocommon.Point,
	blockNumber uint64,
	localTip ochainsync.Tip,
) bool {
	if cs.config.PointOnLocalChain == nil || len(point.Hash) == 0 {
		return false
	}
	if cs.config.AppliedTip != nil {
		if currentTip, ok := cs.config.AppliedTip(); ok {
			localTip = currentTip
		}
	}
	if blockNumber > localTip.BlockNumber || point.Slot > localTip.Point.Slot {
		return false
	}
	return cs.config.PointOnLocalChain(point)
}

// AwaitIngressCorroboration holds a header from connId that ShouldApplyIngress
// denies only because the peer is not yet corroborated, and reports whether
// the header may then be applied. It returns false at once when the denial has
// another cause (Genesis corroboration is off, the advertised tip is stale, or
// the peer is untracked), and returns the context's error if it is cancelled
// while waiting.
//
// The caller's chainsync callback blocks here while witnesses can advance.
// The caller bounds that wait; on expiry it withholds the header but keeps the
// session and staged candidate frontier so another peer can deliver the same
// point. Re-intersecting immediately from the applied tip discards that
// frontier and repeats the hold. Staged observations remain bounded, so a peer
// that runs beyond the admission window cannot corroborate its entire prefix.
func (cs *ChainSelector) AwaitIngressCorroboration(
	ctx context.Context,
	connId ouroboros.ConnectionId,
) (bool, error) {
	var ticker *time.Ticker
	for {
		wake := cs.ingressWakeChan()
		switch cs.ingressDecision(connId) {
		case ingressApply:
			return true, nil
		case ingressDeny:
			return false, nil
		case ingressAwaitCorroboration:
		}
		if ticker == nil {
			// The verdict also moves with ledger snapshot publication, which
			// does not signal the wake channel.
			ticker = time.NewTicker(eagernessPollInterval)
			defer ticker.Stop()
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-wake:
		case <-ticker.C:
		}
	}
}

// ingressWakeChan returns a channel closed by the next change to staged or
// committed peer frontiers.
func (cs *ChainSelector) ingressWakeChan() <-chan struct{} {
	cs.ingressWakeMu.Lock()
	defer cs.ingressWakeMu.Unlock()
	if cs.ingressWake == nil {
		cs.ingressWake = make(chan struct{})
	}
	return cs.ingressWake
}

// signalIngressChange wakes every AwaitIngressCorroboration call to re-read
// its verdict. It takes only its own mutex, so callers may hold cs.mutex.
func (cs *ChainSelector) signalIngressChange() {
	cs.ingressWakeMu.Lock()
	defer cs.ingressWakeMu.Unlock()
	if cs.ingressWake != nil {
		close(cs.ingressWake)
		cs.ingressWake = nil
	}
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

// genesisCorroborationFailureLocked warns (deduped) and returns a
// GenesisCorroborationFailedEvent when the densest Genesis fast source cannot be
// corroborated. The warning log and dedup run whether or not an EventBus is
// configured, so an enabled gate that is stalling bootstrap is always visible;
// only event publishing needs the bus. Returns nil outside Genesis
// corroboration, when the leader is corroborated, when the same failure was
// already reported, or when no EventBus is configured to publish to.
func (cs *ChainSelector) genesisCorroborationFailureLocked() *event.Event {
	if !cs.genesisCorroborationActiveLocked() {
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
	if cs.config.EventBus == nil {
		return nil
	}
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
			Selectable: cs.isPeerSelectableLocked(
				connId,
				peerTip,
			),
		})
	}
	return status
}
