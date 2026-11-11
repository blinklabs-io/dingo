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
	"bytes"
	"cmp"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/blinklabs-io/dingo/consensus/praos"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// The Genesis state machine mirrors ouroboros-consensus' GSM: Genesis
// selection (Syncing) is left only when every ChainSync connection is idle and
// no candidate is better than the selection (blockUntilCaughtUp), and is
// re-entered when the selection becomes older than the configured age
// (blockWhileCaughtUp).

const (
	// DefaultMaxCaughtUpAge is how old the selection tip may be before a
	// caught-up node returns to Genesis selection, and the minimum time spent
	// in Praos after leaving Genesis (the reference's maxCaughtUpAge).
	DefaultMaxCaughtUpAge = 20 * time.Minute

	// CaughtUpBonusMax bounds the random extra delay added to the re-entry
	// deadline once per Praos period, so that nodes sharing a stalled network
	// do not all return to Genesis selection at the same instant.
	CaughtUpBonusMax = 300 * time.Second

	// historicityGrace is added to the Genesis window, converted to time, to
	// form the default historicity cutoff (3k/f slots of one second plus one
	// hour in the reference).
	historicityGrace = time.Hour
)

// DefaultHistoricityCutoff returns the age beyond which a tip an idle peer
// reports is too old for an honest caught-up peer: the Genesis window at one
// second per slot plus one hour, the reference's 3*k/f + 3600 seconds.
func DefaultHistoricityCutoff(windowSlots uint64) time.Duration {
	const maxSeconds = uint64(1<<63-1) / uint64(time.Second)
	if windowSlots > maxSeconds-uint64(historicityGrace/time.Second) {
		return time.Duration(1<<63 - 1)
	}
	return time.Duration(windowSlots)*time.Second + historicityGrace
}

func defaultRandomBonus(maxBonus time.Duration) time.Duration {
	if maxBonus <= 0 {
		return 0
	}
	// A deployment-spreading jitter, not a secret.
	return time.Duration(
		rand.Int64N(int64(maxBonus) + 1), //nolint:gosec // G404: jitter only
	)
}

// HistoricityDisconnect describes a peer that reported itself idle at a tip
// too old for an honest caught-up peer.
type HistoricityDisconnect struct {
	ConnectionId ouroboros.ConnectionId
	// Point is the delivered point the peer is idle at.
	Point ocommon.Point
	// SlotTime is the start time of Point's slot.
	SlotTime time.Time
	// Age is how long before now SlotTime lies.
	Age time.Duration
	// Cutoff is the age beyond which the tip counts as historic.
	Cutoff time.Duration
}

// gsmPeer is one ChainSync connection as the exit rule sees it.
type gsmPeer struct {
	connId ouroboros.ConnectionId
	// idle is true when the peer's latest MsgAwaitReply is at its delivered
	// frontier and no delivered header waits for admission.
	idle bool
	// haveTip is false for a connection that has delivered nothing the
	// selector tracks; point then holds its await point.
	haveTip bool
	tip     ochainsync.Tip
	view    PraosTiebreakerView
	// awaitingFirst marks a tip that is only the confirmed intersection.
	awaitingFirst bool
	// delivered marks a tip taken from the delivered frontier rather than
	// the admission frontier.
	delivered    bool
	await        *ocommon.Point
	pendingCount int
}

type gsmSnapshot struct {
	localTip      ochainsync.Tip
	localTipKnown bool
	now           time.Time
	cutoff        time.Duration
	peers         []gsmPeer
}

func (s gsmSnapshot) sameAs(o gsmSnapshot) bool {
	if !pointsEqual(s.localTip.Point, o.localTip.Point) ||
		s.localTipKnown != o.localTipKnown ||
		len(s.peers) != len(o.peers) {
		return false
	}
	for i := range s.peers {
		a, b := s.peers[i], o.peers[i]
		if a.connId != b.connId || a.idle != b.idle ||
			a.haveTip != b.haveTip || a.delivered != b.delivered ||
			a.tip.BlockNumber != b.tip.BlockNumber ||
			!pointsEqual(a.tip.Point, b.tip.Point) ||
			a.pendingCount != b.pendingCount ||
			(a.await == nil) != (b.await == nil) ||
			(a.await != nil && !pointsEqual(*a.await, *b.await)) {
			return false
		}
	}
	return true
}

func (cs *ChainSelector) historicityCutoffLocked() time.Duration {
	if cs.config.HistoricityCutoff > 0 {
		return cs.config.HistoricityCutoff
	}
	return DefaultHistoricityCutoff(cs.genesisWindowSlotsLocked())
}

func (cs *ChainSelector) maxCaughtUpAge() time.Duration {
	if cs.config.MaxCaughtUpAge > 0 {
		return cs.config.MaxCaughtUpAge
	}
	return DefaultMaxCaughtUpAge
}

// appliedLocalTipLocked returns the live applied tip when it is available and
// the last tip given to SetLocalTip otherwise.
func (cs *ChainSelector) appliedLocalTipLocked() (ochainsync.Tip, bool) {
	if cs.config.AppliedTip != nil {
		if tip, ok := cs.config.AppliedTip(); ok {
			return tip, true
		}
	}
	return cs.localTip, cs.localTipKnown
}

// gsmConnectionsLocked returns every live connection the exit rule must hear
// from: the node's ChainSync connections, observers included, and every
// connection the selector tracks.
func (cs *ChainSelector) gsmConnectionsLocked() []ouroboros.ConnectionId {
	seen := make(map[ouroboros.ConnectionId]struct{})
	var conns []ouroboros.ConnectionId
	add := func(connId ouroboros.ConnectionId) {
		if _, ok := seen[connId]; ok {
			return
		}
		if cs.config.ConnectionLive != nil &&
			!cs.config.ConnectionLive(connId) {
			return
		}
		seen[connId] = struct{}{}
		conns = append(conns, connId)
	}
	if cs.config.ChainSyncConnections != nil {
		for _, connId := range cs.config.ChainSyncConnections() {
			add(connId)
		}
	}
	for connId := range cs.peerTips {
		add(connId)
	}
	for connId := range cs.deliveredPeerTips {
		add(connId)
	}
	for connId := range cs.awaiting {
		add(connId)
	}
	return conns
}

// gsmSnapshotLocked captures the inputs of the exit rule. Idleness is derived
// here, from the await point and the delivered frontier, so that an await that
// reached the selector before the header it was sent at was admitted still
// counts once that header is promoted.
func (cs *ChainSelector) gsmSnapshotLocked() gsmSnapshot {
	snap := gsmSnapshot{now: cs.now(), cutoff: cs.historicityCutoffLocked()}
	snap.localTip, snap.localTipKnown = cs.appliedLocalTipLocked()
	for _, connId := range cs.gsmConnectionsLocked() {
		peer := gsmPeer{
			connId:       connId,
			pendingCount: len(cs.pendingPeerTips[connId]),
		}
		if await, ok := cs.awaiting[connId]; ok {
			point := clonePoint(await)
			peer.await = &point
		}
		tracked, fromDelivered := cs.gsmFrontierLocked(connId, peer.await)
		if tracked != nil {
			peer.haveTip = true
			peer.delivered = fromDelivered
			peer.tip = cloneObservedTip(tracked.SelectionTip())
			peer.view = clonePraosTiebreakerView(tracked.PraosView)
			peer.awaitingFirst = tracked.awaitingFirstHeader
		}
		peer.idle = peer.await != nil && peer.pendingCount == 0 &&
			(!peer.haveTip || pointsEqual(*peer.await, peer.tip.Point))
		snap.peers = append(snap.peers, peer)
	}
	// Map iteration order is random; sameAs compares snapshots positionally.
	slices.SortFunc(snap.peers, func(a, b gsmPeer) int {
		return cmp.Compare(a.connId.String(), b.connId.String())
	})
	return snap
}

// gsmFrontierLocked picks the frontier a connection's idleness is judged
// against: the admission frontier, unless the connection awaits at the tip of
// its delivered frontier instead. A connection demoted to observer keeps its
// last admitted header while its later headers reach only the delivered
// frontier, so the admission frontier alone would never match its await.
func (cs *ChainSelector) gsmFrontierLocked(
	connId ouroboros.ConnectionId,
	await *ocommon.Point,
) (*PeerChainTip, bool) {
	admitted := cs.peerTips[connId]
	delivered := cs.deliveredPeerTips[connId]
	if delivered == nil || !delivered.observedTipSet {
		return admitted, false
	}
	if admitted == nil {
		return delivered, true
	}
	if await != nil &&
		!pointsEqual(*await, admitted.SelectionTip().Point) &&
		pointsEqual(*await, delivered.SelectionTip().Point) {
		return delivered, true
	}
	return admitted, false
}

func (p gsmPeer) idlePoint() ocommon.Point {
	if p.haveTip {
		return p.tip.Point
	}
	if p.await != nil {
		return *p.await
	}
	return ocommon.Point{}
}

// historicDisconnects lists idle peers whose delivered tip is older than the
// cutoff. The tip is read from the peer's delivered headers, never from its
// advertised tip, which an idle peer can set freely.
func (cs *ChainSelector) historicDisconnects(
	snap gsmSnapshot,
) []HistoricityDisconnect {
	if cs.config.SlotTime == nil {
		return nil
	}
	var out []HistoricityDisconnect
	for _, peer := range snap.peers {
		if !peer.idle {
			continue
		}
		point := peer.idlePoint()
		slotTime, err := cs.config.SlotTime(point.Slot)
		if err != nil {
			continue
		}
		age := snap.now.Sub(slotTime)
		if age > snap.cutoff {
			out = append(out, HistoricityDisconnect{
				ConnectionId: peer.connId,
				Point:        clonePoint(point),
				SlotTime:     slotTime,
				Age:          age,
				Cutoff:       snap.cutoff,
			})
		}
	}
	return out
}

// onChain reports whether a point is on the applied chain.
func (cs *ChainSelector) onChain(point ocommon.Point) bool {
	if cs.config.PointOnLocalChain == nil || len(point.Hash) == 0 {
		return false
	}
	return cs.config.PointOnLocalChain(point)
}

// intersectsLocalChain reports whether a peer's delivered frontier shares a
// point with the applied chain. The points of one chain are a prefix on the
// applied chain followed by a suffix off it, so a binary search finds the
// boundary in logarithmically many lookups.
func (cs *ChainSelector) intersectsLocalChain(
	points []ocommon.Point,
) bool {
	if len(points) == 0 {
		return false
	}
	lo, hi := 0, len(points)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if cs.onChain(points[mid]) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo > 0
}

func (cs *ChainSelector) peerObservedPoints(
	connId ouroboros.ConnectionId,
	delivered bool,
) []ocommon.Point {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	tracked := cs.peerTips[connId]
	if delivered || tracked == nil {
		tracked = cs.deliveredPeerTips[connId]
	}
	if tracked == nil {
		return []ocommon.Point{}
	}
	points := make([]ocommon.Point, len(tracked.observedPoints))
	for i, point := range tracked.observedPoints {
		points[i] = clonePoint(point)
	}
	return points
}

// candidateNotBetter reports whether one peer's delivered chain is provably
// not better than the selection: it intersects the applied chain and does not
// win the Praos comparison. A peer on the applied chain is behind or equal.
func (cs *ChainSelector) candidateNotBetter(
	snap gsmSnapshot,
	peer gsmPeer,
	localView PraosTiebreakerView,
	haveLocalView bool,
) bool {
	point := peer.idlePoint()
	if cs.onChain(point) {
		return true
	}
	if !peer.haveTip || peer.awaitingFirst {
		return false
	}
	if !cs.intersectsLocalChain(
		cs.peerObservedPoints(peer.connId, peer.delivered),
	) {
		return false
	}
	// An equal-height fork wins or loses on the tiebreak alone, and a side
	// without a view compares equal, so either view missing blocks the exit.
	if peer.tip.BlockNumber == snap.localTip.BlockNumber &&
		(!haveLocalView || !praosViewKnown(peer.view)) {
		return false
	}
	return ComparePraosTips(
		peer.tip,
		snap.localTip,
		peer.view,
		localView,
	) != ChainABetter
}

// praosViewKnown reports whether a view carries any tiebreak input: a Byron
// block kind, an issuer, or a VRF output. A view cleared by a rollback, or
// never recorded for a delivered-only header, has none.
func praosViewKnown(v PraosTiebreakerView) bool {
	return v.Byron != praos.ByronBlockKindNone || len(v.Issuer) > 0 ||
		len(v.TieBreakVRF) > 0
}

// caughtUp applies the reference's exit rule to a snapshot: at least one
// connection, every connection idle, and no candidate better than the
// selection.
func (cs *ChainSelector) caughtUp(snap gsmSnapshot) bool {
	if len(snap.peers) == 0 || !snap.localTipKnown {
		return false
	}
	for _, peer := range snap.peers {
		if !peer.idle {
			return false
		}
	}
	var localView PraosTiebreakerView
	haveLocalView := false
	if cs.config.LocalTipPraosView != nil {
		localView, haveLocalView = cs.config.LocalTipPraosView(snap.localTip)
	}
	for _, peer := range snap.peers {
		if !cs.candidateNotBetter(snap, peer, localView, haveLocalView) {
			return false
		}
	}
	return true
}

// advanceGenesisStateMachine runs one step of the Genesis state machine and
// reports whether the mode changed. SlotTime, PointOnLocalChain and
// LocalTipPraosView run without the selector lock; AppliedTip,
// ChainSyncConnections and ConnectionLive are read under it, so none of them
// may call back into the selector.
func (cs *ChainSelector) advanceGenesisStateMachine() bool {
	if !cs.genesisEnabled {
		return false
	}
	cs.mutex.RLock()
	mode := cs.mode
	cs.mutex.RUnlock()
	if mode == SelectionModeGenesis {
		return cs.tryLeaveGenesis()
	}
	return cs.tryReenterGenesis()
}

func (cs *ChainSelector) tryLeaveGenesis() bool {
	cs.mutex.RLock()
	if cs.mode != SelectionModeGenesis {
		cs.mutex.RUnlock()
		return false
	}
	snap := cs.gsmSnapshotLocked()
	cs.mutex.RUnlock()

	historic := cs.historicDisconnects(snap)
	cs.reportHistoric(historic)
	if len(historic) > 0 || !cs.caughtUp(snap) {
		return false
	}

	// The unlock is deferred: leaveGenesisLocked logs, and a logger that
	// panics must not leave the selector locked.
	left := func() bool {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()
		if cs.mode != SelectionModeGenesis ||
			!snap.sameAs(cs.gsmSnapshotLocked()) {
			return false
		}
		cs.leaveGenesisLocked(snap.localTip.Point.Slot, len(snap.peers))
		return true
	}()
	if left {
		cs.publishPendingGenesisModeEvents()
	}
	return left
}

// reportHistoric hands each newly found historic peer to the composition
// layer once; the connection stays reported until it disappears.
func (cs *ChainSelector) reportHistoric(found []HistoricityDisconnect) {
	if cs.config.OnHistoricityDisconnect == nil {
		return
	}
	var fresh []HistoricityDisconnect
	cs.mutex.Lock()
	for _, d := range found {
		if _, done := cs.historicityReported[d.ConnectionId]; done {
			continue
		}
		cs.historicityReported[d.ConnectionId] = struct{}{}
		fresh = append(fresh, d)
	}
	cs.mutex.Unlock()
	for _, d := range fresh {
		cs.config.OnHistoricityDisconnect(d)
	}
}

// leaveGenesisLocked performs the Genesis to Praos transition at the applied
// tip slot the exit decision used. Per-peer frontiers are kept: they are
// bounded and let a later return to Genesis selection start with density,
// eagerness and corroboration data.
func (cs *ChainSelector) leaveGenesisLocked(localSlot uint64, peers int) {
	now := cs.now()
	window := cs.genesisWindowSlotsLocked()
	cs.mode = SelectionModePraos
	cs.refreshGenesisSelectionSnapshotLocked()
	cs.lastCorroborationFailedConn = nil
	cs.praosMinStayUntil = now.Add(cs.maxCaughtUpAge())
	cs.praosBonus = cs.drawBonus()
	cs.reentryTip = ocommon.Point{}
	cs.reentryTipSet = false
	cs.config.Logger.Info(
		"exiting Genesis selection mode",
		"local_slot", localSlot,
		"peers", peers,
		"genesis_window_slots", window,
	)
	if cs.config.EventBus != nil {
		cs.pendingGenesisExit = &GenesisModeExitedEvent{
			LocalSlot:          localSlot,
			Peers:              peers,
			GenesisWindowSlots: window,
		}
	}
}

func (cs *ChainSelector) drawBonus() time.Duration {
	draw := cs.config.RandomBonus
	if draw == nil {
		draw = defaultRandomBonus
	}
	bonus := draw(CaughtUpBonusMax)
	return min(max(bonus, 0), CaughtUpBonusMax)
}

func (cs *ChainSelector) tryReenterGenesis() bool {
	if cs.config.SlotTime == nil {
		return false
	}
	cs.mutex.RLock()
	if cs.mode != SelectionModePraos {
		cs.mutex.RUnlock()
		return false
	}
	now := cs.now()
	tip, known := cs.appliedLocalTipLocked()
	minStayUntil := cs.praosMinStayUntil
	bonus := cs.praosBonus
	deadlineTip, deadlineSet := cs.reentryTip, cs.reentryTipSet
	deadline := cs.reentryDeadline
	cs.mutex.RUnlock()

	if !known || now.Before(minStayUntil) {
		return false
	}
	if !deadlineSet || !pointsEqual(deadlineTip, tip.Point) {
		// The selection changed (or this is the first look after the minimum
		// stay): measure its age afresh. A tip that cannot be placed in time,
		// or that is already past the age limit, is too old at once without
		// the bonus; otherwise the bonus spreads the return.
		deadline = now
		origin := tip.Point.Slot == 0 && len(tip.Point.Hash) == 0
		if slotTime, err := cs.config.SlotTime(tip.Point.Slot); err == nil &&
			!origin {
			limit := slotTime.Add(cs.maxCaughtUpAge())
			if limit.After(now) {
				deadline = limit.Add(bonus)
			}
		}
		// SlotTime ran unlocked: a newer selection may have recorded its own
		// deadline meanwhile, which this stale one must not replace.
		cs.mutex.Lock()
		current := cs.reentryStillCurrentLocked(tip, minStayUntil, now)
		if current {
			cs.reentryTip = clonePoint(tip.Point)
			cs.reentryTipSet = true
			cs.reentryDeadline = deadline
		}
		cs.mutex.Unlock()
		if !current {
			return false
		}
	}
	if now.Before(deadline) {
		return false
	}

	entered := func() bool {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()
		if !cs.reentryStillCurrentLocked(tip, minStayUntil, now) ||
			!pointsEqual(cs.reentryTip, tip.Point) ||
			!cs.reentryDeadline.Equal(deadline) {
			return false
		}
		cs.enterGenesisLocked(tip, deadline)
		return true
	}()
	if entered {
		cs.publishPendingGenesisModeEvents()
	}
	return entered
}

// reentryStillCurrentLocked reports whether a re-entry decision taken for tip
// still describes the selector: still in the same Praos period, past its
// minimum stay, and with tip still the applied selection.
func (cs *ChainSelector) reentryStillCurrentLocked(
	tip ochainsync.Tip,
	minStayUntil time.Time,
	now time.Time,
) bool {
	if cs.mode != SelectionModePraos ||
		!cs.praosMinStayUntil.Equal(minStayUntil) ||
		now.Before(cs.praosMinStayUntil) {
		return false
	}
	live, known := cs.appliedLocalTipLocked()
	return known && pointsEqual(live.Point, tip.Point)
}

// enterGenesisLocked performs the Praos to Genesis transition. State derived
// from the previous Genesis period is discarded: the density disconnector's
// reported set and the eagerness anchor describe peers and a common point that
// no longer apply, while the per-peer frontiers they are computed from were
// kept.
func (cs *ChainSelector) enterGenesisLocked(
	tip ochainsync.Tip,
	deadline time.Time,
) {
	cs.mode = SelectionModeGenesis
	cs.refreshGenesisSelectionSnapshotLocked()
	clear(cs.genesisDensityDisconnected)
	clear(cs.historicityReported)
	cs.lastGenesisDensityEval = time.Time{}
	cs.lastCorroborationFailedConn = nil
	cs.eagernessAnchorMu.Lock()
	cs.eagernessAnchor = nil
	cs.eagernessAnchorMu.Unlock()
	cs.config.Logger.Info(
		"entering Genesis selection mode",
		"local_slot", tip.Point.Slot,
		"deadline", deadline,
		"genesis_window_slots", cs.genesisWindowSlotsLocked(),
	)
	if cs.config.EventBus != nil {
		cs.pendingGenesisEntry = &GenesisModeEnteredEvent{
			LocalSlot:          tip.Point.Slot,
			GenesisWindowSlots: cs.genesisWindowSlotsLocked(),
		}
	}
}

// publishPendingGenesisModeEvents publishes staged mode-change events outside
// the selector mutex.
func (cs *ChainSelector) publishPendingGenesisModeEvents() {
	cs.publishPendingGenesisExitEvent()
	cs.publishPendingGenesisEntryEvent()
}

// SetPeerIdling records an explicit ChainSync MsgAwaitReply at point for a
// connection, tracked or not. The record is kept until the connection delivers
// or rolls back, and is compared with the connection's delivered frontier when
// the exit rule is evaluated, so an await that precedes admission of the
// header it was sent at counts once that header is promoted and an await for
// an older point never does.
func (cs *ChainSelector) SetPeerIdling(
	connId ouroboros.ConnectionId,
	point ocommon.Point,
) {
	cs.mutex.Lock()
	cs.awaiting[connId] = clonePoint(point)
	if cs.config.ConnectionLive != nil {
		for other := range cs.awaiting {
			if other != connId && !cs.config.ConnectionLive(other) {
				delete(cs.awaiting, other)
			}
		}
	}
	peerTip := cs.peerTips[connId]
	matched := peerTip != nil &&
		peerTip.ObservedTip.Point.Slot == point.Slot &&
		bytes.Equal(peerTip.ObservedTip.Point.Hash, point.Hash)
	if matched {
		peerTip.idling = true
	}
	cs.mutex.Unlock()
	cs.EvaluateAndSwitch()
}

// ClearPeerIdling revokes a connection's idle proof when it sends a header at
// point: the peer is no longer idle at its previous frontier, although the
// header may take a while to reach admission. An await already recorded at
// point is kept.
func (cs *ChainSelector) ClearPeerIdling(
	connId ouroboros.ConnectionId,
	point ocommon.Point,
) {
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	if await, ok := cs.awaiting[connId]; ok && !pointsEqual(await, point) {
		delete(cs.awaiting, connId)
		if peerTip := cs.peerTips[connId]; peerTip != nil {
			peerTip.idling = false
		}
	}
}

// awaitMatchesLocked reports whether the connection's await point is point.
func (cs *ChainSelector) awaitMatchesLocked(
	connId ouroboros.ConnectionId,
	point ocommon.Point,
) bool {
	await, ok := cs.awaiting[connId]
	return ok && pointsEqual(await, point)
}
