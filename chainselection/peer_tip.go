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
	"slices"
	"sort"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// PeerChainTip tracks the chain tip reported by a specific peer.
type PeerChainTip struct {
	ConnectionId ouroboros.ConnectionId
	// Tip is the remote peer's advertised chain tip. It is untrusted and may
	// be far ahead of the headers the peer has actually delivered.
	Tip ochainsync.Tip
	// ObservedTip is the latest header locally delivered by this peer. Chain
	// comparison and handoff decisions use this frontier.
	ObservedTip ochainsync.Tip
	// observedTipSet distinguishes a delivered origin/rollback frontier from
	// legacy callers that did not provide ObservedTip and need Tip as fallback.
	observedTipSet bool
	// awaitingFirstHeader marks an entry created from a chainsync rollback for
	// a connection the selector was not tracking (the post-FindIntersect
	// MsgRollBackward on a fresh or recycled connection). Such a peer has
	// confirmed an intersection but has not delivered a header yet, so its
	// ObservedTip carries the confirmed intersection point with block number 0.
	// That zero means "nothing delivered yet", NOT "this peer is at block 0",
	// and the two behind-filters in isPeerSelectableLocked would otherwise read
	// it as a peer implausibly far behind and skip it until its next
	// MsgRollForward. Cleared by the first delivered header.
	awaitingFirstHeader bool
	// eagernessPaused counts AwaitEagernessLimit calls holding this peer's
	// header stream at the Limit on Eagerness. Guarded by the selector mutex.
	eagernessPaused int
	VRFOutput       []byte // VRF output from tip block for tie-breaking
	PraosView       PraosTiebreakerView
	LastUpdated     time.Time
	// observedSlots is the recent slot-window frontier used for Genesis
	// density. observedPoints is the entry-bounded hash frontier used for
	// corroboration and GDD; it may retain older sparse points so a common
	// intersection is not lost merely because a peer advanced past the window.
	observedSlots  []uint64
	observedPoints []ocommon.Point
	// observedPointBaseBlock is the block number of observedPoints[0]. The
	// remaining entries are consecutive block numbers, which lets density
	// comparisons align two frontiers without scanning a full Genesis window.
	observedPointBaseBlock uint64
	observedTipHistory     []ochainsync.Tip
	idling                 bool
	// admittedTipHistory retains only points a ledger admission explicitly
	// accepted. It is separate from observedTipHistory because legacy rollback
	// and recovery observations are not proof of ledger admission.
	admittedTipHistory []ocommon.Point
	// nowFn is the owning ChainSelector's clock, so LastUpdated and IsStale
	// share one time source with the selector. Nil for a PeerChainTip built
	// by NewPeerChainTip, which then uses time.Now.
	nowFn func() time.Time
}

// now returns nowFn(), or time.Now when nowFn is unset.
func (p *PeerChainTip) now() time.Time {
	if p.nowFn != nil {
		return p.nowFn()
	}
	return time.Now()
}

// NewPeerChainTip creates a new PeerChainTip with the given connection ID,
// tip, and VRF output.
func NewPeerChainTip(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	vrfOutput []byte,
) *PeerChainTip {
	return &PeerChainTip{
		ConnectionId:   connId,
		Tip:            tip,
		ObservedTip:    tip,
		observedTipSet: true,
		VRFOutput:      vrfOutput,
		PraosView: PraosTiebreakerViewFromTip(
			tip,
			vrfOutput,
			PraosTiebreakerConfigUnknown(),
		),
		LastUpdated: time.Now(),
	}
}

// newPeerChainTipFromRollback creates the tracked tip for a peer that reported
// a chainsync rollback while the selector had no entry for its connection. The
// canonical case is the post-FindIntersect MsgRollBackward that a server sends
// on a fresh connection: it is the only chainsync traffic until the next block
// is minted, so a peer whose entry was dropped by a connection recycle would
// otherwise stay invisible to chain selection for a whole block interval.
//
// The entry deliberately records only what the exchange proved:
//   - Tip is the peer's advertised tip, untrusted exactly as on roll forward.
//   - ObservedTip is the intersection point the peer confirmed it holds, with
//     block number 0 because no header has been delivered. It is never the
//     advertised tip (mirroring ApplyRollback, which refuses that promotion).
//   - No observed slot/point frontier is recorded, so the peer contributes no
//     Genesis density and cannot corroborate another peer until it delivers
//     headers.
//
// awaitingFirstHeader marks the zero block number as "unknown" rather than
// "behind"; the first delivered header clears it and the peer is compared on
// its real frontier from then on.
func newPeerChainTipFromRollback(
	connId ouroboros.ConnectionId,
	point ocommon.Point,
	tip ochainsync.Tip,
) *PeerChainTip {
	return &PeerChainTip{
		ConnectionId:        connId,
		Tip:                 tip,
		ObservedTip:         ochainsync.Tip{Point: clonePoint(point)},
		observedTipSet:      true,
		awaitingFirstHeader: true,
		LastUpdated:         time.Now(),
	}
}

// UpdateTip updates the peer's chain tip, VRF output, and last updated timestamp.
func (p *PeerChainTip) UpdateTip(tip ochainsync.Tip, vrfOutput []byte) {
	p.UpdateTipWithObserved(tip, tip, vrfOutput)
}

// UpdateTipWithObserved updates both the remote advertised tip and the latest
// locally observed frontier for the peer.
func (p *PeerChainTip) UpdateTipWithObserved(
	tip ochainsync.Tip,
	observedTip ochainsync.Tip,
	vrfOutput []byte,
) {
	p.UpdateTipWithObservedPraosView(
		tip,
		observedTip,
		vrfOutput,
		PraosTiebreakerViewFromTip(
			observedTip,
			vrfOutput,
			PraosTiebreakerConfigUnknown(),
		),
	)
}

// UpdateTipWithObservedPraosView updates the remote advertised tip, the latest
// locally observed frontier, the VRF output, and the Praos tiebreaker view for
// the peer. Callers must provide a PraosTiebreakerView derived from observedTip
// and the supplied vrfOutput when that VRF output participates in the view.
// The view is stored as supplied; this method does not validate consistency, so
// an inconsistent view can make later chain-selection comparisons incorrect.
func (p *PeerChainTip) UpdateTipWithObservedPraosView(
	tip ochainsync.Tip,
	observedTip ochainsync.Tip,
	vrfOutput []byte,
	praosView PraosTiebreakerView,
) {
	p.Tip = tip
	p.ObservedTip = observedTip
	p.observedTipSet = true
	p.awaitingFirstHeader = false
	p.idling = false
	p.VRFOutput = vrfOutput
	p.PraosView = praosView
	p.LastUpdated = p.now()
}

// ApplyRollback trims observed history at the rollback point and refreshes the
// peer tip to the chainsync tip reported with the rollback.
func (p *PeerChainTip) ApplyRollback(
	point ocommon.Point,
	tip ochainsync.Tip,
) {
	if p == nil {
		return
	}
	p.trimAdmittedTipHistory(point)
	previousObservedTip := p.ObservedTip
	p.Tip = tip
	p.ObservedTip = ochainsync.Tip{Point: clonePoint(point)}
	p.observedTipSet = true
	p.idling = false
	if point.Slot == 0 && len(point.Hash) == 0 {
		p.observedTipHistory = nil
	} else {
		found := false
		if previousObservedTip.Point.Slot == point.Slot &&
			bytes.Equal(previousObservedTip.Point.Hash, point.Hash) {
			p.ObservedTip = cloneObservedTip(previousObservedTip)
			found = true
		}
		for i := len(p.observedTipHistory) - 1; !found && i >= 0; i-- {
			historyTip := p.observedTipHistory[i]
			if historyTip.Point.Slot == point.Slot &&
				bytes.Equal(historyTip.Point.Hash, point.Hash) {
				p.ObservedTip = cloneObservedTip(historyTip)
				p.observedTipHistory = p.observedTipHistory[:i+1]
				found = true
				break
			}
		}
		if !found {
			// RollBackward carries only a point, not its block number. If the
			// point is outside the bounded delivered-header history, retain the
			// point with a conservative zero block number instead of promoting
			// the peer's untrusted advertised tip.
			p.observedTipHistory = nil
		}
	}
	p.VRFOutput = nil
	p.PraosView = PraosTiebreakerView{}
	p.LastUpdated = p.now()
	if point.Slot == 0 && len(point.Hash) == 0 {
		p.observedSlots = nil
		p.observedPoints = nil
		p.observedPointBaseBlock = 0
		return
	}
	keepSlots := sort.Search(len(p.observedSlots), func(i int) bool {
		return p.observedSlots[i] > point.Slot
	})
	p.observedSlots = p.observedSlots[:keepSlots]
	pointIdx := sort.Search(len(p.observedPoints), func(i int) bool {
		return p.observedPoints[i].Slot >= point.Slot
	})
	keepPoints := 0
	pointMatched := pointIdx < len(p.observedPoints) &&
		p.observedPoints[pointIdx].Slot == point.Slot &&
		bytes.Equal(p.observedPoints[pointIdx].Hash, point.Hash)
	if pointMatched {
		keepPoints = pointIdx + 1
	} else if len(p.observedPoints) > 0 {
		// A rollback outside the retained candidate fragment gives no proof
		// that the old slot frontier belongs to the new chain.
		p.observedSlots = nil
		keepSlots = 0
	}
	clear(p.observedPoints[keepPoints:])
	p.observedPoints = p.observedPoints[:keepPoints]
	if keepSlots == 0 {
		p.observedSlots = nil
	}
	if keepPoints == 0 {
		p.observedPoints = nil
		p.observedPointBaseBlock = 0
	}
}

func (p *PeerChainTip) recordAdmittedTipPoint(
	point ocommon.Point,
	maxEntries int,
) {
	if p == nil {
		return
	}
	for len(p.admittedTipHistory) > 0 &&
		p.admittedTipHistory[len(p.admittedTipHistory)-1].Slot >= point.Slot {
		p.admittedTipHistory = p.admittedTipHistory[:len(p.admittedTipHistory)-1]
	}
	p.admittedTipHistory = append(
		p.admittedTipHistory,
		clonePoint(point),
	)
	if maxEntries <= 0 {
		maxEntries = 1
	}
	if len(p.admittedTipHistory) > maxEntries {
		p.admittedTipHistory = p.admittedTipHistory[len(p.admittedTipHistory)-maxEntries:]
	}
}

func (p *PeerChainTip) trimAdmittedTipHistory(point ocommon.Point) {
	if p == nil {
		return
	}
	if point.Slot == 0 && len(point.Hash) == 0 {
		p.admittedTipHistory = nil
		return
	}
	for i := range slices.Backward(p.admittedTipHistory) {
		admitted := p.admittedTipHistory[i]
		if admitted.Slot == point.Slot && bytes.Equal(admitted.Hash, point.Hash) {
			p.admittedTipHistory = p.admittedTipHistory[:i+1]
			return
		}
	}
	p.admittedTipHistory = nil
}

// recordObservedTipHistory retains enough delivered tips to restore the exact
// observed frontier on a protocol rollback. maxEntries is expressed in blocks;
// callers retain k+1 entries so any valid rollback within k can be resolved.
func (p *PeerChainTip) recordObservedTipHistory(
	tip ochainsync.Tip,
	maxEntries uint64,
) {
	if p == nil {
		return
	}
	if maxEntries == 0 {
		maxEntries = 1
	}
	p.observedTipSet = true
	if tip.Point.Slot == 0 && len(tip.Point.Hash) == 0 {
		p.observedTipHistory = nil
		return
	}
	for len(p.observedTipHistory) > 0 &&
		p.observedTipHistory[len(p.observedTipHistory)-1].Point.Slot >=
			tip.Point.Slot {
		p.observedTipHistory = p.observedTipHistory[:len(p.observedTipHistory)-1]
	}
	p.observedTipHistory = append(
		p.observedTipHistory,
		cloneObservedTip(tip),
	)
	// #nosec G115 -- a slice length is non-negative and always fits uint64.
	for maxEntries > 0 &&
		uint64(len(p.observedTipHistory)) > maxEntries {
		p.observedTipHistory = p.observedTipHistory[1:]
	}
}

// isIdling reports whether this peer explicitly sent MsgAwaitReply after its
// last delivered header.
func (p *PeerChainTip) isIdling() bool { return p != nil && p.idling }

// recordObservedPoint records a (slot, hash) point into the observed frontier
// used for Genesis density (observedSlots) and corroboration/GDD
// (observedPoints). Slots are bounded by the density window; hash-bearing
// points are independently bounded by entry count so sparse candidates retain
// their common intersection. The point frontier keeps consecutive block
// heights; a gap starts a new frontier so density comparison fails undecidable
// instead of assuming missing observations are adjacent.
//
// trackHashes gates the hash frontier: it is stored only in Genesis mode.
// When false the hash frontier is dropped, so normal
// Praos operation does not retain per-peer window-length hash history.
func (p *PeerChainTip) recordObservedPoint(
	point ocommon.Point,
	blockNumber uint64,
	window uint64,
	maxPoints uint64,
	trackHashes bool,
) {
	if p == nil {
		return
	}
	if !trackHashes {
		p.observedPoints = nil
		p.observedPointBaseBlock = 0
	} else if maxPoints == 0 {
		maxPoints = 1
	}
	slot := point.Slot
	// The origin carries no hash. A block at slot 0 does, and is a delivered
	// point like any other.
	if slot == 0 && len(point.Hash) == 0 {
		p.observedSlots = nil
		p.observedPoints = nil
		p.observedPointBaseBlock = 0
		return
	}

	// Keep the history monotonic and bounded even if the observed frontier
	// rolls back. Genesis mode only needs the recent slot frontier, not the
	// full chain history.
	for len(p.observedSlots) > 0 &&
		p.observedSlots[len(p.observedSlots)-1] > slot {
		p.observedSlots = p.observedSlots[:len(p.observedSlots)-1]
	}
	switch {
	case len(p.observedSlots) == 0 ||
		p.observedSlots[len(p.observedSlots)-1] < slot:
		p.observedSlots = append(p.observedSlots, slot)
	}
	if trackHashes {
		var tailBlock uint64
		if len(p.observedPoints) > 0 {
			// #nosec G115 -- the retained slice is bounded by maxPoints.
			tailBlock = p.observedPointBaseBlock +
				uint64(len(p.observedPoints)-1)
		}
		switch {
		case len(p.observedPoints) == 0:
			p.observedPointBaseBlock = blockNumber
			p.observedPoints = append(p.observedPoints, clonePoint(point))
		case p.observedPoints[len(p.observedPoints)-1].Slot == slot &&
			blockNumber == tailBlock:
			p.observedPoints[len(p.observedPoints)-1] = clonePoint(point)
		case p.observedPoints[len(p.observedPoints)-1].Slot < slot &&
			blockNumber > p.observedPointBaseBlock &&
			blockNumber-p.observedPointBaseBlock ==
				uint64(len(p.observedPoints)):
			p.observedPoints = append(p.observedPoints, clonePoint(point))
		default:
			// A height gap or non-tail replacement cannot be represented by one
			// base height. Start a new contiguous frontier so intersection is
			// undecidable until enough linked headers have been observed again.
			clear(p.observedPoints)
			p.observedPoints = p.observedPoints[:0]
			p.observedPointBaseBlock = blockNumber
			p.observedPoints = append(p.observedPoints, clonePoint(point))
		}
		if uint64(len(p.observedPoints)) > maxPoints {
			excess := uint64(len(p.observedPoints)) - maxPoints
			clear(p.observedPoints[:excess])
			p.observedPoints = p.observedPoints[excess:]
			p.observedPointBaseBlock += excess
		}
	}

	if window == 0 {
		if len(p.observedSlots) > 1 {
			p.observedSlots = p.observedSlots[len(p.observedSlots)-1:]
		}
		return
	}

	var cutoff uint64
	if slot > window {
		cutoff = slot - window + 1
	} else {
		cutoff = 1
	}
	pruneIdx := 0
	for pruneIdx < len(p.observedSlots) &&
		p.observedSlots[pruneIdx] < cutoff {
		pruneIdx++
	}
	if pruneIdx > 0 {
		p.observedSlots = p.observedSlots[pruneIdx:]
	}
}

// clonePoint returns a copy of point with its own hash backing array so the
// stored frontier does not alias the caller's chainsync buffers.
func clonePoint(point ocommon.Point) ocommon.Point {
	if len(point.Hash) == 0 {
		return ocommon.Point{Slot: point.Slot}
	}
	hash := make([]byte, len(point.Hash))
	copy(hash, point.Hash)
	return ocommon.Point{Slot: point.Slot, Hash: hash}
}

func cloneObservedTip(tip ochainsync.Tip) ochainsync.Tip {
	return ochainsync.Tip{
		Point:       clonePoint(tip.Point),
		BlockNumber: tip.BlockNumber,
	}
}

// cloneObservedPoints deep-copies an observed-point frontier, including each
// point's hash backing array, for use by the selector's deep-copy getters.
func cloneObservedPoints(points []ocommon.Point) []ocommon.Point {
	if len(points) == 0 {
		return nil
	}
	out := make([]ocommon.Point, len(points))
	for i, pt := range points {
		out[i] = clonePoint(pt)
	}
	return out
}

// observedHistoryConflictsAt reports whether this peer's recorded delivered
// header history contains a DIFFERENT block at point's slot.
//
// It is deliberately one-sided. A false result means "no contradiction found",
// which includes the common case where the slot is outside the retained
// history window (k+1 delivered tips) and therefore cannot be checked at all.
// Only a true result is evidence, and it is evidence of divergence: the two
// peers delivered conflicting blocks at the same slot, so they are not serving
// the same chain regardless of what they advertise.
func (p *PeerChainTip) observedHistoryConflictsAt(point ocommon.Point) bool {
	if p == nil || len(point.Hash) == 0 {
		return false
	}
	for _, historyTip := range p.observedTipHistory {
		if historyTip.Point.Slot != point.Slot {
			continue
		}
		return !bytes.Equal(historyTip.Point.Hash, point.Hash)
	}
	return false
}

// confirmsRecentChain reports whether witness confirms this peer's (candidate's)
// chain. Within the slot range of the candidate's observed frontier, every
// witness-observed point must match the candidate, and the witness must have
// independently delivered the candidate's current point.
//
// This is deliberately stronger than "share any common point": a fast source
// that shares only an old ancestor and then diverges for every later block is
// NOT confirmed, because the witness observed recent blocks the candidate did
// not (or a conflicting hash at the same slot). A witness whose frontier does
// not overlap the candidate's window at all cannot confirm it (returns false),
// so corroboration fails closed — the candidate then stalls rather than being
// followed uncorroborated. Requiring the current point is what keeps a match
// in older candidate history, however recent, from authorizing candidate
// blocks the witness has never observed.
//
// Both frontiers are kept in strictly-ascending slot order; this is a
// two-pointer scan that starts the witness at the candidate's first slot. It
// relies on the observed frontier being populated per header (dense) during
// chainsync, so two peers on the same chain share every block in their
// overlap.
func (p *PeerChainTip) confirmsRecentChainWithin(
	witness *PeerChainTip,
	window uint64,
) bool {
	if p == nil || witness == nil ||
		len(p.observedPoints) == 0 || len(witness.observedPoints) == 0 {
		return false
	}
	candidate := p.observedPoints
	if window > 0 {
		hi := candidate[len(candidate)-1].Slot
		var cutoff uint64
		if hi >= window {
			cutoff = hi - window + 1
		}
		candidate = candidate[sort.Search(len(candidate), func(i int) bool {
			return candidate[i].Slot >= cutoff
		}):]
	}
	lo := candidate[0].Slot
	hi := candidate[len(candidate)-1].Slot
	witnessPoints := witness.observedPoints
	witnessPoints = witnessPoints[sort.Search(len(witnessPoints), func(i int) bool {
		return witnessPoints[i].Slot >= lo
	}):]
	i := 0
	tipMatched := false
	for _, w := range witnessPoints {
		if w.Slot > hi {
			break
		}
		for i < len(candidate) && candidate[i].Slot < w.Slot {
			i++
		}
		if i < len(candidate) && candidate[i].Slot == w.Slot &&
			len(w.Hash) > 0 && bytes.Equal(candidate[i].Hash, w.Hash) {
			tipMatched = i == len(candidate)-1
			continue
		}
		// The witness observed a block within the candidate's range that the
		// candidate does not have (or a conflicting hash at the same slot):
		// they are on different chains, so this witness does not confirm.
		return false
	}
	return tipMatched
}

func (p *PeerChainTip) observedDensity(window uint64) uint64 {
	if p == nil || len(p.observedSlots) == 0 {
		return 0
	}
	if window == 0 {
		return uint64(len(p.observedSlots))
	}

	latestSlot := p.observedSlots[len(p.observedSlots)-1]
	var cutoff uint64
	if latestSlot > window {
		cutoff = latestSlot - window + 1
	} else {
		cutoff = 1
	}
	for i, slot := range p.observedSlots {
		if slot >= cutoff {
			// #nosec G115 -- i < len(p.observedSlots), difference is non-negative
			return uint64(len(p.observedSlots) - i)
		}
	}
	return 0
}

// SelectionTip returns the best locally observed frontier for this peer.
// When available, prefer the latest block the peer has actually delivered to
// us over its remote advertised tip. This avoids switching to peers whose
// far-end tip is high while their chainsync cursor is still lagging.
func (p *PeerChainTip) SelectionTip() ochainsync.Tip {
	if p == nil {
		return ochainsync.Tip{}
	}
	if p.observedTipSet || p.ObservedTip.BlockNumber > 0 ||
		p.ObservedTip.Point.Slot > 0 ||
		len(p.ObservedTip.Point.Hash) > 0 {
		return p.ObservedTip
	}
	return p.Tip
}

// AwaitingFirstHeader reports whether chain selection registered this peer
// from a chainsync rollback (newPeerChainTipFromRollback, the post-FindIntersect
// MsgRollBackward on a connection it was not tracking) and the peer has not
// delivered a header since. For such a peer SelectionTip is the point its
// session intersected at, carrying no block number, and it does not move until
// the peer's first RollForward arrives: it is evidence of the intersection and
// nothing else. Callers that reason about how far a peer has got need to tell
// that apart from a delivered frontier.
//
// It reads the awaitingFirstHeader flag rather than testing for a zero
// delivered block number, because the two are not the same property. A tracked
// peer that has delivered headers and then rolls back to a point outside its
// retained delivered-header history is also left with a zero block number
// (ApplyRollback keeps the point and cannot recover a block number for it),
// but it has delivered headers on this connection and is not awaiting its
// first one. So is a peer whose delivered frontier genuinely is origin.
func (p *PeerChainTip) AwaitingFirstHeader() bool {
	if p == nil {
		return false
	}
	return p.awaitingFirstHeader
}

// Touch marks the peer as recently active without changing its advertised tip.
func (p *PeerChainTip) Touch() {
	p.LastUpdated = p.now()
}

// IsStale returns true if the peer's tip hasn't been updated within the given
// duration.
func (p *PeerChainTip) IsStale(threshold time.Duration) bool {
	return p.now().Sub(p.LastUpdated) > threshold
}
