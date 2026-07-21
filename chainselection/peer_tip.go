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
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// PeerChainTip tracks the chain tip reported by a specific peer.
type PeerChainTip struct {
	ConnectionId ouroboros.ConnectionId
	Tip          ochainsync.Tip
	ObservedTip  ochainsync.Tip
	VRFOutput    []byte // VRF output from tip block for tie-breaking
	PraosView    PraosTiebreakerView
	LastUpdated  time.Time
	// observedSlots is the recent observed slot frontier used for Genesis
	// density. observedPoints is the same frontier with block hashes, used
	// for Genesis corroboration (detecting whether other peers report the
	// same blocks). The two slices are maintained in lockstep: index i of
	// observedPoints is the (slot, hash) for observedSlots[i].
	observedSlots  []uint64
	observedPoints []ocommon.Point
}

// NewPeerChainTip creates a new PeerChainTip with the given connection ID,
// tip, and VRF output.
func NewPeerChainTip(
	connId ouroboros.ConnectionId,
	tip ochainsync.Tip,
	vrfOutput []byte,
) *PeerChainTip {
	return &PeerChainTip{
		ConnectionId: connId,
		Tip:          tip,
		ObservedTip:  tip,
		VRFOutput:    vrfOutput,
		PraosView: PraosTiebreakerViewFromTip(
			tip,
			vrfOutput,
			PraosTiebreakerConfigUnknown(),
		),
		LastUpdated: time.Now(),
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
	p.VRFOutput = vrfOutput
	p.PraosView = praosView
	p.LastUpdated = time.Now()
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
	p.Tip = tip
	p.ObservedTip = tip
	p.VRFOutput = nil
	p.PraosView = PraosTiebreakerView{}
	p.LastUpdated = time.Now()
	if point.Slot == 0 || len(p.observedSlots) == 0 {
		p.observedSlots = nil
		p.observedPoints = nil
		return
	}

	keepUntil := 0
	for keepUntil < len(p.observedSlots) &&
		p.observedSlots[keepUntil] <= point.Slot {
		keepUntil++
	}
	if keepUntil == 0 {
		p.observedSlots = nil
		p.observedPoints = nil
		return
	}
	p.observedSlots = p.observedSlots[:keepUntil]
	p.trimObservedPointsTo(keepUntil)
}

// trimObservedPointsTo keeps observedPoints aligned with observedSlots after a
// slot-frontier trim. observedPoints may be shorter than observedSlots for
// peers whose frontier predates hash tracking; only trim when it is at least
// as long.
func (p *PeerChainTip) trimObservedPointsTo(keepUntil int) {
	if keepUntil <= len(p.observedPoints) {
		p.observedPoints = p.observedPoints[:keepUntil]
	}
}

// recordObservedPoint records a (slot, hash) point into the observed frontier
// used for Genesis density (observedSlots, always maintained in Genesis) and
// corroboration (observedPoints, the block hashes), keeping the two in lockstep
// and bounded to the density window.
//
// trackHashes gates the hash frontier: it is stored only while Genesis
// corroboration is active. When false the hash frontier is dropped, so normal
// Praos operation does not retain per-peer window-length hash history.
func (p *PeerChainTip) recordObservedPoint(
	point ocommon.Point,
	window uint64,
	trackHashes bool,
) {
	if p == nil {
		return
	}
	if !trackHashes {
		p.observedPoints = nil
	}
	slot := point.Slot
	if slot == 0 {
		p.observedSlots = nil
		p.observedPoints = nil
		return
	}

	// Keep the history monotonic and bounded even if the observed frontier
	// rolls back. Genesis mode only needs the recent slot frontier, not the
	// full chain history.
	for len(p.observedSlots) > 0 &&
		p.observedSlots[len(p.observedSlots)-1] > slot {
		p.observedSlots = p.observedSlots[:len(p.observedSlots)-1]
		if trackHashes {
			p.trimObservedPointsTo(len(p.observedSlots))
		}
	}
	switch {
	case len(p.observedSlots) == 0 ||
		p.observedSlots[len(p.observedSlots)-1] < slot:
		p.observedSlots = append(p.observedSlots, slot)
		if trackHashes {
			p.observedPoints = append(p.observedPoints, clonePoint(point))
		}
	case trackHashes && len(p.observedPoints) == len(p.observedSlots):
		// Same slot re-reported: keep the latest hash so corroboration
		// compares against the current frontier block.
		p.observedPoints[len(p.observedPoints)-1] = clonePoint(point)
	}

	if window == 0 {
		if len(p.observedSlots) > 1 {
			p.observedSlots = p.observedSlots[len(p.observedSlots)-1:]
		}
		if len(p.observedPoints) > 1 {
			p.observedPoints = p.observedPoints[len(p.observedPoints)-1:]
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
		if pruneIdx <= len(p.observedPoints) {
			p.observedPoints = p.observedPoints[pruneIdx:]
		} else {
			p.observedPoints = nil
		}
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

// confirmsRecentChain reports whether witness confirms this peer's (candidate's)
// chain across the window range they overlap. It is true when, for every block
// the witness observed within the candidate's frontier slot range, the candidate
// observed the identical (slot, hash) block, AND they share at least one such
// block. In other words the witness's chain, as far as it reaches into the
// candidate's window, is a prefix/subset of the candidate's chain.
//
// This is deliberately stronger than "share any common point": a fast source
// that shares only an old ancestor and then diverges for every later block is
// NOT confirmed, because the witness observed recent blocks the candidate did
// not (or a conflicting hash at the same slot). A witness whose frontier does
// not overlap the candidate's window at all cannot confirm it (returns false),
// so corroboration fails closed — the candidate then stalls rather than being
// followed uncorroborated.
//
// Both frontiers are kept in strictly-ascending slot order; this is a
// two-pointer scan. It relies on the observed frontier being populated per
// header (dense) during chainsync, so two peers on the same chain share every
// block in their overlap.
func (p *PeerChainTip) confirmsRecentChain(witness *PeerChainTip) bool {
	if p == nil || witness == nil ||
		len(p.observedPoints) == 0 || len(witness.observedPoints) == 0 {
		return false
	}
	candidate := p.observedPoints
	lo := candidate[0].Slot
	hi := candidate[len(candidate)-1].Slot
	i := 0
	hadMatch := false
	for _, w := range witness.observedPoints {
		if w.Slot < lo {
			continue
		}
		if w.Slot > hi {
			break
		}
		for i < len(candidate) && candidate[i].Slot < w.Slot {
			i++
		}
		if i < len(candidate) && candidate[i].Slot == w.Slot &&
			len(w.Hash) > 0 && bytes.Equal(candidate[i].Hash, w.Hash) {
			hadMatch = true
			continue
		}
		// The witness observed a block within the candidate's range that the
		// candidate does not have (or a conflicting hash at the same slot):
		// they are on different chains, so this witness does not confirm.
		return false
	}
	return hadMatch
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
	if p.ObservedTip.BlockNumber > 0 || p.ObservedTip.Point.Slot > 0 ||
		len(p.ObservedTip.Point.Hash) > 0 {
		return p.ObservedTip
	}
	return p.Tip
}

// Touch marks the peer as recently active without changing its advertised tip.
func (p *PeerChainTip) Touch() {
	p.LastUpdated = time.Now()
}

// IsStale returns true if the peer's tip hasn't been updated within the given
// duration.
func (p *PeerChainTip) IsStale(threshold time.Duration) bool {
	return time.Since(p.LastUpdated) > threshold
}
