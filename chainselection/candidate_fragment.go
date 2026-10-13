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
	"sort"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// CandidateFragment is a bounded, immutable snapshot of the chain fragment a
// chainsync-eligible peer has actually delivered, ordered from its anchor
// (the oldest retained point) to its most recently delivered point.
//
// This is Dingo's analogue of the upstream consensus interface
// readCandidateChains :: STM m (Map peer (AnchoredFragment header)); see
// ARCHITECTURE.md's "Ouroboros Genesis trust model" section. As with the
// upstream contract, entries are derived only from headers that have already
// passed chain-selection observation (see
// LedgerState.ValidateChainSelectionHeaderCrypto in ARCHITECTURE.md), the
// fragment may hold less than a full window, and its anchor is not required to
// intersect the primary chain or any other peer's fragment.
//
// The fragment snapshots PeerChainTip's hash-bearing Genesis frontier. That
// frontier has capacity for one Genesis window plus the k+1-header eagerness
// suffix.
// The separate k+1 Tip history stays small and serves rollback restoration.
// Entries are kept in strictly ascending slot order.
type CandidateFragment struct {
	points    []ocommon.Point
	baseBlock uint64
}

// Len returns the number of points retained in the fragment.
func (f CandidateFragment) Len() int {
	return len(f.points)
}

// Anchor returns the fragment's oldest retained point. Per the upstream
// AnchoredFragment contract, the anchor is not guaranteed to intersect the
// primary chain or any other peer's fragment — it is simply the oldest point
// this snapshot still remembers. Anchor returns the zero Point when the
// fragment is empty.
func (f CandidateFragment) Anchor() ocommon.Point {
	if len(f.points) == 0 {
		return ocommon.Point{}
	}
	return clonePoint(f.points[0])
}

// HeadPoint returns the fragment's most recently delivered point, or the
// zero Point when the fragment is empty.
func (f CandidateFragment) HeadPoint() ocommon.Point {
	if len(f.points) == 0 {
		return ocommon.Point{}
	}
	return clonePoint(f.points[len(f.points)-1])
}

// Points returns the fragment's retained points, including the anchor, in
// ascending slot order. The caller receives an independent copy.
func (f CandidateFragment) Points() []ocommon.Point {
	if len(f.points) == 0 {
		return nil
	}
	out := make([]ocommon.Point, len(f.points))
	for i, point := range f.points {
		out[i] = clonePoint(point)
	}
	return out
}

// Intersect returns the highest point present in both fragments, comparing
// by (slot, hash). Both fragments must already be in ascending slot order,
// which recordObservedPoint guarantees. This is the primitive the Limit
// on Eagerness and the Genesis Density Disconnector need to compute the
// intersection across candidate fragments (see ARCHITECTURE.md); it does not
// itself implement either.
func (f CandidateFragment) Intersect(
	other CandidateFragment,
) (ocommon.Point, bool) {
	point, ok := intersectPointFrontiers(
		f.points,
		f.baseBlock,
		other.points,
		other.baseBlock,
	)
	if !ok {
		return ocommon.Point{}, false
	}
	return clonePoint(point), true
}

// intersectTip is Intersect with the common point's block number. Candidate
// fragments store consecutive block heights, so the common point's offset in
// either aligned frontier determines its height without scanning the fragment.
func (f CandidateFragment) intersectTip(
	other CandidateFragment,
) (ochainsync.Tip, bool) {
	point, ok := intersectPointFrontiers(
		f.points,
		f.baseBlock,
		other.points,
		other.baseBlock,
	)
	if !ok {
		return ochainsync.Tip{}, false
	}
	idx := sort.Search(len(f.points), func(i int) bool {
		return f.points[i].Slot >= point.Slot
	})
	// #nosec G115 -- idx is bounded by the retained fragment length.
	return ochainsync.Tip{
		Point:       clonePoint(point),
		BlockNumber: f.baseBlock + uint64(idx),
	}, true
}

// intersectPointFrontiers returns the highest common point in two contiguous
// block-height frontiers. Valid chains cannot rejoin after a fork, so equality
// is a monotone prefix once the retained height ranges are aligned. This keeps
// a full-window density pass logarithmic per pair instead of scanning every
// point while the selector lock is held.
//
// The returned point aliases the first input. Callers that expose it must clone
// it; selector-internal callers hold the selector lock for both inputs.
func intersectPointFrontiers(
	a []ocommon.Point,
	aBase uint64,
	b []ocommon.Point,
	bBase uint64,
) (ocommon.Point, bool) {
	if len(a) == 0 || len(b) == 0 {
		return ocommon.Point{}, false
	}
	// #nosec G115 -- retained slice lengths are bounded by maxPoints and fit uint64.
	aEnd := aBase + uint64(len(a)-1)
	// #nosec G115 -- retained slice lengths are bounded by maxPoints and fit uint64.
	bEnd := bBase + uint64(len(b)-1)
	start := max(aBase, bBase)
	end := min(aEnd, bEnd)
	if start > end {
		return ocommon.Point{}, false
	}
	// #nosec G115 -- aligned ranges are subsets of the input slices.
	aStart := int(start - aBase)
	// #nosec G115 -- aligned ranges are subsets of the input slices.
	bStart := int(start - bBase)
	// #nosec G115 -- the overlap cannot exceed either input slice length.
	count := int(end-start) + 1
	equalAt := func(offset int) bool {
		pointA := a[aStart+offset]
		pointB := b[bStart+offset]
		return pointA.Slot == pointB.Slot && bytes.Equal(pointA.Hash, pointB.Hash)
	}
	if !equalAt(0) {
		return ocommon.Point{}, false
	}
	firstDifferent := sort.Search(count, func(offset int) bool {
		return !equalAt(offset)
	})
	return a[aStart+firstDifferent-1], true
}

// fragmentIntersection returns the highest point two peers' delivered
// fragments share, or nil when either peer is nil or they share none. Callers
// hold the selector lock, so the histories do not need to be copied.
func fragmentIntersection(a, b *PeerChainTip) *ocommon.Point {
	if a == nil || b == nil {
		return nil
	}
	i := len(a.observedTipHistory) - 1
	j := len(b.observedTipHistory) - 1
	for i >= 0 && j >= 0 {
		pointA := a.observedTipHistory[i].Point
		pointB := b.observedTipHistory[j].Point
		switch {
		case pointA.Slot == pointB.Slot:
			if bytes.Equal(pointA.Hash, pointB.Hash) {
				cloned := clonePoint(pointA)
				return &cloned
			}
			i--
			j--
		case pointA.Slot > pointB.Slot:
			i--
		default:
			j--
		}
	}
	return nil
}

// candidateFragmentFromPoints snapshots the hash-bearing Genesis frontier
// without duplicating it in the k-bounded rollback history.
func candidateFragmentFromPoints(
	points []ocommon.Point,
	baseBlock uint64,
) CandidateFragment {
	if len(points) == 0 {
		return CandidateFragment{}
	}
	entries := make([]ocommon.Point, len(points))
	for i, point := range points {
		entries[i] = clonePoint(point)
	}
	return CandidateFragment{points: entries, baseBlock: baseBlock}
}

func candidateFragmentForPeer(peerTip *PeerChainTip) CandidateFragment {
	if peerTip == nil {
		return CandidateFragment{}
	}
	if len(peerTip.observedPoints) > 0 {
		return candidateFragmentFromPoints(
			peerTip.observedPoints,
			peerTip.observedPointBaseBlock,
		)
	}
	if len(peerTip.observedTipHistory) == 0 {
		return CandidateFragment{}
	}
	points := make([]ocommon.Point, len(peerTip.observedTipHistory))
	for i, tip := range peerTip.observedTipHistory {
		points[i] = clonePoint(tip.Point)
	}
	return CandidateFragment{
		points:    points,
		baseBlock: peerTip.observedTipHistory[0].BlockNumber,
	}
}

// CandidateFragment returns a snapshot of this peer's candidate chain
// fragment. The caller receives an independent copy.
func (p *PeerChainTip) CandidateFragment() CandidateFragment {
	return candidateFragmentForPeer(p)
}

// CandidateFragments returns a snapshot of the candidate chain fragment
// maintained for every currently tracked peer. This is the Dingo equivalent
// of the upstream readCandidateChains query: a fragment exists only while its
// peer's connection is tracked (i.e. chainsync-eligible per peer governance,
// the sole gate on entry into peerTips — see ouroboros/chainsync.go), and
// RemovePeer drops it along with the rest of that peer's state.
//
// This intentionally does not consult the eligible map (SetConnectionEligible
// / isConnectionEligible): that flag disqualifies a peer from being chosen as
// best peer or counted as a corroboration witness, but it is not a disconnect
// and does not stop the peer from chainsyncing or being tracked here, so
// filtering on it would hide a still-connected candidate's fragment and would
// make this accessor inconsistent with its sibling GetAllPeerTips, which does
// not filter on it either. A future consumer that needs only
// selection-eligible fragments can intersect this result with
// isConnectionEligible/GetPeerTip itself.
func (cs *ChainSelector) CandidateFragments() map[ouroboros.ConnectionId]CandidateFragment {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	result := make(
		map[ouroboros.ConnectionId]CandidateFragment,
		len(cs.peerTips),
	)
	for connId, peerTip := range cs.peerTips {
		result[connId] = candidateFragmentForPeer(peerTip)
	}
	return result
}

// GetCandidateFragment returns the candidate chain fragment for a specific
// peer. The second return value is false when the peer is not tracked (never
// observed, or removed on disconnect).
func (cs *ChainSelector) GetCandidateFragment(
	connId ouroboros.ConnectionId,
) (CandidateFragment, bool) {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	peerTip, ok := cs.peerTips[connId]
	if !ok {
		return CandidateFragment{}, false
	}
	return candidateFragmentForPeer(peerTip), true
}
