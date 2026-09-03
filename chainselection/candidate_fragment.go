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
// fragment may hold fewer than k+1 points, and its anchor is not required to
// intersect the primary chain or any other peer's fragment.
//
// The fragment reuses PeerChainTip's existing delivered-tip history
// (recordObservedTipHistory), which is already updated once per delivered
// header and bounded to k+1 entries for rollback restoration, and exposes it
// as a first-class, independently owned value with an anchor and a pairwise
// intersection primitive. Entries are kept in strictly ascending slot order.
type CandidateFragment struct {
	entries []ochainsync.Tip
}

// Len returns the number of points retained in the fragment.
func (f CandidateFragment) Len() int {
	return len(f.entries)
}

// Anchor returns the fragment's oldest retained point. Per the upstream
// AnchoredFragment contract, the anchor is not guaranteed to intersect the
// primary chain or any other peer's fragment — it is simply the oldest point
// this snapshot still remembers. Anchor returns the zero Point when the
// fragment is empty.
func (f CandidateFragment) Anchor() ocommon.Point {
	if len(f.entries) == 0 {
		return ocommon.Point{}
	}
	return f.entries[0].Point
}

// HeadPoint returns the fragment's most recently delivered point, or the
// zero Point when the fragment is empty.
func (f CandidateFragment) HeadPoint() ocommon.Point {
	if len(f.entries) == 0 {
		return ocommon.Point{}
	}
	return f.entries[len(f.entries)-1].Point
}

// Points returns the fragment's retained points, including the anchor, in
// ascending slot order. The caller receives an independent copy.
func (f CandidateFragment) Points() []ocommon.Point {
	if len(f.entries) == 0 {
		return nil
	}
	out := make([]ocommon.Point, len(f.entries))
	for i, tip := range f.entries {
		out[i] = clonePoint(tip.Point)
	}
	return out
}

// Intersect returns the highest point present in both fragments, comparing
// by (slot, hash). Both fragments must already be in ascending slot order,
// which recordObservedTipHistory guarantees. This is the primitive the Limit
// on Eagerness and the Genesis Density Disconnector need to compute the
// intersection across candidate fragments (see ARCHITECTURE.md); it does not
// itself implement either.
func (f CandidateFragment) Intersect(
	other CandidateFragment,
) (ocommon.Point, bool) {
	i := len(f.entries) - 1
	j := len(other.entries) - 1
	for i >= 0 && j >= 0 {
		a := f.entries[i].Point
		b := other.entries[j].Point
		switch {
		case a.Slot == b.Slot:
			if bytes.Equal(a.Hash, b.Hash) {
				return clonePoint(a), true
			}
			// Same slot, different block: the chains have already diverged
			// by this point, so any common ancestor lies strictly earlier on
			// both sides.
			i--
			j--
		case a.Slot > b.Slot:
			i--
		default:
			j--
		}
	}
	return ocommon.Point{}, false
}

// candidateFragmentFromHistory snapshots a peer's delivered-tip history into
// an independently owned CandidateFragment.
func candidateFragmentFromHistory(
	history []ochainsync.Tip,
) CandidateFragment {
	if len(history) == 0 {
		return CandidateFragment{}
	}
	entries := make([]ochainsync.Tip, len(history))
	for i, tip := range history {
		entries[i] = cloneObservedTip(tip)
	}
	return CandidateFragment{entries: entries}
}

// CandidateFragment returns a snapshot of this peer's candidate chain
// fragment. The caller receives an independent copy.
func (p *PeerChainTip) CandidateFragment() CandidateFragment {
	if p == nil {
		return CandidateFragment{}
	}
	return candidateFragmentFromHistory(p.observedTipHistory)
}

// CandidateFragments returns a snapshot of the candidate chain fragment
// maintained for every currently tracked (chainsync-eligible) peer. This is
// the Dingo equivalent of the upstream readCandidateChains query: a fragment
// exists only while its peer's connection is tracked, and RemovePeer drops it
// along with the rest of that peer's state.
func (cs *ChainSelector) CandidateFragments() map[ouroboros.ConnectionId]CandidateFragment {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	result := make(
		map[ouroboros.ConnectionId]CandidateFragment,
		len(cs.peerTips),
	)
	for connId, peerTip := range cs.peerTips {
		result[connId] = candidateFragmentFromHistory(
			peerTip.observedTipHistory,
		)
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
	return candidateFragmentFromHistory(peerTip.observedTipHistory), true
}
