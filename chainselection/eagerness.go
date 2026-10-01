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
	"context"
	"slices"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// EagernessLimit is the Limit on Eagerness: the highest block number chain
// selection may reach while the cap is active.
type EagernessLimit struct {
	// Active reports whether the cap applies. It is false outside Genesis
	// selection mode (the node is caught up), and for a selector without a
	// security parameter.
	Active bool
	// Intersected is true when the limit was derived from a point common to
	// every candidate fragment, and false when the candidates share no
	// retained point and the limit fell back to the local tip.
	Intersected bool
	// Point is the common point of the candidate fragments. It is the zero
	// Point unless Intersected.
	Point ocommon.Point
	// BlockNumber is the highest block number selection may reach: k past the
	// intersection, or k past the local tip when there is no intersection.
	BlockNumber uint64
}

// EagernessLimit returns the current Limit on Eagerness.
func (cs *ChainSelector) EagernessLimit() EagernessLimit {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	return cs.computeEagernessLimitLocked(nil, cs.localTip)
}

// computeEagernessLimitLocked derives the limit from the candidate fragments of
// every live, eligible, non-stale peer that has delivered a header.
//
// The cap is gated on the selection mode: Genesis mode is the syncing state,
// and Praos mode means the node has caught up, where the cap is off. Dingo has
// no separate Genesis State Machine, so the mode is the only sync signal.
// Before any candidate has delivered a header the cap is at its most
// conservative, k past the local tip.
//
// With a single candidate the intersection is that candidate's own head, so
// the cap never holds back a lone peer. Retained fragments are bounded, so
// candidates can share no retained point; the limit then also falls back to
// the local tip, which keeps it conservative without needing history older
// than the fragments hold.
//
// localTip is the applied tip the fallback limit is measured from. include
// names a peer that counts as a candidate even when it is stale or
// ineligible, so a header's own sender is never held back by a candidate set
// that has dropped it. Pass nil for the selection view.
func (cs *ChainSelector) computeEagernessLimitLocked(
	include *ouroboros.ConnectionId,
	localTip ochainsync.Tip,
) EagernessLimit {
	if cs.mode != SelectionModeGenesis || cs.securityParam == 0 {
		return EagernessLimit{}
	}
	limit := EagernessLimit{
		Active: true,
		BlockNumber: safeAddUint64(
			localTip.BlockNumber,
			cs.securityParam,
		),
	}
	var fragments []CandidateFragment
	for connId, peerTip := range cs.peerTips {
		if peerTip.awaitingFirstHeader ||
			len(peerTip.observedTipHistory) == 0 {
			continue
		}
		if (include == nil || *include != connId) &&
			!cs.peerLiveEligibleNonStaleLocked(connId, peerTip) {
			continue
		}
		// A read-only view: it is never returned or retained.
		fragments = append(
			fragments,
			CandidateFragment{entries: peerTip.observedTipHistory},
		)
	}
	if len(fragments) == 0 {
		return limit
	}
	// Every point shared by all fragments lies on the first fragment, and the
	// lowest of its intersections with the others is the candidate. The
	// fragments are bounded windows, so two of the others can each hold their
	// fork point with the first and still share nothing with each other; only
	// a candidate every fragment retains is accepted. Without that check the
	// result would depend on which fragment map iteration put first.
	common := fragments[0].entries[len(fragments[0].entries)-1]
	for _, other := range fragments[1:] {
		tip, ok := fragments[0].intersectTip(other)
		if !ok {
			return limit
		}
		if tip.BlockNumber < common.BlockNumber {
			common = tip
		}
	}
	for _, fragment := range fragments[1:] {
		if !fragment.containsPoint(common.Point) {
			return limit
		}
	}
	limit.Intersected = true
	limit.Point = clonePoint(common.Point)
	limit.BlockNumber = safeAddUint64(common.BlockNumber, cs.securityParam)
	return limit
}

// containsPoint reports whether the fragment retains point.
func (f CandidateFragment) containsPoint(point ocommon.Point) bool {
	for _, entry := range f.entries {
		if entry.Point.Slot == point.Slot &&
			bytes.Equal(entry.Point.Hash, point.Hash) {
			return true
		}
	}
	return false
}

// SelectedTip returns the tip of the currently selected peer's chain truncated
// at the Limit on Eagerness: the highest point of that peer's retained
// fragment not past the limit, or the local tip when the fragment holds none.
// It returns false when no peer is selected.
func (cs *ChainSelector) SelectedTip() (ochainsync.Tip, bool) {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	if cs.bestPeerConn == nil {
		return ochainsync.Tip{}, false
	}
	peerTip, ok := cs.peerTips[*cs.bestPeerConn]
	if !ok {
		return ochainsync.Tip{}, false
	}
	tip := peerTip.SelectionTip()
	limit := cs.computeEagernessLimitLocked(nil, cs.localTip)
	if !limit.Active || tip.BlockNumber <= limit.BlockNumber {
		return cloneObservedTip(tip), true
	}
	for _, historyTip := range slices.Backward(peerTip.observedTipHistory) {
		if historyTip.BlockNumber <= limit.BlockNumber {
			return cloneObservedTip(historyTip), true
		}
	}
	return cloneObservedTip(cs.localTip), true
}

// beginEagernessLocked caches the limit for the duration of one evaluation so
// that each pairwise comparison does not recompute it. It returns whether the
// caller owns the cache and must call endEagernessLocked.
func (cs *ChainSelector) beginEagernessLocked() bool {
	if cs.eagerness != nil {
		return false
	}
	limit := cs.computeEagernessLimitLocked(nil, cs.localTip)
	cs.eagerness = &limit
	return true
}

func (cs *ChainSelector) endEagernessLocked(owned bool) {
	if owned {
		cs.eagerness = nil
	}
}

// indistinguishableUnderEagernessLocked reports whether two peers with
// different block numbers both reach the Limit on Eagerness, so that selection
// truncated at the limit cannot tell their chains apart.
func (cs *ChainSelector) indistinguishableUnderEagernessLocked(
	a, b ochainsync.Tip,
) bool {
	if cs.eagerness == nil || !cs.eagerness.Active ||
		a.BlockNumber == b.BlockNumber {
		return false
	}
	limit := cs.eagerness.BlockNumber
	return min(a.BlockNumber, limit) == min(b.BlockNumber, limit)
}

// candidateFragmentCapacityLocked is how many delivered points a peer's
// candidate fragment retains. k+1 is enough to restore any rollback within k.
// While Genesis selection is active the fragment keeps 2k+1: a candidate more
// than k past the point where it forked from another is the one the Limit on
// Eagerness must hold back, and a k+1 window would already have dropped that
// fork point.
func (cs *ChainSelector) candidateFragmentCapacityLocked() uint64 {
	capacity := safeAddUint64(cs.securityParam, 1)
	if cs.mode == SelectionModeGenesis {
		capacity = safeAddUint64(cs.securityParam, capacity)
	}
	return capacity
}

// eagernessPollInterval is how often AwaitEagernessLimit re-evaluates the
// limit. The limit also moves with the applied ledger tip, which has no change
// notification, so waiting on selector events alone would miss it.
const eagernessPollInterval = 100 * time.Millisecond

// AwaitEagernessLimit blocks until a header numbered blockNumber, delivered by
// connId, is within the Limit on Eagerness, and returns the context's error if
// it is cancelled first. It returns immediately when the cap is inactive, when
// connId is not a tracked peer, or when the header is within the limit.
//
// appliedTip, when non-nil, supplies the applied ledger tip that the fallback
// limit is measured from and is re-read on every check. A running node gives
// the selector its local tip only at startup and then on the stall recycler's
// tick, which would hold a paused peer for the length of that interval; nil
// uses the selector's own local tip. The
// sender counts as a candidate even when stale, so a lone peer is never held
// back by the limit that its own fragment defines.
//
// The caller's chainsync callback blocks here, which stops that peer's header
// stream without discarding a header: the peer's cursor stays put and the
// header is delivered once the limit allows it.
func (cs *ChainSelector) AwaitEagernessLimit(
	ctx context.Context,
	connId ouroboros.ConnectionId,
	blockNumber uint64,
	appliedTip func() ochainsync.Tip,
) error {
	if cs.withinEagernessLimit(connId, blockNumber, appliedTip) {
		return nil
	}
	ticker := time.NewTicker(eagernessPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if cs.withinEagernessLimit(
				connId, blockNumber, appliedTip,
			) {
				return nil
			}
		}
	}
}

func (cs *ChainSelector) withinEagernessLimit(
	connId ouroboros.ConnectionId,
	blockNumber uint64,
	appliedTip func() ochainsync.Tip,
) bool {
	// Read outside the lock: the ledger tip is the ledger's to guard.
	var applied ochainsync.Tip
	if appliedTip != nil {
		applied = appliedTip()
	}
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	if _, tracked := cs.peerTips[connId]; !tracked {
		return true
	}
	if appliedTip == nil {
		applied = cs.localTip
	}
	limit := cs.computeEagernessLimitLocked(&connId, applied)
	return !limit.Active || blockNumber <= limit.BlockNumber
}
