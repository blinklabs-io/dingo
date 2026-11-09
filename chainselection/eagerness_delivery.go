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
	"math"
	"slices"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type eagernessDelivery struct {
	fragment CandidateFragment
	// fragmentRef locates fragment.points in a window this entry owns
	// exclusively. Its points and hash bytes are copies, never shared with the
	// admitted frontier, so the entry stays an independent record of what the
	// peer delivered. Points are replaced only through the window helpers.
	fragmentRef windowRef[ocommon.Point]
	generation  uint64
	valid       func() bool
	// A normal delivery can refresh continuity without reauthenticating its
	// older ancestors. Only the strict exported range is corroboration proof.
	proofBase       uint64
	proofHead       uint64
	proofGeneration uint64
}

func eagernessFragmentHead(fragment CandidateFragment) (uint64, bool) {
	if len(fragment.points) == 0 {
		return 0, false
	}
	// #nosec G115 -- the nonempty slice has a nonnegative last index.
	distance := uint64(len(fragment.points) - 1)
	if distance > math.MaxUint64-fragment.baseBlock {
		return 0, false
	}
	return fragment.baseBlock + distance, true
}

func fragmentOf(peerTip *PeerChainTip) CandidateFragment {
	if peerTip == nil {
		return CandidateFragment{}
	}
	return CandidateFragment{
		points: peerTip.observedPoints, baseBlock: peerTip.observedPointBaseBlock,
	}
}

// extendsFragment requires matching heights as well as hashes: a delivered
// block-number claim must not reinterpret an admitted point's position.
func extendsFragment(extension, base CandidateFragment) bool {
	baseHead, ok := eagernessFragmentHead(base)
	if !ok {
		return false
	}
	extensionHead, ok := eagernessFragmentHead(extension)
	if !ok {
		return false
	}
	if extension.baseBlock > baseHead || extensionHead < baseHead {
		return false
	}
	start := max(base.baseBlock, extension.baseBlock)
	for number := start; ; number++ {
		left := base.points[number-base.baseBlock]
		right := extension.points[number-extension.baseBlock]
		if left.Slot != right.Slot || !bytes.Equal(left.Hash, right.Hash) {
			return false
		}
		if number == baseHead {
			return true
		}
	}
}

// recordEagernessDeliveryLocked retains same-bearer linkage separately from
// admission. An observer can keep delivering while its last admitted header
// remains unchanged; keepalive activity must not make that old fragment pin
// the limit forever. A gap cannot establish the missing linkage.
func (cs *ChainSelector) recordEagernessDeliveryLocked(
	update PeerTipUpdateEvent,
) {
	if update.HeaderCryptoVerificationGeneration == 0 &&
		(cs.config.HeaderCryptoVerificationGenerationCurrent != nil ||
			cs.config.PinHeaderCryptoVerificationGeneration != nil ||
			cs.config.CurrentHeaderCryptoVerificationGeneration != nil) {
		return
	}
	baseTip, ok := cs.admissionPeerTipLocked(update.ConnectionId)
	if !ok || baseTip == nil || baseTip.awaitingFirstHeader {
		return
	}
	base := fragmentOf(baseTip)
	if base.Len() == 0 {
		return
	}
	entry, exists := cs.eagernessDeliveries[update.ConnectionId]
	if exists && entry.valid != nil && !entry.valid() {
		delete(cs.eagernessDeliveries, update.ConnectionId)
		entry, exists = eagernessDelivery{}, false
	}
	if !exists || !extendsFragment(entry.fragment, base) {
		entry = rebuildEagernessDelivery(entry.fragment, entry.fragmentRef, base)
	}
	fragment := entry.fragment
	headBlock, ok := eagernessFragmentHead(fragment)
	if !ok {
		return
	}
	head := fragment.points[fragment.Len()-1]
	observed := update.ObservedTip
	switch {
	case observed.BlockNumber == headBlock &&
		observed.Point.Slot == head.Slot && bytes.Equal(observed.Point.Hash, head.Hash):
	case headBlock != math.MaxUint64 && observed.BlockNumber == headBlock+1 &&
		observed.Point.Slot > head.Slot && !update.ObservedBoundary &&
		bytes.Equal(update.ObservedPrevHash, head.Hash):
		fragment.points = windowAppend(
			&entry.fragmentRef,
			fragment.points,
			clonePoint(observed.Point),
			pointsEqual,
		)
	default:
		delete(cs.eagernessDeliveries, update.ConnectionId)
		return
	}
	capacity := cs.candidatePointCapacityLocked(true)
	// #nosec G115 -- slice lengths are nonnegative and fit in uint64.
	count := uint64(len(fragment.points))
	if count > capacity {
		excess := count - capacity
		// #nosec G115 -- excess is at most the retained slice length.
		fragment.points = windowTrimFront(
			&entry.fragmentRef,
			fragment.points,
			int(excess),
		)
		fragment.baseBlock += excess
	}
	if !exists && len(cs.eagernessDeliveries) >= cs.maxTrackedPeers {
		return
	}
	entry.fragment = fragment
	entry.generation = update.HeaderCryptoVerificationGeneration
	entry.trimProof()
	cs.eagernessDeliveries[update.ConnectionId] = entry
}

// rebuildEagernessDelivery returns a fresh delivery entry holding a copy of
// base. When previous is a prefix of base, previous's own points are kept and
// only base's newer points are copied, so a delivery that trails the admitted
// frontier by a header does not re-copy the whole window. The result is equal
// in value to a full copy of base.
func rebuildEagernessDelivery(
	previous CandidateFragment,
	ref windowRef[ocommon.Point],
	base CandidateFragment,
) eagernessDelivery {
	previousHead, ok := eagernessFragmentHead(previous)
	if !ok || previous.baseBlock > base.baseBlock ||
		!extendsFragment(base, previous) {
		return eagernessDelivery{fragment: CandidateFragment{
			points: cloneObservedPoints(base.points), baseBlock: base.baseBlock,
		}}
	}
	// base.baseBlock <= previousHead (extendsFragment), so a point of previous
	// remains after dropping what base no longer retains.
	// #nosec G115 -- the dropped count is at most len(previous.points)-1.
	points := windowTrimFront(
		&ref,
		previous.points,
		int(base.baseBlock-previous.baseBlock),
	)
	// #nosec G115 -- previousHead >= base.baseBlock and both are bounded.
	suffix := base.points[previousHead-base.baseBlock+1:]
	for _, point := range suffix {
		points = windowAppend(&ref, points, clonePoint(point), pointsEqual)
	}
	return eagernessDelivery{
		fragment:    CandidateFragment{points: points, baseBlock: base.baseBlock},
		fragmentRef: ref,
	}
}

func (cs *ChainSelector) eagernessFragmentLocked(
	connId ouroboros.ConnectionId, peerTip *PeerChainTip,
) CandidateFragment {
	base := fragmentOf(peerTip)
	entry, ok := cs.eagernessDeliveries[connId]
	if !ok || (entry.valid != nil && !entry.valid()) ||
		!cs.peerTipAdmissionGenerationCurrent(PeerTipUpdateEvent{
			HeaderCryptoVerificationGeneration: entry.generation,
		}) ||
		!extendsFragment(entry.fragment, base) {
		return base
	}
	extension := entry.fragment
	if extension.baseBlock <= base.baseBlock {
		return extension
	}
	// Keep every older candidate point: losing an admitted fork point must
	// not turn a known intersection into the more permissive saved-anchor
	// fallback. Only this temporary read view can span both bounded windows.
	prefix := base.points[:extension.baseBlock-base.baseBlock]
	if len(prefix) > math.MaxInt-len(extension.points) {
		return base
	}
	joined := make([]ocommon.Point, len(prefix)+len(extension.points))
	copy(joined, prefix)
	copy(joined[len(prefix):], extension.points)
	return CandidateFragment{points: joined, baseBlock: base.baseBlock}
}

func (cs *ChainSelector) rollbackEagernessDeliveryLocked(
	connId ouroboros.ConnectionId, point ocommon.Point,
) {
	entry, ok := cs.eagernessDeliveries[connId]
	if !ok {
		return
	}
	index := slices.IndexFunc(
		entry.fragment.points,
		func(p ocommon.Point) bool {
			return p.Slot == point.Slot && bytes.Equal(p.Hash, point.Hash)
		},
	)
	if index < 0 {
		delete(cs.eagernessDeliveries, connId)
		return
	}
	entry.fragment.points = windowTruncate(entry.fragment.points, index+1)
	entry.trimProof()
	cs.eagernessDeliveries[connId] = entry
}

func (entry *eagernessDelivery) trimProof() {
	head, ok := eagernessFragmentHead(entry.fragment)
	if !ok || entry.proofGeneration == 0 ||
		entry.proofHead < entry.fragment.baseBlock ||
		head < entry.proofBase {
		entry.proofGeneration = 0
		return
	}
	entry.proofBase = max(entry.proofBase, entry.fragment.baseBlock)
	entry.proofHead = min(entry.proofHead, head)
}
