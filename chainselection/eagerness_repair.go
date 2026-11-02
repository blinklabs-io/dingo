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

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// EagernessDeliveryRepair returns the exact repair anchor and maximum proof
// length when a verified delivery lacks continuity or corroboration history.
// The caller must authenticate the complete path outside selector locks.
func (cs *ChainSelector) EagernessDeliveryRepair(
	update PeerTipUpdateEvent,
) (ochainsync.Tip, int, bool) {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	if cs.mode != SelectionModeGenesis {
		return ochainsync.Tip{}, 0, false
	}
	peer, ok := cs.admissionPeerTipLocked(update.ConnectionId)
	if !ok || peer == nil || peer.awaitingFirstHeader {
		return ochainsync.Tip{}, 0, false
	}
	base := fragmentOf(peer)
	head, ok := eagernessFragmentHead(base)
	if !ok || update.ObservedTip.BlockNumber < head {
		return ochainsync.Tip{}, 0, false
	}
	anchor := cs.eagernessRepairAnchorLocked(update.ConnectionId, base)
	if update.ObservedTip.BlockNumber <= anchor.BlockNumber {
		return ochainsync.Tip{}, 0, false
	}
	capacity := cs.candidatePointCapacityLocked(true)
	if capacity > math.MaxInt ||
		update.ObservedTip.BlockNumber-anchor.BlockNumber >= capacity {
		return ochainsync.Tip{}, 0, false
	}
	if entry, exists := cs.eagernessDeliveries[update.ConnectionId]; exists &&
		(entry.valid == nil || entry.valid()) &&
		cs.peerTipAdmissionGenerationCurrent(PeerTipUpdateEvent{
			HeaderCryptoVerificationGeneration: entry.generation,
		}) && extendsFragment(entry.fragment, base) {
		if proofHead, valid := eagernessFragmentHead(entry.fragment); valid &&
			proofHead == update.ObservedTip.BlockNumber {
			last := entry.fragment.points[len(entry.fragment.points)-1]
			if last.Slot == update.ObservedTip.Point.Slot &&
				bytes.Equal(last.Hash, update.ObservedTip.Point.Hash) &&
				!cs.corroborationHistoryRepairNeededLocked(
					update.ConnectionId,
					anchor,
				) {
				return ochainsync.Tip{}, 0, false
			}
		}
	}
	return anchor, int(capacity), true
}

// RecordVerifiedEagernessPath consumes a ledger-authenticated ordinary-header
// path beginning at anchor. It changes only read-only delivery evidence, and
// rejects a proof whose ledger generation or repair anchor changed during the read.
func (cs *ChainSelector) RecordVerifiedEagernessPath(
	update PeerTipUpdateEvent,
	anchor ochainsync.Tip,
	path []ochainsync.Tip,
	generation uint64,
	stillCurrent func() bool,
	retained func() bool,
) bool {
	if generation == 0 || stillCurrent == nil || retained == nil ||
		len(path) < 2 ||
		!sameEagernessTip(path[0], anchor) ||
		!sameEagernessTip(path[len(path)-1], update.ObservedTip) {
		return false
	}
	for index, tip := range path {
		if len(tip.Point.Hash) != 32 {
			return false
		}
		if index > 0 {
			previous := path[index-1]
			if previous.BlockNumber == math.MaxUint64 ||
				tip.BlockNumber != previous.BlockNumber+1 || tip.Point.Slot <= previous.Point.Slot {
				return false
			}
		}
	}
	if cs.config.ConnectionLive != nil &&
		!cs.config.ConnectionLive(update.ConnectionId) {
		return false
	}
	release, current := cs.pinPeerTipAdmissionGeneration(PeerTipUpdateEvent{
		HeaderCryptoVerificationGeneration: generation,
	})
	if !current {
		return false
	}
	defer release()
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	if !stillCurrent() || !retained() ||
		cs.mode != SelectionModeGenesis {
		return false
	}
	peer, ok := cs.admissionPeerTipLocked(update.ConnectionId)
	if !ok || peer == nil || peer.awaitingFirstHeader {
		return false
	}
	base := fragmentOf(peer)
	head, ok := eagernessFragmentHead(base)
	if !ok || !sameEagernessTip(cs.eagernessRepairAnchorLocked(update.ConnectionId, base), anchor) ||
		anchor.BlockNumber > head ||
		update.ObservedTip.BlockNumber < head {
		return false
	}
	verified := CandidateFragment{baseBlock: anchor.BlockNumber}
	for _, tip := range path {
		verified.points = append(verified.points, tip.Point)
	}
	if !extendsFragment(verified, base) {
		return false
	}
	capacity := cs.candidatePointCapacityLocked(true)
	if capacity > math.MaxInt || uint64(len(path)) > capacity ||
		len(base.points)-1 > math.MaxInt-len(path) {
		return false
	}
	if _, exists := cs.eagernessDeliveries[update.ConnectionId]; !exists &&
		len(cs.eagernessDeliveries) >= cs.maxTrackedPeers {
		return false
	}
	prefixLength := uint64(0)
	if base.baseBlock < anchor.BlockNumber {
		prefixLength = anchor.BlockNumber - base.baseBlock
	}
	points := make([]ocommon.Point, 0, int(prefixLength)+len(path))
	for _, point := range base.points[:prefixLength] {
		points = append(points, clonePoint(point))
	}
	for _, tip := range path {
		points = append(points, clonePoint(tip.Point))
	}
	baseBlock := min(base.baseBlock, anchor.BlockNumber)
	if len(points) > int(capacity) {
		excess := len(points) - int(capacity)
		clear(points[:excess])
		points = points[excess:]
		// #nosec G115 -- excess is a positive slice length.
		baseBlock += uint64(excess)
	}
	cs.eagernessDeliveries[update.ConnectionId] = eagernessDelivery{
		fragment: CandidateFragment{
			points:    points,
			baseBlock: baseBlock,
		}, generation: generation, valid: retained,
		proofBase: max(
			anchor.BlockNumber,
			baseBlock,
		), proofHead: update.ObservedTip.BlockNumber,
		proofGeneration: generation,
	}
	cs.signalIngressChange()
	return true
}

func sameEagernessTip(left, right ochainsync.Tip) bool {
	return left.BlockNumber == right.BlockNumber &&
		left.Point.Slot == right.Point.Slot &&
		bytes.Equal(left.Point.Hash, right.Point.Hash)
}

// A gap can begin above the applied tip even when the admission head is
// farther ahead. The lower exact point must itself be proved on this peer's
// chain; choosing it never substitutes canonical history for a divergent fork.
func (cs *ChainSelector) eagernessRepairAnchorLocked(
	conn ouroboros.ConnectionId,
	base CandidateFragment,
) ochainsync.Tip {
	head, _ := eagernessFragmentHead(base)
	anchor := ochainsync.Tip{
		Point:       clonePoint(base.points[len(base.points)-1]),
		BlockNumber: head,
	}
	legacy := cs.latestCorroborationWitnessLocked(conn)
	if len(cs.localTip.Point.Hash) == 32 && cs.localTip.BlockNumber < head &&
		legacy != nil && len(legacy.observedPoints) > 0 && legacy.observedPointBaseBlock > cs.localTip.BlockNumber {
		anchor = cloneObservedTip(cs.localTip)
	}
	return anchor
}
