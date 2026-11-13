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
)

func fragmentsOverlapExactly(left, right CandidateFragment) bool {
	leftHead, leftOK := eagernessFragmentHead(left)
	rightHead, rightOK := eagernessFragmentHead(right)
	start, end := max(left.baseBlock, right.baseBlock), min(leftHead, rightHead)
	if !leftOK || !rightOK || start > end {
		return false
	}
	for number := start; ; number++ {
		a, b := left.points[number-left.baseBlock], right.points[number-right.baseBlock]
		if a.Slot != b.Slot || !bytes.Equal(a.Hash, b.Hash) {
			return false
		}
		if number == end {
			return true
		}
	}
}

// proofOverlapsLocked reports whether conn's proof range equals legacy where
// they overlap.
func (cs *ChainSelector) proofOverlapsLocked(
	conn ouroboros.ConnectionId,
	proof, legacy CandidateFragment,
) bool {
	entry := cs.eagernessDeliveries[conn]
	return entry.proofOverlaps(proof, legacy)
}

func (cs *ChainSelector) verifiedCorroborationFragmentLocked(
	conn ouroboros.ConnectionId,
) (CandidateFragment, bool) {
	entry, ok := cs.eagernessDeliveries[conn]
	head, hasHead := eagernessFragmentHead(entry.fragment)
	if !ok || !hasHead || entry.valid == nil || !entry.valid() ||
		entry.proofGeneration == 0 || entry.proofBase < entry.fragment.baseBlock ||
		entry.proofHead < entry.proofBase || entry.proofHead > head ||
		!cs.peerTipAdmissionGenerationCurrent(PeerTipUpdateEvent{
			HeaderCryptoVerificationGeneration: entry.proofGeneration,
		}) {
		return CandidateFragment{}, false
	}
	base, ok := cs.admissionPeerTipLocked(conn)
	if !ok || base == nil ||
		!entry.extends(fragmentOf(base)) {
		return CandidateFragment{}, false
	}
	return CandidateFragment{
		points:    entry.fragment.points[entry.proofBase-entry.fragment.baseBlock : entry.proofHead-entry.fragment.baseBlock+1],
		baseBlock: entry.proofBase,
	}, true
}

func (cs *ChainSelector) corroborationHistoryRepairNeededLocked(
	conn ouroboros.ConnectionId,
	anchor ochainsync.Tip,
) bool {
	if !cs.genesisCorroborationActiveLocked() {
		return false
	}
	legacy := cs.latestCorroborationWitnessLocked(conn)
	if legacy == nil || len(legacy.observedPoints) == 0 ||
		legacy.observedPointBaseBlock <= anchor.BlockNumber {
		return false
	}
	proof, ok := cs.verifiedCorroborationFragmentLocked(conn)
	head, _ := eagernessFragmentHead(proof)
	return !ok || proof.baseBlock > anchor.BlockNumber ||
		head < legacy.observedPointBaseBlock ||
		!cs.proofOverlapsLocked(conn, proof, fragmentOf(legacy))
}

func (cs *ChainSelector) verifiedCorroborationWitnessLocked(
	conn ouroboros.ConnectionId,
	legacy *PeerChainTip,
	candidate *PeerChainTip,
) *PeerChainTip {
	proof, ok := cs.verifiedCorroborationFragmentLocked(conn)
	if !ok || legacy == nil || candidate == nil ||
		!cs.proofOverlapsLocked(conn, proof, fragmentOf(legacy)) {
		return nil
	}
	head, _ := eagernessFragmentHead(proof)
	tip := candidate.SelectionTip()
	if tip.BlockNumber < proof.baseBlock || tip.BlockNumber > head {
		return nil
	}
	point := proof.points[tip.BlockNumber-proof.baseBlock]
	if point.Slot != tip.Point.Slot ||
		!bytes.Equal(point.Hash, tip.Point.Hash) {
		return nil
	}
	// This view can witness only the strict range. Neither density history nor
	// admission sees its older points or advances to the authenticated endpoint.
	view := *legacy
	view.observedPoints = proof.points
	view.observedPointBaseBlock = proof.baseBlock
	view.ObservedTip = ochainsync.Tip{
		Point:       proof.points[len(proof.points)-1],
		BlockNumber: head,
	}
	view.observedTipSet = true
	return &view
}
