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
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// jumpRecord is the candidate fragment ChainSync Jumping assigns to a peer
// that does not stream headers: the dynamo's candidate fragment up to the
// last point the peer agreed to. objector marks a peer that refused a later
// jump; its record stays frozen at the last agreed point.
type jumpRecord struct {
	tip      *PeerChainTip
	objector bool
}

// RecordJumpAgreement records that jumper answered MsgFindIntersect at point,
// a point on dynamo's candidate fragment (JumpFragment), with IntersectFound.
// jumperTip is the tip the jumper reported with that answer. The jumper's
// candidate fragment becomes dynamo's fragment truncated at point, held under
// the jumper's own connection and therefore its own corroboration identity, as
// the reference's processJumpResult sets a jumper's candidate to the dynamo's
// fragment up to an accepted jump (Ouroboros/Consensus/MiniProtocol/
// ChainSync/Client/Jumping.hs). The server proved its chain holds point, and
// header hashes chain, so it holds every point of the fragment below it.
//
// The record is a Limit on Eagerness candidate and witness only: the jumper is
// never selectable, is not compared by the Genesis Density Disconnector, and
// needs no ingress slot. It is used only while the connection has no streamed
// record of its own.
//
// It returns false, recording nothing, when ChainSync Jumping is off, the
// selector is not in Genesis mode, jumper is dynamo, jumper streams headers
// or is an objector, dynamo's candidate fragment does not hold point, or the
// tracked peer limit is reached.
func (cs *ChainSelector) RecordJumpAgreement(
	jumper ouroboros.ConnectionId,
	dynamo ouroboros.ConnectionId,
	point ocommon.Point,
	jumperTip ochainsync.Tip,
) bool {
	if !cs.config.ChainSyncJumping || jumper == dynamo ||
		len(point.Hash) == 0 {
		return false
	}
	if cs.config.ConnectionLive != nil && !cs.config.ConnectionLive(jumper) {
		return false
	}
	recorded := func() bool {
		cs.mutex.Lock()
		defer cs.mutex.Unlock()
		if cs.mode != SelectionModeGenesis || cs.streamedRecordLocked(jumper) {
			return false
		}
		existing := cs.jumpRecords[jumper]
		if existing != nil && existing.objector {
			return false
		}
		if existing == nil && len(cs.jumpRecords) >= cs.maxTrackedPeers {
			return false
		}
		dynamoTip, fragment, ok := cs.jumpFragmentLocked(dynamo)
		if !ok {
			return false
		}
		block, ok := fragment.pointBlock(point)
		if !ok {
			return false
		}
		// #nosec G115 -- block is within the retained fragment.
		retained := int(block - fragment.baseBlock + 1)
		var slots []uint64
		for _, slot := range dynamoTip.observedSlots {
			if slot <= point.Slot {
				slots = append(slots, slot)
			}
		}
		observed := ochainsync.Tip{
			Point:       clonePoint(point),
			BlockNumber: block,
		}
		cs.jumpRecords[jumper] = &jumpRecord{
			tip: &PeerChainTip{
				ConnectionId:   jumper,
				Tip:            cloneObservedTip(jumperTip),
				ObservedTip:    observed,
				observedTipSet: true,
				observedSlots:  slots,
				observedPoints: cloneObservedPoints(
					fragment.points[:retained],
				),
				observedPointBaseBlock: fragment.baseBlock,
				LastUpdated:            cs.nowFn(),
				nowFn:                  cs.nowFn,
				jumpRecord:             true,
			},
		}
		cs.signalIngressChange()
		return true
	}()
	if recorded {
		cs.triggerEvaluation()
	}
	return recorded
}

// RecordJumpRejection records that jumper answered a jump with
// IntersectNotFound, making it an objector. Its record stays at the last
// point it agreed to, so the Limit on Eagerness cannot pass k beyond that
// point while the disagreement is unresolved; once the objector streams, its
// streamed record replaces the frozen one. It returns false when ChainSync
// Jumping is off or jumper holds no jump record.
func (cs *ChainSelector) RecordJumpRejection(
	jumper ouroboros.ConnectionId,
) bool {
	if !cs.config.ChainSyncJumping {
		return false
	}
	cs.mutex.Lock()
	defer cs.mutex.Unlock()
	record := cs.jumpRecords[jumper]
	if record == nil {
		return false
	}
	record.objector = true
	cs.signalIngressChange()
	return true
}

// streamedRecordLocked reports whether connId has a committed or staged
// record from headers it delivered. A record registered from the opening
// MsgRollBackward of a stream (awaitingFirstHeader) does not count: it holds
// no fragment, so it proves nothing about the peer's chain, and an objector's
// frozen jump record must keep bounding the Limit on Eagerness until the
// objector's own headers replace it. Must be called with cs.mutex held.
func (cs *ChainSelector) streamedRecordLocked(
	connId ouroboros.ConnectionId,
) bool {
	if frontier := cs.pendingPeerTipFrontiers[connId]; frontier != nil &&
		!frontier.awaitingFirstHeader {
		return true
	}
	committed := cs.peerTips[connId]
	return committed != nil && !committed.awaitingFirstHeader
}

// jumpCandidateLocked returns connId's jump record when it stands in for the
// connection's candidate fragment, that is when the connection has delivered
// no header (streamedRecordLocked). Must be called with cs.mutex held.
func (cs *ChainSelector) jumpCandidateLocked(
	connId ouroboros.ConnectionId,
) *PeerChainTip {
	record := cs.jumpRecords[connId]
	if record == nil || cs.streamedRecordLocked(connId) {
		return nil
	}
	return record.tip
}

// loeCandidateConnectionLocked reports whether connId's fragment counts in the
// Limit on Eagerness candidate set: it holds a chainsync ingress slot, or its
// fragment is a jump record, which is current without an ingress stream.
// Must be called with cs.mutex held.
func (cs *ChainSelector) loeCandidateConnectionLocked(
	connId ouroboros.ConnectionId,
) bool {
	return cs.eagernessCandidateAdmittedLocked(connId) ||
		cs.jumpCandidateLocked(connId) != nil
}

// JumpFragment returns the points of dynamo's candidate fragment, the
// fragment ChainSync Jumping jumps jumpers to and RecordJumpAgreement
// truncates, in ascending order, and the block number of the last one. It
// returns false when dynamo has no fragment.
func (cs *ChainSelector) JumpFragment(
	dynamo ouroboros.ConnectionId,
) ([]ocommon.Point, uint64, bool) {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	_, fragment, ok := cs.jumpFragmentLocked(dynamo)
	if !ok {
		return nil, 0, false
	}
	head, _ := eagernessFragmentHead(fragment)
	return fragment.Points(), head, true
}

// jumpFragmentLocked returns dynamo's candidate fragment with its staged
// headers, as the reference jumps to the dynamo's candidate fragment rather
// than to any adopted chain. The committed fragment would not do: during
// Genesis the apply gate stages a header until a witness vouches for it, and
// jump agreements are the dynamo's witnesses, so a dynamo with no other
// witness would commit nothing and never be jumped to. This is the same
// staged frontier the Limit on Eagerness and the apply gate already read for
// the dynamo, and a record built from it holds only points a jumper's own
// server proved it holds. It returns false when the fragment is empty. Must
// be called with cs.mutex held.
func (cs *ChainSelector) jumpFragmentLocked(
	dynamo ouroboros.ConnectionId,
) (*PeerChainTip, CandidateFragment, bool) {
	dynamoTip, ok := cs.admissionPeerTipLocked(dynamo)
	if !ok || dynamoTip == nil {
		return nil, CandidateFragment{}, false
	}
	fragment := cs.eagernessFragmentLocked(dynamo, dynamoTip)
	if fragment.Len() == 0 {
		return nil, CandidateFragment{}, false
	}
	return dynamoTip, fragment, true
}
