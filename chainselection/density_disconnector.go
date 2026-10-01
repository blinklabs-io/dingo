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
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// GenesisDensityEvaluationInterval is the minimum time between Genesis
// Density Disconnector evaluations. The pairwise comparison is quadratic in
// the number of tracked peers, so it is not run on every peer tip update.
const GenesisDensityEvaluationInterval = time.Second

// GenesisDensityDisconnect describes a peer the Genesis Density Disconnector
// found provably sparser than another candidate chain.
type GenesisDensityDisconnect struct {
	// ConnectionId is the peer to disconnect.
	ConnectionId ouroboros.ConnectionId
	// DominatingConnectionId is the peer whose chain is provably denser.
	DominatingConnectionId ouroboros.ConnectionId
	// Intersection is the highest block the two candidates share.
	Intersection ocommon.Point
	// WindowSlots is the Genesis window that starts after Intersection.
	WindowSlots uint64
	// DominatingDensity is the blocks the dominating peer has delivered in
	// the window.
	DominatingDensity uint64
	// MaxDensity is the most blocks ConnectionId could still have in the
	// window: what it delivered plus every slot it has not yet covered.
	MaxDensity uint64
}

// fragmentWindowBounds returns the lower and upper bound on the number of
// blocks a candidate has in (after, windowEnd] along its own chain. The lower
// bound is what it has delivered. The upper bound adds every slot between its
// head and windowEnd unless the head already reached windowEnd.
//
// A peer that has delivered up to its advertised tip still gets the trailing
// slots: its tip can move forward, so an honest peer sitting at its own tip on
// a short fork is not complete. ouroboros-consensus densityDisconnect only
// disconnects such a peer for a rival offering more than k blocks after the
// anchor, which a k+1 header fragment that still holds the intersection can
// never show.
func fragmentWindowBounds(
	points []ocommon.Point,
	after uint64,
	windowEnd uint64,
) (uint64, uint64) {
	var lower uint64
	for _, p := range points {
		if p.Slot > after && p.Slot <= windowEnd {
			lower++
		}
	}
	head := points[len(points)-1].Slot
	if head >= windowEnd {
		return lower, lower
	}
	return lower, safeAddUint64(lower, windowEnd-head)
}

// genesisDensityDisconnectsLocked returns the peers that are provably sparser
// than another candidate chain, at most once per
// GenesisDensityEvaluationInterval, and marks them so they are reported once.
//
// For each pair of eligible candidates that fork from a common intersection I,
// peer B is provably sparser than peer A when the blocks A has delivered in
// (I, I+window] exceed the most B can ever have there. Candidates that do not
// intersect within their retained fragments, and pairs where either peer is a
// prefix of the other (a peer that is merely behind), are undecidable and
// never disconnected. A peer is only disconnected for a rival that is itself
// still a candidate, so the last remaining peer is never disconnected.
//
// Callers must hold cs.mutex.
func (cs *ChainSelector) genesisDensityDisconnectsLocked() []GenesisDensityDisconnect {
	if cs.config.OnGenesisDensityDisconnect == nil ||
		cs.mode != SelectionModeGenesis {
		return nil
	}
	now := cs.now()
	if !cs.lastGenesisDensityEval.IsZero() &&
		now.Sub(cs.lastGenesisDensityEval) < GenesisDensityEvaluationInterval {
		return nil
	}
	cs.lastGenesisDensityEval = now

	window := cs.genesisWindowSlotsLocked()
	type candidate struct {
		connId   ouroboros.ConnectionId
		fragment CandidateFragment
		points   []ocommon.Point
	}
	candidates := make([]candidate, 0, len(cs.peerTips))
	for connId, peerTip := range cs.peerTips {
		if _, done := cs.genesisDensityDisconnected[connId]; done ||
			!cs.peerCanCorroborateLocked(connId, peerTip) {
			continue
		}
		fragment := candidateFragmentFromHistory(peerTip.observedTipHistory)
		if fragment.Len() == 0 {
			continue
		}
		candidates = append(candidates, candidate{
			connId:   connId,
			fragment: fragment,
			points:   fragment.Points(),
		})
	}

	var out []GenesisDensityDisconnect
	for i := range candidates {
		b := candidates[i]
		for j := range candidates {
			if i == j {
				continue
			}
			a := candidates[j]
			if _, done := cs.genesisDensityDisconnected[a.connId]; done {
				continue
			}
			intersection, ok := a.fragment.Intersect(b.fragment)
			if !ok ||
				intersection.Slot == a.fragment.HeadPoint().Slot ||
				intersection.Slot == b.fragment.HeadPoint().Slot {
				continue
			}
			windowEnd := safeAddUint64(intersection.Slot, window)
			dominating, _ := fragmentWindowBounds(
				a.points, intersection.Slot, windowEnd,
			)
			_, maxDensity := fragmentWindowBounds(
				b.points, intersection.Slot, windowEnd,
			)
			if dominating <= maxDensity {
				continue
			}
			// Mark now rather than after the pass: density is compared at
			// each pair's own intersection and is not transitive, so a peer
			// reported in this pass must not serve as a rival for a later
			// one, or a cycle could report every candidate.
			if cs.genesisDensityDisconnected == nil {
				cs.genesisDensityDisconnected = make(
					map[ouroboros.ConnectionId]struct{},
				)
			}
			cs.genesisDensityDisconnected[b.connId] = struct{}{}
			out = append(out, GenesisDensityDisconnect{
				ConnectionId:           b.connId,
				DominatingConnectionId: a.connId,
				Intersection:           intersection,
				WindowSlots:            window,
				DominatingDensity:      dominating,
				MaxDensity:             maxDensity,
			})
			break
		}
	}
	return out
}
