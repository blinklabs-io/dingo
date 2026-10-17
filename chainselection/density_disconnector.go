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
	"cmp"
	"slices"
	"sort"
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
	// MaxDensity is the density bound ConnectionId was compared against. For
	// an idling peer this is its delivered density; otherwise it includes all
	// still-possible slots through the window end.
	MaxDensity uint64
	// EagernessStandoff is true when ConnectionId was removed to resolve two
	// forks whose header streams were both held at the Limit on Eagerness.
	EagernessStandoff bool
}

// densityCandidate is one live candidate's retained Genesis frontier.
type densityCandidate struct {
	connId    ouroboros.ConnectionId
	points    []ocommon.Point
	baseBlock uint64
	idling    bool
	held      bool
}

func (c densityCandidate) fragment() CandidateFragment {
	return CandidateFragment{points: c.points, baseBlock: c.baseBlock}
}

// fragmentWindowBounds returns the lower and upper bound on the number of
// blocks a candidate has in (after, windowEnd] along its own chain. The lower
// bound is what it has delivered. The upper bound adds every slot between its
// head and windowEnd unless the head already reached windowEnd.
//
// A peer that has not sent MsgAwaitReply still gets the trailing slots: its tip
// can move forward, so advertised-tip equality alone does not make it idle.
func fragmentWindowBounds(
	points []ocommon.Point,
	after uint64,
	windowEnd uint64,
) (uint64, uint64) {
	first := sort.Search(len(points), func(i int) bool {
		return points[i].Slot > after
	})
	pastWindow := sort.Search(len(points), func(i int) bool {
		return points[i].Slot > windowEnd
	})
	// #nosec G115 -- both indexes are within points and pastWindow >= first.
	lower := uint64(pastWindow - first)
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
// peer B is provably sparser than peer A when A meets B's density bound and
// either B's window is complete or A offers more than k headers after I. An
// explicit MsgAwaitReply lowers B's bound to its delivered density. Candidates
// that do not intersect within their retained fragments and prefix pairs are
// undecidable. A peer is only disconnected for a rival that is itself still a
// candidate, so the last remaining peer is never disconnected.
//
// Callers must hold cs.mutex.
func (cs *ChainSelector) genesisDensityDisconnectsLocked() []GenesisDensityDisconnect {
	// The 3k-slot fallback in genesisWindowSlotsLocked is not a Genesis
	// window: it holds about 3kf blocks, few enough that an honest short
	// fork can lose a complete-window comparison. Only a configured 3k/f
	// window is meaningful here.
	if cs.config.OnGenesisDensityDisconnect == nil ||
		cs.mode != SelectionModeGenesis ||
		cs.config.GenesisWindowSlots == 0 {
		return nil
	}
	now := cs.now()
	if !cs.lastGenesisDensityEval.IsZero() &&
		now.Sub(cs.lastGenesisDensityEval) < GenesisDensityEvaluationInterval {
		return nil
	}
	cs.lastGenesisDensityEval = now

	window := cs.genesisWindowSlotsLocked()
	candidates := cs.densityCandidatesLocked()

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
			intersection, ok := intersectPointFrontiers(
				a.points,
				a.baseBlock,
				b.points,
				b.baseBlock,
			)
			if !ok ||
				intersection.Slot == a.points[len(a.points)-1].Slot ||
				intersection.Slot == b.points[len(b.points)-1].Slot {
				continue
			}
			windowEnd := safeAddUint64(intersection.Slot, window)
			dominating, _ := fragmentWindowBounds(
				a.points, intersection.Slot, windowEnd,
			)
			minDensity, maxDensity := fragmentWindowBounds(
				b.points, intersection.Slot, windowEnd,
			)
			rivalStart := sort.Search(len(a.points), func(i int) bool {
				return a.points[i].Slot > intersection.Slot
			})
			// #nosec G115 -- rivalStart is an index within a.points.
			rivalBlocks := uint64(len(a.points) - rivalStart)
			offersMoreThanK := rivalBlocks > cs.securityParam
			if !offersMoreThanK && minDensity != maxDensity {
				continue
			}
			comparisonBound := maxDensity
			if b.idling {
				comparisonBound = minDensity
			}
			if dominating < comparisonBound {
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
				MaxDensity:             comparisonBound,
			})
			break
		}
	}
	return append(
		out,
		cs.eagernessStandoffDisconnectsLocked(candidates)...,
	)
}

// densityCandidatesLocked returns the density candidates in a stable order so
// a non-transitive comparison cycle has the same survivor on every evaluation.
func (cs *ChainSelector) densityCandidatesLocked() []densityCandidate {
	candidates := make([]densityCandidate, 0, len(cs.peerTips))
	for connId, peerTip := range cs.peerTips {
		if _, done := cs.genesisDensityDisconnected[connId]; done ||
			!cs.peerCanCorroborateLocked(connId, peerTip) {
			continue
		}
		if len(peerTip.observedPoints) == 0 {
			continue
		}
		candidates = append(candidates, densityCandidate{
			connId:    connId,
			points:    peerTip.observedPoints,
			baseBlock: peerTip.observedPointBaseBlock,
			idling:    peerTip.isIdling(),
			held:      peerTip.eagernessPaused > 0,
		})
	}
	slices.SortFunc(candidates, func(a, b densityCandidate) int {
		return cmp.Compare(a.connId.String(), b.connId.String())
	})
	return candidates
}

// eagernessStandoffDisconnectsLocked resolves forks whose streams are both
// held at the Limit on Eagerness. Neither can deliver another header to move
// the common point, so the peer with fewer blocks in the slots both frontiers
// cover loses. Equal density follows the selector's transport ordering.
func (cs *ChainSelector) eagernessStandoffDisconnectsLocked(
	candidates []densityCandidate,
) []GenesisDensityDisconnect {
	var out []GenesisDensityDisconnect
	for i := range candidates {
		for j := i + 1; j < len(candidates); j++ {
			a, b := candidates[i], candidates[j]
			if !a.held || !b.held {
				continue
			}
			if _, done := cs.genesisDensityDisconnected[a.connId]; done {
				continue
			}
			if _, done := cs.genesisDensityDisconnected[b.connId]; done {
				continue
			}
			fragmentA, fragmentB := a.fragment(), b.fragment()
			intersection, ok := fragmentA.Intersect(fragmentB)
			if !ok ||
				intersection.Slot == fragmentA.HeadPoint().Slot ||
				intersection.Slot == fragmentB.HeadPoint().Slot {
				continue
			}
			compared := min(
				fragmentA.HeadPoint().Slot,
				fragmentB.HeadPoint().Slot,
			)
			countA, _ := fragmentWindowBounds(
				a.points, intersection.Slot, compared,
			)
			countB, _ := fragmentWindowBounds(
				b.points, intersection.Slot, compared,
			)
			winner, loser := a, b
			winnerCount, loserCount := countA, countB
			switch {
			case countB > countA:
				winner, loser = b, a
				winnerCount, loserCount = countB, countA
			case countA == countB &&
				cs.compareTransportLocked(a.connId, b.connId, true) ==
					ChainBBetter:
				winner, loser = b, a
			}
			cs.markDensityDisconnectedLocked(loser.connId)
			out = append(out, GenesisDensityDisconnect{
				ConnectionId:           loser.connId,
				DominatingConnectionId: winner.connId,
				Intersection:           intersection,
				WindowSlots:            compared - intersection.Slot,
				DominatingDensity:      winnerCount,
				MaxDensity:             loserCount,
				EagernessStandoff:      true,
			})
		}
	}
	return out
}

func (cs *ChainSelector) markDensityDisconnectedLocked(
	connId ouroboros.ConnectionId,
) {
	if cs.genesisDensityDisconnected == nil {
		cs.genesisDensityDisconnected = make(
			map[ouroboros.ConnectionId]struct{},
		)
	}
	cs.genesisDensityDisconnected[connId] = struct{}{}
}
