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
	"sync"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// The comparisons in this file run for every delivered header and used to
// read both frontiers in full. A frontier holds up to a Genesis window of
// points, and a connection that stays open fills it, so their cost grew with
// the connection's age until the selector lock was held for most of a second.
//
// Each comparison now remembers how far it got, by the addresses of the last
// points it compared. Frontier views are slices of arrays that are only ever
// written past their used length (see frontierWindow), and every other view
// is a fresh array. A view whose point at a remembered position still has the
// remembered address is therefore the same array, holding below that position
// what it held when it was compared. The remembered pointer keeps the array
// alive, so an address cannot be reused by another one. Anything else, a
// rollback onto a new array included, fails the address test and is compared
// in full.

// fragmentMatch records that two fragments were equal on block numbers lo
// through hi. base and extension are the addresses of their points at hi.
type fragmentMatch struct {
	lo, hi          uint64
	base, extension *ocommon.Point
}

// extendsFragmentFrom is extendsFragment that compares only block numbers
// above those match already covers, and updates match. The caller serializes
// access to match.
func extendsFragmentFrom(
	extension, base CandidateFragment,
	match *fragmentMatch,
) bool {
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
	lo, from := start, start
	if match.base != nil && match.lo <= start && match.hi >= start &&
		match.hi <= baseHead &&
		&base.points[match.hi-base.baseBlock] == match.base &&
		&extension.points[match.hi-extension.baseBlock] == match.extension {
		lo, from = match.lo, match.hi+1
	}
	for number := from; number <= baseHead; number++ {
		left := base.points[number-base.baseBlock]
		right := extension.points[number-extension.baseBlock]
		if left.Slot != right.Slot || !bytes.Equal(left.Hash, right.Hash) {
			*match = fragmentMatch{}
			return false
		}
	}
	*match = fragmentMatch{
		lo: lo, hi: baseHead,
		base:      &base.points[baseHead-base.baseBlock],
		extension: &extension.points[baseHead-extension.baseBlock],
	}
	return true
}

// fragmentsOverlapExactlyFrom is fragmentsOverlapExactly that compares only
// block numbers above those match already covers, and updates match. match
// names left as its extension and right as its base. The caller serializes
// access to match.
func fragmentsOverlapExactlyFrom(
	left, right CandidateFragment,
	match *fragmentMatch,
) bool {
	leftHead, leftOK := eagernessFragmentHead(left)
	rightHead, rightOK := eagernessFragmentHead(right)
	start, end := max(left.baseBlock, right.baseBlock), min(leftHead, rightHead)
	if !leftOK || !rightOK || start > end {
		return false
	}
	lo, from := start, start
	if match.base != nil && match.lo <= start && match.hi >= start &&
		match.hi <= end &&
		&right.points[match.hi-right.baseBlock] == match.base &&
		&left.points[match.hi-left.baseBlock] == match.extension {
		lo, from = match.lo, match.hi+1
	}
	for number := from; number <= end; number++ {
		a, b := left.points[number-left.baseBlock], right.points[number-right.baseBlock]
		if a.Slot != b.Slot || !bytes.Equal(a.Hash, b.Hash) {
			*match = fragmentMatch{}
			return false
		}
	}
	*match = fragmentMatch{
		lo: lo, hi: end,
		base:      &right.points[end-right.baseBlock],
		extension: &left.points[end-left.baseBlock],
	}
	return true
}

// witnessMatch records that every point a witness holds in slots lo through
// hi is a candidate's point. candidate and witness are the addresses of their
// points at slot hi.
type witnessMatch struct {
	lo, hi             uint64
	candidate, witness *ocommon.Point
}

// witnessMatchKey names one comparison: a candidate, a witness, and whether
// the witness view was its committed frontier or its verified proof.
type witnessMatchKey struct {
	candidate, witness ouroboros.ConnectionId
	verified           bool
}

// maxWitnessMatches bounds the remembered comparisons. Each can keep one
// retired frontier array alive.
const maxWitnessMatches = 256

type witnessMatches struct {
	mu      sync.Mutex
	matches map[witnessMatchKey]witnessMatch
}

// forget drops what is remembered about connId.
func (m *witnessMatches) forget(connId ouroboros.ConnectionId) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.matches {
		if key.candidate == connId || key.witness == connId {
			delete(m.matches, key)
		}
	}
}

// confirms reports candidate.confirmsRecentChainFrom(witness, window) with
// the comparison remembered under key.
func (m *witnessMatches) confirms(
	key witnessMatchKey,
	candidate, witness *PeerChainTip,
	window uint64,
) bool {
	if candidate == nil || witness == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	match := m.matches[key]
	confirmed := candidate.confirmsRecentChainFrom(witness, window, &match)
	if match.candidate == nil {
		delete(m.matches, key)
		return confirmed
	}
	if _, ok := m.matches[key]; !ok && len(m.matches) >= maxWitnessMatches {
		clear(m.matches)
	}
	if m.matches == nil {
		m.matches = make(map[witnessMatchKey]witnessMatch)
	}
	m.matches[key] = match
	return confirmed
}

// confirmsRecentChainFrom reports whether witness confirms this peer's (candidate's)
// chain. Within the slot range of the candidate's observed frontier, every
// witness-observed point must match the candidate, and the witness must have
// independently delivered the candidate's current point.
//
// This is deliberately stronger than "share any common point": a fast source
// that shares only an old ancestor and then diverges for every later block is
// NOT confirmed, because the witness observed recent blocks the candidate did
// not (or a conflicting hash at the same slot). A witness whose frontier does
// not overlap the candidate's window at all cannot confirm it (returns false),
// so corroboration fails closed — the candidate then stalls rather than being
// followed uncorroborated. Requiring the current point is what keeps a match
// in older candidate history, however recent, from authorizing candidate
// blocks the witness has never observed.
//
// Both frontiers are kept in strictly-ascending slot order; this is a
// two-pointer scan that starts the witness at the candidate's first slot. It
// relies on the observed frontier being populated per header (dense) during
// chainsync, so two peers on the same chain share every block in their
// overlap.
//
// The scan resumes after the witness points match already covers, and updates
// match. The caller serializes access to match.
func (p *PeerChainTip) confirmsRecentChainFrom(
	witness *PeerChainTip,
	window uint64,
	match *witnessMatch,
) bool {
	if p == nil || witness == nil ||
		len(p.observedPoints) == 0 || len(witness.observedPoints) == 0 {
		*match = witnessMatch{}
		return false
	}
	candidate := p.observedPoints
	if window > 0 {
		hi := candidate[len(candidate)-1].Slot
		var cutoff uint64
		if hi >= window {
			cutoff = hi - window + 1
		}
		candidate = candidate[sort.Search(len(candidate), func(i int) bool {
			return candidate[i].Slot >= cutoff
		}):]
	}
	lo := candidate[0].Slot
	hi := candidate[len(candidate)-1].Slot
	witnessPoints := witness.observedPoints
	witnessPoints = witnessPoints[sort.Search(len(witnessPoints), func(i int) bool {
		return witnessPoints[i].Slot >= lo
	}):]
	i := 0
	tipMatched := false
	matchLo := lo
	next := 0
	matched := false
	var matchedCandidate, matchedWitness int
	if match.candidate != nil && match.lo <= lo && match.hi >= lo &&
		match.hi <= hi {
		ci := sort.Search(len(candidate), func(i int) bool {
			return candidate[i].Slot >= match.hi
		})
		wi := sort.Search(len(witnessPoints), func(i int) bool {
			return witnessPoints[i].Slot >= match.hi
		})
		if ci < len(candidate) && wi < len(witnessPoints) &&
			&candidate[ci] == match.candidate &&
			&witnessPoints[wi] == match.witness {
			i, next, matchLo = ci, wi+1, match.lo
			matched, matchedCandidate, matchedWitness = true, ci, wi
			tipMatched = ci == len(candidate)-1
		}
	}
	for wIdx := next; wIdx < len(witnessPoints); wIdx++ {
		w := witnessPoints[wIdx]
		if w.Slot > hi {
			break
		}
		for i < len(candidate) && candidate[i].Slot < w.Slot {
			i++
		}
		if i < len(candidate) && candidate[i].Slot == w.Slot &&
			len(w.Hash) > 0 && bytes.Equal(candidate[i].Hash, w.Hash) {
			tipMatched = i == len(candidate)-1
			matched, matchedCandidate, matchedWitness = true, i, wIdx
			continue
		}
		// The witness observed a block within the candidate's range that the
		// candidate does not have (or a conflicting hash at the same slot):
		// they are on different chains, so this witness does not confirm.
		*match = witnessMatch{}
		return false
	}
	if !matched {
		*match = witnessMatch{}
		return tipMatched
	}
	*match = witnessMatch{
		lo: matchLo, hi: candidate[matchedCandidate].Slot,
		candidate: &candidate[matchedCandidate],
		witness:   &witnessPoints[matchedWitness],
	}
	return tipMatched
}
