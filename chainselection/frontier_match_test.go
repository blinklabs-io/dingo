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
	"math/rand/v2"
	"sort"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// The tests below that "poison" a point rewrite a hash in place, which a
// frontier array never sees in production. A comparison that reads the
// poisoned point again fails, so a passing result shows that a delivered
// header is not paid for with another read of the connection's whole history.

func poison(point *ocommon.Point) { point.Hash[0] ^= 0xff }

// matchTestFrontier is a frontier built through the window helpers, as the
// selector builds them.
type matchTestFrontier struct {
	ref    windowRef[ocommon.Point]
	points []ocommon.Point
	base   uint64
}

func (f *matchTestFrontier) append(point ocommon.Point) {
	f.points = windowAppend(&f.ref, f.points, clonePoint(point), pointsEqual)
}

func (f *matchTestFrontier) trimFront(n int) {
	f.points = windowTrimFront(&f.ref, f.points, n)
	f.base += uint64(n)
}

// rollback keeps the first keep points and continues on a new array, as a
// frontier does once a different point is appended after a truncation.
func (f *matchTestFrontier) rollback(keep int) {
	f.points = windowTruncate(f.points, keep)
}

func (f *matchTestFrontier) fragment() CandidateFragment {
	return CandidateFragment{points: f.points, baseBlock: f.base}
}

func (f *matchTestFrontier) tip() *PeerChainTip {
	return &PeerChainTip{
		observedPoints: f.points, observedPointBaseBlock: f.base,
	}
}

func newMatchTestFrontier(points []ocommon.Point, base uint64) *matchTestFrontier {
	f := &matchTestFrontier{base: base}
	for _, point := range points {
		f.append(point)
	}
	return f
}

// confirmsRecentChainWithin is a comparison that remembers nothing.
func (p *PeerChainTip) confirmsRecentChainWithin(
	witness *PeerChainTip,
	window uint64,
) bool {
	var match witnessMatch
	return p.confirmsRecentChainFrom(witness, window, &match)
}

// confirmsRecentChainInFull is the scan before comparisons were remembered,
// kept as the reference the remembered one must agree with.
func (p *PeerChainTip) confirmsRecentChainInFull(
	witness *PeerChainTip,
	window uint64,
) bool {
	if p == nil || witness == nil ||
		len(p.observedPoints) == 0 || len(witness.observedPoints) == 0 {
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
	for _, w := range witnessPoints {
		if w.Slot > hi {
			break
		}
		for i < len(candidate) && candidate[i].Slot < w.Slot {
			i++
		}
		if i < len(candidate) && candidate[i].Slot == w.Slot &&
			len(w.Hash) > 0 && bytes.Equal(candidate[i].Hash, w.Hash) {
			tipMatched = i == len(candidate)-1
			continue
		}
		// The witness observed a block within the candidate's range that the
		// candidate does not have (or a conflicting hash at the same slot):
		// they are on different chains, so this witness does not confirm.
		return false
	}
	return tipMatched
}

func TestEagernessDeliveryExtendsDoesNotRereadComparedHistory(t *testing.T) {
	t.Parallel()
	chain := joinTestPoints(1, 300, 0)
	base := newMatchTestFrontier(chain[:200], 1)
	delivered := newMatchTestFrontier(chain[50:201], 51)
	entry := eagernessDelivery{join: &eagernessJoin{}}
	entry.fragment = delivered.fragment()
	require.True(t, entry.extends(base.fragment()))

	base.append(chain[200])
	delivered.append(chain[201])
	entry.fragment = delivered.fragment()
	poison(&base.points[100])
	require.True(t, entry.extends(base.fragment()),
		"a delivered header must compare only the points it adds")

	// A point the comparison has not covered is still read.
	base.append(chain[201])
	delivered.append(chain[202])
	entry.fragment = delivered.fragment()
	poison(&delivered.points[len(delivered.points)-2])
	require.False(t, entry.extends(base.fragment()))
}

func TestEagernessProofOverlapDoesNotRereadComparedHistory(t *testing.T) {
	t.Parallel()
	chain := joinTestPoints(1, 300, 0)
	legacy := newMatchTestFrontier(chain[:200], 1)
	delivered := newMatchTestFrontier(chain[50:220], 51)
	entry := eagernessDelivery{join: &eagernessJoin{}}
	require.True(t, entry.proofOverlaps(delivered.fragment(), legacy.fragment()))

	legacy.append(chain[200])
	poison(&legacy.points[100])
	require.True(t, entry.proofOverlaps(delivered.fragment(), legacy.fragment()),
		"a delivered header must compare only the points it adds")

	legacy.append(chain[201])
	poison(&legacy.points[len(legacy.points)-1])
	require.False(t, entry.proofOverlaps(delivered.fragment(), legacy.fragment()))
}

func TestWitnessConfirmationDoesNotRereadComparedHistory(t *testing.T) {
	t.Parallel()
	chain := joinTestPoints(1, 300, 0)
	candidate := newMatchTestFrontier(chain[:200], 1)
	witness := newMatchTestFrontier(chain[20:200], 21)
	var matches witnessMatches
	key := witnessMatchKey{
		candidate: newTestConnectionId(1), witness: newTestConnectionId(2),
	}
	require.True(t, matches.confirms(key, candidate.tip(), witness.tip(), 0))

	// A witness one header behind does not confirm, and catching up costs
	// that header only.
	candidate.append(chain[200])
	poison(&witness.points[90])
	require.False(t, matches.confirms(key, candidate.tip(), witness.tip(), 0))
	witness.append(chain[200])
	require.True(t, matches.confirms(key, candidate.tip(), witness.tip(), 0),
		"a delivered header must compare only the points it adds")

	// A new witness point that the candidate lacks is still found.
	candidate.append(chain[201])
	witness.append(joinTestPoints(202, 1, 1)[0])
	require.False(t, matches.confirms(key, candidate.tip(), witness.tip(), 0))

	matches.forget(key.witness)
	require.Empty(t, matches.matches)
}

// Each remembered comparison must give the answer of the full one after
// every way two frontiers move: delivered headers, a window dropping its
// oldest points, a rollback, and a rollback onto another fork.
func TestRememberedComparisonsMatchFullComparisons(t *testing.T) {
	t.Parallel()
	reused, held := 0, 0
	for seed := uint64(1); seed <= 40; seed++ {
		rng := rand.New(rand.NewPCG(seed, 7))
		chains := [][]ocommon.Point{joinTestPoints(1, 400, 0)}
		for fork := byte(1); fork <= 2; fork++ {
			split := 60 + rng.IntN(200)
			chains = append(chains, append(
				append([]ocommon.Point{}, chains[0][:split]...),
				joinTestPoints(uint64(split)+1, 400-split, fork)...,
			))
		}
		// lower and upper play base and extension, legacy and proof, and
		// witness and candidate in turn.
		lower := newMatchTestFrontier(chains[0][:40], 1)
		upper := newMatchTestFrontier(chains[0][10:45], 11)
		lowerChain, upperChain := 0, 0
		var extends, overlap fragmentMatch
		var confirm witnessMatch
		for step := range 400 {
			f, chain := lower, &lowerChain
			if rng.IntN(2) == 0 {
				f, chain = upper, &upperChain
			}
			head := int(f.base) - 1 + len(f.points)
			switch op := rng.IntN(10); {
			case op < 6 && head < 399:
				f.append(chains[*chain][head])
			case op < 8 && len(f.points) > 5:
				f.trimFront(1 + rng.IntN(3))
			case len(f.points) > 5:
				keep := len(f.points) - 1 - rng.IntN(4)
				f.rollback(keep)
				if rng.IntN(2) == 0 {
					*chain = rng.IntN(len(chains))
				}
			}
			window := uint64(rng.IntN(3) * 50)
			if extends.base != nil {
				reused++
			}
			if confirm.candidate != nil {
				held++
			}
			require.Equal(t,
				extendsFragment(upper.fragment(), lower.fragment()),
				extendsFragmentFrom(upper.fragment(), lower.fragment(), &extends),
				"extends, seed %d step %d", seed, step)
			require.Equal(t,
				fragmentsOverlapExactly(upper.fragment(), lower.fragment()),
				fragmentsOverlapExactlyFrom(
					upper.fragment(), lower.fragment(), &overlap,
				),
				"overlap, seed %d step %d", seed, step)
			require.Equal(t,
				upper.tip().confirmsRecentChainInFull(lower.tip(), window),
				upper.tip().confirmsRecentChainFrom(lower.tip(), window, &confirm),
				"confirms, seed %d step %d", seed, step)
		}
	}
	require.Greater(t, reused, 1000, "the walk must mostly continue a comparison")
	require.Greater(t, held, 1000, "the walk must mostly continue a comparison")
}

func TestObservedDensityCountsSlotsInTheWindow(t *testing.T) {
	t.Parallel()
	tip := &PeerChainTip{observedSlots: []uint64{3, 5, 8, 13, 21, 34}}
	for window, want := range map[uint64]uint64{
		0: 6, 1: 1, 13: 1, 14: 2, 22: 3, 27: 4, 30: 5, 32: 6, 34: 6, 100: 6,
	} {
		require.Equal(t, want, tip.observedDensity(window), "window %d", window)
	}
	require.Zero(t, (&PeerChainTip{}).observedDensity(10))
}
