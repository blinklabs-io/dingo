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
	"encoding/binary"
	"runtime"
	"testing"
	"unsafe"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// joinTestPoints returns count consecutive points starting at block number
// first. fork distinguishes chains that share block numbers.
func joinTestPoints(first uint64, count int, fork byte) []ocommon.Point {
	points := make([]ocommon.Point, count)
	for i := range points {
		hash := make([]byte, 32)
		binary.BigEndian.PutUint64(hash, first+uint64(i))
		hash[31] = fork
		points[i] = ocommon.Point{Slot: first + uint64(i), Hash: hash}
	}
	return points
}

// joinTestSelector holds one peer whose delivered window starts above its
// admitted frontier's, so the eagerness view has to span both.
func joinTestSelector(
	connId ouroboros.ConnectionId,
	base, extension CandidateFragment,
) (*ChainSelector, *PeerChainTip) {
	cs := &ChainSelector{
		eagernessDeliveries: map[ouroboros.ConnectionId]eagernessDelivery{
			connId: {fragment: extension, join: &eagernessJoin{}},
		},
	}
	return cs, &PeerChainTip{
		observedPoints:         base.points,
		observedPointBaseBlock: base.baseBlock,
	}
}

func naiveJoin(base, extension CandidateFragment) CandidateFragment {
	prefix := base.points[:extension.baseBlock-base.baseBlock]
	points := append(append([]ocommon.Point{}, prefix...), extension.points...)
	return CandidateFragment{points: points, baseBlock: base.baseBlock}
}

// The view a connection's next header needs differs from the previous one by
// that header, so building it must not copy both windows again. A connection
// that stays open through a resync fills them, and the copy then ran for every
// delivered header under the selector lock.
//
// Not t.Parallel: runtime.MemStats.TotalAlloc counts the whole process.
func TestEagernessFragmentJoinDoesNotRecopyTheWindows(t *testing.T) {
	const window = 20_000
	connId := newTestConnectionId(1)
	chain := joinTestPoints(1, 3*window, 0)
	base := CandidateFragment{points: chain[:window], baseBlock: 1}
	extension := CandidateFragment{
		points: chain[window/2 : window+1], baseBlock: 1 + window/2,
	}
	cs, peerTip := joinTestSelector(connId, base, extension)
	require.Equal(t, naiveJoin(base, extension),
		cs.eagernessFragmentLocked(connId, peerTip))

	const headers = 200
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for next := window + 2; next < window+2+headers; next++ {
		entry := cs.eagernessDeliveries[connId]
		entry.fragment.points = chain[window/2 : next]
		cs.eagernessDeliveries[connId] = entry
		peerTip.observedPoints = chain[:next-1]
		_ = cs.eagernessFragmentLocked(connId, peerTip)
	}
	runtime.ReadMemStats(&after)
	joinedBytes := uint64(window) * uint64(unsafe.Sizeof(ocommon.Point{}))
	require.Less(t, after.TotalAlloc-before.TotalAlloc, headers*joinedBytes/10,
		"each delivered header must cost an amortized append, not a copy of the view")
	entry := cs.eagernessDeliveries[connId]
	require.Equal(t,
		naiveJoin(fragmentOf(peerTip), entry.fragment),
		cs.eagernessFragmentLocked(connId, peerTip))
}

// The cached view must equal a fresh join after every way the two windows
// move: a delivered header, either window dropping its oldest points, a
// rollback, and a rollback onto another fork at the same heights.
func TestEagernessFragmentJoinMatchesAFreshJoin(t *testing.T) {
	t.Parallel()
	connId := newTestConnectionId(2)
	chain := joinTestPoints(1, 400, 0)
	fork := append(append([]ocommon.Point{}, chain[:150]...),
		joinTestPoints(151, 250, 1)...)
	cs, peerTip := joinTestSelector(connId, CandidateFragment{}, CandidateFragment{})
	step := func(points []ocommon.Point, baseLo, extLo, baseHi, extHi int) {
		t.Helper()
		base := CandidateFragment{
			points: points[baseLo:baseHi], baseBlock: uint64(baseLo) + 1,
		}
		extension := CandidateFragment{
			points: points[extLo:extHi], baseBlock: uint64(extLo) + 1,
		}
		entry := cs.eagernessDeliveries[connId]
		entry.fragment = extension
		cs.eagernessDeliveries[connId] = entry
		peerTip.observedPoints = base.points
		peerTip.observedPointBaseBlock = base.baseBlock
		got := cs.eagernessFragmentLocked(connId, peerTip)
		require.Equal(t, naiveJoin(base, extension), got)
	}
	step(chain, 0, 50, 100, 120)   // first join
	step(chain, 0, 50, 101, 121)   // one header delivered and admitted
	step(chain, 10, 60, 130, 160)  // both windows drop their oldest points
	step(chain, 10, 60, 120, 140)  // rollback on the same chain
	step(chain, 10, 60, 180, 200)  // forward again past the old head
	step(fork, 10, 60, 180, 200)   // another fork at the same heights
	step(fork, 40, 100, 200, 260)  // and on along it
	step(chain, 40, 100, 200, 260) // back to the first chain
}
