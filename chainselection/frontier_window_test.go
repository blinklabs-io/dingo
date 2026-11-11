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
	"fmt"
	"math/rand"
	"testing"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// frontierModel is the observed frontier with every holder owning private,
// deep-copied storage. It restates recordObservedPoint and ApplyRollback with
// plain slices so the shared-window implementation can be compared against it.
type frontierModel struct {
	slots  []uint64
	points []ocommon.Point
	base   uint64
}

func (m *frontierModel) clone() *frontierModel {
	return &frontierModel{
		slots:  append([]uint64(nil), m.slots...),
		points: cloneObservedPoints(m.points),
		base:   m.base,
	}
}

func (m *frontierModel) record(
	point ocommon.Point,
	block, window, maxPoints uint64,
) {
	slot := point.Slot
	for len(m.slots) > 0 && m.slots[len(m.slots)-1] > slot {
		m.slots = m.slots[:len(m.slots)-1]
	}
	if len(m.slots) == 0 || m.slots[len(m.slots)-1] < slot {
		m.slots = append(m.slots, slot)
	}
	var tail uint64
	if len(m.points) > 0 {
		tail = m.base + uint64(len(m.points)-1)
	}
	switch {
	case len(m.points) == 0:
		m.base = block
		m.points = append(m.points, clonePoint(point))
	case m.points[len(m.points)-1].Slot == slot && block == tail:
		m.points[len(m.points)-1] = clonePoint(point)
	case m.points[len(m.points)-1].Slot < slot &&
		block > m.base && block-m.base == uint64(len(m.points)):
		m.points = append(m.points, clonePoint(point))
	default:
		m.points = []ocommon.Point{clonePoint(point)}
		m.base = block
	}
	if uint64(len(m.points)) > maxPoints {
		excess := uint64(len(m.points)) - maxPoints
		m.points = m.points[excess:]
		m.base += excess
	}
	var cutoff uint64 = 1
	if slot > window {
		cutoff = slot - window + 1
	}
	idx := 0
	for idx < len(m.slots) && m.slots[idx] < cutoff {
		idx++
	}
	m.slots = m.slots[idx:]
}

func (m *frontierModel) rollback(point ocommon.Point) {
	keepSlots := 0
	for keepSlots < len(m.slots) && m.slots[keepSlots] <= point.Slot {
		keepSlots++
	}
	m.slots = m.slots[:keepSlots]
	keep := 0
	for i, p := range m.points {
		if pointsEqual(p, point) {
			keep = i + 1
		}
	}
	if keep == 0 && len(m.points) > 0 {
		m.slots = nil
	}
	m.points = m.points[:keep]
	if len(m.slots) == 0 {
		m.slots = nil
	}
	if keep == 0 {
		m.points = nil
		m.base = 0
	}
}

func requireFrontierMatches(
	t *testing.T,
	step int,
	holder int,
	tip *PeerChainTip,
	model *frontierModel,
) {
	t.Helper()
	require.Equal(t, len(model.slots), len(tip.observedSlots),
		"step %d holder %d slots length", step, holder)
	for i := range model.slots {
		require.Equal(t, model.slots[i], tip.observedSlots[i],
			"step %d holder %d slot %d", step, holder, i)
	}
	require.Equal(t, len(model.points), len(tip.observedPoints),
		"step %d holder %d points length", step, holder)
	for i := range model.points {
		require.True(t, pointsEqual(model.points[i], tip.observedPoints[i]),
			"step %d holder %d point %d: want %d/%x got %d/%x",
			step, holder, i,
			model.points[i].Slot, model.points[i].Hash,
			tip.observedPoints[i].Slot, tip.observedPoints[i].Hash)
	}
	if len(model.points) > 0 {
		require.Equal(t, model.base, tip.observedPointBaseBlock,
			"step %d holder %d base block", step, holder)
	}
}

// Holders created by clonePeerTipAdmissionFrontier share one frontier. Random
// appends, forks, rollbacks and re-clones across the holders must leave each
// holder equal to a model that never shares storage; any write that reaches
// another holder's view diverges from the model.
func TestFrontierWindowSharedHoldersMatchIndependentModel(t *testing.T) {
	t.Parallel()
	const (
		window    = 40
		maxPoints = 12
		holders   = 4
	)
	for seed := range int64(20) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewSource(seed))
			cs := NewChainSelector(ChainSelectorConfig{
				GenesisMode:               true,
				SecurityParam:             5,
				GenesisWindowSlots:        window,
				DisableEventSubscriptions: true,
			})
			tips := make([]*PeerChainTip, holders)
			models := make([]*frontierModel, holders)
			for i := range tips {
				tips[i] = &PeerChainTip{}
				models[i] = &frontierModel{}
			}
			next := func(tip *PeerChainTip) (ocommon.Point, uint64) {
				if len(tip.observedPoints) == 0 {
					return ocommon.Point{
						Slot: 1,
						Hash: []byte{byte(rng.Intn(3)), 1},
					}, 1
				}
				head := tip.observedPoints[len(tip.observedPoints)-1]
				block := tip.observedPointBaseBlock +
					uint64(len(tip.observedPoints))
				return ocommon.Point{
					Slot: head.Slot + uint64(1+rng.Intn(3)),
					// Few distinct hashes make independent holders append
					// equal points, which is the adoption path.
					Hash: []byte{byte(rng.Intn(3)), byte(block)},
				}, block
			}
			// 400 steps pass the 2*len+16 window size repeatedly, so rebases
			// are covered too.
			for step := range 400 {
				i := rng.Intn(holders)
				switch op := rng.Intn(10); {
				case op < 6:
					point, block := next(tips[i])
					buf := append([]byte(nil), point.Hash...)
					tips[i].recordObservedPoint(
						ocommon.Point{Slot: point.Slot, Hash: buf},
						block, window, maxPoints, true,
					)
					// The stored point must not alias the caller's buffer.
					buf[0] ^= 0xff
					models[i].record(point, block, window, maxPoints)
				case op < 7 && len(tips[i].observedPoints) > 0:
					// Same slot and block number, different hash: replaces
					// the tail.
					head := tips[i].observedPoints[len(tips[i].observedPoints)-1]
					block := tips[i].observedPointBaseBlock +
						uint64(len(tips[i].observedPoints)-1)
					point := ocommon.Point{
						Slot: head.Slot,
						Hash: []byte{byte(rng.Intn(3)), 9},
					}
					tips[i].recordObservedPoint(point, block, window, maxPoints, true)
					models[i].record(point, block, window, maxPoints)
				case op < 8 && len(tips[i].observedPoints) > 0:
					point := tips[i].observedPoints[rng.Intn(len(tips[i].observedPoints))]
					tips[i].ApplyRollback(point, ochainsync.Tip{})
					models[i].rollback(point)
				default:
					j := rng.Intn(holders)
					tips[i] = cs.clonePeerTipAdmissionFrontier(tips[j])
					models[i] = models[j].clone()
				}
				for h := range tips {
					requireFrontierMatches(t, step, h, tips[h], models[h])
				}
			}
		})
	}
}

// A staged frontier shares history with the committed peer. Appending to,
// rolling back, or trimming either side must not change the other, and the
// stored hash bytes must not follow the caller's buffer.
func TestClonedAdmissionFrontierDoesNotAliasCommittedPeer(t *testing.T) {
	t.Parallel()
	const window, maxPoints = 1000, 6
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:               true,
		SecurityParam:             5,
		GenesisWindowSlots:        window,
		DisableEventSubscriptions: true,
	})
	committed := &PeerChainTip{}
	for block := uint64(1); block <= 6; block++ {
		committed.recordObservedPoint(
			ocommon.Point{Slot: block, Hash: []byte{'c', byte(block)}},
			block, window, maxPoints, true,
		)
	}
	want := cloneObservedPoints(committed.observedPoints)
	wantSlots := append([]uint64(nil), committed.observedSlots...)
	wantBase := committed.observedPointBaseBlock

	frontier := cs.clonePeerTipAdmissionFrontier(committed)
	// A fork at the head and a trim of the oldest point on the staged side.
	frontier.recordObservedPoint(
		ocommon.Point{Slot: 7, Hash: []byte{'f', 7}}, 7, window, maxPoints, true,
	)
	frontier.ApplyRollback(
		ocommon.Point{Slot: 4, Hash: []byte{'c', 4}}, ochainsync.Tip{},
	)
	frontier.recordObservedPoint(
		ocommon.Point{Slot: 5, Hash: []byte{'f', 5}}, 5, window, maxPoints, true,
	)

	require.Equal(t, want, committed.observedPoints,
		"staged-frontier writes must not change the committed points")
	require.Equal(t, wantSlots, committed.observedSlots)
	require.Equal(t, wantBase, committed.observedPointBaseBlock)
	require.Equal(t, []byte{'c', 5}, committed.observedPoints[4].Hash)

	// The committed peer advancing past the staged copy leaves it unchanged.
	stagedBefore := cloneObservedPoints(frontier.observedPoints)
	committed.recordObservedPoint(
		ocommon.Point{Slot: 7, Hash: []byte{'c', 7}}, 7, window, maxPoints, true,
	)
	require.Equal(t, stagedBefore, frontier.observedPoints,
		"committed writes must not change the staged points")
}

// Mutating the deep-copy a selector getter returns must not reach the
// frontier shared by staged copies.
func TestPeerTipGetterCopyDoesNotAliasSharedFrontier(t *testing.T) {
	t.Parallel()
	const window, maxPoints = 1000, 6
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:               true,
		SecurityParam:             5,
		GenesisWindowSlots:        window,
		DisableEventSubscriptions: true,
	})
	conn := newTestConnectionId(1)
	committed := &PeerChainTip{ConnectionId: conn}
	for block := uint64(1); block <= 4; block++ {
		committed.recordObservedPoint(
			ocommon.Point{Slot: block, Hash: []byte{'c', byte(block)}},
			block, window, maxPoints, true,
		)
	}
	cs.mutex.Lock()
	cs.peerTips[conn] = committed
	staged := cs.clonePeerTipAdmissionFrontier(committed)
	cs.mutex.Unlock()

	got := cs.GetAllPeerTips()[conn]
	require.NotNil(t, got)
	require.Len(t, got.observedPoints, 4)
	got.observedPoints[0].Hash[0] = 'x'
	got.observedPoints[3] = ocommon.Point{Slot: 99, Hash: []byte("x")}
	got.observedSlots[0] = 99

	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	for _, tip := range []*PeerChainTip{committed, staged} {
		require.Equal(t, []byte{'c', 1}, tip.observedPoints[0].Hash)
		require.Equal(t, uint64(4), tip.observedPoints[3].Slot)
		require.Equal(t, uint64(1), tip.observedSlots[0])
	}
}

func deliveryTestPoints(from, to uint64, tag byte) []ocommon.Point {
	var points []ocommon.Point
	for block := from; block <= to; block++ {
		points = append(points, ocommon.Point{
			Slot: block * 10,
			Hash: []byte{tag, byte(block), 7},
		})
	}
	return points
}

// A rebuilt delivery entry equals a full copy of the admitted fragment in
// value, and never shares hash bytes with it, whichever prefix of the previous
// entry it reuses.
func TestRebuildEagernessDeliveryEqualsFullCopyAndOwnsHashes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		previous CandidateFragment
		base     CandidateFragment
	}{
		{
			name:     "base extends previous",
			previous: CandidateFragment{points: deliveryTestPoints(10, 20, 'a'), baseBlock: 10},
			base:     CandidateFragment{points: deliveryTestPoints(10, 25, 'a'), baseBlock: 10},
		},
		{
			name:     "base trimmed past previous start",
			previous: CandidateFragment{points: deliveryTestPoints(10, 20, 'a'), baseBlock: 10},
			base:     CandidateFragment{points: deliveryTestPoints(13, 25, 'a'), baseBlock: 13},
		},
		{
			name:     "previous diverges",
			previous: CandidateFragment{points: deliveryTestPoints(10, 20, 'b'), baseBlock: 10},
			base:     CandidateFragment{points: deliveryTestPoints(10, 25, 'a'), baseBlock: 10},
		},
		{
			name:     "previous starts after base",
			previous: CandidateFragment{points: deliveryTestPoints(12, 20, 'a'), baseBlock: 12},
			base:     CandidateFragment{points: deliveryTestPoints(10, 25, 'a'), baseBlock: 10},
		},
		{
			name:     "previous empty",
			previous: CandidateFragment{},
			base:     CandidateFragment{points: deliveryTestPoints(10, 25, 'a'), baseBlock: 10},
		},
		{
			name:     "previous ahead of base",
			previous: CandidateFragment{points: deliveryTestPoints(10, 30, 'a'), baseBlock: 10},
			base:     CandidateFragment{points: deliveryTestPoints(10, 25, 'a'), baseBlock: 10},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Own the inputs: previous is held privately by an entry.
			previous := CandidateFragment{
				points:    cloneObservedPoints(tc.previous.points),
				baseBlock: tc.previous.baseBlock,
			}
			want := cloneObservedPoints(tc.base.points)
			got := rebuildEagernessDelivery(
				previous, windowRef[ocommon.Point]{}, tc.base,
			)
			require.Equal(t, tc.base.baseBlock, got.fragment.baseBlock)
			require.Equal(t, want, got.fragment.points)
			for _, point := range tc.base.points {
				for i := range point.Hash {
					point.Hash[i] ^= 0xff
				}
			}
			require.Equal(t, want, got.fragment.points,
				"rebuilt entry must not alias the admitted fragment's hashes")
		})
	}
}
