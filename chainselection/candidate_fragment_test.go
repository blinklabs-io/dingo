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
	"testing"

	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCandidateFragment_EmptyFragmentIsZeroValue(t *testing.T) {
	var f CandidateFragment
	assert.Equal(t, 0, f.Len())
	assert.Equal(t, ocommon.Point{}, f.Anchor())
	assert.Equal(t, ocommon.Point{}, f.HeadPoint())
	assert.Nil(t, f.Points())
	_, ok := f.Intersect(CandidateFragment{})
	assert.False(t, ok)
}

func TestPeerCandidateFragmentWithoutDeliveredHistoryIsEmpty(t *testing.T) {
	fragment := (&PeerChainTip{}).CandidateFragment()
	assert.Equal(t, 0, fragment.Len())
}

// TestCandidateFragment_AnchorAndHeadPointReturnDefensiveCopies ensures a
// caller mutating the Hash returned by Anchor or HeadPoint cannot corrupt the
// fragment's own backing bytes, which would otherwise change later Points()
// and Intersect() results.
func TestCandidateFragment_AnchorAndHeadPointReturnDefensiveCopies(
	t *testing.T,
) {
	fragment := CandidateFragment{points: []ocommon.Point{
		{Slot: 100, Hash: []byte("anchor-hash")},
		{Slot: 200, Hash: []byte("head-hash")},
	}, baseBlock: 1}

	anchor := fragment.Anchor()
	for i := range anchor.Hash {
		anchor.Hash[i] = 0xff
	}
	head := fragment.HeadPoint()
	for i := range head.Hash {
		head.Hash[i] = 0xff
	}

	points := fragment.Points()
	require.Len(t, points, 2)
	assert.Equal(t, "anchor-hash", string(points[0].Hash))
	assert.Equal(t, "head-hash", string(points[1].Hash))

	other := CandidateFragment{points: []ocommon.Point{
		{Slot: 100, Hash: []byte("anchor-hash")},
	}, baseBlock: 1}
	point, ok := fragment.Intersect(other)
	require.True(t, ok)
	assert.Equal(t, "anchor-hash", string(point.Hash))
}

func TestCandidateFragment_AnchorAndHeadPoint(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{SecurityParam: 10})
	connId := newTestConnectionId(1)

	for i, slot := range []uint64{100, 200, 300} {
		cs.UpdatePeerTip(connId, ochainsync.Tip{
			Point: ocommon.Point{
				Slot: slot,
				Hash: []byte(fmt.Sprintf("h%d", i)),
			},
			BlockNumber: uint64(i + 1),
		}, nil)
	}

	fragment, ok := cs.GetCandidateFragment(connId)
	require.True(t, ok)
	require.Equal(t, 3, fragment.Len())
	assert.Equal(t, uint64(100), fragment.Anchor().Slot)
	assert.Equal(t, uint64(300), fragment.HeadPoint().Slot)

	points := fragment.Points()
	require.Len(t, points, 3)
	assert.Equal(t, []uint64{100, 200, 300}, []uint64{
		points[0].Slot, points[1].Slot, points[2].Slot,
	})
}

func TestCandidateFragment_IntersectFindsHighestCommonPoint(t *testing.T) {
	shared := ocommon.Point{Slot: 200, Hash: []byte("shared")}
	a := CandidateFragment{points: []ocommon.Point{
		{Slot: 100, Hash: []byte("a100")},
		shared,
		{Slot: 300, Hash: []byte("a300")},
	}, baseBlock: 1}
	b := CandidateFragment{points: []ocommon.Point{
		shared,
		{Slot: 250, Hash: []byte("b250")},
	}, baseBlock: 2}

	point, ok := a.Intersect(b)
	require.True(t, ok)
	assert.Equal(t, shared.Slot, point.Slot)
	assert.Equal(t, shared.Hash, point.Hash)

	// Intersection is symmetric.
	point, ok = b.Intersect(a)
	require.True(t, ok)
	assert.Equal(t, shared.Slot, point.Slot)
}

func TestCandidateFragment_IntersectSameSlotDifferentHashIsNotAMatch(
	t *testing.T,
) {
	a := CandidateFragment{points: []ocommon.Point{
		{Slot: 100, Hash: []byte("shared")},
		{Slot: 200, Hash: []byte("a-fork")},
	}}
	b := CandidateFragment{points: []ocommon.Point{
		{Slot: 100, Hash: []byte("shared")},
		{Slot: 200, Hash: []byte("b-fork")},
	}}

	point, ok := a.Intersect(b)
	require.True(t, ok)
	assert.Equal(t, uint64(100), point.Slot)
	assert.Equal(t, "shared", string(point.Hash))
}

func TestCandidateFragment_IntersectNoCommonPointReturnsFalse(t *testing.T) {
	a := CandidateFragment{points: []ocommon.Point{
		{Slot: 100, Hash: []byte("a100")},
	}}
	b := CandidateFragment{points: []ocommon.Point{
		{Slot: 200, Hash: []byte("b200")},
	}}

	_, ok := a.Intersect(b)
	assert.False(t, ok)
}

// TestCandidateFragment_BoundedByGenesisWindow asserts Genesis retains one
// full density window plus the k-block eagerness suffix, while staying bounded.
func TestCandidateFragment_BoundedByGenesisWindow(t *testing.T) {
	const k = 5
	const window = 15
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         k,
		GenesisWindowSlots:    window,
		MinCorroboratingPeers: 1,
	})
	connId := newTestConnectionId(1)

	const delivered = 40
	for slot := uint64(1); slot <= delivered; slot++ {
		cs.UpdatePeerTip(connId, ochainsync.Tip{
			Point: ocommon.Point{
				Slot: slot,
				Hash: []byte(fmt.Sprintf("h%d", slot)),
			},
			BlockNumber: slot,
		}, nil)
	}

	fragment, ok := cs.GetCandidateFragment(connId)
	require.True(t, ok)
	assert.Equal(t, window+k+1, fragment.Len())
	assert.Equal(t, uint64(delivered-(window+k)), fragment.Anchor().Slot)
	assert.Equal(t, uint64(delivered), fragment.HeadPoint().Slot)
	assert.Equal(t, uint64(delivered-(window+k)), fragment.baseBlock)
}

func TestCandidateFragment_DerivedWindowKeepsEagernessSuffix(t *testing.T) {
	const k = 5
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         k,
		MinCorroboratingPeers: 1,
	})
	connId := newTestConnectionId(1)
	for block := uint64(1); block <= 40; block++ {
		cs.UpdatePeerTip(connId, ochainsync.Tip{
			Point: ocommon.Point{
				Slot: block,
				Hash: []byte(fmt.Sprintf("h%d", block)),
			},
			BlockNumber: block,
		}, nil)
	}

	fragment, ok := cs.GetCandidateFragment(connId)
	require.True(t, ok)
	assert.Equal(t, int(3*k+k+1), fragment.Len())
}

// TestCandidateFragment_RemovedOnDisconnect covers the acceptance criterion
// that a candidate fragment's lifetime is bound to its connection's lifetime.
func TestCandidateFragment_RemovedOnDisconnect(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{SecurityParam: 10})
	connId := newTestConnectionId(1)

	cs.UpdatePeerTip(connId, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("h1")},
		BlockNumber: 1,
	}, nil)

	fragment, ok := cs.GetCandidateFragment(connId)
	require.True(t, ok)
	require.Equal(t, 1, fragment.Len())
	require.Contains(t, cs.CandidateFragments(), connId)

	cs.RemovePeer(connId)

	_, ok = cs.GetCandidateFragment(connId)
	assert.False(t, ok, "fragment must be gone once its connection is removed")
	assert.NotContains(t, cs.CandidateFragments(), connId)
}

// TestCandidateFragment_ReconnectStartsEmpty ensures a fragment does not leak
// stale history across a disconnect/reconnect on a reused connection ID.
func TestCandidateFragment_ReconnectStartsEmpty(t *testing.T) {
	cs := NewChainSelector(ChainSelectorConfig{SecurityParam: 10})
	connId := newTestConnectionId(1)

	cs.UpdatePeerTip(connId, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte("h1")},
		BlockNumber: 1,
	}, nil)
	cs.RemovePeer(connId)

	cs.UpdatePeerTip(connId, ochainsync.Tip{
		Point:       ocommon.Point{Slot: 500, Hash: []byte("h2")},
		BlockNumber: 5,
	}, nil)

	fragment, ok := cs.GetCandidateFragment(connId)
	require.True(t, ok)
	require.Equal(t, 1, fragment.Len())
	assert.Equal(t, uint64(500), fragment.Anchor().Slot)
}
