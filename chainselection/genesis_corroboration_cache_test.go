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

	ouroboros "github.com/blinklabs-io/gouroboros"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// confirmsRecentChainWithin is the uncached full comparison of the candidate's
// window against the witness, the reference the cache is checked against.
func (p *PeerChainTip) confirmsRecentChainWithin(
	witness *PeerChainTip,
	window uint64,
) bool {
	ok, _, _, _ := p.confirmsRecentChainFrom(witness, window, 0)
	return ok
}

// One appended header per peer must cost a constant number of witness-point
// comparisons per pair, not a rescan of the Genesis window.
func TestGenesisCorroborationExtendsPreviousResultOnAppend(t *testing.T) {
	t.Parallel()
	const peers = 3
	const warm = 3000 // slots, one block per slot
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		GenesisWindowSlots:    500,
		MinCorroboratingPeers: 1,
	})
	// ConnectionId holds address pointers, so each peer needs one value.
	var conns []ouroboros.ConnectionId
	for p := 1; p <= peers; p++ {
		conns = append(conns, corrConn(p))
	}
	feed := func(slot uint64) {
		for _, conn := range conns {
			cs.UpdatePeerTip(
				conn,
				genesisTip(slot, fmt.Sprintf("a%d", slot), slot),
				nil,
			)
		}
	}
	for slot := uint64(600); slot <= warm; slot++ {
		feed(slot)
	}
	for _, conn := range conns {
		require.Equal(t, peers-1, cs.corroboratingPeers(conn))
	}

	before := cs.corroborationCache.compared.Load()
	feed(warm + 1)
	for _, conn := range conns {
		require.Equal(t, peers-1, cs.corroboratingPeers(conn))
	}
	compared := cs.corroborationCache.compared.Load() - before
	assert.LessOrEqual(
		t,
		compared,
		uint64(peers*(peers-1)*2),
		"appending one point per peer must not rescan the window",
	)
}

// A cached confirmation must never outlive a change to either frontier that
// is not an append. Each step compares the cache against an uncached scan.
func TestCorroborationCacheMatchesFullScanAcrossFrontierChanges(t *testing.T) {
	t.Parallel()
	const window = 600
	rng := rand.New(rand.NewSource(1))
	cache := &corroborationCache{}
	candConn, witConn := corrConn(1), corrConn(2)
	cand, wit := &PeerChainTip{}, &PeerChainTip{}
	next := map[*PeerChainTip]uint64{cand: 0, wit: 0}
	chains := map[*PeerChainTip]string{cand: "a", wit: "a"}
	record := func(p *PeerChainTip, slot, block uint64) {
		p.recordObservedPoint(
			ocommon.Point{
				Slot: slot,
				Hash: []byte(fmt.Sprintf("%s%d", chains[p], slot)),
			},
			block,
			window,
			50,
			true,
		)
	}
	confirmed := 0
	for step := range 4000 {
		p := cand
		if rng.Intn(2) == 0 {
			p = wit
		}
		switch r := rng.Intn(40); {
		case r == 0:
			// Roll back to a retained point, then continue on a fork.
			if n := len(p.observedPoints); n > 2 {
				keep := p.observedPoints[rng.Intn(n-1)]
				p.ApplyRollback(keep, p.Tip)
				next[p] = keep.Slot
				// Either fork, or rejoin the shared chain.
				chains[p] = "a"
				if rng.Intn(2) == 0 {
					chains[p] = fmt.Sprintf("f%d", step)
				}
			}
		case r == 1:
			// Replace the tail with a different hash at the same height.
			if n := len(p.observedPoints); n > 0 {
				tail := p.observedPoints[n-1]
				prior := chains[p]
				chains[p] = fmt.Sprintf("r%d", step)
				record(p, tail.Slot, p.observedPointBaseBlock+uint64(n-1))
				chains[p] = prior
			}
		default:
			next[p]++
			record(p, next[p], next[p])
		}
		want := cand.confirmsRecentChainWithin(wit, window)
		got := cache.confirms(candConn, cand, witConn, wit, window)
		require.Equal(t, want, got, "step %d", step)
		if want {
			confirmed++
		}
	}
	// The comparison is only meaningful if both outcomes keep occurring.
	require.Greater(t, confirmed, 50)
	require.Less(t, confirmed, 3950)
}

// Replacing the candidate's tail changes a point the cache already verified,
// so a later append must not be confirmed from the stale prefix.
func TestCorroborationCacheInvalidatedByCandidateTailReplacement(t *testing.T) {
	t.Parallel()
	const window = 1000
	cache := &corroborationCache{}
	candConn, witConn := corrConn(1), corrConn(2)
	cand, wit := &PeerChainTip{}, &PeerChainTip{}
	record := func(p *PeerChainTip, slot uint64, hash string) {
		p.recordObservedPoint(
			ocommon.Point{Slot: slot, Hash: []byte(hash)},
			slot,
			window,
			window,
			true,
		)
	}
	for slot := uint64(1); slot <= 10; slot++ {
		record(cand, slot, fmt.Sprintf("a%d", slot))
	}
	for slot := uint64(1); slot <= 13; slot++ {
		record(wit, slot, fmt.Sprintf("a%d", slot))
	}
	require.True(t, cache.confirms(candConn, cand, witConn, wit, window))

	record(cand, 10, "forked10")
	require.False(t, cache.confirms(candConn, cand, witConn, wit, window))

	record(cand, 11, "a11")
	require.False(
		t,
		cand.confirmsRecentChainWithin(wit, window),
		"the replaced point still conflicts with the witness",
	)
	assert.False(
		t,
		cache.confirms(candConn, cand, witConn, wit, window),
		"a stale verified prefix must not hide the replaced candidate tail",
	)
}
