// Copyright 2025 Blink Labs Software
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chain_test

import (
	"errors"
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/chain"
)

// behindPeerChainLength is long enough that the intersect ladder reaches
// several sparse rungs past the dense band.
const behindPeerChainLength = 200

// behindPeerSecurityParam is the security parameter K used by these tests. It
// sits above the dense band (32 points) so the ladder's sparse rungs, not the
// dense band, decide where a lagging peer intersects — the same relationship
// every real network has (K = 108/432/2160 against a fixed 32-point band).
const behindPeerSecurityParam = 40

// newBehindPeerChain builds a persistent chain of behindPeerChainLength linked
// blocks with K = behindPeerSecurityParam and returns it with its headers.
func newBehindPeerChain(t *testing.T) (*chain.Chain, []*MockBlock) {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	mustSetLedger(t, cm, behindPeerSecurityParam)
	c := cm.PrimaryChain()
	headers := makeLinkedHeaders(behindPeerChainLength, 0, 1, "")
	for _, header := range headers {
		require.NoError(t, c.AddBlock(header, nil))
	}
	return c, headers
}

// peerIntersectAnswer models what a chainsync server holding every block up to
// peerTipSlot answers to our FindIntersect. Our points are descending, and the
// server replies with the first point it holds, so the answer is the shallowest
// ladder rung at or below the peer's tip.
func peerIntersectAnswer(
	t *testing.T,
	points []ocommon.Point,
	peerTipSlot uint64,
) ocommon.Point {
	t.Helper()
	for _, point := range points {
		if point.Slot <= peerTipSlot {
			return point
		}
	}
	t.Fatalf("peer holding slot %d matched none of our intersect points", peerTipSlot)
	return ocommon.Point{}
}

// makeLinkedHeaders spaces blocks 20 slots apart, so a point's depth below the
// tip in blocks is recoverable from its slot.
func behindPeerDepth(headers []*MockBlock, point ocommon.Point) uint64 {
	tipSlot := headers[len(headers)-1].MockSlot
	return (tipSlot - point.Slot) / 20
}

// A peer that is behind us by fewer than K blocks is inside the security
// parameter by every measure that matters, yet the intersect ladder resolves
// its best common point to the next power-of-two rung, which can sit past K.
// The rollback the peer then asks for is refused as a deeper-than-K fork even
// though the peer's chain is a strict prefix of ours and nothing diverged.
//
// The ladder must offer a rung at K itself so that any peer within K of our
// tip resolves to a point at most K back.
func TestIntersectPointsKeepPeerWithinSecurityParamCrossable(t *testing.T) {
	c, headers := newBehindPeerChain(t)

	// The peer is 35 blocks behind: comfortably inside K=40.
	const peerLag = 35
	peerTip := headers[len(headers)-1-peerLag]

	points := c.IntersectPoints(100)
	answer := peerIntersectAnswer(t, points, peerTip.MockSlot)
	depth := behindPeerDepth(headers, answer)

	require.LessOrEqualf(
		t,
		depth,
		uint64(behindPeerSecurityParam),
		"a peer only %d blocks behind must resolve to an intersect within "+
			"K=%d, got depth %d",
		peerLag,
		behindPeerSecurityParam,
		depth,
	)
	require.NoErrorf(
		t,
		c.ValidateRollback(answer),
		"rollback to the intersect a peer %d blocks behind resolves to "+
			"must be crossable with K=%d",
		peerLag,
		behindPeerSecurityParam,
	)
}

// A peer further behind than K still resolves to a rung past K, and the chain
// layer still refuses that rollback: from the chain's point of view the depth
// is real. Nothing about the peer is divergent, though — its chain is a strict
// prefix of ours — so this rejection must not be turned into an eviction. That
// classification belongs to the ledger's chainsync handler; this test pins the
// chain-layer half of the contract so the ladder change above is not mistaken
// for a full fix.
func TestIntersectPointsStillExceedKForPeerBehindBeyondK(t *testing.T) {
	c, headers := newBehindPeerChain(t)

	// The peer is 45 blocks behind, past K=40.
	const peerLag = 45
	peerTip := headers[len(headers)-1-peerLag]

	points := c.IntersectPoints(100)
	answer := peerIntersectAnswer(t, points, peerTip.MockSlot)

	require.Greater(
		t,
		behindPeerDepth(headers, answer),
		uint64(behindPeerSecurityParam),
	)
	require.True(
		t,
		errors.Is(
			c.ValidateRollback(answer),
			chain.ErrRollbackExceedsSecurityParam,
		),
		"expected the chain layer to refuse a rollback deeper than K",
	)
}

// The K rung is merged into a list the chainsync protocol requires to be
// ordered newest-first, and it must land in its sorted position for every
// relationship between K, the dense band and the chain length — including
// chains shorter than the next doubling rung, where the loop that emits the
// sparse rungs stops early.
func TestIntersectPointsStayDescendingWithSecurityRung(t *testing.T) {
	for _, tc := range []struct {
		name        string
		chainLength int
		securityK   int
	}{
		{name: "k inside dense band", chainLength: 200, securityK: 8},
		{name: "k at dense band edge", chainLength: 200, securityK: 32},
		{name: "k between rungs", chainLength: 200, securityK: 40},
		{name: "k on a rung", chainLength: 200, securityK: 64},
		{name: "k past chain", chainLength: 40, securityK: 100},
		{name: "chain shorter than next rung", chainLength: 45, securityK: 40},
		{name: "chain equal to k", chainLength: 41, securityK: 40},
		{name: "tiny chain", chainLength: 3, securityK: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDB(t)
			cm, err := chain.NewManager(db, nil)
			require.NoError(t, err)
			mustSetLedger(t, cm, tc.securityK)
			c := cm.PrimaryChain()
			headers := makeLinkedHeaders(tc.chainLength, 0, 1, "")
			for _, header := range headers {
				require.NoError(t, c.AddBlock(header, nil))
			}

			points := c.IntersectPoints(100)
			require.NotEmpty(t, points)
			for i := 0; i+1 < len(points); i++ {
				require.Greaterf(
					t,
					points[i].Slot,
					points[i+1].Slot,
					"intersect points must be strictly descending at %d",
					i,
				)
			}

			// Whenever the chain is longer than K, a peer exactly K
			// behind must find a point at most K deep, so the rollback
			// it asks for stays inside the security parameter.
			if tc.chainLength > tc.securityK+1 {
				peerTip := headers[len(headers)-1-tc.securityK]
				answer := peerIntersectAnswer(t, points, peerTip.MockSlot)
				require.LessOrEqual(
					t,
					behindPeerDepth(headers, answer),
					uint64(tc.securityK),
				)
				require.NoError(t, c.ValidateRollback(answer))
			}
		})
	}
}
