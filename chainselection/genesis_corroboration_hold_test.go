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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type corroborationWait struct {
	eligible bool
	err      error
}

func TestAwaitIngressCorroborationHoldsUntilWitnessDelivers(t *testing.T) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		MinCorroboratingPeers: 1,
	})
	fast := corrConn(1)
	witness := corrConn(2)
	feedFrontier(cs, fast,
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
	)

	done := make(chan corroborationWait, 1)
	go func() {
		eligible, err := cs.AwaitIngressCorroboration(t.Context(), fast)
		done <- corroborationWait{eligible: eligible, err: err}
	}()
	testutil.RequireNoReceive(t, done, 200*time.Millisecond,
		"an uncorroborated header must be held, not withheld")

	feedFrontier(cs, witness,
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
	)
	result := testutil.RequireReceive(t, done, 5*time.Second,
		"the header must be released once a witness delivers it")
	require.NoError(t, result.err)
	require.True(t, result.eligible)
}

func TestDeliveredHistoryCorroboratesAfterAdmissionFrontierPruning(
	t *testing.T,
) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		MinCorroboratingPeers: 1,
	})
	applicant := corrConn(3)
	witness := corrConn(4)
	feedFrontier(cs, applicant,
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
		genesisTip(110, "h110", 110),
	)

	updates := []PeerTipUpdateEvent{
		{
			ConnectionId: witness,
			AdmissionID:  1,
			Tip:          genesisTip(100, "h100", 100),
			ObservedTip:  genesisTip(100, "h100", 100),
		},
		{
			ConnectionId: witness,
			AdmissionID:  2,
			Tip:          genesisTip(105, "h105", 105),
			ObservedTip:  genesisTip(105, "h105", 105),
		},
		{
			ConnectionId: witness,
			AdmissionID:  3,
			Tip:          genesisTip(110, "h110", 110),
			ObservedTip:  genesisTip(110, "h110", 110),
		},
	}
	for _, update := range updates {
		require.Equal(t, PeerTipUpdateAccepted,
			cs.PreparePeerTipAdmissionDisposition(update))
		cs.RecordDeliveredPeerTip(update)
	}
	cs.mutex.Lock()
	cs.truncatePendingPeerTipsLocked(witness, 0)
	cs.mutex.Unlock()
	require.Empty(t, cs.pendingPeerTips[witness])

	require.True(
		t,
		cs.ShouldApplyIngress(applicant),
		"pruning the admission frontier must not erase the witness's delivered history",
	)
}

func TestDeliveredHistoryFromObserverCorroboratesAfterPromotion(t *testing.T) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		MinCorroboratingPeers: 1,
	})
	applicant := corrConn(8)
	observer := corrConn(9)
	feedFrontier(cs, applicant,
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
		genesisTip(110, "h110", 110),
	)
	cs.SetConnectionEligible(observer, false)

	for _, tip := range []ochainsync.Tip{
		genesisTip(100, "h100", 100),
		genesisTip(105, "h105", 105),
		genesisTip(110, "h110", 110),
	} {
		cs.RecordDeliveredPeerTip(PeerTipUpdateEvent{
			ConnectionId: observer,
			Tip:          tip,
			ObservedTip:  tip,
		})
	}

	require.False(t, cs.ShouldApplyIngress(applicant),
		"an observer cannot corroborate while it is ineligible")
	cs.SetConnectionEligible(observer, true)
	require.True(
		t,
		cs.ShouldApplyIngress(applicant),
		"the promoted observer's delivered history should corroborate the held ingress frontier",
	)
}

func TestDeliveredPeerHistoryIsBoundedAndRemoved(t *testing.T) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		MinCorroboratingPeers: 1,
		MaxTrackedPeers:       1,
	})
	first := corrConn(6)
	for block := uint64(1); block <= 100; block++ {
		tip := genesisTip(block, fmt.Sprintf("h%d", block), block)
		cs.RecordDeliveredPeerTip(PeerTipUpdateEvent{
			ConnectionId: first,
			Tip:          tip,
			ObservedTip:  tip,
		})
	}
	history := cs.deliveredPeerTips[first]
	require.NotNil(t, history)
	assert.LessOrEqual(t, uint64(len(history.observedPoints)),
		cs.candidatePointCapacityLocked(true))
	assert.LessOrEqual(t, len(history.observedTipHistory), 21)
	assert.Len(t, cs.deliveredPeerTips, 1)

	second := corrConn(7)
	tip := genesisTip(100, "h100", 100)
	cs.RecordDeliveredPeerTip(PeerTipUpdateEvent{
		ConnectionId: second,
		Tip:          tip,
		ObservedTip:  tip,
	})
	assert.NotContains(t, cs.deliveredPeerTips, second)

	cs.RemovePeer(first)
	cs.RecordDeliveredPeerTip(PeerTipUpdateEvent{
		ConnectionId: second,
		Tip:          tip,
		ObservedTip:  tip,
	})
	assert.NotContains(t, cs.deliveredPeerTips, first)
	assert.Contains(t, cs.deliveredPeerTips, second)
}

func TestGenesisIngressAllowsPointsAlreadyOnAppliedChain(t *testing.T) {
	t.Parallel()
	localTip := genesisTip(200, "local-tip", 200)
	for _, tc := range []struct {
		name         string
		point        ochainsync.Tip
		pointOnChain bool
		want         bool
	}{
		{
			name:         "point already applied",
			point:        genesisTip(190, "local-point", 190),
			pointOnChain: true,
			want:         true,
		},
		{
			name:         "same height on another chain",
			point:        genesisTip(190, "fork-point", 190),
			pointOnChain: false,
			want:         false,
		},
		{
			name:         "point above applied tip",
			point:        genesisTip(210, "future-point", 210),
			pointOnChain: true,
			want:         false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cs := NewChainSelector(ChainSelectorConfig{
				GenesisMode:           true,
				SecurityParam:         20,
				MinCorroboratingPeers: 1,
				AppliedTip: func() (ochainsync.Tip, bool) {
					return localTip, true
				},
				PointOnLocalChain: func(point ocommon.Point) bool {
					return tc.pointOnChain &&
						point.Slot == tc.point.Point.Slot &&
						string(point.Hash) == string(tc.point.Point.Hash)
				},
			})
			peer := corrConn(5)
			feedFrontier(cs, peer, tc.point)
			require.Equal(t, tc.want, cs.ShouldApplyIngress(peer))
		})
	}
}

// A denial that corroboration cannot lift is reported at once, so the caller
// keeps withholding rather than holding a peer that can never be released.
func TestAwaitIngressCorroborationReturnsAtOnceForOtherDenials(t *testing.T) {
	t.Parallel()
	t.Run("untracked peer", func(t *testing.T) {
		t.Parallel()
		cs := NewChainSelector(ChainSelectorConfig{
			GenesisMode:           true,
			SecurityParam:         20,
			MinCorroboratingPeers: 1,
		})
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		eligible, err := cs.AwaitIngressCorroboration(ctx, corrConn(1))
		require.NoError(t, err)
		require.False(t, eligible)
	})
	t.Run("wall-clock stale peer", func(t *testing.T) {
		t.Parallel()
		cs, dead, _ := newDeadForkSelector(t, genesisTip(1000, "local", 100))
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		eligible, err := cs.AwaitIngressCorroboration(ctx, dead)
		require.NoError(t, err)
		require.False(t, eligible)
	})
}

func TestAwaitIngressCorroborationEndsWithContext(t *testing.T) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		MinCorroboratingPeers: 1,
	})
	fast := corrConn(1)
	feedFrontier(cs, fast, genesisTip(100, "h100", 100))
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan corroborationWait, 1)
	go func() {
		eligible, err := cs.AwaitIngressCorroboration(ctx, fast)
		done <- corroborationWait{eligible: eligible, err: err}
	}()
	testutil.RequireNoReceive(t, done, 100*time.Millisecond,
		"an uncorroborated header must be held")
	cancel()
	result := testutil.RequireReceive(t, done, 5*time.Second,
		"a cancelled wait must return")
	require.ErrorIs(t, result.err, context.Canceled)
	require.False(t, result.eligible)
}

// The selector learns the applied tip from SetLocalTip only at startup and on
// the stall recycler's tick, while the ledger can apply hundreds of blocks in
// between, as it does replaying blocks already stored after a Mithril import.
// Bounding a delivered frontier by a local tip that old rejects headers the
// ledger has already passed. With an applied-tip source wired, the catch-up
// ceiling is k*2 past the applied tip, not past the tip last pushed in.
func TestPeerTipAdmissionBoundsCatchUpByAppliedTip(t *testing.T) {
	t.Parallel()
	const k = 10
	run := func(t *testing.T, applied uint64) PeerTipUpdateDisposition {
		cs := NewChainSelector(ChainSelectorConfig{
			GenesisMode:   true,
			SecurityParam: k,
			AppliedTip: func() (ochainsync.Tip, bool) {
				return genesisTip(applied*20, "applied", applied), true
			},
		})
		stale := corrConn(1)
		feedFrontier(cs, stale, genesisTip(1800, "h90", 90))
		cs.SetLocalTip(genesisTip(2000, "local", 100))
		fresh := corrConn(2)
		return cs.PreparePeerTipAdmissionDisposition(PeerTipUpdateEvent{
			ConnectionId: fresh,
			AdmissionID:  1,
			Tip:          genesisTip(2500, "h125", 125),
			ObservedTip:  genesisTip(2500, "h125", 125),
		})
	}
	// Control: the applied tip has not moved, so 125 is past 100+2k and is
	// held back as an unproven far claim.
	require.NotEqual(t, PeerTipUpdateAccepted, run(t, 100))
	require.Equal(
		t,
		PeerTipUpdateAccepted,
		run(t, 150),
		"a frontier within 2k of the applied tip was bounded by a stale local tip",
	)
}

func TestPeerClaimsCannotAdvanceAppliedCatchUpCeiling(t *testing.T) {
	t.Parallel()
	const (
		k          = uint64(2)
		appliedTip = uint64(100)
	)
	var applied atomic.Uint64
	applied.Store(appliedTip)
	cs := NewChainSelector(ChainSelectorConfig{
		SecurityParam: k,
		AppliedTip: func() (ochainsync.Tip, bool) {
			block := applied.Load()
			return genesisTip(block*20, fmt.Sprintf("h%d", block), block), true
		},
	})
	cs.SetLocalTip(genesisTip(appliedTip*20, "local", appliedTip))

	// Establish the highest admissible frontier at the applied-tip ceiling.
	reference := genesisTip(104*20, "h104", 104)
	require.True(t, cs.UpdatePeerTip(corrConn(1), reference, nil))

	prepare := func(conn int, admissionID, block uint64) PeerTipUpdateDisposition {
		tip := genesisTip(block*20, fmt.Sprintf("h%d", block), block)
		return cs.PreparePeerTipAdmissionDisposition(PeerTipUpdateEvent{
			ConnectionId: corrConn(conn),
			AdmissionID:  admissionID,
			Tip:          tip,
			ObservedTip:  tip,
		})
	}

	require.NotEqual(t, PeerTipUpdateAccepted, prepare(2, 1, 108))
	require.NotEqual(t, PeerTipUpdateAccepted, prepare(3, 1, 107),
		"matching peer claims advanced the applied catch-up ceiling")

	// Verified ledger progress moves the same frontier inside the ceiling.
	applied.Store(104)
	require.Equal(t, PeerTipUpdateAccepted, prepare(3, 1, 107))
}

// A chain's first block can sit at slot 0, as Preview's does. It carries a
// hash, so it is a delivered point like any other and not the origin; a
// frontier that forgets it gives a peer holding that header nothing a witness
// could match, and a from-genesis sync never applies its first block.
func TestGenesisCorroboratesBlockAtSlotZero(t *testing.T) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:           true,
		SecurityParam:         20,
		MinCorroboratingPeers: 1,
	})
	first := genesisTip(0, "b0", 0)
	a, b := corrConn(1), corrConn(2)
	feedFrontier(cs, a, first)
	require.False(t, cs.ShouldApplyIngress(a))
	feedFrontier(cs, b, first)
	require.True(t, cs.ShouldApplyIngress(b),
		"a block at slot 0 delivered by two peers was not corroborated")
	require.True(t, cs.ShouldApplyIngress(a))
}

// A rollback to the block at slot 0 keeps that block in the frontier; only a
// rollback to the origin, which has no hash, empties it.
func TestPeerRollbackToSlotZeroBlockKeepsIt(t *testing.T) {
	t.Parallel()
	pt := &PeerChainTip{}
	for i, point := range []ocommon.Point{
		{Slot: 0, Hash: []byte("b0")},
		{Slot: 20, Hash: []byte("b20")},
	} {
		pt.recordObservedPoint(point, uint64(i), 100, 100, true)
	}
	pt.ApplyRollback(
		ocommon.Point{Slot: 0, Hash: []byte("b0")},
		ochainsync.Tip{},
	)
	require.Equal(t,
		[]ocommon.Point{{Slot: 0, Hash: []byte("b0")}},
		pt.observedPoints,
	)
	pt.ApplyRollback(ocommon.Point{}, ochainsync.Tip{})
	require.Empty(t, pt.observedPoints)
}

func TestGenesisIngressReleasesGenerationPinBeforeAppliedLookup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                     string
		genesis, tracked, lookup bool
	}{
		{"applied point", true, true, true},
		{"untracked denial", true, false, false},
		{"Praos admission", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var pin sync.RWMutex
			releases, lookups := 0, 0
			cs := NewChainSelector(
				ChainSelectorConfig{
					GenesisMode:           tc.genesis,
					SecurityParam:         20,
					MinCorroboratingPeers: 1,
				},
			)
			peer := corrConn(8)
			point := genesisTip(190, "applied-point", 190)
			if tc.tracked {
				feedFrontier(cs, peer, point)
			}
			cs.config.CurrentHeaderCryptoVerificationGeneration = func() uint64 { return 1 }
			cs.config.PinHeaderCryptoVerificationGeneration = func(uint64) (func(), bool) {
				pin.RLock()
				return func() { releases++; pin.RUnlock() }, true
			}
			cs.config.AppliedTip = func() (ochainsync.Tip, bool) { return genesisTip(200, "tip", 200), true }
			cs.config.PointOnLocalChain = func(got ocommon.Point) bool {
				lookups++
				unlocked := pin.TryLock()
				if unlocked {
					pin.Unlock()
				}
				require.True(
					t,
					unlocked,
					"applied-chain I/O must not hold the generation pin",
				)
				require.Equal(t, point.Point, got)
				return true
			}
			require.Equal(t, tc.tracked, cs.ShouldApplyIngress(peer))
			require.Equal(
				t,
				1,
				releases,
				"every pinned verdict must release exactly once",
			)
			if tc.lookup {
				require.Equal(t, 1, lookups)
			} else {
				require.Zero(t, lookups)
			}
			unlocked := pin.TryLock()
			if unlocked {
				pin.Unlock()
			}
			require.True(t, unlocked, "early verdicts must release the pin too")
		})
	}
}
