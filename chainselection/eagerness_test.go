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
	"sync/atomic"
	"testing"
	"time"

	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const loeTestK = 5

func loeTip(prefix string, block uint64) ochainsync.Tip {
	return ochainsync.Tip{
		Point: ocommon.Point{
			Slot: block * 100,
			Hash: []byte(fmt.Sprintf("%s%d", prefix, block)),
		},
		BlockNumber: block,
	}
}

// feedLoEChain delivers blocks from..to one at a time, as chainsync does.
func feedLoEChain(
	cs *ChainSelector,
	connId ouroboros.ConnectionId,
	prefix string,
	from, to uint64,
) {
	for block := from; block <= to; block++ {
		cs.UpdatePeerTip(connId, loeTip(prefix, block), nil)
	}
}

// newLoEScenario builds two candidates that agree up to block 10 and then
// fork: A runs to blockA, B to blockB. The local chain is at the fork point.
func newLoEScenario(
	genesis bool,
	blockA, blockB uint64,
) (*ChainSelector, ouroboros.ConnectionId, ouroboros.ConnectionId) {
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   genesis,
		SecurityParam: loeTestK,
	})
	a := newTestConnectionId(1)
	b := newTestConnectionId(2)
	feedLoEChain(cs, a, "c", 1, 10)
	feedLoEChain(cs, b, "c", 1, 10)
	feedLoEChain(cs, a, "a", 11, blockA)
	feedLoEChain(cs, b, "b", 11, blockB)
	// Set after the peers deliver: a local tip within the Genesis window of
	// the best peer would leave Genesis mode, which is the cap's off switch.
	cs.SetLocalTip(loeTip("c", 10))
	return cs, a, b
}

func TestLimitOnEagernessCapsSelectionAtKPastIntersection(t *testing.T) {
	t.Parallel()
	cs, a, _ := newLoEScenario(true, 20, 12)
	require.NotNil(t, cs.SelectBestChain())

	limit := cs.EagernessLimit()
	require.True(t, limit.Active)
	require.True(t, limit.Intersected)
	assert.Equal(t, uint64(1000), limit.Point.Slot)
	assert.Equal(t, uint64(10+loeTestK), limit.BlockNumber)

	tip, ok := cs.SelectedTip()
	require.True(t, ok)
	assert.Equal(t, uint64(10+loeTestK), tip.BlockNumber,
		"selection must stop k past the candidate intersection")
	assert.Equal(t, "a15", string(tip.Point.Hash))
	require.Equal(t, a, *cs.SelectBestChain())
}

func TestLimitOnEagernessDisabledSelectionAdvancesPastLimit(t *testing.T) {
	t.Parallel()
	cs, _, _ := newLoEScenario(false, 20, 12)
	require.NotNil(t, cs.SelectBestChain())

	assert.False(t, cs.EagernessLimit().Active)
	tip, ok := cs.SelectedTip()
	require.True(t, ok)
	assert.Equal(t, uint64(20), tip.BlockNumber,
		"with the cap off the same scenario advances past the limit")
}

func TestLimitOnEagernessSingleCandidateDoesNotWedge(t *testing.T) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: loeTestK,
	})
	a := newTestConnectionId(1)
	feedLoEChain(cs, a, "c", 1, 10)
	feedLoEChain(cs, a, "a", 11, 40)
	cs.SetLocalTip(loeTip("c", 10))
	require.NotNil(t, cs.SelectBestChain())

	limit := cs.EagernessLimit()
	require.True(t, limit.Active)
	assert.GreaterOrEqual(t, limit.BlockNumber, uint64(40))
	tip, ok := cs.SelectedTip()
	require.True(t, ok)
	assert.Equal(t, uint64(40), tip.BlockNumber)
}

func TestLimitOnEagernessWithoutCommonPointFallsBackToLocalTip(t *testing.T) {
	t.Parallel()
	// A runs far past the fork, so its retained fragment no longer holds the
	// fork point and shares no point with B.
	cs, _, _ := newLoEScenario(true, 40, 12)
	require.NotNil(t, cs.SelectBestChain())

	limit := cs.EagernessLimit()
	require.True(t, limit.Active)
	assert.False(t, limit.Intersected)
	assert.Equal(t, uint64(10+loeTestK), limit.BlockNumber)
	tip, ok := cs.SelectedTip()
	require.True(t, ok)
	assert.LessOrEqual(t, tip.BlockNumber, limit.BlockNumber)
}

func TestLimitOnEagernessCandidatesBeyondLimitKeepIncumbent(t *testing.T) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: loeTestK,
	})
	// The incumbent has the higher connection ID, so the connection-ID
	// tiebreak alone would displace it.
	a := newTestConnectionId(2)
	b := newTestConnectionId(1)
	feedLoEChain(cs, a, "c", 1, 10)
	feedLoEChain(cs, a, "a", 11, 16)
	cs.SetLocalTip(loeTip("c", 10))
	cs.EvaluateAndSwitch()
	require.Equal(t, a, *cs.GetBestPeer())

	feedLoEChain(cs, b, "c", 1, 10)
	feedLoEChain(cs, b, "b", 11, 19)
	cs.EvaluateAndSwitch()

	limit := cs.EagernessLimit()
	require.True(t, limit.Intersected)
	require.Equal(t, uint64(10+loeTestK), limit.BlockNumber)
	require.Equal(t, a, *cs.GetBestPeer(),
		"candidates equal under the limit must not displace the incumbent")
}

// Two candidates both past the limit still differ in Genesis density, and
// density, not the incumbent, decides between them.
func TestLimitOnEagernessKeepsGenesisDensityPrecedence(t *testing.T) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: loeTestK,
	})
	a := newTestConnectionId(1)
	b := newTestConnectionId(2)
	feedLoEChain(cs, a, "c", 1, 10)
	feedLoEChain(cs, a, "a", 11, 16)
	cs.SetLocalTip(loeTip("c", 5))
	cs.EvaluateAndSwitch()
	require.Equal(t, a, *cs.GetBestPeer())

	// B forks at block 10 like A but fills consecutive slots, so it is the
	// denser chain in the 3k-slot window; A has one block per 100 slots.
	feedLoEChain(cs, b, "c", 1, 10)
	for block := uint64(11); block <= 19; block++ {
		tip := loeTip("b", block)
		tip.Point.Slot = 1000 + block - 10
		cs.UpdatePeerTip(b, tip, nil)
	}
	cs.EvaluateAndSwitch()

	limit := cs.EagernessLimit()
	require.True(t, limit.Intersected)
	require.Equal(t, uint64(10+loeTestK), limit.BlockNumber)
	require.Equal(t, b, *cs.GetBestPeer(),
		"the denser candidate must win even when both reach the limit")
}

// The limit is a function of the candidate set, not of map iteration order.
// A forks at block 10, C at block 20, and B runs to 20 on the shared chain:
// with 2k+1 retention B holds both fork points, C holds neither of A's.
func TestLimitOnEagernessIndependentOfCandidateOrder(t *testing.T) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: loeTestK,
	})
	a := newTestConnectionId(1)
	b := newTestConnectionId(2)
	c := newTestConnectionId(3)
	feedLoEChain(cs, a, "c", 1, 10)
	feedLoEChain(cs, a, "a", 11, 20)
	feedLoEChain(cs, b, "c", 1, 20)
	feedLoEChain(cs, c, "c", 1, 20)
	feedLoEChain(cs, c, "x", 21, 30)
	cs.SetLocalTip(loeTip("c", 5))

	first := cs.EagernessLimit()
	for range 100 {
		require.Equal(t, first, cs.EagernessLimit())
	}
	assert.False(t, first.Intersected,
		"C retains no point A holds, so no point is common to every fragment")
	// A and B agreed up to block 10 while C was still short, and the limit
	// stays k past that last common point rather than k past the local tip.
	assert.Equal(t, uint64(10+loeTestK), first.BlockNumber)
}

// awaitAsync runs AwaitEagernessLimit on its own goroutine and reports its
// result on the returned channel.
func awaitAsync(
	ctx context.Context,
	cs *ChainSelector,
	connId ouroboros.ConnectionId,
	block uint64,
) <-chan error {
	return awaitAsyncApplied(ctx, cs, connId, block, nil)
}

func awaitAsyncApplied(
	ctx context.Context,
	cs *ChainSelector,
	connId ouroboros.ConnectionId,
	block uint64,
	applied func() ochainsync.Tip,
) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- cs.AwaitEagernessLimit(ctx, connId, block, applied)
	}()
	return done
}

func requireStillBlocked(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("header past the limit was admitted (err=%v)", err)
	case <-time.After(150 * time.Millisecond):
	}
}

func requireAdmitted(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("header within the limit was not admitted")
	}
}

// A and B share one chain, A far ahead. The intersection is B's head, so a
// header of A's more than k past it waits until B catches up.
func TestAwaitEagernessLimitHoldsHeaderPastLimitUntilLimitAdvances(
	t *testing.T,
) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: loeTestK,
	})
	a := newTestConnectionId(1)
	b := newTestConnectionId(2)
	feedLoEChain(cs, a, "c", 1, 20)
	feedLoEChain(cs, b, "c", 1, 12)
	cs.SetLocalTip(loeTip("c", 10))
	require.Equal(t, uint64(12+loeTestK), cs.EagernessLimit().BlockNumber)

	requireAdmitted(t, awaitAsync(t.Context(), cs, a, 12+loeTestK))

	done := awaitAsync(t.Context(), cs, a, 12+loeTestK+1)
	requireStillBlocked(t, done)

	feedLoEChain(cs, b, "c", 13, 13)
	requireAdmitted(t, done)
}

func TestAwaitEagernessLimitAdmitsLoneCandidate(t *testing.T) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: loeTestK,
	})
	a := newTestConnectionId(1)
	feedLoEChain(cs, a, "c", 1, 40)
	cs.SetLocalTip(loeTip("c", 10))
	requireAdmitted(t, awaitAsync(t.Context(), cs, a, 40))
}

// The requester must count as a candidate even when staleness would drop it,
// or a lone peer whose last header lies past localTip+k waits forever.
func TestAwaitEagernessLimitAdmitsLoneStaleRequester(t *testing.T) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:       true,
		SecurityParam:     loeTestK,
		StaleTipThreshold: time.Nanosecond,
	})
	a := newTestConnectionId(1)
	feedLoEChain(cs, a, "c", 1, 40)
	cs.SetLocalTip(loeTip("c", 10))
	requireAdmitted(t, awaitAsync(t.Context(), cs, a, 40))
}

func TestAwaitEagernessLimitAdmitsOutsideGenesisMode(t *testing.T) {
	t.Parallel()
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   false,
		SecurityParam: loeTestK,
	})
	a := newTestConnectionId(1)
	b := newTestConnectionId(2)
	feedLoEChain(cs, a, "c", 1, 40)
	feedLoEChain(cs, b, "c", 1, 12)
	cs.SetLocalTip(loeTip("c", 10))
	requireAdmitted(t, awaitAsync(t.Context(), cs, a, 40))
}

func TestAwaitEagernessLimitReturnsOnContextCancel(t *testing.T) {
	t.Parallel()
	cs, a, _ := newLoEScenario(true, 20, 12)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := awaitAsync(ctx, cs, a, 20)
	requireStillBlocked(t, done)
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("wait did not return after cancellation")
	}
}

// Once the candidates have shared a point, a later loss of that point from a
// window must not hand the limit to the applied ledger tip: that tip follows
// the selected peer's blocks, so the cap would move with the chain it bounds.
func TestAwaitEagernessLimitIgnoresAppliedTipOnceAnchored(t *testing.T) {
	t.Parallel()
	var applied atomic.Uint64
	applied.Store(10)
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: loeTestK,
	})
	appliedTip := func() ochainsync.Tip {
		return loeTip("c", applied.Load())
	}
	a := newTestConnectionId(1)
	b := newTestConnectionId(2)
	feedLoEChain(cs, a, "c", 1, 10)
	feedLoEChain(cs, b, "c", 1, 10)
	feedLoEChain(cs, a, "a", 11, 40)
	feedLoEChain(cs, b, "b", 11, 12)
	cs.SetLocalTip(loeTip("c", 10))
	require.False(t, cs.EagernessLimit().Intersected)
	require.Equal(t, uint64(10+loeTestK), cs.EagernessLimit().BlockNumber)

	applied.Store(40 - loeTestK)
	assert.False(t, cs.withinEagernessLimit(a, 40, appliedTip))
	assert.Equal(t, uint64(10+loeTestK), cs.EagernessLimit().BlockNumber)
}

// B is not paused, so its window slides past the fork point while A waits.
// The cap must stay where the candidates last agreed.
func TestAwaitEagernessLimitHoldsWhenCompetitorWindowDropsForkPoint(
	t *testing.T,
) {
	t.Parallel()
	cs, a, b := newLoEScenario(true, 16, 20)
	appliedTip := func() ochainsync.Tip { return loeTip("c", 15) }
	require.False(t, cs.withinEagernessLimit(a, 16, appliedTip))

	feedLoEChain(cs, b, "b", 21, 21)
	assert.False(t, cs.withinEagernessLimit(a, 16, appliedTip),
		"a competitor's window passing the fork point released the cap")
	assert.False(t, cs.withinEagernessLimit(a, 60, appliedTip))
	assert.True(t, cs.withinEagernessLimit(a, 15, appliedTip))
	feedLoEChain(cs, b, "b", 22, 40)
	assert.False(t, cs.withinEagernessLimit(a, 16, appliedTip))
}

func TestAwaitEagernessLimitReleasesWhenCompetitorRemoved(t *testing.T) {
	t.Parallel()
	cs, a, b := newLoEScenario(true, 16, 20)
	done := awaitAsync(t.Context(), cs, a, 16)
	requireStillBlocked(t, done)
	cs.RemovePeer(b)
	requireAdmitted(t, done)
}

type atomicClock struct{ nanos atomic.Int64 }

func (c *atomicClock) Now() time.Time { return time.Unix(0, c.nanos.Load()) }

func (c *atomicClock) Advance(d time.Duration) { c.nanos.Add(int64(d)) }

func pausedWaiters(cs *ChainSelector, connId ouroboros.ConnectionId) int {
	cs.mutex.RLock()
	defer cs.mutex.RUnlock()
	return cs.peerTips[connId].eagernessPaused
}

// Two forks both paused at the limit send no tips. The pause must not age
// either into staleness, or each drops out of the other's candidate set and
// both are released with no new evidence.
func TestAwaitEagernessLimitPauseDoesNotAgePeersStale(t *testing.T) {
	t.Parallel()
	clock := &atomicClock{}
	clock.nanos.Store(time.Now().UnixNano())
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:       true,
		SecurityParam:     loeTestK,
		StaleTipThreshold: time.Second,
	})
	installFakeClock2(cs, clock.Now)
	a := newTestConnectionId(1)
	b := newTestConnectionId(2)
	feedLoEChain(cs, a, "c", 1, 10)
	feedLoEChain(cs, b, "c", 1, 10)
	feedLoEChain(cs, a, "a", 11, 16)
	feedLoEChain(cs, b, "b", 11, 16)
	cs.SetLocalTip(loeTip("c", 10))
	require.Equal(t, uint64(10+loeTestK), cs.EagernessLimit().BlockNumber)

	doneA := awaitAsync(t.Context(), cs, a, 16)
	doneB := awaitAsync(t.Context(), cs, b, 16)
	require.Eventually(t, func() bool {
		return pausedWaiters(cs, a) == 1 && pausedWaiters(cs, b) == 1
	}, 10*time.Second, time.Millisecond)

	clock.Advance(10 * time.Second)
	cs.cleanupStalePeers()
	assert.Equal(t, 2, cs.PeerCount(),
		"a paused peer was removed as very stale")
	assert.False(t, cs.withinEagernessLimit(a, 16, nil))
	assert.False(t, cs.withinEagernessLimit(b, 16, nil))
	requireStillBlocked(t, doneA)
	requireStillBlocked(t, doneB)
}

func installFakeClock2(cs *ChainSelector, now func() time.Time) {
	cs.mutex.Lock()
	cs.nowFn = now
	cs.mutex.Unlock()
}

// A requester that the selection view excludes still counts for its own
// header, but its fragment is not one the live candidates share. When it is
// the only fragment, its head must not become the recorded anchor, or live
// candidates that later stop overlapping fall back to a point they never
// shared.
func TestEagernessAnchorIgnoresExcludedRequesterHead(t *testing.T) {
	t.Parallel()
	var liveForks atomic.Bool
	liveForks.Store(true)
	var requesterLive atomic.Bool
	requesterLive.Store(true)
	requester := newTestConnectionId(3)
	cs := NewChainSelector(ChainSelectorConfig{
		GenesisMode:   true,
		SecurityParam: loeTestK,
		ConnectionLive: func(connId ouroboros.ConnectionId) bool {
			if connId == requester {
				return requesterLive.Load()
			}
			return liveForks.Load()
		},
	})
	feedLoEChain(cs, requester, "s", 1, 30)
	requesterLive.Store(false)
	a := newTestConnectionId(1)
	b := newTestConnectionId(2)
	feedLoEChain(cs, a, "c", 1, 10)
	feedLoEChain(cs, b, "c", 1, 10)
	feedLoEChain(cs, a, "a", 11, 40)
	feedLoEChain(cs, b, "b", 11, 12)
	cs.SetLocalTip(loeTip("c", 10))
	before := cs.EagernessLimit()
	require.True(t, before.Active)
	require.False(t, before.Intersected)
	require.Equal(t, uint64(10+loeTestK), before.BlockNumber)

	liveForks.Store(false)
	require.True(t, cs.withinEagernessLimit(requester, 30, nil),
		"the excluded requester is its own candidate")
	liveForks.Store(true)

	after := cs.EagernessLimit()
	assert.Equal(t, before.BlockNumber, after.BlockNumber,
		"the excluded requester's head became the shared anchor")
	assert.Equal(t, before.Point, after.Point)
}
