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

package forging

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type fixedSlotArbiter struct {
	yield  bool
	winner string
	// adopted is what AwaitWinner reports: whether the winner adopted a
	// block for the slot.
	adopted     bool
	panics      bool
	awaitPanics bool
	asked       []uint64
	awaited     []uint64
	reports     []slotReport
}

type slotReport struct {
	slot    uint64
	adopted bool
}

func (a *fixedSlotArbiter) YieldSlot(slot uint64) (bool, string) {
	a.asked = append(a.asked, slot)
	if a.panics {
		panic("arbiter fault")
	}
	return a.yield, a.winner
}

func (a *fixedSlotArbiter) AwaitWinner(
	_ context.Context,
	slot uint64,
	_ time.Time,
) bool {
	a.awaited = append(a.awaited, slot)
	if a.awaitPanics {
		panic("arbiter wait fault")
	}
	return a.adopted
}

func (a *fixedSlotArbiter) ReportSlot(slot uint64, adopted bool) {
	a.reports = append(a.reports, slotReport{slot, adopted})
}

func TestForgerYieldsALeaderSlotToTheNodesWinningPool(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	forger, builder, _ := newStaleTipTestForger(t, 200, 199, 199, &logs)
	arbiter := &fixedSlotArbiter{yield: true, winner: "pool-b", adopted: true}
	forger.slotArbiter = arbiter

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, []uint64{200}, arbiter.asked)
	require.Equal(t, []uint64{200}, arbiter.awaited)
	require.Zero(t, builder.calls, "a yielded slot must not be built")
	require.Equal(t, float64(1),
		testutil.ToFloat64(forger.metrics.forgeYieldedToOwnPool))
	require.Zero(t,
		testutil.ToFloat64(forger.metrics.forgeMissedLeaderSlots),
		"yielding to the node's own pool is not a missed slot")
	require.Contains(t, logs.String(), "pool-b")
	require.Equal(t, []slotReport{{200, false}}, arbiter.reports)
}

func TestForgerForgesWhenTheArbiterKeepsTheSlot(t *testing.T) {
	t.Parallel()
	for _, arbiter := range []*fixedSlotArbiter{
		{yield: false},
		{panics: true}, // a faulty arbiter fails open
		// The winner released the slot without adopting a block.
		{yield: true, winner: "pool-b", adopted: false},
		// A faulty wait fails open too.
		{yield: true, winner: "pool-b", adopted: true, awaitPanics: true},
	} {
		var logs bytes.Buffer
		forger, builder, _ := newStaleTipTestForger(t, 200, 199, 199, &logs)
		forger.slotArbiter = arbiter

		require.NoError(t, forger.checkAndForgeProduction(context.Background()))

		require.Equal(t, 1, builder.calls)
		require.Zero(t, testutil.ToFloat64(forger.metrics.forgeYieldedToOwnPool))
		require.Equal(t, []slotReport{{200, true}}, arbiter.reports,
			"an adopted block is reported once")
	}
}

func TestForgerAsksTheArbiterOnlyForLeaderSlots(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	forger, _, _ := newStaleTipTestForger(t, 200, 199, 199, &logs)
	forger.leaderChecker = forgeStaleTipTestNonLeader{}
	arbiter := &fixedSlotArbiter{yield: true}
	forger.slotArbiter = arbiter

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Empty(t, arbiter.asked)
	require.Equal(t, []slotReport{{200, false}}, arbiter.reports,
		"every forge check reports its outcome")
}

// pairSlotArbiter decides every slot for winner and makes the other pool
// wait for winner's report, as the node's arbiter does.
type pairSlotArbiter struct {
	winner string

	mu       sync.Mutex
	reported chan struct{}
	adopted  bool
}

func newPairSlotArbiter(winner string) *pairSlotArbiter {
	return &pairSlotArbiter{winner: winner, reported: make(chan struct{})}
}

type pairPoolView struct {
	a      *pairSlotArbiter
	poolID string
}

func (v pairPoolView) YieldSlot(uint64) (bool, string) {
	return v.poolID != v.a.winner, v.a.winner
}

func (v pairPoolView) AwaitWinner(
	ctx context.Context,
	_ uint64,
	until time.Time,
) bool {
	select {
	case <-v.a.reported:
	case <-ctx.Done():
		return false
	case <-time.After(time.Until(until)):
		return false
	}
	v.a.mu.Lock()
	defer v.a.mu.Unlock()
	return v.a.adopted
}

func (v pairPoolView) ReportSlot(_ uint64, adopted bool) {
	if v.poolID != v.a.winner {
		return
	}
	v.a.mu.Lock()
	defer v.a.mu.Unlock()
	select {
	case <-v.a.reported:
		return
	default:
	}
	v.a.adopted = adopted
	close(v.a.reported)
}

// runPoolPair runs the forge check of two pools of one node for slot 200, the
// loser started first so it is already waiting when the winner decides.
func runPoolPair(t *testing.T, winner, loser *BlockForger) {
	t.Helper()
	arbiter := newPairSlotArbiter("winner")
	winner.slotArbiter = pairPoolView{arbiter, "winner"}
	loser.slotArbiter = pairPoolView{arbiter, "loser"}
	done := make(chan error, 1)
	go func() {
		done <- loser.checkAndForgeProduction(context.Background())
	}()
	require.NoError(t, winner.checkAndForgeProduction(context.Background()))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the yielded pool did not finish its forge check")
	}
}

// The arbiter picks the winner from leader VRF alone, before the winner's
// own later refusals. A winner that then refuses -- here its applied tip
// trails its primary chain -- must not cost the slot: the pool that yielded
// to it forges.
func TestForgerForgesASlotItYieldedWhenTheWinnerRefuses(t *testing.T) {
	t.Parallel()
	var winnerLogs, loserLogs bytes.Buffer
	winner, winnerBuilder, _ := newStaleTipTestForger(
		t, 200, 100, 183, &winnerLogs,
	)
	loser, loserBuilder, _ := newStaleTipTestForger(
		t, 200, 199, 199, &loserLogs,
	)

	runPoolPair(t, winner, loser)

	require.Zero(t, winnerBuilder.calls, "the winner refuses the slot")
	require.Equal(t, 1, loserBuilder.calls,
		"the pool that yielded forges the slot the winner refused")
	require.Zero(t,
		testutil.ToFloat64(loser.metrics.forgeYieldedToOwnPool))
	require.Contains(t, loserLogs.String(), "adopted no block")
}

func TestForgerYieldsASlotTheWinnerAdopts(t *testing.T) {
	t.Parallel()
	var winnerLogs, loserLogs bytes.Buffer
	winner, winnerBuilder, _ := newStaleTipTestForger(
		t, 200, 199, 199, &winnerLogs,
	)
	loser, loserBuilder, _ := newStaleTipTestForger(
		t, 200, 199, 199, &loserLogs,
	)

	runPoolPair(t, winner, loser)

	require.Equal(t, 1, winnerBuilder.calls)
	require.Zero(t, loserBuilder.calls)
	require.Equal(t, float64(1),
		testutil.ToFloat64(loser.metrics.forgeYieldedToOwnPool))
}

func TestYieldWaitDeadlineStaysInsideTheSlot(t *testing.T) {
	t.Parallel()
	const margin = 250 * time.Millisecond

	t.Run("slot longer than the retry margin", func(t *testing.T) {
		t.Parallel()
		end := time.Now().Add(time.Second)
		f := &BlockForger{
			slotClock:                 &retryTestSlotClock{currentSlot: 7, slotEnd: end},
			forgeSelectionRetryMargin: margin,
		}
		require.Equal(t, end.Add(-margin), f.yieldWaitDeadline(7))
	})

	t.Run("slot shorter than the retry margin", func(t *testing.T) {
		t.Parallel()
		before := time.Now()
		end := before.Add(100 * time.Millisecond)
		f := &BlockForger{
			slotClock:                 &retryTestSlotClock{currentSlot: 7, slotEnd: end},
			forgeSelectionRetryMargin: margin,
		}
		deadline := f.yieldWaitDeadline(7)
		require.True(t, deadline.After(before),
			"a short slot still waits for the winner")
		require.True(t, deadline.Before(end),
			"the wait leaves part of the slot to forge in")
	})
}
