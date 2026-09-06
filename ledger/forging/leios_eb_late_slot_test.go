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
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestEBSelectionBudgetSkipsALateSlot rejects both earlier behaviours: an
// expired budget neither drops the bound (an unbounded re-validation of
// the whole mempool, the defect this change exists to remove) nor buys a
// fresh one. The slot is over, so there is nothing to produce for.
func TestEBSelectionBudgetSkipsALateSlot(t *testing.T) {
	now := time.Now()
	forger := &BlockForger{
		now:                     func() time.Time { return now },
		forgeEBSelectionReserve: 300 * time.Millisecond,
		slotClock: &ebTestSlotClock{
			currentSlot: 10,
			// The slot ended a second ago.
			slotEnd: now.Add(-time.Second),
		},
	}

	_, expired := forger.ebSelectionBudget(10)
	require.True(t, expired, "a slot that has closed gets no selection")
}

// TestEBSelectionBudgetBoundsWithoutASlotClock closes the other unbounded
// path. Without a clock there is no slot-derived deadline, but a full
// mempool re-validation is no more acceptable without a clock than with
// one, so the minimal fixed budget applies instead.
func TestEBSelectionBudgetBoundsWithoutASlotClock(t *testing.T) {
	now := time.Now()
	forger := &BlockForger{
		now:                     func() time.Time { return now },
		forgeEBSelectionReserve: 300 * time.Millisecond,
	}

	deadline, expired := forger.ebSelectionBudget(10)
	require.False(t, expired, "an unreadable clock is not an expired slot")
	require.Equal(t, now.Add(300*time.Millisecond), deadline)
}

// TestEBSelectionBudgetBoundsWhenForgingAheadOfTheClock covers the other
// direction of clock disagreement. NextSlotTime would describe an earlier
// slot's boundary, which says nothing about this one, so the fallback
// budget applies rather than an expiry the clock has not reached.
func TestEBSelectionBudgetBoundsWhenForgingAheadOfTheClock(t *testing.T) {
	now := time.Now()
	forger := &BlockForger{
		now:                     func() time.Time { return now },
		forgeEBSelectionReserve: 300 * time.Millisecond,
		slotClock: &ebTestSlotClock{
			currentSlot: 9,
			slotEnd:     now.Add(-time.Second),
		},
	}

	deadline, expired := forger.ebSelectionBudget(10)
	require.False(t, expired)
	require.Equal(t, now.Add(300*time.Millisecond), deadline)
}

// TestEBSelectionBudgetRejectsASlotBoundaryStraddle is the regression test
// for reading a moving clock twice. NextSlotTime derives the boundary from
// the clock's own current slot, so if the clock crosses into slot 11 while
// the boundary is being read, its answer is slot 11's end -- nearly a full
// extra slot of budget for a forge whose slot is already over. The reading
// is taken on both sides of NextSlotTime and the straddle is rejected.
func TestEBSelectionBudgetRejectsASlotBoundaryStraddle(t *testing.T) {
	now := time.Now()
	clock := &advancingEBTestSlotClock{
		slots: []uint64{10, 11},
		// The boundary the clock hands back belongs to slot 11.
		slotEnd: now.Add(time.Second),
	}
	forger := &BlockForger{
		now:                     func() time.Time { return now },
		forgeEBSelectionReserve: 300 * time.Millisecond,
		slotClock:               clock,
	}

	_, expired := forger.ebSelectionBudget(10)
	require.True(
		t,
		expired,
		"a boundary read across a slot change must not grant the next slot's budget",
	)
	require.Equal(t, 2, clock.slotReads, "the slot is read on both sides")
}

// advancingEBTestSlotClock returns a different current slot on each read,
// which is what a real clock does when the forge straddles a boundary.
type advancingEBTestSlotClock struct {
	slots     []uint64
	slotReads int
	slotEnd   time.Time
}

func (c *advancingEBTestSlotClock) CurrentSlot() (uint64, error) {
	slot := c.slots[min(c.slotReads, len(c.slots)-1)]
	c.slotReads++
	return slot, nil
}

func (c *advancingEBTestSlotClock) SlotsPerKESPeriod() uint64 { return 100 }

func (c *advancingEBTestSlotClock) ChainTipSlot() uint64 { return 9 }

func (c *advancingEBTestSlotClock) NextSlotTime() (time.Time, error) {
	return c.slotEnd, nil
}

func (c *advancingEBTestSlotClock) UpstreamTipSlot() uint64 { return 0 }

func (c *advancingEBTestSlotClock) UpstreamSyncStatus() (uint64, bool) {
	return 0, false
}

// TestEBSelectionBudgetShrinksReserveOnShortSlots covers fast-slot
// networks. With a 100ms slot a fixed 300ms reserve puts the deadline
// before the slot even began, which would leave selection with no budget
// at all -- or, before this, unbounded. The reserve never takes more than
// half of what is left.
func TestEBSelectionBudgetShrinksReserveOnShortSlots(t *testing.T) {
	now := time.Now()
	slotEnd := now.Add(100 * time.Millisecond)
	forger := &BlockForger{
		now:                     func() time.Time { return now },
		forgeEBSelectionReserve: 300 * time.Millisecond,
		slotClock: &ebTestSlotClock{
			currentSlot: 10,
			slotEnd:     slotEnd,
		},
	}

	deadline, expired := forger.ebSelectionBudget(10)
	require.False(t, expired)
	require.Equal(
		t,
		slotEnd.Add(-50*time.Millisecond),
		deadline,
		"the reserve is capped at half the remaining slot",
	)
	require.True(t, deadline.After(now), "selection must still get budget")
}

// TestEBSelectionBudgetUsesFullReserveOnNormalSlots is the negative case:
// on a one-second slot the configured reserve applies unchanged.
func TestEBSelectionBudgetUsesFullReserveOnNormalSlots(t *testing.T) {
	now := time.Now()
	slotEnd := now.Add(time.Second)
	forger := &BlockForger{
		now:                     func() time.Time { return now },
		forgeEBSelectionReserve: 300 * time.Millisecond,
		slotClock: &ebTestSlotClock{
			currentSlot: 10,
			slotEnd:     slotEnd,
		},
	}

	deadline, expired := forger.ebSelectionBudget(10)
	require.False(t, expired)
	require.Equal(t, slotEnd.Add(-300*time.Millisecond), deadline)
}

// TestCheckAndForgeProductionBoundsEBSelectionWithoutASlotClock closes the
// last unbounded path at the call site. When the slot clock cannot answer
// there is no slot-derived deadline, but a full mempool re-validation is
// no more acceptable without a clock than with one, so the minimal budget
// applies there too.
func TestCheckAndForgeProductionBoundsEBSelectionWithoutASlotClock(
	t *testing.T,
) {
	block := newForgerTestBlock(10, 2)
	leiosCaster := &forgerTestLeiosCaster{}
	validator := &sessionMockTxValidator{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     &forgerTestBuilder{block: block, cbor: block.cbor},
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock: &ebTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			// A slot clock that cannot report the next slot boundary.
			slotEnd: time.Time{},
		},
		LeiosProduceChecker:     &forgerTestLeiosChecker{allowed: true},
		LeiosEBBroadcaster:      leiosCaster,
		LeiosTxValidator:        validator,
		ForgeEBSelectionReserve: 300 * time.Millisecond,
		LeiosMempool: forgerTestMempoolProvider{
			txs: leiosCandidateTxs(t, 20),
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	fakeNow := time.Now()
	forger.now = func() time.Time { return fakeNow }
	validator.onValidate = func(int) {
		fakeNow = fakeNow.Add(200 * time.Millisecond)
	}

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.NotEmpty(t, leiosCaster.txBodies)
	require.Less(
		t,
		len(leiosCaster.txBodies),
		20,
		"a missing slot clock must not license an unbounded pass",
	)
}
