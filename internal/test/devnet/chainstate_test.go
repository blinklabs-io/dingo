//go:build linux

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

package devnet

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// hdr builds an observed header with a deterministic hash derived from
// the slot and a discriminator byte, so tests can construct competing
// chains at the same slot.
func hdr(slot, blockNum uint64, disc byte) ObservedHeader {
	return ObservedHeader{
		Slot:        slot,
		BlockNumber: blockNum,
		Hash:        []byte{disc, byte(slot), byte(slot >> 8)},
	}
}

func tipOf(h ObservedHeader) ChainTip {
	return ChainTip{
		SlotNumber:  h.Slot,
		BlockNumber: h.BlockNumber,
		Hash:        h.Hash,
	}
}

func TestObservedChainRollForwardTracksTip(t *testing.T) {
	c := NewObservedChain("dingo-1")

	require.Equal(t, uint64(0), c.Snapshot().Tip.SlotNumber,
		"a fresh chain has no tip")

	for i := uint64(1); i <= 3; i++ {
		h := hdr(i*10, i, 'a')
		c.RollForward(h, tipOf(h))
	}

	snap := c.Snapshot()
	require.Equal(t, uint64(30), snap.Tip.SlotNumber)
	require.Equal(t, uint64(3), snap.Tip.BlockNumber)
	require.Equal(t, 3, snap.RollForwards)
	require.Equal(t, 0, snap.RollBackwards)
	require.Len(t, snap.Headers, 3)
	require.Equal(t, uint64(30), snap.ServerTip.SlotNumber,
		"the peer-reported tip is recorded alongside our own")
}

func TestObservedChainRollBackwardTruncatesToPoint(t *testing.T) {
	c := NewObservedChain("dingo-1")
	for i := uint64(1); i <= 5; i++ {
		h := hdr(i*10, i, 'a')
		c.RollForward(h, tipOf(h))
	}

	// Roll back to slot 20, dropping the headers at slots 30, 40, 50.
	c.RollBackward(ChainPoint{Slot: 20, Hash: hdr(20, 2, 'a').Hash},
		ChainTip{SlotNumber: 50, BlockNumber: 5})

	snap := c.Snapshot()
	require.Equal(t, uint64(20), snap.Tip.SlotNumber,
		"tip follows the rollback point")
	require.Equal(t, uint64(2), snap.Tip.BlockNumber,
		"block number is recovered from the retained header")
	require.Len(t, snap.Headers, 2)
	require.Equal(t, 1, snap.RollBackwards)
	require.Equal(t, uint64(3), snap.MaxRollbackDepth,
		"three headers were dropped")

	// A subsequent roll forward extends from the rollback point.
	h := hdr(25, 3, 'b')
	c.RollForward(h, tipOf(h))
	snap = c.Snapshot()
	require.Equal(t, uint64(25), snap.Tip.SlotNumber)
	require.Len(t, snap.Headers, 3)
}

func TestObservedChainRollBackwardToOriginClearsChain(t *testing.T) {
	c := NewObservedChain("dingo-1")
	for i := uint64(1); i <= 3; i++ {
		h := hdr(i*10, i, 'a')
		c.RollForward(h, tipOf(h))
	}

	c.RollBackward(ChainPoint{}, ChainTip{})

	snap := c.Snapshot()
	require.Empty(t, snap.Headers, "rollback to origin clears the chain")
	require.Equal(t, uint64(0), snap.Tip.SlotNumber)
	require.Equal(t, uint64(0), snap.Tip.BlockNumber)
	require.Equal(t, uint64(3), snap.MaxRollbackDepth)
}

// A rollback to a slot the observer never saw a header for must still
// truncate everything above it rather than silently keeping headers that
// the peer has abandoned.
func TestObservedChainRollBackwardToUnknownSlotTruncates(t *testing.T) {
	c := NewObservedChain("dingo-1")
	for i := uint64(1); i <= 4; i++ {
		h := hdr(i*10, i, 'a')
		c.RollForward(h, tipOf(h))
	}

	c.RollBackward(ChainPoint{Slot: 25, Hash: []byte{'z'}}, ChainTip{})

	snap := c.Snapshot()
	require.Len(t, snap.Headers, 2,
		"headers above the rollback slot are dropped")
	require.Equal(t, uint64(25), snap.Tip.SlotNumber)
	require.Equal(t, uint64(2), snap.MaxRollbackDepth)
}

func TestObservedChainAwaitReturnsWhenAlreadySatisfied(t *testing.T) {
	c := NewObservedChain("dingo-1")
	h := hdr(100, 7, 'a')
	c.RollForward(h, tipOf(h))

	// An already-true condition must not block, even with a dead context
	// deadline: Await evaluates before it waits.
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	err := c.Await(ctx, "tip at slot 100", func(s ChainSnapshot) bool {
		return s.Tip.SlotNumber >= 100
	})
	require.NoError(t, err)
}

func TestObservedChainAwaitWakesOnEvent(t *testing.T) {
	c := NewObservedChain("dingo-1")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- c.Await(ctx, "block 5", func(s ChainSnapshot) bool {
			return s.Tip.BlockNumber >= 5
		})
	}()

	for i := uint64(1); i <= 5; i++ {
		h := hdr(i*10, i, 'a')
		c.RollForward(h, tipOf(h))
	}

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("Await did not wake on roll-forward events")
	}
}

func TestObservedChainAwaitRespectsContextDeadline(t *testing.T) {
	c := NewObservedChain("dingo-1")

	ctx, cancel := context.WithTimeout(
		context.Background(),
		50*time.Millisecond,
	)
	defer cancel()

	err := c.Await(ctx, "unreachable block 99", func(s ChainSnapshot) bool {
		return s.Tip.BlockNumber >= 99
	})
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Contains(t, err.Error(), "unreachable block 99",
		"the failure names the condition that timed out")
	require.Contains(t, err.Error(), "dingo-1",
		"the failure names the node being observed")
}

// Connection churn is part of the observed record: the scenario restarts a
// relay and interrupts a peer, and the assertions need to know that the
// observer actually saw the drop and the recovery.
func TestObservedChainRecordsConnectionChurn(t *testing.T) {
	c := NewObservedChain("dingo-relay")

	c.Connected()
	h := hdr(10, 1, 'a')
	c.RollForward(h, tipOf(h))
	c.Disconnected(context.Canceled)
	c.Connected()

	snap := c.Snapshot()
	require.Equal(t, 2, snap.Connects)
	require.Equal(t, 1, snap.Disconnects)
	require.True(t, snap.Connected)
	require.Equal(t, uint64(10), snap.Tip.SlotNumber,
		"a reconnect preserves the observed chain")
}

func TestObservedChainHeaderRetentionIsBounded(t *testing.T) {
	c := NewObservedChain("dingo-1")

	total := uint64(maxRetainedHeaders + 100)
	for i := uint64(1); i <= total; i++ {
		h := hdr(i, i, 'a')
		c.RollForward(h, tipOf(h))
	}

	snap := c.Snapshot()
	require.Len(t, snap.Headers, maxRetainedHeaders,
		"retained headers are capped so long runs stay bounded")
	require.Equal(t, total, snap.Tip.SlotNumber,
		"the tip still tracks the newest header")
	require.Equal(t, uint64(101), snap.Headers[0].Slot,
		"the oldest retained header is the cap window's start")
}

func TestChainAgreementAtDeepestCommonSlot(t *testing.T) {
	a := NewObservedChain("dingo-1")
	b := NewObservedChain("dingo-relay")

	// Both nodes observe the same chain; the relay lags by one header.
	for i := uint64(1); i <= 4; i++ {
		h := hdr(i*10, i, 'a')
		a.RollForward(h, tipOf(h))
		if i < 4 {
			b.RollForward(h, tipOf(h))
		}
	}

	result, ok := AgreementAtDeepestCommonSlot(
		[]ChainSnapshot{a.Snapshot(), b.Snapshot()},
	)
	require.True(t, ok, "a common slot exists")
	require.Equal(t, uint64(30), result.Slot,
		"agreement is checked at the deepest slot both nodes observed")
	require.True(t, result.Agree)
}

func TestChainAgreementDetectsHashMismatch(t *testing.T) {
	a := NewObservedChain("dingo-1")
	b := NewObservedChain("dingo-2")

	for i := uint64(1); i <= 3; i++ {
		ha := hdr(i*10, i, 'a')
		a.RollForward(ha, tipOf(ha))
		// dingo-2 is on a fork from slot 30 onwards.
		disc := byte('a')
		if i == 3 {
			disc = 'b'
		}
		hb := hdr(i*10, i, disc)
		b.RollForward(hb, tipOf(hb))
	}

	result, ok := AgreementAtDeepestCommonSlot(
		[]ChainSnapshot{a.Snapshot(), b.Snapshot()},
	)
	require.True(t, ok)
	require.Equal(t, uint64(30), result.Slot)
	require.False(t, result.Agree, "the fork at slot 30 is detected")
	require.Len(t, result.Hashes, 2,
		"both nodes' hashes are reported for diagnosis")
}

func TestChainAgreementWithoutCommonSlot(t *testing.T) {
	a := NewObservedChain("dingo-1")
	b := NewObservedChain("dingo-2")
	ha := hdr(10, 1, 'a')
	a.RollForward(ha, tipOf(ha))
	hb := hdr(11, 1, 'a')
	b.RollForward(hb, tipOf(hb))

	_, ok := AgreementAtDeepestCommonSlot(
		[]ChainSnapshot{a.Snapshot(), b.Snapshot()},
	)
	require.False(t, ok, "no slot was observed by both nodes")
}

func TestChainGroupAwaitAcrossNodes(t *testing.T) {
	g := NewChainGroup("dingo-1", "dingo-2", "dingo-relay")
	require.Len(t, g.Chains(), 3)
	require.NotNil(t, g.Chain("dingo-2"))
	require.Nil(t, g.Chain("nope"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- g.Await(ctx, "all nodes past slot 20",
			func(snaps []ChainSnapshot) bool {
				for _, s := range snaps {
					if s.Tip.SlotNumber < 20 {
						return false
					}
				}
				return true
			})
	}()

	// Feed the nodes one at a time; the group condition must only fire
	// once the last one crosses the threshold.
	for _, name := range []string{"dingo-1", "dingo-2", "dingo-relay"} {
		h := hdr(20, 2, 'a')
		c := g.Chain(name)
		require.NotNil(t, c)
		c.RollForward(h, tipOf(h))
	}

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("group Await did not observe all three nodes advancing")
	}
}

func TestChainGroupAwaitReportsLaggingNodes(t *testing.T) {
	g := NewChainGroup("dingo-1", "dingo-2")
	h := hdr(20, 2, 'a')
	lead := g.Chain("dingo-1")
	require.NotNil(t, lead)
	lead.RollForward(h, tipOf(h))

	ctx, cancel := context.WithTimeout(
		context.Background(),
		50*time.Millisecond,
	)
	defer cancel()
	err := g.Await(ctx, "all nodes past slot 20",
		func(snaps []ChainSnapshot) bool {
			for _, s := range snaps {
				if s.Tip.SlotNumber < 20 {
					return false
				}
			}
			return true
		})
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Contains(t, err.Error(), "dingo-2",
		"the timeout names the node state so failures are diagnosable")
}

// The observer writes from a ChainSync callback goroutine while scenario
// assertions read; -race must stay clean.
func TestObservedChainConcurrentApplyAndAwait(t *testing.T) {
	g := NewChainGroup("dingo-1", "dingo-2")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, name := range []string{"dingo-1", "dingo-2"} {
		wg.Go(func() {
			c := g.Chain(name)
			if c == nil {
				return
			}
			for i := uint64(1); i <= 200; i++ {
				h := hdr(i, i, 'a')
				c.RollForward(h, tipOf(h))
				if i%50 == 0 {
					c.RollBackward(
						ChainPoint{Slot: i - 5, Hash: hdr(i-5, i-5, 'a').Hash},
						tipOf(h),
					)
				}
			}
		})
	}

	readers := make(chan error, 4)
	for range 4 {
		go func() {
			readers <- g.Await(ctx, "both nodes reach block 100",
				func(snaps []ChainSnapshot) bool {
					for _, s := range snaps {
						if s.Tip.BlockNumber < 100 {
							return false
						}
					}
					return true
				})
		}()
	}

	wg.Wait()
	for range 4 {
		require.NoError(t, <-readers)
	}
}

func TestAgreedHeaderAboveFindsFirstCommonBlock(t *testing.T) {
	a := NewObservedChain("dingo-1")
	b := NewObservedChain("dingo-relay")

	// Both nodes observe slots 10..40 identically.
	for i := uint64(1); i <= 4; i++ {
		h := hdr(i*10, i, 'a')
		h.BodySize = i * 100
		a.RollForward(h, tipOf(h))
		b.RollForward(h, tipOf(h))
	}

	got, ok := AgreedHeaderAbove(
		[]ChainSnapshot{a.Snapshot(), b.Snapshot()}, 15,
	)
	require.True(t, ok)
	require.Equal(t, uint64(20), got.Slot,
		"the lowest agreed header above the baseline is returned")
	require.Equal(t, uint64(200), got.BodySize,
		"body size travels with the header so tx carriage is checkable")

	// Nothing above the newest slot has been observed yet.
	_, ok = AgreedHeaderAbove(
		[]ChainSnapshot{a.Snapshot(), b.Snapshot()}, 40,
	)
	require.False(t, ok)
}

func TestAgreedHeaderAboveIgnoresDisagreement(t *testing.T) {
	a := NewObservedChain("dingo-1")
	b := NewObservedChain("dingo-2")

	// Slot 20 differs; slot 30 agrees again.
	for _, spec := range []struct {
		slot uint64
		blk  uint64
		da   byte
		db   byte
	}{
		{10, 1, 'a', 'a'},
		{20, 2, 'a', 'b'},
		{30, 3, 'a', 'a'},
	} {
		ha := hdr(spec.slot, spec.blk, spec.da)
		hb := hdr(spec.slot, spec.blk, spec.db)
		a.RollForward(ha, tipOf(ha))
		b.RollForward(hb, tipOf(hb))
	}

	got, ok := AgreedHeaderAbove(
		[]ChainSnapshot{a.Snapshot(), b.Snapshot()}, 15,
	)
	require.True(t, ok)
	require.Equal(t, uint64(30), got.Slot,
		"a slot the nodes disagree on is not treated as propagated")
}

func TestAgreedHeaderAboveNeedsEveryNode(t *testing.T) {
	a := NewObservedChain("dingo-1")
	b := NewObservedChain("dingo-2")
	ha := hdr(20, 2, 'a')
	a.RollForward(ha, tipOf(ha))

	_, ok := AgreedHeaderAbove(
		[]ChainSnapshot{a.Snapshot(), b.Snapshot()}, 0,
	)
	require.False(t, ok, "a header only one node saw has not propagated")
}

func TestMaxBlockNumber(t *testing.T) {
	a := NewObservedChain("dingo-1")
	b := NewObservedChain("dingo-2")
	ha := hdr(40, 7, 'a')
	a.RollForward(ha, tipOf(ha))
	hb := hdr(25, 3, 'a')
	b.RollForward(hb, tipOf(hb))

	snaps := []ChainSnapshot{a.Snapshot(), b.Snapshot()}
	require.Equal(t, uint64(7), MaxBlockNumber(snaps))
	require.Equal(t, uint64(0), MaxBlockNumber(nil))
}

func TestMinAndMaxTipSlot(t *testing.T) {
	a := NewObservedChain("dingo-1")
	b := NewObservedChain("dingo-2")
	ha := hdr(40, 4, 'a')
	a.RollForward(ha, tipOf(ha))
	hb := hdr(25, 2, 'a')
	b.RollForward(hb, tipOf(hb))

	snaps := []ChainSnapshot{a.Snapshot(), b.Snapshot()}
	require.Equal(t, uint64(25), MinTipSlot(snaps))
	require.Equal(t, uint64(40), MaxTipSlot(snaps))
	require.Equal(t, uint64(0), MinTipSlot(nil))
}

// The observers intersect at origin and replay history before they reach
// the tip, so our own tip lags the node's real one during catch-up. A
// baseline taken from the observed tip would let an already-replayed
// header satisfy a "newly forged" assertion, and would let an
// already-passed epoch boundary satisfy the epoch phase. The peer's
// reported tip is the honest answer to "where is the chain now".
func TestMaxServerTipSlotIgnoresReplayLag(t *testing.T) {
	a := NewObservedChain("dingo-1")
	b := NewObservedChain("dingo-2")

	// Both observers are only at slot 10 but the nodes report slot 400.
	h := hdr(10, 1, 'a')
	a.RollForward(h, ChainTip{SlotNumber: 400, BlockNumber: 160})
	b.RollForward(h, ChainTip{SlotNumber: 395, BlockNumber: 158})

	snaps := []ChainSnapshot{a.Snapshot(), b.Snapshot()}
	require.Equal(t, uint64(10), MaxTipSlot(snaps),
		"the observed tip still reflects how far replay has got")
	require.Equal(t, uint64(400), MaxServerTipSlot(snaps),
		"the peer-reported tip reflects where the chain actually is")
	require.Equal(t, uint64(0), MaxServerTipSlot(nil))
}
