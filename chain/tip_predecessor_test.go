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

package chain_test

import (
	"bytes"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/event"
)

// TestTipPredecessorReportsAlternativeBlockContext covers the context an
// equal-slot alternative is built on: the tip that would be competed with, and
// the tip's immediate predecessor as the alternative's parent. This is what
// ouroboros-consensus' mkCurrentBlockContext returns for its EQ case.
func TestTipPredecessorReportsAlternativeBlockContext(t *testing.T) {
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks[:3] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}

	parent, tip, ok := c.TipPredecessor()
	if !ok {
		t.Fatal("expected a resolvable tip predecessor")
	}
	wantParent := testBlocks[1]
	wantTip := testBlocks[2]
	if parent.Slot != wantParent.MockSlot ||
		!bytes.Equal(parent.Hash, wantParent.Hash().Bytes()) {
		t.Fatalf(
			"parent = %d.%x, want %d.%s",
			parent.Slot,
			parent.Hash,
			wantParent.MockSlot,
			wantParent.MockHash,
		)
	}
	if tip.BlockNumber != wantTip.MockBlockNumber ||
		!bytes.Equal(tip.Point.Hash, wantTip.Hash().Bytes()) {
		t.Fatalf(
			"tip = %d/%x, want %d/%s",
			tip.BlockNumber,
			tip.Point.Hash,
			wantTip.MockBlockNumber,
			wantTip.MockHash,
		)
	}
	// The whole point of the context: our block's parent slot is strictly
	// below the slot we would forge at, which is exactly what binding the
	// live tip as parent fails to give at an equal slot.
	if parent.Slot >= tip.Point.Slot {
		t.Fatalf(
			"parent slot %d must be below the contested slot %d",
			parent.Slot,
			tip.Point.Slot,
		)
	}
}

// TestTipPredecessorRefusesWithoutAResolvablePredecessor pins the fail-closed
// contract. A caller that treated !ok as "use the live tip" would sign a block
// whose parent slot equals its own.
func TestTipPredecessorRefusesWithoutAResolvablePredecessor(t *testing.T) {
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()

	if _, _, ok := c.TipPredecessor(); ok {
		t.Fatal("empty chain must not report a tip predecessor")
	}

	if err := c.AddBlock(testBlocks[0], nil); err != nil {
		t.Fatalf("unexpected error adding block to chain: %s", err)
	}
	if _, _, ok := c.TipPredecessor(); ok {
		t.Fatal(
			"first block on the chain must not report a tip predecessor",
		)
	}
}

// TestTipPredecessorRefusesAParentAtOrAboveTheContestedSlot pins the strict
// slot-ordering half of the contract. The alternative built on this context is
// signed against the parent it names, so a parent whose slot is not strictly
// below the contested tip's would produce a block that Praos ordering and
// ledger envelope validation both reject. The chain does not enforce
// increasing slots on add, so this state is reachable; the answer is "no
// context", never a context the signer cannot use.
func TestTipPredecessorRefusesAParentAtOrAboveTheContestedSlot(t *testing.T) {
	cm, err := chain.NewManager(nil, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks[:3] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	if _, _, ok := c.TipPredecessor(); !ok {
		t.Fatal("expected a resolvable tip predecessor before the same-slot tip")
	}
	// A tip that reuses its parent's slot. Its predecessor is resolvable and
	// the hash linkage is intact, so only the slot check can reject it.
	sameSlotTip := &MockBlock{
		MockBlockNumber: testBlocks[2].MockBlockNumber + 1,
		MockSlot:        testBlocks[2].MockSlot,
		MockHash:        testHashPrefix + "00fe",
		MockPrevHash:    testBlocks[2].MockHash,
	}
	if err := c.AddBlock(sameSlotTip, nil); err != nil {
		t.Fatalf("unexpected error adding same-slot block: %s", err)
	}
	if parent, tip, ok := c.TipPredecessor(); ok {
		t.Fatalf(
			"expected no context for a tip at its parent's slot, got parent %d.%x tip %d",
			parent.Slot,
			parent.Hash,
			tip.Point.Slot,
		)
	}
}

// TestAddLocalBlockDeferredAdoptsSiblingAfterRollback exercises the two chain
// operations an equal-slot alternative needs in sequence: roll the contested
// block off the tip, then adopt the locally forged sibling that shares its
// parent and block number. The sibling is not an extension of the tip the
// chain had when it was forged, which is precisely why AddLocalBlock's
// extend-only path cannot take it.
func TestAddLocalBlockDeferredAdoptsSiblingAfterRollback(t *testing.T) {
	eventBus := event.NewEventBus(nil, nil)
	cm, err := chain.NewManager(nil, eventBus)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 100)
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks[:3] {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	rival := testBlocks[2]
	parent, tip, ok := c.TipPredecessor()
	if !ok {
		t.Fatal("expected a resolvable tip predecessor")
	}

	// Our alternative: same slot and block number as the rival at the tip,
	// the rival's predecessor as parent.
	sibling := &MockBlock{
		MockBlockNumber: tip.BlockNumber,
		MockSlot:        tip.Point.Slot,
		MockHash:        testHashPrefix + "0aa1",
		MockPrevHash:    testBlocks[1].MockHash,
	}

	// Extending the live tip is refused, which is the blocker this pair of
	// operations exists to get past.
	if err := c.AddLocalBlock(sibling); err == nil {
		t.Fatal("expected the sibling to be refused as a tip extension")
	}

	if _, err := c.RollbackDeferred(parent); err != nil {
		t.Fatalf("unexpected error rolling back to the sibling parent: %s", err)
	}
	evt, err := c.AddLocalBlockDeferred(sibling)
	if err != nil {
		t.Fatalf("unexpected error adopting local sibling: %s", err)
	}
	if evt.Type == "" {
		t.Fatal("expected a chain.update event for the adopted sibling")
	}

	newTip := c.Tip()
	if !bytes.Equal(newTip.Point.Hash, sibling.Hash().Bytes()) {
		t.Fatalf(
			"tip = %x, want the adopted sibling %s",
			newTip.Point.Hash,
			sibling.MockHash,
		)
	}
	if newTip.BlockNumber != rival.MockBlockNumber {
		t.Fatalf(
			"adopted sibling block number = %d, want the rival's %d",
			newTip.BlockNumber,
			rival.MockBlockNumber,
		)
	}
	if newTip.Point.Slot != rival.MockSlot {
		t.Fatalf(
			"adopted sibling slot = %d, want the contested slot %d",
			newTip.Point.Slot,
			rival.MockSlot,
		)
	}

	// The context now describes the newly adopted tip, so a second contest
	// at the same slot would build on the same parent again.
	parentAfter, tipAfter, ok := c.TipPredecessor()
	if !ok {
		t.Fatal("expected a resolvable tip predecessor after adoption")
	}
	if !bytes.Equal(parentAfter.Hash, parent.Hash) {
		t.Fatalf(
			"parent after adoption = %x, want the unchanged fork point %x",
			parentAfter.Hash,
			parent.Hash,
		)
	}
	if !bytes.Equal(tipAfter.Point.Hash, sibling.Hash().Bytes()) {
		t.Fatalf(
			"tip after adoption = %x, want %s",
			tipAfter.Point.Hash,
			sibling.MockHash,
		)
	}
	// Both mutations were deferred, so nothing was published inline; the
	// caller drains them once its ledger mutex is released.
	c.PublishPendingChainUpdates()
}
