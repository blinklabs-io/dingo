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
	"testing"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"github.com/blinklabs-io/dingo/chain"
)

// TestChainHoldsPoint verifies HoldsPoint is true only for blocks currently
// on the chain: not for unknown points, a wrong hash at a held slot, or a
// block that was rolled back but stays resolvable from the block cache.
func TestChainHoldsPoint(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("unexpected error creating chain manager: %s", err)
	}
	mustSetLedger(t, cm, 100)
	c := cm.PrimaryChain()
	for _, testBlock := range testBlocks {
		if err := c.AddBlock(testBlock, nil); err != nil {
			t.Fatalf("unexpected error adding block to chain: %s", err)
		}
	}
	last := testBlocks[len(testBlocks)-1]
	held := blockPoint(last)
	if !c.HoldsPoint(held) {
		t.Fatal("expected chain to hold its tip block")
	}
	if c.HoldsPoint(ocommon.Point{Slot: held.Slot, Hash: []byte{0xde, 0xad}}) {
		t.Fatal("expected wrong hash at a held slot to not be held")
	}
	if c.HoldsPoint(ocommon.Point{Slot: held.Slot + 1_000_000, Hash: held.Hash}) {
		t.Fatal("expected unknown point to not be held")
	}
	surviving := blockPoint(testBlocks[len(testBlocks)-2])
	if err := c.Rollback(surviving); err != nil {
		t.Fatalf("unexpected error rolling back chain: %s", err)
	}
	if c.HoldsPoint(held) {
		t.Fatal("expected rolled-back block to not be held")
	}
	if !c.HoldsPoint(surviving) {
		t.Fatal("expected surviving block to remain held")
	}
}
