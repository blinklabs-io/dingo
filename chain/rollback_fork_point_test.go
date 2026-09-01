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
	"errors"
	"slices"
	"testing"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"

	"github.com/blinklabs-io/dingo/chain"
)

// newForkChainFixture builds a persistent primary chain of primaryCount
// blocks and an ephemeral fork anchored at primary block index forkIdx+1,
// carrying forkCount blocks of its own.
func newForkChainFixture(
	t *testing.T,
	primaryCount, forkIdx, forkCount int,
) (primaryBlocks, forkBlocks []ledger.Block, forkChain *chain.Chain) {
	t.Helper()
	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	if err != nil {
		t.Fatalf("NewManager: %s", err)
	}
	mustSetLedger(t, cm, 5)
	primaryChain := cm.PrimaryChain()

	var origin common.Blake2b256
	// Start at slot 20, not 0: rollbackLocked treats slot 0 as origin, so a
	// fixture block there would never resolve to a block index.
	primaryBlocks = generateTestChain(t, 1, origin, 20, 20, primaryCount)
	for i, b := range primaryBlocks {
		if err := primaryChain.AddBlock(b, nil); err != nil {
			t.Fatalf("AddBlock primary[%d]: %s", i, err)
		}
	}

	forkPoint := ocommon.Point{
		Slot: primaryBlocks[forkIdx].SlotNumber(),
		Hash: primaryBlocks[forkIdx].Hash().Bytes(),
	}
	forkChain, err = cm.NewChainFromIntersect([]ocommon.Point{forkPoint})
	if err != nil {
		t.Fatalf("NewChainFromIntersect: %s", err)
	}

	forkBlocks = generateTestChain(
		t,
		uint64(forkIdx+2), //nolint:gosec
		primaryBlocks[forkIdx].Hash(),
		primaryBlocks[forkIdx].SlotNumber()+20,
		20,
		forkCount,
	)
	for i, b := range forkBlocks {
		if err := forkChain.AddBlock(b, nil); err != nil {
			t.Fatalf("AddBlock fork[%d]: %s", i, err)
		}
	}
	return primaryBlocks, forkBlocks, forkChain
}

// TestChainRollbackEphemeralAtAndBeforeForkPoint covers rolling an ephemeral
// fork chain back to a point inside its own blocks, to its fork point, and to
// a common-prefix block before its fork point. The before-fork case walks the
// deletion loop onto the fork point itself, where the in-memory buffer holds
// no entry for the block; computing a buffer index there yields -1.
func TestChainRollbackEphemeralAtAndBeforeForkPoint(t *testing.T) {
	const (
		primaryCount = 6
		forkIdx      = 2 // primary block index 3
		forkCount    = 3
	)

	for _, testDef := range []struct {
		name string
		// target picks the rollback point from the fixture's blocks.
		target func(primary, fork []ledger.Block) ledger.Block
		// want lists the blocks the chain must still deliver afterwards.
		want func(primary, fork []ledger.Block) []ledger.Block
	}{
		{
			name:   "within fork",
			target: func(_, fork []ledger.Block) ledger.Block { return fork[0] },
			want: func(primary, fork []ledger.Block) []ledger.Block {
				return append(slices.Clone(primary[:forkIdx+1]), fork[0])
			},
		},
		{
			name: "at fork point",
			target: func(primary, _ []ledger.Block) ledger.Block {
				return primary[forkIdx]
			},
			want: func(primary, _ []ledger.Block) []ledger.Block {
				return slices.Clone(primary[:forkIdx+1])
			},
		},
		{
			name: "one before fork point",
			target: func(primary, _ []ledger.Block) ledger.Block {
				return primary[forkIdx-1]
			},
			want: func(primary, _ []ledger.Block) []ledger.Block {
				return slices.Clone(primary[:forkIdx])
			},
		},
		{
			name: "two before fork point",
			target: func(primary, _ []ledger.Block) ledger.Block {
				return primary[forkIdx-2]
			},
			want: func(primary, _ []ledger.Block) []ledger.Block {
				return slices.Clone(primary[:forkIdx-1])
			},
		},
	} {
		t.Run(testDef.name, func(t *testing.T) {
			primaryBlocks, forkBlocks, forkChain := newForkChainFixture(
				t, primaryCount, forkIdx, forkCount,
			)
			target := testDef.target(primaryBlocks, forkBlocks)
			rollbackPoint := ocommon.Point{
				Slot: target.SlotNumber(),
				Hash: target.Hash().Bytes(),
			}
			if err := forkChain.Rollback(rollbackPoint); err != nil {
				t.Fatalf("Rollback: %s", err)
			}
			gotTip := forkChain.Tip()
			if gotTip.Point.Slot != target.SlotNumber() {
				t.Fatalf(
					"tip slot after rollback: got %d want %d",
					gotTip.Point.Slot, target.SlotNumber(),
				)
			}
			if gotTip.BlockNumber != target.BlockNumber() {
				t.Fatalf(
					"tip block number after rollback: got %d want %d",
					gotTip.BlockNumber, target.BlockNumber(),
				)
			}
			// The chain must remain usable: iterating from origin has to
			// deliver exactly the blocks up to the rollback target.
			iter, err := forkChain.FromPoint(ocommon.NewPointOrigin(), false)
			if err != nil {
				t.Fatalf("FromPoint: %s", err)
			}
			want := testDef.want(primaryBlocks, forkBlocks)
			for i, wantBlock := range want {
				next, err := iter.Next(false)
				if err != nil {
					t.Fatalf("iter.Next idx %d: %s", i, err)
				}
				if next == nil || next.Rollback {
					t.Fatalf("iter.Next idx %d unexpected: %+v", i, next)
				}
				if next.Block.Number != wantBlock.BlockNumber() {
					t.Fatalf(
						"iter idx %d block number: got %d want %d",
						i, next.Block.Number, wantBlock.BlockNumber(),
					)
				}
			}
			// Nothing may survive past the rollback target: a stale
			// in-memory buffer entry would surface as an extra block
			// here rather than as the chain tip.
			if _, err := iter.Next(false); !errors.Is(
				err, chain.ErrIteratorChainTip,
			) {
				t.Fatalf(
					"expected chain tip after %d blocks, got err %v",
					len(want), err,
				)
			}
		})
	}
}
