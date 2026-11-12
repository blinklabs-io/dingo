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
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/chain"
)

func TestSlotFollowsParent(t *testing.T) {
	t.Parallel()

	const (
		byronEra   = byron.EraIdByron
		conwayEra  = conway.EraIdConway
		parentSlot = 100
	)
	cases := []struct {
		name     string
		eraId    uint8
		boundary bool
		slot     uint64
		parent   chain.SlotParent
		want     bool
	}{
		{
			name:  "later slot",
			eraId: conwayEra,
			slot:  parentSlot + 1,
			parent: chain.SlotParent{
				Slot:  parentSlot,
				EraId: conwayEra,
				Known: true,
			},
			want: true,
		},
		{
			name:  "same slot",
			eraId: conwayEra,
			slot:  parentSlot,
			parent: chain.SlotParent{
				Slot:  parentSlot,
				EraId: conwayEra,
				Known: true,
			},
		},
		{
			name:  "earlier slot",
			eraId: conwayEra,
			slot:  parentSlot - 1,
			parent: chain.SlotParent{
				Slot:  parentSlot,
				EraId: conwayEra,
				Known: true,
			},
		},
		{
			name:   "same slot, unknown parent",
			eraId:  conwayEra,
			slot:   parentSlot,
			parent: chain.SlotParent{Slot: parentSlot},
		},
		{
			name:   "earlier slot, unknown parent",
			eraId:  conwayEra,
			slot:   parentSlot - 1,
			parent: chain.SlotParent{Slot: parentSlot},
		},
		{
			// The unknown parent may be an epoch boundary block.
			name:   "Byron main block at an unknown parent's slot",
			eraId:  byronEra,
			slot:   parentSlot,
			parent: chain.SlotParent{Slot: parentSlot},
			want:   true,
		},
		{
			name:     "Byron boundary block at an unknown parent's slot",
			eraId:    byronEra,
			boundary: true,
			slot:     parentSlot,
			parent:   chain.SlotParent{Slot: parentSlot},
		},
		{
			name:  "Byron main block at its boundary parent's slot",
			eraId: byronEra,
			slot:  parentSlot,
			parent: chain.SlotParent{
				Slot: parentSlot, EraId: byronEra, Boundary: true, Known: true,
			},
			want: true,
		},
		{
			name:  "Byron main block before its boundary parent's slot",
			eraId: byronEra,
			slot:  parentSlot - 1,
			parent: chain.SlotParent{
				Slot: parentSlot, EraId: byronEra, Boundary: true, Known: true,
			},
		},
		{
			name:     "Byron boundary block at a boundary parent's slot",
			eraId:    byronEra,
			boundary: true,
			slot:     parentSlot,
			parent: chain.SlotParent{
				Slot: parentSlot, EraId: byronEra, Boundary: true, Known: true,
			},
		},
		{
			name:  "Byron main block at a main parent's slot",
			eraId: byronEra,
			slot:  parentSlot,
			parent: chain.SlotParent{
				Slot:  parentSlot,
				EraId: byronEra,
				Known: true,
			},
		},
		{
			name:     "Byron boundary block at a main parent's slot",
			eraId:    byronEra,
			boundary: true,
			slot:     parentSlot,
			parent: chain.SlotParent{
				Slot:  parentSlot,
				EraId: byronEra,
				Known: true,
			},
		},
		{
			// A Byron slot number uses a fixed epoch length, so on a network
			// with shorter Byron epochs it can exceed the absolute slot of
			// the first block of the next era.
			name:  "later era after a Byron parent with a larger slot number",
			eraId: conwayEra,
			slot:  parentSlot - 1,
			parent: chain.SlotParent{
				Slot:  parentSlot,
				EraId: byronEra,
				Known: true,
			},
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(
				t,
				tc.want,
				chain.SlotFollowsParent(
					tc.eraId,
					tc.boundary,
					tc.slot,
					tc.parent,
				),
			)
		})
	}
}

func sameSlotSuccessor(parent *MockBlock, hashSuffix string) *MockBlock {
	return &MockBlock{
		MockBlockNumber: parent.MockBlockNumber + 1,
		MockSlot:        parent.MockSlot,
		MockHash:        testHashPrefix + hashSuffix,
		MockPrevHash:    parent.MockHash,
	}
}

// TestAddBlockHeaderRejectsSlotNotAfterParent covers a header that names its
// parent and the next block number but does not advance the slot, against
// both parents a header can be admitted onto. One leader slot otherwise signs
// a header chain of any length.
func TestAddBlockHeaderRejectsSlotNotAfterParent(t *testing.T) {
	t.Parallel()

	tip := testBlocks[1]
	queued := testBlocks[2]
	cases := []struct {
		name        string
		queueHeader bool
		header      *MockBlock
	}{
		{
			name:   "same slot as the tip block",
			header: sameSlotSuccessor(tip, "0e01"),
		},
		{
			name: "earlier slot than the tip block",
			header: &MockBlock{
				MockBlockNumber: tip.MockBlockNumber + 1,
				MockSlot:        tip.MockSlot - 1,
				MockHash:        testHashPrefix + "0e02",
				MockPrevHash:    tip.MockHash,
			},
		},
		{
			name:        "same slot as a queued header",
			queueHeader: true,
			header:      sameSlotSuccessor(queued, "0e03"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newEraOrderTestChain(t)
			for _, block := range testBlocks[:2] {
				require.NoError(t, c.AddBlock(context.Background(), block, nil))
			}
			if tc.queueHeader {
				require.NoError(
					t,
					c.AddBlockHeader(context.Background(), queued),
				)
			}
			headersBefore := c.HeaderCount()
			err := c.AddBlockHeader(context.Background(), tc.header)
			var slotErr chain.SlotNotAfterParentError
			require.ErrorAs(t, err, &slotErr)
			require.Equal(t, tc.header.MockSlot, slotErr.Slot())
			require.Equal(t, headersBefore, c.HeaderCount())
		})
	}
}

// TestAddBlockRejectsSlotNotAfterParent covers a block added with no queued
// header, such as a locally forged one.
func TestAddBlockRejectsSlotNotAfterParent(t *testing.T) {
	t.Parallel()

	tip := testBlocks[1]
	c := newEraOrderTestChain(t)
	for _, block := range testBlocks[:2] {
		require.NoError(t, c.AddBlock(context.Background(), block, nil))
	}
	err := c.AddBlock(context.Background(), sameSlotSuccessor(tip, "0e11"), nil)
	var slotErr chain.SlotNotAfterParentError
	require.ErrorAs(t, err, &slotErr)
	require.Equal(t, tip.Hash().Bytes(), c.Tip().Point.Hash)
	err = c.AddLocalBlock(context.Background(), sameSlotSuccessor(tip, "0e12"))
	require.ErrorAs(t, err, &slotErr)
	require.Equal(t, tip.Hash().Bytes(), c.Tip().Point.Hash)
}

// loadPreprodByronChain returns the preprod Byron chain fixture the ledger
// tests replay: the genesis epoch boundary block and every main block after
// it, as served by a preprod relay.
func loadPreprodByronChain(t *testing.T) []gledger.Block {
	t.Helper()
	raw, err := os.ReadFile(
		filepath.Join("..", "ledger", "testdata", "preprod-byron-chain.cbor"),
	)
	require.NoError(t, err)
	var entries []struct {
		cbor.StructAsArray
		Type  uint
		Block []byte
	}
	_, err = cbor.Decode(raw, &entries)
	require.NoError(t, err)
	blocks := make([]gledger.Block, 0, len(entries))
	for _, entry := range entries {
		block, err := gledger.NewBlockFromCbor(entry.Type, entry.Block)
		require.NoError(t, err)
		blocks = append(blocks, block)
	}
	return blocks
}

// TestSlotOrderAcceptsPreprodByronChain replays the preprod Byron chain, which
// cardano-node accepted, through each add path.
func TestSlotOrderAcceptsPreprodByronChain(t *testing.T) {
	t.Parallel()

	blocks := loadPreprodByronChain(t)
	require.Equal(t, gledger.BlockTypeByronEbb, blocks[0].Type())

	t.Run("headers", func(t *testing.T) {
		t.Parallel()
		c := newEraOrderTestChain(t)
		for _, block := range blocks {
			require.NoError(
				t,
				c.AddBlockHeader(context.Background(), block.Header()),
			)
		}
	})
	t.Run("blocks", func(t *testing.T) {
		t.Parallel()
		c := newEraOrderTestChain(t)
		for _, block := range blocks {
			require.NoError(t, c.AddBlock(context.Background(), block, nil))
		}
	})
	t.Run("batch", func(t *testing.T) {
		t.Parallel()
		c := newEraOrderTestChain(t)
		require.NoError(t, c.AddBlocks(context.Background(), blocks))
		require.Equal(
			t,
			blocks[len(blocks)-1].Hash().Bytes(),
			c.Tip().Point.Hash,
		)
	})
}

// TestSlotOrderAcceptsByronMainBlockAtBoundarySlot extends the preprod Byron
// chain with the next epoch's boundary block and a main block in that epoch's
// first slot, which the boundary block also occupies. The batch case finds the
// boundary parent only through the batch's own transaction.
func TestSlotOrderAcceptsByronMainBlockAtBoundarySlot(t *testing.T) {
	t.Parallel()

	blocks := loadPreprodByronChain(t)
	last := blocks[len(blocks)-1]
	lastHeader, ok := last.Header().(*byron.ByronMainBlockHeader)
	require.True(t, ok)
	nextEpoch := lastHeader.ConsensusData.SlotId.Epoch + 1
	ebbHeader := &byron.ByronEpochBoundaryBlockHeader{PrevBlock: last.Hash()}
	ebbHeader.ConsensusData.Epoch = nextEpoch
	ebbHeader.ConsensusData.Difficulty.Value = last.BlockNumber()
	mainHeader := &byron.ByronMainBlockHeader{PrevBlock: ebbHeader.Hash()}
	mainHeader.ConsensusData.SlotId.Epoch = nextEpoch
	mainHeader.ConsensusData.Difficulty.Value = last.BlockNumber() + 1
	require.Equal(t, ebbHeader.SlotNumber(), mainHeader.SlotNumber())
	ebb := &byron.ByronEpochBoundaryBlock{BlockHeader: ebbHeader}
	main := &byron.ByronMainBlock{BlockHeader: mainHeader}

	t.Run("headers", func(t *testing.T) {
		t.Parallel()
		c := newEraOrderTestChain(t)
		require.NoError(t, c.AddBlock(context.Background(), last, nil))
		require.NoError(t, c.AddBlockHeader(context.Background(), ebbHeader))
		require.NoError(t, c.AddBlockHeader(context.Background(), mainHeader))
	})
	t.Run("blocks", func(t *testing.T) {
		t.Parallel()
		c := newEraOrderTestChain(t)
		require.NoError(t, c.AddBlock(context.Background(), last, nil))
		require.NoError(t, c.AddBlock(context.Background(), ebb, nil))
		require.NoError(t, c.AddBlock(context.Background(), main, nil))
	})
	t.Run("batch", func(t *testing.T) {
		t.Parallel()
		c := newEraOrderTestChain(t)
		require.NoError(
			t,
			c.AddBlocks(context.Background(), []gledger.Block{last, ebb, main}),
		)
		require.Equal(t, main.Hash().Bytes(), c.Tip().Point.Hash)
	})
}

// TestSlotOrderRejectsByronMainBlockReusingSlot is the negative half of the
// Byron exception: only a boundary parent's slot may be shared.
func TestSlotOrderRejectsByronMainBlockReusingSlot(t *testing.T) {
	t.Parallel()

	blocks := loadPreprodByronChain(t)
	parent := blocks[2]
	header := &byron.ByronMainBlockHeader{PrevBlock: parent.Hash()}
	header.ConsensusData.Difficulty.Value = parent.BlockNumber() + 1
	parentHeader, ok := parent.Header().(*byron.ByronMainBlockHeader)
	require.True(t, ok)
	header.ConsensusData.SlotId = parentHeader.ConsensusData.SlotId
	require.Equal(t, parent.SlotNumber(), header.SlotNumber())

	c := newEraOrderTestChain(t)
	for _, block := range blocks[:3] {
		require.NoError(t, c.AddBlock(context.Background(), block, nil))
	}
	var slotErr chain.SlotNotAfterParentError
	require.ErrorAs(
		t,
		c.AddBlockHeader(context.Background(), header),
		&slotErr,
	)
}
