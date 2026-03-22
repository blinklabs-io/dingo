// Copyright 2025 Blink Labs Software
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

package utxorpc

import (
	"context"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/immutable"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

type fakeDumpHistoryIter struct {
	queue []*chain.ChainIteratorResult
	idx   int
}

func (f *fakeDumpHistoryIter) Next(blocking bool) (*chain.ChainIteratorResult, error) {
	if f.idx >= len(f.queue) {
		return nil, chain.ErrIteratorChainTip
	}
	r := f.queue[f.idx]
	f.idx++
	return r, nil
}

func loadTestChainBlocks(t *testing.T, n int) []models.Block {
	t.Helper()
	imm, err := immutable.New("../database/immutable/testdata")
	require.NoError(t, err)
	it, err := imm.BlocksFromPoint(ocommon.Point{})
	require.NoError(t, err)
	defer it.Close()
	var out []models.Block
	for range n {
		b, err := it.Next()
		require.NoError(t, err)
		require.NotNil(t, b)
		lb, err := ledger.NewBlockFromCbor(b.Type, b.Cbor)
		require.NoError(t, err)
		out = append(out, models.Block{
			Hash:     lb.Hash().Bytes(),
			PrevHash: lb.PrevHash().Bytes(),
			Cbor:     b.Cbor,
			Slot:     lb.SlotNumber(),
			Number:   lb.BlockNumber(),
			Type:     uint(lb.Type()),
		})
	}
	return out
}

func TestCollectDumpHistoryPage_FullPageHasNextToken(t *testing.T) {
	t.Parallel()
	blocks := loadTestChainBlocks(t, 4)
	queue := make([]*chain.ChainIteratorResult, len(blocks))
	for i := range blocks {
		b := blocks[i]
		bc := b
		queue[i] = &chain.ChainIteratorResult{
			Point: ocommon.NewPoint(b.Slot, b.Hash),
			Block: bc,
		}
	}
	ctx := context.Background()
	out, last, hasMore, err := collectDumpHistoryPage(
		ctx,
		&fakeDumpHistoryIter{queue: queue},
		2,
	)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.NotNil(t, last)
	require.True(t, hasMore)
	require.Equal(t, blocks[1].Slot, last.Slot)
	ref := syncBlockRefFromModel(*last)
	require.Equal(t, blocks[1].Slot, ref.GetSlot())
	require.Equal(t, blocks[1].Hash, ref.GetHash())
	require.Equal(t, blocks[1].Number, ref.GetHeight())
}

func TestCollectDumpHistoryPage_PartialPageNoNextToken(t *testing.T) {
	t.Parallel()
	blocks := loadTestChainBlocks(t, 2)
	queue := make([]*chain.ChainIteratorResult, len(blocks))
	for i := range blocks {
		b := blocks[i]
		bc := b
		queue[i] = &chain.ChainIteratorResult{
			Point: ocommon.NewPoint(b.Slot, b.Hash),
			Block: bc,
		}
	}
	ctx := context.Background()
	out, last, hasMore, err := collectDumpHistoryPage(
		ctx,
		&fakeDumpHistoryIter{queue: queue},
		10,
	)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.NotNil(t, last)
	require.False(t, hasMore)
}

func TestCollectDumpHistoryPage_MaxItemsZero(t *testing.T) {
	t.Parallel()
	blocks := loadTestChainBlocks(t, 1)
	queue := []*chain.ChainIteratorResult{
		{
			Point: ocommon.NewPoint(blocks[0].Slot, blocks[0].Hash),
			Block: blocks[0],
		},
	}
	ctx := context.Background()
	out, last, hasMore, err := collectDumpHistoryPage(
		ctx,
		&fakeDumpHistoryIter{queue: queue},
		0,
	)
	require.NoError(t, err)
	require.Empty(t, out)
	require.Nil(t, last)
	require.False(t, hasMore)
}

func TestCollectDumpHistoryPage_SkipsRollback(t *testing.T) {
	t.Parallel()
	blocks := loadTestChainBlocks(t, 2)
	queue := []*chain.ChainIteratorResult{
		{
			Point:    ocommon.NewPoint(1, []byte{0x01}),
			Rollback: true,
		},
		{
			Point: ocommon.NewPoint(blocks[0].Slot, blocks[0].Hash),
			Block: blocks[0],
		},
		{
			Point: ocommon.NewPoint(blocks[1].Slot, blocks[1].Hash),
			Block: blocks[1],
		},
	}
	ctx := context.Background()
	out, _, hasMore, err := collectDumpHistoryPage(
		ctx,
		&fakeDumpHistoryIter{queue: queue},
		2,
	)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.False(t, hasMore)
}

func TestSyncBlockRefFromModel_CopiesHash(t *testing.T) {
	t.Parallel()
	b := models.Block{
		Slot:   5,
		Hash:   []byte{1, 2, 3},
		Number: 9,
	}
	ref := syncBlockRefFromModel(b)
	b.Hash[0] = 0xff
	require.Equal(t, byte(1), ref.GetHash()[0])
}
