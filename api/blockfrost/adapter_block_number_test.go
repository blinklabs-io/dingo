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

package blockfrost

import (
	"context"
	"encoding/hex"
	"math"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

func TestBlockNumbersDifferFromStorageIDs(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	raw.SetMaxOpenConns(1)
	// The second entry models an EBB sharing its parent's number.
	blocks := []models.Block{
		{ID: 2, Number: 1, Slot: 10, Hash: fill32(1), Cbor: []byte{1}},
		{ID: 3, Number: 1, Slot: 20, Hash: fill32(2), Cbor: []byte{2}},
		{ID: 4, Number: 2, Slot: 30, Hash: fill32(3), Cbor: []byte{3}},
		{ID: 5, Number: 4, Slot: 40, Hash: fill32(4), Cbor: []byte{4}},
	}
	for _, block := range blocks {
		require.NoError(t, db.BlockCreate(block, nil))
	}
	require.NoError(
		t,
		db.SetTip(
			ochainsync.Tip{
				Point:       ocommon.NewPoint(40, blocks[3].Hash),
				BlockNumber: 4,
			},
			nil,
		),
	)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	t.Run("successor hash", func(t *testing.T) {
		next, err := adapter.nextBlockHash(
			ctx,
			blocks[0].ID,
			ocommon.NewPoint(blocks[1].Slot, blocks[1].Hash),
		)
		require.NoError(t, err)
		require.NotNil(t, next)
		require.Equal(t, hex.EncodeToString(blocks[1].Hash), *next)
	})
	t.Run("numeric lookup", func(t *testing.T) {
		parent, err := adapter.resolveBlock(ctx, "1")
		require.NoError(t, err)
		require.Equal(t, blocks[0].Hash, parent.Hash)
		block, err := adapter.resolveBlock(ctx, "2")
		require.NoError(t, err)
		require.Equal(t, blocks[2].Hash, block.Hash)
		_, err = adapter.resolveBlock(ctx, "3")
		require.ErrorIs(t, err, ErrBlockNotFound)
	})
	t.Run("physical neighbors", func(t *testing.T) {
		next, total, err := adapter.neighborBlocks(
			ctx,
			hex.EncodeToString(blocks[1].Hash),
			PaginationParams{Count: 2, Page: 1, Order: PaginationOrderAsc},
			true,
		)
		require.NoError(t, err)
		require.Equal(t, 2, total)
		require.Equal(t, blocks[2:], next)
		prev, _, err := adapter.neighborBlocks(
			ctx,
			hex.EncodeToString(blocks[2].Hash),
			PaginationParams{Count: 1, Page: 1, Order: PaginationOrderDesc},
			false,
		)
		require.NoError(t, err)
		require.Len(t, prev, 1)
		require.Equal(t, blocks[1].Hash, prev[0].Hash)
	})
	t.Run("range boundaries", func(t *testing.T) {
		index := uint32(7)
		for _, tc := range []struct {
			number uint64
			lower  bool
			slot   uint64
			index  uint32
		}{
			{1, true, 10, 7},
			{1, false, 20, 7},
			{2, true, 30, 7},
			{2, false, 30, 7},
			{3, true, 40, 0},
			{3, false, 30, math.MaxUint32},
			{99, false, 40, math.MaxUint32},
		} {
			bound, ok, err := adapter.resolveBlockRangeBound(
				ctx,
				&BlockRangePosition{Block: tc.number, Index: &index},
				tc.lower,
			)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, tc.slot, bound.Slot)
			require.Equal(t, tc.index, bound.TxIndex)
		}
	})
}

func TestNeighborBlocksStopAtAppliedTip(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	chain := seedChain(t, db, 5)
	require.NoError(
		t,
		db.SetTip(
			ochainsync.Tip{
				Point: ocommon.NewPoint(
					chain[2].SlotNumber(),
					chain[2].Hash().Bytes(),
				),
				BlockNumber: 3,
			},
			nil,
		),
	)
	blocks, total, err := adapter.neighborBlocks(
		context.Background(),
		chain[2].Hash().String(),
		PaginationParams{Count: 2, Page: 1, Order: PaginationOrderAsc},
		true,
	)
	require.NoError(t, err)
	require.Empty(t, blocks)
	require.Zero(t, total)
}

func TestRangeBoundsStopAtAppliedTipBeforePrefetchedEBB(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	for i, number := range []uint64{1, 1, 2} {
		require.NoError(
			t,
			db.BlockCreate(
				models.Block{
					ID:     uint64(i) + 2,
					Number: number,
					Slot:   uint64(i+1) * 10,
					Hash:   fill32(byte(i + 1)),
					Cbor:   []byte{0x80},
				},
				nil,
			),
		)
	}
	require.NoError(
		t,
		db.SetTip(
			ochainsync.Tip{
				Point:       ocommon.NewPoint(10, fill32(1)),
				BlockNumber: 1,
			},
			nil,
		),
	)
	for _, number := range []uint64{1, 99} {
		bound, ok, err := adapter.resolveBlockRangeBound(
			context.Background(),
			&BlockRangePosition{Block: number},
			false,
		)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, uint64(10), bound.Slot)
	}
}

func TestRangeBoundsRequireAnAppliedTip(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	require.NoError(
		t,
		db.BlockCreate(
			models.Block{
				ID:     1,
				Number: 0,
				Slot:   10,
				Hash:   fill32(1),
				Cbor:   []byte{0x80},
			},
			nil,
		),
	)
	for _, lower := range []bool{false, true} {
		bound, ok, err := adapter.resolveBlockRangeBound(
			context.Background(),
			&BlockRangePosition{Block: 0},
			lower,
		)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, bound)
	}
	require.NoError(
		t,
		db.SetTip(
			ochainsync.Tip{
				Point:       ocommon.NewPoint(20, fill32(2)),
				BlockNumber: 0,
			},
			nil,
		),
	)
	_, _, err := adapter.resolveBlockRangeBound(
		context.Background(),
		&BlockRangePosition{Block: 0},
		false,
	)
	require.ErrorIs(t, err, models.ErrBlockNotFound)
}

func TestNeighborBlocksWithoutAppliedTipAreEmpty(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	chain := seedChain(t, db, 2)
	blocks, total, err := adapter.neighborBlocks(
		context.Background(),
		chain[0].Hash().String(),
		PaginationParams{Count: 1, Page: 1, Order: PaginationOrderAsc},
		true,
	)
	require.NoError(t, err)
	require.Empty(t, blocks)
	require.Zero(t, total)
}

func TestNextBlockHashDoesNotRequireSuccessorBody(t *testing.T) {
	t.Parallel()
	adapter, raw, db := newDBBackedAdapter(t)
	raw.SetMaxOpenConns(1)
	hash := fill32(0x81)
	require.NoError(
		t,
		db.BlockCreate(
			models.Block{
				ID:     3,
				Number: 1,
				Slot:   20,
				Hash:   hash,
				Cbor:   []byte{0x80},
			},
			nil,
		),
	)
	txn := db.BlobTxn(true)
	require.NoError(
		t,
		db.Blob().Delete(txn.Blob(), types.BlockBlobKey(20, hash)),
	)
	require.NoError(t, txn.Commit())
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	next, err := adapter.nextBlockHash(ctx, 2, ocommon.NewPoint(20, hash))
	require.NoError(t, err)
	require.NotNil(t, next)
	require.Equal(t, hex.EncodeToString(hash), *next)
}
