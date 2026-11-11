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
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
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
		// Total is the height distance to the tip (4 - 1), not an entry count.
		require.Equal(t, 3, total)
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

func seedSparseChain(
	t *testing.T,
	db *database.Database,
	ids []uint64,
) []models.Block {
	t.Helper()
	blocks := make([]models.Block, 0, len(ids))
	for i, id := range ids {
		hash := fill32(byte(i + 1))
		hash[0] = byte((i + 1) >> 8) // keeps hashes distinct past 255 blocks
		block := models.Block{
			ID:     id,
			Number: uint64(i),
			Slot:   uint64(i+1) * 10,
			Hash:   hash,
			Cbor:   []byte{0x80},
			Type:   uint(gledger.BlockTypeConway),
		}
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}
	last := blocks[len(blocks)-1]
	require.NoError(
		t,
		db.SetTip(
			ochainsync.Tip{
				Point:       ocommon.NewPoint(last.Slot, last.Hash),
				BlockNumber: last.Number,
			},
			nil,
		),
	)
	return blocks
}

func TestNextBlockHashSkipsSparseIDs(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	blocks := seedSparseChain(t, db, []uint64{2, 1000, 1005})
	tip := ocommon.NewPoint(blocks[2].Slot, blocks[2].Hash)
	for i := range 2 {
		next, err := adapter.nextBlockHash(
			t.Context(), blocks[i].ID, tip,
		)
		require.NoError(t, err)
		require.NotNil(t, next)
		require.Equal(t, hex.EncodeToString(blocks[i+1].Hash), *next)
	}
	next, err := adapter.nextBlockHash(t.Context(), blocks[2].ID, tip)
	require.NoError(t, err)
	require.Nil(t, next)
}

func TestNeighborBlocksSkipSparseIDs(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	blocks := seedSparseChain(t, db, []uint64{2, 1000, 1005, 2000})
	heights := func(got []models.Block) []uint64 { return heightsOf(got) }
	ctx := t.Context()

	got, total, err := adapter.neighborBlocks(
		ctx,
		hex.EncodeToString(blocks[0].Hash),
		PaginationParams{Count: 2, Page: 1},
		true,
	)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, heights(got))
	require.Equal(t, 3, total)

	got, _, err = adapter.neighborBlocks(
		ctx,
		hex.EncodeToString(blocks[0].Hash),
		PaginationParams{Count: 2, Page: 2},
		true,
	)
	require.NoError(t, err)
	require.Equal(t, []uint64{3}, heights(got))

	got, total, err = adapter.neighborBlocks(
		ctx,
		hex.EncodeToString(blocks[3].Hash),
		PaginationParams{Count: 2, Page: 1},
		false,
	)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2}, heights(got))
	require.Equal(t, 3, total)

	got, _, err = adapter.neighborBlocks(
		ctx,
		hex.EncodeToString(blocks[3].Hash),
		PaginationParams{Count: 2, Page: 2},
		false,
	)
	require.NoError(t, err)
	require.Equal(t, []uint64{0}, heights(got))
}

// errCountingContext counts how often the walk checks for cancellation, once
// per index entry it steps over.
type errCountingContext struct {
	context.Context
	calls *atomic.Int64
}

func (c errCountingContext) Err() error {
	c.calls.Add(1)
	return c.Context.Err()
}

func TestNeighborBlocksOutOfRangePageDoesNotWalkTheIndex(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	const wide = 100
	ids := make([]uint64, wide+10)
	for i := range ids {
		ids[i] = uint64(i) + 1
	}
	blocks := seedSparseChain(t, db, ids)

	steps := func(hash []byte, page, count int, forward bool) ([]models.Block, int64) {
		var calls atomic.Int64
		got, _, err := adapter.neighborBlocks(
			errCountingContext{Context: t.Context(), calls: &calls},
			hex.EncodeToString(hash),
			PaginationParams{Count: count, Page: page},
			forward,
		)
		require.NoError(t, err)
		return got, calls.Load()
	}

	// A full page checks for cancellation once per block it reads, on top of
	// a fixed lookup cost that varies between runs. An out-of-range page reads
	// no blocks, so it must cost well under half of the full page.
	for _, tc := range []struct {
		name    string
		hash    []byte
		forward bool
	}{
		{"forward", blocks[0].Hash, true},
		{"backward", blocks[len(blocks)-1].Hash, false},
	} {
		full, walked := steps(tc.hash, 1, wide, tc.forward)
		require.Len(t, full, wide, tc.name)
		got, calls := steps(tc.hash, 21474836, wide, tc.forward)
		require.Empty(t, got, tc.name)
		require.Less(t, calls, walked/2, tc.name)
	}
}

func TestNeighborBlocksDeepPageJumpsByBlockNumber(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	ids := make([]uint64, 1200)
	for i := range ids {
		ids[i] = uint64(i)*3 + 1
	}
	blocks := seedSparseChain(t, db, ids)

	run := func(hash []byte, forward bool) ([]uint64, int64) {
		var calls atomic.Int64
		got, _, err := adapter.neighborBlocks(
			errCountingContext{Context: t.Context(), calls: &calls},
			hex.EncodeToString(hash),
			PaginationParams{Count: 5, Page: 60},
			forward,
		)
		require.NoError(t, err)
		return heightsOf(got), calls.Load()
	}

	// Page 60 of 5 starts 295 heights past the origin.
	got, calls := run(blocks[10].Hash, true)
	require.ElementsMatch(t, []uint64{306, 307, 308, 309, 310}, got)
	require.Less(t, calls, int64(150), "forward deep page walked the prefix")

	got, calls = run(blocks[1199].Hash, false)
	require.ElementsMatch(t, []uint64{899, 900, 901, 902, 903}, got)
	require.Less(t, calls, int64(150), "backward deep page walked the prefix")
}

type indexedBlock struct {
	id     uint64
	number uint64
	typ    uint
}

// seedIndexedBlocks stores blocks with explicit IDs, numbers, and types so a
// chain can carry Byron boundary blocks that share their parent's number.
func seedIndexedBlocks(
	t *testing.T,
	db *database.Database,
	specs []indexedBlock,
) []models.Block {
	t.Helper()
	blocks := make([]models.Block, 0, len(specs))
	for i, spec := range specs {
		hash := fill32(byte(i + 1))
		hash[0] = byte((i + 1) >> 8)
		block := models.Block{
			ID:     spec.id,
			Number: spec.number,
			Slot:   uint64(i+1) * 10,
			Hash:   hash,
			Cbor:   []byte{0x80},
			Type:   spec.typ,
		}
		require.NoError(t, db.BlockCreate(block, nil))
		blocks = append(blocks, block)
	}
	last := blocks[len(blocks)-1]
	require.NoError(
		t,
		db.SetTip(
			ochainsync.Tip{
				Point:       ocommon.NewPoint(last.Slot, last.Hash),
				BlockNumber: last.Number,
			},
			nil,
		),
	)
	return blocks
}

func blockHashesOf(blocks []models.Block) []string {
	ret := make([]string, 0, len(blocks))
	for _, block := range blocks {
		ret = append(ret, hex.EncodeToString(block.Hash))
	}
	return ret
}

// assertEveryNeighborPage checks every origin, page size, and page in both
// directions against the entries in chain order.
func assertEveryNeighborPage(
	t *testing.T,
	adapter *NodeAdapter,
	blocks []models.Block,
) {
	t.Helper()
	for i, origin := range blocks {
		hash := hex.EncodeToString(origin.Hash)
		for _, forward := range []bool{true, false} {
			var side []models.Block
			if forward {
				side = blocks[i+1:]
			} else {
				side = make([]models.Block, 0, i)
				for j := i - 1; j >= 0; j-- {
					side = append(side, blocks[j])
				}
			}
			for count := 1; count <= 3; count++ {
				for page := 1; page <= len(blocks)+1; page++ {
					start := min((page-1)*count, len(side))
					end := min(page*count, len(side))
					want := blockHashesOf(side[start:end])
					if !forward {
						slices.Reverse(want)
					}
					got, _, err := adapter.neighborBlocks(
						t.Context(),
						hash,
						PaginationParams{Count: count, Page: page},
						forward,
					)
					require.NoError(t, err)
					require.Equal(
						t, want, blockHashesOf(got),
						"origin %d forward=%v count=%d page=%d",
						i, forward, count, page,
					)
				}
			}
		}
	}
}

func TestNeighborBlocksCountEntriesSharingAHeight(t *testing.T) {
	t.Parallel()
	const (
		ebb  = uint(gledger.BlockTypeByronEbb)
		main = uint(gledger.BlockTypeByronMain)
	)
	for name, specs := range map[string][]indexedBlock{
		"tip after the boundary block": {
			{1, 1, main}, {2, 1, ebb}, {3, 2, main},
		},
		"tip beyond the next height": {
			{1, 1, main}, {2, 1, ebb}, {3, 2, main}, {4, 3, main},
		},
		"boundary blocks at the start": {
			{1, 0, ebb}, {2, 0, main}, {3, 1, main}, {4, 1, ebb},
			{5, 2, main}, {6, 3, main},
		},
		"sparse IDs": {
			{2, 0, main}, {9, 1, main}, {10, 1, ebb}, {50, 2, main},
			{51, 3, main}, {900, 3, ebb}, {901, 4, main},
		},
		"byron into later eras": {
			{1, 0, main}, {2, 1, main}, {3, 1, ebb}, {4, 2, main},
			{5, 3, uint(gledger.BlockTypeConway)}, {6, 4, uint(gledger.BlockTypeConway)},
			{7, 5, uint(gledger.BlockTypeConway)}, {8, 6, uint(gledger.BlockTypeConway)},
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			adapter, _, db := newDBBackedAdapter(t)
			blocks := seedIndexedBlocks(t, db, specs)
			assertEveryNeighborPage(t, adapter, blocks)
		})
	}
}

func TestNeighborBlocksDeepPageAfterBoundaryBlocksStaysBounded(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	specs := []indexedBlock{
		{1, 0, uint(gledger.BlockTypeByronMain)},
		{2, 1, uint(gledger.BlockTypeByronMain)},
		{3, 1, uint(gledger.BlockTypeByronEbb)},
		{4, 2, uint(gledger.BlockTypeByronMain)},
	}
	for i := range 1200 {
		specs = append(specs, indexedBlock{
			id:     uint64(10 + i*3),
			number: uint64(3 + i),
			typ:    uint(gledger.BlockTypeConway),
		})
	}
	blocks := seedIndexedBlocks(t, db, specs)

	run := func(origin models.Block, forward bool) ([]string, int64) {
		var calls atomic.Int64
		got, _, err := adapter.neighborBlocks(
			errCountingContext{Context: t.Context(), calls: &calls},
			hex.EncodeToString(origin.Hash),
			PaginationParams{Count: 5, Page: 60},
			forward,
		)
		require.NoError(t, err)
		return blockHashesOf(got), calls.Load()
	}

	// Page 60 skips 295 entries. From the first block the Byron entries
	// are counted and the rest crossed by number; from the tip the
	// post-Byron blocks are crossed by number and the Byron ones counted.
	got, calls := run(blocks[0], true)
	require.Equal(t, blockHashesOf(blocks[296:301]), got)
	require.Less(t, calls, int64(150), "forward page walked the prefix")

	got, calls = run(blocks[len(blocks)-1], false)
	last := len(blocks) - 1
	require.Equal(t, blockHashesOf(blocks[last-300:last-295]), got)
	require.Less(t, calls, int64(150), "backward page walked the prefix")

	// A page that reaches back across the Byron entries.
	pageBack, _, err := adapter.neighborBlocks(
		t.Context(),
		hex.EncodeToString(blocks[5].Hash),
		PaginationParams{Count: 2, Page: 2},
		false,
	)
	require.NoError(t, err)
	require.Equal(t, blockHashesOf(blocks[1:3]), blockHashesOf(pageBack))
}

func TestNeighborBlocksPageStartOnPrunedChain(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	specs := []indexedBlock{
		{1, 0, uint(gledger.BlockTypeByronMain)},
		{2, 1, uint(gledger.BlockTypeByronMain)},
		{3, 1, uint(gledger.BlockTypeByronEbb)},
		{4, 2, uint(gledger.BlockTypeByronMain)},
	}
	for i := range 40 {
		specs = append(specs, indexedBlock{
			id:     uint64(10 + i),
			number: uint64(3 + i),
			typ:    uint(gledger.BlockTypeConway),
		})
	}
	blocks := seedIndexedBlocks(t, db, specs)

	// History expires oldest first; only the last 20 blocks keep their body.
	expired := blocks[:len(blocks)-20]
	require.NoError(t, db.BlobTxn(true).Do(func(txn *database.Txn) error {
		for _, block := range expired {
			if err := db.Blob().TombstoneBlock(
				txn.Blob(), block.Slot, block.Hash,
			); err != nil {
				return err
			}
		}
		return nil
	}))

	last := len(blocks) - 1
	got, total, err := adapter.neighborBlocks(
		t.Context(),
		hex.EncodeToString(blocks[last].Hash),
		PaginationParams{Count: 5, Page: 2},
		false,
	)
	require.NoError(t, err)
	require.Equal(t, blockHashesOf(blocks[last-10:last-5]), blockHashesOf(got))
	require.Equal(t, int(blocks[last].Number), total)
}

func TestNeighborBlocksTotalIsStableAcrossPages(t *testing.T) {
	t.Parallel()
	adapter, _, db := newDBBackedAdapter(t)
	blocks := seedIndexedBlocks(t, db, []indexedBlock{
		{1, 1, uint(gledger.BlockTypeByronMain)},
		{2, 1, uint(gledger.BlockTypeByronEbb)},
		{3, 2, uint(gledger.BlockTypeByronMain)},
		{4, 3, uint(gledger.BlockTypeByronMain)},
	})
	for _, forward := range []bool{true, false} {
		origin := blocks[0]
		if !forward {
			origin = blocks[3]
		}
		totals := map[int]struct{}{}
		for page := 1; page <= 4; page++ {
			_, total, err := adapter.neighborBlocks(
				t.Context(),
				hex.EncodeToString(origin.Hash),
				PaginationParams{Count: 1, Page: page},
				forward,
			)
			require.NoError(t, err)
			totals[total] = struct{}{}
		}
		require.Len(t, totals, 1, "forward=%v totals=%v", forward, totals)
	}
}
