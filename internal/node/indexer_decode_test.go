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

package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/stretchr/testify/require"
)

// decodeTestBlocks returns n consecutive blocks that each carry transactions.
func decodeTestBlocks(t *testing.T, n int) []models.Block {
	t.Helper()
	chain, err := testfixtures.GenerateConwayChainWithTransactions(n)
	require.NoError(t, err)
	require.Len(t, chain, n)
	blocks := make([]models.Block, n)
	for i, b := range chain {
		require.NotEmpty(t, b.Transactions(), "block %d needs transactions", i)
		blocks[i] = models.Block{
			Slot: b.SlotNumber(), Hash: b.Hash().Bytes(),
			PrevHash: b.PrevHash().Bytes(), Number: b.BlockNumber(),
			Cbor: b.Cbor(), Type: uint(b.Type()),
		}
	}
	return blocks
}

func indexDecodeTestBlocks(
	ctx context.Context,
	db *database.Database,
	bf *Backfill,
	blocks []models.Block,
) error {
	return db.Transaction(context.Background(), true).Do(
		func(txn *database.Txn) error {
			return bf.IndexBlocks(ctx, txn, blocks)
		},
	)
}

// Offsets depend only on the block bytes, so a later block's offsets are
// computed while the writer is still on an earlier one. Serial decoding
// never reaches the second call while the first is in flight.
func TestIndexBlocksDecodesAheadOfWriter(t *testing.T) {
	t.Parallel()
	if runtime.GOMAXPROCS(0) < 4 {
		t.Skip("needs at least two decode workers")
	}
	blocks := decodeTestBlocks(t, 4)
	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())
	bf.SetDelegatorInactivityEnabled(false)
	secondEntered := make(chan struct{})
	var once sync.Once
	var firstTimedOut atomic.Bool
	bf.computeOffsets = func(
		slot uint64,
		hash, blockCbor []byte,
		block gledger.Block,
	) (*database.BlockIngestionResult, error) {
		switch slot {
		case blocks[0].Slot:
			select {
			case <-secondEntered:
			case <-time.After(10 * time.Second):
				firstTimedOut.Store(true)
			}
		case blocks[1].Slot:
			once.Do(func() { close(secondEntered) })
		}
		return database.NewBlockIndexer(slot, hash).ComputeOffsets(
			blockCbor, block,
		)
	}
	require.NoError(
		t, indexDecodeTestBlocks(t.Context(), db, bf, blocks),
	)
	require.False(
		t, firstTimedOut.Load(),
		"block 1 offsets were not computed while block 0 was in flight",
	)
}

// A block that fails to decode fails the call at that block, as the serial
// loop would, and carries the block's slot.
func TestIndexBlocksReportsDecodeErrorAtItsBlock(t *testing.T) {
	t.Parallel()
	blocks := decodeTestBlocks(t, 4)
	bad := 2
	blocks[bad].Cbor = blocks[bad].Cbor[:len(blocks[bad].Cbor)/2]
	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())
	bf.SetDelegatorInactivityEnabled(false)
	err := indexDecodeTestBlocks(t.Context(), db, bf, blocks)
	require.ErrorContains(
		t, err,
		fmt.Sprintf("indexing block at slot %d: parsing block", blocks[bad].Slot),
	)
}

// A cancelled context ends the call with the context's error and leaves no
// decode worker running behind it.
func TestIndexBlocksCancellationStopsDecodeWorkers(t *testing.T) {
	t.Parallel()
	blocks := decodeTestBlocks(t, 4)
	db := newTestDB(t)
	bf := NewBackfill(db, nil, slog.Default())
	bf.SetDelegatorInactivityEnabled(false)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var calls atomic.Int64
	bf.computeOffsets = func(
		slot uint64,
		hash, blockCbor []byte,
		block gledger.Block,
	) (*database.BlockIngestionResult, error) {
		calls.Add(1)
		cancel()
		return database.NewBlockIndexer(slot, hash).ComputeOffsets(
			blockCbor, block,
		)
	}
	err := indexDecodeTestBlocks(ctx, db, bf, blocks)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled), "got %v", err)
	atReturn := calls.Load()
	runtime.Gosched()
	require.Equal(t, atReturn, calls.Load(), "worker ran after return")
}
