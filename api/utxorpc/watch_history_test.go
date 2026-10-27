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
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	cardano "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch"
)

// sizedHistoryEntry returns an entry whose retained size is about n bytes.
func sizedHistoryEntry(slot uint64, n int) watchTxHistoryEntry {
	return watchTxHistoryEntry{
		point: ocommon.NewPoint(slot, []byte{byte(slot)}),
		appliedTxs: []*watch.AnyChainTx{{
			Chain: &watch.AnyChainTx_Cardano{
				Cardano: &cardano.Tx{Hash: make([]byte, n)},
			},
		}},
	}
}

func (b *watchTxHistoryBudget) usedBytes() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// A stream that exceeds its own byte cap drops its oldest entries first and
// returns their bytes to the shared budget.
func TestWatchTxHistoryTracker_EvictsOldestOverStreamBytes(t *testing.T) {
	t.Parallel()
	// The per-stream cap holds exactly two entries of this size.
	budget := newWatchTxHistoryBudget(1 << 30)
	entrySize := watchTxEntrySize(sizedHistoryEntry(0, 100).appliedTxs)
	tracker := newWatchTxHistoryTracker(budget, 256, 2*entrySize)

	var history []watchTxHistoryEntry
	for slot := uint64(1); slot <= 5; slot++ {
		history = tracker.push(history, sizedHistoryEntry(slot, 100))
	}

	// Only the two newest remain, and the budget holds only their bytes.
	require.Len(t, history, 2)
	require.Equal(t, uint64(4), history[0].point.Slot)
	require.Equal(t, uint64(5), history[1].point.Slot)
	require.Equal(t, 2*entrySize, budget.usedBytes())
}

// An entry larger than the per-stream cap is not retained, and it takes the
// older entries with it so the history never has a gap.
func TestWatchTxHistoryTracker_OversizedEntryIsNotRetained(t *testing.T) {
	t.Parallel()
	budget := newWatchTxHistoryBudget(1 << 30)
	tracker := newWatchTxHistoryTracker(budget, 256, 50)

	var history []watchTxHistoryEntry
	history = tracker.push(history, sizedHistoryEntry(1, 10))
	require.Len(t, history, 1)
	history = tracker.push(history, sizedHistoryEntry(2, 1000))

	// Older entries go too: keeping slot 1 without slot 2 would leave a gap
	// that the rollback path could not detect.
	require.Empty(t, history)
	require.Zero(t, budget.usedBytes())
}

// The process-wide budget is shared: when one stream has used it all, another
// stream retains nothing, and a stream frees its share when it closes.
func TestWatchTxHistoryTracker_SharedBudgetBoundsStreams(t *testing.T) {
	t.Parallel()
	// The shared budget holds exactly three entries.
	entrySize := watchTxEntrySize(sizedHistoryEntry(0, 100).appliedTxs)
	budget := newWatchTxHistoryBudget(3 * entrySize)
	a := newWatchTxHistoryTracker(budget, 256, 1<<30)
	b := newWatchTxHistoryTracker(budget, 256, 1<<30)

	var ha, hb []watchTxHistoryEntry
	for slot := uint64(1); slot <= 3; slot++ {
		ha = a.push(ha, sizedHistoryEntry(slot, 100))
	}
	require.Len(t, ha, 3)

	// The budget is full of stream a's entries; b cannot evict them, so its
	// entry is not retained.
	hb = b.push(hb, sizedHistoryEntry(10, 100))
	require.Empty(t, hb)
	require.Equal(t, 3*entrySize, budget.usedBytes())

	// a evicts its own oldest to keep admitting new blocks.
	ha = a.push(ha, sizedHistoryEntry(4, 100))
	require.Len(t, ha, 3)
	require.Equal(t, uint64(2), ha[0].point.Slot)
	require.Equal(t, 3*entrySize, budget.usedBytes())

	a.close()
	require.Zero(t, budget.usedBytes())
	hb = b.push(hb, sizedHistoryEntry(11, 100))
	require.Len(t, hb, 1)
}

// Entries removed by a rollback give their bytes back to the budget, and
// close gives back whatever the stream still holds.
func TestWatchTxHistoryTracker_RollbackReleasesBytes(t *testing.T) {
	t.Parallel()
	budget := newWatchTxHistoryBudget(1 << 30)
	tracker := newWatchTxHistoryTracker(budget, 256, 1<<30)

	var history []watchTxHistoryEntry
	for slot := uint64(1); slot <= 3; slot++ {
		history = tracker.push(history, sizedHistoryEntry(slot, 100))
	}
	_, found := watchTxBuildRollbackMessages(
		&history,
		ocommon.NewPoint(1, []byte{1}),
	)
	require.True(t, found)
	tracker.rolledBack(history)

	require.Equal(
		t,
		watchTxEntrySize(sizedHistoryEntry(1, 100).appliedTxs),
		budget.usedBytes(),
	)
	tracker.close()
	require.Zero(t, budget.usedBytes())
}

// The byte limits sit on top of the existing block-count limit, which still
// applies when the bytes are tiny.
func TestWatchTxHistoryTracker_BlockCountStillApplies(t *testing.T) {
	t.Parallel()
	budget := newWatchTxHistoryBudget(1 << 30)
	tracker := newWatchTxHistoryTracker(budget, 3, 1<<30)

	var history []watchTxHistoryEntry
	for slot := uint64(1); slot <= 10; slot++ {
		history = tracker.push(history, sizedHistoryEntry(slot, 1))
	}
	require.Len(t, history, 3)
	require.Equal(t, uint64(8), history[0].point.Slot)
}

// End to end: when the byte cap evicts a block's undo entry, a later rollback
// of that block must still produce its Undo messages. The rolled-back block is
// already deleted from the database by then, so the rebuild has to find it in
// the chain manager's cache of rolled-back blocks. The ledger state is real:
// nothing here substitutes the block lookup.
func TestConnect_WatchTx_EvictedHistoryRollbackRebuildsFromBlocks(
	t *testing.T,
) {
	// Find the first fixture block that carries transactions.
	scan := loadTestChainBlocksWithPeriodicTransactions(t, 80)
	txIdx := -1
	for idx := 2; idx < len(scan); idx++ {
		block, err := gledger.NewBlockFromCbor(
			scan[idx].Type,
			scan[idx].Cbor,
		)
		require.NoError(t, err)
		if len(block.Transactions()) > 0 {
			txIdx = idx
			break
		}
	}
	require.GreaterOrEqual(t, txIdx, 2, "fixture has no transaction block")
	blocks := scan[:txIdx+1]
	undoCount := requireFixtureWatchTxAppliedCount(t, blocks[txIdx])
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{blocks: blocks})
	// One byte retains no transaction-bearing entry, so the rollback below
	// can only be answered by rebuilding from stored blocks.
	h.U.config.MaxWatchHistoryBytes = 1

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream := startWatchTxAt(t, ctx, h, blocks[txIdx-1])
	for range undoCount {
		require.True(t, stream.Receive(), "stream ended: %v", stream.Err())
		_, ok := stream.Msg().Action.(*watch.WatchTxResponse_Apply)
		require.True(t, ok, "expected Apply, got %T", stream.Msg().Action)
	}
	require.Zero(t, h.U.watchHistory.usedBytes())

	require.NoError(
		t,
		h.LS.Chain().Rollback(context.Background(), ocommon.NewPoint(
			blocks[txIdx-1].Slot,
			blocks[txIdx-1].Hash,
		)),
	)
	// The rollback deleted the block from the database.
	_, err := h.LS.BlockByHash(context.Background(), blocks[txIdx].Hash)
	require.ErrorIs(t, err, models.ErrBlockNotFound)

	requireWatchTxUndos(t, stream, undoCount)
}

// If a rolled-back block cannot be found at all, the stream must fail rather
// than end the rebuild early and leave the client on the abandoned chain.
func TestConnect_WatchTx_UnavailableRolledBackBlockFailsStream(t *testing.T) {
	scan := loadTestChainBlocksWithPeriodicTransactions(t, 80)
	start := findEmptyFixtureRun(t, scan, 2)
	blocks := scan[:start+2]
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{blocks: blocks})
	// No entry is retained, so the rollback needs the rebuild path.
	h.U.config.MaxWatchHistoryBytes = 1
	h.U.config.LedgerState = &watchTxLedgerStateProbe{
		UtxorpcLedgerState: h.LS,
		blockByHash: func([]byte) (models.Block, error) {
			return models.Block{}, models.ErrBlockNotFound
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream := startWatchTxAt(t, ctx, h, blocks[start-1])
	requireWatchTxIdle(t, stream)
	requireWatchTxIdle(t, stream)

	require.NoError(
		t,
		h.LS.Chain().Rollback(context.Background(), ocommon.NewPoint(
			blocks[start-1].Slot,
			blocks[start-1].Hash,
		)),
	)
	require.False(t, stream.Receive())
	require.ErrorContains(t, stream.Err(), "is unavailable")
}

// End to end: after a rollback removes entries from a stream's undo history,
// their bytes are released from the shared budget.
func TestConnect_WatchTx_RollbackReleasesHistoryBudget(t *testing.T) {
	// Find the first fixture block that carries transactions.
	scan := loadTestChainBlocksWithPeriodicTransactions(t, 80)
	txIdx := -1
	for idx := 2; idx < len(scan); idx++ {
		block, err := gledger.NewBlockFromCbor(
			scan[idx].Type,
			scan[idx].Cbor,
		)
		require.NoError(t, err)
		if len(block.Transactions()) > 0 {
			txIdx = idx
			break
		}
	}
	require.GreaterOrEqual(t, txIdx, 2, "fixture has no transaction block")
	blocks := scan[:txIdx+1]
	undoCount := requireFixtureWatchTxAppliedCount(t, blocks[txIdx])
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{blocks: blocks})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream := startWatchTxAt(t, ctx, h, blocks[txIdx-1])
	for range undoCount {
		require.True(t, stream.Receive(), "stream ended: %v", stream.Err())
	}
	require.NotZero(t, h.U.watchHistory.usedBytes())

	require.NoError(
		t,
		h.LS.Chain().Rollback(context.Background(), ocommon.NewPoint(
			blocks[txIdx-1].Slot,
			blocks[txIdx-1].Hash,
		)),
	)
	requireWatchTxUndos(t, stream, undoCount)
	require.Zero(t, h.U.watchHistory.usedBytes())
}
