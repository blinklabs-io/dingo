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

package utxorpc

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/query/queryconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync/syncconnect"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch/watchconnect"
)

// These tests expire a block through Database.PruneBlock, the path the
// history-expiry pruner uses, and read it back over Connect. The stub-ledger
// tests in history_expired_test.go inject the error at the first ledger
// call; here it comes from the blob store wherever production reads it,
// including the chain iterator, whose start point survives expiry.

const expiredHistoryHarnessBlocks = 8

// newPrunedHistoryHarness starts a harness and expires blocks[pruned].
func newPrunedHistoryHarness(
	t *testing.T,
	pruned int,
) (*utxorpcConnectHarness, []models.Block) {
	t.Helper()
	h := newUtxorpcConnectHarness(
		t,
		utxorpcHarnessOptions{numBlocks: expiredHistoryHarnessBlocks},
	)
	blocks := loadTestChainBlocks(t, expiredHistoryHarnessBlocks)
	require.Greater(t, len(blocks), pruned)
	pruneHarnessBlock(t, h, blocks[pruned].Slot, blocks[pruned].Hash)
	return h, blocks
}

func pruneHarnessBlock(
	t *testing.T,
	h *utxorpcConnectHarness,
	slot uint64,
	hash []byte,
) {
	t.Helper()
	_, err := h.DB.PruneBlock(context.Background(), slot, hash)
	require.NoError(t, err)
}

func expiredHistoryContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestConnect_FetchBlock_PrunedBlockIsFailedPrecondition(t *testing.T) {
	t.Parallel()
	h, blocks := newPrunedHistoryHarness(t, 2)
	cli := syncconnect.NewSyncServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	_, err := cli.FetchBlock(
		expiredHistoryContext(t),
		connect.NewRequest(&sync.FetchBlockRequest{
			Ref: []*sync.BlockRef{
				{Slot: blocks[2].Slot, Hash: blocks[2].Hash},
			},
		}),
	)
	requireHistoryExpiredAt(t, err, blocks[2].Slot)
}

// The start token is retained; the next block is not, so the failure comes
// from the chain iterator rather than from resolving the start point.
func TestConnect_DumpHistory_PrunedBlockIsFailedPrecondition(t *testing.T) {
	t.Parallel()
	h, blocks := newPrunedHistoryHarness(t, 2)
	cli := syncconnect.NewSyncServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	_, err := cli.DumpHistory(
		expiredHistoryContext(t),
		connect.NewRequest(&sync.DumpHistoryRequest{
			StartToken: &sync.BlockRef{
				Slot:   blocks[1].Slot,
				Hash:   blocks[1].Hash,
				Height: blocks[1].Number,
			},
			MaxItems: 5,
		}),
	)
	requireHistoryExpiredAt(t, err, blocks[2].Slot)
}

func TestConnect_FollowTip_PrunedReplayIsFailedPrecondition(t *testing.T) {
	t.Parallel()
	h, blocks := newPrunedHistoryHarness(t, 2)
	cli := syncconnect.NewSyncServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	stream, err := cli.FollowTip(
		expiredHistoryContext(t),
		connect.NewRequest(&sync.FollowTipRequest{
			Intersect: []*sync.BlockRef{
				{Slot: blocks[1].Slot, Hash: blocks[1].Hash},
			},
		}),
	)
	require.NoError(t, err)
	for stream.Receive() {
		_, isApply := stream.Msg().Action.(*sync.FollowTipResponse_Apply)
		require.False(
			t,
			isApply,
			"no block after the intersect is retained",
		)
	}
	requireHistoryExpiredAt(t, stream.Err(), blocks[2].Slot)
}

func TestConnect_WatchTx_PrunedReplayIsFailedPrecondition(t *testing.T) {
	t.Parallel()
	h, blocks := newPrunedHistoryHarness(t, 2)
	cli := watchconnect.NewWatchServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	stream, err := cli.WatchTx(
		expiredHistoryContext(t),
		connect.NewRequest(&watch.WatchTxRequest{
			Intersect: []*watch.BlockRef{
				{Slot: blocks[1].Slot, Hash: blocks[1].Hash},
			},
		}),
	)
	require.NoError(t, err)
	for stream.Receive() {
		_, isApply := stream.Msg().Action.(*watch.WatchTxResponse_Apply)
		require.False(
			t,
			isApply,
			"no block after the intersect is retained",
		)
	}
	requireHistoryExpiredAt(t, stream.Err(), blocks[2].Slot)
}

func TestConnect_ReadTx_PrunedBlockIsFailedPrecondition(t *testing.T) {
	t.Parallel()
	h := newUtxorpcConnectHarness(
		t,
		utxorpcHarnessOptions{numBlocks: expiredHistoryHarnessBlocks},
	)
	require.NotEmpty(t, h.IndexedTxHashes)
	txHash := h.IndexedTxHashes[0]
	rec, err := h.LS.TransactionByHash(context.Background(), txHash)
	require.NoError(t, err)
	require.NotNil(t, rec)
	pruneHarnessBlock(t, h, rec.Slot, rec.BlockHash)
	cli := queryconnect.NewQueryServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	_, err = cli.ReadTx(
		expiredHistoryContext(t),
		connect.NewRequest(&query.ReadTxRequest{Hash: txHash}),
	)
	requireHistoryExpiredAt(t, err, rec.Slot)
}

// requireHistoryExpiredAt also pins the reported point, so a failure on a
// retained neighbour (such as the start point) cannot satisfy the check.
func requireHistoryExpiredAt(t *testing.T, err error, slot uint64) {
	t.Helper()
	requireHistoryExpired(t, err)
	require.ErrorContains(t, err, fmt.Sprintf("block at slot=%d hash=", slot))
}

// The pruner leaves its cursor on the last block it expired. The error names
// the first block that still reads back, not the cursor slot.
func TestConnect_FetchBlock_NamesOldestRetainedSlot(t *testing.T) {
	t.Parallel()
	h, blocks := newPrunedHistoryHarness(t, 2)
	pruneHarnessBlock(t, h, blocks[0].Slot, blocks[0].Hash)
	pruneHarnessBlock(t, h, blocks[1].Slot, blocks[1].Hash)
	require.NoError(t, h.DB.SetSyncState(
		database.HistoryExpiryCursorSyncKey,
		strconv.FormatUint(blocks[2].Slot, 10),
		nil,
	))
	cli := syncconnect.NewSyncServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	fetch := func() error {
		_, err := cli.FetchBlock(
			expiredHistoryContext(t),
			connect.NewRequest(&sync.FetchBlockRequest{
				Ref: []*sync.BlockRef{
					{Slot: blocks[0].Slot, Hash: blocks[0].Hash},
				},
			}),
		)
		return err
	}
	// Until the pruner has computed the bound, the error omits it.
	err := fetch()
	requireHistoryExpiredAt(t, err, blocks[0].Slot)
	require.NotContains(t, err.Error(), "oldest retained")

	require.NoError(t, h.DB.RefreshOldestRetainedSlot(t.Context()))
	err = fetch()
	requireHistoryExpiredAt(t, err, blocks[0].Slot)
	require.ErrorContains(
		t,
		err,
		fmt.Sprintf("oldest retained slot=%d", blocks[3].Slot),
	)
}

// The request path reads the bound the pruner computed and never scans for
// it: with the cursor made unreadable after the refresh, a scan would fail
// and drop the bound, but the error still names it.
func TestConnect_FetchBlock_BoundNotScannedPerRequest(t *testing.T) {
	t.Parallel()
	h, blocks := newPrunedHistoryHarness(t, 2)
	pruneHarnessBlock(t, h, blocks[0].Slot, blocks[0].Hash)
	pruneHarnessBlock(t, h, blocks[1].Slot, blocks[1].Hash)
	require.NoError(t, h.DB.SetSyncState(
		database.HistoryExpiryCursorSyncKey,
		strconv.FormatUint(blocks[2].Slot, 10),
		nil,
	))
	require.NoError(t, h.DB.RefreshOldestRetainedSlot(t.Context()))
	require.NoError(t, h.DB.SetSyncState(
		database.HistoryExpiryCursorSyncKey,
		"not a slot",
		nil,
	))
	cli := syncconnect.NewSyncServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	for range 3 {
		_, err := cli.FetchBlock(
			expiredHistoryContext(t),
			connect.NewRequest(&sync.FetchBlockRequest{
				Ref: []*sync.BlockRef{
					{Slot: blocks[0].Slot, Hash: blocks[0].Hash},
				},
			}),
		)
		requireHistoryExpiredAt(t, err, blocks[0].Slot)
		require.ErrorContains(
			t,
			err,
			fmt.Sprintf("oldest retained slot=%d", blocks[3].Slot),
		)
	}
}
