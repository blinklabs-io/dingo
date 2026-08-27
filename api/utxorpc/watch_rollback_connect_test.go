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
	"bytes"
	"context"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database/models"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
	watch "github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/watch/watchconnect"
)

const watchTxRollbackPanicChild = "DINGO_WATCHTX_ROLLBACK_PANIC_CHILD"

type watchTxLedgerStateProbe struct {
	UtxorpcLedgerState
	blockByHash func([]byte) (models.Block, error)
}

func (p *watchTxLedgerStateProbe) BlockByHash(
	hash []byte,
) (models.Block, error) {
	return p.blockByHash(hash)
}

func findEmptyFixtureRun(
	t *testing.T,
	blocks []models.Block,
	runLength int,
) int {
	t.Helper()
	consecutive := 0
	for idx := 2; idx < len(blocks); idx++ {
		block, err := gledger.NewBlockFromCbor(
			blocks[idx].Type,
			blocks[idx].Cbor,
		)
		require.NoError(t, err)
		if len(block.Transactions()) == 0 {
			consecutive++
			if consecutive == runLength {
				return idx - runLength + 1
			}
		} else {
			consecutive = 0
		}
	}
	t.Fatalf("fixture has no run of %d empty blocks", runLength)
	return 0
}

func startWatchTxAt(
	t *testing.T,
	ctx context.Context,
	h *utxorpcConnectHarness,
	intersect models.Block,
) *connect.ServerStreamForClient[watch.WatchTxResponse] {
	t.Helper()
	client := watchconnect.NewWatchServiceClient(
		h.Client,
		h.Server.URL,
		connect.WithGRPC(),
	)
	stream, err := client.WatchTx(
		ctx,
		connect.NewRequest(&watch.WatchTxRequest{
			Intersect: []*watch.BlockRef{{
				Slot:   intersect.Slot,
				Hash:   append([]byte(nil), intersect.Hash...),
				Height: intersect.Number,
			}},
		}),
	)
	require.NoError(t, err)
	return stream
}

func requireWatchTxIdle(
	t *testing.T,
	stream *connect.ServerStreamForClient[watch.WatchTxResponse],
) {
	t.Helper()
	if !stream.Receive() {
		t.Fatalf("WatchTx stream ended: %v", stream.Err())
	}
	_, ok := stream.Msg().Action.(*watch.WatchTxResponse_Idle)
	require.True(t, ok, "expected Idle, got %T", stream.Msg().Action)
}

func TestConnect_WatchTx_InHistoryRollbackSkipsPersistedFetch(t *testing.T) {
	scan := loadTestChainBlocksWithPeriodicTransactions(t, 80)
	start := findEmptyFixtureRun(t, scan, 2)
	blocks := scan[:start+2]
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks: len(blocks),
	})

	var blockReads atomic.Int32
	h.U.config.LedgerState = &watchTxLedgerStateProbe{
		UtxorpcLedgerState: h.LS,
		blockByHash: func(hash []byte) (models.Block, error) {
			blockReads.Add(1)
			return h.LS.BlockByHash(hash)
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream := startWatchTxAt(t, ctx, h, blocks[start-1])
	requireWatchTxIdle(t, stream)
	requireWatchTxIdle(t, stream)

	rollbackPoint := ocommon.NewPoint(
		blocks[start].Slot,
		blocks[start].Hash,
	)
	require.NoError(t, h.LS.Chain().Rollback(rollbackPoint))
	readded, err := gledger.NewBlockFromCbor(
		blocks[start+1].Type,
		blocks[start+1].Cbor,
	)
	require.NoError(t, err)
	require.NoError(t, h.LS.Chain().AddBlock(readded, nil))
	requireWatchTxIdle(t, stream)

	require.Never(
		t,
		func() bool { return blockReads.Load() != 0 },
		200*time.Millisecond,
		5*time.Millisecond,
		"an in-history rollback must not read persisted blocks",
	)
}

func requireFixtureWatchTxAppliedCount(t *testing.T, block models.Block) int {
	t.Helper()
	applied, _, err := watchTxBuildForwardMessages(
		block.Type,
		block.Cbor,
		block.Slot,
		block.Number,
		block.Hash,
		func(gledger.Transaction) bool { return true },
	)
	require.NoError(t, err)
	require.NotEmpty(t, applied)
	return len(applied)
}

func requireWatchTxUndos(
	t *testing.T,
	stream *connect.ServerStreamForClient[watch.WatchTxResponse],
	count int,
) {
	t.Helper()
	for range count {
		require.True(t, stream.Receive(), "WatchTx stream ended: %v", stream.Err())
		_, ok := stream.Msg().Action.(*watch.WatchTxResponse_Undo)
		require.True(t, ok, "expected Undo, got %T", stream.Msg().Action)
	}
}

func TestConnect_WatchTx_SequentialDeepRollbacksRetainCursor(t *testing.T) {
	scan := loadTestChainBlocksWithPeriodicTransactions(t, 80)
	start := findEmptyFixtureRun(t, scan, 4)
	require.GreaterOrEqual(t, start, 3)
	blocks := scan[:start+1]
	txPayload := scan[start-1]
	undoCount := requireFixtureWatchTxAppliedCount(t, txPayload)
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks: len(blocks),
	})

	// Reuse one known-convertible transaction payload for two persisted block
	// identities. The wrapper preserves each requested block's hash, previous
	// hash, slot, and number so this test isolates rollback traversal state
	// without changing the immutable fixture or the live chain.
	persistedWithTxPayload := func(block models.Block) models.Block {
		block.Type = txPayload.Type
		block.Cbor = txPayload.Cbor
		return block
	}
	h.U.config.LedgerState = &watchTxLedgerStateProbe{
		UtxorpcLedgerState: h.LS,
		blockByHash: func(hash []byte) (models.Block, error) {
			switch {
			case bytes.Equal(hash, blocks[start-1].Hash):
				return persistedWithTxPayload(blocks[start-1]), nil
			case bytes.Equal(hash, blocks[start-2].Hash):
				return persistedWithTxPayload(blocks[start-2]), nil
			default:
				return h.LS.BlockByHash(hash)
			}
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream := startWatchTxAt(t, ctx, h, blocks[start-1])
	requireWatchTxIdle(t, stream)

	require.NoError(t, h.LS.Chain().Rollback(ocommon.NewPoint(
		blocks[start-2].Slot,
		blocks[start-2].Hash,
	)))
	requireWatchTxUndos(t, stream, undoCount)

	require.NoError(t, h.LS.Chain().Rollback(ocommon.NewPoint(
		blocks[start-3].Slot,
		blocks[start-3].Hash,
	)))
	readded, err := gledger.NewBlockFromCbor(
		blocks[start-2].Type,
		blocks[start-2].Cbor,
	)
	require.NoError(t, err)
	require.NoError(t, h.LS.Chain().AddBlock(readded, nil))
	requireWatchTxUndos(t, stream, undoCount)
	requireWatchTxIdle(t, stream)
}

func TestConnect_WatchTx_RollbackPanicBecomesStreamError(t *testing.T) {
	if os.Getenv(watchTxRollbackPanicChild) == "1" {
		runWatchTxRollbackPanicChild(t)
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	cmd.Env = append(os.Environ(), watchTxRollbackPanicChild+"=1")
	output, err := cmd.CombinedOutput()
	require.NoError(
		t,
		err,
		"WatchTx rollback failure was not returned as a request error:\n%s",
		output,
	)
}

func runWatchTxRollbackPanicChild(t *testing.T) {
	scan := loadTestChainBlocksWithPeriodicTransactions(t, 80)
	childIdx := findEmptyFixtureRun(t, scan, 1)
	blocks := scan[:childIdx+1]
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks: len(blocks),
	})
	h.U.config.LedgerState = &watchTxLedgerStateProbe{
		UtxorpcLedgerState: h.LS,
		blockByHash: func([]byte) (models.Block, error) {
			panic("rollback conversion probe")
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream := startWatchTxAt(t, ctx, h, blocks[childIdx-1])
	requireWatchTxIdle(t, stream)

	rollbackPoint := ocommon.NewPoint(
		blocks[childIdx-2].Slot,
		blocks[childIdx-2].Hash,
	)
	require.NoError(t, h.LS.Chain().Rollback(rollbackPoint))
	require.False(t, stream.Receive())
	require.ErrorContains(
		t,
		stream.Err(),
		"WatchTx rollback block conversion failed",
	)
}
