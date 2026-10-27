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

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
)

// Historical WatchTx replay needs one of the shared replay slots, as FollowTip
// does; watching at the tip does not.
func TestConnect_WatchTxReplayRefusedWhenReplaySlotsHeld(t *testing.T) {
	t.Parallel()

	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks: followTipReplayBlocks,
		tune:      func(cfg *UtxorpcConfig) { cfg.MaxConcurrentReplays = 1 },
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	blocks := loadTestChainBlocks(t, followTipReplayBlocks)

	// Another client holds the only replay slot.
	release, err := h.U.replays.acquireSlot("192.0.2.1:1")
	require.NoError(t, err)

	// Historical replay needs a slot of its own.
	stream := startWatchTxAt(t, ctx, h, blocks[0])
	require.False(t, stream.Receive())
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(stream.Err()))

	// Watching from one block behind the tip serves only the tip block,
	// which is live and takes no replay slot.
	live := startWatchTxAt(t, ctx, h, blocks[len(blocks)-2])
	require.True(t, live.Receive(), "live watch refused: %v", live.Err())

	// With the slot free the replay is served.
	release()
	stream = startWatchTxAt(t, ctx, h, blocks[0])
	require.True(t, stream.Receive(), "replay refused: %v", stream.Err())
}

// WatchTx replay spends the per-client block budget shared with FollowTip, and
// a replay that exhausts it ends with ResourceExhausted.
func TestConnect_WatchTxReplayStopsAtClientBudget(t *testing.T) {
	t.Parallel()

	const budget = 5
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks: followTipReplayBlocks,
		tune: func(cfg *UtxorpcConfig) {
			cfg.MaxReplayBlocksPerClient = budget
		},
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	blocks := loadTestChainBlocks(t, followTipReplayBlocks)

	// A block yields at least one message, so a replay that spends the
	// budget ends well short of the chain's last block.
	stream := startWatchTxAt(t, ctx, h, blocks[0])
	received := 0
	for stream.Receive() {
		received++
	}
	require.GreaterOrEqual(t, received, budget)
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(stream.Err()))

	// The budget belongs to the client, so a new replay is refused outright.
	again := startWatchTxAt(t, ctx, h, blocks[0])
	require.False(t, again.Receive())
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(again.Err()))

	// Live watching spends none of it.
	live := startWatchTxAt(t, ctx, h, blocks[len(blocks)-2])
	require.True(t, live.Receive(), "live watch refused: %v", live.Err())
}
