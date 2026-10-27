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
	gosync "sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/blinklabs-io/dingo/database/models"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync"
	"github.com/utxorpc/go-codegen/utxorpc/v1alpha/sync/syncconnect"
)

// fakeReplayClock drives a replayLimiter without wall-clock time: now is
// fixed until advance is called and sleep records the requested wait.
type fakeReplayClock struct {
	mu     gosync.Mutex
	t      time.Time
	sleeps []time.Duration
}

func newFakeReplayClock() *fakeReplayClock {
	return &fakeReplayClock{t: time.Unix(1_700_000_000, 0)}
}

func (c *fakeReplayClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeReplayClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func (c *fakeReplayClock) sleep(_ context.Context, d time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sleeps = append(c.sleeps, d)
	return nil
}

func (c *fakeReplayClock) recorded() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.sleeps...)
}

func newTestReplayLimiter(
	clock *fakeReplayClock,
	maxConcurrent, perSecond int,
	budget int64,
	window time.Duration,
) *replayLimiter {
	l := newReplayLimiter(maxConcurrent, 1, perSecond, budget, window)
	l.now = clock.now
	l.sleep = clock.sleep
	return l
}

func TestReplayLimiterSlotCap(t *testing.T) {
	t.Parallel()

	l := newReplayLimiter(2, 1, 1000, 1000, time.Hour)
	rel1, err := l.acquireSlot("10.0.0.1:1")
	require.NoError(t, err)
	_, err = l.acquireSlot("10.0.0.2:1")
	require.NoError(t, err)
	_, err = l.acquireSlot("10.0.0.3:1")
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))

	rel1()
	rel1() // a second release must not free another slot
	_, err = l.acquireSlot("10.0.0.3:1")
	require.NoError(t, err)
	_, err = l.acquireSlot("10.0.0.4:1")
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
}

func TestReplayLimiterSlotCapPerClient(t *testing.T) {
	t.Parallel()

	l := newReplayLimiter(4, 1, 1000, 1000, time.Hour)
	rel, err := l.acquireSlot("10.0.0.1:1")
	require.NoError(t, err)
	// Another port of the same host is the same client.
	_, err = l.acquireSlot("10.0.0.1:2")
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
	_, err = l.acquireSlot("10.0.0.2:1")
	require.NoError(t, err)

	rel()
	_, err = l.acquireSlot("10.0.0.1:2")
	require.NoError(t, err)
}

func TestReplayLimiterPacesBlocksPerClient(t *testing.T) {
	t.Parallel()

	clock := newFakeReplayClock()
	l := newTestReplayLimiter(clock, 1, 2, 1000, time.Hour)
	ctx := t.Context()

	// The burst is one second of blocks.
	require.NoError(t, l.charge(ctx, "10.0.0.1:1"))
	require.NoError(t, l.charge(ctx, "10.0.0.1:2"))
	assert.Empty(t, clock.recorded())

	// Past the burst each block waits for its token, across ports of one host.
	require.NoError(t, l.charge(ctx, "10.0.0.1:3"))
	require.NoError(t, l.charge(ctx, "10.0.0.1:4"))
	assert.Equal(
		t,
		[]time.Duration{500 * time.Millisecond, time.Second},
		clock.recorded(),
	)

	// Another client has its own bucket.
	require.NoError(t, l.charge(ctx, "10.0.0.2:1"))
	assert.Len(t, clock.recorded(), 2)

	// Elapsed time pays the debt back.
	clock.advance(10 * time.Second)
	require.NoError(t, l.charge(ctx, "10.0.0.1:5"))
	assert.Len(t, clock.recorded(), 2)
}

func TestReplayLimiterClientBudgetAndWindow(t *testing.T) {
	t.Parallel()

	clock := newFakeReplayClock()
	l := newTestReplayLimiter(clock, 1, 1_000_000, 3, time.Hour)
	ctx := t.Context()

	for range 3 {
		require.NoError(t, l.charge(ctx, "10.0.0.1:1"))
	}
	err := l.charge(ctx, "10.0.0.1:2")
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))

	// The budget is per client.
	require.NoError(t, l.charge(ctx, "10.0.0.2:1"))

	// It renews once the window has passed.
	clock.advance(time.Hour)
	require.NoError(t, l.charge(ctx, "10.0.0.1:1"))
}

func TestReplayLimiterSweepsIdleClients(t *testing.T) {
	t.Parallel()

	clock := newFakeReplayClock()
	l := newTestReplayLimiter(clock, 1, 1000, 1000, time.Hour)
	require.NoError(t, l.charge(t.Context(), "10.0.0.1:1"))
	require.NoError(t, l.charge(t.Context(), "10.0.0.2:1"))
	require.Len(t, l.clients, 2)

	clock.advance(2 * time.Hour)
	require.NoError(t, l.charge(t.Context(), "10.0.0.3:1"))
	l.mu.Lock()
	defer l.mu.Unlock()
	assert.Len(t, l.clients, 1)
}

func followTipFrom(
	t *testing.T,
	ctx context.Context,
	h *utxorpcConnectHarness,
	intersect models.Block,
) *connect.ServerStreamForClient[sync.FollowTipResponse] {
	t.Helper()
	client := syncconnect.NewSyncServiceClient(
		h.Client, h.Server.URL, connect.WithGRPC(),
	)
	stream, err := client.FollowTip(
		ctx,
		connect.NewRequest(&sync.FollowTipRequest{
			Intersect: []*sync.BlockRef{{
				Slot:   intersect.Slot,
				Hash:   append([]byte(nil), intersect.Hash...),
				Height: intersect.Number,
			}},
		}),
	)
	require.NoError(t, err)
	return stream
}

const followTipReplayBlocks = 25

func TestConnect_FollowTipReplayRefusedWhenReplaySlotsHeld(t *testing.T) {
	t.Parallel()

	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks: followTipReplayBlocks,
		tune:      func(cfg *UtxorpcConfig) { cfg.MaxConcurrentReplays = 1 },
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	blocks := loadTestChainBlocks(t, followTipReplayBlocks)

	release, err := h.U.replays.acquireSlot("192.0.2.1:1")
	require.NoError(t, err)

	// Historical replay needs a slot of its own.
	stream := followTipFrom(t, ctx, h, blocks[0])
	require.False(t, stream.Receive())
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(stream.Err()))

	// Following from one block behind the tip serves only the tip block,
	// which is live and takes no replay slot.
	live := followTipFrom(t, ctx, h, blocks[len(blocks)-2])
	require.True(t, live.Receive(), "live follow refused: %v", live.Err())

	// With the slot free the full replay is served.
	release()
	stream = followTipFrom(t, ctx, h, blocks[0])
	for i := 1; i < len(blocks); i++ {
		require.True(t, stream.Receive(), "block %d: %v", i, stream.Err())
	}
}

func TestConnect_FollowTipReplayStopsAtClientBudget(t *testing.T) {
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

	stream := followTipFrom(t, ctx, h, blocks[0])
	for i := range budget {
		require.True(t, stream.Receive(), "block %d: %v", i, stream.Err())
	}
	require.False(t, stream.Receive())
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(stream.Err()))

	// The budget belongs to the client, so a new stream is refused outright.
	again := followTipFrom(t, ctx, h, blocks[0])
	require.False(t, again.Receive())
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(again.Err()))

	// Live following spends none of it.
	live := followTipFrom(t, ctx, h, blocks[len(blocks)-2])
	require.True(t, live.Receive(), "live follow refused: %v", live.Err())
}

func TestConnect_FollowTipReplayIsPaced(t *testing.T) {
	t.Parallel()

	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks: followTipReplayBlocks,
		tune: func(cfg *UtxorpcConfig) {
			cfg.ReplayBlocksPerSecond = 2
		},
	})
	clock := newFakeReplayClock()
	h.U.replays.now = clock.now
	h.U.replays.sleep = clock.sleep
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	blocks := loadTestChainBlocks(t, followTipReplayBlocks)

	stream := followTipFrom(t, ctx, h, blocks[0])
	for i := 1; i < len(blocks); i++ {
		require.True(t, stream.Receive(), "block %d: %v", i, stream.Err())
	}

	// Blocks 1..23 are behind the tip and charged; block 24 is the tip.
	// The first two ride the burst, the remaining 21 each wait.
	sleeps := clock.recorded()
	require.Len(t, sleeps, followTipReplayBlocks-4)
	assert.Equal(t, 500*time.Millisecond, sleeps[0])
}

// scriptedTipLedger reports a primary chain tip chosen per call, so a test can
// move the tip between the blocks FollowTip serves. The ledger tip stays the
// real one.
type scriptedTipLedger struct {
	UtxorpcLedgerState
	mu       gosync.Mutex
	calls    int
	chainTip func(call int) ochainsync.Tip
}

func (l *scriptedTipLedger) PrimaryChainTip() ochainsync.Tip {
	l.mu.Lock()
	l.calls++
	call := l.calls
	l.mu.Unlock()
	return l.chainTip(call)
}

func tipOf(b models.Block) ochainsync.Tip {
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(b.Slot, b.Hash),
		BlockNumber: b.Number,
	}
}

func TestConnect_FollowTipReplayOneSlotPerClient(t *testing.T) {
	t.Parallel()

	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks: followTipReplayBlocks,
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	blocks := loadTestChainBlocks(t, followTipReplayBlocks)

	// The harness client connects from loopback; another stream of the same
	// host already holds a replay slot.
	release, err := h.U.replays.acquireSlot("127.0.0.1:1")
	require.NoError(t, err)
	stream := followTipFrom(t, ctx, h, blocks[0])
	require.False(t, stream.Receive())
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(stream.Err()))

	release()
	stream = followTipFrom(t, ctx, h, blocks[0])
	require.True(t, stream.Receive(), "replay refused: %v", stream.Err())
}

func TestConnect_FollowTipCaughtUpStreamNeedsNoReplaySlot(t *testing.T) {
	t.Parallel()

	const caughtUpAt = 5
	blocks := loadTestChainBlocks(t, followTipReplayBlocks)
	var h *utxorpcConnectHarness
	var holdOnce gosync.Once
	h = newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks: followTipReplayBlocks,
		tune: func(cfg *UtxorpcConfig) {
			cfg.MaxConcurrentReplays = 1
			// Blocks 1..4 are behind a tip at block 5, which the stream
			// then reaches. From block 6 the tip has moved to the last
			// block, as after a fork switch, and another client holds the
			// only replay slot.
			cfg.LedgerState = &scriptedTipLedger{
				UtxorpcLedgerState: cfg.LedgerState,
				chainTip: func(call int) ochainsync.Tip {
					if call <= caughtUpAt {
						return tipOf(blocks[caughtUpAt])
					}
					holdOnce.Do(func() {
						_, err := h.U.replays.acquireSlot("192.0.2.1:1")
						assert.NoError(t, err)
					})
					return tipOf(blocks[len(blocks)-1])
				},
			}
		},
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	stream := followTipFrom(t, ctx, h, blocks[0])
	for i := 1; i < len(blocks); i++ {
		require.True(t, stream.Receive(), "block %d: %v", i, stream.Err())
	}
}

func TestConnect_FollowTipChargesBlocksAheadOfLedgerTip(t *testing.T) {
	t.Parallel()

	const budget = 5
	blocks := loadTestChainBlocks(t, followTipReplayBlocks)
	h := newUtxorpcConnectHarness(t, utxorpcHarnessOptions{
		numBlocks: followTipReplayBlocks,
		tune: func(cfg *UtxorpcConfig) {
			cfg.MaxReplayBlocksPerClient = budget
			// The ledger has applied only the first block while the chain
			// holds them all, as while the ledger catches up.
			cfg.LedgerState = &laggingLedger{
				UtxorpcLedgerState: cfg.LedgerState,
				tip:                tipOf(blocks[0]),
			}
		},
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	stream := followTipFrom(t, ctx, h, blocks[0])
	for i := range budget {
		require.True(t, stream.Receive(), "block %d: %v", i, stream.Err())
	}
	require.False(t, stream.Receive())
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(stream.Err()))
}

// laggingLedger reports a ledger tip behind the primary chain tip.
type laggingLedger struct {
	UtxorpcLedgerState
	tip ochainsync.Tip
}

func (l *laggingLedger) Tip() ochainsync.Tip { return l.tip }
