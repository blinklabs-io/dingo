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

package ledger

import (
	"io"
	"log/slog"
	"net"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	ouroboros "github.com/blinklabs-io/gouroboros"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandleEventBlockfetchBatchDoneAcceptsShadowCompletion verifies that a
// near-tip shadow peer can finish the batch ahead of a slow primary. Without
// this, BatchDone from the shadow connection was dropped and the batch
// stalled until the primary or the timeout fired, defeating the point of
// dispatching a shadow at all.
func TestHandleEventBlockfetchBatchDoneAcceptsShadowCompletion(t *testing.T) {
	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))

	primary := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	shadow := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}

	requestCount := 0
	requestedConnId := ouroboros.ConnectionId{}

	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       primary,
		shadowBlockfetchConnId:       shadow,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		// Skip the empty-batch retry path: pretend the shadow already
		// delivered a block before sending BatchDone.
		batchBlocksReceived: 1,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				_ = start
				_ = end
				requestCount++
				requestedConnId = connId
				return nil
			},
		},
	}

	require.NoError(t, ls.handleEventBlockfetchBatchDone(BlockfetchEvent{
		ConnectionId: shadow,
		BatchDone:    true,
	}))

	// The shadow's BatchDone is accepted, so the batch advances and a
	// follow-up RequestRange is dispatched for the still-queued header.
	assert.Equal(t, 1, requestCount)
	assert.True(t, sameConnectionId(shadow, requestedConnId))
	// Shadow takes over as the active connection for the next batch.
	assert.True(t, sameConnectionId(shadow, ls.activeBlockfetchConnId))
	// Shadow state for the completed batch is cleared.
	assert.Equal(t, ouroboros.ConnectionId{}, ls.shadowBlockfetchConnId)
	assert.Nil(t, ls.shadowBlockReceivedHashes)
	assert.False(t, ls.firstBlockReceived)

	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
}

// TestHandleEventBlockfetchBatchDoneDropsStaleShadowAfterCleanup verifies the
// other half of the shadow path: once the primary has completed the batch
// and cleanup has cleared the shadow connection ID, the shadow's late
// BatchDone must NOT be treated as completing whatever batch happens to be
// active next.
func TestHandleEventBlockfetchBatchDoneDropsStaleShadowAfterCleanup(
	t *testing.T,
) {
	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))

	primary := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	shadow := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}

	requestCount := 0
	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       primary,
		shadowBlockfetchConnId:       shadow,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		batchBlocksReceived:          1,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				_ = connId
				_ = start
				_ = end
				requestCount++
				return nil
			},
		},
	}

	// Primary completes first; cleanup clears the shadow ID and starts the
	// next batch on the same primary.
	require.NoError(t, ls.handleEventBlockfetchBatchDone(BlockfetchEvent{
		ConnectionId: primary,
		BatchDone:    true,
	}))
	assert.Equal(t, 1, requestCount)
	assert.Equal(t, ouroboros.ConnectionId{}, ls.shadowBlockfetchConnId)

	// The shadow's late BatchDone now arrives. It must not complete the new
	// batch — neither active nor shadow matches it after cleanup.
	require.NoError(t, ls.handleEventBlockfetchBatchDone(BlockfetchEvent{
		ConnectionId: shadow,
		BatchDone:    true,
	}))
	assert.Equal(
		t,
		1,
		requestCount,
		"stale shadow BatchDone must not start another batch",
	)

	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
}

// TestStartQueuedBlockfetchAfterForkRestartClearsShadowState verifies that
// the fork-restart path resets shadow per-batch state. Previously
// restartQueuedBlockfetchAfterForkLocked tore down the active batch
// manually and re-entered startQueuedBlockfetchLocked without going through
// blockfetchRequestRangeCleanup, so a stale shadowBlockfetchConnId could
// leak into the new batch and let the previous shadow's blocks be accepted
// against the new request.
func TestStartQueuedBlockfetchAfterForkRestartClearsShadowState(t *testing.T) {
	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))

	primary := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	staleShadow := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}

	ls := &LedgerState{
		chain:                  testChain,
		activeBlockfetchConnId: primary,
		// Per-batch state from a previous batch that the fork restart
		// must wipe before re-entering startQueuedBlockfetchLocked.
		shadowBlockfetchConnId: staleShadow,
		shadowBlockReceivedHashes: map[string]struct{}{
			"prev-batch-hash": {},
		},
		firstBlockReceived:           true,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) error {
				_ = connId
				_ = start
				_ = end
				return nil
			},
		},
	}

	require.NoError(t, ls.restartQueuedBlockfetchAfterForkLocked(primary))

	// Stale per-batch shadow state must be cleared by the restart so that
	// the new batch starts in a known state.
	assert.Equal(t, ouroboros.ConnectionId{}, ls.shadowBlockfetchConnId)
	assert.Nil(t, ls.shadowBlockReceivedHashes)
	assert.False(t, ls.firstBlockReceived)

	// A block delivered on the previous shadow connection must be rejected
	// because it is no longer the shadow for the active batch.
	require.NoError(t, ls.handleEventBlockfetchBlock(BlockfetchEvent{
		ConnectionId: staleShadow,
		Block:        &mockBabbageBlock{slot: 99},
		Point:        ocommon.Point{Slot: 99, Hash: []byte("stale-shadow")},
	}))
	require.Empty(
		t,
		ls.pendingBlockfetchEvents,
		"stale shadow block must not be accepted after fork restart",
	)

	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
}
