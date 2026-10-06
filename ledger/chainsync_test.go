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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"log/slog"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/consensus/praos"
	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/test/dbtest"
	testfixtures "github.com/blinklabs-io/dingo/internal/test/fixtures"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/forging"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/cbor"
	byronconsensus "github.com/blinklabs-io/gouroboros/consensus/byron"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/mary"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/blinklabs-io/gouroboros/pipeline"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/blinklabs-io/ouroboros-mock/fixtures"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	utxorpc "github.com/utxorpc/go-codegen/utxorpc/v1alpha/cardano"
)

// bodySizedMockHeader is a Shelley-and-later header that reports a body size,
// so a queue of them has a known, large estimated wire size.
type bodySizedMockHeader struct {
	mockHeader
	bodySize uint64
}

func (h bodySizedMockHeader) BlockBodySize() uint64 { return h.bodySize }

func buildBodySizedChain(
	t *testing.T,
	headerCount int,
	bodySize uint64,
) *chain.Chain {
	t.Helper()
	testChain := &chain.Chain{}
	prevHash := lcommon.NewBlake2b256(nil)
	for i := range headerCount {
		hash := lcommon.NewBlake2b256(
			testHashBytes(fmt.Sprintf("range-bytes-hdr-%d", i)),
		)
		require.NoError(t, testChain.AddBlockHeader(bodySizedMockHeader{
			mockHeader: mockHeader{
				hash:        hash,
				prevHash:    prevHash,
				blockNumber: uint64(i + 1),
				slot:        uint64(i + 1),
			},
			bodySize: bodySize,
		}))
		prevHash = hash
	}
	return testChain
}

func TestStartQueuedBlockfetchCutsRangesByEstimatedBytes(t *testing.T) {
	t.Parallel()

	// 1 MiB blocks: a whole 30-header queue is far under the 500-block
	// count cap but 30 MiB in bytes.
	const bodySize = 1 << 20
	testChain := buildBodySizedChain(t, 30, bodySize)
	connId := testChainsyncConnId(6400, 3001)

	var requests []deepCatchupRequest
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				gotConnId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				requests = append(requests, deepCatchupRequest{
					connId: gotConnId,
					start:  start,
					end:    end,
				})
				return uint64(len(requests)), nil
			},
		},
	}
	ls.publishSnapshotsLocked()

	ls.chainsyncBlockfetchMutex.Lock()
	err := ls.startQueuedBlockfetchLocked(connId, nil)
	ls.chainsyncBlockfetchMutex.Unlock()
	require.NoError(t, err)

	require.Len(t, requests, 2)
	active, prefetch := requests[0], requests[1]
	assert.Equal(t, uint64(1), active.start.Slot)
	assert.Equal(
		t,
		uint64(7),
		active.end.Slot,
		"seven 1 MiB blocks fit in BlockfetchMaxRangeBytes; the eighth does not",
	)
	assert.Equal(t, uint64(8), prefetch.start.Slot)
	assert.Equal(t, uint64(14), prefetch.end.Slot)

	got := ls.BlockfetchRangeExpectedBytes(active.start, active.end)
	assert.Greater(t, got, uint64(7*bodySize))
	assert.LessOrEqual(t, got, uint64(BlockfetchMaxRangeBytes))

	ls.chainsyncBlockfetchMutex.Lock()
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

func TestBlockfetchRangeExpectedBytesNoEstimate(t *testing.T) {
	t.Parallel()

	t.Run("nil chain", func(t *testing.T) {
		t.Parallel()
		ls := &LedgerState{}
		assert.Zero(t, ls.BlockfetchRangeExpectedBytes(
			ocommon.Point{Slot: 1, Hash: []byte("a")},
			ocommon.Point{Slot: 2, Hash: []byte("b")},
		))
	})

	t.Run("header without a body size", func(t *testing.T) {
		t.Parallel()
		testChain, _ := buildDeepCatchupChain(t, 3)
		ls := &LedgerState{chain: testChain}
		start, end := testChain.HeaderRange(3)
		assert.Zero(t, ls.BlockfetchRangeExpectedBytes(start, end))
	})

	t.Run("range not in the queue", func(t *testing.T) {
		t.Parallel()
		testChain := buildBodySizedChain(t, 3, 1<<10)
		ls := &LedgerState{chain: testChain}
		start, end := testChain.HeaderRange(3)
		require.NotZero(t, ls.BlockfetchRangeExpectedBytes(start, end))
		end.Hash = []byte("unknown")
		assert.Zero(t, ls.BlockfetchRangeExpectedBytes(start, end))
	})
}

// errBlockfetchNoBlocks is a generic synchronous dispatch failure used by
// fixtures that need BlockfetchRequestRangeFunc to fail before a request is
// ever sent (RequestRange can still fail synchronously that way -- a
// connection lookup failure, for instance). It is not a NoBlocks-shaped
// error: since gouroboros' RequestRange replaced GetBlockRange, a genuine
// MsgNoBlocks reply resolves asynchronously through blockfetchClientRangeDone
// instead, delivered as a BlockfetchEvent with BatchDone set and RangeErr
// wrapping blockfetch.ErrNoBlocks -- see deliverNoBlocksBatchDoneForTest.
var errBlockfetchNoBlocks = errors.New(
	"request block range: block(s) not found",
)

// The production helpers named *Locked require their caller to own the
// blockfetch mutex. Keep direct unit-test calls honest now that the helper
// briefly releases that mutex around the external request callback.
func startQueuedBlockfetchForTest(
	ls *LedgerState,
	connId ouroboros.ConnectionId,
	pending *pendingPublishes,
) error {
	ls.chainsyncBlockfetchMutex.Lock()
	err := ls.startQueuedBlockfetchLocked(connId, pending)
	// The production callback returns after BatchDone has been emitted. This
	// helper's synthetic callback returns without emitting an event, so model
	// that protocol completion explicitly for callers that need to reuse the
	// connection.
	if err == nil {
		ls.completeBlockfetchRequestLocked(connId)
	}
	ls.chainsyncBlockfetchMutex.Unlock()
	return err
}

func startQueuedBlockfetchWithWaitSignalForTest(
	ls *LedgerState,
	connId ouroboros.ConnectionId,
	pending *pendingPublishes,
	waitStarted chan<- struct{},
) error {
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	return ls.startQueuedBlockfetchLockedWithWaitSignal(
		connId,
		pending,
		waitStarted,
	)
}

func restartQueuedBlockfetchAfterForkForTest(
	ls *LedgerState,
	connId ouroboros.ConnectionId,
	pending *pendingPublishes,
) error {
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	return ls.restartQueuedBlockfetchAfterForkLocked(connId, pending)
}

func handleEventBlockfetchBatchDoneForTest(
	ls *LedgerState,
	e BlockfetchEvent,
	pending *pendingPublishes,
) error {
	ls.chainsyncBlockfetchMutex.Lock()
	err := ls.handleEventBlockfetchBatchDone(e, pending)
	ls.chainsyncBlockfetchMutex.Unlock()
	ls.blockfetchContinuationMu.Lock()
	ls.blockfetchContinuationWG.Wait()
	ls.blockfetchContinuationMu.Unlock()
	return err
}

func handleBlockfetchTimeoutForTest(
	ls *LedgerState,
	connId ouroboros.ConnectionId,
	pending *pendingPublishes,
) {
	ls.chainsyncBlockfetchMutex.Lock()
	defer ls.chainsyncBlockfetchMutex.Unlock()
	ls.handleBlockfetchTimeoutLocked(connId, pending)
}

// TestStartQueuedBlockfetchReleasesMutexAroundRequest models the receive-side
// half of the production deadlock. The real blockfetch callback publishes a
// ledger.blockfetch event, whose subscriber needs chainsyncBlockfetchMutex;
// the request callback must therefore be able to run while that mutex is
// available even when the caller started the batch under the lock.
func TestStartQueuedBlockfetchReleasesMutexAroundRequest(t *testing.T) {
	t.Parallel()

	ls, _, _ := newNoBlocksLedgerState(t, "hdr-lock-cycle")
	defer ls.config.EventBus.Stop()
	ls.config.BlockfetchRequestRangeFunc = func(
		_ ouroboros.ConnectionId,
		_ ocommon.Point,
		_ ocommon.Point,
	) (uint64, error) {
		acquired := make(chan struct{})
		go func() {
			ls.chainsyncBlockfetchMutex.Lock()
			close(acquired)
			ls.chainsyncBlockfetchMutex.Unlock()
		}()
		select {
		case <-acquired:
			return 0, nil
		case <-time.After(time.Second):
			return 0, errors.New(
				"blockfetch request ran while blockfetch mutex was held",
			)
		}
	}

	connId := testChainsyncConnId(6111, 3001)
	ls.chainsyncBlockfetchMutex.Lock()
	err := ls.startQueuedBlockfetchLocked(connId, nil)
	ls.chainsyncBlockfetchMutex.Unlock()
	require.NoError(t, err)

	ls.chainsyncBlockfetchMutex.Lock()
	assert.Contains(
		t,
		ls.blockfetchRequestsInFlight,
		connIdKey(connId),
		"a completed protocol request stays in flight until BatchDone is handled",
	)
	ls.completeBlockfetchRequestLocked(connId)
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

// TestWaitForBlockfetchRequestLockedWithSignalWaitsForBothPipelinedRequests
// pins the blockfetchRequestsInFlight generalization from a single channel to
// a slice: with pipelining, two requests can be outstanding on
// the same connection at once (the active batch and one pre-queued "next"
// request), and gouroboros resolves them strictly FIFO, so the wait must
// drain both, in order, not return after only the first.
func TestWaitForBlockfetchRequestLockedWithSignalWaitsForBothPipelinedRequests(
	t *testing.T,
) {
	t.Parallel()

	connId := testChainsyncConnId(6116, 3001)
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.chainsyncBlockfetchMutex.Lock()
	ls.beginBlockfetchRequestLocked(connId)
	ls.beginBlockfetchRequestLocked(connId)
	ls.chainsyncBlockfetchMutex.Unlock()

	waitDone := make(chan error, 1)
	go func() {
		ls.chainsyncBlockfetchMutex.Lock()
		defer ls.chainsyncBlockfetchMutex.Unlock()
		waitDone <- ls.waitForBlockfetchRequestLockedWithSignal(connId, nil)
	}()

	testutil.RequireNoReceive(
		t,
		waitDone,
		50*time.Millisecond,
		"wait must not return while either pipelined request is still outstanding",
	)

	ls.chainsyncBlockfetchMutex.Lock()
	ls.completeBlockfetchRequestLocked(connId) // ends the first (FIFO front)
	ls.chainsyncBlockfetchMutex.Unlock()

	testutil.RequireNoReceive(
		t,
		waitDone,
		50*time.Millisecond,
		"wait must not return after only the first of two pipelined requests drains",
	)

	ls.chainsyncBlockfetchMutex.Lock()
	ls.completeBlockfetchRequestLocked(connId) // ends the second
	ls.chainsyncBlockfetchMutex.Unlock()

	testutil.RequireReceive(
		t,
		waitDone,
		testutil.AsyncWait,
		"wait must return once both pipelined requests have drained",
	)
}

// TestStartQueuedBlockfetchCancelsPriorRequestWaitDuringShutdown verifies
// that a chainsync subscriber does not remain in the prior-request drain
// while the node is shutting down. EventBus.Close waits for an in-flight
// subscriber callback, so ignoring the ledger context here can leave node
// shutdown blocked while the blockfetch connection is being torn down.
func TestStartQueuedBlockfetchCancelsPriorRequestWaitDuringShutdown(
	t *testing.T,
) {
	t.Parallel()

	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("shutdown-header")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))
	connId := testChainsyncConnId(6114, 3001)
	ctx, cancel := context.WithCancel(t.Context())
	ls := &LedgerState{
		chain: testChain,
		ctx:   ctx,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				ouroboros.ConnectionId,
				ocommon.Point,
				ocommon.Point,
			) (uint64, error) {
				t.Fatal(
					"blockfetch request started before shutdown wait was canceled",
				)
				return 0, nil
			},
		},
		blockfetchRequestsInFlight: map[string][]chan struct{}{
			connIdKey(connId): {make(chan struct{})},
		},
	}

	waitStarted := make(chan struct{})
	startDone := make(chan error, 1)
	go func() {
		startDone <- startQueuedBlockfetchWithWaitSignalForTest(
			ls,
			connId,
			nil,
			waitStarted,
		)
	}()
	testutil.RequireReceive(
		t,
		waitStarted,
		testutil.AsyncWait,
		"blockfetch request drain did not start",
	)
	cancel()

	select {
	case err := <-startDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(testutil.AsyncWait):
		t.Fatal("blockfetch request drain ignored shutdown cancellation")
	}
}

// TestStartQueuedBlockfetchDrainsPriorRequestBeforeConnectionReuse verifies
// that a late shadow request cannot share its connection with a later batch.
// Blockfetch events carry only a connection ID, so reusing the connection
// before the prior callback returns would make the old BatchDone ambiguous.
func TestStartQueuedBlockfetchDrainsPriorRequestBeforeConnectionReuse(
	t *testing.T,
) {
	t.Parallel()

	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("reuse-header")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))
	connId := testChainsyncConnId(6113, 3001)
	requestStarted := make(chan struct{})
	waitStarted := make(chan struct{})
	requestDone := make(chan struct{})
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				ouroboros.ConnectionId,
				ocommon.Point,
				ocommon.Point,
			) (uint64, error) {
				close(requestStarted)
				return 0, nil
			},
		},
		blockfetchRequestsInFlight: map[string][]chan struct{}{
			connIdKey(connId): {requestDone},
		},
	}

	startDone := make(chan error, 1)
	go func() {
		startDone <- startQueuedBlockfetchWithWaitSignalForTest(
			ls,
			connId,
			nil,
			waitStarted,
		)
	}()
	select {
	case <-requestStarted:
		t.Fatal("reused connection before prior blockfetch request drained")
	case <-waitStarted:
	}
	testutil.RequireNoReceive(
		t,
		requestStarted,
		50*time.Millisecond,
		"blockfetch request started before prior request drained",
	)

	ls.chainsyncBlockfetchMutex.Lock()
	delete(ls.blockfetchRequestsInFlight, connIdKey(connId))
	close(requestDone)
	ls.chainsyncBlockfetchMutex.Unlock()

	testutil.RequireReceive(
		t,
		requestStarted,
		testutil.AsyncWait,
		"blockfetch request did not start after prior request drained",
	)
	require.NoError(t, <-startDone)

	ls.chainsyncBlockfetchMutex.Lock()
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

// TestBlockfetchBatchDoneDoesNotBlockSubscriberOnContinuation verifies that
// the blockfetch EventBus subscriber does not synchronously enter the next
// GetBlockRange call. GetBlockRange waits for the next BatchDone, so doing so
// from this subscriber deadlocks once the subscriber buffer fills with the
// next batch's blocks.
func TestBlockfetchBatchDoneDoesNotBlockSubscriberOnContinuation(t *testing.T) {
	t.Parallel()

	testChain := &chain.Chain{}
	for blockNumber := uint64(1); blockNumber <= 2; blockNumber++ {
		prevHash := lcommon.NewBlake2b256(nil)
		if blockNumber > 1 {
			prevHash = lcommon.NewBlake2b256([]byte{byte(blockNumber - 1)})
		}
		require.NoError(t, testChain.AddBlockHeader(mockHeader{
			hash:        lcommon.NewBlake2b256([]byte{byte(blockNumber)}),
			prevHash:    prevHash,
			blockNumber: blockNumber,
			slot:        blockNumber,
		}))
	}
	connId := testChainsyncConnId(6112, 3001)
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       connId,
		selectedBlockfetchConnId:     connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		// A batch that made progress: it delivered a block and that block
		// extended the chain, so the continuation runs rather than the
		// unobtained-range recovery.
		batchBlocksReceived: 1,
		batchBlocksApplied:  1,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				ouroboros.ConnectionId,
				ocommon.Point,
				ocommon.Point,
			) (uint64, error) {
				close(requestStarted)
				<-releaseRequest
				return 0, nil
			},
		},
	}

	handlerDone := make(chan struct{})
	go func() {
		ls.handleEventBlockfetch(event.NewEvent(
			BlockfetchEventType,
			BlockfetchEvent{ConnectionId: connId, BatchDone: true},
		))
		close(handlerDone)
	}()
	testutil.RequireReceive(
		t,
		handlerDone,
		testutil.AsyncWait,
		"blockfetch subscriber remained blocked in continuation request",
	)
	testutil.RequireReceive(
		t,
		requestStarted,
		testutil.AsyncWait,
		"continuation request did not start",
	)

	close(releaseRequest)
	ls.blockfetchContinuationMu.Lock()
	ls.blockfetchContinuationWG.Wait()
	ls.blockfetchContinuationMu.Unlock()
	ls.chainsyncBlockfetchMutex.Lock()
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

// TestBlockfetchContinuationRetargetsSelection covers the continuation path's
// two starts. handleEventBlockfetchBatchDone hands this function a connection
// chosen by selectRetryBlockfetchConn, which need not be the current selection,
// and nextBlockfetchConnId prefers the selection when picking the next batch's
// connection. So a selection left behind here sends the following batch back to
// the connection the continuation just moved away from.
func TestBlockfetchContinuationRetargetsSelection(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		// fail reports whether a request on this connection should fail,
		// letting the subtest drive the primary start, its retry, or neither.
		fail func(ouroboros.ConnectionId, ouroboros.ConnectionId) bool
		want func(
			stale, primary, retry ouroboros.ConnectionId,
		) ouroboros.ConnectionId
	}{
		{
			name: "primary start",
			fail: func(ouroboros.ConnectionId, ouroboros.ConnectionId) bool {
				return false
			},
			want: func(
				_, primary, _ ouroboros.ConnectionId,
			) ouroboros.ConnectionId {
				return primary
			},
		},
		{
			// Pins the other half of the contract: a failed attempt must not
			// move the selection. Without this case a regression that
			// retargeted on the attempt would still pass, because the
			// following successful retry puts the expected value back.
			name: "neither the primary nor its retry succeeds",
			fail: func(ouroboros.ConnectionId, ouroboros.ConnectionId) bool {
				return true
			},
			want: func(
				stale, _, _ ouroboros.ConnectionId,
			) ouroboros.ConnectionId {
				return stale
			},
		},
		{
			name: "retry after the primary fails",
			fail: func(connId, primary ouroboros.ConnectionId) bool {
				return sameConnectionId(connId, primary)
			},
			want: func(
				_, _, retry ouroboros.ConnectionId,
			) ouroboros.ConnectionId {
				return retry
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			stale := testChainsyncConnId(6200, 3001)
			primary := testChainsyncConnId(6200, 3002)
			retry := testChainsyncConnId(6200, 3003)

			testChain := &chain.Chain{}
			require.NoError(t, testChain.AddBlockHeader(mockHeader{
				hash:        lcommon.NewBlake2b256([]byte("cont-retarget")),
				prevHash:    lcommon.NewBlake2b256(nil),
				blockNumber: 1,
				slot:        1,
			}))

			ls := &LedgerState{
				chain: testChain,
				// The selection starts on a connection this continuation is
				// moving away from, so neither expected value can be reached
				// without the retarget.
				selectedBlockfetchConnId: stale,
				config: LedgerStateConfig{
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
					GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
						return &retry
					},
					BlockfetchRequestRangeFunc: func(
						connId ouroboros.ConnectionId,
						_ ocommon.Point,
						_ ocommon.Point,
					) (uint64, error) {
						if test.fail(connId, primary) {
							return 0, errors.New("request failed")
						}
						return 0, nil
					},
				},
			}

			ls.chainsyncBlockfetchMutex.Lock()
			ls.startQueuedBlockfetchFromEventLocked(
				primary,
				primary,
				"test continuation",
			)
			ls.chainsyncBlockfetchMutex.Unlock()

			ls.blockfetchContinuationMu.Lock()
			ls.blockfetchContinuationWG.Wait()
			ls.blockfetchContinuationMu.Unlock()

			ls.chainsyncBlockfetchMutex.Lock()
			got := ls.selectedBlockfetchConnId
			ls.blockfetchRequestRangeCleanup()
			ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
			ls.chainsyncBlockfetchMutex.Unlock()

			assert.True(
				t, sameConnectionId(got, test.want(stale, primary, retry)),
				"the selection must name the connection that served the "+
					"continuation, got %s", got.String(),
			)
		})
	}
}

// TestBlockfetchRetargetPreservesConcurrentSelection asserts the retarget does
// not overwrite a newer selection installed while the start was outside the
// mutex. startQueuedBlockfetchLocked releases chainsyncBlockfetchMutex around
// the network request, so a connection switch or close can land in that window;
// its choice is the current one and has to win. The request callback stands in
// for that concurrent writer, since it runs with the mutex released.
func TestBlockfetchRetargetPreservesConcurrentSelection(t *testing.T) {
	t.Parallel()

	stale := testChainsyncConnId(6300, 3001)
	starting := testChainsyncConnId(6300, 3002)
	switched := testChainsyncConnId(6300, 3003)

	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("concurrent-switch")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))

	ls := &LedgerState{
		chain:                    testChain,
		selectedBlockfetchConnId: stale,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.config.BlockfetchRequestRangeFunc = func(
		ouroboros.ConnectionId,
		ocommon.Point,
		ocommon.Point,
	) (uint64, error) {
		// Runs with the mutex released, exactly where a real connection
		// switch would install its own selection.
		ls.selectedBlockfetchConnId = switched
		return 0, nil
	}

	ls.chainsyncBlockfetchMutex.Lock()
	require.NoError(t, ls.startQueuedBlockfetchOnLocked(starting, nil))
	got := ls.selectedBlockfetchConnId
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()

	assert.True(
		t, sameConnectionId(got, switched),
		"a selection installed while the request was outside the mutex must "+
			"survive the retarget, got %s", got.String(),
	)
}

// newNoBlocksLedgerState builds a LedgerState with one queued header and a
// blockfetch dispatch that always succeeds synchronously, the way
// RequestRange does (it fails synchronously only before a request is ever
// sent). It returns the ledger, the dispatch-request counter, and a channel
// of published resync events. Pair it with deliverNoBlocksBatchDoneForTest to
// simulate the peer's NoBlocks reply, which now resolves asynchronously.
func newNoBlocksLedgerState(
	t *testing.T,
	headerLabel string,
) (*LedgerState, *int, chan event.ChainsyncResyncEvent) {
	t.Helper()
	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte(headerLabel)),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))
	require.Equal(t, 1, testChain.HeaderCount())

	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	eventBus := event.NewEventBus(nil, logger)
	resyncChan := make(chan event.ChainsyncResyncEvent, 8)
	eventBus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			if resync, ok := evt.Data.(event.ChainsyncResyncEvent); ok {
				resyncChan <- resync
			}
		},
	)

	requestCount := 0
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger:   logger,
			EventBus: eventBus,
			BlockfetchRequestRangeFunc: func(
				_ ouroboros.ConnectionId,
				_ ocommon.Point,
				_ ocommon.Point,
			) (uint64, error) {
				requestCount++
				return 0, nil
			},
		},
	}
	ls.publishSnapshotsLocked()
	return ls, &requestCount, resyncChan
}

// deliverNoBlocksBatchDoneForTest simulates the peer's NoBlocks resolution
// for connId's active batch. RequestRange (unlike GetBlockRange, which it
// replaced) never returns NoBlocks as a synchronous dispatch error: gouroboros
// resolves it asynchronously through blockfetchClientRangeDone instead,
// delivered here as a BatchDone BlockfetchEvent with RangeErr wrapping
// blockfetch.ErrNoBlocks.
func deliverNoBlocksBatchDoneForTest(
	ls *LedgerState,
	connId ouroboros.ConnectionId,
) error {
	return handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
		ConnectionId: connId,
		BatchDone:    true,
		RangeErr:     blockfetch.ErrNoBlocks,
	}, nil)
}

// TestStartQueuedBlockfetchDropsHeadersAfterRepeatedNoBlocks pins the recovery
// on the path a NoBlocks response actually takes: it arrives asynchronously
// as a BatchDone event carrying RangeErr, not as an error returned
// synchronously from the dispatch. The header queue must still be dropped
// after blockfetchMaxSameRangeFailures consecutive NoBlocks replies, because a
// latched header blocks local forging for as long as it is queued.
func TestStartQueuedBlockfetchDropsHeadersAfterRepeatedNoBlocks(t *testing.T) {
	t.Parallel()

	ls, requestCount, resyncChan := newNoBlocksLedgerState(t, "hdr-no-blocks")
	connId := testChainsyncConnId(6102, 3001)

	// The initial dispatch succeeds synchronously (nothing has failed yet);
	// every later dispatch in this test is the automatic continuation
	// handleEventBlockfetchBatchDone starts after a failure that has not yet
	// reached the drop threshold.
	require.NoError(t, startQueuedBlockfetchForTest(ls, connId, nil))

	const attempts = 25
	require.Greater(
		t,
		attempts,
		blockfetchMaxSameRangeFailures,
		"attempt count must exceed the bound for this test to prove it",
	)
	for range attempts {
		if ls.chain.HeaderCount() == 0 {
			break
		}
		require.NoError(t, deliverNoBlocksBatchDoneForTest(ls, connId))
	}

	assert.LessOrEqual(
		t,
		*requestCount,
		blockfetchMaxSameRangeFailures,
		"a range no peer will serve must stop being requested",
	)
	assert.Equal(
		t,
		0,
		ls.chain.HeaderCount(),
		"the unfetchable queued header must be dropped so locally "+
			"forged blocks are no longer rejected",
	)
	resync := testutil.RequireReceive(
		t,
		resyncChan,
		testutil.AsyncWait,
		"chainsync resync after repeated NoBlocks responses",
	)
	assert.Equal(t, connId, resync.ConnectionId)
}

// TestStartQueuedBlockfetchTransientErrorsDoNotAccumulate verifies that
// request failures which do not establish NoBlocks leave the range-failure
// record untouched. A reconnecting peer can return these errors repeatedly
// for the same queued range while the range remains servable.
func TestStartQueuedBlockfetchTransientErrorsDoNotAccumulate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		err          error
		unconfigured bool
	}{
		{
			name: "transport reset",
			err:  errors.New("connection reset by peer"),
		},
		{
			name: "protocol shutdown",
			err:  errors.New("protocol is shutting down"),
		},
		{
			name: "send queue failure",
			err:  errors.New("failed to enqueue blockfetch request"),
		},
		{
			name:         "wiring error",
			unconfigured: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ls, _, resyncChan := newNoBlocksLedgerState(
				t,
				"hdr-transient-"+test.name,
			)
			if test.unconfigured {
				ls.config.BlockfetchRequestRangeFunc = nil
			} else {
				requestErr := test.err
				ls.config.BlockfetchRequestRangeFunc = func(
					ouroboros.ConnectionId,
					ocommon.Point,
					ocommon.Point,
				) (uint64, error) {
					return 0, requestErr
				}
			}

			connId := testChainsyncConnId(6110, 3001)
			for range blockfetchMaxSameRangeFailures * 3 {
				err := startQueuedBlockfetchForTest(ls, connId, nil)
				require.Error(t, err)
			}

			assert.Equal(t, 1, ls.chain.HeaderCount())
			assert.Equal(
				t,
				0,
				ls.blockfetchRangeFailure.count,
				"transient request errors must not advance the unavailable count",
			)
			testutil.RequireNoReceive(
				t,
				resyncChan,
				100*time.Millisecond,
				"transient request errors must not trigger resync",
			)
		})
	}
}

// TestRestartQueuedBlockfetchAfterForkDropsHeadersOnRepeatedNoBlocks covers
// the specific caller observed wedging on DevNet. After a slot battle,
// tryResolveFork rolls back to the common ancestor, re-queues the winning
// peer's headers and calls restartQueuedBlockfetchAfterForkLocked; if that
// returns an error the fork handler only logs
// "failed to start blockfetch after fork rollback" and reports the fork
// resolved. Nothing else retries, so the queued header stays latched and
// block production stops until an unrelated event clears the queue.
func TestRestartQueuedBlockfetchAfterForkDropsHeadersOnRepeatedNoBlocks(
	t *testing.T,
) {
	t.Parallel()

	ls, requestCount, resyncChan := newNoBlocksLedgerState(
		t,
		"hdr-fork-restart",
	)
	connId := testChainsyncConnId(6103, 3001)

	// The fork-restart path's own special handling (tearing down a stale
	// in-flight batch on the same connection) only matters for this initial
	// dispatch; once the batch is running, its repeated NoBlocks replies are
	// recorded the same way any other dispatch's are, through the BatchDone
	// events delivered below.
	require.NoError(
		t,
		restartQueuedBlockfetchAfterForkForTest(ls, connId, nil),
	)

	const attempts = 25
	for range attempts {
		if ls.chain.HeaderCount() == 0 {
			break
		}
		require.NoError(t, deliverNoBlocksBatchDoneForTest(ls, connId))
	}

	assert.LessOrEqual(
		t,
		*requestCount,
		blockfetchMaxSameRangeFailures,
		"the fork restart must stop re-requesting an unservable range",
	)
	assert.Equal(
		t,
		0,
		ls.chain.HeaderCount(),
		"fork-resolution restart failures must drop the queued header "+
			"so forging is released",
	)
	testutil.RequireReceive(
		t,
		resyncChan,
		testutil.AsyncWait,
		"chainsync resync after repeated fork-restart NoBlocks responses",
	)
}

// TestBlockfetchRangeFailureClearedWhenRangeIsDelivered verifies a peer that
// is merely briefly behind is never punished: once the stuck range's own block
// arrives, its failure record is discarded, so earlier misses cannot combine
// with a later unrelated miss to drop a healthy queue.
func TestBlockfetchRangeFailureClearedWhenRangeIsDelivered(t *testing.T) {
	t.Parallel()

	ls, _, _ := newNoBlocksLedgerState(t, "hdr-delivered")
	connId := testChainsyncConnId(6104, 3001)
	stuckStart, _ := ls.chain.HeaderRange(BlockfetchBatchSize)

	require.NoError(t, startQueuedBlockfetchForTest(ls, connId, nil))
	for range blockfetchMaxSameRangeFailures * 3 {
		require.NoError(t, deliverNoBlocksBatchDoneForTest(ls, connId))
		require.Positive(
			t,
			ls.chain.HeaderCount(),
			"header queue must survive while the range keeps arriving",
		)
		// The block for the stuck range arrived, so the range is
		// fetchable after all and its failure record is stale.
		ls.noteBlockfetchRangeProgress(stuckStart)
	}

	assert.Equal(t, 1, ls.chain.HeaderCount())
}

// TestBlockfetchRangeFailuresAccumulatePerRangeDespiteInterleavedActivity is
// the regression guard for a bound that tripped only by luck in production.
// The failures against one unfetchable range are minutes apart, and between
// them the node fetches normally from other peers and churns the header queue
// on forks, connection switches and header mismatches. A globally scoped
// consecutive counter is reset by all of that, so it fires only when failures
// happen to land back to back: two identical DevNet runs produced 1 recovery
// against 169 wedge events and 9 against 81.
//
// Accounting is therefore keyed to the range start point and survives both
// interleaved deliveries for other ranges and clearQueuedHeaders churn, so
// repeated failures against the *same* unfetchable range still add up.
func TestBlockfetchRangeFailuresAccumulatePerRangeDespiteInterleavedActivity(
	t *testing.T,
) {
	t.Parallel()

	ls, requestCount, resyncChan := newNoBlocksLedgerState(t, "hdr-interleaved")
	connId := testChainsyncConnId(6105, 3001)
	stuckHeader := mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-interleaved")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}
	otherRange := ocommon.NewPoint(
		999,
		lcommon.NewBlake2b256([]byte("some-other-range")).Bytes(),
	)

	require.NoError(t, startQueuedBlockfetchForTest(ls, connId, nil))
	for attempt := 1; attempt <= blockfetchMaxSameRangeFailures; attempt++ {
		require.Equal(
			t,
			1,
			ls.chain.HeaderCount(),
			"stuck header must be queued for attempt %d",
			attempt,
		)
		require.NoError(t, deliverNoBlocksBatchDoneForTest(ls, connId))
		if attempt == blockfetchMaxSameRangeFailures {
			break
		}
		// Between attempts the node keeps working: blocks arrive for
		// other ranges from other peers...
		ls.noteBlockfetchRangeProgress(otherRange)
		// ...and fork, connection-switch and header-mismatch handling
		// repeatedly clears the queue, after which the peer re-offers the
		// same unfetchable header.
		ls.clearQueuedHeaders()
		require.NoError(t, ls.chain.AddBlockHeader(stuckHeader))
	}

	assert.Equal(
		t,
		blockfetchMaxSameRangeFailures,
		*requestCount,
		"each attempt against the stuck range must be counted",
	)
	assert.Equal(
		t,
		0,
		ls.chain.HeaderCount(),
		"repeated failures against the same range must drop the queue "+
			"even when unrelated traffic succeeds in between",
	)
	testutil.RequireReceive(
		t,
		resyncChan,
		testutil.AsyncWait,
		"chainsync resync after repeated same-range failures",
	)
}

// TestBlockfetchRangeFailuresDoNotAccumulateAcrossDifferentRanges is the other
// half of the contract: transient misses spread over different ranges must not
// add up into a queue drop, so a peer that is briefly behind on a few distinct
// points is left alone.
func TestBlockfetchRangeFailuresDoNotAccumulateAcrossDifferentRanges(
	t *testing.T,
) {
	t.Parallel()

	ls, _, resyncChan := newNoBlocksLedgerState(t, "hdr-distinct-0")
	connId := testChainsyncConnId(6106, 3001)

	require.NoError(t, startQueuedBlockfetchForTest(ls, connId, nil))
	for attempt := range blockfetchMaxSameRangeFailures * 3 {
		require.NoError(t, deliverNoBlocksBatchDoneForTest(ls, connId))
		// Each attempt is against a different queued header, as happens
		// when the chain keeps moving and every miss is a one-off.
		ls.clearQueuedHeaders()
		require.NoError(t, ls.chain.AddBlockHeader(mockHeader{
			hash: lcommon.NewBlake2b256(
				[]byte(fmt.Sprintf("hdr-distinct-%d", attempt+1)),
			),
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 1,
			slot:        1,
		}))
	}

	assert.Equal(
		t,
		1,
		ls.chain.HeaderCount(),
		"one-off misses against different ranges must not drop the queue",
	)
	testutil.RequireNoReceive(
		t,
		resyncChan,
		100*time.Millisecond,
		"no resync for transient misses on distinct ranges",
	)
}

// TestHandleEventBlockfetchBatchDoneStopsRepeatingEmptyBatches covers the
// near-tip blockfetch wedge: a peer that answers a queued header's range with
// a batch carrying no blocks (it rolled the block back, so it can no longer
// serve the body) must not be asked for the same range indefinitely.
//
// The queued header is what makes this fatal rather than merely wasteful:
// while a header sits at the head of the queue, chain.AddBlock rejects every
// locally forged block with BlockNotMatchHeaderError ("does not match first
// pending header hash"), so block production stops for as long as the header
// stays latched. After a bounded number of consecutive empty batches the
// pipeline must drop the unfetchable header queue and force a fresh
// intersect instead of re-requesting.
func TestHandleEventBlockfetchBatchDoneStopsRepeatingEmptyBatches(
	t *testing.T,
) {
	t.Parallel()

	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-empty-batch")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))
	require.Equal(t, 1, testChain.HeaderCount())

	connId := testChainsyncConnId(6100, 3001)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	eventBus := event.NewEventBus(nil, logger)
	resyncChan := make(chan event.ChainsyncResyncEvent, 4)
	eventBus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			if resync, ok := evt.Data.(event.ChainsyncResyncEvent); ok {
				resyncChan <- resync
			}
		},
	)

	requestCount := 0
	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger:   logger,
			EventBus: eventBus,
			BlockfetchRequestRangeFunc: func(
				_ ouroboros.ConnectionId,
				_ ocommon.Point,
				_ ocommon.Point,
			) (uint64, error) {
				requestCount++
				return 0, nil
			},
		},
	}
	ls.publishSnapshotsLocked()

	// Every batch completes without delivering a block while the header
	// stays queued. The first attempts retry as before; the streak must be
	// bounded well below the number of attempts made here.
	const emptyBatchAttempts = 25
	require.Greater(
		t,
		emptyBatchAttempts,
		blockfetchMaxSameRangeFailures,
		"attempt count must exceed the bound for this test to prove it",
	)
	for i := range emptyBatchAttempts {
		require.NoError(
			t,
			handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
				ConnectionId: connId,
				BatchDone:    true,
			}, nil),
			"batch done %d", i,
		)
	}

	assert.LessOrEqual(
		t,
		requestCount,
		blockfetchMaxSameRangeFailures,
		"the same unfetchable range must not be re-requested "+
			"once the empty-batch streak hits its bound",
	)
	assert.Equal(
		t,
		0,
		testChain.HeaderCount(),
		"the unfetchable queued header must be dropped so locally "+
			"forged blocks are no longer rejected",
	)
	resync := testutil.RequireReceive(
		t,
		resyncChan,
		testutil.AsyncWait,
		"chainsync resync after repeated empty blockfetch batches",
	)
	assert.Equal(t, connId, resync.ConnectionId)

	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
}

// TestHandleEventBlockfetchBatchDoneEmptyBatchStreakResetsOnProgress verifies
// the streak counter tracks *consecutive* empty batches only: a batch that
// delivers a block clears it, so a peer that occasionally returns an empty
// batch is not eventually punished for it.
func TestHandleEventBlockfetchBatchDoneEmptyBatchStreakResetsOnProgress(
	t *testing.T,
) {
	t.Parallel()

	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-streak-reset")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))

	connId := testChainsyncConnId(6101, 3001)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	requestCount := 0
	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: logger,
			BlockfetchRequestRangeFunc: func(
				_ ouroboros.ConnectionId,
				_ ocommon.Point,
				_ ocommon.Point,
			) (uint64, error) {
				requestCount++
				return 0, nil
			},
		},
	}
	ls.publishSnapshotsLocked()

	// Alternate empty and productive batches well past the bound. The
	// productive batches deliver the queued range itself, so its failure
	// record is discarded each time and the header queue survives.
	queuedStart, _ := testChain.HeaderRange(BlockfetchBatchSize)
	for range blockfetchMaxSameRangeFailures * 3 {
		require.NoError(
			t,
			handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
				ConnectionId: connId,
				BatchDone:    true,
			}, nil),
		)
		// Stand in for a delivered block: handleEventBlockfetchBlock both
		// counts the block and discards that range's failure record.
		ls.batchBlocksReceived = 1
		ls.batchBlocksApplied = 1
		ls.noteBlockfetchRangeProgress(queuedStart)
		require.NoError(
			t,
			handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
				ConnectionId: connId,
				BatchDone:    true,
			}, nil),
		)
	}

	assert.Equal(
		t,
		1,
		testChain.HeaderCount(),
		"queued header must survive when batches keep making progress",
	)
	assert.Positive(t, requestCount)

	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
}

// TestStartQueuedBlockfetchSkipsDispatchWhenCanceledBeforeDispatch covers
// startQueuedBlockfetchLockedWithWaitSignal's own ls.ctx.Err() check, which
// sits between the prior-request drain returning and the blockfetch
// dispatch.
//
// Reaching that check deliberately takes a zero-valued connId. For any real
// connId, waitForBlockfetchRequestLockedWithSignal checks ls.ctx itself
// before its wait loop and returns the cancellation first, so the check
// under test never runs and a test written the obvious way passes with it
// removed. connIdKey returns "" only when both addresses are nil, and the
// drain returns nil on that before consulting ls.ctx -- so this is the one
// path where the check below is what stops the dispatch. Verified by
// reverting it: this test then reaches BlockfetchRequestRangeFunc and fails.
//
// What this does not cover: cancellation after that check but before
// BlockfetchRequestRangeFunc returns. That window cannot be closed at this
// boundary -- the mutex must be released around the external request (see
// the lock-cycle comment in startQueuedBlockfetchLockedWithWaitSignal) and
// BlockfetchRequestRangeFunc takes no context -- so it is not tested here
// rather than being covered by a test that only appears to.
func TestStartQueuedBlockfetchSkipsDispatchWhenCanceledBeforeDispatch(
	t *testing.T,
) {
	t.Parallel()

	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("pre-dispatch-header")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ls := &LedgerState{
		chain: testChain,
		ctx:   ctx,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				ouroboros.ConnectionId,
				ocommon.Point,
				ocommon.Point,
			) (uint64, error) {
				t.Fatal(
					"blockfetch request dispatched after the ledger context was canceled",
				)
				return 0, nil
			},
		},
		blockfetchRequestsInFlight: map[string][]chan struct{}{},
	}

	// Zero connId: connIdKey is "", so the drain returns nil without
	// consulting ls.ctx and the check under test is reached.
	err := startQueuedBlockfetchWithWaitSignalForTest(
		ls,
		ouroboros.ConnectionId{},
		nil,
		nil,
	)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(
		t,
		ouroboros.ConnectionId{},
		ls.activeBlockfetchConnId,
		"a canceled start must not claim the active blockfetch connection",
	)
}

// TestBatchDoneTransportRangeErrDoesNotAccumulate pins the asynchronous half
// of the invariant TestStartQueuedBlockfetchTransientErrorsDoNotAccumulate
// covers synchronously: a failure that does not establish NoBlocks must not
// advance the range-unavailable count. RequestRange resolves every terminal
// outcome through RangeDoneFunc, so a transport, shutdown, or decode failure
// that once returned synchronously from GetBlockRange now arrives here as a
// BatchDone event carrying RangeErr, with no block applied and the headers
// still queued -- the same shape as a genuine NoBlocks. Counting it would
// drop a queued range that is still obtainable and force a chainsync
// re-intersect, which is exactly the false positive
// blockfetchMaxSameRangeFailures documents as out of scope.
func TestBatchDoneTransportRangeErrDoesNotAccumulate(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "transport reset", err: errors.New("connection reset by peer")},
		{
			name: "protocol shutdown",
			err:  errors.New("protocol is shutting down"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ls, _, resyncChan := newNoBlocksLedgerState(
				t,
				"hdr-async-transient-"+test.name,
			)
			connId := testChainsyncConnId(6115, 3001)
			require.NoError(
				t,
				startQueuedBlockfetchForTest(ls, connId, nil),
			)

			for range blockfetchMaxSameRangeFailures * 3 {
				if ls.chain.HeaderCount() == 0 {
					break
				}
				require.NoError(t, handleEventBlockfetchBatchDoneForTest(
					ls,
					BlockfetchEvent{
						ConnectionId: connId,
						BatchDone:    true,
						RangeErr:     test.err,
					},
					nil,
				))
			}

			assert.Equal(
				t,
				1,
				ls.chain.HeaderCount(),
				"an obtainable range must survive a transport failure",
			)
			assert.Equal(
				t,
				0,
				ls.blockfetchRangeFailure.count,
				"transport range errors must not advance the unavailable count",
			)
			testutil.RequireNoReceive(
				t,
				resyncChan,
				100*time.Millisecond,
				"transport range errors must not trigger resync",
			)
		})
	}
}

// TestBatchDoneTransportRangeErrKeepsHeadersAcrossLargeTipGap covers the
// second no-blocks recovery branch, which TestBatchDoneTransportRangeErrDoes
// NotAccumulate cannot reach: that test leaves the upstream tip unset, so the
// branch's tip-gap condition is false throughout. With a gap of at least
// blockfetchMinBatchGapSlots and no alternate connection to retry on, the
// branch clears the queued headers and asks chainsync to re-intersect.
//
// A transport-shaped RangeErr establishes nothing about the queued range --
// it never reached the peer's answer -- so discarding the headers there
// throws away work that the next connection can still fetch, and pays a
// chainsync re-intersect for it. The range must survive instead.
func TestBatchDoneTransportRangeErrKeepsHeadersAcrossLargeTipGap(
	t *testing.T,
) {
	t.Parallel()

	ls, _, resyncChan := newNoBlocksLedgerState(t, "hdr-async-gap-transient")
	connId := testChainsyncConnId(6116, 3001)
	require.NoError(t, startQueuedBlockfetchForTest(ls, connId, nil))

	// selectRetryBlockfetchConn falls back to the reporting connection when
	// no GetActiveConnectionFunc is configured, which is the branch's
	// "no alternate connection" leg -- the one that drops the headers.
	require.Nil(
		t,
		ls.config.GetActiveConnectionFunc,
		"this test's harness must offer no alternate retry connection",
	)
	const upstreamTipSlot = uint64(blockfetchMinBatchGapSlots) * 4
	ls.syncUpstreamTipSlot.Store(upstreamTipSlot)
	require.GreaterOrEqual(
		t,
		upstreamTipSlot-ls.Tip().Point.Slot,
		uint64(blockfetchMinBatchGapSlots),
		"the tip gap must reach the branch this test exercises",
	)

	require.NoError(t, handleEventBlockfetchBatchDoneForTest(
		ls,
		BlockfetchEvent{
			ConnectionId: connId,
			BatchDone:    true,
			RangeErr:     errors.New("connection reset by peer"),
		},
		nil,
	))

	assert.Equal(
		t,
		1,
		ls.chain.HeaderCount(),
		"an obtainable range must survive a transport failure behind a large tip gap",
	)
	testutil.RequireNoReceive(
		t,
		resyncChan,
		100*time.Millisecond,
		"a transport range error must not trigger resync",
	)
}

// requestIdTestLedger builds a deep catch-up ledger whose
// BlockfetchRequestRangeFunc numbers requests from 1 per call, the way
// gouroboros numbers RequestRange calls per connection, and signals each
// dispatch on the returned channel.
func requestIdTestLedger(
	t *testing.T,
	connId ouroboros.ConnectionId,
) (*LedgerState, <-chan uint64, []lcommon.Blake2b256) {
	t.Helper()
	headerCount := BlockfetchBatchSize + 50
	testChain, hashes := buildDeepCatchupChain(t, headerCount)
	dispatched := make(chan uint64, 16)
	var mu sync.Mutex
	var nextId uint64
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				gotConnId ouroboros.ConnectionId,
				_ ocommon.Point,
				_ ocommon.Point,
			) (uint64, error) {
				require.Equal(t, connId, gotConnId)
				mu.Lock()
				nextId++
				id := nextId
				mu.Unlock()
				dispatched <- id
				return id, nil
			},
		},
	}
	// Mithril-covered so delivered blocks skip header crypto verification;
	// these synthetic blocks carry no VRF/KES material.
	ls.mithrilLedgerSlot = uint64(headerCount)
	ls.publishSnapshotsLocked()
	return ls, dispatched, hashes
}

func receiveDispatches(
	t *testing.T,
	dispatched <-chan uint64,
	want ...uint64,
) {
	t.Helper()
	for _, id := range want {
		got := testutil.RequireReceive(
			t,
			dispatched,
			testutil.AsyncWait,
			"expected blockfetch dispatch did not happen",
		)
		require.Equal(t, id, got)
	}
}

func blockfetchReservationsLocked(
	ls *LedgerState,
	connId ouroboros.ConnectionId,
) int {
	return len(ls.blockfetchRequestsInFlight[connIdKey(connId)])
}

// TestBlockfetchRefusedPromotionKeepsQueuedRequestReserved reproduces the
// refused-promotion race: the active batch's BatchDone releases its own
// reservation, promotion refuses the stale pre-queued request, and the
// cleanup that follows must not release the pre-queued request's reservation
// in its place. That request is still outstanding with the peer, so a
// same-connection redispatch must wait for its terminal event, and every
// later terminal event must release exactly its own request.
func TestBlockfetchRefusedPromotionKeepsQueuedRequestReserved(
	t *testing.T,
) {
	t.Parallel()

	connId := testChainsyncConnId(6320, 3001)
	ls, dispatched, _ := requestIdTestLedger(t, connId)

	ls.chainsyncBlockfetchMutex.Lock()
	require.NoError(t, ls.startQueuedBlockfetchLocked(connId, nil))
	require.NotNil(t, ls.nextBlockfetchRequest)
	require.Equal(t, 2, blockfetchReservationsLocked(ls, connId))
	ls.chainsyncBlockfetchMutex.Unlock()
	receiveDispatches(t, dispatched, 1, 2)

	// A rollback after request 2 was pre-queued makes it stale, so promotion
	// refuses it once request 1 completes having applied a block.
	ls.blockfetchRollbackGeneration.Add(1)

	ls.chainsyncBlockfetchMutex.Lock()
	ls.batchBlocksApplied = 1
	require.NoError(t, ls.handleEventBlockfetchBatchDone(BlockfetchEvent{
		ConnectionId: connId,
		BatchDone:    true,
		RequestId:    1,
	}, nil))
	require.Nil(t, ls.nextBlockfetchRequest)
	// Checked before releasing the mutex, so the continuation worker cannot
	// have dispatched yet: request 2 is outstanding and must stay reserved.
	assert.Equal(
		t,
		1,
		blockfetchReservationsLocked(ls, connId),
		"refused promotion released the still-outstanding pre-queued "+
			"request's reservation",
	)
	ls.chainsyncBlockfetchMutex.Unlock()

	testutil.RequireNoReceive(
		t,
		dispatched,
		50*time.Millisecond,
		"redispatched on the connection while pre-queued request 2 was "+
			"still outstanding",
	)

	// Request 2's own terminal event drains it and unblocks the redispatch.
	ls.chainsyncBlockfetchMutex.Lock()
	require.NoError(t, ls.handleEventBlockfetchBatchDone(BlockfetchEvent{
		ConnectionId: connId,
		BatchDone:    true,
		RequestId:    2,
	}, nil))
	ls.chainsyncBlockfetchMutex.Unlock()
	receiveDispatches(t, dispatched, 3, 4)
	ls.blockfetchContinuationMu.Lock()
	ls.blockfetchContinuationWG.Wait()
	ls.blockfetchContinuationMu.Unlock()

	ls.chainsyncBlockfetchMutex.Lock()
	assert.Equal(
		t,
		2,
		blockfetchReservationsLocked(ls, connId),
		"requests 3 and 4 are outstanding and must each hold a reservation",
	)
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

// TestBlockfetchLateTerminalEventReleasesOnlyItsOwnRequest covers the
// force-release side: a timeout teardown releases the active and pre-queued
// requests' reservations while both are still outstanding with the peer and
// redispatches on the same connection. Their late terminal events must not
// release the replacement requests' reservations, and the timed-out active
// request's terminal event must not open the discard latch armed for the
// pre-queued request, whose blocks would then be accepted as the
// replacement batch's.
func TestBlockfetchLateTerminalEventReleasesOnlyItsOwnRequest(
	t *testing.T,
) {
	t.Parallel()

	connId := testChainsyncConnId(6321, 3001)
	ls, dispatched, hashes := requestIdTestLedger(t, connId)

	ls.chainsyncBlockfetchMutex.Lock()
	require.NoError(t, ls.startQueuedBlockfetchLocked(connId, nil))
	ls.chainsyncBlockfetchMutex.Unlock()
	receiveDispatches(t, dispatched, 1, 2)

	handleBlockfetchTimeoutForTest(ls, connId, nil)
	receiveDispatches(t, dispatched, 3, 4)

	ls.chainsyncBlockfetchMutex.Lock()
	require.Equal(t, 2, blockfetchReservationsLocked(ls, connId))
	replacement := append(
		[]chan struct{}(nil),
		ls.blockfetchRequestsInFlight[connIdKey(connId)]...,
	)
	for _, id := range []uint64{1, 2} {
		require.NoError(t, ls.handleEventBlockfetchBatchDone(BlockfetchEvent{
			ConnectionId: connId,
			BatchDone:    true,
			RequestId:    id,
		}, nil))
		assert.Equal(
			t,
			2,
			blockfetchReservationsLocked(ls, connId),
			"late terminal event for force-released request %d released "+
				"a replacement request's reservation",
			id,
		)
		if id != 1 {
			continue
		}
		// Request 2 is still streaming: its block must be dropped, not
		// buffered into the replacement batch.
		require.NoError(t, handleEventBlockfetchBlockDeferred(ls,
			BlockfetchEvent{
				ConnectionId: connId,
				RequestId:    2,
				Point:        ocommon.NewPoint(1, hashes[0].Bytes()),
				Block: &blockfetchTestBlock{
					hash:        hashes[0],
					prevHash:    lcommon.NewBlake2b256(nil),
					slot:        1,
					blockNumber: 1,
				},
			},
			nil,
		))
		assert.Empty(
			t,
			ls.pendingBlockfetchEvents,
			"a block from the abandoned pre-queued request was accepted "+
				"after the timed-out request's terminal event",
		)
	}
	for i, done := range replacement {
		select {
		case <-done:
			t.Errorf("replacement request %d was released early", i+3)
		default:
		}
	}
	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
	ls.chainsyncBlockfetchMutex.Unlock()
}

// TestHandleConnectionClosedReleasesEveryPipelinedRequest pins the close
// path to its contract: a connection close is the terminal barrier for every
// request on it. A connection that no longer owns the active batch can still
// hold more than one reservation (a released-but-outstanding pre-queued
// request and the replacement dispatched after it), and the close must not
// leave any of them for a later same-connection dispatch to wait on.
func TestHandleConnectionClosedReleasesEveryPipelinedRequest(t *testing.T) {
	t.Parallel()

	connId := testChainsyncConnId(6322, 3001)
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.chainsyncBlockfetchMutex.Lock()
	reserved := []chan struct{}{
		ls.beginBlockfetchRequestLocked(connId),
		ls.beginBlockfetchRequestLocked(connId),
	}
	ls.bindBlockfetchRequestIdLocked(connId, reserved[0], 1)
	ls.bindBlockfetchRequestIdLocked(connId, reserved[1], 2)
	ls.chainsyncBlockfetchMutex.Unlock()

	ls.handleConnectionClosedEvent(event.NewEvent(
		ConnectionClosedEventType,
		ConnectionClosedEvent{ConnectionId: connId},
	))

	ls.chainsyncBlockfetchMutex.Lock()
	assert.NotContains(t, ls.blockfetchRequestsInFlight, connIdKey(connId))
	ls.chainsyncBlockfetchMutex.Unlock()
	for i, done := range reserved {
		select {
		case <-done:
		default:
			t.Errorf("connection close left request %d reserved", i+1)
		}
	}
}

// syncSafeBuffer is a mutex-guarded log sink. The blockfetch continuation runs
// on its own worker goroutine, which logs concurrently with the test goroutine
// reading those logs back.
type syncSafeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncSafeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncSafeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// blockfetchTestBlock is a block body whose identity fields the test controls,
// so a body from an abandoned fork can be delivered through the real
// blockfetch subscriber. Only the fields the chain-insertion path reads are
// implemented; the rest of gledger.Block is embedded and never called.
type blockfetchTestBlock struct {
	gledger.Block
	hash        lcommon.Blake2b256
	prevHash    lcommon.Blake2b256
	slot        uint64
	blockNumber uint64
}

func (b *blockfetchTestBlock) Hash() lcommon.Blake2b256 { return b.hash }

func (b *blockfetchTestBlock) PrevHash() lcommon.Blake2b256 { return b.prevHash }
func (b *blockfetchTestBlock) SlotNumber() uint64           { return b.slot }

func (b *blockfetchTestBlock) BlockNumber() uint64 { return b.blockNumber }

func (b *blockfetchTestBlock) Era() lcommon.Era { return babbage.EraBabbage }
func (b *blockfetchTestBlock) Type() int        { return 1 }

func (b *blockfetchTestBlock) Cbor() []byte { return []byte{0x80} }

// blockfetchRollbackFixture is the chainsync rollback fixture plus the
// blockfetch wiring the continuation path needs: a request recorder, a log
// sink, and a resync subscriber.
type blockfetchRollbackFixture struct {
	*chainsyncRollbackFixture
	requests  []ocommon.Point
	logBuf    *syncSafeBuffer
	resyncCh  chan event.ChainsyncResyncEvent
	forkAHash lcommon.Blake2b256
	forkBHash lcommon.Blake2b256
}

func newBlockfetchRollbackFixture(t *testing.T) *blockfetchRollbackFixture {
	t.Helper()
	base := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Close)
	base.ls.config.EventBus = bus

	f := &blockfetchRollbackFixture{
		chainsyncRollbackFixture: base,
		logBuf:                   &syncSafeBuffer{},
		resyncCh:                 make(chan event.ChainsyncResyncEvent, 8),
		forkAHash: lcommon.NewBlake2b256(
			testHashBytes("blockfetch-fork-a-header"),
		),
		forkBHash: lcommon.NewBlake2b256(
			testHashBytes("blockfetch-fork-b-header"),
		),
	}
	base.ls.config.Logger = slog.New(
		slog.NewJSONHandler(f.logBuf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}),
	)
	// Header crypto runs for every slot a Mithril certificate does not cover,
	// and these synthetic blocks carry no VRF/KES material. An
	// epoch the cache covers but whose nonce is not published yet is the
	// state a catching-up node is actually in when wedge appears:
	// headerVerificationEpoch reports errEpochNonceUnavailable, which
	// IsHeaderVerificationDeferred accepts, so the body is admitted and
	// buffered with its stateful verification deferred instead of being
	// rejected as a peer fault. Mithril coverage cannot be used instead --
	// it would have to reach above the fork bodies, and
	// rollbackChainAndStateDeferred refuses a rollback below that boundary.
	fixtureEpoch := models.Epoch{
		EpochId:       0,
		StartSlot:     0,
		LengthInSlots: 1000,
		SlotLength:    1,
		EraId:         eras.BabbageEraDesc.Id,
	}
	base.ls.epochCache = []models.Epoch{fixtureEpoch}
	base.ls.publishSnapshotsLocked()
	// Persisted as well as cached: a rollback reloads the epoch cache from
	// the database, and an in-memory-only entry would vanish at the first
	// fork, putting the tests that deliver a body after the rollback back on
	// the empty-cache rejection.
	require.NoError(t, base.ls.db.SetEpoch(
		fixtureEpoch.StartSlot,
		fixtureEpoch.EpochId,
		nil, nil, nil, nil,
		fixtureEpoch.EraId,
		fixtureEpoch.SlotLength,
		fixtureEpoch.LengthInSlots,
		nil,
	))
	base.ls.config.BlockfetchRequestRangeFunc = func(
		connId ouroboros.ConnectionId,
		start ocommon.Point,
		_ ocommon.Point,
	) (uint64, error) {
		f.requests = append(f.requests, start)
		// The production callback returns after BatchDone has been emitted.
		// This synthetic callback has no protocol event, so release the
		// request reservation before returning to the ledger.
		f.ls.chainsyncBlockfetchMutex.Lock()
		f.ls.completeBlockfetchRequestLocked(connId)
		f.ls.chainsyncBlockfetchMutex.Unlock()
		return 0, nil
	}
	bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			if resync, ok := evt.Data.(event.ChainsyncResyncEvent); ok {
				f.resyncCh <- resync
			}
		},
	)
	return f
}

// queueForkAHeaderAndStartBatch queues a header that continues the fixture's
// current tip and starts a real blockfetch batch for it.
func (f *blockfetchRollbackFixture) queueForkAHeaderAndStartBatch(
	t *testing.T,
) {
	t.Helper()
	require.NoError(t, f.ls.chain.AddBlockHeader(mockHeader{
		hash:        f.forkAHash,
		prevHash:    lcommon.NewBlake2b256(f.currentTip.Point.Hash),
		blockNumber: f.currentTip.BlockNumber + 1,
		slot:        f.currentTip.Point.Slot + 10,
	}))
	require.Equal(t, 1, f.ls.chain.HeaderCount())
	require.NoError(t, startQueuedBlockfetchForTest(f.ls, f.connId, nil))
	require.Len(t, f.requests, 1)
}

// deliverForkABody delivers the body for the fork-A header through the real
// blockfetch event subscriber, where it is buffered pending a commit batch.
func (f *blockfetchRollbackFixture) deliverForkABody(t *testing.T) {
	t.Helper()
	f.ls.handleEventBlockfetch(f.forkABlockEvent())
	require.Len(
		t,
		f.ls.pendingBlockfetchEvents,
		1,
		"the body must be buffered, not committed, for the abandoned "+
			"batch to still hold it when the fork is resolved",
	)
}

func (f *blockfetchRollbackFixture) forkABlockEvent() event.Event {
	point := ocommon.NewPoint(
		f.currentTip.Point.Slot+10,
		f.forkAHash.Bytes(),
	)
	return event.NewEvent(
		BlockfetchEventType,
		BlockfetchEvent{
			ConnectionId: f.connId,
			Point:        point,
			Type:         1,
			Block: &blockfetchTestBlock{
				hash:        f.forkAHash,
				prevHash:    lcommon.NewBlake2b256(f.currentTip.Point.Hash),
				slot:        point.Slot,
				blockNumber: f.currentTip.BlockNumber + 1,
			},
		},
	)
}

// rollbackToAncestorAndQueueForkB reproduces what fork resolution does once it
// has picked the peer's chain: roll the primary chain back to the common
// ancestor, then queue the winning peer's header path from there.
func (f *blockfetchRollbackFixture) rollbackToAncestorAndQueueForkB(
	t *testing.T,
) {
	t.Helper()
	f.ls.chainsyncMutex.Lock()
	var pending pendingPublishes
	err := f.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: f.connId,
			Rollback:     true,
			Point:        f.ancestorTip.Point,
		},
		&pending,
	)
	f.ls.chainsyncMutex.Unlock()
	pending.flush()
	require.NoError(t, err)
	require.Equal(t, f.ancestorTip.Point, f.ls.chain.Tip().Point)
	require.Equal(
		t,
		0,
		f.ls.chain.HeaderCount(),
		"the rollback discards the header queue the abandoned batch was "+
			"fetching",
	)

	require.NoError(t, f.ls.chain.AddBlockHeader(mockHeader{
		hash:        f.forkBHash,
		prevHash:    lcommon.NewBlake2b256(f.ancestorTip.Point.Hash),
		blockNumber: f.ancestorTip.BlockNumber + 1,
		slot:        f.ancestorTip.Point.Slot + 5,
	}))
	require.Equal(t, 1, f.ls.chain.HeaderCount())
}

// TestForkRestartKeepsReplacementHeadersWhenAbandonedBatchArrives covers a
// fork restart. Fork resolution rolls the chain back to the common ancestor, queues
// the winning peer's header path from there, and only then restarts
// blockfetch. That restart flushes whatever the abandoned batch had already
// buffered, so the first body from the losing fork reached chain insertion
// against the replacement header queue, was rejected as not matching it, and
// cleared it -- leaving nothing queued, nothing fetching, and the remaining
// bodies logging "does not fit on current chain tip" on their way out.
//
// A body fetched for a chain the node has since rolled back must be discarded
// instead, so the replacement queue survives and the restarted batch fetches
// it.
func TestForkRestartKeepsReplacementHeadersWhenAbandonedBatchArrives(
	t *testing.T,
) {
	f := newBlockfetchRollbackFixture(t)
	f.queueForkAHeaderAndStartBatch(t)
	f.deliverForkABody(t)
	f.rollbackToAncestorAndQueueForkB(t)

	require.NoError(
		t,
		restartQueuedBlockfetchAfterForkForTest(f.ls, f.connId, nil),
	)

	assert.Equal(
		t,
		1,
		f.ls.chain.HeaderCount(),
		"the replacement header queue must survive the abandoned batch",
	)
	require.Len(
		t,
		f.requests,
		2,
		"the fork restart must issue a request for the replacement queue",
	)
	assert.Equal(
		t,
		ocommon.NewPoint(
			f.ancestorTip.Point.Slot+5,
			f.forkBHash.Bytes(),
		),
		f.requests[1],
		"the restarted batch must fetch from the new continuation point",
	)
	assert.NotContains(
		t,
		f.logBuf.String(),
		"ignoring blockfetch block",
		"a body for a superseded chain must be discarded before it "+
			"reaches chain insertion",
	)
}

func TestForkRestartDropsAbandonedBodyArrivingAfterRestart(t *testing.T) {
	f := newBlockfetchRollbackFixture(t)
	f.queueForkAHeaderAndStartBatch(t)
	f.rollbackToAncestorAndQueueForkB(t)

	require.NoError(
		t,
		restartQueuedBlockfetchAfterForkForTest(f.ls, f.connId, nil),
	)

	// The old request cannot be cancelled. Its final body can arrive after the
	// replacement request has been installed on the same connection and must
	// remain associated with the abandoned request until its BatchDone barrier.
	f.ls.handleEventBlockfetch(f.forkABlockEvent())
	require.Empty(t, f.ls.pendingBlockfetchEvents)
	require.Equal(t, 1, f.ls.chain.HeaderCount())

	f.ls.handleEventBlockfetch(event.NewEvent(
		BlockfetchEventType,
		BlockfetchEvent{
			ConnectionId: f.connId,
			BatchDone:    true,
		},
	))

	point := ocommon.NewPoint(
		f.currentTip.Point.Slot+5,
		f.forkBHash.Bytes(),
	)
	f.ls.handleEventBlockfetch(event.NewEvent(
		BlockfetchEventType,
		BlockfetchEvent{
			ConnectionId: f.connId,
			Point:        point,
			Type:         1,
			Block: &blockfetchTestBlock{
				hash:        f.forkBHash,
				prevHash:    lcommon.NewBlake2b256(f.ancestorTip.Point.Hash),
				slot:        point.Slot,
				blockNumber: f.ancestorTip.BlockNumber + 1,
			},
		},
	))

	require.Len(t, f.ls.pendingBlockfetchEvents, 1)
	require.Equal(t, point, f.ls.pendingBlockfetchEvents[0].Point)
}

// The bounded-recovery case: a batch that delivered bodies but
// extended nothing, while headers stayed queued, must feed the same
// same-range failure streak a NoBlocks reply feeds, so the range is dropped
// and a fresh intersect requested rather than being re-requested forever.
func TestBatchDoneTreatsDiscardedBatchAsUnobtainedRange(t *testing.T) {
	f := newBlockfetchRollbackFixture(t)

	for attempt := 1; attempt <= blockfetchMaxSameRangeFailures; attempt++ {
		if attempt > 1 {
			require.NoError(
				t,
				startQueuedBlockfetchForTest(f.ls, f.connId, nil),
			)
		} else {
			f.queueForkAHeaderAndStartBatch(t)
			f.deliverForkABody(t)
			f.rollbackToAncestorAndQueueForkB(t)
		}
		require.Positive(t, f.ls.chain.HeaderCount())
		var pending pendingPublishes
		require.NoError(t, handleEventBlockfetchBatchDoneForTest(
			f.ls,
			BlockfetchEvent{ConnectionId: f.connId, BatchDone: true},
			&pending,
		))
		pending.flush()
	}

	assert.Equal(
		t,
		0,
		f.ls.chain.HeaderCount(),
		"a range that never extends the chain must stop being requested",
	)
	// The rollback publishes its own "local ledger rollback" resync, so scan
	// for the one the range-failure streak is responsible for.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case resync := <-f.resyncCh:
			if resync.Reason !=
				event.ChainsyncResyncReasonBlockfetchRangeUnavailable {
				continue
			}
			assert.Equal(t, f.connId, resync.ConnectionId)
			return
		case <-deadline:
			t.Fatal(
				"expected a chainsync resync for the queued range that " +
					"repeatedly extended nothing",
			)
		}
	}
}

// The negative case for the discard: the discard is keyed on the chain having
// rolled back under the batch, so a body that genuinely does not fit an
// unchanged tip must still be rejected by chain insertion rather than quietly
// dropped. Accepting such a body to keep the pipeline moving would be far
// worse than the stall.
func TestNonFittingBodyStillRejectedWhenTipDidNotMove(t *testing.T) {
	f := newBlockfetchRollbackFixture(t)
	tipBefore := f.ls.chain.Tip()

	// No queued header and no rollback: the body's own prev hash is what is
	// compared against the tip, and it names a block we do not have.
	point := ocommon.NewPoint(
		tipBefore.Point.Slot+10,
		testHashBytes("orphan-body"),
	)
	f.ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	f.ls.activeBlockfetchConnId = f.connId
	f.ls.handleEventBlockfetch(event.NewEvent(
		BlockfetchEventType,
		BlockfetchEvent{
			ConnectionId: f.connId,
			Point:        point,
			Type:         1,
			Block: &blockfetchTestBlock{
				hash: lcommon.NewBlake2b256(point.Hash),
				prevHash: lcommon.NewBlake2b256(
					testHashBytes("unknown-parent"),
				),
				slot:        point.Slot,
				blockNumber: tipBefore.BlockNumber + 1,
			},
		},
	))
	require.Len(t, f.ls.pendingBlockfetchEvents, 1)

	f.ls.chainsyncBlockfetchMutex.Lock()
	err := f.ls.flushPendingBlockfetchBlocksDeferred(nil)
	f.ls.chainsyncBlockfetchMutex.Unlock()
	require.NoError(t, err)

	assert.Equal(
		t,
		tipBefore.Point,
		f.ls.chain.Tip().Point,
		"a body that does not fit the tip must not be added",
	)
	logs := f.logBuf.String()
	assert.True(
		t,
		strings.Contains(logs, "does not fit on current chain tip"),
		"the body must be rejected by chain insertion, not discarded: %s",
		logs,
	)
	assert.NotContains(
		t,
		logs,
		"discarding blockfetch blocks for superseded chain",
		"nothing rolled back, so the discard path must not run",
	)
}

// A body that is delivered and then discarded has not obtained the range it
// was fetched for, so it must not clear that range's failure record. Clearing
// on arrival kept the streak at zero for exactly the batches the bound exists
// to catch: every round recorded one failure and the next round's delivery
// erased it.
func TestDiscardedBodyDoesNotCountAsRangeProgress(t *testing.T) {
	f := newBlockfetchRollbackFixture(t)
	f.queueForkAHeaderAndStartBatch(t)
	forkAPoint := ocommon.NewPoint(
		f.currentTip.Point.Slot+10,
		f.forkAHash.Bytes(),
	)

	// A batch that ends without obtaining the queued range records the
	// failure against that range's start point, and starts a fresh batch for
	// the same range.
	var pending pendingPublishes
	require.NoError(t, handleEventBlockfetchBatchDoneForTest(
		f.ls,
		BlockfetchEvent{ConnectionId: f.connId, BatchDone: true},
		&pending,
	))
	pending.flush()
	require.True(
		t,
		f.ls.blockfetchRangeFailure.matches(forkAPoint),
		"the unobtained range must be tracked before this test can prove "+
			"anything about clearing it",
	)
	require.Len(t, f.requests, 2)

	// Supersede the batch that is now in flight, then let the peer deliver
	// the tracked range's own start block into it.
	f.ls.chainsyncMutex.Lock()
	var rollbackPending pendingPublishes
	rollbackErr := f.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: f.connId,
			Rollback:     true,
			Point:        f.ancestorTip.Point,
		},
		&rollbackPending,
	)
	f.ls.chainsyncMutex.Unlock()
	rollbackPending.flush()
	require.NoError(t, rollbackErr)
	f.deliverForkABody(t)

	f.ls.chainsyncBlockfetchMutex.Lock()
	err := f.ls.flushPendingBlockfetchBlocksDeferred(nil)
	f.ls.chainsyncBlockfetchMutex.Unlock()
	require.NoError(t, err)

	assert.True(
		t,
		f.ls.blockfetchRangeFailure.matches(forkAPoint),
		"a discarded body must leave the range's failure record intact",
	)
}

// TestRefusedRollbackKeepsInFlightBatch pins the cost of publishing the
// rollback generation before the chain is touched. Publishing early is what
// makes a real rollback observable to a flush that never took the mutex, but
// a rollback validation refuses -- over-K, a point not on the chain, or a
// block the store does not hold -- never moves the chain, and
// handleEventChainsyncRollback treats all three as recoverable. Counting one
// would discard the in-flight batch, leave batchBlocksApplied at zero, and
// feed handleEventBlockfetchBatchDone's same-range failure streak for a range
// the peer is serving correctly.
func TestRefusedRollbackKeepsInFlightBatch(t *testing.T) {
	t.Parallel()

	f := newBlockfetchRollbackFixture(t)
	f.queueForkAHeaderAndStartBatch(t)
	f.deliverForkABody(t)

	tipBefore := f.ls.chain.Tip().Point
	// A point the chain does not hold is refused by
	// validateAndEmitRollbackUndo before chain.RollbackDeferred runs.
	missing := ocommon.NewPoint(
		f.currentTip.Point.Slot-1,
		testHashBytes("rollback-point-not-on-chain"),
	)

	var pending pendingPublishes
	f.ls.chainsyncMutex.Lock()
	err := f.ls.rollbackChainAndStateDeferred(missing, &pending)
	f.ls.chainsyncMutex.Unlock()
	pending.flush()
	require.Error(
		t,
		err,
		"the fixture must reach the refused-rollback branch",
	)
	require.Equal(
		t,
		tipBefore,
		f.ls.chain.Tip().Point,
		"a refused rollback must not move the chain",
	)

	f.ls.chainsyncBlockfetchMutex.Lock()
	superseded := !f.ls.blockfetchBatchStillCurrent()
	f.ls.chainsyncBlockfetchMutex.Unlock()
	require.False(
		t,
		superseded,
		"a rollback the chain never applied must not supersede the batch",
	)

	// The observable consequence: the buffered body still reaches chain
	// insertion, so the batch is not later reported as an unobtained range.
	f.ls.chainsyncBlockfetchMutex.Lock()
	flushErr := f.ls.flushPendingBlockfetchBlocksDeferred(nil)
	applied := f.ls.batchBlocksApplied
	f.ls.chainsyncBlockfetchMutex.Unlock()
	require.NoError(t, flushErr)
	assert.Equal(
		t,
		1,
		applied,
		"the body fetched for the unchanged chain must still be applied",
	)
}

// loadRealByronEBB returns a genuine Byron epoch-boundary block from the
// shared ouroboros-consensus golden fixtures, mirroring loadRealByronMainBlock
// in block_pipeline_validate_test.go.
func loadRealByronEBB(t *testing.T) models.Block {
	t.Helper()
	root, err := fixtures.ExtractEmbeddedFixtures(t.TempDir())
	require.NoError(t, err)
	fixture, err := fixtures.NewFixture(
		root,
		root+"/ouroboros-consensus/ouroboros-consensus-cardano/golden/"+
			"cardano/CardanoNodeToNodeVersion2/Block_Byron_EBB",
	)
	require.NoError(t, err)
	raw, err := fixture.ConsensusLedgerBlockBytes()
	require.NoError(t, err)
	blockType, err := fixture.LedgerBlockType()
	require.NoError(t, err)
	require.Equal(t, uint(gledger.BlockTypeByronEbb), blockType)
	decoded, err := gledger.NewBlockFromCbor(blockType, raw)
	require.NoError(t, err)
	return models.Block{
		Slot:   decoded.SlotNumber(),
		Hash:   decoded.Hash().Bytes(),
		Number: decoded.BlockNumber(),
		Type:   blockType,
		Cbor:   raw,
	}
}

// TestCompareIncomingHeaderToLocalTip_ByronEBBBeatsRegularTip exercises the
// real chain-selection caller (ledger/chainsync.go's
// compareIncomingHeaderToLocalTip) end to end for the exact scenario
// describes: a locally applied Byron regular tip
// against a peer's EBB successor sharing its block number. Canonical Byron
// PBFT counts the boundary block as an additional block despite the shared
// number, so the incoming EBB must beat the local regular tip.
//
// The local tip is a real Byron main block round-tripped through storage
// (database.BlockByHash -> models.Block.Decode, same as
// TestCompareIncomingHeaderToLocalTip_Dijkstra), and the incoming header is a
// real Byron EBB header as chainsync delivers it. Both come from the shared
// ouroboros-consensus golden fixtures, not hand-built CBOR. Only the block
// number the comparator sees is adjusted (a tip's BlockNumber is supplied by
// the caller and by the header's own field, independent of the fixture's
// original block number) so the two tips fall at an equal height, which is
// the only condition this tiebreak needs.
func TestCompareIncomingHeaderToLocalTip_ByronEBBBeatsRegularTip(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	const sharedBlockNumber = 12345

	localBlock := loadRealByronMainBlock(t)
	require.NoError(t, db.BlockCreate(localBlock, nil))
	localTip := ochainsync.Tip{
		Point: ocommon.Point{
			Slot: localBlock.Slot,
			Hash: localBlock.Hash,
		},
		BlockNumber: sharedBlockNumber,
	}

	ebbBlock := loadRealByronEBB(t)
	decodedEBB, err := ebbBlock.Decode()
	require.NoError(t, err)
	ebbHeader, ok := decodedEBB.Header().(*byron.ByronEpochBoundaryBlockHeader)
	require.True(t, ok)
	// The fixture's own block number is irrelevant to the tiebreak; only
	// equal height against the local tip matters here.
	ebbHeader.ConsensusData.Difficulty.Value = sharedBlockNumber

	event := ChainsyncEvent{
		BlockHeader: ebbHeader,
		Point: ocommon.Point{
			Slot: ebbHeader.SlotNumber(),
			Hash: []byte("incoming-ebb-hash"),
		},
	}

	result := ls.compareIncomingHeaderToLocalTip(event, localTip)
	require.Equal(
		t,
		praos.ChainABetter,
		result,
		"a peer's Byron EBB successor must beat the local regular tip at the same block number",
	)
}

// TestBlockfetchContinuationPublishesHeaderInvalidationOnFailure is the
// regression test for the blockfetch continuation discarding the header queue
// without draining the chain's event sequencer.
//
// The continuation runs on its own worker with its own pendingPublishes, so
// the drain registered by the handler that scheduled it covers nothing this
// worker does. When every attempt fails the worker calls clearQueuedHeaders,
// and Chain.ClearHeaders enqueues the invalidation for the announcements those
// headers carried on the chain-level sequencer rather than publishing it
// inline. With nothing draining that sequencer the invalidation was never
// published, so the vote manager kept votes armed for announcements whose
// ranking blocks had just been thrown away -- and, being keyed by announcing
// ranking block, nothing else would retract them.
func TestBlockfetchContinuationPublishesHeaderInvalidationOnFailure(
	t *testing.T,
) {
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	cm, err := chain.NewManager(nil, bus)
	require.NoError(t, err)
	testChain := cm.PrimaryChain()
	require.NotNil(t, testChain)

	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("cont-drain")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))

	subId, headerCh := bus.Subscribe(chain.ChainHeaderEventType)
	defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)

	primary := testChainsyncConnId(6400, 3001)
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return nil
			},
			// Every attempt fails, so the worker reaches the branch that
			// clears the header queue and asks for a chainsync re-sync.
			BlockfetchRequestRangeFunc: func(
				ouroboros.ConnectionId,
				ocommon.Point,
				ocommon.Point,
			) (uint64, error) {
				return 0, errors.New("request failed")
			},
		},
	}

	ls.chainsyncBlockfetchMutex.Lock()
	ls.startQueuedBlockfetchFromEventLocked(
		primary,
		primary,
		"test continuation",
	)
	ls.chainsyncBlockfetchMutex.Unlock()

	ls.blockfetchContinuationMu.Lock()
	ls.blockfetchContinuationWG.Wait()
	ls.blockfetchContinuationMu.Unlock()

	deadline := time.After(5 * time.Second)
	for {
		select {
		case evt := <-headerCh:
			invalidation, ok := evt.Data.(chain.ChainHeaderInvalidationEvent)
			if !ok {
				continue
			}
			require.Equal(
				t,
				chain.HeaderInvalidationQueueCleared,
				invalidation.Reason,
			)
			require.NotEmpty(
				t,
				invalidation.RbHashes,
				"the invalidation must name the discarded headers",
			)
			return
		case <-deadline:
			t.Fatal(
				"the continuation discarded the header queue without " +
					"publishing its invalidation, leaving announcements " +
					"armed in the vote manager",
			)
		}
	}
}

// TestRecoverPeerHeaderHistoryPathWorkIsLinear guards the rollback recovery
// hot path. Every retained suffix head used to rescan the same ancestry while
// chainsyncMutex was held, producing quadratic block-hash lookups when the
// requested rollback point was not present in that peer's history.
func TestRecoverPeerHeaderHistoryPathWorkIsLinear(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.config.GenesisSelectionStateFunc = func() (bool, uint64) {
		return true, ^uint64(0)
	}
	lookupCalls := 0
	fixture.ls.lookupBlockByHash = func([]byte) (models.Block, error) {
		lookupCalls++
		return models.Block{}, models.ErrBlockNotFound
	}
	const headerCount = 2000
	prevHash := testHashBytes("unresolved-root")
	for i := range headerCount {
		hash := testHashBytes(fmt.Sprintf("cpu-probe-%d", i))
		header := mockHeader{
			hash:        lcommon.NewBlake2b256(hash),
			prevHash:    lcommon.NewBlake2b256(prevHash),
			blockNumber: fixture.currentTip.BlockNumber + uint64(i) + 1,
			slot:        fixture.currentTip.Point.Slot + uint64(i) + 1,
		}
		fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        ocommon.NewPoint(header.slot, hash),
			BlockHeader:  header,
		})
		prevHash = hash
	}

	fixture.ls.chainsyncMutex.Lock()
	_, err := fixture.ls.recoverPeerHeaderHistoryFromPointLocked(
		fixture.connId,
		fixture.ancestorTip.Point,
	)
	fixture.ls.chainsyncMutex.Unlock()

	require.NoError(t, err)
	assert.Equal(t, headerCount, lookupCalls)
}

// TestRecoverPeerHeaderHistoryPathWorkHonorsDepthLimit preserves the existing
// safety bound when the external peer-history lookup returns an endless,
// non-cyclic chain. Memoization must not turn a bounded recovery walk into an
// unbounded one.
func TestRecoverPeerHeaderHistoryPathWorkHonorsDepthLimit(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	lookupCalls := 0
	fixture.ls.lookupBlockByHash = func([]byte) (models.Block, error) {
		lookupCalls++
		return models.Block{}, models.ErrBlockNotFound
	}
	peerLookupCalls := 0
	limit := fixture.ls.peerHeaderHistoryLimit()
	fixture.ls.config.PeerHeaderLookupFunc = func(
		_ ouroboros.ConnectionId,
		hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		peerLookupCalls++
		if peerLookupCalls > 2*limit {
			return ChainsyncEvent{}, nil, false
		}
		nextHash := testHashBytes(fmt.Sprintf("depth-next-%d", peerLookupCalls))
		header := mockHeader{
			hash:        lcommon.NewBlake2b256(hash),
			prevHash:    lcommon.NewBlake2b256(nextHash),
			blockNumber: uint64(peerLookupCalls),
			slot:        uint64(peerLookupCalls),
		}
		return ChainsyncEvent{
			Point:       ocommon.NewPoint(header.slot, hash),
			BlockHeader: header,
		}, nextHash, true
	}

	ancestor, path, err := fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		testHashBytes("depth-limit-head"),
		fixture.ancestorTip.Point,
		nil,
		make(map[string]peerHeaderHistoryPathCacheEntry),
	)

	require.NoError(t, err)
	assert.Nil(t, ancestor)
	assert.Nil(t, path)
	assert.Equal(t, limit, lookupCalls)
	assert.Equal(t, limit, peerLookupCalls)
}

func TestFindPeerForkPathCachedTreatsMalformedRetainedRecordAsMissing(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.lookupBlockByHash = func([]byte) (models.Block, error) {
		return models.Block{}, models.ErrBlockNotFound
	}
	malformedHash := testHashBytes("malformed-retained-header")
	history := &peerHeaderChain{
		byHash: map[string]peerHeaderRecord{
			fmt.Sprintf("%x", malformedHash): {
				event: ChainsyncEvent{
					ConnectionId: fixture.connId,
					Point:        ocommon.NewPoint(30, malformedHash),
					Type:         1,
				},
				headerCbor: []byte{0xff},
				prevHash:   fixture.ancestorTip.Point.Hash,
				decodeType: 1,
			},
		},
	}
	peerLookupCalls := 0
	fixture.ls.config.PeerHeaderLookupFunc = func(
		_ ouroboros.ConnectionId,
		_ []byte,
	) (ChainsyncEvent, []byte, bool) {
		peerLookupCalls++
		return ChainsyncEvent{BlockHeader: mockHeader{}},
			fixture.ancestorTip.Point.Hash,
			true
	}

	ancestor, path, err := fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		malformedHash,
		fixture.ancestorTip.Point,
		history,
		make(map[string]peerHeaderHistoryPathCacheEntry),
	)

	require.NoError(t, err)
	assert.Nil(t, ancestor)
	assert.Nil(t, path)
	assert.Zero(t, peerLookupCalls)
}

func TestFindPeerForkPathCachedPreservesShorterSuffixAfterDepthLimit(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	limit := fixture.ls.peerHeaderHistoryLimit()
	lookupCalls := 0
	fixture.ls.lookupBlockByHash = func(hash []byte) (models.Block, error) {
		lookupCalls++
		if bytes.Equal(hash, fixture.ancestorTip.Point.Hash) {
			return models.Block{
				Hash: fixture.ancestorTip.Point.Hash,
				Slot: fixture.ancestorTip.Point.Slot,
			}, nil
		}
		return models.Block{}, models.ErrBlockNotFound
	}
	links := make(map[string][]byte, limit)
	hashes := make([][]byte, limit)
	for i := range limit {
		hashes[i] = testHashBytes(fmt.Sprintf("bounded-suffix-%d", i))
	}
	for i, hash := range hashes {
		nextHash := fixture.ancestorTip.Point.Hash
		if i+1 < len(hashes) {
			nextHash = hashes[i+1]
		}
		links[fmt.Sprintf("%x", hash)] = nextHash
	}
	peerLookupCalls := 0
	fixture.ls.config.PeerHeaderLookupFunc = peerHistoryLookupForTest(
		fixture.connId,
		links,
		&peerLookupCalls,
	)
	cache := make(map[string]peerHeaderHistoryPathCacheEntry)

	ancestor, path, err := fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		hashes[0],
		fixture.ancestorTip.Point,
		nil,
		cache,
	)
	require.NoError(t, err)
	assert.Nil(t, ancestor)
	assert.Nil(t, path)
	assert.Equal(t, limit, lookupCalls)
	assert.Equal(t, limit, peerLookupCalls)

	ancestor, path, err = fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		hashes[1],
		fixture.ancestorTip.Point,
		nil,
		cache,
	)
	require.NoError(t, err)
	require.NotNil(t, ancestor)
	assert.True(t, pointMatches(*ancestor, fixture.ancestorTip.Point))
	assert.Len(t, path, limit-1)
	assert.Equal(t, limit+1, lookupCalls)
	assert.Equal(t, limit, peerLookupCalls)
}

func TestFindPeerForkPathCachedChargesAndPropagatesCachedSuffix(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	limit := fixture.ls.peerHeaderHistoryLimit()
	lookupCalls := 0
	fixture.ls.lookupBlockByHash = func(hash []byte) (models.Block, error) {
		lookupCalls++
		if bytes.Equal(hash, fixture.ancestorTip.Point.Hash) {
			return models.Block{
				Hash: fixture.ancestorTip.Point.Hash,
				Slot: fixture.ancestorTip.Point.Slot,
			}, nil
		}
		return models.Block{}, models.ErrBlockNotFound
	}
	const suffixLength = 128
	prefixLength := limit - suffixLength
	links := make(map[string][]byte, limit)
	suffix := make([][]byte, suffixLength)
	for i := range suffixLength {
		suffix[i] = testHashBytes(fmt.Sprintf("cached-suffix-%d", i))
		nextHash := fixture.ancestorTip.Point.Hash
		if i > 0 {
			links[fmt.Sprintf("%x", suffix[i-1])] = suffix[i]
		}
		links[fmt.Sprintf("%x", suffix[i])] = nextHash
	}
	prefix := make([][]byte, prefixLength)
	for i := range prefixLength {
		prefix[i] = testHashBytes(fmt.Sprintf("cached-prefix-%d", i))
		if i > 0 {
			links[fmt.Sprintf("%x", prefix[i-1])] = prefix[i]
		}
		links[fmt.Sprintf("%x", prefix[i])] = suffix[0]
	}
	peerLookupCalls := 0
	fixture.ls.config.PeerHeaderLookupFunc = peerHistoryLookupForTest(
		fixture.connId,
		links,
		&peerLookupCalls,
	)
	cache := make(map[string]peerHeaderHistoryPathCacheEntry)

	ancestor, _, err := fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		suffix[0],
		fixture.ancestorTip.Point,
		nil,
		cache,
	)
	require.NoError(t, err)
	require.NotNil(t, ancestor)

	ancestor, path, err := fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		prefix[0],
		fixture.ancestorTip.Point,
		nil,
		cache,
	)
	require.NoError(t, err)
	assert.Nil(t, ancestor)
	assert.Nil(t, path)

	lookupsBeforeShorterSuffix := lookupCalls
	peerLookupsBeforeShorterSuffix := peerLookupCalls
	ancestor, path, err = fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		prefix[1],
		fixture.ancestorTip.Point,
		nil,
		cache,
	)
	require.NoError(t, err)
	require.NotNil(t, ancestor)
	assert.True(t, pointMatches(*ancestor, fixture.ancestorTip.Point))
	assert.Len(t, path, limit-1)
	assert.Equal(t, lookupsBeforeShorterSuffix, lookupCalls)
	assert.Equal(t, peerLookupsBeforeShorterSuffix, peerLookupCalls)
}

func TestFindPeerForkPathCachedPropagatesMismatchedAncestor(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	lookupCalls := 0
	fixture.ls.lookupBlockByHash = func(hash []byte) (models.Block, error) {
		lookupCalls++
		if bytes.Equal(hash, fixture.ancestorTip.Point.Hash) {
			return models.Block{
				Hash: fixture.ancestorTip.Point.Hash,
				Slot: fixture.ancestorTip.Point.Slot,
			}, nil
		}
		return models.Block{}, models.ErrBlockNotFound
	}
	suffixHead := testHashBytes("mismatch-suffix-head")
	suffixTail := testHashBytes("mismatch-suffix-tail")
	prefixHead := testHashBytes("mismatch-prefix-head")
	prefixTail := testHashBytes("mismatch-prefix-tail")
	links := map[string][]byte{
		fmt.Sprintf("%x", suffixHead): suffixTail,
		fmt.Sprintf("%x", suffixTail): fixture.ancestorTip.Point.Hash,
		fmt.Sprintf("%x", prefixHead): prefixTail,
		fmt.Sprintf("%x", prefixTail): suffixHead,
	}
	peerLookupCalls := 0
	fixture.ls.config.PeerHeaderLookupFunc = peerHistoryLookupForTest(
		fixture.connId,
		links,
		&peerLookupCalls,
	)
	cache := make(map[string]peerHeaderHistoryPathCacheEntry)

	ancestor, _, err := fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		suffixHead,
		fixture.ancestorTip.Point,
		nil,
		cache,
	)
	require.NoError(t, err)
	require.NotNil(t, ancestor)

	expectedAncestor := ocommon.NewPoint(999, testHashBytes("other-ancestor"))
	ancestor, path, err := fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		prefixHead,
		expectedAncestor,
		nil,
		cache,
	)
	require.NoError(t, err)
	require.NotNil(t, ancestor)
	assert.True(t, pointMatches(*ancestor, fixture.ancestorTip.Point))
	assert.Nil(t, path)

	lookupsBeforeCachedPrefix := lookupCalls
	peerLookupsBeforeCachedPrefix := peerLookupCalls
	ancestor, path, err = fixture.ls.findPeerForkPathCached(
		ChainsyncEvent{ConnectionId: fixture.connId},
		prefixTail,
		expectedAncestor,
		nil,
		cache,
	)
	require.NoError(t, err)
	require.NotNil(t, ancestor)
	assert.True(t, pointMatches(*ancestor, fixture.ancestorTip.Point))
	assert.Nil(t, path)
	assert.Equal(t, lookupsBeforeCachedPrefix, lookupCalls)
	assert.Equal(t, peerLookupsBeforeCachedPrefix, peerLookupCalls)
}

func peerHistoryLookupForTest(
	connId ouroboros.ConnectionId,
	links map[string][]byte,
	lookupCalls *int,
) PeerHeaderLookupFunc {
	return func(
		lookupConnId ouroboros.ConnectionId,
		hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		if lookupConnId != connId {
			return ChainsyncEvent{}, nil, false
		}
		nextHash, ok := links[fmt.Sprintf("%x", hash)]
		if !ok {
			return ChainsyncEvent{}, nil, false
		}
		*lookupCalls++
		header := mockHeader{
			hash:        lcommon.NewBlake2b256(hash),
			prevHash:    lcommon.NewBlake2b256(nextHash),
			blockNumber: uint64(*lookupCalls),
			slot:        uint64(*lookupCalls),
		}
		return ChainsyncEvent{
			ConnectionId: lookupConnId,
			Point:        ocommon.NewPoint(header.slot, hash),
			BlockHeader:  header,
		}, append([]byte(nil), nextHash...), true
	}
}

func TestRecoverPeerHeaderHistoryIncompleteLookupReintersects(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	missingHash := testHashBytes("incomplete-lookup")
	headerHash := testHashBytes("incomplete-lookup-head")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(headerHash),
		prevHash:    lcommon.NewBlake2b256(missingHash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 1,
	}
	fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        ocommon.NewPoint(header.slot, headerHash),
		BlockHeader:  header,
	})
	fixture.ls.config.PeerHeaderLookupFunc = func(
		_ ouroboros.ConnectionId,
		_ []byte,
	) (ChainsyncEvent, []byte, bool) {
		return ChainsyncEvent{}, fixture.ancestorTip.Point.Hash, true
	}

	fixture.ls.chainsyncMutex.Lock()
	headerCount, err := fixture.ls.recoverPeerHeaderHistoryFromPointLocked(
		fixture.connId,
		fixture.ancestorTip.Point,
	)
	fixture.ls.chainsyncMutex.Unlock()

	require.NoError(t, err)
	assert.Zero(t, headerCount)
}

func handleEventBlockfetchBlockDeferred(
	ls *LedgerState,
	e BlockfetchEvent,
	pubs *pendingPublishes,
) error {
	return ls.handleEventBlockfetchBlockDeferredInternal(e, pubs, false)
}

func newTestLedgerStateWithBuffer() (*LedgerState, *bytes.Buffer) {
	var logBuf bytes.Buffer
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(
				slog.NewJSONHandler(
					&logBuf,
					&slog.HandlerOptions{Level: slog.LevelWarn},
				),
			),
		},
	}
	return ls, &logBuf
}

func assertLogContains(t *testing.T, buf *bytes.Buffer, wants []string) {
	t.Helper()
	logOutput := buf.String()
	for _, want := range wants {
		if !strings.Contains(logOutput, want) {
			t.Fatalf(
				"expected log output to contain %q, got %s",
				want,
				logOutput,
			)
		}
	}
}

func TestHandleEventChainsync_WarnsOnUnexpectedEventDataType(t *testing.T) {
	t.Parallel()

	ls, logBuf := newTestLedgerStateWithBuffer()
	evt := event.Event{
		Type:      event.EventType("chainsync.test"),
		Timestamp: time.Date(2026, 3, 16, 12, 0, 0, 0, time.UTC),
		Data:      "unexpected",
	}

	ls.handleEventChainsync(evt)

	assertLogContains(t, logBuf, []string{
		`"msg":"received unexpected event data type"`,
		`"expected":"ChainsyncEvent"`,
		`"data_type":"string"`,
		`"event_type":"chainsync.test"`,
		`"event_timestamp":"2026-03-16T12:00:00Z"`,
		`"event":{"Timestamp":"2026-03-16T12:00:00Z","Data":"unexpected","Type":"chainsync.test"}`,
	})
}

func TestHandleEventBlockfetch_WarnsOnUnexpectedEventDataType(t *testing.T) {
	t.Parallel()

	ls, logBuf := newTestLedgerStateWithBuffer()
	evt := event.Event{
		Type:      event.EventType("blockfetch.test"),
		Timestamp: time.Date(2026, 3, 16, 12, 5, 0, 0, time.UTC),
		Data:      123,
	}

	ls.handleEventBlockfetch(evt)

	assertLogContains(t, logBuf, []string{
		`"msg":"received unexpected event data type"`,
		`"expected":"BlockfetchEvent"`,
		`"data_type":"int"`,
		`"event_type":"blockfetch.test"`,
		`"event_timestamp":"2026-03-16T12:05:00Z"`,
		`"event":{"Timestamp":"2026-03-16T12:05:00Z","Data":123,"Type":"blockfetch.test"}`,
	})
}

// TestRestartQueuedBlockfetchAfterForkPreservesInFlightBatchFromOtherConnection
// pins the second half of the live chain-switch-storm fix: even after
// SwitchBackCooldown bounded the RATE of chain-selection switches, a running
// Preview instance built from that fix still applied zero blocks, because
// tryResolveFork's "fork extends from current tip" branch calls
// restartQueuedBlockfetchAfterForkLocked on essentially every
// active-connection switch (the newly active connection's next header
// almost never fits a header queue built by the connection it replaced),
// and that function previously tore down ANY in-flight batch unconditionally
// -- bypassing handoffPipelineOnSwitchLocked's "preserve in-flight
// blockfetch batch across chain switch" protection through this side
// channel. handleEventBlockfetchBlockDeferred only accepts blocks whose
// connection matches the CURRENT activeBlockfetchConnId, so every block
// already in flight from the torn-down batch was silently discarded on
// arrival, and if the active connection changed faster than one batch's
// round-trip -- true even at a bounded switch rate, confirmed live via
// dingo_ledger_block_stage_duration_seconds{stage="apply"} staying at a
// zero count while blockfetch protocol messages kept arriving -- no block
// ever survived to be applied.
func TestRestartQueuedBlockfetchAfterForkPreservesInFlightBatchFromOtherConnection(
	t *testing.T,
) {
	t.Parallel()

	ls, testChain := newForkExtensionRestartFixture(t)
	otherConn := testChainsyncConnId(6000, 3001)
	newConn := testChainsyncConnId(6000, 3002)

	inFlight := make(chan struct{})
	ls.chainsyncBlockfetchReadyChan = inFlight
	ls.activeBlockfetchConnId = otherConn
	ls.selectedBlockfetchConnId = otherConn
	// Slot 1 is Mithril-covered so handleEventBlockfetchBlockDeferred skips
	// header crypto verification below: that machinery is orthogonal to what
	// this test proves (acceptance is gated on activeBlockfetchConnId, not on
	// whether a restart was attempted), matching the same setup
	// TestBlockfetchHeaderVerificationSkippedForMithrilCoveredSlot uses.
	ls.mithrilLedgerSlot = 1
	ls.publishSnapshotsLocked()

	requestCount := 0
	ls.config.BlockfetchRequestRangeFunc = func(
		connId ouroboros.ConnectionId,
		start ocommon.Point,
		end ocommon.Point,
	) (uint64, error) {
		requestCount++
		return 0, nil
	}

	require.NoError(
		t,
		restartQueuedBlockfetchAfterForkForTest(ls, newConn, nil),
	)

	assert.Equal(
		t,
		0,
		requestCount,
		"a healthy in-flight batch from a different connection must not be interrupted",
	)
	assert.Equal(
		t,
		inFlight,
		ls.chainsyncBlockfetchReadyChan,
		"the in-flight batch's ready channel must survive untouched",
	)
	assert.Equal(
		t,
		otherConn,
		ls.activeBlockfetchConnId,
		"the connection actually fetching must not change",
	)
	assert.Equal(
		t,
		newConn,
		ls.selectedBlockfetchConnId,
		"the NEXT batch must still be retargeted to the new connection",
	)
	assert.Equal(t, 1, testChain.HeaderCount(),
		"queued fork-extension headers survive alongside the preserved batch")

	// The in-flight batch's own connection is still the one blockfetch will
	// accept blocks from: this is the property that actually matters, since
	// handleEventBlockfetchBlockDeferred keys acceptance on
	// activeBlockfetchConnId, not on whether a restart was ever attempted.
	require.NoError(t, handleEventBlockfetchBlockDeferred(ls, BlockfetchEvent{
		ConnectionId: otherConn,
		Block:        &mockBabbageBlock{slot: 1},
		Point:        ocommon.Point{Slot: 1, Hash: []byte("fork-ext-hdr-1")},
	}, nil))
	assert.Len(
		t,
		ls.pendingBlockfetchEvents,
		1,
		"a block from the preserved in-flight connection must still be accepted",
	)

	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
}

// TestRestartQueuedBlockfetchAfterForkStillRestartsSameConnection asserts the
// narrower surviving case: a restart requested for the SAME connection that
// is already fetching still tears down and restarts unconditionally. There
// is no "different peer being preempted" to protect against here, and
// TestStartQueuedBlockfetchAfterForkRestartClearsShadowState
// (chainsync_test.go) depends on this path resetting per-batch shadow
// state even when connId is already the active connection.
func TestRestartQueuedBlockfetchAfterForkStillRestartsSameConnection(
	t *testing.T,
) {
	t.Parallel()

	ls, _ := newForkExtensionRestartFixture(t)
	sameConn := testChainsyncConnId(6000, 3001)

	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.activeBlockfetchConnId = sameConn
	ls.selectedBlockfetchConnId = sameConn

	requestCount := 0
	ls.config.BlockfetchRequestRangeFunc = func(
		connId ouroboros.ConnectionId,
		start ocommon.Point,
		end ocommon.Point,
	) (uint64, error) {
		requestCount++
		return 0, nil
	}

	require.NoError(
		t,
		restartQueuedBlockfetchAfterForkForTest(ls, sameConn, nil),
	)

	assert.Equal(
		t,
		1,
		requestCount,
		"a restart on the same connection must still start a fresh batch",
	)
	assert.Equal(t, sameConn, ls.activeBlockfetchConnId)

	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
}

// These tests pin recoverBlockfetchRestartFailureLocked, the recovery path
// for a failed restartQueuedBlockfetchAfterForkLocked call on the "fork
// extends from current tip" branch. Live Preview-testnet reports showed a
// rapid string of chain-selection reversals recycling the very connection
// this path was about to dispatch a blockfetch request against, logged as
// "failed to start blockfetch after fork extension" immediately followed by
// "failed to lookup connection ID" -- and, before this fix, the queued
// fork-extension headers were then left with nothing ever scheduled to fetch
// their bodies, a silent pipeline stall (continuous reselection, zero
// block-apply progress). Without this recovery function, the caller
// (handleEventChainsyncBlockHeaderWithPending) only logged the failure.

func newForkExtensionRestartFixture(
	t *testing.T,
) (*LedgerState, *chain.Chain) {
	t.Helper()
	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("fork-ext-hdr-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))
	require.Equal(t, 1, testChain.HeaderCount())
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	return ls, testChain
}

// TestRecoverBlockfetchRestartFailureRetriesLiveActiveConnection asserts the
// cheap path: when chain selection currently has a different, live
// connection active, the recovery retries the blockfetch restart against it
// instead of dropping the queue and requesting a re-sync.
func TestRecoverBlockfetchRestartFailureRetriesLiveActiveConnection(t *testing.T) {
	t.Parallel()

	ls, testChain := newForkExtensionRestartFixture(t)
	failedConn := testChainsyncConnId(6000, 3001)
	activeConn := testChainsyncConnId(6000, 3002)

	var requestedConn ouroboros.ConnectionId
	requestCount := 0
	ls.config.GetActiveConnectionFunc = func() *ouroboros.ConnectionId {
		return &activeConn
	}
	ls.config.ConnectionLiveFunc = func(connId ouroboros.ConnectionId) bool {
		return connId == activeConn
	}
	ls.config.BlockfetchRequestRangeFunc = func(
		connId ouroboros.ConnectionId,
		start ocommon.Point,
		end ocommon.Point,
	) (uint64, error) {
		requestCount++
		requestedConn = connId
		return 0, nil
	}

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus
	resyncCh := make(chan event.ChainsyncResyncEvent, 4)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			resyncCh <- e
		},
	)
	t.Cleanup(func() { bus.Unsubscribe(event.ChainsyncResyncEventType, subId) })

	var pending pendingPublishes
	ls.chainsyncBlockfetchMutex.Lock()
	ls.recoverBlockfetchRestartFailureLocked(
		failedConn,
		assertAnError,
		&pending,
	)
	ls.chainsyncBlockfetchMutex.Unlock()
	pending.flush()

	assert.Equal(t, 1, requestCount,
		"must retry the blockfetch request exactly once, against the live active connection")
	assert.Equal(t, activeConn, requestedConn)
	assert.Equal(t, activeConn, ls.activeBlockfetchConnId)
	assert.Equal(t, activeConn, ls.selectedBlockfetchConnId)
	assert.Equal(t, 1, testChain.HeaderCount(),
		"queued fork-extension headers must survive a successful retry")

	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"a successful retry on a live connection must not request a re-sync",
	)
}

// TestRecoverBlockfetchRestartFailureFallsBackToResyncWithoutLiveConnection
// asserts the safety-net path: when chain selection has no live alternative
// (e.g. GetActiveConnectionFunc is unset, or the corresponding
// mode has no configured active-connection callback), the recovery drops the
// stranded header queue and requests a chainsync re-sync rather than leaving
// the pipeline stalled with queued headers nothing will ever fetch.
func TestRecoverBlockfetchRestartFailureFallsBackToResyncWithoutLiveConnection(
	t *testing.T,
) {
	t.Parallel()

	ls, testChain := newForkExtensionRestartFixture(t)
	failedConn := testChainsyncConnId(6000, 3001)
	requestCount := 0
	ls.config.BlockfetchRequestRangeFunc = func(
		connId ouroboros.ConnectionId,
		start ocommon.Point,
		end ocommon.Point,
	) (uint64, error) {
		requestCount++
		return 0, nil
	}

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus
	resyncCh := make(chan event.ChainsyncResyncEvent, 4)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			resyncCh <- e
		},
	)
	t.Cleanup(func() { bus.Unsubscribe(event.ChainsyncResyncEventType, subId) })

	var pending pendingPublishes
	ls.chainsyncBlockfetchMutex.Lock()
	ls.recoverBlockfetchRestartFailureLocked(
		failedConn,
		assertAnError,
		&pending,
	)
	ls.chainsyncBlockfetchMutex.Unlock()
	pending.flush()

	assert.Equal(t, 0, requestCount,
		"no live alternative connection exists, so no retry request is made")
	assert.Equal(t, 0, testChain.HeaderCount(),
		"the stranded queue must be dropped rather than left with nothing to fetch it")

	resyncEvt := testutil.RequireReceive(
		t,
		resyncCh,
		time.Second,
		"a chainsync re-sync must be requested",
	)
	assert.Equal(t, failedConn, resyncEvt.ConnectionId)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonForkExtensionRestartFailed,
		resyncEvt.Reason,
	)
}

// assertAnError is a stand-in restartQueuedBlockfetchAfterForkLocked error:
// the recovery function's behavior depends only on failedConn and the
// current wiring, never on the error's content beyond logging it.
var assertAnError = &testRestartError{}

type testRestartError struct{}

func (e *testRestartError) Error() string {
	return "synthetic restartQueuedBlockfetchAfterForkLocked failure"
}

type encodedHeader struct {
	lcommon.BlockHeader
	cbor []byte
}

func (h encodedHeader) Cbor() []byte { return h.cbor }

func assertPeerHeaderHistoryByteAccounting(t *testing.T, ls *LedgerState) {
	t.Helper()

	total := 0
	for _, history := range ls.peerHeaderHistory {
		historyBytes := 0
		for _, record := range history.byHash {
			historyBytes += record.bytes
		}
		assert.Equal(t, historyBytes, history.retainedBytes)
		total += historyBytes
	}
	assert.Equal(t, total, ls.peerHeaderHistoryBytes)
}

// buildOverflowForkPath constructs a chain of headerCount headers extending
// directly from the fixture's committed tip and records all but the last
// into peerHeaderHistory, exactly as recordPeerHeaderHistory does for every
// observed header regardless of whether it is ever queued (see
// handleEventChainsyncBlockHeaderWithPending, which records before the
// buffering/queuing decision). The last header is returned separately as
// the live trigger event that arrives after the rest of the path is already
// known -- mirroring a large Genesis-mode fork-path reconstruction
// (findPeerForkPath) where the peer's own recent header history resolves
// all the way back to the local committed tip.
//
// Genesis selection is reported active with a large window so
// peerHeaderHistoryLimit accepts a path longer than the default 256-entry
// cap -- required to build a path that exceeds MaxQueuedHeaders (the
// default floor is 10,000), matching the shape of the live-sync freeze this
// file regression-tests (a single reconciliation event with several
// thousand fork-path headers, phase 3).
func buildOverflowForkPath(
	fixture *chainsyncRollbackFixture,
	connId ouroboros.ConnectionId,
	headerCount int,
) mockHeader {
	fixture.ls.config.GenesisSelectionStateFunc = func() (bool, uint64) {
		return true, uint64(headerCount) * 10
	}
	prevHash := fixture.currentTip.Point.Hash
	prevBlockNumber := fixture.currentTip.BlockNumber
	var trigger mockHeader
	for i := range headerCount {
		h := mockHeader{
			hash: lcommon.NewBlake2b256(
				testHashBytes(fmt.Sprintf("overflow-fork-%d", i)),
			),
			prevHash:    lcommon.NewBlake2b256(prevHash),
			blockNumber: prevBlockNumber + 1,
			slot:        fixture.currentTip.Point.Slot + uint64(i) + 1,
		}
		if i == headerCount-1 {
			trigger = h
			break
		}
		fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
			ConnectionId: connId,
			Point: ocommon.NewPoint(
				h.SlotNumber(),
				h.Hash().Bytes(),
			),
			BlockHeader: h,
		})
		prevHash = h.Hash().Bytes()
		prevBlockNumber = h.BlockNumber()
	}
	return trigger
}

func TestRecordPeerHeaderHistoryBoundsRetainedBytes(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.config.GenesisSelectionStateFunc = func() (bool, uint64) {
		return true, ^uint64(0)
	}
	connId := testChainsyncConnId(6201, 3001)
	const headerCount = 5000
	for i := range headerCount {
		hash := testHashBytes(fmt.Sprintf("budget-header-%d", i))
		prevHash := fixture.currentTip.Point.Hash
		if i > 0 {
			prevHash = testHashBytes(fmt.Sprintf("budget-header-%d", i-1))
		}
		header := sizedMockHeader{
			mockHeader: mockHeader{
				hash:        lcommon.NewBlake2b256(hash),
				prevHash:    lcommon.NewBlake2b256(prevHash),
				blockNumber: fixture.currentTip.BlockNumber + uint64(i) + 1,
				slot:        fixture.currentTip.Point.Slot + uint64(i) + 1,
			},
			cbor: bytes.Repeat([]byte{0x01}, 2048),
		}
		fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
			ConnectionId: connId,
			Point:        ocommon.NewPoint(header.SlotNumber(), hash),
			Type:         7,
			BlockHeader:  header,
		})
	}

	history := fixture.ls.peerHeaderHistory[connIdKey(connId)]
	require.NotNil(t, history)
	expectedBytes := peerHeaderHistoryRecordOverhead + 2048 + 32 + 32
	expectedCount := maxPeerHeaderHistoryBytesPerConn / expectedBytes
	assert.Equal(t, expectedCount, len(history.order))
	assert.Equal(
		t,
		hex.EncodeToString(testHashBytes(fmt.Sprintf(
			"budget-header-%d", headerCount-expectedCount,
		))),
		history.order[0],
	)
	assert.Equal(
		t,
		hex.EncodeToString(testHashBytes(fmt.Sprintf(
			"budget-header-%d", headerCount-1,
		))),
		history.order[len(history.order)-1],
	)
	retainedBytes := 0
	decodedHeaders := 0
	for _, record := range history.byHash {
		retainedBytes += record.bytes
		if record.event.BlockHeader != nil {
			decodedHeaders++
		}
		assert.Len(t, record.headerCbor, 2048)
	}
	assert.Zero(t, decodedHeaders)
	assert.Equal(t, retainedBytes, history.retainedBytes)
	assert.Equal(t, retainedBytes, fixture.ls.peerHeaderHistoryBytes)
	assertPeerHeaderHistoryByteAccounting(t, fixture.ls)
	assert.LessOrEqual(
		t,
		history.retainedBytes,
		maxPeerHeaderHistoryBytesPerConn,
	)
}

func TestPeerHeaderHistoryGlobalBudgetRetiresOldestPeerDeterministically(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	ls.peerHeaderHistory = make(map[string]*peerHeaderChain)
	const retainedPerRecord = 32 << 10
	for peer := range 4 {
		key := fmt.Sprintf("peer-%d", peer)
		history := &peerHeaderChain{
			order:  make([]string, 0, minPeerHeaderHistoryRecords),
			byHash: make(map[string]peerHeaderRecord, minPeerHeaderHistoryRecords),
		}
		for idx := range minPeerHeaderHistoryRecords {
			hash := fmt.Sprintf("%s-header-%d", key, idx)
			ls.peerHeaderHistorySequence++
			record := peerHeaderRecord{
				bytes:    retainedPerRecord,
				sequence: ls.peerHeaderHistorySequence,
			}
			history.order = append(history.order, hash)
			history.byHash[hash] = record
			history.retainedBytes += record.bytes
			ls.peerHeaderHistoryBytes += record.bytes
		}
		ls.peerHeaderHistory[key] = history
	}
	require.Equal(t, maxPeerHeaderHistoryBytesTotal, ls.peerHeaderHistoryBytes)

	require.True(t, ls.makePeerHeaderHistoryRoom(1<<20, "new-peer"))
	assert.Nil(t, ls.peerHeaderHistory["peer-0"], "oldest peer retires first")
	for _, key := range []string{"peer-1", "peer-2", "peer-3"} {
		require.Len(t, ls.peerHeaderHistory[key].order, minPeerHeaderHistoryRecords)
	}
	assert.LessOrEqual(t, ls.peerHeaderHistoryBytes, maxPeerHeaderHistoryBytesTotal)
	assertPeerHeaderHistoryByteAccounting(t, ls)
}

func TestPeerHeaderHistoryEvictsExtraRecordsBeforeRetiringPeer(t *testing.T) {
	t.Parallel()
	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	ls.peerHeaderHistory = make(map[string]*peerHeaderChain)
	const retainedPerRecord = 32 << 10
	for peer := range 4 {
		key := fmt.Sprintf("peer-%d", peer)
		count := minPeerHeaderHistoryRecords
		if peer == 3 {
			count++
		}
		history := &peerHeaderChain{
			order:  make([]string, 0, count),
			byHash: make(map[string]peerHeaderRecord, count),
		}
		for idx := range count {
			ls.peerHeaderHistorySequence++
			hash := fmt.Sprintf("%s-header-%d", key, idx)
			bytes := retainedPerRecord
			if peer == 3 && idx >= count-2 {
				bytes = 16 << 10
			}
			record := peerHeaderRecord{
				bytes:    bytes,
				sequence: ls.peerHeaderHistorySequence,
			}
			history.order = append(history.order, hash)
			history.byHash[hash] = record
			history.retainedBytes += bytes
			ls.peerHeaderHistoryBytes += bytes
		}
		ls.peerHeaderHistory[key] = history
	}
	require.Equal(t, maxPeerHeaderHistoryBytesTotal, ls.peerHeaderHistoryBytes)

	require.True(t, ls.makePeerHeaderHistoryRoom(1<<20, "new-peer"))
	assert.Nil(t, ls.peerHeaderHistory["peer-0"], "retire the oldest peer only after excess records are gone")
	require.NotNil(t, ls.peerHeaderHistory["peer-3"])
	assert.Len(t, ls.peerHeaderHistory["peer-3"].order, minPeerHeaderHistoryRecords)
	assert.LessOrEqual(t, ls.peerHeaderHistoryBytes, maxPeerHeaderHistoryBytesTotal)
	assertPeerHeaderHistoryByteAccounting(t, ls)
}

func TestPeerHeaderHistoryRehydratesWireHeader(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	connId := testChainsyncConnId(6202, 3002)
	const slot = 500
	const blockNumber = 42
	header := &babbage.BabbageBlockHeader{
		Body: babbage.BabbageBlockHeaderBody{
			BlockNumber:   blockNumber,
			Slot:          slot,
			PrevHash:      lcommon.NewBlake2b256([]byte("parent")),
			BlockBodyHash: lcommon.NewBlake2b256([]byte("body")),
			ProtoVersion:  babbage.BabbageProtoVersion{Major: 8},
		},
	}
	headerCbor, err := cbor.Encode(header)
	require.NoError(t, err)
	header.SetCbor(headerCbor)
	encoded := encodedHeader{BlockHeader: header, cbor: headerCbor}
	fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
		ConnectionId: connId,
		Point:        ocommon.NewPoint(slot, header.Hash().Bytes()),
		BlockNumber:  blockNumber,
		Type:         gledger.BlockTypeBabbage,
		BlockHeader:  encoded,
	})

	history := fixture.ls.peerHeaderHistory[connIdKey(connId)]
	require.NotNil(t, history)
	record := history.byHash[hex.EncodeToString(header.Hash().Bytes())]
	assert.Nil(t, record.event.BlockHeader)
	rehydrated, ok := record.chainsyncEvent()
	require.True(t, ok)
	require.NotNil(t, rehydrated.BlockHeader)
	assert.Equal(t, header.Hash(), rehydrated.BlockHeader.Hash())
	assert.Equal(t, header.PrevHash(), rehydrated.BlockHeader.PrevHash())
	assert.Equal(t, header.BlockNumber(), rehydrated.BlockHeader.BlockNumber())
	assert.Equal(t, header.SlotNumber(), rehydrated.BlockHeader.SlotNumber())
}

// TestTryResolveForkExtensionRestartsBlockfetchAfterQueueOverflow pins the
// fix for the phase 3 live-sync freeze: a fork-resolution path whose
// length exceeds the header queue's capacity fails partway through
// (chain.ErrHeaderQueueFull) appending onto the current chain tip. Before
// the fix, tryResolveFork's "fork extends from current tip" loop returned
// immediately on that failure without ever restarting blockfetch for the
// headers it DID manage to queue -- and because chain.AddBlockHeader's
// capacity check runs before any "should I start a fetch" decision, no
// later header event, from any peer, fork or not, could ever trigger a
// fresh blockfetch again: the queue would never drain and the node would
// stop advancing permanently.
func TestTryResolveForkExtensionRestartsBlockfetchAfterQueueOverflow(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	maxHeaders := fixture.ls.chain.MaxQueuedHeaders()
	connId := testChainsyncConnId(6201, 3001)
	trigger := buildOverflowForkPath(fixture, connId, maxHeaders+5)

	// Sanity-check: chain.AddBlockHeader must reject the trigger header as
	// not fitting the (empty) header queue's tip, i.e. the not-fit gate the
	// production handler needs to reach tryResolveFork -- not a capacity
	// rejection, since the queue is empty at this point.
	err := fixture.ls.chain.AddBlockHeader(trigger)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAsf(
		t, err, &notFitErr,
		"expected the trigger header to be rejected as not fitting the "+
			"chain tip so the handler reaches tryResolveFork; got err=%v",
		err,
	)

	requestCount := 0
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		_ ouroboros.ConnectionId,
		_ ocommon.Point,
		_ ocommon.Point,
	) (uint64, error) {
		requestCount++
		return 0, nil
	}

	evt := ChainsyncEvent{
		ConnectionId: connId,
		Point: ocommon.NewPoint(
			trigger.SlotNumber(),
			trigger.Hash().Bytes(),
		),
		BlockHeader: trigger,
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				trigger.SlotNumber()+10,
				testHashBytes("overflow-peer-tip-ahead"),
			),
			BlockNumber: trigger.BlockNumber() + 1,
		},
	}

	resolved, err := fixture.ls.tryResolveFork(evt, notFitErr, nil, false)
	require.NoError(t, err)
	require.False(
		t,
		resolved,
		"a fork path longer than the queue's capacity must fail to fully "+
			"append",
	)

	assert.Equal(
		t,
		maxHeaders,
		fixture.ls.chain.HeaderCount(),
		"the loop must queue headers up to exactly the queue's capacity "+
			"before failing",
	)
	assert.Equal(
		t,
		2,
		requestCount,
		"a queue-full fork-extension failure must still restart "+
			"blockfetch for the headers it did manage to queue when "+
			"nothing is currently fetching them -- otherwise the queue "+
			"never drains and the node stops advancing permanently. "+
			"maxHeaders (10,000) is far deeper than BlockfetchBatchSize "+
			"(500), so the restart is also pipelining-eligible (issue "+
			"#4651) and pre-queues a second request for the remaining "+
			"headers instead of waiting for the first batch's round trip",
	)
	assert.NotNil(
		t,
		fixture.ls.chainsyncBlockfetchReadyChan,
		"a fresh blockfetch batch must be recorded as in progress",
	)
}

// TestEnsureBlockfetchDrainingAfterForkQueueFailureRecoversWhenStartFails is
// a regression test for ensureBlockfetchDrainingAfterForkQueueFailure
// previously only logging a warning when the restart it attempts
// (startQueuedBlockfetchLocked/startQueuedBlockfetchOnLocked) itself fails.
// With nothing already fetching and the header queue full, that left the
// queued headers permanently stranded: chain.AddBlockHeader's capacity check
// rejects every later header, from any peer, before it can ever reach the
// "should I start a fetch" logic, so nothing would ever retry. The fix
// clears the queued headers and requests a chainsync re-sync instead of
// just logging, matching noteBlockfetchRangeUnavailable's equivalent
// recovery for the same "stuck queue" shape.
func TestEnsureBlockfetchDrainingAfterForkQueueFailureRecoversWhenStartFails(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	maxHeaders := fixture.ls.chain.MaxQueuedHeaders()
	connId := testChainsyncConnId(6203, 3001)
	trigger := buildOverflowForkPath(fixture, connId, maxHeaders+5)

	err := fixture.ls.chain.AddBlockHeader(trigger)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAs(t, err, &notFitErr)

	// Every restart attempt fails, exercising the failure branch this test
	// targets rather than the already-covered success branch.
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		_ ouroboros.ConnectionId,
		_ ocommon.Point,
		_ ocommon.Point,
	) (uint64, error) {
		return 0, errors.New("simulated blockfetch request failure")
	}

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	fixture.ls.config.EventBus = bus
	resyncSubID, resyncCh := bus.SubscribeWithBuffer(
		event.ChainsyncResyncEventType,
		4,
	)
	require.NotEqual(t, event.EventSubscriberId(0), resyncSubID)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, resyncSubID)
	})

	evt := ChainsyncEvent{
		ConnectionId: connId,
		Point: ocommon.NewPoint(
			trigger.SlotNumber(),
			trigger.Hash().Bytes(),
		),
		BlockHeader: trigger,
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				trigger.SlotNumber()+10,
				testHashBytes("overflow-peer-tip-ahead-start-fails"),
			),
			BlockNumber: trigger.BlockNumber() + 1,
		},
	}

	// nil pending: pendingPublishes.add publishes immediately on a nil
	// receiver, which is what lets the subscription above observe the
	// resync request synchronously.
	resolved, err := fixture.ls.tryResolveFork(evt, notFitErr, nil, false)
	require.NoError(t, err)
	require.False(t, resolved)

	assert.Equal(
		t,
		0,
		fixture.ls.chain.HeaderCount(),
		"a restart failure with nothing else able to ever retry must "+
			"clear the stranded queued headers rather than leave them "+
			"permanently stuck",
	)

	resyncEvt := testutil.RequireReceive(
		t, resyncCh, testutil.AsyncWait,
		"a chainsync re-sync must be requested when the recovery "+
			"restart itself fails",
	)
	data, ok := resyncEvt.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok, "unexpected event payload %T", resyncEvt.Data)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonForkQueueOverflowRestartFailed,
		data.Reason,
	)
}

// TestTryResolveForkExtensionDoesNotThrashAlreadyRunningBlockfetch guards
// the other side of the same fix: the identical queue-full failure can fire
// repeatedly (once per rejected header) while a healthy batch is already
// draining the existing backlog -- this is the common case in practice, as
// many peers race small forks at the live tip in quick succession.
// ensureBlockfetchDrainingAfterForkQueueFailure must not interrupt that
// batch on every such event, or a batch would never be allowed to complete.
func TestTryResolveForkExtensionDoesNotThrashAlreadyRunningBlockfetch(
	t *testing.T,
) {
	t.Parallel()

	// Positive control: prove this test reaches the recovery body when no
	// batch is active. Without this control, replacing the whole body of
	// ensureBlockfetchDrainingAfterForkQueueFailure with `return` would make
	// the absence-only assertions below pass.
	control := newChainsyncRollbackFixture(t)
	controlConnId := testChainsyncConnId(6204, 3001)
	controlHeader := mockHeader{
		hash: lcommon.NewBlake2b256(
			testHashBytes("fork-overflow-drain-positive-control"),
		),
		prevHash:    lcommon.NewBlake2b256(control.currentTip.Point.Hash),
		blockNumber: control.currentTip.BlockNumber + 1,
		slot:        control.currentTip.Point.Slot + 1,
	}
	require.NoError(t, control.ls.chain.AddBlockHeader(controlHeader))
	controlRequests := 0
	control.ls.config.BlockfetchRequestRangeFunc = func(
		_ ouroboros.ConnectionId,
		_ ocommon.Point,
		_ ocommon.Point,
	) (uint64, error) {
		controlRequests++
		return 0, nil
	}
	control.ls.ensureBlockfetchDrainingAfterForkQueueFailure(
		controlConnId,
		nil,
	)
	require.Equal(
		t,
		1,
		controlRequests,
		"positive control must start a fetch when no batch is active",
	)
	require.NotNil(t, control.ls.chainsyncBlockfetchReadyChan)

	fixture := newChainsyncRollbackFixture(t)
	maxHeaders := fixture.ls.chain.MaxQueuedHeaders()
	connId := testChainsyncConnId(6202, 3001)
	trigger := buildOverflowForkPath(fixture, connId, maxHeaders+5)

	err := fixture.ls.chain.AddBlockHeader(trigger)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAs(t, err, &notFitErr)

	requestCount := 0
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		_ ouroboros.ConnectionId,
		_ ocommon.Point,
		_ ocommon.Point,
	) (uint64, error) {
		requestCount++
		return 0, nil
	}
	// Simulate a blockfetch batch already in flight.
	fixture.ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	// Captured before tryResolveFork runs so the assertion below can prove
	// the existing batch's channel specifically survived untouched, not
	// merely that *a* non-nil channel exists afterward -- a regression
	// that replaced it with a brand new channel (without ever calling
	// BlockfetchRequestRangeFunc) would pass requestCount==0 but still be
	// wrong.
	preExistingReadyChan := fixture.ls.chainsyncBlockfetchReadyChan

	evt := ChainsyncEvent{
		ConnectionId: connId,
		Point: ocommon.NewPoint(
			trigger.SlotNumber(),
			trigger.Hash().Bytes(),
		),
		BlockHeader: trigger,
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				trigger.SlotNumber()+10,
				testHashBytes("overflow-peer-tip-ahead-inflight"),
			),
			BlockNumber: trigger.BlockNumber() + 1,
		},
	}

	resolved, err := fixture.ls.tryResolveFork(evt, notFitErr, nil, false)
	require.NoError(t, err)
	require.False(t, resolved)

	assert.Zero(
		t,
		requestCount,
		"an already in-progress blockfetch batch must not be "+
			"interrupted/restarted by a queue-full fork-extension failure",
	)
	assert.True(
		t,
		preExistingReadyChan == fixture.ls.chainsyncBlockfetchReadyChan,
		"the existing batch's ready channel must be left untouched -- a "+
			"regression that replaced it with a new channel without ever "+
			"calling BlockfetchRequestRangeFunc would otherwise pass "+
			"undetected",
	)
}

// TestFindPeerForkPathRejectsAncestorAheadOfTip is a regression test for
// findPeerForkPath: a hash-index hit past the local tip is not reachable and
// must be treated as unresolved. It seeds the stale row directly through
// database.Database.BlockCreate, bypassing chain.Chain's own
// tip-consistency checks, since that is the point: it simulates the
// leftover artifact an incomplete rollback leaves, not a block chain.Chain
// would ever admit itself.
func TestFindPeerForkPathRejectsAncestorAheadOfTip(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	localTip := fixture.ls.chain.Tip()
	require.Equal(t, fixture.currentTip, localTip)

	// A block well AFTER the local tip, reachable only by hash -- exactly
	// what a rollback that removed everything above the tip except this one
	// stray index would leave behind. PrevHash is set to the real tip so
	// the row looks, superficially, like a legitimate direct child -- the
	// defect is not that this hash is unbelievable, it is that nothing
	// checks whether it is still reachable from where we actually are.
	orphanHash := testHashBytes("orphan-block-ahead-of-tip")
	orphanSlot := localTip.Point.Slot + 157
	require.NoError(t, fixture.ls.db.BlockCreate(models.Block{
		Slot:     orphanSlot,
		Hash:     orphanHash,
		Number:   localTip.BlockNumber + 1,
		PrevHash: localTip.Point.Hash,
		Cbor:     []byte{0x80},
	}, nil))

	// Confirm the seed actually reproduces the "reachable by hash, ahead of
	// tip" state the bug depends on, independent of findPeerForkPath: a
	// weakened seed would make every assertion below pass vacuously.
	seeded, err := fixture.ls.blockByHash(orphanHash)
	require.NoError(t, err, "the stale row must be reachable by hash")
	require.Greater(
		t,
		seeded.Slot,
		localTip.Point.Slot,
		"the seeded row must be ahead of the local tip for this test to "+
			"exercise the bug",
	)

	// A peer's incoming header claims orphanHash as its own parent -- this
	// is real, honestly-reported peer data; findPeerForkPath's job is to
	// decide whether OUR OWN local state can vouch for orphanHash as a
	// common ancestor, and it must not.
	evt := ChainsyncEvent{
		ConnectionId: testChainsyncConnId(6301, 3001),
		Point: ocommon.NewPoint(
			orphanSlot+1,
			testHashBytes("peer-child-of-orphan"),
		),
	}

	ancestorPoint, forkPath, err := fixture.ls.findPeerForkPath(
		evt,
		orphanHash,
		localTip.Point.Slot,
	)
	require.NoError(t, err)
	assert.Nil(
		t,
		ancestorPoint,
		"a block-index hit ahead of the local tip must never be accepted "+
			"as a common ancestor: found slot %d > local tip slot %d",
		seeded.Slot,
		localTip.Point.Slot,
	)
	assert.Nil(t, forkPath)
}

// TestFindPeerForkPathAcceptsAncestorAtOrBeforeTip is the positive control
// for the fix above: a block-index hit that genuinely IS at or before the
// local tip must still resolve normally. Without this, a broken fix that
// rejected every blockByHash hit unconditionally would also pass the
// negative test's "ancestorPoint is nil" assertion for the wrong reason.
func TestFindPeerForkPathAcceptsAncestorAtOrBeforeTip(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	localTip := fixture.ls.chain.Tip()

	evt := ChainsyncEvent{
		ConnectionId: testChainsyncConnId(6302, 3001),
		Point: ocommon.NewPoint(
			localTip.Point.Slot+1,
			testHashBytes("peer-child-of-tip"),
		),
	}

	// The fixture's own current tip is itself a valid ancestor of a fork
	// path that extends directly from it.
	ancestorPoint, forkPath, err := fixture.ls.findPeerForkPath(
		evt,
		localTip.Point.Hash,
		localTip.Point.Slot,
	)
	require.NoError(t, err)
	require.NotNil(t, ancestorPoint)
	assert.Equal(t, localTip.Point.Slot, ancestorPoint.Slot)
	assert.Equal(t, localTip.Point.Hash, ancestorPoint.Hash)
	assert.Len(t, forkPath, 1)
}

// TestFindPeerForkPathCachedRejectsAncestorAheadOfTip is the
// findPeerForkPathCached counterpart to the test above: a hit ahead of the
// expected ancestor must resolve as unresolved and must not be memoized, or
// the hop that led to it is cached as resolving to it.
func TestFindPeerForkPathCachedRejectsAncestorAheadOfTip(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	localTip := fixture.ls.chain.Tip()

	// headHash is one hop away from the stranded row: the walk must visit
	// it, record a step for it, and only then reach orphanHash -- this is
	// what exercises cachePeerHeaderHistoryPath's memoization of the hop
	// that led to the bad hit, not just the hit itself.
	headHash := testHashBytes("cached-ancestor-head-ahead-of-tip")
	orphanHash := testHashBytes("cached-orphan-block-ahead-of-tip")
	orphanSlot := localTip.Point.Slot + 157

	fixture.ls.lookupBlockByHash = func(hash []byte) (models.Block, error) {
		if bytes.Equal(hash, orphanHash) {
			return models.Block{Slot: orphanSlot, Hash: orphanHash}, nil
		}
		return models.Block{}, models.ErrBlockNotFound
	}
	headPoint := ocommon.NewPoint(orphanSlot+1, testHashBytes("cached-peer-child"))
	headEvent := ChainsyncEvent{
		ConnectionId: testChainsyncConnId(6304, 3001),
		Point:        headPoint,
		BlockHeader: mockHeader{
			hash:        lcommon.NewBlake2b256(headPoint.Hash),
			prevHash:    lcommon.NewBlake2b256(headHash),
			blockNumber: localTip.BlockNumber + 2,
			slot:        headPoint.Slot,
		},
	}
	fixture.ls.config.PeerHeaderLookupFunc = func(
		_ ouroboros.ConnectionId,
		hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		if bytes.Equal(hash, headHash) {
			return headEvent, orphanHash, true
		}
		return ChainsyncEvent{}, nil, false
	}

	cache := make(map[string]peerHeaderHistoryPathCacheEntry)
	ancestor, path, err := fixture.ls.findPeerForkPathCached(
		headEvent,
		headHash,
		localTip.Point,
		nil,
		cache,
	)
	require.NoError(t, err)
	assert.Nil(
		t,
		ancestor,
		"a block-index hit ahead of the local tip must never be accepted "+
			"as a common ancestor, cached or not",
	)
	assert.Nil(t, path)

	if entry, ok := cache[hex.EncodeToString(headHash)]; ok {
		assert.False(
			t,
			entry.ok,
			"the hop leading to a stranded ahead-of-tip hit must not be "+
				"cached as resolving to it",
		)
	}
}

// chainSwitchBarrierTimeout bounds the wait for the barrier below. It is a
// deadlock bound, not a settling delay: the barrier is already queued behind
// whatever the selector decided by the time the wait starts, so the normal
// cost is one lane hand-off.
const chainSwitchBarrierTimeout = 30 * time.Second

// chainSwitchBarrier is a sentinel published through the chain-switch ordered
// lane so the test below can tell "no switch was decided" from "the switch has
// not been delivered yet".
//
// ChainSelector.publishSelection routes chain switches through
// EventBus.PublishOrdered, so
// HandlePeerTipUpdateEvent returns before the lane worker has handed the event
// to any subscriber. A lane is a FIFO drained by exactly one worker, so a
// sentinel enqueued after those switches is delivered after them: receiving it
// back is proof that every switch published earlier on this goroutine has
// already reached the subscription. Its Data type is not ChainSwitchEvent, so
// it is skipped rather than counted as a decision. Same construction as
// switchBarrier in ouroboros/tests_test.go.
type chainSwitchBarrier struct{}

// TestCanonicalFrontierCrossingDoesNotCloseAPeerAheadOfLocalTip closes the loop
// between chain selection and the ledger's fresh-cursor handling.
//
// Two canonical public roots advertise the SAME tip while their delivered
// frontiers cross by more than k. Every peer-to-peer ChainSwitchEvent the
// selector publishes for a peer whose delivered frontier is ahead of the local
// tip makes chainSwitchNeedsFreshCursorLocked request a resync, and
// ChainsyncResyncReasonChainSwitchCursorAhead requires a fresh connection — so
// each such switch closes the selected connection, resets its cursor back to
// the local tip and hands the frontier lead to the other peer. That is the
// self-sustaining switch loop observed during Preview from-genesis validation.
//
// The assertion is on the pair, not on either component alone: no chain-switch
// event this scenario produces may ask the ledger for a fresh cursor.
func TestCanonicalFrontierCrossingDoesNotCloseAPeerAheadOfLocalTip(
	t *testing.T,
) {
	chainManager, err := chain.NewManager(nil, nil)
	require.NoError(t, err)
	testChain := chainManager.PrimaryChain()
	require.NoError(t, testChain.AddLocalBlock(&mockBabbageBlock{slot: 100}))
	require.Zero(t, testChain.HeaderCount())
	localTip := testChain.Tip()

	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	selectorBus := event.NewEventBus(nil, nil)
	t.Cleanup(selectorBus.Stop)
	_, switchCh := selectorBus.Subscribe(chainselection.ChainSwitchEventType)

	const securityParam = 432 // Preview k
	selector := chainselection.NewChainSelector(
		chainselection.ChainSelectorConfig{
			EventBus:      selectorBus,
			SecurityParam: securityParam,
			Logger:        slog.New(slog.NewJSONHandler(io.Discard, nil)),
			// The rollback subscription is not needed here and would race with
			// the synchronous tip updates below.
			DisableEventSubscriptions: true,
		},
	)
	selector.SetLocalTip(localTip)

	rootA := testChainsyncConnId(6000, 3001)
	rootB := testChainsyncConnId(6000, 3002)
	// The tip both canonical public roots advertised in the reproduction.
	advertised := ochainsync.Tip{
		Point: ocommon.NewPoint(
			121697834,
			[]byte("canonical-preview-tip"),
		),
		BlockNumber: 4625199,
	}
	frontier := func(block uint64) ochainsync.Tip {
		return ochainsync.Tip{
			Point: ocommon.NewPoint(
				600000+block,
				[]byte("hdr-"+strconv.FormatUint(block, 10)),
			),
			BlockNumber: block,
		}
	}
	deliver := func(connId ouroboros.ConnectionId, block uint64) {
		t.Helper()
		observed := frontier(block)
		selector.HandlePeerTipUpdateEvent(event.NewEvent(
			chainselection.PeerTipUpdateEventType,
			chainselection.PeerTipUpdateEvent{
				ConnectionId: connId,
				Tip:          advertised,
				ObservedTip:  observed,
				PraosView: chainselection.PraosTiebreakerViewFromTip(
					observed,
					nil,
					chainselection.PraosTiebreakerConfigUnknown(),
				),
			},
		))
	}

	// Both roots start on the same delivered header, well ahead of the local
	// tip, then their delivered frontiers cross repeatedly. Each step stays
	// within k of the peer's own previous frontier so the plausibility bound
	// accepts it; the crossings at 28842 and 29706 put the lead at 742 blocks,
	// the gap seen in the reproduction.
	deliver(rootA, 27900)
	deliver(rootB, 27900)
	for _, step := range []struct {
		conn  ouroboros.ConnectionId
		block uint64
	}{
		{rootB, 28029},
		{rootA, 28100},
		{rootB, 28461},
		{rootB, 28842},
		{rootA, 28532},
		{rootA, 28964},
		{rootB, 29274},
		{rootB, 29706},
	} {
		deliver(step.conn, step.block)
	}

	// The selector publishes through an ordered lane, so a switch it decided
	// during the deliveries above may not have reached switchCh yet. Enqueue a
	// barrier behind those switches and read until it comes back: everything
	// ahead of it in the lane's FIFO has been delivered by then.
	require.True(
		t,
		selectorBus.PublishOrdered(
			chainselection.ChainSwitchEventType,
			event.NewEvent(
				chainselection.ChainSwitchEventType,
				chainSwitchBarrier{},
			),
		),
		"event bus refused the chain-switch barrier",
	)
	var checked int
	for drained := false; !drained; {
		evt := testutil.RequireReceive(
			t,
			switchCh,
			chainSwitchBarrierTimeout,
			"chain-switch barrier",
		)
		switch switchEvent := evt.Data.(type) {
		case chainSwitchBarrier:
			drained = true
		case chainselection.ChainSwitchEvent:
			checked++
			assert.False(
				t,
				ls.chainSwitchNeedsFreshCursorLocked(
					switchEvent,
					switchEvent.NewConnectionId,
				),
				"a delivered-frontier crossing between peers on the same advertised chain must not close the selected connection (switch to %s at delivered block %d)",
				switchEvent.NewConnectionId.String(),
				switchEvent.NewObservedTip.BlockNumber,
			)
		default:
			// Only the selector and the barrier above publish on this
			// lane, so anything else is a bug in one of them.
			t.Fatalf("unexpected %T on the chain_switch lane", evt.Data)
		}
	}
	require.Positive(
		t,
		checked,
		"the scenario must publish at least the initial selection event",
	)
}

type pastHorizonSlotTimeProvider struct {
	SlotTimeProvider
	rejectedSlot uint64
}

type arrivalPastHorizonSlotTimeProvider struct {
	SlotTimeProvider
}

func (p arrivalPastHorizonSlotTimeProvider) TimeToSlot(
	time.Time,
) (uint64, error) {
	return 0, hardfork.ErrPastHorizon
}

type failingSlotTimeProvider struct {
	SlotTimeProvider
	rejectedSlot uint64
	err          error
}

func (p failingSlotTimeProvider) SlotToTime(slot uint64) (time.Time, error) {
	if slot == p.rejectedSlot {
		return time.Time{}, p.err
	}
	return p.SlotTimeProvider.SlotToTime(slot)
}

func (p pastHorizonSlotTimeProvider) SlotToTime(
	slot uint64,
) (time.Time, error) {
	if slot == p.rejectedSlot {
		return time.Time{}, hardfork.ErrPastHorizon
	}
	return p.SlotTimeProvider.SlotToTime(slot)
}

func newFutureHeaderTestLedger(
	t *testing.T,
	systemStart time.Time,
	now time.Time,
) (*LedgerState, *[]time.Duration) {
	t.Helper()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())
	clock.nowFunc = func() time.Time { return now }
	waits := make([]time.Duration, 0, 1)
	clock.waitFunc = func(_ context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		return nil
	}
	return &LedgerState{
		slotClock: clock,
		ctx:       t.Context(),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}, &waits
}

func futureHeaderEvent(slot uint64, arrival time.Time) ChainsyncEvent {
	header := &envelopeTestHeader{
		slot: slot,
		era:  shelley.EraShelley,
	}
	return ChainsyncEvent{
		BlockHeader: header,
		ArrivalTime: arrival,
		Point:       ocommon.NewPoint(slot, []byte{byte(slot)}),
	}
}

func TestAwaitChainsyncHeaderAdmissionBoundaries(t *testing.T) {
	t.Parallel()

	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)

	t.Run("current header is accepted immediately", func(t *testing.T) {
		arrival := systemStart.Add(100 * time.Second)
		ls, waits := newFutureHeaderTestLedger(t, systemStart, arrival)

		accepted, err := ls.AwaitChainsyncHeaderAdmission(t.Context(),
			futureHeaderEvent(100, arrival),
		)
		require.NoError(t, err)
		require.True(t, accepted)
		require.Empty(t, *waits)
	})

	t.Run("clock skew boundary waits until onset", func(t *testing.T) {
		arrival := systemStart.Add(100 * time.Second)
		ls, waits := newFutureHeaderTestLedger(t, systemStart, arrival)

		accepted, err := ls.AwaitChainsyncHeaderAdmission(t.Context(),
			futureHeaderEvent(102, arrival),
		)
		require.NoError(t, err)
		require.True(t, accepted)
		require.Equal(t, []time.Duration{2 * time.Second}, *waits)
	})

	t.Run(
		"beyond skew is deliberately dropped despite processing delay",
		func(t *testing.T) {
			arrival := systemStart.Add(100*time.Second - time.Nanosecond)
			processTime := systemStart.Add(103 * time.Second)
			ls, waits := newFutureHeaderTestLedger(t, systemStart, processTime)

			accepted, err := ls.AwaitChainsyncHeaderAdmission(t.Context(),
				futureHeaderEvent(102, arrival),
			)
			require.NoError(t, err)
			require.False(t, accepted)
			require.Empty(t, *waits)
		},
	)

	t.Run("slot past the forecast horizon is deferred", func(t *testing.T) {
		arrival := systemStart.Add(100 * time.Second)
		ls, waits := newFutureHeaderTestLedger(t, systemStart, arrival)
		ls.slotClock.provider = pastHorizonSlotTimeProvider{
			SlotTimeProvider: ls.slotClock.provider,
			rejectedSlot:     10_000,
		}

		accepted, err := ls.AwaitChainsyncHeaderAdmission(t.Context(),
			futureHeaderEvent(10_000, arrival),
		)
		require.NoError(t, err)
		require.True(t, accepted)
		require.Empty(t, *waits)
	})

	t.Run(
		"processing delay does not change arrival judgment",
		func(t *testing.T) {
			arrival := systemStart.Add(101 * time.Second)
			processTime := systemStart.Add(103 * time.Second)
			ls, waits := newFutureHeaderTestLedger(t, systemStart, processTime)

			accepted, err := ls.AwaitChainsyncHeaderAdmission(t.Context(),
				futureHeaderEvent(102, arrival),
			)
			require.NoError(t, err)
			require.True(t, accepted)
			require.Empty(t, *waits)
		},
	)

	t.Run(
		"historical catch-up does not convert queued arrival time",
		func(t *testing.T) {
			arrival := systemStart.Add(1_000_000 * time.Second)
			ls, waits := newFutureHeaderTestLedger(t, systemStart, arrival)
			ls.slotClock.provider = arrivalPastHorizonSlotTimeProvider{
				SlotTimeProvider: ls.slotClock.provider,
			}

			accepted, err := ls.AwaitChainsyncHeaderAdmission(t.Context(),
				futureHeaderEvent(900, arrival),
			)
			require.NoError(t, err)
			require.True(t, accepted)
			require.Empty(t, *waits)
		},
	)

	t.Run(
		"synthetic event without arrival remains compatible",
		func(t *testing.T) {
			now := systemStart.Add(100 * time.Second)
			ls, waits := newFutureHeaderTestLedger(t, systemStart, now)

			accepted, err := ls.AwaitChainsyncHeaderAdmission(t.Context(),
				futureHeaderEvent(10_000, time.Time{}),
			)
			require.NoError(t, err)
			require.True(t, accepted)
			require.Empty(t, *waits)
		},
	)
}

func TestAwaitChainsyncHeaderAdmissionPropagatesCancellation(t *testing.T) {
	t.Parallel()

	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	ls, _ := newFutureHeaderTestLedger(t, systemStart, arrival)
	ls.slotClock.waitFunc = func(context.Context, time.Duration) error {
		return context.Canceled
	}

	accepted, err := ls.AwaitChainsyncHeaderAdmission(
		t.Context(),
		futureHeaderEvent(101, arrival),
	)
	require.False(t, accepted)
	require.ErrorIs(t, err, context.Canceled)
}

func TestAwaitChainsyncHeaderAdmissionFailsClosedOnNilContext(t *testing.T) {
	t.Parallel()

	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	ls, _ := newFutureHeaderTestLedger(t, systemStart, arrival)

	accepted, err := ls.AwaitChainsyncHeaderAdmission(
		nil,
		futureHeaderEvent(101, arrival),
	)
	require.False(t, accepted)
	require.EqualError(t, err, "chainsync header admission context is nil")
}

func TestAwaitChainsyncHeaderAdmissionFailsClosedOnConversionError(
	t *testing.T,
) {
	t.Parallel()

	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	ls, waits := newFutureHeaderTestLedger(t, systemStart, arrival)
	wantErr := errors.New("slot conversion unavailable")
	ls.slotClock.provider = failingSlotTimeProvider{
		SlotTimeProvider: ls.slotClock.provider,
		rejectedSlot:     101,
		err:              wantErr,
	}

	accepted, err := ls.AwaitChainsyncHeaderAdmission(
		t.Context(),
		futureHeaderEvent(101, arrival),
	)
	require.False(t, accepted)
	require.ErrorIs(t, err, wantErr)
	require.Empty(t, *waits)
}

func TestFutureHeaderWaitDoesNotHoldChainsyncMutex(t *testing.T) {
	t.Parallel()

	systemStart := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	arrival := systemStart.Add(100 * time.Second)
	ls, _ := newFutureHeaderTestLedger(t, systemStart, arrival)
	waiting := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	ls.slotClock.waitFunc = func(context.Context, time.Duration) error {
		close(waiting)
		<-release
		return nil
	}
	go func() {
		e := futureHeaderEvent(101, arrival)
		accepted, err := ls.AwaitChainsyncHeaderAdmission(t.Context(), e)
		if err == nil && !accepted {
			err = errors.New("header was not accepted")
		}
		done <- err
	}()

	<-waiting
	mutexAvailable := ls.chainsyncMutex.TryLock()
	if mutexAvailable {
		ls.chainsyncMutex.Unlock()
	}
	close(release)
	require.NoError(t, <-done)
	require.True(t, mutexAvailable,
		"peer-local slot wait must not hold the node-wide chainsync mutex")
}

func TestAwaitChainsyncHeaderAdmissionUsesCrossEraSlotOnset(t *testing.T) {
	t.Parallel()

	ls := crossEraLedger(t)
	provider := newSlotTimeConverterProvider(ls.timeConv())
	clock := NewSlotClock(provider, DefaultSlotClockConfig())
	boundary, err := provider.SlotToTime(200)
	require.NoError(t, err)
	arrival := boundary.Add(-defaultHeaderClockSkew)
	clock.nowFunc = func() time.Time { return arrival }
	var waited time.Duration
	clock.waitFunc = func(_ context.Context, delay time.Duration) error {
		waited = delay
		return nil
	}
	ls.slotClock = clock
	ls.ctx = t.Context()

	accepted, err := ls.AwaitChainsyncHeaderAdmission(
		t.Context(),
		futureHeaderEvent(200, arrival),
	)
	require.NoError(t, err)
	require.True(t, accepted)
	require.Equal(t, defaultHeaderClockSkew, waited)
}

// Both Byron header kinds share the peer admission gate, before PBFT validation.
// Twenty-second slots make a one-slot lead insufficient to decide clock skew.
func TestByronHeaderAdmissionClockSkew(t *testing.T) {
	t.Parallel()
	main := &byron.ByronMainBlockHeader{}
	main.ConsensusData.SlotId.Epoch = 1
	ebb := &byron.ByronEpochBoundaryBlockHeader{}
	ebb.ConsensusData.Epoch = 1
	start := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	blocks := map[string]gledger.Block{
		"main": &byron.ByronMainBlock{BlockHeader: main},
		"ebb":  &byron.ByronEpochBoundaryBlock{BlockHeader: ebb},
	}
	for name, block := range blocks {
		header := block.Header()
		t.Run(name, func(t *testing.T) {
			for _, early := range []time.Duration{1999 * time.Millisecond, 2 * time.Second, 2001 * time.Millisecond} {
				t.Run(early.String(), func(t *testing.T) {
					provider := newMockSlotTimeProvider(
						start,
						20*time.Second,
						21600,
					)
					onset, err := provider.SlotToTime(header.SlotNumber())
					require.NoError(t, err)
					arrival := onset.Add(-early)
					ls, _ := newFutureHeaderTestLedger(t, start, arrival)
					ls.slotClock.provider = provider
					now := arrival
					ls.slotClock.nowFunc = func() time.Time { return now }
					require.Error(
						t,
						ls.validateByronPBFTCurrentSlot(block),
						"PBFT must not apply a future block, even within the allowance",
					)
					waiting := make(chan time.Duration, 1)
					release := make(chan struct{})
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					ls.slotClock.waitFunc = func(ctx context.Context, delay time.Duration) error {
						waiting <- delay
						select {
						case <-release:
							now = onset
							return nil
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					type result struct {
						accepted bool
						err      error
					}
					done := make(chan result, 1)
					go func() {
						accepted, err := ls.AwaitChainsyncHeaderAdmission(
							ctx,
							ChainsyncEvent{
								BlockHeader: header, ArrivalTime: arrival,
								Point: ocommon.NewPoint(
									header.SlotNumber(),
									nil,
								),
							},
						)
						done <- result{accepted, err}
					}()
					if early <= 2*time.Second {
						delay := testutil.RequireReceive(
							t,
							waiting,
							time.Second,
							"Byron header must be deferred until slot onset",
						)
						require.Equal(t, early, delay)
						select {
						case <-done:
							t.Fatal(
								"future Byron header admitted before slot onset",
							)
						default:
						}
						close(release)
					}
					got := testutil.RequireReceive(
						t,
						done,
						time.Second,
						"Byron admission result",
					)
					require.NoError(t, got.err)
					require.Equal(t, early <= 2*time.Second, got.accepted)
					if got.accepted {
						require.NoError(
							t,
							ls.validateByronPBFTCurrentSlot(block),
						)
						require.Equal(
							t,
							onset,
							now,
							"admission must finish only at slot onset",
						)
					} else {
						require.Empty(t, waiting, "beyond-skew headers must be rejected without waiting")
					}
				})
			}
		})
	}
}

func TestByronCurrentSlotUsesHeaderOnset(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC)
	header := &byron.ByronEpochBoundaryBlockHeader{}
	block := &byron.ByronEpochBoundaryBlock{BlockHeader: header}
	ls, _ := newFutureHeaderTestLedger(t, start, start.Add(-time.Millisecond))
	// Before genesis, TimeToSlot clamps to zero. Slot equality must not permit
	// the epoch-zero EBB to be applied before its wall-clock onset.
	require.Error(
		t,
		ls.validateByronPBFTCurrentSlot(block),
		"epoch-zero EBB must not apply before system start",
	)
	ls.slotClock.nowFunc = func() time.Time { return start }
	require.NoError(t, ls.validateByronPBFTCurrentSlot(block))
	// A known historical onset remains usable when the current time is beyond
	// the forecast horizon; validating it must not require forecasting now.
	ls.slotClock.provider = arrivalPastHorizonSlotTimeProvider{
		ls.slotClock.provider,
	}
	require.NoError(t, ls.validateByronPBFTCurrentSlot(block))
}

// observeProcessEpochRolloverCallOrder parses chainsync.go, locates the body of
// targetFunc, and reports the source-order appearance of the calls named in
// wantOrder. It returns seen (which markers were found) and observed (the
// markers in source order by first occurrence). Only the markers present in
// wantOrder are considered.
func observeProcessEpochRolloverCallOrder(
	t *testing.T,
	targetFunc string,
	wantOrder []string,
) (seen map[string]bool, observed []string) {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(
		fset,
		"chainsync.go",
		nil,
		parser.SkipObjectResolution,
	)
	require.NoError(t, err, "parse chainsync.go")

	var fnDecl *ast.FuncDecl
	for _, decl := range file.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fd.Name.Name == targetFunc {
			fnDecl = fd
			break
		}
	}
	require.NotNil(t, fnDecl, "%s not found in chainsync.go", targetFunc)
	require.NotNil(t, fnDecl.Body, "%s has no body", targetFunc)

	// Walk the function body, recording the source position of each
	// call whose final identifier is in our marker set.
	wanted := make(map[string]struct{}, len(wantOrder))
	for _, m := range wantOrder {
		wanted[m] = struct{}{}
	}
	type hit struct {
		marker string
		pos    token.Pos
	}
	var hits []hit
	ast.Inspect(fnDecl.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		var name string
		switch fn := call.Fun.(type) {
		case *ast.SelectorExpr:
			name = fn.Sel.Name
		case *ast.Ident:
			name = fn.Name
		default:
			return true
		}
		if _, ok := wanted[name]; ok {
			hits = append(hits, hit{marker: name, pos: call.Pos()})
		}
		return true
	})

	// Build the observed order by first occurrence (a marker can appear
	// in nested branches; we only care about its first appearance).
	seen = make(map[string]bool, len(wantOrder))
	for _, h := range hits {
		if seen[h.marker] {
			continue
		}
		seen[h.marker] = true
		observed = append(observed, h.marker)
	}
	return seen, observed
}

// TestProcessEpochRollover_OrderingInvariant pins the EPOCH→HARDFORK call
// sequence inside processEpochRollover. The order matters for correctness
// (see the long-form comment at the top of processEpochRollover's body)
// but is otherwise hard to test through observable side effects without
// substantial governance + pparams + cert fixture setup.
//
// This is a structural lock-in test: it parses chainsync.go's AST, locates
// the body of processEpochRollover, and asserts that the named call
// expressions appear in the expected source order. A future refactor that
// reorders these calls — even if all unit tests pass — will fail this
// test loudly. The error message points to the comment block that
// documents the invariant.
//
// Markers are matched by suffix on the call's selector expression so that
// receiver renames (`ls.db` → `ls.metadata`) don't churn the test.
func TestProcessEpochRollover_OrderingInvariant(t *testing.T) {
	t.Parallel()

	const targetFunc = "processEpochRollover"

	// In source order, the calls that must appear inside processEpochRollover.
	// Each entry is the trailing identifier of a SelectorExpr (or a bare
	// Ident for unqualified calls).
	//
	// applyMIRCerts precedes applyPoolRetirements because that is the reference
	// sequence: Shelley's NEWEPOCH rule embeds MIR between applyRUpd and EPOCH,
	// and EPOCH's own sub-rules are SNAP then POOLREAP. MIR is therefore both
	// pre-SNAP (its credits belong in the mark snapshot) and pre-POOLREAP (its
	// pot movements are visible to the deposit refunds). dingo ran POOLREAP
	// before MIR until the epoch-boundary snapshot semantics were corrected.
	wantOrder := []string{
		"applyMIRCerts",                       // (1) Shelley-era INSTANT rule, pre-SNAP
		"ComputeAndApplyPParamUpdates",        // (2) Shelley-style pparam updates
		"applyPoolRetirements",                // (3) embedded POOLREAP deposit refunds
		"activateDelegatorInactivityIfNeeded", // (4) CIP-0163 activation
		"ProcessEpoch",                        // (5) Conway-style governance enact
		"SetPParams",                          // (6) persist enacted pparams
		"isHardForkTransition",                // (7) inter-era boundary detection
		"applyIntraEraHardForkRule",           // (8) per-major-version HARDFORK rule
	}

	seen, observed := observeProcessEpochRolloverCallOrder(
		t,
		targetFunc,
		wantOrder,
	)

	for _, m := range wantOrder {
		require.True(t, seen[m],
			"marker %q not found in %s body — was it renamed or removed? "+
				"If renamed, update wantOrder. If removed, also revisit "+
				"the EPOCH→HARDFORK ordering comment in chainsync.go.",
			m, targetFunc)
	}

	// observedFiltered is observed restricted to the wanted markers, in
	// observed order. (Equivalent to observed today since wanted gates
	// inclusion above, but keeps the assertion intent explicit.)
	observedFiltered := observed
	require.Equal(t, wantOrder, observedFiltered,
		"call sequence in %s drifted from the EPOCH→HARDFORK ordering "+
			"invariant. Expected %v, observed %v. See the comment block "+
			"at the top of the function body for the rationale; if the "+
			"reorder is intentional, update both the comment and "+
			"wantOrder in this test.",
		targetFunc, wantOrder, observedFiltered)
}

// TestProcessEpochRollover_RewardOrdering pins the placement of the stake
// reward application and the ADA-pot capture relative to the rest of the
// epoch boundary. The delayed reward update (applyStakeRewards) must run
// before governance reads the treasury, and the ADA-pot snapshot
// (saveRewardAdaPotsForEpoch) must run after every boundary treasury/reserves
// mutation so it observes the fully settled pots. It uses the same structural
// AST approach as TestProcessEpochRollover_OrderingInvariant so a reorder
// fails loudly even when unit tests pass.
func TestProcessEpochRollover_RewardOrdering(t *testing.T) {
	t.Parallel()

	const targetFunc = "processEpochRollover"

	// In source order: reward application first, then the governance/pparam
	// core, then the ADA-pot capture last.
	wantOrder := []string{
		"applyStakeRewards",            // (1) delayed reward update, pre-governance
		"ComputeAndApplyPParamUpdates", // pparam updates
		"ProcessEpoch",                 // governance enact (reads treasury)
		"applyIntraEraHardForkRule",    // last treasury/reserves mutation
		"saveRewardAdaPotsForEpoch",    // (last) post-boundary ADA pot capture
	}

	seen, observed := observeProcessEpochRolloverCallOrder(
		t,
		targetFunc,
		wantOrder,
	)

	for _, m := range wantOrder {
		require.True(t, seen[m],
			"reward marker %q not found in %s body — was it renamed or "+
				"removed? Stake reward application and ADA-pot capture must "+
				"stay wired into the epoch rollover.",
			m, targetFunc)
	}

	require.Equal(t, wantOrder, observed,
		"reward call sequence in %s drifted. applyStakeRewards must precede "+
			"governance and saveRewardAdaPotsForEpoch must follow every "+
			"boundary treasury/reserves mutation. Expected %v, observed %v.",
		targetFunc, wantOrder, observed)
}

// TestHandleEventChainsyncRollbackToBlockTipDoesNotPublishLedgerRollback
// captures the wedge described above. After the
// "fork extends from current tip" branch queues headers, the chain's
// header tip sits ahead of the block tip. If the peer then sends a
// RollBackward to the block tip, the no-op shortcut in
// handleEventChainsyncRollback compares the rollback point against
// HeaderTip and falls through to rollbackChainAndStateDeferred, which calls
// ls.rollback and publishes a spurious "local ledger rollback" event
// even though the ledger never advanced past the block tip in the
// first place. That event drives RecoverAfterLocalRollback every cycle,
// matching the "replayed peer header history after local rollback"
// log line that fires every plateau cycle in the field reports.
func TestHandleEventChainsyncRollbackToBlockTipDoesNotPublishLedgerRollback(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	resyncCh := make(chan event.ChainsyncResyncEvent, 4)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	// Simulate the "fork extends from current tip" branch having
	// queued a peer header whose prevHash matches the current block
	// tip. The header sits in the chain's header queue, so HeaderTip
	// is ahead of Tip while the ledger has not yet advanced.
	forkHeader := mockHeader{
		hash: lcommon.NewBlake2b256(
			testHashBytes("pinned-tip-fork-extend-1"),
		),
		prevHash:    lcommon.NewBlake2b256(fixture.currentTip.Point.Hash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 5,
	}
	require.NoError(t, fixture.ls.chain.AddBlockHeader(forkHeader))
	require.Equal(t, 1, fixture.ls.chain.HeaderCount())
	require.NotEqual(
		t,
		fixture.ls.chain.Tip().Point.Slot,
		fixture.ls.chain.HeaderTip().Point.Slot,
		"setup invariant: header tip must be ahead of block tip",
	)

	// Peer sends RollBackward to the block tip. The ledger is
	// already at the block tip — there is nothing for the ledger to
	// roll back. Only the queued header needs to be cleared.
	require.NoError(t, fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.currentTip.Point,
		},
		nil,
	))

	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Zero(t, fixture.ls.chain.HeaderCount())

	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"no local ledger rollback event should be published when "+
			"rolling back to a point the ledger already sits at",
	)
}

// TestRollbackAtCurrentTipIsNoop pins the ls.rollback contract that
// the no-op fix relies on: calling rollback with the point the ledger
// already sits at must neither mutate state nor publish a "local
// ledger rollback" resync event. This complements the chainsync-level
// test above by exercising ls.rollback directly, so future callers
// (replay recovery, iterator rollback, reconcile paths) inherit the
// same guarantee.
func TestRollbackAtCurrentTipIsNoop(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	resyncCh := make(chan event.ChainsyncResyncEvent, 4)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	preSeq := fixture.ls.lastLocalRollbackSeq
	require.NoError(t, fixture.ls.rollback(fixture.currentTip.Point))

	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
	assert.Equal(
		t,
		preSeq,
		fixture.ls.lastLocalRollbackSeq,
		"lastLocalRollbackSeq should not advance for a no-op rollback",
	)
	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"no local ledger rollback event should fire when "+
			"rolling back to the existing currentTip",
	)
}

// TestShouldVerifyChainsyncHeaderCryptoKeepsAdmissionGate verifies that a
// later pipeline stage does not weaken the chainsync admission gate. Once a
// header is queued, the corresponding block can be persisted and served
// before the ledger reader reaches the pipeline validation stage.
func TestShouldVerifyChainsyncHeaderCryptoKeepsAdmissionGate(
	t *testing.T,
) {
	t.Parallel()

	tb := createTestBlock(t, [32]byte{60}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.validationEnabled = true
	ls.publishSnapshotsLocked()

	slot := tb.block.SlotNumber()
	verifyNow, _ := ls.chainsyncHeaderCryptoPolicy(slot)
	require.True(
		t,
		verifyNow,
		"sanity: the crypto pre-check runs without a validating pipeline",
	)

	ls.blockPipeline = pipeline.NewBlockPipeline()
	ls.config.BlockPipelineValidateEnabled = true
	verifyNow, _ = ls.chainsyncHeaderCryptoPolicy(slot)
	assert.True(
		t,
		verifyNow,
		"pipeline validation must not replace the admission-time crypto gate",
	)
}

// TestHandleEventBlockfetchBlockKeepsAdmissionCryptoWhenPipelineValidates
// proves the blockfetch-time admission gate remains fail-closed when the
// later pipeline validate stage is enabled.
// The test block's VRF proof is deliberately tampered (a genuine crypto
// failure, not a deferred one) while the ledger tip is left one slot behind
// the block so that the non-crypto state checks
// (verifyRegisteredVrfKey/verifyBlockLeaderEligibility) defer rather than
// hard-fail -- matching
// TestVerifyBlockHeaderCryptoBeforeApplyDefersMissingPoolState's fixture.
//
// Without a validating pipeline, handleEventBlockfetchBlock must still catch
// the tampered VRF proof directly (a real, non-deferred error). The pipeline
// cannot replace this gate because the block becomes visible to downstream
// readers before its later validate stage runs.
func TestHandleEventBlockfetchBlockKeepsAdmissionCryptoWhenPipelineValidates(
	t *testing.T,
) {
	t.Parallel()

	tb := createTestBlock(t, [32]byte{61}, 0, tamperVRFProof)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.validationEnabled = true
	ls.currentTip.Point.Slot = tb.block.SlotNumber() - 1
	ls.chain = &chain.Chain{}
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	ls.activeBlockfetchConnId = connId
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.publishSnapshotsLocked()

	evt := BlockfetchEvent{
		ConnectionId: connId,
		Block:        tb.block,
		Point: ocommon.Point{
			Slot: tb.block.SlotNumber(),
			Hash: tb.block.Hash().Bytes(),
		},
	}

	// Without a validating pipeline, the tampered VRF proof is caught
	// directly here as a genuine (non-deferred) crypto failure.
	err := handleEventBlockfetchBlockDeferred(ls, evt, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "crypto verification failed")
	assert.Empty(t, ls.pendingBlockfetchEvents)

	// Reset the per-block dedup/bookkeeping state the failed call above
	// touched, then enable the pipeline's validate stage. The identical
	// tampered block must still be rejected before persistence.
	ls.pendingBlockfetchEvents = nil
	ls.shadowBlockReceivedHashes = nil
	ls.blockPipeline = pipeline.NewBlockPipeline()
	ls.config.BlockPipelineValidateEnabled = true

	err = handleEventBlockfetchBlockDeferred(ls, evt, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "crypto verification failed")
	assert.Empty(t, ls.pendingBlockfetchEvents)

	var (
		rejectedType uint
		rejectedRaw  []byte
	)
	ls.config.RejectBlockDecodeCacheFunc = func(blockType uint, raw []byte) {
		rejectedType = blockType
		rejectedRaw = append([]byte(nil), raw...)
	}
	ls.shadowBlockReceivedHashes = nil
	evt.RawBlock = []byte{0x84, 0x01}
	ls.handleEventBlockfetch(event.NewEvent(BlockfetchEventType, evt))
	assert.Equal(t, evt.Type, rejectedType)
	assert.Equal(t, evt.RawBlock, rejectedRaw)
}

// TestHandleEventBlockfetchBlockRecordsAdmissionVerification pins that only a
// block whose header verification completed is recorded for replay to skip; a
// deferred one is not.
func TestHandleEventBlockfetchBlockRecordsAdmissionVerification(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		seedPool     bool
		wantVerified bool
	}{
		{"verified at admission", true, true},
		{"deferred at admission", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tb := createTestBlock(t, [32]byte{67}, 0, tamperNone)
			ls, db := newEligibilityTestLedger(t, tb.epochNonce)
			if tc.seedPool {
				poolKeyHash := tb.block.IssuerVkey().Hash()
				seedBlockPoolRegistration(t, db, tb.block)
				seedPoolStakeSnapshot(t, db, 4, poolKeyHash[:], 1_000_000_000)
			}
			ls.validationEnabled = true
			ls.currentTip.Point.Slot = tb.block.SlotNumber() - 1
			ls.chain = &chain.Chain{}
			connId := ouroboros.ConnectionId{
				LocalAddr: &net.TCPAddr{
					IP: net.ParseIP("127.0.0.1"), Port: 6002,
				},
				RemoteAddr: &net.TCPAddr{
					IP: net.ParseIP("127.0.0.1"), Port: 3001,
				},
			}
			ls.activeBlockfetchConnId = connId
			ls.chainsyncBlockfetchReadyChan = make(chan struct{})
			ls.config.BlockPipelineValidateEnabled = true
			ls.publishSnapshotsLocked()

			require.NoError(t, handleEventBlockfetchBlockDeferred(
				ls,
				BlockfetchEvent{
					ConnectionId: connId,
					Block:        tb.block,
					Point: ocommon.Point{
						Slot: tb.block.SlotNumber(),
						Hash: tb.block.Hash().Bytes(),
					},
				},
				nil,
			))
			assert.Equal(
				t,
				tc.wantVerified,
				ls.admissionVerifiedSlot(tb.block.SlotNumber()),
			)
			if !tc.wantVerified {
				assert.Equal(
					t,
					connId,
					ls.deferredHeaderSource(ocommon.NewPoint(
						tb.block.SlotNumber(),
						tb.block.Hash().Bytes(),
					)),
					"a deferred marker must name the supplying connection",
				)
			}
		})
	}
}

func TestHandleEventBlockfetchBlockRejectsInvalidOpCertWhenPipelineValidates(
	t *testing.T,
) {
	t.Parallel()

	tb := createTestBlock(t, [32]byte{62}, 0, tamperOpCertSig)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.validationEnabled = true
	ls.currentTip.Point.Slot = tb.block.SlotNumber() - 1
	ls.chain = &chain.Chain{}
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6001},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	ls.activeBlockfetchConnId = connId
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.blockPipeline = pipeline.NewBlockPipeline()
	ls.config.BlockPipelineValidateEnabled = true
	ls.publishSnapshotsLocked()

	err := handleEventBlockfetchBlockDeferred(ls, BlockfetchEvent{
		ConnectionId: connId,
		Block:        tb.block,
		Point: ocommon.Point{
			Slot: tb.block.SlotNumber(),
			Hash: tb.block.Hash().Bytes(),
		},
	}, nil)
	require.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"operational certificate cold signature invalid",
	)
	assert.Empty(t, ls.pendingBlockfetchEvents)
}

// setOpCertSequenceNumber writes an operational certificate counter into a
// gouroboros header's field whatever width that release declares it at.
// cardano-ledger decodes the counter as Word64, so the test's own values are
// uint64; the assignment is written this way rather than as a composite
// literal so it does not have to be edited when the header type's width
// changes.
func setOpCertSequenceNumber[T uint32 | uint64](dst *T, value uint64) {
	*dst = T(value) //nolint:gosec // test-only counters are small
}

// newTestDijkstraBlockCbor builds a minimal, decodable Dijkstra block (empty
// body, plain Babbage-shaped header) and returns its CBOR. The body hash is
// computed from the actual empty body so NewDijkstraBlockFromCbor's
// body-hash check (run by models.Block.Decode via ledger.NewBlockFromCbor)
// passes; header signature/KES verification is not exercised at decode time.
func newTestDijkstraBlockCbor(
	t *testing.T,
	slot, blockNumber uint64,
	issuerFirstByte byte,
	opCertSeqNo uint64,
	vrfOutput []byte,
) []byte {
	t.Helper()
	body := dijkstra.DijkstraBlockBody{}
	var issuer lcommon.IssuerVkey
	issuer[0] = issuerFirstByte
	header := &dijkstra.DijkstraBlockHeader{
		BabbageBlockHeader: babbage.BabbageBlockHeader{
			Body: babbage.BabbageBlockHeaderBody{
				BlockNumber:   blockNumber,
				Slot:          slot,
				IssuerVkey:    issuer,
				VrfResult:     lcommon.VrfResult{Output: vrfOutput},
				BlockBodyHash: body.Hash(),
			},
		},
	}
	setOpCertSequenceNumber(
		&header.Body.OpCert.SequenceNumber,
		opCertSeqNo,
	)
	block := &dijkstra.DijkstraBlock{BlockHeader: header, BlockBody: body}
	cborData, err := block.MarshalCBOR()
	require.NoError(t, err)
	return cborData
}

// insertTestDijkstraBlock stores a Dijkstra block built by
// newTestDijkstraBlockCbor at the given hash so localTipPraosView's
// database.BlockByHash + Decode round trip can find and decode it.
func insertTestDijkstraBlock(
	t *testing.T,
	db *database.Database,
	slot, blockNumber uint64,
	hash []byte,
	issuerFirstByte byte,
	opCertSeqNo uint64,
	vrfOutput []byte,
) {
	t.Helper()
	cborData := newTestDijkstraBlockCbor(
		t, slot, blockNumber, issuerFirstByte, opCertSeqNo, vrfOutput,
	)
	require.NoError(t, db.BlockCreate(models.Block{
		Slot:   slot,
		Hash:   hash,
		Cbor:   cborData,
		Type:   dijkstra.BlockTypeDijkstra,
		Number: blockNumber,
	}, nil))
}

// TestCompareIncomingHeaderToLocalTip_Dijkstra exercises the actual
// chain-selection caller (ledger/chainsync.go's compareIncomingHeaderToLocalTip,
// the mechanism identified as reachable from
// ledger/chainsync.go:1855-1904 and ouroboros/chainsync.go:758) end to end for
// Dijkstra headers: the local tip is a real Dijkstra block round-tripped
// through storage (database.BlockByHash -> models.Block.Decode), and the
// incoming header is a plain in-process *dijkstra.DijkstraBlockHeader as
// chainsync delivers it. Before the view.go fix this always resolved
// ChainEqual for a Dijkstra local tip (GetPraosTiebreakerView returned
// ok=false), silently disarming the VRF tiebreaker in exactly this path.
func TestCompareIncomingHeaderToLocalTip_Dijkstra(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	const blockNumber = 50
	localHash := []byte("dijkstra-local-tip-hash-32-bytes")[:32]
	localSlot := uint64(200)
	localTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: localSlot, Hash: localHash},
		BlockNumber: blockNumber,
	}

	testCases := []struct {
		name          string
		localVRF      []byte
		incomingVRF   []byte
		incomingSlot  uint64
		expectedBeats praos.ChainComparisonResult
	}{
		{
			name:          "incoming lower VRF beats local tip",
			localVRF:      make64ByteVRFFirstByteLedger(0xFF),
			incomingVRF:   make64ByteVRFFirstByteLedger(0x01),
			incomingSlot:  202,
			expectedBeats: praos.ChainABetter,
		},
		{
			name:          "incoming higher VRF loses to local tip",
			localVRF:      make64ByteVRFFirstByteLedger(0x01),
			incomingVRF:   make64ByteVRFFirstByteLedger(0xFF),
			incomingSlot:  202,
			expectedBeats: praos.ChainBBetter,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Re-seed the local tip block for each subtest under a
			// case-specific hash so BlockByHash resolves the intended VRF.
			hash := append([]byte(nil), localHash...)
			hash[len(hash)-1] = byte(len(tc.name)) // vary hash per case
			localTip := localTip
			localTip.Point.Hash = hash
			insertTestDijkstraBlock(
				t, db, localSlot, blockNumber, hash,
				0xAB, 3, tc.localVRF,
			)

			incomingHeader := &dijkstra.DijkstraBlockHeader{
				BabbageBlockHeader: babbage.BabbageBlockHeader{
					Body: babbage.BabbageBlockHeaderBody{
						BlockNumber: blockNumber,
						Slot:        tc.incomingSlot,
						VrfResult:   lcommon.VrfResult{Output: tc.incomingVRF},
						OpCert: babbage.BabbageOpCert{
							SequenceNumber: 3,
						},
					},
				},
			}
			event := ChainsyncEvent{
				BlockHeader: incomingHeader,
				Point: ocommon.Point{
					Slot: tc.incomingSlot,
					Hash: []byte("incoming-hash"),
				},
			}

			result := ls.compareIncomingHeaderToLocalTip(event, localTip)
			require.Equal(
				t,
				tc.expectedBeats,
				result,
				"Dijkstra local tip must participate in the VRF tiebreaker through the real storage/decode path",
			)
		})
	}
}

// make64ByteVRFFirstByteLedger mirrors consensus/praos's test helper of the
// same shape; duplicated here since it is unexported in another package.
func make64ByteVRFFirstByteLedger(first byte) []byte {
	vrf := make([]byte, praos.VRFOutputSize)
	vrf[0] = first
	return vrf
}

func testRecycleConnId() ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
	}
}

// TestChainsyncHeaderVerificationFailurePublishesRecycleEvent verifies that
// a header crypto verification failure on the chainsync path publishes a
// ledger.ConnectionRecycleRequestedEvent with reason "header_verification_failure".
func TestChainsyncHeaderVerificationFailurePublishesRecycleEvent(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	connId := testRecycleConnId()

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	ls := &LedgerState{
		validationEnabled: true,
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 2_000,
				Nonce:         make([]byte, 32),
			},
		},
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
		},
	}
	ls.publishSnapshotsLocked()

	err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId,
		BlockHeader:  mockHeader{slot: 1000, blockNumber: 100},
		Point:        ocommon.Point{Slot: 1000},
	})
	require.Error(t, err)

	got := testutil.RequireReceive(
		t,
		recycled,
		testutil.AsyncWait,
		"recycle event not published",
	)
	assert.Equal(t, connId, got.ConnectionId)
	assert.Equal(t, "header_verification_failure", got.Reason)
}

func TestChainsyncHeaderVerificationMissingEpochDefersToBlockfetch(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	connId := testRecycleConnId()

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	cm, err := chain.NewManager(nil, nil)
	require.NoError(t, err)
	testChain := cm.PrimaryChain()
	ls := &LedgerState{
		validationEnabled: true,
		chain:             testChain,
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
			BlockfetchRequestRangeFunc: func(
				ouroboros.ConnectionId,
				ocommon.Point,
				ocommon.Point,
			) (uint64, error) {
				return 0, nil
			},
		},
	}
	ls.publishSnapshotsLocked()
	t.Cleanup(func() {
		if ls.chainsyncBlockfetchTimeoutTimer != nil {
			ls.chainsyncBlockfetchTimeoutTimer.Stop()
		}
	})
	header := mockHeader{slot: 1000, blockNumber: 100}
	point := ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())

	err = ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId,
		BlockHeader:  header,
		Point:        point,
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				point.Slot+1,
				[]byte("unbound-tip"),
			),
			BlockNumber: header.BlockNumber() + 1,
		},
	})
	require.NoError(t, err)

	testutil.RequireNoReceive(
		t,
		recycled,
		100*time.Millisecond,
		"missing epoch should defer header verification, not recycle peer",
	)
	assert.True(t, testChain.FirstHeaderMatchesPoint(point))
	assert.False(t, testChain.FirstVerifiedHeaderMatchesPoint(point))
	assert.Zero(t, ls.syncUpstreamTipSlot.Load())
}

func TestChainsyncHeaderVerificationEmptyEpochNonceDefersToBlockfetch(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	connId := testRecycleConnId()

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	requested := make(chan ocommon.Point, 1)
	cm, err := chain.NewManager(nil, nil)
	require.NoError(t, err)
	testChain := cm.PrimaryChain()
	ls := &LedgerState{
		validationEnabled: true,
		chain:             testChain,
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 2_000,
			},
		},
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
			BlockfetchRequestRangeFunc: func(
				_ ouroboros.ConnectionId,
				start ocommon.Point,
				_ ocommon.Point,
			) (uint64, error) {
				requested <- start
				return 0, nil
			},
		},
	}
	ls.publishSnapshotsLocked()
	t.Cleanup(func() {
		if ls.chainsyncBlockfetchTimeoutTimer != nil {
			ls.chainsyncBlockfetchTimeoutTimer.Stop()
		}
	})
	header := mockHeader{slot: 1000, blockNumber: 100}
	point := ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())

	err = ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId,
		BlockHeader:  header,
		Point:        point,
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				point.Slot+1,
				[]byte("unbound-tip"),
			),
			BlockNumber: header.BlockNumber() + 1,
		},
	})
	require.NoError(t, err)

	gotStart := testutil.RequireReceive(
		t,
		requested,
		testutil.AsyncWait,
		"empty nonce should start blockfetch for deferred verification",
	)
	assert.Equal(t, point, gotStart)
	testutil.RequireNoReceive(
		t,
		recycled,
		100*time.Millisecond,
		"empty nonce should defer header verification, not recycle peer",
	)
	assert.True(t, testChain.FirstHeaderMatchesPoint(point))
	assert.False(t, testChain.FirstVerifiedHeaderMatchesPoint(point))
	assert.Zero(t, ls.syncUpstreamTipSlot.Load())
}

func TestChainsyncHeaderVerificationMithrilCoverageAdvancesFrontier(
	t *testing.T,
) {
	t.Parallel()

	header := mockHeader{slot: 1000, blockNumber: 100}
	point := ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())
	testChain := &chain.Chain{}
	ls := &LedgerState{
		validationEnabled:            true,
		mithrilLedgerSlot:            point.Slot,
		chain:                        testChain,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	require.NoError(t, ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: testRecycleConnId(),
		BlockHeader:  header,
		Point:        point,
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(point.Slot+1, []byte("peer-tip")),
			BlockNumber: header.BlockNumber() + 1,
		},
	}))

	assert.True(t, testChain.FirstHeaderMatchesPoint(point))
	assert.False(t, testChain.FirstVerifiedHeaderMatchesPoint(point))
	assert.Equal(t, point.Slot, ls.syncUpstreamTipSlot.Load())
}

// TestBlockfetchHeaderVerificationFailurePublishesRecycleEvent verifies that
// a block header crypto verification failure on the blockfetch path publishes a
// ledger.ConnectionRecycleRequestedEvent with reason "block_header_verification_failure".
func TestBlockfetchHeaderVerificationFailurePublishesRecycleEvent(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	connId := testRecycleConnId()

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	ls := &LedgerState{
		validationEnabled:            true,
		activeBlockfetchConnId:       connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		chain:                        &chain.Chain{},
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
		},
	}
	ls.publishSnapshotsLocked()

	ls.handleEventBlockfetch(event.NewEvent(
		BlockfetchEventType,
		BlockfetchEvent{
			ConnectionId: connId,
			Block:        &mockBabbageBlock{slot: 500},
			Point:        ocommon.Point{Slot: 500, Hash: []byte("fake-hash")},
		},
	))

	got := testutil.RequireReceive(
		t,
		recycled,
		testutil.AsyncWait,
		"recycle event not published",
	)
	assert.Equal(t, connId, got.ConnectionId)
	assert.Equal(t, "block_header_verification_failure", got.Reason)
}

// TestBlockfetchHeaderVerificationRunsRegardlessOfValidationEnabled is a
// regression test for a human-review finding: no test failed if
// handleEventBlockfetchBlockDeferred's Mithril-slot gate were reverted to
// the previous validationEnabled check. made header crypto
// verification unconditional -- before it, an entire
// ValidateHistorical=false bulk-sync run skipped VRF/KES/opcert
// verification and stake-derived leader eligibility for every block. This
// proves the fail-closed behavior directly: with validationEnabled=false
// on a non-Mithril slot, a block with unverifiable header crypto still
// returns a definite (non-deferred) error instead of being silently
// accepted.
func TestBlockfetchHeaderVerificationRunsRegardlessOfValidationEnabled(
	t *testing.T,
) {
	t.Parallel()

	connId := testRecycleConnId()
	ls := &LedgerState{
		validationEnabled:            false,
		activeBlockfetchConnId:       connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		chain:                        &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	err := handleEventBlockfetchBlockDeferred(ls, BlockfetchEvent{
		ConnectionId: connId,
		Block:        &mockBabbageBlock{slot: 500},
		Point:        ocommon.Point{Slot: 500, Hash: []byte("fake-hash")},
	}, nil)

	require.Error(t, err)
	assert.False(t, IsHeaderVerificationDeferred(err))
	assert.Contains(t, err.Error(), "block header crypto verification failed")
}

// TestBlockfetchHeaderVerificationSkippedForMithrilCoveredSlot is the
// companion regression test to the one above: verification must still be
// skipped for a slot an imported Mithril snapshot already covers,
// regardless of validationEnabled -- that is the one exemption
// slotCoveredByMithril preserves. A block with unverifiable header crypto
// at a Mithril-covered slot must be accepted without error.
func TestBlockfetchHeaderVerificationSkippedForMithrilCoveredSlot(
	t *testing.T,
) {
	t.Parallel()

	const targetSlot = uint64(500)
	connId := testRecycleConnId()
	ls := &LedgerState{
		validationEnabled:            false,
		mithrilLedgerSlot:            targetSlot,
		activeBlockfetchConnId:       connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		chain:                        &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	err := handleEventBlockfetchBlockDeferred(ls, BlockfetchEvent{
		ConnectionId: connId,
		Block:        &mockBabbageBlock{slot: targetSlot},
		Point: ocommon.Point{
			Slot: targetSlot,
			Hash: []byte("fake-hash"),
		},
	}, nil)

	require.NoError(t, err)
	require.Len(t, ls.pendingBlockfetchEvents, 1)
}

func TestBlockfetchStatefulHeaderVerificationDefersUntilLedgerApply(
	t *testing.T,
) {
	t.Parallel()

	connId := testRecycleConnId()
	tb := createTestBlock(t, [32]byte{47}, 0, tamperNone)
	ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
	ls.validationEnabled = true
	ls.activeBlockfetchConnId = connId
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.chain = &chain.Chain{}

	point := ocommon.NewPoint(tb.block.SlotNumber(), tb.block.Hash().Bytes())
	err := handleEventBlockfetchBlockDeferred(ls, BlockfetchEvent{
		ConnectionId: connId,
		Block:        tb.block,
		Point:        point,
	}, nil)
	require.NoError(t, err)
	require.Len(t, ls.pendingBlockfetchEvents, 1)
	assert.True(t, ls.consumeDeferredHeaderValidation(point))
	value, err := ls.db.GetSyncState(
		deferredHeaderValidationSyncStateKey(point),
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, deferredHeaderValidationSyncStateValue, value)
}

// TestBlockfetchSkipsHeaderCryptoForVerifiedNonHeadQueuedHeader pins the
// blockfetch admission call site: a fetched block whose own header was
// crypto-verified at chainsync ingress must not have its header crypto re-run
// when that header is queued behind the head, while the same block queued
// unverified still fails. The block carries a corrupted KES signature, so only
// a skipped verification can admit it; the stateful half still runs and
// defers against the empty stake state.
func TestBlockfetchSkipsHeaderCryptoForVerifiedNonHeadQueuedHeader(
	t *testing.T,
) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		verified bool
	}{
		{name: "verified", verified: true},
		{name: "unverified", verified: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			connId := testRecycleConnId()
			tb := createTestBlock(t, [32]byte{53}, 0, tamperKESSig)
			ls, _ := newEligibilityTestLedger(t, tb.epochNonce)
			ls.validationEnabled = true
			ls.activeBlockfetchConnId = connId
			ls.chainsyncBlockfetchReadyChan = make(chan struct{})
			ls.chain = &chain.Chain{}

			fetched := tb.block.Header()
			head := mockHeader{
				hash:        fetched.PrevHash(),
				blockNumber: fetched.BlockNumber() - 1,
				slot:        fetched.SlotNumber() - 1,
			}
			require.NoError(t, ls.chain.AddVerifiedBlockHeader(head))
			if tc.verified {
				require.NoError(t, ls.chain.AddVerifiedBlockHeader(fetched))
			} else {
				require.NoError(t, ls.chain.AddBlockHeader(fetched))
			}
			point := ocommon.NewPoint(
				fetched.SlotNumber(),
				fetched.Hash().Bytes(),
			)
			require.False(t, ls.chain.FirstHeaderMatchesPoint(point))

			err := handleEventBlockfetchBlockDeferred(ls, BlockfetchEvent{
				ConnectionId: connId,
				Block:        tb.block,
				Point:        point,
			}, nil)
			if !tc.verified {
				require.Error(t, err)
				assert.Contains(
					t,
					err.Error(),
					"block header crypto verification failed",
				)
				assert.Empty(t, ls.pendingBlockfetchEvents)
				return
			}
			require.NoError(
				t,
				err,
				"verified queued header must not have its crypto re-run",
			)
			require.Len(t, ls.pendingBlockfetchEvents, 1)
			assert.True(t, ls.consumeDeferredHeaderValidation(point))
		})
	}
}

// TestBlockfetchHeaderVerificationEmptyEpochNonceDefersNotFails is a
// regression test for a human-review finding: handleEventBlockfetchBlockDeferred
// checked errors.Is(verifyErr, errHeaderVerificationDeferred) directly
// instead of the exported IsHeaderVerificationDeferred, so a covered epoch
// with no published nonce yet (errEpochNonceUnavailable, which
// IsHeaderVerificationDeferred was broadened to recognize) was still
// treated as a hard crypto failure at this call site. Unlike the chainsync
// admission gate (chainsyncHeaderCryptoPolicy), which skips calling verify
// entirely when the nonce isn't cached, this path only flushes pending
// blocks once and rechecks whether the header was verified elsewhere before
// falling through to verify regardless -- so it reaches this exact case in
// practice, and an honest peer's block would otherwise have its connection
// recycled over a transient local gap.
func TestBlockfetchHeaderVerificationEmptyEpochNonceDefersNotFails(
	t *testing.T,
) {
	t.Parallel()

	const targetSlot = uint64(1000)
	connId := testRecycleConnId()
	// A nil epoch nonce (covered epoch, nonce not yet published) rather
	// than a real one from createTestBlock: headerVerificationEpoch checks
	// epoch/nonce availability before ever touching the VRF proof, so the
	// block's own crypto content doesn't matter for this case.
	ls, _ := newEligibilityTestLedger(t, nil)
	ls.validationEnabled = true
	ls.activeBlockfetchConnId = connId
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	ls.chain = &chain.Chain{}

	block := &mockBabbageBlock{slot: targetSlot}
	point := ocommon.NewPoint(block.SlotNumber(), block.Hash().Bytes())
	err := handleEventBlockfetchBlockDeferred(ls, BlockfetchEvent{
		ConnectionId: connId,
		Block:        block,
		Point:        point,
	}, nil)
	require.NoError(
		t,
		err,
		"an unpublished epoch nonce must defer, not hard-fail, block header verification",
	)
	require.Len(t, ls.pendingBlockfetchEvents, 1)
	assert.True(t, ls.consumeDeferredHeaderValidation(point))
}

// --- non-extending-block flood demotion ---

// TestEvaluateNonExtendingBlockRejection exercises the pure threshold/window
// decision directly against synthetic timestamps, matching
// chainsyncrecycler.shouldRecycleLocalTipPlateau's test style.
func TestEvaluateNonExtendingBlockRejection(t *testing.T) {
	t.Parallel()

	now := time.Now()

	// A handful of rejections, well under the threshold, must never trigger
	// a recycle -- this is the legitimate brief-rollback-race shape: a peer
	// serves a few blocks that no longer fit while its view of a fork
	// converges with ours.
	var state nonExtendingBlockRejectionState
	for i := range nonExtendingBlockRejectionThreshold - 1 {
		var shouldRecycle bool
		state, shouldRecycle = evaluateNonExtendingBlockRejection(
			state,
			now.Add(time.Duration(i)*time.Millisecond),
		)
		require.Falsef(
			t,
			shouldRecycle,
			"rejection %d must not cross the threshold yet", i+1,
		)
	}
	require.Equal(t, nonExtendingBlockRejectionThreshold-1, state.count)

	// The threshold-th rejection, still well inside the window, must
	// trigger the recycle and the state must reset.
	state, shouldRecycle := evaluateNonExtendingBlockRejection(
		state,
		now.Add(time.Duration(nonExtendingBlockRejectionThreshold)*time.Millisecond),
	)
	assert.True(t, shouldRecycle, "threshold-th rejection must recycle")
	assert.Equal(
		t,
		nonExtendingBlockRejectionState{},
		state,
		"state must reset after a recycle decision",
	)
}

// TestEvaluateNonExtendingBlockRejectionWindowExpiry verifies that a gap
// longer than nonExtendingBlockRejectionWindow restarts the count instead of
// accumulating with it. Without this, rejections spread thinly over a very
// long-lived, otherwise healthy connection could eventually cross the bound
// even though none of them were ever part of a flood.
func TestEvaluateNonExtendingBlockRejectionWindowExpiry(t *testing.T) {
	t.Parallel()

	now := time.Now()
	var state nonExtendingBlockRejectionState
	for range nonExtendingBlockRejectionThreshold - 1 {
		var shouldRecycle bool
		state, shouldRecycle = evaluateNonExtendingBlockRejection(state, now)
		require.False(t, shouldRecycle)
	}
	require.Equal(t, nonExtendingBlockRejectionThreshold-1, state.count)

	// A rejection after the window has elapsed restarts the count at 1
	// rather than reaching the threshold.
	later := now.Add(nonExtendingBlockRejectionWindow + time.Second)
	state, shouldRecycle := evaluateNonExtendingBlockRejection(state, later)
	assert.False(
		t,
		shouldRecycle,
		"a rejection after the window expired must not inherit the prior count",
	)
	assert.Equal(t, 1, state.count)
	assert.Equal(t, later, state.windowStart)
}

// TestNoteNonExtendingBlockRejectionPublishesRecycleAtThreshold verifies the
// LedgerState-level wiring: exactly nonExtendingBlockRejectionThreshold calls
// for one connection publish exactly one ConnectionRecycleRequestedEvent with
// reason "non_extending_block_flood", and no earlier call does.
func TestNoteNonExtendingBlockRejectionPublishesRecycleAtThreshold(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	connId := testRecycleConnId()

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
		},
	}

	point := ocommon.Point{Slot: 1000, Hash: []byte("not-fit-hash")}
	for i := range nonExtendingBlockRejectionThreshold - 1 {
		ls.noteNonExtendingBlockRejection(connId, point, nil)
		testutil.RequireNoReceive(
			t,
			recycled,
			20*time.Millisecond,
			fmt.Sprintf("unexpected recycle event after rejection %d", i+1),
		)
	}

	ls.noteNonExtendingBlockRejection(connId, point, nil)
	got := testutil.RequireReceive(
		t,
		recycled,
		2*time.Second,
		"recycle event not published at threshold",
	)
	assert.Equal(t, connId, got.ConnectionId)
	assert.Equal(t, "non_extending_block_flood", got.Reason)
}

// TestNoteNonExtendingBlockRejectionResetsOnAcceptedBlock is the "should NOT
// demote" regression: a connection that occasionally fails to extend the
// chain (a brief rollback/reorg race) but then successfully DOES extend it
// must have its count forgiven. Without noteBlockAcceptedFromConn resetting
// the count, two sub-threshold bursts split by a success would still sum
// past the threshold and wrongly recycle a peer that just proved it was
// healthy.
func TestNoteNonExtendingBlockRejectionResetsOnAcceptedBlock(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	connId := testRecycleConnId()

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
		},
	}
	point := ocommon.Point{Slot: 1000, Hash: []byte("not-fit-hash")}

	// A burst just under the threshold: a brief race, not a flood.
	burst := nonExtendingBlockRejectionThreshold - 1
	for range burst {
		ls.noteNonExtendingBlockRejection(connId, point, nil)
	}
	require.Equal(t, burst, ls.nonExtendingBlockRejections[connIdKey(connId)].count)

	// The same connection then delivers a block that DOES extend the chain.
	ls.noteBlockAcceptedFromConn(connId)
	_, tracked := ls.nonExtendingBlockRejections[connIdKey(connId)]
	require.False(t, tracked, "acceptance must clear the tracked count")

	// A second sub-threshold burst must not combine with the forgiven one:
	// without the reset, burst+burst would have crossed the threshold.
	for i := range burst {
		ls.noteNonExtendingBlockRejection(connId, point, nil)
		testutil.RequireNoReceive(
			t,
			recycled,
			20*time.Millisecond,
			fmt.Sprintf(
				"unexpected recycle event after post-reset rejection %d",
				i+1,
			),
		)
	}
}

// TestNoteNonExtendingBlockRejectionPerConnectionIndependent verifies that
// one connection's rejection count cannot push a different, well-behaved
// connection over the threshold.
func TestNoteNonExtendingBlockRejectionPerConnectionIndependent(t *testing.T) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	floodConn := testRecycleConnId()
	healthyConn := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6002},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3003},
	}

	recycled := make(chan ConnectionRecycleRequestedEvent, 2)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: bus,
		},
	}
	point := ocommon.Point{Slot: 1000, Hash: []byte("not-fit-hash")}

	// healthyConn stays well under the threshold throughout.
	for range nonExtendingBlockRejectionThreshold - 1 {
		ls.noteNonExtendingBlockRejection(healthyConn, point, nil)
	}
	for range nonExtendingBlockRejectionThreshold {
		ls.noteNonExtendingBlockRejection(floodConn, point, nil)
	}

	got := testutil.RequireReceive(
		t,
		recycled,
		2*time.Second,
		"flooding connection must be recycled",
	)
	assert.Equal(t, floodConn, got.ConnectionId)
	testutil.RequireNoReceive(
		t,
		recycled,
		50*time.Millisecond,
		"healthy connection must not be recycled by another connection's flood",
	)
}

// nonExtendingRejectionTestBlock is a minimal ledger.Block, modeled on
// spliceAuditBlock, with a settable BlockNumber so the same helper can build
// both a block that fits the fixture's chain tip (extending it, clearing the
// tracked count) and one that does not (BlockNotFitChainTipError).
type nonExtendingRejectionTestBlock struct {
	hash        lcommon.Blake2b256
	prevHash    lcommon.Blake2b256
	slot        uint64
	blockNumber uint64
}

func (b *nonExtendingRejectionTestBlock) Hash() lcommon.Blake2b256 { return b.hash }
func (b *nonExtendingRejectionTestBlock) PrevHash() lcommon.Blake2b256 {
	return b.prevHash
}
func (b *nonExtendingRejectionTestBlock) SlotNumber() uint64  { return b.slot }
func (b *nonExtendingRejectionTestBlock) BlockNumber() uint64 { return b.blockNumber }
func (b *nonExtendingRejectionTestBlock) IssuerVkey() lcommon.IssuerVkey {
	return lcommon.IssuerVkey{}
}
func (b *nonExtendingRejectionTestBlock) BlockBodySize() uint64 { return 0 }
func (b *nonExtendingRejectionTestBlock) Era() lcommon.Era      { return lcommon.Era{} }
func (b *nonExtendingRejectionTestBlock) Cbor() []byte          { return nil }
func (b *nonExtendingRejectionTestBlock) BlockBodyHash() lcommon.Blake2b256 {
	return lcommon.Blake2b256{}
}
func (b *nonExtendingRejectionTestBlock) Header() lcommon.BlockHeader { return nil }
func (b *nonExtendingRejectionTestBlock) Type() int                   { return 0 }
func (b *nonExtendingRejectionTestBlock) Transactions() []lcommon.Transaction {
	return nil
}
func (b *nonExtendingRejectionTestBlock) Utxorpc() (*utxorpc.Block, error) {
	return nil, nil
}

// TestFlushPendingBlockfetchNonExtendingFloodRecyclesConnection is the
// end-to-end "should demote" regression: nonExtendingBlockRejectionThreshold
// blocks from the same connection, none of which fit the real chain tip,
// delivered through the actual flushPendingBlockfetchBlocksDeferred path
// (the code that logs "ignoring blockfetch block ... does not fit on
// current chain tip"), must publish exactly one recycle request for that
// connection.
func TestFlushPendingBlockfetchNonExtendingFloodRecyclesConnection(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	// None of these fit: their PrevHash matches neither the ancestor nor the
	// current tip, so each one hits BlockNotFitChainTipError. Each carries a
	// unique hash so none collides with another.
	events := make([]BlockfetchEvent, 0, nonExtendingBlockRejectionThreshold)
	for i := range nonExtendingBlockRejectionThreshold {
		hash := testHashBytes(fmt.Sprintf("flood-block-%d", i))
		block := &nonExtendingRejectionTestBlock{
			hash:        lcommon.NewBlake2b256(hash),
			prevHash:    lcommon.NewBlake2b256(testHashBytes("abandoned-parent")),
			slot:        fixture.currentTip.Point.Slot + uint64(i) + 1,
			blockNumber: fixture.currentTip.BlockNumber + 1,
		}
		events = append(events, BlockfetchEvent{
			ConnectionId: fixture.connId,
			Block:        block,
			Point:        ocommon.NewPoint(block.slot, hash),
		})
	}
	ls.pendingBlockfetchEvents = events

	err := ls.flushPendingBlockfetchBlocksDeferred(nil)
	require.NoError(
		t,
		err,
		"a non-extending block is ignored, not a hard processing error",
	)

	got := testutil.RequireReceive(
		t,
		recycled,
		2*time.Second,
		"flooding connection must be recycled",
	)
	assert.Equal(t, fixture.connId, got.ConnectionId)
	assert.Equal(t, "non_extending_block_flood", got.Reason)
	assert.Equal(
		t,
		fixture.currentTip.Point.Hash,
		ls.chain.Tip().Point.Hash,
		"the real chain tip must be completely unaffected by the flood",
	)
	assert.Equal(
		t,
		float64(nonExtendingBlockRejectionThreshold),
		promtestutil.ToFloat64(ls.metrics.nonExtendingBlockRejections),
		"every rejected block in the flood must be counted, not only the ones before threshold",
	)
	assert.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(ls.metrics.nonExtendingBlockFloodRecycles),
		"exactly one recycle event fired for this flood",
	)
}

// TestFlushPendingBlockfetchNonExtendingHandfulNotRecycled is the "should
// NOT demote" regression for a legitimate brief rollback/reorg race: a
// handful of blocks that momentarily do not fit the tip, well under
// nonExtendingBlockRejectionThreshold, must not trigger a recycle.
func TestFlushPendingBlockfetchNonExtendingHandfulNotRecycled(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	ls.config.EventBus = bus

	recycled := make(chan ConnectionRecycleRequestedEvent, 1)
	bus.SubscribeFunc(
		ConnectionRecycleRequestedEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(ConnectionRecycleRequestedEvent)
			if ok {
				recycled <- e
			}
		},
	)

	const handful = 3 // well under nonExtendingBlockRejectionThreshold
	events := make([]BlockfetchEvent, 0, handful)
	for i := range handful {
		hash := testHashBytes(fmt.Sprintf("race-block-%d", i))
		block := &nonExtendingRejectionTestBlock{
			hash:        lcommon.NewBlake2b256(hash),
			prevHash:    lcommon.NewBlake2b256(testHashBytes("abandoned-parent")),
			slot:        fixture.currentTip.Point.Slot + uint64(i) + 1,
			blockNumber: fixture.currentTip.BlockNumber + 1,
		}
		events = append(events, BlockfetchEvent{
			ConnectionId: fixture.connId,
			Block:        block,
			Point:        ocommon.NewPoint(block.slot, hash),
		})
	}
	ls.pendingBlockfetchEvents = events

	require.NoError(t, ls.flushPendingBlockfetchBlocksDeferred(nil))

	testutil.RequireNoReceive(
		t,
		recycled,
		100*time.Millisecond,
		"a handful of rejections from a brief rollback race must not recycle the peer",
	)
	assert.Equal(
		t,
		handful,
		ls.nonExtendingBlockRejections[connIdKey(fixture.connId)].count,
		"the rejections must still be tracked, just below threshold",
	)
	assert.Equal(
		t,
		float64(handful),
		promtestutil.ToFloat64(ls.metrics.nonExtendingBlockRejections),
		"individual rejections are still counted even when no flood is detected",
	)
	assert.Equal(
		t,
		float64(0),
		promtestutil.ToFloat64(ls.metrics.nonExtendingBlockFloodRecycles),
		"a brief rollback race must not increment the recycle counter",
	)
}

// Regression: replayBufferedHeadersAsync must be a no-op once
// Close has been observed, so its goroutine can never reach DB reads
// after the database is closed.
func TestReplayBufferedHeadersAsyncSkippedAfterClose(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls

	// Simulate that Close has begun (closed=true) before the replay
	// is scheduled. The async helper must not spawn a worker.
	ls.closed.Store(true)

	ls.replayBufferedHeadersAsync(fixture.connId)

	done := make(chan struct{})
	go func() {
		ls.replayWG.Wait()
		close(done)
	}()
	testutil.RequireReceive(
		t,
		done,
		testutil.AsyncWait,
		"replayWG did not complete: a worker was spawned despite ls.closed=true",
	)
}

// Regression: Close must drain in-flight replay goroutines
// before returning, so callers (Node.shutdown phase 3) can safely close
// the database without racing the replay's DB reads.
func TestCloseWaitsForInFlightReplay(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls

	// Hold the chainsync mutex so the goroutine spawned by
	// replayBufferedHeadersAsync blocks before touching the DB.
	ls.chainsyncMutex.Lock()
	chainsyncMutexLocked := true
	defer func() {
		if chainsyncMutexLocked {
			ls.chainsyncMutex.Unlock()
		}
	}()
	ls.replayBufferedHeadersAsync(fixture.connId)

	// (The goroutine ran replayWG.Add(1) synchronously before launching,
	// so the wait group counter is non-zero from this point.)
	closeReturned := make(chan error, 1)
	go func() {
		closeReturned <- ls.Close()
	}()

	// Wait until Close has set closed=true and is therefore committed
	// to draining the replay worker before it can return.
	testutil.WaitForCondition(
		t,
		ls.closed.Load,
		testutil.AsyncWait,
		"Close did not set ls.closed",
	)

	// With closed=true and the worker blocked on chainsyncMutex, Close
	// must not return: doing so would close the DB while the worker is
	// still alive.
	testutil.RequireNoReceive(
		t,
		closeReturned,
		50*time.Millisecond,
		"Close returned before replay drained",
	)

	// Releasing the mutex lets the worker observe ls.closed=true and exit
	// without issuing any DB reads; Close can then finish draining.
	ls.chainsyncMutex.Unlock()
	chainsyncMutexLocked = false

	err := testutil.RequireReceive(
		t,
		closeReturned,
		testutil.AsyncWait,
		"Close did not return after replay worker exited",
	)
	if err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
}

// These two tests close the gap the converted rollback tests left open: they
// all pass a nil *pendingPublishes, which takes pendingPublishes.add's
// immediate-publish branch, so none of them can tell a deferred publish from
// an inline one. Reverting requestChainsyncResync's pending.add(...) back to a
// direct ls.config.EventBus.Publish(...) leaves the whole ledger suite green
// because nothing exercises the deferred path with a real queue and a real
// subscriber that needs the held mutex.
//
// Each test below hands requestChainsyncResync a NON-nil queue while holding a
// guarded mutex, with a ChainsyncResyncEventType subscriber that reaches for
// that same mutex from its handler — exactly the cycle pendingPublishes.go
// documents (the real subscriber is RecoverAfterLocalRollback, which takes
// chainsyncMutex and nests chainsyncBlockfetchMutex under it). With the fix the
// event is queued and only published after the unlock, so it completes. Revert
// the publish to inline and it parks forever: the subscriber buffer is full and
// its only reader is the handler waiting for the mutex the publisher still
// holds. The 5s guard turns that hang into a failure instead of wedging the
// whole test binary.

// runResyncDeferredPublishScenario drives requestChainsyncResync under the
// mutex selected by lock/unlock, with a resync subscriber whose handler takes
// that same mutex. It fails if the publish happens inline (deadlock) rather
// than being deferred until after the unlock.
func runResyncDeferredPublishScenario(
	t *testing.T,
	mu *sync.Mutex,
	mutexName string,
) {
	t.Helper()

	bus := event.NewEventBus(nil, nil)
	defer bus.Stop()

	ls := &LedgerState{
		config: LedgerStateConfig{EventBus: bus},
	}

	// entered is signalled from inside the handler, before it blocks on the
	// mutex. Receiving from it proves the dispatch goroutine has pulled an
	// event off the channel and is now committed to a handler call — so it
	// will not read another event until that handler returns, which it cannot
	// until the publisher releases the mutex.
	entered := make(chan struct{}, 4)
	// SubscriberBackpressureBlock models a lossless subscriber (the production
	// resync subscriber is one): a full buffer parks the publisher forever
	// rather than detaching after a timeout, which is what makes an inline
	// publish a true deadlock instead of an eventually-dropped event. Buffer 1
	// is the smallest that lets one priming event occupy the reader while a
	// second fills the channel, so the next publish has nowhere to go.
	bus.SubscribeFuncWithBufferPolicy(
		event.ChainsyncResyncEventType,
		1,
		event.SubscriberBackpressureBlock,
		func(event.Event) {
			entered <- struct{}{}
			mu.Lock()
			mu.Unlock()
		},
	)

	connId := testChainsyncConnId(6000, 7000)
	primer := func() event.Event {
		return event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{ConnectionId: connId},
		)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		mu.Lock()
		// First primer: consumed by the dispatch goroutine, whose handler
		// then parks on the mutex we hold. Waiting on entered guarantees it
		// has left the channel before the buffer is filled below.
		bus.Publish(event.ChainsyncResyncEventType, primer())
		<-entered
		// Second primer: fills the now-empty single buffer slot. The reader
		// is still parked in the first handler, so this event just sits there
		// and the subscriber has no free capacity left.
		bus.Publish(event.ChainsyncResyncEventType, primer())
		// With the fix this queues onto pending and returns immediately.
		// Reverted to an inline ls.config.EventBus.Publish it blocks here: the
		// buffer is full and its only reader is the parked handler.
		var pending pendingPublishes
		ls.requestChainsyncResync(connId, "test resync", &pending)
		mu.Unlock()
		// Only reached once the publish did NOT happen under the lock.
		pending.flush()
	}()

	select {
	case <-done:
		// Deferred: the queued event was published after the unlock, the
		// handler took the mutex, and everything drained.
	case <-time.After(testutil.AsyncWait):
		t.Fatalf(
			"requestChainsyncResync published ChainsyncResyncEventType inline"+
				" while holding %s: the publish parked on a full subscriber"+
				" buffer whose handler was waiting for that same mutex"+
				" (deadlock). Queue it with pendingPublishes and flush after"+
				" the unlock — see pending_publish.go.",
			mutexName,
		)
	}
}

// TestRequestChainsyncResyncDefersPublishUnderChainsyncMutex fails (hangs to
// the 5s guard) if requestChainsyncResync publishes inline while chainsyncMutex
// is held, and passes when the publish is deferred through pendingPublishes.
func TestRequestChainsyncResyncDefersPublishUnderChainsyncMutex(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	runResyncDeferredPublishScenario(t, &ls.chainsyncMutex, "chainsyncMutex")
}

// TestRequestChainsyncResyncDefersPublishUnderChainsyncBlockfetchMutex is the
// same guard for the blockfetch lock: the resync subscriber nests
// chainsyncBlockfetchMutex under chainsyncMutex, so holding the blockfetch lock
// alone across an inline publish deadlocks the same way.
func TestRequestChainsyncResyncDefersPublishUnderChainsyncBlockfetchMutex(
	t *testing.T,
) {
	t.Parallel()

	ls := &LedgerState{}
	runResyncDeferredPublishScenario(
		t, &ls.chainsyncBlockfetchMutex, "chainsyncBlockfetchMutex",
	)
}

// A crossable rollback the node actually applies must not leave its point in
// the per-connection loop detector. Otherwise a later, legitimate rollback to
// the same fork point counts the crossing we already made and is suppressed as
// a false loop, which is the reconnect-churn wedge.
func TestHandleEventChainsyncRollbackClearsLoopHistoryForCrossedPoint(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	err := fixture.ls.handleEventChainsyncRollback(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        fixture.ancestorTip.Point,
	}, nil)
	require.NoError(t, err)
	require.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())

	for _, r := range fixture.ls.rollbackHistory {
		assert.Falsef(
			t,
			pointMatches(r.point, fixture.ancestorTip.Point),
			"a successfully applied rollback must clear its loop-detector record",
		)
	}
}

// The loop shape: after crossing a fork point the node advances forward
// on the peer's chain, then the peer rolls it back to the same fork point
// again. Because the successful first cross reset the loop counter, the second
// crossable rollback must be applied, not suppressed as a false loop.
func TestHandleEventChainsyncRollbackAppliesRepeatedCrossableRollback(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	require.NoError(t, fixture.ls.handleEventChainsyncRollback(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        fixture.ancestorTip.Point,
	}, nil))
	require.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())

	// Re-extend the chain past the fork point so a second rollback to it is
	// once more a real sub-K rollback.
	require.NoError(t, fixture.ls.chain.AddRawBlocks([]chain.RawBlock{
		{
			Slot:        fixture.currentTip.Point.Slot,
			Hash:        fixture.currentTip.Point.Hash,
			BlockNumber: fixture.currentTip.BlockNumber,
			Type:        1,
			PrevHash:    fixture.ancestorTip.Point.Hash,
			Cbor:        []byte{0x80},
		},
	}))
	require.Equal(
		t,
		fixture.currentTip.Point.Slot,
		fixture.ls.chain.Tip().Point.Slot,
	)

	err := fixture.ls.handleEventChainsyncRollback(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        fixture.ancestorTip.Point,
	}, nil)
	require.NoError(t, err)
	require.NotErrorIs(t, err, ErrRollbackLoopDetected)
	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
}

// When the loop detector breaks a loop for a genuinely un-crossable rollback,
// the skip path must surface the stuck condition through the point-keyed
// unrecoverable-rollback tracker so the escalation and metric can fire,
// instead of silently skipping and hiding a persistently un-recoverable
// divergence.
func TestHandleEventChainsyncRollbackSkipReportsUnrecoverableRollback(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	// A rollback point the node cannot cross to (a fork block below our tip
	// that is not present in our chain), so the loop detector genuinely skips
	// it rather than applying it.
	uncrossablePoint := ocommon.Point{
		Slot: fixture.currentTip.Point.Slot - 3,
		Hash: testHashBytes("uncrossable-report-fork-block"),
	}

	// Pre-seed one prior rollback so this call reaches the loop threshold
	// and takes the skip path.
	fixture.ls.rollbackHistory = []rollbackRecord{
		{
			point: ocommon.Point{
				Slot: uncrossablePoint.Slot,
				Hash: append([]byte(nil), uncrossablePoint.Hash...),
			},
			connKey:   connIdKey(fixture.connId),
			timestamp: time.Now(),
		},
	}

	err := fixture.ls.handleEventChainsyncRollback(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        uncrossablePoint,
	}, nil)
	require.ErrorIs(t, err, ErrRollbackLoopDetected)

	_, ok := fixture.ls.unrecoverableRollbacks[unrecoverableRollbackKey(
		uncrossablePoint,
	)]
	assert.True(
		t,
		ok,
		"skip path must record the point in the unrecoverable-rollback tracker",
	)
}

// A crossable rollback that reaches the per-connection loop threshold must
// still be APPLIED, not suppressed as a false loop: the loop detector only
// breaks loops for rollbacks the node cannot cross. This exercises the appliability guard directly by pre-seeding history to
// the threshold, unlike the reset-on-success path which never reaches it.
func TestHandleEventChainsyncRollbackAppliesCrossableRollbackAtLoopThreshold(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	// Seed a prior rollback to the (crossable) shared ancestor so this call
	// hits the loop threshold.
	fixture.ls.rollbackHistory = []rollbackRecord{
		{
			point: ocommon.Point{
				Slot: fixture.ancestorTip.Point.Slot,
				Hash: append([]byte(nil), fixture.ancestorTip.Point.Hash...),
			},
			connKey:   connIdKey(fixture.connId),
			timestamp: time.Now(),
		},
	}

	err := fixture.ls.handleEventChainsyncRollback(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        fixture.ancestorTip.Point,
	}, nil)
	require.NoError(t, err)
	require.NotErrorIs(t, err, ErrRollbackLoopDetected)
	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
}

type chainsyncRollbackFixture struct {
	ls            *LedgerState
	connId        ouroboros.ConnectionId
	ancestorTip   ochainsync.Tip
	currentTip    ochainsync.Tip
	ancestorNonce []byte
	currentNonce  []byte
	forkPoint     ocommon.Point
}

type testSecurityParamLedger struct {
	securityParam int
}

func (m testSecurityParamLedger) SecurityParam() int {
	return m.securityParam
}

func TestHandleEventChainsyncRollbackSynchronizesLedgerTip(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	err := fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.ancestorTip.Point,
		},
		nil,
	)
	require.NoError(t, err)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.ancestorNonce, fixture.ls.currentTipBlockNonce),
	)

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, dbTip)
}

func TestHandleEventChainsyncRollbackRejectsBelowMithrilBoundary(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus
	fixture.ls.mithrilLedgerSlot = fixture.currentTip.Point.Slot
	require.NoError(
		t,
		fixture.ls.db.SetSyncState("mithril_ledger_slot", "20", nil),
	)
	timer := time.NewTimer(time.Hour)
	t.Cleanup(func() { timer.Stop() })
	fixture.ls.activeBlockfetchConnId = fixture.connId
	fixture.ls.selectedBlockfetchConnId = fixture.connId
	fixture.ls.shadowBlockfetchConnId = fixture.connId
	fixture.ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	fixture.ls.chainsyncBlockfetchTimeoutTimer = timer
	fixture.ls.pendingBlockfetchEvents = []BlockfetchEvent{
		{Point: fixture.currentTip.Point},
	}

	resyncCh := make(chan event.ChainsyncResyncEvent, 1)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	err := fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.ancestorTip.Point,
		},
		nil,
	)
	require.NoError(t, err)

	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
	storedBoundary, err := fixture.ls.db.GetSyncState(
		"mithril_ledger_slot",
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "20", storedBoundary)

	e := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"expected Mithril rollback-boundary resync event",
	)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonRollbackExceedsMithril,
		e.Reason,
	)
	assert.Equal(t, fixture.connId, e.ConnectionId)
	assert.Equal(t, ouroboros.ConnectionId{}, fixture.ls.activeBlockfetchConnId)
	assert.Equal(
		t,
		ouroboros.ConnectionId{},
		fixture.ls.selectedBlockfetchConnId,
	)
	assert.Equal(t, ouroboros.ConnectionId{}, fixture.ls.shadowBlockfetchConnId)
	assert.Nil(t, fixture.ls.chainsyncBlockfetchReadyChan)
	assert.Nil(t, fixture.ls.chainsyncBlockfetchTimeoutTimer)
	assert.Empty(t, fixture.ls.pendingBlockfetchEvents)
}

func TestHandleEventChainsyncRollbackPrunesStaleBlockNonces(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	competingHash := testHashBytes("competing-ancestor-slot")
	require.NoError(
		t,
		fixture.ls.db.SetBlockNonce(
			competingHash,
			fixture.ancestorTip.Point.Slot,
			[]byte("stale-same-slot"),
			false,
			nil,
		),
	)
	require.NoError(
		t,
		fixture.ls.db.SetBlockNonce(
			testHashBytes("future-fork-block"),
			fixture.currentTip.Point.Slot+10,
			[]byte("stale-future"),
			false,
			nil,
		),
	)

	err := fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.ancestorTip.Point,
		},
		nil,
	)
	require.NoError(t, err)

	rows, err := fixture.ls.db.GetBlockNoncesInSlotRange(
		fixture.ancestorTip.Point.Slot,
		fixture.currentTip.Point.Slot+11,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, fixture.ancestorTip.Point.Hash, rows[0].Hash)
	assert.Equal(t, fixture.ancestorTip.Point.Slot, rows[0].Slot)
	assert.Equal(t, fixture.ancestorNonce, rows[0].Nonce)
}

func TestLoadTipPrunesStaleBlockNonces(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	competingHash := testHashBytes("competing-tip-slot")
	require.NoError(
		t,
		fixture.ls.db.SetBlockNonce(
			competingHash,
			fixture.currentTip.Point.Slot,
			[]byte("stale-same-slot"),
			false,
			nil,
		),
	)
	require.NoError(
		t,
		fixture.ls.db.SetBlockNonce(
			testHashBytes("future-fork-block"),
			fixture.currentTip.Point.Slot+10,
			[]byte("stale-future"),
			false,
			nil,
		),
	)

	require.NoError(t, fixture.ls.loadTip())

	rows, err := fixture.ls.db.GetBlockNoncesInSlotRange(
		fixture.ancestorTip.Point.Slot,
		fixture.currentTip.Point.Slot+11,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, fixture.ancestorTip.Point.Hash, rows[0].Hash)
	assert.Equal(t, fixture.currentTip.Point.Hash, rows[1].Hash)
	assert.True(
		t,
		bytes.Equal(
			fixture.ls.currentTipBlockNonce,
			rows[1].Nonce,
		),
	)
}

func TestRollbackRepairsTipAtDurableFloorOnNoOpAndSameSlotPaths(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	// Leave currentTip at a point whose nonce was never durably applied. A
	// rollback request at the current tip is normally a no-op, but the durable
	// floor repair must still move it back to the last applied block.
	require.NoError(t, fixture.ls.db.DeleteBlockNoncesAfterPoint(
		fixture.ancestorTip.Point,
		nil,
	))
	require.NoError(t, fixture.ls.rollback(fixture.currentTip.Point))
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)

	// A competing same-slot hash is not covered by a slot-only comparison. Put
	// the in-memory tip on that unapplied point and ensure the canonical floor
	// still repairs it.
	fixture.ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(
			fixture.ancestorTip.Point.Slot,
			[]byte("unapplied-same-slot"),
		),
		BlockNumber: fixture.ancestorTip.BlockNumber,
	}
	require.NoError(t, fixture.ls.rollback(fixture.ls.currentTip.Point))
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
}

func TestHandleEventChainsyncRollbackDoesNotSkipDifferentPeerHistory(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	otherConnId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6001},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	fixture.ls.rollbackHistory = []rollbackRecord{
		{
			point: ocommon.Point{
				Slot: fixture.ancestorTip.Point.Slot,
				Hash: append([]byte(nil), fixture.ancestorTip.Point.Hash...),
			},
			connKey:   connIdKey(otherConnId),
			timestamp: time.Now(),
		},
	}

	err := fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.ancestorTip.Point,
		},
		nil,
	)
	require.NoError(t, err)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.ancestorNonce, fixture.ls.currentTipBlockNonce),
	)
}

func TestHandleEventChainsyncRollbackSkipsSamePeerLoop(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	// A genuinely un-crossable rollback point: a fork block below our tip
	// that is not present in our chain. The loop detector must break the loop
	// only for points the node cannot apply. A crossable point is instead
	// applied even when it repeats (see
	// TestHandleEventChainsyncRollbackAppliesRepeatedCrossableRollback and
	// TestHandleEventChainsyncRollbackAppliesCrossableRollbackAtLoopThreshold),
	// which is the fix.
	uncrossablePoint := ocommon.Point{
		Slot: fixture.currentTip.Point.Slot - 3,
		Hash: testHashBytes("uncrossable-missing-fork-block"),
	}

	fixture.ls.rollbackHistory = []rollbackRecord{
		{
			point: ocommon.Point{
				Slot: uncrossablePoint.Slot,
				Hash: append([]byte(nil), uncrossablePoint.Hash...),
			},
			connKey:   connIdKey(fixture.connId),
			timestamp: time.Now(),
		},
	}

	err := fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        uncrossablePoint,
		},
		nil,
	)
	require.ErrorIs(t, err, ErrRollbackLoopDetected)

	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
}

// TestHandleEventChainsyncRollbackExceedsKDeclinesReconcilingDivergedLedgerTip
// covers rollback-depth bound: the common ancestor between the
// diverged primary chain and the stale ledger tip here sits 3 blocks behind
// the primary chain's tip, beyond the fixture's K=2. Live reconciliation
// must decline rather than force that rewind through, leaving chain and
// ledger state untouched, and fall back to the existing over-K handling
// that rejects the peer chain and requests a fresh intersection.
func TestHandleEventChainsyncRollbackExceedsKDeclinesReconcilingDivergedLedgerTip(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	putPrimaryChainOnForkBeyondK(t, fixture, "live-rollback")

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	resyncCh := subscribeChainsyncResync(t, bus)

	// A subscriber must exist for readBlocksAboveSlot to read anything at
	// all; asserting it sees nothing is what proves the declined,
	// over-K reconciliation never emitted an undo for the common
	// ancestor it did not actually rewind to.
	txSubID, txCh := bus.SubscribeWithBuffer(TransactionEventType, 64)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, txSubID) })

	preReconcileChainTip := fixture.ls.chain.Tip()

	require.NoError(t, fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        ocommon.Point{},
		},
		nil,
	))

	// Nothing was rewound: the primary chain, ledger tip, and durable
	// nonces are all exactly as they were before the declined
	// reconciliation.
	assert.Equal(t, preReconcileChainTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.currentNonce, fixture.ls.currentTipBlockNonce),
	)
	assert.Equal(t, SyncingChainsyncState, fixture.ls.chainsyncState)

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.currentTip, dbTip)

	rows, err := fixture.ls.db.GetBlockNoncesInSlotRange(
		fixture.ancestorTip.Point.Slot,
		fixture.currentTip.Point.Slot+1,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 2)

	e := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"expected over-K resync event",
	)
	assert.Equal(t, event.ChainsyncResyncReasonRollbackExceedsK, e.Reason)
	assert.Equal(t, fixture.connId, e.ConnectionId)

	// The declined over-K reconciliation must not have emitted an undo
	// for the common ancestor: validateAndEmitRollbackUndo's own
	// ValidateRollback pre-check rejects the rewind before it reads or
	// publishes anything.
	testutil.RequireNoReceive(
		t, txCh, 250*time.Millisecond,
		"a declined over-K reconciliation must not publish undo events",
	)
}

// TestHandleEventChainsyncRollbackReconcileFindsMithrilBoundaryAncestor
// covers wolf31o2's review on when an over-K rollback triggers
// reconcileLivePrimaryChainLedgerDivergence and the common ancestor that
// reconciliation itself finds sits at or below the Mithril boundary,
// reconcilePrimaryChainTipWithLedgerTip's own pre-check (added earlier
// this round) returns ErrRollbackExceedsMithrilBoundary -- a case
// handleEventChainsyncRollback's over-K branch did not classify, so it
// propagated as a generic reconciliation error instead of the same
// classified ChainsyncResyncReasonRollbackExceedsMithril resync a direct
// rollback failure against that boundary already gets.
func TestHandleEventChainsyncRollbackReconcileFindsMithrilBoundaryAncestor(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	// A longer fork than putPrimaryChainOnForkBeyondK's: the rollback
	// target below must itself be a real, on-chain point (rollbackPointBlock
	// rejects a splice to a point this chain does not hold), several
	// blocks behind the fork's own tip so rolling back to it exceeds K
	// (2) on its own -- distinct from targeting origin, which would also
	// trip ls.mithrilLedgerSlot's own pre-check in rollbackChainAndState
	// before ever reaching reconciliation.
	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	prevHash := fixture.ancestorTip.Point.Hash
	forkBlocks := make([]chain.RawBlock, 0, 5)
	var rollbackTarget ocommon.Point
	for idx := range 5 {
		blockOffset := uint64(idx + 1)
		hash := testHashBytes(
			fmt.Sprintf("live-rollback-mithril-fork-%d", idx),
		)
		slot := fixture.ancestorTip.Point.Slot + blockOffset*5
		forkBlocks = append(forkBlocks, chain.RawBlock{
			Slot:        slot,
			Hash:        hash,
			BlockNumber: fixture.ancestorTip.BlockNumber + blockOffset,
			Type:        1,
			PrevHash:    prevHash,
			Cbor:        []byte{0x80},
		})
		if idx == 0 {
			// The first fork block: rolling back to it from the tip
			// (5 blocks later) is a depth-4 rollback, exceeding K (2).
			rollbackTarget = ocommon.NewPoint(slot, hash)
		}
		prevHash = hash
	}
	require.NoError(t, fixture.ls.chain.AddRawBlocks(forkBlocks))
	require.NotEqual(t, fixture.currentTip, fixture.ls.chain.Tip())
	require.Equal(t, fixture.currentTip, fixture.ls.currentTip)

	// The Mithril boundary sits above the common ancestor
	// (fixture.ancestorTip, slot 10) reconciliation will find, but at or
	// below the rollback target itself, so only the inner reconcile
	// check -- not rollbackChainAndState's own outer pre-check -- can
	// fire.
	fixture.ls.mithrilLedgerSlot = fixture.ancestorTip.Point.Slot + 1

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	resyncCh := subscribeChainsyncResync(t, bus)

	txSubID, txCh := bus.SubscribeWithBuffer(TransactionEventType, 64)
	t.Cleanup(func() { bus.Unsubscribe(TransactionEventType, txSubID) })

	preReconcileChainTip := fixture.ls.chain.Tip()

	require.NoError(t, fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        rollbackTarget,
		},
		nil,
	))

	// Nothing was rewound.
	assert.Equal(t, preReconcileChainTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)

	e := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"expected a Mithril-boundary resync event, not a generic "+
			"reconciliation-error propagation",
	)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonRollbackExceedsMithril,
		e.Reason,
	)
	assert.Equal(t, fixture.connId, e.ConnectionId)

	testutil.RequireNoReceive(
		t, txCh, 250*time.Millisecond,
		"a declined reconciliation must not publish undo events",
	)
}

// TestLedgerReadChainRequestsResyncOnOverKReconcile verifies that the reader
// emits a resync event when reconciliation exceeds the security parameter,
// matching the behavior of handleEventChainsyncRollback and tryResolveFork.
// This lets connection management reconnect and negotiate a fresh
// intersection instead of silently stopping the reader.
func TestLedgerReadChainRequestsResyncOnOverKReconcile(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	putPrimaryChainOnForkBeyondK(t, fixture, "ledger-read-chain-resync")

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	resyncCh := subscribeChainsyncResync(t, bus)

	// fixture.ls.currentTip (still fixture.currentTip after
	// putPrimaryChainOnForkBeyondK) is not on the now-diverged primary
	// chain, so ledgerReadChain's very first iterator creation misses
	// and immediately drives the retry-reconcile path.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resultCh := make(chan readChainResult, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		fixture.ls.ledgerReadChain(ctx, resultCh)
	}()

	testutil.RequireReceive(
		t, done, testutil.AsyncWait,
		"ledgerReadChain should give up and return, not hang, on an "+
			"unreconcilable over-K divergence",
	)

	e := testutil.RequireReceive(
		t, resyncCh, testutil.AsyncWait,
		"expected an over-K resync event from ledgerReadChain",
	)
	assert.Equal(t, event.ChainsyncResyncReasonRollbackExceedsK, e.Reason)
	assert.Equal(t, fixture.currentTip.Point.Slot, e.Point.Slot)
	assert.Equal(t, fixture.currentTip.Point.Hash, e.Point.Hash)

	// The reader must not have silently rewound the ledger tip past K
	// either: it declined, exactly as handleEventChainsyncRollback does
	// for the same divergence.
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
}

// TestLedgerReadChainRequestsResyncOnMithrilBoundaryReconcile verifies that
// ledgerReadChain emits the same resync reason as
// handleEventChainsyncRollback when reconciliation reaches the Mithril
// boundary, rather than only logging an error.
func TestLedgerReadChainRequestsResyncOnMithrilBoundaryReconcile(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	// Diverge to a fork well within K (forkDepth 1, K is 2), so this
	// reaches the Mithril pre-check rather than the over-K decline --
	// unlike putPrimaryChainOnForkBeyondK's setup above.
	forkHash := testHashBytes("ledger-read-chain-mithril-resync")
	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	require.NoError(t, fixture.ls.chain.AddRawBlocks([]chain.RawBlock{
		{
			Slot:        fixture.currentTip.Point.Slot + 5,
			Hash:        forkHash,
			BlockNumber: fixture.currentTip.BlockNumber + 1,
			Type:        1,
			PrevHash:    fixture.ancestorTip.Point.Hash,
			Cbor:        []byte{0x80},
		},
	}))
	fixture.ls.mithrilLedgerSlot = fixture.ancestorTip.Point.Slot + 1

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	resyncCh := subscribeChainsyncResync(t, bus)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resultCh := make(chan readChainResult, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		fixture.ls.ledgerReadChain(ctx, resultCh)
	}()

	testutil.RequireReceive(
		t, done, testutil.AsyncWait,
		"ledgerReadChain should give up and return, not hang, on an "+
			"unreconcilable Mithril-boundary divergence",
	)

	e := testutil.RequireReceive(
		t, resyncCh, testutil.AsyncWait,
		"expected a Mithril-boundary resync event from ledgerReadChain",
	)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonRollbackExceedsMithril,
		e.Reason,
	)
	assert.Equal(t, fixture.currentTip.Point.Slot, e.Point.Slot)
	assert.Equal(t, fixture.currentTip.Point.Hash, e.Point.Hash)

	// The reader must not have silently rewound the ledger tip below the
	// Mithril boundary either.
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
}

// TestLedgerProcessBlocksRetriesInsteadOfHaltingOnOverKReconcile is the
// pipeline-level regression a wolf31o2 review on required.
// TestLedgerReadChainRequestsResyncOnOverKReconcile above only proves
// ledgerReadChain itself returns and publishes a resync event; it says
// nothing about what happens to block processing afterward. Before this
// fix, this over-K branch returned without ever sending a readChainResult
// on resultCh, which ledgerProcessBlocksFromSource's closed-channel case
// turned into a nil error -- ledgerProcessBlocksWithAttempt's err == nil
// branch then exited its restart loop for good, permanently and silently
// halting all ledger block processing with nothing to resume it short of a
// full LedgerState restart. That the K-bounded rewind is
// what made this branch reachable at all (RewindPrimaryChainToPoint had no
// bound before) is why it must be fixed here rather than
// folded into the general, already-deferred pattern.
//
// This wires the real ledgerReadChain/ledgerProcessBlocksFromSource pair
// through ledgerProcessBlocksWithAttempt exactly as production
// ledgerProcessBlocks does, and proves the pipeline keeps restarting (with
// backoff) rather than exiting cleanly after the first over-K rejection --
// confirmed by seeing a second, independent resync event, which can only
// fire if ledgerReadChain ran again from a fresh attempt.
func TestLedgerProcessBlocksRetriesInsteadOfHaltingOnOverKReconcile(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	putPrimaryChainOnForkBeyondK(t, fixture, "pipeline-retry-over-k")

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus
	resyncCh := subscribeChainsyncResync(t, bus)

	ctx, cancel := context.WithCancel(context.Background())
	attempt := func(attemptCtx context.Context) error {
		return fixture.ls.runLedgerReadChainAttempt(
			attemptCtx,
			fixture.ls.ledgerReadChain,
			fixture.ls.ledgerProcessBlocksFromSource,
		)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fixture.ls.ledgerProcessBlocksWithAttempt(ctx, attempt)
	}()
	t.Cleanup(func() {
		cancel()
		testutil.RequireReceive(
			t, done, testutil.AsyncWait,
			"ledgerProcessBlocksWithAttempt must exit once its context "+
				"is canceled",
		)
	})

	first := testutil.RequireReceive(
		t, resyncCh, testutil.AsyncWait,
		"expected the first over-K resync event",
	)
	assert.Equal(t, event.ChainsyncResyncReasonRollbackExceedsK, first.Reason)

	// A single rejection followed by silence is exactly the bug this
	// covers -- a second, independent rejection proves the pipeline
	// restarted a fresh ledgerReadChain attempt rather than exiting for
	// good after the first one returned.
	second := testutil.RequireReceive(
		t, resyncCh, testutil.AsyncWait,
		"the pipeline must retry and reach the over-K branch again "+
			"instead of halting after the first rejection",
	)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonRollbackExceedsK,
		second.Reason,
	)

	select {
	case <-done:
		t.Fatal(
			"ledgerProcessBlocksWithAttempt must keep retrying, not " +
				"exit, after an over-K rejection",
		)
	default:
	}

	// None of these retries may have silently rewound the ledger tip past
	// K either.
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
}

func TestTryResolveForkSynchronizesLedgerTip(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	forkHash := testHashBytes("fork-block")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.ancestorTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 10,
	}
	err := fixture.ls.chain.AddBlockHeader(header)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAs(t, err, &notFitErr)

	resolved, err := fixture.ls.tryResolveFork(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockHeader: header,
			// The delivered header is the first block after the fork point and
			// is contiguous with the common ancestor. The peer advertises a tip
			// further ahead, so its chain is genuinely longer than ours and the
			// fork is worth resolving.
			Tip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					header.SlotNumber()+10,
					testHashBytes("fork-block-peer-tip-ahead"),
				),
				BlockNumber: header.BlockNumber() + 1,
			},
		},
		notFitErr,
		nil,
		false,
	)
	require.NoError(t, err)
	require.True(t, resolved)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.ancestorNonce, fixture.ls.currentTipBlockNonce),
	)
	assert.Equal(t, 1, fixture.ls.chain.HeaderCount())

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, dbTip)
}

func TestHandleEventChainsyncForkRecordsAdmittedHeaderFrontier(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	// Keep the test at header admission; no blockfetch worker is needed.
	fixture.ls.chainsyncBlockfetchReadyChan = make(chan struct{})

	forkHash := testHashBytes("fork-frontier-header")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.ancestorTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 10,
	}
	advertisedSlot := ^uint64(0)
	require.NoError(
		t,
		fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockHeader: header,
			Tip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					advertisedSlot,
					testHashBytes("unbound-fork-tip"),
				),
				BlockNumber: advertisedSlot,
			},
		}),
	)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	require.Equal(t, 1, fixture.ls.chain.HeaderCount())
	assert.Equal(t, header.slot, fixture.ls.chain.HeaderTip().Point.Slot)
	// mockHeader carries no real VRF/KES material to verify, and this fork's
	// rollback target (the ancestor tip) sits behind the header's own slot,
	// so it cannot be exempted as Mithril-covered either (that would forbid
	// the very rollback this fork resolution performs). An unverified fork
	// header is still admitted onto the local header chain -- that's
	// ordinary, safe chain-shape bookkeeping -- but requires
	// genuine trust (real verification or a Mithril certificate) before it
	// may advance the shared "trusted sync progress" frontier
	// (recordAdmittedHeaderFrontier), so syncUpstreamTipSlot must stay at
	// its zero value here.
	assert.Zero(t, fixture.ls.syncUpstreamTipSlot.Load())
}

func TestTryResolveForkGenesisRejectsLongerSparseCandidate(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.config.GenesisSelectionStateFunc = func() (bool, uint64) {
		return true, 15
	}

	// The local candidate has one block inside (10, 25]. The peer advertises
	// a longer chain, but its first fetched fork block is outside that exact
	// intersection-anchored window, so Genesis density must reject it.
	forkHash := testHashBytes("genesis-sparse-fork")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.ancestorTip.BlockNumber + 1,
		slot:        30,
	}
	err := fixture.ls.chain.AddBlockHeader(header)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAs(t, err, &notFitErr)

	resolved, err := fixture.ls.tryResolveFork(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        ocommon.NewPoint(header.slot, forkHash),
			BlockHeader:  header,
			Tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(header.slot, forkHash),
				BlockNumber: fixture.currentTip.BlockNumber + 1,
			},
		},
		notFitErr,
		nil,
		false,
	)

	require.NoError(t, err)
	assert.False(t, resolved)
	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Zero(t, fixture.ls.chain.HeaderCount())
}

func TestTryResolveForkGenesisAcceptsDenserShorterCandidate(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	require.NoError(
		t,
		fixture.ls.config.ChainManager.SetLedger(
			testSecurityParamLedger{securityParam: 10},
		),
	)
	fixture.ls.config.GenesisSelectionStateFunc = func() (bool, uint64) {
		return true, 15
	}

	// Extend the local chain beyond the Genesis window. It is longer overall
	// (block 4 versus the peer's block 3), but still has only the slot-20
	// block inside (10, 25].
	localHash3 := testHashBytes("genesis-local-3")
	localHash4 := testHashBytes("genesis-local-4")
	require.NoError(t, fixture.ls.chain.AddRawBlocks([]chain.RawBlock{
		{
			Slot:        30,
			Hash:        localHash3,
			BlockNumber: 3,
			Type:        1,
			PrevHash:    fixture.currentTip.Point.Hash,
			Cbor:        []byte{0x80},
		},
		{
			Slot:        40,
			Hash:        localHash4,
			BlockNumber: 4,
			Type:        1,
			PrevHash:    localHash3,
			Cbor:        []byte{0x80},
		},
	}))

	forkHash1 := testHashBytes("genesis-dense-fork-1")
	forkHash2 := testHashBytes("genesis-dense-fork-2")
	header1 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash1),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: 2,
		slot:        12,
	}
	header2 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash2),
		prevHash:    lcommon.NewBlake2b256(forkHash1),
		blockNumber: 3,
		slot:        14,
	}
	fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        ocommon.NewPoint(header1.slot, forkHash1),
		BlockHeader:  header1,
	})
	err := fixture.ls.handleEventChainsyncBlockHeader(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        ocommon.NewPoint(header2.slot, forkHash2),
			BlockHeader:  header2,
			Tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(header2.slot, forkHash2),
				BlockNumber: header2.blockNumber,
			},
		},
	)

	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, 2, fixture.ls.chain.HeaderCount())
}

func TestTryResolveForkUsesPraosAfterGenesisExit(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.config.GenesisSelectionStateFunc = func() (bool, uint64) {
		return false, 15
	}

	// This candidate has no blocks in the Genesis window, but Genesis has
	// exited and its greater block number must therefore win under Praos.
	forkHash := testHashBytes("post-genesis-praos-fork")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.ancestorTip.BlockNumber + 1,
		slot:        30,
	}
	err := fixture.ls.chain.AddBlockHeader(header)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAs(t, err, &notFitErr)

	resolved, err := fixture.ls.tryResolveFork(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        ocommon.NewPoint(header.slot, forkHash),
			BlockHeader:  header,
			Tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(header.slot, forkHash),
				BlockNumber: fixture.currentTip.BlockNumber + 1,
			},
		},
		notFitErr,
		nil,
		false,
	)

	require.NoError(t, err)
	require.True(t, resolved)
	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, 1, fixture.ls.chain.HeaderCount())
}

func TestHandleEventChainsyncBlockHeaderIgnoresObservedPredecessor(
	t *testing.T,
) {
	t.Parallel()

	testCases := []struct {
		name          string
		differentPeer bool
		replayTip     bool
	}{
		{
			name:      "same_peer_duplicate_tip",
			replayTip: true,
		},
		{
			name:          "different_peer_reordered_predecessor",
			differentPeer: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			fixture := newChainsyncRollbackFixture(t)
			fixture.ls.config.GenesisSelectionStateFunc = func() (bool, uint64) {
				return true, 15
			}

			firstHash := testHashBytes(
				"observed-predecessor-first-" + testCase.name,
			)
			secondHash := testHashBytes(
				"observed-predecessor-second-" + testCase.name,
			)
			firstHeader := mockHeader{
				hash: lcommon.NewBlake2b256(firstHash),
				prevHash: lcommon.NewBlake2b256(
					fixture.currentTip.Point.Hash,
				),
				blockNumber: fixture.currentTip.BlockNumber + 1,
				slot:        fixture.currentTip.Point.Slot + 1,
			}
			secondHeader := mockHeader{
				hash:        lcommon.NewBlake2b256(secondHash),
				prevHash:    lcommon.NewBlake2b256(firstHash),
				blockNumber: fixture.currentTip.BlockNumber + 2,
				slot:        fixture.currentTip.Point.Slot + 2,
			}
			require.NoError(t, fixture.ls.chain.AddBlockHeader(firstHeader))
			require.NoError(t, fixture.ls.chain.AddBlockHeader(secondHeader))

			historyConn := fixture.connId
			if testCase.differentPeer {
				historyConn = testChainsyncConnId(4301, 4302)
			}
			for _, header := range []mockHeader{firstHeader, secondHeader} {
				fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
					ConnectionId: historyConn,
					Point: ocommon.NewPoint(
						header.SlotNumber(),
						header.Hash().Bytes(),
					),
					BlockHeader: header,
				})
			}

			replayedHeader := firstHeader
			if testCase.replayTip {
				replayedHeader = secondHeader
			}
			clearCalls := 0
			fixture.ls.config.ClearSeenHeadersFromFunc = func(uint64) {
				clearCalls++
			}
			fixture.ls.headerMismatchCount = 7
			err := fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
				ConnectionId: fixture.connId,
				Point: ocommon.NewPoint(
					replayedHeader.SlotNumber(),
					replayedHeader.Hash().Bytes(),
				),
				BlockHeader: replayedHeader,
				Tip: ochainsync.Tip{
					Point: ocommon.NewPoint(
						secondHeader.SlotNumber(),
						secondHeader.Hash().Bytes(),
					),
					BlockNumber: secondHeader.BlockNumber(),
				},
			})

			require.NoError(t, err)
			assert.Equal(t, 2, fixture.ls.chain.HeaderCount())
			assert.Equal(t, 7, fixture.ls.headerMismatchCount)
			assert.Zero(t, clearCalls)
			assert.True(
				t,
				pointMatches(
					fixture.ls.chain.HeaderTip().Point,
					ocommon.NewPoint(
						secondHeader.SlotNumber(),
						secondHeader.Hash().Bytes(),
					),
				),
			)
		})
	}
}

// TestTryResolveForkExceedsKDeclinesReconcilingDivergedLedgerTip covers
// rollback-depth bound from the fork-resolution call site: the
// common ancestor here sits 3 blocks behind the fixture's K=2, so live
// reconciliation must decline the rewind rather than force it through, and
// fork resolution must fall back to rejecting the fork and requesting a
// fresh intersection instead of silently truncating past K.
func TestTryResolveForkExceedsKDeclinesReconcilingDivergedLedgerTip(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	putPrimaryChainOnForkBeyondK(t, fixture, "live-fork-resolution")

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	resyncCh := subscribeChainsyncResync(t, bus)

	localTip := fixture.ls.chain.Tip()
	forkHash := testHashBytes("over-k-fork-resolution-block")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: localTip.BlockNumber + 1,
		slot:        localTip.Point.Slot + 10,
	}
	err := fixture.ls.chain.AddBlockHeader(header)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAs(t, err, &notFitErr)

	resolved, err := fixture.ls.tryResolveFork(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockHeader: header,
			Tip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					header.SlotNumber(),
					header.Hash().Bytes(),
				),
				BlockNumber: header.BlockNumber(),
			},
		},
		notFitErr,
		nil,
		false,
	)
	require.NoError(t, err)
	// The not-fit error was handled (a resync was requested), even though
	// the fork itself was rejected rather than adopted.
	require.True(t, resolved)

	// Nothing was rewound: the primary chain, ledger tip, and durable
	// nonces are all exactly as they were before the declined
	// reconciliation.
	assert.Equal(t, localTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.currentNonce, fixture.ls.currentTipBlockNonce),
	)
	assert.Zero(t, fixture.ls.headerMismatchCount)
	assert.Zero(t, fixture.ls.chain.HeaderCount())

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.currentTip, dbTip)

	e := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"expected over-K fork-resolution resync event",
	)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonForkResolutionExceedsK,
		e.Reason,
	)
	assert.Equal(t, fixture.connId, e.ConnectionId)
}

func TestTryResolveForkPropagatesAncestorLookupError(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ancestorLookupErr := errors.New("ancestor lookup failed")
	fixture.ls.lookupBlockByHash = func([]byte) (models.Block, error) {
		return models.Block{}, ancestorLookupErr
	}

	forkHash := testHashBytes("lookup-error-fork-block")
	header := mockHeader{
		hash: lcommon.NewBlake2b256(forkHash),
		prevHash: lcommon.NewBlake2b256(
			testHashBytes("lookup-error-ancestor"),
		),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 10,
	}
	err := fixture.ls.chain.AddBlockHeader(header)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAs(t, err, &notFitErr)

	resolved, err := fixture.ls.tryResolveFork(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockHeader: header,
			Tip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					header.SlotNumber(),
					header.Hash().Bytes(),
				),
				BlockNumber: header.BlockNumber(),
			},
		},
		notFitErr,
		nil,
		false,
	)

	require.False(t, resolved)
	require.ErrorIs(t, err, ancestorLookupErr)
	require.NotErrorIs(t, err, models.ErrBlockNotFound)
}

func TestHandleEventChainsyncBlockHeaderRestoresMismatchCountOnAncestorLookupError(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ancestorLookupErr := errors.New("ancestor lookup failed")
	fixture.ls.lookupBlockByHash = func([]byte) (models.Block, error) {
		return models.Block{}, ancestorLookupErr
	}
	fixture.ls.headerMismatchCount = 7

	forkHash := testHashBytes("handler-lookup-error-fork-block")
	header := mockHeader{
		hash: lcommon.NewBlake2b256(forkHash),
		prevHash: lcommon.NewBlake2b256(
			testHashBytes("handler-lookup-error-ancestor"),
		),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 10,
	}

	err := fixture.ls.handleEventChainsyncBlockHeader(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockHeader: header,
			Tip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					header.SlotNumber(),
					header.Hash().Bytes(),
				),
				BlockNumber: header.BlockNumber(),
			},
		},
	)

	require.ErrorIs(t, err, ancestorLookupErr)
	require.NotErrorIs(t, err, models.ErrBlockNotFound)
	assert.Equal(t, 7, fixture.ls.headerMismatchCount)
}

func TestTryResolveForkDoesNotAdvanceLaggingLedgerTip(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	resyncCh := make(chan event.ChainsyncResyncEvent, 1)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	aheadHash := testHashBytes("raw-chain-ahead-of-ledger")
	require.NoError(t, fixture.ls.chain.AddRawBlocks([]chain.RawBlock{
		{
			Slot:        fixture.currentTip.Point.Slot + 10,
			Hash:        aheadHash,
			BlockNumber: fixture.currentTip.BlockNumber + 1,
			Type:        1,
			PrevHash:    fixture.currentTip.Point.Hash,
			Cbor:        []byte{0x80},
		},
	}))
	require.Equal(
		t,
		fixture.currentTip.Point.Slot+10,
		fixture.ls.chain.Tip().Point.Slot,
	)

	// Simulate the raw primary chain being well ahead of the metadata
	// ledger apply loop during historical catch-up.
	require.NoError(t, fixture.ls.db.SetTip(fixture.ancestorTip, nil))
	fixture.ls.currentTip = fixture.ancestorTip
	fixture.ls.currentTipBlockNonce = append(
		[]byte(nil),
		fixture.ancestorNonce...,
	)
	preRollbackSeq := fixture.ls.lastLocalRollbackSeq

	forkHash := testHashBytes("ahead-raw-chain-fork")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash),
		prevHash:    lcommon.NewBlake2b256(fixture.currentTip.Point.Hash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 20,
	}
	err := fixture.ls.chain.AddBlockHeader(header)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAs(t, err, &notFitErr)

	resolved, err := fixture.ls.tryResolveFork(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockHeader: header,
			// The delivered header forks off the current block and is contiguous
			// with it. The peer advertises a tip further ahead, so its chain is
			// longer than the local raw chain tip and the fork resolves.
			Tip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					header.SlotNumber()+10,
					testHashBytes("ahead-raw-chain-fork-peer-tip-ahead"),
				),
				BlockNumber: header.BlockNumber() + 1,
			},
		},
		notFitErr,
		nil,
		false,
	)
	require.NoError(t, err)
	require.True(t, resolved)

	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.ancestorNonce, fixture.ls.currentTipBlockNonce),
	)
	assert.Equal(t, preRollbackSeq, fixture.ls.lastLocalRollbackSeq)
	assert.Equal(t, 1, fixture.ls.chain.HeaderCount())

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, dbTip)

	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"expected no local rollback resync event",
	)
}

func TestTryResolveForkQueuesKnownPeerForkSegment(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	forkHash1 := testHashBytes("fork-block-1")
	forkHash2 := testHashBytes("fork-block-2")
	forkHash3 := testHashBytes("fork-block-3")
	header1 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash1),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.ancestorTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 1,
	}
	header2 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash2),
		prevHash:    lcommon.NewBlake2b256(forkHash1),
		blockNumber: fixture.ancestorTip.BlockNumber + 2,
		slot:        fixture.currentTip.Point.Slot + 2,
	}
	header3 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash3),
		prevHash:    lcommon.NewBlake2b256(forkHash2),
		blockNumber: fixture.ancestorTip.BlockNumber + 3,
		slot:        fixture.currentTip.Point.Slot + 3,
	}

	fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        ocommon.NewPoint(header1.SlotNumber(), forkHash1),
		BlockHeader:  header1,
	})
	fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        ocommon.NewPoint(header2.SlotNumber(), forkHash2),
		BlockHeader:  header2,
	})

	err := fixture.ls.chain.AddBlockHeader(header3)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAs(t, err, &notFitErr)

	resolved, err := fixture.ls.tryResolveFork(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        ocommon.NewPoint(header3.SlotNumber(), forkHash3),
			BlockHeader:  header3,
			Tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(header3.SlotNumber(), forkHash3),
				BlockNumber: header3.BlockNumber(),
			},
		},
		notFitErr,
		nil,
		false,
	)
	require.NoError(t, err)
	require.True(t, resolved)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.Equal(t, 3, fixture.ls.chain.HeaderCount())

	start, end := fixture.ls.chain.HeaderRange(10)
	assert.Equal(t, uint64(header1.SlotNumber()), start.Slot)
	assert.Equal(t, forkHash1, start.Hash)
	assert.Equal(t, uint64(header3.SlotNumber()), end.Slot)
	assert.Equal(t, forkHash3, end.Hash)
}

func TestTryResolveForkUsesObservedPeerHistoryFallback(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	forkHash1 := testHashBytes("observed-fork-block-1")
	forkHash2 := testHashBytes("observed-fork-block-2")
	forkHash3 := testHashBytes("observed-fork-block-3")
	header1 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash1),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.ancestorTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 1,
	}
	header2 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash2),
		prevHash:    lcommon.NewBlake2b256(forkHash1),
		blockNumber: fixture.ancestorTip.BlockNumber + 2,
		slot:        fixture.currentTip.Point.Slot + 2,
	}
	header3 := mockHeader{
		hash:        lcommon.NewBlake2b256(forkHash3),
		prevHash:    lcommon.NewBlake2b256(forkHash2),
		blockNumber: fixture.ancestorTip.BlockNumber + 3,
		slot:        fixture.currentTip.Point.Slot + 3,
	}

	observedHeaders := map[string]peerHeaderRecord{
		hex.EncodeToString(forkHash1): {
			event: ChainsyncEvent{
				ConnectionId: fixture.connId,
				Point:        ocommon.NewPoint(header1.SlotNumber(), forkHash1),
				BlockHeader:  header1,
			},
			prevHash: append([]byte(nil), fixture.ancestorTip.Point.Hash...),
		},
		hex.EncodeToString(forkHash2): {
			event: ChainsyncEvent{
				ConnectionId: fixture.connId,
				Point:        ocommon.NewPoint(header2.SlotNumber(), forkHash2),
				BlockHeader:  header2,
			},
			prevHash: append([]byte(nil), forkHash1...),
		},
	}
	fixture.ls.config.PeerHeaderLookupFunc = func(
		connId ouroboros.ConnectionId,
		hash []byte,
	) (ChainsyncEvent, []byte, bool) {
		require.Equal(t, fixture.connId, connId)
		record, ok := observedHeaders[hex.EncodeToString(hash)]
		if !ok {
			return ChainsyncEvent{}, nil, false
		}
		return record.event, append([]byte(nil), record.prevHash...), true
	}

	err := fixture.ls.chain.AddBlockHeader(header3)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAs(t, err, &notFitErr)

	resolved, err := fixture.ls.tryResolveFork(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        ocommon.NewPoint(header3.SlotNumber(), forkHash3),
			BlockHeader:  header3,
			Tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(header3.SlotNumber(), forkHash3),
				BlockNumber: header3.BlockNumber(),
			},
		},
		notFitErr,
		nil,
		false,
	)
	require.NoError(t, err)
	require.True(t, resolved)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.Equal(t, 3, fixture.ls.chain.HeaderCount())

	start, end := fixture.ls.chain.HeaderRange(10)
	assert.Equal(t, uint64(header1.SlotNumber()), start.Slot)
	assert.Equal(t, forkHash1, start.Hash)
	assert.Equal(t, uint64(header3.SlotNumber()), end.Slot)
	assert.Equal(t, forkHash3, end.Hash)
}

func TestHandleEventChainsyncBlockHeaderMissingAncestorRequestsResync(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	resyncCh := make(chan event.ChainsyncResyncEvent, 1)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	staleHash := testHashBytes("stale-fork-block")
	stalePrevHash := testHashBytes("missing-ancestor")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(staleHash),
		prevHash:    lcommon.NewBlake2b256(stalePrevHash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 10,
	}
	fixture.ls.bufferedHeaderEvents = map[string][]ChainsyncEvent{
		connIdKey(fixture.connId): {{
			ConnectionId: fixture.connId,
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockHeader: header,
			Tip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					header.SlotNumber(),
					header.Hash().Bytes(),
				),
				BlockNumber: header.BlockNumber(),
			},
		}},
	}

	err := fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point: ocommon.NewPoint(
			header.SlotNumber(),
			header.Hash().Bytes(),
		),
		BlockHeader: header,
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockNumber: header.BlockNumber(),
		},
	})
	require.NoError(t, err)

	resync := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"expected chainsync resync event",
	)
	assert.Equal(t, fixture.connId, resync.ConnectionId)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonRollbackNotFound,
		resync.Reason,
	)

	assert.Zero(t, fixture.ls.headerMismatchCount)
	_, ok := fixture.ls.bufferedHeaderEvents[connIdKey(fixture.connId)]
	assert.False(t, ok)
}

func TestRollbackPublishesChainsyncResyncAtRollbackPoint(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	resyncCh := make(chan event.ChainsyncResyncEvent, 1)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	require.NoError(t, fixture.ls.rollback(fixture.ancestorTip.Point))

	resync := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"expected chainsync resync event",
	)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonLocalLedgerRollback,
		resync.Reason,
	)
	assert.Equal(t, fixture.ancestorTip.Point, resync.Point)
	assert.Equal(t, ouroboros.ConnectionId{}, resync.ConnectionId)
}

func TestRecoverAfterLocalRollbackReplaysPeerHeaderHistory(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	require.NoError(
		t,
		fixture.ls.chain.Rollback(fixture.ancestorTip.Point),
	)
	require.NoError(t, fixture.ls.db.SetTip(fixture.ancestorTip, nil))
	fixture.ls.currentTip = fixture.ancestorTip
	fixture.ls.currentTipBlockNonce = append(
		[]byte(nil),
		fixture.ancestorNonce...,
	)

	connId := fixture.connId
	requestCount := 0
	fixture.ls.config.GetActiveConnectionFunc = func() *ouroboros.ConnectionId {
		return &connId
	}
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		requestConnId ouroboros.ConnectionId,
		start ocommon.Point,
		end ocommon.Point,
	) (uint64, error) {
		requestCount++
		assert.True(t, sameConnectionId(connId, requestConnId))
		assert.Equal(t, uint64(11), start.Slot)
		assert.Equal(t, uint64(12), end.Slot)
		return 0, nil
	}

	header1Hash := lcommon.NewBlake2b256(testHashBytes("rollback-replay-1"))
	header2Hash := lcommon.NewBlake2b256(testHashBytes("rollback-replay-2"))
	header1 := mockHeader{
		hash:        header1Hash,
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.ancestorTip.BlockNumber + 1,
		slot:        fixture.ancestorTip.Point.Slot + 1,
	}
	header2 := mockHeader{
		hash:        header2Hash,
		prevHash:    header1Hash,
		blockNumber: fixture.ancestorTip.BlockNumber + 2,
		slot:        fixture.ancestorTip.Point.Slot + 2,
	}
	fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point: ocommon.NewPoint(
			header1.slot,
			header1.hash.Bytes(),
		),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				header2.slot,
				header2.hash.Bytes(),
			),
			BlockNumber: header2.blockNumber,
		},
		BlockHeader: header1,
	})
	fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point: ocommon.NewPoint(
			header2.slot,
			header2.hash.Bytes(),
		),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				header2.slot,
				header2.hash.Bytes(),
			),
			BlockNumber: header2.blockNumber,
		},
		BlockHeader: header2,
	})

	result := fixture.ls.RecoverAfterLocalRollback(
		[]ouroboros.ConnectionId{fixture.connId},
		fixture.ancestorTip.Point,
	)

	require.True(t, result.Recovered)
	assert.False(t, result.SkipConnectionClose)
	assert.Equal(t, 2, fixture.ls.chain.HeaderCount())
	assert.True(
		t,
		sameConnectionId(fixture.ls.headerPipelineConnId, fixture.connId),
	)
	assert.True(
		t,
		sameConnectionId(
			fixture.ls.selectedBlockfetchConnId,
			fixture.connId,
		),
	)
	assert.True(
		t,
		sameConnectionId(
			fixture.ls.activeBlockfetchConnId,
			fixture.connId,
		),
	)
	assert.Equal(t, 1, requestCount)
	fixture.ls.blockfetchRequestRangeCleanup()
}

// TestRecoverAfterLocalRollbackRetargetsSelectedBlockfetchConn asserts the
// active-peer fallback moves the blockfetch selection, not just the one request
// it issues. nextBlockfetchConnId prefers selectedBlockfetchConnId, so a
// selection left on the failed recovery connection sends the next batch of a
// multi-batch replay straight back to the connection that just failed, and the
// first batch is the only one that ever arrives.
func TestRecoverAfterLocalRollbackRetargetsSelectedBlockfetchConn(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	require.NoError(t, fixture.ls.db.SetTip(fixture.ancestorTip, nil))
	fixture.ls.currentTip = fixture.ancestorTip
	fixture.ls.currentTipBlockNonce = append(
		[]byte(nil),
		fixture.ancestorNonce...,
	)

	activeConnId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6001},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	fixture.ls.config.GetActiveConnectionFunc = func() *ouroboros.ConnectionId {
		return &activeConnId
	}
	// The recovery connection is gone; only the active best peer answers.
	var requested []ouroboros.ConnectionId
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		connId ouroboros.ConnectionId,
		_ ocommon.Point,
		_ ocommon.Point,
	) (uint64, error) {
		requested = append(requested, connId)
		if sameConnectionId(connId, activeConnId) {
			return 0, nil
		}
		return 0, errBlockfetchNoBlocks
	}

	header := mockHeader{
		hash: lcommon.NewBlake2b256(
			testHashBytes("rollback-retarget"),
		),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.ancestorTip.BlockNumber + 1,
		slot:        fixture.ancestorTip.Point.Slot + 1,
	}
	fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point: ocommon.NewPoint(
			header.SlotNumber(),
			header.Hash().Bytes(),
		),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockNumber: header.BlockNumber(),
		},
		BlockHeader: header,
	})

	fixture.ls.RecoverAfterLocalRollback(
		[]ouroboros.ConnectionId{fixture.connId},
		fixture.ancestorTip.Point,
	)

	require.Len(
		t, requested, 2,
		"the failed recovery connection then the active best peer",
	)
	assert.True(
		t, sameConnectionId(requested[1], activeConnId),
		"the fallback request must go to the active best peer",
	)
	assert.True(
		t,
		sameConnectionId(
			fixture.ls.selectedBlockfetchConnId,
			activeConnId,
		),
		"the blockfetch selection must follow the fallback, so the next "+
			"batch does not return to the failed connection",
	)
}

// TestRecoverAfterLocalRollbackClearsSelectionWhenEveryConnectionFails asserts
// the blockfetch selection is not left pointing at a connection that just failed
// to serve the replayed range. handleEventChainsync clears it when its own
// fallbacks are exhausted; this path now does the same, so nothing downstream
// has to depend on being the next writer of the field.
func TestRecoverAfterLocalRollbackClearsSelectionWhenEveryConnectionFails(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	require.NoError(t, fixture.ls.db.SetTip(fixture.ancestorTip, nil))
	fixture.ls.currentTip = fixture.ancestorTip
	fixture.ls.currentTipBlockNonce = append(
		[]byte(nil),
		fixture.ancestorNonce...,
	)

	activeConnId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6002},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3003},
	}
	fixture.ls.config.GetActiveConnectionFunc = func() *ouroboros.ConnectionId {
		return &activeConnId
	}
	// Nothing can serve the range: neither the recovery connection nor the
	// active best peer the fallback reaches for.
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		ouroboros.ConnectionId,
		ocommon.Point,
		ocommon.Point,
	) (uint64, error) {
		return 0, errBlockfetchNoBlocks
	}

	header := mockHeader{
		hash: lcommon.NewBlake2b256(
			testHashBytes("rollback-clear-selection"),
		),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.ancestorTip.BlockNumber + 1,
		slot:        fixture.ancestorTip.Point.Slot + 1,
	}
	fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point: ocommon.NewPoint(
			header.SlotNumber(),
			header.Hash().Bytes(),
		),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockNumber: header.BlockNumber(),
		},
		BlockHeader: header,
	})

	result := fixture.ls.RecoverAfterLocalRollback(
		[]ouroboros.ConnectionId{fixture.connId},
		fixture.ancestorTip.Point,
	)
	require.False(t, result.Recovered)

	assert.Empty(
		t, connIdKey(fixture.ls.selectedBlockfetchConnId),
		"an exhausted recovery must not leave the selection on a "+
			"connection that failed to serve the range",
	)
}

func TestRecoverAfterLocalRollbackReportsBlockfetchFailure(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	require.NoError(t, fixture.ls.db.SetTip(fixture.ancestorTip, nil))
	fixture.ls.currentTip = fixture.ancestorTip
	fixture.ls.currentTipBlockNonce = append(
		[]byte(nil),
		fixture.ancestorNonce...,
	)
	requestCount := 0
	fixture.ls.config.BlockfetchRequestRangeFunc = func(
		ouroboros.ConnectionId,
		ocommon.Point,
		ocommon.Point,
	) (uint64, error) {
		requestCount++
		return 0, errBlockfetchNoBlocks
	}

	header := mockHeader{
		hash:        lcommon.NewBlake2b256(testHashBytes("rollback-no-blocks")),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.ancestorTip.BlockNumber + 1,
		slot:        fixture.ancestorTip.Point.Slot + 1,
	}
	fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point: ocommon.NewPoint(
			header.SlotNumber(),
			header.Hash().Bytes(),
		),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				header.SlotNumber(),
				header.Hash().Bytes(),
			),
			BlockNumber: header.BlockNumber(),
		},
		BlockHeader: header,
	})

	result := fixture.ls.RecoverAfterLocalRollback(
		[]ouroboros.ConnectionId{fixture.connId},
		fixture.ancestorTip.Point,
	)

	assert.False(t, result.Recovered)
	assert.False(t, result.SkipConnectionClose)
	assert.Equal(t, 1, requestCount)
	assert.Equal(t, 1, fixture.ls.chain.HeaderCount())
}

func TestRecoverAfterLocalRollbackResetsStateWithoutTrackedClients(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	header := mockHeader{
		hash:        lcommon.NewBlake2b256(testHashBytes("rollback-reset-1")),
		prevHash:    lcommon.NewBlake2b256(fixture.currentTip.Point.Hash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 1,
	}
	require.NoError(t, fixture.ls.chain.AddBlockHeader(header))

	fixture.ls.headerPipelineConnId = fixture.connId
	fixture.ls.selectedBlockfetchConnId = fixture.connId
	fixture.ls.activeBlockfetchConnId = fixture.connId
	fixture.ls.headerMismatchCount = 3
	fixture.ls.rollbackHistory = []rollbackRecord{
		{
			point: ocommon.Point{
				Slot: fixture.currentTip.Point.Slot,
				Hash: append([]byte(nil), fixture.currentTip.Point.Hash...),
			},
			connKey:   connIdKey(fixture.connId),
			timestamp: time.Now(),
		},
	}
	fixture.ls.bufferedHeaderEvents = map[string][]ChainsyncEvent{
		connIdKey(fixture.connId): {
			{
				ConnectionId: fixture.connId,
				Point: ocommon.NewPoint(
					header.slot,
					header.hash.Bytes(),
				),
				BlockHeader: header,
			},
		},
	}

	result := fixture.ls.RecoverAfterLocalRollback(
		nil,
		fixture.ancestorTip.Point,
	)

	require.False(t, result.Recovered)
	assert.False(t, result.SkipConnectionClose)
	assert.Zero(t, fixture.ls.chain.HeaderCount())
	assert.Zero(t, fixture.ls.headerMismatchCount)
	assert.Nil(t, fixture.ls.rollbackHistory)
	assert.Nil(t, fixture.ls.bufferedHeaderEvents)
	assert.True(
		t,
		fixture.ls.headerPipelineConnId == (ouroboros.ConnectionId{}),
	)
	assert.True(
		t,
		fixture.ls.selectedBlockfetchConnId == (ouroboros.ConnectionId{}),
	)
	assert.True(
		t,
		fixture.ls.activeBlockfetchConnId == (ouroboros.ConnectionId{}),
	)
}

func TestRecoverAfterLocalRollbackDoesNotUsePreRollbackTipAsStalenessSignal(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	result := fixture.ls.RecoverAfterLocalRollback(
		[]ouroboros.ConnectionId{fixture.connId},
		fixture.ancestorTip.Point,
	)

	require.False(t, result.Recovered)
	assert.False(t, result.SkipConnectionClose)
	assert.Zero(t, result.PrimaryChainTipSlot)
}

func TestRecoverAfterLocalRollbackReturnsEmptyResultWhenChainNil(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.chain = nil

	result := fixture.ls.RecoverAfterLocalRollback(
		[]ouroboros.ConnectionId{fixture.connId},
		fixture.ancestorTip.Point,
	)

	assert.Equal(t, LocalRollbackRecoveryResult{}, result)
}

func TestRecoverAfterLocalRollbackSkipsConnectionCloseWhenPrimaryChainTipPastRollbackPoint(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	queuedHeader := mockHeader{
		hash: lcommon.NewBlake2b256(
			testHashBytes("rollback-stale-queued"),
		),
		prevHash:    lcommon.NewBlake2b256(fixture.currentTip.Point.Hash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 1,
	}
	require.NoError(t, fixture.ls.chain.AddBlockHeader(queuedHeader))

	fixture.ls.currentTip = fixture.ancestorTip
	fixture.ls.currentTipBlockNonce = append(
		[]byte(nil),
		fixture.ancestorNonce...,
	)
	fixture.ls.headerMismatchCount = 7
	fixture.ls.lastLocalRollbackSeq = 1
	fixture.ls.lastLocalRollbackPoint = ocommon.Point{
		Slot: fixture.ancestorTip.Point.Slot,
		Hash: append([]byte(nil), fixture.ancestorTip.Point.Hash...),
	}

	result := fixture.ls.RecoverAfterLocalRollback(
		[]ouroboros.ConnectionId{fixture.connId},
		fixture.ancestorTip.Point,
	)

	require.False(t, result.Recovered)
	assert.True(t, result.SkipConnectionClose)
	assert.Equal(
		t,
		fixture.currentTip.Point.Slot,
		result.PrimaryChainTipSlot,
	)
	assert.Equal(t, 1, fixture.ls.chain.HeaderCount())
	assert.Equal(t, 7, fixture.ls.headerMismatchCount)
}

// Reproduces a chainsync recovery hang seen during multi-pool DevNet
// runs. After a slot battle the chain has already extended past `point`
// with the very block that peer history hands back as the only forkPath
// entry. The recovery loop must skip events whose slot is at or below
// the chain's header tip; otherwise AddBlockHeader rejects the duplicate
// with BlockNotFitChainTipError, clearQueuedHeaders fires, and the
// chainsync session never re-converges with the peer.
func TestRecoverPeerHeaderHistoryFromPointSkipsHeadersAlreadyAtChainTip(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	advancedHash := testHashBytes("recovery-already-at-tip")
	advancedSlot := fixture.currentTip.Point.Slot + 1
	advancedBlockNumber := fixture.currentTip.BlockNumber + 1
	require.NoError(
		t,
		fixture.ls.chain.AddRawBlocks([]chain.RawBlock{
			{
				Slot:        advancedSlot,
				Hash:        advancedHash,
				BlockNumber: advancedBlockNumber,
				Type:        1,
				PrevHash:    fixture.currentTip.Point.Hash,
				Cbor:        []byte{0x80},
			},
		}),
	)
	require.Equal(
		t,
		advancedSlot,
		fixture.ls.chain.HeaderTip().Point.Slot,
		"chain header tip must reflect the post-rollback advance",
	)

	advancedHeader := mockHeader{
		hash:        lcommon.NewBlake2b256(advancedHash),
		prevHash:    lcommon.NewBlake2b256(fixture.currentTip.Point.Hash),
		blockNumber: advancedBlockNumber,
		slot:        advancedSlot,
	}
	fixture.ls.recordPeerHeaderHistory(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point: ocommon.NewPoint(
			advancedHeader.slot,
			advancedHeader.hash.Bytes(),
		),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				advancedHeader.slot,
				advancedHeader.hash.Bytes(),
			),
			BlockNumber: advancedHeader.blockNumber,
		},
		BlockHeader: advancedHeader,
	})

	fixture.ls.chainsyncMutex.Lock()
	headerCount, err := fixture.ls.recoverPeerHeaderHistoryFromPointLocked(
		fixture.connId,
		fixture.currentTip.Point,
	)
	fixture.ls.chainsyncMutex.Unlock()

	require.NoError(t, err)
	assert.Zero(t, headerCount)
}

func TestHandleEventChainsyncBlockHeaderIgnoresStaleRollForwardBehindTip(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	resyncCh := make(chan event.ChainsyncResyncEvent, 1)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	staleHash := testHashBytes("stale-roll-forward")
	header := mockHeader{
		hash:        lcommon.NewBlake2b256(staleHash),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.ancestorTip.BlockNumber + 1,
		slot:        fixture.ancestorTip.Point.Slot + 5,
	}

	err := fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point: ocommon.NewPoint(
			header.SlotNumber(),
			header.Hash().Bytes(),
		),
		BlockHeader: header,
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				fixture.currentTip.Point.Slot+10,
				testHashBytes("peer-tip"),
			),
			BlockNumber: fixture.currentTip.BlockNumber + 10,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Zero(t, fixture.ls.headerMismatchCount)
	assert.Zero(t, fixture.ls.chain.HeaderCount())

	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"expected no chainsync resync event",
	)
}

func TestReconcilePrimaryChainTipWithLedgerTipRollsBackMetadata(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	require.NoError(t, fixture.ls.reconcilePrimaryChainTipWithLedgerTip())

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.ancestorNonce, fixture.ls.currentTipBlockNonce),
	)

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, dbTip)
}

func TestReconcilePrimaryChainTipWithLedgerTipRollsBackMissingLedgerTipToCommonAncestor(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	forkHash := testHashBytes("startup-primary-chain-fork")
	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	require.NoError(t, fixture.ls.chain.AddRawBlocks([]chain.RawBlock{
		{
			Slot:        fixture.currentTip.Point.Slot + 5,
			Hash:        forkHash,
			BlockNumber: fixture.currentTip.BlockNumber + 1,
			Type:        1,
			PrevHash:    fixture.ancestorTip.Point.Hash,
			Cbor:        []byte{0x80},
		},
	}))

	require.NoError(t, fixture.ls.reconcilePrimaryChainTipWithLedgerTip())

	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.ancestorNonce, fixture.ls.currentTipBlockNonce),
	)
	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, dbTip)

	rows, err := fixture.ls.db.GetBlockNoncesInSlotRange(
		fixture.ancestorTip.Point.Slot,
		fixture.currentTip.Point.Slot+1,
		nil,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, fixture.ancestorTip.Point.Hash, rows[0].Hash)
}

// TestReconcileLivePrimaryChainLedgerDivergenceExportedWrapperRecoversSubKFork
// pins the wire-in the plateau-watchdog path depends on
// (node_chainsync_recycler.go). Sub-K same-slot fork resolutions can
// advance chain.Tip() to the canonical hash while leaving the ledger
// pipeline pinned on the abandoned hash, with no error returned and
// therefore no caller of the existing in-package live-reconciler.
// The exported wrapper is what node-level watchdog code can invoke
// to repair the divergence in place — without it, the plateau path
// can only recycle the upstream peer, which does not unstick a
// locally-pinned ledger.
func TestReconcileLivePrimaryChainLedgerDivergenceExportedWrapperRecoversSubKFork(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	forkHash := testHashBytes("live-sub-k-fork")
	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	require.NoError(t, fixture.ls.chain.AddRawBlocks([]chain.RawBlock{
		{
			Slot:        fixture.currentTip.Point.Slot + 5,
			Hash:        forkHash,
			BlockNumber: fixture.currentTip.BlockNumber + 1,
			Type:        1,
			PrevHash:    fixture.ancestorTip.Point.Hash,
			Cbor:        []byte{0x80},
		},
	}))
	require.NotEqual(
		t,
		fixture.ls.chain.Tip(),
		fixture.ls.currentTip,
		"test setup must produce a chain/ledger tip divergence",
	)
	require.Equal(
		t,
		fixture.currentTip,
		fixture.ls.currentTip,
		"currentTip must remain at the post-switch abandoned hash",
	)

	reconciled, err := fixture.ls.ReconcileLivePrimaryChainLedgerDivergence(
		"local tip plateau",
		fixture.connId,
	)
	require.NoError(t, err)
	require.True(
		t,
		reconciled,
		"exported reconciler must report success",
	)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.ancestorNonce, fixture.ls.currentTipBlockNonce),
	)
}

func TestReconcileLivePrimaryChainLedgerDivergenceRequestsMithrilResync(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	forkHash := testHashBytes("live-mithril-boundary-fork")
	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	require.NoError(t, fixture.ls.chain.AddRawBlocks([]chain.RawBlock{
		{
			Slot:        fixture.currentTip.Point.Slot + 5,
			Hash:        forkHash,
			BlockNumber: fixture.currentTip.BlockNumber + 1,
			Type:        1,
			PrevHash:    fixture.ancestorTip.Point.Hash,
			Cbor:        []byte{0x80},
		},
	}))
	fixture.ls.mithrilLedgerSlot = fixture.ancestorTip.Point.Slot + 1

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus
	resyncCh := subscribeChainsyncResync(t, bus)

	reconciled, err := fixture.ls.ReconcileLivePrimaryChainLedgerDivergence(
		"local tip plateau",
		fixture.connId,
	)
	require.NoError(t, err)
	require.False(t, reconciled)
	e := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"expected live Mithril-boundary resync event",
	)
	require.Equal(
		t,
		event.ChainsyncResyncReasonRollbackExceedsMithril,
		e.Reason,
	)
	require.Equal(t, fixture.connId, e.ConnectionId)
}

// TestReconcilePrimaryChainTipWithLedgerTipCatchesUpWhenAheadBeyondK covers the
// old-Mithril-snapshot shape: the immutable primary chain is far ahead of the
// ledger tip (more than k blocks), but the ledger tip is still a valid ancestor
// on the primary chain. The primary chain must be preserved so ledgerProcessBlocks
// can replay forward and catch up; it must NOT be rewound (which would delete the
// very blocks needed to catch up).
func TestReconcilePrimaryChainTipWithLedgerTipCatchesUpWhenAheadBeyondK(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.currentEra.Id = 1
	fixture.ls.config.CardanoNodeConfig.ShelleyGenesis().SecurityParam = 2
	prevHash := fixture.currentTip.Point.Hash
	blocks := make([]chain.RawBlock, 0, 3)
	for idx, seed := range []string{
		"startup-primary-chain-ahead-1",
		"startup-primary-chain-ahead-2",
		"startup-primary-chain-ahead-3",
	} {
		hash := testHashBytes(seed)
		blockOffset := uint64(idx + 1)
		blocks = append(blocks, chain.RawBlock{
			Slot:        fixture.currentTip.Point.Slot + blockOffset,
			Hash:        hash,
			BlockNumber: fixture.currentTip.BlockNumber + blockOffset,
			Type:        1,
			PrevHash:    prevHash,
			Cbor:        []byte{0x80},
		})
		prevHash = hash
	}
	require.NoError(t, fixture.ls.chain.AddRawBlocks(blocks))
	aheadTip := fixture.ls.chain.Tip()
	require.Equal(
		t,
		fixture.currentTip.Point.Slot+3,
		aheadTip.Point.Slot,
	)

	require.NoError(t, fixture.ls.reconcilePrimaryChainTipWithLedgerTip())

	// Ledger tip is unchanged: reconcile does not itself advance the ledger,
	// the forward replay happens later in ledgerProcessBlocks.
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.currentTip, dbTip)

	// Primary chain is preserved at its original ahead tip, not rewound.
	assert.Equal(t, aheadTip, fixture.ls.chain.Tip())

	// Every block ahead of the ledger tip still exists so it can be replayed.
	for _, block := range blocks {
		_, err := fixture.ls.chain.BlockByPoint(
			ocommon.NewPoint(block.Slot, block.Hash),
			nil,
		)
		assert.NoError(
			t,
			err,
			"ahead block at slot %d should be preserved for catch-up",
			block.Slot,
		)
	}
}

func TestIntersectPointsDoesNotUsePrimaryChainWhenLedgerTipMissing(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	forkHash := testHashBytes("intersect-primary-chain-fork")
	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	require.NoError(t, fixture.ls.chain.AddRawBlocks([]chain.RawBlock{
		{
			Slot:        fixture.currentTip.Point.Slot + 5,
			Hash:        forkHash,
			BlockNumber: fixture.currentTip.BlockNumber + 1,
			Type:        1,
			PrevHash:    fixture.ancestorTip.Point.Hash,
			Cbor:        []byte{0x80},
		},
	}))

	points, err := fixture.ls.IntersectPoints(4)
	require.NoError(t, err)

	require.Empty(t, points)
}

func TestProcessChainIteratorRollbackAppliesMatchingRollback(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	removedBlock, err := database.BlockByPoint(
		fixture.ls.db,
		fixture.currentTip.Point,
	)
	require.NoError(t, err)
	removedBlock.Cbor = []byte{0xff}
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	fixture.ls.config.EventBus = bus
	errSubID, errCh := bus.Subscribe(LedgerErrorEventType)
	require.NotZero(t, errSubID)
	require.NotNil(t, errCh)
	t.Cleanup(func() { bus.Unsubscribe(LedgerErrorEventType, errSubID) })
	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))

	err = fixture.ls.processChainIteratorRollback(
		t.Context(),
		fixture.ancestorTip.Point,
		[]models.Block{removedBlock},
	)
	require.NoError(t, err)
	evt := testutil.RequireReceive(
		t,
		errCh,
		testutil.AsyncWait,
		"undo decode event for the captured rollback block",
	)
	undoDecodeEvent, ok := evt.Data.(LedgerErrorEvent)
	require.True(t, ok, "undo decode event type")
	require.Equal(t, "rollback_tx_undo_decode", undoDecodeEvent.Operation)
	require.Equal(t, fixture.currentTip.Point, undoDecodeEvent.Point)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.Equal(t, fixture.ancestorNonce, fixture.ls.currentTipBlockNonce)
	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, dbTip)
	_, _, pending, err := loadRollbackIntent(fixture.ls.db)
	require.NoError(t, err)
	assert.False(t, pending)
}

func TestProcessChainIteratorRollbackRetainsIntentAfterMetadataTruncationFailure(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	removedBlock, err := database.BlockByPoint(
		fixture.ls.db,
		fixture.currentTip.Point,
	)
	require.NoError(t, err)

	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	injected := errors.New("injected metadata truncation failure")
	fixture.ls.rollbackTruncateAfterSlotFunc = func(
		ocommon.Point,
		uint64,
		*database.Txn,
	) (ochainsync.Tip, []byte, error) {
		return ochainsync.Tip{}, nil, injected
	}
	err = fixture.ls.processChainIteratorRollback(
		t.Context(),
		fixture.ancestorTip.Point,
		[]models.Block{removedBlock},
	)
	require.ErrorIs(t, err, injected)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
	intentPoint, intentBlocks, pending, err := loadRollbackIntent(fixture.ls.db)
	require.NoError(t, err)
	require.True(t, pending)
	assert.Equal(t, fixture.ancestorTip.Point, intentPoint)
	assert.Equal(t, []models.Block{removedBlock}, intentBlocks)
}

func TestProcessChainIteratorRollbackNoopWhenLedgerAlreadyAtPoint(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	fixture.ls.currentTip = fixture.ancestorTip
	fixture.ls.currentTipBlockNonce = append(
		[]byte(nil),
		fixture.ancestorNonce...,
	)
	require.NoError(t, fixture.ls.db.SetTip(fixture.ancestorTip, nil))

	err := fixture.ls.processChainIteratorRollback(
		t.Context(),
		fixture.ancestorTip.Point,
		nil,
	)
	require.NoError(t, err)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, dbTip)
}

func TestProcessChainIteratorRollbackSkipsStaleRollback(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	currentNonce := append([]byte(nil), fixture.ls.currentTipBlockNonce...)
	err := fixture.ls.processChainIteratorRollback(
		t.Context(),
		fixture.ancestorTip.Point,
		nil,
	)
	require.ErrorIs(t, err, errRestartLedgerPipeline)

	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(currentNonce, fixture.ls.currentTipBlockNonce),
	)

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.currentTip, dbTip)
}

// TestProcessChainIteratorRollbackAppliesStaleRollbackWhenLedgerTipAbandoned
// guards the other half of the stale-vs-current distinction that
// TestProcessChainIteratorRollbackSkipsStaleRollback checks: staleness
// (chain tip != point) alone must NOT decide whether to roll back --
// ls.currentTip's own status against the current chain does. Here
// putPrimaryChainOnForkBeyondK leaves ls.currentTip pointing at a block
// Chain.Rollback has already physically removed (an abandoned fork),
// while the chain itself has moved on to a new fork descended from the
// same ancestor. A stale rollback event reporting that ancestor as the
// fork point must still be applied -- skipping it here (the original bug)
// leaves ls.currentTip stuck on the abandoned block forever, since every
// subsequent pipeline restart re-derives expectedPrevHash from that same
// un-rolled-back tip.
func TestProcessChainIteratorRollbackAppliesStaleRollbackWhenLedgerTipAbandoned(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	putPrimaryChainOnForkBeyondK(t, fixture, "abandoned-ledger-tip")

	// Chain tip is now three blocks into the new fork, well past
	// ancestorTip -- reporting ancestorTip as the rollback point is
	// exactly the stale-vs-current mismatch this function must not use,
	// on its own, to decide whether ls.currentTip needs rolling back.
	require.NotEqual(t, fixture.ancestorTip, fixture.ls.chain.Tip())

	err := fixture.ls.processChainIteratorRollback(
		t.Context(),
		fixture.ancestorTip.Point,
		nil,
	)
	require.ErrorIs(t, err, errRestartLedgerPipeline)

	assert.Equal(t, fixture.ancestorTip, fixture.ls.currentTip)
	assert.True(
		t,
		bytes.Equal(fixture.ancestorNonce, fixture.ls.currentTipBlockNonce),
	)

	dbTip, err := fixture.ls.db.GetTip(nil)
	require.NoError(t, err)
	assert.Equal(t, fixture.ancestorTip, dbTip)
}

func TestProcessChainIteratorRollbackUsesCapturedBlocksAfterChainDeletion(
	t *testing.T,
) {
	fixture := newChainsyncRollbackFixture(t)
	removedBlock, err := database.BlockByPoint(
		fixture.ls.db,
		fixture.currentTip.Point,
	)
	require.NoError(t, err)
	putPrimaryChainOnForkBeyondK(t, fixture, "captured-rollback-payload")

	injected := errors.New("injected metadata truncation failure")
	fixture.ls.rollbackTruncateAfterSlotFunc = func(
		ocommon.Point,
		uint64,
		*database.Txn,
	) (ochainsync.Tip, []byte, error) {
		return ochainsync.Tip{}, nil, injected
	}
	err = fixture.ls.processChainIteratorRollback(
		t.Context(),
		fixture.ancestorTip.Point,
		[]models.Block{removedBlock},
	)
	require.ErrorIs(t, err, injected)

	intentPoint, intentBlocks, pending, err := loadRollbackIntent(fixture.ls.db)
	require.NoError(t, err)
	require.True(t, pending)
	require.Equal(t, fixture.ancestorTip.Point, intentPoint)
	require.Equal(t, []models.Block{removedBlock}, intentBlocks)
}

func TestLedgerProcessBlocksFromSourceRestartsOnStaleIteratorRollback(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	readChainResultCh := make(chan readChainResult, 1)
	readChainResultCh <- readChainResult{
		rollback:      true,
		rollbackPoint: fixture.ancestorTip.Point,
	}
	close(readChainResultCh)

	err := fixture.ls.ledgerProcessBlocksFromSource(
		context.Background(),
		readChainResultCh,
	)
	require.ErrorIs(t, err, errRestartLedgerPipeline)
}

func TestHandleEventChainsyncBlockHeaderIgnoresHistoricalPrimaryHeader(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.config.GenesisSelectionStateFunc = func() (bool, uint64) {
		return true, 100
	}

	// Leave the applied ledger at currentTip while extending the authoritative
	// primary chain two blocks farther. A replayed header for the first of those
	// blocks is historical relative to the primary tip, but it is not a fork.
	block3Hash := testHashBytes("historical-primary-block-3")
	block4Hash := testHashBytes("historical-primary-block-4")
	require.NoError(t, fixture.ls.chain.AddRawBlocks([]chain.RawBlock{
		{
			Slot:        30,
			Hash:        block3Hash,
			BlockNumber: 3,
			Type:        1,
			PrevHash:    fixture.currentTip.Point.Hash,
			Cbor:        []byte{0x80},
		},
		{
			Slot:        40,
			Hash:        block4Hash,
			BlockNumber: 4,
			Type:        1,
			PrevHash:    block3Hash,
			Cbor:        []byte{0x80},
		},
	}))

	err := fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: fixture.connId,
		Point:        ocommon.NewPoint(30, block3Hash),
		BlockHeader: mockHeader{
			hash:        lcommon.NewBlake2b256(block3Hash),
			prevHash:    lcommon.NewBlake2b256(fixture.currentTip.Point.Hash),
			blockNumber: 3,
			slot:        30,
		},
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(40, block4Hash),
			BlockNumber: 4,
		},
	})

	require.NoError(t, err)
	assert.Zero(t, fixture.ls.headerMismatchCount)
	assert.Zero(t, fixture.ls.chain.HeaderCount())
	assert.Equal(
		t,
		ochainsync.Tip{
			Point:       ocommon.NewPoint(40, block4Hash),
			BlockNumber: 4,
		},
		fixture.ls.chain.Tip(),
	)
}

// A Byron epoch-boundary block sits at slot 0 with a real hash, so rejecting
// every slot-zero point would classify a replay of that block as a fork.
// Only the origin point, which carries no hash, is rejected outright.
func TestHeaderAlreadyOnPrimaryChainAcceptsSlotZeroBlock(t *testing.T) {
	t.Parallel()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)
	genesisHash := testHashBytes("byron-boundary-slot-zero")
	abandonedHash := testHashBytes("abandoned-slot-zero-fork")
	// Persist the abandoned block first at ID 1. Adding the primary block
	// below reuses ID 1 for the authoritative index while retaining this blob,
	// matching the append-only fork shape primaryChainContainsBlock must reject.
	require.NoError(t, db.BlockCreate(models.Block{
		ID:     1,
		Slot:   0,
		Hash:   abandonedHash,
		Cbor:   []byte{0x80},
		Type:   1,
		Number: 0,
	}, nil))
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks([]chain.RawBlock{
			{
				Slot:        0,
				Hash:        genesisHash,
				BlockNumber: 0,
				Type:        1,
				Cbor:        []byte{0x80},
			},
		}),
	)
	ls, err := NewLedgerState(
		LedgerStateConfig{
			Database:          db,
			ChainManager:      cm,
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger: slog.New(
				slog.NewJSONHandler(io.Discard, nil),
			),
		},
	)
	require.NoError(t, err)
	localTip := ls.chain.Tip()
	require.Equal(t, uint64(0), localTip.Point.Slot)
	require.Equal(t, genesisHash, localTip.Point.Hash)

	assert.True(t, ls.headerAlreadyOnPrimaryChain(
		ChainsyncEvent{Point: ocommon.NewPoint(0, genesisHash)},
		localTip,
	))
	// Blob presence is insufficient: the abandoned block is still retrievable
	// by hash, but ID 1 now indexes the primary block above.
	assert.False(t, ls.headerAlreadyOnPrimaryChain(
		ChainsyncEvent{Point: ocommon.NewPoint(0, abandonedHash)},
		localTip,
	))
	// The origin point is not a block and is not evidence of a duplicate.
	assert.False(t, ls.headerAlreadyOnPrimaryChain(
		ChainsyncEvent{Point: ocommon.NewPointOrigin()},
		localTip,
	))
	// A competing slot-zero block is still a candidate fork.
	assert.False(t, ls.headerAlreadyOnPrimaryChain(
		ChainsyncEvent{
			Point: ocommon.NewPoint(
				0,
				testHashBytes("competing-slot-zero"),
			),
		},
		localTip,
	))
}

func TestHeaderAlreadyOnPrimaryChainUsesHashIndexPrefilter(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	localTip := fixture.ls.chain.Tip()
	lookupCalls := 0
	fixture.ls.lookupBlockByHash = func([]byte) (models.Block, error) {
		lookupCalls++
		return models.Block{}, models.ErrBlockNotFound
	}

	assert.False(t, fixture.ls.headerAlreadyOnPrimaryChain(
		ChainsyncEvent{
			Point: ocommon.NewPoint(
				localTip.Point.Slot,
				testHashBytes("unknown-fork-header"),
			),
		},
		localTip,
	))
	assert.Equal(t, 1, lookupCalls)
}

func TestHeaderAlreadyOnPrimaryChainSupportsLegacyHashIndexMiss(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	localTip := fixture.ls.chain.Tip()

	txn := fixture.ls.db.BlobTxn(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return fixture.ls.db.Blob().Delete(
			txn.Blob(),
			types.BlockHashIndexKey(localTip.Point.Hash),
		)
	}))

	assert.True(t, fixture.ls.headerAlreadyOnPrimaryChain(
		ChainsyncEvent{Point: localTip.Point},
		localTip,
	))
}

func TestHeaderAlreadyOnPrimaryChainSkipsLookupBeyondLocalTip(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	localTip := fixture.ls.chain.Tip()
	lookupCalled := false
	fixture.ls.lookupBlockByHash = func([]byte) (models.Block, error) {
		lookupCalled = true
		return models.Block{}, errors.New("unexpected lookup")
	}

	assert.False(t, fixture.ls.headerAlreadyOnPrimaryChain(
		ChainsyncEvent{
			Point: ocommon.NewPoint(
				localTip.Point.Slot+1,
				testHashBytes("header-beyond-local-tip"),
			),
		},
		localTip,
	))
	assert.False(t, lookupCalled)
}

func newChainsyncRollbackFixture(t *testing.T) *chainsyncRollbackFixture {
	t.Helper()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, nil)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)

	ancestorHash := testHashBytes("ancestor-block")
	currentHash := testHashBytes("current-block")
	ancestorBlock := chain.RawBlock{
		Slot:        10,
		Hash:        ancestorHash,
		BlockNumber: 1,
		Type:        1,
		Cbor:        []byte{0x80},
	}
	currentBlock := chain.RawBlock{
		Slot:        20,
		Hash:        currentHash,
		BlockNumber: 2,
		Type:        1,
		PrevHash:    ancestorHash,
		Cbor:        []byte{0x80},
	}
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks([]chain.RawBlock{
			ancestorBlock,
			currentBlock,
		}),
	)

	ls, err := NewLedgerState(
		LedgerStateConfig{
			Database:          db,
			ChainManager:      cm,
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger: slog.New(
				slog.NewJSONHandler(io.Discard, nil),
			),
		},
	)
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())

	ancestorTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(ancestorBlock.Slot, ancestorBlock.Hash),
		BlockNumber: ancestorBlock.BlockNumber,
	}
	currentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(currentBlock.Slot, currentBlock.Hash),
		BlockNumber: currentBlock.BlockNumber,
	}
	ancestorNonce := []byte("nonce-ancestor")
	currentNonce := []byte("nonce-current")

	require.NoError(
		t,
		db.SetBlockNonce(
			ancestorTip.Point.Hash,
			ancestorTip.Point.Slot,
			ancestorNonce,
			true,
			nil,
		),
	)
	require.NoError(
		t,
		db.SetBlockNonce(
			currentTip.Point.Hash,
			currentTip.Point.Slot,
			currentNonce,
			false,
			nil,
		),
	)
	require.NoError(t, db.SetTip(currentTip, nil))

	ls.currentTip = currentTip
	ls.currentTipBlockNonce = append([]byte(nil), currentNonce...)
	ls.chainsyncState = SyncingChainsyncState
	ls.publishSnapshotsLocked()

	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}

	return &chainsyncRollbackFixture{
		ls:            ls,
		connId:        connId,
		ancestorTip:   ancestorTip,
		currentTip:    currentTip,
		ancestorNonce: ancestorNonce,
		currentNonce:  currentNonce,
		forkPoint: ocommon.NewPoint(
			currentBlock.Slot+10,
			testHashBytes("fork-point"),
		),
	}
}

func putPrimaryChainOnForkBeyondK(
	t *testing.T,
	fixture *chainsyncRollbackFixture,
	seedPrefix string,
) {
	t.Helper()

	require.NoError(t, fixture.ls.chain.Rollback(fixture.ancestorTip.Point))
	prevHash := fixture.ancestorTip.Point.Hash
	blocks := make([]chain.RawBlock, 0, 3)
	for idx := range 3 {
		blockOffset := uint64(idx + 1)
		hash := testHashBytes(fmt.Sprintf("%s-fork-%d", seedPrefix, idx))
		blocks = append(blocks, chain.RawBlock{
			Slot:        fixture.currentTip.Point.Slot + blockOffset*5,
			Hash:        hash,
			BlockNumber: fixture.ancestorTip.BlockNumber + blockOffset,
			Type:        1,
			PrevHash:    prevHash,
			Cbor:        []byte{0x80},
		})
		prevHash = hash
	}
	require.NoError(t, fixture.ls.chain.AddRawBlocks(blocks))
	require.NotEqual(t, fixture.currentTip, fixture.ls.chain.Tip())
	require.Equal(t, fixture.currentTip, fixture.ls.currentTip)
}

// subscribeChainsyncResync wires a buffered channel to bus's
// ChainsyncResyncEventType, registering the subscribe/unsubscribe cleanup,
// for the several over-K-decline tests that assert on the resulting resync
// event.
func subscribeChainsyncResync(
	t *testing.T,
	bus *event.EventBus,
) chan event.ChainsyncResyncEvent {
	t.Helper()

	resyncCh := make(chan event.ChainsyncResyncEvent, 1)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})
	return resyncCh
}

func testHashBytes(seed string) []byte {
	sum := sha256.Sum256([]byte(seed))
	return append([]byte(nil), sum[:]...)
}

// A peer whose own reported tip is below the Mithril trust boundary is
// merely behind (still syncing or stuck) — its FindIntersect matched an
// old rung of our intersect ladder, which is not evidence of a competing
// fork. The rollback must still be refused, but the resync reason must
// classify the peer as stale rather than divergent so peer governance
// can back off instead of treating it as hostile.
func TestHandleEventChainsyncRollbackClassifiesStalePeerBelowMithrilBoundary(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus
	fixture.ls.mithrilLedgerSlot = fixture.currentTip.Point.Slot

	resyncCh := make(chan event.ChainsyncResyncEvent, 1)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	err := fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.ancestorTip.Point,
			// The peer's own tip sits below our trust boundary.
			Tip: fixture.ancestorTip,
		},
		nil,
	)
	require.NoError(t, err)

	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)

	e := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"expected stale-peer resync event",
	)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonPeerTipBehindMithril,
		e.Reason,
	)
	assert.Equal(t, fixture.connId, e.ConnectionId)
}

// A peer that claims a tip at or above the Mithril trust boundary yet
// asks us to roll back below it does not carry our certified boundary
// block (always offered as an intersect point), so its chain genuinely
// diverges below the trust anchor and must be rejected as divergent.
func TestHandleEventChainsyncRollbackRejectsDivergentPeerTipAboveMithrilBoundary(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus
	fixture.ls.mithrilLedgerSlot = fixture.currentTip.Point.Slot

	resyncCh := make(chan event.ChainsyncResyncEvent, 1)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	err := fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.ancestorTip.Point,
			// The peer claims a tip past our boundary while demanding a
			// rollback below it.
			Tip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					fixture.currentTip.Point.Slot+10,
					testHashBytes("divergent-peer-tip"),
				),
				BlockNumber: fixture.currentTip.BlockNumber + 1,
			},
		},
		nil,
	)
	require.NoError(t, err)

	assert.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	assert.Equal(t, fixture.currentTip, fixture.ls.currentTip)

	e := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"expected divergent-peer resync event",
	)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonRollbackExceedsMithril,
		e.Reason,
	)
	assert.Equal(t, fixture.connId, e.ConnectionId)
}

// TestHandleEventBlockfetchBatchDoneAcceptsShadowCompletion verifies that a
// near-tip shadow peer can finish the batch ahead of a slow primary. Without
// this, BatchDone from the shadow connection was dropped and the batch
// stalled until the primary or the timeout fired, defeating the point of
// dispatching a shadow at all.
func TestHandleEventBlockfetchBatchDoneAcceptsShadowCompletion(t *testing.T) {
	t.Parallel()

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
		// Skip the unobtained-range retry path: pretend the shadow already
		// delivered a block, and that it extended the chain, before sending
		// BatchDone.
		batchBlocksReceived: 1,
		batchBlocksApplied:  1,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				_ = start
				_ = end
				requestCount++
				requestedConnId = connId
				return 0, nil
			},
		},
	}

	require.NoError(
		t,
		handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
			ConnectionId: shadow,
			BatchDone:    true,
		}, nil),
	)

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
	t.Parallel()

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
		batchBlocksApplied:           1,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				_ = connId
				_ = start
				_ = end
				requestCount++
				return 0, nil
			},
		},
	}

	// Primary completes first; cleanup clears the shadow ID and starts the
	// next batch on the same primary.
	require.NoError(
		t,
		handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
			ConnectionId: primary,
			BatchDone:    true,
		}, nil),
	)
	assert.Equal(t, 1, requestCount)
	assert.Equal(t, ouroboros.ConnectionId{}, ls.shadowBlockfetchConnId)

	// The shadow's late BatchDone now arrives. It must not complete the new
	// batch — neither active nor shadow matches it after cleanup.
	require.NoError(
		t,
		handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
			ConnectionId: shadow,
			BatchDone:    true,
		}, nil),
	)
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
	t.Parallel()

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
			) (uint64, error) {
				_ = connId
				_ = start
				_ = end
				return 0, nil
			},
		},
	}

	require.NoError(
		t,
		restartQueuedBlockfetchAfterForkForTest(ls, primary, nil),
	)

	// Stale per-batch shadow state must be cleared by the restart so that
	// the new batch starts in a known state.
	assert.Equal(t, ouroboros.ConnectionId{}, ls.shadowBlockfetchConnId)
	assert.Nil(t, ls.shadowBlockReceivedHashes)
	assert.False(t, ls.firstBlockReceived)

	// A block delivered on the previous shadow connection must be rejected
	// because it is no longer the shadow for the active batch.
	require.NoError(t, handleEventBlockfetchBlockDeferred(ls, BlockfetchEvent{
		ConnectionId: staleShadow,
		Block:        &mockBabbageBlock{slot: 99},
		Point:        ocommon.Point{Slot: 99, Hash: []byte("stale-shadow")},
	}, nil))
	require.Empty(
		t,
		ls.pendingBlockfetchEvents,
		"stale shadow block must not be accepted after fork restart",
	)

	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
}

// TestHandleEventChainsyncBlockHeaderRoutesSlotBattleToForkResolution
// pins the wire-up the eras-DevNet Babbage→Conway VRF wedge depended
// on. Two equal-stake pools regularly forge at the same slot (a "slot
// battle"); both forges arrive at the relay, the relay's Praos
// selector picks one via the deterministic tiebreak, and the loser
// must reach this side via chainsync as a fork-resolution request so
// chainselection can adopt the same winner the relay did. If dingo
// adopted its own forge first and then the peer's competing same-slot
// forge arrives, the chainsync header handler MUST fall through to
// the fork-resolution path — dropping the header as "stale roll
// forward behind local tip" because the slots are equal locks dingo
// onto whichever block it forged or received first, diverges its
// chain from the relay's at the slot battle, and the divergent
// block's VRF output then folds into dingo's evolving nonce. By the
// next epoch boundary the eta0 each side derives disagrees, every
// peer Conway header VRF-fails on dingo, and dingo's own Conway
// forges symmetrically VRF-fail on the relay.
//
// The fixture builds a chain ending at slot 20 with a known hash,
// then delivers a chainsync header at the SAME slot 20 with a
// different hash whose prevHash is the ancestor at slot 10. The
// existing handler treated the equal-slot case as stale and bailed
// before incrementing headerMismatchCount or invoking tryResolveFork.
// Asserting that headerMismatchCount becomes 1 demonstrates the
// header took the not-stale branch and reached the fork-resolution
// gate where chainselection can apply the reference implementation's
// Praos select-view rules.
func TestHandleEventChainsyncBlockHeaderRoutesSlotBattleToForkResolution(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)

	// Competing forge at the same slot as fixture.currentTip but a
	// different hash. Its prevHash is the common ancestor (slot 10)
	// so the fork-resolution path can identify the rollback target.
	competingHash := testHashBytes("slot-battle-loser")
	competingHeader := mockHeader{
		hash:        lcommon.NewBlake2b256(competingHash),
		prevHash:    lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
		blockNumber: fixture.currentTip.BlockNumber,
		slot:        fixture.currentTip.Point.Slot,
	}

	// Sanity-check: chain.AddBlockHeader returns
	// BlockNotFitChainTipError for this header — i.e. the underlying
	// gate the chainsync handler needs to reach. If this assertion
	// ever stops holding, the test no longer exercises the handler's
	// not-fit branch and the slot-battle assertion below is
	// vacuously satisfied; fail loudly here instead.
	addErr := fixture.ls.chain.AddBlockHeader(competingHeader)
	var notFitErr chain.BlockNotFitChainTipError
	require.ErrorAsf(
		t, addErr, &notFitErr,
		"expected chain.AddBlockHeader to reject the competing "+
			"same-slot header with BlockNotFitChainTipError so the "+
			"handler hits its not-fit gate; got err=%v", addErr,
	)
	require.Truef(
		t,
		bytes.Equal(
			fixture.currentTip.Point.Hash,
			fixture.ls.chain.Tip().Point.Hash,
		),
		"chain tip hash must still equal currentTip after the "+
			"failed AddBlockHeader; otherwise the test's notion of "+
			"\"slot-battle at the tip\" no longer holds",
	)

	// Replay the same not-fit scenario through the chainsync handler
	// and assert it routes through the fork-resolution gate, not the
	// stale-drop early return.
	err := fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: fixture.connId,
		BlockHeader:  competingHeader,
		Point: ocommon.NewPoint(
			competingHeader.SlotNumber(),
			competingHeader.Hash().Bytes(),
		),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				competingHeader.SlotNumber(),
				competingHeader.Hash().Bytes(),
			),
			BlockNumber: competingHeader.BlockNumber(),
		},
	})
	require.NoError(t, err)

	// The fork-resolution path either:
	//   - rolls back to the common ancestor (slot 10) and adopts the
	//     peer's competing header → chain.Tip() moves off the
	//     original currentTip; chainsyncState flips to
	//     RollbackChainsyncState.
	//   - declines because the reference implementation's Praos
	//     equal-length tiebreaker is not armed for this peer/header
	//     pair -> headerMismatchCount stays at 1 because nothing in
	//     tryResolveFork's success branches reset it.
	//
	// The stale-drop early return takes neither path: chain stays
	// put, chainsyncState stays SyncingChainsyncState, AND
	// headerMismatchCount stays at zero. Asserting "at least one of
	// those three witnesses changed" pins the routing without
	// over-specifying which side of the tiebreak this particular
	// pair of hashes lands on.
	chainAdvanced := !bytes.Equal(
		fixture.currentTip.Point.Hash,
		fixture.ls.chain.Tip().Point.Hash,
	)
	rolledBack := fixture.ls.chainsyncState == RollbackChainsyncState
	mismatchTracked := fixture.ls.headerMismatchCount > 0
	assert.Truef(
		t,
		chainAdvanced || rolledBack || mismatchTracked,
		"slot-battle header took the stale-drop early return: "+
			"chain.Tip() unchanged, chainsyncState still Syncing, "+
			"and headerMismatchCount=%d. The handler dropped the "+
			"peer's competing same-slot forge before "+
			"chainselection could pick a winner; chains will then "+
			"diverge at every slot battle and the divergence will "+
			"fold into the evolving nonce.",
		fixture.ls.headerMismatchCount,
	)
}

// TestProcessEpochRollover_SnapStakeReadOrdering pins the SNAP read point.
//
// The reference sequence is NEWEPOCH = applyRUpd, MIR, EPOCH; EPOCH = SNAP,
// POOLREAP, ratification/enactment. So exactly two boundary rules precede SNAP —
// the delayed reward update and MIR — and every remaining rule that credits
// reward accounts at the boundary slot follows it: POOLREAP deposit refunds
// (applyPoolRetirements) and enacted treasury withdrawals plus proposal-deposit
// refunds (ProcessEpoch).
//
// The snapshot row itself is still written at the very end of the rollover,
// where the new epoch record and the post-enactment protocol version exist, so
// the read point and the write point are deliberately different places in the
// sequence. This test locks the read point; TestProcessEpochRollover_RewardOrdering
// and TestProcessEpochRollover_OrderingInvariant lock the rest of the sequence.
//
// currentBoundarySPOStakeState is a second read at the same point, resolving
// mark[NewEpoch] for RATIFY's SPO tally. Its position is load
// bearing for the same reason: when no SNAP-point distribution was stashed it
// reconstructs the boundary itself, and the reconstruction counts reward
// deltas up to and including the boundary slot. Moved below
// applyPoolRetirements or ProcessEpoch it would absorb the deposit refunds and
// enactment credits those rules record at that slot, and every SPO-gated
// action would be tallied against a mark that cardano-ledger's SNAP never
// sees.
func TestProcessEpochRollover_SnapStakeReadOrdering(t *testing.T) {
	t.Parallel()

	const targetFunc = "processEpochRollover"

	wantOrder := []string{
		"applyStakeRewards",                 // pre-SNAP: delayed reward update
		"applyMIRCerts",                     // pre-SNAP: Shelley-era INSTANT rule
		"captureEpochBoundarySnapshotStake", // SNAP read point
		"currentBoundarySPOStakeState",      // SNAP read point: RATIFY's copy
		"applyPoolRetirements",              // post-SNAP: POOLREAP refunds
		"ProcessEpoch",                      // post-SNAP: enactment credits
		"captureEpochBoundarySnapshot",      // snapshot write, end of rollover
	}

	seen, observed := observeProcessEpochRolloverCallOrder(
		t,
		targetFunc,
		wantOrder,
	)

	for _, m := range wantOrder {
		require.True(t, seen[m],
			"marker %q not found in %s body — the SNAP read must stay wired "+
				"between the pre-SNAP boundary rules and the boundary rules that "+
				"credit reward accounts after SNAP.",
			m, targetFunc)
	}

	require.Equal(
		t,
		wantOrder,
		observed,
		"SNAP read point in %s drifted. The mark snapshot's stake must be read "+
			"after applyStakeRewards and applyMIRCerts, which precede SNAP in "+
			"cardano-ledger, and before applyPoolRetirements and ProcessEpoch, "+
			"which credit reward accounts at the boundary slot after it. "+
			"Expected %v, observed %v.",
		targetFunc,
		wantOrder,
		observed,
	)
}

// TestCaptureEpochBoundarySnapshotStakeHookInvoked verifies the SNAP-point stake
// hook receives the same boundary identity the persist hook later builds from the
// new epoch record, so the two phases of one capture can be matched.
func TestCaptureEpochBoundarySnapshotStakeHookInvoked(t *testing.T) {
	t.Parallel()

	ls, db := newHookTestLedger(t)

	var called bool
	var got event.EpochTransitionEvent
	ls.SetEpochBoundarySnapshotStakeHook(
		func(_ *database.Txn, evt event.EpochTransitionEvent) error {
			called = true
			got = evt
			return nil
		},
	)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		ls.captureEpochBoundarySnapshotStake(
			txn, models.Epoch{EpochId: 0}, 432000, 0,
		)
		return nil
	}))

	require.True(t, called, "stake hook must be invoked during the rollover")
	require.Equal(t, uint64(0), got.PreviousEpoch)
	require.Equal(t, uint64(1), got.NewEpoch)
	require.Equal(t, uint64(432000), got.BoundarySlot)
	require.Equal(t, uint64(431999), got.SnapshotSlot)
}

// TestCaptureEpochBoundarySnapshotStakeHookFailureDeferred verifies a failed
// SNAP-point read neither aborts the rollover nor leaves its writes behind: the
// persist half then reads the stake itself.
func TestCaptureEpochBoundarySnapshotStakeHookFailureDeferred(t *testing.T) {
	t.Parallel()

	ls, db := newHookTestLedger(t)

	ls.SetEpochBoundarySnapshotStakeHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			// Read hooks must not write, but prove the savepoint covers it.
			if err := db.Metadata().SaveRewardSnapshot(&models.RewardSnapshot{
				Epoch:           evt.NewEpoch,
				SnapshotType:    "mark",
				CapturedSlot:    1,
				BoundarySlot:    1,
				ProtocolVersion: 8,
				Authoritative:   true,
			}, txn.Metadata()); err != nil {
				return err
			}
			return errStakeHookBoom
		},
	)

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		ls.captureEpochBoundarySnapshotStake(
			txn, models.Epoch{EpochId: 0}, 432000, 0,
		)
		return nil
	}))

	snap, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.Nil(t, snap,
		"a failed snap-point read must be rolled back to the savepoint")
}

// errStakeHookBoom is a sentinel failure for the snap-point stake hook test.
var errStakeHookBoom = errors.New("snap-point read boom")

func TestChainsyncValidationStateConcurrentAccess(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		chainsyncState:    SyncingChainsyncState,
		validationEnabled: true,
		mithrilLedgerSlot: 10,
	}
	ls.publishSnapshotsLocked()

	const iterations = 1000

	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			for slot := range iterations {
				_, _ = ls.chainsyncHeaderCryptoPolicy(uint64(slot))
				_, _ = ls.validationStateSnapshot()
				_ = ls.mithrilLedgerSlotSnapshot()
			}
		})
	}

	wg.Go(func() {
		for i := range iterations {
			ls.Lock()
			ls.validationEnabled = i%2 == 0
			ls.mithrilLedgerSlot = uint64(i % 128)
			ls.Unlock()

			if i%3 == 0 {
				ls.setChainsyncState(RollbackChainsyncState)
			} else {
				ls.setChainsyncState(SyncingChainsyncState)
			}
			ls.setChainsyncStateIf(
				RollbackChainsyncState,
				SyncingChainsyncState,
			)
		}
	})

	wg.Wait()
}

func testChainsyncConnId(localPort, remotePort int) ouroboros.ConnectionId {
	return ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: localPort,
		},
		RemoteAddr: &net.TCPAddr{
			IP:   net.IPv4(127, 0, 0, 1),
			Port: remotePort,
		},
	}
}

type mockHeader struct {
	hash        lcommon.Blake2b256
	prevHash    lcommon.Blake2b256
	blockNumber uint64
	slot        uint64
}

type sizedMockHeader struct {
	mockHeader
	cbor []byte
}

func (m sizedMockHeader) Cbor() []byte { return m.cbor }

func (m mockHeader) Hash() lcommon.Blake2b256     { return m.hash }
func (m mockHeader) PrevHash() lcommon.Blake2b256 { return m.prevHash }
func (m mockHeader) BlockNumber() uint64          { return m.blockNumber }
func (m mockHeader) SlotNumber() uint64           { return m.slot }

func (m mockHeader) IssuerVkey() lcommon.IssuerVkey { return lcommon.IssuerVkey{} }
func (m mockHeader) BlockBodySize() uint64          { return 0 }

func (m mockHeader) Era() lcommon.Era { return babbage.EraBabbage }
func (m mockHeader) Cbor() []byte     { return nil }

func (m mockHeader) BlockBodyHash() lcommon.Blake2b256 { return lcommon.Blake2b256{} }

func TestDetectConnectionSwitchHandsOffQueuedHeadersToNewActiveConnection(
	t *testing.T,
) {
	t.Parallel()

	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	})
	require.NoError(t, err)
	require.Equal(t, 1, testChain.HeaderCount())

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	currentConn := connId2
	switchCalls := 0
	requestCount := 0

	ls := &LedgerState{
		chain:                        testChain,
		lastActiveConnId:             &connId1,
		activeBlockfetchConnId:       connId1,
		headerPipelineConnId:         connId1,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		pendingBlockfetchEvents: []BlockfetchEvent{
			{
				ConnectionId: connId1,
				Block:        &mockBabbageBlock{slot: 2},
				Point:        ocommon.Point{Slot: 2, Hash: []byte("block-2")},
			},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &currentConn
			},
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				_ = connId
				_ = start
				_ = end
				requestCount++
				return 0, nil
			},
			ConnectionSwitchFunc: func() {
				switchCalls++
			},
		},
	}

	activeConnId, configured := ls.detectConnectionSwitch(nil)
	require.True(t, configured)
	require.NotNil(t, activeConnId)
	assert.Equal(t, connId2, *activeConnId)
	assert.Equal(t, 1, testChain.HeaderCount())
	// In-flight blockfetch is preserved across the switch so the current batch
	// can complete. selectedBlockfetchConnId is updated so the NEXT batch uses
	// the new connection. requestCount stays 0 — no immediate restart.
	assert.Equal(t, 0, requestCount)
	assert.Equal(t, connId1, ls.activeBlockfetchConnId)
	assert.Equal(t, connId2, ls.selectedBlockfetchConnId)
	assert.Equal(t, ouroboros.ConnectionId{}, ls.headerPipelineConnId)
	require.NotNil(t, ls.chainsyncBlockfetchReadyChan)
	assert.Equal(t, 1, len(ls.pendingBlockfetchEvents))
	assert.Equal(t, 1, switchCalls)

	ls.blockfetchRequestRangeCleanup()
}

func TestDetectConnectionSwitchRechecksLivenessBeforeReactivatingFrontier(
	t *testing.T,
) {
	t.Parallel()

	previousConnId := testChainsyncConnId(6000, 3021)
	activeConnId := testChainsyncConnId(6000, 3022)
	callbackCalls := 0
	ls := &LedgerState{
		lastActiveConnId: &previousConnId,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				callbackCalls++
				if callbackCalls == 1 {
					return &activeConnId
				}
				return nil
			},
		},
	}
	ls.syncUpstreamTipSlot.Store(114220800)

	var pending pendingPublishes
	got, configured := ls.detectConnectionSwitch(&pending)

	assert.True(t, configured)
	assert.Nil(t, got)
	assert.Zero(t, ls.UpstreamTipSlot())
	_, active := ls.UpstreamSyncStatus()
	assert.False(t, active)
}

func TestHandleConnectionClosedEventRetainsAdmittedUpstreamFrontier(
	t *testing.T,
) {
	t.Parallel()

	closedConnId := testChainsyncConnId(6000, 3001)
	equivalentClosedConnId := testChainsyncConnId(6000, 3001)
	otherConnId := testChainsyncConnId(6000, 3002)

	tests := []struct {
		name        string
		activeConn  *ouroboros.ConnectionId
		wantVisible uint64
	}{
		{
			name:        "active connection closed",
			activeConn:  &equivalentClosedConnId,
			wantVisible: 0,
		},
		{
			name:        "no active connection",
			activeConn:  nil,
			wantVisible: 0,
		},
		{
			name:        "different active connection awaits admitted target",
			activeConn:  &otherConnId,
			wantVisible: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ls := &LedgerState{
				config: LedgerStateConfig{
					GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
						return tc.activeConn
					},
				},
			}
			ls.syncUpstreamTipSlot.Store(114220800)
			var pending pendingPublishes
			ls.detectConnectionSwitch(&pending)

			ls.handleConnectionClosedEvent(event.NewEvent(
				ConnectionClosedEventType,
				ConnectionClosedEvent{
					ConnectionId: closedConnId,
				},
			))

			assert.Equal(t, uint64(114220800), ls.syncUpstreamTipSlot.Load())
			assert.Equal(t, tc.wantVisible, ls.UpstreamTipSlot())
		})
	}
}

func TestUpstreamTipSlotPreservesForgingGateAcrossStalePeerReconnect(
	t *testing.T,
) {
	t.Parallel()

	closedConnId := testChainsyncConnId(6000, 3001)
	reconnectedConnId := testChainsyncConnId(6000, 3002)
	activeConnId := &closedConnId
	ls := &LedgerState{
		config: LedgerStateConfig{
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return activeConnId
			},
			ConnectionLiveFunc: func(connId ouroboros.ConnectionId) bool {
				return !sameConnectionId(connId, closedConnId)
			},
		},
	}
	ls.syncUpstreamTipSlot.Store(114220800)
	var pending pendingPublishes
	ls.detectConnectionSwitch(&pending)

	ls.handleConnectionClosedEvent(event.NewEvent(
		ConnectionClosedEventType,
		ConnectionClosedEvent{ConnectionId: closedConnId},
	))
	activeConnId = nil
	require.Equal(t, uint64(0), ls.UpstreamTipSlot())

	activeConnId = &reconnectedConnId
	ls.lastActiveConnId = nil
	ls.detectConnectionSwitch(&pending)
	const stalePeerSlot uint64 = 114220700
	if stalePeerSlot > ls.syncUpstreamTipSlot.Load() {
		ls.syncUpstreamTipSlot.Store(stalePeerSlot)
	}
	assert.Equal(t, uint64(114220800), ls.syncUpstreamTipSlot.Load())
	assert.Zero(t, ls.UpstreamTipSlot())
	target, active := ls.UpstreamSyncStatus()
	assert.True(t, active)
	assert.Zero(t, target)
}

func TestAdvanceUpstreamTipSlotDoesNotPublishWithoutAdmittedTarget(
	t *testing.T,
) {
	t.Parallel()

	activeConnID := testChainsyncConnId(6000, 3041)
	ls := &LedgerState{
		config: LedgerStateConfig{
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &activeConnID
			},
		},
	}
	const admittedSlot uint64 = 114220801
	ls.advanceUpstreamTipSlot(admittedSlot)

	assert.Equal(t, admittedSlot, ls.syncUpstreamTipSlot.Load())
	assert.Zero(t, ls.UpstreamTipSlot())
	_, active := ls.UpstreamSyncStatus()
	assert.True(t, active)
}

func TestHandleChainSwitchAfterCloseRejectsDeadTargetKeepsFrontierHidden(
	t *testing.T,
) {
	t.Parallel()

	closedConnId := testChainsyncConnId(6000, 3011)
	activeConnId := &closedConnId
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return activeConnId
			},
			ConnectionLiveFunc: func(connId ouroboros.ConnectionId) bool {
				return !sameConnectionId(connId, closedConnId)
			},
		},
	}
	ls.syncUpstreamTipSlot.Store(114220800)
	var pending pendingPublishes
	ls.detectConnectionSwitch(&pending)

	// Model the EventBus ordering: the close is applied before its already
	// queued chain-switch event, and the connection manager has no live peer.
	ls.handleConnectionClosedEvent(event.NewEvent(
		ConnectionClosedEventType,
		ConnectionClosedEvent{ConnectionId: closedConnId},
	))
	activeConnId = nil
	ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{NewConnectionId: closedConnId},
	))

	assert.Zero(t, ls.UpstreamTipSlot())
	// A zero upstream frontier is the production forger's peerless state; a
	// dead queued switch must not re-enable the retained sync gate.
	_, active := ls.UpstreamSyncStatus()
	assert.False(t, active)
}

func TestHandleChainSwitchRetainsLiveTargetAcrossSubscriberOrdering(
	t *testing.T,
) {
	t.Parallel()

	targetConnId := testChainsyncConnId(6000, 3031)
	activeConnId := testChainsyncConnId(6000, 3032)
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &activeConnId
			},
			ConnectionLiveFunc: func(connId ouroboros.ConnectionId) bool {
				return sameConnectionId(connId, targetConnId) ||
					sameConnectionId(connId, activeConnId)
			},
		},
	}
	ls.syncUpstreamTipSlot.Store(114220800)
	ls.publishActiveUpstream(activeConnId)

	// A close subscriber can update the active pointer before the queued
	// chain-switch subscriber runs. The new connection has no admitted event
	// yet, so it must not inherit the prior connection's frontier.
	ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{NewConnectionId: targetConnId},
	))

	assert.Equal(t, targetConnId, ls.selectedBlockfetchConnId)
	assert.Zero(t, ls.UpstreamTipSlot())
}

func TestHandoffPipelineOnSwitchDropsStaleQueuedHeadersForNewBufferedPeer(
	t *testing.T,
) {
	t.Parallel()

	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	})
	require.NoError(t, err)
	require.Equal(t, 1, testChain.HeaderCount())

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}

	ls := &LedgerState{
		chain:                testChain,
		headerPipelineConnId: connId1,
		bufferedHeaderEvents: map[string][]ChainsyncEvent{
			connId2.String(): {
				{
					ConnectionId: connId2,
					Point:        ocommon.Point{Slot: 2, Hash: []byte("hdr-2")},
					Tip: ochainsync.Tip{
						Point: ocommon.Point{
							Slot: 2,
							Hash: []byte("hdr-2"),
						},
						BlockNumber: 2,
					},
					BlockHeader: mockHeader{
						hash:        lcommon.NewBlake2b256([]byte("hdr-2")),
						prevHash:    lcommon.NewBlake2b256([]byte("hdr-1")),
						blockNumber: 2,
						slot:        2,
					},
				},
			},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	replayConnId, err := ls.handoffPipelineOnSwitchLocked(connId2, nil)
	require.NoError(t, err)
	assert.Equal(t, connId2, replayConnId)
	assert.Equal(t, 0, testChain.HeaderCount())
	assert.Equal(t, ouroboros.ConnectionId{}, ls.headerPipelineConnId)
	assert.Equal(t, connId2, ls.selectedBlockfetchConnId)
}

func TestHandleEventBlockfetchBlockAllowsBlocksFromActiveBatch(t *testing.T) {
	t.Parallel()

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	ls := &LedgerState{
		activeBlockfetchConnId:       connId1,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		// mockBabbageBlock carries no real VRF/KES material to verify. Mark
		// its slot Mithril-covered so the header crypto gate (:
		// required by default, exempt only for a Mithril-certified slot)
		// exempts it, letting this test isolate batch-ownership bookkeeping
		// from crypto verification.
		mithrilLedgerSlot: 2,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				_ = connId
				_ = start
				_ = end
				return 0, nil
			},
		},
	}

	err := handleEventBlockfetchBlockDeferred(ls, BlockfetchEvent{
		ConnectionId: connId1,
		Block:        &mockBabbageBlock{slot: 2},
		Point:        ocommon.Point{Slot: 2, Hash: []byte("block-2")},
	}, nil)
	require.NoError(t, err)
	require.Len(t, ls.pendingBlockfetchEvents, 1)
	assert.Equal(t, connId1, ls.pendingBlockfetchEvents[0].ConnectionId)
}

func TestHandleEventChainsyncIgnoresClosedConnection(t *testing.T) {
	t.Parallel()

	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	testChain := &chain.Chain{}
	ls := &LedgerState{
		chain: testChain,
		bufferedHeaderEvents: map[string][]ChainsyncEvent{
			connId.String(): {
				{ConnectionId: connId},
			},
		},
		peerHeaderHistory: map[string]*peerHeaderChain{
			connId.String(): {
				order: []string{"hdr-2"},
				byHash: map[string]peerHeaderRecord{
					"hdr-2": {event: ChainsyncEvent{ConnectionId: connId}},
				},
			},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			ConnectionLiveFunc: func(candidate ouroboros.ConnectionId) bool {
				return candidate != connId
			},
		},
	}

	ls.handleEventChainsync(event.NewEvent(
		ChainsyncEventType,
		ChainsyncEvent{
			ConnectionId: connId,
			Point:        ocommon.Point{Slot: 2, Hash: []byte("hdr-2")},
			Tip: ochainsync.Tip{
				Point:       ocommon.Point{Slot: 2, Hash: []byte("hdr-2")},
				BlockNumber: 2,
			},
			BlockHeader: mockHeader{
				hash:        lcommon.NewBlake2b256([]byte("hdr-2")),
				prevHash:    lcommon.NewBlake2b256(nil),
				blockNumber: 2,
				slot:        2,
			},
		},
	))

	assert.Equal(t, 0, testChain.HeaderCount())
	assert.Empty(t, ls.bufferedHeaderEvents[connId.String()])
	assert.Empty(t, ls.peerHeaderHistory[connId.String()])
}

func TestHandleEventBlockfetchBlockAllowsEquivalentConnectionId(t *testing.T) {
	t.Parallel()

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId1Dup := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	require.True(t, sameConnectionId(connId1, connId1Dup))

	ls := &LedgerState{
		activeBlockfetchConnId:       connId1,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		// mockBabbageBlock carries no real VRF/KES material to verify. Mark
		// its slot Mithril-covered so the header crypto gate (:
		// required by default, exempt only for a Mithril-certified slot)
		// exempts it, letting this test isolate connection-equivalence
		// bookkeeping from crypto verification.
		mithrilLedgerSlot: 2,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				_ = connId
				_ = start
				_ = end
				return 0, nil
			},
		},
	}

	err := handleEventBlockfetchBlockDeferred(ls, BlockfetchEvent{
		ConnectionId: connId1Dup,
		Block:        &mockBabbageBlock{slot: 2},
		Point:        ocommon.Point{Slot: 2, Hash: []byte("block-2")},
	}, nil)
	require.NoError(t, err)
	require.Len(t, ls.pendingBlockfetchEvents, 1)
	assert.True(
		t,
		sameConnectionId(connId1, ls.pendingBlockfetchEvents[0].ConnectionId),
	)
}

func TestHandleEventBlockfetchBlockDropsBlocksFromStaleConnection(
	t *testing.T,
) {
	t.Parallel()

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	ls := &LedgerState{
		activeBlockfetchConnId:       connId2,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				_ = connId
				_ = start
				_ = end
				return 0, nil
			},
		},
	}

	err := handleEventBlockfetchBlockDeferred(ls, BlockfetchEvent{
		ConnectionId: connId1,
		Block:        &mockBabbageBlock{slot: 2},
		Point:        ocommon.Point{Slot: 2, Hash: []byte("block-2")},
	}, nil)
	require.NoError(t, err)
	require.Empty(t, ls.pendingBlockfetchEvents)
}

func TestHandleEventBlockfetchBatchDoneUsesSelectedConnectionAfterSwitch(
	t *testing.T,
) {
	t.Parallel()

	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	})
	require.NoError(t, err)

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	requestedConnId := ouroboros.ConnectionId{}
	requestCount := 0

	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       connId1,
		batchBlocksReceived:          1,
		batchBlocksApplied:           1,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				requestCount++
				requestedConnId = connId
				return 0, nil
			},
		},
	}
	ls.publishSnapshotsLocked()
	ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{
			PreviousConnectionId: connId1,
			NewConnectionId:      connId2,
		},
	))

	err = handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
		ConnectionId: connId1,
		BatchDone:    true,
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, requestCount)
	assert.Equal(t, connId2, requestedConnId)
	assert.Equal(t, connId2, ls.activeBlockfetchConnId)

	ls.blockfetchRequestRangeCleanup()
}

func TestHandleEventBlockfetchBatchDoneFallsBackToCurrentConnection(
	t *testing.T,
) {
	t.Parallel()

	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	})
	require.NoError(t, err)

	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	requestedConnId := ouroboros.ConnectionId{}
	requestCount := 0

	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       connId,
		batchBlocksReceived:          1,
		batchBlocksApplied:           1,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				requestCount++
				requestedConnId = connId
				return 0, nil
			},
		},
	}
	ls.publishSnapshotsLocked()

	err = handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
		ConnectionId: connId,
		BatchDone:    true,
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, requestCount)
	assert.Equal(t, connId, requestedConnId)
	assert.Equal(t, connId, ls.activeBlockfetchConnId)

	ls.blockfetchRequestRangeCleanup()
	ls.activeBlockfetchConnId = ouroboros.ConnectionId{}
}

func TestHandleChainSwitchEventUpdatesSelectedBlockfetchConnId(t *testing.T) {
	t.Parallel()

	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	ls := &LedgerState{}

	ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{
			NewConnectionId: connId,
		},
	))

	nextConnId, ok := ls.nextBlockfetchConnId()
	require.True(t, ok)
	assert.Equal(t, connId, nextConnId)
}

func TestHandleChainSwitchEventRequestsFreshCursorWhenPeerAheadWithoutHeaders(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	_, resyncCh := bus.Subscribe(event.ChainsyncResyncEventType)
	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{
			PreviousConnectionId: connId1,
			NewConnectionId:      connId2,
			NewTip: ochainsync.Tip{
				Point:       ocommon.NewPoint(200, []byte("peer-tip")),
				BlockNumber: 10,
			},
		},
	))

	evt := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"expected chain-switch cursor resync event",
	)
	resync, ok := evt.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	assert.Equal(t, connId2, resync.ConnectionId)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonChainSwitchCursorAhead,
		resync.Reason,
	)
}

func TestChainSwitchNeedsFreshCursorUsesObservedTip(
	t *testing.T,
) {
	t.Parallel()

	chainManager, err := chain.NewManager(nil, nil)
	require.NoError(t, err)
	testChain := chainManager.PrimaryChain()
	require.NoError(t, testChain.AddLocalBlock(&mockBabbageBlock{slot: 100}))
	require.Zero(t, testChain.HeaderCount())
	localTip := testChain.Tip()

	connId1 := testChainsyncConnId(6000, 3001)
	connId2 := testChainsyncConnId(6000, 3002)
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	needsFreshCursor := ls.chainSwitchNeedsFreshCursorLocked(
		chainselection.ChainSwitchEvent{
			PreviousConnectionId: connId1,
			NewConnectionId:      connId2,
			NewTip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					math.MaxUint64,
					[]byte("advertised-outlier"),
				),
				BlockNumber: math.MaxUint64,
			},
			NewObservedTip:    localTip,
			NewObservedTipSet: true,
		},
		connId2,
	)
	assert.False(
		t,
		needsFreshCursor,
		"an untrusted advertisement must not force a resync when the delivered frontier is at the local tip",
	)
}

func TestChainSwitchNeedsFreshCursorIgnoresFailedTargetFrontier(
	t *testing.T,
) {
	t.Parallel()

	chainManager, err := chain.NewManager(nil, nil)
	require.NoError(t, err)
	testChain := chainManager.PrimaryChain()
	require.NoError(t, testChain.AddLocalBlock(&mockBabbageBlock{slot: 100}))
	require.Zero(t, testChain.HeaderCount())
	localTip := testChain.Tip()

	previousConnId := testChainsyncConnId(6000, 3001)
	failedTargetConnId := testChainsyncConnId(6000, 3002)
	fallbackConnId := testChainsyncConnId(6000, 3003)
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetPeerObservedTipFunc: func(
				connId ouroboros.ConnectionId,
			) (ochainsync.Tip, bool) {
				if sameConnectionId(connId, fallbackConnId) {
					return localTip, true
				}
				return ochainsync.Tip{}, false
			},
		},
	}

	needsFreshCursor := ls.chainSwitchNeedsFreshCursorLocked(
		chainselection.ChainSwitchEvent{
			PreviousConnectionId: previousConnId,
			NewConnectionId:      failedTargetConnId,
			NewObservedTip: ochainsync.Tip{
				Point: ocommon.NewPoint(
					localTip.Point.Slot+100,
					[]byte("failed-target"),
				),
				BlockNumber: localTip.BlockNumber + 100,
			},
		},
		fallbackConnId,
	)
	assert.False(
		t,
		needsFreshCursor,
		"a failed target's observed frontier must not drive recovery for the fallback connection",
	)
}

type chainSwitchFallbackFixture struct {
	ls             *LedgerState
	resyncCh       <-chan event.Event
	previousConnId ouroboros.ConnectionId
	targetConnId   ouroboros.ConnectionId
	activeConnId   ouroboros.ConnectionId
}

func newChainSwitchFallbackFixture(
	t *testing.T,
) chainSwitchFallbackFixture {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	_, resyncCh := bus.Subscribe(event.ChainsyncResyncEventType)

	connId1 := testChainsyncConnId(6000, 3001)
	connId2 := testChainsyncConnId(6000, 3002)
	connId3 := testChainsyncConnId(6000, 3003)
	currentConn := connId3
	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("stale-hdr")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	})
	require.NoError(t, err)

	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &currentConn
			},
			ConnectionLiveFunc: func(connId ouroboros.ConnectionId) bool {
				return sameConnectionId(connId, connId3)
			},
			GetPeerObservedTipFunc: func(
				connId ouroboros.ConnectionId,
			) (ochainsync.Tip, bool) {
				if sameConnectionId(connId, connId3) {
					return ochainsync.Tip{
						Point: ocommon.NewPoint(
							200,
							[]byte("active-tip"),
						),
						BlockNumber: 10,
					}, true
				}
				return ochainsync.Tip{}, false
			},
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				_ = start
				_ = end
				if sameConnectionId(connId, connId2) {
					testChain.ClearHeaders()
					return 0, errors.New("connection closed")
				}
				return 0, nil
			},
		},
	}

	return chainSwitchFallbackFixture{
		ls:             ls,
		resyncCh:       resyncCh,
		previousConnId: connId1,
		targetConnId:   connId2,
		activeConnId:   connId3,
	}
}

func (f chainSwitchFallbackFixture) handleChainSwitchEvent() {
	f.ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{
			PreviousConnectionId: f.previousConnId,
			NewConnectionId:      f.targetConnId,
			NewTip: ochainsync.Tip{
				Point:       ocommon.NewPoint(200, []byte("peer-tip")),
				BlockNumber: 10,
			},
		},
	))
}

func TestHandleChainSwitchEventFallbackResyncUsesActiveConnection(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainSwitchFallbackFixture(t)
	fixture.handleChainSwitchEvent()

	evt := testutil.RequireReceive(
		t,
		fixture.resyncCh,
		testutil.AsyncWait,
		"expected fallback chain-switch cursor resync event",
	)
	resync, ok := evt.Data.(event.ChainsyncResyncEvent)
	require.True(t, ok)
	assert.Equal(t, fixture.activeConnId, resync.ConnectionId)
	assert.Equal(
		t,
		event.ChainsyncResyncReasonChainSwitchCursorAhead,
		resync.Reason,
	)
	assert.Equal(t, fixture.activeConnId, fixture.ls.selectedBlockfetchConnId)
}

func TestHandleChainSwitchEventFallbackReplaysBufferedActiveHeaders(
	t *testing.T,
) {
	t.Parallel()

	bufferedHeaderHash := lcommon.NewBlake2b256([]byte("active-hdr"))
	fixture := newChainSwitchFallbackFixture(t)

	fixture.ls.bufferedHeaderEvents = map[string][]ChainsyncEvent{
		connIdKey(fixture.activeConnId): {{
			ConnectionId: fixture.activeConnId,
			BlockHeader: mockHeader{
				hash:        bufferedHeaderHash,
				prevHash:    lcommon.NewBlake2b256(nil),
				blockNumber: 1,
				slot:        1,
			},
			Point: ocommon.NewPoint(1, bufferedHeaderHash.Bytes()),
			Tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(10, []byte("tip")),
				BlockNumber: 10,
			},
		}},
	}

	fixture.handleChainSwitchEvent()

	testutil.WaitForCondition(
		t,
		func() bool {
			fixture.ls.chainsyncMutex.Lock()
			defer fixture.ls.chainsyncMutex.Unlock()
			return sameConnectionId(
				fixture.ls.headerPipelineConnId,
				fixture.activeConnId,
			) &&
				fixture.ls.chain.HeaderCount() == 1 &&
				len(
					fixture.ls.bufferedHeaderEvents[connIdKey(fixture.activeConnId)],
				) == 0
		},
		testutil.AsyncWait,
		"expected buffered active headers to replay after fallback handoff",
	)
	testutil.RequireNoReceive(
		t,
		fixture.resyncCh,
		200*time.Millisecond,
		"active buffered headers should not trigger fresh cursor resync",
	)
}

func TestHandleChainSwitchEventDoesNotResyncInitialPeerSelection(
	t *testing.T,
) {
	t.Parallel()

	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	_, resyncCh := bus.Subscribe(event.ChainsyncResyncEventType)
	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{
			NewConnectionId: connId,
			NewTip: ochainsync.Tip{
				Point:       ocommon.NewPoint(200, []byte("peer-tip")),
				BlockNumber: 10,
			},
		},
	))

	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"initial peer selection should not trigger resync",
	)
}

func TestShouldBufferHeaderEventDoesNotPreserveIdleSelectedConnection(
	t *testing.T,
) {
	t.Parallel()

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}

	ls := &LedgerState{
		chain:                    &chain.Chain{},
		selectedBlockfetchConnId: connId1,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &connId1
			},
		},
	}

	buffered := ls.shouldBufferHeaderEvent(ChainsyncEvent{
		ConnectionId: connId2,
		Point:        ocommon.NewPoint(1, []byte("hdr-1")),
	})
	require.False(t, buffered)
	assert.True(t, sameConnectionId(ls.headerPipelineConnId, connId2))
	assert.Equal(t, ouroboros.ConnectionId{}, ls.selectedBlockfetchConnId)
}

// TestShouldBufferHeaderEventDoesNotRaceDiscardBufferedPeerHeaders guards a
// real data race: shouldBufferHeaderEvent used to determine the current
// header pipeline owner (currentHeaderPipelineOwner, under
// chainsyncBlockfetchMutex) and then write headerPipelineConnId in a
// SEPARATE, unprotected step afterward, while discardBufferedPeerHeaders
// (and every other mutator) correctly holds chainsyncBlockfetchMutex
// around its own read/write of that same field -- so header admission and
// a concurrent batch-completion/clear could genuinely race on
// headerPipelineConnId. This runs both concurrently under go test -race,
// which fails the test outright if that race still exists; there is
// nothing else to assert; a clean run (no race detected) is the pass
// condition.
func TestShouldBufferHeaderEventDoesNotRaceDiscardBufferedPeerHeaders(
	t *testing.T,
) {
	t.Parallel()

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}

	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 200 {
			ls.shouldBufferHeaderEvent(ChainsyncEvent{
				ConnectionId: connId1,
				Point:        ocommon.NewPoint(uint64(i), []byte("hdr")),
			})
		}
	}()
	go func() {
		defer wg.Done()
		for range 200 {
			ls.discardBufferedPeerHeaders(connId2)
		}
	}()
	wg.Wait()
}

// TestDiscardBufferedPeerHeadersDoesNotRaceBufferedHeaderIteration guards the
// bufferedHeaderEvents map itself, which is a different field from the
// headerPipelineConnId race above.
//
// handleEventBlockfetch holds chainsyncBlockfetchMutex for its whole batch-done
// path, and nextBufferedHeaderConnId ranges over bufferedHeaderEvents inside
// it. discardBufferedPeerHeaders runs on handleEventChainsync's dispatch
// goroutine, which holds only chainsyncMutex, and used to delete from that same
// map before taking chainsyncBlockfetchMutex -- a concurrent map iteration and
// map write, which is fatal at runtime rather than merely racy. It was observed
// killing a mainnet block producer inside nextBufferedHeaderConnId.
//
// This runs both paths concurrently under go test -race; a clean run is the
// pass condition, so there is nothing else to assert.
func TestDiscardBufferedPeerHeadersDoesNotRaceBufferedHeaderIteration(
	t *testing.T,
) {
	t.Parallel()

	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Several connections so the range in nextBufferedHeaderConnId has real
	// work to do and overlaps the concurrent deletes.
	const conns = 50
	connIds := make([]ouroboros.ConnectionId, conns)
	for i := range connIds {
		connIds[i] = ouroboros.ConnectionId{
			LocalAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
			RemoteAddr: &net.TCPAddr{
				IP:   net.ParseIP("127.0.0.1"),
				Port: 3001 + i,
			},
		}
		ls.bufferHeaderEvent(ChainsyncEvent{
			ConnectionId: connIds[i],
			Point:        ocommon.NewPoint(uint64(i), []byte("hdr")),
		})
	}

	var wg sync.WaitGroup
	wg.Add(4)
	// Mirrors handleEventBlockfetch: the read side holds
	// chainsyncBlockfetchMutex across the iteration.
	go func() {
		defer wg.Done()
		for range 200 {
			ls.chainsyncBlockfetchMutex.Lock()
			ls.nextBufferedHeaderConnId()
			ls.chainsyncBlockfetchMutex.Unlock()
		}
	}()
	// Mirrors handleEventChainsync's dispatch goroutine.
	go func() {
		defer wg.Done()
		for i := range 200 {
			ls.discardBufferedPeerHeaders(connIds[i%conns])
		}
	}()
	// The buffering write path, which bufferedHeaderMutex alone protects.
	// claimHeaderPipelineOwnership releases chainsyncBlockfetchMutex on
	// return, so this write never holds that lock -- it is the case the
	// previous, narrower fix missed, and it fails here without the
	// dedicated mutex.
	go func() {
		defer wg.Done()
		for i := range 200 {
			ls.bufferHeaderEvent(ChainsyncEvent{
				ConnectionId: connIds[i%conns],
				Point:        ocommon.NewPoint(uint64(i), []byte("hdr")),
			})
		}
	}()
	// The resync delete path, which reaches the map from callers that do
	// not all hold chainsyncBlockfetchMutex.
	go func() {
		defer wg.Done()
		var pending pendingPublishes
		for i := range 200 {
			ls.requestChainsyncResync(
				connIds[i%conns],
				"race probe",
				&pending,
			)
		}
	}()
	wg.Wait()
}

func TestHandleChainSwitchEventReplaysBufferedHeadersForSelectedConnection(
	t *testing.T,
) {
	t.Parallel()

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	headerHash := lcommon.NewBlake2b256([]byte("hdr-1"))
	ls := &LedgerState{
		chain:                &chain.Chain{},
		headerPipelineConnId: connId1,
		bufferedHeaderEvents: map[string][]ChainsyncEvent{
			connIdKey(connId2): {{
				ConnectionId: connId2,
				BlockHeader: mockHeader{
					hash:        headerHash,
					prevHash:    lcommon.NewBlake2b256(nil),
					blockNumber: 1,
					slot:        1,
				},
				Point: ocommon.NewPoint(1, headerHash.Bytes()),
				Tip: ochainsync.Tip{
					Point:       ocommon.NewPoint(10, []byte("tip")),
					BlockNumber: 10,
				},
			}},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				_ = connId
				_ = start
				_ = end
				return 0, nil
			},
		},
	}

	ls.handleChainSwitchEvent(event.NewEvent(
		chainselection.ChainSwitchEventType,
		chainselection.ChainSwitchEvent{
			PreviousConnectionId: connId1,
			NewConnectionId:      connId2,
		},
	))

	require.Eventually(t, func() bool {
		ls.chainsyncMutex.Lock()
		defer ls.chainsyncMutex.Unlock()
		return sameConnectionId(ls.headerPipelineConnId, connId2) &&
			ls.chain.HeaderCount() == 1 &&
			len(ls.bufferedHeaderEvents[connIdKey(connId2)]) == 0
	}, testutil.AsyncWait, 10*time.Millisecond)
}

func TestHandleEventChainsyncBlockHeaderAcceptsCompatibleNonOwnerConnection(
	t *testing.T,
) {
	t.Parallel()

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	header1Hash := lcommon.NewBlake2b256([]byte("hdr-1"))
	header2Hash := lcommon.NewBlake2b256([]byte("hdr-2"))
	header1 := mockHeader{
		hash:        header1Hash,
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}
	header2 := mockHeader{
		hash:        header2Hash,
		prevHash:    header1Hash,
		blockNumber: 2,
		slot:        2,
	}
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId1,
		BlockHeader:  header1,
		Point:        ocommon.NewPoint(header1.slot, header1.hash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(60001, []byte("tip-1")),
			BlockNumber: 60001,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, connId1, ls.headerPipelineConnId)
	assert.Equal(t, 1, ls.chain.HeaderCount())

	err = ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId2,
		BlockHeader:  header2,
		Point:        ocommon.NewPoint(header2.slot, header2.hash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(60002, []byte("tip-2")),
			BlockNumber: 60002,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, connId2, ls.headerPipelineConnId)
	assert.Equal(t, connId2, ls.selectedBlockfetchConnId)
	assert.Equal(t, 2, ls.chain.HeaderCount())
	require.Empty(t, ls.bufferedHeaderEvents[connIdKey(connId2)])
}

func TestHandleEventChainsyncRecordsOnlyAdmittedHeaderFrontier(t *testing.T) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	ls := fixture.ls
	// Keep the test at header admission; no blockfetch worker is needed.
	ls.chainsyncBlockfetchReadyChan = make(chan struct{})
	connID := fixture.connId
	ls.config.GetActiveConnectionFunc = func() *ouroboros.ConnectionId {
		return &connID
	}
	ls.publishActiveUpstream(connID)
	assert.Zero(
		t,
		ls.UpstreamTipSlot(),
		"selection alone must not publish a target",
	)

	// This header is accepted and establishes the initial upstream tip.
	accepted := mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("accepted-header-2")),
		prevHash:    lcommon.NewBlake2b256(fixture.currentTip.Point.Hash),
		blockNumber: fixture.currentTip.BlockNumber + 1,
		slot:        fixture.currentTip.Point.Slot + 1,
	}
	// mockHeader carries no real VRF/KES material to verify. Mark its slot
	// Mithril-covered so the header crypto gate (: required by
	// default, exempt only for a Mithril-certified slot) exempts it, letting
	// this test isolate frontier-tracking from crypto verification.
	ls.mithrilLedgerSlot = accepted.slot
	ls.publishSnapshotsLocked()
	advertisedSlot := ^uint64(0)
	require.NoError(t, ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connID,
		BlockHeader:  accepted,
		Point:        ocommon.NewPoint(accepted.slot, accepted.hash.Bytes()),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				advertisedSlot,
				[]byte("unbound-advertised-tip"),
			),
			BlockNumber: advertisedSlot,
		},
		SyncTarget: ochainsync.Tip{
			Point: ocommon.NewPoint(accepted.slot, []byte("accepted-target")),
		},
		SyncTargetTrusted: true,
	}))
	require.Equal(t, accepted.slot, ls.syncUpstreamTipSlot.Load())
	assert.Equal(t, accepted.slot, ls.UpstreamTipSlot())

	// The next header does not extend the queued chain. Its advertised tip
	// must not advance shared progress state before fork handling rejects it.
	rejected := mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("rejected-header")),
		prevHash:    lcommon.NewBlake2b256([]byte("unknown-parent")),
		blockNumber: 3,
		slot:        3,
	}
	require.NoError(t, ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connID,
		BlockHeader:  rejected,
		Point:        ocommon.NewPoint(rejected.slot, rejected.hash.Bytes()),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				advertisedSlot-1,
				[]byte("rejected-tip"),
			),
			BlockNumber: advertisedSlot - 1,
		},
		SyncTarget: ochainsync.Tip{
			Point: ocommon.NewPoint(
				advertisedSlot-1,
				[]byte("rejected-target"),
			),
		},
	}))
	assert.Equal(t, accepted.slot, ls.syncUpstreamTipSlot.Load())
	assert.Equal(t, accepted.slot, ls.UpstreamTipSlot(),
		"a rejected header must not publish its advertised target")
}

func TestHandleEventChainsyncBlockHeaderBuffersIncompatibleNonOwnerConnection(
	t *testing.T,
) {
	t.Parallel()

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	header1Hash := lcommon.NewBlake2b256([]byte("hdr-1"))
	header2Hash := lcommon.NewBlake2b256([]byte("hdr-2"))
	header1 := mockHeader{
		hash:        header1Hash,
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}
	header2 := mockHeader{
		hash:        header2Hash,
		prevHash:    lcommon.NewBlake2b256([]byte("other-parent")),
		blockNumber: 2,
		slot:        2,
	}
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	// mockHeader carries no real VRF/KES material to verify. Mark its slot
	// Mithril-covered so the header crypto gate (: required by
	// default, exempt only for a Mithril-certified slot) exempts it, letting
	// this test isolate connection-buffering from crypto verification.
	ls.mithrilLedgerSlot = header1.slot
	ls.publishSnapshotsLocked()

	err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId1,
		BlockHeader:  header1,
		Point:        ocommon.NewPoint(header1.slot, header1.hash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(60001, []byte("tip-1")),
			BlockNumber: 60001,
		},
	})
	require.NoError(t, err)
	require.Equal(t, header1.slot, ls.syncUpstreamTipSlot.Load())

	err = ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId2,
		BlockHeader:  header2,
		Point:        ocommon.NewPoint(header2.slot, header2.hash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(^uint64(0), []byte("unbound-tip-2")),
			BlockNumber: ^uint64(0),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, connId1, ls.headerPipelineConnId)
	assert.Equal(t, 1, ls.chain.HeaderCount())
	assert.Equal(t, header1.slot, ls.syncUpstreamTipSlot.Load())
	events := ls.bufferedHeaderEvents[connIdKey(connId2)]
	require.Len(t, events, 1)
	assert.Equal(
		t,
		header2.slot,
		events[0].Point.Slot,
	)
}

func TestHandleEventChainsyncBlockHeader_ProcessesEligibleNonActivePeer(
	t *testing.T,
) {
	t.Parallel()

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	hash1 := lcommon.NewBlake2b256([]byte("hdr-1"))
	testChain := &chain.Chain{}
	var requestedConn ouroboros.ConnectionId
	ls := &LedgerState{
		chain:            testChain,
		lastActiveConnId: &connId1,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				_ = start
				_ = end
				requestedConn = connId
				return 0, nil
			},
		},
	}

	err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId2,
		Point:        ocommon.NewPoint(1, hash1.Bytes()),
		BlockHeader: mockHeader{
			hash:        hash1,
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 1,
			slot:        1,
		},
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(1, hash1.Bytes()),
			BlockNumber: 1,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, testChain.HeaderCount())
	assert.Equal(t, connId2, requestedConn)
	assert.Equal(t, connId2, ls.activeBlockfetchConnId)
}

func TestHandleEventChainsyncBlockHeaderBuffersMinimumBatchWhenBehind(
	t *testing.T,
) {
	t.Parallel()

	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	hash1 := lcommon.NewBlake2b256([]byte("hdr-1"))
	requestCount := 0

	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				_ = connId
				_ = start
				_ = end
				requestCount++
				return 0, nil
			},
		},
	}

	err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId,
		Point:        ocommon.NewPoint(1, hash1.Bytes()),
		BlockHeader: mockHeader{
			hash:        hash1,
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 1,
			slot:        1,
		},
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(200, []byte("tip-200")),
			BlockNumber: 200,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, ls.chain.HeaderCount())
	assert.Equal(t, 0, requestCount)
	assert.Equal(t, connId, ls.headerPipelineConnId)
}

func TestHandleEventChainsyncBlockHeaderScalesBatchWhenFarBehind(t *testing.T) {
	t.Parallel()

	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	requestCount := 0

	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				_ = connId
				_ = start
				_ = end
				requestCount++
				return 0, nil
			},
		},
	}

	// gapBlocks > 1000 puts the runway in the deepest catchup bucket
	// (minHeaders = 256), so we send enough headers to cross that
	// threshold and trigger exactly one batch. Headers added past the
	// trigger queue against the in-flight batch (the test's
	// BlockfetchRequestRangeFunc mock never completes), so requestCount
	// stays at 1 — that's the "scales up but doesn't re-fire while
	// in-flight" guarantee this test pins.
	const totalHeaders = 260
	prevHash := lcommon.NewBlake2b256(nil)
	for i := 1; i <= totalHeaders; i++ {
		headerHash := lcommon.NewBlake2b256(fmt.Appendf(nil, "hdr-%d", i))
		err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
			ConnectionId: connId,
			Point:        ocommon.NewPoint(uint64(i), headerHash.Bytes()),
			BlockHeader: mockHeader{
				hash:        headerHash,
				prevHash:    prevHash,
				blockNumber: uint64(i),
				slot:        uint64(i),
			},
			Tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(1280, []byte("tip-1280")),
				BlockNumber: 1280,
			},
		})
		require.NoError(t, err)
		prevHash = headerHash
		if i == 7 {
			assert.Equal(t, 0, requestCount,
				"no batch before runway accumulates")
		}
	}

	assert.Equal(t, 1, requestCount)
	assert.Equal(t, totalHeaders, ls.chain.HeaderCount())
}

func TestHandleEventChainsyncBlockHeaderAcceptsEquivalentOwnerConnectionId(
	t *testing.T,
) {
	t.Parallel()

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId1Dup := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	require.True(t, sameConnectionId(connId1, connId1Dup))

	header1Hash := lcommon.NewBlake2b256([]byte("hdr-1"))
	header2Hash := lcommon.NewBlake2b256([]byte("hdr-2"))
	header1 := mockHeader{
		hash:        header1Hash,
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}
	header2 := mockHeader{
		hash:        header2Hash,
		prevHash:    header1Hash,
		blockNumber: 2,
		slot:        2,
	}
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId1,
		BlockHeader:  header1,
		Point:        ocommon.NewPoint(header1.slot, header1.hash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(60001, []byte("tip-1")),
			BlockNumber: 60001,
		},
	})
	require.NoError(t, err)

	err = ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId1Dup,
		BlockHeader:  header2,
		Point:        ocommon.NewPoint(header2.slot, header2.hash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(60002, []byte("tip-2")),
			BlockNumber: 60002,
		},
	})
	require.NoError(t, err)
	assert.True(t, sameConnectionId(ls.headerPipelineConnId, connId1Dup))
	assert.Equal(t, 2, ls.chain.HeaderCount())
	assert.Empty(t, ls.bufferedHeaderEvents)
}

func TestHandleEventBlockfetchBatchDoneReplaysBufferedHeadersAfterDrain(
	t *testing.T,
) {
	t.Parallel()

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	headerHash := lcommon.NewBlake2b256([]byte("hdr-2"))
	ls := &LedgerState{
		chain:                        &chain.Chain{},
		activeBlockfetchConnId:       connId1,
		selectedBlockfetchConnId:     connId2,
		headerPipelineConnId:         connId1,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		bufferedHeaderEvents: map[string][]ChainsyncEvent{
			connIdKey(connId2): {{
				ConnectionId: connId2,
				BlockHeader: mockHeader{
					hash:        headerHash,
					prevHash:    lcommon.NewBlake2b256(nil),
					blockNumber: 1,
					slot:        1,
				},
				Point: ocommon.NewPoint(1, headerHash.Bytes()),
				Tip: ochainsync.Tip{
					Point:       ocommon.NewPoint(60001, []byte("tip-2")),
					BlockNumber: 60001,
				},
			}},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	// mockHeader carries no real VRF/KES material to verify. Mark its slot
	// Mithril-covered so the header crypto gate (: required by
	// default, exempt only for a Mithril-certified slot) exempts it, letting
	// this test isolate buffered-header replay from crypto verification.
	ls.mithrilLedgerSlot = 1
	ls.publishSnapshotsLocked()

	err := handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
		ConnectionId: connId1,
		BatchDone:    true,
	}, nil)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		ls.chainsyncMutex.Lock()
		defer ls.chainsyncMutex.Unlock()
		return sameConnectionId(ls.headerPipelineConnId, connId2) &&
			len(ls.bufferedHeaderEvents[connIdKey(connId2)]) == 0 &&
			ls.chain.HeaderCount() == 1 &&
			ls.syncUpstreamTipSlot.Load() == 1
	}, testutil.AsyncWait, 10*time.Millisecond)
	assert.True(t, sameConnectionId(ls.headerPipelineConnId, connId2))
	assert.Equal(t, 1, ls.chain.HeaderCount())
	assert.Equal(t, uint64(1), ls.syncUpstreamTipSlot.Load())
}

func TestHandleEventChainsyncBlockHeaderKeepsActiveBatchOwner(t *testing.T) {
	t.Parallel()

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	ls := &LedgerState{
		chain:                        &chain.Chain{},
		activeBlockfetchConnId:       connId1,
		selectedBlockfetchConnId:     connId2,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	header1Hash := lcommon.NewBlake2b256([]byte("hdr-1"))
	err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId2,
		BlockHeader: mockHeader{
			hash:        header1Hash,
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 1,
			slot:        1,
		},
		Point: ocommon.NewPoint(1, header1Hash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(60001, []byte("tip-1")),
			BlockNumber: 60001,
		},
	})
	require.NoError(t, err)
	assert.True(t, sameConnectionId(ls.headerPipelineConnId, connId1))
	assert.Equal(t, 0, ls.chain.HeaderCount())
	require.Len(t, ls.bufferedHeaderEvents[connIdKey(connId2)], 1)

	header2Hash := lcommon.NewBlake2b256([]byte("hdr-2"))
	err = ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId1,
		BlockHeader: mockHeader{
			hash:        header2Hash,
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 2,
			slot:        2,
		},
		Point: ocommon.NewPoint(2, header2Hash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(60002, []byte("tip-2")),
			BlockNumber: 60002,
		},
	})
	require.NoError(t, err)
	assert.True(t, sameConnectionId(ls.headerPipelineConnId, connId1))
	assert.Equal(t, 1, ls.chain.HeaderCount())
}

func TestHandleEventChainsyncBlockHeaderIgnoresIdleSelectedOwner(
	t *testing.T,
) {
	t.Parallel()

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	headerHash := lcommon.NewBlake2b256([]byte("hdr-idle-selected"))
	ls := &LedgerState{
		chain:                    &chain.Chain{},
		selectedBlockfetchConnId: connId2,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: connId1,
		BlockHeader: mockHeader{
			hash:        headerHash,
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 1,
			slot:        1,
		},
		Point: ocommon.NewPoint(1, headerHash.Bytes()),
		Tip: ochainsync.Tip{
			Point:       ocommon.NewPoint(60001, []byte("tip-1")),
			BlockNumber: 60001,
		},
	})
	require.NoError(t, err)
	assert.True(t, sameConnectionId(ls.headerPipelineConnId, connId1))
	assert.Equal(t, 1, ls.chain.HeaderCount())
	assert.Empty(t, ls.bufferedHeaderEvents)
	assert.Equal(t, ouroboros.ConnectionId{}, ls.selectedBlockfetchConnId)
}

func TestHandleEventChainsyncBlockHeaderIgnoresStaleHeaderBehindChainTip(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.currentTip = fixture.ancestorTip
	staleHash := lcommon.NewBlake2b256([]byte("stale-header"))
	err := fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: fixture.connId,
		BlockHeader: mockHeader{
			hash:        staleHash,
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 2,
			slot:        fixture.ancestorTip.Point.Slot + 5,
		},
		Point: ocommon.NewPoint(
			fixture.ancestorTip.Point.Slot+5,
			staleHash.Bytes(),
		),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				fixture.currentTip.Point.Slot+10,
				[]byte("tip-30"),
			),
			BlockNumber: fixture.currentTip.BlockNumber + 1,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 0, fixture.ls.headerMismatchCount)
	assert.Equal(t, 0, fixture.ls.chain.HeaderCount())
	assert.Equal(
		t,
		fixture.currentTip.Point.Slot,
		fixture.ls.chain.Tip().Point.Slot,
	)
}

func TestHandleEventChainsyncBlockHeaderSkipsMithrilBoundaryHeaderVerification(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	fixture.ls.validationEnabled = true
	fixture.ls.mithrilLedgerSlot = fixture.currentTip.Point.Slot

	staleHash := lcommon.NewBlake2b256([]byte("mithril-stale-header"))
	err := fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
		ConnectionId: fixture.connId,
		BlockHeader: mockHeader{
			hash:        staleHash,
			prevHash:    lcommon.NewBlake2b256(nil),
			blockNumber: 2,
			slot:        fixture.ancestorTip.Point.Slot + 5,
		},
		Point: ocommon.NewPoint(
			fixture.ancestorTip.Point.Slot+5,
			staleHash.Bytes(),
		),
		Tip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				fixture.currentTip.Point.Slot+10,
				[]byte("tip-after-mithril"),
			),
			BlockNumber: fixture.currentTip.BlockNumber + 1,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 0, fixture.ls.headerMismatchCount)
	assert.Equal(t, 0, fixture.ls.chain.HeaderCount())
	assert.Equal(
		t,
		fixture.currentTip.Point.Slot,
		fixture.ls.chain.Tip().Point.Slot,
	)
}

func TestHandleEventChainsyncRollbackClearsBufferedHeadersForNonActivePeer(
	t *testing.T,
) {
	t.Parallel()

	activeConn := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	bufferedConn := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	bufferedHash := lcommon.NewBlake2b256([]byte("buffered-header"))
	ls := &LedgerState{
		bufferedHeaderEvents: map[string][]ChainsyncEvent{
			connIdKey(bufferedConn): {{
				ConnectionId: bufferedConn,
				BlockHeader: mockHeader{
					hash:        bufferedHash,
					prevHash:    lcommon.NewBlake2b256(nil),
					blockNumber: 1,
					slot:        1,
				},
				Point: ocommon.NewPoint(1, bufferedHash.Bytes()),
				Tip: ochainsync.Tip{
					Point:       ocommon.NewPoint(10, []byte("tip")),
					BlockNumber: 10,
				},
			}},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &activeConn
			},
		},
	}

	err := ls.handleEventChainsyncRollback(ChainsyncEvent{
		ConnectionId: bufferedConn,
		Point:        ocommon.NewPoint(0, nil),
	}, nil)
	require.NoError(t, err)
	assert.Empty(t, ls.bufferedHeaderEvents[connIdKey(bufferedConn)])
}

func TestHandleEventChainsyncBlockHeaderStartsBlockfetchForSmallBlockGap(
	t *testing.T,
) {
	t.Parallel()

	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	requestCount := 0
	ls := &LedgerState{
		chain: &chain.Chain{},
		currentTip: ochainsync.Tip{
			Point:       ocommon.NewPoint(1000, []byte("local-tip")),
			BlockNumber: 100,
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				requestConnId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				requestCount++
				assert.Equal(t, connId, requestConnId)
				assert.Equal(t, uint64(1064), start.Slot)
				assert.Equal(t, uint64(1064), end.Slot)
				return 0, nil
			},
		},
	}

	prevHash := lcommon.NewBlake2b256(nil)
	for i := range 5 {
		hash := lcommon.NewBlake2b256(fmt.Appendf(nil, "hdr-%d", i))
		slot := uint64(1064 + i*4)
		err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
			ConnectionId: connId,
			BlockHeader: mockHeader{
				hash:        hash,
				prevHash:    prevHash,
				blockNumber: 101 + uint64(i),
				slot:        slot,
			},
			Point: ocommon.NewPoint(slot, hash.Bytes()),
			Tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(1080, []byte("peer-tip")),
				BlockNumber: 105,
			},
		})
		require.NoError(t, err)
		prevHash = hash
	}

	assert.Equal(t, 1, requestCount)
	assert.True(t, sameConnectionId(ls.activeBlockfetchConnId, connId))
	ls.blockfetchRequestRangeCleanup()
}

func TestHandleEventChainsyncBlockHeaderStartsBlockfetchForSparseBlockGap(
	t *testing.T,
) {
	t.Parallel()

	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	requestCount := 0
	ls := &LedgerState{
		chain: &chain.Chain{},
		currentTip: ochainsync.Tip{
			Point:       ocommon.NewPoint(107374005, []byte("local-tip")),
			BlockNumber: 4123854,
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				requestConnId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				requestCount++
				assert.Equal(t, connId, requestConnId)
				assert.Equal(t, uint64(107374026), start.Slot)
				assert.Equal(t, uint64(107374047), end.Slot)
				return 0, nil
			},
		},
	}

	headerSlots := []uint64{
		107374026,
		107374033,
		107374047,
		107374530,
	}
	var prevHash lcommon.Blake2b256
	for i, slot := range headerSlots {
		hash := lcommon.NewBlake2b256(
			fmt.Appendf(nil, "sparse-hdr-%d", i),
		)
		err := ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
			ConnectionId: connId,
			BlockHeader: mockHeader{
				hash:        hash,
				prevHash:    prevHash,
				blockNumber: 4123855 + uint64(i),
				slot:        slot,
			},
			Point: ocommon.NewPoint(slot, hash.Bytes()),
			Tip: ochainsync.Tip{
				Point:       ocommon.NewPoint(107374509, []byte("peer-tip")),
				BlockNumber: 4123873,
			},
		})
		require.NoError(t, err)
		prevHash = hash
	}

	assert.Equal(t, 1, requestCount)
	assert.True(t, sameConnectionId(ls.activeBlockfetchConnId, connId))
	ls.blockfetchRequestRangeCleanup()
}

func TestHandleEventChainsyncAwaitReplyStartsBlockfetchForActiveConnection(
	t *testing.T,
) {
	t.Parallel()

	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	testChain := &chain.Chain{}
	var prevHash lcommon.Blake2b256
	for i, slot := range []uint64{1001, 1002, 1003, 1004} {
		hash := lcommon.NewBlake2b256(
			fmt.Appendf(nil, "await-reply-hdr-%d", i),
		)
		err := testChain.AddBlockHeader(mockHeader{
			hash:        hash,
			prevHash:    prevHash,
			blockNumber: 200 + uint64(i),
			slot:        slot,
		})
		require.NoError(t, err)
		prevHash = hash
	}

	requestCount := 0
	activeConn := connId
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &activeConn
			},
			BlockfetchRequestRangeFunc: func(
				requestConnId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				requestCount++
				assert.Equal(t, connId, requestConnId)
				assert.Equal(t, uint64(1001), start.Slot)
				assert.Equal(t, uint64(1004), end.Slot)
				return 0, nil
			},
		},
	}

	ls.handleEventChainsyncAwaitReply(
		event.NewEvent(
			ChainsyncAwaitReplyEventType,
			ChainsyncAwaitReplyEvent{ConnectionId: connId},
		),
	)

	assert.Equal(t, 1, requestCount)
	assert.True(t, sameConnectionId(ls.activeBlockfetchConnId, connId))
	ls.blockfetchRequestRangeCleanup()
}

func TestHandleEventBlockfetchBatchDoneEmptyBatchRetriesAlternateConnection(
	t *testing.T,
) {
	t.Parallel()

	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	})
	require.NoError(t, err)

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	requestedConnIds := make([]ouroboros.ConnectionId, 0, 1)

	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       connId1,
		selectedBlockfetchConnId:     connId2,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &connId2
			},
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				_ = start
				_ = end
				requestedConnIds = append(requestedConnIds, connId)
				return 0, nil
			},
		},
	}
	ls.publishSnapshotsLocked()

	err = handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
		ConnectionId: connId1,
		BatchDone:    true,
	}, nil)
	require.NoError(t, err)
	require.Equal(t, []ouroboros.ConnectionId{connId2}, requestedConnIds)
	assert.Equal(t, connId2, ls.activeBlockfetchConnId)
	require.NotNil(t, ls.chainsyncBlockfetchReadyChan)

	ls.blockfetchRequestRangeCleanup()
}

func TestHandleEventBlockfetchBatchDoneEmptyBatchNearTipRetries(
	t *testing.T,
) {
	t.Parallel()

	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("near-tip-header")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        4,
	})
	require.NoError(t, err)

	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	requestCount := 0
	ls := &LedgerState{
		chain:                        testChain,
		activeBlockfetchConnId:       connId,
		selectedBlockfetchConnId:     connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			BlockfetchRequestRangeFunc: func(
				_ ouroboros.ConnectionId,
				_ ocommon.Point,
				_ ocommon.Point,
			) (uint64, error) {
				requestCount++
				return 0, nil
			},
		},
	}
	ls.publishSnapshotsLocked()

	err = handleEventBlockfetchBatchDoneForTest(ls, BlockfetchEvent{
		ConnectionId: connId,
		BatchDone:    true,
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, 1, requestCount)
	assert.Equal(t, 1, testChain.HeaderCount())
	assert.Equal(t, connId, ls.activeBlockfetchConnId)
	assert.NotNil(t, ls.chainsyncBlockfetchReadyChan)

	ls.blockfetchRequestRangeCleanup()
}

func TestHandleBlockfetchTimeoutLocked_RetriesQueuedRangeUsingActivePeer(
	t *testing.T,
) {
	t.Parallel()

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	shadowConnId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3003},
	}
	hash1 := lcommon.NewBlake2b256([]byte("hdr-1"))
	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        hash1,
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	})
	require.NoError(t, err)

	var requestedConn ouroboros.ConnectionId
	ls := &LedgerState{
		chain:                  testChain,
		activeBlockfetchConnId: connId1,
		shadowBlockfetchConnId: shadowConnId,
		blockfetchRequestsInFlight: map[string][]chan struct{}{
			connIdKey(connId1):      {make(chan struct{})},
			connIdKey(shadowConnId): {make(chan struct{})},
		},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &connId2
			},
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				_ = start
				_ = end
				requestedConn = connId
				return 0, nil
			},
		},
	}

	handleBlockfetchTimeoutForTest(ls, connId1, nil)

	assert.Equal(t, connId2, requestedConn)
	assert.Equal(t, connId2, ls.activeBlockfetchConnId)
	assert.Equal(t, 1, testChain.HeaderCount())
	assert.NotContains(t, ls.blockfetchRequestsInFlight, connIdKey(connId1))
	assert.NotContains(t, ls.blockfetchRequestsInFlight, connIdKey(shadowConnId))
	assert.Contains(t, ls.blockfetchRequestsInFlight, connIdKey(connId2))
}

func TestHandleConnectionClosedReleasesRequestWithoutBatchDone(t *testing.T) {
	t.Parallel()

	connId := testChainsyncConnId(6120, 3001)
	done := make(chan struct{})
	ls := &LedgerState{
		chain: &chain.Chain{},
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
		blockfetchRequestsInFlight: map[string][]chan struct{}{
			connIdKey(connId): {done},
		},
	}

	ls.handleConnectionClosedEvent(event.NewEvent(
		ConnectionClosedEventType,
		ConnectionClosedEvent{ConnectionId: connId},
	))

	assert.NotContains(t, ls.blockfetchRequestsInFlight, connIdKey(connId))
	select {
	case <-done:
	default:
		t.Fatal("connection close did not release blockfetch request waiter")
	}
}

// TestHandleBlockfetchTimeoutLocked_RetryRetargetsSelection asserts a timeout
// retry moves the blockfetch selection to the connection it retries on.
// nextBlockfetchConnId prefers selectedBlockfetchConnId, and the
// batch-completion continuation uses it to choose the next batch's connection,
// so a selection left on the timed-out peer recovers one batch from the working
// peer and sends the next straight back to the one that just timed out.
func TestHandleBlockfetchTimeoutLocked_RetryRetargetsSelection(
	t *testing.T,
) {
	t.Parallel()

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	testChain := &chain.Chain{}
	require.NoError(t, testChain.AddBlockHeader(mockHeader{
		hash:        lcommon.NewBlake2b256([]byte("hdr-retarget-1")),
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	}))

	var requestedConn ouroboros.ConnectionId
	ls := &LedgerState{
		chain:                  testChain,
		activeBlockfetchConnId: connId1,
		// Starts on the connection that is about to time out, so the
		// assertion below cannot pass without the retarget.
		selectedBlockfetchConnId: connId1,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &connId2
			},
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				_ ocommon.Point,
				_ ocommon.Point,
			) (uint64, error) {
				requestedConn = connId
				return 0, nil
			},
		},
	}

	handleBlockfetchTimeoutForTest(ls, connId1, nil)

	require.Equal(t, connId2, requestedConn)
	assert.Equal(
		t, connId2, ls.selectedBlockfetchConnId,
		"the selection must follow the retry, so the next batch does not "+
			"return to the timed-out connection",
	)
}

func TestHandleBlockfetchTimeoutLocked_ClearsActiveConnectionWithoutHeaders(
	t *testing.T,
) {
	t.Parallel()

	connId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}

	ls := &LedgerState{
		chain:                        &chain.Chain{},
		activeBlockfetchConnId:       connId,
		chainsyncBlockfetchReadyChan: make(chan struct{}),
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	handleBlockfetchTimeoutForTest(ls, connId, nil)

	assert.Equal(t, ouroboros.ConnectionId{}, ls.activeBlockfetchConnId)
	assert.Nil(t, ls.chainsyncBlockfetchReadyChan)
	_, ok := ls.nextBlockfetchConnId()
	assert.False(t, ok)
}

func TestHandleBlockfetchTimeoutLocked_RetryFailureUsesAlternateSelectedPeer(
	t *testing.T,
) {
	t.Parallel()

	connId1 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
	}
	connId2 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3002},
	}
	connId3 := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3003},
	}
	hash1 := lcommon.NewBlake2b256([]byte("hdr-1"))
	testChain := &chain.Chain{}
	err := testChain.AddBlockHeader(mockHeader{
		hash:        hash1,
		prevHash:    lcommon.NewBlake2b256(nil),
		blockNumber: 1,
		slot:        1,
	})
	require.NoError(t, err)

	requestedConnIds := make([]ouroboros.ConnectionId, 0, 2)
	ls := &LedgerState{
		chain:                    testChain,
		activeBlockfetchConnId:   connId1,
		selectedBlockfetchConnId: connId3,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetActiveConnectionFunc: func() *ouroboros.ConnectionId {
				return &connId2
			},
			BlockfetchRequestRangeFunc: func(
				connId ouroboros.ConnectionId,
				start ocommon.Point,
				end ocommon.Point,
			) (uint64, error) {
				_ = start
				_ = end
				requestedConnIds = append(requestedConnIds, connId)
				if connId == connId2 {
					return 0, errors.New("retry failed")
				}
				return 0, nil
			},
		},
	}

	handleBlockfetchTimeoutForTest(ls, connId1, nil)

	require.Equal(
		t,
		[]ouroboros.ConnectionId{connId2, connId3},
		requestedConnIds,
	)
	assert.Equal(t, connId3, ls.activeBlockfetchConnId)
	require.NotNil(t, ls.chainsyncBlockfetchReadyChan)
	assert.Equal(t, 1, testChain.HeaderCount())
}

// TestChainSwitchNewObservedTipKeysOnPresenceNotZeroValue covers the
// advertising-only peer this path exists to distrust.
//
// A zero delivered frontier is a real observation: the peer delivered nothing.
// Inferring "field absent" from it fell back to the advertised NewTip, which
// handed that peer's advertisement to ledger cursor recovery. The fallback now
// keys on NewObservedTipSet, which every producer in chainselection sets, so
// only a producer that never populated the field reaches the advertised tip.
func TestChainSwitchNewObservedTipKeysOnPresenceNotZeroValue(t *testing.T) {
	t.Parallel()

	advertised := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 9_000, Hash: []byte{0xaa}},
		BlockNumber: 900,
	}
	delivered := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 100, Hash: []byte{0xbb}},
		BlockNumber: 10,
	}

	t.Run("delivered frontier is used when set", func(t *testing.T) {
		got := chainSwitchNewObservedTip(chainselection.ChainSwitchEvent{
			NewTip:            advertised,
			NewObservedTip:    delivered,
			NewObservedTipSet: true,
		})
		assert.Equal(t, delivered, got)
	})

	t.Run(
		"zero delivered frontier is not the advertised tip",
		func(t *testing.T) {
			got := chainSwitchNewObservedTip(chainselection.ChainSwitchEvent{
				NewTip:            advertised,
				NewObservedTipSet: true,
			})
			assert.Equal(t, ochainsync.Tip{}, got)
			assert.NotEqual(t, advertised, got)
		},
	)

	t.Run("unset falls back to the advertised tip", func(t *testing.T) {
		// Older events and direct unit-test or integration constructors.
		got := chainSwitchNewObservedTip(chainselection.ChainSwitchEvent{
			NewTip: advertised,
		})
		assert.Equal(t, advertised, got)
	})
}

// switchHandoffRequestCount queues headerCount headers and reports the
// number of RequestRange calls made by a handoff onto a new connection whose
// sync target is peerTip.
func switchHandoffRequestCount(
	t *testing.T,
	headerCount int,
	peerTip ochainsync.Tip,
	peerTipKnown bool,
) int {
	t.Helper()
	testChain, _ := buildDeepCatchupChain(t, headerCount)
	newConn := testChainsyncConnId(6410, 3002)
	requestCount := 0
	ls := &LedgerState{
		chain: testChain,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetPeerSyncTargetFunc: func(
				connId ouroboros.ConnectionId,
			) (ochainsync.Tip, bool) {
				if !sameConnectionId(connId, newConn) {
					return ochainsync.Tip{}, false
				}
				return peerTip, peerTipKnown
			},
			BlockfetchRequestRangeFunc: func(
				ouroboros.ConnectionId,
				ocommon.Point,
				ocommon.Point,
			) (uint64, error) {
				requestCount++
				return 1, nil
			},
		},
	}
	ls.chainsyncBlockfetchMutex.Lock()
	_, err := ls.handoffPipelineOnSwitchLocked(newConn, nil)
	ls.chainsyncBlockfetchMutex.Unlock()
	require.NoError(t, err)
	assert.Equal(t, newConn, ls.selectedBlockfetchConnId)
	ls.blockfetchRequestRangeCleanup()
	return requestCount
}

func TestHandoffPipelineOnSwitchAccumulatesMinimumBatchWhenFarBehind(
	t *testing.T,
) {
	t.Parallel()

	farTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 1_000_000, Hash: []byte("far")},
		BlockNumber: 50_000,
	}
	assert.Equal(
		t,
		0,
		switchHandoffRequestCount(t, 2, farTip, true),
		"2 queued headers is below the 256 minimum while far behind",
	)
}

func TestHandoffPipelineOnSwitchStartsOnceMinimumBatchQueuedWhenFarBehind(
	t *testing.T,
) {
	t.Parallel()

	// A 100-block gap needs 32 headers.
	tip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 10_000, Hash: []byte("far")},
		BlockNumber: 140,
	}
	assert.Equal(t, 0, switchHandoffRequestCount(t, 31, tip, true))
	assert.Equal(t, 1, switchHandoffRequestCount(t, 32, tip, true))
}

func TestHandoffPipelineOnSwitchStartsImmediatelyNearTip(t *testing.T) {
	t.Parallel()

	nearTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 10, Hash: []byte("near")},
		BlockNumber: 10,
	}
	assert.Equal(
		t,
		1,
		switchHandoffRequestCount(t, 2, nearTip, true),
		"a switch within the gap threshold of the peer tip must not wait",
	)
}

func TestHandoffPipelineOnSwitchStartsImmediatelyWithoutPeerTarget(
	t *testing.T,
) {
	t.Parallel()

	assert.Equal(
		t,
		1,
		switchHandoffRequestCount(t, 2, ochainsync.Tip{}, false),
		"an unknown peer target keeps the immediate start",
	)
}

// A switch that defers blockfetch leaves queued headers with no in-flight
// batch, so the header pipeline owner is the only thing keeping another
// peer's non-fitting header from clearing the queue.
func TestHandoffPipelineOnSwitchAccumulatingKeepsQueueOwned(t *testing.T) {
	t.Parallel()

	testChain, _ := buildDeepCatchupChain(t, 2)
	oldConn := testChainsyncConnId(6410, 3001)
	newConn := testChainsyncConnId(6410, 3002)
	otherConn := testChainsyncConnId(6410, 3003)
	farTip := ochainsync.Tip{
		Point:       ocommon.Point{Slot: 1_000_000, Hash: []byte("far")},
		BlockNumber: 50_000,
	}
	ls := &LedgerState{
		chain:                testChain,
		headerPipelineConnId: oldConn,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			GetPeerSyncTargetFunc: func(
				ouroboros.ConnectionId,
			) (ochainsync.Tip, bool) {
				return farTip, true
			},
			BlockfetchRequestRangeFunc: func(
				ouroboros.ConnectionId,
				ocommon.Point,
				ocommon.Point,
			) (uint64, error) {
				t.Error("blockfetch must not start below the minimum batch")
				return 1, nil
			},
		},
	}
	ls.chainsyncBlockfetchMutex.Lock()
	_, err := ls.handoffPipelineOnSwitchLocked(newConn, nil)
	ls.chainsyncBlockfetchMutex.Unlock()
	require.NoError(t, err)

	forkHash := lcommon.NewBlake2b256([]byte("other-fork-hdr"))
	buffered := ls.shouldBufferHeaderEvent(ChainsyncEvent{
		ConnectionId: otherConn,
		BlockHeader: mockHeader{
			hash:        forkHash,
			prevHash:    lcommon.NewBlake2b256([]byte("other-fork-parent")),
			blockNumber: 3,
			slot:        3,
		},
		Point: ocommon.NewPoint(3, forkHash.Bytes()),
		Tip:   farTip,
	})
	assert.True(
		t,
		buffered,
		"a non-fitting header from another peer must be buffered, not processed against the queue",
	)
	assert.Equal(t, newConn, ls.headerPipelineConnId)
	assert.Equal(t, 2, ls.chain.HeaderCount())
}

// handleEventChainsyncBlockHeader preserves the direct helper used by focused
// tests. Production callers use handleEventChainsyncBlockHeaderWithPending so
// their outer pending-publish queue is threaded through the whole call chain.
func (ls *LedgerState) handleEventChainsyncBlockHeader(e ChainsyncEvent) error {
	var pending pendingPublishes
	defer pending.flush()
	return ls.handleEventChainsyncBlockHeaderWithPending(e, &pending)
}

func TestDesiredBlockfetchBatchHeaders(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name       string
		gapSlots   uint64
		gapBlocks  uint64
		maxHeaders int
		want       int
	}{
		{
			name:       "no gap",
			gapSlots:   0,
			gapBlocks:  0,
			maxHeaders: 16,
			want:       0,
		},
		{
			name:       "slot gap without block gap",
			gapSlots:   128,
			gapBlocks:  0,
			maxHeaders: 16,
			want:       1,
		},
		{
			name:       "small block gap",
			gapSlots:   128,
			gapBlocks:  3,
			maxHeaders: 16,
			want:       3,
		},
		{
			name:       "medium block gap",
			gapSlots:   128,
			gapBlocks:  8,
			maxHeaders: 16,
			want:       2,
		},
		{
			name:       "large block gap",
			gapSlots:   128,
			gapBlocks:  32,
			maxHeaders: 16,
			want:       8,
		},
		{
			name:       "very large block gap scales up",
			gapSlots:   2048,
			gapBlocks:  512,
			maxHeaders: 256,
			want:       128,
		},
		{
			name:       "deep catchup uses large batch",
			gapSlots:   8192,
			gapBlocks:  2048,
			maxHeaders: 500,
			want:       256,
		},
		{
			name:       "overflow-sized block gap stays bounded",
			gapSlots:   128,
			gapBlocks:  math.MaxUint64,
			maxHeaders: 16,
			want:       16,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			got := desiredBlockfetchBatchHeaders(
				testCase.gapSlots,
				testCase.gapBlocks,
				testCase.maxHeaders,
			)
			if got != testCase.want {
				t.Fatalf(
					"desiredBlockfetchBatchHeaders(%d, %d, %d) = %d, want %d",
					testCase.gapSlots,
					testCase.gapBlocks,
					testCase.maxHeaders,
					got,
					testCase.want,
				)
			}
		})
	}
}

// TestCalculateEpochNonce_ByronEra tests epoch nonce calculation in Byron era
func TestCalculateEpochNonce_ByronEra(t *testing.T) {
	t.Parallel()

	byronGenesisJSON := testByronGenesisJSONForK(432)
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`
	shelleyGenesisHash := "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d"

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: shelleyGenesisHash,
	}
	if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
		t.Fatalf("failed to load Byron genesis: %v", err)
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	ls := &LedgerState{
		currentEra: eras.ByronEraDesc,
		currentEpoch: models.Epoch{
			EpochId:   0,
			StartSlot: 0,
			Nonce:     nil,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Byron era should return nil nonce
	nonce, _, _, _, err := ls.calculateEpochNonce(
		nil,
		0,
		ls.currentEra,
		ls.currentEpoch,
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if nonce != nil {
		t.Errorf("expected nil nonce for Byron era, got %v", nonce)
	}
}

// TestCalculateEpochNonce_InitialEpochWithoutNonce tests initial Shelley epoch
func TestCalculateEpochNonce_InitialEpochWithoutNonce(t *testing.T) {
	t.Parallel()

	shelleyGenesisHash := "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d"
	byronGenesisJSON := testByronGenesisJSONForK(432)
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: shelleyGenesisHash,
	}
	if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
		t.Fatalf("failed to load Byron genesis: %v", err)
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:   0,
			StartSlot: 0,
			Nonce:     nil, // No nonce means initial epoch
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Initial epoch should return genesis hash
	nonce, _, _, _, err := ls.calculateEpochNonce(
		nil,
		0,
		ls.currentEra,
		ls.currentEpoch,
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedNonce, err := hex.DecodeString(shelleyGenesisHash)
	if err != nil {
		t.Fatalf("failed to decode expected nonce: %v", err)
	}

	if len(nonce) != len(expectedNonce) {
		t.Fatalf(
			"nonce length mismatch: expected %d, got %d",
			len(expectedNonce),
			len(nonce),
		)
	}
	for i := range nonce {
		if nonce[i] != expectedNonce[i] {
			t.Errorf(
				"nonce mismatch at byte %d: expected %x, got %x",
				i,
				expectedNonce[i],
				nonce[i],
			)
		}
	}
}

// TestCalculateEpochNonce_InvalidGenesisHash tests handling of invalid genesis hash
func TestCalculateEpochNonce_InvalidGenesisHash(t *testing.T) {
	t.Parallel()

	invalidHash := "not-a-valid-hex-string"
	byronGenesisJSON := testByronGenesisJSONForK(432)
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: invalidHash,
	}
	if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
		t.Fatalf("failed to load Byron genesis: %v", err)
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:   0,
			StartSlot: 0,
			Nonce:     nil,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	_, _, _, _, err := ls.calculateEpochNonce(
		nil,
		0,
		ls.currentEra,
		ls.currentEpoch,
		nil,
	)
	if err == nil {
		t.Fatal("expected error for invalid genesis hash, got nil")
	}
}

// TestCalculateEpochNonce_MissingShelleyGenesis tests handling of missing Shelley genesis
func TestCalculateEpochNonce_MissingShelleyGenesis(t *testing.T) {
	t.Parallel()

	cfg := &cardano.CardanoNodeConfig{}

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:   1,
			StartSlot: 86400,
			Nonce:     nil,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	_, _, _, _, err := ls.calculateEpochNonce(
		nil,
		86400,
		ls.currentEra,
		ls.currentEpoch,
		nil,
	)
	if err == nil {
		t.Fatal("expected error for missing Shelley genesis, got nil")
	}
	if !strings.Contains(err.Error(), "genesis hash") {
		t.Errorf("expected error about Shelley genesis, got: %v", err)
	}
}

// TestCalculateEpochNonce_ShelleyEraDifferentParams tests Shelley era with various parameters
func TestCalculateEpochNonce_ShelleyEraDifferentParams(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name             string
		k                int
		activeSlotsCoeff float64
		description      string
	}{
		{
			name:             "Standard testnet parameters",
			k:                432,
			activeSlotsCoeff: 0.05,
			description:      "k=432, f=0.05",
		},
		{
			name:             "Mainnet parameters",
			k:                2160,
			activeSlotsCoeff: 0.05,
			description:      "k=2160, f=0.05",
		},
		{
			name:             "High activity coefficient",
			k:                432,
			activeSlotsCoeff: 0.2,
			description:      "k=432, f=0.2",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			byronGenesisJSON := testByronGenesisJSONForK(uint64(tc.k))
			shelleyGenesisJSON := fmt.Sprintf(`{
				"activeSlotsCoeff": %f,
				"securityParam": %d,
				"systemStart": "2022-10-25T00:00:00Z"
			}`, tc.activeSlotsCoeff, tc.k)

			cfg := &cardano.CardanoNodeConfig{
				ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
			}
			if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
				t.Fatalf("failed to load Byron genesis: %v", err)
			}
			if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
				t.Fatalf("failed to load Shelley genesis: %v", err)
			}

			ls := &LedgerState{
				currentEra: eras.ShelleyEraDesc,
				currentEpoch: models.Epoch{
					EpochId:   0,
					StartSlot: 0,
					Nonce:     nil,
				},
				config: LedgerStateConfig{
					CardanoNodeConfig: cfg,
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
				},
			}

			// Initial epoch should return genesis hash
			nonce, _, _, _, err := ls.calculateEpochNonce(
				nil,
				0,
				ls.currentEra,
				ls.currentEpoch,
				nil,
			)
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.description, err)
			}
			if nonce == nil {
				t.Errorf(
					"%s: expected non-nil nonce for initial Shelley epoch",
					tc.description,
				)
			}
		})
	}
}

// TestCalculateEpochNonce_ZeroActiveSlots tests handling of zero active slots coefficient
func TestCalculateEpochNonce_ZeroActiveSlots(t *testing.T) {
	t.Parallel()

	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:   1,
			StartSlot: 86400,
			Nonce:     nil,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Verify fallback behavior when ActiveSlotsCoeff cannot be used
	window := ls.calculateStabilityWindow()
	if window != blockfetchBatchSlotThresholdDefault {
		t.Fatalf(
			"expected fallback window %d, got %d",
			blockfetchBatchSlotThresholdDefault,
			window,
		)
	}
}

// TestCalculateEpochNonce_StabilityWindowCalculation tests the stability window calculation logic
func TestCalculateEpochNonce_StabilityWindowCalculation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name             string
		era              eras.EraDesc
		k                int
		activeSlotsCoeff float64
		expectedFormula  string
	}{
		{
			name:             "Byron era uses k directly",
			era:              eras.ByronEraDesc,
			k:                432,
			activeSlotsCoeff: 0.05,
			expectedFormula:  "stability_window = k = 432",
		},
		{
			name:             "Shelley era uses 3k/f",
			era:              eras.ShelleyEraDesc,
			k:                432,
			activeSlotsCoeff: 0.05,
			expectedFormula:  "stability_window = 3*432/0.05 = 25920",
		},
		{
			name:             "Allegra era uses 3k/f",
			era:              eras.AllegraEraDesc,
			k:                2160,
			activeSlotsCoeff: 0.05,
			expectedFormula:  "stability_window = 3*2160/0.05 = 129600",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			byronGenesisJSON := testByronGenesisJSONForK(uint64(tc.k))
			shelleyGenesisJSON := fmt.Sprintf(`{
				"activeSlotsCoeff": %f,
				"securityParam": %d,
				"systemStart": "2022-10-25T00:00:00Z"
			}`, tc.activeSlotsCoeff, tc.k)

			cfg := &cardano.CardanoNodeConfig{
				ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
			}
			if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
				t.Fatalf("failed to load Byron genesis: %v", err)
			}
			if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
				t.Fatalf("failed to load Shelley genesis: %v", err)
			}

			ls := &LedgerState{
				currentEra: tc.era,
				currentEpoch: models.Epoch{
					EpochId:   0,
					StartSlot: 0,
					Nonce:     nil,
				},
				config: LedgerStateConfig{
					CardanoNodeConfig: cfg,
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
				},
			}

			// Test for Byron era - should return nil
			if tc.era.Id == 0 {
				nonce, _, _, _, err := ls.calculateEpochNonce(
					nil,
					0,
					ls.currentEra,
					ls.currentEpoch,
					nil,
				)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if nonce != nil {
					t.Errorf(
						"Byron era should return nil nonce, got: %v",
						nonce,
					)
				}
				return
			}

			// For non-Byron eras, test initial epoch returns genesis hash
			nonce, _, _, _, err := ls.calculateEpochNonce(
				nil,
				0,
				ls.currentEra,
				ls.currentEpoch,
				nil,
			)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if nonce == nil {
				t.Error("expected non-nil nonce for initial non-Byron epoch")
			}
			t.Logf("Formula: %s", tc.expectedFormula)
		})
	}
}

// TestCalculateEpochNonce_IntegerArithmeticPrecision tests precision of integer arithmetic
func TestCalculateEpochNonce_IntegerArithmeticPrecision(t *testing.T) {
	t.Parallel()

	byronGenesisJSON := testByronGenesisJSONForK(1000)
	// Use a coefficient that produces fractional results
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.333333,
		"securityParam": 1000,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
	}
	if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
		t.Fatalf("failed to load Byron genesis: %v", err)
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		currentEpoch: models.Epoch{
			EpochId:   0,
			StartSlot: 0,
			Nonce:     nil,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Should handle fractional coefficients correctly using integer arithmetic
	nonce, _, _, _, err := ls.calculateEpochNonce(
		nil,
		0,
		ls.currentEra,
		ls.currentEpoch,
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error with fractional coefficient: %v", err)
	}
	if nonce == nil {
		t.Error("expected non-nil nonce")
	}
}

// TestHandleEventChainsyncBlockHeader_StabilityWindowUsage tests the stability window usage in block header handling
func TestHandleEventChainsyncBlockHeader_StabilityWindowUsage(t *testing.T) {
	t.Parallel()

	// This test verifies that the handleEventChainsyncBlockHeader function
	// correctly uses calculateStabilityWindow instead of the old constant

	byronGenesisJSON := testByronGenesisJSONForK(432)
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{}
	if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
		t.Fatalf("failed to load Byron genesis: %v", err)
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	ls := &LedgerState{
		currentEra: eras.ShelleyEraDesc,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Verify that calculateStabilityWindow returns correct value
	window := ls.calculateStabilityWindow()
	expectedWindow := uint64(25920) // 3*432/0.05
	if window != expectedWindow {
		t.Errorf("expected stability window %d, got %d", expectedWindow, window)
	}

	// Verify it's different from the old constant
	if window == blockfetchBatchSlotThresholdDefault {
		t.Error(
			"stability window should not equal the old constant for Shelley era",
		)
	}
}

// TestCalculateEpochNonce_AllEras tests epoch nonce calculation across all eras
func TestCalculateEpochNonce_AllEras(t *testing.T) {
	t.Parallel()

	byronGenesisJSON := testByronGenesisJSONForK(432)
	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
	}
	if err := loadByronGenesisForTest(t, cfg, strings.NewReader(byronGenesisJSON)); err != nil {
		t.Fatalf("failed to load Byron genesis: %v", err)
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	testCases := []struct {
		name        string
		era         eras.EraDesc
		expectNil   bool
		description string
	}{
		{
			name:        "Byron era returns nil",
			era:         eras.ByronEraDesc,
			expectNil:   true,
			description: "Byron has no epoch nonce",
		},
		{
			name:        "Shelley era returns nonce",
			era:         eras.ShelleyEraDesc,
			expectNil:   false,
			description: "Shelley uses epoch nonce",
		},
		{
			name:        "Allegra era returns nonce",
			era:         eras.AllegraEraDesc,
			expectNil:   false,
			description: "Allegra uses epoch nonce",
		},
		{
			name:        "Mary era returns nonce",
			era:         eras.MaryEraDesc,
			expectNil:   false,
			description: "Mary uses epoch nonce",
		},
		{
			name:        "Alonzo era returns nonce",
			era:         eras.AlonzoEraDesc,
			expectNil:   false,
			description: "Alonzo uses epoch nonce",
		},
		{
			name:        "Babbage era returns nonce",
			era:         eras.BabbageEraDesc,
			expectNil:   false,
			description: "Babbage uses epoch nonce",
		},
		{
			name:        "Conway era returns nonce",
			era:         eras.ConwayEraDesc,
			expectNil:   false,
			description: "Conway uses epoch nonce",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ls := &LedgerState{
				currentEra: tc.era,
				currentEpoch: models.Epoch{
					EpochId:   0,
					StartSlot: 0,
					Nonce:     nil,
				},
				config: LedgerStateConfig{
					CardanoNodeConfig: cfg,
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
				},
			}

			nonce, _, _, _, err := ls.calculateEpochNonce(
				nil,
				0,
				ls.currentEra,
				ls.currentEpoch,
				nil,
			)
			if err != nil {
				t.Fatalf("%s: unexpected error: %v", tc.description, err)
			}

			if tc.expectNil {
				if nonce != nil {
					t.Errorf(
						"%s: expected nil nonce, got %v",
						tc.description,
						nonce,
					)
				}
			} else {
				if nonce == nil {
					t.Errorf("%s: expected non-nil nonce", tc.description)
				}
			}
		})
	}
}

// TestCalculateEpochNonce_MissingByronGenesisInByronEra tests missing Byron genesis during Byron era
func TestCalculateEpochNonce_MissingByronGenesisInByronEra(t *testing.T) {
	t.Parallel()

	shelleyGenesisJSON := `{
		"activeSlotsCoeff": 0.05,
		"securityParam": 432,
		"systemStart": "2022-10-25T00:00:00Z"
	}`

	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: "363498d1024f84bb39d3fa9593ce391483cb40d479b87233f868d6e57c3a400d",
	}
	if err := cfg.LoadShelleyGenesisFromReader(strings.NewReader(shelleyGenesisJSON)); err != nil {
		t.Fatalf("failed to load Shelley genesis: %v", err)
	}

	ls := &LedgerState{
		currentEra: eras.ByronEraDesc,
		currentEpoch: models.Epoch{
			EpochId:   1,
			StartSlot: 86400,
			Nonce:     nil,
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Byron era returns nil nonce immediately without genesis validation
	nonce, _, _, _, err := ls.calculateEpochNonce(
		nil,
		86400,
		ls.currentEra,
		ls.currentEpoch,
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error for Byron era: %v", err)
	}
	if nonce != nil {
		t.Errorf("expected nil nonce for Byron era, got: %v", nonce)
	}
}

// mockForgedBlockChecker is a test implementation of ForgedBlockChecker.
type mockForgedBlockChecker struct {
	forgedSlots map[uint64][]byte
}

func TestCheckSlotBattle_DetectsConflict(t *testing.T) {
	t.Parallel()

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	localHash := []byte{0x01, 0x02, 0x03, 0x04}
	remoteHash := []byte{0x0A, 0x0B, 0x0C, 0x0D}

	checker := &mockForgedBlockChecker{
		forgedSlots: map[uint64][]byte{
			1000: localHash,
		},
	}

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus:           eventBus,
			ForgedBlockChecker: checker,
			Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Subscribe to slot battle events
	_, evtCh := eventBus.Subscribe(forging.SlotBattleEventType)

	// Simulate an incoming block at slot 1000 with a different hash.
	// Pass a non-nil error to indicate the remote block was rejected
	// (local block stays on chain, so local won).
	e := BlockfetchEvent{
		Point: ocommon.Point{
			Slot: 1000,
			Hash: remoteHash,
		},
	}
	ls.checkSlotBattle(e, errors.New("block rejected"))

	// Verify the event was emitted
	evt := testutil.RequireReceive(
		t,
		evtCh,
		testutil.AsyncWait,
		"timeout waiting for SlotBattleEvent",
	)
	battle, ok := evt.Data.(forging.SlotBattleEvent)
	require.True(t, ok, "event data should be SlotBattleEvent")
	assert.Equal(t, uint64(1000), battle.Slot)
	assert.Equal(t, localHash, battle.LocalBlockHash)
	assert.Equal(t, remoteHash, battle.RemoteBlockHash)
	assert.True(t, battle.Won,
		"local should win when remote block is rejected")
}

func TestCheckSlotBattle_RemoteWinsWhenAccepted(t *testing.T) {
	t.Parallel()

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	localHash := []byte{0x01, 0x02, 0x03, 0x04}
	remoteHash := []byte{0x0A, 0x0B, 0x0C, 0x0D}

	checker := &mockForgedBlockChecker{
		forgedSlots: map[uint64][]byte{
			1000: localHash,
		},
	}

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus:           eventBus,
			ForgedBlockChecker: checker,
			Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	_, evtCh := eventBus.Subscribe(forging.SlotBattleEventType)

	// Pass nil error to indicate the remote block was accepted
	// (remote won, our block is replaced).
	e := BlockfetchEvent{
		Point: ocommon.Point{
			Slot: 1000,
			Hash: remoteHash,
		},
	}
	ls.checkSlotBattle(e, nil)

	evt := testutil.RequireReceive(
		t,
		evtCh,
		testutil.AsyncWait,
		"timeout waiting for SlotBattleEvent",
	)
	battle, ok := evt.Data.(forging.SlotBattleEvent)
	require.True(t, ok)
	assert.Equal(t, uint64(1000), battle.Slot)
	assert.False(t, battle.Won,
		"local should lose when remote block is accepted")
}

func TestCheckSlotBattle_NoConflictDifferentSlot(t *testing.T) {
	t.Parallel()

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	checker := &mockForgedBlockChecker{
		forgedSlots: map[uint64][]byte{
			1000: {0x01, 0x02, 0x03, 0x04},
		},
	}

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus:           eventBus,
			ForgedBlockChecker: checker,
			Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	_, evtCh := eventBus.Subscribe(forging.SlotBattleEventType)

	// Incoming block is at slot 2000, not a slot we forged
	e := BlockfetchEvent{
		Point: ocommon.Point{
			Slot: 2000,
			Hash: []byte{0x0A, 0x0B, 0x0C, 0x0D},
		},
	}
	ls.checkSlotBattle(e, nil)

	// Verify no event was emitted
	testutil.RequireNoReceive(
		t,
		evtCh,
		50*time.Millisecond,
		"unexpected SlotBattleEvent",
	)
}

func TestCheckSlotBattle_SameHashIsNotBattle(t *testing.T) {
	t.Parallel()

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	blockHash := []byte{0x01, 0x02, 0x03, 0x04}

	checker := &mockForgedBlockChecker{
		forgedSlots: map[uint64][]byte{
			1000: blockHash,
		},
	}

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus:           eventBus,
			ForgedBlockChecker: checker,
			Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	_, evtCh := eventBus.Subscribe(forging.SlotBattleEventType)

	// Incoming block has the same hash (it's our own block echoed back)
	e := BlockfetchEvent{
		Point: ocommon.Point{
			Slot: 1000,
			Hash: blockHash,
		},
	}
	ls.checkSlotBattle(e, nil)

	testutil.RequireNoReceive(
		t,
		evtCh,
		50*time.Millisecond,
		"same-hash block should not trigger slot battle",
	)
}

func TestCheckSlotBattle_NilCheckerSkips(t *testing.T) {
	t.Parallel()

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus:           eventBus,
			ForgedBlockChecker: nil, // No checker configured
			Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	_, evtCh := eventBus.Subscribe(forging.SlotBattleEventType)

	e := BlockfetchEvent{
		Point: ocommon.Point{
			Slot: 1000,
			Hash: []byte{0x0A, 0x0B, 0x0C, 0x0D},
		},
	}
	ls.checkSlotBattle(e, nil)

	testutil.RequireNoReceive(
		t,
		evtCh,
		50*time.Millisecond,
		"nil checker should not emit events",
	)
}

func TestCheckSlotBattle_NilEventBus(t *testing.T) {
	t.Parallel()

	localHash := []byte{0x01, 0x02, 0x03, 0x04}
	remoteHash := []byte{0x0A, 0x0B, 0x0C, 0x0D}

	checker := &mockForgedBlockChecker{
		forgedSlots: map[uint64][]byte{
			1000: localHash,
		},
	}

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus:           nil, // No event bus
			ForgedBlockChecker: checker,
			Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Should not panic with nil event bus
	e := BlockfetchEvent{
		Point: ocommon.Point{
			Slot: 1000,
			Hash: remoteHash,
		},
	}
	ls.checkSlotBattle(e, errors.New("rejected"))
}

// TestCheckSlotBattle_UnderWriteLock is a regression test for the
// deadlock where checkSlotBattle was called while holding ls.Lock()
// (write lock) but internally attempted ls.RLock() on the same
// non-reentrant sync.RWMutex.
func TestCheckSlotBattle_UnderWriteLock(t *testing.T) {
	t.Parallel()

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	localHash := []byte{0x01, 0x02, 0x03, 0x04}
	remoteHash := []byte{0x0A, 0x0B, 0x0C, 0x0D}

	checker := &mockForgedBlockChecker{
		forgedSlots: map[uint64][]byte{
			1000: localHash,
		},
	}

	ls := &LedgerState{
		config: LedgerStateConfig{
			EventBus:           eventBus,
			ForgedBlockChecker: checker,
			Logger:             slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	_, evtCh := eventBus.Subscribe(forging.SlotBattleEventType)

	e := BlockfetchEvent{
		Point: ocommon.Point{
			Slot: 1000,
			Hash: remoteHash,
		},
	}

	// Call checkSlotBattle while holding the write lock, exactly
	// as processBlockEvents does. Before the fix this deadlocked.
	done := make(chan struct{})
	go func() {
		defer close(done)
		ls.Lock()
		ls.checkSlotBattle(e, errors.New("block rejected"))
		ls.Unlock()
	}()

	testutil.RequireReceive(
		t,
		done,
		testutil.AsyncWait,
		"checkSlotBattle deadlocked under write lock",
	)

	evt := testutil.RequireReceive(
		t,
		evtCh,
		testutil.AsyncWait,
		"timeout waiting for SlotBattleEvent",
	)
	battle, ok := evt.Data.(forging.SlotBattleEvent)
	require.True(t, ok)
	assert.Equal(t, uint64(1000), battle.Slot)
	assert.True(t, battle.Won)
}

// newChainUpdateEvent builds a throwaway chain.update event for saturating the
// bus in the deadlock regression below.
func newChainUpdateEvent() event.Event {
	return event.NewEvent(chain.ChainUpdateEventType, chain.ChainBlockEvent{})
}

// TestBlockfetchDrainDefersChainUpdatePastLedgerMutex is the regression guard
// for the chainsync/blockfetch drain deadlock (blinklabs-io/dingo preview
// freeze), rewritten to exercise the lane-saturation path the six-block test
// could not reach.
//
// The ledger drains fetched blocks via flushPendingBlockfetchBlocks while
// holding chainsyncBlockfetchMutex. If that drain publishes chain.update
// inline, a terminal chain.update subscriber that stops draining stalls the
// publish WITH the mutex held; handleEventChainsync then blocks acquiring the
// same mutex, the ledger.chainsync buffer fills, and the node deadlocks.
//
// This test puts the bus into the exact state that traps BOTH previously
// attempted inline publishers:
//
//   - a lossless chain.update subscriber whose one buffer slot is filled and
//     never drained, so a synchronous Publish (the original code) blocks; and
//   - the ordered chain.update lane filled to capacity, so a PublishOrdered
//     (the rejected under-lock fix) also blocks -- this is the saturation the
//     maintainer flagged, which a handful of blocks never reaches.
//
// With the bus in that state the drain must still return promptly, because the
// fix hands each block's chain.update back to the ledger's pendingPublishes to
// publish AFTER the mutex is released rather than publishing it inline. Both
// older approaches would block here and fail the timeout.
func TestBlockfetchDrainDefersChainUpdatePastLedgerMutex(t *testing.T) {
	t.Parallel()

	eventBus := event.NewEventBus(nil, nil)
	// Stop releases the goroutines parked on the stalled subscriber / full
	// lane at the end of the test. Run it under a bounded wait: if Stop's
	// shutdown path ever regresses and fails to release those parked
	// publishers, an unbounded t.Cleanup(eventBus.Stop) would hang the whole
	// `go test` binary in Stop with no diagnostic. Fail the test instead so
	// the regression is visible.
	t.Cleanup(func() {
		stopped := make(chan struct{})
		go func() {
			eventBus.Stop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(testutil.AsyncWait):
			t.Error(
				"eventBus.Stop did not return: it failed to " +
					"release the publishers parked on the stalled subscriber / " +
					"full ordered lane (shutdown backpressure-release regressed)",
			)
		}
	})

	// Terminal chain.update subscriber, buffer 1, deliberately never drained.
	// Lossless (SubscriberBackpressureBlock) means a full buffer blocks the
	// publisher forever rather than dropping -- the stalled-subscriber
	// condition behind the freeze.
	subId, ch := eventBus.SubscribeWithBufferPolicy(
		chain.ChainUpdateEventType,
		1,
		event.SubscriberBackpressureBlock,
	)
	require.NotZero(t, subId)
	require.NotNil(t, ch)

	// Fill the subscriber's single buffer slot so any further synchronous
	// Publish blocks. Confirm the stall is real: a second inline Publish must
	// not complete.
	eventBus.Publish(chain.ChainUpdateEventType, newChainUpdateEvent())
	directBlocked := make(chan struct{})
	go func() {
		eventBus.Publish(chain.ChainUpdateEventType, newChainUpdateEvent())
		close(directBlocked)
	}()
	select {
	case <-directBlocked:
		t.Fatal(
			"inline Publish did not block on the stalled subscriber; " +
				"the test cannot exercise the deadlock condition",
		)
	case <-time.After(500 * time.Millisecond):
	}

	// Saturate the ordered chain.update lane to capacity. The lane worker
	// parks on the stalled subscriber, so every enqueued event stays in the
	// lane; once it is full a PublishOrdered blocks too.
	go func() {
		for range event.OrderedQueueSize + 8 {
			eventBus.PublishOrdered(
				chain.ChainUpdateEventType,
				newChainUpdateEvent(),
			)
		}
	}()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			50*time.Millisecond,
		)
		defer cancel()
		// A bounded publish that cannot enqueue reports false: the lane is
		// full.
		return !eventBus.PublishOrderedContext(
			ctx,
			chain.ChainUpdateEventType,
			newChainUpdateEvent(),
		)
	}, testutil.AsyncWait, 20*time.Millisecond,
		"ordered chain.update lane never reached capacity",
	)

	// Real primary chain wired to the saturated bus, plus a minimal ledger
	// state -- enough for the blockfetch drain path.
	cm, err := chain.NewManager(nil, eventBus)
	require.NoError(t, err)
	c := cm.PrimaryChain()
	require.NotNil(t, c)

	ls := &LedgerState{
		chain: c,
		config: LedgerStateConfig{
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EventBus: eventBus,
		},
	}

	blocks, err := testfixtures.GenerateConwayChain(1)
	require.NoError(t, err)
	require.Len(t, blocks, 1)
	ls.pendingBlockfetchEvents = []BlockfetchEvent{
		{
			Block: blocks[0],
			Point: ocommon.Point{
				Slot: blocks[0].SlotNumber(),
				Hash: blocks[0].Hash().Bytes(),
			},
		},
	}

	// THE FIX. flushPendingBlockfetchBlocksDeferred runs on the mutex-holding
	// drain path. It must add the block and return promptly, queueing the
	// chain.update on pubs instead of publishing it into the saturated bus.
	// Under the original synchronous Publish, or the rejected under-lock
	// PublishOrdered, this call would block forever on the stalled subscriber
	// / full lane and the timeout below would fire.
	var pubs pendingPublishes
	drained := make(chan error, 1)
	go func() { drained <- ls.flushPendingBlockfetchBlocksDeferred(&pubs) }()
	select {
	case drainErr := <-drained:
		require.NoError(t, drainErr)
	case <-time.After(testutil.AsyncWait):
		t.Fatal(
			"flushPendingBlockfetchBlocks blocked under a saturated " +
				"chain.update lane: the block's chain.update must be deferred " +
				"past chainsyncBlockfetchMutex, not published inline",
		)
	}

	// The chain.update was deferred, not published: nothing reached the
	// saturated bus under the lock. It is no longer requeued onto pubs.events;
	// AddBlockWithPointDeferred enqueued it on the chain's shared sequencer
	// under c.mutex, and the drain registered the chain on pubs.chainDrains so
	// pubs.flush() publishes it (in chain-mutation order) after the mutex is
	// released. That the chain is registered but not yet drained here is the
	// deadlock-avoidance property: publication is deferred past the lock.
	require.Empty(
		t,
		pubs.events,
		"chain.update must not be requeued on the generic pending queue; it "+
			"lives on the chain's shared sequencer",
	)
	require.Equal(
		t,
		[]*chain.Chain{c},
		pubs.chainDrains,
		"the drain must register the chain so its sequencer is flushed after "+
			"the mutex is released",
	)
	// The block really was added to the chain (the drain did its job, it just
	// did not publish).
	require.Equal(t, blocks[0].SlotNumber(), c.Tip().Point.Slot)
}

// loadByronGenesisForTest fills fields that make a fixture valid as Byron
// genesis while preserving fields the test is exercising.
func loadByronGenesisForTest(
	t testing.TB,
	cfg *cardano.CardanoNodeConfig,
	r io.Reader,
) error {
	t.Helper()

	raw, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	var genesis map[string]json.RawMessage
	if err := json.Unmarshal(raw, &genesis); err != nil {
		return err
	}
	protocolMagic := uint32(164)
	if protocolConsts, ok := genesis["protocolConsts"]; ok {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(protocolConsts, &fields); err != nil {
			return err
		}
		if magic, ok := fields["protocolMagic"]; ok {
			if err := json.Unmarshal(magic, &protocolMagic); err != nil {
				return err
			}
		}
	}
	issuer := newByronPBFTTestKey(0x31)
	delegate := newByronPBFTTestKey(0x32)
	issuerHash, err := byronconsensus.PBFTVerificationKeyHash(
		issuer.verificationKey,
	)
	if err != nil {
		return err
	}
	certificate := newSignedByronPBFTDelegationCertificate(
		t, protocolMagic, 0, issuer, delegate,
	)
	bootStakeholders, err := json.Marshal(map[string]uint64{
		issuerHash.String(): 1,
	})
	if err != nil {
		return err
	}
	heavyDelegation, err := json.Marshal(map[string]any{
		issuerHash.String(): map[string]any{
			"cert": hex.EncodeToString(certificate[3].([]byte)),
			"delegatePk": base64.StdEncoding.EncodeToString(
				delegate.verificationKey,
			),
			"issuerPk": base64.StdEncoding.EncodeToString(
				issuer.verificationKey,
			),
			"omega": 0,
		},
	})
	if err != nil {
		return err
	}
	defaults := map[string]json.RawMessage{
		"avvmDistr":        json.RawMessage(`{}`),
		"bootStakeholders": bootStakeholders,
		"heavyDelegation":  heavyDelegation,
		"nonAvvmBalances":  json.RawMessage(`{}`),
		"startTime":        json.RawMessage(`1788739200`),
		"blockVersionData": json.RawMessage(`{
			"heavyDelThd":"300000000000","maxBlockSize":"2000000",
			"maxHeaderSize":"2000000","maxProposalSize":"700",
			"maxTxSize":"4096","mpcThd":"20000000000000",
			"scriptVersion":0,"slotDuration":"20000",
			"softforkRule":{"initThd":"900000000000000","minThd":"600000000000000","thdDecrement":"50000000000000"},
			"txFeePolicy":{"multiplier":"43946000000","summand":"155381000000000"},
			"unlockStakeEpoch":"18446744073709551615","updateImplicit":"10000",
			"updateProposalThd":"100000000000000","updateVoteThd":"1000000000000"
		}`),
		"protocolConsts": json.RawMessage(`{"k":108,"protocolMagic":164}`),
	}
	for key, value := range defaults {
		if _, ok := genesis[key]; !ok {
			genesis[key] = value
		}
	}
	for key, nestedDefaults := range map[string]map[string]json.RawMessage{
		"blockVersionData": {
			"heavyDelThd": json.RawMessage(`"300000000000"`), "maxBlockSize": json.RawMessage(`"2000000"`),
			"maxHeaderSize": json.RawMessage(`"2000000"`), "maxProposalSize": json.RawMessage(`"700"`),
			"maxTxSize": json.RawMessage(`"4096"`), "mpcThd": json.RawMessage(`"20000000000000"`),
			"scriptVersion": json.RawMessage(`0`), "slotDuration": json.RawMessage(`"20000"`),
			"softforkRule":     json.RawMessage(`{"initThd":"900000000000000","minThd":"600000000000000","thdDecrement":"50000000000000"}`),
			"txFeePolicy":      json.RawMessage(`{"multiplier":"43946000000","summand":"155381000000000"}`),
			"unlockStakeEpoch": json.RawMessage(`"18446744073709551615"`), "updateImplicit": json.RawMessage(`"10000"`),
			"updateProposalThd": json.RawMessage(`"100000000000000"`), "updateVoteThd": json.RawMessage(`"1000000000000"`),
		},
		"protocolConsts": {"k": json.RawMessage(`108`), "protocolMagic": json.RawMessage(`164`)},
	} {
		var nested map[string]json.RawMessage
		if err := json.Unmarshal(genesis[key], &nested); err != nil {
			return err
		}
		if nested == nil {
			continue
		}
		for nestedKey, value := range nestedDefaults {
			if _, ok := nested[nestedKey]; !ok {
				nested[nestedKey] = value
			}
		}
		encoded, err := json.Marshal(nested)
		if err != nil {
			return err
		}
		genesis[key] = encoded
	}
	completed, err := json.Marshal(genesis)
	if err != nil {
		return err
	}
	return cfg.LoadByronGenesisFromReader(bytes.NewReader(completed))
}

// TestCalculateEpochNonce_TPraosToPraosUsesSourceEpochStabilityWindow proves
// that the candidate-freeze cutoff at the Alonzo→Babbage epoch boundary is
// driven by the source epoch's protocol family (TPraos, 3k/f) — not the
// post-transition era (Babbage, Praos, 4k/f).
//
// State up to entering this rollover:
//
//   - currentEpoch = Alonzo (epoch 3, EraId=4, slots 225–299)
//   - currentEra   = Babbage (5)              ← already advanced by
//     applyHardForkTransition
//   - k=6, f=0.4 → TPraos stability window = 3k/f = 45 slots,
//     Praos stability window  = 4k/f = 60 slots
//   - Alonzo cutoff (TPraos): 225 + 75 - 45 = 255
//   - Alonzo cutoff (Praos):  225 + 75 - 60 = 240
//
// The test seeds two pre-stored block-nonce rows at slots 230 and 245:
//
//   - slot 230 is below both cutoffs (always contributes to candidate)
//   - slot 245 sits between the Praos cutoff (240) and the TPraos cutoff
//     (255). It should contribute to candidate iff the cutoff is taken
//     from the SOURCE epoch's era (Alonzo, TPraos)
//
// The fast path freezes candidate at the latest pre-cutoff block's stored
// nonce. So:
//
//   - Correct (source-era cutoff = 255): candidate = nonce(slot 245) =
//     0xab*32. The block at slot 245 is the latest block strictly before
//     255.
//   - Buggy   (post-transition cutoff = 240): candidate = nonce(slot 230)
//     = 0x99*32. The block at slot 245 is past the cutoff and does not
//     contribute, so the latest pre-cutoff block is slot 230.
//
// The test asserts the correct value. Today, calculateEpochNonce passes
// `currentEra.Id` (Babbage) into computeCandidateNonce — i.e. the
// post-transition era — which selects the Praos window and produces the
// buggy value. The fix is to pass `currentEpoch.EraId` (the source
// epoch's era) instead, matching what verify_header.go already does and
// what the function's own comment claims it does.
//
// This is the smaller of the two distinct VRF wedges: the bug
// only fires at TPraos→Praos boundaries (Alonzo→Babbage) because that's
// the only transition where the two stability-window formulas disagree.
// All other era boundaries within TPraos (Shelley→Allegra, Allegra→Mary,
// Mary→Alonzo) and within Praos (Babbage→Conway) use the same multiplier
// either way and therefore mask the bug.
func TestCalculateEpochNonce_TPraosToPraosUsesSourceEpochStabilityWindow(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cfg := newAlonzoToBabbageStabilityCfg(t)

	// Deterministic distinct nonces for slots 230 and 245 so the buggy
	// vs correct candidate values are unambiguous.
	nonceAt230 := bytes.Repeat([]byte{0x99}, 32)
	nonceAt245 := bytes.Repeat([]byte{0xab}, 32)
	hashAt230 := bytes.Repeat([]byte{0x01}, 32)
	hashAt245 := bytes.Repeat([]byte{0x02}, 32)
	prevHashAt230 := bytes.Repeat([]byte{0x10}, 32)
	prevHashAt245 := bytes.Repeat([]byte{0x20}, 32)

	// Insert two Alonzo blocks (slot 230 before any cutoff, slot 245
	// between the Praos and TPraos cutoffs) into the blob store, plus
	// pre-stored block-nonce rows so the fast path can compute the
	// candidate without re-decoding CBOR.
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot: 230, Hash: hashAt230, PrevHash: prevHashAt230,
			Cbor:   []byte{0x80}, // empty CBOR array, never decoded by fast path
			Number: 1, Type: 4,   // Alonzo block type
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot: 245, Hash: hashAt245, PrevHash: prevHashAt245,
			Cbor:   []byte{0x80},
			Number: 2, Type: 4,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAt230, 230, nonceAt230, false, txn,
		); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAt245, 245, nonceAt245, false, txn,
		)
	}))

	// currentTipBlockNonce is intentionally left empty so the
	// resume-from-tip optimisation in computeEpochNonceForSlot
	// /calculateEpochNonce does not short-circuit and the candidate
	// is recomputed across the full epoch range from prevEvolvingNonce.
	ls := &LedgerState{
		db:         db,
		currentEra: eras.BabbageEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             3,
			StartSlot:           225,
			LengthInSlots:       75,
			SlotLength:          1000,
			EraId:               eras.AlonzoEraDesc.Id,
			Nonce:               bytes.Repeat([]byte{0xee}, 32),
			EvolvingNonce:       bytes.Repeat([]byte{0xee}, 32),
			CandidateNonce:      bytes.Repeat([]byte{0xcc}, 32),
			LastEpochBlockNonce: bytes.Repeat([]byte{0xfa}, 32),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	// Drive the rollover that fires at the Alonzo→Babbage boundary.
	// processEpochRollover is called with currentEra already advanced
	// to Babbage (state.go:2457 passes eras.Eras[workingEraId] after
	// applyHardForkTransition). What we want to assert is that the
	// inner candidate-nonce computation still picks the cutoff slot
	// from the era of the epoch being CLOSED, not the era being
	// entered.
	var candidate []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		_, _, c, _, err := ls.calculateEpochNonce(
			txn,
			ls.currentEpoch.StartSlot+uint64(ls.currentEpoch.LengthInSlots),
			eras.BabbageEraDesc,
			ls.currentEpoch,
			nil,
		)
		candidate = c
		return err
	}))

	require.Equalf(
		t,
		hex.EncodeToString(nonceAt245),
		hex.EncodeToString(candidate),
		"candidate must freeze at the source epoch's TPraos cutoff "+
			"(slot 255), so the block at slot 245 is the latest "+
			"pre-cutoff contributor and its stored nonce becomes the "+
			"candidate. Got %x — that's the nonce at slot 230, which "+
			"means the freeze cutoff was computed against Babbage's "+
			"Praos window (240) instead of Alonzo's TPraos window "+
			"(255). #2125 Alonzo→Babbage VRF wedge.",
		candidate,
	)
}

// newAlonzoToBabbageStabilityCfg builds a CardanoNodeConfig with concrete
// k and f values that put the Praos cutoff (4k/f = 60) and the TPraos
// cutoff (3k/f = 45) on opposite sides of slot 245 inside an Alonzo epoch
// of length 75 starting at slot 225. The Shelley genesis hash is set so
// computeCandidateNonce's fall-back paths have something to decode.
func newAlonzoToBabbageStabilityCfg(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, loadByronGenesisForTest(t, cfg, strings.NewReader(`{
		"protocolConsts": {
			"k": 6,
			"protocolMagic": 42
		}
	}`)))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"systemStart": "2026-01-01T00:00:00Z",
		"securityParam": 6,
		"activeSlotsCoeff": 0.4,
		"epochLength": 75,
		"slotLength": 1
	}`)))
	return cfg
}

// TestCalculateEpochNonce_PostMithrilBootstrapFreezesCandidateAtCutoff
// reproduces the persisted-state shape that reports after a
// Mithril bootstrap inside a Conway epoch:
//
//   - The bootstrap epoch row carries CandidateNonce == EvolvingNonce
//     because the snapshot was taken before the candidate-freeze cutoff
//     (psCandidateNonce in cardano-ledger tracks evolving until the
//     stability window closes).
//   - importTip wrote a block_nonce checkpoint at the snapshot tip slot
//     so the resume logic can find the seam between the imported tip
//     and post-import per-block accumulation.
//   - Post-import sync produced block_nonce rows for blocks past the
//     snapshot tip, including blocks straddling the freeze cutoff.
//
// The Conway→Conway rollover that closes the bootstrap epoch must:
//
//   - return candidateNonce frozen at the latest pre-cutoff block's
//     stored nonce (NOT the imported tip-time value), and
//   - return evolvingNonce equal to the last-block-of-epoch's stored
//     nonce.
//
// If the rollover instead returns the imported tip-time value as the
// candidate (i.e. it inherited prevEpoch.CandidateNonce without ever
// iterating past the cutoff), the next epoch's nonce diverges from peers
// and every header in that epoch fails VRF verification — the freeze
// described by this test.
func TestCalculateEpochNonce_PostMithrilBootstrapFreezesCandidateAtCutoff(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cfg := newConwayBootstrapStabilityCfg(t)

	// k=6, f=0.4 → 4k/f = 60 slots. Epoch length 75, start slot 1000,
	// end slot 1075. cutoffSlot = 1075 - 60 = 1015.
	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		cutoffSlot  uint64 = 1015
		snapTipSlot uint64 = 1010 // snapshot taken before cutoff
		preCutSlot  uint64 = 1014 // last block strictly before cutoff
		postCutSlot uint64 = 1070 // last block of epoch (post-cutoff)
	)

	// Imported snapshot tip-time evolving == candidate (psCandidate
	// tracks evolving until the stability window closes).
	importedNonce := bytes.Repeat([]byte{0xaa}, 32)
	// Per-block evolving nonces stored by post-import processing.
	// Distinct, deterministic values so a wrong return is unambiguous.
	nonceAtPreCut := bytes.Repeat([]byte{0xbb}, 32)
	nonceAtPostCut := bytes.Repeat([]byte{0xcc}, 32)

	hashAtSnap := bytes.Repeat([]byte{0x10}, 32)
	hashAtPreCut := bytes.Repeat([]byte{0x14}, 32)
	hashAtPostCut := bytes.Repeat([]byte{0x70}, 32)
	prevHashAtSnap := bytes.Repeat([]byte{0x09}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		// Mithril-imported tip block (no per-block VRF processing
		// for it; importTip writes a single block_nonce checkpoint).
		if err := db.BlockCreate(models.Block{
			Slot:     snapTipSlot,
			Hash:     hashAtSnap,
			PrevHash: prevHashAtSnap,
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		// Post-import blocks. CBOR is a stub byte; the fast path
		// computeCandidateNonceFast does not decode block bodies.
		if err := db.BlockCreate(models.Block{
			Slot:     preCutSlot,
			Hash:     hashAtPreCut,
			PrevHash: hashAtSnap,
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     postCutSlot,
			Hash:     hashAtPostCut,
			PrevHash: hashAtPreCut,
			Cbor:     []byte{0x80},
			Number:   3,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		// importTip checkpoint at the snapshot tip — Branch B in
		// calculateEpochNonce relies on this row to find the seam
		// between imported state and post-import accumulation.
		if err := db.SetBlockNonce(
			hashAtSnap, snapTipSlot, importedNonce, true, txn,
		); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtPreCut, preCutSlot, nonceAtPreCut, false, txn,
		); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn,
		)
	}))

	// LedgerState shaped like the bootstrap epoch row written by
	// generateAndSaveEpochs at import time: EvolvingNonce and
	// CandidateNonce both seeded from the snapshot's mid-epoch value.
	// currentTipBlockNonce intentionally left empty so the
	// resume-from-tip optimisation in calculateEpochNonce takes the
	// Branch B path (block_nonce row search).
	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             100,
			StartSlot:           epochStart,
			LengthInSlots:       uint(epochLength),
			SlotLength:          1000,
			EraId:               eras.ConwayEraDesc.Id,
			Nonce:               bytes.Repeat([]byte{0xee}, 32),
			EvolvingNonce:       importedNonce,
			CandidateNonce:      importedNonce,
			LastEpochBlockNonce: bytes.Repeat([]byte{0xfa}, 32),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	var candidate, evolving []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		_, ev, c, _, err := ls.calculateEpochNonce(
			txn,
			epochEnd,
			eras.ConwayEraDesc,
			ls.currentEpoch,
			nil,
		)
		candidate = c
		evolving = ev
		return err
	}))

	require.Equalf(
		t,
		hex.EncodeToString(nonceAtPreCut),
		hex.EncodeToString(candidate),
		"candidate must freeze at the last pre-cutoff block (slot %d). "+
			"Got %x. If this equals importedNonce (0xaa...0xaa), the "+
			"computation inherited the snapshot's mid-epoch candidate "+
			"and never replaced it with the frozen-at-cutoff value — "+
			"#2128 freeze. cutoff=%d, epoch=[%d,%d).",
		preCutSlot, candidate, cutoffSlot, epochStart, epochEnd,
	)
	require.Equalf(
		t,
		hex.EncodeToString(nonceAtPostCut),
		hex.EncodeToString(evolving),
		"evolving must equal the last block's stored nonce (slot %d). "+
			"Got %x.",
		postCutSlot, evolving,
	)
	require.NotEqualf(
		t,
		hex.EncodeToString(candidate),
		hex.EncodeToString(evolving),
		"candidate and evolving must differ once the chain crosses "+
			"the cutoff. Equal values mean the freeze was not applied.",
	)
}

// TestCalculateEpochNonce_PostMithrilBootstrapNoBlocksBeforeCutoff covers
// the edge case where the chain produces NO blocks in the window between
// the snapshot tip and the freeze cutoff (legal under low active-slots
// coefficient or just unlucky leader assignment). The snapshot tip slot
// is itself the last pre-cutoff slot, so the imported tip-time
// EvolvingNonce IS the correct frozen-at-cutoff value: in cardano-ledger,
// psCandidateNonce tracks evolving until the cutoff fires, so a snapshot
// taken at the very last pre-cutoff block has psCandidateNonce ==
// psEvolvingNonce == that block's accumulated evolving nonce.
//
// The rollover must therefore return candidate == importedNonce, NOT
// some other value derived from a phantom pre-cutoff block.
func TestCalculateEpochNonce_PostMithrilBootstrapNoBlocksBeforeCutoff(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		cutoffSlot  uint64 = 1015
		snapTipSlot uint64 = 1014 // last block strictly before cutoff
		postCutSlot uint64 = 1070 // last block of epoch (post-cutoff)
	)

	importedNonce := bytes.Repeat([]byte{0xaa}, 32)
	nonceAtPostCut := bytes.Repeat([]byte{0xcc}, 32)

	hashAtSnap := bytes.Repeat([]byte{0x14}, 32)
	hashAtPostCut := bytes.Repeat([]byte{0x70}, 32)
	prevHashAtSnap := bytes.Repeat([]byte{0x09}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot:     snapTipSlot,
			Hash:     hashAtSnap,
			PrevHash: prevHashAtSnap,
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     postCutSlot,
			Hash:     hashAtPostCut,
			PrevHash: hashAtSnap,
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtSnap, snapTipSlot, importedNonce, true, txn,
		); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn,
		)
	}))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             100,
			StartSlot:           epochStart,
			LengthInSlots:       uint(epochLength),
			SlotLength:          1000,
			EraId:               eras.ConwayEraDesc.Id,
			Nonce:               bytes.Repeat([]byte{0xee}, 32),
			EvolvingNonce:       importedNonce,
			CandidateNonce:      importedNonce,
			LastEpochBlockNonce: bytes.Repeat([]byte{0xfa}, 32),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	var candidate, evolving []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		_, ev, c, _, err := ls.calculateEpochNonce(
			txn,
			epochEnd,
			eras.ConwayEraDesc,
			ls.currentEpoch,
			nil,
		)
		candidate = c
		evolving = ev
		return err
	}))

	require.Equalf(
		t,
		hex.EncodeToString(importedNonce),
		hex.EncodeToString(candidate),
		"with no post-import blocks before the cutoff (slot %d), the "+
			"snapshot tip slot %d itself IS the last pre-cutoff block "+
			"and its stored block_nonce (= imported tip-time evolving) "+
			"is the correct frozen candidate. Got %x.",
		cutoffSlot, snapTipSlot, candidate,
	)
	require.Equalf(
		t,
		hex.EncodeToString(nonceAtPostCut),
		hex.EncodeToString(evolving),
		"evolving must equal the last block's stored nonce (slot %d). "+
			"Got %x.",
		postCutSlot, evolving,
	)
}

// TestCalculateEpochNonce_PostMithrilBootstrapWithoutCheckpoint covers
// the operational hazard where a deployment was bootstrapped with an
// importer that did not write the block_nonce checkpoint at the
// snapshot tip slot. Branch B
// in calculateEpochNonce searches for a block_nonce row matching
// prevEpoch.EvolvingNonce; with no checkpoint that row does not
// exist, and the resume seam is not found.
//
// The fast path should still produce correct results because it does
// NOT depend on Branch B — it directly looks up the cutoff block and
// the last block of the epoch by slot, and uses their stored
// block_nonce rows. Those rows exist for every post-import block
// (per-block accumulation correctly chains from the imported
// EvolvingNonce, even though the seed itself is not in block_nonce).
//
// This test guards against any future change that makes the fast
// path require a Branch B match.
func TestCalculateEpochNonce_PostMithrilBootstrapWithoutCheckpoint(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		cutoffSlot  uint64 = 1015
		snapTipSlot uint64 = 1010
		preCutSlot  uint64 = 1014
		postCutSlot uint64 = 1070
	)

	importedNonce := bytes.Repeat([]byte{0xaa}, 32)
	nonceAtPreCut := bytes.Repeat([]byte{0xbb}, 32)
	nonceAtPostCut := bytes.Repeat([]byte{0xcc}, 32)

	hashAtSnap := bytes.Repeat([]byte{0x10}, 32)
	hashAtPreCut := bytes.Repeat([]byte{0x14}, 32)
	hashAtPostCut := bytes.Repeat([]byte{0x70}, 32)
	prevHashAtSnap := bytes.Repeat([]byte{0x09}, 32)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot:     snapTipSlot,
			Hash:     hashAtSnap,
			PrevHash: prevHashAtSnap,
			Cbor:     []byte{0x80},
			Number:   1,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     preCutSlot,
			Hash:     hashAtPreCut,
			PrevHash: hashAtSnap,
			Cbor:     []byte{0x80},
			Number:   2,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot:     postCutSlot,
			Hash:     hashAtPostCut,
			PrevHash: hashAtPreCut,
			Cbor:     []byte{0x80},
			Number:   3,
			Type:     conway.BlockTypeConway,
		}, txn); err != nil {
			return err
		}
		// NOTE: deliberately NO checkpoint row at snapTipSlot.
		// Post-import block_nonce rows only.
		if err := db.SetBlockNonce(
			hashAtPreCut, preCutSlot, nonceAtPreCut, false, txn,
		); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn,
		)
	}))

	ls := &LedgerState{
		db:         db,
		currentEra: eras.ConwayEraDesc,
		currentEpoch: models.Epoch{
			EpochId:             100,
			StartSlot:           epochStart,
			LengthInSlots:       uint(epochLength),
			SlotLength:          1000,
			EraId:               eras.ConwayEraDesc.Id,
			Nonce:               bytes.Repeat([]byte{0xee}, 32),
			EvolvingNonce:       importedNonce,
			CandidateNonce:      importedNonce,
			LastEpochBlockNonce: bytes.Repeat([]byte{0xfa}, 32),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}

	var candidate, evolving []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		_, ev, c, _, err := ls.calculateEpochNonce(
			txn,
			epochEnd,
			eras.ConwayEraDesc,
			ls.currentEpoch,
			nil,
		)
		candidate = c
		evolving = ev
		return err
	}))

	require.Equalf(
		t,
		hex.EncodeToString(nonceAtPreCut),
		hex.EncodeToString(candidate),
		"without the snap-tip checkpoint, the fast path must still "+
			"freeze candidate at the last pre-cutoff block (slot %d) "+
			"via direct slot lookup. Got %x.",
		preCutSlot, candidate,
	)
	require.Equalf(
		t,
		hex.EncodeToString(nonceAtPostCut),
		hex.EncodeToString(evolving),
		"without the snap-tip checkpoint, evolving must still equal "+
			"the last block's stored nonce (slot %d). Got %x.",
		postCutSlot, evolving,
	)
}

// newConwayBootstrapStabilityCfg builds a CardanoNodeConfig with k=6,
// f=0.4 so 4k/f = 60 — the Conway nonce stability window. With epoch
// length 75 starting at slot 1000, the candidate-freeze cutoff lands at
// slot 1015, which lets the bootstrap test place blocks on each side of
// the cutoff with single-digit slot gaps.
func newConwayBootstrapStabilityCfg(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	cfg := &cardano.CardanoNodeConfig{
		ShelleyGenesisHash: strings.Repeat("11", 32),
	}
	require.NoError(t, loadByronGenesisForTest(t, cfg, strings.NewReader(`{
		"protocolConsts": {
			"k": 6,
			"protocolMagic": 42
		}
	}`)))
	require.NoError(t, cfg.LoadShelleyGenesisFromReader(strings.NewReader(`{
		"systemStart": "2026-01-01T00:00:00Z",
		"securityParam": 6,
		"activeSlotsCoeff": 0.4,
		"epochLength": 75,
		"slotLength": 1
	}`)))
	return cfg
}

func TestSameConnectionIdHandlesPartialNilAddrs(t *testing.T) {
	t.Parallel()

	remoteAddr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001}
	remoteOnly := ouroboros.ConnectionId{RemoteAddr: remoteAddr}
	remoteOnlySame := ouroboros.ConnectionId{
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
	}
	remoteOnlyOther := ouroboros.ConnectionId{
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3002},
	}
	localOnly := ouroboros.ConnectionId{LocalAddr: remoteAddr}
	fullId := ouroboros.ConnectionId{
		LocalAddr:  &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6000},
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
	}

	if !sameConnectionId(remoteOnly, remoteOnly) {
		t.Fatal(
			"sameConnectionId() = false, want true for identical remote-only ids",
		)
	}
	if !sameConnectionId(remoteOnly, remoteOnlySame) {
		t.Fatal(
			"sameConnectionId() = false, want true for equal remote-only ids",
		)
	}
	if sameConnectionId(remoteOnly, remoteOnlyOther) {
		t.Fatal(
			"sameConnectionId() = true, want false for differing remote-only ids",
		)
	}
	if sameConnectionId(remoteOnly, localOnly) {
		t.Fatal(
			"sameConnectionId() = true, want false for remote-only vs local-only",
		)
	}
	if sameConnectionId(remoteOnly, ouroboros.ConnectionId{}) {
		t.Fatal("sameConnectionId() = true, want false for remote-only vs zero")
	}
	if sameConnectionId(remoteOnly, fullId) {
		t.Fatal(
			"sameConnectionId() = true, want false for remote-only vs full id",
		)
	}
}

func TestConnIdKeyHandlesPartialNilAddrs(t *testing.T) {
	t.Parallel()

	remoteOnly := ouroboros.ConnectionId{
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
	}
	localOnly := ouroboros.ConnectionId{
		LocalAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 3001},
	}

	if connIdKey(ouroboros.ConnectionId{}) != "" {
		t.Fatal("connIdKey() != \"\" for zero value")
	}
	if connIdKey(remoteOnly) == "" {
		t.Fatal("connIdKey() = \"\" for remote-only id, want non-empty")
	}
	if connIdKey(remoteOnly) == connIdKey(localOnly) {
		t.Fatal(
			"connIdKey() equal for remote-only vs local-only, want distinct",
		)
	}
}

// newHookTestLedger builds a minimal LedgerState with a discard logger, matching
// the logger NewLedgerState installs when none is configured.
func newHookTestLedger(t *testing.T) (*LedgerState, *database.Database) {
	t.Helper()
	db := newDonationTestDB(t)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
	}
	return ls, db
}

// TestCaptureEpochBoundarySnapshotHookNil verifies the rollover capture is a
// no-op (and does not error) when no hook is installed — preserving the
// event-driven fallback-only behavior.
func TestCaptureEpochBoundarySnapshotHookNil(t *testing.T) {
	t.Parallel()

	ls, db := newHookTestLedger(t)

	result := &EpochRolloverResult{
		NewCurrentEpoch: models.Epoch{EpochId: 1, StartSlot: 432000},
	}
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.captureEpochBoundarySnapshot(
			txn, models.Epoch{EpochId: 0}, result,
		)
	}))
}

// TestCaptureEpochBoundarySnapshotHookInvoked verifies the hook is called inside
// the rollover transaction with an event derived from the new/previous epoch.
func TestCaptureEpochBoundarySnapshotHookInvoked(t *testing.T) {
	t.Parallel()

	ls, db := newHookTestLedger(t)

	var called bool
	var got event.EpochTransitionEvent
	ls.SetEpochBoundarySnapshotHook(
		func(_ *database.Txn, evt event.EpochTransitionEvent) error {
			called = true
			got = evt
			return nil
		},
	)

	result := &EpochRolloverResult{
		NewCurrentEpoch: models.Epoch{
			EpochId:   1,
			StartSlot: 432000,
			Nonce:     []byte{0xaa, 0xbb},
		},
	}
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.captureEpochBoundarySnapshot(
			txn, models.Epoch{EpochId: 0}, result,
		)
	}))

	require.True(t, called, "hook must be invoked during the rollover")
	require.Equal(t, uint64(0), got.PreviousEpoch)
	require.Equal(t, uint64(1), got.NewEpoch)
	require.Equal(t, uint64(432000), got.BoundarySlot)
	require.Equal(t, uint64(431999), got.SnapshotSlot)
	require.Equal(t, []byte{0xaa, 0xbb}, got.EpochNonce)
}

// TestCaptureEpochBoundarySnapshotHookFailureDeferred verifies that a hook
// failure is swallowed (the rollover is not aborted) and that the failed
// capture's writes are rolled back to the savepoint rather than committed.
func TestCaptureEpochBoundarySnapshotHookFailureDeferred(t *testing.T) {
	t.Parallel()

	ls, db := newHookTestLedger(t)

	ls.SetEpochBoundarySnapshotHook(
		func(txn *database.Txn, evt event.EpochTransitionEvent) error {
			// Write a row, then fail: the savepoint rollback must discard it.
			if err := db.Metadata().SaveRewardSnapshot(&models.RewardSnapshot{
				Epoch:           evt.NewEpoch,
				SnapshotType:    "mark",
				CapturedSlot:    1,
				BoundarySlot:    1,
				ProtocolVersion: 8,
				Authoritative:   true,
			}, txn.Metadata()); err != nil {
				return err
			}
			return errors.New("capture boom")
		},
	)

	result := &EpochRolloverResult{
		NewCurrentEpoch: models.Epoch{EpochId: 1, StartSlot: 432000},
	}
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		// Must NOT surface the hook error: capture failures defer to the
		// event-driven fallback rather than wedging the rollover.
		return ls.captureEpochBoundarySnapshot(
			txn, models.Epoch{EpochId: 0}, result,
		)
	}))

	snap, err := db.Metadata().GetRewardSnapshot(1, "mark", nil)
	require.NoError(t, err)
	require.Nil(t, snap,
		"a failed capture must be rolled back to the savepoint, not committed")
}

// maryPParamsWithExtraEntropy returns a minimally-populated Mary parameter set
// carrying extraEntropy, plus its CBOR. A nil rational field encodes as CBOR
// null and decodes back, so only ExtraEntropy needs a real value here.
func maryPParamsWithExtraEntropy(
	t *testing.T,
	entropy []byte,
) (*mary.MaryProtocolParameters, []byte) {
	t.Helper()
	pp := &mary.MaryProtocolParameters{
		ProtocolMajor: 4,
		// Block sizes the votedFuturePParams guard accepts.
		MaxBlockBodySize:   65536,
		MaxTxSize:          16384,
		MaxBlockHeaderSize: 1100,
	}
	if len(entropy) == lcommon.Blake2b256Size {
		pp.ExtraEntropy.Type = lcommon.NonceTypeNonce
		copy(pp.ExtraEntropy.Value[:], entropy)
	}
	data, err := cbor.Encode(pp)
	require.NoError(t, err)
	// Guard the fixture: the production path reads this back through the era's
	// own decoder, so a shape that does not round-trip would make the test
	// pass for the wrong reason.
	decoded, err := eras.DecodePParamsMary(data)
	require.NoError(t, err)
	decodedMary, ok := decoded.(*mary.MaryProtocolParameters)
	require.True(t, ok)
	require.Equal(t, pp.ExtraEntropy, decodedMary.ExtraEntropy)
	return pp, data
}

// TestCalculateEpochNonceFoldsExtraEntropy covers the authoritative rollover
// path, which writes the epoch record every later consumer reads. Unlike the
// header-verification path it does not forecast: the boundary has already
// enacted the new epoch's protocol parameters, and their extraEntropy is what
// the nonce must mix.
func TestCalculateEpochNonceFoldsExtraEntropy(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		preCutSlot  uint64 = 1014
		postCutSlot uint64 = 1070
		prevEpochID uint64 = 258
	)

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	frozenCandidate := mustDecodeHex(t, mainnetEpoch259Candidate)
	carriedLab := mustDecodeHex(t, mainnetEpoch259Lab)

	importedNonce := mustDecodeHex(t, mainnetEpoch259Lab)
	nonceAtPostCut := mustDecodeHex(t, mainnetEpoch259Nonce)
	hashAtPreCut := mustDecodeHex(t, mainnetEpoch259Candidate)
	hashAtPostCut := mustDecodeHex(t, mainnetEpoch259Nonce)
	prevHashAtPreCut := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot: preCutSlot, Hash: hashAtPreCut, PrevHash: prevHashAtPreCut,
			Cbor: []byte{0x80}, Number: 1, Type: mary.BlockTypeMary,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot: postCutSlot, Hash: hashAtPostCut, PrevHash: hashAtPreCut,
			Cbor: []byte{0x80}, Number: 2, Type: mary.BlockTypeMary,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtPreCut, preCutSlot, frozenCandidate, false, txn); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn)
	}))

	prevEpoch := models.Epoch{
		EpochId:             prevEpochID,
		StartSlot:           epochStart,
		LengthInSlots:       uint(epochLength),
		SlotLength:          1000,
		EraId:               eras.MaryEraDesc.Id,
		Nonce:               mustDecodeHex(t, mainnetEpoch259Lab),
		EvolvingNonce:       importedNonce,
		CandidateNonce:      importedNonce,
		LastEpochBlockNonce: carriedLab,
	}

	ls := &LedgerState{
		db:           db,
		currentEra:   eras.MaryEraDesc,
		currentEpoch: prevEpoch,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	enacted, _ := maryPParamsWithExtraEntropy(t, entropy)
	neutral, _ := maryPParamsWithExtraEntropy(t, nil)

	var withEntropy, withoutParam []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		n, _, candidate, _, err := ls.calculateEpochNonce(
			txn, epochEnd, eras.MaryEraDesc, prevEpoch, enacted,
		)
		if err != nil {
			return err
		}
		require.Equal(
			t,
			frozenCandidate,
			candidate,
			"candidate nonce must freeze at the pre-cutoff block nonce",
		)
		withEntropy = n
		n, _, _, _, err = ls.calculateEpochNonce(
			txn, epochEnd, eras.MaryEraDesc, prevEpoch, neutral,
		)
		withoutParam = n
		return err
	}))

	require.Equal(
		t,
		mainnetEpoch259Nonce,
		hex.EncodeToString(withEntropy),
		"epoch nonce must fold the enacted extraEntropy",
	)

	// Negative case: the same inputs with a neutral extraEntropy must produce
	// the unmixed nonce, so the parameter is what moves the result rather than
	// anything else in the fixture.
	expectedNeutral, err := lcommon.CalculateEpochNonce(
		frozenCandidate, carriedLab, nil,
	)
	require.NoError(t, err)
	require.Equal(
		t,
		expectedNeutral.Bytes(),
		withoutParam,
		"a neutral extraEntropy must leave the epoch nonce unmixed",
	)
}

// TestCalculateEpochNonceNeutralLabMixesExtraEntropy covers the boundary where
// the carried lastEpochBlockNonce is NeutralNonce and the extraEntropy is not.
// NeutralNonce is the identity of the nonce operator, so the assembly collapses
// to candidateNonce ⭒ extraEntropy -- not to candidateNonce alone, which is
// what the NeutralNonce short-circuit returns when the entropy term is dropped.
func TestCalculateEpochNonceNeutralLabMixesExtraEntropy(t *testing.T) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: ""})
	require.NoError(t, err)

	cfg := newConwayBootstrapStabilityCfg(t)

	const (
		epochStart  uint64 = 1000
		epochLength uint64 = 75
		epochEnd    uint64 = epochStart + epochLength
		preCutSlot  uint64 = 1014
		postCutSlot uint64 = 1070
		prevEpochID uint64 = 258
	)

	entropy := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)
	frozenCandidate := mustDecodeHex(t, mainnetEpoch259Candidate)

	importedNonce := mustDecodeHex(t, mainnetEpoch259Lab)
	nonceAtPostCut := mustDecodeHex(t, mainnetEpoch259Nonce)
	hashAtPreCut := mustDecodeHex(t, mainnetEpoch259Candidate)
	hashAtPostCut := mustDecodeHex(t, mainnetEpoch259Nonce)
	prevHashAtPreCut := mustDecodeHex(t, mainnetEpoch259ExtraEntropy)

	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		if err := db.BlockCreate(models.Block{
			Slot: preCutSlot, Hash: hashAtPreCut, PrevHash: prevHashAtPreCut,
			Cbor: []byte{0x80}, Number: 1, Type: mary.BlockTypeMary,
		}, txn); err != nil {
			return err
		}
		if err := db.BlockCreate(models.Block{
			Slot: postCutSlot, Hash: hashAtPostCut, PrevHash: hashAtPreCut,
			Cbor: []byte{0x80}, Number: 2, Type: mary.BlockTypeMary,
		}, txn); err != nil {
			return err
		}
		if err := db.SetBlockNonce(
			hashAtPreCut, preCutSlot, frozenCandidate, false, txn); err != nil {
			return err
		}
		return db.SetBlockNonce(
			hashAtPostCut, postCutSlot, nonceAtPostCut, false, txn)
	}))

	prevEpoch := models.Epoch{
		EpochId:        prevEpochID,
		StartSlot:      epochStart,
		LengthInSlots:  uint(epochLength),
		SlotLength:     1000,
		EraId:          eras.MaryEraDesc.Id,
		Nonce:          mustDecodeHex(t, mainnetEpoch259Lab),
		EvolvingNonce:  importedNonce,
		CandidateNonce: importedNonce,
		// NeutralNonce: no carried last-block nonce.
		LastEpochBlockNonce: nil,
	}

	ls := &LedgerState{
		db:           db,
		currentEra:   eras.MaryEraDesc,
		currentEpoch: prevEpoch,
		config: LedgerStateConfig{
			CardanoNodeConfig: cfg,
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	ls.publishSnapshotsLocked()

	enacted, _ := maryPParamsWithExtraEntropy(t, entropy)

	var nonce []byte
	require.NoError(t, db.Transaction(true).Do(func(txn *database.Txn) error {
		n, _, _, _, err := ls.calculateEpochNonce(
			txn, epochEnd, eras.MaryEraDesc, prevEpoch, enacted,
		)
		nonce = n
		return err
	}))

	require.NotEqual(
		t,
		frozenCandidate,
		nonce,
		"a non-neutral extraEntropy must not leave the candidate nonce unmixed",
	)
	want, err := lcommon.CalculateRollingNonce(frozenCandidate, entropy)
	require.NoError(t, err)
	require.Equal(t, want.Bytes(), nonce)
}

func TestEraTransitionsRunAfterSourceEraPParamEnactment(t *testing.T) {
	t.Parallel()

	path := []uint{eras.BabbageEraDesc.Id}
	before, after := splitEraTransitionsForRollover(path)

	require.Empty(t, before,
		"successor transitions must not replace the source era before rollover")
	require.Equal(t, path, after,
		"the successor transition must run after source-era pparam enactment")
}

// TestCreateGenesisBlockFileBackedNoFKError drives the real genesis sync path
// (createGenesisBlock -> database.SetGenesisTransaction -> UtxoLedgerToModel ->
// metadata SetGenesisTransaction) on a file-backed SQLite store, where
// foreign_keys=ON is enforced.
//
// Genesis UTxOs are unspent/unreferenced, so the utxo columns spent_at_tx_id /
// referenced_by_tx_id / collateral_by_tx_id (FKs to transaction(hash)) must be
// stored as SQL NULL. If they are bound as an empty blob, the FK fails with
// "FOREIGN KEY constraint failed (787)". This is the failure reported for
// BURSA_SYNC=genesis on preview/devnet.
//
// It also re-runs createGenesisBlock to confirm idempotency.
func TestCreateGenesisBlockFileBackedNoFKError(t *testing.T) {
	t.Parallel()

	networks := []struct {
		name       string
		configPath string
	}{
		{name: "preview", configPath: "preview/config.json"},
		{name: "devnet", configPath: "devnet/config.json"},
	}

	for _, nw := range networks {
		t.Run(nw.name, func(t *testing.T) {
			// File-backed store: enables foreign_keys(1) + WAL, the
			// production configuration that matches the reported failure.
			db, err := dbtest.NewDatabase(t, &database.Config{
				DataDir: t.TempDir(),
			})
			require.NoError(t, err)

			nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
				nw.configPath,
				nw.name,
				cardano.EmbeddedConfigFS,
			)
			require.NoError(t, err)

			ls := &LedgerState{
				db: db,
				config: LedgerStateConfig{
					Database:          db,
					CardanoNodeConfig: nodeCfg,
					Logger: slog.New(
						slog.NewTextHandler(io.Discard, nil),
					),
				},
			}

			// First run: must not hit the FK 787 error.
			require.NoError(
				t,
				ls.createGenesisBlock(),
				"createGenesisBlock should not fail with FK constraint",
			)

			raw, err := dbtest.RawSQLiteMetadata(t, db)
			require.NoError(t, err)

			// At least one genesis UTxO should have been created so the
			// assertions below are meaningful.
			var utxoCount int64
			require.NoError(t, raw.QueryRow(
				"SELECT COUNT(*) FROM utxo",
			).Scan(&utxoCount))
			require.Greater(t, utxoCount, int64(0), "genesis UTxOs created")

			// The hash FK columns must be stored as NULL, never empty blobs.
			var nonNull int64
			require.NoError(t, raw.QueryRow(`
SELECT COUNT(*) FROM utxo
WHERE spent_at_tx_id IS NOT NULL
   OR referenced_by_tx_id IS NOT NULL
   OR collateral_by_tx_id IS NOT NULL`,
			).Scan(&nonNull))
			require.Equal(
				t,
				int64(0),
				nonNull,
				"genesis UTxOs must store NULL hash FKs, not empty blobs",
			)

			// Second run: idempotent, still no error.
			require.NoError(
				t,
				ls.createGenesisBlock(),
				"re-running createGenesisBlock must remain idempotent",
			)
		})
	}
}

// The Musashi Conway genesis declares three genesis committee members, all
// key-hash cold credentials, each expiring at epoch 293.
var musashiGenesisCommitteeColdKeys = []string{
	"0fa32e5f69a89afa3f5e1074660b975dde8e5a89c1b8004d49501e33",
	"518a0c96344656d332625e33aa680b6c25bbce6b5972a30adf1dce8d",
	"8feda2412bec6f79bc5996a5055bcff28d230cb9c85fb9d5e8743a46",
}

// TestCreateGenesisBlockSeedsCommitteeOnExistingDatabase covers the upgrade
// path, which is the population that actually has the bug: a node already
// synced from genesis on a build that never seeded the committee.
//
// Such a database has matching genesis CBOR and a nonzero tip, so
// createGenesisBlock takes its early-return branch and never reaches the
// genesis-creation transaction. Seeding only from that transaction would
// therefore fix new nodes and leave every existing one broken.
func TestCreateGenesisBlockSeedsCommitteeOnExistingDatabase(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)

	// Stand in for a database written by a build with no committee seed:
	// genesis CBOR present and matching, a tip well past zero, and no
	// committee_member rows at all.
	genesisHash, err := GenesisBlockHash(ls.config.CardanoNodeConfig)
	require.NoError(t, err)
	require.NoError(t, db.SetGenesisCbor(0, genesisHash[:], []byte{0x80}, nil))
	ls.currentTip.Point = ocommon.Point{Slot: 1_000_000}
	require.Equal(t, 0, committeeMemberRowCount(t, db))

	require.NoError(t, ls.createGenesisBlock())

	lv := &LedgerView{ls: ls}
	for _, coldKeyHex := range musashiGenesisCommitteeColdKeys {
		coldKey, err := hex.DecodeString(coldKeyHex)
		require.NoError(t, err)
		member, err := lv.CommitteeCredentialMember(lcommon.Credential{
			CredType:   lcommon.CredentialTypeAddrKeyHash,
			Credential: lcommon.NewBlake2b224(coldKey),
		})
		require.NoError(t, err)
		require.NotNil(
			t,
			member,
			"genesis committee member %s must be backfilled on an existing database",
			coldKeyHex,
		)
		require.Equal(
			t,
			uint64(musashiGenesisCommitteeExpiry),
			member.ExpiryEpoch,
		)
	}
}

// TestCreateGenesisBlockCommitteeReplayIdempotent proves re-running genesis
// initialization over a store that already holds the genesis committee
// leaves a single row per member rather than a duplicate soft-delete/insert
// pair.
func TestCreateGenesisBlockCommitteeReplayIdempotent(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())
	require.NoError(t, ls.createGenesisBlock())

	require.Equal(
		t,
		len(musashiGenesisCommitteeColdKeys),
		committeeMemberRowCount(t, db),
	)
}

// TestParseGenesisCommitteeCredential exercises both credential prefixes and
// the rejection paths for malformed genesis committee member keys.
func TestParseGenesisCommitteeCredential(t *testing.T) {
	t.Parallel()

	keyHash := bytes.Repeat([]byte{0xab}, 28)
	tag, hash, err := parseGenesisCommitteeCredential(
		"keyHash-" + hex.EncodeToString(keyHash),
	)
	require.NoError(t, err)
	require.Equal(t, uint8(lcommon.CredentialTypeAddrKeyHash), tag)
	require.Equal(t, keyHash, hash)

	scriptHash := bytes.Repeat([]byte{0xcd}, 28)
	tag, hash, err = parseGenesisCommitteeCredential(
		"scriptHash-" + hex.EncodeToString(scriptHash),
	)
	require.NoError(t, err)
	require.Equal(t, uint8(lcommon.CredentialTypeScriptHash), tag)
	require.Equal(t, scriptHash, hash)

	_, _, err = parseGenesisCommitteeCredential("bogus-deadbeef")
	require.Error(t, err)

	_, _, err = parseGenesisCommitteeCredential("keyHash-nothex")
	require.Error(t, err)

	_, _, err = parseGenesisCommitteeCredential("keyHash-abcd")
	require.Error(t, err)
}

// TestEnsureGenesisCommitteeRejectsNegativeExpiry proves a malformed Conway
// genesis fails initialization instead of seating a member with a wrapped
// term.
//
// conway-genesis.json models the committee expiry as a bare JSON number, so it
// decodes into a signed int. Converting a negative value straight to the
// store's unsigned epoch would wrap it to a near-maximum uint64 -- a term no
// epoch boundary would ever expire -- so the seed must refuse it outright.
func TestEnsureGenesisCommitteeRejectsNegativeExpiry(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)

	// The embedded config is parsed fresh on every load, so mutating this
	// state's genesis below cannot leak into the other tests in this file.
	other, _ := genesisConstitutionTestState(t)
	require.NotSame(
		t,
		ls.config.CardanoNodeConfig.ConwayGenesis(),
		other.config.CardanoNodeConfig.ConwayGenesis(),
		"each test state must own its genesis for the mutation below to be safe",
	)

	members := ls.config.CardanoNodeConfig.ConwayGenesis().Committee.Members
	rawKey := "keyHash-" + musashiGenesisCommitteeColdKeys[0]
	require.Contains(t, members, rawKey)
	members[rawKey] = -1

	err := ls.ensureGenesisCommittee(nil)
	require.ErrorContains(t, err, "negative expiry epoch -1")
	require.ErrorContains(t, err, musashiGenesisCommitteeColdKeys[0])
	require.Equal(
		t,
		0,
		committeeMemberRowCount(t, db),
		"a malformed genesis committee must seat no members at all",
	)
}

// TestEnsureGenesisCommitteeRejectsNegativeExpiryWhenAlreadySeeded proves the
// malformed-genesis check fails closed on the upgrade path too.
//
// Validating the expiry only after the already-seeded check would let the same
// malformed genesis start a node whose database happens to hold rows already
// while refusing a fresh one, even though the genesis file is equally
// malformed in both cases.
func TestEnsureGenesisCommitteeRejectsNegativeExpiryWhenAlreadySeeded(
	t *testing.T,
) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())
	seeded := committeeMemberRowCount(t, db)
	require.Equal(t, len(musashiGenesisCommitteeColdKeys), seeded)

	members := ls.config.CardanoNodeConfig.ConwayGenesis().Committee.Members
	rawKey := "keyHash-" + musashiGenesisCommitteeColdKeys[0]
	require.Contains(t, members, rawKey)
	members[rawKey] = -7

	err := ls.ensureGenesisCommittee(nil)
	require.ErrorContains(t, err, "negative expiry epoch -7")
	require.ErrorContains(t, err, musashiGenesisCommitteeColdKeys[0])
	require.Equal(
		t,
		seeded,
		committeeMemberRowCount(t, db),
		"the failed check must not add or remove rows",
	)
}

// TestGenesisCommitteeExpiryEpoch covers the signed-to-unsigned conversion
// directly, including the most negative int, which is the value a straight
// conversion wraps furthest.
func TestGenesisCommitteeExpiryEpoch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expiry  int
		want    uint64
		wantErr bool
	}{
		{"zero", 0, 0, false},
		{
			"musashi genesis expiry",
			musashiGenesisCommitteeExpiry,
			musashiGenesisCommitteeExpiry,
			false,
		},
		{"negative one", -1, 0, true},
		{"most negative int", math.MinInt, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := genesisCommitteeExpiryEpoch(tc.expiry)
			if tc.wantErr {
				require.Error(t, err)
				require.Zero(t, got)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// committeeMemberRowCount returns the number of stored committee_member rows.
func committeeMemberRowCount(t *testing.T, db *database.Database) int {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	defer func() { require.NoError(t, raw.Close()) }()
	var count int
	require.NoError(
		t,
		raw.QueryRow("SELECT COUNT(*) FROM committee_member").Scan(&count),
	)
	return count
}

// The constitution the Musashi Conway genesis declares. Genesis
// initialization must record exactly these bytes, since guardrails
// validation compares the script hash against every parameter-change and
// treasury-withdrawal proposal's policy hash.
const (
	musashiConstitutionURL = "ipfs://" +
		"bafkreiazhhawe7sjwuthcfgl3mmv2swec7sukvclu3oli7qdyz4uhhuvmy"
	musashiConstitutionAnchorHash = "2a61e2f4b63442978140c77a70daab396" +
		"1b22b12b63b13949a390c097214d1c5"
	musashiConstitutionScriptHash = "fa24fb305126805cf2164c161d852a0e" +
		"7330cf988f1fe558cf7d4a64"
)

// genesisConstitutionTestState builds a LedgerState over a file-backed test
// database with the Musashi configuration, whose Conway genesis declares a
// constitution with a guardrails script.
func genesisConstitutionTestState(
	t *testing.T,
) (*LedgerState, *database.Database) {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"musashi/config.json",
		"musashi",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	return ls, db
}

// requireGenesisConstitution asserts the view reports exactly the
// constitution the Musashi Conway genesis declares.
func requireGenesisConstitution(t *testing.T, lv *LedgerView) {
	t.Helper()
	got, err := lv.Constitution()
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, musashiConstitutionURL, got.Anchor.Url)
	require.Equal(
		t,
		musashiConstitutionAnchorHash,
		hex.EncodeToString(got.Anchor.DataHash[:]),
	)
	require.Equal(
		t,
		musashiConstitutionScriptHash,
		hex.EncodeToString(got.ScriptHash),
	)
}

// TestCreateGenesisBlockSeedsConstitution proves a node initialized from
// Conway genesis reports the genesis constitution, so guardrails validation
// accepts a treasury-withdrawal proposal carrying the genesis guardrails
// script hash and rejects one carrying none. Without the seed the lookup
// fails closed and every such proposal is rejected until a NewConstitution
// action is enacted.
func TestCreateGenesisBlockSeedsConstitution(t *testing.T) {
	t.Parallel()

	ls, _ := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())

	lv := &LedgerView{ls: ls}
	requireGenesisConstitution(t, lv)

	scriptHash, err := hex.DecodeString(musashiConstitutionScriptHash)
	require.NoError(t, err)
	require.NoError(t, constitutionTestGuardrails(t, lv, scriptHash))

	err = constitutionTestGuardrails(t, lv, nil)
	require.Error(t, err)
	var mismatch conway.InvalidGuardrailsScriptHashError
	require.ErrorAs(t, err, &mismatch)
	require.Equal(t, scriptHash, mismatch.Expected)
}

// TestCreateGenesisBlockConstitutionReplayIdempotent proves re-running
// genesis initialization over a store that already holds the genesis
// constitution leaves a single slot-0 row rather than a duplicate.
func TestCreateGenesisBlockConstitutionReplayIdempotent(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())
	require.NoError(t, ls.createGenesisBlock())

	requireGenesisConstitution(t, &LedgerView{ls: ls})
	require.Equal(t, 1, constitutionRowCount(t, db))
}

// TestCreateGenesisBlockConstitutionSeededOnRestart proves the restart path
// -- an existing database whose genesis CBOR already matches, which returns
// before genesis storage is rewritten -- still records the genesis
// constitution. A database created before the constitution was seeded
// reaches genesis initialization only through that path.
func TestCreateGenesisBlockConstitutionSeededOnRestart(t *testing.T) {
	t.Parallel()

	ls, db := genesisConstitutionTestState(t)
	require.NoError(t, ls.createGenesisBlock())

	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	_, err = raw.Exec("DELETE FROM constitution")
	require.NoError(t, err)
	require.NoError(t, raw.Close())
	require.Equal(t, 0, constitutionRowCount(t, db))

	// Advance past genesis so the second run takes the existing-database
	// path instead of rewriting genesis storage.
	ls.currentTip.Point = ocommon.Point{Slot: 100}
	require.NoError(t, ls.createGenesisBlock())

	requireGenesisConstitution(t, &LedgerView{ls: ls})
}

// constitutionRowCount returns the number of stored constitution rows.
func constitutionRowCount(t *testing.T, db *database.Database) int {
	t.Helper()
	raw, err := dbtest.RawSQLiteMetadata(t, db)
	require.NoError(t, err)
	defer func() { require.NoError(t, raw.Close()) }()
	var count int
	require.NoError(
		t,
		raw.QueryRow("SELECT COUNT(*) FROM constitution").Scan(&count),
	)
	return count
}

// TestCreateGenesisBlockSkipsGenesisStakingAfterMithrilBootstrap is the
// same-bug-class regression test the PR review recommended: it
// found that SetGenesisStaking (and SetGenesisGovernance, covered by
// TestCreateGenesisBlockSkipsGenesisGovernanceAfterMithrilBootstrap below)
// weren't gated by the same bootstrappedFromMithril guard as genesis UTxO
// insertion. Both upsert current-state rows (ON CONFLICT ... DO UPDATE),
// and a Mithril-bootstrapped node's imported ledger snapshot
// (ledgerstate/import.go's importCertState) already reflects the correct
// current pool/delegation state as of the bootstrap point -- reapplying
// stale genesis-config values would silently resurrect a pool genuinely
// retired (or a delegation genuinely changed) before the bootstrap point.
//
// Uses the embedded devnet config, the only bundled network that declares
// a nonzero genesis pool + stake delegation (mainnet/preview/preprod/
// musashi all declare zero, so the bug was unreachable there, but live on
// devnet).
func TestCreateGenesisBlockSkipsGenesisStakingAfterMithrilBootstrap(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"devnet/config.json",
		"devnet",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)

	genesisPools, _, err := nodeCfg.ShelleyGenesis().InitialPools()
	require.NoError(t, err)
	require.NotEmpty(
		t, genesisPools,
		"devnet must declare at least one genesis pool for this test to "+
			"be meaningful",
	)
	var poolIdHex string
	for k := range genesisPools {
		poolIdHex = k
		break
	}
	poolKeyHash, err := hex.DecodeString(poolIdHex)
	require.NoError(t, err)

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	// Same bootstrap shape as
	// TestCreateGenesisBlockSkipsUtxoInsertionAfterMithrilBootstrap: a
	// currentTip past slot 0 with no genesis CBOR yet stored.
	ls.currentTip.Point.Slot = 42
	require.NoError(t, ls.createGenesisBlock())

	_, err = db.GetPool(lcommon.PoolKeyHash(poolKeyHash), true, nil)
	require.ErrorIs(
		t, err, models.ErrPoolNotFound,
		"a genesis pool must not be (re-)inserted after a Mithril "+
			"bootstrap -- the imported ledger snapshot is the only "+
			"authority on whether it is still registered or was already "+
			"retired before the bootstrap point",
	)

	// Re-running (as a real startup would on every restart) must remain
	// idempotent and continue to skip insertion.
	require.NoError(t, ls.createGenesisBlock())
	_, err = db.GetPool(lcommon.PoolKeyHash(poolKeyHash), true, nil)
	require.ErrorIs(t, err, models.ErrPoolNotFound)
}

// TestCreateGenesisBlockSkipsGenesisGovernanceAfterMithrilBootstrap covers
// the same guard for SetGenesisGovernance. No bundled network config
// declares a genesis DRep today (devnet's conway-genesis.json has an
// empty initialDReps), so this synthesizes a minimal one via
// LoadConwayGenesisFromReader, the same test-only escape hatch
// config/cardano/node.go documents "mostly for tests".
func TestCreateGenesisBlockSkipsGenesisGovernanceAfterMithrilBootstrap(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"devnet/config.json",
		"devnet",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)

	const drepKeyHashHex = "00112233445566778899aabbccddeeff001122334455667788990011"
	require.Len(t, drepKeyHashHex, 56, "must decode to 28 bytes")
	conwayGenesisJson := `{
		"poolVotingThresholds": {},
		"dRepVotingThresholds": {},
		"committeeMinSize": 0,
		"committeeMaxTermLength": 0,
		"govActionLifetime": 0,
		"govActionDeposit": 0,
		"dRepDeposit": 0,
		"dRepActivity": 0,
		"minFeeRefScriptCostPerByte": null,
		"plutusV3CostModel": [],
		"constitution": {"anchor": {"dataHash": "", "url": ""}, "script": ""},
		"committee": {"members": {}, "threshold": null},
		"delegs": {},
		"initialDReps": {
			"keyHash-` + drepKeyHashHex + `": {
				"expiry": 500,
				"deposit": 500000000,
				"anchor": null
			}
		}
	}`
	require.NoError(
		t,
		nodeCfg.LoadConwayGenesisFromReader(
			strings.NewReader(conwayGenesisJson),
		),
	)

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	ls.currentTip.Point.Slot = 42
	require.NoError(t, ls.createGenesisBlock())

	drepKeyHash, err := hex.DecodeString(drepKeyHashHex)
	require.NoError(t, err)
	_, err = db.GetDrep(drepKeyHash, true, nil)
	require.ErrorIs(
		t, err, models.ErrDrepNotFound,
		"a genesis DRep must not be (re-)inserted after a Mithril "+
			"bootstrap -- the imported ledger snapshot is the only "+
			"authority on current DRep/delegation state",
	)

	require.NoError(t, ls.createGenesisBlock())
	_, err = db.GetDrep(drepKeyHash, true, nil)
	require.ErrorIs(t, err, models.ErrDrepNotFound)
}

func TestGenesisUtxoStorageAndRetrieval(t *testing.T) {
	t.Parallel()

	// Create temp directory for database
	tmpDir, err := os.MkdirTemp("", "genesis_utxo_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logger := slog.New(
		slog.NewTextHandler(
			os.Stdout,
			&slog.HandlerOptions{Level: slog.LevelDebug},
		),
	)

	// Create database
	dbConfig := &database.Config{
		DataDir: tmpDir,
		Logger:  logger,
	}
	db, err := dbtest.NewDatabase(t, dbConfig)
	require.NoError(t, err)
	defer dbtest.CloseDatabase(db)

	// Load cardano config from embedded preview network
	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"preview/config.json",
		"preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)

	// Get genesis UTxOs
	byronGenesis := nodeCfg.ByronGenesis()
	require.NotNil(t, byronGenesis)

	byronGenesisUtxos, err := byronGenesis.GenesisUtxos()
	require.NoError(t, err)
	t.Logf("Found %d Byron genesis UTxOs", len(byronGenesisUtxos))

	if len(byronGenesisUtxos) == 0 {
		t.Skip("No Byron genesis UTxOs to test")
	}

	// First, encode the genesis outputs to CBOR (like createGenesisBlock does)
	encodedUtxos := make([]lcommon.Utxo, len(byronGenesisUtxos))
	for i, utxo := range byronGenesisUtxos {
		// Encode the output to CBOR
		cborData, err := cbor.Encode(utxo.Output)
		require.NoError(t, err, "Failed to encode output %d", i)

		// Create a new Utxo with CBOR-encoded output
		switch output := utxo.Output.(type) {
		case byron.ByronTransactionOutput:
			newOutput := output
			(&newOutput).SetCbor(cborData)
			encodedUtxos[i] = lcommon.Utxo{
				Id:     utxo.Id,
				Output: newOutput,
			}
		default:
			t.Fatalf("Unexpected output type: %T", utxo.Output)
		}
		utxoTxID := utxo.Id.Id()
		t.Logf("Encoded UTxO %x#%d: %d bytes",
			utxoTxID[:8], utxo.Id.Index(), len(cborData))
	}

	// Get the Byron genesis hash to use as the synthetic block hash
	genesisHash, err := GenesisBlockHash(nodeCfg)
	require.NoError(t, err)

	// Create a transaction to store genesis UTxOs
	txn := db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		// Build and store genesis block CBOR
		utxoOffsets := make(map[database.UtxoRef]database.CborOffset)

		// Build synthetic genesis block CBOR (simplified version)
		// First, encode each UTxO to get its CBOR
		var blockCbor []byte

		// Track offsets as we build the block
		for _, utxo := range encodedUtxos {
			txId := utxo.Id.Id().Bytes()
			outputIdx := utxo.Id.Index()

			// Get the output CBOR
			outputCbor := utxo.Output.Cbor()
			if len(outputCbor) == 0 {
				return fmt.Errorf(
					"UTxO %x#%d still has no CBOR after encoding",
					txId[:8], outputIdx,
				)
			}

			var txHashArray [32]byte
			copy(txHashArray[:], txId)

			ref := database.UtxoRef{
				TxId:      txHashArray,
				OutputIdx: outputIdx,
			}

			// Track offset within block (simplified: just concatenate)
			offset := uint32(len(blockCbor))
			blockCbor = append(blockCbor, outputCbor...)

			utxoOffsets[ref] = database.CborOffset{
				BlockSlot:  0,
				BlockHash:  genesisHash,
				ByteOffset: offset,
				ByteLength: uint32(len(outputCbor)),
			}

			t.Logf("UTxO %x#%d: offset=%d, length=%d",
				txId[:8], outputIdx, offset, len(outputCbor))
		}

		// Store the genesis block CBOR
		t.Logf("Storing genesis block CBOR: %d bytes", len(blockCbor))
		if err := db.SetGenesisCbor(0, genesisHash[:], blockCbor, txn); err != nil {
			return err
		}

		// Now store each genesis transaction
		for _, utxo := range encodedUtxos {
			txId := utxo.Id.Id().Bytes()
			outputIdx := utxo.Id.Index()

			var txHashArray [32]byte
			copy(txHashArray[:], txId)

			ref := database.UtxoRef{
				TxId:      txHashArray,
				OutputIdx: outputIdx,
			}

			offset, ok := utxoOffsets[ref]
			if !ok {
				return fmt.Errorf(
					"no offset for UTxO %x#%d after building block",
					txId[:8], outputIdx,
				)
			}

			// Store the offset
			offsetData := database.EncodeUtxoOffset(&offset)
			t.Logf(
				"Storing UTxO offset: %s",
				hex.EncodeToString(offsetData[:20]),
			)

			blob := db.Blob()
			if blob == nil {
				return fmt.Errorf("blob store is nil")
			}
			blobTxn := txn.Blob()
			if blobTxn == nil {
				return fmt.Errorf("blob transaction is nil")
			}

			if err := blob.SetUtxo(blobTxn, txId, outputIdx, offsetData); err != nil {
				return err
			}
		}

		return nil
	})
	require.NoError(t, err)

	// Now try to retrieve each genesis UTxO
	t.Log("Retrieving genesis UTxOs...")
	for _, utxo := range byronGenesisUtxos {
		txIDHash := utxo.Id.Id()
		txId := txIDHash[:]
		txIDPrefix := txIDHash[:8]
		outputIdx := utxo.Id.Index()

		// Try to get the UTxO from blob store
		readTxn := db.Transaction(false)
		blob := db.Blob()
		require.NotNil(t, blob)
		blobTxn := readTxn.Blob()
		require.NotNil(t, blobTxn)

		data, err := blob.GetUtxo(blobTxn, txId, outputIdx)
		if err != nil {
			t.Errorf("Failed to get UTxO %s#%d from blob: %v",
				hex.EncodeToString(txIDPrefix), outputIdx, err)
			readTxn.Rollback() //nolint:errcheck
			continue
		}

		t.Logf(
			"Retrieved data for %x#%d: %d bytes, first bytes: %s",
			txIDPrefix,
			outputIdx,
			len(data),
			hex.EncodeToString(data[:min(20, len(data))]),
		)

		// Check if it's an offset
		if database.IsUtxoOffsetStorage(data) {
			t.Logf("Data is offset storage (has DOFF magic)")

			// Decode the offset
			offset, err := database.DecodeUtxoOffset(data)
			require.NoError(
				t,
				err,
				"Failed to decode offset for %x#%d",
				txIDPrefix,
				outputIdx,
			)

			t.Logf(
				"Offset: slot=%d, hash=%x, offset=%d, length=%d",
				offset.BlockSlot,
				offset.BlockHash[:8],
				offset.ByteOffset,
				offset.ByteLength,
			)

			// Try to get the block
			blockCbor, _, err := blob.GetBlock(
				blobTxn,
				offset.BlockSlot,
				offset.BlockHash[:],
			)
			if err != nil {
				t.Errorf(
					"Failed to get block for offset: slot=%d, hash=%x, error=%v",
					offset.BlockSlot,
					offset.BlockHash[:8],
					err,
				)
				readTxn.Rollback() //nolint:errcheck
				continue
			}

			t.Logf("Got block: %d bytes", len(blockCbor))

			// Extract the UTxO CBOR
			end := uint64(offset.ByteOffset) + uint64(offset.ByteLength)
			if end > uint64(len(blockCbor)) {
				t.Errorf(
					"Offset out of bounds: offset=%d, length=%d, block_size=%d",
					offset.ByteOffset,
					offset.ByteLength,
					len(blockCbor),
				)
			} else {
				utxoCbor := blockCbor[offset.ByteOffset:end]
				t.Logf("Extracted UTxO CBOR: %d bytes, first bytes: %s",
					len(utxoCbor), hex.EncodeToString(utxoCbor[:min(20, len(utxoCbor))]))
			}
		} else {
			t.Logf("Data is raw CBOR (legacy format)")
		}

		readTxn.Rollback() //nolint:errcheck
	}
}

// TestCreateGenesisBlockSkipsUtxoInsertionAfterMithrilBootstrap is the
// regression test: after a Mithril bootstrap,
// createGenesisBlock unconditionally recreated every Byron/Shelley genesis
// UTxO as a live row, without checking whether the imported ledger snapshot
// already reflects that output as spent. Found via cmd/node-parity against
// a real cardano-node: genesis-declared funds that the real chain spent long
// ago reappeared as live in dingo's answer, byte-for-byte matching the raw
// genesis declaration.
//
// Simulates the bootstrap shape the same way
// TestCreateGenesisBlockBackfillsMissingNetworkState does: a currentTip past
// slot 0 with no genesis CBOR yet stored, which is exactly the condition
// createGenesisBlock's own comment attributes to "after Mithril bootstrap
// which imports ledger state and ImmutableDB blocks but does not create the
// synthetic genesis block." Proves createGenesisBlock no longer inserts a
// live row for a real preview genesis UTxO on that path, while still
// creating the synthetic genesis block CBOR structurally (other code, e.g.
// this same function's own HasGenesisCbor short-circuit, depends on it
// existing).
func TestCreateGenesisBlockSkipsUtxoInsertionAfterMithrilBootstrap(
	t *testing.T,
) {
	t.Parallel()

	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })

	nodeCfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"preview/config.json",
		"preview",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)

	byronGenesisUtxos, err := nodeCfg.ByronGenesis().GenesisUtxos()
	require.NoError(t, err)
	require.NotEmpty(
		t, byronGenesisUtxos,
		"preview config must declare at least one Byron genesis UTxO "+
			"for this test to be meaningful",
	)
	sample := byronGenesisUtxos[0]
	sampleTxId := sample.Id.Id().Bytes()
	sampleOutputIdx := sample.Id.Index()

	genesisHash, err := GenesisBlockHash(nodeCfg)
	require.NoError(t, err)

	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{
			Database:          db,
			CardanoNodeConfig: nodeCfg,
			Logger: slog.New(
				slog.NewTextHandler(io.Discard, nil),
			),
		},
	}
	// No genesis CBOR pre-seeded (unlike the normal from-genesis case),
	// and currentTip already past slot 0: this is exactly the shape
	// createGenesisBlock's own comment attributes to a fresh Mithril
	// bootstrap, before it has ever run.
	ls.currentTip.Point.Slot = 42
	require.NoError(t, ls.createGenesisBlock())

	require.True(
		t, db.HasGenesisCbor(0, genesisHash[:]),
		"the synthetic genesis block CBOR must still be created "+
			"structurally, even though its UTxOs are not inserted as live",
	)

	exists, err := db.UtxoExists(sampleTxId, sampleOutputIdx, nil)
	require.NoError(t, err)
	require.False(
		t, exists,
		"a genesis UTxO must not be (re-)inserted as a live row after a "+
			"Mithril bootstrap -- the imported ledger snapshot is the only "+
			"authority on whether it is still live or was already spent "+
			"before the bootstrap point",
	)

	// Re-running (as a real startup would on every restart) must remain
	// idempotent and continue to skip insertion.
	require.NoError(t, ls.createGenesisBlock())
	exists, err = db.UtxoExists(sampleTxId, sampleOutputIdx, nil)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestNodeLocalEnactmentWriteErrorAbortsBoundary(t *testing.T) {
	t.Parallel()

	f := newTreasuryRolloverFixture(t, 100)
	withdrawAddress, returnAddress, stakeCredential := f.rewardAddress(t, 0xa1)
	proposal := f.addProposal(
		t,
		0xa2,
		501,
		map[*lcommon.Address]uint64{withdrawAddress: 40},
		returnAddress,
		0,
		true,
	)
	before := f.proposal(t, proposal)
	require.NotNil(t, before.RatifiedSlot)
	originalRatifiedSlot := *before.RatifiedSlot

	raw, err := dbtest.RawSQLiteMetadata(t, f.db)
	require.NoError(t, err)
	_, err = raw.Exec(`
CREATE TRIGGER fail_governance_enact
BEFORE UPDATE OF enacted_slot ON governance_proposal
WHEN NEW.enacted_slot IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'injected enactment write failure');
END`)
	require.NoError(t, err)

	txn := f.db.Transaction(true)
	err = txn.Do(func(txn *database.Txn) error {
		_, rolloverErr := f.ls.processEpochRollover(
			context.Background(),
			txn,
			f.currentEpoch,
			eras.ConwayEraDesc,
			f.currentPParams,
			false,
		)
		return rolloverErr
	})
	assert.Error(t, err, "a storage error must abort the boundary transaction")

	after := f.proposal(t, proposal)
	require.NotNil(t, after.RatifiedSlot)
	assert.Equal(
		t,
		originalRatifiedSlot,
		*after.RatifiedSlot,
		"an aborted boundary must preserve the earlier ratification marker",
	)
	assert.Nil(t, after.EnactedSlot)
	assert.Zero(t, f.accountReward(t, stakeCredential))
	treasury, _, _ := networkState(t, f.db)
	assert.Equal(t, uint64(100), treasury)
	advancedEpoch, epochErr := f.db.Metadata().GetEpoch(
		f.currentEpoch.EpochId+1,
		nil,
	)
	require.NoError(t, epochErr)
	assert.Nil(t, advancedEpoch)
}

func (f *treasuryRolloverFixture) rewardAddress(
	t *testing.T,
	marker byte,
) (*lcommon.Address, []byte, []byte) {
	t.Helper()
	stakeCredential := repeatByte(28, marker)
	address, err := lcommon.NewAddressFromParts(
		lcommon.AddressTypeNoneKey,
		lcommon.AddressNetworkTestnet,
		nil,
		stakeCredential,
	)
	require.NoError(t, err)
	addressBytes, err := address.Bytes()
	require.NoError(t, err)
	require.NoError(t, f.db.CreateAccount(nil, &models.Account{
		StakingKey: stakeCredential,
		Reward:     types.Uint64(0),
		Active:     true,
	}))
	return &address, addressBytes, stakeCredential
}

func (f *treasuryRolloverFixture) addProposal(
	t *testing.T,
	marker byte,
	addedSlot uint64,
	withdrawals map[*lcommon.Address]uint64,
	returnAddress []byte,
	deposit uint64,
	ratified bool,
) *models.GovernanceProposal {
	t.Helper()
	actionCbor, err := cbor.Encode(&lcommon.TreasuryWithdrawalGovAction{
		Type:        2,
		Withdrawals: withdrawals,
	})
	require.NoError(t, err)
	proposal := &models.GovernanceProposal{
		TxHash:        repeatByte(32, marker),
		ActionIndex:   0,
		ActionType:    uint8(lcommon.GovActionTypeTreasuryWithdrawal),
		ProposedEpoch: f.currentEpoch.EpochId,
		ExpiresEpoch:  f.currentEpoch.EpochId + 20,
		AnchorURL:     "https://example.invalid/treasury-withdrawal",
		AnchorHash:    repeatByte(32, marker+1),
		Deposit:       deposit,
		ReturnAddress: returnAddress,
		GovActionCbor: actionCbor,
		AddedSlot:     addedSlot,
	}
	if ratified {
		ratifiedEpoch := f.currentEpoch.EpochId
		ratifiedSlot := f.currentEpoch.StartSlot + 50
		proposal.RatifiedEpoch = &ratifiedEpoch
		proposal.RatifiedSlot = &ratifiedSlot
	}
	require.NoError(t, f.db.SetGovernanceProposal(proposal, nil))
	require.NoError(t, f.db.SetGovernanceVote(&models.GovernanceVote{
		ProposalID:      proposal.ID,
		VoterType:       models.VoterTypeCC,
		VoterCredential: f.hotCredential,
		Vote:            models.VoteYes,
		AddedSlot:       addedSlot + 1,
	}, nil))
	return proposal
}

func (f *treasuryRolloverFixture) accountReward(
	t *testing.T,
	stakeCredential []byte,
) uint64 {
	t.Helper()
	account, err := f.db.GetAccountByCredential(
		0,
		stakeCredential,
		false,
		nil,
	)
	require.NoError(t, err)
	require.NotNil(t, account)
	return uint64(account.Reward)
}

func TestProcessEpochRolloverReplayEnactmentFailureRemainsFatal(
	t *testing.T,
) {
	t.Parallel()

	f := newTreasuryRolloverFixture(t, 100)
	withdrawAddress, _, stakeCredential := f.rewardAddress(t, 0x81)
	proposal := f.addProposal(
		t, 0x82, 501,
		map[*lcommon.Address]uint64{withdrawAddress: 40},
		[]byte{0xff}, 1, true,
	)
	enactedEpoch := f.currentEpoch.EpochId + 1
	enactedSlot := f.currentEpoch.StartSlot +
		uint64(f.currentEpoch.LengthInSlots)
	proposal.EnactedEpoch = &enactedEpoch
	proposal.EnactedSlot = &enactedSlot
	require.NoError(t, f.db.SetGovernanceProposal(proposal, nil))

	txn := f.db.Transaction(true)
	err := txn.Do(func(txn *database.Txn) error {
		_, rolloverErr := f.ls.processEpochRollover(
			context.Background(),
			txn,
			f.currentEpoch,
			eras.ConwayEraDesc,
			f.currentPParams,
			false,
		)
		return rolloverErr
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "replay enacted proposal")
	assert.Zero(t, f.accountReward(t, stakeCredential))
	treasury, _, _ := networkState(t, f.db)
	assert.Equal(t, uint64(100), treasury)
	newEpoch, err := f.db.Metadata().GetEpoch(enactedEpoch, nil)
	require.NoError(t, err)
	assert.Nil(t, newEpoch)
}

// announcingMockHeader is a ranking-block header that carries a Leios
// endorser-block announcement, as a Dijkstra-era header does.
type announcingMockHeader struct {
	mockHeader
	ebHash lcommon.Blake2b256
	ebSize uint64
}

// headerStreamFixture is a LedgerState whose chain publishes onto a real event
// bus, so tests can observe the ordered chain.header stream the Leios vote
// manager consumes.
type headerStreamFixture struct {
	ls     *LedgerState
	bus    *event.EventBus
	connId ouroboros.ConnectionId
	ch     <-chan event.Event
}

func newHeaderStreamLedger(t *testing.T) *headerStreamFixture {
	t.Helper()
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(bus.Stop)
	cm, err := chain.NewManager(nil, bus)
	require.NoError(t, err)
	subId, ch := bus.Subscribe(chain.ChainHeaderEventType)
	t.Cleanup(func() { bus.Unsubscribe(chain.ChainHeaderEventType, subId) })
	ls := &LedgerState{
		chain: cm.PrimaryChain(),
		config: LedgerStateConfig{
			EventBus: bus,
			Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	}
	return &headerStreamFixture{
		ls:  ls,
		bus: bus,
		connId: ouroboros.ConnectionId{
			LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
			RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
		},
		ch: ch,
	}
}

func announcingHeader(
	slot uint64,
	name string,
	prevHash lcommon.Blake2b256,
	blockNumber uint64,
	ebHash lcommon.Blake2b256,
) announcingMockHeader {
	return announcingMockHeader{
		mockHeader: mockHeader{
			hash:        lcommon.NewBlake2b256([]byte(name)),
			prevHash:    prevHash,
			blockNumber: blockNumber,
			slot:        slot,
		},
		ebHash: ebHash,
		ebSize: 4096,
	}
}

// TestChainsyncHeaderAdmissionAnnouncesOnlyWhenCryptoVerified pins the ledger
// half of the crypto gate on the header stream.
//
// chainsyncHeaderCryptoPolicy admits a roll-forward header without verifying
// its VRF/KES on two paths: a slot covered by an imported Mithril snapshot and
// no cached epoch nonce for the slot (verification deferred to blockfetch).
// Both reach
// chain.AddBlockHeader, not AddVerifiedBlockHeader. Announcing such a header
// would let a chainsync peer make this node sign and publish a BLS vote for a
// ranking block it never authenticated, taking the (slot, voterId) pair the
// honest block's own vote needs.
func TestChainsyncHeaderAdmissionAnnouncesOnlyWhenCryptoVerified(
	t *testing.T,
) {
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	header := announcingHeader(
		577, "hdr-1", lcommon.NewBlake2b256(nil), 1, ebHash,
	)
	point := ocommon.NewPoint(header.slot, header.hash.Bytes())

	// The fixture has no cached epoch nonce, so the policy defers verification
	// and the handler takes the AddBlockHeader branch -- the real end-to-end
	// unverified admission.
	t.Run("unverified admission is queued, not announced", func(t *testing.T) {
		fixture := newHeaderStreamLedger(t)
		verifyNow, trusted := fixture.ls.chainsyncHeaderCryptoPolicy(
			header.slot,
		)
		require.False(t, verifyNow, "fixture must exercise the unverified path")
		require.False(t, trusted)

		require.NoError(
			t,
			fixture.ls.handleEventChainsyncBlockHeader(ChainsyncEvent{
				ConnectionId: fixture.connId,
				BlockHeader:  header,
				Point:        point,
				Tip: ochainsync.Tip{
					Point:       ocommon.NewPoint(60001, []byte("tip-1")),
					BlockNumber: 60001,
				},
			}),
		)
		require.Equal(t, 1, fixture.ls.chain.HeaderCount())

		testutil.RequireNoReceive(
			t,
			fixture.ch,
			500*time.Millisecond,
			"an unverified header must not arm a vote",
		)
	})

	// The verified branch of the same handler is one call:
	// ls.chain.AddVerifiedBlockHeader(e.BlockHeader). It is driven directly
	// here because a header that both announces a Leios endorser block and
	// passes real VRF/KES cannot be synthesized in this suite: gouroboros
	// VerifyBlock dispatches on the concrete era header type, so wrapping a
	// valid Babbage header to add LeiosAnnouncement fails verification with
	// "unsupported block type for VRF verification". The gate itself is
	// covered from both sides in chain:
	// TestHeaderAnnouncementRequiresCryptoVerifiedHeader.
	t.Run("verified admission announces", func(t *testing.T) {
		fixture := newHeaderStreamLedger(t)
		require.NoError(t, fixture.ls.chain.AddVerifiedBlockHeader(header))
		fixture.ls.chain.PublishPendingChainUpdates()

		evt := testutil.RequireReceive(
			t,
			fixture.ch,
			testutil.AsyncWait,
			"announcement published from verified header admission",
		)
		data, ok := evt.Data.(chain.ChainHeaderAnnouncementEvent)
		require.True(t, ok)
		assert.Equal(t, uint64(577), data.Slot)
		assert.Equal(t, header.hash, data.RbHash)
		assert.Equal(t, ebHash, data.EbHash)
		assert.NotZero(t, data.Seq)
	})
}

// TestForkResolutionAnnouncesOnlyTheVerifiedIncomingHeader covers the second
// way an announcing header reaches the header queue. A header that does not
// fit the current tip is queued by tryResolveFork rather than by the direct
// admission path, and that branch returns before the caller's ordinary
// bookkeeping runs. Emitting from the chain's own header-queue mutation is
// what keeps this path covered.
//
// tryResolveFork re-queues a whole fork path: the header this event delivered,
// plus earlier headers replayed from recorded peer history. Only the delivered
// one carries a crypto verdict, so only it may be admitted verified, and only
// a verified admission announces (see addForkPathHeader). Both halves are
// asserted here at that composition site.
func TestForkResolutionAnnouncesOnlyTheVerifiedIncomingHeader(t *testing.T) {
	for _, tc := range []struct {
		name           string
		cryptoVerified bool
		wantAnnounce   bool
	}{
		{
			name:           "verified incoming header announces",
			cryptoVerified: true,
			wantAnnounce:   true,
		},
		{
			name:           "unverified incoming header does not announce",
			cryptoVerified: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bus := event.NewEventBus(nil, nil)
			t.Cleanup(bus.Stop)
			fixture := newChainsyncRollbackFixtureWithBus(t, bus)
			subId, ch := bus.Subscribe(chain.ChainHeaderEventType)
			defer bus.Unsubscribe(chain.ChainHeaderEventType, subId)
			// Keep the test at header admission; no blockfetch worker is
			// needed.
			fixture.ls.chainsyncBlockfetchReadyChan = make(chan struct{})

			ebHash := lcommon.NewBlake2b256([]byte("fork-announced-eb"))
			header := announcingHeader(
				fixture.currentTip.Point.Slot+10,
				"fork-announcing-header",
				lcommon.NewBlake2b256(fixture.ancestorTip.Point.Hash),
				fixture.ancestorTip.BlockNumber+1,
				ebHash,
			)
			// The header does not fit the current tip; that failure is the
			// condition tryResolveFork exists to handle.
			var notFitErr chain.BlockNotFitChainTipError
			require.ErrorAs(
				t,
				fixture.ls.chain.AddBlockHeader(header),
				&notFitErr,
			)
			advertisedSlot := ^uint64(0)

			resolved, err := fixture.ls.tryResolveFork(
				ChainsyncEvent{
					ConnectionId: fixture.connId,
					Point: ocommon.NewPoint(
						header.slot,
						header.hash.Bytes(),
					),
					BlockHeader: header,
					Tip: ochainsync.Tip{
						Point: ocommon.NewPoint(
							advertisedSlot,
							[]byte("unbound-fork-tip"),
						),
						BlockNumber: advertisedSlot,
					},
				},
				notFitErr,
				nil,
				tc.cryptoVerified,
			)
			require.NoError(t, err)
			require.True(t, resolved)
			// The header was queued through fork resolution, not direct
			// admission.
			require.Equal(t, fixture.ancestorTip, fixture.ls.chain.Tip())
			require.Equal(t, 1, fixture.ls.chain.HeaderCount())
			fixture.ls.chain.PublishPendingChainUpdates()

			// The rollback's invalidation precedes anything the fork
			// resolution queued after it.
			invalidation := testutil.RequireReceive(
				t, ch, testutil.AsyncWait, "rollback invalidation",
			)
			invalid, ok := invalidation.Data.(chain.ChainHeaderInvalidationEvent)
			require.True(t, ok, "got %T", invalidation.Data)
			assert.Equal(t, chain.HeaderInvalidationRollback, invalid.Reason)

			if !tc.wantAnnounce {
				testutil.RequireNoReceive(
					t,
					ch,
					500*time.Millisecond,
					"an unverified fork header must not arm a vote",
				)
				return
			}

			announcement := testutil.RequireReceive(
				t, ch, testutil.AsyncWait, "announcement from fork resolution",
			)
			announced, ok := announcement.Data.(chain.ChainHeaderAnnouncementEvent)
			require.True(t, ok, "got %T", announcement.Data)
			assert.Equal(t, header.hash, announced.RbHash)
			assert.Equal(t, ebHash, announced.EbHash)
			assert.Greater(
				t,
				announced.Seq,
				invalid.Seq,
				"the fork header is admitted after the rollback that made room for it",
			)
			testutil.RequireNoReceive(
				t,
				ch,
				300*time.Millisecond,
				"the incoming fork header must be announced exactly once",
			)
		})
	}
}

// newChainsyncRollbackFixtureWithBus mirrors newChainsyncRollbackFixture but
// gives the chain a real event bus so its deferred header/rollback events can
// be observed.
func newChainsyncRollbackFixtureWithBus(
	t *testing.T,
	bus *event.EventBus,
) *chainsyncRollbackFixture {
	t.Helper()

	db := newTestDB(t)
	cm, err := chain.NewManager(db, bus)
	require.NoError(t, err)
	require.NoError(
		t,
		cm.SetLedger(testSecurityParamLedger{securityParam: 2}),
	)

	ancestorHash := testHashBytes("ancestor-block")
	currentHash := testHashBytes("current-block")
	ancestorBlock := chain.RawBlock{
		Slot:        10,
		Hash:        ancestorHash,
		BlockNumber: 1,
		Type:        1,
		Cbor:        []byte{0x80},
	}
	currentBlock := chain.RawBlock{
		Slot:        20,
		Hash:        currentHash,
		BlockNumber: 2,
		Type:        1,
		PrevHash:    ancestorHash,
		Cbor:        []byte{0x80},
	}
	require.NoError(
		t,
		cm.PrimaryChain().AddRawBlocks([]chain.RawBlock{
			ancestorBlock,
			currentBlock,
		}),
	)

	ls, err := NewLedgerState(
		LedgerStateConfig{
			Database:          db,
			ChainManager:      cm,
			CardanoNodeConfig: newTestShelleyGenesisCfg(t),
			Logger:            slog.New(slog.NewJSONHandler(io.Discard, nil)),
		},
	)
	require.NoError(t, err)
	ls.metrics.init(prometheus.NewRegistry())
	// Attached after construction so NewLedgerState does not register the
	// node-level subscribers this focused test does not want.
	ls.config.EventBus = bus

	ancestorTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(ancestorBlock.Slot, ancestorBlock.Hash),
		BlockNumber: ancestorBlock.BlockNumber,
	}
	currentTip := ochainsync.Tip{
		Point:       ocommon.NewPoint(currentBlock.Slot, currentBlock.Hash),
		BlockNumber: currentBlock.BlockNumber,
	}
	ancestorNonce := []byte("nonce-ancestor")
	currentNonce := []byte("nonce-current")
	require.NoError(t, db.SetBlockNonce(
		ancestorTip.Point.Hash,
		ancestorTip.Point.Slot,
		ancestorNonce,
		true,
		nil,
	))
	require.NoError(t, db.SetBlockNonce(
		currentTip.Point.Hash, currentTip.Point.Slot, currentNonce, false, nil,
	))
	require.NoError(t, db.SetTip(currentTip, nil))

	ls.currentTip = currentTip
	ls.currentTipBlockNonce = append([]byte(nil), currentNonce...)
	ls.chainsyncState = SyncingChainsyncState
	ls.publishSnapshotsLocked()

	return &chainsyncRollbackFixture{
		ls:          ls,
		ancestorTip: ancestorTip,
		currentTip:  currentTip,
		connId: ouroboros.ConnectionId{
			LocalAddr:  &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 6000},
			RemoteAddr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 3001},
		},
		ancestorNonce: ancestorNonce,
	}
}

// TestConnectionClosedPublishesHeaderInvalidation covers a header-queue
// discard on a peer-stall path. When the connection that owned the header
// pipeline closes, the queue is discarded and Chain.ClearHeaders enqueues the
// invalidation on the chain-level sequencer -- but this handler previously
// registered no drain, so it sat there until some unrelated handler ran. A
// dead peer is exactly the case where no further event is guaranteed, so the
// announcement would stay armed well past the ten-slot vote window.
func TestConnectionClosedPublishesHeaderInvalidation(t *testing.T) {
	fixture := newHeaderStreamLedger(t)
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	header := announcingHeader(
		577, "hdr-1", lcommon.NewBlake2b256(nil), 1, ebHash,
	)
	require.NoError(t, fixture.ls.chain.AddVerifiedBlockHeader(header))
	fixture.ls.headerPipelineConnId = fixture.connId
	require.Equal(t, 1, fixture.ls.chain.HeaderCount())

	// The announcement is still undrained on the sequencer; the closed
	// connection must publish it and the invalidation that voids it.
	fixture.ls.handleConnectionClosedEvent(event.NewEvent(
		ConnectionClosedEventType,
		ConnectionClosedEvent{ConnectionId: fixture.connId},
	))
	assert.Zero(t, fixture.ls.chain.HeaderCount())

	announcement := testutil.RequireReceive(
		t, fixture.ch, testutil.AsyncWait, "announcement",
	)
	announced, ok := announcement.Data.(chain.ChainHeaderAnnouncementEvent)
	require.True(t, ok, "got %T", announcement.Data)
	assert.Equal(t, header.hash, announced.RbHash)

	invalidation := testutil.RequireReceive(
		t,
		fixture.ch,
		testutil.AsyncWait,
		"invalidation published without any later event",
	)
	invalid, ok := invalidation.Data.(chain.ChainHeaderInvalidationEvent)
	require.True(t, ok, "got %T", invalidation.Data)
	assert.Equal(t, chain.HeaderInvalidationQueueCleared, invalid.Reason)
	assert.Contains(t, invalid.RbHashes, header.hash)
	assert.Greater(t, invalid.Seq, announced.Seq)
}

// TestBlockfetchTimeoutDrainsHeaderSequencer covers the other peer-stall path.
// The timeout handler tears the batch down and clears the header queue, and it
// is the last thing that runs for a peer that stopped sending, so it has to
// drain the sequencer rather than leave header events queued behind it.
func TestBlockfetchTimeoutDrainsHeaderSequencer(t *testing.T) {
	fixture := newHeaderStreamLedger(t)
	ebHash := lcommon.NewBlake2b256([]byte("announced-eb"))
	header := announcingHeader(
		577, "hdr-1", lcommon.NewBlake2b256(nil), 1, ebHash,
	)
	require.NoError(t, fixture.ls.chain.AddVerifiedBlockHeader(header))

	var pending pendingPublishes
	func() {
		defer pending.flush()
		fixture.ls.chainsyncBlockfetchMutex.Lock()
		defer fixture.ls.chainsyncBlockfetchMutex.Unlock()
		fixture.ls.handleBlockfetchTimeoutLocked(fixture.connId, &pending)
	}()

	evt := testutil.RequireReceive(
		t,
		fixture.ch,
		testutil.AsyncWait,
		"header events published without any later event",
	)
	announced, ok := evt.Data.(chain.ChainHeaderAnnouncementEvent)
	require.True(t, ok, "got %T", evt.Data)
	assert.Equal(t, header.hash, announced.RbHash)
}

func newDonationTestDB(t *testing.T) *database.Database {
	t.Helper()
	db, err := dbtest.NewDatabase(t, &database.Config{
		DataDir: "",
	})
	require.NoError(t, err)
	t.Cleanup(func() { dbtest.CloseDatabase(db) }) //nolint:errcheck
	return db
}

func networkState(
	t *testing.T,
	db *database.Database,
) (treasury, reserves, slot uint64) {
	t.Helper()
	state, err := db.Metadata().GetNetworkState(nil)
	require.NoError(t, err)
	require.NotNil(t, state)
	return uint64(state.Treasury), uint64(state.Reserves), state.Slot
}

// TestApplyEpochDonations verifies that the ending epoch's donations are added
// to the treasury at the boundary slot, leaving reserves untouched, and that
// only the ended epoch's donations are moved.
func TestApplyEpochDonations(t *testing.T) {
	t.Parallel()

	db := newDonationTestDB(t)
	ls := &LedgerState{db: db}

	// Post-withdrawal treasury/reserves baseline at an earlier slot.
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))
	// Donations for the ending epoch (7) and a later epoch (8) that must not move.
	require.NoError(t, db.Metadata().AddNetworkDonation(60, 7, 100, nil))
	require.NoError(t, db.Metadata().AddNetworkDonation(70, 7, 200, nil))
	require.NoError(t, db.Metadata().AddNetworkDonation(600, 8, 999, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyEpochDonations(txn, 7, 80)
	}))

	treasury, reserves, slot := networkState(t, db)
	assert.Equal(
		t,
		uint64(1_300),
		treasury,
		"treasury += epoch-7 donations (100+200)",
	)
	assert.Equal(t, uint64(5_000), reserves, "reserves untouched by donations")
	assert.Equal(t, uint64(80), slot, "updated at the boundary slot")
}

// TestApplyEpochDonations_NoDonations is a no-op when the ended epoch had none.
func TestApplyEpochDonations_NoDonations(t *testing.T) {
	t.Parallel()

	db := newDonationTestDB(t)
	ls := &LedgerState{db: db}
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))

	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyEpochDonations(txn, 7, 80)
	}))

	treasury, reserves, slot := networkState(t, db)
	assert.Equal(t, uint64(1_000), treasury)
	assert.Equal(t, uint64(5_000), reserves)
	assert.Equal(
		t,
		uint64(50),
		slot,
		"no boundary row written when no donations",
	)
}

// TestEpochDonationWithdrawalRollback exercises the acceptance scenario: a
// treasury withdrawal (modelled as a debited treasury) followed by a donation
// at the boundary, then a rollback past the boundary that restores the prior
// treasury and drops the donation rows so re-application is deterministic.
func TestEpochDonationWithdrawalRollback(t *testing.T) {
	t.Parallel()

	db := newDonationTestDB(t)
	ls := &LedgerState{db: db}

	// Epoch 7 starts with treasury 1_000 (slot 50).
	require.NoError(t, db.Metadata().SetNetworkState(1_000, 5_000, 50, nil))
	// A donation block lands mid-epoch.
	require.NoError(t, db.Metadata().AddNetworkDonation(70, 7, 300, nil))
	// At the 7->8 boundary (slot 80) a treasury withdrawal of 400 is enacted
	// first (checked against the pre-donation treasury of 1_000), debiting the
	// treasury to 600...
	require.NoError(t, db.Metadata().SetNetworkState(600, 5_000, 80, nil))
	// ...then the epoch's donations are added on top.
	txn := db.Transaction(true)
	require.NoError(t, txn.Do(func(txn *database.Txn) error {
		return ls.applyEpochDonations(txn, 7, 80)
	}))
	treasury, reserves, slot := networkState(t, db)
	require.Equal(
		t,
		uint64(900),
		treasury,
		"1_000 - 400 withdrawal + 300 donation",
	)
	require.Equal(t, uint64(5_000), reserves)
	require.Equal(t, uint64(80), slot)

	// Roll back past the boundary (to slot 60): the boundary NetworkState row
	// and the donation row are dropped, restoring epoch 7's starting treasury.
	require.NoError(t, db.DeleteNetworkStateAfterSlot(60, nil))
	require.NoError(t, db.DeleteNetworkDonationsAfterSlot(60, nil))

	treasury, reserves, slot = networkState(t, db)
	assert.Equal(
		t,
		uint64(1_000),
		treasury,
		"treasury restored to pre-boundary value",
	)
	assert.Equal(t, uint64(5_000), reserves)
	assert.Equal(t, uint64(50), slot)
	sum, err := db.Metadata().SumNetworkDonationsForEpoch(7, nil)
	require.NoError(t, err)
	assert.Zero(t, sum, "rolled-back donation rows are gone")
}

// TestRollbackIsAppliableRejectsBelowConsumedUtxoPruneFloor keeps the loop
// detector's crossability predicate in step with the rollback it predicts.
// Reporting a target below the prune floor as crossable would make the detector
// insist on applying a rollback rollbackChainAndStateDeferred refuses.
func TestRollbackIsAppliableRejectsBelowConsumedUtxoPruneFloor(t *testing.T) {
	f := newPrunedUtxoFixture(t, 0)
	f.ls.cleanupConsumedUtxos()

	require.True(
		t,
		f.ls.rollbackIsAppliable(
			ocommon.NewPoint(
				pruneFixtureFloorSlot,
				testHashBytes("3766-floor"),
			),
		),
		"a target at the prune floor is still crossable",
	)
	require.False(
		t,
		f.ls.rollbackIsAppliable(
			ocommon.NewPoint(
				pruneFixtureDeepRewindSlot,
				testHashBytes("3766-deep"),
			),
		),
		"a target below the prune floor cannot be crossed",
	)
}

// TestHandleEventChainsyncRollbackRejectsBelowPruneFloor pins that a peer
// rollback the prune floor refuses is handled as peer divergence, not as a
// local fault. handleEventChainsync routes any error returned by the rollback
// handler to FatalErrorFunc, so returning one here would let a peer's choice of
// rollback point terminate the node. The handler must instead reject the peer
// chain and ask for a fresh intersection, exactly as the Mithril boundary does.
func TestHandleEventChainsyncRollbackRejectsBelowPruneFloor(t *testing.T) {
	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	// A floor above the rollback target, as a sweep at a higher tip would
	// have left behind.
	require.NoError(t, fixture.ls.db.SetSyncState(
		database.ConsumedUtxoPruneFloorSyncKey,
		strconv.FormatUint(fixture.currentTip.Point.Slot, 10),
		nil,
	))

	fatalCalls := 0
	fixture.ls.config.FatalErrorFunc = func(error) { fatalCalls++ }

	resyncCh := make(chan event.ChainsyncResyncEvent, 1)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case resyncCh <- e:
			default:
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	err := fixture.ls.handleEventChainsyncRollback(
		ChainsyncEvent{
			ConnectionId: fixture.connId,
			Point:        fixture.ancestorTip.Point,
		},
		nil,
	)
	require.NoError(
		t,
		err,
		"a refused rollback must not surface as an error the fatal path acts on",
	)
	require.Zero(t, fatalCalls)

	// Neither side moved: the chain was not truncated and the ledger tip
	// stands.
	require.Equal(t, fixture.currentTip, fixture.ls.chain.Tip())
	require.Equal(t, fixture.currentTip, fixture.ls.currentTip)

	e := testutil.RequireReceive(
		t,
		resyncCh,
		time.Second,
		"expected prune-floor rollback resync event",
	)
	require.Equal(
		t,
		event.ChainsyncResyncReasonRollbackBelowUtxoPruneFloor,
		e.Reason,
	)
	require.Equal(t, fixture.connId, e.ConnectionId)
}

func TestCreateGenesisBlockPreservesPoolDeposit(t *testing.T) {
	t.Parallel()
	db, err := dbtest.NewDatabase(t, &database.Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dbtest.CloseDatabase(db)) })
	cfg, err := cardano.LoadCardanoNodeConfigWithFallback(
		"devnet/config.json",
		"devnet",
		cardano.EmbeddedConfigFS,
	)
	require.NoError(t, err)
	cfg.ShelleyGenesis().ProtocolParameters.PoolDeposit = 500_000_000

	pools, _, err := cfg.ShelleyGenesis().InitialPools()
	require.NoError(t, err)
	require.NotEmpty(t, pools)
	ls := &LedgerState{
		db: db,
		config: LedgerStateConfig{Database: db, CardanoNodeConfig: cfg,
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
	}
	require.NoError(t, ls.createGenesisBlock())
	for key := range pools {
		hash, err := hex.DecodeString(key)
		require.NoError(t, err)
		pool, err := db.GetPool(lcommon.PoolKeyHash(hash), true, nil)
		require.NoError(t, err)
		require.NotEmpty(t, pool.Registration)
		require.Equal(
			t,
			uint64(cfg.ShelleyGenesis().ProtocolParameters.PoolDeposit),
			uint64(pool.Registration[0].DepositAmount),
		)
	}
}

// A peer repeating a rollback to our own tip must cost constant work: the
// no-op is recognised before any rollback history is recorded.
func TestHandleEventChainsyncRollbackToCurrentTipRecordsNoHistory(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	tipPoint := fixture.ls.chain.HeaderTip().Point

	for range 3 * rollbackLoopThreshold {
		require.NoError(t, fixture.ls.handleEventChainsyncRollback(
			ChainsyncEvent{
				ConnectionId: fixture.connId,
				Point:        tipPoint,
			},
			nil,
		))
	}

	assert.Equal(
		t,
		0,
		len(fixture.ls.rollbackHistory),
		"a rollback to the current tip must not be recorded",
	)
}

func TestRecordRollbackBoundsHistory(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	now := time.Now()
	for i := range 4 * maxRollbackHistory {
		ls.recordRollback(
			"conn",
			ocommon.NewPoint(uint64(i), []byte{byte(i), byte(i >> 8)}),
			now,
		)
	}

	assert.Equal(t, maxRollbackHistory, len(ls.rollbackHistory))
	assert.Equal(
		t,
		uint64(4*maxRollbackHistory-1),
		ls.rollbackHistory[len(ls.rollbackHistory)-1].point.Slot,
		"the newest record must be retained",
	)
}

func TestRecordRollbackCountsRepeatAfterHistoryEviction(t *testing.T) {
	t.Parallel()
	ls := &LedgerState{}
	now := time.Now()
	point := ocommon.NewPoint(7, []byte("repeated"))

	assert.Equal(t, 1, ls.recordRollback("conn", point, now))
	for i := range maxRollbackHistory {
		ls.recordRollback(
			"conn",
			ocommon.NewPoint(
				uint64(100+i),
				[]byte{byte(i), byte(i >> 8)},
			),
			now,
		)
	}
	assert.Equal(t, maxRollbackHistory, len(ls.rollbackHistory))
	assert.Equal(
		t,
		2,
		ls.recordRollback("conn", point, now),
		"the loop count survives eviction of its first event record",
	)
}

func TestRecordRollbackCountsRepeatsFromOneConnection(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{}
	now := time.Now()
	point := ocommon.NewPoint(7, []byte("p"))

	assert.Equal(t, 1, ls.recordRollback("a", point, now))
	assert.Equal(t, 1, ls.recordRollback("b", point, now))
	assert.Equal(t, 2, ls.recordRollback("a", point, now))
	assert.Equal(
		t,
		1,
		ls.recordRollback(
			"a",
			point,
			now.Add(rollbackLoopWindow+time.Second),
		),
		"records older than the detection window must be pruned",
	)
}

// One divergence episode on a connection delivers many headers that do not
// fit. They must produce one resync request, not one per header.
func TestHandleEventChainsyncBlockHeaderCoalescesResyncPerConnection(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus

	resyncCh := make(chan event.ChainsyncResyncEvent, 16)
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			if e, ok := evt.Data.(event.ChainsyncResyncEvent); ok {
				resyncCh <- e
			}
		},
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})

	for i := range 5 {
		header := mockHeader{
			hash: lcommon.NewBlake2b256(
				testHashBytes(fmt.Sprintf("stale-block-%d", i)),
			),
			prevHash: lcommon.NewBlake2b256(
				testHashBytes(fmt.Sprintf("missing-ancestor-%d", i)),
			),
			blockNumber: fixture.currentTip.BlockNumber + 1,
			slot:        fixture.currentTip.Point.Slot + 10 + uint64(i),
		}
		point := ocommon.NewPoint(header.SlotNumber(), header.Hash().Bytes())
		require.NoError(t, fixture.ls.handleEventChainsyncBlockHeader(
			ChainsyncEvent{
				ConnectionId: fixture.connId,
				Point:        point,
				BlockHeader:  header,
				Tip: ochainsync.Tip{
					Point:       point,
					BlockNumber: header.BlockNumber(),
				},
			},
		))
	}

	resync := testutil.RequireReceive(
		t,
		resyncCh,
		testutil.AsyncWait,
		"expected one chainsync resync event",
	)
	assert.Equal(t, fixture.connId, resync.ConnectionId)
	testutil.RequireNoReceive(
		t,
		resyncCh,
		200*time.Millisecond,
		"one divergence episode must publish a single resync",
	)
}

func TestRequestChainsyncResyncCoalescesPerConnectionWithinWindow(
	t *testing.T,
) {
	t.Parallel()

	fixture := newChainsyncRollbackFixture(t)
	bus := event.NewEventBus(nil, nil)
	t.Cleanup(func() { bus.Stop() })
	fixture.ls.config.EventBus = bus
	var published atomic.Int32
	subId := bus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(event.Event) { published.Add(1) },
	)
	t.Cleanup(func() {
		bus.Unsubscribe(event.ChainsyncResyncEventType, subId)
	})
	waitFor := func(want int32, msg string) {
		t.Helper()
		testutil.WaitForCondition(
			t,
			func() bool { return published.Load() == want },
			testutil.AsyncWait,
			msg,
		)
	}
	otherConn := ouroboros.ConnectionId{
		LocalAddr:  fixture.connId.LocalAddr,
		RemoteAddr: &net.TCPAddr{IP: net.IPv4(10, 9, 8, 7), Port: 3001},
	}

	fixture.ls.requestChainsyncResync(fixture.connId, "first", nil)
	fixture.ls.requestChainsyncResync(fixture.connId, "second", nil)
	fixture.ls.requestChainsyncResync(otherConn, "other peer", nil)
	waitFor(2, "one request per connection must be published")
	require.Never(
		t,
		func() bool { return published.Load() > 2 },
		200*time.Millisecond,
		10*time.Millisecond,
		"a coalesced request must not be published late",
	)

	fixture.ls.handleConnectionClosedEvent(event.Event{
		Type: ConnectionClosedEventType,
		Data: ConnectionClosedEvent{ConnectionId: fixture.connId},
	})
	fixture.ls.requestChainsyncResync(fixture.connId, "reconnected", nil)
	waitFor(3, "a reused connection tuple starts a new episode after close")

	// Once the window has passed the next divergence is a new episode.
	fixture.ls.resyncCoalesceMutex.Lock()
	fixture.ls.resyncCoalesce[connIdKey(fixture.connId)].at = time.Now().
		Add(-2 * chainsyncResyncCoalesceWindow)
	fixture.ls.resyncCoalesceMutex.Unlock()
	fixture.ls.requestChainsyncResync(fixture.connId, "next episode", nil)
	waitFor(4, "a request after the window must be published")
}
