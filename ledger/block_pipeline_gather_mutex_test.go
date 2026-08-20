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
	"context"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/require"
)

// pausingLedgerReadIterator is a ledgerReadIterator whose Next calls block
// waiting on resume immediately before returning the result at index
// pauseAtIndex, closing paused first so a test can observe that the call is
// in flight. It exists to put ledgerReadChainIterator's gather loop in a
// known, held-open state -- "already fetched some raw blocks, about to
// fetch/hand off more" -- so a concurrent goroutine can probe whether
// blockPipelineGatherMutex is held during that window.
type pausingLedgerReadIterator struct {
	results    []*chain.ChainIteratorResult
	pauseAtIdx int
	calls      int
	paused     chan struct{}
	resume     chan struct{}
}

func (p *pausingLedgerReadIterator) Next(
	blocking bool,
) (*chain.ChainIteratorResult, error) {
	idx := p.calls
	p.calls++
	if idx == p.pauseAtIdx {
		close(p.paused)
		<-p.resume
	}
	if idx < len(p.results) {
		return p.results[idx], nil
	}
	if !blocking {
		return nil, chain.ErrIteratorChainTip
	}
	// This test never exercises a genuinely blocking call; block briefly
	// rather than forever so a misuse fails fast instead of hanging.
	<-time.After(2 * time.Second)
	return nil, chain.ErrIteratorChainTip
}

// TestLedgerReadChainIteratorHoldsGatherMutexAcrossGather is a regression
// test for the cubic-flagged gap in issue #1894 phase 5's rollback
// coordination: drainBlockPipelineBeforeRollback only waits for work
// already Submitted to blockPipeline, so a rollback landing while
// ledgerReadChainIterator has already pulled raw blocks off the chain
// iterator into its local batch, but has not yet reached decodeReadChainBatch
// (Submit), would previously go unnoticed -- WaitForDrain sees nothing
// pending and returns immediately.
//
// blockPipelineGatherMutex closes that window by having the reader hold its
// read lock for the whole gather-then-submit span. This test proves the
// reader actually holds it there (not just after Submit): while the
// scripted iterator's second Next call is deliberately paused -- i.e. one
// raw block already gathered, mid-way through gathering the next -- a
// concurrent TryLock for the write side (the lock rollbackChainAndState
// takes) must fail. Once the reader delivers its batch and the mutex is no
// longer needed, TryLock must succeed.
func TestLedgerReadChainIteratorHoldsGatherMutexAcrossGather(t *testing.T) {
	block1, point1 := buildDecodableTestBlock(t, 10, 1)
	block2, point2 := buildDecodableTestBlock(t, 20, 2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	iter := &pausingLedgerReadIterator{
		results: []*chain.ChainIteratorResult{
			{Point: point1, Block: block1},
			{Point: point2, Block: block2},
		},
		// Pause immediately before the second Next call, i.e. after the
		// first raw block has already been appended to the reader's
		// local batch and it is about to fetch more.
		pauseAtIdx: 1,
		paused:     make(chan struct{}),
		resume:     make(chan struct{}),
	}

	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	resultCh := make(chan readChainResult)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		ls.ledgerReadChainIterator(ctx, iter, resultCh)
	}()

	testutil.RequireReceive(
		t, iter.paused, 2*time.Second,
		"reader never reached the paused mid-gather point",
	)

	// The reader is mid-gather with one raw block already collected. The
	// write-side lock rollbackChainAndState takes must not be obtainable
	// right now.
	require.False(
		t,
		ls.blockPipelineGatherMutex.TryLock(),
		"blockPipelineGatherMutex.Lock() succeeded while the reader was "+
			"mid-gather -- a concurrent rollback could truncate the chain "+
			"while stale raw blocks are still about to be submitted",
	)

	close(iter.resume)

	result := testutil.RequireReceive(
		t, resultCh, 2*time.Second,
		"reader never delivered its gathered batch",
	)
	require.False(t, result.rollback)
	require.Len(t, result.blocks, 2)

	// The gather-plus-submit span has ended; the write lock must now be
	// obtainable.
	require.Eventually(t, func() bool {
		if ls.blockPipelineGatherMutex.TryLock() {
			ls.blockPipelineGatherMutex.Unlock()
			return true
		}
		return false
	}, 2*time.Second, 5*time.Millisecond,
		"blockPipelineGatherMutex remained held after the batch was "+
			"delivered",
	)

	close(result.done)
	cancel()
	testutil.RequireReceive(
		t, readerDone, 2*time.Second,
		"ledgerReadChainIterator did not exit after cancellation",
	)
}
