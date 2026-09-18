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

// shrinkGatherCoalesceRetryInterval overrides the package-level
// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam for the
// duration of the calling test and restores it on cleanup, so these tests
// must not run in parallel with each other (see cleanup_consumed_utxos's
// shrinkCleanupConsumedUtxosInterval for the same pattern).
func shrinkGatherCoalesceRetryInterval(
	t *testing.T,
	interval time.Duration,
	attempts int,
) {
	t.Helper()
	prevInterval := gatherCoalesceRetryInterval
	prevAttempts := gatherCoalesceMaxAttempts
	gatherCoalesceRetryInterval = interval
	gatherCoalesceMaxAttempts = attempts
	t.Cleanup(func() {
		gatherCoalesceRetryInterval = prevInterval
		gatherCoalesceMaxAttempts = prevAttempts
	})
}

// scriptedGapLedgerReadIterator scripts a fixed sequence of non-blocking
// Next outcomes. A nil entry simulates the iterator momentarily having
// nothing ready (chain.ErrIteratorChainTip on a non-blocking probe) without
// the chain having actually stopped growing -- e.g. the goroutine that
// appends blocks to ls.chain is a beat behind this reader. Once the script
// is exhausted, a non-blocking call keeps returning ErrIteratorChainTip and
// a blocking call waits on ctx.Done(), matching a real iterator genuinely
// caught up to a still-open chain tip.
type scriptedGapLedgerReadIterator struct {
	ctx    context.Context
	script []*chain.ChainIteratorResult
	idx    int
}

func (s *scriptedGapLedgerReadIterator) Next(
	blocking bool,
) (*chain.ChainIteratorResult, error) {
	if s.idx < len(s.script) {
		next := s.script[s.idx]
		s.idx++
		if next == nil {
			return nil, chain.ErrIteratorChainTip
		}
		return next, nil
	}
	if !blocking {
		return nil, chain.ErrIteratorChainTip
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

// TestLedgerReadChainIteratorCoalescesGapsDuringBulkReplay is a regression
// test for dingo#4464's confirmed premature-flush defect: the gather loop
// used to flush a batch the moment a non-blocking iter.Next(false) returned
// chain.ErrIteratorChainTip, even with only one block gathered and 49 more
// blocks about to arrive. On the harness that produced the issue, this
// fragmented an intended 50-block batch into ~6-9 block commits.
//
// This scripts five blocks separated by momentary gaps (including a run of
// three consecutive gaps) with no upstream tip configured, so isNearTip is
// false throughout (bulk-replay/no-known-upstream default) and the
// coalescing wait applies. Before the fix, the first gap alone flushed a
// batch of 1; after it, all five blocks land in one batch.
func TestLedgerReadChainIteratorCoalescesGapsDuringBulkReplay(t *testing.T) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	shrinkGatherCoalesceRetryInterval(t, time.Millisecond, 5)

	block1, point1 := buildDecodableTestBlock(t, 10, 1)
	block2, point2 := buildDecodableTestBlock(t, 20, 2)
	block3, point3 := buildDecodableTestBlock(t, 30, 3)
	block4, point4 := buildDecodableTestBlock(t, 40, 4)
	block5, point5 := buildDecodableTestBlock(t, 50, 5)

	script := []*chain.ChainIteratorResult{
		{Point: point1, Block: block1},
		nil,
		{Point: point2, Block: block2},
		nil,
		nil,
		{Point: point3, Block: block3},
		nil,
		{Point: point4, Block: block4},
		nil,
		nil,
		nil,
		{Point: point5, Block: block5},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &scriptedGapLedgerReadIterator{ctx: ctx, script: script}

	// Zero-value config: config.GetActiveConnectionFunc is nil and
	// syncUpstreamTipSlot defaults to 0, so UpstreamTipSlot() returns 0 and
	// isNearTip is false for every slot -- the "still catching up, or
	// upstream unknown" default (see isNearTip's doc comment).
	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	result := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered a batch",
	)
	require.NoError(t, result.err)
	require.False(t, result.rollback)
	require.Len(
		t,
		result.blocks,
		5,
		"gaps during bulk replay should be coalesced into one batch instead "+
			"of flushing on the very first empty non-blocking check",
	)
	close(result.done)
}

// TestLedgerReadChainIteratorNearTipFlushesSingleBlockPromptly confirms the
// coalescing wait added for dingo#4464 does not regress live tip-following
// latency: once isNearTip is true, a solitary new block must still commit
// immediately rather than wait for a batch that will never fill.
//
// The retry interval is deliberately set far larger (minutes) than the
// receive deadline (seconds) below, so this does not race a tight timing
// window against scheduler jitter: a correct implementation returns near-
// instantly regardless of load, while a regressed one that started waiting
// would still be asleep by the time the deadline below expires.
func TestLedgerReadChainIteratorNearTipFlushesSingleBlockPromptly(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	shrinkGatherCoalesceRetryInterval(t, 10*time.Minute, 1)

	block, point := buildDecodableTestBlock(t, 100, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &scriptedGapLedgerReadIterator{
		ctx:    ctx,
		script: []*chain.ChainIteratorResult{{Point: point, Block: block}},
	}

	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	// Upstream tip at slot 1: the block's slot (100) is at or past it, so
	// isNearTip reports "caught up" (see nearUpstreamTip).
	ls.advanceUpstreamTipSlot(1)

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	result := testutil.RequireReceive(
		t, resultCh, 10*time.Second,
		"single block at live tip did not commit promptly -- looks like it "+
			"waited on a coalescing batch that will never fill",
	)
	require.NoError(t, result.err)
	require.False(t, result.rollback)
	require.Len(t, result.blocks, 1)
	close(result.done)
}
