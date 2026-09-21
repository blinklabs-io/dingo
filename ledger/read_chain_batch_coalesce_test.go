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
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
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

// farUpstreamTipSlot is far enough past the single-digit slots these tests
// build blocks at to sit well outside the stability window a nil
// CardanoNodeConfig falls back to (blockfetchBatchSlotThresholdDefault,
// 50000), so isNearTip reports "still catching up" against a KNOWN upstream
// tip rather than against the unknown-upstream default.
const farUpstreamTipSlot = 1_000_000

// pausingGapLedgerReadIterator returns one block, then parks inside the Next
// call that reports the first chain-tip gap: it closes gapEntered and waits
// on resume before returning chain.ErrIteratorChainTip. Parking there leaves
// ledgerReadChainIterator holding blockPipelineGatherMutex's read lock with
// one block already gathered and holding no ledger lock, which is the only
// point from which a test can both release the reader into the coalescing
// branch and control what other locks are held when it gets there.
type pausingGapLedgerReadIterator struct {
	ctx        context.Context
	first      *chain.ChainIteratorResult
	calls      int
	gapEntered chan struct{}
	resume     chan struct{}
}

func (p *pausingGapLedgerReadIterator) Next(
	blocking bool,
) (*chain.ChainIteratorResult, error) {
	idx := p.calls
	p.calls++
	if idx == 0 {
		return p.first, nil
	}
	// Next is called serially from the reader goroutine, so indexing is a
	// deterministic way to name the first gap without a sync.Once.
	if idx == 1 {
		close(p.gapEntered)
		<-p.resume
	}
	if !blocking {
		return nil, chain.ErrIteratorChainTip
	}
	<-p.ctx.Done()
	return nil, p.ctx.Err()
}

// tryLockGatherMutex reports whether blockPipelineGatherMutex's write lock --
// the one rollbackChainAndStateDeferred takes -- is obtainable right now,
// releasing it again if it is, so it can be polled.
func tryLockGatherMutex(ls *LedgerState) bool {
	if ls.blockPipelineGatherMutex.TryLock() {
		ls.blockPipelineGatherMutex.Unlock()
		return true
	}
	return false
}

// TestLedgerReadChainIteratorHoldsGatherMutexAcrossCoalesceWait pins the
// safety property the dingo#4464 coalescing branch rests on: unlike the
// genuinely-blocking wait for a still-empty batch, the coalescing wait keeps
// blockPipelineGatherMutex's read lock held, because rawBatch already holds
// real gathered blocks a concurrent rollback must not race ahead of.
//
// TestLedgerReadChainIteratorHoldsGatherMutexAcrossGather does not reach
// here: its scripted reader pauses inside Next, never inside this wait, so
// releasing the lock across the wait leaves the whole ledger package green.
// This test closes that gap by probing for the write lock while the reader
// is inside the wait -- a release there makes the probe succeed.
func TestLedgerReadChainIteratorHoldsGatherMutexAcrossCoalesceWait(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	//
	// One attempt, so the pass makes exactly one coalescing wait, and an
	// interval long enough that the probe below fits comfortably inside
	// that single wait rather than racing its end.
	const coalesceWait = 500 * time.Millisecond
	shrinkGatherCoalesceRetryInterval(t, coalesceWait, 1)

	block, point := buildDecodableTestBlock(t, 10, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &pausingGapLedgerReadIterator{
		ctx:        ctx,
		first:      &chain.ChainIteratorResult{Point: point, Block: block},
		gapEntered: make(chan struct{}),
		resume:     make(chan struct{}),
	}

	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	ls.advanceUpstreamTipSlot(farUpstreamTipSlot)

	resultCh := make(chan readChainResult, 1)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		ls.ledgerReadChainIterator(ctx, iter, resultCh)
	}()

	testutil.RequireReceive(
		t, iter.gapEntered, testutil.AsyncWait,
		"reader never reached the chain-tip gap that triggers coalescing",
	)
	// Releasing the iterator sends the reader straight into the coalescing
	// wait: one block is gathered, the batch is under capacity, no attempt
	// has been spent, and the tip is far away.
	close(iter.resume)

	require.Never(
		t,
		func() bool { return tryLockGatherMutex(ls) },
		coalesceWait/2,
		2*time.Millisecond,
		"blockPipelineGatherMutex.Lock() succeeded while the reader was "+
			"inside the coalescing wait holding blocks it has not yet "+
			"submitted -- a concurrent rollback would drain an empty "+
			"blockPipeline and proceed ahead of them",
	)
	// Half the wait has elapsed at most, so the batch cannot have been
	// delivered yet. A delivery here would mean the probe above ran after
	// the pass ended rather than during the wait.
	select {
	case <-resultCh:
		t.Fatal(
			"batch was delivered before the coalescing wait elapsed -- the " +
				"probe above did not cover the wait",
		)
	default:
	}

	result := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered its coalesced batch",
	)
	require.NoError(t, result.err)
	require.Len(t, result.blocks, 1)
	close(result.done)

	cancel()
	testutil.RequireReceive(
		t, readerDone, testutil.AsyncWait,
		"ledgerReadChainIterator did not exit after cancellation",
	)
}

// TestLedgerReadChainIteratorTakesNoLedgerLockInsideGatherSpan pins the other
// half of that bound. ARCHITECTURE.md states a worst case for how long a
// rollback blocked on blockPipelineGatherMutex waits, derived purely from
// batchSize, gatherCoalesceMaxAttempts and gatherCoalesceRetryInterval. That
// figure only holds if nothing inside the held span can block on anything
// else. ls.isNearTip reaches calculateStabilityWindow, which takes ls.RLock,
// and Go's RWMutex parks a reader behind a pending writer -- so evaluating it
// inside the span folds an unbounded block-apply wait into the stated bound.
//
// Here a block apply's ls.Lock() is taken while the reader sits in the gather
// span, and the gather pass must still finish and release the gather mutex.
func TestLedgerReadChainIteratorTakesNoLedgerLockInsideGatherSpan(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	shrinkGatherCoalesceRetryInterval(t, time.Millisecond, 1)

	block, point := buildDecodableTestBlock(t, 10, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &pausingGapLedgerReadIterator{
		ctx:        ctx,
		first:      &chain.ChainIteratorResult{Point: point, Block: block},
		gapEntered: make(chan struct{}),
		resume:     make(chan struct{}),
	}

	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	ls.advanceUpstreamTipSlot(farUpstreamTipSlot)

	resultCh := make(chan readChainResult, 1)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		ls.ledgerReadChainIterator(ctx, iter, resultCh)
	}()

	testutil.RequireReceive(
		t, iter.gapEntered, testutil.AsyncWait,
		"reader never reached the chain-tip gap that triggers coalescing",
	)

	// The reader is parked inside Next holding the gather read lock and no
	// ledger lock, so this is obtainable now. Holding it across the resume
	// below is what a concurrent block apply does.
	ls.Lock()
	ledgerLockHeld := true
	defer func() {
		if ledgerLockHeld {
			ls.Unlock()
		}
	}()

	close(iter.resume)

	require.Eventually(
		t,
		func() bool { return tryLockGatherMutex(ls) },
		testutil.AsyncWait,
		5*time.Millisecond,
		"gather pass never released blockPipelineGatherMutex while the "+
			"ledger write lock was held -- it takes ls.RLock inside the "+
			"gather span, so the documented coalescing bound also includes "+
			"however long a block apply holds the ledger lock",
	)

	ls.Unlock()
	ledgerLockHeld = false

	result := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered its coalesced batch",
	)
	require.NoError(t, result.err)
	require.Len(t, result.blocks, 1)
	close(result.done)

	cancel()
	testutil.RequireReceive(
		t, readerDone, testutil.AsyncWait,
		"ledgerReadChainIterator did not exit after cancellation",
	)
}

// TestLedgerReadChainIteratorSkipsCoalesceAfterReachingTip covers the case
// isNearTip alone cannot see. UpstreamTipSlot returns 0 whenever no live
// upstream connection is selected, and isNearTipWithStabilityWindow folds an
// unknown upstream into "not near" -- so a node that has already caught up
// and then loses its upstream would start paying the coalescing wait again,
// gather lock held, including for its own forged blocks. reachedTip latches
// once the node first reaches the stability window and never clears, so it
// distinguishes "catching up and not yet connected" from "was at tip, lost
// the upstream".
//
// The retry interval here is minutes against a seconds-long receive
// deadline, so a regression cannot pass by winning a timing race: a correct
// implementation returns near-instantly, a regressed one is still asleep.
func TestLedgerReadChainIteratorSkipsCoalesceAfterReachingTip(t *testing.T) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	shrinkGatherCoalesceRetryInterval(t, 10*time.Minute, 10)

	block, point := buildDecodableTestBlock(t, 10, 1)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &scriptedGapLedgerReadIterator{
		ctx:    ctx,
		script: []*chain.ChainIteratorResult{{Point: point, Block: block}, nil},
	}

	// No upstream tip: UpstreamTipSlot returns 0 and isNearTip is false for
	// every slot, exactly as during bulk replay. reachedTip is what tells
	// the two apart.
	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	ls.reachedTip.Store(true)

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	result := testutil.RequireReceive(
		t, resultCh, 10*time.Second,
		"a node that already reached tip and then lost its upstream waited "+
			"on the bulk-replay coalescing batch instead of committing",
	)
	require.NoError(t, result.err)
	require.False(t, result.rollback)
	require.Len(t, result.blocks, 1)
	close(result.done)
}

// TestLedgerReadChainIteratorCommitBatchBlocksSkipsEmptyPasses pins the
// dingo_ledger_commit_batch_blocks histogram to actual submissions. A gather
// pass whose very first non-blocking probe returns chain.ErrIteratorChainTip
// gathers nothing -- the coalescing wait does not apply, because there is no
// partial batch to protect -- yet it still delivers a zero-block result
// downstream. Observing those would accumulate zeros in the lowest bucket of
// the distribution the histogram exists to measure, exactly during the bulk
// replay where the premature-flush symptom is read off it.
func TestLedgerReadChainIteratorCommitBatchBlocksSkipsEmptyPasses(
	t *testing.T,
) {
	// Not t.Parallel: swaps the package-level
	// gatherCoalesceRetryInterval/gatherCoalesceMaxAttempts seam via
	// shrinkGatherCoalesceRetryInterval.
	shrinkGatherCoalesceRetryInterval(t, time.Millisecond, 2)

	block1, point1 := buildDecodableTestBlock(t, 10, 1)
	block2, point2 := buildDecodableTestBlock(t, 20, 2)

	// The leading nil is consumed by the first, non-blocking probe, so that
	// pass gathers no blocks at all and flushes an empty result. The two
	// blocks then arrive on the following pass.
	script := []*chain.ChainIteratorResult{
		nil,
		{Point: point1, Block: block1},
		{Point: point2, Block: block2},
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	iter := &scriptedGapLedgerReadIterator{ctx: ctx, script: script}

	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	ls.metrics.init(prometheus.NewRegistry())

	resultCh := make(chan readChainResult, 1)
	go ls.ledgerReadChainIterator(ctx, iter, resultCh)

	empty := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered the empty pass",
	)
	require.NoError(t, empty.err)
	require.Empty(t, empty.blocks)
	require.Zero(
		t,
		readCommitBatchBlocksSampleCount(t, ls),
		"an empty gather pass must not be recorded as a commit",
	)
	close(empty.done)

	batch := testutil.RequireReceive(
		t, resultCh, testutil.AsyncWait,
		"reader never delivered the gathered batch",
	)
	require.NoError(t, batch.err)
	require.Len(t, batch.blocks, 2)
	count, sum := readCommitBatchBlocks(t, ls)
	require.Equal(t, uint64(1), count)
	require.InDelta(t, 2.0, sum, 0.0001)
	close(batch.done)
}

func readCommitBatchBlocks(
	t *testing.T,
	ls *LedgerState,
) (uint64, float64) {
	t.Helper()
	metric := &dto.Metric{}
	require.NoError(t, ls.metrics.commitBatchBlocks.Write(metric))
	return metric.GetHistogram().GetSampleCount(),
		metric.GetHistogram().GetSampleSum()
}

func readCommitBatchBlocksSampleCount(
	t *testing.T,
	ls *LedgerState,
) uint64 {
	t.Helper()
	count, _ := readCommitBatchBlocks(t, ls)
	return count
}
