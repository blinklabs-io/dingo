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

package nodeparity

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/koiosparity"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestTxInfoPipeline(
	t *testing.T,
	ctx context.Context,
	fetch txInfoFetchFunc,
	batch, concurrency, window int,
	waits []time.Duration,
	retryTransient bool,
	wait func(context.Context, time.Duration) error,
) *txInfoPipeline {
	t.Helper()
	p := newTxInfoPipeline(
		ctx, fetch, batch, concurrency, window, waits, retryTransient, wait,
		t.Logf,
	)
	t.Cleanup(p.close)
	return p
}

// awaitQueued waits for every chunk still queued to finish fetching, so a
// test can count attempts of chunks behind one that failed.
func awaitQueued(t *testing.T, p *txInfoPipeline) {
	t.Helper()
	for _, c := range p.queue {
		testutil.RequireReceive(t, c.done, 5*time.Second, "chunk fetch")
	}
}

func noWait(context.Context, time.Duration) error { return nil }

// TestTxInfoPipelineRetriesATxKoiosHasNotIndexed pins the live preprod
// failure at the tip: Koios had no /tx_info result yet for a hash Dingo had
// just served, and the flush re-baselined the reconstruction. A chunk that
// fails only that way is fetched again after a wait and then applies.
func TestTxInfoPipelineRetriesATxKoiosHasNotIndexed(t *testing.T) {
	t.Parallel()
	f := &txInfoChunkFake{
		attempts: map[string]int{},
		respond: func(chunk []string, attempt int) error {
			if chunk[0] == "ba" && attempt == 1 {
				return errTxNotIndexed("bb")
			}
			return nil
		},
	}
	waits := &recordingWait{}
	schedule := []time.Duration{time.Second, 2 * time.Second}
	p := newTestTxInfoPipeline(
		t, context.Background(), f.fetch, 2, 8, 8, schedule, false, waits.wait,
	)

	refs := UTxOSet{}
	failed, err := p.add(context.Background(), refs, "aa", "ab", "ba", "bb")
	require.NoError(t, err)
	require.False(t, failed)
	failed, err = p.flush(context.Background(), refs)
	require.NoError(t, err)
	assert.False(t, failed,
		"a tx Koios indexed within the waits must not taint the epoch")
	assert.Equal(t, schedule[:1], waits.waits)
	assert.Equal(t, 1, f.attemptsFor("aa"), "a fetched chunk is not refetched")
	assert.Equal(t, 2, f.attemptsFor("ba"))
}

// TestTxInfoPipelineGivesUpAfterTheLastWait proves the waits are bounded: a
// hash still missing after the last one fails its chunk, which taints the
// epoch, and a failure of any other kind is not fetched again.
func TestTxInfoPipelineGivesUpAfterTheLastWait(t *testing.T) {
	t.Parallel()
	f := &txInfoChunkFake{
		attempts: map[string]int{},
		respond: func(chunk []string, _ int) error {
			switch chunk[0] {
			case "aa":
				return errTxNotIndexed("aa")
			case "ba":
				return errors.New("koios /tx_info: status 500")
			}
			return nil
		},
	}
	waits := &recordingWait{}
	schedule := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	p := newTestTxInfoPipeline(
		t, context.Background(), f.fetch, 1, 8, 8, schedule, false, waits.wait,
	)

	refs := UTxOSet{}
	_, err := p.add(context.Background(), refs, "aa", "ba", "ca")
	require.NoError(t, err)
	failed, err := p.flush(context.Background(), refs)
	require.NoError(t, err)
	assert.True(t, failed,
		"a hash still missing after the last wait must taint the epoch")
	awaitQueued(t, p)
	assert.Equal(t, schedule, waits.waits)
	assert.Equal(t, 1+len(schedule), f.attemptsFor("aa"))
	assert.Equal(t, 1, f.attemptsFor("ba"))
	assert.Equal(t, 1, f.attemptsFor("ca"))
}

// TestTxInfoPipelineRetriesTransientFailuresWhenAsked covers the mainnet
// failure: a Koios gateway timeout on one chunk re-baselined the
// reconstruction and left the epoch's UTxO check not run. With
// retryTransient the chunk is fetched again and applies; a permanent
// rejection still is not.
func TestTxInfoPipelineRetriesTransientFailuresWhenAsked(t *testing.T) {
	t.Parallel()
	f := &txInfoChunkFake{
		attempts: map[string]int{},
		respond: func(chunk []string, attempt int) error {
			switch chunk[0] {
			case "aa":
				if attempt <= 2 {
					return errors.New("koios /tx_info: status 504")
				}
			case "ba":
				return fmt.Errorf("status 400: %w", koiosparity.ErrKoiosPermanent)
			}
			return nil
		},
	}
	waits := &recordingWait{}
	schedule := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	p := newTestTxInfoPipeline(
		t, context.Background(), f.fetch, 1, 8, 8, schedule, true, waits.wait,
	)

	refs := UTxOSet{}
	_, err := p.add(context.Background(), refs, "aa", "ba", "ca")
	require.NoError(t, err)
	failed, err := p.flush(context.Background(), refs)
	require.NoError(t, err)
	assert.True(t, failed, "a permanent rejection still taints the epoch")
	awaitQueued(t, p)
	assert.Equal(t, schedule[:2], waits.waits)
	assert.Equal(t, 3, f.attemptsFor("aa"))
	assert.Equal(t, 1, f.attemptsFor("ba"), "a permanent failure is not refetched")
	assert.Equal(t, 1, f.attemptsFor("ca"))
}

// TestTxInfoPipelineStopsWaitingWhenClosed proves the run ending during a
// wait ends the flush at once, with no further fetch, and is not reported as
// a Koios failure.
func TestTxInfoPipelineStopsWaitingWhenClosed(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := &txInfoChunkFake{
		attempts: map[string]int{},
		respond: func(chunk []string, _ int) error {
			return errTxNotIndexed(chunk[0])
		},
	}
	cancelThenWait := func(ctx context.Context, d time.Duration) error {
		cancel()
		return waitStakeRetry(ctx, d)
	}
	p := newTestTxInfoPipeline(
		t, ctx, f.fetch, 1, 8, 8, []time.Duration{time.Hour}, false,
		cancelThenWait,
	)
	type result struct {
		failed bool
		err    error
	}
	done := make(chan result, 1)
	go func() {
		refs := UTxOSet{}
		if _, err := p.add(context.Background(), refs, "aa"); err != nil {
			done <- result{err: err}
			return
		}
		failed, err := p.flush(context.Background(), refs)
		done <- result{failed, err}
	}()
	got := testutil.RequireReceive(t, done, 5*time.Second, "flush after cancellation")
	require.ErrorIs(t, got.err, context.Canceled)
	assert.False(t, got.failed)
	assert.Equal(t, 1, f.attemptsFor("aa"), "no fetch runs after cancellation")
}

// The pipeline never has more requests in flight than its concurrency, and
// keeps that many in flight while there is work queued.
func TestTxInfoPipelineKeepsConcurrencyInFlight(t *testing.T) {
	t.Parallel()
	var inFlight, peak atomic.Int32
	started := make(chan struct{}, 6)
	release := make(chan struct{})
	fetch := func(
		_ context.Context, chunk []string,
	) ([]koiosparity.KoiosTxInfoItem, error) {
		n := inFlight.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		started <- struct{}{}
		<-release
		inFlight.Add(-1)
		return []koiosparity.KoiosTxInfoItem{{TxHash: chunk[0]}}, nil
	}
	p := newTestTxInfoPipeline(
		t, context.Background(), fetch, 1, 2, 16, nil, false, noWait,
	)
	refs := UTxOSet{}
	_, err := p.add(context.Background(), refs, "a", "b", "c", "d", "e", "f")
	require.NoError(t, err)
	for range 2 {
		testutil.RequireReceive(t, started, 5*time.Second, "request start")
	}
	testutil.RequireNoReceive(t, started, 50*time.Millisecond,
		"a third request started while two were in flight")
	close(release)
	failed, err := p.flush(context.Background(), refs)
	require.NoError(t, err)
	require.False(t, failed)
	assert.Equal(t, int32(2), peak.Load())
}

// The point of the pipeline: while one request stalls, the chain is read on
// and later chunks are fetched, up to the window; only a full window waits.
func TestTxInfoPipelineReadsOnPastAStalledRequest(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	var fetched atomic.Int32
	fetch := func(
		ctx context.Context, chunk []string,
	) ([]koiosparity.KoiosTxInfoItem, error) {
		if chunk[0] == "a" {
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		fetched.Add(1)
		return []koiosparity.KoiosTxInfoItem{{TxHash: chunk[0]}}, nil
	}
	p := newTestTxInfoPipeline(
		t, context.Background(), fetch, 1, 2, 4, nil, false, noWait,
	)
	refs := UTxOSet{}
	for _, h := range []string{"a", "b", "c"} {
		failed, err := p.add(context.Background(), refs, h)
		require.NoError(t, err, "add waited with the window not full")
		require.False(t, failed)
	}
	require.Eventually(t, func() bool { return fetched.Load() == 2 },
		5*time.Second, 5*time.Millisecond,
		"later chunks were not fetched while the first stalled")

	sessionCtx, endSession := context.WithCancel(context.Background())
	endSession()
	_, err := p.add(sessionCtx, refs, "d")
	require.ErrorIs(t, err, context.Canceled, "a full window waits")

	close(release)
	failed, err := p.flush(context.Background(), refs)
	require.NoError(t, err)
	require.False(t, failed)
	assert.Equal(t, int32(4), fetched.Load(),
		"a hash queued before the session ended is still applied")
}

// Results apply in chain order whatever order they arrive in: a later
// chunk's spend of an output an earlier chunk creates must not run first,
// or it removes nothing and the output survives.
func TestTxInfoPipelineAppliesInChainOrder(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	secondDone := make(chan struct{})
	fetch := func(
		_ context.Context, chunk []string,
	) ([]koiosparity.KoiosTxInfoItem, error) {
		switch chunk[0] {
		case "tx1":
			<-release
			return []koiosparity.KoiosTxInfoItem{{
				TxHash: "tx1",
				Inputs: []koiosparity.KoiosTxInfoUtxoRef{
					{TxHash: "spent", TxIndex: 0},
				},
				Outputs: []koiosparity.KoiosTxInfoOutput{
					{TxHash: "tx1", TxIndex: 0},
				},
			}}, nil
		default:
			defer close(secondDone)
			return []koiosparity.KoiosTxInfoItem{{
				TxHash: "tx2",
				Inputs: []koiosparity.KoiosTxInfoUtxoRef{
					{TxHash: "tx1", TxIndex: 0},
				},
				Outputs: []koiosparity.KoiosTxInfoOutput{
					{TxHash: "tx2", TxIndex: 0},
				},
			}}, nil
		}
	}
	p := newTestTxInfoPipeline(
		t, context.Background(), fetch, 1, 2, 4, nil, false, noWait,
	)
	refs := UTxOSet{"spent#0": "addr|100|||"}
	_, err := p.add(context.Background(), refs, "tx1", "tx2")
	require.NoError(t, err)
	testutil.RequireReceive(t, secondDone, 5*time.Second, "second chunk fetched")
	_, err = p.add(context.Background(), refs)
	require.NoError(t, err)
	assert.Contains(t, refs, "spent#0",
		"a later chunk applied before an earlier one had arrived")
	close(release)
	failed, err := p.flush(context.Background(), refs)
	require.NoError(t, err)
	require.False(t, failed)
	assert.Equal(t, UTxOSet{"tx2#0": refs["tx2#0"]}, refs)
}

// reset discards every queued hash, for a reconstruction a rollback or a
// re-baseline replaced: nothing fetched for it may be applied afterwards.
func TestTxInfoPipelineResetDiscardsQueuedHashes(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	fetch := func(
		ctx context.Context, chunk []string,
	) ([]koiosparity.KoiosTxInfoItem, error) {
		if chunk[0] == "old" {
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return []koiosparity.KoiosTxInfoItem{{
			TxHash:  chunk[0],
			Outputs: []koiosparity.KoiosTxInfoOutput{{TxHash: chunk[0]}},
		}}, nil
	}
	p := newTestTxInfoPipeline(
		t, context.Background(), fetch, 1, 2, 4, nil, false, noWait,
	)
	refs := UTxOSet{}
	_, err := p.add(context.Background(), refs, "old", "partial")
	require.NoError(t, err)
	p.reset()
	close(release)
	_, err = p.add(context.Background(), refs, "new")
	require.NoError(t, err)
	failed, err := p.flush(context.Background(), refs)
	require.NoError(t, err)
	require.False(t, failed)
	assert.Len(t, refs, 1)
	assert.Contains(t, refs, "new#0")
}

// A chunk fetched again after a wait takes the next free slot ahead of every
// later chunk: the results behind it cannot apply until it does.
func TestOrderedSlotsGrantsTheEarliestChunkFirst(t *testing.T) {
	t.Parallel()
	s := newOrderedSlots(1)
	require.NoError(t, s.acquire(context.Background(), 5))
	granted := make(chan uint64, 2)
	for _, seq := range []uint64{9, 3} {
		go func() {
			assert.NoError(t, s.acquire(context.Background(), seq))
			granted <- seq
		}()
		require.Eventually(t, func() bool {
			s.mu.Lock()
			defer s.mu.Unlock()
			for _, w := range s.waiting {
				if w.seq == seq {
					return true
				}
			}
			return false
		}, 5*time.Second, time.Millisecond)
	}
	s.release()
	assert.Equal(t, uint64(3),
		testutil.RequireReceive(t, granted, 5*time.Second, "first grant"))
	s.release()
	assert.Equal(t, uint64(9),
		testutil.RequireReceive(t, granted, 5*time.Second, "second grant"))
}

// A chunk waiting to be fetched again gives up its request slot: with one
// slot, the chunk behind it is fetched during the wait. Were the slot held,
// the later chunk could never be fetched and the wait would time out.
func TestTxInfoPipelineLendsAWaitingChunksSlot(t *testing.T) {
	t.Parallel()
	laterFetched := make(chan struct{})
	var once sync.Once
	f := &txInfoChunkFake{
		attempts: map[string]int{},
		respond: func(chunk []string, attempt int) error {
			switch {
			case chunk[0] == "aa" && attempt == 1:
				return errTxNotIndexed("aa")
			case chunk[0] == "ba":
				once.Do(func() { close(laterFetched) })
			}
			return nil
		},
	}
	wait := func(ctx context.Context, _ time.Duration) error {
		select {
		case <-laterFetched:
			return nil
		case <-time.After(5 * time.Second):
			return errors.New("the later chunk was not fetched during the wait")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	p := newTestTxInfoPipeline(
		t, context.Background(), f.fetch, 1, 1, 8,
		[]time.Duration{time.Second}, false, wait,
	)

	refs := UTxOSet{}
	_, err := p.add(context.Background(), refs, "aa")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return f.attemptsFor("aa") >= 1 },
		5*time.Second, time.Millisecond)
	_, err = p.add(context.Background(), refs, "ba")
	require.NoError(t, err)
	failed, err := p.flush(context.Background(), refs)
	require.NoError(t, err)
	assert.False(t, failed, "the waiting chunk must apply after its wait")
	assert.Equal(t, 2, f.attemptsFor("aa"))
	assert.Equal(t, 1, f.attemptsFor("ba"))
}

// takeWaits counts only time add and flush spent blocked: a flush whose
// results have all arrived applies them without waiting, and must not be
// reported as a Koios stall in the epoch's diagnostics.
func TestTxInfoPipelineCountsOnlyBlockingWaits(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	var gated atomic.Bool
	fetch := func(
		ctx context.Context, chunk []string,
	) ([]koiosparity.KoiosTxInfoItem, error) {
		if gated.Load() {
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return []koiosparity.KoiosTxInfoItem{{TxHash: chunk[0]}}, nil
	}
	p := newTestTxInfoPipeline(
		t, context.Background(), fetch, 1, 4, 8, nil, false, noWait,
	)

	refs := UTxOSet{}
	_, err := p.add(context.Background(), refs, "aa", "ab")
	require.NoError(t, err)
	awaitQueued(t, p)
	_, err = p.flush(context.Background(), refs)
	require.NoError(t, err)
	n, d := p.takeWaits()
	assert.Zero(t, n, "results that had arrived are not a wait")
	assert.Zero(t, d)

	gated.Store(true)
	_, err = p.add(context.Background(), refs, "ba")
	require.NoError(t, err)
	go func() {
		time.Sleep(20 * time.Millisecond)
		close(release)
	}()
	_, err = p.flush(context.Background(), refs)
	require.NoError(t, err)
	n, d = p.takeWaits()
	assert.Equal(t, 1, n)
	assert.Positive(t, d)
}
