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

package mithril

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// takenLog records the chunks a run's feed returned decoded, in order.
type takenLog struct {
	mu   sync.Mutex
	nums []uint64
}

func (l *takenLog) add(num uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.nums = append(l.nums, num)
}

func (l *takenLog) has(num uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Contains(l.nums, num)
}

func (l *takenLog) snapshot() []uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.nums)
}

type streamOutcome struct {
	res StreamResult
	err error
}

func runStreamAsync(
	ctx context.Context,
	cfg StreamConfig,
) <-chan streamOutcome {
	done := make(chan streamOutcome, 1)
	go func() {
		res, err := RunStreamCatchUp(ctx, cfg)
		done <- streamOutcome{res: res, err: err}
	}()
	return done
}

// TestStreamCatchUpDecodesAheadOfApply pins that the next chunk is decoded
// while a batch applies, that no more than one batch of chunks is decoded
// ahead, and that the applier still receives exactly the serial batches.
func TestStreamCatchUpDecodesAheadOfApply(t *testing.T) {
	t.Parallel()
	opts := streamFixtureOptions(5, nil)
	chain := readStreamChain(t, opts)
	fixture := newV2Fixture(t, opts)
	db := newSyncModeTestDB(t)
	dir := t.TempDir()

	applying := make(chan struct{})
	unblock := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(unblock) }) }
	// Registered before the run starts so it runs before the fixture's
	// cleanups even when an assertion below fails.
	defer release()
	first := true
	applier := &fakeBatchApplier{}
	applier.beforeApply = func(*fakeBatchApplier, []ledger.Block) error {
		if !first {
			return nil
		}
		first = false
		close(applying)
		<-unblock
		return nil
	}
	var taken takenLog
	// One chunk per batch and a window of four: a feed read only between
	// batches has chunk 1 downloaded but undecoded while batch 0 applies.
	cfg := streamConfig(fixture, dir, db, applier, 1, 4)
	cfg.chunkTaken = taken.add

	done := runStreamAsync(t.Context(), cfg)
	testutil.RequireReceive(t, applying, testutil.AsyncWait, "batch 0 applying")
	testutil.WaitForCondition(t, func() bool { return taken.has(1) },
		5*time.Second, "chunk 1 was not decoded while batch 0 applied")
	// One batch ahead is the bound: chunk 2 must wait for the run to take
	// chunk 1, which it cannot do while batch 0 applies.
	require.Never(t, func() bool { return taken.has(2) },
		300*time.Millisecond, 10*time.Millisecond,
		"chunk 2 was decoded more than one batch ahead")
	release()

	out := testutil.RequireReceive(t, done, testutil.AsyncWait, "run ends")
	require.NoError(t, out.err)
	applied, batches := applier.snapshot()
	assert.Equal(t, chain.points, applied)
	lens := make([]int, 0, len(batches))
	for _, b := range batches {
		lens = append(lens, len(b))
	}
	assert.Equal(t, []int{2, 2, 2, 2, 2, 2}, lens)
	assert.Equal(t, []uint64{0, 1, 2, 3, 4, 5}, taken.snapshot())
	requireMarker(t, db, 5)
	requireDirEmpty(t, dir)
}

// TestStreamCatchUpDecodeErrorKeepsOrder pins that a chunk that fails to
// decode fails the run at its own place in the sequence: every batch before
// it commits, and the batch it would have joined does not.
func TestStreamCatchUpDecodeErrorKeepsOrder(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		batchChunks int
		// committed is the last chunk the run commits before the failure.
		committed uint64
	}{
		{"one chunk per batch", 1, 2},
		{"failure inside a batch", 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opts := streamFixtureOptions(5, nil)
			chain := readStreamChain(t, opts)
			bad := uint64(3)
			opts.undecodableImmutable = &bad
			fixture := newV2Fixture(t, opts)
			db := newSyncModeTestDB(t)
			dir := t.TempDir()
			applier := &fakeBatchApplier{}

			res, err := RunStreamCatchUp(t.Context(), streamConfig(
				fixture, dir, db, applier, tc.batchChunks, 4,
			))
			require.Error(t, err)
			assert.Contains(t, err.Error(), "decoding immutable file 00003")

			applied, _ := applier.snapshot()
			var want []ocommon.Point
			for _, p := range chain.points {
				if chain.chunkOf(p) <= tc.committed {
					want = append(want, p)
				}
			}
			assert.Equal(t, want, applied)
			last := chain.last[tc.committed]
			assert.True(t, samePoint(last, res.Tip), "result tip")
			assert.Equal(t, tc.committed, res.Immutable)
			requireMarker(t, db, tc.committed)
			requireDirEmpty(t, dir)
		})
	}
}

// TestStreamCatchUpStaleRestartWhileDecodingAhead pins that a stale batch
// discards the chunks decoded ahead of it with the feed, and that the replay
// applies every block exactly once.
func TestStreamCatchUpStaleRestartWhileDecodingAhead(t *testing.T) {
	t.Parallel()
	opts := streamFixtureOptions(5, nil)
	chain := readStreamChain(t, opts)
	fixture := newV2Fixture(t, opts)
	db := newSyncModeTestDB(t)
	dir := t.TempDir()

	var taken takenLog
	aheadTaken := make(chan struct{})
	var aheadOnce sync.Once
	sawAhead := false
	fired := false
	applier := &fakeBatchApplier{}
	applier.beforeApply = func(
		a *fakeBatchApplier, blocks []ledger.Block,
	) error {
		if fired || chain.chunkOf(blockPoint(blocks[0])) != 2 {
			return nil
		}
		fired = true
		// Refuse only once chunk 3 is decoded ahead, so the refusal has
		// work in flight to discard.
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case <-aheadTaken:
			sawAhead = true
		case <-timer.C:
		}
		a.applyLocked(blocks)
		return fmt.Errorf("fake: competing writer: %w", ErrStaleBatch)
	}
	cfg := streamConfig(fixture, dir, db, applier, 1, 4)
	cfg.chunkTaken = func(num uint64) {
		taken.add(num)
		if num == 3 {
			aheadOnce.Do(func() { close(aheadTaken) })
		}
	}

	res, err := RunStreamCatchUp(t.Context(), cfg)
	require.NoError(t, err)
	require.True(t, fired)
	applier.mu.Lock()
	ahead := sawAhead
	applier.mu.Unlock()
	assert.True(t, ahead, "chunk 3 was not decoded while batch 2 applied")
	applied, _ := applier.snapshot()
	assert.Equal(t, chain.points, applied, "each block exactly once")
	assert.Equal(t, 1, res.Restarts)
	assert.True(t, samePoint(chain.points[len(chain.points)-1], res.Tip))
	requireMarker(t, db, 5)
	requireDirEmpty(t, dir)
}

// TestStreamCatchUpCancelWhileDecodingAhead pins that cancelling a run while
// a batch applies and the next chunk decodes ends the run with the
// cancellation, leaves only whole committed chunks behind, and leaves no
// decode goroutine running once RunStreamCatchUp returns.
//
// Not t.Parallel: the goroutine check scans every goroutine in the process,
// and a sibling's run would be counted as this one's leak.
func TestStreamCatchUpCancelWhileDecodingAhead(t *testing.T) {
	opts := streamFixtureOptions(7, nil)
	chain := readStreamChain(t, opts)
	fixture := newV2Fixture(t, opts)
	db := newSyncModeTestDB(t)
	dir := t.TempDir()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	applying := make(chan struct{})
	unblock := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(unblock) }) }
	defer release()
	first := true
	applier := &fakeBatchApplier{}
	applier.beforeApply = func(*fakeBatchApplier, []ledger.Block) error {
		if !first {
			return nil
		}
		first = false
		close(applying)
		<-unblock
		return nil
	}
	cfg := streamConfig(fixture, dir, db, applier, 1, 4)

	done := runStreamAsync(ctx, cfg)
	testutil.RequireReceive(t, applying, testutil.AsyncWait, "batch 0 applying")
	cancel()
	release()

	out := testutil.RequireReceive(t, done, testutil.AsyncWait, "run ends")
	require.ErrorIs(t, out.err, context.Canceled)
	assert.False(t,
		testutil.GoroutineParkedIn("mithril.(*decodeAhead).run"),
		"a decode goroutine outlived the run")

	applied, _ := applier.snapshot()
	require.NotEmpty(t, applied, "batch 0 committed")
	assert.Equal(t, chain.points[:len(applied)], applied)
	last := applied[len(applied)-1]
	assert.True(t, samePoint(last, chain.last[chain.chunkOf(last)]),
		"the run stopped inside a chunk")
	requireMarker(t, db, chain.chunkOf(last))
	requireDirEmpty(t, dir)
}

// fakeChunkSource hands out chunks of a fixed size, counting Next calls.
// fail, when set, decides call i's error; block, when set, makes every call
// wait for its context instead.
type fakeChunkSource struct {
	mu     sync.Mutex
	calls  int
	blocks int
	fail   func(i int) error
	block  bool
}

func (s *fakeChunkSource) Next(ctx context.Context) (VerifiedChunk, error) {
	s.mu.Lock()
	i := s.calls
	s.calls++
	s.mu.Unlock()
	if s.block {
		<-ctx.Done()
		return VerifiedChunk{}, ctx.Err()
	}
	if s.fail != nil {
		if err := s.fail(i); err != nil {
			return VerifiedChunk{}, err
		}
	}
	return VerifiedChunk{
		Num:    uint64(i), // #nosec G115 -- a small test counter
		Blocks: make([]ledger.Block, s.blocks),
	}, nil
}

func (s *fakeChunkSource) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func requireStopped(t *testing.T, d *decodeAhead) {
	t.Helper()
	select {
	case <-d.done:
	default:
		t.Fatal("stop returned with the decode goroutine still running")
	}
}

// TestDecodeAheadKeepsOrderAndError pins that chunks and the error that ends
// the source come out in the order Next returned them, that the error
// repeats, and that nothing is taken after it.
func TestDecodeAheadKeepsOrderAndError(t *testing.T) {
	t.Parallel()
	failure := errors.New("fake: decode failed")
	src := &fakeChunkSource{blocks: 1, fail: func(i int) error {
		if i == 3 {
			return failure
		}
		return nil
	}}
	d := startDecodeAhead(t.Context(), src, 8, 0, nil)
	defer d.stop()
	for want := range uint64(3) {
		chunk, err := d.next()
		require.NoError(t, err)
		require.Equal(t, want, chunk.Num)
	}
	_, err := d.next()
	require.ErrorIs(t, err, failure)
	_, again := d.next()
	require.ErrorIs(t, again, failure)
	d.stop()
	requireStopped(t, d)
	assert.Equal(t, 4, src.count(), "Next was called after the error")
}

// TestDecodeAheadBounds pins that the goroutine stops taking chunks once the
// queue holds maxChunks chunks or maxBlocks blocks, and resumes as the run
// receives them.
func TestDecodeAheadBounds(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		blocks    int
		maxChunks int
		maxBlocks int
		// settle is how many chunks are taken before the bound holds.
		settle int
	}{
		{"chunk bound", 0, 2, 0, 2},
		{"block bound", 3, 10, 5, 2},
		{"chunk over the block bound", 7, 10, 5, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			src := &fakeChunkSource{blocks: tc.blocks}
			d := startDecodeAhead(t.Context(), src, tc.maxChunks, tc.maxBlocks, nil)
			defer d.stop()
			testutil.WaitForCondition(t, func() bool {
				return src.count() >= tc.settle
			}, 5*time.Second, "chunks taken ahead")
			require.Never(t, func() bool { return src.count() > tc.settle },
				200*time.Millisecond, 10*time.Millisecond,
				"took a chunk past the bound")
			_, err := d.next()
			require.NoError(t, err)
			testutil.WaitForCondition(t, func() bool {
				return src.count() == tc.settle+1
			}, 5*time.Second, "taking resumed after a receive")
		})
	}
}

// TestDecodeAheadStop pins that stop ends the goroutine before returning,
// whether it is blocked in Next or waiting for the queue to drain.
func TestDecodeAheadStop(t *testing.T) {
	t.Parallel()
	t.Run("blocked in Next", func(t *testing.T) {
		t.Parallel()
		src := &fakeChunkSource{block: true}
		d := startDecodeAhead(t.Context(), src, 1, 0, nil)
		testutil.WaitForCondition(t, func() bool { return src.count() == 1 },
			5*time.Second, "Next called")
		d.stop()
		requireStopped(t, d)
		d.stop()
	})
	t.Run("queue full", func(t *testing.T) {
		t.Parallel()
		src := &fakeChunkSource{blocks: 1}
		d := startDecodeAhead(t.Context(), src, 1, 0, nil)
		testutil.WaitForCondition(t, func() bool { return src.count() == 1 },
			5*time.Second, "queue filled")
		d.stop()
		requireStopped(t, d)
	})
}

// TestDecodeAheadCancel pins that cancelling the run's context reaches a
// blocked Next, and that its error is what the run receives.
func TestDecodeAheadCancel(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	src := &fakeChunkSource{block: true}
	d := startDecodeAhead(ctx, src, 1, 0, nil)
	defer d.stop()
	testutil.WaitForCondition(t, func() bool { return src.count() == 1 },
		5*time.Second, "Next called")
	cancel()
	_, err := d.next()
	require.ErrorIs(t, err, context.Canceled)
}
