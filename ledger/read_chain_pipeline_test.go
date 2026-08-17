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
	"io"
	"log/slog"
	"runtime"
	"testing"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/pipeline"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildDecodableTestBlock returns a models.Block wrapping a real, decodable
// Conway block at the given slot/block number, along with its canonical
// point. The block bytes themselves come from the shared
// testutil.BuildDecodableConwayBlockBytes helper (also used by
// database/models' buildStandardConwayBlock) so this construction isn't
// duplicated across packages.
func buildDecodableTestBlock(
	t *testing.T,
	slot, blockNumber uint64,
) (models.Block, ocommon.Point) {
	t.Helper()
	raw := testutil.BuildDecodableConwayBlockBytes(t, slot, blockNumber)
	decoded, err := gledger.NewBlockFromCbor(gledger.BlockTypeConway, raw)
	require.NoError(t, err)
	hash := decoded.Hash().Bytes()
	return models.Block{
			Slot:   slot,
			Hash:   hash,
			Number: blockNumber,
			Type:   gledger.BlockTypeConway,
			Cbor:   raw,
		}, ocommon.Point{
			Slot: slot,
			Hash: hash,
		}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func TestDecodeReadChainBatchEmptyBatch(t *testing.T) {
	ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	decoded, ok := ls.decodeReadChainBatch(t.Context(), nil)
	assert.True(t, ok)
	assert.Nil(t, decoded)
}

func TestDecodeReadChainBatchSerialAndPipelineAgree(t *testing.T) {
	ctx := t.Context()
	rawBatch := make([]models.Block, 0, 3)
	for i, slot := range []uint64{10, 20, 30} {
		block, _ := buildDecodableTestBlock(t, slot, uint64(i+1)) //nolint:gosec
		rawBatch = append(rawBatch, block)
	}

	serialLS := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	serialDecoded, ok := serialLS.decodeReadChainBatch(ctx, rawBatch)
	require.True(t, ok)
	require.Len(t, serialDecoded, 3)

	pipelineLS := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	pipelineLS.blockPipeline = pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(2),
	)
	require.NoError(t, pipelineLS.blockPipeline.Start(ctx))
	defer func() {
		require.NoError(t, pipelineLS.blockPipeline.Stop())
	}()
	pipelineDecoded, ok := pipelineLS.decodeReadChainBatch(ctx, rawBatch)
	require.True(t, ok)
	require.Len(t, pipelineDecoded, 3)

	for i := range serialDecoded {
		assert.Equal(
			t,
			serialDecoded[i].SlotNumber(),
			pipelineDecoded[i].SlotNumber(),
		)
		assert.Equal(
			t,
			serialDecoded[i].Hash(),
			pipelineDecoded[i].Hash(),
		)
	}
	// Also confirm order matches the raw input order (10, 20, 30), not just
	// that the two modes agree with each other.
	assert.Equal(t, uint64(10), serialDecoded[0].SlotNumber())
	assert.Equal(t, uint64(20), serialDecoded[1].SlotNumber())
	assert.Equal(t, uint64(30), serialDecoded[2].SlotNumber())
}

func TestDecodeReadChainBatchPropagatesDecodeErrorBothModes(t *testing.T) {
	ctx := t.Context()
	good, _ := buildDecodableTestBlock(t, 10, 1)
	bad := models.Block{
		Slot: 20,
		Hash: []byte("bad-hash-bad-hash-bad-hash-32by"),
		Type: gledger.BlockTypeConway,
		Cbor: []byte{0xff, 0xff, 0xff}, // not valid CBOR
	}
	rawBatch := []models.Block{good, bad}

	serialLS := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	_, ok := serialLS.decodeReadChainBatch(ctx, rawBatch)
	assert.False(t, ok)

	pipelineLS := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	pipelineLS.blockPipeline = pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(2),
	)
	require.NoError(t, pipelineLS.blockPipeline.Start(ctx))
	defer func() {
		require.NoError(t, pipelineLS.blockPipeline.Stop())
	}()
	_, ok = pipelineLS.decodeReadChainBatch(ctx, rawBatch)
	assert.False(t, ok)
}

// TestLedgerReadChainIteratorPipelineMatchesSerial exercises the full
// ledgerReadChainIterator loop (gather -> decode -> rollback trim -> emit)
// with a real chain iterator script, once with the block-decode pipeline
// disabled and once enabled, and asserts both modes produce the identical
// decoded batch in the identical order -- the acceptance criterion that the
// chain tip (and everything upstream of it) must match with the pipeline
// enabled and disabled.
func TestLedgerReadChainIteratorPipelineMatchesSerial(t *testing.T) {
	block1, point1 := buildDecodableTestBlock(t, 10, 1)
	block2, point2 := buildDecodableTestBlock(t, 20, 2)
	block3, point3 := buildDecodableTestBlock(t, 30, 3)

	run := func(t *testing.T, ls *LedgerState) []gledger.Block {
		t.Helper()
		// Use an explicit cancellable context (not t.Context()) so the
		// background goroutine below is fully drained before this helper
		// returns, instead of lingering until the whole test completes.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		resultCh := make(chan readChainResult)
		iter := &scriptedLedgerReadIterator{
			ctx: ctx,
			results: []*chain.ChainIteratorResult{
				{Point: point1, Block: block1},
				{Point: point2, Block: block2},
				{Point: point3, Block: block3},
			},
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			ls.ledgerReadChainIterator(ctx, iter, resultCh)
		}()
		result := <-resultCh
		require.False(t, result.rollback)
		require.Len(t, result.blocks, 3)
		close(result.done)
		cancel()
		<-done
		return result.blocks
	}

	serialLS := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	serialBlocks := run(t, serialLS)

	pipelineLS := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	pipelineLS.blockPipeline = pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(2),
	)
	require.NoError(t, pipelineLS.blockPipeline.Start(t.Context()))
	defer func() {
		require.NoError(t, pipelineLS.blockPipeline.Stop())
	}()
	pipelineBlocks := run(t, pipelineLS)

	for i := range serialBlocks {
		assert.Equal(
			t,
			serialBlocks[i].SlotNumber(),
			pipelineBlocks[i].SlotNumber(),
			"block %d slot mismatch",
			i,
		)
		assert.Equal(
			t,
			serialBlocks[i].Hash(),
			pipelineBlocks[i].Hash(),
			"block %d hash mismatch",
			i,
		)
	}
}

// TestLedgerReadChainIteratorRollbackTrimMatchesBothModes verifies that a
// rollback sentinel landing mid-batch trims the decoded batch to the
// canonical prefix identically whether or not the block-decode pipeline is
// enabled.
func TestLedgerReadChainIteratorRollbackTrimMatchesBothModes(t *testing.T) {
	block1, point1 := buildDecodableTestBlock(t, 10, 1)
	block2, point2 := buildDecodableTestBlock(t, 20, 2)

	run := func(t *testing.T, ls *LedgerState) readChainResult {
		t.Helper()
		// Use an explicit cancellable context (not t.Context()) so the
		// background goroutine below is fully drained before this helper
		// returns, instead of lingering until the whole test completes.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		resultCh := make(chan readChainResult)
		iter := &scriptedLedgerReadIterator{
			ctx: ctx,
			results: []*chain.ChainIteratorResult{
				{Point: point1, Block: block1},
				{Point: point2, Block: block2},
				// Rollback to the first block: since it's already in the
				// gathered batch, this should trim to a 1-block batch
				// rather than emit a genuine rollback result.
				{Rollback: true, Point: point1},
			},
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			ls.ledgerReadChainIterator(ctx, iter, resultCh)
		}()
		result := <-resultCh
		close(result.done)
		cancel()
		<-done
		return result
	}

	serialLS := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	serialResult := run(t, serialLS)
	require.False(t, serialResult.rollback)
	require.Len(t, serialResult.blocks, 1)
	assert.Equal(t, uint64(10), serialResult.blocks[0].SlotNumber())

	pipelineLS := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
	pipelineLS.blockPipeline = pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(2),
	)
	require.NoError(t, pipelineLS.blockPipeline.Start(t.Context()))
	defer func() {
		require.NoError(t, pipelineLS.blockPipeline.Stop())
	}()
	pipelineResult := run(t, pipelineLS)
	require.False(t, pipelineResult.rollback)
	require.Len(t, pipelineResult.blocks, 1)
	assert.Equal(t, uint64(10), pipelineResult.blocks[0].SlotNumber())
}

// TestBlockPipelineLifecycleNoGoroutineLeak verifies that repeatedly
// starting and stopping the block-decode pipeline through the exact
// Submit/Results usage decodeReadChainBatch performs does not accumulate
// goroutines, i.e. Stop() fully drains the decode worker pool, apply-stage
// runner, and metrics collector goroutines every time.
//
// This runs many Start/Submit/Stop cycles and compares the goroutine count
// before the first cycle to after the last, rather than a single
// before/after snapshot (or goleak.VerifyNone): runtime.NumGoroutine() is
// not perfectly quiescent even with no leak at all (short-lived runtime/GC
// goroutines come and go), so a single-sample comparison is inherently
// noisy. A genuine per-cycle leak, in contrast, grows roughly linearly with
// the iteration count and is easily distinguished from that noise over many
// cycles.
func TestBlockPipelineLifecycleNoGoroutineLeak(t *testing.T) {
	const iterations = 20

	runtime.GC()
	baseline := runtime.NumGoroutine()

	for i := range iterations {
		ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
		ls.blockPipeline = pipeline.NewBlockPipeline(
			pipeline.WithDecodeWorkers(2),
		)
		ctx := t.Context()
		require.NoError(t, ls.blockPipeline.Start(ctx))

		block, _ := buildDecodableTestBlock(
			t,
			uint64(10+i), //nolint:gosec
			uint64(i+1),  //nolint:gosec
		)
		decoded, ok := ls.decodeReadChainBatch(ctx, []models.Block{block})
		require.True(t, ok)
		require.Len(t, decoded, 1)

		require.NoError(t, ls.blockPipeline.Stop())
	}

	runtime.GC()
	after := runtime.NumGoroutine()
	// Allow generous slack for unrelated ambient runtime goroutines without
	// masking a genuine leak, which would show up as growth proportional to
	// iterations (20), not a handful.
	assert.LessOrEqual(
		t,
		after,
		baseline+5,
		"goroutine count grew from %d to %d over %d block-decode pipeline "+
			"start/stop cycles; possible leak in pipeline lifecycle",
		baseline,
		after,
		iterations,
	)
}

// buildTaggedRawBlocks returns count individually-valid, decodable Conway
// models.Block values with distinct slots starting at baseSlot, so a caller
// can tell which batch a decoded result came from purely from its
// SlotNumber().
func buildTaggedRawBlocks(
	t *testing.T,
	baseSlot uint64,
	count int,
) []models.Block {
	t.Helper()
	blocks := make([]models.Block, 0, count)
	for i := range count {
		block, _ := buildDecodableTestBlock(
			t,
			baseSlot+uint64(i), //nolint:gosec
			uint64(i+1),        //nolint:gosec
		)
		blocks = append(blocks, block)
	}
	return blocks
}

// TestLedgerProcessBlocksRetryDoesNotMixBlocksAcrossAttempts is a regression
// test for a confirmed cross-attempt race in the block-decode pipeline
// (blinklabs-io/dingo#3178 review, human reviewer arepala-uml): the retry
// loop in ledgerProcessBlocks -- exercised here through
// runLedgerReadChainAttempt, the exact goroutine-lifecycle primitive that
// loop uses -- previously started a new attempt's reader goroutine without
// waiting for the previous attempt's reader goroutine to fully exit. Since
// every attempt submits to and drains from the SAME shared,
// whole-LedgerState-lifetime blockPipeline, and the pipeline's apply stage
// reorders decoded results purely by a global sequence number with no
// notion of "whose submission is whose", two attempts' reader goroutines
// submitting concurrently can -- and, per the reviewer's own stress test,
// reliably do -- get each other's decoded blocks back from Results().
//
// This reproduces the exact race the reviewer identified: a restart racing
// a rollback wakes the OLD attempt's reader goroutine (via
// completeReadResult(), in ledgerProcessBlocksFromSource) before the retry
// loop's cancel() takes effect, and that goroutine's gather loop had no
// cancellation check, so it could keep submitting more blocks to the shared
// pipeline. Attempt 1's fake reader below reproduces this directly: after
// delivering its first (legitimate) batch and being told to restart, it
// submits a second, disjoint "straggler" batch using a background context
// (bypassing any cancellation check entirely, exactly like the pre-fix
// gather loop). Attempt 2's fake reader submits its own, disjoint,
// uniquely-identifiable batch. Every block is tagged with a distinct slot
// range (attempt1 / straggler / attempt2), so any cross-attempt
// misattribution shows up directly as an out-of-range slot in attempt 2's
// own result.
//
// With the fix, runLedgerReadChainAttempt does not return from attempt 1
// until its reader goroutine (including the straggler submission) has fully
// exited, so attempt 2 can never start submitting while attempt 1's
// straggler submission is still in flight -- this is deterministic, not
// probabilistic, given the fix. The iteration count and batch sizes here
// exist for the *pre-fix* validation described in the fix's PR: with
// runLedgerReadChainAttempt's final wait removed, these two attempts' reader
// goroutines really do run concurrently, and this loop reproduces
// misattribution reliably (matching the reviewer's own 200-iteration stress
// test methodology) rather than depending on a single lucky (or unlucky)
// scheduling outcome.
func TestLedgerProcessBlocksRetryDoesNotMixBlocksAcrossAttempts(t *testing.T) {
	const iterations = 25
	const attempt1Base, attempt1Size = 100_000, 150
	const stragglerBase, stragglerSize = 200_000, 300
	const attempt2Base, attempt2Size = 300_000, 150

	attempt1Blocks := buildTaggedRawBlocks(t, attempt1Base, attempt1Size)
	stragglerBlocks := buildTaggedRawBlocks(t, stragglerBase, stragglerSize)
	attempt2Blocks := buildTaggedRawBlocks(t, attempt2Base, attempt2Size)

	inRange := func(slot, base uint64, count int) bool {
		return slot >= base && slot < base+uint64(count) //nolint:gosec
	}

	for iter := range iterations {
		ls := &LedgerState{config: LedgerStateConfig{Logger: testLogger()}}
		ls.blockPipeline = pipeline.NewBlockPipeline(
			pipeline.WithDecodeWorkers(2),
		)
		require.NoError(t, ls.blockPipeline.Start(t.Context()))

		// reader1 mimics ledgerReadChain for a first attempt that gets
		// told to restart, then -- like the pre-fix gather loop -- keeps
		// submitting to the shared pipeline regardless.
		reader1 := func(ctx context.Context, resultCh chan readChainResult) {
			defer close(resultCh)
			decoded, ok := ls.decodeReadChainBatch(ctx, attempt1Blocks)
			if !ok {
				return
			}
			done := make(chan struct{})
			select {
			case resultCh <- readChainResult{blocks: decoded, done: done}:
			case <-ctx.Done():
				return
			}
			select {
			case <-done:
			case <-ctx.Done():
				return
			}
			// Simulate the race this fix closes: submit a further batch
			// using a background context, exactly like a gather loop with
			// no cancellation check would after being woken by
			// completeReadResult(), regardless of ctx's state.
			_, _ = ls.decodeReadChainBatch(
				context.Background(),
				stragglerBlocks,
			)
		}
		processFromSource1 := func(
			_ context.Context,
			resultCh <-chan readChainResult,
		) error {
			result, ok := <-resultCh
			require.True(t, ok, "attempt 1 result channel closed early")
			for _, block := range result.blocks {
				require.True(
					t,
					inRange(block.SlotNumber(), attempt1Base, attempt1Size),
					"iteration %d: attempt 1 delivered an unexpected "+
						"block at slot %d",
					iter,
					block.SlotNumber(),
				)
			}
			if result.done != nil {
				close(result.done)
			}
			// Simulates errStaleChainIterator/errRestartLedgerPipeline
			// triggering a retry.
			return errRestartLedgerPipeline
		}

		err := ls.runLedgerReadChainAttempt(
			t.Context(),
			reader1,
			processFromSource1,
		)
		require.ErrorIs(t, err, errRestartLedgerPipeline)

		var attempt2Decoded []gledger.Block
		reader2 := func(ctx context.Context, resultCh chan readChainResult) {
			defer close(resultCh)
			decoded, ok := ls.decodeReadChainBatch(ctx, attempt2Blocks)
			if !ok {
				return
			}
			done := make(chan struct{})
			select {
			case resultCh <- readChainResult{blocks: decoded, done: done}:
			case <-ctx.Done():
				return
			}
			select {
			case <-done:
			case <-ctx.Done():
			}
		}
		processFromSource2 := func(
			_ context.Context,
			resultCh <-chan readChainResult,
		) error {
			result, ok := <-resultCh
			require.True(t, ok, "attempt 2 result channel closed early")
			attempt2Decoded = result.blocks
			if result.done != nil {
				close(result.done)
			}
			return nil
		}

		err = ls.runLedgerReadChainAttempt(
			t.Context(),
			reader2,
			processFromSource2,
		)
		require.NoError(t, err)

		require.Len(
			t,
			attempt2Decoded,
			attempt2Size,
			"iteration %d: attempt 2's decoded batch length changed -- "+
				"contamination altered its size",
			iter,
		)
		for _, block := range attempt2Decoded {
			assert.True(
				t,
				inRange(block.SlotNumber(), attempt2Base, attempt2Size),
				"iteration %d: attempt 2 received a block at slot %d "+
					"that does not belong to its own batch -- "+
					"cross-attempt contamination from the shared "+
					"block-decode pipeline",
				iter,
				block.SlotNumber(),
			)
		}

		require.NoError(t, ls.blockPipeline.Stop())
	}
}
