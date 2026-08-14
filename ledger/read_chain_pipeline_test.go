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
	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/pipeline"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildDecodableConwayBlockBytes constructs a minimal, valid (10-field
// header) Conway block with a correct block body hash, distinguished by slot
// and block number. Mirrors database/models' buildStandardConwayBlock.
func buildDecodableConwayBlockBytes(
	t *testing.T,
	slot, blockNumber uint64,
) []byte {
	t.Helper()
	block := &conway.ConwayBlock{
		BlockHeader: &conway.ConwayBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: blockNumber,
					Slot:        slot,
					VrfKey:      make([]byte, 32),
					VrfResult: lcommon.VrfResult{
						Output: make([]byte, 32),
						Proof:  make([]byte, 80),
					},
					OpCert: babbage.BabbageOpCert{
						HotVkey:   make([]byte, 32),
						Signature: make([]byte, 64),
					},
					ProtoVersion: babbage.BabbageProtoVersion{Major: 10},
				},
				Signature: make([]byte, 448),
			},
		},
	}
	tmp, err := cbor.Encode(block)
	require.NoError(t, err)
	var comps []cbor.RawMessage
	_, err = cbor.Decode(tmp, &comps)
	require.NoError(t, err)
	require.Len(t, comps, 5)
	var concat []byte
	for i := 1; i < 5; i++ {
		h := lcommon.Blake2b256Hash(comps[i])
		concat = append(concat, h.Bytes()...)
	}
	block.BlockHeader.Body.BlockBodyHash = lcommon.Blake2b256Hash(concat)
	raw, err := cbor.Encode(block)
	require.NoError(t, err)
	return raw
}

// buildDecodableTestBlock returns a models.Block wrapping a real, decodable
// Conway block at the given slot/block number, along with its canonical
// point.
func buildDecodableTestBlock(
	t *testing.T,
	slot, blockNumber uint64,
) (models.Block, ocommon.Point) {
	t.Helper()
	raw := buildDecodableConwayBlockBytes(t, slot, blockNumber)
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
