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
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/pipeline"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/require"
)

// TestBlockPipelineWorkerCount covers the CPU-scaled worker count that
// replaced the hardcoded blockPipelineDecodeWorkers/blockPipelineValidateWorkers
// constant of 2. A from-genesis sync profile showed a single core saturated
// while the rest of a 16-core host sat idle running the block pipeline at
// that fixed count; this proves the replacement actually tracks GOMAXPROCS
// within its floor and cap rather than silently staying pinned at 2.
//
// GOMAXPROCS is process-global, so these subtests cannot run in parallel
// with each other or with anything else that reads it; t.Cleanup restores
// the value this test observed on entry.
func TestBlockPipelineWorkerCount(t *testing.T) {
	original := runtime.GOMAXPROCS(0)
	t.Cleanup(func() { runtime.GOMAXPROCS(original) })

	cases := []struct {
		gomaxprocs int
		want       int
	}{
		// Below the floor: a constrained host (a single-core container)
		// must get exactly what it did before this change, never fewer.
		{gomaxprocs: 1, want: blockPipelineMinWorkers},
		// At and within the floor/cap range: tracks GOMAXPROCS exactly.
		{gomaxprocs: blockPipelineMinWorkers, want: blockPipelineMinWorkers},
		{gomaxprocs: 4, want: 4},
		{gomaxprocs: blockPipelineMaxWorkers, want: blockPipelineMaxWorkers},
		// Above the cap: bounded rather than left to scale unboundedly.
		{gomaxprocs: blockPipelineMaxWorkers + 1, want: blockPipelineMaxWorkers},
		{gomaxprocs: 128, want: blockPipelineMaxWorkers},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("GOMAXPROCS=%d", tc.gomaxprocs), func(t *testing.T) {
			runtime.GOMAXPROCS(tc.gomaxprocs)
			require.Equal(t, tc.want, blockPipelineWorkerCount())
		})
	}
}

// buildValidatedPipelineThroughputBlocks generates n genuinely VRF/KES-valid
// Conway blocks sharing one epoch nonce (so a single pipeline instance's
// ValidateStage can check all of them), for measuring pipeline decode+validate
// throughput without any network or database involved. Generation itself
// (real VRF/KES/cold-key material per block) is excluded from the caller's
// timed section.
func buildValidatedPipelineThroughputBlocks(
	t *testing.T,
	n int,
) (blocks []models.Block, epochNonceHex string, slotsPerKesPeriod uint64) {
	t.Helper()
	blocks = make([]models.Block, n)
	for i := range n {
		var seed [32]byte
		seed[0] = byte(i)
		seed[1] = byte(i >> 8)
		vb := testutil.BuildValidatedConwayBlockBytes(
			t,
			seed,
			7, // nonceSeed: shared epoch nonce across every generated block
			uint64(i)*1000,
			uint64(i+1),
		)
		blocks[i] = models.Block{
			Slot:   vb.Slot,
			Hash:   vb.Hash,
			Number: vb.BlockNumber,
			Type:   gledger.BlockTypeConway,
			Cbor:   vb.Cbor,
		}
		epochNonceHex = vb.EpochNonceHex
		slotsPerKesPeriod = vb.SlotsPerKesPeriod
	}
	return blocks, epochNonceHex, slotsPerKesPeriod
}

// runPipelineThroughput submits every block in blocks through a freshly
// constructed pipeline.BlockPipeline configured exactly as NewLedgerState
// wires production (productionValidateVerifyConfig, decode plus VRF/KES
// validate), with the given worker count for both stages, and returns
// blocks-processed-per-second. It fails the test if any block fails to
// decode or validate -- every generated block is genuinely valid, so an
// error here means the harness (not the worker count) is broken.
func runPipelineThroughput(
	t *testing.T,
	blocks []models.Block,
	epochNonceHex string,
	slotsPerKesPeriod uint64,
	workers int,
) float64 {
	t.Helper()
	pl := pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(workers),
		pipeline.WithValidateWorkers(workers),
		pipeline.WithEta0(epochNonceHex),
		pipeline.WithSlotsPerKesPeriod(slotsPerKesPeriod),
		pipeline.WithVerifyConfig(productionValidateVerifyConfig),
	)
	require.NoError(t, pl.Start(t.Context()))
	defer func() { require.NoError(t, pl.Stop()) }()

	start := time.Now()
	for _, blk := range blocks {
		tip := ocommon.Tip{
			Point:       ocommon.NewPoint(blk.Slot, blk.Hash),
			BlockNumber: blk.Number,
		}
		require.NoError(
			t,
			pl.Submit(context.Background(), blk.Type, blk.Cbor, tip),
		)
	}
	results := pl.Results()
	for i := range blocks {
		item := <-results
		require.NoError(t, item.DecodeError(), "block %d", i)
		require.NoError(t, item.ValidationError(), "block %d", i)
	}
	elapsed := time.Since(start)
	return float64(len(blocks)) / elapsed.Seconds()
}

// TestBlockPipelineWorkerCountThroughput asserts the correctness property the
// CPU-scaled worker count puts at risk -- that decode and VRF/KES validation
// still produce a clean result for every block once more than two workers
// share the stages -- and reports decode+validate throughput at the prior
// fixed count and at this host's scaled count alongside it. Blocks are
// genuinely valid and everything runs in-process, so neither the correctness
// check nor the reported numbers depend on peer availability, block density,
// or disk state.
//
// It deliberately does not assert a speedup ratio. A wall-clock ratio states
// something about the host as much as about the code: available parallelism
// bounds it (a 3-CPU runner can reach at most 1.5x going from 2 workers to 3,
// before the submit and drain goroutines take their share), and contention on
// a shared CI runner erases what is left. The regression that would actually
// matter here -- the worker count silently reverting to a fixed value -- is
// pinned deterministically by TestBlockPipelineWorkerCount above, which is
// where that assertion belongs.
func TestBlockPipelineWorkerCountThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("throughput measurement, not a correctness check; skipped in -short")
	}

	const blockCount = 128
	const repetitions = 5
	blocks, epochNonceHex, slotsPerKesPeriod := buildValidatedPipelineThroughputBlocks(
		t, blockCount,
	)

	oldWorkers := blockPipelineMinWorkers
	newWorkers := blockPipelineWorkerCount()

	var oldTotal, newTotal float64
	for range repetitions {
		oldTotal += runPipelineThroughput(
			t, blocks, epochNonceHex, slotsPerKesPeriod, oldWorkers,
		)
		newTotal += runPipelineThroughput(
			t, blocks, epochNonceHex, slotsPerKesPeriod, newWorkers,
		)
	}
	oldAvg := oldTotal / repetitions
	newAvg := newTotal / repetitions

	t.Logf(
		"pipeline throughput on GOMAXPROCS(0)=%d: workers=%d avg=%.0f blocks/sec, workers=%d avg=%.0f blocks/sec, ratio=%.2fx",
		runtime.GOMAXPROCS(0),
		oldWorkers, oldAvg,
		newWorkers, newAvg,
		newAvg/oldAvg,
	)
}
