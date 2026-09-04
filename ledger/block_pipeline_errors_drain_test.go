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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/testutil"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// buildNoNonceValidateBatch returns numBlocks decodable (but not
// cryptographically valid) Conway blocks at distinct slots, all falling
// inside a single epoch whose cached Nonce is empty. Every one of them
// makes blockPipelineEta0Provider fail for a covered slot whose cached Praos
// nonce is unavailable. Byron epochs always have that shape, and a later-era
// epoch can have it transiently while published state catches up.
func buildNoNonceValidateBatch(t *testing.T, numBlocks int) []models.Block {
	t.Helper()
	batch := make([]models.Block, 0, numBlocks)
	for i := range numBlocks {
		slot := uint64(i)
		blockNumber := uint64(i + 1) //nolint:gosec // test slot count is small
		cborBytes := testutil.BuildDecodableConwayBlockBytes(
			t,
			slot,
			blockNumber,
		)
		batch = append(batch, models.Block{
			Slot:   slot,
			Number: blockNumber,
			Type:   gledger.BlockTypeConway,
			Cbor:   cborBytes,
		})
	}
	return batch
}

// TestDecodeReadChainBatchDoesNotDeadlockOnManyValidationErrors is a
// regression test for issue #1894's block-processing-pipeline deadlock:
// gouroboros' pipeline.StageWorkerPool.worker pushes every non-nil
// validate-stage error onto a fixed-size errorsChan (default capacity 1000,
// PipelineConfig.PrefetchBufferSize) *before* forwarding the item onward,
// unconditionally, regardless of whether decodeReadChainBatch will later
// ignore that particular error (as it does for Byron-era blocks). Without a
// permanent reader draining that channel, submitting more than
// PrefetchBufferSize validation-failing blocks fills it, and every validate
// worker then blocks forever on `errors <- err`, cascading backpressure
// back through decodedChan/submitChan into blockPipeline.Submit() -- which
// decodeReadChainBatch calls with a background context specifically so it
// runs to completion once started, so it hangs forever with no error and no
// timeout.
//
// drainBlockPipelineErrors (started here exactly as Start() starts it for a
// real LedgerState) is what prevents that: it continuously reads
// blockPipeline.Errors() for the pipeline's full lifetime, so errorsChan
// never fills regardless of how many validate-stage errors flow through it.
// This test submits well over PrefetchBufferSize (1000) validation-failing
// blocks and asserts decodeReadChainBatch returns within a bounded timeout
// instead of hanging.
func TestDecodeReadChainBatchDoesNotDeadlockOnManyValidationErrors(
	t *testing.T,
) {
	// gouroboros pipeline.DefaultPipelineConfig's PrefetchBufferSize is 1000
	// (submitChan/decodedChan/validatedChan/resultsChan/errorsChan are each
	// that size). Without a permanent errorsChan reader, a full deadlock
	// needs more than just >1000 validation-failing items: it needs enough
	// to (a) fill errorsChan (1000) and jam every validate worker mid-item,
	// (b) fill decodedChan (1000) behind the now-stuck validate stage, and
	// (c) fill submitChan (1000) behind the now-stuck decode stage, before
	// Submit() itself blocks forever. 4000 clears that with margin and was
	// confirmed to reproduce the pre-fix hang deterministically.
	const numBlocks = 4000

	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 432000,
				Nonce:         nil, // no Praos nonce -- every eta0 lookup fails
			},
		},
		config: LedgerStateConfig{
			CardanoNodeConfig:            newTestShelleyGenesisCfg(t),
			Logger:                       testLogger(),
			BlockPipelineValidateEnabled: true,
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.publishSnapshotsLocked()

	ls.blockPipeline = pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(2),
		pipeline.WithValidateWorkers(2),
		pipeline.WithEta0Provider(ls.blockPipelineEta0Provider),
		pipeline.WithSlotsPerKesPeriod(129600),
		pipeline.WithVerifyConfig(productionValidateVerifyConfig),
	)
	require.NoError(t, ls.blockPipeline.Start(t.Context()))
	ls.blockPipelineErrorsDone = make(chan struct{})
	go ls.drainBlockPipelineErrors()
	defer func() {
		require.NoError(t, ls.blockPipeline.Stop())
		testutil.RequireReceive(
			t,
			ls.blockPipelineErrorsDone,
			5*time.Second,
			"drainBlockPipelineErrors did not exit after Stop",
		)
	}()

	rawBatch := buildNoNonceValidateBatch(t, numBlocks)

	done := make(chan struct{})
	go func() {
		defer close(done)
		// Return value intentionally ignored: decodeReadChainBatch always
		// returns ok=false here (it fully drains every remaining expected
		// Results() entry once it sees the first validation failure rather
		// than stopping early -- see its doc comment), but the interesting
		// part is that every one of numBlocks Submit() calls completed
		// without blocking forever, since decodeReadChainBatch submits the
		// entire batch before it starts draining Results().
		_, _ = ls.decodeReadChainBatch(t.Context(), rawBatch)
	}()

	testutil.RequireReceive(
		t,
		done,
		20*time.Second,
		"decodeReadChainBatch deadlocked submitting validation-failing "+
			"blocks whose errors were never drained from errorsChan "+
			"(issue #1894 regression)",
	)

	// decodeReadChainBatch returning only proves drainBlockPipelineErrors
	// (a separate goroutine) received every error off errorsChan, not that
	// it has finished classifying and counting all of them yet -- so poll
	// rather than asserting immediately.
	require.Eventually(
		t,
		func() bool {
			return promtestutil.ToFloat64(
				ls.metrics.blockPipelineExpectedEta0Errors,
			) > 0
		},
		5*time.Second,
		10*time.Millisecond,
		"expected eta0-unavailable errors should have been drained and counted",
	)
	require.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.blockPipelineUnexpectedErrors),
		"no unexpected pipeline errors should have been drained",
	)
}

// TestRecordBlockPipelineErrorClassificationDeferredIsNotUnexpected is a
// regression test for the observability gap flagged on PR #3232:
// errHeaderVerificationDeferred (the pipeline's epoch cache has not yet
// caught up with an already-committed block -- ARCHITECTURE.md's
// "resolves once the epoch cache catches up" case) must be counted
// separately from genuine decode/validate/apply problems, not folded into
// blockPipelineUnexpectedErrors alongside them.
func TestRecordBlockPipelineErrorClassificationDeferredIsNotUnexpected(
	t *testing.T,
) {
	ls := &LedgerState{
		config: LedgerStateConfig{Logger: testLogger()},
	}
	ls.metrics.init(prometheus.NewRegistry())

	ls.recordBlockPipelineError(
		fmt.Errorf(
			"%w: no cached epoch data for slot 42: %w",
			errHeaderVerificationDeferred,
			errors.New("epoch not found"),
		),
	)
	require.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			ls.metrics.blockPipelineDeferredEpochCacheErrors,
		),
		"a deferred epoch-cache lookup must count as its own transient case",
	)
	require.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.blockPipelineUnexpectedErrors),
		"a deferred epoch-cache lookup must not count as unexpected",
	)
	require.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.blockPipelineExpectedEta0Errors),
	)

	// A genuine decode failure must still land in the unexpected bucket,
	// not the new deferred one.
	ls.recordBlockPipelineError(errors.New("failed to decode block: bad cbor"))
	require.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(ls.metrics.blockPipelineUnexpectedErrors),
	)
	require.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			ls.metrics.blockPipelineDeferredEpochCacheErrors,
		),
	)
}

// TestRecordBlockPipelineErrorClassification confirms
// recordBlockPipelineError distinguishes a covered epoch without a nonce
// (errBlockPipelineEta0Unavailable) from every other error, incrementing the
// matching counter for each.
func TestRecordBlockPipelineErrorClassification(t *testing.T) {
	ls := &LedgerState{
		config: LedgerStateConfig{Logger: testLogger()},
	}
	ls.metrics.init(prometheus.NewRegistry())

	ls.recordBlockPipelineError(nil)
	require.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.blockPipelineExpectedEta0Errors),
	)
	require.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.blockPipelineUnexpectedErrors),
	)

	ls.recordBlockPipelineError(
		errors.New("eta0 provider error for slot 1: " +
			errBlockPipelineEta0Unavailable.Error()),
	)
	require.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.blockPipelineExpectedEta0Errors),
		"a plain string containing the sentinel's text (not wrapped via %%w) must not classify as expected",
	)
	require.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(ls.metrics.blockPipelineUnexpectedErrors),
	)

	ls.recordBlockPipelineError(
		errors.New("failed to decode block: bad cbor"),
	)
	require.Equal(
		t,
		float64(2),
		promtestutil.ToFloat64(ls.metrics.blockPipelineUnexpectedErrors),
	)

	// A properly %w-wrapped sentinel, as blockPipelineEta0Provider actually
	// produces, must classify as expected regardless of additional wrapping
	// layers (e.g. gouroboros' own "eta0 provider error for slot %d: %w").
	innerErr := errors.New("epoch has no nonce")
	providerErr := fmt.Errorf(
		"%w: %w",
		errBlockPipelineEta0Unavailable,
		innerErr,
	)
	wrappedByPipeline := fmt.Errorf(
		"eta0 provider error for slot 42: %w",
		providerErr,
	)
	ls.recordBlockPipelineError(wrappedByPipeline)
	require.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(ls.metrics.blockPipelineExpectedEta0Errors),
	)
	require.Equal(
		t,
		float64(2),
		promtestutil.ToFloat64(ls.metrics.blockPipelineUnexpectedErrors),
	)
}
