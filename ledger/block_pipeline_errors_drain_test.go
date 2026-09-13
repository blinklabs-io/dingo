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
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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

// capturedErrorLog is a slog.Handler that retains the ERROR-level records
// written through it. recordBlockPipelineError is the only place an
// unexpected block-pipeline error's text is ever rendered -- errorsChan
// carries a bare error with no item context, and the drain discards it after
// counting -- so a test that asserts "no unexpected error was drained"
// against a discarding logger can only ever report a number. Keeping the
// text here makes a future failure name the error instead of its count.
type capturedErrorLog struct {
	mu    sync.Mutex
	lines []string
}

func (c *capturedErrorLog) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelError
}

func (c *capturedErrorLog) Handle(_ context.Context, r slog.Record) error {
	var sb strings.Builder
	sb.WriteString(r.Message)
	r.Attrs(func(a slog.Attr) bool {
		sb.WriteString(" ")
		sb.WriteString(a.String())
		return true
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, sb.String())
	return nil
}

func (c *capturedErrorLog) WithAttrs(_ []slog.Attr) slog.Handler { return c }

func (c *capturedErrorLog) WithGroup(_ string) slog.Handler { return c }

func (c *capturedErrorLog) logger() *slog.Logger {
	return slog.New(c)
}

// summary renders the distinct captured ERROR lines with their occurrence
// counts, capped so a batch that fails thousands of times does not bury the
// first line in repeats of itself.
func (c *capturedErrorLog) summary() string {
	const maxDistinct = 10
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.lines) == 0 {
		return "(no ERROR-level log records captured)"
	}
	counts := map[string]int{}
	for _, line := range c.lines {
		counts[line]++
	}
	distinct := make([]string, 0, len(counts))
	for line := range counts {
		distinct = append(distinct, line)
	}
	sort.Slice(distinct, func(i, j int) bool {
		if counts[distinct[i]] != counts[distinct[j]] {
			return counts[distinct[i]] > counts[distinct[j]]
		}
		return distinct[i] < distinct[j]
	})
	var sb strings.Builder
	for i, line := range distinct {
		if i == maxDistinct {
			fmt.Fprintf(
				&sb,
				"\n... and %d further distinct ERROR line(s)",
				len(distinct)-maxDistinct,
			)
			break
		}
		fmt.Fprintf(&sb, "\n  x%d %s", counts[line], line)
	}
	return sb.String()
}

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
	t.Parallel()

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

	errLog := &capturedErrorLog{}
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
			Logger:                       errLog.logger(),
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
	stopAndDrain := stopAndDrainBlockPipeline(t, ls)
	defer stopAndDrain()

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
	// it has finished classifying and counting all of them yet. Stopping the
	// pipeline closes errorsChan, which ends the drain's range loop and
	// closes blockPipelineErrorsDone, so waiting for that here is the one
	// definite end-of-drain point: every error the batch produced has been
	// classified and counted by the time the assertions below run.
	//
	// Polling for "expectedEta0 > 0" instead raced in the passing
	// direction -- it is satisfied by the first error drained, so the
	// unexpected-error assertion that followed could read its counter
	// before the errors that would have failed it were classified.
	stopAndDrain()

	require.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.blockPipelineUnexpectedErrors),
		"no unexpected pipeline errors should have been drained; "+
			"ERROR log records captured from recordBlockPipelineError:%s",
		errLog.summary(),
	)
	// Every one of the numBlocks blocks fails the eta0 lookup exactly once
	// in the validate stage, and the drain is now known to have finished,
	// so this is an exact count rather than a lower bound.
	require.Equal(
		t,
		float64(numBlocks),
		promtestutil.ToFloat64(ls.metrics.blockPipelineExpectedEta0Errors),
		"every block's eta0-unavailable error should have been drained and counted",
	)
}

// stopAndDrainBlockPipeline returns an idempotent function that stops ls's
// block pipeline and waits for drainBlockPipelineErrors to exit. Stop()
// closes errorsChan, which is what ends the drain goroutine's range loop, so
// the returned function is the only point at which the drain's counters are
// known to be final. Callers defer it for cleanup and call it directly
// before asserting on those counters; the second call is a no-op.
func stopAndDrainBlockPipeline(
	t *testing.T,
	ls *LedgerState,
) func() {
	t.Helper()
	var once sync.Once
	return func() {
		once.Do(func() {
			require.NoError(t, ls.blockPipeline.Stop())
			testutil.RequireReceive(
				t,
				ls.blockPipelineErrorsDone,
				5*time.Second,
				"drainBlockPipelineErrors did not exit after Stop",
			)
		})
	}
}

// TestDrainBlockPipelineErrorsApplyPendingLimitIsNotUnexpected is a
// regression test for a flake on main: the deadlock regression test above
// intermittently failed with a few hundred "unexpected" pipeline errors and
// no way to see what they were, because recordBlockPipelineError routed
// pipeline.ErrPendingLimitExceeded to the default branch.
//
// That error is not a block failure. gouroboros' apply stage buffers
// out-of-order items until their sequence number comes up; when one stage
// worker falls behind its siblings, the earliest sequence number stalls and
// every later item piles into that buffer. Past MaxPendingBlocks,
// ApplyStage.ProcessWithStatus still buffers the item -- "to prevent
// sequence gaps", per its own comment -- and additionally returns
// ErrPendingLimitExceeded as a backpressure signal, which the apply runner
// forwards to errorsChan. Every one of those blocks is still applied in
// sequence. Counting them as unexpected therefore both fails this package
// under CI scheduling pressure and, in production, logs a burst at ERROR
// level while inflating a counter whose help text tells operators a nonzero
// value means a decode, validation, or apply problem.
//
// The stall is a scheduling accident on CI, so this test creates it on
// purpose: the eta0 provider blocks the first submitted block (sequence 0)
// inside the validate stage until every other block in the batch has
// already passed through it. validatedChan is FIFO and holds the whole
// batch, so the apply stage sees all numBlocks-1 later items -- each of them
// out of order -- before it ever sees sequence 0. MaxPendingBlocks is
// lowered from the production default (2160, Cardano's k) only so the batch
// can stay small; nothing else about the pipeline, the drain, or the
// classification path is substituted.
func TestDrainBlockPipelineErrorsApplyPendingLimitIsNotUnexpected(
	t *testing.T,
) {
	t.Parallel()

	const (
		numBlocks  = 512
		maxPending = 8
	)

	errLog := &capturedErrorLog{}
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
			Logger:                       errLog.logger(),
			BlockPipelineValidateEnabled: true,
		},
	}
	ls.metrics.init(prometheus.NewRegistry())
	ls.publishSnapshotsLocked()

	// held gates slot 0 (the first block submitted, and therefore the
	// pipeline item with sequence number 0) inside the validate stage.
	held := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(held) }) }
	// othersDone fires once every other block in the batch has been through
	// the provider, which is the point at which all of them are queued on
	// validatedChan ahead of slot 0.
	othersDone := make(chan struct{})
	var others atomic.Int64
	eta0Provider := func(slot uint64) (string, error) {
		if slot != 0 {
			if others.Add(1) == numBlocks-1 {
				close(othersDone)
			}
			return ls.blockPipelineEta0Provider(slot)
		}
		select {
		case <-held:
		case <-t.Context().Done():
		}
		return ls.blockPipelineEta0Provider(slot)
	}

	ls.blockPipeline = pipeline.NewBlockPipeline(
		pipeline.WithDecodeWorkers(2),
		pipeline.WithValidateWorkers(2),
		pipeline.WithEta0Provider(eta0Provider),
		pipeline.WithSlotsPerKesPeriod(129600),
		pipeline.WithVerifyConfig(productionValidateVerifyConfig),
		pipeline.WithMaxPendingBlocks(maxPending),
	)
	require.NoError(t, ls.blockPipeline.Start(t.Context()))
	ls.blockPipelineErrorsDone = make(chan struct{})
	go ls.drainBlockPipelineErrors()
	stopAndDrain := stopAndDrainBlockPipeline(t, ls)
	defer stopAndDrain()
	// Registered after the teardown above so it runs before it: Stop()
	// waits for the validate workers, and a failure anywhere below would
	// otherwise leave one of them parked in the provider forever, turning a
	// legible assertion failure into a package timeout.
	defer release()

	rawBatch := buildNoNonceValidateBatch(t, numBlocks)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = ls.decodeReadChainBatch(t.Context(), rawBatch)
	}()

	testutil.RequireReceive(
		t,
		othersDone,
		30*time.Second,
		"every block after the first should have reached the validate "+
			"stage while the first one was held there",
	)
	release()
	testutil.RequireReceive(
		t,
		done,
		30*time.Second,
		"decodeReadChainBatch did not finish after the held block was "+
			"released",
	)
	stopAndDrain()

	require.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.blockPipelineUnexpectedErrors),
		"an apply-stage pending-buffer overflow is backpressure, not an "+
			"unexpected pipeline error; ERROR log records captured from "+
			"recordBlockPipelineError:%s",
		errLog.summary(),
	)
	// Sequence 0 is released only once every other item has been through
	// the provider, so the apply stage buffers essentially the whole batch
	// before it can apply anything and the overflow signal fires for every
	// item after the maxPending'th. The count is bounded on both sides
	// rather than fixed: the gate observes items entering the provider, and
	// the one item still inside the validate worker when it fires may be
	// enqueued on validatedChan either side of the released sequence 0,
	// which moves the total by exactly one.
	pendingLimitErrors := promtestutil.ToFloat64(
		ls.metrics.blockPipelineApplyPendingLimitErrors,
	)
	require.GreaterOrEqual(
		t,
		pendingLimitErrors,
		float64(numBlocks-2-maxPending),
		"every out-of-order block buffered past MaxPendingBlocks should be "+
			"counted as apply-stage backpressure",
	)
	require.LessOrEqual(
		t,
		pendingLimitErrors,
		float64(numBlocks-1-maxPending),
		"no block before the maxPending'th should be counted as apply-stage "+
			"backpressure",
	)
	require.Equal(
		t,
		float64(numBlocks),
		promtestutil.ToFloat64(ls.metrics.blockPipelineExpectedEta0Errors),
		"the eta0-unavailable classification must be unaffected",
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
	t.Parallel()

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
	t.Parallel()

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

// TestRecordBlockPipelineErrorClassificationPendingLimitIsNotUnexpected
// pins the classification of pipeline.ErrPendingLimitExceeded on its own,
// independently of the concurrency needed to provoke it: the apply stage
// buffers the item regardless and applies it in sequence, so the signal
// reports stage-worker scheduling lag and must not be counted or logged
// alongside genuine decode/validate/apply failures.
func TestRecordBlockPipelineErrorClassificationPendingLimitIsNotUnexpected(
	t *testing.T,
) {
	t.Parallel()

	ls := &LedgerState{
		config: LedgerStateConfig{Logger: testLogger()},
	}
	ls.metrics.init(prometheus.NewRegistry())

	ls.recordBlockPipelineError(pipeline.ErrPendingLimitExceeded)
	require.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(
			ls.metrics.blockPipelineApplyPendingLimitErrors,
		),
		"an apply-stage pending-buffer overflow must count as its own "+
			"backpressure case",
	)
	require.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.blockPipelineUnexpectedErrors),
		"an apply-stage pending-buffer overflow must not count as unexpected",
	)
	require.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.blockPipelineExpectedEta0Errors),
	)
	require.Zero(
		t,
		promtestutil.ToFloat64(
			ls.metrics.blockPipelineDeferredEpochCacheErrors,
		),
	)

	// Wrapped by an intermediate layer, it must still classify the same way.
	ls.recordBlockPipelineError(
		fmt.Errorf("apply stage: %w", pipeline.ErrPendingLimitExceeded),
	)
	require.Equal(
		t,
		float64(2),
		promtestutil.ToFloat64(
			ls.metrics.blockPipelineApplyPendingLimitErrors,
		),
	)
	require.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.blockPipelineUnexpectedErrors),
	)

	// The neighbouring apply-stage invariant violation is a genuine
	// problem and must still land in the unexpected bucket.
	ls.recordBlockPipelineError(pipeline.ErrBlockNotValidated)
	require.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(ls.metrics.blockPipelineUnexpectedErrors),
	)
	require.Equal(
		t,
		float64(2),
		promtestutil.ToFloat64(
			ls.metrics.blockPipelineApplyPendingLimitErrors,
		),
	)
}

// TestRecordBlockPipelineErrorClassificationShutdownIsNotUnexpected pins the
// classification of a stage worker's own context cancellation.
// BlockPipeline.Stop cancels the pipeline context before it drains the
// stages, and StageWorkerPool.worker chooses between sending its error and
// returning on ctx.Done() with an unbiased select, so a worker that is
// mid-item when shutdown begins can put context.Canceled on errorsChan
// instead of its item's outcome. That happens on any shutdown with blocks
// still in flight; counting it as unexpected made every such shutdown log at
// ERROR level and move a counter documented as meaning a decode, validation,
// or apply problem.
//
// The occurrence itself is a race inside gouroboros' worker pool and cannot
// be forced from here, so this test pins the classification directly. The
// deferred and eta0 cases above are pinned the same way.
func TestRecordBlockPipelineErrorClassificationShutdownIsNotUnexpected(
	t *testing.T,
) {
	t.Parallel()

	ls := &LedgerState{
		config: LedgerStateConfig{Logger: testLogger()},
	}
	ls.metrics.init(prometheus.NewRegistry())

	ls.recordBlockPipelineError(context.Canceled)
	ls.recordBlockPipelineError(
		fmt.Errorf("validate stage: %w", context.DeadlineExceeded),
	)
	require.Equal(
		t,
		float64(2),
		promtestutil.ToFloat64(ls.metrics.blockPipelineShutdownErrors),
		"a stage worker's own context cancellation must count as its own "+
			"shutdown case",
	)
	require.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.blockPipelineUnexpectedErrors),
		"a shutdown cancellation must not count as unexpected",
	)
	require.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.blockPipelineExpectedEta0Errors),
	)
	require.Zero(
		t,
		promtestutil.ToFloat64(
			ls.metrics.blockPipelineApplyPendingLimitErrors,
		),
	)

	// A genuine failure must still land in the unexpected bucket.
	ls.recordBlockPipelineError(errors.New("failed to decode block: bad cbor"))
	require.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(ls.metrics.blockPipelineUnexpectedErrors),
	)
	require.Equal(
		t,
		float64(2),
		promtestutil.ToFloat64(ls.metrics.blockPipelineShutdownErrors),
	)
}
