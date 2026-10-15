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
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
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
// has no cached Praos nonce to verify against, so decodeReadChainBatch skips
// its header check. Byron epochs always have that shape, and a later-era
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

// buildUndecodableBatch returns numBlocks blocks whose CBOR cannot be decoded,
// so each one makes the pipeline's decode stage report an error.
func buildUndecodableBatch(numBlocks int) []models.Block {
	batch := make([]models.Block, 0, numBlocks)
	for i := range numBlocks {
		batch = append(batch, models.Block{
			Slot:   uint64(i),
			Number: uint64(i + 1), //nolint:gosec // test slot count is small
			Type:   gledger.BlockTypeConway,
			Cbor:   []byte{0xff},
		})
	}
	return batch
}

// TestDecodeReadChainBatchDoesNotDeadlockOnManyDecodeErrors guards against a
// batch of more than PrefetchBufferSize (1000) undecodable blocks stalling
// decodeReadChainBatch. gouroboros' pipeline.StageWorkerPool.worker reports
// each stage error on a fixed-size errorsChan and drops it when the channel
// is full, so an undrained channel loses errors instead of blocking workers.
// drainBlockPipelineErrors (started here as Start() starts it for a real
// LedgerState) reads that channel for the pipeline's lifetime. The test
// asserts decodeReadChainBatch returns within a bounded timeout and that
// drained errors are counted.
func TestDecodeReadChainBatchDoesNotDeadlockOnManyDecodeErrors(
	t *testing.T,
) {
	t.Parallel()

	// A full deadlock needs more than just >1000 failing items: it needs
	// enough to fill errorsChan (1000) and jam every decode worker mid-item,
	// then fill submitChan (1000) behind the now-stuck decode stage, before
	// Submit() itself blocks forever. 4000 clears that with margin.
	const numBlocks = 4000

	ls, errLog, stopAndDrain := newNoNonceValidatePipelineLedger(
		t,
		// Submit waits once MaxPendingBlocks sequences are outstanding past
		// the last completed one, and completion stalls while nothing reads
		// Results(), so the default 2160 would block Submit before errorsChan
		// could fill. Disabling it leaves errorsChan as the only thing that
		// can stop this batch.
		pipeline.WithMaxPendingBlocks(0),
	)
	defer stopAndDrain()

	rawBatch := buildUndecodableBatch(numBlocks)

	done := make(chan struct{})
	go func() {
		defer close(done)
		// The return value is intentionally ignored: the batch fails on its
		// first undecodable block but still drains every remaining result,
		// and the point is that all numBlocks Submit() calls completed.
		_, _ = ls.decodeReadChainBatch(t.Context(), rawBatch)
	}()

	testutil.RequireReceive(
		t,
		done,
		testutil.AsyncWait,
		"decodeReadChainBatch deadlocked submitting undecodable blocks "+
			"whose errors were never drained from errorsChan",
	)

	// Stopping the pipeline closes errorsChan, which ends the drain's range
	// loop, so waiting for it is the one definite end-of-drain point: every
	// error the batch produced has been classified and counted by the time
	// the assertions below run.
	stopAndDrain()

	// The pipeline reports worker errors without blocking and drops one when
	// errorsChan is full, so the count is best effort: at least the first
	// buffered errors are counted, and never more than were produced.
	unexpected := promtestutil.ToFloat64(
		ls.metrics.blockPipelineUnexpectedErrors,
	)
	require.Positive(
		t,
		unexpected,
		"drained decode errors should have been counted; ERROR log "+
			"records captured from recordBlockPipelineError:%s",
		errLog.summary(),
	)
	require.LessOrEqual(t, unexpected, float64(numBlocks))
	require.Zero(
		t,
		promtestutil.ToFloat64(ls.metrics.blockPipelineExpectedEta0Errors),
		"a decode error is not an eta0-unavailable error",
	)
}

// newNoNonceValidatePipelineLedger returns a LedgerState whose started block
// pipeline, built with opts appended to the defaults, validates blocks
// against an epoch with no cached Praos nonce, so every eta0 lookup fails.
// It also returns the captured error log and an idempotent function that
// stops the pipeline and waits for its error drain; see
// stopAndDrainBlockPipeline.
func newNoNonceValidatePipelineLedger(
	t *testing.T,
	opts ...pipeline.PipelineOption,
) (*LedgerState, *capturedErrorLog, func()) {
	t.Helper()
	errLog := &capturedErrorLog{}
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				LengthInSlots: 432000,
				Nonce:         nil,
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
		append([]pipeline.PipelineOption{
			pipeline.WithDecodeWorkers(2),
			pipeline.WithTrustedDecodeOnly(),
		}, opts...)...,
	)
	require.NoError(t, ls.blockPipeline.Start(t.Context()))
	ls.blockPipelineErrorsDone = make(chan struct{})
	go ls.drainBlockPipelineErrors()
	return ls, errLog, stopAndDrainBlockPipeline(t, ls)
}

type decodeBatchResult struct {
	decoded []gledger.Block
	err     error
}

// goDecodeBatch runs decodeReadChainBatchWithError in a goroutine and
// returns the channel its result arrives on.
func goDecodeBatch(
	t *testing.T,
	ls *LedgerState,
	rawBatch []models.Block,
) <-chan decodeBatchResult {
	t.Helper()
	done := make(chan decodeBatchResult, 1)
	go func() {
		decoded, err := ls.decodeReadChainBatchWithError(t.Context(), rawBatch)
		done <- decodeBatchResult{decoded: decoded, err: err}
	}()
	return done
}

// TestDecodeReadChainBatchBatchLargerThanMaxPendingBlocks submits more
// validation-failing blocks than the pipeline's default MaxPendingBlocks.
// Submit waits for completions that only happen once Results() is consumed,
// so decodeReadChainBatch must read results while it is still submitting
// rather than after the whole batch is in.
func TestDecodeReadChainBatchBatchLargerThanMaxPendingBlocks(
	t *testing.T,
) {
	t.Parallel()

	const numBlocks = pipeline.DefaultMaxPendingBlocks * 2

	ls, _, stopAndDrain := newNoNonceValidatePipelineLedger(t)
	defer stopAndDrain()

	res := testutil.RequireReceive(
		t,
		goDecodeBatch(t, ls, buildNoNonceValidateBatch(t, numBlocks)),
		testutil.AsyncWait,
		"decodeReadChainBatch blocked in Submit on a batch larger than "+
			"MaxPendingBlocks while Results() was unread",
	)
	// Returning is not enough: a Submit failure after a prefix also returns.
	// Every block's eta0 lookup fails as deferred, so the whole batch must
	// come back decoded.
	require.NoError(t, res.err)
	require.Len(t, res.decoded, numBlocks)
}

// TestDecodeReadChainBatchSubmitErrorDrainsSubmittedResults fails Submit
// after a prefix of the batch went in. The prefix's results must be read off
// the shared results channel before the error is returned; otherwise the next
// call would take them for its own submissions.
func TestDecodeReadChainBatchSubmitErrorDrainsSubmittedResults(
	t *testing.T,
) {
	t.Parallel()

	const submitOK = 5
	errSubmit := errors.New("injected submit failure")

	ls, _, stopAndDrain := newNoNonceValidatePipelineLedger(t)
	defer stopAndDrain()

	calls := 0
	ls.blockPipelineSubmit = func(
		ctx context.Context,
		blockType uint,
		rawCbor []byte,
		tip ocommon.Tip,
	) error {
		calls++
		if calls > submitOK {
			return errSubmit
		}
		return ls.blockPipeline.Submit(ctx, blockType, rawCbor, tip)
	}

	res := testutil.RequireReceive(
		t,
		goDecodeBatch(t, ls, buildNoNonceValidateBatch(t, submitOK+3)),
		testutil.AsyncWait,
		"decodeReadChainBatch did not return after a Submit error",
	)
	require.ErrorIs(t, res.err, errSubmit)
	require.Nil(t, res.decoded)

	// A fresh submission must come back as the first result: anything left
	// over from the failed batch would be read here instead.
	probe := buildNoNonceValidateBatch(t, submitOK+1)[submitOK]
	require.NoError(t, ls.blockPipeline.Submit(
		t.Context(),
		probe.Type,
		probe.Cbor,
		ocommon.Tip{
			Point:       ocommon.NewPoint(probe.Slot, probe.Hash),
			BlockNumber: probe.Number,
		},
	))
	item := testutil.RequireReceive(
		t,
		ls.blockPipeline.Results(),
		testutil.AsyncWait,
		"probe block result",
	)
	require.NoError(t, item.DecodeError())
	require.Equal(t, probe.Slot, item.Block().SlotNumber())
}

// TestDecodeReadChainBatchSubmitErrorDrainTimesOut fails Submit after a
// prefix whose results never arrive, as when a fatal apply-stage error
// cancels the pipeline without closing Results(). The drain of that prefix
// must give up after CloseBlockPipelineDrainTimeout instead of blocking.
//
// Not t.Parallel: shrinks the package-level CloseBlockPipelineDrainTimeout.
func TestDecodeReadChainBatchSubmitErrorDrainTimesOut(t *testing.T) {
	orig := CloseBlockPipelineDrainTimeout
	CloseBlockPipelineDrainTimeout = 50 * time.Millisecond
	t.Cleanup(func() { CloseBlockPipelineDrainTimeout = orig })

	errSubmit := errors.New("injected submit failure")
	ls, _, stopAndDrain := newNoNonceValidatePipelineLedger(t)
	defer stopAndDrain()

	calls := 0
	ls.blockPipelineSubmit = func(
		context.Context,
		uint,
		[]byte,
		ocommon.Tip,
	) error {
		calls++
		if calls > 1 {
			return errSubmit
		}
		// Reported as submitted, but nothing reaches the pipeline, so its
		// result never arrives.
		return nil
	}

	res := testutil.RequireReceive(
		t,
		goDecodeBatch(t, ls, buildNoNonceValidateBatch(t, 3)),
		testutil.AsyncWait,
		"drain after a Submit error did not time out",
	)
	require.ErrorIs(t, res.err, errSubmit)
	require.ErrorContains(
		t,
		res.err,
		"drain submitted block pipeline results: timeout",
	)
	require.Nil(t, res.decoded)
}

// TestDecodeReadChainBatchJoinsSubmitterOnReturn returns from the batch call
// while its submitting goroutine is blocked inside Submit, by stopping the
// pipeline (which closes Results()). The call must cancel the submit context
// and not return until the goroutine has exited.
func TestDecodeReadChainBatchJoinsSubmitterOnReturn(t *testing.T) {
	t.Parallel()

	ls, _, stopAndDrain := newNoNonceValidatePipelineLedger(t)
	defer stopAndDrain()

	entered := make(chan struct{})
	proceed := make(chan struct{})
	var exited atomic.Bool
	var proceedOnce sync.Once
	release := func() { proceedOnce.Do(func() { close(proceed) }) }
	defer release()
	ls.blockPipelineSubmit = func(
		ctx context.Context,
		_ uint,
		_ []byte,
		_ ocommon.Tip,
	) error {
		close(entered)
		<-ctx.Done() // only returns if the batch call cancels its context
		<-proceed
		exited.Store(true)
		return ctx.Err()
	}

	done := goDecodeBatch(t, ls, buildNoNonceValidateBatch(t, 3))
	testutil.RequireReceive(
		t,
		entered,
		testutil.AsyncWait,
		"submitter did not start",
	)
	stopAndDrain()

	// The submitter is parked until proceed is closed, so a call that
	// returns now has not joined it.
	testutil.RequireNoReceive(
		t,
		done,
		200*time.Millisecond,
		"decodeReadChainBatch returned before its submitter exited",
	)
	release()
	res := testutil.RequireReceive(
		t,
		done,
		testutil.AsyncWait,
		"decodeReadChainBatch did not cancel its submit context",
	)
	require.Error(t, res.err)
	require.True(t, exited.Load(), "submitter must have exited at return")
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
				testutil.AsyncWait,
				"drainBlockPipelineErrors did not exit after Stop",
			)
		})
	}
}

// TestRecordBlockPipelineErrorClassificationDeferredIsNotUnexpected is a
// regression test for an observability gap:
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

	// A properly %w-wrapped sentinel must classify as expected regardless of
	// additional wrapping layers (e.g. gouroboros' own "eta0 provider error
	// for slot %d: %w").
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

// TestRecordBlockPipelineErrorClassificationPendingLimitIsUnexpected pins
// pipeline.ErrPendingLimitExceeded as unexpected: the apply stage drops the
// block and cancels the pipeline when it reports it.
func TestRecordBlockPipelineErrorClassificationPendingLimitIsUnexpected(
	t *testing.T,
) {
	t.Parallel()

	ls := &LedgerState{
		config: LedgerStateConfig{Logger: testLogger()},
	}
	ls.metrics.init(prometheus.NewRegistry())

	ls.recordBlockPipelineError(
		fmt.Errorf("apply stage: %w", pipeline.ErrPendingLimitExceeded),
	)
	require.Equal(
		t,
		float64(1),
		promtestutil.ToFloat64(ls.metrics.blockPipelineUnexpectedErrors),
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
