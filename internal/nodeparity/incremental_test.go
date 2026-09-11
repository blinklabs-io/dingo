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
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	pcommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/internal/test/testutil"
)

// TestLoadCursor_MissingFileReturnsNilNotError covers the expected-first-run
// shape: RunIncremental always establishes its own baseline via a full
// Check regardless of a prior cursor (see its doc comment), so a caller that
// has never run before must see (nil, nil) here, not an error, to tell that
// apart from a real read/parse failure it should actually report.
func TestLoadCursor_MissingFileReturnsNilNotError(t *testing.T) {
	t.Parallel()
	cursor, err := LoadCursor(filepath.Join(t.TempDir(), "does-not-exist.json"))
	require.NoError(t, err)
	assert.Nil(t, cursor)
}

// TestSaveCursorLoadCursor_RoundTrips covers the persistence contract
// RunIncremental relies on to resume across restarts: whatever SaveCursor
// writes, LoadCursor must read back unchanged.
func TestSaveCursorLoadCursor_RoundTrips(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "cursor.json")
	want := &IncrementalCursor{
		Tip:                  Tip{Slot: 123, Hash: "abcd", BlockNumber: 45},
		Epoch:                7,
		BlocksSinceFullCheck: 89,
	}
	require.NoError(t, SaveCursor(path, want))

	got, err := LoadCursor(path)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, want, got)
}

// TestSaveCursor_OverwritesPriorContent covers the case RunIncremental's
// steady-state loop actually exercises every block: saving a second cursor
// to the same path must fully replace the first, not merge or append --
// LoadCursor afterward must see only the latest write.
func TestSaveCursor_OverwritesPriorContent(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "cursor.json")
	require.NoError(t, SaveCursor(path, &IncrementalCursor{
		Tip: Tip{Slot: 1}, Epoch: 1, BlocksSinceFullCheck: 1,
	}))
	require.NoError(t, SaveCursor(path, &IncrementalCursor{
		Tip: Tip{Slot: 2}, Epoch: 2, BlocksSinceFullCheck: 2,
	}))

	got, err := LoadCursor(path)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, uint64(2), got.Tip.Slot)
	assert.Equal(t, 2, got.Epoch)
}

// txIn builds a real ShelleyTransactionInput for a fixed, deterministic
// hash+index pair, matching how queryUTxOByRefs/blockRefsToQuery build the
// same type from a real block's decoded transactions.
func txIn(t *testing.T, hash string, idx int) lcommon.TransactionInput {
	t.Helper()
	return shelley.NewShelleyTransactionInput(hash, idx)
}

// txHashA and txHashB are two distinct, well-formed 64-hex-character
// (32-byte) transaction hashes for test fixtures.
var (
	txHashA = strings.Repeat("11", 32)
	txHashB = strings.Repeat("22", 32)
)

// TestBlockRefsToQuery_DedupesRepeatedRefs covers the defensive dedup
// blockRefsToQuery's doc comment describes: the same ref named by both a
// consumed input and a produced output (not possible for a single real TxIn,
// but kept defensive against two transactions in the same block both naming
// it) must appear exactly once in the resulting query list.
func TestBlockRefsToQuery_DedupesRepeatedRefs(t *testing.T) {
	t.Parallel()
	shared := txIn(t, txHashA, 0)
	refs := blockRefsToQuery(
		[]lcommon.TransactionInput{shared, shared},
		[]lcommon.Utxo{{Id: shared}},
	)
	assert.Len(
		t,
		refs,
		1,
		"the same ref named three times must be queried once",
	)
}

// TestBlockRefsToQuery_DistinctRefsAllIncluded covers the ordinary case:
// distinct consumed and produced refs must all appear in the query list,
// none silently dropped.
func TestBlockRefsToQuery_DistinctRefsAllIncluded(t *testing.T) {
	t.Parallel()
	consumed := txIn(t, txHashA, 0)
	produced := txIn(t, txHashB, 1)
	refs := blockRefsToQuery(
		[]lcommon.TransactionInput{consumed},
		[]lcommon.Utxo{{Id: produced}},
	)
	assert.Len(t, refs, 2)
}

// TestDiffBlockUtxoDelta_MatchingDeltaIsEmpty covers the clean-match case:
// a consumed ref absent from both nodes' answers (correctly spent) and a
// produced ref present with identical content on both must report no
// divergence lines at all.
func TestDiffBlockUtxoDelta_MatchingDeltaIsEmpty(t *testing.T) {
	t.Parallel()
	consumedRef := txIn(t, txHashA, 0)
	producedRef := txIn(t, txHashB, 0)
	consumed := []lcommon.TransactionInput{consumedRef}
	produced := []lcommon.Utxo{{Id: producedRef}}

	key := producedRef.Id().String() + "#0"
	entries := map[string]string{key: "addr1|1000000"}

	lines := diffBlockUtxoDelta(consumed, produced, entries, entries)
	assert.Empty(t, lines)
}

// TestDiffBlockUtxoDelta_ConsumedStillPresentIsReported covers the
// "should be spent but wasn't" case for each node independently: a consumed
// ref still showing up in dingo's answer, cardano-node's answer, or both,
// must be reported, naming which node(s) still have it live.
func TestDiffBlockUtxoDelta_ConsumedStillPresentIsReported(t *testing.T) {
	t.Parallel()
	consumedRef := txIn(t, txHashA, 0)
	key := consumedRef.Id().String() + "#0"
	consumed := []lcommon.TransactionInput{consumedRef}

	t.Run("still present in dingo only", func(t *testing.T) {
		t.Parallel()
		lines := diffBlockUtxoDelta(
			consumed, nil,
			map[string]string{key: "addr1|1000000"},
			map[string]string{},
		)
		require.Len(t, lines, 1)
		assert.Contains(t, lines[0], "still present in dingo")
	})

	t.Run("still present in cardano-node only", func(t *testing.T) {
		t.Parallel()
		lines := diffBlockUtxoDelta(
			consumed, nil,
			map[string]string{},
			map[string]string{key: "addr1|1000000"},
		)
		require.Len(t, lines, 1)
		assert.Contains(t, lines[0], "still present in cardano-node")
	})

	t.Run("still present in both", func(t *testing.T) {
		t.Parallel()
		lines := diffBlockUtxoDelta(
			consumed, nil,
			map[string]string{key: "addr1|1000000"},
			map[string]string{key: "addr1|1000000"},
		)
		assert.Len(
			t,
			lines,
			2,
			"both nodes still holding a spent ref live must produce two lines, one per node",
		)
	})
}

// TestDiffBlockUtxoDelta_ProducedMismatchIsReported covers every way a
// produced output can fail to match: missing from dingo, missing from
// cardano-node (the reference), missing from both, and present in both with
// different content.
func TestDiffBlockUtxoDelta_ProducedMismatchIsReported(t *testing.T) {
	t.Parallel()
	producedRef := txIn(t, txHashB, 0)
	produced := []lcommon.Utxo{{Id: producedRef}}
	key := producedRef.Id().String() + "#0"

	cases := []struct {
		name          string
		dingo         map[string]string
		cardano       map[string]string
		wantSubstring string
	}{
		{
			name:          "missing from dingo",
			dingo:         map[string]string{},
			cardano:       map[string]string{key: "addr1|1000000"},
			wantSubstring: "present in cardano-node, missing in dingo",
		},
		{
			name:          "missing from cardano-node",
			dingo:         map[string]string{key: "addr1|1000000"},
			cardano:       map[string]string{},
			wantSubstring: "present in dingo, missing in cardano-node",
		},
		{
			name:          "missing from both",
			dingo:         map[string]string{},
			cardano:       map[string]string{},
			wantSubstring: "missing from both",
		},
		{
			name:          "content differs",
			dingo:         map[string]string{key: "addr1|1000000"},
			cardano:       map[string]string{key: "addr1|2000000"},
			wantSubstring: "differs",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lines := diffBlockUtxoDelta(nil, produced, tc.dingo, tc.cardano)
			require.Len(t, lines, 1)
			assert.Contains(t, lines[0], tc.wantSubstring)
		})
	}
}

// TestDiffBlockUtxoDelta_IntraBlockSpendIsNotReported is a regression test
// for blinklabs-io/dingo#1900's incremental-mode audit finding: an output
// created by one transaction and spent by a later transaction in the same
// block correctly does not appear in either node's live UTxO query result --
// that is the expected outcome of a real, valid intra-block spend, not a
// divergence. Before the fix, the produced loop flagged any produced-but-
// absent output as "missing from both dingo and cardano-node" without first
// checking whether that same ref was also consumed within this block.
func TestDiffBlockUtxoDelta_IntraBlockSpendIsNotReported(t *testing.T) {
	t.Parallel()
	ref := txIn(t, txHashA, 0)
	consumed := []lcommon.TransactionInput{ref}
	produced := []lcommon.Utxo{{Id: ref}}

	lines := diffBlockUtxoDelta(
		consumed, produced, map[string]string{}, map[string]string{},
	)
	assert.Empty(
		t,
		lines,
		"an output created and spent within the same block must not be reported as a divergence just because it is absent from both nodes' live answers",
	)
}

// TestDiffBlockUtxoDelta_ProducedThenAbsentIsStillReportedWhenNotConsumed
// covers the case TestDiffBlockUtxoDelta_IntraBlockSpendIsNotReported's fix
// must not have broken: a produced ref that is genuinely missing from both
// nodes, and was NOT also consumed within this same block, is still a real
// divergence and must still be reported -- see
// TestDiffBlockUtxoDelta_ProducedMismatchIsReported's own "missing from
// both" case for the non-regression-specific version of this; this test
// exists specifically to prove the new intra-block check does not
// over-suppress unrelated produced refs.
func TestDiffBlockUtxoDelta_ProducedThenAbsentIsStillReportedWhenNotConsumed(
	t *testing.T,
) {
	t.Parallel()
	producedRef := txIn(t, txHashB, 0)
	unrelatedConsumedRef := txIn(t, txHashA, 0)

	lines := diffBlockUtxoDelta(
		[]lcommon.TransactionInput{unrelatedConsumedRef},
		[]lcommon.Utxo{{Id: producedRef}},
		map[string]string{},
		map[string]string{},
	)
	require.Len(t, lines, 1)
	assert.Contains(t, lines[0], "missing from both")
}

// TestDiffBlockUtxoDelta_DedupesRepeatedProducedRef covers the same
// defensive dedup as blockRefsToQuery, but for the diff side: the same
// produced ref appearing twice (e.g. if a caller failed to dedup upstream)
// must still only be reported once, not doubled.
func TestDiffBlockUtxoDelta_DedupesRepeatedProducedRef(t *testing.T) {
	t.Parallel()
	producedRef := txIn(t, txHashB, 0)
	lines := diffBlockUtxoDelta(
		nil,
		[]lcommon.Utxo{{Id: producedRef}, {Id: producedRef}},
		map[string]string{},
		map[string]string{},
	)
	assert.Len(t, lines, 1)
}

// TestRunIncremental_RequiresPositiveFullCheckInterval and
// TestRunIncremental_RequiresCursorFile cover RunIncremental's own
// validation, ahead of ever dialing anything -- a caller that got past
// cmd/node-parity's flag validation by constructing IncrementalConfig
// directly (e.g. a future embedder of this package) must not be able to
// start a loop that can never checkpoint or resume.
func TestRunIncremental_RequiresPositiveFullCheckInterval(t *testing.T) {
	t.Parallel()
	err := RunIncremental(context.Background(), IncrementalConfig{
		DingoAddr:   "127.0.0.1:1",
		CardanoAddr: "127.0.0.1:1",
		CursorFile:  filepath.Join(t.TempDir(), "cursor.json"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "FullCheckInterval must be positive")
}

func TestRunIncremental_RequiresCursorFile(t *testing.T) {
	t.Parallel()
	err := RunIncremental(context.Background(), IncrementalConfig{
		DingoAddr:         "127.0.0.1:1",
		CardanoAddr:       "127.0.0.1:1",
		FullCheckInterval: 1000,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CursorFile is required")
}

// TestHandleIncrementalRollback_NoOpWhenPointMatchesCursor is a regression
// test for a bug caught live against a real node: the ChainSync protocol
// reports a RollBackward to the exact intersection point as the first
// message after any Sync/FindIntersect (confirming the negotiated reading
// position), not just on a genuine reorg. Treating every RollBackward as a
// real rollback fired a full checkpoint on every single session start
// (startup and every reconnect) at zero information gain, since the
// reported point was the cursor's own already-trusted point. This must be a
// pure no-op: cfg.OnFullCheck must never be called, and the cursor must be
// left exactly as it was.
func TestHandleIncrementalRollback_NoOpWhenPointMatchesCursor(t *testing.T) {
	t.Parallel()
	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	cursor := newCursorState(cursorFile, IncrementalCursor{
		Tip: Tip{Slot: 100, Hash: "abcd", BlockNumber: 10}, Epoch: 5,
	})
	cfg := IncrementalConfig{
		DingoAddr:   "127.0.0.1:1",
		CardanoAddr: "127.0.0.1:1",
		CursorFile:  cursorFile,
		Logger:      testDiscardLogger(),
	}
	// A bare worker, its background goroutine never started: this test
	// only asserts on whether handleIncrementalRollback enqueues a request,
	// not on a full check actually running -- see
	// TestFullCheckWorker_RunsRequestsOneAtATime for that.
	worker := &fullCheckWorker{
		cfg: cfg, cursor: cursor, pending: make(chan fullCheckRequest, 1),
	}
	hashBytes, err := hex.DecodeString("abcd")
	require.NoError(t, err)
	samePoint := pcommon.NewPoint(100, hashBytes)

	require.NoError(
		t,
		handleIncrementalRollback(cfg, cursor, worker, samePoint),
	)

	select {
	case <-worker.pending:
		t.Fatal(
			"a rollback to the cursor's own current point must not dispatch a full check",
		)
	default:
	}
	assert.Equal(
		t, Tip{Slot: 100, Hash: "abcd", BlockNumber: 10}, cursor.snapshot().Tip,
		"the cursor must be left unchanged by a no-op rollback",
	)
	_, statErr := os.Stat(cursorFile)
	assert.True(
		t,
		os.IsNotExist(statErr),
		"a no-op rollback must not even write the cursor file, since nothing changed",
	)
}

// TestHandleIncrementalRollback_TriggersFullCheckWhenPointDiffers covers the
// genuine-rollback case: a reported point that actually differs from the
// cursor's current one must reset the cursor to it, persist the new cursor,
// and dispatch a full check (FullCheckRollback) to the worker -- the real
// behavior TestHandleIncrementalRollback_NoOpWhenPointMatchesCursor's fix
// must not have broken while suppressing the false-positive case.
func TestHandleIncrementalRollback_TriggersFullCheckWhenPointDiffers(
	t *testing.T,
) {
	t.Parallel()
	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	cursor := newCursorState(cursorFile, IncrementalCursor{
		Tip: Tip{Slot: 100, Hash: "abcd", BlockNumber: 10}, Epoch: 5,
	})
	cfg := IncrementalConfig{
		DingoAddr:        "127.0.0.1:1",
		CardanoAddr:      "127.0.0.1:1",
		CursorFile:       cursorFile,
		FullCheckTimeout: 2 * time.Second,
		Logger:           testDiscardLogger(),
	}
	worker := &fullCheckWorker{
		cfg: cfg, cursor: cursor, pending: make(chan fullCheckRequest, 1),
	}
	olderHash, err := hex.DecodeString("ffff")
	require.NoError(t, err)
	rollbackPoint := pcommon.NewPoint(50, olderHash)

	require.NoError(
		t,
		handleIncrementalRollback(cfg, cursor, worker, rollbackPoint),
	)

	var req fullCheckRequest
	select {
	case req = <-worker.pending:
	default:
		t.Fatal("a genuine rollback must dispatch a full check request")
	}
	assert.Equal(t, FullCheckRollback, req.reason)
	assert.Equal(t, uint64(50), req.at.Slot)
	assert.Equal(t, "ffff", req.at.Hash)
	assert.Equal(t, uint64(50), cursor.snapshot().Tip.Slot)

	got, loadErr := LoadCursor(cursorFile)
	require.NoError(t, loadErr)
	require.NotNil(t, got)
	assert.Equal(t, uint64(50), got.Tip.Slot)
}

// TestFullCheckWorker_DropsRequestWhilePendingOneQueued covers the
// coalescing contract that keeps a fast-moving chain from queueing up
// redundant full checks behind a slow one: a second request while one is
// already pending must be dropped, not queued, leaving exactly the first
// request's reason waiting.
func TestFullCheckWorker_DropsRequestWhilePendingOneQueued(t *testing.T) {
	t.Parallel()
	w := &fullCheckWorker{pending: make(chan fullCheckRequest, 1)}
	w.request(FullCheckMismatch, Tip{Slot: 1})
	w.request(FullCheckInterval, Tip{Slot: 2})

	req := <-w.pending
	assert.Equal(
		t,
		FullCheckMismatch,
		req.reason,
		"the first request must win; the second must have been dropped, not queued",
	)
	select {
	case <-w.pending:
		t.Fatal(
			"a second request while one was already pending must be dropped",
		)
	default:
	}
}

// TestFullCheckWorker_FailedAttemptDoesNotResetCounter covers the worker's
// actual run loop end to end against unreachable addresses, so Check fails
// fast on a dial error rather than genuinely completing: a dispatched
// request must still reach cfg.OnFullCheck with the request's own reason,
// but BlocksSinceFullCheck must NOT be reset afterward, since no check
// actually completed. This is a regression test for
// blinklabs-io/dingo#1900's incremental-mode audit finding (a): resetting
// the countdown on every attempt regardless of outcome (the prior behavior)
// silently delayed the next legitimate interval checkpoint by up to a full
// --full-check-interval's worth of blocks even though nothing was ever
// confirmed. See TestFullCheckWorker_SuccessfulCheckResetsCounter for the
// case that must still reset it.
func TestFullCheckWorker_FailedAttemptDoesNotResetCounter(t *testing.T) {
	t.Parallel()
	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	cursor := newCursorState(cursorFile, IncrementalCursor{
		Tip:                  Tip{Slot: 100, Hash: "abcd", BlockNumber: 10},
		Epoch:                5,
		BlocksSinceFullCheck: 42,
	})

	var mu sync.Mutex
	var gotReason FullCheckReason
	var gotErr error
	called := make(chan struct{})
	cfg := IncrementalConfig{
		DingoAddr:        "127.0.0.1:1",
		CardanoAddr:      "127.0.0.1:1",
		CursorFile:       cursorFile,
		FullCheckTimeout: 5 * time.Second,
		Logger:           testDiscardLogger(),
		OnFullCheck: func(reason FullCheckReason, _ *CheckResult, err error) {
			mu.Lock()
			gotReason = reason
			gotErr = err
			mu.Unlock()
			close(called)
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	worker := startFullCheckWorker(ctx, cfg, cursor)
	worker.request(FullCheckMismatch, Tip{Slot: 200, Hash: "ef01"})

	select {
	case <-called:
	case <-time.After(10 * time.Second):
		t.Fatal("worker never ran the dispatched request")
	}
	mu.Lock()
	assert.Equal(t, FullCheckMismatch, gotReason)
	require.Error(
		t,
		gotErr,
		"unreachable addresses must make Check fail outright",
	)
	mu.Unlock()

	// A short, fixed wait rather than testutil.WaitForCondition: this test
	// asserts a state does NOT change, so it must observe the worker's own
	// goroutine settle rather than succeed immediately before that goroutine
	// had a chance to (incorrectly) reset the counter.
	time.Sleep(200 * time.Millisecond)
	assert.Equal(
		t, uint64(42), cursor.snapshot().BlocksSinceFullCheck,
		"a failed full-check attempt must not reset BlocksSinceFullCheck",
	)
	assert.Equal(
		t, uint64(100), cursor.snapshot().Tip.Slot,
		"a failed attempt must not disturb Tip either",
	)

	cancel()
	worker.stop()
}

// TestFullCheckWorker_SuccessfulCheckResetsCounter covers the case
// TestFullCheckWorker_FailedAttemptDoesNotResetCounter's fix must not have
// broken: a full check that actually completes (against the fake harness,
// which always accepts Acquire regardless of target -- see
// newIncrementalHarness) must still reset BlocksSinceFullCheck, without
// disturbing Tip/Epoch.
func TestFullCheckWorker_SuccessfulCheckResetsCounter(t *testing.T) {
	t.Parallel()
	dingoAddr, cardanoAddr, _, _ := newIncrementalHarness(t, 3)

	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	cursor := newCursorState(cursorFile, IncrementalCursor{
		Tip:                  Tip{Slot: 100, Hash: "abcd", BlockNumber: 10},
		Epoch:                5,
		BlocksSinceFullCheck: 42,
	})

	var mu sync.Mutex
	var gotErr error
	called := make(chan struct{})
	cfg := IncrementalConfig{
		DingoAddr:        dingoAddr,
		CardanoAddr:      cardanoAddr,
		Magic:            42,
		CursorFile:       cursorFile,
		FullCheckTimeout: 10 * time.Second,
		Logger:           testDiscardLogger(),
		OnFullCheck: func(_ FullCheckReason, _ *CheckResult, err error) {
			mu.Lock()
			gotErr = err
			mu.Unlock()
			close(called)
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	worker := startFullCheckWorker(ctx, cfg, cursor)
	worker.request(FullCheckInterval, Tip{Slot: 200, Hash: "ef01"})

	select {
	case <-called:
	case <-time.After(10 * time.Second):
		t.Fatal("worker never ran the dispatched request")
	}
	mu.Lock()
	require.NoError(
		t,
		gotErr,
		"the fake harness must let this full check actually complete",
	)
	mu.Unlock()

	testutil.WaitForCondition(t, func() bool {
		return cursor.snapshot().BlocksSinceFullCheck == 0
	}, 2*time.Second, "a completed full check must reset BlocksSinceFullCheck")
	assert.Equal(
		t, uint64(100), cursor.snapshot().Tip.Slot,
		"resetting the checkpoint counter must not disturb Tip",
	)

	cancel()
	worker.stop()
}

// TestFullCheckSucceeded covers fullCheckSucceeded's own decision in
// isolation: only a nil error and a non-nil, non-Skipped result counts as a
// completed check.
func TestFullCheckSucceeded(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		result *CheckResult
		err    error
		want   bool
	}{
		{"matched or diverged result, no error", &CheckResult{}, nil, true},
		{
			"query error, even with a result",
			&CheckResult{},
			assert.AnError,
			false,
		},
		{"nil result, no error", nil, nil, false},
		{"skipped result", &CheckResult{Skipped: true}, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, fullCheckSucceeded(tc.result, tc.err))
		})
	}
}

// TestCursorState_AdvanceIncrementsAndPersists covers the per-block
// bookkeeping path: advance must update Tip, increment
// BlocksSinceFullCheck, apply a non-negative epoch, leave a negative one
// (the epoch-lookup-failed signal) alone, and persist every change.
func TestCursorState_AdvanceIncrementsAndPersists(t *testing.T) {
	t.Parallel()
	cursorFile := filepath.Join(t.TempDir(), "cursor.json")
	cursor := newCursorState(cursorFile, IncrementalCursor{
		Tip: Tip{Slot: 1}, Epoch: 5, BlocksSinceFullCheck: 3,
	})

	got, err := cursor.advance(Tip{Slot: 2}, 6)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), got.Tip.Slot)
	assert.Equal(t, 6, got.Epoch)
	assert.Equal(t, uint64(4), got.BlocksSinceFullCheck)

	got, err = cursor.advance(Tip{Slot: 3}, -1)
	require.NoError(t, err)
	assert.Equal(
		t,
		6,
		got.Epoch,
		"a negative epoch (lookup failed) must not overwrite the last known one",
	)
	assert.Equal(t, uint64(5), got.BlocksSinceFullCheck)

	persisted, loadErr := LoadCursor(cursorFile)
	require.NoError(t, loadErr)
	require.NotNil(t, persisted)
	assert.Equal(t, got, *persisted)
}

// TestDecideFullCheckReason covers every trigger decideFullCheckReason
// makes, including the interval and epoch-transition paths that no live
// testnet run could exercise on its own (blinklabs-io/dingo#3854 makes a
// mismatch fire on effectively every block, always preempting the other
// two before their own conditions are ever reached) -- this is that
// coverage, independent of #3854 or any live network at all.
func TestDecideFullCheckReason(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name                 string
		diffEmpty            bool
		epoch, beforeEpoch   int
		blocksSinceFullCheck uint64
		fullCheckInterval    uint64
		wantReason           FullCheckReason
		wantDue              bool
	}{
		{
			name:      "clean block, interval not yet due: no trigger",
			diffEmpty: true, epoch: 5, beforeEpoch: 5,
			blocksSinceFullCheck: 3, fullCheckInterval: 1000,
			wantDue: false,
		},
		{
			name:      "mismatch fires regardless of everything else",
			diffEmpty: false, epoch: 5, beforeEpoch: 5,
			blocksSinceFullCheck: 0, fullCheckInterval: 1000,
			wantReason: FullCheckMismatch, wantDue: true,
		},
		{
			name:      "epoch transition fires on a clean block",
			diffEmpty: true, epoch: 6, beforeEpoch: 5,
			blocksSinceFullCheck: 3, fullCheckInterval: 1000,
			wantReason: FullCheckEpochTransition, wantDue: true,
		},
		{
			name:      "interval reached on a clean block, same epoch",
			diffEmpty: true, epoch: 5, beforeEpoch: 5,
			blocksSinceFullCheck: 1000, fullCheckInterval: 1000,
			wantReason: FullCheckInterval, wantDue: true,
		},
		{
			name:      "interval reached exactly at the boundary still fires",
			diffEmpty: true, epoch: 5, beforeEpoch: 5,
			blocksSinceFullCheck: 1000, fullCheckInterval: 1000,
			wantReason: FullCheckInterval, wantDue: true,
		},
		{
			name:      "interval past the boundary still fires (a missed exact match must not suppress it)",
			diffEmpty: true, epoch: 5, beforeEpoch: 5,
			blocksSinceFullCheck: 1001, fullCheckInterval: 1000,
			wantReason: FullCheckInterval, wantDue: true,
		},
		{
			name:      "mismatch outranks an epoch transition on the same block",
			diffEmpty: false, epoch: 6, beforeEpoch: 5,
			blocksSinceFullCheck: 3, fullCheckInterval: 1000,
			wantReason: FullCheckMismatch, wantDue: true,
		},
		{
			name:      "epoch transition outranks a due interval on the same block",
			diffEmpty: true, epoch: 6, beforeEpoch: 5,
			blocksSinceFullCheck: 1000, fullCheckInterval: 1000,
			wantReason: FullCheckEpochTransition, wantDue: true,
		},
		{
			name:      "mismatch outranks both an epoch transition and a due interval together",
			diffEmpty: false, epoch: 6, beforeEpoch: 5,
			blocksSinceFullCheck: 1000, fullCheckInterval: 1000,
			wantReason: FullCheckMismatch, wantDue: true,
		},
		{
			name:      "current epoch lookup failed (-1): never reports a transition",
			diffEmpty: true, epoch: -1, beforeEpoch: 5,
			blocksSinceFullCheck: 3, fullCheckInterval: 1000,
			wantDue: false,
		},
		{
			name:      "prior epoch unknown (-1, first block after baseline epoch lookup failed): never reports a transition",
			diffEmpty: true, epoch: 6, beforeEpoch: -1,
			blocksSinceFullCheck: 3, fullCheckInterval: 1000,
			wantDue: false,
		},
		{
			name:      "same epoch reported twice: not a transition",
			diffEmpty: true, epoch: 5, beforeEpoch: 5,
			blocksSinceFullCheck: 3, fullCheckInterval: 1000,
			wantDue: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reason, due := decideFullCheckReason(
				tc.diffEmpty, tc.epoch, tc.beforeEpoch,
				tc.blocksSinceFullCheck, tc.fullCheckInterval,
			)
			assert.Equal(t, tc.wantDue, due)
			if tc.wantDue {
				assert.Equal(t, tc.wantReason, reason)
			}
		})
	}
}

// testDiscardLogger returns a logger that writes nowhere, for tests that
// need a non-nil *slog.Logger but do not assert on its output.
func testDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestReportSessionEnd_RecordsErrorViaCallback is a regression test for
// blinklabs-io/dingo#1900's incremental-mode audit finding: a per-block
// query failure that ends an incrementalSession previously reached only a
// log line, never any metric-recording callback, so
// node_parity_check_errors_total could never see this failure class (the
// stake-distribution stall from the same audit's other finding would never
// have paged anyone). A non-nil sessionErr must reach OnBlockCheckError.
func TestReportSessionEnd_RecordsErrorViaCallback(t *testing.T) {
	t.Parallel()
	var got error
	cfg := IncrementalConfig{
		Logger:            testDiscardLogger(),
		OnBlockCheckError: func(err error) { got = err },
	}
	wantErr := errors.New("block delta check at slot 5: dingo query: boom")

	reportSessionEnd(cfg, wantErr, time.Second, 5)

	assert.Equal(t, wantErr, got)
}

// TestReportSessionEnd_NilErrorDoesNotInvokeCallback covers the clean-ending
// case reportSessionEnd's own caller already excludes in practice (ctx
// cancellation returns before this is ever called), but which this function
// itself still guards defensively: a nil sessionErr must not invoke
// OnBlockCheckError at all.
func TestReportSessionEnd_NilErrorDoesNotInvokeCallback(t *testing.T) {
	t.Parallel()
	called := false
	cfg := IncrementalConfig{
		Logger:            testDiscardLogger(),
		OnBlockCheckError: func(error) { called = true },
	}

	reportSessionEnd(cfg, nil, time.Second, 5)

	assert.False(
		t,
		called,
		"a nil session error must not invoke OnBlockCheckError",
	)
}
