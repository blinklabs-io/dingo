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
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/cbor"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/babbage"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	"github.com/blinklabs-io/gouroboros/ledger/shelley"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// leiosWaitTestSlotLen is the Shelley slot length these tests pin, so the
// slot-denominated diffusion window (EndorserBlockWaitSlots) converts to a
// wall-clock window short enough to assert on but long enough to separate
// "returned immediately" from "waited a window" without flaking.
const leiosWaitTestSlotLen = 20 * time.Millisecond

// leiosWaitTestWaitSlots matches the production default
// (CertifyByDeadlineSlots), so the window under test is
// leiosWaitTestWaitSlots * leiosWaitTestSlotLen = 400ms.
const leiosWaitTestWaitSlots = 20

const leiosWaitTestWindow = leiosWaitTestWaitSlots * leiosWaitTestSlotLen

// leiosWaitTestLongWaitSlots gives a 10s window, used by tests where the wait
// must be ended by something other than the deadline. It is far larger than
// any plausible scheduling delay on a loaded runner, so the deadline can never
// win the race against the event the test is actually exercising -- unlike a
// timer racing a short window, which is the classic flake in this file's
// shape. Nothing waits 10s: the wait ends as soon as that event fires.
const leiosWaitTestLongWaitSlots = 500

const leiosWaitTestLongWindow = leiosWaitTestLongWaitSlots * leiosWaitTestSlotLen

// withLeiosWaitTestSlotLength gives a bare-constructed LedgerState a Shelley
// slot length, which is what ensureReferencedEndorserBlocks converts the
// slot-denominated wait window with. Without it the wait is disabled outright
// and the timing assertions below would pass vacuously.
//
// It also pins the OTHER precondition every timing assertion in this file
// depends on: that the fixture's blocks are classified near-head rather than
// as settled backlog. classifyEndorserBlockFetches only calls a block
// historical when the wall-clock slot is KNOWN and more than
// EndorserBlockWaitSlots above it, and these fixtures deliberately leave
// ls.slotClock nil so CurrentSlot errors and wallKnown is false, which makes
// every block near-head no matter what slot the fixture uses. That is an
// invariant, not a coincidence: give one of these states a slot clock reading
// past the fixture slots and the near-head path stops being exercised --
// the shared-window test would skip the wait entirely (ls.leiosBackfill is
// nil) and the late-arrival test would reroute its closure to fetchRequired,
// so both would pass or fail for reasons unrelated to what they assert.
// Assert it here, once, where the fixture is established.
func withLeiosWaitTestSlotLength(t *testing.T, ls *LedgerState) {
	t.Helper()
	if _, err := ls.CurrentSlot(); err == nil {
		t.Fatal(
			"these tests require an unknown wall-clock slot so every " +
				"fixture block is classified near-head; a slot clock has " +
				"been added, so pin the fixture slots relative to it before " +
				"trusting any wait assertion in this file",
		)
	}
	ls.timeConverter = NewSlotTimeConverter(SlotTimeConverterDeps{
		ShelleyGenesis: func() *shelley.ShelleyGenesis {
			return &shelley.ShelleyGenesis{
				SlotLength: lcommon.GenesisRat{
					Rat: big.NewRat(
						int64(leiosWaitTestSlotLen/time.Millisecond),
						1000,
					),
				},
			}
		},
	})
	ls.timeConverterOnce.Do(func() {})
}

// leiosWaitTestAnnouncingBlock builds a Dijkstra ranking block that announces
// ebHash and certifies nothing.
func leiosWaitTestAnnouncingBlock(
	t *testing.T,
	blockNumber, slot uint64,
	ebHash lcommon.Blake2b256,
) *dijkstra.DijkstraBlock {
	t.Helper()
	return &dijkstra.DijkstraBlock{
		BlockHeader: &dijkstra.DijkstraBlockHeader{
			BabbageBlockHeader: babbage.BabbageBlockHeader{
				Body: babbage.BabbageBlockHeaderBody{
					BlockNumber: blockNumber,
					Slot:        slot,
				},
			},
			LeiosHeaderExtension: []cbor.RawMessage{
				leiosTestRaw(t, false),
				leiosTestRaw(t, []any{ebHash.Bytes(), uint64(4096)}),
			},
		},
	}
}

// TestEnsureReferencedEndorserBlocksDoesNotBlockOnUnreadAnnouncement is the
// apply-lag regression. On the Haskell-conformant (Musashi) path, ledger
// application of a ranking block reads only the certified closure announced by
// a certifying block's PARENT; a block's own announcement is never read when
// that block is applied. Blocking the single ledger pipeline on it stalled
// every block queued behind it for a whole diffusion window and then applied
// the block unchanged anyway, which is where the multi-second apply lag and
// the resulting stale-tip forge came from.
//
// Before the fix this returns after the full window; after it, immediately.
func TestEnsureReferencedEndorserBlocksDoesNotBlockOnUnreadAnnouncement(
	t *testing.T,
) {
	ebHash := lcommon.NewBlake2b256(leiosTestHash(0xA1))
	block := leiosWaitTestAnnouncingBlock(t, 1, 100, ebHash)

	var fetched atomic.Int64
	fetchedCh := make(chan struct{}, 1)
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			// The endorser block never arrives.
			return nil, false
		},
		EndorserBlockFetcher: func(
			_ context.Context,
			_ uint64,
			_ []byte,
		) error {
			fetched.Add(1)
			select {
			case fetchedCh <- struct{}{}:
			default:
			}
			return nil
		},
		EndorserBlockWaitSlots: leiosWaitTestWaitSlots,
		// Haskell-conformant path: application reads only certified closures.
		LeiosApplyEndorserBlockTxs: false,
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)
	withLeiosWaitTestSlotLength(t, ls)
	require.Equal(t, leiosWaitTestSlotLen, ls.shelleySlotLength())

	start := time.Now()
	require.NoError(t, ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{block},
	))
	elapsed := time.Since(start)
	require.Less(
		t,
		elapsed,
		leiosWaitTestWindow/2,
		"apply gate blocked on an announcement ledger application never reads",
	)

	// The announcement is prefetched in the background rather than dropped, so
	// it is cached before anything actually depends on it.
	select {
	case <-fetchedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("background by-point fetch was never dispatched")
	}
	require.Positive(t, fetched.Load())
}

// leiosWaitTestPolledFromWait reports whether the current call stack passes
// through waitForEndorserBlock, i.e. whether the endorser-block provider is
// being polled by the WAIT itself rather than by one of the gate's
// availability checks that run before the wait starts.
//
// A test that instead counts provider calls and flips availability on the Nth
// one is silently coupled to how many pre-wait checks the gate happens to
// make. If that count ever grows past the threshold, the endorser block
// becomes available before the wait is entered, the reference is treated as
// already cached, no wait happens at all -- and the test still passes, having
// stopped testing the thing it names. Keying on the caller instead makes the
// phase explicit and immune to that drift.
func leiosWaitTestPolledFromWait() bool {
	pcs := make([]uintptr, 64)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if strings.Contains(frame.Function, "waitForEndorserBlock") {
			return true
		}
		if !more {
			return false
		}
	}
}

// TestEnsureReferencedEndorserBlocksWaitsForCertifiedClosureArrivingLate pins
// the other half of the contract: a certifying ranking block's closure IS read
// at apply time and committing without it would permanently omit the endorser
// block's effects, so that wait is load-bearing and is kept. An endorser block
// that lands part-way through the window must be picked up, not skipped.
//
// The endorser block is unavailable to every check the gate makes before the
// wait, and becomes available only on the wait's own Nth poll. That makes the
// arrival late by construction -- it cannot be satisfied by a pre-wait check,
// however many of those there are -- and it is driven by the wait's progress
// rather than the wall clock, so it cannot lose a race with the deadline on a
// loaded runner. The test then asserts that the arrival really was observed
// inside the wait, which is what stops it passing vacuously if the wait is
// skipped.
func TestEnsureReferencedEndorserBlocksWaitsForCertifiedClosureArrivingLate(
	t *testing.T,
) {
	parent, certifier, ebHash := leiosTestCertifiedBlockPair(t)
	const arrivalWaitPoll = 3
	var preWaitPolls, waitPolls atomic.Int64
	var arrivedInsideWait atomic.Bool
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func(
			hash []byte,
			slot uint64,
		) ([]cbor.RawMessage, bool) {
			if slot != parent.SlotNumber() {
				return nil, false
			}
			if string(hash) != string(ebHash.Bytes()) {
				return nil, false
			}
			// Once it has arrived it stays available to every caller, as a
			// real endorser block landing in the cache would -- including the
			// gate's mandatory-closure checks, which run after the wait and
			// outside its call stack.
			if arrivedInsideWait.Load() {
				return nil, true
			}
			if !leiosWaitTestPolledFromWait() {
				// Every check before the wait sees it missing, so the wait is
				// always entered and the arrival is always "late".
				preWaitPolls.Add(1)
				return nil, false
			}
			if waitPolls.Add(1) < arrivalWaitPoll {
				return nil, false
			}
			arrivedInsideWait.Store(true)
			return nil, true
		},
		EndorserBlockWaitSlots:     leiosWaitTestLongWaitSlots,
		LeiosApplyEndorserBlockTxs: false,
	}
	ls := &LedgerState{config: cfg}
	withLeiosWaitTestSlotLength(t, ls)

	start := time.Now()
	require.NoError(t, ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{parent, certifier},
	))
	require.True(
		t,
		arrivedInsideWait.Load(),
		"the closure must have been picked up by the wait; if it was never "+
			"polled from inside waitForEndorserBlock the gate did not wait "+
			"at all and this test would otherwise pass vacuously",
	)
	require.GreaterOrEqual(
		t,
		waitPolls.Load(),
		int64(arrivalWaitPoll),
		"mandatory certified closure must be waited for, not skipped",
	)
	require.Positive(
		t,
		preWaitPolls.Load(),
		"the gate is expected to check availability before waiting; if it "+
			"stops doing so this test's phase split needs revisiting",
	)
	require.Less(t, time.Since(start), leiosWaitTestLongWindow)
}

// TestEnsureReferencedEndorserBlocksSharesOneWindowAcrossMissingBlocks is the
// serial-stacking regression. The per-endorser-block waits are independent --
// none observes another's result -- so running them back to back charged the
// ledger pipeline one full diffusion window per missing endorser block. A
// batch referencing k missing endorser blocks cost k windows, which is the
// long tail of the measured apply stalls. They must share one window.
//
// The CIP-conformant path is used because every reference there is read at
// apply time, so all three stay blocking and only the concurrency changes.
func TestEnsureReferencedEndorserBlocksSharesOneWindowAcrossMissingBlocks(
	t *testing.T,
) {
	const missing = 3
	blocks := make([]gledger.Block, 0, missing)
	for i := range missing {
		blocks = append(blocks, leiosWaitTestAnnouncingBlock(
			t,
			uint64(i+1),
			uint64(100+i),
			lcommon.NewBlake2b256(leiosTestHash(byte(0xB0+i))),
		))
	}
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		EndorserBlockWaitSlots: leiosWaitTestWaitSlots,
		// CIP-conformant path: every announcement is read at apply time.
		LeiosApplyEndorserBlockTxs: true,
	}
	ls := &LedgerState{config: cfg}
	withLeiosWaitTestSlotLength(t, ls)

	start := time.Now()
	require.NoError(t, ls.ensureReferencedEndorserBlocks(
		t.Context(),
		blocks,
	))
	elapsed := time.Since(start)
	require.GreaterOrEqual(
		t,
		elapsed,
		leiosWaitTestWindow,
		"the window must still be honoured for references application reads",
	)
	require.Less(
		t,
		elapsed,
		2*leiosWaitTestWindow,
		"per-endorser-block waits stacked serially instead of sharing a window",
	)
}

// TestSplitTipWaitByApplyDependency pins the apply-path contract itself,
// independently of timing: on the CIP path every reference is read at apply
// time and stays blocking; on the Musashi path only the mandatory certified
// closures are read, and a block's own announcement is demoted to background
// prefetch.
func TestSplitTipWaitByApplyDependency(t *testing.T) {
	certified := leiosEbRef{
		slot: 100,
		hash: lcommon.NewBlake2b256(leiosTestHash(0xC1)),
	}
	announced := leiosEbRef{
		slot: 140,
		hash: lcommon.NewBlake2b256(leiosTestHash(0xC2)),
	}
	tipWait := []leiosEbRef{certified, announced}

	blocking, prefetch := splitTipWaitByApplyDependency(
		tipWait,
		[]leiosEbRef{certified},
		true,
	)
	require.Equal(t, []leiosEbRef{certified}, blocking)
	require.Equal(t, []leiosEbRef{announced}, prefetch)

	blocking, prefetch = splitTipWaitByApplyDependency(tipWait, nil, false)
	require.Equal(t, tipWait, blocking)
	require.Empty(t, prefetch)
}

// TestAwaitEndorserBlocksFetchesUpFront pins the second half of the wait fix:
// a reference the batch does block on has its by-point fetch dispatched up
// front, concurrently with the wait, instead of the wait polling passively and
// only falling back to a fetch once the whole diffusion window had already been
// spent. It also pins that an already-available reference costs neither a wait
// nor a fetch.
func TestAwaitEndorserBlocksFetchesUpFront(t *testing.T) {
	var cached atomic.Bool
	var fetches atomic.Int64
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, cached.Load()
		},
		EndorserBlockFetcher: func(
			_ context.Context,
			_ uint64,
			_ []byte,
		) error {
			fetches.Add(1)
			// The fetch is what makes the endorser block available; nothing
			// else will deliver it during this test.
			cached.Store(true)
			return nil
		},
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)
	ref := leiosEbRef{
		slot: 100,
		hash: lcommon.NewBlake2b256(leiosTestHash(0xD1)),
	}

	start := time.Now()
	ls.awaitEndorserBlocks(
		t.Context(),
		[]leiosEbRef{ref},
		leiosWaitTestWindow,
		time.Millisecond,
	)
	require.Less(
		t,
		time.Since(start),
		leiosWaitTestWindow/2,
		"wait did not dispatch the by-point fetch until the window expired",
	)
	require.Equal(t, int64(1), fetches.Load())

	// Already cached: no second fetch, no wait.
	start = time.Now()
	ls.awaitEndorserBlocks(
		t.Context(),
		[]leiosEbRef{ref},
		leiosWaitTestWindow,
		time.Millisecond,
	)
	require.Less(t, time.Since(start), leiosWaitTestWindow/2)
	require.Equal(t, int64(1), fetches.Load())
}

// leiosWaitTestHistogram returns the sample count of
// dingo_metrics_leios_eb_wait_seconds for the given outcome label.
func leiosWaitTestHistogram(
	t *testing.T,
	reg *prometheus.Registry,
	outcome string,
) uint64 {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != "dingo_metrics_leios_eb_wait_seconds" {
			continue
		}
		for _, m := range family.GetMetric() {
			for _, label := range m.GetLabel() {
				if label.GetName() == "outcome" &&
					label.GetValue() == outcome {
					return m.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	t.Fatalf("no eb wait histogram series for outcome %q", outcome)
	return 0
}

// TestLeiosEbWaitMetricsRecordOutcomeAndDuration covers the observability gap:
// the apply-path endorser-block wait had no metric at all, only an Info log,
// so a producer sitting in it for tens of seconds per block showed nothing in
// monitoring. Both outcomes are recorded, and both label values exist before
// any wait happens so a dashboard is not looking at an absent series.
func TestLeiosEbWaitMetricsRecordOutcomeAndDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	var available atomic.Bool
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EndorserBlockProvider: func(
				[]byte,
				uint64,
			) ([]cbor.RawMessage, bool) {
				return nil, available.Load()
			},
		},
	}
	ls.metrics.init(reg)

	// Every outcome series is pre-materialized at init, before any wait.
	require.Equal(
		t,
		3,
		testutil.CollectAndCount(ls.metrics.leiosEbWaitSeconds),
	)
	require.Zero(t, leiosWaitTestHistogram(t, reg, "arrived"))
	require.Zero(t, leiosWaitTestHistogram(t, reg, "timeout"))
	require.Zero(t, leiosWaitTestHistogram(t, reg, "cancelled"))
	require.Zero(t, testutil.ToFloat64(ls.metrics.leiosEbWaitTimeouts))

	ebHash := lcommon.NewBlake2b256(leiosTestHash(0xE7))

	// Expiry: recorded as a timeout, on both the histogram and the counter.
	ls.waitForEndorserBlock(
		t.Context(),
		100,
		ebHash,
		leiosWaitTestWindow,
		time.Millisecond,
	)
	require.Equal(t, uint64(1), leiosWaitTestHistogram(t, reg, "timeout"))
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(ls.metrics.leiosEbWaitTimeouts),
	)
	require.Zero(t, leiosWaitTestHistogram(t, reg, "arrived"))

	// Arrival: recorded as arrived, and does not increment the timeout
	// counter. The endorser block is made available by the provider's own
	// second call rather than by a timer racing the deadline, and the window
	// is long enough that the deadline cannot fire first regardless of load.
	available.Store(true)
	ls.waitForEndorserBlock(
		t.Context(),
		100,
		ebHash,
		leiosWaitTestLongWindow,
		time.Millisecond,
	)
	require.Equal(t, uint64(1), leiosWaitTestHistogram(t, reg, "arrived"))
	require.Equal(t, uint64(1), leiosWaitTestHistogram(t, reg, "timeout"))
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(ls.metrics.leiosEbWaitTimeouts),
	)
}

// TestLeiosEbWaitCancellationIsNotCountedAsTimeout covers the review finding:
// the wait's context is a timeout CHILD of the block-processing context, so its
// Done also closes when the parent is cancelled -- node shutdown, or the pass
// being aborted and restarted. That is not a diffusion-window expiry, and
// counting it as one inflates the timeout rate exactly when a node is shutting
// down or restarting its pipeline, which is when the metric is most likely to
// be read.
//
// Neither case uses a timer. Cancellation is either already in effect before
// the wait starts, or is driven by the wait's own polling, and the window is
// large enough that the deadline cannot win either race on a loaded runner.
// A timer firing at a fraction of a short window is precisely the flake this
// avoids: if it landed late the outcome would be "timeout" and the assertions
// below would fail for reasons that have nothing to do with the code.
func TestLeiosEbWaitCancellationIsNotCountedAsTimeout(t *testing.T) {
	// cancelWhen selects how the parent context is cancelled relative to the
	// wait: before it starts, or from inside the availability poll once the
	// wait is already running.
	for name, cancelOnPoll := range map[string]int64{
		"cancelled before the wait starts": 0,
		"cancelled during the wait":        3,
	} {
		t.Run(name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			var polls atomic.Int64
			ls := &LedgerState{
				config: LedgerStateConfig{
					Logger: slog.New(
						slog.NewJSONHandler(io.Discard, nil),
					),
					EndorserBlockProvider: func(
						[]byte,
						uint64,
					) ([]cbor.RawMessage, bool) {
						if cancelOnPoll > 0 &&
							polls.Add(1) == cancelOnPoll {
							cancel()
						}
						// Never arrives, so only a cancellation or the
						// window can end the wait.
						return nil, false
					},
				},
			}
			ls.metrics.init(reg)

			// Every outcome series exists before any wait.
			require.Equal(
				t,
				3,
				testutil.CollectAndCount(ls.metrics.leiosEbWaitSeconds),
			)

			if cancelOnPoll == 0 {
				cancel()
			}

			start := time.Now()
			ls.waitForEndorserBlock(
				ctx,
				100,
				lcommon.NewBlake2b256(leiosTestHash(0xE8)),
				leiosWaitTestLongWindow,
				time.Millisecond,
			)
			require.Less(
				t,
				time.Since(start),
				leiosWaitTestLongWindow,
				"the wait must end on parent cancellation, not run out the window",
			)

			require.Equal(
				t,
				uint64(1),
				leiosWaitTestHistogram(t, reg, "cancelled"),
			)
			require.Zero(t, leiosWaitTestHistogram(t, reg, "timeout"))
			require.Zero(t, leiosWaitTestHistogram(t, reg, "arrived"))
			require.Zero(
				t,
				testutil.ToFloat64(ls.metrics.leiosEbWaitTimeouts),
				"a cancelled pass must not be counted as a diffusion-window timeout",
			)
		})
	}
}

// TestLeiosEbWaitCancellationLeavesCallerBehaviourUnchanged pins that the new
// classification is only a classification: a cancelled pass still runs the
// mandatory-closure check, so it still fails the chunk when a certified
// closure is missing rather than silently committing without it.
func TestLeiosEbWaitCancellationLeavesCallerBehaviourUnchanged(t *testing.T) {
	parent, certifier, _ := leiosTestCertifiedBlockPair(t)
	ls := &LedgerState{
		config: LedgerStateConfig{
			Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
			EndorserBlockProvider: func(
				[]byte,
				uint64,
			) ([]cbor.RawMessage, bool) {
				return nil, false
			},
			EndorserBlockWaitSlots:     leiosWaitTestWaitSlots,
			LeiosApplyEndorserBlockTxs: false,
		},
	}
	withLeiosWaitTestSlotLength(t, ls)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := ls.ensureReferencedEndorserBlocks(
		ctx,
		[]gledger.Block{parent, certifier},
	)
	require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
}

// leiosWaitTestPolledFromGrace reports whether the provider is being polled by
// the post-window fetch wait (awaitInFlightEndorserFetches).
//
// It matches awaitInFlightEndorserFetches itself, NOT awaitFetch. awaitFetch is
// shared: the certificate-driven path reaches it through fetchOnce's dedup
// wait, so keying on it would make a cert-driven test report a grace phase that
// never ran, depending on whether a fetch happened to be in flight. The waits
// are dispatched from a goroutine per reference, and a closure carries its
// enclosing function's name in the stack (…awaitInFlightEndorserFetches.func1),
// so the enclosing frame is still visible from inside the poll.
func leiosWaitTestPolledFromGrace() bool {
	pcs := make([]uintptr, 64)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		frame, more := frames.Next()
		if strings.Contains(
			frame.Function,
			"awaitInFlightEndorserFetches",
		) {
			return true
		}
		if !more {
			return false
		}
	}
}

// TestEnsureReferencedEndorserBlocksAwaitsLateFetchOnCIPPath covers the
// regression the review found in the CIP-conformant path. Application there
// reads each ranking block's own announcement and nothing re-applies an
// endorser block that lands afterwards, so if the by-point fetch is still in
// flight when the diffusion window elapses, the ranking block is applied
// without the endorser-resident outputs and its spends fall through to the
// interim trust path permanently.
//
// The previous code got this right by accident: it issued a SYNCHRONOUS
// by-point fetch after the window, so however slow the fetch was it still
// populated the cache before the batch reached ledgerProcessBlock. Dispatching
// the fetch up front and asynchronously is better for latency but dropped that
// guarantee. The grace phase restores it.
//
// The fetch is released by the grace phase's own first poll rather than by a
// timer, so "slower than the window, faster than the grace" holds by
// construction and cannot flake: until the grace phase runs, the fetch cannot
// complete, so it is always later than the window.
func TestEnsureReferencedEndorserBlocksAwaitsLateFetchOnCIPPath(t *testing.T) {
	ebHash := lcommon.NewBlake2b256(leiosTestHash(0xF1))
	block := leiosWaitTestAnnouncingBlock(t, 1, 100, ebHash)

	// gracePollsBeforeRelease is chosen so the fetch cannot complete until the
	// wait has polled for materially longer than the soft-warn window: the poll
	// interval is a tenth of a slot, so the window is worth about
	// leiosWaitTestWaitSlots*10 polls and this is comfortably beyond it. A wait
	// bounded BY that window -- which is what this test exists to reject --
	// gives up before reaching this count, deterministically and regardless of
	// machine load, because it is counting the wait's own polls rather than
	// racing a clock.
	const gracePollsBeforeRelease = leiosWaitTestWaitSlots * 20

	release := make(chan struct{})
	var releaseOnce sync.Once
	var gracePolls atomic.Int64
	var cached, sawGracePhase, fetchCompleted atomic.Bool

	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func(
			hash []byte,
			slot uint64,
		) ([]cbor.RawMessage, bool) {
			if slot != 100 || string(hash) != string(ebHash.Bytes()) {
				return nil, false
			}
			if leiosWaitTestPolledFromGrace() {
				// The diffusion window has elapsed and the post-window wait is
				// running. Hold the fetch for long enough that a wait bounded
				// by one further window would have abandoned it.
				sawGracePhase.Store(true)
				if gracePolls.Add(1) >= gracePollsBeforeRelease {
					releaseOnce.Do(func() { close(release) })
				}
			}
			return nil, cached.Load()
		},
		EndorserBlockFetcher: func(
			ctx context.Context,
			_ uint64,
			_ []byte,
		) error {
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Second):
				// Backstop so a regression that never runs the grace phase
				// fails the assertions below instead of hanging.
				return nil
			}
			cached.Store(true)
			fetchCompleted.Store(true)
			return nil
		},
		// CIP-conformant path: application reads the announcement.
		EndorserBlockWaitSlots:     leiosWaitTestWaitSlots,
		LeiosApplyEndorserBlockTxs: true,
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)
	withLeiosWaitTestSlotLength(t, ls)

	require.NoError(t, ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{block},
	))

	require.True(
		t,
		sawGracePhase.Load(),
		"the post-window grace phase must run on the CIP path",
	)
	require.True(t, fetchCompleted.Load(), "the in-flight fetch must finish")
	require.GreaterOrEqual(
		t,
		gracePolls.Load(),
		int64(gracePollsBeforeRelease),
		"the wait must outlast a single further diffusion window",
	)
	require.True(
		t,
		endorserBlockAvailableAt(
			ls.config.EndorserBlockProvider,
			ebHash.Bytes(),
			100,
		),
		"the endorser block must be cached before the batch is applied; "+
			"otherwise the ranking block commits without its endorser-resident "+
			"outputs and nothing ever re-applies them",
	)
}

// TestEnsureReferencedEndorserBlocksSkipsGraceOnCertDrivenPath pins that the
// grace is CIP-only, exercised against a MANDATORY certified closure so the
// blocking set is non-empty and the guard is what decides. On this path a
// missing closure is already retried by the bounded fetch that follows, so
// paying a second diffusion window here would add head-of-line blocking on the
// pipeline for nothing -- exactly what this PR removes.
func TestEnsureReferencedEndorserBlocksSkipsGraceOnCertDrivenPath(t *testing.T) {
	parent, certifier, _ := leiosTestCertifiedBlockPair(t)

	var sawGracePhase atomic.Bool
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			if leiosWaitTestPolledFromGrace() {
				sawGracePhase.Store(true)
			}
			return nil, false
		},
		EndorserBlockFetcher: func(
			_ context.Context,
			_ uint64,
			_ []byte,
		) error {
			return nil
		},
		EndorserBlockWaitSlots:     leiosWaitTestWaitSlots,
		LeiosApplyEndorserBlockTxs: false,
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)
	withLeiosWaitTestSlotLength(t, ls)

	// The closure never arrives, so the mandatory check fails -- which is the
	// correct outcome and is what makes the grace pointless here.
	err := ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{parent, certifier},
	)
	require.ErrorIs(t, err, errCertifiedEndorserBlockUnavailable)
	require.False(
		t,
		sawGracePhase.Load(),
		"the post-window grace must not run on the certificate-driven path",
	)
}

// TestEnsureReferencedEndorserBlocksProceedsWhenCIPFetchFindsNothing pins the
// other arm of the CIP wait: when the by-point fetch finishes without caching
// -- no connected peer holds the endorser block -- application proceeds without
// it rather than failing the chunk.
//
// This is deliberate and is NOT a behaviour this change introduces: it is the
// long-standing semantics of the CIP path. Failing the chunk instead would turn
// an unfetchable endorser block into an unbounded pipeline retry, which is a
// wedge this codebase has hit before. The wait exists to stop us abandoning a
// fetch that was about to succeed, not to convert a genuine absence into a
// stall.
func TestEnsureReferencedEndorserBlocksProceedsWhenCIPFetchFindsNothing(
	t *testing.T,
) {
	ebHash := lcommon.NewBlake2b256(leiosTestHash(0xF3))
	block := leiosWaitTestAnnouncingBlock(t, 1, 100, ebHash)

	var fetches atomic.Int64
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		EndorserBlockFetcher: func(
			_ context.Context,
			_ uint64,
			_ []byte,
		) error {
			// Finishes promptly, having found nothing.
			fetches.Add(1)
			return errors.New("no peer holds this endorser block")
		},
		EndorserBlockWaitSlots:     leiosWaitTestWaitSlots,
		LeiosApplyEndorserBlockTxs: true,
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)
	withLeiosWaitTestSlotLength(t, ls)

	start := time.Now()
	require.NoError(t, ls.ensureReferencedEndorserBlocks(
		t.Context(),
		[]gledger.Block{block},
	))
	// One diffusion window, then the fetch's own prompt failure -- not the
	// hard backstop.
	require.Less(t, time.Since(start), leiosTipFetchHardBound)
	require.Positive(t, fetches.Load())
	require.False(t, endorserBlockAvailableAt(
		ls.config.EndorserBlockProvider,
		ebHash.Bytes(),
		100,
	))
}

// TestLeiosGraceDetectorIgnoresSharedAwaitFetch pins the property that makes
// the certificate-driven test above meaningful rather than timing-dependent.
//
// awaitFetch is shared: the certificate-driven path reaches it through
// fetchOnce's dedup wait whenever a spawned fetch for the same reference is
// still in flight. A grace detector keyed on awaitFetch therefore reports a
// post-window wait that never ran, but only when that race happens to occur --
// so the cert-driven test would pass or fail depending on scheduling. Keying on
// awaitInFlightEndorserFetches makes it positive and deterministic, and this
// asserts exactly that: reached through awaitFetch alone, the detector is
// false.
func TestLeiosGraceDetectorIgnoresSharedAwaitFetch(t *testing.T) {
	var sawGrace atomic.Bool
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			if leiosWaitTestPolledFromGrace() {
				sawGrace.Store(true)
			}
			return nil, false
		},
		EndorserBlockFetcher: func(
			_ context.Context,
			_ uint64,
			_ []byte,
		) error {
			return nil
		},
	}
	b := newLeiosBackfiller(cfg)
	require.NotNil(t, b)

	b.awaitFetch(
		t.Context(),
		leiosEbRef{
			slot: 100,
			hash: lcommon.NewBlake2b256(leiosTestHash(0xF4)),
		},
		time.Millisecond,
		20*time.Millisecond,
	)

	require.False(
		t,
		sawGrace.Load(),
		"awaitFetch alone must not be mistaken for the post-window wait",
	)
}

// leiosWaitTestLogBuffer is a concurrency-safe log sink. The waits under test
// dispatch a goroutine per reference and the backfiller logs from its own
// fetch goroutine, so a bare bytes.Buffer is written from several goroutines
// at once -- which the race detector correctly flags.
type leiosWaitTestLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *leiosWaitTestLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *leiosWaitTestLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestFetchRequiredReportsCancellationNotBudgetExpiry covers the second site
// with the same defect the diffusion wait had: the retry budget is a timeout
// CHILD of the block-processing context, so its Done also closes when the
// PARENT is cancelled -- node shutdown, or the pass being aborted. Reporting
// that as "the retry budget elapsed" tells an operator that peers failed to
// serve the endorser block when nothing was asked of them, and it is loudest
// exactly when a node is shutting down.
func TestFetchRequiredReportsCancellationNotBudgetExpiry(t *testing.T) {
	var logs leiosWaitTestLogBuffer
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		EndorserBlockFetcher: func(
			_ context.Context,
			_ uint64,
			_ []byte,
		) error {
			return errors.New("no peer holds it")
		},
	}
	b := newLeiosBackfiller(cfg)
	require.NotNil(t, b)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := b.fetchRequired(
		ctx,
		leiosEbRef{
			slot: 100,
			hash: lcommon.NewBlake2b256(leiosTestHash(0xC7)),
		},
		time.Millisecond,
	)
	require.Error(t, err)
	require.NotContains(
		t,
		logs.String(),
		"certified leios endorser block fetch budget elapsed",
		"a cancelled pass must not be reported as peers failing to serve",
	)
	require.Contains(
		t,
		logs.String(),
		"certified leios endorser block fetch cancelled",
	)
}

// TestCIPFetchWaitReportsCancellationNotFailure covers the third site. On the
// CIP path a fetch that finishes without caching is reported as "could not be
// fetched", which is a real diagnosis -- unless the pass was cancelled, in
// which case nothing was learned about whether any peer holds the block and
// the line would be a false diagnosis emitted on every shutdown.
func TestCIPFetchWaitReportsCancellationNotFailure(t *testing.T) {
	var logs leiosWaitTestLogBuffer
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		EndorserBlockFetcher: func(
			ctx context.Context,
			_ uint64,
			_ []byte,
		) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)

	ref := leiosEbRef{
		slot: 100,
		hash: lcommon.NewBlake2b256(leiosTestHash(0xC8)),
	}
	ctx, cancel := context.WithCancel(t.Context())
	// Dispatch the fetch, then cancel: the fetch is in flight and will end
	// only because of the cancellation.
	ls.leiosBackfill.spawn(ctx, ref)
	cancel()

	ls.awaitInFlightEndorserFetches(
		ctx,
		[]leiosEbRef{ref},
		leiosWaitTestWindow,
		time.Millisecond,
		leiosTipFetchHardBound,
	)

	require.NotContains(
		t,
		logs.String(),
		"endorser block could not be fetched",
		"a cancelled pass must not be reported as an unfetchable endorser block",
	)
	require.Contains(
		t,
		logs.String(),
		"endorser block fetch cancelled before it completed",
	)
}

// TestCIPFetchWaitDoesNotWarnOnRoutineUnfetchableEndorserBlock pins the log
// level of the CIP path's most common non-cached outcome.
//
// awaitFetch returns for two reasons that this wait cannot otherwise tell
// apart: the all-peers fetch cleared its in-flight marker without caching (no
// peer holds this endorser block), or it neither cached nor cleared before the
// hard bound. Only the second is anomalous. The first is the expected,
// long-standing behaviour of this path -- the code this replaced logged its
// equivalent at Debug -- and on a CIP node where endorser blocks are routinely
// unfetchable, a WARN per reference turns normal operation into alertable
// volume.
func TestCIPFetchWaitDoesNotWarnOnRoutineUnfetchableEndorserBlock(
	t *testing.T,
) {
	var logs leiosWaitTestLogBuffer
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		// Fails immediately, the way a sweep that finds no peer holding the
		// block does: the in-flight marker clears and awaitFetch returns
		// without the hard bound ever being approached.
		EndorserBlockFetcher: func(context.Context, uint64, []byte) error {
			return errors.New("no peer holds it")
		},
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)

	ref := leiosEbRef{
		slot: 100,
		hash: lcommon.NewBlake2b256(leiosTestHash(0xC9)),
	}
	ls.leiosBackfill.spawn(t.Context(), ref)

	ls.awaitInFlightEndorserFetches(
		t.Context(),
		[]leiosEbRef{ref},
		leiosWaitTestWindow,
		time.Millisecond,
		leiosWaitTestLongWindow,
	)

	require.Contains(
		t,
		logs.String(),
		"endorser block could not be fetched",
		"the outcome must still be reported, just not at WARN",
	)
	require.NotContains(
		t,
		logs.String(),
		`"level":"WARN"`,
		"a fetch that swept every peer and found none holding the endorser "+
			"block is this path's expected outcome, not an alert",
	)
}

// TestCIPFetchWaitWarnsWhenAFetchNeitherCachesNorClears is the other half:
// the case that IS anomalous must stay at WARN. A fetch that neither caches
// nor clears its in-flight marker held the ledger pipeline for the whole hard
// bound and produced nothing, which is a wedged fetch rather than an absent
// endorser block.
func TestCIPFetchWaitWarnsWhenAFetchNeitherCachesNorClears(t *testing.T) {
	var logs leiosWaitTestLogBuffer
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	cfg := LedgerStateConfig{
		Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		EndorserBlockProvider: func([]byte, uint64) ([]cbor.RawMessage, bool) {
			return nil, false
		},
		// Never returns while the wait runs, so the in-flight marker is still
		// set when awaitFetch gives up at the hard bound. Released by the
		// cleanup so the goroutine does not outlive the test.
		EndorserBlockFetcher: func(context.Context, uint64, []byte) error {
			<-release
			return errors.New("released")
		},
	}
	ls := &LedgerState{config: cfg}
	ls.leiosBackfill = newLeiosBackfiller(cfg)

	ref := leiosEbRef{
		slot: 100,
		hash: lcommon.NewBlake2b256(leiosTestHash(0xCA)),
	}
	// A live parent context, so the wait can only end at the hard bound --
	// never through the cancellation branch.
	ls.leiosBackfill.spawn(t.Context(), ref)

	ls.awaitInFlightEndorserFetches(
		t.Context(),
		[]leiosEbRef{ref},
		leiosWaitTestWindow,
		time.Millisecond,
		leiosWaitTestSlotLen,
	)

	require.Contains(
		t,
		logs.String(),
		"endorser block fetch neither completed nor cached within the hard bound",
	)
	require.Contains(
		t,
		logs.String(),
		`"level":"WARN"`,
		"a fetch wedged for the whole hard bound is worth an alert",
	)
}
