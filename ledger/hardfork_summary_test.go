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
	"sync"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/config/cardano"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// benchEraSpan describes a contiguous run of epochs for one era in the
// synthetic benchmark cache built by buildBenchEpochCache.
type benchEraSpan struct {
	eraID         uint
	slotLengthMs  uint
	lengthInSlots uint
	epochs        int
}

// mainnetLikeEraSpans approximates real mainnet/Preview era history up to
// Conway: a fixed, small number of epochs per pre-Conway era, matching how
// long each era actually ran before the next hard fork. Any benchmark cache
// size beyond this fixed prefix is appended entirely to Conway (the last
// span), which is what a real, still-syncing chain looks like: a handful of
// historical eras followed by one long-running current era.
var mainnetLikeEraSpans = []benchEraSpan{
	{eras.ByronEraDesc.Id, 20_000, 21_600, 74}, // Byron: ~5-day epochs
	{eras.ShelleyEraDesc.Id, 1_000, 432_000, 40},
	{eras.AllegraEraDesc.Id, 1_000, 432_000, 1},
	{eras.MaryEraDesc.Id, 1_000, 432_000, 15},
	{eras.AlonzoEraDesc.Id, 1_000, 432_000, 25},
	{eras.BabbageEraDesc.Id, 1_000, 432_000, 40},
	{eras.ConwayEraDesc.Id, 1_000, 432_000, -1}, // -1: fill remainder
}

// buildBenchEpochCache builds a synthetic, most-recent-last epoch cache of
// exactly n entries, spanning eras.Eras in order per mainnetLikeEraSpans. This
// gives the benchmark the same shape hardForkSummaryAnchoredAt sees on a real
// chain -- a handful of era transitions near the start, then a single current
// era covering the bulk of the epochs -- without depending on any live node.
func buildBenchEpochCache(n int) []models.Epoch {
	cache := make([]models.Epoch, 0, n)
	epochID := uint64(0)
	startSlot := uint64(0)
	for _, span := range mainnetLikeEraSpans {
		count := span.epochs
		if count < 0 || len(cache)+count > n {
			count = n - len(cache)
		}
		for range count {
			cache = append(cache, models.Epoch{
				EpochId:       epochID,
				StartSlot:     startSlot,
				SlotLength:    span.slotLengthMs,
				LengthInSlots: span.lengthInSlots,
				EraId:         span.eraID,
			})
			epochID++
			startSlot += uint64(span.lengthInSlots)
		}
		if len(cache) >= n {
			break
		}
	}
	return cache
}

// newBenchLedgerState builds a bare LedgerState with a published, n-entry
// synthetic epoch cache, ready for hardForkSummaryAnchoredAt / HardForkSummary
// benchmarking. The tip sits mid-way through the final cached epoch, and
// transitionInfo is left at its zero value (TransitionUnknown), matching a
// live node that is not near a known hard fork.
func newBenchLedgerState(tb testing.TB, n int) *LedgerState {
	tb.Helper()
	cache := buildBenchEpochCache(n)
	last := cache[len(cache)-1]
	tipSlot := last.StartSlot + uint64(last.LengthInSlots)/2

	ls := &LedgerState{
		epochCache: cache,
		currentEra: eras.EraDesc{
			Id:   last.EraId,
			Name: "bench",
		},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(tb),
		},
	}
	ls.publishSnapshotsLocked()
	return ls
}

// BenchmarkHardForkSummary_SmallCache measures HardForkSummary's per-call
// cost against a ~10-epoch cache -- the size implied by
// original (incorrect) "O(eras) ~= 7" cost assumption.
func BenchmarkHardForkSummary_SmallCache(b *testing.B) {
	ls := newBenchLedgerState(b, 10)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ls.HardForkSummary(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHardForkSummary_RealisticCache_700 measures HardForkSummary
// against a ~700-epoch cache, matching mainnet's real epoch count today
// (mainnet is at epoch 656 as of this writing).
func BenchmarkHardForkSummary_RealisticCache_700(b *testing.B) {
	ls := newBenchLedgerState(b, 700)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ls.HardForkSummary(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHardForkSummary_RealisticCache_1500 measures HardForkSummary
// against a ~1500-epoch cache, matching (and modestly exceeding) Preview's
// real epoch count today (Preview is at epoch 1424 as of this writing) to
// give the O(epoch-count) growth headroom to show.
func BenchmarkHardForkSummary_RealisticCache_1500(b *testing.B) {
	ls := newBenchLedgerState(b, 1500)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ls.HardForkSummary(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHardForkSummary_RealisticCache_1500_PerBlock simulates the real
// per-block calling pattern instead of a tight repeat: one publish per
// iteration (as ledgerProcessBlocksFromSource does once per applied batch),
// followed by one HardForkSummary call (as headerVerificationEpoch does once
// per verified header). Before the fix this differs from the tight-repeat
// benchmark only in the publish overhead, since every call rebuilds anyway;
// after the fix it demonstrates that per-block invalidation still costs one
// full rebuild per block -- the cache amortizes only the *within-generation*
// callers (multiple transactions, multiple SlotTimeConverter calls) sharing
// that one build, not cross-block calls.
func BenchmarkHardForkSummary_RealisticCache_1500_PerBlock(b *testing.B) {
	ls := newBenchLedgerState(b, 1500)
	b.ReportAllocs()
	for b.Loop() {
		ls.Lock()
		ls.publishSnapshotsLocked()
		ls.Unlock()
		if _, err := ls.HardForkSummary(); err != nil {
			b.Fatal(err)
		}
	}
}

// freshHardForkSummary bypasses ls.hardForkSummaryCache entirely and builds a
// Summary straight from the currently-published snapshots, so tests can prove
// a cached result matches what an uncached build would produce for the same
// state.
func freshHardForkSummary(
	t testing.TB,
	ls *LedgerState,
) (*hardfork.Summary, error) {
	t.Helper()
	consensusState, tipState := ls.loadStateSnapshots()
	return ls.buildHardForkSummary(consensusState, tipState, 0)
}

// TestHardForkSummaryCache_InvalidatesOnEpochRollover proves that a cached
// Summary is not served once the epoch cache and tip advance into a new
// epoch (a plain epoch rollover, no era change).
//
// The scenario is chosen so the rollover changes an *observable* value: the
// current era's safe-zone End bound. Before rollover the tip sits at slot 250
// with a 3-epoch cache (epochs of 100 slots each); the safe zone
// (25_920 slots, from newTestEraHistoryCfg's k=432, f=0.05) measured from
// tip+1 rounds up to epoch 262 (slot 26_200). After rollover -- a 4th epoch
// appended and the tip advanced to slot 305, inside it -- the same
// computation rounds up to epoch 263 (slot 26_300). A cache that failed to
// invalidate would keep serving the epoch-262 bound.
func TestHardForkSummaryCache_InvalidatesOnEpochRollover(t *testing.T) {
	t.Parallel()

	cache := []models.Epoch{
		{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         1,
		},
		{
			EpochId:       1,
			StartSlot:     100,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         1,
		},
		{
			EpochId:       2,
			StartSlot:     200,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         1,
		},
	}
	ls := &LedgerState{
		epochCache: cache,
		currentEra: eras.EraDesc{Id: 1, Name: "Shelley"},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(250, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
		},
	}
	ls.publishSnapshotsLocked()

	before, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.Len(t, before.Eras, 1)
	require.NotNil(t, before.Eras[0].End)
	assert.Equal(t, uint64(26_200), before.Eras[0].End.Slot)
	assert.Equal(t, uint64(262), before.Eras[0].End.Epoch)

	// Simulate a block-driven epoch rollover: a new epoch row is appended to
	// the cache and the tip advances into it, exactly like
	// ledgerProcessBlocksFromSource's rollover branch (state.go) followed by
	// its tip-commit publish.
	ls.epochCache = append(
		ls.epochCache,
		models.Epoch{
			EpochId:       3,
			StartSlot:     300,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         1,
		},
	)
	ls.currentTip = ochainsync.Tip{Point: ocommon.NewPoint(305, []byte("tip2"))}
	ls.publishSnapshotsLocked()

	after, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.Len(t, after.Eras, 1)
	require.NotNil(t, after.Eras[0].End)
	assert.Equal(
		t,
		uint64(26_300),
		after.Eras[0].End.Slot,
		"cache must not keep serving the pre-rollover safe-zone bound",
	)
	assert.Equal(t, uint64(263), after.Eras[0].End.Epoch)

	fresh, err := freshHardForkSummary(t, ls)
	require.NoError(t, err)
	assert.Equal(
		t,
		fresh,
		after,
		"cached result must match an uncached rebuild",
	)
}

// TestHardForkSummaryCache_InvalidatesOnEraTransition proves that a cached
// single-era Summary is not served once a hard fork actually occurs: the
// epoch cache gains an epoch under a new EraId and the tip moves into it.
//
// Before the transition, the whole cache is era 1 (Shelley) and the Summary
// has exactly one (safe-zone-bounded, open-ended) era. After, the cache also
// contains an era-2 (Allegra) epoch and the tip is inside it: the Summary
// must now report two eras, with era 1 closed at the era-2 boundary and era 2
// current with its own safe-zone bound. A cache serving the stale value would
// keep reporting one era-1 Summary with era 1's own (now wrong) safe-zone
// bound.
func TestHardForkSummaryCache_InvalidatesOnEraTransition(t *testing.T) {
	t.Parallel()

	cache := []models.Epoch{
		{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         1,
		},
		{
			EpochId:       1,
			StartSlot:     100,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         1,
		},
	}
	ls := &LedgerState{
		epochCache: cache,
		currentEra: eras.EraDesc{Id: 1, Name: "Shelley"},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(150, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
		},
	}
	ls.publishSnapshotsLocked()

	before, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.Len(t, before.Eras, 1)
	assert.Equal(t, uint(1), before.Eras[0].EraID)
	require.NotNil(t, before.Eras[0].End)
	assert.Equal(t, uint64(26_100), before.Eras[0].End.Slot)
	assert.Equal(t, uint64(261), before.Eras[0].End.Epoch)

	// Simulate a hard fork: an era-2 epoch is appended (applyEraTransition +
	// the epoch rollover that crosses it) and the tip advances into it.
	ls.epochCache = append(
		ls.epochCache,
		models.Epoch{
			EpochId:       2,
			StartSlot:     200,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         2,
		},
	)
	ls.currentEra = eras.EraDesc{Id: 2, Name: "Allegra"}
	ls.currentTip = ochainsync.Tip{Point: ocommon.NewPoint(250, []byte("tip2"))}
	ls.publishSnapshotsLocked()

	after, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.Len(
		t,
		after.Eras,
		2,
		"cache must not keep serving the pre-transition single-era summary",
	)
	assert.Equal(t, uint(1), after.Eras[0].EraID)
	require.NotNil(t, after.Eras[0].End, "era 1 must now be closed")
	assert.Equal(t, uint64(200), after.Eras[0].End.Slot)
	assert.Equal(t, uint64(2), after.Eras[0].End.Epoch)
	assert.Equal(t, uint(2), after.Eras[1].EraID)
	require.NotNil(
		t,
		after.Eras[1].End,
		"era 2 must carry its own (not era 1's stale) safe-zone bound",
	)
	assert.Equal(
		t,
		uint64(26_200),
		after.Eras[1].End.Slot,
		"cache must not keep serving era 1's stale safe-zone bound",
	)
	assert.Equal(t, uint64(262), after.Eras[1].End.Epoch)

	fresh, err := freshHardForkSummary(t, ls)
	require.NoError(t, err)
	assert.Equal(
		t,
		fresh,
		after,
		"cached result must match an uncached rebuild",
	)
}

// TestHardForkSummaryCache_InvalidatesOnRollback proves that a cached Summary
// built from a deeper chain state is not served after a rollback shortens the
// epoch cache and moves the tip backward.
//
// This is the mirror image of the rollover test and specifically stresses a
// *backward*-moving tip: a cache keyed on anything derived from "the highest
// slot or epoch count seen so far" rather than the exact publication
// generation would fail to invalidate here, since every rollback-safe value
// it could derive from the new state is smaller than what produced the
// cached entry.
func TestHardForkSummaryCache_InvalidatesOnRollback(t *testing.T) {
	t.Parallel()

	deepCache := []models.Epoch{
		{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         1,
		},
		{
			EpochId:       1,
			StartSlot:     100,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         1,
		},
		{
			EpochId:       2,
			StartSlot:     200,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         1,
		},
		{
			EpochId:       3,
			StartSlot:     300,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         1,
		},
	}
	ls := &LedgerState{
		epochCache: deepCache,
		currentEra: eras.EraDesc{Id: 1, Name: "Shelley"},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(305, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
		},
	}
	ls.publishSnapshotsLocked()

	before, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.NotNil(t, before.Eras[0].End)
	assert.Equal(t, uint64(26_300), before.Eras[0].End.Slot)
	assert.Equal(t, uint64(263), before.Eras[0].End.Epoch)

	// Simulate a rollback: the fork block(s) that produced epoch 3 and the
	// deeper tip are undone. rollbackWithResync (state.go) truncates
	// epochCache and resets currentTip under ls.Lock before publishing.
	ls.epochCache = ls.epochCache[:3:3]
	ls.currentTip = ochainsync.Tip{
		Point: ocommon.NewPoint(250, []byte("tip-rolled-back")),
	}
	ls.publishSnapshotsLocked()

	after, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.NotNil(t, after.Eras[0].End)
	assert.Equal(
		t,
		uint64(26_200),
		after.Eras[0].End.Slot,
		"cache must not keep serving the pre-rollback (deeper-tip) safe-zone bound",
	)
	assert.Equal(t, uint64(262), after.Eras[0].End.Epoch)

	fresh, err := freshHardForkSummary(t, ls)
	require.NoError(t, err)
	assert.Equal(
		t,
		fresh,
		after,
		"cached result must match an uncached rebuild",
	)
}

// TestHardForkSummaryCache_InvalidatesOnTransitionInfoChange proves that a
// cached Summary built while transitionInfo was TransitionUnknown is not
// served once the ledger confirms a known hard-fork epoch (TransitionKnown),
// even though the epoch cache and tip are untouched.
//
// TransitionKnown pins the current era's End at the announced boundary
// (instead of the rolling tip+safe-zone bound) and appends a successor era so
// the horizon still covers the first post-boundary epoch -- both are
// structurally different from the TransitionUnknown summary, so a stale cache
// is easy to distinguish from a fresh one here.
func TestHardForkSummaryCache_InvalidatesOnTransitionInfoChange(t *testing.T) {
	t.Parallel()

	cache := []models.Epoch{
		{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         1,
		},
		{
			EpochId:       1,
			StartSlot:     100,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         1,
		},
	}
	ls := &LedgerState{
		epochCache: cache,
		currentEra: eras.EraDesc{Id: 1, Name: "Shelley"},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(150, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
		},
	}
	ls.publishSnapshotsLocked()

	before, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.Len(t, before.Eras, 1)
	assert.Equal(t, hardfork.TransitionUnknown, before.Transition.State)

	// Simulate the ledger confirming a known hard-fork epoch (e.g.
	// evaluateTriggerAtEpoch / evaluateProtocolVersionBump in state.go), with
	// no other state change at all.
	ls.transitionInfo = hardfork.NewTransitionKnown(5)
	ls.publishSnapshotsLocked()

	after, err := ls.HardForkSummary()
	require.NoError(t, err)
	assert.Equal(
		t,
		hardfork.TransitionKnown,
		after.Transition.State,
		"cache must not keep serving the pre-confirmation TransitionUnknown summary",
	)
	require.Len(
		t,
		after.Eras,
		2,
		"a known transition must append the forecast successor era",
	)
	require.NotNil(t, after.Eras[0].End)
	assert.Equal(t, uint64(500), after.Eras[0].End.Slot)
	assert.Equal(t, uint64(5), after.Eras[0].End.Epoch)

	fresh, err := freshHardForkSummary(t, ls)
	require.NoError(t, err)
	assert.Equal(
		t,
		fresh,
		after,
		"cached result must match an uncached rebuild",
	)
}

// TestHardForkSummaryCache_ConcurrentAccessIsRaceFree exercises the cache's
// atomic Load/Store under concurrent readers (at several different
// horizonAnchorSlot values, so different cache keys are live at once) racing
// against a writer that republishes a new tip on every iteration -- the same
// shape as block application advancing the tip while mempool/query goroutines
// call HardForkSummary concurrently. It asserts every read succeeds and
// returns a usable Summary; the "no wrong answer, only an extra rebuild"
// property under a racing overwrite is enforced structurally by
// hardForkSummaryAnchoredAt's key check (see hardForkSummaryCacheEntry's doc
// comment), and `go test -race` catches any unsynchronized access to the
// shared entry itself.
func TestHardForkSummaryCache_ConcurrentAccessIsRaceFree(t *testing.T) {
	t.Parallel()

	cache := buildBenchEpochCache(50)
	last := cache[len(cache)-1]
	ls := &LedgerState{
		epochCache: cache,
		currentEra: eras.EraDesc{Id: last.EraId, Name: "bench"},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(last.StartSlot, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
		},
	}
	ls.publishSnapshotsLocked()

	const readers = 8
	const writerIterations = 200

	stop := make(chan struct{})
	errs := make(chan error, readers)

	var readerWG sync.WaitGroup
	for i := range readers {
		readerWG.Add(1)
		// Vary the anchor per reader so several distinct cache keys
		// (generation, anchor) are being built and read back concurrently,
		// not just one.
		anchor := uint64(i) * uint64(last.LengthInSlots) / 4
		go func(anchor uint64) {
			defer readerWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := ls.hardForkSummaryAnchoredAt(anchor); err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
			}
		}(anchor)
	}

	for i := range writerIterations {
		ls.Lock()
		ls.currentTip = ochainsync.Tip{
			Point: ocommon.NewPoint(last.StartSlot+uint64(i), []byte("tip")),
		}
		ls.publishSnapshotsLocked()
		ls.Unlock()
	}
	close(stop)
	readerWG.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}

// TestHardForkSummaryCache_ServesCachedResultWithinGeneration asserts that the
// cache is actually consulted, which no other test in this package does: the
// four invalidation tests above, the concurrency test, and the whole rest of
// the ledger suite all pass unchanged when the cache lookup is removed
// entirely, because they only prove a *stale* result is never served. Without
// this test a refactor that made the key never match would stay green while
// silently restoring the per-call O(known epochs) rebuild that was
// filed for.
//
// Pointer identity is the assertion rather than value equality precisely
// because the published state is left untouched across the repeated calls: a
// rebuild produces an equal Summary at a different address, so only identity
// distinguishes a served cache entry from a fresh walk.
func TestHardForkSummaryCache_ServesCachedResultWithinGeneration(t *testing.T) {
	t.Parallel()

	cache := []models.Epoch{
		{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         1,
		},
		{
			EpochId:       1,
			StartSlot:     100,
			SlotLength:    1000,
			LengthInSlots: 100,
			EraId:         1,
		},
	}
	ls := &LedgerState{
		epochCache: cache,
		currentEra: eras.EraDesc{Id: 1, Name: "Shelley"},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(150, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
		},
	}
	ls.publishSnapshotsLocked()

	first, err := ls.HardForkSummary()
	require.NoError(t, err)
	second, err := ls.HardForkSummary()
	require.NoError(t, err)
	assert.Same(
		t,
		first,
		second,
		"a repeated call within one publication generation must be served "+
			"from the cache, not rebuilt",
	)

	// A new publication generation must force a rebuild even though no
	// summary input actually changed: generation is the whole invalidation
	// signal, so it cannot be skipped when the published values compare equal.
	ls.Lock()
	ls.publishSnapshotsLocked()
	ls.Unlock()

	third, err := ls.HardForkSummary()
	require.NoError(t, err)
	assert.NotSame(
		t,
		second,
		third,
		"a new publication generation must invalidate the cached entry",
	)
	assert.Equal(
		t,
		second,
		third,
		"republishing unchanged state must rebuild to an equal summary",
	)

	fourth, err := ls.HardForkSummary()
	require.NoError(t, err)
	assert.Same(
		t,
		third,
		fourth,
		"the rebuilt entry must itself be cached at the new generation",
	)

	// The horizon anchor is the other half of the key: a different anchor at
	// the same generation must not be served the anchor-0 entry.
	anchored, err := ls.hardForkSummaryAnchoredAt(20_000)
	require.NoError(t, err)
	assert.NotSame(
		t,
		fourth,
		anchored,
		"a different horizon anchor must not be served another anchor's entry",
	)
	anchoredAgain, err := ls.hardForkSummaryAnchoredAt(20_000)
	require.NoError(t, err)
	assert.Same(
		t,
		anchored,
		anchoredAgain,
		"a repeated call at the same anchor and generation must be cached",
	)
}

// minimalShelleyGenesisCfg returns the smallest test configuration that also
// provides the era shape and stability-window inputs used by HardForkSummary.
func minimalShelleyGenesisCfg(t *testing.T) *cardano.CardanoNodeConfig {
	t.Helper()
	return newTestEraHistoryCfg(t)
}

// TestHardForkSummary_SingleEra verifies the simple case: one era spanning
// multiple contiguous epochs, built from epochCache alone.
func TestHardForkSummary_SingleEra(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				SlotLength:    1000,
				LengthInSlots: 100,
				EraId:         1,
			},
			{
				EpochId:       1,
				StartSlot:     100,
				SlotLength:    1000,
				LengthInSlots: 100,
				EraId:         1,
			},
			{
				EpochId:       2,
				StartSlot:     200,
				SlotLength:    1000,
				LengthInSlots: 100,
				EraId:         1,
			},
		},
		currentEra: eras.EraDesc{Id: 1, Name: "Shelley"},
		currentTip: ochainsync.Tip{Point: ocommon.NewPoint(250, []byte("tip"))},
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
	ls.publishSnapshotsLocked()

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.NotNil(t, sum)
	assert.Equal(
		t,
		time.Date(2022, 10, 25, 0, 0, 0, 0, time.UTC),
		sum.SystemStart,
	)

	require.Len(t, sum.Eras, 1)
	era := sum.Eras[0]
	assert.Equal(t, uint(1), era.EraID)
	assert.Equal(
		t,
		hardfork.Bound{RelativeTime: 0, Slot: 0, Epoch: 0},
		era.Start,
	)
	require.NotNil(t, era.End, "current era should be safe-zone bounded")
	assert.Equal(t, uint64(26_200), era.End.Slot)
	assert.Equal(t, uint64(262), era.End.Epoch)
	assert.Equal(t, uint64(100), era.Params.EpochSize)
	assert.Equal(t, time.Second, era.Params.SlotLength)
	assert.Equal(t, uint64(25_920), era.Params.SafeZoneSlots)
	assert.Equal(t, uint64(25_920), era.Params.GenesisWindow)
}

// TestHardForkSummary_TwoEras verifies two contiguous eras produce a Summary
// with the first era bounded and the second (current) era safe-zone bounded.
func TestHardForkSummary_TwoEras(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			// Byron-ish: EraId=0, 20s slots, 100 slots/epoch
			{
				EpochId:       0,
				StartSlot:     0,
				SlotLength:    20_000,
				LengthInSlots: 100,
				EraId:         0,
			},
			{
				EpochId:       1,
				StartSlot:     100,
				SlotLength:    20_000,
				LengthInSlots: 100,
				EraId:         0,
			},
			// Shelley-ish: EraId=1, starts at slot 200, 1s slots, 432 slots/epoch
			{
				EpochId:       2,
				StartSlot:     200,
				SlotLength:    1000,
				LengthInSlots: 432,
				EraId:         1,
			},
			{
				EpochId:       3,
				StartSlot:     632,
				SlotLength:    1000,
				LengthInSlots: 432,
				EraId:         1,
			},
		},
		currentEra: eras.EraDesc{Id: 1, Name: "Shelley"},
		currentTip: ochainsync.Tip{Point: ocommon.NewPoint(700, []byte("tip"))},
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
	ls.publishSnapshotsLocked()

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.Len(t, sum.Eras, 2)

	byron := sum.Eras[0]
	assert.Equal(t, uint(0), byron.EraID)
	assert.Equal(
		t,
		hardfork.Bound{RelativeTime: 0, Slot: 0, Epoch: 0},
		byron.Start,
	)
	require.NotNil(t, byron.End, "past era must be bounded")
	// Byron spans 2 epochs × 100 slots × 20s = 4000s, ending at slot 200, epoch 2.
	assert.Equal(t, hardfork.Bound{
		RelativeTime: 4000 * time.Second,
		Slot:         200,
		Epoch:        2,
	}, *byron.End)

	shelley := sum.Eras[1]
	assert.Equal(t, uint(1), shelley.EraID)
	// Shelley's Start must line up with Byron's End.
	assert.Equal(t, *byron.End, shelley.Start)
	require.NotNil(t, shelley.End, "current era should be safe-zone bounded")
	assert.Equal(t, uint64(26_984), shelley.End.Slot)
	assert.Equal(t, uint64(64), shelley.End.Epoch)
	assert.Equal(t, uint64(432), shelley.Params.EpochSize)
	assert.Equal(t, time.Second, shelley.Params.SlotLength)

	// End-to-end: the summary's SlotToTime should agree with the manual walk.
	// Slot 250 is 50 Shelley slots past era start (slot 200) → 4000s + 50s after SystemStart.
	got, err := sum.SlotToTime(250)
	require.NoError(t, err)
	assert.Equal(t, sum.SystemStart.Add(4000*time.Second+50*time.Second), got)
}

// TestHardForkSummary_EmptyCache errors.
func TestHardForkSummary_EmptyCache(t *testing.T) {
	ls := &LedgerState{
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
	ls.publishSnapshotsLocked()
	_, err := ls.HardForkSummary()
	assert.Error(t, err)
}

// TestHardForkSummary_ToleratesUnpopulatedCachedEraParams pins the contract
// that a past-era cache row with a zero EpochSize or SlotLength — the sentinel
// an epoch record carries before it is populated — still produces a summary
// rather than an error. Bounding durations must not turn those rows into a
// refusal; the zero-divisor path in hardForkCachedEpochDuration is what makes
// the zero slot length case non-trivial.
func TestHardForkSummary_ToleratesUnpopulatedCachedEraParams(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name          string
		lengthInSlots uint
		slotLength    uint
		wantRelTime   time.Duration
	}{
		{
			name:          "minimum valid parameters",
			lengthInSlots: 1,
			slotLength:    1,
			wantRelTime:   time.Millisecond,
		},
		{
			name:        "zero epoch size",
			slotLength:  1_000,
			wantRelTime: 0,
		},
		{
			name:          "zero slot length",
			lengthInSlots: 100,
			wantRelTime:   0,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			ls := &LedgerState{
				epochCache: []models.Epoch{
					{
						EpochId:       0,
						StartSlot:     0,
						SlotLength:    testCase.slotLength,
						LengthInSlots: testCase.lengthInSlots,
						EraId:         0,
					},
					{
						EpochId:       1,
						StartSlot:     100,
						SlotLength:    1_000,
						LengthInSlots: 100,
						EraId:         1,
					},
				},
				currentEra: eras.EraDesc{Id: 1, Name: "Shelley"},
				currentTip: ochainsync.Tip{
					Point: ocommon.NewPoint(150, []byte("tip")),
				},
				config: LedgerStateConfig{
					CardanoNodeConfig: minimalShelleyGenesisCfg(t),
				},
			}
			ls.publishSnapshotsLocked()

			summary, err := ls.HardForkSummary()
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(summary.Eras), 2)
			// The first era's own duration is what the degenerate row
			// contributes, and it is where the second era starts.
			require.Equal(
				t,
				testCase.wantRelTime,
				summary.Eras[1].Start.RelativeTime,
			)
		})
	}
}

// TestHardForkSummary_SkipsUnpopulatedEpochDuration covers an unpopulated row
// in the middle of an era: it contributes nothing to the accumulated relative
// time, and the era still closes on its populated siblings.
func TestHardForkSummary_SkipsUnpopulatedEpochDuration(t *testing.T) {
	t.Parallel()
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				SlotLength:    1_000,
				LengthInSlots: 100,
				EraId:         0,
			},
			{
				EpochId:       1,
				StartSlot:     100,
				SlotLength:    0,
				LengthInSlots: 100,
				EraId:         0,
			},
			{
				EpochId:       2,
				StartSlot:     200,
				SlotLength:    1_000,
				LengthInSlots: 100,
				EraId:         1,
			},
		},
		currentEra: eras.EraDesc{Id: 1, Name: "Shelley"},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(250, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
	ls.publishSnapshotsLocked()

	summary, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(summary.Eras), 2)
	// Only epoch 0 contributes: 100 slots * 1000ms.
	require.Equal(
		t,
		100*1_000*time.Millisecond,
		summary.Eras[1].Start.RelativeTime,
	)
}

func TestHardForkSummary_RejectsEpochDurationOverflow(t *testing.T) {
	t.Parallel()
	const slotLengthMilliseconds = uint64(1)
	maxDurationMilliseconds := uint64(1<<63-1) / uint64(time.Millisecond)
	if uint64(^uint(0)) < maxDurationMilliseconds+1 {
		t.Skip("uint cannot represent the overflow boundary")
	}

	testCases := []struct {
		name          string
		lengthInSlots uint64
		wantErr       bool
	}{
		{
			name:          "maximum representable duration",
			lengthInSlots: maxDurationMilliseconds,
		},
		{
			name:          "multiplication overflow",
			lengthInSlots: maxDurationMilliseconds + 1,
			wantErr:       true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			ls := &LedgerState{
				epochCache: []models.Epoch{{
					EpochId:       0,
					StartSlot:     0,
					SlotLength:    uint(slotLengthMilliseconds),
					LengthInSlots: uint(testCase.lengthInSlots),
					EraId:         1,
				}},
				currentTip: ochainsync.Tip{
					Point: ocommon.NewPoint(0, []byte("tip")),
				},
				currentEra: eras.EraDesc{Id: 1, Name: "Shelley"},
				config: LedgerStateConfig{
					CardanoNodeConfig: minimalShelleyGenesisCfg(t),
				},
			}
			ls.publishSnapshotsLocked()

			_, err := ls.HardForkSummary()
			if testCase.wantErr {
				require.Error(t, err)
				require.ErrorContains(t, err, "duration")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHardForkSummary_RejectsCumulativeDurationOverflow(t *testing.T) {
	t.Parallel()
	maxDurationMilliseconds := uint64(1<<63-1) / uint64(time.Millisecond)
	perEpochLength := maxDurationMilliseconds/2 + 1
	if uint64(^uint(0)) < perEpochLength {
		t.Skip("uint cannot represent the overflow boundary")
	}

	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				SlotLength:    1,
				LengthInSlots: uint(perEpochLength),
				EraId:         1,
			},
			{
				EpochId:       1,
				StartSlot:     perEpochLength,
				SlotLength:    1,
				LengthInSlots: uint(perEpochLength),
				EraId:         1,
			},
		},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(0, []byte("tip")),
		},
	}
	ls.publishSnapshotsLocked()

	_, err := ls.HardForkSummary()
	require.Error(t, err)
	require.ErrorContains(t, err, "duration")
}

// TestHardForkSummary_RejectsUnavailableShape verifies that consensus
// forecasting fails closed when the current era cannot be resolved to the
// configured hard-fork shape. Returning the cache-derived current era in any
// of these cases would leave End nil with a zero safe zone and make every
// future slot appear forecastable.
func TestHardForkSummary_RejectsUnavailableShape(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name           string
		cfg            func(*testing.T) *cardano.CardanoNodeConfig
		currentEra     eras.EraDesc
		enableDijkstra bool
		wantErr        string
	}{
		{
			name:       "missing node config",
			currentEra: eras.ConwayEraDesc,
			wantErr:    "cardano node config is unavailable",
		},
		{
			name: "shape construction failure",
			cfg: func(*testing.T) *cardano.CardanoNodeConfig {
				return &cardano.CardanoNodeConfig{}
			},
			currentEra: eras.ConwayEraDesc,
			wantErr:    "Shelley genesis unavailable",
		},
		{
			name: "current era outside enabled table",
			cfg: func(t *testing.T) *cardano.CardanoNodeConfig {
				return minimalShelleyGenesisCfg(t)
			},
			currentEra: eras.DijkstraEraDesc,
			wantErr:    "Dijkstra era is unavailable in the hard-fork shape",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			var cfg *cardano.CardanoNodeConfig
			if testCase.cfg != nil {
				cfg = testCase.cfg(t)
			}
			ls := &LedgerState{
				epochCache: []models.Epoch{{
					EpochId:       500,
					StartSlot:     100_000,
					SlotLength:    1_000,
					LengthInSlots: 432_000,
					EraId:         testCase.currentEra.Id,
				}},
				currentEra: testCase.currentEra,
				currentTip: ochainsync.Tip{
					Point: ocommon.NewPoint(200_000, []byte("tip")),
				},
				config: LedgerStateConfig{
					CardanoNodeConfig: cfg,
					EnableDijkstra:    testCase.enableDijkstra,
				},
			}
			ls.publishSnapshotsLocked()

			summary, err := ls.HardForkSummary()
			require.ErrorContains(t, err, testCase.wantErr)
			assert.Nil(t, summary)

			// Drive the consensus caller too: it must reject before attempting
			// to forecast or mutate another epoch from the incomplete shape.
			_, err = ls.headerVerificationEpoch(
				context.Background(),
				1_000_000,
				false,
			)
			require.ErrorContains(t, err, testCase.wantErr)
			assert.Len(t, ls.loadConsensusSnapshot().epochCache, 1)
		})
	}
}

// TestHardForkSummary_CarriesTransitionInfo ensures the current transitionInfo
// is reflected in the returned Summary.
func TestHardForkSummary_CarriesTransitionInfo(t *testing.T) {
	t.Parallel()

	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       5,
				StartSlot:     500,
				SlotLength:    1000,
				LengthInSlots: 100,
				EraId:         1,
			},
		},
		currentEra:     eras.EraDesc{Id: 1, Name: "Shelley"},
		transitionInfo: hardfork.NewTransitionKnown(7),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(550, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
	// A resolved zero safe zone is valid and must still flow through
	// BuildSummary so TransitionKnown can set the confirmed era boundary. Its
	// zero SystemStart must not replace the Shelley genesis value.
	shape := hardfork.Shape{
		Eras: []hardfork.ShapeEntry{{
			EraID: 1,
			Params: hardfork.EraParams{
				EpochSize:     100,
				SlotLength:    time.Second,
				SafeZoneSlots: 0,
			},
		}},
	}
	ls.cachedShape.Store(&shape)
	ls.publishSnapshotsLocked()
	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	assert.Equal(t, hardfork.NewTransitionKnown(7), sum.Transition)
	assert.Equal(
		t,
		time.Date(2022, 10, 25, 0, 0, 0, 0, time.UTC),
		sum.SystemStart,
	)
	// A known transition bounds the current era at the announced epoch
	// boundary and appends an open successor era so the header forecast
	// horizon still covers the first post-boundary epoch (see HardForkSummary).
	require.Len(t, sum.Eras, 2)
	require.NotNil(t, sum.Eras[0].End)
	assert.Zero(t, sum.Eras[0].Params.SafeZoneSlots)
	assert.Equal(t, uint64(700), sum.Eras[0].End.Slot)
	assert.Equal(t, uint64(7), sum.Eras[0].End.Epoch)
	// The appended successor era starts exactly at the announced boundary, so
	// SlotToEpoch resolves slots in the first post-boundary epoch instead of
	// returning ErrPastHorizon. It is open here only because the resolved safe
	// zone is zero (UnsafeIndefiniteSafeZone); see
	// TestHardForkSummary_KnownTransitionSuccessorBoundedBySafeZone for the
	// bounded case.
	assert.Equal(t, *sum.Eras[0].End, sum.Eras[1].Start)
	assert.Nil(t, sum.Eras[1].End)
	info, err := sum.SlotToEpoch(700)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), info.Epoch)
}

// TestHardForkSummary_KnownTransitionExtendsHeaderHorizon is a regression test
// for the header forecast-horizon deadlock: a pending hard-fork initiation arms
// TransitionKnown before an epoch boundary, BuildSummary bounds the current era
// at that boundary, and the header verification gate then rejected the first
// header of the post-boundary epoch as past-horizon, so the node could never
// apply the block that would consume the transition and extend era history.
// HardForkSummary must append an open successor era at the boundary so the
// first post-boundary epoch stays within the forecast horizon. Reproduces the
// musashi epoch 6 to 7 wedge in miniature.
func TestHardForkSummary_KnownTransitionExtendsHeaderHorizon(t *testing.T) {
	const (
		epochSize    = uint64(100)
		startEpoch   = uint64(4)
		startSlot    = uint64(400)
		knownEpoch   = uint64(7)
		boundarySlot = uint64(700) // first slot of epoch 7 (400 + (7-4)*100)
	)
	ls := &LedgerState{
		epochCache: []models.Epoch{{
			EpochId:       startEpoch,
			StartSlot:     startSlot,
			SlotLength:    1_000,
			LengthInSlots: 100,
			EraId:         1,
		}},
		currentEra:     eras.EraDesc{Id: 1, Name: "Shelley"},
		transitionInfo: hardfork.NewTransitionKnown(knownEpoch),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(688, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
	// A single modeled era: the ledger already occupies the last era, so the
	// appended successor era reuses the current era's params.
	shape := hardfork.Shape{
		Eras: []hardfork.ShapeEntry{{
			EraID: 1,
			Params: hardfork.EraParams{
				EpochSize:     epochSize,
				SlotLength:    time.Second,
				SafeZoneSlots: 0,
			},
		}},
	}
	ls.cachedShape.Store(&shape)
	ls.publishSnapshotsLocked()

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)

	// The first block of the post-boundary epoch, and slots within it, must
	// resolve to that epoch rather than returning ErrPastHorizon.
	for _, slot := range []uint64{
		boundarySlot,
		boundarySlot + 10,
		boundarySlot + epochSize - 1,
	} {
		info, err := sum.SlotToEpoch(slot)
		require.NoErrorf(t, err, "slot %d must be within horizon", slot)
		assert.Equalf(t, knownEpoch, info.Epoch, "slot %d epoch", slot)
	}

	// The successor era reuses the current (last modeled) era's params and is
	// open because this shape resolves a zero safe zone; the bounded era still
	// ends exactly at the announced boundary.
	require.Len(t, sum.Eras, 2)
	require.NotNil(t, sum.Eras[0].End)
	assert.Equal(t, boundarySlot, sum.Eras[0].End.Slot)
	assert.Equal(t, *sum.Eras[0].End, sum.Eras[1].Start)
	assert.Nil(t, sum.Eras[1].End)
	assert.Equal(t, sum.Eras[0].EraID, sum.Eras[1].EraID)
	assert.Equal(t, sum.Eras[0].Params.EpochSize, sum.Eras[1].Params.EpochSize)
	assert.Equal(
		t,
		sum.Eras[0].Params.SlotLength,
		sum.Eras[1].Params.SlotLength,
	)
}

// TestHardForkSummary_KnownTransitionSuccessorBoundedBySafeZone asserts the
// appended successor era is not unconditionally open: with a non-zero configured
// safe zone it is bounded by that safe zone measured from the announced
// boundary, snapped up to an epoch boundary. The horizon must still cover the
// whole first post-boundary epoch (the liveness requirement), while slots beyond
// the successor's own safe zone are rejected as past-horizon rather than
// silently accepted.
func TestHardForkSummary_KnownTransitionSuccessorBoundedBySafeZone(
	t *testing.T,
) {
	const (
		epochSize     = uint64(100)
		safeZoneSlots = uint64(250)
		knownEpoch    = uint64(7)
		boundarySlot  = uint64(700) // first slot of epoch 7 (400 + (7-4)*100)
		// 700 + 250 = 950 lands mid-epoch 9, so the bound snaps up to the
		// start of epoch 10 at slot 1000.
		succEndSlot = uint64(1_000)
	)
	ls := &LedgerState{
		epochCache: []models.Epoch{{
			EpochId:       4,
			StartSlot:     400,
			SlotLength:    1_000,
			LengthInSlots: 100,
			EraId:         1,
		}},
		currentEra:     eras.EraDesc{Id: 1, Name: "Shelley"},
		transitionInfo: hardfork.NewTransitionKnown(knownEpoch),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(688, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
	shape := hardfork.Shape{
		Eras: []hardfork.ShapeEntry{{
			EraID: 1,
			Params: hardfork.EraParams{
				EpochSize:     epochSize,
				SlotLength:    time.Second,
				SafeZoneSlots: safeZoneSlots,
			},
		}},
	}
	ls.cachedShape.Store(&shape)
	ls.publishSnapshotsLocked()

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.Len(t, sum.Eras, 2)

	// TransitionKnown pins the current era at the announced boundary regardless
	// of the safe zone.
	require.NotNil(t, sum.Eras[0].End)
	assert.Equal(t, boundarySlot, sum.Eras[0].End.Slot)

	// The successor is bounded, not open.
	require.NotNil(t, sum.Eras[1].End)
	assert.Equal(t, *sum.Eras[0].End, sum.Eras[1].Start)
	assert.Equal(t, succEndSlot, sum.Eras[1].End.Slot)
	assert.Equal(t, safeZoneSlots, sum.Eras[1].Params.SafeZoneSlots)

	// The whole first post-boundary epoch stays inside the horizon.
	for _, slot := range []uint64{
		boundarySlot,
		boundarySlot + epochSize - 1,
		succEndSlot - 1,
	} {
		info, err := sum.SlotToEpoch(slot)
		require.NoErrorf(t, err, "slot %d must be within horizon", slot)
		assert.GreaterOrEqualf(t, info.Epoch, knownEpoch, "slot %d epoch", slot)
	}

	// Slots at or past the successor's bound are past-horizon again.
	for _, slot := range []uint64{succEndSlot, succEndSlot + epochSize} {
		_, err := sum.SlotToEpoch(slot)
		require.ErrorIsf(t, err, hardfork.ErrPastHorizon,
			"slot %d must be past horizon", slot)
	}
}

// TestHardForkSummary_KnownTransitionSuccessorByShapeOrder asserts the successor
// era is taken by position in the configured shape rather than by EraID + 1.
// A shape with non-contiguous era IDs would otherwise fail the successor lookup
// and silently reuse the current era's ID and params.
func TestHardForkSummary_KnownTransitionSuccessorByShapeOrder(t *testing.T) {
	const (
		knownEpoch   = uint64(7)
		boundarySlot = uint64(700)
		succEraID    = uint(5)
	)
	ls := &LedgerState{
		epochCache: []models.Epoch{{
			EpochId:       4,
			StartSlot:     400,
			SlotLength:    1_000,
			LengthInSlots: 100,
			EraId:         1,
		}},
		currentEra:     eras.EraDesc{Id: 1, Name: "Shelley"},
		transitionInfo: hardfork.NewTransitionKnown(knownEpoch),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(688, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
	// EraID jumps from 1 to 5: EraForID(1+1) finds nothing, EraIndex(1)+1 finds
	// the real successor.
	shape := hardfork.Shape{
		Eras: []hardfork.ShapeEntry{
			{
				EraID: 1,
				Params: hardfork.EraParams{
					EpochSize:     100,
					SlotLength:    time.Second,
					SafeZoneSlots: 0,
				},
			},
			{
				EraID: succEraID,
				Params: hardfork.EraParams{
					EpochSize:     200,
					SlotLength:    2 * time.Second,
					SafeZoneSlots: 0,
				},
			},
		},
	}
	ls.cachedShape.Store(&shape)
	ls.publishSnapshotsLocked()

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.Len(t, sum.Eras, 2)

	require.NotNil(t, sum.Eras[0].End)
	assert.Equal(t, boundarySlot, sum.Eras[0].End.Slot)
	assert.Equal(t, uint(1), sum.Eras[0].EraID)

	// The successor carries the next shape entry's ID and params, not the
	// current era's.
	assert.Equal(t, succEraID, sum.Eras[1].EraID)
	assert.Equal(t, uint64(200), sum.Eras[1].Params.EpochSize)
	assert.Equal(t, 2*time.Second, sum.Eras[1].Params.SlotLength)
}

// TestHardForkSummary_KnownTransitionCoversStabilityWindow probes whether the
// successor era appended for a known transition covers the full deterministic
// forecast window from the tip, not merely the first post-boundary epoch. This
// is the scenario where a coverage gap would hide: chainsync verifies headers
// ahead of the applied tip, so a header several epochs past the boundary but
// still within one stability window of the tip must resolve. Because the
// successor's own safe zone is measured from the announced boundary (which is
// at or ahead of the tip), one successor already reaches at least tip+safeZone,
// so there is no gap; a slot beyond the deterministic window is still rejected.
func TestHardForkSummary_KnownTransitionCoversStabilityWindow(t *testing.T) {
	const (
		epochSize  = uint64(100)
		safeZone   = uint64(250) // spans 2.5 epochs
		startEpoch = uint64(5)
		startSlot  = uint64(500)
		knownEpoch = uint64(6)   // boundary at slot 600
		boundary   = uint64(600) // first slot of epoch 6
		tipSlot    = uint64(560) // in epoch 5, before the boundary
	)
	ls := &LedgerState{
		epochCache: []models.Epoch{{
			EpochId:       startEpoch,
			StartSlot:     startSlot,
			SlotLength:    1_000,
			LengthInSlots: 100,
			EraId:         1,
		}},
		currentEra:     eras.EraDesc{Id: 1, Name: "Shelley"},
		transitionInfo: hardfork.NewTransitionKnown(knownEpoch),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
	shape := hardfork.Shape{
		Eras: []hardfork.ShapeEntry{{
			EraID: 1,
			Params: hardfork.EraParams{
				EpochSize:     epochSize,
				SlotLength:    time.Second,
				SafeZoneSlots: safeZone,
			},
		}},
	}
	ls.cachedShape.Store(&shape)
	ls.publishSnapshotsLocked()

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)

	// Every slot from the boundary through the deterministic window
	// (tip+safeZone = 810) must resolve, including slots several epochs past the
	// boundary — this is the chainsync-ahead-of-applied-tip case. A single
	// boundary-snapped successor must cover all of it, with no transient
	// past-horizon rejection at the intervening epoch boundaries (700, 800).
	window := tipSlot + safeZone // 810
	for _, tc := range []struct {
		slot  uint64
		epoch uint64
	}{
		{boundary, 6}, // first post-boundary epoch
		{699, 6},      // end of epoch 6
		{700, 7},      // next boundary, one epoch past the transition
		{800, 8},      // epoch 8 start, two epochs past the transition
		{window, 8},   // edge of the deterministic window (810, epoch 8)
	} {
		info, err := sum.SlotToEpoch(tc.slot)
		require.NoErrorf(
			t, err, "slot %d within deterministic window must resolve", tc.slot,
		)
		assert.Equalf(t, tc.epoch, info.Epoch, "slot %d epoch", tc.slot)
	}
}

// TestHardForkSummary_KnownTransitionRejectsPastSuccessorBound is the negative
// half of TestHardForkSummary_KnownTransitionCoversStabilityWindow: the
// successor era extends the horizon but must not make it unbounded. With the
// same shape (boundary at slot 600, safe zone 250), the successor's safe zone
// snaps up to the start of epoch 9, so slot 900 and beyond are past-horizon
// again. Without this, an accidentally open successor would still satisfy the
// in-window assertions.
func TestHardForkSummary_KnownTransitionRejectsPastSuccessorBound(
	t *testing.T,
) {
	const (
		epochSize  = uint64(100)
		safeZone   = uint64(250)
		startEpoch = uint64(5)
		startSlot  = uint64(500)
		knownEpoch = uint64(6)
		tipSlot    = uint64(560)
		// 600 + 250 = 850 lands mid-epoch 8, so the successor's bound snaps up
		// to the start of epoch 9 at slot 900.
		succEndSlot = uint64(900)
	)
	ls := &LedgerState{
		epochCache: []models.Epoch{{
			EpochId:       startEpoch,
			StartSlot:     startSlot,
			SlotLength:    1_000,
			LengthInSlots: 100,
			EraId:         1,
		}},
		currentEra:     eras.EraDesc{Id: 1, Name: "Shelley"},
		transitionInfo: hardfork.NewTransitionKnown(knownEpoch),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(tipSlot, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
	shape := hardfork.Shape{
		Eras: []hardfork.ShapeEntry{{
			EraID: 1,
			Params: hardfork.EraParams{
				EpochSize:     epochSize,
				SlotLength:    time.Second,
				SafeZoneSlots: safeZone,
			},
		}},
	}
	ls.cachedShape.Store(&shape)
	ls.publishSnapshotsLocked()

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)

	// The successor is bounded, and its bound is where the horizon ends.
	require.Len(t, sum.Eras, 2)
	require.NotNil(t, sum.Eras[1].End)
	assert.Equal(t, succEndSlot, sum.Eras[1].End.Slot)

	// The last slot inside the window still resolves...
	info, err := sum.SlotToEpoch(succEndSlot - 1)
	require.NoError(t, err)
	assert.Equal(t, uint64(8), info.Epoch)

	// ...and the bound itself, plus anything past it, does not.
	for _, slot := range []uint64{
		succEndSlot,
		succEndSlot + 1,
		succEndSlot + epochSize,
		succEndSlot + 10*epochSize,
	} {
		_, err := sum.SlotToEpoch(slot)
		require.ErrorIsf(t, err, hardfork.ErrPastHorizon,
			"slot %d must be past horizon", slot)
	}
}

// TestHardForkSummary_KnownTransitionSuccessorTracksLiveTip is a regression
// test for a node-side horizon-computation gap distinct from the horizon-anchor
// gap: the appended successor era used to measure its own safe zone only from
// the announced boundary, never from how far the live tip has actually advanced
// past it. A node that fails to apply the block crossing that boundary (for any
// reason -- this is the exact class of bug this fixture reproduces, not its
// cause) keeps reconstructing this same Summary on every retry with the SAME
// transitionInfo and epoch cache, since neither changes without a successful
// apply. Before this fix, the successor's horizon was pinned at
// boundary+safeZone forever, so once the live tip passed that fixed point every
// further block or transaction slot fell "past horizon" permanently -- a live,
// canonical chain rejected as though its own tip did not exist, even though
// nothing about the transition or the chain's own history changed.
//
// This reproduces the reported live-incident signature directly: the current
// published tip's own slot judged past horizon, looping "block processing
// failed, restarting pipeline" forever with no way to recover because retrying
// recomputes the identical, still-too-narrow bound every time.
func TestHardForkSummary_KnownTransitionSuccessorTracksLiveTip(t *testing.T) {
	t.Parallel()

	const (
		epochSize     = uint64(100)
		safeZoneSlots = uint64(250)
		knownEpoch    = uint64(7)
		boundarySlot  = uint64(700) // first slot of epoch 7 (400 + (7-4)*100)
		// The live tip has advanced far past where a boundary-only successor
		// bound (700+250 snapped to 1_000) could ever reach.
		liveTipSlot = uint64(50_000)
	)
	ls := &LedgerState{
		epochCache: []models.Epoch{{
			EpochId:       4,
			StartSlot:     400,
			SlotLength:    1_000,
			LengthInSlots: 100,
			EraId:         1,
		}},
		currentEra:     eras.EraDesc{Id: 1, Name: "Shelley"},
		transitionInfo: hardfork.NewTransitionKnown(knownEpoch),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(liveTipSlot, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: minimalShelleyGenesisCfg(t),
		},
	}
	shape := hardfork.Shape{
		Eras: []hardfork.ShapeEntry{{
			EraID: 1,
			Params: hardfork.EraParams{
				EpochSize:     epochSize,
				SlotLength:    time.Second,
				SafeZoneSlots: safeZoneSlots,
			},
		}},
	}
	ls.cachedShape.Store(&shape)
	ls.publishSnapshotsLocked()

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.Len(t, sum.Eras, 2)
	require.NotNil(t, sum.Eras[0].End)
	assert.Equal(t, boundarySlot, sum.Eras[0].End.Slot)

	// The live tip's own slot -- what the reported incident actually failed
	// on -- must resolve. Before this fix this returned ErrPastHorizon
	// because the successor's fixed bound (1_000) never accounted for the
	// tip having reached 50_000.
	info, err := sum.SlotToEpoch(liveTipSlot)
	require.NoError(
		t, err,
		"the live tip's own slot must stay within the horizon",
	)
	assert.GreaterOrEqual(t, info.Epoch, knownEpoch)

	require.NotNil(t, sum.Eras[1].End)
	assert.Greater(
		t,
		sum.Eras[1].End.Slot,
		liveTipSlot,
		"the successor horizon must extend past the live tip, not freeze at "+
			"boundary+safeZone",
	)
	assert.Equal(t, uint64(50_300), sum.Eras[1].End.Slot)
}

func TestHardForkSummary_RejectsSlotPastSafeZone(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       500,
				StartSlot:     100_000,
				SlotLength:    1_000,
				LengthInSlots: 432_000,
				EraId:         eras.ConwayEraDesc.Id,
			},
		},
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(200_000, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
		},
	}
	ls.publishSnapshotsLocked()

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.Len(t, sum.Eras, 1)
	require.NotNil(t, sum.Eras[0].End)
	assert.Equal(t, uint64(532_000), sum.Eras[0].End.Slot)

	_, err = sum.SlotToEpoch(531_999)
	require.NoError(t, err)
	_, err = sum.SlotToEpoch(532_000)
	assert.ErrorIs(t, err, hardfork.ErrPastHorizon)
}

// TestHeaderVerificationEpoch_PastHorizonDeferred verifies that a header past
// the forecast horizon is classified as a deferred condition (not a hard
// peer-fault rejection). This is what keeps chainsync/blockfetch from recycling
// the honest peer that served the header: recycling on past-horizon starves the
// peer pool during catch-up and deadlocks at epoch boundaries. The error must
// still carry ErrPastHorizon so the no-apply-past-horizon guard is unchanged.
func TestHeaderVerificationEpoch_PastHorizonDeferred(t *testing.T) {
	ls := &LedgerState{
		epochCache: []models.Epoch{{
			EpochId:       500,
			StartSlot:     100_000,
			SlotLength:    1_000,
			LengthInSlots: 432_000,
			EraId:         eras.ConwayEraDesc.Id,
		}},
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(200_000, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
		},
	}
	ls.publishSnapshotsLocked()

	// The forecast horizon ends at slot 532_000; a header past it must be
	// classified deferred, and still carry ErrPastHorizon.
	_, err := ls.headerVerificationEpoch(context.Background(), 600_000, false)
	require.Error(t, err)
	require.ErrorIs(t, err, errHeaderVerificationDeferred,
		"past-horizon header must be deferred, not a peer-fault rejection")
	require.ErrorIs(t, err, hardfork.ErrPastHorizon)
	require.ErrorIs(t, err, ErrHeaderBeyondForecastHorizon,
		"chainsync must be able to tell past-horizon apart from other deferrals")
	require.True(t, IsHeaderVerificationDeferred(err))

	// A slot inside the horizon whose epoch has no nonce yet is deferred for
	// a different reason and must not be held back as past-horizon.
	_, err = ls.headerVerificationEpoch(context.Background(), 450_000, false)
	require.True(t, IsHeaderVerificationDeferred(err))
	require.NotErrorIs(t, err, ErrHeaderBeyondForecastHorizon)
}

func TestHardForkSummary_MainnetForecastBoundary(t *testing.T) {
	testCases := []struct {
		name          string
		tipSlot       uint64
		wantEndSlot   uint64
		nextEpochOpen bool
	}{
		{
			name:        "before nonce cutoff",
			tipSlot:     302_399,
			wantEndSlot: 432_000,
		},
		{
			name:          "at nonce cutoff",
			tipSlot:       302_400,
			wantEndSlot:   864_000,
			nextEpochOpen: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cfg := minimalShelleyGenesisCfg(t)
			ls := &LedgerState{
				epochCache: []models.Epoch{{
					EpochId:       0,
					StartSlot:     0,
					SlotLength:    1_000,
					LengthInSlots: 432_000,
					EraId:         eras.ConwayEraDesc.Id,
				}},
				currentEra: eras.ConwayEraDesc,
				currentTip: ochainsync.Tip{
					Point: ocommon.NewPoint(
						testCase.tipSlot,
						[]byte("tip"),
					),
				},
				config: LedgerStateConfig{CardanoNodeConfig: cfg},
			}
			shape := hardfork.Shape{
				SystemStart: cfg.ShelleyGenesis().SystemStart,
				Eras: []hardfork.ShapeEntry{{
					EraID: eras.ConwayEraDesc.Id,
					Params: hardfork.EraParams{
						EpochSize:     432_000,
						SlotLength:    time.Second,
						SafeZoneSlots: 129_600,
						GenesisWindow: 129_600,
					},
				}},
			}
			ls.cachedShape.Store(&shape)
			ls.publishSnapshotsLocked()

			sum, err := ls.HardForkSummary()
			require.NoError(t, err)
			require.Len(t, sum.Eras, 1)
			require.NotNil(t, sum.Eras[0].End)
			assert.Equal(t, testCase.wantEndSlot, sum.Eras[0].End.Slot)

			_, err = ls.EpochInfo(1)
			if testCase.nextEpochOpen {
				require.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, hardfork.ErrPastHorizon)
			}

			// Arbitrary time queries stay bounded at the exclusive end.
			endTime := sum.SystemStart.Add(
				time.Duration(testCase.wantEndSlot) * time.Second,
			)
			_, err = ls.TimeToSlot(endTime)
			assert.ErrorIs(t, err, hardfork.ErrPastHorizon)

			// Operational near-now timing remains available to a node whose
			// ledger is catching up from behind the forecast.
			currentSlot, err := ls.TimeToSlot(time.Now())
			require.NoError(t, err)
			assert.Greater(t, currentSlot, testCase.wantEndSlot)
		})
	}
}

func TestEpochInfoUsesMaterializedEpochPastForecast(t *testing.T) {
	cfg := minimalShelleyGenesisCfg(t)
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				SlotLength:    1_000,
				LengthInSlots: 100,
				EraId:         eras.ShelleyEraDesc.Id,
			},
			{
				EpochId:       10,
				StartSlot:     1_000,
				SlotLength:    1_000,
				LengthInSlots: 100,
				EraId:         eras.ShelleyEraDesc.Id,
			},
		},
		currentEra: eras.ShelleyEraDesc,
		config:     LedgerStateConfig{CardanoNodeConfig: cfg},
	}
	shape := hardfork.Shape{
		SystemStart: cfg.ShelleyGenesis().SystemStart,
		Eras: []hardfork.ShapeEntry{{
			EraID: eras.ShelleyEraDesc.Id,
			Params: hardfork.EraParams{
				EpochSize:     100,
				SlotLength:    time.Second,
				SafeZoneSlots: 1,
			},
		}},
	}
	ls.cachedShape.Store(&shape)
	ls.publishSnapshotsLocked()

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	_, err = sum.EpochInfo(10)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon)

	info, err := ls.EpochInfo(10)
	require.NoError(t, err)
	assert.Equal(t, uint64(1_000), info.StartSlot)
	assert.Equal(t, uint(100), info.LengthInSlots)
}

func TestHardForkSummary_TransitionImpossibleStartsAtEraBoundary(
	t *testing.T,
) {
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				SlotLength:    1_000,
				LengthInSlots: 500,
				EraId:         eras.ConwayEraDesc.Id,
			},
		},
		currentEra:     eras.ConwayEraDesc,
		transitionInfo: hardfork.NewTransitionImpossible(),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(455, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
		},
	}
	ls.publishSnapshotsLocked()

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	assert.Equal(t, hardfork.NewTransitionImpossible(), sum.Transition)
	require.Len(t, sum.Eras, 1)
	require.NotNil(t, sum.Eras[0].End)
	assert.Equal(t, uint64(26_000), sum.Eras[0].End.Slot)

	_, err = sum.SlotToEpoch(500)
	require.NoError(t, err,
		"a known same-era epoch boundary must remain forecastable")
}

// TestHardForkSummary_HorizonAnchoredAtAppliedParent is the summary half of the
// horizon-anchor fix. HardForkSummary measures the safe zone from the published
// tip, which only advances when a whole block batch commits; the reference
// implementation measures it from the applied block's immediate predecessor.
// Because applySafeZone snaps up to an epoch boundary, that difference is not
// proportional to the staleness: on Preview, 46 slots of lag cost a full epoch
// of horizon and rejected a canonical Plutus transaction.
func TestHardForkSummary_HorizonAnchoredAtAppliedParent(t *testing.T) {
	ls := previewWedgeLedgerState(t)

	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	require.Len(t, sum.Eras, 1)
	require.NotNil(t, sum.Eras[0].End)
	assert.Equal(
		t,
		uint64(previewTipHorizonSlot),
		sum.Eras[0].End.Slot,
		"the published tip must still produce the horizon that wedged the "+
			"replay; if it does not, the fixture no longer reproduces #3844",
	)

	anchored, err := ls.hardForkSummaryAnchoredAt(previewParentSlot)
	require.NoError(t, err)
	require.Len(t, anchored.Eras, 1)
	require.NotNil(t, anchored.Eras[0].End)
	assert.Equal(
		t,
		uint64(previewParentHorizon),
		anchored.Eras[0].End.Slot,
		"anchoring at the applied block's predecessor must extend the "+
			"horizon by the epoch the stale tip lost",
	)

	behind, err := ls.hardForkSummaryAnchoredAt(previewEraStartSlot)
	require.NoError(t, err)
	require.NotNil(t, behind.Eras[0].End)
	assert.Equal(
		t,
		uint64(previewTipHorizonSlot),
		behind.Eras[0].End.Slot,
		"an anchor behind the published tip must not shrink the horizon",
	)
}

func TestWallClockSlotFromConfirmedHistory_SupportedWhenEraSpansNow(t *testing.T) {
	// A single unbounded era whose slot length is 1s spans the real wall
	// clock, so the method returns the true current slot. The exact slot
	// changes every second, but it must be far larger than the warm-up
	// overhead of a fresh epoch cache (a value that can only grow, never
	// shrink).
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       5,
				StartSlot:     500,
				SlotLength:    1000,
				LengthInSlots: 100,
				EraId:         1,
			},
		},
		currentEra:     eras.EraDesc{Id: 1, Name: "Shelley"},
		transitionInfo: hardfork.NewTransitionKnown(7),
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(550, []byte("tip")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
		},
	}
	// UnsafeIndefiniteSafeZone → End nil → now always in-era.
	ls.cachedShape.Store(&hardfork.Shape{
		Eras: []hardfork.ShapeEntry{{
			EraID: 1,
			Params: hardfork.EraParams{
				EpochSize:     100,
				SlotLength:    time.Second,
				SafeZoneSlots: 0,
			},
		}},
	})
	ls.publishSnapshotsLocked()

	slot, ok, err := ls.WallClockSlotFromConfirmedHistory()
	require.NoError(t, err)
	require.True(t, ok, "a 1s-era covering now must resolve the wall-clock slot")
	assert.Greater(t, slot, uint64(100_000_000),
		"2026 wall-clock slot via 1s slots since 2022-10-25 must exceed 100M")
}

func TestWallClockSlotFromConfirmedHistory_UnsupportedWhenPastHorizon(t *testing.T) {
	// A bounded era whose forecast horizon ends far in the past (fresh
	// mainnet has only a single bounded Byron era) cannot resolve the
	// current wall-clock time, so the method must return false.
	ls := &LedgerState{
		epochCache: []models.Epoch{
			{
				EpochId:       0,
				StartSlot:     0,
				SlotLength:    20000,
				LengthInSlots: 21600,
				EraId:         0,
			},
		},
		currentEra: eras.EraDesc{Id: 0, Name: "Byron"},
		transitionInfo: hardfork.TransitionInfo{
			State: hardfork.TransitionUnknown,
		},
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(0, []byte("genesis")),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
		},
	}
	// NormalSafeZone snaps the end well within the same epoch — far past the
	// real wall clock in 2026.
	ls.cachedShape.Store(&hardfork.Shape{
		Eras: []hardfork.ShapeEntry{{
			EraID: 0,
			Params: hardfork.EraParams{
				EpochSize:     21600,
				SlotLength:    20 * time.Second,
				SafeZoneSlots: 21600,
			},
			NextEraTrigger: hardfork.NewTriggerAtEpoch(1),
		}},
	})
	ls.publishSnapshotsLocked()

	slot, ok, err := ls.WallClockSlotFromConfirmedHistory()
	require.NoError(t, err,
		"past-horizon is the deferral signal, not an internal failure")
	assert.False(t, ok,
		"bounded era horizon must not resolve the current wall-clock slot")
	assert.Zero(t, slot)
}

func TestWallClockSlotFromConfirmedHistory_EmptyCache(t *testing.T) {
	ls := &LedgerState{
		config: LedgerStateConfig{
			CardanoNodeConfig: newTestEraHistoryCfg(t),
		},
	}
	ls.publishSnapshotsLocked()

	// An empty epoch cache is an internal failure, not the past-horizon
	// deferral signal. It must surface as an error: reporting it as
	// unsupported would let the block producer downgrade a hard startup
	// failure into a warning and carry on with an unjudged certificate.
	slot, ok, err := ls.WallClockSlotFromConfirmedHistory()
	require.Error(t, err, "empty epoch cache must not masquerade as deferral")
	assert.False(t, ok)
	assert.Zero(t, slot)
}

// slotToTimeBehindHorizonState builds a ledger whose applied tip is near
// genesis, with an injected wall clock far ahead of it, so the forecast horizon
// is deterministically behind the current slot regardless of when the suite
// runs.
func slotToTimeBehindHorizonState(
	t *testing.T,
	slotLengthMs uint,
	slotsAhead uint64,
) (*LedgerState, uint64, time.Time) {
	t.Helper()
	cfg := newTestEraHistoryCfg(t)
	systemStart := cfg.ShelleyGenesis().SystemStart
	slotLength := time.Duration(slotLengthMs) * time.Millisecond

	ls := &LedgerState{
		epochCache: []models.Epoch{{
			EpochId:       0,
			StartSlot:     0,
			SlotLength:    slotLengthMs,
			LengthInSlots: 432_000,
			EraId:         eras.ConwayEraDesc.Id,
		}},
		currentEra: eras.ConwayEraDesc,
		currentTip: ochainsync.Tip{Point: ocommon.NewPoint(10, []byte("tip"))},
		config:     LedgerStateConfig{CardanoNodeConfig: cfg},
	}
	// A fixed clock slotsAhead slots past genesis: hermetic, and far enough
	// ahead that the era's safe zone cannot cover it.
	now := systemStart.Add(time.Duration(slotsAhead) * slotLength)
	ls.timeConv().nowFunc = func() time.Time { return now }
	ls.publishSnapshotsLocked()
	return ls, slotsAhead, now
}

// TestSlotToTimeExtrapolatesNextSlotWhileBehindHorizon is the regression test
// for the slot clock spinning on "failed to get next slot time" for the whole
// of a from-genesis sync or a `dingo load`.
//
// The clock's tick loop calls TimeToSlot(now) and then SlotToTime(slot+1). The
// first has a near-now current-era extrapolation for exactly this case; the
// second did not, so on a ledger whose applied tip is still near genesis while
// the wall clock is far ahead, every tick logged an error and retried after
// 100ms instead of sleeping to the next slot boundary.
func TestSlotToTimeExtrapolatesNextSlotWhileBehindHorizon(t *testing.T) {
	t.Parallel()

	const slotLengthMs = 1000
	ls, nowSlot, now := slotToTimeBehindHorizonState(
		t, slotLengthMs, 5_000_000,
	)
	nextSlot := nowSlot + 1

	// Confirm the premise: that slot really is past the bounded horizon, so
	// this test cannot silently become vacuous.
	sum, err := ls.HardForkSummary()
	require.NoError(t, err)
	_, horizonErr := sum.SlotToTime(nextSlot)
	require.ErrorIs(t, horizonErr, hardfork.ErrPastHorizon,
		"premise: the next wall-clock slot must be past the forecast horizon")

	// SlotToTime must still resolve it, by extrapolating the current era.
	when, err := ls.SlotToTime(nextSlot)
	require.NoError(t, err,
		"the slot clock must be able to resolve the next slot while behind")
	assert.Equal(t, now.Add(time.Second), when,
		"the next slot starts exactly one slot length after now")

	// Consecutive slots stay one slot length apart.
	next2, err := ls.SlotToTime(nextSlot + 1)
	require.NoError(t, err)
	assert.Equal(t, time.Second, next2.Sub(when))

	// An arbitrary future slot stays bounded: the escape hatch is only for
	// operational timing, not a general weakening of the horizon.
	_, err = ls.SlotToTime(nextSlot + 1_000_000)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon,
		"a far-future slot must still be past the horizon")

	// Slot 0 keeps its genesis special case.
	genesis, err := ls.SlotToTime(0)
	require.NoError(t, err)
	assert.Equal(t, ls.config.CardanoNodeConfig.ShelleyGenesis().SystemStart,
		genesis)

	// A slot inside the horizon is still answered by the bounded Summary.
	inHorizon, err := ls.SlotToTime(100)
	require.NoError(t, err)
	assert.Equal(t,
		ls.config.CardanoNodeConfig.ShelleyGenesis().SystemStart.
			Add(100*time.Second),
		inHorizon)
}

// previewWedgeLedgerState reproduces the ledger state the from-genesis Preview
// replay was in when it wedged on epoch 40 of the Babbage era, a
// published tip at block 168143 (slot 3516450), and the next two blocks not yet
// reflected in that tip because their batch had not committed. Preview's
// genesis gives the 25920-slot safe zone (see newTestEraHistoryCfg).
func previewWedgeLedgerState(t testing.TB) *LedgerState {
	t.Helper()
	nodeConfig := newTestEraHistoryCfg(t)
	nodeConfig.ShelleyGenesis().NetworkId = "Testnet"
	ls := &LedgerState{
		epochCache: []models.Epoch{{
			EpochId:       previewEraStartEpoch,
			StartSlot:     previewEraStartSlot,
			SlotLength:    1_000,
			LengthInSlots: previewEpochSize,
			EraId:         eras.BabbageEraDesc.Id,
		}},
		currentEra: eras.BabbageEraDesc,
		currentTip: ochainsync.Tip{
			Point: ocommon.NewPoint(
				previewPublishedTipSlot,
				[]byte("published-tip"),
			),
		},
		config: LedgerStateConfig{
			CardanoNodeConfig: nodeConfig,
			Logger:            testLogger(),
		},
	}
	ls.publishSnapshotsLocked()
	return ls
}

// TestLedgerViewSlotToTimeUsesHorizonAnchor pins the routing at the call site
// the fix changes. LedgerView.SlotToTime is the converter every Plutus
// script context translates its validity interval through, so the anchor has to
// reach the summary from there and the horizon has to survive the trip.
func TestLedgerViewSlotToTimeUsesHorizonAnchor(t *testing.T) {
	t.Parallel()

	ls := previewWedgeLedgerState(t)

	// Unanchored, this is the wedge: the view falls back to the published tip
	// and refuses the transaction's validity bound.
	unanchored := &LedgerView{ls: ls}
	_, err := unanchored.SlotToTime(previewTxUpperBound)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon,
		"the published tip must still leave this bound past the horizon; "+
			"if it does not, the fixture no longer reproduces #3844")

	// Anchored at the applied block's predecessor, the same bound converts.
	anchored := &LedgerView{ls: ls, horizonAnchorSlot: previewParentSlot}
	when, err := anchored.SlotToTime(previewTxUpperBound)
	require.NoError(t, err,
		"a Plutus validity bound inside the predecessor-anchored horizon "+
			"must translate")
	expected, err := ls.hardForkSummaryAnchoredAt(previewParentSlot)
	require.NoError(t, err)
	wantTime, err := expected.SlotToTime(previewTxUpperBound)
	require.NoError(t, err)
	assert.Equal(t, wantTime, when)

	// The anchor moves the horizon; it does not remove it. cardano-ledger
	// fails a Plutus transaction whose bound cannot be translated
	// (TimeTranslationPastHorizon), so this must stay an error rather than
	// become an in-era extrapolation.
	_, err = anchored.SlotToTime(previewParentHorizon)
	require.ErrorIs(t, err, hardfork.ErrPastHorizon,
		"a bound past the anchored horizon must still be refused")
}
