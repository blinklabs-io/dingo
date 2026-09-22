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
	"sync"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/ledger/eras"
	"github.com/blinklabs-io/dingo/ledger/hardfork"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
// silently restoring the per-call O(known epochs) rebuild that issue #2093 was
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
