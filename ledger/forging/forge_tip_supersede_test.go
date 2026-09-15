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

package forging

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// tipMovingBuilder aborts its first selection pass the way a concurrent
// ledger publication does, and moves the chain tip while doing it. That is
// the real shape of the window this fix closes: the abort and the tip move
// are the same event, so the reading that cleared the forge's entry gates
// is stale by the time the retry or the fallback is decided.
type tipMovingBuilder struct {
	block ledger.Block
	cbor  []byte
	clock *retryTestSlotClock
	// tipDuringBuild is where the applied tip (and, with it, the primary
	// chain tip) lands during the first selection pass.
	tipDuringBuild uint64
	// moveTip, when set, replaces the tipDuringBuild move: it is how a test
	// moves the primary chain tip on its own, leaving the applied tip where
	// it was, to express an apply backlog opening mid-selection.
	moveTip    func(*retryTestSlotClock)
	calls      int
	emptyCalls int
	selectErr  error
}

func (b *tipMovingBuilder) BuildBlock(
	uint64,
	uint64,
) (ledger.Block, []byte, error) {
	b.calls++
	return nil, nil, b.selectErr
}

func (b *tipMovingBuilder) buildBlockWithCredentialGeneration(
	_ uint64,
	_ uint64,
	_ LeiosBlockData,
	_ *credentialGeneration,
	constraints blockSelectionConstraints,
) (ledger.Block, []byte, error) {
	b.calls++
	if constraints.emptyBody {
		b.emptyCalls++
		return b.block, b.cbor, nil
	}
	if b.calls == 1 {
		if b.moveTip != nil {
			b.moveTip(b.clock)
		} else {
			b.clock.chainTipSlot = b.tipDuringBuild
		}
	}
	return nil, nil, b.selectErr
}

var _ credentialGenerationBlockBuilder = (*tipMovingBuilder)(nil)

// requireNoFallbackCounted asserts the slot was refused rather than saved or
// lost by the fallback: refusing before building is not a fallback outcome,
// and reporting one would credit or blame a path that never ran.
func requireNoFallbackCounted(t *testing.T, forger *BlockForger) {
	t.Helper()
	for _, result := range []string{
		forgeSelectionResultRetried,
		forgeSelectionResultEmpty,
		forgeSelectionResultLost,
	} {
		require.Equal(
			t,
			float64(0),
			testutil.ToFloat64(
				forger.metrics.forgeSelectionFallback.WithLabelValues(
					result,
				),
			),
			"no fallback outcome may be counted for result=%s",
			result,
		)
	}
}

// TestForgeRefusesTheFallbackWhenTheTipPassedTheForgedSlot is the blocker
// this test file exists for. The entry gate refuses a slot the chain has
// already covered; the fallback runs precisely because the chain moved, and
// used to build, VRF-prove and KES-sign against the tip read before that
// move. The resulting block names a parent whose slot is not below its own,
// and nothing local rejects that before diffusion: Chain.AddLocalBlock
// checks only prev-hash and block-number contiguity, so the block is admitted
// and broadcast, and ledger.validateBlockOrder runs only later, from
// ledgerProcessBlock, when the ledger applies it.
func TestForgeRefusesTheFallbackWhenTheTipPassedTheForgedSlot(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		// No slot time left, so the retry path is exhausted immediately
		// and the fallback is the only thing that can reach a build.
		slotEnd: time.Now(),
	}
	builder := &tipMovingBuilder{
		block:          block,
		cbor:           block.cbor,
		clock:          clock,
		tipDuringBuild: 11,
		selectErr:      errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(
		t,
		forger.checkAndForgeProduction(context.Background()),
		"a slot the chain took is declined, not an error",
	)
	require.Equal(
		t,
		0,
		builder.emptyCalls,
		"no block may be built for a slot the chain has passed",
	)
	require.Equal(t, 0, broadcaster.calls)
	requireNoFallbackCounted(t, forger)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"a tip beyond the slot is not a slot battle",
	)
}

// TestForgeCountsASlotBattleWhenTheTipReachesTheForgedSlot pins the equal
// case, which the entry gate counts and the fallback used to reach silently.
// Slot time remains here, so the refusal comes from the retry loop rather
// than from the fallback.
func TestForgeCountsASlotBattleWhenTheTipReachesTheForgedSlot(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(2 * time.Second),
	}
	builder := &tipMovingBuilder{
		block:          block,
		cbor:           block.cbor,
		clock:          clock,
		tipDuringBuild: 10,
		selectErr:      errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(
		t,
		1,
		builder.calls,
		"the retry must not re-enter the builder against the moved tip",
	)
	require.Equal(t, 0, builder.emptyCalls)
	require.Equal(t, 0, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"a rival block on our leader slot is the same battle whether it arrives before the forge or during it",
	)
	requireNoFallbackCounted(t, forger)
}

// TestForgeRefusesTheFallbackAsASlotBattle covers the equal case reaching
// the fallback itself, which is the path with no slot time left.
func TestForgeRefusesTheFallbackAsASlotBattle(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now(),
	}
	builder := &tipMovingBuilder{
		block:          block,
		cbor:           block.cbor,
		clock:          clock,
		tipDuringBuild: 10,
		selectErr:      errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 0, builder.emptyCalls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
	)
	requireNoFallbackCounted(t, forger)
}

// TestBuildBlockForSlotReportsBothTheAbortAndTheSupersededSlot keeps the
// refusal diagnosable: the selection abort explains why the fallback was
// reached, the tip error explains why it was refused, and an operator needs
// both to tell a superseded slot from a broken builder.
func TestBuildBlockForSlotReportsBothTheAbortAndTheSupersededSlot(
	t *testing.T,
) {
	block := newForgerTestBlock(10, 2)
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now(),
	}
	builder := &tipMovingBuilder{
		block:          block,
		cbor:           block.cbor,
		clock:          clock,
		tipDuringBuild: 11,
		selectErr:      errTxValidationSnapshotChanged,
	}
	forger := newRetryForger(t, clock, builder, &forgerTestBroadcaster{})

	leiosState := &forgeLeiosState{}
	_, _, _, err := forger.buildBlockForSlot(
		10,
		0,
		leiosState,
		nil,
		forgeTipGates{},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errTxValidationSnapshotChanged)
	require.ErrorIs(t, err, errChainTipAheadOfSlot)
}

// TestForgeCountsTheEmptyFallbackOnlyAfterAdoption pins the counter move.
// dingo_forge_selection_fallback_total{result="empty"} is how the fix is
// read in the field, so it has to mean "the fallback saved this slot", not
// "the fallback built something".
func TestForgeCountsTheEmptyFallbackOnlyAfterAdoption(t *testing.T) {
	build := func(t *testing.T, adopt bool) *BlockForger {
		t.Helper()
		block := newForgerTestBlock(10, 2)
		builder := &fallbackTestBuilder{
			block:     block,
			cbor:      block.cbor,
			selectErr: errTxValidationSnapshotChanged,
		}
		broadcaster := &forgerTestBroadcaster{}
		if !adopt {
			broadcaster.err = errors.New("chain refused the block")
		}
		clock := &retryTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			slotEnd:           time.Now(),
		}
		forger := newRetryForger(t, clock, builder, broadcaster)
		err := forger.checkAndForgeProduction(context.Background())
		if adopt {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
		require.Equal(t, 1, builder.emptyCalls)
		return forger
	}

	t.Run("adopted", func(t *testing.T) {
		forger := build(t, true)
		require.Equal(
			t,
			float64(1),
			testutil.ToFloat64(
				forger.metrics.forgeSelectionFallback.WithLabelValues(
					forgeSelectionResultEmpty,
				),
			),
		)
		require.Equal(
			t,
			float64(0),
			testutil.ToFloat64(
				forger.metrics.forgeSelectionFallback.WithLabelValues(
					forgeSelectionResultLost,
				),
			),
		)
	})

	t.Run("dropped before adoption", func(t *testing.T) {
		forger := build(t, false)
		require.Equal(
			t,
			float64(0),
			testutil.ToFloat64(
				forger.metrics.forgeSelectionFallback.WithLabelValues(
					forgeSelectionResultEmpty,
				),
			),
			"a block that never reached the chain did not save the slot",
		)
		require.Equal(
			t,
			float64(1),
			testutil.ToFloat64(
				forger.metrics.forgeSelectionFallback.WithLabelValues(
					forgeSelectionResultLost,
				),
			),
			"every aborted selection still lands in exactly one bucket",
		)
	})
}

// TestForgeCountsARetriedSlotOnlyAfterAdoption is the same rule for the
// other half of the counter.
func TestForgeCountsARetriedSlotOnlyAfterAdoption(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{
		block:     block,
		cbor:      block.cbor,
		failCount: 1,
		err:       errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{
		err: errors.New("chain refused the block"),
	}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(2 * time.Second),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.Error(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 2, builder.calls)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues(
				forgeSelectionResultRetried,
			),
		),
		"a re-selected block that never reached the chain did not save the slot",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues(
				forgeSelectionResultLost,
			),
		),
	)
}

// The tests below are the second blocker: after #4053 the entry gates decide
// a slot against BOTH tips, and a re-check that reads only the applied tip
// lets through exactly the builds the entry gates refuse. All three share
// one shape -- the applied tip stays below the forged slot for the whole
// attempt, so the applied-tip ordering alone admits every one of them, while
// the primary chain tip, the parent the builder actually uses, has moved to
// where the entry gates would have refused the slot. A retry or the fallback
// runs precisely because the primary tip moved, so this is the window in
// which the two tips are guaranteed to differ.

// applyBacklogClock is a retryTestSlotClock whose primary chain tip is
// driven separately from its applied tip.
func applyBacklogClock(
	currentSlot, appliedSlot uint64,
	appliedHash []byte,
	slotEnd time.Time,
) *retryTestSlotClock {
	return &retryTestSlotClock{
		currentSlot:        currentSlot,
		chainTipSlot:       appliedSlot,
		chainTipHash:       appliedHash,
		primaryTipExplicit: true,
		primaryTipSlot:     appliedSlot,
		primaryTipHash:     appliedHash,
		slotsPerKESPeriod:  100,
		slotEnd:            slotEnd,
	}
}

// requireStaleTipSkips asserts dingo_forge_stale_tip_skip_total holds
// exactly want, one entry per reason, and 0 for every other reason.
func requireStaleTipSkips(
	t *testing.T,
	forger *BlockForger,
	want map[string]float64,
) {
	t.Helper()
	for _, reason := range []string{
		forgeStaleTipReasonSlotGap,
		forgeStaleTipReasonHashDiverged,
		forgeStaleTipReasonPrimaryTipBehind,
		forgeStaleTipReasonUnappliedRival,
		forgeStaleTipReasonAppliedStale,
		forgeStaleTipReasonEbManifestAhead,
	} {
		require.Equal(
			t,
			want[reason],
			testutil.ToFloat64(
				forger.metrics.forgeStaleTipSkip.WithLabelValues(reason),
			),
			"dingo_forge_stale_tip_skip_total{reason=%q}",
			reason,
		)
	}
}

// TestForgeRefusesTheRetryWhenThePrimaryTipReachesTheForgedSlot: a peer's
// block for the forged slot lands on the primary chain tip during selection
// while the ledger has not applied it. At entry this is the
// unapplied-block-at-slot refusal; with slot time left the retry used to
// re-enter the builder and parent a block for slot S on a tip already at S.
func TestForgeRefusesTheRetryWhenThePrimaryTipReachesTheForgedSlot(
	t *testing.T,
) {
	block := newForgerTestBlock(10, 2)
	clock := applyBacklogClock(10, 9, nil, time.Now().Add(2*time.Second))
	builder := &tipMovingBuilder{
		block: block,
		cbor:  block.cbor,
		clock: clock,
		moveTip: func(c *retryTestSlotClock) {
			c.primaryTipSlot = 10
		},
		selectErr: errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(
		t,
		forger.checkAndForgeProduction(context.Background()),
		"a slot the primary chain already holds is declined, not an error",
	)
	require.Equal(
		t,
		1,
		builder.calls,
		"the retry must not re-enter the builder against a primary tip at the forged slot",
	)
	require.Equal(t, 0, builder.emptyCalls)
	require.Equal(t, 0, broadcaster.calls)
	requireNoFallbackCounted(t, forger)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"the rival block is unapplied, which the entry gate does not count as a slot battle",
	)
	requireStaleTipSkips(t, forger, map[string]float64{
		forgeStaleTipReasonUnappliedRival: 1,
	})
}

// TestForgeRefusesTheFallbackWhenThePrimaryTipPassesTheForgedSlot: the
// primary chain tip moves past the forged slot during selection with no slot
// time left, so the transaction-free fallback is the only thing that can
// reach a build. At entry a parent slot past the slot is refused outright;
// the fallback used to build on it.
func TestForgeRefusesTheFallbackWhenThePrimaryTipPassesTheForgedSlot(
	t *testing.T,
) {
	block := newForgerTestBlock(10, 2)
	clock := applyBacklogClock(10, 9, nil, time.Now())
	builder := &tipMovingBuilder{
		block: block,
		cbor:  block.cbor,
		clock: clock,
		moveTip: func(c *retryTestSlotClock) {
			c.primaryTipSlot = 11
		},
		selectErr: errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(
		t,
		0,
		builder.emptyCalls,
		"no block may be built for a slot the primary chain has passed",
	)
	require.Equal(t, 0, broadcaster.calls)
	requireNoFallbackCounted(t, forger)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"a tip beyond the slot is not a slot battle",
	)
	requireStaleTipSkips(t, forger, map[string]float64{})
}

// TestForgeRefusesTheRetryWhenThePrimaryTipIsReplacedAtTheAppliedSlot: chain
// selection replaces the block at the applied tip's slot with a competing
// one at the same slot that the ledger has not applied. Both tips stay below
// the forged slot, so no slot ordering can see it; the entry gate refuses it
// by hash as primary_tip_hash_diverged, and the retry used to build on the
// replacement with transactions chosen against the block it replaced.
func TestForgeRefusesTheRetryWhenThePrimaryTipIsReplacedAtTheAppliedSlot(
	t *testing.T,
) {
	block := newForgerTestBlock(10, 2)
	applied := bytes.Repeat([]byte{0xAA}, 32)
	rival := bytes.Repeat([]byte{0xBB}, 32)
	clock := applyBacklogClock(10, 9, applied, time.Now().Add(2*time.Second))
	builder := &tipMovingBuilder{
		block: block,
		cbor:  block.cbor,
		clock: clock,
		moveTip: func(c *retryTestSlotClock) {
			c.primaryTipHash = rival
		},
		selectErr: errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(
		t,
		1,
		builder.calls,
		"the retry must not re-enter the builder against a replaced parent",
	)
	require.Equal(t, 0, builder.emptyCalls)
	require.Equal(t, 0, broadcaster.calls)
	requireNoFallbackCounted(t, forger)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
	)
	requireStaleTipSkips(t, forger, map[string]float64{
		forgeStaleTipReasonHashDiverged: 1,
	})
}

// TestForgeRefusesTheRetryOnEveryStaleTipReason covers the two remaining
// stale-tip refusals the entry gate applies from the two tips, so the
// per-attempt decision is pinned to the whole of the entry decision rather
// than to the three cases above.
func TestForgeRefusesTheRetryOnEveryStaleTipReason(t *testing.T) {
	applied := bytes.Repeat([]byte{0xAA}, 32)
	cases := map[string]struct {
		appliedSlot uint64
		move        func(*retryTestSlotClock)
		reason      string
	}{
		"primary chain tip behind the applied tip": {
			appliedSlot: 9,
			move: func(c *retryTestSlotClock) {
				c.primaryTipSlot = 8
				c.primaryTipHash = bytes.Repeat([]byte{0xCC}, 32)
			},
			reason: forgeStaleTipReasonPrimaryTipBehind,
		},
		"apply backlog beyond the tolerance": {
			// Applied at 3, primary moves to 9: a gap of 6 against the
			// default tolerance of 5, with the parent slot still below
			// the forged slot so no ordering refusal fires first.
			appliedSlot: 3,
			move: func(c *retryTestSlotClock) {
				c.primaryTipSlot = 9
				c.primaryTipHash = bytes.Repeat([]byte{0xCC}, 32)
			},
			reason: forgeStaleTipReasonSlotGap,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			block := newForgerTestBlock(10, 2)
			clock := applyBacklogClock(
				10,
				tc.appliedSlot,
				applied,
				time.Now().Add(2*time.Second),
			)
			builder := &tipMovingBuilder{
				block:     block,
				cbor:      block.cbor,
				clock:     clock,
				moveTip:   tc.move,
				selectErr: errTxValidationSnapshotChanged,
			}
			broadcaster := &forgerTestBroadcaster{}
			forger := newRetryForger(t, clock, builder, broadcaster)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)
			require.Equal(t, 1, builder.calls)
			require.Equal(t, 0, builder.emptyCalls)
			require.Equal(t, 0, broadcaster.calls)
			requireNoFallbackCounted(t, forger)
			requireStaleTipSkips(t, forger, map[string]float64{
				tc.reason: 1,
			})
		})
	}
}

// TestTipGateRefusalsAreSkipsNotFailures pins the caller's contract: every
// error the per-attempt tip gates can return is the skip the entry gates
// would have taken, so none of them is reported up the forge loop as a
// build failure.
func TestTipGateRefusalsAreSkipsNotFailures(t *testing.T) {
	for _, err := range []error{
		errChainTipAheadOfSlot,
		errChainTipAtSlot,
		errPrimaryTipAtSlot,
		errTipsDisagree,
		errUpstreamSyncing,
	} {
		require.True(t, isTipGateRefusal(err), "%v", err)
		require.True(
			t,
			isTipGateRefusal(
				errors.Join(errTxValidationSnapshotChanged, err),
			),
			"joined with the selection abort: %v",
			err,
		)
	}
	require.False(t, isTipGateRefusal(errTxValidationSnapshotChanged))
	require.False(t, isTipGateRefusal(errBlockConstraintsUnsupported))
}

// TestForgeRefusesABuildAfterARollbackPutsTheTipBehindTheNetwork is the
// upstream-sync half of the per-attempt re-check, and the reason that gate
// had to move inside evaluateTipGates.
//
// The gate is decided from the LEDGER-APPLIED tip, and the applied tip is the
// one input to the gates that can move BACKWARDS during a forge: a rollback
// mid-selection leaves a node that was well inside ForgeSyncToleranceSlots at
// entry far outside it by the time the retry or the fallback builds. While
// the decision was taken once, at entry, and never re-run, the retry and the
// fallback built on the rolled-back view and broadcast the block -- exactly
// what the gate exists to stop -- with dingo_forge_sync_skip_total flat,
// because the only place that counter is reached had already been passed.
//
// Both ends of the ladder are covered: with slot time left the retry must not
// re-enter the builder, and with the slot over the transaction-free fallback
// must not be built either.
func TestForgeRefusesABuildAfterARollbackPutsTheTipBehindTheNetwork(
	t *testing.T,
) {
	cases := map[string]struct {
		slotEnd time.Time
	}{
		// Slot time left: the loop re-applies the gates before the second
		// attempt.
		"before the retry": {slotEnd: time.Now().Add(2 * time.Second)},
		// No slot time left: the retry path is exhausted immediately and
		// the fallback is the only thing that can reach a build.
		"before the transaction-free fallback": {slotEnd: time.Now()},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			block := newForgerTestBlock(10, 2)
			clock := &retryTestSlotClock{
				currentSlot:       10,
				chainTipSlot:      8,
				slotsPerKESPeriod: 100,
				slotEnd:           tc.slotEnd,
				// A live peer whose chain ends at slot 10. At entry the
				// applied tip trails it by 2, inside the tolerance of 3,
				// so the slot is admitted and the forge proceeds.
				upstreamTarget: 10,
				upstreamLive:   true,
			}
			builder := &tipMovingBuilder{
				block: block,
				cbor:  block.cbor,
				clock: clock,
				// The rollback: the applied tip drops to 5, so the same
				// upstream target now leads it by 5, beyond the tolerance.
				tipDuringBuild: 5,
				selectErr:      errTxValidationSnapshotChanged,
			}
			broadcaster := &forgerTestBroadcaster{}
			forger := newRetryForger(
				t,
				clock,
				builder,
				broadcaster,
				withSyncTolerance(3),
			)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
				"a slot refused because the node fell behind the network is declined, not an error",
			)
			require.Equal(
				t,
				1,
				builder.calls,
				"no attempt may be made against a tip the network has left behind",
			)
			require.Equal(
				t,
				0,
				builder.emptyCalls,
				"the transaction-free fallback is a build like any other and the gate refuses it too",
			)
			require.Equal(
				t,
				0,
				broadcaster.calls,
				"nothing may reach the wire from a rolled-back view",
			)
			require.Equal(
				t,
				float64(1),
				testutil.ToFloat64(forger.metrics.forgeSyncSkip),
				"the refusal is counted where the entry gate counts it",
			)
			requireNoFallbackCounted(t, forger)
			require.Equal(
				t,
				float64(0),
				testutil.ToFloat64(forger.metrics.slotBattlesTotal),
				"falling behind the network is not a slot battle",
			)
			requireStaleTipSkips(t, forger, map[string]float64{})
		})
	}
}
