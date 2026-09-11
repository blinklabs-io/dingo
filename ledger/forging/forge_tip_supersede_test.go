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
// are the same event, so the tip read that cleared the forge's entry gates
// is stale by the time the retry or the fallback is decided.
type tipMovingBuilder struct {
	block ledger.Block
	cbor  []byte
	clock *retryTestSlotClock
	// tipDuringBuild is where the chain tip lands during the first
	// selection pass.
	tipDuringBuild uint64
	calls          int
	emptyCalls     int
	selectErr      error
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
		b.clock.chainTipSlot = b.tipDuringBuild
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
// which only ledger.validateBlockOrder at AddBlock would have caught.
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
	_, _, _, err := forger.buildBlockForSlot(10, 0, leiosState, nil)
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
