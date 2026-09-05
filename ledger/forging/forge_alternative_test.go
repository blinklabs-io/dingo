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
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const altTestRivalBlockNumber = uint64(42)

func altTestParentPoint() ocommon.Point {
	return ocommon.NewPoint(9, []byte("fork-point-hash"))
}

func altTestRivalTip(slot uint64) ochainsync.Tip {
	return ochainsync.Tip{
		Point:       ocommon.NewPoint(slot, []byte("rival-block-hash")),
		BlockNumber: altTestRivalBlockNumber,
	}
}

// forgerTestChainContext implements AlternativeChainContextProvider over a
// fixed answer, standing in for chain.Chain.
type forgerTestChainContext struct {
	parent ocommon.Point
	tip    ochainsync.Tip
	ok     bool
	calls  int
}

func newAltTestChainContext(tipSlot uint64) *forgerTestChainContext {
	return &forgerTestChainContext{
		parent: altTestParentPoint(),
		tip:    altTestRivalTip(tipSlot),
		ok:     true,
	}
}

func (c *forgerTestChainContext) TipPredecessor() (
	ocommon.Point,
	ochainsync.Tip,
	bool,
) {
	c.calls++
	return c.parent, c.tip, c.ok
}

// forgerTestSiblingAdopter implements SiblingBlockAdopter with a fixed
// chain-selection outcome.
type forgerTestSiblingAdopter struct {
	adopted bool
	err     error
	panics  bool
	calls   int
	block   ledger.Block
}

func (a *forgerTestSiblingAdopter) AdoptLocalForgedSibling(
	block ledger.Block,
) (bool, error) {
	a.calls++
	a.block = block
	if a.panics {
		panic("sibling adopter panic")
	}
	return a.adopted, a.err
}

// newAlternativeTestForger is newGateTestForger plus the two optional
// providers that enable equal-slot alternative forging, and an observer count
// so a test can assert that a losing alternative is never published.
func newAlternativeTestForger(
	t *testing.T,
	clock forgerTestSlotClock,
	leader LeaderChecker,
	builder BlockBuilder,
	broadcaster BlockBroadcaster,
	fence ForgeFenceStore,
	chainContext AlternativeChainContextProvider,
	adopter SiblingBlockAdopter,
) *BlockForger {
	t.Helper()
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		ForgeFence:       fence,
		SlotClock:        clock,
		ChainContext:     chainContext,
		SiblingAdopter:   adopter,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger
}

// TestEqualSlotAlternativeLosingChainSelectionIsNotPublished pins the losing
// half of a slot battle. Chain selection kept the block already at the tip, so
// our block was never adopted and must not be diffused, must not clear its
// transactions from the mempool, and must not count as adopted.
//
// The fence, by contrast, must stay burned: the block was signed, and a second
// signature for the same slot is equivocation whether or not the first one won.
func TestEqualSlotAlternativeLosingChainSelectionIsNotPublished(
	t *testing.T,
) {
	leader := &forgerCountingLeader{}
	builder := &forgerTestBuilder{
		block: newForgerTestBlock(10, altTestRivalBlockNumber),
		cbor:  []byte{0x01},
	}
	broadcaster := &forgerTestBroadcaster{}
	adopter := &forgerTestSiblingAdopter{adopted: false}
	fence := &fenceTestStore{}
	published := 0

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		ForgeFence:       fence,
		SlotClock: forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      10,
			slotsPerKESPeriod: 100,
		},
		ChainContext:   newAltTestChainContext(10),
		SiblingAdopter: adopter,
		BlockForged: func(ledger.Block, []byte, time.Duration) {
			published++
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	assert.Equal(t, 1, builder.contextCalls, "the alternative is still built")
	assert.Equal(t, 1, adopter.calls)
	assert.Zero(t, published, "a losing alternative must not be published")
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeForged),
		"the block was forged, which is what forgeForged counts",
	)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.forgeAdopted),
		"a losing alternative was not adopted",
	)
	assert.Equal(
		t,
		[]uint64{10},
		fence.stored,
		"the slot stays burned: the block was signed either way",
	)
}

// TestEqualSlotAlternativeReservesTheFenceBeforeSigning pins the ordering the
// #3734 fence depends on. The slot must be recorded durably before the builder
// is asked for a block, so a crash between signing and adoption still leaves
// the slot unusable.
func TestEqualSlotAlternativeReservesTheFenceBeforeSigning(t *testing.T) {
	fence := &fenceTestStore{}
	builder := &forgerTestBuilder{
		block: newForgerTestBlock(10, altTestRivalBlockNumber),
		cbor:  []byte{0x01},
	}
	// Observed at the moment the builder runs.
	var fenceAtBuild []uint64
	builder.onBuild = func() { fenceAtBuild = append([]uint64(nil), fence.stored...) }

	forger := newAlternativeTestForger(
		t,
		forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      10,
			slotsPerKESPeriod: 100,
		},
		&forgerCountingLeader{},
		builder,
		&forgerTestBroadcaster{},
		fence,
		newAltTestChainContext(10),
		&forgerTestSiblingAdopter{adopted: true},
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 1, builder.contextCalls)
	assert.Equal(
		t,
		[]uint64{10},
		fenceAtBuild,
		"the fence must be reserved before the block is signed",
	)
}

// TestEqualSlotAlternativeRefusesASlotWeAlreadyForged is the anti-equivocation
// companion with the alternative path fully wired. Our own block at the tip,
// or a fence already at the slot, must not produce a second signature for it.
func TestEqualSlotAlternativeRefusesASlotWeAlreadyForged(t *testing.T) {
	leader := &forgerCountingLeader{}
	builder := &forgerTestBuilder{
		block: newForgerTestBlock(10, altTestRivalBlockNumber),
		cbor:  []byte{0x01},
	}
	adopter := &forgerTestSiblingAdopter{adopted: true}
	fence := &fenceTestStore{slot: 10, present: true}
	forger := newAlternativeTestForger(
		t,
		forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      10,
			slotsPerKESPeriod: 100,
		},
		leader,
		builder,
		&forgerTestBroadcaster{},
		fence,
		newAltTestChainContext(10),
		adopter,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	assert.Zero(t, builder.contextCalls, "must not forge a second block")
	assert.Zero(t, builder.calls)
	assert.Zero(t, adopter.calls)
	assert.Empty(t, fence.stored, "must not advance the fence")
	assert.Zero(
		t,
		leader.callCount(),
		"our own block at our own slot needs no leader selection",
	)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"our own block is not a rival",
	)
}

// TestCheckAndForgeProductionEqualSlotDeclinesWithoutTheAlternativePath keeps
// the pre-alternative behaviour observable for any node that is not wired for
// it: the contested slot still reaches leader selection, is still counted as a
// leader slot, a slot battle and a could-not-forge, and is still logged rather
// than dropped in silence. Nothing is built and nothing is signed.
func TestCheckAndForgeProductionEqualSlotDeclinesWithoutTheAlternativePath(
	t *testing.T,
) {
	tests := []struct {
		name         string
		chainContext AlternativeChainContextProvider
		adopter      SiblingBlockAdopter
	}{
		{name: "nothing wired"},
		{
			name:         "no sibling adopter",
			chainContext: newAltTestChainContext(10),
		},
		{
			name:    "no chain context",
			adopter: &forgerTestSiblingAdopter{adopted: true},
		},
		{
			name: "tip has no resolvable predecessor",
			chainContext: &forgerTestChainContext{
				ok: false,
			},
			adopter: &forgerTestSiblingAdopter{adopted: true},
		},
		{
			name:         "tip is no longer at the contested slot",
			chainContext: newAltTestChainContext(9),
			adopter:      &forgerTestSiblingAdopter{adopted: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			leader := &forgerCountingLeader{}
			builder := &forgerTestBuilder{}
			broadcaster := &forgerTestBroadcaster{}
			fence := &fenceTestStore{}
			forger := newAlternativeTestForger(
				t,
				forgerTestSlotClock{
					currentSlot:       10,
					chainTipSlot:      10,
					slotsPerKESPeriod: 100,
				},
				leader,
				builder,
				broadcaster,
				fence,
				tt.chainContext,
				tt.adopter,
			)

			require.NoError(
				t,
				forger.checkAndForgeProduction(context.Background()),
			)

			assert.Equal(t, 1, leader.callCount())
			assert.Equal(
				t,
				float64(1),
				testutil.ToFloat64(forger.metrics.slotBattlesTotal),
			)
			assert.Equal(
				t,
				float64(1),
				testutil.ToFloat64(forger.metrics.forgeCouldNot),
			)
			assert.Zero(t, builder.calls)
			assert.Zero(t, builder.contextCalls)
			assert.Zero(t, broadcaster.calls)
			assert.Empty(
				t,
				fence.stored,
				"a declined battle must not burn the slot",
			)
		})
	}
}

// TestEqualSlotAlternativeRecoversAdopterPanic keeps a misbehaving adopter
// from taking down the forge loop goroutine, matching addBlockSafe.
func TestEqualSlotAlternativeRecoversAdopterPanic(t *testing.T) {
	builder := &forgerTestBuilder{
		block: newForgerTestBlock(10, altTestRivalBlockNumber),
		cbor:  []byte{0x01},
	}
	forger := newAlternativeTestForger(
		t,
		forgerTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      10,
			slotsPerKESPeriod: 100,
		},
		&forgerCountingLeader{},
		builder,
		&forgerTestBroadcaster{},
		nil,
		newAltTestChainContext(10),
		&forgerTestSiblingAdopter{panics: true},
	)

	err := forger.checkAndForgeProduction(context.Background())
	require.ErrorContains(t, err, "sibling block adopter panic")
	assert.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}
