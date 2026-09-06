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

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/blinklabs-io/gouroboros/ledger/dijkstra"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func mempoolOfSize(t *testing.T, count int) *mockMempool {
	t.Helper()
	txs := make([]MempoolTransaction, 0, count)
	for i := range count {
		txs = append(txs, MempoolTransaction{
			Hash: string(rune('a' + i)),
			Cbor: makeMinimalTxCbor(t, byte(i+1), 0),
			Type: conway.TxTypeConway,
		})
	}
	return &mockMempool{transactions: txs}
}

// TestSelectionStopsAtTheSlotDeadline pins the cost bound on transaction
// selection. Every candidate costs a full ledger re-validation, so a large
// mempool makes a selection pass run for seconds -- long past the slot it
// is building for, and long enough for a ledger publication to land in the
// middle of it. Selection must stop at the deadline and forge what it has.
func TestSelectionStopsAtTheSlotDeadline(t *testing.T) {
	const (
		mempoolSize   = 10
		perTxCost     = 10 * time.Millisecond
		selectionTime = 55 * time.Millisecond
	)
	start := time.Now()
	fakeNow := start
	validator := &sessionMockTxValidator{}
	validator.onValidate = func(int) { fakeNow = fakeNow.Add(perTxCost) }

	builder := newSelectionTestBuilder(
		t,
		mempoolOfSize(t, mempoolSize),
		selectionTestChainTip(),
		validator,
	)
	builder.now = func() time.Time { return fakeNow }

	generation := builder.creds.acquireCredentialGeneration()
	defer generation.release()
	block, _, err := builder.buildBlockWithCredentialGeneration(
		1001,
		0,
		LeiosBlockData{},
		generation,
		blockSelectionConstraints{deadline: start.Add(selectionTime)},
	)
	require.NoError(t, err)
	// Six candidates are considered before the clock reaches the
	// deadline (checks at 0, 10, 20, 30, 40, 50ms; the check at 60ms
	// stops the pass).
	require.Len(t, block.Transactions(), 6)
	require.Equal(t, 6, validator.validateCalls)
	require.False(
		t,
		fakeNow.After(start.Add(selectionTime).Add(perTxCost)),
		"selection must not run past the deadline by more than one candidate",
	)
}

// TestSelectionAbortsAsSoonAsTheSnapshotChanges is the wasted-work half of
// the lost-slot defect: stillCurrent() was consulted only after the whole
// pass, so a producer kept re-validating transactions against a snapshot
// that had already been superseded before throwing all of it away.
func TestSelectionAbortsAsSoonAsTheSnapshotChanges(t *testing.T) {
	validator := &sessionMockTxValidator{staleAfterCalls: 1}
	builder := newSelectionTestBuilder(
		t,
		mempoolOfSize(t, 10),
		selectionTestChainTip(),
		validator,
	)

	block, _, err := builder.BuildBlock(1001, 0)
	require.Error(t, err)
	require.Nil(t, block)
	require.ErrorIs(t, err, errTxValidationSnapshotChanged)
	require.Equal(
		t,
		1,
		validator.validateCalls,
		"selection must stop at the first check after the snapshot moved",
	)
}

// TestSelectionSkipsValidatingTransactionsThatCannotFit removes the other
// half of the exposure window: a candidate that cannot fit in the block
// body was still paying for a full ledger re-validation before the size
// check rejected it.
func TestSelectionSkipsValidatingTransactionsThatCannotFit(t *testing.T) {
	txCbor := makeMinimalTxCbor(t, 0x01, 0)
	// MaxBlockBodySize is exactly one transaction, which the encoded
	// Dijkstra block body wrapper always exceeds.
	pparams := &dijkstra.DijkstraProtocolParameters{
		ConwayProtocolParameters: conway.ConwayProtocolParameters{
			MaxTxSize:        uint(len(txCbor)),
			MaxBlockBodySize: uint(len(txCbor)),
			ProtocolVersion: lcommon.ProtocolParametersProtocolVersion{
				Major: 10,
			},
			MaxBlockExUnits: lcommon.ExUnits{
				Memory: 62000000,
				Steps:  20000000000,
			},
		},
	}
	validator := &sessionMockTxValidator{}
	builder, err := NewDefaultBlockBuilder(BlockBuilderConfig{
		Mempool: &mockMempool{
			transactions: []MempoolTransaction{
				{
					Hash: "tx1",
					Cbor: txCbor,
					Type: dijkstra.TxTypeDijkstra,
				},
			},
		},
		PParamsProvider: &mockPParamsProvider{pparams: pparams},
		ChainTip: &mockChainTip{
			tip: ochainsync.Tip{
				Point: ocommon.Point{
					Slot: 1000,
					Hash: make([]byte, 32),
				},
				BlockNumber: 100,
			},
		},
		EpochNonce: &mockEpochNonceProvider{
			epoch: 1,
			nonce: make([]byte, 32),
		},
		Credentials: setupTestCredentials(t),
		TxValidator: validator,
	})
	require.NoError(t, err)

	block, _, err := builder.BuildBlock(1001, 0)
	require.NoError(t, err)
	require.Empty(t, block.Transactions())
	require.Zero(
		t,
		validator.validateCalls,
		"a transaction that cannot fit must not be re-validated first",
	)
}

// constraintRecordingBuilder captures the constraints the forge loop hands
// to the builder on the production path.
type constraintRecordingBuilder struct {
	block       ledger.Block
	cbor        []byte
	constraints []blockSelectionConstraints
}

func (b *constraintRecordingBuilder) BuildBlock(
	uint64,
	uint64,
) (ledger.Block, []byte, error) {
	return b.block, b.cbor, nil
}

func (b *constraintRecordingBuilder) buildBlockWithCredentialGeneration(
	_ uint64,
	_ uint64,
	_ LeiosBlockData,
	_ *credentialGeneration,
	constraints blockSelectionConstraints,
) (ledger.Block, []byte, error) {
	b.constraints = append(b.constraints, constraints)
	return b.block, b.cbor, nil
}

var _ credentialGenerationBlockBuilder = (*constraintRecordingBuilder)(nil)

// TestBuildDingoForgeHandsTheSlotDeadlineToSelection follows the runtime
// composition path a production forge takes -- checkAndForgeProduction ->
// buildBlockForSlot -> buildBlock -> buildBlockWithCredentialGeneration --
// and proves the slot deadline actually arrives at the builder. A deadline
// that exists in the config but never reaches selection bounds nothing.
func TestBuildDingoForgeHandsTheSlotDeadlineToSelection(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &constraintRecordingBuilder{block: block, cbor: block.cbor}
	slotEnd := time.Now().Add(2 * time.Second)
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           slotEnd,
	}
	forger := newRetryForger(t, clock, builder, &forgerTestBroadcaster{})

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, builder.constraints, 1)
	require.Equal(
		t,
		slotEnd.Add(-defaultForgeSelectionRetryMargin),
		builder.constraints[0].deadline,
		"selection must be bounded by the end of the slot being forged, less the retry margin",
	)
	require.False(t, builder.constraints[0].emptyBody)
}

// TestForgeDropsTheSelectionDeadlineWhenTheSlotIsOver is the other half:
// once the slot has passed there is nothing left to protect, and cutting
// selection short would drop transactions without recovering any of it.
func TestForgeDropsTheSelectionDeadlineWhenTheSlotIsOver(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &constraintRecordingBuilder{block: block, cbor: block.cbor}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now(),
	}
	forger := newRetryForger(t, clock, builder, &forgerTestBroadcaster{})

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, builder.constraints, 1)
	require.True(t, builder.constraints[0].deadline.IsZero())
}

// TestForgeWithCustomBuilderIgnoresTheSelectionDeadline keeps embedders
// working. A BlockBuilder that predates per-attempt constraints cannot be
// handed a deadline, and an unbounded selection pass is exactly what it
// always did, so the deadline is dropped rather than failing the forge.
// Only the empty-body fallback genuinely needs builder support.
func TestForgeWithCustomBuilderIgnoresTheSelectionDeadline(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(2 * time.Second),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 1, builder.calls)
	require.Equal(t, 1, broadcaster.calls)
}

// advancingSlotClock crosses a slot boundary between the leader check and
// the deadline computation: the first reading is the slot being forged, and
// every later one is the slot after it. That is the ordinary way a forge
// runs out of slot -- the leader check, the Leios payload and the KES
// update all happen first -- and it is the only way the clock can disagree
// with the slot under construction, because the forge slot is that same
// clock's first answer.
type advancingSlotClock struct {
	slot              uint64
	chainTipSlot      uint64
	slotsPerKESPeriod uint64
	// nextSlotEnd is the boundary the clock reports once it has already
	// left the forged slot: the end of the *following* slot, which is
	// budget this forge does not have.
	nextSlotEnd time.Time
	calls       int
}

func (c *advancingSlotClock) CurrentSlot() (uint64, error) {
	c.calls++
	if c.calls == 1 {
		return c.slot, nil
	}
	return c.slot + 1, nil
}

func (c *advancingSlotClock) SlotsPerKESPeriod() uint64 {
	return c.slotsPerKESPeriod
}

func (c *advancingSlotClock) ChainTipSlot() uint64 { return c.chainTipSlot }

func (c *advancingSlotClock) NextSlotTime() (time.Time, error) {
	return c.nextSlotEnd, nil
}

func (c *advancingSlotClock) UpstreamTipSlot() uint64 { return 0 }

func (c *advancingSlotClock) UpstreamSyncStatus() (uint64, bool) {
	return 0, false
}

// TestForgeDropsTheSelectionDeadlineWhenTheClockHasLeftTheSlot pins the
// guard that makes the boundary answer trustworthy. NextSlotTime is derived
// from the clock's own current slot, so once the clock has moved on it
// describes the *next* slot's end -- a budget the block being forged does
// not have, and one long enough to keep selection and its retries running
// well past the slot they belong to. Reading the clock slot first and
// treating a mismatch as "the slot is over" is what keeps that from
// happening; without it a late forge is handed a full extra slot.
func TestForgeDropsTheSelectionDeadlineWhenTheClockHasLeftTheSlot(
	t *testing.T,
) {
	block := newForgerTestBlock(10, 2)
	builder := &constraintRecordingBuilder{block: block, cbor: block.cbor}
	clock := &advancingSlotClock{
		slot:              10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		nextSlotEnd:       time.Now().Add(time.Hour),
	}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Len(t, builder.constraints, 1)
	require.True(
		t,
		builder.constraints[0].deadline.IsZero(),
		"a slot the clock has already left leaves no selection budget, "+
			"so the next slot's boundary must not become this slot's deadline",
	)
}
