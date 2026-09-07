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
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// errFallbackBuilderRefused stands in for the reasons an empty-body build can
// fail that have nothing to do with the mempool: an embedder's BlockBuilder
// that cannot honour the empty-body constraint, missing VRF or KES material.
var errFallbackBuilderRefused = errors.New(
	"block builder cannot honour the empty-body constraint",
)

// TestForgeLostSlotErrorCarriesTheFallbackFailure covers the error the caller
// is handed when both selection and the transaction-free fallback fail.
//
// The selection abort explains why the fallback was reached; it is the
// fallback's own failure that explains why the slot produced nothing. Only
// the selection error used to be returned, so a caller's errors.Is/As saw
// "transaction validation snapshot changed" and nothing else, and an operator
// went looking at the mempool for a fault in their key material or in a
// custom BlockBuilder.
func TestForgeLostSlotErrorCarriesTheFallbackFailure(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &fallbackTestBuilder{
		block:     block,
		cbor:      block.cbor,
		selectErr: errTxValidationSnapshotChanged,
		emptyErr:  errFallbackBuilderRefused,
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now(),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	err := forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.Equal(t, 1, builder.emptyCalls)

	// Both halves stay inspectable: the abort that sent the forge to the
	// fallback, and the fallback failure that actually lost the slot.
	require.ErrorIs(
		t,
		err,
		errTxValidationSnapshotChanged,
		"the selection abort must remain inspectable",
	)
	require.ErrorIs(
		t,
		err,
		errFallbackBuilderRefused,
		"the fallback failure is the cause of the lost slot and must be inspectable",
	)
	require.Contains(t, err.Error(), errFallbackBuilderRefused.Error())
}

// newOutcomeForger builds a production forger whose logs land in logs and
// whose optional self-validation and adoption steps can be made to fail.
func newOutcomeForger(
	t *testing.T,
	logs *bytes.Buffer,
	clock *retryTestSlotClock,
	builder BlockBuilder,
	broadcaster *forgerTestBroadcaster,
	validator BlockValidator,
) *BlockForger {
	t.Helper()
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(logs, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		BlockValidator:   validator,
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger
}

func newOutcomeClock() *retryTestSlotClock {
	return &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(time.Second),
	}
}

// TestForgeTimingReportsAdoptionForASuccessfulSlot pins the shape of the line
// for the ordinary case, so the adopted field the failure cases below rely on
// is known to be true when the block really reaches the chain.
func TestForgeTimingReportsAdoptionForASuccessfulSlot(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{block: block, cbor: block.cbor}
	var logs bytes.Buffer
	forger := newOutcomeForger(
		t,
		&logs,
		newOutcomeClock(),
		builder,
		&forgerTestBroadcaster{},
		nil,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	record := forgeTimingRecord(t, logs.String())
	require.Equal(t, "forged", record["outcome"])
	require.Equal(t, true, record["adopted"])
}

// TestForgeTimingDoesNotClaimSuccessWhenSelfValidationDropsTheBlock is the
// regression for the timing line describing an intermediate outcome.
//
// The line used to be emitted as soon as the block was built, so a block that
// self-validation then dropped left a record saying outcome=forged for a slot
// that put nothing on the chain. The line is the one an operator reads when a
// slot yielded no block, and it was pointing away from the failure.
func TestForgeTimingDoesNotClaimSuccessWhenSelfValidationDropsTheBlock(
	t *testing.T,
) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	var logs bytes.Buffer
	forger := newOutcomeForger(
		t,
		&logs,
		newOutcomeClock(),
		builder,
		broadcaster,
		&forgerTestValidator{err: errors.New("bad VRF proof")},
	)

	require.Error(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 0, broadcaster.calls, "the block must not be adopted")

	record := forgeTimingRecord(t, logs.String())
	require.Equal(
		t,
		false,
		record["adopted"],
		"a dropped block must not be recorded as adopted",
	)
	// The block was built, and saying so is the point of keeping outcome
	// separate from adopted: this slot failed after selection, not during
	// it, which is a different fault to chase.
	require.Equal(t, "forged", record["outcome"])
}

// TestForgeTimingDoesNotClaimSuccessWhenAdoptionFails covers the other
// post-build loss: the block is built and self-validated, and AddBlock
// rejects it.
func TestForgeTimingDoesNotClaimSuccessWhenAdoptionFails(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{
		err: errors.New("block does not fit on the current chain tip"),
	}
	var logs bytes.Buffer
	forger := newOutcomeForger(
		t,
		&logs,
		newOutcomeClock(),
		builder,
		broadcaster,
		nil,
	)

	require.Error(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 1, broadcaster.calls)

	record := forgeTimingRecord(t, logs.String())
	require.Equal(t, false, record["adopted"])
	require.Equal(t, "forged", record["outcome"])
}

// TestForgeTimingIsEmittedWhenTheEmptyFallbackIsAdopted keeps the fallback's
// own success honest: an empty block that reaches the chain is a kept slot.
func TestForgeTimingIsEmittedWhenTheEmptyFallbackIsAdopted(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &fallbackTestBuilder{
		block:     block,
		cbor:      block.cbor,
		selectErr: errTxValidationSnapshotChanged,
	}
	var logs bytes.Buffer
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now(),
	}
	forger := newOutcomeForger(
		t,
		&logs,
		clock,
		builder,
		&forgerTestBroadcaster{},
		nil,
	)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	record := forgeTimingRecord(t, logs.String())
	require.Equal(t, "empty", record["outcome"])
	require.Equal(t, true, record["adopted"])
}
