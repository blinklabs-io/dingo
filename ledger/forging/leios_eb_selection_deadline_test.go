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
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger/conway"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// leiosCandidateTxs builds count mempool transactions that all pass
// validLeiosTransactionReference, so selection cost -- not reference
// filtering -- is what the test observes.
func leiosCandidateTxs(t *testing.T, count int) []MempoolTransaction {
	t.Helper()
	txs := make([]MempoolTransaction, 0, count)
	for i := range count {
		txs = append(txs, MempoolTransaction{
			Hash: fmt.Sprintf("%064x", i+1),
			Cbor: makeMinimalTxCbor(t, byte(i+1), 0),
			Type: conway.TxTypeConway,
		})
	}
	return txs
}

// TestLeiosEBSelectionStopsAtTheDeadline is the endorser-block half of the
// lost-slot defect fixed for ranking blocks in #3988. Endorser-block
// selection re-validated every mempool candidate serially with no clock, so
// on a chain holding ~1000 transactions it spent seconds of a 1-second slot
// before the ranking block was even started. An endorser block with fewer
// references beats one that arrives after its slot.
func TestLeiosEBSelectionStopsAtTheDeadline(t *testing.T) {
	const (
		candidates    = 10
		perTxCost     = 10 * time.Millisecond
		selectionTime = 55 * time.Millisecond
	)
	start := time.Now()
	fakeNow := start
	validator := &sessionMockTxValidator{}
	validator.onValidate = func(int) { fakeNow = fakeNow.Add(perTxCost) }

	selected, truncated, err := selectValidLeiosTransactions(
		leiosCandidateTxs(t, candidates),
		validator,
		leiosSelectionLimits{
			now:      func() time.Time { return fakeNow },
			deadline: start.Add(selectionTime),
		},
	)
	require.NoError(t, err)
	require.True(t, truncated)
	// Checks land at 0, 10, 20, 30, 40 and 50ms; the check at 60ms stops
	// the pass.
	require.Len(t, selected, 6)
	require.Equal(t, 6, validator.validateCalls)
}

// TestLeiosEBSelectionCompletesWithinBudget is the negative case: a pass
// that finishes before the deadline must not report truncation or drop
// candidates.
func TestLeiosEBSelectionCompletesWithinBudget(t *testing.T) {
	validator := &sessionMockTxValidator{}
	selected, truncated, err := selectValidLeiosTransactions(
		leiosCandidateTxs(t, 5),
		validator,
		leiosSelectionLimits{
			now:      time.Now,
			deadline: time.Now().Add(time.Hour),
		},
	)
	require.NoError(t, err)
	require.False(t, truncated)
	require.Len(t, selected, 5)
}

// TestLeiosEBSelectionAbortsWhenSnapshotChanges mirrors the ranking-block
// fix: stillCurrent() was consulted once, after every candidate had already
// been re-validated, so the whole pass was paid for and then discarded.
func TestLeiosEBSelectionAbortsWhenSnapshotChanges(t *testing.T) {
	validator := &sessionMockTxValidator{staleAfterCalls: 1}
	_, _, err := selectValidLeiosTransactions(
		leiosCandidateTxs(t, 10),
		validator,
		leiosSelectionLimits{now: time.Now},
	)
	require.ErrorIs(t, err, errTxValidationSnapshotChanged)
	require.Equal(
		t,
		1,
		validator.validateCalls,
		"selection must stop at the first check after the snapshot moved",
	)
}

// TestLeiosEBSelectionWithoutDeadlineIsUnbounded keeps the zero value
// meaning what it did before: no clock, no bound.
func TestLeiosEBSelectionWithoutDeadlineIsUnbounded(t *testing.T) {
	validator := &sessionMockTxValidator{}
	selected, truncated, err := selectValidLeiosTransactions(
		leiosCandidateTxs(t, 8),
		validator,
		leiosSelectionLimits{},
	)
	require.NoError(t, err)
	require.False(t, truncated)
	require.Len(t, selected, 8)
}

// ebTestSlotClock is forgerTestSlotClock with a controllable slot-end
// instant so a test can place the forge inside or past its slot without
// sleeping.
type ebTestSlotClock struct {
	currentSlot       uint64
	chainTipSlot      uint64
	slotsPerKESPeriod uint64
	slotEnd           time.Time
}

func (c *ebTestSlotClock) CurrentSlot() (uint64, error) {
	return c.currentSlot, nil
}

func (c *ebTestSlotClock) SlotsPerKESPeriod() uint64 {
	return c.slotsPerKESPeriod
}

func (c *ebTestSlotClock) ChainTipSlot() uint64 { return c.chainTipSlot }

func (c *ebTestSlotClock) NextSlotTime() (time.Time, error) {
	return c.slotEnd, nil
}

func (c *ebTestSlotClock) UpstreamTipSlot() uint64 { return 0 }

func (c *ebTestSlotClock) UpstreamSyncStatus() (uint64, bool) {
	return 0, false
}

// TestCheckAndForgeProductionSkipsEBWhenSlotIsOver pins what a late slot
// does: nothing. The endorser block's hash is committed into the
// ranking-block header, so once the slot has closed, selecting would only
// delay the block that actually extends the chain -- and the endorser
// block is announced by that same ranking block, so a ranking block
// orphaned for being late cannot carry it either. The ranking block is
// still forged and broadcast.
func TestCheckAndForgeProductionSkipsEBWhenSlotIsOver(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	broadcaster := &forgerTestBroadcaster{}
	leiosCaster := &forgerTestLeiosCaster{}
	validator := &sessionMockTxValidator{}

	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock: &ebTestSlotClock{
			currentSlot:       10,
			chainTipSlot:      9,
			slotsPerKESPeriod: 100,
			// The slot has already ended.
			slotEnd: time.Now(),
		},
		LeiosProduceChecker:     &forgerTestLeiosChecker{allowed: true},
		LeiosEBBroadcaster:      leiosCaster,
		LeiosTxValidator:        validator,
		ForgeEBSelectionReserve: 300 * time.Millisecond,
		LeiosMempool: forgerTestMempoolProvider{
			txs: leiosCandidateTxs(t, 20),
		},
		PromRegistry: prometheus.NewRegistry(),
	})
	require.NoError(t, err)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Empty(
		t,
		leiosCaster.txBodies,
		"a slot that has closed produces no endorser block",
	)
	require.Zero(
		t,
		validator.validateCalls,
		"a late slot must not re-validate the mempool at all",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.leiosEbSkipped.WithLabelValues("slot_expired"),
		),
	)
	require.Equal(
		t,
		1,
		broadcaster.calls,
		"the ranking block is still forged and broadcast",
	)
}
