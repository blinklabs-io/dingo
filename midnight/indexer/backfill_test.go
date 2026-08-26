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

package indexer

import (
	"bytes"
	"errors"
	"log/slog"
	"math"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// backfillHarness records the range backfill asked its BlockIterator for.
type backfillHarness struct {
	idx        *Indexer
	logs       *bytes.Buffer
	calls      int
	startSlot  uint64
	endSlot    uint64
	yieldedNos []uint64
}

// newBackfillHarness builds an indexer whose BlockIterator records the
// requested range and yields the supplied blocks. checkpoint is persisted
// before New so the indexer loads it the way a restart would.
func newBackfillHarness(
	t *testing.T,
	checkpoint uint64,
	ledgerTipSlot func() (uint64, error),
	reg prometheus.Registerer,
	blocks ...models.Block,
) *backfillHarness {
	t.Helper()
	store := setupTestStore(t)
	if checkpoint > 0 {
		require.NoError(t, store.SetBackfillCheckpoint(
			&models.BackfillCheckpoint{
				Phase:    midnightCheckpointPhase,
				LastSlot: checkpoint,
			},
			nil,
		))
	}
	h := &backfillHarness{logs: &bytes.Buffer{}}
	idx, err := New(Config{
		Metadata: store,
		Logger: slog.New(
			slog.NewTextHandler(
				h.logs,
				&slog.HandlerOptions{Level: slog.LevelInfo},
			),
		),
		PromRegistry:  reg,
		LedgerTipSlot: ledgerTipSlot,
		SlotToEpoch: func(slot uint64) (uint64, error) {
			return slot / 100, nil
		},
		BlockIterator: func(startSlot, endSlot uint64, fn func(models.Block) error) error {
			h.calls++
			h.startSlot = startSlot
			h.endSlot = endSlot
			for _, block := range blocks {
				if block.Slot < startSlot || block.Slot >= endSlot {
					continue
				}
				h.yieldedNos = append(h.yieldedNos, block.Number)
				if err := fn(block); err != nil {
					return err
				}
			}
			return nil
		},
		blockDecoder: func(models.Block) ([]lcommon.Transaction, error) {
			return nil, nil
		},
	})
	require.NoError(t, err)
	h.idx = idx
	return h
}

// TestBackfillStopsAtLedgerTip is the core of the Mithril catch-up contract:
// the stored blob store can hold blocks well past the slot the ledger has
// actually applied (a Mithril bootstrap leaves that whole suffix for ordinary
// ledger replay). Backfill must stop at the applied ledger tip so the suffix
// is indexed once, by the live BlockEvent path, instead of twice.
func TestBackfillStopsAtLedgerTip(t *testing.T) {
	t.Parallel()
	h := newBackfillHarness(
		t,
		0,
		func() (uint64, error) { return 250, nil },
		nil,
		testBlock(1, 100, 0xA1),
		testBlock(2, 250, 0xA2),
		testBlock(3, 400, 0xA3), // above the ledger tip: ledger replay owns it
	)

	require.NoError(t, h.idx.Backfill())

	assert.Equal(t, 1, h.calls)
	assert.Equal(t, uint64(0), h.startSlot)
	assert.Equal(
		t,
		uint64(251),
		h.endSlot,
		"backfill must request an exclusive end one slot past the applied ledger tip",
	)
	assert.Equal(t, []uint64{1, 2}, h.yieldedNos)
}

// TestBackfillWithoutLedgerTipScansToMaxSlot keeps the pre-existing behaviour
// for callers that supply no resolver, so tests and embedders that only wire
// BlockIterator still sweep every stored block.
func TestBackfillWithoutLedgerTipScansToMaxSlot(t *testing.T) {
	t.Parallel()
	h := newBackfillHarness(t, 0, nil, nil, testBlock(1, 100, 0xA1))

	require.NoError(t, h.idx.Backfill())

	assert.Equal(t, 1, h.calls)
	assert.Equal(t, uint64(math.MaxUint64), h.endSlot)
}

// TestBackfillLedgerTipErrorIsFatal proves the resolver failure is not
// silently downgraded to a full scan. Scanning to MaxUint64 after a failed
// tip lookup would re-index the replay suffix, which is exactly what bounding
// the range is meant to prevent.
func TestBackfillLedgerTipErrorIsFatal(t *testing.T) {
	t.Parallel()
	tipErr := errors.New("tip unavailable")
	h := newBackfillHarness(
		t,
		0,
		func() (uint64, error) { return 0, tipErr },
		nil,
		testBlock(1, 100, 0xA1),
	)

	err := h.idx.Backfill()

	require.ErrorIs(t, err, tipErr)
	assert.Equal(t, 0, h.calls, "no blocks may be scanned without a target")
}

// TestBackfillSkippedWhenCheckpointPastLedgerTip covers the ordinary restart
// of an already-caught-up node: the checkpoint sits above the applied ledger
// tip, so there is no gap and no iteration to perform.
func TestBackfillSkippedWhenCheckpointPastLedgerTip(t *testing.T) {
	t.Parallel()
	h := newBackfillHarness(
		t,
		500,
		func() (uint64, error) { return 250, nil },
		nil,
		testBlock(1, 100, 0xA1),
	)

	require.NoError(t, h.idx.Backfill())

	assert.Equal(t, 0, h.calls)
}

// TestBackfillLogsCatchUpGap covers the startup detection requirement: an
// operator must be able to see, from the logs alone, how far behind the
// indexer is before the sweep begins.
func TestBackfillLogsCatchUpGap(t *testing.T) {
	t.Parallel()
	h := newBackfillHarness(
		t,
		100,
		func() (uint64, error) { return 900, nil },
		nil,
		testBlock(1, 100, 0xA1),
	)

	require.NoError(t, h.idx.Backfill())

	logs := h.logs.String()
	assert.Contains(t, logs, "checkpoint_slot=100")
	assert.Contains(t, logs, "target_slot=900")
	assert.Contains(t, logs, "slots_behind=800")
}

// TestBackfillMetricsSurfaceCatchUpProgress covers the progress requirement:
// the target and checkpoint gauges together let a dashboard show remaining
// catch-up, and the in-progress gauge returns to 0 once the sweep finishes.
func TestBackfillMetricsSurfaceCatchUpProgress(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	h := newBackfillHarness(
		t,
		0,
		func() (uint64, error) { return 250, nil },
		reg,
		testBlock(1, 100, 0xA1),
		testBlock(2, 250, 0xA2),
	)

	require.NoError(t, h.idx.Backfill())

	assert.Equal(
		t,
		float64(250),
		testutil.ToFloat64(h.idx.metrics.backfillTargetSlot),
	)
	assert.Equal(
		t,
		float64(250),
		testutil.ToFloat64(h.idx.metrics.checkpointSlot),
	)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(h.idx.metrics.backfillInProgress),
	)
	assert.Equal(t, float64(2), testutil.ToFloat64(h.idx.metrics.blocksIndexed))
}

// TestBackfillInProgressGaugeSetDuringScan proves the gauge is observable
// while the sweep runs, not just reset afterwards.
func TestBackfillInProgressGaugeSetDuringScan(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	store := setupTestStore(t)
	var during float64
	idx, err := New(Config{
		Metadata:      store,
		Logger:        slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)),
		PromRegistry:  reg,
		LedgerTipSlot: func() (uint64, error) { return 250, nil },
		SlotToEpoch:   func(slot uint64) (uint64, error) { return slot / 100, nil },
		blockDecoder: func(models.Block) ([]lcommon.Transaction, error) {
			return nil, nil
		},
	})
	require.NoError(t, err)
	idx.config.BlockIterator = func(_, _ uint64, fn func(models.Block) error) error {
		during = testutil.ToFloat64(idx.metrics.backfillInProgress)
		return fn(testBlock(1, 100, 0xA1))
	}

	require.NoError(t, idx.Backfill())

	assert.Equal(t, float64(1), during)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(idx.metrics.backfillInProgress),
	)
}

// TestCheckpointGaugeTracksLiveProgress proves the checkpoint gauge keeps
// moving after backfill hands over to the live event path, so the catch-up
// view stays accurate for a node that started behind and caught up live.
func TestCheckpointGaugeTracksLiveProgress(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	h := newBackfillHarness(
		t,
		0,
		func() (uint64, error) { return 100, nil },
		reg,
		testBlock(1, 100, 0xA1),
	)
	require.NoError(t, h.idx.Backfill())

	require.NoError(t, h.idx.updateCheckpoint(512))

	assert.Equal(
		t,
		float64(512),
		testutil.ToFloat64(h.idx.metrics.checkpointSlot),
	)
}

// TestCheckpointGaugePublishedAtConstruction proves the resume point is
// visible before any sweep runs, so an indexer wired without a BlockIterator
// (or one that starts caught up) does not report slot 0.
func TestCheckpointGaugePublishedAtConstruction(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	h := newBackfillHarness(
		t,
		777,
		func() (uint64, error) { return 900, nil },
		reg,
	)

	assert.Equal(
		t,
		float64(777),
		testutil.ToFloat64(h.idx.metrics.checkpointSlot),
	)
}

// TestBackfillPublishesTargetWhenNoGap covers the skip path, which is reached
// when the checkpoint sits above the applied ledger tip -- a chain rollback
// lowers the ledger cursor but not the indexer checkpoint. No blocks are
// swept, but the position gauges must still describe where the indexer is.
func TestBackfillPublishesTargetWhenNoGap(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	h := newBackfillHarness(
		t,
		500,
		func() (uint64, error) { return 250, nil },
		reg,
		testBlock(1, 100, 0xA1),
	)

	require.NoError(t, h.idx.Backfill())

	assert.Equal(t, 0, h.calls)
	assert.Equal(
		t,
		float64(250),
		testutil.ToFloat64(h.idx.metrics.backfillTargetSlot),
	)
	assert.Equal(
		t,
		float64(500),
		testutil.ToFloat64(h.idx.metrics.checkpointSlot),
	)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(h.idx.metrics.backfillInProgress),
	)
}

// TestBackfillUnboundedLeavesTargetUnset proves the nil-resolver path does not
// publish MaxUint64-1 as a catch-up target, which would render the gauge
// meaningless on any dashboard that subtracts the checkpoint from it.
func TestBackfillUnboundedLeavesTargetUnset(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	h := newBackfillHarness(t, 0, nil, reg, testBlock(1, 100, 0xA1))

	require.NoError(t, h.idx.Backfill())

	assert.Equal(t, uint64(math.MaxUint64), h.endSlot)
	assert.Equal(
		t,
		float64(0),
		testutil.ToFloat64(h.idx.metrics.backfillTargetSlot),
	)
}
