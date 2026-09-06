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
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// retryTestSlotClock is forgerTestSlotClock with a controllable slot-end
// instant, so tests can put the forge either comfortably inside its slot or
// past the end of it without sleeping.
type retryTestSlotClock struct {
	currentSlot       uint64
	chainTipSlot      uint64
	slotsPerKESPeriod uint64
	slotEnd           time.Time
}

func (c *retryTestSlotClock) CurrentSlot() (uint64, error) {
	return c.currentSlot, nil
}

func (c *retryTestSlotClock) SlotsPerKESPeriod() uint64 {
	return c.slotsPerKESPeriod
}

func (c *retryTestSlotClock) ChainTipSlot() uint64 { return c.chainTipSlot }

func (c *retryTestSlotClock) NextSlotTime() (time.Time, error) {
	return c.slotEnd, nil
}

func (c *retryTestSlotClock) UpstreamTipSlot() uint64 { return 0 }

func (c *retryTestSlotClock) UpstreamSyncStatus() (uint64, bool) {
	return 0, false
}

// retryTestBuilder fails its first failCount build attempts with err and
// succeeds afterwards, so a test can observe whether the forger makes a
// second attempt for the same slot.
type retryTestBuilder struct {
	block     ledger.Block
	cbor      []byte
	calls     int
	failCount int
	err       error
}

func (b *retryTestBuilder) BuildBlock(
	uint64,
	uint64,
) (ledger.Block, []byte, error) {
	b.calls++
	if b.calls <= b.failCount {
		return nil, nil, b.err
	}
	return b.block, b.cbor, nil
}

func newRetryForger(
	t *testing.T,
	clock *retryTestSlotClock,
	builder BlockBuilder,
	broadcaster *forgerTestBroadcaster,
) *BlockForger {
	t.Helper()
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    forgerTestLeader{},
		BlockBuilder:     builder,
		BlockBroadcaster: broadcaster,
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger
}

// TestForgeRetriesSelectionWhenSnapshotChangesMidSlot is the regression for
// the lost leader slot: a ledger publication landing during transaction
// selection aborts the candidate, and before this the forger simply gave up
// and slept to the next slot. With time still left in the slot the forge
// must re-run selection against the new snapshot and still produce a block.
func TestForgeRetriesSelectionWhenSnapshotChangesMidSlot(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{
		block:     block,
		cbor:      block.cbor,
		failCount: 1,
		// Wrapped exactly as DefaultBlockBuilder wraps it, so the
		// forger has to match on the sentinel rather than on a string.
		err: fmt.Errorf(
			"failed to select block transactions: %w",
			errTxValidationSnapshotChanged,
		),
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(2 * time.Second),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 2, builder.calls, "forger must re-select within the slot")
	require.Equal(t, 1, broadcaster.calls)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
		"a slot recovered by retry is not a could-not-forge",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("retried"),
		),
	)
}

// TestForgeDoesNotRetrySelectionWithoutSlotTimeLeft pins the deadline half of
// the retry: when the slot is already over there is no point re-running
// selection, so the attempt is abandoned after one try.
func TestForgeDoesNotRetrySelectionWithoutSlotTimeLeft(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{
		block:     block,
		cbor:      block.cbor,
		failCount: 1,
		err:       errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		// Slot already ended: less than the retry margin remains.
		slotEnd: time.Now(),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	err := forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.ErrorIs(t, err, errTxValidationSnapshotChanged)
	require.Equal(t, 1, builder.calls, "no slot time left means no retry")
	require.Equal(t, 0, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("lost"),
		),
	)
}

// TestForgeStopsRetryingSelectionAtAttemptCap keeps the retry bounded: a
// producer whose ledger publishes continuously must not spin on selection
// for the whole slot.
func TestForgeStopsRetryingSelectionAtAttemptCap(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{
		block:     block,
		cbor:      block.cbor,
		failCount: 1000,
		err:       errTxValidationSnapshotChanged,
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(time.Hour),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	err := forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.Equal(
		t,
		1+defaultForgeSelectionMaxRetries,
		builder.calls,
		"retries must stop at the configured cap",
	)
	require.Equal(t, 0, broadcaster.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("lost"),
		),
	)
}

// TestForgeDoesNotRetryNonSelectionBuildFailures keeps the retry narrow: a
// build that failed for any reason other than the chain moving under
// selection is not made more likely to succeed by trying again.
func TestForgeDoesNotRetryNonSelectionBuildFailures(t *testing.T) {
	block := newForgerTestBlock(10, 2)
	builder := &retryTestBuilder{
		block:     block,
		cbor:      block.cbor,
		failCount: 1,
		err:       errors.New("VRF verification key not loaded"),
	}
	broadcaster := &forgerTestBroadcaster{}
	clock := &retryTestSlotClock{
		currentSlot:       10,
		chainTipSlot:      9,
		slotsPerKESPeriod: 100,
		slotEnd:           time.Now().Add(time.Hour),
	}
	forger := newRetryForger(t, clock, builder, broadcaster)

	err := forger.checkAndForgeProduction(context.Background())
	require.Error(t, err)
	require.Equal(t, 1, builder.calls)
	require.Equal(
		t,
		float64(0),
		testutil.ToFloat64(
			forger.metrics.forgeSelectionFallback.WithLabelValues("lost"),
		),
		"a non-selection build failure is not a selection fallback",
	)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeCouldNot),
	)
}
