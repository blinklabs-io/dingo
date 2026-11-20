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
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// observingLeader is a leader checker that is also a SlotObserver. It records
// the slots reported to it and the slots leader selection ran for.
type observingLeader struct {
	mu       sync.Mutex
	observed []uint64
	selected []uint64
	panics   bool
}

func (l *observingLeader) ShouldProduceBlock(slot uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.selected = append(l.selected, slot)
	return true
}

func (l *observingLeader) NextLeaderSlot(from uint64) (uint64, bool) {
	return from, true
}

func (l *observingLeader) ObserveSlot(slot uint64) {
	l.mu.Lock()
	l.observed = append(l.observed, slot)
	l.mu.Unlock()
	if l.panics {
		panic("observer failure")
	}
}

func (l *observingLeader) snapshot() (observed, selected []uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]uint64(nil), l.observed...),
		append([]uint64(nil), l.selected...)
}

func newObservedForger(
	t *testing.T,
	clock SlotClockProvider,
	leader LeaderChecker,
) (*BlockForger, *forgerTestBuilder) {
	t.Helper()
	block := newForgerTestBlock(300, 2)
	builder := &forgerTestBuilder{block: block, cbor: block.cbor}
	forger, err := NewBlockForger(ForgerConfig{
		Mode:             ModeProduction,
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Credentials:      setupTestCredentials(t),
		LeaderChecker:    leader,
		BlockBuilder:     builder,
		BlockBroadcaster: &forgerTestBroadcaster{},
		SlotClock:        clock,
		PromRegistry:     prometheus.NewRegistry(),
	})
	require.NoError(t, err)
	return forger, builder
}

// A slot the forger refuses because the node is syncing never reaches leader
// selection, so only a report made ahead of the gates can see it.
func TestForgerReportsASlotItSkipsForSync(t *testing.T) {
	t.Parallel()
	leader := &observingLeader{}
	forger, builder := newObservedForger(t, forgerTestSlotClock{
		currentSlot:       114220801,
		chainTipSlot:      114220600,
		upstreamTipSlot:   114220800,
		slotsPerKESPeriod: 114220801,
	}, leader)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(t, builder.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeSyncSkip),
		"the slot must have been refused by the sync gate",
	)
	observed, selected := leader.snapshot()
	require.Equal(t, []uint64{114220801}, observed)
	require.Empty(t, selected, "leader selection never ran for the slot")
}

func TestForgerReportsASlotItSkipsForAStaleTip(t *testing.T) {
	t.Parallel()
	leader := &observingLeader{}
	forger, builder := newObservedForger(t, forgerTestSlotClock{
		currentSlot:        200,
		chainTipSlot:       100,
		primaryTipExplicit: true,
		primaryTipSlot:     183,
		slotsPerKESPeriod:  100,
	}, leader)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Zero(t, builder.calls)
	require.Equal(
		t,
		float64(1),
		testutil.ToFloat64(forger.metrics.forgeStaleTipSkipBlockGap),
		"the slot must have been refused by the stale-tip gate",
	)
	observed, _ := leader.snapshot()
	require.Equal(t, []uint64{200}, observed)
}

// A slot the forger forges is reported too, once.
func TestForgerReportsAForgedSlot(t *testing.T) {
	t.Parallel()
	leader := &observingLeader{}
	forger, builder := newObservedForger(t, forgerTestSlotClock{
		currentSlot:       300,
		chainTipSlot:      299,
		slotsPerKESPeriod: 100,
	}, leader)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls)
	observed, selected := leader.snapshot()
	require.Equal(t, []uint64{300}, observed)
	require.Equal(t, []uint64{300}, selected)
}

// An observer that panics must not stop the forge cycle.
func TestForgerSurvivesAPanickingSlotObserver(t *testing.T) {
	t.Parallel()
	leader := &observingLeader{panics: true}
	forger, builder := newObservedForger(t, forgerTestSlotClock{
		currentSlot:       300,
		chainTipSlot:      299,
		slotsPerKESPeriod: 100,
	}, leader)

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))

	require.Equal(t, 1, builder.calls, "the slot is still forged")
}

// A leader checker that is not an observer is left alone.
func TestForgerWithoutASlotObserverStillForges(t *testing.T) {
	t.Parallel()
	forger, builder := newObservedForger(t, forgerTestSlotClock{
		currentSlot:       300,
		chainTipSlot:      299,
		slotsPerKESPeriod: 100,
	}, forgerTestLeader{})

	require.NoError(t, forger.checkAndForgeProduction(context.Background()))
	require.Equal(t, 1, builder.calls)
}

func TestForgerCountsARemoteSignerNotReadyAheadOfALeaderSlot(t *testing.T) {
	t.Parallel()
	signer := &preparingRemoteKESSigner{prepareErr: errors.New("unreachable")}
	f := &BlockForger{
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		creds:         newRemotePrepareCreds(t, signer),
		leaderChecker: singleLeaderSlot(103),
		slotDuration:  time.Second,
		metrics:       initForgingMetrics(prometheus.NewRegistry()),
	}

	f.prepareRemoteSignerForUpcomingLeaderSlot(100)
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(f.metrics.remoteKESSignerNotReady) == 1
	}, 2*time.Second, 5*time.Millisecond)

	// The same leader slot is prepared, and so counted, once.
	f.prepareRemoteSignerForUpcomingLeaderSlot(101)
	f.prepareRemoteSignerForUpcomingLeaderSlot(102)
	require.Never(t, func() bool {
		return testutil.ToFloat64(f.metrics.remoteKESSignerNotReady) != 1
	}, 50*time.Millisecond, 5*time.Millisecond)
}

// A signer that is ready counts nothing.
func TestForgerDoesNotCountAReadyRemoteSigner(t *testing.T) {
	t.Parallel()
	signer := &preparingRemoteKESSigner{}
	f := &BlockForger{
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		creds:         newRemotePrepareCreds(t, signer),
		leaderChecker: singleLeaderSlot(103),
		slotDuration:  time.Second,
		metrics:       initForgingMetrics(prometheus.NewRegistry()),
	}

	f.prepareRemoteSignerForUpcomingLeaderSlot(100)
	require.Eventually(t, func() bool { return signer.prepares.Load() == 1 },
		2*time.Second, 5*time.Millisecond)
	require.Never(t, func() bool {
		return testutil.ToFloat64(f.metrics.remoteKESSignerNotReady) != 0
	}, 50*time.Millisecond, 5*time.Millisecond)
}

func TestSlotBattleOutcomeIsAttributedToTheForgerThatForgedTheBlock(
	t *testing.T,
) {
	t.Parallel()
	newForger := func() *BlockForger {
		f, err := NewBlockForger(ForgerConfig{
			Mode:         ModeDev,
			Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
			PromRegistry: prometheus.NewRegistry(),
		})
		require.NoError(t, err)
		return f
	}
	forger, other := newForger(), newForger()
	forger.SlotTracker().RecordForgedBlock(10, []byte{0x0a})

	require.Nil(t, other.SlotBattleOutcome(10, []byte{0x0a}),
		"a forger that did not forge the block does not own its battle")
	require.Nil(t, forger.SlotBattleOutcome(10, []byte{0xff}),
		"a different block in the slot is not this forger's battle")
	require.Nil(t, forger.SlotBattleOutcome(11, []byte{0x0a}))

	record := forger.SlotBattleOutcome(10, []byte{0x0a})
	require.NotNil(t, record)
	// The tracker may evict the block before the battle settles; the
	// resolved recorder must not depend on it.
	forger.SlotTracker().RecordForgedBlock(11, []byte{0x0b})
	record(true)
	record(false)
	record(false)

	won := forger.metrics.slotBattleOutcome.WithLabelValues(slotBattleWon)
	lost := forger.metrics.slotBattleOutcome.WithLabelValues(slotBattleLost)
	require.Equal(t, float64(1), testutil.ToFloat64(won))
	require.Equal(t, float64(2), testutil.ToFloat64(lost))
	require.Zero(t, testutil.ToFloat64(other.metrics.slotBattleOutcome.
		WithLabelValues(slotBattleWon)))
	require.Zero(
		t,
		testutil.ToFloat64(forger.metrics.slotBattlesTotal),
		"the battle counter is raised at detection, not at settlement",
	)
}
