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

package leader

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// The epochs in these tests are electionSlotsPerEpoch (10) slots long, so
// epoch 10 is slots 100-109 and epoch 11 is slots 110-119.
const (
	observeEpochA = uint64(10)
	observeEpochB = uint64(11)
)

// observeFixture is an election whose schedules come from a persisted store,
// so no VRF work runs. Tests drive it by calling ObserveSlot with the slots a
// slot clock would report.
type observeFixture struct {
	election *Election
	reg      *prometheus.Registry
	epochs   *mockEpochProvider
	store    *mockScheduleStore
	poolId   lcommon.PoolKeyHash
	bus      *event.EventBus
}

func newObserveFixture(t *testing.T) *observeFixture {
	t.Helper()
	f := &observeFixture{
		reg:    prometheus.NewRegistry(),
		epochs: newMockEpochProvider(),
		store:  newMockScheduleStore(),
		bus:    event.NewEventBus(nil, nil),
	}
	t.Cleanup(f.bus.Stop)
	copy(f.poolId[:], []byte("testpool1234567890123"))
	stake := newMockStakeProvider()
	stake.totalStake = 10000
	stake.poolStakes[string(f.poolId[:])] = 1000
	f.election = NewElection(
		f.poolId,
		vrfSeedSource(electionTestVRFSeed),
		stake,
		f.epochs,
		f.bus,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	f.election.SetScheduleStore(f.store)
	f.election.SetPromRegistry(f.reg)
	// The mock provider has a nonce for its current epoch, which would let a
	// background compute run VRF work before a test persists its schedule.
	f.epochs.SetEpochNonceForEpoch(observeEpochA, nil)
	return f
}

// persist stores a schedule for epoch with the given leader slots, and the
// epoch nonce it must be validated against.
func (f *observeFixture) persist(
	t *testing.T,
	epoch uint64,
	slots ...uint64,
) {
	t.Helper()
	nonce := makeElectionNonce(byte(epoch))
	f.epochs.SetEpochNonceForEpoch(epoch, nonce)
	schedule := NewSchedule(epoch, f.poolId, 1000, 10000, nonce)
	for _, slot := range slots {
		schedule.AddLeaderSlot(slot)
	}
	require.NoError(t, f.store.SaveSchedule(schedule))
}

// run starts the election and registers its stop.
func (f *observeFixture) run(t *testing.T) {
	t.Helper()
	require.NoError(t, f.election.Start(context.Background()))
	t.Cleanup(func() { _ = f.election.Stop() })
}

// load loads the persisted schedule of epoch and returns once it is cached.
func (f *observeFixture) load(t *testing.T, epoch uint64) {
	t.Helper()
	require.NoError(
		t,
		f.election.RefreshScheduleForEpoch(context.Background(), epoch),
	)
	require.NotNil(t, f.election.ScheduleForEpoch(epoch))
}

// runWith persists nothing more: it starts the election and loads each epoch.
func (f *observeFixture) runWith(t *testing.T, epochs ...uint64) {
	t.Helper()
	f.run(t)
	for _, epoch := range epochs {
		f.load(t, epoch)
	}
}

func value(c prometheus.Collector) float64 {
	return promtestutil.ToFloat64(c)
}

func (f *observeFixture) elapsed() float64 {
	return value(f.election.metrics.scheduledSlotsElapsed)
}

// A leader slot counts when the slot clock reaches it, whether or not anything
// forged a block: this test never calls ShouldProduceBlock, which a forger
// skipping the slot for sync or a stale tip does not do either.
func TestObserveSlotCountsEachScheduledSlotOnce(t *testing.T) {
	t.Parallel()
	f := newObserveFixture(t)
	f.persist(t, observeEpochA, 101, 104, 107)
	f.runWith(t, observeEpochA)

	f.election.ObserveSlot(100)
	require.Zero(t, f.elapsed())
	f.election.ObserveSlot(101)
	require.Equal(t, float64(1), f.elapsed(), "slot 101 is scheduled")
	f.election.ObserveSlot(101)
	require.Equal(t, float64(1), f.elapsed(), "the same slot twice counts once")
	f.election.ObserveSlot(103)
	require.Equal(t, float64(1), f.elapsed(), "103 is not scheduled")
	f.election.ObserveSlot(104)
	require.Equal(t, float64(2), f.elapsed())
	f.election.ObserveSlot(102)
	require.Equal(
		t,
		float64(2),
		f.elapsed(),
		"a clock reading behind the last one counts nothing",
	)
	f.election.ObserveSlot(104)
	require.Equal(t, float64(2), f.elapsed())
	f.election.ObserveSlot(107)
	require.Equal(t, float64(3), f.elapsed())
	f.election.ObserveSlot(109)
	require.Equal(t, float64(3), f.elapsed())
}

// A forger that was busy, or a slot clock that paused, reports a slot several
// ahead of the last one; every scheduled slot in between still counts.
func TestObserveSlotCountsSlotsSkippedBetweenReadings(t *testing.T) {
	t.Parallel()
	f := newObserveFixture(t)
	f.persist(t, observeEpochA, 101, 104, 107)
	f.runWith(t, observeEpochA)

	f.election.ObserveSlot(100)
	f.election.ObserveSlot(108)
	require.Equal(t, float64(3), f.elapsed())
	f.election.ObserveSlot(108)
	require.Equal(t, float64(3), f.elapsed())
}

// Slots before the first reading are not counted: a node that starts mid-epoch
// reports its own run, not the epoch.
func TestObserveSlotDoesNotBackfillBeforeFirstReading(t *testing.T) {
	t.Parallel()
	f := newObserveFixture(t)
	f.persist(t, observeEpochA, 101, 104, 107)
	f.runWith(t, observeEpochA)

	f.election.ObserveSlot(105)
	require.Zero(t, f.elapsed(), "101 and 104 passed before the first reading")
	f.election.ObserveSlot(107)
	require.Equal(t, float64(1), f.elapsed())
}

// The first reading's own slot counts: the forger reads at a slot boundary.
func TestObserveSlotCountsTheFirstReadingSlot(t *testing.T) {
	t.Parallel()
	f := newObserveFixture(t)
	f.persist(t, observeEpochA, 101)
	f.runWith(t, observeEpochA)

	f.election.ObserveSlot(101)
	require.Equal(t, float64(1), f.elapsed())
}

// A leader slot that passes before the node has its epoch's schedule (a node
// still syncing, or one whose schedule is still being computed) is counted
// once the schedule arrives, and counted only once.
func TestObserveSlotCountsSlotsPassedBeforeTheScheduleArrived(t *testing.T) {
	t.Parallel()
	f := newObserveFixture(t)
	// No schedule is stored yet and the epoch has no nonce, so none can be
	// produced.
	f.run(t)

	f.election.ObserveSlot(100)
	f.election.ObserveSlot(105)
	require.Zero(t, f.elapsed())

	f.persist(t, observeEpochA, 101, 104, 107)
	f.load(t, observeEpochA)
	f.election.ObserveSlot(106)
	require.Equal(t, float64(2), f.elapsed(), "101 and 104 passed unscheduled")
	f.election.ObserveSlot(106)
	require.Equal(t, float64(2), f.elapsed())
	f.election.ObserveSlot(107)
	require.Equal(t, float64(3), f.elapsed())
}

// A restart of the election inside one process keeps what it has counted.
func TestObserveSlotDoesNotRecountAfterElectionRestart(t *testing.T) {
	t.Parallel()
	f := newObserveFixture(t)
	f.persist(t, observeEpochA, 101, 104)
	f.runWith(t, observeEpochA)

	f.election.ObserveSlot(100)
	f.election.ObserveSlot(104)
	require.Equal(t, float64(2), f.elapsed())

	require.NoError(t, f.election.Stop())
	f.runWith(t, observeEpochA)
	f.election.ObserveSlot(104)
	f.election.ObserveSlot(105)
	require.Equal(t, float64(2), f.elapsed())
}

// An election with no registry observes slots without effect.
func TestObserveSlotWithoutRegistryIsANoOp(t *testing.T) {
	t.Parallel()
	f := newObserveFixture(t)
	election := NewElection(
		f.poolId,
		vrfSeedSource(electionTestVRFSeed),
		newMockStakeProvider(),
		f.epochs,
		f.bus,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	require.NotPanics(t, func() { election.ObserveSlot(105) })
}

func TestScheduleGaugesAreSetByAPersistedLoad(t *testing.T) {
	t.Parallel()
	f := newObserveFixture(t)
	f.persist(t, observeEpochA, 101, 104, 107)
	f.runWith(t, observeEpochA)

	require.Equal(
		t,
		float64(observeEpochA),
		value(f.election.metrics.scheduleEpoch),
	)
	require.Equal(
		t,
		float64(3),
		value(f.election.metrics.scheduleLeaderSlots),
	)
	// The last_epoch gauges keep their meaning: they describe a compute, and
	// a load is not one.
	require.Zero(t, value(f.election.metrics.lastEpochSlotsWon))
}

// A schedule for an epoch that is not the current one, such as the next
// epoch's precomputed schedule, does not move the current-epoch gauges.
func TestScheduleGaugesIgnoreANonCurrentEpoch(t *testing.T) {
	t.Parallel()
	f := newObserveFixture(t)
	f.persist(t, observeEpochA, 101, 104, 107)
	f.persist(t, observeEpochB, 112)
	f.runWith(t, observeEpochA, observeEpochB)

	require.Equal(
		t,
		float64(observeEpochA),
		value(f.election.metrics.scheduleEpoch),
	)
	require.Equal(
		t,
		float64(3),
		value(f.election.metrics.scheduleLeaderSlots),
	)
}

func TestScheduleGaugesFollowTheSlotClockAcrossAnEpochBoundary(t *testing.T) {
	t.Parallel()
	f := newObserveFixture(t)
	f.persist(t, observeEpochA, 101, 104, 107)
	f.persist(t, observeEpochB, 112, 115)
	f.runWith(t, observeEpochA, observeEpochB)

	f.election.ObserveSlot(109)
	require.Equal(
		t,
		float64(observeEpochA),
		value(f.election.metrics.scheduleEpoch),
	)
	f.election.ObserveSlot(110)
	require.Equal(
		t,
		float64(observeEpochB),
		value(f.election.metrics.scheduleEpoch),
	)
	require.Equal(
		t,
		float64(2),
		value(f.election.metrics.scheduleLeaderSlots),
	)
}

// The next epoch's schedule was precomputed, and the refresh the transition
// triggers stores nothing (the epoch has no nonce any more), so the gauges can
// only move because the transition itself publishes them.
func TestScheduleGaugesFollowAnEpochTransitionEvent(t *testing.T) {
	t.Parallel()
	f := newObserveFixture(t)
	f.persist(t, observeEpochA, 101, 104, 107)
	f.persist(t, observeEpochB, 112, 115)
	f.run(t)
	// Start queues the current epoch's schedule on a background worker. Wait
	// for its store and gauge publication to finish, so nothing but the
	// transition can store or publish from here on.
	require.Eventually(t, func() bool {
		return value(f.election.metrics.scheduleEpoch) ==
			float64(observeEpochA) &&
			value(f.election.metrics.scheduleLeaderSlots) == 3
	}, 10*time.Second, 10*time.Millisecond)
	f.load(t, observeEpochB)
	// With no nonce the refresh can neither load nor compute; the cached
	// schedule stays.
	f.epochs.SetEpochNonceForEpoch(observeEpochB, nil)

	f.epochs.currentEpoch.Store(observeEpochB)
	f.bus.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(
			event.EpochTransitionEventType,
			event.EpochTransitionEvent{
				PreviousEpoch: observeEpochA,
				NewEpoch:      observeEpochB,
				BoundarySlot:  110,
			},
		),
	)
	require.Eventually(t, func() bool {
		return value(f.election.metrics.scheduleEpoch) ==
			float64(observeEpochB) &&
			value(f.election.metrics.scheduleLeaderSlots) == 2
	}, 10*time.Second, 10*time.Millisecond)
}

func TestNextLeaderSlotGaugeTracksTheSlotClock(t *testing.T) {
	t.Parallel()
	f := newObserveFixture(t)
	f.persist(t, observeEpochA, 101, 104)
	f.persist(t, observeEpochB, 112)
	f.runWith(t, observeEpochA, observeEpochB)
	gauge := f.election.metrics.nextLeaderSlot

	for _, tc := range []struct{ observed, next uint64 }{
		{100, 101},
		{101, 104},
		{103, 104},
		{104, 112},
		{111, 112},
		{112, 0},
	} {
		f.election.ObserveSlot(tc.observed)
		require.Equal(
			t,
			float64(tc.next),
			value(gauge),
			"next leader slot after observing %d",
			tc.observed,
		)
	}
}

// A schedule arriving after the last reading refreshes the gauge without
// waiting for the next slot.
func TestNextLeaderSlotGaugeIsSetWhenAScheduleIsLoaded(t *testing.T) {
	t.Parallel()
	f := newObserveFixture(t)
	f.run(t)
	f.election.ObserveSlot(100)
	require.Zero(t, value(f.election.metrics.nextLeaderSlot))

	f.persist(t, observeEpochA, 104)
	f.load(t, observeEpochA)
	require.Equal(t, float64(104), value(f.election.metrics.nextLeaderSlot))
}

// A schedule that stays cached while the slot clock runs epochs past it (no
// newer schedule could be produced, as in a stalled sync) is not counted again
// once its bookkeeping ages out.
func TestObserveSlotDoesNotRecountAnAgedOutCachedSchedule(t *testing.T) {
	t.Parallel()
	f := newObserveFixture(t)
	f.persist(t, observeEpochA, 101, 104, 107)
	f.runWith(t, observeEpochA)

	f.election.ObserveSlot(100)
	f.election.ObserveSlot(109)
	require.Equal(t, float64(3), f.elapsed())

	// Slots 135-137 are three epochs past epoch 10, whose schedule is still
	// the only one cached.
	require.NotNil(t, f.election.ScheduleForEpoch(observeEpochA))
	for _, slot := range []uint64{135, 136, 137} {
		f.election.ObserveSlot(slot)
		require.Equal(
			t,
			float64(3),
			f.elapsed(),
			"slot %d recounted epoch 10",
			slot,
		)
	}
}

// A clock reading behind the first one does not move the counted mark below
// the floor, so a later reading never counts a slot that passed before the
// node first read the clock.
func TestObserveSlotDoesNotCountBelowTheFloorAfterABackwardsReading(
	t *testing.T,
) {
	t.Parallel()
	f := newObserveFixture(t)
	f.run(t)

	f.election.ObserveSlot(104)
	f.persist(t, observeEpochA, 103, 105)
	f.load(t, observeEpochA)
	f.election.ObserveSlot(102)
	require.Zero(t, f.elapsed())
	f.election.ObserveSlot(106)
	require.Equal(
		t,
		float64(1),
		f.elapsed(),
		"only 105 is at or after the first reading",
	)
}

// A panic out of the epoch provider inside ObserveSlot, which the forger
// recovers, does not leave the observation lock held, so the next forge cycle
// observes its slot instead of blocking.
func TestObserveSlotReleasesItsLockWhenTheEpochProviderPanics(t *testing.T) {
	t.Parallel()
	f := newObserveFixture(t)
	var panicked atomic.Bool
	f.epochs.epochForSlot = func(slot uint64) (uint64, error) {
		if panicked.CompareAndSwap(false, true) {
			panic("epoch provider fault")
		}
		return slot / electionSlotsPerEpoch, nil
	}

	require.Panics(t, func() { f.election.ObserveSlot(100) })
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.election.ObserveSlot(101)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ObserveSlot blocked after a recovered panic")
	}
}
