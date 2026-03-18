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
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/event"
	ledgerpkg "github.com/blinklabs-io/dingo/ledger"
)

// electionTestNonce is a 32-byte epoch nonce for election tests.
var electionTestNonce = func() []byte {
	nonce := make([]byte, 32)
	for i := range nonce {
		nonce[i] = byte(i + 42)
	}
	return nonce
}()

// electionTestVRFSeed is a 32-byte VRF seed for election tests.
var electionTestVRFSeed = []byte("election_vrf_seed_32_bytes_ok!!!")

// electionSlotsPerEpoch is a small epoch size to keep VRF computation fast
// in tests. VRF Prove is computationally expensive (~0.2s per call), so we
// use a small number of slots to keep test execution reasonable.
const electionSlotsPerEpoch = 10

// mockStakeProvider implements StakeDistributionProvider for testing
type mockStakeProvider struct {
	poolStakes map[string]uint64
	totalStake uint64
	err        error
}

func newMockStakeProvider() *mockStakeProvider {
	return &mockStakeProvider{
		poolStakes: make(map[string]uint64),
	}
}

func (m *mockStakeProvider) GetPoolStake(
	epoch uint64,
	poolKeyHash []byte,
) (uint64, error) {
	if m.err != nil {
		return 0, m.err
	}
	return m.poolStakes[string(poolKeyHash)], nil
}

func (m *mockStakeProvider) GetTotalActiveStake(epoch uint64) (uint64, error) {
	if m.err != nil {
		return 0, m.err
	}
	return m.totalStake, nil
}

// mockEpochProvider implements EpochInfoProvider for testing
type mockEpochProvider struct {
	currentEpoch    atomic.Uint64
	epochNonce      atomic.Value
	nonceMu         sync.RWMutex
	epochNonces     map[uint64][]byte
	slotsPerEpoch   uint64
	activeSlotCoeff float64
	nextEpochReady  atomic.Uint64
}

func newMockEpochProvider() *mockEpochProvider {
	m := &mockEpochProvider{
		slotsPerEpoch:   electionSlotsPerEpoch,
		activeSlotCoeff: 0.05,
		epochNonces:     make(map[uint64][]byte),
	}
	m.currentEpoch.Store(10)
	m.SetEpochNonce(electionTestNonce)
	return m
}

func (m *mockEpochProvider) CurrentEpoch() uint64 {
	return m.currentEpoch.Load()
}

func (m *mockEpochProvider) EpochNonce(epoch uint64) []byte {
	m.nonceMu.RLock()
	if nonce, ok := m.epochNonces[epoch]; ok {
		m.nonceMu.RUnlock()
		return cloneElectionNonce(nonce)
	}
	m.nonceMu.RUnlock()

	nonce, ok := m.epochNonce.Load().([]byte)
	if !ok || len(nonce) == 0 {
		return nil
	}
	return cloneElectionNonce(nonce)
}

func (m *mockEpochProvider) NextEpochNonceReadyEpoch() (uint64, bool) {
	nextEpoch := m.nextEpochReady.Load()
	if nextEpoch == 0 {
		return 0, false
	}
	return nextEpoch, true
}

func (m *mockEpochProvider) SlotsPerEpoch() uint64 {
	return m.slotsPerEpoch
}

func (m *mockEpochProvider) ActiveSlotCoeff() float64 {
	return m.activeSlotCoeff
}

func (m *mockEpochProvider) SetEpochNonce(nonce []byte) {
	m.epochNonce.Store(cloneElectionNonce(nonce))
}

func (m *mockEpochProvider) SetEpochNonceForEpoch(
	epoch uint64,
	nonce []byte,
) {
	m.nonceMu.Lock()
	defer m.nonceMu.Unlock()
	if len(nonce) == 0 {
		delete(m.epochNonces, epoch)
		return
	}
	m.epochNonces[epoch] = cloneElectionNonce(nonce)
}

type mockScheduleStore struct {
	mu        sync.RWMutex
	schedules map[string]*Schedule
	loadErr   error
	saveErr   error
}

func newMockScheduleStore() *mockScheduleStore {
	return &mockScheduleStore{
		schedules: make(map[string]*Schedule),
	}
}

func (m *mockScheduleStore) LoadSchedule(
	epoch uint64,
	poolId lcommon.PoolKeyHash,
) (*Schedule, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.loadErr != nil {
		return nil, m.loadErr
	}
	return m.schedules[m.key(epoch, poolId)], nil
}

func (m *mockScheduleStore) SaveSchedule(schedule *Schedule) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.saveErr != nil {
		return m.saveErr
	}
	m.schedules[m.key(schedule.Epoch, schedule.PoolId)] = schedule
	return nil
}

func (m *mockScheduleStore) key(
	epoch uint64,
	poolId lcommon.PoolKeyHash,
) string {
	return fmt.Sprintf("%d:%x", epoch, poolId[:])
}

func makeElectionNonce(fill byte) []byte {
	return bytes.Repeat([]byte{fill}, 32)
}

func cloneElectionNonce(nonce []byte) []byte {
	if len(nonce) == 0 {
		return nil
	}
	return append([]byte(nil), nonce...)
}

// waitForSchedule polls until CurrentSchedule returns non-nil, or fails
// the test after timeout. VRF computation is expensive (~0.2s per slot),
// so we use a generous timeout.
func waitForSchedule(
	t *testing.T,
	election *Election,
	timeout time.Duration,
) *Schedule {
	t.Helper()
	var schedule *Schedule
	require.Eventually(t, func() bool {
		schedule = election.CurrentSchedule()
		return schedule != nil
	}, timeout, 50*time.Millisecond, "schedule should be computed")
	return schedule
}

func TestNewElection(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("testpool1234567890123"))
	vrfKey := electionTestVRFSeed

	stakeProvider := newMockStakeProvider()
	epochProvider := newMockEpochProvider()
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		vrfKey,
		stakeProvider,
		epochProvider,
		eventBus,
		nil, // nil logger uses default
	)

	require.NotNil(t, election)
	assert.Equal(t, poolId, election.poolId)
	assert.Equal(t, vrfKey, election.poolVrfSkey)
	assert.NotNil(t, election.logger)
}

func TestElectionStartStop(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	stakeProvider.poolStakes[string(poolId[:])] = 1000

	epochProvider := newMockEpochProvider()
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		electionTestVRFSeed,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	// Start should succeed
	err := election.Start(context.Background())
	require.NoError(t, err)

	// Start again should be idempotent
	err = election.Start(context.Background())
	require.NoError(t, err)

	// Stop should succeed
	err = election.Stop()
	require.NoError(t, err)

	// Stop again should be idempotent
	err = election.Stop()
	require.NoError(t, err)
}

func TestElectionScheduleEarlyEpochs(t *testing.T) {
	// Epochs 0 and 1 use the genesis snapshot (epoch 0) for leader
	// election, matching the Cardano spec.
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	stakeProvider.poolStakes[string(poolId[:])] = 1000

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(1)

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		electionTestVRFSeed,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	// Schedule is computed asynchronously; wait for it.
	schedule := waitForSchedule(t, election, 30*time.Second)
	assert.Equal(t, uint64(1), schedule.Epoch)
}

func TestElectionLoadsPersistedSchedule(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("testpool1234567890123"))

	store := newMockScheduleStore()
	persisted := NewSchedule(10, poolId, 1000, 10000, electionTestNonce)
	persisted.AddLeaderSlot(101)
	persisted.AddLeaderSlot(104)
	require.NoError(t, store.SaveSchedule(persisted))

	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	stakeProvider.poolStakes[string(poolId[:])] = 1000
	epochProvider := newMockEpochProvider()
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		electionTestVRFSeed,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)
	election.SetScheduleStore(store)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	schedule := waitForSchedule(t, election, 5*time.Second)
	assert.Equal(
		t,
		persisted.LeaderSlotsSnapshot(),
		schedule.LeaderSlotsSnapshot(),
	)
}

func TestElectionIgnoresStalePersistedSchedule(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("testpool1234567890123"))

	store := newMockScheduleStore()
	persisted := NewSchedule(10, poolId, 1_000_000, 1_000_000, makeElectionNonce(0x44))
	persisted.AddLeaderSlot(999)
	require.NoError(t, store.SaveSchedule(persisted))

	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	epochProvider := newMockEpochProvider()
	epochProvider.activeSlotCoeff = 1.0
	epochProvider.SetEpochNonce(makeElectionNonce(0x55))

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		electionTestVRFSeed,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)
	election.SetScheduleStore(store)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	schedule := waitForSchedule(t, election, 30*time.Second)
	require.NotNil(t, schedule)
	assert.NotEqual(t, []uint64{999}, schedule.LeaderSlotsSnapshot())
	assert.True(t, bytes.Equal(makeElectionNonce(0x55), schedule.EpochNonce))

	var persistedAfterLoad *Schedule
	require.Eventually(t, func() bool {
		var err error
		persistedAfterLoad, err = store.LoadSchedule(10, poolId)
		require.NoError(t, err)
		return persistedAfterLoad != nil &&
			bytes.Equal(makeElectionNonce(0x55), persistedAfterLoad.EpochNonce)
	}, 2*time.Second, 50*time.Millisecond)
}

func TestElectionPersistsComputedSchedule(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	stakeProvider.poolStakes[string(poolId[:])] = 1000

	store := newMockScheduleStore()
	epochProvider := newMockEpochProvider()
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		electionTestVRFSeed,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)
	election.SetScheduleStore(store)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	schedule := waitForSchedule(t, election, 30*time.Second)
	require.NotNil(t, schedule)

	var persisted *Schedule
	require.Eventually(t, func() bool {
		var err error
		persisted, err = store.LoadSchedule(schedule.Epoch, poolId)
		require.NoError(t, err)
		return persisted != nil
	}, 2*time.Second, 50*time.Millisecond)
	assert.Equal(
		t,
		schedule.LeaderSlotsSnapshot(),
		persisted.LeaderSlotsSnapshot(),
	)
}

func TestElectionPrecomputesNextEpochAtStartupWhenNonceReady(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	store := newMockScheduleStore()
	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(10)
	epochProvider.nextEpochReady.Store(11)

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		electionTestVRFSeed,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)
	election.SetScheduleStore(store)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	waitForSchedule(t, election, 30*time.Second)

	require.Eventually(t, func() bool {
		return election.ScheduleForEpoch(11) != nil
	}, 30*time.Second, 100*time.Millisecond,
		"next epoch schedule should be precomputed at startup")

	require.Eventually(t, func() bool {
		schedule, err := store.LoadSchedule(11, poolId)
		require.NoError(t, err)
		return schedule != nil
	}, 2*time.Second, 50*time.Millisecond,
		"next epoch schedule should be persisted at startup")
}

func TestElectionZeroPoolStake(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	// No pool stake set (zero)

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(10)

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		electionTestVRFSeed,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	// With zero pool stake, computeSchedule returns nil (no VRF needed).
	// Verify the schedule stays nil after the background goroutine has
	// time to process the request.
	assert.Never(t, func() bool {
		return election.CurrentSchedule() != nil
	}, 500*time.Millisecond, 50*time.Millisecond,
		"schedule should remain nil with zero pool stake")
}

func TestElectionShouldProduceBlock(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000 // 100% stake

	// Use high active slot coefficient (90%) with small epoch to ensure
	// we reliably get leader slots despite the small sample size.
	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(10)
	epochProvider.activeSlotCoeff = 0.9

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		electionTestVRFSeed,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	// Schedule is computed asynchronously; wait for it.
	schedule := waitForSchedule(t, election, 30*time.Second)
	assert.Greater(t, schedule.SlotCount(), 0,
		"pool with 100%% stake and f=0.9 should have at least one leader slot")

	// Verify ShouldProduceBlock returns true for an actual leader slot
	if schedule.SlotCount() > 0 {
		leaderSlot := schedule.LeaderSlots[0]
		assert.True(t, election.ShouldProduceBlock(leaderSlot),
			"ShouldProduceBlock should return true for a known leader slot")
	}
}

func TestElectionNextLeaderSlot(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(10)
	epochProvider.activeSlotCoeff = 0.9 // High f for reliable election

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		electionTestVRFSeed,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	// No schedule - should return 0, false
	slot, found := election.NextLeaderSlot(0)
	assert.Equal(t, uint64(0), slot)
	assert.False(t, found)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	// Schedule is computed asynchronously; wait for it.
	schedule := waitForSchedule(t, election, 30*time.Second)
	require.Greater(t, schedule.SlotCount(), 0,
		"should have leader slots with 100%% stake and f=0.9")

	epochStart := uint64(10) * electionSlotsPerEpoch
	slot, found = election.NextLeaderSlot(epochStart)
	assert.True(t, found, "should find a leader slot in the epoch")
	assert.GreaterOrEqual(t, slot, epochStart,
		"leader slot should be at or after epoch start")
}

func TestElectionEpochTransition(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(10)

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		electionTestVRFSeed,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	// Initial schedule is computed asynchronously; wait for it.
	schedule := waitForSchedule(t, election, 30*time.Second)
	assert.Equal(t, uint64(10), schedule.Epoch)

	// Simulate epoch transition by updating provider and sending event
	epochProvider.currentEpoch.Store(11)

	// Publish epoch transition event
	eventBus.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(
			event.EpochTransitionEventType,
			event.EpochTransitionEvent{
				PreviousEpoch: 10,
				NewEpoch:      11,
				BoundarySlot:  110,
				EpochNonce:    electionTestNonce,
			},
		),
	)

	// Poll for event to be processed with generous timeout.
	// VRF computation is expensive (~0.2s per slot), so with
	// electionSlotsPerEpoch slots the recalculation takes a few seconds.
	vrfStart := time.Now()
	require.Eventually(t, func() bool {
		schedule := election.CurrentSchedule()
		ready := schedule != nil && schedule.Epoch == 11
		if ready {
			t.Logf(
				"VRF schedule recalculation took %s",
				time.Since(vrfStart),
			)
		}
		return ready
	}, 30*time.Second, 100*time.Millisecond, "schedule should update to epoch 11")

	// Schedule should be updated to new epoch
	schedule = election.CurrentSchedule()
	require.NotNil(t, schedule)
	assert.Equal(t, uint64(11), schedule.Epoch)
}

func TestElectionPrecomputesNextEpochOnNonceReady(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(10)

	store := newMockScheduleStore()
	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		electionTestVRFSeed,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)
	election.SetScheduleStore(store)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	waitForSchedule(t, election, 30*time.Second)
	assert.Nil(t, election.ScheduleForEpoch(11))

	eventBus.Publish(
		event.EpochNonceReadyEventType,
		event.NewEvent(
			event.EpochNonceReadyEventType,
			event.EpochNonceReadyEvent{
				CurrentEpoch: 10,
				ReadyEpoch:   11,
				CutoffSlot:   95,
			},
		),
	)

	require.Eventually(t, func() bool {
		return election.ScheduleForEpoch(11) != nil
	}, 30*time.Second, 100*time.Millisecond,
		"next epoch schedule should be precomputed after nonce-ready event")

	require.Eventually(t, func() bool {
		schedule, err := store.LoadSchedule(11, poolId)
		require.NoError(t, err)
		return schedule != nil
	}, 2*time.Second, 50*time.Millisecond,
		"next epoch schedule should be persisted")
}

func TestElectionRollbackKeepsCurrentSchedule(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	epochProvider := newMockEpochProvider()
	epochProvider.activeSlotCoeff = 1.0

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		electionTestVRFSeed,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	schedule := waitForSchedule(t, election, 30*time.Second)
	leaderSlots := schedule.LeaderSlotsSnapshot()
	if len(leaderSlots) == 0 {
		t.Fatalf("current schedule should contain at least one leader slot")
	}
	leaderSlot := leaderSlots[0]

	eventBus.Publish(
		ledgerpkg.PoolStateRestoredEventType,
		event.NewEvent(
			ledgerpkg.PoolStateRestoredEventType,
			ledgerpkg.PoolStateRestoredEvent{Slot: 95},
		),
	)

	require.Eventually(t, func() bool {
		current := election.ScheduleForEpoch(10)
		if current == nil {
			return false
		}
		currentSlots := current.LeaderSlotsSnapshot()
		return currentSlots != nil &&
			len(currentSlots) > 0 &&
			bytes.Equal(schedule.EpochNonce, current.EpochNonce) &&
			currentSlots[0] == leaderSlot &&
			election.ShouldProduceBlock(leaderSlot)
	}, time.Second, 20*time.Millisecond,
		"rollback should not invalidate a stable current-epoch schedule")
}

func TestElectionRollbackKeepsStableNextSchedule(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	epochProvider := newMockEpochProvider()
	epochProvider.activeSlotCoeff = 1.0
	epochProvider.nextEpochReady.Store(11)
	epochProvider.SetEpochNonceForEpoch(11, makeElectionNonce(0x31))

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		electionTestVRFSeed,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	waitForSchedule(t, election, 30*time.Second)
	var nextBefore *Schedule
	require.Eventually(t, func() bool {
		nextBefore = election.ScheduleForEpoch(11)
		return nextBefore != nil
	}, 30*time.Second, 100*time.Millisecond)
	expectedSlots := nextBefore.LeaderSlotsSnapshot()
	expectedNonce := append([]byte(nil), nextBefore.EpochNonce...)

	eventBus.Publish(
		ledgerpkg.PoolStateRestoredEventType,
		event.NewEvent(
			ledgerpkg.PoolStateRestoredEventType,
			ledgerpkg.PoolStateRestoredEvent{Slot: 95},
		),
	)

	require.Eventually(t, func() bool {
		schedule := election.ScheduleForEpoch(11)
		return schedule == nextBefore &&
			bytes.Equal(expectedNonce, schedule.EpochNonce) &&
			assert.ObjectsAreEqual(expectedSlots, schedule.LeaderSlotsSnapshot())
	}, time.Second, 20*time.Millisecond,
		"rollback after cutoff should not invalidate a stable next-epoch schedule")
}

func TestElectionRollbackClearsUnstableNextSchedule(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	epochProvider := newMockEpochProvider()
	epochProvider.activeSlotCoeff = 1.0
	epochProvider.nextEpochReady.Store(11)
	epochProvider.SetEpochNonceForEpoch(11, makeElectionNonce(0x31))

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		electionTestVRFSeed,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	waitForSchedule(t, election, 30*time.Second)
	var nextBefore *Schedule
	require.Eventually(t, func() bool {
		nextBefore = election.ScheduleForEpoch(11)
		return nextBefore != nil
	}, 30*time.Second, 100*time.Millisecond)
	require.True(
		t,
		bytes.Equal(makeElectionNonce(0x31), nextBefore.EpochNonce),
	)

	epochProvider.nextEpochReady.Store(0)
	epochProvider.SetEpochNonceForEpoch(11, makeElectionNonce(0x32))
	eventBus.Publish(
		ledgerpkg.PoolStateRestoredEventType,
		event.NewEvent(
			ledgerpkg.PoolStateRestoredEventType,
			ledgerpkg.PoolStateRestoredEvent{Slot: 90},
		),
	)

	require.Eventually(t, func() bool {
		return election.ScheduleForEpoch(11) == nil
	}, 5*time.Second, 50*time.Millisecond,
		"rollback before cutoff should clear the precomputed next-epoch schedule")

	epochProvider.nextEpochReady.Store(11)
	eventBus.Publish(
		event.EpochNonceReadyEventType,
		event.NewEvent(
			event.EpochNonceReadyEventType,
			event.EpochNonceReadyEvent{
				CurrentEpoch: 10,
				ReadyEpoch:   11,
				CutoffSlot:   95,
			},
		),
	)

	require.Eventually(t, func() bool {
		schedule := election.ScheduleForEpoch(11)
		return schedule != nil &&
			schedule != nextBefore &&
			bytes.Equal(makeElectionNonce(0x32), schedule.EpochNonce)
	}, 30*time.Second, 100*time.Millisecond,
		"nonce-ready replay should rebuild the next epoch schedule after rollback")
}

func TestElectionConcurrentAccess(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 1_000_000
	stakeProvider.poolStakes[string(poolId[:])] = 1_000_000

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch.Store(10)

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		electionTestVRFSeed,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	// Wait for initial schedule so concurrent goroutines exercise
	// both cache-hit and cache-miss paths.
	require.Eventually(t, func() bool {
		return election.CurrentSchedule() != nil
	}, 30*time.Second, 50*time.Millisecond,
		"initial schedule should be computed before concurrent access")

	// Concurrent reads and operations
	done := make(chan bool)
	for i := range 20 {
		go func(slot int) {
			_ = election.ShouldProduceBlock(uint64(slot))
			_ = election.CurrentSchedule()
			_, _ = election.NextLeaderSlot(uint64(slot))
			done <- true
		}(i)
	}

	// Wait for all goroutines
	for range 20 {
		<-done
	}
}
