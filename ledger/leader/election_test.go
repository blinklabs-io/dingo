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
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blinklabs-io/dingo/event"
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
	epochNonce      []byte
	slotsPerEpoch   uint64
	activeSlotCoeff float64
}

func newMockEpochProvider() *mockEpochProvider {
	m := &mockEpochProvider{
		epochNonce:      electionTestNonce,
		slotsPerEpoch:   electionSlotsPerEpoch,
		activeSlotCoeff: 0.05,
	}
	m.currentEpoch.Store(10)
	return m
}

func (m *mockEpochProvider) CurrentEpoch() uint64 {
	return m.currentEpoch.Load()
}

func (m *mockEpochProvider) EpochNonce(epoch uint64) []byte {
	return m.epochNonce
}

func (m *mockEpochProvider) SlotsPerEpoch() uint64 {
	return m.slotsPerEpoch
}

func (m *mockEpochProvider) ActiveSlotCoeff() float64 {
	return m.activeSlotCoeff
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
