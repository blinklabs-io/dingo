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
	"log/slog"
	"testing"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger/snapshot"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStakeProvider implements StakeDistributionProvider for testing
type mockStakeProvider struct {
	stakeDistribution *snapshot.StakeDistribution
	poolStakes        map[string]uint64
	totalStake        uint64
	err               error
}

func newMockStakeProvider() *mockStakeProvider {
	return &mockStakeProvider{
		poolStakes: make(map[string]uint64),
	}
}

func (m *mockStakeProvider) GetStakeDistribution(
	epoch uint64,
) (*snapshot.StakeDistribution, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.stakeDistribution, nil
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
	currentEpoch    uint64
	epochNonce      []byte
	slotsPerEpoch   uint64
	activeSlotCoeff float64
}

func newMockEpochProvider() *mockEpochProvider {
	return &mockEpochProvider{
		currentEpoch:    10,
		epochNonce:      []byte("testnonce"),
		slotsPerEpoch:   432000,
		activeSlotCoeff: 0.05,
	}
}

func (m *mockEpochProvider) CurrentEpoch() uint64 {
	return m.currentEpoch
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

func TestNewElection(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	copy(poolId[:], []byte("testpool1234567890123"))
	vrfKey := []byte("vrfsecretkey")

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
		nil,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	// Start should succeed
	err := election.Start()
	require.NoError(t, err)

	// Start again should be idempotent
	err = election.Start()
	require.NoError(t, err)

	// Stop should succeed
	err = election.Stop()
	require.NoError(t, err)

	// Stop again should be idempotent
	err = election.Stop()
	require.NoError(t, err)
}

func TestElectionScheduleEarlyEpochs(t *testing.T) {
	// Test behavior when epoch < 2 (no Go snapshot available)
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	stakeProvider.poolStakes[string(poolId[:])] = 1000

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch = 1 // Too early for Go snapshot

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		nil,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start()
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	// Schedule should be nil for early epochs
	assert.Nil(t, election.CurrentSchedule())
	assert.False(t, election.ShouldProduceBlock(100))
}

func TestElectionZeroPoolStake(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	// No pool stake set (zero)

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch = 10

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		nil,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start()
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	// Schedule should be nil when pool has no stake
	assert.Nil(t, election.CurrentSchedule())
}

func TestElectionShouldProduceBlock(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	stakeProvider.poolStakes[string(poolId[:])] = 1000

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch = 10
	epochProvider.slotsPerEpoch = 10 // Small for testing

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		nil,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start()
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	// With placeholder isSlotLeader returning false, no slots are leader slots
	schedule := election.CurrentSchedule()
	require.NotNil(t, schedule)
	assert.Equal(t, 0, schedule.SlotCount())

	// ShouldProduceBlock should return false for all slots
	assert.False(t, election.ShouldProduceBlock(100))
	assert.False(t, election.ShouldProduceBlock(101))
}

func TestElectionNextLeaderSlot(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	stakeProvider.poolStakes[string(poolId[:])] = 1000

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch = 10
	epochProvider.slotsPerEpoch = 10

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		nil,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	// No schedule - should return 0, false
	slot, found := election.NextLeaderSlot(0)
	assert.Equal(t, uint64(0), slot)
	assert.False(t, found)

	err := election.Start()
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	// With placeholder returning no leader slots, should return 0, false
	slot, found = election.NextLeaderSlot(0)
	assert.Equal(t, uint64(0), slot)
	assert.False(t, found)
}

func TestElectionEpochTransition(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	stakeProvider.poolStakes[string(poolId[:])] = 1000

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch = 10
	epochProvider.slotsPerEpoch = 10

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		nil,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start()
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

	// Verify initial schedule
	schedule := election.CurrentSchedule()
	require.NotNil(t, schedule)
	assert.Equal(t, uint64(10), schedule.Epoch)

	// Simulate epoch transition by updating provider and sending event
	epochProvider.currentEpoch = 11

	// Publish epoch transition event
	eventBus.Publish(
		event.EpochTransitionEventType,
		event.NewEvent(
			event.EpochTransitionEventType,
			event.EpochTransitionEvent{
				PreviousEpoch: 10,
				NewEpoch:      11,
				BoundarySlot:  110,
				EpochNonce:    []byte("newepochnonce"),
			},
		),
	)

	// Poll for event to be processed with timeout
	require.Eventually(t, func() bool {
		schedule := election.CurrentSchedule()
		return schedule != nil && schedule.Epoch == 11
	}, 2*time.Second, 10*time.Millisecond, "schedule should update to epoch 11")

	// Schedule should be updated to new epoch
	schedule = election.CurrentSchedule()
	require.NotNil(t, schedule)
	assert.Equal(t, uint64(11), schedule.Epoch)
}

func TestElectionConcurrentAccess(t *testing.T) {
	poolId := lcommon.PoolKeyHash{}
	stakeProvider := newMockStakeProvider()
	stakeProvider.totalStake = 10000
	stakeProvider.poolStakes[string(poolId[:])] = 1000

	epochProvider := newMockEpochProvider()
	epochProvider.currentEpoch = 10
	epochProvider.slotsPerEpoch = 10

	eventBus := event.NewEventBus(nil, nil)
	defer eventBus.Stop()

	election := NewElection(
		poolId,
		nil,
		stakeProvider,
		epochProvider,
		eventBus,
		slog.Default(),
	)

	err := election.Start()
	require.NoError(t, err)
	defer func() { _ = election.Stop() }()

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
