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

package ledger

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSlotTimeProvider implements SlotTimeProvider for testing
type mockSlotTimeProvider struct {
	systemStart   time.Time
	slotLength    time.Duration
	epochLength   uint // slots per epoch
	currentEpoch  uint64
	epochStartMap map[uint64]uint64 // epoch -> startSlot
	mu            sync.RWMutex
}

func newMockSlotTimeProvider(
	systemStart time.Time,
	slotLength time.Duration,
	epochLength uint,
) *mockSlotTimeProvider {
	return &mockSlotTimeProvider{
		systemStart:   systemStart,
		slotLength:    slotLength,
		epochLength:   epochLength,
		epochStartMap: make(map[uint64]uint64),
	}
}

func (m *mockSlotTimeProvider) SlotToTime(slot uint64) (time.Time, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.systemStart.Add(time.Duration(slot) * m.slotLength), nil
}

func (m *mockSlotTimeProvider) TimeToSlot(t time.Time) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if t.Before(m.systemStart) {
		return 0, nil
	}
	elapsed := t.Sub(m.systemStart)
	return uint64(elapsed / m.slotLength), nil
}

func (m *mockSlotTimeProvider) SlotToEpoch(slot uint64) (EpochInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	epochId := slot / uint64(m.epochLength)
	startSlot := epochId * uint64(m.epochLength)
	return EpochInfo{
		EpochId:       epochId,
		StartSlot:     startSlot,
		LengthInSlots: m.epochLength,
	}, nil
}

func TestSlotClockCreation(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)

	clock := NewSlotClock(provider, DefaultSlotClockConfig())
	require.NotNil(t, clock)
	assert.NotNil(t, clock.provider)
	assert.Equal(t, 100*time.Millisecond, clock.config.ClockTolerance)
}

func TestSlotClockCurrentSlot(t *testing.T) {
	systemStart := time.Now().Add(-10 * time.Second)
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)

	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	slot, err := clock.CurrentSlot()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, slot, uint64(9))
	assert.LessOrEqual(t, slot, uint64(11))
}

func TestSlotClockCurrentEpoch(t *testing.T) {
	// Start 150 seconds ago with 1 second slots and 100 slot epochs
	// Should be in epoch 1 (slots 100-199)
	systemStart := time.Now().Add(-150 * time.Second)
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)

	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	epochInfo, err := clock.CurrentEpoch()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), epochInfo.EpochId)
	assert.Equal(t, uint64(100), epochInfo.StartSlot)
	assert.Equal(t, uint(100), epochInfo.LengthInSlots)
}

func TestSlotClockGetEpochForSlot(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	testCases := []struct {
		slot          uint64
		expectedEpoch uint64
		expectedStart uint64
	}{
		{slot: 0, expectedEpoch: 0, expectedStart: 0},
		{slot: 50, expectedEpoch: 0, expectedStart: 0},
		{slot: 99, expectedEpoch: 0, expectedStart: 0},
		{slot: 100, expectedEpoch: 1, expectedStart: 100},
		{slot: 150, expectedEpoch: 1, expectedStart: 100},
		{slot: 200, expectedEpoch: 2, expectedStart: 200},
	}

	for _, tc := range testCases {
		epochInfo, err := clock.GetEpochForSlot(tc.slot)
		require.NoError(t, err)
		assert.Equal(t, tc.expectedEpoch, epochInfo.EpochId, "epoch mismatch for slot %d", tc.slot)
		assert.Equal(t, tc.expectedStart, epochInfo.StartSlot, "start slot mismatch for slot %d", tc.slot)
	}
}

func TestSlotClockNextSlotTime(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)

	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	nextTime, err := clock.NextSlotTime()
	require.NoError(t, err)
	assert.True(t, nextTime.After(time.Now()))
}

func TestSlotClockTimeUntilNextEpoch(t *testing.T) {
	// Start 50 seconds ago, 1 second slots, 100 slot epochs
	// We should be around slot 50, with ~50 seconds until epoch boundary
	systemStart := time.Now().Add(-50 * time.Second)
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)

	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	duration, err := clock.TimeUntilNextEpoch()
	require.NoError(t, err)
	// Should be approximately 50 seconds (give or take a second)
	assert.Greater(t, duration, 48*time.Second)
	assert.Less(t, duration, 52*time.Second)
}

func TestSlotClockTimeUntilSlot(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	// Time until slot 10 (10 seconds in the future)
	duration, err := clock.TimeUntilSlot(10)
	require.NoError(t, err)
	assert.Greater(t, duration, 9*time.Second)
	assert.Less(t, duration, 11*time.Second)

	// Time until slot in the past
	systemStart2 := time.Now().Add(-20 * time.Second)
	provider2 := newMockSlotTimeProvider(systemStart2, time.Second, 100)
	clock2 := NewSlotClock(provider2, DefaultSlotClockConfig())

	duration2, err := clock2.TimeUntilSlot(5)
	require.NoError(t, err)
	assert.Less(t, duration2, time.Duration(0)) // Negative = in the past
}

func TestSlotClockIsEpochBoundary(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	testCases := []struct {
		slot       uint64
		isBoundary bool
	}{
		{slot: 0, isBoundary: true},
		{slot: 1, isBoundary: false},
		{slot: 99, isBoundary: false},
		{slot: 100, isBoundary: true},
		{slot: 101, isBoundary: false},
		{slot: 200, isBoundary: true},
	}

	for _, tc := range testCases {
		isBoundary, err := clock.IsEpochBoundary(tc.slot)
		require.NoError(t, err)
		assert.Equal(t, tc.isBoundary, isBoundary, "boundary check failed for slot %d", tc.slot)
	}
}

func TestEpochInfoEndSlot(t *testing.T) {
	epochInfo := EpochInfo{
		EpochId:       5,
		StartSlot:     500,
		LengthInSlots: 100,
	}
	assert.Equal(t, uint64(600), epochInfo.EndSlot())
}

func TestSlotClockSubscribeUnsubscribe(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	// Subscribe
	ch := clock.Subscribe()
	require.NotNil(t, ch)

	clock.mu.RLock()
	assert.Len(t, clock.subscribers, 1)
	clock.mu.RUnlock()

	// Unsubscribe
	clock.Unsubscribe(ch)

	clock.mu.RLock()
	assert.Len(t, clock.subscribers, 0)
	clock.mu.RUnlock()
}

func TestSlotClockMultipleSubscribers(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	// Subscribe multiple times
	ch1 := clock.Subscribe()
	ch2 := clock.Subscribe()
	ch3 := clock.Subscribe()

	clock.mu.RLock()
	assert.Len(t, clock.subscribers, 3)
	clock.mu.RUnlock()

	// Unsubscribe middle one
	clock.Unsubscribe(ch2)

	clock.mu.RLock()
	assert.Len(t, clock.subscribers, 2)
	clock.mu.RUnlock()

	// Unsubscribe all
	clock.Unsubscribe(ch1)
	clock.Unsubscribe(ch3)

	clock.mu.RLock()
	assert.Len(t, clock.subscribers, 0)
	clock.mu.RUnlock()
}

func TestSlotClockStartStop(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, 100*time.Millisecond, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	ctx := context.Background()

	// Start
	clock.Start(ctx)
	clock.mu.RLock()
	assert.True(t, clock.running)
	clock.mu.RUnlock()

	// Double start should be no-op
	clock.Start(ctx)
	clock.mu.RLock()
	assert.True(t, clock.running)
	clock.mu.RUnlock()

	// Stop
	clock.Stop()
	clock.mu.RLock()
	assert.False(t, clock.running)
	clock.mu.RUnlock()

	// Double stop should be no-op
	clock.Stop()
	clock.mu.RLock()
	assert.False(t, clock.running)
	clock.mu.RUnlock()
}

func TestSlotClockReceivesTicks(t *testing.T) {
	// Use short slot length for faster test
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, 50*time.Millisecond, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	ch := clock.Subscribe()
	ctx := t.Context()

	clock.Start(ctx)
	defer clock.Stop()

	// Wait for a tick
	select {
	case tick := <-ch:
		assert.GreaterOrEqual(t, tick.Slot, uint64(0))
		assert.False(t, tick.SlotStart.IsZero())
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timeout waiting for slot tick")
	}
}

func TestSlotClockEpochBoundary(t *testing.T) {
	// Set up so we're close to an epoch boundary
	// Epoch length = 10 slots, slot length = 20ms
	systemStart := time.Now().Add(-9 * 20 * time.Millisecond) // Start at slot 9
	provider := newMockSlotTimeProvider(systemStart, 20*time.Millisecond, 10)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	ch := clock.Subscribe()
	ctx := t.Context()

	clock.Start(ctx)
	defer clock.Stop()

	// Collect ticks until we see an epoch boundary
	var sawEpochStart bool
	timeout := time.After(500 * time.Millisecond)
	for !sawEpochStart {
		select {
		case tick := <-ch:
			if tick.IsEpochStart {
				sawEpochStart = true
				assert.Equal(t, uint64(0), tick.EpochSlot)
			}
		case <-timeout:
			t.Fatal("timeout waiting for epoch boundary tick")
		}
	}
}

func TestSlotTickSlotsUntilEpoch(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	// Test at various points in epoch
	testCases := []struct {
		slot            uint64
		expectedRemain  uint64
		expectedIsStart bool
	}{
		{slot: 0, expectedRemain: 100, expectedIsStart: true},   // Start of epoch 0
		{slot: 1, expectedRemain: 99, expectedIsStart: false},   // Near start
		{slot: 50, expectedRemain: 50, expectedIsStart: false},  // Middle
		{slot: 99, expectedRemain: 1, expectedIsStart: false},   // Last slot of epoch 0
		{slot: 100, expectedRemain: 100, expectedIsStart: true}, // Start of epoch 1
		{slot: 150, expectedRemain: 50, expectedIsStart: false}, // Middle of epoch 1
		{slot: 199, expectedRemain: 1, expectedIsStart: false},  // Last slot of epoch 1
		{slot: 200, expectedRemain: 100, expectedIsStart: true}, // Start of epoch 2
	}

	for _, tc := range testCases {
		t.Run("slot_"+string(rune(tc.slot)), func(t *testing.T) {
			slotTime, _ := provider.SlotToTime(tc.slot)
			tick, err := clock.buildSlotTick(tc.slot, slotTime)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedRemain, tick.SlotsUntilEpoch, "slots until epoch mismatch for slot %d", tc.slot)
			assert.Equal(t, tc.expectedIsStart, tick.IsEpochStart, "epoch start mismatch for slot %d", tc.slot)
		})
	}
}

func TestSlotClockConcurrentSubscribers(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, 20*time.Millisecond, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	ctx := t.Context()

	clock.Start(ctx)
	defer clock.Stop()

	// Create many concurrent subscribers
	const numSubscribers = 10
	var wg sync.WaitGroup
	wg.Add(numSubscribers)

	for range numSubscribers {
		go func() {
			defer wg.Done()
			ch := clock.Subscribe()
			defer clock.Unsubscribe(ch)

			// Wait for at least one tick
			select {
			case <-ch:
				// Got a tick
			case <-time.After(100 * time.Millisecond):
				// Timeout is ok, just testing concurrency safety
			}
		}()
	}

	wg.Wait()
}

func TestSlotClockContextCancellation(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, 100*time.Millisecond, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	ctx, cancel := context.WithCancel(context.Background())
	clock.Start(ctx)

	// Cancel context
	cancel()

	// Wait briefly for goroutine to exit
	done := make(chan struct{})
	go func() {
		clock.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Good, clock stopped
	case <-time.After(500 * time.Millisecond):
		t.Fatal("clock did not stop after context cancellation")
	}
}

func TestBuildSlotTick(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	slotTime, _ := provider.SlotToTime(150)
	tick, err := clock.buildSlotTick(150, slotTime)
	require.NoError(t, err)

	assert.Equal(t, uint64(150), tick.Slot)
	assert.Equal(t, slotTime, tick.SlotStart)
	assert.Equal(t, uint64(1), tick.Epoch)
	assert.Equal(t, uint64(50), tick.EpochSlot)
	assert.False(t, tick.IsEpochStart)
	assert.Equal(t, uint64(50), tick.SlotsUntilEpoch)
}

func TestEmitTickToSubscribers(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	ch1 := clock.Subscribe()
	ch2 := clock.Subscribe()

	tick := SlotTick{
		Slot:      42,
		SlotStart: time.Now(),
		Epoch:     0,
	}

	clock.emitTick(tick)

	// Both channels should receive the tick
	select {
	case received := <-ch1:
		assert.Equal(t, uint64(42), received.Slot)
	default:
		t.Fatal("ch1 did not receive tick")
	}

	select {
	case received := <-ch2:
		assert.Equal(t, uint64(42), received.Slot)
	default:
		t.Fatal("ch2 did not receive tick")
	}
}

func TestSlotClockDropsTickForSlowSubscriber(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	// Subscribe but don't read from channel
	ch := clock.Subscribe()

	// Fill the buffer
	tick1 := SlotTick{Slot: 1}
	clock.emitTick(tick1)

	// This should be dropped (buffer is full)
	tick2 := SlotTick{Slot: 2}
	clock.emitTick(tick2)

	// Only first tick should be in the channel
	select {
	case received := <-ch:
		assert.Equal(t, uint64(1), received.Slot)
	default:
		t.Fatal("expected tick 1 in channel")
	}

	// Channel should be empty now
	select {
	case <-ch:
		t.Fatal("unexpected second tick in channel")
	default:
		// Expected
	}
}

// =============================================================================
// Epoch Event Coordination Tests
// =============================================================================

func TestMarkEpochEmitted(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	// First call should succeed
	assert.True(t, clock.MarkEpochEmitted(5))
	assert.Equal(t, uint64(5), clock.LastEmittedEpoch())

	// Same epoch should fail (duplicate)
	assert.False(t, clock.MarkEpochEmitted(5))

	// Earlier epoch should fail
	assert.False(t, clock.MarkEpochEmitted(3))

	// Later epoch should succeed
	assert.True(t, clock.MarkEpochEmitted(7))
	assert.Equal(t, uint64(7), clock.LastEmittedEpoch())
}

func TestSetLastEmittedEpoch(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	// Initialize with epoch 10
	clock.SetLastEmittedEpoch(10)
	assert.Equal(t, uint64(10), clock.LastEmittedEpoch())

	// Trying to emit epoch 5 should fail (already past)
	assert.False(t, clock.MarkEpochEmitted(5))

	// Trying to emit epoch 11 should succeed
	assert.True(t, clock.MarkEpochEmitted(11))
}

func TestMarkEpochEmittedConcurrent(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	// Try to emit the same epoch from multiple goroutines
	// Only one should succeed
	const numGoroutines = 10
	successCount := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for range numGoroutines {
		go func() {
			defer wg.Done()
			if clock.MarkEpochEmitted(100) {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, 1, successCount, "exactly one goroutine should succeed")
}

func TestSlotClockSlotToTime(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	slotTime, err := clock.SlotToTime(10)
	require.NoError(t, err)

	expected := systemStart.Add(10 * time.Second)
	assert.Equal(t, expected, slotTime)
}

func TestSlotClockStopClosesSubscriberChannels(t *testing.T) {
	systemStart := time.Now()
	provider := newMockSlotTimeProvider(systemStart, time.Second, 100)
	clock := NewSlotClock(provider, DefaultSlotClockConfig())

	ch := clock.Subscribe()
	ctx := context.Background()
	clock.Start(ctx)

	// Start a goroutine that blocks on the channel
	done := make(chan struct{})
	go func() {
		for range ch {
			// Drain the channel
		}
		close(done)
	}()

	// Stop should close the channel, unblocking the goroutine
	clock.Stop()

	// The goroutine should exit within a reasonable time
	select {
	case <-done:
		// Good, channel was closed and goroutine exited
	case <-time.After(500 * time.Millisecond):
		t.Fatal("subscriber goroutine did not exit after Stop")
	}

	// Verify subscribers list is cleared
	clock.mu.RLock()
	assert.Nil(t, clock.subscribers)
	clock.mu.RUnlock()
}
