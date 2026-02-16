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
	"log/slog"
	"sync"
	"time"
)

// SlotTick represents a notification that a slot boundary has been reached
type SlotTick struct {
	// Slot is the current slot number
	Slot uint64
	// SlotStart is the time when this slot started
	SlotStart time.Time
	// Epoch is the current epoch number
	Epoch uint64
	// EpochSlot is the slot number within the current epoch (0-indexed)
	EpochSlot uint64
	// IsEpochStart indicates whether this is the first slot of a new epoch
	IsEpochStart bool
	// SlotsUntilEpoch is the number of slots until the next epoch boundary
	SlotsUntilEpoch uint64
}

// SlotClockConfig holds configuration for the SlotClock
type SlotClockConfig struct {
	// Logger for slot clock events
	Logger *slog.Logger
	// ClockTolerance is the maximum drift allowed when waking at slot boundaries.
	// If we wake up more than this much after the slot boundary, we log a warning.
	// Default: 100ms
	ClockTolerance time.Duration
}

// DefaultSlotClockConfig returns the default configuration
func DefaultSlotClockConfig() SlotClockConfig {
	return SlotClockConfig{
		ClockTolerance: 100 * time.Millisecond,
	}
}

// SlotTimeProvider defines the interface for slot/time conversion
// This allows testing with mock time
type SlotTimeProvider interface {
	SlotToTime(slot uint64) (time.Time, error)
	TimeToSlot(t time.Time) (uint64, error)
	SlotToEpoch(slot uint64) (EpochInfo, error)
}

// EpochInfo contains epoch boundary information
type EpochInfo struct {
	EpochId       uint64
	StartSlot     uint64
	LengthInSlots uint
}

// EndSlot returns the first slot of the next epoch (one past the last slot of this epoch)
func (e EpochInfo) EndSlot() uint64 {
	return e.StartSlot + uint64(e.LengthInSlots)
}

// SlotClock provides slot-boundary-aware timing for the Cardano node.
// It ticks at each slot boundary and notifies subscribers, enabling
// time-based operations like epoch transitions and block production
// that don't depend on incoming blocks.
//
// The SlotClock provides two categories of functionality:
//  1. Query methods (CurrentSlot, CurrentEpoch, GetEpochForSlot, etc.) that work
//     in all modes (catch up, load, synced) and can be called anytime.
//  2. Tick notifications that fire at real-time slot boundaries, useful for
//     leader election and proactive epoch detection when synced to tip.
type SlotClock struct {
	provider    SlotTimeProvider
	config      SlotClockConfig
	subscribers []chan SlotTick
	mu          sync.RWMutex
	cancel      context.CancelFunc
	ctx         context.Context
	running     bool
	wg          sync.WaitGroup

	// Track last emitted epoch to avoid duplicates and detect discrepancies
	lastEmittedEpoch uint64
	epochEmitMu      sync.Mutex

	// For testing: allow injection of custom time source
	nowFunc func() time.Time
}

// NewSlotClock creates a new SlotClock with the given provider and configuration
func NewSlotClock(
	provider SlotTimeProvider,
	config SlotClockConfig,
) *SlotClock {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	if config.ClockTolerance == 0 {
		config.ClockTolerance = DefaultSlotClockConfig().ClockTolerance
	}
	return &SlotClock{
		provider:    provider,
		config:      config,
		subscribers: make([]chan SlotTick, 0),
		nowFunc:     time.Now,
	}
}

// Start begins the slot clock ticking loop.
// The clock will emit SlotTick notifications at each slot boundary.
// Returns immediately; the tick loop runs in a goroutine.
func (sc *SlotClock) Start(ctx context.Context) {
	sc.mu.Lock()
	if sc.running {
		sc.mu.Unlock()
		return
	}
	sc.running = true
	sc.ctx, sc.cancel = context.WithCancel(ctx)
	sc.mu.Unlock()

	sc.wg.Add(1)
	go sc.run()
}

// Stop halts the slot clock and waits for the tick loop to exit.
// All subscriber channels will be closed, causing any goroutines blocked
// on receiving from them to exit cleanly.
func (sc *SlotClock) Stop() {
	sc.mu.Lock()
	if !sc.running {
		sc.mu.Unlock()
		return
	}
	sc.running = false
	if sc.cancel != nil {
		sc.cancel()
	}
	// Close all subscriber channels to unblock any goroutines waiting on them
	for _, ch := range sc.subscribers {
		close(ch)
	}
	sc.subscribers = nil
	sc.mu.Unlock()

	sc.wg.Wait()
}

// Subscribe returns a channel that will receive SlotTick notifications.
// The channel is buffered to prevent blocking the clock loop.
// Call Unsubscribe to stop receiving notifications and close the channel.
func (sc *SlotClock) Subscribe() <-chan SlotTick {
	ch := make(chan SlotTick, 1)
	sc.mu.Lock()
	sc.subscribers = append(sc.subscribers, ch)
	sc.mu.Unlock()
	return ch
}

// Unsubscribe removes a subscriber channel from the notification list.
// The channel will be closed after this call.
func (sc *SlotClock) Unsubscribe(ch <-chan SlotTick) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	for i, sub := range sc.subscribers {
		if sub == ch {
			// Close the channel
			close(sub)
			// Remove from slice
			sc.subscribers = append(sc.subscribers[:i], sc.subscribers[i+1:]...)
			return
		}
	}
}

// =============================================================================
// Query Methods - These work in all modes (catch up, load, synced)
// =============================================================================

// CurrentSlot returns the current slot number based on wall-clock time.
// This works regardless of sync state and can be called during catch up or load.
func (sc *SlotClock) CurrentSlot() (uint64, error) {
	return sc.provider.TimeToSlot(sc.nowFunc())
}

// CurrentEpoch returns the current epoch based on wall-clock time.
// This works regardless of sync state and can be called during catch up or load.
func (sc *SlotClock) CurrentEpoch() (EpochInfo, error) {
	slot, err := sc.CurrentSlot()
	if err != nil {
		return EpochInfo{}, err
	}
	return sc.provider.SlotToEpoch(slot)
}

// GetEpochForSlot returns epoch information for the given slot.
// This works regardless of sync state and can be called during catch up or load.
func (sc *SlotClock) GetEpochForSlot(slot uint64) (EpochInfo, error) {
	return sc.provider.SlotToEpoch(slot)
}

// SlotToTime returns the time when the given slot starts.
// This works regardless of sync state and can be called during catch up or load.
func (sc *SlotClock) SlotToTime(slot uint64) (time.Time, error) {
	return sc.provider.SlotToTime(slot)
}

// NextSlotTime returns the time when the next slot will start
func (sc *SlotClock) NextSlotTime() (time.Time, error) {
	currentSlot, err := sc.CurrentSlot()
	if err != nil {
		return time.Time{}, err
	}
	return sc.provider.SlotToTime(currentSlot + 1)
}

// TimeUntilNextEpoch returns the duration until the next epoch boundary.
// This works regardless of sync state and can be called during catch up or load.
func (sc *SlotClock) TimeUntilNextEpoch() (time.Duration, error) {
	epochInfo, err := sc.CurrentEpoch()
	if err != nil {
		return 0, err
	}
	epochEndSlot := epochInfo.EndSlot()
	epochEndTime, err := sc.provider.SlotToTime(epochEndSlot)
	if err != nil {
		return 0, err
	}
	return epochEndTime.Sub(sc.nowFunc()), nil
}

// TimeUntilSlot returns the duration until the given slot starts.
// Returns negative duration if the slot is in the past.
func (sc *SlotClock) TimeUntilSlot(slot uint64) (time.Duration, error) {
	slotTime, err := sc.provider.SlotToTime(slot)
	if err != nil {
		return 0, err
	}
	return slotTime.Sub(sc.nowFunc()), nil
}

// IsEpochBoundary returns true if the given slot is the first slot of an epoch.
func (sc *SlotClock) IsEpochBoundary(slot uint64) (bool, error) {
	epochInfo, err := sc.provider.SlotToEpoch(slot)
	if err != nil {
		return false, err
	}
	return slot == epochInfo.StartSlot, nil
}

// =============================================================================
// Epoch Event Tracking
// =============================================================================

// MarkEpochEmitted records that an epoch transition event was emitted for the
// given epoch. This is used to coordinate between slot-based and block-based
// epoch detection to avoid duplicate events.
// Returns true if this is a new epoch (not previously emitted), false if duplicate.
func (sc *SlotClock) MarkEpochEmitted(epoch uint64) bool {
	sc.epochEmitMu.Lock()
	defer sc.epochEmitMu.Unlock()

	if epoch <= sc.lastEmittedEpoch {
		return false
	}
	sc.lastEmittedEpoch = epoch
	return true
}

// LastEmittedEpoch returns the last epoch for which an event was emitted.
func (sc *SlotClock) LastEmittedEpoch() uint64 {
	sc.epochEmitMu.Lock()
	defer sc.epochEmitMu.Unlock()
	return sc.lastEmittedEpoch
}

// SetLastEmittedEpoch sets the last emitted epoch. Used during startup to
// initialize the tracker based on stored state.
func (sc *SlotClock) SetLastEmittedEpoch(epoch uint64) {
	sc.epochEmitMu.Lock()
	defer sc.epochEmitMu.Unlock()
	sc.lastEmittedEpoch = epoch
}

// =============================================================================
// Internal tick loop
// =============================================================================

// run is the main tick loop
func (sc *SlotClock) run() {
	defer sc.wg.Done()

	logger := sc.config.Logger.With("component", "slot_clock")

	for {
		// Check context first
		select {
		case <-sc.ctx.Done():
			return
		default:
		}

		// Calculate current slot and next slot boundary
		now := sc.nowFunc()
		currentSlot, err := sc.provider.TimeToSlot(now)
		if err != nil {
			logger.Error("failed to get current slot", "error", err)
			// Sleep briefly and retry
			select {
			case <-sc.ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}

		// Get the time for the next slot
		nextSlot := currentSlot + 1
		nextSlotTime, err := sc.provider.SlotToTime(nextSlot)
		if err != nil {
			logger.Error(
				"failed to get next slot time",
				"error",
				err,
				"slot",
				nextSlot,
			)
			select {
			case <-sc.ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}

		// Sleep until the next slot boundary
		sleepDuration := nextSlotTime.Sub(now)
		if sleepDuration > 0 {
			select {
			case <-sc.ctx.Done():
				return
			case <-time.After(sleepDuration):
			}
		}

		// Verify we're at the correct slot (handle clock drift)
		actualNow := sc.nowFunc()
		actualSlot, err := sc.provider.TimeToSlot(actualNow)
		if err != nil {
			logger.Error("failed to verify slot after wake", "error", err)
			continue
		}

		// Check for drift
		drift := actualNow.Sub(nextSlotTime)
		if drift > sc.config.ClockTolerance {
			logger.Warn("slot clock drift detected",
				"expected_slot", nextSlot,
				"actual_slot", actualSlot,
				"drift", drift,
			)
		}

		// Emit tick for the slot we're at (might have skipped some if drift is large)
		tick, err := sc.buildSlotTick(actualSlot, actualNow)
		if err != nil {
			logger.Error(
				"failed to build slot tick",
				"error",
				err,
				"slot",
				actualSlot,
			)
			continue
		}

		sc.emitTick(tick)
	}
}

// buildSlotTick creates a SlotTick for the given slot
func (sc *SlotClock) buildSlotTick(
	slot uint64,
	slotStart time.Time,
) (SlotTick, error) {
	epochInfo, err := sc.provider.SlotToEpoch(slot)
	if err != nil {
		return SlotTick{}, err
	}

	epochSlot := slot - epochInfo.StartSlot
	slotsUntilEpoch := epochInfo.EndSlot() - slot

	// Check if this is the first slot of an epoch
	isEpochStart := slot == epochInfo.StartSlot

	return SlotTick{
		Slot:            slot,
		SlotStart:       slotStart,
		Epoch:           epochInfo.EpochId,
		EpochSlot:       epochSlot,
		IsEpochStart:    isEpochStart,
		SlotsUntilEpoch: slotsUntilEpoch,
	}, nil
}

// emitTick sends the tick to all subscribers
func (sc *SlotClock) emitTick(tick SlotTick) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	for _, ch := range sc.subscribers {
		select {
		case ch <- tick:
		default:
			// Channel is full, subscriber is slow - skip to avoid blocking
			sc.config.Logger.Debug("slot tick dropped for slow subscriber",
				"slot", tick.Slot,
			)
		}
	}
}

// =============================================================================
// LedgerState adapter
// =============================================================================

// ledgerStateSlotProvider adapts LedgerState to the SlotTimeProvider interface.
type ledgerStateSlotProvider struct {
	ls *LedgerState
}

// newLedgerStateSlotProvider creates a new adapter wrapping the given LedgerState
func newLedgerStateSlotProvider(ls *LedgerState) *ledgerStateSlotProvider {
	return &ledgerStateSlotProvider{ls: ls}
}

// SlotToTime delegates to LedgerState.SlotToTime
func (p *ledgerStateSlotProvider) SlotToTime(slot uint64) (time.Time, error) {
	return p.ls.SlotToTime(slot)
}

// TimeToSlot delegates to LedgerState.TimeToSlot
func (p *ledgerStateSlotProvider) TimeToSlot(t time.Time) (uint64, error) {
	return p.ls.TimeToSlot(t)
}

// SlotToEpoch delegates to LedgerState.SlotToEpoch and converts the result
func (p *ledgerStateSlotProvider) SlotToEpoch(slot uint64) (EpochInfo, error) {
	epoch, err := p.ls.SlotToEpoch(slot)
	if err != nil {
		return EpochInfo{}, err
	}
	return EpochInfo{
		EpochId:       epoch.EpochId,
		StartSlot:     epoch.StartSlot,
		LengthInSlots: epoch.LengthInSlots,
	}, nil
}
