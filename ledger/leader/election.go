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
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"

	"github.com/blinklabs-io/dingo/event"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// StakeDistributionProvider provides stake distribution data for leader election.
type StakeDistributionProvider interface {
	// GetPoolStake returns the stake for a specific pool in the given epoch.
	// For leader election, this should query the "go" snapshot (epoch - 2).
	GetPoolStake(epoch uint64, poolKeyHash []byte) (uint64, error)

	// GetTotalActiveStake returns the total active stake for the given epoch.
	// For leader election, this should query the "go" snapshot (epoch - 2).
	GetTotalActiveStake(epoch uint64) (uint64, error)
}

// EpochInfoProvider provides epoch-related information.
type EpochInfoProvider interface {
	// CurrentEpoch returns the current epoch number.
	CurrentEpoch() uint64

	// EpochNonce returns the nonce for the given epoch.
	EpochNonce(epoch uint64) []byte

	// SlotsPerEpoch returns the number of slots in an epoch.
	SlotsPerEpoch() uint64

	// ActiveSlotCoeff returns the active slot coefficient (f parameter).
	ActiveSlotCoeff() float64
}

// Election manages leader election for a stake pool.
// It maintains the current epoch's schedule and refreshes it on epoch transitions.
type Election struct {
	poolId      lcommon.PoolKeyHash
	poolVrfSkey []byte

	stakeProvider StakeDistributionProvider
	epochProvider EpochInfoProvider
	eventBus      *event.EventBus
	logger        *slog.Logger

	mu             sync.RWMutex
	schedule       *Schedule
	running        bool
	cancel         context.CancelFunc
	stopCh         chan struct{} // signals the monitoring goroutine to exit
	subscriptionId event.EventSubscriberId
}

// NewElection creates a new leader election manager for a stake pool.
func NewElection(
	poolId lcommon.PoolKeyHash,
	poolVrfSkey []byte,
	stakeProvider StakeDistributionProvider,
	epochProvider EpochInfoProvider,
	eventBus *event.EventBus,
	logger *slog.Logger,
) *Election {
	if logger == nil {
		logger = slog.Default()
	}
	return &Election{
		poolId:        poolId,
		poolVrfSkey:   poolVrfSkey,
		stakeProvider: stakeProvider,
		epochProvider: epochProvider,
		eventBus:      eventBus,
		logger:        logger,
	}
}

// Start begins listening for epoch transitions and maintaining the schedule.
// The provided context controls the election's lifecycle. When the context is
// canceled, the election will automatically stop and clean up resources.
func (e *Election) Start(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running {
		return nil
	}

	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel
	e.running = true
	e.stopCh = make(chan struct{})

	// Calculate initial schedule
	if err := e.refreshScheduleUnsafe(ctx); err != nil {
		e.logger.Warn(
			"failed to calculate initial schedule",
			"component", "leader",
			"error", err,
		)
	}

	// Subscribe to epoch transitions
	e.subscriptionId = e.eventBus.SubscribeFunc(
		event.EpochTransitionEventType,
		func(evt event.Event) {
			epochEvent, ok := evt.Data.(event.EpochTransitionEvent)
			if !ok {
				return
			}
			e.logger.Info(
				"epoch transition, refreshing leader schedule",
				"component", "leader",
				"new_epoch", epochEvent.NewEpoch,
			)
			if err := e.RefreshSchedule(ctx); err != nil {
				e.logger.Error(
					"failed to refresh schedule",
					"component", "leader",
					"epoch", epochEvent.NewEpoch,
					"error", err,
				)
			}
		},
	)

	// Monitor context cancellation to automatically stop.
	// The goroutine exits when either the context is canceled or Stop() is called.
	stopCh := e.stopCh
	go func() {
		select {
		case <-ctx.Done():
			_ = e.Stop()
		case <-stopCh:
			// Stop() was called directly, goroutine should exit
		}
	}()

	e.logger.Info(
		"leader election started",
		"component", "leader",
		"pool_id", hex.EncodeToString(e.poolId[:]),
	)

	return nil
}

// Stop stops the leader election manager.
func (e *Election) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.running {
		return nil
	}

	// Signal the monitoring goroutine to exit before canceling context.
	// This prevents the goroutine from calling Stop() again.
	if e.stopCh != nil {
		close(e.stopCh)
		e.stopCh = nil
	}
	if e.cancel != nil {
		e.cancel()
	}
	if e.subscriptionId != 0 {
		e.eventBus.Unsubscribe(
			event.EpochTransitionEventType,
			e.subscriptionId,
		)
		e.subscriptionId = 0
	}
	e.running = false
	e.schedule = nil

	e.logger.Info("leader election stopped", "component", "leader")
	return nil
}

// RefreshSchedule recalculates the leader schedule for the current epoch.
func (e *Election) RefreshSchedule(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.refreshScheduleUnsafe(ctx)
}

// refreshScheduleUnsafe recalculates the schedule without locking.
// Caller must hold e.mu.
func (e *Election) refreshScheduleUnsafe(ctx context.Context) error {
	// Check for context cancellation
	if ctx.Err() != nil {
		return ctx.Err()
	}

	currentEpoch := e.epochProvider.CurrentEpoch()

	// Leader election uses the Go snapshot (epoch - 2)
	if currentEpoch < 2 {
		e.logger.Debug(
			"no Go snapshot available yet",
			"component", "leader",
			"epoch", currentEpoch,
		)
		e.schedule = nil
		return nil
	}

	snapshotEpoch := currentEpoch - 2

	// Get pool stake from Go snapshot
	poolStake, err := e.stakeProvider.GetPoolStake(snapshotEpoch, e.poolId[:])
	if err != nil {
		return fmt.Errorf("get pool stake: %w", err)
	}

	if poolStake == 0 {
		e.logger.Debug(
			"pool has no stake in Go snapshot",
			"component", "leader",
			"epoch", currentEpoch,
			"snapshot_epoch", snapshotEpoch,
		)
		e.schedule = nil
		return nil
	}

	// Get total stake from Go snapshot
	totalStake, err := e.stakeProvider.GetTotalActiveStake(snapshotEpoch)
	if err != nil {
		return fmt.Errorf("get total stake: %w", err)
	}

	if totalStake == 0 {
		return fmt.Errorf("total stake is zero for epoch %d", snapshotEpoch)
	}

	// Get epoch nonce
	epochNonce := e.epochProvider.EpochNonce(currentEpoch)

	// Create calculator with protocol parameters
	calc := NewCalculator(
		e.epochProvider.ActiveSlotCoeff(),
		e.epochProvider.SlotsPerEpoch(),
	)

	// Calculate schedule
	schedule, err := calc.CalculateSchedule(
		currentEpoch,
		e.poolId,
		e.poolVrfSkey,
		poolStake,
		totalStake,
		epochNonce,
	)
	if err != nil {
		return fmt.Errorf("calculate schedule: %w", err)
	}

	e.schedule = schedule

	e.logger.Info(
		"leader schedule calculated",
		"component", "leader",
		"epoch", currentEpoch,
		"pool_stake", poolStake,
		"total_stake", totalStake,
		"stake_ratio", schedule.StakeRatio(),
		"leader_slots", schedule.SlotCount(),
	)

	return nil
}

// ShouldProduceBlock returns true if this pool should produce a block for the slot.
func (e *Election) ShouldProduceBlock(slot uint64) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.schedule == nil {
		return false
	}
	return e.schedule.IsLeaderForSlot(slot)
}

// CurrentSchedule returns the current leader schedule, or nil if not available.
func (e *Election) CurrentSchedule() *Schedule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.schedule
}

// NextLeaderSlot returns the next slot where this pool is leader, starting from
// the given slot. Returns 0 and false if no leader slot is found.
func (e *Election) NextLeaderSlot(fromSlot uint64) (uint64, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.schedule == nil {
		return 0, false
	}

	for _, slot := range e.schedule.LeaderSlots {
		if slot >= fromSlot {
			return slot, true
		}
	}
	return 0, false
}
