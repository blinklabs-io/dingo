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

// maxCachedSchedules is the number of epoch schedules to keep in memory.
// We keep the current and previous epoch to handle slots near boundaries.
const maxCachedSchedules = 3

// Election manages leader election for a stake pool.
// It maintains schedules for recent epochs and refreshes them on epoch
// transitions (from block processing events) or on demand when the
// slot clock advances into an epoch without a cached schedule.
//
// Schedule computation (VRF for every slot in an epoch) is expensive and
// runs without holding the election lock so that ShouldProduceBlock remains
// a fast, lock-free lookup on the forger's hot path.
type Election struct {
	poolId      lcommon.PoolKeyHash
	poolVrfSkey []byte

	stakeProvider StakeDistributionProvider
	epochProvider EpochInfoProvider
	eventBus      *event.EventBus
	logger        *slog.Logger

	mu             sync.RWMutex
	schedules      map[uint64]*Schedule // epoch -> schedule
	running        bool
	cancel         context.CancelFunc
	stopCh         chan struct{} // signals the monitoring goroutine to exit
	computeCh      chan uint64   // requests background schedule computation
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
//
// The initial schedule for the current epoch is computed asynchronously in
// the background so Start returns immediately and the forger can begin its
// slot-aligned loop without delay.
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
	e.schedules = make(map[uint64]*Schedule)
	e.computeCh = make(chan uint64, 4)

	// Subscribe to epoch transitions using a channel so we can drain
	// stale events during rapid sync (e.g., devnet with 500ms epochs).
	var evtCh <-chan event.Event
	e.subscriptionId, evtCh = e.eventBus.Subscribe(
		event.EpochTransitionEventType,
	)

	if evtCh == nil {
		e.logger.Warn(
			"event bus not available, epoch transitions will not be tracked",
			"component", "leader",
		)
	} else {
		go e.epochTransitionLoop(ctx, evtCh)
	}
	go e.scheduleComputeLoop(ctx, e.computeCh)

	// Kick off initial schedule computation for the current epoch.
	// The next epoch's schedule cannot be pre-computed because
	// its nonce depends on the stability window passing first.
	currentEpoch := e.epochProvider.CurrentEpoch()
	select {
	case e.computeCh <- currentEpoch:
	default:
	}

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

// epochTransitionLoop reads epoch transition events from the channel,
// draining any queued stale events so only the latest is processed.
// This prevents wasted schedule recalculations during rapid sync.
func (e *Election) epochTransitionLoop(
	ctx context.Context,
	evtCh <-chan event.Event,
) {
	for evt := range evtCh {
		// Drain any queued events, keeping only the latest.
		latest := evt
	drain:
		for {
			select {
			case newer, ok := <-evtCh:
				if !ok {
					return
				}
				latest = newer
			default:
				break drain
			}
		}

		if ctx.Err() != nil {
			return
		}

		epochEvent, ok := latest.Data.(event.EpochTransitionEvent)
		if !ok {
			e.logger.Error(
				"invalid event data for epoch transition",
				"component", "leader",
			)
			continue
		}

		e.logger.Info(
			"epoch transition, refreshing leader schedule",
			"component", "leader",
			"new_epoch", epochEvent.NewEpoch,
		)
		if err := e.RefreshScheduleForEpoch(
			ctx,
			epochEvent.NewEpoch,
		); err != nil {
			e.logger.Error(
				"failed to refresh schedule",
				"component", "leader",
				"epoch", epochEvent.NewEpoch,
				"error", err,
			)
		}
	}
}

// scheduleComputeLoop processes background schedule computation requests.
// When ShouldProduceBlock detects a missing schedule for a slot's epoch,
// it sends the epoch number to computeCh. This goroutine picks it up and
// computes the schedule without blocking the forger's hot path.
func (e *Election) scheduleComputeLoop(
	ctx context.Context,
	computeCh <-chan uint64,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case epoch := <-computeCh:
			// Skip if schedule already exists
			e.mu.RLock()
			_, exists := e.schedules[epoch]
			e.mu.RUnlock()
			if exists {
				continue
			}

			e.logger.Info(
				"computing leader schedule",
				"component", "leader",
				"epoch", epoch,
			)
			if err := e.RefreshScheduleForEpoch(
				ctx,
				epoch,
			); err != nil {
				e.logger.Warn(
					"background schedule compute failed",
					"component", "leader",
					"epoch", epoch,
					"error", err,
				)
			}
		}
	}
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
	// Nil out computeCh so ShouldProduceBlock cannot send after Stop.
	e.computeCh = nil
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
	e.schedules = nil

	e.logger.Info("leader election stopped", "component", "leader")
	return nil
}

// RefreshSchedule recalculates the leader schedule for the current epoch.
func (e *Election) RefreshSchedule(ctx context.Context) error {
	return e.RefreshScheduleForEpoch(ctx, e.epochProvider.CurrentEpoch())
}

// RefreshScheduleForEpoch computes the leader schedule for the given epoch.
// The expensive VRF computation runs WITHOUT holding the election lock so
// that ShouldProduceBlock callers are never blocked. The lock is only held
// briefly to store the result.
func (e *Election) RefreshScheduleForEpoch(
	ctx context.Context,
	epoch uint64,
) error {
	e.mu.RLock()
	running := e.running
	e.mu.RUnlock()
	if !running {
		return nil
	}

	schedule, err := e.computeSchedule(ctx, epoch)
	if err != nil {
		return err
	}
	if schedule == nil {
		return nil // no stake or nonce not available
	}
	e.storeSchedule(epoch, schedule)
	return nil
}

// computeSchedule gathers stake data, epoch nonce, and runs VRF for every
// slot in the epoch. This is the expensive operation (~70ms per slot) and
// does NOT hold any lock.
func (e *Election) computeSchedule(
	ctx context.Context,
	currentEpoch uint64,
) (*Schedule, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Leader election uses the Go snapshot (epoch - 2).
	// For genesis epochs (0 and 1), use the genesis snapshot directly.
	// The Cardano spec uses genesis staking for leader election until
	// the first Mark→Set→Go rotation completes at epoch 2.
	var snapshotEpoch uint64
	if currentEpoch >= 2 {
		snapshotEpoch = currentEpoch - 2
	}

	// Get pool stake from Go snapshot
	poolStake, err := e.stakeProvider.GetPoolStake(snapshotEpoch, e.poolId[:])
	if err != nil {
		return nil, fmt.Errorf("get pool stake: %w", err)
	}

	e.logger.Info(
		"pool stake from Go snapshot",
		"component", "leader",
		"epoch", currentEpoch,
		"snapshot_epoch", snapshotEpoch,
		"pool_stake", poolStake,
	)
	if poolStake == 0 {
		e.logger.Info(
			"pool has no stake in Go snapshot, skipping schedule computation",
			"component", "leader",
			"epoch", currentEpoch,
			"snapshot_epoch", snapshotEpoch,
		)
		return nil, nil
	}

	// Get total stake from Go snapshot
	totalStake, err := e.stakeProvider.GetTotalActiveStake(snapshotEpoch)
	if err != nil {
		return nil, fmt.Errorf("get total stake: %w", err)
	}

	e.logger.Info(
		"total active stake from Go snapshot",
		"component", "leader",
		"epoch", currentEpoch,
		"snapshot_epoch", snapshotEpoch,
		"total_stake", totalStake,
	)
	if totalStake == 0 {
		return nil, fmt.Errorf(
			"total stake is zero for epoch %d", snapshotEpoch,
		)
	}

	// Get epoch nonce. The nonce may not be available yet if the slot clock
	// fired the epoch transition before block processing computed the nonce.
	// In that case, skip this schedule — the next epoch transition will retry.
	epochNonce := e.epochProvider.EpochNonce(currentEpoch)
	if len(epochNonce) == 0 {
		e.logger.Info(
			"epoch nonce not yet available, skipping schedule",
			"component", "leader",
			"epoch", currentEpoch,
		)
		return nil, nil
	}

	calc := NewCalculator(
		e.epochProvider.ActiveSlotCoeff(),
		e.epochProvider.SlotsPerEpoch(),
	)

	schedule, err := calc.CalculateSchedule(
		currentEpoch,
		e.poolId,
		e.poolVrfSkey,
		poolStake,
		totalStake,
		epochNonce,
	)
	if err != nil {
		return nil, fmt.Errorf("calculate schedule: %w", err)
	}

	e.logger.Info(
		"leader schedule calculated",
		"component", "leader",
		"epoch", currentEpoch,
		"pool_stake", poolStake,
		"total_stake", totalStake,
		"stake_ratio", schedule.StakeRatio(),
		"leader_slots", schedule.SlotCount(),
	)

	return schedule, nil
}

// storeSchedule saves a computed schedule under a brief write lock and
// prunes old epochs from the cache.
func (e *Election) storeSchedule(epoch uint64, schedule *Schedule) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.running {
		return
	}
	e.schedules[epoch] = schedule

	// Prune old schedules to bound memory usage
	for ep := range e.schedules {
		if epoch >= maxCachedSchedules &&
			ep < epoch-maxCachedSchedules+1 {
			delete(e.schedules, ep)
		}
	}
}

// ShouldProduceBlock returns true if this pool should produce a block for the
// slot. This is a pure lookup with no database access — it checks the cached
// schedule for the slot's epoch. If no schedule is cached, it requests a
// background computation and returns false.
func (e *Election) ShouldProduceBlock(slot uint64) bool {
	slotsPerEpoch := e.epochProvider.SlotsPerEpoch()
	if slotsPerEpoch == 0 {
		return false
	}
	slotEpoch := slot / slotsPerEpoch

	e.mu.RLock()
	var schedule *Schedule
	if e.schedules != nil {
		schedule = e.schedules[slotEpoch]
	}
	computeCh := e.computeCh
	e.mu.RUnlock()

	if schedule == nil {
		// Schedule not yet computed for this epoch.
		// Request background computation (non-blocking).
		e.logger.Debug(
			"no leader schedule for epoch, requesting computation",
			"component", "leader",
			"slot", slot,
			"epoch", slotEpoch,
		)
		if computeCh != nil {
			select {
			case computeCh <- slotEpoch:
			default:
				// Request already pending
			}
		}
		return false
	}
	return schedule.IsLeaderForSlot(slot)
}

// CurrentSchedule returns the leader schedule for the current epoch, or nil.
func (e *Election) CurrentSchedule() *Schedule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.schedules == nil {
		return nil
	}
	epoch := e.epochProvider.CurrentEpoch()
	return e.schedules[epoch]
}

// ScheduleForEpoch returns the cached leader schedule for a specific epoch,
// or nil if not computed.
func (e *Election) ScheduleForEpoch(epoch uint64) *Schedule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.schedules == nil {
		return nil
	}
	return e.schedules[epoch]
}

// NextLeaderSlot returns the next slot where this pool is leader, starting from
// the given slot. Returns 0 and false if no leader slot is found.
func (e *Election) NextLeaderSlot(fromSlot uint64) (uint64, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.schedules == nil {
		return 0, false
	}

	slotsPerEpoch := e.epochProvider.SlotsPerEpoch()
	if slotsPerEpoch == 0 {
		return 0, false
	}

	epoch := fromSlot / slotsPerEpoch
	schedule := e.schedules[epoch]
	if schedule != nil {
		for _, slot := range schedule.LeaderSlots {
			if slot >= fromSlot {
				return slot, true
			}
		}
	}

	// No leader slot found in the current epoch; check the next epoch's
	// cached schedule in case we are near an epoch boundary.
	nextSchedule := e.schedules[epoch+1]
	if nextSchedule != nil {
		for _, slot := range nextSchedule.LeaderSlots {
			if slot >= fromSlot {
				return slot, true
			}
		}
	}

	return 0, false
}
