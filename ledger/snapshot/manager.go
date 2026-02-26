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

// Package snapshot provides stake snapshot management for Ouroboros Praos
// leader election. It captures stake distribution at epoch boundaries and
// maintains the Mark/Set/Go snapshot rotation model.
package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
)

// Manager handles stake snapshot capture and rotation at epoch boundaries.
// It subscribes to EpochTransitionEvents and orchestrates the snapshot
// lifecycle according to the Ouroboros Praos specification.
type Manager struct {
	db       *database.Database
	eventBus *event.EventBus
	logger   *slog.Logger

	mu             sync.RWMutex
	running        bool
	stopping       bool
	cancel         context.CancelFunc
	subscriptionId event.EventSubscriberId
	loopWg         sync.WaitGroup
}

// NewManager creates a new snapshot manager.
func NewManager(
	db *database.Database,
	eventBus *event.EventBus,
	logger *slog.Logger,
) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		db:       db,
		eventBus: eventBus,
		logger:   logger,
	}
}

// Start begins listening for epoch transitions and capturing
// snapshots. The provided context is used as the parent for the
// manager's internal context; cancelling it will stop all
// snapshot operations.
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return nil
	}
	if m.stopping {
		return errors.New(
			"snapshot manager: Stop in progress, cannot Start",
		)
	}

	// Guard against nil dependencies to avoid panics
	if ctx == nil {
		return errors.New(
			"snapshot manager: nil context",
		)
	}
	if m.db == nil {
		return errors.New("snapshot manager: nil database")
	}
	if m.eventBus == nil {
		return errors.New("snapshot manager: nil event bus")
	}

	// Reject an already-cancelled context so we don't mark the manager as
	// running while the event loop would exit immediately.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("snapshot manager: parent context already done: %w", err)
	}

	childCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.running = true

	// Subscribe to epoch transitions using a channel so we can drain
	// stale events during rapid sync (e.g., devnet with 500ms epochs).
	var evtCh <-chan event.Event
	m.subscriptionId, evtCh = m.eventBus.Subscribe(
		event.EpochTransitionEventType,
	)

	if evtCh == nil {
		m.logger.Warn(
			"event bus not available, epoch transitions will not be tracked",
			"component", "snapshot",
		)
	} else {
		m.loopWg.Add(1)
		go func() {
			defer m.loopWg.Done()
			m.epochTransitionLoop(childCtx, evtCh)
			// If the goroutine exits because the parent
			// context was cancelled (not via Stop), reset
			// running and unsubscribe so Start can be
			// called again without leaking a stale
			// subscriber.
			m.mu.Lock()
			if !m.stopping {
				m.running = false
				if m.cancel != nil {
					m.cancel()
					m.cancel = nil
				}
				if m.subscriptionId != 0 {
					m.eventBus.Unsubscribe(
						event.EpochTransitionEventType,
						m.subscriptionId,
					)
					m.subscriptionId = 0
				}
			}
			m.mu.Unlock()
		}()
	}

	m.logger.Info("snapshot manager started", "component", "snapshot")
	return nil
}

// Stop stops the snapshot manager.
func (m *Manager) Stop() error {
	m.mu.Lock()
	if !m.running {
		m.mu.Unlock()
		return nil
	}

	m.stopping = true
	if m.cancel != nil {
		m.cancel()
	}
	if m.subscriptionId != 0 {
		m.eventBus.Unsubscribe(
			event.EpochTransitionEventType,
			m.subscriptionId,
		)
		m.subscriptionId = 0
	}
	m.running = false
	m.mu.Unlock()

	// Wait outside the lock — Stop releases m.mu before Wait so
	// the goroutine's cleanup path can re-acquire it.
	m.loopWg.Wait()

	// Clear transient state so Start can be called again.
	m.mu.Lock()
	m.stopping = false
	m.cancel = nil
	m.mu.Unlock()

	m.logger.Info(
		"snapshot manager stopped",
		"component", "snapshot",
	)
	return nil
}

// epochTransitionLoop reads epoch transition events from the channel,
// draining any queued stale events so only the latest is processed.
// During rapid sync (e.g., devnet with fast epochs), many epoch
// transitions can queue up while a snapshot is being captured. This
// loop skips intermediate epochs to avoid wasting work on snapshots
// that will be immediately superseded.
func (m *Manager) epochTransitionLoop(
	ctx context.Context,
	evtCh <-chan event.Event,
) {
	for {
		var evt event.Event
		var ok bool
		select {
		case <-ctx.Done():
			return
		case evt, ok = <-evtCh:
			if !ok {
				return
			}
		}
		// Drain any queued events, keeping only the latest.
		latest := evt
		skipped := false
	drain:
		for {
			select {
			case <-ctx.Done():
				return
			case newer, chOk := <-evtCh:
				if !chOk {
					return
				}
				latest = newer
				skipped = true
			default:
				break drain
			}
		}

		epochEvent, ok := latest.Data.(event.EpochTransitionEvent)
		if !ok {
			m.logger.Error(
				"invalid event data for epoch transition",
				"component", "snapshot",
			)
			continue
		}

		if skipped {
			// We skipped intermediate events
			skippedEvt, ok := evt.Data.(event.EpochTransitionEvent)
			if ok {
				m.logger.Info(
					"fast-forwarded past intermediate epoch transitions",
					"component", "snapshot",
					"from_epoch", skippedEvt.NewEpoch,
					"to_epoch", epochEvent.NewEpoch,
				)
			}
		}

		if err := m.handleEpochTransition(ctx, epochEvent); err != nil {
			if errors.Is(err, types.ErrNoEpochData) {
				m.logger.Debug(
					"skipping snapshot: epoch data not yet synced",
					"component", "snapshot",
					"epoch", epochEvent.NewEpoch,
				)
			} else {
				m.logger.Error(
					"failed to handle epoch transition",
					"component", "snapshot",
					"epoch", epochEvent.NewEpoch,
					"error", err,
				)
			}
		}
	}
}

// handleEpochTransition processes an epoch boundary event.
func (m *Manager) handleEpochTransition(
	ctx context.Context,
	evt event.EpochTransitionEvent,
) error {
	m.logger.Info(
		"handling epoch transition",
		"component", "snapshot",
		"previous_epoch", evt.PreviousEpoch,
		"new_epoch", evt.NewEpoch,
		"boundary_slot", evt.BoundarySlot,
		"snapshot_slot", evt.SnapshotSlot,
	)

	// 1. Capture new Mark snapshot (current stake distribution)
	if err := m.captureMarkSnapshot(ctx, evt); err != nil {
		return fmt.Errorf("capture mark snapshot: %w", err)
	}

	// 2. Rotate snapshots (Mark→Set→Go)
	m.rotateSnapshots(ctx, evt.NewEpoch)

	// 3. Cleanup old snapshots (keep last 3 epochs)
	if err := m.cleanupOldSnapshots(ctx, evt.NewEpoch); err != nil {
		return fmt.Errorf("cleanup old snapshots: %w", err)
	}

	m.logger.Info(
		"epoch transition complete",
		"component", "snapshot",
		"epoch", evt.NewEpoch,
	)

	return nil
}

// captureMarkSnapshot captures the stake distribution as a Mark snapshot.
func (m *Manager) captureMarkSnapshot(
	ctx context.Context,
	evt event.EpochTransitionEvent,
) error {
	calculator := NewCalculator(m.db)

	// Calculate stake distribution at the snapshot slot
	distribution, err := calculator.CalculateStakeDistribution(
		ctx,
		evt.SnapshotSlot,
	)
	if err != nil {
		return fmt.Errorf("calculate stake distribution: %w", err)
	}

	// Save as Mark snapshot for the new epoch
	if err := m.saveSnapshot(
		ctx,
		evt.NewEpoch,
		"mark",
		distribution,
		evt,
	); err != nil {
		return fmt.Errorf("save mark snapshot: %w", err)
	}

	m.logger.Info(
		"captured mark snapshot",
		"component", "snapshot",
		"epoch", evt.NewEpoch,
		"total_pools", len(distribution.PoolStakes),
		"total_stake", distribution.TotalStake,
	)

	return nil
}

// CaptureGenesisSnapshot captures the initial stake distribution from genesis
// as a mark snapshot for epoch 0. This ensures the "Go" snapshot is available
// at epoch 2 for leader election, matching the Cardano spec which uses the
// genesis stake distribution for the first two epochs.
func (m *Manager) CaptureGenesisSnapshot(ctx context.Context) error {
	calculator := NewCalculator(m.db)

	distribution, err := calculator.CalculateStakeDistribution(ctx, 0)
	if err != nil {
		return fmt.Errorf("calculate genesis distribution: %w", err)
	}

	if distribution.TotalPools == 0 {
		m.logger.Warn(
			"no genesis pools; leader election disabled"+
				" until pool stake is registered",
			"component", "snapshot",
		)
		return nil
	}

	m.logger.Info(
		"genesis stake distribution calculated",
		"component", "snapshot",
		"total_pools", distribution.TotalPools,
		"total_stake", distribution.TotalStake,
	)

	evt := event.EpochTransitionEvent{
		NewEpoch:     0,
		BoundarySlot: 0,
		SnapshotSlot: 0,
	}
	if err := m.saveSnapshot(ctx, 0, "mark", distribution, evt); err != nil {
		return fmt.Errorf("save genesis snapshot: %w", err)
	}

	m.logger.Info(
		"captured genesis snapshot",
		"component", "snapshot",
		"total_pools", distribution.TotalPools,
		"total_stake", distribution.TotalStake,
	)
	return nil
}
