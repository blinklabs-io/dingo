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
	cancel         context.CancelFunc
	subscriptionId event.EventSubscriberId
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

// Start begins listening for epoch transitions and capturing snapshots.
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return nil
	}

	// Guard against nil dependencies to avoid panics
	if m.db == nil {
		return errors.New("snapshot manager: nil database")
	}
	if m.eventBus == nil {
		return errors.New("snapshot manager: nil event bus")
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.running = true

	// Subscribe to epoch transitions
	m.subscriptionId = m.eventBus.SubscribeFunc(
		event.EpochTransitionEventType,
		func(evt event.Event) {
			epochEvent, ok := evt.Data.(event.EpochTransitionEvent)
			if !ok {
				m.logger.Error(
					"invalid event data for epoch transition",
					"component", "snapshot",
				)
				return
			}
			if err := m.handleEpochTransition(ctx, epochEvent); err != nil {
				m.logger.Error(
					"failed to handle epoch transition",
					"component", "snapshot",
					"epoch", epochEvent.NewEpoch,
					"error", err,
				)
			}
		},
	)

	m.logger.Info("snapshot manager started", "component", "snapshot")
	return nil
}

// Stop stops the snapshot manager.
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return nil
	}

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

	m.logger.Info("snapshot manager stopped", "component", "snapshot")
	return nil
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
