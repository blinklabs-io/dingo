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
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
	"github.com/prometheus/client_golang/prometheus"
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
	metrics        *managerMetrics
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

// SetPromRegistry enables snapshot manager metrics.
func (m *Manager) SetPromRegistry(reg prometheus.Registerer) {
	if reg == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.metrics == nil {
		m.metrics = initManagerMetrics(reg)
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
		m.loopWg.Go(func() {
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
		})
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

// epochTransitionLoop reads epoch transition events from the channel
// and processes each one. Every epoch transition must be handled so
// that stake snapshots exist for the full Mark/Set/Go rotation window.
// Skipping intermediate events would leave gaps that break leader
// election (pool_stake=0 for the missing "Go" epoch).
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

		epochEvent, ok := evt.Data.(event.EpochTransitionEvent)
		if !ok {
			m.logger.Error(
				"invalid event data for epoch transition",
				"component", "snapshot",
			)
			continue
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
	start := time.Now()
	calculator := NewCalculator(m.db)

	// Calculate stake distribution at the snapshot slot
	distribution, err := calculator.CalculateStakeDistribution(
		ctx,
		evt.SnapshotSlot,
	)
	if err != nil {
		if m.metrics != nil {
			m.metrics.captureFailureTotal.Inc()
			m.metrics.captureDurationSeconds.Observe(time.Since(start).Seconds())
		}
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
		if m.metrics != nil {
			m.metrics.captureFailureTotal.Inc()
			m.metrics.captureDurationSeconds.Observe(time.Since(start).Seconds())
		}
		return fmt.Errorf("save mark snapshot: %w", err)
	}

	if m.metrics != nil {
		m.metrics.captureDurationSeconds.Observe(time.Since(start).Seconds())
		m.metrics.captureSuccessTotal.Inc()
		m.metrics.capturePoolsTotal.Set(float64(len(distribution.PoolStakes)))
		m.metrics.captureTotalStakeLovelace.Set(float64(distribution.TotalStake))
		m.metrics.lastSuccessfulEpoch.Set(float64(evt.NewEpoch))
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

// CaptureGenesisSnapshot captures the initial stake distribution as mark
// snapshots. For a fresh sync this seeds epoch 0 so that leader election
// works at epoch 2. After a Mithril bootstrap the node starts at a much
// later epoch, so the method also seeds the current Mark/Set/Go window
// (epochs N, N-1, N-2) to ensure leader election can find its "Go"
// snapshot immediately.
func (m *Manager) CaptureGenesisSnapshot(ctx context.Context) error {
	start := time.Now()
	calculator := NewCalculator(m.db)
	successCount := uint64(0)
	lastSuccessfulEpoch := uint64(0)

	distribution, err := calculator.CalculateStakeDistribution(ctx, 0)
	if err != nil {
		if m.metrics != nil {
			m.metrics.captureFailureTotal.Inc()
			m.metrics.captureDurationSeconds.Observe(time.Since(start).Seconds())
		}
		return fmt.Errorf("calculate genesis distribution: %w", err)
	}

	// After Mithril import, slot 0 has no pool data. Fall back to
	// the latest epoch's start slot where imported pools are visible.
	// Also remember the latest epoch so we can seed the Mark/Set/Go
	// window below.
	var currentEpochId uint64
	if distribution.TotalPools == 0 {
		epochs, epErr := m.db.GetEpochs(nil)
		if epErr != nil {
			m.logger.Warn(
				"failed to load epochs for genesis stake fallback",
				"component", "snapshot",
				"error", epErr,
				"total_pools", distribution.TotalPools,
			)
		} else if len(epochs) > 0 {
			lastEpoch := epochs[len(epochs)-1]
			currentEpochId = lastEpoch.EpochId
			m.logger.Debug(
				"attempting genesis stake fallback from latest epoch",
				"component", "snapshot",
				"epoch", currentEpochId,
				"start_slot", lastEpoch.StartSlot,
				"total_pools", distribution.TotalPools,
			)
			dist2, err2 := calculator.CalculateStakeDistribution(
				ctx, lastEpoch.StartSlot,
			)
			if err2 != nil {
				m.logger.Warn(
					"failed to calculate fallback genesis stake distribution",
					"component", "snapshot",
					"error", err2,
					"start_slot", lastEpoch.StartSlot,
					"total_pools", distribution.TotalPools,
				)
			} else if dist2.TotalPools > 0 {
				distribution = dist2
			}
		}
	}

	if distribution.TotalPools == 0 {
		if m.metrics != nil {
			m.metrics.captureDurationSeconds.Observe(time.Since(start).Seconds())
		}
		m.logger.Info(
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

	// Always save epoch 0 for normal bootstrap (leader election at
	// epochs 0 and 1 uses genesis snapshot directly).
	evt := event.EpochTransitionEvent{
		NewEpoch:     0,
		BoundarySlot: 0,
		SnapshotSlot: 0,
	}
	if err := m.saveSnapshot(ctx, 0, "mark", distribution, evt); err != nil {
		if m.metrics != nil {
			m.metrics.captureFailureTotal.Inc()
			m.metrics.captureDurationSeconds.Observe(time.Since(start).Seconds())
		}
		return fmt.Errorf("save genesis snapshot: %w", err)
	}
	successCount++

	// After Mithril bootstrap: seed the Mark/Set/Go window for the
	// current epoch so leader election works immediately. Leader
	// election for epoch N queries the "mark" snapshot at epoch N-2
	// (the "Go" snapshot), so we need mark snapshots at N, N-1, and
	// N-2. Without these the pool shows pool_stake=0 and cannot
	// forge until two epoch transitions pass.
	if currentEpochId > 0 {
		for offset := uint64(0); offset <= 2 && offset <= currentEpochId; offset++ {
			seedEpoch := currentEpochId - offset
			if seedEpoch == 0 {
				continue // already saved above
			}
			seedEvt := event.EpochTransitionEvent{
				NewEpoch: seedEpoch,
			}
			if err := m.saveSnapshot(
				ctx, seedEpoch, "mark", distribution, seedEvt,
			); err != nil {
				if m.metrics != nil {
					m.metrics.captureFailureTotal.Inc()
					m.metrics.captureDurationSeconds.Observe(time.Since(start).Seconds())
				}
				return fmt.Errorf(
					"save bootstrap snapshot for epoch %d: %w",
					seedEpoch, err,
				)
			}
			successCount++
			if seedEpoch > lastSuccessfulEpoch {
				lastSuccessfulEpoch = seedEpoch
			}
			m.logger.Info(
				"seeded post-Mithril snapshot",
				"component", "snapshot",
				"epoch", seedEpoch,
				"total_pools", distribution.TotalPools,
				"total_stake", distribution.TotalStake,
			)
		}
	}

	m.logger.Info(
		"captured genesis snapshot",
		"component", "snapshot",
		"total_pools", distribution.TotalPools,
		"total_stake", distribution.TotalStake,
	)
	if m.metrics != nil {
		m.metrics.captureDurationSeconds.Observe(time.Since(start).Seconds())
		m.metrics.captureSuccessTotal.Add(float64(successCount))
		m.metrics.capturePoolsTotal.Set(float64(distribution.TotalPools))
		m.metrics.captureTotalStakeLovelace.Set(float64(distribution.TotalStake))
		m.metrics.lastSuccessfulEpoch.Set(float64(lastSuccessfulEpoch))
	}
	return nil
}
