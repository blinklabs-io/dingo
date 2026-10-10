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

// Package plannerstats keeps SQLite planner statistics current while the node
// runs. A fresh SQLite metadata database has no sqlite_stat1, so the planner
// picks indexes by shape alone and can scan most of a large table for a query
// that should seek; this package runs the backend's incremental statistics
// maintenance before block processing starts and after each epoch rollover.
package plannerstats

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/dingo/event"
)

// shelleyProtocolMajor is the first protocol major version with an epoch
// nonce; Byron used 0 and 1.
const shelleyProtocolMajor = 2

// Manager runs incremental planner-statistics maintenance. The epoch trigger
// subscribes to event.EpochTransitionEventType, which the ledger publishes
// only after the rollover transaction has committed, so a run never executes
// inside that transaction. A run pauses SQLite writers for its duration, by
// design: the write pool has one connection and the statement holds it.
type Manager struct {
	updater  metadata.IncrementalPlannerStatsUpdater
	eventBus *event.EventBus
	logger   *slog.Logger

	// pending is a one-slot signal: while a run is in flight at most one
	// follow-up is remembered, because a run reads current state and so
	// covers every transition that arrived before it started.
	pending chan struct{}

	mu             sync.Mutex
	cancel         context.CancelFunc
	subscriptionID event.EventSubscriberId
	wg             sync.WaitGroup
}

// NewManager returns a Manager that runs updater. logger may be nil.
func NewManager(
	updater metadata.IncrementalPlannerStatsUpdater,
	eventBus *event.EventBus,
	logger *slog.Logger,
) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		updater:  updater,
		eventBus: eventBus,
		logger:   logger,
		pending:  make(chan struct{}, 1),
	}
}

// RunStartup runs one synchronous pass. Call it before block processing
// begins: a database that has never been analyzed otherwise spends its first
// multi-input transactions on the trap plans the statistics exist to avoid.
func (m *Manager) RunStartup(
	ctx context.Context,
) (types.PlannerStatsResult, error) {
	return m.run(ctx, types.PlannerStatsTriggerStartup)
}

// Start subscribes to epoch transitions and starts the worker. ctx is the
// parent of every run; canceling it interrupts one in flight.
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cancel != nil {
		return nil
	}
	if m.updater == nil {
		return errors.New("planner statistics manager: nil updater")
	}
	if m.eventBus == nil {
		return errors.New("planner statistics manager: nil event bus")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	childCtx, cancel := context.WithCancel(ctx)
	m.subscriptionID = m.eventBus.SubscribeFunc(
		event.EpochTransitionEventType,
		m.handleEpochTransition,
	)
	if m.subscriptionID == 0 {
		cancel()
		m.logger.Warn(
			"event bus not available, epoch planner statistics disabled",
			"component", "plannerstats",
		)
		return nil
	}
	m.cancel = cancel
	m.wg.Go(func() { m.worker(childCtx) })
	return nil
}

// Stop cancels any run in flight and waits for the worker to exit. SQLite
// interrupts a statement whose context is canceled, so this does not wait for
// a run to finish.
func (m *Manager) Stop() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	cancel := m.cancel
	subscriptionID := m.subscriptionID
	m.cancel = nil
	m.subscriptionID = 0
	m.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	m.eventBus.UnsubscribeAndWait(
		event.EpochTransitionEventType,
		subscriptionID,
	)
	m.wg.Wait()
	return nil
}

func (m *Manager) handleEpochTransition(evt event.Event) {
	epochEvent, ok := evt.Data.(event.EpochTransitionEvent)
	if !ok {
		m.logger.Error(
			"invalid event data for epoch transition",
			"component", "plannerstats",
		)
		return
	}
	// Near the tip the slot clock publishes a second transition for the same
	// boundary without a nonce; the ledger's own event is the one that
	// follows a committed rollover. Byron has no epoch nonce, so the ledger's
	// Byron rollovers carry none either and must not be mistaken for it.
	if epochEvent.EpochNonce == nil &&
		epochEvent.ProtocolVersion >= shelleyProtocolMajor {
		return
	}
	select {
	case m.pending <- struct{}{}:
	default:
	}
}

func (m *Manager) worker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.pending:
			_, _ = m.run(ctx, types.PlannerStatsTriggerEpoch)
		}
	}
}

func (m *Manager) run(
	ctx context.Context,
	trigger string,
) (types.PlannerStatsResult, error) {
	result, err := m.updater.OptimizePlannerStatsContext(ctx, trigger)
	switch {
	case err != nil && ctx.Err() != nil:
		m.logger.Debug(
			"planner statistics run interrupted",
			"component", "plannerstats",
			"trigger", trigger,
			"error", err,
		)
	case err != nil:
		m.logger.Error(
			"planner statistics run failed",
			"component", "plannerstats",
			"trigger", trigger,
			"error", err,
		)
	case !result.Supported:
	case result.Skipped:
		m.logger.Debug(
			"planner statistics run skipped",
			"component", "plannerstats",
			"trigger", trigger,
		)
	case result.Changed:
		m.logger.Info(
			"planner statistics optimized",
			"component", "plannerstats",
			"trigger", trigger,
			"duration", result.Duration,
			"changed", true,
			"stat1_rows", result.Stat1Rows,
		)
	default:
		m.logger.Debug(
			"planner statistics unchanged",
			"component", "plannerstats",
			"trigger", trigger,
			"duration", result.Duration,
			"stat1_rows", result.Stat1Rows,
		)
	}
	return result, err
}
