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

package dblifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/lifecycle"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/internal/config"
	"github.com/blinklabs-io/dingo/internal/version"
)

// epochSnapshotDirPrefix names automatic snapshot directories
// deterministically by epoch (epoch-<N>), which gives cross-restart
// idempotency for free: if the manager (or the whole node) restarts and
// the same epoch's transition event is redelivered, Snapshot's own
// refusal to overwrite an existing directory makes the repeat attempt a
// harmless no-op rather than requiring separately persisted state.
const epochSnapshotDirPrefix = "epoch-"

// Manager captures automatic database snapshots at epoch boundaries. It
// subscribes to event.EpochTransitionEventType on the EventBus — the same
// async, decoupled pattern ledger/snapshot.Manager uses for stake/reward
// snapshots — rather than the synchronous in-transaction hook: a
// multi-gigabyte database backup must never run inside the ledger's write
// transaction. Because Badger's Backup and SQLite's VACUUM INTO are both
// non-blocking for concurrent writers, this needs no node quiesce.
type Manager struct {
	db       *database.Database
	eventBus *event.EventBus
	cfg      config.DatabaseLifecycleConfig
	logger   *slog.Logger

	mu             sync.Mutex
	running        bool
	stopping       bool
	cancel         context.CancelFunc
	subscriptionId event.EventSubscriberId
	loopWg         sync.WaitGroup
}

// NewManager creates a new automatic-snapshot manager. db and eventBus
// must not be nil once Start is called.
func NewManager(
	db *database.Database,
	eventBus *event.EventBus,
	cfg config.DatabaseLifecycleConfig,
	logger *slog.Logger,
) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		db:       db,
		eventBus: eventBus,
		cfg:      cfg,
		logger:   logger,
	}
}

// Start begins listening for epoch transitions and capturing automatic
// snapshots, if enabled. The provided context is used as the parent for
// the manager's internal context; cancelling it stops the manager.
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return nil
	}
	if m.stopping {
		return errors.New(
			"database lifecycle manager: Stop in progress, cannot Start",
		)
	}
	if !m.cfg.SnapshotEnabled {
		return nil
	}
	if ctx == nil {
		return errors.New("database lifecycle manager: nil context")
	}
	if m.db == nil {
		return errors.New("database lifecycle manager: nil database")
	}
	if m.eventBus == nil {
		return errors.New("database lifecycle manager: nil event bus")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf(
			"database lifecycle manager: parent context already done: %w",
			err,
		)
	}

	childCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	m.running = true

	var evtCh <-chan event.Event
	m.subscriptionId, evtCh = m.eventBus.Subscribe(
		event.EpochTransitionEventType,
	)
	if evtCh == nil {
		m.logger.Warn(
			"event bus not available, automatic database snapshots disabled",
			"component", "dblifecycle",
		)
		m.running = false
		m.cancel = nil
		return nil
	}

	m.loopWg.Go(func() {
		m.epochTransitionLoop(childCtx, evtCh)
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

	m.logger.Info(
		"database lifecycle manager started",
		"component", "dblifecycle",
		"snapshot_dir", m.cfg.SnapshotDir,
		"every_n_epochs", m.cfg.SnapshotEveryNEpochs,
	)
	return nil
}

// Stop stops the manager.
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

	m.loopWg.Wait()

	m.mu.Lock()
	m.stopping = false
	m.cancel = nil
	m.mu.Unlock()

	m.logger.Info(
		"database lifecycle manager stopped",
		"component", "dblifecycle",
	)
	return nil
}

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
				"component", "dblifecycle",
			)
			continue
		}
		if err := m.handleEpochTransition(ctx, epochEvent); err != nil {
			m.logger.Error(
				"automatic database snapshot failed",
				"component", "dblifecycle",
				"epoch", epochEvent.NewEpoch,
				"error", err,
			)
		}
	}
}

func (m *Manager) handleEpochTransition(
	ctx context.Context,
	evt event.EpochTransitionEvent,
) error {
	everyN := m.cfg.SnapshotEveryNEpochs
	if everyN <= 0 {
		everyN = 1
	}
	if evt.NewEpoch%uint64(everyN) != 0 {
		return nil
	}

	destDir := filepath.Join(
		m.cfg.SnapshotDir,
		fmt.Sprintf("%s%d", epochSnapshotDirPrefix, evt.NewEpoch),
	)
	_, err := lifecycle.SnapshotToCloud(
		ctx,
		m.db,
		destDir,
		lifecycle.TriggerEpochBoundary,
		version.GetVersionString(),
		m.cfg.SnapshotCloudDestination,
	)
	if err != nil {
		if _, statErr := os.Stat(destDir); statErr == nil {
			m.logger.Debug(
				"automatic snapshot for epoch already exists, skipping",
				"component", "dblifecycle",
				"epoch", evt.NewEpoch,
				"dir", destDir,
			)
			return nil
		}
		return fmt.Errorf("capture epoch-boundary snapshot: %w", err)
	}

	m.logger.Info(
		"captured automatic database snapshot",
		"component", "dblifecycle",
		"epoch", evt.NewEpoch,
		"dir", destDir,
	)

	if m.cfg.SnapshotRetention > 0 {
		m.pruneOldSnapshots()
	}
	return nil
}

// pruneOldSnapshots removes automatic snapshot directories beyond the
// configured retention count, oldest epoch first. Failures are logged,
// not returned: pruning is best-effort and must never fail an otherwise
// successful snapshot.
func (m *Manager) pruneOldSnapshots() {
	entries, err := os.ReadDir(m.cfg.SnapshotDir)
	if err != nil {
		m.logger.Warn(
			"failed to list snapshot directory for retention pruning",
			"component", "dblifecycle",
			"dir", m.cfg.SnapshotDir,
			"error", err,
		)
		return
	}

	var epochs []uint64
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		numStr, ok := strings.CutPrefix(entry.Name(), epochSnapshotDirPrefix)
		if !ok {
			continue
		}
		epoch, err := strconv.ParseUint(numStr, 10, 64)
		if err != nil {
			continue
		}
		epochs = append(epochs, epoch)
	}
	if len(epochs) <= m.cfg.SnapshotRetention {
		return
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i] < epochs[j] })

	toRemove := epochs[:len(epochs)-m.cfg.SnapshotRetention]
	for _, epoch := range toRemove {
		dir := filepath.Join(
			m.cfg.SnapshotDir,
			fmt.Sprintf("%s%d", epochSnapshotDirPrefix, epoch),
		)
		if err := os.RemoveAll(dir); err != nil {
			m.logger.Warn(
				"failed to prune old automatic snapshot",
				"component", "dblifecycle",
				"dir", dir,
				"error", err,
			)
			continue
		}
		m.logger.Info(
			"pruned old automatic database snapshot",
			"component", "dblifecycle",
			"dir", dir,
		)
	}
}
