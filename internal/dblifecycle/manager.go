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

	// SubscribeFunc (not the raw channel Subscribe) so a panic anywhere in
	// handleEpochTransition's snapshot path — a multi-gigabyte badger
	// Backup/sqlite VACUUM INTO, cloud upload, or manifest write — is
	// caught by EventBus's own safeHandlerCall recover() and logged,
	// instead of propagating out of a hand-rolled loop's goroutine and
	// crashing the whole node.
	m.subscriptionId = m.eventBus.SubscribeFunc(
		event.EpochTransitionEventType,
		func(evt event.Event) {
			m.handleEpochTransitionEvent(childCtx, evt)
		},
	)
	if m.subscriptionId == 0 {
		m.logger.Warn(
			"event bus not available, automatic database snapshots disabled",
			"component", "dblifecycle",
		)
		m.running = false
		cancel()
		m.cancel = nil
		return nil
	}

	// SubscribeFunc's dispatch goroutine (owned by EventBus, invisible to
	// this package) only reacts to Unsubscribe, never to childCtx being
	// cancelled directly — unlike the old hand-rolled loop, which noticed
	// ctx.Done() itself. This goroutine preserves Start's existing
	// contract that cancelling the context passed to Start, without a
	// separate call to Stop, still cleans up (resets running/cancel and
	// unsubscribes) instead of leaving a stale subscription in place.
	// Guarded by "not stopping" the same way the old loop's wrapper was,
	// so a normal Stop() (which cancels childCtx itself) leaves this
	// cleanup to Stop() instead of racing it.
	m.loopWg.Go(func() {
		<-childCtx.Done()
		m.mu.Lock()
		if m.stopping {
			m.mu.Unlock()
			return
		}
		m.running = false
		if m.cancel != nil {
			m.cancel()
			m.cancel = nil
		}
		subId := m.subscriptionId
		m.subscriptionId = 0
		m.mu.Unlock()
		if subId != 0 {
			m.eventBus.UnsubscribeAndWait(
				event.EpochTransitionEventType,
				subId,
			)
		}
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
	subId := m.subscriptionId
	m.subscriptionId = 0
	m.running = false
	m.mu.Unlock()

	if subId != 0 {
		// UnsubscribeAndWait, not Unsubscribe: blocks until SubscribeFunc's
		// dispatch goroutine has fully exited (including finishing any
		// handleEpochTransitionEvent call already in flight), so Stop
		// doesn't return while a snapshot it just asked to stop is still
		// running.
		m.eventBus.UnsubscribeAndWait(
			event.EpochTransitionEventType,
			subId,
		)
	}

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

// handleEpochTransitionEvent is the SubscribeFunc handler for
// event.EpochTransitionEventType: it validates evt's payload type, then
// hands off to handleEpochTransition. Called synchronously from
// EventBus's own dispatch goroutine (see Start's SubscribeFunc call),
// under that goroutine's safeHandlerCall panic recovery.
func (m *Manager) handleEpochTransitionEvent(
	ctx context.Context,
	evt event.Event,
) {
	epochEvent, ok := evt.Data.(event.EpochTransitionEvent)
	if !ok {
		m.logger.Error(
			"invalid event data for epoch transition",
			"component", "dblifecycle",
		)
		return
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
	// Checked before attempting anything, not after a failure: SnapshotToCloud
	// writes destDir locally first and only then uploads to the cloud
	// destination, so a failure partway through (e.g. the local write
	// succeeding but the cloud upload failing) also leaves destDir
	// existing. Checking only after an error, as this used to, couldn't
	// tell that case apart from "this epoch's snapshot already exists
	// from an earlier successful run" (e.g. a redelivered epoch-
	// transition event) — it silently treated a genuine, undetected
	// cloud-upload failure as routine idempotency. Checking first means
	// any error from here on is always a real failure to report.
	if _, statErr := os.Stat(destDir); statErr == nil {
		m.logger.Debug(
			"automatic snapshot for epoch already exists, skipping",
			"component", "dblifecycle",
			"epoch", evt.NewEpoch,
			"dir", destDir,
		)
		return nil
	}
	_, err := lifecycle.SnapshotToCloud(
		ctx,
		m.db,
		destDir,
		lifecycle.TriggerEpochBoundary,
		version.GetVersionString(),
		m.cfg.SnapshotCloudDestination,
	)
	if err != nil {
		return fmt.Errorf("capture epoch-boundary snapshot: %w", err)
	}

	m.logger.Info(
		"captured automatic database snapshot",
		"component", "dblifecycle",
		"epoch", evt.NewEpoch,
		"dir", destDir,
	)

	if m.cfg.SnapshotRetention > 0 {
		m.pruneOldSnapshots(ctx)
	}
	return nil
}

// pruneOldSnapshots removes automatic snapshot directories beyond the
// configured retention count, oldest epoch first, along with each pruned
// epoch's mirrored cloud copy (if SnapshotCloudDestination is
// configured) — without this, retention only bounds local disk usage
// while every mirrored snapshot accumulates in object storage forever,
// since nothing else ever deletes a cloud mirror once
// SnapshotToCloud/UploadDir has written it. Failures are logged, not
// returned: pruning is best-effort and must never fail an otherwise
// successful snapshot.
func (m *Manager) pruneOldSnapshots(ctx context.Context) {
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
		epochName := fmt.Sprintf("%s%d", epochSnapshotDirPrefix, epoch)
		dir := filepath.Join(m.cfg.SnapshotDir, epochName)
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

		if m.cfg.SnapshotCloudDestination == "" {
			continue
		}
		// Best-effort, like the local removal above: logged, not
		// propagated, since a cloud-delete failure must never be
		// reported as a snapshot failure or retried by this same code
		// path (the local copy is already gone either way).
		cloudURI := lifecycle.JoinCloudURI(m.cfg.SnapshotCloudDestination, epochName)
		ok, err := lifecycle.DeleteCloudSnapshot(ctx, cloudURI)
		switch {
		case err != nil:
			m.logger.Warn(
				"failed to prune old automatic snapshot's cloud mirror",
				"component", "dblifecycle",
				"cloud_uri", cloudURI,
				"error", err,
			)
		case ok:
			m.logger.Info(
				"pruned old automatic database snapshot's cloud mirror",
				"component", "dblifecycle",
				"cloud_uri", cloudURI,
			)
		}
	}
}
