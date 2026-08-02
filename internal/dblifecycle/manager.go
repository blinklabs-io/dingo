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
	"slices"
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
	db                  *database.Database
	eventBus            *event.EventBus
	cfg                 config.DatabaseLifecycleConfig
	blobPluginName      string
	metadataPluginName  string
	destinationRegistry *lifecycle.DestinationRegistry
	logger              *slog.Logger

	mu             sync.Mutex
	running        bool
	stopping       bool
	cancel         context.CancelFunc
	subscriptionId event.EventSubscriberId
	loopWg         sync.WaitGroup

	// retryMu serializes retryUnmirroredSnapshots so the startup scan
	// (launched from Start) and an epoch transition's opportunistic scan
	// (from handleEpochTransition) never race each other into uploading
	// the same local-only snapshot to the cloud twice concurrently.
	retryMu sync.Mutex
}

// NewManager creates a new automatic-snapshot manager. db and eventBus
// must not be nil once Start is called. blobPluginName/metadataPluginName
// are recorded in every automatic snapshot's manifest (the running
// database no longer tracks which provider names resolved its stores).
// destinationRegistry supplies the cloud destination schemes (s3, gcs)
// available for cfg.SnapshotCloudDestination — composition code owns
// constructing it; nil is valid when no cloud destination is configured.
func NewManager(
	db *database.Database,
	eventBus *event.EventBus,
	cfg config.DatabaseLifecycleConfig,
	blobPluginName string,
	metadataPluginName string,
	destinationRegistry *lifecycle.DestinationRegistry,
	logger *slog.Logger,
) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		db:                  db,
		eventBus:            eventBus,
		cfg:                 cfg,
		blobPluginName:      blobPluginName,
		metadataPluginName:  metadataPluginName,
		destinationRegistry: destinationRegistry,
		logger:              logger,
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
	// separate call to Stop, still cleans up (resets running and
	// unsubscribes) instead of leaving a stale subscription in place.
	//
	// This goroutine is the SOLE owner of that cleanup, whichever of the
	// two ways childCtx ends up cancelled: Stop() cancelling it directly,
	// or the parent ctx passed to Start being cancelled externally.
	// Unlike an earlier version that had Stop() race this goroutine to
	// decide which of them was responsible for unsubscribing (guarded by
	// "not stopping"), a single owner is what lets Stop() reliably wait
	// for the actual cleanup via loopWg.Wait() below instead of via its
	// own separate, potentially-redundant copy of the same logic: with
	// two competing paths, Stop() could find m.running already false
	// (because this goroutine's path won the race after an external ctx
	// cancellation) and return immediately, while this goroutine was
	// still blocked inside UnsubscribeAndWait waiting for an in-flight
	// snapshot handler call to finish — reporting "stopped" to Stop's
	// caller before an in-flight snapshot handler had actually exited.
	m.loopWg.Go(func() {
		<-childCtx.Done()
		m.mu.Lock()
		m.running = false
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

	// A normal restart never redelivers an old epoch's own transition
	// event, so without this, a snapshot that was written locally but
	// never mirrored (a transient upload failure) before the last
	// shutdown would sit unmirrored until retention deleted the only copy
	// of it that ever existed. Runs in the background (tracked by loopWg,
	// same as the subscription-cleanup goroutine above, so Stop still
	// waits for it) since it may need to re-upload a multi-gigabyte
	// snapshot and must not delay Start's return.
	m.loopWg.Go(func() {
		m.retryUnmirroredSnapshots(childCtx)
	})

	// A missing per-node prefix is not fatal -- a single node, or several
	// nodes each already pointed at distinct SnapshotCloudDestination
	// values, need no prefix at all -- but every automatic snapshot's
	// remote key is the deterministic epoch-<N> (see destDir in
	// handleEpochTransition), identical across every node. Two nodes
	// sharing the same SnapshotCloudDestination with no distinguishing
	// prefix would silently race to upload to the same remote path at
	// every epoch boundary, so this is worth flagging even though it
	// can't be ruled out as a deliberate, correct single-node setup.
	if m.cfg.SnapshotCloudDestination != "" &&
		m.cfg.SnapshotCloudDestinationPrefix == "" {
		m.logger.Warn(
			"databaseLifecycle.snapshotCloudDestination is configured "+
				"without snapshotCloudDestinationPrefix: if another node "+
				"shares this same cloud destination, their automatic "+
				"epoch-boundary snapshots will collide at the same remote "+
				"key and can corrupt each other's uploads; set a distinct "+
				"snapshotCloudDestinationPrefix per node sharing a "+
				"destination",
			"component", "dblifecycle",
			"cloud_destination", m.cfg.SnapshotCloudDestination,
		)
	}

	m.logger.Info(
		"database lifecycle manager started",
		"component", "dblifecycle",
		"snapshot_dir", m.cfg.SnapshotDir,
		"every_n_epochs", m.cfg.SnapshotEveryNEpochs,
	)
	return nil
}

// effectiveCloudDestination returns the cloud destination automatic
// snapshots are actually mirrored to, retried against, and pruned from:
// cfg.SnapshotCloudDestination with cfg.SnapshotCloudDestinationPrefix
// appended as an additional path segment, if configured. See
// SnapshotCloudDestinationPrefix's doc comment for why this exists --
// without it, two nodes sharing one SnapshotCloudDestination would
// collide at the same deterministic epoch-<N> remote key. Every call site
// that mirrors to, checks mirror status against, or deletes from the
// configured cloud destination must go through this rather than reading
// m.cfg.SnapshotCloudDestination directly, so all of them agree on the
// same effective location.
func (m *Manager) effectiveCloudDestination() string {
	if m.cfg.SnapshotCloudDestination == "" ||
		m.cfg.SnapshotCloudDestinationPrefix == "" {
		return m.cfg.SnapshotCloudDestination
	}
	return lifecycle.JoinCloudURI(
		m.cfg.SnapshotCloudDestination, m.cfg.SnapshotCloudDestinationPrefix,
	)
}

// Stop stops the manager, waiting for any in-flight snapshot handler call
// to finish before returning — including when the context passed to
// Start was already cancelled externally (in which case Start's own
// cleanup goroutine may already be tearing things down concurrently; Stop
// still reliably waits for that same goroutine via loopWg, rather than
// racing it to separately decide the manager is already stopped and
// returning early — see the comment on that goroutine in Start for why
// two competing cleanup paths used to let Stop return before an in-flight
// handler call had actually exited).
func (m *Manager) Stop() error {
	m.mu.Lock()
	if m.cancel == nil {
		// Never started, or a previous Stop (or this same external-
		// cancellation cleanup, already fully finished) already cleared
		// it: nothing left to wait for.
		m.mu.Unlock()
		return nil
	}
	m.stopping = true
	m.cancel()
	m.cancel = nil
	m.mu.Unlock()

	m.loopWg.Wait()

	m.mu.Lock()
	m.stopping = false
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
	// Give every epoch transition -- not just a redelivery of the exact
	// epoch that failed to mirror -- a chance to heal an older epoch's
	// snapshot that was written locally but never made it to the cloud.
	// Without this, advancing past the failed epoch (the normal case)
	// never retries it: the old epoch's own transition event is gone for
	// good once a newer epoch has begun.
	m.retryUnmirroredSnapshots(ctx)

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
		// destDir existing is NOT by itself proof this epoch is fully
		// done: a cloud destination is configured separately from the
		// local write, so a previous attempt could have written destDir
		// successfully and then failed (or never even tried) to mirror
		// it to the cloud. Treating that local-only partial success as
		// "already done" would permanently skip the cloud mirror for
		// this epoch -- and, combined with retention pruning the local
		// copy once enough newer epochs accumulate, silently lose the
		// only copy of this snapshot ever having existed. So when a
		// cloud destination is configured, only skip if this directory
		// is actually marked as mirrored; otherwise retry just the
		// upload from the already-valid local copy, not the whole
		// snapshot.
		cloudDest := m.effectiveCloudDestination()
		if cloudDest == "" ||
			lifecycle.IsCloudMirroredTo(destDir, cloudDest) {
			m.logger.Debug(
				"automatic snapshot for epoch already exists, skipping",
				"component", "dblifecycle",
				"epoch", evt.NewEpoch,
				"dir", destDir,
			)
			return nil
		}
		m.logger.Warn(
			"automatic snapshot for epoch exists locally but was never mirrored to the cloud, retrying upload",
			"component", "dblifecycle",
			"epoch", evt.NewEpoch,
			"dir", destDir,
		)
		if err := lifecycle.MirrorToCloud(
			ctx, m.destinationRegistry, destDir, cloudDest,
		); err != nil {
			return fmt.Errorf(
				"retry epoch-boundary snapshot cloud mirror: %w", err,
			)
		}
		m.logger.Info(
			"mirrored previously local-only automatic database snapshot to the cloud",
			"component", "dblifecycle",
			"epoch", evt.NewEpoch,
			"dir", destDir,
		)
		if m.cfg.SnapshotRetention > 0 {
			m.pruneOldSnapshots(ctx)
		}
		return nil
	}
	_, err := lifecycle.SnapshotToCloud(
		ctx,
		m.destinationRegistry,
		m.db,
		destDir,
		lifecycle.TriggerEpochBoundary,
		version.GetVersionString(),
		m.blobPluginName,
		m.metadataPluginName,
		m.effectiveCloudDestination(),
		"",
		"",
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

// retryUnmirroredSnapshots scans cfg.SnapshotDir for existing epoch-*
// snapshot directories that are not mirrored to the currently configured
// SnapshotCloudDestination, and retries the upload for each one. Called
// both from Start (so a restart heals a snapshot stranded local-only by
// the last run, without waiting for another epoch transition) and from
// handleEpochTransition (so a long-running node heals it as soon as any
// later epoch boundary occurs, without needing the failed epoch's own
// event redelivered). Uses IsCloudMirroredTo, not IsCloudMirrored, so a
// marker left over from a since-reconfigured cloud destination is treated
// as still needing an upload to the destination actually configured now.
// Best-effort like pruneOldSnapshots: failures are logged, never returned,
// since this is opportunistic healing on top of whatever handling already
// happened for the snapshot that triggered this call.
func (m *Manager) retryUnmirroredSnapshots(ctx context.Context) {
	cloudDest := m.effectiveCloudDestination()
	if cloudDest == "" {
		return
	}
	m.retryMu.Lock()
	defer m.retryMu.Unlock()

	entries, err := os.ReadDir(m.cfg.SnapshotDir)
	if err != nil {
		// Most commonly: no snapshot has ever been captured yet, so
		// SnapshotDir doesn't exist. Nothing to retry either way.
		return
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		if !entry.IsDir() ||
			!strings.HasPrefix(entry.Name(), epochSnapshotDirPrefix) {
			continue
		}
		dir := filepath.Join(m.cfg.SnapshotDir, entry.Name())
		if lifecycle.IsCloudMirroredTo(dir, cloudDest) {
			continue
		}
		m.logger.Warn(
			"found an automatic database snapshot not mirrored to the "+
				"currently configured cloud destination, retrying upload",
			"component", "dblifecycle",
			"dir", dir,
		)
		if err := m.retryMirrorToCloud(ctx, dir); err != nil {
			m.logger.Warn(
				"retrying cloud mirror for a previously unmirrored "+
					"automatic database snapshot failed, will retry again later",
				"component", "dblifecycle",
				"dir", dir,
				"error", err,
			)
			continue
		}
		m.logger.Info(
			"mirrored previously local-only automatic database snapshot to the cloud",
			"component", "dblifecycle",
			"dir", dir,
		)
	}
}

// retryMirrorToCloud calls lifecycle.MirrorToCloud with its own panic
// recovery, converting a panic into an ordinary error. Unlike the direct
// call in handleEpochTransition's own-epoch retry path (which relies on
// EventBus's safeHandlerCall to catch a panic from a genuinely broken
// destination plugin, exactly the scenario TestManagerSurvivesHandlerPanic
// exercises), retryUnmirroredSnapshots iterates every unmirrored snapshot
// dir in one call: an unrecovered panic on one older, already-broken
// snapshot would otherwise abort the loop before later directories are
// even considered, and -- since this whole scan also runs synchronously
// at the top of handleEpochTransition -- would prevent the current epoch's
// own snapshot from ever running, for every future epoch, for as long as
// that one old snapshot's cloud destination keeps panicking.
func (m *Manager) retryMirrorToCloud(ctx context.Context, dir string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic retrying cloud mirror: %v", r)
		}
	}()
	return lifecycle.MirrorToCloud(
		ctx, m.destinationRegistry, dir, m.effectiveCloudDestination(),
	)
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
	slices.Sort(epochs)

	cloudDest := m.effectiveCloudDestination()
	toRemove := epochs[:len(epochs)-m.cfg.SnapshotRetention]
	for _, epoch := range toRemove {
		epochName := fmt.Sprintf("%s%d", epochSnapshotDirPrefix, epoch)
		dir := filepath.Join(m.cfg.SnapshotDir, epochName)

		// A cloud destination is configured but this directory was never
		// actually confirmed mirrored to it (no valid .cloud-mirrored
		// marker for the CURRENTLY configured destination — see
		// IsCloudMirroredTo): DeleteCloudSnapshot below can't tell "deleted
		// a real remote copy" apart from "there was nothing there because
		// it was never mirrored in the first place" (both a real object
		// deletion and a no-op against an empty/nonexistent remote prefix
		// return ok=true, err=nil). Removing the local directory in that
		// case would destroy the only surviving copy of a snapshot that
		// was never actually backed up to the cloud, with nothing left to
		// retry from. So skip pruning this one entirely -- neither
		// deleting a (nonexistent) cloud copy nor the local directory --
		// and leave it for retryUnmirroredSnapshots to find and mirror on
		// a later pass, after which a later pruning pass can finish the
		// job. This deliberately means retention no longer strictly bounds
		// local disk usage for a snapshot that has never been
		// successfully mirrored: that's the safer tradeoff, since the
		// alternative is silently losing the snapshot's only copy.
		if cloudDest != "" && !lifecycle.IsCloudMirroredTo(dir, cloudDest) {
			m.logger.Warn(
				"old automatic snapshot beyond retention was never "+
					"confirmed mirrored to the configured cloud "+
					"destination, keeping local copy so a later retry can "+
					"still mirror (and then prune) it",
				"component", "dblifecycle",
				"dir", dir,
			)
			continue
		}

		// Cloud deletion must happen before the local directory is
		// removed, not after: the next pruning pass rediscovers retry
		// candidates by scanning this local SnapshotDir for epoch-*
		// directories, so removing dir first and then failing to delete
		// its cloud mirror would permanently orphan that cloud copy —
		// nothing would ever select it for a retry again, and retention
		// would no longer bound cloud storage at all.
		if cloudDest != "" {
			cloudURI := lifecycle.JoinCloudURI(cloudDest, epochName)
			ok, err := lifecycle.DeleteCloudSnapshot(ctx, m.destinationRegistry, cloudURI)
			if err != nil {
				m.logger.Warn(
					"failed to prune old automatic snapshot's cloud mirror, "+
						"keeping local copy so a later pruning pass can retry",
					"component", "dblifecycle",
					"cloud_uri", cloudURI,
					"error", err,
				)
				continue
			}
			if ok {
				m.logger.Info(
					"pruned old automatic database snapshot's cloud mirror",
					"component", "dblifecycle",
					"cloud_uri", cloudURI,
				)
			}
		}

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
