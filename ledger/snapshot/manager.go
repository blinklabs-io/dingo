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
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
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

	// delegatorInactivityEnabled is the CIP-0163 reward-account inactivity
	// consensus gate, mirrored from LedgerStateConfig at node/load construction.
	// When true, mark-snapshot capture excludes accounts expired before the
	// snapshot epoch from leader-election stake, the reward basis, and SPO vote
	// power. It is consensus-affecting, so every node on a network must agree.
	delegatorInactivityEnabled bool
	delegatorInactivityPeriod  uint64
	configurationLocked        bool

	mu             sync.RWMutex
	running        bool
	stopping       bool
	cancel         context.CancelFunc
	subscriptionId event.EventSubscriberId
	loopWg         sync.WaitGroup
	metrics        *managerMetrics

	// retentionGuard, when set, runs the pool-stake snapshot prune under the
	// deferred-header lock so the retention-floor selection and the prune are
	// atomic with respect to deferred-header admission (issue #3727 race).
	// cleanupOldSnapshots passes its default currentEpoch-3 boundary and a
	// prune closure that deletes+commits pool snapshots below the boundary the
	// guard hands back (lowered to keep snapshots a queued/deferred header
	// still needs). A nil guard (the default) preserves the original pruning
	// behaviour exactly. Guarded by mu; wired by the node to
	// LedgerState.PrunePoolSnapshotsWithRetentionFloor.
	retentionGuard PoolSnapshotRetentionGuard

	// pendingBoundary holds the stake distribution computed at the SNAP point of
	// an in-flight epoch rollover, waiting for the same rollover transaction to
	// persist it once the new epoch row (nonce, boundary slot, protocol version)
	// exists. See ComputeEpochBoundarySnapshot.
	pendingBoundary *pendingBoundarySnapshot
}

// pendingBoundarySnapshot is a SNAP-point stake distribution plus the exact
// boundary identity it was computed for. Persisting it requires an exact match
// on that identity, so a distribution left behind by an aborted rollover can
// never be attached to a different boundary.
type pendingBoundarySnapshot struct {
	distribution *StakeDistribution
	// txn binds the read half to the exact rollover transaction that produced
	// it. A transaction can roll back after ComputeEpochBoundarySnapshot and
	// before CaptureEpochBoundarySnapshot; allowing a later retry with the same
	// boundary identity to consume that stale distribution would persist state
	// from the abandoned transaction instead of recomputing SNAP.
	txn          *database.Txn
	newEpoch     uint64
	boundarySlot uint64
	snapshotSlot uint64
	expiryEpoch  uint64
}

func (m *Manager) stashBoundaryDistribution(
	pending *pendingBoundarySnapshot,
) {
	m.mu.Lock()
	m.pendingBoundary = pending
	m.mu.Unlock()
}

// takeBoundaryDistribution returns the SNAP-point distribution computed for
// exactly this boundary, clearing it. Any stashed distribution is dropped even
// when it does not match, because a non-matching one can only be the residue of
// a rollover that never reached its persist phase.
func (m *Manager) takeBoundaryDistribution(
	txn *database.Txn,
	evt event.EpochTransitionEvent,
	expiryEpoch uint64,
) *StakeDistribution {
	m.mu.Lock()
	pending := m.pendingBoundary
	m.pendingBoundary = nil
	m.mu.Unlock()
	if pending == nil || pending.txn != txn {
		return nil
	}
	if pending.newEpoch != evt.NewEpoch ||
		pending.boundarySlot != evt.BoundarySlot ||
		pending.snapshotSlot != evt.SnapshotSlot ||
		pending.expiryEpoch != expiryEpoch {
		return nil
	}
	return pending.distribution
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

// SetDelegatorInactivity mirrors the CIP-0163 reward-account inactivity gate
// and inactivity window from LedgerStateConfig into the snapshot manager. It
// must be called before snapshot capture begins (i.e. before
// CaptureGenesisSnapshot/Start), matching how node.go and load.go configure the
// ledger. Once capture can begin, the configuration is permanently locked.
// Default (unset) is gate off, which keeps snapshot capture byte-identical to
// the pre-CIP behavior.
func (m *Manager) SetDelegatorInactivity(
	enabled bool,
	inactivityPeriod uint64,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.configurationLocked {
		return errors.New(
			"snapshot manager: delegator inactivity configuration is locked",
		)
	}
	if enabled && (inactivityPeriod == 0 || inactivityPeriod > 10_000) {
		return fmt.Errorf(
			"snapshot manager: delegator inactivity period %d is outside [1, 10000]",
			inactivityPeriod,
		)
	}
	m.delegatorInactivityEnabled = enabled
	if enabled {
		m.delegatorInactivityPeriod = inactivityPeriod
	} else {
		m.delegatorInactivityPeriod = 0
	}
	return nil
}

// DelegatorInactivityConfig returns the CIP-0163 reward-account inactivity
// gate and window currently configured on this manager, as last set by
// SetDelegatorInactivity (mirrors LedgerState.DelegatorInactivityConfig's
// identical pattern) — used to verify a live restore/truncate's rebuilt
// snapshot manager actually picked up the operator's configured value.
func (m *Manager) DelegatorInactivityConfig() (enabled bool, period uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.delegatorInactivityEnabled, m.delegatorInactivityPeriod
}

// lockConfiguration freezes consensus-affecting options before snapshot
// capture can observe them. The lock is permanent for the manager's lifetime:
// stopping and restarting must not permit snapshots produced by one manager to
// use different consensus rules.
func (m *Manager) lockConfiguration() {
	m.mu.Lock()
	m.configurationLocked = true
	m.mu.Unlock()
}

// expiryEpoch returns the CIP-0163 gate argument for a snapshot being computed
// for snapshotEpoch: 0 when the gate is off (query byte-identical to pre-CIP),
// otherwise snapshotEpoch, so the aggregation excludes accounts whose
// expiration_epoch is nonzero and strictly less than the snapshot epoch.
func (m *Manager) expiryEpoch(snapshotEpoch uint64) uint64 {
	m.mu.RLock()
	enabled := m.delegatorInactivityEnabled
	m.mu.RUnlock()
	if !enabled {
		return 0
	}
	return snapshotEpoch
}

func (m *Manager) inactivityPeriod() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.delegatorInactivityEnabled {
		return 0
	}
	return m.delegatorInactivityPeriod
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

// PoolSnapshotRetentionGuard runs the caller's pool-snapshot prune with the
// deferred-header set held stable, returning the boundary to prune below.
// defaultBefore is cleanup's default currentEpoch-3 pool boundary; the guard
// lowers it to the retention floor a queued/deferred header still requires
// (or to 0 = retain everything while any deferred slot is unmappable), then
// clamps it UP to minBefore, a hard backstop bounding how many historical
// epochs the pin can ever hold. All of this — plus eviction of deferred
// headers the apply cursor has passed — happens under one lock so admission
// cannot interleave (issue #3727). prune must delete AND commit before
// returning.
type PoolSnapshotRetentionGuard func(
	defaultBefore uint64,
	minBefore uint64,
	prune func(before uint64) error,
) error

// SetPoolSnapshotRetentionGuard installs the guard cleanupOldSnapshots uses to
// prune pool snapshots atomically with the deferred-header retention floor, so
// a snapshot a queued/deferred header still needs is retained beyond the
// default currentEpoch-3 window until the header resolves (issue #3727). Pass
// nil to clear it. It should be set before Start; a nil guard (the default)
// preserves the original pruning behaviour exactly.
func (m *Manager) SetPoolSnapshotRetentionGuard(g PoolSnapshotRetentionGuard) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.retentionGuard = g
}

// poolSnapshotRetentionGuard returns the installed guard, if any. Read under mu
// so a guard installed via SetPoolSnapshotRetentionGuard races safely with the
// manager's epoch-transition goroutine.
func (m *Manager) poolSnapshotRetentionGuard() PoolSnapshotRetentionGuard {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.retentionGuard
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
		return fmt.Errorf(
			"snapshot manager: parent context already done: %w",
			err,
		)
	}

	m.configurationLocked = true
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
	//
	// The pre-check runs inside a short read-only transaction rather than
	// passing a nil transaction directly. That keeps the metadata connection
	// reserved for the complete read and avoids competing with other cold
	// query preparations while the read pool is busy. The transaction still
	// sees a fresh WAL snapshot when it begins.
	preCheckTxn := m.db.Transaction(false)
	exists, err := m.authoritativeMarkRewardSnapshotExists(
		evt,
		preCheckTxn.Metadata(),
	)
	_ = preCheckTxn.Commit()
	if err != nil {
		return fmt.Errorf("check existing mark snapshot: %w", err)
	}
	if exists {
		m.logger.Debug(
			"mark snapshot already captured",
			"component", "snapshot",
			"epoch", evt.NewEpoch,
		)
	} else if err := m.captureMarkSnapshot(ctx, evt); err != nil {
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

// authoritativeMarkRewardSnapshotExists reports whether an authoritative
// (reward_snapshot.authoritative = true) mark reward snapshot for evt.NewEpoch
// already reflects the exact boundary described by evt (matching BoundarySlot
// and, when both sides have one, EpochNonce). Provisional fallback rows return
// false so a later block-based fallback can still refresh them.
//
// This is a best-effort pre-check (handleEpochTransition skips the fallback
// capture when it returns true). It does not by itself close the
// fallback-vs-authoritative race — that is handled inside saveSnapshotInTxn by
// ClaimFallbackRewardSnapshot, which takes the reward_snapshot row lock. txn
// scopes the read: nil reads a fresh, out-of-transaction view; passing an
// open write transaction's metadata handle reads within that transaction.
func (m *Manager) authoritativeMarkRewardSnapshotExists(
	evt event.EpochTransitionEvent,
	txn types.Txn,
) (bool, error) {
	snapshot, err := m.db.Metadata().
		GetRewardSnapshot(evt.NewEpoch, "mark", txn)
	if err != nil {
		return false, err
	}
	if snapshot == nil {
		return false, nil
	}
	// Only an authoritative row (captured in the epoch-rollover transaction)
	// counts as "already captured". A provisional fallback row must not block a
	// later block-based fallback that carries the real epoch nonce.
	if !snapshot.Authoritative {
		return false, nil
	}
	if snapshot.CalculationVersion != models.RewardStakeCalculationVersion {
		return false, nil
	}
	if snapshot.BoundarySlot != evt.BoundarySlot {
		return false, nil
	}
	if len(snapshot.EpochNonce) == 0 && len(evt.EpochNonce) > 0 {
		return false, nil
	}
	if len(snapshot.EpochNonce) > 0 &&
		len(evt.EpochNonce) > 0 &&
		!bytes.Equal(snapshot.EpochNonce, evt.EpochNonce) {
		return false, nil
	}
	return true, nil
}

// ComputeEpochBoundarySnapshot computes the Mark snapshot's stake distribution
// at the SNAP point of the caller's open epoch-boundary transaction and holds it
// for the matching CaptureEpochBoundarySnapshot call later in the same rollover.
// It writes nothing.
//
// cardano-ledger runs SNAP before POOLREAP and before governance enactment, so a
// mark snapshot must reflect stake as of the boundary with only the pre-SNAP
// boundary rules applied (applyRUpd and MIR). dingo's authoritative capture reads
// the live reward aggregate, which has no slot predicate, and used to run at the
// very end of the rollover — so the mark snapshot also absorbed every credit
// cardano-ledger applies after SNAP: POOLREAP deposit refunds, enacted treasury
// withdrawals and proposal-deposit refunds, all recorded at the boundary slot.
//
// Splitting the capture is what fixes that without subtracting those credits
// back out: the stake read happens at the reference SNAP point (after
// applyStakeRewards and applyMIRCerts, before applyPoolRetirements), while the
// write stays at the end where the new epoch row, its nonce and the
// post-enactment protocol version exist. Both phases run in the one rollover
// transaction, so a rollback or replay of the boundary re-executes the same
// deterministic read and reproduces the same snapshot.
func (m *Manager) ComputeEpochBoundarySnapshot(
	ctx context.Context,
	txn *database.Txn,
	evt event.EpochTransitionEvent,
) error {
	m.lockConfiguration()
	expiryEpoch := m.expiryEpoch(evt.NewEpoch)
	calculator := NewCalculator(m.db)
	distribution, err := calculator.calculateStakeDistributionInTxn(
		ctx,
		txn,
		evt.SnapshotSlot,
		expiryEpoch,
	)
	if err != nil {
		// Leave nothing stashed: the persist phase recomputes with the
		// boundary-aware historical fallback and returns any failure.
		m.stashBoundaryDistribution(nil)
		return fmt.Errorf("calculate snap-point stake distribution: %w", err)
	}
	m.stashBoundaryDistribution(&pendingBoundarySnapshot{
		distribution: distribution,
		txn:          txn,
		newEpoch:     evt.NewEpoch,
		boundarySlot: evt.BoundarySlot,
		snapshotSlot: evt.SnapshotSlot,
		expiryEpoch:  expiryEpoch,
	})
	m.logger.Debug(
		"computed snap-point stake distribution",
		"component", "snapshot",
		"epoch", evt.NewEpoch,
		"pools", len(distribution.PoolStakes),
		"total_stake", distribution.TotalStake,
		"slot", evt.SnapshotSlot,
	)
	return nil
}

// CaptureEpochBoundarySnapshot persists the Mark snapshot using the caller's
// open epoch-boundary transaction. Ledger invokes this hook last in the rollover
// ordering, after POOLREAP, governance enactment, donation accounting, and the
// new epoch row have been applied, because the row it writes needs the new
// epoch's nonce and the post-enactment protocol version.
//
// The stake distribution it persists is the one ComputeEpochBoundarySnapshot
// read at the SNAP point earlier in this same transaction. When no matching
// SNAP-point distribution exists — the compute hook is not installed, or its
// read failed — it reconstructs the exact boundary with slot-aware reward
// semantics. It never falls back to the live aggregate, whose post-SNAP
// credits would corrupt the Mark snapshot.
func (m *Manager) CaptureEpochBoundarySnapshot(
	ctx context.Context,
	txn *database.Txn,
	evt event.EpochTransitionEvent,
) error {
	m.lockConfiguration()
	start := time.Now()
	expiryEpoch := m.expiryEpoch(evt.NewEpoch)

	distribution := m.takeBoundaryDistribution(txn, evt, expiryEpoch)
	if distribution == nil {
		m.logger.Debug(
			"no snap-point stake distribution for boundary; reading at persist time",
			"component",
			"snapshot",
			"epoch",
			evt.NewEpoch,
			"boundary_slot",
			evt.BoundarySlot,
		)
		calculator := NewCalculator(m.db)
		var err error
		// Persist runs after POOLREAP/governance. Force historical
		// reconstruction: the normal boundary helper may use the live fast path
		// when the tip is <= SnapshotSlot, but live state already has post-SNAP
		// credits at this point.
		distribution, err = calculator.calculateHistoricalBoundaryStakeDistributionInTxn(
			ctx,
			txn,
			evt.SnapshotSlot,
			evt.BoundarySlot,
			expiryEpoch,
			m.inactivityPeriod(),
		)
		if err != nil {
			if m.metrics != nil {
				m.metrics.captureFailureTotal.Inc()
				m.metrics.captureDurationSeconds.Observe(
					time.Since(start).Seconds(),
				)
			}
			return fmt.Errorf("calculate stake distribution: %w", err)
		}
	}

	if err := m.saveSnapshotInTxn(
		evt.NewEpoch,
		"mark",
		distribution,
		evt,
		true,
		true,
		// checkAuthoritativeMark: false. This IS the authoritative capture,
		// run synchronously inside the epoch-rollover write transaction at
		// the exact SNAP point, so it must always be allowed to overwrite
		// any provisional rows a fallback (event-driven) capture already
		// wrote for this epoch.
		false,
		txn,
	); err != nil {
		if m.metrics != nil {
			m.metrics.captureFailureTotal.Inc()
			m.metrics.captureDurationSeconds.Observe(
				time.Since(start).Seconds(),
			)
		}
		return fmt.Errorf("save mark snapshot: %w", err)
	}

	// The capture is staged in the caller's still-open epoch-rollover
	// transaction, so success must be reported only once that transaction
	// commits: a rollback (including a failed commit) must not advance the
	// success counter or the "latest snapshot" gauges to describe a snapshot
	// that never persisted. Register them as an after-commit callback, which
	// matches the fallback captureMarkSnapshot path whose own transaction has
	// already committed by the time it records success. The duration histogram
	// measures the capture work itself, so it is observed inline like the
	// failure paths above.
	if m.metrics != nil {
		m.metrics.captureDurationSeconds.Observe(time.Since(start).Seconds())
		pools := float64(len(distribution.PoolStakes))
		totalStake := float64(distribution.TotalStake)
		epoch := float64(evt.NewEpoch)
		txn.AfterCommit(func() {
			m.metrics.captureSuccessTotal.Inc()
			m.metrics.capturePoolsTotal.Set(pools)
			m.metrics.captureTotalStakeLovelace.Set(totalStake)
			m.metrics.lastSuccessfulEpoch.Set(epoch)
		})
	}

	m.logger.Debug(
		"staged epoch-boundary mark snapshot",
		"component", "snapshot",
		"epoch", evt.NewEpoch,
		"pools", len(distribution.PoolStakes),
		"total_stake", distribution.TotalStake,
		"slot", evt.SnapshotSlot,
	)

	return nil
}

// calculateSnapshotDistribution computes a snapshot distribution outside the
// rollover transaction. boundarySlot is the epoch boundary the snapshot belongs
// to (0 when the caller is not reconstructing one, e.g. genesis and
// post-Mithril seeding); see boundaryRewardSlot for how it is validated.
func (m *Manager) calculateSnapshotDistribution(
	ctx context.Context,
	slot uint64,
	boundarySlot uint64,
	expiryEpoch uint64,
) (*StakeDistribution, error) {
	calculator := NewCalculator(m.db)
	txn := m.db.Transaction(false)
	defer func() { _ = txn.Commit() }()
	return calculator.calculateBoundaryStakeDistributionInTxn(
		ctx, txn, slot, boundarySlot, expiryEpoch, m.inactivityPeriod(),
	)
}

// captureMarkSnapshot captures the stake distribution as a Mark snapshot.
func (m *Manager) captureMarkSnapshot(
	ctx context.Context,
	evt event.EpochTransitionEvent,
) error {
	m.lockConfiguration()
	start := time.Now()

	// Calculate canonical, slot-aware leader-election pool totals and attach
	// RewardLiveStake credential inputs for reward capture.
	distribution, err := m.calculateSnapshotDistribution(
		ctx,
		evt.SnapshotSlot,
		evt.BoundarySlot,
		m.expiryEpoch(evt.NewEpoch),
	)
	if err != nil {
		if m.metrics != nil {
			m.metrics.captureFailureTotal.Inc()
			m.metrics.captureDurationSeconds.Observe(
				time.Since(start).Seconds(),
			)
		}
		return fmt.Errorf("calculate stake distribution: %w", err)
	}

	// Save as Mark snapshot for the new epoch. At a normal epoch
	// transition live Pool/Account state matches the boundary, so
	// the CIP-1694 reward-account auto-vote resolves correctly.
	saved, err := m.saveSnapshot(
		ctx,
		evt.NewEpoch,
		"mark",
		distribution,
		evt,
		true, // resolveAutoVote: live state == boundary at this call site
		true, // persistRewardInputs: live state == boundary at this call site
		// checkAuthoritativeMark: true. This is the fallback (event-driven)
		// capture path — the authoritative CaptureEpochBoundarySnapshot can
		// commit concurrently while this method is still scanning the live
		// aggregate above, so the write must re-verify, inside its own
		// transaction, that it isn't about to clobber an authoritative
		// snapshot that landed in the meantime.
		true,
	)
	if err != nil {
		if m.metrics != nil {
			m.metrics.captureFailureTotal.Inc()
			m.metrics.captureDurationSeconds.Observe(
				time.Since(start).Seconds(),
			)
		}
		return fmt.Errorf("save mark snapshot: %w", err)
	}
	if !saved {
		return nil
	}

	if m.metrics != nil {
		m.metrics.captureDurationSeconds.Observe(time.Since(start).Seconds())
		m.metrics.captureSuccessTotal.Inc()
		m.metrics.capturePoolsTotal.Set(float64(len(distribution.PoolStakes)))
		m.metrics.captureTotalStakeLovelace.Set(
			float64(distribution.TotalStake),
		)
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

// HandleGenesisSnapshotError applies the standard policy for a failed genesis
// (epoch 0) mark-snapshot capture: it is fatal for a block producer (which
// cannot elect leaders without the snapshot) and a warning for a relay or
// replay-only node (which does not perform leader election). A nil err returns
// nil, so callers can forward the CaptureGenesisSnapshot result directly.
func HandleGenesisSnapshotError(
	blockProducer bool,
	logger *slog.Logger,
	err error,
) error {
	if err == nil {
		return nil
	}
	if blockProducer {
		return fmt.Errorf("failed to capture genesis snapshot: %w", err)
	}
	logger.Warn(
		"failed to capture genesis snapshot",
		"error", err,
	)
	return nil
}

// CaptureGenesisSnapshot captures the initial stake distribution as mark
// snapshots. For a fresh sync this seeds epoch 0. After a Mithril bootstrap
// the node starts at a much later epoch, so the method also seeds the recent
// historical window (epochs N, N-1, N-2).
func (m *Manager) CaptureGenesisSnapshot(ctx context.Context) error {
	m.lockConfiguration()
	start := time.Now()
	if ok, epoch, pools, stake, err := m.hasExistingPostMithrilSnapshotWindow(); err != nil {
		m.logger.Warn(
			"failed to check existing post-Mithril snapshot window",
			"component", "snapshot",
			"error", err,
		)
	} else if ok {
		if m.metrics != nil {
			m.metrics.capturePoolsTotal.Set(float64(pools))
			m.metrics.captureTotalStakeLovelace.Set(float64(stake))
			m.metrics.lastSuccessfulEpoch.Set(float64(epoch))
		}
		m.logger.Info(
			"post-Mithril snapshot window already present; skipping genesis snapshot capture",
			"component", "snapshot",
			"epoch", epoch,
			"total_pools", pools,
			"total_stake", stake,
		)
		return nil
	}

	successCount := uint64(0)
	lastSuccessfulEpoch := uint64(0)

	// Fresh sync seeds epoch 0, where no account can be expired, so the gate
	// argument is 0 regardless of the enabled flag.
	distribution, err := m.calculateSnapshotDistribution(
		ctx, 0, 0, m.expiryEpoch(0),
	)
	if err != nil {
		if m.metrics != nil {
			m.metrics.captureFailureTotal.Inc()
			m.metrics.captureDurationSeconds.Observe(
				time.Since(start).Seconds(),
			)
		}
		return fmt.Errorf("calculate genesis distribution: %w", err)
	}

	// After Mithril import, slot 0 has no pool data. Fall back to
	// the latest epoch's start slot where imported pools are visible.
	// Also remember the latest epoch so we can seed the Mark/Set/Go
	// window below.
	var currentEpochId uint64
	// latestEpochKnown records that the lookup below actually determined the
	// database's current epoch. currentEpochId's zero value is also a valid
	// answer -- a fresh sync -- so it cannot itself distinguish "epoch 0"
	// from "not determined", and the fresh-sync classification further down
	// must not read a failed lookup as a stakeless genesis.
	latestEpochKnown := false
	if distribution.TotalPools == 0 {
		epochs, epErr := m.db.GetEpochs(nil)
		if epErr != nil {
			// Returned rather than logged: the classification below decides
			// whether to persist an epoch-0 mark snapshot, and doing that for
			// a post-Mithril database would hand later reward calculation a
			// fabricated basis. Every caller routes this through
			// HandleGenesisSnapshotError, which is fatal for a block producer
			// and a warning otherwise.
			if m.metrics != nil {
				m.metrics.captureFailureTotal.Inc()
				m.metrics.captureDurationSeconds.Observe(
					time.Since(start).Seconds(),
				)
			}
			return fmt.Errorf(
				"load epochs for genesis stake fallback: %w",
				epErr,
			)
		} else if len(epochs) > 0 {
			latestEpochKnown = true
			lastEpoch := epochs[len(epochs)-1]
			currentEpochId = lastEpoch.EpochId
			m.logger.Debug(
				"attempting genesis stake fallback from latest epoch",
				"component", "snapshot",
				"epoch", currentEpochId,
				"start_slot", lastEpoch.StartSlot,
				"total_pools", distribution.TotalPools,
			)
			dist2, err2 := m.calculateSnapshotDistribution(
				ctx, lastEpoch.StartSlot, 0, m.expiryEpoch(currentEpochId),
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

	// An empty distribution means two different things depending on where the
	// chain actually is.
	//
	// Behind a later current epoch it means the pool data predates a Mithril
	// import and is simply not visible at slot 0. There is no trustworthy
	// epoch-0 basis to persist, so the capture is skipped as before.
	//
	// On a true fresh sync it means the network's Shelley genesis registers no
	// pools and the first ones register on chain during epoch 0 -- preview is
	// such a network. cardano-ledger's Go stake distribution is empty at
	// genesis there, so the epoch-0 mark snapshot is empty rather than absent,
	// and it is still persisted below: the reward rounds at the boundaries
	// into epochs 1, 2 and 3 all resolve against snapshot epoch 0, and without
	// the row every one of them skips for a missing reward snapshot and the
	// ADA pots never move (dingo #3381).
	//
	// That branch is taken only when the lookup above positively determined
	// the current epoch to be 0. An undetermined epoch is not a fresh sync.
	freshSync := latestEpochKnown && currentEpochId == 0
	if distribution.TotalPools == 0 && !freshSync {
		if m.metrics != nil {
			m.metrics.captureDurationSeconds.Observe(
				time.Since(start).Seconds(),
			)
		}
		m.logger.Info(
			"no genesis pools; leader election disabled"+
				" until pool stake is registered",
			"component", "snapshot",
		)
		return nil
	}
	if distribution.TotalPools == 0 {
		m.logger.Info(
			"no genesis pools; seeding an empty epoch 0 mark snapshot."+
				" leader election is disabled until pool stake is registered",
			"component", "snapshot",
		)
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
	// Same rule as the seeding loop below: live-state-derived fields
	// are persisted only when the target epoch matches the current
	// live-state epoch. For a true fresh sync currentEpochId is 0 and
	// live Pool/Account/reward aggregate state IS the genesis boundary.
	// For post-Mithril bootstrap currentEpochId > 0 and `distribution`
	// plus the live tables reflect that later epoch — passing true here
	// would freeze today's delegation/reward map onto an epoch-0 row.
	if _, err := m.saveSnapshot(
		ctx, 0, "mark", distribution, evt,
		currentEpochId == 0,
		currentEpochId == 0,
		// checkAuthoritativeMark: false. Genesis/bootstrap capture runs
		// once at startup, before the epoch-transition event loop can be
		// racing an in-flight epoch-boundary rollover.
		false,
	); err != nil {
		if m.metrics != nil {
			m.metrics.captureFailureTotal.Inc()
			m.metrics.captureDurationSeconds.Observe(
				time.Since(start).Seconds(),
			)
		}
		return fmt.Errorf("save genesis snapshot: %w", err)
	}
	successCount++

	// After Mithril bootstrap: seed the recent historical mark-snapshot
	// window so epoch-offset consumers have the same nearby history normal
	// syncing would have.
	if currentEpochId > 0 {
		for offset := uint64(0); offset <= 2 && offset <= currentEpochId; offset++ {
			seedEpoch := currentEpochId - offset
			if seedEpoch == 0 {
				continue // already saved above
			}
			// ExpirationEpoch is live account state, not historical state.
			// Once CIP-0163 is enabled a later witness may have renewed an
			// account that was expired at this older boundary, so the current
			// rows cannot safely reconstruct N-1/N-2. Leave those rows absent
			// instead of persisting a consensus-incorrect historical snapshot.
			if offset > 0 && m.expiryEpoch(seedEpoch) > 0 {
				m.logger.Warn(
					"skipping post-Mithril historical snapshot with delegator inactivity enabled",
					"component",
					"snapshot",
					"epoch",
					seedEpoch,
				)
				continue
			}
			seedEvt := event.EpochTransitionEvent{
				NewEpoch: seedEpoch,
			}
			// Resolve live-state-derived auto-vote and reward inputs
			// only on offset==0 (seedEpoch == currentEpochId) — that's
			// the one iteration where live Pool/Account/reward state
			// matches the target boundary. For offset 1 and 2 we are
			// seeding boundaries that predate the live state by 1–2
			// epochs, so resolving would freeze today's delegation map
			// onto a historical row and persisting reward inputs would
			// make reward calculation consume synthetic current-state
			// data as historical inputs. Those rows go in with
			// RewardAccountAutoVoteResolved=false and no reward input
			// bundle.
			if _, err := m.saveSnapshot(
				ctx, seedEpoch, "mark", distribution, seedEvt,
				offset == 0,
				offset == 0,
				// checkAuthoritativeMark: false, same reasoning as the
				// epoch-0 save above — bootstrap-time seeding, not racing
				// a concurrent epoch-boundary rollover.
				false,
			); err != nil {
				if m.metrics != nil {
					m.metrics.captureFailureTotal.Inc()
					m.metrics.captureDurationSeconds.Observe(
						time.Since(start).Seconds(),
					)
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
		m.metrics.captureTotalStakeLovelace.Set(
			float64(distribution.TotalStake),
		)
		m.metrics.lastSuccessfulEpoch.Set(float64(lastSuccessfulEpoch))
	}
	return nil
}

func (m *Manager) hasExistingPostMithrilSnapshotWindow() (
	bool,
	uint64,
	uint64,
	uint64,
	error,
) {
	epochs, err := m.db.GetEpochs(nil)
	if err != nil {
		return false, 0, 0, 0, fmt.Errorf("load epochs: %w", err)
	}
	if len(epochs) == 0 {
		return false, 0, 0, 0, nil
	}
	currentEpoch := epochs[len(epochs)-1].EpochId
	if currentEpoch == 0 {
		return false, 0, 0, 0, nil
	}

	meta := m.db.Metadata()
	var currentPools uint64
	var currentStake uint64
	for offset := uint64(0); offset <= 2 && offset <= currentEpoch; offset++ {
		epoch := currentEpoch - offset
		snapshots, err := meta.GetPoolStakeSnapshotsByEpoch(
			epoch,
			"mark",
			nil,
		)
		if err != nil {
			return false, 0, 0, 0, fmt.Errorf(
				"load mark snapshots for epoch %d: %w",
				epoch,
				err,
			)
		}
		if len(snapshots) == 0 {
			return false, 0, 0, 0, nil
		}
		var totalStake uint64
		for _, snapshot := range snapshots {
			totalStake += uint64(snapshot.TotalStake)
		}
		if totalStake == 0 {
			return false, 0, 0, 0, nil
		}
		if offset == 0 {
			currentPools = uint64(len(snapshots))
			currentStake = totalStake
		}
	}
	return true, currentEpoch, currentPools, currentStake, nil
}
