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

// Package chainsyncrecycler detects stalled chainsync clients and recycles
// truly stuck connections. It owns only the stall/plateau decision logic; the
// node supplies the live components each tick reads and the event bus the
// recovery requests are published to, so the recycler can be exercised without
// constructing a node.
package chainsyncrecycler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	ouroboros "github.com/blinklabs-io/gouroboros"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
)

// catchUpMultiplier extends every recycling threshold while the node is behind
// the chain tip. Connection recycling during bulk sync causes pipeline resets,
// TIME_WAIT socket exhaustion, and dropped rollbacks that slow catch-up far
// more than the stall itself.
const catchUpMultiplier = 5

// defaultRestartDelay is how long the run loop waits before restarting after a
// recovered panic.
const defaultRestartDelay = time.Second

// LedgerSource is the ledger state the recycler reads to judge local progress
// and to repair a primary-chain/ledger divergence locally.
type LedgerSource interface {
	Tip() ochainsync.Tip
	IsAtTip() bool
	SecurityParam() int
	PrimaryChainTipSlot() uint64
	ReconcileLivePrimaryChainLedgerDivergence(
		reason string,
		connId ouroboros.ConnectionId,
	) (bool, error)
}

// ChainsyncState is the chainsync client tracking the recycler drives.
type ChainsyncState interface {
	CheckStalledClients() []ouroboros.ConnectionId
	AdvanceHeaderSyncRotation()
	GetTrackedClients() []chainsync.TrackedClient
	GetClientConnId() *ouroboros.ConnectionId
}

// ChainSelector is the peer-tip view used to detect a local tip plateau.
type ChainSelector interface {
	SetLocalTip(tip ochainsync.Tip)
	SetSecurityParam(k uint64)
	GetBestPeer() *ouroboros.ConnectionId
	GetPeerTip(connId ouroboros.ConnectionId) *chainselection.PeerChainTip
}

// EventPublisher publishes the recovery requests the recycler decides on.
type EventPublisher interface {
	Publish(eventType event.EventType, evt event.Event)
	PublishAsync(eventType event.EventType, evt event.Event) bool
}

// LiveComponents is the component set a single tick operates on. Ledger and
// ChainsyncState are always non-nil; ChainSelector is nil when chain selection
// is not wired.
type LiveComponents struct {
	Ledger         LedgerSource
	ChainsyncState ChainsyncState
	ChainSelector  ChainSelector
}

// ComponentProvider hands the recycler the currently live components.
//
// A live database restore/truncate replaces the node's ledger and chainsync
// state, so the recycler must not hold either across ticks. The provider
// invokes fn with the current set while guaranteeing it is not swapped
// underneath, and returns false without calling fn when the components are
// unavailable — mid-reinitialization, or because acquiring them would have
// meant blocking. A skipped tick is always safe: this is a periodic
// best-effort check.
type ComponentProvider interface {
	WithLiveComponents(fn func(LiveComponents)) bool
}

// Config holds the recycler's dependencies and timing policy.
type Config struct {
	// Components supplies the live node components each tick reads.
	Components ComponentProvider
	// EventBus receives resync, recycle, and client-remove requests.
	EventBus EventPublisher
	Logger   *slog.Logger
	// StallTimeout is the chainsync stall timeout the thresholds derive from.
	StallTimeout time.Duration
	// Interval is the stall-check cadence.
	Interval time.Duration
	// Grace is how long a stalled client is left alone before recycling.
	Grace time.Duration
	// Cooldown is the minimum spacing between recycles of one connection.
	Cooldown time.Duration
}

// Recycler is the chainsync stall recycler background component.
type Recycler struct {
	config          Config
	logger          *slog.Logger
	plateauRecovery time.Duration
	// restartDelay is the backoff before the loop restarts after a recovered
	// panic. Fixed at defaultRestartDelay in production; tests shrink it.
	restartDelay time.Duration

	mu      sync.Mutex
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	started bool
}

// New builds a recycler. Dependencies are validated by Start.
func New(cfg Config) *Recycler {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	return &Recycler{
		config:          cfg,
		logger:          logger,
		plateauRecovery: plateauThreshold(cfg.StallTimeout),
		restartDelay:    defaultRestartDelay,
	}
}

// Start launches the stall-check loop. It returns an error when a dependency
// is missing or the tick interval is not positive.
func (r *Recycler) Start(ctx context.Context) error {
	if r.config.Components == nil {
		return errors.New("chainsync stall recycler: components must not be nil")
	}
	if r.config.EventBus == nil {
		return errors.New("chainsync stall recycler: event bus must not be nil")
	}
	if r.config.Interval <= 0 {
		return errors.New(
			"chainsync stall recycler: interval must be greater than zero",
		)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.started {
		return errors.New("chainsync stall recycler: already started")
	}
	runCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.started = true
	r.wg.Go(func() {
		r.run(runCtx)
	})
	return nil
}

// Stop cancels the loop and waits for it to exit.
//
// The wait is intentionally unbounded: advancing shutdown while the recycler is
// still active races teardown of the ledger, chainsync, and connection state it
// reads and publishes to.
func (r *Recycler) Stop() {
	r.mu.Lock()
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	r.wg.Wait()
	r.mu.Lock()
	r.started = false
	r.mu.Unlock()
}

// run keeps the stall-check loop alive: a panic inside the loop is logged and
// the loop restarts unless shutdown was requested.
func (r *Recycler) run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if !r.runLoop(func() { r.loop(ctx) }) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.restartDelay):
		}
	}
}

// loop runs stall checks on the configured interval until ctx is cancelled.
func (r *Recycler) loop(ctx context.Context) {
	ticker := time.NewTicker(r.config.Interval)
	defer ticker.Stop()
	st := newTickState()
	r.initProgressBaseline(st)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.runTick(func() {
				r.config.Components.WithLiveComponents(
					func(live LiveComponents) {
						now := time.Now()
						localTipSlot := r.observeLocalTip(live)
						r.tick(now, st, live, localTipSlot)
					},
				)
			})
		}
	}
}

// initProgressBaseline seeds the plateau baseline from the current ledger tip.
//
// The read is best-effort: when the components are unavailable the baseline
// stays at zero, and the first tick that reads a nonzero local tip resets it
// anyway (see tick). That can only make plateau detection more lenient at
// startup, never trigger it early.
func (r *Recycler) initProgressBaseline(st *tickState) {
	r.config.Components.WithLiveComponents(func(live LiveComponents) {
		st.lastProgressSlot = live.Ledger.Tip().Point.Slot
	})
	st.lastProgressAt = time.Now()
}

// observeLocalTip pushes the current ledger tip into chain selection and
// returns the local tip slot for this tick.
func (r *Recycler) observeLocalTip(live LiveComponents) uint64 {
	localTip := live.Ledger.Tip()
	if live.ChainSelector != nil {
		live.ChainSelector.SetLocalTip(localTip)
		if k := live.Ledger.SecurityParam(); k > 0 {
			live.ChainSelector.SetSecurityParam(uint64(k)) //nolint:gosec
		}
	}
	return localTip.Point.Slot
}

// runTick runs one stall check, logging and swallowing a panic so later ticks
// keep running.
func (r *Recycler) runTick(fn func()) {
	defer func() {
		if err := recover(); err != nil {
			r.logger.Error(
				"panic in stall checker tick, continuing",
				"panic", err,
				"stack", string(debug.Stack()),
			)
		}
	}()
	fn()
}

// runLoop runs the stall-check loop, reporting whether it exited via a
// recovered panic (and so should be restarted).
func (r *Recycler) runLoop(fn func()) (recovered bool) {
	defer func() {
		if err := recover(); err != nil {
			recovered = true
			r.logger.Error(
				"panic in stall checker goroutine",
				"panic", err,
				"stack", string(debug.Stack()),
			)
		}
	}()
	fn()
	return false
}

// tickState is the per-loop bookkeeping carried across stall checks.
type tickState struct {
	// recycleAt is the guarded recycle deadline per stalled connection.
	recycleAt map[string]time.Time
	// lastRecycled is the recycle cooldown clock per connection.
	lastRecycled map[string]time.Time
	// lastProgressSlot/lastProgressAt track local tip plateau duration.
	lastProgressSlot uint64
	lastProgressAt   time.Time
}

func newTickState() *tickState {
	return &tickState{
		recycleAt:    make(map[string]time.Time),
		lastRecycled: make(map[string]time.Time),
	}
}

func plateauThreshold(stallTimeout time.Duration) time.Duration {
	return max(2*stallTimeout, 4*time.Minute)
}

func shouldRecycleLocalTipPlateau(
	now time.Time,
	lastProgressAt time.Time,
	localTipSlot uint64,
	bestPeerTipSlot uint64,
	lastRecycledAt *time.Time,
	cooldown time.Duration,
	threshold time.Duration,
) bool {
	if bestPeerTipSlot <= localTipSlot {
		return false
	}
	if now.Sub(lastProgressAt) <= threshold {
		return false
	}
	if lastRecycledAt != nil && now.Sub(*lastRecycledAt) < cooldown {
		return false
	}
	return true
}

// isLedgerApplicationBacklog reports whether a local-tip plateau is caused by
// the ledger pipeline replaying a backlog of already-fetched blocks rather than
// by a stalled chainsync stream.
//
// A plateau only means the APPLIED ledger tip stopped advancing while a peer is
// ahead. That is a chainsync problem only when headers are actually missing. On
// Leios (and any deep catch-up) the header/primary chain routinely runs far
// ahead of the applied ledger tip while the ledger pipeline replays a large
// backlog of blocks it has already fetched. When the primary chain has covered
// the bulk of the distance to the best peer and the remaining gap is dominated
// by downloaded-but-not-yet-applied blocks, the chainsync stream is healthy and
// caught up: recycling it cannot advance the applied tip and only churns the
// connection. Requiring the apply backlog to be at least as large as the
// residual header gap keeps a genuinely lagging header chain (headers missing,
// nothing applied) on the recycle path.
func isLedgerApplicationBacklog(
	appliedTipSlot uint64,
	primaryChainTipSlot uint64,
	bestPeerTipSlot uint64,
) bool {
	if primaryChainTipSlot <= appliedTipSlot {
		// Header chain is not ahead of the applied tip, so there is no
		// backlog to drain: any plateau here is an upstream/header stall.
		return false
	}
	var headerGap uint64
	if bestPeerTipSlot > primaryChainTipSlot {
		headerGap = bestPeerTipSlot - primaryChainTipSlot
	}
	applyBacklog := primaryChainTipSlot - appliedTipSlot
	return applyBacklog >= headerGap
}

// tick runs one stall check against the live components.
func (r *Recycler) tick(
	now time.Time,
	st *tickState,
	live LiveComponents,
	localTipSlot uint64,
) {
	if localTipSlot > st.lastProgressSlot {
		st.lastProgressSlot = localTipSlot
		st.lastProgressAt = now
	}
	// During catch-up, extend all recycling thresholds to avoid churning
	// connections while the node is making progress.
	multiplier := 1
	if !live.Ledger.IsAtTip() {
		multiplier = catchUpMultiplier
	}
	effectiveGrace := time.Duration(multiplier) * r.config.Grace
	effectivePlateau := time.Duration(multiplier) * r.plateauRecovery
	effectiveCooldown := time.Duration(multiplier) * r.config.Cooldown
	live.ChainsyncState.CheckStalledClients()
	// Rotate the round-robin header-ingress driver on the stall-check
	// cadence. No-op under the primary/parallel strategies.
	live.ChainsyncState.AdvanceHeaderSyncRotation()
	trackedClients := live.ChainsyncState.GetTrackedClients()
	trackedByID := make(
		map[string]chainsync.TrackedClient,
		len(trackedClients),
	)
	eligibleCount := 0
	for _, conn := range trackedClients {
		connKey := conn.ConnId.String()
		trackedByID[connKey] = conn
		if conn.Status != chainsync.ClientStatusStalled {
			delete(st.recycleAt, connKey)
		}
		if !conn.ObservabilityOnly {
			eligibleCount++
		}
	}
	// Prune expired cooldown entries so this map does not grow without bound
	// over long runtimes.
	for connKey, last := range st.lastRecycled {
		if now.Sub(last) >= effectiveCooldown {
			delete(st.lastRecycled, connKey)
		}
	}
	r.checkLocalTipPlateau(
		now,
		st,
		live,
		localTipSlot,
		trackedClients,
		eligibleCount,
		effectivePlateau,
		effectiveCooldown,
	)
	r.scheduleStalledRecycles(now, st, trackedClients, effectiveGrace)
	r.processDueRecycles(
		now,
		st,
		live,
		trackedByID,
		eligibleCount,
		effectiveCooldown,
	)
}

// checkLocalTipPlateau is the safety net for a local tip that has not moved for
// a long time while peers are ahead: recycle the selected chainsync connection
// even if it is not marked stalled.
func (r *Recycler) checkLocalTipPlateau(
	now time.Time,
	st *tickState,
	live LiveComponents,
	localTipSlot uint64,
	trackedClients []chainsync.TrackedClient,
	eligibleCount int,
	effectivePlateau time.Duration,
	effectiveCooldown time.Duration,
) {
	if live.ChainSelector == nil {
		return
	}
	bestPeer := live.ChainSelector.GetBestPeer()
	if bestPeer == nil {
		return
	}
	bestPeerTip := live.ChainSelector.GetPeerTip(*bestPeer)
	if bestPeerTip == nil || bestPeerTip.Tip.Point.Slot <= localTipSlot {
		return
	}
	targetConn := live.ChainsyncState.GetClientConnId()
	if targetConn == nil {
		targetCopy := *bestPeer
		targetConn = &targetCopy
	}
	connKey := targetConn.String()
	var lastRecycledAt *time.Time
	if last, ok := st.lastRecycled[connKey]; ok {
		lastCopy := last
		lastRecycledAt = &lastCopy
	}
	if !shouldRecycleLocalTipPlateau(
		now,
		st.lastProgressAt,
		localTipSlot,
		bestPeerTip.Tip.Point.Slot,
		lastRecycledAt,
		effectiveCooldown,
		effectivePlateau,
	) {
		return
	}
	// First, always attempt the LOCAL ledger reconcile. It repairs a silent
	// primary-chain / ledger divergence (chain.Tip() advanced, or the ledger
	// tip fell off the primary chain, while the ledger pipeline stayed pinned
	// on an abandoned fork so fetched blocks "do not fit on current chain
	// tip") by rolling the ledger back to the latest common ancestor. The two
	// call sites in ledger/chainsync.go only fire on
	// ErrRollbackExceedsSecurityParam; sub-K fork resolutions leave no error
	// to trigger reconcile. This is a purely local repair -- it does not touch
	// the peer connection -- so it is safe, and the only available recovery,
	// even with a single eligible upstream (e.g. a one-relay devnet/leios
	// topology), which would otherwise wedge here permanently.
	reconcileFailed := false
	reconciled, err := live.Ledger.ReconcileLivePrimaryChainLedgerDivergence(
		"local tip plateau",
		*targetConn,
	)
	if err != nil {
		reconcileFailed = true
		r.logger.Warn(
			"plateau reconcile failed",
			"connection_id", connKey,
			"error", err.Error(),
		)
	} else if reconciled {
		r.logger.Warn(
			"local tip plateau resolved via ledger reconcile",
			"connection_id", connKey,
			"local_tip_slot", localTipSlot,
			"best_peer_tip_slot", bestPeerTip.Tip.Point.Slot,
			"plateau_duration", now.Sub(st.lastProgressAt),
		)
		// Reset the plateau clock so we don't immediately re-trigger on the
		// next tick before forward application has had a chance to advance the
		// ledger.
		st.lastProgressAt = now
		st.lastRecycled[connKey] = now
		// Reconciliation reset the plateau clock and recorded cooldown above.
		// Give ledger replay a chance to resume from the repaired tip before
		// trying any connection-level recovery.
		return
	}
	// A local-tip plateau on Leios is usually the ledger pipeline replaying a
	// backlog of already-fetched blocks, not a stalled header stream. When the
	// primary chain is already caught up to the peer, recycling the (healthy)
	// chainsync connection cannot advance the applied tip and only churns the
	// connection -- dropped pipelines, MustReply timeouts from ingress
	// backpressure, and TIME_WAIT/goroutine growth. Only trust this heuristic
	// after the local reconcile had a chance to repair, or at least rule out,
	// a primary-chain / ledger divergence.
	primaryChainTipSlot := live.Ledger.PrimaryChainTipSlot()
	if !reconcileFailed && isLedgerApplicationBacklog(
		localTipSlot,
		primaryChainTipSlot,
		bestPeerTip.Tip.Point.Slot,
	) {
		// The header chain is already caught up to the peer; the plateau is
		// the ledger pipeline draining a backlog of already-fetched blocks.
		// Recycling the chainsync stream cannot help and only churns the
		// connection, so leave it running and let the pipeline advance.
		// Surfacing this at INFO also stops a wedged ledger pipeline from
		// being masked as a chainsync stall.
		r.logger.Info(
			"local tip plateau is a ledger-application backlog; header chain already caught up, not recycling chainsync",
			"connection_id", connKey,
			"applied_tip_slot", localTipSlot,
			"primary_chain_tip_slot", primaryChainTipSlot,
			"best_peer_tip_slot", bestPeerTip.Tip.Point.Slot,
			"plateau_duration", now.Sub(st.lastProgressAt),
		)
		// Reset the plateau clock so we re-evaluate only after another full
		// plateau window instead of every tick while the ledger pipeline
		// drains.
		st.lastProgressAt = now
		delete(st.recycleAt, connKey)
		return
	}
	// The local reconcile found nothing to repair (or failed), so the stall is
	// in the upstream chainsync stream itself: the active peer's server-side
	// cursor has stopped advancing (a flaky/stalled relay) while chain
	// selection still tracks it AT a higher tip.
	//
	// A plateau resync is NOT a peer recycle. Recycling (the stalled path
	// below) drops the peer and fails over to a SPARE, so it genuinely needs a
	// spare and is suppressed at eligibleCount <= 1. A plateau resync instead
	// closes the connection so peer governance reconnects to the SAME remote
	// and re-enters FindIntersect with fresh intersect points anchored at the
	// current local tip (see chainsyncResyncRequiresFreshConnection). That is
	// exactly the recovery a single-peer plateau needs: it restarts header
	// delivery from local-tip+1 on the only upstream we have. The plateau
	// predicate (peer ahead AND no local progress for the full plateau
	// threshold) plus the recycle cooldown gate this so a healthy single peer
	// is never churned.
	r.logger.Warn(
		"local tip plateau detected, resyncing chainsync client",
		"connection_id", connKey,
		"local_tip_slot", localTipSlot,
		"best_peer_tip_slot", bestPeerTip.Tip.Point.Slot,
		"plateau_duration", now.Sub(st.lastProgressAt),
		"eligible_peer_count", eligibleCount,
	)
	r.config.EventBus.Publish(
		event.ChainsyncResyncEventType,
		event.NewEvent(
			event.ChainsyncResyncEventType,
			event.ChainsyncResyncEvent{
				ConnectionId: *targetConn,
				Reason:       event.ChainsyncResyncReasonLocalTipPlateau,
			},
		),
	)
	// Realign only matters when there are spare peers whose cursors raced
	// ahead while the active peer was stuck; with a single eligible peer it is
	// a no-op.
	if eligibleCount > 1 {
		r.realignOtherPeersAfterPlateau(
			*targetConn,
			trackedClients,
			localTipSlot,
		)
	}
	delete(st.recycleAt, connKey)
	st.lastRecycled[connKey] = now
	st.lastProgressAt = now
}

// scheduleStalledRecycles gives every newly stalled client a guarded recycle
// deadline, shrinking an existing deadline when the grace period drops.
func (r *Recycler) scheduleStalledRecycles(
	now time.Time,
	st *tickState,
	trackedClients []chainsync.TrackedClient,
	effectiveGrace time.Duration,
) {
	for _, conn := range trackedClients {
		if conn.Status != chainsync.ClientStatusStalled {
			continue
		}
		connKey := conn.ConnId.String()
		desiredDueAt := now.Add(effectiveGrace)
		if dueAt, exists := st.recycleAt[connKey]; !exists {
			st.recycleAt[connKey] = desiredDueAt
			r.logger.Info(
				"chainsync client stalled, scheduling guarded recycle",
				"connection_id", connKey,
				"stall_timeout", r.config.StallTimeout,
				"grace_period", effectiveGrace,
			)
		} else if dueAt.After(desiredDueAt) {
			// Shrink deadline when transitioning from catch-up to at-tip so
			// stalls aren't delayed unnecessarily.
			st.recycleAt[connKey] = desiredDueAt
		}
	}
}

// processDueRecycles acts on every stalled client whose guarded deadline has
// passed.
func (r *Recycler) processDueRecycles(
	now time.Time,
	st *tickState,
	live LiveComponents,
	trackedByID map[string]chainsync.TrackedClient,
	eligibleCount int,
	effectiveCooldown time.Duration,
) {
	for connKey, dueAt := range st.recycleAt {
		if now.Before(dueAt) {
			continue
		}
		tracked, ok := trackedByID[connKey]
		if !ok || tracked.Status != chainsync.ClientStatusStalled {
			delete(st.recycleAt, connKey)
			continue
		}
		connId := tracked.ConnId
		if last, ok := st.lastRecycled[connKey]; ok &&
			now.Sub(last) < effectiveCooldown {
			st.recycleAt[connKey] = now.Add(effectiveCooldown - now.Sub(last))
			continue
		}
		// Never recycle the only eligible peer. A block producer with a single
		// relay would lose its only propagation path during the reconnect
		// window. Observability-only connections are not eligible, so
		// recycling them does not reduce the eligible count.
		if eligibleCount <= 1 && !tracked.ObservabilityOnly {
			r.logger.Warn(
				"chainsync client stalled but is only eligible peer, skipping recycle",
				"connection_id", connKey,
				"stall_timeout", r.config.StallTimeout,
			)
			st.recycleAt[connKey] = now.Add(r.config.Grace)
			continue
		}
		active := live.ChainsyncState.GetClientConnId()
		if active == nil {
			// If no active client is selected and this client is overdue +
			// stalled, recycle to force a fresh connection attempt and avoid
			// indefinite stalls.
			r.logger.Warn(
				"chainsync client stalled with no active selection, recycling connection",
				"connection_id", connKey,
				"stall_timeout", r.config.StallTimeout,
				"grace_period", r.config.Grace,
				"recycle_cooldown", r.config.Cooldown,
			)
			r.publishConnectionRecycle(
				connId,
				connKey,
				"stalled_connection_no_active_selection",
			)
			delete(st.recycleAt, connKey)
			st.lastRecycled[connKey] = now
			continue
		}
		if active.String() != connKey {
			// Don't recycle non-primary stalled clients. Keep state clean.
			r.config.EventBus.PublishAsync(
				chainsync.ClientRemoveRequestedEventType,
				event.NewEvent(
					chainsync.ClientRemoveRequestedEventType,
					chainsync.ClientRemoveRequestedEvent{
						ConnId:  connId,
						ConnKey: connKey,
						Reason:  "stalled_non_primary_connection",
					},
				),
			)
			delete(st.recycleAt, connKey)
			continue
		}
		r.logger.Warn(
			"chainsync client stalled, recycling active connection",
			"connection_id", connKey,
			"stall_timeout", r.config.StallTimeout,
			"grace_period", r.config.Grace,
			"recycle_cooldown", r.config.Cooldown,
		)
		r.publishConnectionRecycle(connId, connKey, "stalled_active_connection")
		delete(st.recycleAt, connKey)
		st.lastRecycled[connKey] = now
	}
}

func (r *Recycler) publishConnectionRecycle(
	connId ouroboros.ConnectionId,
	connKey string,
	reason string,
) {
	r.config.EventBus.PublishAsync(
		connmanager.ConnectionRecycleRequestedEventType,
		event.NewEvent(
			connmanager.ConnectionRecycleRequestedEventType,
			connmanager.ConnectionRecycleRequestedEvent{
				ConnectionId: connId,
				ConnKey:      connKey,
				Reason:       reason,
			},
		),
	)
}

// realignOtherPeersAfterPlateau requests a fresh-connection chainsync resync
// for every ingress-eligible tracked peer other than the one being closed for
// plateau. Without realignment, a peer that has been streaming RollForwards
// while the active peer was stuck holds a server-side cursor far past our local
// tip; the chain selector will promote one of these peers as the next active,
// and its next RollForward delivers a header beyond the local block tip with no
// in-memory ancestor history to bridge the gap. The local fork resolver then
// fails and closes that peer too, cycling through peers until process restart.
// Realigning candidate peers' cursors to the current local tip lets whichever
// peer is promoted next deliver headers from local-tip+1 onward.
func (r *Recycler) realignOtherPeersAfterPlateau(
	closedConnId ouroboros.ConnectionId,
	trackedClients []chainsync.TrackedClient,
	localTipSlot uint64,
) {
	closedKey := closedConnId.String()
	for _, conn := range trackedClients {
		if conn.ObservabilityOnly {
			continue
		}
		if conn.ConnId.String() == closedKey {
			continue
		}
		if conn.Cursor.Slot <= localTipSlot {
			continue
		}
		r.logger.Info(
			"realigning peer chainsync cursor after plateau",
			"connection_id", conn.ConnId.String(),
			"cursor_slot", conn.Cursor.Slot,
			"local_tip_slot", localTipSlot,
		)
		r.config.EventBus.Publish(
			event.ChainsyncResyncEventType,
			event.NewEvent(
				event.ChainsyncResyncEventType,
				event.ChainsyncResyncEvent{
					ConnectionId: conn.ConnId,
					Reason:       event.ChainsyncResyncReasonPostPlateauRealign,
				},
			),
		)
	}
}
