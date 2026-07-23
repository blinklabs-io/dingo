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

package dingo

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/chainsync"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/peergov"
	ouroboros "github.com/blinklabs-io/gouroboros"
)

func plateauThreshold(stallTimeout time.Duration) time.Duration {
	return max(2*stallTimeout, 4*time.Minute)
}

// chainsyncObservePeerTip synchronously feeds a peer tip update into chain
// selection (and peergov) when the Genesis corroboration gate is active, so the
// ChainsyncApplyEligible check that immediately follows in the roll-forward
// handler reflects the header currently being admitted. This closes the race
// where the apply gate would otherwise read corroboration state that predates
// this header (the tip update is normally delivered asynchronously). It returns
// true when it handled the observation synchronously, so the ouroboros layer
// skips the async PeerTipUpdateEvent publish to avoid a double update.
//
// When corroboration is inactive the async path is used unchanged (returns
// false), so normal high-throughput sync keeps its parallelism.
func (n *Node) chainsyncObservePeerTip(
	e chainselection.PeerTipUpdateEvent,
) bool {
	if n.chainSelector == nil ||
		!n.chainSelector.GenesisCorroborationActive() {
		return false
	}
	n.chainSelector.HandlePeerTipUpdateEvent(
		event.NewEvent(chainselection.PeerTipUpdateEventType, e),
	)
	if n.peerGov != nil {
		n.peerGov.TouchPeerByConnId(e.ConnectionId)
	}
	return true
}

// chainsyncObserveRollback synchronously applies a peer rollback into chain
// selection when the Genesis corroboration gate is active, so the
// ChainsyncApplyEligible check that immediately follows in the roll-backward
// handler reflects the post-rollback corroboration state. A rollback trims the
// peer's observed frontier (ApplyRollback), which can change its corroboration
// status; delivering that observation asynchronously would let the apply gate
// read pre-trim state and forward a rollback for a peer that the rollback has
// just made uncorroborated (issue #2928). It returns true when handled
// synchronously, so the ouroboros layer skips the async PeerRollbackEvent
// publish to avoid a double update. Unlike chainsyncObservePeerTip there is no
// peergov touch: only the chain selector subscribes to PeerRollbackEvent.
//
// When corroboration is inactive the async path is used unchanged (returns
// false).
func (n *Node) chainsyncObserveRollback(
	e chainselection.PeerRollbackEvent,
) bool {
	if n.chainSelector == nil ||
		!n.chainSelector.GenesisCorroborationActive() {
		return false
	}
	n.chainSelector.HandlePeerRollbackEvent(
		event.NewEvent(chainselection.PeerRollbackEventType, e),
	)
	return true
}

// chainsyncApplyEligible gates whether a peer's headers/rollbacks are APPLIED to
// the ledger, on top of ingress eligibility. It defers to the chain selector's
// corroboration decision so an uncorroborated Genesis fast source is observed
// (its tips still feed corroboration) but its blocks are withheld — the real
// enforcement of the corroboration stall, since ingress is otherwise independent
// of the selected best peer. Returns true (apply) when no chain selector is
// wired yet or outside Genesis corroboration.
func (n *Node) chainsyncApplyEligible(
	connId ouroboros.ConnectionId,
) bool {
	if n.chainSelector == nil {
		return true
	}
	return n.chainSelector.ShouldApplyIngress(connId)
}

func (n *Node) isChainsyncIngressEligible(
	connId ouroboros.ConnectionId,
) bool {
	if n.peerGov != nil {
		return n.peerGov.IsChainSelectionEligible(connId)
	}
	n.chainsyncIngressEligibilityMu.RLock()
	defer n.chainsyncIngressEligibilityMu.RUnlock()
	if n.chainsyncIngressEligibilityCache == nil {
		return false
	}
	eligible, ok := n.chainsyncIngressEligibilityCache[connId]
	if !ok {
		return false
	}
	return eligible
}

func (n *Node) setChainsyncIngressEligibility(
	connId ouroboros.ConnectionId,
	eligible bool,
) {
	n.chainsyncIngressEligibilityMu.Lock()
	defer n.chainsyncIngressEligibilityMu.Unlock()
	if n.chainsyncIngressEligibilityCache == nil {
		n.chainsyncIngressEligibilityCache = make(
			map[ouroboros.ConnectionId]bool,
		)
	}
	n.chainsyncIngressEligibilityCache[connId] = eligible
}

func (n *Node) deleteChainsyncIngressEligibility(
	connId ouroboros.ConnectionId,
) {
	n.chainsyncIngressEligibilityMu.Lock()
	defer n.chainsyncIngressEligibilityMu.Unlock()
	if n.chainsyncIngressEligibilityCache == nil {
		return
	}
	delete(n.chainsyncIngressEligibilityCache, connId)
}

func (n *Node) handlePeerEligibilityChangedEvent(evt event.Event) {
	e, ok := evt.Data.(peergov.PeerEligibilityChangedEvent)
	if !ok {
		return
	}
	n.setChainsyncIngressEligibility(e.ConnectionId, e.Eligible)
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

func (n *Node) startChainsyncStallRecycler(
	ctx context.Context,
	chainsyncCfg chainsync.Config,
	interval time.Duration,
	grace time.Duration,
	cooldown time.Duration,
) context.CancelFunc {
	recyclerCtx, recyclerCancel := context.WithCancel(ctx)
	// Track the recycler as a node-owned background worker so shutdown can
	// wait for it before closing the dependencies it reads or publishes to.
	n.chainsyncStallRecyclerWG.Go(func() {
		// Mark the worker complete no matter whether it exits by cancellation
		// or after a recovered panic stops the outer loop.
		n.runChainsyncStallRecycler(
			recyclerCtx,
			chainsyncCfg,
			interval,
			grace,
			cooldown,
		)
	})
	return recyclerCancel
}

func (n *Node) runChainsyncStallRecycler(
	ctx context.Context,
	chainsyncCfg chainsync.Config,
	interval time.Duration,
	grace time.Duration,
	cooldown time.Duration,
) {
	for {
		if ctx.Err() != nil {
			return
		}
		// Keep the existing panic-recovery behavior: a panic in the loop is
		// logged and the recycler restarts unless shutdown was requested.
		if !n.runStallCheckerLoop(func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			recycleAt := make(map[string]time.Time)
			lastRecycled := make(map[string]time.Time)
			lastProgressSlot := n.ledgerState.Tip().Point.Slot
			lastProgressAt := time.Now()
			plateauRecoveryThreshold := plateauThreshold(
				chainsyncCfg.StallTimeout,
			)
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					n.runStallCheckerTick(func() {
						now := time.Now()
						localTip := n.ledgerState.Tip()
						localTipSlot := localTip.Point.Slot
						if n.chainSelector != nil {
							n.chainSelector.SetLocalTip(localTip)
							if k := n.ledgerState.SecurityParam(); k > 0 {
								n.chainSelector.SetSecurityParam(uint64(k)) //nolint:gosec
							}
						}
						n.processChainsyncRecyclerTick(
							now,
							localTipSlot,
							chainsyncCfg,
							recycleAt,
							lastRecycled,
							&lastProgressSlot,
							&lastProgressAt,
							plateauRecoveryThreshold,
							grace,
							cooldown,
						)
					})
				}
			}
		}) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (n *Node) waitChainsyncStallRecycler() {
	// This wait is intentionally not bounded by the shutdown timeout: advancing
	// while the recycler is still active can race dependency teardown.
	n.chainsyncStallRecyclerWG.Wait()
}

func (n *Node) processChainsyncRecyclerTick(
	now time.Time,
	localTipSlot uint64,
	chainsyncCfg chainsync.Config,
	recycleAt map[string]time.Time,
	lastRecycled map[string]time.Time,
	lastProgressSlot *uint64,
	lastProgressAt *time.Time,
	plateauRecoveryThreshold time.Duration,
	grace time.Duration,
	cooldown time.Duration,
) {
	if localTipSlot > *lastProgressSlot {
		*lastProgressSlot = localTipSlot
		*lastProgressAt = now
	}
	// During catch-up, extend all recycling thresholds to avoid
	// churning connections while the node is making progress.
	// Connection recycling during bulk sync causes pipeline resets,
	// TIME_WAIT socket exhaustion, and dropped rollbacks that slow
	// catch-up far more than the stall itself.
	catchUpMultiplier := 1
	if n.ledgerState != nil && !n.ledgerState.IsAtTip() {
		catchUpMultiplier = 5
	}
	effectiveGrace := time.Duration(catchUpMultiplier) * grace
	effectivePlateau := time.Duration(catchUpMultiplier) * plateauRecoveryThreshold
	effectiveCooldown := time.Duration(catchUpMultiplier) * cooldown
	n.chainsyncState.CheckStalledClients()
	// Rotate the round-robin header-ingress driver on the stall-check
	// cadence. No-op under the primary/parallel strategies.
	n.chainsyncState.AdvanceHeaderSyncRotation()
	trackedClients := n.chainsyncState.GetTrackedClients()
	trackedByID := make(
		map[string]chainsync.TrackedClient,
		len(trackedClients),
	)
	eligibleCount := 0
	for _, conn := range trackedClients {
		connKey := conn.ConnId.String()
		trackedByID[connKey] = conn
		if conn.Status != chainsync.ClientStatusStalled {
			delete(recycleAt, connKey)
		}
		if !conn.ObservabilityOnly {
			eligibleCount++
		}
	}
	// Prune expired cooldown entries so this map does
	// not grow without bound over long runtimes.
	for connKey, last := range lastRecycled {
		if now.Sub(last) >= effectiveCooldown {
			delete(lastRecycled, connKey)
		}
	}
	// Safety net: if local tip has not moved for a long time
	// while peers are ahead, recycle the selected chainsync
	// connection even if it is not marked stalled.
	if n.chainSelector != nil {
		if bestPeer := n.chainSelector.GetBestPeer(); bestPeer != nil {
			if bestPeerTip := n.chainSelector.GetPeerTip(*bestPeer); bestPeerTip != nil &&
				bestPeerTip.Tip.Point.Slot > localTipSlot {
				targetConn := n.chainsyncState.GetClientConnId()
				if targetConn == nil {
					targetCopy := *bestPeer
					targetConn = &targetCopy
				}
				connKey := targetConn.String()
				var lastRecycledAt *time.Time
				if last, ok := lastRecycled[connKey]; ok {
					lastCopy := last
					lastRecycledAt = &lastCopy
				}
				if shouldRecycleLocalTipPlateau(
					now,
					*lastProgressAt,
					localTipSlot,
					bestPeerTip.Tip.Point.Slot,
					lastRecycledAt,
					effectiveCooldown,
					effectivePlateau,
				) {
					// First, always attempt the LOCAL ledger reconcile.
					// It repairs a silent primary-chain / ledger
					// divergence (chain.Tip() advanced, or the ledger
					// tip fell off the primary chain, while the ledger
					// pipeline stayed pinned on an abandoned fork so
					// fetched blocks "do not fit on current chain tip")
					// by rolling the ledger back to the latest common
					// ancestor. The two call sites in
					// ledger/chainsync.go only fire on
					// ErrRollbackExceedsSecurityParam; sub-K fork
					// resolutions leave no error to trigger reconcile.
					// This is a purely local repair -- it does not touch
					// the peer connection -- so it is safe, and the only
					// available recovery, even with a single eligible
					// upstream (e.g. a one-relay devnet/leios topology),
					// which would otherwise wedge here permanently.
					reconciledByLedger := false
					reconcileFailed := false
					if n.ledgerState != nil {
						reconciled, err := n.ledgerState.ReconcileLivePrimaryChainLedgerDivergence(
							"local tip plateau",
							*targetConn,
						)
						if err != nil {
							reconcileFailed = true
							n.config.logger.Warn(
								"plateau reconcile failed",
								"connection_id", connKey,
								"error", err.Error(),
							)
						} else if reconciled {
							n.config.logger.Warn(
								"local tip plateau resolved via ledger reconcile",
								"connection_id", connKey,
								"local_tip_slot", localTipSlot,
								"best_peer_tip_slot", bestPeerTip.Tip.Point.Slot,
								"plateau_duration", now.Sub(*lastProgressAt),
							)
							// Reset the plateau clock so we don't
							// immediately re-trigger on the next tick
							// before forward application has had a chance
							// to advance the ledger.
							*lastProgressAt = now
							lastRecycled[connKey] = now
							reconciledByLedger = true
						}
					}
					// A local-tip plateau on Leios is usually the ledger
					// pipeline replaying a backlog of already-fetched blocks,
					// not a stalled header stream. When the primary chain is
					// already caught up to the peer, recycling the (healthy)
					// chainsync connection cannot advance the applied tip and
					// only churns the connection -- dropped pipelines,
					// MustReply timeouts from ingress backpressure, and
					// TIME_WAIT/goroutine growth. Only trust this heuristic
					// after the local reconcile had a chance to repair, or at
					// least rule out, a primary-chain / ledger divergence.
					var primaryChainTipSlot uint64
					if n.ledgerState != nil {
						primaryChainTipSlot = n.ledgerState.PrimaryChainTipSlot()
					}
					isBacklog := isLedgerApplicationBacklog(
						localTipSlot,
						primaryChainTipSlot,
						bestPeerTip.Tip.Point.Slot,
					)
					if reconciledByLedger {
						// Reconciliation already reset the plateau clock and
						// recorded cooldown. Give ledger replay a chance to
						// resume from the repaired tip before trying any
						// connection-level recovery.
					} else if isBacklog && !reconcileFailed {
						// The header chain is already caught up to the peer;
						// the plateau is the ledger pipeline draining a
						// backlog of already-fetched blocks. Recycling the
						// chainsync stream cannot help and only churns the
						// connection, so leave it running and let the pipeline
						// advance. Surfacing this at INFO also stops a wedged
						// ledger pipeline from being masked as a chainsync
						// stall.
						n.config.logger.Info(
							"local tip plateau is a ledger-application backlog; header chain already caught up, not recycling chainsync",
							"connection_id", connKey,
							"applied_tip_slot", localTipSlot,
							"primary_chain_tip_slot", primaryChainTipSlot,
							"best_peer_tip_slot", bestPeerTip.Tip.Point.Slot,
							"plateau_duration", now.Sub(*lastProgressAt),
						)
						// Reset the plateau clock so we re-evaluate only after
						// another full plateau window instead of every tick
						// while the ledger pipeline drains.
						*lastProgressAt = now
						delete(recycleAt, connKey)
					} else {
						// The local reconcile found nothing to repair (or
						// failed), so the stall is in the upstream chainsync
						// stream itself: the active peer's server-side cursor
						// has stopped advancing (a flaky/stalled relay) while
						// chain selection still tracks it AT a higher tip.
						//
						// A plateau resync is NOT a peer recycle. Recycling
						// (the stalled path below) drops the peer and fails
						// over to a SPARE, so it genuinely needs a spare and
						// is suppressed at eligibleCount <= 1. A plateau
						// resync instead closes the connection so peer
						// governance reconnects to the SAME remote and
						// re-enters FindIntersect with fresh intersect points
						// anchored at the current local tip (see
						// chainsyncResyncRequiresFreshConnection). That is
						// exactly the recovery a single-peer plateau needs:
						// it restarts header delivery from local-tip+1 on the
						// only upstream we have. The plateau predicate
						// (peer ahead AND no local progress for the full
						// plateau threshold) plus the recycle cooldown gate
						// this so a healthy single peer is never churned.
						n.config.logger.Warn(
							"local tip plateau detected, resyncing chainsync client",
							"connection_id", connKey,
							"local_tip_slot", localTipSlot,
							"best_peer_tip_slot", bestPeerTip.Tip.Point.Slot,
							"plateau_duration", now.Sub(*lastProgressAt),
							"eligible_peer_count", eligibleCount,
						)
						n.eventBus.Publish(
							event.ChainsyncResyncEventType,
							event.NewEvent(
								event.ChainsyncResyncEventType,
								event.ChainsyncResyncEvent{
									ConnectionId: *targetConn,
									Reason:       event.ChainsyncResyncReasonLocalTipPlateau,
								},
							),
						)
						// Realign only matters when there are spare peers
						// whose cursors raced ahead while the active peer was
						// stuck; with a single eligible peer it is a no-op.
						if eligibleCount > 1 {
							n.realignOtherPeersAfterPlateau(
								*targetConn,
								trackedClients,
								localTipSlot,
							)
						}
						delete(recycleAt, connKey)
						lastRecycled[connKey] = now
						*lastProgressAt = now
					}
				}
			}
		}
	}
	for _, conn := range trackedClients {
		if conn.Status != chainsync.ClientStatusStalled {
			continue
		}
		connKey := conn.ConnId.String()
		desiredDueAt := now.Add(effectiveGrace)
		if dueAt, exists := recycleAt[connKey]; !exists {
			recycleAt[connKey] = desiredDueAt
			n.config.logger.Info(
				"chainsync client stalled, scheduling guarded recycle",
				"connection_id", connKey,
				"stall_timeout", chainsyncCfg.StallTimeout,
				"grace_period", effectiveGrace,
			)
		} else if dueAt.After(desiredDueAt) {
			// Shrink deadline when transitioning from catch-up
			// to at-tip so stalls aren't delayed unnecessarily.
			recycleAt[connKey] = desiredDueAt
		}
	}
	for connKey, dueAt := range recycleAt {
		if now.Before(dueAt) {
			continue
		}
		tracked, ok := trackedByID[connKey]
		if !ok || tracked.Status != chainsync.ClientStatusStalled {
			delete(recycleAt, connKey)
			continue
		}
		connId := tracked.ConnId
		if last, ok := lastRecycled[connKey]; ok &&
			now.Sub(last) < effectiveCooldown {
			recycleAt[connKey] = now.Add(effectiveCooldown - now.Sub(last))
			continue
		}
		// Never recycle the only eligible peer. A block producer
		// with a single relay would lose its only propagation
		// path during the reconnect window. Observability-only
		// connections are not eligible, so recycling them does
		// not reduce the eligible count.
		if eligibleCount <= 1 && !tracked.ObservabilityOnly {
			n.config.logger.Warn(
				"chainsync client stalled but is only eligible peer, skipping recycle",
				"connection_id", connKey,
				"stall_timeout", chainsyncCfg.StallTimeout,
			)
			recycleAt[connKey] = now.Add(grace)
			continue
		}
		active := n.chainsyncState.GetClientConnId()
		if active == nil {
			// If no active client is selected and this client
			// is overdue + stalled, recycle to force a fresh
			// connection attempt and avoid indefinite stalls.
			n.config.logger.Warn(
				"chainsync client stalled with no active selection, recycling connection",
				"connection_id", connKey,
				"stall_timeout", chainsyncCfg.StallTimeout,
				"grace_period", grace,
				"recycle_cooldown", cooldown,
			)
			n.eventBus.PublishAsync(
				connmanager.ConnectionRecycleRequestedEventType,
				event.NewEvent(
					connmanager.ConnectionRecycleRequestedEventType,
					connmanager.ConnectionRecycleRequestedEvent{
						ConnectionId: connId,
						ConnKey:      connKey,
						Reason:       "stalled_connection_no_active_selection",
					},
				),
			)
			delete(recycleAt, connKey)
			lastRecycled[connKey] = now
			continue
		}
		if active.String() != connKey {
			// Don't recycle non-primary stalled clients. Keep state clean.
			n.eventBus.PublishAsync(
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
			delete(recycleAt, connKey)
			continue
		}
		n.config.logger.Warn(
			"chainsync client stalled, recycling active connection",
			"connection_id", connKey,
			"stall_timeout", chainsyncCfg.StallTimeout,
			"grace_period", grace,
			"recycle_cooldown", cooldown,
		)
		n.eventBus.PublishAsync(
			connmanager.ConnectionRecycleRequestedEventType,
			event.NewEvent(
				connmanager.ConnectionRecycleRequestedEventType,
				connmanager.ConnectionRecycleRequestedEvent{
					ConnectionId: connId,
					ConnKey:      connKey,
					Reason:       "stalled_active_connection",
				},
			),
		)
		delete(recycleAt, connKey)
		lastRecycled[connKey] = now
	}
}

// realignOtherPeersAfterPlateau requests a fresh-connection chainsync
// resync for every ingress-eligible tracked peer
// other than the one being closed for plateau. Without realignment, a
// peer that has been streaming RollForwards while the active peer was
// stuck holds a server-side cursor far past our local tip; the chain
// selector will promote one of these peers as the next active, and its
// next RollForward delivers a header beyond the local block tip with
// no in-memory ancestor history to bridge the gap. The local fork
// resolver then fails and closes that peer too, cycling through peers
// until process restart. Realigning candidate peers' cursors to the
// current local tip lets whichever peer is promoted next deliver
// headers from local-tip+1 onward.
func (n *Node) realignOtherPeersAfterPlateau(
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
		n.config.logger.Info(
			"realigning peer chainsync cursor after plateau",
			"connection_id", conn.ConnId.String(),
			"cursor_slot", conn.Cursor.Slot,
			"local_tip_slot", localTipSlot,
		)
		n.eventBus.Publish(
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

func (n *Node) handleChainSwitchEvent(evt event.Event) {
	e, ok := evt.Data.(chainselection.ChainSwitchEvent)
	if !ok {
		return
	}
	prevConn := "(none)"
	if e.PreviousConnectionId.LocalAddr != nil &&
		e.PreviousConnectionId.RemoteAddr != nil {
		prevConn = e.PreviousConnectionId.String()
	}
	n.config.logger.Info(
		"chain switch: updating active connection",
		"previous_connection", prevConn,
		"new_connection", e.NewConnectionId.String(),
		"new_tip_block", e.NewTip.BlockNumber,
		"new_tip_slot", e.NewTip.Point.Slot,
	)
	// Peer switches only change which already-running chainsync stream feeds
	// the ledger. Restarting chainsync here re-enters FindIntersect and can
	// race the protocol state machine under load.
	n.chainsyncState.SetClientConnId(e.NewConnectionId)
}

// handleChainSelectedNoneEvent logs a selected-to-none transition. Chain
// selection has stalled with no eligible/corroborated peer; under Genesis
// corroboration the stalled source's blocks are already withheld from the ledger
// by the ChainsyncApplyEligible gate, so this is observability only.
func (n *Node) handleChainSelectedNoneEvent(evt event.Event) {
	e, ok := evt.Data.(chainselection.ChainSelectedNoneEvent)
	if !ok {
		return
	}
	prevConn := "(none)"
	if e.PreviousConnectionId.LocalAddr != nil &&
		e.PreviousConnectionId.RemoteAddr != nil {
		prevConn = e.PreviousConnectionId.String()
	}
	n.config.logger.Info(
		"chain selection stalled: no selectable peer",
		"previous_connection", prevConn,
		"genesis_corroboration", e.GenesisCorroboration,
	)
}

func (n *Node) runStallCheckerTick(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			n.config.logger.Error(
				"panic in stall checker tick, continuing",
				"panic", r,
				"stack", string(stack),
			)
		}
	}()
	fn()
}

func (n *Node) runStallCheckerLoop(fn func()) (recovered bool) {
	defer func() {
		if r := recover(); r != nil {
			recovered = true
			stack := debug.Stack()
			n.config.logger.Error(
				"panic in stall checker goroutine",
				"panic", r,
				"stack", string(stack),
			)
		}
	}()
	fn()
	return false
}
