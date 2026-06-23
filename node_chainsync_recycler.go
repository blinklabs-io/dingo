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
					if n.ledgerState != nil {
						reconciled, err := n.ledgerState.ReconcileLivePrimaryChainLedgerDivergence(
							"local tip plateau",
							*targetConn,
						)
						if err != nil {
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
					if !reconciledByLedger {
						// The local reconcile found nothing to repair (or
						// failed). Recycling the upstream peer only helps
						// when there is a spare eligible peer to fail over
						// to: closing the only upstream forces a reconnect
						// to the same remote on a fresh source port, which
						// cannot recover a locally-pinned tip and amplifies
						// disruption when the remote RSTs the reconnect.
						// Mirrors the stalled-recycle guard below.
						if eligibleCount <= 1 {
							n.config.logger.Warn(
								"local tip plateau detected but no spare eligible peer, skipping peer recycle",
								"connection_id", connKey,
								"local_tip_slot", localTipSlot,
								"best_peer_tip_slot", bestPeerTip.Tip.Point.Slot,
								"plateau_duration", now.Sub(*lastProgressAt),
								"eligible_peer_count", eligibleCount,
							)
							// Throttle the warning to plateau cadence
							// instead of every tick.
							*lastProgressAt = now
						} else {
							n.config.logger.Warn(
								"local tip plateau detected, resyncing chainsync client",
								"connection_id", connKey,
								"local_tip_slot", localTipSlot,
								"best_peer_tip_slot", bestPeerTip.Tip.Point.Slot,
								"plateau_duration", now.Sub(*lastProgressAt),
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
							n.realignOtherPeersAfterPlateau(
								*targetConn,
								trackedClients,
								localTipSlot,
							)
							delete(recycleAt, connKey)
							lastRecycled[connKey] = now
							*lastProgressAt = now
						}
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
