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

package ouroboros

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/chainselection"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	chainsyncIntersectPointCount = 100

	// chainsyncRestartTimeout bounds how long the restart of a
	// chainsync client can take before we give up and close the
	// connection. Increase this for slow or congested networks.
	chainsyncRestartTimeout = 30 * time.Second
)

func (o *Ouroboros) chainsyncServerConnOpts() []ochainsync.ChainSyncOptionFunc {
	return []ochainsync.ChainSyncOptionFunc{
		ochainsync.WithFindIntersectFunc(o.chainsyncServerFindIntersect),
		ochainsync.WithRequestNextFunc(o.chainsyncServerRequestNext),
		// Increase intersect timeout from the 10s default. Downstream
		// peers may send FindIntersect with many points during initial
		// sync, and processing can be slow under load.
		ochainsync.WithIntersectTimeout(30 * time.Second),
		// Set idle timeout to 1 hour. The spec default (3673s per
		// Table 3.8) is similar, but we set it explicitly to keep
		// server connections alive during periods of low block
		// production (e.g. DevNets with low activeSlotsCoeff).
		ochainsync.WithIdleTimeout(1 * time.Hour),
	}
}

func (o *Ouroboros) chainsyncClientConnOpts() []ochainsync.ChainSyncOptionFunc {
	return []ochainsync.ChainSyncOptionFunc{
		ochainsync.WithRollForwardFunc(o.chainsyncClientRollForward),
		ochainsync.WithRollBackwardFunc(o.chainsyncClientRollBackward),
		// Pipeline enough headers to keep one blockfetch batch (500
		// blocks) ready while the previous batch processes. A depth
		// of 10 is sufficient; higher values flood the header queue
		// and waste CPU parsing headers that are immediately dropped.
		ochainsync.WithPipelineLimit(10),
		// Recv queue at 2x pipeline limit to absorb bursts without
		// blocking the protocol goroutine.
		ochainsync.WithRecvQueueSize(20),
		// Increase the intersect timeout from the 5s default. The
		// upstream peer may need time to process FindIntersect when
		// under load (e.g. fast DevNet block production or initial
		// sync from genesis).
		ochainsync.WithIntersectTimeout(30 * time.Second),
	}
}

func normalizeIntersectPoints(points []ocommon.Point) []ocommon.Point {
	if len(points) == 0 {
		return nil
	}
	result := make([]ocommon.Point, 0, len(points))
	seen := make(map[string]struct{}, len(points))
	for _, point := range points {
		key := fmt.Sprintf("%d:%x", point.Slot, point.Hash)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, point)
	}
	return result
}

func isOriginPoint(point ocommon.Point) bool {
	return point.Slot == 0 && len(point.Hash) == 0
}

func sameConnectionId(a, b ouroboros.ConnectionId) bool {
	if a.LocalAddr == nil && a.RemoteAddr == nil {
		return b.LocalAddr == nil && b.RemoteAddr == nil
	}
	if b.LocalAddr == nil && b.RemoteAddr == nil {
		return false
	}
	return a.String() == b.String()
}

func (o *Ouroboros) buildDefaultChainsyncIntersectPoints(
	connId ouroboros.ConnectionId,
) ([]ocommon.Point, error) {
	if o.LedgerState == nil {
		return nil, errors.New("ledger state not available")
	}
	conn := o.ConnManager.GetConnectionById(connId)
	if conn == nil {
		return nil, fmt.Errorf("failed to lookup connection ID: %s", connId.String())
	}
	if conn.ChainSync() == nil || conn.ChainSync().Client == nil {
		return nil, fmt.Errorf(
			"ChainSync client not available on connection: %s",
			connId.String(),
		)
	}
	intersectPoints, err := o.LedgerState.IntersectPoints(
		chainsyncIntersectPointCount,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"LedgerState.IntersectPoints failed: %w",
			err,
		)
	}
	// Determine start point if we have no stored chain points
	if len(intersectPoints) == 0 {
		if o.config.IntersectTip {
			// Start initial chainsync from current chain tip
			tip, err := conn.ChainSync().Client.GetCurrentTip()
			if err != nil {
				return nil, fmt.Errorf(
					"ChainSync.Client.GetCurrentTip failed: %w",
					err,
				)
			}
			intersectPoints = append(intersectPoints, tip.Point)
		} else if len(o.config.IntersectPoints) > 0 {
			// Start initial chainsync at specific point(s)
			intersectPoints = append(
				intersectPoints,
				o.config.IntersectPoints...,
			)
		}
	}
	intersectPoints = normalizeIntersectPoints(intersectPoints)
	// Always include origin as the last intersect point. This
	// ensures FindIntersect succeeds even when the peer follows
	// a different fork (e.g. multi-producer DevNet). Without
	// origin, peers on divergent chains have no common point.
	if len(intersectPoints) == 0 ||
		!isOriginPoint(intersectPoints[len(intersectPoints)-1]) {
		intersectPoints = append(intersectPoints, ocommon.NewPointOrigin())
	}
	return intersectPoints, nil
}

func (o *Ouroboros) syncChainsyncClient(
	connId ouroboros.ConnectionId,
	intersectPoints []ocommon.Point,
) error {
	conn := o.ConnManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf("failed to lookup connection ID: %s", connId.String())
	}
	if conn.ChainSync() == nil || conn.ChainSync().Client == nil {
		return fmt.Errorf(
			"ChainSync client not available on connection: %s",
			connId.String(),
		)
	}
	intersectPoints = normalizeIntersectPoints(intersectPoints)
	if len(intersectPoints) == 0 {
		intersectPoints = []ocommon.Point{ocommon.NewPointOrigin()}
	}
	if o.PeerGov != nil {
		o.PeerGov.SetPeerHotByConnId(connId)
	}
	return conn.ChainSync().Client.Sync(intersectPoints)
}

// RestartChainsyncClient restarts the chainsync client on an existing
// connection without closing the TCP connection. This avoids disrupting
// other mini-protocols (blockfetch, txsubmission, keepalive) and
// prevents the relay's muxer from seeing unexpected bearer closures.
//
// The caller must stop the chainsync client first (which sends MsgDone).
// This function then performs: Start (re-register with muxer) → Sync
// (FindIntersect + RequestNext). The server will send RollBackward if
// the intersection point is behind the client's current position, which
// triggers the normal rollback handler.
func (o *Ouroboros) RestartChainsyncClient(connId ouroboros.ConnectionId) error {
	intersectPoints, err := o.buildDefaultChainsyncIntersectPoints(connId)
	if err != nil {
		return fmt.Errorf(
			"build default chainsync intersect points: %w",
			err,
		)
	}
	return o.RestartChainsyncClientWithPoints(connId, intersectPoints)
}

// RestartChainsyncClientWithPoints restarts the chainsync client on an
// existing connection and begins syncing from the specified intersect point(s).
func (o *Ouroboros) RestartChainsyncClientWithPoints(
	connId ouroboros.ConnectionId,
	intersectPoints []ocommon.Point,
) error {
	conn := o.ConnManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf("connection not found: %s", connId.String())
	}
	cs := conn.ChainSync()
	if cs == nil || cs.Client == nil {
		return fmt.Errorf(
			"chainsync client not available: %s",
			connId.String(),
		)
	}
	// Start re-registers the protocol with the muxer (handles
	// stopped→starting→running transition internally).
	cs.Client.Start()
	if err := o.syncChainsyncClient(connId, intersectPoints); err != nil {
		return fmt.Errorf(
			"chainsync restart failed for conn %v: %w",
			connId, err,
		)
	}
	return nil
}

func (o *Ouroboros) resyncChainsyncClientWithPoints(
	connId ouroboros.ConnectionId,
	intersectPoints []ocommon.Point,
) error {
	conn := o.ConnManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf("connection not found: %s", connId.String())
	}
	cs := conn.ChainSync()
	if cs == nil || cs.Client == nil {
		return fmt.Errorf(
			"chainsync client not available: %s",
			connId.String(),
		)
	}
	if err := cs.Client.Stop(); err != nil {
		return fmt.Errorf(
			"stop chainsync client for conn %s: %w",
			connId.String(),
			err,
		)
	}
	return o.RestartChainsyncClientWithPoints(connId, intersectPoints)
}

func (o *Ouroboros) chainsyncClientStart(connId ouroboros.ConnectionId) error {
	intersectPoints, err := o.buildDefaultChainsyncIntersectPoints(connId)
	if err != nil {
		return fmt.Errorf(
			"build default chainsync intersect points for start: %w",
			err,
		)
	}
	if err := o.syncChainsyncClient(connId, intersectPoints); err != nil {
		return fmt.Errorf("sync chainsync client: %w", err)
	}
	return nil
}

func (o *Ouroboros) chainsyncServerFindIntersect(
	ctx ochainsync.CallbackContext,
	points []ocommon.Point,
) (ocommon.Point, ochainsync.Tip, error) {
	var retPoint ocommon.Point
	o.config.Logger.Debug(
		"chainsync server: FindIntersect callback entered",
		"component", "ouroboros",
		"connection_id", ctx.ConnectionId.String(),
		"num_points", len(points),
	)
	tip := o.LedgerState.Tip()
	o.config.Logger.Debug(
		"chainsync server: got tip",
		"component", "ouroboros",
		"tip_slot", tip.Point.Slot,
	)
	intersectPoint, err := o.LedgerState.GetIntersectPoint(points)
	if err != nil {
		o.config.Logger.Error(
			"chainsync server: GetIntersectPoint error",
			"component", "ouroboros",
			"error", err,
		)
		return retPoint, tip, fmt.Errorf("get intersect point: %w", err)
	}
	o.config.Logger.Debug(
		"chainsync server: GetIntersectPoint done",
		"component", "ouroboros",
		"found", intersectPoint != nil,
	)
	if intersectPoint == nil {
		return retPoint, tip, ochainsync.ErrIntersectNotFound
	}
	// Add our client to the chainsync state
	_, err = o.ChainsyncState.AddClient(
		ctx.ConnectionId,
		*intersectPoint,
	)
	if err != nil {
		return retPoint, tip, fmt.Errorf(
			"add chainsync client for connection %s: %w",
			ctx.ConnectionId.String(),
			err,
		)
	}
	retPoint = *intersectPoint
	return retPoint, tip, nil
}

// refreshTip returns the current ledger tip, ensuring it is never behind
// the block being sent. The chain iterator delivers blocks before the
// ledger processes them, so the tip can be stale. A tip slot behind the
// block slot is a protocol violation that causes peers to disconnect.
func (o *Ouroboros) refreshTip(next *chain.ChainIteratorResult) ochainsync.Tip {
	tip := o.LedgerState.Tip()
	if !next.Rollback && next.Point.Slot > tip.Point.Slot {
		tip = ochainsync.Tip{
			Point:       next.Point,
			BlockNumber: next.Block.Number,
		}
	}
	return tip
}

func (o *Ouroboros) chainsyncServerRequestNext(
	ctx ochainsync.CallbackContext,
) error {
	// Create/retrieve chainsync state for connection
	tip := o.LedgerState.Tip()
	clientState, err := o.ChainsyncState.AddClient(
		ctx.ConnectionId,
		tip.Point,
	)
	if err != nil {
		return fmt.Errorf(
			"add chainsync client for connection %s: %w",
			ctx.ConnectionId.String(),
			err,
		)
	}
	if clientState.NeedsInitialRollback {
		o.config.Logger.Debug(
			"chainsync server: initial rollback",
			"connection_id", ctx.ConnectionId.String(),
			"cursor_slot", clientState.Cursor.Slot,
		)
		err := ctx.Server.RollBackward(
			clientState.Cursor,
			tip,
		)
		if err != nil {
			return err
		}
		clientState.NeedsInitialRollback = false
		return nil
	}
	// Check for available block
	next, err := clientState.ChainIter.Next(false)
	if err != nil {
		if !errors.Is(err, chain.ErrIteratorChainTip) {
			return err
		}
	}
	if next != nil {
		tip = o.refreshTip(next)
		if next.Rollback {
			err = ctx.Server.RollBackward(
				next.Point,
				tip,
			)
		} else {
			err = ctx.Server.RollForward(
				next.Block.Type,
				next.Block.Cbor,
				tip,
			)
		}
		return err
	}
	// Send AwaitReply
	o.config.Logger.Debug(
		"chainsync server: sending AwaitReply",
		"connection_id", ctx.ConnectionId.String(),
		"tip_slot", tip.Point.Slot,
	)
	if err := ctx.Server.AwaitReply(); err != nil {
		return err
	}
	// Wait for next block and send
	conn := o.ConnManager.GetConnectionById(ctx.ConnectionId)
	if conn == nil {
		return fmt.Errorf("connection %s not found", ctx.ConnectionId.String())
	}
	go func() {
		// Wait for next block in a separate goroutine so we can
		// also monitor the connection for errors. This avoids
		// leaking the monitor goroutine when Next returns first.
		done := make(chan struct{})
		var next *chain.ChainIteratorResult
		var nextErr error
		go func() {
			defer close(done)
			next, nextErr = clientState.ChainIter.Next(true)
		}()
		select {
		case <-done:
			// Iterator returned
		case <-conn.ErrorChan():
			clientState.ChainIter.Cancel()
			return
		}
		if nextErr != nil {
			// Don't log context.Canceled errors as they're
			// expected during connection closure.
			if !errors.Is(nextErr, context.Canceled) {
				o.config.Logger.Debug(
					"failed to get next block from chain iterator",
					"error", nextErr,
				)
			}
			return
		}
		if next == nil {
			o.config.Logger.Debug(
				"chainsync server: goroutine got nil block",
				"connection_id", ctx.ConnectionId.String(),
			)
			return
		}
		tip := o.refreshTip(next)
		if next.Rollback {
			if err := ctx.Server.RollBackward(
				next.Point,
				tip,
			); err != nil {
				o.config.Logger.Error(
					"failed to roll backward",
					"error", err,
				)
			}
		} else {
			if err := ctx.Server.RollForward(
				next.Block.Type,
				next.Block.Cbor,
				tip,
			); err != nil {
				o.config.Logger.Error(
					"failed to roll forward",
					"error", err,
				)
			}
		}
	}()
	return nil
}

func (o *Ouroboros) chainsyncClientRollBackward(
	ctx ochainsync.CallbackContext,
	point ocommon.Point,
	tip ochainsync.Tip,
) error {
	if !o.reconcileChainsyncIngressAdmission(
		ctx.ConnectionId,
		o.shouldPublishChainsyncToLedger(ctx.ConnectionId),
	) {
		return nil
	}
	// Generate event
	o.EventBus.Publish(
		ledger.ChainsyncEventType,
		event.NewEvent(
			ledger.ChainsyncEventType,
			ledger.ChainsyncEvent{
				ConnectionId: ctx.ConnectionId,
				Rollback:     true,
				Point:        point,
				Tip:          tip,
			},
		),
	)
	o.EventBus.Publish(
		chainselection.PeerRollbackEventType,
		event.NewEvent(
			chainselection.PeerRollbackEventType,
			chainselection.PeerRollbackEvent{
				ConnectionId: ctx.ConnectionId,
				Point:        point,
				Tip:          tip,
			},
		),
	)
	return nil
}

func (o *Ouroboros) chainsyncClientRollForward(
	ctx ochainsync.CallbackContext,
	blockType uint,
	blockData any,
	tip ochainsync.Tip,
) error {
	switch v := blockData.(type) {
	case gledger.BlockHeader:
		blockSlot := v.SlotNumber()
		blockHash := v.Hash().Bytes()
		point := ocommon.NewPoint(blockSlot, blockHash)
		// Extract VRF output from block header once for chain
		// selection tie-breaking (used in both dedup and normal
		// paths below).
		vrfOutput := chainselection.GetVRFOutput(v)
		// Ingress eligibility is the sole gate for feeding the ledger
		// and chain selection. reconcileChainsyncIngressAdmission
		// defers to ChainsyncIngressEligible (peergov), which already
		// filters random inbound peers via chainSelectionEligible.
		// A full-duplex inbound from a configured upstream remains
		// eligible here, so the node doesn't stall when the relay
		// dials us first after a crash.
		ingressEligible := o.reconcileChainsyncIngressAdmission(
			ctx.ConnectionId,
			o.shouldPublishChainsyncToLedger(ctx.ConnectionId),
		)
		o.config.Logger.Debug(
			"chainsync: header received",
			"component", "ouroboros",
			"slot", blockSlot,
			"tip_slot", tip.Point.Slot,
			"connection_id", ctx.ConnectionId.String(),
			"ingress_eligible", ingressEligible,
		)
		// Update tracked client state and deduplicate headers.
		// If this header has already been reported by another
		// eligible client, skip publishing it into the ledger.
		isNew := true
		if o.ChainsyncState != nil {
			if ingressEligible {
				isNew = o.ChainsyncState.UpdateClientTip(
					ctx.ConnectionId,
					point,
					tip,
				)
			} else {
				o.ChainsyncState.UpdateClientTipWithoutDedup(
					ctx.ConnectionId,
					point,
					tip,
				)
			}
		}
		// Publish peer tip update for chain selection only for
		// ingress-eligible peers. Random inbound peers reporting
		// ephemeral tips would cause spurious chain switches; peergov
		// filters them via chainSelectionEligible so they fail the
		// reconcile above and get skipped here.
		if ingressEligible {
			observedTip := ochainsync.Tip{
				Point:       point,
				BlockNumber: v.BlockNumber(),
			}
			o.EventBus.Publish(
				chainselection.PeerTipUpdateEventType,
				event.NewEvent(
					chainselection.PeerTipUpdateEventType,
					chainselection.PeerTipUpdateEvent{
						ConnectionId: ctx.ConnectionId,
						Tip:          tip,
						ObservedTip:  observedTip,
						VRFOutput:    vrfOutput,
					},
				),
			)
		}
		if ingressEligible && o.ChainsyncState != nil {
			o.ChainsyncState.RecordObservedHeader(
				ledger.ChainsyncEvent{
					ConnectionId: ctx.ConnectionId,
					Point:        point,
					Type:         blockType,
					BlockHeader:  v,
					Tip:          tip,
				},
			)
		}
		if !ingressEligible {
			o.config.Logger.Debug(
				"chainsync: header dropped (not ingress eligible)",
				"component", "ouroboros",
				"slot", blockSlot,
				"connection_id", ctx.ConnectionId.String(),
			)
			o.updateChainsyncMetrics(ctx.ConnectionId, tip)
			return nil
		}
		if !isNew {
			shouldReplayDuplicate := false
			if o.ChainsyncState != nil {
				activeConnId := o.ChainsyncState.GetClientConnId()
				if activeConnId != nil &&
					sameConnectionId(*activeConnId, ctx.ConnectionId) &&
					o.ChainsyncState.HeaderPreviouslySeenFromOtherConn(
						ctx.ConnectionId,
						point,
					) {
					shouldReplayDuplicate = true
				}
			}
			if !shouldReplayDuplicate {
				o.config.Logger.Debug(
					"chainsync: header dropped (duplicate)",
					"component", "ouroboros",
					"slot", blockSlot,
					"connection_id", ctx.ConnectionId.String(),
				)
				o.updateChainsyncMetrics(ctx.ConnectionId, tip)
				return nil
			}
		}
		o.EventBus.Publish(
			ledger.ChainsyncEventType,
			event.NewEvent(
				ledger.ChainsyncEventType,
				ledger.ChainsyncEvent{
					ConnectionId: ctx.ConnectionId,
					Point:        point,
					Type:         blockType,
					BlockHeader:  v,
					Tip:          tip,
				},
			),
		)
		if point.Slot == tip.Point.Slot &&
			bytes.Equal(point.Hash, tip.Point.Hash) {
			if o.ChainsyncState != nil {
				o.ChainsyncState.MarkClientSynced(ctx.ConnectionId)
			}
			if ingressEligible && o.EventBus != nil {
				o.EventBus.Publish(
					ledger.ChainsyncAwaitReplyEventType,
					event.NewEvent(
						ledger.ChainsyncAwaitReplyEventType,
						ledger.ChainsyncAwaitReplyEvent{
							ConnectionId: ctx.ConnectionId,
						},
					),
				)
			}
		}
		// Update ChainSync performance metrics for peer scoring
		o.updateChainsyncMetrics(ctx.ConnectionId, tip)
	default:
		return fmt.Errorf("unexpected block data type: %T", v)
	}
	return nil
}

// shouldPublishChainsyncToLedger reports whether headers from connId should
// feed the ledger / chain selector. When ChainsyncIngressEligible is wired,
// it is always authoritative. When no policy is wired we fall back to the
// tracked client's recorded direction: outbound-started chainsync defaults
// to eligible (legacy behaviour) while inbound-started chainsync defaults
// to observability-only. The inbound default is intentionally fail-closed
// so a misconfigured node cannot accept chain headers from random inbound
// peers that happen to negotiate full-duplex.
func (o *Ouroboros) shouldPublishChainsyncToLedger(
	connId ouroboros.ConnectionId,
) bool {
	if o.config.ChainsyncIngressEligible != nil {
		return o.config.ChainsyncIngressEligible(connId)
	}
	if o.ChainsyncState == nil {
		return false
	}
	outbound, exists := o.ChainsyncState.ClientStartedAsOutbound(connId)
	return exists && outbound
}

// isInboundChainsyncClient returns true if the chainsync client for
// connId was started on an inbound connection. This uses the tracked
// client's recorded direction instead of connmanager.IsInboundConnection,
// making it immune to ConnectionId collisions under listen-port reuse.
// Returns true (treat as inbound) if the client is not tracked.
//
//nolint:unused // Retained as a test helper and for future diagnostics.
func (o *Ouroboros) isInboundChainsyncClient(
	connId ouroboros.ConnectionId,
) bool {
	if o.ChainsyncState == nil {
		return false
	}
	outbound, exists := o.ChainsyncState.ClientStartedAsOutbound(connId)
	if !exists {
		// Unknown client — treat as inbound (conservative: don't
		// feed untracked connections into the ledger).
		return true
	}
	return !outbound
}

func (o *Ouroboros) maxTrackedChainsyncClients() int {
	maxClients := defaultMaxChainsyncClients
	if o.ChainsyncState != nil && o.ChainsyncState.MaxClients() > 0 {
		maxClients = o.ChainsyncState.MaxClients()
	}
	return maxClients
}

func (o *Ouroboros) registerTrackedChainsyncClient(
	connId ouroboros.ConnectionId,
	ingressEligible bool,
	startedAsOutbound bool,
) bool {
	if o.ChainsyncState == nil {
		return false
	}
	if ingressEligible {
		if o.ChainsyncState.TryAddClientConnIdWithDirection(
			connId,
			o.maxTrackedChainsyncClients(),
			startedAsOutbound,
		) {
			return true
		}
		if o.ChainsyncState.HasClientConnId(connId) {
			o.ChainsyncState.SetClientStartedAsOutbound(
				connId,
				startedAsOutbound,
			)
			observabilityOnly, exists := o.ChainsyncState.ClientObservabilityOnly(
				connId,
			)
			if !exists {
				return false
			}
			if observabilityOnly {
				return o.reconcileChainsyncIngressAdmission(connId, true)
			}
			return true
		}
		return false
	}
	if o.ChainsyncState.TryAddObservedClientConnIdWithDirection(
		connId,
		startedAsOutbound,
	) {
		return true
	}
	return o.reconcileChainsyncIngressAdmission(connId, false)
}

func (o *Ouroboros) reconcileChainsyncIngressAdmission(
	connId ouroboros.ConnectionId,
	desiredEligible bool,
) bool {
	if o.ChainsyncState == nil {
		return desiredEligible
	}
	observabilityOnly, exists := o.ChainsyncState.ClientObservabilityOnly(
		connId,
	)
	if !exists {
		return false
	}
	if desiredEligible {
		if !observabilityOnly {
			return true
		}
		if !o.ChainsyncState.SetClientObservabilityOnly(connId, false) {
			return false
		}
		observabilityOnly, exists = o.ChainsyncState.ClientObservabilityOnly(
			connId,
		)
		return exists && !observabilityOnly
	}
	if !observabilityOnly {
		_ = o.ChainsyncState.SetClientObservabilityOnly(connId, true)
	}
	return false
}

// updateChainsyncMetrics calculates and updates ChainSync performance metrics
// for the given peer connection. This is called on each RollForward event.
func (o *Ouroboros) updateChainsyncMetrics(
	connId ouroboros.ConnectionId,
	peerTip ochainsync.Tip,
) {
	if o.PeerGov == nil || o.LedgerState == nil {
		return
	}

	now := time.Now()

	// Get or create stats for this connection
	o.chainsyncMutex.Lock()
	stats, exists := o.chainsyncStats[connId]
	if !exists {
		stats = &chainsyncPeerStats{
			lastObservationTime: now,
			headerCount:         0,
		}
		o.chainsyncStats[connId] = stats
	}

	// Increment header count
	stats.headerCount++

	// Calculate header rate over the observation period
	// We update the peer score periodically (at least 1 second between updates)
	// to avoid excessive computation on every header
	elapsed := now.Sub(stats.lastObservationTime)
	if elapsed < time.Second {
		o.chainsyncMutex.Unlock()
		return
	}

	// Calculate headers per second
	headerRate := float64(stats.headerCount) / elapsed.Seconds()

	// Reset counters for next observation period
	stats.headerCount = 0
	stats.lastObservationTime = now
	o.chainsyncMutex.Unlock()

	// Calculate tip delta (our tip slot - peer's tip slot)
	// Positive means peer is behind us, negative means peer is ahead
	ourTip := o.LedgerState.Tip()
	// Use signed subtraction to handle the delta correctly
	// Slots are uint64, but the difference fits in int64 for reasonable cases
	// Cap at math.MaxInt64 to avoid overflow
	var tipDelta int64
	if ourTip.Point.Slot >= peerTip.Point.Slot {
		diff := ourTip.Point.Slot - peerTip.Point.Slot
		if diff > math.MaxInt64 {
			tipDelta = math.MaxInt64
		} else {
			tipDelta = int64(diff) //nolint:gosec // overflow handled above
		}
	} else {
		diff := peerTip.Point.Slot - ourTip.Point.Slot
		if diff > math.MaxInt64 {
			tipDelta = math.MinInt64
		} else {
			tipDelta = -int64(diff) //nolint:gosec // overflow handled above
		}
	}

	// Update peer scoring
	o.PeerGov.UpdatePeerChainSyncObservation(connId, headerRate, tipDelta)
}

func (o *Ouroboros) restartChainsyncClientAsync(
	ctx context.Context,
	connId ouroboros.ConnectionId,
	reason string,
	restartFn func() error,
) {
	conn := o.ConnManager.GetConnectionById(connId)
	if conn == nil {
		return
	}
	go func() {
		// Serialize restarts for the same connection to prevent
		// overlapping stop/restart goroutines.
		muVal, _ := o.restartMu.LoadOrStore(
			connId, &sync.Mutex{},
		)
		mu := muVal.(*sync.Mutex)
		mu.Lock()

		o.config.Logger.Info(
			"restarting chainsync client",
			"connection_id", connId.String(),
			"reason", reason,
		)
		var closeOnce sync.Once
		closeConn := func() {
			closeOnce.Do(func() { conn.Close() })
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			if err := restartFn(); err != nil {
				o.config.Logger.Warn(
					"chainsync restart failed, closing connection",
					"error", err,
					"connection_id", connId.String(),
					"reason", reason,
				)
				closeConn()
			}
		}()
		select {
		case <-done:
		case <-ctx.Done():
			o.config.Logger.Info(
				"node shutting down, aborting chainsync restart",
				"connection_id", connId.String(),
				"reason", reason,
			)
			closeConn()
			<-done
		case <-time.After(chainsyncRestartTimeout):
			o.config.Logger.Warn(
				"chainsync restart timed out, closing connection",
				"connection_id", connId.String(),
				"reason", reason,
			)
			closeConn()
			<-done
		}
		mu.Unlock()
	}()
}

// SubscribeChainsyncResync registers an EventBus subscriber that
// handles chainsync re-sync events. Ordinary resyncs restart the
// ChainSync mini-protocol on the existing bearer after resetting
// local dedup state. Local authoritative rollbacks additionally
// rewind tracked client cursors and attempt ledger-side recovery
// before recycling affected connections as a fallback.
func (o *Ouroboros) SubscribeChainsyncResync(ctx context.Context) {
	if o.EventBus == nil {
		return
	}
	o.EventBus.SubscribeFunc(
		event.ChainsyncResyncEventType,
		func(evt event.Event) {
			e, ok := evt.Data.(event.ChainsyncResyncEvent)
			if !ok {
				return
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
			var connIds []ouroboros.ConnectionId
			if e.ConnectionId != (ouroboros.ConnectionId{}) {
				connIds = append(connIds, e.ConnectionId)
			} else if o.ChainsyncState != nil {
				connIds = o.ChainsyncState.RewindTrackedClientsTo(e.Point)
				if e.Reason == "local ledger rollback" &&
					len(connIds) == 0 {
					connIds = o.ChainsyncState.GetClientConnIds()
				}
			}
			if o.ChainsyncState != nil {
				if e.Point.Slot > 0 || len(e.Point.Hash) > 0 {
					o.ChainsyncState.ClearSeenHeadersFrom(e.Point.Slot)
				} else {
					o.ChainsyncState.ClearSeenHeaders()
				}
			}
			if e.Reason == "local ledger rollback" {
				if o.LedgerState == nil {
					return
				}
				recovery := o.LedgerState.RecoverAfterLocalRollback(
					connIds,
					e.Point,
				)
				if recovery.Recovered || len(connIds) == 0 {
					return
				}
				if recovery.SkipConnectionClose {
					o.config.Logger.Info(
						"skipping connection closure: chain already past rollback point",
						"component", "ouroboros",
						"rollback_slot", e.Point.Slot,
						"chain_tip_slot", recovery.PrimaryChainTipSlot,
					)
					return
				}
				// No recoverable peer history — close the affected
				// connections so peer governance reconnects and starts
				// fresh chainsync from the updated intersect points.
				// Previous approach tried Stop→Start→Sync on each
				// connection, but Stop() blocks for up to 30s when the
				// protocol is in MustReply state (waiting for a server
				// response). All connections would timeout and be closed
				// anyway, wasting 30s during which stale events
				// accumulated and prevented recovery after reconnect.
				// RecoverAfterLocalRollback already cleaned up blockfetch
				// state, so there are no in-flight lookups to break.
				o.config.Logger.Info(
					"local rollback had no recoverable peer history, closing connections for fresh chainsync",
					"component", "ouroboros",
					"rollback_slot", e.Point.Slot,
					"connection_count", len(connIds),
				)
				for _, connId := range connIds {
					if o.ChainsyncState != nil {
						o.ChainsyncState.ClearObservedHeaderHistory(connId)
					}
					if o.ConnManager == nil {
						continue
					}
					conn := o.ConnManager.GetConnectionById(connId)
					if conn == nil {
						continue
					}
					o.config.Logger.Info(
						"closing connection for fresh chainsync after local rollback",
						"component", "ouroboros",
						"connection_id", connId.String(),
					)
					conn.Close()
				}
				return
			}
			if len(connIds) == 0 {
				return
			}
			// Plateau and unresolved fork events close the connection
			// immediately rather than attempting an in-place
			// Stop→Start→Sync restart. Stop() blocks for up to 30s
			// when the protocol is in MustReply state, during which
			// no recovery can happen. Closing lets peer governance
			// reconnect with a fresh bearer and updated intersect
			// points.
			if e.Reason == "local_tip_plateau" ||
				e.Reason == "rollback point not found" ||
				e.Reason == "persistent chain fork" {
				for _, connId := range connIds {
					if o.ChainsyncState != nil {
						o.ChainsyncState.ClearObservedHeaderHistory(connId)
					}
					if o.ConnManager == nil {
						continue
					}
					conn := o.ConnManager.GetConnectionById(connId)
					if conn == nil {
						continue
					}
					o.config.Logger.Info(
						"closing stalled connection for fresh chainsync",
						"connection_id", connId.String(),
						"reason", e.Reason,
					)
					conn.Close()
				}
				return
			}
			for _, connId := range connIds {
				if o.ChainsyncState != nil {
					o.ChainsyncState.ClearObservedHeaderHistory(connId)
				}
				if o.ConnManager == nil {
					continue
				}
				o.restartChainsyncClientAsync(
					ctx,
					connId,
					e.Reason,
					func() error {
						intersectPoints, err := o.buildDefaultChainsyncIntersectPoints(
							connId,
						)
						if err != nil {
							return fmt.Errorf(
								"build default chainsync intersect points: %w",
								err,
							)
						}
						return o.resyncChainsyncClientWithPoints(
							connId,
							intersectPoints,
						)
					},
				)
			}
		},
	)
}
