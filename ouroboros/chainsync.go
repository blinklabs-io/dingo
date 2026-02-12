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
	// Re-negotiate intersection using the same logic as initial start.
	if err := o.chainsyncClientStart(connId); err != nil {
		return fmt.Errorf(
			"chainsync restart failed for conn %v: %w",
			connId, err,
		)
	}
	return nil
}

func (o *Ouroboros) chainsyncClientStart(connId ouroboros.ConnectionId) error {
	conn := o.ConnManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf("failed to lookup connection ID: %s", connId.String())
	}
	if conn.ChainSync() == nil {
		return fmt.Errorf(
			"ChainSync protocol not available on connection: %s",
			connId.String(),
		)
	}
	intersectPoints, err := o.LedgerState.RecentChainPoints(
		chainsyncIntersectPointCount,
	)
	if err != nil {
		return err
	}
	// Determine start point if we have no stored chain points
	if len(intersectPoints) == 0 {
		if o.config.IntersectTip {
			// Start initial chainsync from current chain tip
			tip, err := conn.ChainSync().Client.GetCurrentTip()
			if err != nil {
				return err
			}
			intersectPoints = append(
				intersectPoints,
				tip.Point,
				ocommon.NewPointOrigin(),
			)
			if o.PeerGov != nil {
				o.PeerGov.SetPeerHotByConnId(connId)
			}
			return conn.ChainSync().Client.Sync(intersectPoints)
		} else if len(o.config.IntersectPoints) > 0 {
			// Start initial chainsync at specific point(s)
			intersectPoints = append(
				intersectPoints,
				o.config.IntersectPoints...,
			)
		}
	}
	// Always include origin as the last intersect point. This
	// ensures FindIntersect succeeds even when the peer follows
	// a different fork (e.g. multi-producer DevNet). Without
	// origin, peers on divergent chains have no common point.
	intersectPoints = append(intersectPoints, ocommon.NewPointOrigin())
	if o.PeerGov != nil {
		o.PeerGov.SetPeerHotByConnId(connId)
	}
	return conn.ChainSync().Client.Sync(intersectPoints)
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
		// Update tracked client state and deduplicate headers.
		// If this header has already been reported by another
		// client, skip publishing events for it.
		if o.ChainsyncState != nil {
			isNew := o.ChainsyncState.UpdateClientTip(
				ctx.ConnectionId,
				point,
				tip,
			)
			if !isNew {
				// Duplicate header already seen from another
				// client; skip downstream processing but still
				// refresh peer liveness and update metrics.
				o.EventBus.Publish(
					chainselection.PeerTipUpdateEventType,
					event.NewEvent(
						chainselection.PeerTipUpdateEventType,
						chainselection.PeerTipUpdateEvent{
							ConnectionId: ctx.ConnectionId,
							Tip:          tip,
							VRFOutput:    vrfOutput,
						},
					),
				)
				o.updateChainsyncMetrics(
					ctx.ConnectionId,
					tip,
				)
				return nil
			}
		}
		// Publish peer tip update for chain selection
		o.EventBus.Publish(
			chainselection.PeerTipUpdateEventType,
			event.NewEvent(
				chainselection.PeerTipUpdateEventType,
				chainselection.PeerTipUpdateEvent{
					ConnectionId: ctx.ConnectionId,
					Tip:          tip,
					VRFOutput:    vrfOutput,
				},
			),
		)
		// Publish chainsync event for ledger processing
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
		// Update ChainSync performance metrics for peer scoring
		o.updateChainsyncMetrics(ctx.ConnectionId, tip)
	default:
		return fmt.Errorf("unexpected block data type: %T", v)
	}
	return nil
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

// SubscribeChainsyncResync registers an EventBus subscriber that
// handles chainsync re-sync events. When a resync is requested
// (e.g. because the ledger detected a persistent fork), this
// subscriber stops the chainsync client, clears the header dedup
// cache, and restarts chainsync on the same TCP connection.
//
// This consolidates the stop/clear/restart orchestration in one
// place rather than spreading it across node.go closures.
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
			connId := e.ConnectionId
			conn := o.ConnManager.GetConnectionById(connId)
			if conn == nil {
				return
			}

			// Run stop + clear + restart asynchronously.
			// cs.Client.Stop() can block waiting for the
			// protocol to drain, and Sync() waits for a
			// FindIntersect response delivered via the
			// chainsync callback which needs the chainsync
			// mutex held by our caller. Doing either
			// synchronously would block the EventBus or
			// deadlock.
			go func() {
				// Serialize restarts for the same connection
				// to prevent overlapping stop/restart goroutines.
				muVal, _ := o.restartMu.LoadOrStore(
					connId, &sync.Mutex{},
				)
				mu := muVal.(*sync.Mutex)
				mu.Lock()

				o.config.Logger.Info(
					"restarting chainsync for re-sync",
					"connection_id", connId.String(),
					"reason", e.Reason,
				)
				var closeOnce sync.Once
				closeConn := func() {
					closeOnce.Do(func() { conn.Close() })
				}
				done := make(chan struct{})
				go func() {
					defer close(done)
					// Stop the chainsync client to halt
					// mismatching headers.
					cs := conn.ChainSync()
					if cs != nil && cs.Client != nil {
						if err := cs.Client.Stop(); err != nil {
							o.config.Logger.Warn(
								"chainsync stop failed, closing connection",
								"error", err,
								"connection_id", connId.String(),
							)
							closeConn()
							return
						}
					}
					// Clear the header dedup cache so the
					// new intersection can re-deliver
					// headers.
					if o.ChainsyncState != nil {
						o.ChainsyncState.ClearSeenHeaders()
					}
					if err := o.RestartChainsyncClient(
						connId,
					); err != nil {
						o.config.Logger.Warn(
							"chainsync restart failed, closing connection",
							"error", err,
							"connection_id", connId.String(),
						)
						closeConn()
					}
				}()
				// Hold the mutex until the restart worker
				// completes to prevent overlapping restarts.
				// On timeout or shutdown, close the connection
				// but still wait for done before releasing.
				select {
				case <-done:
				case <-ctx.Done():
					o.config.Logger.Info(
						"node shutting down, aborting chainsync restart",
						"connection_id", connId.String(),
					)
					closeConn()
					<-done
				case <-time.After(chainsyncRestartTimeout):
					o.config.Logger.Warn(
						"chainsync restart timed out, closing connection",
						"connection_id", connId.String(),
					)
					closeConn()
					<-done
				}
				mu.Unlock()
			}()
		},
	)
}
