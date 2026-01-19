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
)

func (o *Ouroboros) chainsyncServerConnOpts() []ochainsync.ChainSyncOptionFunc {
	return []ochainsync.ChainSyncOptionFunc{
		ochainsync.WithFindIntersectFunc(o.chainsyncServerFindIntersect),
		ochainsync.WithRequestNextFunc(o.chainsyncServerRequestNext),
	}
}

func (o *Ouroboros) chainsyncClientConnOpts() []ochainsync.ChainSyncOptionFunc {
	return []ochainsync.ChainSyncOptionFunc{
		ochainsync.WithRollForwardFunc(o.chainsyncClientRollForward),
		ochainsync.WithRollBackwardFunc(o.chainsyncClientRollBackward),
		// Enable pipelining of RequestNext messages to speed up chainsync
		ochainsync.WithPipelineLimit(50),
		// Set the recv queue size to 2x our pipeline limit
		ochainsync.WithRecvQueueSize(100),
	}
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
	if o.PeerGov != nil {
		o.PeerGov.SetPeerHotByConnId(connId)
	}
	return conn.ChainSync().Client.Sync(intersectPoints)
}

func (o *Ouroboros) chainsyncServerFindIntersect(
	ctx ochainsync.CallbackContext,
	points []ocommon.Point,
) (ocommon.Point, ochainsync.Tip, error) {
	o.LedgerState.RLock()
	defer o.LedgerState.RUnlock()
	var retPoint ocommon.Point
	var retTip ochainsync.Tip
	// Find intersection
	intersectPoint, err := o.LedgerState.GetIntersectPoint(points)
	if err != nil {
		return retPoint, retTip, err
	}

	// Populate return tip
	retTip = o.LedgerState.Tip()

	if intersectPoint == nil {
		return retPoint, retTip, ochainsync.ErrIntersectNotFound
	}

	// Add our client to the chainsync state
	_, err = o.ChainsyncState.AddClient(
		ctx.ConnectionId,
		*intersectPoint,
	)
	if err != nil {
		return retPoint, retTip, err
	}

	// Populate return point
	retPoint = *intersectPoint

	return retPoint, retTip, nil
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
		return err
	}
	if clientState.NeedsInitialRollback {
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
	if err := ctx.Server.AwaitReply(); err != nil {
		return err
	}
	// Wait for next block and send
	conn := o.ConnManager.GetConnectionById(ctx.ConnectionId)
	if conn == nil {
		return fmt.Errorf("connection %s not found", ctx.ConnectionId.String())
	}
	go func() {
		// Monitor connection and cancel iterator if connection fails
		go func() {
			<-conn.ErrorChan()
			clientState.ChainIter.Cancel()
		}()

		// Wait for next block from iterator
		next, err := clientState.ChainIter.Next(true)
		if err != nil {
			// Don't log context.Canceled errors as they're expected during connection closure
			if !errors.Is(err, context.Canceled) {
				o.config.Logger.Debug(
					"failed to get next block from chain iterator",
					"error",
					err,
				)
			}
			return
		}
		if next == nil {
			return
		}
		tip := o.LedgerState.Tip()
		if next.Rollback {
			if err := ctx.Server.RollBackward(
				next.Point,
				tip,
			); err != nil {
				o.config.Logger.Debug(
					"failed to roll backward",
					"error",
					err,
				)
			}
		} else {
			if err := ctx.Server.RollForward(
				next.Block.Type,
				next.Block.Cbor,
				tip,
			); err != nil {
				o.config.Logger.Debug("failed to roll forward", "error", err)
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
		// Extract VRF output from block header for chain selection tie-breaking
		vrfOutput := chainselection.GetVRFOutput(v)
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
					Point:        ocommon.NewPoint(blockSlot, blockHash),
					Type:         blockType,
					BlockHeader:  v,
					Tip:          tip,
				},
			),
		)
	default:
		return fmt.Errorf("unexpected block data type: %T", v)
	}
	return nil
}
