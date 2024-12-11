// Copyright 2024 Blink Labs Software
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
	"encoding/hex"
	"fmt"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/state"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	chainsyncIntersectPointCount = 100
)

func (n *Node) chainsyncServerConnOpts() []ochainsync.ChainSyncOptionFunc {
	return []ochainsync.ChainSyncOptionFunc{
		ochainsync.WithFindIntersectFunc(n.chainsyncServerFindIntersect),
		ochainsync.WithRequestNextFunc(n.chainsyncServerRequestNext),
	}
}

func (n *Node) chainsyncClientConnOpts() []ochainsync.ChainSyncOptionFunc {
	return []ochainsync.ChainSyncOptionFunc{
		ochainsync.WithRollForwardFunc(n.chainsyncClientRollForward),
		ochainsync.WithRollBackwardFunc(n.chainsyncClientRollBackward),
		// Enable pipelining of RequestNext messages to speed up chainsync
		ochainsync.WithPipelineLimit(10),
	}
}

func (n *Node) chainsyncClientStart(connId ouroboros.ConnectionId) error {
	conn := n.connManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf("failed to lookup connection ID: %s", connId.String())
	}
	intersectPoints, err := n.ledgerState.RecentChainPoints(
		chainsyncIntersectPointCount,
	)
	if err != nil {
		return err
	}
	// Determine start point if we have no stored chain points
	if len(intersectPoints) == 0 {
		if n.config.intersectTip {
			// Start initial chainsync from current chain tip
			tip, err := conn.ChainSync().Client.GetCurrentTip()
			if err != nil {
				return err
			}
			intersectPoints = append(
				intersectPoints,
				tip.Point,
			)
			return conn.ChainSync().Client.Sync(intersectPoints)
		} else if len(n.config.intersectPoints) > 0 {
			// Start initial chainsync at specific point(s)
			intersectPoints = append(
				intersectPoints,
				n.config.intersectPoints...,
			)
		}
	}
	return conn.ChainSync().Client.Sync(intersectPoints)
}

func (n *Node) chainsyncServerFindIntersect(
	ctx ochainsync.CallbackContext,
	points []ocommon.Point,
) (ocommon.Point, ochainsync.Tip, error) {
	n.ledgerState.RLock()
	defer n.ledgerState.RUnlock()
	var retPoint ocommon.Point
	var retTip ochainsync.Tip
	// Find intersection
	intersectPoint, err := n.ledgerState.GetIntersectPoint(points)
	if err != nil {
		return retPoint, retTip, err
	}

	// Populate return tip
	retTip = n.ledgerState.Tip()

	if intersectPoint == nil {
		return retPoint, retTip, ochainsync.IntersectNotFoundError
	}

	// Add our client to the chainsync state
	_, err = n.chainsyncState.AddClient(
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

func (n *Node) chainsyncServerRequestNext(
	ctx ochainsync.CallbackContext,
) error {
	// Create/retrieve chainsync state for connection
	tip := n.ledgerState.Tip()
	clientState, err := n.chainsyncState.AddClient(
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
		return err
	}
	if next != nil {
		return ctx.Server.RollForward(
			next.Block.Type,
			next.Block.Cbor,
			tip,
		)
	}
	// Send AwaitReply
	if err := ctx.Server.AwaitReply(); err != nil {
		return err
	}
	// Wait for next block and send
	go func() {
		next, _ := clientState.ChainIter.Next(true)
		if next == nil {
			return
		}
		tip := n.ledgerState.Tip()
		_ = ctx.Server.RollForward(
			next.Block.Type,
			next.Block.Cbor,
			tip,
		)
	}()
	return nil
}

func (n *Node) chainsyncClientRollBackward(
	ctx ochainsync.CallbackContext,
	point ocommon.Point,
	tip ochainsync.Tip,
) error {
	// Generate event
	n.eventBus.Publish(
		state.ChainsyncEventType,
		event.NewEvent(
			state.ChainsyncEventType,
			state.ChainsyncEvent{
				Rollback: true,
				Point:    point,
				Tip:      tip,
			},
		),
	)
	return nil
}

func (n *Node) chainsyncClientRollForward(
	ctx ochainsync.CallbackContext,
	blockType uint,
	blockData interface{},
	tip ochainsync.Tip,
) error {
	switch v := blockData.(type) {
	case ledger.BlockHeader:
		blockSlot := v.SlotNumber()
		blockHash, _ := hex.DecodeString(v.Hash())
		n.eventBus.Publish(
			state.ChainsyncEventType,
			event.NewEvent(
				state.ChainsyncEventType,
				state.ChainsyncEvent{
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
