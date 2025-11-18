// Copyright 2025 Blink Labs Software
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
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	chainsyncIntersectPointCount = 100
)

// ChainsyncServerConnOpts returns server connection options for the ChainSync protocol.
func ChainsyncServerConnOpts(
	findIntersectFunc ochainsync.FindIntersectFunc,
	requestNextFunc ochainsync.RequestNextFunc,
) []ochainsync.ChainSyncOptionFunc {
	return []ochainsync.ChainSyncOptionFunc{
		ochainsync.WithFindIntersectFunc(findIntersectFunc),
		ochainsync.WithRequestNextFunc(requestNextFunc),
	}
}

// ChainsyncClientConnOpts returns client connection options for the ChainSync protocol.
func ChainsyncClientConnOpts(
	rollForwardFunc ochainsync.RollForwardFunc,
	rollBackwardFunc ochainsync.RollBackwardFunc,
	pipelineLimit int,
) []ochainsync.ChainSyncOptionFunc {
	return []ochainsync.ChainSyncOptionFunc{
		ochainsync.WithRollForwardFunc(rollForwardFunc),
		ochainsync.WithRollBackwardFunc(rollBackwardFunc),
		// Enable pipelining of RequestNext messages to speed up chainsync
		ochainsync.WithPipelineLimit(pipelineLimit),
		// Set the recv queue size to 2x our pipeline limit
		ochainsync.WithRecvQueueSize(2 * pipelineLimit),
	}
}

// ChainsyncClientStart starts the ChainSync client for the given connection.
func ChainsyncClientStart(
	connManager *connmanager.ConnectionManager,
	ledgerState *ledger.LedgerState,
	intersectTip bool,
	intersectPoints []ocommon.Point,
	connId ConnectionId,
) error {
	conn := connManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf("failed to lookup connection ID: %s", connId.String())
	}
	if conn.ChainSync() == nil {
		return fmt.Errorf(
			"ChainSync protocol not available on connection: %s",
			connId.String(),
		)
	}
	if conn.ChainSync().Client == nil {
		return fmt.Errorf(
			"ChainSync client not available on connection: %s",
			connId.String(),
		)
	}
	points, err := ledgerState.RecentChainPoints(
		chainsyncIntersectPointCount,
	)
	if err != nil {
		return err
	}
	// Determine start point if we have no stored chain points
	if len(points) == 0 {
		if intersectTip {
			// Start initial chainsync from current chain tip
			tip, err := conn.ChainSync().Client.GetCurrentTip()
			if err != nil {
				return err
			}
			points = append(
				points,
				tip.Point,
			)
		} else if len(intersectPoints) > 0 {
			// Start initial chainsync at specific point(s)
			points = append(
				points,
				intersectPoints...,
			)
		} else {
			return errors.New(
				"no chain points available; specify intersectTip or provide intersect points",
			)
		}
	}
	return conn.ChainSync().Client.Sync(points)
}

// ChainsyncServerFindIntersect handles the FindIntersect request from a ChainSync client.
func ChainsyncServerFindIntersect(
	ctx ochainsync.CallbackContext,
	node NodeInterface,
	points []ocommon.Point,
) (ocommon.Point, ochainsync.Tip, error) {
	var retPoint ocommon.Point
	var retTip ochainsync.Tip
	ls := node.LedgerState()
	// Find intersection and tip under read lock for consistency
	ls.RLock()
	intersectPoint, err := ls.GetIntersectPoint(points)
	if err == nil {
		retTip = ls.Tip()
	}
	ls.RUnlock()
	if err != nil {
		return retPoint, retTip, err
	}

	if intersectPoint == nil {
		return retPoint, retTip, ochainsync.ErrIntersectNotFound
	}

	// Add our client to the chainsync state
	_, err = node.ChainsyncState().AddClient(
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

// ChainsyncServerRequestNext handles the RequestNext request from a ChainSync client.
func ChainsyncServerRequestNext(
	ctx ochainsync.CallbackContext,
	node NodeInterface,
) error {
	// Create/retrieve chainsync state for connection
	tip := node.LedgerState().Tip()
	clientState, err := node.ChainsyncState().AddClient(
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
	next, err := clientState.ChainIter.Next(context.Background(), false)
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
	conn := node.ConnManager().GetConnectionById(ctx.ConnectionId)
	if conn == nil {
		return fmt.Errorf(
			"failed to lookup connection ID: %s",
			ctx.ConnectionId.String(),
		)
	}
	// Wait for next block and send
	go func() {
		iterCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			select {
			case <-conn.ErrorChan():
				cancel()
			case <-iterCtx.Done():
				return
			}
		}()
		next, err := clientState.ChainIter.Next(iterCtx, true)
		if err != nil {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				// Connection or server context shutdown; treat as normal termination.
				return
			}
			node.Config().
				Logger().
				Error("error in chain iterator", "error", err)
			return
		}
		if next == nil {
			return
		}
		tip := node.LedgerState().Tip()
		if next.Rollback {
			if err := ctx.Server.RollBackward(next.Point, tip); err != nil {
				node.Config().
					Logger().
					Error("error in RollBackward", "error", err)
			}
		} else {
			if err := ctx.Server.RollForward(next.Block.Type, next.Block.Cbor, tip); err != nil {
				node.Config().Logger().Error("error in RollForward", "error", err)
			}
		}
	}()
	return nil
}

// ChainsyncClientRollBackward handles a RollBackward message from the ChainSync server.
func ChainsyncClientRollBackward(
	ctx ochainsync.CallbackContext,
	node NodeInterface,
	point ocommon.Point,
	tip ochainsync.Tip,
) error {
	// Generate event
	node.EventBus().Publish(
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

// ChainsyncClientRollForward handles a RollForward message from the ChainSync server.
func ChainsyncClientRollForward(
	ctx ochainsync.CallbackContext,
	node NodeInterface,
	blockType uint,
	blockData any,
	tip ochainsync.Tip,
) error {
	switch v := blockData.(type) {
	case gledger.BlockHeader:
		blockSlot := v.SlotNumber()
		blockHash := v.Hash().Bytes()
		node.EventBus().Publish(
			ledger.ChainsyncEventType,
			event.NewEvent(
				ledger.ChainsyncEventType,
				ledger.ChainsyncEvent{
					ConnectionId: ctx.ConnectionId,
					Point:        ocommon.NewPoint(blockSlot, blockHash),
					Type:         blockType,
					BlockHeader:  v,
					BlockNumber:  v.BlockNumber(),
					Tip:          tip,
				},
			),
		)
	default:
		return fmt.Errorf("unexpected block data type: %T", v)
	}
	return nil
}
