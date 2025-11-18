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
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/connmanager"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	oblockfetch "github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const blockBatchSize = 256

// BlockfetchServerConnOpts returns server connection options for the BlockFetch protocol.
func BlockfetchServerConnOpts(
	requestRangeFunc oblockfetch.RequestRangeFunc,
) []oblockfetch.BlockFetchOptionFunc {
	return []oblockfetch.BlockFetchOptionFunc{
		oblockfetch.WithRequestRangeFunc(requestRangeFunc),
	}
}

// BlockfetchClientConnOpts returns client connection options for the BlockFetch protocol.
func BlockfetchClientConnOpts(
	blockFunc oblockfetch.BlockFunc,
	batchDoneFunc oblockfetch.BatchDoneFunc,
) []oblockfetch.BlockFetchOptionFunc {
	return []oblockfetch.BlockFetchOptionFunc{
		oblockfetch.WithBlockFunc(blockFunc),
		oblockfetch.WithBatchDoneFunc(batchDoneFunc),
		oblockfetch.WithBatchStartTimeout(2 * time.Second),
		oblockfetch.WithBlockTimeout(2 * time.Second),
		// Set the recv queue size to 2x our block batch size
		oblockfetch.WithRecvQueueSize(2 * blockBatchSize),
	}
}

// BlockfetchClientRequestRange requests a range of blocks from the server.
func BlockfetchClientRequestRange(
	connManager *connmanager.ConnectionManager,
	connId ConnectionId,
	start ocommon.Point,
	end ocommon.Point,
) error {
	conn := connManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf("failed to lookup connection ID: %s", connId.String())
	}
	bf := conn.BlockFetch()
	if bf == nil {
		return fmt.Errorf(
			"connection %s has no BlockFetch client",
			connId.String(),
		)
	}
	if bf.Client == nil {
		return fmt.Errorf(
			"BlockFetch client not available on connection: %s",
			connId.String(),
		)
	}
	return bf.Client.GetBlockRange(start, end)
}

// BlockfetchServerRequestRange handles block range requests from clients.
func BlockfetchServerRequestRange(
	ctx oblockfetch.CallbackContext,
	node NodeInterface,
	start ocommon.Point,
	end ocommon.Point,
) error {
	// TODO: check if we have requested block range available and send NoBlocks if not (#397)
	chainIter, err := node.LedgerState().GetChainFromPoint(start, true)
	if err != nil {
		return err
	}
	// Start async process to send requested block range
	go func() {
		conn := node.ConnManager().GetConnectionById(ctx.ConnectionId)
		if conn == nil {
			node.Config().
				Logger().
				Error("failed to lookup connection ID", "connectionId", ctx.ConnectionId.String())
			return
		}
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
		if err := ctx.Server.StartBatch(); err != nil {
			node.Config().
				Logger().
				Error("error in StartBatch", "error", err)
			return
		}
		for {
			next, err := chainIter.Next(iterCtx, false)
			if err != nil {
				if errors.Is(err, context.Canceled) ||
					errors.Is(err, context.DeadlineExceeded) {
					// Connection or server context shutdown; treat as normal termination.
					return
				}
				if !errors.Is(err, chain.ErrIteratorChainTip) {
					node.Config().
						Logger().
						Error("error in chain iterator", "error", err)
					return
				}
				break
			}
			if next == nil {
				break
			}
			if next.Rollback {
				// Rollback while serving a range; abort the batch.
				node.Config().
					Logger().
					Error("rollback encountered during BlockFetch", "connectionId", ctx.ConnectionId.String())
				return
			}
			if next.Block.Slot > end.Slot {
				break
			}
			blockBytes := next.Block.Cbor
			if err := ctx.Server.Block(next.Block.Type, blockBytes); err != nil {
				node.Config().
					Logger().
					Error("error in Block", "error", err)
				return
			}
			// Make sure we don't hang waiting for the next block if we've already hit the end
			if next.Block.Slot == end.Slot {
				break
			}
		}
		if err := ctx.Server.BatchDone(); err != nil {
			node.Config().
				Logger().
				Error("error in BatchDone", "error", err)
		}
	}()
	return nil
}

// BlockfetchClientBlock processes received blocks from the server.
func BlockfetchClientBlock(
	ctx oblockfetch.CallbackContext,
	node NodeInterface,
	blockType uint,
	block gledger.Block,
) error {
	// Generate event
	node.EventBus().Publish(
		ledger.BlockfetchEventType,
		event.NewEvent(
			ledger.BlockfetchEventType,
			ledger.BlockfetchEvent{
				ConnectionId: ctx.ConnectionId,
				Point: ocommon.NewPoint(
					block.SlotNumber(),
					block.Hash().Bytes(),
				),
				Type:  blockType,
				Block: block,
			},
		),
	)
	return nil
}

// BlockfetchClientBatchDone handles batch completion notifications.
func BlockfetchClientBatchDone(
	ctx oblockfetch.CallbackContext,
	node NodeInterface,
) error {
	// Generate event
	node.EventBus().Publish(
		ledger.BlockfetchEventType,
		event.NewEvent(
			ledger.BlockfetchEventType,
			ledger.BlockfetchEvent{
				ConnectionId: ctx.ConnectionId,
				BatchDone:    true,
			},
		),
	)
	return nil
}
