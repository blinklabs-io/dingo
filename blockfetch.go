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
	"fmt"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func (n *Node) blockfetchServerConnOpts() []blockfetch.BlockFetchOptionFunc {
	return []blockfetch.BlockFetchOptionFunc{
		blockfetch.WithRequestRangeFunc(n.blockfetchServerRequestRange),
	}
}

func (n *Node) blockfetchClientConnOpts() []blockfetch.BlockFetchOptionFunc {
	return []blockfetch.BlockFetchOptionFunc{
		blockfetch.WithBlockFunc(n.blockfetchClientBlock),
		blockfetch.WithBatchDoneFunc(n.blockfetchClientBatchDone),
		blockfetch.WithBatchStartTimeout(2 * time.Second),
		blockfetch.WithBlockTimeout(2 * time.Second),
		// Set the recv queue size to 2x our block batch size
		blockfetch.WithRecvQueueSize(1000),
	}
}

func (n *Node) blockfetchServerRequestRange(
	ctx blockfetch.CallbackContext,
	start ocommon.Point,
	end ocommon.Point,
) error {
	// TODO: check if we have requested block range available and send NoBlocks if not (#397)
	chainIter, err := n.ledgerState.GetChainFromPoint(start, true)
	if err != nil {
		return err
	}
	// Start async process to send requested block range
	go func() {
		if err := ctx.Server.StartBatch(); err != nil {
			return
		}
		for {
			next, _ := chainIter.Next(false)
			if next == nil {
				break
			}
			if next.Block.Slot > end.Slot {
				break
			}
			blockBytes := next.Block.Cbor[:]
			err := ctx.Server.Block(
				next.Block.Type,
				blockBytes,
			)
			if err != nil {
				// TODO: push this error somewhere (#398)
				return
			}
			// Make sure we don't hang waiting for the next block if we've already hit the end
			if next.Block.Slot == end.Slot {
				break
			}
		}
		if err := ctx.Server.BatchDone(); err != nil {
			return
		}
	}()
	return nil
}

// blockfetchClientRequestRange is called by the ledger when it needs to request a range of block bodies
func (n *Node) blockfetchClientRequestRange(
	connId ouroboros.ConnectionId,
	start ocommon.Point,
	end ocommon.Point,
) error {
	conn := n.connManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf("failed to lookup connection ID: %s", connId.String())
	}
	if err := conn.BlockFetch().Client.GetBlockRange(start, end); err != nil {
		return err
	}
	return nil
}

func (n *Node) blockfetchClientBlock(
	ctx blockfetch.CallbackContext,
	blockType uint,
	block gledger.Block,
) error {
	// Generate event
	n.eventBus.Publish(
		ledger.BlockfetchEventType,
		event.NewEvent(
			ledger.BlockfetchEventType,
			ledger.BlockfetchEvent{
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

func (n *Node) blockfetchClientBatchDone(
	ctx blockfetch.CallbackContext,
) error {
	// Generate event
	n.eventBus.Publish(
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
