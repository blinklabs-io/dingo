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
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/state"

	ouroboros "github.com/blinklabs-io/gouroboros"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	oblockfetch "github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func (n *Node) blockfetchServerConnOpts() []oblockfetch.BlockFetchOptionFunc {
	return []oblockfetch.BlockFetchOptionFunc{
		oblockfetch.WithRequestRangeFunc(n.blockfetchServerRequestRange),
	}
}

func (n *Node) blockfetchClientConnOpts() []oblockfetch.BlockFetchOptionFunc {
	return []oblockfetch.BlockFetchOptionFunc{
		oblockfetch.WithBlockFunc(n.blockfetchClientBlock),
		oblockfetch.WithBatchDoneFunc(n.blockfetchClientBatchDone),
		oblockfetch.WithBatchStartTimeout(2 * time.Second),
		oblockfetch.WithBlockTimeout(2 * time.Second),
	}
}

func (n *Node) blockfetchServerRequestRange(
	ctx oblockfetch.CallbackContext,
	start ocommon.Point,
	end ocommon.Point,
) error {
	// TODO: check if we have requested block range available and send NoBlocks if not
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
				// TODO: push this error somewhere
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
	block ledger.Block,
) error {
	// Generate event
	blkHash, err := hex.DecodeString(block.Hash())
	if err != nil {
		return fmt.Errorf("decode block hash: %w", err)
	}
	n.eventBus.Publish(
		state.BlockfetchEventType,
		event.NewEvent(
			state.BlockfetchEventType,
			state.BlockfetchEvent{
				Point: ocommon.NewPoint(block.SlotNumber(), blkHash),
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
		state.BlockfetchEventType,
		event.NewEvent(
			state.BlockfetchEventType,
			state.BlockfetchEvent{
				ConnectionId: ctx.ConnectionId,
				BatchDone:    true,
			},
		),
	)
	return nil
}
