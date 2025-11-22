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
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

func (o *Ouroboros) blockfetchServerConnOpts() []blockfetch.BlockFetchOptionFunc {
	return []blockfetch.BlockFetchOptionFunc{
		blockfetch.WithRequestRangeFunc(o.blockfetchServerRequestRange),
	}
}

func (o *Ouroboros) blockfetchClientConnOpts() []blockfetch.BlockFetchOptionFunc {
	return []blockfetch.BlockFetchOptionFunc{
		blockfetch.WithBlockFunc(o.blockfetchClientBlock),
		blockfetch.WithBatchDoneFunc(o.blockfetchClientBatchDone),
		blockfetch.WithBatchStartTimeout(2 * time.Second),
		blockfetch.WithBlockTimeout(2 * time.Second),
	}
}

func (o *Ouroboros) blockfetchServerRequestRange(
	ctx blockfetch.CallbackContext,
	start ocommon.Point,
	end ocommon.Point,
) error {
	// TODO: check if we have requested block range available and send NoBlocks if not (#397)
	chainIter, err := o.LedgerState.GetChainFromPoint(start, true)
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
			blockBytes := next.Block.Cbor
			err := ctx.Server.Block(
				next.Block.Type,
				blockBytes,
			)
			if err != nil {
				// TODO: push this error somewhere (#398)
				return
			}
			if o.metrics != nil {
				o.metrics.servedBlockCount.Inc()
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

// BlockfetchClientRequestRange is called by the ledger when it needs to request a range of block bodies
func (o *Ouroboros) BlockfetchClientRequestRange(
	connId ouroboros.ConnectionId,
	start ocommon.Point,
	end ocommon.Point,
) error {
	if o.ConnManager == nil {
		return errors.New("ConnManager not initialized")
	}
	conn := o.ConnManager.GetConnectionById(connId)
	if conn == nil {
		return fmt.Errorf("failed to lookup connection ID: %s", connId.String())
	}
	// Record start time for metrics
	if o.metrics != nil {
		o.blockFetchMutex.Lock()
		o.blockFetchStarts[connId] = time.Now()
		o.blockFetchMutex.Unlock()
	}
	if err := conn.BlockFetch().Client.GetBlockRange(start, end); err != nil {
		// Clean up start time on error to prevent stale entries
		if o.metrics != nil {
			o.blockFetchMutex.Lock()
			delete(o.blockFetchStarts, connId)
			o.blockFetchMutex.Unlock()
		}
		return err
	}
	return nil
}

func (o *Ouroboros) blockfetchClientBlock(
	ctx blockfetch.CallbackContext,
	blockType uint,
	block gledger.Block,
) error {
	// Update metrics
	if o.metrics != nil {
		o.blockFetchMutex.Lock()
		startTime, exists := o.blockFetchStarts[ctx.ConnectionId]
		o.blockFetchMutex.Unlock()
		if exists {
			fetchDuration := time.Since(startTime)
			fetchSeconds := fetchDuration.Seconds()

			// Calculate block delay as wallclock time minus block slot time (cardano-node compatible)
			var delaySeconds float64
			if o.LedgerState != nil {
				blockSlotTime, err := o.LedgerState.SlotToTime(
					block.SlotNumber(),
				)
				if err == nil {
					delaySeconds = time.Since(blockSlotTime).Seconds()
				} else {
					// Fallback to fetch time if slot conversion fails
					delaySeconds = fetchSeconds
				}
			} else {
				// Fallback to fetch time if no ledger state
				delaySeconds = fetchSeconds
			}

			o.metrics.blockDelay.Set(delaySeconds)
			atomic.AddInt64(&o.metrics.totalBlocksFetched, 1)
			if delaySeconds < 1.0 {
				atomic.AddInt64(&o.metrics.blocksUnder1s, 1)
			} else if delaySeconds < 3.0 {
				atomic.AddInt64(&o.metrics.blocksUnder3s, 1)
			} else if delaySeconds < 5.0 {
				atomic.AddInt64(&o.metrics.blocksUnder5s, 1)
			} else {
				// delaySeconds >= 5.0
				o.metrics.lateBlocks.Inc()
			}
			// Update CDF percentages based on block delay
			total := atomic.LoadInt64(&o.metrics.totalBlocksFetched)
			if total > 0 {
				under1 := atomic.LoadInt64(&o.metrics.blocksUnder1s)
				under3 := atomic.LoadInt64(&o.metrics.blocksUnder3s)
				under5 := atomic.LoadInt64(&o.metrics.blocksUnder5s)
				o.metrics.blockDelayCdfOne.Set(
					float64(under1) / float64(total) * 100,
				)
				o.metrics.blockDelayCdfThree.Set(
					float64(under3) / float64(total) * 100,
				)
				o.metrics.blockDelayCdfFive.Set(
					float64(under5) / float64(total) * 100,
				)
			}
		}
	}
	// Generate event
	o.EventBus.Publish(
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

func (o *Ouroboros) blockfetchClientBatchDone(
	ctx blockfetch.CallbackContext,
) error {
	// Clean up start time
	if o.metrics != nil {
		o.blockFetchMutex.Lock()
		delete(o.blockFetchStarts, ctx.ConnectionId)
		o.blockFetchMutex.Unlock()
	}
	// Generate event
	o.EventBus.Publish(
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
