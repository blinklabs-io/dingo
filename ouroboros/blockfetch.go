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
	"time"

	"github.com/blinklabs-io/dingo/chain"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/dingo/ledger"
	ouroboros "github.com/blinklabs-io/gouroboros"
	gledger "github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/protocol/blockfetch"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// blockfetchMetricsCdfUpdateInterval controls how often CDF metrics are
// recomputed. Updating on every block is wasteful; every 32 blocks (or
// on late blocks) is sufficient for dashboard accuracy.
const blockfetchMetricsCdfUpdateInterval = 32

// MaxBlockFetchRange is the maximum slot range allowed for a single block
// fetch request. This prevents peers from requesting unbounded ranges that
// would cause the server to iterate the entire chain. The value of 129600
// corresponds to the stability window (3k/f) on mainnet (k=2160, f=0.05).
const MaxBlockFetchRange = 129600

// blockfetchMaxConsecutiveNoBlocks is the number of consecutive NoBlocks
// responses for the same (connId, start) tuple before closing the connection.
const blockfetchMaxConsecutiveNoBlocks = 5

// blockfetchServerSendDrainTimeout is the maximum time the range server waits
// for the underlying protocol send queue to drain between blockfetch messages.
const blockfetchServerSendDrainTimeout = 60 * time.Second

// blockfetchNoBlocksPoint identifies the start point used by the last
// NoBlocks response tracked for a connection.
type blockfetchNoBlocksPoint struct {
	Slot uint64
	Hash string
}

type blockfetchNoBlocksState struct {
	Point blockfetchNoBlocksPoint
	Count int
}

type blockfetchRangeIterator interface {
	Next(bool) (*chain.ChainIteratorResult, error)
	Cancel()
}

type blockfetchBatchServer interface {
	StartBatch() error
	Block(uint, []byte) error
	BatchDone() error
}

type blockfetchSendDrainWaiter interface {
	WaitSendQueueDrained(time.Duration) bool
}

type blockfetchConnection interface {
	ErrorChan() chan error
	Close() error
}

func (o *Ouroboros) blockfetchServerConnOpts() []blockfetch.BlockFetchOptionFunc {
	return []blockfetch.BlockFetchOptionFunc{
		blockfetch.WithRequestRangeFunc(
			o.instrumentBlockfetchRequestRange(o.blockfetchServerRequestRange),
		),
	}
}

func (o *Ouroboros) blockfetchClientConnOpts() []blockfetch.BlockFetchOptionFunc {
	return []blockfetch.BlockFetchOptionFunc{
		// Take the raw block callback so dingo decodes the block itself. This
		// lets the Musashi-scoped Conway-with-Leios-header decode run; the
		// decoded callback would let gouroboros' strict Conway decode fail
		// before dingo can intervene (mirrors the chain-sync header path).
		blockfetch.WithBlockRawFunc(
			o.instrumentBlockfetchBlockRaw(o.blockfetchClientBlockRaw),
		),
		blockfetch.WithBatchDoneFunc(
			o.instrumentBlockfetchBatchDone(o.blockfetchClientBatchDone),
		),
		blockfetch.WithBatchStartTimeout(60 * time.Second),
		blockfetch.WithBlockTimeout(60 * time.Second),
	}
}

// decodeBlockfetchBlock decodes a fetched block, choosing the decoder by block
// type. On the Musashi prototype network, blocks tagged Conway (block type 7)
// carry the Leios header extension (leios_certified/leios_announcement) in a
// 12-field header body that gouroboros' strict Conway decoder rejects.
// models.DecodeConwayBlock reconstructs those while preserving the original
// wire bytes so the block hash matches the chain-sync header hash. The strict
// Conway decoder that every real Conway network relies on is left untouched.
// All other networks and block types decode exactly as gouroboros would.
func (o *Ouroboros) decodeBlockfetchBlock(
	blockType uint,
	raw []byte,
) (gledger.Block, error) {
	if o.config.NetworkMagic == ouroboros.NetworkCardanoMusashi.NetworkMagic &&
		blockType == gledger.BlockTypeConway {
		return models.DecodeConwayBlock(raw)
	}
	return gledger.NewBlockFromCbor(blockType, raw)
}

// blockfetchClientBlockRaw decodes the raw fetched block (via
// decodeBlockfetchBlock) and forwards the decoded block to the shared block
// handler.
func (o *Ouroboros) blockfetchClientBlockRaw(
	ctx blockfetch.CallbackContext,
	blockType uint,
	blockData []byte,
) error {
	block, err := o.decodeBlockfetchBlock(blockType, blockData)
	if err != nil {
		return fmt.Errorf(
			"decode block-fetch block (block type %d): %w",
			blockType,
			err,
		)
	}
	return o.blockfetchClientBlock(ctx, blockType, block)
}

func (o *Ouroboros) blockfetchServerRequestRange(
	ctx blockfetch.CallbackContext,
	start ocommon.Point,
	end ocommon.Point,
) error {
	// Validate that start is not after end (#397)
	if start.Slot > end.Slot {
		o.config.Logger.Warn(
			"blockfetch: requested range has start after end, sending NoBlocks",
			"connection_id", ctx.ConnectionId.String(),
			"start_slot", start.Slot,
			"end_slot", end.Slot,
		)
		if err := ctx.Server.NoBlocks(); err != nil {
			return fmt.Errorf(
				"blockfetch NoBlocks after invalid range: %w",
				err,
			)
		}
		return nil
	}
	// Validate that the requested slot range is not too large
	slotRange := end.Slot - start.Slot
	if slotRange > MaxBlockFetchRange {
		o.config.Logger.Debug(
			"blockfetch: requested range exceeds maximum, sending NoBlocks",
			"connection_id", ctx.ConnectionId.String(),
			"start_slot", start.Slot,
			"end_slot", end.Slot,
			"slot_range", slotRange,
			"max_range", MaxBlockFetchRange,
		)
		if err := ctx.Server.NoBlocks(); err != nil {
			return fmt.Errorf(
				"blockfetch NoBlocks after oversized range: %w",
				err,
			)
		}
		o.blockfetchRecordNoBlocksAndMaybeClose(
			ctx.ConnectionId,
			start,
			"blockfetch: closing stuck peer after repeated oversized range requests",
			"blockfetch: peer stuck on oversized range",
		)
		return nil
	}
	// Validate that the start point exists in our chain (#397)
	chainIter, err := o.LedgerState.GetChainFromPoint(start, true)
	if err != nil {
		o.config.Logger.Debug(
			"blockfetch: start point not found in chain, sending NoBlocks",
			"connection_id", ctx.ConnectionId.String(),
			"start_slot", start.Slot,
			"error", err,
		)
		if err := ctx.Server.NoBlocks(); err != nil {
			return fmt.Errorf(
				"blockfetch NoBlocks after start point not found: %w",
				err,
			)
		}
		o.blockfetchRecordNoBlocksAndMaybeClose(
			ctx.ConnectionId,
			start,
			"blockfetch: closing stuck peer after repeated missing-point requests",
			"blockfetch: peer stuck on missing point",
		)
		return nil
	}
	o.blockfetchResetNoBlocks(ctx.ConnectionId)
	// Start async process to send requested block range
	go func() {
		conn := o.ConnManager.GetConnectionById(ctx.ConnectionId)
		if conn == nil {
			chainIter.Cancel()
			return
		}
		err := o.blockfetchServerSendBatch(
			ctx.ConnectionId.String(),
			start,
			end,
			chainIter,
			ctx.Server,
			conn,
		)
		if err != nil {
			o.reportBlockfetchServerAsyncError(
				conn,
				ctx.ConnectionId.String(),
				start,
				end,
				err,
			)
		}
	}()
	return nil
}

func (o *Ouroboros) blockfetchServerSendBatch(
	connectionID string,
	start ocommon.Point,
	end ocommon.Point,
	chainIter blockfetchRangeIterator,
	server blockfetchBatchServer,
	conn blockfetchConnection,
) error {
	defer chainIter.Cancel()
	if err := server.StartBatch(); err != nil {
		o.config.Logger.Error(
			"blockfetch: failed to start batch",
			"connection_id", connectionID,
			"error", err,
		)
		return fmt.Errorf("blockfetch StartBatch failed: %w", err)
	}
	if err := o.blockfetchServerWaitForSendDrain(
		connectionID,
		start,
		end,
		server,
		conn,
		"StartBatch",
	); err != nil {
		return err
	}
Loop:
	for {
		select {
		case <-conn.ErrorChan():
			return nil
		default:
			next, iterErr := chainIter.Next(false)
			if iterErr != nil {
				if errors.Is(iterErr, chain.ErrIteratorChainTip) {
					break Loop
				}
				o.config.Logger.Error(
					"blockfetch: iterator error, aborting batch",
					"connection_id", connectionID,
					"start_slot", start.Slot,
					"end_slot", end.Slot,
					"error", iterErr,
				)
				o.closeBlockfetchConnection(
					conn,
					connectionID,
					"iterator error after StartBatch",
				)
				return fmt.Errorf("blockfetch iterator failed: %w", iterErr)
			}
			if next == nil {
				break Loop
			}
			if next.Block.Slot > end.Slot {
				break Loop
			}
			blockBytes := next.Block.Cbor
			err := server.Block(
				next.Block.Type,
				blockBytes,
			)
			if err != nil {
				// After StartBatch(), the only safe recovery for a
				// failed stream is to drop the transport.
				o.config.Logger.Error(
					"blockfetch: failed to send block to peer",
					"connection_id", connectionID,
					"block_slot", next.Block.Slot,
					"start_slot", start.Slot,
					"end_slot", end.Slot,
					"error", err,
				)
				o.closeBlockfetchConnection(
					conn,
					connectionID,
					"failed to stream block after StartBatch",
				)
				return fmt.Errorf("blockfetch Block failed: %w", err)
			}
			if o.blockfetchMetrics != nil {
				o.blockfetchMetrics.servedBlockCount.Inc()
			}
			if err := o.blockfetchServerWaitForSendDrain(
				connectionID,
				start,
				end,
				server,
				conn,
				"Block",
			); err != nil {
				return err
			}
			// Make sure we don't hang waiting for the next block if we've already hit the end
			if next.Block.Slot == end.Slot {
				break Loop
			}
		}
	}
	// Signal batch completion
	if err := server.BatchDone(); err != nil {
		o.config.Logger.Error(
			"blockfetch: failed to signal batch completion",
			"connection_id", connectionID,
			"start_slot", start.Slot,
			"end_slot", end.Slot,
			"error", err,
		)
		o.closeBlockfetchConnection(
			conn,
			connectionID,
			"failed to send BatchDone after StartBatch",
		)
		return fmt.Errorf("blockfetch BatchDone failed: %w", err)
	}
	return nil
}

func (o *Ouroboros) blockfetchServerWaitForSendDrain(
	connectionID string,
	start ocommon.Point,
	end ocommon.Point,
	server blockfetchBatchServer,
	conn blockfetchConnection,
	phase string,
) error {
	drainWaiter, ok := server.(blockfetchSendDrainWaiter)
	if !ok {
		return nil
	}
	if drainWaiter.WaitSendQueueDrained(blockfetchServerSendDrainTimeout) {
		return nil
	}
	select {
	case <-conn.ErrorChan():
		return nil
	default:
	}
	o.config.Logger.Error(
		"blockfetch: send queue did not drain",
		"connection_id", connectionID,
		"start_slot", start.Slot,
		"end_slot", end.Slot,
		"phase", phase,
		"timeout", blockfetchServerSendDrainTimeout,
	)
	o.closeBlockfetchConnection(
		conn,
		connectionID,
		"blockfetch send queue did not drain",
	)
	return fmt.Errorf(
		"blockfetch send queue did not drain after %s within %s",
		phase,
		blockfetchServerSendDrainTimeout,
	)
}

func (o *Ouroboros) reportBlockfetchServerAsyncError(
	conn blockfetchConnection,
	connectionID string,
	start ocommon.Point,
	end ocommon.Point,
	err error,
) {
	o.config.Logger.Error(
		"blockfetch: async range server failed",
		"connection_id", connectionID,
		"start_slot", start.Slot,
		"end_slot", end.Slot,
		"error", err,
	)
	if !sendBlockfetchConnError(conn.ErrorChan(), err) {
		o.config.Logger.Debug(
			"blockfetch: failed to forward async server error to connection error channel",
			"connection_id", connectionID,
		)
	}
}

func sendBlockfetchConnError(errCh chan error, err error) (sent bool) {
	defer func() {
		if recover() != nil {
			sent = false
		}
	}()
	select {
	case errCh <- err:
		return true
	default:
		return false
	}
}

func (o *Ouroboros) closeBlockfetchConnection(
	conn blockfetchConnection,
	connectionID string,
	reason string,
) {
	if err := conn.Close(); err != nil {
		o.config.Logger.Debug(
			"blockfetch: failed to close connection after aborted batch",
			"connection_id", connectionID,
			"reason", reason,
			"error", err,
		)
	}
}

func (o *Ouroboros) blockfetchRecordNoBlocksAndMaybeClose(
	connId ouroboros.ConnectionId,
	start ocommon.Point,
	logMessage string,
	closeReason string,
) {
	if !o.blockfetchRecordNoBlocks(connId, start) {
		return
	}
	o.config.Logger.Warn(
		logMessage,
		"connection_id", connId.String(),
		"start_slot", start.Slot,
	)
	if o.ConnManager == nil {
		return
	}
	conn := o.ConnManager.GetConnectionById(connId)
	if conn == nil {
		return
	}
	o.closeBlockfetchConnection(conn, connId.String(), closeReason)
}

// blockfetchRecordNoBlocks increments the consecutive NoBlocks counter for
// (connId, start) and returns true when blockfetchMaxConsecutiveNoBlocks is reached.
func (o *Ouroboros) blockfetchRecordNoBlocks(
	connId ouroboros.ConnectionId,
	start ocommon.Point,
) bool {
	key := blockfetchNoBlocksPoint{Slot: start.Slot, Hash: string(start.Hash)}
	o.blockFetchMutex.Lock()
	defer o.blockFetchMutex.Unlock()
	state, ok := o.blockfetchNoBlocksCounts[connId]
	if !ok || state.Point != key {
		o.blockfetchNoBlocksCounts[connId] = blockfetchNoBlocksState{
			Point: key,
			Count: 1,
		}
		return false
	}
	state.Count++
	o.blockfetchNoBlocksCounts[connId] = state
	return state.Count >= blockfetchMaxConsecutiveNoBlocks
}

func (o *Ouroboros) blockfetchResetNoBlocks(connId ouroboros.ConnectionId) {
	o.blockFetchMutex.Lock()
	delete(o.blockfetchNoBlocksCounts, connId)
	o.blockFetchMutex.Unlock()
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
	// Record start time for metrics and scoring
	o.blockFetchMutex.Lock()
	o.blockFetchStarts[connId] = time.Now()
	o.blockFetchMutex.Unlock()
	if err := conn.BlockFetch().Client.GetBlockRange(start, end); err != nil {
		// Clean up start time and record failed observation
		o.blockFetchMutex.Lock()
		startTime, exists := o.blockFetchStarts[connId]
		delete(o.blockFetchStarts, connId)
		o.blockFetchMutex.Unlock()
		if exists && o.PeerGov != nil {
			latencyMs := time.Since(startTime).Milliseconds()
			o.PeerGov.UpdatePeerBlockFetchObservation(
				connId,
				float64(latencyMs),
				false,
			)
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
	// Update metrics and peer scoring
	o.blockFetchMutex.Lock()
	startTime, exists := o.blockFetchStarts[ctx.ConnectionId]
	o.blockFetchMutex.Unlock()
	if exists {
		fetchDuration := time.Since(startTime)

		// Only publish block delay metrics after reaching tip once.
		// During catch-up all blocks are naturally "late" relative to
		// wall-clock time, which permanently poisons the CDF.
		atTip := o.LedgerState != nil && o.LedgerState.IsAtTip()
		if atTip && o.blockfetchMetrics != nil {
			// Calculate block delay as wallclock time minus block slot time (cardano-node compatible)
			var delaySeconds float64
			if blockSlotTime, err := o.LedgerState.SlotToTime(block.SlotNumber()); err == nil {
				delaySeconds = time.Since(blockSlotTime).Seconds()
			} else {
				delaySeconds = fetchDuration.Seconds()
			}

			o.blockfetchMetrics.blockDelay.Set(delaySeconds)
			total := o.blockfetchMetrics.totalBlocksFetched.Add(1)
			// Cumulative CDF buckets: each counter includes all
			// blocks at or below its threshold.
			if delaySeconds < 1.0 {
				o.blockfetchMetrics.blocksUnder1s.Add(1)
			}
			if delaySeconds < 3.0 {
				o.blockfetchMetrics.blocksUnder3s.Add(1)
			}
			if delaySeconds < 5.0 {
				o.blockfetchMetrics.blocksUnder5s.Add(1)
			} else {
				o.blockfetchMetrics.lateBlocks.Inc()
			}
			if total == 1 ||
				total%blockfetchMetricsCdfUpdateInterval == 0 ||
				delaySeconds >= 5.0 {
				under1 := o.blockfetchMetrics.blocksUnder1s.Load()
				under3 := o.blockfetchMetrics.blocksUnder3s.Load()
				under5 := o.blockfetchMetrics.blocksUnder5s.Load()
				o.blockfetchMetrics.blockDelayCdfOne.Set(
					float64(under1) / float64(total) * 100,
				)
				o.blockfetchMetrics.blockDelayCdfThree.Set(
					float64(under3) / float64(total) * 100,
				)
				o.blockfetchMetrics.blockDelayCdfFive.Set(
					float64(under5) / float64(total) * 100,
				)
			}
		}

		if o.PeerGov != nil {
			latencyMs := fetchDuration.Milliseconds()
			o.PeerGov.UpdatePeerBlockFetchObservation(
				ctx.ConnectionId,
				float64(latencyMs),
				true,
			)
		}
	}
	if o.EventBus != nil && o.EventBus.HasSubscribers(ledger.BlockfetchEventType) {
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
	}
	return nil
}

func (o *Ouroboros) blockfetchClientBatchDone(
	ctx blockfetch.CallbackContext,
) error {
	// Clean up start time
	o.blockFetchMutex.Lock()
	delete(o.blockFetchStarts, ctx.ConnectionId)
	o.blockFetchMutex.Unlock()
	if o.EventBus != nil && o.EventBus.HasSubscribers(ledger.BlockfetchEventType) {
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
	}
	return nil
}

// instrumentBlockfetchRequestRange wraps the RequestRange callback. Note
// that blockfetchServerRequestRange validates the request synchronously
// then launches block delivery in a goroutine. The metric outcome label
// reflects only the synchronous validation result; failures during async
// streaming (iterator errors, Block, BatchDone) close the connection via
// reportBlockfetchServerAsyncError and are not visible here.
func (o *Ouroboros) instrumentBlockfetchRequestRange(
	fn func(blockfetch.CallbackContext, ocommon.Point, ocommon.Point) error,
) func(blockfetch.CallbackContext, ocommon.Point, ocommon.Point) error {
	return func(
		ctx blockfetch.CallbackContext,
		start ocommon.Point,
		end ocommon.Point,
	) error {
		startTime := time.Now()
		err := fn(ctx, start, end)
		o.recordProtocolMessage("blockfetch", err, time.Since(startTime))
		return err
	}
}

func (o *Ouroboros) instrumentBlockfetchBlockRaw(
	fn func(blockfetch.CallbackContext, uint, []byte) error,
) func(blockfetch.CallbackContext, uint, []byte) error {
	return func(
		ctx blockfetch.CallbackContext,
		blockType uint,
		blockData []byte,
	) error {
		start := time.Now()
		err := fn(ctx, blockType, blockData)
		o.recordProtocolMessage("blockfetch", err, time.Since(start))
		return err
	}
}

func (o *Ouroboros) instrumentBlockfetchBatchDone(
	fn func(blockfetch.CallbackContext) error,
) func(blockfetch.CallbackContext) error {
	return func(ctx blockfetch.CallbackContext) error {
		start := time.Now()
		err := fn(ctx)
		o.recordProtocolMessage("blockfetch", err, time.Since(start))
		return err
	}
}
