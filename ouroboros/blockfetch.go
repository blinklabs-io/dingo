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
	"bytes"
	"context"
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

// blockfetchMaxBlocksFloor is the minimum cap on blocks served for a single
// BlockFetch range request, applied regardless of the network's security
// parameter K. This prevents a peer from using a single request to make the
// server stream the entire chain, while never capping below
// ledger.BlockfetchBatchSize (500), the largest range this implementation's
// own chainsync client ever requests in one call: without this floor, a
// custom or test network configured with a small K would reject Dingo's own
// chainsync batches.
const blockfetchMaxBlocksFloor = 10 * ledger.BlockfetchBatchSize

// maxBlockFetchBlocksForSecurityParam returns the maximum number of blocks
// served for a single BlockFetch range request. The bound is on the actual
// resource cost (blocks iterated and sent) rather than on slot distance: on
// a sparse or low-active-slot-coefficient custom network, a run of
// consecutive real blocks can span far more slots than mainnet's 3k/f
// stability window, so rejecting purely on slot distance discards valid
// requests (#4354).
//
// It is not sized to Dingo's own chainsync client; it governs every peer,
// and an honest peer's candidate fragment -- and so a legitimate BlockFetch
// range -- scales with the network's own security parameter K, not with any
// one implementation's batch size. 3*K reuses the same numerator the
// removed slot-distance check (3k/f) used, reinterpreted as a block count
// once the active-slot-coefficient factor no longer applies; it is a chosen
// safety margin, not a protocol guarantee. blockfetchMaxBlocksFloor keeps a
// small-K network from capping below Dingo's own batch size.
func maxBlockFetchBlocksForSecurityParam(k int) int {
	if k < 0 {
		k = 0
	}
	if dynamic := 3 * k; dynamic > blockfetchMaxBlocksFloor {
		return dynamic
	}
	return blockfetchMaxBlocksFloor
}

// errBlockfetchRangeExceededMaxBlocks is returned by blockfetchServerSendBatch
// when a range would serve more blocks than maxBlockFetchBlocksForSecurityParam
// allows. blockfetchServerRequestRange's async goroutine reports every
// non-nil error through reportBlockfetchServerAsyncError, which logs at
// Error and closes the connection again; this sentinel lets that reporter
// recognize the case already fully handled (WARN logged, connection closed)
// at the point the cap was hit, and skip the redundant Error log and second
// Close() call.
//
// In practice this should be rare: blockfetchServerRequestRange rejects an
// oversized range with NoBlocks before StartBatch for any range whose block
// count is knowable up front, so an honest peer gets a recoverable signal
// instead of reaching this backstop. It stays as defense in depth for cases
// the up-front check cannot cover, such as a concurrent rollback changing
// the chain after validation or a Byron-EBB block-number tie undercounting
// the range.
var errBlockfetchRangeExceededMaxBlocks = errors.New(
	"blockfetch range exceeded maximum block count",
)

// blockfetchMaxConsecutiveNoBlocks is the number of consecutive NoBlocks
// responses for the same (connId, start) tuple before closing the connection.
//
// "Consecutive" is strict: blockfetchRecordNoBlocks restarts the count at 1
// when the start point differs from the last one recorded, and every
// successfully validated range request from that peer calls
// blockfetchResetNoBlocks. The valve therefore fires only for a peer stuck
// re-requesting one point it has been told we do not have, and stays silent
// when a peer asks for a handful of different unavailable points among normal
// traffic — which is what a tip slot battle produces, where both sides roll
// their own block back and briefly ask each other for a body neither still
// has. Not firing there is intended: those are healthy peers.
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

// blockFetchKey identifies one outstanding RequestRange call. gouroboros'
// nextRequestId is scoped per connection, so requestId alone is not globally
// unique; connId alone is exactly the clobber this key exists to avoid, since
// pipelining lets more than one request be outstanding on the same
// connection at once.
type blockFetchKey struct {
	connId    ouroboros.ConnectionId
	requestId uint64
}

// blockfetchRangeRequester is the subset of *blockfetch.Client
// BlockfetchClientRequestRange calls. Extracted so its dispatch and
// blockFetchStarts bookkeeping can be tested without a live connection
// registered in connManager.
type blockfetchRangeRequester interface {
	RequestRange(
		ctx context.Context,
		req blockfetch.RangeRequest,
	) (uint64, error)
}

// blockfetchConnClientFunc resolves the live request-range client for a
// connection. The production value (blockfetchConnClientLive) looks it up
// through connManager; tests override the Ouroboros field this is stored in
// to exercise BlockfetchClientRequestRange without a live connection.
type blockfetchConnClientFunc func(
	ouroboros.ConnectionId,
) (blockfetchRangeRequester, error)

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
		// RangeDoneFunc replaces BatchDoneFunc: with RequestPipelining
		// enabled, every request's terminal outcome (success, NoBlocks, or
		// any transport/protocol failure) is reported here instead, exactly
		// once per request, whether or not it ever started streaming.
		// BatchDoneFunc is never invoked for a pipelined request.
		blockfetch.WithRangeDoneFunc(
			o.instrumentBlockfetchRangeDone(o.blockfetchClientRangeDone),
		),
		blockfetch.WithRequestPipelining(true),
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
) (block gledger.Block, err error) {
	defer func() {
		if err == nil && block != nil {
			block.Hash()
		}
	}()
	if o.config.NetworkMagic == ouroboros.NetworkCardanoMusashi.NetworkMagic &&
		blockType == gledger.BlockTypeConway {
		return models.DecodeConwayBlock(raw)
	}
	return gledger.NewBlockFromCbor(blockType, raw)
}

// blockfetchClientBlockRaw decodes the raw fetched block (via
// decodeBlockfetchBlock, through the shared decode cache) and forwards the
// decoded block to the shared block handler.
//
// Multiple connections routinely deliver byte-identical block bytes around
// the same time (several peers relaying the same freshly-produced block), so
// the decode is keyed by content hash and shared across connections: the
// first connection to submit a given block's bytes decodes it, and every
// other connection submitting the identical bytes -- concurrently or
// afterward -- reuses that result instead of redoing the parse. See #489.
func (o *Ouroboros) blockfetchClientBlockRaw(
	ctx blockfetch.CallbackContext,
	blockType uint,
	blockData []byte,
) error {
	key := hashDecodeInput(blockType, blockData)
	block, err := decodeWithPanicSafeMetrics(
		o.blockDecodeCache,
		key,
		func() (gledger.Block, error) {
			decodeStart := time.Now()
			block, err := o.decodeBlockfetchBlock(blockType, blockData)
			if o.blockfetchMetrics != nil {
				o.blockfetchMetrics.stageDecode.Observe(
					time.Since(decodeStart).Seconds(),
				)
			}
			return block, err
		},
		o.recordBlockDecodeCacheOutcome,
	)
	if err != nil {
		return fmt.Errorf(
			"decode block-fetch block (block type %d): %w",
			blockType,
			err,
		)
	}
	if block == nil {
		// decodeCache's contract is (nil value, non-nil err) on failure, but
		// that is a convention on decodeFn, not something the generic cache
		// itself enforces -- guard explicitly rather than trust it silently.
		return fmt.Errorf(
			"decode block-fetch block (block type %d): decoded nil block with no error",
			blockType,
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
		o.blockfetchRecordNoBlocksAndMaybeClose(
			ctx.ConnectionId,
			start,
			"blockfetch: closing stuck peer after repeated inverted range requests",
			"blockfetch: peer stuck on inverted range",
		)
		return nil
	}
	// The requested slot span is not validated here: on a sparse or
	// low-active-slot-coefficient network, a valid run of consecutive
	// blocks can span far more slots than mainnet's stability window
	// (#4354). Resource usage is instead bounded by actual block count,
	// below, scaled to the network's own security parameter.
	//
	// Validate that the start point exists in our chain (#397)
	chainIter, err := o.ledgerState.GetChainFromPoint(start, true)
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
	// Validate the requested end point against the same canonical chain. The
	// iterator must not be allowed to turn a missing or forked end point into a
	// successful short batch.
	endIter, err := o.ledgerState.GetChainFromPoint(end, true)
	if err != nil {
		o.config.Logger.Debug(
			"blockfetch: end point not found in chain, sending NoBlocks",
			"connection_id", ctx.ConnectionId.String(),
			"end_slot", end.Slot,
			"error", err,
		)
		chainIter.Cancel()
		if err := ctx.Server.NoBlocks(); err != nil {
			return fmt.Errorf(
				"blockfetch NoBlocks after end point not found: %w",
				err,
			)
		}
		o.blockfetchRecordNoBlocksAndMaybeClose(
			ctx.ConnectionId,
			start,
			"blockfetch: closing stuck peer after repeated missing end-point requests",
			"blockfetch: peer stuck on missing end point",
		)
		return nil
	}
	endIter.Cancel()
	maxBlocks := maxBlockFetchBlocksForSecurityParam(o.ledgerState.SecurityParam())
	// maxBlockFetchBlocksForSecurityParam never returns negative.
	maxBlocksU64 := uint64(maxBlocks) // #nosec G115
	// Validate that the range does not exceed the block-count bound (#4354).
	// This mirrors the other invalid-range rejections above instead of
	// silently dropping the connection mid-batch: an honest peer whose range
	// is genuinely larger than the network supports gets a clean, accounted
	// NoBlocks it can act on, rather than a transport reset it would only
	// repeat by retrying the identical range. Best-effort: if either
	// endpoint's block cannot be resolved here -- for example, a race with a
	// concurrent rollback after the checks above -- skip this early check
	// and let blockfetchServerSendBatch's own resource bound enforce it
	// during streaming instead.
	if startBlock, startErr := o.ledgerState.GetBlock(start); startErr == nil {
		if endBlock, endErr := o.ledgerState.GetBlock(end); endErr == nil &&
			endBlock.Number >= startBlock.Number {
			blockCount := endBlock.Number - startBlock.Number + 1
			if blockCount > maxBlocksU64 {
				o.config.Logger.Debug(
					"blockfetch: range exceeds maximum block count, sending NoBlocks",
					"connection_id", ctx.ConnectionId.String(),
					"start_slot", start.Slot,
					"end_slot", end.Slot,
					"block_count", blockCount,
					"max_blocks", maxBlocks,
				)
				chainIter.Cancel()
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
		}
	}
	o.blockfetchResetNoBlocks(ctx.ConnectionId)
	// Start async process to send requested block range
	go func() {
		conn := o.connManager.GetConnectionById(ctx.ConnectionId)
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
			maxBlocks,
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
	maxBlocks int,
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
	reachedEnd := false
	blocksServed := 0
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
			if next.Rollback {
				// A rollback raced this in-flight batch: the iterator
				// surfaced a rollback sentinel with a zero-value Block.
				// Serving it would stream a [0, null] block that a fetching
				// peer decodes as a nil-header Byron EBB and crashes
				// dereferencing it in SlotNumber(). Blockfetch has no
				// rollback message, so end the batch cleanly; the client
				// re-requests against its updated chain. Mirrors the
				// next.Rollback handling in chainsync.
				break Loop
			}
			if next.Block.Slot > end.Slot {
				// The end point was validated before streaming started, so an
				// overshoot means the chain changed under the iterator. BlockFetch
				// has no rollback message; end this batch cleanly and let the peer
				// request again against the new chain.
				break Loop
			}
			if next.Block.Slot == end.Slot &&
				bytes.Equal(next.Point.Hash, end.Hash) {
				reachedEnd = true
			}
			blocksServed++
			if blocksServed > maxBlocks {
				// blockfetchServerRequestRange already rejects an oversized
				// range with NoBlocks before StartBatch when both
				// endpoints' block numbers are resolvable, so reaching this
				// backstop means that check could not run or undercounted
				// (a concurrent rollback, or a Byron-EBB block-number tie).
				// StartBatch() has already committed the protocol to this
				// batch, so the only safe recovery left is to drop the
				// transport (mirrors the other post-StartBatch error paths
				// below).
				o.config.Logger.Warn(
					"blockfetch: range exceeded maximum block count, closing connection",
					"connection_id", connectionID,
					"start_slot", start.Slot,
					"end_slot", end.Slot,
					"max_blocks", maxBlocks,
				)
				o.closeBlockfetchConnection(
					conn,
					connectionID,
					"range exceeded maximum block count after StartBatch",
				)
				return errBlockfetchRangeExceededMaxBlocks
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
			if reachedEnd {
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
	if errors.Is(err, errBlockfetchRangeExceededMaxBlocks) {
		// blockfetchServerSendBatch already logged a WARN and closed the
		// connection for this expected, peer-triggered condition; an Error
		// log and a second Close() attempt here would be pure noise.
		return
	}
	o.config.Logger.Error(
		"blockfetch: async range server failed",
		"connection_id", connectionID,
		"start_slot", start.Slot,
		"end_slot", end.Slot,
		"error", err,
	)
	if closeErr := conn.Close(); closeErr != nil {
		o.config.Logger.Debug(
			"blockfetch: failed to close connection after async server error",
			"connection_id", connectionID,
			"error", closeErr,
		)
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
	if o.connManager == nil {
		return
	}
	conn := o.connManager.GetConnectionById(connId)
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

// blockfetchConnClientLive resolves the live blockfetch client for a
// connection through connManager. This is the production value of the
// Ouroboros.blockfetchConnClient seam; see blockfetchConnClientFunc.
func (o *Ouroboros) blockfetchConnClientLive(
	connId ouroboros.ConnectionId,
) (blockfetchRangeRequester, error) {
	if o.connManager == nil {
		return nil, errors.New("ConnManager not initialized")
	}
	conn := o.connManager.GetConnectionById(connId)
	if conn == nil {
		return nil, fmt.Errorf(
			"failed to lookup connection ID: %s",
			connId.String(),
		)
	}
	return conn.BlockFetch().Client, nil
}

// BlockfetchClientRequestRange is called by the ledger when it needs to
// request a range of block bodies. It returns the request ID gouroboros
// assigned the range, which the caller can use to distinguish this request's
// events from another one outstanding on the same connection.
//
// RequestRange (unlike the GetBlockRange this replaced) returns as soon as
// the request is sent, not once the range has been delivered: the terminal
// outcome always arrives later through blockfetchClientRangeDone via
// RangeDoneFunc, including for a request that fails synchronously here for a
// reason other than never having been sent. Peer-governance failure scoring
// on a synchronous error is therefore this function's own responsibility --
// blockfetchClientRangeDone only ever sees a request that was actually
// queued.
func (o *Ouroboros) BlockfetchClientRequestRange(
	connId ouroboros.ConnectionId,
	start ocommon.Point,
	end ocommon.Point,
) (uint64, error) {
	client, err := o.blockfetchConnClient(connId)
	if err != nil {
		return 0, err
	}
	dispatchStart := time.Now()
	// context.Background() is deliberate: sendRequestRange's internal waits
	// (admission against the in-flight byte budget, and the queue-append send
	// token) already select on the connection's own protocol shutdown channel
	// in addition to ctx.Done(), so a request unblocks on connection teardown
	// even though this caller's context is never canceled directly.
	requestId, err := client.RequestRange(
		context.Background(),
		blockfetch.RangeRequest{Start: start, End: end},
	)
	if err != nil {
		if o.peerGov != nil {
			latencyMs := time.Since(dispatchStart).Milliseconds()
			o.peerGov.UpdatePeerBlockFetchObservation(
				connId,
				float64(latencyMs),
				false,
			)
		}
		return 0, err
	}
	// RequestRange returns once the request is on the wire, so a peer that
	// replies immediately can drive blockfetchClientRangeDone to completion
	// on the protocol's receive goroutine before this insert runs. Recording
	// the start time anyway would leave an entry whose only deleter has
	// already fired, so consume the marker it left instead.
	key := blockFetchKey{connId: connId, requestId: requestId}
	o.blockFetchMutex.Lock()
	if _, doneEarly := o.blockFetchDoneEarly[key]; doneEarly {
		delete(o.blockFetchDoneEarly, key)
	} else {
		o.blockFetchStarts[key] = dispatchStart
	}
	o.blockFetchMutex.Unlock()
	return requestId, nil
}

func (o *Ouroboros) blockfetchClientBlock(
	ctx blockfetch.CallbackContext,
	blockType uint,
	block gledger.Block,
) error {
	// Update metrics and peer scoring
	key := blockFetchKey{connId: ctx.ConnectionId, requestId: ctx.RequestId}
	o.blockFetchMutex.Lock()
	startTime, exists := o.blockFetchStarts[key]
	o.blockFetchMutex.Unlock()
	if exists {
		fetchDuration := time.Since(startTime)

		// Only publish block delay metrics after reaching tip once.
		// During catch-up all blocks are naturally "late" relative to
		// wall-clock time, which permanently poisons the CDF.
		atTip := o.ledgerState != nil && o.ledgerState.IsAtTip()
		if atTip && o.blockfetchMetrics != nil {
			// Calculate block delay as wallclock time minus block slot time (cardano-node compatible)
			var delaySeconds float64
			if blockSlotTime, err := o.ledgerState.SlotToTime(block.SlotNumber()); err == nil {
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

		if o.peerGov != nil {
			latencyMs := fetchDuration.Milliseconds()
			o.peerGov.UpdatePeerBlockFetchObservation(
				ctx.ConnectionId,
				float64(latencyMs),
				true,
			)
		}
	}
	if o.eventBus != nil &&
		o.eventBus.HasSubscribers(ledger.BlockfetchEventType) {
		o.eventBus.Publish(
			ledger.BlockfetchEventType,
			event.NewEvent(
				ledger.BlockfetchEventType,
				ledger.BlockfetchEvent{
					ConnectionId: ctx.ConnectionId,
					RequestId:    ctx.RequestId,
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

// blockfetchClientRangeDone is the RangeDoneFunc for a pipelined request. It
// replaces blockfetchClientBatchDone: with RequestPipelining enabled, every
// request's terminal outcome -- success, NoBlocks, or any other
// transport/protocol failure -- is reported here exactly once, whether or not
// the request ever reached MsgStartBatch. rangeErr is nil for a request that
// completed successfully.
func (o *Ouroboros) blockfetchClientRangeDone(
	ctx blockfetch.CallbackContext,
	rangeErr error,
) error {
	// Clean up start time. An absent entry means this callback beat the
	// dispatching BlockfetchClientRequestRange to the map, so leave a marker
	// for it to consume rather than letting it insert an entry that no
	// further callback will ever remove.
	key := blockFetchKey{connId: ctx.ConnectionId, requestId: ctx.RequestId}
	o.blockFetchMutex.Lock()
	if _, started := o.blockFetchStarts[key]; started {
		delete(o.blockFetchStarts, key)
	} else {
		o.blockFetchDoneEarly[key] = struct{}{}
	}
	o.blockFetchMutex.Unlock()
	if o.eventBus != nil &&
		o.eventBus.HasSubscribers(ledger.BlockfetchEventType) {
		o.eventBus.Publish(
			ledger.BlockfetchEventType,
			event.NewEvent(
				ledger.BlockfetchEventType,
				ledger.BlockfetchEvent{
					ConnectionId: ctx.ConnectionId,
					RequestId:    ctx.RequestId,
					BatchDone:    true,
					RangeErr:     rangeErr,
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

func (o *Ouroboros) instrumentBlockfetchRangeDone(
	fn func(blockfetch.CallbackContext, error) error,
) func(blockfetch.CallbackContext, error) error {
	return func(ctx blockfetch.CallbackContext, rangeErr error) error {
		start := time.Now()
		err := fn(ctx, rangeErr)
		o.recordProtocolMessage("blockfetch", err, time.Since(start))
		return err
	}
}
