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

package chain

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	initialBlockIndex uint64 = 1
	// Mainnet full blocks can make larger batches exceed practical Badger
	// transaction limits during import, so keep the runtime batch size
	// conservative even if smaller benchmark fixtures tolerate more.
	blockImportBatchSize = 50
	// Keep a dense window near the tip so short peer gaps intersect on a
	// recent block, then fall back to exponentially older points to avoid
	// origin intersects when the peer lags by more than a few dozen blocks.
	intersectDensePointCount = 32
)

type Chain struct {
	eventBus             *event.EventBus
	manager              *ChainManager
	waitingChan          chan struct{}
	headers              []queuedHeader
	blocks               []ocommon.Point
	iterators            []*ChainIterator
	currentTip           ochainsync.Tip
	tipBlockIndex        uint64
	lastCommonBlockIndex uint64
	id                   ChainId
	mutex                sync.RWMutex
	waitingChanMutex     sync.Mutex
	persistent           bool
}

type queuedHeader struct {
	header         ledger.BlockHeader
	point          ocommon.Point
	prevHash       []byte
	blockNumber    uint64
	cryptoVerified bool
}

func (c *Chain) Tip() ochainsync.Tip {
	if c == nil {
		return ochainsync.Tip{}
	}
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.currentTip
}

func (c *Chain) HeaderTip() ochainsync.Tip {
	if c == nil {
		return ochainsync.Tip{}
	}
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.headerTip()
}

// blockNumberContiguous reports whether a block number legitimately follows its
// parent's. Shelley-era and later increment by exactly one per block. Byron
// epoch-boundary blocks reuse the parent's block number (they do not increment),
// so in the Byron era both parentNumber and parentNumber+1 are valid. Any other
// value (notably an inflated one) is non-contiguous and rejected.
func blockNumberContiguous(eraId uint8, blockNumber, parentNumber uint64) bool {
	if blockNumber == parentNumber+1 {
		return true
	}
	if eraId == byron.EraIdByron && blockNumber == parentNumber {
		return true
	}
	return false
}

func (c *Chain) headerTip() ochainsync.Tip {
	if len(c.headers) == 0 {
		return c.currentTip
	}
	lastHeader := c.headers[len(c.headers)-1]
	return ochainsync.Tip{
		Point:       lastHeader.point,
		BlockNumber: lastHeader.blockNumber,
	}
}

// MaxQueuedHeaders returns the maximum number of headers that may be
// queued. The limit is the larger of securityParam * 2 and
// DefaultMaxQueuedHeaders. Using the default as a floor ensures the
// queue is large enough for the chainsync/blockfetch pipeline: headers
// arrive much faster than blocks, so the queue must accommodate several
// blockfetch batches worth of headers beyond the accumulation threshold
// to avoid drops that break the header chain.
func (c *Chain) MaxQueuedHeaders() int {
	if c == nil || c.manager == nil {
		return DefaultMaxQueuedHeaders
	}
	// Before SetLedger succeeds, securityParam is zero and the default
	// floor applies (tests or early bootstrap only).
	if sp := c.manager.securityParam; sp > 0 {
		return max(sp*2, DefaultMaxQueuedHeaders)
	}
	return DefaultMaxQueuedHeaders
}

func (c *Chain) AddBlockHeader(header ledger.BlockHeader) error {
	return c.addBlockHeader(header, false)
}

func (c *Chain) AddVerifiedBlockHeader(header ledger.BlockHeader) error {
	return c.addBlockHeader(header, true)
}

func (c *Chain) addBlockHeader(
	header ledger.BlockHeader,
	cryptoVerified bool,
) error {
	if c == nil {
		return errors.New("chain is nil")
	}
	headerHash := header.Hash()
	headerPrevHash := header.PrevHash()
	queued := queuedHeader{
		header: header,
		point: ocommon.Point{
			Slot: header.SlotNumber(),
			Hash: headerHash.Bytes(),
		},
		prevHash:       headerPrevHash.Bytes(),
		blockNumber:    header.BlockNumber(),
		cryptoVerified: cryptoVerified,
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Reject headers when the queue is at capacity to prevent
	// unbounded memory growth from a malicious peer.
	if len(c.headers) >= c.MaxQueuedHeaders() {
		return ErrHeaderQueueFull
	}
	// Make sure header fits on chain tip
	if c.tipBlockIndex >= initialBlockIndex ||
		len(c.headers) > 0 {
		headerTip := c.headerTip()
		if !bytes.Equal(queued.prevHash, headerTip.Point.Hash) {
			return NewBlockNotFitChainTipError(
				headerHash.String(),
				headerPrevHash.String(),
				hex.EncodeToString(headerTip.Point.Hash),
			)
		}
		// Bind the header's self-reported block number to the parent's. The
		// header chains onto the tip (prev hash matched above), so its block
		// number must be contiguous: exactly parent+1 for Shelley-era and
		// later. Byron epoch-boundary blocks reuse the parent's number, so in
		// the Byron era both parent and parent+1 are accepted. Rejecting a
		// non-contiguous number stops a forged (inflated) block number from
		// entering the chain and winning chain selection's longest-chain rule.
		if !blockNumberContiguous(
			header.Era().Id,
			queued.blockNumber,
			headerTip.BlockNumber,
		) {
			return NewBlockNumberNotContiguousError(
				headerHash.String(),
				queued.blockNumber,
				headerTip.BlockNumber,
			)
		}
	}
	// Add header
	c.headers = append(c.headers, queued)
	return nil
}

func (c *Chain) AddBlock(
	block ledger.Block,
	txn *database.Txn,
) error {
	evt, err := c.addBlockInternal(block, ocommon.Point{}, txn, true)
	if err != nil {
		return err
	}
	// Publish event immediately for standalone (non-batched) calls
	if c.eventBus != nil && evt.Type != "" {
		c.eventBus.Publish(ChainUpdateEventType, evt)
	}
	return nil
}

// HandleBlockProposedEvent applies locally forged block proposals published on
// the EventBus and acknowledges the result to the proposer when requested.
func (c *Chain) HandleBlockProposedEvent(evt event.Event) {
	proposal, ok := evt.Data.(BlockProposedEvent)
	if !ok {
		return
	}
	if proposal.Block == nil {
		proposal.Respond(errors.New("proposed block is nil"))
		return
	}
	proposal.Respond(c.AddBlock(proposal.Block, nil))
}

// AddBlockWithPoint adds a block using a caller-supplied point. This avoids
// recomputing the block hash when the caller already has the canonical slot/hash
// pair from a validated upstream source such as blockfetch.
func (c *Chain) AddBlockWithPoint(
	block ledger.Block,
	point ocommon.Point,
	txn *database.Txn,
) error {
	evt, err := c.addBlockInternal(block, point, txn, true)
	if err != nil {
		return err
	}
	if c.eventBus != nil && evt.Type != "" {
		c.eventBus.Publish(ChainUpdateEventType, evt)
	}
	return nil
}

// addBlockInternal performs all block-adding logic but returns the event
// instead of publishing it. This allows AddBlocks to defer event
// publication until the entire batch transaction has committed, preventing
// subscribers from observing data that may be rolled back.
func (c *Chain) addBlockInternal(
	block ledger.Block,
	point ocommon.Point,
	txn *database.Txn,
	notifyWaiters bool,
) (event.Event, error) {
	if c == nil {
		return event.Event{}, errors.New("chain is nil")
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// We get a write lock on the manager to cover the integrity checks and adding the block below
	c.manager.mutex.Lock()
	defer c.manager.mutex.Unlock()
	// Verify chain integrity
	if err := c.reconcile(); err != nil {
		return event.Event{}, fmt.Errorf("reconcile chain: %w", err)
	}
	return c.addBlockLocked(block, point, txn, notifyWaiters)
}

func (c *Chain) addBlockLocked(
	block ledger.Block,
	point ocommon.Point,
	txn *database.Txn,
	notifyWaiters bool,
) (event.Event, error) {
	blockHashBytes := point.Hash
	if len(blockHashBytes) == 0 {
		blockHashBytes = block.Hash().Bytes()
		point = ocommon.NewPoint(block.SlotNumber(), blockHashBytes)
	}
	blockPrevHashBytes := []byte(nil)
	blockNumber := block.BlockNumber()
	// Check that the new block matches our first header, if any
	if len(c.headers) > 0 {
		firstHeader := c.headers[0]
		if !bytes.Equal(blockHashBytes, firstHeader.point.Hash) {
			return event.Event{}, NewBlockNotMatchHeaderError(
				hex.EncodeToString(blockHashBytes),
				firstHeader.header.Hash().String(),
			)
		}
		blockPrevHashBytes = firstHeader.prevHash
		blockNumber = firstHeader.blockNumber
	}
	if len(blockPrevHashBytes) == 0 {
		blockPrevHashBytes = block.PrevHash().Bytes()
	}
	// Check that this block fits on the current chain tip
	if c.tipBlockIndex >= initialBlockIndex {
		if !bytes.Equal(blockPrevHashBytes, c.currentTip.Point.Hash) {
			return event.Event{}, NewBlockNotFitChainTipError(
				hex.EncodeToString(blockHashBytes),
				hex.EncodeToString(blockPrevHashBytes),
				hex.EncodeToString(c.currentTip.Point.Hash),
			)
		}
		// Bind the block number to the parent's (defense in depth alongside the
		// header-ingestion check): a block that fits on the tip must carry a
		// contiguous number so a forged number cannot enter the chain.
		if !blockNumberContiguous(
			block.Era().Id,
			blockNumber,
			c.currentTip.BlockNumber,
		) {
			return event.Event{}, NewBlockNumberNotContiguousError(
				hex.EncodeToString(blockHashBytes),
				blockNumber,
				c.currentTip.BlockNumber,
			)
		}
	}
	// Build new block record
	tmpPoint := point
	newBlockIndex := c.tipBlockIndex + 1
	tmpBlock := models.Block{
		ID:       newBlockIndex,
		Slot:     tmpPoint.Slot,
		Hash:     tmpPoint.Hash,
		Number:   blockNumber,
		Type:     uint(block.Type()), //nolint:gosec
		PrevHash: blockPrevHashBytes,
		Cbor:     block.Cbor(),
	}
	if err := c.manager.addBlock(tmpBlock, txn, c.persistent); err != nil {
		return event.Event{}, fmt.Errorf("store block: %w", err)
	}
	if !c.persistent {
		c.blocks = append(c.blocks, tmpPoint)
	}
	// Remove matching header entry, if any
	if len(c.headers) > 0 {
		c.headers = slices.Delete(c.headers, 0, 1)
	}
	// Update tip
	c.currentTip = ochainsync.Tip{
		Point:       tmpPoint,
		BlockNumber: blockNumber,
	}
	c.tipBlockIndex = newBlockIndex
	if notifyWaiters {
		c.notifyWaitingIterators()
	}
	if c.eventBus == nil {
		return event.Event{}, nil
	}
	// Build event for caller to publish after transaction commit
	evt := event.NewEvent(
		ChainUpdateEventType,
		ChainBlockEvent{
			Point: tmpPoint,
			Block: tmpBlock,
		},
	)
	return evt, nil
}

func (c *Chain) AddBlocks(blocks []ledger.Block) error {
	if c == nil {
		return errors.New("chain is nil")
	}
	batchOffset := 0
	batchSize := 0
	for {
		batchSize = min(
			blockImportBatchSize,
			len(blocks)-batchOffset,
		)
		if batchSize == 0 {
			break
		}
		// Collect events during the transaction so they can be
		// published only after the transaction commits successfully.
		// This prevents subscribers from observing rolled-back data
		// when a later block in the batch fails.
		pendingEvents := make([]event.Event, 0, batchSize)
		txn := c.manager.db.BlobTxn(true)
		err := txn.Do(func(txn *database.Txn) error {
			c.mutex.Lock()
			defer c.mutex.Unlock()
			c.manager.mutex.Lock()
			defer c.manager.mutex.Unlock()
			if err := c.reconcile(); err != nil {
				return fmt.Errorf("reconcile chain: %w", err)
			}
			for _, tmpBlock := range blocks[batchOffset : batchOffset+batchSize] {
				evt, err := c.addBlockLocked(
					tmpBlock,
					ocommon.Point{},
					txn,
					false,
				)
				if err != nil {
					return err
				}
				if evt.Type != "" {
					pendingEvents = append(pendingEvents, evt)
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		c.notifyWaitingIterators()
		// Transaction committed successfully; publish all events
		if c.eventBus != nil {
			for _, evt := range pendingEvents {
				c.eventBus.Publish(ChainUpdateEventType, evt)
			}
		}
		batchOffset += batchSize
	}
	return nil
}

// RawBlock contains pre-extracted block fields for direct storage
// without requiring a full ledger.Block decode.
type RawBlock struct {
	Slot        uint64
	Hash        []byte
	BlockNumber uint64
	Type        uint
	PrevHash    []byte
	Cbor        []byte
}

func (c *Chain) addRawBlockLocked(
	rb RawBlock,
	txn *database.Txn,
	callback func(RawBlock, *database.Txn) error,
) (event.Event, error) {
	// Validate hash fields before any comparisons
	if len(rb.Hash) == 0 {
		return event.Event{}, errors.New(
			"invalid raw block: empty Hash",
		)
	}
	// Validate PrevHash only when tipBlockIndex >= initialBlockIndex. When tipBlockIndex < initialBlockIndex
	// but headers are queued, we may be inserting the genesis/first block which legitimately has no PrevHash.
	// The narrower check ensures we only enforce PrevHash presence once the chain is beyond the initial block.
	if c.tipBlockIndex >= initialBlockIndex &&
		len(rb.PrevHash) == 0 {
		return event.Event{}, errors.New(
			"invalid raw block: empty PrevHash",
		)
	}
	// Check that the new block matches our first header, if any
	if len(c.headers) > 0 {
		firstHeader := c.headers[0]
		if !bytes.Equal(rb.Hash, firstHeader.point.Hash) {
			return event.Event{}, NewBlockNotMatchHeaderError(
				hex.EncodeToString(rb.Hash),
				firstHeader.header.Hash().String(),
			)
		}
	}
	// Check that this block fits on the current chain tip
	if c.tipBlockIndex >= initialBlockIndex {
		if !bytes.Equal(rb.PrevHash, c.currentTip.Point.Hash) {
			return event.Event{}, NewBlockNotFitChainTipError(
				hex.EncodeToString(rb.Hash),
				hex.EncodeToString(rb.PrevHash),
				hex.EncodeToString(c.currentTip.Point.Hash),
			)
		}
	}
	tmpPoint := ocommon.NewPoint(rb.Slot, rb.Hash)
	newBlockIndex := c.tipBlockIndex + 1
	tmpBlock := models.Block{
		ID:       newBlockIndex,
		Slot:     tmpPoint.Slot,
		Hash:     tmpPoint.Hash,
		Number:   rb.BlockNumber,
		Type:     rb.Type,
		PrevHash: rb.PrevHash,
		Cbor:     rb.Cbor,
	}
	if err := c.manager.addBlock(tmpBlock, txn, c.persistent); err != nil {
		return event.Event{}, fmt.Errorf("persisting block: %w", err)
	}
	if callback != nil {
		if err := callback(rb, txn); err != nil {
			return event.Event{}, err
		}
	}
	if !c.persistent {
		c.blocks = append(c.blocks, tmpPoint)
	}
	if len(c.headers) > 0 {
		c.headers = slices.Delete(c.headers, 0, 1)
	}
	c.currentTip = ochainsync.Tip{
		Point:       tmpPoint,
		BlockNumber: rb.BlockNumber,
	}
	c.tipBlockIndex = newBlockIndex
	// Build event for deferred publication (same pattern as
	// addBlockLocked — publish after the transaction commits).
	if c.eventBus != nil {
		return event.NewEvent(
			ChainUpdateEventType,
			ChainBlockEvent{
				Point: tmpPoint,
				Block: tmpBlock,
			},
		), nil
	}
	return event.Event{}, nil
}

// AddRawBlocks adds a batch of pre-extracted blocks to the chain.
func (c *Chain) AddRawBlocks(blocks []RawBlock) error {
	return c.addRawBlocks(blocks, nil)
}

// AddRawBlocksWithCallback adds a batch of pre-extracted blocks to the chain
// and runs the callback in the same transaction after each block is persisted.
// Callers can use this to atomically attach additional blob-side state, such as
// offset indexes, without reopening the immutable DB on resume.
//
// The callback executes with c.mutex and c.manager.mutex locked, inside the
// active blob transaction, and BEFORE c.currentTip / c.tipBlockIndex are
// updated for the just-persisted block. As a result:
//   - The callback must not call back into Chain or ChainManager methods that
//     acquire those same locks (e.g., c.Tip(), c.HeaderTip(),
//     c.BlockByPoint()) — doing so will deadlock.
//   - Tip-state observed via fields read under those locks reflects the
//     pre-update tip, not the block being added.
//
// Error semantics: a callback error aborts the entire current batch, not just
// the offending block. addRawBlocks drives the loop inside txn.Do, which rolls
// back every block persisted by that transaction when the callback returns
// non-nil. Callers should make per-batch decisions idempotent so a retry on a
// later batch does not duplicate effects from a partial earlier attempt.
func (c *Chain) AddRawBlocksWithCallback(
	blocks []RawBlock,
	callback func(RawBlock, *database.Txn) error,
) error {
	return c.addRawBlocks(blocks, callback)
}

func (c *Chain) addRawBlocks(
	blocks []RawBlock,
	callback func(RawBlock, *database.Txn) error,
) error {
	if c == nil {
		return errors.New("chain is nil")
	}
	batchOffset := 0
	for {
		batchSize := min(blockImportBatchSize, len(blocks)-batchOffset)
		if batchSize == 0 {
			break
		}
		// Collect events inside the transaction callback and
		// publish them only after the transaction commits
		// successfully.
		pendingEvents := make([]event.Event, 0, batchSize)
		txn := c.manager.db.BlobTxn(true)
		// addRawBlockLocked mutates c.currentTip, c.tipBlockIndex,
		// c.headers, and c.blocks before the txn commits. If a
		// later block in the batch fails the closure restores the
		// pre-batch state, but txn.Do also runs Commit *after* the
		// closure returns and after the chain locks are released —
		// a Commit failure rolls back the persistent state while
		// leaving the in-memory chain advanced. Capture the
		// snapshot here so we can also restore on Commit failure.
		var (
			snapshotTaken      bool
			savedTip           ochainsync.Tip
			savedTipBlockIndex uint64
			savedHeaders       []queuedHeader
			savedBlocks        []ocommon.Point
		)
		err := txn.Do(func(txn *database.Txn) error {
			batch := blocks[batchOffset : batchOffset+batchSize]
			c.mutex.Lock()
			defer c.mutex.Unlock()
			c.manager.mutex.Lock()
			defer c.manager.mutex.Unlock()
			if err := c.reconcile(); err != nil {
				return fmt.Errorf("reconcile: %w", err)
			}
			savedTip = c.currentTip
			savedTipBlockIndex = c.tipBlockIndex
			savedHeaders = slices.Clone(c.headers)
			if !c.persistent {
				savedBlocks = slices.Clone(c.blocks)
			}
			snapshotTaken = true
			for _, rb := range batch {
				evt, err := c.addRawBlockLocked(
					rb,
					txn,
					callback,
				)
				if err != nil {
					c.currentTip = savedTip
					c.tipBlockIndex = savedTipBlockIndex
					c.headers = savedHeaders
					if !c.persistent {
						c.blocks = savedBlocks
					}
					return err
				}
				if evt.Type != "" {
					pendingEvents = append(
						pendingEvents, evt,
					)
				}
			}
			return nil
		})
		if err != nil {
			// Cover the Commit-failure path: closure returned nil
			// but txn.Do's later Commit failed, so memory still
			// reflects the post-batch tip while the DB rolled
			// back. Re-acquire the locks and restore. Restoring
			// after a closure-internal error path is a safe no-op
			// because the closure already wrote the same values.
			if snapshotTaken {
				c.mutex.Lock()
				c.manager.mutex.Lock()
				c.currentTip = savedTip
				c.tipBlockIndex = savedTipBlockIndex
				c.headers = savedHeaders
				if !c.persistent {
					c.blocks = savedBlocks
				}
				c.manager.mutex.Unlock()
				c.mutex.Unlock()
			}
			return fmt.Errorf("add raw block batch: %w", err)
		}
		c.notifyWaitingIterators()
		// Publish events (only when eventBus is set).
		if c.eventBus != nil {
			for _, evt := range pendingEvents {
				c.eventBus.Publish(
					ChainUpdateEventType, evt,
				)
			}
		}
		batchOffset += batchSize
	}
	return nil
}

func (c *Chain) notifyWaitingIterators() {
	c.waitingChanMutex.Lock()
	defer c.waitingChanMutex.Unlock()
	if c.waitingChan != nil {
		close(c.waitingChan)
		c.waitingChan = nil
	}
}

func (c *Chain) Rollback(point ocommon.Point) error {
	if c == nil {
		return errors.New("chain is nil")
	}
	pendingEvents, err := c.rollbackLocked(point)
	if err != nil {
		return err
	}
	// Publish events after locks are released to prevent deadlocks
	// when subscribers call back into chain/manager state.
	if c.eventBus != nil {
		for _, evt := range pendingEvents {
			c.eventBus.Publish(evt.Type, evt)
		}
	}
	return nil
}

// ValidateRollback verifies that Rollback(point) would be accepted without
// mutating chain state. Callers can use this to avoid applying external
// side effects before the chain's rollback pre-checks have run.
func (c *Chain) ValidateRollback(point ocommon.Point) error {
	if c == nil {
		return errors.New("chain is nil")
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.manager.mutex.Lock()
	defer c.manager.mutex.Unlock()
	// Verify chain integrity
	if err := c.reconcile(); err != nil {
		return fmt.Errorf("reconcile chain: %w", err)
	}
	if c.persistent && c.manager.securityParam <= 0 {
		return ErrSecurityParamNotConfigured
	}
	// Check headers for rollback point without mutating them
	if len(c.headers) > 0 {
		var header queuedHeader
		for _, v := range slices.Backward(c.headers) {
			header = v
			if header.point.Slot > point.Slot {
				continue
			}
			if header.point.Slot == point.Slot &&
				bytes.Equal(header.point.Hash, point.Hash) {
				return nil
			}
			if header.point.Slot < point.Slot {
				return models.ErrBlockNotFound
			}
		}
	}
	// Lookup block for rollback point
	var rollbackBlockIndex uint64
	if point.Slot > 0 {
		tmpBlock, err := c.manager.blockByPoint(point, nil)
		if err != nil {
			return fmt.Errorf("lookup rollback point: %w", err)
		}
		rollbackBlockIndex = tmpBlock.ID
	}
	// Calculate fork depth before deleting blocks
	forkDepth := c.tipBlockIndex - rollbackBlockIndex
	// Reject rollbacks that exceed the security parameter K on
	// the persistent chain. Ephemeral (fork-tracking) chains are
	// not subject to this limit. When the chain is shorter than K
	// blocks (initial sync), the entire chain can be safely
	// replaced during sync.
	securityParam := c.manager.securityParam
	if c.persistent &&
		c.tipBlockIndex >= uint64(securityParam) && //nolint:gosec
		forkDepth > uint64(securityParam) { //nolint:gosec
		slog.Default().Warn(
			"rejecting rollback that exceeds "+
				"security parameter K",
			"fork_depth", forkDepth,
			"security_param", securityParam,
			"rollback_slot", point.Slot,
		)
		return ErrRollbackExceedsSecurityParam
	}
	return nil
}

// rollbackLocked performs all rollback logic under locks and returns
// events to be published by the caller after locks are released.
func (c *Chain) rollbackLocked(
	point ocommon.Point,
) ([]event.Event, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// We get a write lock on the manager to cover the integrity checks and block deletions
	c.manager.mutex.Lock()
	defer c.manager.mutex.Unlock()
	// Verify chain integrity
	if err := c.reconcile(); err != nil {
		return nil, fmt.Errorf("reconcile chain: %w", err)
	}
	if c.persistent && c.manager.securityParam <= 0 {
		return nil, ErrSecurityParamNotConfigured
	}
	// Check headers for rollback point
	if len(c.headers) > 0 {
		// Iterate backwards to make deletion safe
		var header queuedHeader
		for i, v := range slices.Backward(c.headers) {
			header = v
			// Remove headers after rollback slot
			if header.point.Slot > point.Slot {
				c.headers = slices.Delete(c.headers, i, i+1)
				continue
			}
			if header.point.Slot == point.Slot &&
				bytes.Equal(header.point.Hash, point.Hash) {
				return nil, nil
			}
			if header.point.Slot < point.Slot {
				return nil, models.ErrBlockNotFound
			}
		}
	}
	// Lookup block for rollback point
	var rollbackBlockIndex uint64
	var tmpBlock models.Block
	if point.Slot > 0 {
		var err error
		tmpBlock, err = c.manager.blockByPoint(point, nil)
		if err != nil {
			return nil, fmt.Errorf(
				"lookup rollback point: %w", err,
			)
		}
		rollbackBlockIndex = tmpBlock.ID
	}
	// Calculate fork depth before deleting blocks
	forkDepth := c.tipBlockIndex - rollbackBlockIndex
	// Reject rollbacks that exceed the security parameter K on
	// the persistent chain. Ephemeral (fork-tracking) chains are
	// not subject to this limit. When the chain is shorter than K
	// blocks (initial sync), the entire chain can be safely
	// replaced during sync.
	securityParam := c.manager.securityParam
	if c.persistent &&
		c.tipBlockIndex >= uint64(securityParam) && //nolint:gosec
		forkDepth > uint64(securityParam) { //nolint:gosec
		slog.Default().Warn(
			"rejecting rollback that exceeds "+
				"security parameter K",
			"fork_depth", forkDepth,
			"security_param", securityParam,
			"rollback_slot", point.Slot,
		)
		return nil, ErrRollbackExceedsSecurityParam
	}
	// Capture old tip for fork event before we modify it
	oldTip := c.currentTip
	// Collect and delete rolled-back blocks in a single pass
	var rolledBackBlocks []models.Block
	for i := c.tipBlockIndex; i > rollbackBlockIndex; i-- {
		if c.persistent {
			// Remove block from persistent store, returns the removed block
			block, err := c.manager.removeBlockByIndex(i)
			if err != nil {
				return nil, fmt.Errorf(
					"remove block at index %d: %w", i, err,
				)
			}
			if c.eventBus != nil {
				rolledBackBlocks = append(rolledBackBlocks, block)
			}
		} else {
			// Collect block for event emission before deletion
			if c.eventBus != nil {
				block, err := c.blockByIndex(i)
				if err != nil {
					slog.Default().Warn(
						"failed to get block for rollback event",
						"index", i,
						"error", err,
					)
				} else {
					rolledBackBlocks = append(rolledBackBlocks, block)
				}
			}
			// Decrement our fork point block index if we rollback beyond it
			if i < c.lastCommonBlockIndex {
				c.lastCommonBlockIndex = i
				continue
			}
			// Remove from memory buffer
			memBlockIndex := int(i - c.lastCommonBlockIndex - initialBlockIndex) //nolint:gosec
			c.blocks = slices.Delete(
				c.blocks,
				memBlockIndex,
				memBlockIndex+1,
			)
		}
	}
	// Clear out any headers
	c.headers = slices.Delete(c.headers, 0, len(c.headers))
	// Update tip
	c.currentTip = ochainsync.Tip{
		Point:       point,
		BlockNumber: tmpBlock.Number,
	}
	c.tipBlockIndex = rollbackBlockIndex
	// Update iterators for rollback
	for _, iter := range c.iterators {
		// Reverse iterators never deliver rollback markers, but if a
		// rollback shortened the chain past nextBlockIndex the
		// iterator must be clamped to the new tip so subsequent Next
		// calls return the still-present predecessor blocks instead
		// of mistaking missing blocks for origin. Clamping also
		// applies to rollback-to-origin (rollbackBlockIndex == 0,
		// the pre-genesis index — initialBlockIndex is 1): without
		// it, a regrown chain reaching the iterator's stale index
		// would silently hand out unrelated blocks.
		if iter.reverse {
			if iter.nextBlockIndex > rollbackBlockIndex {
				iter.nextBlockIndex = rollbackBlockIndex
			}
			continue
		}
		// Use startPoint for iterators that haven't delivered any blocks
		// yet (lastPoint is zero-value). Without this, newly created
		// iterators miss rollback signals entirely.
		refSlot := iter.lastPoint.Slot
		if refSlot == 0 && len(iter.lastPoint.Hash) == 0 {
			refSlot = iter.startPoint.Slot
		}
		if refSlot > point.Slot {
			// Don't update rollback point if the iterator already has an older one pending
			if iter.needsRollback && point.Slot > iter.rollbackPoint.Slot {
				continue
			}
			iter.rollbackPoint = point
			iter.needsRollback = true
		}
	}
	// Wake any iterators that are blocked waiting for new blocks so
	// they can process the rollback signal promptly.
	c.notifyWaitingIterators()
	// Build events for caller to publish after locks are released
	var pendingEvents []event.Event
	if len(rolledBackBlocks) > 0 {
		// Rollback event - only emit when blocks were actually removed
		pendingEvents = append(
			pendingEvents,
			event.NewEvent(
				ChainUpdateEventType,
				ChainRollbackEvent{
					Point:            point,
					RolledBackBlocks: rolledBackBlocks,
				},
			),
		)
		// Fork event - only emit if we actually rolled back blocks
		if forkDepth > 0 {
			pendingEvents = append(
				pendingEvents,
				event.NewEvent(
					ChainForkEventType,
					ChainForkEvent{
						ForkPoint:     point,
						ForkDepth:     forkDepth,
						AlternateHead: oldTip.Point,
						CanonicalHead: point,
					},
				),
			)
		}
	}
	return pendingEvents, nil
}

// ClearHeaders removes all queued block headers. This is used when
// the active peer changes and stale headers from the previous peer's
// chainsync session no longer fit the current chain tip.
func (c *Chain) ClearHeaders() {
	if c == nil {
		return
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.headers = c.headers[:0]
}

// RecentPoints returns up to count recent chain points in descending
// order (most recent first) using the in-memory chain state. This
// includes the current tip and, for non-persistent chains, any blocks
// stored in the in-memory buffer. For persistent chains, it walks
// backwards through the database using block indices.
//
// This method is useful for building intersection point lists that
// remain accurate even when the blob store has not yet been fully
// flushed, since the chain's in-memory tip is always up-to-date.
func (c *Chain) RecentPoints(count int) []ocommon.Point {
	if c == nil || count <= 0 {
		return nil
	}
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	// If the chain has no blocks yet, return nothing
	if c.tipBlockIndex < initialBlockIndex {
		return nil
	}
	var points []ocommon.Point
	// Always include the current tip
	tip := c.currentTip.Point
	if tip.Slot > 0 || len(tip.Hash) > 0 {
		points = append(points, tip)
	}
	if len(points) >= count {
		return points[:count]
	}
	// Walk backwards through block indices to gather more points
	for idx := c.tipBlockIndex - 1; idx >= initialBlockIndex && len(points) < count; idx-- {
		blk, err := c.blockByIndex(idx)
		if err != nil {
			break
		}
		points = append(
			points,
			ocommon.NewPoint(blk.Slot, blk.Hash),
		)
	}
	return points
}

// IntersectPoints returns up to count points in descending order for
// chainsync FindIntersect. It keeps a dense window near the tip and
// then samples exponentially older blocks so lagging peers can still
// find a recent common point without falling all the way back to origin.
func (c *Chain) IntersectPoints(count int) []ocommon.Point {
	if c == nil || count <= 0 {
		return nil
	}
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if c.tipBlockIndex < initialBlockIndex {
		return nil
	}
	points := make([]ocommon.Point, 0, count)
	seen := make(map[string]struct{}, count)
	appendPoint := func(point ocommon.Point) {
		if len(points) >= count {
			return
		}
		key := fmt.Sprintf("%d:%x", point.Slot, point.Hash)
		if _, ok := seen[key]; ok {
			return
		}
		points = append(points, point)
		seen[key] = struct{}{}
	}
	appendBlockPoint := func(blockIndex uint64) {
		if len(points) >= count {
			return
		}
		blk, err := c.blockByIndex(blockIndex)
		if err != nil {
			return
		}
		appendPoint(ocommon.NewPoint(blk.Slot, blk.Hash))
	}
	denseStartIndex := c.tipBlockIndex
	tip := c.currentTip.Point
	if tip.Slot > 0 || len(tip.Hash) > 0 {
		appendPoint(tip)
		if denseStartIndex > initialBlockIndex {
			denseStartIndex--
		} else {
			denseStartIndex = 0
		}
	}
	denseCount := min(count, intersectDensePointCount)
	for idx := denseStartIndex; idx >= initialBlockIndex && len(points) < denseCount; idx-- {
		appendBlockPoint(idx)
		if idx == initialBlockIndex {
			break
		}
	}
	if len(points) >= count {
		return points
	}
	if c.tipBlockIndex <= initialBlockIndex {
		return points
	}
	for offset := uint64(denseCount); len(points) < count; offset *= 2 { //nolint:gosec // denseCount is bounded to non-negative values
		if offset == 0 || offset >= c.tipBlockIndex {
			break
		}
		appendBlockPoint(c.tipBlockIndex - offset)
	}
	if len(points) < count {
		appendBlockPoint(initialBlockIndex)
	}
	return points
}

func (c *Chain) HeaderCount() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return len(c.headers)
}

func (c *Chain) FirstHeaderMatchesPoint(point ocommon.Point) bool {
	return c.firstHeaderMatchesPoint(point, false)
}

func (c *Chain) FirstVerifiedHeaderMatchesPoint(point ocommon.Point) bool {
	return c.firstHeaderMatchesPoint(point, true)
}

func (c *Chain) firstHeaderMatchesPoint(
	point ocommon.Point,
	requireCryptoVerified bool,
) bool {
	if c == nil {
		return false
	}
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if len(c.headers) == 0 {
		return false
	}
	header := c.headers[0]
	if header.point.Slot != point.Slot {
		return false
	}
	if !bytes.Equal(header.point.Hash, point.Hash) {
		return false
	}
	return !requireCryptoVerified || header.cryptoVerified
}

func (c *Chain) HeaderRange(count int) (ocommon.Point, ocommon.Point) {
	if c == nil {
		return ocommon.Point{}, ocommon.Point{}
	}
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	var startPoint, endPoint ocommon.Point
	if len(c.headers) > 0 {
		firstHeader := c.headers[0]
		startPoint = firstHeader.point
		lastHeaderIdx := min(count, len(c.headers)) - 1
		lastHeader := c.headers[lastHeaderIdx]
		endPoint = lastHeader.point
	}
	return startPoint, endPoint
}

// FromPoint returns a ChainIterator starting at the specified point. If inclusive is true, the iterator
// will start at the specified point. Otherwise it will start at the point following the specified point
func (c *Chain) FromPoint(
	point ocommon.Point,
	inclusive bool,
) (*ChainIterator, error) {
	return c.FromPointContext(context.Background(), point, inclusive)
}

// FromPointContext returns a ChainIterator that inherits cancellation from ctx.
func (c *Chain) FromPointContext(
	ctx context.Context,
	point ocommon.Point,
	inclusive bool,
) (*ChainIterator, error) {
	if c == nil {
		return nil, errors.New("chain is nil")
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	iter, err := newChainIteratorWithContext(
		ctx,
		c,
		point,
		inclusive,
		false,
	)
	if err != nil {
		return nil, err
	}
	c.iterators = append(c.iterators, iter)
	iter.startCancelWatcher()
	return iter, nil
}

// FromPointReverse returns a ChainIterator that walks backward from the
// specified point toward chain origin. If inclusive is true the iterator
// yields the start point first; otherwise it yields the block preceding it.
// Blocking Next calls on a reverse iterator do not wait for new blocks; once
// origin is reached, Next returns ErrIteratorChainOrigin.
func (c *Chain) FromPointReverse(
	point ocommon.Point,
	inclusive bool,
) (*ChainIterator, error) {
	return c.FromPointReverseContext(context.Background(), point, inclusive)
}

// FromPointReverseContext returns a reverse ChainIterator that inherits cancellation from ctx.
func (c *Chain) FromPointReverseContext(
	ctx context.Context,
	point ocommon.Point,
	inclusive bool,
) (*ChainIterator, error) {
	if c == nil {
		return nil, errors.New("chain is nil")
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	iter, err := newChainIteratorWithContext(
		ctx,
		c,
		point,
		inclusive,
		true,
	)
	if err != nil {
		return nil, err
	}
	c.iterators = append(c.iterators, iter)
	iter.startCancelWatcher()
	return iter, nil
}

// removeIterator removes an iterator from the chain's iterator list.
// This is called when an iterator is cancelled to prevent memory leaks.
func (c *Chain) removeIterator(iter *ChainIterator) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for i, it := range c.iterators {
		if it == iter {
			c.iterators = slices.Delete(c.iterators, i, i+1)
			return
		}
	}
}

func (c *Chain) BlockByPoint(
	point ocommon.Point,
	txn *database.Txn,
) (models.Block, error) {
	return c.manager.BlockByPoint(point, txn)
}

// BlockBeforeSlot returns the highest-slot block before slotNumber on this
// chain. It walks the chain index instead of scanning blob keys so retained
// fork or synthetic blobs cannot be returned as canonical blocks.
func (c *Chain) BlockBeforeSlot(slotNumber uint64) (models.Block, error) {
	if c == nil {
		return models.Block{}, errors.New("chain is nil")
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.manager.mutex.RLock()
	defer c.manager.mutex.RUnlock()
	if err := c.reconcile(); err != nil {
		return models.Block{}, err
	}
	if c.tipBlockIndex < initialBlockIndex {
		return models.Block{}, models.ErrBlockNotFound
	}
	// Block slots are strictly increasing with block index on the canonical
	// chain, so binary-search for the highest index whose slot is below
	// slotNumber rather than walking backward from the tip. The old linear walk
	// cost O(tip - boundary) block reads; during catch-up the header chain runs
	// far ahead of the ledger tip, so a boundary near the ledger tip made every
	// lookup scan the entire header-ahead gap (the epoch-lab-nonce heal ran this
	// per recent epoch, wedging large-DB startup for minutes — #2771). The
	// search still resolves each candidate via blockByIndex (the active chain),
	// so retained fork or synthetic blobs are never returned.
	lo, hi := initialBlockIndex, c.tipBlockIndex
	var (
		result models.Block
		found  bool
	)
	for lo <= hi {
		mid := lo + (hi-lo)/2
		block, err := c.blockByIndex(mid)
		if err != nil {
			return models.Block{}, err
		}
		if block.Slot < slotNumber {
			result = block
			found = true
			lo = mid + 1
		} else {
			if mid == initialBlockIndex {
				break
			}
			hi = mid - 1
		}
	}
	if !found {
		return models.Block{}, models.ErrBlockNotFound
	}
	return result, nil
}

func (c *Chain) blockByIndex(
	blockIndex uint64,
) (models.Block, error) {
	if c.persistent || blockIndex <= c.lastCommonBlockIndex {
		// Query via manager for common blocks
		tmpBlock, err := c.manager.blockByIndex(blockIndex, nil)
		if err != nil {
			return models.Block{}, err
		}
		return tmpBlock, nil
	}
	// Get from memory buffer
	//nolint:gosec
	memBlockIndex := int(
		blockIndex - c.lastCommonBlockIndex - initialBlockIndex,
	)
	if memBlockIndex < 0 || len(c.blocks) < memBlockIndex+1 {
		return models.Block{}, models.ErrBlockNotFound
	}
	memBlockPoint := c.blocks[memBlockIndex]
	tmpBlock, err := c.manager.blockByPoint(memBlockPoint, nil)
	if err != nil {
		return models.Block{}, err
	}
	return tmpBlock, nil
}

func chainIteratorPreviousPoint(iter *ChainIterator) ocommon.Point {
	if iter.lastPoint.Slot > 0 || len(iter.lastPoint.Hash) > 0 {
		return iter.lastPoint
	}
	return iter.startPoint
}

func blockFollowsPoint(block models.Block, point ocommon.Point) bool {
	if point.Slot == 0 && len(point.Hash) == 0 {
		return len(block.PrevHash) == 0
	}
	return bytes.Equal(block.PrevHash, point.Hash)
}

func (c *Chain) nextPersistentBlockAfterSparseIndex(
	iter *ChainIterator,
) (models.Block, bool, error) {
	if !c.persistent || iter.reverse ||
		iter.nextBlockIndex > c.tipBlockIndex {
		return models.Block{}, false, nil
	}
	previousPoint := chainIteratorPreviousPoint(iter)
	block, err := c.manager.blockAtOrAfterIndex(
		iter.nextBlockIndex+1,
		nil,
	)
	if errors.Is(err, models.ErrBlockNotFound) {
		return models.Block{}, false, fmt.Errorf(
			"persistent chain tip index %d is ahead of missing iterator index %d, but no later indexed block was found",
			c.tipBlockIndex,
			iter.nextBlockIndex,
		)
	}
	if err != nil {
		return models.Block{}, false, err
	}
	if blockFollowsPoint(block, previousPoint) {
		return block, true, nil
	}
	return models.Block{}, false, fmt.Errorf(
		"persistent chain index gap after index %d: block %d/%s at index %d has prev hash %s, expected %s",
		iter.nextBlockIndex,
		block.Slot,
		hex.EncodeToString(block.Hash),
		block.ID,
		hex.EncodeToString(block.PrevHash),
		hex.EncodeToString(previousPoint.Hash),
	)
}

func (c *Chain) iterNext(
	iter *ChainIterator,
	blocking bool,
) (*ChainIteratorResult, error) {
	for {
		c.mutex.Lock()
		// We get a read lock on the manager for the integrity check and initial block lookup
		c.manager.mutex.RLock()
		// Verify chain integrity
		if err := c.reconcile(); err != nil {
			c.mutex.Unlock()
			c.manager.mutex.RUnlock()
			return nil, err
		}
		// Check for pending rollback (forward iterators only; reverse
		// iterators never have rollbacks queued).
		if iter.needsRollback {
			ret := &ChainIteratorResult{}
			ret.Point = iter.rollbackPoint
			ret.Rollback = true
			iter.lastPoint = iter.rollbackPoint
			iter.needsRollback = false
			if iter.rollbackPoint.Slot > 0 {
				// Lookup block index for rollback point
				tmpBlock, err := c.manager.blockByPoint(
					iter.rollbackPoint,
					nil,
				)
				if err != nil {
					c.mutex.Unlock()
					c.manager.mutex.RUnlock()
					return nil, err
				}
				iter.nextBlockIndex = tmpBlock.ID + 1
			} else {
				// Rolling back to origin: reset to the first
				// block index so the iterator delivers all
				// blocks from genesis.
				iter.nextBlockIndex = initialBlockIndex
			}
			c.mutex.Unlock()
			c.manager.mutex.RUnlock()
			return ret, nil
		}
		// Reverse iterators terminate when they walk past origin.
		// nextBlockIndex == 0 is the sentinel set by newChainIterator
		// for "no more blocks available behind this point".
		if iter.reverse && iter.nextBlockIndex < initialBlockIndex {
			c.mutex.Unlock()
			c.manager.mutex.RUnlock()
			return nil, ErrIteratorChainOrigin
		}
		ret := &ChainIteratorResult{}
		// Lookup next block in metadata DB
		tmpBlock, err := c.blockByIndex(iter.nextBlockIndex)
		if errors.Is(err, models.ErrBlockNotFound) && !iter.reverse {
			recoveredBlock, recovered, recoverErr := c.nextPersistentBlockAfterSparseIndex(
				iter,
			)
			if recoverErr != nil {
				err = recoverErr
			} else if recovered {
				tmpBlock = recoveredBlock
				iter.nextBlockIndex = tmpBlock.ID
				err = nil
			}
		}
		// Return immedidately if a block is found
		if err == nil {
			ret.Point = ocommon.NewPoint(tmpBlock.Slot, tmpBlock.Hash)
			ret.Block = tmpBlock
			if iter.reverse {
				if iter.nextBlockIndex == initialBlockIndex {
					// Just delivered the genesis block; mark
					// the iterator as past origin so the next
					// call returns ErrIteratorChainOrigin.
					iter.nextBlockIndex = 0
				} else {
					iter.nextBlockIndex--
				}
			} else {
				iter.nextBlockIndex++
			}
			iter.lastPoint = ret.Point
			c.mutex.Unlock()
			c.manager.mutex.RUnlock()
			return ret, nil
		}
		// Return any actual error
		if !errors.Is(err, models.ErrBlockNotFound) {
			c.mutex.Unlock()
			c.manager.mutex.RUnlock()
			return ret, err
		}
		// Reverse iterators never wait — origin does not grow.
		if iter.reverse {
			c.mutex.Unlock()
			c.manager.mutex.RUnlock()
			return nil, ErrIteratorChainOrigin
		}
		// Return immediately if we're not blocking
		if !blocking {
			c.mutex.Unlock()
			c.manager.mutex.RUnlock()
			return nil, ErrIteratorChainTip
		}
		// Register the wait channel before releasing c.mutex. Otherwise
		// a concurrent AddBlock can commit and notify in the gap between
		// the at-tip check above and this iterator joining the waiter set,
		// stranding ChainSync peers until a later chain event.
		c.waitingChanMutex.Lock()
		if c.waitingChan == nil {
			c.waitingChan = make(chan struct{})
		}
		waitChan := c.waitingChan
		c.waitingChanMutex.Unlock()
		c.mutex.Unlock()
		c.manager.mutex.RUnlock()

		select {
		case <-waitChan:
			// Loop again now that we should have new data
			continue
		case <-iter.ctx.Done():
			// Iterator was cancelled
			return nil, iter.ctx.Err()
		}
	}
}

// NotifyIterators wakes all blocked iterators waiting for new blocks.
// Call this after a DB transaction that adds blocks has been committed
// to ensure iterators see the newly visible data.
func (c *Chain) NotifyIterators() {
	c.notifyWaitingIterators()
}

func (c *Chain) reconcile() error {
	// We reconcile against the primary/persistent chain, so no need to check if we are that chain
	if c.persistent {
		return nil
	}
	// Check with manager if there have been any primary chain rollback events that would trigger a reconcile
	if !c.manager.chainNeedsReconcile(c.id, c.lastCommonBlockIndex) {
		return nil
	}
	if c.manager.securityParam <= 0 {
		return ErrSecurityParamNotConfigured
	}
	securityParam := c.manager.securityParam
	// Check our blocks against primary chain until we find a match
	primaryChain := c.manager.primaryChainLocked()
	if primaryChain == nil {
		return models.ErrBlockNotFound
	}
	blockIndex := c.tipBlockIndex
	for i, v := range slices.Backward(c.blocks) {
		tmpBlock, err := primaryChain.blockByIndex(blockIndex)
		if err != nil && !errors.Is(err, models.ErrBlockNotFound) {
			return err
		}
		if err == nil &&
			v.Slot == tmpBlock.Slot &&
			bytes.Equal(v.Hash, tmpBlock.Hash) {
			// Adjust our chain-local blocks and offset point from primary chain
			c.blocks = slices.Delete(c.blocks, 0, i+1)
			c.lastCommonBlockIndex = tmpBlock.ID
			return nil
		}
		if blockIndex == 0 {
			break
		}
		blockIndex--
	}
	// Determine prev-hash from earliest known good block
	knownPoint := c.currentTip.Point
	// Iterate backward through chain based on prev-hash until we find a matching block on the primary chain
	// Accumulate blocks locally to avoid O(K²) prepending
	newBlocks := make([]ocommon.Point, 0, securityParam)
	if len(c.blocks) > 0 {
		knownPoint = c.blocks[0]
	} else {
		// No in-memory blocks: the chain's current tip is itself the
		// earliest known good block. Seed newBlocks so the tip is
		// preserved after we re-anchor lastCommonBlockIndex against
		// the primary; otherwise iteration past the new common point
		// would silently truncate at it.
		newBlocks = append(newBlocks, knownPoint)
	}
	knownBlock, err := c.manager.blockByPoint(knownPoint, nil)
	if err != nil {
		return err
	}
	decodedKnownBlock, err := knownBlock.Decode()
	if err != nil {
		return err
	}
	lastPrevHash := decodedKnownBlock.PrevHash().Bytes()
	iterationCount := 0
	for {
		if iterationCount >= securityParam {
			return models.ErrBlockNotFound
		}
		iterationCount++
		tmpBlock, err := c.manager.blockByHash(lastPrevHash)
		if err != nil {
			return err
		}
		// Lookup same block index on primary chain. When the primary
		// has rolled back past tmpBlock's old index the lookup misses;
		// treat tmpBlock as non-common and keep walking back via its
		// PrevHash rather than aborting reconcile.
		primaryBlock, err := primaryChain.blockByIndex(tmpBlock.ID)
		if err != nil && !errors.Is(err, models.ErrBlockNotFound) {
			return err
		}
		// Update last common block index and return when we find a matching block on the primary chain
		if err == nil &&
			tmpBlock.Slot == primaryBlock.Slot &&
			bytes.Equal(tmpBlock.Hash, primaryBlock.Hash) {
			c.lastCommonBlockIndex = tmpBlock.ID
			break
		}
		// Decode block and extract prev-hash
		decodedBlock, err := tmpBlock.Decode()
		if err != nil {
			return err
		}
		lastPrevHash = decodedBlock.PrevHash().Bytes()
		tmpPoint := ocommon.Point{
			Hash: tmpBlock.Hash,
			Slot: tmpBlock.Slot,
		}
		newBlocks = append(newBlocks, tmpPoint)
		c.lastCommonBlockIndex--
	}
	// Prepend accumulated blocks in a single operation (O(K) instead of O(K²))
	if len(newBlocks) > 0 {
		// Reverse newBlocks since they were collected in reverse order
		slices.Reverse(newBlocks)
		c.blocks = slices.Concat(newBlocks, c.blocks)
	}
	return nil
}
