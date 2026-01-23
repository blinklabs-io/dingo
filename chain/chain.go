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
	"encoding/hex"
	"errors"
	"log/slog"
	"slices"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	initialBlockIndex uint64 = 1
)

type Chain struct {
	eventBus             *event.EventBus
	manager              *ChainManager
	waitingChan          chan struct{}
	headers              []ledger.BlockHeader
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

func (c *Chain) headerTip() ochainsync.Tip {
	if len(c.headers) == 0 {
		return c.currentTip
	}
	lastHeader := c.headers[len(c.headers)-1]
	return ochainsync.Tip{
		Point: ocommon.Point{
			Slot: lastHeader.SlotNumber(),
			Hash: lastHeader.Hash().Bytes(),
		},
		BlockNumber: lastHeader.BlockNumber(),
	}
}

func (c *Chain) AddBlockHeader(header ledger.BlockHeader) error {
	if c == nil {
		return errors.New("chain is nil")
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Make sure header fits on chain tip
	if c.tipBlockIndex >= initialBlockIndex ||
		len(c.headers) > 0 {
		headerTip := c.headerTip()
		if string(header.PrevHash().Bytes()) != string(headerTip.Point.Hash) {
			return NewBlockNotFitChainTipError(
				header.Hash().String(),
				header.PrevHash().String(),
				hex.EncodeToString(headerTip.Point.Hash),
			)
		}
	}
	// Add header
	c.headers = append(c.headers, header)
	return nil
}

func (c *Chain) AddBlock(
	block ledger.Block,
	txn *database.Txn,
) error {
	if c == nil {
		return errors.New("chain is nil")
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// We get a write lock on the manager to cover the integrity checks and adding the block below
	c.manager.mutex.Lock()
	defer c.manager.mutex.Unlock()
	// Verify chain integrity
	if err := c.reconcile(); err != nil {
		return err
	}
	// Check that the new block matches our first header, if any
	if len(c.headers) > 0 {
		firstHeader := c.headers[0]
		if block.Hash().String() != firstHeader.Hash().String() {
			return NewBlockNotMatchHeaderError(
				block.Hash().String(),
				firstHeader.Hash().String(),
			)
		}
	}
	// Check that this block fits on the current chain tip
	if c.tipBlockIndex >= initialBlockIndex {
		if string(block.PrevHash().Bytes()) != string(c.currentTip.Point.Hash) {
			return NewBlockNotFitChainTipError(
				block.Hash().String(),
				block.PrevHash().String(),
				hex.EncodeToString(c.currentTip.Point.Hash),
			)
		}
	}
	// Build new block record
	tmpPoint := ocommon.NewPoint(
		block.SlotNumber(),
		block.Hash().Bytes(),
	)
	newBlockIndex := c.tipBlockIndex + 1
	tmpBlock := models.Block{
		ID:       newBlockIndex,
		Slot:     tmpPoint.Slot,
		Hash:     tmpPoint.Hash,
		Number:   block.BlockNumber(),
		Type:     uint(block.Type()), //nolint:gosec
		PrevHash: block.PrevHash().Bytes(),
		Cbor:     block.Cbor(),
	}
	if err := c.manager.addBlock(tmpBlock, txn, c.persistent); err != nil {
		return err
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
		BlockNumber: block.BlockNumber(),
	}
	c.tipBlockIndex = newBlockIndex
	// Notify waiting iterators
	c.waitingChanMutex.Lock()
	if c.waitingChan != nil {
		close(c.waitingChan)
		c.waitingChan = nil
	}
	c.waitingChanMutex.Unlock()
	// Generate event
	if c.eventBus != nil {
		c.eventBus.Publish(
			ChainUpdateEventType,
			event.NewEvent(
				ChainUpdateEventType,
				ChainBlockEvent{
					Point: tmpPoint,
					Block: tmpBlock,
				},
			),
		)
	}
	return nil
}

func (c *Chain) AddBlocks(blocks []ledger.Block) error {
	if c == nil {
		return errors.New("chain is nil")
	}
	batchOffset := 0
	batchSize := 0
	for {
		batchSize = min(
			50,
			len(blocks)-batchOffset,
		)
		if batchSize == 0 {
			break
		}
		txn := c.manager.db.BlobTxn(true)
		err := txn.Do(func(txn *database.Txn) error {
			for _, tmpBlock := range blocks[batchOffset : batchOffset+batchSize] {
				if err := c.AddBlock(tmpBlock, txn); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		batchOffset += batchSize
	}
	return nil
}

func (c *Chain) Rollback(point ocommon.Point) error {
	if c == nil {
		return errors.New("chain is nil")
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// We get a write lock on the manager to cover the integrity checks and block deletions
	c.manager.mutex.Lock()
	defer c.manager.mutex.Unlock()
	// Verify chain integrity
	if err := c.reconcile(); err != nil {
		return err
	}
	// Check headers for rollback point
	if len(c.headers) > 0 {
		// Iterate backwards to make deletion safe
		var header ledger.BlockHeader
		for i := len(c.headers) - 1; i >= 0; i-- {
			header = c.headers[i]
			// Remove headers after rollback slot
			if header.SlotNumber() > point.Slot {
				c.headers = slices.Delete(c.headers, i, i+1)
				continue
			}
			if header.SlotNumber() == point.Slot &&
				string(header.Hash().Bytes()) == string(point.Hash) {
				return nil
			}
			if header.SlotNumber() < point.Slot {
				return models.ErrBlockNotFound
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
			return err
		}
		rollbackBlockIndex = tmpBlock.ID
	}
	// Calculate fork depth before deleting blocks
	forkDepth := c.tipBlockIndex - rollbackBlockIndex
	// Capture old tip for fork event before we modify it
	oldTip := c.currentTip
	// Collect and delete rolled-back blocks in a single pass
	var rolledBackBlocks []models.Block
	for i := c.tipBlockIndex; i > rollbackBlockIndex; i-- {
		if c.persistent {
			// Remove block from persistent store, returns the removed block
			block, err := c.manager.removeBlockByIndex(i)
			if err != nil {
				return err
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
		if iter.lastPoint.Slot > point.Slot {
			// Don't update rollback point if the iterator already has an older one pending
			if iter.needsRollback && point.Slot > iter.rollbackPoint.Slot {
				continue
			}
			iter.rollbackPoint = point
			iter.needsRollback = true
		}
	}
	// Generate events
	if c.eventBus != nil {
		// Rollback event (existing)
		c.eventBus.Publish(
			ChainUpdateEventType,
			event.NewEvent(
				ChainUpdateEventType,
				ChainRollbackEvent{
					Point:            point,
					RolledBackBlocks: rolledBackBlocks,
				},
			),
		)
		// Fork event (new) - only emit if we actually rolled back blocks
		if forkDepth > 0 {
			c.eventBus.Publish(
				ChainForkEventType,
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
	return nil
}

func (c *Chain) HeaderCount() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return len(c.headers)
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
		startPoint = ocommon.Point{
			Slot: firstHeader.SlotNumber(),
			Hash: firstHeader.Hash().Bytes(),
		}
		lastHeaderIdx := min(count, len(c.headers)) - 1
		lastHeader := c.headers[lastHeaderIdx]
		endPoint = ocommon.Point{
			Slot: lastHeader.SlotNumber(),
			Hash: lastHeader.Hash().Bytes(),
		}
	}
	return startPoint, endPoint
}

// FromPoint returns a ChainIterator starting at the specified point. If inclusive is true, the iterator
// will start at the specified point. Otherwise it will start at the point following the specified point
func (c *Chain) FromPoint(
	point ocommon.Point,
	inclusive bool,
) (*ChainIterator, error) {
	if c == nil {
		return nil, errors.New("chain is nil")
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	iter, err := newChainIterator(
		c,
		point,
		inclusive,
	)
	if err != nil {
		return nil, err
	}
	c.iterators = append(c.iterators, iter)
	return iter, nil
}

func (c *Chain) BlockByPoint(
	point ocommon.Point,
	txn *database.Txn,
) (models.Block, error) {
	return c.manager.BlockByPoint(point, txn)
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

func (c *Chain) iterNext(
	iter *ChainIterator,
	blocking bool,
) (*ChainIteratorResult, error) {
	c.mutex.Lock()
	// We get a read lock on the manager for the integrity check and initial block lookup
	c.manager.mutex.RLock()
	// Verify chain integrity
	if err := c.reconcile(); err != nil {
		c.mutex.Unlock()
		c.manager.mutex.RUnlock()
		return nil, err
	}
	// Check for pending rollback
	if iter.needsRollback {
		ret := &ChainIteratorResult{}
		ret.Point = iter.rollbackPoint
		ret.Rollback = true
		iter.lastPoint = iter.rollbackPoint
		iter.needsRollback = false
		if iter.rollbackPoint.Slot > 0 {
			// Lookup block index for rollback point
			tmpBlock, err := c.manager.blockByPoint(iter.rollbackPoint, nil)
			if err != nil {
				c.mutex.Unlock()
				c.manager.mutex.RUnlock()
				return nil, err
			}
			iter.nextBlockIndex = tmpBlock.ID + 1
		}
		c.mutex.Unlock()
		c.manager.mutex.RUnlock()
		return ret, nil
	}
	ret := &ChainIteratorResult{}
	// Lookup next block in metadata DB
	tmpBlock, err := c.blockByIndex(iter.nextBlockIndex)
	// Return immedidately if a block is found
	if err == nil {
		ret.Point = ocommon.NewPoint(tmpBlock.Slot, tmpBlock.Hash)
		ret.Block = tmpBlock
		iter.nextBlockIndex++
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
	// Return immediately if we're not blocking
	if !blocking {
		c.mutex.Unlock()
		c.manager.mutex.RUnlock()
		return nil, ErrIteratorChainTip
	}
	c.mutex.Unlock()
	c.manager.mutex.RUnlock()
	// Wait for chain update
	c.waitingChanMutex.Lock()
	if c.waitingChan == nil {
		c.waitingChan = make(chan struct{})
	}
	waitChan := c.waitingChan
	c.waitingChanMutex.Unlock()

	select {
	case <-waitChan:
		// Call ourselves again now that we should have new data
		return c.iterNext(iter, blocking)
	case <-iter.ctx.Done():
		// Iterator was cancelled
		return nil, iter.ctx.Err()
	}
}

func (c *Chain) reconcile() error {
	securityParam := c.manager.securityParam
	// We reconcile against the primary/persistent chain, so no need to check if we are that chain
	if c.persistent {
		return nil
	}
	// Check with manager if there have been any primary chain rollback events that would trigger a reconcile
	if !c.manager.chainNeedsReconcile(c.id, c.lastCommonBlockIndex) {
		return nil
	}
	// Check our blocks against primary chain until we find a match
	primaryChain := c.manager.PrimaryChain()
	if primaryChain == nil {
		return models.ErrBlockNotFound
	}
	for i := len(c.blocks) - 1; i >= 0; i-- {
		tmpBlock, err := primaryChain.blockByIndex(
			// Add 1 to prevent off-by-one error
			c.lastCommonBlockIndex + uint64(i) + 1,
		)
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				continue
			}
			return err
		}
		if c.blocks[i].Slot != tmpBlock.Slot {
			continue
		}
		if string(c.blocks[i].Hash) != string(tmpBlock.Hash) {
			continue
		}
		// Adjust our chain-local blocks and offset point from primary chain
		c.blocks = slices.Delete(c.blocks, 0, i+1)
		c.lastCommonBlockIndex = tmpBlock.ID
		return nil
	}
	// Determine prev-hash from earliest known good block
	knownPoint := c.currentTip.Point
	if len(c.blocks) > 0 {
		knownPoint = c.blocks[0]
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
	// Iterate backward through chain based on prev-hash until we find a matching block on the primary chain
	// Accumulate blocks locally to avoid O(K²) prepending
	newBlocks := make([]ocommon.Point, 0, securityParam)
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
		// Lookup same block index on primary chain
		primaryBlock, err := primaryChain.blockByIndex(tmpBlock.ID)
		if err != nil {
			return err
		}
		// Update last common block index and return when we find a matching block on the primary chain
		if tmpBlock.Slot == primaryBlock.Slot &&
			string(tmpBlock.Hash) == string(primaryBlock.Hash) {
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
