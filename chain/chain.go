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
	"fmt"
	"slices"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	"github.com/blinklabs-io/gouroboros/ledger"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	initialBlockIndex uint64 = 1
)

type Chain struct {
	mutex            sync.RWMutex
	db               *database.Database
	eventBus         *event.EventBus
	currentTip       ochainsync.Tip
	tipBlockIndex    uint64
	persistent       bool
	lastDbSlot       uint64
	lastDbBlockIndex uint64
	blocks           []database.Block
	headers          []ledger.BlockHeader
}

func NewChain(
	db *database.Database,
	eventBus *event.EventBus,
	persistent bool,
) (*Chain, error) {
	c := &Chain{
		db:         db,
		eventBus:   eventBus,
		persistent: persistent,
	}
	if persistent && db == nil {
		return nil, errors.New("persistence enabled but no database provided")
	}
	if db != nil {
		if err := c.load(); err != nil {
			return nil, fmt.Errorf("failed to load chain: %w", err)
		}
		// Set last block index and slot from database
		c.lastDbBlockIndex = c.tipBlockIndex
		c.lastDbSlot = c.currentTip.Point.Slot
	}
	return c, nil
}

func (c *Chain) load() error {
	recentBlocks, err := database.BlocksRecent(c.db, 1)
	if err != nil {
		return err
	}
	if len(recentBlocks) > 0 {
		c.currentTip = ochainsync.Tip{
			Point: ocommon.Point{
				Slot: recentBlocks[0].Slot,
				Hash: recentBlocks[0].Hash,
			},
			BlockNumber: recentBlocks[0].Number,
		}
		c.tipBlockIndex = recentBlocks[0].ID
	}
	return nil
}

func (c *Chain) Tip() ochainsync.Tip {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.currentTip
}

func (c *Chain) HeaderTip() ochainsync.Tip {
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	headerTip := c.headerTip()
	// Make sure header fits on chain tip
	if string(header.PrevHash().Bytes()) != string(headerTip.Point.Hash) {
		return NewBlockNotFitChainTipError(
			header.Hash().String(),
			header.PrevHash().String(),
			hex.EncodeToString(headerTip.Point.Hash),
		)
	}
	// Add header
	c.headers = append(c.headers, header)
	return nil
}

func (c *Chain) AddBlock(block ledger.Block, blockNonce []byte, txn *database.Txn) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
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
	tmpBlock := database.Block{
		ID:       newBlockIndex,
		Slot:     tmpPoint.Slot,
		Hash:     tmpPoint.Hash,
		Number:   block.BlockNumber(),
		Type:     uint(block.Type()), //nolint:gosec
		PrevHash: block.PrevHash().Bytes(),
		Nonce:    blockNonce,
		Cbor:     block.Cbor(),
	}
	if c.persistent {
		// Add block to database
		if err := c.db.BlockCreate(tmpBlock, txn); err != nil {
			return err
		}
		c.lastDbBlockIndex = newBlockIndex
		c.lastDbSlot = tmpPoint.Slot
	} else {
		// Add block to memory buffer
		c.blocks = append(
			c.blocks,
			tmpBlock,
		)
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
	// Generate event
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
	return nil
}

func (c *Chain) Rollback(point ocommon.Point) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Check headers for rollback point
	if len(c.headers) > 0 {
		for idx, header := range c.headers {
			if header.SlotNumber() == point.Slot &&
				string(header.Hash().Bytes()) == string(point.Hash) {
				// Remove headers after rollback point
				if idx < len(c.headers)-1 {
					c.headers = slices.Delete(c.headers, idx+1, len(c.headers))
				}
				return nil
			}
		}
	}
	// Lookup block for rollback point
	var rollbackBlockIndex uint64
	var tmpBlock database.Block
	if point.Slot > 0 {
		var err error
		tmpBlock, err = c.BlockByPoint(point, nil)
		if err != nil {
			return err
		}
		rollbackBlockIndex = tmpBlock.ID
	}
	// Delete any rolled-back blocks
	for i := c.tipBlockIndex; i > rollbackBlockIndex; i-- {
		if c.persistent {
			// Remove from database
			txn := c.db.BlobTxn(true)
			err := txn.Do(func(txn *database.Txn) error {
				tmpBlock, err := c.db.BlockByIndex(i, txn)
				if err != nil {
					return err
				}
				if err := database.BlockDeleteTxn(txn, tmpBlock); err != nil {
					return err
				}
				return nil
			})
			if err != nil {
				return err
			}
		} else {
			// Return an error if we try to rollback beyond memory buffer
			if i <= c.lastDbBlockIndex {
				return ErrRollbackBeyondEphemeralChain
			}
			// Remove from memory buffer
			memBlockIndex := int(i - c.lastDbBlockIndex - initialBlockIndex) //nolint:gosec
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
	// Generate event
	c.eventBus.Publish(
		ChainUpdateEventType,
		event.NewEvent(
			ChainUpdateEventType,
			ChainRollbackEvent{
				Point: point,
			},
		),
	)
	return nil
}

func (c *Chain) HeaderRange() (ocommon.Point, ocommon.Point) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	var startPoint, endPoint ocommon.Point
	if len(c.headers) > 0 {
		firstHeader := c.headers[0]
		startPoint = ocommon.Point{
			Slot: firstHeader.SlotNumber(),
			Hash: firstHeader.Hash().Bytes(),
		}
		lastHeader := c.headers[len(c.headers)-1]
		endPoint = ocommon.Point{
			Slot: lastHeader.SlotNumber(),
			Hash: lastHeader.Hash().Bytes(),
		}
	}
	return startPoint, endPoint
}

// FromPoint returns a ChainIterator starting at the specified point. If inclusive is true, the iterator
// will start at the specified point. Otherwise it will start at the point following the specified point
func (c *Chain) FromPoint(point ocommon.Point, inclusive bool) (*ChainIterator, error) {
	return newChainIterator(
		c,
		point,
		inclusive,
	)
}

func (c *Chain) BlockByPoint(point ocommon.Point, txn *database.Txn) (database.Block, error) {
	if point.Slot <= c.lastDbSlot {
		// Query database
		tmpBlock, err := database.BlockByPoint(c.db, point)
		if err != nil {
			if errors.Is(err, database.ErrBlockNotFound) {
				return database.Block{}, ErrBlockNotFound
			}
			return database.Block{}, err
		}
		return tmpBlock, nil
	}
	// Search memory buffer
	for _, block := range c.blocks {
		if point.Slot != block.Slot {
			continue
		}
		if string(point.Hash) != string(block.Hash) {
			continue
		}
		return block, nil
	}
	return database.Block{}, ErrBlockNotFound
}

func (c *Chain) blockByIndex(blockIndex uint64, txn *database.Txn) (database.Block, error) {
	if blockIndex <= c.lastDbBlockIndex {
		// Query database
		tmpBlock, err := c.db.BlockByIndex(blockIndex, txn)
		if err != nil {
			if errors.Is(err, database.ErrBlockNotFound) {
				return database.Block{}, ErrBlockNotFound
			}
			return database.Block{}, err
		}
		return tmpBlock, nil
	}
	// Get from memory buffer
	memBlockIndex := int(blockIndex - c.lastDbBlockIndex - initialBlockIndex) //nolint:gosec
	if memBlockIndex < 0 || len(c.blocks) < memBlockIndex+1 {
		return database.Block{}, ErrBlockNotFound
	}
	return c.blocks[memBlockIndex], nil
}
