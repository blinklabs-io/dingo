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

var ErrBlockNotFound = errors.New("block not found")

type Chain struct {
	mutex         sync.RWMutex
	db            *database.Database
	eventBus      *event.EventBus
	currentTip    ochainsync.Tip
	tipBlockIndex uint64
}

func NewChain(
	db *database.Database,
	eventBus *event.EventBus,
) (*Chain, error) {
	c := &Chain{
		db:       db,
		eventBus: eventBus,
	}
	if db != nil {
		if err := c.load(); err != nil {
			return nil, fmt.Errorf("failed to load chain: %w", err)
		}
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
	return c.currentTip
}

func (c *Chain) AddBlock(block ledger.Block, blockNonce []byte, txn *database.Txn) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// Get block hash and previous hash
	hashBytes, err := hex.DecodeString(block.Hash())
	if err != nil {
		return err
	}
	prevHashBytes, err := hex.DecodeString(block.PrevHash())
	if err != nil {
		return err
	}
	// Check that this block fits on the current chain tip
	if c.tipBlockIndex >= initialBlockIndex {
		if string(prevHashBytes) != string(c.currentTip.Point.Hash) {
			return fmt.Errorf(
				"block %s (with prev hash %x) does not fit on current chain tip (%x)",
				block.Hash(),
				prevHashBytes,
				c.currentTip.Point.Hash,
			)
		}
	}
	// Add block to database
	tmpPoint := ocommon.NewPoint(
		block.SlotNumber(),
		hashBytes,
	)
	newBlockIndex := c.tipBlockIndex + 1
	tmpBlock := database.Block{
		ID:       newBlockIndex,
		Slot:     tmpPoint.Slot,
		Hash:     tmpPoint.Hash,
		Number:   block.BlockNumber(),
		Type:     uint(block.Type()), //nolint:gosec
		PrevHash: prevHashBytes,
		Nonce:    blockNonce,
		Cbor:     block.Cbor(),
	}
	// Add block to database
	if err := c.db.BlockCreate(tmpBlock, txn); err != nil {
		return err
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
	// Lookup block for rollback point
	var rollbackBlockIndex uint64
	var tmpBlock database.Block
	if point.Slot > 0 {
		var err error
		tmpBlock, err = database.BlockByPoint(c.db, point)
		if err != nil {
			return err
		}
		rollbackBlockIndex = tmpBlock.ID
	}
	// Delete any rolled-back blocks
	for i := c.tipBlockIndex; i > rollbackBlockIndex; i-- {
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
	}
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

// FromPoint returns a ChainIterator starting at the specified point. If inclusive is true, the iterator
// will start at the specified point. Otherwise it will start at the point following the specified point
func (c *Chain) FromPoint(point ocommon.Point, inclusive bool) (*ChainIterator, error) {
	return newChainIterator(
		c,
		point,
		inclusive,
	)
}
