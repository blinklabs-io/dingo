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
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type ChainId uint64

const (
	primaryChainId ChainId = 1
)

type ChainManager struct {
	mutex               sync.RWMutex
	db                  *database.Database
	eventBus            *event.EventBus
	chains              map[ChainId]*Chain
	chainRollbackEvents map[ChainId][]uint64
	blocks              map[string]database.Block
}

func NewManager(db *database.Database, eventBus *event.EventBus) (*ChainManager, error) {
	cm := &ChainManager{
		db:                  db,
		eventBus:            eventBus,
		chains:              make(map[ChainId]*Chain),
		chainRollbackEvents: make(map[ChainId][]uint64),
		blocks:              make(map[string]database.Block),
	}
	if err := cm.loadPrimaryChain(); err != nil {
		return nil, err
	}
	return cm, nil
}

func (cm *ChainManager) PrimaryChain() *Chain {
	return cm.chains[primaryChainId]
}

func (cm *ChainManager) Chain(id ChainId) *Chain {
	return cm.chains[id]
}

// NewChain creates a new Chain that forks from the primary chain at the specified point. This is useful for managing outbound ChainSync clients
func (cm *ChainManager) NewChain(point ocommon.Point) (*Chain, error) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	primaryChain := cm.PrimaryChain()
	primaryChain.mutex.Lock()
	defer primaryChain.mutex.Unlock()
	intersectBlock, err := cm.BlockByPoint(point, nil)
	if err != nil {
		return nil, err
	}
	// Increment current largest chain ID for new ID
	chainIds := slices.Sorted(maps.Keys(cm.chains))
	chainId := chainIds[len(chainIds)-1] + 1
	c := &Chain{
		id:                   chainId,
		manager:              cm,
		eventBus:             cm.eventBus,
		persistent:           false,
		lastCommonBlockIndex: intersectBlock.ID,
		tipBlockIndex:        intersectBlock.ID,
		currentTip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: intersectBlock.Slot,
				Hash: intersectBlock.Hash,
			},
			BlockNumber: intersectBlock.Number,
		},
	}
	cm.chains[chainId] = c
	return c, nil
}

// NewChainFromIntersect creates a new Chain that forks the primary chain at the latest common point.
func (cm *ChainManager) NewChainFromIntersect(points []ocommon.Point) (*Chain, error) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	primaryChain := cm.PrimaryChain()
	primaryChain.mutex.Lock()
	defer primaryChain.mutex.Unlock()
	tip := primaryChain.currentTip
	var intersectPoint ocommon.Point
	var intersectBlock database.Block
	var err error
	foundOrigin := false
	txn := cm.db.BlobTxn(false)
	err = txn.Do(func(txn *database.Txn) error {
		for _, point := range points {
			// Ignore points with a slot later than our current tip
			if point.Slot > tip.Point.Slot {
				continue
			}
			// Ignore points with a slot earlier than an existing match
			if point.Slot < intersectPoint.Slot {
				continue
			}
			// Check for special origin point
			if point.Slot == 0 && len(point.Hash) == 0 {
				foundOrigin = true
				continue
			}
			// Lookup block in database
			intersectBlock, err = cm.blockByPoint(point, txn)
			if err != nil {
				if errors.Is(err, ErrBlockNotFound) {
					continue
				}
				return fmt.Errorf("failed to get block: %w", err)
			}
			// Update return value
			intersectPoint.Slot = intersectBlock.Slot
			intersectPoint.Hash = intersectBlock.Hash
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if intersectPoint.Slot == 0 && !foundOrigin {
		return nil, ErrIntersectNotFound
	}
	// Increment current largest chain ID for new ID
	chainIds := slices.Sorted(maps.Keys(cm.chains))
	chainId := chainIds[len(chainIds)-1] + 1
	c := &Chain{
		id:                   chainId,
		manager:              cm,
		eventBus:             cm.eventBus,
		persistent:           false,
		lastCommonBlockIndex: intersectBlock.ID,
		tipBlockIndex:        intersectBlock.ID,
		currentTip: ochainsync.Tip{
			Point: ocommon.Point{
				Slot: intersectBlock.Slot,
				Hash: intersectBlock.Hash,
			},
			BlockNumber: intersectBlock.Number,
		},
	}
	cm.chains[chainId] = c
	return c, nil
}

func (cm *ChainManager) BlockByPoint(
	point ocommon.Point,
	txn *database.Txn,
) (database.Block, error) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.blockByPoint(point, txn)
}

func (cm *ChainManager) blockByPoint(
	point ocommon.Point,
	txn *database.Txn,
) (database.Block, error) {
	// Check in-memory blocks
	if blk, ok := cm.blocks[string(point.Hash)]; ok {
		if blk.Slot == point.Slot {
			return blk, nil
		}
	}
	// Query database
	if cm.db != nil {
		var tmpBlock database.Block
		var err error
		if txn == nil {
			tmpBlock, err = database.BlockByPoint(cm.db, point)
		} else {
			tmpBlock, err = database.BlockByPointTxn(txn, point)
		}
		if err != nil {
			if errors.Is(err, database.ErrBlockNotFound) {
				return database.Block{}, ErrBlockNotFound
			}
			return database.Block{}, err
		}
		return tmpBlock, nil
	}
	return database.Block{}, ErrBlockNotFound
}

func (cm *ChainManager) blockByHash(
	blockHash []byte,
) (database.Block, error) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	// Check in-memory blocks
	if blk, ok := cm.blocks[string(blockHash)]; ok {
		return blk, nil
	}
	return database.Block{}, ErrBlockNotFound
}

func (cm *ChainManager) blockByIndex(
	blockIndex uint64,
	txn *database.Txn,
) (database.Block, error) {
	// Query database
	if cm.db != nil {
		tmpBlock, err := cm.db.BlockByIndex(blockIndex, txn)
		if err != nil {
			if errors.Is(err, database.ErrBlockNotFound) {
				return database.Block{}, ErrBlockNotFound
			}
			return database.Block{}, err
		}
		return tmpBlock, nil
	}
	return database.Block{}, ErrBlockNotFound
}

func (cm *ChainManager) loadPrimaryChain() error {
	persistent := (cm.db != nil)
	chain := &Chain{
		id:         primaryChainId,
		manager:    cm,
		eventBus:   cm.eventBus,
		persistent: persistent,
	}
	if persistent {
		recentBlocks, err := database.BlocksRecent(cm.db, 1)
		if err != nil {
			return err
		}
		if len(recentBlocks) > 0 {
			chain.currentTip = ochainsync.Tip{
				Point: ocommon.Point{
					Slot: recentBlocks[0].Slot,
					Hash: recentBlocks[0].Hash,
				},
				BlockNumber: recentBlocks[0].Number,
			}
			chain.tipBlockIndex = recentBlocks[0].ID
		}
	}
	cm.chains[primaryChainId] = chain
	return nil
}

func (cm *ChainManager) addBlock(block database.Block, txn *database.Txn, persistent bool) error {
	if persistent {
		// Add block to database
		if err := cm.db.BlockCreate(block, txn); err != nil {
			return err
		}
		// TODO: trigger periodic async signal to chains to do reconcile to prune buffer
	} else {
		// Add block to memory buffer
		cm.blocks[string(block.Hash)] = block
	}
	return nil
}

func (cm *ChainManager) removeBlockByIndex(blockIndex uint64) error {
	// Record removed block event for each non-primary chain
	for chainId := range cm.chains {
		if chainId == primaryChainId {
			continue
		}
		cm.chainRollbackEvents[chainId] = append(
			cm.chainRollbackEvents[chainId],
			blockIndex,
		)
	}
	// Remove from database
	txn := cm.db.BlobTxn(true)
	err := txn.Do(func(txn *database.Txn) error {
		tmpBlock, err := cm.db.BlockByIndex(blockIndex, txn)
		if err != nil {
			return err
		}
		// Add block to memory buffer in case other chains are using it
		cm.blocks[string(tmpBlock.Hash)] = tmpBlock
		if err := database.BlockDeleteTxn(txn, tmpBlock); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (cm *ChainManager) chainNeedsReconcile(chainId ChainId, lastCommonBlockIndex uint64) bool {
	events, ok := cm.chainRollbackEvents[chainId]
	if !ok {
		return false
	}
	ret := false
	for _, evtIndex := range events {
		if evtIndex <= lastCommonBlockIndex {
			ret = true
			break
		}
	}
	// Clear out events
	delete(cm.chainRollbackEvents, chainId)
	return ret
}
