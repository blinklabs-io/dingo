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
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/event"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
)

type ChainId uint64

const (
	primaryChainId ChainId = 1
)

type ChainManager struct {
	db                  *database.Database
	eventBus            *event.EventBus
	securityParam       int
	chains              map[ChainId]*Chain
	chainRollbackEvents map[ChainId][]uint64
	blockCache          *blockCache
	mutex               sync.RWMutex
}

func NewManager(
	db *database.Database,
	eventBus *event.EventBus,
	promRegistry ...prometheus.Registerer,
) (*ChainManager, error) {
	var registry prometheus.Registerer
	if len(promRegistry) > 0 {
		registry = promRegistry[0]
	}
	cm := &ChainManager{
		db:       db,
		eventBus: eventBus,
		chains:   make(map[ChainId]*Chain),
		chainRollbackEvents: make(
			map[ChainId][]uint64,
		),
		blockCache: newBlockCache(
			DefaultBlockCacheCapacity,
			registry,
		),
	}
	if err := cm.loadPrimaryChain(); err != nil {
		return nil, err
	}
	return cm, nil
}

func (cm *ChainManager) SetLedger(
	ledgerState interface{ SecurityParam() int },
) {
	cm.securityParam = ledgerState.SecurityParam()
}

func (cm *ChainManager) PrimaryChain() *Chain {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.primaryChainLocked()
}

// primaryChainLocked returns the primary chain without acquiring the mutex.
// The caller must already hold cm.mutex (read or write).
func (cm *ChainManager) primaryChainLocked() *Chain {
	if cm.chains == nil {
		return nil
	}
	return cm.chains[primaryChainId]
}

func (cm *ChainManager) Chain(id ChainId) *Chain {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.chains[id]
}

// NewChain creates a new Chain that forks from the primary chain at the specified point. This is useful for managing outbound ChainSync clients
func (cm *ChainManager) NewChain(point ocommon.Point) (*Chain, error) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	primaryChain := cm.primaryChainLocked()
	if primaryChain == nil {
		return nil, errors.New("primary chain not available")
	}
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
func (cm *ChainManager) NewChainFromIntersect(
	points []ocommon.Point,
) (*Chain, error) {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	primaryChain := cm.primaryChainLocked()
	if primaryChain == nil {
		return nil, errors.New("primary chain not available")
	}
	primaryChain.mutex.Lock()
	defer primaryChain.mutex.Unlock()
	tip := primaryChain.currentTip
	var intersectPoint ocommon.Point
	var intersectBlock models.Block
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
				if errors.Is(err, models.ErrBlockNotFound) {
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
) (models.Block, error) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	return cm.blockByPoint(point, txn)
}

func (cm *ChainManager) blockByPoint(
	point ocommon.Point,
	txn *database.Txn,
) (models.Block, error) {
	// Check in-memory cache
	if blk, ok := cm.blockCache.Get(point.Hash); ok {
		if blk.Slot == point.Slot {
			return blk, nil
		}
	}
	// Query database
	if cm.db != nil {
		var tmpBlock models.Block
		var err error
		if txn == nil {
			tmpBlock, err = database.BlockByPoint(cm.db, point)
		} else {
			tmpBlock, err = database.BlockByPointTxn(txn, point)
		}
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				return models.Block{}, models.ErrBlockNotFound
			}
			return models.Block{}, err
		}
		return tmpBlock, nil
	}
	return models.Block{}, models.ErrBlockNotFound
}

func (cm *ChainManager) blockByHash(
	blockHash []byte,
) (models.Block, error) {
	cm.mutex.RLock()
	defer cm.mutex.RUnlock()
	// Check in-memory cache
	if blk, ok := cm.blockCache.Get(blockHash); ok {
		return blk, nil
	}
	return models.Block{}, models.ErrBlockNotFound
}

func (cm *ChainManager) blockByIndex(
	blockIndex uint64,
	txn *database.Txn,
) (models.Block, error) {
	// Query database
	if cm.db != nil {
		tmpBlock, err := cm.db.BlockByIndex(blockIndex, txn)
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				return models.Block{}, models.ErrBlockNotFound
			}
			return models.Block{}, err
		}
		return tmpBlock, nil
	}
	return models.Block{}, models.ErrBlockNotFound
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

// RewindPrimaryChainToPoint silently prunes the persistent primary chain back
// to the specified point without emitting rollback/fork events. This runs
// during startup reconciliation to discard speculative blob-only blocks that
// were never applied to the authoritative ledger metadata tip, so emitting the
// normal ChainRollbackEvent/ChainForkEvent flow would be incorrect and could
// trigger external undo/transaction rollback handlers for work that never
// actually happened. Callers should treat this as persistent chain pruning
// only, not a normal rollback/fork transition.
func (cm *ChainManager) RewindPrimaryChainToPoint(
	point ocommon.Point,
) error {
	cm.mutex.Lock()
	defer cm.mutex.Unlock()
	primaryChain := cm.primaryChainLocked()
	if primaryChain == nil {
		return errors.New("primary chain not available")
	}
	if !primaryChain.persistent {
		return errors.New("primary chain is not persistent")
	}
	primaryChain.mutex.Lock()
	defer primaryChain.mutex.Unlock()

	rollbackIndex := uint64(0)
	rollbackBlockNumber := uint64(0)
	targetTip := ochainsync.Tip{}
	targetTipSet := false
	err := func() error {
		txn := cm.db.BlobTxn(true)
		return txn.Do(func(txn *database.Txn) error {
			currentTip := primaryChain.currentTip
			if point.Slot > 0 || len(point.Hash) > 0 {
				tmpBlock, err := cm.blockByPoint(point, txn)
				if err != nil {
					return fmt.Errorf("lookup rewind point: %w", err)
				}
				rollbackIndex = tmpBlock.ID
				rollbackBlockNumber = tmpBlock.Number
				if primaryChain.tipBlockIndex < rollbackIndex {
					return fmt.Errorf(
						"primary chain tip index %d is behind rewind point index %d",
						primaryChain.tipBlockIndex,
						rollbackIndex,
					)
				}
				if primaryChain.tipBlockIndex == rollbackIndex &&
					primaryChain.currentTip.Point.Slot == point.Slot &&
					bytes.Equal(primaryChain.currentTip.Point.Hash, point.Hash) {
					// Already at the requested tip, so there is nothing to prune.
					targetTip = primaryChain.currentTip
					targetTipSet = true
					return nil
				}
				targetTip = ochainsync.Tip{
					Point:       point,
					BlockNumber: rollbackBlockNumber,
				}
				targetTipSet = true
			} else {
				rollbackIndex = 0
				targetTip = ochainsync.Tip{
					Point:       point,
					BlockNumber: 0,
				}
				targetTipSet = true
			}
			for currentTip.Point.Slot != point.Slot ||
				!bytes.Equal(currentTip.Point.Hash, point.Hash) {
				currentBlock, err := cm.blockByPoint(currentTip.Point, txn)
				if err != nil {
					return fmt.Errorf(
						"lookup current primary block: %w",
						err,
					)
				}
				if err := database.BlockDeleteTxn(txn, currentBlock); err != nil {
					return fmt.Errorf(
						"delete primary block %d: %w",
						currentBlock.ID,
						err,
					)
				}
				if len(currentBlock.PrevHash) == 0 {
					currentTip = ochainsync.Tip{}
					break
				}
				prevBlock, err := database.BlockByHashTxn(txn, currentBlock.PrevHash)
				if err != nil {
					return fmt.Errorf(
						"lookup previous primary block: %w",
						err,
					)
				}
				currentTip = ochainsync.Tip{
					Point: ocommon.NewPoint(
						prevBlock.Slot,
						prevBlock.Hash,
					),
					BlockNumber: prevBlock.Number,
				}
			}
			if !targetTipSet {
				targetTip = currentTip
			}
			return nil
		})
	}()
	if err != nil {
		return err
	}
	primaryChain.headers = primaryChain.headers[:0]
	primaryChain.tipBlockIndex = rollbackIndex
	primaryChain.currentTip = targetTip
	return nil
}

func (cm *ChainManager) addBlock(
	block models.Block,
	txn *database.Txn,
	persistent bool,
) error {
	if persistent {
		// Add block to database
		if err := cm.db.BlockCreate(block, txn); err != nil {
			return err
		}
	} else {
		// Add block to LRU cache (evicts oldest if at capacity)
		cm.blockCache.Put(block)
	}
	return nil
}

func (cm *ChainManager) removeBlockByIndex(
	blockIndex uint64,
) (models.Block, error) {
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
	var removedBlock models.Block
	txn := cm.db.BlobTxn(true)
	err := txn.Do(func(txn *database.Txn) error {
		tmpBlock, err := cm.db.BlockByIndex(blockIndex, txn)
		if err != nil {
			return err
		}
		removedBlock = tmpBlock
		// Add block to LRU cache in case other chains are using it
		cm.blockCache.Put(tmpBlock)
		if err := database.BlockDeleteTxn(txn, tmpBlock); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return models.Block{}, err
	}
	return removedBlock, nil
}

func (cm *ChainManager) chainNeedsReconcile(
	chainId ChainId,
	lastCommonBlockIndex uint64,
) bool {
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
