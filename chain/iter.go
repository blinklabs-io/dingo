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
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

var ErrIteratorChainTip = errors.New("chain iterator is at chain tip")

type ChainIterator struct {
	mutex            sync.Mutex
	chain            *Chain
	startPoint       ocommon.Point
	nextBlockIndex   uint64
	lastPoint        ocommon.Point
	chainUpdateSubId event.EventSubscriberId
	chainUpdateChan  <-chan event.Event
	needsRollback    bool
	rollbackPoint    ocommon.Point
	waitingChan      chan event.Event
}

type ChainIteratorResult struct {
	Point    ocommon.Point
	Block    database.Block
	Rollback bool
}

func newChainIterator(
	chain *Chain,
	startPoint ocommon.Point,
	inclusive bool,
) (*ChainIterator, error) {
	chain.mutex.RLock()
	defer chain.mutex.RUnlock()
	// Subscribe to chain updates
	chainUpdateSubId, chainUpdateChan := chain.eventBus.Subscribe(
		ChainUpdateEventType,
	)
	ci := &ChainIterator{
		chain:            chain,
		startPoint:       startPoint,
		chainUpdateSubId: chainUpdateSubId,
		chainUpdateChan:  chainUpdateChan,
		nextBlockIndex:   initialBlockIndex,
	}
	// Lookup start block in metadata DB if not origin
	if startPoint.Slot > 0 || len(startPoint.Hash) > 0 {
		tmpBlock, err := chain.BlockByPoint(startPoint, nil)
		if err != nil {
			return nil, err
		}
		ci.nextBlockIndex = tmpBlock.ID
		// Increment next block number if non-inclusive
		if !inclusive {
			ci.nextBlockIndex++
		}
	}
	go ci.handleChainUpdateEvents()
	return ci, nil
}

func (ci *ChainIterator) handleChainUpdateEvents() {
	for {
		evt, ok := <-ci.chainUpdateChan
		if !ok {
			return
		}
		ci.mutex.Lock()
		switch e := evt.Data.(type) {
		case ChainBlockEvent:
			if ci.waitingChan != nil {
				// Send event without blocking
				select {
				case ci.waitingChan <- evt:
				default:
				}
			}
		case ChainRollbackEvent:
			if ci.lastPoint.Slot > e.Point.Slot {
				ci.rollbackPoint = e.Point
				ci.needsRollback = true
			}
			if ci.waitingChan != nil {
				// Send event without blocking
				select {
				case ci.waitingChan <- evt:
				default:
				}
			}
		}
		ci.mutex.Unlock()
	}
}

func (ci *ChainIterator) Next(blocking bool) (*ChainIteratorResult, error) {
	// We lock the chain first to prevent a deadlock
	ci.chain.mutex.RLock()
	ci.mutex.Lock()
	// Check for pending rollback
	if ci.needsRollback {
		ret := &ChainIteratorResult{}
		ret.Point = ci.rollbackPoint
		ret.Rollback = true
		ci.lastPoint = ci.rollbackPoint
		ci.needsRollback = false
		if ci.rollbackPoint.Slot > 0 {
			// Lookup block index for rollback point
			tmpBlock, err := ci.chain.BlockByPoint(ci.rollbackPoint, nil)
			if err != nil {
				ci.mutex.Unlock()
				ci.chain.mutex.RUnlock()
				return nil, err
			}
			ci.nextBlockIndex = tmpBlock.ID + 1
		}
		ci.mutex.Unlock()
		ci.chain.mutex.RUnlock()
		return ret, nil
	}
	ret := &ChainIteratorResult{}
	// Lookup next block in metadata DB
	tmpBlock, err := ci.chain.blockByIndex(ci.nextBlockIndex, nil)
	// Return immedidately if a block is found
	if err == nil {
		ret.Point = ocommon.NewPoint(tmpBlock.Slot, tmpBlock.Hash)
		ret.Block = tmpBlock
		ci.nextBlockIndex++
		ci.lastPoint = ret.Point
		ci.mutex.Unlock()
		ci.chain.mutex.RUnlock()
		return ret, nil
	}
	// Return any actual error
	if !errors.Is(err, ErrBlockNotFound) {
		ci.mutex.Unlock()
		ci.chain.mutex.RUnlock()
		return ret, err
	}
	// Return immediately if we're not blocking
	if !blocking {
		ci.mutex.Unlock()
		ci.chain.mutex.RUnlock()
		return nil, ErrIteratorChainTip
	}
	ci.chain.mutex.RUnlock()
	// Wait for chain update
	ci.waitingChan = make(chan event.Event, 1)
	// Release read lock while we wait for new event
	ci.mutex.Unlock()
	_, ok := <-ci.waitingChan
	if !ok {
		return nil, ErrBlockNotFound
	}
	// Call ourselves again now that we should have new data
	return ci.Next(blocking)
}
