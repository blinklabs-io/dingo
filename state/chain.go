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

package state

import (
	"errors"
	"fmt"
	"sync"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/event"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

var ErrIteratorChainTip = errors.New("chain iterator is at chain tip")

type ChainIterator struct {
	mutex            sync.Mutex
	ls               *LedgerState
	startPoint       ocommon.Point
	blockNumber      uint64
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
	ls *LedgerState,
	startPoint ocommon.Point,
	inclusive bool,
) (*ChainIterator, error) {
	// Subscribe to chain updates
	chainUpdateSubId, chainUpdateChan := ls.config.EventBus.Subscribe(
		ChainUpdateEventType,
	)
	ci := &ChainIterator{
		ls:               ls,
		startPoint:       startPoint,
		chainUpdateSubId: chainUpdateSubId,
		chainUpdateChan:  chainUpdateChan,
	}
	// Lookup start block in metadata DB if not origin
	if startPoint.Slot > 0 || len(startPoint.Hash) > 0 {
		tmpBlock, err := database.BlockByPoint(ls.db, startPoint)
		if err != nil {
			if errors.Is(err, database.ErrBlockNotFound) {
				return nil, ErrBlockNotFound
			}
			return nil, err
		}
		ci.blockNumber = tmpBlock.Number
		// Increment next block number if non-inclusive
		if !inclusive {
			ci.blockNumber++
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
			if ci.blockNumber > 0 {
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

func (ci *ChainIterator) Tip() (ochainsync.Tip, error) {
	return ci.ls.chainTip(nil)
}

func (ci *ChainIterator) Next(blocking bool) (*ChainIteratorResult, error) {
	ci.mutex.Lock()
	// Check for pending rollback
	if ci.needsRollback {
		ret := &ChainIteratorResult{}
		ret.Point = ci.rollbackPoint
		ret.Rollback = true
		ci.needsRollback = false
		if ci.rollbackPoint.Slot > 0 {
			// Lookup block number for rollback point
			tmpBlock, err := database.BlockByPoint(ci.ls.db, ci.rollbackPoint)
			if err != nil {
				ci.mutex.Unlock()
				return nil, err
			}
			ci.blockNumber = tmpBlock.Number + 1
		}
		ci.mutex.Unlock()
		return ret, nil
	}
	ret := &ChainIteratorResult{}
	// Lookup next block in metadata DB
	tmpBlock, err := database.BlockByNumber(ci.ls.db, ci.blockNumber)
	// Return immedidately if a block is found
	if err == nil {
		ret.Point = ocommon.NewPoint(tmpBlock.Slot, tmpBlock.Hash)
		ret.Block = tmpBlock
		ci.blockNumber++
		ci.mutex.Unlock()
		return ret, nil
	}
	// Return any actual error
	if !errors.Is(err, database.ErrBlockNotFound) {
		ci.mutex.Unlock()
		return ret, err
	}
	// Return immediately if we're not blocking
	if !blocking {
		ci.mutex.Unlock()
		return nil, ErrIteratorChainTip
	}
	// Wait for chain update
	ci.waitingChan = make(chan event.Event, 1)
	// Release read lock while we wait for new event
	ci.mutex.Unlock()
	evt, ok := <-ci.waitingChan
	if !ok {
		// TODO: return an actual error (#389)
		return nil, nil
	}
	ci.mutex.Lock()
	defer ci.mutex.Unlock()
	ci.waitingChan = nil
	switch e := evt.Data.(type) {
	case ChainBlockEvent:
		ret.Point = e.Point
		ret.Block = e.Block
		ci.blockNumber++
	case ChainRollbackEvent:
		ret.Point = e.Point
		ret.Rollback = true
		ci.needsRollback = false
		if e.Point.Slot > 0 {
			// Lookup block number for rollback point
			tmpBlock, err := database.BlockByPoint(ci.ls.db, e.Point)
			if err != nil {
				return nil, err
			}
			ci.blockNumber = tmpBlock.Number + 1
		}
	default:
		return nil, fmt.Errorf("unexpected event type %T", e)
	}
	return ret, nil
}
