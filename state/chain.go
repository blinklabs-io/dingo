// Copyright 2024 Blink Labs Software
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

	"github.com/blinklabs-io/node/state/models"
	"gorm.io/gorm"

	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type ChainIterator struct {
	ls          *LedgerState
	startPoint  ocommon.Point
	blockNumber uint64
}

type ChainIteratorResult struct {
	Point    ocommon.Point
	Block    models.Block
	Rollback bool
}

func newChainIterator(
	ls *LedgerState,
	startPoint ocommon.Point,
	inclusive bool,
) (*ChainIterator, error) {
	ls.RLock()
	defer ls.RUnlock()
	// Lookup start block in metadata DB
	tmpBlock, err := models.BlockByPoint(ls.db, startPoint)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrBlockNotFound
		}
		return nil, err
	}
	ci := &ChainIterator{
		ls:          ls,
		startPoint:  startPoint,
		blockNumber: tmpBlock.Number,
	}
	// Increment next block number is non-inclusive
	if !inclusive {
		ci.blockNumber++
	}
	return ci, nil
}

func (ci *ChainIterator) Next(blocking bool) (*ChainIteratorResult, error) {
	ci.ls.RLock()
	ret := &ChainIteratorResult{}
	// Lookup next block in metadata DB
	tmpBlock, err := models.BlockByNumber(ci.ls.db, ci.blockNumber)
	// Return immedidately if a block is found
	if err == nil {
		ret.Point = ocommon.NewPoint(tmpBlock.Slot, tmpBlock.Hash)
		ret.Block = tmpBlock
		ci.blockNumber++
		ci.ls.RUnlock()
		return ret, nil
	}
	// Return any actual error
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		ci.ls.RUnlock()
		return ret, err
	}
	// Return immediately if we're not blocking
	if !blocking {
		ci.ls.RUnlock()
		return nil, nil
	}
	// Wait for new block or a rollback
	blockSubId, blockChan := ci.ls.eventBus.Subscribe(ChainBlockEventType)
	rollbackSubId, rollbackChan := ci.ls.eventBus.Subscribe(
		ChainRollbackEventType,
	)
	// Release read lock while we wait for new event
	ci.ls.RUnlock()
	select {
	case blockEvt, ok := <-blockChan:
		if !ok {
			// TODO
			return nil, nil
		}
		blockData := blockEvt.Data.(ChainBlockEvent)
		ret.Point = blockData.Point
		ret.Block = blockData.Block
		ci.blockNumber++
	case rollbackEvt, ok := <-rollbackChan:
		if !ok {
			// TODO
			return nil, nil
		}
		rollbackData := rollbackEvt.Data.(ChainRollbackEvent)
		ret.Point = rollbackData.Point
		ret.Rollback = true
	}
	ci.ls.eventBus.Unsubscribe(ChainBlockEventType, blockSubId)
	ci.ls.eventBus.Unsubscribe(ChainRollbackEventType, rollbackSubId)
	return ret, nil
}
