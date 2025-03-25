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

	"github.com/blinklabs-io/dingo/database"
	ochainsync "github.com/blinklabs-io/gouroboros/protocol/chainsync"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

var ErrIteratorChainTip = errors.New("chain iterator is at chain tip")

type ChainIterator struct {
	ls          *LedgerState
	startPoint  ocommon.Point
	blockNumber uint64
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
	ci := &ChainIterator{
		ls:         ls,
		startPoint: startPoint,
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
	return ci, nil
}

func (ci *ChainIterator) Tip() (ochainsync.Tip, error) {
	tmpBlocks, err := database.BlocksRecent(ci.ls.db, 1)
	if err != nil {
		return ochainsync.Tip{}, err
	}
	var tmpBlock database.Block
	if len(tmpBlocks) > 0 {
		tmpBlock = tmpBlocks[0]
	}
	tip := ochainsync.Tip{
		Point:       ocommon.NewPoint(tmpBlock.Slot, tmpBlock.Hash),
		BlockNumber: tmpBlock.Number,
	}
	return tip, nil
}

func (ci *ChainIterator) Next(blocking bool) (*ChainIteratorResult, error) {
	ci.ls.RLock()
	ret := &ChainIteratorResult{}
	// Lookup next block in metadata DB
	tmpBlock, err := database.BlockByNumber(ci.ls.db, ci.blockNumber)
	// Return immedidately if a block is found
	if err == nil {
		ret.Point = ocommon.NewPoint(tmpBlock.Slot, tmpBlock.Hash)
		ret.Block = tmpBlock
		ci.blockNumber++
		ci.ls.RUnlock()
		return ret, nil
	}
	// Return any actual error
	if !errors.Is(err, database.ErrBlockNotFound) {
		ci.ls.RUnlock()
		return ret, err
	}
	// Check against current tip to see if it was rolled back
	tip, err := ci.Tip()
	if err != nil {
		return nil, err
	}
	if ci.blockNumber > 0 && ci.blockNumber-1 > tip.BlockNumber {
		ret.Point = tip.Point
		ret.Rollback = true
		ci.ls.RUnlock()
		return ret, nil
	}
	// Return immediately if we're not blocking
	if !blocking {
		ci.ls.RUnlock()
		return nil, ErrIteratorChainTip
	}
	// Wait for new block or a rollback
	blockSubId, blockChan := ci.ls.config.EventBus.Subscribe(
		ChainBlockEventType,
	)
	rollbackSubId, rollbackChan := ci.ls.config.EventBus.Subscribe(
		ChainRollbackEventType,
	)
	// Release read lock while we wait for new event
	ci.ls.RUnlock()
	select {
	case blockEvt, ok := <-blockChan:
		if !ok {
			// TODO: return an actual error (#389)
			return nil, nil
		}
		blockData := blockEvt.Data.(ChainBlockEvent)
		ret.Point = blockData.Point
		ret.Block = blockData.Block
		ci.blockNumber++
	case rollbackEvt, ok := <-rollbackChan:
		if !ok {
			// TODO: return an actual error (#389)
			return nil, nil
		}
		rollbackData := rollbackEvt.Data.(ChainRollbackEvent)
		ret.Point = rollbackData.Point
		ret.Rollback = true
	}
	ci.ls.config.EventBus.Unsubscribe(ChainBlockEventType, blockSubId)
	ci.ls.config.EventBus.Unsubscribe(ChainRollbackEventType, rollbackSubId)
	return ret, nil
}
