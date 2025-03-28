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
	return ci.ls.chainTip(nil)
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
		ci.blockNumber = tip.BlockNumber + 1
		ci.ls.RUnlock()
		return ret, nil
	}
	// Return immediately if we're not blocking
	if !blocking {
		ci.ls.RUnlock()
		return nil, ErrIteratorChainTip
	}
	// Wait for new block or a rollback
	chainUpdateSubId, chainUpdateChan := ci.ls.config.EventBus.Subscribe(
		ChainUpdateEventType,
	)
	// Release read lock while we wait for new event
	ci.ls.RUnlock()
	evt, ok := <-chainUpdateChan
	if !ok {
		// TODO: return an actual error (#389)
		return nil, nil
	}
	switch e := evt.Data.(type) {
	case ChainBlockEvent:
		ret.Point = e.Point
		ret.Block = e.Block
		ci.blockNumber++
	case ChainRollbackEvent:
		ret.Point = e.Point
		ret.Rollback = true
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
	ci.ls.config.EventBus.Unsubscribe(ChainUpdateEventType, chainUpdateSubId)
	return ret, nil
}
