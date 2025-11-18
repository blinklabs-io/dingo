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
	"context"

	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

type ChainIterator struct {
	chain          *Chain
	startPoint     ocommon.Point
	lastPoint      ocommon.Point
	rollbackPoint  ocommon.Point
	nextBlockIndex uint64
	needsRollback  bool
}

type ChainIteratorResult struct {
	Point    ocommon.Point
	Block    models.Block
	Rollback bool
}

func newChainIterator(
	chain *Chain,
	startPoint ocommon.Point,
	inclusive bool,
) (*ChainIterator, error) {
	ci := &ChainIterator{
		chain:          chain,
		startPoint:     startPoint,
		nextBlockIndex: initialBlockIndex,
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
	return ci, nil
}

func (ci *ChainIterator) Next(
	ctx context.Context,
	blocking bool,
) (*ChainIteratorResult, error) {
	return ci.chain.iterNext(ctx, ci, blocking)
}
