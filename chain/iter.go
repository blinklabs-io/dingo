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
	reverse        bool
	ctx            context.Context
	cancel         context.CancelFunc
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
	reverse bool,
) (*ChainIterator, error) {
	ctx, cancel := context.WithCancel(context.Background())
	ci := &ChainIterator{
		chain:          chain,
		startPoint:     startPoint,
		nextBlockIndex: initialBlockIndex,
		reverse:        reverse,
		ctx:            ctx,
		cancel:         cancel,
	}
	// Lookup start block in metadata DB if not origin
	if startPoint.Slot > 0 || len(startPoint.Hash) > 0 {
		tmpBlock, err := chain.BlockByPoint(startPoint, nil)
		if err != nil {
			return nil, err
		}
		ci.nextBlockIndex = tmpBlock.ID
		if !inclusive {
			if reverse {
				// Walking backward: the first block returned must
				// precede startPoint.
				if ci.nextBlockIndex <= initialBlockIndex {
					// Non-inclusive reverse from the first block has
					// no predecessor; mark as already past origin.
					ci.nextBlockIndex = 0
				} else {
					ci.nextBlockIndex--
				}
			} else {
				ci.nextBlockIndex++
			}
		}
	} else if reverse {
		// Reverse iteration from origin has no blocks to deliver.
		ci.nextBlockIndex = 0
	}
	return ci, nil
}

func (ci *ChainIterator) Next(blocking bool) (*ChainIteratorResult, error) {
	return ci.chain.iterNext(ci, blocking)
}

func (ci *ChainIterator) Cancel() {
	if ci.cancel != nil {
		ci.cancel()
	}
	// Remove from chain's iterator list to prevent memory leak
	if ci.chain != nil {
		ci.chain.removeIterator(ci)
	}
}
