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
	"errors"
	"sync"

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
	cancelOnce     sync.Once
}

type ChainIteratorResult struct {
	Point    ocommon.Point
	Block    models.Block
	Rollback bool
}

func newChainIteratorWithContext(
	parentCtx context.Context,
	chain *Chain,
	startPoint ocommon.Point,
	inclusive bool,
	reverse bool,
) (*ChainIterator, error) {
	if parentCtx == nil {
		return nil, errors.New("chain iterator context is nil")
	}
	iterCtx, cancel := context.WithCancel(parentCtx)
	ci := &ChainIterator{
		chain:          chain,
		startPoint:     startPoint,
		nextBlockIndex: initialBlockIndex,
		reverse:        reverse,
		ctx:            iterCtx,
		cancel:         cancel,
	}
	// Lookup start block in metadata DB if not origin
	if startPoint.Slot > 0 || len(startPoint.Hash) > 0 {
		tmpBlock, err := chain.BlockByPoint(startPoint, nil)
		if err != nil {
			return nil, err
		}
		// A block this chain rolled back stays resolvable by point:
		// removeBlockByIndex deletes the row but retains the block in the
		// manager's LRU cache so non-primary chains can still reconcile
		// against it. Positioning an iterator at that index hands the
		// caller an iterator that can never yield the block it asked for,
		// because the index is no longer part of this chain. Blockfetch
		// turns that into a StartBatch/BatchDone pair carrying no blocks,
		// which the requesting peer cannot tell apart from a served range,
		// so it re-requests the same range instead of asking another peer.
		// Reject the point here so callers get "not found" and can fail
		// over (blockfetch answers NoBlocks).
		if !chain.holdsBlockAtIndex(tmpBlock.ID, startPoint.Hash) {
			return nil, models.ErrBlockNotFound
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

func (ci *ChainIterator) startCancelWatcher() {
	go func() {
		<-ci.ctx.Done()
		ci.Cancel()
	}()
}

func (ci *ChainIterator) Next(blocking bool) (*ChainIteratorResult, error) {
	ret, err := ci.chain.iterNext(ci, blocking)
	if ret == nil && err == nil {
		return nil, ErrIteratorChainTip
	}
	return ret, err
}

func (ci *ChainIterator) Cancel() {
	ci.cancelOnce.Do(func() {
		if ci.cancel != nil {
			ci.cancel()
		}
		// Remove from chain's iterator list to prevent memory leak
		if ci.chain != nil {
			ci.chain.removeIterator(ci)
		}
	})
}
