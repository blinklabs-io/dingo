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
	rollbackBlocks []models.Block
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
	// RollbackBlocks contains the blocks removed while reaching Point, in
	// newest-first order. The payload is carried because persistent chain
	// rollback removes block bodies before a lagging ledger iterator handles
	// the rollback signal.
	RollbackBlocks []models.Block
}

func newChainIteratorWithContext(
	ctx context.Context,
	parentCtx context.Context,
	chain *Chain,
	startPoint ocommon.Point,
	inclusive bool,
	reverse bool,
) (*ChainIterator, error) {
	if parentCtx == nil {
		return nil, errors.New("chain iterator context is nil")
	}
	startBlockIndex := initialBlockIndex
	// Lookup start block in metadata DB if not origin
	if startPoint.Slot > 0 || len(startPoint.Hash) > 0 {
		tmpBlock, err := chain.BlockByPoint(ctx, startPoint, nil)
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
		if !chain.holdsBlockAtIndex(ctx, tmpBlock.ID, startPoint.Hash) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return nil, models.ErrBlockNotFound
		}
		startBlockIndex = tmpBlock.ID
	}
	return newChainIteratorAtIndex(
		parentCtx,
		chain,
		startPoint,
		startBlockIndex,
		inclusive,
		reverse,
	), nil
}

// newChainIteratorAtIndex constructs an iterator after the caller has verified
// that startBlockIndex is the active index entry for startPoint.
func newChainIteratorAtIndex(
	parentCtx context.Context,
	chain *Chain,
	startPoint ocommon.Point,
	startBlockIndex uint64,
	inclusive bool,
	reverse bool,
) *ChainIterator {
	iterCtx, cancel := context.WithCancel(parentCtx)
	ci := &ChainIterator{
		chain:          chain,
		startPoint:     startPoint,
		nextBlockIndex: startBlockIndex,
		reverse:        reverse,
		ctx:            iterCtx,
		cancel:         cancel,
	}
	if startPoint.Slot == 0 && len(startPoint.Hash) == 0 && reverse {
		// Reverse iteration from origin has no blocks to deliver.
		ci.nextBlockIndex = 0
	} else if (startPoint.Slot > 0 || len(startPoint.Hash) > 0) && !inclusive {
		if reverse {
			if ci.nextBlockIndex <= initialBlockIndex {
				ci.nextBlockIndex = 0
			} else {
				ci.nextBlockIndex--
			}
		} else {
			ci.nextBlockIndex++
		}
	}
	return ci
}

func (ci *ChainIterator) startCancelWatcher() {
	go func() {
		<-ci.ctx.Done()
		ci.Cancel()
	}()
}

func (ci *ChainIterator) Next(blocking bool) (*ChainIteratorResult, error) {
	ret, err := ci.chain.iterNext(ci.ctx, ci, blocking)
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

// HasPendingRollback reports whether the chain has invalidated the iterator's
// current forward position. It does not consume the rollback event.
func (ci *ChainIterator) HasPendingRollback() bool {
	if ci.chain == nil {
		return false
	}
	ci.chain.mutex.Lock()
	defer ci.chain.mutex.Unlock()
	return ci.needsRollback
}
