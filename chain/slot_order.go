// Copyright 2026 Blink Labs Software
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
	"context"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
)

// SlotParent describes the header or block another one extends, as far as
// SlotFollowsParent needs it.
type SlotParent struct {
	Slot  uint64
	EraId uint8
	// Boundary is set when the parent is a Byron epoch boundary block.
	Boundary bool
	// Known is false when the parent's era and kind could not be determined.
	Known bool
}

// SlotFollowsParent reports whether a header or block of era eraId at slot may
// extend parent. Its slot must be later than the parent's, as every header's
// envelope must be in ouroboros-consensus, with two exceptions:
//
//   - A Byron block that is not an epoch boundary block may share the slot of
//     an epoch boundary parent, which sits at the first slot of its epoch.
//   - A later-era block extending a Byron parent is not ordered here. A Byron
//     slot number is derived with a fixed epoch length, which equals the
//     absolute slot only on networks whose Byron epochs have that length; the
//     ledger's envelope check orders that boundary with the network's own
//     Byron epoch length.
//
// A parent whose era and kind are unknown gets the plain later-slot rule.
func SlotFollowsParent(
	eraId uint8,
	boundary bool,
	slot uint64,
	parent SlotParent,
) bool {
	if slot > parent.Slot {
		return true
	}
	if !parent.Known {
		return false
	}
	if parent.EraId == byron.EraIdByron && eraId != byron.EraIdByron {
		return true
	}
	return eraId == byron.EraIdByron &&
		!boundary &&
		parent.Boundary &&
		slot == parent.Slot
}

// IsByronBoundaryHeader reports whether header is a Byron epoch boundary
// block header.
func IsByronBoundaryHeader(header ledger.BlockHeader) bool {
	_, ok := header.(*byron.ByronEpochBoundaryBlockHeader)
	return ok
}

func isByronBoundaryBlock(block ledger.Block) bool {
	return block.Type() == ledger.BlockTypeByronEbb
}

// parentLinkLocked returns the slot, era and kind of the header or block
// prevHash names when it is this chain's last queued header or its tip block,
// the two parents a header can be admitted onto. found is false for any other
// hash. The caller must hold c.mutex.
func (c *Chain) parentLinkLocked(
	ctx context.Context,
	prevHash []byte,
) (SlotParent, bool, error) {
	if n := len(c.headers); n > 0 &&
		bytes.Equal(c.headers[n-1].point.Hash, prevHash) {
		last := c.headers[n-1]
		return SlotParent{
			Slot:     last.point.Slot,
			EraId:    last.header.Era().Id,
			Boundary: IsByronBoundaryHeader(last.header),
			Known:    true,
		}, true, nil
	}
	if c.tipBlockIndex < initialBlockIndex ||
		!bytes.Equal(c.currentTip.Point.Hash, prevHash) {
		return SlotParent{}, false, nil
	}
	unlockBlockIndexReadLocks := c.lockBlockIndexReadLocks()
	defer unlockBlockIndexReadLocks()
	tipBlock, err := c.blockByIndexLocked(ctx, c.tipBlockIndex)
	if err != nil {
		return SlotParent{}, false, fmt.Errorf("load chain tip block: %w", err)
	}
	return slotParentFromBlock(tipBlock), true, nil
}

// tipSlotParentLocked describes this chain's tip block for SlotFollowsParent.
// txn is the caller's transaction, through which a tip added earlier in it is
// visible. The caller must hold c.mutex and c.manager.mutex.
func (c *Chain) tipSlotParentLocked(
	ctx context.Context,
	txn *database.Txn,
) (SlotParent, error) {
	parent := SlotParent{Slot: c.currentTip.Point.Slot}
	var (
		tipBlock models.Block
		err      error
	)
	if c.persistent || c.tipBlockIndex <= c.lastCommonBlockIndex {
		tipBlock, err = c.manager.blockByIndexLocked(ctx, c.tipBlockIndex, txn)
	} else {
		tipBlock, err = c.manager.blockByPoint(ctx, c.currentTip.Point, txn)
	}
	if err != nil {
		return parent, fmt.Errorf("load chain tip block: %w", err)
	}
	return slotParentFromBlock(tipBlock), nil
}

func slotParentFromBlock(block models.Block) SlotParent {
	parent := SlotParent{
		Slot:     block.Slot,
		Boundary: block.Type == ledger.BlockTypeByronEbb,
	}
	parent.EraId, parent.Known = EraIdForBlockType(block.Type)
	return parent
}
