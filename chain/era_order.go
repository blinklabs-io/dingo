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
	"errors"
	"fmt"
	"math"

	"github.com/blinklabs-io/gouroboros/ledger"
	"github.com/blinklabs-io/gouroboros/ledger/byron"
)

// ErrEraRegression reports a header or block whose era precedes the era of
// the block it extends. The hard-fork combinator only moves a chain's ledger
// state forward (ouroboros-consensus State.extendToSlot via Telescope.extend)
// and rejects a header or block from any era but its parent's ledger view
// (HardForkEnvelopeErrWrongEra, HardForkLedgerErrorWrongEra), so eras along a
// chain never decrease.
var ErrEraRegression = errors.New("era precedes the era of its parent")

// CheckEraOrder returns an error wrapping ErrEraRegression when a header or
// block of era eraId extends a parent of a later era.
func CheckEraOrder(eraId, parentEraId uint8) error {
	if eraId < parentEraId {
		return fmt.Errorf(
			"%w: era %d extends era %d",
			ErrEraRegression,
			eraId,
			parentEraId,
		)
	}
	return nil
}

// EraIdForBlockType returns the era ID of a stored block type, or false for a
// type this build does not know.
func EraIdForBlockType(blockType uint) (uint8, bool) {
	switch blockType {
	case ledger.BlockTypeByronEbb, ledger.BlockTypeByronMain:
		return byron.EraIdByron, true
	}
	eraId, ok := ledger.BlockToBlockHeaderTypeMap[blockType]
	if !ok || eraId > math.MaxUint8 {
		return 0, false
	}
	return uint8(eraId), true
}

// ParentEra returns the era of the header or block prevHash names when it is
// this chain's last queued header or its tip block, the two parents a header
// can be admitted onto. found is false for any other hash, including one
// naming a block this chain holds further back.
func (c *Chain) ParentEra(prevHash []byte) (uint8, bool, error) {
	if c == nil {
		return 0, false, nil
	}
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.parentEraLocked(prevHash)
}

// parentEraLocked implements ParentEra. The caller must hold c.mutex.
func (c *Chain) parentEraLocked(prevHash []byte) (uint8, bool, error) {
	if n := len(c.headers); n > 0 &&
		bytes.Equal(c.headers[n-1].point.Hash, prevHash) {
		return c.headers[n-1].header.Era().Id, true, nil
	}
	if c.tipBlockIndex < initialBlockIndex ||
		!bytes.Equal(c.currentTip.Point.Hash, prevHash) {
		return 0, false, nil
	}
	unlockBlockIndexReadLocks := c.lockBlockIndexReadLocks()
	defer unlockBlockIndexReadLocks()
	tipBlock, err := c.blockByIndexLocked(c.tipBlockIndex)
	if err != nil {
		return 0, false, fmt.Errorf("load chain tip block: %w", err)
	}
	eraId, ok := EraIdForBlockType(tipBlock.Type)
	return eraId, ok, nil
}
