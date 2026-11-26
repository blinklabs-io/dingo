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

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// RewindMetadata reverts the metadata store of a database that keeps its
// ledger state there to the block depth blocks behind its tip, as Truncate
// does, and keeps every block: a ledger started on the result applies the
// blocks after the target again. The target is raised to the Mithril trust
// boundary and to the consumed-output prune floor, below which the metadata
// store cannot be reverted, and past a block that shares its slot with its
// successor.
//
// record runs in the transaction that reverts the store, so a caller's mark
// that the rewind is done commits with it or not at all; it also runs when
// there is nothing to revert, in which case rewound is false.
func RewindMetadata(
	ctx context.Context,
	db *database.Database,
	depth uint64,
	delegatorInactivityEnabled bool,
	delegatorInactivity uint64,
	record func(*database.Txn) error,
) (target models.Block, rewound bool, err error) {
	target, mithrilFloor, rewound, err := rewindTarget(ctx, db, depth)
	if err != nil {
		return models.Block{}, false, fmt.Errorf("rewind: %w", err)
	}
	txn := db.Transaction(ctx, true)
	err = txn.Do(func(txn *database.Txn) error {
		if rewound {
			if err := revertStoredLedger(
				ctx, db, txn,
				ocommon.Point{Slot: target.Slot, Hash: target.Hash},
				mithrilFloor,
				delegatorInactivityEnabled, delegatorInactivity,
			); err != nil {
				return err
			}
		}
		return record(txn)
	})
	if err != nil {
		return models.Block{}, false, fmt.Errorf("rewind: %w", err)
	}
	return target, rewound, nil
}

// rewindTarget returns the block RewindMetadata reverts to; ok is false when
// no block below the tip can be reverted to.
func rewindTarget(
	ctx context.Context,
	db *database.Database,
	depth uint64,
) (target models.Block, mithrilFloor uint64, ok bool, err error) {
	tip, err := db.GetTip(nil)
	if err != nil {
		return models.Block{}, 0, false, fmt.Errorf("get tip: %w", err)
	}
	if tip.Point.Slot == 0 && len(tip.Point.Hash) == 0 {
		return models.Block{}, 0, false, nil
	}
	tipBlock, err := database.BlockByPoint(ctx, db, tip.Point)
	if err != nil {
		return models.Block{}, 0, false, fmt.Errorf("get tip block: %w", err)
	}
	mithrilFloor, err = db.ImportedLedgerStateBoundarySlotStrict(nil)
	if err != nil {
		return models.Block{}, 0, false, fmt.Errorf(
			"read the Mithril trust boundary: %w", err,
		)
	}
	floor := mithrilFloor
	raw, err := db.GetSyncState(database.ConsumedUtxoPruneFloorSyncKey, nil)
	if err != nil {
		return models.Block{}, 0, false, fmt.Errorf(
			"read consumed UTxO prune floor: %w", err,
		)
	}
	if raw != "" {
		pruned, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return models.Block{}, 0, false, fmt.Errorf(
				"parse consumed UTxO prune floor marker %q: %w", raw, err,
			)
		}
		floor = max(floor, pruned)
	}
	// The first block is the lowest target: the store has no rows that
	// stand for the state before it.
	number := uint64(1)
	if tipBlock.Number > depth+1 {
		number = tipBlock.Number - depth
	}
	if number >= tipBlock.Number {
		return models.Block{}, mithrilFloor, false, nil
	}
	target, err = ResolveTargetByNumber(ctx, db, number)
	if err != nil {
		return models.Block{}, 0, false, err
	}
	if target.Slot < floor {
		// The last block at or below the floor, or the first above it.
		target, err = database.BlockBeforeSlot(ctx, db, floor+1)
		if err != nil && !errors.Is(err, models.ErrBlockNotFound) {
			return models.Block{}, 0, false, fmt.Errorf(
				"find the block at slot %d: %w", floor, err,
			)
		}
		if err != nil || target.Slot < floor {
			target, err = db.BlockAtOrAfterIndex(target.ID+1, nil)
			if errors.Is(err, models.ErrBlockNotFound) {
				return models.Block{}, mithrilFloor, false, nil
			}
			if err != nil {
				return models.Block{}, 0, false, fmt.Errorf(
					"find the block after slot %d: %w", floor, err,
				)
			}
		}
	}
	// The sweep deletes by slot, so it cannot keep one of two blocks of a
	// slot: the later one is the target.
	for target.ID < tipBlock.ID {
		next, err := db.BlockAtOrAfterIndex(target.ID+1, nil)
		if err != nil {
			return models.Block{}, 0, false, fmt.Errorf(
				"find the block after the target: %w", err,
			)
		}
		if next.Slot != target.Slot {
			break
		}
		target = next
	}
	if target.ID >= tipBlock.ID {
		return models.Block{}, mithrilFloor, false, nil
	}
	return target, mithrilFloor, true, nil
}
