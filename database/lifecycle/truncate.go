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

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

// ErrTruncateNotStarted marks a Truncate failure that occurred entirely
// during read-only validation — before DeleteBlocksAfter made any on-disk
// change — as opposed to a failure during or after it, where a batched
// bulk delete spanning more than one batch may have already partially
// committed. Callers deciding whether it's safe to resume normal service
// after a failed live truncate (rather than treat the data directory as
// possibly inconsistent) should check errors.Is against this.
var ErrTruncateNotStarted = errors.New("truncate: not started, no data was modified")

// ResolveTargetByHash resolves a truncate target identified by block hash.
func ResolveTargetByHash(db *database.Database, hash []byte) (models.Block, error) {
	block, err := database.BlockByHash(db, hash)
	if err != nil {
		return models.Block{}, fmt.Errorf(
			"resolve target by hash: %w", err,
		)
	}
	return block, nil
}

// ResolveTargetBySlot resolves a truncate target as the highest-slot
// block at or before the given slot, against whatever chain the local
// database currently has. Slots without a block of their own (the common
// case — cardano-node's ~20s average slot time means most slots are
// empty) resolve to their nearest ancestor, since an operator invoking a
// disaster-recovery truncate is very unlikely to know a block-populated
// slot exactly and should not have to.
func ResolveTargetBySlot(db *database.Database, slot uint64) (models.Block, error) {
	tip, err := db.GetTip(nil)
	if err != nil {
		return models.Block{}, fmt.Errorf(
			"resolve target by slot: get tip: %w", err,
		)
	}
	tipBlock, err := database.BlockByPoint(db, tip.Point)
	if err != nil {
		return models.Block{}, fmt.Errorf(
			"resolve target by slot: get tip block: %w", err,
		)
	}
	if slot >= tipBlock.Slot {
		return tipBlock, nil
	}
	// Binary search the contiguous internal-ID space (chronological order)
	// for the highest ID whose Slot does not exceed the target slot.
	lo, hi := uint64(1), tipBlock.ID
	var best *models.Block
	for lo <= hi {
		mid := lo + (hi-lo)/2
		block, err := db.BlockByIndex(mid, nil)
		if err != nil {
			return models.Block{}, fmt.Errorf(
				"resolve target by slot: look up block at index %d: %w",
				mid,
				err,
			)
		}
		if block.Slot <= slot {
			best = &block
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if best == nil {
		return models.Block{}, fmt.Errorf(
			"resolve target by slot: no block found at or before slot %d",
			slot,
		)
	}
	return *best, nil
}

// ResolveTargetByNumber resolves a truncate target identified by chain
// block number (height). Block numbers are not directly indexed in the
// blob store (only slot, hash, and internal sequential ID are), so this
// binary-searches the contiguous internal-ID space bounded by the current
// tip, comparing each candidate's Number field, mirroring the technique
// Chain.BlockBeforeSlot uses for slot-ordered lookups.
func ResolveTargetByNumber(
	db *database.Database,
	number uint64,
) (models.Block, error) {
	tip, err := db.GetTip(nil)
	if err != nil {
		return models.Block{}, fmt.Errorf(
			"resolve target by number: get tip: %w", err,
		)
	}
	tipBlock, err := database.BlockByPoint(db, tip.Point)
	if err != nil {
		return models.Block{}, fmt.Errorf(
			"resolve target by number: get tip block: %w", err,
		)
	}
	if number > tipBlock.Number {
		return models.Block{}, fmt.Errorf(
			"resolve target by number: block number %d is ahead of tip (%d)",
			number,
			tipBlock.Number,
		)
	}
	lo, hi := uint64(1), tipBlock.ID
	for lo <= hi {
		mid := lo + (hi-lo)/2
		block, err := db.BlockByIndex(mid, nil)
		if err != nil {
			return models.Block{}, fmt.Errorf(
				"resolve target by number: look up block at index %d: %w",
				mid,
				err,
			)
		}
		switch {
		case block.Number == number:
			return block, nil
		case block.Number < number:
			lo = mid + 1
		default:
			hi = mid - 1
		}
	}
	return models.Block{}, fmt.Errorf(
		"resolve target by number: no block found with number %d",
		number,
	)
}

// Truncate reverts the database to target: target becomes the new chain
// tip, every block with a strictly greater internal ID is removed from
// the blob store, and every metadata row (and blob-referenced UTxO/tx
// CBOR) added after target's slot is removed or restored to its
// pre-target state via database.TruncateAfterSlot.
//
// Unlike Chain.Rollback, this does not reject a target beyond the
// configured security parameter — that guard protects automatic rollback
// during normal sync; an operator explicitly invoking Truncate (e.g. for
// CIP-0135 disaster recovery from a long network partition) is the
// informed-consent replacement for it. It still refuses to truncate to a
// point before the Mithril trust boundary, if one is recorded: that
// boundary reflects what UTxO history is actually available locally, not
// a policy choice, and going below it would leave the database unable to
// validate the first block past the (now missing) boundary.
//
// This is an offline operation in the sense that it performs no chain-
// manager or in-memory ledger-state bookkeeping — it is safe to call
// against a database not concurrently owned by a live Chain/LedgerState
// (the offline CLI path, or the live path after quiescing the node).
//
// blocksRemoved is the number of blocks deleted: tipBlock.ID - target.ID,
// valid because block IDs are assigned contiguously (see
// ResolveTargetByNumber's doc comment) — every ID in (target.ID, tipBlock.ID]
// is a block DeleteBlocksAfter actually removes, so no separate count is
// needed.
func Truncate(
	ctx context.Context,
	db *database.Database,
	target models.Block,
	batchSize int,
) (blocksRemoved uint64, err error) {
	tip, err := db.GetTip(nil)
	if err != nil {
		return 0, fmt.Errorf("%w: get tip: %w", ErrTruncateNotStarted, err)
	}
	tipBlock, err := database.BlockByPoint(db, tip.Point)
	if err != nil {
		return 0, fmt.Errorf("%w: get tip block: %w", ErrTruncateNotStarted, err)
	}
	if target.ID == tipBlock.ID {
		return 0, nil
	}
	if target.ID > tipBlock.ID {
		return 0, fmt.Errorf(
			"%w: target block (id=%d, slot=%d) is ahead of current tip (id=%d, slot=%d)",
			ErrTruncateNotStarted,
			target.ID,
			target.Slot,
			tipBlock.ID,
			tip.Point.Slot,
		)
	}

	mithrilFloor := db.MithrilTrustBoundarySlot(nil)
	if mithrilFloor > 0 && target.Slot < mithrilFloor {
		return 0, fmt.Errorf(
			"%w: target slot %d is before the Mithril trust boundary (%d); "+
				"the local database does not have complete history before that point",
			ErrTruncateNotStarted,
			target.Slot,
			mithrilFloor,
		)
	}

	// Everything above this point is read-only validation; DeleteBlocksAfter
	// is where on-disk mutation actually begins (and, for a truncate
	// spanning more than one delete batch, may partially commit before an
	// error or context cancellation is noticed) — so an error from here on
	// is deliberately NOT wrapped in ErrTruncateNotStarted, unlike the
	// validation failures above.
	if err := DeleteBlocksAfter(ctx, db, target.ID, tipBlock.ID, batchSize); err != nil {
		return 0, fmt.Errorf("truncate: delete blocks after target: %w", err)
	}

	point := ocommon.Point{Slot: target.Slot, Hash: target.Hash}
	if _, _, err := db.TruncateAfterSlot(point, mithrilFloor, nil); err != nil {
		return 0, fmt.Errorf("truncate: truncate metadata: %w", err)
	}
	return tipBlock.ID - target.ID, nil
}
