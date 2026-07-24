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
)

// DefaultBlockDeleteBatchSize bounds how many blocks are deleted per blob
// transaction in DeleteBlocksAfter. Chain.Rollback deletes one block per
// transaction, which is fine for the handful-to-low-hundreds of blocks a
// normal in-bounds rollback removes, but is too slow for a disaster-recovery
// truncate that may remove millions of blocks.
const DefaultBlockDeleteBatchSize = 10_000

// DeleteBlocksAfter removes every block whose internal, sequentially
// assigned block ID (models.Block.ID — the basis of the blob store's "bi"
// index, distinct from the chain's Number/height field) falls in
// (afterID, tipID], deleting bp/bi/bh keys and their metadata companion via
// BlobStore.DeleteBlock. Deletes are batched batchSize per blob transaction
// instead of one transaction per block. Returns the number of blocks
// actually found and deleted, which may be far fewer than tipID-afterID:
// IDs are assigned sequentially by BlockCreate for any chain built
// entirely through it, but a chain bootstrapped/drained from a Mithril
// snapshot can leave large gaps of never-imported IDs in that range (see
// database.BlockAtOrAfterIndex's doc comment) — every ID in (afterID,
// tipID] is only an upper bound on how many blocks exist there, not a
// count of how many actually do.
//
// This is a bulk-performance variant of what Chain.Rollback already does
// one block at a time via ChainManager.removeBlockByIndex — it performs no
// chain-manager or fork bookkeeping, so it is only safe to call against a
// database that is not concurrently owned by a live Chain/ChainManager
// (i.e. the offline CLI path, or the live path after quiescing the node).
func DeleteBlocksAfter(
	ctx context.Context,
	db *database.Database,
	afterID uint64,
	tipID uint64,
	batchSize int,
) (blocksDeleted uint64, err error) {
	if batchSize <= 0 {
		batchSize = DefaultBlockDeleteBatchSize
	}
	if tipID <= afterID {
		return 0, nil
	}
	for start := afterID + 1; start <= tipID; {
		if err := ctx.Err(); err != nil {
			return blocksDeleted, err
		}
		end := start + uint64(batchSize) - 1
		if end > tipID {
			end = tipID
		}
		var batchDeleted uint64
		txn := db.BlobTxn(true)
		err := txn.Do(func(txn *database.Txn) error {
			for n := start; n <= end; n++ {
				// Checked every block, not just once per batch: batchSize
				// defaults to 10,000, so a ctx cancellation landing mid-batch
				// would otherwise sit unnoticed until the rest of the
				// current batch finished deleting — a real, potentially
				// long delay for a disaster-recovery truncate an operator
				// just asked to cancel.
				if err := ctx.Err(); err != nil {
					return err
				}
				block, err := db.BlockByIndex(n, txn)
				if err != nil {
					if errors.Is(err, models.ErrBlockNotFound) {
						continue
					}
					return fmt.Errorf(
						"look up block at index %d: %w",
						n,
						err,
					)
				}
				if err := database.BlockDeleteTxn(txn, block); err != nil {
					return fmt.Errorf(
						"delete block at index %d: %w",
						n,
						err,
					)
				}
				batchDeleted++
			}
			return nil
		})
		if err != nil {
			// batchDeleted only reflects this batch's own uncommitted
			// attempt; txn.Do rolled it back on error, so none of it
			// actually took effect and must not be added to the running
			// total.
			return blocksDeleted, err
		}
		blocksDeleted += batchDeleted
		start = end + 1
	}
	return blocksDeleted, nil
}
