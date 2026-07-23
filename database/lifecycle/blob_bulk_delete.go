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
// instead of one transaction per block.
//
// IDs are assigned sequentially by BlockCreate (one higher than the
// previous highest), so the range (afterID, tipID] is contiguous for any
// chain built entirely through BlockCreate.
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
) error {
	if batchSize <= 0 {
		batchSize = DefaultBlockDeleteBatchSize
	}
	if tipID <= afterID {
		return nil
	}
	for start := afterID + 1; start <= tipID; {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start + uint64(batchSize) - 1
		if end > tipID {
			end = tipID
		}
		txn := db.BlobTxn(true)
		err := txn.Do(func(txn *database.Txn) error {
			for n := start; n <= end; n++ {
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
			}
			return nil
		})
		if err != nil {
			return err
		}
		start = end + 1
	}
	return nil
}
