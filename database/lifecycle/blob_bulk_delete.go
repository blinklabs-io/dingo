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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// DefaultBlockDeleteBatchSize bounds how many blocks are deleted per blob
// transaction in DeleteBlocksAfter. Chain.Rollback deletes one block per
// transaction, which is fine for the handful-to-low-hundreds of blocks a
// normal in-bounds rollback removes, but is too slow for a disaster-recovery
// truncate that may remove millions of blocks.
const DefaultBlockDeleteBatchSize = 10_000

// blockIndexID decodes a "bi"-prefixed block index key back into the block
// ID it names, mirroring the encoding types.BlockBlobIndexKey produces
// (prefix + 8-byte big-endian ID). ok is false for anything not shaped like
// an index key, which the caller should skip rather than misinterpret.
func blockIndexID(key []byte) (id uint64, ok bool) {
	if len(key) != len(types.BlockBlobIndexKeyPrefix)+8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[len(types.BlockBlobIndexKeyPrefix):]), true
}

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
// Each batch walks the ordered "bi" index keys with a single iterator
// seeked to the batch's start, rather than probing every numeric ID in
// [start, end] individually: cost is therefore proportional to how many
// blocks are actually stored in the batch's range, not to how wide a
// never-imported gap it spans. Probing one ID at a time would turn a
// truncate across a large sparse gap into one remote lookup per absent ID
// for a cloud-backed blob store (GCS/S3) — catastrophic for a deep
// disaster-recovery truncate that spans a big Mithril-imported gap.
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
	blob := db.Blob()
	if blob == nil {
		return 0, types.ErrBlobStoreUnavailable
	}
	indexPrefix := []byte(types.BlockBlobIndexKeyPrefix)
	for start := afterID + 1; start <= tipID; {
		if err := ctx.Err(); err != nil {
			return blocksDeleted, err
		}
		end := min(start+uint64(batchSize)-1, tipID)
		var batchDeleted uint64
		txn := db.BlobTxn(true)
		err := txn.Do(func(txn *database.Txn) error {
			it := blob.NewIterator(
				txn.Blob(),
				types.BlobIteratorOptions{Prefix: indexPrefix},
			)
			if it == nil {
				return errors.New("blob iterator is nil")
			}
			defer it.Close()
			// Cloud-backed iterators (gcs/s3) page the full key listing
			// eagerly inside NewIterator itself: a failed list call is
			// stored on the iterator and surfaces only through Err(),
			// not as a nil return above. Left unchecked, ValidForPrefix
			// below would look identical to "prefix is genuinely empty"
			// and this batch would silently delete nothing while still
			// being reported as a clean success.
			if err := it.Err(); err != nil {
				return fmt.Errorf("blob iterator: %w", err)
			}
			// Seek once to the first index entry at or after start, then
			// walk forward: present blocks are visited in order and any
			// gap in between costs nothing, unlike looping every numeric
			// ID in [start, end].
			it.Seek(types.BlockBlobIndexKey(start))
			for it.ValidForPrefix(indexPrefix) {
				item := it.Item()
				if item == nil {
					it.Next()
					continue
				}
				id, ok := blockIndexID(item.Key())
				if !ok {
					it.Next()
					continue
				}
				if id > end {
					// The next present block belongs to a later batch;
					// nothing left in this one. Checked before ctx.Err()
					// below: this lookahead item is never actually deleted
					// by this batch, so a cancellation observed here would
					// wrongly fail (and roll back) a batch that in fact
					// completed cleanly -- undoing every delete already
					// issued within this same transaction for nothing.
					break
				}
				// Checked every block actually in this batch, not just
				// once per batch: batchSize defaults to 10,000, so a ctx
				// cancellation landing mid-batch would otherwise sit
				// unnoticed until the rest of the current batch finished
				// deleting — a real, potentially long delay for a
				// disaster-recovery truncate an operator just asked to
				// cancel.
				if err := ctx.Err(); err != nil {
					return err
				}
				blockKey, err := item.ValueCopy(nil)
				if err != nil {
					return fmt.Errorf(
						"read index entry at %d: %w",
						id,
						err,
					)
				}
				slot, hash, err := types.ParseBlockBlobKey(blockKey)
				if err != nil {
					return fmt.Errorf(
						"parse index entry at %d: %w",
						id,
						err,
					)
				}
				if err := database.BlockDeleteTxn(txn, models.Block{
					ID:   id,
					Slot: slot,
					Hash: hash,
				}); err != nil {
					return fmt.Errorf(
						"delete block at index %d: %w",
						id,
						err,
					)
				}
				batchDeleted++
				it.Next()
			}
			// A cloud iterator can also fail mid-walk (a paginator error
			// partway through listing), which ValidForPrefix again
			// reports identically to "no more keys" -- so the loop above
			// exiting cleanly is not proof every block in range was seen.
			if err := it.Err(); err != nil {
				return fmt.Errorf("blob iterator: %w", err)
			}
			return nil
		})
		if err != nil {
			// batchDeleted blocks were actually issued to the blob store
			// before this error, and must be counted as deleted even
			// though the batch as a whole is reported as failed: a local
			// badger store rolls its transaction back atomically on
			// error, but a cloud store (GCS/S3) does not -- its Commit
			// and Rollback are no-ops, so every Delete already issued is
			// real and permanent regardless of what happens afterward
			// (see BlobStoreGCS/BlobStoreS3's gcsTxn/s3Txn). Undercounting
			// here would tell the caller fewer blocks are gone than
			// actually are, which is most dangerous for exactly the
			// backend where it happens. This is still safe to resume from:
			// finishPendingTruncate always retries the identical
			// (afterID, tipID] range recorded in the pending-truncate
			// marker, and redoing it is idempotent regardless of how far
			// a previous attempt got -- an already-removed "bi" entry
			// simply will not be found by the next attempt's iterator, and
			// a still-present one whose referenced bp/bh/metadata objects
			// are already gone deletes harmlessly again (cloud DeleteBlock
			// tolerates a missing object; badger's Delete tolerates a
			// missing key).
			return blocksDeleted + batchDeleted, err
		}
		blocksDeleted += batchDeleted
		start = end + 1
	}
	return blocksDeleted, nil
}
