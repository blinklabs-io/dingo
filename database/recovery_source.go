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

package database

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/recovery"
	"github.com/blinklabs-io/dingo/database/types"
)

// errStopSample ends a bounded iteration once the sample limit is reached. It
// never escapes the function that raises it.
var errStopSample = errors.New("sample limit reached")

// RecoveryStateSource returns the read-only view of stored state that the
// crash-recovery consistency checks run against.
//
// It reports only what the two stores hold. The chain manager's tip is not
// visible from here, so the node wraps this value to add it; see
// recovery.ChainTipSource.
func (d *Database) RecoveryStateSource() recovery.StateSource {
	return recoveryStateSource{db: d}
}

// recoveryStateSource adapts a Database to recovery.StateSource.
type recoveryStateSource struct {
	db *Database
}

// MetadataTip returns the tip the metadata store records.
func (s recoveryStateSource) MetadataTip() (recovery.Point, uint64, error) {
	tip, err := s.db.GetTip(nil)
	if err != nil {
		return recovery.Point{}, 0, err
	}
	return recovery.Point{
		Slot: tip.Point.Slot,
		Hash: tip.Point.Hash,
	}, tip.BlockNumber, nil
}

// BlobTip returns the newest block the blob store holds.
func (s recoveryStateSource) BlobTip() (recovery.Point, error) {
	var point recovery.Point
	txn := s.db.Transaction(false)
	err := txn.Do(func(txn *Txn) error {
		blocks, err := BlocksRecentTxn(txn, 1)
		if err != nil {
			return err
		}
		if len(blocks) == 0 {
			return nil
		}
		point = recovery.Point{
			Slot: blocks[0].Slot,
			Hash: blocks[0].Hash,
		}
		return nil
	})
	if err != nil {
		return recovery.Point{}, err
	}
	return point, nil
}

// CommitTimestamps returns the cross-store commit fence each store holds.
func (s recoveryStateSource) CommitTimestamps() (int64, int64, error) {
	metadataTS, err := s.db.Metadata().GetCommitTimestamp()
	if err != nil {
		return 0, 0, fmt.Errorf("metadata commit timestamp: %w", err)
	}
	blobTS, err := s.db.Blob().GetCommitTimestamp()
	if err != nil {
		return 0, 0, fmt.Errorf("blob commit timestamp: %w", err)
	}
	return metadataTS, blobTS, nil
}

// RecentBlocks returns up to limit blocks ending at the blob tip, newest first.
//
// The block CBOR each lookup returns is dropped here rather than retained: the
// continuity check only needs the hash linkage, and holding thousands of block
// bodies would cost far more memory than the check is worth.
func (s recoveryStateSource) RecentBlocks(
	limit int,
) ([]recovery.BlockRef, error) {
	if limit <= 0 {
		return nil, nil
	}
	var refs []recovery.BlockRef
	txn := s.db.Transaction(false)
	err := txn.Do(func(txn *Txn) error {
		blocks, err := BlocksRecentTxn(txn, limit)
		if err != nil {
			return err
		}
		refs = make([]recovery.BlockRef, 0, len(blocks))
		for _, block := range blocks {
			refs = append(refs, recovery.BlockRef{
				Hash:     block.Hash,
				PrevHash: block.PrevHash,
				Slot:     block.Slot,
				Number:   block.Number,
				ID:       block.ID,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return refs, nil
}

// OrphanBlobs returns up to limit blocks the blob store holds above afterSlot,
// oldest first.
//
// Slot and hash come straight out of the key, so the scan never reads a block
// body. That matters when the gap is large: after a snapshot bootstrap the blob
// store can legitimately hold a great many blocks above the applied tip, and
// this has to stay cheap enough to run at every start.
func (s recoveryStateSource) OrphanBlobs(
	afterSlot uint64,
	limit int,
) ([]recovery.BlockRef, error) {
	if limit <= 0 {
		return nil, nil
	}
	blobStore := s.db.Blob()
	if blobStore == nil {
		return nil, types.ErrBlobStoreUnavailable
	}
	readTxn := blobStore.NewTransaction(false)
	defer readTxn.Rollback() //nolint:errcheck
	if afterSlot == ^uint64(0) {
		return nil, nil
	}
	seekKey := make([]byte, 0, len(types.BlockBlobKeyPrefix)+8)
	seekKey = append(seekKey, types.BlockBlobKeyPrefix...)
	seekKey = binary.BigEndian.AppendUint64(seekKey, afterSlot+1)
	it := blobStore.NewIterator(readTxn, types.BlobIteratorOptions{
		Prefix: []byte(types.BlockBlobKeyPrefix),
		Start:  seekKey,
		Limit:  limit*4 + 16,
	})
	if it == nil {
		return nil, errors.New("blob iterator is nil")
	}
	defer it.Close()
	// Seek past the boundary slot. Keys are prefix + big-endian slot, so
	// seeking to slot+1 lands on the first block above it.
	var refs []recovery.BlockRef
	for it.Seek(seekKey); it.ValidForPrefix(
		[]byte(types.BlockBlobKeyPrefix),
	); it.Next() {
		item := it.Item()
		if item == nil {
			continue
		}
		key := item.Key()
		if key == nil {
			continue
		}
		// Per-block metadata is stored under a suffixed variant of the
		// same key and is not itself a block.
		if strings.HasSuffix(
			string(key),
			types.BlockBlobMetadataKeySuffix,
		) {
			continue
		}
		slot, hash, err := types.ParseBlockBlobKey(key)
		if err != nil {
			return nil, fmt.Errorf("parse block blob key %q: %w", key, err)
		}
		if slot <= afterSlot {
			continue
		}
		refs = append(refs, recovery.BlockRef{Slot: slot, Hash: hash})
		if len(refs) >= limit {
			break
		}
	}
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("orphan blob scan: %w", err)
	}
	return refs, nil
}

// CheckUtxos resolves up to limit live UTxOs against their stored CBOR.
//
// Live UTxOs are stored as offset references into block CBOR rather than as
// inline values, so a row whose block went missing resolves to nothing while
// still looking present in the metadata store. Nothing notices until a
// transaction tries to spend it, which is why a bounded sample runs at startup.
func (s recoveryStateSource) CheckUtxos(
	limit int,
) (recovery.UtxoIntegrityResult, error) {
	var result recovery.UtxoIntegrityResult
	if limit <= 0 {
		return result, nil
	}
	txn := s.db.Transaction(false)
	err := txn.Do(func(txn *Txn) error {
		return s.db.metadata.IterateLiveUtxos(
			txn.Metadata(),
			func(utxo *models.Utxo) error {
				result.Checked++
				if err := loadCbor(utxo, txn); err != nil {
					result.Unresolvable = append(
						result.Unresolvable,
						fmt.Sprintf(
							"%x#%d",
							utxo.TxId,
							utxo.OutputIdx,
						),
					)
				}
				if result.Checked >= limit {
					return errStopSample
				}
				return nil
			},
		)
	})
	if err != nil && !errors.Is(err, errStopSample) {
		return recovery.UtxoIntegrityResult{}, err
	}
	return result, nil
}

// trimBatchSize bounds one removal pass. Badger rejects a transaction that
// grows past its size limit, and a repair has no bound on how many blocks it
// may be asked to remove, so the work is committed in batches and the scan
// repeats until nothing is left above the boundary.
const trimBatchSize = 500

// TrimBlobAbove removes every block the blob store holds above slot and returns
// how many it removed.
//
// This is the repair for the residue an interrupted cross-store commit leaves:
// blob writes that landed before the process died while the metadata commit
// that would have made them part of the chain never did. Callers are
// responsible for choosing a boundary that no live chain data sits above.
func (d *Database) TrimBlobAbove(slot uint64) (int, error) {
	blobStore := d.Blob()
	if blobStore == nil {
		return 0, types.ErrBlobStoreUnavailable
	}
	source := recoveryStateSource{db: d}
	total := 0
	for {
		orphans, err := source.OrphanBlobs(slot, trimBatchSize)
		if err != nil {
			return total, err
		}
		if len(orphans) == 0 {
			break
		}
		deleted, err := d.trimBlobBatch(blobStore, orphans)
		total += deleted
		if err != nil {
			return total, err
		}
		if deleted == 0 {
			// The scan still sees these blocks and the batch removed
			// none of them. Repeating would spin forever, so stop and
			// say so rather than looping.
			return total, fmt.Errorf(
				"orphan removal made no progress at slot %d",
				orphans[0].Slot,
			)
		}
	}
	if total > 0 {
		if err := blobStore.Sync(); err != nil {
			return total, fmt.Errorf("sync after orphan removal: %w", err)
		}
	}
	return total, nil
}

// trimBlobBatch removes one batch of blocks in a single transaction.
func (d *Database) trimBlobBatch(
	blobStore blob.BlobStore,
	orphans []recovery.BlockRef,
) (int, error) {
	// DeleteBlock needs the block's index, which only the stored metadata
	// carries, so this pass does read the per-block metadata the key scan
	// skipped. It runs only when there is something to delete.
	readTxn := blobStore.NewTransaction(false)
	ids := make([]uint64, len(orphans))
	for i, orphan := range orphans {
		_, metadata, err := blobStore.GetBlock(
			readTxn,
			orphan.Slot,
			orphan.Hash,
		)
		if err != nil {
			var expired *types.HistoryExpiredError
			if errors.As(err, &expired) && metadata.ID != 0 {
				// Tombstoned blocks retain their metadata/index so they
				// can be served by an archive wrapper. Recovery still
				// removes the complete orphan, using the retained ID.
				ids[i] = metadata.ID
				continue
			}
			_ = readTxn.Rollback()
			return 0, fmt.Errorf(
				"read metadata for block at slot %d: %w",
				orphan.Slot,
				err,
			)
		}
		ids[i] = metadata.ID
	}
	if err := readTxn.Rollback(); err != nil {
		d.logger.Debug(
			"failed to release orphan metadata read transaction",
			"error", err,
		)
	}
	writeTxn := blobStore.NewTransaction(true)
	deleted := 0
	for i, orphan := range orphans {
		if err := blobStore.DeleteBlock(
			writeTxn,
			orphan.Slot,
			orphan.Hash,
			ids[i],
		); err != nil {
			_ = writeTxn.Rollback()
			return 0, fmt.Errorf(
				"delete block at slot %d: %w",
				orphan.Slot,
				err,
			)
		}
		deleted++
	}
	if err := writeTxn.Commit(); err != nil {
		return 0, fmt.Errorf("commit orphan block removal: %w", err)
	}
	return deleted, nil
}

// ResetCommitFence brings both stores back onto a common commit timestamp
// after a repair has made their contents consistent again.
//
// It does not take the value to write. A combined commit stamps both stores
// with its own timestamp as part of committing, so an empty combined commit is
// all this needs to be, and any value passed in would be overwritten by that
// stamp anyway. What matters to recovery is that the two agree, not what they
// agree on.
func (d *Database) ResetCommitFence() error {
	txn := d.Transaction(true)
	return txn.Do(func(*Txn) error { return nil })
}
