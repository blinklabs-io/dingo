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
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// ForEachBlockInRange iterates blocks in the slot range [startSlot, endSlot)
// from the blob store and calls fn for each block. Blocks are visited in
// ascending slot order. Iteration stops early if fn returns a non-nil error.
func ForEachBlockInRange(
	txn *Txn,
	startSlot, endSlot uint64,
	fn func(block models.Block) error,
) error {
	if fn == nil {
		return errors.New("ForEachBlockInRange: callback fn must not be nil")
	}
	if txn == nil {
		return types.ErrNilTxn
	}
	if startSlot >= endSlot {
		return nil
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return types.ErrNilTxn
	}
	blob := txn.DB().Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	iterOpts := types.BlobIteratorOptions{
		Prefix: []byte(types.BlockBlobKeyPrefix),
	}
	it := blob.NewIterator(blobTxn, iterOpts)
	if it == nil {
		return errors.New("blob iterator is nil")
	}
	defer it.Close()
	startKey := slices.Concat(
		[]byte(types.BlockBlobKeyPrefix),
		types.BlockBlobKeyUint64ToBytes(startSlot),
	)
	for it.Seek(startKey); it.ValidForPrefix(
		[]byte(types.BlockBlobKeyPrefix),
	); it.Next() {
		item := it.Item()
		if item == nil {
			continue
		}
		k := item.Key()
		if k == nil {
			continue
		}
		// Skip the metadata key
		if strings.HasSuffix(
			string(k),
			types.BlockBlobMetadataKeySuffix,
		) {
			continue
		}
		// Check if we've passed the end slot
		point, err := BlockBlobKeyToPoint(k)
		if err != nil {
			return fmt.Errorf(
				"BlockBlobKeyToPoint failed for key=%q: %w",
				k, err,
			)
		}
		if point.Slot >= endSlot {
			break
		}
		block, err := blockByKey(txn, k)
		if err != nil {
			return fmt.Errorf(
				"blockByKey failed for key=%q: %w", k, err,
			)
		}
		if err := fn(block); err != nil {
			return fmt.Errorf(
				"processing block slot=%d key=%q: %w",
				point.Slot, k, err,
			)
		}
	}
	return it.Err()
}

// ForEachBlockInRangeDB is a convenience wrapper that creates a read-only
// transaction and calls ForEachBlockInRange.
func ForEachBlockInRangeDB(
	db *Database,
	startSlot, endSlot uint64,
	fn func(block models.Block) error,
) error {
	txn := db.Transaction(false)
	return txn.Do(func(txn *Txn) error {
		return ForEachBlockInRange(txn, startSlot, endSlot, fn)
	})
}
