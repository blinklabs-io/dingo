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
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/blinklabs-io/dingo/database/types"
)

const (
	// blobIteratorBatchSize controls how many block keys are fetched per
	// batch from the blob iterator. This avoids loading the entire chain
	// into memory while keeping I/O efficient.
	blobIteratorBatchSize = 1000
)

// blobBlockEntry holds a block key discovered during batch scanning.
type blobBlockEntry struct {
	key  []byte
	slot uint64
	hash []byte
}

// BlobBlockIterator iterates blocks from the blob store in slot order.
// The blob store keys are formatted as "bp" + big-endian(slot) + hash,
// so forward iteration naturally yields blocks in ascending slot order.
//
// The iterator fetches block keys in batches to avoid loading the entire
// chain index into memory, and retrieves CBOR data on demand for each
// call to NextRaw.
type BlobBlockIterator struct {
	db         *Database
	startSlot  uint64
	endSlot    uint64
	hasEndSlot bool

	// internal state
	mu          sync.Mutex
	batch       []blobBlockEntry
	batchIdx    int
	currentSlot uint64
	exhausted   bool
	closed      bool

	// resumeKey is the blob key to seek past when fetching the next batch.
	// nil means start from the beginning (or from startSlot).
	resumeKey []byte
}

// BlocksFromSlot returns an iterator that yields blocks starting from
// startSlot, continuing through all subsequent blocks in the blob store.
func (d *Database) BlocksFromSlot(startSlot uint64) *BlobBlockIterator {
	return &BlobBlockIterator{
		db:        d,
		startSlot: startSlot,
	}
}

// BlocksInRange returns an iterator for a specific slot range [start, end].
// Both endpoints are inclusive.
func (d *Database) BlocksInRange(
	startSlot, endSlot uint64,
) *BlobBlockIterator {
	return &BlobBlockIterator{
		db:         d,
		startSlot:  startSlot,
		endSlot:    endSlot,
		hasEndSlot: true,
	}
}

// BlobBlockResult holds the data returned by BlobBlockIterator.NextRaw.
type BlobBlockResult struct {
	Slot      uint64
	Hash      []byte
	Cbor      []byte
	BlockType uint
	Height    uint64
	PrevHash  []byte
}

// NextRaw returns the next block as raw CBOR bytes along with its
// metadata. When iteration is complete, it returns (nil, nil).
// Blocks whose CBOR cannot be fetched from the blob store are
// skipped with a warning log.
func (it *BlobBlockIterator) NextRaw() (*BlobBlockResult, error) {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return nil, nil
	}

	for {
		// Refill batch if needed
		if it.batchIdx >= len(it.batch) {
			if it.exhausted {
				return nil, nil
			}
			if err := it.fetchBatch(); err != nil {
				return nil, err
			}
			if len(it.batch) == 0 {
				it.exhausted = true
				return nil, nil
			}
		}

		entry := it.batch[it.batchIdx]
		it.batchIdx++
		it.currentSlot = entry.slot

		// Fetch CBOR and metadata from blob store
		cbor, meta, fetchErr := it.fetchBlock(
			entry.slot,
			entry.hash,
		)
		if fetchErr != nil {
			if errors.Is(fetchErr, types.ErrBlobKeyNotFound) {
				it.db.logger.Warn(
					"blob iterator: skipping block with missing CBOR",
					"slot", entry.slot,
					"error", fetchErr,
				)
				continue
			}
			return nil, fmt.Errorf(
				"fetching block at slot %d: %w",
				entry.slot, fetchErr,
			)
		}

		return &BlobBlockResult{
			Slot:      entry.slot,
			Hash:      entry.hash,
			Cbor:      cbor,
			BlockType: meta.Type,
			Height:    meta.Height,
			PrevHash:  meta.PrevHash,
		}, nil
	}
}

// Progress returns the current slot being iterated and the end slot.
// If no end slot was specified (iterate to tip), end returns 0.
func (it *BlobBlockIterator) Progress() (current, end uint64) {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.currentSlot, it.endSlot
}

// Close releases any resources held by the iterator. It is safe to call
// Close multiple times.
func (it *BlobBlockIterator) Close() {
	it.mu.Lock()
	defer it.mu.Unlock()
	it.closed = true
	it.batch = nil
	it.resumeKey = nil
}

// fetchBatch retrieves the next batch of block keys from the blob store.
// Must be called with it.mu held.
func (it *BlobBlockIterator) fetchBatch() error {
	blob := it.db.Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}

	txn := blob.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck

	iterOpts := types.BlobIteratorOptions{
		Prefix: []byte(types.BlockBlobKeyPrefix),
	}
	blobIter := blob.NewIterator(txn, iterOpts)
	if blobIter == nil {
		return errors.New("blob iterator is nil")
	}
	defer blobIter.Close()

	// Determine seek position
	var seekKey []byte
	if it.resumeKey != nil {
		// Seek past the last key we processed
		seekKey = it.resumeKey
	} else {
		// Start from the configured start slot
		seekKey = buildSlotSeekKey(it.startSlot)
	}

	// Build end prefix for range limiting.
	// When endSlot is max uint64, all slots are in range so we
	// skip the prefix check to avoid overflow on endSlot+1.
	var endPrefix []byte
	if it.hasEndSlot && it.endSlot < ^uint64(0) {
		endPrefix = buildSlotSeekKey(it.endSlot + 1)
	}

	batch := make([]blobBlockEntry, 0, blobIteratorBatchSize)
	prefix := []byte(types.BlockBlobKeyPrefix)

	resuming := it.resumeKey != nil

	for blobIter.Seek(seekKey); blobIter.ValidForPrefix(prefix); blobIter.Next() {
		item := blobIter.Item()
		if item == nil {
			continue
		}
		key := item.Key()
		if key == nil {
			continue
		}

		// Skip metadata keys
		if bytes.HasSuffix(key, []byte(types.BlockBlobMetadataKeySuffix)) {
			continue
		}

		// When resuming, skip the exact key we left off at.
		// If resumeKey was deleted (compaction), Seek lands on the
		// next key which should be included — so we only continue
		// when there is an exact match.
		if resuming {
			resuming = false
			if bytes.Equal(key, it.resumeKey) {
				continue
			}
		}

		// Check end range
		if endPrefix != nil && bytes.Compare(key, endPrefix) >= 0 {
			break
		}

		// Parse slot and hash from key
		point, parseErr := BlockBlobKeyToPoint(key)
		if parseErr != nil {
			it.db.logger.Warn(
				"blob iterator: skipping unparseable key",
				"error", parseErr,
			)
			continue
		}

		entry := blobBlockEntry{
			key:  make([]byte, len(key)),
			slot: point.Slot,
			hash: point.Hash,
		}
		copy(entry.key, key)

		batch = append(batch, entry)
		if len(batch) >= blobIteratorBatchSize {
			break
		}
	}

	if err := blobIter.Err(); err != nil {
		return fmt.Errorf("scanning blob keys: %w", err)
	}

	it.batch = batch
	it.batchIdx = 0

	if len(batch) > 0 {
		it.resumeKey = batch[len(batch)-1].key
	}

	// If we got fewer than a full batch, we've exhausted the range
	if len(batch) < blobIteratorBatchSize {
		it.exhausted = true
	}

	return nil
}

// fetchBlock retrieves the raw CBOR bytes and metadata for a block.
// Must be called with it.mu held.
func (it *BlobBlockIterator) fetchBlock(
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	blob := it.db.Blob()
	if blob == nil {
		return nil, types.BlockMetadata{}, types.ErrBlobStoreUnavailable
	}

	txn := blob.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck

	return blob.GetBlock(txn, slot, hash)
}

// buildSlotSeekKey constructs a blob key prefix for seeking to a specific slot.
func buildSlotSeekKey(slot uint64) []byte {
	key := []byte(types.BlockBlobKeyPrefix)
	slotBytes := types.BlockBlobKeyUint64ToBytes(slot)
	key = append(key, slotBytes...)
	return key
}
