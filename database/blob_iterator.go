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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
)

const (
	// blobIteratorBatchSize controls how many block keys are fetched per
	// batch from the blob iterator. This avoids loading the entire chain
	// into memory while keeping I/O efficient, and it bounds how long a
	// single blob read transaction and store pin are held open.
	//
	// fetchBatch opens a fresh iterator per batch and seeks it past the last
	// key taken, so the backend has to bound the listing at that key rather
	// than scan the prefix from its start: badger seeks the LSM tree, s3
	// passes StartAfter, gcs passes Query.StartOffset. A backend that lists
	// from the head of the prefix instead makes the per-batch reopen cost a
	// full prefix scan.
	blobIteratorBatchSize = 1000
)

// blobBlockEntry holds a block key discovered during batch scanning.
type blobBlockEntry struct {
	key   []byte
	slot  uint64
	hash  []byte
	index uint64
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
	ctx        context.Context
	startSlot  uint64
	endSlot    uint64
	hasEndSlot bool
	canonical  bool
	startIndex uint64

	// internal state
	mu                sync.Mutex
	batch             []blobBlockEntry
	batchIdx          int
	currentSlot       uint64
	lastCanonicalSlot uint64
	hasCanonicalSlot  bool
	exhausted         bool
	closed            bool

	// resumeKey is the blob key to seek past when fetching the next batch.
	// nil means start from the beginning (or from startSlot).
	resumeKey []byte

	// batchStore is the blob store batch's keys were scanned from, and
	// batchRelease the pin that keeps that store usable until the batch
	// has been consumed. Every entry in batch is read back through
	// batchStore, so the scan that found a key and the read that fetches
	// its CBOR can never land on two different installations: a
	// SetBlobStore in between would otherwise make a block that exists in
	// the scanned store look absent in the new one, and NextRaw skips a
	// missing block with a warning rather than reporting it. Both fields
	// are set and cleared together, under mu.
	batchStore   blob.BlobStore
	batchRelease func()
}

// BlocksFromSlot returns an iterator that yields blocks starting from
// startSlot, continuing through all subsequent blocks in the blob store.
func (d *Database) BlocksFromSlot(startSlot uint64) *BlobBlockIterator {
	return &BlobBlockIterator{
		db:        d,
		startSlot: startSlot,
	}
}

// CanonicalBlocksFromIndex returns an iterator over the primary-chain block
// index starting at startIndex. previousSlot seeds strict slot ordering for a
// resumed traversal. When endSlot is set, entries above it are audited without
// fetching their block bodies; iteration still continues through index EOF.
func (d *Database) CanonicalBlocksFromIndex(
	ctx context.Context,
	startIndex uint64,
	previousSlot *uint64,
	endSlot *uint64,
) (*BlobBlockIterator, error) {
	ctx = nonNilContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ret := &BlobBlockIterator{
		db:         d,
		ctx:        ctx,
		canonical:  true,
		startIndex: startIndex,
	}
	if previousSlot != nil {
		if *previousSlot == ^uint64(0) {
			return nil, errors.New("previous canonical slot overflows resume position")
		}
		ret.startSlot = *previousSlot + 1
		ret.lastCanonicalSlot = *previousSlot
		ret.hasCanonicalSlot = true
	}
	if endSlot != nil {
		ret.endSlot = *endSlot
		ret.hasEndSlot = true
	}
	return ret, nil
}

// BlocksInRange returns an iterator for a specific slot range [start, end].
// Both endpoints are inclusive. The iterator ignores cancellation; use
// BlocksInRangeContext when the scan must stop with a caller's context.
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

// BlocksInRangeContext returns an iterator for the inclusive slot range
// [start, end] that stops with ctx. NextRaw returns ctx.Err() before each
// key batch scan and block read once ctx is done, so a canceled scan never
// reports a partial range as complete. A nil ctx is never canceled.
func (d *Database) BlocksInRangeContext(
	ctx context.Context,
	startSlot, endSlot uint64,
) *BlobBlockIterator {
	ret := d.BlocksInRange(startSlot, endSlot)
	ret.ctx = nonNilContext(ctx)
	return ret
}

// BlobBlockResult holds the data returned by BlobBlockIterator.NextRaw.
type BlobBlockResult struct {
	Index     uint64
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
		if it.ctx != nil {
			if err := it.ctx.Err(); err != nil {
				return nil, err
			}
		}

		// Refill batch if needed
		if it.batchIdx >= len(it.batch) {
			if it.exhausted {
				// Nothing left to read through the batch's
				// store, so stop holding it open.
				it.releaseBatchPinLocked()
				return nil, nil
			}
			if err := it.fetchBatch(); err != nil {
				return nil, err
			}
			if len(it.batch) == 0 {
				it.exhausted = true
				it.releaseBatchPinLocked()
				return nil, nil
			}
		}

		entry := it.batch[it.batchIdx]
		it.batchIdx++
		var (
			cbor []byte
			meta types.BlockMetadata
		)
		if it.canonical {
			var yieldBlock bool
			var err error
			entry, cbor, meta, yieldBlock, err = it.fetchCanonicalBlock(entry)
			if err != nil {
				return nil, err
			}
			if !yieldBlock {
				continue
			}
		} else {
			var fetchErr error
			cbor, meta, fetchErr = it.fetchBlock(
				entry.slot,
				entry.hash,
			)
			if errors.Is(fetchErr, types.ErrBlobKeyNotFound) {
				it.db.logger.Warn(
					"blob iterator: skipping block with missing CBOR",
					"slot", entry.slot,
					"error", fetchErr,
				)
				continue
			}
			if fetchErr != nil {
				return nil, fmt.Errorf(
					"fetching block at slot %d: %w",
					entry.slot, fetchErr,
				)
			}
		}
		if it.canonical && meta.ID != entry.index {
			return nil, fmt.Errorf(
				"canonical block at slot %d has metadata index %d, expected %d",
				entry.slot, meta.ID, entry.index,
			)
		}
		it.currentSlot = entry.slot

		return &BlobBlockResult{
			Index:     entry.index,
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
	it.releaseBatchPinLocked()
}

// releaseBatchPinLocked drops the pin held for the current batch, if any.
// The fields are cleared before the pin is released, so a repeat call is a
// no-op rather than an unmatched WaitGroup.Done -- which matters because
// both Close (idempotent by contract) and the exhaustion paths in NextRaw
// call it. Callers must hold it.mu.
func (it *BlobBlockIterator) releaseBatchPinLocked() {
	if it.batchRelease == nil {
		return
	}
	release := it.batchRelease
	it.batchRelease = nil
	it.batchStore = nil
	release()
}

// fetchBatch retrieves the next batch of block keys from the blob store.
// Must be called with it.mu held.
func (it *BlobBlockIterator) fetchBatch() error {
	// The previous batch is about to be replaced, so the store it was
	// scanned from is no longer needed. The new pin is held past this
	// function, until the batch it scans has been consumed.
	it.releaseBatchPinLocked()
	store, releaseBlob := it.db.PinBlob()
	if store == nil {
		releaseBlob()
		return types.ErrBlobStoreUnavailable
	}
	it.batchStore = store
	it.batchRelease = releaseBlob
	// A batch that is never produced has nothing to read back, so every
	// error return below drops the pin it just took.
	scanned := false
	defer func() {
		if !scanned {
			it.releaseBatchPinLocked()
		}
	}()

	txn := store.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck

	prefix := []byte(types.BlockBlobKeyPrefix)
	if it.canonical {
		prefix = []byte(types.BlockBlobIndexKeyPrefix)
	}
	iterOpts := types.BlobIteratorOptions{Prefix: prefix}
	blobIter := store.NewIterator(txn, iterOpts)
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
		if it.canonical {
			seekKey = types.BlockBlobIndexKey(it.startIndex)
		} else {
			// Start from the configured start slot
			seekKey = buildSlotSeekKey(it.startSlot)
		}
	}

	// Build end prefix for range limiting.
	// When endSlot is max uint64, all slots are in range so we
	// skip the prefix check to avoid overflow on endSlot+1.
	var endPrefix []byte
	if it.hasEndSlot && it.endSlot < ^uint64(0) {
		endPrefix = buildSlotSeekKey(it.endSlot + 1)
	}

	batch := make([]blobBlockEntry, 0, blobIteratorBatchSize)
	resuming := it.resumeKey != nil

	for blobIter.Seek(seekKey); blobIter.ValidForPrefix(prefix); blobIter.Next() {
		item := blobIter.Item()
		if item == nil {
			if it.canonical {
				return errors.New("canonical block index iterator returned nil item")
			}
			continue
		}
		key := item.Key()
		if key == nil {
			if it.canonical {
				return errors.New("canonical block index iterator returned nil key")
			}
			continue
		}

		var blockIndex uint64
		if it.canonical {
			if len(key) != len(prefix)+8 {
				return fmt.Errorf("invalid canonical block index key: %x", key)
			}
			blockIndex = binary.BigEndian.Uint64(key[len(prefix):])
		} else {
			// Skip metadata keys
			if bytes.HasSuffix(key, []byte(types.BlockBlobMetadataKeySuffix)) {
				continue
			}
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
		if !it.canonical && endPrefix != nil && bytes.Compare(key, endPrefix) >= 0 {
			break
		}

		entry := blobBlockEntry{
			key:   append([]byte(nil), key...),
			index: blockIndex,
		}
		if !it.canonical {
			// Parse slot and hash from key
			point, parseErr := BlockBlobKeyToPoint(key)
			if parseErr != nil {
				it.db.logger.Warn(
					"blob iterator: skipping unparseable key",
					"error", parseErr,
				)
				continue
			}
			if point.Slot < it.startSlot {
				continue
			}
			if it.hasEndSlot && point.Slot > it.endSlot {
				break
			}
			entry.slot = point.Slot
			entry.hash = point.Hash
		}

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

	scanned = true
	return nil
}

type contextBlobGetter interface {
	GetContext(context.Context, types.Txn, []byte) ([]byte, error)
}

type contextLimitedBlobGetter interface {
	GetContextLimited(
		context.Context,
		types.Txn,
		[]byte,
		int64,
	) ([]byte, error)
}

func getBlobContext(
	ctx context.Context,
	store blob.BlobStore,
	txn types.Txn,
	key []byte,
	maxBytes int64,
) ([]byte, error) {
	ctx = nonNilContext(ctx)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var (
		value []byte
		err   error
	)
	if getter, ok := store.(contextLimitedBlobGetter); ok {
		value, err = getter.GetContextLimited(ctx, txn, key, maxBytes)
	} else if getter, ok := store.(contextBlobGetter); ok {
		value, err = getter.GetContext(ctx, txn, key)
	} else {
		value, err = store.Get(txn, key)
	}
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return value, nil
}

// fetchCanonicalBlock reads and validates one canonical index value when its
// block is requested, then reads the body in the same transaction. Keeping
// value reads out of fetchBatch prevents a remote store from issuing a full
// batch of serial GETs before returning the first block.
func (it *BlobBlockIterator) fetchCanonicalBlock(
	entry blobBlockEntry,
) (blobBlockEntry, []byte, types.BlockMetadata, bool, error) {
	store := it.batchStore
	if store == nil {
		return entry, nil, types.BlockMetadata{}, false,
			types.ErrBlobStoreUnavailable
	}
	// CanonicalBlocksFromIndex, the only constructor of a canonical
	// iterator, already replaced a nil context, so it.ctx is non-nil here.
	ctx := it.ctx
	if err := ctx.Err(); err != nil {
		return entry, nil, types.BlockMetadata{}, false, err
	}

	txn := store.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck

	blockKey, err := getBlobContext(
		ctx, store, txn, entry.key, int64(types.BlockBlobKeySize),
	)
	if err != nil {
		return entry, nil, types.BlockMetadata{}, false, fmt.Errorf(
			"reading canonical block index %d: %w", entry.index, err,
		)
	}
	if len(blockKey) != types.BlockBlobKeySize {
		return entry, nil, types.BlockMetadata{}, false, fmt.Errorf(
			"canonical block index %d has invalid value length %d",
			entry.index, len(blockKey),
		)
	}
	point, err := BlockBlobKeyToPoint(blockKey)
	if err != nil {
		return entry, nil, types.BlockMetadata{}, false, fmt.Errorf(
			"parsing canonical block index %d value: %w",
			entry.index, err,
		)
	}
	entry.slot = point.Slot
	entry.hash = point.Hash
	if it.hasCanonicalSlot && entry.slot <= it.lastCanonicalSlot {
		return entry, nil, types.BlockMetadata{}, false, fmt.Errorf(
			"canonical block index %d points to non-increasing slot %d after slot %d",
			entry.index, entry.slot, it.lastCanonicalSlot,
		)
	}
	it.lastCanonicalSlot = entry.slot
	it.hasCanonicalSlot = true
	if it.hasEndSlot && entry.slot > it.endSlot {
		return entry, nil, types.BlockMetadata{}, false, nil
	}
	cbor, meta, err := store.GetBlock(txn, entry.slot, entry.hash)
	if err != nil {
		return entry, nil, types.BlockMetadata{}, false, fmt.Errorf(
			"fetching block at slot %d: %w", entry.slot, err,
		)
	}
	return entry, cbor, meta, true, nil
}

// fetchBlock retrieves the raw CBOR bytes and metadata for a block.
// Must be called with it.mu held.
func (it *BlobBlockIterator) fetchBlock(
	slot uint64,
	hash []byte,
) ([]byte, types.BlockMetadata, error) {
	// The store this batch's keys came from, held by the batch's own pin
	// -- not whichever store is installed now. See batchStore.
	store := it.batchStore
	if store == nil {
		return nil, types.BlockMetadata{}, types.ErrBlobStoreUnavailable
	}

	txn := store.NewTransaction(false)
	defer txn.Rollback() //nolint:errcheck

	return store.GetBlock(txn, slot, hash)
}

// buildSlotSeekKey constructs a blob key prefix for seeking to a specific slot.
func buildSlotSeekKey(slot uint64) []byte {
	key := []byte(types.BlockBlobKeyPrefix)
	slotBytes := types.BlockBlobKeyUint64ToBytes(slot)
	key = append(key, slotBytes...)
	return key
}
