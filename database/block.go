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
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	BlockInitialIndex uint64 = 1
)

// blockByHashIndexHits and blockByHashIndexMisses track how often
// BlockByHashTxn resolves via the O(1) hash index versus returning a
// hard miss. A non-trivial miss rate on a healthy node is the load-only
// signal that operators can use to justify a hash-index back-fill for
// pre-#1915 blocks (see #2105).
var (
	blockByHashIndexHits   atomic.Uint64
	blockByHashIndexMisses atomic.Uint64
)

// RegisterBlockByHashMetrics exposes the block-hash index hit/miss counters
// on the given Prometheus registry. The underlying counters are process-wide
// atomics, so every registry observes the same totals; registering the same
// registry more than once is a no-op. reg.Register is used instead of
// promauto so a registration conflict never panics during Database.New; an
// AlreadyRegisteredError is ignored and any other error is returned.
func RegisterBlockByHashMetrics(reg prometheus.Registerer) error {
	if reg == nil {
		return nil
	}
	collectors := []prometheus.Collector{
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "dingo_database_block_hash_index_hits_total",
			Help: "Total block-hash index lookups resolved via the O(1) fast path",
		}, func() float64 {
			return float64(blockByHashIndexHits.Load())
		}),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "dingo_database_block_hash_index_misses_total",
			Help: "Total block-hash index lookups that were a hard miss (ErrBlockNotFound)",
		}, func() float64 {
			return float64(blockByHashIndexMisses.Load())
		}),
	}
	for _, c := range collectors {
		err := reg.Register(c)
		if err == nil {
			continue
		}
		// A reused registry already exposes these counters. Both the existing
		// and the new collector read the same process-wide atomics, so the
		// registry still observes the correct totals and the duplicate is
		// safe to ignore.
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if !errors.As(err, &alreadyRegistered) {
			return err
		}
	}
	return nil
}

// BlockByHashStats returns the cumulative hit/miss counts for the
// hash-index fast path used by BlockByHashTxn.
func BlockByHashStats() (hits, misses uint64) {
	return blockByHashIndexHits.Load(), blockByHashIndexMisses.Load()
}

func (d *Database) BlockCreate(block models.Block, txn *Txn) error {
	owned := false
	if txn == nil {
		txn = d.BlobTxn(true)
		owned = true
		defer txn.Rollback() //nolint:errcheck
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return types.ErrNilTxn
	}
	blob := txn.DB().Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	// Set index if not provided
	if block.ID == 0 {
		recentBlocks, err := BlocksRecentTxn(txn, 1)
		if err != nil {
			return err
		}
		if len(recentBlocks) > 0 {
			block.ID = recentBlocks[0].ID + 1
		} else {
			block.ID = BlockInitialIndex
		}
	}
	// Use the new SetBlock method
	if err := blob.SetBlock(blobTxn, block.Slot, block.Hash, block.Cbor, block.ID, block.Type, block.Number, block.PrevHash); err != nil {
		return err
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// SetGenesisCbor stores synthetic genesis CBOR data without creating a block
// index entry. This allows the CBOR to be retrieved for offset-based UTxO
// extraction while preventing the chain iterator from trying to decode it
// as a real block (which would fail since genesis CBOR is just concatenated
// UTxO data, not a valid block structure).
func (d *Database) SetGenesisCbor(slot uint64, hash []byte, cborData []byte, txn *Txn) error {
	owned := false
	if txn == nil {
		txn = d.BlobTxn(true)
		owned = true
		defer txn.Rollback() //nolint:errcheck
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return types.ErrNilTxn
	}
	blob := txn.DB().Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	// Store CBOR data at the block key (allows GetBlock to find it)
	// but don't create an index entry (prevents chain iterator from finding it)
	key := types.BlockBlobKey(slot, hash)
	if err := blob.Set(blobTxn, key, cborData); err != nil {
		return fmt.Errorf("SetGenesisCbor: failed to set block CBOR: %w", err)
	}
	// Also store metadata (required by GetBlock) with ID=0 to indicate genesis
	metadataKey := types.BlockBlobMetadataKey(key)
	tmpMetadata := types.BlockMetadata{
		ID:       0, // Genesis block has no sequential ID
		Type:     0, // Genesis/synthetic block type
		Height:   0,
		PrevHash: nil,
	}
	tmpMetadataBytes, err := cbor.Encode(tmpMetadata)
	if err != nil {
		return err
	}
	if err := blob.Set(blobTxn, metadataKey, tmpMetadataBytes); err != nil {
		return fmt.Errorf("SetGenesisCbor: failed to set block metadata: %w", err)
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return fmt.Errorf("SetGenesisCbor: failed to commit txn: %w", err)
		}
	}
	return nil
}

// HasGenesisCbor checks whether genesis CBOR data exists at the expected
// blob key for the given slot and hash. This is used to validate that
// existing chain data matches the current genesis configuration.
func (d *Database) HasGenesisCbor(slot uint64, hash []byte) bool {
	blob := d.Blob()
	if blob == nil {
		return false
	}
	txn := d.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	key := types.BlockBlobKey(slot, hash)
	_, err := blob.Get(txn.Blob(), key)
	return err == nil
}

// HasAnyGenesisCbor checks whether any genesis CBOR data exists at the
// given slot, regardless of hash. This is used to distinguish between
// "no genesis CBOR" (e.g., after Mithril bootstrap) and "genesis CBOR
// exists but with a different hash" (true network mismatch).
func (d *Database) HasAnyGenesisCbor(slot uint64) bool {
	blob := d.Blob()
	if blob == nil {
		return false
	}
	txn := d.BlobTxn(false)
	defer txn.Rollback() //nolint:errcheck
	prefix := slices.Concat(
		[]byte(types.BlockBlobKeyPrefix),
		types.BlockBlobKeyUint64ToBytes(slot),
	)
	opts := types.BlobIteratorOptions{Prefix: prefix}
	it := blob.NewIterator(txn.Blob(), opts)
	if it == nil {
		return false
	}
	defer it.Close()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		if item == nil {
			continue
		}
		key := item.Key()
		if key == nil {
			continue
		}
		// Skip the metadata key
		if strings.HasSuffix(string(key), types.BlockBlobMetadataKeySuffix) {
			continue
		}
		return true
	}
	return false
}

func BlockDeleteTxn(txn *Txn, block models.Block) error {
	if txn == nil {
		return types.ErrNilTxn
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return types.ErrNilTxn
	}
	blob := txn.DB().Blob()
	if blob == nil {
		return types.ErrBlobStoreUnavailable
	}
	// Use the new DeleteBlock method
	return blob.DeleteBlock(blobTxn, block.Slot, block.Hash, block.ID)
}

func BlockByPoint(db *Database, point ocommon.Point) (models.Block, error) {
	var ret models.Block
	txn := db.Transaction(false)
	err := txn.Do(func(txn *Txn) error {
		var err error
		ret, err = BlockByPointTxn(txn, point)
		return err
	})
	return ret, err
}

func BlockByHash(db *Database, hash []byte) (models.Block, error) {
	var ret models.Block
	txn := db.Transaction(false)
	err := txn.Do(func(txn *Txn) error {
		var err error
		ret, err = BlockByHashTxn(txn, hash)
		return err
	})
	return ret, err
}

func BlockBySlot(db *Database, slot uint64) (models.Block, error) {
	var ret models.Block
	txn := db.Transaction(false)
	err := txn.Do(func(txn *Txn) error {
		var err error
		ret, err = BlockBySlotTxn(txn, slot)
		return err
	})
	return ret, err
}

func BlockURL(
	ctx context.Context,
	db *Database,
	point ocommon.Point,
) (types.SignedURL, types.BlockMetadata, error) {
	var (
		ret      types.SignedURL
		metadata types.BlockMetadata
	)

	txn := db.BlobTxn(false)
	if txn == nil {
		return types.SignedURL{}, types.BlockMetadata{}, types.ErrNilTxn
	}

	err := txn.Do(func(txn *Txn) error {
		var err error
		if txn == nil {
			return types.ErrNilTxn
		}

		blob := txn.DB().Blob()
		if blob == nil {
			return types.ErrBlobStoreUnavailable
		}

		ret, metadata, err = blob.GetBlockURL(ctx, txn.Blob(), point)
		return err
	})
	if err != nil {
		return types.SignedURL{}, types.BlockMetadata{},
			fmt.Errorf("failed getting block url: %w", err)
	}

	return ret, metadata, nil
}

func blockByKey(txn *Txn, blockKey []byte) (models.Block, error) {
	if txn == nil {
		return models.Block{}, types.ErrNilTxn
	}
	if txn.Blob() == nil {
		return models.Block{}, types.ErrNilTxn
	}
	point, err := BlockBlobKeyToPoint(blockKey)
	if err != nil {
		return models.Block{}, fmt.Errorf(
			"parsing block key: %w", err,
		)
	}
	blob := txn.DB().Blob()
	if blob == nil {
		return models.Block{}, types.ErrBlobStoreUnavailable
	}
	cborData, metadata, err := blob.GetBlock(txn.Blob(), point.Slot, point.Hash)
	if err != nil {
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return models.Block{}, models.ErrBlockNotFound
		}
		return models.Block{}, err
	}
	ret := models.Block{
		Slot:     point.Slot,
		Hash:     point.Hash,
		Cbor:     cborData,
		ID:       metadata.ID,
		Type:     metadata.Type,
		Number:   metadata.Height,
		PrevHash: metadata.PrevHash,
	}
	return ret, nil
}

func BlockByPointTxn(txn *Txn, point ocommon.Point) (models.Block, error) {
	key := types.BlockBlobKey(point.Slot, point.Hash)
	return blockByKey(txn, key)
}

func BlockByHashTxn(txn *Txn, hash []byte) (models.Block, error) {
	if txn == nil {
		return models.Block{}, types.ErrNilTxn
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return models.Block{}, types.ErrNilTxn
	}
	blob := txn.DB().Blob()
	if blob == nil {
		return models.Block{}, types.ErrBlobStoreUnavailable
	}
	// O(1) hash-index lookup. The index has been written for every block
	// since #1915, so a miss on a healthy DB means either (a) the hash is
	// genuinely unknown (the common fork-resolution case, peer probes an
	// ancestor we have not seen) or (b) the block pre-dates the index and
	// needs a back-fill. Treating both as ErrBlockNotFound keeps the path
	// allocation-free; the previous iterator fallback ran a full block-
	// blob prefix scan per probe and was the second largest CPU consumer
	// during catch-up (issue #2105).
	hashIndexKey := types.BlockHashIndexKey(hash)
	blockKey, err := blob.Get(blobTxn, hashIndexKey)
	if err == nil && len(blockKey) > 0 {
		block, err := blockByKey(txn, blockKey)
		if err != nil {
			return models.Block{}, err
		}
		blockByHashIndexHits.Add(1)
		return block, nil
	}
	if err == nil && len(blockKey) == 0 {
		// Empty value at a present hash-index entry is local DB corruption,
		// not a soft miss. Surface a descriptive error so the operator sees
		// the problem rather than retrying on another peer.
		return models.Block{}, fmt.Errorf("empty block hash index entry for %s", hex.EncodeToString(hash))
	}
	if err != nil && !errors.Is(err, types.ErrBlobKeyNotFound) {
		// Surface real backend errors (I/O, closed DB) instead of folding
		// them into ErrBlockNotFound.
		return models.Block{}, err
	}
	blockByHashIndexMisses.Add(1)
	// Index miss is a hard miss (#2105). Per-peer chainsync /
	// intersect-point callers should fall back to PeerHeaderLookupFunc
	// rather than scan the bp prefix on every miss; legacy blocks
	// predating the hash index need an offline backfill.
	return models.Block{}, models.ErrBlockNotFound
}

func BlockBySlotTxn(txn *Txn, slot uint64) (models.Block, error) {
	if txn == nil {
		return models.Block{}, types.ErrNilTxn
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return models.Block{}, types.ErrNilTxn
	}
	blob := txn.DB().Blob()
	if blob == nil {
		return models.Block{}, types.ErrBlobStoreUnavailable
	}
	slotPrefix := slices.Concat(
		[]byte(types.BlockBlobKeyPrefix),
		types.BlockBlobKeyUint64ToBytes(slot),
	)
	iterOpts := types.BlobIteratorOptions{
		Prefix: slotPrefix,
	}
	it := blob.NewIterator(blobTxn, iterOpts)
	if it == nil {
		return models.Block{}, errors.New("blob iterator is nil")
	}
	defer it.Close()
	var ret models.Block
	found := false
	for it.Seek(slotPrefix); it.ValidForPrefix(slotPrefix); it.Next() {
		item := it.Item()
		if item == nil {
			continue
		}
		k := item.Key()
		if k == nil {
			continue
		}
		if strings.HasSuffix(string(k), types.BlockBlobMetadataKeySuffix) {
			continue
		}
		block, err := blockByKey(txn, k)
		if err != nil {
			return models.Block{}, err
		}
		if block.Slot != slot {
			continue
		}
		indexedBlock, err := txn.DB().BlockByIndex(block.ID, txn)
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				continue
			}
			return models.Block{}, err
		}
		if indexedBlock.Slot != block.Slot ||
			!bytes.Equal(indexedBlock.Hash, block.Hash) {
			continue
		}
		if !found || block.ID > ret.ID {
			ret = block
			found = true
		}
	}
	if err := it.Err(); err != nil {
		return models.Block{}, err
	}
	if !found {
		return models.Block{}, models.ErrBlockNotFound
	}
	return ret, nil
}

func (d *Database) BlockByIndex(
	blockIndex uint64,
	txn *Txn,
) (models.Block, error) {
	if txn == nil {
		txn = d.BlobTxn(false)
		defer txn.Rollback() //nolint:errcheck
	}
	indexKey := types.BlockBlobIndexKey(blockIndex)
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return models.Block{}, types.ErrNilTxn
	}
	blob := txn.DB().Blob()
	if blob == nil {
		return models.Block{}, types.ErrBlobStoreUnavailable
	}
	val, err := blob.Get(blobTxn, indexKey)
	if err != nil {
		if errors.Is(err, types.ErrBlobKeyNotFound) {
			return models.Block{}, models.ErrBlockNotFound
		}
		return models.Block{}, err
	}
	return blockByKey(txn, val)
}

// BlockAtOrAfterIndex returns the first block whose chain index is greater
// than or equal to blockIndex. It seeks the ordered block-index keys so sparse
// imported chains do not require one lookup and transaction per missing index.
func (d *Database) BlockAtOrAfterIndex(
	blockIndex uint64,
	txn *Txn,
) (models.Block, error) {
	if txn == nil {
		txn = d.BlobTxn(false)
		defer txn.Rollback() //nolint:errcheck
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return models.Block{}, types.ErrNilTxn
	}
	blob := txn.DB().Blob()
	if blob == nil {
		return models.Block{}, types.ErrBlobStoreUnavailable
	}
	prefix := []byte(types.BlockBlobIndexKeyPrefix)
	it := blob.NewIterator(
		blobTxn,
		types.BlobIteratorOptions{Prefix: prefix},
	)
	if it == nil {
		return models.Block{}, errors.New("blob iterator is nil")
	}
	defer it.Close()
	it.Seek(types.BlockBlobIndexKey(blockIndex))
	for it.ValidForPrefix(prefix) {
		item := it.Item()
		if item == nil {
			it.Next()
			continue
		}
		indexKey := item.Key()
		if indexKey == nil {
			it.Next()
			continue
		}
		blockKey, err := item.ValueCopy(nil)
		if err != nil {
			return models.Block{}, err
		}
		block, err := blockByKey(txn, blockKey)
		if err != nil {
			if errors.Is(err, models.ErrBlockNotFound) {
				it.Next()
				continue
			}
			return models.Block{}, err
		}
		// The index value is an indirection to the block blob. A stale
		// mapping may still resolve successfully but point at a block from
		// an older index. Only accept the block when its metadata ID matches
		// the ordered index key currently being visited.
		if !bytes.Equal(indexKey, types.BlockBlobIndexKey(block.ID)) {
			it.Next()
			continue
		}
		return block, nil
	}
	if err := it.Err(); err != nil {
		return models.Block{}, err
	}
	return models.Block{}, models.ErrBlockNotFound
}

func BlocksRecent(db *Database, count int) ([]models.Block, error) {
	var ret []models.Block
	txn := db.Transaction(false)
	err := txn.Do(func(txn *Txn) error {
		var err error
		ret, err = BlocksRecentTxn(txn, count)
		return err
	})
	return ret, err
}

// BlocksRecentTxn returns the N most recent blocks; keep txn valid until results are consumed.
func BlocksRecentTxn(txn *Txn, count int) ([]models.Block, error) {
	if txn == nil {
		return nil, types.ErrNilTxn
	}
	ret := make([]models.Block, 0, count)
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return ret, types.ErrNilTxn
	}
	blob := txn.DB().Blob()
	if blob == nil {
		return ret, types.ErrBlobStoreUnavailable
	}
	iterOpts := types.BlobIteratorOptions{
		Reverse: true,
		Prefix:  []byte(types.BlockBlobIndexKeyPrefix),
	}
	it := blob.NewIterator(blobTxn, iterOpts)
	if it == nil {
		return ret, errors.New("blob iterator is nil")
	}
	defer it.Close()
	var foundCount int
	// Generate our seek key
	// We use our block index key prefix and append 0xFF to get a key that should be
	// after any legitimate key. This should leave our most recent block as the next
	// item when doing reverse iteration
	tmpPrefix := append([]byte(types.BlockBlobIndexKeyPrefix), 0xff)
	var blockKey []byte
	var err error
	var tmpBlock models.Block
	for it.Seek(tmpPrefix); it.ValidForPrefix([]byte(types.BlockBlobIndexKeyPrefix)); it.Next() {
		item := it.Item()
		if item == nil {
			continue
		}
		blockKey, err = item.ValueCopy(nil)
		if err != nil {
			return ret, err
		}
		tmpBlock, err = blockByKey(txn, blockKey)
		if err != nil {
			return ret, err
		}
		ret = append(ret, tmpBlock)
		foundCount++
		if foundCount >= count {
			break
		}
	}
	if err := it.Err(); err != nil {
		return ret, err
	}
	return ret, nil
}

func BlockBeforeSlot(db *Database, slotNumber uint64) (models.Block, error) {
	var ret models.Block
	txn := db.Transaction(false)
	err := txn.Do(func(txn *Txn) error {
		var err error
		ret, err = BlockBeforeSlotTxn(txn, slotNumber)
		return err
	})
	return ret, err
}

func BlockBeforeSlotTxn(txn *Txn, slotNumber uint64) (models.Block, error) {
	if txn == nil {
		return models.Block{}, types.ErrNilTxn
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return models.Block{}, types.ErrNilTxn
	}
	blob := txn.DB().Blob()
	if blob == nil {
		return models.Block{}, types.ErrBlobStoreUnavailable
	}
	iterOpts := types.BlobIteratorOptions{
		Reverse: true,
		Prefix:  []byte(types.BlockBlobKeyPrefix),
	}
	it := blob.NewIterator(blobTxn, iterOpts)
	if it == nil {
		return models.Block{}, errors.New("blob iterator is nil")
	}
	defer it.Close()
	keyPrefix := slices.Concat(
		[]byte(types.BlockBlobKeyPrefix),
		types.BlockBlobKeyUint64ToBytes(slotNumber),
	)
	for it.Seek(keyPrefix); it.Valid(); it.Next() {
		// Skip if we get a key matching the input slot number
		if it.ValidForPrefix(keyPrefix) {
			continue
		}
		// Check for end of block keys
		if !it.ValidForPrefix([]byte(types.BlockBlobKeyPrefix)) {
			return models.Block{}, models.ErrBlockNotFound
		}
		item := it.Item()
		if item == nil {
			continue
		}
		k := item.Key()
		if k == nil {
			continue
		}
		// Skip the metadata key
		if strings.HasSuffix(string(k), types.BlockBlobMetadataKeySuffix) {
			continue
		}
		blk, blkErr := blockByKey(txn, k)
		if blkErr != nil {
			return models.Block{}, blkErr
		}
		// Skip synthetic blobs. Genesis CBOR and Leios endorser blocks are
		// persisted at block-blob keys via SetGenesisCbor with ID=0 and no
		// chain index entry; they are not ranking blocks. Returning one here
		// would feed callers (epoch-nonce labNonce/candidate computation,
		// chain reconciliation) a non-ranking blob whose hash is not on the
		// chain. Older PrevHash-based lab lookup also collapsed this case to
		// an empty lab. Real blocks created via BlockCreate always have ID >=
		// BlockInitialIndex.
		if blk.ID == 0 {
			continue
		}
		return blk, nil
	}
	if err := it.Err(); err != nil {
		return models.Block{}, err
	}
	return models.Block{}, models.ErrBlockNotFound
}

// BlocksAfterSlotTxn returns all blocks after the specified slot; keep txn valid until results are consumed.
func BlocksAfterSlotTxn(txn *Txn, slotNumber uint64) ([]models.Block, error) {
	if txn == nil {
		return nil, types.ErrNilTxn
	}
	var ret []models.Block
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return ret, types.ErrNilTxn
	}
	blob := txn.DB().Blob()
	if blob == nil {
		return ret, types.ErrBlobStoreUnavailable
	}
	iterOpts := types.BlobIteratorOptions{
		Prefix: []byte(types.BlockBlobKeyPrefix),
	}
	it := blob.NewIterator(blobTxn, iterOpts)
	if it == nil {
		return ret, errors.New("blob iterator is nil")
	}
	defer it.Close()
	keyPrefix := slices.Concat(
		[]byte(types.BlockBlobKeyPrefix),
		types.BlockBlobKeyUint64ToBytes(slotNumber),
	)
	var err error
	var k []byte
	var tmpBlock models.Block
	for it.Seek(keyPrefix); it.ValidForPrefix([]byte(types.BlockBlobKeyPrefix)); it.Next() {
		// Skip the start slot
		if it.ValidForPrefix(keyPrefix) {
			continue
		}
		item := it.Item()
		if item == nil {
			continue
		}
		k = item.Key()
		if k == nil {
			continue
		}
		// Skip the metadata key
		if strings.HasSuffix(string(k), types.BlockBlobMetadataKeySuffix) {
			continue
		}
		tmpBlock, err = blockByKey(txn, k)
		if err != nil {
			return ret, err
		}
		ret = append(ret, tmpBlock)
	}
	if err := it.Err(); err != nil {
		return ret, err
	}
	return ret, nil
}

// CountBlocksAndOldestSlot iterates every block-content ("bp") key in the
// blob store once, returning the total count of retained blocks and the
// smallest slot among them. Block-content keys sort in ascending slot
// order, so the oldest slot is simply whichever one is seen first — no
// separate MIN pass is needed. A tombstoned entry (history-expiry pruned
// its content, keeping only the bp key alive so hash/index lookups still
// resolve — see TombstoneBlock) is excluded from both: its data isn't
// actually retained, so counting it would misrepresent both how much
// history is available and how far back it goes. The blob plugin's own
// ValueCopy already turns a tombstoned entry's read into
// types.ErrHistoryExpired (matching GetBlock's convention) rather than
// handing back the raw marker bytes, so that error — not a hand-rolled
// magic-byte check — is what this treats as "skip, don't count". Reading
// each entry's value at all (to tell a tombstone from a real block) is
// the dominant cost of this scan — negligible if history-expiry is
// disabled (no block is ever tombstoned), non-trivial on a large chain
// otherwise.
//
// There is no maintained counter for either value, so this is a genuine
// full scan of the block-index keyspace: appropriate for an
// operator-facing diagnostic (bark's GetDatabaseInfo RPC) called
// occasionally, not a hot path.
func (d *Database) CountBlocksAndOldestSlot(
	txn *Txn,
) (count uint64, oldestSlot uint64, err error) {
	owned := false
	if txn == nil {
		txn = d.BlobTxn(false)
		owned = true
		defer func() {
			if owned {
				txn.Rollback() //nolint:errcheck
			}
		}()
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return 0, 0, types.ErrNilTxn
	}
	blob := d.Blob()
	if blob == nil {
		return 0, 0, types.ErrBlobStoreUnavailable
	}

	iterOpts := types.BlobIteratorOptions{
		Prefix: []byte(types.BlockBlobKeyPrefix),
	}
	it := blob.NewIterator(blobTxn, iterOpts)
	if it == nil {
		return 0, 0, errors.New("blob iterator is nil")
	}
	defer it.Close()

	first := true
	for it.Seek([]byte(types.BlockBlobKeyPrefix)); it.ValidForPrefix([]byte(types.BlockBlobKeyPrefix)); it.Next() {
		item := it.Item()
		if item == nil {
			continue
		}
		key := item.Key()
		if key == nil ||
			strings.HasSuffix(string(key), types.BlockBlobMetadataKeySuffix) {
			continue
		}
		slot, _, parseErr := types.ParseBlockBlobKey(key)
		if parseErr != nil {
			continue
		}
		if _, valErr := item.ValueCopy(nil); valErr != nil {
			if errors.Is(valErr, types.ErrHistoryExpired) {
				continue
			}
			return count, oldestSlot, fmt.Errorf(
				"read block content for count: %w", valErr,
			)
		}
		count++
		if first {
			oldestSlot = slot
			first = false
		}
	}
	if err := it.Err(); err != nil {
		return count, oldestSlot, err
	}
	return count, oldestSlot, nil
}

// BlockBlobKeyToPoint extracts slot and hash from a block blob key.
// Key format: "bp" (2 bytes) + slot (8 bytes big-endian) + hash (32 bytes).
func BlockBlobKeyToPoint(key []byte) (ocommon.Point, error) {
	slot, hash, err := types.ParseBlockBlobKey(key)
	if err != nil {
		return ocommon.Point{}, err
	}
	return ocommon.NewPoint(slot, hash), nil
}
