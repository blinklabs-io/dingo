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
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
)

const (
	BlockInitialIndex uint64 = 1
)

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
	err := txn.Do(func(txn *Txn) error {
		var err error
		ret, metadata, err = txn.DB().Blob().GetBlockURL(ctx, txn.Blob(), point)
		return err
	})
	return ret, metadata, err
}

func blockByKey(txn *Txn, blockKey []byte) (models.Block, error) {
	if txn == nil {
		return models.Block{}, types.ErrNilTxn
	}
	if txn.Blob() == nil {
		return models.Block{}, types.ErrNilTxn
	}
	point := blockBlobKeyToPoint(blockKey)
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
	iterOpts := types.BlobIteratorOptions{
		Prefix: []byte(types.BlockBlobKeyPrefix),
	}
	it := blob.NewIterator(blobTxn, iterOpts)
	if it == nil {
		return models.Block{}, errors.New("blob iterator is nil")
	}
	defer it.Close()
	for it.Seek([]byte(types.BlockBlobKeyPrefix)); it.ValidForPrefix([]byte(types.BlockBlobKeyPrefix)); it.Next() {
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
		// Validate key length and hash segment before comparing.
		if len(key) < 10+len(hash) {
			continue
		}
		if !bytes.Equal(key[10:10+len(hash)], hash) {
			continue
		}
		return blockByKey(txn, key)
	}
	if err := it.Err(); err != nil {
		return models.Block{}, err
	}
	return models.Block{}, models.ErrBlockNotFound
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
		return blockByKey(txn, k)
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

func blockBlobKeyToPoint(key []byte) ocommon.Point {
	slotBytes := make([]byte, 8)
	copy(slotBytes, key[2:2+8])
	hash := make([]byte, 32)
	copy(hash, key[10:10+32])
	slot := new(big.Int).SetBytes(slotBytes).Uint64()
	return ocommon.NewPoint(slot, hash)
}
