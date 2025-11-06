// Copyright 2025 Blink Labs Software
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
	"encoding/hex"
	"errors"
	"math/big"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/gouroboros/cbor"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
)

const (
	BlockInitialIndex uint64 = 1

	blockBlobKeyPrefix         = "bp"
	blockBlobIndexKeyPrefix    = "bi"
	blockBlobMetadataKeySuffix = "_metadata"
)

func (d *Database) BlockCreate(block models.Block, txn *Txn) error {
	owned := false
	if txn == nil {
		txn = d.BlobTxn(true)
		owned = true
		defer txn.Rollback() //nolint:errcheck
	}
	// Block content by point
	key := BlockBlobKey(block.Slot, block.Hash)
	if err := txn.Blob().Set(key, block.Cbor); err != nil {
		return err
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
	// Block index to point key
	indexKey := BlockBlobIndexKey(block.ID)
	if err := txn.Blob().Set(indexKey, key); err != nil {
		return err
	}
	// Block metadata by point
	metadataKey := BlockBlobMetadataKey(key)
	tmpMetadata := BlockBlobMetadata{
		ID:       block.ID,
		Type:     block.Type,
		Height:   block.Number,
		PrevHash: block.PrevHash,
	}
	tmpMetadataBytes, err := cbor.Encode(tmpMetadata)
	if err != nil {
		return err
	}
	if err := txn.Blob().Set(metadataKey, tmpMetadataBytes); err != nil {
		return err
	}
	if owned {
		if err := txn.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func BlockDeleteTxn(txn *Txn, block models.Block) error {
	// Remove from blob store
	key := BlockBlobKey(block.Slot, block.Hash)
	if err := txn.Blob().Delete(key); err != nil {
		return err
	}
	indexKey := BlockBlobIndexKey(block.ID)
	if err := txn.Blob().Delete(indexKey); err != nil {
		return err
	}
	metadataKey := BlockBlobMetadataKey(key)
	if err := txn.Blob().Delete(metadataKey); err != nil {
		return err
	}
	return nil
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

func blockByKey(txn *Txn, blockKey []byte) (models.Block, error) {
	point := blockBlobKeyToPoint(blockKey)
	ret := models.Block{
		Slot: point.Slot,
		Hash: point.Hash,
	}
	blobTxn := txn.Blob()
	if blobTxn == nil {
		return ret, errors.New("blob transaction is not available")
	}
	item, err := blobTxn.Get(blockKey)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ret, models.ErrBlockNotFound
		}
		return ret, err
	}
	ret.Cbor, err = item.ValueCopy(nil)
	if err != nil {
		return ret, err
	}
	metadataKey := BlockBlobMetadataKey(blockKey)
	item, err = txn.Blob().Get(metadataKey)
	if err != nil {
		return ret, err
	}
	metadataBytes, err := item.ValueCopy(nil)
	if err != nil {
		return ret, err
	}
	var tmpMetadata BlockBlobMetadata
	if _, err := cbor.Decode(metadataBytes, &tmpMetadata); err != nil {
		return ret, err
	}
	ret.ID = tmpMetadata.ID
	ret.Type = tmpMetadata.Type
	ret.Number = tmpMetadata.Height
	ret.PrevHash = tmpMetadata.PrevHash
	return ret, nil
}

func BlockByPointTxn(txn *Txn, point ocommon.Point) (models.Block, error) {
	key := BlockBlobKey(point.Slot, point.Hash)
	return blockByKey(txn, key)
}

func (d *Database) BlockByIndex(
	blockIndex uint64,
	txn *Txn,
) (models.Block, error) {
	if txn == nil {
		txn = d.BlobTxn(false)
		defer txn.Rollback() //nolint:errcheck
	}
	indexKey := BlockBlobIndexKey(blockIndex)
	item, err := txn.Blob().Get(indexKey)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return models.Block{}, models.ErrBlockNotFound
		}
		return models.Block{}, err
	}
	blockKey, err := item.ValueCopy(nil)
	if err != nil {
		return models.Block{}, err
	}
	return blockByKey(txn, blockKey)
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

func BlocksRecentTxn(txn *Txn, count int) ([]models.Block, error) {
	ret := make([]models.Block, 0, count)
	iterOpts := badger.IteratorOptions{
		Reverse: true,
	}
	it := txn.Blob().NewIterator(iterOpts)
	defer it.Close()
	var foundCount int
	// Generate our seek key
	// We use our block index key prefix and append 0xFF to get a key that should be
	// after any legitimate key. This should leave our most recent block as the next
	// item when doing reverse iteration
	tmpPrefix := append([]byte(blockBlobIndexKeyPrefix), 0xff)
	var blockKey []byte
	var err error
	var tmpBlock models.Block
	for it.Seek(tmpPrefix); it.ValidForPrefix([]byte(blockBlobIndexKeyPrefix)); it.Next() {
		item := it.Item()
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
	if txn == nil || txn.Blob() == nil {
		return models.Block{}, errors.New("valid blob transaction is required")
	}
	iterOpts := badger.IteratorOptions{
		Reverse: true,
	}
	it := txn.Blob().NewIterator(iterOpts)
	defer it.Close()
	keyPrefix := slices.Concat(
		[]byte(blockBlobKeyPrefix),
		blockBlobKeyUint64ToBytes(slotNumber),
	)
	for it.Seek(keyPrefix); it.Valid(); it.Next() {
		// Skip if we get a key matching the input slot number
		if it.ValidForPrefix(keyPrefix) {
			continue
		}
		// Check for end of block keys
		if !it.ValidForPrefix([]byte(blockBlobKeyPrefix)) {
			return models.Block{}, models.ErrBlockNotFound
		}
		item := it.Item()
		k := item.Key()
		// Skip the metadata key
		if strings.HasSuffix(string(k), blockBlobMetadataKeySuffix) {
			continue
		}
		return blockByKey(txn, k)
	}
	return models.Block{}, models.ErrBlockNotFound
}

func BlocksAfterSlotTxn(txn *Txn, slotNumber uint64) ([]models.Block, error) {
	var ret []models.Block
	iterOpts := badger.IteratorOptions{}
	it := txn.Blob().NewIterator(iterOpts)
	defer it.Close()
	keyPrefix := slices.Concat(
		[]byte(blockBlobKeyPrefix),
		blockBlobKeyUint64ToBytes(slotNumber),
	)
	var err error
	var k []byte
	var tmpBlock models.Block
	for it.Seek(keyPrefix); it.ValidForPrefix([]byte(blockBlobKeyPrefix)); it.Next() {
		// Skip the start slot
		if it.ValidForPrefix(keyPrefix) {
			continue
		}
		item := it.Item()
		k = item.Key()
		// Skip the metadata key
		if strings.HasSuffix(string(k), blockBlobMetadataKeySuffix) {
			continue
		}
		tmpBlock, err = blockByKey(txn, k)
		if err != nil {
			return []models.Block{}, err
		}
		ret = append(ret, tmpBlock)
	}
	return ret, nil
}

func blockBlobKeyUint64ToBytes(input uint64) []byte {
	ret := make([]byte, 8)
	new(big.Int).SetUint64(input).FillBytes(ret)
	return ret
}

func blockBlobKeyToPoint(key []byte) ocommon.Point {
	slotBytes := make([]byte, 8)
	copy(slotBytes, key[2:2+8])
	hash := make([]byte, 32)
	copy(hash, key[10:10+32])
	slot := new(big.Int).SetBytes(slotBytes).Uint64()
	return ocommon.NewPoint(slot, hash)
}

func BlockBlobKey(slot uint64, hash []byte) []byte {
	key := []byte(blockBlobKeyPrefix)
	// Convert slot to bytes
	slotBytes := blockBlobKeyUint64ToBytes(slot)
	key = append(key, slotBytes...)
	key = append(key, hash...)
	return key
}

func BlockBlobIndexKey(blockNumber uint64) []byte {
	key := []byte(blockBlobIndexKeyPrefix)
	// Convert block number to bytes
	blockNumberBytes := blockBlobKeyUint64ToBytes(blockNumber)
	key = append(key, blockNumberBytes...)
	return key
}

func BlockBlobMetadataKey(baseKey []byte) []byte {
	return slices.Concat(baseKey, []byte(blockBlobMetadataKeySuffix))
}

func BlockBlobKeyHashHex(slot uint64, hashHex string) ([]byte, error) {
	hashBytes, err := hex.DecodeString(hashHex)
	if err != nil {
		return nil, err
	}
	return BlockBlobKey(slot, hashBytes), nil
}

type BlockBlobMetadata struct {
	cbor.StructAsArray
	ID       uint64
	Type     uint
	Height   uint64
	PrevHash []byte
}
