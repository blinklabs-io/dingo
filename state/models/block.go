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

package models

import (
	"encoding/hex"
	"errors"
	"math/big"
	"slices"
	"strings"

	"github.com/blinklabs-io/dingo/database"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
)

const (
	blockBlobKeyPrefix         = "bp"
	blockBlobHeightKeyPrefix   = "bh"
	blockBlobMetadataKeySuffix = "_metadata"
)

var ErrBlockNotFound = errors.New("block not found")

type Block struct {
	Slot     uint64
	Number   uint64
	Hash     []byte
	Type     uint
	PrevHash []byte
	Nonce    []byte
	Cbor     []byte
}

func (b Block) Decode() (ledger.Block, error) {
	return ledger.NewBlockFromCbor(b.Type, b.Cbor)
}

func BlockCreateTxn(txn *database.Txn, block Block) error {
	// Block content by point
	key := BlockBlobKey(block.Slot, block.Hash)
	if err := txn.Blob().Set(key, block.Cbor); err != nil {
		return err
	}
	// Block height to point key
	heightKey := BlockBlobHeightKey(block.Number)
	if err := txn.Blob().Set(heightKey, key); err != nil {
		return err
	}
	// Block metadata by point
	metadataKey := BlockBlobMetadataKey(key)
	tmpMetadata := BlockBlobMetadata{
		Type:     block.Type,
		Height:   block.Number,
		PrevHash: block.PrevHash,
		Nonce:    block.Nonce,
	}
	tmpMetadataBytes, err := cbor.Encode(tmpMetadata)
	if err != nil {
		return err
	}
	if err := txn.Blob().Set(metadataKey, tmpMetadataBytes); err != nil {
		return err
	}
	return nil
}

func BlockDeleteTxn(txn *database.Txn, block Block) error {
	// Remove from blob store
	key := BlockBlobKey(block.Slot, block.Hash)
	if err := txn.Blob().Delete(key); err != nil {
		return err
	}
	heightKey := BlockBlobHeightKey(block.Number)
	if err := txn.Blob().Delete(heightKey); err != nil {
		return err
	}
	metadataKey := BlockBlobMetadataKey(key)
	if err := txn.Blob().Delete(metadataKey); err != nil {
		return err
	}
	return nil
}

func BlockByPoint(db database.Database, point ocommon.Point) (Block, error) {
	var ret Block
	txn := db.Transaction(false)
	err := txn.Do(func(txn *database.Txn) error {
		var err error
		ret, err = BlockByPointTxn(txn, point)
		return err
	})
	return ret, err
}

func blockByKey(txn *database.Txn, blockKey []byte) (Block, error) {
	point := blockBlobKeyToPoint(blockKey)
	ret := Block{
		Slot: point.Slot,
		Hash: point.Hash,
	}
	item, err := txn.Blob().Get(blockKey)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ret, ErrBlockNotFound
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
	ret.Type = tmpMetadata.Type
	ret.Number = tmpMetadata.Height
	ret.PrevHash = tmpMetadata.PrevHash
	ret.Nonce = tmpMetadata.Nonce
	return ret, nil
}

func BlockByPointTxn(txn *database.Txn, point ocommon.Point) (Block, error) {
	key := BlockBlobKey(point.Slot, point.Hash)
	return blockByKey(txn, key)
}

func BlockByNumber(db database.Database, blockNumber uint64) (Block, error) {
	var ret Block
	txn := db.Transaction(false)
	err := txn.Do(func(txn *database.Txn) error {
		var err error
		ret, err = BlockByNumberTxn(txn, blockNumber)
		return err
	})
	return ret, err
}

func BlockByNumberTxn(txn *database.Txn, blockNumber uint64) (Block, error) {
	heightKey := BlockBlobHeightKey(blockNumber)
	item, err := txn.Blob().Get(heightKey)
	if err != nil {
		return Block{}, err
	}
	blockKey, err := item.ValueCopy(nil)
	if err != nil {
		return Block{}, err
	}
	return blockByKey(txn, blockKey)
}

func BlocksRecent(db database.Database, count int) ([]Block, error) {
	var ret []Block
	txn := db.Transaction(false)
	err := txn.Do(func(txn *database.Txn) error {
		var err error
		ret, err = BlocksRecentTxn(txn, count)
		return err
	})
	return ret, err
}

func BlocksRecentTxn(txn *database.Txn, count int) ([]Block, error) {
	ret := make([]Block, 0, count)
	iterOpts := badger.IteratorOptions{
		Reverse: true,
	}
	it := txn.Blob().NewIterator(iterOpts)
	defer it.Close()
	var foundCount int
	// Generate our seek key
	// We use our block key prefix and append 0xFF to get a key that should be after
	// any legitimate block key. This should leave our most recent block as the next
	// item when doing reverse iteration
	tmpPrefix := append([]byte(blockBlobKeyPrefix), 0xff)
	for it.Seek(tmpPrefix); it.Valid(); it.Next() {
		item := it.Item()
		k := item.Key()
		// Skip the metadata key
		if strings.HasSuffix(string(k), blockBlobMetadataKeySuffix) {
			continue
		}
		tmpBlock, err := blockByKey(txn, k)
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

func BlockBeforeSlot(db database.Database, slotNumber uint64) (Block, error) {
	var ret Block
	txn := db.Transaction(false)
	err := txn.Do(func(txn *database.Txn) error {
		var err error
		ret, err = BlockBeforeSlotTxn(txn, slotNumber)
		return err
	})
	return ret, err
}

func BlockBeforeSlotTxn(txn *database.Txn, slotNumber uint64) (Block, error) {
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
			return Block{}, ErrBlockNotFound
		}
		item := it.Item()
		k := item.Key()
		// Skip the metadata key
		if strings.HasSuffix(string(k), blockBlobMetadataKeySuffix) {
			continue
		}
		return blockByKey(txn, k)
	}
	return Block{}, ErrBlockNotFound
}

func BlocksAfterSlotTxn(txn *database.Txn, slotNumber uint64) ([]Block, error) {
	var ret []Block
	iterOpts := badger.IteratorOptions{}
	it := txn.Blob().NewIterator(iterOpts)
	defer it.Close()
	keyPrefix := slices.Concat(
		[]byte(blockBlobKeyPrefix),
		blockBlobKeyUint64ToBytes(slotNumber),
	)
	for it.Seek(keyPrefix); it.ValidForPrefix([]byte(blockBlobKeyPrefix)); it.Next() {
		// Skip the start slot
		if it.ValidForPrefix(keyPrefix) {
			continue
		}
		item := it.Item()
		k := item.Key()
		// Skip the metadata key
		if strings.HasSuffix(string(k), blockBlobMetadataKeySuffix) {
			continue
		}
		tmpBlock, err := blockByKey(txn, k)
		if err != nil {
			return []Block{}, err
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

func BlockBlobHeightKey(blockNumber uint64) []byte {
	key := []byte(blockBlobHeightKeyPrefix)
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
	Type     uint
	Height   uint64
	PrevHash []byte
	Nonce    []byte
}
