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
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"strings"

	"github.com/blinklabs-io/dingo/database/plugin/blob"
	"github.com/blinklabs-io/dingo/database/types"
)

const blockInventoryValueSize = 24

var (
	blockInventoryKey         = []byte("dingo:block-inventory:v1")
	blockInventoryIndexPrefix = []byte("dingo:block-inventory-block:v1:")
	blockInventoryMagic       = [4]byte{'D', 'B', 'I', '1'}
)

type blockInventory struct {
	Count      uint64
	OldestSlot uint64
}

func encodeBlockInventory(inventory blockInventory) []byte {
	ret := make([]byte, blockInventoryValueSize)
	copy(ret[:4], blockInventoryMagic[:])
	binary.BigEndian.PutUint64(ret[4:12], inventory.Count)
	binary.BigEndian.PutUint64(ret[12:20], inventory.OldestSlot)
	binary.BigEndian.PutUint32(ret[20:24], crc32.ChecksumIEEE(ret[:20]))
	return ret
}

func decodeBlockInventory(data []byte) (blockInventory, error) {
	if len(data) != blockInventoryValueSize {
		return blockInventory{}, fmt.Errorf(
			"invalid block inventory length: got %d, want %d",
			len(data), blockInventoryValueSize,
		)
	}
	if string(data[:4]) != string(blockInventoryMagic[:]) {
		return blockInventory{}, errors.New("invalid block inventory magic")
	}
	if got, want := binary.BigEndian.Uint32(data[20:24]), crc32.ChecksumIEEE(data[:20]); got != want {
		return blockInventory{}, errors.New("invalid block inventory checksum")
	}
	ret := blockInventory{
		Count:      binary.BigEndian.Uint64(data[4:12]),
		OldestSlot: binary.BigEndian.Uint64(data[12:20]),
	}
	if ret.Count == 0 && ret.OldestSlot != 0 {
		return blockInventory{}, errors.New(
			"empty block inventory has a nonzero oldest slot",
		)
	}
	return ret, nil
}

func readBlockInventory(
	store blob.BlobStore,
	txn types.Txn,
) (blockInventory, error) {
	data, err := store.Get(txn, blockInventoryKey)
	if err != nil {
		return blockInventory{}, err
	}
	return decodeBlockInventory(data)
}

func writeBlockInventory(
	store blob.BlobStore,
	txn types.Txn,
	inventory blockInventory,
) error {
	return store.Set(txn, blockInventoryKey, encodeBlockInventory(inventory))
}

func scanBlockInventory(
	store blob.BlobStore,
	txn types.Txn,
) (blockInventory, error) {
	iterOpts := types.BlobIteratorOptions{
		Prefix: []byte(types.BlockBlobKeyPrefix),
	}
	it := store.NewIterator(txn, iterOpts)
	if it == nil {
		return blockInventory{}, errors.New("blob iterator is nil")
	}
	defer it.Close()

	var ret blockInventory
	for it.Seek([]byte(types.BlockBlobKeyPrefix)); it.ValidForPrefix([]byte(types.BlockBlobKeyPrefix)); it.Next() {
		item := it.Item()
		if item == nil {
			continue
		}
		key := item.Key()
		if key == nil || strings.HasSuffix(string(key), types.BlockBlobMetadataKeySuffix) {
			continue
		}
		slot, hash, err := types.ParseBlockBlobKey(key)
		if err != nil {
			continue
		}
		if _, err := item.ValueCopy(nil); err != nil {
			if errors.Is(err, types.ErrHistoryExpired) {
				continue
			}
			return blockInventory{}, fmt.Errorf(
				"read block content for inventory: %w", err,
			)
		}
		if ret.Count == 0 {
			ret.OldestSlot = slot
		}
		if ret.Count == math.MaxUint64 {
			return blockInventory{}, errors.New("block inventory count overflow")
		}
		indexKey, err := blockInventoryIndexKey(slot, hash)
		if err != nil {
			return blockInventory{}, err
		}
		if err := store.Set(txn, indexKey, nil); err != nil {
			return blockInventory{}, fmt.Errorf(
				"index retained block for inventory: %w", err,
			)
		}
		ret.Count++
	}
	if err := it.Err(); err != nil {
		return blockInventory{}, err
	}
	return ret, nil
}

func blockInventoryIndexKey(slot uint64, hash []byte) ([]byte, error) {
	blockKey := types.BlockBlobKey(slot, hash)
	if _, _, err := types.ParseBlockBlobKey(blockKey); err != nil {
		return nil, fmt.Errorf("build block inventory index key: %w", err)
	}
	ret := make([]byte, 0, len(blockInventoryIndexPrefix)+len(blockKey)-len(types.BlockBlobKeyPrefix))
	ret = append(ret, blockInventoryIndexPrefix...)
	ret = append(ret, blockKey[len(types.BlockBlobKeyPrefix):]...)
	return ret, nil
}

func blockInventoryIndexSlot(key []byte) (uint64, error) {
	if !bytes.HasPrefix(key, blockInventoryIndexPrefix) {
		return 0, errors.New("invalid block inventory index prefix")
	}
	blockKey := make([]byte, 0, len(types.BlockBlobKeyPrefix)+len(key)-len(blockInventoryIndexPrefix))
	blockKey = append(blockKey, types.BlockBlobKeyPrefix...)
	blockKey = append(blockKey, key[len(blockInventoryIndexPrefix):]...)
	slot, _, err := types.ParseBlockBlobKey(blockKey)
	if err != nil {
		return 0, fmt.Errorf("parse block inventory index key: %w", err)
	}
	return slot, nil
}

func clearBlockInventoryIndex(store blob.BlobStore, txn types.Txn) error {
	it := store.NewIterator(txn, types.BlobIteratorOptions{
		Prefix: blockInventoryIndexPrefix,
	})
	if it == nil {
		return errors.New("block inventory index iterator is nil")
	}
	defer it.Close()
	for it.Seek(blockInventoryIndexPrefix); it.ValidForPrefix(blockInventoryIndexPrefix); it.Next() {
		item := it.Item()
		if item == nil {
			continue
		}
		if err := store.Delete(txn, item.Key()); err != nil {
			return fmt.Errorf("clear block inventory index: %w", err)
		}
	}
	return it.Err()
}

func oldestIndexedBlockSlot(
	store blob.BlobStore,
	txn *Txn,
) (uint64, error) {
	it := store.NewIterator(txn.Blob(), types.BlobIteratorOptions{
		Prefix: blockInventoryIndexPrefix,
	})
	if it == nil {
		return 0, errors.New("block inventory index iterator is nil")
	}
	defer it.Close()
	it.Seek(blockInventoryIndexPrefix)
	var (
		oldest uint64
		found  bool
	)
	if it.ValidForPrefix(blockInventoryIndexPrefix) {
		item := it.Item()
		if item == nil {
			return 0, errors.New("block inventory index item is nil")
		}
		slot, err := blockInventoryIndexSlot(item.Key())
		if err != nil {
			return 0, err
		}
		oldest, found = slot, true
	}
	if err := it.Err(); err != nil {
		return 0, err
	}
	for _, slot := range txn.blockInventoryAdded {
		if !found || slot < oldest {
			oldest, found = slot, true
		}
	}
	if !found {
		return 0, errors.New("block inventory index is empty")
	}
	return oldest, nil
}

func (d *Database) initBlockInventory() error {
	txn := d.BlobTxn(true)
	defer txn.Rollback() //nolint:errcheck
	store := txn.BlobStore()
	if store == nil || txn.Blob() == nil {
		return types.ErrBlobStoreUnavailable
	}
	if _, err := readBlockInventory(store, txn.Blob()); err == nil {
		return nil
	} else if !errors.Is(err, types.ErrBlobKeyNotFound) {
		return fmt.Errorf("read block inventory: %w", err)
	}
	if err := clearBlockInventoryIndex(store, txn.Blob()); err != nil {
		return fmt.Errorf("clear block inventory index: %w", err)
	}
	inventory, err := scanBlockInventory(store, txn.Blob())
	if err != nil {
		return fmt.Errorf("initialize block inventory: %w", err)
	}
	if err := writeBlockInventory(store, txn.Blob(), inventory); err != nil {
		return fmt.Errorf("write block inventory: %w", err)
	}
	if err := txn.Commit(); err != nil {
		return fmt.Errorf("commit block inventory: %w", err)
	}
	if err := store.Sync(); err != nil {
		return fmt.Errorf("sync block inventory: %w", err)
	}
	return nil
}

func (d *Database) blockInventory(
	txn *Txn,
) (count uint64, oldestSlot uint64, err error) {
	if txn == nil {
		txn = d.BlobTxn(false)
		defer txn.Rollback() //nolint:errcheck
	}
	store := txn.BlobStore()
	if store == nil || txn.Blob() == nil {
		return 0, 0, types.ErrBlobStoreUnavailable
	}
	inventory, err := readBlockInventory(store, txn.Blob())
	if err != nil {
		return 0, 0, fmt.Errorf("read block inventory: %w", err)
	}
	return inventory.Count, inventory.OldestSlot, nil
}

func addRetainedBlock(
	store blob.BlobStore,
	txn *Txn,
	slot uint64,
	hash []byte,
) error {
	inventory, err := readBlockInventory(store, txn.Blob())
	if err != nil {
		return fmt.Errorf("read block inventory: %w", err)
	}
	if inventory.Count == 0 || slot < inventory.OldestSlot {
		inventory.OldestSlot = slot
	}
	if inventory.Count == math.MaxUint64 {
		return errors.New("block inventory count overflow")
	}
	indexKey, err := blockInventoryIndexKey(slot, hash)
	if err != nil {
		return err
	}
	if err := store.Set(txn.Blob(), indexKey, nil); err != nil {
		return fmt.Errorf("index retained block: %w", err)
	}
	if txn.blockInventoryAdded == nil {
		txn.blockInventoryAdded = make(map[string]uint64)
	}
	txn.blockInventoryAdded[string(indexKey)] = slot
	inventory.Count++
	return writeBlockInventory(store, txn.Blob(), inventory)
}

func removeRetainedBlock(
	store blob.BlobStore,
	txn *Txn,
	slot uint64,
	hash []byte,
) error {
	inventory, err := readBlockInventory(store, txn.Blob())
	if err != nil {
		return fmt.Errorf("read block inventory: %w", err)
	}
	if inventory.Count == 0 {
		return errors.New("block inventory underflow")
	}
	indexKey, err := blockInventoryIndexKey(slot, hash)
	if err != nil {
		return err
	}
	if err := store.Delete(txn.Blob(), indexKey); err != nil {
		return fmt.Errorf("remove retained block index: %w", err)
	}
	delete(txn.blockInventoryAdded, string(indexKey))
	inventory.Count--
	if inventory.Count == 0 {
		inventory.OldestSlot = 0
	} else if slot == inventory.OldestSlot {
		oldest, err := oldestIndexedBlockSlot(store, txn)
		if err != nil {
			return err
		}
		inventory.OldestSlot = oldest
	}
	return writeBlockInventory(store, txn.Blob(), inventory)
}
