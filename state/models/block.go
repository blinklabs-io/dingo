// Copyright 2024 Blink Labs Software
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

	"github.com/blinklabs-io/dingo/database"

	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/blinklabs-io/gouroboros/ledger"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"github.com/dgraph-io/badger/v4"
)

type Block struct {
	ID     uint   `gorm:"primarykey"`
	Slot   uint64 `gorm:"index:slot_hash"`
	Number uint64 `gorm:"index"`
	Hash   []byte `gorm:"index:slot_hash"`
	Type   uint
	Nonce  []byte
	Cbor   []byte `gorm:"-"` // This is here for convenience but not represented in the metadata DB
}

func (Block) TableName() string {
	return "block"
}

func (b Block) Decode() (ledger.Block, error) {
	return ledger.NewBlockFromCbor(b.Type, b.Cbor)
}

func (b *Block) loadCbor(txn *database.Txn) error {
	key := BlockBlobKey(b.Slot, b.Hash)
	item, err := txn.Blob().Get(key)
	if err != nil {
		return err
	}
	b.Cbor, err = item.ValueCopy(nil)
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
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

func BlockByPointTxn(txn *database.Txn, point ocommon.Point) (Block, error) {
	var tmpBlock Block
	result := txn.Metadata().
		First(&tmpBlock, "slot = ? AND hash = ?", point.Slot, point.Hash)
	if result.Error != nil {
		return tmpBlock, result.Error
	}
	if err := tmpBlock.loadCbor(txn); err != nil {
		return tmpBlock, err
	}
	return tmpBlock, nil
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
	var tmpBlock Block
	result := txn.Metadata().First(&tmpBlock, "number = ?", blockNumber)
	if result.Error != nil {
		return tmpBlock, result.Error
	}
	if err := tmpBlock.loadCbor(txn); err != nil {
		return tmpBlock, err
	}
	return tmpBlock, nil
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
	var tmpBlock Block
	result := txn.Metadata().Order("id DESC").Where("slot < ?", slotNumber).First(&tmpBlock)
	if result.Error != nil {
		return tmpBlock, result.Error
	}
	if err := tmpBlock.loadCbor(txn); err != nil {
		return tmpBlock, err
	}
	return tmpBlock, nil
}

func BlockBlobKey(slot uint64, hash []byte) []byte {
	key := []byte("b")
	// Convert slot to bytes
	slotBytes := make([]byte, 8)
	new(big.Int).SetUint64(slot).FillBytes(slotBytes)
	key = append(key, slotBytes...)
	key = append(key, hash...)
	return key
}

func BlockBlobMetadataKey(baseKey []byte) []byte {
	return slices.Concat(baseKey, []byte("_metadata"))
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
	Type   uint
	Height uint64
}
