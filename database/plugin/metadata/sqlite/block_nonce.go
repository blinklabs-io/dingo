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

package sqlite

import (
	"errors"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"gorm.io/gorm"
)

// SetBlockNonce inserts a block nonce into the block_nonce table
func (d *MetadataStoreSqlite) SetBlockNonce(
	blockHash []byte,
	slotNumber uint64,
	nonce []byte,
	isCheckpoint bool,
	txn types.Txn,
) error {
	item := models.BlockNonce{
		Hash:         blockHash,
		Slot:         slotNumber,
		Nonce:        nonce,
		IsCheckpoint: isCheckpoint,
	}

	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	result := db.Create(&item)

	if result.Error != nil {
		return result.Error
	}

	return nil
}

// GetBlockNonce retrieves the block nonce for a specific block
func (d *MetadataStoreSqlite) GetBlockNonce(
	point ocommon.Point,
	txn types.Txn,
) ([]byte, error) {
	ret := models.BlockNonce{}
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	result := db.Where("hash = ? AND slot = ?", point.Hash, point.Slot).
		First(&ret)
	if result.Error != nil {
		if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, result.Error
		}
		return nil, nil // Record not found
	}
	return ret.Nonce, nil
}

// DeleteBlockNoncesBeforeSlot deletes block_nonce records with slot less than the specified value
func (d *MetadataStoreSqlite) DeleteBlockNoncesBeforeSlot(
	slotNumber uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	result := db.
		Where("slot < ?", slotNumber).
		Delete(&models.BlockNonce{})

	if result.Error != nil {
		return result.Error
	}

	return nil
}

// DeleteBlockNoncesBeforeSlotWithoutCheckpoints deletes block_nonce records with slot < given value AND is_checkpoint = false
func (d *MetadataStoreSqlite) DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
	slotNumber uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	result := db.
		Where("slot < ? AND is_checkpoint = ?", slotNumber, false).
		Delete(&models.BlockNonce{})

	return result.Error
}
