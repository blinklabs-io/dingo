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
	"fmt"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/models"
	"gorm.io/gorm"
)

// SetBlockNonce inserts a block nonce into the block_nonce table
// SetBlockNonce inserts a block nonce into the block_nonce table
func (d *MetadataStoreSqlite) SetBlockNonce(
	blockHash string,
	slotNumber uint64,
	nonce []byte,
	isCheckpoint bool,
	txn *gorm.DB,
) error {
	item := models.BlockNonce{
		BlockHash:    blockHash,
		SlotNumber:   slotNumber,
		Nonce:        nonce,
		IsCheckpoint: isCheckpoint,
	}

	var result *gorm.DB
	if txn != nil {
		result = txn.Create(&item)
	} else {
		result = d.DB().Create(&item)
	}

	if result.Error != nil {
		return result.Error
	}

	// Optional: same pattern as SetEpoch
	if err := d.runVacuum(); err != nil {
		d.logger.Error(
			"failed to vacuum after SetBlockNonce",
			"slot", fmt.Sprintf("%d", slotNumber),
			"error", err,
		)
	}

	return nil
}

// DeleteBlockNoncesBeforeSlot deletes block_nonce records with slot_number less than the specified value
func (d *MetadataStoreSqlite) DeleteBlockNoncesBeforeSlot(
	slotNumber uint64,
	txn *gorm.DB,
) error {
	var result *gorm.DB
	if txn != nil {
		result = txn.
			Where("slot_number < ?", slotNumber).
			Delete(&models.BlockNonce{})
	} else {
		result = d.DB().
			Where("slot_number < ?", slotNumber).
			Delete(&models.BlockNonce{})
	}

	if result.Error != nil {
		return result.Error
	}

	return nil
}

// DeleteBlockNoncesBeforeSlotWithoutCheckpoints deletes block_nonce records with slot_number < given value AND is_checkpoint = false
func (d *MetadataStoreSqlite) DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
	slotNumber uint64,
	txn *gorm.DB,
) error {
	db := txn
	if db == nil {
		db = d.DB()
	}
	result := db.
		Where("slot_number < ? AND is_checkpoint = ?", slotNumber, false).
		Delete(&models.BlockNonce{})

	return result.Error
}
