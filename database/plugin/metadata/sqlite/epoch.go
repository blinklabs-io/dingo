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
	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/models"
	"gorm.io/gorm"
)

// GetEpochLatest returns the latest epoch
func (d *MetadataStoreSqlite) GetEpochLatest(
	txn *gorm.DB,
) (models.Epoch, error) {
	ret := models.Epoch{}
	// Create table if it doesn't exist
	if err := d.DB().AutoMigrate(&models.Epoch{}); err != nil {
		return ret, err
	}
	if txn != nil {
		result := txn.Order("epoch_id DESC").First(&ret)
		if result.Error != nil {
			return ret, result.Error
		}
	} else {
		result := d.DB().Order("epoch_id DESC").First(&ret)
		if result.Error != nil {
			return ret, result.Error
		}
	}
	return ret, nil
}

// SetEpoch saves an epoch
func (d *MetadataStoreSqlite) SetEpoch(
	slot, epoch uint64,
	nonce []byte,
	era, slotLength, lengthInSlots uint,
	txn *gorm.DB,
) error {
	// Create table if it doesn't exist
	if err := d.DB().AutoMigrate(&models.Epoch{}); err != nil {
		return err
	}
	tmpItem := models.Epoch{
		EpochId:       epoch,
		StartSlot:     slot,
		Nonce:         nonce,
		EraId:         era,
		SlotLength:    slotLength,
		LengthInSlots: lengthInSlots,
	}
	if txn != nil {
		if result := txn.Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	} else {
		if result := d.DB().Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	}
	return nil
}
