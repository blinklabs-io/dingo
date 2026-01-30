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

package mysql

import (
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm/clause"
)

// GetEpochsByEra returns the list of epochs by era
func (d *MetadataStoreMysql) GetEpochsByEra(
	eraId uint,
	txn types.Txn,
) ([]models.Epoch, error) {
	var ret []models.Epoch
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	result := db.Where("era_id = ?", eraId).Order("epoch_id").Find(&ret)
	if result.Error != nil {
		return nil, result.Error
	}
	return ret, nil
}

// GetEpochs returns the list of epochs
func (d *MetadataStoreMysql) GetEpochs(
	txn types.Txn,
) ([]models.Epoch, error) {
	var ret []models.Epoch
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	result := db.Order("epoch_id").Find(&ret)
	if result.Error != nil {
		return nil, result.Error
	}
	return ret, nil
}

// SetEpoch saves an epoch
func (d *MetadataStoreMysql) SetEpoch(
	slot, epoch uint64,
	nonce []byte,
	era, slotLength, lengthInSlots uint,
	txn types.Txn,
) error {
	tmpItem := models.Epoch{
		EpochId:       epoch,
		StartSlot:     slot,
		Nonce:         nonce,
		EraId:         era,
		SlotLength:    slotLength,
		LengthInSlots: lengthInSlots,
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	if result := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "epoch_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"start_slot",
			"nonce",
			"era_id",
			"slot_length",
			"length_in_slots",
		}),
	}).Create(&tmpItem); result.Error != nil {
		return result.Error
	}
	return nil
}
