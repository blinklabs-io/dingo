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
	"fmt"
	"strconv"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GetEpoch returns a single epoch by its ID, or nil if not found.
func (d *MetadataStoreSqlite) GetEpoch(
	epochId uint64,
	txn types.Txn,
) (*models.Epoch, error) {
	var ret models.Epoch
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetEpoch: resolve db: %w", err,
		)
	}
	result := db.Where("epoch_id = ?", epochId).First(&ret)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf(
			"GetEpoch: query: %w", result.Error,
		)
	}
	return &ret, nil
}

// GetEpochsByEra returns the list of epochs by era
func (d *MetadataStoreSqlite) GetEpochsByEra(
	eraId uint,
	txn types.Txn,
) ([]models.Epoch, error) {
	var ret []models.Epoch
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetEpochsByEra: resolve db: %w", err,
		)
	}
	result := db.Where("era_id = ?", eraId).Order("epoch_id").Find(&ret)
	if result.Error != nil {
		return nil, fmt.Errorf(
			"GetEpochsByEra: query: %w", result.Error,
		)
	}
	return ret, nil
}

// GetEpochs returns the list of epochs
func (d *MetadataStoreSqlite) GetEpochs(
	txn types.Txn,
) ([]models.Epoch, error) {
	var ret []models.Epoch
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetEpochs: resolve db: %w", err,
		)
	}
	result := db.Order("epoch_id").Find(&ret)
	if result.Error != nil {
		return nil, fmt.Errorf(
			"GetEpochs: query: %w", result.Error,
		)
	}
	return ret, nil
}

// DeleteEpochsAfterSlot removes all epoch entries whose start slot
// is after the given slot.
func (d *MetadataStoreSqlite) DeleteEpochsAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf(
			"DeleteEpochsAfterSlot: resolve db: %w", err,
		)
	}
	result := db.Where("start_slot > ?", slot).Delete(&models.Epoch{})
	if result.Error != nil {
		return fmt.Errorf(
			"DeleteEpochsAfterSlot: delete: %w",
			result.Error,
		)
	}
	return nil
}

// SetEpoch saves an epoch
func (d *MetadataStoreSqlite) SetEpoch(
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
		return fmt.Errorf("SetEpoch: resolve db: %w", err)
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
		return fmt.Errorf(
			"SetEpoch: create epoch: %w",
			result.Error,
		)
	}
	// Run a vacuum only when not in a transaction, on error only log
	// (VACUUM during transaction causes lock contention)
	if txn == nil {
		if err := d.runVacuum(); err != nil {
			d.logger.Warn(
				"failed to free space in metadata store",
				"epoch", strconv.FormatUint(epoch, 10),
				"error", err,
			)
		}
	}
	return nil
}
