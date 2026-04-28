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

package postgres

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	ocommon "github.com/blinklabs-io/gouroboros/protocol/common"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SetBlockNonce inserts or updates a block nonce. The (hash, slot)
// uniqueIndex makes this safe to call repeatedly for the same block,
// which happens when the metadata backfill resumes from a checkpoint
// that pre-dates a previously-written nonce row.
func (d *MetadataStorePostgres) SetBlockNonce(
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
	// is_checkpoint is updated with an OR so a later upsert with
	// false cannot demote a previously-promoted checkpoint row.
	result := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "hash"},
			{Name: "slot"},
		},
		DoUpdates: clause.Set{
			{
				Column: clause.Column{Name: "nonce"},
				Value:  gorm.Expr("excluded.nonce"),
			},
			{
				Column: clause.Column{Name: "is_checkpoint"},
				Value: gorm.Expr(
					"block_nonce.is_checkpoint OR excluded.is_checkpoint",
				),
			},
		},
	}).Create(&item)

	if result.Error != nil {
		return result.Error
	}

	return nil
}

// GetBlockNonce retrieves the block nonce for a specific block
func (d *MetadataStorePostgres) GetBlockNonce(
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

// GetBlockNoncesInSlotRange retrieves all block nonces where slot >= startSlot and slot < endSlot.
func (d *MetadataStorePostgres) GetBlockNoncesInSlotRange(
	startSlot uint64,
	endSlot uint64,
	txn types.Txn,
) ([]models.BlockNonce, error) {
	var results []models.BlockNonce
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, fmt.Errorf("resolveDB failed for slot range [%d,%d): %w", startSlot, endSlot, err)
	}
	result := db.
		Where("slot >= ? AND slot < ?", startSlot, endSlot).
		Order("slot ASC").
		Find(&results)
	if result.Error != nil {
		return nil, fmt.Errorf("query failed for slot range [%d,%d): %w", startSlot, endSlot, result.Error)
	}
	return results, nil
}

// GetLastBlockNonceInRange retrieves the block nonce with the highest slot
// in [startSlot, endSlot). Returns nil nonce and no error if none found.
func (d *MetadataStorePostgres) GetLastBlockNonceInRange(
	startSlot uint64,
	endSlot uint64,
	txn types.Txn,
) ([]byte, error) {
	ret := models.BlockNonce{}
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"resolveDB for slot range [%d,%d): %w",
			startSlot, endSlot, err,
		)
	}
	result := db.
		Where("slot >= ? AND slot < ?", startSlot, endSlot).
		Order("slot DESC, hash DESC").
		Limit(1).
		First(&ret)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf(
			"query block nonce in slot range [%d,%d): %w",
			startSlot, endSlot, result.Error,
		)
	}
	return ret.Nonce, nil
}

// DeleteBlockNoncesBeforeSlot deletes block_nonce records with slot less than the specified value
func (d *MetadataStorePostgres) DeleteBlockNoncesBeforeSlot(
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
func (d *MetadataStorePostgres) DeleteBlockNoncesBeforeSlotWithoutCheckpoints(
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
