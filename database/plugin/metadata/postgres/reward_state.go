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

//go:build dingo_extra_plugins

package postgres

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// SaveRewardAdaPots saves reward-related ADA pots for an epoch.
func (d *MetadataStorePostgres) SaveRewardAdaPots(
	pots *models.RewardAdaPots,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	if err := db.Clauses(
		clause.OnConflict{
			Columns: []clause.Column{{Name: "epoch"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"treasury",
				"reserves",
				"fees",
				"rewards",
				"captured_slot",
			}),
		},
	).Create(pots).Error; err != nil {
		return fmt.Errorf("save reward ADA pots: %w", err)
	}
	return nil
}

// GetRewardAdaPots retrieves reward-related ADA pots for an epoch.
func (d *MetadataStorePostgres) GetRewardAdaPots(
	epoch uint64,
	txn types.Txn,
) (*models.RewardAdaPots, error) {
	var pots models.RewardAdaPots
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	result := db.Where("epoch = ?", epoch).First(&pots)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &pots, nil
}

// SaveRewardSnapshot saves reward snapshot metadata for an epoch.
func (d *MetadataStorePostgres) SaveRewardSnapshot(
	snapshot *models.RewardSnapshot,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	if err := db.Clauses(
		clause.OnConflict{
			Columns: []clause.Column{
				{Name: "epoch"},
				{Name: "snapshot_type"},
			},
			DoUpdates: clause.AssignmentColumns([]string{
				"total_active_stake",
				"total_pool_count",
				"total_delegators",
				"captured_slot",
				"boundary_slot",
				"epoch_nonce",
				"protocol_version",
			}),
		},
	).Create(snapshot).Error; err != nil {
		return fmt.Errorf("save reward snapshot: %w", err)
	}
	return nil
}

// GetRewardSnapshot retrieves reward snapshot metadata for an epoch.
func (d *MetadataStorePostgres) GetRewardSnapshot(
	epoch uint64,
	snapshotType string,
	txn types.Txn,
) (*models.RewardSnapshot, error) {
	var snapshot models.RewardSnapshot
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	result := db.Where(
		"epoch = ? AND snapshot_type = ?",
		epoch,
		snapshotType,
	).First(&snapshot)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &snapshot, nil
}

// SaveRewardPoolInputs saves per-pool reward inputs for an epoch.
func (d *MetadataStorePostgres) SaveRewardPoolInputs(
	inputs []*models.RewardPoolInput,
	txn types.Txn,
) error {
	if len(inputs) == 0 {
		return nil
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	if err := db.Clauses(
		clause.OnConflict{
			Columns: []clause.Column{
				{Name: "epoch"},
				{Name: "pool_key_hash"},
			},
			DoUpdates: clause.AssignmentColumns([]string{
				"blocks_produced",
				"total_blocks_in_epoch",
				"pledge",
				"delegated_stake",
				"cost",
				"margin",
				"delegator_count",
				"captured_slot",
				"boundary_slot",
			}),
		},
	).Create(inputs).Error; err != nil {
		return fmt.Errorf("save reward pool inputs: %w", err)
	}
	return nil
}

// GetRewardPoolInputs retrieves all per-pool reward inputs for an epoch.
func (d *MetadataStorePostgres) GetRewardPoolInputs(
	epoch uint64,
	txn types.Txn,
) ([]*models.RewardPoolInput, error) {
	var inputs []*models.RewardPoolInput
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	result := db.Where("epoch = ?", epoch).
		Order("pool_key_hash ASC").
		Find(&inputs)
	if result.Error != nil {
		return nil, result.Error
	}
	return inputs, nil
}

// DeleteRewardStateAfterSlot deletes reward-state rows captured from
// rolled-back blocks.
func (d *MetadataStorePostgres) DeleteRewardStateAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("delete reward state after slot: resolve db: %w", err)
	}

	deleteRows := func(tx *gorm.DB) error {
		if err := tx.Where(
			"captured_slot > ?",
			slot,
		).Delete(&models.RewardAdaPots{}).Error; err != nil {
			return fmt.Errorf("delete reward ADA pots after slot: %w", err)
		}
		if err := tx.Where(
			"captured_slot > ? OR boundary_slot > ?",
			slot,
			slot,
		).Delete(&models.RewardSnapshot{}).Error; err != nil {
			return fmt.Errorf("delete reward snapshots after slot: %w", err)
		}
		if err := tx.Where(
			"captured_slot > ? OR boundary_slot > ?",
			slot,
			slot,
		).Delete(&models.RewardPoolInput{}).Error; err != nil {
			return fmt.Errorf("delete reward pool inputs after slot: %w", err)
		}
		return nil
	}

	if txn != nil {
		return deleteRows(db)
	}
	return db.Transaction(deleteRows)
}

// DeleteRewardStateBeforeEpoch deletes reward-state rows older than the
// retained snapshot window.
func (d *MetadataStorePostgres) DeleteRewardStateBeforeEpoch(
	epoch uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("delete reward state before epoch: resolve db: %w", err)
	}

	deleteRows := func(tx *gorm.DB) error {
		if err := tx.Where("epoch < ?", epoch).
			Delete(&models.RewardAdaPots{}).Error; err != nil {
			return fmt.Errorf("delete reward ADA pots before epoch: %w", err)
		}
		if err := tx.Where("epoch < ?", epoch).
			Delete(&models.RewardSnapshot{}).Error; err != nil {
			return fmt.Errorf("delete reward snapshots before epoch: %w", err)
		}
		if err := tx.Where("epoch < ?", epoch).
			Delete(&models.RewardPoolInput{}).Error; err != nil {
			return fmt.Errorf("delete reward pool inputs before epoch: %w", err)
		}
		return nil
	}

	if txn != nil {
		return deleteRows(db)
	}
	return db.Transaction(deleteRows)
}
