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

package rewardstate

import (
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// rewardSaveBatchSize bounds the rows per multi-row INSERT for reward pool
// inputs. A single Create binds rows*columns parameters; the
// widest reward row (RewardPoolInput, ~14 columns) at full delegator/account
// scale (~1M rows on mainnet) would bind millions of parameters and exceed
// every backend's bind limit (SQLite 32766, Postgres/MySQL 65535), rolling
// back the epoch reward transaction. 1000 rows * ~14 columns = ~14000 stays
// well under all three, matching the existing importAssetBatchSize precedent.
const rewardSaveBatchSize = 1000

// SaveAdaPots saves reward-related ADA pots for an epoch.
func SaveAdaPots(db *gorm.DB, pots *models.RewardAdaPots) error {
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

// GetAdaPots retrieves reward-related ADA pots for an epoch.
func GetAdaPots(
	db *gorm.DB,
	epoch uint64,
) (*models.RewardAdaPots, error) {
	var pots models.RewardAdaPots
	result := db.Where("epoch = ?", epoch).First(&pots)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &pots, nil
}

// SaveSnapshot saves reward snapshot metadata for an epoch.
func SaveSnapshot(db *gorm.DB, snapshot *models.RewardSnapshot) error {
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

// GetSnapshot retrieves reward snapshot metadata for an epoch.
func GetSnapshot(
	db *gorm.DB,
	epoch uint64,
	snapshotType string,
) (*models.RewardSnapshot, error) {
	var snapshot models.RewardSnapshot
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

// SavePoolInputs saves per-pool reward inputs for an epoch.
func SavePoolInputs(db *gorm.DB, inputs []*models.RewardPoolInput) error {
	if len(inputs) == 0 {
		return nil
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
				"owner_stake",
				"cost",
				"margin",
				"reward_account",
				"reward_account_credential_tag",
				"delegator_count",
				"captured_slot",
				"boundary_slot",
			}),
		},
	).CreateInBatches(inputs, rewardSaveBatchSize).Error; err != nil {
		return fmt.Errorf("save reward pool inputs: %w", err)
	}
	return nil
}

// GetPoolInputs retrieves all per-pool reward inputs for an epoch.
func GetPoolInputs(
	db *gorm.DB,
	epoch uint64,
) ([]*models.RewardPoolInput, error) {
	var inputs []*models.RewardPoolInput
	result := db.Where("epoch = ?", epoch).
		Order("pool_key_hash ASC").
		Find(&inputs)
	if result.Error != nil {
		return nil, result.Error
	}
	return inputs, nil
}

// DeleteStateAfterSlot deletes reward-state rows captured from rolled-back
// blocks. When txn is non-nil, db is used as-is; otherwise the deletes are
// wrapped in their own transaction.
func DeleteStateAfterSlot(
	db *gorm.DB,
	slot uint64,
	txn types.Txn,
) error {
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

// DeleteStateBeforeEpoch deletes reward-state rows older than the retained
// snapshot window. When txn is non-nil, db is used as-is; otherwise the
// deletes are wrapped in their own transaction.
func DeleteStateBeforeEpoch(
	db *gorm.DB,
	epoch uint64,
	txn types.Txn,
) error {
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
