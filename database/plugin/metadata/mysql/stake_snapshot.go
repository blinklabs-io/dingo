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
	"errors"
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Pool Stake Snapshot Operations

// SavePoolStakeSnapshot saves a pool stake snapshot
func (d *MetadataStoreMysql) SavePoolStakeSnapshot(
	snapshot *models.PoolStakeSnapshot,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Create(snapshot).Error
}

// SavePoolStakeSnapshots saves multiple pool stake snapshots in batch
func (d *MetadataStoreMysql) SavePoolStakeSnapshots(
	snapshots []*models.PoolStakeSnapshot,
	txn types.Txn,
) error {
	if len(snapshots) == 0 {
		return nil
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(snapshots).Error
}

// GetPoolStakeSnapshot retrieves a specific pool's stake snapshot
func (d *MetadataStoreMysql) GetPoolStakeSnapshot(
	epoch uint64,
	snapshotType string,
	poolKeyHash []byte,
	txn types.Txn,
) (*models.PoolStakeSnapshot, error) {
	var snapshot models.PoolStakeSnapshot
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	result := db.Where(
		"epoch = ? AND snapshot_type = ? AND pool_key_hash = ?",
		epoch, snapshotType, poolKeyHash,
	).First(&snapshot)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &snapshot, nil
}

// GetPoolStakeSnapshotsByEpoch retrieves all pool stake snapshots for an epoch
func (d *MetadataStoreMysql) GetPoolStakeSnapshotsByEpoch(
	epoch uint64,
	snapshotType string,
	txn types.Txn,
) ([]*models.PoolStakeSnapshot, error) {
	var snapshots []*models.PoolStakeSnapshot
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	result := db.Where(
		"epoch = ? AND snapshot_type = ?",
		epoch, snapshotType,
	).Find(&snapshots)
	if result.Error != nil {
		return nil, result.Error
	}
	return snapshots, nil
}

// GetTotalActiveStake returns the total active stake for an epoch.
// It uses the pre-calculated total from EpochSummary when available,
// falling back to summing individual pool snapshots.
func (d *MetadataStoreMysql) GetTotalActiveStake(
	epoch uint64,
	snapshotType string,
	txn types.Txn,
) (uint64, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return 0, err
	}
	// Prefer the pre-calculated total from EpochSummary to avoid
	// issues with duplicate snapshot rows inflating the sum.
	// EpochSummary stores the "mark" snapshot total (the only type
	// physically saved), so only use the fast path for "mark" queries.
	if snapshotType == "mark" {
		var summary models.EpochSummary
		result := db.Where("epoch = ?", epoch).First(&summary)
		if result.Error != nil && !errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return 0, fmt.Errorf(
				"GetTotalActiveStake: query epoch summary for epoch %d: %w",
				epoch,
				result.Error,
			)
		}
		if result.Error == nil && summary.SnapshotReady {
			return uint64(summary.TotalActiveStake), nil
		}
	}
	// Fall back to summing individual pool snapshots
	var total uint64
	result := db.Model(&models.PoolStakeSnapshot{}).
		Where("epoch = ? AND snapshot_type = ?", epoch, snapshotType).
		Select("COALESCE(SUM(CAST(total_stake AS UNSIGNED)), 0)").
		Scan(&total)
	if result.Error != nil {
		return 0, fmt.Errorf(
			"GetTotalActiveStake: aggregate pool snapshots for epoch %d: %w",
			epoch,
			result.Error,
		)
	}
	return total, nil
}

// Epoch Summary Operations

// SaveEpochSummary saves an epoch summary
func (d *MetadataStoreMysql) SaveEpochSummary(
	summary *models.EpochSummary,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(summary).Error
}

// GetEpochSummary retrieves an epoch summary by epoch number
func (d *MetadataStoreMysql) GetEpochSummary(
	epoch uint64,
	txn types.Txn,
) (*models.EpochSummary, error) {
	var summary models.EpochSummary
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	result := db.Where("epoch = ?", epoch).First(&summary)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &summary, nil
}

// GetLatestEpochSummary retrieves the most recent epoch summary
func (d *MetadataStoreMysql) GetLatestEpochSummary(
	txn types.Txn,
) (*models.EpochSummary, error) {
	var summary models.EpochSummary
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	result := db.Order("epoch DESC").First(&summary)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &summary, nil
}

// Deletion Operations for Rollback Support

// DeletePoolStakeSnapshotsForEpoch deletes all pool stake snapshots for a specific epoch
func (d *MetadataStoreMysql) DeletePoolStakeSnapshotsForEpoch(
	epoch uint64,
	snapshotType string,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Where(
		"epoch = ? AND snapshot_type = ?",
		epoch, snapshotType,
	).Delete(&models.PoolStakeSnapshot{}).Error
}

// DeletePoolStakeSnapshotsAfterEpoch deletes all pool stake snapshots after a given epoch.
// This is used during chain rollbacks.
func (d *MetadataStoreMysql) DeletePoolStakeSnapshotsAfterEpoch(
	epoch uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf(
			"DeletePoolStakeSnapshotsAfterEpoch: resolve db: %w",
			err,
		)
	}
	if err := db.Where("epoch > ?", epoch).Delete(&models.PoolStakeSnapshot{}).Error; err != nil {
		return fmt.Errorf(
			"DeletePoolStakeSnapshotsAfterEpoch: failed to delete snapshots after epoch %d: %w",
			epoch,
			err,
		)
	}
	return nil
}

// DeletePoolStakeSnapshotsBeforeEpoch deletes all pool stake snapshots before a given epoch.
func (d *MetadataStoreMysql) DeletePoolStakeSnapshotsBeforeEpoch(
	epoch uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf(
			"DeletePoolStakeSnapshotsBeforeEpoch: resolve db: %w",
			err,
		)
	}
	if err := db.Where("epoch < ?", epoch).Delete(&models.PoolStakeSnapshot{}).Error; err != nil {
		return fmt.Errorf(
			"DeletePoolStakeSnapshotsBeforeEpoch: failed to delete snapshots before epoch %d: %w",
			epoch,
			err,
		)
	}
	return nil
}

// DeleteEpochSummariesAfterEpoch deletes all epoch summaries after a given epoch.
// This is used during chain rollbacks.
func (d *MetadataStoreMysql) DeleteEpochSummariesAfterEpoch(
	epoch uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf(
			"DeleteEpochSummariesAfterEpoch: resolve db: %w",
			err,
		)
	}
	if err := db.Where("epoch > ?", epoch).Delete(&models.EpochSummary{}).Error; err != nil {
		return fmt.Errorf(
			"DeleteEpochSummariesAfterEpoch: failed to delete summaries after epoch %d: %w",
			epoch,
			err,
		)
	}
	return nil
}

// DeleteEpochSummariesBeforeEpoch deletes all epoch summaries before a given epoch.
func (d *MetadataStoreMysql) DeleteEpochSummariesBeforeEpoch(
	epoch uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf(
			"DeleteEpochSummariesBeforeEpoch: resolve db: %w",
			err,
		)
	}
	if err := db.Where("epoch < ?", epoch).Delete(&models.EpochSummary{}).Error; err != nil {
		return fmt.Errorf(
			"DeleteEpochSummariesBeforeEpoch: failed to delete summaries before epoch %d: %w",
			epoch,
			err,
		)
	}
	return nil
}
