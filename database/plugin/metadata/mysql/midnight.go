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

package mysql

import (
	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
)

// CreateMidnightAssetCreate inserts a cNIGHT UTxO creation row.
func (d *MetadataStoreMysql) CreateMidnightAssetCreate(
	txn types.Txn,
	row *models.MidnightAssetCreate,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Create(row).Error
}

// CreateMidnightAssetSpend inserts a cNIGHT UTxO spend row.
func (d *MetadataStoreMysql) CreateMidnightAssetSpend(
	txn types.Txn,
	row *models.MidnightAssetSpend,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Create(row).Error
}

// CreateMidnightRegistration inserts a mapping-validator registration row.
func (d *MetadataStoreMysql) CreateMidnightRegistration(
	txn types.Txn,
	row *models.MidnightRegistration,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Create(row).Error
}

// CreateMidnightDeregistration inserts a mapping-validator deregistration row.
func (d *MetadataStoreMysql) CreateMidnightDeregistration(
	txn types.Txn,
	row *models.MidnightDeregistration,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Create(row).Error
}

// FindUnspentMidnightAssetCreates returns cNIGHT create rows that have no
// matching spend row, used to restore the in-memory tracked-UTxO set on startup.
func (d *MetadataStoreMysql) FindUnspentMidnightAssetCreates() (
	[]models.MidnightAssetCreate,
	error,
) {
	var rows []models.MidnightAssetCreate
	err := d.DB().
		Where(
			"NOT EXISTS (SELECT 1 FROM midnight_asset_spends" +
				" WHERE utxo_tx_hash = midnight_asset_creates.tx_hash" +
				" AND utxo_index = midnight_asset_creates.output_index)",
		).
		Find(&rows).Error
	return rows, err
}

// FindUnspentMidnightRegistrations returns registration rows that have no
// matching deregistration row, used to restore the in-memory tracked-UTxO set on startup.
func (d *MetadataStoreMysql) FindUnspentMidnightRegistrations() (
	[]models.MidnightRegistration,
	error,
) {
	var rows []models.MidnightRegistration
	err := d.DB().
		Where(
			"NOT EXISTS (SELECT 1 FROM midnight_deregistrations" +
				" WHERE utxo_tx_hash = midnight_registrations.tx_hash" +
				" AND utxo_index = midnight_registrations.output_index)",
		).
		Find(&rows).Error
	return rows, err
}

// DeleteMidnightAssetCreatesByBlock deletes all cNIGHT create rows for the
// given block number and returns them for in-memory state reconciliation.
func (d *MetadataStoreMysql) DeleteMidnightAssetCreatesByBlock(
	txn types.Txn,
	blockNumber uint64,
) ([]models.MidnightAssetCreate, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	var rows []models.MidnightAssetCreate
	if err := db.Where("block_number = ?", blockNumber).Find(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows, db.Where("block_number = ?", blockNumber).
		Delete(&models.MidnightAssetCreate{}).Error
}

// DeleteMidnightAssetSpendsByBlock deletes all cNIGHT spend rows for the
// given block number and returns them for in-memory state reconciliation.
func (d *MetadataStoreMysql) DeleteMidnightAssetSpendsByBlock(
	txn types.Txn,
	blockNumber uint64,
) ([]models.MidnightAssetSpend, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	var rows []models.MidnightAssetSpend
	if err := db.Where("block_number = ?", blockNumber).Find(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows, db.Where("block_number = ?", blockNumber).
		Delete(&models.MidnightAssetSpend{}).Error
}

// DeleteMidnightRegistrationsByBlock deletes all registration rows for the
// given block number and returns them for in-memory state reconciliation.
func (d *MetadataStoreMysql) DeleteMidnightRegistrationsByBlock(
	txn types.Txn,
	blockNumber uint64,
) ([]models.MidnightRegistration, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	var rows []models.MidnightRegistration
	if err := db.Where("block_number = ?", blockNumber).Find(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows, db.Where("block_number = ?", blockNumber).
		Delete(&models.MidnightRegistration{}).Error
}

// DeleteMidnightDeregistrationsByBlock deletes all deregistration rows for the
// given block number and returns them for in-memory state reconciliation.
func (d *MetadataStoreMysql) DeleteMidnightDeregistrationsByBlock(
	txn types.Txn,
	blockNumber uint64,
) ([]models.MidnightDeregistration, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	var rows []models.MidnightDeregistration
	if err := db.Where("block_number = ?", blockNumber).Find(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return rows, db.Where("block_number = ?", blockNumber).
		Delete(&models.MidnightDeregistration{}).Error
}
