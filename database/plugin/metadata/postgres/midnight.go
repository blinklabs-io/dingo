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

package postgres

import (
	"errors"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CreateMidnightAssetCreate inserts a cNIGHT UTxO creation row.
// Uses ON CONFLICT DO NOTHING so that backfill replays are idempotent.
func (d *MetadataStorePostgres) CreateMidnightAssetCreate(
	txn types.Txn,
	row *models.MidnightAssetCreate,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(row).Error
}

// CreateMidnightAssetSpend inserts a cNIGHT UTxO spend row.
// Uses ON CONFLICT DO NOTHING so that backfill replays are idempotent.
func (d *MetadataStorePostgres) CreateMidnightAssetSpend(
	txn types.Txn,
	row *models.MidnightAssetSpend,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(row).Error
}

// CreateMidnightRegistration inserts a mapping-validator registration row.
// Uses ON CONFLICT DO NOTHING so that backfill replays are idempotent.
func (d *MetadataStorePostgres) CreateMidnightRegistration(
	txn types.Txn,
	row *models.MidnightRegistration,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(row).Error
}

// CreateMidnightDeregistration inserts a mapping-validator deregistration row.
// Uses ON CONFLICT DO NOTHING so that backfill replays are idempotent.
func (d *MetadataStorePostgres) CreateMidnightDeregistration(
	txn types.Txn,
	row *models.MidnightDeregistration,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(row).Error
}

// FindUnspentMidnightAssetCreates returns cNIGHT create rows that have no
// matching spend row, used to restore the in-memory tracked-UTxO set on startup.
func (d *MetadataStorePostgres) FindUnspentMidnightAssetCreates() (
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
func (d *MetadataStorePostgres) FindUnspentMidnightRegistrations() (
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
func (d *MetadataStorePostgres) DeleteMidnightAssetCreatesByBlock(
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
	if err := db.Where("block_number = ?", blockNumber).Delete(&models.MidnightAssetCreate{}).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteMidnightAssetSpendsByBlock deletes all cNIGHT spend rows for the
// given block number and returns them for in-memory state reconciliation.
func (d *MetadataStorePostgres) DeleteMidnightAssetSpendsByBlock(
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
	if err := db.Where("block_number = ?", blockNumber).Delete(&models.MidnightAssetSpend{}).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteMidnightRegistrationsByBlock deletes all registration rows for the
// given block number and returns them for in-memory state reconciliation.
func (d *MetadataStorePostgres) DeleteMidnightRegistrationsByBlock(
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
	if err := db.Where("block_number = ?", blockNumber).Delete(&models.MidnightRegistration{}).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// DeleteMidnightDeregistrationsByBlock deletes all deregistration rows for the
// given block number and returns them for in-memory state reconciliation.
func (d *MetadataStorePostgres) DeleteMidnightDeregistrationsByBlock(
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
	if err := db.Where("block_number = ?", blockNumber).Delete(&models.MidnightDeregistration{}).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func (d *MetadataStorePostgres) GetMidnightCandidates(
	addr ledger.Address,
	txn types.Txn,
) ([]models.Utxo, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	addrQuery, err := addressWhereClause(db, addr)
	if err != nil {
		return nil, err
	}
	if addrQuery == nil {
		return nil, nil
	}
	type candidateRow struct {
		TxId      []byte `gorm:"column:tx_id"`
		Datum     []byte `gorm:"column:datum"`
		OutputIdx uint32 `gorm:"column:output_idx"`
	}
	var rows []candidateRow
	if err := db.Table("utxo").
		Select("utxo.tx_id, utxo.output_idx, datum.raw_datum AS datum").
		Joins("LEFT JOIN datum ON datum.hash = utxo.datum_hash").
		Where("utxo.deleted_slot = 0").
		Where(addrQuery).
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	ret := make([]models.Utxo, len(rows))
	for i, row := range rows {
		ret[i] = models.Utxo{
			TxId:      row.TxId,
			OutputIdx: row.OutputIdx,
			Datum:     row.Datum,
		}
	}
	return ret, nil
}

func (d *MetadataStorePostgres) InsertMidnightGovernanceDatum(
	datum *models.MidnightGovernanceDatum,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Create(datum).Error
}

func (d *MetadataStorePostgres) GetLatestMidnightGovernanceDatum(
	datumType string,
	blockNumber uint64,
	txn types.Txn,
) (*models.MidnightGovernanceDatum, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	var datum models.MidnightGovernanceDatum
	result := db.Where("datum_type = ? AND block_number <= ?", datumType, blockNumber).
		Order("block_number DESC, id DESC").First(&datum)
	if errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	return &datum, result.Error
}

func (d *MetadataStorePostgres) GetLatestMidnightAriadneParams(
	txn types.Txn,
) (*models.MidnightAriadneParams, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	var params models.MidnightAriadneParams
	if result := db.Order("epoch DESC").First(&params); result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &params, nil
}

func (d *MetadataStorePostgres) UpsertMidnightAriadneParams(
	params *models.MidnightAriadneParams,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "epoch"}},
		DoUpdates: clause.AssignmentColumns([]string{"datum"}),
	}).Create(params).Error
}

func (d *MetadataStorePostgres) UpsertMidnightEpochCandidates(
	ec *models.MidnightEpochCandidates,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "epoch"}},
		DoUpdates: clause.AssignmentColumns([]string{"candidates_cbor"}),
	}).Create(ec).Error
}
