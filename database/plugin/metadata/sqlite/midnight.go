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

package sqlite

import (
	"errors"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// CreateMidnightAssetCreate inserts a cNIGHT UTxO creation row.
// Uses OR IGNORE so that backfill replays are idempotent.
func (d *MetadataStoreSqlite) CreateMidnightAssetCreate(
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
// Uses OR IGNORE so that backfill replays are idempotent.
func (d *MetadataStoreSqlite) CreateMidnightAssetSpend(
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
// Uses OR IGNORE so that backfill replays are idempotent.
func (d *MetadataStoreSqlite) CreateMidnightRegistration(
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
// Uses OR IGNORE so that backfill replays are idempotent.
func (d *MetadataStoreSqlite) CreateMidnightDeregistration(
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
func (d *MetadataStoreSqlite) FindUnspentMidnightAssetCreates() (
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
func (d *MetadataStoreSqlite) FindUnspentMidnightRegistrations() (
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
func (d *MetadataStoreSqlite) DeleteMidnightAssetCreatesByBlock(
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
func (d *MetadataStoreSqlite) DeleteMidnightAssetSpendsByBlock(
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
func (d *MetadataStoreSqlite) DeleteMidnightRegistrationsByBlock(
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
func (d *MetadataStoreSqlite) DeleteMidnightDeregistrationsByBlock(
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

// FindMidnightAssetCreatesFrom returns cNIGHT create rows ordered by
// (block_number, tx_index) ascending, starting strictly after
// (startBlock, startTxIndex). limit <= 0 means no SQL LIMIT is applied.
func (d *MetadataStoreSqlite) FindMidnightAssetCreatesFrom(
	startBlock uint64,
	startTxIndex uint32,
	limit int,
	txn types.Txn,
) ([]models.MidnightAssetCreate, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	query := db.Where(
		"(block_number > ?) OR (block_number = ? AND tx_index > ?)",
		startBlock, startBlock, startTxIndex,
	).Order("block_number ASC, tx_index ASC")
	if limit > 0 {
		query = query.Limit(limit)
	}
	var rows []models.MidnightAssetCreate
	if err := query.Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// FindMidnightAssetSpendsFrom returns cNIGHT spend rows ordered by
// (block_number, tx_index) ascending, starting strictly after
// (startBlock, startTxIndex). limit <= 0 means no SQL LIMIT is applied.
func (d *MetadataStoreSqlite) FindMidnightAssetSpendsFrom(
	startBlock uint64,
	startTxIndex uint32,
	limit int,
	txn types.Txn,
) ([]models.MidnightAssetSpend, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	query := db.Where(
		"(block_number > ?) OR (block_number = ? AND tx_index > ?)",
		startBlock, startBlock, startTxIndex,
	).Order("block_number ASC, tx_index ASC")
	if limit > 0 {
		query = query.Limit(limit)
	}
	var rows []models.MidnightAssetSpend
	if err := query.Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// FindMidnightRegistrationsFrom returns registration rows ordered by
// (block_number, tx_index) ascending, starting strictly after
// (startBlock, startTxIndex). limit <= 0 means no SQL LIMIT is applied.
func (d *MetadataStoreSqlite) FindMidnightRegistrationsFrom(
	startBlock uint64,
	startTxIndex uint32,
	limit int,
	txn types.Txn,
) ([]models.MidnightRegistration, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	query := db.Where(
		"(block_number > ?) OR (block_number = ? AND tx_index > ?)",
		startBlock, startBlock, startTxIndex,
	).Order("block_number ASC, tx_index ASC")
	if limit > 0 {
		query = query.Limit(limit)
	}
	var rows []models.MidnightRegistration
	if err := query.Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// FindMidnightDeregistrationsFrom returns deregistration rows ordered by
// (block_number, tx_index) ascending, starting strictly after
// (startBlock, startTxIndex). limit <= 0 means no SQL LIMIT is applied.
func (d *MetadataStoreSqlite) FindMidnightDeregistrationsFrom(
	startBlock uint64,
	startTxIndex uint32,
	limit int,
	txn types.Txn,
) ([]models.MidnightDeregistration, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	query := db.Where(
		"(block_number > ?) OR (block_number = ? AND tx_index > ?)",
		startBlock, startBlock, startTxIndex,
	).Order("block_number ASC, tx_index ASC")
	if limit > 0 {
		query = query.Limit(limit)
	}
	var rows []models.MidnightDeregistration
	if err := query.Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

func (d *MetadataStoreSqlite) GetMidnightCandidates(
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

func (d *MetadataStoreSqlite) InsertMidnightGovernanceDatum(
	txn types.Txn,
	datum *models.MidnightGovernanceDatum,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(datum).Error
}

func (d *MetadataStoreSqlite) DeleteMidnightGovernanceDatumsByBlock(
	txn types.Txn,
	blockNumber uint64,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Where("block_number = ?", blockNumber).
		Delete(&models.MidnightGovernanceDatum{}).Error
}

func (d *MetadataStoreSqlite) GetLatestMidnightGovernanceDatum(
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
	if result.Error != nil {
		return nil, result.Error
	}
	return &datum, nil
}

func (d *MetadataStoreSqlite) GetLatestMidnightAriadneParams(
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

func (d *MetadataStoreSqlite) GetMidnightAriadneParamsByEpoch(
	epoch uint64,
	txn types.Txn,
) (*models.MidnightAriadneParams, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	var params models.MidnightAriadneParams
	if result := db.Where("epoch = ?", epoch).First(&params); result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return &params, nil
}

func (d *MetadataStoreSqlite) UpsertMidnightAriadneParams(
	txn types.Txn,
	params *models.MidnightAriadneParams,
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

func (d *MetadataStoreSqlite) DeleteMidnightAriadneParamsByEpoch(
	txn types.Txn,
	epoch uint64,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Where("epoch = ?", epoch).
		Delete(&models.MidnightAriadneParams{}).Error
}

func (d *MetadataStoreSqlite) CreateMidnightAriadneRollback(
	txn types.Txn,
	rollback *models.MidnightAriadneRollback,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Clauses(clause.OnConflict{DoNothing: true}).Create(rollback).Error
}

func (d *MetadataStoreSqlite) FindMidnightAriadneRollbacksByBlock(
	txn types.Txn,
	blockNumber uint64,
) ([]models.MidnightAriadneRollback, error) {
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	var rollbacks []models.MidnightAriadneRollback
	if err := db.Where("block_number = ?", blockNumber).
		Order("epoch ASC").
		Find(&rollbacks).Error; err != nil {
		return nil, err
	}
	return rollbacks, nil
}

func (d *MetadataStoreSqlite) DeleteMidnightAriadneRollbacksByBlock(
	txn types.Txn,
	blockNumber uint64,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Where("block_number = ?", blockNumber).
		Delete(&models.MidnightAriadneRollback{}).Error
}

func (d *MetadataStoreSqlite) DeleteMidnightAriadneRollbacksBeforeBlock(
	txn types.Txn,
	blockNumber uint64,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Where("block_number < ?", blockNumber).
		Delete(&models.MidnightAriadneRollback{}).Error
}

func (d *MetadataStoreSqlite) UpsertMidnightEpochCandidates(
	txn types.Txn,
	ec *models.MidnightEpochCandidates,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "epoch"}},
		DoUpdates: clause.AssignmentColumns([]string{"block_number", "candidates_cbor"}),
	}).Create(ec).Error
}

func (d *MetadataStoreSqlite) DeleteMidnightEpochCandidatesByBlock(
	txn types.Txn,
	blockNumber uint64,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	return db.Where("block_number = ?", blockNumber).
		Delete(&models.MidnightEpochCandidates{}).Error
}
