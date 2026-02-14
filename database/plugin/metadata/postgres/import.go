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
	"fmt"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// importUtxoBatchSize limits each INSERT to avoid exceeding
// database parameter limits (e.g. PostgreSQL's 65535 limit).
const importUtxoBatchSize = 500

// ImportUtxos inserts UTxOs in bulk, ignoring duplicates.
// Splits into sub-batches to stay within parameter limits.
func (d *MetadataStorePostgres) ImportUtxos(
	utxos []models.Utxo,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	for i := 0; i < len(utxos); i += importUtxoBatchSize {
		end := min(i+importUtxoBatchSize, len(utxos))
		batch := utxos[i:end]
		result := db.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "tx_id"},
				{Name: "output_idx"},
			},
			DoNothing: true,
		}).Create(&batch)
		if result.Error != nil {
			return fmt.Errorf("import utxos: %w", result.Error)
		}
	}
	return nil
}

// ImportAccount upserts an account (insert or update delegation
// fields on conflict).
func (d *MetadataStorePostgres) ImportAccount(
	account *models.Account,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	result := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "staking_key"}},
		DoUpdates: clause.AssignmentColumns(
			[]string{"pool", "drep", "active", "reward"},
		),
	}).Create(account)
	if result.Error != nil {
		return fmt.Errorf("import account: %w", result.Error)
	}
	return nil
}

// ImportPool upserts a pool and creates a registration record.
func (d *MetadataStorePostgres) ImportPool(
	pool *models.Pool,
	reg *models.PoolRegistration,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}

	// Upsert the pool record
	result := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "pool_key_hash"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"vrf_key_hash", "pledge", "cost",
			"margin", "reward_account",
		}),
	}).Create(pool)
	if result.Error != nil {
		return fmt.Errorf("import pool: %w", result.Error)
	}

	// On conflict, GORM may not populate pool.ID (PostgreSQL
	// returns 0 for last_insert_id on conflict). Re-fetch
	// the pool ID to ensure it is set for related records.
	if pool.ID == 0 {
		var existing models.Pool
		if err := db.Where(
			"pool_key_hash = ?", pool.PoolKeyHash,
		).First(&existing).Error; err != nil {
			return fmt.Errorf(
				"fetching pool ID after upsert: %w", err,
			)
		}
		pool.ID = existing.ID
	}

	// Link registration to pool
	reg.PoolID = pool.ID
	for i := range reg.Owners {
		reg.Owners[i].PoolID = pool.ID
	}
	for i := range reg.Relays {
		reg.Relays[i].PoolID = pool.ID
	}

	// Create registration record. Idempotency is handled at the
	// application level via ImportCheckpoint, not by DB constraints.
	if result := db.Create(reg); result.Error != nil {
		return fmt.Errorf(
			"import pool registration: %w",
			result.Error,
		)
	}

	return nil
}

// ImportDrep upserts a DRep and creates a registration record.
func (d *MetadataStorePostgres) ImportDrep(
	drep *models.Drep,
	reg *models.RegistrationDrep,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}

	// Upsert the DRep record
	result := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "credential"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"anchor_url", "anchor_hash", "active",
		}),
	}).Create(drep)
	if result.Error != nil {
		return fmt.Errorf("import drep: %w", result.Error)
	}

	// On conflict, GORM may not populate drep.ID. Re-fetch.
	if drep.ID == 0 {
		var existing models.Drep
		if err := db.Where(
			"credential = ?", drep.Credential,
		).First(&existing).Error; err != nil {
			return fmt.Errorf(
				"fetching drep ID after upsert: %w", err,
			)
		}
		drep.ID = existing.ID
	}

	// Create registration record. Idempotency is handled at the
	// application level via ImportCheckpoint, not by DB constraints.
	if result := db.Create(reg); result.Error != nil {
		return fmt.Errorf(
			"import drep registration: %w",
			result.Error,
		)
	}

	return nil
}

// GetImportCheckpoint retrieves the checkpoint for a given import
// key. Returns nil if no checkpoint exists.
func (d *MetadataStorePostgres) GetImportCheckpoint(
	importKey string,
	txn types.Txn,
) (*models.ImportCheckpoint, error) {
	db, err := d.resolveDB(txn)
	if err != nil {
		return nil, err
	}
	var cp models.ImportCheckpoint
	result := db.Where("import_key = ?", importKey).First(&cp)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf(
			"get import checkpoint: %w",
			result.Error,
		)
	}
	return &cp, nil
}

// SetImportCheckpoint creates or updates a checkpoint.
func (d *MetadataStorePostgres) SetImportCheckpoint(
	checkpoint *models.ImportCheckpoint,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "import_key"}},
		DoUpdates: clause.AssignmentColumns([]string{"phase"}),
	}).Create(checkpoint)
	if result.Error != nil {
		return fmt.Errorf(
			"set import checkpoint: %w",
			result.Error,
		)
	}
	return nil
}
