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
	"github.com/blinklabs-io/dingo/database/plugin/metadata/importutil"
	"github.com/blinklabs-io/dingo/database/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// importUtxoBatchSize limits each INSERT to avoid exceeding
// database parameter limits (e.g. PostgreSQL's 65535 limit).
const importUtxoBatchSize = 500

// importAssetBatchSize limits each asset INSERT to avoid exceeding
// PostgreSQL's 65535 parameter limit. Each Asset has ~7 columns,
// so 1000 x 7 = 7000 parameters per batch.
const importAssetBatchSize = 1000

// ImportUtxos inserts UTxOs in bulk, ignoring duplicates.
// Assets are inserted in a second pass to avoid cascading
// the associated Assets into the same bulk INSERT, which can
// push the parameter count over PostgreSQL limits.
func (d *MetadataStorePostgres) ImportUtxos(
	utxos []models.Utxo,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return fmt.Errorf("import utxos: %w", err)
	}

	// Collect assets from UTxOs before insert. We work on a
	// local copy of each UTxO so the caller's slice is not
	// mutated (Assets remains intact after this call).
	type pendingAsset struct {
		utxoIdx int
		asset   models.Asset
	}
	var pending []pendingAsset
	stripped := make([]models.Utxo, len(utxos))
	copy(stripped, utxos)
	for i := range stripped {
		for _, a := range stripped[i].Assets {
			pending = append(pending, pendingAsset{
				utxoIdx: i,
				asset:   a,
			})
		}
		stripped[i].Assets = nil
	}

	// Insert UTxOs (without assets)
	for i := 0; i < len(stripped); i += importUtxoBatchSize {
		end := min(i+importUtxoBatchSize, len(stripped))
		batch := stripped[i:end]
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

	// Link assets to their UTxO IDs and batch-insert.
	// When a UTxO hit the ON CONFLICT DO NOTHING path,
	// GORM does not populate its ID. We batch re-fetch
	// those IDs so their assets can still be inserted.
	if len(pending) > 0 {
		if err := importutil.BatchRefetchUtxoIDs(
			db, stripped,
		); err != nil {
			return fmt.Errorf(
				"batch re-fetching utxo ids: %w", err,
			)
		}
		assets := make([]models.Asset, 0, len(pending))
		for _, p := range pending {
			p.asset.UtxoID = stripped[p.utxoIdx].ID
			assets = append(assets, p.asset)
		}
		for i := 0; i < len(assets); i += importAssetBatchSize {
			end := min(i+importAssetBatchSize, len(assets))
			batch := assets[i:end]
			result := db.Clauses(clause.OnConflict{
				Columns: []clause.Column{
					{Name: "utxo_id"},
					{Name: "policy_id"},
					{Name: "name"},
				},
				DoNothing: true,
			}).Create(&batch)
			if result.Error != nil {
				return fmt.Errorf(
					"import utxo assets: %w",
					result.Error,
				)
			}
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
		return fmt.Errorf("import account: %w", err)
	}
	result := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "staking_key"}},
		DoUpdates: clause.AssignmentColumns(
			[]string{"pool", "drep", "drep_type", "active", "reward"},
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
		return fmt.Errorf("import pool: %w", err)
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

	// Create registration record (ignore duplicates on retry).
	// Target pool_id + added_slot as the logical uniqueness key.
	if result := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "pool_id"},
			{Name: "added_slot"},
		},
		DoNothing: true,
	}).Create(reg); result.Error != nil {
		return fmt.Errorf(
			"import pool registration: %w",
			result.Error,
		)
	}

	// On conflict, GORM may not populate reg.ID (PostgreSQL
	// returns 0 for last_insert_id on DoNothing). Re-fetch
	// the registration ID so Owners/Relays associations get
	// the correct PoolRegistrationID instead of 0.
	if reg.ID == 0 {
		var existing models.PoolRegistration
		if err := db.Where(
			"pool_id = ? AND added_slot = ?",
			reg.PoolID, reg.AddedSlot,
		).First(&existing).Error; err != nil {
			return fmt.Errorf(
				"fetching pool registration ID after upsert: %w",
				err,
			)
		}
		reg.ID = existing.ID

		// When DoNothing was triggered, GORM skipped nested
		// Owners/Relays. Delete existing ones for this
		// registration to avoid duplicates on retry, then
		// insert fresh. This must run inside a transaction
		// so the delete+insert is atomic.
		if txn == nil {
			return errors.New("ImportPool requires a transaction for " +
				"atomic owner/relay replacement",
			)
		}
		if len(reg.Owners) > 0 {
			for i := range reg.Owners {
				reg.Owners[i].PoolRegistrationID = reg.ID
				reg.Owners[i].PoolID = pool.ID
			}
			// Delete existing owners for this registration
			// to avoid duplicates on retry.
			if result := db.Where(
				"pool_registration_id = ?", reg.ID,
			).Delete(
				&models.PoolRegistrationOwner{},
			); result.Error != nil {
				return fmt.Errorf(
					"deleting existing owners: %w",
					result.Error,
				)
			}
			if result := db.Create(
				&reg.Owners,
			); result.Error != nil {
				return fmt.Errorf(
					"importing pool owners: %w",
					result.Error,
				)
			}
		}
		if len(reg.Relays) > 0 {
			for i := range reg.Relays {
				reg.Relays[i].PoolRegistrationID = reg.ID
				reg.Relays[i].PoolID = pool.ID
			}
			// Delete existing relays for this registration
			// to avoid duplicates on retry.
			if result := db.Where(
				"pool_registration_id = ?", reg.ID,
			).Delete(
				&models.PoolRegistrationRelay{},
			); result.Error != nil {
				return fmt.Errorf(
					"deleting existing relays: %w",
					result.Error,
				)
			}
			if result := db.Create(
				&reg.Relays,
			); result.Error != nil {
				return fmt.Errorf(
					"importing pool relays: %w",
					result.Error,
				)
			}
		}
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
		return fmt.Errorf("import drep: %w", err)
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

	// Create registration record (ignore duplicates on retry).
	// Target drep_credential + added_slot as the logical
	// uniqueness key.
	if result := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "drep_credential"},
			{Name: "added_slot"},
		},
		DoNothing: true,
	}).Create(reg); result.Error != nil {
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
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, fmt.Errorf(
			"GetImportCheckpoint: resolve db: %w", err,
		)
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
		return fmt.Errorf("set import checkpoint: %w", err)
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
