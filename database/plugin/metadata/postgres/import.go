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
	"log/slog"

	"github.com/blinklabs-io/dingo/database/models"
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
		return err
	}

	// Collect assets and strip them from UTxOs before insert.
	// NOTE: this clears utxos[i].Assets on the caller's slice.
	// The caller must not rely on Assets after this call.
	type pendingAsset struct {
		utxoIdx int
		asset   models.Asset
	}
	var pending []pendingAsset
	for i := range utxos {
		for _, a := range utxos[i].Assets {
			pending = append(pending, pendingAsset{
				utxoIdx: i,
				asset:   a,
			})
		}
		utxos[i].Assets = nil
	}

	// Insert UTxOs (without assets)
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

	// Link assets to their UTxO IDs and batch-insert.
	// When a UTxO hit the ON CONFLICT DO NOTHING path,
	// GORM does not populate its ID. We batch re-fetch
	// those IDs so their assets can still be inserted.
	if len(pending) > 0 {
		if err := batchRefetchUtxoIDs(
			db, utxos,
		); err != nil {
			return fmt.Errorf(
				"batch re-fetching utxo ids: %w", err,
			)
		}
		assets := make([]models.Asset, 0, len(pending))
		var droppedAssets int
		for _, p := range pending {
			if utxos[p.utxoIdx].ID == 0 {
				droppedAssets++
				continue
			}
			p.asset.UtxoID = utxos[p.utxoIdx].ID
			assets = append(assets, p.asset)
		}
		if droppedAssets > 0 {
			slog.Warn(
				"assets dropped: parent UTxO ID "+
					"not resolved after re-fetch",
				"dropped", droppedAssets,
			)
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

// refetchChunkSize limits the number of tx_ids in a single
// WHERE IN clause to stay within database variable limits.
const refetchChunkSize = 500

// batchRefetchUtxoIDs resolves IDs for UTxOs whose ID was not
// populated by GORM after an ON CONFLICT DO NOTHING insert. It
// replaces per-UTxO SELECT queries with chunked batch lookups.
func batchRefetchUtxoIDs(
	db *gorm.DB,
	utxos []models.Utxo,
) error {
	// Collect unique tx_ids that need re-fetching
	txIdSet := make(map[string]struct{})
	for i := range utxos {
		if utxos[i].ID == 0 {
			txIdSet[string(utxos[i].TxId)] = struct{}{}
		}
	}
	if len(txIdSet) == 0 {
		return nil
	}
	txIds := make([][]byte, 0, len(txIdSet))
	for k := range txIdSet {
		txIds = append(txIds, []byte(k))
	}

	// Build lookup map: (tx_id, output_idx) -> ID
	type utxoKey struct {
		txId      string
		outputIdx uint32
	}
	lookup := make(map[utxoKey]uint, len(txIdSet))

	// Fetch in chunks to stay within variable limits
	for i := 0; i < len(txIds); i += refetchChunkSize {
		end := min(i+refetchChunkSize, len(txIds))
		chunk := txIds[i:end]
		var existing []models.Utxo
		if err := db.Select("id", "tx_id", "output_idx").
			Where("tx_id IN ?", chunk).
			Find(&existing).Error; err != nil {
			return err
		}
		for _, e := range existing {
			lookup[utxoKey{
				string(e.TxId), e.OutputIdx,
			}] = e.ID
		}
	}

	// Map IDs back to the original slice
	for i := range utxos {
		if utxos[i].ID != 0 {
			continue
		}
		k := utxoKey{
			string(utxos[i].TxId),
			utxos[i].OutputIdx,
		}
		if id, ok := lookup[k]; ok {
			utxos[i].ID = id
		}
	}
	return nil
}
