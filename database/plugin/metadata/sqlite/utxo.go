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

package sqlite

import (
	"errors"
	"fmt"
	"strings"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// UtxoRef represents a reference to a UTXO by transaction ID and output index
type UtxoRef struct {
	TxId      []byte
	OutputIdx uint32
}

// GetUtxo returns a Utxo by reference
func (d *MetadataStoreSqlite) GetUtxo(
	txId []byte,
	idx uint32,
	txn types.Txn,
) (*models.Utxo, error) {
	ret := &models.Utxo{}
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	result := db.Where("deleted_slot = 0").
		First(ret, "tx_id = ? AND output_idx = ?", txId, idx)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return ret, nil
}

// batchChunkSize is the maximum number of UTXO refs to query in a single SQL statement.
// Each ref uses 2 bind parameters (tx_id and output_idx), so this must be <= 499 to stay
// under SQLite's default SQLITE_MAX_VARIABLE_NUMBER limit of 999 bind parameters.
const batchChunkSize = 499

// GetUtxosBatch retrieves multiple UTXOs by their references in a single query.
// Returns a map keyed by "txid:outputidx" for easy lookup.
// Large batches are automatically chunked to avoid SQLite expression limits.
func (d *MetadataStoreSqlite) GetUtxosBatch(
	refs []UtxoRef,
	txn types.Txn,
) (map[string]*models.Utxo, error) {
	if len(refs) == 0 {
		return make(map[string]*models.Utxo), nil
	}

	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*models.Utxo, len(refs))

	// Process in chunks to avoid SQLite expression depth limits
	for i := 0; i < len(refs); i += batchChunkSize {
		end := min(i+batchChunkSize, len(refs))
		chunk := refs[i:end]

		// Build OR conditions for this chunk with preallocated slices
		conditions := make([]string, 0, len(chunk))
		args := make([]any, 0, len(chunk)*2)
		for _, ref := range chunk {
			conditions = append(conditions, "(tx_id = ? AND output_idx = ?)")
			args = append(args, ref.TxId, ref.OutputIdx)
		}

		var utxos []models.Utxo
		// Wrap OR conditions in parentheses to ensure deleted_slot=0 applies to all refs.
		// Without parens, SQL operator precedence (AND > OR) causes deleted_slot=0
		// to only apply to the first condition.
		query := db.Where("deleted_slot = 0").
			Where("("+strings.Join(conditions, " OR ")+")", args...)
		if queryResult := query.Find(&utxos); queryResult.Error != nil {
			return nil, queryResult.Error
		}

		// Add to result map
		for j := range utxos {
			key := fmt.Sprintf("%x:%d", utxos[j].TxId, utxos[j].OutputIdx)
			result[key] = &utxos[j]
		}
	}

	return result, nil
}

// GetUtxosAddedAfterSlot returns a list of Utxos added after a given slot
func (d *MetadataStoreSqlite) GetUtxosAddedAfterSlot(
	slot uint64,
	txn types.Txn,
) ([]models.Utxo, error) {
	var ret []models.Utxo
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	result := db.Where("added_slot > ?", slot).
		Order("id DESC").
		Find(&ret)
	if result.Error != nil {
		return ret, result.Error
	}
	return ret, nil
}

// GetUtxosDeletedBeforeSlot returns a list of Utxos marked as deleted before a given slot
func (d *MetadataStoreSqlite) GetUtxosDeletedBeforeSlot(
	slot uint64,
	limit int,
	txn types.Txn,
) ([]models.Utxo, error) {
	var ret []models.Utxo
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}
	db = db.Where("deleted_slot > 0 AND deleted_slot <= ?", slot).
		Order("id DESC")
	if limit > 0 {
		db = db.Limit(limit)
	}
	result := db.Find(&ret)
	if result.Error != nil {
		return ret, result.Error
	}
	return ret, nil
}

// GetUtxosByAddress returns a list of Utxos
func (d *MetadataStoreSqlite) GetUtxosByAddress(
	addr ledger.Address,
	txn types.Txn,
) ([]models.Utxo, error) {
	var ret []models.Utxo
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}

	hasPaymentKey := addr.PaymentKeyHash() != ledger.NewBlake2b224(nil)
	hasStakeKey := addr.StakeKeyHash() != ledger.NewBlake2b224(nil)

	// Build sub-query for address
	var addrQuery *gorm.DB
	if hasPaymentKey && hasStakeKey {
		// Base address: both credentials must match (AND)
		addrQuery = db.Where(
			"payment_key = ? AND staking_key = ?",
			addr.PaymentKeyHash().Bytes(),
			addr.StakeKeyHash().Bytes(),
		)
	} else if hasPaymentKey {
		addrQuery = db.Where("payment_key = ?", addr.PaymentKeyHash().Bytes())
	} else if hasStakeKey {
		addrQuery = db.Where("staking_key = ?", addr.StakeKeyHash().Bytes())
	}

	if addrQuery == nil {
		return ret, nil
	}
	result := db.
		Where("deleted_slot = 0").
		Where(addrQuery).
		Find(&ret)
	if result.Error != nil {
		return nil, result.Error
	}
	return ret, nil
}

// GetUtxosByAssets returns a list of Utxos that contain the specified assets
// policyId: the policy ID of the asset (required)
// assetName: the asset name (pass nil to match all assets under the policy, or empty []byte{} to match assets with empty names)
func (d *MetadataStoreSqlite) GetUtxosByAssets(
	policyId []byte,
	assetName []byte,
	txn types.Txn,
) ([]models.Utxo, error) {
	var ret []models.Utxo
	db, err := d.resolveReadDB(txn)
	if err != nil {
		return nil, err
	}

	// Build the asset query
	assetQuery := db.Table("asset").
		Select("utxo_id").
		Where("policy_id = ?", policyId)
	if assetName != nil {
		assetQuery = assetQuery.Where("name = ?", assetName)
	}

	// Query UTxOs that have matching assets and are not deleted
	result := db.
		Where("deleted_slot = 0").
		Where("id IN (?)", assetQuery).
		Preload("Assets").
		Find(&ret)
	if result.Error != nil {
		return nil, result.Error
	}
	return ret, nil
}

func (d *MetadataStoreSqlite) DeleteUtxo(
	utxoId models.UtxoId,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	result := db.Where("tx_id = ? AND output_idx = ?", utxoId.Hash, utxoId.Idx).
		Delete(&models.Utxo{})
	if result.Error != nil {
		return result.Error
	}
	return nil
}

func (d *MetadataStoreSqlite) DeleteUtxos(
	utxos []models.UtxoId,
	txn types.Txn,
) error {
	if len(utxos) == 0 {
		return nil
	}
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	// Process in chunks to avoid SQLite bind parameter limits
	for i := 0; i < len(utxos); i += batchChunkSize {
		end := min(i+batchChunkSize, len(utxos))
		chunk := utxos[i:end]

		// Build batch delete with OR conditions for this chunk (preallocated slices)
		conditions := make([]string, 0, len(chunk))
		args := make([]any, 0, len(chunk)*2)
		for _, u := range chunk {
			conditions = append(conditions, "(tx_id = ? AND output_idx = ?)")
			args = append(args, u.Hash, u.Idx)
		}
		query := strings.Join(conditions, " OR ")
		result := db.Where(query, args...).Delete(&models.Utxo{})
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}

func (d *MetadataStoreSqlite) DeleteUtxosAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	result := db.Where("added_slot > ?", slot).
		Delete(&models.Utxo{})
	if result.Error != nil {
		return result.Error
	}
	return nil
}

// AddUtxos saves a batch of UTxOs directly
func (d *MetadataStoreSqlite) AddUtxos(
	utxos []models.UtxoSlot,
	txn types.Txn,
) error {
	if len(utxos) == 0 {
		return nil
	}

	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}

	items := make([]models.Utxo, 0, len(utxos))
	for _, utxo := range utxos {
		items = append(items, models.UtxoLedgerToModel(utxo.Utxo, utxo.Slot))
	}

	result := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "tx_id"}, {Name: "output_idx"}},
		DoNothing: true,
	}).Create(&items)
	return result.Error
}

// SetUtxoDeletedAtSlot marks a UTxO as deleted at the given slot
func (d *MetadataStoreSqlite) SetUtxoDeletedAtSlot(
	input ledger.TransactionInput,
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	result := db.Model(&models.Utxo{}).
		Where("tx_id = ? AND output_idx = ?", input.Id().Bytes(), input.Index()).
		Update("deleted_slot", slot)
	if result.Error != nil {
		return result.Error
	}
	return nil
}

// SetUtxosNotDeletedAfterSlot marks a list of Utxos as not deleted after a given slot
func (d *MetadataStoreSqlite) SetUtxosNotDeletedAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	db, err := d.resolveDB(txn)
	if err != nil {
		return err
	}
	result := db.Model(models.Utxo{}).
		Where("deleted_slot > ?", slot).
		Update("deleted_slot", 0)
	if result.Error != nil {
		return result.Error
	}
	return nil
}
