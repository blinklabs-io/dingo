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

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	"gorm.io/gorm"
)

// GetUtxo returns a Utxo by reference
func (d *MetadataStoreSqlite) GetUtxo(
	txId []byte,
	idx uint32,
	txn types.Txn,
) (*models.Utxo, error) {
	ret := &models.Utxo{}
	var db *gorm.DB
	if txn == nil {
		db = d.DB()
	} else {
		db = d.dbFromTxn(txn)
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

// GetUtxosAddedAfterSlot returns a list of Utxos added after a given slot
func (d *MetadataStoreSqlite) GetUtxosAddedAfterSlot(
	slot uint64,
	txn types.Txn,
) ([]models.Utxo, error) {
	var ret []models.Utxo
	var db *gorm.DB
	if txn == nil {
		db = d.DB()
	} else {
		db = d.dbFromTxn(txn)
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
	var query *gorm.DB
	if txn != nil {
		query = d.dbFromTxn(txn)
	} else {
		query = d.DB()
	}
	query = query.Where("deleted_slot > 0 AND deleted_slot <= ?", slot).
		Order("id DESC")
	if limit > 0 {
		query = query.Limit(limit)
	}
	result := query.Find(&ret)
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
	// Build sub-query for address
	var addrQuery *gorm.DB
	var db *gorm.DB
	if txn == nil {
		db = d.DB()
	} else {
		db = d.dbFromTxn(txn)
	}
	if addr.PaymentKeyHash() != ledger.NewBlake2b224(nil) {
		addrQuery = db.Where("payment_key = ?", addr.PaymentKeyHash().Bytes())
	}
	if addr.StakeKeyHash() != ledger.NewBlake2b224(nil) {
		if addrQuery != nil {
			addrQuery = addrQuery.Or(
				"staking_key = ?",
				addr.StakeKeyHash().Bytes(),
			)
		} else {
			addrQuery = db.Where("staking_key = ?", addr.StakeKeyHash().Bytes())
		}
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

func (d *MetadataStoreSqlite) DeleteUtxo(
	utxo any,
	txn types.Txn,
) error {
	var tmpUtxo models.Utxo
	switch val := utxo.(type) {
	case models.Utxo:
		tmpUtxo = val
	case *models.Utxo:
		if val == nil {
			return fmt.Errorf("failed to convert utxo from %T", utxo)
		}
		tmpUtxo = *val
	default:
		return fmt.Errorf("failed to convert utxo from %T", utxo)
	}
	var db *gorm.DB
	if txn == nil {
		db = d.DB()
	} else {
		db = d.dbFromTxn(txn)
	}
	result := db.Delete(&tmpUtxo)
	if result.Error != nil {
		return result.Error
	}
	return nil
}

func (d *MetadataStoreSqlite) DeleteUtxos(
	utxos []any,
	txn types.Txn,
) error {
	tmpUtxos := []models.Utxo{}
	for _, utxo := range utxos {
		switch val := utxo.(type) {
		case models.Utxo:
			tmpUtxos = append(tmpUtxos, val)
		case *models.Utxo:
			if val == nil {
				return fmt.Errorf("failed to convert utxo from %T", utxo)
			}
			tmpUtxos = append(tmpUtxos, *val)
		default:
			return fmt.Errorf("failed to convert utxo from %T", utxo)
		}
	}
	var db *gorm.DB
	if txn == nil {
		db = d.DB()
	} else {
		db = d.dbFromTxn(txn)
	}
	result := db.Delete(&tmpUtxos)
	if result.Error != nil {
		return result.Error
	}
	return nil
}

func (d *MetadataStoreSqlite) DeleteUtxosAfterSlot(
	slot uint64,
	txn types.Txn,
) error {
	var db *gorm.DB
	if txn == nil {
		db = d.DB()
	} else {
		db = d.dbFromTxn(txn)
	}
	result := db.Where("added_slot > ?", slot).
		Delete(&models.Utxo{})
	if result.Error != nil {
		return result.Error
	}
	return nil
}

// AddUtxos saves a batch of UTxOs
func (d *MetadataStoreSqlite) AddUtxos(
	utxos []models.UtxoSlot,
	txn types.Txn,
) error {
	items := make([]models.Utxo, 0, len(utxos))
	for _, utxo := range utxos {
		items = append(
			items,
			models.UtxoLedgerToModel(utxo.Utxo, utxo.Slot),
		)
	}
	var db *gorm.DB
	if txn == nil {
		db = d.DB()
	} else {
		db = d.dbFromTxn(txn)
	}
	result := db.Session(&gorm.Session{FullSaveAssociations: true}).
		CreateInBatches(items, 1000)
	if result.Error != nil {
		return result.Error
	}
	return nil
}

// SetUtxoDeletedAtSlot marks a UTxO as deleted at a given slot
func (d *MetadataStoreSqlite) SetUtxoDeletedAtSlot(
	utxoId ledger.TransactionInput,
	slot uint64,
	txn types.Txn,
) error {
	var db *gorm.DB
	if txn == nil {
		db = d.DB()
	} else {
		db = d.dbFromTxn(txn)
	}
	result := db.Model(models.Utxo{}).
		Where("tx_id = ? AND output_idx = ?", utxoId.Id().Bytes(), utxoId.Index()).
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
	var db *gorm.DB
	if txn == nil {
		db = d.DB()
	} else {
		db = d.dbFromTxn(txn)
	}
	result := db.Model(models.Utxo{}).
		Where("deleted_slot > ?", slot).
		Update("deleted_slot", 0)
	if result.Error != nil {
		return result.Error
	}
	return nil
}
