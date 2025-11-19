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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

// GetUtxo returns a Utxo by reference
func (d *MetadataStoreSqlite) GetUtxo(
	txId []byte,
	idx uint32,
	txn *gorm.DB,
) (*models.Utxo, error) {
	ret := &models.Utxo{}
	if txn == nil {
		txn = d.DB()
	}
	result := txn.Where("deleted_slot = 0").
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
	txn *gorm.DB,
) ([]models.Utxo, error) {
	var ret []models.Utxo
	if txn != nil {
		result := txn.Where("added_slot > ?", slot).
			Order("id DESC").
			Find(&ret)
		if result.Error != nil {
			return ret, result.Error
		}
	} else {
		result := d.DB().Where("added_slot > ?", slot).
			Order("id DESC").
			Find(&ret)
		if result.Error != nil {
			return ret, result.Error
		}
	}
	return ret, nil
}

// GetUtxosDeletedBeforeSlot returns a list of Utxos marked as deleted before a given slot
func (d *MetadataStoreSqlite) GetUtxosDeletedBeforeSlot(
	slot uint64,
	limit int,
	txn *gorm.DB,
) ([]models.Utxo, error) {
	var ret []models.Utxo
	var query *gorm.DB
	if txn != nil {
		query = txn
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
	addr lcommon.Address,
	txn *gorm.DB,
) ([]models.Utxo, error) {
	var ret []models.Utxo
	if txn == nil {
		txn = d.DB()
	}

	zeroHash := lcommon.NewBlake2b224(nil)
	pkh := addr.PaymentKeyHash()
	skh := addr.StakeKeyHash()

	// Build query with explicit conditions for clarity and correctness
	query := txn.Where("deleted_slot = 0")

	// Add address-based conditions
	if pkh != zeroHash && skh != zeroHash {
		// Both payment and staking keys present: match both (exact address)
		query = query.Where(
			"payment_key = ? AND staking_key = ?",
			pkh.Bytes(),
			skh.Bytes(),
		)
	} else if pkh != zeroHash {
		// Only payment key present
		query = query.Where("payment_key = ?", pkh.Bytes())
	} else if skh != zeroHash {
		// Only staking key present
		query = query.Where("staking_key = ?", skh.Bytes())
	} else {
		// No valid keys, return empty result
		return ret, nil
	}

	result := query.Find(&ret)
	if result.Error != nil {
		return nil, result.Error
	}
	return ret, nil
}

func (d *MetadataStoreSqlite) DeleteUtxo(
	utxo any,
	txn *gorm.DB,
) error {
	tmpUtxo, ok := utxo.(models.Utxo)
	if !ok {
		return fmt.Errorf("failed to convert utxo from %T", utxo)
	}
	if txn != nil {
		result := txn.Delete(&tmpUtxo)
		if result.Error != nil {
			return result.Error
		}
	} else {
		result := d.DB().Delete(&tmpUtxo)
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}

func (d *MetadataStoreSqlite) DeleteUtxos(
	utxos []any,
	txn *gorm.DB,
) error {
	tmpUtxos := []models.Utxo{}
	for _, utxo := range utxos {
		tmpUtxo, ok := utxo.(models.Utxo)
		if !ok {
			return fmt.Errorf("failed to convert utxo from %T", utxo)
		}
		tmpUtxos = append(tmpUtxos, tmpUtxo)
	}
	if txn != nil {
		result := txn.Delete(&tmpUtxos)
		if result.Error != nil {
			return result.Error
		}
	} else {
		result := d.DB().Delete(&tmpUtxos)
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}

func (d *MetadataStoreSqlite) DeleteUtxosAfterSlot(
	slot uint64,
	txn *gorm.DB,
) error {
	if txn != nil {
		result := txn.Where("added_slot > ?", slot).
			Delete(&models.Utxo{})
		if result.Error != nil {
			return result.Error
		}
	} else {
		result := d.DB().Where("added_slot > ?", slot).
			Delete(&models.Utxo{})
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// AddUtxos saves a batch of UTxOs
func (d *MetadataStoreSqlite) AddUtxos(
	utxos []models.UtxoSlot,
	txn *gorm.DB,
) error {
	items := make([]models.Utxo, 0, len(utxos))
	for _, utxo := range utxos {
		items = append(
			items,
			models.UtxoLedgerToModel(utxo.Utxo, utxo.Slot, false),
		)
	}
	if txn == nil {
		txn = d.DB()
	}
	result := txn.Session(&gorm.Session{FullSaveAssociations: true}).
		CreateInBatches(items, 1000)
	if result.Error != nil {
		return result.Error
	}
	return nil
}

// SetUtxoDeletedAtSlot marks a UTxO as deleted at a given slot
func (d *MetadataStoreSqlite) SetUtxoDeletedAtSlot(
	utxoId lcommon.TransactionInput,
	slot uint64,
	txn *gorm.DB,
) error {
	if txn != nil {
		result := txn.Model(models.Utxo{}).
			Where("tx_id = ? AND output_idx = ?", utxoId.Id().Bytes(), utxoId.Index()).
			Update("deleted_slot", slot)
		if result.Error != nil {
			return result.Error
		}
	} else {
		result := d.DB().Model(models.Utxo{}).
			Where("tx_id = ? AND output_idx = ?", utxoId.Id().Bytes(), utxoId.Index()).
			Update("deleted_slot", slot)
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// SetUtxosNotDeletedAfterSlot marks a list of Utxos as not deleted after a given slot
func (d *MetadataStoreSqlite) SetUtxosNotDeletedAfterSlot(
	slot uint64,
	txn *gorm.DB,
) error {
	if txn != nil {
		result := txn.Model(models.Utxo{}).
			Where("deleted_slot > ?", slot).
			Update("deleted_slot", 0)
		if result.Error != nil {
			return result.Error
		}
	} else {
		result := d.DB().Model(models.Utxo{}).
			Where("deleted_slot > ?", slot).
			Update("deleted_slot", 0)
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}
