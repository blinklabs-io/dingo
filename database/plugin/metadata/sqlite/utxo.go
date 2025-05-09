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
	"fmt"

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/models"
	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/ledger"
	"gorm.io/gorm"
)

// GetUtxo returns a Utxo by reference
func (d *MetadataStoreSqlite) GetUtxo(
	txId []byte,
	idx uint32,
	txn *gorm.DB,
) (models.Utxo, error) {
	ret := models.Utxo{}
	tmpUtxo := models.Utxo{}
	if txn != nil {
		result := txn.Where("deleted_slot = 0").
			First(&tmpUtxo, "tx_id = ? AND output_idx = ?", txId, idx)
		if result.Error != nil {
			return ret, result.Error
		}
	} else {
		result := d.DB().Where("deleted_slot = 0").First(&tmpUtxo, "tx_id = ? AND output_idx = ?", txId, idx)
		if result.Error != nil {
			return ret, result.Error
		}
	}
	ret = tmpUtxo
	return ret, nil
}

// GetUtxosAddedAfterSlot returns a list of Utxos added after a given slot
func (d *MetadataStoreSqlite) GetUtxosAddedAfterSlot(
	slot uint64,
	txn *gorm.DB,
) ([]models.Utxo, error) {
	var ret []models.Utxo
	if txn != nil {
		result := txn.Where("added_slot > 0", slot).
			Order("id DESC").
			Find(&ret)
		if result.Error != nil {
			return ret, result.Error
		}
	} else {
		result := d.DB().Where("added_slot > 0", slot).
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
	addr ledger.Address,
	txn *gorm.DB,
) ([]models.Utxo, error) {
	var ret []models.Utxo
	// Build sub-query for address
	var addrQuery *gorm.DB
	if addr.PaymentKeyHash() != ledger.NewBlake2b224(nil) {
		addrQuery = txn.Where("payment_key = ?", addr.PaymentKeyHash().Bytes())
	}
	if addr.StakeKeyHash() != ledger.NewBlake2b224(nil) {
		if addrQuery != nil {
			addrQuery = addrQuery.Or(
				"staking_key = ?",
				addr.StakeKeyHash().Bytes(),
			)
		} else {
			addrQuery = txn.Where("staking_key = ?", addr.StakeKeyHash().Bytes())
		}
	}
	result := txn.
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
		result := txn.Where("deleted_slot > ?", slot).
			Delete(&models.Utxo{})
		if result.Error != nil {
			return result.Error
		}
	} else {
		result := d.DB().Where("deleted_slot > ?", slot).
			Delete(&models.Utxo{})
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// SetUtxo saves a UTxO
func (d *MetadataStoreSqlite) SetUtxo(
	txId []byte, // hash
	idx uint32, // idx
	slot uint64, // slot
	payment []byte, // payment
	stake []byte, // stake
	txn *gorm.DB,
) error {
	tmpUtxo := models.Utxo{
		TxId:       txId,
		OutputIdx:  idx,
		AddedSlot:  slot,
		PaymentKey: payment,
		StakingKey: stake,
	}
	if txn != nil {
		result := txn.Create(&tmpUtxo)
		if result.Error != nil {
			return result.Error
		}
	} else {
		result := d.DB().Create(&tmpUtxo)
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// AddUtxos saves a batch of UTxOs
func (d *MetadataStoreSqlite) AddUtxos(
	utxos []types.UtxoSlot,
	txn *gorm.DB,
) error {
	items := make([]models.Utxo, 0, len(utxos))
	for _, utxo := range utxos {
		items = append(
			items,
			utxoLedgerToModel(utxo.Utxo, utxo.Slot),
		)
	}
	if txn != nil {
		result := txn.Create(items)
		if result.Error != nil {
			return result.Error
		}
	} else {
		result := d.DB().Create(items)
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// SetUtxoDeletedAtSlot marks a UTxO as deleted at a given slot
func (d *MetadataStoreSqlite) SetUtxoDeletedAtSlot(
	utxoId ledger.TransactionInput,
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

func utxoLedgerToModel(
	utxo ledger.Utxo,
	slot uint64,
) models.Utxo {
	outAddr := utxo.Output.Address()
	ret := models.Utxo{
		TxId:       utxo.Id.Id().Bytes(),
		OutputIdx:  utxo.Id.Index(),
		AddedSlot:  slot,
		PaymentKey: outAddr.PaymentKeyHash().Bytes(),
		StakingKey: outAddr.StakeKeyHash().Bytes(),
		Cbor:       utxo.Output.Cbor(),
	}
	return ret
}
