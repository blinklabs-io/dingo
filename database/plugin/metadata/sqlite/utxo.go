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

	"github.com/blinklabs-io/dingo/database/plugin/metadata/sqlite/models"
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
	// Create table if it doesn't exist
	if err := d.DB().AutoMigrate(&models.Utxo{}); err != nil {
		return ret, err
	}
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
		return errors.New("failed to convert utxo")
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
			return errors.New("failed to convert utxo")
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
