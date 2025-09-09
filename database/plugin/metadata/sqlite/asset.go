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
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

// GetAssetByID returns an asset by its primary key ID
func (d *MetadataStoreSqlite) GetAssetByID(
	id uint,
	txn *gorm.DB,
) (models.Asset, error) {
	var asset models.Asset
	var result *gorm.DB

	query := d.DB()
	if txn != nil {
		query = txn
	}

	result = query.Where("id = ?", id).First(&asset)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return models.Asset{}, nil
		}
		return models.Asset{}, result.Error
	}
	return asset, nil
}

// GetAssetByPolicyAndName returns an asset by policy ID and asset name
func (d *MetadataStoreSqlite) GetAssetByPolicyAndName(
	policyId lcommon.Blake2b224,
	assetName []byte,
	txn *gorm.DB,
) (models.Asset, error) {
	var asset models.Asset
	var result *gorm.DB

	query := d.DB()
	if txn != nil {
		query = txn
	}

	result = query.Where("policy_id = ? AND name = ?", policyId[:], assetName).First(&asset)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return models.Asset{}, nil
		}
		return models.Asset{}, result.Error
	}
	return asset, nil
}

// GetAssetsByPolicy returns all assets for a given policy ID
func (d *MetadataStoreSqlite) GetAssetsByPolicy(
	policyId lcommon.Blake2b224,
	txn *gorm.DB,
) ([]models.Asset, error) {
	var assets []models.Asset
	var result *gorm.DB

	query := d.DB()
	if txn != nil {
		query = txn
	}

	result = query.Where("policy_id = ?", policyId[:]).Find(&assets)
	if result.Error != nil {
		return nil, result.Error
	}
	return assets, nil
}

// GetAssetsByUTxO returns all assets for a given UTxO
func (d *MetadataStoreSqlite) GetAssetsByUTxO(
	utxoId []byte,
	txn *gorm.DB,
) ([]models.Asset, error) {
	var assets []models.Asset
	var result *gorm.DB

	query := d.DB()
	if txn != nil {
		query = txn
	}

	result = query.Where("utxo_id = ?", utxoId).Find(&assets)
	if result.Error != nil {
		return nil, result.Error
	}
	return assets, nil
}

// SetAsset saves an asset into the database
func (d *MetadataStoreSqlite) SetAsset(
	utxoId []byte,
	name []byte,
	nameHex []byte,
	policyId lcommon.Blake2b224,
	fingerprint []byte,
	amount uint64,
	txn *gorm.DB,
) error {
	tmpItem := models.Asset{
		UTxOID:      utxoId,
		Name:        name,
		NameHex:     nameHex,
		PolicyId:    policyId[:],
		Fingerprint: fingerprint,
		Amount:      amount,
	}

	if txn != nil {
		if result := txn.Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	} else {
		if result := d.DB().Create(&tmpItem); result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// SetAssets saves multiple assets into the database
func (d *MetadataStoreSqlite) SetAssets(
	assets []models.Asset,
	txn *gorm.DB,
) error {
	if len(assets) == 0 {
		return nil
	}

	if txn != nil {
		if result := txn.Create(&assets); result.Error != nil {
			return result.Error
		}
	} else {
		if result := d.DB().Create(&assets); result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// UpdateAsset updates an existing asset
func (d *MetadataStoreSqlite) UpdateAsset(
	asset models.Asset,
	txn *gorm.DB,
) error {
	if txn != nil {
		if result := txn.Save(&asset); result.Error != nil {
			return result.Error
		}
	} else {
		if result := d.DB().Save(&asset); result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// DeleteAsset deletes a specific asset by ID
func (d *MetadataStoreSqlite) DeleteAsset(
	id uint,
	txn *gorm.DB,
) error {
	if txn != nil {
		if result := txn.Where("id = ?", id).Delete(&models.Asset{}); result.Error != nil {
			return result.Error
		}
	} else {
		if result := d.DB().Where("id = ?", id).Delete(&models.Asset{}); result.Error != nil {
			return result.Error
		}
	}
	return nil
}

// DeleteAssetsByUTxO deletes all assets associated with a specific UTxO
func (d *MetadataStoreSqlite) DeleteAssetsByUTxO(
	utxoId []byte,
	txn *gorm.DB,
) error {
	if txn != nil {
		if result := txn.Where("utxo_id = ?", utxoId).Delete(&models.Asset{}); result.Error != nil {
			return result.Error
		}
	} else {
		if result := d.DB().Where("utxo_id = ?", utxoId).Delete(&models.Asset{}); result.Error != nil {
			return result.Error
		}
	}
	return nil
}
