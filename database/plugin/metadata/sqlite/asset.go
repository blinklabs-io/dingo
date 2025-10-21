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

	"github.com/blinklabs-io/dingo/database/models"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"gorm.io/gorm"
)

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

	result = query.Where("policy_id = ? AND name = ?", policyId[:], assetName).
		First(&asset)
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

// GetAssetsByUTxO returns all assets for a given UTxO using transaction ID and output index
func (d *MetadataStoreSqlite) GetAssetsByUTxO(
	txId []byte,
	idx uint32,
	txn *gorm.DB,
) ([]models.Asset, error) {
	var assets []models.Asset
	var result *gorm.DB

	query := d.DB()
	if txn != nil {
		query = txn
	}

	// Join with UTxO table to find assets by transaction ID and output index
	result = query.Joins("INNER JOIN utxos ON assets.utxo_id = utxos.id").
		Where("utxos.tx_id = ? AND utxos.idx = ?", txId, idx).
		Find(&assets)
	if result.Error != nil {
		return nil, result.Error
	}
	return assets, nil
}
