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

package models

import (
	"encoding/hex"

	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

type Asset struct {
	Name        []byte `gorm:"index;size:64"`
	NameHex     []byte `gorm:"index;size:64"`
	PolicyId    []byte `gorm:"index;size:64"`
	Fingerprint []byte `gorm:"index;size:64"`
	ID          uint         `gorm:"primaryKey"`
	UtxoID      uint         `gorm:"index"`
	Amount      types.Uint64 `gorm:"index"`
}

func (Asset) TableName() string {
	return "asset"
}

// ConvertMultiAssetToModels converts a MultiAsset structure into a slice of Asset models.
// Each asset is populated with its name, hex-encoded name, policy ID, fingerprint, and amount.
// Returns an empty slice if multiAsset is nil or contains no assets.
func ConvertMultiAssetToModels(
	multiAsset *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput],
) []Asset {
	if multiAsset == nil {
		return []Asset{}
	}
	numAssets := 0
	// Get all policy IDs
	policyIds := multiAsset.Policies()
	for _, policyId := range policyIds {
		numAssets += len(multiAsset.Assets(policyId))
	}
	assets := make([]Asset, 0, numAssets)
	for _, policyId := range policyIds {
		policyIdBytes := policyId.Bytes()

		// Get asset names for this policy
		assetNames := multiAsset.Assets(policyId)
		for _, assetNameBytes := range assetNames {
			amount := multiAsset.Asset(policyId, assetNameBytes)

			// Calculate fingerprint
			fingerprint := lcommon.NewAssetFingerprint(
				policyIdBytes,
				assetNameBytes,
			)

			asset := Asset{
				Name:        assetNameBytes,
				NameHex:     []byte(hex.EncodeToString(assetNameBytes)),
				PolicyId:    policyIdBytes,
				Fingerprint: []byte(fingerprint.String()),
				Amount:      types.Uint64(amount.Uint64()),
			}
			assets = append(assets, asset)
		}
	}

	return assets
}
