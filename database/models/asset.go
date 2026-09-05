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
	"errors"
	"fmt"
	"math/big"

	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

type Asset struct {
	Name        []byte
	NameHex     []byte
	PolicyId    []byte
	Fingerprint []byte
	ID          uint
	UtxoID      uint
	Amount      types.Uint64
}

// AssetMintBurn records a single asset mint or burn event: one row per
// (transaction, asset) pair for every transaction that mints or burns the
// asset. Quantity is a signed decimal string (positive for a mint, negative
// for a burn). Only populated in API storage mode.
//
// Unlike Asset (which tracks live UTxO holdings), this table preserves the
// full mint/burn history so the Blockfrost API can derive an asset's
// initial_mint_tx_hash (earliest event) and mint_or_burn_count (row count).
type AssetMintBurn struct {
	ID          uint
	TxHash      []byte
	PolicyId    []byte
	Name        []byte
	Fingerprint []byte
	Slot        uint64
	Quantity    string
	TxIndex     uint32
}

// ConvertMintToAssetMintBurnModels converts a transaction's mint field into a
// slice of AssetMintBurn models, one per (policy, asset name) pair with a
// non-zero net quantity. Returns nil when there is nothing minted or burned.
func ConvertMintToAssetMintBurnModels(
	mint *lcommon.MultiAsset[lcommon.MultiAssetTypeMint],
	txHash []byte,
	slot uint64,
	txIndex uint32,
) []AssetMintBurn {
	if mint == nil {
		return nil
	}
	var rows []AssetMintBurn
	for _, policyId := range mint.Policies() {
		policyIdBytes := policyId.Bytes()
		for _, assetNameBytes := range mint.Assets(policyId) {
			amount := mint.Asset(policyId, assetNameBytes)
			if amount == nil || amount.Sign() == 0 {
				continue
			}
			fingerprint := lcommon.NewAssetFingerprint(
				policyIdBytes,
				assetNameBytes,
			)
			rows = append(rows, AssetMintBurn{
				TxHash:      append([]byte(nil), txHash...),
				PolicyId:    policyIdBytes,
				Name:        append([]byte(nil), assetNameBytes...),
				Fingerprint: []byte(fingerprint.String()),
				Slot:        slot,
				Quantity:    amount.String(),
				TxIndex:     txIndex,
			})
		}
	}
	return rows
}

// checkedUint64FromBigInt converts amount to a uint64, rejecting negative
// values and values that exceed math.MaxUint64. big.Int.Uint64 silently
// keeps only the low 64 bits on overflow, which would otherwise let an
// indexed asset amount wrap into an unrelated, much smaller value.
func checkedUint64FromBigInt(amount *big.Int) (uint64, error) {
	if amount == nil {
		return 0, errors.New("asset amount is nil")
	}
	if !amount.IsUint64() {
		return 0, fmt.Errorf(
			"asset amount %s does not fit in uint64",
			amount.String(),
		)
	}
	return amount.Uint64(), nil
}

// ConvertMultiAssetToModels converts a MultiAsset structure into a slice of Asset models.
// Each asset is populated with its name, hex-encoded name, policy ID, fingerprint, and amount.
// Returns an empty slice if multiAsset is nil or contains no assets.
func ConvertMultiAssetToModels(
	multiAsset *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput],
) ([]Asset, error) {
	if multiAsset == nil {
		return []Asset{}, nil
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
			amount, err := checkedUint64FromBigInt(
				multiAsset.Asset(policyId, assetNameBytes),
			)
			if err != nil {
				return nil, fmt.Errorf(
					"asset %x.%x: %w",
					policyIdBytes,
					assetNameBytes,
					err,
				)
			}

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
				Amount:      types.Uint64(amount),
			}
			assets = append(assets, asset)
		}
	}

	return assets, nil
}
