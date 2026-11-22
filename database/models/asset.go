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
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"slices"

	"github.com/blinklabs-io/dingo/database/types"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
)

// Asset is one native-asset quantity held by an output. The asset's CIP-14
// fingerprint is derived from PolicyId and Name where it is needed.
type Asset struct {
	Name     []byte
	PolicyId []byte
	Amount   types.Uint64
}

type AssetHolderQuery struct {
	PolicyID   []byte
	AssetName  []byte
	Descending bool
	Limit      int
	Offset     int
}

// AssetHolder is one metadata-aggregated live holder.
type AssetHolder struct {
	Address  []byte
	Quantity uint64
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
	TxHash      []byte
	PolicyId    []byte
	Name        []byte
	Fingerprint []byte
	Slot        uint64
	Quantity    string
	TxIndex     uint32
}

// MintedAsset identifies an asset with a recorded mint or burn event.
type MintedAsset struct {
	PolicyId []byte
	Name     []byte
}

// ConvertMintToAssetMintBurnModels converts a transaction's mint field into a
// slice of AssetMintBurn models, one per (policy, asset name) pair with a
// non-zero net quantity. Fingerprint is left empty: it derives from the policy
// and name and is computed where the history is read. Returns nil when there
// is nothing minted or burned.
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
	for _, policyId := range SortedPolicies(mint) {
		policyIdBytes := policyId.Bytes()
		for _, assetNameBytes := range SortedAssetNames(mint, policyId) {
			amount := mint.Asset(policyId, assetNameBytes)
			if amount == nil || amount.Sign() == 0 {
				continue
			}
			rows = append(rows, AssetMintBurn{
				TxHash:   append([]byte(nil), txHash...),
				PolicyId: policyIdBytes,
				Name:     append([]byte(nil), assetNameBytes...),
				Slot:     slot,
				Quantity: amount.String(),
				TxIndex:  txIndex,
			})
		}
	}
	return rows
}

// CheckedUint64FromBigInt converts amount to a uint64, rejecting negative
// values and values that exceed math.MaxUint64. big.Int.Uint64 silently
// keeps only the low 64 bits on overflow, which would otherwise let an
// indexed asset amount wrap into an unrelated, much smaller value. Exported
// so other packages indexing arbitrary-precision on-chain quantities (e.g.
// midnight/indexer's cNIGHT amounts) share this one check rather than each
// reimplementing it.
func CheckedUint64FromBigInt(amount *big.Int) (uint64, error) {
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
// The slice is in canonical order (policy ID bytes, then asset name bytes), so
// row insertion order and every API listing built from it are stable.
// Each asset is populated with its name, policy ID, and amount.
// Returns an empty slice if multiAsset is nil or contains no assets.
func ConvertMultiAssetToModels(
	multiAsset *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput],
) ([]Asset, error) {
	if multiAsset == nil {
		return []Asset{}, nil
	}
	numAssets := 0
	// Get all policy IDs
	policyIds := SortedPolicies(multiAsset)
	for _, policyId := range policyIds {
		numAssets += len(multiAsset.Assets(policyId))
	}
	assets := make([]Asset, 0, numAssets)
	for _, policyId := range policyIds {
		policyIdBytes := policyId.Bytes()

		// Get asset names for this policy
		assetNames := SortedAssetNames(multiAsset, policyId)
		for _, assetNameBytes := range assetNames {
			amount, err := CheckedUint64FromBigInt(
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

			asset := Asset{
				Name:     assetNameBytes,
				PolicyId: policyIdBytes,
				Amount:   types.Uint64(amount),
			}
			assets = append(assets, asset)
		}
	}

	return assets, nil
}

// SortedPolicies returns the policy IDs of multiAsset in canonical order:
// ascending by the raw policy ID bytes. MultiAsset.Policies iterates a Go map,
// so its order differs from call to call.
func SortedPolicies[T int64 | uint64 | *big.Int](
	multiAsset *lcommon.MultiAsset[T],
) []lcommon.Blake2b224 {
	policies := multiAsset.Policies()
	slices.SortFunc(policies, func(a, b lcommon.Blake2b224) int {
		return bytes.Compare(a[:], b[:])
	})
	return policies
}

// SortedAssetNames returns the asset names under policyId in canonical order:
// ascending by raw bytes. MultiAsset.Assets iterates a Go map, so its order
// differs from call to call.
func SortedAssetNames[T int64 | uint64 | *big.Int](
	multiAsset *lcommon.MultiAsset[T],
	policyId lcommon.Blake2b224,
) [][]byte {
	names := multiAsset.Assets(policyId)
	slices.SortFunc(names, bytes.Compare)
	return names
}

// SortAssetsCanonical sorts assets in place into canonical order: policy ID
// bytes, then asset name bytes. The sort is stable, so equal keys keep their
// relative order.
func SortAssetsCanonical(assets []Asset) {
	slices.SortStableFunc(assets, func(a, b Asset) int {
		if c := bytes.Compare(a.PolicyId, b.PolicyId); c != 0 {
			return c
		}
		return bytes.Compare(a.Name, b.Name)
	})
}
