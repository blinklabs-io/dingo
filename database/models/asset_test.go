// Copyright 2026 Blink Labs Software
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
	"math"
	"math/big"
	"testing"

	"github.com/blinklabs-io/dingo/database/types"
	"github.com/blinklabs-io/gouroboros/cbor"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestMultiAsset builds a single-policy, single-asset MultiAsset for a
// caller-supplied amount, bypassing CBOR decoding's pruneZeroAssets so
// out-of-range or negative values reach ConvertMultiAssetToModels untouched.
func newTestMultiAsset(
	policyId lcommon.Blake2b224,
	assetName string,
	amount *big.Int,
) *lcommon.MultiAsset[lcommon.MultiAssetTypeOutput] {
	multiAsset := lcommon.NewMultiAsset[lcommon.MultiAssetTypeOutput](
		map[lcommon.Blake2b224]map[cbor.ByteString]lcommon.MultiAssetTypeOutput{
			policyId: {
				cbor.NewByteString([]byte(assetName)): amount,
			},
		},
	)
	return &multiAsset
}

// TestConvertMultiAssetToModelsMaxUint64 covers the maximum in-range value:
// CheckedUint64FromBigInt must not reject anything that legitimately fits
// in a uint64.
func TestConvertMultiAssetToModelsMaxUint64(t *testing.T) {
	var policyId lcommon.Blake2b224
	policyId[0] = 0xaa
	amount := new(big.Int).SetUint64(math.MaxUint64)
	multiAsset := newTestMultiAsset(policyId, "asset", amount)

	assets, err := ConvertMultiAssetToModels(multiAsset)
	require.NoError(t, err)
	require.Len(t, assets, 1)
	assert.Equal(t, types.Uint64(math.MaxUint64), assets[0].Amount)
}

// TestConvertMultiAssetToModelsRejectsOverflow guards the regression this
// issue was filed for: one past math.MaxUint64, big.Int.Uint64 would
// silently keep only the low 64 bits (wrapping to 0) instead of reporting
// this.
func TestConvertMultiAssetToModelsRejectsOverflow(t *testing.T) {
	var policyId lcommon.Blake2b224
	policyId[0] = 0xbb
	amount := new(big.Int).SetUint64(math.MaxUint64)
	amount.Add(amount, big.NewInt(1))
	multiAsset := newTestMultiAsset(policyId, "asset", amount)

	_, err := ConvertMultiAssetToModels(multiAsset)
	assert.Error(t, err)
}

// TestConvertMultiAssetToModelsRejectsNegative covers a negative indexed
// amount, which big.Int.Uint64 would otherwise reinterpret as a huge
// positive value instead of reporting.
func TestConvertMultiAssetToModelsRejectsNegative(t *testing.T) {
	var policyId lcommon.Blake2b224
	policyId[0] = 0xcc
	amount := big.NewInt(-1)
	multiAsset := newTestMultiAsset(policyId, "asset", amount)

	_, err := ConvertMultiAssetToModels(multiAsset)
	assert.Error(t, err)
}

// TestConvertMultiAssetToModelsNilMultiAsset covers the pre-existing nil
// shortcut: it must keep returning an empty, non-nil slice with no error.
func TestConvertMultiAssetToModelsNilMultiAsset(t *testing.T) {
	assets, err := ConvertMultiAssetToModels(nil)
	require.NoError(t, err)
	assert.Empty(t, assets)
}

// TestCheckedUint64FromBigIntRejectsNil covers the defensive nil guard, so
// a caller passing a nil *big.Int fails loudly instead of panicking inside
// IsUint64/Uint64.
func TestCheckedUint64FromBigIntRejectsNil(t *testing.T) {
	_, err := CheckedUint64FromBigInt(nil)
	assert.Error(t, err)
}
