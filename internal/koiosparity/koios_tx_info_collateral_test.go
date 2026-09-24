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

package koiosparity

// Covers KoiosTxInfoItem's phase-2 validity handling: which refs a
// transaction actually consumed and produced, and the two /tx_info response
// quirks that make reading them non-obvious -- valid_contract living under
// plutus_contracts rather than at the top level, and collateral_output's
// asset_list arriving as a JSON string instead of a JSON array.
//
// The payloads below are trimmed copies of real preview /tx_info responses
// (preview-koios.tosidrop.me), keeping the field shapes exactly as the API
// emits them.

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boolPtr(b bool) *bool { return &b }

// TestKoiosTxInfoItem_DecodesCollateralAndValidity pins the decode against
// the real response shape. A top-level "valid_contract" key does not exist
// in /tx_info at all, and collateral_output's asset_list is a string; a
// decoder that assumed either otherwise would read every transaction as
// valid, or fail outright on any transaction with a collateral return.
func TestKoiosTxInfoItem_DecodesCollateralAndValidity(t *testing.T) {
	const payload = `[{
	  "tx_hash": "aa",
	  "inputs": [{"tx_hash": "in0", "tx_index": 0, "asset_list": []}],
	  "outputs": [
	    {"tx_hash": "aa", "tx_index": 0, "value": "1000000",
	     "payment_addr": {"bech32": "addr_test1body0"},
	     "datum_hash": null, "inline_datum": null,
	     "reference_script": null, "asset_list": []},
	    {"tx_hash": "aa", "tx_index": 1, "value": "2000000",
	     "payment_addr": {"bech32": "addr_test1body1"},
	     "datum_hash": null, "inline_datum": null,
	     "reference_script": null, "asset_list": []}
	  ],
	  "collateral_inputs": [{"tx_hash": "col0", "tx_index": 3, "asset_list": []}],
	  "collateral_output": {
	    "tx_hash": "aa", "tx_index": 2, "value": "4277281629",
	    "payment_addr": {"bech32": "addr_test1collateralreturn"},
	    "datum_hash": null, "inline_datum": null, "reference_script": null,
	    "asset_list": "[]"
	  },
	  "plutus_contracts": [{"valid_contract": false, "script_hash": "ss"}]
	}]`

	var items []KoiosTxInfoItem
	require.NoError(t, json.Unmarshal([]byte(payload), &items))
	require.Len(t, items, 1)
	item := items[0]

	require.Len(t, item.PlutusContracts, 1)
	require.NotNil(t, item.PlutusContracts[0].ValidContract)
	assert.False(t, *item.PlutusContracts[0].ValidContract)
	assert.False(t, item.IsValid())

	require.NotNil(t, item.CollateralOutput)
	assert.Equal(t, 2, item.CollateralOutput.TxIndex)
	assert.Equal(t, "4277281629", item.CollateralOutput.Value)
	assert.Empty(
		t, item.CollateralOutput.AssetList,
		`collateral_output's "[]" string form must decode to an empty list`,
	)
	require.Len(t, item.CollateralInputs, 1)
	assert.Equal(t, "col0", item.CollateralInputs[0].TxHash)
}

// TestKoiosTxInfoAssetList_BothForms pins the two serialisations Koios uses
// for the same content, plus the null case.
func TestKoiosTxInfoAssetList_BothForms(t *testing.T) {
	const arrayForm = `[{"policy_id": "pp", "asset_name": "nn", "quantity": "5"}]`
	const stringForm = `"[{\"policy_id\": \"pp\", \"asset_name\": \"nn\", \"quantity\": \"5\"}]"`

	var fromArray, fromString, fromNull, fromEmptyString KoiosTxInfoAssetList
	require.NoError(t, json.Unmarshal([]byte(arrayForm), &fromArray))
	require.NoError(t, json.Unmarshal([]byte(stringForm), &fromString))
	require.NoError(t, json.Unmarshal([]byte(`null`), &fromNull))
	require.NoError(t, json.Unmarshal([]byte(`""`), &fromEmptyString))

	assert.Equal(t, fromArray, fromString)
	require.Len(t, fromArray, 1)
	assert.Equal(t, "pp", fromArray[0].PolicyID)
	assert.Equal(t, "5", fromArray[0].Quantity)
	assert.Empty(t, fromNull)
	assert.Empty(t, fromEmptyString)

	// Marshalling is always the array form, so a cached row written from
	// this struct reads back through the array branch.
	encoded, err := json.Marshal(fromString)
	require.NoError(t, err)
	assert.JSONEq(t, arrayForm, string(encoded))
}

// TestKoiosTxInfoItem_ConsumedProduced pins the semantics against gouroboros'
// Transaction.Consumed()/Produced(): body inputs and outputs when phase-2
// validation passed, collateral inputs and the collateral return when it
// failed.
func TestKoiosTxInfoItem_ConsumedProduced(t *testing.T) {
	body := KoiosTxInfoItem{
		TxHash:           "aa",
		Inputs:           []KoiosTxInfoUtxoRef{{TxHash: "in0", TxIndex: 0}},
		Outputs:          []KoiosTxInfoOutput{{TxHash: "aa", TxIndex: 0}},
		CollateralInputs: []KoiosTxInfoUtxoRef{{TxHash: "col0", TxIndex: 3}},
		CollateralOutput: &KoiosTxInfoOutput{TxHash: "aa", TxIndex: 1},
	}

	t.Run("no plutus contracts is valid", func(t *testing.T) {
		assert.True(t, body.IsValid())
		assert.Equal(t, body.Inputs, body.Consumed())
		assert.Equal(t, body.Outputs, body.Produced())
	})

	t.Run("valid_contract true is valid", func(t *testing.T) {
		item := body
		item.PlutusContracts = []KoiosTxInfoPlutusContract{
			{ValidContract: boolPtr(true)},
			{ValidContract: boolPtr(true)},
		}
		assert.True(t, item.IsValid())
		assert.Equal(t, item.Inputs, item.Consumed())
		assert.Equal(t, item.Outputs, item.Produced())
	})

	t.Run("no verdict reported is valid", func(t *testing.T) {
		item := body
		item.PlutusContracts = []KoiosTxInfoPlutusContract{{ValidContract: nil}}
		assert.True(
			t, item.IsValid(),
			"a null valid_contract is an unreported verdict, not an invalid one",
		)
	})

	t.Run("valid_contract false consumes collateral", func(t *testing.T) {
		item := body
		item.PlutusContracts = []KoiosTxInfoPlutusContract{
			{ValidContract: boolPtr(false)},
		}
		assert.False(t, item.IsValid())
		assert.Equal(t, item.CollateralInputs, item.Consumed())
		require.Len(t, item.Produced(), 1)
		assert.Equal(t, 1, item.Produced()[0].TxIndex)
	})

	t.Run("invalid with no collateral return produces nothing", func(t *testing.T) {
		item := body
		item.CollateralOutput = nil
		item.PlutusContracts = []KoiosTxInfoPlutusContract{
			{ValidContract: boolPtr(false)},
		}
		assert.False(t, item.IsValid())
		assert.Empty(t, item.Produced())
		assert.Equal(t, item.CollateralInputs, item.Consumed())
	})
}
