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

package koiosparity

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

// realCollateralOutputShowForm is the verbatim collateral_output.asset_list
// string returned by the live preview Koios mirror
// (https://preview-koios.tosidrop.me/api/v1) for transaction
// c2c84d18534c49ef8a383f7ff24d62c0b9bb7cf887ea42e1e9b1bf24807f00bf, captured
// while diagnosing the ~50%-of-epochs UTxO-check skip.
//
// It is cardano-ledger's Haskell `Show` rendering of the output's MultiAsset,
// NOT JSON -- feeding it to json.Unmarshal fails on the first "(" with
// "invalid character '(' looking for beginning of value", which is the error
// that was failing whole 40-transaction chunks in production.
const realCollateralOutputShowForm = `[(PolicyID {policyID = ScriptHash "09e56a1dcecb140f4416b48f5ca475aab638d64fcb2f76e9d6496c0e"},[("474f565f4e4654",1)]),(PolicyID {policyID = ScriptHash "65a938b778af9a654b2a005d8ea902485f16a878ff81c458eaa7bdbb"},[("494e4459",32200000000000)])]`

// TestAssetListDecodesLedgerShowMultiAsset is the regression test for the
// collateral_output.asset_list decode failure. Every input here is a verbatim
// capture from the live preview API.
//
// Before the fix this test fails on every non-empty case with
// "invalid character '(' looking for beginning of value".
func TestAssetListDecodesLedgerShowMultiAsset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want KoiosTxInfoAssetList
	}{
		{
			// Empty-but-non-nil, exactly as before the fix: this shape is
			// what gets marshalled back into a koios_tx_info cache row, so
			// it must not change (see
			// TestAssetListRoundTripsThroughCachePayload).
			name: "empty list (the case that always worked)",
			in:   `"[]"`,
			want: KoiosTxInfoAssetList{},
		},
		{
			name: "single policy, single asset",
			in:   `"[(PolicyID {policyID = ScriptHash \"ed133dc2813622728057b951e4c4567d72bb1d78dba55c3a37184247\"},[(\"494e4459\",32200000000000)])]"`,
			want: KoiosTxInfoAssetList{{
				PolicyID:  "ed133dc2813622728057b951e4c4567d72bb1d78dba55c3a37184247",
				AssetName: "494e4459",
				Quantity:  "32200000000000",
			}},
		},
		{
			name: "two policies (real tx c2c84d18...)",
			in:   mustJSONString(t, realCollateralOutputShowForm),
			want: KoiosTxInfoAssetList{
				{
					PolicyID:  "09e56a1dcecb140f4416b48f5ca475aab638d64fcb2f76e9d6496c0e",
					AssetName: "474f565f4e4654",
					Quantity:  "1",
				},
				{
					PolicyID:  "65a938b778af9a654b2a005d8ea902485f16a878ff81c458eaa7bdbb",
					AssetName: "494e4459",
					Quantity:  "32200000000000",
				},
			},
		},
		{
			name: "six policies (real tx e36966d1...)",
			in: mustJSONString(t, `[(PolicyID {policyID = ScriptHash "3cc8d01846f7288fb42b750bb2afaabe2db04c71a53ab5a5ed4be90f"},[("55504752414445",1)]),(PolicyID {policyID = ScriptHash "4d3b6f594725489d9d5f6d167b2fbe1cf39c4138befc6b0a76b7ac56"},[("504f4c4c5f4d414e41474552",1)]),(PolicyID {policyID = ScriptHash "55a68b2630c4f47523e369a87a8263074fd5349fed89f294d1b9b2d2"},[("474f565f4e4654",1)]),(PolicyID {policyID = ScriptHash "961c251cf3560569c9dc25342d9687af84f1ac46728b7a7c820cd3d7"},[("494153534554",1)]),(PolicyID {policyID = ScriptHash "bfc00a0a8626cde875f237e621482d04cbf9d67849874d98666d9855"},[("53544142494c4954595f504f4f4c",1)]),(PolicyID {policyID = ScriptHash "ed133dc2813622728057b951e4c4567d72bb1d78dba55c3a37184247"},[("494e4459",32200000000000)])]`),
			want: KoiosTxInfoAssetList{
				{PolicyID: "3cc8d01846f7288fb42b750bb2afaabe2db04c71a53ab5a5ed4be90f", AssetName: "55504752414445", Quantity: "1"},
				{PolicyID: "4d3b6f594725489d9d5f6d167b2fbe1cf39c4138befc6b0a76b7ac56", AssetName: "504f4c4c5f4d414e41474552", Quantity: "1"},
				{PolicyID: "55a68b2630c4f47523e369a87a8263074fd5349fed89f294d1b9b2d2", AssetName: "474f565f4e4654", Quantity: "1"},
				{PolicyID: "961c251cf3560569c9dc25342d9687af84f1ac46728b7a7c820cd3d7", AssetName: "494153534554", Quantity: "1"},
				{PolicyID: "bfc00a0a8626cde875f237e621482d04cbf9d67849874d98666d9855", AssetName: "53544142494c4954595f504f4f4c", Quantity: "1"},
				{PolicyID: "ed133dc2813622728057b951e4c4567d72bb1d78dba55c3a37184247", AssetName: "494e4459", Quantity: "32200000000000"},
			},
		},
		{
			name: "multiple assets under one policy",
			in:   mustJSONString(t, `[(PolicyID {policyID = ScriptHash "aa"},[("01",5),("02",7)])]`),
			want: KoiosTxInfoAssetList{
				{PolicyID: "aa", AssetName: "01", Quantity: "5"},
				{PolicyID: "aa", AssetName: "02", Quantity: "7"},
			},
		},
		{
			name: "empty asset name",
			in:   mustJSONString(t, `[(PolicyID {policyID = ScriptHash "aa"},[("",3)])]`),
			want: KoiosTxInfoAssetList{
				{PolicyID: "aa", AssetName: "", Quantity: "3"},
			},
		},
		{
			name: "array form still decodes (inputs/outputs/cached rows)",
			in:   `[{"policy_id":"aa","asset_name":"01","quantity":"9"}]`,
			want: KoiosTxInfoAssetList{
				{PolicyID: "aa", AssetName: "01", Quantity: "9"},
			},
		},
		{
			name: "null",
			in:   `null`,
			want: nil,
		},
		{
			name: "empty string",
			in:   `""`,
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var got KoiosTxInfoAssetList
			require.NoError(t, json.Unmarshal([]byte(tt.in), &got))
			require.Equal(t, tt.want, got)
		})
	}
}

// TestAssetListShowFormMatchesArrayFormForTheSameTokens cross-checks the two
// serialisations against each other using real data for transaction
// c2c84d18...: its collateral_inputs report these exact tokens in the JSON
// array form, and its collateral_output reports them in the Show form. Both
// must produce identical KoiosTxInfoAssets, since CanonicalKoiosUTxOEntry
// compares them against Dingo's own decode of the same UTxO.
func TestAssetListShowFormMatchesArrayFormForTheSameTokens(t *testing.T) {
	t.Parallel()

	arrayForm := `[
		{"decimals":0,"quantity":"1","policy_id":"09e56a1dcecb140f4416b48f5ca475aab638d64fcb2f76e9d6496c0e","asset_name":"474f565f4e4654","fingerprint":"asset1sy4k02hdjqcdl0kq309ujqtnt3d0mjzqzlrzy5"},
		{"decimals":0,"quantity":"32200000000000","policy_id":"65a938b778af9a654b2a005d8ea902485f16a878ff81c458eaa7bdbb","asset_name":"494e4459","fingerprint":"asset17pxgv9fap9zfqmykvlk8s643z2wd9u78lw88a0"}
	]`

	var fromArray, fromShow KoiosTxInfoAssetList
	require.NoError(t, json.Unmarshal([]byte(arrayForm), &fromArray))
	require.NoError(t, json.Unmarshal(
		[]byte(mustJSONString(t, realCollateralOutputShowForm)), &fromShow,
	))
	require.Equal(t, fromArray, fromShow)
}

// TestAssetListRoundTripsThroughCachePayload proves the fix needs no
// koiosTxInfoPayloadVersion bump: what is STORED is unchanged. A decoded
// asset list marshals to the plain array form, and reading that back yields
// the same assets, so every koios_tx_info row already written at version 2
// stays valid. (The bug only ever prevented rows from being written at all --
// a chunk containing an undecodable transaction cached nothing -- so no
// stored row can hold a mis-parsed collateral asset list.)
func TestAssetListRoundTripsThroughCachePayload(t *testing.T) {
	t.Parallel()

	var decoded KoiosTxInfoAssetList
	require.NoError(t, json.Unmarshal(
		[]byte(mustJSONString(t, realCollateralOutputShowForm)), &decoded,
	))
	require.Len(t, decoded, 2)

	stored, err := json.Marshal(decoded)
	require.NoError(t, err)
	require.JSONEq(t,
		`[{"policy_id":"09e56a1dcecb140f4416b48f5ca475aab638d64fcb2f76e9d6496c0e","asset_name":"474f565f4e4654","quantity":"1"},`+
			`{"policy_id":"65a938b778af9a654b2a005d8ea902485f16a878ff81c458eaa7bdbb","asset_name":"494e4459","quantity":"32200000000000"}]`,
		string(stored),
	)

	var reread KoiosTxInfoAssetList
	require.NoError(t, json.Unmarshal(stored, &reread))
	require.Equal(t, decoded, reread)
}

// TestAssetListRejectsUnrecognisedStringFormLoudly preserves the safety
// property. A string form this parser genuinely cannot read must still be an
// error -- never silently an empty asset list, which would drop tokens from
// the reconstructed UTxO and turn a parser gap into a phantom Dingo
// mismatch. The error must carry the offending text.
func TestAssetListRejectsUnrecognisedStringFormLoudly(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{
		`"[(SomethingElse {x = 1},[(\"01\",2)])]"`,
		`"[(PolicyID {policyID = ScriptHash \"aa\"},[(\"01\",)])]"`,
		`"[(PolicyID {policyID = ScriptHash \"aa\"},[(\"01\",2)])"`,
		`"not a list at all"`,
	} {
		var got KoiosTxInfoAssetList
		err := json.Unmarshal([]byte(bad), &got)
		require.Error(t, err, "input %s must not decode silently", bad)
		require.Contains(t, err.Error(), "asset_list")
		require.Nil(t, got)
	}
}

// TestGetTxInfosNamesOffendingTransactionOnDecodeFailure pins the error
// context improvement: an undecodable transaction must be identified by hash
// in the error, instead of the bare "koios /tx_info decode: invalid character
// ..." that gave no clue which of 40 transactions (or which field) was at
// fault. The chunk still fails -- see describeTxInfoDecodeFailure for why
// that is deliberate.
func TestGetTxInfosNamesOffendingTransactionOnDecodeFailure(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			// Second transaction carries an asset_list string this parser
			// cannot read.
			_, _ = w.Write([]byte(`[
				{"tx_hash":"aaaa","outputs":[],"inputs":[]},
				{"tx_hash":"bbbb","collateral_output":{"tx_hash":"bbbb","tx_index":0,"value":"1","asset_list":"[(Mystery {})]"}}
			]`))
		},
	))
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	_, err := k.GetTxInfos(t.Context(), []string{"aaaa", "bbbb"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "bbbb",
		"the error must name the offending transaction")
	require.Contains(t, err.Error(), "asset_list",
		"the error must name the offending field")
	require.Contains(t, err.Error(), "Mystery",
		"the error must include the text that failed to parse")
}

// TestGetTxInfosDecodesRealCollateralChunk is the end-to-end proof: a chunk
// containing a transaction with a token-bearing collateral return decodes
// completely, where before the fix the whole chunk failed -- which is what
// tainted the epoch and skipped its UTxO comparison.
func TestGetTxInfosDecodesRealCollateralChunk(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			body := `[
				{"tx_hash":"aaaa","inputs":[],"outputs":[{"tx_hash":"aaaa","tx_index":0,"value":"1000000","asset_list":[]}]},
				{"tx_hash":"bbbb","inputs":[],"outputs":[],
				 "collateral_output":{"tx_hash":"bbbb","tx_index":2,"value":"956015319","asset_list":` +
				mustJSONString(t, realCollateralOutputShowForm) + `},
				 "plutus_contracts":[{"valid_contract":false}]}
			]`
			_, _ = w.Write([]byte(body))
		},
	))
	defer srv.Close()

	k := newTestKoiosClient(srv.URL)
	items, err := k.GetTxInfos(t.Context(), []string{"aaaa", "bbbb"})
	require.NoError(t, err)
	require.Len(t, items, 2)

	collateral := items[1].CollateralOutput
	require.NotNil(t, collateral)
	require.Len(t, collateral.AssetList, 2)
	require.Equal(t, "32200000000000", collateral.AssetList[1].Quantity)

	// The phase-2-invalid transaction's produced UTxO is its collateral
	// return, and its canonical encoding must carry the tokens -- the whole
	// reason this field has to decode correctly.
	produced := items[1].Produced()
	require.Len(t, produced, 1)
	require.Contains(t,
		CanonicalKoiosUTxOEntry(produced[0]),
		"65a938b778af9a654b2a005d8ea902485f16a878ff81c458eaa7bdbb.494e4459=32200000000000",
	)
}

func mustJSONString(t *testing.T, s string) string {
	t.Helper()
	b, err := json.Marshal(s)
	require.NoError(t, err)
	return string(b)
}
