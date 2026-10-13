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

package blockfrost

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testScriptHash = "5ec5b0d7f2eeb3c21ba2e7b6e3d9a0b6a1c5c8d1f3e2a4b6c8d0e2f4"
	testDatumHash  = "923918e403bf43c34b4ef6b48eb2ee04babed17320d8d1b9ff9ad086e86f44ec"
)

func TestHandleScripts(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.scripts = []string{testScriptHash, "bb"}
	node.scriptTotal = 150
	w := serve(t, node, "/api/v0/scripts?count=2&page=2&order=desc")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(
		t,
		`[{"script_hash":"`+testScriptHash+`"},{"script_hash":"bb"}]`,
		w.Body.String(),
	)
	require.Equal(t, "150", w.Header().Get("X-Pagination-Count-Total"))
	require.Equal(t, "75", w.Header().Get("X-Pagination-Page-Total"))
	node.mu.Lock()
	require.Equal(t, PaginationParams{Count: 2, Page: 2, Order: "desc"}, node.lastScriptPage)
	node.mu.Unlock()

	w = serve(t, &mockNode{}, "/api/v0/scripts")
	require.JSONEq(t, `[]`, w.Body.String())
	w = serve(t, &mockNode{}, "/api/v0/scripts?count=101")
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleScript(t *testing.T) {
	t.Parallel()
	size := 42
	node := &mockNode{}
	node.scriptInfo = ScriptInfo{Hash: testScriptHash, Type: "plutusV2", SerialisedSize: &size}
	w := serve(t, node, "/api/v0/scripts/"+testScriptHash)
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"script_hash":"`+testScriptHash+`","type":"plutusV2","serialised_size":42}`, w.Body.String())
	want, _ := hex.DecodeString(testScriptHash)
	node.mu.Lock()
	require.Equal(t, want, node.lastScriptHash)
	node.mu.Unlock()

	node = &mockNode{}
	node.scriptInfo = ScriptInfo{Hash: testScriptHash, Type: "timelock"}
	w = serve(t, node, "/api/v0/scripts/"+testScriptHash)
	require.JSONEq(t, `{"script_hash":"`+testScriptHash+`","type":"timelock","serialised_size":null}`, w.Body.String())
}

func TestHandleScriptJSONAndCBOR(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.scriptJSON = json.RawMessage(`{"type":"sig","keyHash":"aa"}`)
	w := serve(t, node, "/api/v0/scripts/"+testScriptHash+"/json")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"json":{"type":"sig","keyHash":"aa"}}`, w.Body.String())

	w = serve(t, &mockNode{}, "/api/v0/scripts/"+testScriptHash+"/json")
	require.JSONEq(t, `{"json":null}`, w.Body.String())

	cbor := "4e4d0100"
	node = &mockNode{}
	node.scriptCBOR = &cbor
	w = serve(t, node, "/api/v0/scripts/"+testScriptHash+"/cbor")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"cbor":"4e4d0100"}`, w.Body.String())
	w = serve(t, &mockNode{}, "/api/v0/scripts/"+testScriptHash+"/cbor")
	require.JSONEq(t, `{"cbor":null}`, w.Body.String())
}

func TestHandleScriptRedeemers(t *testing.T) {
	t.Parallel()
	datumHash := "dd"
	node := &mockNode{}
	node.scriptRedeemers = []ScriptRedeemerInfo{
		{
			TxHash: "aa", TxIndex: 1, Purpose: "spend",
			RedeemerDataHash: "rd", DatumHash: &datumHash,
			UnitMem: "10", UnitSteps: "20", Fee: "30",
		},
		{
			TxHash: "bb", TxIndex: 0, Purpose: "mint",
			RedeemerDataHash: "re", UnitMem: "1", UnitSteps: "2", Fee: "3",
		},
	}
	node.scriptTotal = 2
	w := serve(t, node, "/api/v0/scripts/"+testScriptHash+"/redeemers?order=desc")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `[
{"tx_hash":"aa","tx_index":1,"purpose":"spend","redeemer_data_hash":"rd","datum_hash":"dd","unit_mem":"10","unit_steps":"20","fee":"30"},
{"tx_hash":"bb","tx_index":0,"purpose":"mint","redeemer_data_hash":"re","datum_hash":null,"unit_mem":"1","unit_steps":"2","fee":"3"}]`, w.Body.String())
	require.Equal(t, "2", w.Header().Get("X-Pagination-Count-Total"))
	node.mu.Lock()
	require.Equal(t, "desc", node.lastScriptPage.Order)
	node.mu.Unlock()
}

func TestHandleScriptUTXOs(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.scriptUTXOs = []ScriptUTXOInfo{{
		Address:             "addr1",
		TxHash:              "aa",
		OutputIndex:         2,
		Amount:              []AddressAmountInfo{{Unit: "lovelace", Quantity: "7"}},
		Block:               "bb",
		ReferenceScriptHash: testScriptHash,
	}}
	node.scriptTotal = 1
	w := serve(t, node, "/api/v0/scripts/"+testScriptHash+"/utxos")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `[{"address":"addr1","tx_hash":"aa","output_index":2,
"amount":[{"unit":"lovelace","quantity":"7"}],"block":"bb","data_hash":null,
"inline_datum":null,"reference_script_hash":"`+testScriptHash+`"}]`, w.Body.String())
	require.Equal(t, "1", w.Header().Get("X-Pagination-Page-Total"))
}

func TestHandleDatum(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.datumJSON = json.RawMessage(`{"int":42}`)
	node.datumCBOR = "182a"
	w := serve(t, node, "/api/v0/scripts/datum/"+testDatumHash)
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"json_value":{"int":42}}`, w.Body.String())
	w = serve(t, node, "/api/v0/scripts/datum/"+testDatumHash+"/cbor")
	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"cbor":"182a"}`, w.Body.String())
	want, _ := hex.DecodeString(testDatumHash)
	node.mu.Lock()
	require.Equal(t, want, node.lastScriptHash)
	node.mu.Unlock()
}

func TestHandleScriptRoutesValidateAndMapErrors(t *testing.T) {
	t.Parallel()
	scriptRoutes := []string{
		"", "/json", "/cbor", "/redeemers", "/utxos",
	}
	for _, route := range scriptRoutes {
		path := "/api/v0/scripts/" + testScriptHash + route
		for _, hash := range []string{"abcd", strings.Repeat("z", 56), testDatumHash} {
			w := serve(t, &mockNode{}, "/api/v0/scripts/"+hash+route)
			require.Equal(t, http.StatusBadRequest, w.Code, hash+route)
			require.Contains(t, w.Body.String(), "Invalid script hash.")
		}
		node := &mockNode{}
		node.scriptErr = ErrScriptNotFound
		w := serve(t, node, path)
		require.Equal(t, http.StatusNotFound, w.Code, route)
		require.Contains(t, w.Body.String(), "The requested component has not been found.")

		node = &mockNode{}
		node.scriptErr = errors.New("boom")
		w = serve(t, node, path)
		require.Equal(t, http.StatusInternalServerError, w.Code, route)
	}
	for _, route := range []string{"/redeemers", "/utxos"} {
		w := serve(t, &mockNode{}, "/api/v0/scripts/"+testScriptHash+route+"?page=0")
		require.Equal(t, http.StatusBadRequest, w.Code, route)
		require.Contains(t, w.Body.String(), "Invalid pagination parameters.")
	}

	for _, route := range []string{"", "/cbor"} {
		path := "/api/v0/scripts/datum/" + testDatumHash + route
		for _, hash := range []string{"abcd", strings.Repeat("z", 64), testScriptHash} {
			w := serve(t, &mockNode{}, "/api/v0/scripts/datum/"+hash+route)
			require.Equal(t, http.StatusBadRequest, w.Code, hash+route)
			require.Contains(t, w.Body.String(), "Invalid datum hash.")
		}
		node := &mockNode{}
		node.scriptErr = ErrDatumNotFound
		w := serve(t, node, path)
		require.Equal(t, http.StatusNotFound, w.Code, route)
		node = &mockNode{}
		node.scriptErr = errors.New("boom")
		w = serve(t, node, path)
		require.Equal(t, http.StatusInternalServerError, w.Code, route)
	}
}

// "datum" and a script hash both live under /scripts/{x}/..., and neither may
// capture the other's resources.
func TestHandleScriptRoutesDoNotShadowEachOther(t *testing.T) {
	t.Parallel()
	node := &mockNode{}
	node.datumJSON = json.RawMessage(`{}`)
	w := serve(t, node, "/api/v0/scripts/datum/json")
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "Invalid datum hash.")
	w = serve(t, node, "/api/v0/scripts/"+testScriptHash+"/unknown")
	require.Equal(t, http.StatusNotFound, w.Code)
	w = serve(t, node, "/api/v0/scripts/datum/"+testDatumHash+"/unknown")
	require.Equal(t, http.StatusNotFound, w.Code)
}
