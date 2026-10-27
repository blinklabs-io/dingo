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

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// encodingJSONMaxDepth is encoding/json's fixed nesting limit; fixtures nest
// past it so they fail without elision.
const encodingJSONMaxDepth = 10000

// nestedNativeScript renders a native script as Koios does, as "all" levels
// each wrapping the next in an object and an array, so levels*2 containers
// deep.
func nestedNativeScript(levels int) string {
	var sb strings.Builder
	for range levels {
		sb.WriteString(`{"type":"all","scripts":[`)
	}
	sb.WriteString(`{"type":"sig","keyHash":"ff3efca65569f6b0b868a3d34abdb1ad8eccf745e0da71fa94fb4f18"}`)
	for range levels {
		sb.WriteString(`]}`)
	}
	return sb.String()
}

// nestedPlutusData renders Plutus data as a constructor field chain, two
// containers per level.
func nestedPlutusData(levels int) string {
	return strings.Repeat(`{"constructor":0,"fields":[`, levels) +
		`{"int":1}` + strings.Repeat(`]}`, levels)
}

func serveBody(t *testing.T, body string) *KoiosClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(body))
		},
	))
	t.Cleanup(srv.Close)
	return newTestKoiosClient(srv.URL)
}

// TestGetTxInfosDecodesValuesNestedPastJSONLimit reproduces the preprod
// chunk that failed with "exceeded max depth": a native script, a reference
// script and an inline datum each nested past encoding/json's limit must not
// stop the fields node-parity reads from decoding.
func TestGetTxInfosDecodesValuesNestedPastJSONLimit(t *testing.T) {
	t.Parallel()

	levels := encodingJSONMaxDepth/2 + 400
	const (
		realHash   = "f90dce5765108da976abdbb9fc618f9a6ffd9fa4d93b2f288eed1808545424c9"
		datumHash  = "923918e403bf43c34b4ef6b48eb2ee04babed17320d8d1b9ff9ad086e86f44ec"
		scriptHash = "ff3efca65569f6b0b868a3d34abdb1ad8eccf745e0da71fa94fb4f18"
	)
	// The first transaction is preprod's, trimmed to the fields read here,
	// with its native script regenerated at the same depth class.
	body := `[
		{"tx_hash":"` + realHash + `",
		 "inputs":[{"value":"4000000","tx_hash":"56f123b500ca30d51a3b65539a1a0980696531a4d53a416b485a4b9055ff0545","tx_index":0,"asset_list":[],"datum_hash":null,"inline_datum":null,"payment_addr":{"cred":"ff3efca65569f6b0b868a3d34abdb1ad8eccf745e0da71fa94fb4f18","bech32":"addr_test1wrlnal9x245ldv9cdz3axj4akxkcan8hghsd5u06jna57xqps7t9j"},"reference_script":null}],
		 "outputs":[{"value":"3123767","tx_hash":"` + realHash + `","tx_index":0,"asset_list":[],"datum_hash":null,"inline_datum":null,"payment_addr":{"cred":"d8188ff2e2bd2384524e2e52b9f0ee0dc19e50ab7baac6d771f6d45e","bech32":"addr_test1vrvp3rlju27j8pzjfch99w0sacxur8js4da643khw8mdghs2yklfz"},"reference_script":null}],
		 "collateral_inputs":[],"collateral_output":null,"plutus_contracts":[],
		 "native_scripts":[{"script_hash":"` + scriptHash + `","script_json":` + nestedNativeScript(levels) + `}]},
		{"tx_hash":"bbbb",
		 "inputs":[{"tx_hash":"cccc","tx_index":3}],
		 "outputs":[{"tx_hash":"bbbb","tx_index":0,"value":"2000000",
		   "payment_addr":{"bech32":"addr_test1x"},
		   "asset_list":[{"policy_id":"aa","asset_name":"01","quantity":"5"}],
		   "datum_hash":"` + datumHash + `",
		   "inline_datum":{"bytes":"d87980","value":` + nestedPlutusData(levels) + `},
		   "reference_script":{"hash":"` + scriptHash + `","type":"plutusV2","bytes":"4e4d","value":` + nestedPlutusData(levels) + `}}],
		 "plutus_contracts":[{"valid_contract":true,"input":{"datum":{"value":` + nestedPlutusData(levels) + `}}}]}
	]`
	require.False(t, json.Valid([]byte(body)),
		"the fixture must exceed encoding/json's nesting limit")

	items, err := serveBody(t, body).GetTxInfos(
		t.Context(), []string{realHash, "bbbb"},
	)
	require.NoError(t, err)
	require.Len(t, items, 2)

	require.Equal(t, []KoiosTxInfoUtxoRef{{
		TxHash:  "56f123b500ca30d51a3b65539a1a0980696531a4d53a416b485a4b9055ff0545",
		TxIndex: 0,
	}}, items[0].Consumed())
	produced := items[0].Produced()
	require.Len(t, produced, 1)
	require.Equal(t,
		"addr_test1vrvp3rlju27j8pzjfch99w0sacxur8js4da643khw8mdghs2yklfz|3123767",
		CanonicalKoiosUTxOEntry(produced[0]),
	)

	require.True(t, items[1].IsValid())
	require.Equal(t,
		[]KoiosTxInfoUtxoRef{{TxHash: "cccc", TxIndex: 3}},
		items[1].Consumed(),
	)
	out := items[1].Produced()
	require.Len(t, out, 1)
	require.NotNil(t, out[0].InlineDatum)
	require.Equal(t, "d87980", out[0].InlineDatum.Bytes)
	require.Equal(t,
		"addr_test1x|2000000|aa.01=5|datum=inline:"+datumHash+
			"|scriptref="+scriptHash,
		CanonicalKoiosUTxOEntry(out[0]),
	)
}

func TestGetTxInfosRejectsMalformedDeepResponse(t *testing.T) {
	t.Parallel()

	// The defect sits inside a region that elision replaces, so it must be
	// caught by the elision pass rather than hidden behind a null.
	body := `[{"tx_hash":"aaaa","inputs":[],"outputs":[],"native_scripts":` +
		strings.Replace(nestedNativeScript(encodingJSONMaxDepth), `"sig"`, `"sig",`, 1) +
		`}]`
	_, err := serveBody(t, body).GetTxInfos(t.Context(), []string{"aaaa"})
	require.ErrorContains(t, err, "koios /tx_info decode")
}

func TestElideDeepJSON(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		in, want string
	}{
		{`[]`, `[]`},
		{` {"a":[1,"x",true,null,-2.5e3]} `, ` {"a":[1,"x",true,null,-2.5e3]} `},
		{`[[[1]],2]`, `[[null],2]`},
		{`{"a":{"b":{"c":[]}},"d":{"e":1}}`, `{"a":{"b":null},"d":{"e":1}}`},
		{`[["\"]}[{",[[]]]]`, `[["\"]}[{",null]]`},
		{`[[{"k":"v"}, [1, [2]]], [3]]`, `[[null, null], [3]]`},
	} {
		got, err := elideDeepJSON([]byte(tc.in), 2)
		require.NoError(t, err, "input %s", tc.in)
		require.Equal(t, tc.want, string(got), "input %s", tc.in)
	}

	for _, bad := range []string{
		``,
		`   `,
		`[`,
		`[]]`,
		`[] []`,
		`[1,]`,
		`[,1]`,
		`{"a" 1}`,
		`{"a":}`,
		`{1:2}`,
		`{"a":1,}`,
		`[}`,
		`"unterminated`,
		`["bad \q escape"]`,
		"[\"raw\x01control\"]",
		`[nul]`,
		`[01]`,
		`[1 2]`,
		`[[[[1,]]]]`,
		`[[[[}]]]]`,
		`[[[["\q"]]]]`,
		`[[[[tru]]]]`,
		`[[[[1 2]]]]`,
	} {
		require.False(t, json.Valid([]byte(bad)), "fixture %q must be invalid", bad)
		_, err := elideDeepJSON([]byte(bad), 2)
		require.Error(t, err, "input %q must not validate", bad)
	}
}

func TestElideDeepJSONHandlesDepthIteratively(t *testing.T) {
	t.Parallel()

	const depth = 1_000_000
	in := strings.Repeat("[", depth) + strings.Repeat("]", depth)
	got, err := elideDeepJSON([]byte(in), koiosMaxDecodeDepth)
	require.NoError(t, err)
	want := strings.Repeat("[", koiosMaxDecodeDepth) + "null" +
		strings.Repeat("]", koiosMaxDecodeDepth)
	require.Equal(t, want, string(got))
}

// TestTxInfoReadPathsAreShallowerThanElisionDepth pins the invariant that
// makes elision lossless: every container GetTxInfos decodes into sits at or
// above koiosMaxDecodeDepth, so only values no field reads are replaced.
func TestTxInfoReadPathsAreShallowerThanElisionDepth(t *testing.T) {
	t.Parallel()

	depth := jsonContainerDepth(reflect.TypeFor[[]KoiosTxInfoItem]())
	require.Equal(t, 6, depth,
		"response array > item > outputs > output > asset_list > asset")
	require.Less(t, depth, koiosMaxDecodeDepth)
}

// jsonContainerDepth is the deepest JSON container decoding into typ can
// populate. The tx_info types are acyclic, so the recursion terminates.
func jsonContainerDepth(typ reflect.Type) int {
	switch typ.Kind() {
	case reflect.Pointer:
		return jsonContainerDepth(typ.Elem())
	case reflect.Slice, reflect.Array, reflect.Map:
		return 1 + jsonContainerDepth(typ.Elem())
	case reflect.Struct:
		deepest := 0
		for f := range typ.Fields() {
			if !f.IsExported() || f.Tag.Get("json") == "-" {
				continue
			}
			deepest = max(deepest, jsonContainerDepth(f.Type))
		}
		return 1 + deepest
	default:
		return 0
	}
}
