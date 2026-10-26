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

package mesh

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/database/models"
	"github.com/blinklabs-io/dingo/internal/test/openapiconformance"
	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/mesh-api-v1.4.15.json
var meshOpenAPI []byte

var meshImplementedRoutes = []openapiconformance.Route{
	{Operation: operation(http.MethodPost, "/account/balance")},
	{Operation: operation(http.MethodPost, "/account/coins")},
	{Operation: operation(http.MethodPost, "/block")},
	{Operation: operation(http.MethodPost, "/block/transaction")},
	{Operation: operation(http.MethodPost, "/construction/combine")},
	{Operation: operation(http.MethodPost, "/construction/derive")},
	{Operation: operation(http.MethodPost, "/construction/hash")},
	{Operation: operation(http.MethodPost, "/construction/metadata")},
	{Operation: operation(http.MethodPost, "/construction/parse")},
	{Operation: operation(http.MethodPost, "/construction/payloads")},
	{Operation: operation(http.MethodPost, "/construction/preprocess")},
	{Operation: operation(http.MethodPost, "/construction/submit")},
	{Operation: operation(http.MethodPost, "/mempool")},
	{Operation: operation(http.MethodPost, "/mempool/transaction")},
	{Operation: operation(http.MethodPost, "/network/list")},
	{Operation: operation(http.MethodPost, "/network/options")},
	{Operation: operation(http.MethodPost, "/network/status")},
}

var meshUnimplementedRoutes = map[openapiconformance.Operation]string{
	operation(http.MethodPost, "/call"):                "Not implemented",
	operation(http.MethodPost, "/events/blocks"):       "Not implemented",
	operation(http.MethodPost, "/search/transactions"): "Not implemented",
}

func TestOpenAPIConformance(t *testing.T) {
	t.Parallel()

	openapiconformance.AssertProvenance(t, meshOpenAPI, openapiconformance.Provenance{
		URL:      "https://github.com/coinbase/mesh-specifications/blob/7a41b1c73f9567947837871342c78e051c8a4847/api.json",
		Revision: "7a41b1c73f9567947837871342c78e051c8a4847",
		SHA256:   "00a8e2e09748aef67f3abbfc73f69eb3fe9fbcca7807d068bf3b9ec9b302c980",
	})
	document, err := openapiconformance.ParseJSON(meshOpenAPI)
	require.NoError(t, err)
	require.Equal(t, "1.4.13", document.Version())
	require.Equal(t, "1.4.15", rosettaVersion)

	recorder := &meshRouteRecorder{}
	(&Server{}).registerRoutes(recorder)
	document.AssertRoutes(
		t,
		recorder.operations,
		meshImplementedRoutes,
		meshUnimplementedRoutes,
	)

	for _, contract := range meshSchemaContracts() {
		t.Run(contract.operation.Method+" "+contract.operation.Path, func(t *testing.T) {
			t.Parallel()
			requestSchema, err := document.RequestSchema(contract.operation)
			require.NoError(t, err)
			document.AssertGoType(t, requestSchema, contract.request)
			responseSchema, err := document.ResponseSchema(
				contract.operation,
				http.StatusOK,
				"application/json",
			)
			require.NoError(t, err)
			document.AssertGoType(t, responseSchema, contract.response)
		})
	}
	assertMeshResponses(t, document)
	assertMeshErrorResponses(t, document)
}

func assertMeshResponses(
	t *testing.T,
	document openapiconformance.Document,
) {
	t.Helper()
	deps := newTestDeps()
	handler := newTestHandler(t, deps)
	responses := []struct {
		operation openapiconformance.Operation
		body      any
	}{
		{operation(http.MethodPost, "/network/list"), MetadataRequest{}},
		{operation(http.MethodPost, "/network/options"), NetworkRequest{networkIdentifierField: networkIdentifierField{NetworkIdentifier: testNetworkID()}}},
		{operation(http.MethodPost, "/network/status"), NetworkRequest{networkIdentifierField: networkIdentifierField{NetworkIdentifier: testNetworkID()}}},
		{operation(http.MethodPost, "/mempool"), NetworkRequest{networkIdentifierField: networkIdentifierField{NetworkIdentifier: testNetworkID()}}},
	}
	for _, contract := range responses {
		response := postJSON(t, handler, contract.operation.Path, contract.body)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var payload any
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payload))
		schema, err := document.ResponseSchema(
			contract.operation, http.StatusOK, "application/json",
		)
		require.NoError(t, err)
		require.NoError(t, document.ValidateJSON(schema, payload))
	}
}

// assertMeshErrorResponses checks that every error a handler writes uses a
// status the operation declares and an Error body that validates against it.
// The spec declares only 200 and 500, so any other status fails the lookup.
func assertMeshErrorResponses(
	t *testing.T,
	document openapiconformance.Document,
) {
	t.Helper()
	addr := testAddress(
		t, lcommon.AddressTypeKeyNone, testKeyHash(0x36), nil,
	)
	wrongNetwork := &NetworkIdentifier{
		Blockchain: blockchain,
		Network:    "not-" + testNetwork,
	}
	wrongNetworkCoins := coinsRequest(addr)
	wrongNetworkCoins.NetworkIdentifier = wrongNetwork

	deps := newTestDeps()
	handler := newTestHandler(t, deps)

	busy := newTestServer(t, newTestDeps())
	for range cap(busy.bulkSlots) {
		busy.bulkSlots <- struct{}{}
	}
	busyMux := http.NewServeMux()
	busy.registerRoutes(busyMux)

	oversizedDeps := newTestDeps()
	oversizedDeps.ledger.utxos = func(
		[]lcommon.Address,
	) ([]models.Utxo, error) {
		return []models.Utxo{
			testUtxo(testHash(1), 0, 1_000_000, testKeyHash(0x36), nil),
			testUtxo(testHash(2), 0, 1_000_000, testKeyHash(0x36), nil),
		}, nil
	}
	oversized := newTestHandler(t, oversizedDeps, func(c *ServerConfig) {
		c.maxAccountCoinUtxos = 1
	})

	cases := []struct {
		name      string
		handler   http.Handler
		operation openapiconformance.Operation
		body      string
		want      *Error
	}{
		{
			name:      "wrong network",
			handler:   handler,
			operation: operation(http.MethodPost, "/account/coins"),
			body:      mustJSON(t, wrongNetworkCoins),
			want:      ErrNetworkNotSupported,
		},
		{
			name:      "malformed body",
			handler:   handler,
			operation: operation(http.MethodPost, "/network/status"),
			body:      "{",
			want:      ErrInvalidRequest,
		},
		{
			name:      "missing account",
			handler:   handler,
			operation: operation(http.MethodPost, "/account/coins"),
			body:      mustJSON(t, coinsRequest("")),
			want:      ErrInvalidRequest,
		},
		{
			name:      "busy account coins",
			handler:   busyMux,
			operation: operation(http.MethodPost, "/account/coins"),
			body:      mustJSON(t, coinsRequest(addr)),
			want:      ErrUnavailable,
		},
		{
			name:      "oversized account coins",
			handler:   oversized,
			operation: operation(http.MethodPost, "/account/coins"),
			body:      mustJSON(t, coinsRequest(addr)),
			want:      ErrAccountTooLarge,
		},
	}
	for _, tc := range cases {
		t.Run("error "+tc.name, func(t *testing.T) {
			response := postRaw(t, tc.handler, tc.operation.Path, tc.body)
			schema, err := document.ResponseSchema(
				tc.operation, response.Code, "application/json",
			)
			require.NoError(t, err, response.Body.String())
			var payload any
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payload))
			require.NoError(t, document.ValidateJSON(schema, payload))
			got := requireMeshError(t, response, tc.want, response.Code)
			require.Equal(t, tc.want.Retriable, got.Retriable)
		})
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	return string(raw)
}

func TestAccountCoinsOpenAPIHasNoPagination(t *testing.T) {
	t.Parallel()

	document, err := openapiconformance.ParseJSON(meshOpenAPI)
	require.NoError(t, err)
	requestSchema, err := document.RequestSchema(
		operation(http.MethodPost, "/account/coins"),
	)
	require.NoError(t, err)
	encoded := strings.ToLower(string(meshOpenAPI))
	require.NotContains(t, encoded, "next_cursor")
	require.NotContains(t, encoded, `"paginate"`)
	document.AssertGoType(t, requestSchema, reflect.TypeFor[AccountCoinsRequest]())
}

type meshRouteRecorder struct {
	operations []openapiconformance.Operation
}

func (r *meshRouteRecorder) HandleFunc(
	pattern string,
	_ func(http.ResponseWriter, *http.Request),
) {
	method, path, ok := strings.Cut(pattern, " ")
	if !ok {
		method = http.MethodGet
		path = pattern
	}
	r.operations = append(r.operations, operation(method, path))
}

type meshSchemaContract struct {
	operation openapiconformance.Operation
	request   reflect.Type
	response  reflect.Type
}

func meshSchemaContracts() []meshSchemaContract {
	return []meshSchemaContract{
		{operation(http.MethodPost, "/network/list"), reflect.TypeFor[MetadataRequest](), reflect.TypeFor[NetworkListResponse]()},
		{operation(http.MethodPost, "/network/options"), reflect.TypeFor[NetworkRequest](), reflect.TypeFor[NetworkOptionsResponse]()},
		{operation(http.MethodPost, "/network/status"), reflect.TypeFor[NetworkRequest](), reflect.TypeFor[NetworkStatusResponse]()},
		{operation(http.MethodPost, "/block"), reflect.TypeFor[BlockRequest](), reflect.TypeFor[BlockResponse]()},
		{operation(http.MethodPost, "/block/transaction"), reflect.TypeFor[BlockTransactionRequest](), reflect.TypeFor[BlockTransactionResponse]()},
		{operation(http.MethodPost, "/mempool"), reflect.TypeFor[NetworkRequest](), reflect.TypeFor[MempoolResponse]()},
		{operation(http.MethodPost, "/mempool/transaction"), reflect.TypeFor[MempoolTransactionRequest](), reflect.TypeFor[MempoolTransactionResponse]()},
		{operation(http.MethodPost, "/account/balance"), reflect.TypeFor[AccountBalanceRequest](), reflect.TypeFor[AccountBalanceResponse]()},
		{operation(http.MethodPost, "/account/coins"), reflect.TypeFor[AccountCoinsRequest](), reflect.TypeFor[AccountCoinsResponse]()},
		{operation(http.MethodPost, "/construction/derive"), reflect.TypeFor[ConstructionDeriveRequest](), reflect.TypeFor[ConstructionDeriveResponse]()},
		{operation(http.MethodPost, "/construction/preprocess"), reflect.TypeFor[ConstructionPreprocessRequest](), reflect.TypeFor[ConstructionPreprocessResponse]()},
		{operation(http.MethodPost, "/construction/metadata"), reflect.TypeFor[ConstructionMetadataRequest](), reflect.TypeFor[ConstructionMetadataResponse]()},
		{operation(http.MethodPost, "/construction/payloads"), reflect.TypeFor[ConstructionPayloadsRequest](), reflect.TypeFor[ConstructionPayloadsResponse]()},
		{operation(http.MethodPost, "/construction/combine"), reflect.TypeFor[ConstructionCombineRequest](), reflect.TypeFor[ConstructionCombineResponse]()},
		{operation(http.MethodPost, "/construction/parse"), reflect.TypeFor[ConstructionParseRequest](), reflect.TypeFor[ConstructionParseResponse]()},
		{operation(http.MethodPost, "/construction/hash"), reflect.TypeFor[ConstructionHashRequest](), reflect.TypeFor[ConstructionHashResponse]()},
		{operation(http.MethodPost, "/construction/submit"), reflect.TypeFor[ConstructionSubmitRequest](), reflect.TypeFor[ConstructionSubmitResponse]()},
	}
}

func operation(method, path string) openapiconformance.Operation {
	return openapiconformance.Operation{Method: method, Path: path}
}
