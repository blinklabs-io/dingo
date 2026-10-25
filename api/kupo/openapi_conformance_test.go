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

package kupo

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/blinklabs-io/dingo/internal/test/openapiconformance"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/kupo-api-v2.12.0.yaml
var kupoOpenAPI []byte

func TestOpenAPIConformance(t *testing.T) {
	t.Parallel()

	openapiconformance.AssertProvenance(t, kupoOpenAPI, openapiconformance.Provenance{
		URL:      "https://github.com/CardanoSolutions/kupo/blob/491918a50b45ed2b3de682ee79e03c6dc09779a3/docs/api/v2.12.0.yaml",
		Revision: "491918a50b45ed2b3de682ee79e03c6dc09779a3",
		SHA256:   "39032f8f4ac8de32f4871fc13ded6148c330fb0bc6eac00aec83dec202ab1a68",
	})
	document, err := openapiconformance.ParseYAML(kupoOpenAPI)
	require.NoError(t, err)
	require.Equal(t, "v2.12.0", document.Version())
	canonical := make([]openapiconformance.Route, 0, len(document.Operations()))
	for _, operation := range document.Operations() {
		canonical = append(canonical, openapiconformance.Route{Operation: operation})
	}
	document.AssertRoutes(t, document.Operations(), canonical, nil)

	recorder := &kupoRouteRecorder{}
	registerRoutes(recorder, "", &Server{})
	registerRoutes(recorder, "/v1", &Server{})
	document.AssertRoutes(t, recorder.operations, kupoRoutes(), nil)

	for _, operation := range document.Operations() {
		headers, err := document.Parameters(operation, "header")
		require.NoError(t, err)
		require.Empty(t, headers, "%s %s", operation.Method, operation.Path)
	}
	assertKupoQueryParameters(t, document)
	assertKupoGoTypes(t, document)
	assertKupoResponses(t, document)
}

func assertKupoQueryParameters(
	t *testing.T,
	document openapiconformance.Document,
) {
	t.Helper()
	contracts := map[openapiconformance.Operation]map[string]bool{
		kupoOperation(http.MethodGet, "/matches"):               matchQueryParameters,
		kupoOperation(http.MethodGet, "/matches/{pattern}"):     matchQueryParameters,
		kupoOperation(http.MethodGet, "/checkpoints/{slot-no}"): checkpointQueryParameters,
		kupoOperation(http.MethodGet, "/metadata/{slot-no}"):    metadataQueryParameters,
	}
	for _, operation := range document.Operations() {
		document.AssertQueryParameters(t, operation, mapKeys(contracts[operation]))
	}
}

func assertKupoGoTypes(t *testing.T, document openapiconformance.Document) {
	t.Helper()
	contracts := []struct {
		operation   openapiconformance.Operation
		status      int
		contentType string
		typ         reflect.Type
	}{
		{kupoOperation(http.MethodGet, "/datums/{datum-hash}"), 200, jsonContentType, reflect.TypeFor[*Datum]()},
		{kupoOperation(http.MethodGet, "/scripts/{script-hash}"), 200, jsonContentType, reflect.TypeFor[*Script]()},
		{kupoOperation(http.MethodGet, "/checkpoints"), 200, jsonContentType, reflect.TypeFor[[]Point]()},
		{kupoOperation(http.MethodGet, "/checkpoints/{slot-no}"), 200, jsonContentType, reflect.TypeFor[*Point]()},
		{kupoOperation(http.MethodGet, "/metadata/{slot-no}"), 200, jsonContentType, reflect.TypeFor[[]Metadata]()},
		{kupoOperation(http.MethodGet, "/health"), 200, jsonContentType, reflect.TypeFor[Health]()},
	}
	for _, contract := range contracts {
		schema, err := document.ResponseSchema(
			contract.operation, contract.status, contract.contentType,
		)
		require.NoError(t, err)
		document.AssertGoType(t, schema, contract.typ)
	}
}

func assertKupoResponses(t *testing.T, document openapiconformance.Document) {
	t.Helper()
	hash := strings.Repeat("aa", 32)
	datumHash := strings.Repeat("bb", 32)
	scriptHash := strings.Repeat("cc", 28)
	node := &mockNode{
		tip:         Point{SlotNo: 42, HeaderHash: hash},
		snapshotTip: Point{SlotNo: 42, HeaderHash: hash},
		matches: []Match{{
			TransactionIndex: 1,
			TransactionID:    strings.Repeat("11", 32),
			OutputIndex:      2,
			Address:          "addr1vy3qpx09uscywhpp0ekg9zwmq2yj5vp08husfq6qyh2mpps865j6t",
			Value: Value{
				Coins:  123,
				Assets: map[string]uint64{strings.Repeat("22", 28) + ".01": 3},
			},
			DatumHash:  &datumHash,
			DatumType:  "inline",
			ScriptHash: &scriptHash,
			CreatedAt:  Point{SlotNo: 41, HeaderHash: strings.Repeat("33", 32)},
		}},
		datum:       &Datum{Datum: "d87980"},
		script:      &Script{Language: "plutus:v3", Script: "4d01"},
		checkpoints: []Point{{SlotNo: 42, HeaderHash: hash}},
		checkpoint:  &Point{SlotNo: 42, HeaderHash: hash},
		metadata: []Metadata{{
			Hash: strings.Repeat("44", 32), Raw: "a10001",
			Schema: map[string]any{"0": map[string]any{"int": 1}},
		}},
		healthCode: http.StatusOK,
	}
	node.health.ConnectionStatus = "connected"
	node.health.Configuration.Indexes = "installed"
	node.health.Version = "2.12.0"
	server := newTestServer(node)

	rollback := `{"rollback_to":{"slot_no":0}}`
	bulkRollback := `{"patterns":["*"],"rollback_to":{"slot_no":0}}`
	responses := []struct {
		operation openapiconformance.Operation
		target    string
		body      string
		status    int
	}{
		{kupoOperation(http.MethodGet, "/matches"), "/matches", "", 200},
		{kupoOperation(http.MethodGet, "/matches/{pattern}"), "/matches/*", "", 200},
		{kupoOperation(http.MethodDelete, "/matches/{pattern}"), "/matches/*", "", 400},
		{kupoOperation(http.MethodGet, "/datums/{datum-hash}"), "/datums/" + strings.Repeat("11", 32), "", 200},
		{kupoOperation(http.MethodGet, "/scripts/{script-hash}"), "/scripts/" + strings.Repeat("22", 28), "", 200},
		{kupoOperation(http.MethodGet, "/patterns"), "/patterns", "", 200},
		{kupoOperation(http.MethodPut, "/patterns"), "/patterns", bulkRollback, 200},
		{kupoOperation(http.MethodGet, "/patterns/{pattern}"), "/patterns/*", "", 200},
		{kupoOperation(http.MethodDelete, "/patterns/{pattern}"), "/patterns/*", "", 400},
		{kupoOperation(http.MethodPut, "/patterns/{pattern}"), "/patterns/*", rollback, 200},
		{kupoOperation(http.MethodGet, "/checkpoints"), "/checkpoints", "", 200},
		{kupoOperation(http.MethodGet, "/checkpoints/{slot-no}"), "/checkpoints/42", "", 200},
		{kupoOperation(http.MethodGet, "/metadata/{slot-no}"), "/metadata/42", "", 200},
		{kupoOperation(http.MethodGet, "/health"), "/health", "", 200},
	}
	for _, contract := range responses {
		t.Run(contract.operation.Method+" "+contract.operation.Path, func(t *testing.T) {
			response := serve(
				t, server, contract.operation.Method, contract.target,
				bytes.NewBufferString(contract.body),
			)
			require.Equal(t, contract.status, response.Code, response.Body.String())
			schema, err := document.ResponseSchema(
				contract.operation, contract.status,
				response.Header().Get("Content-Type"),
			)
			require.NoError(t, err)
			var payload any
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payload))
			require.NoError(t, document.ValidateJSON(schema, payload))
		})
	}

	metricsRequest := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRequest.Header.Set("Accept", "text/plain")
	metrics := httptest.NewRecorder()
	server.handler().ServeHTTP(metrics, metricsRequest)
	require.Equal(t, http.StatusOK, metrics.Code)
	metricsSchema, err := document.ResponseSchema(
		kupoOperation(http.MethodGet, "/metrics"), 200,
		metrics.Header().Get("Content-Type"),
	)
	require.NoError(t, err)
	require.NoError(t, document.ValidateJSON(metricsSchema, metrics.Body.String()))

	for _, body := range []struct {
		operation openapiconformance.Operation
		value     string
	}{
		{kupoOperation(http.MethodPut, "/patterns"), bulkRollback},
		{kupoOperation(http.MethodPut, "/patterns/{pattern}"), rollback},
	} {
		schema, err := document.RequestSchema(body.operation)
		require.NoError(t, err)
		var payload any
		require.NoError(t, json.Unmarshal([]byte(body.value), &payload))
		require.NoError(t, document.ValidateJSON(schema, payload))
	}
}

type kupoRouteRecorder struct {
	operations []openapiconformance.Operation
}

func (r *kupoRouteRecorder) HandleFunc(
	pattern string,
	_ func(http.ResponseWriter, *http.Request),
) {
	method, path, _ := strings.Cut(pattern, " ")
	r.operations = append(r.operations, kupoOperation(method, path))
}

func kupoRoutes() []openapiconformance.Route {
	// Kupo v2.12's App.Http.route accepts a recursive /v1 prefix and joins one
	// or two pattern path segments. These aliases are upstream behavior even
	// though docs/api/v2.12.0.yaml lists only the canonical templates:
	// https://github.com/CardanoSolutions/kupo/blob/491918a50b45ed2b3de682ee79e03c6dc09779a3/src/Kupo/App/Http.hs#L276-L464
	root := []openapiconformance.Route{
		{Operation: kupoOperation(http.MethodGet, "/matches")},
		{Operation: kupoOperation(http.MethodGet, "/matches/{pattern}")},
		{Operation: kupoOperation(http.MethodGet, "/matches/{pattern}"), RuntimePath: "/matches/{payment}/{delegation}", EquivalentPath: true},
		{Operation: kupoOperation(http.MethodDelete, "/matches/{pattern}")},
		{Operation: kupoOperation(http.MethodDelete, "/matches/{pattern}"), RuntimePath: "/matches/{payment}/{delegation}", EquivalentPath: true},
		{Operation: kupoOperation(http.MethodGet, "/datums/{datum-hash}"), RuntimePath: "/datums/{datum_hash}"},
		{Operation: kupoOperation(http.MethodGet, "/scripts/{script-hash}"), RuntimePath: "/scripts/{script_hash}"},
		{Operation: kupoOperation(http.MethodGet, "/patterns")},
		{Operation: kupoOperation(http.MethodPut, "/patterns")},
		{Operation: kupoOperation(http.MethodGet, "/patterns/{pattern}")},
		{Operation: kupoOperation(http.MethodGet, "/patterns/{pattern}"), RuntimePath: "/patterns/{payment}/{delegation}", EquivalentPath: true},
		{Operation: kupoOperation(http.MethodPut, "/patterns/{pattern}")},
		{Operation: kupoOperation(http.MethodPut, "/patterns/{pattern}"), RuntimePath: "/patterns/{payment}/{delegation}", EquivalentPath: true},
		{Operation: kupoOperation(http.MethodDelete, "/patterns/{pattern}")},
		{Operation: kupoOperation(http.MethodDelete, "/patterns/{pattern}"), RuntimePath: "/patterns/{payment}/{delegation}", EquivalentPath: true},
		{Operation: kupoOperation(http.MethodGet, "/checkpoints")},
		{Operation: kupoOperation(http.MethodGet, "/checkpoints/{slot-no}"), RuntimePath: "/checkpoints/{slot_no}"},
		{Operation: kupoOperation(http.MethodGet, "/metadata/{slot-no}"), RuntimePath: "/metadata/{slot_no}"},
		{Operation: kupoOperation(http.MethodGet, "/health")},
		{Operation: kupoOperation(http.MethodGet, "/metrics")},
	}
	ret := make([]openapiconformance.Route, 0, len(root)*2)
	ret = append(ret, root...)
	for _, route := range root {
		runtimePath := route.RuntimePath
		if runtimePath == "" {
			runtimePath = route.Path
		}
		route.RuntimePath = "/v1" + runtimePath
		ret = append(ret, route)
	}
	return ret
}

func kupoOperation(method, path string) openapiconformance.Operation {
	return openapiconformance.Operation{Method: method, Path: path}
}

func mapKeys(values map[string]bool) []string {
	ret := make([]string, 0, len(values))
	for value := range values {
		ret = append(ret, value)
	}
	return ret
}
