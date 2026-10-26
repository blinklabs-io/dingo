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
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	lcommon "github.com/blinklabs-io/gouroboros/ledger/common"
	"github.com/btcsuite/btcd/btcutil/bech32"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const pinnedBlockfrostOpenAPIVersion = "0.1.93"

var openAPIPathParameter = regexp.MustCompile(`\{[^}]+\}`)

type openAPIDocument struct {
	Info struct {
		Version string `yaml:"version"`
	} `yaml:"info"`
	Paths map[string]map[string]any `yaml:"paths"`
	raw   map[string]any
}

type openAPIOperation struct {
	method string
	path   string
	value  map[string]any
}

func loadPinnedBlockfrostOpenAPI(t *testing.T) openAPIDocument {
	t.Helper()
	data, err := os.ReadFile("testdata/openapi-v0.1.93.yaml")
	require.NoError(t, err)
	var doc openAPIDocument
	require.NoError(t, yaml.Unmarshal(data, &doc))
	require.Equal(t, pinnedBlockfrostOpenAPIVersion, doc.Info.Version)
	require.NoError(t, yaml.Unmarshal(data, &doc.raw))
	return doc
}

func (d openAPIDocument) operations() []openAPIOperation {
	ret := make([]openAPIOperation, 0)
	for path, item := range d.Paths {
		for method, rawValue := range item {
			method = strings.ToUpper(method)
			value, ok := rawValue.(map[string]any)
			if !ok {
				continue
			}
			switch method {
			case http.MethodGet, http.MethodPost, http.MethodPut,
				http.MethodPatch, http.MethodDelete:
				ret = append(ret, openAPIOperation{
					method: method,
					path:   "/api/v0" + path,
					value:  value,
				})
			}
		}
	}
	return ret
}

func (o openAPIOperation) key() string {
	return o.method + " " + o.path
}

func (o openAPIOperation) samplePath() string {
	return openAPIPathParameter.ReplaceAllString(o.path, "sample")
}

// validSamplePath fills each path parameter with a value its handler accepts,
// so a request reaches query validation instead of failing on the path.
func (o openAPIOperation) validSamplePath(t *testing.T) string {
	t.Helper()
	policy := strings.Repeat("ab", 28)
	committee, err := bech32.ConvertBits(
		append(
			[]byte{committeeCredentialHeader(false, false)},
			make([]byte, 28)...),
		8,
		5,
		true,
	)
	require.NoError(t, err)
	committeeID, err := bech32.Encode("cc_hot", committee)
	require.NoError(t, err)
	govActionID := lcommon.GovActionId{}
	govAction, err := govActionID.MarshalText()
	require.NoError(t, err)
	samples := map[string]string{
		"address":        "addr1vx2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzers66hrl8",
		"asset":          policy + "6e7574",
		"cc_id":          committeeID,
		"cert_index":     "0",
		"datum_hash":     strings.Repeat("ab", 32),
		"drep_id":        "drep1y2zpa7e9f23g7snuhfqss73ufeshsqhdt4qqfu74p9y4kfqcpsxym",
		"epoch_number":   "1",
		"gov_action_id":  string(govAction),
		"hash":           strings.Repeat("ab", 32),
		"hash_or_number": "1",
		"label":          "1",
		"number":         "1",
		"policy_id":      policy,
		"pool_id":        "pool1vzqtn3mtfvvuy8ghksy34gs9g97tszj5f8mr3sn7asy5vk577ec",
		"script_hash":    policy,
		"slot_number":    "1",
		"stake_address":  "stake1u98nnlkvkk23vtvf9273uq7cph5ww6u2yq2389psuqet90sv4xv9v",
		"tx_hash":        strings.Repeat("ab", 32),
	}
	return openAPIPathParameter.ReplaceAllStringFunc(
		o.path,
		func(m string) string {
			name := strings.Trim(m, "{}")
			sample, ok := samples[name]
			require.True(t, ok, "no valid sample for path parameter %q", name)
			return sample
		},
	)
}

func registeredBlockfrostOperations(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "blockfrost.go", nil, 0)
	require.NoError(t, err)
	ret := make([]string, 0)
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok ||
			(selector.Sel.Name != "Handle" && selector.Sel.Name != "HandleFunc") {
			return true
		}
		ident, ok := selector.X.(*ast.Ident)
		if !ok || ident.Name != "mux" {
			return true
		}
		literal, ok := call.Args[0].(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		pattern, err := strconv.Unquote(literal.Value)
		require.NoError(t, err)
		if strings.Contains(pattern, " ") {
			ret = append(ret, pattern)
		}
		return true
	})
	return ret
}

func operationMux(t *testing.T, patterns []string) *http.ServeMux {
	t.Helper()
	mux := http.NewServeMux()
	for _, pattern := range patterns {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})
	}
	return mux
}

func muxMatchesOperation(mux *http.ServeMux, operation openAPIOperation) bool {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		operation.method,
		operation.samplePath(),
		nil,
	)
	mux.ServeHTTP(recorder, request)
	return recorder.Code == http.StatusNoContent
}

func unsupportedMatchesOperation(operation openAPIOperation) bool {
	for _, candidate := range unsupportedOperations {
		method, path, _ := strings.Cut(candidate, " ")
		if method == operation.method &&
			matchesOperationPath(path, operation.samplePath()) {
			return true
		}
	}
	return false
}

func TestPinnedOpenAPICoversEveryOperation(t *testing.T) {
	doc := loadPinnedBlockfrostOpenAPI(t)
	operations := doc.operations()
	patterns := registeredBlockfrostOperations(t)
	mux := operationMux(t, patterns)

	for _, operation := range operations {
		t.Run(operation.key(), func(t *testing.T) {
			supported := muxMatchesOperation(mux, operation)
			unsupported := unsupportedMatchesOperation(operation)
			require.NotEqual(
				t,
				supported,
				unsupported,
				"operation must be implemented or explicitly unsupported, but not both",
			)
		})
	}

	for _, pattern := range patterns {
		method, path, _ := strings.Cut(pattern, " ")
		if !strings.HasPrefix(path, "/api/v0") {
			continue
		}
		single := operationMux(t, []string{pattern})
		matched := false
		for _, operation := range operations {
			if operation.method == method &&
				muxMatchesOperation(single, operation) {
				matched = true
				break
			}
		}
		require.True(
			t,
			matched,
			"registered operation is absent from pinned OpenAPI: %s",
			pattern,
		)
	}

	for _, candidate := range unsupportedOperations {
		_, path, _ := strings.Cut(candidate, " ")
		if !strings.HasPrefix(path, "/api/v0") {
			continue
		}
		matched := false
		for _, operation := range operations {
			if unsupportedMatchesCandidate(candidate, operation) {
				matched = true
				break
			}
		}
		require.True(
			t,
			matched,
			"unsupported operation is absent from pinned OpenAPI: %s",
			candidate,
		)
	}
}

func unsupportedMatchesCandidate(
	candidate string,
	operation openAPIOperation,
) bool {
	method, path, _ := strings.Cut(candidate, " ")
	return method == operation.method &&
		matchesOperationPath(path, operation.samplePath())
}

func TestPinnedOpenAPIQueryBounds(t *testing.T) {
	doc := loadPinnedBlockfrostOpenAPI(t)
	patterns := registeredBlockfrostOperations(t)
	mux := operationMux(t, patterns)
	handler := newTestBlockfrost(&mockNode{}).handler()

	for _, operation := range doc.operations() {
		if !muxMatchesOperation(mux, operation) {
			continue
		}
		parameters, _ := operation.value["parameters"].([]any)
		for _, rawParameter := range parameters {
			parameter, _ := rawParameter.(map[string]any)
			if parameter["in"] != "query" {
				continue
			}
			name, _ := parameter["name"].(string)
			schema, _ := parameter["schema"].(map[string]any)
			invalid, ok := invalidQueryValue(schema)
			if !ok {
				continue
			}
			t.Run(operation.key()+" "+name, func(t *testing.T) {
				baseline := httptest.NewRecorder()
				handler.ServeHTTP(baseline, httptest.NewRequest(
					operation.method,
					operation.validSamplePath(t),
					nil,
				))
				require.NotEqual(
					t,
					http.StatusBadRequest,
					baseline.Code,
					"sample path is rejected before query validation",
				)
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(
					operation.method,
					operation.validSamplePath(t)+"?"+name+"="+invalid,
					nil,
				)
				handler.ServeHTTP(recorder, request)
				require.Equal(t, http.StatusBadRequest, recorder.Code)
			})
		}
	}
}

func TestPinnedOpenAPIResponseContracts(t *testing.T) {
	doc := loadPinnedBlockfrostOpenAPI(t)
	patterns := registeredBlockfrostOperations(t)
	mux := operationMux(t, patterns)
	handler := newTestBlockfrost(openAPIContractMockNode()).handler()

	for _, operation := range doc.operations() {
		if !muxMatchesOperation(mux, operation) {
			continue
		}
		t.Run(operation.key(), func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(
				operation.method,
				operation.validSamplePath(t),
				strings.NewReader(""),
			)
			request.Header.Set("Authorization", "Bearer "+testMutationAuthToken)
			request.Header.Set(
				"Content-Type",
				operationRequestContentType(operation),
			)
			handler.ServeHTTP(recorder, request)
			// Every operation without a request body must reach its documented
			// success schema. One with a body is sent empty, which is invalid
			// input, so its error schema is what can be checked here.
			if _, hasBody := operation.value["requestBody"]; hasBody {
				require.Equal(t, http.StatusBadRequest, recorder.Code)
			} else {
				require.True(
					t,
					recorder.Code >= 200 && recorder.Code < 300,
					"status %d, want success: %s",
					recorder.Code,
					recorder.Body.String(),
				)
			}

			response := operationResponse(t, doc, operation, recorder.Code)
			content, _ := response["content"].(map[string]any)
			if len(content) == 0 || recorder.Body.Len() == 0 {
				return
			}
			contentType, _, err := mime.ParseMediaType(
				recorder.Header().Get("Content-Type"),
			)
			require.NoError(t, err)
			schemaContent, ok := content[contentType].(map[string]any)
			require.True(
				t,
				ok,
				"response media type %q is absent from pinned OpenAPI",
				contentType,
			)
			schema, _ := schemaContent["schema"].(map[string]any)
			if contentType != "application/json" || len(schema) == 0 {
				return
			}
			var value any
			require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &value))
			require.NoError(
				t,
				validateOpenAPIValue(doc.raw, schema, value, "$"),
				"response body: %s",
				recorder.Body.String(),
			)
		})
	}
}

func openAPIContractMockNode() *mockNode {
	node := &mockNode{
		queryMock: queryMock{
			addressExtended: AddressExtendedInfo{
				Amount: []AddressExtendedAmountInfo{},
				Type:   "shelley",
			},
		},
		addressInfo: AddressInfo{
			Amount: []AddressAmountInfo{},
			Type:   "shelley",
		},
	}
	// A zero-valued enum or datum is not a documented value and an asset with
	// no holders reads as not found, so these carry schema-valid values.
	node.assetHolders = []AssetHolderInfo{{
		Address:  "addr1vx2fxv2umyhttkxyxp8x0dlpdt3k6cwng5pxj3jhsydzers66hrl8",
		Quantity: "1",
	}}
	node.assetHoldersTotal = 1
	node.scriptInfo = ScriptInfo{Type: "timelock"}
	node.datumJSON = json.RawMessage(`{"int":42}`)
	node.proposal.value = GovernanceProposalDetailInfo{
		GovernanceProposalInfo: GovernanceProposalInfo{
			GovernanceType: "info_action",
		},
	}
	return node
}

func operationRequestContentType(operation openAPIOperation) string {
	requestBody, _ := operation.value["requestBody"].(map[string]any)
	content, _ := requestBody["content"].(map[string]any)
	for _, preferred := range []string{"application/json", "application/cbor"} {
		if _, ok := content[preferred]; ok {
			return preferred
		}
	}
	contentTypes := make([]string, 0, len(content))
	for contentType := range content {
		contentTypes = append(contentTypes, contentType)
	}
	if len(contentTypes) > 0 {
		sort.Strings(contentTypes)
		return contentTypes[0]
	}
	return "application/json"
}

func operationResponse(
	t *testing.T,
	doc openAPIDocument,
	operation openAPIOperation,
	status int,
) map[string]any {
	t.Helper()
	responses, ok := operation.value["responses"].(map[string]any)
	require.True(t, ok, "operation has no response map")
	raw, ok := responses[strconv.Itoa(status)]
	if !ok {
		raw, ok = responses["default"]
	}
	require.True(t, ok, "status %d is absent from pinned OpenAPI", status)
	response, ok := raw.(map[string]any)
	require.True(t, ok, "response %d is not an object", status)
	resolved, err := resolveOpenAPIRef(doc.raw, response)
	require.NoError(t, err)
	return resolved
}

func resolveOpenAPIRef(root, value map[string]any) (map[string]any, error) {
	ref, _ := value["$ref"].(string)
	if ref == "" {
		return value, nil
	}
	const prefix = "#/"
	if !strings.HasPrefix(ref, prefix) {
		return nil, fmt.Errorf("unsupported OpenAPI reference %q", ref)
	}
	var current any = root
	for segment := range strings.SplitSeq(strings.TrimPrefix(ref, prefix), "/") {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf(
				"OpenAPI reference %q crosses a non-object",
				ref,
			)
		}
		current, ok = object[segment]
		if !ok {
			return nil, fmt.Errorf(
				"OpenAPI reference %q is missing segment %q",
				ref,
				segment,
			)
		}
	}
	resolved, ok := current.(map[string]any)
	if !ok {
		return nil, fmt.Errorf(
			"OpenAPI reference %q does not resolve to an object",
			ref,
		)
	}
	return resolved, nil
}

func validateOpenAPIValue(
	root, schema map[string]any,
	value any,
	path string,
) error {
	resolved, err := resolveOpenAPIRef(root, schema)
	if err != nil {
		return err
	}
	schema = resolved
	typeName, _ := schema["type"].(string)
	if value == nil {
		if nullable, _ := schema["nullable"].(bool); nullable ||
			typeName == "null" {
			return nil
		}
		// A null can still satisfy a composition through a member such as
		// `anyOf: [{type: string}, {type: 'null'}]`, so only a schema with no
		// composition rejects it outright.
		if !hasOpenAPIComposition(schema) {
			return fmt.Errorf("%s is null but the schema is not nullable", path)
		}
	}

	for _, keyword := range []string{"allOf", "anyOf", "oneOf"} {
		alternatives, _ := schema[keyword].([]any)
		if len(alternatives) == 0 {
			continue
		}
		valid := 0
		var lastErr error
		for _, rawAlternative := range alternatives {
			alternative, ok := rawAlternative.(map[string]any)
			if !ok {
				return fmt.Errorf(
					"%s %s member is not an object",
					path,
					keyword,
				)
			}
			if err := validateOpenAPIValue(root, alternative, value, path); err == nil {
				valid++
			} else {
				lastErr = err
			}
		}
		switch keyword {
		case "allOf":
			if valid != len(alternatives) {
				return lastErr
			}
		case "anyOf":
			if valid == 0 {
				return lastErr
			}
		case "oneOf":
			if valid != 1 {
				return fmt.Errorf("%s matches %d oneOf schemas", path, valid)
			}
		}
	}

	if enum, ok := schema["enum"].([]any); ok {
		matched := false
		for _, candidate := range enum {
			if reflect.DeepEqual(candidate, value) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s is outside the schema enum", path)
		}
	}

	if value == nil {
		if typeName != "" {
			return fmt.Errorf("%s is null, want %s", path, typeName)
		}
		return nil
	}
	switch typeName {
	case "":
		return nil
	case "null":
		return fmt.Errorf("%s is %T, want null", path, value)
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("%s is %T, want boolean", path, value)
		}
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("%s is %T, want string", path, value)
		}
	case "integer":
		number, ok := value.(float64)
		if !ok || math.Trunc(number) != number {
			return fmt.Errorf("%s is %T, want integer", path, value)
		}
	case "number":
		if _, ok := value.(float64); !ok {
			return fmt.Errorf("%s is %T, want number", path, value)
		}
	case "array":
		items, ok := value.([]any)
		if !ok {
			return fmt.Errorf("%s is %T, want array", path, value)
		}
		itemSchema, _ := schema["items"].(map[string]any)
		for index, item := range items {
			if err := validateOpenAPIValue(
				root,
				itemSchema,
				item,
				fmt.Sprintf("%s[%d]", path, index),
			); err != nil {
				return err
			}
		}
	case "object":
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%s is %T, want object", path, value)
		}
		required, _ := schema["required"].([]any)
		for _, rawName := range required {
			name, _ := rawName.(string)
			if _, ok := object[name]; !ok {
				return fmt.Errorf(
					"%s is missing required property %q",
					path,
					name,
				)
			}
		}
		properties, _ := schema["properties"].(map[string]any)
		for name, propertyValue := range object {
			rawPropertySchema, ok := properties[name]
			if !ok {
				continue
			}
			propertySchema, ok := rawPropertySchema.(map[string]any)
			if !ok {
				return fmt.Errorf("%s.%s schema is not an object", path, name)
			}
			if err := validateOpenAPIValue(
				root,
				propertySchema,
				propertyValue,
				path+"."+name,
			); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%s uses unsupported schema type %q", path, typeName)
	}
	return nil
}

func hasOpenAPIComposition(schema map[string]any) bool {
	for _, keyword := range []string{"allOf", "anyOf", "oneOf"} {
		if alternatives, _ := schema[keyword].([]any); len(alternatives) > 0 {
			return true
		}
	}
	return false
}

func invalidQueryValue(schema map[string]any) (string, bool) {
	if enum, ok := schema["enum"].([]any); ok && len(enum) > 0 {
		return "invalid-openapi-enum-value", true
	}
	if minimum, ok := numberValue(schema["minimum"]); ok {
		return strconv.FormatInt(int64(minimum)-1, 10), true
	}
	if maximum, ok := numberValue(schema["maximum"]); ok {
		return strconv.FormatInt(int64(maximum)+1, 10), true
	}
	return "", false
}

func numberValue(value any) (float64, bool) {
	switch value := value.(type) {
	case int:
		return float64(value), true
	case int64:
		return float64(value), true
	case uint64:
		return float64(value), true
	case float64:
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, false
		}
		return value, true
	default:
		return 0, false
	}
}

func TestValidateOpenAPIValueNull(t *testing.T) {
	t.Parallel()
	stringOrNull := map[string]any{"anyOf": []any{
		map[string]any{"type": "string"},
		map[string]any{"type": "null"},
	}}
	stringOrNumber := map[string]any{"anyOf": []any{
		map[string]any{"type": "string"},
		map[string]any{"type": "number"},
	}}
	tests := []struct {
		name   string
		schema map[string]any
		value  any
		valid  bool
	}{
		{"anyOf null member accepts null", stringOrNull, nil, true},
		{"anyOf null member accepts string", stringOrNull, "x", true},
		{"anyOf without null member rejects null", stringOrNumber, nil, false},
		{
			"type null rejects string",
			map[string]any{"type": "null"},
			"x",
			false,
		},
		{"type null accepts null", map[string]any{"type": "null"}, nil, true},
		{
			"non-nullable string rejects null",
			map[string]any{"type": "string"},
			nil,
			false,
		},
		{
			"nullable string accepts null",
			map[string]any{"type": "string", "nullable": true},
			nil,
			true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := validateOpenAPIValue(
				map[string]any{},
				test.schema,
				test.value,
				"$",
			)
			if test.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
