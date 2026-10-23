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

// Package openapiconformance provides test-only checks against pinned OpenAPI
// documents. It deliberately implements only the OpenAPI vocabulary used by
// the Mesh and Kupo specifications.
package openapiconformance

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

var httpMethods = map[string]bool{
	"delete":  true,
	"get":     true,
	"head":    true,
	"options": true,
	"patch":   true,
	"post":    true,
	"put":     true,
}

// Document is a normalized OpenAPI document.
type Document struct {
	root map[string]any
}

// Operation identifies one HTTP operation.
type Operation struct {
	Method string
	Path   string
}

// Route maps an upstream path onto the path registered by an implementation.
// RuntimePath differs only where a router requires a different placeholder
// spelling or exposes an equivalent prefix alias.
type Route struct {
	Operation
	RuntimePath string
	// EquivalentPath is for source-authorized aliases whose placeholder shape
	// differs from the OpenAPI template, such as Kupo's split pattern form.
	EquivalentPath bool
}

// Provenance pins the source and content of a vendored OpenAPI document.
type Provenance struct {
	URL      string
	Revision string
	SHA256   string
}

// AssertProvenance prevents an upstream document from being changed without
// updating its recorded immutable source and digest.
func AssertProvenance(t *testing.T, data []byte, provenance Provenance) {
	t.Helper()
	if !strings.HasPrefix(provenance.URL, "https://github.com/") {
		t.Errorf("OpenAPI source is not a canonical GitHub URL: %q", provenance.URL)
	}
	if matched, _ := regexp.MatchString(`^[0-9a-f]{40}$`, provenance.Revision); !matched {
		t.Errorf("OpenAPI revision is not a full commit: %q", provenance.Revision)
	}
	if !strings.Contains(provenance.URL, "/blob/"+provenance.Revision+"/") {
		t.Errorf("OpenAPI source URL does not pin revision %s", provenance.Revision)
	}
	got := fmt.Sprintf("%x", sha256.Sum256(data))
	if got != provenance.SHA256 {
		t.Errorf("OpenAPI SHA-256 = %s, want %s", got, provenance.SHA256)
	}
}

// ParseJSON parses an OpenAPI JSON document.
func ParseJSON(data []byte) (Document, error) {
	var root map[string]any
	if err := json.Unmarshal(data, &root); err != nil {
		return Document{}, fmt.Errorf("decode OpenAPI JSON: %w", err)
	}
	return Document{root: root}, nil
}

// ParseYAML parses an OpenAPI YAML document.
func ParseYAML(data []byte) (Document, error) {
	var decoded any
	if err := yaml.Unmarshal(data, &decoded); err != nil {
		return Document{}, fmt.Errorf("decode OpenAPI YAML: %w", err)
	}
	root, ok := normalizeYAML(decoded).(map[string]any)
	if !ok {
		return Document{}, errorsForType("OpenAPI YAML", decoded)
	}
	return Document{root: root}, nil
}

func normalizeYAML(value any) any {
	switch value := value.(type) {
	case map[string]any:
		ret := make(map[string]any, len(value))
		for key, child := range value {
			ret[key] = normalizeYAML(child)
		}
		return ret
	case map[any]any:
		ret := make(map[string]any, len(value))
		for key, child := range value {
			ret[fmt.Sprint(key)] = normalizeYAML(child)
		}
		return ret
	case []any:
		ret := make([]any, len(value))
		for i, child := range value {
			ret[i] = normalizeYAML(child)
		}
		return ret
	default:
		return value
	}
}

// Version returns info.version.
func (d Document) Version() string {
	return stringValue(objectValue(d.root, "info"), "version")
}

// Operations returns every HTTP operation in stable order.
func (d Document) Operations() []Operation {
	paths := objectValue(d.root, "paths")
	ret := make([]Operation, 0)
	for path, rawItem := range paths {
		item, ok := rawItem.(map[string]any)
		if !ok {
			continue
		}
		for method := range item {
			method = strings.ToLower(method)
			if httpMethods[method] {
				ret = append(ret, Operation{
					Method: strings.ToUpper(method),
					Path:   path,
				})
			}
		}
	}
	slices.SortFunc(ret, compareOperation)
	return ret
}

// AssertRoutes requires every upstream operation to be either registered or
// explicitly declared "Not implemented", and rejects runtime routes that do
// not correspond to an upstream operation.
func (d Document) AssertRoutes(
	t *testing.T,
	registered []Operation,
	routes []Route,
	notImplemented map[Operation]string,
) {
	t.Helper()
	spec := d.Operations()
	wantSpec := make(map[Operation]bool, len(spec))
	for _, operation := range spec {
		wantSpec[operation] = true
	}

	declaredRuntime := make(map[Operation]Operation, len(routes))
	declaredSpec := make(map[Operation]bool, len(routes))
	for _, route := range routes {
		if !wantSpec[route.Operation] {
			t.Errorf("declared route absent from upstream: %s %s", route.Method, route.Path)
		}
		declaredSpec[route.Operation] = true
		runtimePath := route.RuntimePath
		if runtimePath == "" {
			runtimePath = route.Path
		}
		if !route.EquivalentPath && !samePathParameters(route.Path, runtimePath) {
			t.Errorf("%s path parameters differ: upstream %q, runtime %q", route.Method, route.Path, runtimePath)
		}
		runtimeOperation := Operation{Method: route.Method, Path: runtimePath}
		if _, ok := declaredRuntime[runtimeOperation]; ok {
			t.Errorf("duplicate runtime route: %s %s", route.Method, runtimePath)
		}
		declaredRuntime[runtimeOperation] = route.Operation
	}
	for operation, reason := range notImplemented {
		if reason != "Not implemented" {
			t.Errorf("%s %s exception must be exactly %q", operation.Method, operation.Path, "Not implemented")
		}
		if !wantSpec[operation] {
			t.Errorf("unimplemented route absent from upstream: %s %s", operation.Method, operation.Path)
		}
		if declaredSpec[operation] {
			t.Errorf("route is both implemented and unimplemented: %s %s", operation.Method, operation.Path)
		}
	}
	for _, operation := range spec {
		if !declaredSpec[operation] && notImplemented[operation] == "" {
			t.Errorf("upstream operation has no disposition: %s %s", operation.Method, operation.Path)
		}
	}

	gotRuntime := make(map[Operation]bool, len(registered))
	for _, operation := range registered {
		if gotRuntime[operation] {
			t.Errorf("runtime route is registered more than once: %s %s", operation.Method, operation.Path)
		}
		gotRuntime[operation] = true
		if _, ok := declaredRuntime[operation]; !ok {
			t.Errorf("registered route absent from conformance declaration: %s %s", operation.Method, operation.Path)
		}
	}
	for operation := range declaredRuntime {
		if !gotRuntime[operation] {
			t.Errorf("declared route is not registered: %s %s", operation.Method, operation.Path)
		}
	}
}

// Parameters returns the parameter names at one OpenAPI location.
func (d Document) Parameters(operation Operation, location string) ([]string, error) {
	op, err := d.operation(operation)
	if err != nil {
		return nil, err
	}
	pathItem := objectValue(objectValue(d.root, "paths"), operation.Path)
	parameters := append(arrayValue(pathItem, "parameters"), arrayValue(op, "parameters")...)
	ret := make([]string, 0, len(parameters))
	for _, raw := range parameters {
		parameter, err := d.resolveObject(raw)
		if err != nil {
			return nil, err
		}
		if stringValue(parameter, "in") == location {
			ret = append(ret, stringValue(parameter, "name"))
		}
	}
	slices.Sort(ret)
	return slices.Compact(ret), nil
}

// QueryParameters returns the query parameter names on one operation.
func (d Document) QueryParameters(operation Operation) ([]string, error) {
	return d.Parameters(operation, "query")
}

// AssertQueryParameters compares an implementation allowlist with upstream.
func (d Document) AssertQueryParameters(
	t *testing.T,
	operation Operation,
	implemented []string,
) {
	t.Helper()
	want, err := d.QueryParameters(operation)
	if err != nil {
		t.Fatal(err)
	}
	implemented = append([]string(nil), implemented...)
	slices.Sort(implemented)
	implemented = slices.Compact(implemented)
	if !slices.Equal(want, implemented) {
		t.Errorf("%s %s query parameters = %v, want %v", operation.Method, operation.Path, implemented, want)
	}
}

// RequestSchema returns the application/json request schema for an operation.
func (d Document) RequestSchema(operation Operation) (map[string]any, error) {
	op, err := d.operation(operation)
	if err != nil {
		return nil, err
	}
	requestBody, err := d.resolveObject(op["requestBody"])
	if err != nil || requestBody == nil {
		return nil, err
	}
	return d.contentSchema(requestBody)
}

// ResponseSchema returns the schema for status and content type. A media type
// parameter mismatch falls back to the base media type only when the document
// defines no more specific entry.
func (d Document) ResponseSchema(
	operation Operation,
	status int,
	contentType string,
) (map[string]any, error) {
	op, err := d.operation(operation)
	if err != nil {
		return nil, err
	}
	responses := objectValue(op, "responses")
	raw := responses[strconv.Itoa(status)]
	response, err := d.resolveObject(raw)
	if err != nil {
		return nil, err
	}
	if response == nil {
		return nil, fmt.Errorf("%s %s has no %d response", operation.Method, operation.Path, status)
	}
	content := objectValue(response, "content")
	if len(content) == 0 {
		return nil, nil
	}
	contentType = strings.TrimSpace(contentType)
	if rawMedia, ok := content[contentType]; ok {
		return schemaFromMedia(rawMedia)
	}
	base := strings.TrimSpace(strings.Split(contentType, ";")[0])
	for mediaType, rawMedia := range content {
		if strings.TrimSpace(strings.Split(mediaType, ";")[0]) == base {
			return schemaFromMedia(rawMedia)
		}
	}
	return nil, fmt.Errorf("%s %s response %d has no content type %q", operation.Method, operation.Path, status, contentType)
}

// AssertGoType compares a Go JSON representation with an OpenAPI schema.
func (d Document) AssertGoType(t *testing.T, schema map[string]any, typ reflect.Type) {
	t.Helper()
	name := typ.Name()
	if typ.Kind() == reflect.Pointer {
		name = typ.Elem().Name()
	}
	if err := d.compareGoType(schema, typ, name, map[string]bool{}); err != nil {
		t.Error(err)
	}
}

// ValidateJSON validates a decoded JSON value against the schema vocabulary
// used by the pinned documents.
func (d Document) ValidateJSON(schema map[string]any, value any) error {
	return d.validate(schema, value, "$", 0)
}

func (d Document) operation(operation Operation) (map[string]any, error) {
	item := objectValue(objectValue(d.root, "paths"), operation.Path)
	raw, ok := item[strings.ToLower(operation.Method)]
	if !ok {
		return nil, fmt.Errorf("operation not found: %s %s", operation.Method, operation.Path)
	}
	op, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("operation is not an object: %s %s", operation.Method, operation.Path)
	}
	return op, nil
}

func (d Document) contentSchema(container map[string]any) (map[string]any, error) {
	content := objectValue(container, "content")
	for mediaType, rawMedia := range content {
		if strings.HasPrefix(mediaType, "application/json") {
			return schemaFromMedia(rawMedia)
		}
	}
	return nil, nil
}

func schemaFromMedia(raw any) (map[string]any, error) {
	media, ok := raw.(map[string]any)
	if !ok {
		return nil, errorsForType("media type", raw)
	}
	schema, _ := media["schema"].(map[string]any)
	return schema, nil
}

func (d Document) resolveObject(raw any) (map[string]any, error) {
	if raw == nil {
		return nil, nil
	}
	obj, ok := raw.(map[string]any)
	if !ok {
		return nil, errorsForType("OpenAPI object", raw)
	}
	ref := stringValue(obj, "$ref")
	if ref == "" {
		return obj, nil
	}
	resolved, err := d.resolveRef(ref)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]any, len(resolved)+len(obj)-1)
	maps.Copy(ret, resolved)
	for key, value := range obj {
		if key != "$ref" {
			ret[key] = value
		}
	}
	return ret, nil
}

func (d Document) resolveRef(ref string) (map[string]any, error) {
	if !strings.HasPrefix(ref, "#/") {
		return nil, fmt.Errorf("external OpenAPI reference is not pinned: %q", ref)
	}
	var current any = d.root
	for token := range strings.SplitSeq(strings.TrimPrefix(ref, "#/"), "/") {
		token = strings.ReplaceAll(strings.ReplaceAll(token, "~1", "/"), "~0", "~")
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("reference %q traverses a non-object", ref)
		}
		current, ok = obj[token]
		if !ok {
			return nil, fmt.Errorf("reference %q does not exist", ref)
		}
	}
	obj, ok := current.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("reference %q does not name an object", ref)
	}
	return obj, nil
}

func (d Document) compareGoType(
	schema map[string]any,
	typ reflect.Type,
	path string,
	seen map[string]bool,
) error {
	originalType := typ
	resolved, err := d.resolveObject(schema)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	schema = resolved
	if nullable, _ := schema["nullable"].(bool); nullable && originalType.Kind() != reflect.Pointer {
		return fmt.Errorf("%s: nullable OpenAPI value uses non-pointer Go type %s", path, originalType)
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if branches := arrayValue(schema, "oneOf"); len(branches) > 0 {
		nullable := false
		for _, branch := range branches {
			obj, ok := branch.(map[string]any)
			if ok && stringValue(obj, "type") == "null" {
				nullable = true
			}
		}
		if nullable && originalType.Kind() != reflect.Pointer {
			return fmt.Errorf("%s: nullable OpenAPI value uses non-pointer Go type %s", path, originalType)
		}
		for _, branch := range branches {
			obj, ok := branch.(map[string]any)
			if !ok || stringValue(obj, "type") == "null" {
				continue
			}
			if err := d.compareGoType(obj, typ, path, seen); err == nil {
				return nil
			}
		}
		return fmt.Errorf("%s: Go type %s matches no oneOf branch", path, typ)
	}
	if branches := arrayValue(schema, "anyOf"); len(branches) > 0 {
		for _, branch := range branches {
			obj, ok := branch.(map[string]any)
			if ok && d.compareGoType(obj, typ, path, seen) == nil {
				return nil
			}
		}
		return fmt.Errorf("%s: Go type %s matches no anyOf branch", path, typ)
	}

	want := stringValue(schema, "type")
	if want == "" && len(objectValue(schema, "properties")) > 0 {
		want = "object"
	}
	switch typ.Kind() {
	case reflect.Interface:
		return nil
	case reflect.Struct:
		if want != "object" && want != "" {
			return fmt.Errorf("%s: Go struct %s represents OpenAPI %s", path, typ, want)
		}
		key := path + "=" + typ.PkgPath() + "." + typ.Name()
		if seen[key] {
			return nil
		}
		seen[key] = true
		defer delete(seen, key)
		return d.compareStruct(schema, typ, path, seen)
	case reflect.Map:
		if want != "object" && want != "" {
			return fmt.Errorf("%s: Go map represents OpenAPI %s", path, want)
		}
		if typ.Key().Kind() != reflect.String {
			return fmt.Errorf("%s: JSON map key is %s", path, typ.Key())
		}
		return nil
	case reflect.Slice, reflect.Array:
		if typ.Elem().Kind() == reflect.Uint8 && want == "string" {
			return nil
		}
		if want != "array" {
			return fmt.Errorf("%s: Go slice represents OpenAPI %s", path, want)
		}
		items, _ := schema["items"].(map[string]any)
		if items == nil {
			return nil
		}
		return d.compareGoType(items, typ.Elem(), path+"[]", seen)
	case reflect.String:
		if want != "string" && want != "" {
			return fmt.Errorf("%s: Go string represents OpenAPI %s", path, want)
		}
	case reflect.Bool:
		if want != "boolean" {
			return fmt.Errorf("%s: Go bool represents OpenAPI %s", path, want)
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if want != "integer" {
			return fmt.Errorf("%s: Go integer represents OpenAPI %s", path, want)
		}
		format := stringValue(schema, "format")
		if format == "int32" && typ.Bits() != 32 {
			return fmt.Errorf("%s: OpenAPI int32 uses %s", path, typ)
		}
		if format == "int64" && typ.Bits() != 64 {
			return fmt.Errorf("%s: OpenAPI int64 uses %s", path, typ)
		}
	case reflect.Float32, reflect.Float64:
		if want != "number" {
			return fmt.Errorf("%s: Go float represents OpenAPI %s", path, want)
		}
	case reflect.Invalid, reflect.Uintptr, reflect.Complex64, reflect.Complex128,
		reflect.Chan, reflect.Func, reflect.Pointer, reflect.UnsafePointer:
		return fmt.Errorf("%s: unsupported Go JSON type %s", path, typ)
	}
	return nil
}

func (d Document) compareStruct(
	schema map[string]any,
	typ reflect.Type,
	path string,
	seen map[string]bool,
) error {
	properties := objectValue(schema, "properties")
	required := make(map[string]bool)
	for _, raw := range arrayValue(schema, "required") {
		if name, ok := raw.(string); ok {
			required[name] = true
		}
	}
	fields := make(map[string]reflect.StructField)
	collectJSONFields(typ, fields)
	for name := range properties {
		if _, ok := fields[name]; !ok {
			return fmt.Errorf("%s: missing JSON field %q", path, name)
		}
	}
	for name, field := range fields {
		rawSchema, ok := properties[name]
		if !ok {
			return fmt.Errorf("%s: JSON field %q is absent from OpenAPI", path, name)
		}
		_, options := splitJSONTag(field.Tag.Get("json"))
		if required[name] && options["omitempty"] {
			return fmt.Errorf("%s.%s: required field has omitempty", path, name)
		}
		fieldSchema, ok := rawSchema.(map[string]any)
		if !ok {
			return fmt.Errorf("%s.%s: schema is not an object", path, name)
		}
		if err := d.compareGoType(fieldSchema, field.Type, path+"."+name, seen); err != nil {
			return err
		}
	}
	return nil
}

func collectJSONFields(typ reflect.Type, dst map[string]reflect.StructField) {
	for field := range typ.Fields() {
		if field.PkgPath != "" && !field.Anonymous {
			continue
		}
		name, _ := splitJSONTag(field.Tag.Get("json"))
		if name == "-" {
			continue
		}
		if field.Anonymous && name == "" {
			embedded := field.Type
			for embedded.Kind() == reflect.Pointer {
				embedded = embedded.Elem()
			}
			if embedded.Kind() == reflect.Struct {
				collectJSONFields(embedded, dst)
			}
			continue
		}
		if name == "" {
			name = field.Name
		}
		dst[name] = field
	}
}

func splitJSONTag(tag string) (string, map[string]bool) {
	parts := strings.Split(tag, ",")
	options := make(map[string]bool, len(parts)-1)
	for _, option := range parts[1:] {
		options[option] = true
	}
	return parts[0], options
}

func (d Document) validate(schema map[string]any, value any, path string, depth int) error {
	if depth > 100 {
		return fmt.Errorf("%s: schema nesting exceeds 100", path)
	}
	resolved, err := d.resolveObject(schema)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	schema = resolved
	if branches := arrayValue(schema, "oneOf"); len(branches) > 0 {
		matches := 0
		for _, raw := range branches {
			branch, ok := raw.(map[string]any)
			if ok && d.validate(branch, value, path, depth+1) == nil {
				matches++
			}
		}
		if matches != 1 {
			return fmt.Errorf("%s: value matches %d oneOf branches", path, matches)
		}
		return nil
	}
	if branches := arrayValue(schema, "anyOf"); len(branches) > 0 {
		for _, raw := range branches {
			branch, ok := raw.(map[string]any)
			if ok && d.validate(branch, value, path, depth+1) == nil {
				return nil
			}
		}
		return fmt.Errorf("%s: value matches no anyOf branch", path)
	}
	if enum := arrayValue(schema, "enum"); len(enum) > 0 {
		matched := false
		for _, candidate := range enum {
			if reflect.DeepEqual(candidate, value) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("%s: value %v is outside enum %v", path, value, enum)
		}
	}

	wantType := stringValue(schema, "type")
	if value == nil {
		nullable, _ := schema["nullable"].(bool)
		if wantType == "null" || nullable {
			return nil
		}
		return fmt.Errorf("%s: null does not satisfy %s", path, wantType)
	}
	switch wantType {
	case "", "object":
		obj, ok := value.(map[string]any)
		if !ok {
			if wantType == "" {
				return nil
			}
			return fmt.Errorf("%s: expected object, got %T", path, value)
		}
		properties := objectValue(schema, "properties")
		if propertyNames, ok := schema["propertyNames"].(map[string]any); ok {
			for name := range obj {
				propertyValue := any(name)
				if stringValue(propertyNames, "type") == "integer" {
					parsed, parseErr := strconv.ParseFloat(name, 64)
					if parseErr != nil || math.Trunc(parsed) != parsed {
						return fmt.Errorf("%s: property name %q is not an integer", path, name)
					}
					propertyValue = parsed
				}
				if err := d.validate(propertyNames, propertyValue, path+" property", depth+1); err != nil {
					return err
				}
			}
		}
		for _, raw := range arrayValue(schema, "required") {
			name, ok := raw.(string)
			if ok {
				if _, present := obj[name]; !present {
					return fmt.Errorf("%s: missing required property %q", path, name)
				}
			}
		}
		for name, child := range obj {
			rawChildSchema, known := properties[name]
			if !known {
				if allowed, ok := schema["additionalProperties"].(bool); ok && !allowed {
					return fmt.Errorf("%s: unexpected property %q", path, name)
				}
				if additional, ok := schema["additionalProperties"].(map[string]any); ok {
					if err := d.validate(additional, child, path+"."+name, depth+1); err != nil {
						return err
					}
				}
				continue
			}
			childSchema, ok := rawChildSchema.(map[string]any)
			if ok {
				if err := d.validate(childSchema, child, path+"."+name, depth+1); err != nil {
					return err
				}
			}
		}
	case "array":
		items, ok := value.([]any)
		if !ok {
			return fmt.Errorf("%s: expected array, got %T", path, value)
		}
		if min, ok := numberValue(schema, "minItems"); ok && float64(len(items)) < min {
			return fmt.Errorf("%s: array length %d is below %g", path, len(items), min)
		}
		if max, ok := numberValue(schema, "maxItems"); ok && float64(len(items)) > max {
			return fmt.Errorf("%s: array length %d exceeds %g", path, len(items), max)
		}
		itemSchema, _ := schema["items"].(map[string]any)
		for i, item := range items {
			if itemSchema != nil {
				if err := d.validate(itemSchema, item, fmt.Sprintf("%s[%d]", path, i), depth+1); err != nil {
					return err
				}
			}
		}
	case "string":
		str, ok := value.(string)
		if !ok {
			return fmt.Errorf("%s: expected string, got %T", path, value)
		}
		if err := validateString(schema, str, path); err != nil {
			return err
		}
	case "integer":
		number, ok := value.(float64)
		if !ok || math.Trunc(number) != number {
			return fmt.Errorf("%s: expected integer, got %v", path, value)
		}
		if err := validateNumber(schema, number, path); err != nil {
			return err
		}
	case "number":
		number, ok := value.(float64)
		if !ok {
			return fmt.Errorf("%s: expected number, got %T", path, value)
		}
		if err := validateNumber(schema, number, path); err != nil {
			return err
		}
	case "boolean":
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("%s: expected boolean, got %T", path, value)
		}
	case "null":
		return fmt.Errorf("%s: expected null, got %T", path, value)
	default:
		return fmt.Errorf("%s: unsupported OpenAPI type %q", path, wantType)
	}
	return nil
}

func validateString(schema map[string]any, value, path string) error {
	if min, ok := numberValue(schema, "minLength"); ok && float64(len(value)) < min {
		return fmt.Errorf("%s: string length %d is below %g", path, len(value), min)
	}
	if max, ok := numberValue(schema, "maxLength"); ok && float64(len(value)) > max {
		return fmt.Errorf("%s: string length %d exceeds %g", path, len(value), max)
	}
	if pattern := stringValue(schema, "pattern"); pattern != "" {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return fmt.Errorf("%s: invalid upstream pattern %q: %w", path, pattern, err)
		}
		if !re.MatchString(value) {
			return fmt.Errorf("%s: %q does not match %q", path, value, pattern)
		}
	}
	switch stringValue(schema, "contentEncoding") {
	case "base16":
		if _, err := hex.DecodeString(value); err != nil {
			return fmt.Errorf("%s: value is not base16", path)
		}
	case "base58":
		if matched, _ := regexp.MatchString(`^[1-9A-HJ-NP-Za-km-z]+$`, value); !matched {
			return fmt.Errorf("%s: value is not base58", path)
		}
	case "bech32":
		if matched, _ := regexp.MatchString(`^[a-z0-9]+1[02-9ac-hj-np-z]+$`, value); !matched {
			return fmt.Errorf("%s: value is not bech32", path)
		}
	}
	return nil
}

func validateNumber(schema map[string]any, value float64, path string) error {
	if min, ok := numberValue(schema, "minimum"); ok && value < min {
		return fmt.Errorf("%s: %g is below %g", path, value, min)
	}
	if max, ok := numberValue(schema, "maximum"); ok && value > max {
		return fmt.Errorf("%s: %g exceeds %g", path, value, max)
	}
	return nil
}

func objectValue(obj map[string]any, key string) map[string]any {
	ret, _ := obj[key].(map[string]any)
	return ret
}

func arrayValue(obj map[string]any, key string) []any {
	ret, _ := obj[key].([]any)
	return ret
}

func stringValue(obj map[string]any, key string) string {
	ret, _ := obj[key].(string)
	return ret
}

func numberValue(obj map[string]any, key string) (float64, bool) {
	switch value := obj[key].(type) {
	case float64:
		return value, true
	case int:
		return float64(value), true
	case uint64:
		return float64(value), true
	default:
		return 0, false
	}
}

func compareOperation(a, b Operation) int {
	if c := strings.Compare(a.Path, b.Path); c != 0 {
		return c
	}
	return strings.Compare(a.Method, b.Method)
}

func samePathParameters(specPath, runtimePath string) bool {
	normalize := func(path string) []string {
		matches := regexp.MustCompile(`\{([^}]+)\}`).FindAllStringSubmatch(path, -1)
		ret := make([]string, 0, len(matches))
		for _, match := range matches {
			ret = append(ret, strings.ReplaceAll(match[1], "_", "-"))
		}
		return ret
	}
	return slices.Equal(normalize(specPath), normalize(runtimePath))
}

func errorsForType(name string, value any) error {
	return fmt.Errorf("%s is %T, want object", name, value)
}
