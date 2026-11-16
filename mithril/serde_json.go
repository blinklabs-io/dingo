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
//
// Ported from mithril2pq (internal/mithril/chain/strict.go and
// internal/mithril/stm/codec.go), Copyright 2026 Chris Guiney, Apache-2.0,
// which mirrors the serde_json derived Deserialize of the mithril-common
// 0.8.3 and mithril-stm 0.13.0 message types at Mithril release 2642.0
// (e85df3ddc52b528c7b9ddbdddf1941fec6d5f348).
// Mithril: Copyright 2022 Input Output (Hong Kong) Ltd., Apache-2.0.
// Modifications: translated from Rust to Go; the check streams instead of
// building a tree.

package mithril

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
)

// Mithril decodes its JSON messages with serde's derived Deserialize and
// dingo with encoding/json, and the two disagree on malformed input:
// encoding/json matches keys case-insensitively, lets a repeated key
// overwrite the earlier one, and reads null and absent fields as zero
// values. serdeJSON checks a message against the shape serde expects and
// drops every key serde would ignore, so decoding its output with
// encoding/json reads the values serde reads, or the check rejects what
// serde rejects.
//
// It streams: the output is no larger than the input and no tree of the
// message is held, so bounded decoders that run on its output still bound
// what a message can make dingo retain.

type serdeKind int

const (
	serdeString serdeKind = iota
	serdeU64
	serdeF64
	serdeBytes // an array of integers 0..255
	serdeU64s  // an array of u64
	serdeNull  // PhantomData: null and nothing else
	serdeObject
	serdeArray // elements all of kind fields[0]
	serdeTuple // exactly len(fields) elements, element i of kind fields[i]
	serdeStringMap
	serdeAny
)

// serdeField is a field of a serde struct, or an element of an array or
// tuple. A field may be absent if it is an Option or has serde(default),
// and null only if it is an Option.
type serdeField struct {
	name   string
	kind   serdeKind
	absent bool
	null   bool
	fields []serdeField // object fields, or array and tuple elements
	values []string     // the allowed values of a string enum
}

// serdeJSON checks data against root and returns it with the keys serde
// ignores removed.
func serdeJSON(data []byte, root *serdeField) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	out := bytes.NewBuffer(make([]byte, 0, len(data)))
	if err := checkSerdeValue(dec, out, root); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing data after JSON value")
	}
	return out.Bytes(), nil
}

func writeJSONString(out *bytes.Buffer, s string) {
	b, _ := json.Marshal(s) //nolint:errchkjson // a string always marshals
	out.Write(b)
}

// checkSerdeValue reads one value for f and writes it to out.
func checkSerdeValue(
	dec *json.Decoder,
	out *bytes.Buffer,
	f *serdeField,
) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	if tok == nil {
		if !f.null && f.kind != serdeNull {
			return errors.New("null")
		}
		out.WriteString("null")
		return nil
	}
	switch f.kind {
	case serdeAny:
		return copyJSONValue(dec, out, tok)
	case serdeNull:
		return errors.New("must be null")
	case serdeString:
		s, ok := tok.(string)
		if !ok {
			return errors.New("want a string")
		}
		if f.values != nil && !slices.Contains(f.values, s) {
			return fmt.Errorf("unknown variant %q", s)
		}
		writeJSONString(out, s)
	case serdeU64, serdeF64:
		n, ok := tok.(json.Number)
		if !ok {
			return errors.New("want a number")
		}
		if f.kind == serdeU64 {
			if _, err := strconv.ParseUint(string(n), 10, 64); err != nil {
				return err
			}
		}
		out.WriteString(string(n))
	case serdeBytes, serdeU64s:
		if tok != json.Delim('[') {
			return errors.New("want an array")
		}
		bits := 64
		if f.kind == serdeBytes {
			bits = 8
		}
		out.WriteByte('[')
		for i := 0; dec.More(); i++ {
			t, err := dec.Token()
			if err != nil {
				return err
			}
			n, ok := t.(json.Number)
			if !ok {
				return fmt.Errorf("element %d is not a number", i)
			}
			if _, err := strconv.ParseUint(string(n), 10, bits); err != nil {
				return fmt.Errorf("element %d: %w", i, err)
			}
			if i > 0 {
				out.WriteByte(',')
			}
			out.WriteString(string(n))
		}
		if _, err := dec.Token(); err != nil {
			return err
		}
		out.WriteByte(']')
	case serdeObject:
		if tok != json.Delim('{') {
			return errors.New("want an object")
		}
		return checkSerdeObject(dec, out, f.fields)
	case serdeArray, serdeTuple:
		if tok != json.Delim('[') {
			return errors.New("want an array")
		}
		out.WriteByte('[')
		n := 0
		for ; dec.More(); n++ {
			elem := &f.fields[0]
			if f.kind == serdeTuple {
				if n >= len(f.fields) {
					return fmt.Errorf("want %d elements", len(f.fields))
				}
				elem = &f.fields[n]
			}
			if n > 0 {
				out.WriteByte(',')
			}
			if err := checkSerdeValue(dec, out, elem); err != nil {
				return fmt.Errorf("element %d: %w", n, err)
			}
		}
		if f.kind == serdeTuple && n != len(f.fields) {
			return fmt.Errorf("want %d elements, got %d", len(f.fields), n)
		}
		if _, err := dec.Token(); err != nil {
			return err
		}
		out.WriteByte(']')
	case serdeStringMap:
		if tok != json.Delim('{') {
			return errors.New("want an object")
		}
		out.WriteByte('{')
		for i := 0; dec.More(); i++ {
			k, err := dec.Token()
			if err != nil {
				return err
			}
			v, err := dec.Token()
			if err != nil {
				return err
			}
			s, ok := v.(string)
			if !ok {
				return fmt.Errorf("%v: want a string", k)
			}
			if i > 0 {
				out.WriteByte(',')
			}
			writeJSONString(out, k.(string))
			out.WriteByte(':')
			writeJSONString(out, s)
		}
		if _, err := dec.Token(); err != nil {
			return err
		}
		out.WriteByte('}')
	}
	return nil
}

// checkSerdeObject reads an object's members after its opening brace. Keys
// that are not fields are skipped rather than copied: serde ignores them,
// repeated or not, and encoding/json would assign one that differs from a
// field name only in case to that field.
func checkSerdeObject(
	dec *json.Decoder,
	out *bytes.Buffer,
	fields []serdeField,
) error {
	seen := make([]bool, len(fields))
	out.WriteByte('{')
	written := 0
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		k := tok.(string)
		idx := slices.IndexFunc(fields, func(f serdeField) bool {
			return f.name == k
		})
		if idx < 0 {
			if err := skipJSONValue(dec); err != nil {
				return err
			}
			continue
		}
		if seen[idx] {
			return fmt.Errorf("duplicate field %q", k)
		}
		seen[idx] = true
		if written > 0 {
			out.WriteByte(',')
		}
		written++
		writeJSONString(out, k)
		out.WriteByte(':')
		if err := checkSerdeValue(dec, out, &fields[idx]); err != nil {
			return fmt.Errorf("%s: %w", k, err)
		}
	}
	if _, err := dec.Token(); err != nil {
		return err
	}
	out.WriteByte('}')
	for i, f := range fields {
		if !seen[i] && !f.absent {
			return fmt.Errorf("missing field %q", f.name)
		}
	}
	return nil
}

// copyJSONValue writes the value that starts with tok.
func copyJSONValue(dec *json.Decoder, out *bytes.Buffer, tok json.Token) error {
	switch v := tok.(type) {
	case json.Delim:
		out.WriteRune(rune(v))
		isObject := v == '{'
		for i := 0; dec.More(); i++ {
			if i > 0 {
				out.WriteByte(',')
			}
			if isObject {
				k, err := dec.Token()
				if err != nil {
					return err
				}
				writeJSONString(out, k.(string))
				out.WriteByte(':')
			}
			next, err := dec.Token()
			if err != nil {
				return err
			}
			if err := copyJSONValue(dec, out, next); err != nil {
				return err
			}
		}
		end, err := dec.Token()
		if err != nil {
			return err
		}
		out.WriteRune(rune(end.(json.Delim)))
	case string:
		writeJSONString(out, v)
	case json.Number:
		out.WriteString(string(v))
	case bool:
		out.WriteString(strconv.FormatBool(v))
	}
	return nil
}

func skipJSONValue(dec *json.Decoder) error {
	depth := 0
	for {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		switch tok {
		case json.Delim('{'), json.Delim('['):
			depth++
		case json.Delim('}'), json.Delim(']'):
			depth--
		}
		if depth == 0 {
			return nil
		}
	}
}
