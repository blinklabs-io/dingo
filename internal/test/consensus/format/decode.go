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

package format

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// DecodeTestVector parses raw JSON into a TestVector. Unknown fields
// are rejected. The result is validated before return.
func DecodeTestVector(raw []byte) (TestVector, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()

	var v TestVector
	if err := dec.Decode(&v); err != nil {
		return TestVector{}, fmt.Errorf("test vector: decode: %w", err)
	}
	// dec.More() only reports tokens inside the current container,
	// not whether a second top-level JSON value follows. Attempting
	// a second Decode and requiring io.EOF is the canonical way to
	// reject trailing top-level input.
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return TestVector{}, errors.New(
				"test vector: trailing JSON after vector",
			)
		}
		return TestVector{}, fmt.Errorf(
			"test vector: trailing JSON after vector: %w", err,
		)
	}
	if err := v.validate(); err != nil {
		return TestVector{}, fmt.Errorf("test vector: %w", err)
	}
	return v, nil
}
