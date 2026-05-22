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
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// HexBytes is a []byte that marshals to/from a JSON hex string. nil and
// empty both round-trip as "".
type HexBytes []byte

// MarshalJSON encodes the bytes as a lowercase hex string.
func (h HexBytes) MarshalJSON() ([]byte, error) {
	return json.Marshal(hex.EncodeToString(h))
}

// UnmarshalJSON decodes a JSON hex string (case-insensitive) into raw
// bytes.
func (h *HexBytes) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("hex bytes: not a JSON string: %w", err)
	}
	if s == "" {
		*h = nil
		return nil
	}
	out, err := hex.DecodeString(s)
	if err != nil {
		return fmt.Errorf("hex bytes: %w", err)
	}
	*h = out
	return nil
}
