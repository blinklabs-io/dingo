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

	"github.com/blinklabs-io/plutigo/data"
)

// plutusDataDetailedJSON renders Plutus data CBOR in the detailed JSON schema
// Blockfrost serves for datums: {"int": n}, {"bytes": hex}, {"list": [...]},
// {"map": [{"k": ..., "v": ...}]} and {"constructor": n, "fields": [...]}.
func plutusDataDetailedJSON(raw []byte) (json.RawMessage, error) {
	decoded, err := data.Decode(raw)
	if err != nil {
		return nil, fmt.Errorf("decode Plutus data: %w", err)
	}
	encoded, err := data.EncodeJSON(decoded)
	if err != nil {
		return nil, fmt.Errorf("encode Plutus data JSON: %w", err)
	}
	return encoded, nil
}
