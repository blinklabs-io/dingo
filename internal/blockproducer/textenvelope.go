// Copyright 2025 Blink Labs Software
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

package blockproducer

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"slices"
)

// textEnvelope is the cardano-cli on-disk format for serialized keys and
// certificates. The CBOR payload is hex-encoded and wrapped in a JSON object
// with a type tag that identifies what it contains.
type textEnvelope struct {
	Type        string `json:"type"`
	Description string `json:"description"`
	CborHex     string `json:"cborHex"`
}

func readEnvelope(path string) (*textEnvelope, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}
	var env textEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("parse %q: %w", path, err)
	}
	if env.Type == "" {
		return nil, fmt.Errorf("parse %q: missing type field", path)
	}
	if env.CborHex == "" {
		return nil, fmt.Errorf("parse %q: missing cborHex field", path)
	}
	return &env, nil
}

// envelopeBytes returns the raw CBOR payload of a TextEnvelope after
// validating that the type matches one of the accepted values.
func envelopeBytes(env *textEnvelope, accepted ...string) ([]byte, error) {
	if !slices.Contains(accepted, env.Type) {
		return nil, fmt.Errorf(
			"unexpected envelope type %q (want one of %v)",
			env.Type,
			accepted,
		)
	}
	raw, err := hex.DecodeString(env.CborHex)
	if err != nil {
		return nil, fmt.Errorf("decode cborHex: %w", err)
	}
	return raw, nil
}
