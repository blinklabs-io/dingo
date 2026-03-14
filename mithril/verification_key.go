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

package mithril

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	gcbor "github.com/blinklabs-io/gouroboros/cbor"
)

// VerificationKey is a parsed Mithril/Cardano verification key file.
type VerificationKey struct {
	Type        string
	Description string
	CborHex     string
	RawKeyBytes []byte
}

type textEnvelopeVerificationKey struct {
	Type        string `json:"type"`
	Description string `json:"description"`
	CborHex     string `json:"cborHex"`
}

// ParseVerificationKey parses a verification key from either Cardano text
// envelope JSON or a raw hex-encoded key string.
func ParseVerificationKey(data string) (*VerificationKey, error) {
	trimmed := strings.TrimSpace(data)
	if trimmed == "" {
		return nil, errors.New("verification key is empty")
	}

	var envelope textEnvelopeVerificationKey
	if err := json.Unmarshal([]byte(trimmed), &envelope); err == nil &&
		envelope.CborHex != "" {
		rawKey, err := decodeVerificationKeyCborHex(envelope.CborHex)
		if err != nil {
			return nil, err
		}
		return &VerificationKey{
			Type:        envelope.Type,
			Description: envelope.Description,
			CborHex:     envelope.CborHex,
			RawKeyBytes: rawKey,
		}, nil
	}

	rawKey, err := decodeHexString(trimmed)
	if err != nil {
		return nil, fmt.Errorf("parsing raw verification key hex: %w", err)
	}
	if mithrilKey, err := decodeMithrilJSONHexKey(rawKey); err == nil {
		rawKey = mithrilKey
	}
	return &VerificationKey{
		RawKeyBytes: rawKey,
	}, nil
}

func decodeVerificationKeyCborHex(cborHex string) ([]byte, error) {
	cborBytes, err := decodeHexString(cborHex)
	if err != nil {
		return nil, fmt.Errorf("decoding verification key cborHex: %w", err)
	}
	var keyBytes []byte
	if _, err := gcbor.Decode(cborBytes, &keyBytes); err != nil {
		return nil, fmt.Errorf("decoding verification key CBOR: %w", err)
	}
	if len(keyBytes) == 0 {
		return nil, errors.New("verification key CBOR decoded to empty bytes")
	}
	return keyBytes, nil
}

func decodeMithrilJSONHexKey(raw []byte) ([]byte, error) {
	var keyBytes []byte
	if err := json.Unmarshal(raw, &keyBytes); err != nil {
		return nil, fmt.Errorf(
			"unmarshaling mithril JSON hex key: %w", err,
		)
	}
	if len(keyBytes) == 0 {
		return nil, errors.New("mithril JSON-hex key decoded to empty bytes")
	}
	return keyBytes, nil
}

// RawKeyBytesHex returns the raw key bytes as a hex-encoded string.
func (v *VerificationKey) RawKeyBytesHex() string {
	return hex.EncodeToString(v.RawKeyBytes)
}

func decodeHexString(data string) ([]byte, error) {
	normalized := strings.TrimPrefix(strings.TrimSpace(data), "0x")
	if len(normalized)%2 != 0 {
		return nil, errors.New("hex string has odd length")
	}
	if normalized == "" {
		return nil, errors.New("hex string is empty")
	}
	ret, err := hex.DecodeString(normalized)
	if err != nil {
		return nil, fmt.Errorf("failed to decode hex string: %w", err)
	}
	return ret, nil
}
