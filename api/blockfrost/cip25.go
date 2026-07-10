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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package blockfrost

import (
	"encoding/hex"
	"encoding/json"
)

const (
	// metadataLabelCIP25 is the transaction metadata label reserved for NFT
	// (and general native token) mint metadata by CIP-25.
	metadataLabelCIP25 = 721

	onchainMetadataStandardCIP25v1 = "CIP25v1"
	onchainMetadataStandardCIP25v2 = "CIP25v2"
)

// parseCIP25Metadata extracts the on-chain metadata object for a single asset
// from the JSON form of a transaction's CIP-25 (label 721) metadata.
//
// The label 721 structure is:
//
//	{ "<policy_id>": { "<asset_name>": { ...metadata... } }, "version": 1|2 }
//
// Dingo's metadata codec renders byte-string map keys as hex, so the policy id
// key is always the hex policy id for both v1 (text hex) and v2 (bytes) forms.
// The asset name key is the UTF-8 name in v1 and the hex name in v2, so both
// candidates are tried.
//
// It returns the metadata object, the detected standard ("CIP25v1" or
// "CIP25v2"), and whether a matching asset entry was found.
func parseCIP25Metadata(
	label721JSON string,
	policyIDHex string,
	assetName []byte,
) (any, string, bool) {
	if label721JSON == "" {
		return nil, "", false
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal([]byte(label721JSON), &top); err != nil {
		return nil, "", false
	}

	standard := onchainMetadataStandardCIP25v1
	if raw, ok := top["version"]; ok && isCIP25Version2(raw) {
		standard = onchainMetadataStandardCIP25v2
	}

	policyRaw, ok := top[policyIDHex]
	if !ok {
		return nil, "", false
	}
	var byAsset map[string]json.RawMessage
	if err := json.Unmarshal(policyRaw, &byAsset); err != nil {
		return nil, "", false
	}

	candidates := []string{hex.EncodeToString(assetName), string(assetName)}
	for _, key := range candidates {
		raw, ok := byAsset[key]
		if !ok {
			continue
		}
		var metadata any
		if err := json.Unmarshal(raw, &metadata); err != nil {
			return nil, "", false
		}
		return metadata, standard, true
	}
	return nil, "", false
}

// isCIP25Version2 reports whether a CIP-25 "version" value denotes version 2.
// The value may be encoded as a JSON number (2) or a JSON string ("2").
func isCIP25Version2(raw json.RawMessage) bool {
	var num json.Number
	if err := json.Unmarshal(raw, &num); err == nil {
		return num.String() == "2"
	}
	var str string
	if err := json.Unmarshal(raw, &str); err == nil {
		return str == "2"
	}
	return false
}
