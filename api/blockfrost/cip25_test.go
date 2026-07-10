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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCIP25Metadata(t *testing.T) {
	const policyID = "00112233445566778899aabbccddeeff00112233445566778899aabb"
	assetName := []byte("token") // ascii "token" -> hex 746f6b656e

	tests := []struct {
		name         string
		json         string
		wantStandard string
		wantOK       bool
		wantName     string
	}{
		{
			name:         "v1 utf8 asset key, no version",
			json:         `{"` + policyID + `":{"token":{"name":"My Token","image":"ipfs://x"}}}`,
			wantStandard: onchainMetadataStandardCIP25v1,
			wantOK:       true,
			wantName:     "My Token",
		},
		{
			name:         "v2 hex asset key, numeric version",
			json:         `{"` + policyID + `":{"746f6b656e":{"name":"Hex Token"}},"version":2}`,
			wantStandard: onchainMetadataStandardCIP25v2,
			wantOK:       true,
			wantName:     "Hex Token",
		},
		{
			name:         "v2 string version",
			json:         `{"` + policyID + `":{"746f6b656e":{"name":"Str Ver"}},"version":"2"}`,
			wantStandard: onchainMetadataStandardCIP25v2,
			wantOK:       true,
			wantName:     "Str Ver",
		},
		{
			name:   "policy not present",
			json:   `{"deadbeef":{"token":{"name":"nope"}}}`,
			wantOK: false,
		},
		{
			name:   "asset not present under policy",
			json:   `{"` + policyID + `":{"other":{"name":"nope"}}}`,
			wantOK: false,
		},
		{
			name:   "empty",
			json:   "",
			wantOK: false,
		},
		{
			name:   "invalid json",
			json:   "{not json",
			wantOK: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			meta, standard, ok := parseCIP25Metadata(tc.json, policyID, assetName)
			assert.Equal(t, tc.wantOK, ok)
			if !tc.wantOK {
				return
			}
			assert.Equal(t, tc.wantStandard, standard)
			m, isMap := meta.(map[string]any)
			require.True(t, isMap)
			assert.Equal(t, tc.wantName, m["name"])
		})
	}
}
