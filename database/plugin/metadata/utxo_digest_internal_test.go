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

package metadata

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCanonicalOutputCborIdempotent checks that canonical output CBOR is a
// fixed point, and that CBOR which is not an output is returned unchanged.
func TestCanonicalOutputCborIdempotent(t *testing.T) {
	t.Parallel()
	for name, h := range map[string]string{
		"legacy empty multiasset": "82581d606af53ff4f054348ad825c692dd9db8f1760a8e0eacf9af9f993065131b0000000019792b94",
		"zero quantity":           "82581d60ef0f4519a2c1856d9486bec2f938cad9dab5145c50449fa2cd7fbcd1821a00117e5ca1581cf6f49b186751e61f1fb8c64e7504e771f968cea9f4d11f5222b169e3a14674484f534b5900",
		"names unsorted":          "82581d70204215e45ce89569f0501597cd7bd7a97816b1f6c67643d7739cc54b821a0012378ea1581c204215e45ce89569f0501597cd7bd7a97816b1f6c67643d7739cc54ba24553656c6c54014a53656c6c4d617267696e01",
	} {
		raw, err := hex.DecodeString(h)
		require.NoError(t, err)
		once := canonicalOutputCbor(raw)
		require.NotEqual(t, raw, once, name)
		require.Equal(t, once, canonicalOutputCbor(once), name)
	}
	for name, raw := range map[string][]byte{
		"empty":             {},
		"uint":              {0x01},
		"truncated map":     {0xa2, 0x00},
		"map without value": {0xa1, 0x00, 0x41, 0x01},
		"value is text":     {0xa2, 0x00, 0x41, 0x01, 0x01, 0x61, 'a'},
	} {
		require.Equal(t, raw, canonicalOutputCbor(raw), name)
	}
}
