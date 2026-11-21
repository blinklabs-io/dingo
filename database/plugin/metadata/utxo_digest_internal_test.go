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

// TestCanonicalOutputCborIdempotent checks that canonical output CBOR, and an
// imported output, is a fixed point, and that CBOR which is not an output is
// returned unchanged.
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
	// Outputs as a Preview snapshot import stored them: a digest the ledger
	// recorded over them before the canonicalisation existed stays valid only
	// if they are already canonical.
	for _, h := range []string{
		"a2005839001b9cdd4e5a6cb08501e46e10551b7a859d4a98251009bb91d69785f65691d68ad87582fc89b9ac43fd0227cfa4108efb791b9987b290a9ba011a00124f80",
		"a200581d70204215e45ce89569f0501597cd7bd7a97816b1f6c67643d7739cc54b01821a0012378ea1581c204215e45ce89569f0501597cd7bd7a97816b1f6c67643d7739cc54ba24a53656c6c4d617267696e014553656c6c5401",
		"a200581d60ef0f4519a2c1856d9486bec2f938cad9dab5145c50449fa2cd7fbcd1011a00117e5c",
	} {
		raw, err := hex.DecodeString(h)
		require.NoError(t, err)
		require.Equal(t, raw, canonicalOutputCbor(raw), h)
	}
	for name, raw := range map[string][]byte{
		"empty":             {},
		"uint":              {0x01},
		"truncated map":     {0xa2, 0x00},
		"map without value": {0xa1, 0x00, 0x41, 0x01},
		"value is text":     {0xa2, 0x00, 0x41, 0x01, 0x01, 0x61, 'a'},
		"unknown datum kind": {
			0xa3, 0x00, 0x41, 0x01, 0x01, 0x01, 0x02, 0x82, 0x02, 0x00,
		},
		"script ref untagged": {
			0xa3, 0x00, 0x41, 0x01, 0x01, 0x01, 0x03, 0x41, 0x00,
		},
	} {
		require.Equal(t, raw, canonicalOutputCbor(raw), name)
	}
}

// TestCanonicalOutputCborTrailingBytes checks that an output followed by
// bytes past its CBOR item, at the top level or inside a reference script's
// tag 24 wrapper, is hashed as stored rather than canonicalised to the clean
// output's bytes, so a corrupt stored output still digests differently.
func TestCanonicalOutputCborTrailingBytes(t *testing.T) {
	t.Parallel()
	mustHex := func(h string) []byte {
		b, err := hex.DecodeString(h)
		require.NoError(t, err)
		return b
	}
	outputs := map[string][]byte{
		"imported": mustHex(
			"a200581d60ef0f4519a2c1856d9486bec2f938cad9dab5145c50449fa2cd7fbcd1011a00117e5c",
		),
		"legacy": mustHex(
			"82581d60ef0f4519a2c1856d9486bec2f938cad9dab5145c50449fa2cd7fbcd1821a00117e5ca1581cf6f49b186751e61f1fb8c64e7504e771f968cea9f4d11f5222b169e3a14674484f534b5900",
		),
		// {0: h'01', 1: 1, 3: #6.24(h'82004100')}
		"script ref": mustHex("a300410101010" + "3d818448200" + "4100"),
	}
	suffixes := map[string][]byte{
		"break byte":    {0xff},
		"complete item": {0x00},
		"second output": outputs["imported"],
	}
	for name, clean := range outputs {
		want := canonicalOutputCbor(clean)
		for sname, suffix := range suffixes {
			dirty := append(append([]byte{}, clean...), suffix...)
			got := canonicalOutputCbor(dirty)
			require.Equal(t, dirty, got, "%s + %s", name, sname)
			require.NotEqual(t, want, got, "%s + %s", name, sname)
		}
	}
	for name, dirty := range map[string][]byte{
		// The tag 24 content [0, h'00'] followed by 0x00.
		"script ref content": mustHex("a3004101010103d81845820041" + "0000"),
		// The tag 24 content [0, h'00'] followed by 0xff.
		"script ref content break": mustHex(
			"a3004101010103d81845820041" + "00ff",
		),
	} {
		require.Equal(t, dirty, canonicalOutputCbor(dirty), name)
		require.NotEqual(
			t,
			canonicalOutputCbor(outputs["script ref"]),
			canonicalOutputCbor(dirty),
			name,
		)
	}
	// An inline datum's bytes are kept, so bytes past the datum item inside
	// its tag 24 wrapper survive canonicalisation.
	// {0: h'01', 1: 1, 2: [1, #6.24(h'00')]} with and without a trailing 0x00.
	inline := canonicalOutputCbor(mustHex("a30041010101028201d8184100"))
	padded := canonicalOutputCbor(mustHex("a30041010101028201d818420000"))
	require.NotEqual(t, inline, padded)
}
