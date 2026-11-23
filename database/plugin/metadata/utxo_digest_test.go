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

package metadata_test

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/blinklabs-io/dingo/database/plugin/metadata"
	"github.com/blinklabs-io/gouroboros/cbor"
	"github.com/stretchr/testify/require"
)

func digestOne(t *testing.T, output []byte) metadata.UtxoSetDigest {
	t.Helper()
	d := metadata.NewUtxoDigester()
	d.Add(
		[]byte{1, 2, 3}, 1, "5000000",
		[]byte{4}, nil, nil, true, output,
	)
	return d.Sum()
}

func mustCbor(t *testing.T, v any) []byte {
	t.Helper()
	b, err := cbor.Encode(v)
	require.NoError(t, err)
	return b
}

// TestUtxoDigesterLegacyArrayOutputMatchesMapForm checks that an output
// created in the pre-Babbage array form digests the same as the map form a
// snapshot import stores for it.
func TestUtxoDigesterLegacyArrayOutputMatchesMapForm(t *testing.T) {
	t.Parallel()
	addr := make([]byte, 29)
	addr[0] = 0x60
	datumHash := make([]byte, 32)
	datumHash[0] = 0xaa

	cases := map[string]struct{ legacy, mapped []byte }{
		"two element": {
			legacy: mustCbor(t, []any{addr, uint64(5000000)}),
			mapped: mustCbor(t, map[uint64]any{
				0: addr, 1: uint64(5000000),
			}),
		},
		"datum hash": {
			legacy: mustCbor(t, []any{addr, uint64(5000000), datumHash}),
			mapped: mustCbor(t, map[uint64]any{
				0: addr, 1: uint64(5000000),
				2: []any{uint64(0), datumHash},
			}),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Equal(
				t, digestOne(t, tc.mapped), digestOne(t, tc.legacy),
				"array and map encodings of one output must digest equal",
			)
		})
	}
}

// TestUtxoDigesterDistinguishesOutputs guards against canonicalisation
// collapsing outputs that differ.
func TestUtxoDigesterDistinguishesOutputs(t *testing.T) {
	t.Parallel()
	addr := make([]byte, 29)
	a := mustCbor(t, []any{addr, uint64(1)})
	b := mustCbor(t, []any{addr, uint64(2)})
	require.NotEqual(t, digestOne(t, a), digestOne(t, b))
	require.NotEqual(t, digestOne(t, a), digestOne(t, []byte{0xff}))
}

// TestUtxoDigesterByronOutputMatchesMapForm checks a Byron-era output, whose
// block bytes carry the address as an inline CBOR array, against the map form
// a snapshot import stores with the address bytes in a byte string.
func TestUtxoDigesterByronOutputMatchesMapForm(t *testing.T) {
	t.Parallel()
	payload := mustCbor(t, []any{
		make([]byte, 28), map[uint64]any{}, uint64(0),
	})
	addr := mustCbor(t, []any{
		cbor.Tag{Number: 24, Content: payload}, uint64(3735928559),
	})
	legacy := mustCbor(t, []any{cbor.RawMessage(addr), uint64(5000000)})
	mapped := mustCbor(t, map[uint64]any{0: addr, 1: uint64(5000000)})
	require.Equal(
		t, digestOne(t, mapped), digestOne(t, legacy),
		"a Byron output must digest as its map form",
	)
}

// rawAsset, rawPolicy and rawAssets hand-assemble a multiasset map with its
// entries in the order given, which cbor.Encode of a Go map would not keep.
// Names are short and quantities below 24, so every head is one byte.
func rawAsset(name string, qty byte) []byte {
	return append(append([]byte{0x40 | byte(len(name))}, name...), qty)
}

func rawPolicy(policy []byte, assets ...[]byte) []byte {
	out := append([]byte{0x58, byte(len(policy))}, policy...)
	out = append(out, 0xa0|byte(len(assets)))
	for _, a := range assets {
		out = append(out, a...)
	}
	return out
}

func rawAssets(policies ...[]byte) cbor.RawMessage {
	out := []byte{0xa0 | byte(len(policies))}
	for _, p := range policies {
		out = append(out, p...)
	}
	return out
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

// TestUtxoDigesterValueMatchesLedgerEncoding checks outputs as a block holds
// them against the form a snapshot import stores. The first four cases are
// recorded from a Preview snapshot and its blocks.
func TestUtxoDigesterValueMatchesLedgerEncoding(t *testing.T) {
	t.Parallel()
	addr := make([]byte, 29)
	addr[0] = 0x60
	pol1 := bytes.Repeat([]byte{0x11}, 28)
	pol2 := bytes.Repeat([]byte{0x22}, 28)
	datumHash := bytes.Repeat([]byte{0xaa}, 32)
	inline := cbor.Tag{Number: 24, Content: []byte{0x01}}
	type m = map[uint64]any
	type assets = map[cbor.ByteString]map[cbor.ByteString]uint64
	bs := cbor.NewByteString

	cases := map[string]struct{ block, ledger []byte }{
		"empty multiasset stripped": {
			block: mustHex(
				t,
				"825839001b9cdd4e5a6cb08501e46e10551b7a859d4a98251009bb91d69785f65691d68ad87582fc89b9ac43fd0227cfa4108efb791b9987b290a9ba821a00124f80a0",
			),
			ledger: mustHex(
				t,
				"a2005839001b9cdd4e5a6cb08501e46e10551b7a859d4a98251009bb91d69785f65691d68ad87582fc89b9ac43fd0227cfa4108efb791b9987b290a9ba011a00124f80",
			),
		},
		"asset names sorted bytewise": {
			block: mustHex(
				t,
				"82581d70204215e45ce89569f0501597cd7bd7a97816b1f6c67643d7739cc54b821a0012378ea1581c204215e45ce89569f0501597cd7bd7a97816b1f6c67643d7739cc54ba24553656c6c54014a53656c6c4d617267696e01",
			),
			ledger: mustHex(
				t,
				"a200581d70204215e45ce89569f0501597cd7bd7a97816b1f6c67643d7739cc54b01821a0012378ea1581c204215e45ce89569f0501597cd7bd7a97816b1f6c67643d7739cc54ba24a53656c6c4d617267696e014553656c6c5401",
			),
		},
		"zero quantity asset dropped": {
			block: mustHex(
				t,
				"82581d60ef0f4519a2c1856d9486bec2f938cad9dab5145c50449fa2cd7fbcd1821a00117e5ca1581cf6f49b186751e61f1fb8c64e7504e771f968cea9f4d11f5222b169e3a14674484f534b5900",
			),
			ledger: mustHex(
				t,
				"a200581d60ef0f4519a2c1856d9486bec2f938cad9dab5145c50449fa2cd7fbcd1011a00117e5c",
			),
		},
		"non-minimal integer head": {
			block: mustHex(
				t,
				"82581d606af53ff4f054348ad825c692dd9db8f1760a8e0eacf9af9f993065131b0000000019792b94",
			),
			ledger: mustHex(
				t,
				"a200581d606af53ff4f054348ad825c692dd9db8f1760a8e0eacf9af9f99306513011a19792b94",
			),
		},
		"policies sorted bytewise": {
			block: mustCbor(t, m{0: addr, 1: []any{uint64(2000000), rawAssets(
				rawPolicy(pol2, rawAsset("b", 1)),
				rawPolicy(pol1, rawAsset("a", 2)),
			)}}),
			ledger: mustCbor(t, m{0: addr, 1: []any{uint64(2000000), rawAssets(
				rawPolicy(pol1, rawAsset("a", 2)),
				rawPolicy(pol2, rawAsset("b", 1)),
			)}}),
		},
		"names sorted bytewise not length first": {
			block: mustCbor(t, m{0: addr, 1: []any{uint64(2000000), rawAssets(
				rawPolicy(pol1, rawAsset("b", 1), rawAsset("aa", 1)),
			)}}),
			ledger: mustCbor(t, m{0: addr, 1: []any{uint64(2000000), rawAssets(
				rawPolicy(pol1, rawAsset("aa", 1), rawAsset("b", 1)),
			)}}),
		},
		"partial zero quantity keeps rest": {
			block: mustCbor(t, m{0: addr, 1: []any{uint64(2000000), assets{
				bs(pol1): {bs([]byte("a")): 0, bs([]byte("b")): 7},
			}}}),
			ledger: mustCbor(t, m{0: addr, 1: []any{uint64(2000000), assets{
				bs(pol1): {bs([]byte("b")): 7},
			}}}),
		},
		"policy with only zero quantities dropped": {
			block: mustCbor(t, m{0: addr, 1: []any{uint64(2000000), assets{
				bs(pol1): {bs([]byte("a")): 0},
				bs(pol2): {bs([]byte("b")): 3},
			}}}),
			ledger: mustCbor(t, m{0: addr, 1: []any{uint64(2000000), assets{
				bs(pol2): {bs([]byte("b")): 3},
			}}}),
		},
		"legacy datum hash with empty multiasset": {
			block: mustCbor(t, []any{
				addr, []any{uint64(5000000), map[uint64]any{}}, datumHash,
			}),
			ledger: mustCbor(t, m{
				0: addr, 1: uint64(5000000), 2: []any{uint64(0), datumHash},
			}),
		},
		"inline datum with unsorted assets": {
			block: mustCbor(t, m{
				0: addr,
				1: []any{uint64(5000000), rawAssets(
					rawPolicy(pol1, rawAsset("b", 1), rawAsset("aa", 1)),
				)},
				2: []any{uint64(1), inline},
			}),
			ledger: mustCbor(t, m{
				0: addr,
				1: []any{uint64(5000000), rawAssets(
					rawPolicy(pol1, rawAsset("aa", 1), rawAsset("b", 1)),
				)},
				2: []any{uint64(1), inline},
			}),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Equal(
				t, digestOne(t, tc.ledger), digestOne(t, tc.block),
				"block and ledger encodings of one output must digest equal",
			)
		})
	}
}

// TestUtxoDigesterValueDistinguishesOutputs guards against the value
// canonicalisation collapsing outputs that differ.
func TestUtxoDigesterValueDistinguishesOutputs(t *testing.T) {
	t.Parallel()
	addr := make([]byte, 29)
	pol := bytes.Repeat([]byte{0x11}, 28)
	type assets = map[cbor.ByteString]map[cbor.ByteString]uint64
	bs := cbor.NewByteString
	out := func(coin uint64, a assets) []byte {
		return mustCbor(t, map[uint64]any{
			0: addr, 1: []any{coin, a},
		})
	}
	base := digestOne(t, out(1, assets{bs(pol): {bs([]byte("a")): 1}}))
	for name, other := range map[string][]byte{
		"quantity": out(1, assets{bs(pol): {bs([]byte("a")): 2}}),
		"name":     out(1, assets{bs(pol): {bs([]byte("b")): 1}}),
		"policy": out(1, assets{
			bs(bytes.Repeat([]byte{0x12}, 28)): {bs([]byte("a")): 1},
		}),
		"coin":      out(2, assets{bs(pol): {bs([]byte("a")): 1}}),
		"no assets": out(1, nil),
	} {
		require.NotEqual(t, base, digestOne(t, other), name)
	}
}

// longHead encodes b as a byte string whose length head is two bytes wide, a
// valid but non-minimal encoding.
func longHead(b []byte) []byte {
	return append([]byte{0x59, 0, byte(len(b))}, b...)
}

// chunked encodes b as an indefinite-length byte string of two chunks.
func chunked(t *testing.T, b []byte) []byte {
	t.Helper()
	out := []byte{0x5f}
	out = append(out, mustCbor(t, b[:1])...)
	out = append(out, mustCbor(t, b[1:])...)
	return append(out, 0xff)
}

// tag24 wraps an already encoded byte string in tag 24.
func tag24(encoded []byte) cbor.RawMessage {
	return append([]byte{0xd8, 0x18}, encoded...)
}

// TestUtxoDigesterFieldEncodingMatchesLedger checks an output's address,
// datum and reference script as a block may encode them against the form a
// snapshot import writes, which re-encodes each with its shortest heads.
func TestUtxoDigesterFieldEncodingMatchesLedger(t *testing.T) {
	t.Parallel()
	addr := make([]byte, 29)
	addr[0] = 0x60
	datumHash := bytes.Repeat([]byte{0xaa}, 32)
	datum := mustCbor(t, []any{uint64(1), []byte("datum")})
	native := mustCbor(t, []any{uint64(0), bytes.Repeat([]byte{0x33}, 28)})
	plutus := bytes.Repeat([]byte{0x44}, 40)
	plutusRef := mustCbor(t, []any{uint64(2), plutus})
	nativeRef := mustCbor(t, []any{uint64(0), cbor.RawMessage(native)})
	type m = map[uint64]any
	const coin = uint64(5000000)
	inline := []any{uint64(1), cbor.Tag{Number: 24, Content: datum}}

	cases := map[string]struct{ block, ledger []byte }{
		"address head non-minimal": {
			block:  mustCbor(t, m{0: cbor.RawMessage(longHead(addr)), 1: coin}),
			ledger: mustCbor(t, m{0: addr, 1: coin}),
		},
		"legacy address head non-minimal": {
			block:  mustCbor(t, []any{cbor.RawMessage(longHead(addr)), coin}),
			ledger: mustCbor(t, m{0: addr, 1: coin}),
		},
		"datum hash head non-minimal": {
			block: mustCbor(t, m{0: addr, 1: coin, 2: []any{
				uint64(0), cbor.RawMessage(longHead(datumHash)),
			}}),
			ledger: mustCbor(t, m{
				0: addr, 1: coin, 2: []any{uint64(0), datumHash},
			}),
		},
		"inline datum head non-minimal": {
			block: mustCbor(t, m{0: addr, 1: coin, 2: []any{
				uint64(1), tag24(longHead(datum)),
			}}),
			ledger: mustCbor(t, m{0: addr, 1: coin, 2: inline}),
		},
		"inline datum chunked": {
			block: mustCbor(t, m{0: addr, 1: coin, 2: []any{
				uint64(1), tag24(chunked(t, datum)),
			}}),
			ledger: mustCbor(t, m{0: addr, 1: coin, 2: inline}),
		},
		"plutus script ref head non-minimal": {
			block: mustCbor(t, m{
				0: addr, 1: coin, 3: tag24(longHead(plutusRef)),
			}),
			ledger: mustCbor(t, m{
				0: addr, 1: coin, 3: cbor.Tag{Number: 24, Content: plutusRef},
			}),
		},
		"plutus script bytes head non-minimal": {
			block: mustCbor(t, m{0: addr, 1: coin, 3: cbor.Tag{
				Number:  24,
				Content: append([]byte{0x82, 0x02}, longHead(plutus)...),
			}}),
			ledger: mustCbor(t, m{
				0: addr, 1: coin, 3: cbor.Tag{Number: 24, Content: plutusRef},
			}),
		},
		"native script ref head non-minimal": {
			block: mustCbor(t, m{
				0: addr, 1: coin, 3: tag24(longHead(nativeRef)),
			}),
			ledger: mustCbor(t, m{
				0: addr, 1: coin, 3: cbor.Tag{Number: 24, Content: nativeRef},
			}),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Equal(
				t, digestOne(t, tc.ledger), digestOne(t, tc.block),
				"block and ledger encodings of one output must digest equal",
			)
		})
	}
}

// TestUtxoDigesterFieldsDistinguishOutputs guards against the field
// canonicalisation collapsing outputs whose datum or script differ.
func TestUtxoDigesterFieldsDistinguishOutputs(t *testing.T) {
	t.Parallel()
	addr := make([]byte, 29)
	type m = map[uint64]any
	out := func(k uint64, v any) []byte {
		return mustCbor(t, m{0: addr, 1: uint64(1), k: v})
	}
	ref := func(typ uint64, script []byte) cbor.Tag {
		return cbor.Tag{Number: 24, Content: mustCbor(t, []any{typ, script})}
	}
	inline := func(d []byte) []any {
		return []any{uint64(1), cbor.Tag{Number: 24, Content: d}}
	}
	outs := map[string][]byte{
		"no datum":      mustCbor(t, m{0: addr, 1: uint64(1)}),
		"datum hash":    out(2, []any{uint64(0), bytes.Repeat([]byte{1}, 32)}),
		"other hash":    out(2, []any{uint64(0), bytes.Repeat([]byte{2}, 32)}),
		"inline datum":  out(2, inline([]byte{0x01})),
		"other datum":   out(2, inline([]byte{0x02})),
		"plutus v2 ref": out(3, ref(2, []byte{0x01})),
		"plutus v3 ref": out(3, ref(3, []byte{0x01})),
		"other script":  out(3, ref(2, []byte{0x02})),
	}
	seen := map[metadata.UtxoSetDigest]string{}
	for name, o := range outs {
		d := digestOne(t, o)
		require.NotContains(t, seen, d, "%s collides with %s", name, seen[d])
		seen[d] = name
	}
}

// TestUtxoDigesterAddCanonicalHashesBytesAsGiven checks that AddCanonical
// digests exactly what Add does once the output is canonicalized, and does
// not canonicalize again.
func TestUtxoDigesterAddCanonicalHashesBytesAsGiven(t *testing.T) {
	t.Parallel()
	addr := make([]byte, 29)
	addr[0] = 0x60
	legacy := mustCbor(t, []any{addr, uint64(5000000)})
	canonical := metadata.CanonicalOutputCbor(legacy)
	require.NotEqual(t, legacy, canonical)

	add := func(canonicalize bool, out []byte) metadata.UtxoSetDigest {
		d := metadata.NewUtxoDigester()
		if canonicalize {
			d.Add([]byte{1}, 0, "5", nil, nil, nil, true, out)
		} else {
			d.AddCanonical([]byte{1}, 0, "5", nil, nil, nil, true, out)
		}
		return d.Sum()
	}
	require.Equal(t, add(true, legacy), add(false, canonical))
	require.NotEqual(t, add(true, legacy), add(false, legacy))
	require.Equal(
		t, canonical, metadata.CanonicalOutputCbor(canonical),
		"canonical form is a fixed point",
	)
}
